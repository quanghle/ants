//! The ant's entry in the colony, kept alive by heartbeats.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::{
    ANTS_VERSION, Activity, AntModel, COLONY_HEARTBEAT, COLONY_TTL, ColonyEntry, HarnessAgent, HarnessFailing,
    HarnessLimit, MemoryLow, PERMISSIONS_VERSION, Paused, ServingFallback, SignInNeeded, UserId,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, kv};
use chrono::{DateTime, Utc};
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::bus;
use crate::daemon::Config;
use crate::sync::MutexExt as _;

/// An ant listed in the colony. Dropping it stops the heartbeats, so the entry expires;
/// [`Registration::deregister`] removes it at once.
pub struct Registration {
    kv: kv::Store,
    key: String,
    heartbeat: JoinHandle<()>,
    /// Of a held entry ([`Registration::hold`]): the revision last put, `None` once another daemon put the entry.
    held: Option<watch::Receiver<Option<u64>>>,
}

/// What the daemon is doing, shared by the inbox loop and the heartbeats: the messages being handled, the live
/// harnesses, the agent the latest harness start named, whether the harness is signed out, held back by a provider
/// limit or failing to start, the fallback answering meanwhile, whether memory is low, whether the ant is paused, its
/// drift and the profile it published.
#[derive(Debug, Default)]
pub struct Workload {
    handling: AtomicUsize,
    harnesses: AtomicUsize,
    agent: Mutex<Option<HarnessAgent>>,
    signed_out: Mutex<Option<SignInNeeded>>,
    limited: Mutex<Option<HarnessLimit>>,
    failing: Mutex<Option<HarnessFailing>>,
    fallback: Mutex<Option<ServingFallback>>,
    memory_low: Mutex<Option<MemoryLow>>,
    paused: Mutex<Option<Paused>>,
    drift: Mutex<Vec<String>>,
    profile: Mutex<Option<String>>,
    /// When the last message handled ended.
    worked: Mutex<Option<DateTime<Utc>>>,
    /// Wakes the heartbeats when the ant turns idle or working, its harness signed out or in or started with another
    /// agent, a provider limit starts or ends, the harness's start failures hold messages or no longer do, a fallback
    /// starts or stops answering, memory turns low or recovers, the ant is paused or resumed, or its drift or profile
    /// changes, so the colony shows it before the next beat.
    changed: Notify,
}

impl Workload {
    /// Count a message as handled until the returned guard drops.
    pub fn working(&self) -> Working<'_> {
        if self.handling.fetch_add(1, Ordering::Relaxed) == 0 {
            self.changed.notify_one();
        }
        Working(self)
    }

    pub fn set_harnesses(&self, live: usize) {
        self.harnesses.store(live, Ordering::Relaxed);
    }

    /// Note the agent a harness just started with named itself as, `None` when it did not name itself.
    pub fn set_agent(&self, agent: Option<HarnessAgent>) {
        let mut current = self.agent.locked();
        if *current != agent {
            *current = agent;
            self.changed.notify_one();
        }
    }

    pub fn agent(&self) -> Option<HarnessAgent> {
        self.agent.locked().clone()
    }

    /// Note the harness as signed out, or with `None` as signed in.
    pub fn set_signed_out(&self, signed_out: Option<SignInNeeded>) {
        let mut current = self.signed_out.locked();
        if *current != signed_out {
            *current = signed_out;
            self.changed.notify_one();
        }
    }

    pub fn signed_out(&self) -> Option<SignInNeeded> {
        self.signed_out.locked().clone()
    }

    /// Note the provider limit messages wait out, or with `None` that they no longer wait.
    pub fn set_limited(&self, limited: Option<HarnessLimit>) {
        let mut current = self.limited.locked();
        if *current != limited {
            *current = limited;
            self.changed.notify_one();
        }
    }

    pub fn limited(&self) -> Option<HarnessLimit> {
        self.limited.locked().clone()
    }

    /// Note that the harness's start failures hold the messages, or with `None` that they no longer do.
    pub fn set_failing(&self, failing: Option<HarnessFailing>) {
        let mut current = self.failing.locked();
        if *current != failing {
            *current = failing;
            self.changed.notify_one();
        }
    }

    pub fn failing(&self) -> Option<HarnessFailing> {
        self.failing.locked().clone()
    }

    /// Note the fallback answering for the ant's own harness, or with `None` that the ant's own answers again.
    pub fn set_fallback(&self, fallback: Option<ServingFallback>) {
        let mut current = self.fallback.locked();
        if *current != fallback {
            *current = fallback;
            self.changed.notify_one();
        }
    }

    /// Note that memory is low, or with `None` that it is not. Only turning low or recovering wakes the heartbeats.
    pub fn set_memory_low(&self, low: Option<MemoryLow>) {
        let mut current = self.memory_low.locked();
        let changed = current.is_some() != low.is_some();
        *current = low;
        if changed {
            self.changed.notify_one();
        }
    }

    pub fn fallback(&self) -> Option<ServingFallback> {
        self.fallback.locked().clone()
    }

    /// Note the ant as paused, or with `None` as resumed.
    pub fn set_paused(&self, paused: Option<Paused>) {
        let mut current = self.paused.locked();
        if *current != paused {
            *current = paused;
            self.changed.notify_one();
        }
    }

    pub fn paused(&self) -> Option<Paused> {
        self.paused.locked().clone()
    }

    /// Note the fields of the ant's shared settings its machine's config sets otherwise.
    pub fn set_drift(&self, drift: Vec<String>) {
        let mut current = self.drift.locked();
        if *current != drift {
            *current = drift;
            self.changed.notify_one();
        }
    }

    pub fn drift(&self) -> Vec<String> {
        self.drift.locked().clone()
    }

    /// Note the profile the ant's confirmed persona holds, or with `None` that it holds none.
    pub fn set_profile(&self, profile: Option<String>) {
        let mut current = self.profile.locked();
        if *current != profile {
            *current = profile;
            self.changed.notify_one();
        }
    }

    pub fn profile(&self) -> Option<String> {
        self.profile.locked().clone()
    }

    pub fn memory_low(&self) -> Option<MemoryLow> {
        self.memory_low.locked().clone()
    }

    pub fn handling(&self) -> usize {
        self.handling.load(Ordering::Relaxed)
    }

    /// When the last message handled ended; none before the first.
    pub fn worked(&self) -> Option<DateTime<Utc>> {
        *self.worked.locked()
    }

    pub fn harnesses(&self) -> usize {
        self.harnesses.load(Ordering::Relaxed)
    }

    pub fn activity(&self) -> Activity {
        if self.handling() == 0 { Activity::Idle } else { Activity::Working }
    }

    /// Write the current workload into `entry`.
    pub fn stamp(&self, entry: &mut ColonyEntry) {
        let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        entry.activity = self.activity();
        entry.handling = count(self.handling());
        entry.harnesses = count(self.harnesses());
        entry.agent = self.agent();
        entry.signed_out = self.signed_out();
        entry.limited = self.limited();
        entry.failing = self.failing();
        entry.fallback = self.fallback();
        entry.memory_low = self.memory_low();
        entry.paused = self.paused();
        entry.drift = self.drift();
        entry.profile = self.profile();
    }
}

/// A message being handled, counted in its [`Workload`] while alive.
pub struct Working<'a>(&'a Workload);

impl Drop for Working<'_> {
    fn drop(&mut self) {
        *self.0.worked.locked() = Some(Utc::now());
        if self.0.handling.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.changed.notify_one();
        }
    }
}

/// The colony entry of the ant of `config` on this machine, started now, naming the machine as `config` does, else by
/// its host name.
pub async fn entry(config: &Config) -> ColonyEntry {
    let now = Utc::now();
    let (presence, harness) = (&config.presence, &config.harness);
    let machine = match &presence.machine {
        Some(machine) => machine.clone(),
        None => machine().await,
    };
    ColonyEntry {
        ant: config.ant.clone(),
        machine,
        instance: uuid::Uuid::new_v4().to_string(),
        harness: harness.harness.to_string(),
        agent: None,
        capabilities: presence.capabilities.clone(),
        whoami: presence.whoami.clone(),
        profile: None,
        workdir: Some(harness.workdir.display().to_string()),
        started_at: now,
        heartbeat_at: now,
        activity: Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: config.policy.escalation.escalate_to.clone(),
        sage: config.policy.sage.clone(),
        version: ANTS_VERSION.to_owned(),
        perms: PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
        paused: config.pause.get(),
        drift: config.drift.get(),
        mcp: harness.harness.reports_mcp(),
        model: AntModel::of(
            harness.model_settings.model.clone(),
            harness.model_settings.reasoning_effort.clone(),
            harness.model_settings.context.clone(),
        ),
    }
}

/// List `entry` in the colony and keep it fresh, with its `workload`, until the registration ends.
pub async fn register(
    js: &jetstream::Context,
    mut entry: ColonyEntry,
    workload: Arc<Workload>,
) -> Result<Registration> {
    let kv = bus::ensure_colony(js, COLONY_TTL).await?;
    entry.heartbeat_at = Utc::now();
    Registration::start_with(kv, entry, COLONY_HEARTBEAT, Some(workload)).await
}

/// Whether a daemon may start serving an ant as `ours` without serving it twice, when the ant's colony entry is
/// `current`: only if the colony lists none, or `ours` itself (a put whose answer was lost). An entry stays in the
/// colony until its daemon stops or it goes [`COLONY_TTL`] without a heartbeat, by the server's clock, so no clocks are
/// compared. Two daemons on one machine are told apart by [`ColonyEntry::instance`].
pub fn may_start(current: Option<&ColonyEntry>, ours: &ColonyEntry) -> bool {
    current.is_none_or(|e| e.instance == ours.instance)
}

/// What one look at an ant's colony entry came to.
enum Claim {
    /// The entry was put, at this revision.
    Put(u64),
    /// Another daemon's entry is there.
    Held { machine: String, instance: String },
    /// The entry changed between the look and the put.
    Raced,
}

/// Put `entry` if [`may_start`] allows it, expecting the revision looked at, so of two daemons only one goes ahead.
async fn claim(kv: &kv::Store, entry: &ColonyEntry) -> Result<Claim> {
    let key = entry.key();
    let found = kv.entry(key).await.with_context(|| format!("reading {key} in the colony"))?;
    let current = found
        .as_ref()
        .filter(|e| e.operation == kv::Operation::Put)
        .and_then(|e| serde_json::from_slice::<ColonyEntry>(&e.value).ok());
    if let Some(other) = current.filter(|c| !may_start(Some(c), entry)) {
        return Ok(Claim::Held { machine: other.machine, instance: other.instance });
    }
    let value = serde_json::to_vec(entry)?.into();
    let put = match &found {
        Some(e) => kv
            .update(key, value, e.revision)
            .await
            .map_err(|e| (e.kind() == kv::UpdateErrorKind::WrongLastRevision, anyhow::Error::from(e))),
        None => kv
            .create(key, value)
            .await
            .map_err(|e| (e.kind() == kv::CreateErrorKind::AlreadyExists, anyhow::Error::from(e))),
    };
    match put {
        Ok(revision) => Ok(Claim::Put(revision)),
        Err((true, _)) => Ok(Claim::Raced),
        Err((false, e)) => Err(e.context(format!("putting {key} in the colony"))),
    }
}

/// How often [`wait_for_turn`] looks at the colony when a watch shows the entry's expiry: only in case the watch
/// misses it.
pub const LOOK_AGAIN: Duration = Duration::from_secs(60);

/// Wait until [`may_start`] allows `entry`, then put `entry`, started now, and return the revision put, for [`hold`].
/// The put expects the entry it looked at, so of two daemons starting at once only one goes ahead; the other waits on.
/// It looks again as soon as a watch of the entry shows it deleted or expired (the bucket leaves a marker of an
/// expired entry), and otherwise only every [`LOOK_AGAIN`]; every `every` when it has no watch.
pub async fn wait_for_turn(kv: &kv::Store, entry: &mut ColonyEntry, every: Duration) -> Result<u64> {
    let key = entry.key().to_owned();
    let mut changes = match kv.watch(&key).await {
        Ok(changes) => Some(changes),
        Err(e) => {
            tracing::warn!(ant = key, error = %e, "watching the colony entry; looking at the colony every {every:?}");
            None
        }
    };
    let mut waiting_for = None;
    loop {
        let now = Utc::now();
        (entry.started_at, entry.heartbeat_at) = (now, now);
        let mut wait = every;
        match claim(kv, entry).await {
            Ok(Claim::Put(revision)) => return Ok(revision),
            Ok(Claim::Held { machine, instance }) => {
                if waiting_for.as_ref() != Some(&instance) {
                    tracing::info!(ant = key, machine, "waiting while another daemon serves it");
                    waiting_for = Some(instance);
                }
                if changes.is_some() {
                    wait = every.max(LOOK_AGAIN);
                }
            }
            Ok(Claim::Raced) => {
                tracing::info!(ant = key, "colony entry changed; looking again");
                continue;
            }
            Err(e) => tracing::warn!(ant = key, error = format!("{e:#}"), "looking at the colony; trying again"),
        }
        let look_again = tokio::time::sleep(wait);
        tokio::pin!(look_again);
        loop {
            tokio::select! {
                () = &mut look_again => break,
                change = next_change(&mut changes) => match change {
                    Some(change) if change.operation == kv::Operation::Put => {}
                    // Deleted or expired, or the watch ended: look now.
                    _ => break,
                },
            }
        }
    }
}

/// List `entry` in the colony over `revision`, which [`wait_for_turn`] put, and keep it fresh with its `workload` as
/// [`register`] does, as long as no other daemon puts the entry: then the registration is lost
/// ([`Registration::lost`]) and puts nothing more.
pub async fn hold(
    js: &jetstream::Context,
    entry: ColonyEntry,
    revision: u64,
    workload: Arc<Workload>,
) -> Result<Registration> {
    let kv = bus::ensure_colony(js, COLONY_TTL).await?;
    Registration::hold(kv, entry, revision, COLONY_HEARTBEAT, Some(workload)).await
}

/// Put `entry` over `revision`. When another write came in between: put it again if it is gone or ours, and give the
/// revision put, or `None` if another daemon's entry is there.
async fn renew(kv: &kv::Store, entry: &ColonyEntry, revision: u64) -> Result<Option<u64>> {
    match kv.update(entry.key(), serde_json::to_vec(entry)?.into(), revision).await {
        Ok(revision) => return Ok(Some(revision)),
        Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => {}
        Err(e) => return Err(e.into()),
    }
    loop {
        match claim(kv, entry).await? {
            Claim::Put(revision) => return Ok(Some(revision)),
            Claim::Held { .. } => return Ok(None),
            Claim::Raced => {}
        }
    }
}

/// Wait until `entry` is due for a put: at the next tick of `ticks`, or as soon as `workload` turns idle or working.
/// Then stamp it with the workload and the time.
async fn due(ticks: &mut tokio::time::Interval, workload: Option<&Workload>, entry: &mut ColonyEntry) {
    loop {
        let changed = async {
            match workload {
                Some(w) => w.changed.notified().await,
                None => std::future::pending().await,
            }
        };
        let beat = tokio::select! {
            _ = ticks.tick() => true,
            () = changed => false,
        };
        if let Some(workload) = workload {
            if !beat
                && entry.activity == workload.activity()
                && entry.agent == workload.agent()
                && entry.signed_out == workload.signed_out()
                && entry.limited == workload.limited()
                && entry.failing == workload.failing()
                && entry.fallback == workload.fallback()
                && entry.paused == workload.paused()
                && entry.drift == workload.drift()
                && entry.profile == workload.profile()
            {
                // Turned and turned back before this task ran: nothing new to show.
                continue;
            }
            workload.stamp(entry);
        }
        entry.heartbeat_at = Utc::now();
        return;
    }
}

/// The heartbeats of a held entry, put at `held`'s revision until another daemon's write shows up, in `changes` or
/// as a failed put: then `held` turns `None`.
async fn keep_held(
    kv: kv::Store,
    mut entry: ColonyEntry,
    every: Duration,
    workload: Option<Arc<Workload>>,
    held: watch::Sender<Option<u64>>,
    mut changes: Option<kv::Watch>,
) {
    let Some(mut revision) = *held.borrow() else { return };
    let mut ticks = tokio::time::interval(every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticks.tick().await;
    loop {
        tokio::select! {
            () = due(&mut ticks, workload.as_deref(), &mut entry) => {}
            Some(change) = next_change(&mut changes) => {
                if change.revision <= revision {
                    continue;
                }
                if change.operation == kv::Operation::Put {
                    let ours = serde_json::from_slice::<ColonyEntry>(&change.value)
                        .is_ok_and(|e| e.instance == entry.instance);
                    if !ours {
                        break;
                    }
                    continue;
                }
                // Deleted under it: put it back now, unless another daemon got there first.
                entry.heartbeat_at = Utc::now();
            }
        }
        match renew(&kv, &entry, revision).await {
            Ok(Some(put)) => {
                revision = put;
                held.send_replace(Some(put));
            }
            Ok(None) => break,
            Err(e) => tracing::warn!(ant = %entry.ant, error = format!("{e:#}"), "colony heartbeat"),
        }
    }
    tracing::info!(ant = %entry.ant, "another daemon took over the colony entry");
    held.send_replace(None);
}

/// The next change `changes` shows, if any; on an error it stops watching, so only the heartbeats see changes.
async fn next_change(changes: &mut Option<kv::Watch>) -> Option<kv::Entry> {
    let Some(watch) = changes else { return std::future::pending().await };
    match watch.next().await {
        Some(Ok(change)) => Some(change),
        Some(Err(e)) => {
            tracing::warn!(error = %e, "watching the colony entry; the heartbeats still notice another daemon");
            *changes = None;
            None
        }
        None => {
            *changes = None;
            None
        }
    }
}

impl Registration {
    /// Put `entry` now, then again every `every`. The first put must succeed; later failures are logged and retried
    /// on the next beat, so a brief outage costs nothing as long as it is shorter than the bucket's max age.
    pub async fn start(kv: kv::Store, entry: ColonyEntry, every: Duration) -> Result<Self> {
        Self::start_with(kv, entry, every, None).await
    }

    /// Like [`Registration::start`], with `workload` stamped on every put, and an extra put as soon as the ant
    /// turns idle or working.
    pub async fn start_with(
        kv: kv::Store,
        mut entry: ColonyEntry,
        every: Duration,
        workload: Option<Arc<Workload>>,
    ) -> Result<Self> {
        let key = entry.key().to_owned();
        if let Some(workload) = &workload {
            workload.stamp(&mut entry);
        }
        put(&kv, &entry).await.with_context(|| format!("registering {key} in the colony"))?;
        let beat_kv = kv.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(every);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticks.tick().await;
            loop {
                due(&mut ticks, workload.as_deref(), &mut entry).await;
                if let Err(e) = put(&beat_kv, &entry).await {
                    tracing::warn!(ant = %entry.ant, error = format!("{e:#}"), "colony heartbeat");
                }
            }
        });
        Ok(Self { kv, key, heartbeat, held: None })
    }

    /// Like [`Registration::start_with`], but every put expects the revision put before, starting at `revision`, and
    /// the registration is lost as soon as another daemon puts the entry: on its watch of the entry, or at the next
    /// heartbeat. An entry deleted or expired under it is put again if no other daemon got there first.
    pub async fn hold(
        kv: kv::Store,
        mut entry: ColonyEntry,
        revision: u64,
        every: Duration,
        workload: Option<Arc<Workload>>,
    ) -> Result<Self> {
        let key = entry.key().to_owned();
        let changes = match kv.watch(&key).await {
            Ok(changes) => Some(changes),
            Err(e) => {
                tracing::warn!(ant = key, error = %e, "watching the colony entry; the heartbeats still notice another daemon");
                None
            }
        };
        if let Some(workload) = &workload {
            workload.stamp(&mut entry);
        }
        entry.heartbeat_at = Utc::now();
        let first = renew(&kv, &entry, revision).await.with_context(|| format!("registering {key} in the colony"))?;
        let (held, watching) = watch::channel(first);
        let heartbeat = tokio::spawn(keep_held(kv.clone(), entry, every, workload, held, changes));
        Ok(Self { kv, key, heartbeat, held: Some(watching) })
    }

    /// Resolves once another daemon put the entry this registration held ([`Registration::hold`]); never for one that
    /// puts it regardless ([`Registration::start`]).
    pub async fn lost(&self) {
        match &self.held {
            Some(held) => drop(held.clone().wait_for(Option::is_none).await),
            None => std::future::pending().await,
        }
    }

    /// Whether another daemon put the entry this registration held.
    pub fn is_lost(&self) -> bool {
        self.held.as_ref().is_some_and(|held| held.borrow().is_none())
    }

    /// Stop the heartbeats and remove the entry, unless another daemon put it since.
    pub async fn deregister(self) {
        self.heartbeat.abort();
        let deleted = match &self.held {
            None => self.kv.delete(&self.key).await,
            Some(held) => {
                let revision = *held.borrow();
                match revision {
                    Some(revision) => self.kv.delete_expect_revision(&self.key, Some(revision)).await,
                    None => return,
                }
            }
        };
        if let Err(e) = deleted {
            tracing::warn!(ant = %self.key, error = %e, "leaving the colony; the entry expires on its own");
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

/// The entries in the colony, by ant. An entry removed while listing is left out, and a malformed one is skipped
/// with a warning. So is one naming another ant than its key: credentials vouch only for the key.
pub async fn list_entries(kv: &kv::Store) -> Result<Vec<ColonyEntry>> {
    let mut keys: Vec<String> = kv.keys().await.context("listing the colony")?.try_collect().await?;
    keys.sort();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(value) = kv.get(&key).await.with_context(|| format!("reading {key} in the colony"))? else { continue };
        match serde_json::from_slice::<ColonyEntry>(&value) {
            Ok(entry) => match entry.check_key(&key) {
                Ok(()) => entries.push(entry),
                Err(e) => tracing::warn!(key, error = %e, "skipping a colony entry for another ant"),
            },
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed colony entry"),
        }
    }
    Ok(entries)
}

/// Whether `entry` advertises `capability`. Tags compare whole, ignoring case and surrounding space.
pub fn has_capability(entry: &ColonyEntry, capability: &str) -> bool {
    let capability = capability.trim();
    entry.capabilities.iter().any(|c| c.trim().eq_ignore_ascii_case(capability))
}

/// An entry not refreshed for this long has missed a heartbeat: its daemon may be gone, and the entry expires once
/// [`COLONY_TTL`] has passed.
pub const STALE_AFTER: Duration = COLONY_HEARTBEAT.saturating_mul(2);

/// Whether `entry` was refreshed within [`STALE_AFTER`] of `now`. A heartbeat ahead of `now` counts as fresh.
pub fn is_live(entry: &ColonyEntry, now: DateTime<Utc>) -> bool {
    (now - entry.heartbeat_at).to_std().unwrap_or_default() <= STALE_AFTER
}

/// The ant a request needing `capability` goes to, of the entries other than `asking` that advertise it: ants not
/// paused before paused ones, then ants whose harness is signed in and starting before those signed out or failing to
/// start, then those held back by no provider limit before limited ones, unless a fallback answers for them, then
/// those answered by their own harness before those answered by a fallback, then live entries before stale ones, then
/// idle ants before those of an activity unknown to this daemon (newer daemons) and those working, then the fewest
/// messages in hand, then the first by id. The same colony at the same time always gives the same answer.
pub fn pick<'a>(
    entries: &'a [ColonyEntry],
    capability: &str,
    asking: &UserId,
    now: DateTime<Utc>,
) -> Option<&'a ColonyEntry> {
    entries.iter().filter(|e| &e.ant != asking && has_capability(e, capability)).min_by_key(|e| {
        let busyness = match e.activity {
            Activity::Idle => 0,
            Activity::Unknown => 1,
            Activity::Working => 2,
        };
        (
            e.paused.is_some(),
            (e.signed_out.is_some() || e.failing.is_some()) && e.fallback.is_none(),
            e.limited.is_some() && e.fallback.is_none(),
            e.fallback.is_some(),
            !is_live(e, now),
            busyness,
            e.handling,
            &e.ant,
        )
    })
}

async fn put(kv: &kv::Store, entry: &ColonyEntry) -> Result<()> {
    kv.put(entry.key(), serde_json::to_vec(entry)?.into()).await?;
    Ok(())
}

/// Host name of this machine, as `hostname` prints it.
pub async fn machine() -> String {
    let output = tokio::process::Command::new("hostname").output().await;
    match output {
        Ok(out) if out.status.success() => {
            let name = String::from_utf8_lossy(&out.stdout).trim().to_owned();
            if !name.is_empty() {
                return name;
            }
        }
        _ => {}
    }
    std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")).unwrap_or_else(|_| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ant: &str, capabilities: &[&str]) -> ColonyEntry {
        entry_on(ant, "m", capabilities)
    }

    fn entry_on(ant: &str, machine: &str, capabilities: &[&str]) -> ColonyEntry {
        let now = Utc::now();
        ColonyEntry {
            ant: ant.parse().unwrap(),
            machine: machine.to_owned(),
            instance: format!("{ant}@{machine}"),
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
            whoami: None,
            profile: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: Activity::Idle,
            handling: 0,
            harnesses: 0,
            escalate_to: Vec::new(),
            sage: None,
            version: ANTS_VERSION.to_owned(),
            perms: PERMISSIONS_VERSION,
            signed_out: None,
            limited: None,
            failing: None,
            fallback: None,
            memory_low: None,
            model: None,
            paused: None,
            drift: Vec::new(),
            mcp: false,
        }
    }

    #[tokio::test]
    async fn the_entry_names_the_sage_the_ant_s_config_and_shared_settings_resolve() {
        use crate::config::{Ant, Sage};
        use crate::profile::shared::Roles;

        let harnesses = crate::harness::Harnesses::builtin();
        let sage_of = async |ant: Ant| super::entry(&ant.into_config("nats://unused", &harnesses).unwrap()).await.sage;
        let mut dev_a = Ant::new("dev-a".parse().unwrap(), "/".into());
        dev_a.harness = Some("copilot".to_owned());
        dev_a.machine = Some("m1".to_owned());
        assert_eq!(sage_of(dev_a.clone()).await, Some(UserId::sage()), "the default");
        let shared = Roles { sage: Some(Sage::Ant("lead".parse().unwrap())), ..Roles::default() };
        assert_eq!(sage_of(shared.overlay(dev_a.clone())).await, Some("lead".parse().unwrap()), "shared settings'");
        dev_a.sage = Some(Sage::None);
        assert_eq!(sage_of(dev_a.clone()).await, None, "reviews off");
        dev_a.sage = None;
        dev_a.is_sage = true;
        assert_eq!(sage_of(dev_a).await, None, "a sage has none");
    }

    #[test]
    fn workload_counts_messages_in_hand_and_signals_turns() {
        let workload = Workload::default();
        let mut stamped = entry("dev-a", &[]);
        workload.set_harnesses(2);
        workload.stamp(&mut stamped);
        assert_eq!((stamped.activity, stamped.handling, stamped.harnesses), (Activity::Idle, 0, 2));

        let first = workload.working();
        let second = workload.working();
        workload.stamp(&mut stamped);
        assert_eq!((stamped.activity, stamped.handling), (Activity::Working, 2));
        drop(first);
        assert_eq!(workload.activity(), Activity::Working);
        drop(second);
        assert_eq!(workload.activity(), Activity::Idle);

        // Turning working and turning idle each leave a wakeup, which a later waiter takes at once.
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some());

        let out = SignInNeeded { since: Utc::now(), sign_in: Some("copilot login".to_owned()) };
        workload.set_signed_out(Some(out.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "signing out shows at once");
        workload.set_signed_out(Some(out.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "nothing new");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.signed_out, Some(out));
        workload.set_signed_out(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "signing in shows at once");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.signed_out, None);

        let agent = HarnessAgent { name: "copilot".to_owned(), version: "1.0.3".to_owned() };
        workload.set_agent(Some(agent.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a new agent shows at once");
        workload.set_agent(Some(agent.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "the same agent again");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.agent, Some(agent));

        let now = Utc::now();
        let limited = HarnessLimit { kind: ants_proto::LimitKind::Quota, since: now, until: now };
        workload.set_limited(Some(limited.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a limit shows at once");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.limited, Some(limited));
        workload.set_limited(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "its end too");

        let fallback = ServingFallback { name: "claude:opus".to_owned(), since: now };
        workload.set_fallback(Some(fallback.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a fallback shows at once");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.fallback, Some(fallback));

        let low = MemoryLow { since: now, available: 900 << 20, max_harnesses: 1 };
        workload.set_memory_low(Some(low.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "low memory shows at once");
        let lower = MemoryLow { available: 800 << 20, ..low };
        workload.set_memory_low(Some(lower.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "a new reading waits a beat");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.memory_low, Some(lower));
        workload.set_memory_low(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "recovering too");

        let profile = Some("Good for Rust daemons.".to_owned());
        workload.set_profile(profile.clone());
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a profile shows at once");
        workload.set_profile(profile.clone());
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "the same profile again");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.profile, profile);
        workload.set_profile(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "its drop too");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.profile, None);
    }

    #[test]
    fn pick_is_the_first_other_ant_with_the_whole_tag() {
        let entries = [
            entry("ops", &["rust-ops"]),
            entry("dev-c", &[" Rust "]),
            entry("dev-b", &["rust"]),
            entry("dev-a", &["rust"]),
        ];
        let me: UserId = "dev-a".parse().unwrap();
        let now = Utc::now();
        let picked = |capability| pick(&entries, capability, &me, now).map(|e| e.ant.as_str());
        assert_eq!(picked("rust"), Some("dev-b"));
        assert_eq!(picked("RUST "), Some("dev-b"));
        assert_eq!(picked("rust-ops"), Some("ops"));
        assert_eq!(picked("ops"), None, "tags match whole, not in part");
        assert_eq!(pick(&entries, "rust", &"dev-b".parse().unwrap(), now).map(|e| e.ant.as_str()), Some("dev-a"));
    }

    #[test]
    fn pick_prefers_live_then_idle_then_least_loaded_ants() {
        let now = Utc::now();
        let with = |ant, activity, handling, beat_secs_ago| ColonyEntry {
            activity,
            handling,
            heartbeat_at: now - chrono::Duration::seconds(beat_secs_ago),
            ..entry(ant, &["rust"])
        };
        let me: UserId = "me".parse().unwrap();
        let picked = |entries: &[ColonyEntry]| pick(entries, "rust", &me, now).map(|e| e.ant.to_string());
        let (idle, working, unknown) = (Activity::Idle, Activity::Working, Activity::Unknown);

        let mut entries = vec![
            with("dev-a", working, 1, 0),
            with("dev-b", unknown, 0, 0),
            with("dev-c", working, 3, 0),
            with("dev-d", idle, 0, 0),
            with("dev-e", idle, 0, 0),
            with("dev-0", idle, 0, 25),
            with("me", idle, 0, 0),
        ];
        assert_eq!(picked(&entries).as_deref(), Some("dev-d"), "idle and live, ties by name, never the caller");
        entries.retain(|e| !["dev-d", "dev-e"].contains(&e.ant.as_str()));
        assert_eq!(picked(&entries).as_deref(), Some("dev-b"), "a newer daemon's unknown activity before working");
        entries.remove(1);
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "fewest messages in hand");
        entries[0].handling = 5;
        assert_eq!(picked(&entries).as_deref(), Some("dev-c"));
        entries.retain(|e| e.ant.as_str() != "dev-c");
        entries[0].heartbeat_at = now - chrono::Duration::seconds(21);
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a stale entry still takes requests when all are stale");
        entries[0].heartbeat_at = now + chrono::Duration::seconds(5);
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "a heartbeat ahead of this clock counts as live");
        entries[0].signed_out = Some(SignInNeeded { since: now, sign_in: None });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a signed-in ant, even stale, before a signed-out one");
        let until = now + chrono::Duration::seconds(60);
        entries[1].limited = Some(HarnessLimit { kind: ants_proto::LimitKind::RateLimit, since: now, until });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a limited ant before a signed-out one");
        entries[0].signed_out = None;
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "an ant held back by no limit before a limited one");
        entries[0].failing = Some(HarnessFailing { since: now, error: "not found".to_owned() });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a limited ant before one failing to start");
        entries[0].fallback = Some(ServingFallback { name: "claude".to_owned(), since: now });
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "an ant a fallback answers for before a limited one");
        entries[1].limited = None;
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "its own harness before a fallback");
        entries[0].fallback = None;
        entries[0].failing = None;
        entries[0].paused = Some(ants_proto::Paused { since: now, by: None });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "an ant not paused, even stale, before a paused one");
        entries[1].signed_out = Some(SignInNeeded { since: now, sign_in: None });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a signed-out ant before a paused one");
    }

    #[test]
    fn starts_unless_another_daemon_lists_the_ant() {
        let run = |machine, instance: &str| ColonyEntry {
            instance: instance.to_owned(),
            ..entry_on("messenger", machine, &[])
        };
        let ours = run("m1", "a");
        assert!(may_start(None, &ours), "nobody serves it");
        assert!(may_start(Some(&ours), &ours), "its own put");
        assert!(!may_start(Some(&run("m1", "b")), &ours), "another daemon on this machine");
        assert!(!may_start(Some(&run("m2", "b")), &ours), "served on another machine");
    }
}
