//! The persona's entry in the roster, kept alive by heartbeats.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ants_proto::{Activity, PersonaId, ROSTER_HEARTBEAT, ROSTER_TTL, RosterEntry};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, kv};
use chrono::Utc;
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::bus;
use crate::daemon::Config;

/// A persona listed in the roster. Dropping it stops the heartbeats, so the entry expires;
/// [`Registration::deregister`] removes it at once.
pub struct Registration {
    kv: kv::Store,
    key: String,
    heartbeat: JoinHandle<()>,
    /// Of a held entry ([`Registration::hold`]): the revision last put, `None` once another daemon put the entry.
    held: Option<watch::Receiver<Option<u64>>>,
}

/// What the daemon is doing, shared by the inbox loop and the heartbeats: the messages being handled and the live
/// harnesses.
#[derive(Debug, Default)]
pub struct Workload {
    handling: AtomicUsize,
    harnesses: AtomicUsize,
    /// Wakes the heartbeats when the persona turns idle or working, so the roster shows it before the next beat.
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

    pub fn handling(&self) -> usize {
        self.handling.load(Ordering::Relaxed)
    }

    pub fn harnesses(&self) -> usize {
        self.harnesses.load(Ordering::Relaxed)
    }

    pub fn activity(&self) -> Activity {
        if self.handling() == 0 { Activity::Idle } else { Activity::Working }
    }

    /// Write the current workload into `entry`.
    pub fn stamp(&self, entry: &mut RosterEntry) {
        let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        entry.activity = Some(self.activity());
        entry.handling = Some(count(self.handling()));
        entry.harnesses = Some(count(self.harnesses()));
    }
}

/// A message being handled, counted in its [`Workload`] while alive.
pub struct Working<'a>(&'a Workload);

impl Drop for Working<'_> {
    fn drop(&mut self) {
        if self.0.handling.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.changed.notify_one();
        }
    }
}

/// The roster entry of the persona of `config` on this machine, started now.
pub async fn entry(config: &Config) -> RosterEntry {
    let now = Utc::now();
    RosterEntry {
        persona: config.persona.clone(),
        machine: machine().await,
        instance: Some(uuid::Uuid::new_v4().to_string()),
        harness: config.harness.to_string(),
        capabilities: config.capabilities.clone(),
        whoami: config.whoami.clone(),
        workdir: Some(config.workdir.display().to_string()),
        started_at: now,
        heartbeat_at: now,
        activity: None,
        handling: None,
        harnesses: None,
        escalate_to: config.escalation.escalate_to.clone(),
    }
}

/// List `entry` in the roster and keep it fresh, with its `workload`, until the registration ends.
pub async fn register(
    js: &jetstream::Context,
    mut entry: RosterEntry,
    workload: Arc<Workload>,
) -> Result<Registration> {
    let kv = bus::ensure_roster(js, ROSTER_TTL).await?;
    entry.heartbeat_at = Utc::now();
    Registration::start_with(kv, entry, ROSTER_HEARTBEAT, Some(workload)).await
}

/// Whether a daemon may start serving a persona as `ours` without serving it twice, when the persona's roster entry is
/// `current`: only if the roster lists none, or `ours` itself (a put whose answer was lost). An entry stays in the
/// roster until its daemon stops or it goes [`ROSTER_TTL`] without a heartbeat, by the server's clock, so no clocks are
/// compared. Two daemons on one machine are told apart by [`RosterEntry::instance`].
pub fn may_start(current: Option<&RosterEntry>, ours: &RosterEntry) -> bool {
    current.is_none_or(|e| e.instance.is_some() && e.instance == ours.instance)
}

/// What one look at a persona's roster entry came to.
enum Claim {
    /// The entry was put, at this revision.
    Put(u64),
    /// Another daemon's entry is there.
    Held { machine: String, instance: Option<String> },
    /// The entry changed between the look and the put.
    Raced,
}

/// Put `entry` if [`may_start`] allows it, expecting the revision looked at, so of two daemons only one goes ahead.
async fn claim(kv: &kv::Store, entry: &RosterEntry) -> Result<Claim> {
    let key = entry.key();
    let found = kv.entry(key).await.with_context(|| format!("reading {key} in the roster"))?;
    let current = found
        .as_ref()
        .filter(|e| e.operation == kv::Operation::Put)
        .and_then(|e| serde_json::from_slice::<RosterEntry>(&e.value).ok());
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
        Err((false, e)) => Err(e.context(format!("putting {key} in the roster"))),
    }
}

/// Wait until [`may_start`] allows `entry`, looking at the roster every `every`, then put `entry`, started now, and
/// return the revision put, for [`hold`]. The put expects the entry it looked at, so of two daemons starting at once
/// only one goes ahead; the other waits on.
pub async fn wait_for_turn(kv: &kv::Store, entry: &mut RosterEntry, every: Duration) -> Result<u64> {
    let key = entry.key().to_owned();
    let mut ticks = tokio::time::interval(every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut waiting_for = None;
    loop {
        ticks.tick().await;
        let now = Utc::now();
        (entry.started_at, entry.heartbeat_at) = (now, now);
        match claim(kv, entry).await {
            Ok(Claim::Put(revision)) => return Ok(revision),
            Ok(Claim::Held { machine, instance }) => {
                if waiting_for.as_ref() != Some(&instance) {
                    tracing::info!(persona = key, machine, "waiting while another daemon serves it");
                    waiting_for = Some(instance);
                }
            }
            Ok(Claim::Raced) => tracing::info!(persona = key, "roster entry changed; looking again"),
            Err(e) => tracing::warn!(persona = key, error = format!("{e:#}"), "looking at the roster; trying again"),
        }
    }
}

/// List `entry` in the roster over `revision`, which [`wait_for_turn`] put, and keep it fresh with its `workload` as
/// [`register`] does, as long as no other daemon puts the entry: then the registration is lost
/// ([`Registration::lost`]) and puts nothing more.
pub async fn hold(
    js: &jetstream::Context,
    entry: RosterEntry,
    revision: u64,
    workload: Arc<Workload>,
) -> Result<Registration> {
    let kv = bus::ensure_roster(js, ROSTER_TTL).await?;
    Registration::hold(kv, entry, revision, ROSTER_HEARTBEAT, Some(workload)).await
}

/// Put `entry` over `revision`. When another write came in between: put it again if it is gone or ours, and give the
/// revision put, or `None` if another daemon's entry is there.
async fn renew(kv: &kv::Store, entry: &RosterEntry, revision: u64) -> Result<Option<u64>> {
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
async fn due(ticks: &mut tokio::time::Interval, workload: Option<&Workload>, entry: &mut RosterEntry) {
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
            if !beat && entry.activity == Some(workload.activity()) {
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
    mut entry: RosterEntry,
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
                    let ours = serde_json::from_slice::<RosterEntry>(&change.value)
                        .is_ok_and(|e| e.instance.is_some() && e.instance == entry.instance);
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
            Err(e) => tracing::warn!(persona = %entry.persona, error = format!("{e:#}"), "roster heartbeat"),
        }
    }
    tracing::info!(persona = %entry.persona, "another daemon took over the roster entry");
    held.send_replace(None);
}

/// The next change `changes` shows, if any; on an error it stops watching, so only the heartbeats see changes.
async fn next_change(changes: &mut Option<kv::Watch>) -> Option<kv::Entry> {
    let Some(watch) = changes else { return std::future::pending().await };
    match watch.next().await {
        Some(Ok(change)) => Some(change),
        Some(Err(e)) => {
            tracing::warn!(error = %e, "watching the roster entry; the heartbeats still notice another daemon");
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
    pub async fn start(kv: kv::Store, entry: RosterEntry, every: Duration) -> Result<Self> {
        Self::start_with(kv, entry, every, None).await
    }

    /// Like [`Registration::start`], with `workload` stamped on every put, and an extra put as soon as the persona
    /// turns idle or working.
    pub async fn start_with(
        kv: kv::Store,
        mut entry: RosterEntry,
        every: Duration,
        workload: Option<Arc<Workload>>,
    ) -> Result<Self> {
        let key = entry.key().to_owned();
        if let Some(workload) = &workload {
            workload.stamp(&mut entry);
        }
        put(&kv, &entry).await.with_context(|| format!("registering {key} in the roster"))?;
        let beat_kv = kv.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(every);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticks.tick().await;
            loop {
                due(&mut ticks, workload.as_deref(), &mut entry).await;
                if let Err(e) = put(&beat_kv, &entry).await {
                    tracing::warn!(persona = %entry.persona, error = format!("{e:#}"), "roster heartbeat");
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
        mut entry: RosterEntry,
        revision: u64,
        every: Duration,
        workload: Option<Arc<Workload>>,
    ) -> Result<Self> {
        let key = entry.key().to_owned();
        let changes = match kv.watch(&key).await {
            Ok(changes) => Some(changes),
            Err(e) => {
                tracing::warn!(persona = key, error = %e, "watching the roster entry; the heartbeats still notice another daemon");
                None
            }
        };
        if let Some(workload) = &workload {
            workload.stamp(&mut entry);
        }
        entry.heartbeat_at = Utc::now();
        let first = renew(&kv, &entry, revision).await.with_context(|| format!("registering {key} in the roster"))?;
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
            tracing::warn!(persona = %self.key, error = %e, "leaving the roster; the entry expires on its own");
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

/// The entries in the roster, by persona. An entry removed while listing is left out, and a malformed one is skipped
/// with a warning. So is one naming another persona than its key: credentials vouch only for the key.
pub async fn list_entries(kv: &kv::Store) -> Result<Vec<RosterEntry>> {
    let mut keys: Vec<String> = kv.keys().await.context("listing the roster")?.try_collect().await?;
    keys.sort();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(value) = kv.get(&key).await.with_context(|| format!("reading {key} in the roster"))? else { continue };
        match serde_json::from_slice::<RosterEntry>(&value) {
            Ok(entry) => match entry.check_key(&key) {
                Ok(()) => entries.push(entry),
                Err(e) => tracing::warn!(key, error = %e, "skipping a roster entry for another persona"),
            },
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed roster entry"),
        }
    }
    Ok(entries)
}

/// Whether `entry` advertises `capability`. Tags compare whole, ignoring case and surrounding space.
pub fn has_capability(entry: &RosterEntry, capability: &str) -> bool {
    let capability = capability.trim();
    entry.capabilities.iter().any(|c| c.trim().eq_ignore_ascii_case(capability))
}

/// The persona a request needing `capability` goes to: of the entries other than `asking` that advertise it, the
/// first by persona id, so the same roster always gives the same answer.
pub fn pick<'a>(entries: &'a [RosterEntry], capability: &str, asking: &PersonaId) -> Option<&'a RosterEntry> {
    entries
        .iter()
        .filter(|e| &e.persona != asking && has_capability(e, capability))
        .min_by(|a, b| a.persona.cmp(&b.persona))
}

async fn put(kv: &kv::Store, entry: &RosterEntry) -> Result<()> {
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

    fn entry(persona: &str, capabilities: &[&str]) -> RosterEntry {
        entry_on(persona, "m", capabilities)
    }

    fn entry_on(persona: &str, machine: &str, capabilities: &[&str]) -> RosterEntry {
        let now = Utc::now();
        RosterEntry {
            persona: persona.parse().unwrap(),
            machine: machine.to_owned(),
            instance: None,
            harness: "copilot".to_owned(),
            capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
            whoami: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
        }
    }

    #[test]
    fn workload_counts_messages_in_hand_and_signals_turns() {
        let workload = Workload::default();
        let mut stamped = entry("dev-a", &[]);
        workload.set_harnesses(2);
        workload.stamp(&mut stamped);
        assert_eq!((stamped.activity, stamped.handling, stamped.harnesses), (Some(Activity::Idle), Some(0), Some(2)));

        let first = workload.working();
        let second = workload.working();
        workload.stamp(&mut stamped);
        assert_eq!((stamped.activity, stamped.handling), (Some(Activity::Working), Some(2)));
        drop(first);
        assert_eq!(workload.activity(), Activity::Working);
        drop(second);
        assert_eq!(workload.activity(), Activity::Idle);

        // Turning working and turning idle each leave a wakeup, which a later waiter takes at once.
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some());
    }

    #[test]
    fn pick_is_the_first_other_persona_with_the_whole_tag() {
        let entries = [
            entry("ops", &["rust-ops"]),
            entry("dev-c", &[" Rust "]),
            entry("dev-b", &["rust"]),
            entry("dev-a", &["rust"]),
        ];
        let me: PersonaId = "dev-a".parse().unwrap();
        let picked = |capability| pick(&entries, capability, &me).map(|e| e.persona.as_str());
        assert_eq!(picked("rust"), Some("dev-b"));
        assert_eq!(picked("RUST "), Some("dev-b"));
        assert_eq!(picked("rust-ops"), Some("ops"));
        assert_eq!(picked("ops"), None, "tags match whole, not in part");
        assert_eq!(pick(&entries, "rust", &"dev-b".parse().unwrap()).map(|e| e.persona.as_str()), Some("dev-a"));
    }

    #[test]
    fn starts_unless_another_daemon_lists_the_persona() {
        let run = |machine, instance: Option<&str>| RosterEntry {
            instance: instance.map(str::to_owned),
            ..entry_on("messenger", machine, &[])
        };
        let ours = run("m1", Some("a"));
        assert!(may_start(None, &ours), "nobody serves it");
        assert!(may_start(Some(&ours), &ours), "its own put");
        assert!(!may_start(Some(&run("m1", Some("b"))), &ours), "another daemon on this machine");
        assert!(!may_start(Some(&run("m2", Some("b"))), &ours), "served on another machine");
        assert!(!may_start(Some(&run("m1", None)), &ours), "an older daemon on this machine");
    }
}
