//! Listing an ant in the colony: claiming its entry, waiting for a turn at it, and the heartbeats keeping it fresh.

use std::sync::Arc;
use std::time::Duration;

use ants_proto::{COLONY_HEARTBEAT, COLONY_TTL, ColonyEntry};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, kv};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::Workload;
use crate::bus;
use crate::bus::relayed::{self, GuardedKv as _};

/// An ant listed in the colony. Dropping it stops the heartbeats, so the entry expires;
/// [`Registration::deregister`] removes it at once.
pub struct Registration {
    kv: kv::Store,
    key: String,
    heartbeat: JoinHandle<()>,
    /// Of a held entry ([`Registration::hold`]): the revision last put, `None` once another daemon put the entry.
    held: Option<watch::Receiver<Option<u64>>>,
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

/// Keep `entry`, of an ant whose serving failed ([`ColonyEntry::unserved`]), in the colony: put it now and again every
/// `every`, so it neither turns stale nor expires while the daemon waits to serve the ant again. Never returns; a put
/// that fails is warned of and tried at the next beat.
pub async fn stand_unserved(js: &jetstream::Context, mut entry: ColonyEntry, every: Duration) {
    let mut ticks = ticker(every);
    let mut kv = None;
    loop {
        ticks.tick().await;
        if kv.is_none() {
            match bus::ensure_colony(js, COLONY_TTL).await {
                Ok(store) => kv = Some(store),
                Err(e) => beat_failed(&entry, e),
            }
        }
        if let Some(kv) = &kv {
            entry.heartbeat_at = Utc::now();
            if let Err(e) = put(kv, &entry).await {
                beat_failed(&entry, e);
            }
        }
    }
}

/// Remove `entry`, put by [`stand_unserved`], unless the colony holds another entry for its ant now.
pub async fn leave_unserved(js: &jetstream::Context, entry: &ColonyEntry) -> Result<()> {
    let kv = bus::ensure_colony(js, COLONY_TTL).await?;
    let key = entry.key();
    let Some(found) = kv.guarded_entry(key).await.with_context(|| format!("reading {key} in the colony"))? else {
        return Ok(());
    };
    let ours = found.operation == kv::Operation::Put
        && serde_json::from_slice::<ColonyEntry>(&found.value).is_ok_and(|e| e.instance == entry.instance);
    if ours {
        kv.delete_expect_revision(key, Some(found.revision)).await?;
    }
    Ok(())
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
    /// Another daemon's entry is there, put this long ago by the server's clock against this machine's.
    Held { machine: String, instance: String, age: Duration },
    /// The entry changed between the look and the put.
    Raced,
}

/// Put `entry` if [`may_start`] allows it, expecting the revision looked at, so of two daemons only one goes ahead.
async fn claim(kv: &kv::Store, entry: &ColonyEntry) -> Result<Claim> {
    let key = entry.key();
    let found = kv.guarded_entry(key).await.with_context(|| format!("reading {key} in the colony"))?;
    let current = found
        .as_ref()
        .filter(|e| e.operation == kv::Operation::Put)
        .and_then(|e| serde_json::from_slice::<ColonyEntry>(&e.value).ok());
    if let Some(other) = current.filter(|c| !may_start(Some(c), entry)) {
        let put_at = found.as_ref().and_then(|e| i64::try_from(e.created.unix_timestamp_nanos()).ok());
        let age = put_at.map_or(Duration::ZERO, |at| {
            (Utc::now() - DateTime::from_timestamp_nanos(at)).to_std().unwrap_or_default()
        });
        return Ok(Claim::Held { machine: other.machine, instance: other.instance, age });
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

/// How often at most [`wait_for_turn`] looks at the colony when a watch shows the entry's deletion and its puts: only
/// in case the watch misses one.
pub const LOOK_AGAIN: Duration = Duration::from_secs(60);

/// How long after an entry is due to expire [`wait_for_turn`] looks at it, nats-server removing it a little late.
const EXPIRY_SLACK: Duration = Duration::from_millis(250);

/// Wait until [`may_start`] allows `entry`, then put `entry`, started now, and return the revision put, for [`hold`].
/// The put expects the entry it looked at, so of two daemons starting at once only one goes ahead; the other waits on.
/// It looks again as soon as a watch of the entry shows it deleted, and as soon as the entry is due to expire, the
/// bucket's max age after its last put: the bucket leaves no marker of an expired entry ([`crate::hub::colony_config`]).
/// The last put is the one the watch showed last, timed by this machine's clock alone, or, for the entry first looked
/// at, the server's time of the put. Otherwise it looks every [`LOOK_AGAIN`]; every `every` when it has no watch.
pub async fn wait_for_turn(kv: &kv::Store, entry: &mut ColonyEntry, every: Duration) -> Result<u64> {
    let ttl = Some(kv.stream.cached_info().config.max_age).filter(|ttl| !ttl.is_zero()).unwrap_or(COLONY_TTL);
    let key = entry.key().to_owned();
    let mut changes = match kv.guarded_watch(&key).await {
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
        let most = if changes.is_some() { every.max(LOOK_AGAIN) } else { every };
        let mut wait = every;
        match claim(kv, entry).await {
            Ok(Claim::Put(revision)) => return Ok(revision),
            Ok(Claim::Held { machine, instance, age }) => {
                if waiting_for.as_ref() != Some(&instance) {
                    tracing::info!(ant = key, machine, "waiting while another daemon serves it");
                    waiting_for = Some(instance);
                }
                wait = most.min(ttl.saturating_sub(age) + EXPIRY_SLACK);
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
                    Some(change) if change.operation == kv::Operation::Put => {
                        look_again.as_mut().reset(tokio::time::Instant::now() + most.min(ttl + EXPIRY_SLACK));
                    }
                    // Deleted, or the watch ended: look now.
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
                Some(w) => w.changed().await,
                None => std::future::pending().await,
            }
        };
        let beat = tokio::select! {
            _ = ticks.tick() => true,
            () = changed => false,
        };
        if let Some(workload) = workload {
            if !beat && workload.shown_by(entry) {
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
    mut changes: Option<relayed::Watch>,
) {
    let Some(mut revision) = *held.borrow() else { return };
    let mut ticks = ticker(every);
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
            Err(e) => beat_failed(&entry, e),
        }
    }
    tracing::info!(ant = %entry.ant, "another daemon took over the colony entry");
    held.send_replace(None);
}

/// The next change `changes` shows, if any; on an error it stops watching, so only the heartbeats see changes.
async fn next_change(changes: &mut Option<relayed::Watch>) -> Option<kv::Entry> {
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
            let mut ticks = ticker(every);
            ticks.tick().await;
            loop {
                due(&mut ticks, workload.as_deref(), &mut entry).await;
                if let Err(e) = put(&beat_kv, &entry).await {
                    beat_failed(&entry, e);
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
        let changes = match kv.guarded_watch(&key).await {
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

/// A ticker firing now and then every `every`, a late tick delaying the ones after it.
fn ticker(every: Duration) -> tokio::time::Interval {
    let mut ticks = tokio::time::interval(every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticks
}

/// Warn that the heartbeat putting `entry` failed with `e`, naming a full store as such ([`bus::explain_full`]).
fn beat_failed(entry: &ColonyEntry, e: anyhow::Error) {
    let e = bus::explain_full(e, &format!("$KV.{}.{}", ants_proto::COLONY_BUCKET, entry.key()));
    tracing::warn!(ant = %entry.ant, error = format!("{e:#}"), "colony heartbeat");
}

async fn put(kv: &kv::Store, entry: &ColonyEntry) -> Result<()> {
    kv.put(entry.key(), serde_json::to_vec(entry)?.into()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::colony::tests::entry_on;

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
