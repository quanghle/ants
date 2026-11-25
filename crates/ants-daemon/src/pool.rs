//! Live harness sessions, one per active thread: capped, stopped when idle, and resumed with session/load.
//!
//! Threads run concurrently, each in its own harness. A harness with a prompt in flight is never stopped, so when the
//! cap is reached and every live harness is busy, a new thread waits for one to finish. While memory is low
//! (`crate::limits::pressure`) idle harnesses stop at once and the cap is lowered. An idle harness whose last turn left
//! work running in the background ([`Session::background`]), such as a long check its agent waits on, is kept past the
//! idle timeout while that runs, up to [`Pool::keeping_background_for`], and is the last stopped to stay within the cap.
//!
//! With more threads busy than the cap, the idle harness of a thread with more messages in hand
//! ([`Pool::set_in_hand`]) is kept for its next one, up to [`BURST`] prompts back to back and for [`KEEP_FOR`] after
//! each, rather than stopped for another thread's: otherwise nearly every prompt would start a harness and stop one.
//!
//! A prompt runs on the ant's own harness or on one of its fallbacks (`crate::limits::fallback`). A thread whose live
//! harness is another one's moves to a new harness once that one is idle, and a session that last ran in another
//! harness, as stored beside it, is not resumed: the new one starts over from the thread's history.
//!
//! A session stays in the directory it was created in, stored beside it: the ant's workdir, or the thread's git
//! worktree ([`Worktrees`]). A thread whose stored session worked in the workdir stays there when threads get worktrees,
//! as its session started while the repository had no commit or worktrees were off, until that session expires. A
//! stored session whose directory the thread no longer works in, as with worktrees turned off or the workdir changed,
//! is not resumed: a new one starts over from the thread's history, told where the earlier one worked.
//!
//! A restart ([`Pool::restart`]) stops the idle harnesses at once and each busy one once its prompts end; the thread's
//! next prompt starts a new harness, which resumes its session.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::provider::{EventKind, Rebuilt};
use ants_proto::{ThreadId, UserId, session_key};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::checkouts::Checkouts;
use crate::colony::Workload;
use crate::harness::{Launch, Prompt, Reply, Session};
use crate::limits::fallback::OnFallback;
use crate::limits::pressure;
use crate::provider::events::Lifecycle;
use crate::records::direct_kv::DirectKv;
use crate::stream::history::Rebuild;
use crate::sync::MutexExt as _;
use crate::worktrees::{Branch, Pruned, WorktreeFailed, Worktrees};

/// Most prompts a thread with messages in hand runs back to back on its harness while other threads wait for one.
pub const BURST: usize = 8;

/// Longest the idle harness of a thread with messages in hand is kept for its next prompt while other threads wait.
pub const KEEP_FOR: Duration = Duration::from_secs(5);

/// A thread's stored session, the harness it ran in and the directory it was created in, so a restarted daemon
/// resumes it only in that harness and there.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stored {
    pub session: SessionId,
    pub harness: String,
    /// The directory the session was created in: the thread works there for as long as the session is stored.
    pub cwd: PathBuf,
    /// Set by a clone order for a session it could not fork: the clone's first session of the thread starts over,
    /// seeded from the thread's history ([`Rebuild::Cloned`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cloned: bool,
}

/// Where the session of every thread is kept, so a stopped harness or a restarted daemon resumes it.
#[derive(Clone)]
pub enum SessionStore {
    /// The KV bucket [`ants_proto::SESSIONS_BUCKET`], under the ant's own keys, as JSON [`Stored`].
    Kv { kv: Box<DirectKv>, ant: UserId },
    /// In this process only, shared by clones.
    Memory(Arc<Mutex<HashMap<ThreadId, Stored>>>),
}

impl SessionStore {
    pub fn kv(kv: DirectKv, ant: UserId) -> Self {
        Self::Kv { kv: Box::new(kv), ant }
    }

    pub fn memory() -> Self {
        Self::Memory(Arc::default())
    }

    /// The session the thread last ran in, if any, and its harness.
    pub async fn get(&self, thread: &ThreadId) -> Result<Option<Stored>> {
        match self {
            Self::Kv { kv, ant } => {
                let Some(value) =
                    kv.get(session_key(ant, thread)).await.context("reading the session of the thread")?
                else {
                    return Ok(None);
                };
                serde_json::from_slice(&value).context("reading the session of the thread").map(Some)
            }
            Self::Memory(map) => Ok(map.locked().get(thread).cloned()),
        }
    }

    /// [`SessionStore::get`] for starting the thread's harness, with whether a record was stored that this version
    /// cannot read (one an earlier version wrote): the thread then starts over from its history and stores its new
    /// session over the record, instead of failing every message of the thread. Pruning keeps reading such a record
    /// as an error, which keeps the thread's worktree.
    async fn get_to_start(&self, thread: &ThreadId) -> Result<(Option<Stored>, bool)> {
        to_start(thread, self.get(thread).await)
    }

    /// Forget the thread's session, as a failed clone order does with those it stored.
    pub async fn delete(&self, thread: &ThreadId) -> Result<()> {
        match self {
            Self::Kv { kv, ant } => kv.delete(session_key(ant, thread)).await,
            Self::Memory(map) => {
                map.locked().remove(thread);
                Ok(())
            }
        }
    }

    pub async fn put(&self, thread: &ThreadId, stored: Stored) -> Result<()> {
        match self {
            Self::Kv { kv, ant } => {
                kv.put(session_key(ant, thread), serde_json::to_vec(&stored)?)
                    .await
                    .context("storing the session of the thread")?;
            }
            Self::Memory(map) => {
                map.locked().insert(thread.clone(), stored);
            }
        }
        Ok(())
    }
}

struct Live {
    session: Arc<Session>,
    /// The directory its harness works in.
    cwd: PathBuf,
    /// The fallback it runs, `None` for the ant's own harness.
    fallback: Option<usize>,
    /// Whether it runs a review's prompts ([`Launch::for_review`]).
    review: bool,
    last_used: Instant,
    /// Prompts in flight. A busy harness is never stopped.
    busy: usize,
    /// The pool's [`Pool::restarts`] when the harness started: a restart since stops it once it is idle.
    restarts: u64,
    /// Prompts it ran since it started or its thread last had no message in hand.
    turns: usize,
    /// Whether it is idle past the idle timeout, kept while its last turn's background work runs.
    kept: bool,
}

enum Slot {
    /// The harness is starting; it already counts against the cap.
    Starting,
    Ready(Live),
}

enum Claim {
    /// The session, and the directory its harness works in.
    Live(Arc<Session>, PathBuf),
    Start,
    /// Until a harness frees, starts or stops, or until the time given, when an idle one is no longer kept.
    Wait(Option<Instant>),
}

pub struct Pool {
    launch: Launch,
    /// The fallbacks, by name, in order.
    fallbacks: Vec<(String, Launch)>,
    store: SessionStore,
    max_live: usize,
    idle_after: Duration,
    /// How long after its last prompt an idle harness is kept at most while its last turn's background work runs;
    /// None for as long as that runs.
    background_for: Option<Duration>,
    slots: Mutex<HashMap<ThreadId, Slot>>,
    /// The threads with messages in hand ([`Pool::set_in_hand`]). Locked after [`Pool::slots`], never before.
    in_hand: Mutex<HashSet<ThreadId>>,
    /// Signalled when a harness becomes free, starts or stops.
    changed: Notify,
    /// The git worktrees of threads, pruned as their sessions expire.
    worktrees: Option<Arc<Worktrees>>,
    /// Each thread works in its worktree, instead of the shared workdir.
    isolate: bool,
    /// The checkouts of reviews: a thread with some works among them.
    checkouts: Option<Checkouts>,
    /// Told the agent each harness starts with, and whether memory is low, for the colony.
    workload: Option<Arc<Workload>>,
    /// Watches for low memory, with the cap while it is low.
    pressure: Option<(Mutex<pressure::Gauge>, usize)>,
    /// Whether memory was low at the latest check.
    memory_low: AtomicBool,
    /// How many restarts were asked for ([`Pool::restart`]).
    restarts: AtomicU64,
    /// Told of the sessions lost and rebuilt, and of the threads whose session expired.
    lifecycle: Option<Lifecycle>,
}

/// What a [`Pool::restart`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Restarting {
    /// The idle harnesses stopped at once.
    pub stopped: usize,
    /// The threads whose harness is busy or starting, which stops once its prompts end.
    pub working: Vec<ThreadId>,
}

impl Pool {
    /// How the pool starts harnesses.
    pub fn launch(&self) -> &Launch {
        &self.launch
    }

    /// Where the sessions of threads are kept.
    pub fn store(&self) -> &SessionStore {
        &self.store
    }

    /// At most `max_live` harnesses (at least one); each stops after `idle_after` without a prompt. Sessions are
    /// looked up in `store` when a thread's harness starts, so only live threads are held in memory.
    pub fn new(launch: Launch, store: SessionStore, max_live: usize, idle_after: Duration) -> Self {
        Self {
            launch,
            fallbacks: Vec::new(),
            store,
            max_live: max_live.max(1),
            idle_after,
            background_for: Some(crate::harness::DEFAULT_PROMPT_TIMEOUT),
            slots: Mutex::default(),
            in_hand: Mutex::default(),
            changed: Notify::new(),
            worktrees: None,
            isolate: false,
            checkouts: None,
            workload: None,
            pressure: None,
            memory_low: AtomicBool::new(false),
            restarts: AtomicU64::new(0),
            lifecycle: None,
        }
    }

    /// Run prompts on `fallbacks`, by name, when asked to ([`Pool::run_on`]).
    #[must_use]
    pub fn with_fallbacks(self, fallbacks: Vec<(String, Launch)>) -> Self {
        Self { fallbacks, ..self }
    }

    /// Keep an idle harness whose last turn left work running in the background ([`Session::background`]) while that
    /// runs, for up to `limit` after its last prompt, the default prompt timeout unless set; None for as long as it runs.
    #[must_use]
    pub fn keeping_background_for(self, limit: Option<Duration>) -> Self {
        Self { background_for: limit, ..self }
    }

    pub fn fallback_count(&self) -> usize {
        self.fallbacks.len()
    }

    /// The names of the fallbacks, in order.
    pub fn fallbacks(&self) -> Vec<String> {
        self.fallbacks.iter().map(|(name, _)| name.clone()).collect()
    }

    /// While `probe` tells of low memory by `settings`, stop idle harnesses at once and run at most
    /// [`pressure::Settings::max_harnesses`], checking at each [`Pool::evict_idle`]. Nothing when `settings` are off.
    #[must_use]
    pub(crate) fn with_memory_pressure(self, settings: pressure::Settings, probe: pressure::Probe) -> Self {
        if settings.is_off() {
            return self;
        }
        let cap = settings.max_harnesses.clamp(1, self.max_live);
        Self { pressure: Some((Mutex::new(pressure::Gauge::new(settings, probe)), cap)), ..self }
    }

    /// Tell `workload` the agent each harness starts with ([`Workload::set_agent`]).
    #[must_use]
    pub fn reporting_to(self, workload: Arc<Workload>) -> Self {
        Self { workload: Some(workload), ..self }
    }

    /// Tell `lifecycle` of the sessions lost and rebuilt, and of the threads whose worktree goes as their session
    /// expired.
    #[must_use]
    pub fn with_lifecycle(self, lifecycle: Option<Lifecycle>) -> Self {
        Self { lifecycle, ..self }
    }

    /// Each thread works in its own git worktree ([`Worktrees`]).
    #[must_use]
    pub fn with_worktrees(self, worktrees: Worktrees) -> Self {
        Self { worktrees: Some(Arc::new(worktrees)), isolate: true, ..self }
    }

    /// A thread under review works among its checkouts ([`Checkouts::workdir`]), not in its worktree or the workdir.
    #[must_use]
    pub fn with_checkouts(self, checkouts: Checkouts) -> Self {
        Self { checkouts: Some(checkouts), ..self }
    }

    /// The checkouts of reviews.
    pub fn checkouts(&self) -> Option<&Checkouts> {
        self.checkouts.as_ref()
    }

    /// Threads share the workdir, and the worktrees left from when each had its own are pruned
    /// ([`Worktrees::left_behind`]).
    #[must_use]
    pub fn with_worktrees_left_behind(self, worktrees: Worktrees) -> Self {
        Self { worktrees: Some(Arc::new(worktrees)), isolate: false, ..self }
    }

    /// Remove the worktrees of threads with no live harness whose session is no longer in the store, at once and then
    /// every `every`, handing the branches kept for their commits to `kept`. Never returns; pending for good without
    /// worktrees.
    pub async fn prune_worktrees_every(&self, every: Duration, kept: impl AsyncFn(Vec<Branch>)) {
        if self.worktrees.is_none() {
            return std::future::pending().await;
        }
        let mut tick = tokio::time::interval(every);
        loop {
            tick.tick().await;
            let pruned = self.prune_worktrees().await;
            if !pruned.kept.is_empty() {
                kept(pruned.kept).await;
            }
        }
    }

    /// Remove the worktrees of threads with no live harness whose session is no longer in the store. A store that
    /// cannot be read removes none.
    pub async fn prune_worktrees(&self) -> Pruned {
        let Some(worktrees) = &self.worktrees else { return Pruned::default() };
        worktrees
            .prune(async |thread: &ThreadId| {
                let live = self.slots().contains_key(thread);
                if live {
                    return false;
                }
                match self.store.get(thread).await {
                    Ok(Some(_)) => false,
                    Ok(None) => {
                        if let Some(lifecycle) = &self.lifecycle {
                            lifecycle.emit(EventKind::ThreadExpired, Some(thread)).await;
                        }
                        true
                    }
                    Err(e) => {
                        tracing::warn!(%thread, error = format!("{e:#}"), "keeping the thread's git worktree");
                        false
                    }
                }
            })
            .await
    }

    /// Whether the thread has a session: a live harness or one stored. A store that cannot be read counts as one.
    pub async fn has_session(&self, thread: &ThreadId) -> bool {
        self.slots().contains_key(thread) || !matches!(self.store.get(thread).await, Ok(None))
    }

    /// The thread's branch, when the thread works in its own worktree and the branch holds commits not in the
    /// repository's `HEAD` ([`Worktrees::ahead`]).
    pub async fn branch_ahead(&self, thread: &ThreadId) -> Option<Branch> {
        self.worktrees()?.ahead(thread).await
    }

    /// The worktrees threads work in, if each works in its own.
    pub fn worktrees(&self) -> Option<&Worktrees> {
        self.worktrees.as_deref().filter(|_| self.isolate)
    }

    /// [`Pool::worktrees`], shared.
    pub fn shared_worktrees(&self) -> Option<Arc<Worktrees>> {
        self.worktrees.clone().filter(|_| self.isolate)
    }

    /// Run a prompt in the thread's session, starting or resuming its harness when none is live. Waits while the cap
    /// is reached and every live harness is busy.
    pub async fn prompt(&self, thread: &ThreadId, text: &str) -> Result<String> {
        Ok(self.run(thread, Prompt::new(text)).await?.text)
    }

    /// [`Pool::prompt`] with the prompt's own limits, spend reports and stop.
    pub async fn run(&self, thread: &ThreadId, prompt: Prompt) -> Result<Reply> {
        self.run_on(thread, prompt, None).await
    }

    /// [`Pool::run`] on `fallback`, `None` for the ant's own harness. An error on a fallback carries `OnFallback`.
    pub async fn run_on(&self, thread: &ThreadId, mut prompt: Prompt, fallback: Option<usize>) -> Result<Reply> {
        let on = |e: anyhow::Error| match fallback.and_then(|i| Some((i, self.fallbacks.get(i)?))) {
            Some((index, (name, _))) => e.context(OnFallback { index, name: name.clone() }),
            None => e,
        };
        let review = self.launch.policy().reviewing(thread);
        let began = prompt.began.take();
        let lease = self.checkout(thread, fallback, review, began).await.map_err(on)?;
        let before = lease.session.id();
        let mut reply = lease.session.run(prompt).await.map_err(on);
        let after = lease.session.id();
        if let Ok(reply) = &mut reply {
            reply.rebuilt = lease.rebuilt || after != before;
        }
        if after != before {
            tracing::info!(%thread, session = %after, "the harness moved to a new session");
            if let Some(lifecycle) = &self.lifecycle {
                lifecycle.emit(EventKind::SessionRebuilt { reason: Rebuilt::Overflow }, Some(thread)).await;
            }
            let harness = self.launch_on(fallback)?.harness_name().to_owned();
            let stored = Stored { session: after, harness, cwd: lease.cwd.clone(), cloned: false };
            if let Err(e) = self.store.put(thread, stored).await {
                tracing::warn!(%thread, error = format!("{e:#}"), "storing the session of the thread");
            }
        }
        reply
    }

    /// Stop harnesses idle since before `now - idle_after`, or while memory is low every one with no prompt in flight,
    /// least recently used first. One whose last turn left work running in the background is kept while that runs, up
    /// to [`Pool::keeping_background_for`] after its last prompt, unless memory is low. Returns how many stopped.
    pub fn evict_idle(&self, now: Instant) -> usize {
        let low = self.check_memory();
        let idle_after = if low { Duration::ZERO } else { self.idle_after };
        let mut slots = self.slots();
        let mut idle: Vec<(ThreadId, Instant)> = slots
            .iter_mut()
            .filter_map(|(thread, slot)| match slot {
                Slot::Ready(live) if live.busy == 0 && now.saturating_duration_since(live.last_used) >= idle_after => {
                    (low || !self.keeps_for_background(thread, live, now)).then(|| (thread.clone(), live.last_used))
                }
                _ => None,
            })
            .collect();
        idle.sort_by_key(|(_, last_used)| *last_used);
        for (thread, _) in &idle {
            slots.remove(thread);
            if low {
                tracing::info!(%thread, "stopping idle harness: memory is low");
            } else {
                tracing::info!(%thread, "stopping idle harness");
            }
        }
        drop(slots);
        if !idle.is_empty() {
            self.changed.notify_waiters();
        }
        idle.len()
    }

    /// Whether `live`, the idle harness of `thread`, is kept at `now` for the work its last turn left running in the
    /// background: while that runs, up to [`Pool::keeping_background_for`] after its last prompt. Logs as it starts
    /// and stops being kept.
    fn keeps_for_background(&self, thread: &ThreadId, live: &mut Live, now: Instant) -> bool {
        let idle_for = now.saturating_duration_since(live.last_used);
        if let Some(limit) = self.background_for.filter(|limit| idle_for >= *limit) {
            if live.kept {
                tracing::info!(%thread, ?limit, "the background work of the idle harness ran past the limit");
            }
            return false;
        }
        let left = live.session.background();
        if left.is_empty() {
            if live.kept {
                tracing::info!(%thread, "the background work of the idle harness ended");
            }
            return false;
        }
        if !live.kept {
            live.kept = true;
            let processes: Vec<String> = left.iter().map(|left| format!("{} {}", left.pid, left.command)).collect();
            tracing::info!(
                %thread,
                processes = processes.join("; "),
                "keeping the idle harness while its last turn's background work runs"
            );
        }
        true
    }

    /// Tell which threads have messages in hand, waiting for a prompt or about to run one: the idle harness of such a
    /// thread is kept for its next prompt ([`BURST`], [`KEEP_FOR`]) rather than stopped for another thread's.
    pub fn set_in_hand(&self, threads: HashSet<ThreadId>) {
        let mut slots = self.slots();
        let mut in_hand = self.in_hand.locked();
        let released = in_hand.iter().any(|thread| !threads.contains(thread));
        for (thread, slot) in slots.iter_mut() {
            if let Slot::Ready(live) = slot
                && !threads.contains(thread)
            {
                live.turns = 0;
            }
        }
        *in_hand = threads;
        drop(in_hand);
        drop(slots);
        if released {
            self.changed.notify_waiters();
        }
    }

    /// Read the memory, if watched: whether it is low, which the colony is told. Recovering wakes the threads waiting
    /// for a harness under the lower cap.
    fn check_memory(&self) -> bool {
        let Some((gauge, cap)) = &self.pressure else { return false };
        let low = gauge.locked().check(chrono::Utc::now(), *cap);
        if let Some(workload) = &self.workload {
            let max_harnesses = u32::try_from(*cap).unwrap_or(u32::MAX);
            workload.set_memory_low(low.map(|low| ants_proto::MemoryLow {
                since: low.since,
                available: low.available,
                max_harnesses,
            }));
        }
        let was = self.memory_low.swap(low.is_some(), Ordering::Relaxed);
        if was && low.is_none() {
            self.changed.notify_waiters();
        }
        low.is_some()
    }

    /// The most harnesses alive at once now: fewer while memory is low.
    fn cap(&self) -> usize {
        match &self.pressure {
            Some((_, low_cap)) if self.memory_low.load(Ordering::Relaxed) => *low_cap,
            _ => self.max_live,
        }
    }

    /// Stop the idle harnesses at once, and each busy or starting one once its prompts end. A thread's next prompt
    /// starts a new harness, resuming its session, rather than run on one started before.
    pub fn restart(&self) -> Restarting {
        let mut slots = self.slots();
        self.restarts.fetch_add(1, Ordering::Relaxed);
        let idle: Vec<ThreadId> = slots
            .iter()
            .filter(|(_, slot)| matches!(slot, Slot::Ready(live) if live.busy == 0))
            .map(|(thread, _)| thread.clone())
            .collect();
        for thread in &idle {
            slots.remove(thread);
            tracing::info!(%thread, "stopping idle harness: restart");
        }
        let mut working: Vec<ThreadId> = slots.keys().cloned().collect();
        working.sort();
        drop(slots);
        for thread in &working {
            tracing::info!(%thread, "stopping harness once its prompt ends: restart");
        }
        self.changed.notify_waiters();
        Restarting { stopped: idle.len(), working }
    }

    /// Stop every harness and wait for them to exit. Prompts still in flight end with an error.
    pub async fn shutdown(self) {
        let slots = self.slots.into_inner().unwrap_or_else(PoisonError::into_inner);
        let sessions = slots.into_values().filter_map(|slot| match slot {
            Slot::Ready(live) => Arc::into_inner(live.session),
            Slot::Starting => None,
        });
        futures::future::join_all(sessions.map(Session::close)).await;
    }

    pub fn live(&self) -> usize {
        self.slots().values().filter(|slot| matches!(slot, Slot::Ready(_))).count()
    }

    fn slots(&self) -> MutexGuard<'_, HashMap<ThreadId, Slot>> {
        self.slots.locked()
    }

    /// The thread's live session on `fallback`, a review's if `review`, marked busy until the lease drops. Tells
    /// `began` when the wait for it is over, before a harness starts.
    async fn checkout(
        &self,
        thread: &ThreadId,
        fallback: Option<usize>,
        review: bool,
        began: Option<tokio::sync::oneshot::Sender<tokio::time::Instant>>,
    ) -> Result<Lease<'_>> {
        let tell = |began: Option<tokio::sync::oneshot::Sender<_>>| {
            if let Some(began) = began {
                let _ = began.send(tokio::time::Instant::now());
            }
        };
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            match self.claim(thread, fallback, review) {
                Claim::Live(session, cwd) => {
                    tell(began);
                    return Ok(Lease { pool: self, thread: thread.clone(), session, cwd, rebuilt: false });
                }
                Claim::Start => {
                    tell(began);
                    return self.start(thread, fallback, review).await;
                }
                Claim::Wait(None) => changed.await,
                Claim::Wait(Some(until)) => {
                    let _ = tokio::time::timeout_at(tokio::time::Instant::from_std(until), changed).await;
                }
            }
        }
    }

    fn claim(&self, thread: &ThreadId, fallback: Option<usize>, review: bool) -> Claim {
        let mut slots = self.slots();
        let restarts = self.restarts.load(Ordering::Relaxed);
        match slots.get_mut(thread) {
            Some(Slot::Ready(live))
                if live.session.is_alive()
                    && live.fallback == fallback
                    && live.review == review
                    && live.restarts == restarts =>
            {
                live.busy += 1;
                live.turns += 1;
                live.kept = false;
                return Claim::Live(live.session.clone(), live.cwd.clone());
            }
            // A dead harness with a prompt in flight is about to fail it; take over once it has.
            Some(Slot::Ready(Live { busy: 1.., .. }) | Slot::Starting) => return Claim::Wait(None),
            // Dead, idle on another harness or for another kind of prompt, or started before a restart.
            Some(Slot::Ready(_)) => {
                slots.remove(thread);
            }
            None => {}
        }
        let cap = self.cap();
        let now = Instant::now();
        while slots.len() >= cap {
            if let Err(until) = evict_least_recently_used(&mut slots, &self.in_hand.locked(), now) {
                return Claim::Wait(until);
            }
        }
        slots.insert(thread.clone(), Slot::Starting);
        Claim::Start
    }

    /// The launch of `fallback`, `None` for the ant's own harness.
    fn launch_on(&self, fallback: Option<usize>) -> Result<&Launch> {
        match fallback {
            Some(i) => Ok(&self.fallbacks.get(i).with_context(|| format!("no fallback {i}"))?.1),
            None => Ok(&self.launch),
        }
    }

    /// The directory the thread's session works in, and the one its stored session was created in, `was`, when that is
    /// another: its review's checkouts while it has some, then `was` when that is `workdir`, even when threads get
    /// worktrees, then the thread's worktree when threads get one, else `workdir`.
    async fn directory(
        &self,
        thread: &ThreadId,
        workdir: &Path,
        was: Option<&Path>,
    ) -> Result<(PathBuf, Option<PathBuf>)> {
        if let Some(dir) = self.checkouts.as_ref().and_then(|checkouts| checkouts.workdir(thread)) {
            return Ok((dir, None));
        }
        // A session created among a review's checkouts, since removed, goes on wherever the thread works.
        let reviewed = self.checkouts.as_ref().map(|checkouts| checkouts.review_dir(thread));
        let was = was.filter(|was| reviewed.as_deref() != Some(*was));
        let stays = was.is_some_and(|was| same_dir(was, workdir));
        let dir = match self.worktrees() {
            Some(_) if stays => {
                tracing::info!(%thread, workdir = %workdir.display(), "the thread stays in the shared workdir");
                workdir.to_owned()
            }
            // Never in the workdir other threads share, which the worktree is there to keep them out of.
            Some(worktrees) => match worktrees.checkout(thread).await {
                Ok(dir) => dir,
                Err(e) => {
                    tracing::warn!(%thread, error = format!("{e:#}"), "the thread's git worktree failed; its message waits");
                    return Err(e.context(WorktreeFailed));
                }
            },
            None => workdir.to_owned(),
        };
        let left = was.filter(|was| !same_dir(was, &dir)).map(Path::to_owned);
        Ok((dir, left))
    }

    /// Start the harness of a thread claimed as [`Slot::Starting`] on `fallback`, for a review if `review`, resuming
    /// its stored session unless that last ran in another harness or in a directory the thread no longer works in
    /// ([`Pool::directory`]).
    async fn start(&self, thread: &ThreadId, fallback: Option<usize>, review: bool) -> Result<Lease<'_>> {
        let mut starting = Starting { pool: self, thread, done: false };
        let restarts = self.restarts.load(Ordering::Relaxed);
        let launch = self.launch_on(fallback)?;
        let reviewing;
        let launch = if review {
            reviewing = launch
                .for_review()
                .with_context(|| format!("not reviewing: {}", launch.unheld_review().unwrap_or_default()))?;
            &reviewing
        } else {
            launch
        };
        let harness = launch.harness_name();
        let (stored, unreadable) = self.store.get_to_start(thread).await?;
        let moved = stored.as_ref().is_some_and(|stored| stored.harness != harness);
        let cloned = stored.as_ref().is_some_and(|stored| stored.cloned);
        let was = stored.as_ref().map(|stored| stored.cwd.as_path());
        let (cwd, left) = self.directory(thread, launch.cwd(), was).await?;
        let resume =
            stored.as_ref().filter(|_| !moved && !cloned && left.is_none()).map(|stored| stored.session.clone());
        let launch = launch.for_thread(thread).in_dir(cwd.clone());
        let session = if unreadable {
            Session::start_over(&launch, Rebuild::Lost).await?
        } else if moved {
            Session::start_over(&launch, Rebuild::Moved).await?
        } else if cloned {
            Session::start_over(&launch, Rebuild::Cloned).await?
        } else if let Some(left) = left {
            tracing::info!(%thread, left = %left.display(), cwd = %cwd.display(), "the session starts over in another directory");
            Session::start_over(&launch, Rebuild::Relocated(left)).await?
        } else {
            Session::start(&launch, resume.clone()).await?
        };
        if let Some(workload) = self.workload.as_ref().filter(|_| fallback.is_none()) {
            workload.set_agent(session.agent().cloned());
        }
        match &resume {
            Some(old) if *old == session.id() => tracing::info!(%thread, session = %old, "resumed harness session"),
            _ => tracing::info!(%thread, session = %session.id(), "started harness session"),
        }
        let lost = unreadable || resume.as_ref().is_some_and(|old| *old != session.id());
        if let Some(lifecycle) = self.lifecycle.as_ref().filter(|_| lost) {
            lifecycle.emit(EventKind::SessionLost, Some(thread)).await;
            lifecycle.emit(EventKind::SessionRebuilt { reason: Rebuilt::Lost }, Some(thread)).await;
        }
        // A session resumed among a review's checkouts keeps the directory it was created in.
        let resumed = resume.is_some() && !lost;
        let created_in = stored.filter(|_| resumed).map_or_else(|| cwd.clone(), |stored| stored.cwd);
        // Stored on every start, so the key of a thread still in use does not age out. Losing it only costs the
        // thread its history after the next stop, so the prompt goes ahead.
        let stored = Stored { session: session.id(), harness: harness.to_owned(), cwd: created_in, cloned: false };
        if let Err(e) = self.store.put(thread, stored).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing the session of the thread");
        }
        let session = Arc::new(session);
        let (running, last_used) = (session.clone(), Instant::now());
        let live = Live {
            session: running,
            cwd: cwd.clone(),
            fallback,
            review,
            last_used,
            busy: 1,
            restarts,
            turns: 1,
            kept: false,
        };
        self.slots().insert(thread.clone(), Slot::Ready(live));
        starting.done = true;
        Ok(Lease { pool: self, thread: thread.clone(), session, cwd, rebuilt: lost })
    }
}

/// What [`SessionStore::get`] found, as [`SessionStore::get_to_start`] takes it: a record that does not parse is none,
/// flagged, and any other error stays one.
fn to_start(thread: &ThreadId, got: Result<Option<Stored>>) -> Result<(Option<Stored>, bool)> {
    match got {
        Ok(stored) => Ok((stored, false)),
        Err(e) if e.downcast_ref::<serde_json::Error>().is_some() => {
            tracing::warn!(%thread, error = format!("{e:#}"), "the stored session of the thread is unreadable: starting over");
            Ok((None, true))
        }
        Err(e) => Err(e),
    }
}

/// Whether `a` and `b` are one directory, however spelled.
fn same_dir(a: &Path, b: &Path) -> bool {
    a == b || matches!((a.canonicalize(), b.canonicalize()), (Ok(a), Ok(b)) if a == b)
}

/// Stop the least recently used harness that has no prompt in flight and is not kept for the next prompt of its
/// thread, which has messages `in_hand`: kept for [`BURST`] prompts back to back, each time for [`KEEP_FOR`]. One whose
/// last turn left work running in the background goes only once no other can. When there is none, fails with when the
/// first kept one is no longer kept, if any is.
fn evict_least_recently_used(
    slots: &mut HashMap<ThreadId, Slot>,
    in_hand: &HashSet<ThreadId>,
    now: Instant,
) -> Result<(), Option<Instant>> {
    let mut kept_until: Option<Instant> = None;
    let mut oldest: Option<(&ThreadId, (bool, Instant))> = None;
    for (thread, slot) in slots.iter() {
        let Slot::Ready(live) = slot else { continue };
        if live.busy > 0 {
            continue;
        }
        let until = live.last_used + KEEP_FOR;
        if live.turns < BURST && until > now && in_hand.contains(thread) {
            kept_until = Some(kept_until.map_or(until, |kept| kept.min(until)));
            continue;
        }
        // A harness's background work is read only where it could make the harness the one to stop.
        let older = oldest.is_none_or(|(_, (working, last_used))| working || live.last_used < last_used);
        if older {
            let key = (!live.session.background().is_empty(), live.last_used);
            if oldest.is_none_or(|(_, oldest)| key < oldest) {
                oldest = Some((thread, key));
            }
        }
    }
    let Some((oldest, (working, _))) = oldest else { return Err(kept_until) };
    let oldest = oldest.clone();
    if working {
        tracing::info!(thread = %oldest, "stopping harness to stay within the cap, with its background work");
    } else {
        tracing::info!(thread = %oldest, "stopping harness to stay within the cap");
    }
    slots.remove(&oldest);
    Ok(())
}

/// A prompt in flight on a thread's session.
struct Lease<'a> {
    pool: &'a Pool,
    thread: ThreadId,
    session: Arc<Session>,
    /// The directory its harness works in.
    cwd: PathBuf,
    /// Whether the session was started anew, the harness having lost the stored one.
    rebuilt: bool,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let mut slots = self.pool.slots();
        if let Some(Slot::Ready(live)) = slots.get_mut(&self.thread)
            && Arc::ptr_eq(&live.session, &self.session)
        {
            live.busy -= 1;
            live.last_used = Instant::now();
            if live.busy == 0 && live.restarts != self.pool.restarts.load(Ordering::Relaxed) {
                tracing::info!(thread = %self.thread, "stopping harness after its prompt: restart");
                slots.remove(&self.thread);
            }
        }
        drop(slots);
        self.pool.changed.notify_waiters();
    }
}

/// Frees the slot of a harness that failed to start, or whose start was cancelled.
struct Starting<'a> {
    pool: &'a Pool,
    thread: &'a ThreadId,
    done: bool,
}

impl Drop for Starting<'_> {
    fn drop(&mut self) {
        if !self.done {
            let mut slots = self.pool.slots();
            if matches!(slots.get(self.thread), Some(Slot::Starting)) {
                slots.remove(self.thread);
            }
        }
        self.pool.changed.notify_waiters();
    }
}

#[cfg(test)]
pub(crate) mod fake;

#[cfg(test)]
mod tests;
