//! Live harness sessions, one per active thread: capped, stopped when idle, and resumed with session/load.
//!
//! Threads run concurrently, each in its own harness. A harness with a prompt in flight is never stopped, so when the
//! cap is reached and every live harness is busy, a new thread waits for one to finish. While memory is low
//! ([`crate::limits::pressure`]) idle harnesses stop at once and the cap is lowered.
//!
//! A prompt runs on the ant's own harness or on one of its fallbacks ([`crate::limits::fallback`]). A thread whose live
//! harness is another one's moves to a new harness once that one is idle, and a session that last ran in another
//! harness, as stored beside it, is not resumed: the new one starts over from the thread's history.
//!
//! A restart ([`Pool::restart`]) stops the idle harnesses at once and each busy one once its prompts end; the thread's
//! next prompt starts a new harness, which resumes its session.

use std::collections::HashMap;
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
use crate::worktrees::{Branch, Pruned, Worktrees};

/// A thread's stored session and the harness it ran in, so a restarted daemon resumes it only in that harness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stored {
    pub session: SessionId,
    pub harness: String,
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
    /// The fallback it runs, `None` for the ant's own harness.
    fallback: Option<usize>,
    /// Whether it runs a review's prompts ([`Launch::for_review`]).
    review: bool,
    last_used: Instant,
    /// Prompts in flight. A busy harness is never stopped.
    busy: usize,
    /// The pool's [`Pool::restarts`] when the harness started: a restart since stops it once it is idle.
    restarts: u64,
}

enum Slot {
    /// The harness is starting; it already counts against the cap.
    Starting,
    Ready(Live),
}

enum Claim {
    Live(Arc<Session>),
    Start,
    Wait,
}

pub struct Pool {
    launch: Launch,
    /// The fallbacks, by name, in order.
    fallbacks: Vec<(String, Launch)>,
    store: SessionStore,
    max_live: usize,
    idle_after: Duration,
    slots: Mutex<HashMap<ThreadId, Slot>>,
    /// Signalled when a harness becomes free, starts or stops.
    changed: Notify,
    /// The git worktrees of threads, pruned as their sessions expire.
    worktrees: Option<Worktrees>,
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
            slots: Mutex::default(),
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
    pub fn with_memory_pressure(self, settings: pressure::Settings, probe: pressure::Probe) -> Self {
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
        Self { worktrees: Some(worktrees), isolate: true, ..self }
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
        Self { worktrees: Some(worktrees), isolate: false, ..self }
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
        self.worktrees.as_ref().filter(|_| self.isolate)
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

    /// [`Pool::run`] on `fallback`, `None` for the ant's own harness. An error on a fallback carries [`OnFallback`].
    pub async fn run_on(&self, thread: &ThreadId, prompt: Prompt, fallback: Option<usize>) -> Result<Reply> {
        let on = |e: anyhow::Error| match fallback.and_then(|i| Some((i, self.fallbacks.get(i)?))) {
            Some((index, (name, _))) => e.context(OnFallback { index, name: name.clone() }),
            None => e,
        };
        let review = self.launch.policy().reviewing(thread);
        let lease = self.checkout(thread, fallback, review).await.map_err(on)?;
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
            if let Err(e) = self.store.put(thread, Stored { session: after, harness, cloned: false }).await {
                tracing::warn!(%thread, error = format!("{e:#}"), "storing the session of the thread");
            }
        }
        reply
    }

    /// Stop harnesses idle since before `now - idle_after`, or while memory is low every one with no prompt in flight,
    /// least recently used first. Returns how many stopped.
    pub fn evict_idle(&self, now: Instant) -> usize {
        let low = self.check_memory();
        let idle_after = if low { Duration::ZERO } else { self.idle_after };
        let mut slots = self.slots();
        let mut idle: Vec<(ThreadId, Instant)> = slots
            .iter()
            .filter_map(|(thread, slot)| match slot {
                Slot::Ready(live) if live.busy == 0 && now.saturating_duration_since(live.last_used) >= idle_after => {
                    Some((thread.clone(), live.last_used))
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

    /// The thread's live session on `fallback`, a review's if `review`, marked busy until the lease drops.
    async fn checkout(&self, thread: &ThreadId, fallback: Option<usize>, review: bool) -> Result<Lease<'_>> {
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            match self.claim(thread, fallback, review) {
                Claim::Live(session) => {
                    return Ok(Lease { pool: self, thread: thread.clone(), session, rebuilt: false });
                }
                Claim::Start => return self.start(thread, fallback, review).await,
                Claim::Wait => changed.await,
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
                return Claim::Live(live.session.clone());
            }
            // A dead harness with a prompt in flight is about to fail it; take over once it has.
            Some(Slot::Ready(Live { busy: 1.., .. }) | Slot::Starting) => return Claim::Wait,
            // Dead, idle on another harness or for another kind of prompt, or started before a restart.
            Some(Slot::Ready(_)) => {
                slots.remove(thread);
            }
            None => {}
        }
        let cap = self.cap();
        while slots.len() >= cap {
            if !evict_least_recently_used(&mut slots) {
                return Claim::Wait;
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

    /// Start the harness of a thread claimed as [`Slot::Starting`] on `fallback`, for a review if `review`, resuming
    /// its stored session unless that last ran in another harness.
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
        let stored = self.store.get(thread).await?;
        let moved = stored.as_ref().is_some_and(|stored| stored.harness != harness);
        let cloned = stored.as_ref().is_some_and(|stored| stored.cloned);
        let resume = stored.filter(|_| !moved && !cloned).map(|stored| stored.session);
        let mut launch = launch.for_thread(thread);
        if let Some(dir) = self.checkouts.as_ref().and_then(|checkouts| checkouts.workdir(thread)) {
            launch = launch.in_dir(dir);
        } else if let Some(worktrees) = self.worktrees.as_ref().filter(|_| self.isolate) {
            match worktrees.checkout(thread).await {
                Ok(dir) => launch = launch.in_dir(dir),
                Err(e) => tracing::warn!(%thread, error = format!("{e:#}"), "working in the shared workdir"),
            }
        }
        let session = if moved {
            Session::start_over(&launch, Rebuild::Moved).await?
        } else if cloned {
            Session::start_over(&launch, Rebuild::Cloned).await?
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
        let lost = resume.as_ref().is_some_and(|old| *old != session.id());
        if let Some(lifecycle) = self.lifecycle.as_ref().filter(|_| lost) {
            lifecycle.emit(EventKind::SessionLost, Some(thread)).await;
            lifecycle.emit(EventKind::SessionRebuilt { reason: Rebuilt::Lost }, Some(thread)).await;
        }
        // Stored on every start, so the key of a thread still in use does not age out. Losing it only costs the
        // thread its history after the next stop, so the prompt goes ahead.
        if let Err(e) =
            self.store.put(thread, Stored { session: session.id(), harness: harness.to_owned(), cloned: false }).await
        {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing the session of the thread");
        }
        let session = Arc::new(session);
        let live = Live { session: session.clone(), fallback, review, last_used: Instant::now(), busy: 1, restarts };
        self.slots().insert(thread.clone(), Slot::Ready(live));
        starting.done = true;
        Ok(Lease { pool: self, thread: thread.clone(), session, rebuilt: lost })
    }
}

/// Stop the least recently used harness that has no prompt in flight. False when every one is busy.
fn evict_least_recently_used(slots: &mut HashMap<ThreadId, Slot>) -> bool {
    let oldest = slots
        .iter()
        .filter_map(|(thread, slot)| match slot {
            Slot::Ready(live) if live.busy == 0 => Some((thread, live.last_used)),
            _ => None,
        })
        .min_by_key(|(_, last_used)| *last_used)
        .map(|(thread, _)| thread.clone());
    let Some(oldest) = oldest else { return false };
    tracing::info!(thread = %oldest, "stopping harness to stay within the cap");
    slots.remove(&oldest);
    true
}

/// A prompt in flight on a thread's session.
struct Lease<'a> {
    pool: &'a Pool,
    thread: ThreadId,
    session: Arc<Session>,
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
pub(crate) mod fake {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use agent_client_protocol::v1::schema::{
        AgentCapabilities, CancelNotification, ContentBlock, ContentChunk, Cost, ForkSessionRequest,
        ForkSessionResponse, Implementation, InitializeRequest, InitializeResponse, LoadSessionRequest,
        LoadSessionResponse, NewSessionRequest, NewSessionResponse, PromptCapabilities, PromptRequest, PromptResponse,
        SessionCapabilities, SessionForkCapabilities, SessionId, SessionNotification, SessionUpdate, StopReason,
        TextContent, ToolCall, Usage, UsageUpdate,
    };
    use agent_client_protocol::v1::{ConnectionTo, Responder};
    use agent_client_protocol::{Agent, Client, DynConnectTo, Error};
    use tokio::sync::Semaphore;

    use crate::sync::MutexExt as _;

    use crate::harness::{Launch, SteerRequest, SteerResponse, advertise_steering};

    /// What the fake harnesses were asked to do, across all their "processes".
    #[derive(Default, Debug)]
    pub struct Log {
        pub starts: usize,
        pub new_sessions: usize,
        pub loaded: Vec<String>,
        /// Working directories of the sessions created or loaded, in order.
        pub cwds: Vec<PathBuf>,
        /// Prompts received, before any is held or answered.
        pub prompted: usize,
        /// Their texts, in order.
        pub prompts: Vec<String>,
        /// Prompts in the order they were answered.
        pub answered: Vec<String>,
        /// Prompts ended by session/cancel.
        pub cancelled: usize,
        /// Prompts with the word `doomed` that started, then failed.
        pub doomed: usize,
        pub doomed_failed: usize,
        flaky_failed: Vec<String>,
        /// While set, opening a session or prompting fails with ACP's `auth_required`, as a harness that is not
        /// signed in does.
        pub signed_out: bool,
        /// While set, session/load fails, as with a harness whose session store was cleared.
        pub lose_sessions: bool,
        /// While set, prompts stream this text and fail with it, as Claude's adapter reports a provider limit.
        pub limited: Option<String>,
        /// Whether [`Log::limited`] ends the prompt as its reply instead, as Copilot CLI and Antigravity do.
        pub limited_reply: bool,
        /// While set, the handshake fails, as with a broken install.
        pub broken: bool,
        /// While set, harnesses starting advertise that they take images in prompts.
        pub takes_images: bool,
        /// The images prompts carried after their text, as media type and base64 data, in order.
        pub images: Vec<(String, String)>,
        /// While set, harnesses starting advertise session/fork.
        pub forks: bool,
        /// The sessions forked, each with the working directory of its fork, in order; fork `n` is session `f<n>`.
        pub forked: Vec<(String, PathBuf)>,
        /// While set, harnesses starting advertise `_ants/steer`.
        pub steers: bool,
        /// The texts running prompts took as steers, in order.
        pub steered: Vec<String>,
    }

    /// In-process fake harnesses that answer each prompt with `<session>: <text>`. A prompt starting with `hold`, or
    /// with a line starting with `hold:`, waits for a permit from the returned semaphore first; one starting with
    /// `flaky` fails the first time; one with the word `doomed` waits for a permit too, then fails, every time. Words
    /// in a prompt make it report spend and work: `tools N` starts N tool calls, `cost X` reports X as the session's
    /// cost, `tokens N` reports N tokens in the response, and `stall` replies `partial` and waits for session/cancel.
    /// `overflows` fails as a prompt that outgrew the context window, and so does `overflow` unless the session was
    /// rebuilt for it. While [`Log::signed_out`] is set, sessions and prompts fail as with a harness that is not signed
    /// in, and a prompt with the word `sign-out` starts its `tools N`, then sets it and fails so; while
    /// [`Log::limited`] is, prompts start their `tools N`, then stream its text and fail with it, or end with it as
    /// their reply; while [`Log::broken`] is, the handshake fails; while [`Log::takes_images`] is, harnesses starting
    /// take images, which they log; while [`Log::steers`] is, harnesses starting take steers into a running prompt,
    /// whose reply ends with a line `+ <steer>` for each, though they refuse a steer ending with `refused`. They can load stored sessions.
    pub fn launch() -> (Launch, Arc<Mutex<Log>>, Arc<Semaphore>) {
        launch_with(Sessions::Loadable)
    }

    /// Whether fake harnesses can load a stored session, or only start new ones.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum Sessions {
        Loadable,
        NewOnly,
    }

    /// [`launch`], with harnesses that load stored `sessions` or not.
    pub fn launch_with(sessions: Sessions) -> (Launch, Arc<Mutex<Log>>, Arc<Semaphore>) {
        let log = Arc::new(Mutex::new(Log::default()));
        let gate = Arc::new(Semaphore::new(0));
        let (shared, held) = (log.clone(), gate.clone());
        let connect = Arc::new(move || {
            shared.locked().starts += 1;
            let fake = Fake {
                log: shared.clone(),
                gate: held.clone(),
                cancels: Arc::new(Semaphore::new(0)),
                sessions,
                running: Arc::default(),
            };
            let (on_cancel, on_init, on_new, on_load, on_fork, on_steer, on_prompt) =
                (fake.clone(), fake.clone(), fake.clone(), fake.clone(), fake.clone(), fake.clone(), fake);
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("fake")
                    .on_receive_notification(
                        async move |_: CancelNotification, _cx| {
                            on_cancel.cancels.add_permits(1);
                            Ok(())
                        },
                        agent_client_protocol::on_receive_notification!(),
                    )
                    .on_receive_request(
                        async move |init: InitializeRequest, responder, _cx| on_init.initialize(init, responder),
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |new: NewSessionRequest, responder, _cx| on_new.new_session(new, responder),
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |load: LoadSessionRequest, responder, cx| on_load.load_session(load, responder, &cx),
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |fork: ForkSessionRequest, responder, _cx| on_fork.fork_session(fork, responder),
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |steer: SteerRequest, responder, _cx| on_steer.steer(steer, responder),
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |prompt: PromptRequest, responder, cx| on_prompt.prompt(prompt, responder, &cx),
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        (Launch::with_transport(connect, PathBuf::from("/"), Default::default()), log, gate)
    }

    /// One fake harness "process": the shared log and gate, and the permits its session/cancel notifications leave.
    #[derive(Clone)]
    struct Fake {
        log: Arc<Mutex<Log>>,
        gate: Arc<Semaphore>,
        cancels: Arc<Semaphore>,
        sessions: Sessions,
        /// The sessions running a prompt, with the steers it took.
        running: Arc<Mutex<HashMap<SessionId, Vec<String>>>>,
    }

    impl Fake {
        fn initialize(&self, init: InitializeRequest, responder: Responder<InitializeResponse>) -> Result<(), Error> {
            let log = self.log.locked();
            if log.broken {
                return responder.respond_with_internal_error("broken install");
            }
            let prompts = PromptCapabilities::new().image(log.takes_images);
            let fork = log.forks.then(SessionForkCapabilities::new);
            let steers = log.steers;
            drop(log);
            let capabilities = AgentCapabilities::new()
                .load_session(self.sessions == Sessions::Loadable)
                .prompt_capabilities(prompts)
                .session_capabilities(SessionCapabilities::new().fork(fork));
            let capabilities = if steers { advertise_steering(capabilities) } else { capabilities };
            responder.respond(
                InitializeResponse::new(init.protocol_version)
                    .agent_capabilities(capabilities)
                    .agent_info(Implementation::new("fake", "1.0")),
            )
        }

        /// Take a steer into the prompt running in its session, if one is.
        fn steer(&self, steer: SteerRequest, responder: Responder<SteerResponse>) -> Result<(), Error> {
            let ContentBlock::Text(t) = &steer.prompt[0] else { unreachable!() };
            let mut running = self.running.locked();
            let accepted = match running.get_mut(&steer.session_id) {
                Some(_) if t.text.ends_with("refused") => false,
                Some(steers) => {
                    steers.push(t.text.clone());
                    self.log.locked().steered.push(t.text.clone());
                    true
                }
                None => false,
            };
            responder.respond(SteerResponse { accepted })
        }

        fn new_session(&self, new: NewSessionRequest, responder: Responder<NewSessionResponse>) -> Result<(), Error> {
            let mut log = self.log.locked();
            if log.signed_out {
                return responder.respond_with_error(Error::auth_required());
            }
            log.new_sessions += 1;
            log.cwds.push(new.cwd);
            responder.respond(NewSessionResponse::new(format!("s{}", log.new_sessions)))
        }

        fn load_session(
            &self,
            load: LoadSessionRequest,
            responder: Responder<LoadSessionResponse>,
            cx: &ConnectionTo<Client>,
        ) -> Result<(), Error> {
            let mut log = self.log.locked();
            if log.signed_out {
                return responder.respond_with_error(Error::auth_required());
            }
            if log.lose_sessions {
                return responder.respond_with_internal_error("Session not found");
            }
            log.loaded.push(load.session_id.to_string());
            log.cwds.push(load.cwd.clone());
            drop(log);
            // Replayed history must not leak into the next reply.
            cx.send_notification(chunk(&load.session_id, "old history"))?;
            responder.respond(LoadSessionResponse::new())
        }

        fn fork_session(
            &self,
            fork: ForkSessionRequest,
            responder: Responder<ForkSessionResponse>,
        ) -> Result<(), Error> {
            let mut log = self.log.locked();
            log.forked.push((fork.session_id.to_string(), fork.cwd));
            responder.respond(ForkSessionResponse::new(format!("f{}", log.forked.len())))
        }

        fn prompt(
            &self,
            prompt: PromptRequest,
            responder: Responder<PromptResponse>,
            cx: &ConnectionTo<Client>,
        ) -> Result<(), Error> {
            let mut log = self.log.locked();
            if log.signed_out {
                return responder.respond_with_error(Error::auth_required());
            }
            log.prompted += 1;
            let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
            let words: Vec<&str> = t.text.split_whitespace().collect();
            let signs_out = words.contains(&"sign-out");
            if signs_out || log.limited.is_some() {
                // Tools run before the sign-out or the limit is met.
                let tools = words.windows(2).find(|w| w[0] == "tools").and_then(|w| w[1].parse().ok());
                for i in 0..tools.unwrap_or(0u64) {
                    let call = SessionUpdate::ToolCall(ToolCall::new(format!("call-{i}"), "tool"));
                    cx.send_notification(SessionNotification::new(prompt.session_id.clone(), call))?;
                }
            }
            if signs_out {
                log.signed_out = true;
                return responder.respond_with_error(Error::auth_required());
            }
            if let Some(limited) = &log.limited {
                cx.send_notification(chunk(&prompt.session_id, limited))?;
                if log.limited_reply {
                    return responder.respond(PromptResponse::new(StopReason::EndTurn));
                }
                let error = Error::internal_error().data(limited.clone());
                return responder.respond_with_error(error);
            }
            log.prompts.push(t.text.clone());
            for block in &prompt.prompt[1..] {
                let ContentBlock::Image(image) = block else { unreachable!() };
                log.images.push((image.mime_type.clone(), image.data.clone()));
            }
            drop(log);
            self.running.locked().insert(prompt.session_id.clone(), Vec::new());
            let (fake, text, sender) = (self.clone(), t.text.clone(), cx.clone());
            // Answer from a task, so a held prompt does not stall the connection.
            cx.spawn(async move { fake.answer(text, prompt.session_id, responder, sender).await })
        }

        /// Answer a prompt with `text`, after the waits and failures its words ask for.
        async fn answer(
            self,
            text: String,
            session: SessionId,
            responder: Responder<PromptResponse>,
            sender: ConnectionTo<Client>,
        ) -> Result<(), Error> {
            let Fake { log, gate, cancels, running, .. } = self;
            if text.starts_with("hold") || text.lines().any(|line| line.starts_with("hold:")) {
                gate.acquire().await.unwrap().forget();
            }
            let steered = running.locked().remove(&session).unwrap_or_default();
            let text = steered.iter().fold(text, |text, steer| format!("{text}\n+ {steer}"));
            if text.split_whitespace().any(|w| w == "doomed") {
                log.locked().doomed += 1;
                gate.acquire().await.unwrap().forget();
                log.locked().doomed_failed += 1;
                return responder.respond_with_internal_error("doomed");
            }
            if text.starts_with("flaky") {
                let mut log = log.locked();
                if !log.flaky_failed.contains(&text) {
                    log.flaky_failed.push(text);
                    return responder.respond_with_internal_error("flaky");
                }
            }
            let session = &session;
            let words: Vec<&str> = text.split_whitespace().collect();
            let rebuilt = text.contains("[ants] The session of this thread outgrew");
            if words.contains(&"overflows") || words.contains(&"overflow") && !rebuilt {
                return responder.respond_with_internal_error("Prompt is too long");
            }
            // A word followed by a number, so prose such as the redelivery warning is no command.
            let arg = |word: &str| {
                let i = words.iter().position(|w| *w == word)?;
                words.get(i + 1)?.parse::<f64>().ok()
            };
            for i in 0..arg("tools").map_or(0, |n| n as u64) {
                let call = SessionUpdate::ToolCall(ToolCall::new(format!("call-{i}"), "tool"));
                sender.send_notification(SessionNotification::new(session.clone(), call))?;
            }
            if let Some(total) = arg("cost") {
                let cost = Cost::new(total, "USD");
                let usage = SessionUpdate::UsageUpdate(UsageUpdate::new(1, 100).cost(cost));
                sender.send_notification(SessionNotification::new(session.clone(), usage))?;
            }
            if words.contains(&"stall") {
                sender.send_notification(chunk(session, "partial"))?;
                cancels.acquire().await.unwrap().forget();
                log.locked().cancelled += 1;
                return responder.respond(PromptResponse::new(StopReason::Cancelled));
            }
            log.locked().answered.push(text.clone());
            // In two chunks, as harnesses stream a reply.
            sender.send_notification(chunk(session, &format!("{session}: ")))?;
            sender.send_notification(chunk(session, &text))?;
            let tokens = arg("tokens").map(|n| n as u64);
            let usage = tokens.map(|n| Usage::new(n, n, 0));
            responder.respond(PromptResponse::new(StopReason::EndTurn).usage(usage))
        }
    }

    fn chunk(session: &SessionId, text: &str) -> SessionNotification {
        SessionNotification::new(
            session.clone(),
            SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(text)))),
        )
    }
}

#[cfg(test)]
mod tests;
