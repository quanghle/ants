//! Live harness sessions, one per active thread: capped, stopped when idle, and resumed with session/load.
//!
//! Threads run concurrently, each in its own harness. A harness with a prompt in flight is never stopped, so when the
//! cap is reached and every live harness is busy, a new thread waits for one to finish.

use std::collections::HashMap;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::{ThreadId, UserId, session_key};
use anyhow::{Context as _, Result};
use tokio::sync::Notify;

use crate::direct_kv::DirectKv;
use crate::harness::{Launch, Prompt, Reply, Session};
use crate::roster::Workload;
use crate::sync::MutexExt as _;
use crate::worktrees::{Branch, Pruned, Worktrees};

/// Where the session of every thread is kept, so a stopped harness or a restarted daemon resumes it.
#[derive(Clone)]
pub enum SessionStore {
    /// The KV bucket [`ants_proto::SESSIONS_BUCKET`], under the ant's own keys.
    Kv { kv: Box<DirectKv>, ant: UserId },
    /// In this process only, shared by clones.
    Memory(Arc<Mutex<HashMap<ThreadId, SessionId>>>),
}

impl SessionStore {
    pub fn kv(kv: DirectKv, ant: UserId) -> Self {
        Self::Kv { kv: Box::new(kv), ant }
    }

    pub fn memory() -> Self {
        Self::Memory(Arc::default())
    }

    /// The session the thread last ran in, if any.
    pub async fn get(&self, thread: &ThreadId) -> Result<Option<SessionId>> {
        match self {
            Self::Kv { kv, ant } => {
                let value = kv.get(session_key(ant, thread)).await.context("reading the session of the thread")?;
                Ok(value.and_then(|v| String::from_utf8(v).ok()).map(SessionId::new))
            }
            Self::Memory(map) => Ok(map.locked().get(thread).cloned()),
        }
    }

    pub async fn put(&self, thread: &ThreadId, session: &SessionId) -> Result<()> {
        match self {
            Self::Kv { kv, ant } => {
                kv.put(session_key(ant, thread), session.to_string())
                    .await
                    .context("storing the session of the thread")?;
            }
            Self::Memory(map) => {
                map.locked().insert(thread.clone(), session.clone());
            }
        }
        Ok(())
    }
}

struct Live {
    session: Arc<Session>,
    last_used: Instant,
    /// Prompts in flight. A busy harness is never stopped.
    busy: usize,
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
    /// Told the agent each harness starts with, for the roster.
    workload: Option<Arc<Workload>>,
}

impl Pool {
    /// How the pool starts harnesses.
    pub fn launch(&self) -> &Launch {
        &self.launch
    }

    /// At most `max_live` harnesses (at least one); each stops after `idle_after` without a prompt. Sessions are
    /// looked up in `store` when a thread's harness starts, so only live threads are held in memory.
    pub fn new(launch: Launch, store: SessionStore, max_live: usize, idle_after: Duration) -> Self {
        Self {
            launch,
            store,
            max_live: max_live.max(1),
            idle_after,
            slots: Mutex::default(),
            changed: Notify::new(),
            worktrees: None,
            isolate: false,
            workload: None,
        }
    }

    /// Tell `workload` the agent each harness starts with ([`Workload::set_agent`]).
    pub fn reporting_to(self, workload: Arc<Workload>) -> Self {
        Self { workload: Some(workload), ..self }
    }

    /// Each thread works in its own git worktree ([`Worktrees`]).
    pub fn with_worktrees(self, worktrees: Worktrees) -> Self {
        Self { worktrees: Some(worktrees), isolate: true, ..self }
    }

    /// Threads share the workdir, and the worktrees left from when each had its own are pruned
    /// ([`Worktrees::left_behind`]).
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
                    Ok(session) => session.is_none(),
                    Err(e) => {
                        tracing::warn!(%thread, error = format!("{e:#}"), "keeping the thread's git worktree");
                        false
                    }
                }
            })
            .await
    }

    /// The thread's branch, when the thread works in its own worktree and the branch holds commits not in the
    /// repository's `HEAD` ([`Worktrees::ahead`]).
    pub async fn branch_ahead(&self, thread: &ThreadId) -> Option<Branch> {
        self.worktrees.as_ref().filter(|_| self.isolate)?.ahead(thread).await
    }

    /// Run a prompt in the thread's session, starting or resuming its harness when none is live. Waits while the cap
    /// is reached and every live harness is busy.
    pub async fn prompt(&self, thread: &ThreadId, text: &str) -> Result<String> {
        Ok(self.run(thread, Prompt::new(text)).await?.text)
    }

    /// [`Pool::prompt`] with the prompt's own limits, spend reports and stop.
    pub async fn run(&self, thread: &ThreadId, prompt: Prompt) -> Result<Reply> {
        let lease = self.checkout(thread).await?;
        let before = lease.session.id();
        let reply = lease.session.run(prompt).await;
        let after = lease.session.id();
        if after != before {
            tracing::info!(%thread, session = %after, "the harness moved to a new session");
            if let Err(e) = self.store.put(thread, &after).await {
                tracing::warn!(%thread, error = format!("{e:#}"), "storing the session of the thread");
            }
        }
        reply
    }

    /// Stop harnesses idle since before `now - idle_after`. Returns how many stopped.
    pub fn evict_idle(&self, now: Instant) -> usize {
        let mut slots = self.slots();
        let stopped = slots
            .extract_if(|_, slot| match slot {
                Slot::Ready(live) => live.busy == 0 && now.saturating_duration_since(live.last_used) >= self.idle_after,
                Slot::Starting => false,
            })
            .inspect(|(thread, _)| tracing::info!(%thread, "stopping idle harness"))
            .count();
        drop(slots);
        if stopped > 0 {
            self.changed.notify_waiters();
        }
        stopped
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

    /// The thread's live session, marked busy until the lease drops.
    async fn checkout(&self, thread: &ThreadId) -> Result<Lease<'_>> {
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            match self.claim(thread) {
                Claim::Live(session) => return Ok(Lease { pool: self, thread: thread.clone(), session }),
                Claim::Start => return self.start(thread).await,
                Claim::Wait => changed.await,
            }
        }
    }

    fn claim(&self, thread: &ThreadId) -> Claim {
        let mut slots = self.slots();
        match slots.get_mut(thread) {
            Some(Slot::Ready(live)) if live.session.is_alive() => {
                live.busy += 1;
                return Claim::Live(live.session.clone());
            }
            // A dead harness with a prompt in flight is about to fail it; take over once it has.
            Some(Slot::Ready(Live { busy: 1.., .. }) | Slot::Starting) => return Claim::Wait,
            Some(Slot::Ready(_)) => {
                slots.remove(thread);
            }
            None => {}
        }
        if slots.len() >= self.max_live && !evict_least_recently_used(&mut slots) {
            return Claim::Wait;
        }
        slots.insert(thread.clone(), Slot::Starting);
        Claim::Start
    }

    /// Start the harness of a thread claimed as [`Slot::Starting`], resuming its stored session.
    async fn start(&self, thread: &ThreadId) -> Result<Lease<'_>> {
        let mut starting = Starting { pool: self, thread, done: false };
        let resume = self.store.get(thread).await?;
        let mut launch = self.launch.for_thread(thread);
        if let Some(worktrees) = self.worktrees.as_ref().filter(|_| self.isolate) {
            match worktrees.checkout(thread).await {
                Ok(dir) => launch = launch.in_dir(dir),
                Err(e) => tracing::warn!(%thread, error = format!("{e:#}"), "working in the shared workdir"),
            }
        }
        let session = Session::start(&launch, resume.clone()).await?;
        if let Some(workload) = &self.workload {
            workload.set_agent(session.agent().cloned());
        }
        match &resume {
            Some(old) if *old == session.id() => tracing::info!(%thread, session = %old, "resumed harness session"),
            _ => tracing::info!(%thread, session = %session.id(), "started harness session"),
        }
        // Stored on every start, so the key of a thread still in use does not age out. Losing it only costs the
        // thread its history after the next stop, so the prompt goes ahead.
        if let Err(e) = self.store.put(thread, &session.id()).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing the session of the thread");
        }
        let session = Arc::new(session);
        let live = Live { session: session.clone(), last_used: Instant::now(), busy: 1 };
        self.slots().insert(thread.clone(), Slot::Ready(live));
        starting.done = true;
        Ok(Lease { pool: self, thread: thread.clone(), session })
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
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if let Some(Slot::Ready(live)) = self.pool.slots().get_mut(&self.thread)
            && Arc::ptr_eq(&live.session, &self.session)
        {
            live.busy -= 1;
            live.last_used = Instant::now();
        }
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
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use agent_client_protocol::v1::schema::{
        AgentCapabilities, CancelNotification, ContentBlock, ContentChunk, Cost, Implementation, InitializeRequest,
        InitializeResponse, LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
        PromptRequest, PromptResponse, SessionId, SessionNotification, SessionUpdate, StopReason, TextContent,
        ToolCall, Usage, UsageUpdate,
    };
    use agent_client_protocol::{Agent, DynConnectTo};
    use tokio::sync::Semaphore;

    use crate::sync::MutexExt as _;

    use crate::harness::Launch;

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
    }

    /// In-process fake harnesses that answer each prompt with `<session>: <text>`. A prompt starting with `hold`
    /// waits for a permit from the returned semaphore first; one starting with `flaky` fails the first time; one with
    /// the word `doomed` waits for a permit too, then fails, every time. Words in
    /// a prompt make it report spend and work: `tools N` starts N tool calls, `cost X` reports X as the session's
    /// cost, `tokens N` reports N tokens in the response, and `stall` replies `partial` and waits for session/cancel.
    /// `overflows` fails as a prompt that outgrew the context window, and so does `overflow` unless the session was
    /// rebuilt for it. While [`Log::signed_out`] is set, sessions and prompts fail as with a harness that is not signed
    /// in; while [`Log::limited`] is, prompts stream its text and fail with it, or end with it as their reply; while
    /// [`Log::broken`] is, the handshake fails. They can load stored sessions.
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
    #[allow(clippy::too_many_lines)]
    pub fn launch_with(sessions: Sessions) -> (Launch, Arc<Mutex<Log>>, Arc<Semaphore>) {
        let log = Arc::new(Mutex::new(Log::default()));
        let gate = Arc::new(Semaphore::new(0));
        let (shared, held) = (log.clone(), gate.clone());
        let connect = Arc::new(move || {
            let log = shared.clone();
            log.locked().starts += 1;
            let (on_init, on_new, on_load, on_prompt, gate) =
                (log.clone(), log.clone(), log.clone(), log, held.clone());
            let cancels = Arc::new(Semaphore::new(0));
            let on_cancel = cancels.clone();
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("fake")
                    .on_receive_notification(
                        async move |_: CancelNotification, _cx| {
                            on_cancel.add_permits(1);
                            Ok(())
                        },
                        agent_client_protocol::on_receive_notification!(),
                    )
                    .on_receive_request(
                        async move |init: InitializeRequest, responder, _cx| {
                            if on_init.locked().broken {
                                return responder.respond_with_internal_error("broken install");
                            }
                            responder.respond(
                                InitializeResponse::new(init.protocol_version)
                                    .agent_capabilities(
                                        AgentCapabilities::new().load_session(sessions == Sessions::Loadable),
                                    )
                                    .agent_info(Implementation::new("fake", "1.0")),
                            )
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |new: NewSessionRequest, responder, _cx| {
                            let mut log = on_new.locked();
                            if log.signed_out {
                                return responder.respond_with_error(agent_client_protocol::Error::auth_required());
                            }
                            log.new_sessions += 1;
                            log.cwds.push(new.cwd);
                            responder.respond(NewSessionResponse::new(format!("s{}", log.new_sessions)))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |load: LoadSessionRequest, responder, cx| {
                            let mut log = on_load.locked();
                            if log.signed_out {
                                return responder.respond_with_error(agent_client_protocol::Error::auth_required());
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
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |prompt: PromptRequest, responder, cx| {
                            let mut log = on_prompt.locked();
                            if log.signed_out {
                                return responder.respond_with_error(agent_client_protocol::Error::auth_required());
                            }
                            log.prompted += 1;
                            if let Some(limited) = &log.limited {
                                cx.send_notification(chunk(&prompt.session_id, limited))?;
                                if log.limited_reply {
                                    return responder.respond(PromptResponse::new(StopReason::EndTurn));
                                }
                                let error = agent_client_protocol::Error::internal_error().data(limited.clone());
                                return responder.respond_with_error(error);
                            }
                            let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
                            log.prompts.push(t.text.clone());
                            drop(log);
                            let (text, gate, sender, log, cancels) =
                                (t.text.clone(), gate.clone(), cx.clone(), on_prompt.clone(), cancels.clone());
                            // Answer from a task, so a held prompt does not stall the connection.
                            cx.spawn(async move {
                                if text.starts_with("hold") {
                                    gate.acquire().await.unwrap().forget();
                                }
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
                                let session = &prompt.session_id;
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
                            })
                        },
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        (Launch::with_transport(connect, PathBuf::from("/"), Default::default()), log, gate)
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
