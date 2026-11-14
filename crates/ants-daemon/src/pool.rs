//! Live harness sessions, one per active thread: capped, stopped when idle, and resumed with session/load.
//!
//! Threads run concurrently, each in its own harness. A harness with a prompt in flight is never stopped, so when the
//! cap is reached and every live harness is busy, a new thread waits for one to finish.

use std::collections::HashMap;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::{ThreadId, UserId, session_key};
use anyhow::{Context as _, Result};
use tokio::sync::Notify;

use crate::direct_kv::DirectKv;
use crate::harness::{Launch, Prompt, Reply, Session};
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
            Self::Memory(map) => Ok(map.lock().expect("session store").get(thread).cloned()),
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
                map.lock().expect("session store").insert(thread.clone(), session.clone());
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
        }
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
        lease.session.run(prompt).await
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
        let slots = self.slots.into_inner().expect("pool");
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
        self.slots.lock().expect("pool")
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
        match &resume {
            Some(old) if old == session.id() => tracing::info!(%thread, session = %old, "resumed harness session"),
            _ => tracing::info!(%thread, session = %session.id(), "started harness session"),
        }
        // Stored on every start, so the key of a thread still in use does not age out. Losing it only costs the
        // thread its history after the next stop, so the prompt goes ahead.
        if let Err(e) = self.store.put(thread, session.id()).await {
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
        AgentCapabilities, CancelNotification, ContentBlock, ContentChunk, Cost, InitializeRequest, InitializeResponse,
        LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse,
        SessionId, SessionNotification, SessionUpdate, StopReason, TextContent, ToolCall, Usage, UsageUpdate,
    };
    use agent_client_protocol::{Agent, DynConnectTo};
    use tokio::sync::Semaphore;

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
        /// While set, prompts fail with this text, as a harness reporting a provider limit does.
        pub limited: Option<String>,
    }

    /// In-process fake harnesses that answer each prompt with `<session>: <text>`. A prompt starting with `hold`
    /// waits for a permit from the returned semaphore first; one starting with `flaky` fails the first time; one with
    /// the word `doomed` waits for a permit too, then fails, every time. Words in
    /// a prompt make it report spend and work: `tools N` starts N tool calls, `cost X` reports X as the session's
    /// cost, `tokens N` reports N tokens in the response, and `stall` replies `partial` and waits for session/cancel.
    /// While [`Log::signed_out`] is set, sessions and prompts fail as with a harness that is not signed in; while
    /// [`Log::limited`] is, prompts fail with its text.
    pub fn launch(can_load: bool) -> (Launch, Arc<Mutex<Log>>, Arc<Semaphore>) {
        let log = Arc::new(Mutex::new(Log::default()));
        let gate = Arc::new(Semaphore::new(0));
        let (shared, held) = (log.clone(), gate.clone());
        let connect = Arc::new(move || {
            let log = shared.clone();
            log.lock().unwrap().starts += 1;
            let (on_new, on_load, on_prompt, gate) = (log.clone(), log.clone(), log, held.clone());
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
                            responder.respond(
                                InitializeResponse::new(init.protocol_version)
                                    .agent_capabilities(AgentCapabilities::new().load_session(can_load)),
                            )
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |new: NewSessionRequest, responder, _cx| {
                            let mut log = on_new.lock().unwrap();
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
                            let mut log = on_load.lock().unwrap();
                            if log.signed_out {
                                return responder.respond_with_error(agent_client_protocol::Error::auth_required());
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
                            let mut log = on_prompt.lock().unwrap();
                            if log.signed_out {
                                return responder.respond_with_error(agent_client_protocol::Error::auth_required());
                            }
                            log.prompted += 1;
                            if let Some(limited) = &log.limited {
                                let error = agent_client_protocol::Error::internal_error().data(limited.clone());
                                return responder.respond_with_error(error);
                            }
                            drop(log);
                            let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
                            let (text, gate, sender, log, cancels) =
                                (t.text.clone(), gate.clone(), cx.clone(), on_prompt.clone(), cancels.clone());
                            // Answer from a task, so a held prompt does not stall the connection.
                            cx.spawn(async move {
                                if text.starts_with("hold") {
                                    gate.acquire().await.unwrap().forget();
                                }
                                if text.split_whitespace().any(|w| w == "doomed") {
                                    log.lock().unwrap().doomed += 1;
                                    gate.acquire().await.unwrap().forget();
                                    log.lock().unwrap().doomed_failed += 1;
                                    return responder.respond_with_internal_error("doomed");
                                }
                                if text.starts_with("flaky") {
                                    let mut log = log.lock().unwrap();
                                    if !log.flaky_failed.contains(&text) {
                                        log.flaky_failed.push(text);
                                        return responder.respond_with_internal_error("flaky");
                                    }
                                }
                                let session = &prompt.session_id;
                                let words: Vec<&str> = text.split_whitespace().collect();
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
                                    log.lock().unwrap().cancelled += 1;
                                    return responder.respond(PromptResponse::new(StopReason::Cancelled));
                                }
                                log.lock().unwrap().answered.push(text.clone());
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
mod tests {
    use std::sync::Mutex;

    use tokio::sync::{Semaphore, mpsc, oneshot};

    use super::*;
    use crate::harness::{PromptLimits, Spend};

    const IDLE: Duration = Duration::from_secs(60);

    fn fake_pool(max_live: usize, can_load: bool) -> (Pool, Arc<Mutex<fake::Log>>, Arc<Semaphore>) {
        let (launch, log, gate) = fake::launch(can_load);
        (Pool::new(launch, SessionStore::memory(), max_live, IDLE), log, gate)
    }

    fn thread(n: u8) -> ThreadId {
        format!("t{n}").parse().unwrap()
    }

    /// Let spawned harness tasks run until they block.
    async fn settle() {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn each_thread_works_in_its_own_worktree_until_its_session_expires() {
        let Some(repo) = crate::worktrees::tests::repo("pool-worktrees") else { return };
        let home = repo.parent().unwrap().join("home");
        let (launch, log, _) = fake::launch(true);
        let store = SessionStore::memory();
        let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
        let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees(worktrees);
        pool.prompt(&thread(1), "a").await.unwrap();
        pool.prompt(&thread(2), "b").await.unwrap();
        pool.evict_idle(Instant::now() + IDLE * 2);
        pool.prompt(&thread(1), "c").await.unwrap();
        let (t1, t2) = (home.join("worktrees/t1"), home.join("worktrees/t2"));
        assert_eq!(log.lock().unwrap().cwds, [t1.clone(), t2.clone(), t1.clone()], "session/load in the same worktree");
        crate::worktrees::tests::commit(&t2, "work of t2");
        assert_eq!(pool.branch_ahead(&thread(1)).await, None);
        assert_eq!(
            pool.branch_ahead(&thread(2)).await.map(|b| (b.name, b.commits)),
            Some(("ants/dev-a/t2".to_owned(), 1))
        );
        assert_eq!(pool.prune_worktrees().await.removed, 0, "sessions still stored");
        let SessionStore::Memory(sessions) = &store else { unreachable!() };
        sessions.lock().unwrap().clear();
        let pruned = pool.prune_worktrees().await;
        assert_eq!(pruned.removed, 1, "only the thread without a live harness");
        assert_eq!(
            pruned.kept.iter().map(|b| &b.thread).collect::<Vec<_>>(),
            [&thread(2)],
            "its branch holds a commit"
        );
        assert!(t1.is_dir() && !t2.exists());
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn worktrees_left_behind_are_pruned_while_threads_share_the_workdir() {
        let Some(repo) = crate::worktrees::tests::repo("pool-worktrees-off") else { return };
        let home = repo.parent().unwrap().join("home");
        let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
        worktrees.checkout(&thread(1)).await.unwrap();
        worktrees.checkout(&thread(2)).await.unwrap();
        let (launch, log, _) = fake::launch(true);
        let store = SessionStore::memory();
        let left = Worktrees::left_behind(&"dev-a".parse().unwrap(), &home).await.unwrap();
        let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees_left_behind(left);
        pool.prompt(&thread(1), "a").await.unwrap();
        pool.prompt(&thread(3), "b").await.unwrap();
        assert_eq!(log.lock().unwrap().cwds, [repo.clone(), repo.clone()], "threads share the workdir");
        crate::worktrees::tests::commit(&home.join("worktrees/t1"), "work of t1");
        assert_eq!(pool.branch_ahead(&thread(1)).await, None, "the thread works in the workdir");
        assert_eq!(pool.prune_worktrees().await.removed, 1, "only the thread with no session and no live harness");
        let SessionStore::Memory(sessions) = &store else { unreachable!() };
        sessions.lock().unwrap().clear();
        pool.evict_idle(Instant::now() + IDLE * 2);
        assert_eq!(pool.prune_worktrees().await.removed, 1);
        assert!(!home.join("worktrees/t1").exists() && !home.join("worktrees/t3").exists());
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_thread_whose_worktree_fails_shares_the_workdir() {
        let Some(repo) = crate::worktrees::tests::repo("pool-worktrees-fail") else { return };
        let home = repo.parent().unwrap().join("home");
        let (launch, log, _) = fake::launch(true);
        let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
        crate::worktrees::tests::run(&repo, &["branch", "ants/dev-a/t1"]);
        crate::worktrees::tests::run(&repo, &["switch", "--quiet", "ants/dev-a/t1"]);
        let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 4, IDLE).with_worktrees(worktrees);
        assert_eq!(pool.prompt(&thread(1), "a").await.unwrap(), "s1: a", "its branch is checked out elsewhere");
        assert_eq!(log.lock().unwrap().cwds, std::slice::from_ref(&repo));
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_thread_keeps_its_harness_between_messages() {
        let (pool, log, _) = fake_pool(4, true);
        assert_eq!(pool.prompt(&thread(1), "one").await.unwrap(), "s1: one");
        assert_eq!(pool.prompt(&thread(1), "two").await.unwrap(), "s1: two");
        assert_eq!(pool.prompt(&thread(2), "three").await.unwrap(), "s2: three");
        let log = log.lock().unwrap();
        assert_eq!((log.starts, log.new_sessions), (2, 2));
    }

    #[tokio::test]
    async fn the_cap_stops_the_least_recently_used_and_it_resumes() {
        let (pool, log, _) = fake_pool(1, true);
        pool.prompt(&thread(1), "a").await.unwrap();
        pool.prompt(&thread(2), "b").await.unwrap();
        assert_eq!(pool.live(), 1);
        assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s1: c", "no replayed history in the reply");
        let log = log.lock().unwrap();
        assert_eq!((log.starts, log.new_sessions, log.loaded.as_slice()), (3, 2, &["s1".to_owned()][..]));
    }

    #[tokio::test]
    async fn idle_harnesses_stop_and_resume_on_the_next_message() {
        let (pool, log, _) = fake_pool(4, true);
        pool.prompt(&thread(1), "a").await.unwrap();
        assert_eq!(pool.evict_idle(Instant::now()), 0);
        assert_eq!(pool.evict_idle(Instant::now() + IDLE + Duration::from_secs(1)), 1);
        assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
        assert_eq!(log.lock().unwrap().loaded, ["s1"]);
    }

    #[tokio::test]
    async fn a_harness_without_session_load_starts_over() {
        let (pool, log, _) = fake_pool(1, false);
        pool.prompt(&thread(1), "a").await.unwrap();
        pool.prompt(&thread(2), "b").await.unwrap();
        assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s3: c");
        assert!(log.lock().unwrap().loaded.is_empty());
    }

    #[tokio::test]
    async fn threads_run_concurrently_up_to_the_cap() {
        let (pool, log, gate) = fake_pool(2, true);
        let (t1, t2) = (thread(1), thread(2));
        let (a, b) = (pool.prompt(&t1, "hold a"), pool.prompt(&t2, "hold b"));
        let release = async {
            settle().await;
            assert_eq!(log.lock().unwrap().starts, 2, "both threads started before either finished");
            gate.add_permits(2);
        };
        let (a, b, ()) = tokio::join!(a, b, release);
        assert_eq!((a.unwrap(), b.unwrap()), ("s1: hold a".to_owned(), "s2: hold b".to_owned()));
    }

    #[tokio::test]
    async fn a_busy_harness_is_never_stopped() {
        let (pool, log, gate) = fake_pool(1, true);
        let (t1, t2) = (thread(1), thread(2));
        let busy = pool.prompt(&t1, "hold a");
        let others = async {
            settle().await;
            assert_eq!(pool.evict_idle(Instant::now() + IDLE * 2), 0, "idle eviction skips a busy harness");
            let waiting = pool.prompt(&t2, "b");
            let release = async {
                settle().await;
                assert_eq!(log.lock().unwrap().starts, 1, "a new thread waits instead of stopping a busy harness");
                gate.add_permits(1);
            };
            let (reply, ()) = tokio::join!(waiting, release);
            reply.unwrap()
        };
        let (a, b) = tokio::join!(busy, others);
        assert_eq!((a.unwrap().as_str(), b.as_str()), ("s1: hold a", "s2: b"));
        assert_eq!(pool.live(), 1);
    }

    #[tokio::test]
    async fn a_restarted_pool_resumes_the_stored_session() {
        let (launch, log, _) = fake::launch(true);
        let store = SessionStore::memory();
        let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
        pool.prompt(&thread(1), "a").await.unwrap();
        pool.shutdown().await;

        let pool = Pool::new(launch, store, 4, IDLE);
        assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
        let log = log.lock().unwrap();
        assert_eq!((log.new_sessions, log.loaded.as_slice()), (1, &["s1".to_owned()][..]));
    }

    #[tokio::test]
    async fn ant_and_tool_instructions_reach_every_prompt_and_resumed_session() {
        let (launch, log, _) = fake::launch(true);
        let mut ant = crate::config::Ant::new("dev-a".parse().unwrap(), env!("CARGO_MANIFEST_DIR").into());
        ant.whoami = Some("Maintain the Rust backend.".to_owned());
        ant.memory = Some("Use notes.".to_owned());
        ant.tasks = Some("Use tickets.".to_owned());
        let config = ant.into_config("nats://unused", &Default::default()).unwrap();
        let instructions =
            "[ants role dev-a]\nMaintain the Rust backend.\n\n[ants memory]\nUse notes.\n\n[ants tasks]\nUse tickets.";
        let pool = Pool::new(launch.with_instructions(config.instructions), SessionStore::memory(), 1, IDLE);
        for (thread_id, text, session) in [(1, "one", "s1"), (1, "two", "s1"), (2, "three", "s2"), (1, "four", "s1")] {
            assert_eq!(
                pool.prompt(&thread(thread_id), text).await.unwrap(),
                format!("{session}: {instructions}\n\n{text}")
            );
        }
        assert_eq!(log.lock().unwrap().loaded, ["s1"]);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn a_prompt_past_its_time_limit_is_cancelled_and_the_session_goes_on() {
        let (pool, log, _) = fake_pool(1, true);
        let limits = PromptLimits { timeout: Some(Duration::from_millis(50)), max_tool_calls: None };
        let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("stall") }).await.unwrap();
        let cut = "Prompt time limit reached (50ms)".to_owned();
        assert_eq!(reply, Reply { text: "partial".to_owned(), cut: Some(cut) });
        assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
        let log = log.lock().unwrap();
        assert_eq!((log.starts, log.cancelled), (1, 1));
    }

    #[tokio::test]
    async fn a_prompt_past_its_tool_call_limit_is_cancelled() {
        let (pool, log, _) = fake_pool(1, true);
        let limits = PromptLimits { timeout: None, max_tool_calls: Some(2) };
        let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 2") }).await.unwrap();
        assert_eq!(reply.cut, None, "up to the limit is fine");
        let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 3 stall") }).await.unwrap();
        assert_eq!(reply.cut.as_deref(), Some("Tool call limit reached (2 tool calls in one prompt)"));
        assert_eq!(log.lock().unwrap().cancelled, 1);
    }

    #[tokio::test]
    async fn spend_is_reported_and_a_stop_cancels_the_prompt() {
        let (pool, log, _) = fake_pool(1, true);
        let (spend, mut spent) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel();
        let prompt = Prompt { spend: Some(spend), stop: Some(stopped), ..Prompt::new("cost 0.5 stall") };
        let watch = async {
            assert_eq!(spent.recv().await, Some(Spend::Cost { session: SessionId::new("s1"), total: 0.5 }));
            stop.send("Enough".to_owned()).unwrap();
        };
        let t1 = thread(1);
        let (reply, ()) = tokio::join!(pool.run(&t1, prompt), watch);
        assert_eq!(reply.unwrap().cut.as_deref(), Some("Enough"));
        assert_eq!(log.lock().unwrap().cancelled, 1);

        let (spend, mut spent) = mpsc::unbounded_channel();
        pool.run(&thread(1), Prompt { spend: Some(spend), ..Prompt::new("tokens 42") }).await.unwrap();
        let cost = Spend::Cost { session: SessionId::new("s1"), total: 0.5 };
        assert_eq!(spent.try_recv(), Ok(cost), "the last cost comes again as the next prompt starts");
        assert_eq!(spent.try_recv(), Ok(Spend::Tokens(42)));
    }
}
