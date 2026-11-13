//! Live harness sessions, one per active thread: capped, stopped when idle, and resumed with session/load.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::ThreadId;
use anyhow::Result;

use crate::harness::{Launch, Session};

struct Live {
    session: Session,
    last_used: Instant,
}

pub struct Pool {
    launch: Launch,
    max_live: usize,
    idle_after: Duration,
    live: HashMap<ThreadId, Live>,
    /// Session of every thread seen, live or not, so a stopped one resumes where it left off.
    sessions: HashMap<ThreadId, SessionId>,
}

impl Pool {
    /// At most `max_live` harnesses (at least one); each stops after `idle_after` without a prompt.
    pub fn new(launch: Launch, max_live: usize, idle_after: Duration) -> Self {
        Self { launch, max_live: max_live.max(1), idle_after, live: HashMap::new(), sessions: HashMap::new() }
    }

    /// Run a prompt in the thread's session, starting or resuming its harness when none is live.
    pub async fn prompt(&mut self, thread: &ThreadId, text: &str) -> Result<String> {
        if !self.live.get(thread).is_some_and(|l| l.session.is_alive()) {
            self.live.remove(thread);
            self.make_room();
            let resume = self.sessions.get(thread).cloned();
            let session = Session::start(&self.launch.for_thread(thread), resume.clone()).await?;
            match &resume {
                Some(old) if old == session.id() => tracing::info!(%thread, session = %old, "resumed harness session"),
                _ => tracing::info!(%thread, session = %session.id(), "started harness session"),
            }
            self.sessions.insert(thread.clone(), session.id().clone());
            self.live.insert(thread.clone(), Live { session, last_used: Instant::now() });
        }
        let live = self.live.get_mut(thread).expect("live session");
        let reply = live.session.prompt(text).await;
        live.last_used = Instant::now();
        reply
    }

    /// Stop harnesses idle since before `now - idle_after`. Returns how many stopped.
    pub fn evict_idle(&mut self, now: Instant) -> usize {
        let before = self.live.len();
        let idle_after = self.idle_after;
        self.live.retain(|thread, live| {
            let keep = now.saturating_duration_since(live.last_used) < idle_after;
            if !keep {
                tracing::info!(%thread, "stopping idle harness");
            }
            keep
        });
        before - self.live.len()
    }

    /// Stop every harness and wait for them to exit.
    pub async fn shutdown(self) {
        futures::future::join_all(self.live.into_values().map(|l| l.session.close())).await;
    }

    pub fn live(&self) -> usize {
        self.live.len()
    }

    /// Stop least recently used harnesses until one more fits.
    fn make_room(&mut self) {
        while self.live.len() >= self.max_live {
            let Some(oldest) = self.live.iter().min_by_key(|(_, l)| l.last_used).map(|(t, _)| t.clone()) else {
                return;
            };
            tracing::info!(thread = %oldest, "stopping harness to stay within the cap");
            self.live.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use agent_client_protocol::v1::schema::{
        AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse, LoadSessionRequest,
        LoadSessionResponse, NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse, SessionNotification,
        SessionUpdate, StopReason, TextContent,
    };
    use agent_client_protocol::{Agent, DynConnectTo};

    use super::*;

    /// What the fake harnesses were asked to do, across all their "processes".
    #[derive(Default, Debug)]
    struct Log {
        starts: usize,
        new_sessions: usize,
        loaded: Vec<String>,
    }

    /// A pool over in-process fake harnesses that answer each prompt with `<session>: <text>`.
    fn fake_pool(max_live: usize, can_load: bool) -> (Pool, Arc<Mutex<Log>>) {
        let log = Arc::new(Mutex::new(Log::default()));
        let shared = log.clone();
        let connect = Arc::new(move || {
            let log = shared.clone();
            log.lock().unwrap().starts += 1;
            let (on_new, on_load) = (log.clone(), log);
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("fake")
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
                        async move |_: NewSessionRequest, responder, _cx| {
                            let mut log = on_new.lock().unwrap();
                            log.new_sessions += 1;
                            responder.respond(NewSessionResponse::new(format!("s{}", log.new_sessions)))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |load: LoadSessionRequest, responder, cx| {
                            on_load.lock().unwrap().loaded.push(load.session_id.to_string());
                            // Replayed history must not leak into the next reply.
                            cx.send_notification(chunk(&load.session_id, "old history"))?;
                            responder.respond(LoadSessionResponse::new())
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |prompt: PromptRequest, responder, cx| {
                            let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
                            cx.send_notification(chunk(
                                &prompt.session_id,
                                &format!("{}: {}", prompt.session_id, t.text),
                            ))?;
                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        },
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        (
            Pool::new(
                Launch::with_transport(connect, PathBuf::from("/"), Default::default()),
                max_live,
                Duration::from_secs(60),
            ),
            log,
        )
    }

    fn chunk(session: &SessionId, text: &str) -> SessionNotification {
        SessionNotification::new(
            session.clone(),
            SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(text)))),
        )
    }

    fn thread(n: u8) -> ThreadId {
        format!("t{n}").parse().unwrap()
    }

    #[tokio::test]
    async fn a_thread_keeps_its_harness_between_messages() {
        let (mut pool, log) = fake_pool(4, true);
        assert_eq!(pool.prompt(&thread(1), "one").await.unwrap(), "s1: one");
        assert_eq!(pool.prompt(&thread(1), "two").await.unwrap(), "s1: two");
        assert_eq!(pool.prompt(&thread(2), "three").await.unwrap(), "s2: three");
        let log = log.lock().unwrap();
        assert_eq!((log.starts, log.new_sessions), (2, 2));
    }

    #[tokio::test]
    async fn the_cap_stops_the_least_recently_used_and_it_resumes() {
        let (mut pool, log) = fake_pool(1, true);
        pool.prompt(&thread(1), "a").await.unwrap();
        pool.prompt(&thread(2), "b").await.unwrap();
        assert_eq!(pool.live(), 1);
        assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s1: c", "no replayed history in the reply");
        let log = log.lock().unwrap();
        assert_eq!((log.starts, log.new_sessions, log.loaded.as_slice()), (3, 2, &["s1".to_owned()][..]));
    }

    #[tokio::test]
    async fn idle_harnesses_stop_and_resume_on_the_next_message() {
        let (mut pool, log) = fake_pool(4, true);
        pool.prompt(&thread(1), "a").await.unwrap();
        assert_eq!(pool.evict_idle(Instant::now()), 0);
        assert_eq!(pool.evict_idle(Instant::now() + Duration::from_secs(61)), 1);
        assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
        assert_eq!(log.lock().unwrap().loaded, ["s1"]);
    }

    #[tokio::test]
    async fn a_harness_without_session_load_starts_over() {
        let (mut pool, log) = fake_pool(1, false);
        pool.prompt(&thread(1), "a").await.unwrap();
        pool.prompt(&thread(2), "b").await.unwrap();
        assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s3: c");
        assert!(log.lock().unwrap().loaded.is_empty());
    }
}
