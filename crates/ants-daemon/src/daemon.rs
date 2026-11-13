//! The inbox loop of one persona.
//!
//! Messages of one thread are handled one at a time, in order; different threads run concurrently, each in its own
//! harness from the pool.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ants_proto::{Envelope, Error as ProtoError, PersonaId, ThreadId, subject};
use anyhow::{Result, anyhow};
use async_nats::jetstream::{self, AckKind};
use futures::stream::{FuturesUnordered, Stream, StreamExt as _};

use crate::approvals::NatsApprover;
use crate::dead::{self, Attempt};
use crate::harness::{HarnessKind, Launch};
use crate::mcp::Mcp;
use crate::permission::{Allowlist, PermissionPolicy};
use crate::pool::{Pool, SessionStore};
use crate::{bus, roster, workspace};

/// Redelivery window. The daemon extends it with progress acks while a message waits or its prompt runs, so a crash
/// mid-task redelivers.
const ACK_WAIT: Duration = Duration::from_secs(5 * 60);
const PROGRESS_EVERY: Duration = Duration::from_secs(60);
/// Attempts a message gets before it becomes a dead letter on `ants.dead.<persona>`.
const MAX_ATTEMPTS: i64 = 5;
/// The consumer allows one more delivery, to dead-letter a message whose last attempt never finished.
const MAX_DELIVER: i64 = MAX_ATTEMPTS + 1;
const RETRY_DELAY: Duration = Duration::from_secs(30);
/// How often idle harnesses are looked for.
const EVICT_EVERY: Duration = Duration::from_secs(30);
/// Inbox messages in hand per harness: prompting, or waiting for their thread or for a free harness.
const IN_HAND_PER_HARNESS: usize = 2;

#[derive(Debug, Clone)]
pub struct Config {
    pub persona: PersonaId,
    pub nats_url: String,
    /// Credentials file of the persona, when the server requires them.
    pub creds: Option<PathBuf>,
    /// Directory the harness works in.
    pub workdir: PathBuf,
    /// The persona's own files, its bd workspace among them (usually `~/.ants/<persona>`).
    pub home: PathBuf,
    pub harness: HarnessKind,
    /// Tool requests the harness may run without asking (see [`crate::permission`] for the rules). Every other one
    /// goes to the human on `ants.approvals.<persona>`.
    pub allow: Vec<String>,
    /// How long a tool request waits for the human before it is rejected.
    pub approval_timeout: Duration,
    /// Most harness processes alive at once; the least recently used idle one stops to make room.
    pub max_harnesses: usize,
    /// A thread's harness stops after this long without a message, and resumes its session on the next one.
    pub idle_timeout: Duration,
    /// Capabilities the persona advertises in the roster.
    pub capabilities: Vec<String>,
}

/// Serve the persona's inbox until Ctrl-C. Prompts in flight are finished first; a second Ctrl-C stops at once.
pub async fn run(config: Config) -> Result<()> {
    let allow = Allowlist::parse(&config.allow)?;
    let db = workspace::ensure(&config.home, &config.persona).await?;
    let (client, js) =
        bus::connect(&config.nats_url, config.creds.as_deref(), &config.persona, &format!("antsd-{}", config.persona))
            .await?;
    let approver = NatsApprover::new(client.clone(), js.clone(), config.persona.clone(), config.approval_timeout);
    let policy = PermissionPolicy::new(allow, Some(std::sync::Arc::new(approver)));
    let mut mcp = Mcp::start(config.persona.clone(), &config.home, crate::mcp::jetstream_outbox(js.clone())).await?;
    mcp.listen(client.subscribe(subject::inbox_of(&config.persona)).await?);
    let launch = Launch::new(config.harness, config.workdir.clone(), workspace::harness_env(&db)?, policy)
        .with_mcp(mcp.servers());
    let stream = bus::ensure_stream(&js).await?;
    let sessions = SessionStore::kv(bus::ensure_sessions(&js).await?, config.persona.clone());
    let pool = Pool::new(launch, sessions, config.max_harnesses, config.idle_timeout);
    let in_hand = i64::try_from(config.max_harnesses.max(1) * IN_HAND_PER_HARNESS)?;
    let messages = bus::inbox_consumer(&stream, &config.persona, ACK_WAIT, MAX_DELIVER, in_hand).await?;
    let registration = roster::register(&js, &config).await?;
    tracing::info!(
        persona = %config.persona,
        harness = %config.harness,
        workdir = %config.workdir.display(),
        workspace = %db.display(),
        max_harnesses = config.max_harnesses,
        idle_timeout = ?config.idle_timeout,
        capabilities = ?config.capabilities,
        allow = ?config.allow,
        approval_timeout = ?config.approval_timeout,
        "serving inbox"
    );

    let inbox = Inbox {
        client: &client,
        js: &js,
        persona: &config.persona,
        pool: &pool,
        mcp: Some(&mcp),
        retry_delay: RETRY_DELAY,
    };
    let ctrl_c = || async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "waiting for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    inbox.serve(messages, config.idle_timeout, ctrl_c).await;
    registration.deregister().await;
    pool.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}

type InHand = (Envelope, jetstream::Message);

/// The messages of one thread in hand: one in flight at most, the rest waiting their turn.
#[derive(Default)]
struct Thread {
    waiting: VecDeque<InHand>,
    busy: bool,
    /// A failed message that is to be redelivered. Until it is back (or the deadline passes) the rest of the thread
    /// waits rather than overtake it.
    retry: Option<Retry>,
}

#[derive(Clone, Copy)]
struct Retry {
    seq: u64,
    until: tokio::time::Instant,
}

struct Inbox<'a> {
    client: &'a async_nats::Client,
    js: &'a jetstream::Context,
    persona: &'a PersonaId,
    pool: &'a Pool,
    mcp: Option<&'a Mcp>,
    retry_delay: Duration,
}

impl Inbox<'_> {
    /// Handle messages until they end or `stop` resolves, then finish the prompts in flight unless it resolves again.
    async fn serve<S, E, F>(&self, mut messages: S, idle_timeout: Duration, mut stop: impl FnMut() -> F)
    where
        S: Stream<Item = Result<jetstream::Message, E>> + Unpin,
        E: Display,
        F: Future<Output = ()>,
    {
        let mut threads: HashMap<ThreadId, Thread> = HashMap::new();
        let mut running = FuturesUnordered::new();
        let mut evict = tokio::time::interval(EVICT_EVERY.min(idle_timeout).max(Duration::from_secs(1)));
        let mut progress = tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS_EVERY, PROGRESS_EVERY);
        let shutdown = stop();
        tokio::pin!(shutdown);
        loop {
            let retry_deadline = threads.values().filter_map(|t| t.retry.map(|r| r.until)).min();
            let mut ready = None;
            tokio::select! {
                () = &mut shutdown => break,
                _ = evict.tick() => {
                    self.pool.evict_idle(Instant::now());
                }
                _ = progress.tick() => {
                    for (_, msg) in threads.values().flat_map(|t| &t.waiting) {
                        if let Err(e) = msg.ack_with(AckKind::Progress).await {
                            tracing::warn!(error = %e, "progress ack");
                        }
                    }
                }
                () = sleep_until(retry_deadline), if retry_deadline.is_some() => {
                    let now = tokio::time::Instant::now();
                    for (thread, t) in &mut threads {
                        if t.retry.is_some_and(|r| r.until <= now) {
                            tracing::warn!(%thread, "failed message not redelivered in time; going on with the thread");
                            t.retry = None;
                        }
                    }
                    ready = Some(threads.keys().cloned().collect());
                }
                Some((thread, retry)) = running.next(), if !running.is_empty() => {
                    if let Some(t) = threads.get_mut(&thread) {
                        t.busy = false;
                        t.retry = retry;
                    }
                    ready = Some(vec![thread]);
                }
                next = messages.next() => match next {
                    None => break,
                    Some(Err(e)) => tracing::warn!(error = %e, "inbox stream"),
                    Some(Ok(msg)) => {
                        let envelope: Envelope = match serde_json::from_slice(&msg.payload) {
                            Ok(e) => e,
                            Err(e) => {
                                self.bury_unanswerable(&msg, &format!("not an envelope: {e}")).await;
                                continue;
                            }
                        };
                        // A forged sender is buried without its envelope, so a replay cannot pass it on as genuine.
                        if let Err(e) = envelope.check_subject(&msg.subject) {
                            self.bury_unanswerable(&msg, &format!("sender mismatch: {e}")).await;
                            continue;
                        }
                        if self.claim(&envelope, &msg).await {
                            continue;
                        }
                        let thread = envelope.thread_id.clone();
                        let t = threads.entry(thread.clone()).or_default();
                        if t.retry.is_some_and(|r| Some(r.seq) == stream_seq(&msg)) {
                            t.retry = None;
                            t.waiting.push_front((envelope, msg));
                        } else {
                            t.waiting.push_back((envelope, msg));
                        }
                        ready = Some(vec![thread]);
                    }
                }
            }
            for thread in ready.into_iter().flatten() {
                if let Some((envelope, msg)) = next_turn(&mut threads, thread) {
                    running.push(self.handle(envelope, msg));
                }
            }
        }

        drop(messages);
        // Hand waiting messages back at once rather than after the ack wait, oldest first.
        let mut waiting: Vec<_> = threads.into_values().flat_map(|t| t.waiting).map(|(_, msg)| msg).collect();
        waiting.sort_by_key(stream_seq);
        for msg in &waiting {
            nak(msg, None).await;
        }
        if !running.is_empty() {
            tracing::info!(prompts = running.len(), "finishing prompts in flight; Ctrl-C again stops now");
            let drain = async { while running.next().await.is_some() {} };
            tokio::select! {
                () = drain => {}
                () = stop() => tracing::warn!("stopping mid-prompt; those messages are redelivered"),
            }
        }
    }

    /// Whether `envelope` is a reply to an `ask_agent` call, now handed over and acked. It must not wait in its
    /// thread's queue: the prompt that asked is in flight on that thread.
    async fn claim(&self, envelope: &Envelope, msg: &jetstream::Message) -> bool {
        let Some(mcp) = self.mcp else { return false };
        match mcp.claim(envelope).await {
            Ok(false) => false,
            Ok(true) => {
                if let Err(e) = msg.double_ack().await {
                    tracing::warn!(error = %e, "acking a reply to ask_agent");
                }
                true
            }
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "keeping a reply to ask_agent");
                nak(msg, Some(self.retry_delay)).await;
                true
            }
        }
    }

    /// Dead-letter a message that is never to be prompted with, keeping only an excerpt of its payload.
    async fn bury_unanswerable(&self, msg: &jetstream::Message, error: &str) {
        tracing::warn!(error, subject = %msg.subject, "dead-lettering a message without answering it");
        if let Err(e) = dead::bury(self.js, self.persona, msg, None, error).await {
            // Never redelivered, dead letter or not.
            tracing::warn!(error = format!("{e:#}"), "dead-lettering");
            if let Err(e) = msg.ack_with(AckKind::Term).await {
                tracing::warn!(error = %e, "terminating the message");
            }
        }
    }

    /// Answer one message. A failed attempt is redelivered after a delay, and the last one becomes a dead letter.
    /// Returns its thread, and the redelivery to wait for when there is one.
    async fn handle(&self, envelope: Envelope, msg: jetstream::Message) -> (ThreadId, Option<Retry>) {
        let thread = envelope.thread_id.clone();
        let attempt = Attempt::of(msg.info().map_or(1, |i| i.delivered), MAX_ATTEMPTS);
        if attempt == Attempt::Overdue {
            let error = format!("no outcome after {MAX_ATTEMPTS} attempts: the daemon stopped or the ack wait ran out");
            if let Err(e) = dead::bury(self.js, self.persona, &msg, Some(&envelope), &error).await {
                tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering");
            }
            return (thread, None);
        }
        let Err(e) = self.answer(&envelope, &msg).await else { return (thread, None) };
        tracing::error!(%thread, error = format!("{e:#}"), "handling message");
        if let Err(e) = self.client.publish(subject::status(self.persona, &thread), "failed".into()).await {
            tracing::warn!(error = %e, "publishing status");
        }
        if attempt == Attempt::Last {
            match dead::bury(self.js, self.persona, &msg, Some(&envelope), &format!("{e:#}")).await {
                Ok(_) => return (thread, None),
                // The overdue delivery tries again.
                Err(bury) => tracing::warn!(error = format!("{bury:#}"), "dead-lettering"),
            }
        }
        nak(&msg, Some(self.retry_delay)).await;
        let retry = msg.info().ok().filter(|info| info.delivered < MAX_DELIVER).map(|info| Retry {
            seq: info.stream_sequence,
            // Redelivery may lag behind the delay; past this the thread goes on without the message.
            until: tokio::time::Instant::now() + self.retry_delay * 3,
        });
        (thread, retry)
    }

    async fn answer(&self, envelope: &Envelope, msg: &jetstream::Message) -> Result<()> {
        if let Err(ProtoError::BudgetExhausted(_)) = envelope.reply(self.persona.clone(), "") {
            // Out of turns: hand the thread to the human instead of spending another prompt.
            let mut escalation = Envelope::on_thread(
                envelope.thread_id.clone(),
                self.persona.clone(),
                PersonaId::human(),
                format!("Turn budget spent; needs a decision. Last message from {}:\n{}", envelope.from, envelope.text),
            );
            escalation.in_reply_to = Some(envelope.id);
            bus::publish(self.js, &escalation).await?;
            return msg.double_ack().await.map_err(|e| anyhow!(e));
        }

        let status = subject::status(self.persona, &envelope.thread_id);
        tracing::info!(thread = %envelope.thread_id, from = %envelope.from, "prompting");
        self.client.publish(status.clone(), "working".into()).await?;

        let _turn = self.mcp.map(|mcp| mcp.begin(envelope));
        let prompt = self.pool.prompt(&envelope.thread_id, &envelope.text);
        tokio::pin!(prompt);
        let mut progress = tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS_EVERY, PROGRESS_EVERY);
        let text = loop {
            tokio::select! {
                r = &mut prompt => break r?,
                _ = progress.tick() => {
                    if let Err(e) = msg.ack_with(AckKind::Progress).await {
                        tracing::warn!(error = %e, "progress ack");
                    }
                }
            }
        };

        let reply = envelope.reply(self.persona.clone(), text)?;
        // Store the reply before acking: a crash in between redelivers rather than loses the message.
        bus::publish(self.js, &reply).await?;
        msg.double_ack().await.map_err(|e| anyhow!(e))?;
        self.client.publish(status, "idle".into()).await?;
        Ok(())
    }
}

/// The thread's next message to prompt, unless one is in flight or it waits for a redelivery. Forgets a thread with
/// nothing left in hand.
fn next_turn(threads: &mut HashMap<ThreadId, Thread>, thread: ThreadId) -> Option<InHand> {
    let Entry::Occupied(mut entry) = threads.entry(thread) else { return None };
    let t = entry.get_mut();
    if t.busy || t.retry.is_some() {
        return None;
    }
    let next = t.waiting.pop_front();
    match next {
        Some(_) => t.busy = true,
        None => {
            entry.remove();
        }
    }
    next
}

fn stream_seq(msg: &jetstream::Message) -> Option<u64> {
    msg.info().ok().map(|info| info.stream_sequence)
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn nak(msg: &jetstream::Message, delay: Option<Duration>) {
    if let Err(e) = msg.ack_with(AckKind::Nak(delay)).await {
        tracing::warn!(error = %e, "nak");
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_nats::jetstream::consumer::PullConsumer;
    use tokio::sync::Semaphore;
    use tokio::time::timeout;

    use super::*;
    use crate::pool::fake;

    const WAIT: Duration = Duration::from_secs(10);
    const IDLE: Duration = Duration::from_secs(60);

    /// A nats-server with JetStream on a port of its choosing, its files under `target/`, removed on drop.
    struct Server {
        child: Child,
        dir: PathBuf,
        url: String,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    impl Server {
        /// None when nats-server is not installed.
        fn start() -> Option<Self> {
            if Command::new("nats-server").arg("--version").output().is_err() {
                eprintln!("nats-server not on PATH; skipping");
                return None;
            }
            static SERVERS: AtomicUsize = AtomicUsize::new(0);
            let n = SERVERS.fetch_add(1, Ordering::Relaxed);
            let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../../target/test-nats-daemon-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let child = Command::new("nats-server")
                .args(["-js", "-a", "127.0.0.1", "-p", "-1", "-sd"])
                .arg(dir.join("js"))
                .arg("--ports_file_dir")
                .arg(&dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let ports = dir.join(format!("nats-server_{}.ports", child.id()));
            let mut server = Self { child, dir, url: String::new() };
            for _ in 0..50 {
                let url = std::fs::read_to_string(&ports).ok().and_then(|json| {
                    let ports: serde_json::Value = serde_json::from_str(&json).ok()?;
                    Some(ports["nats"][0].as_str()?.to_owned())
                });
                if let Some(url) = url {
                    server.url = url;
                    return Some(server);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            panic!("nats-server did not start");
        }
    }

    /// dev-a's side of a server: it serves its inbox, and the human sends to it.
    struct Bus {
        _server: Server,
        client: async_nats::Client,
        js: jetstream::Context,
        stream: jetstream::stream::Stream,
        dev: PersonaId,
    }

    impl Bus {
        /// The bus, and the replies to the human.
        async fn start() -> Option<(Self, async_nats::Subscriber)> {
            let server = Server::start()?;
            let dev: PersonaId = "dev-a".parse().unwrap();
            let (client, js) = bus::connect(&server.url, None, &dev, "test").await.unwrap();
            let stream = bus::ensure_stream(&js).await.unwrap();
            let replies = client.subscribe(subject::inbox_of(&PersonaId::human())).await.unwrap();
            client.flush().await.unwrap();
            Some((Self { _server: server, client, js, stream, dev }, replies))
        }

        async fn send(&self, thread: &ThreadId, text: &str) {
            let envelope = Envelope::on_thread(thread.clone(), PersonaId::human(), self.dev.clone(), text);
            bus::publish(&self.js, &envelope).await.unwrap();
        }

        /// Serve the inbox until a permit is added to `ctrl_c` (twice to stop mid-prompt).
        async fn serve(&self, pool: &Pool, ctrl_c: &Arc<Semaphore>) {
            let messages = bus::inbox_consumer(&self.stream, &self.dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap();
            let inbox = Inbox {
                client: &self.client,
                js: &self.js,
                persona: &self.dev,
                pool,
                mcp: None,
                retry_delay: Duration::from_millis(300),
            };
            let stop = || {
                let ctrl_c = ctrl_c.clone();
                async move { ctrl_c.acquire().await.unwrap().forget() }
            };
            inbox.serve(messages, IDLE, stop).await;
        }

        async fn unacked(&self) -> usize {
            let mut consumer: PullConsumer =
                self.stream.get_consumer(&ants_proto::inbox_consumer(&self.dev)).await.unwrap();
            consumer.info().await.unwrap().num_ack_pending
        }
    }

    async fn next_reply(replies: &mut async_nats::Subscriber) -> (ThreadId, String) {
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let envelope: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        let text = envelope.text.split_once(": ").expect("fake reply").1.to_owned();
        (envelope.thread_id, text)
    }

    async fn until(what: &str, done: impl Fn() -> bool) {
        timeout(WAIT, async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    #[tokio::test]
    async fn threads_run_concurrently_and_each_stays_in_order() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, gate) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;
        bus.send(&t2, "c").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t2.clone(), "c".to_owned()), "t2 does not wait for t1");
            assert_eq!(log.lock().unwrap().answered, ["c"], "b waits for the message before it on t1");
            gate.add_permits(1);
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_failed_message_is_retried_before_the_rest_of_its_thread() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "flaky a").await;
        bus.send(&t1, "b").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "flaky a".to_owned()));
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, ["flaky a", "b"]);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_forged_sender_is_dead_lettered_unanswered() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let thread = ThreadId::generate();
        // dev-b sends on its own subject, but the envelope claims the human sent it.
        let forged = Envelope::on_thread(thread.clone(), PersonaId::human(), bus.dev.clone(), "forged");
        let dev_b: PersonaId = "dev-b".parse().unwrap();
        crate::bus::publish_json(&bus.js, subject::inbox(&bus.dev, &dev_b), &forged, None).await.unwrap();
        bus.send(&thread, "genuine").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (thread.clone(), "genuine".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, ["genuine"]);
        let letters = dead::list(&bus.stream).await.unwrap();
        let [(_, letter)] = letters.as_slice() else { panic!("{letters:?}") };
        assert!(letter.error.contains("sender mismatch"), "{}", letter.error);
        assert_eq!(letter.envelope, None, "a forged envelope is not replayable");
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn inbox_consumers_from_before_sender_subjects_get_the_new_filter() {
        let Some((bus, _)) = Bus::start().await else { return };
        let human = PersonaId::human();
        for persona in [&bus.dev, &human] {
            let name = ants_proto::inbox_consumer(persona);
            let old = jetstream::consumer::pull::Config {
                durable_name: Some(name.clone()),
                filter_subject: format!("ants.inbox.{persona}"),
                ..Default::default()
            };
            bus.stream.create_consumer(old).await.unwrap();
        }
        drop(crate::bus::inbox_consumer(&bus.stream, &bus.dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap());
        crate::bus::human_inbox_consumer(&bus.stream, ACK_WAIT).await.unwrap();
        for persona in [&bus.dev, &human] {
            let mut consumer: PullConsumer =
                bus.stream.get_consumer(&ants_proto::inbox_consumer(persona)).await.unwrap();
            assert_eq!(consumer.info().await.unwrap().config.filter_subject, subject::inbox_of(persona));
        }
    }

    #[tokio::test]
    async fn ctrl_c_finishes_prompts_in_flight_and_a_restart_resumes() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, gate) = fake::launch(true);
        let store = SessionStore::memory();
        let t1 = ThreadId::generate();
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;

        let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let stop_mid_prompt = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            ctrl_c.add_permits(1);
            gate.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), stop_mid_prompt);
        pool.shutdown().await;
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(log.lock().unwrap().answered, ["hold a"], "b was handed back, not prompted");

        let pool = Pool::new(launch, store, 2, IDLE);
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().loaded, ["s1"], "the restarted daemon resumed the thread's session");
        assert_eq!(bus.unacked().await, 0);
    }
}
