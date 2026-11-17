use std::collections::HashMap;
use std::sync::Arc;

use ants_proto::{
    ChannelEntry, ChannelId, DEFAULT_TURN_BUDGET, EscalationReason, Hold, Humans, LimitKind, QueueRequest,
    QueuedMessage, Role, State, StatusEvent, StatusUpdate, Uuid, WEBHOOK_ALLOW,
};
use ants_test_support::{NatsServer as Server, free_port};
use async_nats::jetstream::consumer::PullConsumer;
use rmcp::model::CallToolResult;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use super::delivery::reply_id;
use super::prompt::prompt_text;
use super::*;
use crate::admin::restart::Restarted;
use crate::attempts::Reply;
use crate::pool::fake;
use crate::sync::MutexExt as _;
use crate::{channel, dead};

mod admission;
mod control;
mod delivery;
mod escalations;
mod handling;
mod holds;
mod outbox;
mod prompt;
mod queue;

const WAIT: Duration = Duration::from_secs(10);
const IDLE: Duration = Duration::from_secs(60);

/// dev-a's side of a server: it serves its inbox, and the human sends to it.
struct Bus {
    _server: Server,
    client: async_nats::Client,
    js: jetstream::Context,
    stream: jetstream::stream::Stream,
    attempts: Attempts,
    answered: Answered,
    buried: Buried,
    markers: crate::direct_kv::DirectKv,
    dev: UserId,
    limits: PromptLimits,
    ledger: Arc<Ledger>,
    /// dev-a's escalation chain.
    chain: Chain,
    workload: colony::Workload,
    pause: crate::pause::Pause,
    mcp: Option<Mcp>,
    channels: jetstream::kv::Store,
    threads: Threads,
    colony: Colony,
    humans: Registry,
    redactor: Redactor,
    publisher: Option<crate::publish::Publisher>,
    restarts: Restarts,
    /// The team's account whose assistants' records the daemon takes; none: no record is taken.
    account: Option<String>,
    /// How long a failed message waits for its redelivery.
    retry_delay: Duration,
}

impl Bus {
    /// The bus, and the replies to the human.
    async fn start() -> Option<(Self, async_nats::Subscriber)> {
        let server = Server::start()?;
        let url = server.url.clone();
        Some(Self::connect(server, &url, None).await)
    }

    /// dev-a's side of `server`, connected through `url`, with the hub's JetStream in `js_domain`.
    async fn connect(server: Server, url: &str, js_domain: Option<&JsDomain>) -> (Self, async_nats::Subscriber) {
        let dev: UserId = "dev-a".parse().unwrap();
        let (client, js) = bus::connect(url, None, js_domain, &dev, "test").await.unwrap();
        let stream = bus::ensure_stream(&js).await.unwrap();
        let markers = bus::ensure_attempts(&js, js_domain, &dev).await.unwrap();
        let attempts = Attempts::new(markers.clone(), dev.clone());
        let answered = Answered::new(&server.dir().join("dev-a"));
        let buried = Buried::new(&server.dir().join("dev-a"));
        let replies = client.subscribe(subject::inbox_of(&UserId::human())).await.unwrap();
        client.flush().await.unwrap();
        let ledger = Arc::new(Ledger::memory(dev.clone(), Caps::default()));
        let limits = PromptLimits::default();
        let workload = colony::Workload::default();
        let channels = bus::ensure_channels(&js).await.unwrap();
        // Before any post: a new channel consumer starts at the posts stored from then on.
        bus::chan_consumer(&stream, &dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap();
        let threads = Threads::new(js.clone(), dev.clone(), bus::ensure_sessions(&js, js_domain, &dev).await.unwrap());
        let colony = crate::mcp::jetstream_colony(js.clone());
        let bus = Self {
            _server: server,
            client,
            js,
            stream,
            attempts,
            answered,
            buried,
            markers,
            dev,
            limits,
            ledger,
            chain: Chain::default(),
            workload,
            pause: crate::pause::Pause::new(false),
            mcp: None,
            channels,
            threads,
            colony,
            humans: Registry::default(),
            redactor: Redactor::default(),
            publisher: None,
            restarts: Restarts::default(),
            account: None,
            retry_delay: Duration::from_millis(300),
        };
        (bus, replies)
    }

    /// Send `text` from the human, returning the envelope and its stream sequence.
    async fn send(&self, thread: &ThreadId, text: &str) -> (Envelope, u64) {
        self.send_from(UserId::human(), thread, text).await
    }

    /// Send `text` from ant dev-b, marked to wait its turn rather than steer the prompt running on the thread.
    async fn send_ant(&self, thread: &ThreadId, text: &str) -> (Envelope, u64) {
        let envelope = Envelope::on_thread(thread.clone(), "dev-b".parse().unwrap(), self.dev.clone(), text);
        let envelope = Envelope { queue: true, ..envelope };
        let seq = bus::publish_json(&self.js, envelope.subject(), &envelope, None).await.unwrap();
        (envelope, seq)
    }

    async fn replies(&self) -> async_nats::Subscriber {
        let replies = self.client.subscribe(subject::inbox_from(&self.dev)).await.unwrap();
        self.client.flush().await.unwrap();
        replies
    }

    async fn send_from(&self, from: UserId, thread: &ThreadId, text: &str) -> (Envelope, u64) {
        let envelope = Envelope::on_thread(thread.clone(), from, self.dev.clone(), text);
        let seq = bus::publish_json(&self.js, envelope.subject(), &envelope, None).await.unwrap();
        (envelope, seq)
    }

    /// Publish `ant` in the colony with `escalate_to` as its chain.
    async fn put_colony(&self, ant: &UserId, escalate_to: &[UserId]) {
        let colony = bus::ensure_colony(&self.js, ants_proto::COLONY_TTL).await.unwrap();
        let now = chrono::Utc::now();
        let entry = ants_proto::ColonyEntry {
            ant: ant.clone(),
            machine: "m".to_owned(),
            instance: "run-1".to_owned(),
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: Vec::new(),
            whoami: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: ants_proto::Activity::Idle,
            handling: 0,
            harnesses: 0,
            escalate_to: escalate_to.to_vec(),
            version: ants_proto::ANTS_VERSION.to_owned(),
            perms: ants_proto::PERMISSIONS_VERSION,
            signed_out: None,
            limited: None,
            failing: None,
            fallback: None,
            memory_low: None,
            model: None,
            paused: None,
            drift: Vec::new(),
            mcp: false,
        };
        colony.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    }

    /// Publish `text` from `from` on `thread` with no turns left, carrying `escalation`.
    async fn send_spent(
        &self,
        from: &UserId,
        thread: &ThreadId,
        text: &str,
        escalation: Option<&ants_proto::Escalation>,
    ) {
        let mut envelope = Envelope::on_thread(thread.clone(), from.clone(), self.dev.clone(), text)
            .with_escalation(escalation.cloned());
        envelope.turn_budget = 0;
        bus::publish_json(&self.js, envelope.subject(), &envelope, None).await.unwrap();
    }

    /// Put the marker of `hook`, as `ants webhook serve` does before it delivers.
    async fn mark_webhook(&self, hook: &UserId) {
        let config = jetstream::kv::Config {
            bucket: ants_proto::WEBHOOKS_BUCKET.to_owned(),
            max_age: ants_proto::WEBHOOK_RETENTION,
            ..Default::default()
        };
        let webhooks = self.js.create_key_value(config).await.unwrap();
        let marker = serde_json::to_vec(&ants_proto::Serving::here("m")).unwrap();
        webhooks.put(ants_proto::webhook_marker_key(hook), marker.into()).await.unwrap();
    }

    async fn marked(&self, seq: u64) -> bool {
        self.markers.get(ants_proto::attempt_key(&self.dev, seq)).await.unwrap().is_some()
    }

    /// Serve the inbox until a permit is added to `ctrl_c` (twice to stop mid-prompt).
    async fn serve(&self, pool: &Pool, ctrl_c: &Arc<Semaphore>) {
        Box::pin(self.serve_timed(pool, ctrl_c, PROGRESS_EVERY, ACK_WAIT)).await;
    }

    async fn serve_timed(&self, pool: &Pool, ctrl_c: &Arc<Semaphore>, progress_every: Duration, ack_wait: Duration) {
        let open = async || {
            let messages = bus::inbox_consumer(&self.stream, &self.dev, ack_wait, MAX_DELIVER, 4).await?;
            let posts = bus::chan_consumer(&self.stream, &self.dev, ack_wait, MAX_DELIVER, 4).await?;
            anyhow::Ok(futures::stream::select(messages, posts))
        };
        let messages = Box::pin(pause::pausable(self.pause.subscribe(), None, open));
        let channels = Channels::watch(self.channels.clone()).await.unwrap();
        let escalator = self.escalator();
        let notes = crate::notes::JetStreamNotes::new(self.js.clone(), self.dev.clone());
        let home = self._server.dir().join("dev-a");
        let inbox = Inbox {
            client: &self.client,
            js: &self.js,
            ant: &self.dev,
            reading: crate::reading::Reading::Consumer,
            machine: "test-machine",
            pool,
            cancels: Cancels::default(),
            steers: Default::default(),
            unnarrowed: Unnarrowed::default(),
            restarts: &self.restarts,
            attempts: &self.attempts,
            answered: &self.answered,
            buried: &self.buried,
            mcp: self.mcp.as_ref(),
            approvals: None,
            publisher: self.publisher.as_ref(),
            redactor: &self.redactor,
            workload: &self.workload,
            pause: &self.pause,
            drift: &crate::shared::Drift::default(),
            metrics: crate::metrics::ant(&self.dev),
            limits: self.limits,
            ledger: &self.ledger,
            escalator: &escalator,
            notes: &notes,
            threads: &self.threads,
            webhooks: &Webhooks::new(self.js.clone()),
            colony: &self.colony,
            humans: &self.humans,
            behalf: Records::new(self.js.clone(), None, self.account.clone()),
            retry_delay: self.retry_delay,
            progress_every,
            timeline_every: Duration::from_millis(300),
            sign_in: SignIn::default(),
            sign_in_every: Duration::from_millis(300),
            breaker: Breaker::default(),
            throttle: Throttle::default(),
            backoffs: Backoffs {
                rate_limit: ratelimit::Backoff { first: Duration::from_millis(100), max: Duration::from_millis(300) },
                quota: ratelimit::Backoff { first: Duration::from_millis(200), max: Duration::from_millis(400) },
            },
            failover: crate::fallback::Failover::default(),
            fallback_retry: Duration::from_millis(300),
            identity: None,
            home: &home,
        };
        let stop = || {
            let ctrl_c = ctrl_c.clone();
            async move { ctrl_c.acquire().await.unwrap().forget() }
        };
        tokio::select! {
            () = inbox.serve(messages, channels, IDLE, stop) => {}
            () = escalator.deadlines() => {}
            () = inbox.hear_cancels() => {}
            () = inbox.hear_restarts() => {}
            () = inbox.hear_clones() => {}
            () = inbox.hear_mcp() => {}
        }
    }

    fn escalator(&self) -> Escalator {
        let send = escalation::jetstream_sender(self.js.clone());
        let (dev, chain, colony, humans) = (&self.dev, &self.chain, &self.colony, &self.humans);
        Escalator::new(dev.clone(), chain.clone(), colony.clone(), humans.clone(), self.ledger.clone(), send)
            .with_notes(escalation::jetstream_notes_sender(self.js.clone()))
    }

    async fn until_acked(&self, seq: u64) {
        self.until_acked_by(&ants_proto::inbox_consumer(&self.dev), seq).await;
    }

    /// Until the channel posts up to stream sequence `seq` are acked.
    async fn until_posts_acked(&self, seq: u64) {
        self.until_acked_by(&ants_proto::chan_consumer(&self.dev), seq).await;
    }

    async fn until_acked_by(&self, name: &str, seq: u64) {
        timeout(WAIT, async {
            loop {
                // The consumer may not be there yet.
                if let Ok(mut consumer) = self.stream.get_consumer::<jetstream::consumer::pull::Config>(name).await
                    && consumer.info().await.unwrap().ack_floor.stream_sequence >= seq
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("no ack");
    }

    async fn unacked(&self) -> usize {
        self.consumer().await.num_ack_pending
    }

    /// Channel `name` with `members`, and its posts by dev-a.
    async fn channel(&self, name: &str, members: &[&str]) -> (ChannelId, async_nats::Subscriber) {
        let mut entry = ChannelEntry::new(name.parse().unwrap());
        for member in members {
            entry.add_member(member.parse().unwrap());
        }
        self.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
        let posts = self.client.subscribe(subject::chan(&entry.channel, &self.dev)).await.unwrap();
        self.client.flush().await.unwrap();
        (entry.channel, posts)
    }

    /// Store `post` on `subject`, returning its stream sequence.
    async fn post_on(&self, subject: String, post: &ChannelPost) -> u64 {
        bus::publish_json(&self.js, subject, post, None).await.unwrap()
    }

    async fn post(&self, post: &ChannelPost) -> u64 {
        self.post_on(post.subject(), post).await
    }

    /// How many messages the stream keeps on `subject`.
    async fn stored(&self, subject: &str) -> usize {
        use futures::TryStreamExt as _;
        let subjects = self.stream.info_with_subjects(subject).await.unwrap();
        subjects.try_fold(0, |n, (_, count)| async move { Ok(n + count) }).await.unwrap()
    }

    async fn consumer(&self) -> jetstream::consumer::Info {
        let mut consumer: PullConsumer =
            self.stream.get_consumer(&ants_proto::inbox_consumer(&self.dev)).await.unwrap();
        consumer.info().await.unwrap().clone()
    }
}

fn warned(text: &str) -> String {
    prompt_text(text, true, 0)
}

async fn next_reply(replies: &mut async_nats::Subscriber) -> (ThreadId, String) {
    let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
    let envelope: Envelope = serde_json::from_slice(&msg.payload).unwrap();
    let text = envelope.text.split_once(": ").expect("fake reply").1.to_owned();
    (envelope.thread_id, text)
}

async fn next_post(subscriber: &mut async_nats::Subscriber) -> ChannelPost {
    let msg = timeout(WAIT, subscriber.next()).await.expect("no post").unwrap();
    let post: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(post.check_subject(&msg.subject), Ok(()));
    post
}

async fn next_envelope(subscriber: &mut async_nats::Subscriber) -> Envelope {
    let msg = timeout(WAIT, subscriber.next()).await.expect("no message").unwrap();
    serde_json::from_slice(&msg.payload).unwrap()
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

/// Ask the daemon to cancel the prompt on `thread` until it answers, as the daemon may not listen yet.
async fn cancel(bus: &Bus, thread: &ThreadId) -> bool {
    let request = ants_proto::CancelRequest::new(bus.dev.clone(), thread.clone(), UserId::human());
    ask_cancel(bus, &request, WAIT).await.expect("no answer to the cancel request").cancelled
}

/// Send `request` until the daemon answers or `wait` passes.
async fn ask_cancel(bus: &Bus, request: &ants_proto::CancelRequest, wait: Duration) -> Option<ants_proto::CancelReply> {
    let mut answers = bus.client.subscribe(request.reply_subject()).await.unwrap();
    let answer = timeout(wait, async {
        loop {
            let payload = serde_json::to_vec(request).unwrap();
            bus.client.publish(request.subject(), payload.into()).await.unwrap();
            if let Ok(Some(msg)) = timeout(Duration::from_millis(200), answers.next()).await {
                let answer: ants_proto::CancelReply = serde_json::from_slice(&msg.payload).unwrap();
                assert_eq!(answer.check_subject(&msg.subject), Ok(()));
                assert_eq!(answer.request_id, request.id);
                return answer;
            }
        }
    });
    answer.await.ok()
}

async fn queue_with(bus: &Bus, request: QueueRequest) -> ants_proto::QueueReply {
    let mut answers = bus.client.subscribe(request.reply_subject()).await.unwrap();
    timeout(WAIT, async {
        loop {
            let payload = serde_json::to_vec(&request).unwrap();
            bus.client.publish(request.subject(), payload.into()).await.unwrap();
            if let Ok(Some(msg)) = timeout(Duration::from_millis(200), answers.next()).await {
                let answer: ants_proto::QueueReply = serde_json::from_slice(&msg.payload).unwrap();
                assert_eq!(answer.check_subject(&msg.subject), Ok(()));
                assert_eq!(answer.request_id, request.id);
                return answer;
            }
        }
    })
    .await
    .expect("no answer to the queue request")
}

async fn mark_of(bus: &Bus, message: &Envelope) -> Option<ants_proto::MessageMark> {
    let mark = bus.markers.get(ants_proto::message_mark_key(&bus.dev, &message.id)).await.unwrap();
    mark.map(|mark| serde_json::from_slice(&mark).unwrap())
}

fn ants<const N: usize>(names: [&str; N]) -> [UserId; N] {
    names.map(|name| name.parse().unwrap())
}

async fn inboxes<const N: usize>(bus: &Bus, ants: &[UserId; N]) -> [async_nats::Subscriber; N] {
    let mut subscribers = Vec::new();
    for ant in ants {
        subscribers.push(bus.client.subscribe(subject::inbox_of(ant)).await.unwrap());
    }
    bus.client.flush().await.unwrap();
    subscribers.try_into().unwrap_or_else(|_| unreachable!())
}

/// `stuck`'s escalation on `thread`, gone up `chains`, one per level.
fn escalated(stuck: &UserId, thread: &ThreadId, chains: &[&[UserId]]) -> ants_proto::Escalation {
    let mut escalation = ants_proto::Escalation::new(stuck.clone(), thread.clone(), EscalationReason::TurnBudget);
    for chain in chains {
        let ants_proto::Hop::Chain { escalation: up, .. } = escalation.up(chain, 5) else { panic!("a chain") };
        escalation = *up;
    }
    escalation
}

async fn read_notes(bus: &Bus, thread: &ThreadId) -> Vec<ants_proto::NotesPost> {
    crate::notes::read(&bus.js, &"reader".parse().unwrap(), crate::reading::Reading::Consumer, thread)
        .await
        .unwrap()
        .posts
}

/// The `no_reply` entry of the last timeline record of dev-a on `thread`, and the message it is about, once stored.
async fn no_reply_on(bus: &Bus, thread: &ThreadId) -> (Uuid, ants_proto::TimelineEntry) {
    let subject = subject::timeline(thread, &bus.dev);
    let msg = timeout(WAIT, async {
        loop {
            if let Ok(msg) = bus.stream.direct_get_last_for_subject(&subject).await {
                break msg;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("no timeline record");
    let record: ants_proto::Timeline = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(record.check_subject(&msg.subject), Ok(()));
    let [entry] = &record.entries[..] else { panic!("{record:?}") };
    (record.message, entry.clone())
}

/// Call the tool `name` of `mcp` with `args`, as the session of `thread` does.
async fn call_tool(mcp: &Mcp, thread: &ThreadId, name: &'static str, args: serde_json::Value) -> CallToolResult {
    use agent_client_protocol::v1::schema::McpServer;
    use rmcp::ServiceExt as _;
    use rmcp::model::{CallToolRequestParams, ClientConfig};
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

    let servers = mcp.servers()(thread, std::path::Path::new("/"), true);
    let [McpServer::Http(server)] = &servers[..] else { panic!("one HTTP server") };
    let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
    let config = StreamableHttpClientTransportConfig::with_uri(server.url.as_str()).auth_header(token);
    let client = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap();
    let serde_json::Value::Object(args) = args else { panic!("arguments are an object") };
    client.call_tool(CallToolRequestParams::new(name).with_arguments(args)).await.unwrap()
}
