#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use ants_proto::{ChannelEntry, ChannelId, DEFAULT_TURN_BUDGET, LimitKind, Role, StatusEvent, StatusUpdate};
use ants_test_support::{NatsServer as Server, free_port};
use async_nats::jetstream::consumer::PullConsumer;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use super::*;
use crate::pool::fake;

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
    workload: roster::Workload,
    pause: crate::pause::Pause,
    mcp: Option<Mcp>,
    channels: jetstream::kv::Store,
    threads: Threads,
    roster: Roster,
    humans: Registry,
    redactor: Redactor,
    publisher: Option<crate::publish::Publisher>,
    restarts: Restarts,
    /// The team's account whose assistants' records the daemon takes; none: no record is taken.
    account: Option<String>,
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
        let workload = roster::Workload::default();
        let channels = bus::ensure_channels(&js).await.unwrap();
        // Before any post: a new channel consumer starts at the posts stored from then on.
        bus::chan_consumer(&stream, &dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap();
        let threads = Threads::new(js.clone(), dev.clone(), bus::ensure_sessions(&js, js_domain, &dev).await.unwrap());
        let roster = crate::mcp::jetstream_roster(js.clone());
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
            roster,
            humans: Registry::default(),
            redactor: Redactor::default(),
            publisher: None,
            restarts: Restarts::default(),
            account: None,
        };
        (bus, replies)
    }

    /// Send `text` from the human, returning the envelope and its stream sequence.
    async fn send(&self, thread: &ThreadId, text: &str) -> (Envelope, u64) {
        self.send_from(UserId::human(), thread, text).await
    }

    async fn send_from(&self, from: UserId, thread: &ThreadId, text: &str) -> (Envelope, u64) {
        let envelope = Envelope::on_thread(thread.clone(), from, self.dev.clone(), text);
        let seq = bus::publish_json(&self.js, envelope.subject(), &envelope, None).await.unwrap();
        (envelope, seq)
    }

    /// Publish `ant` in the roster with `escalate_to` as its chain.
    async fn put_roster(&self, ant: &UserId, escalate_to: &[UserId]) {
        let roster = bus::ensure_roster(&self.js, ants_proto::ROSTER_TTL).await.unwrap();
        let now = chrono::Utc::now();
        let entry = ants_proto::RosterEntry {
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
        };
        roster.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
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
            roster: &self.roster,
            humans: &self.humans,
            behalf: Records::new(self.js.clone(), None, self.account.clone()),
            retry_delay: Duration::from_millis(300),
            progress_every,
            timeline_every: Some(Duration::from_millis(300)),
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
        }
    }

    fn escalator(&self) -> Escalator {
        let send = escalation::jetstream_sender(self.js.clone());
        let (dev, chain, roster, humans) = (&self.dev, &self.chain, &self.roster, &self.humans);
        Escalator::new(dev.clone(), chain.clone(), roster.clone(), humans.clone(), self.ledger.clone(), send)
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

#[tokio::test]
async fn threads_run_concurrently_and_each_stays_in_order() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t2.clone(), "c".to_owned()), "t2 does not wait for t1");
        assert_eq!(log.locked().answered, ["c"], "b waits for the message before it on t1");
        until("c to be done", || bus.workload.handling() == 1).await;
        assert_eq!(bus.workload.activity(), ants_proto::Activity::Working);
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        until("the inbox to go idle", || bus.workload.activity() == ants_proto::Activity::Idle).await;
        assert_eq!(bus.workload.harnesses(), 2, "a harness per thread stays live");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_paused_ant_keeps_its_messages_queued_and_takes_no_prompt_until_resumed() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.pause = crate::pause::Pause::new(true);
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let resume = || bus.pause.replace(&crate::pause::Pause::new(false));
    let pause = || bus.pause.replace(&crate::pause::Pause::new(true));
    let check = async {
        until("the roster to show the pause", || bus.workload.paused().is_some()).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(log.locked().prompted, 0, "paused from the start");
        let name = ants_proto::inbox_consumer(&bus.dev);
        let consumer = bus.stream.get_consumer::<jetstream::consumer::pull::Config>(&name).await;
        assert!(consumer.is_err(), "nothing pulled while paused");

        resume();
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "a".to_owned()));
        assert_eq!(bus.workload.paused(), None);
        bus.send(&t1, "hold b").await;
        until("b to start", || log.locked().prompted == 2).await;
        bus.send(&t1, "c").await;
        timeout(WAIT, async {
            while bus.consumer().await.num_ack_pending < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("c never came in hand");
        pause();
        until("the roster to show the pause", || bus.workload.paused().is_some()).await;
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold b".to_owned()), "a running prompt finishes");
        bus.send(&t2, "d").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(log.locked().prompted, 2, "neither the message in hand nor a new one starts");
        let consumer = bus.consumer().await;
        assert_eq!((consumer.num_pending, consumer.num_ack_pending), (1, 1), "d queued, c in hand: {consumer:?}");

        resume();
        let mut rest = [next_reply(&mut replies).await, next_reply(&mut replies).await];
        rest.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(rest, [(t1.clone(), "c".to_owned()), (t2.clone(), "d".to_owned())]);
        assert_eq!(log.locked().starts, 2, "a harness per thread, none restarted");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn replies_are_redacted() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.redactor = Redactor::default().with_env([("DEPLOY_TOKEN", "d3pl0y-s3cret")]);
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "use d3pl0y-s3cret or ghp_0123456789abcdefghijABCDEFGHIJ").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "use [redacted] or [redacted]".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_human_s_summary_request_is_prompted_as_one_and_the_reply_carries_it_with_its_tokens() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let (asked, summarised) = (ThreadId::generate(), ThreadId::generate());
    let request = ants_proto::SummaryRequest::thread(summarised.clone(), 42);
    for (from, thread) in [(UserId::human(), asked.clone()), (dev_b.clone(), ThreadId::generate())] {
        let mut envelope = Envelope::on_thread(thread, from, bus.dev.clone(), "tokens 7");
        envelope.summary = Some(request.clone());
        bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.thread_id, asked);
        let spent = ants_proto::SummaryRequest { tokens: Some(7), ..request.clone() };
        assert_eq!(reply.summary, Some(spent), "the reply carries the request and its tokens");
        assert_eq!(next_envelope(&mut to_dev_b).await.summary, None, "only a human asks for a summary");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    // The two threads are prompted in either order.
    let mut prompts = log.locked().prompts.clone();
    prompts.sort_by_key(|p| p == "tokens 7");
    let tldr = format!("[ants] A human asks for a TL;DR of thread {summarised} from the entries below");
    assert!(prompts[0].starts_with(&tldr), "{prompts:?}");
    assert!(prompts[0].contains("use no tools and message no one") && prompts[0].ends_with("\n\ntokens 7"));
    assert_eq!(prompts[1], "tokens 7", "an ant's request is an ordinary message");
}

#[tokio::test]
async fn progress_streams_to_the_status_subject() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut statuses = bus.client.subscribe(subject::status(&bus.dev, &t1)).await.unwrap();
    bus.client.flush().await.unwrap();
    bus.send(&t1, "tools 1").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "tools 1".to_owned()));
        let mut updates = Vec::new();
        while !matches!(updates.last(), Some(StatusUpdate::State { state: State::Idle })) {
            let msg = timeout(WAIT, statuses.next()).await.expect("no status").unwrap();
            let event: StatusEvent = serde_json::from_slice(&msg.payload).unwrap();
            assert_eq!(event.check_subject(&msg.subject), Ok(()));
            // A flush may fall between the chunks of the reply.
            match (updates.last_mut(), event.update) {
                (Some(StatusUpdate::Message { text }), StatusUpdate::Message { text: more }) => text.push_str(&more),
                (_, update) => updates.push(update),
            }
        }
        let tool = StatusUpdate::Tool {
            id: "call-0".to_owned(),
            title: Some("tool".to_owned()),
            tool_kind: Some("other".to_owned()),
            status: Some("pending".to_owned()),
            paths: None,
        };
        assert_eq!(
            updates,
            [
                StatusUpdate::State { state: State::Working },
                tool,
                StatusUpdate::Message { text: "s1: tools 1".to_owned() },
                StatusUpdate::State { state: State::Idle },
            ]
        );
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn tool_calls_are_stored_on_the_thread_timeline() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (sent, _) = bus.send(&t1, "tools 2").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "tools 2".to_owned()));
        let subject = subject::timeline(&t1, &bus.dev);
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
        assert_eq!(record.message, sent.id);
        let calls: Vec<_> = record
            .entries
            .iter()
            .map(|e| match e {
                ants_proto::TimelineEntry::Tool(call) => {
                    (call.id.as_str(), call.title.as_str(), call.status.as_deref())
                }
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(calls, [("call-0", "tool", Some("pending")), ("call-1", "tool", Some("pending"))]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn messages_waiting_on_their_thread_hear_where_they_stand() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut statuses = bus.client.subscribe(subject::status(&bus.dev, &t1)).await.unwrap();
    bus.client.flush().await.unwrap();
    bus.send(&t1, "hold a").await;
    let (b, _) = bus.send(&t1, "b").await;
    let (c, _) = bus.send(&t1, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut queued = Vec::new();
        while queued.len() < 5 {
            let msg = timeout(WAIT, statuses.next()).await.expect("no status").unwrap();
            let event: StatusEvent = serde_json::from_slice(&msg.payload).unwrap();
            if let StatusUpdate::Queued { message, ahead } = event.update {
                queued.push((message, ahead));
            }
            if queued.len() == 2 {
                gate.add_permits(1);
            }
        }
        // As they arrive, then as the queue moves: b's turn comes, c waits behind it, then c's turn comes.
        assert_eq!(queued, [(b.id, 1), (c.id, 2), (b.id, 0), (c.id, 1), (c.id, 0)]);
        for _ in 0..3 {
            next_reply(&mut replies).await;
        }
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_failed_message_is_retried_before_the_rest_of_its_thread() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "flaky a").await;
    bus.send(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // The failed attempt may have run tools too, so the retry carries the warning.
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), warned("flaky a")));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, [warned("flaky a"), "b".to_owned()]);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_prompt_too_large_even_for_a_new_session_goes_to_the_owner_with_the_thread_history_tried() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    bus.js.create_stream(crate::hub::files_config()).await.unwrap();
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut hello = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "hello");
    let to = hello.clone();
    let notes = attach(&bus, |id| to.attachment_handle(id), "notes.txt", "text/plain", b"notes").await;
    hello.attachments = vec![notes.clone()];
    bus::publish_json(&bus.js, hello.subject(), &hello, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await.0, t1);
        bus.send(&t1, "overflows").await;
        let notice = next_envelope(&mut replies).await;
        let why = "The prompt does not fit the context window of test's model, even in a new session: ";
        assert!(notice.text.starts_with(why), "{}", notice.text);
        assert!(notice.text.contains("Prompt is too long"), "{}", notice.text);
        assert!(notice.text.ends_with("; needs a decision. Last message from human:\noverflows"), "{}", notice.text);
        assert_eq!(notice.thread_id, t1);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let line = format!(
        "[ants] Attached: \"notes.txt\" (text/plain, 5 bytes), handle human.inbox.dev-a.{}; call read_attachment with \
         the handle to read it.",
        notes.id
    );
    // The fake harness echoes its prompt, the attachment's line included.
    let history = format!(
        "[ants] The thread's history, read back from ants, oldest first:\n\nhuman to dev-a:\nhello\n{line}\n\n\
         dev-a to human:\ns1: hello\n\n{line}\n\n[ants] End of the thread's history.\n\noverflows"
    );
    let prompts = log.locked().prompts.clone();
    assert_eq!(prompts.len(), 3, "{prompts:?}");
    assert!(prompts[2].ends_with(&history), "{}", prompts[2]);
    assert_eq!(bus.unacked().await, 0);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty(), "no dead letter");
}

#[tokio::test]
async fn messages_wait_unspent_while_the_harness_is_signed_out() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().signed_out = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let mut statuses = bus.client.subscribe(subject::status_of(&bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let (_, seq) = bus.send(&t1, "a").await;
    bus.send(&t1, "b").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let signed_out = |notice: &Envelope| {
        assert_eq!(notice.from, bus.dev);
        assert!(notice.text.contains("harness test on") && notice.text.contains("is not signed in"), "{notice:?}");
        assert!(notice.text.contains("Sign test in on"), "{}", notice.text);
    };
    let check = async {
        let notice = next_envelope(&mut replies).await;
        signed_out(&notice);
        assert!([&t1, &t2].contains(&&notice.thread_id));
        loop {
            let msg = timeout(WAIT, statuses.next()).await.expect("no signed_out state").unwrap();
            let event: StatusEvent = serde_json::from_slice(&msg.payload).unwrap();
            if event.update == (StatusUpdate::State { state: State::SignedOut }) {
                break;
            }
        }
        let shown = bus.workload.signed_out().expect("the roster shows the harness signed out");
        assert_eq!(shown.sign_in, None, "the test harness names no sign-in command");
        // Several checks of the harness pass, telling nobody again and spending no attempt.
        let starts = log.locked().starts;
        until("checks of the harness", || log.locked().starts >= starts + 3).await;
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, 0);
        assert_eq!(log.locked().prompted, 0);

        log.locked().signed_out = false;
        let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
        answered.push(next_reply(&mut replies).await);
        answered.sort_by_key(|(_, text)| text.clone());
        let expected = [(t1.clone(), "a"), (t1.clone(), "b"), (t2.clone(), "c")].map(|(t, s)| (t, s.to_owned()));
        assert_eq!(answered, expected, "answered unwarned: no attempt started");
        assert_eq!(bus.workload.signed_out(), None, "signed in again: the roster no longer shows it");

        // Signed out again once it worked: a live harness turns the prompt away, and a human is told again.
        log.locked().signed_out = true;
        bus.send(&t1, "d").await;
        signed_out(&next_envelope(&mut replies).await);
        assert!(bus.workload.signed_out().is_some_and(|s| s.since > shown.since), "a new signed-out period");
        log.locked().signed_out = false;
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "d".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let mut answered = log.locked().answered.clone();
    answered.sort();
    assert_eq!(answered, ["a", "b", "c", "d"]);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_fallback_answers_while_the_harness_is_signed_out() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let (spare, spare_log, _) = fake::launch();
    let spare = spare.named("spare");
    log.locked().signed_out = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE).with_fallbacks(vec![("spare".to_owned(), spare)]);
    let thread = ThreadId::generate();
    let (_, seq) = bus.send(&thread, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert!(notice.text.contains("cannot answer with its own harness: harness test on"), "{}", notice.text);
        assert!(notice.text.contains("go to its fallback spare meanwhile, and back to the harness test"), "{notice:?}");
        let reply = next_envelope(&mut replies).await;
        assert_eq!(reply.thread_id, thread);
        assert!(
            reply.text.ends_with("a\n\n[ants] Answered by fallback spare, harness test being signed out."),
            "{reply:?}"
        );
        let shown = bus.workload.fallback().expect("the roster shows the fallback");
        assert_eq!(shown.name, "spare");
        assert!(bus.workload.signed_out().is_some(), "the harness itself is still signed out");
        bus.until_acked(seq).await;

        // The fallback turned away too: the message waits, the owner told once, until one answers.
        let mut passed_over = async |text: &str| {
            spare_log.locked().signed_out = true;
            bus.send(&thread, text).await;
            let notice = next_envelope(&mut replies).await;
            assert_eq!(notice.thread_id, thread);
            assert!(
                notice
                    .text
                    .starts_with("[ants] dev-a cannot answer with a fallback either: fallback spare is signed out"),
                "{}",
                notice.text
            );
            assert!(notice.text.contains("until harness test, signed out, answers again or a fallback"), "{notice:?}");
            let starts = spare_log.locked().starts;
            until("the fallback tried again", || spare_log.locked().starts > starts).await;
            assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
            spare_log.locked().signed_out = false;
            let reply = next_envelope(&mut replies).await;
            assert!(reply.text.contains(&format!("{text}\n\n[ants] Answered by fallback spare")), "{reply:?}");
        };
        passed_over("b").await;
        assert_eq!(bus.workload.fallback().map(|f| f.since), Some(shown.since), "the same outage");
        passed_over("b2").await;

        // Signed in again: the thread goes back to the ant's own harness, rebuilt from its history.
        log.locked().signed_out = false;
        until("the fallback stood down", || bus.workload.fallback().is_none()).await;
        bus.send(&thread, "c").await;
        let reply = next_envelope(&mut replies).await;
        assert!(reply.text.ends_with("\nc"), "answered by the ant's own harness, with no footer: {reply:?}");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(spare_log.locked().answered, ["a", "b", "b2"]);
    let answered = log.locked().answered.clone();
    let [rebuilt] = answered.as_slice() else { panic!("{answered:?}") };
    assert!(rebuilt.contains("ran in another harness") && rebuilt.ends_with('c'), "{rebuilt}");
    assert!(log.locked().loaded.is_empty(), "the fallback's session is not loaded in another harness");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn messages_wait_unspent_while_the_harness_keeps_failing_to_start() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().broken = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (_, seq) = bus.send(&t1, "a").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // Failures on both threads open the breaker before either message runs out of attempts.
        let notice = next_envelope(&mut replies).await;
        assert_eq!(notice.from, bus.dev);
        assert!(notice.text.contains("ants harness check --harness test"), "{}", notice.text);
        let escalation = notice.escalation.as_ref().expect("pushed as an escalation");
        assert_eq!((escalation.reason, escalation.level), (EscalationReason::Harness, 0));
        let shown = bus.workload.failing().expect("the roster shows the harness failing");
        assert!(shown.error.contains("broken install"), "{}", shown.error);
        let redelivered = bus.consumer().await.num_redelivered;
        assert!(redelivered < 3, "{redelivered}");
        // Several checks of the harness fail, telling nobody again and spending no attempt.
        let starts = log.locked().starts;
        until("checks of the harness", || log.locked().starts >= starts + 3).await;
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, redelivered);
        assert!(bus.workload.failing().is_some_and(|f| f.since == shown.since), "the same failing period");

        log.locked().broken = false;
        let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
        answered.sort_by_key(|(_, text)| text.clone());
        assert_eq!(answered, [(t1.clone(), "a".to_owned()), (t2.clone(), "c".to_owned())]);
        assert_eq!(bus.workload.failing(), None, "started again: the roster no longer shows it");

        // Once messages get through, a lone failure is tried again as before, holding nothing.
        log.locked().broken = true;
        let t3 = ThreadId::generate();
        let starts = log.locked().starts;
        bus.send(&t3, "d").await;
        until("a failed start", || log.locked().starts > starts).await;
        log.locked().broken = false;
        assert_eq!(next_reply(&mut replies).await, (t3.clone(), "d".to_owned()));
        assert_eq!(bus.workload.failing(), None);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let mut answered = log.locked().answered.clone();
    answered.sort();
    assert_eq!(answered, ["a", "c", "d"]);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn messages_wait_unspent_through_a_provider_rate_limit() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().limited = Some("API Error: 429 Too Many Requests, retry after 0.2s".to_owned());
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (_, seq) = bus.send(&t1, "a").await;
    bus.send(&t1, "b").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("prompts tried again", || log.locked().prompted >= 4).await;
        // Each backoff clears the limit until the next try meets it again.
        let shown = timeout(WAIT, async {
            loop {
                if let Some(shown) = bus.workload.limited() {
                    break shown;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the roster shows the limit");
        assert_eq!(shown.kind, LimitKind::RateLimit);
        assert!(shown.until > shown.since);
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "a rate limit tells nobody");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, 0);

        log.locked().limited = None;
        let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
        answered.push(next_reply(&mut replies).await);
        answered.sort_by_key(|(_, text)| text.clone());
        let expected = [(t1.clone(), "a"), (t1.clone(), "b"), (t2.clone(), "c")].map(|(t, s)| (t, s.to_owned()));
        assert_eq!(answered, expected, "answered unwarned: no attempt started");
        until("the limit cleared", || bus.workload.limited().is_none()).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn an_exhausted_quota_is_told_once_and_spends_no_attempt() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    // Copilot CLI's reply when the provider turns the prompt away, which is no answer.
    let error = "Error: Failed to get response from the AI model; retried 5 times (total retry wait time: 56.70 \
                     seconds) Last error: 429 You exceeded your current quota, please check your plan and billing \
                     details.";
    {
        let mut faked = log.locked();
        (faked.limited, faked.limited_reply) = (Some(error.to_owned()), true);
    }
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let (_, seq) = bus.send(&thread, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert_eq!(notice.thread_id, thread);
        assert!(notice.text.contains("harness test on") && notice.text.contains("out of quota"), "{notice:?}");
        assert!(notice.text.contains("spending no attempts"), "{}", notice.text);
        assert_eq!(bus.workload.limited().map(|l| l.kind), Some(LimitKind::Quota));
        until("prompts tried again", || log.locked().prompted >= 3).await;
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, 0);

        log.locked().limited = None;
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), "a".to_owned()));
        until("the limit cleared", || bus.workload.limited().is_none()).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["a"]);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_forged_sender_is_dead_lettered_unanswered() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    // dev-b sends on its own subject, but the envelope claims the human sent it.
    let forged = Envelope::on_thread(thread.clone(), UserId::human(), bus.dev.clone(), "forged");
    let dev_b: UserId = "dev-b".parse().unwrap();
    crate::bus::publish_json(&bus.js, subject::inbox(&bus.dev, &dev_b), &forged, None).await.unwrap();
    bus.send(&thread, "genuine").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), "genuine".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["genuine"]);
    let letters = dead::list(&bus.stream).await.unwrap();
    let [(_, letter)] = letters.as_slice() else { panic!("{letters:?}") };
    assert!(letter.error.contains("sender mismatch"), "{}", letter.error);
    assert_eq!(letter.envelope, None, "a forged envelope is not replayable");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn an_approval_decision_is_acked_without_a_prompt() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let dev_b: UserId = "dev-b".parse().unwrap();
    let decision =
        ants_proto::ApprovalDecision::new(ants_proto::Uuid::new_v4(), bus.dev.clone(), ants_proto::Verdict::AllowOnce);
    let late = Envelope::decision(thread.clone(), dev_b, decision);
    crate::bus::publish_json(&bus.js, late.subject(), &late, None).await.unwrap();
    bus.send(&thread, "genuine").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), "genuine".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["genuine"]);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn inbox_consumers_from_before_sender_subjects_get_the_new_filter() {
    let Some((bus, _)) = Bus::start().await else { return };
    let name = ants_proto::inbox_consumer(&bus.dev);
    let old = jetstream::consumer::pull::Config {
        durable_name: Some(name.clone()),
        filter_subject: format!("ants.inbox.{}", bus.dev),
        ..Default::default()
    };
    bus.stream.create_consumer(old).await.unwrap();
    drop(crate::bus::inbox_consumer(&bus.stream, &bus.dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap());
    let mut consumer: PullConsumer = bus.stream.get_consumer(&name).await.unwrap();
    assert_eq!(consumer.info().await.unwrap().config.filter_subject, subject::inbox_of(&bus.dev));
}

#[tokio::test]
async fn ctrl_c_finishes_prompts_in_flight_and_a_restart_resumes() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;

    let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let stop_mid_prompt = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        ctrl_c.add_permits(1);
        gate.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), stop_mid_prompt);
    pool.shutdown().await;
    assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
    assert_eq!(log.locked().answered, ["hold a"], "b was handed back, not prompted");

    let pool = Pool::new(launch, store, 2, IDLE);
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().loaded, ["s1"], "the restarted daemon resumed the thread's session");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn progress_acks_keep_messages_in_hand_past_the_ack_wait() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));
    // A consumer left by a daemon configured with a longer ack wait takes the new one.
    drop(bus::inbox_consumer(&bus.stream, &bus.dev, ack_wait * 100, MAX_DELIVER, 4).await.unwrap());
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        assert_eq!(bus.consumer().await.config.ack_wait, ack_wait);
        tokio::time::sleep(ack_wait * 4).await;
        let consumer = bus.consumer().await;
        assert_eq!((consumer.num_redelivered, consumer.num_ack_pending), (0, 2), "neither message came back");
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_redelivery_of_a_message_in_hand_is_not_prompted_again() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;

    // Progress acks lost, as on a silent link: both messages come back while still in hand.
    let ack_wait = Duration::from_millis(300);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        timeout(WAIT, async {
            while bus.consumer().await.num_redelivered < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no redelivery");
        gate.add_permits(10);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        assert!(timeout(ack_wait * 3, replies.next()).await.is_err(), "no duplicate reply");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, Duration::from_secs(3600), ack_wait), check);
    assert_eq!(log.locked().answered, ["hold a", "b"]);
    assert_eq!(bus.unacked().await, 0, "acks of the first delivery count");
}

#[tokio::test]
async fn a_prompt_past_its_limit_is_cancelled_and_the_human_told() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.limits.timeout = Some(Duration::from_millis(100));
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;
    bus.send(&t1, "after").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        let text = "partial\n\n[ants] Prompt time limit reached (100ms); the prompt was cancelled.";
        assert_eq!(notice.text, text);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "after".to_owned()), "and nothing else");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().cancelled, 1);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_reply_names_the_thread_branch_holding_new_commits() {
    let Some(repo) = crate::worktrees::tests::repo("daemon-branch-ahead") else { return };
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let home = repo.parent().unwrap().join("home");
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let setup = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let worktree = setup.checkout(&t1).await.unwrap();
    crate::worktrees::tests::commit(&worktree, "work of t1");
    let (launch, _, _) = fake::launch();
    let worktrees = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 2, IDLE).with_worktrees(worktrees);
    bus.send(&t1, "one").await;
    bus.send(&t2, "two").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut texts = HashMap::new();
        for _ in 0..2 {
            let reply = next_envelope(&mut replies).await;
            texts.insert(reply.thread_id, reply.text);
        }
        let named = format!("one\n\n[ants] Branch ants/dev-a/{t1} holds 1 commit not in the repository's HEAD.");
        assert!(texts[&t1].ends_with(&named), "{}", texts[&t1]);
        assert!(texts[&t2].ends_with("two"), "no commits on its branch: {}", texts[&t2]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

/// A fake gh in `dir`, logging its arguments to `dir/gh.log`: `pr create` prints a pull request's URL, which `pr list`
/// prints from then on.
#[cfg(unix)]
fn fake_gh(dir: &std::path::Path) -> PathBuf {
    let gh = dir.join("gh");
    let script = r#"#!/bin/sh
dir=$(dirname "$0")
printf '[%s]' "$@" >> "$dir/gh.log"
echo >> "$dir/gh.log"
case "$1 $2" in
"pr list") cat "$dir/open" 2>/dev/null || true ;;
"pr create") echo "Creating draft pull request"; echo https://github.test/o/r/pull/7 | tee "$dir/open" ;;
esac
"#;
    std::fs::write(&gh, script).unwrap();
    std::fs::set_permissions(&gh, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    gh
}

#[cfg(unix)]
#[tokio::test]
async fn a_thread_branch_with_new_commits_is_pushed_with_a_draft_pull_request() {
    use crate::worktrees::tests::{commit, run};
    let Some(repo) = crate::worktrees::tests::repo("daemon-publish") else { return };
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    let dir = repo.parent().unwrap().to_owned();
    let remote = dir.join("remote.git");
    run(&dir, &["init", "--quiet", "--bare", remote.to_str().unwrap()]);
    run(&repo, &["remote", "add", "origin", remote.to_str().unwrap()]);
    bus.js.create_stream(crate::hub::threads_config()).await.unwrap();
    let titles = crate::direct_kv::DirectKv::new(&bus.js, None, ants_proto::THREADS_BUCKET).await.unwrap();
    bus.publisher = Some(crate::publish::Publisher::new(bus.dev.clone()).with_gh(fake_gh(&dir)).with_titles(titles));
    let home = dir.join("home");
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let mut title = ants_proto::ThreadTitle::new(t1.clone(), "Lunch plans", UserId::human()).unwrap();
    let sealed =
        ants_proto::Sealed { v: 1, device: ants_proto::DeviceId::generate(), keys: Vec::new(), body: title.title };
    (title.title, title.sealed) = (String::new(), Some(Box::new(sealed)));
    let key = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::title_key(&t1));
    bus.js.publish(key, serde_json::to_vec(&title).unwrap().into()).await.unwrap().await.unwrap();
    let setup = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let (w1, w2) = (setup.checkout(&t1).await.unwrap(), setup.checkout(&t2).await.unwrap());
    commit(&w1, "work of t1");
    commit(&w2, "work of t2");
    let (launch, _, _) = fake::launch();
    let allow = Allowlist::parse(&["execute:git push origin ants/dev-a/*"]).unwrap();
    let launch = launch.in_dir(repo.clone()).with_policy(PermissionPolicy::new(allow, None));
    let worktrees = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE).with_worktrees(worktrees);
    let branch = format!("ants/dev-a/{t1}");

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        bus.send(&t1, "  Fix the build\nthen test it").await;
        let pushed = format!("[ants] Branch {branch} was pushed to origin, with draft pull request ");
        let text = next_envelope(&mut replies).await.text;
        assert!(text.ends_with(&format!("{pushed}https://github.test/o/r/pull/7.")), "{text}");
        assert_eq!(run(&repo, &["ls-remote", "--heads", "origin"]).matches(&branch).count(), 1);
        let log = std::fs::read_to_string(dir.join("gh.log")).unwrap();
        assert!(log.starts_with(&format!("[pr][list][--head][{branch}][--state][open]")), "{log}");
        let titled = log.contains("[--title][Fix the build][--body]");
        assert!(log.contains("[--draft][--head]") && titled, "the opening line, as the title is sealed: {log}");
        assert!(log.contains(&format!("thread {t1}, which `ants thread show {t1}` shows")), "{log}");

        bus.send(&t1, "nothing new").await;
        let text = next_envelope(&mut replies).await.text;
        assert!(text.ends_with("not in the repository's HEAD."), "pushed as it is: {text}");
        commit(&w1, "more work of t1");
        bus.send(&t1, "more").await;
        let text = next_envelope(&mut replies).await.text;
        let updated = format!("[ants] Branch {branch} was pushed to origin, updating pull request ");
        assert!(text.ends_with(&format!("{updated}https://github.test/o/r/pull/7.")), "{text}");
        assert_eq!(std::fs::read_to_string(dir.join("gh.log")).unwrap().matches("[pr][create]").count(), 1);

        // Another ant's message narrows the thread's rules to what it carries.
        let dev_b: UserId = "dev-b".parse().unwrap();
        let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
        bus.client.flush().await.unwrap();
        let narrowed =
            Envelope::on_thread(t2.clone(), dev_b, bus.dev.clone(), "two").with_allow(Some(vec!["read".to_owned()]));
        bus::publish_json(&bus.js, narrowed.subject(), &narrowed, None).await.unwrap();
        let text = next_envelope(&mut to_dev_b).await.text;
        let rejected = format!(
            "[ants] Branch ants/dev-a/{t2} was not pushed: `git push origin ants/dev-a/{t2}` was rejected. An allow \
             rule such as `execute:git push origin ants/dev-a/*` lets it run unasked."
        );
        assert!(text.ends_with(&rejected), "outside the thread's narrowed rules: {text}");
        assert!(!run(&repo, &["ls-remote", "--heads", "origin"]).contains(t2.as_str()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_kept_branch_is_told_to_the_thread_owner_else_the_default_human() {
    let humans = Registry::new("alice".parse().unwrap());
    let (ant, thread): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
    let branch = Branch { thread: thread.clone(), name: "ants/dev-a/t1".to_owned(), commits: 2, upstream: None };
    let notice = kept_branch_notice(&ant, &branch, None, &humans);
    assert_eq!((notice.to.as_str(), notice.owner.as_ref(), &notice.thread_id), ("alice", None, &thread));
    assert_eq!(
        notice.text,
        "[ants] The session of thread t1 expired and its git worktree was removed; branch ants/dev-a/t1 is kept, \
             holding 2 commits not in the repository's HEAD."
    );
    let notice = kept_branch_notice(&ant, &branch, Some(UserId::human()), &humans);
    assert_eq!((&notice.to, notice.owner.as_ref()), (&UserId::human(), Some(&UserId::human())));
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

#[tokio::test]
async fn a_human_cancels_a_running_prompt_and_the_sender_gets_what_it_wrote() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(dev_b.clone(), &t1, "stall").await;
    bus.send_from(dev_b.clone(), &t1, "after").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert!(!cancel(&bus, &ThreadId::generate()).await, "no prompt runs on another thread");
        until("the prompt to run", || log.locked().prompted == 1).await;
        assert!(cancel(&bus, &t1).await);
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!(reply.text, "partial\n\n[ants] The prompt was cancelled by human (ants cancel).");
        assert_eq!(next_reply(&mut to_dev_b).await, (t1.clone(), "after".to_owned()), "the thread goes on");
        assert!(!cancel(&bus, &t1).await, "nothing left to cancel");
        assert!(timeout(Duration::from_millis(300), to_human.next()).await.is_err(), "no escalation");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().cancelled, 1);
    assert_eq!(log.locked().prompted, 2, "not retried");
    assert_eq!(bus.unacked().await, 0);
}

/// A clone's snapshot of dev-a is read once dev-a is paused and the prompt it runs ended: each thread it has a session
/// of, with the thread's history, and how its harness starts.
#[tokio::test]
async fn a_clone_s_snapshot_waits_for_the_pause_and_the_prompt_running() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold one").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let dev = bus.dev.clone();
        let _served = bus.restarts.serve(&dev);
        until("the held prompt to run", || log.locked().prompted == 1).await;
        until("the daemon to hear clones", || bus.restarts.hears_clones(&dev)).await;
        let mut snapshot = std::pin::pin!(bus.restarts.snapshot(&dev, "dev-c".parse().unwrap()));
        assert!(timeout(Duration::from_millis(500), &mut snapshot).await.is_err(), "not before the pause");
        bus.pause.replace(&crate::pause::Pause::new(true));
        assert!(timeout(Duration::from_millis(500), &mut snapshot).await.is_err(), "not while the prompt runs");
        gate.add_permits(1);
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "hold one".to_owned()));
        let snapshot = timeout(WAIT, snapshot).await.expect("read once the prompt ended").expect("served").unwrap();
        assert_eq!(snapshot.threads.iter().map(|copy| &copy.thread).collect::<Vec<_>>(), [&t1]);
        assert!(snapshot.threads[0].history.contains("hold one"), "{}", snapshot.threads[0].history);
        assert_eq!(snapshot.threads[0].stored.session.to_string(), "s1");
        assert!(snapshot.launch.is_some());
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

/// Have the daemon restart dev-a's harnesses for alice once it serves the inbox.
async fn restart(bus: &Bus, now: bool) -> Restarted {
    let (dev, alice) = (bus.dev.clone(), "alice".parse::<UserId>().unwrap());
    let _served = bus.restarts.serve(&dev);
    timeout(WAIT, async {
        while !bus.restarts.is_listening(&dev) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bus.restarts.restart(&dev, alice, now).await.expect("dev-a is served")
    })
    .await
    .expect("the daemon heard no restart")
}

#[tokio::test]
async fn a_restart_stops_idle_harnesses_and_working_ones_after_their_prompt() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "one").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // One thread after the other, so t2's session is s2.
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "one".to_owned()));
        bus.send(&t2, "hold two").await;
        until("the held prompt to run", || log.locked().prompted == 2).await;
        until("the first message to be handled", || bus.workload.handling() == 1).await;
        assert_eq!(restart(&bus, false).await, Restarted { stopped: 1, after_prompt: 1, cancelled: 0 });
        gate.add_permits(1);
        assert_eq!(next_reply(&mut to_human).await, (t2.clone(), "hold two".to_owned()), "the working prompt ends");
        bus.send(&t2, "again").await;
        assert_eq!(next_reply(&mut to_human).await, (t2.clone(), "again".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.starts, log.cancelled), (3, 0), "a new harness, no prompt cancelled");
        assert_eq!(log.loaded, ["s2"], "it resumes the session");
    }
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_restart_now_cancels_the_running_prompt_and_the_sender_gets_what_it_wrote() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;
    bus.send(&t1, "after").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        assert_eq!(restart(&bus, true).await, Restarted { stopped: 0, after_prompt: 1, cancelled: 1 });
        let reply = next_envelope(&mut to_human).await;
        let notice = "[ants] The prompt was cancelled by alice to restart dev-a's harnesses (ants ant restart --now).";
        assert_eq!(reply.text, format!("partial\n\n{notice}"));
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "after".to_owned()), "the thread goes on");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.starts, log.cancelled, log.prompted), (2, 1, 2), "not retried, on a new harness");
        assert_eq!(log.loaded, ["s1"], "it resumes the session");
    }
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_human_steers_a_running_prompt_ahead_of_the_queue_in_the_same_session() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;
    bus.send(&t1, "queued").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        steer(&bus, UserId::human(), &t1, "go left").await;
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.text, "partial\n\n[ants] human interrupted the prompt to steer dev-a (ants ask --steer).");
        let steered = next_envelope(&mut to_human).await;
        let note = "[ants] human interrupted the previous prompt on this thread with this message; tool calls it had \
                    in flight were cancelled, so check what they left before repeating any.";
        assert_eq!(steered.text, format!("s1: {note}\n\ngo left"), "the same session takes the steer first");
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "queued".to_owned()), "the queue waits");
        assert!(timeout(Duration::from_millis(300), to_human.next()).await.is_err(), "no escalation");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.cancelled, log.prompted, log.new_sessions), (1, 3, 1), "not retried");
        assert!(log.loaded.is_empty(), "the session goes on without a load");
    }
    assert_eq!(bus.unacked().await, 0);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty(), "no dead letter");
}

#[tokio::test]
async fn a_steer_with_no_prompt_running_or_from_an_ant_waits_its_turn() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    steer(&bus, UserId::human(), &t1, "idle").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "idle".to_owned()), "an ordinary prompt");
        bus.send(&t1, "stall").await;
        until("the prompt to run", || log.locked().prompted == 2).await;
        steer(&bus, dev_b.clone(), &t1, "from an ant").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(log.locked().cancelled, 0, "an ant does not steer");
        assert!(cancel(&bus, &t1).await);
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.text, "partial\n\n[ants] The prompt was cancelled by human (ants cancel).");
        assert_eq!(next_reply(&mut to_dev_b).await, (t1.clone(), "from an ant".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

/// Send `text` on `thread` from `from` as a steer.
async fn steer(bus: &Bus, from: UserId, thread: &ThreadId, text: &str) {
    let envelope = Envelope::on_thread(thread.clone(), from, bus.dev.clone(), text).steering();
    bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
}

/// Ask the daemon which messages it holds until it answers, as the daemon may not listen yet.
async fn queue(bus: &Bus) -> ants_proto::QueueReply {
    queue_with(bus, QueueRequest::new(bus.dev.clone(), UserId::human())).await
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

/// Withdraw `message` as the human, returning the mark found instead if the withdrawal lost.
async fn withdraw(bus: &Bus, message: &Envelope) -> Option<ants_proto::MessageMark> {
    crate::attempts::withdraw(&bus.markers, &bus.dev, &message.id, &UserId::human(), None).await.unwrap()
}

async fn mark_of(bus: &Bus, message: &Envelope) -> Option<ants_proto::MessageMark> {
    let mark = bus.markers.get(ants_proto::message_mark_key(&bus.dev, &message.id)).await.unwrap();
    mark.map(|mark| serde_json::from_slice(&mark).unwrap())
}

const WITHDRAWN: &str = "[ants] Withdrawn by human before dev-a was prompted with it (ants withdraw).";

#[tokio::test]
async fn a_queued_message_is_listed_and_withdrawn_while_the_running_prompt_goes_on() {
    use ants_proto::MessageMark;
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, a_seq) = bus.send(&t1, "hold a").await;
    let (b, b_seq) = bus.send(&t1, "b").await;
    let (c, c_seq) = bus.send(&t1, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let listed = timeout(WAIT, async {
            loop {
                let answer = queue(&bus).await;
                if answer.messages.len() == 3 {
                    return answer;
                }
            }
        })
        .await
        .expect("all three in hand");
        let at = |m: &QueuedMessage| (m.id, m.seq, m.running, m.ahead, m.from.clone(), m.thread_id.clone());
        let human = UserId::human();
        assert_eq!(
            listed.messages.iter().map(at).collect::<Vec<_>>(),
            [
                (a.id, a_seq, true, 0, human.clone(), t1.clone()),
                (b.id, b_seq, false, 1, human.clone(), t1.clone()),
                (c.id, c_seq, false, 2, human.clone(), t1.clone()),
            ]
        );
        assert_eq!((listed.delivered, listed.undelivered), (Some(c_seq), Some(0)));

        assert_eq!(withdraw(&bus, &b).await, None, "b is withdrawn");
        assert!(
            matches!(withdraw(&bus, &b).await, Some(MessageMark::Withdrawn { by, .. }) if by == human),
            "withdrawn once"
        );
        assert!(
            matches!(withdraw(&bus, &a).await, Some(MessageMark::Started { .. })),
            "a running prompt is cancelled, not withdrawn"
        );
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        let notice = next_envelope(&mut replies).await;
        assert_eq!((notice.text.as_str(), notice.in_reply_to, &notice.thread_id), (WITHDRAWN, Some(b.id), &t1));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "c".to_owned()), "the thread goes on");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["hold a", "c"], "b never reached the harness");
    assert_eq!(bus.unacked().await, 0);
    assert!(matches!(mark_of(&bus, &a).await, Some(MessageMark::Finished { .. })));
    assert!(matches!(withdraw(&bus, &c).await, Some(MessageMark::Finished { .. })), "c is answered already");
}

#[tokio::test]
async fn a_message_withdrawn_before_its_delivery_is_never_prompted() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (a, a_seq) = bus.send(&t1, "a").await;
    assert_eq!(withdraw(&bus, &a).await, None);
    bus.send(&t2, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut texts = HashMap::new();
        for _ in 0..2 {
            let reply = next_envelope(&mut replies).await;
            texts.insert(reply.thread_id, reply.text);
        }
        assert_eq!(texts[&t1], WITHDRAWN);
        assert!(texts[&t2].ends_with(": b"), "{}", texts[&t2]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    bus.until_acked(a_seq).await;
    assert_eq!(log.locked().answered, ["b"]);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_withdrawn_message_redelivered_after_a_crash_is_acked_unprompted() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (progress_every, ack_wait) = (Duration::from_millis(100), ack_wait(Duration::from_millis(100)));
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    let (b, _) = bus.send(&t1, "b").await;

    // Withdrawn while it waits in hand behind a, then the daemon stops mid-prompt: both are redelivered.
    let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let crash = async {
        until("the prompt to reach the harness", || log.locked().prompted == 1).await;
        assert_eq!(withdraw(&bus, &b).await, None);
        ctrl_c.add_permits(2);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), crash);
    drop(pool);

    let pool = Pool::new(launch, store, 2, IDLE);
    let check = async {
        // b, handed back at once, may come back before a.
        let mut texts = vec![next_envelope(&mut replies).await.text, next_envelope(&mut replies).await.text];
        texts.sort();
        assert_eq!(texts, [WITHDRAWN.to_owned(), format!("s1: {}", warned("hold a"))]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(log.locked().answered, [warned("hold a")], "b never reached the harness");
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), replies.next()).await.is_err(), "one notice");
}

#[tokio::test]
async fn a_message_held_for_a_sign_in_can_be_withdrawn() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().signed_out = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, _) = bus.send(&t1, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert!(notice.text.contains("is not signed in"), "{}", notice.text);
        assert_eq!(withdraw(&bus, &a).await, None, "turned away unprompted, a is not started");
        log.locked().signed_out = false;
        assert_eq!(next_envelope(&mut replies).await.text, WITHDRAWN);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(log.locked().answered.is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_first_delivery_is_prompted_as_sent_and_leaves_no_marker() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (_, seq) = bus.send(&t1, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "a".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["a"]);
    assert!(!bus.marked(seq).await, "the marker is removed with the ack");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_spent_cap_hands_the_thread_to_the_human_until_the_human_answers() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(dev_b.clone(), &t1, "cost 2 stall").await;
    bus.send_from(dev_b.clone(), &t1, "more").await;
    bus.send(&t1, "go on").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let cap = "Thread cost cap reached (2.00 of 1.00 since a human's last message)";
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!(reply.text, format!("partial\n\n[ants] {cap}; the prompt was cancelled."));
        let escalation = next_envelope(&mut to_human).await;
        assert_eq!(escalation.text, format!("{cap}; needs a decision. Last message from dev-b:\ncost 2 stall"));
        let escalation = next_envelope(&mut to_human).await;
        assert_eq!(escalation.text, format!("{cap}; needs a decision. Last message from dev-b:\nmore"));
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "go on".to_owned()), "a fresh allowance");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    let log = log.locked();
    assert_eq!((log.cancelled, log.answered.as_slice()), (1, &["go on".to_owned()][..]));
}

#[tokio::test]
async fn spend_nearing_a_cap_warns_the_human_once_on_a_thread_of_its_own() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let caps = Caps { thread_cost: Some(1.0), ..Caps::default() };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), caps).warn_at(80));
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(dev_b.clone(), &t1, "cost 0.85").await;
    bus.send_from(dev_b.clone(), &t1, "cost 0.9").await;
    bus.send(&t1, "done?").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let warning = next_envelope(&mut to_human).await;
        assert_eq!(warning.from, bus.dev);
        assert_ne!(warning.thread_id, t1, "a thread of its own, so a wait on the thread goes on");
        let start = format!("[ants] dev-a spent 0.85/1.00 (85%) of max_thread_cost on thread {t1} since");
        assert!(warning.text.starts_with(&start), "{}", warning.text);
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "done?".to_owned()), "no second warning");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn escalations_go_to_the_owner_and_a_registered_human_renews_the_allowance() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    let alice: UserId = "alice".parse().unwrap();
    // The registry looks for the bucket before it exists, then finds it.
    let _watching = bus.humans.watch(bus.js.clone(), Duration::from_millis(50));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let config = jetstream::kv::Config { bucket: ants_proto::HUMANS_BUCKET.to_owned(), ..Default::default() };
    let humans = bus.js.create_key_value(config).await.unwrap();
    let entry = ants_proto::HumanEntry::new(alice.clone());
    humans.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    until("alice to be a human", || bus.humans.is_human(&alice)).await;

    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    let mut to_alice = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus::ensure_owners(&bus.js).await.unwrap();
    // alice asked both on the thread.
    for to in [&bus.dev, &dev_b] {
        crate::owners::record(&bus.js, None, &t1, to, &alice).await.unwrap();
    }
    let mut go_on = Envelope::on_thread(t1.clone(), alice.clone(), bus.dev.clone(), "go on");
    go_on.turn_budget = 0;
    for envelope in [
        Envelope::on_thread(t1.clone(), dev_b.clone(), bus.dev.clone(), "cost 2 stall").with_owner(Some(alice.clone())),
        go_on,
    ] {
        bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let cap = "Thread cost cap reached (2.00 of 1.00 since a human's last message)";
        assert!(next_envelope(&mut to_dev_b).await.text.ends_with("the prompt was cancelled."));
        let escalation = next_envelope(&mut to_alice).await;
        assert_eq!((&escalation.to, &escalation.owner), (&alice, &Some(alice.clone())));
        assert_eq!(escalation.text, format!("{cap}; needs a decision. Last message from dev-b:\ncost 2 stall"));
        let reply = next_envelope(&mut to_alice).await;
        assert!(reply.text.ends_with(": go on"), "a fresh allowance: {}", reply.text);
        assert_eq!(reply.turn_budget, 0, "a reply to a human spends no turn, even with none left");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing went to human");
    assert_eq!(log.locked().answered, ["go on"]);
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

#[tokio::test]
async fn a_spent_budget_goes_up_the_chain_and_its_leader_renews_it_once_per_level() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let [lead, dev_b, boss, carol] = ants(["lead", "dev-b", "boss", "carol"]);
    bus.chain = Chain { escalate_to: vec![lead.clone(), dev_b.clone()], after: None, ..Chain::default() };
    bus.put_roster(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_lead, mut to_dev_b, mut to_boss] = inboxes(&bus, &[lead.clone(), dev_b.clone(), boss.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "no turns", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let (first, copy) = (next_envelope(&mut to_lead).await, next_envelope(&mut to_dev_b).await);
        let one = first.escalation.as_deref().expect("the escalation").clone();
        let visited = vec![bus.dev.clone(), lead.clone(), dev_b.clone()];
        assert_eq!((one.level, &one.leader, &one.stuck, &one.visited), (1, &lead, &bus.dev, &visited));
        assert_eq!((copy.escalation, &copy.text), (first.escalation.clone(), &first.text), "every member gets it");
        let head = format!("[ants] Escalation on thread {t1}, level 1: dev-a is stuck (turn budget spent).");
        assert!(first.text.starts_with(&head), "{}", first.text);
        assert!(first.text.contains("Sent to lead, dev-b; lead leads."), "{}", first.text);
        assert!(first.text.ends_with("Turn budget spent; needs a decision. Last message from carol:\nno turns"));

        // The leader answers with no turns left either: a fresh budget, once at this level.
        bus.send_spent(&lead, &t1, "go on", Some(&one)).await;
        bus.send_spent(&lead, &t1, "and on", Some(&one)).await;
        let reply = next_envelope(&mut to_lead).await;
        assert!(reply.text.ends_with(": go on"), "{}", reply.text);
        assert_eq!(reply.escalation.as_deref(), Some(&one), "the answer carries the escalation");

        // The next spend goes one level higher, to the leader's chain in the roster.
        let up = next_envelope(&mut to_boss).await;
        let two = up.escalation.as_deref().expect("the escalation").clone();
        assert_eq!((two.level, &two.leader, &two.stuck), (2, &boss, &bus.dev));
        assert!(up.text.contains("level 2: dev-a is stuck (turn budget spent). Sent to boss; boss leads."));
        assert!(up.text.ends_with("Last message from lead:\nand on"), "{}", up.text);
        bus.send_spent(&boss, &t1, "from boss", Some(&two)).await;
        assert!(next_envelope(&mut to_boss).await.text.ends_with(": from boss"));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert_eq!(log.locked().answered, ["go on", "from boss"]);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing went to the human");
    assert!(timeout(Duration::from_millis(50), to_dev_b.next()).await.is_err(), "dev-b got level 1 only");
    let pending = bus.ledger.pending(&t1).await.unwrap().expect("the escalation is kept");
    assert_eq!((pending.escalation.level, pending.to, pending.renewed_by.is_some()), (2, vec![boss], true));
}

#[tokio::test]
async fn the_leader_s_answer_to_a_channel_thread_out_of_turns_is_posted_back_with_fresh_turns() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    let [lead, dev_b] = ants(["lead", "dev-b"]);
    bus.chain = Chain { escalate_to: vec![lead.clone()], after: None, ..Chain::default() };
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut posts) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let root = ChannelPost::new(ops.clone(), UserId::human(), "deploy friday?");
    bus.post(&root).await;
    let mut spent = root.reply(dev_b.clone(), "@dev-a agree?", &[], &Humans::default()).unwrap();
    spent.turn_budget = 0;
    bus.post(&spent).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut to_lead).await;
        let one = notice.escalation.as_deref().expect("the escalation").clone();
        assert_eq!(notice.thread_id, root.thread_id);
        let mut answer = notice.reply(lead.clone(), "go on", Role::Ant).unwrap();
        answer = answer.with_escalation(Some(one));
        answer.turn_budget = 0;
        bus::publish_json(&bus.js, answer.subject(), &answer, None).await.unwrap();

        // Posted back to the thread with the leader's fresh budget, not sent to the leader.
        let resumed = next_post(&mut posts).await;
        assert_eq!((&resumed.channel, &resumed.thread_id), (&ops, &root.thread_id));
        assert_eq!((resumed.turn_budget, resumed.owner.as_ref()), (DEFAULT_TURN_BUDGET - 1, Some(&root.from)));

        // Later posts on the thread go by the fresh budget: no escalation again.
        let more = resumed.reply(dev_b.clone(), "@dev-a and then?", &[], &Humans::default()).unwrap();
        bus.post(&more).await;
        let reply = next_post(&mut posts).await;
        assert_eq!((reply.in_reply_to, reply.turn_budget), (Some(more.id), DEFAULT_TURN_BUDGET - 3));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_lead.next()).await.is_err(), "the leader gets no reply");
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 2, "{answered:?}");
    let note = format!("on that thread and spends one of its turns ({} left after it)", DEFAULT_TURN_BUDGET - 1);
    assert!(answered[0].starts_with("go on\n\n[ants] The escalation's leader answered"), "{}", answered[0]);
    assert!(answered[0].contains(&note), "{}", answered[0]);
    let pending = bus.ledger.pending(&root.thread_id).await.unwrap().expect("the escalation is kept");
    assert_eq!((pending.channel.as_ref(), pending.renewed_by.is_some()), (Some(&ops), true));
}

#[tokio::test]
async fn past_the_hop_limit_the_owner_gets_it_and_a_human_starts_the_chain_over() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let [lead, boss, carol] = ants(["lead", "boss", "carol"]);
    bus.chain = Chain { escalate_to: vec![lead.clone()], max_hops: 1, after: None, ..Chain::default() };
    bus.put_roster(&lead, &[boss]).await;
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "stuck", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let one = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        bus.send_spent(&lead, &t1, "go on", Some(&one)).await;
        assert!(next_envelope(&mut to_lead).await.text.ends_with(": go on"));
        bus.send_spent(&lead, &t1, "again", Some(&one)).await;
        let notice = next_envelope(&mut to_human).await;
        let text = "Turn budget spent; needs a decision. Last message from lead:\nagain\n\n[ants] The escalation of \
                        dev-a reached the hop limit (max_escalation_hops = 1).";
        assert_eq!(notice.text, text);
        assert_eq!(notice.escalation.as_deref(), Some(&*one), "the human sees how far it went");

        // A human's answer ends the escalation: the next spend starts at the ant's chain again.
        bus.send(&t1, "carry on").await;
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "carry on".to_owned()));
        bus.send_spent(&carol, &t1, "stuck again", None).await;
        let again = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        assert_eq!((again.level, &again.leader), (1, &lead));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert_eq!(log.locked().answered, ["go on", "carry on"]);
}

#[tokio::test]
async fn an_unanswered_escalation_goes_one_level_higher_after_escalate_after() {
    let Some((mut bus, _to_human)) = Bus::start().await else { return };
    let [lead, boss, carol] = ants(["lead", "boss", "carol"]);
    bus.chain = Chain { escalate_to: vec![lead.clone()], after: Some(Duration::from_millis(300)), ..Chain::default() };
    bus.put_roster(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_lead, mut to_boss] = inboxes(&bus, &[lead.clone(), boss.clone()]).await;
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "stuck", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let one = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        assert!(timeout(Duration::from_millis(150), to_boss.next()).await.is_err(), "not before the deadline");
        let up = next_envelope(&mut to_boss).await;
        assert_eq!(up.escalation.as_deref().map(|e| (e.level, &e.leader)), Some((2, &boss)));
        let late =
            format!("[ants] No answer from lead on thread {t1} within 300 ms; the escalation goes one level higher.");
        assert!(up.text.contains(&late), "{}", up.text);
        assert!(up.text.ends_with("Last message from carol:\nstuck"), "{}", up.text);
        assert!(timeout(Duration::from_millis(500), to_boss.next()).await.is_err(), "no deadline without one set");
        assert_eq!(one.level, 1);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

/// `stuck`'s escalation on `thread`, gone up `chains`, one per level.
fn escalated(stuck: &UserId, thread: &ThreadId, chains: &[&[UserId]]) -> ants_proto::Escalation {
    let mut escalation = ants_proto::Escalation::new(stuck.clone(), thread.clone(), EscalationReason::TurnBudget);
    for chain in chains {
        let ants_proto::Hop::Chain { escalation: up, .. } = escalation.up(chain, 5) else { panic!("a chain") };
        escalation = up;
    }
    escalation
}

async fn read_notes(bus: &Bus, thread: &ThreadId) -> Vec<ants_proto::NotesPost> {
    crate::notes::read(&bus.js, &"reader".parse().unwrap(), crate::reading::Reading::Consumer, thread)
        .await
        .unwrap()
        .posts
}

#[tokio::test]
async fn only_the_leader_answers_the_stuck_ant_and_members_answer_in_the_thread_notes() {
    let Some((bus, _to_human)) = Bus::start().await else { return };
    let [lead, carol, boss] = ants(["lead", "carol", "boss"]);
    bus.put_roster(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_carol, mut to_lead, mut to_boss] = inboxes(&bus, &[carol.clone(), lead.clone(), boss.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2, t3) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    // dev-a is a member of carol's escalation on t1 and t3, led by lead, and leads it on t2, one level up.
    let member = escalated(&carol, &t1, &[&[lead.clone(), bus.dev.clone()]]);
    let leader = escalated(&carol, &t2, &[std::slice::from_ref(&lead), std::slice::from_ref(&bus.dev)]);
    let spent = escalated(&carol, &t3, &[&[lead.clone(), bus.dev.clone()]]);
    let full = ants_proto::NotesPost::spend(t3.clone(), lead.clone(), "last turn", 1, None).unwrap();
    crate::notes::publish(&bus.js, &full, None).await.unwrap();
    for (from, thread, text, escalation) in
        [(&carol, &t1, "help", member), (&lead, &t2, "help more", leader.clone()), (&carol, &t3, "x", spent)]
    {
        let envelope =
            Envelope::on_thread(thread.clone(), from.clone(), bus.dev.clone(), text).with_escalation(Some(escalation));
        bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // t2 and t3 run side by side: their messages to carol come in either order.
        let (first, second) = (next_envelope(&mut to_carol).await, next_envelope(&mut to_carol).await);
        let (answer, notice) = if first.thread_id == t2 { (first, second) } else { (second, first) };
        assert_eq!((&answer.thread_id, answer.escalation.as_deref()), (&t2, Some(&leader)), "the leader's answer");
        assert!(answer.text.ends_with("The reply to this message goes to carol.\n\nhelp more"), "{}", answer.text);
        // Out of turns in the thread notes, the member's answer goes one level up with the escalation, and carol
        // hears.
        let up = next_envelope(&mut to_boss).await;
        assert_eq!(up.escalation.as_deref().map(|e| (e.level, &e.stuck)), Some((2, &carol)));
        assert!(up.text.contains("Unposted answer from dev-a:"), "{}", up.text);
        assert_eq!(notice.thread_id, t3);
        timeout(WAIT, async {
            while read_notes(&bus, &t1).await.is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the member's answer in the thread notes");
        assert!(notice.text.starts_with("[ants] dev-a escalated thread"), "{}", notice.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_carol.next()).await.is_err(), "a member does not answer carol");
    assert!(timeout(Duration::from_millis(50), to_lead.next()).await.is_err());
    let posts = read_notes(&bus, &t1).await;
    assert_eq!(posts.len(), 1, "the member's answer");
    assert_eq!((&posts[0].from, posts[0].turn_budget), (&bus.dev, DEFAULT_TURN_BUDGET - 1));
    assert!(posts[0].text.ends_with("not to carol.\n\nhelp"), "{}", posts[0].text);
    let prompt = log.locked().answered.iter().find(|p| p.ends_with("\n\nhelp")).cloned().unwrap();
    assert!(
        prompt.starts_with(&format!(
            "[ants] dev-a is part of an escalation group for carol's thread {t1}, level 1; members: lead, dev-a."
        )),
        "{prompt}"
    );
    assert!(prompt.contains("Only lead resolves it"), "{prompt}");
    let grant = read_notes(&bus, &t3).await.pop().unwrap();
    assert_eq!((&grant.from, grant.turn_budget), (&bus.dev, DEFAULT_TURN_BUDGET), "a fresh budget at level 2");
}

#[tokio::test]
async fn a_member_sending_the_escalation_higher_moves_the_stuck_thread_up_with_it() {
    let Some((mut bus, _to_human)) = Bus::start().await else { return };
    let [lead, boss, carol] = ants(["lead", "boss", "carol"]);
    let after = Duration::from_secs(600);
    bus.chain = Chain { escalate_to: vec![lead.clone()], after: Some(after), ..Chain::default() };
    bus.put_roster(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_lead, mut to_boss] = inboxes(&bus, &[lead.clone(), boss.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "stuck", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let one = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        let grant = read_notes(&bus, &t1).await.pop().expect("the thread notes have a fresh budget");
        assert_eq!((&grant.from, grant.turn_budget), (&bus.dev, DEFAULT_TURN_BUDGET));
        assert!(grant.text.contains("level 1: sent to lead; lead leads"), "{}", grant.text);

        // lead sends it one level higher and says so: the thread waits for boss from then on.
        let two = escalated(&bus.dev, &t1, &[std::slice::from_ref(&lead), std::slice::from_ref(&boss)]);
        let climbed = Envelope::on_thread(t1.clone(), lead.clone(), bus.dev.clone(), "[ants] lead escalated")
            .with_escalation(Some(two.clone()));
        let seq = bus::publish_json(&bus.js, climbed.subject(), &climbed, None).await.unwrap();
        bus.until_acked(seq).await;
        let pending = bus.ledger.pending(&t1).await.unwrap().expect("the escalation");
        assert_eq!((pending.escalation.level, &pending.to, pending.renewed_by), (2, &vec![boss.clone()], None));
        assert!(pending.deadline.is_some(), "the stuck ant keeps the deadline");

        // Stuck again before boss answers: boss gets it again, not lead.
        bus.send_spent(&carol, &t1, "still stuck", None).await;
        let again = next_envelope(&mut to_boss).await;
        assert_eq!(again.escalation.as_deref(), Some(&two));
        bus.send_spent(&boss, &t1, "from boss", Some(&two)).await;
        let pending = timeout(WAIT, async {
            loop {
                let pending = bus.ledger.pending(&t1).await.unwrap().unwrap();
                if pending.renewed_by.is_some() {
                    break pending;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("boss renews the thread");
        assert_eq!(pending.escalation.level, 2);
        assert_eq!(one.level, 1);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(timeout(Duration::from_millis(100), to_lead.next()).await.is_err(), "lead got level 1 only");
    assert_eq!(log.locked().answered, ["from boss"], "the notice is the daemon's alone");
}

#[tokio::test]
async fn an_owner_that_is_no_human_is_dropped_and_the_default_human_gets_the_thread() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    // dev-b names itself the owner, so an escalation would reach it with a fresh budget, again and again.
    let t1 = ThreadId::generate();
    let ask = Envelope::on_thread(t1.clone(), dev_b.clone(), bus.dev.clone(), "hi").with_owner(Some(dev_b.clone()));
    let mut spent =
        Envelope::on_thread(t1.clone(), dev_b.clone(), bus.dev.clone(), "again").with_owner(Some(dev_b.clone()));
    spent.turn_budget = 0;
    for envelope in [&ask, &spent] {
        bus::publish_json(&bus.js, envelope.subject(), envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!((reply.in_reply_to, reply.owner), (Some(ask.id), None), "the owner is not passed on");
        let escalation = next_envelope(&mut to_human).await;
        assert_eq!((&escalation.to, &escalation.owner), (&UserId::human(), &None));
        assert!(escalation.text.starts_with("Turn budget spent; needs a decision."), "{}", escalation.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_dev_b.next()).await.is_err(), "no escalation went to dev-b");
    assert_eq!(log.locked().answered, ["hi"]);
}

#[tokio::test]
async fn a_message_cut_off_mid_prompt_is_prompted_again_with_a_warning() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (progress_every, ack_wait) = (Duration::from_millis(100), ack_wait(Duration::from_millis(100)));
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    let (_, seq) = bus.send(&t1, "hold a").await;

    // Stopped mid-prompt, as by a crash: the harness may have run tools, and the message was not acked.
    let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let crash = async {
        // Not just the harness's start: stopped before its session is stored, the thread would start afresh.
        until("the prompt to reach the harness", || log.locked().prompted == 1).await;
        ctrl_c.add_permits(2);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), crash);
    // Its harness is still held mid-prompt, so it is dropped rather than shut down.
    drop(pool);
    assert!(bus.marked(seq).await, "the attempt is marked as started");
    assert!(log.locked().answered.is_empty());

    let pool = Pool::new(launch, store, 2, IDLE);
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), warned("hold a")));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    {
        let log = log.locked();
        assert_eq!(log.answered, [warned("hold a")]);
        assert_eq!(log.loaded, ["s1"], "the session resumes, with the history of the cut-off attempt");
    }
    assert!(!bus.marked(seq).await);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_reply_recorded_before_a_crash_is_stored_once_without_a_prompt() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (ask, seq) = bus.send(&t1, "a").await;

    // The crashed attempt recorded its reply and stored it, but never acked.
    let reply = ask.reply(bus.dev.clone(), "s1: done before", Role::Human).unwrap();
    bus.attempts.start(seq, &ask).await.unwrap();
    bus.attempts.replied(seq, &ask, &Reply::Message(Box::new(reply.clone()))).await.unwrap();
    bus::publish_json(&bus.js, reply.subject(), &reply, Some(&reply_id(&bus.dev, &ask))).await.unwrap();
    assert_eq!(next_reply(&mut replies).await, (t1.clone(), "done before".to_owned()));

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().starts, 0, "not prompted again");
    let state = bus.stream.get_info().await.unwrap().state;
    assert_eq!(state.messages, 2, "the message and one copy of its reply");
    assert!(!bus.marked(seq).await);
    assert_eq!(bus.unacked().await, 0);
}

/// Whether the uplink of [`Uplinked`] reaches the hub.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Link {
    Up,
    Down,
}

/// A hub (JetStream domain `hub`) and a leaf node (domain `m1`) with an outbox, whose uplink can be cut.
struct Uplinked {
    leaf: Server,
    uplink_port: u16,
    dead_port: u16,
    /// The human on the hub.
    hub_js: jetstream::Context,
    /// The hub's JetStream through the leaf.
    through_leaf: jetstream::Context,
    outbox: jetstream::stream::Stream,
}

impl Uplinked {
    /// The hub, for [`Bus::connect`] through [`Uplinked::url`], and its leaf node with the uplink up.
    async fn start() -> Option<(Server, Self)> {
        let (uplink_port, dead_port) = (free_port(), free_port());
        let hub = Server::spawn_with(|dir| {
            format!(
                "jetstream {{ store_dir: {:?}, domain: hub }}\nleafnodes {{ listen: \"127.0.0.1:{uplink_port}\" }}\n",
                dir.join("js")
            )
        })?;
        let leaf = Server::spawn_with(|dir| {
            Self::write_uplink(dir, uplink_port);
            format!("jetstream {{ store_dir: {:?}, domain: m1 }}\ninclude \"uplink.conf\"\n", dir.join("js"))
        })
        .unwrap();
        let human = UserId::human();
        let (_, hub_js) = bus::connect(&hub.url, None, None, &human, "test").await.unwrap();
        bus::ensure_stream(&hub_js).await.unwrap();
        let (local, local_js) = bus::connect(&leaf.url, None, None, &human, "test").await.unwrap();
        let outbox = local_js
            .create_stream(jetstream::stream::Config {
                name: ants_proto::OUTBOX_STREAM.to_owned(),
                subjects: vec![ants_proto::OUTBOX_SUBJECTS.to_owned()],
                ..Default::default()
            })
            .await
            .unwrap();
        let mut through_leaf = bus::jetstream(local, Some(&"hub".parse().unwrap()));
        through_leaf.set_timeout(Duration::from_secs(1));
        let uplinked = Self { leaf, uplink_port, dead_port, hub_js, through_leaf, outbox };
        uplinked.until(Link::Up).await;
        Some((hub, uplinked))
    }

    fn write_uplink(dir: &std::path::Path, port: u16) {
        let remote = format!("leafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{port}\" }}] }}\n");
        std::fs::write(dir.join("uplink.conf"), remote).unwrap();
    }

    fn url(&self) -> String {
        self.leaf.url.clone()
    }

    /// Point the uplink at the hub, or at a dead port, and wait until it is so.
    async fn switch(&self, link: Link) {
        let port = match link {
            Link::Up => self.uplink_port,
            Link::Down => self.dead_port,
        };
        Self::write_uplink(self.leaf.dir(), port);
        let reload = std::process::Command::new("kill").arg("-HUP").arg(self.leaf.pid().to_string()).status();
        assert!(reload.unwrap().success());
        self.until(link).await;
    }

    async fn until(&self, link: Link) {
        let up = link == Link::Up;
        timeout(WAIT * 2, async {
            while bus::ensure_stream(&self.through_leaf).await.is_ok() != up {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the uplink is not {}", if up { "up" } else { "down" }));
    }

    async fn kept(&self) -> u64 {
        self.outbox.clone().info().await.unwrap().state.messages
    }

    /// Shorten the hub stream's duplicate window to [`SHORT_WINDOW`], so a test can outwait it.
    async fn shorten_duplicate_window(&self) {
        let mut config = self.hub_js.get_stream(ants_proto::STREAM).await.unwrap().cached_info().config.clone();
        config.duplicate_window = SHORT_WINDOW;
        self.hub_js.update_stream(&config).await.unwrap();
    }
}

/// A duplicate window a test outwaits in three times as long.
const SHORT_WINDOW: Duration = Duration::from_millis(300);

/// dev-a's daemon on a leaf node replies while the leaf's uplink is down: the reply goes to the machine's outbox
/// and the ack fails. Once the uplink is back, the redelivery to a restarted daemon is acked, not answered again.
#[tokio::test]
async fn a_reply_kept_in_the_outbox_answers_its_message_once() {
    let Some((hub, uplinked)) = Uplinked::start().await else { return };
    let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
    let (launch, log, gate) = fake::launch();
    let t1 = ThreadId::generate();
    let ask = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "hold a");
    let seq = bus::publish_json(&uplinked.hub_js, ask.subject(), &ask, None).await.unwrap();
    let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));

    let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let down = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        uplinked.switch(Link::Down).await;
        gate.add_permits(1);
        until("the answer", || log.locked().answered.len() == 1).await;
        // Stopped, as for a restart, after the prompt in flight is handled.
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
    drop(pool);
    assert_eq!(uplinked.kept().await, 1, "the reply is kept in the outbox");

    uplinked.switch(Link::Up).await;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let check = async {
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(log.locked().answered, ["hold a"], "answered once");
    assert_eq!(
        uplinked.hub_js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().state.messages,
        1,
        "no second reply"
    );
    assert_eq!(bus.unacked().await, 0);
    assert!(!bus.answered.has(&ask).await, "forgotten once acked");
}

/// dev-a's daemon on a leaf node gives up on a message while the leaf's uplink is down: the dead letter goes to the
/// machine's outbox, and the message can be neither terminated nor its owner told. Once the uplink is back, the
/// redelivery to a restarted daemon stores that same dead letter, with the error of the last attempt, tells the
/// human and terminates the message, without another prompt.
#[tokio::test]
async fn a_dead_letter_kept_in_the_outbox_is_buried_once_the_uplink_is_back() {
    Box::pin(dead_letter_kept_in_the_outbox(false)).await;
}

/// As above, with the outbox copy landing long before the redelivery, past the hub's duplicate window: the
/// redelivery finds it and stores no other.
#[tokio::test]
async fn a_dead_letter_kept_in_the_outbox_lands_once_past_the_duplicate_window() {
    Box::pin(dead_letter_kept_in_the_outbox(true)).await;
}

async fn dead_letter_kept_in_the_outbox(past_window: bool) {
    let Some((hub, uplinked)) = Uplinked::start().await else { return };
    if past_window {
        uplinked.shorten_duplicate_window().await;
    }
    let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
    let (launch, log, gate) = fake::launch();
    let t1 = ThreadId::generate();
    let ask = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "doomed a");
    let seq = bus::publish_json(&uplinked.hub_js, ask.subject(), &ask, None).await.unwrap();
    let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));
    let stream = uplinked.hub_js.get_stream(ants_proto::STREAM).await.unwrap();

    let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let attempts = MAX_ATTEMPTS as usize;
    gate.add_permits(attempts - 1);
    let down = async {
        until("the last attempt", || log.locked().doomed == attempts).await;
        uplinked.switch(Link::Down).await;
        gate.add_permits(1);
        timeout(WAIT, async {
            while uplinked.kept().await == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the dead letter is kept in the outbox");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
    drop(pool);
    let kept = bus.buried.get(&ask).await.expect("noted on the machine");
    assert_eq!(kept.last_delivery, None, "the extra delivery finishes the burial");
    let kept = kept.dead;
    assert!(kept.error.contains("doomed"), "{}", kept.error);
    assert!(dead::list(&stream).await.unwrap().is_empty(), "not in the hub's stream yet");

    uplinked.switch(Link::Up).await;
    if past_window {
        // As the hub's stream takes the outbox copy once the uplink is back.
        bus::publish_json(&uplinked.hub_js, kept.subject(), &kept, Some(&kept.msg_id())).await.unwrap();
        tokio::time::sleep(SHORT_WINDOW * 3).await;
    }
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let check = async {
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(log.locked().doomed, attempts, "not prompted again");
    let letters = dead::list(&stream).await.unwrap();
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(letters[0].1, kept, "the dead letter of the last attempt");
    let notice = stream.get_last_raw_message_by_subject(&subject::inbox(&UserId::human(), &bus.dev)).await;
    let notice: Envelope = serde_json::from_slice(&notice.unwrap().payload).unwrap();
    assert_eq!((&notice.thread_id, notice.in_reply_to), (&t1, Some(ask.id)));
    assert!(notice.text.contains(&format!("ants dead replay {}", letters[0].0)), "{}", notice.text);
    assert_eq!(bus.unacked().await, 0);
    assert!(bus.buried.get(&ask).await.is_none(), "forgotten once terminated");
}

/// dev-a's daemon on a leaf node gives up on a message on the consumer's extra delivery while the leaf's uplink is
/// down: the dead letter goes to the machine's outbox, and no delivery is left to send its notice. Once the uplink
/// is back, the running daemon stores the dead letter in the hub's stream, tells the human and terminates the
/// message, before JetStream gives up on it.
#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_is_buried_once_the_uplink_is_back() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Live)).await;
}

/// As above, to a daemon restarted after JetStream gave up on the message, so a watcher buried it from the
/// advisory: after the outbox copy landed, finding it and telling nobody, or before, under the same message ids.
/// The human gets one notice either way.
#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_is_told_once_after_the_outbox_copy() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted {
        watcher_first: false,
        past_window: false,
    }))
    .await;
}

#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_is_told_once_after_the_watcher_s() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted { watcher_first: true, past_window: false }))
        .await;
}

/// As above, with the daemon restarted past the hub's duplicate window: it finds the letter there and stores no
/// other, and tells only when the watcher did not.
#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_lands_once_past_the_duplicate_window_after_the_outbox_copy() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted { watcher_first: false, past_window: true }))
        .await;
}

#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_lands_once_past_the_duplicate_window_after_the_watcher_s() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted { watcher_first: true, past_window: true }))
        .await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum UplinkBack {
    /// Within the hub's ack wait, with the daemon running.
    Live,
    /// After JetStream gave up on the message and a watcher heard so, `watcher_first` or after the outbox copy
    /// landed, with the daemon restarted, `past_window`: after the hub's duplicate window ran out.
    Restarted { watcher_first: bool, past_window: bool },
}

async fn dead_letter_kept_on_the_extra_delivery(back: UplinkBack) {
    let Some((hub, uplinked)) = Uplinked::start().await else { return };
    let past_window = matches!(back, UplinkBack::Restarted { past_window: true, .. });
    if past_window {
        uplinked.shorten_duplicate_window().await;
    }
    let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
    let (launch, log, gate) = fake::launch();
    let t1 = ThreadId::generate();
    let hold = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "hold a");
    let doomed = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "doomed b");
    let hold_seq = bus::publish_json(&uplinked.hub_js, hold.subject(), &hold, None).await.unwrap();
    let seq = bus::publish_json(&uplinked.hub_js, doomed.subject(), &doomed, None).await.unwrap();
    let progress_every = Duration::from_millis(200);
    // Live, the hub still waits for the ack of the extra delivery when the uplink is back.
    let ack_wait = if back == UplinkBack::Live { WAIT * 3 } else { ack_wait(progress_every) };
    let stream = uplinked.hub_js.get_stream(ants_proto::STREAM).await.unwrap();

    // The doomed message's five attempts never finished; the held one, ahead of it on the thread, had one.
    let consumer: PullConsumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some(ants_proto::inbox_consumer(&bus.dev)),
            filter_subject: subject::inbox_of(&bus.dev),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ack_wait: WAIT,
            max_deliver: MAX_DELIVER,
            ..Default::default()
        })
        .await
        .unwrap();
    let fetch = |n| {
        let consumer = consumer.clone();
        async move {
            let batch = consumer.fetch().max_messages(n).expires(WAIT).messages().await.unwrap();
            batch.map(Result::unwrap).collect::<Vec<_>>().await
        }
    };
    let mut first = fetch(2).await;
    let (mut held, mut last) = (first.remove(0), first.remove(0));
    for _ in 1..MAX_ATTEMPTS {
        last.ack_with(AckKind::Nak(None)).await.unwrap();
        last = fetch(1).await.remove(0);
    }
    assert_eq!(last.info().unwrap().delivered, MAX_ATTEMPTS);
    for msg in [&mut held, &mut last] {
        msg.ack_with(AckKind::Nak(None)).await.unwrap();
    }

    let buried = &bus.buried;
    let until_buried = |kept: bool| async move {
        let done = timeout(WAIT, async {
            while buried.undelivered().await.is_empty() == kept {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        done.await.unwrap_or_else(|_| panic!("{}", if kept { "not kept" } else { "not buried" }));
    };
    let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let down = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        uplinked.switch(Link::Down).await;
        gate.add_permits(1);
        until_buried(true).await;
        let kept = bus.buried.get(&doomed).await.expect("noted on the machine");
        if back == UplinkBack::Live {
            uplinked.switch(Link::Up).await;
            until_buried(false).await;
            // Only the held message is left, whose ack failed while the uplink was down.
            timeout(WAIT, async {
                while bus.unacked().await != 1 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the doomed message is not terminated");
        }
        ctrl_c.add_permits(1);
        kept
    };
    let ((), kept) = tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
    drop(pool);
    assert!(kept.last_delivery.is_some(), "{kept:?}");
    assert_eq!(kept.dead.stream_seq, seq);

    let notices = || async {
        let mut notices = Vec::new();
        let (subject, mut from) = (subject::inbox(&UserId::human(), &bus.dev), 1);
        while let Ok(msg) = stream.get_first_raw_message_by_subject(&subject, from).await {
            from = msg.sequence + 1;
            let notice: Envelope = serde_json::from_slice(&msg.payload).unwrap();
            notices.extend((notice.in_reply_to == Some(doomed.id)).then_some(notice));
        }
        notices
    };
    if let UplinkBack::Restarted { watcher_first, .. } = back {
        // The watcher's view of JetStream's max-deliveries advisory.
        let advisory = dead::MaxDeliveries {
            stream: ants_proto::STREAM.to_owned(),
            consumer: ants_proto::inbox_consumer(&bus.dev),
            stream_seq: seq,
            deliveries: MAX_DELIVER as u64,
        };
        let humans = Registry::default();
        let webhooks = Webhooks::new(uplinked.hub_js.clone());
        let sessions = bus::ensure_sessions(&uplinked.hub_js, None, &UserId::human()).await.unwrap();
        let threads = Threads::new(uplinked.hub_js.clone(), UserId::human(), sessions);
        let claims = Claims { humans: &humans, webhooks: &webhooks, threads: &threads };
        let watch = || dead::bury_undelivered(&uplinked.hub_js, &stream, &advisory, &claims);
        // As the hub's stream takes the outbox copy once the uplink is back.
        let dead_id = kept.dead.msg_id();
        let outbox_copy = || bus::publish_json(&uplinked.hub_js, kept.dead.subject(), &kept.dead, Some(&dead_id));
        if watcher_first {
            let (dead_seq, _) = watch().await.unwrap().expect("buried by the watcher");
            uplinked.switch(Link::Up).await;
            assert_eq!(outbox_copy().await.unwrap(), dead_seq, "the outbox copy is dropped");
        } else {
            uplinked.switch(Link::Up).await;
            outbox_copy().await.unwrap();
            assert_eq!(watch().await.unwrap(), None, "a dead letter already");
            assert!(notices().await.is_empty(), "the watcher sends no notice for the outbox copy");
        }
        if past_window {
            tokio::time::sleep(SHORT_WINDOW * 3).await;
        }

        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let check = async {
            bus.until_acked(hold_seq).await;
            until_buried(false).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        assert_eq!(bus.unacked().await, 0);
    }
    let (letters, notices) = (dead::list(&stream).await.unwrap(), notices().await);
    let prompted = {
        let log = log.locked();
        (log.starts, log.doomed)
    };
    assert_eq!(prompted, (1, 0), "neither prompted again nor the doomed one at all");
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].thread_id, t1);
    assert!(notices[0].text.contains(&format!("ants dead replay {}", letters[0].0)), "{}", notices[0].text);
    assert!(bus.buried.get(&doomed).await.is_none(), "forgotten once terminated");
}

#[tokio::test]
async fn the_reply_carries_the_title_suggested_during_its_turn() {
    use agent_client_protocol::v1::schema::McpServer;
    use rmcp::ServiceExt as _;
    use rmcp::model::{CallToolRequestParams, ClientConfig};
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/daemon-t/title");
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, roster) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_roster(bus.js.clone()));
    let notes = Arc::new(crate::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, roster, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let (titled, plain) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&titled, "hold on").await;

    let suggest = async |thread: &ThreadId, title: &str| {
        let servers = bus.mcp.as_ref().unwrap().servers()(thread, std::path::Path::new("/"), true);
        let [McpServer::Http(server)] = &servers[..] else { panic!("one HTTP server") };
        let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
        let config = StreamableHttpClientTransportConfig::with_uri(server.url.as_str()).auth_header(token);
        let client = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap();
        let serde_json::Value::Object(args) = serde_json::json!({"title": title}) else { unreachable!() };
        let call = CallToolRequestParams::new("suggest_thread_title").with_arguments(args);
        let result = client.call_tool(call).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{result:?}");
    };
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        suggest(&titled, "  Fix the build ").await;
        gate.add_permits(1);
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!((&reply.thread_id, reply.title.as_deref()), (&titled, Some("Fix the build")));

        bus.send(&titled, "again").await;
        bus.send(&plain, "plain").await;
        for _ in 0..2 {
            let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
            let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
            assert_eq!(reply.title, None, "the suggestion went with its turn's reply");
        }

        // A reply posted to a channel thread carries it too.
        let ask = ChannelPost::new(ops.clone(), UserId::human(), "hold: @dev-a the deploy failed");
        bus.post(&ask).await;
        until("the post to be prompted", || log.locked().prompted == 4).await;
        suggest(&ask.thread_id, "Fix the deploy").await;
        gate.add_permits(1);
        let reply = next_post(&mut posts).await;
        assert_eq!((&reply.thread_id, reply.title.as_deref()), (&ask.thread_id, Some("Fix the deploy")));

        // And so does a member's answer posted to the thread notes of an escalation.
        let [carol, lead] = ants(["carol", "lead"]);
        let member = ThreadId::generate();
        let escalation = escalated(&carol, &member, &[&[lead, bus.dev.clone()]]);
        let help =
            Envelope::on_thread(member.clone(), carol, bus.dev.clone(), "hold: help").with_escalation(Some(escalation));
        bus::publish_json(&bus.js, help.subject(), &help, None).await.unwrap();
        until("the member to be prompted", || log.locked().prompted == 5).await;
        suggest(&member, "Unstick carol").await;
        gate.add_permits(1);
        let posted = timeout(WAIT, async {
            loop {
                if let Some(post) = read_notes(&bus, &member).await.pop() {
                    break post;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the member's answer in the thread notes");
        assert_eq!((&posted.from, posted.title.as_deref()), (&bus.dev, Some("Unstick carol")));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn the_answer_carries_the_files_attached_during_its_turn() {
    use agent_client_protocol::v1::schema::McpServer;
    use rmcp::ServiceExt as _;
    use rmcp::model::{CallToolRequestParams, ClientConfig};
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.js.create_stream(crate::hub::files_config()).await.unwrap();
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/daemon-t/reply-files");
    let _ = std::fs::remove_dir_all(&home);
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("report.md"), "# done").unwrap();
    let (outbox, roster) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_roster(bus.js.clone()));
    let notes = Arc::new(crate::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, roster, notes, bus.humans.clone(), None).await.unwrap();
    mcp.with_files(crate::mcp::jetstream_files(bus.js.clone()), home.clone());
    bus.mcp = Some(mcp);
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let thread = ThreadId::generate();
    bus.send(&thread, "hold on").await;

    let attach_to_reply = async |thread: &ThreadId, files: &[&str]| {
        let servers = bus.mcp.as_ref().unwrap().servers()(thread, &work, true);
        let [McpServer::Http(server)] = &servers[..] else { panic!("one HTTP server") };
        let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
        let config = StreamableHttpClientTransportConfig::with_uri(server.url.as_str()).auth_header(token);
        let client = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap();
        let serde_json::Value::Object(args) = serde_json::json!({"attach": files}) else { unreachable!() };
        let result = client.call_tool(CallToolRequestParams::new("attach_to_reply").with_arguments(args)).await;
        assert_ne!(result.unwrap().is_error, Some(true));
    };
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        attach_to_reply(&thread, &["report.md"]).await;
        gate.add_permits(1);
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        let [report] = &reply.attachments[..] else { panic!("{reply:?}") };
        assert_eq!((report.name.as_str(), report.mime.as_str()), ("report.md", "text/markdown"));
        let (_, bytes) = crate::files::get(&bus.js, &reply.attachment_handle(report.id), Some(report)).await.unwrap();
        assert_eq!(bytes, b"# done");

        // A channel post carries them too; one gone by the end of the turn leaves them out, the answer saying why.
        let ask = ChannelPost::new(ops.clone(), UserId::human(), "hold: @dev-a the report?");
        bus.post(&ask).await;
        until("the post to be prompted", || log.locked().prompted == 2).await;
        attach_to_reply(&ask.thread_id, &["report.md"]).await;
        gate.add_permits(1);
        let post = next_post(&mut posts).await;
        let [report] = &post.attachments[..] else { panic!("{post:?}") };
        let (_, bytes) = crate::files::get(&bus.js, &post.attachment_handle(report.id), Some(report)).await.unwrap();
        assert_eq!(bytes, b"# done");

        bus.send(&thread, "hold again").await;
        until("the next prompt", || log.locked().prompted == 3).await;
        attach_to_reply(&thread, &["report.md"]).await;
        std::fs::remove_file(work.join("report.md")).unwrap();
        gate.add_permits(1);
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        assert!(reply.attachments.is_empty());
        let left_out = "\n\n[ants] The files attached to this answer are left out: cannot attach \"report.md\"";
        assert!(reply.text.contains(left_out), "{}", reply.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn the_next_prompt_mentions_late_replies_waiting_on_its_thread() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    // Two ask_ant calls on t1 that gave up, remembered across a restart.
    let asks = [1, 2].map(|_| Envelope::on_thread(t1.clone(), bus.dev.clone(), dev_b.clone(), "?"));
    let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/daemon-t/late");
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    let stored = serde_json::json!({"asks": asks.iter().map(|a| (a.id, &a.to)).collect::<Vec<_>>(), "late": []});
    std::fs::write(home.join("mcp.json"), stored.to_string()).unwrap();
    let (outbox, roster) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_roster(bus.js.clone()));
    let notes = Arc::new(crate::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, roster, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    for ask in &asks {
        let reply = ask.reply(dev_b.clone(), "late", Role::Ant).unwrap();
        bus::publish_json(&bus.js, reply.subject(), &reply, None).await.unwrap();
    }
    bus.send(&t1, "a").await;
    bus.send(&t2, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut got = [next_reply(&mut replies).await, next_reply(&mut replies).await];
        got.sort_by_key(|(thread, _)| thread != &t1);
        let note = "[ants] 2 late replies to ask_ant are waiting on this thread; call check_inbox to read them.";
        assert_eq!(got, [(t1.clone(), format!("{note}\n\na")), (t2.clone(), "b".to_owned())]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered.len(), 2, "the late replies are no prompts");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_human_s_answer_to_the_ant_s_question_is_prompted_with_an_ants_line() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let options = ["yes".to_owned(), "not yet".to_owned()];
    let question = ants_proto::Question::new("Ship it?", &options, false).unwrap();
    let asked = Envelope::asking(thread.clone(), bus.dev.clone(), UserId::human(), question);
    bus::publish_json(&bus.js, asked.subject(), &asked, None).await.unwrap();
    timeout(WAIT, replies.next()).await.expect("the question").unwrap();
    let picked = Envelope::answering(&asked, UserId::human(), ants_proto::Choice::Option(2)).unwrap();
    bus::publish_json(&bus.js, picked.subject(), &picked, None).await.unwrap();
    // No such option: an ordinary message.
    let mut wrong = picked.clone();
    wrong.id = Uuid::now_v7();
    wrong.answer.as_mut().unwrap().option = Some(3);
    bus::publish_json(&bus.js, wrong.subject(), &wrong, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let line = format!(
            "[ants] human answers the question asked with ask_human as message {} (\"Ship it?\") with option 2, \
             \"not yet\"; the message is that option's text.",
            asked.id
        );
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), format!("{line}\n\nnot yet")));
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), "not yet".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_mentioned_member_answers_on_the_thread_after_the_posts_it_missed() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let root = ChannelPost::new(ops.clone(), UserId::human(), "deploy friday?");
    bus.post(&root).await;
    let ask = root.reply("dev-b".parse().unwrap(), "@dev-a agree?", &[], &Humans::default()).unwrap();
    let ask_seq = bus.post(&ask).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_post(&mut replies).await;
        assert_eq!((&reply.channel, &reply.thread_id, reply.in_reply_to), (&ops, &root.thread_id, Some(ask.id)));
        assert_eq!(reply.turn_budget, DEFAULT_TURN_BUDGET - 2, "the ask spent one turn, the reply another");
        bus.until_posts_acked(ask_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let first = log.locked().answered.clone();
    assert_eq!(first.len(), 1, "the post that mentions nobody is not prompted: {first:?}");
    assert!(first[0].contains("human: deploy friday?\n") && first[0].ends_with("nobody."), "{}", first[0]);
    assert!(first[0].contains("Post from dev-b mentioning dev-a:\n@dev-a agree?"), "{}", first[0]);

    // After a restart, only what was posted since shows, without the ant's own reply. Owning the channel now,
    // the ant is told so.
    let mut owned: ChannelEntry = serde_json::from_slice(&bus.channels.get("ops").await.unwrap().unwrap()).unwrap();
    owned.owner = Some(bus.dev.clone());
    bus.channels.put(owned.key(), serde_json::to_vec(&owned).unwrap().into()).await.unwrap();
    let more = root.reply(UserId::human(), "rollback plan is ready", &[], &Humans::default()).unwrap();
    bus.post(&more).await;
    let again = root.reply(UserId::human(), "@dev-a and now?", &[], &Humans::default()).unwrap();
    let again_seq = bus.post(&again).await;
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_post(&mut replies).await;
        assert_eq!((reply.in_reply_to, reply.turn_budget), (Some(again.id), DEFAULT_TURN_BUDGET - 1));
        bus.until_posts_acked(again_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let second = log.locked().answered[1].clone();
    let shown = "Earlier posts on the thread not shown before, oldest first:\nhuman: rollback plan is ready\n\n";
    assert!(second.contains(shown), "{second}");
    assert!(!second.contains("deploy friday") && !second.contains("dev-a: "), "{second}");
    assert!(second.ends_with(&format!("\n\n{}", channel::owner_note(&ops))), "{second}");
}

/// Store `bytes` as a new attachment named `name` of type `mime`, under the handle `handle` gives its id.
async fn attach(
    bus: &Bus,
    handle: impl Fn(Uuid) -> AttachmentHandle,
    name: &str,
    mime: &str,
    bytes: &[u8],
) -> Attachment {
    let attachment = crate::files::reference(name, mime, bytes).unwrap();
    crate::files::put(&bus.js, &handle(attachment.id), &attachment, bytes).await.unwrap();
    attachment
}

#[tokio::test]
async fn attachments_reach_a_harness_taking_images_as_images_and_any_other_as_a_line() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    bus.js.create_stream(crate::hub::files_config()).await.unwrap();
    let (launch, log, _) = fake::launch();
    log.locked().takes_images = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let line = |a: &Attachment, handle: &AttachmentHandle| {
        format!("[ants] Attached: {:?} ({}, {} bytes), handle {handle}", a.name, a.mime, a.size)
    };

    let mut message = Envelope::on_thread(ThreadId::generate(), UserId::human(), bus.dev.clone(), "look");
    let to = message.clone();
    let handle = |id| to.attachment_handle(id);
    let shot = attach(&bus, handle, "shot.png", "image/png", b"\x89PNG shot").await;
    let log_file = attach(&bus, handle, "build.log", "text/plain", b"error: E0308").await;
    // Stored as other bytes than its reference says: unavailable, with no chunk missing to wait for.
    let gone = crate::files::reference("gone.png", "image/png", b"as referenced").unwrap();
    let other = Attachment { id: gone.id, ..crate::files::reference("gone.png", "image/png", b"other").unwrap() };
    crate::files::put(&bus.js, &handle(gone.id), &other, b"other").await.unwrap();
    message.attachments = vec![shot.clone(), log_file.clone(), gone.clone()];
    bus::publish_json(&bus.js, message.subject(), &message, None).await.unwrap();

    let mut post = ChannelPost::new(ops.clone(), UserId::human(), "@dev-a see the chart");
    let on = post.clone();
    let chart = attach(&bus, |id| on.attachment_handle(id), "chart.webp", "image/webp", b"RIFF chart").await;
    post.attachments = vec![chart.clone()];

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        next_reply(&mut replies).await;
        bus.post(&post).await;
        next_post(&mut posts).await;
        // A harness started now takes no images.
        log.locked().takes_images = false;
        let mut plain = Envelope::on_thread(ThreadId::generate(), UserId::human(), bus.dev.clone(), "again");
        plain.attachments = vec![attach(&bus, |id| plain.attachment_handle(id), "shot.png", "image/png", b"x").await];
        bus::publish_json(&bus.js, plain.subject(), &plain, None).await.unwrap();
        next_reply(&mut replies).await;
        ctrl_c.add_permits(1);
        plain
    };
    let ((), plain) = tokio::join!(bus.serve(&pool, &ctrl_c), check);

    let log = log.locked();
    let [first, second, third] = &log.prompts[..] else { panic!("{:?}", log.prompts) };
    let shown = format!("{}; shown as an image after this text.", line(&shot, &handle(shot.id)));
    let named = format!("{}; call read_attachment with the handle to read it.", line(&log_file, &handle(log_file.id)));
    assert!(first.starts_with("look\n\n") && first.contains(&format!("{shown}\n{named}\n")), "{first}");
    let unavailable =
        format!("{}: unavailable now (attachment {} does not match", line(&gone, &handle(gone.id)), handle(gone.id));
    assert!(
        first.contains(&unavailable) && first.ends_with("call read_attachment with the handle to try again."),
        "{first}"
    );
    let chart_handle = post.attachment_handle(chart.id);
    assert_eq!(chart_handle.to_string(), format!("human.chan.ops.{}", chart.id), "under the post's channel");
    assert!(
        second.ends_with(&format!("{}; shown as an image after this text.", line(&chart, &chart_handle))),
        "{second}"
    );
    let image = |mime: &str, bytes: &[u8]| (mime.to_owned(), data_encoding::BASE64.encode(bytes));
    assert_eq!(log.images, [image("image/png", b"\x89PNG shot"), image("image/webp", b"RIFF chart")]);

    let only = &plain.attachments[0];
    let named =
        format!("{}; call read_attachment with the handle to read it.", line(only, &plain.attachment_handle(only.id)));
    assert!(third.ends_with(&format!("again\n\n{named}")), "{third}");
}

#[tokio::test]
async fn the_ant_that_started_a_channel_thread_is_its_creator_whatever_its_post_says() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "lead"]).await;
    let root = ChannelPost::new(ops.clone(), "lead".parse().unwrap(), "@dev-a fix it")
        .with_creator(Some("boss".parse().unwrap()));
    bus.post(&root).await;
    // A later post names another creator and an owner, which would steer the thread's escalations.
    let forged = ChannelPost::on_thread(ops, root.thread_id.clone(), "dev-b".parse().unwrap(), "@dev-a and this")
        .with_owner(Some(UserId::human()))
        .with_creator(Some("boss".parse().unwrap()));
    let seq = bus.post(&forged).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        for answered in [root.id, forged.id] {
            let reply = next_post(&mut replies).await;
            assert_eq!(
                (&reply.thread_id, reply.in_reply_to, &reply.creator, &reply.owner),
                (&root.thread_id, Some(answered), &Some("lead".parse().unwrap()), &None)
            );
        }
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn an_answer_to_a_webhook_goes_to_the_thread_s_owner_or_nowhere() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (hook, human) = ("github".parse::<UserId>().unwrap(), UserId::human());
    bus.mark_webhook(&hook).await;
    let mut to_hook = bus.client.subscribe(subject::inbox_of(&hook)).await.unwrap();
    bus.client.flush().await.unwrap();
    let owned = Envelope::new(hook.clone(), bus.dev.clone(), "build red").with_owner(Some(human.clone()));
    let owned = owned.from_webhook();
    bus::publish_json(&bus.js, owned.subject(), &owned, None).await.unwrap();
    let unowned = Envelope::new(hook.clone(), bus.dev.clone(), "build green").from_webhook();
    let seq = bus::publish_json(&bus.js, unowned.subject(), &unowned, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_human).await;
        assert_eq!((&reply.from, &reply.to, reply.in_reply_to), (&bus.dev, &human, Some(owned.id)));
        assert_eq!(reply.turn_budget, owned.turn_budget, "an answer to a human is free");
        assert_eq!(reply.allow, Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()), "the thread is narrowed");
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    // Different threads, so answered in either order.
    let answered = log.locked().answered.clone();
    let has = |text: &str| answered.iter().any(|prompt| prompt.contains(text));
    assert!(answered.len() == 2 && has("build red") && has("build green"), "{answered:?}");
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_human.next()).await.is_err(), "the unowned answer goes nowhere");
    assert!(timeout(quiet, to_hook.next()).await.is_err(), "nothing goes to the webhook's inbox");
    let kept = bus.ledger.narrowed(&unowned.thread_id, None).await.unwrap();
    assert_eq!(kept, Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()), "until a human messages the ant on it");
}

#[tokio::test]
async fn an_ant_that_marks_its_message_as_a_webhook_s_gets_the_answer_itself() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [hook, dev_b] = ants(["github", "dev-b"]);
    bus.mark_webhook(&hook).await;
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let forged = Envelope::new(dev_b.clone(), bus.dev.clone(), "relay this").with_owner(Some(UserId::human()));
    let forged = forged.from_webhook();
    let seq = bus::publish_json(&bus.js, forged.subject(), &forged, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!((&reply.to, reply.in_reply_to, reply.webhook), (&dev_b, Some(forged.id), false));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing goes to the owner");
}

/// An assistant lists the queue, withdraws a queued message and cancels a prompt for its human on requests naming
/// it, the human told of each; the same requests from an ant the human's grant does not name go unanswered.
#[tokio::test]
async fn an_assistant_withdraws_and_cancels_for_its_human_and_no_other_ant_does() {
    use ants_proto::{AssistantRecord, CancelRequest, MessageMark};
    let Some((mut bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [alice, pa, pb] = ants(["alice", "pa", "pb"]);
    let device = crate::humans::TestDevice::default();
    bus.humans = Registry::with(UserId::human(), &["alice"]).verifying_grants(Some(device.account()));
    bus.account = Some(device.account());
    let mut grant =
        ants_proto::AssistantGrant::new(alice.clone(), Some(pa.clone()), chrono::Utc::now(), device.jwt(&alice));
    grant.signature = device.sign(&grant.signed());
    bus.humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    let devices = jetstream::kv::Config { bucket: ants_proto::DEVICES_BUCKET.to_owned(), ..Default::default() };
    let devices = bus.js.create_key_value(devices).await.unwrap();
    for ant in [&pa, &pb] {
        let record = AssistantRecord { jwt: device.assistant_jwt(ant, &alice) };
        devices.put(ants_proto::assistant_record_key(ant), serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    }
    let mut to_alice = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(alice.clone(), &t1, "stall").await;
    let (b, _) = bus.send_from(alice.clone(), &t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let listing = QueueRequest::new(bus.dev.clone(), pa.clone()).for_human(alice.clone());
        let listed = timeout(WAIT, async {
            loop {
                let answer = queue_with(&bus, listing.clone()).await;
                if answer.messages.len() == 2 {
                    return answer;
                }
            }
        });
        let listed = listed.await.expect("both in hand");
        assert_eq!((listed.messages[1].id, listed.messages[1].running), (b.id, false));
        let quiet = Duration::from_millis(600);
        for by in [&pb, &pa] {
            let forged = CancelRequest::new(bus.dev.clone(), t1.clone(), by.clone()).withdrawing(b.id);
            assert_eq!(ask_cancel(&bus, &forged, quiet).await, None, "a withdrawal for none is not answered");
        }
        let forged = CancelRequest::new(bus.dev.clone(), t1.clone(), pb.clone()).for_human(alice.clone());
        assert_eq!(ask_cancel(&bus, &forged.clone().withdrawing(b.id), quiet).await, None, "pb is not alice's");
        assert_eq!(ask_cancel(&bus, &forged, quiet).await, None);
        assert_eq!(log.locked().cancelled, 0);

        let request = CancelRequest::new(bus.dev.clone(), t1.clone(), pa.clone()).for_human(alice.clone());
        let withdrawn = ask_cancel(&bus, &request.clone().withdrawing(b.id), WAIT).await.unwrap();
        assert!(withdrawn.cancelled);
        assert!(matches!(mark_of(&bus, &b).await, Some(MessageMark::Withdrawn { by, on_behalf_of: Some(human), .. })
                if by == pa && human == alice));
        let told = next_envelope(&mut to_alice).await;
        assert!(told.text.contains("pa, as the assistant of alice, withdrew message"), "{}", told.text);
        assert!(ask_cancel(&bus, &request, WAIT).await.unwrap().cancelled);
        let mut texts = Vec::new();
        for _ in 0..3 {
            texts.push(next_envelope(&mut to_alice).await.text);
        }
        texts.sort();
        assert_eq!(
            texts,
            [
                "[ants] Withdrawn by pa for alice before dev-a was prompted with it.".to_owned(),
                format!(
                    "[ants] pa, as the assistant of alice, cancelled the prompt of dev-a on thread {t1} for alice."
                ),
                "partial\n\n[ants] The prompt was cancelled by pa, the assistant of alice.".to_owned(),
            ]
        );
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let done = |log: &fake::Log| (log.cancelled, log.prompted);
    assert_eq!(done(&log.locked()), (1, 1), "b never reached the harness");
    assert_eq!(bus.unacked().await, 0);
}

/// While no daemon of the ant serves, the assistant of a human withdraws a message of the human's, or one it sent for
/// the human, by a record under a key of its own, which the daemon honours as it takes the message up, telling the
/// human; one recorded by an ant the grant does not name, or after the message was taken up, withdraws nothing.
#[tokio::test]
async fn a_withdrawal_an_assistant_records_while_no_daemon_serves_is_honoured_before_the_prompt() {
    use crate::attempts::{Recorded, record_withdrawal};
    use ants_proto::{AssistantRecord, MessageMark};
    let Some((mut bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [alice, pa, pb] = ants(["alice", "pa", "pb"]);
    let device = crate::humans::TestDevice::default();
    bus.humans = Registry::with(UserId::human(), &["alice"]).verifying_grants(Some(device.account()));
    bus.account = Some(device.account());
    let mut grant =
        ants_proto::AssistantGrant::new(alice.clone(), Some(pa.clone()), chrono::Utc::now(), device.jwt(&alice));
    grant.signature = device.sign(&grant.signed());
    bus.humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    let devices = jetstream::kv::Config { bucket: ants_proto::DEVICES_BUCKET.to_owned(), ..Default::default() };
    let devices = bus.js.create_key_value(devices).await.unwrap();
    for ant in [&pa, &pb] {
        let record = AssistantRecord { jwt: device.assistant_jwt(ant, &alice) };
        devices.put(ants_proto::assistant_record_key(ant), serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    }
    let mut to_alice = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let [t1, t2, t3, t4] = [(); 4].map(|()| ThreadId::generate());
    let (a, _) = bus.send_from(alice.clone(), &t1, "a").await;
    let (b, _) = bus.send_from(alice.clone(), &t2, "b").await;
    let (c, _) = bus.send_from(alice.clone(), &t3, "c").await;
    let mut for_alice = Envelope::on_thread(t4.clone(), pa.clone(), bus.dev.clone(), "d");
    for_alice.on_behalf_of = Some(alice.clone());
    bus::publish_json(&bus.js, for_alice.subject(), &for_alice, None).await.unwrap();

    let record = async |message: &Envelope, assistant: &UserId| {
        record_withdrawal(&bus.markers, &bus.dev, &message.id, assistant, &alice).await
    };
    assert_eq!(record(&a, &pa).await.unwrap(), Recorded::Holds);
    assert_eq!(record(&for_alice, &pa).await.unwrap(), Recorded::Holds);
    assert_eq!(record(&b, &pb).await.unwrap(), Recorded::Holds, "stored, but pb is not alice's assistant");
    assert_eq!(bus.attempts.take_up(&c).await.unwrap(), None, "c is taken up");
    assert!(matches!(record(&c, &pa).await.unwrap(), Recorded::Marked(MessageMark::Started { .. })), "too late");

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut texts = Vec::new();
        for _ in 0..5 {
            texts.push(next_envelope(&mut to_alice).await.text);
        }
        let has = |part: &str| texts.iter().any(|t| t.contains(part));
        assert!(has("[ants] Withdrawn by pa for alice before dev-a was prompted with it."), "{texts:?}");
        assert!(has(&format!("pa, as the assistant of alice, withdrew message {} from the queue", for_alice.id)));
        assert!(!has(&format!("withdrew message {}", a.id)), "a's withdrawal notice went to alice: {texts:?}");
        assert!(has(": b") && has(": c"), "{texts:?}");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let mut answered = log.locked().answered.clone();
    answered.sort();
    assert_eq!(answered, ["b", "c"]);
    for message in [&a, &for_alice] {
        assert!(
            matches!(mark_of(&bus, message).await, Some(MessageMark::Withdrawn { by, on_behalf_of: Some(human), .. })
                if by == pa && human == alice)
        );
    }
    assert_eq!(bus.unacked().await, 0);
}

/// A message claiming to be sent for a human by an ant its grant does not name is the ant's own: answered as any
/// other, with nothing told to the human.
#[tokio::test]
async fn a_claim_to_send_for_a_human_without_its_grant_is_forged() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [pa] = ants(["pa"]);
    let [mut to_pa] = inboxes(&bus, std::array::from_ref(&pa)).await;
    let claimed = Envelope::new(pa.clone(), bus.dev.clone(), "for the human").on_behalf(Some(UserId::human()));
    let seq = bus::publish_json(&bus.js, claimed.subject(), &claimed, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_pa).await;
        assert_eq!((&reply.to, reply.in_reply_to, &reply.on_behalf_of), (&pa, Some(claimed.id), &None));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["for the human"]);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "the human is told nothing");
}

/// A post claiming to be posted for a human by an ant its grant does not name is the ant's own.
#[tokio::test]
async fn a_claim_to_post_for_a_human_without_its_grant_is_forged() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "pa"]).await;
    let claimed = ChannelPost::new(ops, "pa".parse().unwrap(), "@dev-a for the human").on_behalf(Some(UserId::human()));
    let seq = bus.post(&claimed).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_post(&mut replies).await;
        assert_eq!((reply.in_reply_to, &reply.on_behalf_of), (Some(claimed.id), &None));
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert!(answered[0].contains("Post from pa mentioning dev-a:\n"), "{}", answered[0]);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "the human is told nothing");
}

#[tokio::test]
async fn an_ant_names_a_creator_only_as_the_top_level_post_of_the_thread_says() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [dev_b, lead, boss] = ants(["dev-b", "lead", "boss"]);
    let [mut to_dev_b, mut to_boss] = inboxes(&bus, &[dev_b.clone(), boss.clone()]).await;
    let (ops, _) = bus.channel("ops", &["lead"]).await;
    let root = ChannelPost::new(ops, lead.clone(), "plan the release");
    bus.post(&root).await;
    let forge = |thread: ThreadId, text: &str| {
        Envelope::on_thread(thread, dev_b.clone(), bus.dev.clone(), text)
            .with_owner(Some(UserId::human()))
            .with_creator(Some(boss.clone()))
    };
    let plain = forge(ThreadId::generate(), "on a thread a message started");
    let on_root = forge(root.thread_id.clone(), "on lead's channel thread");
    // Out of turns: escalated up the creator's chain, were the creator taken from the message.
    let mut spent = forge(ThreadId::generate(), "stuck");
    spent.turn_budget = 0;
    let mut seq = 0;
    for envelope in [&plain, &on_root, &spent] {
        seq = bus::publish_json(&bus.js, envelope.subject(), envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut replies = [next_envelope(&mut to_dev_b).await, next_envelope(&mut to_dev_b).await];
        replies.sort_by_key(|reply| reply.in_reply_to != Some(plain.id));
        let named = |reply: &Envelope| (reply.in_reply_to, reply.creator.clone(), reply.owner.clone());
        assert_eq!(named(&replies[0]), (Some(plain.id), None, None), "no creator, nor an owner nobody recorded");
        assert_eq!(named(&replies[1]), (Some(on_root.id), Some(lead.clone()), None), "as lead's post says");
        let escalation = next_envelope(&mut to_human).await;
        assert_eq!((&escalation.thread_id, &escalation.creator), (&spent.thread_id, &None));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(timeout(Duration::from_millis(200), to_boss.next()).await.is_err(), "nothing goes to boss");
}

#[tokio::test]
async fn an_ant_names_an_owner_only_as_recorded_for_the_thread_or_the_one_it_was_spun_off() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let alice: UserId = "alice".parse().unwrap();
    let _watching = bus.humans.watch(bus.js.clone(), Duration::from_millis(50));
    let config = jetstream::kv::Config { bucket: ants_proto::HUMANS_BUCKET.to_owned(), ..Default::default() };
    let humans = bus.js.create_key_value(config).await.unwrap();
    let entry = ants_proto::HumanEntry::new(alice.clone());
    humans.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    until("alice to be a human", || bus.humans.is_human(&alice)).await;
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let [mut to_dev_b, mut to_alice] = inboxes(&bus, &[dev_b.clone(), alice.clone()]).await;
    // alice asked dev-a and dev-b on `asked`, and dev-a on `stuck`, recording herself as their owner; nobody
    // recorded one for `unrecorded`.
    let [asked, stuck, unrecorded] = [(); 3].map(|()| ThreadId::generate());
    bus::ensure_owners(&bus.js).await.unwrap();
    for (thread, to) in [(&asked, &bus.dev), (&asked, &dev_b), (&stuck, &bus.dev)] {
        crate::owners::record(&bus.js, None, thread, to, &alice).await.unwrap();
    }
    // dev-b names the default human as the owner of each, and links spin-offs to `asked`, `unrecorded` and
    // `stuck`, where nobody messaged it.
    let forge = |thread: &ThreadId, origin: Option<&ThreadId>, text: &str| {
        Envelope::on_thread(thread.clone(), dev_b.clone(), bus.dev.clone(), text)
            .with_owner(Some(UserId::human()))
            .with_origin_thread(origin.cloned())
    };
    let on_asked = forge(&asked, None, "on alice's thread");
    let on_stuck = forge(&stuck, None, "on a thread alice asked another on");
    let on_unrecorded = forge(&unrecorded, None, "on a thread nobody recorded");
    let spun_off_asked = forge(&ThreadId::generate(), Some(&asked), "spun off alice's thread");
    let spun_off_unrecorded = forge(&ThreadId::generate(), Some(&unrecorded), "spun off the other");
    let spun_off_stuck = forge(&ThreadId::generate(), Some(&stuck), "spun off a thread it took no part in");
    // Out of turns: escalated to the thread's owner.
    let mut spent = forge(&asked, None, "stuck");
    spent.turn_budget = 0;
    let mut seq = 0;
    let sent = [&on_asked, &on_stuck, &on_unrecorded, &spun_off_asked, &spun_off_unrecorded, &spun_off_stuck, &spent];
    for envelope in sent {
        seq = bus::publish_json(&bus.js, envelope.subject(), envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut named = HashMap::new();
        for _ in 0..6 {
            let reply = next_envelope(&mut to_dev_b).await;
            named.insert(reply.in_reply_to.unwrap(), (reply.owner, reply.origin_thread));
        }
        let alice = Some(alice.clone());
        assert_eq!(named[&on_asked.id], (alice.clone(), None), "as recorded");
        assert_eq!(named[&on_stuck.id], (None, None), "nobody messaged dev-b on it");
        assert_eq!(named[&on_unrecorded.id], (None, None), "nobody recorded one");
        assert_eq!(named[&spun_off_asked.id], (alice.clone(), Some(asked.clone())), "as recorded for its origin");
        assert_eq!(named[&spun_off_unrecorded.id], (None, None), "a link giving nothing is dropped");
        assert_eq!(named[&spun_off_stuck.id], (None, None), "nor one to a thread dev-b took no part in");
        let escalation = next_envelope(&mut to_alice).await;
        assert_eq!((&escalation.thread_id, &escalation.owner), (&asked, &alice));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing goes to the named one");
}

#[tokio::test]
async fn a_human_s_message_from_an_outbox_waits_for_its_thread_s_owner_to_be_recorded() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    bus::ensure_owners(&bus.js).await.unwrap();
    let human = UserId::human();
    let ask = |text: &str| Envelope::new(human.clone(), bus.dev.clone(), text).with_owner(Some(human.clone()));
    let (sourced, direct) = (ask("from the outbox"), ask("unrecorded, straight to the hub"));
    // Sourced, as from a machine's outbox, ahead of the owner's record.
    let outbox = jetstream::stream::Config { name: "OUT".into(), subjects: vec!["out.>".into()], ..Default::default() };
    bus.js.create_stream(outbox).await.unwrap();
    let mut config = bus.js.get_stream(ants_proto::STREAM).await.unwrap().cached_info().config.clone();
    config.sources = Some(vec![jetstream::stream::Source {
        name: "OUT".into(),
        subject_transforms: vec![jetstream::stream::SubjectTransform {
            source: "out.>".into(),
            destination: "ants.>".into(),
        }],
        ..Default::default()
    }]);
    bus.js.update_stream(&config).await.unwrap();
    let out = sourced.subject().replacen("ants.", "out.", 1);
    bus::publish_json(&bus.js, out, &sourced, None).await.unwrap();
    let seq = bus::publish_json(&bus.js, direct.subject(), &direct, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_envelope(&mut to_human).await.in_reply_to, Some(direct.id), "no record to wait for");
        assert!(timeout(Duration::from_millis(700), to_human.next()).await.is_err(), "waiting for the record");
        crate::owners::record(&bus.js, None, &sourced.thread_id, &bus.dev, &human).await.unwrap();
        assert_eq!(next_envelope(&mut to_human).await.in_reply_to, Some(sourced.id));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_thread_spun_off_a_channel_thread_keeps_its_creator_only_from_an_ant_taking_part_in_it() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [dev_b, dev_c, eve, lead, boss] = ants(["dev-b", "dev-c", "eve", "lead", "boss"]);
    let [mut to_dev_b, mut to_dev_c, mut to_eve] = inboxes(&bus, &[dev_b.clone(), dev_c.clone(), eve.clone()]).await;
    let (ops, _) = bus.channel("ops", &["lead", "dev-b", "dev-c"]).await;
    let root = ChannelPost::new(ops.clone(), lead.clone(), "@dev-b plan the release");
    bus.post(&root).await;
    // eve, no member, can still store a post on the thread.
    bus.post(&ChannelPost::on_thread(ops, root.thread_id.clone(), eve.clone(), "me too")).await;
    let spin = |from: &UserId, text: &str| {
        Envelope::on_thread(root.thread_id.clone(), from.clone(), bus.dev.clone(), text)
            .with_creator(Some(boss.clone()))
            .spun_off(ThreadId::generate())
    };
    let spun = spin(&dev_b, "woken on lead's thread");
    let mut spent = spin(&dev_b, "stuck");
    spent.turn_budget = 0;
    let no_part = spin(&dev_c, "a member that took no part");
    let no_member = spin(&eve, "no member");
    let mut seq = 0;
    for envelope in [&spun, &no_part, &no_member, &spent] {
        seq = bus::publish_json(&bus.js, envelope.subject(), envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let named = |reply: &Envelope| (reply.in_reply_to, reply.creator.clone(), reply.origin_thread.clone());
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!(named(&reply), (Some(spun.id), Some(lead.clone()), Some(root.thread_id.clone())));
        assert_eq!(named(&next_envelope(&mut to_dev_c).await), (Some(no_part.id), None, None));
        assert_eq!(named(&next_envelope(&mut to_eve).await), (Some(no_member.id), None, None));
        let escalation = next_envelope(&mut to_human).await;
        assert_eq!((&escalation.thread_id, &escalation.creator), (&spent.thread_id, &Some(lead.clone())));
        let created = format!("starts at lead, which created thread {} and has no escalate_to.", spent.thread_id);
        assert!(escalation.text.ends_with(&created), "{}", escalation.text);
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_link_passed_on_from_an_ant_taking_part_carries_the_creator_further() {
    let Some((bus, _to_human)) = Bus::start().await else { return };
    bus::ensure_links(&bus.js).await.unwrap();
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [dev_b, dev_x, dev_y, eve, mallory, lead, boss] =
        ants(["dev-b", "dev-x", "dev-y", "eve", "mallory", "lead", "boss"]);
    let [mut to_dev_y, mut to_eve] = inboxes(&bus, &[dev_y.clone(), eve.clone()]).await;
    let (ops, _) = bus.channel("ops", &["lead", "dev-b"]).await;
    let root = ChannelPost::new(ops, lead.clone(), "@dev-b plan the release");
    bus.post(&root).await;
    let spin = |from: &UserId, to: &UserId, text: &str| {
        Envelope::on_thread(root.thread_id.clone(), from.clone(), to.clone(), text)
            .with_creator(Some(boss.clone()))
            .spun_off(ThreadId::generate())
    };
    // dev-b, woken on lead's thread, passes the link on to dev-x, which passes it on to dev-y. eve passes it on
    // to itself and gets it from mallory, neither with a link of its own.
    for (from, to) in [(&dev_b, &dev_x), (&dev_x, &dev_y), (&eve, &eve), (&mallory, &eve)] {
        crate::links::pass_on(&bus.js, &spin(from, to, "")).await.unwrap();
    }
    let passed = spin(&dev_y, &bus.dev, "two hops away");
    let forged = spin(&eve, &bus.dev, "no link");
    let mut seq = 0;
    for envelope in [&passed, &forged] {
        seq = bus::publish_json(&bus.js, envelope.subject(), envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let named = |reply: &Envelope| (reply.in_reply_to, reply.creator.clone(), reply.origin_thread.clone());
        let reply = next_envelope(&mut to_dev_y).await;
        assert_eq!(named(&reply), (Some(passed.id), Some(lead.clone()), Some(root.thread_id.clone())));
        assert_eq!(named(&next_envelope(&mut to_eve).await), (Some(forged.id), None, None));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_link_to_a_message_thread_passed_on_from_an_ant_messaged_on_it_carries_the_owner_further() {
    let Some((bus, _to_human)) = Bus::start().await else { return };
    bus::ensure_links(&bus.js).await.unwrap();
    bus::ensure_owners(&bus.js).await.unwrap();
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let human = UserId::human();
    let [dev_b, dev_x, dev_y, dev_z, eve, mallory] = ants(["dev-b", "dev-x", "dev-y", "dev-z", "eve", "mallory"]);
    let [mut to_dev_y, mut to_dev_z, mut to_eve] = inboxes(&bus, &[dev_y.clone(), dev_z.clone(), eve.clone()]).await;
    let asked = ThreadId::generate();
    crate::owners::record(&bus.js, None, &asked, &dev_b, &human).await.unwrap();
    let on_asked = |from: &UserId, to: &UserId, text: &str| {
        Envelope::on_thread(asked.clone(), from.clone(), to.clone(), text).with_owner(Some(human.clone()))
    };
    let spin = |from: &UserId, to: &UserId, text: &str| on_asked(from, to, text).spun_off(ThreadId::generate());
    // dev-b, messaged on the thread, passes the link on to dev-x, which passes it on to dev-y, and to dev-z with
    // a message on the thread itself. eve passes it on to itself and gets it from mallory, neither with a link.
    for passed in [
        spin(&dev_b, &dev_x, ""),
        spin(&dev_x, &dev_y, ""),
        on_asked(&dev_b, &dev_z, ""),
        spin(&eve, &eve, ""),
        spin(&mallory, &eve, ""),
    ] {
        crate::links::pass_on(&bus.js, &passed).await.unwrap();
    }
    let passed = spin(&dev_y, &bus.dev, "two hops away");
    let on_thread = spin(&dev_z, &bus.dev, "messaged on the thread by dev-b");
    let forged = spin(&eve, &bus.dev, "no link");
    // The same on the thread itself.
    let on_it = on_asked(&dev_z, &bus.dev, "on the thread");
    let forged_on_it = on_asked(&eve, &bus.dev, "on the thread, no link");
    let mut seq = 0;
    for envelope in [&passed, &on_thread, &forged, &on_it, &forged_on_it] {
        seq = bus::publish_json(&bus.js, envelope.subject(), envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut named = HashMap::new();
        for (to, replies) in [(&mut to_dev_y, 1), (&mut to_dev_z, 2), (&mut to_eve, 2)] {
            for _ in 0..replies {
                let reply = next_envelope(to).await;
                named.insert(reply.in_reply_to.unwrap(), (reply.owner, reply.origin_thread));
            }
        }
        let linked = (Some(human.clone()), Some(asked.clone()));
        assert_eq!(named[&passed.id], linked);
        assert_eq!(named[&on_thread.id], linked);
        assert_eq!(named[&forged.id], (None, None));
        assert_eq!(named[&on_it.id], (Some(human.clone()), None));
        assert_eq!(named[&forged_on_it.id], (None, None));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn posts_that_do_not_wake_the_ant_are_acked_without_a_prompt() {
    let Some((bus, mut human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let (dev_b, human_id) = ("dev-b".parse::<UserId>().unwrap(), UserId::human());
    bus.channel("lab", &["dev-b"]).await;
    bus.post(&ChannelPost::new("lab".parse().unwrap(), human_id.clone(), "@dev-a not a member")).await;
    bus.post(&ChannelPost::new(ops.clone(), human_id.clone(), "@dev-b another member")).await;
    bus.post(&ChannelPost::new(ops.clone(), bus.dev.clone(), "@dev-a its own")).await;
    let forged = ChannelPost::new(ops.clone(), human_id.clone(), "@dev-a forged");
    bus.post_on(subject::chan(&ops, &dev_b), &forged).await;
    let mut spent = ChannelPost::new(ops.clone(), dev_b.clone(), "@dev-a no turns left");
    spent.turn_budget = 0;
    bus.post(&spent).await;
    let ask = ChannelPost::new(ops.clone(), human_id, "@dev-a go");
    let seq = bus.post(&ask).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let escalation = next_envelope(&mut human).await;
        assert_eq!((&escalation.thread_id, escalation.in_reply_to), (&spent.thread_id, Some(spent.id)));
        // dev-b started the thread, so the escalation starts at dev-b, which has no chain.
        let last = "Turn budget spent; needs a decision. Last post from dev-b in #ops:\n@dev-a no turns left";
        let created = format!("starts at dev-b, which created thread {} and has no escalate_to.", spent.thread_id);
        assert_eq!(escalation.text, format!("{last}\n\n[ants] The escalation of dev-a {created}"));
        assert_eq!(next_post(&mut replies).await.text, "@dev-a its own");
        assert_eq!(next_post(&mut replies).await.in_reply_to, Some(ask.id));
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 1, "{answered:?}");
    assert!(answered[0].contains("Post from human mentioning dev-a:\n@dev-a go"), "{}", answered[0]);
}

#[tokio::test]
async fn members_woken_by_one_post_spend_the_thread_s_turns_together() {
    let Some((bus, mut human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "dev-b", "dev-c"]).await;
    let human_id = UserId::human();
    let mut review = ChannelPost::new(ops.clone(), human_id.clone(), "@dev-a @dev-b @dev-c review");
    review.turn_budget = 3;
    bus.post(&review).await;
    // The other members answered first, each spending one of the thread's turns.
    let b = review.reply("dev-b".parse().unwrap(), "fine", &[], &Humans::default()).unwrap();
    bus.post(&b).await;
    let c = review.reply("dev-c".parse().unwrap(), "fine", std::slice::from_ref(&b), &Humans::default()).unwrap();
    let reviewed = bus.post(&c).await;
    let mut ship = ChannelPost::new(ops.clone(), human_id, "@dev-a @dev-b ship it");
    ship.turn_budget = 1;
    bus.post(&ship).await;
    let shipped = bus.post(&ship.reply("dev-b".parse().unwrap(), "shipped", &[], &Humans::default()).unwrap()).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let answer = next_post(&mut replies).await;
        assert_eq!((answer.in_reply_to, answer.turn_budget), (Some(review.id), 0), "the last turn of the thread");
        let escalation = next_envelope(&mut human).await;
        assert_eq!((&escalation.thread_id, escalation.in_reply_to), (&ship.thread_id, Some(ship.id)));
        assert!(escalation.text.starts_with("Turn budget spent; needs a decision."), "{}", escalation.text);
        bus.until_posts_acked(reviewed.max(shipped)).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 1, "no prompt once the thread is out of turns: {answered:?}");
    assert!(answered[0].contains("(0 left after it)"), "{}", answered[0]);
}

#[tokio::test]
async fn the_messenger_answers_a_post_mentioning_nobody_with_the_members_it_concerns() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    // dev-a is the channel's messenger, not a member.
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.add_member("dev-b".parse().unwrap());
    entry.add_member("dev-c".parse().unwrap());
    entry.messenger = Some(bus.dev.clone());
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let mut replies = bus.client.subscribe(subject::chan(&entry.channel, &bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let roster = bus::ensure_roster(&bus.js, ants_proto::ROSTER_TTL).await.unwrap();
    let now = chrono::Utc::now();
    let dev_b = ants_proto::RosterEntry {
        ant: "dev-b".parse().unwrap(),
        machine: "m".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: vec!["ci".to_owned()],
        whoami: Some("Keeps the build green.".to_owned()),
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
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
    };
    roster.put(dev_b.key(), serde_json::to_vec(&dev_b).unwrap().into()).await.unwrap();
    let post = ChannelPost::new(entry.channel.clone(), UserId::human(), "the build is red");
    let seq = bus.post(&post).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let answer = next_post(&mut replies).await;
        assert_eq!((&answer.thread_id, answer.in_reply_to), (&post.thread_id, Some(post.id)));
        assert_eq!(answer.turn_budget, DEFAULT_TURN_BUDGET - 1);
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let prompt = log.locked().answered[0].clone();
    assert!(prompt.contains(channel::TRIAGE_RULES), "{prompt}");
    let members = "- @dev-b [ci]: Keeps the build green.\n- @dev-c (offline: not in the roster)\n";
    assert!(prompt.contains(members), "{prompt}");
}

#[tokio::test]
async fn an_ant_s_post_marked_as_a_webhook_s_counts_as_the_ant_s() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [hook, dev_b] = ants(["github", "dev-b"]);
    bus.mark_webhook(&hook).await;
    // dev-a is the channel's messenger and a member.
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.add_member(bus.dev.clone());
    entry.add_member(dev_b.clone());
    entry.messenger = Some(bus.dev.clone());
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let mut replies = bus.client.subscribe(subject::chan(&entry.channel, &bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let human = Some(UserId::human());
    let real = ChannelPost::new(entry.channel.clone(), hook.clone(), "build red").with_owner(human.clone());
    let real = real.from_webhook(Vec::new());
    bus.post(&real).await;
    let forged = ChannelPost::new(entry.channel.clone(), dev_b.clone(), "triage me").with_owner(human.clone());
    let forged = forged.from_webhook(Vec::new());
    bus.post(&forged).await;
    let on_forged = forged.reply(dev_b.clone(), "@dev-a over to you", &[], &Humans::default()).unwrap();
    assert_eq!((&on_forged.owner, &on_forged.creator), (&human, &None), "as the forged post says");
    let seq = bus.post(&on_forged).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut answers = [next_post(&mut replies).await, next_post(&mut replies).await];
        answers.sort_by_key(|answer| answer.thread_id != real.thread_id);
        let named = |post: &ChannelPost| (post.in_reply_to, post.owner.clone(), post.creator.clone());
        assert_eq!(named(&answers[0]), (Some(real.id), human.clone(), None), "the webhook's");
        assert_eq!(named(&answers[1]), (Some(on_forged.id), None, Some(dev_b.clone())), "dev-b's thread");
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 2, "no triage of dev-b's post: {answered:?}");
    assert!(answered.iter().any(|p| p.contains("Post from github (a webhook) mentioning nobody")), "{answered:?}");
}

#[tokio::test]
async fn a_thread_a_human_started_stays_hers_and_goes_back_to_her_out_of_turns() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    bus.humans = Registry::with(UserId::human(), &["alice"]);
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    // dev-a is the channel's messenger and a member.
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.add_member(bus.dev.clone());
    entry.add_member("dev-b".parse().unwrap());
    entry.messenger = Some(bus.dev.clone());
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let mut replies = bus.client.subscribe(subject::chan(&entry.channel, &bus.dev)).await.unwrap();
    let alice: UserId = "alice".parse().unwrap();
    let mut alice_inbox = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let root =
        ChannelPost::new(entry.channel.clone(), alice.clone(), "the build is red").with_owner(Some(alice.clone()));
    bus.post(&root).await;
    let bob = ChannelPost::new(entry.channel.clone(), "bob".parse().unwrap(), "no human, no triage");
    bus.post(&bob).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let answer = next_post(&mut replies).await;
        assert_eq!((&answer.thread_id, answer.in_reply_to), (&root.thread_id, Some(root.id)));
        assert_eq!((answer.owner.as_ref(), answer.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET - 1));
        let mut spent =
            answer.reply("dev-b".parse().unwrap(), "@dev-a over to you", &[], &bus.humans.humans()).unwrap();
        spent.turn_budget = 0;
        assert_eq!(spent.owner.as_ref(), Some(&alice), "ants' posts keep the owner");
        let seq = bus.post(&spent).await;
        let escalation = next_envelope(&mut alice_inbox).await;
        assert_eq!((&escalation.thread_id, escalation.in_reply_to), (&root.thread_id, Some(spent.id)));
        assert_eq!((&escalation.to, escalation.owner.as_ref()), (&alice, Some(&alice)));
        assert!(escalation.text.starts_with("Turn budget spent; needs a decision."), "{}", escalation.text);
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 1, "alice's post only: {answered:?}");
    assert!(answered[0].contains(channel::TRIAGE_RULES), "{}", answered[0]);
    assert!(answered[0].contains("Post from alice mentioning nobody:\nthe build is red"), "{}", answered[0]);
}

#[tokio::test]
async fn the_ant_s_own_post_notifies_the_human_members_it_mentions_and_wakes_no_messenger() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    bus.humans = Registry::with(UserId::human(), &["alice", "bob"]);
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (alice, bob, dev_b) =
        ("alice".parse::<UserId>().unwrap(), "bob".parse::<UserId>().unwrap(), "dev-b".parse::<UserId>().unwrap());
    // dev-a is a member and the messenger; bob is no member.
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    for member in [&bus.dev, &dev_b, &alice] {
        entry.add_member(member.clone());
    }
    entry.messenger = Some(bus.dev.clone());
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let ops = entry.channel.clone();
    let mut replies = bus.client.subscribe(subject::chan(&ops, &bus.dev)).await.unwrap();
    let mut alice_inbox = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let own = ChannelPost::new(ops.clone(), bus.dev.clone(), "@alice @bob @dev-b heads up");
    bus.post(&own).await;
    // A human's post mentioning only a human goes to no triage; another's mention of alice is its daemon's to tell.
    bus.post(&ChannelPost::new(ops.clone(), UserId::human(), "@alice see")).await;
    let mixed = ChannelPost::new(ops.clone(), dev_b.clone(), "@dev-a @alice check");
    let seq = bus.post(&mixed).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut alice_inbox).await;
        assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&bus.dev, &alice, &own.thread_id));
        assert_eq!(notice.mention, Some(ants_proto::Mention { channel: ops.clone(), post: own.id, author: None }));
        assert_eq!(notice.text, own.text);
        assert_eq!(next_post(&mut replies).await.id, own.id);
        let answer = next_post(&mut replies).await;
        assert_eq!(answer.in_reply_to, Some(mixed.id));
        // The fake harness quotes the prompt, so the answer on the thread mentions alice too.
        let notice = next_envelope(&mut alice_inbox).await;
        assert_eq!((notice.mention.map(|m| m.post), &notice.thread_id), (Some(answer.id), &mixed.thread_id));
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 1, "the mention of dev-a only: {answered:?}");
    assert!(answered[0].contains("Post from dev-b mentioning dev-a:\n@dev-a @alice check"), "{}", answered[0]);
    assert_eq!(bus.stored(&subject::inbox_of(&alice)).await, 2, "one notice per post, from dev-a's daemon");
    assert_eq!(bus.stored(&subject::inbox(&bob, &bus.dev)).await, 0, "bob is no member");
}

#[tokio::test]
async fn a_mention_notice_that_failed_is_sent_on_the_post_s_redelivery_and_only_once() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    bus.humans = Registry::with(UserId::human(), &["alice"]);
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let alice: UserId = "alice".parse().unwrap();
    let (ops, _) = bus.channel("ops", &["dev-a", "alice"]).await;
    let own = ChannelPost::new(ops.clone(), bus.dev.clone(), "@alice done");
    // The markers' bucket is gone, so the first attempts fail and the post waits for a redelivery.
    bus.js.delete_key_value(ants_proto::SESSIONS_BUCKET).await.unwrap();
    let seq = bus.post(&own).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(bus.stored(&subject::inbox(&alice, &bus.dev)).await, 0);
        let mut consumer: PullConsumer = bus.stream.get_consumer(&ants_proto::chan_consumer(&bus.dev)).await.unwrap();
        assert!(consumer.info().await.unwrap().ack_floor.stream_sequence < seq, "not acked before the notice");
        bus::ensure_sessions(&bus.js, None, &bus.dev).await.unwrap();
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(log.locked().answered.is_empty());
    assert_eq!(bus.stored(&subject::inbox(&alice, &bus.dev)).await, 1);
    let stored: Envelope = serde_json::from_slice(
        &bus.stream.get_last_raw_message_by_subject(&subject::inbox(&alice, &bus.dev)).await.unwrap().payload,
    )
    .unwrap();
    assert_eq!(stored.mention.map(|m| m.post), Some(own.id));

    // Taken again, as after a restart past JetStream's duplicate window, the post finds its marker.
    let entry = bus.channels.get("ops").await.unwrap().unwrap();
    let entry: ChannelEntry = serde_json::from_slice(&entry).unwrap();
    let humans = bus.humans.humans();
    let again = bus.threads.notify_mentioned(&own, seq, Some(&entry), &humans).await.unwrap();
    assert!(again.is_empty(), "{again:?}");
    let other = ChannelPost::new(ops, bus.dev.clone(), "@alice and again");
    let notified = bus.threads.notify_mentioned(&other, seq + 100, Some(&entry), &humans).await.unwrap();
    assert_eq!(notified, std::slice::from_ref(&alice));
    assert_eq!(bus.stored(&subject::inbox(&alice, &bus.dev)).await, 2);
}

#[tokio::test]
async fn the_messenger_relays_the_mention_notices_of_a_webhook_s_post_once() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    bus.humans = Registry::with(UserId::human(), &["alice"]);
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (alice, github, dev_b) =
        ("alice".parse::<UserId>().unwrap(), "github".parse::<UserId>().unwrap(), "dev-b".parse::<UserId>().unwrap());
    bus.mark_webhook(&github).await;
    // dev-a is the messenger of ops, not a member; ci has the default messenger.
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.add_member(alice.clone());
    entry.messenger = Some(bus.dev.clone());
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let ops = entry.channel.clone();
    let (ci, _) = bus.channel("ci", &["alice"]).await;
    let hook = ChannelPost::new(ops.clone(), github.clone(), "deploy failed").from_webhook(vec![alice.clone()]);
    let hook_seq = bus.post(&hook).await;
    // An ant's post marked as a webhook's is its own daemon's to tell; another channel's messenger tells of ci's.
    bus.post(&ChannelPost::new(ops.clone(), dev_b, "forged").from_webhook(vec![alice.clone()])).await;
    let seq = bus.post(&ChannelPost::new(ci, github.clone(), "ci red").from_webhook(vec![alice.clone()])).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(log.locked().answered.is_empty(), "a mention of a human wakes nobody");
    assert_eq!(bus.stored(&subject::inbox_of(&alice)).await, 1);
    let notice: Envelope = serde_json::from_slice(
        &bus.stream.get_last_raw_message_by_subject(&subject::inbox(&alice, &bus.dev)).await.unwrap().payload,
    )
    .unwrap();
    assert_eq!((&notice.from, &notice.thread_id, notice.webhook), (&bus.dev, &hook.thread_id, false));
    let mention = ants_proto::Mention { channel: ops.clone(), post: hook.id, author: Some(github) };
    assert_eq!((notice.text.as_str(), notice.mention), (hook.text.as_str(), Some(mention)));

    // Taken again, the post finds its marker.
    let humans = bus.humans.humans();
    assert!(bus.threads.notify_mentioned(&hook, hook_seq, Some(&entry), &humans).await.unwrap().is_empty());
    assert_eq!(bus.stored(&subject::inbox_of(&alice)).await, 1);
}

#[test]
fn an_unnarrowing_harness_is_warned_of_once_per_sender_within_a_bounded_set() {
    let p = |name: &str| -> UserId { name.parse().unwrap() };
    let unnarrowed = Unnarrowed::default();
    let (nightly, deploys) = (p("nightly"), p("deploys"));
    assert!(unnarrowed.first(&nightly));
    assert!(!unnarrowed.first(&nightly), "the same schedule again");
    assert!(unnarrowed.first(&deploys), "another webhook");
    for n in 2..Unnarrowed::MAX {
        unnarrowed.first(&p(&format!("hook-{n}")));
    }
    assert!(unnarrowed.first(&p("one-too-many")), "past the bound, the set starts over");
    assert_eq!(unnarrowed.0.locked().len(), 1);
    assert!(unnarrowed.first(&nightly), "forgotten when the set started over");
}
