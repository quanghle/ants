//! The inbox loop of one ant.
//!
//! Messages of one thread are handled one at a time, in order; different threads run concurrently, each in its own
//! harness from the pool.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Display;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, DeadLetter, Envelope, Error as ProtoError, EscalationReason, Humans,
    JsDomain, State, ThreadId, UserId, subject,
};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use futures::stream::{FuturesUnordered, Stream, StreamExt as _};

use crate::approvals::NatsApprover;
use crate::attempts::{Answered, Attempts, Marker, Reply};
use crate::caps::{Caps, Ledger};
use crate::channel::{self, Channels, Context, Threads, Wake};
use crate::dead::{self, Attempt, Burial, Buried};
use crate::escalation::{self, Chain, Escalator, Route, Stuck};
use crate::harness::{HarnessKind, Launch, ModelSettings, Prompt, PromptLimits, Spend};
use crate::humans::Registry;
use crate::mcp::{Mcp, Roster};
use crate::notes::{self, NotesStore};
use crate::permission::{Allowlist, PermissionPolicy};
use crate::pool::{Pool, SessionStore};
use crate::status::{self, Status};
use crate::{bus, roster};

/// Period of the progress acks sent for every message in hand, from its arrival to its ack: waiting for its thread,
/// and prompting, which covers waiting for a harness, starting it and a tool request waiting for the human.
const PROGRESS_EVERY: Duration = Duration::from_secs(20);
/// Redelivery window: a message in hand of a daemon that crashed, or sent into a dead link, comes back this long after
/// its last progress ack.
const ACK_WAIT: Duration = ack_wait(PROGRESS_EVERY);
/// Attempts a message gets before it becomes a dead letter on `ants.dead.<ant>`.
const MAX_ATTEMPTS: i64 = 5;
/// The consumer allows one more delivery, to dead-letter a message whose last attempt never finished.
const MAX_DELIVER: i64 = MAX_ATTEMPTS + 1;
const RETRY_DELAY: Duration = Duration::from_secs(30);
/// How often idle harnesses are looked for.
const EVICT_EVERY: Duration = Duration::from_secs(30);
/// Inbox messages in hand per harness: prompting, or waiting for their thread or for a free harness.
const IN_HAND_PER_HARNESS: usize = 2;

/// Put before the text of a message whose earlier attempt started and stored no reply.
const CUT_OFF: &str = "[ants] An earlier attempt at this message was cut off before its reply was stored, so tools it \
ran may already have taken effect. The session history may show what ran. Check the current state before repeating \
any action.";

/// A few progress periods, so one late or lost progress ack does not redeliver a message still in hand.
const fn ack_wait(progress_every: Duration) -> Duration {
    progress_every.saturating_mul(3)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub ant: UserId,
    pub nats_url: String,
    /// Credentials file of the ant, when the server requires them.
    pub creds: Option<PathBuf>,
    /// JetStream domain of the hub, if it has one.
    pub js_domain: Option<JsDomain>,
    /// Directory the harness works in.
    pub workdir: PathBuf,
    /// The ant's own files (usually `~/.ants/<ant>`).
    pub home: PathBuf,
    /// Role, memory and task instructions prepended to every prompt.
    pub instructions: String,
    /// The ant's role instructions, shared in the roster.
    pub whoami: Option<String>,
    pub env: BTreeMap<String, String>,
    pub harness: HarnessKind,
    pub model_settings: ModelSettings,
    /// Tool requests the harness may run without asking (see [`crate::permission`] for the rules). Every other one
    /// goes to the human on `ants.approvals.<ant>`.
    pub allow: Vec<String>,
    /// How long a tool request waits for the human before it is rejected.
    pub approval_timeout: Duration,
    /// Most harness processes alive at once; the least recently used idle one stops to make room.
    pub max_harnesses: usize,
    /// A thread's harness stops after this long without a message, and resumes its session on the next one.
    pub idle_timeout: Duration,
    /// Capabilities the ant advertises in the roster.
    pub capabilities: Vec<String>,
    /// Limits on every prompt.
    pub limits: PromptLimits,
    /// Token and cost caps of each thread and of the ant.
    pub caps: Caps,
    /// Serve the ant only while holding its roster entry, as the built-in messenger does: wait while another
    /// daemon lists it, and hand over to a daemon that puts it later ([`roster::hold`]).
    pub defer_to_others: bool,
    /// The default human, who gets escalations and notices of threads with no owner.
    pub human: UserId,
    /// The ant's escalation chain, published in the roster, and its limits.
    pub escalation: Chain,
}

/// Serve the ant's inbox until Ctrl-C. Prompts in flight are finished first; a second Ctrl-C stops at once. Until
/// the NATS server can be reached and its JetStream answers the daemon keeps trying ([`bus::connect_retrying`]). A daemon
/// that defers to others ([`Config::defer_to_others`]) and hands the ant over waits for its turn again.
pub async fn run(config: Config) -> Result<()> {
    while serve(&config).await? == Served::HandedOver {
        tracing::info!("another daemon serves the ant; waiting for it to stop");
    }
    Ok(())
}

/// How [`serve`] ended.
#[derive(Debug, PartialEq, Eq)]
enum Served {
    Stopped,
    /// Another daemon took over the ant's roster entry.
    HandedOver,
}

/// Serve the ant's inbox until Ctrl-C, or until another daemon takes over its roster entry.
async fn serve(config: &Config) -> Result<Served> {
    let allow = Allowlist::parse(&config.allow)?;
    let Some((client, js)) = bus::connect_retrying(
        &config.nats_url,
        config.creds.as_deref(),
        config.js_domain.as_ref(),
        &config.ant,
        &format!("antsd-{}", config.ant),
        async {
            if stop_signal().await.is_err() {
                std::future::pending::<()>().await;
            }
        },
    )
    .await?
    else {
        tracing::info!("stopped before connecting");
        return Ok(Served::Stopped);
    };
    let mut entry = roster::entry(config).await;
    let mut claimed = None;
    if config.defer_to_others {
        let kv = bus::ensure_roster(&js, ants_proto::ROSTER_TTL).await?;
        tokio::select! {
            revision = roster::wait_for_turn(&kv, &mut entry, ants_proto::ROSTER_HEARTBEAT) => claimed = Some(revision?),
            () = async {
                if stop_signal().await.is_err() {
                    std::future::pending::<()>().await;
                }
            } => {
                tracing::info!("stopped before serving");
                return Ok(Served::Stopped);
            }
        }
    }
    let approver = NatsApprover::new(client.clone(), js.clone(), config.ant.clone(), config.approval_timeout)
        .with_chain(config.escalation.clone());
    let policy = PermissionPolicy::new(allow, Some(std::sync::Arc::new(approver.clone())));
    let (outbox, roster) = (crate::mcp::jetstream_outbox(js.clone()), crate::mcp::jetstream_roster(js.clone()));
    let notes = std::sync::Arc::new(crate::notes::JetStreamNotes::new(js.clone(), config.ant.clone()));
    let humans = Registry::new(config.human.clone());
    let _watching = humans.watch(js.clone(), crate::humans::RETRY);
    let sessions_kv = bus::ensure_sessions(&js).await?;
    let ledger = std::sync::Arc::new(Ledger::kv(sessions_kv.clone(), config.ant.clone(), config.caps));
    let escalator = Escalator::new(
        config.ant.clone(),
        config.escalation.clone(),
        roster.clone(),
        humans.clone(),
        ledger.clone(),
        escalation::jetstream_sender(js.clone()),
    )
    .with_chains(crate::chains::jetstream_chains(js.clone()))
    .with_notes(escalation::jetstream_notes_sender(js.clone()));
    let escalator = std::sync::Arc::new(escalator);
    if let Err(e) = crate::chains::publish(&js, &config.ant, &config.escalation.escalate_to).await {
        tracing::warn!(error = format!("{e:#}"), "keeping the escalation chain past the roster");
    }
    let mut mcp = Mcp::start(
        config.ant.clone(),
        &config.home,
        outbox,
        roster.clone(),
        notes.clone(),
        humans.clone(),
        Some(escalator.clone()),
    )
    .await?;
    mcp.listen(bus::live_inbox(&client, &config.ant).await?);
    mcp.with_channels(crate::mcp::jetstream_channels(js.clone()));
    mcp.manage_channels(crate::chanadm::nats_admin(client.clone(), js.clone()));
    let launch = Launch::new(
        config.harness,
        config.workdir.clone(),
        config.env.clone().into_iter().collect(),
        policy,
        config.model_settings.clone(),
    )
    .with_instructions(config.instructions.clone())
    .with_mcp(mcp.servers());
    let stream = bus::ensure_stream(&js).await?;
    let threads = Threads::new(js.clone(), config.ant.clone(), sessions_kv.clone());
    let sessions = SessionStore::kv(sessions_kv, config.ant.clone());
    let attempts = Attempts::new(bus::ensure_attempts(&js).await?, config.ant.clone());
    let answered = Answered::new(&config.home);
    answered.prune(bus::RETENTION).await;
    let buried = Buried::new(&config.home);
    buried.prune(bus::RETENTION).await;
    let pool = Pool::new(launch, sessions, config.max_harnesses, config.idle_timeout);
    let in_hand = i64::try_from(config.max_harnesses.max(1) * IN_HAND_PER_HARNESS)?;
    let messages = bus::inbox_consumer(&stream, &config.ant, ACK_WAIT, MAX_DELIVER, in_hand).await?;
    let (channels, posts) = match channels(&js, &stream, &config.ant, in_hand).await {
        Ok((channels, posts)) => (channels, Some(posts)),
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "serving the inbox without channels");
            (Channels::default(), None)
        }
    };
    let messages = futures::stream::select(messages, futures::stream::iter(posts).flatten());
    let workload = std::sync::Arc::new(roster::Workload::default());
    let registration = match claimed {
        Some(revision) => roster::hold(&js, entry, revision, workload.clone()).await?,
        None => roster::register(&js, entry, workload.clone()).await?,
    };
    tracing::info!(
        ant = %config.ant,
        harness = %config.harness,
        model_settings = ?config.model_settings,
        workdir = %config.workdir.display(),
        max_harnesses = config.max_harnesses,
        idle_timeout = ?config.idle_timeout,
        capabilities = ?config.capabilities,
        allow = ?config.allow,
        approval_timeout = ?config.approval_timeout,
        limits = ?config.limits,
        caps = ?config.caps,
        human = %config.human,
        escalation = ?config.escalation,
        "serving inbox"
    );

    let inbox = Inbox {
        client: &client,
        js: &js,
        ant: &config.ant,
        pool: &pool,
        attempts: &attempts,
        answered: &answered,
        buried: &buried,
        mcp: Some(&mcp),
        approvals: Some(&approver),
        workload: &workload,
        limits: config.limits,
        ledger: &ledger,
        escalator: &escalator,
        notes: notes.as_ref(),
        threads: &threads,
        roster: &roster,
        humans: &humans,
        retry_delay: RETRY_DELAY,
        progress_every: PROGRESS_EVERY,
    };
    let ctrl_c = || async {
        if let Err(e) = stop_signal().await {
            tracing::warn!(error = %e, "waiting for Ctrl-C or SIGTERM");
            std::future::pending::<()>().await;
        }
    };
    let (stopping, mut asked) = (std::cell::Cell::new(false), 0);
    // The first stop is Ctrl-C or the hand-over, after which the prompts in flight finish; the second only Ctrl-C.
    let stop = || {
        asked += 1;
        let (first, stopping, registration) = (asked == 1, &stopping, &registration);
        async move {
            tokio::select! {
                () = ctrl_c() => stopping.set(true),
                () = registration.lost(), if first => {
                    tracing::info!("another daemon took over the ant; taking no more messages");
                }
            }
        }
    };
    tokio::select! {
        () = inbox.serve(messages, channels, config.idle_timeout, stop) => {}
        () = escalator.deadlines() => {}
    }
    let handed_over = registration.is_lost() && !stopping.get();
    registration.deregister().await;
    pool.shutdown().await;
    if handed_over {
        return Ok(Served::HandedOver);
    }
    tracing::info!("stopped");
    Ok(Served::Stopped)
}

/// The channels, and the posts to them through the ant's channel consumer.
async fn channels(
    js: &jetstream::Context,
    stream: &jetstream::stream::Stream,
    ant: &UserId,
    in_hand: i64,
) -> Result<(Channels, jetstream::consumer::pull::Stream)> {
    let channels = Channels::watch(bus::ensure_channels(js).await?).await?;
    let posts = bus::chan_consumer(stream, ant, ACK_WAIT, MAX_DELIVER, in_hand).await?;
    Ok((channels, posts))
}

/// Ctrl-C, or SIGTERM, which launchd and systemd send to stop a service.
async fn stop_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

type InHand = (Incoming, jetstream::Message);

/// A message to answer: one in the inbox, or a channel post that wakes the ant, read as a message from its author
/// ([`channel::as_envelope`]).
struct Incoming {
    envelope: Envelope,
    post: Option<Box<Posted>>,
    /// The channel the answer goes to instead of the sender's inbox: that of the thread, when the message is the
    /// escalation leader's reply that renewed it ([`Inbox::arrived`]).
    resume: Option<ChannelId>,
}

struct Posted {
    post: ChannelPost,
    /// The channel's entry, when the ant is to triage the post as its messenger.
    triage: Option<ChannelEntry>,
    /// Whether the ant owns the channel.
    owned: bool,
}

/// The messages of one thread in hand: one in flight at most, the rest waiting their turn.
#[derive(Default)]
struct Thread {
    waiting: VecDeque<InHand>,
    busy: bool,
    /// Stream sequence of the message in flight.
    in_flight: Option<u64>,
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
    ant: &'a UserId,
    pool: &'a Pool,
    attempts: &'a Attempts,
    answered: &'a Answered,
    /// Messages whose dead letter waits in the machine's outbox, to bury again on their redelivery.
    buried: &'a Buried,
    mcp: Option<&'a Mcp>,
    /// Takes the decisions agents of the ant's chain send on its tool requests.
    approvals: Option<&'a NatsApprover>,
    /// Counts the messages being handled and the live harnesses, for the roster.
    workload: &'a roster::Workload,
    limits: PromptLimits,
    ledger: &'a Ledger,
    /// Sends what the ant cannot settle up its chain, and notes the answers.
    escalator: &'a Escalator,
    /// The thread notes, where the members of an escalation group other than its leader answer.
    notes: &'a dyn NotesStore,
    threads: &'a Threads,
    /// The roster, for the members a triage prompt shows.
    roster: &'a Roster,
    /// Who is a human, and who gets the escalations of a thread with no owner.
    humans: &'a Registry,
    retry_delay: Duration,
    progress_every: Duration,
}

impl Inbox<'_> {
    /// Handle messages, from the inbox and channel posts, until they end or `stop` resolves, then finish the prompts in
    /// flight unless it resolves again. `channels` says which posts are meant for the ant.
    async fn serve<S, E, F>(
        &self,
        mut messages: S,
        mut channels: Channels,
        idle_timeout: Duration,
        mut stop: impl FnMut() -> F,
    ) where
        S: Stream<Item = Result<jetstream::Message, E>> + Unpin,
        E: Display,
        F: Future<Output = ()>,
    {
        let mut threads: HashMap<ThreadId, Thread> = HashMap::new();
        let mut running = FuturesUnordered::new();
        let mut evict = tokio::time::interval(EVICT_EVERY.min(idle_timeout).max(Duration::from_secs(1)));
        let mut progress = self.progress();
        let shutdown = stop();
        tokio::pin!(shutdown);
        loop {
            let retry_deadline = threads.values().filter_map(|t| t.retry.map(|r| r.until)).min();
            let mut ready = None;
            tokio::select! {
                () = &mut shutdown => break,
                () = channels.changed() => {}
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
                        t.in_flight = None;
                        t.retry = retry;
                    }
                    ready = Some(vec![thread]);
                }
                next = messages.next() => match next {
                    None => break,
                    Some(Err(e)) => tracing::warn!(error = %e, "inbox stream"),
                    Some(Ok(msg)) => {
                        let incoming = if is_post(&msg) {
                            self.admit_post(&msg, &mut channels).await
                        } else {
                            self.admit(&msg).await
                        };
                        let Some(incoming) = incoming else { continue };
                        let thread = incoming.envelope.thread_id.clone();
                        let t = threads.entry(thread.clone()).or_default();
                        if t.holds(stream_seq(&msg)) {
                            // The copy in hand gets the ack: the server tracks a message by its stream sequence.
                            tracing::debug!(%thread, "ignoring a redelivery of a message in hand");
                            continue;
                        }
                        if t.retry.is_some_and(|r| Some(r.seq) == stream_seq(&msg)) {
                            t.retry = None;
                            t.waiting.push_front((incoming, msg));
                        } else {
                            t.waiting.push_back((incoming, msg));
                        }
                        ready = Some(vec![thread]);
                    }
                }
            }
            for thread in ready.into_iter().flatten() {
                if let Some((incoming, msg)) = next_turn(&mut threads, thread) {
                    running.push(self.handle(incoming, msg));
                }
            }
            self.workload.set_harnesses(self.pool.live());
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

    /// The inbox message `msg` to answer, if it is one: a malformed or forged one is buried, a reply to an
    /// `ask_agent` call is handed over, and so is an approval decision, which is acked and never prompted with
    /// ([`NatsApprover::offer`]). An owner that is no human is dropped ([`Registry::human_owner`]), so neither the
    /// escalations nor the messages that follow on from this one name it, and so is a creator that is a human.
    async fn admit(&self, msg: &jetstream::Message) -> Option<Incoming> {
        let mut envelope: Envelope = match serde_json::from_slice(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                self.bury_unanswerable(msg, &format!("not an envelope: {e}")).await;
                return None;
            }
        };
        // A forged sender is buried without its envelope, so a replay cannot pass it on as genuine.
        if let Err(e) = envelope.check_subject(&msg.subject) {
            self.bury_unanswerable(msg, &format!("sender mismatch: {e}")).await;
            return None;
        }
        if envelope.approval.is_some() {
            if let Some(approvals) = self.approvals {
                approvals.offer(&envelope);
            }
            if let Err(e) = bus::double_ack(self.client, msg).await {
                tracing::warn!(error = %e, "acking an approval decision");
            }
            return None;
        }
        envelope.owner = self.humans.human_owner(envelope.owner.as_ref());
        envelope.creator = self.humans.ant_creator(envelope.creator.as_ref());
        if self.claim(&envelope, msg).await {
            return None;
        }
        Some(Incoming { envelope, post: None, resume: None })
    }

    /// The channel post `msg` to answer, if it wakes the ant ([`channel::wake`]). Any other is acked at once: the
    /// ant's own, one in a channel it is not a member of, one that does not @mention it and is not for it to
    /// triage. A malformed or forged one is dropped, with no dead letter: every daemon sees it. The post kept names
    /// its thread's owner ([`ChannelPost::thread_owner`]), a human, and its creator ([`ChannelPost::thread_creator`]),
    /// an ant.
    async fn admit_post(&self, msg: &jetstream::Message, channels: &mut Channels) -> Option<Incoming> {
        let post = serde_json::from_slice::<ChannelPost>(&msg.payload)
            .map_err(|e| e.to_string())
            .and_then(|post| post.check_subject(&msg.subject).map(|()| post).map_err(|e| e.to_string()));
        let mut post = match post {
            Ok(post) => post,
            Err(error) => {
                tracing::warn!(subject = %msg.subject, error, "dropping a channel post");
                if let Err(e) = msg.ack_with(AckKind::Term).await {
                    tracing::warn!(error = %e, "terminating the channel post");
                }
                return None;
            }
        };
        let humans = self.humans.humans();
        let mut wake = channel::wake(self.ant, channels.get(&post.channel), &post, &humans);
        let unseen = channels.get(&post.channel).is_none() && post.mentions.is_empty() && humans.contains(&post.from);
        if wake == Wake::Skip && (post.mentions(self.ant) && post.from != *self.ant || unseen) {
            // The post may have overtaken the change that made the ant a member, or created the channel.
            channels.refresh(&post.channel).await;
            wake = channel::wake(self.ant, channels.get(&post.channel), &post, &humans);
        }
        if wake == Wake::Skip {
            if let Err(e) = msg.ack().await {
                tracing::warn!(error = %e, "acking a channel post");
            }
            return None;
        }
        let triage = (wake == Wake::Triage).then(|| channels.get(&post.channel).cloned()).flatten();
        let owned = channels.get(&post.channel).is_some_and(|e| e.owner.as_ref() == Some(self.ant));
        post.owner = post.thread_owner(&humans);
        post.creator = post.thread_creator(&humans);
        let envelope = channel::as_envelope(&post, self.ant);
        Some(Incoming { envelope, post: Some(Box::new(Posted { post, triage, owned })), resume: None })
    }

    /// Whether `envelope` is a reply to an `ask_agent` call, now handed over and acked. It must not wait in its
    /// thread's queue: the prompt that asked is in flight on that thread.
    async fn claim(&self, envelope: &Envelope, msg: &jetstream::Message) -> bool {
        let Some(mcp) = self.mcp else { return false };
        match mcp.claim(envelope).await {
            Ok(false) => false,
            Ok(true) => {
                if let Err(e) = bus::double_ack(self.client, msg).await {
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
        if let Err(e) = dead::bury(self.js, self.ant, msg, None, error).await {
            // Never redelivered, dead letter or not.
            tracing::warn!(error = format!("{e:#}"), "dead-lettering");
            if let Err(e) = msg.ack_with(AckKind::Term).await {
                tracing::warn!(error = %e, "terminating the message");
            }
        }
    }

    /// Answer one message. A failed attempt is redelivered after a delay, and the last one becomes a dead letter.
    /// Returns its thread, and the redelivery to wait for when there is one.
    async fn handle(&self, mut incoming: Incoming, msg: jetstream::Message) -> (ThreadId, Option<Retry>) {
        let _working = self.workload.working();
        let envelope = &incoming.envelope;
        let thread = envelope.thread_id.clone();
        let (delivered, seq) = match msg.info() {
            Ok(info) => (info.delivered, info.stream_sequence),
            Err(e) => {
                self.bury_unanswerable(&msg, &format!("no delivery info: {e}")).await;
                return (thread, None);
            }
        };
        if self.answered.has(envelope).await {
            // Its ack failed, as while a leaf node's uplink is down; its reply is in the stream or on its way there.
            tracing::info!(%thread, "acking a message whose reply is stored already");
            match bus::double_ack(self.client, &msg).await {
                Ok(()) => {
                    self.attempts.forget(seq).await;
                    self.answered.forget(envelope).await;
                }
                Err(e) => tracing::warn!(%thread, error = %e, "acking a message whose reply is stored"),
            }
            return (thread, None);
        }
        if let Some(dead) = self.buried.get(envelope).await {
            // Its dead letter went to the machine's outbox while the hub was out of reach; the redelivery finishes it.
            tracing::info!(%thread, "burying a message whose dead letter is in the outbox");
            match self.bury_letter(&incoming, &msg, &dead).await {
                Ok(()) => self.attempts.forget(seq).await,
                Err(e) => tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering"),
            }
            return (thread, None);
        }
        let attempt = Attempt::of(delivered, MAX_ATTEMPTS);
        let marker = self.attempts.get(seq, envelope).await;
        if attempt == Attempt::Overdue && !matches!(marker, Ok(Some(Marker::Replied(_)))) {
            let error = format!("no outcome after {MAX_ATTEMPTS} attempts: the daemon stopped or the ack wait ran out");
            match self.bury(&incoming, &msg, &error).await {
                Ok(_) => self.attempts.forget(seq).await,
                Err(e) => tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering"),
            }
            return (thread, None);
        }
        let result = match marker {
            Ok(marker) => match self.arrived(&mut incoming).await {
                Ok(true) => self.answer(&incoming, &msg, seq, marker).await,
                Ok(false) => self.done(&msg, seq).await,
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        let Err(e) = result else { return (thread, None) };
        tracing::error!(%thread, error = format!("{e:#}"), "handling message");
        status::state(self.client, self.ant, &thread, State::Failed);
        // A failed last attempt, or an overdue one that failed to send its recorded reply: no delivery follows.
        if attempt != Attempt::Early {
            match self.bury(&incoming, &msg, &format!("{e:#}")).await {
                Ok(_) => {
                    self.attempts.forget(seq).await;
                    return (thread, None);
                }
                // The overdue delivery tries again.
                Err(bury) => tracing::warn!(error = format!("{bury:#}"), "dead-lettering"),
            }
        }
        nak(&msg, Some(self.retry_delay)).await;
        let retry = (delivered < MAX_DELIVER).then(|| Retry {
            seq,
            // Redelivery may lag behind the delay; past this the thread goes on without the message.
            until: tokio::time::Instant::now() + self.retry_delay * 3,
        });
        (thread, retry)
    }

    /// Store `msg` as a dead letter, escalating it up the ant's chain (in the end to the thread's owner, else the
    /// default human), and terminate it. A channel post names its thread's owner ([`Inbox::admit_post`]), and its
    /// envelope does too. The dead letter of a channel post keeps its payload.
    async fn bury(&self, incoming: &Incoming, msg: &jetstream::Message, error: &str) -> Result<()> {
        let envelope = incoming.post.is_none().then_some(&incoming.envelope);
        let dead = dead::letter_for(self.ant, msg, envelope, error)?;
        self.bury_letter(incoming, msg, &dead).await
    }

    /// Bury `msg` as `dead` ([`dead::bury_letter`]). A dead letter kept in the machine's outbox is noted in `buried`,
    /// so the redelivery buries it again, even in a restarted daemon.
    async fn bury_letter(&self, incoming: &Incoming, msg: &jetstream::Message, dead: &DeadLetter) -> Result<()> {
        let post = incoming.post.as_ref().map(|posted| &posted.post);
        let escalator = Some(self.escalator);
        match dead::bury_letter(self.js, msg, dead, post, self.humans, escalator).await? {
            Burial::Hub(_) => self.buried.forget(&incoming.envelope).await,
            Burial::Outbox => {
                if let Err(e) = self.buried.add(&incoming.envelope, dead).await {
                    // The redelivery buries it with an error of its own, under the same message id.
                    tracing::warn!(error = format!("{e:#}"), "noting the dead letter");
                }
            }
        }
        Ok(())
    }

    /// Note the message the ant is about to answer with its escalator: a human's ends the thread's escalation and
    /// gives the thread a fresh allowance, and the escalation leader's answer gives the thread a fresh budget and
    /// allowance once per level ([`Escalator::arrived`]). The leader's reply that does so on a thread the ant got
    /// stuck on in a channel is answered in that channel, with the fresh budget: the thread's turns come from its posts.
    /// Returns whether to answer the message: a member's notice that it sent the ant's escalation higher is the
    /// daemon's alone ([`Escalator::took_climb`]).
    async fn arrived(&self, incoming: &mut Incoming) -> Result<bool> {
        let human = self.humans.is_human(&incoming.envelope.from);
        if incoming.post.is_none() && !human && self.escalator.took_climb(&incoming.envelope).await? {
            return Ok(false);
        }
        let renewed = self.escalator.arrived(&mut incoming.envelope, human).await?;
        // Not a message from ask_agent or send_message: an ask_agent call waits for the answer in the leader's inbox.
        if renewed && incoming.post.is_none() && incoming.envelope.in_reply_to.is_some() {
            let pending = self.escalator.ledger().pending(&incoming.envelope.thread_id).await?;
            incoming.resume = pending.and_then(|p| p.channel);
        }
        Ok(true)
    }

    /// Ack the message at stream sequence `seq`, which needs no reply.
    async fn done(&self, msg: &jetstream::Message, seq: u64) -> Result<()> {
        bus::double_ack(self.client, msg).await?;
        self.attempts.forget(seq).await;
        Ok(())
    }

    /// Answer the message at stream sequence `seq`, given what an earlier attempt at it left. Every attempt after one
    /// that started, whether it failed or was cut off, is told that tools may already have run; one whose reply was
    /// recorded is not prompted again.
    async fn answer(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        marker: Option<Marker>,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let cut_off = match marker {
            Some(Marker::Replied(reply)) => {
                tracing::info!(%thread, "sending the reply an earlier attempt recorded");
                return self.deliver(incoming, &reply, msg, seq).await;
            }
            Some(Marker::Started) => true,
            None => false,
        };
        // A post's thread has the turns its posts since left, which other members' replies may have spent.
        let humans = self.humans.humans();
        let read = match &incoming.post {
            Some(posted) => Some(self.threads.context(&posted.post, seq, true, &humans).await?),
            None => None,
        };
        let spent = match (&incoming.post, &read) {
            (Some(posted), Some(read)) => posted.post.reply(self.ant.clone(), "", &read.later, &humans).err(),
            _ => envelope.reply(self.ant.clone(), "", self.humans.role(&envelope.from)).err(),
        };
        if let Some(ProtoError::BudgetExhausted(_)) = spent {
            // Out of turns: escalate the thread instead of spending another prompt.
            self.escalate(incoming, "Turn budget spent", EscalationReason::TurnBudget, seq, None).await?;
            bus::double_ack(self.client, msg).await?;
            self.attempts.forget(seq).await;
            return Ok(());
        }
        match self.ledger.reached(thread).await {
            Ok(Some(cap)) => {
                // Out of allowance: the same, until a human or the escalation's leader answers, or the day ends.
                self.escalate(incoming, &cap, EscalationReason::Cap, seq, None).await?;
                bus::double_ack(self.client, msg).await?;
                self.attempts.forget(seq).await;
                return Ok(());
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(%thread, error = format!("{e:#}"), "reading what the thread spent"),
        }
        if !cut_off {
            // Durable before the harness can run anything.
            self.attempts.start(seq, envelope).await?;
        }

        tracing::info!(%thread, from = %envelope.from, cut_off, "prompting");
        status::state(self.client, self.ant, thread, State::Working);

        let _turn = self.mcp.map(|mcp| mcp.begin(envelope));
        let (spend_tx, mut spend) = tokio::sync::mpsc::unbounded_channel();
        let (stop_tx, stop) = tokio::sync::oneshot::channel();
        let mut stop_tx = Some(stop_tx);
        let mut reached = None;
        let (progress_tx, mut streamed) = tokio::sync::mpsc::channel(status::BUFFER);
        let mut status = Status::new(self.client, self.ant, thread);
        let mut flush =
            tokio::time::interval_at(tokio::time::Instant::now() + status::FLUSH_EVERY, status::FLUSH_EVERY);
        let late = match self.mcp {
            Some(mcp) => mcp.late_on(thread).await,
            None => 0,
        };
        let text = match (&incoming.post, &read) {
            (Some(posted), Some(read)) => self.channel_prompt(posted, read, &humans).await,
            _ => match escalation::group_note(self.ant, envelope) {
                Some(note) => format!("{note}\n\n{}", envelope.text),
                None => envelope.text.clone(),
            },
        };
        let text = match &incoming.resume {
            Some(channel) => {
                let left = envelope.turn_budget.saturating_sub(1);
                format!("{text}\n\n{}", channel::resume_note(channel, thread, left))
            }
            None => text,
        };
        let text = prompt_text(&text, cut_off, late);
        let prompt =
            Prompt { text, limits: self.limits, spend: Some(spend_tx), stop: Some(stop), progress: Some(progress_tx) };
        let prompt = self.pool.run(thread, prompt);
        tokio::pin!(prompt);
        let mut progress = self.progress();
        let reply = loop {
            tokio::select! {
                r = &mut prompt => break r,
                Some(update) = streamed.recv() => status.push(update),
                _ = flush.tick() => status.flush(),
                Some(spent) = spend.recv() => {
                    if let Some(cap) = self.record(thread, &spent).await {
                        if let Some(stop) = stop_tx.take() {
                            let _ = stop.send(cap.clone());
                        }
                        reached = Some(cap);
                    }
                }
                _ = progress.tick() => {
                    if let Err(e) = msg.ack_with(AckKind::Progress).await {
                        tracing::warn!(error = %e, "progress ack");
                    }
                }
            }
        };
        while let Ok(update) = streamed.try_recv() {
            status.push(update);
        }
        status.flush();
        let reply = reply?;
        // Spend reported as the prompt ended, such as its tokens.
        while let Ok(spent) = spend.try_recv() {
            if let Some(cap) = self.record(thread, &spent).await {
                reached.get_or_insert(cap);
            }
        }

        let cut = reply.cut.is_some();
        let limit = reply.cut.or(reached);
        let text = match &limit {
            None => reply.text,
            Some(limit) => {
                let notice = format!("[ants] {limit}{}", if cut { "; the prompt was cancelled." } else { "." });
                [reply.text, notice].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>().join("\n\n")
            }
        };
        let reply = match (&incoming.post, read) {
            (Some(posted), Some(read)) => {
                // Other members' replies stored during the prompt spent the thread's turns too.
                let humans = self.humans.humans();
                let later = match self.threads.context(&posted.post, seq, false, &humans).await {
                    Ok(now) => now.later,
                    Err(e) => {
                        tracing::warn!(%thread, error = format!("{e:#}"), "rereading the thread's turns");
                        read.later
                    }
                };
                match posted.post.reply(self.ant.clone(), text.as_str(), &later, &humans) {
                    Ok(post) => Reply::Post(post),
                    Err(ProtoError::BudgetExhausted(_)) => {
                        // The answer is not posted: the escalation carries it.
                        let why = "Turn budget spent before the answer was posted";
                        self.escalate(incoming, why, EscalationReason::TurnBudget, seq, Some(&text)).await?;
                        bus::double_ack(self.client, msg).await?;
                        self.attempts.forget(seq).await;
                        return Ok(());
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            _ if let Some(channel) = &incoming.resume => {
                let post = ChannelPost::on_thread(channel.clone(), thread.clone(), self.ant.clone(), text);
                let post = post.with_owner(envelope.owner.clone()).with_creator(envelope.creator.clone());
                Reply::Post(post.spend(envelope.turn_budget)?)
            }
            _ => {
                // A reply on an escalation carries it, so its leader's answer is known as one.
                let mut reply = envelope.reply(self.ant.clone(), text, self.humans.role(&envelope.from))?;
                match escalation::route(self.ant, envelope).map(|(_, route)| route) {
                    Some(Route::Notes) => {
                        let (store, budget) = (self.notes, envelope.turn_budget);
                        match notes::spend(store, thread, self.ant, &reply.text, budget).await? {
                            Some(post) => Reply::Notes(post),
                            None => {
                                let why = "Thread notes out of turns before the answer was posted";
                                let reason = EscalationReason::Notes;
                                self.escalate(incoming, why, reason, seq, Some(&reply.text)).await?;
                                return self.done(msg, seq).await;
                            }
                        }
                    }
                    route => {
                        if let Some(Route::Stuck(stuck)) = route {
                            reply.to = stuck;
                        }
                        Reply::Message(reply.with_escalation(envelope.escalation.as_deref().cloned()))
                    }
                }
            }
        };
        // Before the reply is recorded, so a redelivery that only resends the reply still escalates; its message id
        // keeps a second escalation out of the stream.
        if let Some(limit) = limit
            && !self.humans.is_human(&envelope.from)
        {
            self.escalate(incoming, &limit, EscalationReason::Cap, seq, None).await?;
        }
        // From here on a redelivery sends this reply rather than prompting again. Out of the hub's reach, as while a
        // leaf node's uplink is down, the reply still goes to the machine's outbox, and `answered` keeps it from being
        // prompted again.
        if let Err(e) = self.attempts.replied(seq, envelope, &reply).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "recording the reply");
        }
        self.deliver(incoming, &reply, msg, seq).await
    }

    /// The prompt for a channel post, with what was read of its thread: the posts not shown before, and the turns left.
    async fn channel_prompt(&self, posted: &Posted, read: &Context, humans: &Humans) -> String {
        let post = &posted.post;
        let left = post.turns_left(&read.later, humans).saturating_sub(1);
        let mut text = match &posted.triage {
            None => channel::prompt(self.ant, post, read, left),
            Some(entry) => {
                let roster = (self.roster)().await.unwrap_or_else(|e| {
                    tracing::warn!(error = format!("{e:#}"), "reading the roster; members show as offline");
                    Vec::new()
                });
                channel::triage_prompt(post, entry, &roster, read, left)
            }
        };
        if posted.owned {
            text.push_str(&format!("\n\n{}", channel::owner_note(&post.channel)));
        }
        text
    }

    /// Store `reply` to `request`, the message at stream sequence `seq`, in the hub's stream or, out of its reach, in
    /// the machine's outbox, then ack the message and forget its marker. The message id, from the request's, makes
    /// JetStream drop a copy an earlier attempt stored within the stream's duplicate window. When the ack fails, the
    /// note in `answered` has the redelivery acked without answering again, even by a restarted daemon. Of a channel
    /// post it also notes that the posts on its thread up to it were shown.
    async fn deliver(&self, incoming: &Incoming, reply: &Reply, msg: &jetstream::Message, seq: u64) -> Result<()> {
        let request = &incoming.envelope;
        // Store the reply before acking: a crash in between redelivers rather than loses the message. A post to thread
        // notes is in the notes already.
        if !matches!(reply, Reply::Notes(_)) {
            bus::store(self.js, reply.subject(), reply, &reply_id(self.ant, request)).await?;
        }
        if incoming.post.is_some()
            && let Err(e) = self.threads.seen(&request.thread_id, seq).await
        {
            tracing::warn!(thread = %request.thread_id, error = format!("{e:#}"), "noting the posts shown");
        }
        let noted = self
            .answered
            .add(request, reply)
            .await
            .inspect_err(|e| tracing::warn!(error = format!("{e:#}"), "noting the stored reply"));
        match bus::double_ack(self.client, msg).await {
            Ok(()) => {
                self.attempts.forget(seq).await;
                self.answered.forget(request).await;
            }
            Err(e) if noted.is_ok() => {
                tracing::warn!(thread = %request.thread_id, error = %e, "acking; the redelivery is acked unanswered");
            }
            Err(e) => return Err(e),
        }
        status::state(self.client, self.ant, reply.thread_id(), State::Idle);
        Ok(())
    }

    /// Count what the harness spent on the thread. Returns the cap reached, if any.
    async fn record(&self, thread: &ThreadId, spent: &Spend) -> Option<String> {
        self.ledger.record(thread, spent).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing what the thread spent");
            None
        })
    }

    /// Escalate the thread for `reason` up its origin chain, in the end to its owner, else the default human
    /// ([`Escalator::escalate`]): `why` it needs a decision, and the message or post it was on, and an `unposted`
    /// answer. Its message id, from the stream sequence `seq` of the message, keeps a redelivery from escalating twice.
    async fn escalate(
        &self,
        incoming: &Incoming,
        why: &str,
        reason: EscalationReason,
        seq: u64,
        unposted: Option<&str>,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        let human = self.humans.owner_of(envelope);
        let last = match &incoming.post {
            Some(posted) => format!("Last post from {} in #{}", envelope.from, posted.post.channel),
            None => format!("Last message from {}", envelope.from),
        };
        let mut text = format!("{why}; needs a decision. {last}:\n{}", envelope.text);
        if let Some(answer) = unposted {
            text.push_str(&format!("\n\nUnposted answer from {}:\n{answer}", self.ant));
        }
        let mut notice = envelope.follow_up(self.ant.clone(), human, text);
        notice.in_reply_to = Some(envelope.id);
        let id = format!("escalation-{}-{seq}", self.ant);
        let stuck = Stuck::new(reason, notice, id, Some(envelope));
        let channel = incoming.post.as_ref().map(|posted| posted.post.channel.clone());
        let escalated = self.escalator.escalate(Stuck { channel, ..stuck }).await?;
        tracing::info!(thread = %envelope.thread_id, why, %escalated, "escalated");
        Ok(())
    }

    fn progress(&self) -> tokio::time::Interval {
        tokio::time::interval_at(tokio::time::Instant::now() + self.progress_every, self.progress_every)
    }
}

impl Thread {
    /// Whether the message with stream sequence `seq` is in flight or waiting. The server redelivers one whose progress
    /// acks were lost, as on a link that went silent for longer than the ack wait.
    fn holds(&self, seq: Option<u64>) -> bool {
        seq.is_some() && (self.in_flight == seq || self.waiting.iter().any(|(_, msg)| stream_seq(msg) == seq))
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
    match &next {
        Some((_, msg)) => {
            t.busy = true;
            t.in_flight = stream_seq(msg);
        }
        None => {
            entry.remove();
        }
    }
    next
}

/// The prompt for a message: warned when an earlier attempt at it was cut off, and noting the `late` replies to
/// `ask_agent` that wait on its thread for `check_inbox`.
fn prompt_text(text: &str, cut_off: bool, late: usize) -> String {
    let late = match late {
        0 => None,
        1 => {
            Some("[ants] 1 late reply to ask_agent is waiting on this thread; call check_inbox to read it.".to_owned())
        }
        n => Some(format!(
            "[ants] {n} late replies to ask_agent are waiting on this thread; call check_inbox to read them."
        )),
    };
    let cut_off = cut_off.then(|| CUT_OFF.to_owned());
    cut_off.into_iter().chain(late).chain([text.to_owned()]).collect::<Vec<_>>().join("\n\n")
}

/// The message id of `ant`'s reply to `request`: the same for every attempt at it.
fn reply_id(ant: &UserId, request: &Envelope) -> String {
    format!("reply-{ant}-{}", request.id)
}

/// Whether `msg` came through the channel consumer.
fn is_post(msg: &jetstream::Message) -> bool {
    let prefix = subject::ALL_CHANNELS.trim_end_matches('>');
    msg.subject.starts_with(prefix)
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
    use std::sync::Arc;

    use ants_proto::{ChannelEntry, ChannelId, DEFAULT_TURN_BUDGET, Role, StatusEvent, StatusUpdate};
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
        markers: jetstream::kv::Store,
        dev: UserId,
        limits: PromptLimits,
        ledger: Arc<Ledger>,
        /// dev-a's escalation chain.
        chain: Chain,
        workload: roster::Workload,
        mcp: Option<Mcp>,
        channels: jetstream::kv::Store,
        threads: Threads,
        roster: Roster,
        humans: Registry,
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
            let markers = bus::ensure_attempts(&js).await.unwrap();
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
            let threads = Threads::new(js.clone(), dev.clone(), bus::ensure_sessions(&js).await.unwrap());
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
                mcp: None,
                channels,
                threads,
                roster,
                humans: Registry::default(),
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
                instance: None,
                harness: "copilot".to_owned(),
                capabilities: Vec::new(),
                whoami: None,
                workdir: None,
                started_at: now,
                heartbeat_at: now,
                activity: None,
                handling: None,
                harnesses: None,
                escalate_to: escalate_to.to_vec(),
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

        async fn marked(&self, seq: u64) -> bool {
            self.markers.get(ants_proto::attempt_key(&self.dev, seq)).await.unwrap().is_some()
        }

        /// Serve the inbox until a permit is added to `ctrl_c` (twice to stop mid-prompt).
        async fn serve(&self, pool: &Pool, ctrl_c: &Arc<Semaphore>) {
            self.serve_timed(pool, ctrl_c, PROGRESS_EVERY, ACK_WAIT).await;
        }

        async fn serve_timed(
            &self,
            pool: &Pool,
            ctrl_c: &Arc<Semaphore>,
            progress_every: Duration,
            ack_wait: Duration,
        ) {
            let messages = bus::inbox_consumer(&self.stream, &self.dev, ack_wait, MAX_DELIVER, 4).await.unwrap();
            let posts = bus::chan_consumer(&self.stream, &self.dev, ack_wait, MAX_DELIVER, 4).await.unwrap();
            let messages = futures::stream::select(messages, posts);
            let channels = Channels::watch(self.channels.clone()).await.unwrap();
            let escalator = self.escalator();
            let notes = crate::notes::JetStreamNotes::new(self.js.clone(), self.dev.clone());
            let inbox = Inbox {
                client: &self.client,
                js: &self.js,
                ant: &self.dev,
                pool,
                attempts: &self.attempts,
                answered: &self.answered,
                buried: &self.buried,
                mcp: self.mcp.as_ref(),
                approvals: None,
                workload: &self.workload,
                limits: self.limits,
                ledger: &self.ledger,
                escalator: &escalator,
                notes: &notes,
                threads: &self.threads,
                roster: &self.roster,
                humans: &self.humans,
                retry_delay: Duration::from_millis(300),
                progress_every,
            };
            let stop = || {
                let ctrl_c = ctrl_c.clone();
                async move { ctrl_c.acquire().await.unwrap().forget() }
            };
            tokio::select! {
                () = inbox.serve(messages, channels, IDLE, stop) => {}
                () = escalator.deadlines() => {}
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
    async fn progress_streams_to_the_status_subject() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, _, _) = fake::launch(true);
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
                    (Some(StatusUpdate::Message { text }), StatusUpdate::Message { text: more }) => {
                        text.push_str(&more)
                    }
                    (_, update) => updates.push(update),
                }
            }
            let tool = StatusUpdate::Tool {
                id: "call-0".to_owned(),
                title: Some("tool".to_owned()),
                tool_kind: Some("other".to_owned()),
                status: Some("pending".to_owned()),
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
    async fn a_failed_message_is_retried_before_the_rest_of_its_thread() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
        assert_eq!(log.lock().unwrap().answered, [warned("flaky a"), "b".to_owned()]);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_forged_sender_is_dead_lettered_unanswered() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
        assert_eq!(log.lock().unwrap().answered, ["genuine"]);
        let letters = dead::list(&bus.stream).await.unwrap();
        let [(_, letter)] = letters.as_slice() else { panic!("{letters:?}") };
        assert!(letter.error.contains("sender mismatch"), "{}", letter.error);
        assert_eq!(letter.envelope, None, "a forged envelope is not replayable");
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn an_approval_decision_is_acked_without_a_prompt() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let thread = ThreadId::generate();
        let dev_b: UserId = "dev-b".parse().unwrap();
        let decision = ants_proto::ApprovalDecision::new(
            ants_proto::Uuid::new_v4(),
            bus.dev.clone(),
            ants_proto::Verdict::AllowOnce,
        );
        let late = Envelope::decision(thread.clone(), dev_b, decision);
        crate::bus::publish_json(&bus.js, late.subject(), &late, None).await.unwrap();
        bus.send(&thread, "genuine").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (thread.clone(), "genuine".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, ["genuine"]);
        assert!(dead::list(&bus.stream).await.unwrap().is_empty());
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn inbox_consumers_from_before_sender_subjects_get_the_new_filter() {
        let Some((bus, _)) = Bus::start().await else { return };
        let human = UserId::human();
        for user in [&bus.dev, &human] {
            let name = ants_proto::inbox_consumer(user);
            let old = jetstream::consumer::pull::Config {
                durable_name: Some(name.clone()),
                filter_subject: format!("ants.inbox.{user}"),
                ..Default::default()
            };
            bus.stream.create_consumer(old).await.unwrap();
        }
        drop(crate::bus::inbox_consumer(&bus.stream, &bus.dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap());
        crate::bus::human_inbox_consumer(&bus.stream, &human, ACK_WAIT).await.unwrap();
        for user in [&bus.dev, &human] {
            let mut consumer: PullConsumer = bus.stream.get_consumer(&ants_proto::inbox_consumer(user)).await.unwrap();
            assert_eq!(consumer.info().await.unwrap().config.filter_subject, subject::inbox_of(user));
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

    #[tokio::test]
    async fn progress_acks_keep_messages_in_hand_past_the_ack_wait() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));
        // A consumer left by an older daemon with a longer ack wait takes the new one.
        drop(bus::inbox_consumer(&bus.stream, &bus.dev, ack_wait * 100, MAX_DELIVER, 4).await.unwrap());
        let (launch, log, gate) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
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
        let (launch, log, gate) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;

        // Progress acks lost, as on a silent link: both messages come back while still in hand.
        let ack_wait = Duration::from_millis(300);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
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
        assert_eq!(log.lock().unwrap().answered, ["hold a", "b"]);
        assert_eq!(bus.unacked().await, 0, "acks of the first delivery count");
    }

    #[tokio::test]
    async fn a_prompt_past_its_limit_is_cancelled_and_the_human_told() {
        let Some((mut bus, mut replies)) = Bus::start().await else { return };
        bus.limits.timeout = Some(Duration::from_millis(100));
        let (launch, log, _) = fake::launch(true);
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
        assert_eq!(log.lock().unwrap().cancelled, 1);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_first_delivery_is_prompted_as_sent_and_leaves_no_marker() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        let (_, seq) = bus.send(&t1, "a").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "a".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, ["a"]);
        assert!(!bus.marked(seq).await, "the marker is removed with the ack");
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_spent_cap_hands_the_thread_to_the_human_until_the_human_answers() {
        let Some((mut bus, mut to_human)) = Bus::start().await else { return };
        bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
        let (launch, log, _) = fake::launch(true);
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
        let log = log.lock().unwrap();
        assert_eq!((log.cancelled, log.answered.as_slice()), (1, &["go on".to_owned()][..]));
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

        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let dev_b: UserId = "dev-b".parse().unwrap();
        let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
        let mut to_alice = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
        bus.client.flush().await.unwrap();
        let t1 = ThreadId::generate();
        let mut go_on = Envelope::on_thread(t1.clone(), alice.clone(), bus.dev.clone(), "go on");
        go_on.turn_budget = 0;
        for envelope in [
            Envelope::on_thread(t1.clone(), dev_b.clone(), bus.dev.clone(), "cost 2 stall")
                .with_owner(Some(alice.clone())),
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
        assert_eq!(log.lock().unwrap().answered, ["go on"]);
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
        let [mut to_lead, mut to_dev_b, mut to_boss] =
            inboxes(&bus, &[lead.clone(), dev_b.clone(), boss.clone()]).await;
        let (launch, log, _) = fake::launch(true);
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
        assert_eq!(log.lock().unwrap().answered, ["go on", "from boss"]);
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
        let (launch, log, _) = fake::launch(true);
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
        let answered = log.lock().unwrap().answered.clone();
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
        bus.chain = Chain { escalate_to: vec![lead.clone()], max_hops: 1, after: None };
        bus.put_roster(&lead, &[boss]).await;
        let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
        let (launch, log, _) = fake::launch(true);
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
        assert_eq!(log.lock().unwrap().answered, ["go on", "carry on"]);
    }

    #[tokio::test]
    async fn an_unanswered_escalation_goes_one_level_higher_after_escalate_after() {
        let Some((mut bus, _to_human)) = Bus::start().await else { return };
        let [lead, boss, carol] = ants(["lead", "boss", "carol"]);
        bus.chain =
            Chain { escalate_to: vec![lead.clone()], after: Some(Duration::from_millis(300)), ..Chain::default() };
        bus.put_roster(&lead, std::slice::from_ref(&boss)).await;
        let [mut to_lead, mut to_boss] = inboxes(&bus, &[lead.clone(), boss.clone()]).await;
        let (launch, _, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send_spent(&carol, &t1, "stuck", None).await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            let one = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
            assert!(timeout(Duration::from_millis(150), to_boss.next()).await.is_err(), "not before the deadline");
            let up = next_envelope(&mut to_boss).await;
            assert_eq!(up.escalation.as_deref().map(|e| (e.level, &e.leader)), Some((2, &boss)));
            let late = format!(
                "[ants] No answer from lead on thread {t1} within 300 ms; the escalation goes one level higher."
            );
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
        crate::notes::read(&bus.js, &"reader".parse().unwrap(), thread).await.unwrap().posts
    }

    #[tokio::test]
    async fn only_the_leader_answers_the_stuck_ant_and_members_answer_in_the_thread_notes() {
        let Some((bus, _to_human)) = Bus::start().await else { return };
        let [lead, carol, boss] = ants(["lead", "carol", "boss"]);
        bus.put_roster(&lead, std::slice::from_ref(&boss)).await;
        let [mut to_carol, mut to_lead, mut to_boss] =
            inboxes(&bus, &[carol.clone(), lead.clone(), boss.clone()]).await;
        let (launch, log, _) = fake::launch(true);
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
            let envelope = Envelope::on_thread(thread.clone(), from.clone(), bus.dev.clone(), text)
                .with_escalation(Some(escalation));
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
        let prompt = log.lock().unwrap().answered.iter().find(|p| p.ends_with("\n\nhelp")).cloned().unwrap();
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
        let (launch, log, _) = fake::launch(true);
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
        assert_eq!(log.lock().unwrap().answered, ["from boss"], "the notice is the daemon's alone");
    }

    #[tokio::test]
    async fn an_owner_that_is_no_human_is_dropped_and_the_default_human_gets_the_thread() {
        let Some((bus, mut to_human)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
        assert_eq!(log.lock().unwrap().answered, ["hi"]);
    }

    #[tokio::test]
    async fn a_message_cut_off_mid_prompt_is_prompted_again_with_a_warning() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (progress_every, ack_wait) = (Duration::from_millis(100), ack_wait(Duration::from_millis(100)));
        let (launch, log, _) = fake::launch(true);
        let store = SessionStore::memory();
        let t1 = ThreadId::generate();
        let (_, seq) = bus.send(&t1, "hold a").await;

        // Stopped mid-prompt, as by a crash: the harness may have run tools, and the message was not acked.
        let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let crash = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            ctrl_c.add_permits(2);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), crash);
        // Its harness is still held mid-prompt, so it is dropped rather than shut down.
        drop(pool);
        assert!(bus.marked(seq).await, "the attempt is marked as started");
        assert!(log.lock().unwrap().answered.is_empty());

        let pool = Pool::new(launch, store, 2, IDLE);
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), warned("hold a")));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        {
            let log = log.lock().unwrap();
            assert_eq!(log.answered, [warned("hold a")]);
            assert_eq!(log.loaded, ["s1"], "the session resumes, with the history of the cut-off attempt");
        }
        assert!(!bus.marked(seq).await);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_reply_recorded_before_a_crash_is_stored_once_without_a_prompt() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        let (ask, seq) = bus.send(&t1, "a").await;

        // The crashed attempt recorded its reply and stored it, but never acked.
        let reply = ask.reply(bus.dev.clone(), "s1: done before", Role::Human).unwrap();
        bus.attempts.start(seq, &ask).await.unwrap();
        bus.attempts.replied(seq, &ask, &Reply::Message(reply.clone())).await.unwrap();
        bus::publish_json(&bus.js, reply.subject(), &reply, Some(&reply_id(&bus.dev, &ask))).await.unwrap();
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "done before".to_owned()));

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            bus.until_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().starts, 0, "not prompted again");
        let state = bus.stream.get_info().await.unwrap().state;
        assert_eq!(state.messages, 2, "the message and one copy of its reply");
        assert!(!bus.marked(seq).await);
        assert_eq!(bus.unacked().await, 0);
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
            uplinked.until(true).await;
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
        async fn switch(&self, up: bool) {
            Self::write_uplink(self.leaf.dir(), if up { self.uplink_port } else { self.dead_port });
            let reload = std::process::Command::new("kill").arg("-HUP").arg(self.leaf.pid().to_string()).status();
            assert!(reload.unwrap().success());
            self.until(up).await;
        }

        async fn until(&self, up: bool) {
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
    }

    /// dev-a's daemon on a leaf node replies while the leaf's uplink is down: the reply goes to the machine's outbox
    /// and the ack fails. Once the uplink is back, the redelivery to a restarted daemon is acked, not answered again.
    #[tokio::test]
    async fn a_reply_kept_in_the_outbox_answers_its_message_once() {
        let Some((hub, uplinked)) = Uplinked::start().await else { return };
        let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
        let (launch, log, gate) = fake::launch(true);
        let t1 = ThreadId::generate();
        let ask = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "hold a");
        let seq = bus::publish_json(&uplinked.hub_js, ask.subject(), &ask, None).await.unwrap();
        let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));

        let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let down = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            uplinked.switch(false).await;
            gate.add_permits(1);
            until("the answer", || log.lock().unwrap().answered.len() == 1).await;
            // Stopped, as for a restart, after the prompt in flight is handled.
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
        drop(pool);
        assert_eq!(uplinked.kept().await, 1, "the reply is kept in the outbox");

        uplinked.switch(true).await;
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let check = async {
            bus.until_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        assert_eq!(log.lock().unwrap().answered, ["hold a"], "answered once");
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
        let Some((hub, uplinked)) = Uplinked::start().await else { return };
        let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
        let (launch, log, gate) = fake::launch(true);
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
            until("the last attempt", || log.lock().unwrap().doomed == attempts).await;
            uplinked.switch(false).await;
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
        assert!(kept.error.contains("doomed"), "{}", kept.error);
        assert!(dead::list(&stream).await.unwrap().is_empty(), "not in the hub's stream yet");

        uplinked.switch(true).await;
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let check = async {
            bus.until_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        assert_eq!(log.lock().unwrap().doomed, attempts, "not prompted again");
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

    #[tokio::test]
    async fn the_next_prompt_mentions_late_replies_waiting_on_its_thread() {
        let Some((mut bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let dev_b: UserId = "dev-b".parse().unwrap();
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        // Two ask_agent calls on t1 that gave up, remembered across a restart.
        let asks = [1, 2].map(|_| Envelope::on_thread(t1.clone(), bus.dev.clone(), dev_b.clone(), "?"));
        let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/daemon-t/late");
        let _ = std::fs::remove_dir_all(&home);
        std::fs::create_dir_all(&home).unwrap();
        let stored = serde_json::json!({"asks": asks.iter().map(|a| (a.id, &a.to)).collect::<Vec<_>>(), "late": []});
        std::fs::write(home.join("mcp.json"), stored.to_string()).unwrap();
        let (outbox, roster) =
            (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_roster(bus.js.clone()));
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
            let note = "[ants] 2 late replies to ask_agent are waiting on this thread; call check_inbox to read them.";
            assert_eq!(got, [(t1.clone(), format!("{note}\n\na")), (t2.clone(), "b".to_owned())]);
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered.len(), 2, "the late replies are no prompts");
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_mentioned_member_answers_on_the_thread_after_the_posts_it_missed() {
        let Some((bus, _)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
        let first = log.lock().unwrap().answered.clone();
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
        let second = log.lock().unwrap().answered[1].clone();
        let shown = "Earlier posts on the thread not shown before, oldest first:\nhuman: rollback plan is ready\n\n";
        assert!(second.contains(shown), "{second}");
        assert!(!second.contains("deploy friday") && !second.contains("dev-a: "), "{second}");
        assert!(second.ends_with(&format!("\n\n{}", channel::owner_note(&ops))), "{second}");
    }

    #[tokio::test]
    async fn the_ant_that_started_a_channel_thread_is_its_creator_whatever_its_post_says() {
        let Some((bus, _)) = Bus::start().await else { return };
        let (launch, _, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let (ops, mut replies) = bus.channel("ops", &["dev-a", "lead"]).await;
        let root =
            ChannelPost::new(ops, "lead".parse().unwrap(), "@dev-a fix it").with_creator(Some("boss".parse().unwrap()));
        let seq = bus.post(&root).await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            let reply = next_post(&mut replies).await;
            assert_eq!(
                (&reply.thread_id, &reply.creator, &reply.owner),
                (&root.thread_id, &Some("lead".parse().unwrap()), &None)
            );
            bus.until_posts_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
    }

    #[tokio::test]
    async fn posts_that_do_not_wake_the_ant_are_acked_without_a_prompt() {
        let Some((bus, mut human)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
        let answered = log.lock().unwrap().answered.clone();
        assert_eq!(answered.len(), 1, "{answered:?}");
        assert!(answered[0].contains("Post from human mentioning dev-a:\n@dev-a go"), "{}", answered[0]);
    }

    #[tokio::test]
    async fn members_woken_by_one_post_spend_the_thread_s_turns_together() {
        let Some((bus, mut human)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
        let shipped =
            bus.post(&ship.reply("dev-b".parse().unwrap(), "shipped", &[], &Humans::default()).unwrap()).await;

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
        let answered = log.lock().unwrap().answered.clone();
        assert_eq!(answered.len(), 1, "no prompt once the thread is out of turns: {answered:?}");
        assert!(answered[0].contains("(0 left after it)"), "{}", answered[0]);
    }

    #[tokio::test]
    async fn the_messenger_answers_a_post_mentioning_nobody_with_the_members_it_concerns() {
        let Some((bus, _)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
            instance: None,
            harness: "copilot".to_owned(),
            capabilities: vec!["ci".to_owned()],
            whoami: Some("Keeps the build green.".to_owned()),
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
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
        let prompt = log.lock().unwrap().answered[0].clone();
        assert!(prompt.contains(channel::TRIAGE_RULES), "{prompt}");
        let members = "- @dev-b [ci]: Keeps the build green.\n- @dev-c (offline: not in the roster)\n";
        assert!(prompt.contains(members), "{prompt}");
    }

    #[tokio::test]
    async fn a_thread_a_human_started_stays_hers_and_goes_back_to_her_out_of_turns() {
        let Some((mut bus, _)) = Bus::start().await else { return };
        bus.humans = Registry::with(UserId::human(), &["alice"]);
        let (launch, log, _) = fake::launch(true);
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
        // No owner on it, as from before posts carried one: alice, a human, started the thread.
        let root = ChannelPost::new(entry.channel.clone(), alice.clone(), "the build is red");
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
            assert_eq!(spent.owner.as_ref(), Some(&alice), "agents' posts keep the owner");
            let seq = bus.post(&spent).await;
            let escalation = next_envelope(&mut alice_inbox).await;
            assert_eq!((&escalation.thread_id, escalation.in_reply_to), (&root.thread_id, Some(spent.id)));
            assert_eq!((&escalation.to, escalation.owner.as_ref()), (&alice, Some(&alice)));
            assert!(escalation.text.starts_with("Turn budget spent; needs a decision."), "{}", escalation.text);
            bus.until_posts_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        let answered = log.lock().unwrap().answered.clone();
        assert_eq!(answered.len(), 1, "alice's post only: {answered:?}");
        assert!(answered[0].contains(channel::TRIAGE_RULES), "{}", answered[0]);
        assert!(answered[0].contains("Post from alice mentioning nobody:\nthe build is red"), "{}", answered[0]);
    }
}
