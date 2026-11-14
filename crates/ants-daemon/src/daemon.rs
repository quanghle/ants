//! The inbox loop of one ant.
//!
//! Messages of one thread are handled one at a time, in order; different threads run concurrently, each in its own
//! harness from the pool.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Display;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ants_proto::{
    CancelRequest, ChannelEntry, ChannelId, ChannelPost, DeadLetter, Envelope, Error as ProtoError, EscalationReason,
    Humans, JsDomain, Role, State, ThreadId, ThreadOrigin, UserId, subject,
};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use futures::future::{FusedFuture as _, FutureExt as _};
use futures::stream::{FuturesUnordered, Stream, StreamExt as _};

use crate::approvals::NatsApprover;
use crate::attempts::{Answered, Attempts, Marker, Reply};
use crate::caps::{Caps, Ledger, Recorded, Warning};
use crate::channel::{self, Channels, Context, Threads, Wake};
use crate::dead::{self, Attempt, Burial, Buried};
use crate::escalation::{self, Chain, Escalator, Route, Stuck};
use crate::harness::{self, Harness, Launch, ModelSettings, Prompt, PromptLimits, SignedOut, Spend};
use crate::humans::Registry;
use crate::mcp::{Mcp, Roster};
use crate::notes::{self, NotesStore};
use crate::overflow::ContextOverflow;
use crate::permission::{Allowlist, PermissionPolicy};
use crate::pool::{Pool, SessionStore};
use crate::ratelimit::{self, Backoffs, Limited, Throttle};
use crate::signin::{self, SignIn};
use crate::status::{self, Status};
use crate::webhooks::Webhooks;
use crate::worktrees::{Branch, Worktrees};
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
/// How long a human's message from a machine's outbox waits for its thread's owner to be recorded.
const OWNER_LANDS_WITHIN: Duration = Duration::from_secs(10);
/// The header JetStream marks a message it sourced from another stream with.
const STREAM_SOURCE: &str = "Nats-Stream-Source";
/// How often idle harnesses are looked for.
const EVICT_EVERY: Duration = Duration::from_secs(30);
/// How often the worktrees of threads whose session expired are looked for, besides at start.
const PRUNE_WORKTREES_EVERY: Duration = Duration::from_hours(1);
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

#[derive(Debug, Clone, PartialEq)]
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
    pub harness: std::sync::Arc<Harness>,
    pub model_settings: ModelSettings,
    /// Tool requests the harness may run without asking (see [`crate::permission`] for the rules). Every other one
    /// goes to the human on `ants.approvals.<ant>`.
    pub allow: Vec<String>,
    /// How long a tool request waits for the human before it is rejected.
    pub approval_timeout: Duration,
    /// Most harness processes alive at once; the least recently used idle one stops to make room.
    pub max_harnesses: usize,
    /// Each thread works in its own git worktree of the workdir's repository, under the home ([`Worktrees`]).
    pub worktrees: bool,
    /// A thread's harness stops after this long without a message, and resumes its session on the next one.
    pub idle_timeout: Duration,
    /// Capabilities the ant advertises in the roster.
    pub capabilities: Vec<String>,
    /// Limits on every prompt.
    pub limits: PromptLimits,
    /// Token and cost caps of each thread and of the ant.
    pub caps: Caps,
    /// The share of a cap, in percent, past which a human is warned once; 0 for never.
    pub spend_warn_percent: u8,
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
    run_until(config, &Stop::default()).await
}

/// A request to stop serving one ant, as the first Ctrl-C does: it takes no more messages, hands back those waiting
/// and finishes the prompts in flight; a later Ctrl-C or SIGTERM stops it at once.
#[derive(Debug, Clone, Default)]
pub struct Stop(std::sync::Arc<tokio::sync::Notify>);

impl Stop {
    /// Ask the daemon to stop, also before it started serving.
    pub fn request(&self) {
        self.0.notify_one();
    }
}

/// [`run`], also stopping on `stop`'s request.
pub async fn run_until(config: Config, stop: &Stop) -> Result<()> {
    let _serving = crate::metrics::ant(&config.ant).serving();
    if crate::worktrees::shares_git_workdir(&config.workdir, config.max_harnesses, config.worktrees) {
        tracing::warn!(
            ant = %config.ant,
            workdir = %config.workdir.display(),
            max_harnesses = config.max_harnesses,
            "threads running at once share one git work tree and may clobber each other's edits; \
             worktrees = true gives each thread its own"
        );
    }
    while Box::pin(serve(&config, stop)).await? == Served::HandedOver {
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

/// Serve the ant's inbox until Ctrl-C or `stop`, or until another daemon takes over its roster entry.
async fn serve(config: &Config, stop: &Stop) -> Result<Served> {
    let allow = Allowlist::parse(&config.allow)?;
    let Some((client, js)) = bus::connect_retrying(
        &config.nats_url,
        config.creds.as_deref(),
        config.js_domain.as_ref(),
        &config.ant,
        &format!("antsd-{}", config.ant),
        stopped(stop),
    )
    .await?
    else {
        tracing::info!("stopped before connecting");
        return Ok(Served::Stopped);
    };
    let metrics = crate::metrics::ant(&config.ant);
    let workload = std::sync::Arc::new(roster::Workload::default());
    let _connected = metrics.connected(client.clone(), workload.clone());
    let mut entry = roster::entry(config).await;
    let mut claimed = None;
    if config.defer_to_others {
        let kv = bus::ensure_roster(&js, ants_proto::ROSTER_TTL).await?;
        tokio::select! {
            revision = roster::wait_for_turn(&kv, &mut entry, ants_proto::ROSTER_HEARTBEAT) => claimed = Some(revision?),
            () = stopped(stop) => {
                tracing::info!("stopped before serving");
                return Ok(Served::Stopped);
            }
        }
    }
    let humans = Registry::new(config.human.clone());
    let _watching = humans.watch(js.clone(), crate::humans::RETRY);
    let approver = NatsApprover::new(client.clone(), js.clone(), config.ant.clone(), config.approval_timeout)
        .with_chain(config.escalation.clone())
        .with_humans(humans.clone());
    let policy = PermissionPolicy::new(allow, Some(std::sync::Arc::new(approver.clone())));
    let (outbox, roster) = (crate::mcp::jetstream_outbox(js.clone()), crate::mcp::jetstream_roster(js.clone()));
    let notes = std::sync::Arc::new(crate::notes::JetStreamNotes::new(js.clone(), config.ant.clone()));
    let sessions_kv = bus::ensure_sessions(&js, config.js_domain.as_ref(), &config.ant).await?;
    let ledger = Ledger::kv(sessions_kv.clone(), config.ant.clone(), config.caps).warn_at(config.spend_warn_percent);
    let ledger = std::sync::Arc::new(ledger);
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
    mcp.with_lookup(crate::mcp::jetstream_lookup(js.clone()));
    mcp.manage_channels(crate::chanadm::nats_admin(client.clone(), js.clone()));
    let launch = Launch::new(
        config.harness.clone(),
        config.workdir.clone(),
        config.env.clone().into_iter().collect(),
        policy,
        config.model_settings.clone(),
    )
    .with_instructions(config.instructions.clone())
    .with_mcp(mcp.servers());
    let stream = bus::open_stream(&js, &config.ant).await?;
    let threads = Threads::new(js.clone(), config.ant.clone(), sessions_kv.clone());
    let webhooks = Webhooks::new(js.clone());
    let sessions = SessionStore::kv(sessions_kv, config.ant.clone());
    let attempts =
        Attempts::new(bus::ensure_attempts(&js, config.js_domain.as_ref(), &config.ant).await?, config.ant.clone());
    let answered = Answered::new(&config.home);
    answered.prune(bus::RETENTION).await;
    let buried = Buried::new(&config.home);
    buried.prune(bus::RETENTION).await;
    let mut pool = Pool::new(launch, sessions, config.max_harnesses, config.idle_timeout);
    let isolating =
        if config.worktrees { Worktrees::find(&config.ant, &config.workdir, &config.home).await } else { None };
    if let Some(worktrees) = isolating {
        pool = pool.with_worktrees(worktrees);
    } else if let Some(worktrees) = Worktrees::left_behind(&config.ant, &config.home).await {
        pool = pool.with_worktrees_left_behind(worktrees);
    }
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
        worktrees = config.worktrees,
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
        cancels: Cancels::default(),
        attempts: &attempts,
        answered: &answered,
        buried: &buried,
        mcp: Some(&mcp),
        approvals: Some(&approver),
        workload: &workload,
        metrics,
        limits: config.limits,
        ledger: &ledger,
        escalator: &escalator,
        notes: notes.as_ref(),
        threads: &threads,
        webhooks: &webhooks,
        roster: &roster,
        humans: &humans,
        retry_delay: RETRY_DELAY,
        progress_every: PROGRESS_EVERY,
        sign_in: SignIn::default(),
        sign_in_every: signin::CHECK_EVERY,
        throttle: Throttle::default(),
        backoffs: ratelimit::BACKOFFS,
    };
    let (stopping, mut asked) = (std::cell::Cell::new(false), 0);
    // The first stop is Ctrl-C, a stop request or the hand-over, after which the prompts in flight finish; the second
    // only Ctrl-C or a stop request.
    let stop_or_hand_over = || {
        asked += 1;
        let (first, stopping, registration) = (asked == 1, &stopping, &registration);
        async move {
            tokio::select! {
                () = stopped(stop) => stopping.set(true),
                () = registration.lost(), if first => {
                    tracing::info!("another daemon took over the ant; taking no more messages");
                }
            }
        }
    };
    tokio::select! {
        () = inbox.serve(messages, channels, config.idle_timeout, stop_or_hand_over) => {}
        () = escalator.deadlines() => {}
        () = pool.prune_worktrees_every(PRUNE_WORKTREES_EVERY, async |kept| {
            tell_kept_branches(&js, &threads, &humans, &config.ant, kept).await;
        }) => {}
        () = inbox.hear_cancels() => {}
        () = crate::creds::recheck(config.creds.as_deref()) => {}
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
async fn channels<I>(
    js: &jetstream::Context,
    stream: &jetstream::stream::Stream<I>,
    ant: &UserId,
    in_hand: i64,
) -> Result<(Channels, jetstream::consumer::pull::Stream)> {
    let channels = Channels::watch(bus::ensure_channels(js).await?).await?;
    let posts = bus::chan_consumer(stream, ant, ACK_WAIT, MAX_DELIVER, in_hand).await?;
    Ok((channels, posts))
}

/// Ctrl-C, SIGTERM or a request on `stop`, whichever comes first.
async fn stopped(stop: &Stop) {
    let signal = async {
        if let Err(e) = stop_signal().await {
            tracing::warn!(error = %e, "waiting for Ctrl-C or SIGTERM");
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        () = signal => {}
        () = stop.0.notified() => {}
    }
}

/// Ctrl-C, or SIGTERM, which launchd and systemd send to stop a service.
pub async fn stop_signal() -> std::io::Result<()> {
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
#[derive(Clone)]
struct Incoming {
    envelope: Envelope,
    post: Option<Box<Posted>>,
    /// The channel the answer goes to instead of the sender's inbox: that of the thread, when the message is the
    /// escalation leader's reply that renewed it ([`Inbox::arrived`]).
    resume: Option<ChannelId>,
}

#[derive(Clone)]
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

/// How handling a message ended.
enum Handled {
    /// Answered, acked or buried.
    Done,
    /// Failed; its thread waits for the redelivery.
    Retry(Retry),
    /// The harness is signed out: the message, as it arrived, waits in hand for it to be signed in.
    SignedOut(Box<InHand>),
    /// A provider limit: the message, as it arrived, waits in hand for the backoff to pass.
    Limited(Box<InHand>),
}

/// What a message handed back waits for, first on its thread.
enum Wait {
    SignIn,
    Limit,
}

/// The prompt running on each thread, and where to send the human who cancels it.
#[derive(Default)]
struct Cancels(std::sync::Mutex<HashMap<ThreadId, tokio::sync::mpsc::Sender<UserId>>>);

impl Cancels {
    /// Make the prompt about to run on `thread` cancellable until the guard drops; the cancels arrive on the receiver.
    fn register<'a>(&'a self, thread: &ThreadId) -> (Cancellable<'a>, tokio::sync::mpsc::Receiver<UserId>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.0.lock().unwrap().insert(thread.clone(), tx);
        (Cancellable { cancels: self, thread: thread.clone() }, rx)
    }

    /// Cancel the prompt running on `thread` for `by`, if one is; one cancelled already stays so.
    fn cancel(&self, thread: &ThreadId, by: UserId) -> bool {
        use tokio::sync::mpsc::error::TrySendError;
        let cancels = self.0.lock().unwrap();
        cancels.get(thread).is_some_and(|tx| !matches!(tx.try_send(by), Err(TrySendError::Closed(_))))
    }
}

struct Cancellable<'a> {
    cancels: &'a Cancels,
    thread: ThreadId,
}

impl Drop for Cancellable<'_> {
    fn drop(&mut self) {
        self.cancels.0.lock().unwrap().remove(&self.thread);
    }
}

struct Inbox<'a> {
    client: &'a async_nats::Client,
    js: &'a jetstream::Context,
    ant: &'a UserId,
    pool: &'a Pool,
    /// The prompts running, for `ants cancel`.
    cancels: Cancels,
    attempts: &'a Attempts,
    answered: &'a Answered,
    /// Messages whose dead letter waits in the machine's outbox, to bury again on their redelivery.
    buried: &'a Buried,
    mcp: Option<&'a Mcp>,
    /// Takes the decisions agents of the ant's chain send on its tool requests.
    approvals: Option<&'a NatsApprover>,
    /// Counts the messages being handled and the live harnesses, for the roster.
    workload: &'a roster::Workload,
    /// The ant's counters, for monitoring.
    metrics: std::sync::Arc<crate::metrics::Ant>,
    limits: PromptLimits,
    ledger: &'a Ledger,
    /// Sends what the ant cannot settle up its chain, and notes the answers.
    escalator: &'a Escalator,
    /// The thread notes, where the members of an escalation group other than its leader answer.
    notes: &'a dyn NotesStore,
    threads: &'a Threads,
    /// Who of the senders that say so are webhooks.
    webhooks: &'a Webhooks,
    /// The roster, for the members a triage prompt shows.
    roster: &'a Roster,
    /// Who is a human, and who gets the escalations of a thread with no owner.
    humans: &'a Registry,
    retry_delay: Duration,
    progress_every: Duration,
    /// Whether the harness is signed out, holding the messages.
    sign_in: SignIn,
    /// How often a signed-out harness is checked.
    sign_in_every: Duration,
    /// Whether messages wait out a provider limit.
    throttle: Throttle,
    backoffs: Backoffs,
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
        let finishing = self.finish_buried();
        let signing_in = futures::future::Fuse::terminated();
        let lifting = futures::future::Fuse::terminated();
        tokio::pin!(shutdown, finishing, signing_in, lifting);
        loop {
            let retry_deadline = threads.values().filter_map(|t| t.retry.map(|r| r.until)).min();
            let mut ready = None;
            tokio::select! {
                () = &mut shutdown => break,
                () = &mut finishing => {}
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
                () = &mut signing_in => ready = Some(threads.keys().cloned().collect()),
                () = &mut lifting => ready = Some(threads.keys().cloned().collect()),
                Some((thread, handled)) = running.next(), if !running.is_empty() => {
                    match handed_back(&mut threads, &thread, handled) {
                        Some(Wait::SignIn) if signing_in.is_terminated() => signing_in.set(self.signed_in().fuse()),
                        Some(Wait::Limit) if lifting.is_terminated() => lifting.set(self.limit_passed().fuse()),
                        _ => {}
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
            // While the harness is signed out or a provider limit holds, messages wait in hand: the consumer's limit on
            // them stops deliveries.
            let held = self.sign_in.is_out() || self.throttle.is_held();
            for thread in ready.into_iter().flatten().filter(|_| !held) {
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

    /// Answer the humans' cancel requests ([`CancelRequest`]) for as long as the daemon serves, cancelling the prompt
    /// running on the thread each one names.
    async fn hear_cancels(&self) {
        let mut requests = match self.client.subscribe(subject::cancel_of(self.ant)).await {
            Ok(requests) => requests,
            Err(e) => {
                tracing::warn!(error = %e, "serving without ants cancel");
                return std::future::pending().await;
            }
        };
        while let Some(msg) = requests.next().await {
            let request: CancelRequest = match serde_json::from_slice(&msg.payload) {
                Ok(request) => request,
                Err(e) => {
                    tracing::warn!(subject = %msg.subject, error = %e, "ignoring a malformed cancel request");
                    continue;
                }
            };
            if let Err(e) = request.check_subject(&msg.subject) {
                tracing::warn!(error = %e, "ignoring a forged cancel request");
                continue;
            }
            let cancelled = self.cancels.cancel(&request.thread_id, request.by.clone());
            if !cancelled {
                tracing::info!(thread = %request.thread_id, by = %request.by, "no prompt to cancel");
            }
            let answer = request.answer(cancelled);
            let payload = serde_json::to_vec(&answer).expect("a cancel answer serializes");
            if let Err(e) = self.client.publish(answer.subject(), payload.into()).await {
                tracing::warn!(error = %e, "answering a cancel request");
            }
        }
        std::future::pending().await
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
    /// triage. A malformed or forged one is dropped, with no dead letter: every daemon sees it. A post is a webhook's
    /// only from a webhook ([`Webhooks`]) or a human. A top-level post kept names its thread's owner, a human, and its
    /// creator, an ant ([`ThreadOrigin`]); any other names none until [`Inbox::handle`] reads its thread's.
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
        if post.webhook && !self.humans.is_human(&post.from) && !self.webhooks.is_webhook(&post.from).await {
            tracing::warn!(
                channel = %post.channel,
                thread = %post.thread_id,
                sender = %post.from,
                "taking a post marked as a webhook's for an ant's: the sender has no webhook marker"
            );
            post.webhook = false;
        }
        let humans = self.humans.humans();
        if (post.from == *self.ant || post.webhook) && !self.notify_mentioned(msg, &post, channels, &humans).await {
            return None;
        }
        let mut wake = channel::wake(self.ant, channels.get(&post.channel), &post, &humans);
        let unseen = channels.get(&post.channel).is_none() && post.to_triage(&humans);
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
        let origin = if post.starts_thread() { ThreadOrigin::of_root(&post, &humans) } else { ThreadOrigin::default() };
        origin.apply(&mut post);
        let envelope = channel::as_envelope(&post, self.ant);
        Some(Incoming { envelope, post: Some(Box::new(Posted { post, triage, owned })), resume: None })
    }

    /// Notify the human members `post`, in `msg`, @mentions ([`Threads::notify_mentioned`]) before the post is acked,
    /// when the ant sends its notices: its own post, or a webhook's, checked against the webhook's marker, in a channel
    /// the ant is the messenger of. False when that failed: the post is naked, to be tried again.
    async fn notify_mentioned(
        &self,
        msg: &jetstream::Message,
        post: &ChannelPost,
        channels: &mut Channels,
        humans: &Humans,
    ) -> bool {
        let mut mentioned = post.mentions.iter().filter(|m| humans.contains(m)).peekable();
        if mentioned.peek().is_none() {
            return true;
        }
        if channels.get(&post.channel).is_none_or(|entry| mentioned.any(|m| !entry.is_member(m))) {
            // The post may have overtaken the change that made the human a member, or the ant the messenger.
            channels.refresh(&post.channel).await;
        }
        let Some(seq) = stream_seq(msg) else {
            tracing::warn!(channel = %post.channel, "notifying no human of a post without its stream sequence");
            return true;
        };
        match self.threads.notify_mentioned(post, seq, channels.get(&post.channel), humans).await {
            Ok(notified) => {
                if !notified.is_empty() {
                    tracing::info!(channel = %post.channel, thread = %post.thread_id, ?notified, "notified humans");
                }
                true
            }
            Err(e) => {
                tracing::warn!(channel = %post.channel, error = format!("{e:#}"), "notifying the humans mentioned");
                nak(msg, Some(self.retry_delay)).await;
                false
            }
        }
    }

    /// Name the owner and creator of the thread of the channel post in `incoming`, at stream sequence `seq`, as its
    /// thread's posts say ([`Threads::origin`]), not as the post does: its author chose those. Unread, the thread has
    /// neither.
    async fn thread_origin(&self, incoming: &mut Incoming, seq: u64) {
        let Some(posted) = incoming.post.as_mut().filter(|posted| !posted.post.starts_thread()) else { return };
        let origin = match self.threads.origin(&posted.post, seq, &self.humans.humans()).await {
            Ok(origin) => origin,
            Err(e) => {
                tracing::warn!(thread = %posted.post.thread_id, error = format!("{e:#}"), "reading the thread's origin");
                ThreadOrigin::default()
            }
        };
        origin.apply(&mut posted.post);
        incoming.envelope.owner.clone_from(&posted.post.owner);
        incoming.envelope.creator.clone_from(&posted.post.creator);
    }

    /// Check what the inbox message in `incoming`, at stream sequence `seq`, says of its sender and thread
    /// ([`Claims::sender_claims`]).
    async fn sender_claims(&self, incoming: &mut Incoming, seq: u64) {
        if incoming.post.is_some() {
            return;
        }
        let claims = Claims { humans: self.humans, webhooks: self.webhooks, threads: self.threads };
        claims.sender_claims(&mut incoming.envelope, seq).await;
    }
}

impl Inbox<'_> {
    /// Wait, up to [`OWNER_LANDS_WITHIN`], for the owner a human's message in `incoming` names to be recorded for its
    /// thread and the ant when the message came from a machine's outbox: the owners bucket sources the record from the
    /// same outbox, but apart from the hub's stream, and the ant's own messages on or from the thread take their owner
    /// from it.
    async fn owner_landed(&self, incoming: &Incoming, msg: &jetstream::Message) {
        let envelope = &incoming.envelope;
        let sourced = msg.headers.as_ref().is_some_and(|h| h.get(STREAM_SOURCE).is_some());
        if !sourced || incoming.post.is_some() || envelope.owner.is_none() || !self.humans.is_human(&envelope.from) {
            return;
        }
        match self.threads.awaited_owner(&envelope.thread_id, self.ant, OWNER_LANDS_WITHIN).await {
            Ok(Some(_)) => {}
            Ok(None) => tracing::debug!(thread = %envelope.thread_id, "no owner recorded for the thread"),
            Err(e) => {
                tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "reading the thread's owner")
            }
        }
    }
}

/// What stored state checks the claims of an inbox message against, for the ant's daemon and for the human's side
/// that buries what the daemon could not ([`dead::bury_undelivered`]).
pub struct Claims<'a> {
    /// Who is a human.
    pub humans: &'a Registry,
    /// Who of the senders that say so are webhooks.
    pub webhooks: &'a Webhooks,
    /// The channel threads, whose posts say who started each.
    pub threads: &'a Threads,
}

impl Claims<'_> {
    /// Check what inbox message `envelope`, at stream sequence `seq`, says of its sender and thread: its sender wrote
    /// it. `webhook` holds only from a webhook ([`Webhooks`]) or a human. A human's owner and creator hold, and a
    /// webhook's owner, the human its config names. From an ant, a thread a channel post started has the owner and
    /// creator its posts say ([`Threads::origin_of`]); any other has the owner a human's or a webhook's message on it to
    /// the sender, or to an ant that passed the link to it on to the sender, named ([`Threads::owner_for`]), else that
    /// of the thread it was spun off ([`Envelope::origin_thread`]): a channel thread's under the same check as its
    /// creator, another's under the same as its own; else none, whatever owner the ant names.
    pub async fn sender_claims(&self, envelope: &mut Envelope, seq: u64) {
        if envelope.webhook && !self.humans.is_human(&envelope.from) && !self.webhooks.is_webhook(&envelope.from).await
        {
            tracing::warn!(
                thread = %envelope.thread_id,
                sender = %envelope.from,
                "taking a message marked as a webhook's for an ant's: the sender has no webhook marker"
            );
            envelope.webhook = false;
        }
        if self.humans.sender_role(envelope).is_human() {
            return;
        }
        if envelope.webhook {
            envelope.creator = None;
            envelope.origin_thread = None;
            return;
        }
        if envelope.owner.is_none() && envelope.creator.is_none() {
            envelope.origin_thread = None;
            return;
        }
        let humans = self.humans.humans();
        match self.threads.origin_of(&envelope.thread_id, seq, &humans).await {
            Ok(Some(origin)) => {
                envelope.owner = origin.owner;
                envelope.creator = origin.creator;
                envelope.origin_thread = None;
            }
            Ok(None) => self.spun_off(envelope, seq, &humans).await,
            Err(e) => {
                tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "reading the thread's origin");
                if envelope.owner.is_some() {
                    envelope.owner = self.recorded_owner(&envelope.thread_id, &envelope.from).await;
                }
                envelope.creator = None;
                envelope.origin_thread = None;
            }
        }
    }

    /// Check the owner and creator `envelope`, an ant's message at stream sequence `seq` on a thread no channel post
    /// started, names: its owner is the one recorded for its thread and sender, else the one of the thread it links to
    /// ([`Claims::linked`]); its creator that of the channel thread it links to. It keeps the link only when it gives
    /// either.
    async fn spun_off(&self, envelope: &mut Envelope, seq: u64, humans: &Humans) {
        let (named_owner, named_creator) = (envelope.owner.take(), envelope.creator.take());
        let recorded = match named_owner {
            Some(_) => self.recorded_owner(&envelope.thread_id, &envelope.from).await,
            None => None,
        };
        let (linked_owner, linked_creator) = match envelope.origin_thread.take() {
            Some(origin) if (named_owner.is_some() && recorded.is_none()) || named_creator.is_some() => {
                let (owner, creator) = self.linked(&origin, &envelope.from, seq, humans).await;
                let owner = owner.filter(|_| named_owner.is_some() && recorded.is_none());
                let creator = creator.filter(|_| named_creator.is_some());
                if owner.is_some() || creator.is_some() {
                    envelope.origin_thread = Some(origin);
                }
                (owner, creator)
            }
            _ => (None, None),
        };
        envelope.owner = recorded.or(linked_owner);
        envelope.creator = linked_creator;
        if named_owner.is_some() && envelope.owner.is_none() || named_creator.is_some() && envelope.creator.is_none() {
            tracing::debug!(
                thread = %envelope.thread_id,
                sender = %envelope.from,
                "dropping the message's owner or creator: no human or webhook recorded the thread's owner for the \
                 sender, and it links to no thread that gives one"
            );
        }
    }

    /// The owner and creator of `origin`, the thread an ant's message from `sender` at stream sequence `seq` links to,
    /// when `sender` has a link to it: a channel thread's ([`Threads::origin_for`]); else the owner recorded for it
    /// ([`Threads::owner_for`]) and no creator.
    async fn linked(
        &self,
        origin: &ThreadId,
        sender: &UserId,
        seq: u64,
        humans: &Humans,
    ) -> (Option<UserId>, Option<UserId>) {
        let read = async {
            if self.threads.is_channel_thread(origin, seq).await? {
                let origin = self.threads.origin_for(origin, sender, seq, humans).await?;
                return anyhow::Ok(origin.map_or((None, None), |o| (o.owner, o.creator)));
            }
            Ok((self.threads.owner_for(origin, sender).await?.filter(|o| self.humans.is_human(o)), None))
        };
        read.await.unwrap_or_else(|e| {
            tracing::warn!(thread = %origin, error = format!("{e:#}"), "reading the origin thread");
            (None, None)
        })
    }

    /// The owner recorded for `thread` and `sender`, when a human ([`Threads::owner_for`]).
    async fn recorded_owner(&self, thread: &ThreadId, sender: &UserId) -> Option<UserId> {
        match self.threads.owner_for(thread, sender).await {
            Ok(owner) => owner.filter(|o| self.humans.is_human(o)),
            Err(e) => {
                tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's owner");
                None
            }
        }
    }
}

impl Inbox<'_> {
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
        match dead::bury(self.js, self.ant, msg, None, error).await {
            Ok(_) => self.metrics.buried(),
            Err(e) => {
                // Never redelivered, dead letter or not.
                tracing::warn!(error = format!("{e:#}"), "dead-lettering");
                if let Err(e) = msg.ack_with(AckKind::Term).await {
                    tracing::warn!(error = %e, "terminating the message");
                }
            }
        }
    }

    /// Answer one message. A failed attempt is redelivered after a delay, and the last one becomes a dead letter; one
    /// the harness failed for want of signing in, or for a provider limit, is no attempt, and is handed back to wait.
    /// Returns its thread, and how handling ended.
    async fn handle(&self, mut incoming: Incoming, msg: jetstream::Message) -> (ThreadId, Handled) {
        let _working = self.workload.working();
        let arrived = incoming.clone();
        let envelope = &incoming.envelope;
        let thread = envelope.thread_id.clone();
        let (delivered, seq) = match msg.info() {
            Ok(info) => (info.delivered, info.stream_sequence),
            Err(e) => {
                self.bury_unanswerable(&msg, &format!("no delivery info: {e}")).await;
                return (thread, Handled::Done);
            }
        };
        self.thread_origin(&mut incoming, seq).await;
        self.sender_claims(&mut incoming, seq).await;
        self.owner_landed(&incoming, &msg).await;
        let envelope = &incoming.envelope;
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
            return (thread, Handled::Done);
        }
        if let Some(kept) = self.buried.get(envelope).await {
            // Its dead letter went to the machine's outbox while the hub was out of reach; the redelivery finishes it.
            tracing::info!(%thread, "burying a message whose dead letter is in the outbox");
            match self.bury_letter(&incoming, &msg, &kept.dead, true).await {
                Ok(()) => self.attempts.forget(seq).await,
                Err(e) => tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering"),
            }
            return (thread, Handled::Done);
        }
        let attempt = Attempt::of(delivered, MAX_ATTEMPTS);
        let marker = self.attempts.get(seq, envelope).await;
        if attempt == Attempt::Overdue && !matches!(marker, Ok(Some(Marker::Replied(_)))) {
            let error = format!("no outcome after {MAX_ATTEMPTS} attempts: the daemon stopped or the ack wait ran out");
            match self.bury(&incoming, &msg, &error).await {
                Ok(_) => self.attempts.forget(seq).await,
                Err(e) => tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering"),
            }
            return (thread, Handled::Done);
        }
        let result = match marker {
            Ok(marker) => match self.arrived(&mut incoming).await {
                Ok(true) => self.answer(&incoming, &msg, seq, marker).await,
                Ok(false) => self.done(&msg, seq).await,
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        let Err(e) = result else {
            self.sign_in.settled();
            self.throttle.settled();
            return (thread, Handled::Done);
        };
        if let Some(signed_out) = e.downcast_ref::<SignedOut>() {
            self.wait_for_sign_in(&arrived, signed_out).await;
            return (thread, Handled::SignedOut(Box::new((arrived, msg))));
        }
        self.sign_in.settled();
        if let Some(limited) = e.downcast_ref::<Limited>() {
            self.wait_out_limit(&arrived, limited).await;
            return (thread, Handled::Limited(Box::new((arrived, msg))));
        }
        self.throttle.settled();
        tracing::error!(%thread, error = format!("{e:#}"), "handling message");
        status::state(self.client, self.ant, &thread, State::Failed);
        // A failed last attempt, or an overdue one that failed to send its recorded reply: no delivery follows.
        if attempt != Attempt::Early {
            match self.bury(&incoming, &msg, &format!("{e:#}")).await {
                Ok(_) => {
                    self.attempts.forget(seq).await;
                    return (thread, Handled::Done);
                }
                // The overdue delivery tries again.
                Err(bury) => tracing::warn!(error = format!("{bury:#}"), "dead-lettering"),
            }
        }
        nak(&msg, Some(self.retry_delay)).await;
        self.metrics.retried();
        let retry = (delivered < MAX_DELIVER).then(|| Retry {
            seq,
            // Redelivery may lag behind the delay; past this the thread goes on without the message.
            until: tokio::time::Instant::now() + self.retry_delay * 3,
        });
        (thread, retry.map_or(Handled::Done, Handled::Retry))
    }

    /// Hold the messages, `incoming` having found the harness signed out, show that in the roster entry, and tell its
    /// thread's owner, else the default human, how to sign it in, unless told already since the harness last worked.
    /// The notice goes to the human directly: no ant up the chain can sign the harness in.
    async fn wait_for_sign_in(&self, incoming: &Incoming, signed_out: &SignedOut) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::Idle);
        let tell = self.sign_in.out();
        if let Some(since) = self.sign_in.out_since() {
            let sign_in = signed_out.sign_in.clone();
            self.workload.set_signed_out(Some(ants_proto::SignInNeeded { since, sign_in }));
        }
        let Some(since) = tell else {
            tracing::info!(%thread, error = %signed_out, "harness signed out; the message waits");
            return;
        };
        tracing::warn!(%thread, error = %signed_out, "harness signed out; messages wait until it is signed in");
        let machine = roster::machine().await;
        let post = incoming.post.is_some();
        let notice =
            signin::notice(self.ant, &incoming.envelope, post, signed_out, &machine, self.sign_in_every, self.humans);
        let id = format!("sign-in-{}-{since}", self.ant);
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human to sign the harness in");
            self.sign_in.untold();
        }
    }

    /// Check the harness every `sign_in_every` until a handshake with it succeeds, then clear the roster entry's
    /// signed-out state and let the messages go on.
    async fn signed_in(&self) {
        loop {
            tokio::time::sleep(self.sign_in_every).await;
            let checked = harness::handshake(self.pool.launch()).await;
            match checked.readiness {
                harness::Readiness::Ready { agent } => {
                    tracing::info!(%agent, "harness signed in; answering the waiting messages");
                    self.sign_in.back();
                    self.workload.set_signed_out(None);
                    return;
                }
                harness::Readiness::SignedOut { detail } => {
                    tracing::debug!(%detail, "harness still signed out");
                }
                harness::Readiness::Missing => tracing::warn!("harness not installed; messages wait"),
                harness::Readiness::Failed { detail } => {
                    tracing::warn!(%detail, "checking the signed-out harness failed; messages wait");
                }
            }
        }
    }

    /// Hold the messages, `incoming` having met a provider limit, for the backoff, and show that in the roster entry.
    /// A rate limit passes quietly; an exhausted quota is told to the thread's owner, else the default human, once
    /// until a message gets past a limit.
    async fn wait_out_limit(&self, incoming: &Incoming, limited: &Limited) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::Idle);
        let hold = self.throttle.hit(limited, &self.backoffs);
        self.workload.set_limited(Some(hold.shown.clone()));
        let until = hold.shown.until;
        if !hold.tell {
            tracing::info!(%thread, error = %limited, %until, "provider limit; messages wait");
            return;
        }
        tracing::warn!(%thread, error = %limited, %until, "provider quota exhausted; messages wait");
        let machine = roster::machine().await;
        let post = incoming.post.is_some();
        let max = self.backoffs.quota.max;
        let notice =
            ratelimit::notice(self.ant, &incoming.envelope, post, limited, &hold.shown, &machine, max, self.humans);
        let id = format!("quota-{}-{}", self.ant, hold.shown.since.timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human of the exhausted quota");
            self.throttle.untold();
        }
    }

    /// Wait until the backoff of the provider limit passes, then clear the roster entry's limit and let the messages
    /// go on.
    async fn limit_passed(&self) {
        while let Some(until) = self.throttle.until() {
            tokio::time::sleep_until(until).await;
            if self.throttle.lift() {
                break;
            }
        }
        tracing::info!("provider limit waited out; trying the waiting messages again");
        self.workload.set_limited(None);
    }

    /// Store `msg` as a dead letter, escalating it up the ant's chain (in the end to the thread's owner, else the
    /// default human), and terminate it. A channel post names its thread's owner ([`Inbox::admit_post`]), and its
    /// envelope does too. The dead letter of a channel post keeps its payload.
    async fn bury(&self, incoming: &Incoming, msg: &jetstream::Message, error: &str) -> Result<()> {
        let envelope = incoming.post.is_none().then_some(&incoming.envelope);
        let dead = dead::letter_for(self.ant, msg, envelope, error)?;
        self.bury_letter(incoming, msg, &dead, false).await
    }

    /// Bury `msg` as `dead` ([`dead::bury_letter`]), or, `kept` in the machine's outbox on an earlier delivery, as the
    /// letter the hub's stream may hold already ([`dead::bury_kept`]). A dead letter kept in the machine's outbox is
    /// noted in `buried`, so the redelivery buries it again, even in a restarted daemon; on the last delivery, with its
    /// ack subject, for [`Inbox::finish_buried`].
    async fn bury_letter(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        dead: &DeadLetter,
        kept: bool,
    ) -> Result<()> {
        let post = incoming.post.as_ref().map(|posted| &posted.post);
        let escalator = Some(self.escalator);
        let burial = if kept {
            dead::bury_kept(self.js, msg, dead, post, self.humans, escalator).await?
        } else {
            dead::bury_letter(self.js, msg, dead, post, self.humans, escalator).await?
        };
        if !kept {
            self.metrics.buried();
        }
        match burial {
            Burial::Hub(_) => self.buried.forget(&incoming.envelope).await,
            Burial::Outbox => {
                let last = msg.info().is_ok_and(|info| info.delivered >= MAX_DELIVER);
                let kept = dead::Kept {
                    dead: dead.clone(),
                    post: post.cloned(),
                    last_delivery: msg.reply.as_ref().filter(|_| last).map(|reply| reply.to_string()),
                };
                if let Err(e) = self.buried.add(&incoming.envelope, &kept).await {
                    // The redelivery buries it with an error of its own, under the same message id.
                    tracing::warn!(error = format!("{e:#}"), "noting the dead letter");
                }
            }
        }
        Ok(())
    }

    /// Every `retry_delay`, finish the burial of each dead letter kept in the machine's outbox on its message's last
    /// delivery once the hub is in reach ([`dead::finish`]): no redelivery comes to do it. Runs until dropped.
    async fn finish_buried(&self) {
        let mut every = tokio::time::interval(self.retry_delay);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            every.tick().await;
            for (message, kept) in self.buried.undelivered().await {
                let seq = kept.dead.stream_seq;
                match dead::finish(self.js, &kept, self.humans).await {
                    Ok(Some(_)) => {
                        self.attempts.forget(seq).await;
                        self.buried.forget_id(message).await;
                    }
                    Ok(None) => tracing::debug!(stream_seq = seq, "the hub is out of reach; the dead letter waits"),
                    Err(e) => tracing::warn!(stream_seq = seq, error = format!("{e:#}"), "dead-lettering"),
                }
            }
        }
    }

    /// Note the message the ant is about to answer with its escalator: a human's ends the thread's escalation and
    /// gives the thread a fresh allowance, and the escalation leader's answer gives the thread a fresh budget and
    /// allowance once per level ([`Escalator::arrived`]). The leader's reply that does so on a thread the ant got
    /// stuck on in a channel is answered in that channel, with the fresh budget: the thread's turns come from its posts.
    /// Returns whether to answer the message: a member's notice that it sent the ant's escalation higher is the
    /// daemon's alone ([`Escalator::took_climb`]).
    async fn arrived(&self, incoming: &mut Incoming) -> Result<bool> {
        let human = self.humans.sender_role(&incoming.envelope).is_human();
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
            _ => envelope.reply(self.ant.clone(), "", self.humans.sender_role(envelope)).err(),
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
        let _asking = self.approvals.map(|approvals| approvals.begin(envelope));
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
        let history = crate::history::Thread {
            js: self.js.clone(),
            ant: self.ant.clone(),
            thread: thread.clone(),
            channel: incoming.post.as_ref().map(|posted| posted.post.channel.clone()).or(incoming.resume.clone()),
            before: seq,
        };
        let prompt = Prompt {
            text,
            limits: self.limits,
            spend: Some(spend_tx),
            stop: Some(stop),
            progress: Some(progress_tx),
            history: Some(history.history()),
        };
        let (cancellable, mut cancel) = self.cancels.register(thread);
        let (mut cancelled_by, mut cut_by_cancel) = (None, false);
        let prompting = self.metrics.prompting();
        let prompt = self.pool.run(thread, prompt);
        tokio::pin!(prompt);
        let mut progress = self.progress();
        let mut worked = false;
        let reply = loop {
            tokio::select! {
                r = &mut prompt => break r,
                Some(update) = streamed.recv() => {
                    worked = true;
                    status.push(update);
                }
                _ = flush.tick() => status.flush(),
                Some(by) = cancel.recv(), if cancelled_by.is_none() => {
                    tracing::info!(%thread, %by, "cancelling the prompt");
                    if let Some(stop) = stop_tx.take() {
                        let _ = stop.send(format!("Cancelled by {by}"));
                        cut_by_cancel = true;
                    }
                    cancelled_by = Some(by);
                }
                Some(spent) = spend.recv() => {
                    if let Some(cap) = self.record(envelope, &spent).await {
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
            worked = true;
            status.push(update);
        }
        status.flush();
        drop((cancellable, prompting));
        let reply = match reply {
            // What a human cancelled is answered as it stands, never tried again.
            Err(e) if cancelled_by.is_some() => {
                tracing::warn!(%thread, error = format!("{e:#}"), "cancelled prompt failed");
                crate::harness::Reply { text: String::new(), cut: None }
            }
            // Nothing ran: the harness turned the prompt away for want of signing in, so the attempt never started.
            Err(e) if e.is::<SignedOut>() && !cut_off && !worked => {
                self.attempts.forget(seq).await;
                return Err(e);
            }
            // A provider limit spends no attempt. The marker stays when tools may have run, to warn the retry.
            Err(e) if e.is::<Limited>() => {
                if !cut_off && !worked {
                    self.attempts.forget(seq).await;
                }
                return Err(e);
            }
            // Not even a new session holds the prompt: no further attempt can, so the owner decides.
            Err(e) if e.is::<ContextOverflow>() => {
                self.escalate(incoming, &e.to_string(), EscalationReason::ContextWindow, seq, None).await?;
                bus::double_ack(self.client, msg).await?;
                self.attempts.forget(seq).await;
                status::state(self.client, self.ant, thread, State::Failed);
                return Ok(());
            }
            reply => reply?,
        };
        // Spend reported as the prompt ended, such as its tokens.
        while let Ok(spent) = spend.try_recv() {
            if let Some(cap) = self.record(envelope, &spent).await {
                reached.get_or_insert(cap);
            }
        }

        // A human's cancel is no limit: the reply tells of it, and nothing is escalated for it.
        let cut = reply.cut.is_some() && !cut_by_cancel;
        let limit = reply.cut.filter(|_| !cut_by_cancel).or(reached);
        let limit_notice =
            limit.as_ref().map(|l| format!("[ants] {l}{}", if cut { "; the prompt was cancelled." } else { "." }));
        let cancel_notice = cancelled_by.map(|by| format!("[ants] The prompt was cancelled by {by} (ants cancel)."));
        let branch_notice = self.pool.branch_ahead(thread).await.map(|branch| {
            format!("[ants] Branch {} holds {} not in the repository's HEAD.", branch.name, branch.commits_text())
        });
        let notice = [limit_notice, cancel_notice, branch_notice].into_iter().flatten().collect::<Vec<_>>().join("\n");
        let text = [reply.text, notice].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>().join("\n\n");
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
            _ if envelope.webhook => {
                // Nobody reads a webhook's inbox: the answer goes to the thread's owner, else only to the log.
                let Some(owner) = self.humans.human_owner(envelope.owner.as_ref()) else {
                    if let Some(limit) = limit {
                        self.escalate(incoming, &limit, EscalationReason::Cap, seq, None).await?;
                    }
                    tracing::info!(%thread, webhook = %envelope.from, answer = text, "answered a webhook with no owner");
                    return self.done(msg, seq).await;
                };
                let mut reply = envelope.reply(self.ant.clone(), text, Role::Human)?;
                reply.to = owner;
                Reply::Message(reply)
            }
            _ => {
                // A reply on an escalation carries it, so its leader's answer is known as one.
                let mut reply = envelope.reply(self.ant.clone(), text, self.humans.sender_role(envelope))?;
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
            && !self.humans.sender_role(envelope).is_human()
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
                channel::triage_prompt(post, entry, &roster, humans, read, left)
            }
        };
        if posted.owned {
            let _ = write!(text, "\n\n{}", channel::owner_note(&post.channel));
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

    /// Count what the harness spent on the thread of `envelope`, warning a human of each cap nearly reached. Returns
    /// the cap reached, if any.
    async fn record(&self, envelope: &Envelope, spent: &Spend) -> Option<String> {
        let thread = &envelope.thread_id;
        let recorded = self.ledger.record(thread, spent).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing what the thread spent");
            Recorded::default()
        });
        for warning in &recorded.warnings {
            self.warn(envelope, warning).await;
        }
        recorded.reached
    }

    /// Tell a human that spend on the thread of `envelope` passed the warning share of a cap: the thread's owner, else
    /// the default human, of a thread cap, and the default human of a daily cap. On a thread of its own, so that
    /// `ants ask --wait` on the thread still waits for the answer.
    async fn warn(&self, envelope: &Envelope, warning: &Warning) {
        let thread = &envelope.thread_id;
        let owner = if warning.daily() { None } else { self.humans.human_owner(envelope.owner.as_ref()) };
        let to = owner.unwrap_or_else(|| self.humans.default_human().clone());
        tracing::info!(%thread, %to, cap = warning.key, spent = warning.figure, "spend nearing a cap; warning a human");
        let notice = Envelope::new(self.ant.clone(), to, warning.text(self.ant, thread));
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, None).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "warning a human of spend nearing a cap");
        }
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
            let _ = write!(text, "\n\nUnposted answer from {}:\n{answer}", self.ant);
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
/// Note how handling the message in flight on `thread` ended. Returns what it waits for, first on its thread, if it
/// was handed back.
fn handed_back(threads: &mut HashMap<ThreadId, Thread>, thread: &ThreadId, handled: Handled) -> Option<Wait> {
    let t = threads.entry(thread.clone()).or_default();
    t.busy = false;
    t.in_flight = None;
    t.retry = None;
    match handled {
        Handled::Done => None,
        Handled::Retry(retry) => {
            t.retry = Some(retry);
            None
        }
        Handled::SignedOut(held) => {
            t.waiting.push_front(*held);
            Some(Wait::SignIn)
        }
        Handled::Limited(held) => {
            t.waiting.push_front(*held);
            Some(Wait::Limit)
        }
    }
}

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

/// Tell the owner of the thread of each branch in `kept`, else the default human of `humans`, that the thread's
/// worktree was removed and its branch kept for the commits it holds. Failing to is only logged.
async fn tell_kept_branches(
    js: &jetstream::Context,
    threads: &Threads,
    humans: &Registry,
    ant: &UserId,
    kept: Vec<Branch>,
) {
    for branch in kept {
        let thread = &branch.thread;
        let owner = match threads.owner_for(thread, ant).await {
            Ok(owner) => humans.human_owner(owner.as_ref()),
            Err(e) => {
                tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's owner");
                None
            }
        };
        let notice = kept_branch_notice(ant, &branch, owner, humans);
        if let Err(e) = bus::publish_json(js, notice.subject(), &notice, None).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human of the thread's kept branch");
        }
    }
}

/// The message on the thread of `branch` that tells `owner`, else the default human of `humans`, that the thread's
/// worktree was removed and its branch kept.
fn kept_branch_notice(ant: &UserId, branch: &Branch, owner: Option<UserId>, humans: &Registry) -> Envelope {
    let Branch { thread, name, .. } = branch;
    let text = format!(
        "[ants] The session of thread {thread} expired and its git worktree was removed; branch {name} is kept, \
         holding {} not in the repository's HEAD.",
        branch.commits_text()
    );
    let to = owner.clone().unwrap_or_else(|| humans.default_human().clone());
    Envelope::on_thread(thread.clone(), ant.clone(), to, text).with_owner(owner)
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
            let threads =
                Threads::new(js.clone(), dev.clone(), bus::ensure_sessions(&js, js_domain, &dev).await.unwrap());
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
                version: None,
                perms: None,
                signed_out: None,
                limited: None,
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
            webhooks.put(ants_proto::webhook_marker_key(hook), hook.to_string().into()).await.unwrap();
        }

        async fn marked(&self, seq: u64) -> bool {
            self.markers.get(ants_proto::attempt_key(&self.dev, seq)).await.unwrap().is_some()
        }

        /// Serve the inbox until a permit is added to `ctrl_c` (twice to stop mid-prompt).
        async fn serve(&self, pool: &Pool, ctrl_c: &Arc<Semaphore>) {
            Box::pin(self.serve_timed(pool, ctrl_c, PROGRESS_EVERY, ACK_WAIT)).await;
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
                cancels: Cancels::default(),
                attempts: &self.attempts,
                answered: &self.answered,
                buried: &self.buried,
                mcp: self.mcp.as_ref(),
                approvals: None,
                workload: &self.workload,
                metrics: crate::metrics::ant(&self.dev),
                limits: self.limits,
                ledger: &self.ledger,
                escalator: &escalator,
                notes: &notes,
                threads: &self.threads,
                webhooks: &Webhooks::new(self.js.clone()),
                roster: &self.roster,
                humans: &self.humans,
                retry_delay: Duration::from_millis(300),
                progress_every,
                sign_in: SignIn::default(),
                sign_in_every: Duration::from_millis(300),
                throttle: Throttle::default(),
                backoffs: Backoffs {
                    rate_limit: ratelimit::Backoff {
                        first: Duration::from_millis(100),
                        max: Duration::from_millis(300),
                    },
                    quota: ratelimit::Backoff { first: Duration::from_millis(200), max: Duration::from_millis(400) },
                },
            };
            let stop = || {
                let ctrl_c = ctrl_c.clone();
                async move { ctrl_c.acquire().await.unwrap().forget() }
            };
            tokio::select! {
                () = inbox.serve(messages, channels, IDLE, stop) => {}
                () = escalator.deadlines() => {}
                () = inbox.hear_cancels() => {}
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
    async fn a_prompt_too_large_even_for_a_new_session_goes_to_the_owner_with_the_thread_history_tried() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "hello").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hello".to_owned()));
            bus.send(&t1, "overflows").await;
            let notice = next_envelope(&mut replies).await;
            let why = "The prompt does not fit the context window of test's model, even in a new session: ";
            assert!(notice.text.starts_with(why), "{}", notice.text);
            assert!(notice.text.contains("Prompt is too long"), "{}", notice.text);
            assert!(
                notice.text.ends_with("; needs a decision. Last message from human:\noverflows"),
                "{}",
                notice.text
            );
            assert_eq!(notice.thread_id, t1);
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        let history = "[ants] The thread's history, read back from ants, oldest first:\n\nhuman to dev-a:\nhello\n\n\
                       dev-a to human:\ns1: hello\n\n[ants] End of the thread's history.\n\noverflows";
        let prompts = log.lock().unwrap().prompts.clone();
        assert_eq!(prompts.len(), 3, "{prompts:?}");
        assert!(prompts[2].ends_with(history), "{}", prompts[2]);
        assert_eq!(bus.unacked().await, 0);
        assert!(dead::list(&bus.stream).await.unwrap().is_empty(), "no dead letter");
    }

    #[tokio::test]
    async fn messages_wait_unspent_while_the_harness_is_signed_out() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        log.lock().unwrap().signed_out = true;
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
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
            let shown = bus.workload.signed_out().expect("the roster shows the harness signed out");
            assert_eq!(shown.sign_in, None, "the test harness names no sign-in command");
            // Several checks of the harness pass, telling nobody again and spending no attempt.
            let starts = log.lock().unwrap().starts;
            until("checks of the harness", || log.lock().unwrap().starts >= starts + 3).await;
            assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
            assert!(!bus.marked(seq).await, "no attempt is recorded");
            assert_eq!(bus.consumer().await.num_redelivered, 0);
            assert_eq!(log.lock().unwrap().prompted, 0);

            log.lock().unwrap().signed_out = false;
            let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
            answered.push(next_reply(&mut replies).await);
            answered.sort_by_key(|(_, text)| text.clone());
            let expected = [(t1.clone(), "a"), (t1.clone(), "b"), (t2.clone(), "c")].map(|(t, s)| (t, s.to_owned()));
            assert_eq!(answered, expected, "answered unwarned: no attempt started");
            assert_eq!(bus.workload.signed_out(), None, "signed in again: the roster no longer shows it");

            // Signed out again once it worked: a live harness turns the prompt away, and a human is told again.
            log.lock().unwrap().signed_out = true;
            bus.send(&t1, "d").await;
            signed_out(&next_envelope(&mut replies).await);
            assert!(bus.workload.signed_out().is_some_and(|s| s.since > shown.since), "a new signed-out period");
            log.lock().unwrap().signed_out = false;
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "d".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        let mut answered = log.lock().unwrap().answered.clone();
        answered.sort();
        assert_eq!(answered, ["a", "b", "c", "d"]);
        assert!(dead::list(&bus.stream).await.unwrap().is_empty());
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn messages_wait_unspent_through_a_provider_rate_limit() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        log.lock().unwrap().limited = Some("API Error: 429 Too Many Requests, retry after 0.2s".to_owned());
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        let (_, seq) = bus.send(&t1, "a").await;
        bus.send(&t1, "b").await;
        bus.send(&t2, "c").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            until("prompts tried again", || log.lock().unwrap().prompted >= 4).await;
            let shown = bus.workload.limited().expect("the roster shows the limit");
            assert_eq!(shown.kind, LimitKind::RateLimit);
            assert!(shown.until > shown.since);
            assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "a rate limit tells nobody");
            assert!(!bus.marked(seq).await, "no attempt is recorded");
            assert_eq!(bus.consumer().await.num_redelivered, 0);

            log.lock().unwrap().limited = None;
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
        let (launch, log, _) = fake::launch(true);
        log.lock().unwrap().limited = Some("You've hit your usage limit. Upgrade to get more.".to_owned());
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
            until("prompts tried again", || log.lock().unwrap().prompted >= 3).await;
            assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
            assert!(!bus.marked(seq).await, "no attempt is recorded");
            assert_eq!(bus.consumer().await.num_redelivered, 0);

            log.lock().unwrap().limited = None;
            assert_eq!(next_reply(&mut replies).await, (thread.clone(), "a".to_owned()));
            until("the limit cleared", || bus.workload.limited().is_none()).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, ["a"]);
        assert!(dead::list(&bus.stream).await.unwrap().is_empty());
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
    async fn a_reply_names_the_thread_branch_holding_new_commits() {
        let Some(repo) = crate::worktrees::tests::repo("daemon-branch-ahead") else { return };
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let home = repo.parent().unwrap().join("home");
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        let setup = Worktrees::find(&bus.dev, &repo, &home).await.unwrap();
        let worktree = setup.checkout(&t1).await.unwrap();
        crate::worktrees::tests::commit(&worktree, "work of t1");
        let (launch, _, _) = fake::launch(true);
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

    #[test]
    fn a_kept_branch_is_told_to_the_thread_owner_else_the_default_human() {
        let humans = Registry::new("alice".parse().unwrap());
        let (ant, thread): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let branch = Branch { thread: thread.clone(), name: "ants/dev-a/t1".to_owned(), commits: 2 };
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
        let mut answers = bus.client.subscribe(request.reply_subject()).await.unwrap();
        timeout(WAIT, async {
            loop {
                let payload = serde_json::to_vec(&request).unwrap();
                bus.client.publish(request.subject(), payload.into()).await.unwrap();
                if let Ok(Some(msg)) = timeout(Duration::from_millis(200), answers.next()).await {
                    let answer: ants_proto::CancelReply = serde_json::from_slice(&msg.payload).unwrap();
                    assert_eq!(answer.check_subject(&msg.subject), Ok(()));
                    assert_eq!(answer.request_id, request.id);
                    return answer.cancelled;
                }
            }
        })
        .await
        .expect("no answer to the cancel request")
    }

    #[tokio::test]
    async fn a_human_cancels_a_running_prompt_and_the_sender_gets_what_it_wrote() {
        let Some((bus, mut to_human)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
            until("the prompt to run", || log.lock().unwrap().prompted == 1).await;
            assert!(cancel(&bus, &t1).await);
            let reply = next_envelope(&mut to_dev_b).await;
            assert_eq!(reply.text, "partial\n\n[ants] The prompt was cancelled by human (ants cancel).");
            assert_eq!(next_reply(&mut to_dev_b).await, (t1.clone(), "after".to_owned()), "the thread goes on");
            assert!(!cancel(&bus, &t1).await, "nothing left to cancel");
            assert!(timeout(Duration::from_millis(300), to_human.next()).await.is_err(), "no escalation");
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().cancelled, 1);
        assert_eq!(log.lock().unwrap().prompted, 2, "not retried");
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
    async fn spend_nearing_a_cap_warns_the_human_once_on_a_thread_of_its_own() {
        let Some((mut bus, mut to_human)) = Bus::start().await else { return };
        let caps = Caps { thread_cost: Some(1.0), ..Caps::default() };
        bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), caps).warn_at(80));
        let (launch, _, _) = fake::launch(true);
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

        let (launch, log, _) = fake::launch(true);
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
            // Not just the harness's start: stopped before its session is stored, the thread would start afresh.
            until("the prompt to reach the harness", || log.lock().unwrap().prompted == 1).await;
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
        assert_eq!(kept.last_delivery, None, "the extra delivery finishes the burial");
        let kept = kept.dead;
        assert!(kept.error.contains("doomed"), "{}", kept.error);
        assert!(dead::list(&stream).await.unwrap().is_empty(), "not in the hub's stream yet");

        uplinked.switch(true).await;
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
        Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted {
            watcher_first: true,
            past_window: false,
        }))
        .await;
    }

    /// As above, with the daemon restarted past the hub's duplicate window: it finds the letter there and stores no
    /// other, and tells only when the watcher did not.
    #[tokio::test]
    async fn a_dead_letter_kept_on_the_extra_delivery_lands_once_past_the_duplicate_window_after_the_outbox_copy() {
        Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted {
            watcher_first: false,
            past_window: true,
        }))
        .await;
    }

    #[tokio::test]
    async fn a_dead_letter_kept_on_the_extra_delivery_lands_once_past_the_duplicate_window_after_the_watcher_s() {
        Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted {
            watcher_first: true,
            past_window: true,
        }))
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
        let (launch, log, gate) = fake::launch(true);
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
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            uplinked.switch(false).await;
            gate.add_permits(1);
            until_buried(true).await;
            let kept = bus.buried.get(&doomed).await.expect("noted on the machine");
            if back == UplinkBack::Live {
                uplinked.switch(true).await;
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
                uplinked.switch(true).await;
                assert_eq!(outbox_copy().await.unwrap(), dead_seq, "the outbox copy is dropped");
            } else {
                uplinked.switch(true).await;
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
            let log = log.lock().unwrap();
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
        let (launch, log, _) = fake::launch(true);
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
            bus.until_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        // Different threads, so answered in either order.
        let answered = log.lock().unwrap().answered.clone();
        let has = |text: &str| answered.iter().any(|prompt| prompt.contains(text));
        assert!(answered.len() == 2 && has("build red") && has("build green"), "{answered:?}");
        let quiet = Duration::from_millis(200);
        assert!(timeout(quiet, to_human.next()).await.is_err(), "the unowned answer goes nowhere");
        assert!(timeout(quiet, to_hook.next()).await.is_err(), "nothing goes to the webhook's inbox");
    }

    #[tokio::test]
    async fn an_ant_that_marks_its_message_as_a_webhook_s_gets_the_answer_itself() {
        let Some((bus, mut to_human)) = Bus::start().await else { return };
        let (launch, _, _) = fake::launch(true);
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

    #[tokio::test]
    async fn an_ant_names_a_creator_only_as_the_top_level_post_of_the_thread_says() {
        let Some((bus, mut to_human)) = Bus::start().await else { return };
        let (launch, _, _) = fake::launch(true);
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
        let (launch, _, _) = fake::launch(true);
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
        let sent =
            [&on_asked, &on_stuck, &on_unrecorded, &spun_off_asked, &spun_off_unrecorded, &spun_off_stuck, &spent];
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
        let (launch, _, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        bus::ensure_owners(&bus.js).await.unwrap();
        let human = UserId::human();
        let ask = |text: &str| Envelope::new(human.clone(), bus.dev.clone(), text).with_owner(Some(human.clone()));
        let (sourced, direct) = (ask("from the outbox"), ask("unrecorded, straight to the hub"));
        // Sourced, as from a machine's outbox, ahead of the owner's record.
        let outbox =
            jetstream::stream::Config { name: "OUT".into(), subjects: vec!["out.>".into()], ..Default::default() };
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
        let (launch, _, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let [dev_b, dev_c, eve, lead, boss] = ants(["dev-b", "dev-c", "eve", "lead", "boss"]);
        let [mut to_dev_b, mut to_dev_c, mut to_eve] =
            inboxes(&bus, &[dev_b.clone(), dev_c.clone(), eve.clone()]).await;
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
        let (launch, _, _) = fake::launch(true);
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
        let (launch, _, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let human = UserId::human();
        let [dev_b, dev_x, dev_y, dev_z, eve, mallory] = ants(["dev-b", "dev-x", "dev-y", "dev-z", "eve", "mallory"]);
        let [mut to_dev_y, mut to_dev_z, mut to_eve] =
            inboxes(&bus, &[dev_y.clone(), dev_z.clone(), eve.clone()]).await;
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
            version: None,
            perms: None,
            signed_out: None,
            limited: None,
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
    async fn an_ant_s_post_marked_as_a_webhook_s_counts_as_the_ant_s() {
        let Some((bus, _)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
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
        let answered = log.lock().unwrap().answered.clone();
        assert_eq!(answered.len(), 2, "no triage of dev-b's post: {answered:?}");
        assert!(answered.iter().any(|p| p.contains("Post from github (a webhook) mentioning nobody")), "{answered:?}");
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

    #[tokio::test]
    async fn the_ant_s_own_post_notifies_the_human_members_it_mentions_and_wakes_no_messenger() {
        let Some((mut bus, _)) = Bus::start().await else { return };
        bus.humans = Registry::with(UserId::human(), &["alice", "bob"]);
        let (launch, log, _) = fake::launch(true);
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
        let answered = log.lock().unwrap().answered.clone();
        assert_eq!(answered.len(), 1, "the mention of dev-a only: {answered:?}");
        assert!(answered[0].contains("Post from dev-b mentioning dev-a:\n@dev-a @alice check"), "{}", answered[0]);
        assert_eq!(bus.stored(&subject::inbox_of(&alice)).await, 2, "one notice per post, from dev-a's daemon");
        assert_eq!(bus.stored(&subject::inbox(&bob, &bus.dev)).await, 0, "bob is no member");
    }

    #[tokio::test]
    async fn a_mention_notice_that_failed_is_sent_on_the_post_s_redelivery_and_only_once() {
        let Some((mut bus, _)) = Bus::start().await else { return };
        bus.humans = Registry::with(UserId::human(), &["alice"]);
        let (launch, log, _) = fake::launch(true);
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
            let mut consumer: PullConsumer =
                bus.stream.get_consumer(&ants_proto::chan_consumer(&bus.dev)).await.unwrap();
            assert!(consumer.info().await.unwrap().ack_floor.stream_sequence < seq, "not acked before the notice");
            bus::ensure_sessions(&bus.js, None, &bus.dev).await.unwrap();
            bus.until_posts_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert!(log.lock().unwrap().answered.is_empty());
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
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let (alice, github, dev_b) = (
            "alice".parse::<UserId>().unwrap(),
            "github".parse::<UserId>().unwrap(),
            "dev-b".parse::<UserId>().unwrap(),
        );
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
        assert!(log.lock().unwrap().answered.is_empty(), "a mention of a human wakes nobody");
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
}
