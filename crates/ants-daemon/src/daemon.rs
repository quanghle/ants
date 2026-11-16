//! The inbox loop of one ant.
//!
//! Messages of one thread are handled one at a time, in order; different threads run concurrently, each in its own
//! harness from the pool.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ants_proto::{
    Acting, Attachment, AttachmentHandle, AttachmentScope, CancelReply, CancelRequest, ChannelEntry, ChannelId,
    ChannelPost, ConsumerPlace, DeadLetter, Envelope, Error as ProtoError, EscalationReason, Humans, JsDomain,
    MessageMark, QueueRequest, QueuedMessage, Role, State, StatusUpdate, SummaryOf, SummaryRequest, TIMELINE_EVERY,
    ThreadId, ThreadOrigin, UserId, Uuid, WEBHOOK_ALLOW, subject,
};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use futures::future::{FusedFuture as _, FutureExt as _};
use futures::stream::{FuturesUnordered, Stream, StreamExt as _};

use crate::admin::restart::{Restarted, Restarts};
use crate::approvals::NatsApprover;
use crate::attempts::{Answered, Attempts, Marker, Reply};
use crate::behalf::Records;
use crate::breaker::{self, Breaker, StartFailed};
use crate::caps::{Caps, Ledger, Recorded, Warning};
use crate::channel::{self, Channels, Context, Earlier, Threads, Wake};
use crate::dead::{self, Attempt, Burial, Buried};
use crate::escalation::{self, Chain, Escalator, Route, Stuck};
use crate::fallback::{Failover, OnFallback};
use crate::files::Attached;
use crate::harness::{self, Harness, Launch, ModelSettings, Prompt, PromptLimits, SignedOut, Spend};
use crate::humans::Registry;
use crate::identity::Identity;
use crate::mcp::{Colony, Mcp};
use crate::notes::{self, NotesStore};
use crate::overflow::ContextOverflow;
use crate::permission::{Allowlist, PermissionPolicy};
use crate::pool::{Pool, SessionStore};
use crate::publish::{Publish, Publisher};
use crate::ratelimit::{self, Backoffs, Limited, Throttle};
use crate::reading::Reading;
use crate::redact::Redactor;
use crate::signin::{self, SignIn};
use crate::status::{self, Status};
use crate::sync::MutexExt as _;
use crate::webhooks::Webhooks;
use crate::worktrees::{Branch, Worktrees};
use crate::{bus, colony, pause, unserved};

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
/// How often a snapshot for a clone checks whether the ant is paused and its last prompt ended.
const CLONE_WAIT_CHECK: Duration = Duration::from_millis(200);
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
    /// The ant's role instructions, shared in the colony.
    pub whoami: Option<String>,
    pub env: BTreeMap<String, String>,
    pub harness: std::sync::Arc<Harness>,
    pub model_settings: ModelSettings,
    /// Harnesses and models tried in order while the ant's own harness cannot serve.
    pub fallbacks: Vec<crate::fallback::Fallback>,
    /// Tool requests the harness may run without asking (see [`crate::permission`] for the rules). Every other one
    /// goes to the human on `ants.approvals.<ant>`.
    pub allow: Vec<String>,
    /// How long a tool request waits for the human before it is rejected.
    pub approval_timeout: Duration,
    /// Most harness processes alive at once; the least recently used idle one stops to make room.
    pub max_harnesses: usize,
    /// Each thread works in its own git worktree of the workdir's repository, under the home ([`Worktrees`]); unset,
    /// [`crate::worktrees::per_thread`] decides at start.
    pub worktrees: Option<bool>,
    /// Where a thread's new worktree branch starts ([`Worktrees::with_base`]).
    pub worktree_base: crate::worktrees::Base,
    /// A thread's harness stops after this long without a message, and resumes its session on the next one.
    pub idle_timeout: Duration,
    /// Capabilities the ant advertises in the colony.
    pub capabilities: Vec<String>,
    /// Limits on every prompt.
    pub limits: PromptLimits,
    /// Token and cost caps of each thread and of the ant.
    pub caps: Caps,
    /// The share of a cap, in percent, past which a human is warned once; 0 for never.
    pub spend_warn_percent: u8,
    /// When to warn a human of JetStream storage filling up.
    pub storage: crate::storage::Settings,
    /// When memory is low, and the harnesses run at most while it is ([`crate::pressure`]).
    pub memory_pressure: crate::pressure::Settings,
    /// The daily caps the ant shares with the other ants added from its template, if any.
    pub pool: Option<crate::caps::Pool>,
    /// Serve the ant only while holding its colony entry, as the built-in messenger does: wait while another
    /// daemon lists it, and hand over to a daemon that puts it later ([`colony::hold`]).
    pub defer_to_others: bool,
    /// The default human, who gets escalations and notices of threads with no owner.
    pub human: UserId,
    /// The ant's escalation chain, published in the colony, and its limits.
    pub escalation: Chain,
    /// For a manager ant of an `[[ant_template]]`, what adds the ants its `request_ant` tool asks for.
    pub requester: crate::mcp::Requester,
    /// Redact secrets from what the ant publishes ([`crate::redact`]).
    pub redact: bool,
    /// The machine the ant's colony entry and notices name; the host name when None.
    pub machine: Option<String>,
    /// What becomes of a thread's worktree branch holding new commits ([`crate::publish`]).
    pub publish: crate::publish::Publish,
    /// Whether the ant is paused, and since when and by whom; equal to any other, so that pausing or resuming alone
    /// restarts no harness ([`crate::pause::Pause::replace`]).
    pub pause: crate::pause::Pause,
    /// The ant's settings this machine's config sets otherwise than its shared settings, which the ant runs with
    /// ([`crate::shared`]), as its colony entry reports them; equal to any other, so that a change of drift alone
    /// restarts no harness ([`crate::shared::Drift::replace`]).
    pub drift: crate::shared::Drift,
}

/// Serve the ant's inbox until Ctrl-C. Prompts in flight are finished first; a second Ctrl-C stops at once. Until
/// the NATS server can be reached and its JetStream answers the daemon keeps trying ([`bus::connect_retrying`]). A daemon
/// that defers to others ([`Config::defer_to_others`]) and hands the ant over waits for its turn again.
pub async fn run(config: Config) -> Result<()> {
    run_until(config, &Stop::default(), &Restarts::default()).await
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

/// [`run`], also stopping on `stop`'s request, and restarting the ant's harnesses on the admin orders `restarts`
/// passes on.
pub async fn run_until(config: Config, stop: &Stop, restarts: &Restarts) -> Result<()> {
    let _serving = crate::metrics::ant(&config.ant).serving();
    let _served = restarts.serve(&config.ant);
    if let Some(detail) = config.harness.unenforced(&config.allow, !config.escalation.escalate_to.is_empty()) {
        tracing::warn!(ant = %config.ant, fix = config.harness.gated_fix(), "{detail}");
    }
    let config = settle_worktrees(config).await;
    loop {
        match Box::pin(serve(&config, stop, restarts)).await? {
            Served::Stopped => return Ok(()),
            Served::HandedOver => tracing::info!("another daemon serves the ant; waiting for it to stop"),
            Served::Moved => tracing::info!("the ant moved to another bus; waiting for `ants ant unseal`"),
        }
    }
}

/// The config with worktrees turned on or off for good, telling of a default turning them on and warning of threads
/// running at once in one git work tree.
async fn settle_worktrees(config: Config) -> Config {
    let worktrees = crate::worktrees::per_thread(&config.workdir, config.max_harnesses, config.worktrees).await;
    if config.worktrees.is_none() && worktrees {
        tracing::info!(
            ant = %config.ant,
            workdir = %config.workdir.display(),
            max_harnesses = config.max_harnesses,
            "giving each thread its own git worktree, as threads may run at once in a git workdir; \
             worktrees = false shares the workdir"
        );
    } else if crate::worktrees::shares_git_workdir(&config.workdir, config.max_harnesses, config.worktrees) {
        tracing::warn!(
            ant = %config.ant,
            workdir = %config.workdir.display(),
            max_harnesses = config.max_harnesses,
            "threads running at once share one git work tree and may clobber each other's edits; \
             worktrees = true gives each thread its own"
        );
    }
    Config { worktrees: Some(worktrees), ..config }
}

/// How [`serve`] ended.
#[derive(Debug, PartialEq, Eq)]
enum Served {
    Stopped,
    /// Another daemon took over the ant's colony entry.
    HandedOver,
    /// The ant's identity record was marked moved ([`ants_proto::AntIdentity::moved`]).
    Moved,
}

/// For an assistant's credentials, keeps their record for readers of what it does for its human and lets its tools
/// act for the human ([`Mcp::with_assisting`]), opening and sealing the human's sealed messages with the ant's sealing
/// key when it has one ([`crate::sealing::AssistantSealing`]).
async fn assist(mcp: &Mcp, config: &Config, js: &async_nats::jetstream::Context, humans: &Registry) {
    let Some(creds) = &config.creds else { return };
    let human = match crate::behalf::keep_record(js, config.js_domain.as_ref(), &config.ant, creds).await {
        Ok(Some(human)) => human,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(
                error = format!("{e:#}"),
                "keeping the record of an assistant; nothing it does counts for its human"
            );
            return;
        }
    };
    mcp.with_assisting(human);
    mcp.with_human_store(crate::mcp::jetstream_store(js.clone(), config.js_domain.clone(), creds.clone()));
    match assistant_sealing(config, creds, js, humans).await {
        Ok(Some(sealing)) => mcp.with_sealing(std::sync::Arc::new(sealing)),
        Ok(None) => {}
        Err(e) => tracing::warn!(error = format!("{e:#}"), "opening and sealing none of the human's sealed messages"),
    }
}

/// What an assistant opens and seals its human's messages with: none without a sealing key beside its credentials
/// at `creds`.
async fn assistant_sealing(
    config: &Config,
    creds: &std::path::Path,
    js: &async_nats::jetstream::Context,
    humans: &Registry,
) -> Result<Option<crate::sealing::AssistantSealing>> {
    let Some(xkey) = crate::sealing::load_key(creds)? else { return Ok(None) };
    let Some(account) = crate::humans::account(Some(creds)) else { return Ok(None) };
    let directory = crate::devices::Directory::open(js, &account).await?;
    Ok(Some(crate::sealing::AssistantSealing::new(config.ant.clone(), xkey, humans.clone(), directory)))
}

/// Serve the ant's inbox until Ctrl-C or `stop`, or until another daemon takes over its colony entry.
#[allow(clippy::too_many_lines)]
async fn serve(config: &Config, stop: &Stop, restarts: &Restarts) -> Result<Served> {
    let allow = Allowlist::parse(&config.allow)?;
    let Some((client, js)) = connect(config, stop).await? else {
        tracing::info!("stopped before connecting");
        return Ok(Served::Stopped);
    };
    if !unsealed(config, &js, stop).await {
        return Ok(Served::Stopped);
    }
    let metrics = crate::metrics::ant(&config.ant);
    let workload = std::sync::Arc::new(colony::Workload::default());
    workload.set_paused(config.pause.get());
    workload.set_drift(config.drift.get());
    let _connected = metrics.connected(client.clone(), workload.clone());
    let mut entry = colony::entry(config).await;
    let mut claimed = None;
    if config.defer_to_others {
        let Some(revision) = wait_for_turn(&js, &mut entry, stop).await? else {
            return Ok(Served::Stopped);
        };
        claimed = Some(revision);
    }
    let humans = Registry::new(config.human.clone()).verifying_grants(crate::humans::account(config.creds.as_deref()));
    let _watching = humans.watch(js.clone(), crate::humans::RETRY);
    let redactor = std::sync::Arc::new(redactor(config));
    let (approver, policy) = approval_policy(config, &client, &js, &humans, allow, &redactor);
    let outbox = redacting_outbox(crate::mcp::jetstream_outbox(js.clone()), redactor.clone());
    let colony = crate::mcp::jetstream_colony(js.clone());
    let reading = Reading::daemon(crate::creds::is_assistant(config.creds.as_deref()));
    let notes = crate::notes::JetStreamNotes::new(js.clone(), config.ant.clone()).reading(reading);
    let notes = std::sync::Arc::new(RedactingNotes { notes, redactor: redactor.clone() });
    let sessions_kv = bus::ensure_sessions(&js, config.js_domain.as_ref(), &config.ant).await?;
    let ledger = ledger(config, &sessions_kv);
    let escalator = escalator(config, &js, &colony, &humans, &ledger, &redactor);
    let identity = ant_records(config, &js).await;
    let mut mcp = Mcp::start(
        config.ant.clone(),
        &config.home,
        outbox,
        colony.clone(),
        notes.clone(),
        humans.clone(),
        Some(escalator.clone()),
    )
    .await?;
    connect_mcp(&mut mcp, config, &client, &js, &redactor, &humans).await?;
    let launch = launch(config, policy, &mcp);
    let stream = bus::open_stream(&js, &config.ant).await?;
    let threads = Threads::new(js.clone(), config.ant.clone(), sessions_kv.clone()).reading(reading);
    let webhooks = Webhooks::new(js.clone());
    let storage = storage_watcher(config, &js, &sessions_kv);
    let sessions = SessionStore::kv(sessions_kv, config.ant.clone());
    let attempts =
        Attempts::new(bus::ensure_attempts(&js, config.js_domain.as_ref(), &config.ant).await?, config.ant.clone());
    let (answered, buried) = delivery_marks(config).await;
    let pool = harness_pool(config, launch, sessions, &workload).await;
    let publisher = publisher(config, &js, &redactor, &pool).await;
    let (messages, channels) = inbox_messages(&js, &stream, config).await?;
    let registration = enter_colony(&js, entry.clone(), claimed, &workload).await?;
    log_serving(config);

    let inbox = Inbox {
        client: &client,
        js: &js,
        ant: &config.ant,
        reading,
        machine: &entry.machine,
        pool: &pool,
        cancels: Cancels::default(),
        unnarrowed: Unnarrowed::default(),
        restarts,
        attempts: &attempts,
        answered: &answered,
        buried: &buried,
        mcp: Some(&mcp),
        approvals: Some(&approver),
        publisher: publisher.as_ref(),
        redactor: &redactor,
        workload: &workload,
        pause: &config.pause,
        drift: &config.drift,
        metrics,
        limits: config.limits,
        ledger: &ledger,
        escalator: &escalator,
        notes: notes.as_ref(),
        threads: &threads,
        webhooks: &webhooks,
        colony: &colony,
        humans: &humans,
        behalf: Records::new(js.clone(), config.js_domain.clone(), crate::humans::account(config.creds.as_deref())),
        retry_delay: RETRY_DELAY,
        progress_every: PROGRESS_EVERY,
        timeline_every: TIMELINE_EVERY,
        sign_in: SignIn::default(),
        sign_in_every: signin::CHECK_EVERY,
        breaker: Breaker::default(),
        throttle: Throttle::default(),
        backoffs: ratelimit::BACKOFFS,
        failover: Failover::default(),
        fallback_retry: crate::fallback::RETRY_AFTER,
        identity: identity.as_ref(),
        home: &config.home,
    };
    let served = Box::pin(serve_inbox(&inbox, messages, channels, config, stop, &registration, &storage)).await?;
    registration.deregister().await;
    pool.shutdown().await;
    if served == Served::Stopped {
        tracing::info!("stopped");
    }
    Ok(served)
}

/// Wait while the ant's identity record is marked moved, so no daemon starts a moved ant: false when `stop` comes
/// first. A record that cannot be read does not keep the ant from being served.
async fn unsealed(config: &Config, js: &jetstream::Context, stop: &Stop) -> bool {
    let identity = match Identity::new(js, config.js_domain.as_ref(), &config.ant).await {
        Ok(identity) => identity,
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "reading the ant's identity for a move");
            return true;
        }
    };
    tokio::select! {
        () = identity.unsealed(crate::identity::MOVED_EVERY) => true,
        () = stopped(stop) => false,
    }
}

/// Keep the ant's escalation chain past the colony, and return its identity record, created when absent: None when it
/// cannot be read, which the warning says.
async fn ant_records(config: &Config, js: &jetstream::Context) -> Option<Identity> {
    if let Err(e) = crate::chains::publish(js, &config.ant, &config.escalation.escalate_to).await {
        tracing::warn!(error = format!("{e:#}"), "keeping the escalation chain past the colony");
    }
    let identity = Identity::new(js, config.js_domain.as_ref(), &config.ant).await;
    let ensured = match &identity {
        Ok(identity) => identity.ensure().await,
        Err(e) => Err(anyhow::anyhow!("{e:#}")),
    };
    match ensured {
        Ok(Some(_)) => {}
        Ok(None) => tracing::warn!("no identities bucket: `ants init` creates it; the ant's prompts go uncounted"),
        Err(e) => tracing::warn!(error = format!("{e:#}"), "reading the ant's identity; its prompts go uncounted"),
    }
    identity.ok()
}

/// The ant's approver of tool requests, and the permission policy asking it for calls its allowlist does not cover.
fn approval_policy(
    config: &Config,
    client: &async_nats::Client,
    js: &jetstream::Context,
    humans: &Registry,
    allow: Allowlist,
    redactor: &std::sync::Arc<Redactor>,
) -> (NatsApprover, PermissionPolicy) {
    let approver = NatsApprover::new(client.clone(), js.clone(), config.ant.clone(), config.approval_timeout)
        .with_chain(config.escalation.clone())
        .with_humans(humans.clone());
    let policy =
        PermissionPolicy::new(allow, Some(std::sync::Arc::new(approver.clone()))).with_redactor(redactor.clone());
    (approver, policy)
}

/// Tell the server the messages waiting in hand are still being worked on, so it does not redeliver them.
async fn ack_progress(threads: &HashMap<ThreadId, Thread>) {
    for (_, msg) in threads.values().flat_map(|t| &t.waiting) {
        if let Err(e) = msg.ack_with(AckKind::Progress).await {
            tracing::warn!(error = %e, "progress ack");
        }
    }
}

/// Stop waiting for the failed messages not redelivered by their deadline.
fn give_up_retries(threads: &mut HashMap<ThreadId, Thread>) {
    let now = tokio::time::Instant::now();
    for (thread, t) in threads {
        if t.retry.as_ref().is_some_and(|r| r.until <= now) {
            tracing::warn!(%thread, "failed message not redelivered in time; going on with the thread");
            t.retry = None;
        }
    }
}

/// What came of running a prompt.
struct Ran {
    reply: Result<crate::harness::Reply>,
    /// The human's cancel of the prompt, if one came.
    cancelled_by: Option<Cancel>,
    /// Whether the cancel cut the prompt short.
    cut_by_cancel: bool,
    /// Whether a tool call started, so tools may have run.
    worked: bool,
    /// The cap the prompt's spend reached.
    reached: Option<String>,
    /// The spend still to record.
    spend: tokio::sync::mpsc::UnboundedReceiver<Spend>,
    /// The fallback the prompt ran on, `None` for the ant's own harness.
    fallback: Option<usize>,
    /// The tokens the prompt spent so far, as the harness reported them.
    tokens: u64,
}

/// Connect to the bus as the ant's daemon, retrying until it answers: `None` when `stop` comes first.
async fn connect(config: &Config, stop: &Stop) -> Result<Option<(async_nats::Client, jetstream::Context)>> {
    bus::connect_retrying(
        &config.nats_url,
        config.creds.as_deref(),
        config.js_domain.as_ref(),
        &config.ant,
        &format!("ants-daemon-{}", config.ant),
        stopped(stop),
    )
    .await
}

/// Wait until no other daemon serves the ant: the revision of its colony entry claimed, or `None` when `stop` comes
/// first.
async fn wait_for_turn(
    js: &jetstream::Context,
    entry: &mut ants_proto::ColonyEntry,
    stop: &Stop,
) -> Result<Option<u64>> {
    let kv = bus::ensure_colony(js, ants_proto::COLONY_TTL).await?;
    tokio::select! {
        revision = colony::wait_for_turn(&kv, entry, ants_proto::COLONY_HEARTBEAT) => revision.map(Some),
        () = stopped(stop) => {
            tracing::info!("stopped before serving");
            Ok(None)
        }
    }
}

/// The ledger of the ant's spending, against its caps and its pool's.
fn ledger(config: &Config, sessions: &crate::direct_kv::DirectKv) -> std::sync::Arc<Ledger> {
    let ledger = Ledger::kv(sessions.clone(), config.ant.clone(), config.caps)
        .warn_at(config.spend_warn_percent)
        .with_pool(config.pool.clone());
    std::sync::Arc::new(ledger)
}

/// The ant's escalator, sending what it cannot settle up its chain.
fn escalator(
    config: &Config,
    js: &jetstream::Context,
    colony: &Colony,
    humans: &Registry,
    ledger: &std::sync::Arc<Ledger>,
    redactor: &std::sync::Arc<Redactor>,
) -> std::sync::Arc<Escalator> {
    let escalator = Escalator::new(
        config.ant.clone(),
        config.escalation.clone(),
        colony.clone(),
        humans.clone(),
        ledger.clone(),
        redacting_sender(escalation::jetstream_sender(js.clone()), redactor.clone()),
    )
    .with_chains(crate::chains::jetstream_chains(js.clone()))
    .with_notes(redacting_notes_sender(escalation::jetstream_notes_sender(js.clone()), redactor.clone()));
    std::sync::Arc::new(escalator)
}

/// Give the ant's MCP server its live inbox and its ways to the bus, and an assistant's the human it acts for.
async fn connect_mcp(
    mcp: &mut Mcp,
    config: &Config,
    client: &async_nats::Client,
    js: &jetstream::Context,
    redactor: &std::sync::Arc<Redactor>,
    humans: &Registry,
) -> Result<()> {
    mcp.listen(bus::live_inbox(client, &config.ant).await?);
    mcp.with_channels(redacting_posts(crate::mcp::jetstream_channels(js.clone()), redactor.clone()));
    mcp.with_lookup(crate::mcp::jetstream_lookup(js.clone()));
    mcp.with_files(crate::mcp::jetstream_files(js.clone()), config.workdir.clone());
    mcp.with_redactor(redactor.clone());
    // Without credentials the server is open, with no `ants signer` to answer, and the daemon reads what it would.
    mcp.with_reaction_asker(match config.creds {
        Some(_) => crate::mcp::nats_reaction_asker(client.clone()),
        None => crate::mcp::local_reaction_asker(js.clone(), config.js_domain.clone()),
    });
    if let Some(requester) = &config.requester.0 {
        mcp.with_requester(requester.clone());
    }
    mcp.manage_channels(crate::chanadm::nats_admin(client.clone(), js.clone()));
    assist(mcp, config, js, humans).await;
    Ok(())
}

/// How the ant's harnesses start.
fn launch(config: &Config, policy: PermissionPolicy, mcp: &Mcp) -> Launch {
    Launch::new(
        config.harness.clone(),
        config.workdir.clone(),
        config.env.clone().into_iter().collect(),
        policy,
        config.model_settings.clone(),
    )
    .with_instructions(config.instructions.clone())
    .with_mcp(mcp.servers())
}

fn storage_watcher(
    config: &Config,
    js: &jetstream::Context,
    sessions: &crate::direct_kv::DirectKv,
) -> crate::storage::Watcher {
    crate::storage::Watcher {
        js: js.clone(),
        hub_domain: config.js_domain.clone(),
        notes: sessions.clone(),
        ant: config.ant.clone(),
        human: config.human.clone(),
        settings: config.storage,
        server: format!("{} {:?}", config.nats_url, config.js_domain),
    }
}

/// The marks of the messages answered and buried, pruned of those the stream no longer keeps.
async fn delivery_marks(config: &Config) -> (Answered, Buried) {
    let answered = Answered::new(&config.home);
    answered.prune(bus::RETENTION).await;
    let buried = Buried::new(&config.home);
    buried.prune(bus::RETENTION).await;
    (answered, buried)
}

/// The ant's harness pool, isolating threads in worktrees when asked to, or else cleaning up the ones left behind.
async fn harness_pool(
    config: &Config,
    launch: Launch,
    sessions: SessionStore,
    workload: &std::sync::Arc<colony::Workload>,
) -> Pool {
    let fallbacks = config.fallbacks.iter().map(|f| (f.name(), launch.with_fallback(f))).collect();
    let mut pool = Pool::new(launch, sessions, config.max_harnesses, config.idle_timeout)
        .with_fallbacks(fallbacks)
        .reporting_to(workload.clone())
        .with_memory_pressure(config.memory_pressure, crate::pressure::system_probe());
    let isolating = if config.worktrees == Some(true) {
        Worktrees::find(&config.ant, &config.workdir, &config.home).await
    } else {
        None
    };
    if let Some(worktrees) = isolating {
        pool = pool.with_worktrees(worktrees.with_base(config.worktree_base));
    } else if let Some(worktrees) = Worktrees::left_behind(&config.ant, &config.home).await {
        pool = pool.with_worktrees_left_behind(worktrees.with_base(config.worktree_base));
    }
    pool
}

/// What publishes the branches of the ant's threads, when it does ([`crate::publish`]): never without a worktree per
/// thread in `pool`.
async fn publisher(
    config: &Config,
    js: &jetstream::Context,
    redactor: &std::sync::Arc<Redactor>,
    pool: &Pool,
) -> Option<Publisher> {
    if config.publish == Publish::Off {
        return None;
    }
    if pool.worktrees().is_none() {
        tracing::warn!("publish needs each thread in its own git worktree (worktrees = true); publishing nothing");
        return None;
    }
    let publisher = Publisher::new(config.ant.clone()).with_redactor(redactor.clone());
    Some(match crate::direct_kv::DirectKv::new(js, config.js_domain.as_ref(), ants_proto::THREADS_BUCKET).await {
        Ok(titles) => publisher.with_titles(titles),
        Err(e) => {
            tracing::info!(error = format!("{e:#}"), "pull requests take the opening line, not the thread's title");
            publisher
        }
    })
}

/// The messages to the ant, from its inbox and the posts to its channels, and the channels; the inbox alone when the
/// channels fail. While the ant is paused ([`Config::pause`]) the consumers are not pulled from ([`pause::pausable`]).
async fn inbox_messages(
    js: &jetstream::Context,
    stream: &jetstream::stream::Stream<()>,
    config: &Config,
) -> Result<(impl Stream<Item = Result<jetstream::Message, anyhow::Error>> + Unpin, Channels)> {
    let in_hand = i64::try_from(config.max_harnesses.max(1) * IN_HAND_PER_HARNESS)?;
    let channels = match async { Channels::watch(bus::ensure_channels(js).await?).await }.await {
        Ok(channels) => Some(channels),
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "serving the inbox without channels");
            None
        }
    };
    let posts = channels.is_some();
    let paused = config.pause.subscribe();
    let current = match *paused.borrow() {
        Some(_) => None,
        None => Some(open_messages(stream, &config.ant, in_hand, posts).await?),
    };
    let (stream, ant) = (stream.clone(), config.ant.clone());
    let messages = pause::pausable(paused, current, async move || open_messages(&stream, &ant, in_hand, posts).await);
    Ok((Box::pin(messages), channels.unwrap_or_default()))
}

/// The ant's inbox consumer, with its channel consumer if `posts`, as one stream.
async fn open_messages(
    stream: &jetstream::stream::Stream<()>,
    ant: &UserId,
    in_hand: i64,
    posts: bool,
) -> Result<impl Stream<Item = <jetstream::consumer::pull::Stream as Stream>::Item> + Unpin + use<>> {
    let messages = bus::inbox_consumer(stream, ant, ACK_WAIT, MAX_DELIVER, in_hand).await?;
    let posts = if posts {
        match bus::chan_consumer(stream, ant, ACK_WAIT, MAX_DELIVER, in_hand).await {
            Ok(posts) => Some(posts),
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "serving the inbox without channel posts");
                None
            }
        }
    } else {
        None
    };
    Ok(futures::stream::select(messages, futures::stream::iter(posts).flatten()))
}

/// Put the ant on the colony, holding its entry from the revision claimed, if any.
async fn enter_colony(
    js: &jetstream::Context,
    entry: ants_proto::ColonyEntry,
    claimed: Option<u64>,
    workload: &std::sync::Arc<colony::Workload>,
) -> Result<colony::Registration> {
    Ok(match claimed {
        Some(revision) => colony::hold(js, entry, revision, workload.clone()).await?,
        None => colony::register(js, entry, workload.clone()).await?,
    })
}

fn log_serving(config: &Config) {
    tracing::info!(
        ant = %config.ant,
        harness = %config.harness,
        model_settings = ?config.model_settings,
        workdir = %config.workdir.display(),
        max_harnesses = config.max_harnesses,
        worktrees = config.worktrees == Some(true),
        worktree_base = config.worktree_base.as_str(),
        publish = config.publish.as_str(),
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
}

/// Serve the inbox, and the ant's watches beside it, until a stop, the hand-over or the move: which one ended it. A
/// hand-over or a move drains the ant as a first stop does.
async fn serve_inbox<S, E>(
    inbox: &Inbox<'_>,
    messages: S,
    channels: Channels,
    config: &Config,
    stop: &Stop,
    registration: &colony::Registration,
    storage: &crate::storage::Watcher,
) -> Result<Served>
where
    S: Stream<Item = Result<jetstream::Message, E>> + Unpin,
    E: Display,
{
    let mut unserved =
        unserved::Watch::new(inbox.js, config.js_domain.as_ref(), config.ant.clone(), Role::Ant, inbox.colony.clone())
            .await?
            .reading(inbox.reading);
    let (stopping, moving, mut asked) = (std::cell::Cell::new(false), std::cell::Cell::new(false), 0);
    // The first stop is Ctrl-C, a stop request, the hand-over or the move, after which the prompts in flight finish;
    // the second only Ctrl-C or a stop request.
    let stop_or_hand_over = || {
        asked += 1;
        let (first, stopping, moving) = (asked == 1, &stopping, &moving);
        async move {
            let moved = async {
                match inbox.identity {
                    Some(identity) => identity.moved(crate::identity::MOVED_EVERY).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = stopped(stop) => stopping.set(true),
                () = registration.lost(), if first => {
                    tracing::info!("another daemon took over the ant; taking no more messages");
                }
                () = moved, if first => {
                    tracing::info!("the ant was moved to another bus; taking no more messages");
                    moving.set(true);
                }
            }
        }
    };
    tokio::select! {
        () = inbox.serve(messages, channels, config.idle_timeout, stop_or_hand_over) => {}
        () = inbox.escalator.deadlines() => {}
        () = inbox.pool.prune_worktrees_every(PRUNE_WORKTREES_EVERY, async |kept| {
            tell_kept_branches(inbox.js, inbox.threads, inbox.humans, &config.ant, kept).await;
        }) => {}
        () = inbox.hear_cancels() => {}
        () = inbox.hear_restarts() => {}
        () = inbox.hear_clones() => {}
        () = crate::creds::recheck(config.creds.as_deref()) => {}
        () = storage.watch(crate::storage::CHECK_EVERY) => {}
        () = tell_unserved(inbox.js, inbox.humans, &mut unserved) => {}
    }
    Ok(if stopping.get() {
        Served::Stopped
    } else if moving.get() {
        Served::Moved
    } else if registration.is_lost() {
        Served::HandedOver
    } else {
        Served::Stopped
    })
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

/// Ctrl-C, or SIGTERM, which launchd and systemd send to stop a service; on Windows Ctrl-C, Ctrl-Break, or the console
/// closing or the system shutting down.
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
    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_close, ctrl_shutdown};
        let (mut brk, mut close, mut shutdown) = (ctrl_break()?, ctrl_close()?, ctrl_shutdown()?);
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = brk.recv() => Ok(()),
            _ = close.recv() => Ok(()),
            _ = shutdown.recv() => Ok(()),
        }
    }
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
    /// How a human's steer ([`Envelope::steer`]) placed the message on its thread.
    steered: Steered,
}

/// Whether a message steers its thread: one that does goes ahead of the messages waiting there, after those that
/// steered it before.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Steered {
    /// An ordinary message, waiting its turn.
    #[default]
    No,
    /// It cancelled the prompt running on its thread as it arrived.
    Interrupted,
    /// It arrived behind another steer that cancelled the running prompt already.
    Ahead,
}

impl Incoming {
    /// The files attached to the message, each with its handle: a channel post's under the post's channel, as the
    /// envelope made of it carries none ([`channel::as_envelope`]).
    fn attached(&self) -> Vec<(AttachmentHandle, Attachment)> {
        match &self.post {
            Some(posted) => {
                let post = &posted.post;
                post.attachments.iter().map(|a| (post.attachment_handle(a.id), a.clone())).collect()
            }
            None => {
                let envelope = &self.envelope;
                envelope.attachments.iter().map(|a| (envelope.attachment_handle(a.id), a.clone())).collect()
            }
        }
    }
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
    /// The message in flight, as `ants queue` lists it.
    running: Option<QueuedMessage>,
    /// A failed message that is to be redelivered. Until it is back (or the deadline passes) the rest of the thread
    /// waits rather than overtake it.
    retry: Option<Retry>,
    /// The messages told they wait behind others ([`StatusUpdate::Queued`]), to tell when their turn comes.
    queued: HashSet<Uuid>,
}

/// Who withdrew a message before the ant was prompted with it.
struct Withdrawal {
    by: UserId,
    /// The human an assistant withdrew it for.
    on_behalf_of: Option<UserId>,
    /// Whether the assistant recorded it under a key of its own, the human untold yet.
    recorded: bool,
}

struct Retry {
    seq: u64,
    until: tokio::time::Instant,
    /// The message, as `ants queue` lists it.
    queued: Option<QueuedMessage>,
}

/// A dead letter to bury: a new one, or one kept in the machine's outbox on an earlier delivery.
#[derive(Clone, Copy)]
enum Letter {
    New,
    Kept,
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
    /// The harness keeps failing to start: the message, as it arrived, waits in hand for a check to start it.
    Broken(Box<InHand>),
    /// A fallback turned the message away: it goes, as it arrived, to the next one, or waits like the others.
    Failover(Box<InHand>),
}

/// What a message handed back waits for, first on its thread.
enum Wait {
    SignIn,
    Limit,
    Repair,
}

/// The prompt running on each thread, and where to send the human who cancels it.
#[derive(Default)]
struct Cancels(std::sync::Mutex<HashMap<ThreadId, tokio::sync::mpsc::Sender<Cancel>>>);

/// A human's cancel of a running prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Cancel {
    by: UserId,
    why: Why,
}

/// What a prompt is cancelled for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Why {
    /// `ants cancel`.
    Cancel,
    /// A cancel request of an assistant for this human, whose grant names it.
    CancelFor(UserId),
    /// A steer message ([`Envelope::steer`]), which the thread takes next.
    Steer,
    /// An admin's order to restart the ant's harnesses now ([`crate::admin::restart`]).
    Restart,
}

impl Cancel {
    /// The `[ants]` line ending the cancelled prompt's reply.
    fn notice(&self, ant: &UserId) -> String {
        let by = &self.by;
        match &self.why {
            Why::Cancel => format!("[ants] The prompt was cancelled by {by} (ants cancel)."),
            Why::CancelFor(human) => format!("[ants] The prompt was cancelled by {by}, the assistant of {human}."),
            Why::Steer => format!("[ants] {by} interrupted the prompt to steer {ant} (ants ask --steer)."),
            Why::Restart => {
                format!(
                    "[ants] The prompt was cancelled by {by} to restart {ant}'s harnesses (ants ant restart --now)."
                )
            }
        }
    }
}

impl Cancels {
    /// Make the prompt about to run on `thread` cancellable until the guard drops; the cancels arrive on the receiver.
    fn register<'a>(&'a self, thread: &ThreadId) -> (Cancellable<'a>, tokio::sync::mpsc::Receiver<Cancel>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.0.locked().insert(thread.clone(), tx);
        (Cancellable { cancels: self, thread: thread.clone() }, rx)
    }

    /// Cancel the prompt running on `thread`, if one is; one cancelled already stays so.
    fn cancel(&self, thread: &ThreadId, cancel: Cancel) -> bool {
        use tokio::sync::mpsc::error::TrySendError;
        let cancels = self.0.locked();
        cancels.get(thread).is_some_and(|tx| !matches!(tx.try_send(cancel), Err(TrySendError::Closed(_))))
    }
}

/// The senders of the narrowed messages warned of, so a harness running its tools unasked is warned of once per webhook
/// or schedule, or ant passing a narrowing on, rather than on each prompt.
#[derive(Default)]
struct Unnarrowed(std::sync::Mutex<HashSet<UserId>>);

impl Unnarrowed {
    /// The most senders kept; past it, the set starts over.
    const MAX: usize = 1024;

    /// Whether `sender` was not warned of yet, noting it so.
    fn first(&self, sender: &UserId) -> bool {
        let mut warned = self.0.locked();
        if warned.contains(sender) {
            return false;
        }
        if warned.len() >= Self::MAX {
            warned.clear();
        }
        warned.insert(sender.clone())
    }
}

struct Cancellable<'a> {
    cancels: &'a Cancels,
    thread: ThreadId,
}

impl Drop for Cancellable<'_> {
    fn drop(&mut self) {
        self.cancels.0.locked().remove(&self.thread);
    }
}

struct Inbox<'a> {
    client: &'a async_nats::Client,
    js: &'a jetstream::Context,
    ant: &'a UserId,
    /// How the daemon reads through the ant's own readers ([`Reading::daemon`]).
    reading: Reading,
    /// The machine its notices name, as its colony entry does.
    machine: &'a str,
    pool: &'a Pool,
    /// The prompts running, for `ants cancel`.
    cancels: Cancels,
    /// The senders whose narrowing the harness, running its tools unasked, was warned not to apply.
    unnarrowed: Unnarrowed,
    /// Where admin orders to restart the ant's harnesses arrive.
    restarts: &'a Restarts,
    attempts: &'a Attempts,
    answered: &'a Answered,
    /// Messages whose dead letter waits in the machine's outbox, to bury again on their redelivery.
    buried: &'a Buried,
    mcp: Option<&'a Mcp>,
    /// Takes the decisions ants of the ant's chain send on its tool requests.
    approvals: Option<&'a NatsApprover>,
    /// Pushes the branch of a thread holding new commits, with a draft pull request.
    publisher: Option<&'a Publisher>,
    /// Hides secrets in what the harness streams and answers.
    redactor: &'a Redactor,
    /// Counts the messages being handled and the live harnesses, for the colony.
    workload: &'a colony::Workload,
    /// Whether the ant is paused: then no prompt starts, the messages in hand waiting.
    pause: &'a crate::pause::Pause,
    /// The ant's drift, which its colony entry reports as it changes.
    drift: &'a crate::shared::Drift,
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
    /// The colony, for the members a triage prompt shows.
    colony: &'a Colony,
    /// Who is a human, and who gets the escalations of a thread with no owner.
    humans: &'a Registry,
    /// The assistants' records, against which an `on_behalf_of` is checked with the grants of `humans`.
    behalf: Records,
    retry_delay: Duration,
    progress_every: Duration,
    /// How often at most a prompt's tool-call timeline is stored while it runs.
    timeline_every: Duration,
    /// Whether the harness is signed out, holding the messages.
    sign_in: SignIn,
    /// How often a signed-out harness, or one failing to start, is checked.
    sign_in_every: Duration,
    /// Whether the harness keeps failing to start, holding the messages.
    breaker: Breaker,
    /// Whether messages wait out a provider limit.
    throttle: Throttle,
    backoffs: Backoffs,
    /// Which fallbacks answer while the harness is signed out, out of quota or failing to start.
    failover: Failover,
    /// How long a fallback that turned a message away is passed over, unless its provider limit says.
    fallback_retry: Duration,
    /// The ant's identity record, which counts each prompt completed; None when it cannot be read.
    identity: Option<&'a Identity>,
    /// The ant's home, where a clone's copy of its threads' history is ([`crate::history::cloned`]).
    home: &'a std::path::Path,
}

impl Inbox<'_> {
    /// The requests for the ant's queue, those naming their sender, an assistant's, apart: credentials older than
    /// them allow only the others.
    async fn queue_asks(&self) -> impl Stream<Item = async_nats::Message> + Unpin {
        let asking = [subject::queue(self.ant), subject::queues_by_of(self.ant)].map(|s| self.client.subscribe(s));
        match futures::future::try_join_all(asking).await {
            Ok(asks) => futures::stream::select_all(asks).left_stream(),
            Err(e) => {
                tracing::warn!(error = %e, "serving without ants queue");
                futures::stream::pending().right_stream()
            }
        }
    }

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
        let repairing = futures::future::Fuse::terminated();
        tokio::pin!(shutdown, finishing, signing_in, lifting, repairing);
        let mut asks = self.queue_asks().await;
        let mut answering = FuturesUnordered::new();
        let mut paused = self.pause.subscribe();
        let mut drift = self.drift.subscribe();
        loop {
            let retry_deadline = threads.values().filter_map(|t| t.retry.as_ref().map(|r| r.until)).min();
            // Every fallback was turned away lately: the first to be tried again lets the messages on.
            let now = tokio::time::Instant::now();
            let fallback_up =
                self.failover.next_up(now).filter(|_| self.outage().is_some() && self.fallback(now).is_none());
            let mut ready = None;
            tokio::select! {
                () = &mut shutdown => break,
                () = &mut finishing => {}
                () = channels.changed() => {}
                _ = evict.tick() => {
                    self.pool.evict_idle(Instant::now());
                }
                _ = progress.tick() => ack_progress(&threads).await,
                () = sleep_until(retry_deadline), if retry_deadline.is_some() => {
                    give_up_retries(&mut threads);
                    ready = Some(threads.keys().cloned().collect());
                }
                () = &mut signing_in => ready = Some(threads.keys().cloned().collect()),
                () = &mut lifting => ready = Some(threads.keys().cloned().collect()),
                () = &mut repairing => ready = Some(threads.keys().cloned().collect()),
                Ok(()) = paused.changed() => ready = Some(threads.keys().cloned().collect()),
                Ok(()) = drift.changed() => self.workload.set_drift(drift.borrow_and_update().clone()),
                () = sleep_until(fallback_up), if fallback_up.is_some() => ready = Some(threads.keys().cloned().collect()),
                Some(ask) = asks.next() => {
                    if let Some(request) = queue_request(&ask) {
                        answering.push(self.answer_queue(request, in_hand(&threads)));
                    }
                }
                Some(()) = answering.next(), if !answering.is_empty() => {}
                Some((thread, handled)) = running.next(), if !running.is_empty() => {
                    match handed_back(&mut threads, &thread, handled) {
                        Some(Wait::SignIn) if signing_in.is_terminated() => signing_in.set(self.signed_in().fuse()),
                        Some(Wait::Limit) if lifting.is_terminated() => lifting.set(self.limit_passed().fuse()),
                        Some(Wait::Repair) if repairing.is_terminated() => repairing.set(self.repaired().fuse()),
                        _ => {}
                    }
                    ready = Some(vec![thread]);
                }
                next = messages.next() => match next {
                    None => break,
                    Some(Err(e)) => tracing::warn!(error = %e, "inbox stream"),
                    Some(Ok(msg)) => {
                        let Some(thread) = self.take_in(msg, &mut channels, &mut threads).await else { continue };
                        ready = Some(vec![thread]);
                    }
                }
            }
            // While the harness is signed out, a provider limit holds or the harness keeps failing to start, messages
            // wait in hand, unless a fallback answers them: the consumer's limit on them stops deliveries. A rate limit
            // is waited out. While the ant is paused they wait in hand too, and the consumers are not pulled from.
            let now = tokio::time::Instant::now();
            let pause = paused.borrow_and_update().clone();
            let held = pause.is_some()
                || self.throttle.is_held() && !self.throttle.is_quota_held()
                || self.outage().is_some() && self.fallback(now).is_none();
            self.workload.set_paused(pause);
            for thread in ready.into_iter().flatten().filter(|_| !held) {
                if let Some((incoming, msg)) = next_turn(&mut threads, thread.clone()) {
                    if let Some(t) = threads.get_mut(&thread) {
                        self.queue_moved(&thread, t, incoming.envelope.id);
                    }
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

    /// Admit `msg` and queue it on its thread: the thread, or `None` when the message is not to be handled now.
    async fn take_in(
        &self,
        msg: jetstream::Message,
        channels: &mut Channels,
        threads: &mut HashMap<ThreadId, Thread>,
    ) -> Option<ThreadId> {
        let incoming = if is_post(&msg) { self.admit_post(&msg, channels).await } else { self.admit(&msg).await };
        let incoming = incoming?;
        let thread = incoming.envelope.thread_id.clone();
        let t = threads.entry(thread.clone()).or_default();
        if t.holds(stream_seq(&msg)) {
            // The copy in hand gets the ack: the server tracks a message by its stream sequence.
            tracing::debug!(%thread, "ignoring a redelivery of a message in hand");
            return None;
        }
        if t.retry.as_ref().is_some_and(|r| Some(r.seq) == stream_seq(&msg)) {
            t.retry = None;
            t.waiting.push_front((incoming, msg));
        } else if let Some(steered) = self.steer(&incoming, &thread, t) {
            let at = t.waiting.iter().take_while(|(waiting, _)| waiting.steered != Steered::No).count();
            tracing::info!(%thread, by = %incoming.envelope.from, ahead = at, "a human steers the thread");
            t.waiting.insert(at, (Incoming { steered, ..incoming }, msg));
            let behind = t.waiting.iter().map(|(waiting, _)| waiting.envelope.id).enumerate().skip(at + 1);
            for (before, message) in behind.filter(|(_, message)| t.queued.contains(message)) {
                status::publish(self.client, self.ant, &thread, StatusUpdate::Queued { message, ahead: before + 1 });
            }
        } else {
            let (message, ahead) = (incoming.envelope.id, t.ahead());
            t.waiting.push_back((incoming, msg));
            if ahead > 0 {
                t.queued.insert(message);
                status::publish(self.client, self.ant, &thread, StatusUpdate::Queued { message, ahead });
            }
            if self.sign_in.is_out() && self.fallback(tokio::time::Instant::now()).is_none() {
                status::state(self.client, self.ant, &thread, State::SignedOut);
            }
        }
        Some(thread)
    }

    /// How the message in `incoming` steers `thread`, holding `t`, if it does: a human's steer to the ant while a prompt
    /// runs there ([`Envelope::steer`]), which it cancels unless a steer waiting already did.
    fn steer(&self, incoming: &Incoming, thread: &ThreadId, t: &Thread) -> Option<Steered> {
        let envelope = &incoming.envelope;
        if !envelope.steer || incoming.post.is_some() || !self.humans.sender_role(envelope).is_human() || !t.busy {
            return None;
        }
        if t.waiting.front().is_some_and(|(waiting, _)| waiting.steered != Steered::No) {
            return Some(Steered::Ahead);
        }
        let cancel = Cancel { by: envelope.from.clone(), why: Why::Steer };
        self.cancels.cancel(thread, cancel).then_some(Steered::Interrupted)
    }

    /// Tell the messages waiting on `thread` where they stand now that `started` left the queue, and `started` that
    /// its turn came if it was told it waits.
    fn queue_moved(&self, thread: &ThreadId, t: &mut Thread, started: Uuid) {
        if t.queued.remove(&started) {
            status::publish(self.client, self.ant, thread, StatusUpdate::Queued { message: started, ahead: 0 });
        }
        for (before, (incoming, _)) in t.waiting.iter().enumerate() {
            let message = incoming.envelope.id;
            t.queued.insert(message);
            status::publish(self.client, self.ant, thread, StatusUpdate::Queued { message, ahead: before + 1 });
        }
    }

    /// Answer a human's [`QueueRequest`], or an assistant's for its human, with `messages`, those in hand, and where the
    /// inbox consumer stands: the messages it has not delivered yet wait in the stream.
    async fn answer_queue(&self, request: QueueRequest, messages: Vec<QueuedMessage>) {
        let assisted = request.on_behalf_of.as_ref();
        if assisted.is_some() && self.assisted(&request.by, assisted, "queue request").await.is_none() {
            return;
        }
        let place = async |name: String| {
            let stream = self.js.get_stream_no_info(ants_proto::STREAM).await?;
            let info = stream.consumer_info(name).await?;
            anyhow::Ok(ConsumerPlace {
                acked: info.ack_floor.stream_sequence,
                delivered: info.delivered.stream_sequence,
                undelivered: info.num_pending,
                ack_pending: u64::try_from(info.num_ack_pending).unwrap_or(u64::MAX),
            })
        };
        let inbox = place(ants_proto::inbox_consumer(self.ant)).await.inspect_err(|e| {
            tracing::warn!(error = format!("{e:#}"), "reading the inbox consumer for ants queue");
        });
        // Missing while the daemon serves no channel posts.
        let posts = place(ants_proto::chan_consumer(self.ant)).await.inspect_err(|e| {
            tracing::debug!(error = format!("{e:#}"), "reading the channel consumer for ants queue");
        });
        let answer = request.answer(messages, inbox.ok(), posts.ok());
        let payload = serde_json::to_vec(&answer).expect("a queue answer serializes");
        if let Err(e) = self.client.publish(answer.subject(), payload.into()).await {
            tracing::warn!(error = %e, "answering a queue request");
        }
    }

    /// Answer the humans' cancel requests ([`CancelRequest`]) for as long as the daemon serves, cancelling the prompt
    /// running on the thread each one names, and the assistants' for their humans, which may withdraw a message instead.
    async fn hear_cancels(&self) {
        let subscribe =
            [subject::cancel_of(self.ant), subject::cancels_by_of(self.ant)].map(|s| self.client.subscribe(s));
        let mut requests = match futures::future::try_join_all(subscribe).await {
            Ok(requests) => futures::stream::select_all(requests),
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
            let answer = if request.on_behalf_of.is_some() || request.withdraw.is_some() {
                let Some(human) = self.assisted(&request.by, request.on_behalf_of.as_ref(), "cancel request").await
                else {
                    continue;
                };
                match self.cancel_for(&request, &human).await {
                    Ok(answer) => answer,
                    Err(e) => {
                        tracing::warn!(by = %request.by, error = format!("{e:#}"), "withdrawing a message");
                        continue;
                    }
                }
            } else {
                let cancelled =
                    self.cancels.cancel(&request.thread_id, Cancel { by: request.by.clone(), why: Why::Cancel });
                if !cancelled {
                    tracing::info!(thread = %request.thread_id, by = %request.by, "no prompt to cancel");
                }
                request.answer(cancelled)
            };
            let payload = serde_json::to_vec(&answer).expect("a cancel answer serializes");
            if let Err(e) = self.client.publish(answer.subject(), payload.into()).await {
                tracing::warn!(error = %e, "answering a cancel request");
            }
        }
        std::future::pending::<()>().await;
    }

    /// The human `by`, an assistant, makes `what` for, when it claims `on_behalf_of` and the human's grant names it:
    /// a request on a subject naming its sender is an assistant's for its human, or none.
    async fn assisted(&self, by: &UserId, on_behalf_of: Option<&UserId>, what: &str) -> Option<UserId> {
        match self.behalf.acting(self.humans, by, on_behalf_of).await {
            Acting::For(human) => Some(human),
            Acting::Forged(human) => {
                tracing::warn!(%by, %human, "ignoring a {what} for a human whose grant does not name its sender");
                None
            }
            Acting::Own => {
                tracing::warn!(%by, "ignoring a {what} naming its sender but no human it acts for");
                None
            }
        }
    }

    /// Cancel the prompt, or withdraw the message, `request` asks for, for `human` its assistant acts for, and tell the
    /// human when done: an assistant's withdrawal repeated is answered as done again, untold.
    async fn cancel_for(&self, request: &CancelRequest, human: &UserId) -> Result<CancelReply> {
        let (by, thread, ant) = (&request.by, &request.thread_id, self.ant);
        let (answer, text) = match request.withdraw {
            Some(message) => {
                let found = self.attempts.withdraw(&message, by, Some(human)).await?;
                match found {
                    None => {
                        tracing::info!(%thread, %by, %human, %message, "withdrew a message for an assistant's human");
                        let text = format!(
                            "[ants] {by}, as the assistant of {human}, withdrew message {message} from the queue of \
                             {ant} on thread {thread} for {human}."
                        );
                        (request.answer(true), Some(text))
                    }
                    Some(MessageMark::Withdrawn { by: was, on_behalf_of: Some(for_human), .. })
                        if &was == by && &for_human == human =>
                    {
                        (request.answer(true), None)
                    }
                    Some(mark) => (request.found(mark), None),
                }
            }
            None => {
                let cancel = Cancel { by: by.clone(), why: Why::CancelFor(human.clone()) };
                let cancelled = self.cancels.cancel(thread, cancel);
                tracing::info!(%thread, %by, %human, cancelled, "cancel request of an assistant for its human");
                let text = format!(
                    "[ants] {by}, as the assistant of {human}, cancelled the prompt of {ant} on thread {thread} for \
                     {human}."
                );
                (request.answer(cancelled), cancelled.then_some(text))
            }
        };
        if let Some(text) = text {
            let notice = Envelope::new(ant.clone(), human.clone(), text).with_owner(Some(human.clone()));
            let id = format!("on-behalf-{}", request.id);
            if let Err(e) = bus::store(self.js, notice.subject(), &notice, &id).await {
                tracing::warn!(%human, error = format!("{e:#}"), "telling the human of its assistant's act");
            }
        }
        Ok(answer)
    }

    /// Restart the ant's harnesses on each admin order to ([`crate::admin::restart`]) for as long as the daemon serves:
    /// the idle ones stop at once, the working ones once their prompt ends, cancelled first on an order to restart now.
    async fn hear_restarts(&self) {
        let mut listening = self.restarts.listen(self.ant);
        loop {
            let request = listening.next().await;
            let restarting = self.pool.restart();
            let cancel = Cancel { by: request.by.clone(), why: Why::Restart };
            let cancelled = if request.now {
                restarting.working.iter().filter(|thread| self.cancels.cancel(thread, cancel.clone())).count()
            } else {
                0
            };
            let restarted =
                Restarted { stopped: restarting.stopped, after_prompt: restarting.working.len(), cancelled };
            tracing::info!(
                by = %request.by,
                now = request.now,
                stopped = restarted.stopped,
                after_prompt = restarted.after_prompt,
                cancelled,
                "restarting the harnesses"
            );
            let _ = request.done.send(restarted);
        }
    }

    /// Read a snapshot of the ant for each clone of it asked ([`crate::clone`]) for as long as the daemon serves: once
    /// the ant is paused and handles no message, so the snapshot is of one point in time.
    async fn hear_clones(&self) {
        let mut listening = self.restarts.listen_clones(self.ant);
        loop {
            let mut request = listening.next().await;
            let snapshot = async {
                while self.pause.get().is_none() || self.workload.handling() > 0 {
                    tokio::time::sleep(CLONE_WAIT_CHECK).await;
                }
                tracing::info!(new = %request.new, "reading a snapshot of the ant for a clone");
                let source = crate::clone::Source {
                    js: self.js,
                    ant: self.ant,
                    home: self.home,
                    store: self.pool.store(),
                    identity: self.identity,
                    launch: Some(self.pool.launch().clone()),
                    reading: self.reading,
                };
                let root = async |thread: &ThreadId| match self.threads.root(thread, u64::MAX).await {
                    Ok(channel) => channel,
                    Err(e) => {
                        tracing::warn!(%thread, error = format!("{e:#}"), "looking for the channel of a thread");
                        None
                    }
                };
                source.snapshot(&request.new, root).await
            };
            tokio::select! {
                snapshot = snapshot => {
                    let _ = request.done.send(snapshot);
                }
                () = request.done.closed() => {}
            }
        }
    }

    /// The inbox message `msg` to answer, if it is one: a malformed or forged one is buried, a reply to an
    /// `ask_ant` call is handed over, and so is an approval decision, which is acked and never prompted with
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
        self.check_behalf(&mut envelope).await;
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
        Some(Incoming { envelope, post: None, resume: None, steered: Steered::No })
    }

    /// Check the `on_behalf_of` of `envelope`, which arrived on its sender's subject: a forged one is cleared, so the
    /// message counts as the sender's own; for an honoured one the human is told what its assistant did for it, as of
    /// a stand-in's decision. Failing to tell, it only warns.
    async fn check_behalf(&self, envelope: &mut Envelope) {
        let acting = self.behalf.acting(self.humans, &envelope.from, envelope.on_behalf_of.as_ref()).await;
        let notice = match &acting {
            Acting::Own => return,
            Acting::Forged(human) => {
                tracing::warn!(from = %envelope.from, %human, "ignoring a forged on_behalf_of; taking the message as its sender's own");
                envelope.on_behalf_of = None;
                return;
            }
            Acting::For(human) => match envelope.approval.as_deref() {
                Some(decision) => crate::behalf::held_notice(envelope, decision, human),
                None => crate::behalf::sent_notice(envelope, human),
            },
        };
        let id = format!("on-behalf-{}", envelope.id);
        match bus::store(self.js, notice.subject(), &notice, &id).await {
            Ok(_) => tracing::info!(from = %envelope.from, to = %notice.to, "told the human of its assistant's act"),
            Err(e) => tracing::warn!(
                from = %envelope.from,
                to = %notice.to,
                error = format!("{e:#}"),
                "telling the human of its assistant's act"
            ),
        }
    }

    /// The channel post `msg` to answer, if it wakes the ant ([`channel::wake`]). Any other is acked at once: the
    /// ant's own, one in a channel it is not a member of, one that does not @mention it and is not for it to
    /// triage. A malformed or forged one is dropped, with no dead letter: every daemon sees it. A post is a webhook's
    /// only from a webhook ([`Webhooks`]) or a human, and for a human only from its assistant while the grant stands: a
    /// forged `on_behalf_of` is cleared. No daemon tells the human of an honoured one, as every daemon sees it. A top-level post kept names its thread's owner, a human, and its
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
        if let Acting::Forged(human) = self.behalf.acting(self.humans, &post.from, post.on_behalf_of.as_ref()).await {
            tracing::warn!(
                channel = %post.channel,
                sender = %post.from,
                %human,
                "ignoring a forged on_behalf_of; taking the post as its sender's own"
            );
            post.on_behalf_of = None;
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
        let posted = Some(Box::new(Posted { post, triage, owned }));
        Some(Incoming { envelope, post: posted, resume: None, steered: Steered::No })
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
                tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "reading the thread's owner");
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
    /// creator, another's under the same as its own; else, for an assistant's message for its human naming the human,
    /// the human; else none, whatever owner the ant names.
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
                if let Some(named) = envelope.owner.take() {
                    let recorded = self.recorded_owner(&envelope.thread_id, &envelope.from, Some(&named)).await;
                    envelope.owner = recorded.or_else(|| self.assisted_owner(envelope, named));
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
        let recorded = match &named_owner {
            Some(named) => self.recorded_owner(&envelope.thread_id, &envelope.from, Some(named)).await,
            None => None,
        };
        let (linked_owner, linked_creator) = match envelope.origin_thread.take() {
            Some(origin) if (named_owner.is_some() && recorded.is_none()) || named_creator.is_some() => {
                let (owner, creator) = self.linked(&origin, &envelope.from, named_owner.as_ref(), seq, humans).await;
                let owner = owner.filter(|_| named_owner.is_some() && recorded.is_none());
                let creator = creator.filter(|_| named_creator.is_some());
                if owner.is_some() || creator.is_some() {
                    envelope.origin_thread = Some(origin);
                }
                (owner, creator)
            }
            _ => (None, None),
        };
        let assisted = named_owner.clone().and_then(|named| self.assisted_owner(envelope, named));
        envelope.owner = recorded.or(linked_owner).or(assisted);
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
        named: Option<&UserId>,
        seq: u64,
        humans: &Humans,
    ) -> (Option<UserId>, Option<UserId>) {
        let read = async {
            if self.threads.is_channel_thread(origin, seq).await? {
                let origin = self.threads.origin_for(origin, sender, seq, humans).await?;
                return anyhow::Ok(origin.map_or((None, None), |o| (o.owner, o.creator)));
            }
            let owner = self.recorded_owner_of(origin, sender, named).await?;
            Ok((owner, None))
        };
        read.await.unwrap_or_else(|e| {
            tracing::warn!(thread = %origin, error = format!("{e:#}"), "reading the origin thread");
            (None, None)
        })
    }

    /// The owner recorded for `thread` and `sender`, when a human ([`Threads::owner_for`]), or `named`, the owner the
    /// message names, as its assistant recorded it.
    async fn recorded_owner(&self, thread: &ThreadId, sender: &UserId, named: Option<&UserId>) -> Option<UserId> {
        self.recorded_owner_of(thread, sender, named).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's owner");
            None
        })
    }

    async fn recorded_owner_of(
        &self,
        thread: &ThreadId,
        sender: &UserId,
        named: Option<&UserId>,
    ) -> Result<Option<UserId>> {
        let assistant = named.and_then(|human| self.humans.assistant(human));
        let owner = self.threads.owner_for(thread, sender, named.zip(assistant.as_ref())).await?;
        Ok(owner.filter(|o| self.humans.is_human(o)))
    }

    /// `named`, the owner `envelope` names, when the message is its assistant's for it: its sender is the assistant
    /// the human's grant names now, and it claims to act for the human.
    fn assisted_owner(&self, envelope: &Envelope, named: UserId) -> Option<UserId> {
        let assistant = self.humans.assistant(&named);
        (envelope.on_behalf_of.as_ref() == Some(&named) && assistant.as_ref() == Some(&envelope.from)).then_some(named)
    }
}

impl Inbox<'_> {
    /// Whether `envelope` is a reply to an `ask_ant` call, now handed over and acked. It must not wait in its
    /// thread's queue: the prompt that asked is in flight on that thread.
    async fn claim(&self, envelope: &Envelope, msg: &jetstream::Message) -> bool {
        let Some(mcp) = self.mcp else { return false };
        match mcp.claim(envelope).await {
            Ok(false) => false,
            Ok(true) => {
                match bus::double_ack(self.client, msg).await {
                    // Finished, not left waiting to the watcher of its sender ([`unserved`]).
                    Ok(()) => self.attempts.finish(envelope).await,
                    Err(e) => tracing::warn!(error = %e, "acking a reply to ask_ant"),
                }
                true
            }
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "keeping a reply to ask_ant");
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
                    self.attempts.finish(envelope).await;
                    self.answered.forget(envelope).await;
                }
                Err(e) => tracing::warn!(%thread, error = %e, "acking a message whose reply is stored"),
            }
            return (thread, Handled::Done);
        }
        if let Some(kept) = self.buried.get(envelope).await {
            // Its dead letter went to the machine's outbox while the hub was out of reach; the redelivery finishes it.
            tracing::info!(%thread, "burying a message whose dead letter is in the outbox");
            match self.bury_letter(&incoming, &msg, &kept.dead, Letter::Kept).await {
                Ok(()) => {
                    self.attempts.forget(seq).await;
                    self.attempts.finish(envelope).await;
                }
                Err(e) => tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering"),
            }
            return (thread, Handled::Done);
        }
        let attempt = Attempt::of(delivered, MAX_ATTEMPTS);
        // Taken up before anything is done for it, unless a human withdrew it first (`ants withdraw`).
        let result = match self.take_up(envelope).await {
            Ok(Some(withdrawal)) => self.withdrawn(&incoming, &msg, seq, &withdrawal).await,
            Ok(None) => self.take(&mut incoming, &msg, seq, attempt).await,
            Err(e) => Err(e),
        };
        let Err(e) = result else {
            self.sign_in.settled();
            self.throttle.settled();
            self.breaker.settled();
            return (thread, Handled::Done);
        };
        if self.passed_over(&e, &arrived).await {
            return (thread, Handled::Failover(Box::new((arrived, msg))));
        }
        match e.downcast_ref::<StartFailed>() {
            Some(failed) => {
                if let Some(hold) = self.breaker.failed(&thread, &self.redactor.redact(&failed.detail)) {
                    self.attempts.release(&arrived.envelope).await;
                    self.wait_for_repair(&arrived, failed, hold).await;
                    return (thread, Handled::Broken(Box::new((arrived, msg))));
                }
            }
            None => self.breaker.settled(),
        }
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
                    self.attempts.finish(&incoming.envelope).await;
                    return (thread, Handled::Done);
                }
                // The overdue delivery tries again.
                Err(bury) => tracing::warn!(error = format!("{bury:#}"), "dead-lettering"),
            }
        }
        // Withdrawable until its redelivery, unless that only sends the reply it stored again.
        if delivered < MAX_DELIVER
            && !matches!(self.attempts.get(seq, &incoming.envelope).await, Ok(Some(Marker::Replied(_))))
        {
            self.attempts.retrying(&incoming.envelope).await;
        }
        nak(&msg, Some(self.retry_delay)).await;
        self.metrics.retried();
        let retry = (delivered < MAX_DELIVER).then(|| Retry {
            seq,
            // Redelivery may lag behind the delay; past this the thread goes on without the message.
            until: tokio::time::Instant::now() + self.retry_delay * 3,
            queued: queued_message(&arrived, &msg, Place::Retry),
        });
        (thread, retry.map_or(Handled::Done, Handled::Retry))
    }

    /// Handle the message at stream sequence `seq`, taken up: answer it, or bury it when overdue. Marks it finished
    /// once it is acked or buried.
    async fn take(&self, incoming: &mut Incoming, msg: &jetstream::Message, seq: u64, attempt: Attempt) -> Result<()> {
        let marker = self.attempts.get(seq, &incoming.envelope).await?;
        if attempt == Attempt::Overdue && !matches!(marker, Some(Marker::Replied(_))) {
            let error = format!("no outcome after {MAX_ATTEMPTS} attempts: the daemon stopped or the ack wait ran out");
            match self.bury(incoming, msg, &error).await {
                Ok(_) => {
                    self.attempts.forget(seq).await;
                    self.attempts.finish(&incoming.envelope).await;
                }
                Err(e) => {
                    let thread = &incoming.envelope.thread_id;
                    tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering");
                }
            }
            return Ok(());
        }
        if self.arrived(incoming).await? {
            self.answer(incoming, msg, seq, marker).await?;
        } else {
            self.done(msg, seq).await?;
        }
        self.attempts.finish(&incoming.envelope).await;
        Ok(())
    }

    /// Mark `envelope` as started right before handling it ([`Attempts::take_up`]), unless it was withdrawn first: by
    /// a human (`ants withdraw`), by an assistant for its human through a daemon of the ant ([`Self::cancel_for`]), or
    /// by the assistant of the message's human, its sender or the one it was sent for, under a key of the assistant's
    /// own while no daemon answered ([`crate::attempts::record_withdrawal`]), honoured as a cancel request is: while the
    /// human's grant names the assistant.
    async fn take_up(&self, envelope: &Envelope) -> Result<Option<Withdrawal>> {
        if let Some((by, on_behalf_of)) = self.attempts.take_up(envelope).await? {
            return Ok(Some(Withdrawal { by, on_behalf_of, recorded: false }));
        }
        let sender = Some(&envelope.from).filter(|from| self.humans.is_human(from));
        let mut humans: Vec<&UserId> = sender.into_iter().chain(envelope.on_behalf_of.as_ref()).collect();
        humans.dedup();
        for human in humans {
            let Some(assistant) = self.humans.assistant(human) else { continue };
            let Some((claimed, started)) = self.attempts.withdrawal_by(envelope, &assistant).await? else { continue };
            if &claimed != human {
                tracing::warn!(%assistant, %claimed, %human, "ignoring a withdrawal for a human the message is not of");
                continue;
            }
            if self.assisted(&assistant, Some(&claimed), "withdrawal").await.is_none() {
                continue;
            }
            if self.attempts.withdraw_started(envelope, &assistant, &claimed, started).await? {
                return Ok(Some(Withdrawal { by: assistant, on_behalf_of: Some(claimed), recorded: true }));
            }
            tracing::warn!(message = %envelope.id, "the mark of a message changed while honouring its withdrawal");
        }
        Ok(None)
    }

    /// Ack the message at stream sequence `seq`, which `withdrawal.by` withdrew before the ant was prompted with it,
    /// or retried it after a failed attempt, once a notice of it is stored on its thread, so `ants thread show` tells
    /// what became of it. The notice answers the sender, as a reply would, unless no reply can go to it: a webhook's
    /// goes to the thread's owner, and one to an ant out of turns, or to a channel post, to the human who withdrew it.
    /// The human an assistant withdrew a message for by a record of its own is told too, unless the notice goes to it.
    async fn withdrawn(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        withdrawal: &Withdrawal,
    ) -> Result<()> {
        let (by, on_behalf_of) = (&withdrawal.by, withdrawal.on_behalf_of.as_ref());
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        tracing::info!(%thread, %by, ?on_behalf_of, message = %envelope.id, "acking a withdrawn message unprompted");
        let ant = self.ant;
        // A failed attempt leaves its marker, to warn the retry.
        let before = match self.attempts.get(seq, envelope).await {
            Ok(Some(_)) => format!("before {ant} retried it after a failed attempt"),
            Ok(None) | Err(_) => format!("before {ant} was prompted with it"),
        };
        let text = match on_behalf_of {
            None => format!("[ants] Withdrawn by {by} {before} (ants withdraw)."),
            Some(human) => format!("[ants] Withdrawn by {by} for {human} {before}."),
        };
        // The one who asked for the withdrawal: the human an assistant acted for.
        let by = on_behalf_of.unwrap_or(by);
        let to_human = |to: UserId| {
            let mut notice = envelope.follow_up(self.ant.clone(), to, text.clone());
            notice.in_reply_to = Some(envelope.id);
            notice
        };
        let notice = if incoming.post.is_some() {
            to_human(by.clone())
        } else if envelope.webhook {
            to_human(self.humans.human_owner(envelope.owner.as_ref()).unwrap_or_else(|| by.clone()))
        } else {
            match envelope.reply(self.ant.clone(), text.clone(), self.humans.sender_role(envelope)) {
                Ok(reply) => reply.with_escalation(envelope.escalation.as_deref().cloned()),
                Err(_) => to_human(by.clone()),
            }
        };
        bus::store(self.js, notice.subject(), &notice, &reply_id(self.ant, envelope)).await?;
        if let Some(human) = on_behalf_of.filter(|human| withdrawal.recorded && **human != notice.to) {
            let (assistant, message) = (&withdrawal.by, envelope.id);
            let text = format!(
                "[ants] {assistant}, as the assistant of {human}, withdrew message {message} from the queue of {ant} \
                 on thread {thread} for {human}."
            );
            let told = Envelope::new(ant.clone(), human.clone(), text).with_owner(Some(human.clone()));
            bus::store(self.js, told.subject(), &told, &format!("on-behalf-withdrawn-{message}")).await?;
        }
        self.done(msg, seq).await
    }

    /// Why the ant's own harness cannot answer now, if it cannot: messages go to a fallback meanwhile, if one is up.
    fn outage(&self) -> Option<&'static str> {
        if self.sign_in.is_out() {
            Some("signed out")
        } else if self.breaker.is_open() {
            Some("failing to start")
        } else if self.throttle.is_quota_held() {
            Some("out of quota")
        } else {
            None
        }
    }

    /// The fallback messages go to at `now`: the first not passed over, while the ant's own harness cannot answer.
    fn fallback(&self, now: tokio::time::Instant) -> Option<usize> {
        self.outage()?;
        self.failover.pick(self.pool.fallback_count(), now)
    }

    /// How long the fallback `e` came from is passed over, when it turned the prompt away: signed out, limited or
    /// failing to start.
    fn turned_away(&self, e: &anyhow::Error) -> Option<Duration> {
        if let Some(limited) = e.downcast_ref::<Limited>() {
            return Some(limited.retry_after.unwrap_or(self.fallback_retry));
        }
        (e.is::<SignedOut>() || e.is::<StartFailed>()).then_some(self.fallback_retry)
    }

    /// The ant's own harness answers again: no fallback serves.
    fn back_on_own(&self) {
        if self.failover.clear() {
            tracing::info!("the harness answers again; fallbacks stand down");
        }
        self.workload.set_fallback(None);
    }

    /// Hold the messages, `incoming` having found the harness signed out, show that in the colony entry, and tell its
    /// thread's owner, else the default human, how to sign it in, unless told already since the harness last worked.
    /// The notice goes to the human directly: no ant up the chain can sign the harness in.
    async fn wait_for_sign_in(&self, incoming: &Incoming, signed_out: &SignedOut) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::SignedOut);
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
        let fallbacks = self.pool.fallbacks();
        let post = incoming.post.is_some();
        let notice = signin::notice(
            self.ant,
            &incoming.envelope,
            post,
            signed_out,
            self.machine,
            self.sign_in_every,
            &fallbacks,
            self.humans,
        );
        let id = format!("sign-in-{}-{since}", self.ant);
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human to sign the harness in");
            self.sign_in.untold();
        }
    }

    /// Check the harness every `sign_in_every` until a handshake with it succeeds, then clear the colony entry's
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
                    if self.outage().is_none() {
                        self.back_on_own();
                    }
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

    /// Hold the messages, `incoming` having met a provider limit, for the backoff, and show that in the colony entry.
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
        let fallbacks = self.pool.fallbacks();
        let post = incoming.post.is_some();
        let max = self.backoffs.quota.max;
        let notice = ratelimit::notice(
            self.ant,
            &incoming.envelope,
            post,
            limited,
            &hold.shown,
            self.machine,
            max,
            &fallbacks,
            self.humans,
        );
        let id = format!("quota-{}-{}", self.ant, hold.shown.since.timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human of the exhausted quota");
            self.throttle.untold();
        }
    }

    /// Hold the messages, `incoming` having found the harness failing to start once too often ([`Breaker::failed`]),
    /// show that in the colony entry, and tell its thread's owner, else the default human, how to check the harness,
    /// unless told already since a message last got past its start. The notice goes to the human directly, carrying a
    /// level-0 escalation so it is pushed: no ant up the chain can mend the harness.
    async fn wait_for_repair(&self, incoming: &Incoming, failed: &StartFailed, hold: breaker::Hold) {
        let thread = &incoming.envelope.thread_id;
        status::state(self.client, self.ant, thread, State::Idle);
        self.workload.set_failing(Some(hold.shown.clone()));
        if !hold.tell {
            tracing::info!(%thread, error = %failed, "harness failing to start; the message waits");
            return;
        }
        tracing::warn!(%thread, error = %failed, "harness keeps failing to start; messages wait until a check starts it");
        let fallbacks = self.pool.fallbacks();
        let post = incoming.post.is_some();
        let mut notice = breaker::notice(
            self.ant,
            &incoming.envelope,
            post,
            failed,
            self.machine,
            self.sign_in_every,
            &fallbacks,
            self.humans,
        );
        self.redactor.redact_string(&mut notice.text);
        let id = format!("harness-{}-{}", self.ant, hold.shown.since.timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human the harness fails to start");
            self.breaker.untold();
        }
    }

    /// Check the harness every `sign_in_every`, starting it as for a thread ([`harness::probe`]), until it starts or
    /// says it is signed out or limited, then clear the colony entry's failing state and let the messages go on.
    async fn repaired(&self) {
        loop {
            tokio::time::sleep(self.sign_in_every).await;
            match harness::probe(self.pool.launch()).await {
                Ok(()) => tracing::info!("harness started; answering the waiting messages"),
                Err(e) if e.is::<SignedOut>() || e.is::<Limited>() => {
                    tracing::info!(error = %e, "harness started; trying the waiting messages again");
                }
                Err(e) => {
                    let error = format!("{e:#}");
                    tracing::warn!(error, "harness still fails to start; messages wait");
                    self.workload.set_failing(self.breaker.still_failing(&self.redactor.redact(&error)));
                    continue;
                }
            }
            self.breaker.closed();
            self.workload.set_failing(None);
            if self.outage().is_none() {
                self.back_on_own();
            }
            return;
        }
    }

    /// Wait until the backoff of the provider limit passes, then clear the colony entry's limit and let the messages
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
        self.bury_letter(incoming, msg, &dead, Letter::New).await
    }

    /// Bury `msg` as `dead` ([`dead::bury_letter`]), or, [`Letter::Kept`] in the machine's outbox on an earlier delivery,
    /// as the letter the hub's stream may hold already ([`dead::bury_kept`]). A dead letter kept in the machine's outbox is
    /// noted in `buried`, so the redelivery buries it again, even in a restarted daemon; on the last delivery, with its
    /// ack subject, for [`Inbox::finish_buried`].
    async fn bury_letter(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        dead: &DeadLetter,
        letter: Letter,
    ) -> Result<()> {
        let post = incoming.post.as_ref().map(|posted| &posted.post);
        let escalator = Some(self.escalator);
        let burial = match letter {
            Letter::Kept => dead::bury_kept(self.js, msg, dead, post, self.humans, escalator).await?,
            Letter::New => {
                let burial = dead::bury_letter(self.js, msg, dead, post, self.humans, escalator).await?;
                self.metrics.buried();
                burial
            }
        };
        match burial {
            Burial::Hub(_) => self.buried.forget(&incoming.envelope).await,
            Burial::Outbox => {
                let last = msg.info().is_ok_and(|info| info.delivered >= MAX_DELIVER);
                let kept = dead::Kept {
                    dead: dead.clone(),
                    post: post.cloned(),
                    last_delivery: msg.reply.as_ref().filter(|_| last).map(ToString::to_string),
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
    /// The message takes the tool rules its thread is narrowed to ([`Inbox::narrow`]).
    /// Returns whether to answer the message: a member's notice that it sent the ant's escalation higher is the
    /// daemon's alone ([`Escalator::took_climb`]).
    async fn arrived(&self, incoming: &mut Incoming) -> Result<bool> {
        let sender = self.humans.sender_role(&incoming.envelope);
        // Only a human asks for a summary.
        if !sender.is_human() {
            incoming.envelope.summary = None;
        }
        if incoming.post.is_none() && !sender.is_human() && self.escalator.took_climb(&incoming.envelope).await? {
            return Ok(false);
        }
        let renewed = self.escalator.arrived(&mut incoming.envelope, sender).await?;
        self.narrow(incoming, sender).await?;
        // Not a message from ask_ant or send_message: an ask_ant call waits for the answer in the leader's inbox.
        if renewed && incoming.post.is_none() && incoming.envelope.in_reply_to.is_some() {
            let pending = self.escalator.ledger().pending(&incoming.envelope.thread_id).await?;
            incoming.resume = pending.and_then(|p| p.channel);
        }
        Ok(true)
    }

    /// Set the tool rules of the message, and of its post, to those its thread is narrowed to: a human's message lifts
    /// the narrowing ([`Ledger::reset`]) and has none, a thread keeps the first rules a message brought it, and a
    /// webhook's message brings [`WEBHOOK_ALLOW`] when it has none of its own.
    async fn narrow(&self, incoming: &mut Incoming, sender: Role) -> Result<()> {
        let envelope = &mut incoming.envelope;
        let carried = envelope.allow.take();
        if !sender.is_human() {
            let carried = match carried {
                None if envelope.webhook => Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()),
                carried => carried,
            };
            envelope.allow = self.ledger.narrowed(&envelope.thread_id, carried).await?;
        }
        if let Some(posted) = &mut incoming.post {
            posted.post.allow.clone_from(&envelope.allow);
        }
        Ok(())
    }

    /// Ack the message at stream sequence `seq`, which needs no reply.
    async fn done(&self, msg: &jetstream::Message, seq: u64) -> Result<()> {
        bus::double_ack(self.client, msg).await?;
        self.attempts.forget(seq).await;
        Ok(())
    }

    /// Whether a fallback turned the message away with `e`, which then goes to the next one up.
    async fn passed_over(&self, e: &anyhow::Error, arrived: &Incoming) -> bool {
        let (Some(on), Some(retry)) = (e.downcast_ref::<OnFallback>(), self.turned_away(e)) else {
            return false;
        };
        let thread = &arrived.envelope.thread_id;
        tracing::warn!(%thread, error = format!("{e:#}"), "fallback turned the message away; passing it over");
        let now = tokio::time::Instant::now();
        self.failover.down(on.index, now + retry);
        if e.is::<StartFailed>() {
            self.attempts.release(&arrived.envelope).await;
        }
        if let Some(outage) = self.outage()
            && self.fallback(now).is_none()
            && self.failover.exhausted()
        {
            self.tell_fallbacks_passed_over(e, on, arrived, outage, now).await;
        }
        true
    }

    /// Tell the owner of the thread `arrived` came on, else the default human, that every fallback is passed over
    /// while the ant's own harness is `outage`, the last, `on`, having turned the message away with `e`.
    async fn tell_fallbacks_passed_over(
        &self,
        e: &anyhow::Error,
        on: &OnFallback,
        arrived: &Incoming,
        outage: &str,
        now: tokio::time::Instant,
    ) {
        let thread = &arrived.envelope.thread_id;
        tracing::warn!(%thread, "every fallback is turned away; messages wait");
        let why = if e.is::<SignedOut>() {
            "signed out"
        } else if e.is::<StartFailed>() {
            "failing to start"
        } else {
            "limited"
        };
        let detail: Vec<String> = e.chain().map(ToString::to_string).filter(|c| *c != on.to_string()).collect();
        let detail = self.redactor.redact(&detail.join(": ")).into_owned();
        let next = self.failover.next_up(now).map(|at| {
            chrono::Utc::now() + chrono::Duration::from_std(at.saturating_duration_since(now)).unwrap_or_default()
        });
        let own = self.pool.launch().harness_name();
        let notice = crate::fallback::exhausted_notice(
            self.ant,
            &arrived.envelope,
            arrived.post.is_some(),
            (own, outage),
            &self.pool.fallbacks(),
            (&on.name, why, &detail),
            next,
            self.humans,
        );
        let id = format!("fallbacks-{}-{}", self.ant, chrono::Utc::now().timestamp_millis());
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, Some(&id)).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "telling a human every fallback is turned away");
            self.failover.untold();
        }
    }

    /// Record which backend answered: the colony names a fallback, and the reply's notice says why it answered.
    fn served_by(&self, fallback: Option<usize>) -> Option<String> {
        let Some(name) = fallback.and_then(|i| self.pool.fallbacks().into_iter().nth(i)) else {
            self.back_on_own();
            return None;
        };
        let since = self.failover.served();
        self.workload.set_fallback(Some(ants_proto::ServingFallback { name: name.clone(), since }));
        let why = self.outage().unwrap_or("unavailable");
        let own = self.pool.launch().harness_name();
        Some(format!("[ants] Answered by fallback {name}, harness {own} being {why}."))
    }

    /// Answer the message at stream sequence `seq`, given what an earlier attempt at it left. Every attempt after one
    /// that started, whether it failed or was cut off, is told that tools may already have run; one whose reply was
    /// recorded is not prompted again.
    /// The `[ants]` lines for the thread's branch holding new commits, and for publishing it.
    async fn branch_notices(
        &self,
        envelope: &Envelope,
        msg: &jetstream::Message,
        cancelled: bool,
    ) -> [Option<String>; 2] {
        let thread = &envelope.thread_id;
        let publishing = self.publisher.zip(self.pool.worktrees());
        if let Some((publisher, worktrees)) = publishing {
            publisher.note_opening(worktrees, thread, &envelope.text).await;
        }
        let branch = self.pool.branch_ahead(thread).await;
        let branch_notice = branch.as_ref().map(|branch| {
            format!("[ants] Branch {} holds {} not in {}.", branch.name, branch.commits_text(), branch.outside())
        });
        // What a human cancelled is not published.
        let publish_notice = match (publishing, &branch) {
            (Some((publisher, worktrees)), Some(branch)) if !cancelled => {
                let policy = self.pool.launch().policy();
                self.acking(msg, publisher.publish(worktrees, branch, policy)).await
            }
            _ => None,
        };
        [branch_notice, publish_notice]
    }

    /// Add the prompt just completed to the ant's identity record; a failure only warns.
    async fn count_prompt(&self) {
        if let Some(identity) = self.identity
            && let Err(e) = identity.count_prompt().await
        {
            tracing::warn!(error = format!("{e:#}"), "counting the prompt in the ant's identity");
        }
    }

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
            Some(posted) => Some(self.threads.context(&posted.post, seq, Earlier::Unseen, &humans).await?),
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

        let launch = self.pool.launch();
        if envelope.allow.as_deref().is_some_and(|allow| !crate::harness::allows_all(allow))
            && launch.bypasses_permissions()
            && self.unnarrowed.first(&envelope.from)
        {
            tracing::warn!(
                %thread,
                from = %envelope.from,
                harness = launch.harness_name(),
                "the thread's tool rules are narrowed, but the harness runs its tools unasked: the narrowing does not \
                 apply to them (warned once per sender)"
            );
        }
        let _narrowed = launch.policy().narrow(thread, envelope.allow.as_deref());
        let carried = incoming.attached().into_iter().map(|(handle, _)| handle).collect();
        let _turn = self.mcp.map(|mcp| mcp.begin(envelope).carrying(carried, crate::files::stored(msg)));
        let _asking = self.approvals.map(|approvals| approvals.begin(envelope));
        let flush = tokio::time::interval_at(tokio::time::Instant::now() + status::FLUSH_EVERY, status::FLUSH_EVERY);
        let text = self.prompt_for(incoming, read.as_ref(), &humans, cut_off).await;
        let Ran { reply, cancelled_by, cut_by_cancel, worked, mut reached, spend, fallback, tokens } =
            self.run_prompt(incoming, msg, seq, text, flush).await;
        let reply = match reply {
            // What a human cancelled is answered as it stands, never tried again.
            Err(e) if cancelled_by.is_some() => {
                tracing::warn!(%thread, error = format!("{e:#}"), "cancelled prompt failed");
                crate::harness::Reply { text: String::new(), cut: None }
            }
            Err(e) => return self.prompt_failed(e, incoming, msg, seq, cut_off, worked).await,
            Ok(reply) => reply,
        };
        self.count_prompt().await;
        let fallback_notice = self.served_by(fallback);
        let tokens = tokens + self.record_ended(envelope, spend, &mut reached).await;

        // A human's cancel is no limit: the reply tells of it, and nothing is escalated for it.
        let cut = reply.cut.is_some() && !cut_by_cancel;
        let limit = reply.cut.filter(|_| !cut_by_cancel).or(reached);
        let limit_notice =
            limit.as_ref().map(|l| format!("[ants] {l}{}", if cut { "; the prompt was cancelled." } else { "." }));
        let cancelled = cancelled_by.is_some();
        let cancel_notice = cancelled_by.map(|cancel| cancel.notice(self.ant));
        let [branch_notice, publish_notice] = self.branch_notices(envelope, msg, cancelled).await;
        let notice = [fallback_notice, limit_notice, cancel_notice, branch_notice, publish_notice];
        let notice = notice.into_iter().flatten().collect::<Vec<_>>().join("\n");
        let text = [reply.text, notice].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>().join("\n\n");
        let text = self.redactor.redact(&text).into_owned();
        let Some(mut reply) = self.reply_for(incoming, read, text, limit.as_ref(), msg, seq).await? else {
            return Ok(());
        };
        if let Reply::Message(message) = &mut reply {
            message.summary = envelope.summary.clone().map(|s| SummaryRequest { tokens: Some(tokens), ..s });
        }
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

    /// Settle the message at stream sequence `seq` after its prompt failed with `e`, which is returned unless the
    /// failure was escalated. `worked` says whether tools may have run.
    async fn prompt_failed(
        &self,
        e: anyhow::Error,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        cut_off: bool,
        worked: bool,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        // Nothing ran: the harness turned the prompt away for want of signing in, so the attempt never started, and
        // the message can be withdrawn while it waits.
        if e.is::<SignedOut>() && !cut_off && !worked {
            self.attempts.forget(seq).await;
            self.attempts.release(envelope).await;
            return Err(e);
        }
        // Nothing ran: the harness did not start. Its retry, or the message held for the harness to start, carries no
        // cut-off warning.
        if e.is::<StartFailed>() {
            if !cut_off {
                self.attempts.forget(seq).await;
            }
            return Err(e);
        }
        // A provider limit spends no attempt. The marker stays when tools may have run, to warn the retry.
        if e.is::<Limited>() {
            if !cut_off && !worked {
                self.attempts.forget(seq).await;
                self.attempts.release(envelope).await;
            }
            return Err(e);
        }
        // Not even a new session holds the prompt: no further attempt can, so the owner decides.
        if e.is::<ContextOverflow>() {
            self.escalate(incoming, &e.to_string(), EscalationReason::ContextWindow, seq, None).await?;
            bus::double_ack(self.client, msg).await?;
            self.attempts.forget(seq).await;
            status::state(self.client, self.ant, &envelope.thread_id, State::Failed);
            return Ok(());
        }
        Err(e)
    }

    /// The reply carrying `text`, the answer to `incoming`, and the title the turn suggested for the thread, whatever
    /// kind of reply it is: `None` when the answer went up the chain or nowhere instead, and the message is done with.
    async fn reply_for(
        &self,
        incoming: &Incoming,
        read: Option<Context>,
        text: String,
        limit: Option<&String>,
        msg: &jetstream::Message,
        seq: u64,
    ) -> Result<Option<Reply>> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let title = self.mcp.and_then(|mcp| mcp.take_title(thread));
        let reply = match (&incoming.post, read) {
            (Some(posted), Some(read)) => {
                // Other members' replies stored during the prompt spent the thread's turns too.
                let humans = self.humans.humans();
                let later = match self.threads.context(&posted.post, seq, Earlier::Omitted, &humans).await {
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
                        return Ok(None);
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
                        self.escalate(incoming, limit, EscalationReason::Cap, seq, None).await?;
                    }
                    tracing::info!(%thread, webhook = %envelope.from, answer = text, "answered a webhook with no owner");
                    self.done(msg, seq).await?;
                    return Ok(None);
                };
                let mut reply = envelope.reply(self.ant.clone(), text, Role::Human)?;
                reply.to = owner;
                Reply::Message(Box::new(reply))
            }
            _ => {
                // A reply on an escalation carries it, so its leader's answer is known as one.
                let mut reply = envelope.reply(self.ant.clone(), text, self.humans.sender_role(envelope))?;
                match escalation::route(self.ant, envelope).map(|(_, route)| route) {
                    Some(Route::Notes) => {
                        let (store, budget) = (self.notes, envelope.turn_budget);
                        let ant = self.ant.clone();
                        let handle_of = |id| AttachmentHandle {
                            from: ant.clone(),
                            scope: AttachmentScope::Notes(thread.clone()),
                            id,
                        };
                        let (files, left_out) = self.reply_files(thread, handle_of).await;
                        let text = with_left_out(reply.text, left_out);
                        match notes::spend(store, thread, self.ant, &text, budget, title.clone(), files).await? {
                            Some(post) => Reply::Notes(post),
                            None => {
                                let why = "Thread notes out of turns before the answer was posted";
                                let reason = EscalationReason::Notes;
                                self.escalate(incoming, why, reason, seq, Some(&text)).await?;
                                self.done(msg, seq).await?;
                                return Ok(None);
                            }
                        }
                    }
                    route => {
                        if let Some(Route::Stuck(stuck)) = route {
                            reply.to = stuck;
                        }
                        Reply::Message(Box::new(reply.with_escalation(envelope.escalation.as_deref().cloned())))
                    }
                }
            }
        };
        Ok(Some(match reply {
            Reply::Message(message) => {
                let (attachments, left_out) = self.reply_files(thread, |id| message.attachment_handle(id)).await;
                let text = with_left_out(message.text, left_out);
                Reply::Message(Box::new(Envelope { title, text, attachments, ..*message }))
            }
            Reply::Post(post) => {
                let (attachments, left_out) = self.reply_files(thread, |id| post.attachment_handle(id)).await;
                let text = with_left_out(post.text, left_out);
                Reply::Post(ChannelPost { title, text, attachments, ..post })
            }
            // Posted with the title and files already.
            Reply::Notes(post) => Reply::Notes(post),
        }))
    }

    /// Store the files the session of `thread` attached to its final answer under the handles `handle_of` gives,
    /// returning their references, else none and why they are left out.
    async fn reply_files(
        &self,
        thread: &ThreadId,
        handle_of: impl Fn(Uuid) -> AttachmentHandle,
    ) -> (Vec<Attachment>, Option<String>) {
        let Some(mcp) = self.mcp else { return (Vec::new(), None) };
        match mcp.reply_files(thread, handle_of).await {
            Ok(files) => (files, None),
            Err(why) => {
                tracing::warn!(%thread, error = why, "attaching files to the answer");
                (Vec::new(), Some(why))
            }
        }
    }

    /// The prompt for `incoming`, given what was read of a post's thread.
    async fn prompt_for(&self, incoming: &Incoming, read: Option<&Context>, humans: &Humans, cut_off: bool) -> String {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let late = match self.mcp {
            Some(mcp) => mcp.late_on(thread).await,
            None => 0,
        };
        let text = match (&incoming.post, read) {
            (Some(posted), Some(read)) => self.channel_prompt(posted, read, humans).await,
            _ => match (&envelope.summary, escalation::group_note(self.ant, envelope)) {
                (Some(summary), _) => format!("{}\n\n{}", summary_note(summary), envelope.text),
                (None, Some(note)) => format!("{note}\n\n{}", envelope.text),
                (None, None) => envelope.text.clone(),
            },
        };
        let text = match &incoming.resume {
            Some(channel) => {
                let left = envelope.turn_budget.saturating_sub(1);
                format!("{text}\n\n{}", channel::resume_note(channel, thread, left))
            }
            None => text,
        };
        let text = match self.answer_line(envelope, humans).await {
            Some(line) => format!("{line}\n\n{text}"),
            None => text,
        };
        let text = prompt_text(&text, cut_off, late);
        match incoming.steered {
            Steered::Interrupted => format!("{}\n\n{text}", steer_note(&envelope.from)),
            Steered::No | Steered::Ahead => text,
        }
    }

    /// The `[ants]` line telling that `envelope`, a human's message or one an assistant sent for its human, verified
    /// on arrival, answers a question the ant asked on the thread ([`crate::questions::answer_line`]); none for any
    /// other message, which is prompted as an ordinary one.
    async fn answer_line(&self, envelope: &Envelope, humans: &Humans) -> Option<String> {
        let human = envelope.on_behalf_of.is_some() || envelope.sender_role(humans).is_human();
        if envelope.answer.is_none() || !human {
            return None;
        }
        match crate::questions::asked(self.js, self.ant, self.reading, envelope).await {
            Ok(question) => crate::questions::answer_line(envelope, question.as_ref()?),
            Err(e) => {
                let thread = &envelope.thread_id;
                tracing::warn!(%thread, error = format!("{e:#}"), "reading back the question a message answers");
                None
            }
        }
    }

    /// Run the prompt `text` for `incoming`, streaming its status, recording its spend, renewing the message's ack wait
    /// and cancelling it on request or at a cap.
    async fn run_prompt(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        text: String,
        mut flush: tokio::time::Interval,
    ) -> Ran {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let (spend_tx, mut spend) = tokio::sync::mpsc::unbounded_channel();
        let (stop_tx, stop) = tokio::sync::oneshot::channel();
        let mut stop_tx = Some(stop_tx);
        let mut reached = None;
        let (progress_tx, mut streamed) = tokio::sync::mpsc::channel(status::BUFFER);
        let mut status = Status::new(self.client, self.ant, thread, self.redactor);
        let every = self.timeline_every;
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut calls = crate::timeline::Collector::new(self.ant, thread, envelope.id, self.redactor);
        let history = crate::history::Thread {
            js: self.js.clone(),
            ant: self.ant.clone(),
            thread: thread.clone(),
            channel: incoming.post.as_ref().map(|posted| posted.post.channel.clone()).or(incoming.resume.clone()),
            before: seq,
            reading: self.reading,
            home: Some(self.home.to_owned()),
        };
        let prompt = Prompt {
            text,
            limits: self.limits,
            spend: Some(spend_tx),
            stop: Some(stop),
            progress: Some(progress_tx),
            history: Some(history.history()),
            attachments: Attached::new(self.js.clone(), incoming.attached(), crate::files::stored(msg)),
        };
        let (cancellable, mut cancel) = self.cancels.register(thread);
        let (mut cancelled_by, mut cut_by_cancel) = (None, false);
        let prompting = self.metrics.prompting();
        let fallback = self.fallback(tokio::time::Instant::now());
        if let Some(i) = fallback {
            tracing::info!(%thread, fallback = self.pool.fallbacks().get(i).map(String::as_str), "prompting a fallback");
        }
        let prompt = self.pool.run_on(thread, prompt, fallback);
        tokio::pin!(prompt);
        let mut progress = self.progress();
        // Whether a tool call started, so tools may have run; streamed text, such as a harness's error, runs nothing.
        let mut worked = false;
        let mut tokens = 0;
        let reply = loop {
            tokio::select! {
                r = &mut prompt => break r,
                Some(update) = streamed.recv() => {
                    worked |= matches!(update, StatusUpdate::Tool { .. });
                    calls.push(&update, chrono::Utc::now());
                    status.push(update);
                }
                _ = flush.tick() => status.flush_settled(),
                () = stored_every(&calls, &mut tick) => {
                    if let Some(record) = calls.take() {
                        crate::timeline::store(self.js, record);
                    }
                }
                Some(by) = cancel.recv(), if cancelled_by.is_none() => {
                    tracing::info!(%thread, by = %by.by, why = ?by.why, "cancelling the prompt");
                    if let Some(stop) = stop_tx.take() {
                        let _ = stop.send(format!("Cancelled by {}", by.by));
                        cut_by_cancel = true;
                    }
                    cancelled_by = Some(by);
                }
                Some(spent) = spend.recv() => {
                    tokens += spent_tokens(&spent);
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
            worked |= matches!(update, StatusUpdate::Tool { .. });
            calls.push(&update, chrono::Utc::now());
            status.push(update);
        }
        status.flush();
        while let Some(record) = calls.take() {
            crate::timeline::store(self.js, record);
        }
        drop((cancellable, prompting));
        Ran { reply, cancelled_by, cut_by_cancel, worked, reached, spend, fallback, tokens }
    }

    /// The prompt for a channel post, with what was read of its thread: the posts not shown before, and the turns left.
    async fn channel_prompt(&self, posted: &Posted, read: &Context, humans: &Humans) -> String {
        let post = &posted.post;
        let left = post.turns_left(&read.later, humans).saturating_sub(1);
        let mut text = match &posted.triage {
            None => channel::prompt(self.ant, post, read, left),
            Some(entry) => {
                let colony = (self.colony)().await.unwrap_or_else(|e| {
                    tracing::warn!(error = format!("{e:#}"), "reading the colony; members show as offline");
                    Vec::new()
                });
                channel::triage_prompt(post, entry, &colony, humans, read, left)
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

    /// Record the spend reported as a prompt ended, such as its tokens, keeping the first cap it reached in `reached`.
    /// The tokens it reported.
    async fn record_ended(
        &self,
        envelope: &Envelope,
        mut spend: tokio::sync::mpsc::UnboundedReceiver<Spend>,
        reached: &mut Option<String>,
    ) -> u64 {
        let mut tokens = 0;
        while let Ok(spent) = spend.try_recv() {
            tokens += spent_tokens(&spent);
            if let Some(cap) = self.record(envelope, &spent).await {
                reached.get_or_insert(cap);
            }
        }
        tokens
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

    /// Wait for `work`, telling the server meanwhile that `msg` is still being worked on.
    async fn acking<T>(&self, msg: &jetstream::Message, work: impl Future<Output = T>) -> T {
        tokio::pin!(work);
        let mut progress = self.progress();
        loop {
            tokio::select! {
                done = &mut work => return done,
                _ = progress.tick() => {
                    if let Err(e) = msg.ack_with(AckKind::Progress).await {
                        tracing::warn!(error = %e, "progress ack");
                    }
                }
            }
        }
    }
}

impl Thread {
    /// Whether the message with stream sequence `seq` is in flight or waiting. The server redelivers one whose progress
    /// acks were lost, as on a link that went silent for longer than the ack wait.
    fn holds(&self, seq: Option<u64>) -> bool {
        seq.is_some() && (self.in_flight == seq || self.waiting.iter().any(|(_, msg)| stream_seq(msg) == seq))
    }

    /// How many messages one arriving now waits behind: the one in flight or to be redelivered, and those waiting.
    fn ahead(&self) -> usize {
        usize::from(self.busy) + usize::from(self.retry.is_some()) + self.waiting.len()
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
    t.running = None;
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
        Handled::Broken(held) => {
            t.waiting.push_front(*held);
            Some(Wait::Repair)
        }
        Handled::Failover(held) => {
            t.waiting.push_front(*held);
            None
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
        Some((incoming, msg)) => {
            t.busy = true;
            t.in_flight = stream_seq(msg);
            t.running = queued_message(incoming, msg, Place::Running);
        }
        None => {
            entry.remove();
        }
    }
    next
}

/// The messages in hand on every thread, as `ants queue` lists them, by stream sequence.
fn in_hand(threads: &HashMap<ThreadId, Thread>) -> Vec<QueuedMessage> {
    let mut messages: Vec<_> = threads
        .values()
        .flat_map(|t| {
            let before = usize::from(t.busy) + usize::from(t.retry.is_some());
            let waiting = t.waiting.iter().enumerate();
            let waiting = waiting.filter_map(move |(at, (incoming, msg))| {
                queued_message(incoming, msg, Place::Waiting { ahead: before + at })
            });
            let retry = t.retry.as_ref().and_then(|r| r.queued.clone());
            let retry = retry.map(|queued| QueuedMessage { ahead: usize::from(t.busy), ..queued });
            t.running.clone().into_iter().chain(retry).chain(waiting)
        })
        .collect();
    messages.sort_by_key(|m| m.seq);
    messages
}

/// Where a message in hand stands on its thread.
#[derive(Clone, Copy)]
enum Place {
    Running,
    /// Waiting with `ahead` messages of its thread before it.
    Waiting {
        ahead: usize,
    },
    /// Awaiting its redelivery after a failed attempt.
    Retry,
}

fn queued_message(incoming: &Incoming, msg: &jetstream::Message, place: Place) -> Option<QueuedMessage> {
    let envelope = &incoming.envelope;
    let (running, ahead) = match place {
        Place::Running => (true, 0),
        Place::Waiting { ahead } => (false, ahead),
        Place::Retry => (false, 0),
    };
    Some(QueuedMessage {
        id: envelope.id,
        thread_id: envelope.thread_id.clone(),
        from: envelope.from.clone(),
        channel: incoming.post.as_ref().map(|posted| posted.post.channel.clone()),
        sent_at: envelope.sent_at,
        seq: stream_seq(msg)?,
        running,
        ahead,
        redelivery: matches!(place, Place::Retry),
    })
}

/// The queue request in `msg`, unless it is malformed or names another ant than its subject.
fn queue_request(msg: &async_nats::Message) -> Option<QueueRequest> {
    let request: QueueRequest = match serde_json::from_slice(&msg.payload) {
        Ok(request) => request,
        Err(e) => {
            tracing::warn!(subject = %msg.subject, error = %e, "ignoring a malformed queue request");
            return None;
        }
    };
    if let Err(e) = request.check_subject(&msg.subject) {
        tracing::warn!(error = %e, "ignoring a forged queue request");
        return None;
    }
    tracing::info!(by = %request.by, "listing the messages in hand");
    Some(request)
}

/// The tokens `spent` reports.
fn spent_tokens(spent: &Spend) -> u64 {
    match spent {
        Spend::Tokens(n) => *n,
        Spend::Cost { .. } => 0,
    }
}

/// `text`, the answer, followed by an `[ants]` line saying why the files attached to it are left out, if they are.
fn with_left_out(text: String, left_out: Option<String>) -> String {
    match left_out {
        Some(why) => format!("{text}\n\n[ants] The files attached to this answer are left out: {why}."),
        None => text,
    }
}

/// The `[ants]` line that turns a human's message carrying `summary` into a request for a summary of the entries its
/// text carries.
fn summary_note(summary: &SummaryRequest) -> String {
    let what = match &summary.of {
        SummaryOf::Thread(thread) => format!(
            "a TL;DR of thread {thread} from the entries below: what was asked, decided and done, and what waits on whom"
        ),
        SummaryOf::Since(since) => format!(
            "a digest of the threads with entries since {} from the entries below: what finished, what is stuck, and \
             what waits on the human",
            since.format("%Y-%m-%d %H:%M UTC")
        ),
    };
    format!(
        "[ants] A human asks for {what}. Answer with at most 120 words of plain text; use no tools and message no \
         one: the reply is the summary."
    )
}

/// The prompt for a message: warned when an earlier attempt at it was cut off, and noting the `late` replies to
/// `ask_ant` that wait on its thread for `check_inbox`.
/// The line starting the prompt of a human's steer message that interrupted the prompt running on its thread.
fn steer_note(by: &UserId) -> String {
    format!(
        "[ants] {by} interrupted the previous prompt on this thread with this message; tool calls it had in flight \
         were cancelled, so check what they left before repeating any."
    )
}

fn prompt_text(text: &str, cut_off: bool, late: usize) -> String {
    let late = match late {
        0 => None,
        1 => Some("[ants] 1 late reply to ask_ant is waiting on this thread; call check_inbox to read it.".to_owned()),
        n => Some(format!(
            "[ants] {n} late replies to ask_ant are waiting on this thread; call check_inbox to read them."
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
        let owner = match threads.owner_for(thread, ant, None).await {
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

/// Tell a human of the messages the ant sent that keep waiting for an ant no daemon serves ([`unserved`]): the owner
/// they name, else the default human, on a thread of its own.
async fn tell_unserved(js: &jetstream::Context, humans: &Registry, watch: &mut unserved::Watch) {
    let ant = watch.sender().clone();
    loop {
        for found in watch.next(|user| humans.is_human(user)).await {
            let Some(found) = watch.claim(found).await else { continue };
            let to = humans.owner_or_default(found.owner.as_ref());
            let notice = Envelope::new(ant.clone(), to, found.text(&ant, chrono::Utc::now()));
            if let Err(e) = bus::publish_json(js, notice.subject(), &notice, None).await {
                tracing::warn!(waiting_for = %found.ant, error = format!("{e:#}"), "telling a human of messages waiting");
            }
        }
    }
}

/// The message on the thread of `branch` that tells `owner`, else the default human of `humans`, that the thread's
/// worktree was removed and its branch kept.
fn kept_branch_notice(ant: &UserId, branch: &Branch, owner: Option<UserId>, humans: &Registry) -> Envelope {
    let Branch { thread, name, .. } = branch;
    let text = format!(
        "[ants] The session of thread {thread} expired and its git worktree was removed; branch {name} is kept, \
         holding {} not in {}.",
        branch.commits_text(),
        branch.outside()
    );
    let to = owner.clone().unwrap_or_else(|| humans.default_human().clone());
    Envelope::on_thread(thread.clone(), ant.clone(), to, text).with_owner(owner)
}

/// The message id of `ant`'s reply to `request`: the same for every attempt at it.
/// What hides the ant's secrets: the values of its env and its harness's, the daemon's environment variables named
/// like secrets, and the common patterns; nothing with `redact = false`.
fn redactor(config: &Config) -> Redactor {
    if !config.redact {
        return Redactor::off();
    }
    Redactor::default().with_env(&config.env).with_env(&config.harness.env).with_process_env()
}

/// `outbox`, redacting the text of what the ant sends with its MCP tools.
fn redacting_outbox(outbox: crate::mcp::Outbox, redactor: std::sync::Arc<Redactor>) -> crate::mcp::Outbox {
    std::sync::Arc::new(move |mut envelope: Envelope| {
        redactor.redact_string(&mut envelope.text);
        outbox(envelope)
    })
}

/// `channels`, redacting the text of what the ant posts with its MCP tools.
fn redacting_posts(
    channels: crate::mcp::ChannelAccess,
    redactor: std::sync::Arc<Redactor>,
) -> crate::mcp::ChannelAccess {
    let post = channels.post;
    crate::mcp::ChannelAccess {
        post: std::sync::Arc::new(move |mut posted: ChannelPost| {
            redactor.redact_string(&mut posted.text);
            post(posted)
        }),
        ..channels
    }
}

/// `sender`, redacting the text of the escalations it stores.
fn redacting_sender(sender: escalation::Sender, redactor: std::sync::Arc<Redactor>) -> escalation::Sender {
    std::sync::Arc::new(move |mut envelope: Envelope, id| {
        redactor.redact_string(&mut envelope.text);
        sender(envelope, id)
    })
}

/// `sender`, redacting the text of the notes posts it stores.
fn redacting_notes_sender(
    sender: escalation::NotesSender,
    redactor: std::sync::Arc<Redactor>,
) -> escalation::NotesSender {
    std::sync::Arc::new(move |mut post: ants_proto::NotesPost, id| {
        redactor.redact_string(&mut post.text);
        sender(post, id)
    })
}

/// Thread notes whose posts are redacted before they are stored.
struct RedactingNotes<N> {
    notes: N,
    redactor: std::sync::Arc<Redactor>,
}

impl<N: NotesStore> NotesStore for RedactingNotes<N> {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> futures::future::BoxFuture<'a, Result<notes::Notes>> {
        self.notes.read(thread)
    }

    fn post<'a>(
        &'a self,
        post: &'a ants_proto::NotesPost,
        last_seq: u64,
    ) -> futures::future::BoxFuture<'a, Result<bool>> {
        match self.redactor.redact(&post.text) {
            std::borrow::Cow::Borrowed(_) => self.notes.post(post, last_seq),
            std::borrow::Cow::Owned(text) => {
                let post = ants_proto::NotesPost { text, ..post.clone() };
                Box::pin(async move { self.notes.post(&post, last_seq).await })
            }
        }
    }
}

/// When the next record of a prompt's tool-call timeline is due: at its next tick once something changed.
async fn stored_every(calls: &crate::timeline::Collector<'_>, tick: &mut tokio::time::Interval) {
    if calls.pending() {
        tick.tick().await;
    } else {
        std::future::pending::<()>().await;
    }
}

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
mod tests;
