//! The inbox loop of one ant.
//!
//! Messages of one thread are handled one at a time, in order; different threads run concurrently, each in its own
//! harness from the pool.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{
    Attachment, AttachmentHandle, BuiltinAnt, ChannelEntry, ChannelId, ChannelPost, Envelope, JsDomain, Role,
    TIMELINE_EVERY, ThreadId, UserId, subject,
};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use futures::stream::{Stream, StreamExt as _};

use crate::admin::restart::Restarts;
use crate::approvals::NatsApprover;
use crate::channel::{Channels, Threads};
use crate::delivery::attempts::{Answered, Attempts};
use crate::delivery::dead::Buried;
use crate::delivery::unserved;
use crate::escalation::{self, Chain, Escalator};
use crate::harness::{Harness, Launch, ModelSettings, PromptLimits};
use crate::limits::breaker::Breaker;
use crate::limits::caps::{Caps, Ledger};
use crate::limits::fallback::Failover;
use crate::limits::ratelimit::{self, Backoffs, Throttle};
use crate::limits::signin::{self, SignIn};
use crate::mcp::{Colony, Mcp};
use crate::people::assistant_first::AssistantFirst;
use crate::people::behalf::Records;
use crate::people::humans::Registry;
use crate::permission::{Allowlist, PermissionPolicy};
use crate::pool::{Pool, SessionStore};
use crate::profile::identity::Identity;
use crate::provider::Providers;
use crate::provider::context::Callers;
use crate::provider::events::{self, Lifecycle};
use crate::provider::wake;
use crate::publish::{Publish, Publisher};
use crate::records::webhooks::Webhooks;
use crate::secrets::redact::Redactor;
use crate::stream::notes::{self, NotesStore};
use crate::stream::reading::Reading;
use crate::trail::Trails;
use crate::worktrees::{Branch, Worktrees};
use crate::{bus, colony, pause};

mod admission;
mod burial;
mod control;
mod delivery;
mod handling;
mod holds;
mod mcp_sign_in;
mod prompt;
mod queue;
mod steering;

pub use admission::Claims;
use control::Cancels;
use prompt::Unnarrowed;

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
/// How long a daemon stopping waits for the ant's sense to note its growth ([`growth`]).
const GROWN_WAIT: Duration = Duration::from_secs(10);

/// A few progress periods, so one late or lost progress ack does not redeliver a message still in hand.
const fn ack_wait(progress_every: Duration) -> Duration {
    progress_every.saturating_mul(3)
}

/// The settings an ant is served with, grouped by what uses them.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub ant: UserId,
    /// How the daemon reaches the bus.
    pub connection: Connection,
    /// What the ant's colony entry shows of it, and how the daemon holds that entry.
    pub presence: Presence,
    /// What the ant's harnesses run, where and with what.
    pub harness: HarnessSettings,
    /// How many harnesses run at once, and for how long.
    pub pooling: Pooling,
    /// Whether each thread works in its own git worktree, and what becomes of its branch.
    pub worktrees: WorktreeSettings,
    /// Who the ant answers to and what it may do unasked.
    pub policy: Policy,
    /// What the ant may spend.
    pub spending: Spending,
    /// Where the ant keeps its files, and when the bus's storage filling up is told.
    pub storage: Storage,
    /// What serves the ant's identity, memories and work.
    pub provider: crate::provider::Setting,
    /// For a manager ant of an `[[ant_template]]`, what adds the ants its `request_ant` tool asks for.
    pub requester: crate::mcp::Requester,
    /// Whether the ant is paused, and since when and by whom; equal to any other, so that pausing or resuming alone
    /// restarts no harness ([`crate::pause::Pause::replace`]).
    pub pause: crate::pause::Pause,
    /// The ant's settings this machine's config sets otherwise than its shared settings, which the ant runs with
    /// ([`crate::profile::shared`]), as its colony entry reports them; equal to any other, so that a change of drift alone
    /// restarts no harness ([`crate::profile::shared::Drift::replace`]).
    pub drift: crate::profile::shared::Drift,
}

/// How the daemon reaches the bus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub nats_url: String,
    /// Credentials file of the ant, when the server requires them.
    pub creds: Option<PathBuf>,
    /// JetStream domain of the hub, if it has one.
    pub js_domain: Option<JsDomain>,
}

/// What the ant's colony entry shows of it, besides its harness and chain, and how the daemon holds that entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Presence {
    /// The ant's role instructions, shared in the colony.
    pub whoami: Option<String>,
    /// Capabilities the ant advertises in the colony.
    pub capabilities: Vec<String>,
    /// The machine the ant's colony entry and notices name; the host name when None.
    pub machine: Option<String>,
    /// Built-in ants serve only while holding their colony entry ([`colony::hold`]).
    pub builtin: Option<BuiltinAnt>,
}

/// What the ant's harnesses run, where and with what.
#[derive(Debug, Clone, PartialEq)]
pub struct HarnessSettings {
    pub harness: std::sync::Arc<Harness>,
    pub model_settings: ModelSettings,
    /// Harnesses and models tried in order while the ant's own harness cannot serve.
    pub fallbacks: Vec<crate::limits::fallback::Fallback>,
    /// Directory the harness works in.
    pub workdir: PathBuf,
    pub env: BTreeMap<String, String>,
    /// Role, memory and task instructions prepended to every prompt.
    pub instructions: String,
    /// Limits on every prompt.
    pub limits: PromptLimits,
}

/// How many harnesses run at once, and for how long.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pooling {
    /// Most harness processes alive at once; the least recently used idle one stops to make room.
    pub max_harnesses: usize,
    /// A thread's harness stops after this long without a message, and resumes its session on the next one.
    pub idle_timeout: Duration,
    /// When memory is low, and the harnesses run at most while it is ([`crate::limits::pressure`]).
    pub memory_pressure: crate::limits::pressure::Settings,
    /// Trail workers computing at once across the process's runs ([`crate::trail::Conducting::workers`]).
    pub trail_workers: usize,
}

/// Whether each thread works in its own git worktree, and what becomes of its branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorktreeSettings {
    /// Each thread works in its own git worktree of the workdir's repository, under the home ([`Worktrees`]); unset,
    /// [`crate::worktrees::per_thread`] decides at start.
    pub per_thread: Option<bool>,
    /// Where a thread's new worktree branch starts ([`Worktrees::with_base`]).
    pub base: crate::worktrees::Base,
    /// What becomes of a thread's worktree branch holding new commits ([`crate::publish`]).
    pub publish: Publish,
}

/// Who the ant answers to and what it may do unasked.
#[derive(Debug, Clone, PartialEq)]
pub struct Policy {
    /// The default human, who gets escalations and notices of threads with no owner.
    pub human: UserId,
    /// Tool requests the harness may run without asking (see [`crate::permission`] for the rules). Every other one
    /// goes to the human on `ants.approvals.<ant>`.
    pub allow: Vec<String>,
    /// How long a tool request waits for the human before it is rejected.
    pub approval_timeout: Duration,
    /// The ant's escalation chain, published in the colony, and its limits.
    pub escalation: Chain,
    /// Redact secrets from what the ant publishes ([`crate::secrets::redact`]).
    pub redact: bool,
    /// The ant reviewing this one, its sage ([`crate::config::Ant::reviewed_by`]), published in its colony entry; none
    /// for a sage, which has none of its own, or where reviews are off.
    pub sage: Option<UserId>,
}

/// What the ant may spend.
#[derive(Debug, Clone, PartialEq)]
pub struct Spending {
    /// Token and cost caps of each thread and of the ant.
    pub caps: Caps,
    /// The share of a cap, in percent, past which a human is warned once; 0 for never.
    pub warn_percent: u8,
    /// The daily caps the ant shares with the other ants added from its template, if any.
    pub pool: Option<crate::limits::caps::Pool>,
}

/// Where the ant keeps its files, and when the bus's storage filling up is told.
#[derive(Debug, Clone, PartialEq)]
pub struct Storage {
    /// The ant's own files (usually `~/.ants/agents/<ant>`).
    pub home: PathBuf,
    /// When to warn a human of JetStream storage filling up.
    pub jetstream: crate::monitoring::storage::Settings,
}

/// Serve the ant's inbox until Ctrl-C. Prompts in flight are finished first; a second Ctrl-C stops at once. Until
/// the NATS server can be reached and its JetStream answers the daemon keeps trying ([`bus::connect_retrying`]). A daemon
/// that serves a built-in ant and hands it over waits for its turn again.
pub async fn run(config: Config) -> Result<()> {
    run_until(config, &Stop::default(), &Restarts::default(), &Providers::default()).await
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

/// [`run`], also stopping on `stop`'s request, restarting the ant's harnesses on the admin orders `restarts` passes on,
/// and serving the ant with its provider among `providers`, those of the process.
pub async fn run_until(config: Config, stop: &Stop, restarts: &Restarts, providers: &Providers) -> Result<()> {
    let _serving = crate::monitoring::metrics::ant(&config.ant).serving();
    let _served = restarts.serve(&config.ant);
    let harness = &config.harness.harness;
    if let Some(detail) = harness.unenforced(&config.policy.allow, !config.policy.escalation.escalate_to.is_empty()) {
        tracing::warn!(ant = %config.ant, fix = harness.gated_fix(), "{detail}");
    }
    let config = settle_worktrees(config).await;
    loop {
        match Box::pin(serve(&config, stop, restarts, providers)).await? {
            Served::Stopped => return Ok(()),
            Served::HandedOver => tracing::info!("another daemon serves the ant; waiting for it to stop"),
            Served::Moved => tracing::info!("the ant moved to another bus; waiting for `ants ant unseal`"),
        }
    }
}

/// The config with worktrees turned on or off for good, telling of a default turning them on and warning of threads
/// running at once in one git work tree.
async fn settle_worktrees(config: Config) -> Config {
    let (workdir, max_harnesses, asked) =
        (&config.harness.workdir, config.pooling.max_harnesses, config.worktrees.per_thread);
    let per_thread = crate::worktrees::per_thread(workdir, max_harnesses, asked).await;
    if asked.is_none() && per_thread {
        tracing::info!(
            ant = %config.ant,
            workdir = %workdir.display(),
            max_harnesses,
            "giving each thread its own git worktree, as threads may run at once in a git workdir; \
             worktrees = false shares the workdir"
        );
    } else if crate::worktrees::shares_git_workdir(workdir, max_harnesses, asked) {
        tracing::warn!(
            ant = %config.ant,
            workdir = %workdir.display(),
            max_harnesses,
            "threads running at once share one git work tree and may clobber each other's edits; \
             worktrees = true gives each thread its own"
        );
    }
    let worktrees = WorktreeSettings { per_thread: Some(per_thread), ..config.worktrees };
    Config { worktrees, ..config }
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
/// key when it has one ([`crate::secrets::sealing::AssistantSealing`]).
async fn assist(mcp: &Mcp, config: &Config, js: &async_nats::jetstream::Context, humans: &Registry) {
    let Some(creds) = &config.connection.creds else { return };
    let js_domain = &config.connection.js_domain;
    let human = match crate::people::behalf::keep_record(js, js_domain.as_ref(), &config.ant, creds).await {
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
    mcp.with_human_store(crate::mcp::jetstream_store(js.clone(), js_domain.clone(), creds.clone()));
    match assistant_sealing(&config.ant, creds, js, humans).await {
        Ok(Some(sealing)) => mcp.with_sealing(std::sync::Arc::new(sealing)),
        Ok(None) => {}
        Err(e) => tracing::warn!(error = format!("{e:#}"), "opening and sealing none of the human's sealed messages"),
    }
}

/// What an assistant opens and seals its human's messages with: none without a sealing key beside its credentials
/// at `creds`.
async fn assistant_sealing(
    ant: &UserId,
    creds: &std::path::Path,
    js: &async_nats::jetstream::Context,
    humans: &Registry,
) -> Result<Option<crate::secrets::sealing::AssistantSealing>> {
    let Some(xkey) = crate::secrets::sealing::load_key(creds)? else { return Ok(None) };
    let Some(account) = crate::people::humans::account(Some(creds)) else { return Ok(None) };
    let directory = crate::people::devices::Directory::open(js, &account).await?;
    Ok(Some(crate::secrets::sealing::AssistantSealing::new(ant.clone(), xkey, humans.clone(), directory)))
}

/// Serve the ant's inbox until Ctrl-C or `stop`, or until another daemon takes over its colony entry. A stop by a
/// retirement marked while served ([`events::mark_retired`]) deletes the ant's channel and done consumers
/// ([`bus::forget_retired`]) before it leaves the colony, which the retirement waits for to revoke its credentials.
async fn serve(config: &Config, stop: &Stop, restarts: &Restarts, providers: &Providers) -> Result<Served> {
    let allow = Allowlist::parse(&config.policy.allow)?;
    // A mark left by an earlier retirement the ant was served again after, which a provider taking no events keeps.
    let retired_before = events::retirement(&config.storage.home).await;
    let Some((client, js)) = connect(&config.connection, &config.ant, stop).await? else {
        tracing::info!("stopped before connecting");
        return Ok(Served::Stopped);
    };
    if !unsealed(config, &js, stop).await {
        return Ok(Served::Stopped);
    }
    let (metrics, workload) = (crate::monitoring::metrics::ant(&config.ant), workload(config));
    let _connected = metrics.connected(client.clone(), workload.clone());
    let Some((entry, claimed)) = take_turn(config, &js, stop).await? else {
        return Ok(Served::Stopped);
    };
    let account = || crate::people::humans::account(config.connection.creds.as_deref());
    let humans = Registry::new(config.policy.human.clone()).verifying_grants(account());
    let _watching = humans.watch(js.clone(), crate::people::humans::RETRY);
    let Services {
        redactor,
        approver,
        colony,
        reading,
        notes,
        sessions_kv,
        ledger,
        escalator,
        identity,
        lifecycle,
        mcp,
        launch,
        mcp_sign_ins,
    } = services(config, &client, &js, &humans, allow, &entry.machine, providers).await?;
    let stream = bus::open_stream(&js, &config.ant).await?;
    let threads = Threads::new(js.clone(), config.ant.clone(), sessions_kv.clone()).reading(reading);
    let storage = storage_watcher(config, &js, &sessions_kv);
    let sessions = SessionStore::kv(sessions_kv, config.ant.clone());
    let attempts_kv = bus::ensure_attempts(&js, config.connection.js_domain.as_ref(), &config.ant).await?;
    let outbox = redacting_outbox(crate::mcp::jetstream_outbox(js.clone()), redactor.clone());
    let _waking = wake::serve(providers, &config.ant, &js, outbox.clone(), sessions.clone(), attempts_kv.clone()).await;
    let (answered, buried) = delivery_marks(&config.storage.home).await;
    let pool = harness_pool(config, launch, sessions, &workload).await.with_lifecycle(lifecycle.clone());
    let publisher = publisher(config, &js, &redactor, &pool).await;
    let (messages, channels) = inbox_messages(&js, &stream, config).await?;
    let sense = providers.sense_of(&config.provider);
    let registration = enter_colony(&js, entry.clone(), claimed, &workload, sense).await?;
    let inbox = Inbox {
        client: &client,
        js: &js,
        ant: &config.ant,
        builtin: config.presence.builtin,
        reading,
        machine: &entry.machine,
        pool: &pool,
        cancels: Cancels::default(),
        steers: steering::Steers::default(),
        unnarrowed: Unnarrowed::default(),
        restarts,
        attempts: &Attempts::new(attempts_kv.clone(), config.ant.clone()),
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
        limits: config.harness.limits,
        ledger: &ledger,
        escalator: &escalator,
        notes: notes.as_ref(),
        threads: &threads,
        webhooks: &Webhooks::new(js.clone()),
        colony: &colony,
        humans: &humans,
        behalf: Records::new(js.clone(), config.connection.js_domain.clone(), account()),
        retry_delay: RETRY_DELAY,
        progress_every: PROGRESS_EVERY,
        timeline_every: TIMELINE_EVERY,
        sign_in: SignIn::default(),
        sign_in_every: signin::CHECK_EVERY,
        mcp_sign_ins,
        breaker: Breaker::default(),
        throttle: Throttle::default(),
        backoffs: ratelimit::BACKOFFS,
        failover: Failover::default(),
        fallback_retry: crate::limits::fallback::RETRY_AFTER,
        identity: identity.as_ref(),
        home: &config.storage.home,
        lifecycle: lifecycle.as_ref(),
        sense,
        trails: trails(config, &client, &js, &humans, &mcp, outbox, attempts_kv, notes.clone()).await,
    };
    let served = Box::pin(serve_inbox(&inbox, messages, channels, config, stop, &registration, &storage)).await?;
    forget_if_retired(&served, config, &js, retired_before).await;
    wind_down(&served, registration, pool, lifecycle.as_ref(), growth(config, identity.as_ref(), providers)).await;
    Ok(served)
}

/// Note on the sense card of the ant the prompts its identity record counts, so a record made again after a revoke
/// starts from all of them ([`crate::sense::Sense::stopping`]); a failure only warns.
async fn growth(config: &Config, identity: Option<&Identity>, providers: &Providers) {
    let (Some(identity), Some(sense)) = (identity, providers.sense_of(&config.provider)) else { return };
    let error = match tokio::time::timeout(GROWN_WAIT, sense.stopping(&config.ant, identity)).await {
        Ok(Ok(())) => return,
        Ok(Err(e)) => format!("{e:#}"),
        Err(_) => format!("no answer within {GROWN_WAIT:?}"),
    };
    tracing::warn!(error, "noting the ant's growth in its sense data");
}

/// The trails the ant conducts, their runs continued from their records and `mcp` serving `run_trail` for them; None,
/// with a warning, when the bus keeps no runs.
#[expect(clippy::too_many_arguments, reason = "the services a conductor writes through, each built by serve")]
async fn trails(
    config: &Config,
    client: &async_nats::Client,
    js: &async_nats::jetstream::Context,
    humans: &Registry,
    mcp: &Mcp,
    outbox: crate::mcp::Outbox,
    attempts: crate::records::direct_kv::DirectKv,
    notes: std::sync::Arc<dyn NotesStore>,
) -> Option<Trails> {
    let conducting = crate::trail::Conducting {
        js: js.clone(),
        js_domain: config.connection.js_domain.clone(),
        client: client.clone(),
        ant: config.ant.clone(),
        worker: std::env::current_exe().ok()?,
        trails_dir: crate::config::home_dir().ok().map(|home| home.join(".ants").join("trails")),
        humans: humans.clone(),
        colony: crate::mcp::jetstream_colony(js.clone()),
        outbox,
        attempts,
        workers: config.pooling.trail_workers,
        notes,
    };
    match Trails::new(conducting).await {
        Ok(trails) => {
            trails.restore().await;
            mcp.with_trails(trails.clone());
            Some(trails)
        }
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "conducting no trails");
            None
        }
    }
}

/// The ant's workload, paused and drifting as the config says at start.
fn workload(config: &Config) -> std::sync::Arc<colony::Workload> {
    let workload = std::sync::Arc::new(colony::Workload::default());
    workload.set_paused(config.pause.get());
    workload.set_drift(config.drift.get());
    workload
}

/// Delete the ant's channel and done consumers when [`serve`] stopped as a retirement other than `retired_before` is
/// marked.
async fn forget_if_retired(
    served: &Served,
    config: &Config,
    js: &jetstream::Context,
    retired_before: Option<uuid::Uuid>,
) {
    if *served == Served::Stopped
        && events::retirement(&config.storage.home).await.is_some_and(|r| Some(r) != retired_before)
    {
        bus::forget_retired(js, &config.ant).await;
    }
}

/// Leave the colony and end the harness sessions once [`serve`] ended; on a stop, once no prompt is in flight, have the
/// ant's sense note its growth (`grown`) and tell its provider of its retirement when that stop retired it
/// ([`Lifecycle::stopped`]), before leaving the colony: a retirement revokes the ant's credentials once it left, and
/// the provider may sign what it records with them. A handover or a move leaves the growth to the daemon serving the
/// ant next, which notes it on opening the ant's sense data.
async fn wind_down(
    served: &Served,
    registration: colony::Registration,
    pool: Pool,
    lifecycle: Option<&Lifecycle>,
    grown: impl Future<Output = ()>,
) {
    if *served != Served::Stopped {
        registration.deregister().await;
        pool.shutdown().await;
        return;
    }
    pool.shutdown().await;
    grown.await;
    if let Some(lifecycle) = lifecycle {
        lifecycle.stopped(events::RETIRED_WAIT).await;
    }
    registration.deregister().await;
    tracing::info!("stopped");
}

/// The ant's colony entry, and the revision of it claimed when the daemon defers to others ([`wait_for_turn`]):
/// `None` when `stop` comes first.
async fn take_turn(
    config: &Config,
    js: &jetstream::Context,
    stop: &Stop,
) -> Result<Option<(ants_proto::ColonyEntry, Option<u64>)>> {
    let mut entry = colony::entry(config).await;
    if config.presence.builtin.is_none() {
        return Ok(Some((entry, None)));
    }
    let Some(revision) = wait_for_turn(js, &mut entry, stop).await? else {
        return Ok(None);
    };
    Ok(Some((entry, Some(revision))))
}

/// What the ant's inbox serves with on the bus, besides its harness pool, in the order [`services`] builds it.
struct Services {
    redactor: std::sync::Arc<Redactor>,
    approver: NatsApprover,
    colony: Colony,
    reading: Reading,
    notes: std::sync::Arc<RedactingNotes<crate::stream::notes::JetStreamNotes>>,
    sessions_kv: crate::records::direct_kv::DirectKv,
    ledger: std::sync::Arc<Ledger>,
    escalator: std::sync::Arc<Escalator>,
    identity: Option<Identity>,
    /// Where the ant's lifecycle events go, when its provider takes them and its identity record could be read.
    lifecycle: Option<Lifecycle>,
    mcp: Mcp,
    /// How the harnesses start, with the ant's MCP server and its tool requests going to `approver`, and the
    /// sign-ins to MCP servers of their sessions going to `mcp_sign_ins`.
    launch: Launch,
    mcp_sign_ins: mcp_sign_in::Untold,
}

/// Set up the ant's redactor, approvals, notes, spending ledger, escalator and records, and start its MCP server, its
/// provider among `providers` taking tool calls naming `machine` for the machine serving the ant.
async fn services(
    config: &Config,
    client: &async_nats::Client,
    js: &jetstream::Context,
    humans: &Registry,
    allow: Allowlist,
    machine: &str,
    providers: &Providers,
) -> Result<Services> {
    let redactor = std::sync::Arc::new(redactor(config.policy.redact, &config.harness));
    let colony = crate::mcp::jetstream_colony(js.clone());
    let account = crate::people::humans::account(config.connection.creds.as_deref());
    let records = Records::new(js.clone(), config.connection.js_domain.clone(), account);
    let assistants = AssistantFirst::with_records(humans.clone(), colony.clone(), records);
    let approver = approver(&config.ant, &config.policy, client, js, humans).with_assistants(assistants.clone());
    let policy =
        PermissionPolicy::new(allow, Some(std::sync::Arc::new(approver.clone()))).with_redactor(redactor.clone());
    let outbox = redacting_outbox(crate::mcp::jetstream_outbox(js.clone()), redactor.clone());
    let reading = Reading::daemon(crate::secrets::creds::is_assistant(config.connection.creds.as_deref()));
    let notes = crate::stream::notes::JetStreamNotes::new(js.clone(), config.ant.clone()).reading(reading);
    let notes = std::sync::Arc::new(RedactingNotes { notes, redactor: redactor.clone() });
    let sessions_kv = bus::ensure_sessions(js, config.connection.js_domain.as_ref(), &config.ant).await?;
    let ledger = ledger(&config.ant, &config.spending, &sessions_kv);
    let escalator = escalator(config, js, &colony, humans, &ledger, &redactor, assistants);
    let (identity, identity_id) = ant_records(config, js).await;
    let sense = providers.sense_of(&config.provider);
    if let Some(sense) = sense {
        sense.serve(&config.ant, &config.storage.home, config.connection.creds.clone());
        sense.identify(&config.ant, identity.clone());
    }
    let lifecycle = match &identity_id {
        Some(id) => Lifecycle::open(providers.outbox(), &config.ant, id, &config.storage.home)
            .await
            .map(|lifecycle| lifecycle.with_workdir(config.harness.workdir.clone())),
        None => None,
    };
    let mut mcp = Mcp::start(
        config.ant.clone(),
        &config.storage.home,
        outbox,
        colony.clone(),
        notes.clone(),
        humans.clone(),
        Some(escalator.clone()),
    )
    .await?;
    connect_mcp(&mut mcp, config, client, js, &redactor, humans, providers).await?;
    mcp.with_origins(crate::provider::origins::OriginStore::kv(sessions_kv.clone(), config.ant.clone()));
    if let Some(identity) = &identity {
        mcp.with_callers(Callers::new(config.ant.clone(), machine.to_owned(), identity.clone()));
    }
    let mcp_sign_ins = mcp_sign_in::Untold::default();
    let mut launch = launch(&config.harness, policy, &mcp).with_mcp_sign_ins(mcp_sign_ins.sink());
    if let Some(sense) = sense {
        launch = launch.with_skills(sense.skills(&config.storage.home));
    }
    Ok(Services {
        redactor,
        approver,
        colony,
        reading,
        notes,
        sessions_kv,
        ledger,
        escalator,
        identity,
        lifecycle,
        mcp,
        launch,
        mcp_sign_ins,
    })
}

/// Wait while the ant's identity record is marked moved, so no daemon starts a moved ant: false when `stop` comes
/// first. A record that cannot be read does not keep the ant from being served.
async fn unsealed(config: &Config, js: &jetstream::Context, stop: &Stop) -> bool {
    let identity = match Identity::new(js, config.connection.js_domain.as_ref(), &config.ant).await {
        Ok(identity) => identity,
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "reading the ant's identity for a move");
            return true;
        }
    };
    tokio::select! {
        () = identity.unsealed(crate::profile::identity::MOVED_EVERY) => true,
        () = stopped(stop) => false,
    }
}

/// Keep the ant's escalation chain past the colony, and return its identity record, created when absent, with its id:
/// None when it cannot be read, which the warning says.
async fn ant_records(config: &Config, js: &jetstream::Context) -> (Option<Identity>, Option<String>) {
    if let Err(e) = crate::records::chains::publish(js, &config.ant, &config.policy.escalation.escalate_to).await {
        tracing::warn!(error = format!("{e:#}"), "keeping the escalation chain past the colony");
    }
    let identity = Identity::new(js, config.connection.js_domain.as_ref(), &config.ant).await;
    let ensured = match &identity {
        Ok(identity) => identity.ensure().await,
        Err(e) => Err(anyhow::anyhow!("{e:#}")),
    };
    let id = match ensured {
        Ok(Some(record)) => Some(record.id),
        Ok(None) => {
            tracing::warn!("no identities bucket: `ants init` creates it; the ant's prompts go uncounted");
            None
        }
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "reading the ant's identity; its prompts go uncounted");
            None
        }
    };
    (identity.ok(), id)
}

/// The ant's approver of tool requests, which the permission policy asks for calls its allowlist does not cover.
fn approver(
    ant: &UserId,
    settings: &Policy,
    client: &async_nats::Client,
    js: &jetstream::Context,
    humans: &Registry,
) -> NatsApprover {
    NatsApprover::new(client.clone(), js.clone(), ant.clone(), settings.approval_timeout)
        .with_chain(settings.escalation.clone())
        .with_humans(humans.clone())
}

/// Connect to the bus as the ant's daemon, retrying until it answers: `None` when `stop` comes first.
async fn connect(
    connection: &Connection,
    ant: &UserId,
    stop: &Stop,
) -> Result<Option<(async_nats::Client, jetstream::Context)>> {
    bus::connect_retrying(
        &connection.nats_url,
        connection.creds.as_deref(),
        connection.js_domain.as_ref(),
        ant,
        &format!("ants-daemon-{ant}"),
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
fn ledger(ant: &UserId, spending: &Spending, sessions: &crate::records::direct_kv::DirectKv) -> std::sync::Arc<Ledger> {
    let ledger = Ledger::kv(sessions.clone(), ant.clone(), spending.caps)
        .warn_at(spending.warn_percent)
        .with_pool(spending.pool.clone());
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
    assistants: AssistantFirst,
) -> std::sync::Arc<Escalator> {
    let escalator = Escalator::new(
        config.ant.clone(),
        config.policy.escalation.clone(),
        colony.clone(),
        humans.clone(),
        ledger.clone(),
        redacting_sender(escalation::jetstream_sender(js.clone()), redactor.clone()),
    )
    .with_chains(crate::records::chains::jetstream_chains(js.clone()))
    .with_notes(redacting_notes_sender(escalation::jetstream_notes_sender(js.clone()), redactor.clone()))
    .with_heed(crate::records::heed::jetstream_heed(js.clone(), config.connection.js_domain.clone()))
    .with_assistants(assistants);
    std::sync::Arc::new(escalator)
}

/// Give the ant's MCP server its live inbox, its ways to the bus and its provider among `providers`, and an
/// assistant's the human it acts for.
async fn connect_mcp(
    mcp: &mut Mcp,
    config: &Config,
    client: &async_nats::Client,
    js: &jetstream::Context,
    redactor: &std::sync::Arc<Redactor>,
    humans: &Registry,
    providers: &Providers,
) -> Result<()> {
    mcp.listen(bus::live_inbox(client, &config.ant).await?);
    let channels = crate::mcp::jetstream_channels(js.clone(), config.connection.js_domain.clone());
    mcp.with_channels(redacting_posts(channels, redactor.clone()));
    mcp.with_lookup(crate::mcp::jetstream_lookup(js.clone()));
    mcp.with_files(crate::mcp::jetstream_files(js.clone()), config.harness.workdir.clone());
    mcp.with_redactor(redactor.clone());
    // Without credentials the server is open, with no `ants signer` to answer, and the daemon reads what it would.
    mcp.with_reaction_asker(match config.connection.creds {
        Some(_) => crate::mcp::nats_reaction_asker(client.clone()),
        None => crate::mcp::local_reaction_asker(js.clone(), config.connection.js_domain.clone()),
    });
    if let Some(requester) = &config.requester.0 {
        mcp.with_requester(requester.clone());
    }
    mcp.manage_channels(crate::chanadm::nats_admin(client.clone(), js.clone()));
    providers.assign(&config.ant, &config.provider);
    if let Some(provider) = providers.of(&config.ant).await {
        mcp.with_provider(provider);
    }
    assist(mcp, config, js, humans).await;
    Ok(())
}

/// How the ant's harnesses start.
fn launch(harness: &HarnessSettings, policy: PermissionPolicy, mcp: &Mcp) -> Launch {
    Launch::new(
        harness.harness.clone(),
        harness.workdir.clone(),
        harness.env.clone().into_iter().collect(),
        policy,
        harness.model_settings.clone(),
    )
    .with_instructions(harness.instructions.clone())
    .with_mcp(mcp.servers())
}

fn storage_watcher(
    config: &Config,
    js: &jetstream::Context,
    sessions: &crate::records::direct_kv::DirectKv,
) -> crate::monitoring::storage::Watcher {
    let connection = &config.connection;
    crate::monitoring::storage::Watcher {
        js: js.clone(),
        hub_domain: connection.js_domain.clone(),
        notes: sessions.clone(),
        ant: config.ant.clone(),
        human: config.policy.human.clone(),
        settings: config.storage.jetstream,
        server: format!("{} {:?}", connection.nats_url, connection.js_domain),
    }
}

/// The marks of the messages answered and buried, pruned of those the stream no longer keeps.
async fn delivery_marks(home: &std::path::Path) -> (Answered, Buried) {
    let answered = Answered::new(home);
    answered.prune(bus::RETENTION).await;
    let buried = Buried::new(home);
    buried.prune(bus::RETENTION).await;
    (answered, buried)
}

/// The ant's harness pool, isolating threads in worktrees when asked to, or else cleaning up the ones left behind, and
/// reviews among their checkouts, cleared of those an earlier run left.
async fn harness_pool(
    config: &Config,
    launch: Launch,
    sessions: SessionStore,
    workload: &std::sync::Arc<colony::Workload>,
) -> Pool {
    let (pooling, home, base) = (&config.pooling, &config.storage.home, config.worktrees.base);
    let fallbacks = config.harness.fallbacks.iter().map(|f| (f.name(), launch.with_fallback(f))).collect();
    let checkouts = crate::checkouts::Checkouts::new(home);
    checkouts.clear();
    let mut pool = Pool::new(launch, sessions, pooling.max_harnesses, pooling.idle_timeout)
        .with_fallbacks(fallbacks)
        .with_checkouts(checkouts)
        .reporting_to(workload.clone())
        .with_memory_pressure(pooling.memory_pressure, crate::limits::pressure::system_probe());
    let isolating = if config.worktrees.per_thread == Some(true) {
        Worktrees::find(&config.ant, &config.harness.workdir, home).await
    } else {
        None
    };
    if let Some(worktrees) = isolating {
        pool = pool.with_worktrees(worktrees.with_base(base));
    } else if let Some(worktrees) = Worktrees::left_behind(&config.ant, home).await {
        pool = pool.with_worktrees_left_behind(worktrees.with_base(base));
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
    if config.worktrees.publish == Publish::Off {
        return None;
    }
    if pool.worktrees().is_none() {
        tracing::warn!("publish needs each thread in its own git worktree (worktrees = true); publishing nothing");
        return None;
    }
    let publisher = Publisher::new(config.ant.clone()).with_redactor(redactor.clone());
    let js_domain = config.connection.js_domain.as_ref();
    Some(match crate::records::direct_kv::DirectKv::new(js, js_domain, ants_proto::THREADS_BUCKET).await {
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
    let in_hand = i64::try_from(config.pooling.max_harnesses.max(1) * IN_HAND_PER_HARNESS)?;
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

/// Put the ant on the colony, holding its entry from the revision claimed, if any, its first entry showing the profile
/// `sense`, its provider if sense, holds of it.
async fn enter_colony(
    js: &jetstream::Context,
    entry: ants_proto::ColonyEntry,
    claimed: Option<u64>,
    workload: &std::sync::Arc<colony::Workload>,
    sense: Option<&crate::sense::Sense>,
) -> Result<colony::Registration> {
    if let Some(sense) = sense {
        sense.first_profile(&entry.ant, workload).await;
    }
    Ok(match claimed {
        Some(revision) => colony::hold(js, entry, revision, workload.clone()).await?,
        None => colony::register(js, entry, workload.clone()).await?,
    })
}

fn log_serving(config: &Config) {
    tracing::info!(
        ant = %config.ant,
        harness = %config.harness.harness,
        model_settings = ?config.harness.model_settings,
        workdir = %config.harness.workdir.display(),
        max_harnesses = config.pooling.max_harnesses,
        worktrees = config.worktrees.per_thread == Some(true),
        worktree_base = config.worktrees.base.as_str(),
        publish = config.worktrees.publish.as_str(),
        idle_timeout = ?config.pooling.idle_timeout,
        capabilities = ?config.presence.capabilities,
        allow = ?config.policy.allow,
        approval_timeout = ?config.policy.approval_timeout,
        limits = ?config.harness.limits,
        caps = ?config.spending.caps,
        human = %config.policy.human,
        escalation = ?config.policy.escalation,
        "serving inbox"
    );
}

/// What the dreams of the ant `config` serves take from its daemon.
fn dreaming(inbox: &Inbox<'_>, config: &Config) -> std::sync::Arc<dyn crate::sense::Dreaming> {
    std::sync::Arc::new(crate::sense::Daemon {
        js: inbox.js.clone(),
        js_domain: config.connection.js_domain.clone(),
        ant: config.ant.clone(),
        reading: inbox.reading,
        origins: inbox.mcp.and_then(crate::mcp::Mcp::origin_store).cloned(),
        workload: inbox.workload.clone(),
        started: chrono::Utc::now(),
        workdir: config.harness.workdir.clone(),
        sage: config.policy.sage.clone(),
        colony: crate::mcp::jetstream_colony(inbox.js.clone()),
        outbox: redacting_outbox(
            crate::mcp::jetstream_outbox(inbox.js.clone()),
            std::sync::Arc::new(inbox.redactor.clone()),
        ),
        reactions: inbox.mcp.and_then(crate::mcp::Mcp::reaction_asker),
    })
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
    storage: &crate::monitoring::storage::Watcher,
) -> Result<Served>
where
    S: Stream<Item = Result<jetstream::Message, E>> + Unpin,
    E: Display,
{
    log_serving(config);
    let mut unserved = unserved::Watch::new(
        inbox.js,
        config.connection.js_domain.as_ref(),
        config.ant.clone(),
        Role::Ant,
        inbox.colony.clone(),
    )
    .await?
    .reading(inbox.reading);
    let _sense_workdirs = inbox.sense.zip(inbox.mcp).map(|(sense, mcp)| sense.workdirs(&config.ant, mcp.workdirs()));
    let scheduler =
        inbox.sense.map(|sense| sense.schedule(config, inbox.machine, dreaming(inbox, config), inbox.workload.clone()));
    let (stopping, moving, mut asked) = (std::cell::Cell::new(false), std::cell::Cell::new(false), 0);
    // The first stop is Ctrl-C, a stop request, the hand-over or the move, after which the prompts in flight finish;
    // the second only Ctrl-C or a stop request.
    let stop_or_hand_over = || {
        asked += 1;
        let (first, stopping, moving) = (asked == 1, &stopping, &moving);
        let scheduler = scheduler.as_ref();
        async move {
            let moved = async {
                match inbox.identity {
                    Some(identity) => identity.moved(crate::profile::identity::MOVED_EVERY).await,
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
            if let Some(scheduler) = scheduler {
                scheduler.stop().await;
            }
        }
    };
    tokio::select! {
        () = inbox.serve(messages, channels, config.pooling.idle_timeout, stop_or_hand_over) => {}
        () = inbox.escalator.deadlines() => {}
        () = inbox.pool.prune_worktrees_every(PRUNE_WORKTREES_EVERY, async |kept| {
            tell_kept_branches(inbox.js, inbox.threads, inbox.humans, &config.ant, kept).await;
        }) => {}
        () = inbox.hear_cancels() => {}
        () = inbox.hear_restarts() => {}
        () = inbox.hear_clones() => {}
        () = inbox.hear_mcp() => {}
        () = async {
            match &inbox.trails {
                Some(trails) => trails.serve().await,
                None => std::future::pending().await,
            }
        } => {}
        () = async {
            match inbox.sense {
                Some(sense) => sense.publish(&config.ant, inbox.workload).await,
                None => std::future::pending().await,
            }
        } => {}
        () = crate::secrets::creds::recheck(config.connection.creds.as_deref()) => {}
        () = storage.watch(crate::monitoring::storage::CHECK_EVERY) => {}
        () = tell_unserved(inbox.js, inbox.humans, &mut unserved) => {}
        () = events::done::hear(inbox.js, inbox.ant, inbox.lifecycle, async |thread| {
            inbox.pool.has_session(thread).await
        }) => {}
    }
    if let Some(scheduler) = &scheduler {
        scheduler.stop().await;
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
    /// How the message steered its thread.
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
    /// It arrived behind another steer that cancelled the running prompt already, or the prompt it was to join ended
    /// before taking it.
    Ahead,
    /// It joins the prompt running on its thread, whose harness takes it into its turn ([`steering`]).
    Joining,
}

impl Incoming {
    /// The channel of the thread the message came on, as its origin names it: a post's, or the channel of a thread it
    /// renewed ([`Incoming::resume`]).
    fn channel(&self) -> Option<&ChannelId> {
        self.post.as_ref().map(|posted| &posted.post.channel).or(self.resume.as_ref())
    }

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
    /// What the channel is for, as its entry said when the post arrived.
    description: Option<String>,
}

struct Inbox<'a> {
    client: &'a async_nats::Client,
    js: &'a jetstream::Context,
    ant: &'a UserId,
    builtin: Option<BuiltinAnt>,
    /// How the daemon reads through the ant's own readers ([`Reading::daemon`]).
    reading: Reading,
    /// The machine its notices name, as its colony entry does.
    machine: &'a str,
    pool: &'a Pool,
    /// The prompts running, for `ants cancel`.
    cancels: Cancels,
    /// The prompts running that take messages joining them.
    steers: steering::Steers,
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
    workload: &'a std::sync::Arc<colony::Workload>,
    /// Whether the ant is paused: then no prompt starts, the messages in hand waiting.
    pause: &'a crate::pause::Pause,
    /// The ant's drift, which its colony entry reports as it changes.
    drift: &'a crate::profile::shared::Drift,
    /// The ant's counters, for monitoring.
    metrics: std::sync::Arc<crate::monitoring::metrics::Ant>,
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
    /// The sign-ins to MCP servers the harness's sessions made as they opened, until told.
    mcp_sign_ins: mcp_sign_in::Untold,
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
    /// The ant's home, where a clone's copy of its threads' history is ([`crate::stream::history::cloned`]).
    home: &'a std::path::Path,
    /// Told of the threads the ant starts ([`Inbox::thread_started`]); None when its provider takes no events.
    lifecycle: Option<&'a Lifecycle>,
    /// The ant's sense, which takes its sage's answers to the reviews of its dreams ([`Inbox::arrived`]) and is told
    /// of its growth ([`Inbox::count_prompt`]); None when sense is not its provider.
    sense: Option<&'a crate::sense::Sense>,
    /// The trails the ant conducts, which take the replies to their steps ([`Inbox::arrived`]); None when the bus
    /// keeps no runs.
    trails: Option<Trails>,
}

impl Inbox<'_> {
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
            let notice = Envelope::new(ant.clone(), to, found.text(&ant, chrono::Utc::now()))
                .as_notice(ants_proto::NoticeKind::Unserved);
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
    Envelope::on_thread(thread.clone(), ant.clone(), to, text)
        .with_owner(owner)
        .as_notice(ants_proto::NoticeKind::Expired)
}

/// What hides the ant's secrets: the values of its env and its harness's, the daemon's environment variables named
/// like secrets, and the common patterns; nothing with `redact = false`.
fn redactor(redact: bool, harness: &HarnessSettings) -> Redactor {
    if !redact {
        return Redactor::off();
    }
    Redactor::default().with_env(&harness.env).with_env(&harness.harness.env).with_process_env()
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
        post: std::sync::Arc::new(move |mut posted: ChannelPost, source| {
            redactor.redact_string(&mut posted.text);
            post(posted, source)
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
