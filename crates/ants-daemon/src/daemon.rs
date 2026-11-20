//! The inbox loop of one ant.
//!
//! Messages of one thread are handled one at a time, in order; different threads run concurrently, each in its own
//! harness from the pool.

use std::fmt::Display;
use std::time::Duration;

use ants_proto::{BuiltinAnt, Envelope, Role, TIMELINE_EVERY, UserId, subject};
use anyhow::Result;
use async_nats::jetstream;
use futures::stream::Stream;

use crate::admin::restart::Restarts;
use crate::approvals::NatsApprover;
use crate::channel::{Channels, Threads};
use crate::delivery::attempts::{Answered, Attempts};
use crate::delivery::dead::Buried;
use crate::delivery::unserved;
use crate::escalation::Escalator;
use crate::harness::PromptLimits;
use crate::limits::caps::Ledger;
use crate::limits::ratelimit::{self, Backoffs};
use crate::limits::signin;
use crate::mcp::{Colony, Mcp};
use crate::people::behalf::Records;
use crate::people::humans::Registry;
use crate::permission::Allowlist;
use crate::pool::{Pool, SessionStore};
use crate::profile::identity::Identity;
use crate::provider::Providers;
use crate::provider::events::{self, Lifecycle};
use crate::provider::wake;
use crate::publish::Publisher;
use crate::secrets::redact::Redactor;
use crate::stream::notes::NotesStore;
use crate::stream::reading::Reading;
use crate::trail::Trails;
use crate::worktrees::Branch;
use crate::{bus, colony};

mod admission;
mod burial;
mod control;
mod delivery;
mod handling;
mod holds;
mod incoming;
mod mcp_sign_in;
mod prompt;
mod queue;
mod redacting;
mod review;
mod settings;
mod setup;
mod steering;

pub use admission::Claims;
use control::Cancels;
use incoming::{Handling, InHand, Incoming, Posted, Steered};
use prompt::Unnarrowed;
pub use settings::{
    Config, Connection, HarnessSettings, Policy, Pooling, Presence, Spending, Storage, WorktreeSettings,
};
use setup::{
    Bus, Services, connect, delivery_marks, enter_colony, forget_if_retired, growth, harness_pool, inbox_messages,
    publisher, services, settle_worktrees, storage_watcher, take_turn, trails, unsealed, wind_down, workload,
};

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

/// How [`serve`] ended.
#[derive(Debug, PartialEq, Eq)]
enum Served {
    Stopped,
    /// Another daemon took over the ant's colony entry.
    HandedOver,
    /// The ant's identity record was marked moved ([`ants_proto::AntIdentity::moved`]).
    Moved,
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
    let shared = Bus { config, client: &client, js: &js, humans: &humans, providers };
    let (services, launch) = services(shared, allow, &entry.machine).await?;
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
        ..
    } = &services;
    let stream = bus::open_stream(&js, &config.ant).await?;
    let threads = Threads::new(js.clone(), config.ant.clone(), sessions_kv.clone()).reading(*reading);
    let storage = storage_watcher(config, &js, sessions_kv);
    let sessions = SessionStore::kv(sessions_kv.clone(), config.ant.clone());
    let (outbox, attempts_kv) = (services.outbox.clone(), services.attempts_kv.clone());
    let _waking = wake::serve(providers, &config.ant, &js, outbox, sessions.clone(), attempts_kv.clone()).await;
    let (answered, buried) = delivery_marks(&config.storage.home).await;
    let pool = harness_pool(config, launch, sessions, &workload).await.with_lifecycle(lifecycle.clone());
    let publisher = publisher(config, &js, redactor, &pool).await;
    let (messages, channels) = inbox_messages(&js, &stream, config).await?;
    let sense = providers.sense_of(&config.provider);
    let registration = enter_colony(&js, entry.clone(), claimed, &workload, sense).await?;
    let inbox = Inbox {
        client: &client,
        js: &js,
        ant: &config.ant,
        builtin: config.presence.builtin,
        reading: *reading,
        machine: &entry.machine,
        pool: &pool,
        running: Running::default(),
        restarts,
        attempts: &Attempts::new(attempts_kv, config.ant.clone()),
        answered: &answered,
        buried: &buried,
        mcp: Some(&services.mcp),
        approvals: Some(approver),
        publisher: publisher.as_ref(),
        redactor,
        workload: &workload,
        pause: &config.pause,
        drift: &config.drift,
        metrics,
        limits: config.harness.limits,
        ledger,
        escalator,
        notes: notes.as_ref(),
        threads: &threads,
        colony,
        humans: &humans,
        behalf: Records::new(js.clone(), config.connection.js_domain.clone(), account()),
        pacing: Pacing::default(),
        holds: holds::Holds { mcp_sign_ins: services.mcp_sign_ins.clone(), ..holds::Holds::default() },
        identity: identity.as_ref(),
        home: &config.storage.home,
        lifecycle: lifecycle.as_ref(),
        sense,
        trails: trails(shared, &services).await,
    };
    let served = Box::pin(serve_inbox(&inbox, messages, channels, config, stop, &registration, &storage)).await?;
    forget_if_retired(&served, config, &js, retired_before).await;
    wind_down(&served, registration, pool, lifecycle.as_ref(), growth(config, identity.as_ref(), providers)).await;
    Ok(served)
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
        outbox: crate::mcp::jetstream_outbox(inbox.js.clone()),
        redactor: inbox.redactor.clone(),
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
                Some(trails) => {
                    let _conducted = inbox.restarts.conduct(inbox.ant, trails);
                    trails.serve().await;
                }
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

/// How often the inbox acks, retries, stores and checks; tests shorten them.
struct Pacing {
    /// How long a message that failed waits before it is tried again.
    retry_delay: Duration,
    /// Period of the progress acks of the messages in hand.
    progress_every: Duration,
    /// How often at most a prompt's tool-call timeline is stored while it runs.
    timeline_every: Duration,
    /// How often a signed-out harness, or one failing to start, is checked.
    sign_in_every: Duration,
    /// How long messages wait out a provider limit.
    backoffs: Backoffs,
    /// How long a fallback that turned a message away is passed over, unless its provider limit says.
    fallback_retry: Duration,
}

impl Default for Pacing {
    fn default() -> Self {
        Self {
            retry_delay: RETRY_DELAY,
            progress_every: PROGRESS_EVERY,
            timeline_every: TIMELINE_EVERY,
            sign_in_every: signin::CHECK_EVERY,
            backoffs: ratelimit::BACKOFFS,
            fallback_retry: crate::limits::fallback::RETRY_AFTER,
        }
    }
}

/// The prompts running, and what the inbox keeps of them.
#[derive(Default)]
struct Running {
    /// The prompts running, for `ants cancel`.
    cancels: Cancels,
    /// The prompts running that take messages joining them.
    steers: steering::Steers,
    /// The senders whose narrowing the harness, running its tools unasked, was warned not to apply.
    unnarrowed: Unnarrowed,
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
    /// The prompts running, and what the inbox keeps of them.
    running: Running,
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
    /// The colony, for the members a triage prompt shows.
    colony: &'a Colony,
    /// Who is a human, and who gets the escalations of a thread with no owner.
    humans: &'a Registry,
    /// The assistants' records, against which an `on_behalf_of` is checked with the grants of `humans`.
    behalf: Records,
    /// How often the inbox acks, retries, stores and checks.
    pacing: Pacing,
    /// What holds the messages while the ant's own harness cannot answer, and the sign-ins to tell.
    holds: holds::Holds,
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
        tokio::time::interval_at(tokio::time::Instant::now() + self.pacing.progress_every, self.pacing.progress_every)
    }

    /// Wait for `work`, telling the server meanwhile that `msg` is still being worked on.
    async fn acking<T>(&self, msg: &jetstream::Message, work: impl Future<Output = T>) -> T {
        bus::in_progress(msg, self.pacing.progress_every, work).await
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

#[cfg(test)]
mod tests;
