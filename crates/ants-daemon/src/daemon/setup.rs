//! What [`super::serve`] sets up before serving the ant's inbox: its connection and turn, its services on the bus,
//! its harness pool and the messages it serves.

use ants_proto::UserId;
use anyhow::Result;
use async_nats::jetstream;
use futures::stream::Stream;

use crate::approvals::NatsApprover;
use crate::channel::Channels;
use crate::escalation::{self, Escalator};
use crate::harness::Launch;
use crate::limits::caps::Ledger;
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
use crate::publish::{Publish, Publisher};
use crate::secrets::redact::Redactor;
use crate::stream::reading::Reading;
use crate::trail::Trails;
use crate::worktrees::Worktrees;
use crate::{bus, colony, pause};

use super::redacting::{
    RedactingNotes, redacting_notes_sender, redacting_outbox, redacting_posts, redacting_sender, redactor,
};
use super::settings::{Config, Connection, HarnessSettings, Spending, WorktreeSettings};
use super::window::Window;
use super::{ACK_WAIT, GROWN_WAIT, IN_HAND_PER_HARNESS, Served, Stop, mcp_sign_in, stopped};

/// The config with worktrees turned on or off for good, telling of a default turning them on and warning of threads
/// running at once in one git work tree. Whether threads do get a worktree each is told once the repository is looked
/// at ([`Worktrees::find`]).
pub(super) async fn settle_worktrees(config: Config) -> Config {
    let (workdir, max_harnesses, asked) =
        (&config.harness.workdir, config.pooling.max_harnesses, config.worktrees.per_thread);
    let per_thread = crate::worktrees::per_thread(workdir, max_harnesses, asked).await;
    if asked.is_none() && per_thread {
        tracing::info!(
            ant = %config.ant,
            workdir = %workdir.display(),
            max_harnesses,
            "worktrees are on, as threads may run at once in a git workdir; worktrees = false shares the workdir"
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

/// For an assistant's credentials, keeps their record for readers of what it does for its human and lets its tools
/// act for the human ([`Mcp::with_assisting`]), opening and sealing the human's sealed messages with the ant's sealing
/// key when it has one ([`crate::secrets::sealing::AssistantSealing`]).
async fn assist(mcp: &Mcp, bus: Bus<'_>) {
    let Bus { config, js, humans, .. } = bus;
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

/// Note on the sense card of the ant the prompts its identity record counts, so a record made again after a revoke
/// starts from all of them ([`crate::sense::Sense::stopping`]); a failure only warns.
pub(super) async fn growth(config: &Config, identity: Option<&Identity>, providers: &Providers) {
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
pub(super) async fn trails(bus: Bus<'_>, services: &Services) -> Option<Trails> {
    let Bus { config, client, js, humans, .. } = bus;
    let conducting = crate::trail::Conducting {
        js: js.clone(),
        js_domain: config.connection.js_domain.clone(),
        client: client.clone(),
        ant: config.ant.clone(),
        worker: std::env::current_exe().ok()?,
        trails_dir: crate::config::home_dir().ok().map(|home| home.join(".ants").join("trails")),
        humans: humans.clone(),
        colony: crate::mcp::jetstream_colony(js.clone()),
        outbox: services.outbox.clone(),
        attempts: services.attempts_kv.clone(),
        workers: config.pooling.trail_workers,
        notes: services.notes.clone(),
        // Without credentials the server is open, with no `ants signer` to answer.
        saves: match config.connection.creds {
            Some(_) => crate::trail::SaveThrough::Signer,
            None => crate::trail::SaveThrough::Bucket,
        },
        pause: config.pause.clone(),
    };
    match Trails::new(conducting).await {
        Ok(trails) => {
            trails.with_escalator(services.escalator.clone());
            trails.restore().await;
            services.mcp.with_trails(trails.clone());
            Some(trails)
        }
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "conducting no trails");
            None
        }
    }
}

/// The ant's workload, paused and drifting as the config says at start.
pub(super) fn workload(config: &Config) -> std::sync::Arc<colony::Workload> {
    let workload = std::sync::Arc::new(colony::Workload::default());
    workload.set_paused(config.pause.get());
    workload.set_drift(config.drift.get());
    workload
}

/// How long ending a retired conductor's runs may take before the retirement goes on without the rest.
const END_RUNS_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// End the runs `trails` conducts when [`super::serve`] stopped as a retirement other than `retired_before` is marked:
/// no daemon conducts them once the ant's credentials go, so each fails, its owner told why on its thread.
pub(super) async fn end_runs_if_retired(
    served: &Served,
    config: &Config,
    trails: Option<&Trails>,
    retired_before: Option<uuid::Uuid>,
) {
    let Some(trails) = trails.filter(|trails| !trails.unfinished().is_empty()) else { return };
    if *served != Served::Stopped
        || !events::retirement(&config.storage.home).await.is_some_and(|r| Some(r) != retired_before)
    {
        return;
    }
    let why = format!("its conductor {} was retired, so no daemon conducts it any more", config.ant);
    match tokio::time::timeout(END_RUNS_WAIT, trails.end_all(&why)).await {
        Ok(ended) => tracing::info!(ended, "ended the trail runs of the retired conductor"),
        Err(_) => tracing::warn!(
            wait = ?END_RUNS_WAIT,
            "ending the trail runs of the retired conductor took too long; the rest stay as they are"
        ),
    }
}

/// Delete the ant's channel and done consumers when [`super::serve`] stopped as a retirement other than `retired_before` is
/// marked.
pub(super) async fn forget_if_retired(
    served: &Served,
    config: &Config,
    js: &jetstream::Context,
    retired_before: Option<uuid::Uuid>,
) {
    if *served == Served::Stopped
        && events::retirement(&config.storage.home).await.is_some_and(|r| Some(r) != retired_before)
        && let Err(e) = bus::forget_retired(js, &config.ant).await
    {
        tracing::warn!(ant = %config.ant, error = format!("{e:#}"), "deleting consumers of the retired ant");
    }
}

/// Leave the colony and end the harness sessions once [`super::serve`] ended; on a stop, once no prompt is in flight, have the
/// ant's sense note its growth (`grown`) and tell its provider of its retirement when that stop retired it
/// ([`Lifecycle::stopped`]), before leaving the colony: a retirement revokes the ant's credentials once it left, and
/// the provider may sign what it records with them. A handover or a move leaves the growth to the daemon serving the
/// ant next, which notes it on opening the ant's sense data.
pub(super) async fn wind_down(
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
pub(super) async fn take_turn(
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

/// The daemon's ways to the bus and what it shares with every service it sets up, as [`super::serve`] has them.
#[derive(Clone, Copy)]
pub(super) struct Bus<'a> {
    pub(super) config: &'a Config,
    pub(super) client: &'a async_nats::Client,
    pub(super) js: &'a jetstream::Context,
    pub(super) humans: &'a Registry,
    /// The providers of the process, among which the ant's.
    pub(super) providers: &'a Providers,
}

/// What the ant's inbox serves with on the bus, besides its harness pool, in the order [`services`] builds it.
pub(super) struct Services {
    pub(super) redactor: std::sync::Arc<Redactor>,
    pub(super) approver: NatsApprover,
    pub(super) colony: Colony,
    /// Where the ant's MCP tools, its trails and its wakes send messages, redacted.
    pub(super) outbox: crate::mcp::Outbox,
    pub(super) reading: Reading,
    pub(super) notes: std::sync::Arc<RedactingNotes<crate::stream::notes::JetStreamNotes>>,
    pub(super) sessions_kv: crate::records::direct_kv::DirectKv,
    /// The attempts of the messages in hand, also those of the trails' steps.
    pub(super) attempts_kv: crate::records::direct_kv::DirectKv,
    pub(super) ledger: std::sync::Arc<Ledger>,
    pub(super) escalator: std::sync::Arc<Escalator>,
    pub(super) identity: Option<Identity>,
    /// Where the ant's lifecycle events go, when its provider takes them and its identity record could be read.
    pub(super) lifecycle: Option<Lifecycle>,
    pub(super) mcp: Mcp,
    /// The sign-ins to MCP servers of the harnesses' sessions.
    pub(super) mcp_sign_ins: mcp_sign_in::Untold,
}

impl Services {
    /// What the inbox takes of the ant's identity record and of its provider, `sense` when that is its provider.
    pub(super) fn provider<'a>(&'a self, sense: Option<&'a crate::sense::Sense>) -> super::Provider<'a> {
        super::Provider { identity: self.identity.as_ref(), lifecycle: self.lifecycle.as_ref(), sense }
    }
}

/// Set up the ant's redactor, approvals, notes, spending ledger, escalator and records, and start its MCP server, its
/// provider among the bus's taking tool calls naming `machine` for the machine serving the ant; with how the harnesses
/// start, with the ant's MCP server and their tool requests going to its approver, and the sign-ins to MCP servers of
/// their sessions going to its `mcp_sign_ins`.
pub(super) async fn services(bus: Bus<'_>, allow: Allowlist, machine: &str) -> Result<(Services, Launch)> {
    let Bus { config, js, humans, providers, .. } = bus;
    let redactor = std::sync::Arc::new(redactor(config.policy.redact, &config.harness));
    let colony = crate::mcp::jetstream_colony(js.clone());
    let account = crate::people::humans::account(config.connection.creds.as_deref());
    let records = Records::new(js.clone(), config.connection.js_domain.clone(), account);
    let assistants = AssistantFirst::with_records(humans.clone(), colony.clone(), records);
    let approver = approver(bus).with_assistants(assistants.clone());
    let policy =
        PermissionPolicy::new(allow, Some(std::sync::Arc::new(approver.clone()))).with_redactor(redactor.clone());
    let outbox = redacting_outbox(crate::mcp::jetstream_outbox(js.clone()), redactor.clone());
    let reading = Reading::daemon(crate::secrets::creds::is_assistant(config.connection.creds.as_deref()));
    let notes = crate::stream::notes::JetStreamNotes::new(js.clone(), config.ant.clone()).reading(reading);
    let notes = std::sync::Arc::new(RedactingNotes { notes, redactor: redactor.clone() });
    let sessions_kv = bus::ensure_sessions(js, config.connection.js_domain.as_ref(), &config.ant).await?;
    let attempts_kv = bus::ensure_attempts(js, config.connection.js_domain.as_ref(), &config.ant).await?;
    let ledger = ledger(&config.ant, &config.spending, &sessions_kv);
    let escalator = escalator(bus, &colony, &ledger, &redactor, assistants);
    let (identity, identity_id) = ant_records(config, js).await;
    let sense = providers.sense_of(&config.provider);
    if let Some(sense) = sense {
        sense.serve(&config.ant, &config.storage.home, config.connection.creds.clone());
        sense.read_times_in(&config.ant, config.timezone.as_ref());
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
        outbox.clone(),
        colony.clone(),
        notes.clone(),
        humans.clone(),
        Some(escalator.clone()),
    )
    .await?;
    connect_mcp(&mut mcp, bus, &redactor).await?;
    if let Some(sense) = sense {
        mcp.with_dreams(sense.dreams());
        mcp.with_triggers(crate::sense::TriggerCommands::of(config));
    }
    mcp.with_origins(crate::provider::origins::OriginStore::kv(sessions_kv.clone(), config.ant.clone()));
    if let Some(identity) = &identity {
        mcp.with_callers(Callers::new(config.ant.clone(), machine.to_owned(), identity.clone()));
    }
    let mcp_sign_ins = mcp_sign_in::Untold::default();
    let mut launch = launch(&config.harness, policy, &mcp).with_mcp_sign_ins(mcp_sign_ins.sink());
    if let Some(sense) = sense {
        launch = launch.with_skills(sense.skills(&config.storage.home));
    }
    let services = Services {
        redactor,
        approver,
        colony,
        outbox,
        reading,
        notes,
        sessions_kv,
        attempts_kv,
        ledger,
        escalator,
        identity,
        lifecycle,
        mcp,
        mcp_sign_ins,
    };
    Ok((services, launch))
}

/// Wait while the ant's identity record is marked moved, so no daemon starts a moved ant: false when `stop` comes
/// first. A record that cannot be read does not keep the ant from being served.
pub(super) async fn unsealed(config: &Config, js: &jetstream::Context, stop: &Stop) -> bool {
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
fn approver(bus: Bus<'_>) -> NatsApprover {
    let Bus { config, client, js, humans, .. } = bus;
    let settings = &config.policy;
    NatsApprover::new(client.clone(), js.clone(), config.ant.clone(), settings.approval_timeout, humans.clone())
        .with_chain(settings.escalation.clone())
        .with_teams(crate::escalation::jetstream_teams(js.clone(), config.connection.js_domain.clone()))
}

/// Connect to the bus as the ant's daemon, retrying until it answers: `None` when `stop` comes first.
pub(super) async fn connect(
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
    bus: Bus<'_>,
    colony: &Colony,
    ledger: &std::sync::Arc<Ledger>,
    redactor: &std::sync::Arc<Redactor>,
    assistants: AssistantFirst,
) -> std::sync::Arc<Escalator> {
    let Bus { config, js, humans, .. } = bus;
    let escalator = Escalator::new(
        config.ant.clone(),
        config.policy.escalation.clone(),
        colony.clone(),
        humans.clone(),
        ledger.clone(),
        redacting_sender(escalation::jetstream_sender(js.clone()), redactor.clone()),
    )
    .with_chains(crate::records::chains::jetstream_chains(js.clone()))
    .with_teams(escalation::jetstream_teams(js.clone(), config.connection.js_domain.clone()))
    .with_notes(redacting_notes_sender(escalation::jetstream_notes_sender(js.clone()), redactor.clone()))
    .with_heed(crate::records::heed::jetstream_heed(js.clone(), config.connection.js_domain.clone()))
    .with_assistants(assistants);
    std::sync::Arc::new(escalator)
}

/// Give the ant's MCP server its live inbox, its ways to the bus and its provider among `providers`, and an
/// assistant's the human it acts for.
async fn connect_mcp(mcp: &mut Mcp, bus: Bus<'_>, redactor: &std::sync::Arc<Redactor>) -> Result<()> {
    let Bus { config, client, js, providers, .. } = bus;
    mcp.listen(bus::live_inbox(client, &config.ant).await?);
    let channels = crate::mcp::jetstream_channels(js.clone(), config.connection.js_domain.clone());
    mcp.with_channels(redacting_posts(channels, redactor.clone()));
    mcp.with_lookup(crate::mcp::jetstream_lookup(js.clone()));
    mcp.with_files(crate::mcp::jetstream_files(js.clone()), config.harness.workdir.clone());
    mcp.with_redactor(redactor.clone());
    mcp.with_max_payload(bus::max_payload(client));
    // Without credentials the server is open, with no `ants signer` to answer, and the daemon reads what it would.
    mcp.with_reaction_asker(match config.connection.creds {
        Some(_) => crate::mcp::nats_reaction_asker(client.clone()),
        None => crate::mcp::local_reaction_asker(js.clone(), config.connection.js_domain.clone()),
    });
    if let Some(requester) = &config.requester.0 {
        mcp.with_requester(requester.clone());
    }
    mcp.manage_channels(crate::chanadm::nats_admin(client.clone(), js.clone()));
    teams(mcp, bus).await;
    providers.assign(&config.ant, &config.provider);
    if let Some(provider) = providers.of(&config.ant).await {
        mcp.with_provider(provider);
    }
    assist(mcp, bus).await;
    Ok(())
}

/// Serve the team tools on `mcp`, changing teams through `ants signer` when the daemon connected with credentials, else
/// as it would; with a warning, none when the team directory cannot be named.
async fn teams(mcp: &Mcp, bus: Bus<'_>) {
    let Bus { config, client, js, .. } = bus;
    let domain = config.connection.js_domain.as_ref();
    let kv = match crate::teams::open(js, domain).await {
        Ok(kv) => kv,
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "serving no team tools");
            return;
        }
    };
    let signer = config.connection.creds.is_some();
    let (identities, humans) = if signer {
        (None, None)
    } else {
        use crate::records::direct_kv::DirectKv;
        let identities = DirectKv::whole(js, domain, ants_proto::IDENTITIES_BUCKET).await.ok();
        (identities, DirectKv::new(js, domain, ants_proto::HUMANS_BUCKET).await.ok())
    };
    let changer = crate::teams::keeper::Changer { client: client.clone(), kv, identities, humans, signer };
    mcp.with_teams(crate::mcp::nats_teams(changer, config.ant.clone()));
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

pub(super) fn storage_watcher(
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

/// The marks of the deliveries of the messages of `ant`, its attempts in `attempts`, those of the messages answered
/// and buried pruned of what the stream no longer keeps.
pub(super) async fn delivery_marks(
    attempts: crate::records::direct_kv::DirectKv,
    ant: &UserId,
    home: &std::path::Path,
) -> super::Marks {
    let marks = super::Marks::new(attempts, ant, home);
    marks.answered.prune(bus::RETENTION).await;
    marks.buried.prune(bus::RETENTION).await;
    marks
}

/// The ant's harness pool, isolating threads in worktrees when asked to, or else cleaning up the ones left behind, and
/// reviews among their checkouts, cleared of those an earlier run left.
pub(super) async fn harness_pool(
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
pub(super) async fn publisher(
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

/// The messages to the ant, from its inbox and the posts to its channels, the channels, and the window of their
/// consumers ([`Window`]); the inbox alone when the channels fail. While the ant is paused ([`Config::pause`]) the
/// consumers are not pulled from ([`pause::pausable`]).
pub(super) async fn inbox_messages(
    js: &jetstream::Context,
    stream: &jetstream::stream::Stream<()>,
    config: &Config,
) -> Result<(impl Stream<Item = Result<jetstream::Message, anyhow::Error>> + Unpin, Channels, std::sync::Arc<Window>)> {
    let in_hand = i64::try_from(config.pooling.max_harnesses.max(1) * IN_HAND_PER_HARNESS)?;
    let window = std::sync::Arc::new(Window::new(stream.clone(), config.ant.clone(), ACK_WAIT, in_hand));
    let channels = match async { Channels::watch(bus::ensure_channels(js).await?).await }.await {
        Ok(channels) => Some(channels),
        Err(e) => {
            tracing::warn!(error = format!("{e:#}"), "serving the inbox without channels");
            None
        }
    };
    let posts = channels.is_some();
    let paused = config.pause.subscribe();
    // Not across the opening: a reload replacing the pause meanwhile would block the daemon's thread on its lock.
    let is_paused = paused.borrow().is_some();
    let current = if is_paused { None } else { Some(window.open(posts).await?) };
    let opening = window.clone();
    let messages = pause::pausable(paused, current, async move || opening.open(posts).await);
    Ok((Box::pin(messages), channels.unwrap_or_default(), window))
}

/// Put the ant on the colony, holding its entry from the revision claimed, if any, its first entry showing the profile
/// `sense`, its provider if sense, holds of it. With `open`, a server taking no credentials, its heartbeats create the
/// buckets held in memory again when nats-server lost them ([`colony::Recreate`]). Its heartbeats tell the process's
/// outage watch whether JetStream answers through the server at `nats_url` ([`bus::outage`]).
pub(super) async fn enter_colony(
    js: &jetstream::Context,
    nats_url: &str,
    entry: ants_proto::ColonyEntry,
    claimed: Option<u64>,
    workload: &std::sync::Arc<colony::Workload>,
    sense: Option<&crate::sense::Sense>,
    open: bool,
) -> Result<colony::Registration> {
    if let Some(sense) = sense {
        sense.first_profile(&entry.ant, workload).await;
    }
    let answers = bus::outage::Reporter::jetstream(nats_url);
    Ok(match claimed {
        Some(revision) => colony::hold(js, entry, revision, workload.clone(), open, answers).await?,
        None => colony::register(js, entry, workload.clone(), open, answers).await?,
    })
}
