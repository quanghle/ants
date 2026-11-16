//! `ants daemon`: which ants to serve, from the config file and the command line, and serving them together.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ants_daemon::admin::restart::Restarts;
use ants_daemon::admin::{ConfigFile, LeafNode};
use ants_daemon::config::{self, Ant, HostSettings, Summary};
use ants_daemon::host::{self, Host};
use ants_daemon::schedule::Schedule;
use ants_daemon::shared::{self, Drift, Shared, Via};
use ants_daemon::{Config, Harnesses, Stop};
use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use futures::stream::{FuturesUnordered, StreamExt as _};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::Instrument as _;

use crate::reload::Triggers;
use crate::scheduler;

#[derive(clap::Args, Debug)]
pub struct DaemonArgs {
    /// Serve only this ant: picked from the config file when one is given, described by the flags below otherwise.
    #[arg(long, env = "ANTS_ANT")]
    ant: Option<UserId>,
    /// Config file listing the ants to serve (default without --ant: ~/.ants/config.toml, if it exists).
    #[arg(long, env = "ANTS_CONFIG")]
    config: Option<PathBuf>,
    /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464, in
    /// place of the config file's `metrics` (default: off).
    #[arg(long, env = "ANTS_METRICS", value_name = "ADDR")]
    metrics: Option<SocketAddr>,
    #[command(flatten)]
    options: AntOptions,
    #[command(flatten)]
    pub upgrade: crate::upgrade::UpgradeArgs,
}

/// The settings of an ant on the command line, of `ants daemon` and `ants add`.
#[derive(clap::Args, Debug, Default)]
pub struct AntOptions {
    /// Directory the harness works in (default: the current directory).
    #[arg(long)]
    workdir: Option<PathBuf>,
    /// The ant's own files (default: ~/.ants/<ant>).
    #[arg(long)]
    home: Option<PathBuf>,
    /// Role instructions shared in the roster and prepended to every prompt.
    #[arg(long)]
    whoami: Option<String>,
    /// Instructions for the memory tool available through the harness; prepended to every prompt.
    #[arg(long)]
    memory: Option<String>,
    /// Instructions for the task tool available through the harness; prepended to every prompt.
    #[arg(long)]
    tasks: Option<String>,
    /// Add NAME=VALUE to the harness environment (repeatable); overrides the file's entry for NAME.
    #[arg(long = "env", value_name = "NAME=VALUE", value_parser = env_pair)]
    env: Vec<(String, String)>,
    /// Agent harness: built in (`copilot`, `claude`, `codex`, `antigravity`) or a name the config file defines under
    /// `[harnesses.NAME]` (default: the config file's top-level `harness`; with none, required).
    #[arg(long)]
    harness: Option<String>,
    /// Model ID advertised by the harness. Omitted: keep the harness default.
    #[arg(long)]
    model: Option<String>,
    /// Reasoning effort value advertised by the selected model.
    #[arg(long)]
    reasoning_effort: Option<String>,
    /// Provider-native context: Copilot default/long_context, Claude default/1m.
    #[arg(long)]
    context: Option<String>,
    /// Let the harness run tool requests matching RULE without asking: `*`, a tool kind such as `read`, or
    /// `<kind>:<pattern>` such as `execute:cargo test*` or `edit:src/*`. Repeatable, added to the file's `allow`.
    /// Other requests go to the human.
    #[arg(long = "allow", value_name = "RULE")]
    allow: Vec<String>,
    /// Allow every tool request (same as `--allow '*'`). Only for trusted, sandboxed workdirs.
    #[arg(long)]
    allow_tools: bool,
    /// How long a tool request outside the allowlist waits for the human's decision before it is rejected (a
    /// duration such as 90s, 10m or 1h; default: 5m).
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
    approval_timeout: Option<Duration>,
    /// Most harness processes alive at once, one per active thread (default: 4).
    #[arg(long)]
    max_harnesses: Option<usize>,
    /// Give each thread its own git worktree of the workdir's repository, under the ant's home, so threads running
    /// at once do not share a checkout. A workdir outside a git repository stays shared. Default: on for a git
    /// workdir with max_harnesses above 1.
    #[arg(long)]
    worktrees: bool,
    /// Have all threads share the workdir, also a git workdir with max_harnesses above 1.
    #[arg(long, conflicts_with = "worktrees")]
    no_worktrees: bool,
    /// How long a thread's harness stays without a message before it stops; its session resumes on the next message
    /// (a duration such as 90s, 10m or 1h; default: 10m).
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
    idle_timeout: Option<Duration>,
    /// How long a prompt may run, tool requests waiting for the human included, before it is cancelled and the thread
    /// goes to the human (a duration such as 90s, 10m or 1h; 0 for no limit; default: 30m).
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
    prompt_timeout: Option<Duration>,
    /// Tool calls a prompt may start before it is cancelled and the thread goes to the human; 0 for no limit
    /// (default: 300).
    #[arg(long)]
    max_tool_calls: Option<usize>,
    /// Tokens a thread may spend between messages from the human, as the harness reports them (default: no cap).
    #[arg(long)]
    max_thread_tokens: Option<u64>,
    /// Cost a thread may spend between messages from the human, in the harness's unit such as USD (default: no cap).
    #[arg(long)]
    max_thread_cost: Option<f64>,
    /// Tokens the ant may spend in a UTC day (default: no cap).
    #[arg(long)]
    max_daily_tokens: Option<u64>,
    /// Cost the ant may spend in a UTC day (default: no cap).
    #[arg(long)]
    max_daily_cost: Option<f64>,
    /// Percent of a spend cap past which the thread's owner, else the default human, is warned once (default: 80;
    /// 0 for never).
    #[arg(long, value_name = "PERCENT")]
    spend_warn_percent: Option<u8>,
    /// Tag the ant is good for, listed in its roster entry for routing (repeatable, or comma-separated). Replaces
    /// the file's `capabilities`.
    #[arg(long = "capability", value_name = "TAG", value_delimiter = ',')]
    capabilities: Vec<String>,
}

impl AntOptions {
    /// Whether any setting is given.
    pub fn sets_any(&self) -> bool {
        self.workdir.is_some()
            || self.home.is_some()
            || self.whoami.is_some()
            || self.memory.is_some()
            || self.tasks.is_some()
            || !self.env.is_empty()
            || self.harness.is_some()
            || self.model.is_some()
            || self.reasoning_effort.is_some()
            || self.context.is_some()
            || !self.allow.is_empty()
            || self.allow_tools
            || self.approval_timeout.is_some()
            || self.max_harnesses.is_some()
            || self.worktrees
            || self.no_worktrees
            || self.idle_timeout.is_some()
            || self.prompt_timeout.is_some()
            || self.max_tool_calls.is_some()
            || self.max_thread_tokens.is_some()
            || self.max_thread_cost.is_some()
            || self.max_daily_tokens.is_some()
            || self.max_daily_cost.is_some()
            || self.spend_warn_percent.is_some()
            || !self.capabilities.is_empty()
    }

    /// The ant with the flags given on the command line in place of its own settings.
    pub fn apply(&self, mut p: Ant) -> Ant {
        if let Some(dir) = &self.workdir {
            p.workdir.clone_from(dir);
        }
        if let Some(home) = &self.home {
            p.home = Some(home.clone());
        }
        if let Some(whoami) = &self.whoami {
            p.whoami = Some(whoami.clone());
        }
        if let Some(memory) = &self.memory {
            p.memory = Some(memory.clone());
        }
        if let Some(tasks) = &self.tasks {
            p.tasks = Some(tasks.clone());
        }
        p.env.extend(self.env.iter().cloned());
        if let Some(harness) = &self.harness {
            p.harness = Some(harness.clone());
        }
        if let Some(model) = &self.model {
            p.model = Some(model.clone());
        }
        if let Some(effort) = &self.reasoning_effort {
            p.reasoning_effort = Some(effort.clone());
        }
        if let Some(context) = &self.context {
            p.context = Some(context.clone());
        }
        p.allow.extend(self.allow.iter().cloned());
        p.allow_tools |= self.allow_tools;
        if let Some(timeout) = self.approval_timeout {
            p.approval_timeout = timeout;
        }
        if let Some(n) = self.max_harnesses {
            p.max_harnesses = n;
        }
        if self.worktrees || self.no_worktrees {
            p.worktrees = Some(self.worktrees);
        }
        if let Some(timeout) = self.idle_timeout {
            p.idle_timeout = timeout;
        }
        if let Some(timeout) = self.prompt_timeout {
            p.prompt_timeout = timeout;
        }
        if let Some(n) = self.max_tool_calls {
            p.max_tool_calls = n;
        }
        p.max_thread_tokens = self.max_thread_tokens.or(p.max_thread_tokens);
        p.max_thread_cost = self.max_thread_cost.or(p.max_thread_cost);
        p.max_daily_tokens = self.max_daily_tokens.or(p.max_daily_tokens);
        p.max_daily_cost = self.max_daily_cost.or(p.max_daily_cost);
        if let Some(percent) = self.spend_warn_percent {
            p.spend_warn_percent = percent;
        }
        if !self.capabilities.is_empty() {
            p.capabilities.clone_from(&self.capabilities);
        }
        p
    }
}

fn env_pair(text: &str) -> Result<(String, String), String> {
    let (name, value) = text.split_once('=').ok_or("expected NAME=VALUE")?;
    if name.is_empty() || name.contains('\0') || value.contains('\0') {
        return Err("invalid environment variable".to_owned());
    }
    Ok((name.to_owned(), value.to_owned()))
}

#[derive(Debug, PartialEq)]
enum Source {
    Flags(UserId),
    File(PathBuf),
}

/// `--config` (or `ANTS_CONFIG`) names the file; otherwise `--ant` alone means the flags describe it, and with
/// neither the file at `default_path` is used.
fn source(args: &DaemonArgs, default_path: &Path) -> Result<Source> {
    Ok(match (&args.config, &args.ant) {
        (Some(path), _) => Source::File(path.clone()),
        (None, Some(ant)) => Source::Flags(ant.clone()),
        (None, None) if default_path.exists() => Source::File(default_path.to_owned()),
        (None, None) => bail!("no --ant given and no config file at {}", default_path.display()),
    })
}

/// The file a reload reads: the config file, or for an ant described by flags the file at `default_path`, for its
/// harnesses.
pub fn config_file(args: &DaemonArgs, default_path: &Path) -> Result<PathBuf> {
    Ok(match source(args, default_path)? {
        Source::Flags(_) => default_path.to_owned(),
        Source::File(path) => path,
    })
}

/// What `ants daemon` is to serve, as listed.
#[derive(Debug)]
pub struct Listed {
    pub ants: Vec<Ant>,
    /// The harnesses the ants may name.
    pub harnesses: Harnesses,
    pub schedules: Vec<Schedule>,
    /// Where to serve health and metrics: `--metrics`, else the file's `metrics`.
    pub metrics: Option<SocketAddr>,
    /// The ants that get `request_ant`, managers of the file's templates, and the file they add ants to: only when
    /// the daemon serves the whole file, which the ants added join.
    pub managers: Option<(Vec<UserId>, PathBuf)>,
    /// The machine's host connection the file describes: only when the daemon serves the whole file.
    pub host: Option<HostSettings>,
}

/// The ants to serve, the harnesses they may name and the schedules to run: those of the config file, or for an ant
/// described by flags the harnesses of the file at `default_path` if it exists, and its top-level `harness` unless
/// the flags name one. The schedules run only when the
/// daemon serves the whole file, not one ant picked with `--ant`.
pub fn ants(args: &DaemonArgs, default_path: &Path) -> Result<Listed> {
    match source(args, default_path)? {
        Source::Flags(name) => {
            let mut ant = args.options.apply(Ant::new(name, std::env::current_dir()?));
            let summary = Summary::load(default_path)?;
            if ant.harness.is_none() {
                ant.harness = summary.harness;
            }
            let harnesses = summary.harnesses;
            let (schedules, metrics) = (Vec::new(), args.metrics);
            Ok(Listed { ants: vec![ant], harnesses, schedules, metrics, managers: None, host: None })
        }
        Source::File(path) => {
            let loaded = config::load(&path)?;
            let whole = args.ant.is_none();
            let schedules = if whole { loaded.schedules } else { Vec::new() };
            let metrics = args.metrics.or(loaded.metrics);
            let managers = loaded
                .ants
                .iter()
                .filter(|a| crate::manager::manages(&loaded.templates, &a.name, a.template.is_some()))
                .map(|a| a.name.clone())
                .collect();
            let managers = whole.then(|| (managers, path.clone()));
            let host = whole.then_some(loaded.host);
            let ants = pick(loaded.ants, args, &path)?;
            Ok(Listed { ants, harnesses: loaded.harnesses, schedules, metrics, managers, host })
        }
    }
}

fn pick(all: Vec<Ant>, args: &DaemonArgs, path: &Path) -> Result<Vec<Ant>> {
    match &args.ant {
        Some(name) => {
            let p = all
                .into_iter()
                .find(|p| &p.name == name)
                .with_context(|| format!("ant {name} is not in {}", path.display()))?;
            if let Some(template) = p.template.as_ref().filter(|_| args.options.sets_any()) {
                bail!(
                    "ant {name} was added from template {template}, which bounds its settings: change its entry in {} \
                     instead of passing ant flags such as --workdir",
                    path.display()
                );
            }
            Ok(vec![args.options.apply(p)])
        }
        None if args.options.sets_any() => {
            bail!("ant flags such as --workdir apply to one ant; add --ant to pick it from {}", path.display())
        }
        None => Ok(all),
    }
}

/// Daemon configs, with their harnesses from `harnesses`, `nats` as the server, `js_domain` as the JetStream domain
/// and, for a lone ant, `creds` as the credentials where the ant sets none. Several ants cannot share one set of
/// credentials, so `creds` is ignored for them. An ant with settings in `shared` is served with those in place of its
/// table's ([`Shared::overlaid`]), and told the fields its table sets otherwise (its drift); shared settings this
/// machine cannot serve, such as a harness it lacks, are warned of and its table's serve it.
pub fn configs(
    ants: Vec<Ant>,
    harnesses: &Harnesses,
    nats: &str,
    creds: Option<PathBuf>,
    js_domain: Option<JsDomain>,
    shared: Option<&Shared>,
) -> Result<Vec<Config>> {
    let lone = ants.len() == 1;
    if !lone && creds.is_some() {
        tracing::warn!("ignoring --creds (ANTS_CREDS) while serving several ants; set creds per ant instead");
    }
    ants.into_iter()
        .map(|mut p| {
            if lone && p.creds.is_none() {
                p.creds.clone_from(&creds);
            }
            if p.js_domain.is_none() {
                p.js_domain.clone_from(&js_domain);
            }
            if let Some((overlaid, drift)) = shared.and_then(|shared| shared.overlaid(&p)) {
                match overlaid.into_config(nats, harnesses) {
                    Ok(config) => return Ok(Config { drift: Drift::new(drift), ..config }),
                    Err(e) => tracing::warn!(
                        ant = %p.name, error = format!("{e:#}"),
                        "the ant's shared settings do not suit this machine; serving it with its config's"
                    ),
                }
            }
            p.into_config(nats, harnesses)
        })
        .collect()
}

/// What `ants daemon` serves: its ants' configs and its schedules, ready to run.
pub struct Served {
    pub configs: Vec<Config>,
    pub schedules: Vec<Schedule>,
    pub metrics: Option<SocketAddr>,
    /// The machine's host connection, with the ants to record as the machine's ([`host`]).
    pub host: Option<(Host, Vec<UserId>)>,
    /// The shared settings `configs` were overlaid with: kept current by the host connection, else, without one, by
    /// reading them as the ants ([`shared::follow`]).
    pub shared: Option<Shared>,
}

impl Served {
    /// Where to read the shared settings of the ants served without a host connection: each server the ants connect
    /// to, as the first of them there. None with a host connection, which reads them, or no shared settings to keep.
    fn vias(&self) -> Option<Vec<Via>> {
        if self.host.is_some() || self.shared.is_none() {
            return None;
        }
        let mut vias: Vec<Via> = Vec::new();
        for config in self.configs.iter().filter(|c| c.ant != UserId::messenger()) {
            match vias.iter_mut().find(|v| v.nats == config.nats_url && v.js_domain == config.js_domain) {
                Some(via) => via.ants.push(config.ant.clone()),
                None => vias.push(Via {
                    nats: config.nats_url.clone(),
                    creds: config.creds.clone(),
                    js_domain: config.js_domain.clone(),
                    user: config.ant.clone(),
                    ants: vec![config.ant.clone()],
                }),
            }
        }
        Some(vias)
    }
}

/// The host connection of a daemon serving `configs` as `settings` describe it, `nats` and `js_domain` standing for
/// what the file does not set ([`Host::new`]), and admin orders editing the file `config`.
pub fn host(
    settings: Option<HostSettings>,
    configs: &[Config],
    nats: &str,
    js_domain: Option<&JsDomain>,
    config: Option<ConfigFile>,
) -> Option<(Host, Vec<UserId>)> {
    let HostSettings { creds, machine, nats: file_nats, js_domain: file_domain, remote_admin } = settings?;
    let nats = file_nats.unwrap_or_else(|| nats.to_owned());
    let mut host =
        Host::new(machine, nats, creds, file_domain.or(js_domain.cloned()), configs.iter().map(|c| c.creds.is_some()))?;
    host.remote_admin = remote_admin;
    host.config = config;
    Some((host, configs.iter().map(|c| c.ant.clone()).collect()))
}

/// How long [`run`] waits at start for the first read of the shared settings, by the host connection or without one,
/// to serve the ants with them rather than restart them once read.
const SHARED_FIRST_READ: Duration = Duration::from_secs(5);

/// The machine's leaf node, at ~/.config/ants/leaf.conf, whose uplink an add order renews to carry the ant added
/// ([`crate::add::renew_and_reconnect`]); none without a home directory.
pub fn leaf_node() -> Option<LeafNode> {
    let conf = crate::auth::default_dir().ok()?.with_file_name("leaf.conf");
    Some(LeafNode {
        conf,
        uplink: crate::leaf::read_uplink,
        renew: crate::add::renew_and_reconnect,
        shrink: |conf, asked, shrunk| crate::add::shrink_and_reconnect(conf, Some(asked), shrunk),
    })
}

/// How [`run`] ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    Stopped,
    /// The executable was replaced and every ant stopped, for the new one to serve them.
    Replaced,
}

/// Serve every ant until Ctrl-C or SIGTERM, each over its own NATS connection, and run the schedules alongside, each
/// over its own as well. An ant that fails is reported and the others keep running; it is an error only when none is
/// left. On SIGHUP, once the config file changed and settled, or at once after an admin order edited it (`triggers`),
/// `reload` lists the ants and schedules
/// again and the daemon serves those ([`reconcile`], [`Schedules::reconcile`]); a list it cannot read is reported and
/// what runs stays as it is. Health and metrics are
/// served on `served.metrics`, if given, until the daemon stops; a reload does not move them. Once `replaced` the ants
/// stop as on SIGTERM, finishing their prompts in flight, and the schedules at once.
pub async fn run(
    served: Served,
    reload: impl Fn() -> Result<Served>,
    mut triggers: Triggers,
    replaced: impl Future<Output = ()>,
) -> Result<Ended> {
    let metrics = served.metrics;
    let _metrics = crate::serve_metrics(metrics, Arc::new(ants_daemon::metrics::Daemon)).await?;
    let mut running = HashMap::new();
    // The ants served, which the host connection's admin orders restart.
    let restarts = Restarts::default();
    let serve = |ant| serve(ant, restarts.clone());
    let restarting = |wanted: Option<(Host, Vec<UserId>)>| {
        wanted.map(|(host, ants)| (Host { restarts: restarts.clone(), ..host }, ants))
    };
    let mut links = Links::default();
    links.reconcile(restarting(served.host.clone()), Some(&served));
    let served = match served.shared.clone() {
        Some(shared) if shared.wait_polled(SHARED_FIRST_READ).await => match reload() {
            Ok(again) => {
                links.reconcile(restarting(again.host.clone()), Some(&again));
                again
            }
            Err(e) => {
                tracing::error!(
                    error = format!("{e:#}"),
                    "reload with the shared settings failed; serving the config's"
                );
                served
            }
        },
        Some(_) => {
            tracing::warn!("the shared settings could not be read yet; serving the config's until they are");
            served
        }
        None => served,
    };
    let mut tasks: FuturesUnordered<_> = reconcile(&mut running, served.configs).into_iter().map(serve).collect();
    let mut schedules = Schedules::default();
    schedules.reconcile(served.schedules, run_schedule);
    let shutdown = async {
        if let Err(e) = ants_daemon::daemon::stop_signal().await {
            tracing::warn!(error = %e, "waiting for Ctrl-C or SIGTERM");
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(shutdown, replaced);
    let (mut stopping, mut served, mut errors, mut ended) = (false, 0, Vec::new(), Ended::Stopped);
    while !tasks.is_empty() {
        tokio::select! {
            Some((ant, result)) = tasks.next() => {
                match result {
                    Ok(()) => served += 1,
                    Err(e) => {
                        tracing::error!(%ant, error = format!("{e:#}"), "ant stopped");
                        errors.push(e);
                    }
                }
                let next = running.remove(&ant).and_then(|r| r.next);
                if let Some(config) = next.filter(|_| !stopping) {
                    tracing::info!(%ant, "serving the ant again with its listed entry");
                    tasks.push(serve(Running::insert(&mut running, config)));
                }
            }
            () = &mut shutdown, if !stopping => {
                stopping = true;
                schedules.reconcile(Vec::new(), run_schedule);
                links.reconcile(None, None);
            }
            () = &mut replaced, if !stopping => {
                tracing::info!("stopping every ant as on SIGTERM, finishing the prompts in flight");
                (stopping, ended) = (true, Ended::Replaced);
                schedules.reconcile(Vec::new(), run_schedule);
                links.reconcile(None, None);
                for r in running.values_mut() {
                    r.next = None;
                    r.ask_to_stop();
                }
            }
            trigger = triggers.next(), if !stopping => match reload() {
                Ok(served) => {
                    tracing::info!(on = trigger.describe(), "config reloaded");
                    if served.metrics != metrics {
                        tracing::warn!("the metrics address changed; it takes effect on a restart");
                    }
                    links.reconcile(restarting(served.host.clone()), Some(&served));
                    tasks.extend(reconcile(&mut running, served.configs).into_iter().map(serve));
                    schedules.reconcile(served.schedules, run_schedule);
                }
                Err(e) => tracing::error!(
                    on = trigger.describe(),
                    error = format!("{e:#}"),
                    "reload failed; serving the ants as before"
                ),
            },
        }
    }
    match (ended, served, errors.len()) {
        (Ended::Replaced, _, _) => Ok(Ended::Replaced),
        (_, 0, 1) => Err(errors.remove(0)),
        (_, 0, n) if n > 0 => bail!("none of the {n} ants could be served"),
        (ended, _, _) => Ok(ended),
    }
}

/// Serve one ant until it stops, reporting which, restarting its harnesses on the admin orders `restarts` passes on.
fn serve((config, stop): (Config, Stop), restarts: Restarts) -> impl Future<Output = (UserId, Result<()>)> {
    let ant = config.ant.clone();
    let span = tracing::info_span!("ant", name = %ant);
    async move { (ant, ants_daemon::run_until(config, &stop, &restarts).await) }.instrument(span)
}

/// Run `schedule` in a task of its own until aborted.
fn run_schedule(schedule: Schedule) -> JoinHandle<()> {
    let name = schedule.name.clone();
    let span = tracing::info_span!("schedule", name = %name);
    tokio::spawn(
        async move {
            if let Err(e) = scheduler::run(schedule).await {
                tracing::error!(schedule = %name, error = format!("{e:#}"), "schedule stopped");
            }
        }
        .instrument(span),
    )
}

/// The schedules running, each in its task, which stops with them.
#[derive(Default)]
struct Schedules(HashMap<UserId, ScheduleRun>);

struct ScheduleRun {
    schedule: Schedule,
    /// The contents of its credentials file when it started ([`creds_snapshot`]).
    creds: Option<Vec<u8>>,
    task: JoinHandle<()>,
}

impl Schedules {
    /// Run the schedules `wanted`, started with `spawn`. One no longer listed stops at once, between firings or in
    /// the middle of one, which another daemon or a restart sends as it was not marked sent; one whose entry or
    /// credentials file changed, or which stopped, starts again; the others carry on untouched.
    fn reconcile(&mut self, wanted: Vec<Schedule>, spawn: impl Fn(Schedule) -> JoinHandle<()>) {
        let listed: HashSet<UserId> = wanted.iter().map(|s| s.name.clone()).collect();
        for (name, ScheduleRun { task, .. }) in self.0.extract_if(|name, _| !listed.contains(name)) {
            tracing::info!(schedule = %name, "stopping the schedule no longer listed");
            task.abort();
        }
        for schedule in wanted {
            let name = schedule.name.clone();
            let creds = creds_snapshot(schedule.creds.as_deref());
            match self.0.get(&name) {
                Some(run) if run.schedule == schedule && run.creds == creds && !run.task.is_finished() => continue,
                Some(_) => tracing::info!(
                    schedule = %name,
                    "restarting the schedule: its entry or credentials changed, or it stopped"
                ),
                None => tracing::info!(schedule = %name, "starting the schedule"),
            }
            let task = spawn(schedule.clone());
            if let Some(old) = self.0.insert(name, ScheduleRun { schedule, creds, task }) {
                old.task.abort();
            }
        }
    }
}

impl Drop for Schedules {
    fn drop(&mut self) {
        for run in self.0.values() {
            run.task.abort();
        }
    }
}

/// The machine's links to the bus besides its ants' own: its host connection, or else the reading of the shared
/// settings without one.
#[derive(Default)]
struct Links {
    host: HostRun,
    follower: Follower,
}

impl Links {
    /// Run the host connection `host` and the reading of the shared settings `served` lists; with neither, both stop.
    fn reconcile(&mut self, host: Option<(Host, Vec<UserId>)>, served: Option<&Served>) {
        self.host.reconcile(host);
        self.follower.reconcile(served.and_then(Served::vias), served.and_then(|s| s.shared.as_ref()));
    }
}

/// The machine's host connection, if any, in its task.
#[derive(Default)]
struct HostRun(Option<HostTask>);

struct HostTask {
    host: Host,
    /// The contents of its credentials file when it started ([`creds_snapshot`]).
    creds: Option<Vec<u8>>,
    ants: watch::Sender<Vec<UserId>>,
    task: JoinHandle<()>,
}

impl HostRun {
    /// Run the host connection `wanted` recording its ants: the one running is told the ants unless it changed, its
    /// credentials file changed or it stopped, when it starts again; with none wanted, it stops.
    fn reconcile(&mut self, wanted: Option<(Host, Vec<UserId>)>) {
        let Some((wanted, ants)) = wanted else {
            if let Some(old) = self.0.take() {
                tracing::info!("stopping the host connection");
                old.task.abort();
            }
            return;
        };
        let creds = creds_snapshot(wanted.creds.as_deref());
        if let Some(run) = self.0.as_ref().filter(|r| r.host == wanted && r.creds == creds && !r.task.is_finished()) {
            run.ants.send_replace(ants);
            return;
        }
        let (sender, receiver) = watch::channel(ants);
        let started = wanted.clone();
        let task = tokio::spawn(
            async move {
                if let Err(e) = Box::pin(host::run(started, receiver)).await {
                    tracing::error!(error = format!("{e:#}"), "host connection stopped");
                }
            }
            .instrument(tracing::info_span!("host")),
        );
        if let Some(old) = self.0.replace(HostTask { host: wanted, creds, ants: sender, task }) {
            old.task.abort();
        }
    }
}

/// The reading of the shared settings without a host connection ([`shared::follow`]), if any, in its task.
#[derive(Default)]
struct Follower(Option<(watch::Sender<Vec<Via>>, JoinHandle<()>)>);

impl Follower {
    /// Read the shared settings into `shared` where `wanted` says: the reading running is told unless it stopped,
    /// when it starts again; with none wanted, it stops.
    fn reconcile(&mut self, wanted: Option<Vec<Via>>, shared: Option<&Shared>) {
        let (Some(wanted), Some(shared)) = (wanted, shared) else {
            if let Some((_, task)) = self.0.take() {
                task.abort();
            }
            return;
        };
        if let Some((vias, task)) = &self.0
            && !task.is_finished()
        {
            vias.send_replace(wanted);
            return;
        }
        let (sender, receiver) = watch::channel(wanted);
        let task = tokio::spawn(shared::follow(shared.clone(), receiver).instrument(tracing::info_span!("shared")));
        if let Some((_, old)) = self.0.replace((sender, task)) {
            old.abort();
        }
    }
}

impl Drop for Follower {
    fn drop(&mut self) {
        if let Some((_, task)) = &self.0 {
            task.abort();
        }
    }
}

impl Drop for HostRun {
    fn drop(&mut self) {
        if let Some(run) = &self.0 {
            run.task.abort();
        }
    }
}

/// The contents of the credentials file at `path`, to tell when it changed (`ants renew`): a NATS connection reads it
/// only when it connects.
pub(crate) fn creds_snapshot(path: Option<&Path>) -> Option<Vec<u8>> {
    std::fs::read(path?).ok()
}

/// An ant being served.
#[derive(Debug)]
struct Running {
    config: Config,
    /// The contents of its credentials file when it started ([`creds_snapshot`]).
    creds: Option<Vec<u8>>,
    stop: Stop,
    /// Asked to stop, as it is no longer listed or its entry changed.
    stopping: bool,
    /// What to serve the ant with once it stopped.
    next: Option<Config>,
}

impl Running {
    /// Note `config` as served, returning it with the stop to serve it until.
    fn insert(running: &mut HashMap<UserId, Running>, config: Config) -> (Config, Stop) {
        let stop = Stop::default();
        let creds = creds_snapshot(config.creds.as_deref());
        let entry = Running { config: config.clone(), creds, stop: stop.clone(), stopping: false, next: None };
        running.insert(config.ant.clone(), entry);
        (config, stop)
    }

    fn ask_to_stop(&mut self) {
        if !self.stopping {
            self.stop.request();
            self.stopping = true;
        }
    }
}

/// Bring the ants `running` in line with those `wanted`, returning the ants to start: those newly listed. An ant no
/// longer listed is asked to stop, finishing its prompts in flight; one whose config or credentials file changed is
/// asked to stop and is served again with its new config once it stopped; the others carry on untouched.
fn reconcile(running: &mut HashMap<UserId, Running>, wanted: Vec<Config>) -> Vec<(Config, Stop)> {
    let listed: HashSet<UserId> = wanted.iter().map(|c| c.ant.clone()).collect();
    for (ant, r) in running.iter_mut().filter(|(ant, _)| !listed.contains(*ant)) {
        if !r.stopping || r.next.is_some() {
            tracing::info!(%ant, "stopping the ant no longer listed");
        }
        r.next = None;
        r.ask_to_stop();
    }
    let mut start = Vec::new();
    for config in wanted {
        let ant = config.ant.clone();
        let Some(r) = running.get_mut(&ant) else {
            tracing::info!(%ant, "serving the ant newly listed");
            start.push(Running::insert(running, config));
            continue;
        };
        config.pause.carry(&r.config.pause);
        if r.stopping {
            r.next = Some(config);
        } else if r.config != config {
            tracing::info!(%ant, "restarting the ant: its entry changed");
            r.next = Some(config);
            r.ask_to_stop();
        } else if r.creds != creds_snapshot(config.creds.as_deref()) {
            tracing::info!(%ant, "restarting the ant: its credentials changed");
            r.next = Some(config);
            r.ask_to_stop();
        } else {
            // Pool members, the pause and the drift are left out of the comparison: the running ant takes them as it
            // is.
            if let (Some(serving), Some(listed)) = (&r.config.pool, &config.pool) {
                serving.members.replace(&listed.members);
            }
            r.config.pause.replace(&config.pause);
            r.config.drift.replace(&config.drift);
        }
    }
    start
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cmd {
        #[command(flatten)]
        args: DaemonArgs,
    }

    fn args(line: &str) -> DaemonArgs {
        Cmd::try_parse_from(std::iter::once("daemon").chain(line.split_whitespace())).unwrap().args
    }

    fn here() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn ant(name: &str) -> Ant {
        let mut p = Ant::new(name.parse().unwrap(), here());
        p.nats = Some(format!("nats://{name}:4222"));
        p.creds = Some(PathBuf::from(format!("{name}.creds")));
        p.harness = Some("copilot".to_owned());
        p
    }

    #[test]
    fn picks_the_source() {
        let existing = here().join("Cargo.toml");
        let missing = here().join("no-such-config.toml");
        assert_eq!(source(&args("--ant dev-a"), &existing).unwrap(), Source::Flags("dev-a".parse().unwrap()));
        assert_eq!(source(&args(""), &existing).unwrap(), Source::File(existing.clone()));
        assert_eq!(source(&args("--config c.toml --ant dev-a"), &missing).unwrap(), Source::File("c.toml".into()));
        let err = source(&args(""), &missing).unwrap_err().to_string();
        assert!(err.contains("no --ant given") && err.contains("no-such-config.toml"), "{err}");
    }

    #[test]
    fn flags_override_the_picked_ant() {
        let all = vec![ant("dev-a"), ant("dev-b")];
        let path = Path::new("config.toml");
        let picked = pick(
            all.clone(),
            &args(
                "--ant dev-b --max-harnesses 1 --allow-tools --allow read --capability rust,nats --capability acp \
                 --max-tool-calls 20 --max-daily-cost 7.5",
            ),
            path,
        )
        .unwrap();
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].name.as_str(), "dev-b");
        assert_eq!((picked[0].max_harnesses, picked[0].allow_tools), (1, true));
        assert_eq!(picked[0].allow, ["read"]);
        assert_eq!(picked[0].capabilities, ["rust", "nats", "acp"]);
        assert_eq!(picked[0].idle_timeout, config::DEFAULT_IDLE_TIMEOUT);
        assert_eq!((picked[0].max_tool_calls, picked[0].max_daily_cost), (20, Some(7.5)));
        assert_eq!(picked[0].prompt_timeout, config::DEFAULT_PROMPT_TIMEOUT);
        assert_eq!(picked[0].worktrees, None, "unset unless a flag sets it");
        let worktrees =
            |flag: &str| pick(all.clone(), &args(&format!("--ant dev-a {flag}")), path).unwrap()[0].worktrees;
        assert_eq!((worktrees("--worktrees"), worktrees("--no-worktrees")), (Some(true), Some(false)));

        assert_eq!(pick(all.clone(), &args(""), path).unwrap(), all);
        let err = pick(all.clone(), &args("--ant dev-c"), path).unwrap_err().to_string();
        assert!(err.contains("ant dev-c is not in config.toml"), "{err}");
        let err = pick(all, &args("--workdir ."), path).unwrap_err().to_string();
        assert!(err.contains("add --ant"), "{err}");

        let added = vec![Ant { template: Some("worker".to_owned()), ..ant("lead-a") }];
        assert_eq!(pick(added.clone(), &args("--ant lead-a"), path).unwrap(), added);
        let err = pick(added, &args("--ant lead-a --max-daily-cost 99"), path).unwrap_err().to_string();
        assert!(err.contains("ant lead-a was added from template worker"), "{err}");
    }

    #[test]
    fn the_file_wins_over_global_flags() {
        let mut bare = ant("dev-a");
        bare.nats = None;
        bare.creds = None;
        let cli_creds = Some(PathBuf::from("cli.creds"));
        let (cli_domain, own_domain): (JsDomain, JsDomain) = ("cli".parse().unwrap(), "own".parse().unwrap());

        let one = configs(
            vec![bare.clone()],
            &Harnesses::builtin(),
            "nats://cli:4222",
            cli_creds.clone(),
            Some(cli_domain.clone()),
            None,
        )
        .unwrap();
        assert_eq!((one[0].nats_url.as_str(), one[0].creds.clone()), ("nats://cli:4222", cli_creds.clone()));
        assert_eq!(one[0].js_domain, Some(cli_domain.clone()));

        let mut own = ant("dev-a");
        own.js_domain = Some(own_domain.clone());
        let one =
            configs(vec![own], &Harnesses::builtin(), "nats://cli:4222", cli_creds.clone(), Some(cli_domain), None)
                .unwrap();
        assert_eq!(one[0].nats_url, "nats://dev-a:4222");
        assert_eq!(one[0].creds, Some(PathBuf::from("dev-a.creds")));
        assert_eq!(one[0].js_domain, Some(own_domain));

        let two =
            configs(vec![bare, ant("dev-b")], &Harnesses::builtin(), "nats://cli:4222", cli_creds, None, None).unwrap();
        assert_eq!((two[0].nats_url.as_str(), two[0].creds.clone()), ("nats://cli:4222", None));
        assert_eq!(two[1].creds, Some(PathBuf::from("dev-b.creds")));
    }

    #[test]
    fn a_reload_starts_new_ants_and_stops_or_restarts_the_others() {
        let config = |name: &str, model: Option<&str>| {
            let mut p = ant(name);
            p.model = model.map(str::to_owned);
            configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0)
        };
        let names = |start: Vec<(Config, Stop)>| start.into_iter().map(|(c, _)| c.ant.to_string()).collect::<Vec<_>>();
        let state = |running: &HashMap<UserId, Running>, name: &str| {
            let r = &running[&name.parse::<UserId>().unwrap()];
            (r.stopping, r.next.as_ref().map(|c| c.model_settings.model.clone()))
        };
        let mut running = HashMap::new();
        let (a, b) = (config("dev-a", None), config("dev-b", None));
        assert_eq!(names(reconcile(&mut running, vec![a.clone(), b.clone()])), ["dev-a", "dev-b"]);
        assert!(names(reconcile(&mut running, vec![a.clone(), b.clone()])).is_empty());
        assert_eq!((state(&running, "dev-a"), state(&running, "dev-b")), ((false, None), (false, None)));

        let changed = config("dev-a", Some("smart"));
        assert_eq!(names(reconcile(&mut running, vec![changed, config("dev-c", None)])), ["dev-c"]);
        assert_eq!(state(&running, "dev-a"), (true, Some(Some("smart".to_owned()))));
        assert_eq!(state(&running, "dev-b"), (true, None));
        assert_eq!(state(&running, "dev-c"), (false, None));

        // Listed again before it stopped, an ant is served again once it did.
        assert!(names(reconcile(&mut running, vec![a, b, config("dev-c", None)])).is_empty());
        assert_eq!((state(&running, "dev-a"), state(&running, "dev-b")), ((true, Some(None)), (true, Some(None))));
        assert_eq!(state(&running, "dev-c"), (false, None));
    }

    #[test]
    fn a_reload_hands_a_running_ant_its_pool_s_new_members_without_a_restart() {
        use ants_daemon::caps::{Members, Pool};
        let member = |name: &str| name.parse::<UserId>().unwrap();
        let config = |members: &[&str]| {
            let pool = Pool {
                template: "worker".to_owned(),
                daily_tokens: Some(100),
                daily_cost: None,
                members: Members::new(members.iter().map(|m| member(m)).collect()),
            };
            let p = Ant { pool: Some(pool), ..ant("lead-a") };
            configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0)
        };
        let mut running = HashMap::new();
        let (served, _) = reconcile(&mut running, vec![config(&["lead-a"])]).remove(0);
        assert!(reconcile(&mut running, vec![config(&["lead-a", "lead-b"])]).is_empty());
        assert!(!running[&member("lead-a")].stopping, "only its pool's members changed");
        assert_eq!(served.pool.unwrap().members.get(), [member("lead-a"), member("lead-b")]);
    }

    #[test]
    fn a_reload_pauses_and_resumes_a_running_ant_without_a_restart() {
        let ant_named = |paused: bool, model: Option<&str>| {
            let p = Ant { paused, model: model.map(str::to_owned), ..ant("dev-a") };
            configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0)
        };
        let dev_a: UserId = "dev-a".parse().unwrap();
        let mut running = HashMap::new();
        let (served, _) = reconcile(&mut running, vec![ant_named(false, None)]).remove(0);
        let paused = ant_named(true, None);
        paused.pause.credit(Some("alice".parse().unwrap()));
        assert!(reconcile(&mut running, vec![paused]).is_empty());
        assert!(!running[&dev_a].stopping, "only paused changed");
        let pause = served.pause.get().expect("the running ant is paused");
        assert_eq!(pause.by.as_ref().map(UserId::as_str), Some("alice"));
        assert!(reconcile(&mut running, vec![ant_named(true, None)]).is_empty());
        assert_eq!(served.pause.get(), Some(pause.clone()), "a pause on keeps its start and admin");

        assert!(reconcile(&mut running, vec![ant_named(true, Some("smart"))]).is_empty());
        let next = running[&dev_a].next.as_ref().expect("restarted for its model");
        assert_eq!(next.pause.get(), Some(pause), "carried over the restart");
        running.get_mut(&dev_a).unwrap().stopping = false;
        running.get_mut(&dev_a).unwrap().next = None;
        assert!(reconcile(&mut running, vec![ant_named(false, None)]).is_empty());
        assert!(!running[&dev_a].stopping, "only paused changed");
        assert_eq!(served.pause.get(), None, "resumed");
    }

    #[test]
    fn a_change_of_drift_alone_restarts_no_ant() {
        let dev_a = || {
            let p = Ant { model: Some("gpt-4".to_owned()), ..ant("dev-a") };
            Ant { creds: None, ..p }
        };
        let shared = Shared::default();
        let record = |fields: serde_json::Value| {
            let fields = fields.as_object().unwrap().clone();
            ants_proto::SharedSettings::new("dev-a".parse().unwrap(), fields, "m1".parse().unwrap(), None)
        };
        shared.store(record(serde_json::json!({ "model": "gpt-5" })));
        let served = |ant: Ant| {
            shared.listed(std::slice::from_ref(&ant));
            let harnesses = Harnesses::builtin();
            configs(vec![ant], &harnesses, "nats://cli:4222", None, None, Some(&shared)).unwrap().remove(0)
        };
        let mut running = HashMap::new();
        let (config, _) = reconcile(&mut running, vec![served(dev_a())]).remove(0);
        assert_eq!((config.model_settings.model.as_deref(), config.drift.get()), (Some("gpt-5"), vec!["model".into()]));
        let drift = config.drift.subscribe();

        let mut same = dev_a();
        same.model = Some("gpt-5".to_owned());
        assert!(reconcile(&mut running, vec![served(same)]).is_empty());
        let dev_a_id: UserId = "dev-a".parse().unwrap();
        assert!(!running[&dev_a_id].stopping, "only the drift changed");
        assert!(drift.has_changed().unwrap(), "the running ant hears of it");
        assert_eq!(config.drift.get(), Vec::<String>::new());

        shared.store(record(serde_json::json!({ "model": "o3" })));
        assert!(reconcile(&mut running, vec![served(dev_a())]).is_empty());
        assert!(running[&dev_a_id].stopping, "the shared settings changed");
    }

    #[test]
    fn a_reload_restarts_an_ant_whose_credentials_file_changed() {
        let dir = ants_test_support::scratch_dir("reload-creds");
        let creds = dir.join("dev-a.creds");
        std::fs::write(&creds, "issued").unwrap();
        let mut p = ant("dev-a");
        p.creds = Some(creds.clone());
        let config = configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0);
        let mut running = HashMap::new();
        assert_eq!(reconcile(&mut running, vec![config.clone()]).len(), 1);
        assert!(reconcile(&mut running, vec![config.clone()]).is_empty());
        assert!(!running[&config.ant].stopping, "same entry, same credentials");
        std::fs::write(&creds, "renewed").unwrap();
        assert!(reconcile(&mut running, vec![config.clone()]).is_empty());
        let r = &running[&config.ant];
        assert!(r.stopping && r.next.as_ref() == Some(&config), "served again with the renewed credentials");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_reload_starts_stops_or_restarts_schedules() {
        use std::cell::RefCell;
        let entry = |name: &str, text: &str| {
            let toml = format!("name = \"{name}\"\nevery = \"1h\"\nant = \"dev-a\"\ntext = \"{text}\"\n");
            Schedule::from_entry(toml::from_str(&toml).unwrap(), None, Path::to_path_buf).unwrap()
        };
        let started = RefCell::new(Vec::new());
        let spawn = |s: Schedule| {
            started.borrow_mut().push(s.name.to_string());
            tokio::spawn(std::future::pending())
        };
        let mut schedules = Schedules::default();
        schedules.reconcile(vec![entry("nightly", "a"), entry("pulse", "a")], spawn);
        schedules.reconcile(vec![entry("nightly", "a"), entry("pulse", "a")], spawn);
        assert_eq!(started.take(), ["nightly", "pulse"]);
        let pulse = schedules.0[&"pulse".parse::<UserId>().unwrap()].task.abort_handle();

        schedules.reconcile(vec![entry("nightly", "b"), entry("weekly", "a")], spawn);
        assert_eq!(started.take(), ["nightly", "weekly"]);
        tokio::task::yield_now().await;
        assert!(pulse.is_finished(), "a schedule no longer listed stops");
        assert_eq!(schedules.0.len(), 2);
    }

    #[test]
    fn duration_flags_need_a_unit() {
        let p = args("--idle-timeout 90s --prompt-timeout 0 --approval-timeout 1h30m").options.apply(ant("dev-a"));
        assert_eq!(
            (p.idle_timeout, p.prompt_timeout, p.approval_timeout),
            (Duration::from_secs(90), Duration::ZERO, Duration::from_mins(90))
        );
        for line in ["--idle-timeout 90", "--idle-timeout-secs 90", "--prompt-timeout 1.5h"] {
            assert!(Cmd::try_parse_from(std::iter::once("daemon").chain(line.split_whitespace())).is_err(), "{line}");
        }
    }

    #[test]
    fn model_flags_override_only_the_selected_ant() {
        let mut p = ant("dev-a");
        p.model = Some("old-model".to_owned());
        p.reasoning_effort = Some("low".to_owned());
        p.context = Some("default".to_owned());
        let picked = pick(
            vec![p.clone(), ant("dev-b")],
            &args("--ant dev-a --model smart --reasoning-effort high --context long_context"),
            Path::new("config.toml"),
        )
        .unwrap();
        assert_eq!(picked[0].model.as_deref(), Some("smart"));
        assert_eq!(picked[0].reasoning_effort.as_deref(), Some("high"));
        assert_eq!(picked[0].context.as_deref(), Some("long_context"));
        let only_model = args("--model smart").options.apply(p);
        assert_eq!(only_model.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(only_model.context.as_deref(), Some("default"));
        for line in ["--model smart", "--reasoning-effort high", "--context long_context"] {
            assert!(pick(vec![ant("dev-a")], &args(line), Path::new("config.toml")).is_err());
        }
    }

    #[test]
    fn tool_flags_override_only_the_selected_settings() {
        let mut p = ant("dev-a");
        p.whoami = Some("file-role".to_owned());
        p.memory = Some("file-memory".to_owned());
        p.tasks = Some("file-tasks".to_owned());
        p.env.insert("TASK_DB".to_owned(), "file.db".to_owned());
        p.env.insert("KEEP".to_owned(), "keep".to_owned());
        let picked = pick(
            vec![p],
            &args("--ant dev-a --whoami custom-role --memory custom-memory --env TASK_DB=cli.db --env EXTRA=a=b"),
            Path::new("config.toml"),
        )
        .unwrap();
        assert_eq!(picked[0].whoami.as_deref(), Some("custom-role"));
        assert_eq!(picked[0].memory.as_deref(), Some("custom-memory"));
        assert_eq!(picked[0].tasks.as_deref(), Some("file-tasks"));
        assert_eq!(picked[0].env["TASK_DB"], "cli.db");
        assert_eq!(picked[0].env["KEEP"], "keep");
        assert_eq!(picked[0].env["EXTRA"], "a=b");
        let clear = Cmd::try_parse_from(["daemon", "--ant", "dev-a", "--whoami", "", "--tasks", "", "--env", "EMPTY="])
            .unwrap()
            .args;
        let cleared = clear.options.apply(picked[0].clone());
        assert_eq!(cleared.whoami.as_deref(), Some(""));
        assert_eq!(cleared.tasks.as_deref(), Some(""));
        assert_eq!(cleared.env["EMPTY"], "");
        assert!(Cmd::try_parse_from(["daemon", "--env", "=value"]).is_err());
        assert!(Cmd::try_parse_from(["daemon", "--env", "NAME"]).is_err());
        for line in ["--whoami role", "--memory notes", "--tasks tickets", "--env NAME=value"] {
            assert!(pick(vec![ant("dev-a")], &args(line), Path::new("config.toml")).is_err());
        }
    }
}
