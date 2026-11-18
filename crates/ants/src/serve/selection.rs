//! Which ants `ants daemon` serves: its flags, the ants they pick from the config file or describe on the command
//! line, and their configs.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ants_daemon::config::{self, Ant, HostSettings, Summary};
use ants_daemon::profile::shared::{Drift, Shared};
use ants_daemon::schedule::Schedule;
use ants_daemon::{Config, Harnesses};
use ants_proto::{JsDomain, Machine, UserId};
use anyhow::{Context as _, Result, bail};

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
    pub(super) options: AntOptions,
    #[command(flatten)]
    pub upgrade: crate::release::upgrade::UpgradeArgs,
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
    /// Role instructions shared in the colony and prepended to every prompt.
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
    /// Tag the ant is good for, listed in its colony entry for routing (repeatable, or comma-separated). Replaces
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

pub(super) fn env_pair(text: &str) -> Result<(String, String), String> {
    let (name, value) = text.split_once('=').ok_or("expected NAME=VALUE")?;
    if name.is_empty() || name.contains('\0') || value.contains('\0') {
        return Err("invalid environment variable".to_owned());
    }
    Ok((name.to_owned(), value.to_owned()))
}

#[derive(Debug, PartialEq)]
pub(super) enum Source {
    Flags(UserId),
    File(PathBuf),
}

/// `--config` (or `ANTS_CONFIG`) names the file; otherwise `--ant` alone means the flags describe it, and with
/// neither the file at `default_path` is used.
pub(super) fn source(args: &DaemonArgs, default_path: &Path) -> Result<Source> {
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
    /// The machine's host settings for sense's link of its own ([`crate::serve::sense_link`]): only when the daemon
    /// serves one ant, making no host connection; for an ant described by flags those of the file at `default_path`,
    /// on the ant's server rather than the file's.
    pub sense_link: Option<HostSettings>,
    /// The machine's name as the file's host settings give it ([`HostSettings::named`]); None for an ant described by
    /// flags, which goes by the host name.
    pub machine: Option<Machine>,
    /// The nats-server the daemon starts and supervises ([`config::NatsServer`]): only when the daemon serves the
    /// whole file.
    pub nats_server: Option<config::NatsServer>,
    /// The file's `auto_update`: only when the daemon serves the whole file, off otherwise.
    pub auto_update: config::AutoUpdate,
}

/// The ants to serve, the harnesses they may name and the schedules to run: those of the config file, or for an ant
/// described by flags the harnesses and host settings of the file at `default_path` if it exists, and its top-level
/// `harness` unless the flags name one. The schedules run only when the daemon serves the whole file, not one ant
/// picked with `--ant`. `hostname` names the machine where the file does
/// not. A daemon serving the whole file says once why it leaves the built-in sage to another machine
/// ([`config::Loaded::sage_elsewhere`]).
pub fn ants(args: &DaemonArgs, default_path: &Path, hostname: &str) -> Result<Listed> {
    match source(args, default_path)? {
        Source::Flags(name) => {
            let mut ant = args.options.apply(Ant::new(name, std::env::current_dir()?));
            let summary = Summary::load(default_path)?;
            if ant.harness.is_none() {
                ant.harness = summary.harness;
            }
            let harnesses = summary.harnesses;
            let (schedules, metrics) = (Vec::new(), args.metrics);
            let (managers, host, machine, nats_server) = (None, None, None, None);
            let sense_link = Some(HostSettings { nats: None, js_domain: None, ..summary.host });
            let auto_update = config::AutoUpdate::Off;
            Ok(Listed {
                ants: vec![ant],
                harnesses,
                schedules,
                metrics,
                managers,
                host,
                sense_link,
                machine,
                nats_server,
                auto_update,
            })
        }
        Source::File(path) => {
            let loaded = config::load(&path)?;
            let whole = args.ant.is_none();
            if let Some(creds) = loaded.sage_elsewhere.as_deref().filter(|_| whole)
                && !SAGE_ELSEWHERE_TOLD.swap(true, Ordering::Relaxed)
            {
                tracing::info!("{}", sage_elsewhere(creds));
            }
            let schedules = if whole { loaded.schedules } else { Vec::new() };
            let metrics = args.metrics.or(loaded.metrics);
            let managers = loaded
                .ants
                .iter()
                .filter(|a| crate::signing::manager::manages(&loaded.templates, &a.name, a.template.is_some()))
                .map(|a| a.name.clone())
                .collect();
            let managers = whole.then(|| (managers, path.clone()));
            let machine = Some(loaded.host.named(hostname));
            let (host, sense_link) = if whole { (Some(loaded.host), None) } else { (None, Some(loaded.host)) };
            let nats_server = loaded.nats_server.filter(|_| whole);
            let auto_update = if whole { loaded.auto_update } else { config::AutoUpdate::Off };
            let ants = pick(loaded.ants, loaded.sage_elsewhere.as_deref(), args, &path)?;
            let harnesses = loaded.harnesses;
            Ok(Listed {
                ants,
                harnesses,
                schedules,
                metrics,
                managers,
                host,
                sense_link,
                machine,
                nats_server,
                auto_update,
            })
        }
    }
}

/// Whether a daemon said already why it leaves the built-in sage to another machine.
static SAGE_ELSEWHERE_TOLD: AtomicBool = AtomicBool::new(false);

/// Why the machine leaves the built-in sage to another, its credentials belonging at `creds`, and how to serve it here.
fn sage_elsewhere(creds: &Path) -> String {
    format!(
        "no credentials of the built-in sage at {}, which the server's authentication needs: the sage is left to \
         another machine, the ants here naming it all the same; to serve it here, issue them with `ants auth user \
         sage` on the machine holding the team keys, copy them there and reload the daemon",
        creds.display()
    )
}

/// The ants of the file at `path` that `args` pick: the one `--ant` names, with the ant flags applied, else all of
/// them. `sage_elsewhere` is where the built-in sage's credentials belong when the file leaves it to another machine,
/// for which `--ant sage` fails.
pub(super) fn pick(all: Vec<Ant>, sage_elsewhere: Option<&Path>, args: &DaemonArgs, path: &Path) -> Result<Vec<Ant>> {
    match &args.ant {
        Some(name) => {
            let p = all.into_iter().find(|p| &p.name == name).with_context(|| match sage_elsewhere {
                Some(creds) if *name == UserId::sage() => format!(
                    "the built-in sage has no credentials at {}, which the server's authentication needs: issue them \
                     with `ants auth user sage` on the machine holding the team keys and copy them there, or serve the \
                     sage where they are",
                    creds.display()
                ),
                _ => format!("ant {name} is not in {}", path.display()),
            })?;
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
