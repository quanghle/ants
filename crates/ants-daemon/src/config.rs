//! The daemon config file, `~/.ants/config.toml`: the ants one daemon process serves.
//!
//! ```toml
//! harness = "copilot"               # default harness of every ant and of the built-in messenger
//! messenger = true                  # serve the built-in messenger (the default)
//! nats = "wss://hub.example/nats"   # default server for every ant
//! js_domain = "hub"                 # JetStream domain of the hub, if it has one; also per ant
//! human = "alice"                   # default human, for escalations of threads with no owner; also per ant
//! metrics = "127.0.0.1:9464"        # serve /healthz and /metrics there (crate::metrics); off without it
//! timezone = "America/New_York"     # default zone of the schedules' cron, else UTC
//!
//! [cli]                             # the CLI's own connection, where no flag or environment variable sets it
//! creds = "~/.config/ants/auth/alice.creds"
//! nats = "wss://hub.example/nats"   # default: the top-level nats
//! js_domain = "hub"                 # default: the top-level js_domain
//!
//! [[ant]]
//! name = "dev-a"
//! workdir = "~/src/repo-a"
//! creds = "~/.config/ants/auth/dev-a.creds"
//! escalate_to = ["lead", "dev-b"]   # escalations go to these ants first, lead leading
//! idle_timeout = "10m"              # durations: 90s, 10m, 1h30m, 0
//! worktrees = true                  # a git worktree per thread, under the ant's home
//! harness = "my-agent"
//!
//! [harnesses.my-agent]              # an ACP adapter on stdio, besides the built-in ones (harnesses.toml)
//! command = "~/bin/my-agent-acp"     # a path, or a bare name looked up on PATH
//! args = ["--stdio"]
//! env = { MY_AGENT_LOG = "warn" }   # under the ant's own env
//! mode = "default"                  # ACP session mode every session must run in
//!
//! [[ant_template]]                  # what a manager ant may add worker ants as; see crate::template
//! name = "worker"
//! managers = ["lead"]
//! max_ants = 3
//! workdirs = ["~/src/repo-a"]
//!
//! [[ant]]                           # an ant added from a template: its other settings come from the template
//! name = "lead-docs"
//! template = "worker"
//! added_by = "lead"
//! creds = "~/.config/ants/auth/lead-docs.creds"
//!
//! [[schedule]]                      # a prompt on a timetable; see crate::schedule
//! name = "nightly"
//! cron = "0 3 * * *"
//! ant = "dev-a"
//! text = "Audit the dependencies."
//! ```
//!
//! `~` expands to the home directory; other relative paths, harness commands with a `/` included, are relative to the
//! file's directory.
//!
//! The `*.toml` files of the `harnesses.d` directory beside the file ([`dropin_dir`]), read in name order, hidden
//! ones skipped, define harnesses too: each holds `[harnesses.<name>]` tables alone, as in this file, a relative
//! command being relative to `harnesses.d`. A name is defined once across this file and those.
//!
//! A file also serves the built-in messenger ([`builtin_messenger`]) unless it sets `messenger = false` or lists an
//! ant named `messenger`; an empty file is a complete one.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use crate::caps::{Caps, Members, Pool};
use crate::daemon::Config;
use crate::escalation::{self, Chain};
use crate::harness::{self, DEFAULT_HARNESS, Harness, Harnesses, ModelSettings, PromptLimits};
use crate::schedule::{self, Schedule};
use crate::template::{AntTemplate, REQUEST_KEYS, Request};

/// Environment variable naming the config file.
pub const ENV: &str = "ANTS_CONFIG";
pub const DEFAULT_MAX_HARNESSES: usize = 4;
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);
pub const DEFAULT_APPROVAL_TIMEOUT: Duration = crate::approvals::DEFAULT_TIMEOUT;
pub const DEFAULT_PROMPT_TIMEOUT: Duration = harness::DEFAULT_PROMPT_TIMEOUT;
pub const DEFAULT_MAX_TOOL_CALLS: usize = harness::DEFAULT_MAX_TOOL_CALLS;
pub const DEFAULT_ESCALATE_AFTER: Duration = escalation::DEFAULT_ESCALATE_AFTER;
pub const DEFAULT_ESCALATION_REMINDER: Duration = escalation::DEFAULT_ESCALATION_REMINDER;
pub const DEFAULT_SPEND_WARN_PERCENT: u8 = 80;

/// `~/.ants/config.toml`.
pub fn default_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".ants").join("config.toml"))
}

fn home_dir() -> Result<PathBuf> {
    std::env::home_dir().context("no home directory")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    /// Default harness of the file's ants, and the harness of the built-in messenger.
    harness: Option<String>,
    /// Harness definitions, added to the built-in ones or replacing them by name.
    #[serde(default)]
    harnesses: BTreeMap<String, Harness>,
    /// Whether to serve the built-in messenger, unless an ant is named `messenger`.
    #[serde(default = "yes")]
    messenger: bool,
    nats: Option<String>,
    js_domain: Option<JsDomain>,
    human: Option<UserId>,
    /// Where to serve health and metrics ([`crate::metrics`]).
    metrics: Option<SocketAddr>,
    /// Default time zone of the schedules' cron timetables.
    timezone: Option<String>,
    #[serde(default)]
    cli: CliSection,
    #[serde(default, rename = "ant_template")]
    ant_templates: Vec<AntTemplate>,
    #[serde(default, rename = "ant")]
    ants: Vec<Ant>,
    #[serde(default, rename = "schedule")]
    schedules: Vec<schedule::Entry>,
}

/// The keys of an `[[ant]]` entry added from a template besides those of a [`Request`].
const TEMPLATED_KEYS: [&str; 6] = ["name", "template", "added_by", "creds", "nats", "js_domain"];

/// One ant the daemon serves, as the config file (or the command line) describes it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ant {
    pub name: UserId,
    /// NATS server; the file's top-level `nats`, then `--nats`, when absent.
    pub nats: Option<String>,
    pub creds: Option<PathBuf>,
    /// JetStream domain of the hub; the file's top-level `js_domain`, then `--js-domain`, when absent.
    pub js_domain: Option<JsDomain>,
    /// The default human, who gets what is meant for the owner of a thread that has none: escalations, dead-letter
    /// notices. The file's top-level `human`, then `human`, when absent.
    pub human: Option<UserId>,
    /// Required, except of an ant added from a template, whose default is the template's first workdir.
    #[serde(default)]
    pub workdir: PathBuf,
    /// The `[[ant_template]]` the ant was added from, which bounds its settings and gives those it leaves out.
    pub template: Option<String>,
    /// The manager ant that added it from its template.
    pub added_by: Option<UserId>,
    /// The template's named sets of tool rules it has, besides the template's `allow`.
    #[serde(default)]
    pub allow_sets: Vec<String>,
    /// Default: `~/.ants/<name>`.
    pub home: Option<PathBuf>,
    /// Role instructions shared in the roster and prepended to every prompt.
    pub whoami: Option<String>,
    /// Memory and task instructions prepended to every prompt. Tools are provided by the harness.
    pub memory: Option<String>,
    pub tasks: Option<String>,
    /// Added to the harness's inherited environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// A harness name ([`Harnesses`]); the file's top-level `harness`, then [`DEFAULT_HARNESS`], when absent.
    pub harness: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub context: Option<String>,
    /// Bytes of the thread's history a rebuilt session is seeded with, over the harness's and its context's.
    pub history_budget: Option<usize>,
    /// Same as `allow = ["*"]`.
    #[serde(default)]
    pub allow_tools: bool,
    /// Tool allowlist; see [`crate::permission`].
    #[serde(default)]
    pub allow: Vec<String>,
    /// How long a tool request outside the allowlist waits for the human.
    #[serde(default = "default_approval_timeout", deserialize_with = "ants_proto::duration::deserialize")]
    pub approval_timeout: Duration,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default = "default_max_harnesses")]
    pub max_harnesses: usize,
    /// Give each thread its own git worktree of the workdir's repository under the home.
    #[serde(default)]
    pub worktrees: bool,
    #[serde(default = "default_idle_timeout", deserialize_with = "ants_proto::duration::deserialize")]
    pub idle_timeout: Duration,
    /// How long a prompt may run before it is cancelled; 0 for no limit.
    #[serde(default = "default_prompt_timeout", deserialize_with = "ants_proto::duration::deserialize")]
    pub prompt_timeout: Duration,
    /// Tool calls a prompt may start before it is cancelled; 0 for no limit.
    #[serde(default = "default_max_tool_calls")]
    pub max_tool_calls: usize,
    /// Tokens a thread may spend between messages from a human.
    pub max_thread_tokens: Option<u64>,
    /// Cost a thread may spend between messages from a human, in the harness's unit.
    pub max_thread_cost: Option<f64>,
    /// Tokens the ant may spend in a UTC day.
    pub max_daily_tokens: Option<u64>,
    /// Cost the ant may spend in a UTC day.
    pub max_daily_cost: Option<f64>,
    /// The share of a spend cap, in percent, past which a human is warned once; 0 for never.
    #[serde(default = "default_spend_warn_percent")]
    pub spend_warn_percent: u8,
    /// Ants the ant's escalations go to before any human, the first one leading; published in the roster.
    #[serde(default)]
    pub escalate_to: Vec<UserId>,
    /// Chain levels an escalation climbs before it goes to the thread's owner.
    #[serde(default = "default_max_escalation_hops")]
    pub max_escalation_hops: u32,
    /// How long an escalation waits for its leader's answer before going one level higher; 0 waits for good.
    #[serde(default = "default_escalate_after", deserialize_with = "ants_proto::duration::deserialize")]
    pub escalate_after: Duration,
    /// How long an escalation that reached a human waits for a human's message on the thread before a reminder, sent
    /// daily after that; 0 never reminds.
    #[serde(default = "default_escalation_reminder", deserialize_with = "ants_proto::duration::deserialize")]
    pub escalation_reminder: Duration,
    /// The built-in messenger, served only while no other daemon lists a messenger in the roster.
    #[serde(skip)]
    pub builtin: bool,
    /// The daily caps it shares with the other ants added from its template, if the template has any.
    #[serde(skip)]
    pub pool: Option<Pool>,
}

fn default_max_harnesses() -> usize {
    DEFAULT_MAX_HARNESSES
}

fn yes() -> bool {
    true
}

fn default_idle_timeout() -> Duration {
    DEFAULT_IDLE_TIMEOUT
}

fn default_approval_timeout() -> Duration {
    DEFAULT_APPROVAL_TIMEOUT
}

fn default_prompt_timeout() -> Duration {
    DEFAULT_PROMPT_TIMEOUT
}

fn default_max_tool_calls() -> usize {
    DEFAULT_MAX_TOOL_CALLS
}

fn default_spend_warn_percent() -> u8 {
    DEFAULT_SPEND_WARN_PERCENT
}

fn default_max_escalation_hops() -> u32 {
    ants_proto::DEFAULT_MAX_ESCALATION_HOPS
}

fn default_escalate_after() -> Duration {
    DEFAULT_ESCALATE_AFTER
}

fn default_escalation_reminder() -> Duration {
    DEFAULT_ESCALATION_REMINDER
}

impl Ant {
    /// An ant with default settings.
    pub fn new(name: UserId, workdir: PathBuf) -> Self {
        Self {
            name,
            nats: None,
            creds: None,
            js_domain: None,
            human: None,
            workdir,
            template: None,
            added_by: None,
            allow_sets: Vec::new(),
            home: None,
            whoami: None,
            memory: None,
            tasks: None,
            env: BTreeMap::new(),
            harness: None,
            model: None,
            reasoning_effort: None,
            context: None,
            history_budget: None,
            allow_tools: false,
            allow: Vec::new(),
            approval_timeout: DEFAULT_APPROVAL_TIMEOUT,
            capabilities: Vec::new(),
            max_harnesses: DEFAULT_MAX_HARNESSES,
            worktrees: false,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            prompt_timeout: DEFAULT_PROMPT_TIMEOUT,
            max_tool_calls: DEFAULT_MAX_TOOL_CALLS,
            max_thread_tokens: None,
            max_thread_cost: None,
            max_daily_tokens: None,
            max_daily_cost: None,
            spend_warn_percent: DEFAULT_SPEND_WARN_PERCENT,
            escalate_to: Vec::new(),
            max_escalation_hops: ants_proto::DEFAULT_MAX_ESCALATION_HOPS,
            escalate_after: DEFAULT_ESCALATE_AFTER,
            escalation_reminder: DEFAULT_ESCALATION_REMINDER,
            builtin: false,
            pool: None,
        }
    }

    /// The daemon config, with `default_nats` as the server when none is set and the harness from `harnesses`.
    pub fn into_config(self, default_nats: &str, harnesses: &Harnesses) -> Result<Config> {
        harness::validate_env(&self.env)?;
        self.validate()?;
        let instructions = self.prompt_instructions();
        let caps = self.caps();
        let harness = harnesses
            .get(self.harness.as_deref().unwrap_or(DEFAULT_HARNESS))
            .with_context(|| format!("ant {}", self.name))?;
        let model_settings = ModelSettings {
            model: self.model,
            reasoning_effort: self.reasoning_effort,
            context: self.context,
            history_budget: self.history_budget,
        };
        model_settings.validate(&harness).with_context(|| format!("ant {}", self.name))?;
        if self.builtin {
            std::fs::create_dir_all(&self.workdir)
                .with_context(|| format!("ant {}: creating workdir {}", self.name, self.workdir.display()))?;
        }
        let workdir = self
            .workdir
            .canonicalize()
            .with_context(|| format!("ant {}: workdir {}", self.name, self.workdir.display()))?;
        let home = match self.home {
            Some(home) => home,
            None => home_dir()?.join(".ants").join(self.name.as_str()),
        };
        let nats_url = self.nats.unwrap_or_else(|| default_nats.to_owned());
        crate::bus::check_url(&nats_url).with_context(|| format!("ant {}", self.name))?;
        Ok(Config {
            nats_url,
            creds: self.creds,
            js_domain: self.js_domain,
            human: self.human.unwrap_or_else(UserId::human),
            workdir,
            home,
            instructions,
            whoami: self.whoami,
            env: self.env,
            harness,
            model_settings,
            max_harnesses: self.max_harnesses,
            worktrees: self.worktrees,
            idle_timeout: self.idle_timeout,
            allow: if self.allow_tools { [self.allow, vec!["*".to_owned()]].concat() } else { self.allow },
            approval_timeout: self.approval_timeout,
            capabilities: self.capabilities,
            limits: PromptLimits {
                timeout: Some(self.prompt_timeout).filter(|t| !t.is_zero()),
                max_tool_calls: Some(self.max_tool_calls).filter(|n| *n > 0),
            },
            caps,
            spend_warn_percent: self.spend_warn_percent,
            pool: self.pool,
            escalation: Chain {
                escalate_to: self.escalate_to,
                max_hops: self.max_escalation_hops,
                after: Some(self.escalate_after).filter(|t| !t.is_zero()),
                remind: Some(self.escalation_reminder).filter(|t| !t.is_zero()),
            },
            ant: self.name,
            defer_to_others: self.builtin,
            requester: crate::mcp::Requester::default(),
        })
    }

    /// Its spend caps.
    pub fn caps(&self) -> Caps {
        Caps {
            thread_tokens: self.max_thread_tokens,
            thread_cost: self.max_thread_cost,
            daily_tokens: self.max_daily_tokens,
            daily_cost: self.max_daily_cost,
        }
    }

    /// The escalation chain names other ants, each once, and the spend warning comes short of the caps.
    fn validate(&self) -> Result<()> {
        if self.spend_warn_percent >= 100 {
            bail!("ant {}: spend_warn_percent must be below 100 (0 for no warning)", self.name);
        }
        let mut seen = BTreeSet::new();
        for member in &self.escalate_to {
            if *member == self.name {
                bail!("ant {}: escalate_to names the ant itself", self.name);
            }
            if !seen.insert(member) {
                bail!("ant {}: escalate_to lists {member} twice", self.name);
            }
        }
        Ok(())
    }

    fn prompt_instructions(&self) -> String {
        [
            (format!("role {}", self.name), self.whoami.as_deref()),
            ("memory".to_owned(), self.memory.as_deref()),
            ("tasks".to_owned(), self.tasks.as_deref()),
        ]
        .into_iter()
        .filter_map(|(role, text)| {
            text.filter(|text| !text.trim().is_empty()).map(|text| format!("[ants {role}]\n{text}"))
        })
        .collect::<Vec<_>>()
        .join("\n\n")
    }
}

/// The ant a file serves unless it sets `messenger = false` or lists one named `messenger`: the file's `harness`
/// (else the default harness) with its default model, reasoning effort and context, and otherwise the settings of an ant that sets nothing. Its home and
/// workdir are `~/.ants/messenger`, created when it starts, and its credentials
/// `~/.config/ants/auth/messenger.creds` (where `ants auth user messenger` writes them) if that file exists.
pub fn builtin_messenger(harness: &str, home: &Path) -> Ant {
    let name = UserId::messenger();
    let dir = home.join(".ants").join(name.as_str());
    let creds = home.join(".config/ants/auth").join(format!("{name}.creds"));
    Ant {
        harness: Some(harness.to_owned()),
        home: Some(dir.clone()),
        creds: creds.is_file().then_some(creds),
        builtin: true,
        ..Ant::new(name, dir)
    }
}

/// What a config file lists, checked.
#[derive(Debug)]
pub struct Loaded {
    pub ants: Vec<Ant>,
    /// The harnesses the ants may name.
    pub harnesses: Harnesses,
    /// The templates manager ants may add ants from, workdirs resolved.
    pub templates: Vec<AntTemplate>,
    pub schedules: Vec<Schedule>,
    /// Where to serve health and metrics, if anywhere.
    pub metrics: Option<SocketAddr>,
    /// The file's directory, which relative paths are relative to, and the home directory `~` stands for.
    base: PathBuf,
    home: PathBuf,
}

impl Loaded {
    /// Check that `manager` may add an ant named `name` from `template` as `request` asks: the template exists and
    /// lists the manager among its `managers`, the manager was not itself added from a template, no ant or schedule
    /// has the name, the template has room under its `max_ants`, and the request keeps within the template's bounds
    /// ([`AntTemplate::resolve`]). Returns the ant as the file would list it once its entry ([`template_entry`]) is
    /// added; the error tells the first bound broken. A relative or `~` workdir in the request is resolved as in the
    /// file.
    pub fn check_request(&self, manager: &UserId, name: &UserId, template: &str, request: &Request) -> Result<Ant> {
        let t = self.templates.iter().find(|t| t.name == template).with_context(|| {
            let names: Vec<_> = self.templates.iter().map(|t| t.name.as_str()).collect();
            format!("no ant template {template} (the config file has: {})", list_or_none(&names))
        })?;
        if !t.managers.contains(manager) {
            bail!("{manager} is not among the managers of template {template}");
        }
        if self.ants.iter().any(|a| a.name == *manager && a.template.is_some()) {
            bail!("{manager} was added from a template, and added ants never add ants");
        }
        if self.ants.iter().any(|a| a.name == *name) || self.schedules.iter().any(|s| s.name == *name) {
            bail!("the config file already lists {name}");
        }
        let count = self.ants.iter().filter(|a| a.template.as_deref() == Some(template)).count();
        if count >= t.max_ants {
            bail!("template {template} has {count} ants already, its max_ants");
        }
        let request = Request {
            workdir: request.workdir.as_deref().map(|d| resolve(d, &self.base, &self.home)),
            ..request.clone()
        };
        t.resolve(manager, name, &request)
    }
}

fn list_or_none(items: &[&str]) -> String {
    if items.is_empty() { "none".to_owned() } else { items.join(", ") }
}

/// Read and check the config file at `path`: its ants, the harnesses they may name, and its schedules.
pub fn load(path: &Path) -> Result<Loaded> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    parse(&text, &std::path::absolute(path)?, &home_dir()?)
}

/// Parse the text of the config file at `path`: expand `~` to `home`, resolve relative paths against the file's
/// directory, apply the top-level `harness`, `nats`, `js_domain` and `human`, check that names are unique, workdirs
/// exist and every harness named is defined, add the built-in messenger unless `messenger = false` or an ant is
/// named `messenger`, and check the schedules, each named once and after no ant, with the top-level `nats`,
/// `js_domain` and `timezone` where they set none.
pub fn parse(text: &str, path: &Path, home: &Path) -> Result<Loaded> {
    let mut file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
    let base = path.parent().unwrap_or(Path::new("."));
    let harnesses = define(std::mem::take(&mut file.harnesses), path, home)?;
    if let Some(harness) = &file.harness {
        harnesses.get(harness).with_context(|| format!("config {}", path.display()))?;
    }
    if let Some(url) = &file.cli.nats {
        crate::bus::check_url(url).with_context(|| format!("config {}: [cli]", path.display()))?;
    }
    if file.ants.is_empty() && !file.messenger {
        bail!("config {}: messenger = false and no [[ant]] entries: nothing to serve", path.display());
    }
    if file.messenger && !file.ants.iter().any(|ant| ant.name == UserId::messenger()) {
        file.ants.push(builtin_messenger(file.harness.as_deref().unwrap_or(DEFAULT_HARNESS), home));
    }
    let templates = templates(std::mem::take(&mut file.ant_templates), &harnesses, base, home)
        .with_context(|| format!("config {}", path.display()))?;
    let tables = ant_tables(text)?;
    let mut names = BTreeSet::new();
    let mut ants = Vec::with_capacity(file.ants.len());
    for (i, mut ant) in file.ants.into_iter().enumerate() {
        if !names.insert(ant.name.clone()) {
            bail!("config {}: ant {} is listed twice", path.display(), ant.name);
        }
        if !ant.builtin {
            let table = tables.get(i).context("the [[ant]] entries read differently twice")?;
            ant = from_template(ant, table, &templates, base, home)
                .with_context(|| format!("config {}", path.display()))?;
        }
        harness::validate_env(&ant.env)?;
        ant.validate().with_context(|| format!("config {}", path.display()))?;
        if ant.harness.is_none() {
            ant.harness.clone_from(&file.harness);
        }
        if let Some(harness) = &ant.harness {
            harnesses.get(harness).with_context(|| format!("config {}: ant {}", path.display(), ant.name))?;
        }
        if !ant.builtin {
            let abs = |dir: &Path| resolve(dir, base, home);
            ant.workdir = abs(&ant.workdir).canonicalize().with_context(|| {
                format!("config {}: ant {}: workdir {}", path.display(), ant.name, ant.workdir.display())
            })?;
            ant.home = ant.home.as_deref().map(abs);
            ant.creds = ant.creds.as_deref().map(abs);
        }
        if ant.nats.is_none() {
            ant.nats.clone_from(&file.nats);
        }
        if let Some(url) = &ant.nats {
            crate::bus::check_url(url).with_context(|| format!("config {}: ant {}", path.display(), ant.name))?;
        }
        if ant.js_domain.is_none() {
            ant.js_domain.clone_from(&file.js_domain);
        }
        if ant.human.is_none() {
            ant.human.clone_from(&file.human);
        }
        ants.push(ant);
    }
    let timezone = file
        .timezone
        .as_deref()
        .map(schedule::time_zone)
        .transpose()
        .with_context(|| format!("config {}", path.display()))?;
    check_added(&mut ants, &templates).with_context(|| format!("config {}", path.display()))?;
    let mut schedules: Vec<Schedule> = Vec::with_capacity(file.schedules.len());
    for entry in file.schedules {
        let name = entry.name.clone();
        let mut schedule = Schedule::from_entry(entry, timezone.as_ref(), |p| resolve(p, base, home))
            .with_context(|| format!("config {}: schedule {name}", path.display()))?;
        let name = &schedule.name;
        if schedules.iter().any(|s| s.name == *name) {
            bail!("config {}: schedule {name} is listed twice", path.display());
        }
        if names.contains(name) {
            bail!(
                "config {}: schedule {name} has the name of an ant; a schedule sends as a user of its own",
                path.display()
            );
        }
        if schedule.nats.is_none() {
            schedule.nats.clone_from(&file.nats);
        }
        if schedule.js_domain.is_none() {
            schedule.js_domain.clone_from(&file.js_domain);
        }
        schedules.push(schedule);
    }
    let (base, home) = (base.to_owned(), home.to_owned());
    Ok(Loaded { ants, harnesses, templates, schedules, metrics: file.metrics, base, home })
}

/// The file's `templates`, checked, each named once, their workdirs resolved and the harnesses they list defined.
fn templates(
    mut templates: Vec<AntTemplate>,
    harnesses: &Harnesses,
    base: &Path,
    home: &Path,
) -> Result<Vec<AntTemplate>> {
    let mut names = BTreeSet::new();
    for template in &mut templates {
        if !names.insert(template.name.clone()) {
            bail!("ant template {} is listed twice", template.name);
        }
        template.validate(|dir| resolve(dir, base, home), |name| harnesses.get(name).map(drop))?;
    }
    Ok(templates)
}

/// The `[[ant]]` tables of the file `text`, as written, to tell which keys an entry sets.
fn ant_tables(text: &str) -> Result<Vec<toml::Table>> {
    #[derive(Deserialize)]
    struct Tables {
        #[serde(default, rename = "ant")]
        ants: Vec<toml::Table>,
    }
    Ok(toml::from_str::<Tables>(text)?.ants)
}

/// `ant`, written as `table`, with the settings of its template when it names one: the template's, as the entry's
/// keys of a [`Request`] choose within the template's bounds, with the entry's own credentials and connection. Only
/// those keys may be set, so the template bounds every setting.
fn from_template(ant: Ant, table: &toml::Table, templates: &[AntTemplate], base: &Path, home: &Path) -> Result<Ant> {
    let name = &ant.name;
    let Some(template) = &ant.template else {
        if ant.added_by.is_some() || !ant.allow_sets.is_empty() {
            bail!("ant {name}: added_by and allow_sets are for an ant added from a template; set template too");
        }
        if ant.workdir.as_os_str().is_empty() {
            bail!("ant {name}: workdir is missing");
        }
        return Ok(ant);
    };
    let t = templates
        .iter()
        .find(|t| t.name == *template)
        .with_context(|| format!("ant {name}: no [[ant_template]] named {template}"))?;
    let manager = ant.added_by.as_ref().with_context(|| {
        format!("ant {name}: added from template {template}, so added_by must name the manager ant that added it")
    })?;
    if let Some(key) =
        table.keys().find(|k| !TEMPLATED_KEYS.contains(&k.as_str()) && !REQUEST_KEYS.contains(&k.as_str()))
    {
        bail!(
            "ant {name}: {key} comes from template {template}; an ant added from a template sets only {}, and what \
             the template lets it choose: {}",
            TEMPLATED_KEYS.join(", "),
            REQUEST_KEYS.join(", ")
        );
    }
    let chosen: toml::Table =
        table.iter().filter(|(k, _)| REQUEST_KEYS.contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut request: Request = toml::Value::Table(chosen).try_into().with_context(|| format!("ant {name}"))?;
    request.workdir = request.workdir.map(|dir| resolve(&dir, base, home));
    let resolved = t.resolve(manager, name, &request).with_context(|| format!("ant {name}"))?;
    Ok(Ant { creds: ant.creds, nats: ant.nats, js_domain: ant.js_domain, ..resolved })
}

/// Check the ants added from templates against what the templates allow together: no more of a template's than its
/// `max_ants`, and none a manager, as added ants never add ants. Hold those of a template with pool caps to them.
fn check_added(ants: &mut [Ant], templates: &[AntTemplate]) -> Result<()> {
    let added: BTreeSet<UserId> = ants.iter().filter(|a| a.template.is_some()).map(|a| a.name.clone()).collect();
    for ant in ants.iter() {
        if let Some(manager) = ant.added_by.as_ref().filter(|m| added.contains(*m)) {
            bail!(
                "ant {}: added_by names {manager}, itself added from a template; added ants never add ants",
                ant.name
            );
        }
    }
    for t in templates {
        if let Some(manager) = t.managers.iter().find(|m| added.contains(*m)) {
            bail!("ant template {}: manager {manager} was added from a template; added ants never add ants", t.name);
        }
        let members: Vec<UserId> =
            ants.iter().filter(|a| a.template.as_ref() == Some(&t.name)).map(|a| a.name.clone()).collect();
        if members.len() > t.max_ants {
            bail!(
                "ant template {}: {} ants are added from it, more than its max_ants of {}",
                t.name,
                members.len(),
                t.max_ants
            );
        }
        if t.pooled() {
            let pool = Pool {
                template: t.name.clone(),
                daily_tokens: t.pool_daily_tokens,
                daily_cost: t.pool_daily_cost,
                members: Members::new(members),
            };
            for ant in ants.iter_mut().filter(|a| a.template.as_ref() == Some(&t.name)) {
                ant.pool = Some(pool.clone());
            }
        }
    }
    Ok(())
}

/// The built-in harnesses with the definitions of the config file at `path`, `definitions`, and those of its drop-in
/// files ([`dropin_files`]), their commands given as paths resolved like other paths of their file. A name defined
/// twice, in two drop-in files or in one and the config file, fails naming both files.
fn define(definitions: BTreeMap<String, Harness>, path: &Path, home: &Path) -> Result<Harnesses> {
    let mut files = vec![(path.to_owned(), definitions)];
    let dir = dropin_dir(path);
    for file in dropin_files(&dir).with_context(|| format!("reading harness directory {}", dir.display()))? {
        files.push((file.clone(), read_dropin(&file)?));
    }
    let mut harnesses = Harnesses::builtin();
    let mut defined: BTreeMap<String, &Path> = BTreeMap::new();
    for (file, definitions) in &files {
        for name in definitions.keys() {
            if let Some(first) = defined.insert(name.clone(), file) {
                bail!("harness {name} is defined in both {} and {}", first.display(), file.display());
            }
        }
    }
    for (i, (file, mut definitions)) in files.into_iter().enumerate() {
        let base = file.parent().unwrap_or(Path::new("."));
        for harness in definitions.values_mut() {
            if harness.command_is_path() {
                harness.command = resolve(&harness.command, base, home);
            }
            if i > 0 {
                harness.file = Some(file.clone());
            }
        }
        let what = if i == 0 { "config" } else { "harness file" };
        harnesses = harnesses.define(definitions).with_context(|| format!("{what} {}", file.display()))?;
    }
    Ok(harnesses)
}

/// The directory of drop-in harness files beside the config file at `path`: `harnesses.d`.
pub fn dropin_dir(path: &Path) -> PathBuf {
    path.parent().unwrap_or(Path::new(".")).join("harnesses.d")
}

/// The drop-in harness files in `dir`, sorted by name: its `*.toml` entries other than hidden ones and directories.
/// None when there is no directory.
pub fn dropin_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if !name.starts_with('.') && path.extension().is_some_and(|ext| ext == "toml") && !path.is_dir() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// The `[harnesses.<name>]` tables of the drop-in file at `path`, its only keys.
fn read_dropin(path: &Path) -> Result<BTreeMap<String, Harness>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct DropIn {
        #[serde(default)]
        harnesses: BTreeMap<String, Harness>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading harness file {}", path.display()))?;
    let file: DropIn = toml::from_str(&text).with_context(|| format!("parsing harness file {}", path.display()))?;
    Ok(file.harnesses)
}

fn resolve(path: &Path, base: &Path, home: &Path) -> PathBuf {
    match path.strip_prefix("~") {
        Ok(rest) => home.join(rest),
        Err(_) => base.join(path),
    }
}

/// What adding an ant, running a harness directly or reporting spend needs to know of a config file: its top-level
/// `nats`, `js_domain` and `harness`, its harnesses, and the ants it lists with their credentials, resolved as
/// [`parse`] does, and their spend caps. Nothing else is checked.
#[derive(Debug, Default, PartialEq)]
pub struct Summary {
    pub nats: Option<String>,
    pub js_domain: Option<JsDomain>,
    pub harness: Option<String>,
    pub harnesses: Harnesses,
    pub ants: Vec<(UserId, Option<PathBuf>)>,
    pub caps: BTreeMap<UserId, Caps>,
}

impl Summary {
    /// Read the summary of the file at `path`; an empty one, with the built-in harnesses and those of its drop-in
    /// files, when there is no file.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text, &std::path::absolute(path)?, &home_dir()?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Self::parse("", &std::path::absolute(path)?, &home_dir()?)
            }
            Err(e) => Err(e).with_context(|| format!("reading config {}", path.display())),
        }
    }

    fn parse(text: &str, path: &Path, home: &Path) -> Result<Self> {
        let file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        let harnesses = define(file.harnesses, path, home)?;
        let caps = file.ants.iter().map(|ant| (ant.name.clone(), summary_caps(ant, &file.ant_templates))).collect();
        let ants = file.ants.into_iter().map(|ant| (ant.name, ant.creds.map(|c| resolve(&c, base, home)))).collect();
        Ok(Self { nats: file.nats, js_domain: file.js_domain, harness: file.harness, harnesses, ants, caps })
    }

    /// The harness named `name`, else the file's top-level `harness`, else [`DEFAULT_HARNESS`].
    pub fn harness(&self, name: Option<&str>) -> Result<std::sync::Arc<Harness>> {
        self.harnesses.get(name.or(self.harness.as_deref()).unwrap_or(DEFAULT_HARNESS))
    }

    /// The credentials the file names for `ant`: `None` when it does not list the ant, `Some(None)` when its
    /// entry names none.
    pub fn creds_of(&self, ant: &UserId) -> Option<Option<&Path>> {
        self.ants.iter().find(|(name, _)| name == ant).map(|(_, creds)| creds.as_deref())
    }
}

/// The caps of `ant`: those of its entry, else of its template, if it names one of `templates`.
fn summary_caps(ant: &Ant, templates: &[AntTemplate]) -> Caps {
    let caps = ant.caps();
    match ant.template.as_ref().and_then(|name| templates.iter().find(|t| t.name == *name)) {
        Some(t) => Caps {
            thread_tokens: caps.thread_tokens.or(t.max_thread_tokens),
            thread_cost: caps.thread_cost.or(t.max_thread_cost),
            daily_tokens: caps.daily_tokens.or(t.max_daily_tokens),
            daily_cost: caps.daily_cost.or(t.max_daily_cost),
        },
        None => caps,
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CliSection {
    nats: Option<String>,
    creds: Option<PathBuf>,
    js_domain: Option<JsDomain>,
}

/// What the CLI connects with where neither a flag nor the environment sets it: the `[cli]` section of a config
/// file, else its top-level `nats` and `js_domain`. Credentials come from `[cli]` alone, an ant's being no human's.
#[derive(Debug, Default, PartialEq)]
pub struct CliDefaults {
    pub nats: Option<String>,
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
}

impl CliDefaults {
    /// Read the defaults of the file at `path`; none when there is no file.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text, &std::path::absolute(path)?, &home_dir()?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("reading config {}", path.display())),
        }
    }

    /// Only the keys the CLI uses are read and checked, so that a mistake in the ants' settings does not stop every
    /// command; the daemon refuses it.
    fn parse(text: &str, path: &Path, home: &Path) -> Result<Self> {
        #[derive(Deserialize)]
        struct View {
            nats: Option<String>,
            js_domain: Option<JsDomain>,
            #[serde(default)]
            cli: CliSection,
        }
        let view: View = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        let nats = view.cli.nats.or(view.nats);
        if let Some(url) = &nats {
            crate::bus::check_url(url).with_context(|| format!("config {}", path.display()))?;
        }
        let creds = view.cli.creds.map(|c| resolve(&c, base, home));
        Ok(Self { nats, creds, js_domain: view.cli.js_domain.or(view.js_domain) })
    }
}

/// Add `entry` ([`entry`]) at the end of the config file at `path`, creating the file (and its directory) when
/// missing. The text already there stays as it is, comments included; the file must still parse ([`parse`]) with the
/// entry, or nothing is written.
pub fn append(path: &Path, entry: &str) -> Result<()> {
    let old = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading config {}", path.display())),
    };
    let mut text = old;
    if !text.is_empty() {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push('\n');
    }
    text.push_str(entry);
    parse(&text, &std::path::absolute(path)?, &home_dir()?)?;
    if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    write(path, &text)
}

/// Write `text` to the config file at `path`: beside it first, then renamed over it, so a failure leaves the old file
/// whole.
pub fn write(path: &Path, text: &str) -> Result<()> {
    let mut new = path.as_os_str().to_owned();
    new.push(".new");
    let new = PathBuf::from(new);
    std::fs::write(&new, text).with_context(|| format!("writing {}", new.display()))?;
    std::fs::rename(&new, path).with_context(|| format!("writing config {}", path.display()))
}

/// The text of the config file at `path` with `rule` added to the `allow` list of `ant`'s `[[ant]]` table, or None
/// when the list holds it already. The text around stays as it is, comments included: the rule goes after the list's
/// last item, or a new `allow` line after the table's `name`. Fails, leaving the file alone, when the file does not
/// list the ant or does not parse ([`parse`]) with the rule in place.
pub fn with_allow_rule(path: &Path, ant: &UserId, rule: &str) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct Spans {
        #[serde(default, rename = "ant")]
        ants: Vec<AntSpans>,
    }
    #[derive(Deserialize)]
    struct AntSpans {
        name: toml::Spanned<String>,
        allow: Option<toml::Spanned<Vec<toml::Spanned<String>>>>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let spans: Spans = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    let entry = spans
        .ants
        .into_iter()
        .find(|entry| entry.name.get_ref() == ant.as_str())
        .with_context(|| format!("config {} lists no ant {ant}", path.display()))?;
    let (at, insert) = match &entry.allow {
        Some(allow) if allow.get_ref().iter().any(|r| r.get_ref() == rule) => return Ok(None),
        Some(allow) => match allow.get_ref().last() {
            Some(last) => (last.span().end, format!(", {}", toml_string(rule))),
            // Just inside the `[`.
            None => (allow.span().start + 1, toml_string(rule)),
        },
        None => {
            let end = entry.name.span().end;
            match text[end..].find('\n') {
                Some(newline) => (end + newline + 1, format!("allow = [{}]\n", toml_string(rule))),
                None => (text.len(), format!("\nallow = [{}]\n", toml_string(rule))),
            }
        }
    };
    let mut new = text;
    new.insert_str(at, &insert);
    let loaded = parse(&new, &std::path::absolute(path)?, &home_dir()?)?;
    if !loaded.ants.iter().any(|a| &a.name == ant && a.allow.iter().any(|r| r == rule)) {
        bail!("cannot add {} to the allow list of {ant} in {}: add it by hand", toml_string(rule), path.display());
    }
    Ok(Some(new))
}

/// The template the config file at `path` adds `ant` from, None when the file lists the ant without one. Fails when the
/// file does not list the ant or does not parse as TOML.
pub fn template_of(path: &Path, ant: &UserId) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct Names {
        #[serde(default, rename = "ant")]
        ants: Vec<Named>,
    }
    #[derive(Deserialize)]
    struct Named {
        name: String,
        template: Option<String>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let names: Names = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    let entry = names
        .ants
        .into_iter()
        .find(|entry| entry.name == ant.as_str())
        .with_context(|| format!("config {} lists no ant {ant}", path.display()))?;
    Ok(entry.template)
}

/// The `[[ant_template]]` tables of the config file at `path`, everything else in it ignored, each checked
/// ([`AntTemplate::check`]) and named once, with any harness taken as defined. Their workdirs are resolved as
/// [`parse`] does, and made canonical where they exist here; they need not. For the signer (`ants signer`), which
/// checks requests for ants that run on other machines against its own copy of the templates.
pub fn signer_templates(path: &Path) -> Result<Vec<AntTemplate>> {
    #[derive(Deserialize)]
    struct Templates {
        #[serde(default, rename = "ant_template")]
        templates: Vec<AntTemplate>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let at = || format!("config {}", path.display());
    let mut templates = toml::from_str::<Templates>(&text).with_context(|| format!("parsing {}", at()))?.templates;
    let path = std::path::absolute(path)?;
    let (base, home) = (path.parent().unwrap_or(Path::new(".")), home_dir()?);
    let mut names = BTreeSet::new();
    for template in &mut templates {
        if !names.insert(template.name.clone()) {
            bail!("{}: ant template {} is listed twice", at(), template.name);
        }
        template.check(|_| Ok(())).with_context(at)?;
        for dir in &mut template.workdirs {
            let resolved = resolve(dir, base, &home);
            *dir = resolved.canonicalize().unwrap_or(resolved);
        }
    }
    Ok(templates)
}

/// The `[[ant]]` table of `ant` for the config file: its name, workdir and the settings that differ from those of
/// [`Ant::new`], paths as they are.
pub fn entry(ant: &Ant) -> Result<String> {
    let default = Ant::new(ant.name.clone(), ant.workdir.clone());
    let mut out = String::from("[[ant]]\n");
    let mut set = |key: &str, value: String| {
        let _ = writeln!(out, "{key} = {value}");
    };
    let path = |path: &Path| {
        path.to_str().map(toml_string).with_context(|| format!("ant {}: {} is no UTF-8", ant.name, path.display()))
    };
    let strings =
        |items: &mut dyn Iterator<Item = &str>| format!("[{}]", items.map(toml_string).collect::<Vec<_>>().join(", "));
    set("name", toml_string(ant.name.as_str()));
    let texts = [("nats", &ant.nats), ("model", &ant.model), ("reasoning_effort", &ant.reasoning_effort)];
    for (key, value) in texts.into_iter().chain([("context", &ant.context)]) {
        if let Some(value) = value {
            set(key, toml_string(value));
        }
    }
    if let Some(creds) = &ant.creds {
        set("creds", path(creds)?);
    }
    if let Some(domain) = &ant.js_domain {
        set("js_domain", toml_string(domain.as_str()));
    }
    if let Some(human) = &ant.human {
        set("human", toml_string(human.as_str()));
    }
    set("workdir", path(&ant.workdir)?);
    if let Some(home) = &ant.home {
        set("home", path(home)?);
    }
    for (key, value) in [("whoami", &ant.whoami), ("memory", &ant.memory), ("tasks", &ant.tasks)] {
        if let Some(value) = value {
            set(key, toml_string(value));
        }
    }
    if !ant.env.is_empty() {
        let pairs: Vec<String> =
            ant.env.iter().map(|(k, v)| format!("{} = {}", toml_string(k), toml_string(v))).collect();
        set("env", format!("{{ {} }}", pairs.join(", ")));
    }
    if let Some(harness) = &ant.harness {
        set("harness", toml_string(harness));
    }
    if ant.allow_tools {
        set("allow_tools", "true".to_owned());
    }
    if ant.worktrees {
        set("worktrees", "true".to_owned());
    }
    if !ant.allow.is_empty() {
        set("allow", strings(&mut ant.allow.iter().map(String::as_str)));
    }
    if !ant.capabilities.is_empty() {
        set("capabilities", strings(&mut ant.capabilities.iter().map(String::as_str)));
    }
    if !ant.escalate_to.is_empty() {
        set("escalate_to", strings(&mut ant.escalate_to.iter().map(UserId::as_str)));
    }
    let numbers = [
        ("max_harnesses", ant.max_harnesses as u64, default.max_harnesses as u64),
        ("max_tool_calls", ant.max_tool_calls as u64, default.max_tool_calls as u64),
        ("max_escalation_hops", ant.max_escalation_hops.into(), default.max_escalation_hops.into()),
        ("spend_warn_percent", ant.spend_warn_percent.into(), default.spend_warn_percent.into()),
    ];
    for (key, value, default) in numbers {
        if value != default {
            set(key, value.to_string());
        }
    }
    let durations = [
        ("approval_timeout", ant.approval_timeout, default.approval_timeout),
        ("idle_timeout", ant.idle_timeout, default.idle_timeout),
        ("prompt_timeout", ant.prompt_timeout, default.prompt_timeout),
        ("escalate_after", ant.escalate_after, default.escalate_after),
        ("escalation_reminder", ant.escalation_reminder, default.escalation_reminder),
    ];
    for (key, value, default) in durations {
        if value != default {
            set(key, toml_string(&ants_proto::duration::format(value)));
        }
    }
    let counts = [
        ("history_budget", ant.history_budget.map(|n| n as u64)),
        ("max_thread_tokens", ant.max_thread_tokens),
        ("max_daily_tokens", ant.max_daily_tokens),
    ];
    for (key, value) in counts {
        if let Some(value) = value {
            set(key, value.to_string());
        }
    }
    for (key, value) in [("max_thread_cost", ant.max_thread_cost), ("max_daily_cost", ant.max_daily_cost)] {
        if let Some(value) = value {
            set(key, toml_float(value));
        }
    }
    Ok(out)
}

/// The `[[ant]]` table of an ant named `name` that `manager` adds from `template` as `request` asks, with `creds` as
/// its credentials: those and what the request sets, paths as they are. Its other settings come from the template
/// whenever the file loads.
pub fn template_entry(
    name: &UserId,
    template: &str,
    manager: &UserId,
    creds: Option<&Path>,
    request: &Request,
) -> Result<String> {
    let mut out = String::from("[[ant]]\n");
    let mut set = |key: &str, value: String| {
        let _ = writeln!(out, "{key} = {value}");
    };
    let path = |path: &Path| {
        path.to_str().map(toml_string).with_context(|| format!("ant {name}: {} is no UTF-8", path.display()))
    };
    let strings =
        |items: &[String]| format!("[{}]", items.iter().map(|s| toml_string(s)).collect::<Vec<_>>().join(", "));
    set("name", toml_string(name.as_str()));
    set("template", toml_string(template));
    set("added_by", toml_string(manager.as_str()));
    if let Some(creds) = creds {
        set("creds", path(creds)?);
    }
    if let Some(workdir) = &request.workdir {
        set("workdir", path(workdir)?);
    }
    let texts = [
        ("harness", &request.harness),
        ("model", &request.model),
        ("reasoning_effort", &request.reasoning_effort),
        ("context", &request.context),
        ("whoami", &request.whoami),
        ("memory", &request.memory),
        ("tasks", &request.tasks),
    ];
    for (key, value) in texts {
        if let Some(value) = value {
            set(key, toml_string(value));
        }
    }
    if let Some(capabilities) = &request.capabilities {
        set("capabilities", strings(capabilities));
    }
    if !request.allow_sets.is_empty() {
        set("allow_sets", strings(&request.allow_sets));
    }
    let numbers = [
        ("max_thread_tokens", request.max_thread_tokens),
        ("max_daily_tokens", request.max_daily_tokens),
        ("max_harnesses", request.max_harnesses.map(|n| n as u64)),
        ("max_tool_calls", request.max_tool_calls.map(|n| n as u64)),
    ];
    for (key, value) in numbers {
        if let Some(value) = value {
            set(key, value.to_string());
        }
    }
    for (key, value) in [("max_thread_cost", request.max_thread_cost), ("max_daily_cost", request.max_daily_cost)] {
        if let Some(value) = value {
            set(key, toml_float(value));
        }
    }
    if let Some(timeout) = request.prompt_timeout {
        set("prompt_timeout", toml_string(&ants_proto::duration::format(timeout)));
    }
    Ok(out)
}

/// `s` as a TOML basic string.
pub fn toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push('\t'),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn toml_float(x: f64) -> String {
    if x.is_nan() {
        "nan".to_owned()
    } else if x.is_infinite() {
        if x > 0.0 { "inf" } else { "-inf" }.to_owned()
    } else {
        // Debug always shows a fraction or an exponent, as TOML floats need.
        format!("{x:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    /// Parse as if the file sat in this crate's directory, with the workspace root as home.
    fn parse_here(text: &str) -> Result<Vec<Ant>> {
        Ok(parse_with_harnesses(text)?.0)
    }

    fn parse_with_harnesses(text: &str) -> Result<(Vec<Ant>, Harnesses)> {
        parse_all(text).map(|loaded| (loaded.ants, loaded.harnesses))
    }

    fn parse_all(text: &str) -> Result<Loaded> {
        let dir = manifest_dir();
        parse(text, &dir.join("config.toml"), dir.parent().unwrap().parent().unwrap())
    }

    fn config(ant: Ant) -> Result<Config> {
        ant.into_config("nats://unused", &Harnesses::builtin())
    }

    #[test]
    fn parses_several_ants() {
        let ants = parse_here(
            r#"
            nats = "wss://hub.example/nats"
            js_domain = "hub"
            human = "alice"

            [[ant]]
            name = "dev-a"
            workdir = "~/crates/ants"
            creds = "~/auth/dev-a.creds"
            home = "/srv/ants/dev-a"
            whoami = """Maintain the Rust backend.
Review changes for correctness."""
            memory = "Use the memory MCP server."
            tasks = "Use the tracker CLI."
            env = { TRACKER_DB = "/srv/ants/dev-a/tasks.db" }
            harness = "copilot"
            model = "test-model"
            reasoning_effort = "high"
            context = "long_context"
            history_budget = 131072
            allow = ["read", "search"]
            capabilities = ["rust", "review"]
            max_harnesses = 2
            worktrees = true
            idle_timeout = "1m"
            prompt_timeout = "0"
            max_tool_calls = 50
            max_thread_cost = 2.5
            max_daily_tokens = 1000000
            spend_warn_percent = 90
            escalate_to = ["lead", "dev-b"]
            max_escalation_hops = 2
            escalate_after = "0"
            escalation_reminder = "0"

            [[ant]]
            name = "dev-b"
            nats = "nats://127.0.0.1:4222"
            human = "bob"
            workdir = "src"
            "#,
        )
        .unwrap();
        let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
        let a = &ants[0];
        assert_eq!(a.name.as_str(), "dev-a");
        assert_eq!(a.nats.as_deref(), Some("wss://hub.example/nats"));
        assert_eq!(a.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        assert_eq!(a.human.as_ref().map(UserId::as_str), Some("alice"));
        assert_eq!(a.workdir, root.join("crates/ants").canonicalize().unwrap());
        assert_eq!(a.creds, Some(root.join("auth/dev-a.creds")));
        assert_eq!(a.home, Some(PathBuf::from("/srv/ants/dev-a")));
        assert_eq!((a.allow.len(), a.capabilities.len()), (2, 2));
        assert_eq!((a.max_harnesses, a.idle_timeout), (2, Duration::from_secs(60)));
        let config = a.clone().into_config("nats://unused", &Harnesses::builtin()).unwrap();
        assert_eq!(
            config.model_settings,
            ModelSettings {
                model: Some("test-model".to_owned()),
                reasoning_effort: Some("high".to_owned()),
                context: Some("long_context".to_owned()),
                history_budget: Some(131072),
            }
        );
        assert_eq!(config.whoami.as_deref(), Some("Maintain the Rust backend.\nReview changes for correctness."));
        assert_eq!(
            config.instructions,
            "[ants role dev-a]\nMaintain the Rust backend.\nReview changes for correctness.\n\n\
             [ants memory]\nUse the memory MCP server.\n\n[ants tasks]\nUse the tracker CLI."
        );
        assert_eq!(config.env.get("TRACKER_DB").map(String::as_str), Some("/srv/ants/dev-a/tasks.db"));
        assert_eq!(config.limits, PromptLimits { timeout: None, max_tool_calls: Some(50) }, "0 is no limit");
        let caps = Caps { thread_cost: Some(2.5), daily_tokens: Some(1_000_000), ..Caps::default() };
        assert_eq!(config.caps, caps);
        assert_eq!(config.spend_warn_percent, 90);
        let escalate_to = vec!["lead".parse().unwrap(), "dev-b".parse().unwrap()];
        let chain = Chain { escalate_to, max_hops: 2, after: None, remind: None };
        assert_eq!(config.escalation, chain, "0 waits for the leader for good and never reminds");
        assert!(config.worktrees);

        let b = ants[1].clone();
        assert_eq!(b.nats.as_deref(), Some("nats://127.0.0.1:4222"));
        assert_eq!(b.workdir, manifest_dir().join("src").canonicalize().unwrap());
        let defaults = Ant::new(b.name.clone(), b.workdir.clone());
        assert_eq!(b, Ant { nats: b.nats.clone(), js_domain: b.js_domain.clone(), human: b.human.clone(), ..defaults });

        let config = b.into_config("nats://unused", &Harnesses::builtin()).unwrap();
        assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
        assert_eq!(config.human.as_str(), "bob", "an ant's own default human comes first");
        assert!(config.allow.is_empty(), "rejects every tool request unless the human allows it");
        assert_eq!(config.approval_timeout, crate::approvals::DEFAULT_TIMEOUT);
        assert_eq!(config.idle_timeout, DEFAULT_IDLE_TIMEOUT);
        assert!(!config.worktrees, "threads share the workdir by default");
        assert_eq!(config.limits, PromptLimits::default());
        assert_eq!(config.caps, Caps::default(), "no spend caps unless set");
        assert_eq!(config.spend_warn_percent, DEFAULT_SPEND_WARN_PERCENT);
        assert_eq!(config.escalation, Chain::default(), "no chain: escalations go to the owner");
        assert_eq!(config.escalation.after, Some(DEFAULT_ESCALATE_AFTER));
        assert_eq!(config.escalation.remind, Some(DEFAULT_ESCALATION_REMINDER));
        assert!(config.home.ends_with(".ants/dev-b"));
        assert_eq!(config.whoami, None);
        assert_eq!(config.model_settings, ModelSettings::default());
        assert!(config.instructions.is_empty(), "no memory or task tool by default");
        assert!(config.env.is_empty(), "no tool-specific environment by default");
    }

    #[test]
    fn whoami_memory_and_tasks_are_independent_and_can_be_cleared() {
        let mut ant = Ant::new("dev-a".parse().unwrap(), manifest_dir());
        ant.whoami = Some("Maintain the backend.".to_owned());
        ant.memory = Some("Use notes.".to_owned());
        assert_eq!(ant.prompt_instructions(), "[ants role dev-a]\nMaintain the backend.\n\n[ants memory]\nUse notes.");
        ant.whoami = Some(String::new());
        assert_eq!(ant.prompt_instructions(), "[ants memory]\nUse notes.");
        ant.memory = Some(String::new());
        ant.tasks = Some("Use tickets.".to_owned());
        assert_eq!(ant.prompt_instructions(), "[ants tasks]\nUse tickets.");
        ant.whoami = Some(" \n".to_owned());
        ant.tasks = Some(" \n".to_owned());
        assert!(ant.prompt_instructions().is_empty());
    }

    #[test]
    fn invalid_native_settings_fail_before_starting_an_ant() {
        for (harness, model, effort, context, error) in [
            ("copilot", Some(" "), None, None, "model must not be blank"),
            ("claude", None, Some(""), None, "reasoning_effort must not be blank"),
            ("copilot", None, None, Some("128000"), "harness copilot: context must be default or long_context"),
            ("claude", None, None, Some("1m"), "harness claude: context 1m requires an explicit model"),
            ("claude", Some("smart[1m]"), None, Some("default"), "conflicts with model smart[1m]"),
            ("antigravity", None, None, Some("long_context"), "harness antigravity has no context setting"),
            ("vim", None, None, None, "unknown harness \"vim\""),
        ] {
            let mut ant = Ant::new("dev-a".parse().unwrap(), manifest_dir());
            ant.harness = Some(harness.to_owned());
            ant.model = model.map(str::to_owned);
            ant.reasoning_effort = effort.map(str::to_owned);
            ant.context = context.map(str::to_owned);
            let err = format!("{:#}", config(ant).unwrap_err());
            assert!(err.contains(error), "{err}");
        }
    }

    #[test]
    fn rejects_invalid_environment_without_exposing_values() {
        for (name, value) in [("", "secret"), ("bad=name", "secret"), ("bad\0name", "secret"), ("NAME", "secret\0")] {
            let mut ant = Ant::new("dev-a".parse().unwrap(), manifest_dir());
            ant.env.insert(name.to_owned(), value.to_owned());
            let err = config(ant).unwrap_err().to_string();
            assert!(err.contains("invalid harness environment variable"), "{err}");
            assert!(!err.contains("secret"), "{err}");
        }
        let err = parse_here("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nenv = { 'bad=name' = 'secret' }")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid harness environment variable"), "{err}");
    }

    #[test]
    fn falls_back_to_the_default_server_and_human() {
        let ants = parse_here("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
        assert_eq!((&ants[0].nats, &ants[0].human), (&None, &None));
        let config = ants[0].clone().into_config("nats://leaf:4222", &Harnesses::builtin()).unwrap();
        assert_eq!((config.nats_url.as_str(), config.human), ("nats://leaf:4222", UserId::human()));
        let err = parse_here("human = \"Not A Name\"").unwrap_err();
        assert!(format!("{err:#}").contains("human"), "{err:#}");
    }

    #[test]
    fn metrics_are_off_unless_an_address_is_given() {
        assert_eq!(parse_all("").unwrap().metrics, None);
        let loaded = parse_all("metrics = \"127.0.0.1:9464\"").unwrap();
        assert_eq!(loaded.metrics, Some("127.0.0.1:9464".parse().unwrap()));
        let err = format!("{:#}", parse_all("metrics = \"localhost\"").unwrap_err());
        assert!(err.contains("metrics"), "{err}");
    }

    #[test]
    fn refuses_a_server_with_a_user_and_password() {
        let ant = "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"";
        for text in [format!("nats = \"nats://dev:secret@hub\"\n{ant}"), format!("{ant}\nnats = \"wss://secret@hub\"")]
        {
            let err = format!("{:#}", parse_here(&text).unwrap_err());
            assert!(err.contains("ant dev-a: NATS URL") && err.contains(".creds") && !err.contains("secret"), "{err}");
        }
        let ants = parse_here(ant).unwrap();
        let err = ants[0].clone().into_config("nats://dev:secret@leaf", &Harnesses::builtin()).unwrap_err();
        assert!(format!("{err:#}").contains("ant dev-a: NATS URL nats://***@leaf"), "{err:#}");
    }

    #[test]
    fn the_top_level_harness_is_the_default_one_and_runs_the_builtin_messenger() {
        let ants = parse_here(
            "harness = \"claude\"\njs_domain = \"hub\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nharness = \"antigravity\"",
        )
        .unwrap();
        let harnesses: Vec<_> =
            ants.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref(), ant.builtin)).collect();
        assert_eq!(
            harnesses,
            [
                ("dev-a", Some("claude"), false),
                ("dev-b", Some("antigravity"), false),
                ("messenger", Some("claude"), true),
            ]
        );
        let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
        let messenger = &ants[2];
        let dir = root.join(".ants/messenger");
        assert_eq!((&messenger.workdir, messenger.home.as_ref()), (&dir, Some(&dir)));
        assert_eq!(messenger.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        let stock = Ant {
            harness: Some("claude".to_owned()),
            home: Some(dir.clone()),
            js_domain: messenger.js_domain.clone(),
            builtin: true,
            ..Ant::new(UserId::messenger(), dir)
        };
        assert_eq!(messenger, &stock, "harness defaults for model, reasoning effort and context; stock settings");

        let alone = parse_here("harness = \"antigravity\"").unwrap();
        assert_eq!(alone.len(), 1, "harness alone is a complete file");
        assert_eq!((alone[0].name.as_str(), alone[0].builtin), ("messenger", true));
        assert_eq!(alone[0].harness.as_deref(), Some("antigravity"));
    }

    #[test]
    fn the_builtin_messenger_is_on_unless_turned_off() {
        let empty = parse_here("").unwrap();
        let only: Vec<_> = empty.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref(), ant.builtin)).collect();
        assert_eq!(only, [("messenger", Some("copilot"), true)], "the default harness");

        let ants = parse_here("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
        let names: Vec<_> = ants.iter().map(|ant| ant.name.as_str()).collect();
        assert_eq!(names, ["dev-a", "messenger"]);

        let ants = parse_here("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
        assert_eq!((ants.len(), ants[0].name.as_str()), (1, "dev-a"));

        let err = format!("{:#}", parse_here("messenger = false\nharness = \"claude\"").unwrap_err());
        assert!(err.contains("nothing to serve"), "{err}");
    }

    #[test]
    fn a_listed_messenger_replaces_the_builtin_one() {
        let ants = parse_here("harness = \"claude\"\n[[ant]]\nname = \"messenger\"\nworkdir = \".\"").unwrap();
        assert_eq!(ants.len(), 1);
        assert_eq!((ants[0].harness.as_deref(), ants[0].builtin), (Some("claude"), false));
        assert_eq!(ants[0].workdir, manifest_dir().canonicalize().unwrap());
    }

    #[test]
    fn harnesses_defined_in_the_file_run_ants_and_the_builtin_messenger() {
        let (ants, harnesses) = parse_with_harnesses(
            r#"
            harness = "my-agent"

            [[ant]]
            name = "dev-a"
            workdir = "."
            env = { SHARED = "ant" }

            [[ant]]
            name = "dev-b"
            workdir = "."
            harness = "claude"

            [harnesses.my-agent]
            command = "bin/my-agent-acp"
            args = ["--stdio"]
            env = { SHARED = "definition", MY_AGENT_LOG = "warn" }
            mode = "default"

            [harnesses.home-agent]
            command = "~/bin/agent"

            [harnesses.path-agent]
            command = "agent-acp"

            [harnesses.claude]
            command = "/opt/claude/claude-agent-acp"
            "#,
        )
        .unwrap();
        let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
        let harness = |name: &str| harnesses.get(name).unwrap();
        assert_eq!(harness("my-agent").command, manifest_dir().join("bin/my-agent-acp"), "relative to the file");
        assert_eq!(harness("home-agent").command, root.join("bin/agent"));
        assert_eq!(harness("path-agent").command, PathBuf::from("agent-acp"), "looked up on PATH");
        assert_eq!(harness("claude").command, PathBuf::from("/opt/claude/claude-agent-acp"));
        assert_eq!(harness("claude").mode, None, "a definition replaces the built-in one whole");
        assert_eq!(harness("antigravity"), Harnesses::builtin().get("antigravity").unwrap());

        let named: Vec<_> = ants.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref())).collect();
        assert_eq!(named, [("dev-a", Some("my-agent")), ("dev-b", Some("claude")), ("messenger", Some("my-agent"))]);
        let configs: Vec<_> =
            ants.into_iter().map(|ant| ant.into_config("nats://unused", &harnesses).unwrap()).collect();
        assert_eq!(configs[0].harness, harness("my-agent"));
        assert_eq!(configs[0].harness.mode.as_deref(), Some("default"));
        assert_eq!(configs[1].harness, harness("claude"));
        assert_eq!(configs[2].harness, harness("my-agent"));

        let mut alone = Ant::new("dev-a".parse().unwrap(), manifest_dir());
        alone.harness = Some("my-agent".to_owned());
        let err = config(alone).unwrap_err();
        assert!(format!("{err:#}").contains("ant dev-a: unknown harness \"my-agent\""), "{err:#}");
    }

    #[test]
    fn unknown_harness_references_and_bad_definitions_fail_to_load() {
        let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
        let top = err("harness = \"my-agent\"");
        assert!(top.contains("config.toml") && top.contains("unknown harness \"my-agent\""), "{top}");
        let ant = err("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"my-agent\"");
        assert!(ant.contains("ant dev-a: unknown harness \"my-agent\""), "{ant}");
        let empty = err("[harnesses.my-agent]\ncommand = \"\"");
        assert!(
            empty.contains("config.toml") && empty.contains("harness my-agent: command must not be empty"),
            "{empty}"
        );
        assert!(err("[harnesses.my-agent]\ncommand = \"x\"\nshell = \"sh -c\"").contains("unknown field `shell`"));
        assert!(err("[harnesses.my-agent]\nargs = []").contains("missing field `command`"));
        assert!(err("[harnesses.\"My.Agent\"]\ncommand = \"x\"").contains("use lowercase letters"));
        let summary =
            Summary::parse("[harnesses.my-agent]\ncommand = \"\"", Path::new("/c/config.toml"), Path::new("/h"));
        assert!(format!("{:#}", summary.unwrap_err()).contains("command must not be empty"));
    }

    #[test]
    fn the_summary_resolves_harnesses_for_direct_use() {
        let text = "harness = \"my-agent\"\n[harnesses.my-agent]\ncommand = \"./my-agent\"";
        let summary = Summary::parse(text, Path::new("/c/config.toml"), Path::new("/h")).unwrap();
        assert_eq!(summary.harness(None).unwrap().command, PathBuf::from("/c/./my-agent"));
        assert_eq!(summary.harness(Some("claude")).unwrap().name(), "claude");
        assert!(summary.harness(Some("vim")).is_err());
        assert_eq!(Summary::default().harness(None).unwrap().name(), DEFAULT_HARNESS);
    }

    #[test]
    fn dropin_files_define_harnesses_the_ants_may_name() {
        let dir = ants_test_support::scratch_dir("config-dropins").canonicalize().unwrap();
        let config = dir.join("config.toml");
        let dropins = dir.join("harnesses.d");
        std::fs::create_dir(&dropins).unwrap();
        let text = "messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"codex\"\n\
                    [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nharness = \"mine\"\n\
                    [harnesses.mine]\ncommand = \"mine-acp\"\n";
        std::fs::write(&config, text).unwrap();
        std::fs::write(
            dropins.join("codex.toml"),
            "[harnesses.codex]\ncommand = \"./codex-acp\"\nmode = \"read-only\"",
        )
        .unwrap();
        std::fs::write(dropins.join("claude.toml"), "[harnesses.claude]\ncommand = \"claude-acp\"").unwrap();
        std::fs::write(dropins.join(".hidden.toml"), "not toml").unwrap();
        std::fs::write(dropins.join("notes.txt"), "not toml").unwrap();
        let loaded = load(&config).unwrap();
        let codex = loaded.harnesses.get("codex").unwrap();
        assert_eq!(codex.command, dropins.join("./codex-acp"), "relative to harnesses.d");
        assert_eq!(codex.file(), Some(dropins.join("codex.toml").as_path()));
        assert_eq!(loaded.harnesses.get("claude").unwrap().mode, None, "replaces the built-in one whole");
        assert_eq!(loaded.harnesses.get("mine").unwrap().file(), None);
        let configs: Vec<_> =
            loaded.ants.into_iter().map(|ant| ant.into_config("nats://unused", &loaded.harnesses).unwrap()).collect();
        assert_eq!((configs[0].harness.name(), configs[1].harness.name()), ("codex", "mine"));
        assert_eq!(Summary::load(&config).unwrap().harness(Some("codex")).unwrap(), codex);
        std::fs::remove_file(&config).unwrap();
        assert_eq!(Summary::load(&config).unwrap().harness(Some("codex")).unwrap(), codex, "no config file");

        let err = |file: &str, text: &str| {
            std::fs::write(dropins.join(file), text).unwrap();
            let err = format!("{:#}", Summary::load(&config).unwrap_err());
            std::fs::remove_file(dropins.join(file)).unwrap();
            err
        };
        let twice = err("other.toml", "[harnesses.codex]\ncommand = \"x\"");
        let (first, second) = (dropins.join("codex.toml"), dropins.join("other.toml"));
        assert!(twice.contains(&format!("in both {} and {}", first.display(), second.display())), "{twice}");
        std::fs::write(&config, "[harnesses.codex]\ncommand = \"x\"").unwrap();
        let with_config = format!("{:#}", load(&config).unwrap_err());
        assert!(
            with_config.contains(&format!("in both {} and {}", config.display(), first.display())),
            "{with_config}"
        );
        std::fs::write(&config, "").unwrap();
        let path = dropins.join("bad.toml");
        let unknown = err("bad.toml", "harness = \"codex\"");
        assert!(
            unknown.contains(&path.display().to_string()) && unknown.contains("unknown field `harness`"),
            "{unknown}"
        );
        let field = err("bad.toml", "[harnesses.bad]\ncommand = \"x\"\nshell = \"sh\"");
        assert!(field.contains(&path.display().to_string()) && field.contains("unknown field `shell`"), "{field}");
        let empty = err("bad.toml", "[harnesses.bad]\ncommand = \"\"");
        assert!(empty.contains(&path.display().to_string()) && empty.contains("must not be empty"), "{empty}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_summary_keeps_each_ant_s_caps() {
        let text = "[[ant]]\nname = \"dev-a\"\nworkdir = \"/nowhere\"\nmax_daily_cost = 2.5\nmax_thread_tokens = 900";
        let summary = Summary::parse(text, Path::new("/c/config.toml"), Path::new("/h")).unwrap();
        let caps = Caps { daily_cost: Some(2.5), thread_tokens: Some(900), ..Caps::default() };
        assert_eq!(summary.caps.get(&"dev-a".parse().unwrap()), Some(&caps));
    }

    #[test]
    fn without_a_top_level_harness_ants_get_the_default_one() {
        let ants = parse_here("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
        assert_eq!(ants.len(), 1);
        assert_eq!(ants[0].harness, None);
        let config = config(ants[0].clone()).unwrap();
        assert_eq!((config.harness.name(), config.defer_to_others), ("copilot", false));
    }

    #[test]
    fn the_builtin_messenger_finds_its_credentials_and_creates_its_workdir() {
        let home = ants_test_support::scratch_dir("config-home").canonicalize().unwrap();
        let auth = home.join(".config/ants/auth");
        assert_eq!(builtin_messenger("copilot", &home).creds, None, "no credentials issued");
        std::fs::create_dir_all(&auth).unwrap();
        std::fs::write(auth.join("messenger.creds"), "").unwrap();
        let messenger = builtin_messenger("copilot", &home);
        assert_eq!(messenger.creds, Some(auth.join("messenger.creds")));
        let config = config(messenger).unwrap();
        assert!(config.defer_to_others);
        assert_eq!(config.workdir, home.join(".ants/messenger").canonicalize().unwrap());
        assert_eq!(config.model_settings, ModelSettings::default());
        std::fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn rejects_bad_files() {
        let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
        let twice = err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"");
        assert!(twice.contains("ant dev-a is listed twice"), "{twice}");
        assert!(twice.contains("config.toml"), "{twice}");
        let missing = err("[[ant]]\nname = \"dev-a\"\nworkdir = \"no/such/dir\"");
        assert!(missing.contains("workdir no/such/dir"), "{missing}");
        assert!(err("harness = \"vim\"").contains("unknown harness"));
        assert!(err("provider = \"copilot\"").contains("unknown field `provider`"));
        assert!(err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"vim\"").contains("unknown harness"));
        assert!(err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nworkdirs = 1").contains("unknown field"));
        assert!(err("[[ant]]\nname = \"Dev.A\"\nworkdir = \".\"").contains("parsing config"));
        let chain = |names: &str| err(&format!("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nescalate_to = {names}"));
        assert!(chain("[\"dev-a\"]").contains("escalate_to names the ant itself"));
        let warn = err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nspend_warn_percent = 100");
        assert!(warn.contains("spend_warn_percent must be below 100"), "{warn}");
        assert!(chain("[\"lead\", \"lead\"]").contains("escalate_to lists lead twice"));
        assert!(chain("[\"Lead!\"]").contains("parsing config"));
    }

    #[test]
    fn schedules_take_the_top_level_server_and_unique_names() {
        let loaded = parse_all(
            "nats = \"wss://hub.example/nats\"\njs_domain = \"hub\"\n\
             [[schedule]]\nname = \"nightly\"\ncreds = \"~/auth/nightly.creds\"\ncron = \"0 3 * * *\"\n\
             ant = \"dev-a\"\nowner = \"alice\"\ntext = \"Audit.\"\n\
             [[schedule]]\nname = \"pulse\"\nnats = \"nats://127.0.0.1:4222\"\njs_domain = \"own\"\nevery = \"15m\"\n\
             channel = \"ops\"\nmention = [\"dev-b\"]\ntext = \"Stuck?\"\n",
        )
        .unwrap();
        let [nightly, pulse] = &loaded.schedules[..] else { panic!("two schedules") };
        let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
        assert_eq!(nightly.creds, Some(root.join("auth/nightly.creds")));
        assert_eq!(nightly.nats.as_deref(), Some("wss://hub.example/nats"));
        assert_eq!(nightly.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        assert_eq!(nightly.owner.as_ref().map(UserId::as_str), Some("alice"));
        assert_eq!(pulse.nats.as_deref(), Some("nats://127.0.0.1:4222"));
        assert_eq!(pulse.js_domain.as_ref().map(JsDomain::as_str), Some("own"));
        assert_eq!(pulse.mentions, schedule::Mentions::Listed(vec!["dev-b".parse().unwrap()]));

        let err = |text: &str| format!("{:#}", parse_all(text).unwrap_err());
        let one = "[[schedule]]\nname = \"nightly\"\ncron = \"@daily\"\nant = \"dev-a\"\ntext = \"t\"\n";
        assert!(err(&format!("{one}{one}")).contains("schedule nightly is listed twice"));
        let ant = one.replace("nightly", "messenger");
        assert!(err(&ant).contains("schedule messenger has the name of an ant"));
        let bad = err(&one.replace("@daily", "0 25 * * *"));
        assert!(bad.contains("config.toml: schedule nightly: cron \"0 25 * * *\": hour"), "{bad}");
        let bad = err(&format!("timezone = \"Mars/Olympus_Mons\"\n{one}"));
        assert!(bad.contains("config.toml: timezone \"Mars/Olympus_Mons\": unknown time zone"), "{bad}");
        let bad = err(&format!("{one}timezone = \"Europe/Londn\"\n"));
        assert!(bad.contains("schedule nightly: timezone \"Europe/Londn\": unknown time zone"), "{bad}");

        let zoned = |text: &str| parse_all(text).unwrap().schedules[0].when.to_string();
        assert_eq!(zoned(one), "cron \"@daily\"", "UTC by default");
        let tokyo = format!("timezone = \"Asia/Tokyo\"\n{one}");
        assert_eq!(zoned(&tokyo), "cron \"@daily\" in Asia/Tokyo", "the top-level default");
        assert_eq!(zoned(&format!("{tokyo}timezone = \"UTC\"\n")), "cron \"@daily\"", "the schedule's own");
    }

    #[test]
    fn the_cli_section_overrides_the_top_level_server() {
        let (path, home) = (Path::new("/team/config.toml"), Path::new("/home/alice"));
        let cli = |text: &str| CliDefaults::parse(text, path, home);
        assert_eq!(cli("").unwrap(), CliDefaults::default());
        let top = "nats = \"wss://hub.example/nats\"\njs_domain = \"hub\"\n\
                   [[ant]]\nname = \"dev-a\"\nworkdir = \"/nowhere\"\ncreds = \"dev-a.creds\"\nbogus = 1\n";
        let defaults = cli(top).unwrap();
        assert_eq!(defaults.nats.as_deref(), Some("wss://hub.example/nats"));
        assert_eq!(defaults.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        assert_eq!(defaults.creds, None, "an ant's credentials are not the CLI's");

        let own = cli(&format!("{top}[cli]\nnats = \"nats://127.0.0.1:4222\"\ncreds = \"~/alice.creds\"\n")).unwrap();
        assert_eq!(own.nats.as_deref(), Some("nats://127.0.0.1:4222"));
        assert_eq!(own.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        assert_eq!(own.creds.as_deref(), Some(Path::new("/home/alice/alice.creds")));
        let relative = cli("[cli]\ncreds = \"auth/alice.creds\"\njs_domain = \"own\"").unwrap();
        assert_eq!(relative.creds.as_deref(), Some(Path::new("/team/auth/alice.creds")));
        assert_eq!(relative.js_domain.as_ref().map(JsDomain::as_str), Some("own"));

        for bad in ["[cli]\ncred = \"a.creds\"", "nats = 4222", "nats = \"nats://u:secret@hub\"", "[cli"] {
            let err = format!("{:#}", cli(bad).unwrap_err());
            assert!(err.contains("/team/config.toml") && !err.contains("secret"), "{bad}: {err}");
        }
        assert!(parse_all("[cli]\nnats = \"nats://hub:4222\"\ncreds = \"alice.creds\"").is_ok(), "daemon accepts it");
        assert!(parse_all("[cli]\nharness = \"claude\"").is_err());
    }

    #[test]
    fn durations_need_a_unit() {
        let idle = |line: &str| parse_here(&format!("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n{line}"));
        assert_eq!(idle("idle_timeout = \"10m\"").unwrap()[0].idle_timeout, Duration::from_secs(600));
        for bad in ["idle_timeout_secs = 600", "idle_timeout = 600", "idle_timeout = \"600\""] {
            assert!(idle(bad).is_err(), "{bad} loaded");
        }
        let err = format!("{:#}", idle("escalate_after = \"30\"").unwrap_err());
        assert!(err.contains("a number without a unit"), "{err}");
    }

    #[test]
    fn entries_read_back_as_written() {
        let workdir = manifest_dir().canonicalize().unwrap();
        let plain = Ant::new("dev-a".parse().unwrap(), workdir.clone());
        let text = entry(&plain).unwrap();
        assert_eq!(text, format!("[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\n", workdir.to_str().unwrap()));
        assert_eq!(parse_here(&format!("messenger = false\n{text}")).unwrap(), [plain]);

        let mut full = Ant::new("dev-b".parse().unwrap(), workdir);
        full.nats = Some("wss://hub.example/nats".to_owned());
        full.creds = Some(PathBuf::from("/keys/dev b.creds"));
        full.js_domain = Some("hub".parse().unwrap());
        full.human = Some("alice".parse().unwrap());
        full.home = Some(PathBuf::from("/srv/ants/dev-b"));
        full.whoami = Some("Say \"hi\".\nThen\ttab \\ back\u{7f}.".to_owned());
        full.env.insert("A B".to_owned(), "x=\"y\"".to_owned());
        full.harness = Some("claude".to_owned());
        full.model = Some("m".to_owned());
        full.history_budget = Some(20_000);
        full.allow_tools = true;
        full.allow = vec!["execute:cargo test*".to_owned()];
        full.capabilities = vec!["rust".to_owned(), "nats".to_owned()];
        full.escalate_to = vec!["lead".parse().unwrap()];
        full.max_harnesses = 1;
        full.worktrees = true;
        full.prompt_timeout = Duration::ZERO;
        full.idle_timeout = Duration::from_mins(90);
        full.max_thread_cost = Some(2.5);
        full.max_daily_tokens = Some(1000);
        full.spend_warn_percent = 0;
        let text = entry(&full).unwrap();
        assert!(text.contains("\nidle_timeout = \"1h30m\"\nprompt_timeout = \"0\"\n"), "{text}");
        let read: Vec<_> = parse_here(&text).unwrap().into_iter().filter(|ant| !ant.builtin).collect();
        assert_eq!(read, [full]);
    }

    #[test]
    fn appending_keeps_the_file_as_it_was_and_checks_it() {
        let dir = ants_test_support::scratch_dir("config-append").canonicalize().unwrap();
        let path = dir.join("ants/config.toml");
        let add = |name: &str| append(&path, &entry(&Ant::new(name.parse().unwrap(), dir.clone())).unwrap());
        add("dev-a").unwrap();
        let first = std::fs::read_to_string(&path).unwrap();
        assert!(first.starts_with("[[ant]]\nname = \"dev-a\""), "{first}");

        let old = "# the team\nharness = \"copilot\"\nnats = \"nats://hub:4222\" # the hub\n\n[[ant]]\nname = \"dev-a\"\n\
                   workdir = \".\"\ncreds = \"keys/dev-a.creds\"";
        std::fs::write(&path, old).unwrap();
        add("dev-b").unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.starts_with(&format!("{old}\n\n[[ant]]\nname = \"dev-b\"")), "{text}");
        let names: Vec<_> = load(&path).unwrap().ants.into_iter().map(|ant| ant.name.to_string()).collect();
        assert_eq!(names, ["dev-a", "dev-b", "messenger"]);

        let err = format!("{:#}", add("dev-a").unwrap_err());
        assert!(err.contains("ant dev-a is listed twice"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "left as it was");

        let summary = Summary::load(&path).unwrap();
        assert_eq!(summary.nats.as_deref(), Some("nats://hub:4222"));
        assert_eq!(
            summary.creds_of(&"dev-a".parse().unwrap()),
            Some(Some(dir.join("ants/keys/dev-a.creds").as_path()))
        );
        assert_eq!(summary.creds_of(&"dev-b".parse().unwrap()), Some(None));
        assert_eq!(summary.creds_of(&"dev-c".parse().unwrap()), None);
        assert_eq!(Summary::load(&dir.join("none.toml")).unwrap(), Summary::default());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_allow_rule_goes_into_the_ants_list_and_the_rest_stays() {
        let dir = ants_test_support::scratch_dir("config-allow").canonicalize().unwrap();
        let path = dir.join("config.toml");
        let old = "# the team\nnats = \"nats://hub:4222\"\n\n[[ant]]\nname = \"dev-a\" # first\nworkdir = \".\"\n\n\
                   [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nallow = [\n  \"read\", # reading is fine\n]\n\n\
                   [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nallow = []\n";
        std::fs::write(&path, old).unwrap();
        let add = |ant: &str, rule: &str| {
            let text = with_allow_rule(&path, &ant.parse().unwrap(), rule)?;
            if let Some(text) = &text {
                write(&path, text)?;
            }
            anyhow::Ok(text.is_some())
        };
        assert!(add("dev-a", "execute:cargo test").unwrap());
        assert!(add("dev-b", "edit:/a \"b\"").unwrap());
        assert!(add("dev-c", "read").unwrap());
        assert!(!add("dev-b", "read").unwrap(), "listed already");
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            text,
            old.replace("# first\n", "# first\nallow = [\"execute:cargo test\"]\n")
                .replace("\"read\", #", "\"read\", \"edit:/a \\\"b\\\"\", #")
                .replace("[]", "[\"read\"]")
        );
        let allow: Vec<_> = load(&path).unwrap().ants.into_iter().map(|a| a.allow).collect();
        assert_eq!(allow[..3], [vec!["execute:cargo test"], vec!["read", "edit:/a \"b\""], vec!["read"]]);

        let err = format!("{:#}", add("dev-d", "read").unwrap_err());
        assert!(err.contains("lists no ant dev-d"), "{err}");
        std::fs::write(&path, "ant = [{ name = \"dev-a\", workdir = \".\" }]\n").unwrap();
        assert!(add("dev-a", "read").is_err(), "an inline table is left to edit by hand");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    const TEMPLATE: &str = r#"
        nats = "nats://hub:4222"

        [[ant_template]]
        name = "worker"
        managers = ["lead"]
        max_ants = 2
        workdirs = [".", "src"]
        harnesses = ["copilot", "claude"]
        models = { "gpt-5" = "high" }
        max_daily_cost = 10.0
        pool_daily_cost = 15.0
        allow = ["read"]
        allow_sets = { build = ["execute:cargo build*"] }
        env = { A = "b" }

        [[ant]]
        name = "lead"
        workdir = "."
    "#;

    fn with_ants(entries: &str) -> String {
        format!("{TEMPLATE}\n{entries}")
    }

    #[test]
    fn an_ant_added_from_a_template_takes_its_settings_and_shares_its_pool() {
        let text = with_ants(
            r#"
            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            creds = "a.creds"
            model = "gpt-5"
            reasoning_effort = "medium"
            allow_sets = ["build"]
            max_daily_cost = 4.0

            [[ant]]
            name = "lead-b"
            template = "worker"
            added_by = "lead"
            workdir = "src"
            model = "gpt-5"
        "#,
        );
        let loaded = parse_all(&text).unwrap();
        let a = loaded.ants.iter().find(|a| a.name.as_str() == "lead-a").unwrap();
        assert_eq!(
            (a.workdir.clone(), a.creds.clone()),
            (manifest_dir().canonicalize().unwrap(), Some(manifest_dir().join("a.creds")))
        );
        assert_eq!(
            (a.harness.as_deref(), a.model.as_deref(), a.reasoning_effort.as_deref()),
            (Some("copilot"), Some("gpt-5"), Some("medium"))
        );
        assert_eq!(
            (a.allow.clone(), a.env.len(), a.max_daily_cost),
            (vec!["read".to_owned(), "execute:cargo build*".to_owned()], 1, Some(4.0))
        );
        assert_eq!(
            (a.escalate_to.clone(), a.nats.as_deref()),
            (vec!["lead".parse().unwrap()], Some("nats://hub:4222"))
        );
        let pool = a.pool.clone().unwrap();
        assert_eq!((pool.template.as_str(), pool.daily_cost, pool.daily_tokens), ("worker", Some(15.0), None));
        assert_eq!(pool.members.get(), ["lead-a".parse::<UserId>().unwrap(), "lead-b".parse().unwrap()]);
        let b = loaded.ants.iter().find(|a| a.name.as_str() == "lead-b").unwrap();
        assert_eq!(
            (b.workdir.clone(), b.max_daily_cost),
            (manifest_dir().join("src").canonicalize().unwrap(), Some(10.0))
        );
        assert!(loaded.ants.iter().find(|a| a.name.as_str() == "lead").unwrap().pool.is_none());
        assert_eq!(config(a.clone()).unwrap().pool, Some(pool));

        let summary = Summary::parse(&text, &manifest_dir().join("config.toml"), Path::new("/home")).unwrap();
        assert_eq!(summary.caps[&"lead-b".parse::<UserId>().unwrap()].daily_cost, Some(10.0), "the template's ceiling");
    }

    #[test]
    fn an_ant_added_from_a_template_is_checked_against_it_on_every_load() {
        let entry = r#"
            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            model = "gpt-5"
            max_daily_cost = 8.0
        "#;
        parse_all(&with_ants(entry)).unwrap();
        let tightened = with_ants(entry).replace("max_daily_cost = 10.0", "max_daily_cost = 5.0");
        let err = format!("{:#}", parse_all(&tightened).unwrap_err());
        assert!(err.contains("ant lead-a: max_daily_cost 8 is above 5, the ceiling of template worker"), "{err}");

        for (change, error) in [
            (
                ("model = \"gpt-5\"", "model = \"gpt-5\"\nenv = { B = \"c\" }"),
                "ant lead-a: env comes from template worker",
            ),
            (
                ("model = \"gpt-5\"", "model = \"gpt-5\"\nreasoning_effort = \"max\""),
                "reasoning_effort max is above high",
            ),
            (("template = \"worker\"", "template = \"other\""), "ant lead-a: no [[ant_template]] named other"),
            (("added_by = \"lead\"", ""), "added_by must name the manager ant"),
            (("added_by = \"lead\"", "added_by = \"boss\""), "name lead-a does not fit name_pattern"),
            (("template = \"worker\"", ""), "added_by and allow_sets are for an ant added from a template"),
        ] {
            let err = format!("{:#}", parse_all(&with_ants(&entry.replace(change.0, change.1))).unwrap_err());
            assert!(err.contains(error), "{err}");
        }
        let err = format!(
            "{:#}",
            parse_all(&with_ants(entry).replace("max_ants = 2", "max_ants = 2\nmax_ant = 3")).unwrap_err()
        );
        assert!(err.contains("unknown field `max_ant`"), "{err}");
        let err = format!("{:#}", parse_all(&with_ants(entry).replace("max_ants = 2", "max_ants = 0")).unwrap_err());
        assert!(err.contains("ant template worker: 1 ants are added from it, more than its max_ants of 0"), "{err}");
        let err = format!("{:#}", parse_here("[[ant]]\nname = \"dev-a\"\n").unwrap_err());
        assert!(err.contains("ant dev-a: workdir is missing"), "{err}");
    }

    #[test]
    fn the_signer_reads_only_the_templates_and_checks_requests_against_its_copy() {
        let dir = ants_test_support::scratch_dir("config-signer").canonicalize().unwrap();
        std::fs::create_dir(dir.join("src")).unwrap();
        let path = dir.join("config.toml");
        // Workdirs of the managers' machines need not exist on the signer's, nor harnesses be defined.
        let text =
            with_ants("[[ant]]\nname = \"lead-a\"\ntemplate = \"worker\"\nadded_by = \"lead\"\nmodel = \"gpt-5\"\n")
                .replace("workdirs = [\".\", \"src\"]", "workdirs = [\"src\", \"elsewhere\"]")
                .replace("harnesses = [\"copilot\", \"claude\"]", "harnesses = [\"other\"]");
        std::fs::write(&path, &text).unwrap();
        let templates = signer_templates(&path).unwrap();
        assert_eq!(templates.len(), 1);
        assert_eq!(templates[0].workdirs, [dir.join("src"), dir.join("elsewhere")]);
        let (lead, name): (UserId, UserId) = ("lead".parse().unwrap(), "lead-b".parse().unwrap());
        let request: Request = serde_json::from_str(r#"{"model": "gpt-5", "prompt_timeout": "10m"}"#).unwrap();
        let same = |dir: &Path| Ok(dir.to_owned());
        let ant = templates[0].resolve_with(&lead, &name, &request, same).unwrap();
        assert_eq!((ant.workdir, ant.prompt_timeout), (dir.join("src"), Duration::from_mins(10)));
        let elsewhere = Request { workdir: Some(dir.join("elsewhere")), ..request.clone() };
        templates[0].resolve_with(&lead, &name, &elsewhere, same).unwrap();
        let back: Request = serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(back, request, "a request goes over the wire as JSON");

        assert_eq!(template_of(&path, &"lead-a".parse().unwrap()).unwrap().as_deref(), Some("worker"));
        assert_eq!(template_of(&path, &lead).unwrap(), None);
        assert!(template_of(&path, &name).is_err());
        std::fs::write(
            &path,
            format!("{text}\n{}", TEMPLATE.split("[[ant]]").next().unwrap().replace("nats =", "x =")),
        )
        .unwrap();
        let err = format!("{:#}", signer_templates(&path).unwrap_err());
        assert!(err.contains("ant template worker is listed twice"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn added_ants_never_add_ants() {
        let entry = r#"
            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            model = "gpt-5"
        "#;
        let as_manager = with_ants(entry).replace("managers = [\"lead\"]", "managers = [\"lead\", \"lead-a\"]");
        let err = format!("{:#}", parse_all(&as_manager).unwrap_err());
        assert!(err.contains("manager lead-a was added from a template; added ants never add ants"), "{err}");
        let nested = format!(
            "{}\n{}",
            with_ants(entry),
            entry
                .replace("name = \"lead-a\"", "name = \"lead-a-x\"")
                .replace("added_by = \"lead\"", "added_by = \"lead-a\"")
        );
        let err = format!("{:#}", parse_all(&nested).unwrap_err());
        assert!(err.contains("added_by names lead-a, itself added from a template"), "{err}");
    }

    #[test]
    fn a_request_is_checked_and_its_entry_loads_as_the_ant_checked() {
        let loaded = parse_all(&with_ants("")).unwrap();
        let (lead, name): (UserId, UserId) = ("lead".parse().unwrap(), "lead-docs".parse().unwrap());
        let request = Request {
            workdir: Some("src".into()),
            model: Some("gpt-5".to_owned()),
            reasoning_effort: Some("high".to_owned()),
            harness: Some("claude".to_owned()),
            allow_sets: vec!["build".to_owned()],
            capabilities: Some(vec!["docs".to_owned()]),
            whoami: Some("Write \"docs\".".to_owned()),
            max_daily_cost: Some(2.5),
            max_thread_tokens: Some(500),
            max_harnesses: Some(1),
            prompt_timeout: Some(Duration::from_mins(10)),
            max_tool_calls: Some(40),
            ..Request::default()
        };
        let checked = loaded.check_request(&lead, &name, "worker", &request).unwrap();
        let entry = template_entry(&name, "worker", &lead, Some(Path::new("docs.creds")), &request).unwrap();
        let reloaded = parse_all(&with_ants(&entry)).unwrap();
        let ant = reloaded.ants.into_iter().find(|a| a.name == name).unwrap();
        assert_eq!(
            ant,
            Ant {
                creds: Some(manifest_dir().join("docs.creds")),
                nats: Some("nats://hub:4222".to_owned()),
                pool: ant.pool.clone(),
                ..checked
            }
        );

        for (manager, name, template, error) in [
            ("lead", "lead-docs", "other", "no ant template other (the config file has: worker)"),
            ("boss", "boss-docs", "worker", "boss is not among the managers of template worker"),
            ("lead", "lead", "worker", "the config file already lists lead"),
            ("lead", "dev-docs", "worker", "does not fit name_pattern"),
        ] {
            let err = loaded
                .check_request(&manager.parse().unwrap(), &name.parse().unwrap(), template, &request)
                .unwrap_err();
            assert!(err.to_string().contains(error), "{err}");
        }
        let full =
            parse_all(&format!("{}\n{entry}\n{}", with_ants(""), entry.replace("lead-docs", "lead-more"))).unwrap();
        let err = full.check_request(&lead, &"lead-x".parse().unwrap(), "worker", &request).unwrap_err().to_string();
        assert!(err.contains("template worker has 2 ants already, its max_ants"), "{err}");
        let listed = with_ants(&entry).replace("managers = [\"lead\"]", "managers = [\"lead\", \"lead-docs\"]");
        let err = format!("{:#}", parse_all(&listed).unwrap_err());
        assert!(err.contains("manager lead-docs was added from a template"), "{err}");
    }
}
