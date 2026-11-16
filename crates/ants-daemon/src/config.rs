//! The daemon config file, `~/.ants/config.toml`: the ants one daemon process serves.
//!
//! ```toml
//! harness = "claude"                # harness of every ant naming none and of the built-in messenger; no default
//! messenger = true                  # serve the built-in messenger (the default)
//! nats = "wss://hub.example/nats"   # default server for every ant
//! js_domain = "hub"                 # JetStream domain of the hub, if it has one; also per ant
//! human = "alice"                   # default human, for escalations of threads with no owner; also per ant
//! metrics = "127.0.0.1:9464"        # serve /healthz and /metrics there (crate::metrics); off without it
//! timezone = "America/New_York"     # default zone of the schedules' cron, else UTC
//! redact = true                     # redact secrets from what the ants publish (the default; crate::redact)
//! machine = "dev-vm1"               # the machine the roster names for the ants, and the host's where [host] and
//!                                   # its credentials name none (default: the host name)
//!
//! [cli]                             # the CLI's own connection, where no flag or environment variable sets it
//! creds = "~/.config/ants/auth/alice.creds"
//! nats = "wss://hub.example/nats"   # default: the top-level nats
//! js_domain = "hub"                 # default: the top-level js_domain
//! auto_archive = "14d"              # archive threads idle this long for the CLI's human (default 0: never)
//! summaries = "on-demand"           # or "ahead": also ask for TL;DRs of the threads shown (needs the next key)
//! summary_daily_tokens = 50000      # most tokens a human's summaries spend a UTC day ahead of time (default 0)
//! summary_ant = "messenger"         # the ant asked for TL;DRs and digests (default: the messenger)
//! inline_images = true              # draw image attachments in `ants chat` (default false: their lines only)
//! theme = "auto"                    # the avatars' colours: "auto" (ask the terminal, the default), "dark" or "light"
//! animate = true                    # move the avatars in `ants chat` with their ants' states (the default)
//!
//! [storage]                         # warnings of JetStream storage filling up (crate::storage)
//! warn_percent = 80                 # warn a human past this share of a store's limit; 0 never
//! max_file_store = "10GB"           # the hub's limits, which its account's JetStream info does not tell
//! max_memory_store = "1GB"
//!
//! [memory_pressure]                 # while memory is low (crate::pressure)
//! min_available = "1GB"             # below this much memory available, or a share such as "10%"; "0" never
//! max_harnesses = 1                 # the most harnesses each ant runs while memory is low
//!
//! [host]                            # this machine's host connection (crate::host); off without credentials
//! creds = "~/.config/ants/auth/host.creds"  # default: that file, else host-<machine>.creds beside it, if either exists
//! machine = "laptop"                # default: the name the credentials carry, else the top-level machine if it is
//!                                   # a machine name (ants_proto::Machine), else the host name's first label
//!
//! [admin]                           # admin orders to this machine's ants (crate::admin)
//! remote = false                    # refuse every order (default true: carry out those an admin signed)
//!
//! [[ant]]
//! name = "dev-a"
//! workdir = "~/src/repo-a"
//! creds = "~/.config/ants/auth/dev-a.creds"
//! escalate_to = ["lead", "dev-b"]   # escalations go to these ants first, lead leading
//! stand_in = "bob"                  # human who also gets its escalations and tool requests while their human is away
//! idle_timeout = "10m"              # durations: 90s, 10m, 1h30m, 0
//! worktrees = true                  # a git worktree per thread, under the ant's home; unset: on for a git
//!                                   # workdir git does not ignore, with max_harnesses above 1
//!                                   # (crate::worktrees::per_thread)
//! worktree_base = "upstream"        # start its branch from the fetched remote default branch, not HEAD
//! publish = "draft-pr"              # push a branch with new commits and open a draft pull request with gh
//! harness = "my-agent"
//! fallback = ["claude", "copilot:gpt-5"]  # tried in order while my-agent cannot serve (crate::fallback)
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

use ants_proto::{JsDomain, Machine, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

pub mod edit;
pub mod registration;

use crate::caps::{Caps, Members, Pool};
use crate::daemon::Config;
use crate::escalation::{self, Chain};
use crate::fallback::{self, Fallback};
use crate::harness::{self, Harness, Harnesses, ModelSettings, PromptLimits};
use crate::pressure;
use crate::publish::Publish;
use crate::schedule::{self, Schedule};
use crate::storage;
use crate::template::{AntTemplate, REQUEST_KEYS, Request};
use crate::worktrees::Base;

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

pub(crate) fn home_dir() -> Result<PathBuf> {
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
    /// Redact secrets from what the file's ants publish ([`crate::redact`]).
    #[serde(default = "yes")]
    redact: bool,
    /// The machine the file's ants are served on, as their roster entries name it; the host name when absent.
    machine: Option<String>,
    /// The warnings of JetStream storage filling up ([`crate::storage`]).
    #[serde(default)]
    storage: storage::Settings,
    /// What the ants do while memory is low ([`crate::pressure`]).
    #[serde(default)]
    memory_pressure: pressure::Settings,
    #[serde(default)]
    cli: CliSection,
    #[serde(default)]
    host: HostSection,
    #[serde(default)]
    admin: AdminSection,
    #[serde(default, rename = "ant_template")]
    ant_templates: Vec<AntTemplate>,
    #[serde(default, rename = "ant")]
    ants: Vec<Ant>,
    #[serde(default, rename = "schedule")]
    schedules: Vec<schedule::Entry>,
    /// Read by `ants signer` only ([`registration::signer_registration`]).
    #[serde(default, rename = "registration")]
    _registration: Option<serde::de::IgnoredAny>,
}

/// The keys of an `[[ant]]` entry added from a template besides those of a [`Request`].
const TEMPLATED_KEYS: [&str; 7] = ["name", "template", "added_by", "creds", "nats", "js_domain", "paused"];

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
    /// A harness name ([`Harnesses`]); the file's top-level `harness` when absent, which one of them must set.
    pub harness: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub context: Option<String>,
    /// Bytes of the thread's history a rebuilt session is seeded with, over the harness's and its context's.
    pub history_budget: Option<usize>,
    /// Harnesses and models tried in order while the ant's own harness cannot serve ([`crate::fallback`]):
    /// `<harness>`, `<harness>:<model>` or `:<model>` for another model of its own harness. Of an ant added from a
    /// template, the template's, less any naming the ant's own harness and model ([`fallback::inherit`]).
    #[serde(default)]
    pub fallback: Vec<String>,
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
    /// Give each thread its own git worktree of the workdir's repository under the home; unset, see
    /// [`crate::worktrees::per_thread`].
    pub worktrees: Option<bool>,
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
    /// The human who also gets the ant's escalations and tool requests while the human they are for is away, in place
    /// of that human's own stand-in.
    pub stand_in: Option<UserId>,
    /// Where a thread's new worktree branch starts.
    #[serde(default)]
    pub worktree_base: Base,
    /// What becomes of a thread's worktree branch holding new commits.
    #[serde(default)]
    pub publish: Publish,
    /// Paused: the daemon takes no new message and starts no new prompt for the ant, its messages waiting queued,
    /// until this is unset again (`ants ant resume`). Changing it alone restarts no harness.
    #[serde(default)]
    pub paused: bool,
    /// The built-in messenger, served only while no other daemon lists a messenger in the roster.
    #[serde(skip)]
    pub builtin: bool,
    /// The daily caps it shares with the other ants added from its template, if the template has any.
    #[serde(skip)]
    pub pool: Option<Pool>,
    /// Redact secrets from what it publishes: the file's top-level `redact`.
    #[serde(skip, default = "yes")]
    pub redact: bool,
    /// The machine its roster entry names: the file's top-level `machine`; the host name when absent.
    #[serde(skip)]
    pub machine: Option<String>,
    /// The file's `[storage]` table.
    #[serde(skip)]
    pub storage: storage::Settings,
    /// The file's `[memory_pressure]` table.
    #[serde(skip)]
    pub memory_pressure: pressure::Settings,
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
            fallback: Vec::new(),
            allow_tools: false,
            allow: Vec::new(),
            approval_timeout: DEFAULT_APPROVAL_TIMEOUT,
            capabilities: Vec::new(),
            max_harnesses: DEFAULT_MAX_HARNESSES,
            worktrees: None,
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
            stand_in: None,
            worktree_base: Base::Head,
            publish: Publish::Off,
            paused: false,
            builtin: false,
            pool: None,
            redact: true,
            machine: None,
            storage: storage::Settings::default(),
            memory_pressure: pressure::Settings::default(),
        }
    }

    /// The daemon config, with `default_nats` as the server when none is set and the harness from `harnesses`.
    pub fn into_config(self, default_nats: &str, harnesses: &Harnesses) -> Result<Config> {
        harness::validate_env(&self.env)?;
        self.validate()?;
        let instructions = self.prompt_instructions();
        let caps = self.caps();
        let harness = harnesses.named(self.harness.as_deref()).with_context(|| format!("ant {}", self.name))?;
        let model_settings = ModelSettings {
            model: self.model,
            reasoning_effort: self.reasoning_effort,
            context: self.context,
            history_budget: self.history_budget,
        };
        model_settings.validate(&harness).with_context(|| format!("ant {}", self.name))?;
        let fallbacks = fallbacks(self.template.is_some(), &self.fallback, &harness, &model_settings, harnesses)
            .with_context(|| format!("ant {}", self.name))?;
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
            fallbacks,
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
            storage: self.storage,
            memory_pressure: self.memory_pressure,
            pool: self.pool,
            escalation: Chain {
                escalate_to: self.escalate_to,
                max_hops: self.max_escalation_hops,
                after: Some(self.escalate_after).filter(|t| !t.is_zero()),
                remind: Some(self.escalation_reminder).filter(|t| !t.is_zero()),
                stand_in: self.stand_in,
            },
            ant: self.name,
            defer_to_others: self.builtin,
            requester: crate::mcp::Requester::default(),
            redact: self.redact,
            machine: self.machine,
            worktree_base: self.worktree_base,
            publish: self.publish,
            pause: crate::pause::Pause::new(self.paused),
            drift: crate::shared::Drift::default(),
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

    /// The escalation chain names other ants, each once, the stand-in is not the ant, and the spend warning comes short
    /// of the caps.
    fn validate(&self) -> Result<()> {
        if self.spend_warn_percent >= 100 {
            bail!("ant {}: spend_warn_percent must be below 100 (0 for no warning)", self.name);
        }
        if self.stand_in.as_ref() == Some(&self.name) {
            bail!("ant {}: stand_in names the ant itself, not a human", self.name);
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

/// The ant a file serves unless it sets `messenger = false` or lists one named `messenger`: the file's `harness`, which
/// it needs, with its default model, reasoning effort and context, and otherwise the settings of an ant that sets
/// nothing. Its home and workdir are `~/.ants/messenger`, created when it starts, and its credentials
/// `~/.config/ants/auth/messenger.creds` (where `ants auth user messenger` writes them) if that file exists.
pub fn builtin_messenger(harness: Option<&str>, home: &Path) -> Ant {
    let name = UserId::messenger();
    let dir = home.join(".ants").join(name.as_str());
    let creds = home.join(".config/ants/auth").join(format!("{name}.creds"));
    Ant {
        harness: harness.map(str::to_owned),
        home: Some(dir.clone()),
        creds: creds.is_file().then_some(creds),
        builtin: true,
        ..Ant::new(name, dir)
    }
}

/// The `[host]` table: the machine's host connection ([`crate::host`]).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostSection {
    creds: Option<PathBuf>,
    machine: Option<Machine>,
}

/// The `[admin]` table: the admin orders to the machine's ants ([`crate::admin`]).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdminSection {
    /// Whether to carry out the orders an admin signed; false refuses every one.
    #[serde(default = "yes")]
    remote: bool,
}

impl Default for AdminSection {
    fn default() -> Self {
        Self { remote: true }
    }
}

/// The machine's host connection a config file describes ([`crate::host`]), made only by a daemon serving the whole
/// file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSettings {
    /// The host credentials (`ants auth host`): the `[host]` table's `creds`, else `~/.config/ants/auth/host.creds`,
    /// else `host-<machine>.creds` there for the table's `machine`, else for the top-level `machine` or the host
    /// name's first label, whichever exists first; None for none.
    pub creds: Option<PathBuf>,
    /// The `[host]` table's `machine`.
    pub machine: Option<Machine>,
    /// The file's top-level `machine`, as written.
    pub top_machine: Option<String>,
    /// The file's top-level `nats` and `js_domain`.
    pub nats: Option<String>,
    pub js_domain: Option<JsDomain>,
    /// The `[admin]` table's `remote`: whether the admin orders to the machine's ants may be carried out.
    pub remote_admin: bool,
}

impl Default for HostSettings {
    fn default() -> Self {
        Self { creds: None, machine: None, top_machine: None, nats: None, js_domain: None, remote_admin: true }
    }
}

impl HostSettings {
    fn resolve(file: &mut File, base: &Path, home: &Path) -> Self {
        let section = std::mem::take(&mut file.host);
        let top_machine = file.machine.clone();
        let fallback = top_machine.as_deref().and_then(|m| m.parse().ok());
        let creds = match section.creds {
            Some(creds) => Some(resolve(&creds, base, home)),
            None => default_host_creds(&home.join(".config/ants/auth"), section.machine.as_ref(), fallback.as_ref()),
        };
        Self {
            creds,
            machine: section.machine,
            top_machine,
            nats: file.nats.clone(),
            js_domain: file.js_domain.clone(),
            remote_admin: file.admin.remote,
        }
    }

    /// The top-level `machine` if it is a machine name ([`Machine`]): the machine's name where neither the `[host]`
    /// table nor the host credentials name one.
    pub fn fallback(&self) -> Option<Machine> {
        self.top_machine.as_deref().and_then(|m| m.parse().ok())
    }

    /// The machine's name, the host credentials left aside: the `[host]` table's `machine`, else [`Self::fallback`],
    /// else the first label of `hostname`.
    pub fn machine_or(&self, hostname: &str) -> Machine {
        self.machine.clone().or_else(|| self.fallback()).unwrap_or_else(|| Machine::from_hostname(hostname))
    }

    /// The machine's name as the host connection takes it ([`crate::host::machine_of`]): the `[host]` table's
    /// `machine`, else the one the host credentials name, else [`Self::machine_or`], which also stands when the
    /// credentials do not load or name another machine than the table.
    pub fn named(&self, hostname: &str) -> Machine {
        let otherwise = self.machine_or(hostname);
        crate::host::machine_of(self.machine.as_ref(), self.creds.as_deref(), otherwise.clone()).unwrap_or(otherwise)
    }
}

/// `host.creds` in the auth directory `dir`, else `host-<machine>.creds` there, `machine` being the one named, else
/// `fallback` or the first label of the host name, whichever's file exists; the host name is looked up only when
/// `dir` holds some host's credentials.
fn default_host_creds(dir: &Path, machine: Option<&Machine>, fallback: Option<&Machine>) -> Option<PathBuf> {
    let file = |name: &str| Some(dir.join(name)).filter(|path| path.is_file());
    let named = |machine: &Machine| file(&format!("{}.creds", ants_proto::host_user(machine)));
    file("host.creds").or_else(|| match machine {
        Some(machine) => named(machine),
        None => {
            let mut entries = std::fs::read_dir(dir).ok()?;
            let any = entries.any(|e| e.is_ok_and(|e| e.file_name().to_string_lossy().starts_with("host-")));
            any.then(|| fallback.and_then(named).or_else(|| named(&Machine::from_hostname(&hostname()?)))).flatten()
        }
    })
}

/// This machine's host name, as `hostname` prints it.
fn hostname() -> Option<String> {
    let out = std::process::Command::new("hostname").output().ok().filter(|out| out.status.success())?;
    Some(String::from_utf8_lossy(&out.stdout).trim().to_owned()).filter(|name| !name.is_empty())
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
    /// The `[storage]` table.
    pub storage: storage::Settings,
    /// The machine's host connection.
    pub host: HostSettings,
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
    file.cli.summary().with_context(|| format!("config {}", path.display()))?;
    file.storage.validate().with_context(|| format!("config {}", path.display()))?;
    file.memory_pressure.validate().with_context(|| format!("config {}", path.display()))?;
    if let Some(machine) = &file.machine {
        check_machine(machine).with_context(|| format!("config {}", path.display()))?;
    }
    let host = HostSettings::resolve(&mut file, base, home);
    if file.ants.is_empty() && !file.messenger {
        bail!("config {}: messenger = false and no [[ant]] entries: nothing to serve", path.display());
    }
    if file.messenger && !file.ants.iter().any(|ant| ant.name == UserId::messenger()) {
        file.ants.push(builtin_messenger(file.harness.as_deref(), home));
    }
    let templates = templates(std::mem::take(&mut file.ant_templates), &harnesses, file.harness.is_some(), base, home)
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
        check_harnesses(&ant, &harnesses).with_context(|| format!("config {}: ant {}", path.display(), ant.name))?;
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
        ant.redact = file.redact;
        ant.machine.clone_from(&file.machine);
        ant.storage = file.storage;
        ant.memory_pressure = file.memory_pressure;
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
    Ok(Loaded { ants, harnesses, templates, schedules, metrics: file.metrics, storage: file.storage, host, base, home })
}

/// The most characters of the top-level `machine`.
const MAX_MACHINE: usize = 64;

/// Check the top-level `machine`: not blank, at most [`MAX_MACHINE`] characters, none of them a control character.
fn check_machine(machine: &str) -> Result<()> {
    if machine.trim().is_empty() {
        bail!("machine is blank");
    }
    if machine.chars().count() > MAX_MACHINE {
        bail!("machine {machine:?} is longer than {MAX_MACHINE} characters");
    }
    if machine.chars().any(char::is_control) {
        bail!("machine {machine:?} holds a control character");
    }
    Ok(())
}

/// Check that `ant` names a harness, and that it and those of its fallbacks are defined.
fn check_harnesses(ant: &Ant, harnesses: &Harnesses) -> Result<()> {
    let own = harnesses.named(ant.harness.as_deref())?;
    let settings = ModelSettings { model: ant.model.clone(), ..ModelSettings::default() };
    fallbacks(ant.template.is_some(), &ant.fallback, &own, &settings, harnesses).map(drop)
}

/// The fallbacks `entries` name for an ant whose own harness is `own` with `settings`: as [`fallback::resolve`] has
/// them, or, of an ant added from a template (`templated`), as [`fallback::inherit`] does.
fn fallbacks(
    templated: bool,
    entries: &[String],
    own: &std::sync::Arc<Harness>,
    settings: &ModelSettings,
    harnesses: &Harnesses,
) -> Result<Vec<Fallback>> {
    let resolve = if templated { fallback::inherit } else { fallback::resolve };
    resolve(entries, own, settings, harnesses)
}

/// The file's `templates`, checked, each named once, their workdirs resolved and the harnesses they list defined;
/// one listing none needs the file's top-level `harness` (`default_harness`).
fn templates(
    mut templates: Vec<AntTemplate>,
    harnesses: &Harnesses,
    default_harness: bool,
    base: &Path,
    home: &Path,
) -> Result<Vec<AntTemplate>> {
    let mut names = BTreeSet::new();
    for template in &mut templates {
        if !names.insert(template.name.clone()) {
            bail!("ant template {} is listed twice", template.name);
        }
        if template.harnesses.is_empty() && !default_harness {
            let lists = format!("ant template {} lists no harnesses", template.name);
            return Err(anyhow::Error::from(harness::Unset).context(lists));
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
    Ok(Ant { creds: ant.creds, nats: ant.nats, js_domain: ant.js_domain, paused: ant.paused, ..resolved })
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

pub(crate) fn resolve(path: &Path, base: &Path, home: &Path) -> PathBuf {
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
    /// The machine's host connection.
    pub host: HostSettings,
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
        let mut file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        let host = HostSettings::resolve(&mut file, base, home);
        let harnesses = define(file.harnesses, path, home)?;
        let caps = file.ants.iter().map(|ant| (ant.name.clone(), summary_caps(ant, &file.ant_templates))).collect();
        let ants = file.ants.into_iter().map(|ant| (ant.name, ant.creds.map(|c| resolve(&c, base, home)))).collect();
        Ok(Self { nats: file.nats, js_domain: file.js_domain, harness: file.harness, harnesses, ants, caps, host })
    }

    /// The harness named `name`, else the file's top-level `harness`; with neither, an [`harness::Unset`] error.
    pub fn harness(&self, name: Option<&str>) -> Result<std::sync::Arc<Harness>> {
        self.harnesses.named(name.or(self.harness.as_deref()))
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
    #[serde(default, deserialize_with = "ants_proto::duration::deserialize")]
    auto_archive: Duration,
    #[serde(default)]
    summaries: Summaries,
    summary_ant: Option<UserId>,
    #[serde(default)]
    summary_daily_tokens: u64,
    #[serde(default)]
    inline_images: bool,
    #[serde(default)]
    theme: Theme,
    animate: Option<bool>,
}

/// The terminal background the avatars' colours are picked for: `[cli]` `theme`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Theme {
    /// Ask the terminal for its background colour (OSC 11), else go by `COLORFGBG`, else dark.
    #[default]
    Auto,
    Dark,
    Light,
}

impl CliSection {
    /// What the section says of catch-up summaries: refused for `ahead` with no daily tokens to spend.
    fn summary(&self) -> Result<SummaryConfig> {
        if self.summaries == Summaries::Ahead && self.summary_daily_tokens == 0 {
            bail!("[cli] summaries = \"ahead\" needs summary_daily_tokens above 0");
        }
        Ok(SummaryConfig {
            mode: self.summaries,
            ant: self.summary_ant.clone().unwrap_or_else(UserId::messenger),
            daily_tokens: self.summary_daily_tokens,
        })
    }
}

/// When the CLI asks for a thread's TL;DR: `[cli]` `summaries`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Summaries {
    /// Only when a human asks for one.
    #[default]
    OnDemand,
    /// Also ahead of time, for each thread shown with entries newer than its TL;DR, within the daily tokens.
    Ahead,
}

/// What `[cli]` says of catch-up summaries: when to ask, which ant to ask, and how many tokens a human's summaries
/// spend a UTC day at most when asked ahead of time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryConfig {
    pub mode: Summaries,
    pub ant: UserId,
    pub daily_tokens: u64,
}

impl Default for SummaryConfig {
    fn default() -> Self {
        Self { mode: Summaries::OnDemand, ant: UserId::messenger(), daily_tokens: 0 }
    }
}

/// What the CLI connects with where neither a flag nor the environment sets it: the `[cli]` section of a config
/// file, else its top-level `nats` and `js_domain`. Credentials come from `[cli]` alone, an ant's being no human's,
/// as do `auto_archive`, the summary keys, `inline_images`, `theme` and `animate`.
#[derive(Debug, PartialEq)]
pub struct CliDefaults {
    pub nats: Option<String>,
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// How long a thread goes without a message before `ants thread list` and `ants dash` take it as archived for
    /// the CLI's human; None (unset or 0) for never.
    pub auto_archive: Option<Duration>,
    /// When and whom to ask for catch-up summaries.
    pub summaries: SummaryConfig,
    /// Whether `ants chat` draws image attachments in the terminal.
    pub inline_images: bool,
    /// The terminal background the avatars' colours are picked for.
    pub theme: Theme,
    /// Whether `ants chat` moves the avatars with their ants' states; still heads otherwise.
    pub animate: bool,
}

impl Default for CliDefaults {
    fn default() -> Self {
        Self {
            nats: None,
            creds: None,
            js_domain: None,
            auto_archive: None,
            summaries: SummaryConfig::default(),
            inline_images: false,
            theme: Theme::Auto,
            animate: true,
        }
    }
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
        let summaries = view.cli.summary().with_context(|| format!("config {}", path.display()))?;
        let nats = view.cli.nats.or(view.nats);
        if let Some(url) = &nats {
            crate::bus::check_url(url).with_context(|| format!("config {}", path.display()))?;
        }
        let creds = view.cli.creds.map(|c| resolve(&c, base, home));
        let auto_archive = Some(view.cli.auto_archive).filter(|d| !d.is_zero());
        let js_domain = view.cli.js_domain.or(view.js_domain);
        let (inline_images, theme, animate) =
            (view.cli.inline_images, view.cli.theme, view.cli.animate.unwrap_or(true));
        Ok(Self { nats, creds, js_domain, auto_archive, summaries, inline_images, theme, animate })
    }
}

/// The text of the config file at `path` (none when missing) with `[cli]` naming the credentials at `creds`, and the
/// server at `nats` unless `[cli]` names one, as `ants login` sets them; None when `[cli]` names credentials already,
/// which stay the CLI's. Returns whether `nats` was set too. The text around stays as it is, comments included: the
/// keys go right after the `[cli]` header, or in a new `[cli]` table at the end. Fails when the file does not parse,
/// or would not with the keys in place.
pub fn with_cli_login(path: &Path, nats: &str, creds: &Path) -> Result<Option<(String, bool)>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading config {}", path.display())),
    };
    cli_login_text(&text, &std::path::absolute(path)?, &home_dir()?, nats, creds)
}

fn cli_login_text(text: &str, path: &Path, home: &Path, nats: &str, creds: &Path) -> Result<Option<(String, bool)>> {
    CliDefaults::parse(text, path, home)?;
    let table: toml::Table = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
    let cli = table.get("cli").and_then(toml::Value::as_table);
    if cli.is_some_and(|cli| cli.contains_key("creds")) {
        return Ok(None);
    }
    let set_nats = !cli.is_some_and(|cli| cli.contains_key("nats"));
    let quoted = |s: &str| serde_json::to_string(s).expect("a string serializes");
    let creds_text = creds.to_str().with_context(|| format!("{} is not UTF-8", creds.display()))?;
    let mut keys = format!("creds = {}\n", quoted(creds_text));
    if set_nats {
        let _ = writeln!(keys, "nats = {}", quoted(nats));
    }
    let is_header = |line: &str| {
        let Some(rest) = line.trim_start().strip_prefix('[') else { return false };
        let Some((inside, after)) = rest.split_once(']') else { return false };
        let after = after.trim_start();
        inside.trim() == "cli" && (after.is_empty() || after.starts_with('#'))
    };
    let mut new = String::with_capacity(text.len() + keys.len() + 8);
    let mut placed = false;
    for line in text.split_inclusive('\n') {
        new.push_str(line);
        if !placed && is_header(line) {
            if !line.ends_with('\n') {
                new.push('\n');
            }
            new.push_str(&keys);
            placed = true;
        }
    }
    if !placed {
        if !new.is_empty() {
            if !new.ends_with('\n') {
                new.push('\n');
            }
            new.push('\n');
        }
        new.push_str("[cli]\n");
        new.push_str(&keys);
    }
    let fail = || format!("cannot set [cli] in config {}: set creds = {creds_text:?} there by hand", path.display());
    let defaults = CliDefaults::parse(&new, path, home).with_context(fail)?;
    let nats_ok = !set_nats || defaults.nats.as_deref() == Some(nats);
    if defaults.creds.as_deref() != Some(creds) || !nats_ok {
        bail!(fail());
    }
    Ok(Some((new, set_nats)))
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

/// Write `text` to the config file at `path`: beside it first, with the old file's permissions, flushed to disk, then
/// renamed over it, so a failure leaves the old file whole.
pub fn write(path: &Path, text: &str) -> Result<()> {
    use std::io::Write as _;
    let mut new = path.as_os_str().to_owned();
    new.push(".new");
    let new = PathBuf::from(new);
    let written = (|| {
        let mut file = std::fs::File::create(&new)?;
        file.write_all(text.as_bytes())?;
        if let Ok(old) = std::fs::metadata(path) {
            file.set_permissions(old.permissions())?;
        }
        file.sync_all()
    })();
    written.with_context(|| format!("writing {}", new.display()))?;
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

/// The text of the config file at `path` without the `[[ant]]` table of `ant`: from its header up to the next table's
/// header, the comment lines right above that one left to it. The text around stays as it is, comments included.
/// Fails, leaving the file alone, when the file does not list the ant, or when the cut would change anything else or
/// leave a file that does not parse ([`parse`]).
pub fn without_ant(path: &Path, ant: &UserId) -> Result<String> {
    #[derive(Deserialize)]
    struct Spans {
        #[serde(default, rename = "ant")]
        ants: Vec<Named>,
    }
    #[derive(Deserialize)]
    struct Named {
        name: toml::Spanned<String>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let spans: Spans = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    let index = spans
        .ants
        .iter()
        .position(|entry| entry.name.get_ref() == ant.as_str())
        .with_context(|| format!("config {} lists no ant {ant}", path.display()))?;
    let at = spans.ants[index].name.span().start;
    let by_hand = || format!("cannot remove the [[ant]] table of {ant} from {}: remove it by hand", path.display());
    let mut lines = Vec::new();
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        lines.push((offset, line));
        offset += line.len();
    }
    let bare = |line: &str| -> String {
        line.split('#').next().unwrap_or_default().chars().filter(|c| !c.is_whitespace()).collect()
    };
    let header = |line: &str| {
        let bare = bare(line);
        bare.starts_with('[') && bare.ends_with(']')
    };
    let first =
        lines.iter().rposition(|(start, line)| *start <= at && bare(line) == "[[ant]]").with_context(by_hand)?;
    let next = lines.iter().skip(first + 1).position(|(_, line)| header(line)).map(|n| first + 1 + n);
    let (mut from, mut to) = (first, next.unwrap_or(lines.len()));
    match next {
        Some(_) => {
            while to > from + 1 && lines[to - 1].1.trim_start().starts_with('#') {
                to -= 1;
            }
        }
        // Last in the file: the blank lines that set it apart go too.
        None => {
            while from > 0 && lines[from - 1].1.trim().is_empty() {
                from -= 1;
            }
        }
    }
    let end = lines.get(to).map_or(text.len(), |(start, _)| *start);
    let new = format!("{}{}", &text[..lines[from].0], &text[end..]);
    let mut expected: toml::Table = toml::from_str(&text)?;
    if let Some(toml::Value::Array(ants)) = expected.get_mut("ant") {
        ants.remove(index);
        if ants.is_empty() {
            expected.remove("ant");
        }
    }
    if toml::from_str::<toml::Table>(&new).ok() != Some(expected) {
        bail!(by_hand());
    }
    parse(&new, &std::path::absolute(path)?, &home_dir()?)?;
    Ok(new)
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
#[allow(clippy::too_many_lines)]
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
    if !ant.fallback.is_empty() {
        set("fallback", strings(&mut ant.fallback.iter().map(String::as_str)));
    }
    if ant.allow_tools {
        set("allow_tools", "true".to_owned());
    }
    if let Some(worktrees) = ant.worktrees {
        set("worktrees", worktrees.to_string());
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
    if let Some(stand_in) = &ant.stand_in {
        set("stand_in", toml_string(stand_in.as_str()));
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
    if ant.worktree_base != default.worktree_base {
        set("worktree_base", toml_string(ant.worktree_base.as_str()));
    }
    if ant.publish != default.publish {
        set("publish", toml_string(ant.publish.as_str()));
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
mod tests;
