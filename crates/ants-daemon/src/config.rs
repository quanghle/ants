//! The daemon config file, `~/.ants/config.toml`: the ants one daemon process serves.
//!
//! ```toml
//! harness = "copilot"               # default harness of every ant and of the built-in messenger
//! messenger = true                  # serve the built-in messenger (the default)
//! nats = "wss://hub.example/nats"   # default server for every ant
//! js_domain = "hub"                 # JetStream domain of the hub, if it has one; also per ant
//! human = "alice"                   # default human, for escalations of threads with no owner; also per ant
//! metrics = "127.0.0.1:9464"        # serve /healthz and /metrics there (crate::metrics); off without it
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
//! A file also serves the built-in messenger ([`builtin_messenger`]) unless it sets `messenger = false` or lists an
//! ant named `messenger`; an empty file is a complete one.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use crate::caps::Caps;
use crate::daemon::Config;
use crate::escalation::{self, Chain};
use crate::harness::{self, DEFAULT_HARNESS, Harness, Harnesses, ModelSettings, PromptLimits};
use crate::schedule::{self, Schedule};

/// Environment variable naming the config file.
pub const ENV: &str = "ANTS_CONFIG";
pub const DEFAULT_MAX_HARNESSES: usize = 4;
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);
pub const DEFAULT_APPROVAL_TIMEOUT: Duration = crate::approvals::DEFAULT_TIMEOUT;
pub const DEFAULT_PROMPT_TIMEOUT: Duration = harness::DEFAULT_PROMPT_TIMEOUT;
pub const DEFAULT_MAX_TOOL_CALLS: usize = harness::DEFAULT_MAX_TOOL_CALLS;
pub const DEFAULT_ESCALATE_AFTER: Duration = escalation::DEFAULT_ESCALATE_AFTER;

/// `~/.ants/config.toml`.
pub fn default_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".ants").join("config.toml"))
}

fn home_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).context("no home directory")?;
    Ok(PathBuf::from(home))
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
    #[serde(default)]
    cli: CliSection,
    #[serde(default, rename = "ant")]
    ants: Vec<Ant>,
    #[serde(default, rename = "schedule")]
    schedules: Vec<schedule::Entry>,
}

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
    pub workdir: PathBuf,
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
    /// Ants the ant's escalations go to before any human, the first one leading; published in the roster.
    #[serde(default)]
    pub escalate_to: Vec<UserId>,
    /// Chain levels an escalation climbs before it goes to the thread's owner.
    #[serde(default = "default_max_escalation_hops")]
    pub max_escalation_hops: u32,
    /// How long an escalation waits for its leader's answer before going one level higher; 0 waits for good.
    #[serde(default = "default_escalate_after", deserialize_with = "ants_proto::duration::deserialize")]
    pub escalate_after: Duration,
    /// The built-in messenger, served only while no other daemon lists a messenger in the roster.
    #[serde(skip)]
    pub builtin: bool,
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

fn default_max_escalation_hops() -> u32 {
    ants_proto::DEFAULT_MAX_ESCALATION_HOPS
}

fn default_escalate_after() -> Duration {
    DEFAULT_ESCALATE_AFTER
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
            home: None,
            whoami: None,
            memory: None,
            tasks: None,
            env: BTreeMap::new(),
            harness: None,
            model: None,
            reasoning_effort: None,
            context: None,
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
            escalate_to: Vec::new(),
            max_escalation_hops: ants_proto::DEFAULT_MAX_ESCALATION_HOPS,
            escalate_after: DEFAULT_ESCALATE_AFTER,
            builtin: false,
        }
    }

    /// The daemon config, with `default_nats` as the server when none is set and the harness from `harnesses`.
    pub fn into_config(self, default_nats: &str, harnesses: &Harnesses) -> Result<Config> {
        harness::validate_env(&self.env)?;
        self.validate_chain()?;
        let instructions = self.prompt_instructions();
        let caps = self.caps();
        let harness = harnesses
            .get(self.harness.as_deref().unwrap_or(DEFAULT_HARNESS))
            .with_context(|| format!("ant {}", self.name))?;
        let model_settings =
            ModelSettings { model: self.model, reasoning_effort: self.reasoning_effort, context: self.context };
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
            escalation: Chain {
                escalate_to: self.escalate_to,
                max_hops: self.max_escalation_hops,
                after: Some(self.escalate_after).filter(|t| !t.is_zero()),
            },
            ant: self.name,
            defer_to_others: self.builtin,
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

    /// The escalation chain names other ants, each once.
    fn validate_chain(&self) -> Result<()> {
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
    pub schedules: Vec<Schedule>,
    /// Where to serve health and metrics, if anywhere.
    pub metrics: Option<SocketAddr>,
}

/// Read and check the config file at `path`: its ants, the harnesses they may name, and its schedules.
pub fn load(path: &Path) -> Result<Loaded> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    parse(&text, &std::path::absolute(path)?, &home_dir()?)
}

/// Parse the text of the config file at `path`: expand `~` to `home`, resolve relative paths against the file's
/// directory, apply the top-level `harness`, `nats`, `js_domain` and `human`, check that names are unique, workdirs
/// exist and every harness named is defined, add the built-in messenger unless `messenger = false` or an ant is
/// named `messenger`, and check the schedules, each named once and after no ant, with the top-level `nats` and
/// `js_domain` where they set none.
pub fn parse(text: &str, path: &Path, home: &Path) -> Result<Loaded> {
    let mut file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
    let base = path.parent().unwrap_or(Path::new("."));
    let harnesses = define(std::mem::take(&mut file.harnesses), base, home)
        .with_context(|| format!("config {}", path.display()))?;
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
    let mut names = BTreeSet::new();
    let mut ants = Vec::with_capacity(file.ants.len());
    for mut ant in file.ants {
        if !names.insert(ant.name.clone()) {
            bail!("config {}: ant {} is listed twice", path.display(), ant.name);
        }
        harness::validate_env(&ant.env)?;
        ant.validate_chain().with_context(|| format!("config {}", path.display()))?;
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
    let mut schedules: Vec<Schedule> = Vec::with_capacity(file.schedules.len());
    for entry in file.schedules {
        let name = entry.name.clone();
        let mut schedule = Schedule::from_entry(entry, |p| resolve(p, base, home))
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
    Ok(Loaded { ants, harnesses, schedules, metrics: file.metrics })
}

/// The built-in harnesses with the file's `definitions`, their commands given as paths resolved like other paths.
fn define(mut definitions: BTreeMap<String, Harness>, base: &Path, home: &Path) -> Result<Harnesses> {
    for harness in definitions.values_mut() {
        if harness.command_is_path() {
            harness.command = resolve(&harness.command, base, home);
        }
    }
    Harnesses::builtin().define(definitions)
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
    /// Read the summary of the file at `path`; an empty one, with the built-in harnesses, when there is no file.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text, &std::path::absolute(path)?, &home_dir()?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("reading config {}", path.display())),
        }
    }

    fn parse(text: &str, path: &Path, home: &Path) -> Result<Self> {
        let file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        let harnesses = define(file.harnesses, base, home).with_context(|| format!("config {}", path.display()))?;
        let caps = file.ants.iter().map(|ant| (ant.name.clone(), ant.caps())).collect();
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
    let mut text = old.clone();
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
    // Written beside the file and renamed over it, so a failure leaves the old file whole.
    let mut new = path.as_os_str().to_owned();
    new.push(".new");
    let new = PathBuf::from(new);
    std::fs::write(&new, text).with_context(|| format!("writing {}", new.display()))?;
    std::fs::rename(&new, path).with_context(|| format!("writing config {}", path.display()))
}

/// The `[[ant]]` table of `ant` for the config file: its name, workdir and the settings that differ from those of
/// [`Ant::new`], paths as they are.
pub fn entry(ant: &Ant) -> Result<String> {
    let default = Ant::new(ant.name.clone(), ant.workdir.clone());
    let mut out = String::from("[[ant]]\n");
    let mut set = |key: &str, value: String| out.push_str(&format!("{key} = {value}\n"));
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
    ];
    for (key, value, default) in durations {
        if value != default {
            set(key, toml_string(&ants_proto::duration::format(value)));
        }
    }
    for (key, value) in [("max_thread_tokens", ant.max_thread_tokens), ("max_daily_tokens", ant.max_daily_tokens)] {
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

/// `s` as a TOML basic string.
fn toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push('\t'),
            c if c.is_control() => out.push_str(&format!("\\u{:04X}", c as u32)),
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
            allow = ["read", "search"]
            capabilities = ["rust", "review"]
            max_harnesses = 2
            worktrees = true
            idle_timeout = "1m"
            prompt_timeout = "0"
            max_tool_calls = 50
            max_thread_cost = 2.5
            max_daily_tokens = 1000000
            escalate_to = ["lead", "dev-b"]
            max_escalation_hops = 2
            escalate_after = "0"

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
        let chain =
            Chain { escalate_to: vec!["lead".parse().unwrap(), "dev-b".parse().unwrap()], max_hops: 2, after: None };
        assert_eq!(config.escalation, chain, "0 waits for the leader for good");
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
        assert_eq!(config.escalation, Chain::default(), "no chain: escalations go to the owner");
        assert_eq!(config.escalation.after, Some(DEFAULT_ESCALATE_AFTER));
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
}
