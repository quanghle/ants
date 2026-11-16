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
//! machine = "dev-vm1"               # the machine the colony names for the ants, and the host's where [host] and
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
//! protocol = "acp"                  # what it speaks on stdio (crate::harness::native)
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
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

mod amend;
mod ant;
mod cli;
mod dropin;
pub mod edit;
mod entry;
mod host;
pub mod registration;
mod settle;
mod summary;
mod templated;

pub use amend::{append, template_of, with_allow_rule, without_ant, write};
pub use ant::{Ant, builtin_messenger};
pub use cli::{CliDefaults, Summaries, SummaryConfig, Theme, with_cli_login};
pub use dropin::{dropin_dir, dropin_files};
pub use entry::{entry, template_entry, toml_string};
pub use host::HostSettings;
pub use summary::Summary;
pub use templated::signer_templates;

use crate::escalation;
use crate::harness::{self, Harness, Harnesses};
use crate::pressure;
use crate::schedule::{self, Schedule};
use crate::storage;
use crate::template::AntTemplate;
use cli::CliSection;
use dropin::define;
use host::{AdminSection, HostSection};
use settle::{check_top_level, schedules, settle_ant};
use templated::{ant_tables, check_added, from_template, templates};

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
    /// The machine the file's ants are served on, as their colony entries name it; the host name when absent.
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

fn yes() -> bool {
    true
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
    check_top_level(&file, &harnesses, path)?;
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
    for (i, mut ant) in std::mem::take(&mut file.ants).into_iter().enumerate() {
        if !names.insert(ant.name.clone()) {
            bail!("config {}: ant {} is listed twice", path.display(), ant.name);
        }
        if !ant.builtin {
            let table = tables.get(i).context("the [[ant]] entries read differently twice")?;
            ant = from_template(ant, table, &templates, base, home)
                .with_context(|| format!("config {}", path.display()))?;
        }
        ants.push(settle_ant(ant, &file, &harnesses, base, home, path)?);
    }
    let timezone = file
        .timezone
        .as_deref()
        .map(schedule::time_zone)
        .transpose()
        .with_context(|| format!("config {}", path.display()))?;
    check_added(&mut ants, &templates).with_context(|| format!("config {}", path.display()))?;
    let entries = std::mem::take(&mut file.schedules);
    let schedules = schedules(entries, &file, timezone.as_ref(), &names, base, home, path)?;
    let (base, home) = (base.to_owned(), home.to_owned());
    Ok(Loaded { ants, harnesses, templates, schedules, metrics: file.metrics, storage: file.storage, host, base, home })
}

pub(crate) fn resolve(path: &Path, base: &Path, home: &Path) -> PathBuf {
    match path.strip_prefix("~") {
        Ok(rest) => home.join(rest),
        Err(_) => base.join(path),
    }
}

#[cfg(test)]
mod tests;
