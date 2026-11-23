//! The settings an ant is served with ([`super::Config`]).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{BuiltinAnt, JsDomain, UserId};

use crate::escalation::Chain;
use crate::harness::{Harness, ModelSettings, PromptLimits};
use crate::limits::caps::Caps;
use crate::publish::Publish;

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
    /// Built-in ants serve only while holding their colony entry ([`crate::colony::hold`]).
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
    pub(crate) memory_pressure: crate::limits::pressure::Settings,
    /// Trail workers computing at once across the process's runs ([`crate::trail::Conducting::workers`]).
    pub trail_workers: usize,
}

/// Whether each thread works in its own git worktree, and what becomes of its branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorktreeSettings {
    /// Each thread works in its own git worktree of the workdir's repository, under the home ([`crate::worktrees::Worktrees`]); unset,
    /// [`crate::worktrees::per_thread`] decides at start.
    pub per_thread: Option<bool>,
    /// Where a thread's new worktree branch starts ([`crate::worktrees::Worktrees::with_base`]).
    pub base: crate::worktrees::Base,
    /// What becomes of a thread's worktree branch holding new commits ([`crate::publish`]).
    pub(crate) publish: Publish,
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

impl Config {
    /// What the ant's inbox takes of its settings.
    pub(super) fn inbox(&self) -> super::InboxSettings<'_> {
        super::InboxSettings {
            builtin: self.presence.builtin,
            pause: &self.pause,
            drift: &self.drift,
            limits: self.harness.limits,
            home: &self.storage.home,
        }
    }
}
