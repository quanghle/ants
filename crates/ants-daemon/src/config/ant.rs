//! One `[[ant]]` entry of the config file ([`Ant`]): its keys and their defaults, the checks of its own settings, and
//! the daemon config it becomes ([`Ant::into_config`]); also the built-in ants a file serves: the messenger unless told
//! otherwise ([`builtin_messenger`]), and the sage while an ant names it ([`builtin_sage`]).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use super::sage::Sage;
use super::{
    DEFAULT_APPROVAL_TIMEOUT, DEFAULT_ESCALATE_AFTER, DEFAULT_ESCALATION_REMINDER, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_MAX_HARNESSES, DEFAULT_MAX_TOOL_CALLS, DEFAULT_PROMPT_TIMEOUT, DEFAULT_SPEND_WARN_PERCENT, home_dir,
};
use crate::daemon::{
    Config, Connection, HarnessSettings, Policy, Pooling, Presence, Spending, Storage, WorktreeSettings,
};
use crate::escalation::Chain;
use crate::harness::{self, Harness, Harnesses, ModelSettings, PromptLimits};
use crate::limits::caps::{Caps, Pool};
use crate::limits::fallback::{self, Fallback};
use crate::limits::pressure;
use crate::monitoring::storage;
use crate::provider::Setting;
use crate::publish::Publish;
use crate::worktrees::Base;

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
    /// Role instructions shared in the colony and prepended to every prompt.
    pub whoami: Option<String>,
    /// Memory and task instructions prepended to every prompt. Tools are provided by the harness.
    pub memory: Option<String>,
    pub tasks: Option<String>,
    /// Added to the harness's inherited environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// A harness name ([`Harnesses`]); the file's top-level `harness` when absent, which one of them must set.
    pub harness: Option<String>,
    /// The provider it is served with ([`Setting::named`]); the file's top-level `provider` when absent.
    pub provider: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub context: Option<String>,
    /// Bytes of the thread's history a rebuilt session is seeded with, over the harness's and its context's.
    pub history_budget: Option<usize>,
    /// Harnesses and models tried in order while the ant's own harness cannot serve ([`crate::limits::fallback`]):
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
    /// Ants the ant's escalations go to before any human, the first one leading; published in the colony.
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
    /// The ant reviewing it ([`Sage`]); the file's top-level `sage` when absent ([`Ant::reviewed_by`]).
    pub sage: Option<Sage>,
    /// Paused: the daemon takes no new message and starts no new prompt for the ant, its messages waiting queued,
    /// until this is unset again (`ants ant resume`). Changing it alone restarts no harness.
    #[serde(default)]
    pub paused: bool,
    /// The built-in messenger or sage, served only while no other daemon lists an ant of its name in the colony.
    #[serde(skip)]
    pub builtin: bool,
    /// The file's top-level `sage`.
    #[serde(skip)]
    pub file_sage: Sage,
    /// Whether another ant of the file names it as its sage ([`super::sage::settle`]), so it has none of its own.
    #[serde(skip)]
    pub is_sage: bool,
    /// The daily caps it shares with the other ants added from its template, if the template has any.
    #[serde(skip)]
    pub pool: Option<Pool>,
    /// Redact secrets from what it publishes: the file's top-level `redact`.
    #[serde(skip, default = "super::yes")]
    pub redact: bool,
    /// The machine its colony entry names: the file's top-level `machine`; the host name when absent.
    #[serde(skip)]
    pub machine: Option<String>,
    /// The file's `[storage]` table.
    #[serde(skip)]
    pub storage: storage::Settings,
    /// The file's `[memory_pressure]` table.
    #[serde(skip)]
    pub memory_pressure: pressure::Settings,
    /// What `provider` names among the file's providers; sense while it names none.
    #[serde(skip)]
    pub provider_setting: Setting,
}

fn default_max_harnesses() -> usize {
    DEFAULT_MAX_HARNESSES
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
            provider: None,
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
            sage: None,
            paused: false,
            builtin: false,
            file_sage: Sage::default(),
            is_sage: false,
            pool: None,
            redact: true,
            machine: None,
            storage: storage::Settings::default(),
            memory_pressure: pressure::Settings::default(),
            provider_setting: Setting::Sense,
        }
    }

    /// The daemon config, with `default_nats` as the server when none is set and the harness from `harnesses`.
    pub fn into_config(self, default_nats: &str, harnesses: &Harnesses) -> Result<Config> {
        harness::validate_env(&self.env)?;
        self.validate()?;
        let instructions = self.prompt_instructions();
        let caps = self.caps();
        let sage = self.reviewed_by();
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
            ant: self.name,
            connection: Connection { nats_url, creds: self.creds, js_domain: self.js_domain },
            presence: Presence {
                whoami: self.whoami,
                capabilities: self.capabilities,
                machine: self.machine,
                defer_to_others: self.builtin,
            },
            harness: HarnessSettings {
                harness,
                model_settings,
                fallbacks,
                workdir,
                env: self.env,
                instructions,
                limits: PromptLimits {
                    timeout: Some(self.prompt_timeout).filter(|t| !t.is_zero()),
                    max_tool_calls: Some(self.max_tool_calls).filter(|n| *n > 0),
                },
            },
            pooling: Pooling {
                max_harnesses: self.max_harnesses,
                idle_timeout: self.idle_timeout,
                memory_pressure: self.memory_pressure,
            },
            worktrees: WorktreeSettings { per_thread: self.worktrees, base: self.worktree_base, publish: self.publish },
            policy: Policy {
                human: self.human.unwrap_or_else(UserId::human),
                allow: if self.allow_tools { [self.allow, vec!["*".to_owned()]].concat() } else { self.allow },
                approval_timeout: self.approval_timeout,
                escalation: Chain {
                    escalate_to: self.escalate_to,
                    max_hops: self.max_escalation_hops,
                    after: Some(self.escalate_after).filter(|t| !t.is_zero()),
                    remind: Some(self.escalation_reminder).filter(|t| !t.is_zero()),
                    stand_in: self.stand_in,
                },
                redact: self.redact,
                sage,
            },
            spending: Spending { caps, warn_percent: self.spend_warn_percent, pool: self.pool },
            storage: Storage { home, jetstream: self.storage },
            provider: self.provider_setting,
            requester: crate::mcp::Requester::default(),
            pause: crate::pause::Pause::new(self.paused),
            drift: crate::profile::shared::Drift::default(),
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
    pub(super) fn validate(&self) -> Result<()> {
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

    /// The ant reviewing this one: its `sage` setting, else the file's top-level one; none where that is none or
    /// names the ant itself, and for a sage, which has none of its own: a built-in ant, or one another ant of the file
    /// names as its sage.
    pub fn reviewed_by(&self) -> Option<UserId> {
        if self.builtin || self.is_sage {
            return None;
        }
        match self.sage.as_ref().unwrap_or(&self.file_sage) {
            Sage::Ant(sage) if *sage != self.name => Some(sage.clone()),
            _ => None,
        }
    }

    pub(super) fn prompt_instructions(&self) -> String {
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
/// `~/.config/ants/auth/messenger.creds` (where `ants auth user messenger` writes them) if that file exists; on a
/// server taking credentials, a machine without that file leaves it to another ([`super::builtins`]).
pub fn builtin_messenger(harness: Option<&str>, home: &Path) -> Ant {
    builtin(UserId::messenger(), harness, home)
}

/// The ant a file serves while one of its ants names `sage` as its sage and it lists no ant of the name
/// ([`super::sage::settle`]); set up, and left to another machine, as the built-in messenger is ([`builtin_messenger`]),
/// in `~/.ants/sage`, with `~/.config/ants/auth/sage.creds`. Its shared settings, such as its model and reasoning effort, change as any
/// ant's.
pub fn builtin_sage(harness: Option<&str>, home: &Path) -> Ant {
    builtin(UserId::sage(), harness, home)
}

/// Where the credentials of the built-in ant `name` belong: `~/.config/ants/auth/<name>.creds`, `home` standing for
/// `~`.
pub(super) fn builtin_creds(name: &UserId, home: &Path) -> PathBuf {
    home.join(".config/ants/auth").join(format!("{name}.creds"))
}

fn builtin(name: UserId, harness: Option<&str>, home: &Path) -> Ant {
    let dir = home.join(".ants").join(name.as_str());
    let creds = builtin_creds(&name, home);
    Ant {
        harness: harness.map(str::to_owned),
        home: Some(dir.clone()),
        creds: creds.is_file().then_some(creds),
        builtin: true,
        ..Ant::new(name, dir)
    }
}

/// The fallbacks `entries` name for an ant whose own harness is `own` with `settings`: as [`fallback::resolve`] has
/// them, or, of an ant added from a template (`templated`), as [`fallback::inherit`] does.
pub(super) fn fallbacks(
    templated: bool,
    entries: &[String],
    own: &std::sync::Arc<Harness>,
    settings: &ModelSettings,
    harnesses: &Harnesses,
) -> Result<Vec<Fallback>> {
    let resolve = if templated { fallback::inherit } else { fallback::resolve };
    resolve(entries, own, settings, harnesses)
}
