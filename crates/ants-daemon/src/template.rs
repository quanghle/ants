//! Ant templates, `[[ant_template]]` in the config file: what a manager ant may ask for when it adds a worker ant.
//!
//! ```toml
//! [[ant_template]]
//! name = "worker"
//! managers = ["lead"]                    # the ants that may add ants from it, by exact name; none by default
//! name_pattern = "<manager>-*"           # the default; `*` stands for any text
//! max_ants = 4                           # ants listed from it at once
//! workdirs = ["~/src/repo"]              # allowlist; the first is the default
//! harnesses = ["claude"]                 # allowlist; the first is the default; none: the file's default harness
//! models = { "claude-sonnet-4-5" = "high", "claude-haiku-4-5" = "max" }   # model ID -> highest reasoning effort
//! contexts = ["default"]                 # allowlist; none: the harness default only
//! max_daily_cost = 10.0                  # ceilings, also the defaults: max_thread_tokens/cost, max_daily_tokens/cost,
//! max_harnesses = 2                      # max_harnesses, prompt_timeout, max_tool_calls
//! allow = ["read", "search"]             # rules every ant from it has
//! allow_sets = { build = ["execute:cargo build*", "execute:cargo test*"] }   # named sets a request picks from
//! pool_daily_cost = 40.0                 # all its ants together in a UTC day; also pool_daily_tokens
//! whoami = "Implement what the manager assigns."   # defaults a request may replace: whoami, memory, tasks,
//! capabilities = ["rust"]                # capabilities
//! env = { CARGO_TARGET_DIR = "target" }  # fixed: env, worktrees, worktree_base, publish, idle_timeout,
//! worktrees = true                       # approval_timeout, spend_warn_percent
//! ```
//!
//! An ant added from a template ([`Request`], [`AntTemplate::resolve`]) is an `[[ant]]` entry carrying
//! `template = "<name>"` and `added_by = "<manager>"`, and only the settings the request asked for: the others come
//! from the template each time the file loads, so a reload re-checks the ant against the template as it is then. Its
//! escalations go to its manager first. Added ants never add ants: a manager is listed by a human.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::config::{
    Ant, DEFAULT_APPROVAL_TIMEOUT, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_HARNESSES, DEFAULT_MAX_TOOL_CALLS,
    DEFAULT_PROMPT_TIMEOUT, DEFAULT_SPEND_WARN_PERCENT,
};
use crate::publish::Publish;
use crate::worktrees::Base;

/// What `name_pattern` is unless set: the manager's name, a dash and anything.
pub const DEFAULT_NAME_PATTERN: &str = "<manager>-*";
/// The placeholder of a name pattern for the manager's name.
pub const MANAGER: &str = "<manager>";

/// A reasoning effort, ranked from the least to the most.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl Effort {
    const ALL: [Self; 5] = [Self::Low, Self::Medium, Self::High, Self::Xhigh, Self::Max];

    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

impl fmt::Display for Effort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Effort {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::ALL.into_iter().find(|e| e.as_str() == s).with_context(|| {
            let ranks: Vec<_> = Self::ALL.iter().map(|e| e.as_str()).collect();
            format!("reasoning_effort {s:?} is none of {}", ranks.join(" < "))
        })
    }
}

/// An `[[ant_template]]` entry: the bounds of every ant added from it, and the settings they get.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AntTemplate {
    pub name: String,
    /// The ants that may add ants from it, by exact name.
    #[serde(default)]
    pub managers: Vec<UserId>,
    /// What the names of its ants look like: [`MANAGER`] stands for the manager's name, `*` for any text.
    #[serde(default = "default_name_pattern")]
    pub name_pattern: String,
    /// How many ants the config file may list from it.
    pub max_ants: usize,
    /// The workdirs its ants may have, the first being the default.
    pub workdirs: Vec<PathBuf>,
    /// The harnesses its ants may run, the first being the default; empty: the file's default harness only.
    #[serde(default)]
    pub harnesses: Vec<String>,
    /// The models its ants may run, each with the highest reasoning effort allowed; empty: the harness default only.
    #[serde(default)]
    pub models: BTreeMap<String, Effort>,
    /// The contexts its ants may select; empty: the harness default only.
    #[serde(default)]
    pub contexts: Vec<String>,
    pub max_thread_tokens: Option<u64>,
    pub max_thread_cost: Option<f64>,
    pub max_daily_tokens: Option<u64>,
    pub max_daily_cost: Option<f64>,
    #[serde(default = "default_max_harnesses")]
    pub max_harnesses: usize,
    #[serde(default = "default_prompt_timeout", deserialize_with = "ants_proto::duration::deserialize")]
    pub prompt_timeout: Duration,
    #[serde(default = "default_max_tool_calls")]
    pub max_tool_calls: usize,
    /// Tool rules every ant from it has.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Named sets of tool rules a request may pick.
    #[serde(default)]
    pub allow_sets: BTreeMap<String, Vec<String>>,
    /// Tokens all its ants together may spend in a UTC day.
    pub pool_daily_tokens: Option<u64>,
    /// Cost all its ants together may spend in a UTC day.
    pub pool_daily_cost: Option<f64>,
    pub whoami: Option<String>,
    pub memory: Option<String>,
    pub tasks: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub worktrees: Option<bool>,
    #[serde(default = "default_idle_timeout", deserialize_with = "ants_proto::duration::deserialize")]
    pub idle_timeout: Duration,
    #[serde(default = "default_approval_timeout", deserialize_with = "ants_proto::duration::deserialize")]
    pub approval_timeout: Duration,
    #[serde(default = "default_spend_warn_percent")]
    pub spend_warn_percent: u8,
    #[serde(default)]
    pub worktree_base: Base,
    #[serde(default)]
    pub publish: Publish,
}

fn default_name_pattern() -> String {
    DEFAULT_NAME_PATTERN.to_owned()
}

fn default_max_harnesses() -> usize {
    DEFAULT_MAX_HARNESSES
}

fn default_prompt_timeout() -> Duration {
    DEFAULT_PROMPT_TIMEOUT
}

fn default_max_tool_calls() -> usize {
    DEFAULT_MAX_TOOL_CALLS
}

fn default_idle_timeout() -> Duration {
    DEFAULT_IDLE_TIMEOUT
}

fn default_approval_timeout() -> Duration {
    DEFAULT_APPROVAL_TIMEOUT
}

fn default_spend_warn_percent() -> u8 {
    DEFAULT_SPEND_WARN_PERCENT
}

/// What a manager asks for when it adds an ant from a template: settings within the template's bounds, each left out
/// taking the template's. The same keys, in an `[[ant]]` entry carrying `template`, are what the request asked for.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workdir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub whoami: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tasks: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
    /// Names of the template's `allow_sets`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_sets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_thread_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_thread_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_daily_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_daily_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_harnesses: Option<usize>,
    #[serde(
        default,
        deserialize_with = "optional_duration",
        serialize_with = "format_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub prompt_timeout: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<usize>,
}

/// The keys of a [`Request`].
pub const REQUEST_KEYS: [&str; 17] = [
    "workdir",
    "harness",
    "model",
    "reasoning_effort",
    "context",
    "whoami",
    "memory",
    "tasks",
    "capabilities",
    "allow_sets",
    "max_thread_tokens",
    "max_thread_cost",
    "max_daily_tokens",
    "max_daily_cost",
    "max_harnesses",
    "prompt_timeout",
    "max_tool_calls",
];

fn format_duration<S: Serializer>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error> {
    match duration {
        Some(d) => serializer.serialize_str(&ants_proto::duration::format(*d)),
        None => serializer.serialize_none(),
    }
}

fn optional_duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Duration>, D::Error> {
    ants_proto::duration::deserialize(deserializer).map(Some)
}

impl AntTemplate {
    /// Check the template itself, with its workdirs resolved by `resolve` and made canonical, and every harness it
    /// lists known to `known`.
    pub(crate) fn validate(
        &mut self,
        resolve: impl Fn(&Path) -> PathBuf,
        known: impl Fn(&str) -> Result<()>,
    ) -> Result<()> {
        self.check(known)?;
        let at = || format!("ant template {}", self.name);
        let mut workdirs = Vec::with_capacity(self.workdirs.len());
        for dir in &self.workdirs {
            workdirs.push(resolve(dir).canonicalize().with_context(|| format!("{}: workdir {}", at(), dir.display()))?);
        }
        self.workdirs = workdirs;
        Ok(())
    }

    /// Check the template's own settings, leaving its workdirs as they are, with every harness it lists known to
    /// `known`.
    pub fn check(&self, known: impl Fn(&str) -> Result<()>) -> Result<()> {
        let name = &self.name;
        if name.is_empty()
            || !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        {
            bail!("ant template name {name:?}: use lowercase letters, digits, '-' and '_'");
        }
        let at = || format!("ant template {name}");
        if self.workdirs.is_empty() {
            bail!("{}: workdirs must list at least one directory", at());
        }
        for harness in &self.harnesses {
            known(harness).with_context(at)?;
        }
        if self.name_pattern.trim().is_empty() {
            bail!("{}: name_pattern must not be empty", at());
        }
        if self.spend_warn_percent >= 100 {
            bail!("{}: spend_warn_percent must be below 100 (0 for no warning)", at());
        }
        crate::harness::validate_env(&self.env).with_context(at)?;
        crate::permission::Allowlist::parse(&self.allow).with_context(|| format!("{}: allow", at()))?;
        for (set, rules) in &self.allow_sets {
            crate::permission::Allowlist::parse(rules).with_context(|| format!("{}: allow_sets.{set}", at()))?;
        }
        let mut seen = BTreeSet::new();
        if let Some(twice) = self.managers.iter().find(|m| !seen.insert(*m)) {
            bail!("{}: managers lists {twice} twice", at());
        }
        Ok(())
    }

    /// Whether `name` fits the template's name pattern for ants `manager` adds.
    pub fn name_fits(&self, manager: &UserId, name: &UserId) -> bool {
        glob(&self.name_pattern.replace(MANAGER, manager.as_str()), name.as_str())
    }

    /// The ant `manager` asks for as `request`, named `name`, with the template's settings where the request leaves
    /// them out; refused with the bound the request breaks. Whether the manager may use the template, the name is
    /// free and the template has room is left to [`crate::config::Loaded::check_request`].
    pub fn resolve(&self, manager: &UserId, name: &UserId, request: &Request) -> Result<Ant> {
        self.resolve_with(manager, name, request, |dir| {
            dir.canonicalize().with_context(|| format!("workdir {}", dir.display()))
        })
    }

    /// [`AntTemplate::resolve`], with the workdir a request names made comparable to the template's by `canonical`.
    pub fn resolve_with(
        &self,
        manager: &UserId,
        name: &UserId,
        request: &Request,
        canonical: impl Fn(&Path) -> Result<PathBuf>,
    ) -> Result<Ant> {
        let t = &self.name;
        if !self.name_fits(manager, name) {
            bail!(
                "name {name} does not fit name_pattern {:?} of template {t} for manager {manager}",
                self.name_pattern
            );
        }
        let workdir = match &request.workdir {
            None => self.workdirs[0].clone(),
            Some(dir) => {
                let canonical = canonical(dir)?;
                if !self.workdirs.contains(&canonical) {
                    bail!(
                        "workdir {} is not in the workdirs of template {t} ({})",
                        dir.display(),
                        paths(&self.workdirs)
                    );
                }
                canonical
            }
        };
        let harness = match &request.harness {
            Some(h) if !self.harnesses.contains(h) => {
                if self.harnesses.is_empty() {
                    bail!("template {t} lists no harnesses: its ants run the file's default harness, not {h}");
                }
                bail!("harness {h} is not in the harnesses of template {t} ({})", self.harnesses.join(", "));
            }
            Some(h) => Some(h.clone()),
            None => self.harnesses.first().cloned(),
        };
        let model = match &request.model {
            Some(m) if !self.models.contains_key(m) => {
                if self.models.is_empty() {
                    bail!("template {t} lists no models: its ants run the harness default model, not {m}");
                }
                bail!("model {m} is not in the models of template {t} ({})", self.model_list());
            }
            Some(m) => Some(m.clone()),
            None if self.models.is_empty() => None,
            None => bail!("pick a model of template {t}: {}", self.model_list()),
        };
        if let Some(effort) = &request.reasoning_effort {
            let Some(model) = &model else {
                bail!("template {t} lists no models, so its ants keep the harness default reasoning effort");
            };
            let ceiling = self.models[model];
            if effort.parse::<Effort>()? > ceiling {
                bail!(
                    "reasoning_effort {effort} is above {ceiling}, the highest template {t} allows for model {model}"
                );
            }
        }
        if let Some(context) = &request.context
            && !self.contexts.contains(context)
        {
            if self.contexts.is_empty() {
                bail!("template {t} lists no contexts: its ants keep the harness default context, not {context}");
            }
            bail!("context {context} is not in the contexts of template {t} ({})", self.contexts.join(", "));
        }
        let allow = self.allow_for(request)?;
        let ceiling = Ceiling(t);
        let max_harnesses = ceiling.count("max_harnesses", request.max_harnesses, self.max_harnesses)?;
        if max_harnesses == 0 {
            bail!("max_harnesses must be at least 1");
        }
        Ok(Ant {
            template: Some(t.clone()),
            added_by: Some(manager.clone()),
            allow_sets: request.allow_sets.clone(),
            whoami: request.whoami.clone().or_else(|| self.whoami.clone()),
            memory: request.memory.clone().or_else(|| self.memory.clone()),
            tasks: request.tasks.clone().or_else(|| self.tasks.clone()),
            env: self.env.clone(),
            harness,
            model,
            reasoning_effort: request.reasoning_effort.clone(),
            context: request.context.clone(),
            allow,
            approval_timeout: self.approval_timeout,
            capabilities: request.capabilities.clone().unwrap_or_else(|| self.capabilities.clone()),
            max_harnesses,
            worktrees: self.worktrees,
            idle_timeout: self.idle_timeout,
            prompt_timeout: ceiling.limit("prompt_timeout", request.prompt_timeout, self.prompt_timeout)?,
            max_tool_calls: ceiling.limit("max_tool_calls", request.max_tool_calls, self.max_tool_calls)?,
            max_thread_tokens: ceiling.cap("max_thread_tokens", request.max_thread_tokens, self.max_thread_tokens)?,
            max_thread_cost: ceiling.cap("max_thread_cost", request.max_thread_cost, self.max_thread_cost)?,
            max_daily_tokens: ceiling.cap("max_daily_tokens", request.max_daily_tokens, self.max_daily_tokens)?,
            max_daily_cost: ceiling.cap("max_daily_cost", request.max_daily_cost, self.max_daily_cost)?,
            spend_warn_percent: self.spend_warn_percent,
            worktree_base: self.worktree_base,
            publish: self.publish,
            escalate_to: vec![manager.clone()],
            ..Ant::new(name.clone(), workdir)
        })
    }

    /// The template's allow rules, with those of the allow sets `request` names.
    fn allow_for(&self, request: &Request) -> Result<Vec<String>> {
        let t = &self.name;
        let mut allow = self.allow.clone();
        for set in &request.allow_sets {
            let rules = self.allow_sets.get(set).with_context(|| {
                let sets: Vec<_> = self.allow_sets.keys().map(String::as_str).collect();
                format!("template {t} has no allow set {set} (it has: {})", list(&sets))
            })?;
            allow.extend(rules.iter().filter(|r| !allow.contains(r)).cloned().collect::<Vec<_>>());
        }
        Ok(allow)
    }

    fn model_list(&self) -> String {
        let models: Vec<_> = self.models.iter().map(|(m, e)| format!("{m} up to {e}")).collect();
        models.join(", ")
    }

    /// Whether it bounds what its ants spend together.
    pub fn pooled(&self) -> bool {
        self.pool_daily_tokens.is_some() || self.pool_daily_cost.is_some()
    }
}

/// The ceilings of template `.0`: a request may ask for less than each, never more.
struct Ceiling<'a>(&'a str);

impl Ceiling<'_> {
    fn above<T: fmt::Display>(&self, key: &str, asked: T, ceiling: T) -> anyhow::Error {
        anyhow::anyhow!("{key} {asked} is above {ceiling}, the ceiling of template {}", self.0)
    }

    /// A spend cap: none in the template is no ceiling.
    fn cap<T: PartialOrd + Copy + fmt::Display>(
        &self,
        key: &str,
        asked: Option<T>,
        ceiling: Option<T>,
    ) -> Result<Option<T>> {
        match (asked, ceiling) {
            (Some(a), Some(c)) if a > c => Err(self.above(key, a, c)),
            (asked, ceiling) => Ok(asked.or(ceiling)),
        }
    }

    fn count(&self, key: &str, asked: Option<usize>, ceiling: usize) -> Result<usize> {
        match asked {
            Some(a) if a > ceiling => Err(self.above(key, a, ceiling)),
            asked => Ok(asked.unwrap_or(ceiling)),
        }
    }

    /// A prompt limit, where 0 is none: the template's 0 lets a request ask for anything, else it must ask for a
    /// limit no higher.
    fn limit<T: Limit>(&self, key: &str, asked: Option<T>, ceiling: T) -> Result<T> {
        match asked {
            Some(a) if !ceiling.unlimited() && (a.unlimited() || a > ceiling) => Err(anyhow::anyhow!(
                "{key} {} is above {}, the ceiling of template {}",
                a.show(),
                ceiling.show(),
                self.0
            )),
            asked => Ok(asked.unwrap_or(ceiling)),
        }
    }
}

trait Limit: PartialOrd + Copy {
    fn unlimited(&self) -> bool;
    fn show(&self) -> String;
}

impl Limit for usize {
    fn unlimited(&self) -> bool {
        *self == 0
    }

    fn show(&self) -> String {
        if *self == 0 { "0 (no limit)".to_owned() } else { self.to_string() }
    }
}

impl Limit for Duration {
    fn unlimited(&self) -> bool {
        self.is_zero()
    }

    fn show(&self) -> String {
        if self.is_zero() { "0 (no limit)".to_owned() } else { ants_proto::duration::format(*self) }
    }
}

fn paths(dirs: &[PathBuf]) -> String {
    dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(", ")
}

fn list(items: &[&str]) -> String {
    if items.is_empty() { "none".to_owned() } else { items.join(", ") }
}

/// Whether `text` matches `pattern`, where `*` stands for any text, and every other character for itself.
fn glob(pattern: &str, text: &str) -> bool {
    let (p, t) = (pattern.as_bytes(), text.as_bytes());
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == b'*' {
            star = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&b| b == b'*')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(name: &str) -> UserId {
        name.parse().unwrap()
    }

    fn dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).canonicalize().unwrap()
    }

    fn template() -> AntTemplate {
        let text = r#"
            name = "worker"
            managers = ["lead"]
            max_ants = 2
            workdirs = ["."]
            harnesses = ["claude", "copilot"]
            models = { "sonnet" = "high", "haiku" = "max" }
            contexts = ["default"]
            max_daily_cost = 10.0
            max_thread_tokens = 1000
            max_harnesses = 2
            prompt_timeout = "20m"
            max_tool_calls = 0
            allow = ["read"]
            allow_sets = { build = ["execute:cargo build*", "read"] }
            whoami = "Work."
            env = { A = "b" }
            worktrees = true
        "#;
        let mut t: AntTemplate = toml::from_str(text).unwrap();
        t.validate(|p| dir().join(p), |_| Ok(())).unwrap();
        t
    }

    #[test]
    fn efforts_rank_and_unknown_ones_are_refused() {
        assert!(Effort::Low < Effort::Medium && Effort::High < Effort::Xhigh && Effort::Xhigh < Effort::Max);
        let err = "turbo".parse::<Effort>().unwrap_err().to_string();
        assert!(err.contains("low < medium < high < xhigh < max"), "{err}");
    }

    #[test]
    fn names_fit_the_pattern_with_the_manager_in_it() {
        let t = template();
        assert!(t.name_fits(&id("lead"), &id("lead-rust")));
        assert!(!t.name_fits(&id("lead"), &id("other-rust")));
        assert!(!t.name_fits(&id("lead"), &id("leadx")));
        assert!(glob("a*b*c", "aXbYc") && glob("*", "") && !glob("a*b", "ab-") && glob("a**", "a"));
    }

    #[test]
    fn a_request_left_empty_takes_the_template_s_defaults_and_ceilings() {
        let t = template();
        let request = Request { model: Some("sonnet".to_owned()), ..Request::default() };
        let ant = t.resolve(&id("lead"), &id("lead-a"), &request).unwrap();
        assert_eq!((ant.template.as_deref(), ant.added_by.as_ref()), (Some("worker"), Some(&id("lead"))));
        assert_eq!((ant.workdir, ant.harness.as_deref()), (dir(), Some("claude")));
        assert_eq!((ant.max_daily_cost, ant.max_thread_tokens, ant.max_thread_cost), (Some(10.0), Some(1000), None));
        assert_eq!((ant.max_harnesses, ant.max_tool_calls), (2, 0));
        assert_eq!(ant.prompt_timeout, Duration::from_mins(20));
        assert_eq!((ant.allow, ant.escalate_to), (vec!["read".to_owned()], vec![id("lead")]));
        assert_eq!((ant.whoami.as_deref(), ant.worktrees, ant.env.len()), (Some("Work."), Some(true), 1));
    }

    #[test]
    fn a_request_may_ask_for_less_never_more() {
        let t = template();
        let less = Request {
            harness: Some("copilot".to_owned()),
            model: Some("haiku".to_owned()),
            reasoning_effort: Some("max".to_owned()),
            context: Some("default".to_owned()),
            max_daily_cost: Some(2.5),
            max_thread_cost: Some(99.0),
            max_harnesses: Some(1),
            prompt_timeout: Some(Duration::from_mins(5)),
            max_tool_calls: Some(50),
            allow_sets: vec!["build".to_owned()],
            whoami: Some("Review.".to_owned()),
            ..Request::default()
        };
        let ant = t.resolve(&id("lead"), &id("lead-b"), &less).unwrap();
        assert_eq!((ant.max_daily_cost, ant.max_thread_cost), (Some(2.5), Some(99.0)), "no ceiling, no bound");
        assert_eq!((ant.max_harnesses, ant.max_tool_calls, ant.prompt_timeout), (1, 50, Duration::from_mins(5)));
        assert_eq!(ant.allow, ["read", "execute:cargo build*"]);
        assert_eq!(ant.whoami.as_deref(), Some("Review."));

        let sonnet = Request { model: Some("sonnet".to_owned()), ..Request::default() };
        for (request, error) in [
            (Request::default(), "pick a model of template worker: haiku up to max, sonnet up to high"),
            (Request { model: Some("opus".to_owned()), ..sonnet.clone() }, "model opus is not in the models"),
            (
                Request { reasoning_effort: Some("xhigh".to_owned()), ..sonnet.clone() },
                "reasoning_effort xhigh is above high, the highest template worker allows for model sonnet",
            ),
            (Request { reasoning_effort: Some("hard".to_owned()), ..sonnet.clone() }, "is none of low < medium"),
            (Request { harness: Some("codex".to_owned()), ..sonnet.clone() }, "harness codex is not in"),
            (Request { context: Some("1m".to_owned()), ..sonnet.clone() }, "context 1m is not in"),
            (
                Request { max_daily_cost: Some(10.5), ..sonnet.clone() },
                "max_daily_cost 10.5 is above 10, the ceiling of template worker",
            ),
            (Request { max_harnesses: Some(3), ..sonnet.clone() }, "max_harnesses 3 is above 2"),
            (Request { max_harnesses: Some(0), ..sonnet.clone() }, "max_harnesses must be at least 1"),
            (
                Request { prompt_timeout: Some(Duration::ZERO), ..sonnet.clone() },
                "prompt_timeout 0 (no limit) is above 20m",
            ),
            (Request { prompt_timeout: Some(Duration::from_hours(1)), ..sonnet.clone() }, "prompt_timeout 1h is above"),
            (Request { allow_sets: vec!["all".to_owned()], ..sonnet.clone() }, "has no allow set all (it has: build)"),
            (Request { workdir: Some("/".into()), ..sonnet.clone() }, "workdir / is not in the workdirs"),
        ] {
            let err = format!("{:#}", t.resolve(&id("lead"), &id("lead-c"), &request).unwrap_err());
            assert!(err.contains(error), "{err}");
        }
        let err = t.resolve(&id("lead"), &id("dev-c"), &sonnet).unwrap_err().to_string();
        assert!(err.contains("does not fit name_pattern \"<manager>-*\" of template worker"), "{err}");
    }

    #[test]
    fn a_template_without_models_keeps_the_harness_defaults() {
        let t = AntTemplate { models: BTreeMap::new(), contexts: Vec::new(), harnesses: Vec::new(), ..template() };
        let ant = t.resolve(&id("lead"), &id("lead-a"), &Request::default()).unwrap();
        assert_eq!((ant.harness, ant.model, ant.reasoning_effort, ant.context), (None, None, None, None));
        for (request, error) in [
            (Request { model: Some("m".to_owned()), ..Request::default() }, "lists no models"),
            (Request { reasoning_effort: Some("low".to_owned()), ..Request::default() }, "lists no models"),
            (Request { context: Some("default".to_owned()), ..Request::default() }, "lists no contexts"),
            (Request { harness: Some("claude".to_owned()), ..Request::default() }, "lists no harnesses"),
        ] {
            let err = t.resolve(&id("lead"), &id("lead-a"), &request).unwrap_err().to_string();
            assert!(err.contains(error), "{err}");
        }
    }
}
