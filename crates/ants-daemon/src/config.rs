//! The daemon config file, `~/.ants/config.toml`: the personas one daemon process serves.
//!
//! ```toml
//! nats = "wss://hub.example/nats"   # default server for every persona
//! js_domain = "hub"                 # JetStream domain of the hub, if it has one; also per persona
//!
//! [[persona]]
//! name = "dev-a"
//! workdir = "~/src/repo-a"
//! creds = "~/.config/ants/auth/dev-a.creds"
//! ```
//!
//! `~` expands to the home directory; other relative paths are relative to the file's directory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{JsDomain, PersonaId};
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Deserializer};

use crate::caps::Caps;
use crate::daemon::Config;
use crate::harness::{self, HarnessKind, PromptLimits};

/// Environment variable naming the config file.
pub const ENV: &str = "ANTS_CONFIG";
pub const DEFAULT_MAX_HARNESSES: usize = 4;
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_APPROVAL_TIMEOUT_SECS: u64 = crate::approvals::DEFAULT_TIMEOUT.as_secs();
pub const DEFAULT_PROMPT_TIMEOUT_SECS: u64 = harness::DEFAULT_PROMPT_TIMEOUT.as_secs();
pub const DEFAULT_MAX_TOOL_CALLS: usize = harness::DEFAULT_MAX_TOOL_CALLS;

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
    nats: Option<String>,
    js_domain: Option<JsDomain>,
    #[serde(default, rename = "persona")]
    personas: Vec<Persona>,
}

/// One persona the daemon serves, as the config file (or the command line) describes it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Persona {
    pub name: PersonaId,
    /// NATS server; the file's top-level `nats`, then `--nats`, when absent.
    pub nats: Option<String>,
    pub creds: Option<PathBuf>,
    /// JetStream domain of the hub; the file's top-level `js_domain`, then `--js-domain`, when absent.
    pub js_domain: Option<JsDomain>,
    pub workdir: PathBuf,
    /// Default: `~/.ants/<name>`.
    pub home: Option<PathBuf>,
    /// Memory and task instructions prepended to every prompt. Tools are provided by the harness.
    pub memory: Option<String>,
    pub tasks: Option<String>,
    /// Added to the harness's inherited environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, deserialize_with = "harness")]
    pub harness: HarnessKind,
    /// Same as `allow = ["*"]`.
    #[serde(default)]
    pub allow_tools: bool,
    /// Tool allowlist; see [`crate::permission`].
    #[serde(default)]
    pub allow: Vec<String>,
    /// Seconds a tool request outside the allowlist waits for the human.
    #[serde(default = "default_approval_timeout_secs")]
    pub approval_timeout_secs: u64,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default = "default_max_harnesses")]
    pub max_harnesses: usize,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
    /// Seconds a prompt may run before it is cancelled; 0 for no limit.
    #[serde(default = "default_prompt_timeout_secs")]
    pub prompt_timeout_secs: u64,
    /// Tool calls a prompt may start before it is cancelled; 0 for no limit.
    #[serde(default = "default_max_tool_calls")]
    pub max_tool_calls: usize,
    /// Tokens a thread may spend between messages from the human.
    pub max_thread_tokens: Option<u64>,
    /// Cost a thread may spend between messages from the human, in the harness's unit.
    pub max_thread_cost: Option<f64>,
    /// Tokens the persona may spend in a UTC day.
    pub max_daily_tokens: Option<u64>,
    /// Cost the persona may spend in a UTC day.
    pub max_daily_cost: Option<f64>,
}

fn default_max_harnesses() -> usize {
    DEFAULT_MAX_HARNESSES
}

fn default_idle_timeout_secs() -> u64 {
    DEFAULT_IDLE_TIMEOUT_SECS
}

fn default_approval_timeout_secs() -> u64 {
    DEFAULT_APPROVAL_TIMEOUT_SECS
}

fn default_prompt_timeout_secs() -> u64 {
    DEFAULT_PROMPT_TIMEOUT_SECS
}

fn default_max_tool_calls() -> usize {
    DEFAULT_MAX_TOOL_CALLS
}

fn harness<'de, D: Deserializer<'de>>(d: D) -> Result<HarnessKind, D::Error> {
    String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
}

impl Persona {
    /// A persona with default settings.
    pub fn new(name: PersonaId, workdir: PathBuf) -> Self {
        Self {
            name,
            nats: None,
            creds: None,
            js_domain: None,
            workdir,
            home: None,
            memory: None,
            tasks: None,
            env: BTreeMap::new(),
            harness: HarnessKind::default(),
            allow_tools: false,
            allow: Vec::new(),
            approval_timeout_secs: DEFAULT_APPROVAL_TIMEOUT_SECS,
            capabilities: Vec::new(),
            max_harnesses: DEFAULT_MAX_HARNESSES,
            idle_timeout_secs: DEFAULT_IDLE_TIMEOUT_SECS,
            prompt_timeout_secs: DEFAULT_PROMPT_TIMEOUT_SECS,
            max_tool_calls: DEFAULT_MAX_TOOL_CALLS,
            max_thread_tokens: None,
            max_thread_cost: None,
            max_daily_tokens: None,
            max_daily_cost: None,
        }
    }

    /// The daemon config, with `default_nats` as the server when none is set.
    pub fn into_config(self, default_nats: &str) -> Result<Config> {
        validate_env(&self.env)?;
        let instructions = self.instructions();
        let workdir = self
            .workdir
            .canonicalize()
            .with_context(|| format!("persona {}: workdir {}", self.name, self.workdir.display()))?;
        let home = match self.home {
            Some(home) => home,
            None => home_dir()?.join(".ants").join(self.name.as_str()),
        };
        Ok(Config {
            nats_url: self.nats.unwrap_or_else(|| default_nats.to_owned()),
            creds: self.creds,
            js_domain: self.js_domain,
            workdir,
            home,
            instructions,
            env: self.env,
            harness: self.harness,
            max_harnesses: self.max_harnesses,
            idle_timeout: Duration::from_secs(self.idle_timeout_secs),
            allow: if self.allow_tools { [self.allow, vec!["*".to_owned()]].concat() } else { self.allow },
            approval_timeout: Duration::from_secs(self.approval_timeout_secs),
            capabilities: self.capabilities,
            limits: PromptLimits {
                timeout: Some(Duration::from_secs(self.prompt_timeout_secs)).filter(|t| !t.is_zero()),
                max_tool_calls: Some(self.max_tool_calls).filter(|n| *n > 0),
            },
            caps: Caps {
                thread_tokens: self.max_thread_tokens,
                thread_cost: self.max_thread_cost,
                daily_tokens: self.max_daily_tokens,
                daily_cost: self.max_daily_cost,
            },
            persona: self.name,
        })
    }

    fn instructions(&self) -> String {
        [("memory", self.memory.as_deref()), ("tasks", self.tasks.as_deref())]
            .into_iter()
            .filter_map(|(role, text)| {
                text.filter(|text| !text.trim().is_empty()).map(|text| format!("[ants {role}]\n{text}"))
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    }
}

fn validate_env(env: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in env {
        if name.is_empty() || name.contains(['=', '\0']) || value.contains('\0') {
            bail!("invalid harness environment variable {name:?}");
        }
    }
    Ok(())
}

/// Read and check the config file at `path`.
pub fn load(path: &Path) -> Result<Vec<Persona>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    parse(&text, &std::path::absolute(path)?, &home_dir()?)
}

/// Parse the text of the config file at `path`: expand `~` to `home`, resolve relative paths against the file's
/// directory, apply the top-level `nats` and `js_domain`, and check that names are unique and workdirs exist.
pub fn parse(text: &str, path: &Path, home: &Path) -> Result<Vec<Persona>> {
    let file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
    if file.personas.is_empty() {
        bail!("config {}: no [[persona]] entries", path.display());
    }
    let base = path.parent().unwrap_or(Path::new("."));
    let mut names = BTreeSet::new();
    let mut personas = Vec::with_capacity(file.personas.len());
    for mut p in file.personas {
        if !names.insert(p.name.clone()) {
            bail!("config {}: persona {} is listed twice", path.display(), p.name);
        }
        validate_env(&p.env)?;
        let abs = |dir: &Path| resolve(dir, base, home);
        p.workdir = abs(&p.workdir).canonicalize().with_context(|| {
            format!("config {}: persona {}: workdir {}", path.display(), p.name, p.workdir.display())
        })?;
        p.home = p.home.as_deref().map(abs);
        p.creds = p.creds.as_deref().map(abs);
        if p.nats.is_none() {
            p.nats.clone_from(&file.nats);
        }
        if p.js_domain.is_none() {
            p.js_domain.clone_from(&file.js_domain);
        }
        personas.push(p);
    }
    Ok(personas)
}

fn resolve(path: &Path, base: &Path, home: &Path) -> PathBuf {
    match path.strip_prefix("~") {
        Ok(rest) => home.join(rest),
        Err(_) => base.join(path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    /// Parse as if the file sat in this crate's directory, with the workspace root as home.
    fn parse_here(text: &str) -> Result<Vec<Persona>> {
        let dir = manifest_dir();
        parse(text, &dir.join("config.toml"), dir.parent().unwrap().parent().unwrap())
    }

    #[test]
    fn parses_several_personas() {
        let personas = parse_here(
            r#"
            nats = "wss://hub.example/nats"
            js_domain = "hub"

            [[persona]]
            name = "dev-a"
            workdir = "~/crates/ants"
            creds = "~/auth/dev-a.creds"
            home = "/srv/ants/dev-a"
            memory = "Use the memory MCP server."
            tasks = "Use the tracker CLI."
            env = { TRACKER_DB = "/srv/ants/dev-a/tasks.db" }
            harness = "copilot"
            allow = ["read", "search"]
            capabilities = ["rust", "review"]
            max_harnesses = 2
            idle_timeout_secs = 60
            prompt_timeout_secs = 0
            max_tool_calls = 50
            max_thread_cost = 2.5
            max_daily_tokens = 1000000

            [[persona]]
            name = "dev-b"
            nats = "nats://127.0.0.1:4222"
            workdir = "src"
            "#,
        )
        .unwrap();
        let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
        let a = &personas[0];
        assert_eq!(a.name.as_str(), "dev-a");
        assert_eq!(a.nats.as_deref(), Some("wss://hub.example/nats"));
        assert_eq!(a.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        assert_eq!(a.workdir, root.join("crates/ants").canonicalize().unwrap());
        assert_eq!(a.creds, Some(root.join("auth/dev-a.creds")));
        assert_eq!(a.home, Some(PathBuf::from("/srv/ants/dev-a")));
        assert_eq!((a.allow.len(), a.capabilities.len()), (2, 2));
        assert_eq!((a.max_harnesses, a.idle_timeout_secs), (2, 60));
        let config = a.clone().into_config("nats://unused").unwrap();
        assert_eq!(
            config.instructions,
            "[ants memory]\nUse the memory MCP server.\n\n[ants tasks]\nUse the tracker CLI."
        );
        assert_eq!(config.env.get("TRACKER_DB").map(String::as_str), Some("/srv/ants/dev-a/tasks.db"));
        assert_eq!(config.limits, PromptLimits { timeout: None, max_tool_calls: Some(50) }, "0 is no limit");
        let caps = Caps { thread_cost: Some(2.5), daily_tokens: Some(1_000_000), ..Caps::default() };
        assert_eq!(config.caps, caps);

        let b = personas[1].clone();
        assert_eq!(b.nats.as_deref(), Some("nats://127.0.0.1:4222"));
        assert_eq!(b.workdir, manifest_dir().join("src").canonicalize().unwrap());
        let defaults = Persona::new(b.name.clone(), b.workdir.clone());
        assert_eq!(b, Persona { nats: b.nats.clone(), js_domain: b.js_domain.clone(), ..defaults });

        let config = b.into_config("nats://unused").unwrap();
        assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
        assert!(config.allow.is_empty(), "rejects every tool request unless the human allows it");
        assert_eq!(config.approval_timeout, crate::approvals::DEFAULT_TIMEOUT);
        assert_eq!(config.idle_timeout, Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS));
        assert_eq!(config.limits, PromptLimits::default());
        assert_eq!(config.caps, Caps::default(), "no spend caps unless set");
        assert!(config.home.ends_with(".ants/dev-b"));
        assert!(config.instructions.is_empty(), "no memory or task tool by default");
        assert!(config.env.is_empty(), "no tool-specific environment by default");
    }

    #[test]
    fn memory_and_tasks_are_independent_and_can_be_cleared() {
        let mut p = Persona::new("dev-a".parse().unwrap(), manifest_dir());
        p.memory = Some("Use notes.".to_owned());
        assert_eq!(p.instructions(), "[ants memory]\nUse notes.");
        p.memory = Some(String::new());
        p.tasks = Some("Use tickets.".to_owned());
        assert_eq!(p.instructions(), "[ants tasks]\nUse tickets.");
        p.tasks = Some(" \n".to_owned());
        assert!(p.instructions().is_empty());
    }

    #[test]
    fn rejects_invalid_environment_without_exposing_values() {
        for (name, value) in [("", "secret"), ("bad=name", "secret"), ("bad\0name", "secret"), ("NAME", "secret\0")] {
            let mut p = Persona::new("dev-a".parse().unwrap(), manifest_dir());
            p.env.insert(name.to_owned(), value.to_owned());
            let err = p.into_config("nats://unused").unwrap_err().to_string();
            assert!(err.contains("invalid harness environment variable"), "{err}");
            assert!(!err.contains("secret"), "{err}");
        }
        let err = parse_here("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"\nenv = { 'bad=name' = 'secret' }")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid harness environment variable"), "{err}");
    }

    #[test]
    fn falls_back_to_the_default_server() {
        let personas = parse_here("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
        assert_eq!(personas[0].nats, None);
        assert_eq!(personas[0].clone().into_config("nats://leaf:4222").unwrap().nats_url, "nats://leaf:4222");
    }

    #[test]
    fn rejects_bad_files() {
        let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
        let twice =
            err("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"\n[[persona]]\nname = \"dev-a\"\nworkdir = \".\"");
        assert!(twice.contains("persona dev-a is listed twice"), "{twice}");
        assert!(twice.contains("config.toml"), "{twice}");
        let missing = err("[[persona]]\nname = \"dev-a\"\nworkdir = \"no/such/dir\"");
        assert!(missing.contains("workdir no/such/dir"), "{missing}");
        assert!(err("nats = \"nats://x\"").contains("no [[persona]] entries"));
        assert!(err("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"vim\"").contains("unknown harness"));
        assert!(err("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"\nworkdirs = 1").contains("unknown field"));
        assert!(err("[[persona]]\nname = \"Dev.A\"\nworkdir = \".\"").contains("parsing config"));
    }
}
