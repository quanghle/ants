//! The daemon config file, `~/.ants/config.toml`: the personas one daemon process serves.
//!
//! ```toml
//! provider = "copilot"              # default harness of every persona; also serves the built-in messenger
//! nats = "wss://hub.example/nats"   # default server for every persona
//! js_domain = "hub"                 # JetStream domain of the hub, if it has one; also per persona
//! human = "alice"                   # default human, for escalations of threads with no owner; also per persona
//!
//! [[persona]]
//! name = "dev-a"
//! workdir = "~/src/repo-a"
//! creds = "~/.config/ants/auth/dev-a.creds"
//! ```
//!
//! `~` expands to the home directory; other relative paths are relative to the file's directory.
//!
//! A file with `provider` also serves the built-in messenger ([`builtin_messenger`]) unless it lists a persona named
//! `messenger`; `provider` alone is a complete file.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{JsDomain, PersonaId};
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Deserializer};

use crate::caps::Caps;
use crate::daemon::Config;
use crate::harness::{self, HarnessKind, ModelSettings, PromptLimits};

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
    /// Default harness of the file's personas, and the harness of the built-in messenger.
    #[serde(default, deserialize_with = "some_harness")]
    provider: Option<HarnessKind>,
    nats: Option<String>,
    js_domain: Option<JsDomain>,
    human: Option<PersonaId>,
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
    /// The default human, who gets what is meant for the owner of a thread that has none: escalations, dead-letter
    /// notices. The file's top-level `human`, then `human`, when absent.
    pub human: Option<PersonaId>,
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
    /// The file's top-level `provider`, then the default harness, when absent.
    #[serde(default, deserialize_with = "some_harness")]
    pub harness: Option<HarnessKind>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub context: Option<String>,
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
    /// Tokens a thread may spend between messages from a human.
    pub max_thread_tokens: Option<u64>,
    /// Cost a thread may spend between messages from a human, in the harness's unit.
    pub max_thread_cost: Option<f64>,
    /// Tokens the persona may spend in a UTC day.
    pub max_daily_tokens: Option<u64>,
    /// Cost the persona may spend in a UTC day.
    pub max_daily_cost: Option<f64>,
    /// The built-in messenger, which waits to start while the roster lists a live messenger on another machine.
    #[serde(skip)]
    pub builtin: bool,
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

fn some_harness<'de, D: Deserializer<'de>>(d: D) -> Result<Option<HarnessKind>, D::Error> {
    String::deserialize(d)?.parse().map(Some).map_err(serde::de::Error::custom)
}

impl Persona {
    /// A persona with default settings.
    pub fn new(name: PersonaId, workdir: PathBuf) -> Self {
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
            builtin: false,
        }
    }

    /// The daemon config, with `default_nats` as the server when none is set.
    pub fn into_config(self, default_nats: &str) -> Result<Config> {
        validate_env(&self.env)?;
        let instructions = self.prompt_instructions();
        let harness = self.harness.unwrap_or_default();
        let model_settings =
            ModelSettings { model: self.model, reasoning_effort: self.reasoning_effort, context: self.context };
        model_settings.validate(harness)?;
        if self.builtin {
            std::fs::create_dir_all(&self.workdir)
                .with_context(|| format!("persona {}: creating workdir {}", self.name, self.workdir.display()))?;
        }
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
            human: self.human.unwrap_or_else(PersonaId::human),
            workdir,
            home,
            instructions,
            whoami: self.whoami,
            env: self.env,
            harness,
            model_settings,
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
            defer_to_other_machines: self.builtin,
        })
    }

    fn prompt_instructions(&self) -> String {
        [
            (format!("persona {}", self.name), self.whoami.as_deref()),
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

fn validate_env(env: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in env {
        if name.is_empty() || name.contains(['=', '\0']) || value.contains('\0') {
            bail!("invalid harness environment variable {name:?}");
        }
    }
    Ok(())
}

/// The persona a file with `provider` serves unless it lists one named `messenger`: the provider's harness with its
/// default model, reasoning effort and context, and otherwise the settings of a persona that sets nothing. Its home and
/// workdir are `~/.ants/messenger`, created when it starts, and its credentials
/// `~/.config/ants/auth/messenger.creds` (where `ants auth user messenger` writes them) if that file exists.
pub fn builtin_messenger(provider: HarnessKind, home: &Path) -> Persona {
    let name = PersonaId::messenger();
    let dir = home.join(".ants").join(name.as_str());
    let creds = home.join(".config/ants/auth").join(format!("{name}.creds"));
    Persona {
        harness: Some(provider),
        home: Some(dir.clone()),
        creds: creds.is_file().then_some(creds),
        builtin: true,
        ..Persona::new(name, dir)
    }
}

/// Read and check the config file at `path`.
pub fn load(path: &Path) -> Result<Vec<Persona>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    parse(&text, &std::path::absolute(path)?, &home_dir()?)
}

/// Parse the text of the config file at `path`: expand `~` to `home`, resolve relative paths against the file's
/// directory, apply the top-level `provider`, `nats`, `js_domain` and `human`, check that names are unique and workdirs exist,
/// and add the built-in messenger when `provider` is set and no persona is named `messenger`.
pub fn parse(text: &str, path: &Path, home: &Path) -> Result<Vec<Persona>> {
    let mut file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
    if file.personas.is_empty() && file.provider.is_none() {
        bail!("config {}: no provider and no [[persona]] entries", path.display());
    }
    if let Some(provider) = file.provider
        && !file.personas.iter().any(|p| p.name == PersonaId::messenger())
    {
        file.personas.push(builtin_messenger(provider, home));
    }
    let base = path.parent().unwrap_or(Path::new("."));
    let mut names = BTreeSet::new();
    let mut personas = Vec::with_capacity(file.personas.len());
    for mut p in file.personas {
        if !names.insert(p.name.clone()) {
            bail!("config {}: persona {} is listed twice", path.display(), p.name);
        }
        validate_env(&p.env)?;
        if p.harness.is_none() {
            p.harness = file.provider;
        }
        if !p.builtin {
            let abs = |dir: &Path| resolve(dir, base, home);
            p.workdir = abs(&p.workdir).canonicalize().with_context(|| {
                format!("config {}: persona {}: workdir {}", path.display(), p.name, p.workdir.display())
            })?;
            p.home = p.home.as_deref().map(abs);
            p.creds = p.creds.as_deref().map(abs);
        }
        if p.nats.is_none() {
            p.nats.clone_from(&file.nats);
        }
        if p.js_domain.is_none() {
            p.js_domain.clone_from(&file.js_domain);
        }
        if p.human.is_none() {
            p.human.clone_from(&file.human);
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
            human = "alice"

            [[persona]]
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
            idle_timeout_secs = 60
            prompt_timeout_secs = 0
            max_tool_calls = 50
            max_thread_cost = 2.5
            max_daily_tokens = 1000000

            [[persona]]
            name = "dev-b"
            nats = "nats://127.0.0.1:4222"
            human = "bob"
            workdir = "src"
            "#,
        )
        .unwrap();
        let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
        let a = &personas[0];
        assert_eq!(a.name.as_str(), "dev-a");
        assert_eq!(a.nats.as_deref(), Some("wss://hub.example/nats"));
        assert_eq!(a.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        assert_eq!(a.human.as_ref().map(PersonaId::as_str), Some("alice"));
        assert_eq!(a.workdir, root.join("crates/ants").canonicalize().unwrap());
        assert_eq!(a.creds, Some(root.join("auth/dev-a.creds")));
        assert_eq!(a.home, Some(PathBuf::from("/srv/ants/dev-a")));
        assert_eq!((a.allow.len(), a.capabilities.len()), (2, 2));
        assert_eq!((a.max_harnesses, a.idle_timeout_secs), (2, 60));
        let config = a.clone().into_config("nats://unused").unwrap();
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
            "[ants persona dev-a]\nMaintain the Rust backend.\nReview changes for correctness.\n\n\
             [ants memory]\nUse the memory MCP server.\n\n[ants tasks]\nUse the tracker CLI."
        );
        assert_eq!(config.env.get("TRACKER_DB").map(String::as_str), Some("/srv/ants/dev-a/tasks.db"));
        assert_eq!(config.limits, PromptLimits { timeout: None, max_tool_calls: Some(50) }, "0 is no limit");
        let caps = Caps { thread_cost: Some(2.5), daily_tokens: Some(1_000_000), ..Caps::default() };
        assert_eq!(config.caps, caps);

        let b = personas[1].clone();
        assert_eq!(b.nats.as_deref(), Some("nats://127.0.0.1:4222"));
        assert_eq!(b.workdir, manifest_dir().join("src").canonicalize().unwrap());
        let defaults = Persona::new(b.name.clone(), b.workdir.clone());
        assert_eq!(
            b,
            Persona { nats: b.nats.clone(), js_domain: b.js_domain.clone(), human: b.human.clone(), ..defaults }
        );

        let config = b.into_config("nats://unused").unwrap();
        assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
        assert_eq!(config.human.as_str(), "bob", "a persona's own default human comes first");
        assert!(config.allow.is_empty(), "rejects every tool request unless the human allows it");
        assert_eq!(config.approval_timeout, crate::approvals::DEFAULT_TIMEOUT);
        assert_eq!(config.idle_timeout, Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS));
        assert_eq!(config.limits, PromptLimits::default());
        assert_eq!(config.caps, Caps::default(), "no spend caps unless set");
        assert!(config.home.ends_with(".ants/dev-b"));
        assert_eq!(config.whoami, None);
        assert_eq!(config.model_settings, ModelSettings::default());
        assert!(config.instructions.is_empty(), "no memory or task tool by default");
        assert!(config.env.is_empty(), "no tool-specific environment by default");
    }

    #[test]
    fn whoami_memory_and_tasks_are_independent_and_can_be_cleared() {
        let mut p = Persona::new("dev-a".parse().unwrap(), manifest_dir());
        p.whoami = Some("Maintain the backend.".to_owned());
        p.memory = Some("Use notes.".to_owned());
        assert_eq!(p.prompt_instructions(), "[ants persona dev-a]\nMaintain the backend.\n\n[ants memory]\nUse notes.");
        p.whoami = Some(String::new());
        assert_eq!(p.prompt_instructions(), "[ants memory]\nUse notes.");
        p.memory = Some(String::new());
        p.tasks = Some("Use tickets.".to_owned());
        assert_eq!(p.prompt_instructions(), "[ants tasks]\nUse tickets.");
        p.whoami = Some(" \n".to_owned());
        p.tasks = Some(" \n".to_owned());
        assert!(p.prompt_instructions().is_empty());
    }

    #[test]
    fn invalid_native_settings_fail_before_starting_a_persona() {
        for (harness, model, effort, context, error) in [
            (HarnessKind::Copilot, Some(" "), None, None, "model must not be blank"),
            (HarnessKind::Claude, None, Some(""), None, "reasoning_effort must not be blank"),
            (HarnessKind::Copilot, None, None, Some("128000"), "default or long_context"),
            (HarnessKind::Claude, None, None, Some("1m"), "requires an explicit model"),
            (HarnessKind::Claude, Some("smart[1m]"), None, Some("default"), "conflicts"),
            (HarnessKind::Antigravity, None, None, Some("long_context"), "no separate context selector"),
        ] {
            let mut p = Persona::new("dev-a".parse().unwrap(), manifest_dir());
            p.harness = Some(harness);
            p.model = model.map(str::to_owned);
            p.reasoning_effort = effort.map(str::to_owned);
            p.context = context.map(str::to_owned);
            let err = p.into_config("nats://unused").unwrap_err().to_string();
            assert!(err.contains(error), "{err}");
        }
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
    fn falls_back_to_the_default_server_and_human() {
        let personas = parse_here("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
        assert_eq!((&personas[0].nats, &personas[0].human), (&None, &None));
        let config = personas[0].clone().into_config("nats://leaf:4222").unwrap();
        assert_eq!((config.nats_url.as_str(), config.human), ("nats://leaf:4222", PersonaId::human()));
        let err = parse_here("human = \"Not A Name\"\nprovider = \"copilot\"").unwrap_err();
        assert!(format!("{err:#}").contains("human"), "{err:#}");
    }

    #[test]
    fn provider_is_the_default_harness_and_serves_the_builtin_messenger() {
        let personas = parse_here(
            "provider = \"claude\"\njs_domain = \"hub\"\n\
             [[persona]]\nname = \"dev-a\"\nworkdir = \".\"\n\
             [[persona]]\nname = \"dev-b\"\nworkdir = \".\"\nharness = \"antigravity\"",
        )
        .unwrap();
        let harnesses: Vec<_> = personas.iter().map(|p| (p.name.as_str(), p.harness, p.builtin)).collect();
        assert_eq!(
            harnesses,
            [
                ("dev-a", Some(HarnessKind::Claude), false),
                ("dev-b", Some(HarnessKind::Antigravity), false),
                ("messenger", Some(HarnessKind::Claude), true),
            ]
        );
        let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
        let messenger = &personas[2];
        let dir = root.join(".ants/messenger");
        assert_eq!((&messenger.workdir, messenger.home.as_ref()), (&dir, Some(&dir)));
        assert_eq!(messenger.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        let stock = Persona {
            harness: Some(HarnessKind::Claude),
            home: Some(dir.clone()),
            js_domain: messenger.js_domain.clone(),
            builtin: true,
            ..Persona::new(PersonaId::messenger(), dir)
        };
        assert_eq!(messenger, &stock, "provider defaults for model, reasoning effort and context; stock settings");

        let alone = parse_here("provider = \"copilot\"").unwrap();
        assert_eq!(alone.len(), 1, "provider alone is a complete file");
        assert_eq!((alone[0].name.as_str(), alone[0].builtin), ("messenger", true));
    }

    #[test]
    fn a_listed_messenger_replaces_the_builtin_one() {
        let personas = parse_here("provider = \"claude\"\n[[persona]]\nname = \"messenger\"\nworkdir = \".\"").unwrap();
        assert_eq!(personas.len(), 1);
        assert_eq!((personas[0].harness, personas[0].builtin), (Some(HarnessKind::Claude), false));
        assert_eq!(personas[0].workdir, manifest_dir().canonicalize().unwrap());
    }

    #[test]
    fn without_provider_there_is_no_builtin_messenger() {
        let personas = parse_here("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
        assert_eq!(personas.len(), 1);
        assert_eq!(personas[0].harness, None);
        let config = personas[0].clone().into_config("nats://unused").unwrap();
        assert_eq!((config.harness, config.defer_to_other_machines), (HarnessKind::Copilot, false));
    }

    #[test]
    fn the_builtin_messenger_finds_its_credentials_and_creates_its_workdir() {
        let home = ants_test_support::scratch_dir("config-home").canonicalize().unwrap();
        let auth = home.join(".config/ants/auth");
        assert_eq!(builtin_messenger(HarnessKind::Copilot, &home).creds, None, "no credentials issued");
        std::fs::create_dir_all(&auth).unwrap();
        std::fs::write(auth.join("messenger.creds"), "").unwrap();
        let messenger = builtin_messenger(HarnessKind::Copilot, &home);
        assert_eq!(messenger.creds, Some(auth.join("messenger.creds")));
        let config = messenger.into_config("nats://unused").unwrap();
        assert!(config.defer_to_other_machines);
        assert_eq!(config.workdir, home.join(".ants/messenger").canonicalize().unwrap());
        assert_eq!(config.model_settings, ModelSettings::default());
        std::fs::remove_dir_all(&home).unwrap();
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
        assert!(err("nats = \"nats://x\"").contains("no provider and no [[persona]] entries"));
        assert!(err("provider = \"vim\"").contains("unknown harness"));
        assert!(err("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"vim\"").contains("unknown harness"));
        assert!(err("[[persona]]\nname = \"dev-a\"\nworkdir = \".\"\nworkdirs = 1").contains("unknown field"));
        assert!(err("[[persona]]\nname = \"Dev.A\"\nworkdir = \".\"").contains("parsing config"));
    }
}
