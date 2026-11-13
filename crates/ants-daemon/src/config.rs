//! The daemon config file, `~/.ants/config.toml`: the personas one daemon process serves.
//!
//! ```toml
//! nats = "wss://hub.example/nats"   # default server for every persona
//!
//! [[persona]]
//! name = "dev-a"
//! workdir = "~/src/repo-a"
//! creds = "~/.config/ants/auth/dev-a.creds"
//! ```
//!
//! `~` expands to the home directory; other relative paths are relative to the file's directory.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::PersonaId;
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Deserializer};

use crate::daemon::Config;
use crate::harness::HarnessKind;

/// Environment variable naming the config file.
pub const ENV: &str = "ANTS_CONFIG";
pub const DEFAULT_MAX_HARNESSES: usize = 4;
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_APPROVAL_TIMEOUT_SECS: u64 = crate::approvals::DEFAULT_TIMEOUT.as_secs();

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
    pub workdir: PathBuf,
    /// Default: `~/.ants/<name>`.
    pub home: Option<PathBuf>,
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
            workdir,
            home: None,
            harness: HarnessKind::default(),
            allow_tools: false,
            allow: Vec::new(),
            approval_timeout_secs: DEFAULT_APPROVAL_TIMEOUT_SECS,
            capabilities: Vec::new(),
            max_harnesses: DEFAULT_MAX_HARNESSES,
            idle_timeout_secs: DEFAULT_IDLE_TIMEOUT_SECS,
        }
    }

    /// The daemon config, with `default_nats` as the server when none is set.
    pub fn into_config(self, default_nats: &str) -> Result<Config> {
        let workdir = self
            .workdir
            .canonicalize()
            .with_context(|| format!("persona {}: workdir {}", self.name, self.workdir.display()))?;
        let home = match self.home {
            Some(home) => home,
            None => crate::workspace::default_home(&self.name)?,
        };
        Ok(Config {
            nats_url: self.nats.unwrap_or_else(|| default_nats.to_owned()),
            creds: self.creds,
            workdir,
            home,
            harness: self.harness,
            max_harnesses: self.max_harnesses,
            idle_timeout: Duration::from_secs(self.idle_timeout_secs),
            allow: if self.allow_tools { [self.allow, vec!["*".to_owned()]].concat() } else { self.allow },
            approval_timeout: Duration::from_secs(self.approval_timeout_secs),
            capabilities: self.capabilities,
            persona: self.name,
        })
    }
}

/// Read and check the config file at `path`.
pub fn load(path: &Path) -> Result<Vec<Persona>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    parse(&text, &std::path::absolute(path)?, &home_dir()?)
}

/// Parse the text of the config file at `path`: expand `~` to `home`, resolve relative paths against the file's
/// directory, apply the top-level `nats`, and check that names are unique and workdirs exist.
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
        let abs = |dir: &Path| resolve(dir, base, home);
        p.workdir = abs(&p.workdir).canonicalize().with_context(|| {
            format!("config {}: persona {}: workdir {}", path.display(), p.name, p.workdir.display())
        })?;
        p.home = p.home.as_deref().map(abs);
        p.creds = p.creds.as_deref().map(abs);
        if p.nats.is_none() {
            p.nats.clone_from(&file.nats);
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

            [[persona]]
            name = "dev-a"
            workdir = "~/crates/ants"
            creds = "~/auth/dev-a.creds"
            home = "/srv/ants/dev-a"
            harness = "copilot"
            allow = ["read", "search"]
            capabilities = ["rust", "review"]
            max_harnesses = 2
            idle_timeout_secs = 60

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
        assert_eq!(a.workdir, root.join("crates/ants").canonicalize().unwrap());
        assert_eq!(a.creds, Some(root.join("auth/dev-a.creds")));
        assert_eq!(a.home, Some(PathBuf::from("/srv/ants/dev-a")));
        assert_eq!((a.allow.len(), a.capabilities.len()), (2, 2));
        assert_eq!((a.max_harnesses, a.idle_timeout_secs), (2, 60));

        let b = personas[1].clone();
        assert_eq!(b.nats.as_deref(), Some("nats://127.0.0.1:4222"));
        assert_eq!(b.workdir, manifest_dir().join("src").canonicalize().unwrap());
        let defaults = Persona::new(b.name.clone(), b.workdir.clone());
        assert_eq!(b, Persona { nats: b.nats.clone(), ..defaults });

        let config = b.into_config("nats://unused").unwrap();
        assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
        assert!(config.allow.is_empty(), "rejects every tool request unless the human allows it");
        assert_eq!(config.approval_timeout, crate::approvals::DEFAULT_TIMEOUT);
        assert_eq!(config.idle_timeout, Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS));
        assert!(config.home.ends_with(".ants/dev-b"));
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
