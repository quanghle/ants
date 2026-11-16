//! Starting a harness process: its executable, arguments and environment, and the model settings an ant selects.

use std::collections::BTreeMap;
use std::path::PathBuf;

use agent_client_protocol::{AcpAgentConfig, Client, DynConnectTo};
use anyhow::{Result, bail};

use super::{Harness, native};

impl Harness {
    /// The harness process: its arguments with those of the ant's `context`, its environment with the ant's `env`
    /// over it.
    pub(crate) fn process(&self, env: &[(String, String)], settings: &ModelSettings) -> AcpAgentConfig {
        AcpAgentConfig::new(self.executable(env)).args(self.launch_args(settings)).envs(self.launch_env(env))
    }

    /// Its arguments with those of the ant's `context`.
    pub(super) fn launch_args(&self, settings: &ModelSettings) -> Vec<String> {
        let context = settings.context.as_ref().and_then(|value| self.context.get(value));
        self.args.iter().chain(context.into_iter().flat_map(|value| &value.args)).cloned().collect()
    }

    /// Its environment with the ant's `env` over it, which the harness process gets over the daemon's.
    pub(super) fn launch_env(&self, env: &[(String, String)]) -> BTreeMap<String, String> {
        self.env.clone().into_iter().chain(env.iter().cloned()).collect()
    }

    /// The executable to start: the command, except on Windows a name found on `PATH` ([`Self::locate`]), since
    /// starting a process by name finds only `.exe` files there, while npm installs harnesses as `.cmd` scripts.
    pub(super) fn executable(&self, env: &[(String, String)]) -> PathBuf {
        if cfg!(windows) && !self.command_is_path() {
            return self.locate(env).unwrap_or_else(|| self.command.clone());
        }
        self.command.clone()
    }

    /// What the daemon's ACP client connects to: the harness process, or a translator of its protocol.
    pub(super) fn agent(&self, env: &[(String, String)], settings: &ModelSettings) -> DynConnectTo<Client> {
        native::agent(self, env, settings)
    }

    /// Where the harness's executable is, with the ant's `env` over the definition's over the daemon's: the path its
    /// command names, else the first match on `PATH`. None when it is not installed there; a relative path, which the
    /// harness's working directory resolves, counts as found.
    pub fn locate(&self, env: &[(String, String)]) -> Option<PathBuf> {
        if self.command_is_path() {
            let unsure = self.command.is_relative() || self.command.is_file();
            return unsure.then(|| self.command.clone());
        }
        let path = env.iter().rev().find(|(name, _)| name == "PATH").map(|(_, path)| path.into());
        let path = path.or_else(|| self.env.get("PATH").map(Into::into)).or_else(|| std::env::var_os("PATH"))?;
        on_path(self.command.as_os_str(), &path)
    }
}

/// The first file named `name` in the directories of `path`, as a harness command is looked up: on Windows as an
/// `.exe`, `.cmd` or `.bat` file, in that order, then as named.
pub fn on_path(name: &std::ffi::OsStr, path: &std::ffi::OsStr) -> Option<PathBuf> {
    // npm puts a shell script without extension beside the `.cmd` one, which Windows cannot start.
    let extensions: &[&str] = if cfg!(windows) { &[".exe", ".cmd", ".bat", ""] } else { &[""] };
    std::env::split_paths(path).find_map(|dir| {
        extensions
            .iter()
            .map(|ext| {
                let mut file = name.to_owned();
                file.push(ext);
                dir.join(file)
            })
            .find(|p| p.is_file())
    })
}

/// Explicit per-ant settings; absent values keep the harness's defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelSettings {
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    /// Provider-native context selection, not a token spending cap.
    pub context: Option<String>,
    /// Bytes of the thread's history a rebuilt session is seeded with, over the harness's.
    pub history_budget: Option<usize>,
}

impl ModelSettings {
    pub fn validate(&self, harness: &Harness) -> Result<()> {
        for (name, value) in
            [("model", &self.model), ("reasoning_effort", &self.reasoning_effort), ("context", &self.context)]
        {
            if value.as_ref().is_some_and(|s| s.trim().is_empty()) {
                bail!("{name} must not be blank; omit it to keep the harness default");
            }
        }
        let Some(context) = self.context.as_deref() else { return Ok(()) };
        if harness.context.is_empty() {
            bail!(
                "harness {harness} has no context setting; omit context and select a model with the needed native window"
            );
        }
        let Some(selects) = harness.context.get(context) else {
            let values: Vec<_> = harness.context.keys().map(String::as_str).collect();
            bail!("harness {harness}: context must be {}", one_of(&values));
        };
        if selects.model_suffix.is_some() && self.model.is_none() {
            bail!("harness {harness}: context {context} requires an explicit model");
        }
        if let Some(model) = &self.model {
            let other = harness.context.iter().find(|(value, other)| {
                value.as_str() != context
                    && other.model_suffix.as_ref().is_some_and(|suffix| {
                        model.ends_with(suffix.as_str()) && selects.model_suffix.as_ref() != Some(suffix)
                    })
            });
            if let Some((value, _)) = other {
                bail!(
                    "harness {harness}: context {context} conflicts with model {model}, which selects context {value}"
                );
            }
        }
        Ok(())
    }

    pub(crate) fn model_id(&self, harness: &Harness) -> Option<String> {
        let suffix = self.context.as_ref().and_then(|value| harness.context.get(value)?.model_suffix.as_deref());
        self.model.as_ref().map(|model| match suffix {
            Some(suffix) => format!("{}{suffix}", model.trim_end_matches(suffix)),
            None => model.clone(),
        })
    }
}

/// `a`, `a or b`, `a, b or c`.
pub(super) fn one_of(values: &[&str]) -> String {
    match values {
        [] => String::new(),
        [one] => (*one).to_owned(),
        [rest @ .., last] => format!("{} or {last}", rest.join(", ")),
    }
}
