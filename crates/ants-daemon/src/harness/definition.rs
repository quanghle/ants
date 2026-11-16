//! A harness definition, as a `[harnesses.<name>]` table holds it, and what it says of tool gating.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use super::ModelSettings;
use super::native::{self, Protocol};

/// A named agent harness ants drives over ACP on stdio: an executable and its arguments, run directly (no shell),
/// with the environment it adds, the session mode it must run in and the context settings it takes. A harness whose CLI
/// speaks another protocol runs behind a translator ([`native`]).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Harness {
    #[serde(skip)]
    pub(super) name: String,
    /// The drop-in file defining it ([`crate::config::dropin_files`]), if one does.
    #[serde(skip)]
    pub(crate) file: Option<PathBuf>,
    /// A path, or a bare name looked up on `PATH` when the harness starts.
    pub(crate) command: PathBuf,
    /// The protocol the command speaks on stdio: ACP, else one a translator speaks ([`native`]).
    #[serde(default)]
    pub(crate) protocol: Protocol,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    /// Added to the daemon's environment; the ant's own `env` wins over it.
    #[serde(default)]
    pub(crate) env: BTreeMap<String, String>,
    /// ACP session mode every session is switched to, such as one sending tool requests to the client or one
    /// approving them all; a harness that does not offer it fails the session. For a native protocol, the CLI's own
    /// permission mode.
    pub(crate) mode: Option<String>,
    /// It runs its own tools without asking the client, as its arguments or mode make it: allow rules, approvals and
    /// a thread's narrowed tool rules do not reach them.
    #[serde(default)]
    pub(crate) bypass_permissions: bool,
    /// The values an ant's `context` may take, and what each selects. Empty: the harness takes no `context`.
    #[serde(default)]
    pub(crate) context: BTreeMap<String, ContextValue>,
    /// The command a human runs to sign the harness in, named when it reports it is signed out.
    pub(crate) sign_in: Option<String>,
    /// Phrases of its errors that mean a provider rate limit or overload, besides the common ones
    /// ([`crate::ratelimit`]).
    #[serde(default)]
    pub(crate) rate_limited: Vec<String>,
    /// Phrases of its errors that mean an exhausted provider quota, besides the common ones.
    #[serde(default)]
    pub(crate) quota_exhausted: Vec<String>,
    /// Phrases of its prompt errors that mean the session outgrew the model's context window, besides the ones every
    /// harness shares ([`crate::overflow`]).
    #[serde(default)]
    pub(crate) context_overflow: Vec<String>,
    /// Tokens of the model's context window, when known: sizes the history a rebuilt session is seeded with.
    pub(crate) window: Option<u64>,
    /// Bytes of the thread's history a rebuilt session is seeded with ([`crate::history::budget`]).
    pub(crate) history_budget: Option<usize>,
    /// Globs of the files it keeps memories in, which an admin's read of the ant's memories shows
    /// ([`crate::admin`]): relative to the ant's workdir, or under `~`. Unset, a table replacing a built-in harness
    /// keeps the built-in's.
    pub(crate) memory_files: Option<Vec<String>>,
}

/// What one `context` value selects.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextValue {
    /// Appended to the harness's arguments.
    #[serde(default)]
    pub(crate) args: Vec<String>,
    /// Appended to the model ID selected through ACP. A value with a suffix requires an explicit model.
    pub(crate) model_suffix: Option<String>,
    /// Tokens of the context window it selects, over the harness's.
    pub(crate) window: Option<u64>,
    /// Bytes of history a rebuilt session is seeded with, over the harness's.
    pub(crate) history_budget: Option<usize>,
}

impl Harness {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Its executable: a path, or a name looked up on `PATH`.
    pub fn command(&self) -> &Path {
        &self.command
    }

    /// The drop-in file defining it, if one does rather than the config file or ants itself.
    pub fn file(&self) -> Option<&Path> {
        self.file.as_deref()
    }

    /// The command a human runs to sign the harness in, if its definition names one.
    pub fn sign_in(&self) -> Option<&str> {
        self.sign_in.as_deref()
    }

    /// The globs of its memory files ([`Self::memory_files`]).
    pub fn memory_files(&self) -> &[String] {
        self.memory_files.as_deref().unwrap_or_default()
    }

    /// The protocol its command speaks.
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// Whether it runs its own tools without asking ants ([`Self::bypass_permissions`]), as every harness of a native
    /// protocol does: the CLI's own permission settings decide.
    pub fn bypasses_permissions(&self) -> bool {
        self.bypass_permissions || self.protocol.is_native()
    }

    /// What of an ant's tool gating does not reach this harness's own tools, when it bypasses permissions and the ant
    /// gates any: `allow` rules short of `*`, and tool approvals by its `escalate_to` ants when it `escalates`.
    pub fn unenforced(&self, allow: &[String], escalates: bool) -> Option<String> {
        if !self.bypasses_permissions() {
            return None;
        }
        let rules = !allow.is_empty() && !allows_all(allow);
        let gating = match (rules, escalates) {
            (false, false) => return None,
            (true, false) => "its allow rules",
            (false, true) => "tool approvals by its escalate_to ants",
            (true, true) => "its allow rules and tool approvals by its escalate_to ants",
        };
        Some(format!("harness {} runs its tools unasked, so {gating} do not apply to them", self.name))
    }

    /// What to change for the ant's tool gating to reach the harness's tools, or, for a native protocol, which no
    /// tool request leaves, where to restrict them.
    pub fn gated_fix(&self) -> String {
        if self.protocol.is_native() {
            return format!(
                "restrict its tools with the native permission settings of [harnesses.{}] (protocol {}: its mode and \
                 arguments; docs/config.md#harness-protocols), or pick a harness speaking ACP",
                self.name, self.protocol
            );
        }
        format!(
            "redefine [harnesses.{}] without bypass_permissions, with a mode or arguments that ask before running \
             tools (docs/config.md#harnesses), or pick another harness",
            self.name
        )
    }

    /// Whether `command` is a path rather than a name to look up on `PATH`.
    pub(crate) fn command_is_path(&self) -> bool {
        self.command.components().count() > 1
    }

    pub(super) fn validate(&self) -> Result<()> {
        let name = &self.name;
        if name.is_empty()
            || !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        {
            bail!("harness name {name:?}: use lowercase letters, digits, '-' and '_'");
        }
        if self.command.as_os_str().is_empty() {
            bail!("harness {name}: command must not be empty");
        }
        let args = self.args.iter().chain(self.context.values().flat_map(|value| &value.args));
        if self.command.as_os_str().as_encoded_bytes().contains(&0) || args.into_iter().any(|arg| arg.contains('\0')) {
            bail!("harness {name}: command and arguments must not contain NUL");
        }
        validate_env(&self.env).with_context(|| format!("harness {name}"))?;
        native::validate(self).with_context(|| format!("harness {name}"))?;
        if self.mode.as_ref().is_some_and(|mode| mode.trim().is_empty()) {
            bail!("harness {name}: mode must not be blank; omit it to keep the harness's own mode");
        }
        if self.sign_in.as_ref().is_some_and(|command| command.trim().is_empty()) {
            bail!("harness {name}: sign_in must not be blank; omit it when the harness names no sign-in command");
        }
        if self.rate_limited.iter().chain(&self.quota_exhausted).any(|phrase| phrase.trim().is_empty()) {
            bail!("harness {name}: rate_limited and quota_exhausted phrases must not be blank");
        }
        if self.context_overflow.iter().any(|phrase| phrase.trim().is_empty()) {
            bail!("harness {name}: context_overflow phrases must not be blank");
        }
        for glob in self.memory_files() {
            if glob.trim().is_empty()
                || glob.contains('\0')
                || Path::new(glob).components().any(|c| c == std::path::Component::ParentDir)
            {
                bail!("harness {name}: memory_files glob {glob:?} must not be blank nor name a parent directory (..)");
            }
        }
        for (value, selects) in &self.context {
            if value.trim().is_empty() {
                bail!("harness {name}: context values must not be blank");
            }
            if selects.model_suffix.as_ref().is_some_and(String::is_empty) {
                bail!("harness {name}: context {value}: model_suffix must not be empty");
            }
            if selects.window == Some(0) {
                bail!("harness {name}: context {value}: window must be above 0; omit it when unknown");
            }
        }
        if self.window == Some(0) {
            bail!("harness {name}: window must be above 0; omit it when unknown");
        }
        Ok(())
    }

    /// Bytes of the thread's history a session rebuilt for an ant with `settings` is seeded with: the ant's
    /// `history_budget`, else the one of its `context` value or one derived from that value's window, else the
    /// harness's, else one derived from the harness's window ([`crate::history::budget`]).
    pub(crate) fn history_budget(&self, settings: &ModelSettings) -> usize {
        let selected = settings.context.as_ref().and_then(|value| self.context.get(value));
        let window = selected.and_then(|value| value.window).or(self.window);
        let set = match selected {
            Some(value) if value.history_budget.is_some() || value.window.is_some() => value.history_budget,
            _ => self.history_budget,
        };
        crate::history::budget(settings.history_budget.or(set), window)
    }
}

impl fmt::Display for Harness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// Whether tool rules allow every tool: one of them is `*`.
pub fn allows_all(rules: &[String]) -> bool {
    rules.iter().any(|rule| rule.trim() == "*")
}

/// Environment variable names must be non-empty without `=` or NUL, values without NUL. Errors never show values.
pub(crate) fn validate_env(env: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in env {
        if name.is_empty() || name.contains(['=', '\0']) || value.contains('\0') {
            bail!("invalid harness environment variable {name:?}");
        }
    }
    Ok(())
}
