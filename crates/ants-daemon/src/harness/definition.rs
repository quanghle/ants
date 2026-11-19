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
    /// How a session reviewing for a sage starts instead ([`ants_proto::REVIEW_ALLOW`]): keeping its tools read only,
    /// or sending its tool requests to ants. Unset, a harness that runs its tools unasked declines reviews.
    pub(crate) review: Option<Variant>,
    /// Whether the ants running it approve its tools as its arguments and mode make it, or ask ants about what it
    /// would ask a human ([`Tools`]); an ant's own `tools` wins over it.
    #[serde(default)]
    pub(crate) tools: Tools,
    /// How a session starts when its ant asks ants about its tools (`tools = "ask"`): in a mode of the harness's own
    /// that decides what is safe and asks about the rest. Unset, a harness that runs its tools unasked cannot ask.
    pub(crate) ask: Option<Variant>,
    /// The values an ant's `context` may take, and what each selects. Empty: the harness takes no `context`.
    #[serde(default)]
    pub(crate) context: BTreeMap<String, ContextValue>,
    /// The command a human runs to sign the harness in, named when it reports it is signed out.
    pub(crate) sign_in: Option<String>,
    /// For protocol copilot-sdk, the Application (client) ID of a Microsoft Entra app registration, sent on the SDK's
    /// handshake so the CLI can sign in to MCP servers authenticating through Entra.
    pub(crate) entra_client_id: Option<String>,
    /// For protocol copilot-sdk, the MCP servers a session serving a thread signs in to with OAuth whenever one needs
    /// it, by the names the session gives them.
    #[serde(default)]
    pub(crate) mcp_sign_in: Vec<String>,
    /// Phrases of its errors that mean a provider rate limit or overload, besides the common ones
    /// ([`crate::limits::ratelimit`]).
    #[serde(default)]
    pub(crate) rate_limited: Vec<String>,
    /// Phrases of its errors that mean an exhausted provider quota, besides the common ones.
    #[serde(default)]
    pub(crate) quota_exhausted: Vec<String>,
    /// Phrases of its prompt errors that mean the session outgrew the model's context window, besides the ones every
    /// harness shares ([`crate::limits::overflow`]).
    #[serde(default)]
    pub(crate) context_overflow: Vec<String>,
    /// Tokens of the model's context window, when known: sizes the history a rebuilt session is seeded with.
    pub(crate) window: Option<u64>,
    /// Bytes of the thread's history a rebuilt session is seeded with ([`crate::stream::history::budget`]).
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

/// The arguments and mode a session starts with in place of the harness's, each when set: a review session
/// ([`Harness::review`]) or one asking ants about its tools ([`Harness::ask`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Variant {
    pub(crate) args: Option<Vec<String>>,
    pub(crate) mode: Option<String>,
}

/// What becomes of the tool calls a harness would ask a human about: an ant's `tools`, else its harness's.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Tools {
    /// The harness runs as its arguments and mode make it, the built-ins approving every tool.
    #[default]
    AutoApprove,
    /// The harness runs its [`Harness::ask`] variant and sends what it would ask a human to ants: the ant's allow
    /// rules, then its approvers, else rejected.
    Ask,
}

impl Tools {
    /// Its name, as the `tools` key takes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AutoApprove => "auto-approve",
            Self::Ask => "ask",
        }
    }
}

impl fmt::Display for Tools {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Harness {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether its protocol reports the MCP servers of its sessions ([`super::open_mcp`]).
    pub fn reports_mcp(&self) -> bool {
        super::native::reports_mcp(self.protocol)
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

    /// Whether it runs its own tools without asking ants ([`Self::bypass_permissions`]), as a harness of a native
    /// protocol does unless it asks ants ([`Tools::Ask`]): the CLI's own permission settings decide.
    pub fn bypasses_permissions(&self) -> bool {
        self.bypass_permissions || (self.protocol.is_native() && self.tools != Tools::Ask)
    }

    /// The `tools` its definition sets for the ants running it.
    pub fn tools(&self) -> Tools {
        self.tools
    }

    /// The harness an ant runs with `tools`, else with the definition's: itself, approving its tools as its arguments
    /// and mode make it, or its variant asking ants about them ([`Self::ask`]).
    pub fn with_tools(&self, tools: Option<Tools>) -> Result<Self> {
        match tools.unwrap_or(self.tools) {
            Tools::AutoApprove => Ok(Self { tools: Tools::AutoApprove, ..self.clone() }),
            Tools::Ask => self.asking(),
        }
    }

    /// Its variant sending what it would ask a human about its tools to ants: its [`Self::ask`] arguments and mode,
    /// else its own when it asks the client already or speaks a native protocol whose translator forwards what the
    /// CLI asks.
    fn asking(&self) -> Result<Self> {
        let name = &self.name;
        if !native::asks(self.protocol) {
            bail!(
                "harness {name}: protocol {} has no hook to send its tool requests to ants, so tools = \"ask\" does \
                 not apply to it",
                self.protocol
            );
        }
        if self.ask.is_none() && self.bypass_permissions && !self.protocol.is_native() {
            bail!(
                "harness {name} runs its tools unasked and its definition sets no ask table to start it asking, so \
                 tools = \"ask\" does not apply to it: {}",
                self.ask_fix()
            );
        }
        let ask = self.ask.clone().unwrap_or_default();
        Ok(Self {
            args: ask.args.unwrap_or_else(|| self.args.clone()),
            mode: ask.mode.or_else(|| self.mode.clone()),
            bypass_permissions: false,
            tools: Tools::Ask,
            ask: None,
            ..self.clone()
        })
    }

    /// What to change for the harness to ask ants about its tools.
    pub fn ask_fix(&self) -> String {
        format!(
            "set ask in [harnesses.{}] to the args or mode that make it ask before running tools it deems unsafe \
             (docs/config.md#asking-ants-about-tools)",
            self.name
        )
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

    /// The harness a review session runs: its [`Self::review`] variant, else itself when it asks before running tools.
    /// None when it runs them unasked and sets no review, so nothing holds a review to read-only tools.
    pub fn reviewing(&self) -> Option<Self> {
        let Some(review) = &self.review else { return (!self.bypasses_permissions()).then(|| self.clone()) };
        Some(Self {
            args: review.args.clone().unwrap_or_else(|| self.args.clone()),
            mode: review.mode.clone().or_else(|| self.mode.clone()),
            bypass_permissions: false,
            review: None,
            ..self.clone()
        })
    }

    /// Why a review sent to an ant on this harness is declined, when nothing holds it to read-only tools
    /// ([`Self::reviewing`]).
    pub fn unheld_review(&self) -> Option<String> {
        self.reviewing().is_none().then(|| {
            format!(
                "harness {} runs its tools unasked and its definition sets no review to hold a review to read-only \
                 tools",
                self.name
            )
        })
    }

    /// What to change for the harness to hold reviews to read-only tools.
    pub fn review_fix(&self) -> String {
        format!(
            "set review in [harnesses.{}] to the args or mode that keep its tools read only or make it ask before \
             running them (docs/config.md#harnesses)",
            self.name
        )
    }

    /// What to change for the ant's tool gating to reach the harness's tools, or, for a native protocol, which no
    /// tool request leaves, where to restrict them.
    pub fn gated_fix(&self) -> String {
        if self.with_tools(Some(Tools::Ask)).is_ok() {
            return format!(
                "set tools = \"ask\" on the ant or in [harnesses.{}] to send the tool calls it would ask a human \
                 about to ants (docs/config.md#asking-ants-about-tools)",
                self.name
            );
        }
        if self.protocol.is_native() {
            return format!(
                "restrict its tools with the native permission settings of [harnesses.{}] (protocol {}: its mode and \
                 arguments; docs/config.md#harness-protocols), or pick a harness speaking ACP",
                self.name, self.protocol
            );
        }
        format!(
            "{}, or redefine it without bypass_permissions, with a mode or arguments that ask before running tools \
             (docs/config.md#harnesses), or pick another harness",
            self.ask_fix()
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
        if let Some(id) = &self.entra_client_id {
            if self.protocol != Protocol::CopilotSdk {
                bail!("harness {name}: entra_client_id applies only to protocol copilot-sdk, not {}", self.protocol);
            }
            if id.trim().is_empty() {
                bail!(
                    "harness {name}: entra_client_id must not be blank; omit it when no Microsoft Entra app \
                     registration signs MCP servers in"
                );
            }
        }
        if !self.mcp_sign_in.is_empty() && self.protocol != Protocol::CopilotSdk {
            bail!("harness {name}: mcp_sign_in applies only to protocol copilot-sdk, not {}", self.protocol);
        }
        for (i, server) in self.mcp_sign_in.iter().enumerate() {
            if server.trim().is_empty() {
                bail!("harness {name}: mcp_sign_in names must not be blank");
            }
            if self.mcp_sign_in[..i].contains(server) {
                bail!("harness {name}: mcp_sign_in names {server} twice");
            }
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
        if let Some(review) = &self.review {
            if review.args.is_none() && review.mode.is_none() {
                bail!("harness {name}: review must set args or mode");
            }
            if let Some(reviewing) = self.reviewing() {
                reviewing.validate().with_context(|| format!("harness {name}: review"))?;
            }
        }
        if let Some(ask) = &self.ask
            && ask.args.is_none()
            && ask.mode.is_none()
        {
            bail!("harness {name}: ask must set args or mode");
        }
        if self.ask.is_some() {
            self.asking()?.validate().with_context(|| format!("harness {name}: ask"))?;
        } else if self.tools == Tools::Ask {
            self.asking()?;
        }
        Ok(())
    }

    /// Bytes of the thread's history a session rebuilt for an ant with `settings` is seeded with: the ant's
    /// `history_budget`, else the one of its `context` value or one derived from that value's window, else the
    /// harness's, else one derived from the harness's window ([`crate::stream::history::budget`]).
    pub(crate) fn history_budget(&self, settings: &ModelSettings) -> usize {
        let selected = settings.context.as_ref().and_then(|value| self.context.get(value));
        let window = selected.and_then(|value| value.window).or(self.window);
        let set = match selected {
            Some(value) if value.history_budget.is_some() || value.window.is_some() => value.history_budget,
            _ => self.history_budget,
        };
        crate::stream::history::budget(settings.history_budget.or(set), window)
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
