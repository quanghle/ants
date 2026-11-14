//! ACP client side: start harnesses over stdio, keep their sessions, and run prompts through them.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    AuthMethod, CancelNotification, ContentBlock, InitializeRequest, LoadSessionRequest, McpServer, NewSessionRequest,
    PromptRequest, PromptResponse, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelectOptions, SessionId,
    SessionModeState, SessionNotification, SessionUpdate, SetSessionConfigOptionRequest, SetSessionModeRequest,
    TextContent, ToolCall, ToolCallId, ToolCallUpdate,
};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, Client, DynConnectTo};
use ants_proto::{PlanStep, StatusUpdate, ThreadId};
use anyhow::{Context as _, Result, anyhow, bail};
use serde::Deserialize;
use tokio::sync::{Notify, mpsc, oneshot, watch};

use crate::permission::PermissionPolicy;

/// The harness of an ant that names none, in a file that names none.
pub const DEFAULT_HARNESS: &str = "copilot";

/// The built-in definitions, in the same form as a config file's `[harnesses.<name>]` tables.
pub const BUILTIN_HARNESSES: &str = include_str!("harnesses.toml");

/// A named agent harness ants drives over ACP on stdio: an executable and its arguments, run directly (no shell),
/// with the environment it adds, the session mode it must run in and the context settings it takes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Harness {
    #[serde(skip)]
    name: String,
    /// A path, or a bare name looked up on `PATH` when the harness starts.
    pub(crate) command: PathBuf,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    /// Added to the daemon's environment; the ant's own `env` wins over it.
    #[serde(default)]
    pub(crate) env: BTreeMap<String, String>,
    /// ACP session mode every session is switched to, so that tool requests reach the client instead of being
    /// approved by the harness itself; a harness that does not offer it fails the session.
    pub(crate) mode: Option<String>,
    /// The values an ant's `context` may take, and what each selects. Empty: the harness takes no `context`.
    #[serde(default)]
    pub(crate) context: BTreeMap<String, ContextValue>,
    /// The command a human runs to sign the harness in, named when it reports it is signed out.
    pub(crate) sign_in: Option<String>,
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
}

impl Harness {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The command a human runs to sign the harness in, if its definition names one.
    pub fn sign_in(&self) -> Option<&str> {
        self.sign_in.as_deref()
    }

    /// Whether `command` is a path rather than a name to look up on `PATH`.
    pub(crate) fn command_is_path(&self) -> bool {
        self.command.components().count() > 1
    }

    fn validate(&self) -> Result<()> {
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
        if self.mode.as_ref().is_some_and(|mode| mode.trim().is_empty()) {
            bail!("harness {name}: mode must not be blank; omit it to keep the harness's own mode");
        }
        if self.sign_in.as_ref().is_some_and(|command| command.trim().is_empty()) {
            bail!("harness {name}: sign_in must not be blank; omit it when the harness names no sign-in command");
        }
        for (value, selects) in &self.context {
            if value.trim().is_empty() {
                bail!("harness {name}: context values must not be blank");
            }
            if selects.model_suffix.as_ref().is_some_and(String::is_empty) {
                bail!("harness {name}: context {value}: model_suffix must not be empty");
            }
        }
        Ok(())
    }

    /// The harness process: its arguments with those of the ant's `context`, its environment with the ant's `env`
    /// over it.
    pub(crate) fn process(&self, env: &[(String, String)], settings: &ModelSettings) -> AcpAgentConfig {
        let context = settings.context.as_ref().and_then(|value| self.context.get(value));
        let args = self.args.iter().chain(context.into_iter().flat_map(|value| &value.args)).cloned();
        AcpAgentConfig::new(&self.command).args(args).envs(self.env.clone()).envs(env.iter().cloned())
    }

    fn agent(&self, env: &[(String, String)], settings: &ModelSettings) -> AcpAgent {
        AcpAgent::new(self.process(env, settings))
    }

    /// Where the harness's executable is, with the ant's `env` over the definition's over the daemon's: the path its
    /// command names, else the first match on `PATH`. None when it is not installed there; a relative path, which the
    /// harness's working directory resolves, counts as found.
    pub(crate) fn locate(&self, env: &[(String, String)]) -> Option<PathBuf> {
        if self.command_is_path() {
            let unsure = self.command.is_relative() || self.command.is_file();
            return unsure.then(|| self.command.clone());
        }
        let path = env.iter().rev().find(|(name, _)| name == "PATH").map(|(_, path)| path.into());
        let path = path.or_else(|| self.env.get("PATH").map(Into::into)).or_else(|| std::env::var_os("PATH"))?;
        let names: &[&str] = if cfg!(windows) { &["", ".exe", ".cmd", ".bat"] } else { &[""] };
        std::env::split_paths(&path).find_map(|dir| {
            names.iter().map(|ext| dir.join(format!("{}{ext}", self.command.display()))).find(|p| p.is_file())
        })
    }

    /// The command that signs the harness in: the definition's, else the one of a terminal auth method it offers, run
    /// as the harness with that method's arguments.
    fn sign_in_command(&self, methods: &[AuthMethod]) -> Option<String> {
        if let Some(command) = &self.sign_in {
            return Some(command.clone());
        }
        methods.iter().find_map(|method| match method {
            AuthMethod::Terminal(terminal) => {
                let args = self.args.iter().chain(&terminal.args);
                Some(
                    std::iter::once(self.command.display().to_string())
                        .chain(args.cloned())
                        .collect::<Vec<_>>()
                        .join(" "),
                )
            }
            _ => None,
        })
    }
}

impl fmt::Display for Harness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
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

/// The harnesses ants can run, by name: the built-in ones and those a config file defines.
#[derive(Debug, Clone, PartialEq)]
pub struct Harnesses(Arc<BTreeMap<String, Arc<Harness>>>);

static BUILTIN: LazyLock<Harnesses> = LazyLock::new(|| {
    Harnesses(Arc::default())
        .define(parse_definitions(BUILTIN_HARNESSES).expect("built-in harnesses parse"))
        .expect("built-in harnesses are valid")
});

/// `[harnesses.<name>]` tables, as a config file holds them.
pub fn parse_definitions(text: &str) -> Result<BTreeMap<String, Harness>> {
    #[derive(Deserialize)]
    struct Tables {
        harnesses: BTreeMap<String, Harness>,
    }
    Ok(toml::from_str::<Tables>(text)?.harnesses)
}

impl Default for Harnesses {
    fn default() -> Self {
        Self::builtin()
    }
}

impl Harnesses {
    /// The built-in harnesses alone ([`BUILTIN_HARNESSES`]).
    pub fn builtin() -> Self {
        BUILTIN.clone()
    }

    /// These harnesses with `definitions` added, each replacing a harness of the same name whole.
    pub fn define(&self, definitions: BTreeMap<String, Harness>) -> Result<Self> {
        let mut all = (*self.0).clone();
        for (name, mut harness) in definitions {
            harness.name.clone_from(&name);
            harness.validate()?;
            all.insert(name, Arc::new(harness));
        }
        Ok(Self(Arc::new(all)))
    }

    /// The harness named `name`.
    pub fn get(&self, name: &str) -> Result<Arc<Harness>> {
        self.0.get(name).cloned().ok_or_else(|| {
            let known: Vec<_> = self.0.keys().map(String::as_str).collect();
            anyhow!(
                "unknown harness {name:?}, expected one of {} or a [harnesses.{name}] table in the config file",
                known.join(", ")
            )
        })
    }
}

/// Explicit per-ant settings; absent values keep the harness's defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelSettings {
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    /// Provider-native context selection, not a token spending cap.
    pub context: Option<String>,
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

    fn model_id(&self, harness: &Harness) -> Option<String> {
        let suffix = self.context.as_ref().and_then(|value| harness.context.get(value)?.model_suffix.as_deref());
        self.model.as_ref().map(|model| match suffix {
            Some(suffix) => format!("{}{suffix}", model.trim_end_matches(suffix)),
            None => model.clone(),
        })
    }
}

/// `a`, `a or b`, `a, b or c`.
fn one_of(values: &[&str]) -> String {
    match values {
        [] => String::new(),
        [one] => (*one).to_owned(),
        [rest @ .., last] => format!("{} or {last}", rest.join(", ")),
    }
}

/// How long a [`handshake`] may take.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);

/// Phrases of a harness's error or stderr that mean it needs signing in, lowercase. Kept to those that cannot mean
/// anything else, such as an MCP server's or a proxy's authentication.
const SIGNED_OUT_TEXTS: &[&str] = &[
    "no authentication information found",
    "not logged in",
    "not signed in",
    "please log in",
    "please login",
    "please sign in",
    "please run /login",
    "login required",
];

/// A harness that cannot work until a human signs it in, as its ACP handshake or a prompt said: ACP's
/// `auth_required` error, or an error or stderr that says so ([`SIGNED_OUT_TEXTS`]). Nothing ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedOut {
    pub harness: String,
    /// The command that signs it in, if known ([`Harness::sign_in`], else a terminal auth method it offers).
    pub sign_in: Option<String>,
    /// What the harness said.
    pub detail: String,
}

impl fmt::Display for SignedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} is not signed in: {}", self.harness, self.detail)?;
        match &self.sign_in {
            Some(command) => write!(f, "; sign in with `{command}`"),
            None => Ok(()),
        }
    }
}

impl std::error::Error for SignedOut {}

/// Whether an ACP error from a harness means it needs signing in.
fn needs_sign_in(e: &agent_client_protocol::Error) -> bool {
    if e.code == agent_client_protocol::ErrorCode::AUTH_REQUIRED {
        return true;
    }
    let text = e.to_string().to_lowercase();
    SIGNED_OUT_TEXTS.iter().any(|phrase| text.contains(phrase))
}

/// `e`, from the harness of `launch` while doing `what`, as an error: a [`SignedOut`] one when it says the harness
/// needs signing in, given the auth methods the harness offered.
fn harness_error(
    launch: &Launch,
    what: &str,
    e: &agent_client_protocol::Error,
    methods: &[AuthMethod],
) -> anyhow::Error {
    if needs_sign_in(e) {
        let harness = &launch.harness;
        let signed_out = SignedOut {
            harness: harness.name.clone(),
            sign_in: harness.sign_in_command(methods),
            detail: e.to_string(),
        };
        return signed_out.into();
    }
    anyhow!("{} {what}: {e}", launch.harness)
}

/// What an ACP handshake with a harness found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    /// It answered `initialize`, describing itself, and opened a session: installed and signed in.
    Ready { agent: String },
    /// Its executable is neither at the path its command names nor on `PATH`.
    Missing,
    /// It needs a human to sign it in ([`SignedOut`]).
    SignedOut { detail: String },
    /// It failed otherwise, or took longer than a minute.
    Failed { detail: String },
}

/// The outcome of a [`handshake`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub harness: String,
    /// The command that signs it in, if known: the definition's, else a terminal auth method it offered.
    pub sign_in: Option<String>,
    pub readiness: Readiness,
}

/// Check that the harness of `launch` is installed and signed in: start it, complete the ACP handshake and open a
/// session in the launch's directory, with no MCP servers or settings, then stop it. Prompts nothing.
pub async fn handshake(launch: &Launch) -> Handshake {
    let harness = &launch.harness;
    let mut checked =
        Handshake { harness: harness.name.clone(), sign_in: harness.sign_in.clone(), readiness: Readiness::Missing };
    if launch.process_env.as_ref().is_some_and(|env| harness.locate(env).is_none()) {
        return checked;
    }
    let methods = Mutex::new(Vec::new());
    let connect = Client.v1().name("ants").connect_with((launch.connect)(), async |cx: ConnectionTo<Agent>| {
        let init = cx.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
        methods.lock().expect("auth methods").clone_from(&init.auth_methods);
        cx.send_request(NewSessionRequest::new(launch.cwd.clone())).block_task().await?;
        Ok(match &init.agent_info {
            Some(info) => format!("{} {}", info.name, info.version),
            None => "an agent that does not name itself".to_owned(),
        })
    });
    let result = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect).await;
    let methods = methods.into_inner().expect("auth methods");
    checked.sign_in = harness.sign_in_command(&methods);
    checked.readiness = match result {
        Ok(Ok(agent)) => Readiness::Ready { agent },
        Ok(Err(e)) if needs_sign_in(&e) => Readiness::SignedOut { detail: e.to_string() },
        Ok(Err(e)) => Readiness::Failed { detail: e.to_string() },
        Err(_) => Readiness::Failed { detail: format!("no handshake within {HANDSHAKE_TIMEOUT:?}") },
    };
    checked
}

/// Check `harness` as [`handshake`] does, in the current directory, and describe the agent.
pub async fn check(harness: &Arc<Harness>) -> Result<String> {
    let launch = Launch::new(
        harness.clone(),
        std::env::current_dir()?,
        Vec::new(),
        PermissionPolicy::reject_all(),
        ModelSettings::default(),
    );
    let checked = handshake(&launch).await;
    let harness = &launch.harness;
    match checked.readiness {
        Readiness::Ready { agent } => Ok(agent),
        Readiness::Missing => bail!("{harness}: {} is not installed (not found on PATH)", harness.command.display()),
        Readiness::SignedOut { detail } => {
            Err(SignedOut { harness: checked.harness, sign_in: checked.sign_in, detail }.into())
        }
        Readiness::Failed { detail } => bail!("{harness} ACP handshake: {detail}"),
    }
}

/// Makes the transport to a fresh harness: a process, or an in-process agent in tests.
type Connector = Arc<dyn Fn() -> DynConnectTo<Client> + Send + Sync>;

/// The MCP servers a session of a thread gets; the flag tells whether the harness takes HTTP ones.
pub type McpServers = Arc<dyn Fn(&ThreadId, bool) -> Vec<McpServer> + Send + Sync>;

/// How to start a harness and open sessions in it.
#[derive(Clone)]
pub struct Launch {
    harness: Arc<Harness>,
    settings: ModelSettings,
    connect: Connector,
    cwd: PathBuf,
    policy: PermissionPolicy,
    /// The thread the sessions serve, so a permission request can be escalated on it.
    thread: Option<ThreadId>,
    mcp: Option<McpServers>,
    instructions: String,
    /// The environment the harness process gets over the daemon's, to find its executable; None for an in-process
    /// agent.
    process_env: Option<Vec<(String, String)>>,
}

impl Launch {
    /// `harness` working in `cwd`, with its own environment and then `env` added to the daemon's.
    pub fn new(
        harness: Arc<Harness>,
        cwd: PathBuf,
        env: Vec<(String, String)>,
        policy: PermissionPolicy,
        settings: ModelSettings,
    ) -> Self {
        let (process, process_settings, process_env) = (harness.clone(), settings.clone(), env.clone());
        let connect: Connector = Arc::new(move || DynConnectTo::new(process.agent(&process_env, &process_settings)));
        let process_env = Some(env);
        Self {
            harness,
            settings,
            connect,
            cwd,
            policy,
            thread: None,
            mcp: None,
            instructions: String::new(),
            process_env,
        }
    }

    /// The same launch, for sessions serving `thread`.
    pub fn for_thread(&self, thread: &ThreadId) -> Self {
        Self { thread: Some(thread.clone()), ..self.clone() }
    }

    /// The same launch, with sessions working in `cwd`.
    pub fn in_dir(self, cwd: PathBuf) -> Self {
        Self { cwd, ..self }
    }

    /// The same launch, offering `mcp` to the sessions of each thread.
    pub fn with_mcp(self, mcp: McpServers) -> Self {
        Self { mcp: Some(mcp), ..self }
    }

    /// Prepend the ant's instructions to every prompt, including after session/load.
    pub fn with_instructions(self, instructions: String) -> Self {
        Self { instructions, ..self }
    }

    fn mcp_servers(&self, http: bool) -> Vec<McpServer> {
        match (&self.mcp, &self.thread) {
            (Some(mcp), Some(thread)) => mcp(thread, http),
            _ => Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_transport(connect: Connector, cwd: PathBuf, policy: PermissionPolicy) -> Self {
        let harness = Harness {
            name: "test".to_owned(),
            command: "test".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            mode: None,
            context: BTreeMap::new(),
            sign_in: None,
        };
        Self {
            harness: Arc::new(harness),
            settings: ModelSettings::default(),
            connect,
            cwd,
            policy,
            thread: None,
            mcp: None,
            instructions: String::new(),
            process_env: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_harness(self, harness: Arc<Harness>) -> Self {
        Self { harness, ..self }
    }
}

impl fmt::Debug for Launch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launch")
            .field("harness", &self.harness.name)
            .field("settings", &self.settings)
            .field("cwd", &self.cwd)
            .field("policy", &self.policy)
            .field("thread", &self.thread)
            .finish()
    }
}

/// Wall-clock time a prompt may run by default.
pub const DEFAULT_PROMPT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Tool calls a prompt may start by default.
pub const DEFAULT_MAX_TOOL_CALLS: usize = 300;
/// How long a harness has to end a cancelled prompt before it is stopped.
const CANCEL_GRACE: Duration = Duration::from_secs(30);

/// Limits every prompt runs under, whatever the harness. A prompt that reaches one is cancelled with `session/cancel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromptLimits {
    /// Wall-clock time from sending the prompt, tool requests waiting for the human included.
    pub timeout: Option<Duration>,
    /// Tool calls the prompt may start; the one past it cancels the prompt.
    pub max_tool_calls: Option<usize>,
}

impl Default for PromptLimits {
    fn default() -> Self {
        Self { timeout: Some(DEFAULT_PROMPT_TIMEOUT), max_tool_calls: Some(DEFAULT_MAX_TOOL_CALLS) }
    }
}

/// Spend a harness reports, for the caps in [`crate::caps`].
#[derive(Debug, Clone, PartialEq)]
pub enum Spend {
    /// Cumulative cost of the session so far, from ACP `usage_update`.
    Cost { session: SessionId, total: f64 },
    /// Tokens one prompt used, from its ACP prompt response.
    Tokens(u64),
}

/// One prompt to run, and what bounds it.
pub struct Prompt {
    pub text: String,
    pub limits: PromptLimits,
    /// Receives the spend the harness reports while the prompt runs, and a cost reported between prompts when the
    /// next one starts.
    pub spend: Option<mpsc::UnboundedSender<Spend>>,
    /// Cancels the prompt, with the reason sent.
    pub stop: Option<oneshot::Receiver<String>>,
    /// Receives the reply text, thoughts, tool calls and plan as the harness streams them. An update that finds the
    /// channel full is dropped rather than hold up the harness.
    pub progress: Option<mpsc::Sender<StatusUpdate>>,
}

impl Prompt {
    /// `text` under the default limits.
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into(), limits: PromptLimits::default(), spend: None, stop: None, progress: None }
    }
}

/// What a prompt came back with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub text: String,
    /// Why the prompt was cancelled, if it was.
    pub cut: Option<String>,
}

struct Turn {
    prompt: Prompt,
    reply: oneshot::Sender<Result<Reply>>,
}

/// A live harness holding one ACP session. Dropping it stops the harness once a running prompt is done.
pub struct Session {
    id: SessionId,
    turns: mpsc::Sender<Turn>,
    task: tokio::task::JoinHandle<()>,
}

impl Session {
    /// Start a harness and open a session in it: `resume` through session/load when the harness can, else a new one.
    pub async fn start(launch: &Launch, resume: Option<SessionId>) -> Result<Self> {
        let (opened_tx, opened) = oneshot::channel();
        let (turns, turns_rx) = mpsc::channel(1);
        let task = tokio::spawn(serve(launch.clone(), resume, opened_tx, turns_rx));
        let id = opened.await.map_err(|_| anyhow!("{} exited before opening a session", launch.harness))??;
        Ok(Self { id, turns, task })
    }

    pub fn id(&self) -> &SessionId {
        &self.id
    }

    /// False once the harness has exited or a prompt failed; the session can then only be resumed in a new one.
    pub fn is_alive(&self) -> bool {
        !self.turns.is_closed()
    }

    /// Run one prompt under the default limits and return the agent's reply text.
    pub async fn prompt(&self, text: &str) -> Result<String> {
        Ok(self.run(Prompt::new(text)).await?.text)
    }

    /// Run one prompt.
    pub async fn run(&self, prompt: Prompt) -> Result<Reply> {
        let (reply, answer) = oneshot::channel();
        self.turns.send(Turn { prompt, reply }).await.map_err(|_| anyhow!("harness session ended"))?;
        answer.await.map_err(|_| anyhow!("harness exited mid-prompt"))?
    }

    /// Stop the harness and wait for it to exit.
    pub async fn close(self) {
        drop(self.turns);
        if let Err(e) = self.task.await {
            tracing::warn!(error = %e, "harness task");
        }
    }
}

/// Run one prompt in a fresh session, and return the agent's reply text.
pub async fn prompt_once(launch: &Launch, text: &str) -> Result<String> {
    let session = Session::start(launch, None).await?;
    let reply = session.prompt(text).await;
    session.close().await;
    reply
}

/// The prompt in flight, as the session's notifications fill it in.
struct Current {
    text: String,
    /// What the session reported of each tool call, without its content or output.
    tool_calls: HashMap<ToolCallId, ToolCall>,
    max_tool_calls: Option<usize>,
    too_many: Arc<Notify>,
    spend: Option<mpsc::UnboundedSender<Spend>>,
    progress: Option<mpsc::Sender<StatusUpdate>>,
}

#[derive(Default)]
struct Observed {
    /// Some while a prompt runs. session/load replays the history as notifications; those find None and are dropped.
    current: Option<Current>,
    /// The session's last cumulative cost.
    cost: Option<f64>,
}

impl Observed {
    fn observe(&mut self, n: SessionNotification) {
        if let SessionUpdate::UsageUpdate(usage) = &n.update {
            if let Some(cost) = &usage.cost {
                self.cost = Some(cost.amount);
                if let Some(tx) = self.current.as_ref().and_then(|c| c.spend.as_ref()) {
                    let _ = tx.send(Spend::Cost { session: n.session_id.clone(), total: cost.amount });
                }
            }
            return;
        }
        let Some(current) = self.current.as_mut() else { return };
        if let Some(tx) = &current.progress
            && let Some(update) = progress(&n.update)
        {
            let _ = tx.try_send(update);
        }
        match n.update {
            SessionUpdate::AgentMessageChunk(chunk) => {
                if let ContentBlock::Text(t) = chunk.content {
                    current.text.push_str(&t.text);
                }
            }
            SessionUpdate::ToolCall(mut call) => {
                strip(&mut call);
                current.tool_calls.insert(call.tool_call_id.clone(), call);
                if current.max_tool_calls.is_some_and(|max| current.tool_calls.len() > max) {
                    current.too_many.notify_one();
                }
            }
            SessionUpdate::ToolCallUpdate(update) => {
                if let Some(call) = current.tool_calls.get_mut(&update.tool_call_id) {
                    call.update(update.fields);
                    strip(call);
                }
            }
            _ => {}
        }
    }

    /// Fill in what a permission request leaves out of its tool call from what the session reported of that call,
    /// as an ACP tool call update only carries the fields that change. Codex asks to run an MCP tool by its id alone.
    fn describe(&self, request: &mut ToolCallUpdate) {
        let Some(known) = self.current.as_ref().and_then(|c| c.tool_calls.get(&request.tool_call_id)) else { return };
        let fields = &mut request.fields;
        fields.kind.get_or_insert(known.kind);
        fields.title.get_or_insert_with(|| known.title.clone());
        if fields.name.is_none() {
            fields.name.clone_from(&known.name);
        }
        if fields.raw_input.is_none() {
            fields.raw_input.clone_from(&known.raw_input);
        }
        if fields.locations.is_none() && !known.locations.is_empty() {
            fields.locations = Some(known.locations.clone());
        }
    }
}

/// Drop a tool call's content and output, which permission requests do not need.
fn strip(call: &mut ToolCall) {
    call.content = Vec::new();
    call.raw_output = None;
}

/// The status update a session update streams, if any. A tool call update counts only when its title or status
/// changes, not for every piece of output.
fn progress(update: &SessionUpdate) -> Option<StatusUpdate> {
    Some(match update {
        SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
            ContentBlock::Text(t) => StatusUpdate::Message { text: t.text.clone() },
            _ => return None,
        },
        SessionUpdate::AgentThoughtChunk(chunk) => match &chunk.content {
            ContentBlock::Text(t) => StatusUpdate::Thought { text: t.text.clone() },
            _ => return None,
        },
        SessionUpdate::ToolCall(call) => StatusUpdate::Tool {
            id: call.tool_call_id.to_string(),
            title: Some(call.title.clone()),
            tool_kind: name(&call.kind),
            status: name(&call.status),
        },
        SessionUpdate::ToolCallUpdate(call) if call.fields.title.is_some() || call.fields.status.is_some() => {
            StatusUpdate::Tool {
                id: call.tool_call_id.to_string(),
                title: call.fields.title.clone(),
                tool_kind: call.fields.kind.as_ref().and_then(name),
                status: call.fields.status.as_ref().and_then(name),
            }
        }
        SessionUpdate::Plan(plan) => StatusUpdate::Plan {
            steps: plan
                .entries
                .iter()
                .map(|e| PlanStep { content: e.content.clone(), status: name(&e.status).unwrap_or_default() })
                .collect(),
        },
        _ => return None,
    })
}

/// The wire name of an ACP enum value.
fn name(value: &impl serde::Serialize) -> Option<String> {
    serde_json::to_value(value).ok()?.as_str().map(str::to_owned)
}

/// Own the harness connection: open the session, then run turns until the session is dropped or a prompt fails.
async fn serve(
    launch: Launch,
    resume: Option<SessionId>,
    opened: oneshot::Sender<Result<SessionId>>,
    mut turns: mpsc::Receiver<Turn>,
) {
    let observed: Arc<Mutex<Observed>> = Arc::default();
    let (sink, known) = (observed.clone(), observed.clone());
    // Bumped when a prompt is cancelled, which answers the permission requests still waiting.
    let (cancelled, cancels) = watch::channel(0u64);
    let (policy, thread, workdir) = (launch.policy.clone(), launch.thread.clone(), launch.cwd.clone());
    let mut opened = Some(opened);
    let methods = Mutex::new(Vec::new());
    let result = Client
        .v1()
        .name("ants")
        .on_receive_notification(
            async move |n: SessionNotification, _cx| {
                sink.lock().expect("observed").observe(n);
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |mut request: RequestPermissionRequest, responder, cx| {
                known.lock().expect("observed").describe(&mut request.tool_call);
                tracing::info!(tool = ?request.tool_call, "harness asked for permission");
                let (policy, thread, workdir) = (policy.clone(), thread.clone(), workdir.clone());
                let mut cancel = cancels.clone();
                cancel.mark_unchanged();
                // The human may take minutes; answer off the dispatch loop so the connection keeps flowing.
                cx.spawn(async move {
                    let response = tokio::select! {
                        response = policy.answer(&request, thread.as_ref(), &workdir) => response,
                        _ = cancel.changed() => RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled),
                    };
                    responder.respond(response)
                })
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with((launch.connect)(), async |cx: ConnectionTo<Agent>| {
            let init = cx.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
            methods.lock().expect("auth methods").clone_from(&init.auth_methods);
            let caps = &init.agent_capabilities;
            let mcp = launch.mcp_servers(caps.mcp_capabilities.http);
            let id = open(&cx, &launch, resume, caps.load_session, mcp).await?;
            if let Some(tx) = opened.take() {
                let _ = tx.send(Ok(id.clone()));
            }
            while let Some(Turn { prompt, reply }) = turns.recv().await {
                let Prompt { text, limits, spend, stop, progress } = prompt;
                let too_many = Arc::new(Notify::new());
                {
                    let mut observed = observed.lock().expect("observed");
                    // A cost reported after the last prompt ended counts now.
                    if let (Some(tx), Some(total)) = (&spend, observed.cost) {
                        let _ = tx.send(Spend::Cost { session: id.clone(), total });
                    }
                    observed.current = Some(Current {
                        text: String::new(),
                        tool_calls: HashMap::new(),
                        max_tool_calls: limits.max_tool_calls,
                        too_many: too_many.clone(),
                        spend: spend.clone(),
                        progress,
                    });
                }
                let text =
                    if launch.instructions.is_empty() { text } else { format!("{}\n\n{text}", launch.instructions) };
                let request = cx
                    .send_request(PromptRequest::new(id.clone(), vec![ContentBlock::Text(TextContent::new(text))]))
                    .block_task();
                let (done, cut) = bounded(&cx, &id, request, limits, &too_many, stop, &cancelled).await;
                let text = observed.lock().expect("observed").current.take().map(|c| c.text).unwrap_or_default();
                match done {
                    Some(Ok(done)) => {
                        tracing::debug!(session = %id, stop_reason = ?done.stop_reason, "prompt finished");
                        if let (Some(tx), Some(usage)) = (&spend, &done.usage) {
                            let _ = tx.send(Spend::Tokens(usage.total_tokens));
                        }
                        let _ = reply.send(Ok(Reply { text, cut }));
                    }
                    Some(Err(e)) if cut.is_none() => {
                        let error = harness_error(&launch, "prompt", &e, &methods.lock().expect("auth methods"));
                        let _ = reply.send(Err(error));
                        return Err(e);
                    }
                    // Cancelled, but the harness failed or did not end the prompt: stop it, the prompt is over.
                    _ => {
                        tracing::warn!(session = %id, "{} did not end a cancelled prompt; stopping it", launch.harness);
                        let _ = reply.send(Ok(Reply { text, cut }));
                        return Ok(());
                    }
                }
            }
            Ok(())
        })
        .await;
    if let Err(e) = result {
        match opened {
            Some(tx) => {
                let _ = tx.send(Err(harness_error(&launch, "session", &e, &methods.lock().expect("auth methods"))));
            }
            None => tracing::warn!(error = %e, "{} session ended", launch.harness),
        }
    }
}

/// Wait for a prompt's response, cancelling the prompt with `session/cancel` when it reaches a limit or `stop` fires.
/// Returns the response, None when a cancelled prompt did not end within the grace period, and why it was cancelled.
async fn bounded(
    cx: &ConnectionTo<Agent>,
    id: &SessionId,
    request: impl Future<Output = Result<PromptResponse, agent_client_protocol::Error>>,
    limits: PromptLimits,
    too_many: &Notify,
    stop: Option<oneshot::Receiver<String>>,
    cancelled: &watch::Sender<u64>,
) -> (Option<Result<PromptResponse, agent_client_protocol::Error>>, Option<String>) {
    tokio::pin!(request);
    let timeout = async {
        match limits.timeout {
            Some(t) => tokio::time::sleep(t).await,
            None => std::future::pending().await,
        }
    };
    let stop = async {
        match stop {
            Some(rx) => rx.await.ok(),
            None => None,
        }
    };
    let cut = tokio::select! {
        biased;
        done = &mut request => return (Some(done), None),
        () = timeout => format!("Prompt time limit reached ({:?})", limits.timeout.unwrap_or_default()),
        () = too_many.notified() => format!(
            "Tool call limit reached ({} tool calls in one prompt)",
            limits.max_tool_calls.unwrap_or_default()
        ),
        Some(reason) = stop => reason,
    };
    tracing::warn!(session = %id, reason = %cut, "cancelling the prompt");
    if let Err(e) = cx.send_notification(CancelNotification::new(id.clone())) {
        tracing::warn!(error = %e, "session/cancel");
    }
    cancelled.send_modify(|n| *n += 1);
    (tokio::time::timeout(CANCEL_GRACE, request).await.ok(), Some(cut))
}

async fn open(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    resume: Option<SessionId>,
    can_load: bool,
    mcp: Vec<McpServer>,
) -> Result<SessionId, agent_client_protocol::Error> {
    if let Some(id) = resume {
        if !can_load {
            tracing::warn!(session = %id, "{} cannot load sessions; starting a new one", launch.harness);
        } else {
            let load = LoadSessionRequest::new(id.clone(), launch.cwd.clone()).mcp_servers(mcp.clone());
            match cx.send_request(load).block_task().await {
                Ok(loaded) => {
                    configure(cx, launch, &id, loaded.modes, loaded.config_options.unwrap_or_default()).await?;
                    return Ok(id);
                }
                Err(e) => tracing::warn!(session = %id, error = %e, "session/load failed; starting a new session"),
            }
        }
    }
    let new = cx.send_request(NewSessionRequest::new(launch.cwd.clone()).mcp_servers(mcp)).block_task().await?;
    configure(cx, launch, &new.session_id, new.modes, new.config_options.unwrap_or_default()).await?;
    Ok(new.session_id)
}

async fn configure(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    id: &SessionId,
    modes: Option<SessionModeState>,
    mut options: Vec<SessionConfigOption>,
) -> Result<(), agent_client_protocol::Error> {
    launch
        .settings
        .validate(&launch.harness)
        .map_err(|e| agent_client_protocol::Error::internal_error().data(e.to_string()))?;
    set_mode(cx, launch, id, modes).await?;
    let model = launch.settings.model_id(&launch.harness);
    for (name, value) in
        [("model", model.as_deref()), ("reasoning_effort", launch.settings.reasoning_effort.as_deref())]
    {
        if let Some(value) = value {
            options = set_option(cx, id, name, value, options).await?;
        }
    }
    // Applying a later option must not silently reset an earlier explicit setting.
    for (name, value) in
        [("model", model.as_deref()), ("reasoning_effort", launch.settings.reasoning_effort.as_deref())]
    {
        if let Some(value) = value {
            check_option(&options, name, value)?;
        }
    }
    Ok(())
}

fn option<'a>(options: &'a [SessionConfigOption], name: &str) -> Option<&'a SessionConfigOption> {
    options.iter().find(|option| match name {
        "model" => option.id.0.as_ref() == "model" || option.category == Some(SessionConfigOptionCategory::Model),
        "reasoning_effort" => {
            matches!(option.id.0.as_ref(), "reasoning_effort" | "effort")
                || option.category == Some(SessionConfigOptionCategory::ThoughtLevel)
        }
        _ => false,
    })
}

fn check_option(options: &[SessionConfigOption], name: &str, value: &str) -> Result<(), agent_client_protocol::Error> {
    if option(options, name).is_some_and(
        |option| matches!(&option.kind, SessionConfigKind::Select(select) if select.current_value.0.as_ref() == value),
    ) {
        return Ok(());
    }
    Err(agent_client_protocol::Error::internal_error().data(format!("harness did not apply {name}={value:?}")))
}

async fn set_option(
    cx: &ConnectionTo<Agent>,
    id: &SessionId,
    name: &str,
    value: &str,
    options: Vec<SessionConfigOption>,
) -> Result<Vec<SessionConfigOption>, agent_client_protocol::Error> {
    let fail = |message: String| agent_client_protocol::Error::internal_error().data(message);
    let option = option(&options, name).ok_or_else(|| fail(format!("harness does not expose {name} configuration")))?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return Err(fail(format!("harness {name} is not a selectable option")));
    };
    let values: Vec<&str> = match &select.options {
        SessionConfigSelectOptions::Ungrouped(values) => values.iter().map(|v| v.value.0.as_ref()).collect(),
        SessionConfigSelectOptions::Grouped(groups) => {
            groups.iter().flat_map(|group| &group.options).map(|v| v.value.0.as_ref()).collect()
        }
        _ => return Err(fail(format!("harness {name} has an unsupported option format"))),
    };
    if !values.contains(&value) {
        return Err(fail(format!("unsupported {name}={value:?}; available values: {}", values.join(", "))));
    }
    let response =
        cx.send_request(SetSessionConfigOptionRequest::new(id.clone(), option.id.clone(), value)).block_task().await?;
    check_option(&response.config_options, name, value)?;
    Ok(response.config_options)
}

/// Put the session in the launch's mode, if it has one; a harness that does not offer it fails the session.
async fn set_mode(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    id: &SessionId,
    modes: Option<SessionModeState>,
) -> Result<(), agent_client_protocol::Error> {
    let Some(want) = launch.harness.mode.as_deref() else { return Ok(()) };
    let offered = modes.as_ref().is_some_and(|m| m.available_modes.iter().any(|mode| &*mode.id.0 == want));
    if !offered {
        return Err(agent_client_protocol::Error::internal_error()
            .data(format!("{} does not offer session mode {want}", launch.harness)));
    }
    if modes.is_some_and(|m| &*m.current_mode_id.0 == want) {
        return Ok(());
    }
    cx.send_request(SetSessionModeRequest::new(id.clone(), want.to_owned())).block_task().await?;
    tracing::debug!(session = %id, mode = want, "session mode set");
    Ok(())
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::v1::schema::{
        AgentCapabilities, AuthMethodTerminal, ContentChunk, Implementation, InitializeResponse, LoadSessionResponse,
        NewSessionResponse, PermissionOption, PermissionOptionKind, SessionConfigSelectGroup,
        SessionConfigSelectOption, SessionMode, SetSessionConfigOptionResponse, SetSessionModeResponse, StopReason,
        ToolCallStatus, ToolCallUpdateFields, ToolKind,
    };
    use serde_json::json;

    use super::*;
    use crate::permission::Allowlist;

    /// A fake harness starting its sessions in `auto`, offering `modes`; returns the modes it was switched to.
    fn launch(modes: &'static [&'static str]) -> (Launch, Arc<Mutex<Vec<String>>>) {
        let set = Arc::new(Mutex::new(Vec::new()));
        let log = set.clone();
        let connect: Connector = Arc::new(move || {
            let log = log.clone();
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("fake")
                    .on_receive_request(
                        async move |init: InitializeRequest, responder, _cx| {
                            responder.respond(InitializeResponse::new(init.protocol_version))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |_: NewSessionRequest, responder, _cx| {
                            let offered = modes.iter().map(|m| SessionMode::new(*m, *m)).collect();
                            responder
                                .respond(NewSessionResponse::new("s1").modes(SessionModeState::new("auto", offered)))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |req: SetSessionModeRequest, responder, _cx| {
                            log.lock().unwrap().push(req.mode_id.0.to_string());
                            responder.respond(SetSessionModeResponse::new())
                        },
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        let custom = custom("command = \"custom-acp\"\nmode = \"default\"");
        (Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_harness(custom), set)
    }

    /// The harness a config file defines as `[harnesses.custom]` with `table`.
    fn custom(table: &str) -> Arc<Harness> {
        let definitions = parse_definitions(&format!("[harnesses.custom]\n{table}")).unwrap();
        Harnesses::builtin().define(definitions).unwrap().get("custom").unwrap()
    }

    fn builtin(name: &str) -> Arc<Harness> {
        Harnesses::builtin().get(name).unwrap()
    }

    #[tokio::test]
    async fn sessions_are_switched_to_the_harness_mode() {
        let (launch, set) = launch(&["auto", "default"]);
        Session::start(&launch, None).await.unwrap().close().await;
        assert_eq!(*set.lock().unwrap(), ["default"]);
    }

    #[tokio::test]
    async fn a_harness_without_the_mode_fails_the_session() {
        let (launch, set) = launch(&["auto"]);
        let err = Session::start(&launch, None).await.err().expect("session should fail").to_string();
        assert!(err.contains("does not offer session mode default"), "{err}");
        assert!(set.lock().unwrap().is_empty());
    }

    /// A fake harness offering to sign in on a terminal with `--login`, whose session/new fails with `refusal` when
    /// there is one.
    fn signing(refusal: Option<agent_client_protocol::Error>) -> Launch {
        let connect: Connector = Arc::new(move || {
            let refusal = refusal.clone();
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("fake")
                    .on_receive_request(
                        async move |init: InitializeRequest, responder, _cx| {
                            let login = AuthMethodTerminal::new("login", "Log in").args(vec!["--login".to_owned()]);
                            let init = InitializeResponse::new(init.protocol_version)
                                .auth_methods(vec![AuthMethod::Terminal(login)])
                                .agent_info(Implementation::new("fake", "1.0"));
                            responder.respond(init)
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |_: NewSessionRequest, responder, _cx| match refusal.clone() {
                            Some(e) => responder.respond_with_error(e),
                            None => responder.respond(NewSessionResponse::new("s1")),
                        },
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        Launch::with_transport(connect, PathBuf::from("/"), Default::default())
    }

    #[tokio::test]
    async fn a_signed_out_harness_is_told_apart_from_a_failing_one() {
        let ready = handshake(&signing(None)).await;
        assert_eq!(ready.readiness, Readiness::Ready { agent: "fake 1.0".to_owned() });
        assert_eq!(ready.sign_in.as_deref(), Some("test --login"), "the terminal auth method it offers");

        let required = signing(Some(agent_client_protocol::Error::auth_required()));
        assert!(matches!(handshake(&required).await.readiness, Readiness::SignedOut { .. }));
        let err = Session::start(&required, None).await.err().expect("a signed-out session");
        let signed_out = err.downcast_ref::<SignedOut>().expect("a sign-in error");
        assert_eq!((signed_out.harness.as_str(), signed_out.sign_in.as_deref()), ("test", Some("test --login")));
        let named = required.with_harness(custom("command = \"custom-acp\"\nsign_in = \"custom-acp login\""));
        assert_eq!(handshake(&named).await.sign_in.as_deref(), Some("custom-acp login"), "the definition's wins");

        // An adapter that exits on start: its stderr comes with the error.
        let exited = |stderr: &str| {
            agent_client_protocol::Error::internal_error().data(format!("Process exited with exit status: 1: {stderr}"))
        };
        let stderr = signing(Some(exited("Error: No authentication information found.")));
        assert!(matches!(handshake(&stderr).await.readiness, Readiness::SignedOut { .. }));
        let proxy = signing(Some(exited("Proxy authentication required")));
        assert!(matches!(handshake(&proxy).await.readiness, Readiness::Failed { .. }), "no sign-in of the harness");
        let err = Session::start(&proxy, None).await.err().expect("a failing session");
        assert!(err.downcast_ref::<SignedOut>().is_none(), "{err:#}");
    }

    #[tokio::test]
    async fn a_harness_not_installed_is_missing() {
        let on_path = |command: &str| {
            let harness = custom(&format!("command = \"{command}\""));
            let env = vec![("PATH".to_owned(), "/nonexistent-ants-bin".to_owned())];
            Launch::new(harness, PathBuf::from("/"), env, Default::default(), ModelSettings::default())
        };
        assert_eq!(handshake(&on_path("no-such-acp")).await.readiness, Readiness::Missing);
        assert_eq!(handshake(&on_path("/nonexistent-ants-bin/acp")).await.readiness, Readiness::Missing);
    }

    /// A fake harness asking permission as Codex does for an MCP tool: it reports the call, then asks by the call's
    /// id alone. Replies with the option chosen.
    fn codex(policy: PermissionPolicy) -> Launch {
        let connect: Connector = Arc::new(|| {
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("codex")
                    .on_receive_request(
                        async move |init: InitializeRequest, responder, _cx| {
                            responder.respond(InitializeResponse::new(init.protocol_version))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |_: NewSessionRequest, responder, _cx| {
                            responder.respond(NewSessionResponse::new("s1"))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |prompt: PromptRequest, responder, cx| {
                            let sender = cx.clone();
                            cx.spawn(async move {
                                let session = prompt.session_id;
                                let input = json!({ "server": "ants", "tool": "send_message", "arguments": {} });
                                let call = ToolCall::new("c1", "mcp.ants.send_message")
                                    .kind(ToolKind::Execute)
                                    .raw_input(input);
                                sender.send_notification(SessionNotification::new(
                                    session.clone(),
                                    SessionUpdate::ToolCall(call),
                                ))?;
                                let fields =
                                    ToolCallUpdateFields::new().kind(ToolKind::Execute).status(ToolCallStatus::Pending);
                                let options = vec![
                                    PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
                                    PermissionOption::new("cancel", "Cancel", PermissionOptionKind::RejectOnce),
                                ];
                                let ask = RequestPermissionRequest::new(
                                    session.clone(),
                                    ToolCallUpdate::new("c1", fields),
                                    options,
                                );
                                let chosen = match sender.send_request(ask).block_task().await?.outcome {
                                    RequestPermissionOutcome::Selected(s) => s.option_id.0.to_string(),
                                    _ => "cancelled".to_owned(),
                                };
                                sender.send_notification(SessionNotification::new(
                                    session,
                                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                                        TextContent::new(chosen),
                                    ))),
                                ))?;
                                responder.respond(PromptResponse::new(StopReason::EndTurn))
                            })
                        },
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        Launch::with_transport(connect, PathBuf::from("/"), policy)
    }

    #[tokio::test]
    async fn permission_requests_are_matched_with_the_tool_call_they_name() {
        for (rule, chosen) in [("other:send_message", "allow"), ("execute", "cancel"), ("other:check_inbox", "cancel")]
        {
            let policy = PermissionPolicy::new(Allowlist::parse(&[rule]).unwrap(), None);
            let session = Session::start(&codex(policy), None).await.unwrap();
            assert_eq!(session.prompt("go").await.unwrap(), chosen, "{rule}");
            session.close().await;
        }
    }

    fn command_line(
        harness: &Harness,
        env: &[(&str, &str)],
        context: Option<&str>,
    ) -> (String, Vec<String>, Vec<String>) {
        let env: Vec<_> = env.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
        let settings = ModelSettings { context: context.map(str::to_owned), ..Default::default() };
        let process = harness.process(&env, &settings);
        let env = process.environment().iter().map(|(k, v)| format!("{k}={v}")).collect();
        (process.command().display().to_string(), process.arguments().to_vec(), env)
    }

    #[test]
    fn builtin_harnesses_launch_as_before() {
        let strings = |items: &[&str]| items.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        let cases = [
            ("copilot", None, "copilot", &["--acp"][..], None),
            ("copilot", Some("long_context"), "copilot", &["--acp", "--context", "long_context"][..], None),
            ("copilot", Some("default"), "copilot", &["--acp", "--context", "default"][..], None),
            ("claude", Some("1m"), "claude-agent-acp", &[][..], Some("default")),
            ("antigravity", None, "agy", &["acp", "--disable-telemetry"][..], Some("default")),
        ];
        for (name, context, command, args, mode) in cases {
            let harness = builtin(name);
            assert_eq!(command_line(&harness, &[], context), (command.to_owned(), strings(args), Vec::new()), "{name}");
            assert_eq!(harness.mode.as_deref(), mode, "{name}");
        }
        let claude =
            ModelSettings { model: Some("smart".to_owned()), context: Some("1m".to_owned()), ..Default::default() };
        assert_eq!(claude.model_id(&builtin("claude")).as_deref(), Some("smart[1m]"));
        assert_eq!(claude.model_id(&builtin("copilot")).as_deref(), Some("smart"));
    }

    #[test]
    fn a_custom_adapter_launches_with_its_arguments_and_environment() {
        let harness = custom(
            "command = \"/opt/acp/bin/my-adapter\"\nargs = [\"--stdio\", \"--log=warn\"]\n\
             env = { ADAPTER_HOME = \"/opt/acp\", SHARED = \"definition\" }\n\
             [harnesses.custom.context]\nwide = { args = [\"--window\", \"wide\"], model_suffix = \"-wide\" }",
        );
        let (command, args, env) = command_line(&harness, &[("SHARED", "ant"), ("OWN", "1")], None);
        assert_eq!(command, "/opt/acp/bin/my-adapter");
        assert_eq!(args, ["--stdio", "--log=warn"]);
        assert_eq!(env, ["ADAPTER_HOME=/opt/acp", "OWN=1", "SHARED=ant"], "the ant's env wins over the definition's");
        assert_eq!(command_line(&harness, &[], Some("wide")).1, ["--stdio", "--log=warn", "--window", "wide"]);

        let wide = |model: Option<&str>| ModelSettings {
            model: model.map(str::to_owned),
            context: Some("wide".to_owned()),
            ..Default::default()
        };
        assert_eq!(wide(Some("m")).model_id(&harness).as_deref(), Some("m-wide"));
        let err = wide(None).validate(&harness).unwrap_err().to_string();
        assert!(err.contains("harness custom: context wide requires an explicit model"), "{err}");
        let narrow = ModelSettings { context: Some("narrow".to_owned()), ..Default::default() };
        assert!(narrow.validate(&harness).unwrap_err().to_string().contains("context must be wide"));
        let plain = custom("command = \"my-adapter\"");
        let err = wide(Some("m")).validate(&plain).unwrap_err().to_string();
        assert!(err.contains("harness custom has no context setting"), "{err}");
        assert!(ModelSettings::default().validate(&plain).is_ok());
    }

    #[test]
    fn definitions_are_checked_and_replace_builtins_whole() {
        let define = |text: &str| parse_definitions(text).and_then(|defs| Harnesses::builtin().define(defs));
        let err = |text: &str| format!("{:#}", define(text).unwrap_err());
        assert!(err("[harnesses.custom]\ncommand = \"\"").contains("harness custom: command must not be empty"));
        assert!(err("[harnesses.custom]\nargs = [\"x\"]").contains("missing field `command`"));
        assert!(err("[harnesses.custom]\ncommand = \"x\"\nshell = true").contains("unknown field `shell`"));
        assert!(err("[harnesses.custom]\ncommand = \"x\"\nargs = [\"a\\u0000\"]").contains("must not contain NUL"));
        assert!(err("[harnesses.custom]\ncommand = \"x\"\nmode = \" \"").contains("mode must not be blank"));
        assert!(err("[harnesses.custom]\ncommand = \"x\"\nsign_in = \"\"").contains("sign_in must not be blank"));
        assert_eq!(builtin("copilot").sign_in(), Some("copilot login"));
        let env = err("[harnesses.custom]\ncommand = \"x\"\nenv = { 'A=B' = \"secret\" }");
        assert!(env.contains("harness custom") && env.contains("invalid harness environment variable"), "{env}");
        assert!(!env.contains("secret"), "{env}");
        assert!(err("[harnesses.\"My Agent\"]\ncommand = \"x\"").contains("use lowercase letters"));
        let suffix = err("[harnesses.custom]\ncommand = \"x\"\ncontext = { big = { model_suffix = \"\" } }");
        assert!(suffix.contains("model_suffix must not be empty"), "{suffix}");

        let harnesses = define("[harnesses.copilot]\ncommand = \"/usr/local/bin/copilot-acp\"").unwrap();
        let copilot = harnesses.get("copilot").unwrap();
        assert_eq!((copilot.name(), copilot.args.is_empty(), copilot.context.is_empty()), ("copilot", true, true));
        assert_eq!(harnesses.get("claude").unwrap(), builtin("claude"));
        let unknown = harnesses.get("vim").unwrap_err().to_string();
        assert!(unknown.contains("unknown harness \"vim\", expected one of antigravity, claude, copilot"), "{unknown}");
    }

    #[derive(Clone, Copy)]
    enum ConfigBehavior {
        Apply,
        NoOptions,
        Ignore,
        ResetModel,
    }

    type ConfigCalls = Arc<Mutex<Vec<(String, String)>>>;

    fn model_options(model: &str, effort: &str) -> Vec<SessionConfigOption> {
        let models = ["base", "smart", "smart[1m]"]
            .into_iter()
            .map(|value| SessionConfigSelectOption::new(value, value))
            .collect();
        let efforts = if model == "base" { vec!["low"] } else { vec!["medium", "high"] };
        vec![
            SessionConfigOption::select(
                "model",
                "Model",
                model.to_owned(),
                vec![SessionConfigSelectGroup::new("available", "Available models", models)],
            )
            .category(SessionConfigOptionCategory::Model),
            SessionConfigOption::select(
                "provider-effort",
                "Thinking level",
                effort.to_owned(),
                efforts.into_iter().map(|value| SessionConfigSelectOption::new(value, value)).collect::<Vec<_>>(),
            )
            .category(SessionConfigOptionCategory::ThoughtLevel),
        ]
    }

    fn configurable(harness: &str, settings: ModelSettings, behavior: ConfigBehavior) -> (Launch, ConfigCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let log = calls.clone();
        let connect: Connector = Arc::new(move || {
            let options =
                if matches!(behavior, ConfigBehavior::NoOptions) { vec![] } else { model_options("base", "low") };
            let initial = options.clone();
            let loaded = options.clone();
            let state = Arc::new(Mutex::new(options));
            let log = log.clone();
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("configurable")
                    .on_receive_request(
                        async move |init: InitializeRequest, responder, _cx| {
                            responder.respond(
                                InitializeResponse::new(init.protocol_version)
                                    .agent_capabilities(AgentCapabilities::new().load_session(true)),
                            )
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |_: NewSessionRequest, responder, _cx| {
                            responder.respond(NewSessionResponse::new("s1").config_options(initial.clone()))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |_: LoadSessionRequest, responder, _cx| {
                            responder.respond(LoadSessionResponse::new().config_options(loaded.clone()))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |request: SetSessionConfigOptionRequest, responder, _cx| {
                            let value = request.value.as_value_id().unwrap().0.to_string();
                            let config_id = request.config_id.0.as_ref();
                            log.lock().unwrap().push((config_id.to_owned(), value.clone()));
                            let mut options = state.lock().unwrap();
                            if !matches!(behavior, ConfigBehavior::Ignore) {
                                if config_id == "model" {
                                    *options = model_options(&value, "medium");
                                } else {
                                    let model = if matches!(behavior, ConfigBehavior::ResetModel) {
                                        "base".to_owned()
                                    } else {
                                        let SessionConfigKind::Select(select) = &options[0].kind else {
                                            unreachable!()
                                        };
                                        select.current_value.0.to_string()
                                    };
                                    *options = model_options(&model, &value);
                                }
                            }
                            responder.respond(SetSessionConfigOptionResponse::new(options.clone()))
                        },
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        let mut launch = Launch::with_transport(connect, PathBuf::from("/"), Default::default());
        // The fake offers no session modes.
        launch.harness = Arc::new(Harness { mode: None, ..(*builtin(harness)).clone() });
        launch.settings = settings;
        (launch, calls)
    }

    #[tokio::test]
    async fn model_then_effort_are_applied_to_new_and_resumed_sessions() {
        let settings =
            ModelSettings { model: Some("smart".to_owned()), reasoning_effort: Some("high".to_owned()), context: None };
        for harness in ["copilot", "claude", "antigravity"] {
            let (launch, calls) = configurable(harness, settings.clone(), ConfigBehavior::Apply);
            Session::start(&launch, None).await.unwrap().close().await;
            Session::start(&launch, Some("s1".into())).await.unwrap().close().await;
            let calls = calls.lock().unwrap();
            assert_eq!(
                calls.as_slice(),
                [
                    ("model".to_owned(), "smart".to_owned()),
                    ("provider-effort".to_owned(), "high".to_owned()),
                    ("model".to_owned(), "smart".to_owned()),
                    ("provider-effort".to_owned(), "high".to_owned()),
                ]
            );
        }
    }

    #[tokio::test]
    async fn explicit_settings_never_silently_fall_back() {
        for (behavior, model, effort, expected) in [
            (ConfigBehavior::NoOptions, "smart", None, "does not expose model"),
            (ConfigBehavior::Apply, "missing", None, "unsupported model"),
            (ConfigBehavior::Apply, "smart", Some("max"), "unsupported reasoning_effort"),
            (ConfigBehavior::Ignore, "smart", None, "did not apply model"),
            (ConfigBehavior::ResetModel, "smart", Some("high"), "did not apply model"),
        ] {
            let settings = ModelSettings {
                model: Some(model.to_owned()),
                reasoning_effort: effort.map(str::to_owned),
                context: None,
            };
            let (launch, _) = configurable("copilot", settings, behavior);
            for resume in [None, Some("s1".into())] {
                let err = Session::start(&launch, resume).await.err().expect("settings should fail").to_string();
                assert!(err.contains(expected), "{err}");
            }
        }
    }

    #[tokio::test]
    async fn claude_context_selects_only_an_advertised_model_variant() {
        for (model, expected) in [("smart", Some("smart[1m]")), ("smart[1m]", Some("smart[1m]")), ("base", None)] {
            let settings =
                ModelSettings { model: Some(model.to_owned()), context: Some("1m".to_owned()), ..Default::default() };
            let (launch, calls) = configurable("claude", settings, ConfigBehavior::Apply);
            let session = Session::start(&launch, None).await;
            if let Some(expected) = expected {
                session.unwrap().close().await;
                assert_eq!(calls.lock().unwrap()[0], ("model".to_owned(), expected.to_owned()));
            } else {
                let err = session.err().expect("unavailable context variant must fail").to_string();
                assert!(err.contains("unsupported model") && err.contains("base[1m]"), "{err}");
                assert!(calls.lock().unwrap().is_empty());
            }
        }
    }
}
