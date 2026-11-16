//! Native harness protocols: a harness definition whose CLI speaks its own protocol rather than ACP (its `protocol` key)
//! runs behind an in-process ACP agent that starts the CLI and translates, so the daemon's ACP client ([`super`]), and
//! with it the pool, fallbacks, sign-in, provider limits, context overflow, caps, status updates, MCP servers and
//! `ants harness check`, drive every harness alike.
//!
//! A protocol implements [`Native`] and [`NativeSession`] in a file of its own under `native/`, and is registered by
//! one arm of [`registration`]. This module answers ACP for it: `initialize` (naming the CLI and offering the
//! definition's sign-in), `session/new`, `session/load` and `session/fork` (advertised as the protocol supports them),
//! `session/prompt`, `session/cancel`, and the session mode and model and reasoning effort selectors the daemon sets.
//! It never asks the client for permission: tool approval is the CLI's own, set by the definition ([`deny_approval`]).

#![allow(
    dead_code,
    unused_imports,
    reason = "the helpers serve the native protocols to come; only the test protocol uses them yet"
)]

mod antigravity;
mod claude;
mod codex;
mod io;
mod rpc;
#[cfg(test)]
mod stub;
#[cfg(all(test, unix))]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::v1::schema::{
    AgentCapabilities, AuthMethod, AuthMethodAgent, CancelNotification, ContentBlock, ContentChunk, Cost,
    ForkSessionRequest, ForkSessionResponse, Implementation, InitializeRequest, InitializeResponse, LoadSessionRequest,
    LoadSessionResponse, McpCapabilities, McpServer, NewSessionRequest, NewSessionResponse, Plan, PromptCapabilities,
    PromptRequest, PromptResponse, SessionCapabilities, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOption, SessionForkCapabilities, SessionId, SessionMode, SessionModeState, SessionNotification,
    SessionUpdate, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse, SetSessionModeRequest,
    SetSessionModeResponse, TextContent, ToolCall, ToolCallUpdate, UsageUpdate,
};
use agent_client_protocol::v1::{ConnectionTo, Responder};
use agent_client_protocol::{AcpAgent, Agent, Client, DynConnectTo, Error, ErrorCode, JsonRpcResponse};
use anyhow::bail;
use serde::Deserialize;
use serde_json::{Value, json};

pub(crate) use io::{JsonLines, JsonWriter, Process};
pub(crate) use rpc::{Incoming, Rpc, RpcError};

use super::{Harness, ModelSettings};
use crate::sync::MutexExt as _;

/// The protocol a harness definition's CLI speaks on stdio: its `protocol` key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Protocol {
    /// The Agent Client Protocol, which the daemon speaks itself.
    #[default]
    Acp,
    /// Claude Code's stream-json (`claude -p --input-format stream-json --output-format stream-json`).
    Claude,
    /// Codex's app server (`codex app-server`).
    Codex,
    /// Antigravity's headless stream-json (`agy --input-format stream-json --output-format stream-json`).
    Antigravity,
    /// Copilot CLI through the GitHub Copilot SDK.
    CopilotSdk,
    /// A test protocol over JSON-RPC ([`stub`]).
    #[cfg(test)]
    Stub,
}

impl Protocol {
    /// Its name, as the `protocol` key takes it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Acp => "acp",
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Antigravity => "antigravity",
            Self::CopilotSdk => "copilot-sdk",
            #[cfg(test)]
            Self::Stub => "stub",
        }
    }

    /// Whether a translator speaks it to the CLI, not ACP: then the CLI's own permission settings decide which tools
    /// run, and no tool request reaches ants.
    pub fn is_native(self) -> bool {
        self != Self::Acp
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// How a native protocol's definitions are checked and its translator connected.
#[derive(Clone, Copy)]
struct Registration {
    validate: fn(&Harness) -> anyhow::Result<()>,
    connect: fn(Cli) -> DynConnectTo<Client>,
}

impl Registration {
    fn of<N: Native>() -> Self {
        Self { validate: N::validate, connect: connect::<N> }
    }
}

/// The translator of each native protocol, None for ACP: the one place a protocol is registered.
fn registration(protocol: Protocol) -> anyhow::Result<Option<Registration>> {
    match protocol {
        Protocol::Acp => Ok(None),
        #[cfg(test)]
        Protocol::Stub => Ok(Some(Registration::of::<stub::Stub>())),
        Protocol::Claude => Ok(Some(Registration::of::<claude::Claude>())),
        Protocol::Codex => Ok(Some(Registration::of::<codex::Codex>())),
        Protocol::Antigravity => Ok(Some(Registration::of::<antigravity::Antigravity>())),
        Protocol::CopilotSdk => {
            bail!("protocol {protocol} is not implemented yet; omit protocol to run the harness over ACP")
        }
    }
}

/// Check the keys of `harness` its protocol reads, such as the native permission mode `mode` names.
pub(crate) fn validate(harness: &Harness) -> anyhow::Result<()> {
    match registration(harness.protocol)? {
        Some(registration) => (registration.validate)(harness),
        None => Ok(()),
    }
}

/// What the daemon's ACP client connects to for `harness`, with the ant's `env` and `settings`: the harness process
/// itself for ACP, else an in-process ACP agent that starts the CLI and translates its protocol.
pub(crate) fn agent(harness: &Harness, env: &[(String, String)], settings: &ModelSettings) -> DynConnectTo<Client> {
    match registration(harness.protocol) {
        Ok(None) => DynConnectTo::new(AcpAgent::new(harness.process(env, settings))),
        Ok(Some(registration)) => (registration.connect)(Cli::new(harness, env, settings)),
        Err(e) => refusing(format!("harness {harness}: {e}")),
    }
}

/// An agent failing `initialize` with `detail`, for a definition that never passed [`validate`].
fn refusing(detail: String) -> DynConnectTo<Client> {
    DynConnectTo::new(Agent.v1().name("ants-native").on_receive_request(
        async move |_: InitializeRequest, responder, _cx| responder.respond_with_error(failed(&detail)),
        agent_client_protocol::on_receive_request!(),
    ))
}

/// A native CLI as an ant runs it: the definition with the ant's environment and model settings.
#[derive(Debug, Clone)]
pub(crate) struct Cli {
    harness: Arc<Harness>,
    /// The executable: the command, else on Windows the file found on `PATH`.
    program: PathBuf,
    /// The definition's arguments and those of the ant's `context`.
    args: Vec<String>,
    /// The definition's environment with the ant's over it, added to the daemon's.
    env: BTreeMap<String, String>,
    /// The ant's model, with the suffix its `context` selects.
    model: Option<String>,
    effort: Option<String>,
}

impl Cli {
    fn new(harness: &Harness, env: &[(String, String)], settings: &ModelSettings) -> Self {
        Self {
            harness: Arc::new(harness.clone()),
            program: harness.executable(env),
            args: harness.launch_args(settings),
            env: harness.launch_env(env),
            model: settings.model_id(harness),
            effort: settings.reasoning_effort.clone(),
        }
    }

    /// The harness definition, for its phrases, `sign_in`, `window` and the like.
    pub(crate) fn harness(&self) -> &Harness {
        &self.harness
    }

    /// The harness's name.
    pub(crate) fn name(&self) -> &str {
        self.harness.name()
    }

    /// The executable started.
    pub(crate) fn program(&self) -> &std::path::Path {
        &self.program
    }

    /// The definition's arguments and those of the ant's `context`, which [`Self::command`] passes after the
    /// protocol's own.
    pub(crate) fn args(&self) -> &[String] {
        &self.args
    }

    /// The environment [`Self::command`] adds to the daemon's.
    pub(crate) fn env(&self) -> &BTreeMap<String, String> {
        &self.env
    }

    /// The definition's `mode`: for a native protocol, the CLI's own permission mode.
    pub(crate) fn mode(&self) -> Option<&str> {
        self.harness.mode.as_deref()
    }

    /// The model the ant runs, if it sets one, its `context`'s suffix included.
    pub(crate) fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// The reasoning effort the ant runs at, if it sets one.
    pub(crate) fn effort(&self) -> Option<&str> {
        self.effort.as_deref()
    }
}

/// What `initialize` says of a native CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Info {
    /// The CLI's name and version, as `ants harness check` and `ants colony` show them.
    pub name: String,
    pub version: String,
    /// Whether it resumes a session by id ([`Native::load_session`]).
    pub load: bool,
    /// Whether it forks a session ([`Native::fork_session`]).
    pub fork: bool,
    /// Whether prompts may carry images.
    pub images: bool,
    /// Whether it takes HTTP MCP servers, else only stdio ones.
    pub http_mcp: bool,
}

/// Where a session works and the MCP servers it gets ([`mcp_servers_json`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Open {
    pub cwd: PathBuf,
    pub mcp_servers: Vec<McpServer>,
}

/// A setting the daemon selects on a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Setting {
    Model,
    ReasoningEffort,
}

impl Setting {
    fn id(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::ReasoningEffort => "reasoning_effort",
        }
    }
}

/// A native protocol: what runs one ACP connection's CLI. The translator calls it from tasks of its own, so a slow call
/// holds up no other; a session's prompt and its cancel run at once.
pub(crate) trait Native: Sized + Send + Sync + 'static {
    type Session: NativeSession;

    /// Check the keys of a definition the protocol reads, as it loads: which `mode` values (the CLI's permission
    /// modes) it takes, and arguments that would make the CLI wait on a human.
    fn validate(harness: &Harness) -> anyhow::Result<()>;

    /// Start for an ACP connection, on `initialize`: start the CLI and complete its handshake, or keep `cli` for
    /// sessions that each start one. Fails with [`signed_out`] when the CLI says it is not signed in.
    fn start(cli: Cli) -> impl Future<Output = Result<Self, Error>> + Send;

    /// What `initialize` answers of the CLI.
    fn info(&self) -> Info;

    /// Open a new session with the ant's settings ([`Cli::model`], [`Cli::effort`], [`Cli::mode`]).
    fn new_session(&self, open: Open) -> impl Future<Output = Result<Self::Session, Error>> + Send;

    /// Resume session `id`; called only when [`Info::load`].
    fn load_session(&self, id: &str, open: Open) -> impl Future<Output = Result<Self::Session, Error>> + Send {
        let _ = (id, open);
        async { Err(unsupported("session/load")) }
    }

    /// Fork session `id` into a new one; called only when [`Info::fork`].
    fn fork_session(&self, id: &str, open: Open) -> impl Future<Output = Result<Self::Session, Error>> + Send {
        let _ = (id, open);
        async { Err(unsupported("session/fork")) }
    }
}

/// A session of a native CLI.
pub(crate) trait NativeSession: Send + Sync + 'static {
    /// The CLI's id of the session, which ants stores and resumes it by.
    fn id(&self) -> String;

    /// Run a prompt, streaming what it does to `updates`, until the CLI ends the turn: with
    /// [`StopReason::Cancelled`](agent_client_protocol::v1::schema::StopReason) after [`Self::cancel`], and with the
    /// tokens it used. A failure is one of [`signed_out`], [`rate_limited`], [`quota_exhausted`], [`context_overflow`]
    /// or [`failed`].
    fn prompt(
        &self,
        prompt: Vec<ContentBlock>,
        updates: Updates,
    ) -> impl Future<Output = Result<PromptResponse, Error>> + Send;

    /// Interrupt the running prompt: with the CLI's own interrupt, else by stopping it ([`Process::stop`]), the
    /// session resuming later. The daemon stops the connection if the prompt does not end within its grace.
    fn cancel(&self) -> impl Future<Output = ()> + Send;

    /// Select another model or reasoning effort than the session opened with.
    fn configure(&self, setting: Setting, value: &str) -> impl Future<Output = Result<(), Error>> + Send {
        let _ = value;
        async move { Err(unsupported(&format!("changing {} of an open session", setting.id()))) }
    }
}

/// Where a prompt's session updates go: the daemon's client, which streams them as status and reads the reply text,
/// tool calls and cost from them.
#[derive(Clone)]
pub(crate) struct Updates {
    cx: ConnectionTo<Client>,
    session: SessionId,
}

impl Updates {
    pub(crate) fn send(&self, update: SessionUpdate) {
        if let Err(e) = self.cx.send_notification(SessionNotification::new(self.session.clone(), update)) {
            tracing::debug!(error = %e, "session update");
        }
    }

    /// A piece of the reply.
    pub(crate) fn message(&self, text: impl Into<String>) {
        self.send(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(text)))));
    }

    /// A piece of the agent's reasoning.
    pub(crate) fn thought(&self, text: impl Into<String>) {
        self.send(SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(text)))));
    }

    /// A tool call started.
    pub(crate) fn tool_call(&self, call: ToolCall) {
        self.send(SessionUpdate::ToolCall(call));
    }

    /// What changed of a tool call.
    pub(crate) fn tool_update(&self, update: ToolCallUpdate) {
        self.send(SessionUpdate::ToolCallUpdate(update));
    }

    /// The agent's plan, whole.
    pub(crate) fn plan(&self, plan: Plan) {
        self.send(SessionUpdate::Plan(plan));
    }

    /// The context window's use, in tokens, and the session's cost so far in US dollars, which spending caps count.
    pub(crate) fn usage(&self, used: u64, size: u64, cost_usd: Option<f64>) {
        let usage = UsageUpdate::new(used, size).cost(cost_usd.map(|amount| Cost::new(amount, "USD")));
        self.send(SessionUpdate::UsageUpdate(usage));
    }
}

/// The text of a prompt's blocks, joined by blank lines.
pub(crate) fn prompt_text(prompt: &[ContentBlock]) -> String {
    let texts: Vec<&str> = prompt
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect();
    texts.join("\n\n")
}

/// The MCP servers of a session as most CLIs configure them (Claude Code's `--mcp-config` takes them under
/// `mcpServers`): by name, `{"type": "stdio", "command", "args", "env"}` or `{"type": "http", "url", "headers"}`. They
/// carry the ant's token: pass them on stdin or in a file only its user reads, never as arguments, which other users
/// see.
pub(crate) fn mcp_servers_json(servers: &[McpServer]) -> Value {
    let mut config = serde_json::Map::new();
    for server in servers {
        let (name, entry) = match server {
            McpServer::Stdio(stdio) => {
                let env: serde_json::Map<_, _> =
                    stdio.env.iter().map(|var| (var.name.clone(), var.value.clone().into())).collect();
                let entry = json!({ "type": "stdio", "command": stdio.command, "args": stdio.args, "env": env });
                (&stdio.name, entry)
            }
            McpServer::Http(http) => {
                let headers: serde_json::Map<_, _> =
                    http.headers.iter().map(|header| (header.name.clone(), header.value.clone().into())).collect();
                (&http.name, json!({ "type": "http", "url": http.url, "headers": headers }))
            }
            other => {
                tracing::warn!(server = ?other, "skipping an MCP server of a transport native CLIs are not given");
                continue;
            }
        };
        config.insert(name.clone(), entry);
    }
    Value::Object(config)
}

/// The CLI says it is not signed in: ACP's `auth_required`, which makes the daemon hold messages and name the
/// definition's `sign_in`.
pub(crate) fn signed_out(detail: impl fmt::Display) -> Error {
    Error::new(ErrorCode::AUTH_REQUIRED, format!("not signed in: {detail}"))
}

/// The provider rate limited or is overloaded, with the HTTP status it answered and how long it says to wait, if
/// known: a message [`crate::ratelimit::classify`] reads as a rate limit and its retry hint.
pub(crate) fn rate_limited(status: Option<u16>, retry_after: Option<Duration>, detail: impl fmt::Display) -> Error {
    let status = status.map_or_default(|status| format!(" (status {status})"));
    failed(format!("rate limit{status}: {detail}{}", hint(retry_after)))
}

/// The provider's quota or usage allowance is exhausted, until `retry_after` if known: a message
/// [`crate::ratelimit::classify`] reads as an exhausted quota (or, back within 5 minutes, as a rate limit).
pub(crate) fn quota_exhausted(retry_after: Option<Duration>, detail: impl fmt::Display) -> Error {
    failed(format!("quota exceeded: {detail}{}", hint(retry_after)))
}

/// The session outgrew the model's context window: a message [`crate::overflow`] recognizes, so the daemon retries
/// the prompt once in a new session seeded with the thread's history.
pub(crate) fn context_overflow(detail: impl fmt::Display) -> Error {
    failed(format!("context window exceeded: {detail}"))
}

/// Any other failure, `detail` naming it as the CLI did; the daemon still reads the common phrases of sign-in, limits
/// and context overflow in it.
pub(crate) fn failed(detail: impl fmt::Display) -> Error {
    Error::new(ErrorCode::INTERNAL_ERROR, detail.to_string())
}

/// A request the protocol does not support.
pub(crate) fn unsupported(what: &str) -> Error {
    Error::new(ErrorCode::METHOD_NOT_FOUND, format!("{what} is not supported"))
}

fn hint(retry_after: Option<Duration>) -> String {
    retry_after.map_or_default(|wait| format!("; retry after {}s", wait.as_secs().max(1)))
}

/// Log that a native approval request, `request` describing what the CLI asked to do, is denied; the caller answers
/// the CLI with its protocol's denial. ants sends no tool request of a native protocol to `ants.approvals`: the
/// definition's native permission settings decide what runs unasked, and what the CLI would ask a human about is
/// denied at once rather than left waiting.
pub(crate) fn deny_approval(cli: &Cli, request: &str) {
    tracing::warn!(
        harness = cli.name(),
        request,
        "denied a tool request of the harness: its definition's native permission settings decide which tools run"
    );
}

/// An open session, with the model and reasoning effort it runs.
struct Live<S> {
    session: Arc<S>,
    settings: Mutex<BTreeMap<&'static str, String>>,
}

/// The ACP agent side of one connection.
struct Translator<N: Native> {
    cli: Cli,
    native: tokio::sync::OnceCell<N>,
    sessions: Mutex<HashMap<SessionId, Arc<Live<N::Session>>>>,
}

/// The in-process ACP agent translating to protocol `N` for `cli`.
fn connect<N: Native>(cli: Cli) -> DynConnectTo<Client> {
    let name = cli.name().to_owned();
    let t = Arc::new(Translator::<N> { cli, native: tokio::sync::OnceCell::new(), sessions: Mutex::default() });
    let [t1, t2, t3, t4, t5, t6, t7, t8] = [(); 8].map(|()| t.clone());
    DynConnectTo::new(
        Agent
            .v1()
            .name(name)
            .on_receive_request(
                async move |r: InitializeRequest, responder, cx| answer(&cx, responder, t1.clone().initialize(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: NewSessionRequest, responder, cx| answer(&cx, responder, t2.clone().new_session(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: LoadSessionRequest, responder, cx| answer(&cx, responder, t3.clone().load_session(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: ForkSessionRequest, responder, cx| answer(&cx, responder, t4.clone().fork_session(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: PromptRequest, responder, cx| {
                    let work = t5.clone().prompt(r, cx.clone());
                    answer(&cx, responder, work)
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_notification(
                async move |n: CancelNotification, cx| {
                    let live = t6.live(&n.session_id);
                    cx.spawn(async move {
                        match live {
                            Ok(live) => live.session.cancel().await,
                            Err(e) => tracing::debug!(error = %e, "session/cancel"),
                        }
                        Ok(())
                    })
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .on_receive_request(
                async move |r: SetSessionModeRequest, responder, cx| answer(&cx, responder, t7.clone().set_mode(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: SetSessionConfigOptionRequest, responder, cx| {
                    answer(&cx, responder, t8.clone().set_option(r))
                },
                agent_client_protocol::on_receive_request!(),
            ),
    )
}

/// Answer a request with what `work` comes to, off the dispatch loop so that it holds up no other message.
fn answer<R: JsonRpcResponse>(
    cx: &ConnectionTo<Client>,
    responder: Responder<R>,
    work: impl Future<Output = Result<R, Error>> + Send + 'static,
) -> Result<(), Error> {
    cx.spawn(async move { responder.respond_with_result(work.await) })
}

impl<N: Native> Translator<N> {
    async fn initialize(self: Arc<Self>, init: InitializeRequest) -> Result<InitializeResponse, Error> {
        let native = N::start(self.cli.clone()).await?;
        let info = native.info();
        if self.native.set(native).is_err() {
            return Err(Error::new(ErrorCode::INVALID_REQUEST, "initialize was already called"));
        }
        let mut sessions = SessionCapabilities::new();
        if info.fork {
            sessions = sessions.fork(SessionForkCapabilities::new());
        }
        let capabilities = AgentCapabilities::new()
            .load_session(info.load)
            .session_capabilities(sessions)
            .prompt_capabilities(PromptCapabilities::new().image(info.images))
            .mcp_capabilities(McpCapabilities::new().http(info.http_mcp));
        let methods = self.cli.harness.sign_in().map(|command| {
            AuthMethod::Agent(AuthMethodAgent::new("sign_in", "Sign in").description(command.to_owned()))
        });
        Ok(InitializeResponse::new(init.protocol_version)
            .agent_info(Implementation::new(info.name, info.version))
            .agent_capabilities(capabilities)
            .auth_methods(methods.into_iter().collect()))
    }

    fn native(&self) -> Result<&N, Error> {
        self.native.get().ok_or_else(|| Error::new(ErrorCode::INVALID_REQUEST, "initialize first"))
    }

    fn live(&self, id: &SessionId) -> Result<Arc<Live<N::Session>>, Error> {
        let live = self.sessions.locked().get(id).cloned();
        live.ok_or_else(|| Error::new(ErrorCode::RESOURCE_NOT_FOUND, format!("no session {id}")))
    }

    /// Keep `session` open as `id`, with the ant's settings: the session modes and selectors to answer with.
    fn opened(
        &self,
        id: SessionId,
        session: N::Session,
    ) -> (SessionId, Option<SessionModeState>, Vec<SessionConfigOption>) {
        let mut settings = BTreeMap::new();
        for (setting, value) in [(Setting::Model, self.cli.model()), (Setting::ReasoningEffort, self.cli.effort())] {
            if let Some(value) = value {
                settings.insert(setting.id(), value.to_owned());
            }
        }
        let options = options(&settings);
        let live = Live { session: Arc::new(session), settings: Mutex::new(settings) };
        self.sessions.locked().insert(id.clone(), Arc::new(live));
        let modes = self.cli.mode().map(|mode| {
            SessionModeState::new(mode.to_owned(), vec![SessionMode::new(mode.to_owned(), mode.to_owned())])
        });
        (id, modes, options)
    }

    async fn new_session(self: Arc<Self>, request: NewSessionRequest) -> Result<NewSessionResponse, Error> {
        let open = Open { cwd: request.cwd, mcp_servers: request.mcp_servers };
        let session = self.native()?.new_session(open).await?;
        let (id, modes, options) = self.opened(SessionId::new(session.id()), session);
        Ok(NewSessionResponse::new(id).modes(modes).config_options(options))
    }

    async fn load_session(self: Arc<Self>, request: LoadSessionRequest) -> Result<LoadSessionResponse, Error> {
        let open = Open { cwd: request.cwd, mcp_servers: request.mcp_servers };
        let session = self.native()?.load_session(&request.session_id.0, open).await?;
        let (_, modes, options) = self.opened(request.session_id, session);
        Ok(LoadSessionResponse::new().modes(modes).config_options(options))
    }

    async fn fork_session(self: Arc<Self>, request: ForkSessionRequest) -> Result<ForkSessionResponse, Error> {
        let native = self.native()?;
        if !native.info().fork {
            return Err(unsupported("session/fork"));
        }
        let open = Open { cwd: request.cwd, mcp_servers: request.mcp_servers };
        let session = native.fork_session(&request.session_id.0, open).await?;
        let (id, modes, options) = self.opened(SessionId::new(session.id()), session);
        Ok(ForkSessionResponse::new(id).modes(modes).config_options(options))
    }

    async fn prompt(
        self: Arc<Self>,
        request: PromptRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        let live = self.live(&request.session_id)?;
        let updates = Updates { cx, session: request.session_id };
        live.session.prompt(request.prompt, updates).await
    }

    async fn set_mode(self: Arc<Self>, request: SetSessionModeRequest) -> Result<SetSessionModeResponse, Error> {
        self.live(&request.session_id)?;
        match self.cli.mode() {
            Some(mode) if *request.mode_id.0 == *mode => Ok(SetSessionModeResponse::new()),
            _ => Err(failed(format!(
                "harness {}: the session mode is the definition's mode, the CLI's permission mode",
                self.cli.name()
            ))),
        }
    }

    async fn set_option(
        self: Arc<Self>,
        request: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, Error> {
        let live = self.live(&request.session_id)?;
        let setting = match &*request.config_id.0 {
            "model" => Setting::Model,
            "reasoning_effort" => Setting::ReasoningEffort,
            other => return Err(Error::new(ErrorCode::INVALID_PARAMS, format!("no session setting {other}"))),
        };
        let Some(value) = request.value.as_value_id().map(|value| value.0.to_string()) else {
            return Err(Error::new(ErrorCode::INVALID_PARAMS, format!("{} takes a value id", setting.id())));
        };
        let current = live.settings.locked().get(setting.id()).cloned();
        if current.as_deref() != Some(value.as_str()) {
            live.session.configure(setting, &value).await?;
            live.settings.locked().insert(setting.id(), value);
        }
        Ok(SetSessionConfigOptionResponse::new(options(&live.settings.locked())))
    }
}

/// The selectors of a session running `settings`, each offering the value it runs.
fn options(settings: &BTreeMap<&'static str, String>) -> Vec<SessionConfigOption> {
    settings
        .iter()
        .map(|(&id, value)| {
            let (name, category) = match id {
                "model" => ("Model", SessionConfigOptionCategory::Model),
                _ => ("Reasoning effort", SessionConfigOptionCategory::ThoughtLevel),
            };
            let offered = vec![SessionConfigSelectOption::new(value.clone(), value.clone())];
            SessionConfigOption::select(id, name, value.clone(), offered).category(category)
        })
        .collect()
}
