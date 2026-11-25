//! Native harness protocols: a harness definition whose CLI speaks its own protocol rather than ACP (its `protocol` key)
//! runs behind an in-process ACP agent that starts the CLI and translates, so the daemon's ACP client ([`super`]), and
//! with it the pool, fallbacks, sign-in, provider limits, context overflow, caps, status updates, MCP servers and
//! `ants harness check`, drive every harness alike.
//!
//! A protocol implements [`Native`] and [`NativeSession`] in a file of its own under `native/`, and is registered by
//! one arm of [`registration`]. This module answers ACP for it: `initialize` (naming the CLI and offering the
//! definition's sign-in), `session/new`, `session/load` and `session/fork` (advertised as the protocol supports them),
//! `session/prompt`, `session/cancel`, `session/close`, and the session mode and model and reasoning effort selectors the
//! daemon sets.
//! Tool approval is the CLI's own, set by the definition: what the CLI would ask a human about is denied
//! ([`deny_approval`]), unless the ant asks ants about its tools (`tools = "ask"`) and the protocol has a hook for it
//! ([`Native::ASKS`]), when it goes to the client as an ACP permission request ([`Updates::ask`]).
//!
//! A harness speaking ACP itself runs here too ([`acp`]): its process started and released as a native CLI's, the
//! daemon's ACP client speaking to it without a translator.

mod acp;
mod antigravity;
mod claude;
mod cli;
mod codex;
mod copilot;
mod failure;
mod io;
mod leftovers;
mod rpc;
#[cfg(test)]
mod stub;
#[cfg(all(test, unix))]
mod tests;
mod translator;
mod turn;
mod updates;

use std::fmt;
use std::path::PathBuf;

use agent_client_protocol::v1::schema::{ContentBlock, InitializeRequest, McpServer, PromptResponse};
use agent_client_protocol::{Agent, Client, DynConnectTo, Error};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Value, json};

pub(crate) use cli::{Cli, is_flag};
pub(crate) use failure::{context_overflow, failed, quota_exhausted, rate_limited, signed_out, unsupported};
pub(crate) use io::is_loopback;
pub(crate) use io::{JsonLines, JsonWriter, Process};
pub(crate) use leftovers::{runs, sweep_at_start};
#[cfg(test)]
pub(crate) use rpc::RpcError;
pub(crate) use rpc::{Incoming, Rpc};
pub(crate) use updates::{Updates, approve, deny_approval};

use super::launch::Reports;
use super::mcp_sign_in::Report;
use super::own_turn::OwnTurnReport;
use super::{Harness, ModelSettings};

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
    /// run, and only what it would ask a human about reaches ants, when the ant asks ([`asks`]).
    pub(crate) fn is_native(self) -> bool {
        self != Self::Acp
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// How a native protocol's definitions are checked, its translator connected and, where it reports them, its MCP
/// servers read.
#[derive(Clone, Copy)]
struct Registration {
    validate: fn(&Harness) -> anyhow::Result<()>,
    connect: fn(Cli) -> DynConnectTo<Client>,
    mcp: Option<OpenHost>,
    asks: bool,
    cost_with: Option<&'static str>,
}

impl Registration {
    fn of<N: Native>() -> Self {
        Self {
            validate: N::validate,
            connect: translator::connect::<N>,
            mcp: N::MCP.then_some(open_host::<N> as OpenHost),
            asks: N::ASKS,
            cost_with: N::COST_WITH,
        }
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
        Protocol::CopilotSdk => Ok(Some(Registration::of::<copilot::Copilot>())),
    }
}

/// Whether a session of a CLI speaking `protocol` reports its MCP servers and signs them in ([`mcp_host`]).
pub(crate) fn reports_mcp(protocol: Protocol) -> bool {
    registration(protocol).ok().flatten().is_some_and(|registration| registration.mcp.is_some())
}

/// Whether a harness speaking `protocol` can send what it would ask a human about its tools to ants: ACP, or a native
/// protocol with the hook ([`Native::ASKS`]).
pub(crate) fn asks(protocol: Protocol) -> bool {
    registration(protocol).ok().is_some_and(|registration| registration.is_none_or(|registration| registration.asks))
}

/// The sign-in a CLI speaking `protocol` reports a cost with, when it reports one only with that sign-in
/// ([`Native::COST_WITH`]).
pub(crate) fn cost_with(protocol: Protocol) -> Option<&'static str> {
    registration(protocol).ok().flatten().and_then(|registration| registration.cost_with)
}

/// Check the keys of `harness` its protocol reads, such as the native permission mode `mode` names.
pub(crate) fn validate(harness: &Harness) -> anyhow::Result<()> {
    match registration(harness.protocol)? {
        Some(registration) => (registration.validate)(harness),
        None => Ok(()),
    }
}

/// What the daemon's ACP client connects to for `harness`, with the ant's `env` and `settings`: the harness process
/// itself for ACP ([`acp`], started and released as a native CLI is), else an in-process ACP agent that starts the CLI
/// and translates its protocol.
/// The sessions of a native CLI report their sign-ins to MCP servers and the turns of their own to `reports`.
pub(crate) fn agent(
    harness: &Harness,
    env: &[(String, String)],
    settings: &ModelSettings,
    reports: Reports,
) -> DynConnectTo<Client> {
    match registration(harness.protocol) {
        Ok(None) => acp::connect(Cli::new(harness, env, settings)),
        Ok(Some(registration)) => (registration.connect)(Cli::new(harness, env, settings).reporting(reports)),
        Err(e) => refusing(format!("harness {harness}: {e}")),
    }
}

/// A session of `harness` with the ant's `env` and `settings`, working in `cwd` with the MCP servers `servers` gives
/// (told whether the CLI takes HTTP ones), that reports its MCP servers and signs them in, bypassing ACP; None when the
/// harness's protocol does not report them.
pub(crate) fn mcp_host(
    harness: &Harness,
    env: &[(String, String)],
    settings: &ModelSettings,
    cwd: PathBuf,
    servers: ServersFor,
) -> Option<BoxFuture<'static, Result<Box<dyn McpHost>, Error>>> {
    let open = registration(harness.protocol).ok()??.mcp?;
    Some(open(Cli::new(harness, env, settings), cwd, servers))
}

/// The MCP servers of a session, given whether the CLI takes HTTP ones.
pub(crate) type ServersFor = Box<dyn FnOnce(bool) -> Vec<McpServer> + Send>;

type OpenHost = fn(Cli, PathBuf, ServersFor) -> BoxFuture<'static, Result<Box<dyn McpHost>, Error>>;

fn open_host<N: Native>(
    cli: Cli,
    cwd: PathBuf,
    servers: ServersFor,
) -> BoxFuture<'static, Result<Box<dyn McpHost>, Error>> {
    Box::pin(async move {
        let native = N::start(cli).await?;
        let mcp_servers = servers(native.info().http_mcp);
        let session =
            native.new_session(Open { cwd, mcp_servers, sign_ins: None, own_turns: None, skills: None }).await?;
        Ok(Box::new(Host { _native: native, session }) as Box<dyn McpHost>)
    })
}

/// A session of a native CLI held open for its MCP servers ([`mcp_host`]): the CLI and its MCP servers stop once it is
/// closed and dropped.
pub(crate) trait McpHost: Send + Sync {
    fn servers(&self) -> BoxFuture<'_, Result<Vec<ants_proto::McpServerState>, Error>>;
    fn sign_in<'a>(&'a self, server: &'a str) -> BoxFuture<'a, Result<Option<String>, Error>>;
    fn callback<'a>(&'a self, server: &'a str, callback: &'a str) -> BoxFuture<'a, Result<(), Error>>;
    fn close(&self) -> BoxFuture<'_, ()>;
}

struct Host<N: Native> {
    /// Keeps the CLI running, for CLIs shared by their sessions.
    _native: N,
    session: N::Session,
}

impl<N: Native> McpHost for Host<N> {
    fn servers(&self) -> BoxFuture<'_, Result<Vec<ants_proto::McpServerState>, Error>> {
        Box::pin(self.session.mcp_servers())
    }

    fn sign_in<'a>(&'a self, server: &'a str) -> BoxFuture<'a, Result<Option<String>, Error>> {
        Box::pin(self.session.mcp_sign_in(server))
    }

    fn callback<'a>(&'a self, server: &'a str, callback: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(self.session.mcp_callback(server, callback))
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.session.discard())
    }
}

/// An agent failing `initialize` with `detail`, for a definition that never passed [`validate`].
fn refusing(detail: String) -> DynConnectTo<Client> {
    DynConnectTo::new(Agent.v1().name("ants-native").on_receive_request(
        async move |_: InitializeRequest, responder, _cx| responder.respond_with_error(failed(&detail)),
        agent_client_protocol::on_receive_request!(),
    ))
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
    /// Whether a running prompt takes more input ([`NativeSession::steer`]).
    pub steer: bool,
}

/// Where a session works and the MCP servers it gets ([`mcp_servers_json`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Open {
    pub cwd: PathBuf,
    pub mcp_servers: Vec<McpServer>,
    /// Where it reports its sign-ins to the definition's `mcp_sign_in` servers, signing in to them only then: a session
    /// serving a thread, not one opened to read them ([`mcp_host`]), to probe the harness or to fork (the session/load
    /// following it signs in).
    pub sign_ins: Option<Report>,
    /// Where it reports the turns it starts of its own between prompts: a session serving a thread, as for `sign_ins`.
    pub own_turns: Option<OwnTurnReport>,
    /// The ant's confirmed skills to offer it, if any ([`super::skills`]).
    pub skills: Option<super::Skills>,
}

/// A setting the daemon selects on a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Setting {
    Model,
    ReasoningEffort,
}

impl Setting {
    pub(super) fn id(self) -> &'static str {
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

    /// Resume the session of ACP id `id` ([`NativeSession::id`]); called only when [`Info::load`].
    fn load_session(&self, id: &str, open: Open) -> impl Future<Output = Result<Self::Session, Error>> + Send {
        let _ = (id, open);
        async { Err(unsupported("session/load")) }
    }

    /// Fork the session of ACP id `id` into a new one; called only when [`Info::fork`].
    fn fork_session(&self, id: &str, open: Open) -> impl Future<Output = Result<Self::Session, Error>> + Send {
        let _ = (id, open);
        async { Err(unsupported("session/fork")) }
    }

    /// The credentials the CLI signs in with as they are now, where the protocol can read them. Once a prompt fails
    /// with [`signed_out`], no session opens with the same credentials, so a handshake does not find signed in a CLI
    /// whose provider refuses them only at a prompt.
    fn credentials(&self) -> Option<Credentials> {
        None
    }

    /// Whether its sessions report their MCP servers and sign them in ([`NativeSession::mcp_servers`]).
    const MCP: bool = false;

    /// Whether it can send what the CLI would ask a human about its tools to the client ([`Cli::asks`]), so an ant
    /// may set `tools = "ask"` on it.
    const ASKS: bool = false;

    /// The sign-in its CLI reports a prompt's cost with, when it reports one with that sign-in only, such as
    /// `"ChatGPT"`: a cap of cost on a definition reporting no cost is then warned of as tripping only with it.
    const COST_WITH: Option<&'static str> = None;
}

/// The credentials a native CLI signs in with ([`Native::credentials`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Credentials {
    /// Where the CLI keeps them.
    pub(super) place: String,
    /// A hash of them, which tells whether they changed.
    pub(super) fingerprint: u64,
}

impl Credentials {
    /// The credentials kept at `place`, made of `parts`.
    pub(crate) fn new(place: impl Into<String>, parts: &[&[u8]]) -> Self {
        use std::hash::{DefaultHasher, Hash as _, Hasher as _};
        let mut hasher = DefaultHasher::new();
        parts.hash(&mut hasher);
        Self { place: place.into(), fingerprint: hasher.finish() }
    }
}

/// A session of a native CLI.
pub(crate) trait NativeSession: Send + Sync + 'static {
    /// The session's ACP id, which ants stores and resumes it by: the CLI's id of it, with the cost a fork starts at
    /// ([`based_id`]).
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

    /// Run the prompt taking the turn of its own `id` the session reported ([`Open::own_turns`]): the turn's updates
    /// streamed to `updates` and its end answering the prompt, without `prompt`, which is for a session that does not
    /// know the turn, by default every session, prompted with it instead.
    fn take_own_turn(
        &self,
        id: &str,
        prompt: Vec<ContentBlock>,
        updates: Updates,
    ) -> impl Future<Output = Result<PromptResponse, Error>> + Send {
        let _ = id;
        self.prompt(prompt, updates)
    }

    /// Add `prompt` to the running prompt as the user's next input to its turn, whose answer then answers it too;
    /// called only when [`Info::steer`]. `false` when the CLI cannot take it now, as with no turn running or one
    /// ending, the daemon then prompting it after the turn.
    fn steer(&self, prompt: Vec<ContentBlock>) -> impl Future<Output = Result<bool, Error>> + Send {
        let _ = prompt;
        async { Ok(false) }
    }

    /// Interrupt the running prompt: with the CLI's own interrupt, else by stopping it ([`Process::stop`]), the
    /// session resuming later. The daemon stops the connection if the prompt does not end within its grace.
    fn cancel(&self) -> impl Future<Output = ()> + Send;

    /// Release what the CLI holds for the session, which the daemon abandoned (`session/close`), such as the MCP servers
    /// it started for it; the session is dropped next, which ends what the translator runs for it alone.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Close a session opened only to read its MCP servers, leaving nothing of it to resume.
    fn discard(&self) -> impl Future<Output = ()> + Send {
        self.close()
    }

    /// The session's MCP servers and how each stands; called only when [`Native::MCP`].
    fn mcp_servers(&self) -> impl Future<Output = Result<Vec<ants_proto::McpServerState>, Error>> + Send {
        async { Err(unsupported("reading MCP servers")) }
    }

    /// Start signing the session in to MCP server `server` with OAuth: the address to open in a browser, which comes
    /// back to the CLI's own loopback listener; none when the server needs no sign-in, such as with credentials the
    /// CLI keeps. Called only when [`Native::MCP`].
    fn mcp_sign_in(&self, server: &str) -> impl Future<Output = Result<Option<String>, Error>> + Send {
        let _ = server;
        async { Err(unsupported("signing in to MCP servers")) }
    }

    /// Hand the CLI `callback`, the address a browser came back to after a sign-in [`Self::mcp_sign_in`] started,
    /// checked to be on the loopback listener of the sign-in: by default by requesting it, as the browser would have.
    fn mcp_callback(&self, server: &str, callback: &str) -> impl Future<Output = Result<(), Error>> + Send {
        let _ = server;
        let callback = callback.to_owned();
        async move { io::get_loopback(&callback).await.map_err(|e| failed(format!("{e:#}"))) }
    }

    /// Select another model or reasoning effort than the session opened with.
    fn configure(&self, setting: Setting, value: &str) -> impl Future<Output = Result<(), Error>> + Send {
        let _ = value;
        async move { Err(unsupported(&format!("changing {} of an open session", setting.id()))) }
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

/// An MCP server a CLI reports as the JSON object `server`, under its `name`; the protocol picks the other fields.
pub(crate) fn mcp_server_state(
    server: &Value,
    status: ants_proto::McpStatus,
    error: Option<&str>,
    source: Option<&str>,
    tools: Option<usize>,
) -> ants_proto::McpServerState {
    ants_proto::McpServerState::new(server["name"].as_str().unwrap_or_default(), status, error, source)
        .with_tools(tools)
}

/// How an MCP server stands, in the words Claude Code and the Copilot CLI share: `connected`, `pending`,
/// `needs-auth`, `failed` and `disabled`.
pub(crate) fn mcp_status(word: Option<&str>) -> ants_proto::McpStatus {
    use ants_proto::McpStatus;
    match word {
        Some("connected") => McpStatus::Connected,
        Some("pending") => McpStatus::Pending,
        Some("needs-auth") => McpStatus::NeedsAuth,
        Some("failed") => McpStatus::Failed,
        Some("disabled") => McpStatus::Disabled,
        _ => McpStatus::Unknown,
    }
}

/// The ACP id of a session the CLI knows as `id` whose cost totals start at `base` US dollars, carried over from the
/// session it was forked from: `<id>~<base in micro-dollars>`. The daemon stores the ACP id in `ants_sessions`, so a
/// fork resumed after a restart still reports only its own cost ([`cost_base`]).
pub(crate) fn based_id(id: &str, base: f64) -> String {
    let micros = (base * 1e6).round();
    if micros >= 1.0 { format!("{id}~{}", micros as u64) } else { id.to_owned() }
}

/// The CLI's id of the session ACP id `id` names, and the cost its totals start at ([`based_id`]).
pub(crate) fn cost_base(id: &str) -> (&str, f64) {
    match id.rsplit_once('~').map(|(cli, micros)| (cli, micros.parse::<u64>())) {
        Some((cli, Ok(micros))) if !cli.is_empty() => (cli, micros as f64 / 1e6),
        _ => (id, 0.0),
    }
}
