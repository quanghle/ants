//! Copilot CLI through the GitHub Copilot SDK (`protocol = "copilot-sdk"`): `copilot --server --stdio`, one CLI per ACP
//! connection, driven by the SDK's client over the CLI's stdio.
//!
//! The SDK is linked without its features, so it neither downloads nor starts a CLI: the translator starts the
//! definition's command itself and hands the SDK its stdio. Sessions are the CLI's own, created with an id chosen here,
//! resumed and forked by id; a fork's totals start at its parent's, so its ACP id carries that base ([`based_id`]). Each
//! carries the ant's model and reasoning effort, the ants MCP server and a permission
//! handler approving whatever the CLI asks about the tools the definition leaves it, so nothing waits on a human, or,
//! when the ant asks ants about its tools, sending it to ants ([`super::approve`]).
//! Events are read by their type name and JSON data rather than as the SDK's typed enum, which lacks the types of
//! newer CLIs.

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{
    ContentBlock, PromptResponse, StopReason, ToolCall, ToolCallLocation, ToolCallStatus, ToolCallUpdate,
    ToolCallUpdateFields, ToolKind,
};
use ants_proto::{McpServerState, McpStatus};
use anyhow::bail;
use github_copilot_sdk as sdk;
use sdk::handler::PermissionResult;
use sdk::session::Session;
use sdk::subscription::{EventSubscription, RecvErrorKind};
use sdk::types::{PermissionRequestKind, RequestId};
use sdk::{
    Attachment, DeliveryMode, ErrorKind, MessageOptions, PermissionRequestData, ProtocolErrorKind, ResumeSessionConfig,
    SessionConfig, SessionEvent, SessionId, SetModelOptions,
};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{mpsc, watch};

use super::turn::{Reply, Spent, Tokens};
use super::{
    Cli, Harness, Info, Native, NativeSession, Open, Process, Setting, Updates, approve, based_id, context_overflow,
    cost_base, deny_approval, failed, mcp_servers_json, prompt_text, quota_exhausted, rate_limited, signed_out,
    unsupported,
};
use crate::harness::mcp_sign_in::Report;
use crate::sync::MutexExt as _;

mod mcp_sign_in;

/// The release of github-copilot-sdk linked, as pinned in ants-daemon's Cargo.toml: the CLI must speak its protocol.
const SDK_VERSION: &str = "1.0.29";

/// The CLI's own arguments, before the definition's: its JSON-RPC server on stdio, neither updating itself nor sending
/// its client telemetry.
const SERVER_ARGS: [&str; 4] = ["--server", "--stdio", "--no-auto-update", "--disable-client-telemetry"];

/// Arguments making the CLI run something other than its server.
const FOREIGN_ARGS: [&str; 9] =
    ["--acp", "-p", "--prompt", "-i", "--interactive", "--resume", "-r", "--continue", "--connect"];

/// Permission rules the CLI's server ignores, its sessions taking their tools from the SDK.
const IGNORED_ARGS: [&str; 2] = ["--allow-tool", "--deny-tool"];

/// US dollars of a nano AI credit, the unit the CLI counts a session's cost in: an AI credit is US$0.01.
const USD_PER_NANO_AIU: f64 = 1e-11;

/// What the daemon's `initialize` names the CLI, as `copilot --version` does.
const NAME: &str = "GitHub Copilot CLI";

pub(crate) struct Copilot {
    shared: Arc<Shared>,
    version: String,
}

/// What the definition's arguments set of each session rather than of the CLI, whose server ignores them on its
/// command line: the tools left to the model (`--available-tools`, `--excluded-tools`, each tool named, or several
/// separated by commas), the context tier (`--context`) and MCP servers added to the session's own
/// (`--additional-mcp-config`, as JSON or `@` a file of it, each naming them in `mcpServers`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SessionArgs {
    available: Option<Vec<String>>,
    excluded: Option<Vec<String>>,
    context: Option<String>,
    mcp: Vec<String>,
}

impl SessionArgs {
    /// Take them out of `cli_args`, leaving the CLI's own; an error names an argument it cannot take.
    fn take(cli_args: &mut Vec<String>) -> Result<Self, String> {
        let mut taken = Self::default();
        let mut rest = Vec::new();
        let mut args = std::mem::take(cli_args).into_iter().peekable();
        while let Some(arg) = args.next() {
            let (flag, value) = match arg.split_once('=') {
                Some((flag, value)) if flag.starts_with("--") => (flag.to_owned(), Some(value.to_owned())),
                _ => (arg.clone(), None),
            };
            let tools = match flag.as_str() {
                "--available-tools" => &mut taken.available,
                "--excluded-tools" => &mut taken.excluded,
                "--context" => {
                    let tier = value.or_else(|| args.next_if(|next| !next.starts_with('-')));
                    taken.context = Some(tier.ok_or("--context takes a tier")?);
                    continue;
                }
                "--additional-mcp-config" => {
                    let config = value.or_else(|| args.next()).ok_or("--additional-mcp-config takes JSON or @FILE")?;
                    if !config.starts_with('@') {
                        mcp_config(&config)?;
                    }
                    taken.mcp.push(config);
                    continue;
                }
                flag if IGNORED_ARGS.contains(&flag) => {
                    return Err(format!(
                        "the CLI's server ignores {flag}: choose the tools with --available-tools or --excluded-tools"
                    ));
                }
                _ => {
                    rest.push(arg);
                    continue;
                }
            };
            let mut names = value.into_iter().collect::<Vec<_>>();
            if names.is_empty() {
                while let Some(next) = args.next_if(|next| !next.starts_with('-')) {
                    names.push(next);
                }
            }
            let names = names.iter().flat_map(|names| names.split(',')).map(str::trim).filter(|n| !n.is_empty());
            let tools = tools.get_or_insert_default();
            for name in names {
                if name == "*" {
                    return Err(format!("{flag} takes builtin:*, mcp:* or custom:* rather than *"));
                }
                tools.push(name.to_owned());
            }
        }
        *cli_args = rest;
        Ok(taken)
    }

    /// The servers `--additional-mcp-config` adds, those of a later one over an earlier one's, reading its files.
    fn mcp_servers(&self) -> Result<Map<String, Value>, String> {
        let mut servers = Map::new();
        for config in &self.mcp {
            let read = match config.strip_prefix('@') {
                Some(path) => std::fs::read_to_string(path)
                    .map_err(|e| format!("--additional-mcp-config: reading {path}: {e}"))
                    .and_then(|text| mcp_config(&text).map_err(|e| format!("{e} in {path}")))?,
                None => mcp_config(config)?,
            };
            servers.extend(read);
        }
        Ok(servers)
    }
}

/// The servers of an MCP configuration, as `--additional-mcp-config` takes it: `{"mcpServers": {NAME: SERVER}}`, a
/// server without a `type` being `stdio` with a `command`, else `http`.
fn mcp_config(text: &str) -> Result<Map<String, Value>, String> {
    let config: Value = serde_json::from_str(text).map_err(|e| format!("--additional-mcp-config: {e}"))?;
    let Some(Value::Object(servers)) = config.get("mcpServers") else {
        return Err("--additional-mcp-config takes an object of servers in mcpServers".to_owned());
    };
    let mut servers = servers.clone();
    for (name, server) in &mut servers {
        let Value::Object(server) = server else {
            return Err(format!("--additional-mcp-config: server {name} is no object"));
        };
        if !server.contains_key("type") {
            let kind = if server.contains_key("command") { "stdio" } else { "http" };
            server.insert("type".to_owned(), kind.into());
        }
    }
    Ok(servers)
}

/// One CLI and the SDK's client of it, shared by its sessions.
struct Shared {
    cli: Cli,
    args: SessionArgs,
    /// The servers `--additional-mcp-config` adds to every session.
    mcp: Map<String, Value>,
    client: sdk::Client,
    process: tokio::sync::Mutex<Process>,
    /// True once the CLI's output ended.
    closed: watch::Receiver<bool>,
}

impl Shared {
    /// The error answering a failed SDK call: how the CLI exited once its output ended, else the SDK's error.
    async fn error(&self, e: &sdk::Error) -> Error {
        if e.is_transport_failure() || *self.closed.borrow() {
            self.process.lock().await.exited().await
        } else {
            failed(e)
        }
    }

    /// Wait for the CLI's output to end.
    async fn ended(&self) {
        let mut closed = self.closed.clone();
        // Fails only once the reader is gone, which ends the output too.
        let _ = closed.wait_for(|closed| *closed).await;
    }

    /// Check the CLI speaks the SDK's protocol and is signed in; its version.
    async fn handshake(&self) -> Result<String, Error> {
        if let Err(e) = self.client.verify_protocol_version().await {
            if let ErrorKind::Protocol(ProtocolErrorKind::VersionMismatch { server, min, max, .. }) = e.kind() {
                let cli = self.client.get_status().await.map_or_else(|_| "copilot".to_owned(), |s| s.version);
                return Err(failed(format!(
                    "Copilot CLI {cli} speaks SDK protocol {server}, github-copilot-sdk {SDK_VERSION} linked into ants \
                     speaks {min} to {max}: run a Copilot CLI speaking protocol {max}"
                )));
            }
            return Err(self.error(&e).await);
        }
        let status = match self.client.get_status().await {
            Ok(status) => status,
            Err(e) => return Err(self.error(&e).await),
        };
        let auth = match self.client.get_auth_status().await {
            Ok(auth) => auth,
            Err(e) => return Err(self.error(&e).await),
        };
        if !auth.is_authenticated {
            let detail = auth.status_message.unwrap_or_else(|| "Not authenticated".to_owned());
            return Err(signed_out(format!("Copilot CLI {}: {detail}", status.version)));
        }
        Ok(status.version)
    }

    /// The handler answering the CLI's permission requests of a session whose prompt running sends them to `asking`
    /// ([`Permissions`]).
    fn permissions(&self, asking: Asking) -> Arc<dyn sdk::handler::PermissionHandler> {
        Arc::new(Permissions { cli: self.cli.clone(), asking })
    }
}

/// Where the permission requests of a session go: the prompt running, if any.
type Asking = Arc<Mutex<Option<Updates>>>;

/// The handler answering a session's permission requests: one that managed policy says a human must decide is
/// denied; else every one is approved, as the tools left to the model are the ones the definition enables, unless the
/// ant asks ants about its tools, when ants decide ([`approve`]).
struct Permissions {
    cli: Cli,
    asking: Asking,
}

impl sdk::handler::PermissionHandler for Permissions {
    fn handle<'a, 'b>(
        &'a self,
        _session: SessionId,
        _request: RequestId,
        data: PermissionRequestData,
    ) -> Pin<Box<dyn Future<Output = PermissionResult> + Send + 'b>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            let described = data.extra.get("permissionRequest").unwrap_or(&data.extra).to_string();
            if data.managed_approval_required == Some(true) {
                deny_approval(&self.cli, &described);
                return PermissionResult::reject(None);
            }
            if !self.cli.asks() {
                tracing::debug!(
                    harness = self.cli.name(),
                    request = described,
                    "approved a tool request of the harness"
                );
                return PermissionResult::approve_once();
            }
            let updates = self.asking.locked().clone();
            if approve(&self.cli, updates, asked_call(&data), &described).await {
                PermissionResult::approve_once()
            } else {
                PermissionResult::reject(Some("The ant's tool permissions did not allow it.".to_owned()))
            }
        })
    }
}

/// The tool call a permission request of the CLI asks to run, by the id of the one it reported.
fn asked_call(data: &PermissionRequestData) -> ToolCallUpdate {
    let request = data.extra.get("permissionRequest").unwrap_or(&data.extra);
    let text = |key: &str| request[key].as_str().unwrap_or_default().to_owned();
    let (kind, title, path) = match data.kind {
        Some(PermissionRequestKind::Shell) => (ToolKind::Execute, text("fullCommandText"), None),
        Some(PermissionRequestKind::Write) => {
            (ToolKind::Edit, format!("Write {}", text("fileName")), Some(text("fileName")))
        }
        Some(PermissionRequestKind::Read) => (ToolKind::Read, format!("Read {}", text("path")), Some(text("path"))),
        Some(PermissionRequestKind::Url) => (ToolKind::Fetch, format!("Fetch {}", text("url")), None),
        Some(PermissionRequestKind::Mcp) => {
            (ToolKind::Other, format!("{}/{}", text("serverName"), text("toolName")), None)
        }
        _ => (ToolKind::Other, text("intention"), None),
    };
    let id = data.tool_call_id.clone().or_else(|| request["toolCallId"].as_str().map(str::to_owned));
    let fields = ToolCallUpdateFields::new()
        .kind(kind)
        .title(title)
        .raw_input(request.clone())
        .locations(path.filter(|path| !path.is_empty()).map(|path| vec![ToolCallLocation::new(path)]));
    ToolCallUpdate::new(id.unwrap_or_else(|| "permission".to_owned()), fields)
}

/// The settings every session opens with, whether new, resumed or forked: streamed replies, the session's working
/// directory, MCP servers and skills, the ant's model and reasoning effort, no session telemetry, and the permission
/// handler.
macro_rules! settings {
    ($config:expr, $shared:expr, $open:expr, $asking:expr) => {{
        let shared: &Shared = $shared;
        let open: Open = $open;
        let mut config = $config
            .with_streaming(true)
            .with_working_directory(open.cwd)
            .with_enable_session_telemetry(false)
            .with_permission_handler(shared.permissions($asking));
        if let Some(model) = shared.cli.model() {
            config = config.with_model(model);
        }
        if let Some(effort) = shared.cli.effort() {
            config = config.with_reasoning_effort(effort);
        }
        if let Some(tier) = &shared.args.context {
            config = config.with_context_tier(tier.clone());
        }
        if let Some(tools) = &shared.args.available {
            config = config.with_available_tools(tools.clone());
        }
        if let Some(tools) = &shared.args.excluded {
            config = config.with_excluded_tools(tools.clone());
        }
        if let Some(skills) = open.skills {
            config = config.with_skill_directories([skills.dir]);
        }
        let mut servers = shared.mcp.clone();
        if let Value::Object(own) = mcp_servers_json(&open.mcp_servers) {
            servers.extend(own);
        }
        if !servers.is_empty() {
            let servers = serde_json::from_value(Value::Object(servers))
                .map_err(|e| failed(format!("the session's MCP servers: {e}")))?;
            config = config.with_mcp_servers(servers);
        }
        config
    }};
}

impl Native for Copilot {
    type Session = CopilotSession;

    fn validate(harness: &Harness) -> anyhow::Result<()> {
        if let Some(mode) = &harness.mode {
            bail!(
                "protocol copilot-sdk takes no mode, not {mode}: the definition's args choose Copilot's tools \
                 (--available-tools, --excluded-tools)"
            );
        }
        let foreign = |arg: &&String| {
            FOREIGN_ARGS.iter().any(|f| arg == f || arg.strip_prefix(f).is_some_and(|rest| rest.starts_with('=')))
        };
        if let Some(arg) = harness.args.iter().find(foreign) {
            bail!("protocol copilot-sdk runs the CLI's server, which {arg} in args would replace");
        }
        SessionArgs::take(&mut harness.args.clone()).map_err(anyhow::Error::msg)?;
        for (name, value) in &harness.context {
            SessionArgs::take(&mut value.args.clone()).map_err(|e| anyhow::anyhow!("context {name}: {e}"))?;
        }
        Ok(())
    }

    async fn start(mut cli: Cli) -> Result<Self, Error> {
        let args = SessionArgs::take(&mut cli.args).map_err(failed)?;
        let mcp = args.mcp_servers().map_err(failed)?;
        let mut process = cli.spawn(SERVER_ARGS, None)?;
        let (stdin, stdout) = process.take_io().ok_or_else(|| failed("no stdio"))?;
        let (closing, closed) = watch::channel(false);
        let cwd = std::env::current_dir().unwrap_or_default();
        let entra =
            cli.entra_client_id().map(|id| sdk::EntraAuthConfig { client_id: id.to_owned(), mats_app_name: None });
        let client = sdk::Client::from_streams_with_entra(Output { inner: stdout, closing }, stdin, cwd, entra)
            .map_err(|e| failed(format!("{}: {e}", cli.name())))?;
        let shared = Arc::new(Shared { cli, args, mcp, client, process: tokio::sync::Mutex::new(process), closed });
        let version = shared.handshake().await?;
        Ok(Self { shared, version })
    }

    fn info(&self) -> Info {
        Info {
            name: NAME.to_owned(),
            version: self.version.clone(),
            load: true,
            fork: true,
            images: true,
            http_mcp: true,
            steer: true,
        }
    }

    const MCP: bool = true;

    const ASKS: bool = true;

    async fn new_session(&self, open: Open) -> Result<CopilotSession, Error> {
        let shared = &self.shared;
        let id = uuid::Uuid::new_v4().to_string();
        let sign_ins = open.sign_ins.clone();
        let asking = Asking::default();
        let config = settings!(SessionConfig::default().with_session_id(id), shared, open, asking.clone());
        let prepared = shared.client.prepare_session(config).map_err(failed)?;
        let events = prepared.subscribe();
        match prepared.start().await {
            Ok(session) => Ok(CopilotSession::open(shared.clone(), session, events, 0.0, sign_ins, asking).await),
            Err(e) => Err(shared.error(&e).await),
        }
    }

    async fn load_session(&self, id: &str, open: Open) -> Result<CopilotSession, Error> {
        let (id, base) = cost_base(id);
        self.resume(id, open, base).await
    }

    async fn fork_session(&self, id: &str, open: Open) -> Result<CopilotSession, Error> {
        let parent = SessionId::from(cost_base(id).0);
        let request = sdk::rpc::SessionsForkRequest { session_id: parent, ..Default::default() };
        let forked = match self.shared.client.rpc().sessions().fork(request).await {
            Ok(forked) => forked.session_id,
            Err(e) => return Err(self.shared.error(&e).await),
        };
        // The fork's accounting starts at its parent's, which the parent's thread already counted.
        let metrics = json!({ "sessionId": forked });
        let base = match self.shared.client.call("session.usage.getMetrics", Some(metrics)).await {
            Ok(metrics) => metrics["totalNanoAiu"].as_f64().unwrap_or_default() * USD_PER_NANO_AIU,
            Err(e) => return Err(self.shared.error(&e).await),
        };
        self.resume(&forked, open, base).await
    }
}

impl Copilot {
    /// Resume the CLI's session `id`, whose totals start at `base` US dollars.
    async fn resume(&self, id: &str, open: Open, base: f64) -> Result<CopilotSession, Error> {
        let shared = &self.shared;
        let sign_ins = open.sign_ins.clone();
        let asking = Asking::default();
        let config = settings!(ResumeSessionConfig::new(SessionId::from(id)), shared, open, asking.clone());
        let prepared = shared.client.prepare_resume_session(config).map_err(failed)?;
        let events = prepared.subscribe();
        match prepared.start().await {
            Ok(session) => Ok(CopilotSession::open(shared.clone(), session, events, base, sign_ins, asking).await),
            Err(e) => Err(shared.error(&e).await),
        }
    }
}

/// The CLI's stdout, saying when it ends.
struct Output<R> {
    inner: R,
    closing: watch::Sender<bool>,
}

impl<R: AsyncRead + Unpin> AsyncRead for Output<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let (before, room) = (buf.filled().len(), buf.remaining() > 0);
        let read = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(result) = &read
            && (result.is_err() || (room && buf.filled().len() == before))
        {
            self.closing.send_replace(true);
        }
        read
    }
}

/// An MCP server as the CLI's `session.mcp.list` reports it.
fn mcp_server(server: &Value) -> McpServerState {
    let status = match server["status"].as_str() {
        Some("stopped" | "not_configured") => McpStatus::Disabled,
        word => super::mcp_status(word),
    };
    super::mcp_server_state(server, status, server["error"].as_str(), server["source"].as_str(), None)
}

/// Where a session's events go: the prompt running, if any.
type Current = Arc<Mutex<Option<mpsc::UnboundedSender<SessionEvent>>>>;

pub(crate) struct CopilotSession {
    shared: Arc<Shared>,
    session: Session,
    current: Current,
    asking: Asking,
    router: tokio::task::JoinHandle<()>,
    /// The model and reasoning effort it runs, if set.
    model: Mutex<Option<String>>,
    effort: Mutex<Option<String>>,
    /// The context window's use and the session's cost, as last reported.
    spent: Mutex<Spent>,
    steers: Steers,
    /// The session, for sign-ins to MCP servers.
    opened: Arc<mcp_sign_in::Opened>,
    /// Signs it in to the definition's `mcp_sign_in` servers as they need it, if it serves a thread.
    watcher: Option<tokio::task::JoinHandle<()>>,
}

/// The messages steering the prompt running ([`NativeSession::steer`]), sent with immediate delivery. The prompt ends
/// at a `session.idle` only once the CLI took each into its loop, as its `user.message` tells: one sent as a turn ended
/// starts another, whose answer is the prompt's.
#[derive(Default)]
struct Steers {
    state: Mutex<SteerState>,
    /// Wakes the prompt when a send is answered or fails, which may leave a `session.idle` nothing more to wait for.
    settled: tokio::sync::Notify,
}

#[derive(Default)]
struct SteerState {
    /// Whether a prompt runs that takes steers: until it ends.
    open: bool,
    /// Sends not yet answered.
    sending: usize,
    /// Messages sent whose `user.message` has not come.
    pending: HashSet<String>,
    /// Messages whose `user.message` came before their send was answered.
    delivered: HashSet<String>,
}

impl Steers {
    fn open(&self) {
        *self.state.locked() = SteerState { open: true, ..SteerState::default() };
    }

    /// Note the `user.message` of message `id`.
    fn delivered(&self, id: &str) {
        let mut state = self.state.locked();
        if !state.pending.remove(id) {
            state.delivered.insert(id.to_owned());
        }
    }

    /// At a `session.idle`, whether the prompt ends, taking no more steers; else a steer is yet to come.
    fn close(&self) -> bool {
        let mut state = self.state.locked();
        state.open = state.sending > 0 || !state.pending.is_empty();
        !state.open
    }
}

impl Drop for CopilotSession {
    fn drop(&mut self) {
        self.router.abort();
        if let Some(watcher) = &self.watcher {
            watcher.abort();
        }
    }
}

impl CopilotSession {
    /// Call `method` of the session with `params`.
    async fn call(&self, method: &str, mut params: Value) -> Result<Value, Error> {
        params["sessionId"] = json!(self.session.id());
        match self.shared.client.call(method, Some(params)).await {
            Ok(result) => Ok(result),
            Err(e) => Err(self.shared.error(&e).await),
        }
    }

    /// The session started, once signed in to the definition's `mcp_sign_in` servers needing it if it reports its
    /// sign-ins to `sign_ins` ([`mcp_sign_in`]).
    async fn open(
        shared: Arc<Shared>,
        session: Session,
        events: EventSubscription,
        base: f64,
        sign_ins: Option<Report>,
        asking: Asking,
    ) -> Self {
        let current = Current::default();
        let opened = Arc::new(mcp_sign_in::Opened { shared: shared.clone(), session: session.id().clone() });
        let servers = shared.cli.mcp_sign_in().to_vec();
        let (watched, watching) = mpsc::unbounded_channel();
        let watched = sign_ins.as_ref().filter(|_| !servers.is_empty()).map(|_| watched);
        let router = tokio::spawn(route(events, current.clone(), Arc::downgrade(&opened), watched));
        let (watcher, ready) = match sign_ins.filter(|_| !servers.is_empty()) {
            Some(report) => {
                let (ready, opening) = tokio::sync::oneshot::channel();
                let watch = mcp_sign_in::watch(opened.clone(), servers, report, watching, ready);
                (Some(tokio::spawn(watch)), Some(opening))
            }
            None => (None, None),
        };
        let (model, effort) = (shared.cli.model().map(str::to_owned), shared.cli.effort().map(str::to_owned));
        let opened = Self {
            shared,
            session,
            current,
            asking,
            router,
            model: Mutex::new(model),
            effort: Mutex::new(effort),
            spent: Mutex::new(Spent::from(base)),
            steers: Steers::default(),
            opened,
            watcher,
        };
        if let Some(ready) = ready {
            let _ = ready.await;
        }
        opened
    }

    /// Report the context window's use and the session's cost, as `change` updates them.
    fn spent(&self, updates: &Updates, change: impl FnOnce(&mut Spent)) {
        let spent = {
            let mut spent = self.spent.locked();
            change(&mut spent);
            *spent
        };
        spent.report(updates);
    }

    /// Translate an event of the prompt running; what the prompt comes to once the session is idle.
    fn translate(
        &self,
        turn: &mut Turn,
        event: &SessionEvent,
        updates: &Updates,
    ) -> Option<Result<PromptResponse, Error>> {
        let data = &event.data;
        // Sub-agents run tools for the main agent, which answers with what they found.
        let main = event.agent_id.is_none();
        match event.event_type.as_str() {
            "assistant.message_delta" if main => {
                turn.reply.delta(updates, text(data, "messageId"), text(data, "deltaContent"));
            }
            "assistant.message" if main => turn.reply.whole(updates, text(data, "messageId"), text(data, "content")),
            "assistant.reasoning_delta" if main => {
                turn.reply.thought_delta(updates, text(data, "reasoningId"), text(data, "deltaContent"));
            }
            "assistant.reasoning" if main => {
                turn.reply.whole_thought(updates, text(data, "reasoningId"), text(data, "content"));
            }
            "tool.execution_start" => updates.tool_call(tool_call(data)),
            "tool.execution_complete" => {
                let status = if data["success"] == true { ToolCallStatus::Completed } else { ToolCallStatus::Failed };
                let fields = ToolCallUpdateFields::new().status(status);
                updates.tool_update(ToolCallUpdate::new(text(data, "toolCallId").to_owned(), fields));
            }
            "assistant.usage" => {
                turn.calls += 1;
                turn.tokens.add(tokens(data));
            }
            "session.usage_info" => self.spent(updates, |spent| {
                spent.used = data["currentTokens"].as_u64().unwrap_or(spent.used);
                spent.size = data["tokenLimit"].as_u64().unwrap_or(spent.size);
            }),
            "session.usage_checkpoint" => {
                if let Some(nano) = data["totalNanoAiu"].as_f64() {
                    self.spent(updates, |spent| spent.cost = Some((nano * USD_PER_NANO_AIU - spent.base).max(0.0)));
                }
            }
            "user.message" if main => {
                // The CLI works on: an idle before it ended an earlier turn.
                turn.idle = None;
                self.steers.delivered(text(data, "messageId"));
            }
            "session.error" => turn.failed(data),
            "session.idle" => {
                turn.idle = Some(data["aborted"] == true);
                return self.steers.close().then(|| turn.end());
            }
            _ => {}
        }
        None
    }
}

/// Send the session's events to the prompt running; those coming between prompts, such as a resumed session's, are
/// dropped. Those of its MCP servers also go to `watched`, the watcher signing it in, if any, and may end a sign-in
/// waiting for a browser ([`mcp_sign_in::observe`]).
async fn route(
    mut events: EventSubscription,
    current: Current,
    opened: std::sync::Weak<mcp_sign_in::Opened>,
    watched: Option<mpsc::UnboundedSender<SessionEvent>>,
) {
    loop {
        match events.recv().await {
            Ok(event) => {
                if event.event_type.starts_with("session.mcp_") {
                    if let Some(opened) = opened.upgrade() {
                        mcp_sign_in::observe(&opened, &event);
                    }
                    if let Some(watched) = &watched {
                        let _ = watched.send(event.clone());
                    }
                }
                if let Some(prompt) = current.locked().as_ref() {
                    super::io::forward(prompt, event);
                }
            }
            Err(e) if matches!(e.kind(), RecvErrorKind::Lagged(_)) => {
                tracing::warn!(error = %e, "fell behind the Copilot CLI's session events");
            }
            Err(_) => break,
        }
    }
}

/// Clears a slot of the prompt running, such as where the session's events go, when it ends, however it does.
struct Running<'a, T>(&'a Mutex<Option<T>>);

impl<T> Drop for Running<'_, T> {
    fn drop(&mut self) {
        self.0.locked().take();
    }
}

impl NativeSession for CopilotSession {
    fn id(&self) -> String {
        based_id(self.session.id(), self.spent.locked().base)
    }

    async fn mcp_servers(&self) -> Result<Vec<McpServerState>, Error> {
        let list = self.call("session.mcp.list", json!({})).await?;
        Ok(list["servers"].as_array().into_iter().flatten().map(mcp_server).collect())
    }

    async fn mcp_sign_in(&self, server: &str) -> Result<Option<String>, Error> {
        let login = self.call("session.mcp.oauth.login", json!({ "serverName": server })).await?;
        let url = mcp_sign_in::browser_url(&login);
        if let Some(url) = &url {
            mcp_sign_in::waiting(&self.opened, server, url);
        }
        Ok(url)
    }

    async fn discard(&self) {
        self.close().await;
        if let Err(e) = self.shared.client.delete_session(self.session.id()).await {
            tracing::warn!(harness = self.shared.cli.name(), error = %e, "deleting the Copilot CLI's session");
        }
    }

    async fn prompt(&self, prompt: Vec<ContentBlock>, updates: Updates) -> Result<PromptResponse, Error> {
        let (sender, mut events) = mpsc::unbounded_channel();
        *self.current.locked() = Some(sender);
        let _running = Running(&self.current);
        *self.asking.locked() = Some(updates.clone());
        let _asking = Running(&self.asking);
        self.steers.open();
        if let Err(e) = self.session.send(message(prompt)).await {
            self.steers.state.locked().open = false;
            return Err(self.shared.error(&e).await);
        }
        let mut turn = Turn::default();
        loop {
            let event = tokio::select! {
                biased;
                event = events.recv() => event,
                () = self.steers.settled.notified() => {
                    if turn.idle.is_some() && self.steers.close() {
                        return turn.end();
                    }
                    continue;
                }
                () = self.shared.ended() => None,
            };
            let Some(event) = event else { return Err(self.shared.process.lock().await.exited().await) };
            if let Some(done) = self.translate(&mut turn, &event, &updates) {
                return done;
            }
        }
    }

    async fn steer(&self, prompt: Vec<ContentBlock>) -> Result<bool, Error> {
        {
            let mut state = self.steers.state.locked();
            if !state.open {
                return Ok(false);
            }
            state.sending += 1;
        }
        let sent = self.session.send(message(prompt).with_mode(DeliveryMode::Immediate)).await;
        let sent = {
            let mut state = self.steers.state.locked();
            state.sending -= 1;
            sent.map(|id| {
                if !state.delivered.remove(&id) {
                    state.pending.insert(id);
                }
            })
        };
        self.steers.settled.notify_one();
        match sent {
            Ok(()) => Ok(true),
            Err(e) => Err(self.shared.error(&e).await),
        }
    }

    async fn cancel(&self) {
        if let Err(e) = self.session.abort().await {
            tracing::warn!(harness = self.shared.cli.name(), error = %e, "aborting the Copilot CLI's turn");
        }
    }

    async fn close(&self) {
        if let Err(e) = self.session.disconnect().await {
            tracing::warn!(harness = self.shared.cli.name(), error = %e, "detaching the Copilot CLI's session");
        }
    }

    async fn configure(&self, setting: Setting, value: &str) -> Result<(), Error> {
        let (mut model, mut effort) = (self.model.locked().clone(), self.effort.locked().clone());
        match setting {
            Setting::Model => model = Some(value.to_owned()),
            Setting::ReasoningEffort => effort = Some(value.to_owned()),
        }
        let Some(model) = model else { return Err(unsupported("a reasoning effort without a model")) };
        let options =
            effort.clone().map(|effort| SetModelOptions { reasoning_effort: Some(effort), ..Default::default() });
        if let Err(e) = self.session.set_model(&model, options).await {
            return Err(self.shared.error(&e).await);
        }
        *self.model.locked() = Some(model);
        *self.effort.locked() = effort;
        Ok(())
    }
}

/// The message sending a prompt: its text, and its images attached.
fn message(prompt: Vec<ContentBlock>) -> MessageOptions {
    let text = prompt_text(&prompt);
    let images: Vec<Attachment> = prompt
        .into_iter()
        .filter_map(|block| match block {
            ContentBlock::Image(image) => {
                Some(Attachment::Blob { data: image.data, mime_type: image.mime_type, display_name: None })
            }
            _ => None,
        })
        .collect();
    let message = MessageOptions::new(text);
    if images.is_empty() { message } else { message.with_attachments(images) }
}

/// The string `key` of an event's data, empty if it has none.
fn text<'a>(data: &'a Value, key: &str) -> &'a str {
    data[key].as_str().unwrap_or_default()
}

/// A tool call the CLI started, named by its title, kind and the file it works on.
fn tool_call(data: &Value) -> ToolCall {
    let name = text(data, "toolName");
    let title = data["toolTitle"].as_str().filter(|title| !title.is_empty()).unwrap_or(name);
    let arguments = &data["arguments"];
    let mut call = ToolCall::new(text(data, "toolCallId").to_owned(), title)
        .kind(tool_kind(name))
        .status(ToolCallStatus::InProgress);
    if !arguments.is_null() {
        call = call.raw_input(arguments.clone());
    }
    if let Some(path) = arguments["path"].as_str() {
        call = call.locations(vec![ToolCallLocation::new(path)]);
    }
    call
}

/// The kind of the CLI's built-in tool `name`.
fn tool_kind(name: &str) -> ToolKind {
    match name {
        "bash" | "read_bash" | "write_bash" | "stop_bash" | "powershell" | "read_powershell" | "write_powershell"
        | "stop_powershell" => ToolKind::Execute,
        "view" | "read" => ToolKind::Read,
        "edit" | "create" | "str_replace" | "apply_patch" => ToolKind::Edit,
        "grep" | "glob" | "rg" | "search" => ToolKind::Search,
        "web_fetch" | "fetch" => ToolKind::Fetch,
        _ => ToolKind::Other,
    }
}

/// The tokens of a model call, from its `assistant.usage`.
fn tokens(data: &Value) -> Tokens {
    let count = |key: &str| data[key].as_u64().unwrap_or_default();
    Tokens {
        input: count("inputTokens"),
        output: count("outputTokens"),
        cache_read: count("cacheReadTokens"),
        cache_write: count("cacheWriteTokens"),
        thought: count("reasoningTokens"),
    }
}

/// What a prompt's turn came to so far.
#[derive(Default)]
struct Turn {
    /// The reply and reasoning: what streamed in pieces is not repeated whole.
    reply: Reply,
    /// The tokens the prompt used, summed over the model calls of its turn, and how many those were.
    tokens: Tokens,
    calls: u64,
    /// The first error ending the turn.
    error: Option<Error>,
    /// The last error of a model call the CLI retries.
    retried: Option<Error>,
    /// Whether the last `session.idle` said the turn was aborted, once one came.
    idle: Option<bool>,
}

impl Turn {
    /// Note a `session.error`.
    fn failed(&mut self, data: &Value) {
        let error = turn_error(data);
        if data["errorType"] == "model_call" {
            tracing::debug!(error = %error, "the Copilot CLI retries a failed model call");
            self.retried = Some(error);
        } else if self.error.is_none() {
            self.error = Some(error);
        }
    }

    /// What the prompt comes to at its last `session.idle`: cancelled if aborted, else the error that ended the turn,
    /// or the last failed model call of a turn that ended without a reply.
    fn end(&mut self) -> Result<PromptResponse, Error> {
        let stop = if self.idle == Some(true) {
            StopReason::Cancelled
        } else if let Some(error) = self.error.take() {
            return Err(error);
        } else if let Some(error) = self.retried.take().filter(|_| !self.reply.said() && self.calls == 0) {
            return Err(error);
        } else {
            StopReason::EndTurn
        };
        let usage = (self.calls > 0).then(|| self.tokens.usage(self.tokens.input + self.tokens.output));
        Ok(PromptResponse::new(stop).usage(usage))
    }
}

/// The error a `session.error` reports, by its type: not signed in, a rate limit, a weekly limit or exhausted quota
/// (of AI credits or premium requests), a context overflow, or another failure.
fn turn_error(data: &Value) -> Error {
    let message = data["message"].as_str().filter(|m| !m.is_empty()).unwrap_or("Copilot CLI failed");
    let status = data["statusCode"].as_u64().and_then(|status| u16::try_from(status).ok());
    if data["remediation"] == "sign_in" {
        return signed_out(message);
    }
    match (text(data, "errorType"), text(data, "errorCode")) {
        ("authentication", _) => signed_out(message),
        ("rate_limit", "user_weekly_rate_limited") => quota_exhausted(None, format!("weekly limit: {message}")),
        ("rate_limit", _) => rate_limited(status, None, message),
        ("quota", _) => quota_exhausted(None, message),
        ("context_limit", _) => context_overflow(message),
        (_, _) if matches!(status, Some(429)) => rate_limited(status, None, message),
        _ => failed(message),
    }
}

#[cfg(all(test, unix))]
mod tests;
