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
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{
    ContentBlock, PromptResponse, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
};
use ants_proto::{McpServerState, McpStatus};
use anyhow::bail;
use github_copilot_sdk as sdk;
use sdk::session::Session;
use sdk::subscription::{EventSubscription, RecvErrorKind};
use sdk::{
    DeliveryMode, ErrorKind, ProtocolErrorKind, ResumeSessionConfig, SessionConfig, SessionEvent, SessionId,
    SetModelOptions,
};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{mpsc, watch};

use self::args::SessionArgs;
use self::permissions::{Asking, Permissions};
use self::turn::{Turn, message, text, tokens, tool_call};
use super::turn::Spent;
use super::{
    Cli, Harness, Info, Native, NativeSession, Open, Process, Setting, Updates, based_id, cost_base, failed, is_flag,
    mcp_servers_json, signed_out, unsupported,
};
use crate::harness::mcp_sign_in::Report;
use crate::sync::MutexExt as _;

mod args;
mod mcp_sign_in;
mod permissions;
mod turn;

/// The release of github-copilot-sdk linked, as pinned in ants-daemon's Cargo.toml: the CLI must speak its protocol.
const SDK_VERSION: &str = "1.0.29";

/// The CLI's own arguments, before the definition's: its JSON-RPC server on stdio, neither updating itself nor sending
/// its client telemetry.
const SERVER_ARGS: [&str; 4] = ["--server", "--stdio", "--no-auto-update", "--disable-client-telemetry"];

/// Arguments making the CLI run something other than its server.
const FOREIGN_ARGS: [&str; 9] =
    ["--acp", "-p", "--prompt", "-i", "--interactive", "--resume", "-r", "--continue", "--connect"];

/// US dollars of a nano AI credit, the unit the CLI counts a session's cost in: an AI credit is US$0.01.
const USD_PER_NANO_AIU: f64 = 1e-11;

/// What the daemon's `initialize` names the CLI, as `copilot --version` does.
const NAME: &str = "GitHub Copilot CLI";

pub(crate) struct Copilot {
    shared: Arc<Shared>,
    version: String,
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
        if e.is_transport_failure() || *self.closed.borrow() { self.exited().await } else { failed(e) }
    }

    /// The error of the CLI's output ending.
    async fn exited(&self) -> Error {
        self.process.lock().await.exited().await
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
        let foreign = |arg: &&String| FOREIGN_ARGS.iter().any(|flag| is_flag(arg, flag));
        if let Some(arg) = harness.args.iter().find(foreign) {
            bail!("protocol copilot-sdk runs the CLI's server, which {arg} in args would replace");
        }
        SessionArgs::take(&mut harness.args.clone()).map_err(anyhow::Error::msg)?;
        for (name, value) in &harness.context {
            SessionArgs::take(&mut value.args.clone()).map_err(|e| anyhow::anyhow!("context {name}: {e}"))?;
        }
        Ok(())
    }

    async fn start(cli: Cli) -> Result<Self, Error> {
        let mut own = cli.args().to_vec();
        let args = SessionArgs::take(&mut own).map_err(failed)?;
        let cli = cli.with_args(own);
        let mcp = args.mcp_servers().map_err(failed)?;
        let (process, stdin, stdout) = cli.spawn(SERVER_ARGS, None)?;
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
            let Some(event) = event else { return Err(self.shared.exited().await) };
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

#[cfg(all(test, unix))]
mod tests;
