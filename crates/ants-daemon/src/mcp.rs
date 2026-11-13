//! MCP tools for the persona's harness sessions: `send_message`, `ask_agent` and `check_inbox`. To-dos and memories go
//! through the persona's bd workspace instead (see [`crate::workspace`]).
//!
//! One server per persona, with two ways in: streamable HTTP on a loopback port, for harnesses that take HTTP MCP
//! servers, and a Unix socket that `ants mcp` bridges stdio to, for the others. The sessions of each thread get their
//! own bearer token, which tells the tools which thread calls them.
//!
//! A reply to `ask_agent` lands in the persona's inbox like any message. It reaches the waiting call through a core NATS
//! subscription to the inbox ([`Mcp::listen`]), so it never waits behind the prompt that asked, and [`Mcp::claim`] keeps
//! the inbox consumer from prompting with it. A reply that comes after the call gave up is kept for `check_inbox`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::v1::schema::{EnvVariable, HttpHeader, McpServer, McpServerHttp, McpServerStdio};
use ants_proto::{Envelope, PersonaId, ThreadId, Uuid};
use anyhow::{Context as _, Result};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::http::{Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData, RoleServer, ServerHandler, ServiceExt as _, schemars, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::bus;
use crate::harness::McpServers;

/// Environment variable carrying the session's token to `ants mcp`.
pub const TOKEN_ENV: &str = "ANTS_MCP_TOKEN";
/// Name the harness shows the tools under.
const SERVER_NAME: &str = "ants";
const DEFAULT_ASK_SECS: u64 = 300;
const MAX_ASK_SECS: u64 = 1800;
/// Asks remembered for their replies; the oldest are forgotten first.
const MAX_ASKS: usize = 1024;
/// Late replies kept for `check_inbox`; the oldest are dropped first.
const MAX_LATE: usize = 256;
/// Replies taken by a waiting call that the inbox consumer has not handed over yet.
const MAX_ROUTED: usize = 1024;

/// Publishes an envelope and resolves once JetStream has stored it.
pub type Outbox = Arc<dyn Fn(Envelope) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Publishes through JetStream.
pub fn jetstream_outbox(js: async_nats::jetstream::Context) -> Outbox {
    Arc::new(move |envelope| {
        let js = js.clone();
        Box::pin(async move { bus::publish(&js, &envelope).await })
    })
}

/// The MCP server of one persona. Dropping it stops the listeners.
pub struct Mcp {
    hub: Arc<Hub>,
    url: String,
    socket: Option<PathBuf>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Mcp {
    fn drop(&mut self) {
        self.tasks.iter().for_each(JoinHandle::abort);
        if let Some(socket) = &self.socket {
            let _ = std::fs::remove_file(socket);
        }
    }
}

impl Mcp {
    /// Listen on a loopback port and on `<home>/mcp.sock`. What must survive a restart (asks waiting for a reply, late
    /// replies) lives in `<home>/mcp.json`.
    pub async fn start(persona: PersonaId, home: &Path, outbox: Outbox) -> Result<Self> {
        let state_path = home.join("mcp.json");
        let stored = match tokio::fs::read(&state_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("reading {}", state_path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Stored::default(),
            Err(e) => return Err(e).with_context(|| format!("reading {}", state_path.display())),
        };
        let hub = Arc::new(Hub {
            persona,
            outbox,
            state_path,
            sessions: Mutex::default(),
            asks: tokio::sync::Mutex::new(Asks { stored, waiting: HashMap::new() }),
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.context("binding the MCP port")?;
        let url = format!("http://{}/mcp", listener.local_addr()?);
        let mut tasks = vec![tokio::spawn(serve_http(listener, hub.clone()))];

        // The stdio fallback is optional: without the socket, harnesses lacking HTTP MCP just get no tools.
        let path = home.join("mcp.sock");
        let _ = std::fs::remove_file(&path);
        let socket = match bind_private(&path) {
            Ok(listener) => {
                tasks.push(tokio::spawn(serve_socket(listener, hub.clone())));
                Some(path)
            }
            Err(e) => {
                tracing::warn!(socket = %path.display(), error = format!("{e:#}"), "no MCP socket for stdio harnesses");
                None
            }
        };
        tracing::info!(%url, "serving MCP tools");
        Ok(Self { hub, url, socket, tasks })
    }

    /// The MCP servers each harness session gets: HTTP where the harness takes it, else `ants mcp` over the socket.
    pub fn servers(&self) -> McpServers {
        let (hub, url, socket) = (self.hub.clone(), self.url.clone(), self.socket.clone());
        let exe = std::env::current_exe().ok();
        Arc::new(move |thread, http| {
            let token = hub.token(thread);
            if http {
                let auth = HttpHeader::new(header::AUTHORIZATION.as_str(), format!("Bearer {token}"));
                return vec![McpServer::Http(McpServerHttp::new(SERVER_NAME, &url).headers(vec![auth]))];
            }
            match (&exe, socket.as_ref().and_then(|s| s.to_str())) {
                (Some(exe), Some(socket)) => vec![McpServer::Stdio(
                    McpServerStdio::new(SERVER_NAME, exe)
                        .args(vec!["mcp".to_owned(), "--socket".to_owned(), socket.to_owned()])
                        .env(vec![EnvVariable::new(TOKEN_ENV, token)]),
                )],
                _ => Vec::new(),
            }
        })
    }

    /// Hand replies to waiting `ask_agent` calls as they arrive on `inbox`, a core subscription to the persona's inbox.
    pub fn listen(&mut self, mut inbox: async_nats::Subscriber) {
        let hub = self.hub.clone();
        self.tasks.push(tokio::spawn(async move {
            while let Some(msg) = inbox.next().await {
                let Ok(envelope) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
                // The inbox consumer dead-letters a forged sender; it must not answer an ask meanwhile.
                match envelope.check_subject(&msg.subject) {
                    Ok(()) => hub.route(envelope).await,
                    Err(e) => tracing::warn!(error = %e, "ignoring a reply with a forged sender"),
                }
            }
        }));
    }

    /// Whether `envelope`, from the inbox consumer, answers an `ask_agent` call; then it is no prompt but the call's
    /// reply, or kept for `check_inbox` when the call gave up. True means it is handled and may be acked.
    pub async fn claim(&self, envelope: &Envelope) -> Result<bool> {
        self.hub.claim(envelope).await
    }

    /// Mark `envelope` as the message its thread is handling until the guard drops. Messages the thread's session
    /// sends meanwhile follow on from it, and spend its turn budget.
    pub fn begin(&self, envelope: &Envelope) -> Turn {
        self.hub.sessions.lock().expect("sessions").current.insert(envelope.thread_id.clone(), envelope.clone());
        Turn { hub: self.hub.clone(), thread: envelope.thread_id.clone() }
    }
}

/// See [`Mcp::begin`].
pub struct Turn {
    hub: Arc<Hub>,
    thread: ThreadId,
}

impl Drop for Turn {
    fn drop(&mut self) {
        self.hub.sessions.lock().expect("sessions").current.remove(&self.thread);
    }
}

/// Bridge stdin and stdout to the MCP socket of a daemon, as `ants mcp` does for harnesses without HTTP MCP. Ends when
/// the daemon closes the connection.
pub async fn bridge_stdio(socket: &Path, token: &str) -> Result<()> {
    let stream = UnixStream::connect(socket).await.with_context(|| format!("connecting to {}", socket.display()))?;
    let (mut from_daemon, mut to_daemon) = stream.into_split();
    to_daemon.write_all(format!("{token}\n").as_bytes()).await?;
    let up = async {
        tokio::io::copy(&mut tokio::io::stdin(), &mut to_daemon).await?;
        // The harness closed stdin: let the daemon finish and close its side.
        to_daemon.shutdown().await?;
        std::future::pending::<std::io::Result<()>>().await
    };
    let down = async {
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut from_daemon, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::select! {
        r = up => r?,
        r = down => r?,
    }
    Ok(())
}

#[derive(Default)]
struct Sessions {
    tokens: HashMap<String, ThreadId>,
    by_thread: HashMap<ThreadId, String>,
    /// The message each thread is handling.
    current: HashMap<ThreadId, Envelope>,
}

/// What `<home>/mcp.json` keeps.
#[derive(Default, Serialize, Deserialize)]
struct Stored {
    /// Asks whose reply the inbox consumer has not handed over yet, oldest first, with whom they went to.
    asks: Vec<(Uuid, PersonaId)>,
    /// Replies that came after their `ask_agent` call gave up, oldest first.
    late: Vec<Envelope>,
    /// Replies a waiting call took, oldest first, until the inbox consumer hands them over.
    #[serde(default)]
    routed: Vec<Uuid>,
}

impl Stored {
    fn asked(&self, ask: Uuid) -> Option<&PersonaId> {
        self.asks.iter().find(|(id, _)| *id == ask).map(|(_, to)| to)
    }

    fn forget(&mut self, ask: Uuid) {
        self.asks.retain(|(id, _)| *id != ask);
    }
}

struct Asks {
    stored: Stored,
    /// `ask_agent` calls waiting, by the id of their ask.
    waiting: HashMap<Uuid, oneshot::Sender<Envelope>>,
}

struct Hub {
    persona: PersonaId,
    outbox: Outbox,
    state_path: PathBuf,
    sessions: Mutex<Sessions>,
    asks: tokio::sync::Mutex<Asks>,
}

impl Hub {
    /// The token of `thread`'s sessions, made on first use.
    fn token(&self, thread: &ThreadId) -> String {
        let mut sessions = self.sessions.lock().expect("sessions");
        if let Some(token) = sessions.by_thread.get(thread) {
            return token.clone();
        }
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        sessions.tokens.insert(token.clone(), thread.clone());
        sessions.by_thread.insert(thread.clone(), token.clone());
        token
    }

    fn caller(&self, token: &str) -> Option<ThreadId> {
        self.sessions.lock().expect("sessions").tokens.get(token).cloned()
    }

    /// A message from the persona, following on from the one `thread` is handling: on the same thread unless `on`
    /// names another (`new` for a fresh one), and with one turn less unless it answers the human.
    fn outgoing(&self, thread: &ThreadId, to: &str, text: String, on: Option<&str>) -> Result<Envelope, String> {
        let to: PersonaId = to.trim().trim_start_matches('@').parse().map_err(|e| format!("{e}"))?;
        if to == self.persona {
            return Err(format!("{to} is this persona; message another one"));
        }
        let current = self.sessions.lock().expect("sessions").current.get(thread).cloned();
        let current = current.ok_or("no message is being handled on this thread; messages go out only while one is")?;
        let mut envelope = current
            .reply(self.persona.clone(), text)
            .map_err(|_| format!("thread {thread} has no turns left; answer {} in the reply instead", current.from))?;
        envelope.to = to;
        envelope.in_reply_to = None;
        match on.map(str::trim) {
            None | Some("") => {}
            Some("new") => envelope.thread_id = ThreadId::generate(),
            Some(other) => envelope.thread_id = other.parse().map_err(|e| format!("{e}"))?,
        }
        Ok(envelope)
    }

    async fn send(&self, envelope: &Envelope) -> Result<(), String> {
        (self.outbox)(envelope.clone()).await.map_err(|e| format!("not sent: {e:#}"))
    }

    /// Send `envelope` and wait up to `wait` (or until `cancelled`) for its reply.
    async fn ask(
        &self,
        envelope: Envelope,
        wait: Duration,
        cancelled: impl Future<Output = ()>,
    ) -> Result<Envelope, String> {
        let (tx, mut rx) = oneshot::channel();
        {
            let mut asks = self.asks.lock().await;
            asks.stored.asks.push((envelope.id, envelope.to.clone()));
            let excess = asks.stored.asks.len().saturating_sub(MAX_ASKS);
            asks.stored.asks.drain(..excess);
            asks.waiting.insert(envelope.id, tx);
            // Remembered before it is sent, so a reply after a restart is not taken for a new message.
            if let Err(e) = self.save(&asks.stored).await {
                asks.stored.forget(envelope.id);
                asks.waiting.remove(&envelope.id);
                return Err(format!("not sent: {e:#}"));
            }
        }
        if let Err(e) = self.send(&envelope).await {
            let mut asks = self.asks.lock().await;
            asks.waiting.remove(&envelope.id);
            asks.stored.forget(envelope.id);
            return Err(e);
        }
        tokio::select! {
            reply = &mut rx => if let Ok(reply) = reply { return Ok(reply) },
            () = tokio::time::sleep(wait) => {}
            () = cancelled => {}
        }
        // Routing sends under the lock, so once the waiter is gone a reply is either in `rx` already or kept for later.
        self.asks.lock().await.waiting.remove(&envelope.id);
        rx.try_recv().map_err(|_| {
            format!(
                "no reply from {} within {}s; a reply that comes later shows up in check_inbox",
                envelope.to,
                wait.as_secs()
            )
        })
    }

    /// Hand a reply from the live subscription to the call waiting for it.
    async fn route(&self, envelope: Envelope) {
        let Some(ask) = envelope.in_reply_to else { return };
        let mut asks = self.asks.lock().await;
        if asks.stored.asked(ask) != Some(&envelope.from) {
            return;
        }
        let Some(waiter) = asks.waiting.remove(&ask) else { return };
        let id = envelope.id;
        if waiter.send(envelope).is_ok() {
            asks.stored.routed.push(id);
            let excess = asks.stored.routed.len().saturating_sub(MAX_ROUTED);
            asks.stored.routed.drain(..excess);
            // Kept so that after a restart the inbox consumer's copy is not shown again; losing it only risks that.
            if let Err(e) = self.save(&asks.stored).await {
                tracing::warn!(error = format!("{e:#}"), "saving MCP state");
            }
        }
    }

    async fn claim(&self, envelope: &Envelope) -> Result<bool> {
        let Some(ask) = envelope.in_reply_to else { return Ok(false) };
        let mut asks = self.asks.lock().await;
        if asks.stored.asked(ask) != Some(&envelope.from) {
            return Ok(false);
        }
        asks.stored.forget(ask);
        let routed = asks.stored.routed.len();
        asks.stored.routed.retain(|id| *id != envelope.id);
        let taken = asks.stored.routed.len() < routed
            || asks.waiting.remove(&ask).is_some_and(|waiter| waiter.send(envelope.clone()).is_ok());
        if taken {
            // Saving only shortens the list of asks; a failure leaves an entry that never matches again.
            if let Err(e) = self.save(&asks.stored).await {
                tracing::warn!(error = format!("{e:#}"), "saving MCP state");
            }
            return Ok(true);
        }
        if !asks.stored.late.iter().any(|e| e.id == envelope.id) {
            tracing::info!(thread = %envelope.thread_id, from = %envelope.from, "keeping a late reply for check_inbox");
            asks.stored.late.push(envelope.clone());
            let excess = asks.stored.late.len().saturating_sub(MAX_LATE);
            asks.stored.late.drain(..excess);
        }
        if let Err(e) = self.save(&asks.stored).await {
            // Not acked, so it comes again; it must still count as a reply then.
            asks.stored.asks.push((ask, envelope.from.clone()));
            return Err(e);
        }
        Ok(true)
    }

    /// Take the late replies on `thread`, or on every thread.
    async fn take_late(&self, thread: Option<&ThreadId>) -> Result<Vec<Envelope>> {
        let mut asks = self.asks.lock().await;
        let (taken, kept) =
            std::mem::take(&mut asks.stored.late).into_iter().partition(|e| thread.is_none_or(|t| &e.thread_id == t));
        asks.stored.late = kept;
        if let Err(e) = self.save(&asks.stored).await {
            asks.stored.late.extend(taken);
            return Err(e);
        }
        Ok(taken)
    }

    async fn save(&self, stored: &Stored) -> Result<()> {
        let tmp = self.state_path.with_extension("json.tmp");
        tokio::fs::write(&tmp, serde_json::to_vec(stored)?).await?;
        tokio::fs::rename(&tmp, &self.state_path)
            .await
            .with_context(|| format!("writing {}", self.state_path.display()))
    }
}

/// The thread an HTTP request comes from, after its token checked out.
#[derive(Clone)]
struct Caller(ThreadId);

async fn serve_http(listener: TcpListener, hub: Arc<Hub>) {
    let factory_hub = hub.clone();
    let service = StreamableHttpService::new(
        move || Ok(Tools::new(factory_hub.clone(), None)),
        Arc::new(NeverSessionManager::default()),
        StreamableHttpServerConfig::default().with_legacy_session_mode(false),
    );
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!(error = %e, "accepting an MCP connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (service, hub) = (service.clone(), hub.clone());
        tokio::spawn(async move {
            let handle = hyper::service::service_fn(move |mut request: Request<Incoming>| {
                let mut service = service.clone();
                let caller = bearer(&request).and_then(|token| hub.caller(token));
                async move {
                    let Some(thread) = caller else {
                        let body = Full::new(Bytes::from_static(b"unknown or missing bearer token")).boxed();
                        let mut response = Response::new(body);
                        *response.status_mut() = StatusCode::UNAUTHORIZED;
                        return Ok(response);
                    };
                    request.extensions_mut().insert(Caller(thread));
                    tower_service::Service::call(&mut service, request).await
                }
            });
            if let Err(e) =
                hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), handle).await
            {
                tracing::debug!(error = %e, "MCP connection");
            }
        });
    }
}

fn bearer<B>(request: &Request<B>) -> Option<&str> {
    request.headers().get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")
}

/// A Unix socket only the daemon's user may connect to.
fn bind_private(path: &Path) -> Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt as _;
    let listener = UnixListener::bind(path).with_context(|| format!("binding {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Each connection starts with the session's token on a line of its own, then carries MCP as JSON lines.
async fn serve_socket(listener: UnixListener, hub: Arc<Hub>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!(error = %e, "accepting an MCP socket connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let hub = hub.clone();
        tokio::spawn(async move {
            let (read, write) = stream.into_split();
            let mut read = BufReader::new(read);
            let mut line = String::new();
            if (&mut read).take(256).read_line(&mut line).await.is_err() {
                return;
            }
            let Some(thread) = hub.caller(line.trim_end()) else {
                tracing::warn!("MCP socket connection with an unknown token");
                return;
            };
            match Tools::new(hub, Some(thread)).serve((read, write)).await {
                Ok(service) => {
                    let _ = service.waiting().await;
                }
                Err(e) => tracing::debug!(error = %e, "MCP socket session"),
            }
        });
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SendMessage {
    /// Recipient persona, such as `dev-b`, or `human`.
    to: String,
    /// The message.
    text: String,
    /// Thread to send on: omitted for the current one, `new` for a fresh one, or a thread id.
    #[serde(default)]
    thread: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AskAgent {
    /// Persona to ask, such as `dev-b`.
    to: String,
    /// The question.
    text: String,
    /// Seconds to wait for the reply (default 300, at most 1800).
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CheckInbox {
    /// Take the messages of every thread of the persona, not only this one's.
    #[serde(default)]
    all_threads: bool,
}

/// The tools, served to one MCP connection. Over HTTP the caller's thread comes with each request instead.
#[derive(Clone)]
struct Tools {
    hub: Arc<Hub>,
    caller: Option<ThreadId>,
    tool_router: ToolRouter<Self>,
}

impl Tools {
    fn new(hub: Arc<Hub>, caller: Option<ThreadId>) -> Self {
        Self { hub, caller, tool_router: Self::tool_router() }
    }

    fn thread(&self, cx: &RequestContext<RoleServer>) -> Result<ThreadId, ErrorData> {
        self.caller
            .clone()
            .or_else(|| {
                let parts = cx.extensions.get::<hyper::http::request::Parts>()?;
                parts.extensions.get::<Caller>().map(|c| c.0.clone())
            })
            .ok_or_else(|| ErrorData::invalid_request("no ants session behind this connection", None))
    }
}

fn text(result: Result<String, String>) -> CallToolResult {
    match result {
        Ok(text) => CallToolResult::success(vec![ContentBlock::text(text)]),
        Err(text) => CallToolResult::error(vec![ContentBlock::text(text)]),
    }
}

#[tool_router]
impl Tools {
    #[tool(description = "Send a message to another persona, or to `human`, and return at once. It goes on the \
        current thread unless `thread` says otherwise. A message between agents spends one turn of the thread's \
        budget; with none left it is refused and the thread is the human's to decide. A reply comes later as a new \
        message.")]
    async fn send_message(
        &self,
        Parameters(args): Parameters<SendMessage>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        let result = async {
            let envelope = self.hub.outgoing(&thread, &args.to, args.text, args.thread.as_deref())?;
            self.hub.send(&envelope).await?;
            Ok(format!(
                "Sent to {} on thread {} ({} turns left).",
                envelope.to, envelope.thread_id, envelope.turn_budget
            ))
        }
        .await;
        Ok(text(result))
    }

    #[tool(description = "Ask another persona and wait for its reply, which is returned. The question goes on the \
        current thread and spends one turn of its budget. A reply that arrives after the wait gives up is kept for \
        check_inbox.")]
    async fn ask_agent(
        &self,
        Parameters(args): Parameters<AskAgent>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        let wait = Duration::from_secs(args.timeout_secs.unwrap_or(DEFAULT_ASK_SECS).clamp(1, MAX_ASK_SECS));
        let result = async {
            let envelope = self.hub.outgoing(&thread, &args.to, args.text, None)?;
            if envelope.to.is_human() {
                return Err("ask_agent is for agents; send_message reaches the human".to_owned());
            }
            if envelope.turn_budget == 0 {
                return Err(format!("thread {thread} has no turns left for a reply; ask the human instead"));
            }
            let reply = self.hub.ask(envelope, wait, cx.ct.cancelled()).await?;
            Ok(format!("{} replied:\n{}", reply.from, reply.text))
        }
        .await;
        Ok(text(result))
    }

    #[tool(description = "Take the messages kept for this persona outside of any prompt: replies to ask_agent calls \
        that arrived after the call gave up. Each is returned once. Only this thread's unless `all_threads` is set.")]
    async fn check_inbox(
        &self,
        Parameters(args): Parameters<CheckInbox>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        let late = match self.hub.take_late((!args.all_threads).then_some(&thread)).await {
            Ok(late) => late,
            Err(e) => return Ok(text(Err(format!("{e:#}")))),
        };
        if late.is_empty() {
            return Ok(text(Ok("No messages.".to_owned())));
        }
        let shown: Vec<String> = late
            .iter()
            .map(|e| format!("From {} on thread {} at {}:\n{}", e.from, e.thread_id, e.sent_at.to_rfc3339(), e.text))
            .collect();
        Ok(text(Ok(shown.join("\n\n"))))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Tools {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(SERVER_NAME, env!("CARGO_PKG_VERSION")))
            .with_instructions(format!(
                "Messaging between ants personas; this session speaks as {}. send_message and ask_agent reach other \
                 personas, check_inbox collects late replies. To-dos and memories go through bd.",
                self.hub.persona
            ))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;

    use agent_client_protocol::v1::schema::{
        AgentCapabilities, ContentBlock as AcpContent, ContentChunk, InitializeRequest, InitializeResponse,
        LoadSessionRequest, LoadSessionResponse, McpCapabilities, NewSessionRequest, NewSessionResponse, PromptRequest,
        PromptResponse, SessionNotification, SessionUpdate, StopReason, TextContent,
    };
    use agent_client_protocol::{Agent, DynConnectTo};
    use rmcp::RoleClient;
    use rmcp::model::{CallToolRequestParams, ClientConfig};
    use rmcp::service::RunningService;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use tokio::sync::mpsc;

    use super::*;
    use crate::harness::Launch;
    use crate::pool::{Pool, SessionStore};

    type Client = RunningService<RoleClient, ClientConfig>;

    fn persona(name: &str) -> PersonaId {
        name.parse().unwrap()
    }

    /// A fresh daemon home under the target directory, short enough for a socket path.
    fn home(name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/mcp-t").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// An MCP server for `dev-a` whose sent envelopes come out of the receiver.
    async fn start(home: &Path) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let outbox: Outbox = Arc::new(move |envelope| {
            let _ = tx.send(envelope);
            Box::pin(async { Ok(()) })
        });
        (Mcp::start(persona("dev-a"), home, outbox).await.unwrap(), rx)
    }

    /// The human's message `dev-a` is handling on a fresh thread.
    fn from_human() -> Envelope {
        Envelope::new(PersonaId::human(), persona("dev-a"), "go")
    }

    async fn http_client(url: &str, token: &str) -> Client {
        let config = StreamableHttpClientTransportConfig::with_uri(url).auth_header(token);
        ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap()
    }

    /// The URL and token the sessions of `thread` get over HTTP.
    fn http_server(mcp: &Mcp, thread: &ThreadId) -> (String, String) {
        let [McpServer::Http(server)] = &mcp.servers()(thread, true)[..] else { panic!("one HTTP server") };
        let token = server.headers[0].value.strip_prefix("Bearer ").unwrap().to_owned();
        (server.url.clone(), token)
    }

    async fn call(client: &Client, tool: &'static str, args: serde_json::Value) -> (bool, String) {
        let serde_json::Value::Object(args) = args else { panic!("arguments are an object") };
        let result = client.call_tool(CallToolRequestParams::new(tool).with_arguments(args)).await.unwrap();
        let text = result.content.iter().filter_map(|c| c.as_text()).map(|t| t.text.as_str()).collect();
        (result.is_error != Some(true), text)
    }

    #[tokio::test]
    async fn send_message_follows_on_from_the_current_message() {
        let (mcp, mut sent) = start(&home("send")).await;
        let current = from_human();
        let (url, token) = http_server(&mcp, &current.thread_id);
        let client = http_client(&url, &token).await;
        let tools: Vec<_> = client.list_all_tools().await.unwrap().into_iter().map(|t| t.name).collect();
        assert_eq!(tools.len(), 3, "{tools:?}");

        let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "hi"})).await;
        assert!(!ok && text.contains("no message is being handled"), "{text}");

        let turn = mcp.begin(&current);
        let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "@dev-b", "text": "hi"})).await;
        assert!(ok, "{text}");
        let out = sent.recv().await.unwrap();
        assert_eq!((&out.from, &out.to, &out.thread_id), (&persona("dev-a"), &persona("dev-b"), &current.thread_id));
        assert_eq!((out.turn_budget, out.in_reply_to), (current.turn_budget, None), "free after the human's message");

        let (ok, _) =
            call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "x", "thread": "new"})).await;
        assert!(ok);
        assert_ne!(sent.recv().await.unwrap().thread_id, current.thread_id);

        let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-a", "text": "me"})).await;
        assert!(!ok && text.contains("this persona"), "{text}");
        drop(turn);
        let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "late"})).await;
        assert!(!ok, "nothing goes out once the message is handled");
    }

    #[tokio::test]
    async fn messages_between_agents_spend_the_budget() {
        let (mcp, mut sent) = start(&home("budget")).await;
        let mut current = Envelope::new(persona("dev-b"), persona("dev-a"), "q");
        current.turn_budget = 1;
        let _turn = mcp.begin(&current);
        let client = {
            let (url, token) = http_server(&mcp, &current.thread_id);
            http_client(&url, &token).await
        };
        let (ok, text) = call(&client, "ask_agent", serde_json::json!({"to": "dev-c", "text": "?"})).await;
        assert!(!ok && text.contains("no turns left"), "{text}");
        let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "fyi"})).await;
        assert!(ok);
        assert_eq!(sent.recv().await.unwrap().turn_budget, 0);

        current.turn_budget = 0;
        let _turn = mcp.begin(&current);
        let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "more"})).await;
        assert!(!ok && text.contains("no turns left"), "{text}");
    }

    #[tokio::test]
    async fn http_needs_a_known_token() {
        let (mcp, _sent) = start(&home("auth")).await;
        let (url, _) = http_server(&mcp, &from_human().thread_id);
        let config = StreamableHttpClientTransportConfig::with_uri(url).auth_header("nope");
        let refused = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await;
        assert!(refused.is_err());
    }

    #[tokio::test]
    async fn ask_agent_gets_the_reply_while_the_inbox_consumer_holds_it() {
        let (mcp, mut sent) = start(&home("ask")).await;
        let current = from_human();
        let _turn = mcp.begin(&current);
        let (url, token) = http_server(&mcp, &current.thread_id);
        let client = http_client(&url, &token).await;
        let asking = tokio::spawn(async move {
            call(&client, "ask_agent", serde_json::json!({"to": "dev-b", "text": "6*7?"})).await
        });
        let ask = sent.recv().await.unwrap();
        assert_eq!((&ask.to, &ask.thread_id), (&persona("dev-b"), &current.thread_id));

        // Someone else answering does not count.
        let mut forged = ask.reply(persona("dev-c"), "13").unwrap();
        forged.in_reply_to = Some(ask.id);
        mcp.hub.route(forged.clone()).await;
        assert!(!mcp.claim(&forged).await.unwrap(), "a forged reply is an ordinary message");

        let reply = ask.reply(persona("dev-b"), "42").unwrap();
        mcp.hub.route(reply.clone()).await;
        let (ok, text) = asking.await.unwrap();
        assert!(ok && text.ends_with("42"), "{text}");
        assert!(mcp.claim(&reply).await.unwrap(), "the consumer's copy is acked, not prompted");
        assert!(!mcp.claim(&reply).await.unwrap(), "the ask is settled");
    }

    #[tokio::test]
    async fn the_consumer_can_deliver_the_reply_too() {
        let (mcp, mut sent) = start(&home("ask-js")).await;
        let current = from_human();
        let _turn = mcp.begin(&current);
        let (url, token) = http_server(&mcp, &current.thread_id);
        let client = http_client(&url, &token).await;
        let asking =
            tokio::spawn(
                async move { call(&client, "ask_agent", serde_json::json!({"to": "dev-b", "text": "?"})).await },
            );
        let reply = sent.recv().await.unwrap().reply(persona("dev-b"), "yes").unwrap();
        assert!(mcp.claim(&reply).await.unwrap());
        assert!(asking.await.unwrap().1.ends_with("yes"));
        mcp.hub.route(reply).await;
    }

    #[tokio::test]
    async fn a_late_reply_waits_in_check_inbox_across_restarts() {
        let dir = home("late");
        let (mcp, mut sent) = start(&dir).await;
        let current = from_human();
        let turn = mcp.begin(&current);
        let (url, token) = http_server(&mcp, &current.thread_id);
        let client = http_client(&url, &token).await;
        let (ok, text) =
            call(&client, "ask_agent", serde_json::json!({"to": "dev-b", "text": "?", "timeout_secs": 1})).await;
        assert!(!ok && text.contains("check_inbox"), "{text}");
        let ask = sent.recv().await.unwrap();
        drop((turn, client, mcp));

        // The reply comes after a restart: it is still known as a reply, not a new message.
        let (mcp, _sent) = start(&dir).await;
        let reply = ask.reply(persona("dev-b"), "late answer").unwrap();
        assert!(mcp.claim(&reply).await.unwrap());
        assert!(mcp.claim(&reply).await.is_ok_and(|claimed| !claimed), "kept once");
        drop(mcp);

        let (mcp, _sent) = start(&dir).await;
        let (url, token) = http_server(&mcp, &current.thread_id);
        let client = http_client(&url, &token).await;
        let (ok, text) = call(&client, "check_inbox", serde_json::json!({})).await;
        assert!(ok && text.contains("From dev-b") && text.contains("late answer"), "{text}");
        let (_, text) = call(&client, "check_inbox", serde_json::json!({"all_threads": true})).await;
        assert_eq!(text, "No messages.");
    }

    #[tokio::test]
    async fn the_socket_serves_the_tools_after_the_token() {
        let (mcp, mut sent) = start(&home("sock")).await;
        let current = from_human();
        let _turn = mcp.begin(&current);
        let [McpServer::Stdio(server)] = &mcp.servers()(&current.thread_id, false)[..] else { panic!("stdio") };
        assert_eq!(server.args[..2], ["mcp", "--socket"]);
        let token = &server.env.iter().find(|e| e.name == TOKEN_ENV).unwrap().value;

        let mut stream = UnixStream::connect(&server.args[2]).await.unwrap();
        stream.write_all(format!("{token}\n").as_bytes()).await.unwrap();
        let client = ClientConfig::default().serve(stream).await.unwrap();
        let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "human", "text": "done"})).await;
        assert!(ok, "{text}");
        assert!(sent.recv().await.unwrap().to.is_human());

        let mut stranger = UnixStream::connect(&server.args[2]).await.unwrap();
        stranger.write_all(b"nope\n").await.unwrap();
        assert!(ClientConfig::default().serve(stranger).await.is_err());
    }

    /// Fake harnesses that note the MCP servers each session gets and, on a prompt, send the prompt's text to dev-b
    /// through the HTTP one.
    fn fake_pool(mcp: &Mcp, http: bool, seen: Arc<StdMutex<Vec<Vec<McpServer>>>>) -> Pool {
        let connect = Arc::new(move || {
            let (on_new, on_load, on_prompt) = (seen.clone(), seen.clone(), seen.clone());
            DynConnectTo::new(
                Agent
                    .v1()
                    .name("fake")
                    .on_receive_request(
                        async move |init: InitializeRequest, responder, _cx| {
                            let caps = AgentCapabilities::new()
                                .load_session(true)
                                .mcp_capabilities(McpCapabilities::new().http(http));
                            responder.respond(InitializeResponse::new(init.protocol_version).agent_capabilities(caps))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |new: NewSessionRequest, responder, _cx| {
                            let mut seen = on_new.lock().unwrap();
                            seen.push(new.mcp_servers);
                            responder.respond(NewSessionResponse::new(format!("s{}", seen.len())))
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |load: LoadSessionRequest, responder, _cx| {
                            on_load.lock().unwrap().push(load.mcp_servers);
                            responder.respond(LoadSessionResponse::new())
                        },
                        agent_client_protocol::on_receive_request!(),
                    )
                    .on_receive_request(
                        async move |prompt: PromptRequest, responder, cx| {
                            let AcpContent::Text(t) = &prompt.prompt[0] else { unreachable!() };
                            let server = on_prompt.lock().unwrap().last().cloned().unwrap_or_default();
                            let answer = match server.first() {
                                Some(McpServer::Http(server)) => {
                                    let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
                                    let client = http_client(&server.url, token).await;
                                    let args = serde_json::json!({"to": "dev-b", "text": t.text});
                                    call(&client, "send_message", args).await.1
                                }
                                _ => "no tools".to_owned(),
                            };
                            cx.send_notification(SessionNotification::new(
                                prompt.session_id,
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(AcpContent::Text(
                                    TextContent::new(answer),
                                ))),
                            ))?;
                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        },
                        agent_client_protocol::on_receive_request!(),
                    ),
            )
        });
        let launch = Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_mcp(mcp.servers());
        Pool::new(launch, SessionStore::memory(), 1, Duration::from_secs(60))
    }

    #[tokio::test]
    async fn sessions_get_the_tools_of_their_thread() {
        let (mcp, mut sent) = start(&home("pool")).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let pool = fake_pool(&mcp, true, seen.clone());
        let (one, two) = (from_human(), from_human());

        for envelope in [&one, &two, &one] {
            let _turn = mcp.begin(envelope);
            let answer = pool.prompt(&envelope.thread_id, &envelope.text).await.unwrap();
            assert!(answer.starts_with("Sent to dev-b"), "{answer}");
            assert_eq!(sent.recv().await.unwrap().thread_id, envelope.thread_id);
        }
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3, "new, new, then load with the cap of one");
        let token = |servers: &[McpServer]| match servers {
            [McpServer::Http(http)] => http.headers[0].value.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(token(&seen[0]), token(&seen[2]), "a thread keeps its token across a load");
        assert_ne!(token(&seen[0]), token(&seen[1]));
    }

    #[tokio::test]
    async fn harnesses_without_http_get_the_stdio_bridge() {
        let (mcp, _sent) = start(&home("stdio")).await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let pool = fake_pool(&mcp, false, seen.clone());
        let current = from_human();
        assert_eq!(pool.prompt(&current.thread_id, "x").await.unwrap(), "no tools");
        let seen = seen.lock().unwrap();
        let [McpServer::Stdio(stdio)] = &seen[0][..] else { panic!("{:?}", seen[0]) };
        assert_eq!(stdio.command, std::env::current_exe().unwrap());
    }
}
