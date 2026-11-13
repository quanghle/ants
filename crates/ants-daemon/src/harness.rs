//! ACP client side: start harnesses over stdio, keep their sessions, and run prompts through them.

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    CancelNotification, ContentBlock, InitializeRequest, LoadSessionRequest, McpServer, NewSessionRequest,
    PromptRequest, PromptResponse, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SessionId, SessionModeState, SessionNotification, SessionUpdate, SetSessionModeRequest, TextContent, ToolCallId,
};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, Client, DynConnectTo};
use ants_proto::ThreadId;
use anyhow::{Result, anyhow};
use tokio::sync::{Notify, mpsc, oneshot, watch};

use crate::permission::PermissionPolicy;

/// An agent harness ants can drive over ACP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HarnessKind {
    /// GitHub Copilot CLI, `copilot --acp`. Uses the sign-in from `copilot login`.
    #[default]
    Copilot,
    /// Claude Code through its ACP adapter, `claude-agent-acp` (npm `@agentclientprotocol/claude-agent-acp`). Uses
    /// the sign-in from `claude login`, or `ANTHROPIC_API_KEY`.
    Claude,
    /// Google Antigravity CLI, `agy acp`. Uses the sign-in from `agy` (or `agy acp --login`), or a Gemini API key.
    Antigravity,
}

impl HarnessKind {
    pub const ALL: [HarnessKind; 3] = [HarnessKind::Copilot, HarnessKind::Claude, HarnessKind::Antigravity];

    pub fn name(self) -> &'static str {
        match self {
            HarnessKind::Copilot => "copilot",
            HarnessKind::Claude => "claude",
            HarnessKind::Antigravity => "antigravity",
        }
    }

    fn command(self) -> (&'static str, &'static [&'static str]) {
        match self {
            HarnessKind::Copilot => ("copilot", &["--acp"]),
            HarnessKind::Claude => ("claude-agent-acp", &[]),
            HarnessKind::Antigravity => ("agy", &["acp", "--disable-telemetry"]),
        }
    }

    /// The ACP session mode every session must run in, so tool requests reach the client instead of being approved
    /// by the harness itself. Claude Code starts in its `auto` mode, where a classifier approves tools; Antigravity
    /// CLI starts in the saved `agentMode`, `auto` unless set, where edits and commands it deems safe run unasked.
    fn mode(self) -> Option<&'static str> {
        match self {
            HarnessKind::Copilot => None,
            HarnessKind::Claude | HarnessKind::Antigravity => Some("default"),
        }
    }

    /// The harness process, with `env` added to the daemon's environment.
    fn agent(self, env: &[(String, String)]) -> AcpAgent {
        let (program, args) = self.command();
        AcpAgent::new(AcpAgentConfig::new(program).args(args.iter().copied()).envs(env.iter().cloned()))
    }
}

impl fmt::Display for HarnessKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for HarnessKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::ALL.into_iter().find(|h| h.name() == s).ok_or_else(|| {
            let known: Vec<_> = Self::ALL.iter().map(|h| h.name()).collect();
            anyhow!("unknown harness {s:?}, expected one of {}", known.join(", "))
        })
    }
}

/// Start the harness, complete the ACP handshake and describe the agent.
pub async fn check(kind: HarnessKind) -> Result<String> {
    Client
        .v1()
        .name("ants")
        .connect_with(kind.agent(&[]), async |cx: ConnectionTo<Agent>| {
            let init = cx.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
            Ok(format!("{:?}", init.agent_info))
        })
        .await
        .map_err(|e| anyhow!("{kind} ACP handshake: {e}"))
}

/// Makes the transport to a fresh harness: a process, or an in-process agent in tests.
type Connector = Arc<dyn Fn() -> DynConnectTo<Client> + Send + Sync>;

/// The MCP servers a session of a thread gets; the flag tells whether the harness takes HTTP ones.
pub type McpServers = Arc<dyn Fn(&ThreadId, bool) -> Vec<McpServer> + Send + Sync>;

/// How to start a harness and open sessions in it.
#[derive(Clone)]
pub struct Launch {
    name: String,
    connect: Connector,
    /// See [`HarnessKind::mode`].
    mode: Option<&'static str>,
    cwd: PathBuf,
    policy: PermissionPolicy,
    /// The thread the sessions serve, so a permission request can be escalated on it.
    thread: Option<ThreadId>,
    mcp: Option<McpServers>,
}

impl Launch {
    /// `kind` working in `cwd`, with `env` added to the daemon's environment.
    pub fn new(kind: HarnessKind, cwd: PathBuf, env: Vec<(String, String)>, policy: PermissionPolicy) -> Self {
        let connect: Connector = Arc::new(move || DynConnectTo::new(kind.agent(&env)));
        Self { name: kind.name().to_owned(), connect, mode: kind.mode(), cwd, policy, thread: None, mcp: None }
    }

    /// The same launch, for sessions serving `thread`.
    pub fn for_thread(&self, thread: &ThreadId) -> Self {
        Self { thread: Some(thread.clone()), ..self.clone() }
    }

    /// The same launch, offering `mcp` to the sessions of each thread.
    pub fn with_mcp(self, mcp: McpServers) -> Self {
        Self { mcp: Some(mcp), ..self }
    }

    fn mcp_servers(&self, http: bool) -> Vec<McpServer> {
        match (&self.mcp, &self.thread) {
            (Some(mcp), Some(thread)) => mcp(thread, http),
            _ => Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_transport(connect: Connector, cwd: PathBuf, policy: PermissionPolicy) -> Self {
        Self { name: "test".to_owned(), connect, mode: None, cwd, policy, thread: None, mcp: None }
    }

    #[cfg(test)]
    pub(crate) fn with_mode(self, mode: &'static str) -> Self {
        Self { mode: Some(mode), ..self }
    }
}

impl fmt::Debug for Launch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launch")
            .field("name", &self.name)
            .field("mode", &self.mode)
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
}

impl Prompt {
    /// `text` under the default limits.
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into(), limits: PromptLimits::default(), spend: None, stop: None }
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
        let id = opened.await.map_err(|_| anyhow!("{} exited before opening a session", launch.name))??;
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
    tool_calls: HashSet<ToolCallId>,
    max_tool_calls: Option<usize>,
    too_many: Arc<Notify>,
    spend: Option<mpsc::UnboundedSender<Spend>>,
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
        match n.update {
            SessionUpdate::AgentMessageChunk(chunk) => {
                if let ContentBlock::Text(t) = chunk.content {
                    current.text.push_str(&t.text);
                }
            }
            SessionUpdate::ToolCall(call) => {
                current.tool_calls.insert(call.tool_call_id);
                if current.max_tool_calls.is_some_and(|max| current.tool_calls.len() > max) {
                    current.too_many.notify_one();
                }
            }
            _ => {}
        }
    }
}

/// Own the harness connection: open the session, then run turns until the session is dropped or a prompt fails.
async fn serve(
    launch: Launch,
    resume: Option<SessionId>,
    opened: oneshot::Sender<Result<SessionId>>,
    mut turns: mpsc::Receiver<Turn>,
) {
    let observed: Arc<Mutex<Observed>> = Arc::default();
    let sink = observed.clone();
    // Bumped when a prompt is cancelled, which answers the permission requests still waiting.
    let (cancelled, cancels) = watch::channel(0u64);
    let (policy, thread, workdir) = (launch.policy.clone(), launch.thread.clone(), launch.cwd.clone());
    let mut opened = Some(opened);
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
            async move |request: RequestPermissionRequest, responder, cx| {
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
            let caps = &init.agent_capabilities;
            let mcp = launch.mcp_servers(caps.mcp_capabilities.http);
            let id = open(&cx, &launch, resume, caps.load_session, mcp).await?;
            if let Some(tx) = opened.take() {
                let _ = tx.send(Ok(id.clone()));
            }
            while let Some(Turn { prompt, reply }) = turns.recv().await {
                let Prompt { text, limits, spend, stop } = prompt;
                let too_many = Arc::new(Notify::new());
                {
                    let mut observed = observed.lock().expect("observed");
                    // A cost reported after the last prompt ended counts now.
                    if let (Some(tx), Some(total)) = (&spend, observed.cost) {
                        let _ = tx.send(Spend::Cost { session: id.clone(), total });
                    }
                    observed.current = Some(Current {
                        text: String::new(),
                        tool_calls: HashSet::new(),
                        max_tool_calls: limits.max_tool_calls,
                        too_many: too_many.clone(),
                        spend: spend.clone(),
                    });
                }
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
                        let _ = reply.send(Err(anyhow!("{} prompt: {e}", launch.name)));
                        return Err(e);
                    }
                    // Cancelled, but the harness failed or did not end the prompt: stop it, the prompt is over.
                    _ => {
                        tracing::warn!(session = %id, "{} did not end a cancelled prompt; stopping it", launch.name);
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
                let _ = tx.send(Err(anyhow!("{} session: {e}", launch.name)));
            }
            None => tracing::warn!(error = %e, "{} session ended", launch.name),
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
            tracing::warn!(session = %id, "{} cannot load sessions; starting a new one", launch.name);
        } else {
            let load = LoadSessionRequest::new(id.clone(), launch.cwd.clone()).mcp_servers(mcp.clone());
            match cx.send_request(load).block_task().await {
                Ok(loaded) => {
                    set_mode(cx, launch, &id, loaded.modes).await?;
                    return Ok(id);
                }
                Err(e) => tracing::warn!(session = %id, error = %e, "session/load failed; starting a new session"),
            }
        }
    }
    let new = cx.send_request(NewSessionRequest::new(launch.cwd.clone()).mcp_servers(mcp)).block_task().await?;
    set_mode(cx, launch, &new.session_id, new.modes).await?;
    Ok(new.session_id)
}

/// Put the session in the launch's mode, if it has one; a harness that does not offer it fails the session.
async fn set_mode(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    id: &SessionId,
    modes: Option<SessionModeState>,
) -> Result<(), agent_client_protocol::Error> {
    let Some(want) = launch.mode else { return Ok(()) };
    let offered = modes.as_ref().is_some_and(|m| m.available_modes.iter().any(|mode| &*mode.id.0 == want));
    if !offered {
        return Err(agent_client_protocol::Error::internal_error()
            .data(format!("{} does not offer session mode {want}", launch.name)));
    }
    if modes.is_some_and(|m| &*m.current_mode_id.0 == want) {
        return Ok(());
    }
    cx.send_request(SetSessionModeRequest::new(id.clone(), want)).block_task().await?;
    tracing::debug!(session = %id, mode = want, "session mode set");
    Ok(())
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::v1::schema::{
        InitializeResponse, NewSessionResponse, SessionMode, SetSessionModeResponse,
    };

    use super::*;

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
        (Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_mode("default"), set)
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
}
