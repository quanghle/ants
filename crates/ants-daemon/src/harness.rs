//! ACP client side: start harnesses over stdio, keep their sessions, and run prompts through them.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    ContentBlock, InitializeRequest, LoadSessionRequest, McpServer, NewSessionRequest, PromptRequest,
    RequestPermissionRequest, SessionId, SessionNotification, SessionUpdate, TextContent,
};
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, Client, DynConnectTo};
use ants_proto::ThreadId;
use anyhow::{Result, anyhow};
use tokio::sync::{mpsc, oneshot};

use crate::permission::PermissionPolicy;

/// An agent harness ants can drive over ACP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HarnessKind {
    /// GitHub Copilot CLI, `copilot --acp`. Uses the sign-in from `copilot login`.
    #[default]
    Copilot,
}

impl HarnessKind {
    pub const ALL: [HarnessKind; 1] = [HarnessKind::Copilot];

    pub fn name(self) -> &'static str {
        match self {
            HarnessKind::Copilot => "copilot",
        }
    }

    fn command(self) -> (&'static str, &'static [&'static str]) {
        match self {
            HarnessKind::Copilot => ("copilot", &["--acp"]),
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
        Self::ALL.into_iter().find(|h| h.name() == s).ok_or_else(|| anyhow!("unknown harness {s:?}"))
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
        Self { name: kind.name().to_owned(), connect, cwd, policy, thread: None, mcp: None }
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
        Self { name: "test".to_owned(), connect, cwd, policy, thread: None, mcp: None }
    }
}

impl fmt::Debug for Launch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launch")
            .field("name", &self.name)
            .field("cwd", &self.cwd)
            .field("policy", &self.policy)
            .field("thread", &self.thread)
            .finish()
    }
}

struct Turn {
    text: String,
    reply: oneshot::Sender<Result<String>>,
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

    /// Run one prompt and return the agent's reply text.
    pub async fn prompt(&self, text: &str) -> Result<String> {
        let (reply, answer) = oneshot::channel();
        self.turns.send(Turn { text: text.to_owned(), reply }).await.map_err(|_| anyhow!("harness session ended"))?;
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

/// Own the harness connection: open the session, then run turns until the session is dropped or a prompt fails.
async fn serve(
    launch: Launch,
    resume: Option<SessionId>,
    opened: oneshot::Sender<Result<SessionId>>,
    mut turns: mpsc::Receiver<Turn>,
) {
    // Some while a prompt runs. session/load replays the history as chunks; those land in None and are dropped.
    let reply: Arc<Mutex<Option<String>>> = Arc::default();
    let sink = reply.clone();
    let (policy, thread, workdir) = (launch.policy.clone(), launch.thread.clone(), launch.cwd.clone());
    let mut opened = Some(opened);
    let result = Client
        .v1()
        .name("ants")
        .on_receive_notification(
            async move |n: SessionNotification, _cx| {
                if let SessionUpdate::AgentMessageChunk(chunk) = n.update
                    && let ContentBlock::Text(t) = chunk.content
                    && let Some(buf) = sink.lock().expect("reply buffer").as_mut()
                {
                    buf.push_str(&t.text);
                }
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, cx| {
                tracing::info!(tool = ?request.tool_call, "harness asked for permission");
                let (policy, thread, workdir) = (policy.clone(), thread.clone(), workdir.clone());
                // The human may take minutes; answer off the dispatch loop so the connection keeps flowing.
                cx.spawn(async move { responder.respond(policy.answer(&request, thread.as_ref(), &workdir).await) })
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
            while let Some(turn) = turns.recv().await {
                *reply.lock().expect("reply buffer") = Some(String::new());
                let done = cx
                    .send_request(PromptRequest::new(id.clone(), vec![ContentBlock::Text(TextContent::new(turn.text))]))
                    .block_task()
                    .await;
                let text = reply.lock().expect("reply buffer").take().unwrap_or_default();
                match done {
                    Ok(done) => {
                        tracing::debug!(session = %id, stop_reason = ?done.stop_reason, "prompt finished");
                        let _ = turn.reply.send(Ok(text));
                    }
                    Err(e) => {
                        let _ = turn.reply.send(Err(anyhow!("{} prompt: {e}", launch.name)));
                        return Err(e);
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
                Ok(_) => return Ok(id),
                Err(e) => tracing::warn!(session = %id, error = %e, "session/load failed; starting a new session"),
            }
        }
    }
    Ok(cx.send_request(NewSessionRequest::new(launch.cwd.clone()).mcp_servers(mcp)).block_task().await?.session_id)
}
