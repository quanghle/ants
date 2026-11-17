//! A live harness holding one ACP session, the prompts run through it and the one-off uses of a harness.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{ForkSessionRequest, InitializeRequest, SessionId};
use agent_client_protocol::{Agent, Client};
use ants_proto::{HarnessAgent, StatusUpdate};
use anyhow::{Context as _, Result, anyhow, bail};
use tokio::sync::{mpsc, oneshot};

use super::connection::serve;
use super::sign_in::HANDSHAKE_TIMEOUT;
use super::{Launch, SignedOut};
use crate::files::Attached;
use crate::limits::breaker::StartFailed;
use crate::provider::brief::Briefing;
use crate::stream::history::{History, Rebuild};
use crate::sync::MutexExt as _;

/// Wall-clock time a prompt may run by default.
pub const DEFAULT_PROMPT_TIMEOUT: Duration = Duration::from_mins(30);
/// Tool calls a prompt may start by default.
pub const DEFAULT_MAX_TOOL_CALLS: usize = 300;

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

/// Spend a harness reports, for the caps in [`crate::limits::caps`].
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
    /// The thread's history, read only when the prompt runs in a session that lost the thread's context.
    pub history: Option<History>,
    /// The files attached to the message prompted with, read as the prompt runs: as images after the text for a
    /// harness taking them, else named at the end of the text ([`Attached::show`]).
    pub attachments: Option<Attached>,
    /// Steers the prompt with more messages while it runs, should the harness take them ([`Steering`]).
    pub steering: Option<Steering>,
    /// Asks the ant's provider for its brief, only when the prompt is the first of a session that started, was loaded
    /// or was rebuilt ([`Briefing`]).
    pub brief: Option<Briefing>,
}

/// Messages added to a running prompt, for a harness that takes them into its turn (`_ants/steer`).
pub struct Steering {
    /// Fires as the prompt starts in a session whose harness takes steers; with none, `steers` is never read.
    pub ready: oneshot::Sender<()>,
    pub steers: mpsc::Receiver<Steer>,
}

/// A message to add to the running prompt, and where to answer whether the prompt took it: refused once the prompt
/// ends or is cancelled, as when the harness turns it away.
pub struct Steer {
    pub text: String,
    pub accepted: oneshot::Sender<bool>,
}

impl Prompt {
    /// `text` under the default limits.
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            limits: PromptLimits::default(),
            spend: None,
            stop: None,
            progress: None,
            history: None,
            attachments: None,
            steering: None,
            brief: None,
        }
    }
}

/// What a prompt came back with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub text: String,
    /// Why the prompt was cancelled, if it was.
    pub cut: Option<String>,
}

pub(super) struct Turn {
    pub(super) prompt: Prompt,
    pub(super) reply: oneshot::Sender<Result<Reply>>,
}

/// A live harness holding one ACP session. Dropping it stops the harness once a running prompt is done.
pub struct Session {
    /// A prompt that outgrows the model's context window moves the harness to a new session.
    id: Arc<Mutex<SessionId>>,
    turns: mpsc::Sender<Turn>,
    task: tokio::task::JoinHandle<()>,
    agent: Option<HarnessAgent>,
}

impl Session {
    /// Start a harness and open a session in it: `resume` through session/load when the harness can, else a new one.
    /// A failure other than for want of signing in or a provider limit is a [`StartFailed`] one.
    pub async fn start(launch: &Launch, resume: Option<SessionId>) -> Result<Self> {
        Self::open(launch, resume, None).await
    }

    /// Start a harness in a new session whose first prompt is seeded with the thread's history for `why`.
    pub async fn start_over(launch: &Launch, why: Rebuild) -> Result<Self> {
        Self::open(launch, None, Some(why)).await
    }

    async fn open(launch: &Launch, resume: Option<SessionId>, rebuild: Option<Rebuild>) -> Result<Self> {
        let (opened_tx, opened) = oneshot::channel();
        let (turns, turns_rx) = mpsc::channel(1);
        let id = Arc::new(Mutex::new(SessionId::new("")));
        let task = tokio::spawn(serve(launch.clone(), resume, rebuild, id.clone(), opened_tx, turns_rx));
        // Given up on before the session opens (a timed-out probe), the harness stops.
        let mut abort = AbortOnDrop(Some(task.abort_handle()));
        let opened =
            opened.await.unwrap_or_else(|_| Err(anyhow!("{} exited before opening a session", launch.harness)));
        abort.0 = None;
        let agent = opened.map_err(|e| {
            if e.is::<SignedOut>() || e.is::<crate::limits::ratelimit::Limited>() {
                return e;
            }
            StartFailed { harness: launch.harness.name.clone(), detail: format!("{e:#}") }.into()
        })?;
        Ok(Self { id, turns, task, agent })
    }

    /// The agent the harness runs, as it named itself in `initialize`; None when it did not.
    pub fn agent(&self) -> Option<&HarnessAgent> {
        self.agent.as_ref()
    }

    /// The session the harness holds now.
    pub fn id(&self) -> SessionId {
        self.id.locked().clone()
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

/// Aborts a task when dropped, unless emptied first.
struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// Check that the harness of `launch` starts as it does for a thread, with the ant's settings but no MCP servers: start
/// it, open a new session within the handshake's time and stop it. Prompts nothing.
pub async fn probe(launch: &Launch) -> Result<()> {
    let started = tokio::time::timeout(HANDSHAKE_TIMEOUT, Session::start(launch, None)).await;
    let session = started.map_err(|_| StartFailed {
        harness: launch.harness.name.clone(),
        detail: format!("no session within {HANDSHAKE_TIMEOUT:?}"),
    })??;
    session.close().await;
    Ok(())
}

/// Run one prompt in a fresh session, and return the agent's reply text.
pub async fn prompt_once(launch: &Launch, text: &str) -> Result<String> {
    let session = Session::start(launch, None).await?;
    let reply = session.prompt(text).await;
    session.close().await;
    reply
}

/// Fork `session` with ACP's session/fork into a new session working in the launch's directory, with no MCP servers
/// (a later session/load offers them): the new session's id, or none when the harness does not advertise forking.
pub async fn fork(launch: &Launch, session: &SessionId) -> Result<Option<SessionId>> {
    let connect = Client.v1().name("ants").connect_with((launch.connect)(), async |cx: ConnectionTo<Agent>| {
        let init = cx.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
        if init.agent_capabilities.session_capabilities.fork.is_none() {
            return Ok(None);
        }
        let fork = ForkSessionRequest::new(session.clone(), launch.cwd.clone());
        Ok(Some(cx.send_request(fork).block_task().await?.session_id))
    });
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, connect).await {
        Ok(forked) => forked.with_context(|| format!("{} session/fork of {session}", launch.harness)),
        Err(_) => bail!("{}: no session/fork answer within {HANDSHAKE_TIMEOUT:?}", launch.harness),
    }
}
