//! The task owning a harness connection: it opens the session, answers permission requests, and runs prompts under
//! their limits, cancelling one that reaches a limit.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    AuthMethod, CancelNotification, CloseSessionRequest, ContentBlock, ImageContent, InitializeRequest, McpServer,
    PromptRequest, PromptResponse, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SessionId, SessionNotification, TextContent,
};
use agent_client_protocol::{Agent, Client};
use ants_proto::provider::BriefReason;
use ants_proto::{HarnessAgent, StatusUpdate};
use anyhow::Result;
use data_encoding::BASE64;
use futures::FutureExt as _;
use futures::future::FusedFuture as _;
use futures::stream::{FuturesUnordered, StreamExt as _};
use tokio::sync::{Notify, mpsc, oneshot, watch};

use super::configure::{new_session, open};
use super::observe::{Current, Observed};
use super::session::Turn;
use super::sign_in::harness_error;
use super::steer::{self, SteerRequest};
use super::{Launch, Prompt, PromptLimits, Reply, Spend, Steer};
use crate::files::Attached;
use crate::limits::overflow::{ContextOverflow, overflows};
use crate::provider::brief::Briefing;
use crate::stream::history::{History, Rebuild};
use crate::sync::MutexExt as _;

/// How long a harness has to end a cancelled prompt before it is stopped.
const CANCEL_GRACE: Duration = Duration::from_secs(30);
/// How long a harness has to answer the steers sent to a prompt that ended.
const STEER_GRACE: Duration = Duration::from_secs(10);

/// Own the harness connection: open the session, then run turns until the session is dropped or a prompt fails.
pub(super) async fn serve(
    launch: Launch,
    resume: Option<SessionId>,
    rebuild: Option<Rebuild>,
    current: Arc<Mutex<SessionId>>,
    opened: oneshot::Sender<Result<Option<HarnessAgent>>>,
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
                sink.locked().observe(n);
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |mut request: RequestPermissionRequest, responder, cx| {
                known.locked().describe(&mut request.tool_call);
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
            methods.locked().clone_from(&init.auth_methods);
            let caps = &init.agent_capabilities;
            let mcp = launch.mcp_servers(caps.mcp_capabilities.http);
            let mut id = open(&cx, &launch, resume.clone(), caps.load_session, mcp.clone()).await?;
            *current.locked() = id.clone();
            let resumed = resume.is_some();
            if let Some(tx) = opened.take() {
                let agent = init
                    .agent_info
                    .as_ref()
                    .map(|info| HarnessAgent { name: info.name.clone(), version: info.version.clone() });
                let _ = tx.send(Ok(agent));
            }
            let mut lost = resume
                .filter(|old| *old != id)
                .map(|old| {
                    tracing::warn!(lost = %old, session = %id, "the session was not resumed; rebuilding its context");
                    Rebuild::Lost
                })
                .or(rebuild);
            // Why the session opened, which the first prompt's brief tells the provider.
            let mut briefing = Some(match &lost {
                Some(_) => BriefReason::Rebuilt,
                None if resumed => BriefReason::Loaded,
                None => BriefReason::Started,
            });
            let connected = Connected {
                cx: &cx,
                launch: &launch,
                observed: &observed,
                cancelled: &cancelled,
                methods: &methods,
                current: &current,
                mcp: &mcp,
                images: caps.prompt_capabilities.image,
                close: caps.session_capabilities.close.is_some(),
                steers: steer::advertised(caps),
            };
            while let Some(turn) = turns.recv().await {
                if connected.run(turn, &mut id, lost.take(), briefing.take()).await?.is_break() {
                    return Ok(());
                }
            }
            Ok(())
        })
        .await;
    if let Err(e) = result {
        match opened {
            Some(tx) => {
                let _ = tx.send(Err(harness_error(&launch, "session", &e, &methods.locked())));
            }
            None => tracing::warn!(error = %e, "{} session ended", launch.harness),
        }
    }
}

/// The lines ending a prompt's text for its `attachments`, and the image blocks following the text for a harness
/// taking `images`.
async fn attached(attachments: Option<Attached>, images: bool) -> (String, Vec<ContentBlock>) {
    let Some(attachments) = attachments else { return Default::default() };
    let shown = attachments.show(images).await;
    let images = shown.images.into_iter().map(|(mime, bytes)| ImageContent::new(BASE64.encode(&bytes), mime));
    (shown.lines, images.map(ContentBlock::Image).collect())
}

/// What [`serve`] runs prompts with once the harness is connected.
struct Connected<'a> {
    cx: &'a ConnectionTo<Agent>,
    launch: &'a Launch,
    observed: &'a Mutex<Observed>,
    /// Bumped when a prompt is cancelled.
    cancelled: &'a watch::Sender<u64>,
    methods: &'a Mutex<Vec<AuthMethod>>,
    /// The session's id, as the session's owner reads it.
    current: &'a Mutex<SessionId>,
    mcp: &'a Vec<McpServer>,
    /// Whether the agent takes image content in prompts.
    images: bool,
    /// Whether the agent closes sessions (`session/close`), which a native protocol's translator does.
    close: bool,
    /// Whether the agent takes steers into a running prompt (`_ants/steer`).
    steers: bool,
}

/// How a prompt ended: its response (None when a cancelled prompt did not end within the grace period), why it was
/// cancelled, and the reply text the harness streamed, with whether all of it read as errors or notices.
struct Ended {
    done: Option<Result<PromptResponse, agent_client_protocol::Error>>,
    cut: Option<String>,
    text: String,
    errors_only: bool,
}

impl Connected<'_> {
    /// Run `turn` in session `id`, or in a new one should the prompt outgrow the context window, and send its reply;
    /// the first turn of a session gets the ant's brief for why it `opened`. Breaks when the harness is to stop; fails
    /// with the error that ends the connection.
    async fn run(
        &self,
        turn: Turn,
        id: &mut SessionId,
        mut rebuild: Option<Rebuild>,
        opened: Option<BriefReason>,
    ) -> Result<ControlFlow<()>, agent_client_protocol::Error> {
        let Turn { prompt, reply } = turn;
        let Prompt { text, limits, spend, mut stop, progress, mut history, attachments, steering, brief } = prompt;
        let ((lines, images), mut briefed) =
            tokio::join!(attached(attachments, self.images), self.brief(brief.as_ref(), opened));
        // Without steering by the agent, the steering is dropped unready: steers stop the prompt instead.
        let mut steers = steering.filter(|_| self.steers).map(|steering| {
            let _ = steering.ready.send(());
            steering.steers
        });
        // Once in a new session when the prompt outgrows the context window, unless it ran in one already.
        let mut retry = rebuild.is_none();
        let ended = loop {
            let too_many = self.observe(id, limits, spend.as_ref(), progress.as_ref());
            let text = self.compose(rebuild, &briefed, &mut history, &text, &lines).await;
            let content = std::iter::once(ContentBlock::Text(TextContent::new(text))).chain(images.iter().cloned());
            let request = self.cx.send_request(PromptRequest::new(id.clone(), content.collect())).block_task();
            let bounds = Bounds { limits, too_many: &too_many, stop: &mut stop, steers: &mut steers };
            let (done, cut) = bounded(self.cx, id, request, bounds, self.cancelled).await;
            let ended = self.ended(done, cut);
            let overflowed = |e: &agent_client_protocol::Error| overflows(&self.launch.harness, &e.to_string());
            match &ended.done {
                Some(Err(e)) if ended.cut.is_none() && retry && overflowed(e) => {
                    tracing::warn!(session = %id, error = %e, "the prompt outgrew the context window");
                    if let Err(e) = self.renew(id).await {
                        let methods = self.methods.locked();
                        let _ = reply.send(Err(harness_error(self.launch, "session", &e, &methods)));
                        return Err(e);
                    }
                    (rebuild, retry) = (Some(Rebuild::Overflow), false);
                    briefed = self.brief(brief.as_ref(), Some(BriefReason::Rebuilt)).await;
                }
                Some(Err(e)) if ended.cut.is_none() && overflowed(e) => {
                    let harness = self.launch.harness.name.clone();
                    let _ = reply.send(Err(ContextOverflow { harness, detail: e.to_string() }.into()));
                    return Err(e.clone());
                }
                _ => break ended,
            }
        };
        self.reply(id, ended, spend.as_ref(), reply)
    }

    /// Start observing a prompt in session `id`, reporting a cost left from the last one first. Returns what notifies
    /// the prompt having started too many tool calls.
    fn observe(
        &self,
        id: &SessionId,
        limits: PromptLimits,
        spend: Option<&mpsc::UnboundedSender<Spend>>,
        progress: Option<&mpsc::Sender<StatusUpdate>>,
    ) -> Arc<Notify> {
        let too_many = Arc::new(Notify::new());
        let mut observed = self.observed.locked();
        // A cost reported after the last prompt ended counts now.
        if let (Some(tx), Some(total)) = (spend, observed.cost) {
            let _ = tx.send(Spend::Cost { session: id.clone(), total });
        }
        observed.current = Some(Current {
            text: String::new(),
            errors_only: true,
            notices: 0,
            tool_calls: HashMap::new(),
            max_tool_calls: limits.max_tool_calls,
            too_many: too_many.clone(),
            spend: spend.cloned(),
            progress: progress.cloned(),
        });
        too_many
    }

    /// The part of a prompt `brief` adds when the prompt is the first of a session that opened for `reason`.
    async fn brief(&self, brief: Option<&Briefing>, reason: Option<BriefReason>) -> String {
        match (brief, reason) {
            (Some(brief), Some(reason)) => {
                let model = self.launch.settings.model.as_deref();
                brief.section(reason, &self.launch.harness.name, model).await
            }
            _ => String::new(),
        }
    }

    /// The prompt's text: the instructions, the ant's brief when `briefed`, the thread's history when the session is a
    /// `rebuild`, the message's `text` and the `lines` naming its attachments.
    async fn compose(
        &self,
        rebuild: Option<Rebuild>,
        briefed: &str,
        history: &mut Option<History>,
        text: &str,
        lines: &str,
    ) -> String {
        let seed = match rebuild {
            Some(why) => {
                let budget = self.launch.harness.history_budget(&self.launch.settings);
                crate::stream::history::seed(why, history.take(), budget).await
            }
            None => String::new(),
        };
        [self.launch.instructions.as_str(), briefed, &seed, text, lines]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    /// Stop observing the prompt that ended with `done` and `cut`, taking the text it streamed.
    fn ended(&self, done: Option<Result<PromptResponse, agent_client_protocol::Error>>, cut: Option<String>) -> Ended {
        let (text, errors_only) = self.observed.locked().current.take().map_or_else(Default::default, |c| {
            let text =
                if c.errors_only { c.text } else { crate::limits::ratelimit::strip_notices(&c.text, c.notices).into() };
            (text, c.errors_only)
        });
        Ended { done, cut, text, errors_only }
    }

    /// Move from session `id`, whose context window the prompt outgrew, to a new one.
    async fn renew(&self, id: &mut SessionId) -> Result<(), agent_client_protocol::Error> {
        // The thread leaves the session for good: what the agent runs for it can stop.
        if self.close
            && let Err(e) = self.cx.send_request(CloseSessionRequest::new(id.clone())).block_task().await
        {
            tracing::warn!(session = %id, error = %e, "session/close");
        }
        *id = new_session(self.cx, self.launch, self.mcp.clone()).await?;
        *self.current.locked() = id.clone();
        self.observed.locked().cost = None;
        Ok(())
    }

    /// Send the reply to the prompt that `ended` in session `id`. Breaks when the harness is to stop; fails with the
    /// error that ends the connection.
    fn reply(
        &self,
        id: &SessionId,
        ended: Ended,
        spend: Option<&mpsc::UnboundedSender<Spend>>,
        reply: oneshot::Sender<Result<Reply>>,
    ) -> Result<ControlFlow<()>, agent_client_protocol::Error> {
        let Ended { done, cut, text, errors_only } = ended;
        match done {
            Some(Ok(done)) => {
                tracing::debug!(session = %id, stop_reason = ?done.stop_reason, "prompt finished");
                if let (Some(tx), Some(usage)) = (spend, &done.usage) {
                    let _ = tx.send(Spend::Tokens(usage.total_tokens));
                }
                // Copilot CLI and Antigravity end a prompt their provider turned away as a reply of error
                // lines, which is no answer.
                let limited = (errors_only && cut.is_none())
                    .then(|| crate::limits::ratelimit::classify(&self.launch.harness, &text))
                    .flatten();
                let _ = reply.send(match limited {
                    Some(limited) => Err(limited.into()),
                    None => Ok(Reply { text, cut }),
                });
            }
            Some(Err(e)) if cut.is_none() => {
                let error = harness_error(self.launch, "prompt", &e, &self.methods.locked());
                let _ = reply.send(Err(error));
                return Err(e);
            }
            // Cancelled, but the harness failed or did not end the prompt: stop it, the prompt is over.
            _ => {
                tracing::warn!(session = %id, "{} did not end a cancelled prompt; stopping it", self.launch.harness);
                let _ = reply.send(Ok(Reply { text, cut }));
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }
}

/// What bounds a running prompt: its limits, what notifies it started too many tool calls, the human's cancel, and
/// the steers to send it.
struct Bounds<'a> {
    limits: PromptLimits,
    too_many: &'a Notify,
    stop: &'a mut Option<oneshot::Receiver<String>>,
    steers: &'a mut Option<mpsc::Receiver<Steer>>,
}

/// Wait for a prompt's response, cancelling the prompt with `session/cancel` when it reaches a limit or `stop` fires,
/// and sending it the steers that arrive meanwhile with `_ants/steer`. Returns the response, None when a cancelled
/// prompt did not end within the grace period, and why it was cancelled.
async fn bounded(
    cx: &ConnectionTo<Agent>,
    id: &SessionId,
    request: impl Future<Output = Result<PromptResponse, agent_client_protocol::Error>>,
    bounds: Bounds<'_>,
    cancelled: &watch::Sender<u64>,
) -> (Option<Result<PromptResponse, agent_client_protocol::Error>>, Option<String>) {
    let Bounds { limits, too_many, stop, steers } = bounds;
    tokio::pin!(request);
    let timeout = async {
        match limits.timeout {
            Some(t) => tokio::time::sleep(t).await,
            None => std::future::pending().await,
        }
    };
    let stopped = async {
        let reason = match stop.as_mut() {
            Some(rx) => rx.await.ok(),
            None => None,
        };
        // Spent: a prompt retried in a new session must not wait on it again.
        *stop = None;
        reason
    }
    .fuse();
    let too_many = too_many.notified();
    tokio::pin!(timeout, stopped, too_many);
    let mut sending = FuturesUnordered::new();
    let cut = loop {
        tokio::select! {
            biased;
            done = &mut request => {
                // A steer the agent took before the prompt ended is answered so.
                let _ = tokio::time::timeout(STEER_GRACE, async { while sending.next().await.is_some() {} }).await;
                return (Some(done), None);
            }
            () = &mut timeout => break format!("Prompt time limit reached ({:?})", limits.timeout.unwrap_or_default()),
            () = &mut too_many => break format!(
                "Tool call limit reached ({} tool calls in one prompt)",
                limits.max_tool_calls.unwrap_or_default()
            ),
            reason = &mut stopped, if !stopped.is_terminated() => if let Some(reason) = reason {
                break reason;
            },
            Some(steer) = next_steer(steers) => sending.push(send_steer(cx, id, steer)),
            Some(()) = sending.next(), if !sending.is_empty() => {}
        }
    };
    tracing::warn!(session = %id, reason = %cut, "cancelling the prompt");
    if let Some(steers) = steers.as_mut() {
        steers.close();
    }
    if let Err(e) = cx.send_notification(CancelNotification::new(id.clone())) {
        tracing::warn!(error = %e, "session/cancel");
    }
    cancelled.send_modify(|n| *n += 1);
    (tokio::time::timeout(CANCEL_GRACE, request).await.ok(), Some(cut))
}

/// The next steer for the running prompt, if it takes any.
async fn next_steer(steers: &mut Option<mpsc::Receiver<Steer>>) -> Option<Steer> {
    match steers {
        Some(steers) => steers.recv().await,
        None => std::future::pending().await,
    }
}

/// Send `steer` to the prompt running in session `id`, answering whether the agent took it.
async fn send_steer(cx: &ConnectionTo<Agent>, id: &SessionId, steer: Steer) {
    let Steer { text, accepted } = steer;
    let prompt = vec![ContentBlock::Text(TextContent::new(text))];
    let took = match cx.send_request(SteerRequest { session_id: id.clone(), prompt }).block_task().await {
        Ok(response) => response.accepted,
        Err(e) => {
            tracing::warn!(session = %id, error = %e, "_ants/steer");
            false
        }
    };
    tracing::info!(session = %id, took, "steered the running prompt");
    let _ = accepted.send(took);
}
