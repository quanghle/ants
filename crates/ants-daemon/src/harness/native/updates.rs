//! Where a native prompt's session updates go ([`Updates`]), and how what its CLI would ask a human about is answered
//! ([`approve`]).

use agent_client_protocol::Client;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    ContentBlock, ContentChunk, Cost, PermissionOption, PermissionOptionKind, Plan, RequestPermissionOutcome,
    RequestPermissionRequest, SessionId, SessionNotification, SessionUpdate, TextContent, ToolCall, ToolCallUpdate,
    UsageUpdate,
};

use super::Cli;

/// Where a prompt's session updates go: the daemon's client, which streams them as status and reads the reply text,
/// tool calls and cost from them.
#[derive(Clone)]
pub(crate) struct Updates {
    cx: ConnectionTo<Client>,
    session: SessionId,
}

impl Updates {
    /// The updates of a prompt of `session`, going to the client of `cx`.
    pub(super) fn new(cx: ConnectionTo<Client>, session: SessionId) -> Self {
        Self { cx, session }
    }

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

    /// Ask the client whether the tool call `call` describes may run, offering allow once and reject: true only when
    /// it allows. The answer comes from the ant's allow rules, else its approvers, so callers await it off the loop
    /// reading the CLI.
    pub(crate) async fn ask(&self, call: ToolCallUpdate) -> bool {
        let options = vec![
            PermissionOption::new(ALLOW, "Allow", PermissionOptionKind::AllowOnce),
            PermissionOption::new(REJECT, "Reject", PermissionOptionKind::RejectOnce),
        ];
        let request = RequestPermissionRequest::new(self.session.clone(), call, options);
        match self.cx.send_request(request).block_task().await {
            Ok(response) => matches!(
                response.outcome,
                RequestPermissionOutcome::Selected(selected) if selected.option_id.0.as_ref() == ALLOW
            ),
            Err(e) => {
                tracing::debug!(error = %e, "permission request");
                false
            }
        }
    }
}

/// The option ids of a permission request sent for a native CLI ([`Updates::ask`]).
const ALLOW: &str = "allow";
const REJECT: &str = "reject";

/// Whether the tool call `call` describes, which the CLI asks to run, may: asked of the client through `updates`, the
/// running prompt's, when the ant asks ants about its tools ([`Cli::asks`]), else denied ([`deny_approval`]).
/// `request` describes it for the log.
pub(crate) async fn approve(cli: &Cli, updates: Option<Updates>, call: ToolCallUpdate, request: &str) -> bool {
    match updates.filter(|_| cli.asks()) {
        Some(updates) => updates.ask(call).await,
        None => {
            deny_approval(cli, request);
            false
        }
    }
}

/// Log that a native approval request, `request` describing what the CLI asked to do, is denied; the caller answers
/// the CLI with its protocol's denial. Unless the ant asks ants about its tools ([`approve`]), no tool request of a
/// native protocol reaches `ants.approvals`: the definition's native permission settings decide what runs unasked,
/// and what the CLI would ask a human about is denied at once rather than left waiting.
pub(crate) fn deny_approval(cli: &Cli, request: &str) {
    tracing::warn!(
        harness = cli.name(),
        request,
        "denied a tool request of the harness: its definition's native permission settings decide which tools run"
    );
}
