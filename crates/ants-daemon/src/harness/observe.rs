//! What a session's notifications report while a prompt runs: its reply text, tool calls, spend and progress.

use std::collections::HashMap;
use std::sync::Arc;

use agent_client_protocol::v1::schema::{
    ContentBlock, SessionNotification, SessionUpdate, ToolCall, ToolCallId, ToolCallLocation, ToolCallUpdate,
};
use ants_proto::{MAX_TOOL_PATHS, PlanStep, StatusUpdate};
use tokio::sync::{Notify, mpsc};

use super::Spend;

/// The prompt in flight, as the session's notifications fill it in.
pub(super) struct Current {
    pub(super) text: String,
    /// Whether every piece of the text so far reads as the harness's error or notice
    /// ([`crate::limits::ratelimit::is_error_chunk`]).
    pub(super) errors_only: bool,
    /// Where the last of the pieces read as errors or notices ends in the text, while no other piece came: the end of
    /// the retry notices a harness may stream before the answer ([`crate::limits::ratelimit::strip_notices`]).
    pub(super) notices: usize,
    /// What the session reported of each tool call, without its content or output.
    pub(super) tool_calls: HashMap<ToolCallId, ToolCall>,
    pub(super) max_tool_calls: Option<usize>,
    pub(super) too_many: Arc<Notify>,
    pub(super) spend: Option<mpsc::UnboundedSender<Spend>>,
    pub(super) progress: Option<mpsc::Sender<StatusUpdate>>,
}

#[derive(Default)]
pub(super) struct Observed {
    /// Some while a prompt runs. session/load replays the history as notifications; those find None and are dropped.
    pub(super) current: Option<Current>,
    /// The session's last cumulative cost.
    pub(super) cost: Option<f64>,
}

impl Observed {
    pub(super) fn observe(&mut self, n: SessionNotification) {
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
        if let Some(tx) = &current.progress
            && let Some(update) = progress(&n.update)
        {
            // Progress is a hint: one the prompt is too far behind to take is dropped.
            let _ = tx.try_send(update);
        }
        match n.update {
            SessionUpdate::AgentMessageChunk(chunk) => {
                if let ContentBlock::Text(t) = chunk.content {
                    let error = crate::limits::ratelimit::is_error_chunk(&t.text);
                    if !t.text.trim().is_empty() && !error {
                        current.errors_only = false;
                    }
                    current.text.push_str(&t.text);
                    if current.errors_only && error {
                        current.notices = current.text.len();
                    }
                }
            }
            SessionUpdate::ToolCall(mut call) => {
                strip(&mut call);
                current.tool_calls.insert(call.tool_call_id.clone(), call);
                if current.max_tool_calls.is_some_and(|max| current.tool_calls.len() > max) {
                    current.too_many.notify_one();
                }
            }
            SessionUpdate::ToolCallUpdate(update) => {
                if let Some(call) = current.tool_calls.get_mut(&update.tool_call_id) {
                    call.update(update.fields);
                    strip(call);
                }
            }
            _ => {}
        }
    }

    /// Fill in what a permission request leaves out of its tool call from what the session reported of that call,
    /// as an ACP tool call update only carries the fields that change. Codex asks to run an MCP tool by its id alone.
    pub(super) fn describe(&self, request: &mut ToolCallUpdate) {
        let Some(known) = self.current.as_ref().and_then(|c| c.tool_calls.get(&request.tool_call_id)) else { return };
        let fields = &mut request.fields;
        fields.kind.get_or_insert(known.kind);
        fields.title.get_or_insert_with(|| known.title.clone());
        if fields.name.is_none() {
            fields.name.clone_from(&known.name);
        }
        if fields.raw_input.is_none() {
            fields.raw_input.clone_from(&known.raw_input);
        }
        if fields.locations.is_none() && !known.locations.is_empty() {
            fields.locations = Some(known.locations.clone());
        }
    }
}

/// Drop a tool call's content and output, which permission requests do not need.
fn strip(call: &mut ToolCall) {
    call.content = Vec::new();
    call.raw_output = None;
}

/// The status update a session update streams, if any. A tool call update counts only when its title, status or
/// locations change, not for every piece of output.
fn progress(update: &SessionUpdate) -> Option<StatusUpdate> {
    Some(match update {
        SessionUpdate::AgentMessageChunk(chunk) => match &chunk.content {
            ContentBlock::Text(t) => StatusUpdate::Message { text: t.text.clone() },
            _ => return None,
        },
        SessionUpdate::AgentThoughtChunk(chunk) => match &chunk.content {
            ContentBlock::Text(t) => StatusUpdate::Thought { text: t.text.clone() },
            _ => return None,
        },
        SessionUpdate::ToolCall(call) => StatusUpdate::Tool {
            id: call.tool_call_id.to_string(),
            title: Some(call.title.clone()),
            tool_kind: name(&call.kind),
            status: name(&call.status),
            paths: paths(&call.locations),
        },
        SessionUpdate::ToolCallUpdate(call)
            if call.fields.title.is_some() || call.fields.status.is_some() || call.fields.locations.is_some() =>
        {
            StatusUpdate::Tool {
                id: call.tool_call_id.to_string(),
                title: call.fields.title.clone(),
                tool_kind: call.fields.kind.as_ref().and_then(name),
                status: call.fields.status.as_ref().and_then(name),
                paths: call.fields.locations.as_deref().and_then(paths),
            }
        }
        SessionUpdate::Plan(plan) => StatusUpdate::Plan {
            steps: plan
                .entries
                .iter()
                .map(|e| PlanStep { content: e.content.clone(), status: name(&e.status).unwrap_or_default() })
                .collect(),
        },
        _ => return None,
    })
}

/// The paths of a tool call's first [`MAX_TOOL_PATHS`] locations; None when it has none.
fn paths(locations: &[ToolCallLocation]) -> Option<Vec<String>> {
    let paths: Vec<String> =
        locations.iter().take(MAX_TOOL_PATHS).map(|l| l.path.to_string_lossy().into_owned()).collect();
    (!paths.is_empty()).then_some(paths)
}

/// The wire name of an ACP enum value.
fn name(value: &impl serde::Serialize) -> Option<String> {
    serde_json::to_value(value).ok()?.as_str().map(str::to_owned)
}
