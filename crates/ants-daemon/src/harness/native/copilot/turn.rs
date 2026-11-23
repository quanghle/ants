//! A prompt to the Copilot CLI and its turn's events as session updates: the message sent, tool calls, tokens, and
//! the errors a turn ends with.

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{
    ContentBlock, PromptResponse, StopReason, ToolCall, ToolCallLocation, ToolCallStatus, ToolKind,
};
use github_copilot_sdk::{Attachment, MessageOptions};
use serde_json::Value;

use crate::harness::native::turn::{Reply, Tokens};
use crate::harness::native::{context_overflow, failed, prompt_text, quota_exhausted, rate_limited, signed_out};

/// The message sending a prompt: its text, and its images attached.
pub(super) fn message(prompt: Vec<ContentBlock>) -> MessageOptions {
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
pub(super) fn text<'a>(data: &'a Value, key: &str) -> &'a str {
    data[key].as_str().unwrap_or_default()
}

/// A tool call the CLI started, named by its title, kind and the file it works on.
pub(super) fn tool_call(data: &Value) -> ToolCall {
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
pub(super) fn tokens(data: &Value) -> Tokens {
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
pub(super) struct Turn {
    /// The reply and reasoning: what streamed in pieces is not repeated whole.
    pub(super) reply: Reply,
    /// The tokens the prompt used, summed over the model calls of its turn, and how many those were.
    pub(super) tokens: Tokens,
    pub(super) calls: u64,
    /// The first error ending the turn.
    pub(super) error: Option<Error>,
    /// The last error of a model call the CLI retries.
    pub(super) retried: Option<Error>,
    /// Whether the last `session.idle` said the turn was aborted, once one came.
    pub(super) idle: Option<bool>,
}

impl Turn {
    /// Note a `session.error`.
    pub(super) fn failed(&mut self, data: &Value) {
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
    pub(super) fn end(&mut self) -> Result<PromptResponse, Error> {
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
