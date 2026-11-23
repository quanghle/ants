//! How a Claude Code turn ended, from its `result`: a stop reason, or the error the daemon classifies.

use std::time::{Duration, SystemTime};

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::StopReason;
use serde_json::Value;

use super::turn::Turn;
use crate::harness::native::{context_overflow, failed, quota_exhausted, rate_limited, signed_out};

/// The starts of Claude Code's messages of an exhausted plan, credit or spend allowance.
const USAGE_LIMITS: &[&str] = &[
    "You've hit your",
    "You've reached your",
    "You're out of usage credits",
    "You're out of extra usage",
    "Your org is out of usage",
    "Your seat type doesn't include",
    "Your usage allocation has been disabled",
    "Your group's usage limit is set to $0",
    "Credit balance is too low",
    "requires usage credits",
];

/// The starts of Claude Code's messages of a conversation outgrowing the model's context window.
const OVERFLOWS: &[&str] = &["Prompt is too long", "Context limit reached", "Context exceeds", "Request too large"];

/// How a turn ended, from its result: a stop reason, or the error the daemon classifies.
pub(super) fn ended(result: &Value, turn: &Turn<'_>, interrupted: bool) -> Result<StopReason, Error> {
    let terminal = result["terminal_reason"].as_str().unwrap_or_default();
    if interrupted || matches!(terminal, "aborted_streaming" | "aborted_tools") {
        return Ok(StopReason::Cancelled);
    }
    if result["subtype"] == "error_max_turns" {
        return Ok(StopReason::MaxTurnRequests);
    }
    if result["is_error"] != true {
        return Ok(match result["stop_reason"].as_str() {
            Some("max_tokens") => StopReason::MaxTokens,
            Some("refusal") => StopReason::Refusal,
            _ => StopReason::EndTurn,
        });
    }
    Err(error(result, turn.error.as_deref(), turn.synthetic.as_deref(), turn.limit.as_ref()))
}

/// The error of a failed turn's `result`, its last assistant message having been of error `kind` (such as
/// `rate_limit`) and Claude Code's own message `synthetic`, with the last rate limit state `limit`.
pub(super) fn error(result: &Value, kind: Option<&str>, synthetic: Option<&str>, limit: Option<&Value>) -> Error {
    let text = error_text(result, synthetic);
    let kind = kind.unwrap_or_default();
    let status = result["api_error_status"].as_u64();
    if matches!(kind, "authentication_failed" | "oauth_org_not_allowed") || text.to_lowercase().contains("run /login") {
        return signed_out(text);
    }
    let rejected = limit.is_some_and(|limit| limit["status"] == "rejected");
    if matches!(kind, "billing_error" | "account_on_hold")
        || rejected
        || USAGE_LIMITS.iter().any(|start| text.contains(start))
    {
        return quota_exhausted(limit.and_then(resets), text);
    }
    if kind == "rate_limit" || status == Some(429) {
        // Claude Code says a capacity limit is "not your usage limit", which would read as an exhausted quota.
        return rate_limited(Some(429), None, text.replace(" (not your usage limit)", ""));
    }
    if kind == "overloaded" || status == Some(529) {
        return rate_limited(Some(529), None, text);
    }
    if result["terminal_reason"] == "prompt_too_long" || OVERFLOWS.iter().any(|start| text.contains(start)) {
        return context_overflow(text);
    }
    failed(text)
}

/// What a failed turn's `result` says: its message, else its errors, else Claude Code's own message.
pub(super) fn error_text(result: &Value, synthetic: Option<&str>) -> String {
    if let Some(text) = result["result"].as_str().filter(|text| !text.is_empty()) {
        return text.to_owned();
    }
    let errors: Vec<&str> = result["errors"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    if !errors.is_empty() {
        return errors.join("; ");
    }
    synthetic.map_or_else(|| format!("Claude Code ended the turn with {}", result["subtype"]), str::to_owned)
}

/// How long until a rejected rate limit resets.
fn resets(limit: &Value) -> Option<Duration> {
    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(limit["resetsAt"].as_u64()?);
    at.duration_since(SystemTime::now()).ok()
}
