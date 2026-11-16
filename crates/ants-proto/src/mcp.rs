//! The MCP servers an ant's harness runs, and how each stands, as the daemon serving the ant reads them for an admin
//! order ([`AdminOp::ReadMcp`]): it starts a harness of the ant's, opens a session with the ant's MCP servers, reads
//! what the harness says of them once they settled, and stops it, prompting nothing. Only harnesses whose protocol
//! reports them do ([`McpReport::servers`]); a sign-in order ([`AdminOp::McpSignIn`]) signs the harness in to a server
//! asking for OAuth ([`McpSignIn`]).
//!
//! A machine's daemon answers such an order only while it is online: one that waited longer than [`MCP_FRESH`] in the
//! stream for it is refused as stale. The outcome stays in a KV bucket, so what it carries is capped: at most
//! [`MCP_SERVERS_CAP`] servers, each text at [`MCP_TEXT_CAP`] bytes.

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::AdminOp;

/// How long a read of MCP servers, or a sign-in, may wait in the stream for its machine's daemon before it is refused
/// as stale.
pub const MCP_FRESH: TimeDelta = TimeDelta::minutes(2);

/// The most servers an outcome lists.
pub const MCP_SERVERS_CAP: usize = 64;

/// The most bytes of a server's name, error or source shown.
pub const MCP_TEXT_CAP: usize = 1024;

/// How long a sign-in waits for the browser to come back, the harness that started it kept running meanwhile.
pub const MCP_SIGN_IN_FOR: TimeDelta = TimeDelta::minutes(10);

/// The MCP servers of an ant's harness. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpReport {
    /// The harness read, and the protocol it speaks.
    pub harness: String,
    pub protocol: String,
    /// The servers and how each stands; none when the harness's protocol does not report them (`acp`,
    /// `antigravity`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub servers: Option<Vec<McpServerState>>,
    /// Whether servers were left out past [`MCP_SERVERS_CAP`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
    pub read_at: DateTime<Utc>,
}

/// One MCP server of a harness's session, as the harness reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerState {
    pub name: String,
    pub status: McpStatus,
    /// Why it failed or waits, as the harness says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Where the harness took it from, as the harness names it: `user`, `workspace`, the session's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// How many tools it offers, where the harness says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<usize>,
}

impl McpServerState {
    /// A server named `name` standing `status`, its texts capped at [`MCP_TEXT_CAP`].
    pub fn new(name: &str, status: McpStatus, error: Option<&str>, source: Option<&str>) -> Self {
        let error = error.filter(|e| !e.is_empty()).map(capped);
        Self { name: capped(name), status, error, source: source.filter(|s| !s.is_empty()).map(capped), tools: None }
    }

    #[must_use]
    pub fn with_tools(self, tools: Option<usize>) -> Self {
        Self { tools, ..self }
    }
}

/// `text` cut at [`MCP_TEXT_CAP`] bytes, on a character boundary.
fn capped(text: &str) -> String {
    let mut end = text.len().min(MCP_TEXT_CAP);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// How an MCP server stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpStatus {
    /// Connected, its tools offered.
    Connected,
    /// Still starting when the read ended.
    Pending,
    /// Waiting for an OAuth sign-in ([`AdminOp::McpSignIn`]).
    NeedsAuth,
    Failed,
    /// Turned off or stopped.
    Disabled,
    #[serde(other)]
    Unknown,
}

impl McpStatus {
    /// Its name, as it travels: `needs_auth`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Pending => "pending",
            Self::NeedsAuth => "needs_auth",
            Self::Failed => "failed",
            Self::Disabled => "disabled",
            Self::Unknown => "unknown",
        }
    }
}

/// What a sign-in order did ([`AdminOp::McpSignIn`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpSignIn {
    /// The server, as it stands after the order.
    pub server: McpServerState,
    /// The address to open in a browser to sign in, when the server asks for one; the harness waits for the browser
    /// to come back until `expires_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_state_caps_its_texts_and_reads_unknown_statuses() {
        let long = "é".repeat(MCP_TEXT_CAP);
        let state = McpServerState::new("s", McpStatus::Failed, Some(&long), Some(""));
        assert_eq!((state.error.as_ref().map(String::len), state.source), (Some(MCP_TEXT_CAP), None));
        let read: McpServerState = serde_json::from_str(r#"{"name":"s","status":"rebooting"}"#).unwrap();
        assert_eq!(read.status, McpStatus::Unknown);
        let json = serde_json::to_value(McpServerState::new("s", McpStatus::NeedsAuth, None, None)).unwrap();
        assert_eq!(json, serde_json::json!({ "name": "s", "status": "needs_auth" }));
    }
}
