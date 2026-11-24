//! The kinds of a daemon's notice ([`NoticeKind`]) and the rules following from a kind.

use serde::{Deserialize, Serialize};

#[cfg(doc)]
use super::Envelope;

/// What a daemon's notice ([`Envelope::notice`]) tells of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoticeKind {
    /// The ant's spend passed the warning share of a cap.
    Spend,
    /// The ant's harness is signed out: messages wait, or go to its fallbacks.
    SignedOut,
    /// The ant's harness is out of quota: messages wait, or go to its fallbacks.
    Quota,
    /// The ant's harness keeps failing to start.
    Failing,
    /// Every fallback of the ant turns messages away while its own harness cannot serve.
    NoFallback,
    /// The ant gave up on a message or post after its deliveries.
    GaveUp,
    /// A message was withdrawn from the ant's queue before it answered it.
    Withdrawn,
    /// A thread's session expired and its git worktree was removed, its branch kept.
    Expired,
    /// Messages the ant sent wait for an ant no daemon serves.
    Unserved,
    /// A human's assistant acted for the human.
    Assistant,
    /// Another human decided a tool request, or answered an escalation, in place of the one it reached.
    InPlace,
    /// A tool request an ant of the chain was asked to decide expired unanswered.
    RequestExpired,
    /// A later message steered the prompt answering a message, so the answer goes to that one.
    Steered,
    /// An MCP server the ant's harness signs in to as its sessions open needs a browser sign-in, or failed to sign
    /// in: the ant answers without the server meanwhile.
    McpSignIn,
    /// A run the ant started with `run_trail` while answering a trail step ended or stopped: the notice, on the step's
    /// thread to its conductor, tells what the runs started answering the step spent ([`Envelope::trail_spent`]).
    RunSpent,
    /// The ant turned a trail step away unanswered: it reached a spending cap, or the step's thread has no turns left.
    /// The notice, on the step's thread to its conductor ([`Envelope::step_notice`]), fails the step's call.
    Unanswered,
}

impl NoticeKind {
    /// Its name on the wire, as serde writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spend => "spend",
            Self::SignedOut => "signed_out",
            Self::Quota => "quota",
            Self::Failing => "failing",
            Self::NoFallback => "no_fallback",
            Self::GaveUp => "gave_up",
            Self::Withdrawn => "withdrawn",
            Self::Expired => "expired",
            Self::Unserved => "unserved",
            Self::Assistant => "assistant",
            Self::InPlace => "in_place",
            Self::RequestExpired => "request_expired",
            Self::Steered => "steered",
            Self::McpSignIn => "mcp_sign_in",
            Self::RunSpent => "run_spent",
            Self::Unanswered => "unanswered",
        }
    }

    /// Whether it needs a human to act: to sign the ant's harness or one of its MCP servers in, wait for or raise its
    /// quota, mend its harness or replay the message given up on.
    pub fn needs_action(self) -> bool {
        self.holds_thread() || self == Self::McpSignIn
    }

    /// Whether the ant answers nothing on the thread until a human acts ([`NoticeKind::needs_action`]): what the
    /// thread is stuck on. An MCP server to sign in to holds nothing: the ant answers without it.
    pub fn holds_thread(self) -> bool {
        matches!(self, Self::SignedOut | Self::Quota | Self::Failing | Self::NoFallback | Self::GaveUp)
    }
}
