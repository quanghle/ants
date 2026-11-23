//! What a review message carries beside its text: the outcomes of the reviewer's earlier findings
//! ([`ReviewOutcomes`]) and the commits the dream rests on ([`ReviewCommit`]).

use serde::{Deserialize, Serialize};

#[cfg(doc)]
use super::Envelope;
use crate::ThreadId;

/// The dream a review message asks a review of and what is new in the outcomes of the reviewer's earlier findings
/// ([`Envelope::review_outcomes`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewOutcomes {
    /// The number of the sender's dream under review.
    pub dream: u64,
    /// A batch of earlier findings whose verdict or outcome changed since the sender last told it, oldest first,
    /// each once, up to [`MAX_REVIEW_FINDINGS_BYTES`] bytes as a compact JSON array.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<FindingOutcome>,
    /// The sage's finding ids in the sender's earlier-findings window, before the packet's display limit.
    /// Only these ids are eligible targets of a follow-up; an absent list grants no links.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub earlier_findings: Vec<String>,
}

/// Where one of a reviewer's earlier findings stands with the ant it reviewed ([`ReviewOutcomes::findings`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindingOutcome {
    /// The finding's id, `<dream>.<n>`: the n-th finding of the reviewer's answer to the review of that dream.
    pub finding: String,
    pub verdict: FindingVerdict,
    /// Whether the change applying it still holds; only for an applied finding.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub holds: bool,
    /// The sender's reason for its verdict, at most [`MAX_OUTCOME_REASON`] characters: the sender's text, untrusted.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

/// The most characters of a verdict's reason ([`FindingOutcome::reason`]) a sender tells and a receiver keeps.
pub const MAX_OUTCOME_REASON: usize = 200;

/// The most bytes of the compact JSON `review_outcomes.findings` array sent on one review.
pub const MAX_REVIEW_FINDINGS_BYTES: usize = 64 * 1024;

/// An ant's verdict on a reviewer's finding ([`FindingOutcome::verdict`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingVerdict {
    Applied,
    Rejected,
    Deferred,
}

/// A commit a dream's review packet names ([`Envelope::review_commits`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewCommit {
    /// The repository's normalized git remote, such as `github.com/owner/repo`.
    pub repo: String,
    /// The full commit id.
    pub commit: String,
    /// The thread whose worktree is at it; none for the ant's workdir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadId>,
    /// The repository's git directory on the machine of the sender's daemon, for a recipient on the same machine to
    /// fetch the commit from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<String>,
}
