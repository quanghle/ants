//! The names ACP gives a tool call's kind and status, which travel as plain strings ([`ToolCall::kind`],
//! [`TimelineCall::tool_kind`] and [`TimelineCall::status`], [`TimelineRejection::kind`], [`PlanStep::status`],
//! [`StatusUpdate::Tool`]): a harness may send a name not listed here, and a reader keeps it as it came.
//!
//! [`ToolCall::kind`]: crate::ToolCall::kind
//! [`TimelineCall::tool_kind`]: crate::TimelineCall::tool_kind
//! [`TimelineCall::status`]: crate::TimelineCall::status
//! [`TimelineRejection::kind`]: crate::TimelineRejection::kind
//! [`PlanStep::status`]: crate::PlanStep::status
//! [`StatusUpdate::Tool`]: crate::StatusUpdate::Tool

/// ACP tool kinds.
pub mod tool_kind {
    pub const READ: &str = "read";
    pub const EDIT: &str = "edit";
    pub const DELETE: &str = "delete";
    pub const MOVE: &str = "move";
    pub const SEARCH: &str = "search";
    pub const EXECUTE: &str = "execute";
    pub const THINK: &str = "think";
    pub const FETCH: &str = "fetch";
    pub const SWITCH_MODE: &str = "switch_mode";
    /// What a call of no other kind is, and one whose harness names none.
    pub const OTHER: &str = "other";

    /// Every kind.
    pub const ALL: [&str; 10] = [READ, EDIT, DELETE, MOVE, SEARCH, EXECUTE, THINK, FETCH, SWITCH_MODE, OTHER];
}

/// ACP tool call statuses. A plan's step has one of the first three.
pub mod call_status {
    pub const PENDING: &str = "pending";
    pub const IN_PROGRESS: &str = "in_progress";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
}

/// Whether a tool call of status `status` ended: completed or failed.
pub fn call_ended(status: Option<&str>) -> bool {
    matches!(status, Some(call_status::COMPLETED | call_status::FAILED))
}
