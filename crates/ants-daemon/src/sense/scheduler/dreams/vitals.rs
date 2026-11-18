//! The vital signs of the ant's work on a thread over a dream's period ([`Signs`]), folded from what its daemon read
//! back of the thread ([`history::Active`]) and what was done with it ([`Action`]).

use ants_proto::{MergedCall, MergedEnd, PromptEnd};
use ants_sense::dream::{Action, Signs};

#[cfg(doc)]
use crate::stream::history;

/// The vital signs of the work on a thread: its tool `calls` and the `ends` of its prompts as the ant's timeline keeps
/// them, the issues it escalated (`escalations`), and the `actions` done with it.
pub(super) fn signs(calls: &[MergedCall], ends: &[MergedEnd], escalations: u32, actions: &[Action]) -> Signs {
    let mut signs = Signs { threads: 1, escalations, ..Signs::default() };
    for action in actions {
        match action {
            Action::Done { .. } => signs.done = 1,
            Action::Rejected { by: Some(_), .. } => signs.rejected += 1,
            Action::Rejected { by: None, .. } => signs.expired += 1,
            Action::Reopened { .. } | Action::Reaction { .. } => {}
        }
    }
    for MergedCall { call, .. } in calls {
        let tally = signs.calls.entry(call.tool_kind.clone().unwrap_or_else(|| "other".to_owned())).or_default();
        tally.calls += 1;
        if call.status.as_deref() == Some("failed") {
            tally.failed += 1;
        }
        if sense_call(&call.title) {
            signs.sense_calls += 1;
        }
    }
    for MergedEnd { end, .. } in ends {
        let prompts = &mut signs.prompts;
        match end.how {
            PromptEnd::Replied => prompts.replied += 1,
            PromptEnd::NoReply => prompts.no_reply += 1,
            PromptEnd::Cancelled => prompts.cancelled += 1,
            PromptEnd::Capped => prompts.capped += 1,
            PromptEnd::Failed => prompts.failed += 1,
            PromptEnd::Unknown => continue,
        }
        signs.prompt_ms += end.duration_ms;
        signs.tokens += end.tokens;
        // Saturates on a cost out of range, and a negative one counts as none.
        signs.cost_micros += (end.cost * 1e6).round() as u64;
        signs.asked += end.asked;
        signs.rebuilt += u32::from(end.rebuilt);
    }
    signs
}

/// Whether a tool call titled `title` calls one of sense's own tools, served as `sense_<tool>`: as Claude Code titles
/// it, `mcp__<server>__sense_<tool>`, as Codex does, `mcp.<server>.sense_<tool>`, or by its name alone.
fn sense_call(title: &str) -> bool {
    let first = title.split_whitespace().next().unwrap_or_default();
    let tool = first.rsplit(['/', '.', ':']).find(|part| !part.is_empty()).unwrap_or_default();
    let tool = tool.rsplit("__").next().unwrap_or_default();
    tool.strip_prefix(ants_sense::NAME).and_then(|rest| rest.strip_prefix('_')).is_some_and(|rest| !rest.is_empty())
}

#[cfg(test)]
mod tests {
    use ants_proto::{ThreadId, TimelineCall, TimelineEnd, UserId, Uuid};
    use chrono::Utc;

    use super::*;

    fn call(kind: Option<&str>, title: &str, status: &str) -> MergedCall {
        let call = TimelineCall {
            id: Uuid::now_v7().to_string(),
            tool_kind: kind.map(str::to_owned),
            title: title.to_owned(),
            status: Some(status.to_owned()),
            started: Utc::now(),
            duration_ms: None,
            paths: Vec::new(),
        };
        let (ant, thread_id): (UserId, ThreadId) = ("dev-a".parse().unwrap(), ThreadId::generate());
        MergedCall { ant, thread_id, message: Uuid::nil(), call }
    }

    fn end(how: PromptEnd, duration_ms: u64, cost: f64, asked: u32, rebuilt: bool) -> MergedEnd {
        let end = TimelineEnd { how, duration_ms, tokens: 100, cost, asked, rebuilt };
        let (ant, thread_id): (UserId, ThreadId) = ("dev-a".parse().unwrap(), ThreadId::generate());
        MergedEnd { ant, thread_id, message: Uuid::nil(), end, at: Utc::now() }
    }

    #[test]
    fn a_threads_signs_count_its_calls_ends_requests_and_marking_done() {
        let calls = [
            call(Some("execute"), "cargo test", "completed"),
            call(Some("execute"), "cargo build", "failed"),
            call(None, "mcp__ants__sense_remember", "completed"),
            call(Some("other"), "mcp.ants.sense_recall", "failed"),
            call(Some("read"), "sense_notes.md", "completed"),
        ];
        let ends = [
            end(PromptEnd::Replied, 60_000, 0.25, 1, false),
            end(PromptEnd::Cancelled, 30_000, 0.5, 0, true),
            end(PromptEnd::Capped, 1_000, -1.0, 2, false),
            end(PromptEnd::Unknown, 99_000, 9.0, 9, true),
        ];
        let at = Utc::now();
        let actions = [
            Action::Rejected { at, tool: "execute: rm -rf target".into(), by: Some("human alice".into()) },
            Action::Rejected { at, tool: "execute: rm -rf target".into(), by: None },
            Action::Reaction {
                reaction: "thumbsup".into(),
                by: "bob".into(),
                message: Uuid::nil(),
                opening: "x".into(),
            },
            Action::Done { at, by: "alice".into() },
        ];
        let signs = signs(&calls, &ends, 2, &actions);
        assert_eq!((signs.threads, signs.done, signs.rejected, signs.expired, signs.escalations), (1, 1, 1, 1, 2));
        let kinds: Vec<_> =
            signs.calls.iter().map(|(kind, tally)| (kind.as_str(), tally.calls, tally.failed)).collect();
        assert_eq!(kinds, [("execute", 2, 1), ("other", 2, 1), ("read", 1, 0)]);
        assert_eq!(signs.sense_calls, 2);
        let prompts = signs.prompts;
        assert_eq!((prompts.replied, prompts.cancelled, prompts.capped, prompts.total()), (1, 1, 1, 3));
        assert_eq!((signs.prompt_ms, signs.tokens, signs.cost_micros), (91_000, 300, 750_000));
        assert_eq!((signs.asked, signs.rebuilt), (3, 1));
    }

    #[test]
    fn a_call_of_senses_own_tools_is_told_by_its_title() {
        for title in ["mcp__ants__sense_remember", "mcp.ants.sense_recall", "sense_dream", "ants/sense_promise x"] {
            assert!(sense_call(title), "{title}");
        }
        for title in ["mcp__ants__send_message", "sense_", "cat sense_notes.md", "nonsense_tool", "", "sensed"] {
            assert!(!sense_call(title), "{title}");
        }
    }
}
