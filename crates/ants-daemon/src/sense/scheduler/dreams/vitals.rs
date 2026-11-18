//! The vital signs of the ant's work on a thread over a dream's period ([`Signs`]), folded from what its daemon read
//! back of the thread ([`history::Active`]) and what was done with it ([`Action`]).

use ants_proto::{MergedEnd, PromptEnd};
use ants_sense::dream::{Action, Signs};

#[cfg(doc)]
use crate::stream::history;

/// The vital signs of the work on a thread: the `ends` of its prompts as the ant's timeline keeps them, with their tool
/// calls, the issues it escalated (`escalations`), and the `actions` done with it.
pub(super) fn signs(ends: &[MergedEnd], escalations: u32, actions: &[Action]) -> Signs {
    let mut signs = Signs { threads: 1, escalations, ..Signs::default() };
    for action in actions {
        match action {
            Action::Done { .. } => signs.done = 1,
            Action::Rejected { by: Some(_), .. } => signs.rejected += 1,
            Action::Rejected { by: None, .. } => signs.expired += 1,
            Action::Reaction { .. } => {}
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
        for (kind, count) in &end.calls {
            let tally = signs.calls.entry(kind.clone()).or_default();
            tally.calls += count.calls;
            tally.failed += count.failed;
        }
        signs.sense_calls += end.sense_calls;
    }
    signs
}

#[cfg(test)]
mod tests {
    use ants_proto::{CallCount, ThreadId, TimelineEnd, UserId, Uuid};
    use chrono::Utc;

    use super::*;

    fn end(how: PromptEnd, duration_ms: u64, cost: f64, asked: u32, rebuilt: bool) -> MergedEnd {
        let end = TimelineEnd {
            how,
            duration_ms,
            tokens: 100,
            cost,
            asked,
            rebuilt,
            calls: [("execute", 2, 1), ("read", 1, 0)]
                .map(|(kind, calls, failed)| (kind.to_owned(), CallCount { calls, failed }))
                .into(),
            sense_calls: 1,
        };
        let (ant, thread_id): (UserId, ThreadId) = ("dev-a".parse().unwrap(), ThreadId::generate());
        MergedEnd { ant, thread_id, message: Uuid::nil(), end, at: Utc::now() }
    }

    #[test]
    fn a_threads_signs_count_its_ends_with_their_calls_requests_and_marking_done() {
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
        let signs = signs(&ends, 2, &actions);
        assert_eq!((signs.threads, signs.done, signs.rejected, signs.expired, signs.escalations), (1, 1, 1, 1, 2));
        let kinds: Vec<_> =
            signs.calls.iter().map(|(kind, tally)| (kind.as_str(), tally.calls, tally.failed)).collect();
        assert_eq!(kinds, [("execute", 6, 3), ("read", 3, 0)], "an unknown end aside");
        assert_eq!(signs.sense_calls, 3);
        let prompts = signs.prompts;
        assert_eq!((prompts.replied, prompts.cancelled, prompts.capped, prompts.total()), (1, 1, 1, 3));
        assert_eq!((signs.prompt_ms, signs.tokens, signs.cost_micros), (91_000, 300, 750_000));
        assert_eq!((signs.asked, signs.rebuilt), (3, 1));
    }
}
