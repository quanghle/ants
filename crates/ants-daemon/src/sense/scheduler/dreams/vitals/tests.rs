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
        Action::Reaction { reaction: "thumbsup".into(), by: "bob".into(), message: Uuid::nil(), opening: "x".into() },
        Action::Done { at, by: "alice".into() },
    ];
    let signs = signs(&ends, 2, &actions);
    assert_eq!((signs.threads, signs.done, signs.rejected, signs.expired, signs.escalations), (1, 1, 1, 1, 2));
    let kinds: Vec<_> = signs.calls.iter().map(|(kind, tally)| (kind.as_str(), tally.calls, tally.failed)).collect();
    assert_eq!(kinds, [("execute", 6, 3), ("read", 3, 0)], "an unknown end aside");
    assert_eq!(signs.sense_calls, 3);
    let prompts = signs.prompts;
    assert_eq!((prompts.replied, prompts.cancelled, prompts.capped, prompts.total()), (1, 1, 1, 3));
    assert_eq!((signs.prompt_ms, signs.tokens, signs.cost_micros), (91_000, 300, 750_000));
    assert_eq!((signs.asked, signs.rebuilt), (3, 1));
}
