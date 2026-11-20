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
            Action::Undone { .. } | Action::Reopened { .. } | Action::Reaction { .. } => {}
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
mod tests;
