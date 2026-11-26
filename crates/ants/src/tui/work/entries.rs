//! What a thread's entries, read in full, tell before something is sent on it: which ant is stuck, whether a
//! question still waits, and the latest message to react to; who a reply goes to is every client's
//! ([`ants_client::sending::bar::replied`]).

use ants_client::entry::{Entry, Kind};
use ants_proto::{EscalationReason, ThreadId, UserId, Uuid};
use anyhow::{Result, bail};

/// The ant stuck on a thread whose `entries` are these: that of its latest escalation but for a tool request's, or
/// the ant of its latest dead letter.
pub fn stuck(entries: &[Entry]) -> Option<UserId> {
    entries.iter().rev().find_map(|e| match e.kind {
        Kind::Escalation if e.reason != Some(EscalationReason::Approval) && !e.forged => e.stuck.clone(),
        Kind::Dead => e.to.clone(),
        _ => None,
    })
}

/// Whether the question asked of `me` in the message `id` on `thread`, whose `entries` are these, waits for an answer:
/// an error when it is gone from the thread or a message of `me` answered it already.
pub fn waiting(entries: &[Entry], me: &UserId, thread: &ThreadId, id: Uuid) -> Result<()> {
    if !entries.iter().any(|e| e.id == Some(id) && e.question.is_some() && e.to.as_ref() == Some(me) && !e.forged) {
        bail!("question {id} to {me} is gone from thread {thread}");
    }
    if entries.iter().any(|e| e.from.as_ref() == Some(me) && e.answer.as_ref().is_some_and(|a| a.question == id)) {
        bail!("question {id} on thread {thread} is answered already");
    }
    Ok(())
}

/// The latest message or post among `entries` not from `me`, to react to.
pub fn latest(entries: &[Entry], me: &UserId) -> Option<Uuid> {
    entries
        .iter()
        .rev()
        .filter(|e| matches!(e.kind, Kind::Message | Kind::Escalation | Kind::Post) && e.from.as_ref() != Some(me))
        .find_map(|e| e.id)
}

#[cfg(test)]
mod tests {
    use ants_proto::{EscalationReason, Question};

    use super::*;
    use crate::test_support::live::{message, user};

    #[test]
    fn the_stuck_ant_is_that_of_the_latest_escalation_or_dead_letter() {
        let thread = ThreadId::generate();
        let mut escalated = message(1, &thread, "dev-a", "alice", "out of turns");
        (escalated.kind, escalated.reason, escalated.stuck) =
            (Kind::Escalation, Some(EscalationReason::TurnBudget), Some(user("dev-a")));
        let mut approval = escalated.clone();
        approval.reason = Some(EscalationReason::Approval);
        assert_eq!(stuck(&[escalated.clone(), approval.clone()]), Some(user("dev-a")));
        assert_eq!(stuck(&[approval]), None);
        let mut dead = message(2, &thread, "alice", "dev-b", "hi");
        dead.kind = Kind::Dead;
        assert_eq!(stuck(&[escalated, dead]), Some(user("dev-b")));
    }

    #[test]
    fn the_message_to_react_to_is_the_latest_of_another() {
        let (thread, me) = (ThreadId::generate(), user("alice"));
        let question = Question::new("which?", &["a".into(), "b".into()], true).unwrap();
        let mut asked = message(1, &thread, "dev-a", "alice", "which?");
        asked.question = Some(question.into());
        let answered = message(2, &thread, "alice", "dev-a", "a");
        assert_eq!(latest(&[asked.clone(), answered], &me), asked.id);
    }
}
