//! What an escalation does next: up from which level, or to the same members again ([`next_step`]).

use ants_proto::{Envelope, Escalation, EscalationReason, ThreadId, UserId};
use uuid::Uuid;

use super::{Escalated, SUMMARY_CHARS, Stuck};
use crate::limits::caps::Pending;

/// What an escalation does next.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Step {
    /// Go to the same members again, at the same level: the leader has not answered yet. Not for a stuck ant only
    /// repeating it ([`repeats`]).
    Again(Box<Pending>),
    /// Go one level above this escalation.
    Climb(Box<Escalation>),
}

/// Whose escalation for what, should it start afresh.
#[derive(Debug, Clone, Copy)]
pub(super) struct Fresh<'a> {
    /// The stuck ant.
    pub(super) ant: &'a UserId,
    /// The ant that created the thread, if one did: the origin of a fresh escalation unless it is the stuck one.
    pub(super) origin: Option<&'a UserId>,
    pub(super) thread: &'a ThreadId,
    pub(super) reason: EscalationReason,
}

/// What `fresh.ant`'s escalation for `fresh.reason` on `fresh.thread` does next, given the escalation `carried` by
/// the message it got stuck on and the thread's `pending` one. A carried escalation the ant started itself, with none
/// pending, was ended by a human: it starts afresh, from the thread's origin ([`Escalation::new_from`]).
pub(super) fn next_step(
    fresh: Fresh<'_>,
    request_id: Option<Uuid>,
    carried: Option<&Escalation>,
    pending: Option<&Pending>,
) -> Step {
    let Fresh { ant, origin, thread, reason } = fresh;
    let carried = carried.filter(|c| &c.thread_id == thread);
    let pending = pending.filter(|p| !settled_question(p));
    let retarget = |e: &Escalation| Box::new(Escalation { reason, request_id, ..e.clone() });
    match (carried, pending) {
        (Some(c), Some(p)) if c.level > p.escalation.level => Step::Climb(retarget(c)),
        (Some(c), None) if c.stuck != *ant => Step::Climb(retarget(c)),
        (_, Some(p)) if p.renewed_by.is_some() => Step::Climb(retarget(&p.escalation)),
        (_, Some(p)) => Step::Again(Box::new(p.clone())),
        _ => Step::Climb(retarget(&Escalation::new_from(ant.clone(), thread.clone(), reason, origin))),
    }
}

pub(super) fn chain_of(escalation: &Escalation, to: Vec<UserId>) -> Escalated {
    match &escalation.assistant_of {
        Some(human) => Escalated::Assistant { assistant: escalation.leader.clone(), human: human.clone() },
        None => Escalated::Chain { level: escalation.level, leader: escalation.leader.clone(), to },
    }
}

/// Whether `pending` is an ask_human question settled: answered by the assistant or asked of the human. It holds back
/// neither another question nor an escalation, which start afresh.
pub(super) fn settled_question(pending: &Pending) -> bool {
    pending.escalation.reason == EscalationReason::Question && (pending.renewed_by.is_some() || pending.at_owner())
}

/// Whether `pending` is an ask_human question still with the assistant that an escalation for `reason` takes over.
pub(super) fn superseded(pending: &Pending, reason: EscalationReason) -> bool {
    pending.escalation.reason == EscalationReason::Question
        && reason != EscalationReason::Question
        && !settled_question(pending)
}

/// The question `asked`, with the assistant, become the escalation of `stuck` at the same level: labelled with its
/// reason and about its notice, then the question, which no longer goes on to the human as a question.
pub(super) fn supersede(asked: Pending, stuck: &Stuck) -> Pending {
    let escalation = Escalation { reason: stuck.reason, request_id: stuck.request_id, ..asked.escalation };
    let summary =
        format!("{}\n\nAsked before: {}", stuck.notice.text, asked.summary).chars().take(SUMMARY_CHARS).collect();
    Pending { escalation, summary, question: None, sent: None, ..asked }
}

/// Whether `reason` is one every message on the thread meets again until the escalation is answered: its turns or
/// allowance spent, or its notes out of turns. Such an escalation is kept even when it goes straight to a human, so
/// the next message's waits for it ([`repeats`]).
pub(super) fn waits_for_turns(reason: EscalationReason) -> bool {
    matches!(reason, EscalationReason::TurnBudget | EscalationReason::Cap | EscalationReason::Notes)
}

/// Whether `stuck`, going to the members of the thread's `pending` escalation again, would only repeat it: stuck for
/// want of turns ([`waits_for_turns`]) on another message than the one the escalation went out for at this level. In
/// a channel every member woken by every post meets the spent budget, and each would send the same escalation once
/// per post; a retry of the same message sends it again, so a failed send is not lost. Out of allowance on `today`
/// (`YYYY-MM-DD`), a later UTC day than the one the escalation went out on, it is no repeat: the daily allowance came
/// back at 00:00 UTC and was spent again, so a cap's escalation goes out once a UTC day until answered.
pub(super) fn repeats(pending: &Pending, stuck: &Stuck, today: &str) -> bool {
    let Some(sent) = pending.sent.as_ref().filter(|_| waits_for_turns(stuck.reason)) else { return false };
    sent.id != stuck.id && (stuck.reason != EscalationReason::Cap || sent.day == today)
}

/// Whether `envelope` passes the escalation `carried` of the ant's thread on to a human: from the assistant leading the
/// level below, it carries the level that human leads ([`Escalation::passed_on`]).
pub(super) fn passes_on(ant: &UserId, envelope: &Envelope, carried: &Escalation) -> bool {
    carried.thread_id == envelope.thread_id
        && carried.stuck == *ant
        && carried.assistant_of.is_none()
        && envelope.from != *ant
        && carried.visited.ends_with(&[envelope.from.clone(), carried.leader.clone()])
        && carried.reason != EscalationReason::Approval
}
