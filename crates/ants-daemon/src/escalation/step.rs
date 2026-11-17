//! What an escalation does next: up from which level, or to the same members again ([`next_step`]).

use ants_proto::{Envelope, Escalation, EscalationReason, ThreadId, UserId};
use uuid::Uuid;

use super::{Escalated, SUMMARY_CHARS, Stuck};
use crate::limits::caps::Pending;

/// What an escalation does next.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Step {
    /// Go to the same members again, at the same level: the leader has not answered yet.
    Again(Pending),
    /// Go one level above this escalation.
    Climb(Escalation),
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
    let retarget = |e: &Escalation| Escalation { reason, request_id, ..e.clone() };
    match (carried, pending) {
        (Some(c), Some(p)) if c.level > p.escalation.level => Step::Climb(retarget(c)),
        (Some(c), None) if c.stuck != *ant => Step::Climb(retarget(c)),
        (_, Some(p)) if p.renewed_by.is_some() => Step::Climb(retarget(&p.escalation)),
        (_, Some(p)) => Step::Again(p.clone()),
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
    Pending { escalation, summary, question: None, ..asked }
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
