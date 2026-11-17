//! Where a member of an escalation's group sends its reply ([`route`]).

use ants_proto::{Envelope, Escalation, EscalationReason, UserId};

/// The members of `escalation`'s level: its leader and the ants sent it with the leader, in chain order.
pub fn members_at(escalation: &Escalation) -> &[UserId] {
    let from = escalation.visited.iter().position(|p| *p == escalation.leader).unwrap_or(escalation.visited.len());
    &escalation.visited[from..]
}

/// Where a member's reply to a message carrying another ant's escalation goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// To the stuck ant: the leader resolves the escalation.
    Stuck(UserId),
    /// To the thread's notes, for the leader: a member other than the leader does not answer the stuck ant.
    Notes,
    /// To the sender, a fellow member of the group, as for any message.
    Sender,
}

/// The escalation `envelope` carries, when it is another ant's on the message's thread, which makes `ant` a
/// member of its group. Not an approval request's: any member decides one, and it goes no higher.
pub fn group<'a>(ant: &UserId, envelope: &'a Envelope) -> Option<&'a Escalation> {
    envelope
        .escalation
        .as_deref()
        .filter(|e| e.thread_id == envelope.thread_id && e.stuck != *ant && e.reason != EscalationReason::Approval)
}

/// The escalation of the group `ant` is a member of on `envelope` ([`group`]), and where its reply goes.
pub fn route<'a>(ant: &UserId, envelope: &'a Envelope) -> Option<(&'a Escalation, Route)> {
    let e = group(ant, envelope)?;
    let route = if members_at(e).contains(&envelope.from) {
        Route::Sender
    } else if e.is_leader(ant) {
        Route::Stuck(e.stuck.clone())
    } else {
        Route::Notes
    };
    Some((e, route))
}
