//! The text of escalations, notices and the notes posts that go with them.

use std::time::Duration;

use ants_proto::{DEFAULT_TURN_BUDGET, Envelope, Escalation, EscalationReason, OwnerBecause, UserId};

use super::route::{Route, members_at, route};
use crate::limits::caps::Pending;

/// Bytes a notice leaves under the server's `max_payload`, for its headers.
const NOTICE_HEADROOM: usize = 1024;
/// Most of an escalation's text kept for the escalations that follow it up.
const SUMMARY_CHARS: usize = 4000;

/// What is kept of `text`, an escalation's, for the escalations that follow it up.
pub(super) fn summary(text: &str) -> String {
    text.chars().take(SUMMARY_CHARS).collect()
}

/// `notice` with its text cut to fit `max_payload`, its headers aside, ending with an `[ants]` line telling of the cut
/// when it is cut: an escalation or notice quoting a message near the limit would be over it whole.
pub fn fit(mut notice: Envelope, max_payload: usize) -> Envelope {
    let room = max_payload.saturating_sub(NOTICE_HEADROOM);
    let size = |notice: &Envelope| serde_json::to_vec(notice).map_or(usize::MAX, |json| json.len());
    let mut over = size(&notice).saturating_sub(room);
    if over == 0 {
        return notice;
    }
    let text = std::mem::take(&mut notice.text);
    let mut keep = text.len();
    // Each byte cut shrinks the JSON by one or more, its escape included: a few rounds at most.
    loop {
        keep = text.floor_char_boundary(keep.saturating_sub(over));
        notice.text = format!(
            "{}\n\n[ants] This notice was cut to fit the bus's max_payload: {keep} of its {} bytes are kept.",
            &text[..keep],
            text.len()
        );
        over = size(&notice).saturating_sub(room);
        if over == 0 || keep == 0 {
            return notice;
        }
    }
}

/// What an assistant passes on of `pending`: the question or the escalation.
pub(super) fn what_passes(pending: &Pending) -> &'static str {
    match pending.escalation.reason {
        EscalationReason::Question => "the question",
        _ => "the escalation",
    }
}

/// The text every member of a chain gets: the escalation, how the members settle it, and `summary`; for the
/// assistant leading a level alone, how it settles it or passes it on to its human.
pub(super) fn chain_text(escalation: &Escalation, to: &[UserId], summary: &str) -> String {
    let (thread, leader, stuck) = (&escalation.thread_id, &escalation.leader, &escalation.stuck);
    if let Some(human) = &escalation.assistant_of {
        let what = match escalation.reason {
            EscalationReason::Question => format!("{stuck} asks {human} a question"),
            reason => format!("{stuck} is stuck ({}) and would reach {human}", reason_text(reason)),
        };
        return format!(
            "[ants] Escalation on thread {thread}: {what}; it comes to {leader}, the assistant of {human}, first. \
             {leader} settles what it can, by answering {stuck} on thread {thread} or acting for {human}, and \
             passes on to {human} what it cannot (escalate tool); without an answer from {leader} it goes on to \
             {human} after escalate_after. A message from {human} ends it.\n\n{summary}"
        );
    }
    format!(
        "[ants] Escalation on thread {thread}, level {}: {stuck} is stuck ({}). Sent to {}; {leader} leads. The members \
         coordinate in the notes of thread {thread} (read_notes, post_notes). Only {leader} resolves it, by \
         answering {stuck} on thread {thread}, or escalates it further (escalate).\n\n{summary}",
        escalation.level,
        reason_text(escalation.reason),
        join(to),
    )
}

/// What an escalation going up to `to` posts to the thread's notes, with a fresh budget.
pub(super) fn notes_text(escalation: &Escalation, to: &[UserId]) -> String {
    format!(
        "[ants] Escalation of {}'s thread, level {}: sent to {}; {} leads. The notes have a fresh budget of {} turns.",
        escalation.stuck,
        escalation.level,
        join(to),
        escalation.leader,
        DEFAULT_TURN_BUDGET
    )
}

/// What `ant`'s harness is told about the escalation `envelope` carries, when it is another ant's on the
/// message's thread: who is in the group, who leads, and where the reply goes.
pub fn group_note(ant: &UserId, envelope: &Envelope) -> Option<String> {
    let (e, route) = route(ant, envelope)?;
    let (thread, leader, stuck) = (&e.thread_id, &e.leader, &e.stuck);
    if let Some(human) = &e.assistant_of {
        return Some(format!(
            "[ants] {ant} has {stuck}'s escalation on thread {thread} as the assistant of {human}, which it would \
             reach: answer {stuck} with what settles it (the reply to this message goes to {stuck}), act for {human} \
             where it needs that, or pass it on to {human} with the escalate tool, saying why."
        ));
    }
    let role = if e.is_leader(ant) {
        format!(
            "{ant} leads: it alone resolves the escalation, by answering {stuck} on thread {thread}, or sends it \
             one level higher (escalate tool) when the group cannot. The other members post to the thread notes."
        )
    } else {
        format!(
            "Only {leader} resolves it (answers {stuck} on thread {thread}) or escalates it further (escalate tool)."
        )
    };
    let reply = match route {
        Route::Stuck(_) => format!("The reply to this message goes to {stuck}."),
        Route::Notes => format!("The reply to this message goes to the thread notes for {leader}, not to {stuck}."),
        Route::Sender => format!("The reply to this message goes to {}.", envelope.from),
    };
    Some(format!(
        "[ants] {ant} is part of an escalation group for {stuck}'s thread {thread}, level {}; members: {}. The group \
         shares the thread's notes (read_notes, post_notes). {role} {reply}",
        e.level,
        join(members_at(e))
    ))
}

/// Why an escalation went to the owner, for the ant that asked for it.
pub(super) fn why_text(escalation: &Escalation, because: &OwnerBecause) -> String {
    let default = || match because {
        OwnerBecause::PassedOn => "the assistant passed it on.".to_owned(),
        _ => format!("{} has no escalate_to chain.", escalation.leader),
    };
    owner_text(escalation, because).unwrap_or_else(default).trim_start_matches("[ants] ").to_owned()
}

/// Why an escalation went to the owner, unless it never went up a chain for want of one.
pub(super) fn owner_text(escalation: &Escalation, because: &OwnerBecause) -> Option<String> {
    let (stuck, level) = (&escalation.stuck, escalation.level);
    match because {
        OwnerBecause::NoChain if level == 0 && escalation.leader == *stuck => None,
        OwnerBecause::NoChain if level == 0 => Some(format!(
            "[ants] The escalation of {stuck} starts at {}, which created thread {} and has no escalate_to.",
            escalation.leader, escalation.thread_id
        )),
        OwnerBecause::NoChain => Some(format!(
            "[ants] The escalation of {stuck} reached level {level}; {} has no escalate_to above it.",
            escalation.leader
        )),
        OwnerBecause::Visited(ant) => Some(format!(
            "[ants] The escalation of {stuck} reached level {level}; {ant} had it already, so the chains loop."
        )),
        OwnerBecause::HopLimit(max) => {
            Some(format!("[ants] The escalation of {stuck} reached the hop limit (max_escalation_hops = {max})."))
        }
        OwnerBecause::PassedOn => None,
    }
}

pub(super) fn reason_text(reason: EscalationReason) -> &'static str {
    match reason {
        EscalationReason::TurnBudget => "turn budget spent",
        EscalationReason::Cap => "limit or cap reached",
        EscalationReason::Notes => "thread notes out of turns",
        EscalationReason::DeadLetter => "a message became a dead letter",
        EscalationReason::Approval => "a tool request awaits a decision",
        EscalationReason::Asked => "asked for help",
        EscalationReason::ContextWindow => "context window exceeded",
        EscalationReason::Harness => "the harness keeps failing to start",
        EscalationReason::Question => "a question for a human",
        EscalationReason::Storage => "JetStream storage filling up",
        EscalationReason::Unknown => "reason unknown",
    }
}

pub(super) fn join(ants: &[UserId]) -> String {
    ants.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ")
}

pub(super) fn duration(d: Duration) -> String {
    match d.as_secs() {
        s if s >= 60 && s % 60 == 0 => format!("{} min", s / 60),
        s if s > 0 => format!("{s} s"),
        _ => format!("{} ms", d.as_millis()),
    }
}
