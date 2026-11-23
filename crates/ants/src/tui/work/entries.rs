//! What a thread's entries, read in full, tell before something is sent on it: who a reply goes to, which ant is
//! stuck, whether a question still waits, and the latest message to react to.

use ants_proto::{EscalationReason, ThreadId, UserId, Uuid};
use anyhow::{Result, bail};

use crate::threads::thread::{Entry, Kind};

/// Who a reply on a thread whose `entries` are these goes to: the last ant that messaged `me` there, else the last ant
/// `me` messaged there; `human` tells the humans.
pub fn reply_to(entries: &[Entry], me: &UserId, human: impl Fn(&UserId) -> bool) -> Option<UserId> {
    let messages = || entries.iter().rev().filter(|e| matches!(e.kind, Kind::Message | Kind::Escalation) && !e.forged);
    let ant = |user: &Option<UserId>| user.as_ref().filter(|u| *u != me && !human(u)).cloned();
    messages()
        .filter(|e| e.to.as_ref() == Some(me))
        .find_map(|e| ant(&e.from))
        .or_else(|| messages().filter(|e| e.from.as_ref() == Some(me)).find_map(|e| ant(&e.to)))
}

/// The other human of the latest message between `me` and another human among `entries`, for a reply on a thread
/// with no ant; `human` tells the humans.
pub fn dm_with(entries: &[Entry], me: &UserId, human: impl Fn(&UserId) -> bool) -> Option<UserId> {
    let other = |from: &UserId, to: &UserId| match (from == me, to == me) {
        (true, false) => Some(to.clone()),
        (false, true) => Some(from.clone()),
        _ => None,
    };
    entries
        .iter()
        .rev()
        .filter(|e| e.kind == Kind::Message && !e.forged)
        .find_map(|e| other(e.from.as_ref()?, e.to.as_ref()?).filter(|u| human(u)))
}

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
