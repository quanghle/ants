//! Fixtures of the live view: fixed times, users, messages and tool requests.

use std::time::Duration;

use ants_client::entry::Entry;
use ants_proto::{
    ApprovalRequest, Envelope, Escalation, EscalationReason, ThreadId, ToolCall, UserId, Verdict, subject,
};
use chrono::{DateTime, Utc};

use crate::threads::thread::Parser;

pub(crate) fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
}

pub(crate) fn user(name: &str) -> UserId {
    name.parse().unwrap()
}

/// The entry of a message from `from` to `to` on `thread`, stored at sequence `seq`.
pub(crate) fn message(seq: u64, thread: &ThreadId, from: &str, to: &str, text: &str) -> Entry {
    let envelope = Envelope { thread_id: thread.clone(), ..Envelope::new(user(from), user(to), text) };
    let subject = subject::inbox(&envelope.to, &envelope.from);
    let payload = serde_json::to_vec(&envelope).unwrap();
    Parser::default().entry(seq, at(seq.cast_signed()), &subject, &payload).unwrap()
}

/// The entry of an escalation of `thread` from `from` to `to` for `reason`, stored at sequence `seq`.
pub(crate) fn escalation(seq: u64, thread: &ThreadId, from: &str, to: &str, reason: EscalationReason) -> Entry {
    let x = Escalation::new(user(from), thread.clone(), reason);
    let envelope = Envelope {
        thread_id: thread.clone(),
        escalation: Some(Box::new(x)),
        ..Envelope::new(user(from), user(to), "help")
    };
    let payload = serde_json::to_vec(&envelope).unwrap();
    Parser::default().entry(seq, at(seq.cast_signed()), &subject::inbox(&user(to), &user(from)), &payload).unwrap()
}

pub(crate) fn request(ant: &str, title: &str, options: Vec<Verdict>) -> ApprovalRequest {
    let tool = ToolCall { kind: "execute".into(), title: title.into(), ..Default::default() };
    ApprovalRequest::new(user(ant), ThreadId::generate(), tool, options, Duration::from_mins(5))
}
