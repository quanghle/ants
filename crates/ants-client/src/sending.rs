//! What a human sends: who gets a message naming no one ([`unaddressed`]), the human's assistant while its grant
//! stands ([`designated`]), else the messenger, and the [`Envelope`] of a message as `ants ask` sends it
//! ([`ask_envelope`], `compose`).

use ants_proto::{AssistantGrant, Envelope, GrantError, Quote, ThreadId, UserId, Uuid, assistant_grant_key};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Who a message goes to, with the human it assists when it got the message as that human's assistant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recipient {
    pub to: UserId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assistant_of: Option<UserId>,
}

impl From<UserId> for Recipient {
    fn from(to: UserId) -> Self {
        Self { to, assistant_of: None }
    }
}

impl std::fmt::Display for Recipient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.assistant_of {
            Some(human) => write!(f, "{} (assistant of {human})", self.to),
            None => write!(f, "{}", self.to),
        }
    }
}

/// The ant `grant`, the record of `human` read now, designates, as verified at `now` for the team of `account` (the
/// issuer of the reader's credentials): none after a withdrawal, and why not for a grant that does not verify.
/// Whether the ant's own credentials name `human` is for the readers of what the ant does for `human` to check, not
/// for whoever writes to it.
pub fn designated(
    grant: &AssistantGrant,
    account: &str,
    human: &UserId,
    now: DateTime<Utc>,
) -> Result<Option<UserId>, GrantError> {
    Ok(grant.verify(&assistant_grant_key(human), account, now)?.assistant)
}

/// Who gets what `me` sends without naming anyone: `assistant`, the human's assistant while the grant naming it
/// stands ([`designated`]), else the messenger.
pub fn unaddressed(me: &UserId, assistant: Option<UserId>) -> Recipient {
    match assistant {
        Some(assistant) => Recipient { to: assistant, assistant_of: Some(me.clone()) },
        None => UserId::messenger().into(),
    }
}

/// What `ants ask` sends as `from`: a thread `from` owns, as it starts it or continues it, when `human`. A thread an
/// ant starts has no owner: only a human gets a thread's escalations, with a fresh turn budget. A human's owner is
/// recorded for the thread and the recipient before the message goes out; the first record of a thread to an ant
/// stays.
pub fn ask_envelope(from: &UserId, human: bool, thread: Option<ThreadId>, to: UserId, text: String) -> Envelope {
    let envelope = match thread {
        Some(thread) => Envelope::on_thread(thread, from.clone(), to, text),
        None => Envelope::new(from.clone(), to, text),
    };
    envelope.with_owner(human.then(|| from.clone()))
}

/// The message a send replies to: its id, and the quote of it the reply carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyTo {
    pub id: Uuid,
    pub quote: Quote,
}

/// A message a human sends from its client's bar.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Send {
    /// The ant, or human, it goes to; none for a message naming no one ([`unaddressed`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<UserId>,
    /// The thread it goes on; none to start one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadId>,
    pub text: String,
    /// The message on that thread it replies to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ReplyTo>,
    /// Wait behind the prompt running on the thread rather than steering it, as `/later` and `ants ask --queue` do.
    #[serde(default)]
    pub queue: bool,
}

/// The envelope of `send` from the human `me` to `to`, as `ants ask` makes it; refused with no text, or to wait on
/// no thread.
pub(crate) fn compose(me: &UserId, to: UserId, send: Send) -> Result<Envelope, Error> {
    const WHAT: &str = "sending the message";
    let Send { thread, text, reply_to, queue, .. } = send;
    if text.trim().is_empty() {
        return Err(Error::invalid(WHAT, "a message needs text"));
    }
    if queue && thread.is_none() {
        return Err(Error::invalid(WHAT, "a message waits behind a prompt on an existing thread only"));
    }
    let mut envelope = ask_envelope(me, true, thread, to, text);
    if let Some(ReplyTo { id, quote }) = reply_to {
        envelope = envelope.quoting(id, quote);
    }
    envelope.queue = queue;
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[test]
    fn a_message_naming_no_one_goes_to_the_assistant_while_its_grant_stands_else_to_the_messenger() {
        let alice = p("alice");
        let to = unaddressed(&alice, Some(p("pa")));
        assert_eq!(to, Recipient { to: p("pa"), assistant_of: Some(alice.clone()) });
        assert_eq!(to.to_string(), "pa (assistant of alice)");
        let to = unaddressed(&alice, None);
        assert_eq!((to.to.as_str(), to.assistant_of.as_ref()), ("system-messenger", None));
        assert_eq!(to.to_string(), "system-messenger");
    }

    #[test]
    fn a_human_s_message_names_it_owner_of_its_thread_and_replies_steer_unless_queued() {
        let (alice, dev_a) = (p("alice"), p("dev-a"));
        let first = compose(&alice, dev_a.clone(), Send { text: "fix it".into(), ..Send::default() }).unwrap();
        assert_eq!((first.owner.as_ref(), first.queue, first.in_reply_to), (Some(&alice), false, None));
        assert_eq!(first.subject(), "ants.inbox.dev-a.alice");

        let quote = Quote::new(dev_a.clone(), "done, see the diff");
        let reply = Send {
            thread: Some(first.thread_id.clone()),
            text: "then the docs".into(),
            reply_to: Some(ReplyTo { id: first.id, quote: quote.clone() }),
            queue: true,
            ..Send::default()
        };
        let later = compose(&alice, dev_a.clone(), reply).unwrap();
        assert_eq!((&later.thread_id, later.queue), (&first.thread_id, true));
        assert_eq!((later.in_reply_to, later.quote), (Some(first.id), Some(quote)));

        let nowhere = Send { text: "later".into(), queue: true, ..Send::default() };
        assert!(matches!(compose(&alice, dev_a.clone(), nowhere).unwrap_err().kind, ErrorKind::Invalid(_)));
        assert!(matches!(compose(&alice, dev_a.clone(), Send::default()).unwrap_err().kind, ErrorKind::Invalid(_)));
        assert_eq!(ask_envelope(&dev_a, false, None, alice, "hi".into()).owner, None, "an ant's thread has no owner");
    }
}
