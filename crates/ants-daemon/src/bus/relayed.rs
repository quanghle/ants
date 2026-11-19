//! Messages a stream sent rather than a client published. nats-server checks a publisher's rights on the subject it
//! publishes to, never on the reply subject it names, and a stream answers a direct get by sending the stored message,
//! the payload and headers its writer chose, to that reply subject, from inside the account: so whoever may store a
//! message and direct-get it has it delivered on any subject, to the subscribers of that subject and into any stream
//! capturing it, past every subject permission. Such a message carries the headers the stream adds to each answer,
//! `Nats-Stream`, `Nats-Subject` and `Nats-Sequence` naming where it was stored, after those its writer stored; a
//! consumer's delivery, on a reply or deliver subject its creator chose, carries a `$JS.` reply subject. No client
//! publishes either, so readers of what subjects pin take neither as the message it claims to be.

use async_nats::header::{NATS_SEQUENCE, NATS_STREAM, NATS_SUBJECT};
use async_nats::{HeaderMap, HeaderName, Message};

/// The headers a stream adds to each message it sends to a direct get's reply subject.
const STREAM_HEADERS: [HeaderName; 3] = [NATS_STREAM, NATS_SUBJECT, NATS_SEQUENCE];

/// How a stored message was read back, which tells the headers the read itself added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Read {
    /// By a direct get, whose answer carries one of each of the stream's headers after those stored.
    DirectGet,
    /// Through a consumer, which hands on the stored headers alone.
    Consumer,
}

/// Whether `msg`, heard on a subject live, is one a stream sent: with any of the stream's headers, or a consumer's
/// `$JS.` reply subject.
pub fn heard_relayed(msg: &Message) -> bool {
    msg.reply.as_deref().is_some_and(|reply| reply.starts_with("$JS.")) || carries(msg.headers.as_ref(), 0)
}

/// Whether a stored message whose headers, as read back the way `read` says, are `headers` was stored as a stream sent
/// it: its headers carry one of the stream's headers more often than the read added them.
pub fn stored_relayed(headers: Option<&HeaderMap>, read: Read) -> bool {
    carries(headers, usize::from(read == Read::DirectGet))
}

/// Whether `headers` carry any of the stream's headers more than `added` times.
fn carries(headers: Option<&HeaderMap>, added: usize) -> bool {
    headers.is_some_and(|headers| STREAM_HEADERS.iter().any(|name| headers.get_all(name.clone()).count() > added))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(*name, *value);
        }
        headers
    }

    fn message(reply: Option<&str>, pairs: &[(&str, &str)]) -> Message {
        Message {
            subject: "ants.trail.dev-a".into(),
            reply: reply.map(Into::into),
            payload: Default::default(),
            headers: Some(headers(pairs)),
            status: None,
            description: None,
            length: 0,
        }
    }

    #[test]
    fn a_message_heard_live_is_relayed_with_a_stream_header_or_a_consumer_reply() {
        assert!(!heard_relayed(&message(None, &[("Nats-Msg-Id", "1")])), "a client's publish");
        assert!(!heard_relayed(&message(Some("_INBOX.dev-a.x"), &[])), "a client's request");
        assert!(heard_relayed(&message(None, &[("Nats-Stream", "KV_ants_trail_runs")])), "a direct get's answer");
        assert!(heard_relayed(&message(None, &[("Nats-Subject", "$KV.ants_trail_runs.dev-a.x")])));
        assert!(heard_relayed(&message(None, &[("Nats-Sequence", "7")])));
        assert!(heard_relayed(&message(Some("$JS.ACK.ANTS.notes-dev-a.1.7.7.0.0"), &[])), "a consumer's delivery");
    }

    #[test]
    fn a_stored_message_is_relayed_with_more_stream_headers_than_its_read_added() {
        let read_back = headers(&[("Nats-Stream", "KV_ants_trails"), ("Nats-Subject", "$KV.ants_trails.deploy")]);
        assert!(!stored_relayed(Some(&read_back), Read::DirectGet), "the get's own");
        assert!(stored_relayed(Some(&read_back), Read::Consumer), "none of a consumer's");
        assert!(!stored_relayed(None, Read::Consumer));
        let forged = headers(&[
            ("Nats-Stream", "KV_ants_trail_runs"),
            ("Nats-Subject", "$KV.ants_trail_runs.dev-a.x"),
            ("Nats-Stream", "KV_ants_trails"),
            ("Nats-Subject", "$KV.ants_trails.deploy"),
        ]);
        assert!(stored_relayed(Some(&forged), Read::DirectGet), "stored as a direct get answered");
    }
}
