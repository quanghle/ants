//! Messages a stream sent rather than a client published. nats-server checks a publisher's rights on the subject it
//! publishes to, never on the reply subject it names, and a stream answers a direct get by sending the stored message,
//! the payload and headers its writer chose, to that reply subject, from inside the account: so whoever may store a
//! message and direct-get it has it delivered on any subject, to the subscribers of that subject and into any stream
//! capturing it, past every subject permission. Such a message carries the headers the stream adds to each answer,
//! `Nats-Stream`, `Nats-Subject` and `Nats-Sequence` naming where it was stored, after those its writer stored; a
//! consumer's delivery, on a reply or deliver subject its creator chose, carries a `$JS.` reply subject. No client
//! publishes either, so readers of what subjects pin take neither as the message it claims to be.
//!
//! The server answers a reply subject in other ways as well, each from inside the account and so into any stream
//! capturing the subject: a publish into a stream is acked there (`{"stream":…,"seq":…}`, or `{"error":…}`), a
//! JetStream API request answered (`{"type":"io.nats.jetstream.api.v1.…",…}`), a consumer's ack of a message acked
//! (an empty message), and a direct get finding nothing told so by a status, which a stream stores as an empty message,
//! without the status. None carries a header, so a stored message with no header of its own and the payload of such
//! an answer is taken as the server's, not its writer's; writers store none of those.
//!
//! The checks themselves are ants-client's ([`ants_client::relayed`]), which every reader of a human's shares; here
//! they take async-nats messages.

use ants_client::relayed as checks;
pub use ants_client::relayed::{DIRECT_HEADERS, Read};
use async_nats::{HeaderMap, Message};

mod gets;
mod heard;
mod kv;

pub use gets::{Gets, GetsError, Got};
pub use heard::{Heard, queue_subscribe, request, subscribe, subscribe_all};
pub use kv::{GuardedKv, Watch};

/// The names of `headers`, one per value, as the checks of what a stream relayed take them.
fn names(headers: Option<&HeaderMap>) -> impl Iterator<Item = &str> + Clone {
    let headers = headers.into_iter().flat_map(HeaderMap::iter);
    headers.flat_map(|(name, values)| std::iter::repeat_n(name.as_ref(), values.len()))
}

/// `msg` as ants-client's readers of answers take one ([`ants_client::requests`]): its headers a name once per value,
/// the values of one name in the order the message carries them.
fn message(msg: Message) -> ants_client::Message {
    let headers = msg.headers.iter().flat_map(HeaderMap::iter);
    let headers = headers.flat_map(|(name, values)| values.iter().map(move |v| (name.to_string(), v.to_string())));
    ants_client::Message {
        subject: msg.subject.to_string(),
        reply: msg.reply.map(|reply| reply.to_string()),
        headers: headers.collect(),
        payload: msg.payload.into(),
        status: msg.status.map(|status| status.as_u16()),
        description: msg.description,
    }
}

/// Whether `msg`, heard on a subject live, is one a stream sent: with any of the stream's headers, or a consumer's
/// `$JS.` reply subject.
pub fn heard_relayed(msg: &Message) -> bool {
    checks::heard_relayed(msg.reply.as_deref(), names(msg.headers.as_ref()))
}

/// Whether a stored message with `headers` and `payload`, read back the way `read` says, was stored as the server or a
/// stream sent it rather than as its writer put it: its headers carry one of the stream's headers more often than the
/// read added them, or it carries no header of its own and its payload is one of the server's answers.
pub fn stored_relayed(headers: Option<&HeaderMap>, payload: &[u8], read: Read) -> bool {
    checks::stored_relayed(names(headers), payload, read)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(reply: Option<&str>, pairs: &[(&str, &str)]) -> Message {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(*name, *value);
        }
        Message {
            subject: "ants.trail.dev-a".into(),
            reply: reply.map(Into::into),
            payload: Default::default(),
            headers: Some(headers),
            status: None,
            description: None,
            length: 0,
        }
    }

    #[test]
    fn a_message_s_headers_are_checked_one_name_per_value() {
        assert!(!heard_relayed(&message(None, &[("Nats-Msg-Id", "1")])), "a client's publish");
        assert!(!heard_relayed(&message(Some("_INBOX.dev-a.x"), &[])), "a client's request");
        assert!(heard_relayed(&message(None, &[("Nats-Stream", "KV_ants_trail_runs")])), "a direct get's answer");
        assert!(heard_relayed(&message(Some("$JS.ACK.ANTS.notes-dev-a.1.7.7.0.0"), &[])), "a consumer's delivery");

        let got =
            [("Nats-Stream", "KV_ants_trails"), ("Nats-Subject", "$KV.ants_trails.deploy"), ("Nats-Sequence", "7")];
        let once = message(None, &got);
        assert!(!stored_relayed(once.headers.as_ref(), b"{}", Read::DirectGet), "the get's own");
        assert!(stored_relayed(once.headers.as_ref(), b"{}", Read::Consumer), "none of a consumer's");
        let twice = message(None, &[got[0], got[1], got[2], ("Nats-Stream", "KV_ants_trail_runs")]);
        assert!(stored_relayed(twice.headers.as_ref(), b"{}", Read::DirectGet), "a second value of one name");
        assert!(stored_relayed(None, br#"{"stream":"ANTS","seq":2}"#, Read::Consumer), "an ack, with no header");
        assert!(!stored_relayed(None, b"{}", Read::Consumer));
    }
}
