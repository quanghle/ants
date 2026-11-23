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
//! Headers are given by name, one name per value a message carries, whatever the client that read them keeps them in.
//! nats-server tells names apart by case: a header a writer stored under another case of one of the stream's names is
//! the writer's own, not counted here, and a reader takes the stream's by their exact names, the last value of each
//! ([`crate::transport::Message::header_value`]).

use serde_json::Value;

/// The headers a stream adds to each message it sends to a direct get's reply subject.
pub(crate) const STREAM_HEADERS: [&str; 3] = ["Nats-Stream", "Nats-Subject", "Nats-Sequence"];
/// Every header a direct get adds, a batched one's included.
pub(crate) const DIRECT_HEADERS: [&str; 6] =
    ["Nats-Stream", "Nats-Subject", "Nats-Sequence", "Nats-Time-Stamp", "Nats-Num-Pending", "Nats-Last-Sequence"];
/// The fields of a publish's ack, or of its refusal.
const ACK_FIELDS: [&str; 8] = ["stream", "seq", "domain", "duplicate", "val", "batch", "count", "error"];

/// How a stored message was read back, which tells the headers the read itself added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Read {
    /// By a direct get, whose answer carries one of each of the stream's headers after those stored.
    DirectGet,
    /// Through a consumer, which hands on the stored headers alone.
    Consumer,
}

/// Whether a message heard on a subject live, naming `reply` and carrying the headers `names`, is one a stream sent:
/// with any of the stream's headers, or a consumer's `$JS.` reply subject.
pub fn heard_relayed<'a>(reply: Option<&str>, names: impl Iterator<Item = &'a str> + Clone) -> bool {
    reply.is_some_and(|reply| reply.starts_with("$JS.")) || carries(names, 0)
}

/// Whether a stored message carrying the headers `names` and `payload`, read back the way `read` says, was stored as
/// the server or a stream sent it rather than as its writer put it: its headers carry one of the stream's headers more
/// often than the read added them, or it carries no header of its own and its payload is one of the server's answers.
pub fn stored_relayed<'a>(names: impl Iterator<Item = &'a str> + Clone, payload: &[u8], read: Read) -> bool {
    carries(names.clone(), usize::from(read == Read::DirectGet)) || (!has_own_header(names, read) && is_answer(payload))
}

/// Whether `names` carry any of the stream's headers more than `added` times.
fn carries<'a>(names: impl Iterator<Item = &'a str> + Clone, added: usize) -> bool {
    STREAM_HEADERS.iter().any(|header| names.clone().filter(|name| name == header).count() > added)
}

/// Whether `names` hold any header the read did not add.
fn has_own_header<'a>(mut names: impl Iterator<Item = &'a str>, read: Read) -> bool {
    names.any(|name| read == Read::Consumer || !DIRECT_HEADERS.iter().any(|added| name.eq_ignore_ascii_case(added)))
}

/// Whether `payload` is one the server answers a reply subject with: empty (a consumer's ack of an ack), a publish's
/// ack or refusal, or a JetStream API answer.
fn is_answer(payload: &[u8]) -> bool {
    if payload.is_empty() {
        return true;
    }
    let Ok(Value::Object(fields)) = serde_json::from_slice::<Value>(payload) else { return false };
    let api = fields.get("type").and_then(Value::as_str).is_some_and(|kind| kind.starts_with("io.nats."));
    let acked = fields.get("stream").is_some_and(Value::is_string);
    let refused = fields.get("error").is_some_and(|error| error.get("code").is_some());
    api || ((acked || refused) && fields.keys().all(|field| ACK_FIELDS.contains(&field.as_str())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Iterator<Item = &'a str> + Clone {
        pairs.iter().map(|(name, _)| *name)
    }

    #[test]
    fn a_message_heard_live_is_relayed_with_a_stream_header_or_a_consumer_reply() {
        assert!(!heard_relayed(None, names(&[("Nats-Msg-Id", "1")])), "a client's publish");
        assert!(!heard_relayed(Some("_INBOX.dev-a.x"), names(&[])), "a client's request");
        assert!(heard_relayed(None, names(&[("Nats-Stream", "KV_ants_trail_runs")])), "a direct get's answer");
        assert!(heard_relayed(None, names(&[("Nats-Subject", "$KV.ants_trail_runs.dev-a.x")])));
        assert!(heard_relayed(None, names(&[("Nats-Sequence", "7")])));
        assert!(heard_relayed(Some("$JS.ACK.ANTS.notes-dev-a.1.7.7.0.0"), names(&[])), "a consumer's delivery");
    }

    fn relayed(pairs: &[(&str, &str)], payload: &str, read: Read) -> bool {
        stored_relayed(names(pairs), payload.as_bytes(), read)
    }

    const GET: [(&str, &str); 4] = [
        ("Nats-Stream", "KV_ants_trails"),
        ("Nats-Subject", "$KV.ants_trails.deploy"),
        ("Nats-Sequence", "7"),
        ("Nats-Time-Stamp", "2026-11-20T00:00:00Z"),
    ];

    #[test]
    fn a_stored_message_is_relayed_with_more_stream_headers_than_its_read_added() {
        assert!(!relayed(&GET, "{}", Read::DirectGet), "the get's own");
        assert!(relayed(&GET, "{}", Read::Consumer), "none of a consumer's");
        assert!(!relayed(&[], "{}", Read::Consumer));
        let forged = [
            ("Nats-Stream", "KV_ants_trail_runs"),
            ("Nats-Subject", "$KV.ants_trail_runs.dev-a.x"),
            ("Nats-Stream", "KV_ants_trails"),
            ("Nats-Subject", "$KV.ants_trails.deploy"),
        ];
        assert!(relayed(&forged, "{}", Read::DirectGet), "stored as a direct get answered");
    }

    #[test]
    fn a_stream_s_header_under_another_case_is_its_writer_s_and_counts_as_none_of_the_stream_s() {
        let mut stored = vec![
            ("nats-stream", "KV_ants_trail_runs"),
            ("NATS-STREAM", "KV_ants_trail_runs"),
            ("nats-subject", "$KV.ants_trail_runs.dev-a.x"),
            ("Nats-subject", "$KV.ants_trail_runs.dev-a.x"),
            ("nats-sequence", "1"),
            ("NATS-Sequence", "2"),
        ];
        assert!(!heard_relayed(None, names(&stored)), "no stream sends those");
        assert!(!relayed(&stored, "{}", Read::Consumer));
        stored.extend(GET);
        assert!(!relayed(&stored, "{}", Read::DirectGet), "the get's own, once each");
        assert!(relayed(&stored, "", Read::DirectGet), "nor is any a header of its own");
        stored.push(("Nats-Subject", "$KV.ants_trails.deploy"));
        assert!(relayed(&stored, "{}", Read::DirectGet));
    }

    #[test]
    fn a_stored_message_with_no_header_of_its_own_is_the_servers_with_the_payload_of_an_answer() {
        for read in [Read::DirectGet, Read::Consumer] {
            let pairs: &[(&str, &str)] = if read == Read::DirectGet { &GET } else { &[] };
            assert!(relayed(pairs, r#"{"stream":"KV_ants_trail_runs","seq":2}"#, read), "an ack");
            assert!(relayed(pairs, r#"{"stream":"ANTS","domain":"hub","seq":2,"duplicate":true}"#, read));
            assert!(relayed(pairs, r#"{"error":{"code":400,"err_code":10071}}"#, read), "a refusal");
            let api = r#"{"type":"io.nats.jetstream.api.v1.stream_info_response","total":0}"#;
            assert!(relayed(pairs, api, read), "a JetStream API answer");
            assert!(relayed(pairs, "", read), "an ack of an ack, or a status");
            assert!(!relayed(pairs, r#"{"runs":[]}"#, read), "a record");
            assert!(!relayed(pairs, r#"{"stream":"x","seq":2,"runs":[]}"#, read), "a record with more");
            assert!(!relayed(pairs, r#"{"type":"save"}"#, read));
            assert!(!relayed(pairs, r#"{"error":"no such ant"}"#, read), "a call's answer");
            assert!(!relayed(pairs, r#"{"seq":2}"#, read));
            assert!(!relayed(pairs, "not json", read));
        }
        let mut own = GET.to_vec();
        own.push(("KV-Operation", "DEL"));
        assert!(!relayed(&own, "", Read::DirectGet), "a delete marker");
        assert!(!relayed(&[("Nats-Msg-Id", "1")], r#"{"stream":"x","seq":1}"#, Read::Consumer), "a header of its own");
    }
}
