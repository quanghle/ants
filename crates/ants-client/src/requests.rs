//! The requests of a human's reads and writes, built with no I/O, and the reading of their answers: a window of the
//! stream by batched direct gets (`batch_get`, [`batch_answer`]), the publish of an [`Envelope`] under its id
//! with the stream's ack (`publish`, `stored`), the record of a thread's owner (`owner_record`, `owner_recorded`),
//! a bucket's entry read by a direct get and put by compare-and-set (`kv_get`, `kv_entry`, `kv_update`,
//! `kv_updated`), as the human's [`ReadMarker`] is, and what is heard live: an ant's [`StatusEvent`]s on a thread
//! (`status_event`, [`status_on`]) and the messages to the human (`heard`).
//!
//! What a client of another kind sends and awaits itself, over async-nats, is public; the rest is
//! [`crate::client::Client`]'s alone.

use std::time::Duration;

use ants_proto::{
    Envelope, HUMANS_BUCKET, JsDomain, OWNERS_BUCKET, READ_BUCKET, ReadMarker, STREAM, StatusEvent, ThreadId, UserId,
    assistant_grant_key, owner_key, owner_record_id, subject,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind};
use crate::relayed::{self, DIRECT_HEADERS, Read};
use crate::transport::Message;

/// How long a request waits for its answer.
pub const REQUEST_WAIT: Duration = Duration::from_secs(10);
/// Messages asked for at once by a direct get, and the bytes they come to at most (a larger message comes alone).
pub const DIRECT_BATCH: usize = 256;
pub(crate) const DIRECT_BATCH_BYTES: usize = 4 << 20;
/// The header of the messages left of a batched direct get, on the status ending each batch.
pub(crate) const NUM_PENDING: &str = "Nats-Num-Pending";

const MSG_ID: &str = "Nats-Msg-Id";
const SEQUENCE: &str = "Nats-Sequence";
const SUBJECT: &str = "Nats-Subject";
const TIME_STAMP: &str = "Nats-Time-Stamp";
/// The header making the stream refuse a message unless the last on its subject has the given sequence (0: none).
const EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";
/// The header marking a KV delete or purge, and its values.
const KV_OPERATION: &str = "KV-Operation";
const DELETED: [&str; 2] = ["DEL", "PURGE"];
/// JetStream's error codes for a publish expecting another last sequence, and for one past a subject's limit.
const WRONG_LAST_SEQUENCE: u32 = 10071;
const MAX_PER_SUBJECT: u32 = 10077;
/// What a put of a second record of a thread to an ant fails with: the owners' stream discards new messages per
/// subject.
const RECORDED: &str = "maximum messages per subject";

/// The prefix of the JetStream API's subjects, in `domain` if the hub has one.
pub(crate) fn api(domain: Option<&JsDomain>) -> String {
    domain.map_or_else(|| "$JS.API.".to_owned(), |domain| format!("$JS.{domain}.API."))
}

/// The subject a put under `key` of `bucket` goes to: through the JetStream API of the hub's domain, if it has one.
fn put_subject(domain: Option<&JsDomain>, bucket: &str, key: &str) -> String {
    let api = domain.map_or_default(|domain| api(Some(domain)));
    format!("{api}{}", subject::kv(bucket, key))
}

/// A batched direct get: up to `batch` messages on `next_by_subj`, the first at or after `seq`, or stored at or after
/// `start_time`, followed by an end of batch.
#[derive(Debug, Serialize)]
pub struct BatchGet<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub next_by_subj: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    pub batch: usize,
    pub max_bytes: usize,
}

/// Where a read by direct gets goes on from: the first message on the filter at or after `seq`, or stored at or after
/// `since` when nothing was read yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
}

impl Position {
    /// From the first message kept.
    pub fn all() -> Self {
        Self { seq: 1, since: None }
    }

    /// From the message at `seq` on.
    pub fn from_seq(seq: u64) -> Self {
        Self { seq: seq.max(1), since: None }
    }

    /// From the first message stored at `time` or later.
    pub fn since(time: DateTime<Utc>) -> Self {
        Self { seq: 0, since: Some(time.to_rfc3339_opts(SecondsFormat::Nanos, true)) }
    }

    /// After the message stored at `seq`.
    pub fn after(&mut self, seq: u64) {
        (self.seq, self.since) = (seq + 1, None);
    }

    /// The request of the next batch on `filter` from here.
    pub fn request<'a>(&self, filter: &'a str) -> BatchGet<'a> {
        BatchGet {
            seq: (self.seq > 0).then_some(self.seq),
            next_by_subj: filter,
            start_time: self.since.clone(),
            batch: DIRECT_BATCH,
            max_bytes: DIRECT_BATCH_BYTES,
        }
    }
}

/// The batched direct get of the stream's messages on `filter` from `at` on, its answers to come on `reply`.
pub(crate) fn batch_get(domain: Option<&JsDomain>, filter: &str, at: &Position, reply: String) -> Message {
    let payload = serde_json::to_vec(&at.request(filter)).unwrap_or_default();
    Message { reply: Some(reply), ..Message::new(format!("{}DIRECT.GET.{STREAM}", api(domain)), payload) }
}

/// A message as the stream stored it, read back by a direct get.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub seq: u64,
    pub stored_at: DateTime<Utc>,
    pub subject: String,
    /// The headers its writer stored, without those the get added.
    pub headers: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

/// One answer to a batched direct get.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Got {
    Stored(Stored),
    /// The message at this sequence is one a stream relayed ([`crate::relayed`]): a reader refuses it.
    Relayed(u64),
    /// The batch ended, with how many messages are left after it, as far as told.
    End {
        pending: Option<u64>,
    },
    /// Nothing is stored from the position on.
    Nothing,
}

/// What `answer`, one of the messages a batched direct get is answered with, is.
pub fn batch_answer(answer: Message) -> Result<Got, Error> {
    const WHAT: &str = "a direct get of the stream";
    let number = |name: &str| answer.header_value(name).and_then(|n| n.parse::<u64>().ok());
    match answer.status {
        None => {}
        Some(404) => return Ok(Got::Nothing),
        Some(204) => return Ok(Got::End { pending: number(NUM_PENDING) }),
        Some(status) => return Err(Error::status(WHAT, status, answer.description)),
    }
    let seq = number(SEQUENCE).ok_or_else(|| Error::malformed(WHAT, "no sequence"))?;
    if relayed::stored_relayed(answer.header_names(), &answer.payload, Read::DirectGet) {
        return Ok(Got::Relayed(seq));
    }
    let subject = answer.header_value(SUBJECT).ok_or_else(|| Error::malformed(WHAT, "no subject"))?.to_owned();
    let time = answer.header_value(TIME_STAMP).ok_or_else(|| Error::malformed(WHAT, "no time"))?;
    let stored_at = DateTime::parse_from_rfc3339(time).map_err(|e| Error::malformed(WHAT, e))?.with_timezone(&Utc);
    let own = |(name, _): &(String, String)| !DIRECT_HEADERS.iter().any(|added| name.eq_ignore_ascii_case(added));
    let headers = answer.headers.into_iter().filter(own).collect();
    Ok(Got::Stored(Stored { seq, stored_at, subject, headers, payload: answer.payload }))
}

/// The stream's ack of a publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub stream: String,
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    /// The stream held a message of the same id already, and kept that one.
    #[serde(default)]
    pub duplicate: bool,
}

/// The publish of `envelope` on its subject under its id, which the stream keeps once however often it is sent
/// within its duplicate window.
pub(crate) fn publish(envelope: &Envelope) -> Result<Message, Error> {
    let payload = serde_json::to_vec(envelope).map_err(|e| Error::invalid("sending the message", e))?;
    Ok(Message::new(envelope.subject(), payload).header(MSG_ID, envelope.id.to_string()))
}

/// The ack in `answer`, a stream's answer to a publish made to `what`; its refusal otherwise.
pub(crate) fn stored(what: &str, answer: &Message) -> Result<Ack, Error> {
    #[derive(Deserialize)]
    struct Refusal {
        #[serde(default)]
        code: u16,
        #[serde(default)]
        err_code: u32,
        #[serde(default)]
        description: String,
    }
    #[derive(Deserialize)]
    struct Answer {
        error: Option<Refusal>,
        #[serde(default)]
        stream: String,
        #[serde(default)]
        seq: u64,
        domain: Option<String>,
        #[serde(default)]
        duplicate: bool,
    }
    if let Some(status) = answer.status {
        return Err(Error::status(what, status, answer.description.clone()));
    }
    let answer: Answer = serde_json::from_slice(&answer.payload).map_err(|e| Error::malformed(what, e))?;
    match answer.error {
        Some(Refusal { code, err_code, description }) => {
            Err(Error::new(what, ErrorKind::Refused { code, err_code, description }))
        }
        None if answer.stream.is_empty() => Err(Error::malformed(what, "an ack naming no stream")),
        None => Ok(Ack { stream: answer.stream, seq: answer.seq, domain: answer.domain, duplicate: answer.duplicate }),
    }
}

/// The record of `owner` as the owner of `thread` that a message to `to` names, in the owners bucket: the first
/// record of a thread to an ant stays, the bucket refusing another or dropping it as a repeat of the first's id.
pub(crate) fn owner_record(domain: Option<&JsDomain>, thread: &ThreadId, to: &UserId, owner: &UserId) -> Message {
    let key = owner_key(thread, to);
    let id = owner_record_id(&key);
    Message::new(put_subject(domain, OWNERS_BUCKET, &key), owner.to_string()).header(MSG_ID, id)
}

/// Whether `answer` to an [`owner_record`] says the thread's owner for the ant is recorded, by this record or by one
/// before it; why not otherwise.
pub(crate) fn owner_recorded(answer: &Message) -> Result<(), Error> {
    match stored("recording the thread's owner", answer) {
        Ok(_) => Ok(()),
        Err(Error { kind: ErrorKind::Refused { err_code, description, .. }, .. })
            if err_code == MAX_PER_SUBJECT || description.contains(RECORDED) =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// The direct get of the last message under `key` of `bucket`.
pub(crate) fn kv_get(domain: Option<&JsDomain>, bucket: &str, key: &str) -> Message {
    Message::new(
        format!("{}DIRECT.GET.{}.{}", api(domain), subject::kv_stream(bucket), subject::kv(bucket, key)),
        Vec::new(),
    )
}

/// A bucket's entry: its value, none for a key with none or deleted, and its revision, 0 for a key never written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvEntry {
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) revision: u64,
}

/// The entry in `answer` to a [`kv_get`] made to `what`.
pub(crate) fn kv_entry(what: &str, answer: Message) -> Result<KvEntry, Error> {
    match answer.status {
        None => {}
        Some(404) => return Ok(KvEntry { value: None, revision: 0 }),
        Some(status) => return Err(Error::status(what, status, answer.description)),
    }
    if relayed::stored_relayed(answer.header_names(), &answer.payload, Read::DirectGet) {
        return Err(Error::new(what, ErrorKind::Relayed));
    }
    let revision = answer.header_value(SEQUENCE).and_then(|seq| seq.parse().ok());
    let revision = revision.ok_or_else(|| Error::malformed(what, "no sequence"))?;
    // The server adds no such header: it is the writer's, read first as the daemons and async-nats read it.
    let deleted = answer.stored_header_value(KV_OPERATION).is_some_and(|op| DELETED.contains(&op));
    Ok(KvEntry { value: (!deleted).then_some(answer.payload), revision })
}

/// The put of `value` under `key` of `bucket`, stored only while the key's last message is still the one at
/// `revision` (0: none).
pub(crate) fn kv_update(domain: Option<&JsDomain>, bucket: &str, key: &str, value: Vec<u8>, revision: u64) -> Message {
    Message::new(put_subject(domain, bucket, key), value).header(EXPECTED_LAST_SUBJECT_SEQUENCE, revision.to_string())
}

/// Whether `answer` to a [`kv_update`] made to `what` says the value is stored, with its revision; None when the key
/// changed meanwhile.
pub(crate) fn kv_updated(what: &str, answer: &Message) -> Result<Option<u64>, Error> {
    match stored(what, answer) {
        Ok(ack) => Ok(Some(ack.seq)),
        Err(Error { kind: ErrorKind::Refused { err_code: WRONG_LAST_SEQUENCE, .. }, .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The direct get of the read marker of `human`.
pub(crate) fn marker_get(domain: Option<&JsDomain>, human: &UserId) -> Message {
    kv_get(domain, READ_BUCKET, human.as_str())
}

/// The put of `marker` as the read marker of `human`, stored only while its entry is still at `revision`.
pub(crate) fn marker_put(domain: Option<&JsDomain>, human: &UserId, marker: &ReadMarker, revision: u64) -> Message {
    let value = serde_json::to_vec(marker).unwrap_or_default();
    kv_update(domain, READ_BUCKET, human.as_str(), value, revision)
}

/// The direct get of the record designating the assistant of `human`.
pub(crate) fn grant_get(domain: Option<&JsDomain>, human: &UserId) -> Message {
    kv_get(domain, HUMANS_BUCKET, &assistant_grant_key(human))
}

/// The update `message`, heard live on a status subject, tells; none for one a stream relayed, one that does not
/// parse, and one naming another ant or thread than its subject.
pub(crate) fn status_event(message: &Message) -> Option<StatusEvent> {
    if relayed::heard_relayed(message.reply.as_deref(), message.header_names()) {
        return None;
    }
    status_on(&message.subject, &message.payload)
}

/// The update `payload`, on the status subject `subject`, tells; none for one that does not parse, and one naming
/// another ant or thread than its subject.
pub fn status_on(subject: &str, payload: &[u8]) -> Option<StatusEvent> {
    let event = serde_json::from_slice::<StatusEvent>(payload).ok()?;
    event.check_subject(subject).ok().map(|()| event)
}

/// The envelope `message`, heard live on an inbox subject, carries; none for one a stream relayed, one that parses
/// as no envelope, and one whose sender is forged.
pub(crate) fn heard(message: &Message) -> Option<Envelope> {
    if relayed::heard_relayed(message.reply.as_deref(), message.header_names()) {
        return None;
    }
    crate::inbox::parse(&message.subject, &message.payload).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    /// What went wrong with a read that failed.
    fn kind<T: std::fmt::Debug>(read: Result<T, Error>) -> ErrorKind {
        read.unwrap_err().kind
    }

    fn headers(message: &Message) -> Vec<(&str, &str)> {
        message.headers.iter().map(|(name, value)| (name.as_str(), value.as_str())).collect()
    }

    fn got(seq: &str, payload: &str) -> Message {
        Message::new("_INBOX.alice.x", payload)
            .header("Nats-Stream", "ANTS")
            .header("Nats-Subject", "ants.inbox.alice.dev-a")
            .header("Nats-Sequence", seq)
            .header("Nats-Time-Stamp", "2026-11-20T10:00:00.5Z")
    }

    #[test]
    fn a_window_is_asked_for_in_batches_from_a_sequence_or_a_time() {
        let hub: JsDomain = "hub".parse().unwrap();
        let since = DateTime::parse_from_rfc3339("2026-11-20T10:00:00Z").unwrap().with_timezone(&Utc);
        let mut at = Position::since(since);
        let first = batch_get(Some(&hub), "ants.inbox.>", &at, "_INBOX.alice.r".into());
        assert_eq!(first.subject, "$JS.hub.API.DIRECT.GET.ANTS");
        assert_eq!(first.reply.as_deref(), Some("_INBOX.alice.r"));
        let request: serde_json::Value = serde_json::from_slice(&first.payload).unwrap();
        let expected = serde_json::json!({
            "next_by_subj": "ants.inbox.>",
            "start_time": "2026-11-20T10:00:00.000000000Z",
            "batch": 256,
            "max_bytes": 4 << 20,
        });
        assert_eq!(request, expected);
        at.after(41);
        let next = batch_get(None, "ants.inbox.>", &at, "_INBOX.alice.r".into());
        assert_eq!(next.subject, "$JS.API.DIRECT.GET.ANTS");
        let request: serde_json::Value = serde_json::from_slice(&next.payload).unwrap();
        assert_eq!((request["seq"].as_u64(), request.get("start_time")), (Some(42), None));
        assert_eq!((Position::all().seq, Position::from_seq(0).seq), (1, 1));
    }

    #[test]
    fn a_batch_s_answers_are_stored_messages_relayed_ones_and_its_end() {
        let stored = got("7", r#"{"text":"hi"}"#).header("Nats-Msg-Id", "m1");
        let Got::Stored(stored) = batch_answer(stored).unwrap() else { panic!("a stored message") };
        assert_eq!((stored.seq, stored.subject.as_str()), (7, "ants.inbox.alice.dev-a"));
        assert_eq!(stored.stored_at.to_rfc3339(), "2026-11-20T10:00:00.500+00:00");
        assert_eq!(stored.headers, [("Nats-Msg-Id".to_owned(), "m1".to_owned())], "as its writer stored them");

        let relayed = Message::new("_INBOX.alice.x", "{}").header("Nats-Stream", "KV_ants_read");
        let relayed = relayed.header("Nats-Sequence", "3");
        let relayed = Message { headers: [relayed.headers, got("8", "").headers].concat(), ..relayed };
        assert_eq!(batch_answer(relayed).unwrap(), Got::Relayed(8));
        assert_eq!(batch_answer(got("9", r#"{"stream":"ANTS","seq":2}"#)).unwrap(), Got::Relayed(9), "an ack stored");

        let end = Message { status: Some(204), ..Message::new("_INBOX.alice.x", "") };
        let end = end.header("Nats-Num-Pending", "12").header("Nats-Last-Sequence", "300");
        assert_eq!(batch_answer(end).unwrap(), Got::End { pending: Some(12) });
        let nothing = Message { status: Some(404), ..Message::default() };
        assert_eq!(batch_answer(nothing).unwrap(), Got::Nothing);
        let refused = Message { status: Some(408), description: Some("Bad Request".into()), ..Message::default() };
        assert!(matches!(kind(batch_answer(refused)), ErrorKind::Status { status: 408, .. }));
        assert!(matches!(kind(batch_answer(Message::new("x", "{}"))), ErrorKind::Malformed(_)), "no sequence");
    }

    /// The stream's headers a writer stored under other cases of their names, and a time under the stream's own.
    fn decoys(message: Message) -> Message {
        message
            .header("nats-stream", "KV_ants_read")
            .header("NATS-STREAM", "KV_ants_read")
            .header("nats-subject", "ants.inbox.alice.dev-b")
            .header("Nats-subject", "ants.inbox.alice.dev-b")
            .header("nats-sequence", "1")
            .header("NATS-Sequence", "2")
            .header("nats-time-stamp", "2020-01-01T00:00:00Z")
            .header("Nats-Time-Stamp", "2020-01-01T00:00:00Z")
    }

    #[test]
    fn a_stored_message_s_own_headers_under_the_stream_s_names_are_never_read_as_the_stream_s() {
        let own = decoys(Message::new("_INBOX.alice.x", r#"{"text":"hi"}"#)).header("Nats-Msg-Id", "m1");
        let answer = Message { headers: [own.headers, got("7", "").headers].concat(), ..own };
        let Got::Stored(stored) = batch_answer(answer.clone()).unwrap() else { panic!("a stored message") };
        assert_eq!((stored.seq, stored.subject.as_str()), (7, "ants.inbox.alice.dev-a"), "where the stream has it");
        assert_eq!(stored.stored_at.to_rfc3339(), "2026-11-20T10:00:00.500+00:00");
        assert_eq!(stored.headers, [("Nats-Msg-Id".to_owned(), "m1".to_owned())]);

        let entry = kv_entry("reading", answer.clone()).unwrap();
        assert_eq!(entry.revision, 7);

        let twice = answer.header("Nats-Sequence", "9");
        assert_eq!(batch_answer(twice).unwrap(), Got::Relayed(9), "the stream's own name twice, its value the last");
    }

    #[test]
    fn an_envelope_is_published_under_its_id_and_stored_once_the_stream_acks() {
        let ask = Envelope::new(p("alice"), p("dev-a"), "fix it");
        let message = publish(&ask).unwrap();
        assert_eq!(message.subject, "ants.inbox.dev-a.alice");
        assert_eq!(headers(&message), [("Nats-Msg-Id", ask.id.to_string().as_str())]);
        assert_eq!(serde_json::from_slice::<Envelope>(&message.payload).unwrap(), ask);

        let ack = Message::new("r", r#"{"stream":"ANTS","domain":"hub","seq":12}"#);
        let expected = Ack { stream: "ANTS".into(), seq: 12, domain: Some("hub".into()), duplicate: false };
        assert_eq!(stored("sending", &ack).unwrap(), expected);
        let again = Message::new("r", r#"{"stream":"ANTS","seq":12,"duplicate":true}"#);
        assert!(stored("sending", &again).unwrap().duplicate);
        let refused = Message::new("r", r#"{"error":{"code":503,"err_code":10077,"description":"full"}}"#);
        assert!(matches!(kind(stored("sending", &refused)), ErrorKind::Refused { code: 503, err_code: 10077, .. }));
        assert!(matches!(kind(stored("sending", &Message::new("r", "{}"))), ErrorKind::Malformed(_)));
        assert!(matches!(kind(stored("sending", &Message::new("r", ""))), ErrorKind::Malformed(_)));
    }

    #[test]
    fn a_thread_s_owner_is_recorded_once_under_the_record_s_id() {
        let (thread, hub): (ThreadId, JsDomain) = ("t1".parse().unwrap(), "hub".parse().unwrap());
        let record = owner_record(None, &thread, &p("dev-a"), &p("alice"));
        assert_eq!(record.subject, "$KV.ants_owners.t1.dev-a");
        assert_eq!(headers(&record), [("Nats-Msg-Id", "owner.t1.dev-a")]);
        assert_eq!(record.payload, b"alice");
        let in_domain = owner_record(Some(&hub), &thread, &p("dev-a"), &p("alice"));
        assert_eq!(in_domain.subject, "$JS.hub.API.$KV.ants_owners.t1.dev-a");

        assert!(owner_recorded(&Message::new("r", r#"{"stream":"KV_ants_owners","seq":1}"#)).is_ok());
        assert!(owner_recorded(&Message::new("r", r#"{"stream":"KV_ants_owners","seq":1,"duplicate":true}"#)).is_ok());
        let second = r#"{"error":{"code":503,"err_code":10077,"description":"maximum messages per subject exceeded"}}"#;
        assert!(owner_recorded(&Message::new("r", second)).is_ok(), "the first record stays");
        let full = r#"{"error":{"code":503,"err_code":10047,"description":"insufficient storage resources"}}"#;
        assert!(owner_recorded(&Message::new("r", full)).is_err());
    }

    #[test]
    fn the_read_marker_is_read_by_a_direct_get_and_put_by_compare_and_set() {
        let alice = p("alice");
        assert_eq!(marker_get(None, &alice).subject, "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice");
        let marker = ReadMarker { below: 7, ..ReadMarker::default() };
        let put = marker_put(None, &alice, &marker, 41);
        assert_eq!(put.subject, "$KV.ants_read.alice");
        assert_eq!(headers(&put), [("Nats-Expected-Last-Subject-Sequence", "41")]);
        assert_eq!(serde_json::from_slice::<ReadMarker>(&put.payload).unwrap(), marker);

        let none = Message { status: Some(404), ..Message::default() };
        assert_eq!(kv_entry("reading", none).unwrap(), KvEntry { value: None, revision: 0 });
        let entry = kv_entry("reading", got("41", r#"{"below":7}"#)).unwrap();
        assert_eq!(entry, KvEntry { value: Some(br#"{"below":7}"#.to_vec()), revision: 41 });
        let deleted = kv_entry("reading", got("42", "").header("KV-Operation", "DEL")).unwrap();
        assert_eq!(deleted, KvEntry { value: None, revision: 42 });
        let twice = got("42", "").header("KV-Operation", "PUT").header("KV-Operation", "DEL");
        assert!(
            kv_entry("reading", twice).unwrap().value.is_some(),
            "its writer's first value, as the daemons read it"
        );
        let relayed = got("43", r#"{"below":9}"#).header("Nats-Sequence", "5");
        assert!(matches!(kind(kv_entry("reading", relayed)), ErrorKind::Relayed));

        assert_eq!(kv_updated("marking", &Message::new("r", r#"{"stream":"KV_ants_read","seq":44}"#)), Ok(Some(44)));
        let moved = r#"{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 43"}}"#;
        assert_eq!(kv_updated("marking", &Message::new("r", moved)), Ok(None), "another device moved it first");
        let full = r#"{"error":{"code":503,"err_code":10047,"description":"insufficient storage resources"}}"#;
        assert!(kv_updated("marking", &Message::new("r", full)).is_err());
    }

    #[test]
    fn what_is_heard_live_is_taken_only_as_its_subject_names_it_and_never_from_a_stream() {
        let (dev_a, thread): (UserId, ThreadId) = (p("dev-a"), "t1".parse().unwrap());
        let update = ants_proto::StatusUpdate::State { state: ants_proto::State::Working };
        let event = StatusEvent::new(dev_a.clone(), thread, update);
        let payload = serde_json::to_vec(&event).unwrap();
        let live = Message::new(event.subject(), payload.clone());
        assert_eq!(status_event(&live), Some(event));
        assert_eq!(status_event(&Message::new("ants.status.dev-b.t1", payload)), None, "another ant's subject");
        assert_eq!(status_event(&live.clone().header("Nats-Stream", "ANTS")), None, "a stream sent it");
        assert_eq!(status_event(&Message::new(live.subject, "not json")), None);

        let reply = Envelope::new(dev_a, p("alice"), "done");
        let payload = serde_json::to_vec(&reply).unwrap();
        let live = Message::new(reply.subject(), payload.clone());
        assert_eq!(heard(&live), Some(reply));
        assert_eq!(heard(&Message::new("ants.inbox.alice.dev-b", payload)), None, "a forged sender");
        let delivered = Message { reply: Some("$JS.ACK.ANTS.c.1.7.7.0.0".into()), ..live.clone() };
        assert_eq!(heard(&delivered), None, "a consumer's delivery");
        assert_eq!(heard(&Message::new(live.subject, r#"{"stream":"ANTS","seq":2}"#)), None, "no envelope");
    }
}
