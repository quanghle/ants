//! KV buckets read by direct gets and written by plain publishes alone. A `kv::Store` asks for its bucket's stream info
//! (`STREAM.INFO`) on opening, and credentials allowing that allow listing every subject of the stream, which is every
//! key of the bucket. So an ant reads and writes its own keys of [`ants_proto::SESSIONS_BUCKET`] and
//! [`ants_proto::ATTEMPTS_BUCKET`], and reads [`ants_proto::OWNERS_BUCKET`], through a [`DirectKv`], and its credentials
//! allow no stream info of these buckets. A webhook reads and records the events it delivered, under its own keys of
//! [`ants_proto::WEBHOOKS_BUCKET`], the same way.
//!
//! A stream may store a message it sent, rather than a writer put, on any key ([`crate::bus::relayed`]). Every read
//! skips such a message. The readers of a bucket keeping each key's earlier messages, as the trails' buckets do
//! ([`crate::hub`]), read past it to the last message the key's writer put ([`DirectKv::past_relays`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::context::{PublishError, PublishErrorKind};
use async_nats::jetstream::message::StreamMessage;
use async_nats::jetstream::stream::{DirectGetError, DirectGetErrorKind, Stream};
use async_nats::jetstream::{self, consumer};
use async_nats::{HeaderMap, RequestErrorKind, StatusCode};
use futures::{StreamExt as _, TryStreamExt as _};

use crate::bus::relayed::{self, Read};

/// The header marking a KV delete or purge, and its values.
const KV_OPERATION: &str = "KV-Operation";
const DELETED: [&str; 2] = ["DEL", "PURGE"];
/// The header making the stream refuse a message unless the last on its subject has the given sequence (0: none).
const EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";
/// The header making the stream refuse a message unless its last message has the given sequence.
const EXPECTED_LAST_SEQUENCE: &str = "Nats-Expected-Last-Sequence";
/// The header of the messages left of a batched direct get, on the status ending each batch.
const NUM_PENDING: &str = "Nats-Num-Pending";
/// Messages a [`DirectKv::scan`] asks for at once, and the bytes they come to at most (a larger message comes alone).
const BATCH: usize = 256;
const BATCH_BYTES: usize = 4 << 20;
/// How often [`DirectKv::put_sequenced`] tries while other keys keep changing.
const SEQUENCED_TRIES: usize = 10;
/// The header making the stream remove a message after the given number of seconds, if it allows per-message TTLs.
const TTL: &str = "Nats-TTL";
/// How long a reader consumer left behind by an interrupted read past a relayed message lives on.
const READER_INACTIVE: Duration = Duration::from_secs(30);

/// A KV bucket of history 1 that allows direct gets, as [`crate::bus`] creates them.
#[derive(Clone)]
pub struct DirectKv {
    js: jetstream::Context,
    stream: Stream<()>,
    bucket: String,
    /// What puts and deletes go through: the JetStream API of the hub's domain, if it has one, as with `kv::Store`.
    api: String,
    /// Whether reads name their key in the request rather than in the subject ([`DirectKv::whole`]).
    whole: bool,
    /// How a read of a key whose last message a stream relayed finds the last one its writer put; None: it fails.
    past: Option<Past>,
}

/// How a read finds a key's last message its writer put, past those a stream relayed.
#[derive(Clone)]
enum Past {
    /// By batched direct gets naming no subject ([`DirectKv::whole`]).
    Gets,
    /// Through the reader consumer named, its filter pinned to the user's own keys by its credentials, created for the
    /// read and deleted after it, one read at a time.
    Consumer { reader: String, held: Arc<tokio::sync::Mutex<()>> },
}

/// A message a writer put under a key.
struct Put {
    payload: Vec<u8>,
    headers: HeaderMap,
    time: SystemTime,
}

/// What a direct get of a key found.
pub enum Lookup {
    Value(Vec<u8>),
    /// The key has no value, or was deleted.
    NoValue,
    NoBucket,
}

impl DirectKv {
    /// The bucket `bucket`, without a request: whether it exists shows on the first read. `js_domain` is the domain `js`
    /// was made for ([`crate::bus::jetstream`]).
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>, bucket: &str) -> Result<Self> {
        let stream = js.get_stream_no_info(format!("KV_{bucket}")).await.with_context(|| format!("naming {bucket}"))?;
        let api = js_domain.map_or_default(|domain| format!("$JS.{domain}.API."));
        Ok(Self { js: js.clone(), stream, bucket: bucket.to_owned(), api, whole: false, past: None })
    }

    /// [`DirectKv::new`], for credentials that may read the whole bucket by direct gets naming no subject
    /// (`$JS.API.DIRECT.GET.KV_<bucket>`) rather than its keys one by one: such a get names its key in the request.
    pub async fn whole(js: &jetstream::Context, js_domain: Option<&JsDomain>, bucket: &str) -> Result<Self> {
        Ok(Self { whole: true, ..Self::new(js, js_domain, bucket).await? })
    }

    /// For a bucket keeping each key's earlier messages, read [`DirectKv::whole`]: a key whose last message a stream
    /// relayed reads as the last message its writer put, found by batched direct gets.
    #[must_use]
    pub fn past_relays(self) -> Self {
        Self { past: Some(Past::Gets), ..self }
    }

    /// [`DirectKv::past_relays`] for credentials reading their own keys one by one: through the consumer `reader`,
    /// which they may create on the bucket's stream filtering their own keys, take messages from and delete.
    #[must_use]
    pub fn past_relays_through(self, reader: String) -> Self {
        Self { past: Some(Past::Consumer { reader, held: Arc::default() }), ..self }
    }

    /// Whether the bucket exists, found by reading a key of `ant`'s, which its credentials allow.
    pub async fn exists(&self, ant: &UserId) -> Result<bool> {
        Ok(!matches!(self.lookup(format!("{ant}.probe")).await?, Lookup::NoBucket))
    }

    /// The value of `key`; None when it has none or was deleted. Fails when the bucket does not exist.
    pub async fn get(&self, key: impl AsRef<str>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        match self.lookup(key).await? {
            Lookup::Value(value) => Ok(Some(value)),
            Lookup::NoValue => Ok(None),
            Lookup::NoBucket => Err(anyhow!("reading {key}: there is no bucket {}", self.bucket)),
        }
    }

    pub async fn lookup(&self, key: impl AsRef<str>) -> Result<Lookup> {
        Ok(self.lookup_last(key).await?.0)
    }

    /// [`DirectKv::lookup`], with the stream sequence of the key's last message (0 for none), which
    /// [`DirectKv::update`] expects.
    pub async fn lookup_last(&self, key: impl AsRef<str>) -> Result<(Lookup, u64)> {
        let (found, last, _) = self.lookup_stored(key).await?;
        Ok((found, last))
    }

    /// [`DirectKv::lookup_last`], with when the key's last message was stored (None for none).
    pub async fn lookup_stored(&self, key: impl AsRef<str>) -> Result<(Lookup, u64, Option<SystemTime>)> {
        let (found, last, at, _) = self.lookup_with_marker(key, &[]).await?;
        Ok((found, last, at))
    }

    /// As [`DirectKv::lookup`], also telling when a delete with the given marker was stored.
    pub async fn lookup_marked(&self, key: impl AsRef<str>, marker: &[u8]) -> Result<(Lookup, Option<SystemTime>)> {
        let (found, _, at, marked) = self.lookup_with_marker(key, marker).await?;
        Ok((found, at.filter(|_| marked)))
    }

    async fn lookup_with_marker(
        &self,
        key: impl AsRef<str>,
        marker: &[u8],
    ) -> Result<(Lookup, u64, Option<SystemTime>, bool)> {
        let key = key.as_ref();
        let msg = match self.last(key).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => return Ok((Lookup::NoValue, 0, None, false)),
            Err(e) if matches!(e.kind(), DirectGetErrorKind::ErrorResponse(StatusCode::NO_RESPONDERS, _)) => {
                return Ok((Lookup::NoBucket, 0, None, false));
            }
            Err(e) => return Err(e).with_context(|| format!("reading {key}")),
        };
        let put = if relayed::stored_relayed(Some(&msg.headers), Read::DirectGet) {
            let Some(past) = &self.past else {
                bail!("reading {key}: its entry is a message a stream relayed, not one its writer put");
            };
            tracing::warn!(key, bucket = self.bucket, "reading past a message a stream relayed onto the key");
            Box::pin(self.last_put(past, key)).await.with_context(|| format!("reading {key} past a relayed message"))?
        } else {
            Some(Put { payload: msg.payload.to_vec(), headers: msg.headers, time: msg.time.into() })
        };
        // The sequence stays the key's last message's, a relayed one too, which a write expecting it expects.
        let Some(put) = put else { return Ok((Lookup::NoValue, msg.sequence, None, false)) };
        let marked = !marker.is_empty()
            && put.headers.get(KV_OPERATION).is_some_and(|op| op.as_str() == DELETED[0])
            && put.payload == marker;
        let found = if is_deleted(&put.headers) { Lookup::NoValue } else { Lookup::Value(put.payload) };
        Ok((found, msg.sequence, Some(put.time), marked))
    }

    /// The last message its writer put under `key`, past those a stream relayed; None when there is none.
    async fn last_put(&self, past: &Past, key: &str) -> Result<Option<Put>> {
        match past {
            Past::Gets => {
                let mut last = None;
                self.each_stored(key, |msg, relayed| {
                    if !relayed {
                        last = Some(Put { payload: msg.payload.to_vec(), headers: msg.headers, time: msg.time.into() });
                    }
                })
                .await?;
                Ok(last)
            }
            Past::Consumer { reader, held } => {
                let _held = held.lock().await;
                self.last_put_through(reader, key).await
            }
        }
    }

    /// [`DirectKv::last_put`] through the consumer `reader`, created filtering `key` alone and deleted after.
    async fn last_put_through(&self, reader: &str, key: &str) -> Result<Option<Put>> {
        let stream = format!("KV_{}", self.bucket);
        // A consumer left by an interrupted read filters another key; as a rule none is left to delete.
        let _ = self.js.delete_consumer_from_stream(reader, &stream).await;
        let config = consumer::pull::Config {
            name: Some(reader.to_owned()),
            filter_subject: self.subject(key),
            deliver_policy: consumer::DeliverPolicy::All,
            ack_policy: consumer::AckPolicy::None,
            inactive_threshold: READER_INACTIVE,
            memory_storage: true,
            ..Default::default()
        };
        let consumer = self.js.create_consumer_on_stream(config, &stream).await.context("creating the reader")?;
        let read = async {
            let (mut left, mut last) = (consumer.cached_info().num_pending, None);
            while left > 0 {
                let mut batch = consumer.fetch().max_messages(left.min(BATCH as u64) as usize).messages().await?;
                let mut got = 0;
                while let Some(msg) = batch.next().await {
                    let msg = msg.map_err(|e| anyhow!(e))?;
                    got += 1;
                    let Ok(info) = msg.info() else { continue };
                    if !relayed::stored_relayed(msg.headers.as_ref(), Read::Consumer) {
                        let time = SystemTime::from(info.published);
                        let headers = msg.headers.clone().unwrap_or_default();
                        last = Some(Put { payload: msg.payload.to_vec(), headers, time });
                    }
                }
                if got == 0 {
                    break;
                }
                left = left.saturating_sub(got);
            }
            Ok(last)
        }
        .await;
        if let Err(e) = self.js.delete_consumer_from_stream(reader, &stream).await {
            tracing::debug!(error = %e, consumer = reader, "deleting the reader");
        }
        read
    }

    /// The last message of `key`, by a direct get naming it in its subject, or in its request for [`DirectKv::whole`].
    async fn last(&self, key: &str) -> Result<StreamMessage, DirectGetError> {
        if !self.whole {
            return self.stream.direct_get_last_for_subject(self.subject(key)).await;
        }
        let request = serde_json::json!({ "last_by_subj": self.subject(key) }).to_string();
        let subject = format!("{}DIRECT.GET.KV_{}", self.request_api(), self.bucket);
        let msg = match self.js.client().request(subject, request.into()).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == RequestErrorKind::NoResponders => {
                let kind = DirectGetErrorKind::ErrorResponse(StatusCode::NO_RESPONDERS, String::new());
                return Err(DirectGetError::new(kind));
            }
            Err(e) => return Err(DirectGetError::with_source(DirectGetErrorKind::Other, e)),
        };
        match msg.status {
            None => StreamMessage::try_from(msg).map_err(|e| DirectGetError::with_source(DirectGetErrorKind::Other, e)),
            Some(StatusCode::NOT_FOUND) => Err(DirectGetError::new(DirectGetErrorKind::NotFound)),
            Some(status) => {
                Err(DirectGetError::new(DirectGetErrorKind::ErrorResponse(status, msg.description.unwrap_or_default())))
            }
        }
    }

    /// The JetStream API prefix of requests, `$JS.API.` or that of the hub's domain.
    fn request_api(&self) -> &str {
        if self.api.is_empty() { "$JS.API." } else { &self.api }
    }

    /// Store `value` under `key` if the key's last message is still the one at stream sequence `last` (0: none), as
    /// [`DirectKv::lookup_last`] found it. Whether it stored the value: false when the key changed meanwhile.
    pub async fn update(&self, key: impl AsRef<str>, value: impl Into<Vec<u8>>, last: u64) -> Result<bool> {
        let key = key.as_ref();
        match self.put_expecting(key, value.into(), last).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => Ok(false),
            Err(e) => Err(e).with_context(|| format!("storing {key}")),
        }
    }

    /// Store under `key` the value `value` makes for the stream sequence it is to be stored at, so that it is stored at
    /// that sequence or not at all, if the key's last message is still the one at stream sequence `last` (0: none), as
    /// [`DirectKv::lookup_last`] found it; with no `last`, whatever the key holds. Whether it stored the value: false
    /// when the key changed meanwhile.
    ///
    /// Each try expects the stream's last message at the sequence before; a refusal tells the stream's last sequence,
    /// for the next try. A value made for a sequence it was refused at is never stored there afterwards: the stream
    /// holds another message there by then, or the key changed, which is checked before trying again.
    pub async fn put_sequenced(
        &self,
        key: impl AsRef<str>,
        last: Option<u64>,
        mut value: impl FnMut(u64) -> Result<Vec<u8>>,
    ) -> Result<bool> {
        let key = key.as_ref();
        let mut stream_last = last.unwrap_or_default();
        for _ in 0..SEQUENCED_TRIES {
            let mut headers = HeaderMap::new();
            headers.insert(EXPECTED_LAST_SEQUENCE, stream_last.to_string().as_str());
            if let Some(last) = last {
                headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, last.to_string().as_str());
            }
            let payload = value(stream_last + 1)?;
            let stored =
                async { self.js.publish_with_headers(self.put_subject(key), headers, payload.into()).await?.await };
            let refused = match stored.await {
                Ok(_) => return Ok(true),
                Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => e,
                Err(e) => return Err(e).with_context(|| format!("storing {key}")),
            };
            // The stream tells the last sequence of the key when that was the one refused, which the key changing shows.
            if let Some(last) = last
                && self.lookup_last(key).await?.1 != last
            {
                return Ok(false);
            }
            stream_last = reported_last(&refused).with_context(|| format!("storing {key}: {refused:#}"))?;
        }
        Err(anyhow!("storing {key}: the bucket kept changing"))
    }

    /// Store `value` under `key`, once JetStream has it.
    pub async fn put(&self, key: impl AsRef<str>, value: impl Into<Vec<u8>>) -> Result<()> {
        let key = key.as_ref();
        let ack = self.js.publish(self.put_subject(key), value.into().into()).await;
        ack.with_context(|| format!("storing {key}"))?.await.with_context(|| format!("storing {key}"))?;
        Ok(())
    }

    /// [`DirectKv::put`] as message `id` (`Nats-Msg-Id`): the stream drops a repeat of it within its duplicate window.
    pub async fn put_as(&self, key: impl AsRef<str>, value: impl Into<Vec<u8>>, id: &str) -> Result<()> {
        let key = key.as_ref();
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", id);
        let ack = self.js.publish_with_headers(self.put_subject(key), headers, value.into().into()).await;
        ack.with_context(|| format!("storing {key}"))?.await.with_context(|| format!("storing {key}"))?;
        Ok(())
    }

    /// Store `value` under `key` unless it has a value, as `kv::Store::create` does: the stream refuses the put when the
    /// key's last message is not the one expected, none or its delete marker. Whether it stored the value.
    pub async fn create(&self, key: impl AsRef<str>, value: impl Into<Vec<u8>>) -> Result<bool> {
        let key = key.as_ref();
        let value = value.into();
        match self.put_expecting(key, value.clone(), 0).await {
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {}
            stored => return stored.map(|()| true).with_context(|| format!("storing {key}")),
        }
        let deleted = match self.last(key).await {
            Ok(msg) if is_deleted(&msg.headers) => msg.sequence,
            Ok(msg) if self.past.is_some() && relayed::stored_relayed(Some(&msg.headers), Read::DirectGet) => {
                match self.lookup_last(key).await? {
                    (Lookup::Value(_), _) => return Ok(false),
                    (_, last) => last,
                }
            }
            Ok(_) => return Ok(false),
            Err(e) => return Err(e).with_context(|| format!("reading {key}")),
        };
        match self.put_expecting(key, value, deleted).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => Ok(false),
            Err(e) => Err(e).with_context(|| format!("storing {key}")),
        }
    }

    async fn put_expecting(&self, key: &str, value: Vec<u8>, last: u64) -> Result<(), PublishError> {
        let mut headers = HeaderMap::new();
        headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, last.to_string().as_str());
        self.js.publish_with_headers(self.put_subject(key), headers, value.into()).await?.await?;
        Ok(())
    }

    /// Delete `key`, leaving a delete marker, as `kv::Store::delete` does.
    pub async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();
        let mut headers = HeaderMap::new();
        headers.insert(KV_OPERATION, DELETED[0]);
        let ack = self.js.publish_with_headers(self.put_subject(key), headers, Vec::new().into()).await;
        ack.with_context(|| format!("deleting {key}"))?.await.with_context(|| format!("deleting {key}"))?;
        Ok(())
    }

    /// Delete `key`, leaving a delete marker, if the key's last message is still the one at stream sequence `last`, as
    /// [`DirectKv::lookup_last`] found it. With `ttl`, the stream removes the marker that long after (at least a
    /// second), leaving the key no message, if it allows per-message TTLs, and refuses the delete otherwise. Whether
    /// it deleted the key: false when the key changed meanwhile.
    pub async fn delete_expecting(&self, key: impl AsRef<str>, last: u64, ttl: Option<Duration>) -> Result<bool> {
        self.delete_with_marker(key, last, ttl, &[]).await
    }

    /// As [`DirectKv::delete_expecting`], with an opaque marker as the delete's payload rather than a value.
    pub async fn delete_with_marker(
        &self,
        key: impl AsRef<str>,
        last: u64,
        ttl: Option<Duration>,
        marker: &[u8],
    ) -> Result<bool> {
        let key = key.as_ref();
        let mut headers = HeaderMap::new();
        headers.insert(KV_OPERATION, DELETED[0]);
        headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, last.to_string().as_str());
        if let Some(ttl) = ttl {
            headers.insert(TTL, ttl.as_secs().max(1).to_string().as_str());
        }
        let deleted =
            async { self.js.publish_with_headers(self.put_subject(key), headers, marker.to_vec().into()).await?.await };
        match deleted.await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => Ok(false),
            Err(e) => Err(e).with_context(|| format!("deleting {key}")),
        }
    }

    /// The keys matching `filter` that have a message, deleted ones included, from the stream's info: only for
    /// credentials allowed it (`STREAM.INFO`).
    pub async fn keys(&self, filter: &str) -> Result<Vec<String>> {
        let prefix = self.subject("");
        let mut subjects = self.stream.info_with_subjects(self.subject(filter)).await.context("listing keys")?;
        let mut keys = Vec::new();
        while let Some((subject, _)) = subjects.try_next().await.context("listing keys")? {
            keys.extend(subject.strip_prefix(&prefix).map(str::to_owned));
        }
        Ok(keys)
    }

    /// Hand `each` the key and value of every entry under the keys matching `filter`, as its writer last put it, in
    /// the order they were put, deleted entries left out: batched direct gets naming no subject, for credentials
    /// allowed them ([`DirectKv::whole`]). A message a stream relayed is skipped, leaving the key as its writer put it
    /// before, when the bucket keeps it. Fails when the bucket does not exist.
    pub async fn scan(&self, filter: &str, mut each: impl FnMut(&str, &[u8])) -> Result<()> {
        let prefix = self.subject("");
        let mut last: HashMap<String, (u64, Option<Vec<u8>>)> = HashMap::new();
        self.each_stored(filter, |msg, relayed| {
            if relayed {
                tracing::warn!(subject = %msg.subject, "skipping an entry a stream relayed");
            } else if let Some(key) = msg.subject.strip_prefix(prefix.as_str()) {
                let value = (!is_deleted(&msg.headers)).then(|| msg.payload.to_vec());
                last.insert(key.to_owned(), (msg.sequence, value));
            }
        })
        .await?;
        let mut entries: Vec<_> = last.into_iter().filter_map(|(key, (seq, value))| Some((seq, key, value?))).collect();
        entries.sort_unstable_by_key(|(seq, ..)| *seq);
        for (_, key, value) in entries {
            each(&key, &value);
        }
        Ok(())
    }

    /// Hand `each` every message stored under the keys matching `filter`, oldest first, and whether a stream relayed
    /// it: batched direct gets naming no subject ([`DirectKv::whole`]).
    async fn each_stored(&self, filter: &str, mut each: impl FnMut(StreamMessage, bool)) -> Result<()> {
        let bucket = &self.bucket;
        let client = self.js.client();
        let wait = client.timeout().unwrap_or(Duration::from_secs(10));
        let inbox = client.new_inbox();
        let mut replies = client.subscribe(inbox.clone()).await?;
        let mut seq = 1;
        loop {
            let request = serde_json::json!({
                "seq": seq,
                "next_by_subj": self.subject(filter),
                "batch": BATCH,
                "max_bytes": BATCH_BYTES,
            });
            let request = async_nats::Request::new().inbox(inbox.clone()).payload(request.to_string().into());
            self.js
                .send_request(format!("DIRECT.GET.KV_{bucket}"), request)
                .await
                .with_context(|| format!("reading {filter} of {bucket}"))?;
            let mut got = 0;
            loop {
                let Ok(Some(msg)) = tokio::time::timeout(wait, replies.next()).await else {
                    bail!("no answer reading {filter} of {bucket} (`ants init` creates it)");
                };
                match msg.status {
                    None => {}
                    Some(StatusCode::NOT_FOUND) => return Ok(()),
                    Some(StatusCode::NO_RESPONDERS) => bail!("there is no bucket {bucket}: `ants init` creates it"),
                    Some(status) if status.as_u16() == 204 => {
                        let pending = msg.headers.as_ref().and_then(|h| h.get(NUM_PENDING));
                        if got == 0 || pending.is_some_and(|n| n.as_str() == "0") {
                            return Ok(());
                        }
                        break;
                    }
                    Some(status) => bail!("reading {filter} of {bucket}: answered {status} {:?}", msg.description),
                }
                got += 1;
                let relayed = relayed::stored_relayed(msg.headers.as_ref(), Read::DirectGet);
                let msg = StreamMessage::try_from(msg).map_err(|e| anyhow!(e))?;
                seq = msg.sequence + 1;
                each(msg, relayed);
            }
        }
    }

    fn subject(&self, key: &str) -> String {
        format!("$KV.{}.{key}", self.bucket)
    }

    fn put_subject(&self, key: &str) -> String {
        format!("{}{}", self.api, self.subject(key))
    }
}

/// The last sequence the stream told refusing a publish that expected another.
fn reported_last(refused: &PublishError) -> Option<u64> {
    let refusal = std::error::Error::source(refused)?.downcast_ref::<jetstream::Error>()?;
    refusal.to_string().strip_prefix("wrong last sequence: ")?.split(' ').next()?.parse().ok()
}

fn is_deleted(headers: &HeaderMap) -> bool {
    headers.get(KV_OPERATION).is_some_and(|op| DELETED.contains(&op.as_str()))
}
