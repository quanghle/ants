//! KV buckets read by direct gets and written by plain publishes alone. A `kv::Store` asks for its bucket's stream info
//! (`STREAM.INFO`) on opening, and credentials allowing that allow listing every subject of the stream, which is every
//! key of the bucket. So an ant reads and writes its own keys of [`ants_proto::SESSIONS_BUCKET`] and
//! [`ants_proto::ATTEMPTS_BUCKET`], and reads [`ants_proto::OWNERS_BUCKET`], through a [`DirectKv`], and its credentials
//! allow no stream info of these buckets. A webhook reads and records the events it delivered, under its own keys of
//! [`ants_proto::WEBHOOKS_BUCKET`], the same way.

use std::time::{Duration, SystemTime};

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use async_nats::jetstream::context::{PublishError, PublishErrorKind};
use async_nats::jetstream::message::StreamMessage;
use async_nats::jetstream::stream::{DirectGetError, DirectGetErrorKind, Stream};
use async_nats::{HeaderMap, RequestErrorKind, StatusCode};
use futures::{StreamExt as _, TryStreamExt as _};

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
        Ok(Self { js: js.clone(), stream, bucket: bucket.to_owned(), api, whole: false })
    }

    /// [`DirectKv::new`], for credentials that may read the whole bucket by direct gets naming no subject
    /// (`$JS.API.DIRECT.GET.KV_<bucket>`) rather than its keys one by one: such a get names its key in the request.
    pub async fn whole(js: &jetstream::Context, js_domain: Option<&JsDomain>, bucket: &str) -> Result<Self> {
        Ok(Self { whole: true, ..Self::new(js, js_domain, bucket).await? })
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
        let marked = !marker.is_empty()
            && msg.headers.get(KV_OPERATION).is_some_and(|op| op.as_str() == DELETED[0])
            && msg.payload.as_ref() == marker;
        let found = if is_deleted(&msg.headers) { Lookup::NoValue } else { Lookup::Value(msg.payload.to_vec()) };
        Ok((found, msg.sequence, Some(msg.time.into()), marked))
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

    /// Hand `each` the key and value of every entry under the keys matching `filter`, as last put, in the order
    /// they were put, deleted entries left out: batched direct gets naming no subject, for credentials allowed them
    /// ([`DirectKv::whole`]). Fails when the bucket does not exist.
    pub async fn scan(&self, filter: &str, mut each: impl FnMut(&str, &[u8])) -> Result<()> {
        let (bucket, prefix) = (&self.bucket, self.subject(""));
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
                let msg = StreamMessage::try_from(msg).map_err(|e| anyhow!(e))?;
                seq = msg.sequence + 1;
                if let Some(key) = msg.subject.strip_prefix(prefix.as_str()).filter(|_| !is_deleted(&msg.headers)) {
                    each(key, &msg.payload);
                }
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
