//! KV buckets read by direct gets and written by plain publishes alone. A `kv::Store` asks for its bucket's stream info
//! (`STREAM.INFO`) on opening, and credentials allowing that allow listing every subject of the stream, which is every
//! key of the bucket. So an ant reads and writes its own keys of [`ants_proto::SESSIONS_BUCKET`] and
//! [`ants_proto::ATTEMPTS_BUCKET`], and reads [`ants_proto::OWNERS_BUCKET`], through a [`DirectKv`], and its credentials
//! allow no stream info of these buckets. A webhook reads and records the events it delivered, under its own keys of
//! [`ants_proto::WEBHOOKS_BUCKET`], the same way.

use std::time::{Duration, SystemTime};

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream;
use async_nats::jetstream::context::{PublishError, PublishErrorKind};
use async_nats::jetstream::stream::{DirectGetErrorKind, Stream};
use async_nats::{HeaderMap, StatusCode};
use futures::TryStreamExt as _;

/// The header marking a KV delete or purge, and its values.
const KV_OPERATION: &str = "KV-Operation";
const DELETED: [&str; 2] = ["DEL", "PURGE"];
/// The header making the stream refuse a message unless the last on its subject has the given sequence (0: none).
const EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";
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
        Ok(Self { js: js.clone(), stream, bucket: bucket.to_owned(), api })
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
        let key = key.as_ref();
        let msg = match self.stream.direct_get_last_for_subject(self.subject(key)).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => return Ok((Lookup::NoValue, 0, None)),
            Err(e) if matches!(e.kind(), DirectGetErrorKind::ErrorResponse(StatusCode::NO_RESPONDERS, _)) => {
                return Ok((Lookup::NoBucket, 0, None));
            }
            Err(e) => return Err(e).with_context(|| format!("reading {key}")),
        };
        let found = if is_deleted(&msg.headers) { Lookup::NoValue } else { Lookup::Value(msg.payload.to_vec()) };
        Ok((found, msg.sequence, Some(msg.time.into())))
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
        let deleted = match self.stream.direct_get_last_for_subject(self.subject(key)).await {
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
        let key = key.as_ref();
        let mut headers = HeaderMap::new();
        headers.insert(KV_OPERATION, DELETED[0]);
        headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, last.to_string().as_str());
        if let Some(ttl) = ttl {
            headers.insert(TTL, ttl.as_secs().max(1).to_string().as_str());
        }
        let deleted =
            async { self.js.publish_with_headers(self.put_subject(key), headers, Vec::new().into()).await?.await };
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

    fn subject(&self, key: &str) -> String {
        format!("$KV.{}.{key}", self.bucket)
    }

    fn put_subject(&self, key: &str) -> String {
        format!("{}{}", self.api, self.subject(key))
    }
}

fn is_deleted(headers: &HeaderMap) -> bool {
    headers.get(KV_OPERATION).is_some_and(|op| DELETED.contains(&op.as_str()))
}
