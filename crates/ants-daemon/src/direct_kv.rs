//! KV buckets read by direct gets and written by plain publishes alone. A `kv::Store` asks for its bucket's stream info
//! (`STREAM.INFO`) on opening, and credentials allowing that allow listing every subject of the stream, which is every
//! key of the bucket. So an ant reads and writes its own keys of [`ants_proto::SESSIONS_BUCKET`] and
//! [`ants_proto::ATTEMPTS_BUCKET`], and reads [`ants_proto::OWNERS_BUCKET`], through a [`DirectKv`], and its credentials
//! allow no stream info of these buckets.

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream;
use async_nats::jetstream::stream::{DirectGetErrorKind, Stream};
use async_nats::{HeaderMap, StatusCode};

/// The header marking a KV delete or purge, and its values.
const KV_OPERATION: &str = "KV-Operation";
const DELETED: [&str; 2] = ["DEL", "PURGE"];

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
        let api = js_domain.map(|domain| format!("$JS.{domain}.API.")).unwrap_or_default();
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
        let key = key.as_ref();
        let msg = match self.stream.direct_get_last_for_subject(self.subject(key)).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => return Ok(Lookup::NoValue),
            Err(e) if matches!(e.kind(), DirectGetErrorKind::ErrorResponse(StatusCode::NO_RESPONDERS, _)) => {
                return Ok(Lookup::NoBucket);
            }
            Err(e) => return Err(e).with_context(|| format!("reading {key}")),
        };
        let deleted = msg.headers.get(KV_OPERATION).is_some_and(|op| DELETED.contains(&op.as_str()));
        Ok(if deleted { Lookup::NoValue } else { Lookup::Value(msg.payload.to_vec()) })
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

    /// Delete `key`, leaving a delete marker, as `kv::Store::delete` does.
    pub async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();
        let mut headers = HeaderMap::new();
        headers.insert(KV_OPERATION, DELETED[0]);
        let ack = self.js.publish_with_headers(self.put_subject(key), headers, Vec::new().into()).await;
        ack.with_context(|| format!("deleting {key}"))?.await.with_context(|| format!("deleting {key}"))?;
        Ok(())
    }

    fn subject(&self, key: &str) -> String {
        format!("$KV.{}.{key}", self.bucket)
    }

    fn put_subject(&self, key: &str) -> String {
        format!("{}{}", self.api, self.subject(key))
    }
}
