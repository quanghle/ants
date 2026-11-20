//! Reads of a `kv::Store` that take a message a stream stored on a key ([`stored_relayed`]) for no value of that key:
//! a read finds the key removed, and a watch sees a delete, at the message's revision, so a compare-and-set writer puts
//! its genuine value back over it. `kv::Store`'s own reads drop the headers that tell such a message, so these read
//! the key's last message by a direct get, which a watch does for each put it sees.

use std::str::FromStr as _;

use async_nats::header;
use async_nats::jetstream::kv::{Entry, EntryError, EntryErrorKind, Operation, Store, WatchError, WatcherError};
use async_nats::jetstream::message::StreamMessage;
use async_nats::jetstream::stream::{DirectGetError, DirectGetErrorKind};
use futures::StreamExt as _;
use futures::stream::BoxStream;
use hyper::body::Bytes;

use super::{Read, stored_relayed};

/// The header marking a KV delete or purge.
const KV_OPERATION: &str = "KV-Operation";

/// The changes a [`GuardedKv::guarded_watch`] sees.
pub type Watch = BoxStream<'static, Result<Entry, WatcherError>>;

/// The reads of a bucket that take what a stream relayed onto a key for no value.
pub trait GuardedKv: Sync {
    /// `kv::Store::entry`: the last entry of `key`, a delete for a message a stream relayed.
    fn guarded_entry(&self, key: &str) -> impl Future<Output = Result<Option<Entry>, EntryError>> + Send;

    /// `kv::Store::get`: the value of `key`, None when it has none, was deleted or a stream relayed its last message.
    fn guarded_get(&self, key: &str) -> impl Future<Output = Result<Option<Bytes>, EntryError>> + Send {
        async move {
            let entry = self.guarded_entry(key).await?;
            Ok(entry.filter(|entry| entry.operation == Operation::Put).map(|entry| entry.value))
        }
    }

    /// `kv::Store::watch`: the changes of the keys `key` matches from now on, a message a stream relayed seen as a
    /// delete. A put is checked by a direct get of its key, and skipped when the key moved on past it meanwhile: the
    /// change that moved it follows.
    fn guarded_watch(&self, key: &str) -> impl Future<Output = Result<Watch, WatchError>> + Send;
}

impl GuardedKv for Store {
    async fn guarded_entry(&self, key: &str) -> Result<Option<Entry>, EntryError> {
        match last(self, key).await {
            Ok(msg) => Ok(Some(entry_of(self, key.to_owned(), msg)?)),
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[expect(clippy::disallowed_methods, reason = "the guarded watch itself, which checks each put")]
    async fn guarded_watch(&self, key: &str) -> Result<Watch, WatchError> {
        let store = self.clone();
        let changes = self.watch(key).await?;
        Ok(changes.filter_map(move |change| checked(store.clone(), change)).boxed())
    }
}

/// `change`, a put checked against its key's last message: None when the key moved on past it.
async fn checked(store: Store, change: Result<Entry, WatcherError>) -> Option<Result<Entry, WatcherError>> {
    let change = match change {
        Ok(change) if change.operation == Operation::Put => change,
        other => return Some(other),
    };
    let failed = |e: String| WatcherError::with_source(async_nats::jetstream::kv::WatcherErrorKind::Other, e);
    let msg = match last(&store, &change.key).await {
        Ok(msg) if msg.sequence == change.revision => msg,
        Ok(_) => return None,
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => return None,
        Err(e) => return Some(Err(failed(format!("reading {} back: {e}", change.key)))),
    };
    let found = entry_of(&store, change.key.clone(), msg).map_err(|e| failed(e.to_string()));
    Some(found.map(|found| Entry { delta: change.delta, seen_current: change.seen_current, ..found }))
}

/// The last message stored under `key`.
async fn last(store: &Store, key: &str) -> Result<StreamMessage, DirectGetError> {
    store.stream.direct_get_last_for_subject(format!("{}{key}", store.prefix)).await
}

/// The entry of `msg`, read by a direct get of `key`: a delete with no value if a stream relayed it.
fn entry_of(store: &Store, key: String, msg: StreamMessage) -> Result<Entry, EntryError> {
    let relayed = stored_relayed(Some(&msg.headers), &msg.payload, Read::DirectGet);
    let (value, operation) = if relayed {
        tracing::warn!(bucket = store.name, key, "taking a message a stream relayed onto the key for no value");
        (Bytes::new(), Operation::Delete)
    } else {
        (msg.payload, operation(&msg.headers).map_err(|e| EntryError::with_source(EntryErrorKind::Other, e))?)
    };
    let (revision, created) = (msg.sequence, msg.time);
    Ok(Entry { bucket: store.name.clone(), key, value, revision, delta: 0, created, operation, seen_current: false })
}

/// The operation a stored message's headers name: a put without any.
fn operation(headers: &async_nats::HeaderMap) -> Result<Operation, String> {
    if let Some(op) = headers.get(KV_OPERATION) {
        return Operation::from_str(op.as_str()).map_err(|e| e.to_string());
    }
    match headers.get(header::NATS_MARKER_REASON).map(async_nats::HeaderValue::as_str) {
        None => Ok(Operation::Put),
        Some("MaxAge" | "Purge") => Ok(Operation::Purge),
        Some("Remove") => Ok(Operation::Delete),
        Some(reason) => Err(format!("invalid marker reason {reason}")),
    }
}
