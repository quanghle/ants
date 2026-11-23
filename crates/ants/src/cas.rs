//! Compare-and-set of an entry of a KV bucket read, changed and stored again: whether the store lost to another
//! writer's, told by the kind of its error, so that the caller reads the entry again and retries. A
//! [`DirectKv`](ants_daemon::records::direct_kv::DirectKv) tells the same of its own update.

use async_nats::jetstream::kv::{self, CreateError, CreateErrorKind, UpdateError, UpdateErrorKind};

/// Store `value` under `key` of `kv` while its entry is at `revision`: false when another writer stored one meanwhile.
pub(crate) async fn update(kv: &kv::Store, key: &str, value: Vec<u8>, revision: u64) -> Result<bool, UpdateError> {
    match kv.update(key, value.into(), revision).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => Ok(false),
        Err(e) => Err(e),
    }
}

/// Store `value` under `key` of `kv` while it has no entry: false when another writer stored one meanwhile.
pub(crate) async fn create(kv: &kv::Store, key: &str, value: Vec<u8>) -> Result<bool, CreateError> {
    match kv.create(key, value.into()).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == CreateErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}
