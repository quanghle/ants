//! Where each thread's origin, as the daemon settled it ([`super::context::settled`]), is kept, so a restarted daemon
//! carries on with it: a thread a webhook reached stays untrusted, and the provider's tools are called with the origin
//! before the thread's next message.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ants_proto::provider::Origin;
use ants_proto::{ThreadId, UserId, thread_origin_key};
use anyhow::{Context as _, Result};

use crate::records::direct_kv::DirectKv;
use crate::sync::MutexExt as _;

/// The origins of an ant's threads.
#[derive(Clone)]
pub enum OriginStore {
    /// The KV bucket [`ants_proto::SESSIONS_BUCKET`], under the ant's own keys ([`thread_origin_key`]), as JSON.
    Kv { kv: Box<DirectKv>, ant: UserId },
    /// In this process only, shared by clones.
    Memory(Arc<Mutex<HashMap<ThreadId, Origin>>>),
}

impl OriginStore {
    pub fn kv(kv: DirectKv, ant: UserId) -> Self {
        Self::Kv { kv: Box::new(kv), ant }
    }

    pub fn memory() -> Self {
        Self::Memory(Arc::default())
    }

    /// The origin `thread` settled on, if it has one.
    pub async fn get(&self, thread: &ThreadId) -> Result<Option<Origin>> {
        match self {
            Self::Kv { kv, ant } => {
                let Some(value) =
                    kv.get(thread_origin_key(ant, thread)).await.context("reading the origin of the thread")?
                else {
                    return Ok(None);
                };
                serde_json::from_slice(&value).context("reading the origin of the thread").map(Some)
            }
            Self::Memory(map) => Ok(map.locked().get(thread).cloned()),
        }
    }

    pub async fn put(&self, thread: &ThreadId, origin: &Origin) -> Result<()> {
        match self {
            Self::Kv { kv, ant } => kv
                .put(thread_origin_key(ant, thread), serde_json::to_vec(origin)?)
                .await
                .context("storing the origin of the thread"),
            Self::Memory(map) => {
                map.locked().insert(thread.clone(), origin.clone());
                Ok(())
            }
        }
    }

    /// Forget the thread's origin, as a failed clone order does with those it stored.
    pub async fn delete(&self, thread: &ThreadId) -> Result<()> {
        match self {
            Self::Kv { kv, ant } => kv.delete(thread_origin_key(ant, thread)).await,
            Self::Memory(map) => {
                map.locked().remove(thread);
                Ok(())
            }
        }
    }
}
