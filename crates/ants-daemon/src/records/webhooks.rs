//! Which senders are webhooks. A webhook's credentials alone write its marker in [`WEBHOOKS_BUCKET`]
//! ([`webhook_marker_key`]), put by `ants webhook serve` before it delivers: a message marked as a webhook's
//! ([`ants_proto::Envelope::webhook`]) is taken as one only from a sender with a marker there.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ants_proto::{UserId, WEBHOOKS_BUCKET, webhook_marker_key};
use async_nats::jetstream;

use crate::records::direct_kv::{DirectKv, Lookup};
use crate::sync::MutexExt as _;

/// How long a marker found is trusted before it is read again.
const KEEP: Duration = Duration::from_hours(1);
/// Most webhooks kept in memory; past it they are read again.
const MAX_KEPT: usize = 1024;

pub struct Webhooks {
    js: jetstream::Context,
    /// The webhooks found, with when.
    found: Mutex<HashMap<UserId, Instant>>,
}

impl Webhooks {
    pub fn new(js: jetstream::Context) -> Self {
        Self { js, found: Mutex::default() }
    }

    /// Whether `sender` is a webhook: its marker is in the webhooks bucket. A marker that cannot be read counts as
    /// none.
    pub async fn is_webhook(&self, sender: &UserId) -> bool {
        self.marked(sender).await.unwrap_or(false)
    }

    /// Whether `sender` is a webhook: its marker is in the webhooks bucket; an error when the marker cannot be read.
    pub async fn marked(&self, sender: &UserId) -> Result<bool, String> {
        if self.found.locked().get(sender).is_some_and(|at| at.elapsed() < KEEP) {
            return Ok(true);
        }
        let found = self.read(sender).await.inspect_err(|error| {
            tracing::warn!(
                %sender,
                error,
                "reading the webhook's marker; with credentials, `ants auth user` (or `ants auth leaf`) as the \
                 human issues them again with the right to read it"
            );
        })?;
        let mut kept = self.found.locked();
        if found {
            if kept.len() >= MAX_KEPT {
                kept.clear();
            }
            kept.insert(sender.clone(), Instant::now());
        } else {
            kept.remove(sender);
        }
        Ok(found)
    }

    async fn read(&self, sender: &UserId) -> Result<bool, String> {
        let bucket = DirectKv::new(&self.js, None, WEBHOOKS_BUCKET).await.map_err(|e| format!("{e:#}"))?;
        match bucket.lookup(webhook_marker_key(sender)).await {
            Ok(Lookup::Value(marker)) => Ok(!marker.is_empty()),
            Ok(Lookup::NoValue) => Ok(false),
            Ok(Lookup::NoBucket) => Err(format!("there is no bucket {WEBHOOKS_BUCKET}")),
            Err(e) => Err(format!("{e:#}")),
        }
    }
}
