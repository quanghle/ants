//! Which senders are webhooks. A webhook's credentials alone write its marker in [`WEBHOOKS_BUCKET`]
//! ([`webhook_marker_key`]), put by `ants webhook serve` before it delivers: a message marked as a webhook's
//! ([`ants_proto::Envelope::webhook`]) is taken as one only from a sender with a marker there.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ants_proto::{UserId, WEBHOOKS_BUCKET, webhook_marker_subject};
use async_nats::jetstream::{self, stream::DirectGetErrorKind};

/// How long a marker found is trusted before it is read again.
const KEEP: Duration = Duration::from_secs(60 * 60);
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
        if self.found.lock().expect("webhooks").get(sender).is_some_and(|at| at.elapsed() < KEEP) {
            return true;
        }
        let found = match self.read(sender).await {
            Ok(found) => found,
            Err(error) => {
                tracing::warn!(
                    %sender,
                    error,
                    "reading the webhook's marker; with credentials, `ants auth user` (or `ants auth leaf`) as the \
                     human issues them again with the right to read it"
                );
                false
            }
        };
        let mut kept = self.found.lock().expect("webhooks");
        if found {
            if kept.len() >= MAX_KEPT {
                kept.clear();
            }
            kept.insert(sender.clone(), Instant::now());
        } else {
            kept.remove(sender);
        }
        found
    }

    async fn read(&self, sender: &UserId) -> Result<bool, String> {
        let bucket = match self.js.get_stream_no_info(format!("KV_{WEBHOOKS_BUCKET}")).await {
            Ok(bucket) => bucket,
            Err(e) => return Err(e.to_string()),
        };
        match bucket.direct_get_last_for_subject(webhook_marker_subject(sender)).await {
            // A deleted or purged marker is stored empty.
            Ok(marker) => Ok(!marker.payload.is_empty()),
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.to_string()),
        }
    }
}
