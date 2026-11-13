//! The team's humans: the KV bucket [`HUMANS_BUCKET`], which only humans' credentials write and personas read.

use ants_proto::HUMANS_BUCKET;
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, kv, stream};

/// The humans bucket, created when missing and the credentials allow it (a human's). Entries never expire: a human
/// stays one until removed.
pub async fn ensure(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(HUMANS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: HUMANS_BUCKET.to_owned(),
        description: "Humans of the team, written by humans".to_owned(),
        history: 1,
        storage: stream::StorageType::File,
        ..Default::default()
    })
    .await
    .context("creating the humans bucket (with credentials, `ants init` as a human creates it)")
}
