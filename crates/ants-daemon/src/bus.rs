//! NATS connection and the JetStream objects ants relies on.

use std::time::Duration;

use ants_proto::{Envelope, STREAM, STREAM_SUBJECTS};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, stream};

/// How long the stream keeps messages.
const RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Connect to NATS. `url` may be `nats://`, `tls://`, `ws://` or `wss://`, so the same binary works over a dev tunnel,
/// a reverse proxy or a direct WebSocket endpoint.
pub async fn connect(url: &str, name: &str) -> Result<(async_nats::Client, jetstream::Context)> {
    let client = async_nats::ConnectOptions::new()
        .name(name)
        .connect(url)
        .await
        .with_context(|| format!("connecting to NATS at {url}"))?;
    let js = jetstream::new(client.clone());
    Ok((client, js))
}

/// Create the `ANTS` stream, or return it as it is.
pub async fn ensure_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    js.get_or_create_stream(stream::Config {
        name: STREAM.to_owned(),
        subjects: STREAM_SUBJECTS.iter().map(|s| (*s).to_owned()).collect(),
        storage: stream::StorageType::File,
        max_age: RETENTION,
        ..Default::default()
    })
    .await
    .context("creating the ANTS stream")
}

/// Publish an envelope and wait until JetStream has stored it.
pub async fn publish(js: &jetstream::Context, envelope: &Envelope) -> Result<()> {
    let payload = serde_json::to_vec(envelope)?;
    js.publish(envelope.subject(), payload.into())
        .await
        .context("publishing")?
        .await
        .context("waiting for JetStream to store the message")?;
    Ok(())
}
