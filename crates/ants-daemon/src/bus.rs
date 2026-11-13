//! NATS connection and the JetStream objects ants relies on.

use std::path::Path;
use std::time::Duration;

use ants_proto::{Envelope, PersonaId, STREAM, STREAM_SUBJECTS, inbox_prefix, subject};
use anyhow::{Context as _, Result};
use async_nats::Event;
use async_nats::jetstream::consumer::{self, PullConsumer, pull};
use async_nats::jetstream::{self, stream};

/// How long the stream keeps messages.
const RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Connect to NATS as `persona`. `url` may be `nats://`, `tls://`, `ws://` or `wss://`, so the same binary works over a
/// dev tunnel, a reverse proxy or a direct WebSocket endpoint. `creds` is a `.creds` file from `ants auth user`.
pub async fn connect(
    url: &str,
    creds: Option<&Path>,
    persona: &PersonaId,
    name: &str,
) -> Result<(async_nats::Client, jetstream::Context)> {
    let mut options = async_nats::ConnectOptions::new()
        .name(name)
        .custom_inbox_prefix(inbox_prefix(persona))
        .event_callback(|event| async move {
            match event {
                // Permission violations arrive only here; the denied request itself just times out.
                Event::ServerError(e) => tracing::warn!("{e}"),
                event => tracing::debug!("{event}"),
            }
        });
    if let Some(path) = creds {
        options =
            options.credentials_file(path).await.with_context(|| format!("reading credentials {}", path.display()))?;
    }
    let client = options.connect(url).await.with_context(|| format!("connecting to NATS at {url}"))?;
    let js = jetstream::new(client.clone());
    Ok((client, js))
}

/// Create the `ANTS` stream, or return it as it is. Only the human's credentials may create it.
pub async fn ensure_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    js.get_or_create_stream(stream::Config {
        name: STREAM.to_owned(),
        subjects: STREAM_SUBJECTS.iter().map(|s| (*s).to_owned()).collect(),
        storage: stream::StorageType::File,
        max_age: RETENTION,
        ..Default::default()
    })
    .await
    .context("creating the ANTS stream (with credentials, `ants init` as the human creates it)")
}

/// Messages from the durable consumer of `persona`'s inbox, which is created if missing.
pub async fn inbox_consumer(
    stream: &stream::Stream,
    persona: &PersonaId,
    ack_wait: Duration,
    max_deliver: i64,
) -> Result<pull::Stream> {
    let name = ants_proto::inbox_consumer(persona);
    let consumer: PullConsumer = stream
        .get_or_create_consumer(
            &name,
            pull::Config {
                durable_name: Some(name.clone()),
                filter_subject: subject::inbox(persona),
                ack_policy: consumer::AckPolicy::Explicit,
                ack_wait,
                max_deliver,
                // One message at a time keeps one harness alive at most; per-thread concurrency comes later.
                max_ack_pending: 1,
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("creating consumer {name}"))?;
    Ok(consumer.messages().await?)
}

/// The durable consumer of the human's inbox, created if missing. A reply stays in it, unread, until acked. Any number
/// may be outstanding and none is ever given up on, so a reply shown without being marked read never expires.
pub async fn human_inbox_consumer(stream: &stream::Stream, ack_wait: Duration) -> Result<PullConsumer> {
    let human = PersonaId::human();
    let name = ants_proto::inbox_consumer(&human);
    stream
        .get_or_create_consumer(
            &name,
            pull::Config {
                durable_name: Some(name.clone()),
                filter_subject: subject::inbox(&human),
                ack_policy: consumer::AckPolicy::Explicit,
                ack_wait,
                max_deliver: -1,
                max_ack_pending: -1,
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("creating consumer {name}"))
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
