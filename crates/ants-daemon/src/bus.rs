//! NATS connection and the JetStream objects ants relies on.

use std::path::Path;
use std::time::Duration;

use ants_proto::{Envelope, PersonaId, ROSTER_BUCKET, SESSIONS_BUCKET, STREAM, STREAM_SUBJECTS, inbox_prefix, subject};
use anyhow::{Context as _, Result};
use async_nats::Event;
use async_nats::jetstream::consumer::{self, PullConsumer, pull};
use async_nats::jetstream::{self, kv, stream};

/// How long the stream keeps messages, and the sessions bucket a thread's session.
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

fn stream_config() -> stream::Config {
    stream::Config {
        name: STREAM.to_owned(),
        subjects: STREAM_SUBJECTS.iter().map(|s| (*s).to_owned()).collect(),
        storage: stream::StorageType::File,
        max_age: RETENTION,
        ..Default::default()
    }
}

/// Open the roster bucket, creating it if missing with entries that expire `ttl` after their last put. Only the
/// human's credentials may create it; an existing bucket keeps its own max age.
pub async fn ensure_roster(js: &jetstream::Context, ttl: Duration) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(ROSTER_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: ROSTER_BUCKET.to_owned(),
        description: "Personas being served, refreshed by daemon heartbeats".to_owned(),
        history: 1,
        max_age: ttl,
        // Entries are rebuilt by the next heartbeat, so they need not survive a server restart.
        storage: stream::StorageType::Memory,
        ..Default::default()
    })
    .await
    .context("creating the roster bucket (with credentials, `ants init` as the human creates it)")
}

/// Create the `ANTS` stream, or return it as it is. Only the human's credentials may create it. A stream from an older
/// version that lacks some of [`STREAM_SUBJECTS`] is returned with a warning: [`init_stream`] upgrades it.
pub async fn ensure_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    let stream = js
        .get_or_create_stream(stream_config())
        .await
        .context("creating the ANTS stream (with credentials, `ants init` as the human creates it)")?;
    let missing = missing_subjects(&stream.cached_info().config.subjects);
    if !missing.is_empty() {
        tracing::warn!(?missing, "stream {STREAM} lacks subjects; `ants init` as the human adds them");
    }
    Ok(stream)
}

/// Create the `ANTS` stream, or add the subjects an older version did not capture. Needs the human's credentials.
pub async fn init_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    let stream = ensure_stream(js).await?;
    let mut config = stream.cached_info().config.clone();
    let missing = missing_subjects(&config.subjects);
    if missing.is_empty() {
        return Ok(stream);
    }
    config.subjects.extend(missing);
    js.update_stream(&config).await.context("adding subjects to the ANTS stream")?;
    Ok(js.get_stream(STREAM).await?)
}

/// The subjects of [`STREAM_SUBJECTS`] that `subjects` lacks.
pub fn missing_subjects(subjects: &[String]) -> Vec<String> {
    STREAM_SUBJECTS.iter().filter(|s| !subjects.iter().any(|have| have == *s)).map(|s| (*s).to_owned()).collect()
}

/// The KV bucket mapping threads to harness sessions, created when missing and the credentials allow it.
pub async fn ensure_sessions(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(SESSIONS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: SESSIONS_BUCKET.to_owned(),
        history: 1,
        max_age: RETENTION,
        storage: stream::StorageType::File,
        ..Default::default()
    })
    .await
    .with_context(|| {
        format!("creating the {SESSIONS_BUCKET} bucket (with credentials, `ants init` as the human creates it)")
    })
}

/// Messages from the durable consumer of `persona`'s inbox, which is created if missing and updated if its settings
/// changed. At most `max_ack_pending` messages are in hand at once.
pub async fn inbox_consumer(
    stream: &stream::Stream,
    persona: &PersonaId,
    ack_wait: Duration,
    max_deliver: i64,
    max_ack_pending: i64,
) -> Result<pull::Stream> {
    let name = ants_proto::inbox_consumer(persona);
    let consumer: PullConsumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: subject::inbox(persona),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait,
            max_deliver,
            max_ack_pending,
            ..Default::default()
        })
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
    publish_json(js, envelope.subject(), envelope, None).await?;
    Ok(())
}

/// Publish `value` as JSON and wait until JetStream has stored it, returning its stream sequence. With `msg_id`, a
/// second publish within the duplicate window stores nothing and returns the first one's sequence.
pub async fn publish_json<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: Option<&str>,
) -> Result<u64> {
    let mut message = jetstream::message::PublishMessage::build().payload(serde_json::to_vec(value)?.into());
    if let Some(id) = msg_id {
        message = message.message_id(id);
    }
    let ack = js
        .send_publish(subject, message)
        .await
        .context("publishing")?
        .await
        .context("waiting for JetStream to store the message")?;
    Ok(ack.sequence)
}
