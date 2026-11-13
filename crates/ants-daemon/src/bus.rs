//! NATS connection and the JetStream objects ants relies on.

use std::path::Path;
use std::time::Duration;

use ants_proto::{
    ATTEMPTS_BUCKET, CHANNELS_BUCKET, Envelope, JsDomain, PersonaId, ROSTER_BUCKET, SESSIONS_BUCKET, STREAM,
    STREAM_SUBJECTS, inbox_prefix, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::consumer::{self, PullConsumer, pull};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::{self, kv, stream};
use async_nats::{ConnectErrorKind, Event};

/// How long the stream keeps messages, the sessions bucket a thread's session, and the attempts bucket a marker: a
/// message can be redelivered for as long as the stream keeps it.
pub const RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Client PING period. The client drops a connection after two unanswered PINGs, so a link that went silent (laptop
/// sleep, dropped tunnel) is noticed within about a minute instead of three, and proxies with a 60 s idle timeout see
/// traffic. Reconnects are unlimited by default.
const PING_INTERVAL: Duration = Duration::from_secs(20);

/// Connect to NATS as `persona`. `url` may be `nats://`, `tls://`, `ws://` or `wss://`, so the same binary works over a
/// dev tunnel, a reverse proxy or a direct WebSocket endpoint. `creds` is a `.creds` file from `ants auth user`, and
/// `js_domain` the hub's JetStream domain, if it has one (see [`jetstream`]).
pub async fn connect(
    url: &str,
    creds: Option<&Path>,
    js_domain: Option<&JsDomain>,
    persona: &PersonaId,
    name: &str,
) -> Result<(async_nats::Client, jetstream::Context)> {
    let client = options(creds, persona, name)
        .await?
        .connect(url)
        .await
        .with_context(|| format!("connecting to NATS at {}", redacted(url)))?;
    Ok((client.clone(), jetstream(client, js_domain)))
}

/// Delay before the second attempt of [`connect_retrying`], doubled after each failed one up to [`RETRY_MAX`].
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// Like [`connect`], but waits for a server that cannot be reached yet, as the local nats-server when a service manager
/// starts the daemon first: a refused, timed-out or unresolved connection is retried for as long as it takes, after
/// [`RETRY_FIRST`] and then twice as long each time up to [`RETRY_MAX`], with a warning each time. What another attempt
/// cannot cure fails at once: an unreadable credentials file, a malformed URL, credentials the server rejects, a TLS
/// error. None when `stop` completes first.
pub async fn connect_retrying(
    url: &str,
    creds: Option<&Path>,
    js_domain: Option<&JsDomain>,
    persona: &PersonaId,
    name: &str,
    stop: impl Future<Output = ()>,
) -> Result<Option<(async_nats::Client, jetstream::Context)>> {
    let options = options(creds, persona, name).await?;
    let shown = redacted(url);
    let attempts = async {
        let mut delay = RETRY_FIRST;
        loop {
            match options.clone().connect(url).await {
                Ok(client) => return Ok(client),
                Err(e) if worth_retrying(&e) => {
                    tracing::warn!(url = %shown, error = %e, retry_in = ?delay, "NATS server not reachable yet");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(RETRY_MAX);
                }
                Err(e) => return Err(e).with_context(|| format!("connecting to NATS at {shown}")),
            }
        }
    };
    tokio::select! {
        client = attempts => {
            let client = client?;
            Ok(Some((client.clone(), jetstream(client, js_domain))))
        }
        () = stop => Ok(None),
    }
}

async fn options(creds: Option<&Path>, persona: &PersonaId, name: &str) -> Result<async_nats::ConnectOptions> {
    let options = async_nats::ConnectOptions::new()
        .name(name)
        .ping_interval(PING_INTERVAL)
        .custom_inbox_prefix(inbox_prefix(persona))
        .event_callback(|event| async move {
            match event {
                // Permission violations arrive only here; the denied request itself just times out.
                Event::ServerError(e) => tracing::warn!("{e}"),
                event => tracing::debug!("{event}"),
            }
        });
    match creds {
        Some(path) => {
            options.credentials_file(path).await.with_context(|| format!("reading credentials {}", path.display()))
        }
        None => Ok(options),
    }
}

/// Whether the server may answer a later attempt: it is not listening yet, or not reachable.
fn worth_retrying(e: &async_nats::ConnectError) -> bool {
    matches!(e.kind(), ConnectErrorKind::Io | ConnectErrorKind::TimedOut | ConnectErrorKind::Dns)
}

/// `url`, a comma-separated list of server URLs too, with any user and password masked, for logs and errors.
fn redacted(url: &str) -> String {
    url.split(',')
        .map(|one| {
            let start = one.find("://").map_or(0, |i| i + 3);
            let end = one[start..].find(['/', '?', '#']).map_or(one.len(), |i| start + i);
            match one[start..end].rfind('@') {
                Some(at) => format!("{}***{}", &one[..start], &one[start + at..]),
                None => one.to_owned(),
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// The JetStream context every ants process uses. With a domain, requests go to `$JS.<domain>.API`, which reaches the
/// hub's JetStream from a leaf node without JetStream as well as on the hub itself; the credentials must have been
/// issued for the same domain ([`ants_proto::Permissions::with_js_domain`]).
pub fn jetstream(client: async_nats::Client, js_domain: Option<&JsDomain>) -> jetstream::Context {
    match js_domain {
        Some(domain) => jetstream::with_domain(client, domain.as_str()),
        None => jetstream::new(client),
    }
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
    ensure_kept(js, SESSIONS_BUCKET).await
}

/// The KV bucket of attempt markers ([`ATTEMPTS_BUCKET`]), created when missing and the credentials allow it.
pub async fn ensure_attempts(js: &jetstream::Context) -> Result<kv::Store> {
    ensure_kept(js, ATTEMPTS_BUCKET).await
}

/// The KV bucket of the channels ([`CHANNELS_BUCKET`]), created when missing and the credentials allow it (the
/// human's). Entries never expire: a channel lasts until the human removes it.
pub async fn ensure_channels(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(CHANNELS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: CHANNELS_BUCKET.to_owned(),
        description: "Channels: their members and messenger, written by the human".to_owned(),
        history: 1,
        storage: stream::StorageType::File,
        ..Default::default()
    })
    .await
    .context("creating the channels bucket (with credentials, `ants init` as the human creates it)")
}

/// A file-backed bucket whose entries expire with the stream's messages.
async fn ensure_kept(js: &jetstream::Context, bucket: &str) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(bucket).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: bucket.to_owned(),
        history: 1,
        max_age: RETENTION,
        storage: stream::StorageType::File,
        ..Default::default()
    })
    .await
    .with_context(|| format!("creating the {bucket} bucket (with credentials, `ants init` as the human creates it)"))
}

/// Messages from the durable consumer of `persona`'s inbox, which is created if missing and updated if its settings
/// changed (such as the filter of a consumer from before inbox subjects named the sender). At most `max_ack_pending`
/// messages are in hand at once.
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
            filter_subject: subject::inbox_of(persona),
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

/// Messages from the durable consumer `chan-<persona>` of every channel post, with the settings of
/// [`inbox_consumer`]; the daemon picks out the posts meant for the persona. A new consumer starts with the posts
/// stored from then on, so a first start does not answer a channel's history.
pub async fn chan_consumer(
    stream: &stream::Stream,
    persona: &PersonaId,
    ack_wait: Duration,
    max_deliver: i64,
    max_ack_pending: i64,
) -> Result<pull::Stream> {
    let name = ants_proto::chan_consumer(persona);
    let consumer: PullConsumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: subject::ALL_CHANNELS.to_owned(),
            deliver_policy: consumer::DeliverPolicy::New,
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

/// The durable consumer of the inbox of `human`, created if missing. A reply stays in it, unread, until acked. Any
/// number may be outstanding and none is ever given up on, so a reply shown without being marked read never expires. A
/// consumer from before inbox subjects named the sender gets the current filter.
pub async fn human_inbox_consumer(
    stream: &stream::Stream,
    human: &PersonaId,
    ack_wait: Duration,
) -> Result<PullConsumer> {
    let name = ants_proto::inbox_consumer(human);
    let config = pull::Config {
        durable_name: Some(name.clone()),
        filter_subject: subject::inbox_of(human),
        ack_policy: consumer::AckPolicy::Explicit,
        ack_wait,
        max_deliver: -1,
        max_ack_pending: -1,
        ..Default::default()
    };
    let consumer: PullConsumer = stream
        .get_or_create_consumer(&name, config.clone())
        .await
        .with_context(|| format!("creating consumer {name}"))?;
    if consumer.cached_info().config.filter_subject == config.filter_subject {
        return Ok(consumer);
    }
    tracing::info!(consumer = name, filter = config.filter_subject, "updating the inbox filter");
    stream.create_consumer(config).await.with_context(|| format!("updating consumer {name}"))
}

/// Where [`store`] put a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stored {
    /// In the hub's stream.
    Hub,
    /// In the outbox of the machine's leaf node, which the hub's stream takes it from once the uplink is back.
    Outbox,
}

/// Publish an envelope and wait until JetStream has stored it, in the hub's stream or, out of its reach, in the
/// machine's outbox ([`store`]).
pub async fn publish(js: &jetstream::Context, envelope: &Envelope) -> Result<Stored> {
    store(js, envelope.subject(), envelope, &envelope.id.to_string()).await
}

/// Publish `value` as JSON to `subject` in the hub's stream and wait until it is stored. When no stream answers (the
/// leaf node's uplink is down) or none answers in time, store it in the leaf node's outbox instead, under
/// [`subject::outbox`], for the hub to source once the uplink is back. Both copies carry `msg_id`, so the hub's stream
/// keeps one if the first publish did land after all, as long as the second arrives within its duplicate window.
pub async fn store<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: &str,
) -> Result<Stored> {
    let payload = serde_json::to_vec(value)?;
    let message = || jetstream::message::PublishMessage::build().payload(payload.clone().into()).message_id(msg_id);
    let hub = match js.send_publish(subject.clone(), message()).await {
        Ok(ack) => ack.await,
        Err(e) => Err(e),
    };
    let unreachable = match hub {
        Ok(_) => return Ok(Stored::Hub),
        Err(e) if matches!(e.kind(), PublishErrorKind::StreamNotFound | PublishErrorKind::TimedOut) => e,
        Err(e) => return Err(e).context("waiting for JetStream to store the message"),
    };
    let Some(outbox) = subject::outbox(&subject) else {
        return Err(unreachable).context("waiting for JetStream to store the message");
    };
    let kept = match js.send_publish(outbox, message()).await {
        Ok(ack) => ack.await,
        Err(e) => Err(e),
    };
    if let Err(e) = kept {
        return Err(anyhow::anyhow!(e)).with_context(|| {
            format!(
                "the hub's stream did not store {subject} ({unreachable}), nor did this machine's outbox \
                 (`ants leaf outbox` sets it up on a leaf node)"
            )
        });
    }
    tracing::warn!(%subject, "the hub's stream is out of reach ({unreachable}); kept in this machine's outbox");
    Ok(Stored::Outbox)
}

/// Ack `msg` and wait until the server confirms it. [`jetstream::Message::double_ack`] takes the "no responders"
/// status the client gets while a leaf node's uplink is down for a confirmation; this does not.
pub async fn double_ack(client: &async_nats::Client, msg: &jetstream::Message) -> Result<()> {
    let reply = msg.reply.clone().context("acking a message that is not from JetStream")?;
    client.request(reply, jetstream::AckKind::Ack.into()).await.context("acking")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use ants_test_support::{NatsServer, free_port, have};

    fn dev() -> PersonaId {
        "dev-a".parse().unwrap()
    }

    #[tokio::test]
    async fn waits_for_a_server_that_starts_later() {
        if !have("nats-server") {
            return;
        }
        let port = free_port();
        let url = format!("nats://127.0.0.1:{port}");
        let later = async {
            // After the first attempt has failed.
            tokio::time::sleep(RETRY_FIRST / 2).await;
            tokio::task::spawn_blocking(move || NatsServer::start_on(port).unwrap()).await.unwrap()
        };
        let dev = dev();
        let connecting = connect_retrying(&url, None, None, &dev, "test", std::future::pending());
        let (connected, _server) = tokio::join!(tokio::time::timeout(Duration::from_secs(20), connecting), later);
        let (_client, js) = connected.expect("still waiting for the server").unwrap().unwrap();
        ensure_stream(&js).await.unwrap();
    }

    #[tokio::test]
    async fn stops_waiting_on_the_stop_signal() {
        let url = format!("nats://127.0.0.1:{}", free_port());
        let stop = tokio::time::sleep(Duration::from_millis(100));
        let stopped = tokio::time::timeout(RETRY_MAX, connect_retrying(&url, None, None, &dev(), "test", stop)).await;
        assert!(stopped.expect("ignored the stop signal").unwrap().is_none());
    }

    #[tokio::test]
    async fn fails_at_once_on_what_retrying_cannot_cure() {
        let Some(server) = NatsServer::start_with(|_| "authorization { user: dev, password: right }".to_owned()) else {
            return;
        };
        // async-nats sends no credentials from the URL, so the server rejects the connection; the URL's are masked.
        let wrong = server.url.replacen("nats://", "nats://dev:wrong@", 1);
        let missing = Path::new("no-such.creds");
        let cases = [
            (wrong.as_str(), None, "authorization violation"),
            (server.url.as_str(), Some(missing), "reading credentials"),
            ("nats://[::1", None, "NATS server URL is invalid"),
        ];
        for (url, creds, why) in cases {
            let failed = tokio::time::timeout(
                RETRY_FIRST * 3,
                connect_retrying(url, creds, None, &dev(), "test", std::future::pending()),
            )
            .await
            .unwrap_or_else(|_| panic!("retried {url} with {creds:?}"));
            let err = format!("{:#}", failed.err().unwrap());
            assert!(err.contains(why) && !err.contains("wrong"), "{err}");
        }
    }

    #[test]
    fn masks_users_and_passwords() {
        assert_eq!(redacted("nats://127.0.0.1:4222"), "nats://127.0.0.1:4222");
        assert_eq!(redacted("wss://dev:secret@hub.example.com/nats?x=a@b"), "wss://***@hub.example.com/nats?x=a@b");
        assert_eq!(redacted("nats://t0ken@a:4222,tls://u:p@b"), "nats://***@a:4222,tls://***@b");
    }
}
