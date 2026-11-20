//! The ants stream, the buckets and the consumers ants relies on.

use std::time::Duration;

use ants_proto::{COLONY_BUCKET, JsDomain, STREAM, THREADS_BUCKET, UserId, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::consumer::{self, PullConsumer, pull};
use async_nats::jetstream::context::ConsumerInfoErrorKind;
use async_nats::jetstream::stream::ConsumerErrorKind;
use async_nats::jetstream::{self, ErrorCode, kv, stream};

use crate::hub;
use crate::records::direct_kv::DirectKv;

/// Open the colony bucket, creating it if missing with entries that expire `ttl` after their last put. Only the
/// human's credentials may create it; an existing bucket keeps its own max age.
pub async fn ensure_colony(js: &jetstream::Context, ttl: Duration) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(COLONY_BUCKET).await {
        return Ok(store);
    }
    hub::create_colony(js, ttl)
        .await
        .context("creating the colony bucket (with credentials, `ants init` as the human creates it)")
}

/// The `ANTS` stream for `user`, without its info, which no ant's or webhook's credentials allow: with a subjects
/// filter it lists every subject stored, so who messages whom. Whether it exists shows in the info of the user's own
/// inbox consumer, found or not; a missing stream is created when the credentials allow it (no credentials, or a
/// human's), as [`ensure_stream`] does, and otherwise the error says `ants init` creates it.
pub async fn open_stream(js: &jetstream::Context, user: &UserId) -> Result<stream::Stream<()>> {
    let stream = js.get_stream_no_info(STREAM).await.context("naming the ANTS stream")?;
    match stream.consumer_info(ants_proto::inbox_consumer(user)).await {
        Ok(_) => {}
        Err(e) if e.kind() == ConsumerInfoErrorKind::NotFound => {}
        Err(e) if e.kind() == ConsumerInfoErrorKind::StreamNotFound => {
            js.create_stream(hub::stream_config())
                .await
                .context("creating the ANTS stream (with credentials, `ants init` as the human creates it)")?;
        }
        Err(e) => return Err(e).context("looking up the ANTS stream"),
    }
    Ok(stream)
}

/// Create the `ANTS` stream, or return it as it is, with its info: only the human's credentials allow both.
pub async fn ensure_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    js.get_or_create_stream(hub::stream_config())
        .await
        .context("creating the ANTS stream (with credentials, `ants init` as the human creates it)")
}

/// The KV bucket mapping threads to harness sessions, under `ant`'s own keys, created when missing and the credentials
/// allow it. Read and written without stream info ([`DirectKv`]), which no ant's credentials allow. `js_domain` is the
/// domain `js` was made for ([`jetstream`]).
pub async fn ensure_sessions(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<DirectKv> {
    ensure_own(js, js_domain, hub::Object::Sessions, ant).await
}

/// The KV bucket of attempt markers ([`ATTEMPTS_BUCKET`](ants_proto::ATTEMPTS_BUCKET)), under `ant`'s own keys, as [`ensure_sessions`].
pub async fn ensure_attempts(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<DirectKv> {
    ensure_own(js, js_domain, hub::Object::Attempts, ant).await
}

/// The sessions and attempts buckets, created when missing: `ants init`, with the human's credentials.
pub async fn init_own_buckets(js: &jetstream::Context) -> Result<()> {
    for object in [hub::Object::Sessions, hub::Object::Attempts] {
        ensure_kept(js, object).await?;
    }
    Ok(())
}

async fn ensure_own(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    object: hub::Object,
    ant: &UserId,
) -> Result<DirectKv> {
    let kv = DirectKv::new(js, js_domain, object.bucket().unwrap_or_default()).await?;
    match kv.exists(ant).await {
        Ok(true) => {}
        Ok(false) => create_kept(js, object).await?,
        // Through a leaf node, a direct get of a missing stream goes unanswered rather than finding no responders.
        Err(unanswered) => create_kept(js, object).await.map_err(|e| unanswered.context(format!("{e:#}")))?,
    }
    Ok(kv)
}

/// The KV bucket of the channels ([`CHANNELS_BUCKET`](ants_proto::CHANNELS_BUCKET)), created as `ants init` does when missing and the credentials
/// allow it (the human's). Entries never expire: a channel lasts until the human removes it.
pub async fn ensure_channels(js: &jetstream::Context) -> Result<kv::Store> {
    ensure_bucket(js, hub::Object::Channels).await
}

/// The KV bucket of the escalation chains ([`CHAINS_BUCKET`](ants_proto::CHAINS_BUCKET)), created as `ants init` does when missing and the
/// credentials allow it (the human's). Entries never expire: a chain outlives its ant's colony entry.
pub async fn ensure_chains(js: &jetstream::Context) -> Result<kv::Store> {
    ensure_bucket(js, hub::Object::Chains).await
}

/// The KV bucket of the links to channel threads ants passed on ([`LINKS_BUCKET`](ants_proto::LINKS_BUCKET)), created when missing and the
/// credentials allow it (the human's). Entries expire with the stream's messages.
pub async fn ensure_links(js: &jetstream::Context) -> Result<kv::Store> {
    ensure_kept(js, hub::Object::Links).await
}

/// The bucket of the owners of message threads ([`OWNERS_BUCKET`](ants_proto::OWNERS_BUCKET)), created when missing and
/// the credentials allow it (the human's), as [`hub::owners_config`] describes it.
pub async fn ensure_owners(js: &jetstream::Context) -> Result<()> {
    if js.get_stream(crate::records::owners::owners_stream()).await.is_ok() {
        return Ok(());
    }
    let created = js.create_stream(hub::owners_config()).await;
    created.context("creating the owners bucket (with credentials, `ants init` as the human creates it)")?;
    Ok(())
}

/// A file-backed bucket whose entries expire with the stream's messages.
async fn ensure_kept(js: &jetstream::Context, object: hub::Object) -> Result<kv::Store> {
    ensure_bucket(js, object).await
}

/// The KV bucket of `object`, created as `ants init` does when missing: its rollups denied.
async fn ensure_bucket(js: &jetstream::Context, object: hub::Object) -> Result<kv::Store> {
    Box::pin(hub::ensure_bucket(js, object))
        .await
        .with_context(|| format!("opening {object} (with credentials, `ants init` as the human creates it)"))
}

/// Create the bucket of `object` as `ants init` does, without its stream info.
async fn create_kept(js: &jetstream::Context, object: hub::Object) -> Result<()> {
    Box::pin(hub::ensure_unseen_bucket(js, object))
        .await
        .with_context(|| format!("creating {object} (with credentials, `ants init` as the human creates it)"))
}

/// Messages from the durable consumer of `ant`'s inbox, which is created if missing and updated if its settings
/// changed. At most `max_ack_pending` messages are in hand at once.
pub async fn inbox_consumer<I>(
    stream: &stream::Stream<I>,
    ant: &UserId,
    ack_wait: Duration,
    max_deliver: i64,
    max_ack_pending: i64,
) -> Result<pull::Stream> {
    let name = ants_proto::inbox_consumer(ant);
    let consumer: PullConsumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: subject::inbox_of(ant),
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

/// Messages from the durable consumer `chan-<ant>` of every channel post, with the settings of
/// [`inbox_consumer`]; the daemon picks out the posts meant for the ant. A new consumer starts with the posts
/// stored from then on, so a first start does not answer a channel's history.
pub async fn chan_consumer<I>(
    stream: &stream::Stream<I>,
    ant: &UserId,
    ack_wait: Duration,
    max_deliver: i64,
    max_ack_pending: i64,
) -> Result<pull::Stream> {
    let name = ants_proto::chan_consumer(ant);
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

/// Delete the durable consumers that keep for `ant` what reaches it while no daemon serves it, but its inbox's: its
/// channel posts' ([`ants_proto::chan_consumer`]) and the done states' ([`ants_proto::done_reader`]), as its
/// retirement stops it, so neither piles up for an ant that may never be served again. Serving it again creates them
/// anew, starting with what is stored from then on.
pub async fn forget_retired(js: &jetstream::Context, ant: &UserId) -> Result<()> {
    let threads = format!("KV_{THREADS_BUCKET}");
    let mut failed = Vec::new();
    for (stream, name) in [(STREAM, ants_proto::chan_consumer(ant)), (threads.as_str(), ants_proto::done_reader(ant))] {
        match js.delete_consumer_from_stream(&name, stream).await {
            Ok(_) => tracing::info!(consumer = name, "deleted the consumer of the retired ant"),
            Err(e)
                if matches!(e.kind(), ConsumerErrorKind::JetStream(e)
                if [ErrorCode::CONSUMER_NOT_FOUND, ErrorCode::STREAM_NOT_FOUND].contains(&e.error_code())) => {}
            Err(e) => failed.push(format!("{stream}/{name}: {e:#}")),
        }
    }
    if !failed.is_empty() {
        anyhow::bail!("{}", failed.join("; "));
    }
    Ok(())
}
