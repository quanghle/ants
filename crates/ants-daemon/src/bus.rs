//! NATS connection and the JetStream objects ants relies on.

use std::collections::{HashSet, VecDeque};
use std::hash::{Hash as _, Hasher as _};
use std::ops::ControlFlow;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use ants_proto::{
    ATTEMPTS_BUCKET, CHAINS_BUCKET, CHANNELS_BUCKET, Envelope, JsDomain, LINKS_BUCKET, ROSTER_BUCKET, Role,
    SESSIONS_BUCKET, STREAM, STREAM_SUBJECTS, UserId, inbox_prefix, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::consumer::{self, PullConsumer, pull};
use async_nats::jetstream::context::{ConsumerInfoErrorKind, PublishErrorKind};
use async_nats::jetstream::{self, kv, stream};
use async_nats::{ConnectErrorKind, Event};
use futures::{Stream, StreamExt as _};

use crate::direct_kv::DirectKv;
use crate::hub;
use crate::keystore::{self, Store};
use crate::reading::{self, Reading};

/// How long the stream keeps messages, the sessions bucket a thread's session, and the attempts bucket a marker: a
/// message can be redelivered for as long as the stream keeps it.
pub const RETENTION: Duration = Duration::from_hours(30 * 24);

/// How long the `ANTS` stream drops a message repeating the `Nats-Msg-Id` of one it stored. A copy kept in a leaf's
/// outbox lands up to about two and a half minutes after the uplink is back, so it may follow its twin stored
/// directly by the length of an outage plus that: a day covers outages overnight.
pub const DUPLICATE_WINDOW: Duration = Duration::from_hours(24);

/// Client PING period. The client drops a connection after two unanswered PINGs, so a link that went silent (laptop
/// sleep, dropped tunnel) is noticed within about a minute instead of three, and proxies with a 60 s idle timeout see
/// traffic. Reconnects are unlimited by default.
const PING_INTERVAL: Duration = Duration::from_secs(20);

/// Connect to NATS as `user`. `url` may be `nats://`, `tls://`, `ws://` or `wss://`, so the same binary works over a
/// dev tunnel, a reverse proxy or a direct WebSocket endpoint. `creds` is a `.creds` file from `ants auth user`, and
/// `js_domain` the hub's JetStream domain, if it has one (see [`jetstream`]). Credentials outdated or expiring soon
/// get a warning naming the command that renews them ([`crate::creds::warn`]).
pub async fn connect(
    url: &str,
    creds: Option<&Path>,
    js_domain: Option<&JsDomain>,
    user: &UserId,
    name: &str,
) -> Result<(async_nats::Client, jetstream::Context)> {
    connect_with(url, creds, keystore::os(), js_domain, user, name).await
}

/// [`connect`], reading a seed the credentials file keeps in `store` rather than in itself.
pub async fn connect_with(
    url: &str,
    creds: Option<&Path>,
    store: &dyn Store,
    js_domain: Option<&JsDomain>,
    user: &UserId,
    name: &str,
) -> Result<(async_nats::Client, jetstream::Context)> {
    check_url(url)?;
    let client = options(creds, store, user, name)?
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
/// [`RETRY_FIRST`] and then twice as long each time up to [`RETRY_MAX`], with a warning each time. Once connected it
/// waits the same way for JetStream to answer, as a leaf node that listens before its uplink has joined the hub
/// answers JetStream requests with no responders or not at all. JetStream answers when it looks up the info of the
/// user's own inbox consumer ([`ants_proto::inbox_consumer`]) in the `ANTS` stream, found or not, which every user's
/// credentials allow: none but a human's get the stream's info, which lists every subject stored. What another attempt
/// cannot cure fails at once: an unreadable credentials file, a malformed URL or one carrying a user and password
/// ([`check_url`]), credentials the server rejects, a TLS error, a JetStream request the credentials do not permit.
/// None when `stop` completes first.
pub async fn connect_retrying(
    url: &str,
    creds: Option<&Path>,
    js_domain: Option<&JsDomain>,
    user: &UserId,
    name: &str,
    stop: impl Future<Output = ()>,
) -> Result<Option<(async_nats::Client, jetstream::Context)>> {
    check_url(url)?;
    // A denied request just times out; the server's error naming it arrives through the event callback.
    let (denied_tx, mut denied) = tokio::sync::mpsc::unbounded_channel();
    let options = options(creds, keystore::os(), user, name)?.event_callback(move |event| {
        let denied_tx = denied_tx.clone();
        async move {
            if let Event::ServerError(async_nats::ServerError::Other(e)) = &event
                && e.to_lowercase().contains("permissions violation")
            {
                let _ = denied_tx.send(e.clone());
            }
            log_event(event);
        }
    });
    let shown = redacted(url);
    let attempts = async {
        let mut delay = RETRY_FIRST;
        let client = loop {
            match options.clone().connect(url).await {
                Ok(client) => break client,
                Err(e) if worth_retrying(&e) => {
                    tracing::warn!(url = %shown, error = %e, retry_in = ?delay, "NATS server not reachable yet");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(RETRY_MAX);
                }
                Err(e) => return Err(e).with_context(|| format!("connecting to NATS at {shown}")),
            }
        };
        let js = jetstream(client.clone(), js_domain);
        let mut delay = RETRY_FIRST;
        loop {
            let reached = tokio::select! {
                biased;
                Some(e) = denied.recv() => return Err(anyhow::anyhow!(e)).context("reaching JetStream"),
                reached = reach_jetstream(&js, user) => reached,
            };
            match reached {
                // Creating the consumer or the stream it found missing is up to the caller.
                Ok(()) => return Ok((client, js)),
                Err(e) if matches!(e.kind(), ConsumerInfoErrorKind::TimedOut | ConsumerInfoErrorKind::NoResponders) => {
                    tracing::warn!(url = %shown, error = %e, retry_in = ?delay, "JetStream not reachable yet");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(RETRY_MAX);
                }
                Err(e) => return Err(e).context("reaching JetStream"),
            }
        }
    };
    tokio::select! {
        connected = attempts => connected.map(Some),
        () = stop => Ok(None),
    }
}

/// Ask JetStream for the info of `user`'s own inbox consumer ([`ants_proto::inbox_consumer`]) in the `ANTS` stream,
/// which every user's credentials allow: Ok when JetStream answered, if only that the consumer or the stream is
/// missing, the error otherwise, such as no responders or a timeout when no JetStream answers in the domain of `js`.
pub async fn reach_jetstream(
    js: &jetstream::Context,
    user: &UserId,
) -> std::result::Result<(), async_nats::jetstream::context::ConsumerInfoError> {
    let stream = js.get_stream_no_info(STREAM).await.expect("naming a stream makes no request");
    match stream.consumer_info(ants_proto::inbox_consumer(user)).await {
        Ok(_) => Ok(()),
        Err(e)
            if matches!(
                e.kind(),
                ConsumerInfoErrorKind::NotFound
                    | ConsumerInfoErrorKind::StreamNotFound
                    | ConsumerInfoErrorKind::Offline
                    | ConsumerInfoErrorKind::JetStream(_)
            ) =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// The options of a connection as `user`, named `name`, with the credentials file `creds`, after warning about what
/// [`crate::creds::warn`] finds in it.
fn options(creds: Option<&Path>, store: &dyn Store, user: &UserId, name: &str) -> Result<async_nats::ConnectOptions> {
    let options = async_nats::ConnectOptions::new()
        .name(name)
        .ping_interval(PING_INTERVAL)
        .custom_inbox_prefix(inbox_prefix(user))
        .event_callback(|event| async move { log_event(event) });
    match creds {
        Some(path) => {
            crate::creds::warn(path);
            authenticate(options, path, store)
        }
        None => Ok(options),
    }
}

/// `options` authenticating with the credentials file at `path`: its user JWT, and the server's challenge signed with
/// its key, read once from the file or from `store` ([`crate::creds::load`]) and kept in memory to sign again on
/// every reconnect.
pub fn authenticate(
    options: async_nats::ConnectOptions,
    path: &Path,
    store: &dyn Store,
) -> Result<async_nats::ConnectOptions> {
    let (jwt, key) = crate::creds::load(path, store)?;
    let key = std::sync::Arc::new(key);
    Ok(options.jwt(jwt, move |nonce| {
        let key = key.clone();
        async move { key.sign(&nonce).map_err(async_nats::AuthError::new) }
    }))
}

fn log_event(event: Event) {
    match event {
        // Permission violations arrive only here; the denied request itself just times out.
        Event::ServerError(e) => tracing::warn!("{e}"),
        event => tracing::debug!("{event}"),
    }
}

/// Whether the server may answer a later attempt: it is not listening yet, or not reachable.
fn worth_retrying(e: &async_nats::ConnectError) -> bool {
    matches!(e.kind(), ConnectErrorKind::Io | ConnectErrorKind::TimedOut | ConnectErrorKind::Dns)
}

/// Fail when `url`, or one in a comma-separated list, carries a user and password (or a token): async-nats would
/// connect without them, as no one. ants authenticates with `.creds` files only.
pub fn check_url(url: &str) -> Result<()> {
    match url.split(',').find(|one| userinfo(one).is_some()) {
        Some(one) => bail!(
            "NATS URL {} carries a user and password, which are not used; authenticate with a .creds file instead",
            redacted(one)
        ),
        None => Ok(()),
    }
}

/// `url`, a comma-separated list of server URLs too, with any user and password masked, for logs and errors.
fn redacted(url: &str) -> String {
    url.split(',')
        .map(|one| match userinfo(one) {
            Some(at) => format!("{}***{}", &one[..at.start], &one[at.end..]),
            None => one.to_owned(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Where the `user:password` of `user:password@` is in one server URL, if it has any.
fn userinfo(one: &str) -> Option<std::ops::Range<usize>> {
    let start = one.find("://").map_or(0, |i| i + 3);
    let end = one[start..].find(['/', '?', '#']).map_or(one.len(), |i| start + i);
    one[start..end].rfind('@').map(|at| start..start + at)
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

/// Open the roster bucket, creating it if missing with entries that expire `ttl` after their last put. Only the
/// human's credentials may create it; an existing bucket keeps its own max age.
pub async fn ensure_roster(js: &jetstream::Context, ttl: Duration) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(ROSTER_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(hub::roster_config(ttl))
        .await
        .context("creating the roster bucket (with credentials, `ants init` as the human creates it)")
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

/// Create the `ANTS` stream, or return it as it is, with its info: only the human's credentials allow both. A stream from an older
/// version that lacks some of [`STREAM_SUBJECTS`], direct gets, the republishing of inbox messages or a duplicate
/// window of [`DUPLICATE_WINDOW`] is returned with a warning: [`init_stream`] upgrades it.
pub async fn ensure_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    let stream = js
        .get_or_create_stream(hub::stream_config())
        .await
        .context("creating the ANTS stream (with credentials, `ants init` as the human creates it)")?;
    let missing = missing_subjects(&stream.cached_info().config.subjects);
    if !missing.is_empty() {
        tracing::warn!(?missing, "stream {STREAM} lacks subjects; `ants init` as the human adds them");
    }
    if !stream.cached_info().config.allow_direct {
        tracing::warn!("stream {STREAM} lacks direct gets; `ants init` as the human allows them");
    }
    if stream.cached_info().config.republish.as_ref() != Some(&hub::stored_inbox()) {
        tracing::warn!(
            "stream {STREAM} does not republish inbox messages, so replies kept in a leaf's outbox reach only inbox \
             consumers; `ants init` as the human sets it up"
        );
    }
    let config = &stream.cached_info().config;
    let window = hub::duplicate_window(config);
    if config.duplicate_window < window {
        tracing::warn!(
            "stream {STREAM} drops repeated message ids for only {}, so a message kept in a leaf's outbox can land \
             twice; `ants init` as the human widens the window to {}",
            ants_proto::duration::format(config.duplicate_window),
            ants_proto::duration::format(window)
        );
    }
    Ok(stream)
}

/// Create the `ANTS` stream, or add the subjects an older version did not capture, allow direct gets, republish
/// inbox messages and widen the duplicate window. Needs the human's credentials.
pub async fn init_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    let stream = ensure_stream(js).await?;
    let mut config = stream.cached_info().config.clone();
    if hub::upgrade_stream(&mut config).is_empty() {
        return Ok(stream);
    }
    js.update_stream(&config).await.context("upgrading the ANTS stream")?;
    Ok(js.get_stream(STREAM).await?)
}

/// The subjects of [`STREAM_SUBJECTS`] that `subjects` lacks.
pub fn missing_subjects(subjects: &[String]) -> Vec<String> {
    STREAM_SUBJECTS.iter().filter(|s| !subjects.iter().any(|have| have == *s)).map(|s| (*s).to_owned()).collect()
}

/// The KV bucket mapping threads to harness sessions, under `ant`'s own keys, created when missing and the credentials
/// allow it. Read and written without stream info ([`DirectKv`]), which no ant's credentials allow. `js_domain` is the
/// domain `js` was made for ([`jetstream`]).
pub async fn ensure_sessions(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<DirectKv> {
    ensure_own(js, js_domain, SESSIONS_BUCKET, ant).await
}

/// The KV bucket of attempt markers ([`ATTEMPTS_BUCKET`]), under `ant`'s own keys, as [`ensure_sessions`].
pub async fn ensure_attempts(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<DirectKv> {
    ensure_own(js, js_domain, ATTEMPTS_BUCKET, ant).await
}

/// The sessions and attempts buckets, created when missing: `ants init`, with the human's credentials.
pub async fn init_own_buckets(js: &jetstream::Context) -> Result<()> {
    for bucket in [SESSIONS_BUCKET, ATTEMPTS_BUCKET] {
        ensure_kept(js, bucket).await?;
    }
    Ok(())
}

async fn ensure_own(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    bucket: &str,
    ant: &UserId,
) -> Result<DirectKv> {
    let kv = DirectKv::new(js, js_domain, bucket).await?;
    match kv.exists(ant).await {
        Ok(true) => {}
        Ok(false) => _ = create_kept(js, bucket).await?,
        // Through a leaf node, a direct get of a missing stream goes unanswered rather than finding no responders.
        Err(unanswered) => _ = create_kept(js, bucket).await.map_err(|e| unanswered.context(format!("{e:#}")))?,
    }
    Ok(kv)
}

/// The KV bucket of the channels ([`CHANNELS_BUCKET`]), created when missing and the credentials allow it (the
/// human's). Entries never expire: a channel lasts until the human removes it.
pub async fn ensure_channels(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(CHANNELS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(hub::channels_config())
        .await
        .context("creating the channels bucket (with credentials, `ants init` as the human creates it)")
}

/// The KV bucket of the escalation chains ([`CHAINS_BUCKET`]), created when missing and the credentials allow it (the
/// human's). Entries never expire: a chain outlives its ant's roster entry.
pub async fn ensure_chains(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(CHAINS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(hub::chains_config())
        .await
        .context("creating the chains bucket (with credentials, `ants init` as the human creates it)")
}

/// The KV bucket of the links to channel threads ants passed on ([`LINKS_BUCKET`]), created when missing and the
/// credentials allow it (the human's). Entries expire with the stream's messages.
pub async fn ensure_links(js: &jetstream::Context) -> Result<kv::Store> {
    ensure_kept(js, LINKS_BUCKET).await
}

/// The bucket of the owners of message threads ([`OWNERS_BUCKET`]), created when missing and the credentials allow it
/// (the human's). Its stream keeps one record per key and refuses another, but drops one repeating the first's
/// message id while the first is kept, as a refused one from a machine's outbox would stall its source
/// ([`crate::owners::first_records_only`]); it denies deletes and purges, and records expire with the stream's
/// messages. A stream from before, dropping repeats only for 2 min, is changed to drop them while it keeps records.
pub async fn ensure_owners(js: &jetstream::Context) -> Result<()> {
    let name = crate::owners::owners_stream();
    if let Ok(stream) = js.get_stream(&name).await {
        let mut config = stream.cached_info().config.clone();
        if crate::owners::first_records_only(&mut config) {
            js.update_stream(&config).await.context("having the owners bucket drop repeated records")?;
        }
        return Ok(());
    }
    let created = js.create_stream(hub::owners_config()).await;
    created.context("creating the owners bucket (with credentials, `ants init` as the human creates it)")?;
    Ok(())
}

/// A file-backed bucket whose entries expire with the stream's messages.
async fn ensure_kept(js: &jetstream::Context, bucket: &str) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(bucket).await {
        return Ok(store);
    }
    create_kept(js, bucket).await
}

async fn create_kept(js: &jetstream::Context, bucket: &str) -> Result<kv::Store> {
    js.create_key_value(hub::kept_config(bucket)).await.with_context(|| {
        format!("creating the {bucket} bucket (with credentials, `ants init` as the human creates it)")
    })
}

/// Messages from the durable consumer of `ant`'s inbox, which is created if missing and updated if its settings
/// changed (such as the filter of a consumer from before inbox subjects named the sender). At most `max_ack_pending`
/// messages are in hand at once.
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
/// keeps one if the first publish did land after all, as long as the second arrives within its duplicate window
/// ([`DUPLICATE_WINDOW`]).
/// When no stream captures `subject` though the hub's JetStream answers ([`uncaptured`]), the error, or the warning
/// for a message kept in the outbox, names `ants init` as the fix; the warning comes once per kind of subject.
pub async fn store<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: &str,
) -> Result<Stored> {
    let ack = store_acked(js, subject, value, msg_id).await?;
    Ok(if ack.is_some() { Stored::Hub } else { Stored::Outbox })
}

/// [`store`], returning the hub stream's ack, or `None` when the message went to the machine's outbox.
pub async fn store_acked<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: &str,
) -> Result<Option<jetstream::publish::PublishAck>> {
    let payload = serde_json::to_vec(value)?;
    let message = || jetstream::message::PublishMessage::build().payload(payload.clone().into()).message_id(msg_id);
    let unreachable = match publish_to_hub(js, &subject, message()).await {
        Ok(ack) => return Ok(Some(ack)),
        Err(e) if out_of_reach(&e) => e,
        Err(e) => return Err(e).context("waiting for JetStream to store the message"),
    };
    let uncaptured = unreachable.kind() == PublishErrorKind::StreamNotFound && uncaptured(js).await;
    let Some(outbox) = subject::outbox(&subject) else {
        return Err(explained(unreachable, &subject, uncaptured)).context("waiting for JetStream to store the message");
    };
    let kept = match js.send_publish(outbox, message()).await {
        Ok(ack) => ack.await,
        Err(e) => Err(e),
    };
    if let Err(e) = kept {
        if uncaptured {
            return Err(explained(unreachable, &subject, true)).context("waiting for JetStream to store the message");
        }
        return Err(anyhow::anyhow!(e)).with_context(|| {
            format!(
                "the hub's stream did not store {subject} ({unreachable}), nor did this machine's outbox \
                 (`ants leaf outbox` sets it up on a leaf node)"
            )
        });
    }
    if uncaptured {
        warn_uncaptured(&subject, "kept in this machine's outbox");
    } else {
        tracing::warn!(%subject, "the hub's stream is out of reach ({unreachable}); kept in this machine's outbox");
    }
    Ok(None)
}

/// After a publish found no stream capturing its subject: whether the hub's JetStream answers all the same, so the
/// hub is in reach and its `ANTS` stream is missing or predates the subject, as when an older version created it and
/// no `ants init` with a human's credentials has run since. Out of the hub's reach (a leaf node's uplink is down),
/// false. It asks for the info of the connection's own inbox consumer, as [`reach_jetstream`] does, which every
/// user's credentials allow; the user is the one the connection's reply inboxes are named after ([`inbox_prefix`]).
pub async fn uncaptured(js: &jetstream::Context) -> bool {
    let inbox = js.client().new_inbox();
    let Some(user) = inbox.strip_prefix("_INBOX.").and_then(|rest| rest.split('.').next()?.parse::<UserId>().ok())
    else {
        return false;
    };
    matches!(tokio::time::timeout(PROBE_TIMEOUT, reach_jetstream(js, &user)).await, Ok(Ok(())))
}

/// How long [`uncaptured`] waits for JetStream: a leaf node whose uplink is down answers at once with no responders.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// What to do when no stream captures `subject` while the hub is in reach ([`uncaptured`]).
fn uncaptured_hint(subject: &str) -> String {
    format!(
        "the hub's JetStream answers, but no stream captures {subject}: its {STREAM} stream is missing or predates \
         this version; run `ants init` with a human's credentials to create or upgrade it"
    )
}

/// `e`, the error of a publish to `subject`, with [`uncaptured_hint`] when `uncaptured`.
fn explained(e: jetstream::context::PublishError, subject: &str, uncaptured: bool) -> anyhow::Error {
    let e = anyhow::Error::new(e);
    if uncaptured { e.context(uncaptured_hint(subject)) } else { e }
}

/// `e`, the error of a publish to `subject` in the hub's stream, with what to do about it when no stream captures the
/// subject while the hub is in reach ([`uncaptured`]).
pub async fn explain(js: &jetstream::Context, subject: &str, e: jetstream::context::PublishError) -> anyhow::Error {
    let uncaptured = e.kind() == PublishErrorKind::StreamNotFound && uncaptured(js).await;
    explained(e, subject, uncaptured)
}

/// The kinds of subject ([`subject_kind`]) [`warn_uncaptured`] warned about.
static WARNED_UNCAPTURED: std::sync::Mutex<std::collections::BTreeSet<String>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

/// The first two tokens of `subject`, such as `ants.chan`: what the `ANTS` stream captures or not.
fn subject_kind(subject: &str) -> &str {
    subject.match_indices('.').nth(1).map_or(subject, |(at, _)| &subject[..at])
}

/// Warn that the hub's stream does not capture `subject` ([`uncaptured_hint`]), once per process for each kind of
/// subject ([`subject_kind`]), then only at debug level. True when it warned.
fn warn_uncaptured(subject: &str, done: &str) -> bool {
    let first = WARNED_UNCAPTURED.lock().unwrap_or_else(|e| e.into_inner()).insert(subject_kind(subject).to_owned());
    if first {
        tracing::warn!(%subject, "{}; {done}", uncaptured_hint(subject));
    } else {
        tracing::debug!(%subject, "the hub's stream does not capture the subject; {done}");
    }
    first
}

/// Publish `value` as JSON to `subject` in the hub's stream only, never the machine's outbox, and wait until it is
/// stored, returning the stream's ack, or `None` while the hub is out of reach (see [`store`]) or its stream does not
/// capture `subject`, which a warning says once ([`uncaptured`]).
pub async fn store_in_hub<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: &str,
) -> Result<Option<jetstream::publish::PublishAck>> {
    let message =
        jetstream::message::PublishMessage::build().payload(serde_json::to_vec(value)?.into()).message_id(msg_id);
    match publish_to_hub(js, &subject, message).await {
        Ok(ack) => Ok(Some(ack)),
        Err(e) if out_of_reach(&e) => {
            if e.kind() == PublishErrorKind::StreamNotFound && uncaptured(js).await {
                warn_uncaptured(&subject, "kept until the hub's stream captures it");
            }
            Ok(None)
        }
        Err(e) => Err(e).context("waiting for JetStream to store the message"),
    }
}

async fn publish_to_hub(
    js: &jetstream::Context,
    subject: &str,
    message: jetstream::message::PublishMessage,
) -> Result<jetstream::publish::PublishAck, jetstream::context::PublishError> {
    js.send_publish(subject.to_owned(), message).await?.await
}

/// Whether publishing failed because no stream answered (the leaf node's uplink is down) or none answered in time.
fn out_of_reach(e: &jetstream::context::PublishError) -> bool {
    matches!(e.kind(), PublishErrorKind::StreamNotFound | PublishErrorKind::TimedOut)
}

/// Messages to a user as they come, from [`live_inbox`].
pub type LiveInbox = Pin<Box<dyn Stream<Item = async_nats::Message> + Send>>;

/// How many messages [`live_inbox`] remembers to take each once.
const SEEN: usize = 1024;

/// Messages to `user` as they come, each once, on their inbox subject: published there, and as the hub's stream
/// stored them ([`subject::stored_inbox`]). A message sourced from a leaf's outbox comes only the second way, and so
/// does any message while the stream lacks the republishing `ants init` sets up. Subscribed before it returns, so
/// nothing published after that is missed while the connection reaches the hub; [`caught_up_inbox`] also gets what
/// the hub stored meanwhile.
pub async fn live_inbox(client: &async_nats::Client, user: &UserId) -> Result<LiveInbox> {
    inbox(client, user, None).await
}

/// How often [`caught_up_inbox`] reads back what the hub's stream stored.
pub const CATCH_UP: Duration = Duration::from_secs(5);

/// Most messages [`caught_up_inbox`] reads back at once.
const CATCH_UP_BATCH: usize = 100;

/// [`live_inbox`], which also reads back from the hub's stream, every [`CATCH_UP`], the messages stored for `user`
/// since it was called (or since the hub first answered, when out of its reach then): what the hub stored while this
/// connection, or the uplink of the leaf node it goes through, was down never comes live. It reads as the CLI with
/// credentials in `role` does ([`Reading::cli`]): a human by message gets, an ant through a spare of its inbox
/// consumer, which filters only its own inbox, so the read never touches the consumer its daemon takes the inbox from.
pub async fn caught_up_inbox(
    client: &async_nats::Client,
    js: &jetstream::Context,
    user: &UserId,
    role: Role,
) -> Result<LiveInbox> {
    let read_back =
        read_back(js.clone(), ants_proto::inbox_consumer(user), Reading::cli(role), subject::inbox_of(user));
    inbox(client, user, Some(read_back)).await
}

async fn inbox(client: &async_nats::Client, user: &UserId, read_back: Option<LiveInbox>) -> Result<LiveInbox> {
    let live = client.subscribe(subject::inbox_of(user)).await?;
    let stored = client.subscribe(subject::stored_inbox(user)).await?;
    let user = user.clone();
    let stored = stored.filter_map(move |msg| std::future::ready(as_published(msg, &user)));
    let mut all: Vec<LiveInbox> = vec![Box::pin(live), Box::pin(stored)];
    all.extend(read_back);
    let mut seen = Seen::default();
    Ok(Box::pin(futures::stream::select_all(all).filter(move |msg| std::future::ready(seen.first(msg)))))
}

/// The messages on `filter` the hub's stream stores from now on, read back every [`CATCH_UP`] as `reading` says,
/// through spares of `reader` for an ant, until dropped.
fn read_back(js: jetstream::Context, reader: String, reading: Reading, filter: String) -> LiveInbox {
    let (found, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut after = None;
        loop {
            for msg in stored_after(&js, &reader, reading, &filter, &mut after).await {
                if found.send(msg).is_err() {
                    return;
                }
            }
            tokio::select! {
                () = found.closed() => return,
                () = tokio::time::sleep(CATCH_UP) => {}
            }
        }
    });
    Box::pin(futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|msg| (msg, rx)) }))
}

/// The messages on `filter` the hub's stream stored after sequence `after`, which moves past them. Without `after`, it
/// becomes the stream's last sequence. Out of the hub's reach, none.
async fn stored_after(
    js: &jetstream::Context,
    reader: &str,
    reading: Reading,
    filter: &str,
    after: &mut Option<u64>,
) -> Vec<async_nats::Message> {
    let Some(last) = *after else {
        match reading::last_sequence(js, reader, reading, filter).await {
            Ok(last) => *after = Some(last),
            Err(e) => tracing::debug!(error = format!("{e:#}"), "cannot read back the inbox yet"),
        }
        return Vec::new();
    };
    let mut found = Vec::new();
    let start = consumer::DeliverPolicy::ByStartSequence { start_sequence: last + 1 };
    let read = reading::read_back_messages(js, reader, reading, filter, start, |seq, _, msg| {
        *after = Some(seq);
        found.push(msg);
        if found.len() < CATCH_UP_BATCH { ControlFlow::Continue(()) } else { ControlFlow::Break(()) }
    })
    .await;
    if let Err(e) = read {
        tracing::debug!(error = format!("{e:#}"), "reading back the inbox");
    }
    found
}

/// A stored message republished to `user`, back on the inbox subject it was stored under.
fn as_published(mut msg: async_nats::Message, user: &UserId) -> Option<async_nats::Message> {
    let subject = msg.headers.as_ref()?.get(async_nats::header::NATS_SUBJECT)?.as_str().to_owned();
    let (to, _) = subject::parse_inbox(&subject)?;
    if to != *user {
        return None;
    }
    msg.subject = subject.into();
    Some(msg)
}

/// The latest messages taken, by message id, or payload for one without.
#[derive(Default)]
struct Seen {
    order: VecDeque<u64>,
    keys: HashSet<u64>,
}

impl Seen {
    /// Whether `msg` is new, then remembered.
    fn first(&mut self, msg: &async_nats::Message) -> bool {
        let mut hasher = std::hash::DefaultHasher::new();
        match msg.headers.as_ref().and_then(|h| h.get(async_nats::header::NATS_MESSAGE_ID)) {
            Some(id) => (0u8, id.as_str()).hash(&mut hasher),
            None => (1u8, &msg.payload[..]).hash(&mut hasher),
        }
        let key = hasher.finish();
        if !self.keys.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > SEEN
            && let Some(old) = self.order.pop_front()
        {
            self.keys.remove(&old);
        }
        true
    }
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
    let ack = js.send_publish(subject.clone(), message).await.context("publishing")?.await;
    match ack {
        Ok(ack) => Ok(ack.sequence),
        Err(e) => Err(explain(js, &subject, e).await).context("waiting for JetStream to store the message"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ants_test_support::{NatsServer, free_port, have};

    fn dev() -> UserId {
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

    /// A leaf node listens before its uplink joins the hub, whose JetStream it reaches through the hub's domain.
    #[tokio::test]
    async fn waits_for_jetstream_behind_a_leaf_whose_uplink_joins_later() {
        let uplink_port = free_port();
        let Some(leaf) = NatsServer::spawn_with(|_| {
            format!("leafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{uplink_port}\" }}] }}\n")
        }) else {
            return;
        };
        let later = async {
            // After the first JetStream request has gone unanswered.
            tokio::time::sleep(RETRY_FIRST / 2).await;
            tokio::task::spawn_blocking(move || {
                NatsServer::spawn_with(|dir| {
                    format!(
                        "jetstream {{ store_dir: {:?}, domain: hub }}\nleafnodes {{ listen: \"127.0.0.1:{uplink_port}\" }}\n",
                        dir.join("js")
                    )
                })
                .unwrap()
            })
            .await
            .unwrap()
        };
        let (dev, hub) = (dev(), "hub".parse().unwrap());
        let connecting = connect_retrying(&leaf.url, None, Some(&hub), &dev, "test", std::future::pending());
        let (connected, _hub) = tokio::join!(tokio::time::timeout(Duration::from_secs(20), connecting), later);
        let (_client, js) = connected.expect("still waiting for JetStream").unwrap().unwrap();
        ensure_stream(&js).await.unwrap();
    }

    #[tokio::test]
    async fn fails_at_once_on_a_denied_jetstream_request() {
        let Some(server) = NatsServer::start_with(|_| {
            "authorization { users: [{ user: dev, password: pw, permissions: { publish: { deny: \"$JS.API.>\" } } }] }\n\
             no_auth_user: dev\n"
                .to_owned()
        }) else {
            return;
        };
        // Sooner than the JetStream request's own timeout.
        let failed = tokio::time::timeout(
            RETRY_FIRST * 3,
            connect_retrying(&server.url, None, None, &dev(), "test", std::future::pending()),
        )
        .await
        .expect("retried the denied request");
        let err = format!("{:#}", failed.err().unwrap());
        assert!(err.to_lowercase().contains("permissions violation"), "{err}");
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
        // async-nats would send no credentials from the URL, so it is refused; the URL's are masked.
        let wrong = server.url.replacen("nats://", "nats://dev:wrong@", 1);
        let missing = Path::new("no-such.creds");
        let cases = [
            (server.url.as_str(), None, "authorization violation"),
            (wrong.as_str(), None, "authenticate with a .creds file"),
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

    /// The `ANTS` stream of a version before channels: it captures no `ants.chan.>`.
    async fn stream_before_channels(js: &jetstream::Context) {
        let mut config = hub::stream_config();
        config.subjects.retain(|s| s != subject::ALL_CHANNELS);
        js.create_stream(config).await.unwrap();
    }

    fn post() -> ants_proto::ChannelPost {
        ants_proto::ChannelPost::new("ops".parse().unwrap(), dev(), "hi")
    }

    /// Straight to the hub, a publish no stream captures fails, naming `ants init`; one the stream captures does not.
    #[tokio::test]
    async fn a_publish_the_hubs_stream_does_not_capture_names_ants_init() {
        let Some(server) = NatsServer::start() else {
            return;
        };
        let (_client, js) = connect(&server.url, None, None, &dev(), "test").await.unwrap();
        stream_before_channels(&js).await;
        let post = post();
        let err = format!("{:#}", store(&js, post.subject(), &post, &post.id.to_string()).await.unwrap_err());
        assert!(err.contains("`ants init` with a human's credentials") && err.contains(&post.subject()), "{err}");
        let err = format!("{:#}", publish_json(&js, post.subject(), &post, None).await.unwrap_err());
        assert!(err.contains("`ants init`"), "{err}");
        let envelope = Envelope::new(dev(), UserId::human(), "captured");
        assert_eq!(publish(&js, &envelope).await.unwrap(), Stored::Hub);
    }

    /// Through a leaf node whose uplink is up, a publish the hub's stream does not capture is kept in the machine's
    /// outbox with a warning naming `ants init`; with the uplink down, the hub is just out of reach.
    #[tokio::test]
    async fn through_a_leaf_a_publish_the_hubs_stream_does_not_capture_names_ants_init() {
        let uplink_port = free_port();
        let Some(hub) = NatsServer::spawn_with(|dir| {
            format!(
                "jetstream {{ store_dir: {:?}, domain: hub }}\nleafnodes {{ listen: \"127.0.0.1:{uplink_port}\" }}\n",
                dir.join("js")
            )
        }) else {
            return;
        };
        let leaf = NatsServer::spawn_with(|dir| {
            format!(
                "jetstream {{ store_dir: {:?}, domain: m1 }}\nleafnodes {{ remotes: [{{ url: \
                 \"nats-leaf://127.0.0.1:{uplink_port}\" }}] }}\n",
                dir.join("js")
            )
        })
        .unwrap();
        let (dev, domain) = (dev(), "hub".parse().unwrap());
        let (client, js) = connect_retrying(&leaf.url, None, Some(&domain), &dev, "test", std::future::pending())
            .await
            .unwrap()
            .unwrap();
        stream_before_channels(&js).await;
        let outbox = stream::Config {
            name: ants_proto::OUTBOX_STREAM.to_owned(),
            subjects: vec![ants_proto::OUTBOX_SUBJECTS.to_owned()],
            ..Default::default()
        };
        jetstream::with_domain(client, "m1").create_stream(outbox).await.unwrap();

        assert!(uncaptured(&js).await);
        let post = post();
        assert_eq!(store(&js, post.subject(), &post, &post.id.to_string()).await.unwrap(), Stored::Outbox);
        assert!(WARNED_UNCAPTURED.lock().unwrap().contains("ants.chan"), "no warning naming `ants init`");

        drop(hub);
        let start = std::time::Instant::now();
        while uncaptured(&js).await {
            assert!(start.elapsed() < Duration::from_secs(10), "the hub still answers");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[test]
    fn warns_once_per_kind_of_subject() {
        assert_eq!(subject_kind("ants.kind-test.ops.dev-a"), "ants.kind-test");
        assert_eq!(subject_kind("ants"), "ants");
        assert!(warn_uncaptured("ants.kind-test.ops.dev-a", "kept"));
        assert!(!warn_uncaptured("ants.kind-test.dev.dev-b", "kept"));
        assert!(warn_uncaptured("ants.other-kind-test.dev-a", "kept"));
    }

    #[test]
    fn masks_users_and_passwords() {
        assert_eq!(redacted("nats://127.0.0.1:4222"), "nats://127.0.0.1:4222");
        assert_eq!(redacted("wss://dev:secret@hub.example.com/nats?x=a@b"), "wss://***@hub.example.com/nats?x=a@b");
        assert_eq!(redacted("nats://t0ken@a:4222,tls://u:p@b"), "nats://***@a:4222,tls://***@b");
    }

    #[test]
    fn refuses_urls_with_users_and_passwords() {
        for url in ["nats://127.0.0.1:4222", "wss://hub.example.com/nats?x=a@b", "nats://a:4222,tls://b", "a:4222"] {
            check_url(url).unwrap();
        }
        for (url, shown) in [
            ("nats://dev:secret@hub:4222", "nats://***@hub:4222"),
            ("nats://a:4222,wss://secret@hub/nats", "wss://***@hub/nats"),
            ("dev:secret@hub:4222", "***@hub:4222"),
        ] {
            let err = check_url(url).unwrap_err().to_string();
            assert!(err.contains(shown) && err.contains(".creds file") && !err.contains("secret"), "{err}");
        }
    }

    /// A waiter takes each message once, live, as the stream stored it, or both in either order, and nothing the
    /// stream stored for another user.
    #[test]
    fn a_live_inbox_takes_each_message_once() {
        let msg = |subject: &str, headers: &[(&str, &str)], payload: &'static str| async_nats::Message {
            subject: subject.into(),
            reply: None,
            payload: payload.into(),
            headers: (!headers.is_empty()).then(|| {
                let mut map = async_nats::HeaderMap::new();
                headers.iter().for_each(|(k, v)| map.insert(*k, *v));
                map
            }),
            status: None,
            description: None,
            length: 0,
        };
        let stored = |of: &str, id: &str| {
            let headers = [("Nats-Stream", STREAM), ("Nats-Subject", of), ("Nats-Msg-Id", id)];
            as_published(msg("_INBOX.dev-a.stored.human", &headers, "{}"), &dev())
        };
        let mut seen = Seen::default();
        assert!(seen.first(&msg("ants.inbox.dev-a.human", &[("Nats-Msg-Id", "1")], "{}")));
        let again = stored("ants.inbox.dev-a.human", "1").unwrap();
        assert_eq!(again.subject.as_str(), "ants.inbox.dev-a.human");
        assert!(!seen.first(&again));
        assert!(seen.first(&stored("ants.inbox.dev-a.human", "2").unwrap()));
        assert!(!seen.first(&msg("ants.inbox.dev-a.human", &[("Nats-Msg-Id", "2")], "{}")));
        // Without an id, by payload.
        assert!(seen.first(&msg("ants.inbox.dev-a.human", &[], "3")));
        assert!(!seen.first(&msg("ants.inbox.dev-a.human", &[("Nats-Stream", STREAM)], "3")));
        assert!(seen.first(&msg("ants.inbox.dev-a.human", &[], "4")));

        assert!(stored("ants.inbox.dev-b.human", "5").is_none(), "another user's");
        assert!(as_published(msg("_INBOX.dev-a.stored.human", &[], "{}"), &dev()).is_none(), "without its subject");
    }
}
