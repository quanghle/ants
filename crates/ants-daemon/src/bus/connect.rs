//! Connecting to NATS as a user, retrying until the server and its JetStream answer.

use std::path::Path;
use std::time::Duration;

use ants_proto::{JsDomain, STREAM, UserId, inbox_prefix};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::context::ConsumerInfoErrorKind;
use async_nats::{ConnectErrorKind, Event};

use super::outage::Reporter;
use crate::secrets::keystore::{self, Store};

/// Client PING period. The client drops a connection after two unanswered PINGs, so a link that went silent (laptop
/// sleep, dropped tunnel) is noticed within about a minute instead of three, and proxies with a 60 s idle timeout see
/// traffic. Reconnects are unlimited by default.
const PING_INTERVAL: Duration = Duration::from_secs(20);

/// Connect to NATS as `user`. `url` may be `nats://`, `tls://`, `ws://` or `wss://`, so the same binary works over a
/// dev tunnel, a reverse proxy or a direct WebSocket endpoint. `creds` is a `.creds` file from `ants auth user`, and
/// `js_domain` the hub's JetStream domain, if it has one (see [`jetstream()`]). Credentials outdated or expiring soon
/// get a warning naming the command that renews them ([`crate::secrets::creds::warn`]).
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
    connect_options(url, options(creds, store, user, name)?, js_domain).await
}

/// [`connect`], also passing each event of the connection, after logging it, to `events`: a server error, a
/// disconnection or a failed reconnect, which the client otherwise retries forever.
pub async fn connect_watched(
    url: &str,
    creds: Option<&Path>,
    js_domain: Option<&JsDomain>,
    user: &UserId,
    name: &str,
    events: tokio::sync::mpsc::UnboundedSender<Event>,
) -> Result<(async_nats::Client, jetstream::Context)> {
    check_url(url)?;
    let options = options(creds, keystore::os(), user, name)?.event_callback(move |event| {
        let events = events.clone();
        async move {
            let _ = events.send(event.clone());
            log_event(event);
        }
    });
    connect_options(url, options, js_domain).await
}

async fn connect_options(
    url: &str,
    options: async_nats::ConnectOptions,
    js_domain: Option<&JsDomain>,
) -> Result<(async_nats::Client, jetstream::Context)> {
    let client = options.connect(url).await.with_context(|| format!("connecting to NATS at {}", redacted(url)))?;
    Ok((client.clone(), jetstream(client, js_domain)))
}

/// Delay before the second attempt of [`connect_retrying`], doubled after each failed one up to [`RETRY_MAX`].
pub(super) const RETRY_FIRST: Duration = Duration::from_secs(1);
pub(super) const RETRY_MAX: Duration = Duration::from_secs(30);

/// Like [`connect`], but waits for a server that cannot be reached yet, as the local nats-server when a service manager
/// starts the daemon first: a refused, timed-out or unresolved connection is retried for as long as it takes, after
/// `RETRY_FIRST` and then twice as long each time up to `RETRY_MAX`, with a warning each time. Once connected it
/// waits the same way for JetStream to answer, as a leaf node that listens before its uplink has joined the hub
/// answers JetStream requests with no responders or not at all. JetStream answers when it looks up the info of the
/// user's own inbox consumer ([`ants_proto::inbox_consumer`]) in the `ANTS` stream, found or not, which every user's
/// credentials allow: none but a human's get the stream's info, which lists every subject stored. What another attempt
/// cannot cure fails at once: an unreadable credentials file, a malformed URL or one carrying a user and password
/// ([`check_url`]), credentials the server rejects, a TLS error, a JetStream request the credentials do not permit.
/// Before all that, credentials whose seed is in the OS credential store wait the same way for a store that is locked
/// or not available yet ([`keystore::Unavailable`]), as before its user logs in; a store holding no seed for their key,
/// or another key's, fails at once. None when `stop` completes first. The connection tells the process's outage watch
/// when it cannot reach the server ([`super::outage`]).
pub async fn connect_retrying(
    url: &str,
    creds: Option<&Path>,
    js_domain: Option<&JsDomain>,
    user: &UserId,
    name: &str,
    stop: impl Future<Output = ()>,
) -> Result<Option<(async_nats::Client, jetstream::Context)>> {
    connect_retrying_with(url, creds, keystore::os(), js_domain, user, name, stop).await
}

/// [`connect_retrying`], reading a seed the credentials file keeps in `store` rather than in itself.
pub async fn connect_retrying_with(
    url: &str,
    creds: Option<&Path>,
    store: &dyn Store,
    js_domain: Option<&JsDomain>,
    user: &UserId,
    name: &str,
    stop: impl Future<Output = ()>,
) -> Result<Option<(async_nats::Client, jetstream::Context)>> {
    check_url(url)?;
    // A denied request just times out; the server's error naming it arrives through the event callback.
    let (denied_tx, mut denied) = tokio::sync::mpsc::unbounded_channel();
    let reporter = Reporter::new(url);
    let reporting = reporter.clone();
    let options = options(None, store, user, name)?.event_callback(move |event| {
        let denied_tx = denied_tx.clone();
        reporting.event(&event);
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
        let options = match creds {
            Some(path) => authenticate_retrying(options, path, store).await?,
            None => options,
        };
        let client = connect_until_reachable(&options, url, &shown, &reporter).await?;
        let js = jetstream(client.clone(), js_domain);
        reach_jetstream_retrying(&js, user, &mut denied, &shown).await?;
        Ok((client, js))
    };
    tokio::select! {
        connected = attempts => connected.map(Some),
        () = stop => Ok(None),
    }
}

/// `options` with the credentials at `path`, waiting, with backoff, while the OS credential store holding their seed
/// is locked.
async fn authenticate_retrying(
    options: async_nats::ConnectOptions,
    path: &Path,
    store: &dyn Store,
) -> Result<async_nats::ConnectOptions> {
    crate::secrets::creds::warn(path);
    let mut delay = RETRY_FIRST;
    loop {
        match authenticate(options.clone(), path, store) {
            Ok(options) => return Ok(options),
            Err(e) if keystore::is_unavailable(&e) => {
                tracing::warn!(
                    creds = %path.display(),
                    error = format!("{e:#}"),
                    retry_in = ?delay,
                    "waiting for the OS credential store to be unlocked"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
            Err(e) => return Err(e),
        }
    }
}

/// A client connected to `url` (`shown` redacted) with `options`, retrying, with backoff, while the server is not
/// reachable yet, which each failed attempt tells `reporter`.
async fn connect_until_reachable(
    options: &async_nats::ConnectOptions,
    url: &str,
    shown: &str,
    reporter: &Reporter,
) -> Result<async_nats::Client> {
    let mut delay = RETRY_FIRST;
    loop {
        match options.clone().connect(url).await {
            Ok(client) => {
                reporter.up();
                return Ok(client);
            }
            Err(e) if worth_retrying(&e) => {
                reporter.down();
                tracing::warn!(url = %shown, error = %e, retry_in = ?delay, "NATS server not reachable yet");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
            Err(e) => return Err(e).with_context(|| format!("connecting to NATS at {shown}")),
        }
    }
}

/// [`reach_jetstream`], retrying, with backoff, while no JetStream answers, unless the server reports a permissions
/// violation on `denied` first.
async fn reach_jetstream_retrying(
    js: &jetstream::Context,
    user: &UserId,
    denied: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    shown: &str,
) -> Result<()> {
    let mut delay = RETRY_FIRST;
    loop {
        let reached = tokio::select! {
            biased;
            Some(e) = denied.recv() => return Err(anyhow::anyhow!(e)).context("reaching JetStream"),
            reached = reach_jetstream(js, user) => reached,
        };
        match reached {
            // Creating the consumer or the stream it found missing is up to the caller.
            Ok(()) => return Ok(()),
            Err(e) if matches!(e.kind(), ConsumerInfoErrorKind::TimedOut | ConsumerInfoErrorKind::NoResponders) => {
                tracing::warn!(url = %shown, error = %e, retry_in = ?delay, "JetStream not reachable yet");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
            Err(e) => return Err(e).context("reaching JetStream"),
        }
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

/// Whether `e` is JetStream not answering: no responders, as through a leaf node whose uplink is down, or no answer in
/// time. A request JetStream answered, if only that what it names is missing, is not.
pub fn unanswered(e: &anyhow::Error) -> bool {
    use async_nats::jetstream::context::{
        AccountError, AccountErrorKind, ConsumerInfoError, CreateStreamError, CreateStreamErrorKind, PublishError,
        PublishErrorKind, RequestError, RequestErrorKind,
    };
    use async_nats::jetstream::stream::{DirectGetError, DirectGetErrorKind};
    e.chain().any(|e| {
        if let Some(e) = e.downcast_ref::<RequestError>() {
            matches!(e.kind(), RequestErrorKind::NoResponders | RequestErrorKind::TimedOut)
        } else if let Some(e) = e.downcast_ref::<async_nats::RequestError>() {
            matches!(e.kind(), async_nats::RequestErrorKind::NoResponders | async_nats::RequestErrorKind::TimedOut)
        } else if let Some(e) = e.downcast_ref::<AccountError>() {
            matches!(e.kind(), AccountErrorKind::JetStreamUnavailable | AccountErrorKind::TimedOut)
        } else if let Some(e) = e.downcast_ref::<CreateStreamError>() {
            matches!(e.kind(), CreateStreamErrorKind::JetStreamUnavailable | CreateStreamErrorKind::TimedOut)
        } else if let Some(e) = e.downcast_ref::<ConsumerInfoError>() {
            matches!(e.kind(), ConsumerInfoErrorKind::NoResponders | ConsumerInfoErrorKind::TimedOut)
        } else if let Some(e) = e.downcast_ref::<PublishError>() {
            matches!(e.kind(), PublishErrorKind::StreamNotFound | PublishErrorKind::TimedOut)
        } else if let Some(e) = e.downcast_ref::<DirectGetError>() {
            match e.kind() {
                DirectGetErrorKind::TimedOut => true,
                DirectGetErrorKind::ErrorResponse(code, _) => code == async_nats::StatusCode::NO_RESPONDERS,
                _ => false,
            }
        } else {
            false
        }
    })
}

/// The options of a connection as `user`, named `name`, with the credentials file `creds`, after warning about what
/// [`crate::secrets::creds::warn`] finds in it.
fn options(creds: Option<&Path>, store: &dyn Store, user: &UserId, name: &str) -> Result<async_nats::ConnectOptions> {
    let options = async_nats::ConnectOptions::new()
        .name(name)
        .ping_interval(PING_INTERVAL)
        .custom_inbox_prefix(inbox_prefix(user))
        .event_callback(|event| async move { log_event(event) });
    match creds {
        Some(path) => {
            crate::secrets::creds::warn(path);
            authenticate(options, path, store)
        }
        None => Ok(options),
    }
}

/// `options` authenticating with the credentials file at `path`: its user JWT, and the server's challenge signed with
/// its key, read once from the file or from `store` ([`crate::secrets::creds::load`]) and kept in memory to sign again on
/// every reconnect.
pub fn authenticate(
    options: async_nats::ConnectOptions,
    path: &Path,
    store: &dyn Store,
) -> Result<async_nats::ConnectOptions> {
    let (jwt, key) = crate::secrets::creds::load(path, store)?;
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
pub fn worth_retrying(e: &async_nats::ConnectError) -> bool {
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
pub(super) fn redacted(url: &str) -> String {
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
