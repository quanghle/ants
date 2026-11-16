//! `ants webhook serve`: take events from outside services over HTTP and deliver each to the ant or channel its
//! webhook names.
//!
//! Each `[[webhook]]` of the config file ([`Config`]) is its own NATS user, with credentials from
//! `ants auth user NAME --webhook --to ANT` or `--channel CHANNEL` ([`ants_proto::Permissions::for_webhook`]), or
//! with its entry and secret from `ants webhook add` ([`crate::cmd::webhook::add`]): it sends as itself, to its destination
//! only. `POST <path>/<name>` with the event:
//! - is authenticated before anything else: an HMAC-SHA256 signature of the body, a bearer token, a token in a
//!   named header, or a [Standard Webhooks](https://www.standardwebhooks.com) signature with a recent timestamp
//!   (401);
//! - is identified by an id from a header, a JSON field or the body's hash; an event already delivered answers
//!   200 with `"duplicate": true` and delivers nothing ([`WEBHOOKS_BUCKET`], and the stream's `Nats-Msg-Id`);
//! - becomes the text of a message to the ant, or of a channel post, on a new thread, with the mentions the config
//!   names, never taken from the event;
//! - answers 202 once the hub's stream stored it, else 503 with `Retry-After`: nothing is kept here.
//!
//! Each `[[human]]` is notified of tool requests and escalations through its sinks, with approve and reject links
//! answered at `<path>/_approval` ([`crate::notify`]).
//!
//! The server speaks plain HTTP, for a TLS-terminating reverse proxy in front of it.
//!
//! [`WEBHOOKS_BUCKET`]: ants_proto::WEBHOOKS_BUCKET

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use ants_daemon::bus;
use ants_daemon::metrics::{self, Exposition, Kind};
use ants_daemon::sync::{MutexExt as _, RwLockExt as _};
use ants_proto::{JsDomain, ThreadId, UserId, webhook_seen_key};
use anyhow::Result;
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{OnceCell, Semaphore, mpsc, watch};
use tokio::task::{AbortHandle, JoinSet};

use self::config::Auth;
use self::requests::{Event, Refusal, refuse};
use crate::notify;
use crate::serve::Ended;

pub use self::config::{Config, SecretSource, Webhook, outline};
pub(crate) use self::config::{Listing, check_creds, read_secret, signing_key};
#[cfg(test)]
pub use self::delivery::ensure_bucket;
pub(crate) use self::delivery::{Bus, Delivered, Delivery};

mod config;
mod delivery;
mod requests;

/// Where `ants webhook serve` listens unless told otherwise: loopback, behind the reverse proxy.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:8424";
const DEFAULT_PATH: &str = "/hooks";
const DEFAULT_TEXT: &str = "[webhook {webhook}] {id}\n\n{body}";
const DEFAULT_SIGNATURE_HEADER: &str = "x-hub-signature-256";
/// How far a Standard Webhooks timestamp may be from now, unless the entry's `tolerance` says otherwise.
const DEFAULT_TOLERANCE: Duration = Duration::from_mins(5);
/// The longest `tolerance`, well within the 7 days an event's id is remembered.
const MAX_TOLERANCE: Duration = Duration::from_hours(24);
const WEBHOOK_ID: HeaderName = HeaderName::from_static("webhook-id");
const WEBHOOK_TIMESTAMP: HeaderName = HeaderName::from_static("webhook-timestamp");
const WEBHOOK_SIGNATURE: HeaderName = HeaderName::from_static("webhook-signature");
/// Largest body taken: the message must stay under the NATS payload limit once JSON-escaped.
const MAX_BODY: usize = 256 * 1024;
/// Most of a refused body read, and dropped, before answering.
const DRAIN: usize = 4 * MAX_BODY;
/// How long a request may take, body and delivery included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// How long, past the request timeout, the answers of the requests under way at a stop may take to go out.
const ANSWER_GRACE: Duration = Duration::from_secs(1);
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// Requests handled at once; more answer 503.
const MAX_IN_FLIGHT: usize = 64;
/// What a 503 asks the sender to wait before retrying.
const RETRY_AFTER: &str = "5";
/// How long the hub's stream may take to store an event.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// Hex digits of the event id's SHA-256 in its key.
const ID_HASH_LEN: usize = 32;
/// Least time between two warnings about rejected requests of one webhook.
const WARN_EVERY: Duration = Duration::from_secs(60);

/// `~/.ants/webhooks.toml`.
pub fn default_path() -> Result<PathBuf> {
    Ok(ants_daemon::config::default_path()?.with_file_name("webhooks.toml"))
}

// ---- server ----------------------------------------------------------------------------------------------------

struct Hook {
    config: Webhook,
    bus: OnceCell<Bus>,
    last_warned: Mutex<Option<Instant>>,
    /// Kept across reloads that restart the webhook.
    outcomes: Arc<Outcomes>,
}

/// How a webhook's requests ended, for the metrics.
#[derive(Default)]
struct Outcomes {
    delivered: AtomicU64,
    duplicate: AtomicU64,
    refused: AtomicU64,
    failed: AtomicU64,
}

struct Server {
    /// Swapped whole on a reload; requests under way finish with the routes they started with.
    routes: RwLock<Arc<Routes>>,
    in_flight: Arc<Semaphore>,
    /// How long a request may take ([`REQUEST_TIMEOUT`]).
    timeout: Duration,
}

/// Where requests go, as of the config last loaded.
struct Routes {
    /// `<path>/`, before each webhook's name.
    prefix: String,
    hooks: HashMap<String, Arc<Hook>>,
    /// Answers approval links, at `<path>/_approval`.
    answers: Option<notify::Answers>,
    humans: Vec<Arc<notify::Notified>>,
}

impl Server {
    fn routes(&self) -> Arc<Routes> {
        self.routes.read_locked().clone()
    }
}

/// How [`serve`] reloads its config: on each trigger, with `load`.
pub struct Reload<L> {
    pub triggers: crate::reload::Triggers,
    pub load: L,
}

/// Serve the webhooks of `config` on `listener`, and notify its humans, until `stop` or a webhook's or a human's
/// credentials fail. Each webhook and human connects as itself to the config's `nats`, else to `nats`; until it is
/// connected, its requests answer 503. Health and metrics are served on `config.metrics`, if given.
///
/// On each trigger of `reload` the config is loaded again: webhooks and humans newly listed start, those no longer
/// listed stop, those whose entry or credentials file changed start again, and the others carry on untouched, as
/// does the listener. A config that fails to load is logged and the one served stays.
///
/// Once `stop` ends, no connection is accepted any more and the requests under way get the rest of their request
/// timeout to finish and be answered, before returning how `stop` ended.
pub async fn serve<L: FnMut() -> Result<Config>>(
    config: Config,
    nats: &str,
    js_domain: Option<&JsDomain>,
    listener: TcpListener,
    reload: Reload<L>,
    stop: impl Future<Output = Ended>,
) -> Result<Ended> {
    serve_timing(config, nats, js_domain, listener, reload, stop, REQUEST_TIMEOUT).await
}

/// [`serve`], with requests limited to `timeout`.
async fn serve_timing<L: FnMut() -> Result<Config>>(
    config: Config,
    nats: &str,
    js_domain: Option<&JsDomain>,
    listener: TcpListener,
    mut reload: Reload<L>,
    stop: impl Future<Output = Ended>,
    timeout: Duration,
) -> Result<Ended> {
    bus::check_url(nats)?;
    let mut tasks = JoinSet::new();
    let (failed_tx, mut failed) = mpsc::unbounded_channel();
    let mut running = Running::new(nats, js_domain, &config);
    let metrics_addr = config.metrics;
    let routes = running.apply(config, &mut tasks, &failed_tx);
    let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let server = Arc::new(Server { routes: RwLock::new(Arc::new(routes)), in_flight, timeout });
    let _metrics = match metrics_addr {
        Some(addr) => Some(metrics::start(addr, server.clone()).await?),
        None => None,
    };
    let (draining_tx, draining) = watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(stop);
    let ended = loop {
        while tasks.try_join_next().is_some() {}
        while connections.try_join_next().is_some() {}
        let stream = tokio::select! {
            Some(e) = failed.recv() => return Err(e),
            ended = &mut stop => break ended,
            trigger = reload.triggers.next() => {
                match (reload.load)() {
                    Ok(config) => {
                        let routes = running.apply(config, &mut tasks, &failed_tx);
                        *server.routes.write_locked() = Arc::new(routes);
                        tracing::info!(on = trigger.describe(), "reloaded the webhook config");
                    }
                    Err(e) => tracing::error!(
                        on = trigger.describe(),
                        error = format!("{e:#}"),
                        "reloading the webhook config failed; still serving the one loaded before"
                    ),
                }
                continue;
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a webhook connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let (server, mut draining) = (server.clone(), draining.clone());
        connections.spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let server = server.clone();
                async move { Ok::<_, Infallible>(server.handle(request).await) }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_TIMEOUT)
                .serve_connection(TokioIo::new(stream), service);
            tokio::pin!(connection);
            let served = tokio::select! {
                served = connection.as_mut() => served,
                // An idle connection closes at once, one with a request under way once it is answered.
                _ = async { draining.wait_for(|draining| *draining).await.is_ok() } => {
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            };
            if let Err(e) = served {
                tracing::debug!(error = %e, "webhook connection");
            }
        });
    };
    drop(listener);
    let _ = draining_tx.send(true);
    let in_flight = MAX_IN_FLIGHT - server.in_flight.available_permits();
    tracing::info!(in_flight, "stopping: accepting no more webhook requests, finishing those under way");
    let grace = timeout + ANSWER_GRACE;
    if tokio::time::timeout(grace, async { while connections.join_next().await.is_some() {} }).await.is_err() {
        tracing::warn!(connections = connections.len(), "stopping with connections still open after {grace:?}");
    }
    Ok(ended)
}

/// The webhooks and humans served, each with the task connecting it and the contents of its credentials file when
/// it started ([`crate::serve::creds_snapshot`]).
struct Running {
    default_nats: String,
    default_js_domain: Option<JsDomain>,
    nats: String,
    js_domain: Option<JsDomain>,
    listen: Option<String>,
    metrics: Option<SocketAddr>,
    path: String,
    links: Option<Arc<notify::Links>>,
    hooks: HashMap<UserId, Started<Hook>>,
    humans: HashMap<UserId, Started<notify::Notified>>,
}

struct Started<T> {
    served: Arc<T>,
    creds: Option<Vec<u8>>,
    task: AbortHandle,
}

impl Running {
    fn new(nats: &str, js_domain: Option<&JsDomain>, config: &Config) -> Self {
        Self {
            default_nats: nats.to_owned(),
            default_js_domain: js_domain.cloned(),
            nats: String::new(),
            js_domain: None,
            listen: config.listen.clone(),
            metrics: config.metrics,
            path: config.path.clone(),
            links: None,
            hooks: HashMap::new(),
            humans: HashMap::new(),
        }
    }

    /// Serve `config`: start what is new, stop what is gone, start again what changed (all of it with another hub or
    /// domain, every human with other links), and return the routes to it.
    fn apply(
        &mut self,
        config: Config,
        tasks: &mut JoinSet<()>,
        failed: &mpsc::UnboundedSender<anyhow::Error>,
    ) -> Routes {
        if config.listen != self.listen || config.metrics != self.metrics {
            tracing::warn!("listen and metrics of the webhook config take effect on a restart only");
        }
        if config.path != self.path {
            tracing::info!("webhooks now at {}/<webhook>", config.path);
            self.path = config.path.clone();
        }
        let nats = config.nats.clone().unwrap_or_else(|| self.default_nats.clone());
        let js_domain = config.js_domain.clone().or_else(|| self.default_js_domain.clone());
        let hub_changed = (&nats, &js_domain) != (&self.nats, &self.js_domain);
        (self.nats, self.js_domain) = (nats, js_domain);
        let links = config.links.map(Arc::new);
        let links_changed = links != self.links;
        self.links = links;

        self.apply_hooks(config.webhooks, hub_changed, tasks, failed);
        self.apply_humans(config.humans, hub_changed || links_changed, tasks, failed);

        let mut notified: Vec<_> = self.humans.values().map(|h| h.served.clone()).collect();
        notified.sort_by(|a, b| a.config.name.cmp(&b.config.name));
        let answers = self.links.clone().map(|links| notify::Answers {
            humans: notified.iter().map(|human| (human.config.name.clone(), human.clone())).collect(),
            links,
        });
        Routes {
            prefix: format!("{}/", config.path),
            hooks: self.hooks.iter().map(|(name, hook)| (name.to_string(), hook.served.clone())).collect(),
            answers,
            humans: notified,
        }
    }

    /// Start the new `webhooks`, stop those gone, and restart those changed, or all of them when `hub_changed`.
    fn apply_hooks(
        &mut self,
        webhooks: Vec<Webhook>,
        hub_changed: bool,
        tasks: &mut JoinSet<()>,
        failed: &mpsc::UnboundedSender<anyhow::Error>,
    ) {
        let mut hooks = HashMap::new();
        for webhook in webhooks {
            let name = webhook.name.clone();
            let creds = crate::serve::creds_snapshot(webhook.creds.as_deref());
            let outcomes = match self.hooks.remove(&name) {
                Some(old) if !hub_changed && old.served.config == webhook && old.creds == creds => {
                    hooks.insert(name, old);
                    continue;
                }
                Some(old) => {
                    tracing::info!(webhook = %name, "restarting the webhook: its entry, credentials or hub changed");
                    old.task.abort();
                    old.served.outcomes.clone()
                }
                None => Arc::default(),
            };
            let hook =
                Arc::new(Hook { config: webhook, bus: OnceCell::new(), last_warned: Mutex::default(), outcomes });
            let (served, nats, domain, failed) =
                (hook.clone(), self.nats.clone(), self.js_domain.clone(), failed.clone());
            let task = tasks.spawn(async move {
                if let Err(e) = connect(&served, &nats, domain.as_ref()).await {
                    let _ = failed.send(e.context(format!("webhook {}", served.config.name)));
                }
            });
            hooks.insert(name, Started { served: hook, creds, task });
        }
        for (name, old) in self.hooks.drain() {
            tracing::info!(webhook = %name, "stopping the webhook no longer listed");
            old.task.abort();
        }
        self.hooks = hooks;
    }

    /// Start the notifications of the new humans `listed`, stop those gone, and restart those changed, or all of them when
    /// `restart_all`.
    fn apply_humans(
        &mut self,
        listed: Vec<notify::Human>,
        restart_all: bool,
        tasks: &mut JoinSet<()>,
        failed: &mpsc::UnboundedSender<anyhow::Error>,
    ) {
        let mut humans = HashMap::new();
        for human in listed {
            let name = human.name.clone();
            let creds = crate::serve::creds_snapshot(human.creds.as_deref());
            match self.humans.remove(&name) {
                Some(old) if !restart_all && old.served.config == human && old.creds == creds => {
                    humans.insert(name, old);
                    continue;
                }
                Some(old) => {
                    tracing::info!(human = %name, "restarting the human's notifications: its entry, credentials, links or hub changed");
                    old.task.abort();
                }
                None => {}
            }
            let notified = Arc::new(notify::Notified::new(human));
            let (served, links, nats, domain, failed) =
                (notified.clone(), self.links.clone(), self.nats.clone(), self.js_domain.clone(), failed.clone());
            let task = tasks.spawn(async move {
                let name = served.config.name.clone();
                if let Err(e) = notify::run(served, links, nats, domain).await {
                    let _ = failed.send(e.context(format!("human {name}")));
                }
            });
            humans.insert(name, Started { served: notified, creds, task });
        }
        for (name, old) in self.humans.drain() {
            tracing::info!(human = %name, "stopping the notifications of the human no longer listed");
            old.task.abort();
        }
        self.humans = humans;
    }
}

/// Connect `hook` as itself, and open the bucket of delivered events, waiting for both as long as it takes.
async fn connect(hook: &Hook, nats: &str, js_domain: Option<&JsDomain>) -> Result<()> {
    let config = &hook.config;
    let bus = Bus::connect(&config.name, config.creds.as_deref(), nats, js_domain, "ants-webhook").await?;
    tracing::info!(webhook = %config.name, to = ?config.destination, "webhook ready");
    let _ = hook.bus.set(bus);
    Ok(())
}

impl Server {
    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Ok(_permit) = self.in_flight.clone().try_acquire_owned() else {
            return unavailable("too many requests at once");
        };
        match self.accept(request).await {
            Ok(response) => response,
            Err(Refusal(StatusCode::SERVICE_UNAVAILABLE, why)) => unavailable(&why),
            Err(Refusal(status, why)) => {
                let mut response = reply(status, &why);
                if status == StatusCode::METHOD_NOT_ALLOWED {
                    response.headers_mut().insert(header::ALLOW, HeaderValue::from_static("POST"));
                }
                response
            }
        }
    }

    /// Answer `request` within [`Server::timeout`], counting a webhook's outcome, a request timed out included.
    async fn accept(&self, request: Request<Incoming>) -> Result<Response<Full<Bytes>>, Refusal> {
        let (parts, body) = request.into_parts();
        let routes = self.routes();
        let name = parts.uri.path().strip_prefix(&routes.prefix);
        if let Some(answers) = routes.answers.as_ref().filter(|_| name == notify::APPROVAL_PATH.strip_prefix('/')) {
            return self
                .in_time(Box::pin(async {
                    if let Err(e) = read_body(body, Body::Drain).await {
                        return refuse(StatusCode::BAD_REQUEST, format!("reading the body: {e}"));
                    }
                    Ok(answers.answer(&parts.method, parts.uri.query()).await)
                }))
                .await;
        }
        let Some(hook) = name.and_then(|name| routes.hooks.get(name)) else {
            return self
                .in_time(async {
                    let _ = read_body(body, Body::Drain).await;
                    refuse(StatusCode::NOT_FOUND, "no such webhook")
                })
                .await;
        };
        let outcome = self.in_time(hook.take(parts, body)).await;
        hook.outcomes.count(&outcome);
        outcome
    }

    /// What `answering` answers, or 503 once it took longer than [`Server::timeout`].
    async fn in_time<T>(&self, answering: impl Future<Output = Result<T, Refusal>>) -> Result<T, Refusal> {
        match tokio::time::timeout(self.timeout, answering).await {
            Ok(answered) => answered,
            Err(_) => refuse(StatusCode::SERVICE_UNAVAILABLE, "the request took too long"),
        }
    }
}

/// Healthy while every webhook and human is connected to the hub.
impl metrics::Source for Server {
    fn healthy(&self) -> bool {
        let routes = self.routes();
        routes.hooks.values().all(|hook| hook.connected()) && routes.humans.iter().all(|human| human.connected())
    }

    fn render(&self, out: &mut Exposition) {
        let routes = self.routes();
        let mut hooks: Vec<_> = routes.hooks.values().collect();
        hooks.sort_by(|a, b| a.config.name.cmp(&b.config.name));
        out.family("ants_nats_connected", Kind::Gauge, "Whether the user's connection to NATS is up (1) or not (0).");
        for hook in &hooks {
            out.sample("ants_nats_connected", &[("user", hook.config.name.as_str())], metrics::flag(hook.connected()));
        }
        for human in &routes.humans {
            let name = human.config.name.as_str();
            out.sample("ants_nats_connected", &[("user", name)], metrics::flag(human.connected()));
        }
        let in_flight = MAX_IN_FLIGHT - self.in_flight.available_permits();
        out.single("ants_webhook_requests_in_flight", Kind::Gauge, "Requests being answered.", in_flight as f64);
        out.family(
            "ants_webhook_events_total",
            Kind::Counter,
            "Requests to a webhook: delivered, duplicate (delivered before), refused (4xx) or failed (503).",
        );
        for hook in &hooks {
            let o = &hook.outcomes;
            for (outcome, n) in [
                ("delivered", &o.delivered),
                ("duplicate", &o.duplicate),
                ("refused", &o.refused),
                ("failed", &o.failed),
            ] {
                let labels = [("webhook", hook.config.name.as_str()), ("outcome", outcome)];
                out.sample("ants_webhook_events_total", &labels, n.load(Ordering::Relaxed) as f64);
            }
        }
    }
}

impl Outcomes {
    fn count(&self, outcome: &Result<Response<Full<Bytes>>, Refusal>) {
        let counter = match outcome {
            Ok(response) if response.status() == StatusCode::OK => &self.duplicate,
            Ok(_) => &self.delivered,
            Err(Refusal(StatusCode::SERVICE_UNAVAILABLE, _)) => &self.failed,
            Err(_) => &self.refused,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

impl Hook {
    fn connected(&self) -> bool {
        self.bus.get().is_some_and(|bus| metrics::connected(&bus.client))
    }

    /// Take a request to this webhook: check it, and deliver its event.
    async fn take(&self, parts: hyper::http::request::Parts, body: Incoming) -> Result<Response<Full<Bytes>>, Refusal> {
        let refused = if parts.method != Method::POST {
            Some(Refusal(StatusCode::METHOD_NOT_ALLOWED, "webhooks take POST only".to_owned()))
        } else if !self.config.authorized_before_body(&parts.headers) {
            self.warn_rejected();
            let why = match self.config.auth {
                Auth::Standard { .. } => "missing webhook-id or webhook-signature, or no recent webhook-timestamp",
                _ => "missing or wrong token",
            };
            Some(Refusal(StatusCode::UNAUTHORIZED, why.to_owned()))
        } else {
            None
        };
        let body = read_body(body, if refused.is_none() { Body::Keep } else { Body::Drain }).await;
        if let Some(refused) = refused {
            return Err(refused);
        }
        let body = match body {
            Ok(Some(body)) => body,
            Ok(None) => {
                return refuse(StatusCode::PAYLOAD_TOO_LARGE, format!("bodies are limited to {MAX_BODY} bytes"));
            }
            Err(e) => return refuse(StatusCode::BAD_REQUEST, format!("reading the body: {e}")),
        };
        if !self.config.authorized(&parts.headers, &body) {
            self.warn_rejected();
            return refuse(StatusCode::UNAUTHORIZED, "missing or wrong signature");
        }
        let event = self.config.event(&parts.headers, &body)?;
        let Some(bus) = self.bus.get() else { return refuse(StatusCode::SERVICE_UNAVAILABLE, "not connected yet") };
        if !metrics::connected(&bus.client) {
            return refuse(StatusCode::SERVICE_UNAVAILABLE, "the hub is not reachable");
        }
        self.deliver(bus, event).await.map_err(|e| {
            tracing::warn!(webhook = %self.config.name, error = format!("{e:#}"), "delivering an event");
            Refusal(StatusCode::SERVICE_UNAVAILABLE, "the hub did not store the event".to_owned())
        })
    }
}

/// Whether [`read_body`] keeps the body read, or only drains it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Body {
    Keep,
    Drain,
}

/// The body, empty unless [`Body::Keep`], or None when it is longer than [`MAX_BODY`]. The rest of the body is read
/// and dropped up to [`DRAIN`], so the answer reaches the sender rather than a reset connection.
async fn read_body(mut body: Incoming, read: Body) -> Result<Option<Vec<u8>>, hyper::Error> {
    let (mut kept, mut length) = (Vec::new(), 0);
    while let Some(frame) = body.frame().await {
        let Ok(data) = frame?.into_data() else { continue };
        length += data.len();
        if length > DRAIN {
            break;
        }
        if read == Body::Keep && length <= MAX_BODY {
            kept.extend_from_slice(&data);
        }
    }
    Ok((length <= MAX_BODY).then_some(kept))
}

impl Hook {
    /// Deliver `event` unless it was delivered already, and answer once the hub's stream stored it.
    async fn deliver(&self, bus: &Bus, event: Event) -> Result<Response<Full<Bytes>>> {
        let hook = &self.config;
        let delivered = bus
            .deliver(Delivery {
                from: &hook.name,
                destination: &hook.destination,
                mentions: &hook.mentions,
                owner: hook.owner.as_ref(),
                allow: hook.allow.as_deref(),
                key: webhook_seen_key(&hook.name, &event.hash),
                message_id: format!("webhook.{}.{}", hook.name, event.hash),
                text: event.text,
            })
            .await?;
        if delivered.duplicate {
            return Ok(accepted(StatusCode::OK, delivered.thread.as_ref(), delivered.duplicate));
        }
        if let Some(thread) = &delivered.thread {
            tracing::info!(webhook = %hook.name, %thread, "event delivered");
        }
        Ok(accepted(StatusCode::ACCEPTED, delivered.thread.as_ref(), delivered.duplicate))
    }

    fn warn_rejected(&self) {
        let mut last = self.last_warned.locked();
        if last.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
            *last = Some(Instant::now());
            tracing::warn!(webhook = %self.config.name, "rejected a request that failed authentication");
        }
    }
}

fn accepted(status: StatusCode, thread: Option<&ThreadId>, duplicate: bool) -> Response<Full<Bytes>> {
    let body = match thread {
        Some(thread) => json!({ "thread": thread, "duplicate": duplicate }),
        None => json!({ "duplicate": duplicate }),
    };
    let mut response = reply(status, &body.to_string());
    response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn unavailable(why: &str) -> Response<Full<Bytes>> {
    let mut response = reply(StatusCode::SERVICE_UNAVAILABLE, why);
    response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static(RETRY_AFTER));
    response
}

fn reply(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(text.to_owned())));
    *response.status_mut() = status;
    response
}

#[cfg(test)]
pub(crate) mod tests;
