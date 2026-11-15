//! Health and metrics over HTTP, for monitoring: `GET /healthz` and `GET /metrics` in the Prometheus text format, on
//! an address of its own, off unless configured. Shared by `ants daemon`, `ants bridge` and `ants webhook serve`, each
//! a [`Source`] of its own.
//!
//! The daemon's counters ([`Ant`]) are kept per ant in one registry ([`ant`]) whether or not anything listens: a few
//! atomics each. The JetStream storage usage [`crate::storage`] last measured is kept beside them ([`stored`]), and so
//! is the system memory [`crate::pressure`] last read ([`memory_available`]).

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::UserId;
use anyhow::{Context as _, Result};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::http::{HeaderValue, Method, Request, Response, StatusCode, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::roster::Workload;
use crate::sync::MutexExt as _;

/// Content type of the Prometheus text exposition format.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
/// How long a client may take to send its request head; the head itself is capped at hyper's smallest buffer.
const HEAD_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a connection may last, answer included.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
/// Smallest buffer hyper takes: the most of a request head read.
const MAX_HEAD: usize = 8192;
/// Connections served at once; more are closed unanswered.
const MAX_CONNECTIONS: usize = 8;

/// What a health and metrics endpoint reports.
pub trait Source: Send + Sync + 'static {
    /// Whether `/healthz` answers 200: the NATS connections are up.
    fn healthy(&self) -> bool;
    /// Write the metrics.
    fn render(&self, out: &mut Exposition);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
}

/// Metrics in the Prometheus text exposition format (version 0.0.4): each family's `# HELP` and `# TYPE` lines,
/// then its samples.
#[derive(Debug, Default)]
pub struct Exposition(String);

impl Exposition {
    /// Start the family `name`; its samples follow.
    pub fn family(&mut self, name: &str, kind: Kind, help: &str) {
        let kind = match kind {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
        };
        let help = help.replace('\\', "\\\\").replace('\n', "\\n");
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} {kind}");
    }

    /// One sample of the family `name`.
    pub fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.0.push_str(name);
        if !labels.is_empty() {
            self.0.push('{');
            for (i, (label, text)) in labels.iter().enumerate() {
                if i > 0 {
                    self.0.push(',');
                }
                let _ = write!(self.0, "{label}=\"");
                for c in text.chars() {
                    match c {
                        '\\' => self.0.push_str("\\\\"),
                        '"' => self.0.push_str("\\\""),
                        '\n' => self.0.push_str("\\n"),
                        c => self.0.push(c),
                    }
                }
                self.0.push('"');
            }
            self.0.push('}');
        }
        let _ = match value {
            v if v.is_nan() => writeln!(self.0, " NaN"),
            v if v == f64::INFINITY => writeln!(self.0, " +Inf"),
            v if v == f64::NEG_INFINITY => writeln!(self.0, " -Inf"),
            v => writeln!(self.0, " {v}"),
        };
    }

    /// A family of a single unlabelled sample.
    pub fn single(&mut self, name: &str, kind: Kind, help: &str, value: f64) {
        self.family(name, kind, help);
        self.sample(name, &[], value);
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// The answer to `method` on `path`: its status, content type and body.
pub fn route(method: &Method, path: &str, source: &dyn Source) -> (StatusCode, &'static str, String) {
    const TEXT: &str = "text/plain; charset=utf-8";
    if !matches!(path, "/healthz" | "/metrics") {
        return (StatusCode::NOT_FOUND, TEXT, "not found: try /healthz or /metrics\n".to_owned());
    }
    if method != Method::GET && method != Method::HEAD {
        return (StatusCode::METHOD_NOT_ALLOWED, TEXT, "GET only\n".to_owned());
    }
    if path == "/healthz" {
        return if source.healthy() {
            (StatusCode::OK, TEXT, "ok\n".to_owned())
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, TEXT, "unhealthy: not connected to NATS\n".to_owned())
        };
    }
    let mut out = Exposition::default();
    source.render(&mut out);
    (StatusCode::OK, CONTENT_TYPE, out.into_string())
}

/// The listener of a health and metrics endpoint, serving until dropped.
pub struct Listening {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Listening {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Listen on `addr` and serve `source` there until the returned [`Listening`] drops. Any address may be given; one
/// that is not loopback is warned about, as the endpoint has no authentication.
pub async fn start(addr: SocketAddr, source: Arc<dyn Source>) -> Result<Listening> {
    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "metrics on a non-loopback address: anyone who reaches it reads them, without authentication"
        );
    }
    let listener = TcpListener::bind(addr).await.with_context(|| format!("listening for metrics on {addr}"))?;
    let addr = listener.local_addr()?;
    tracing::info!("health on http://{addr}/healthz, metrics on http://{addr}/metrics");
    Ok(Listening { addr, task: tokio::spawn(serve(listener, source)) })
}

/// Answer requests from `listener` with [`route`], one request per connection, until dropped.
pub async fn serve(listener: TcpListener, source: Arc<dyn Source>) {
    let open = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!(error = %e, "accepting a metrics connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Ok(permit) = open.clone().try_acquire_owned() else { continue };
        let source = source.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                let (status, content_type, body) = route(request.method(), request.uri().path(), source.as_ref());
                let mut response = Response::new(Full::new(Bytes::from(body)));
                *response.status_mut() = status;
                response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
                if status == StatusCode::METHOD_NOT_ALLOWED {
                    response.headers_mut().insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
                }
                std::future::ready(Ok::<_, Infallible>(response))
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEAD_TIMEOUT)
                .max_buf_size(MAX_HEAD)
                .keep_alive(false)
                .serve_connection(TokioIo::new(stream), service);
            match tokio::time::timeout(CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => tracing::debug!(error = %e, "metrics connection"),
                Err(_) => tracing::debug!("metrics connection timed out"),
                Ok(Ok(())) => {}
            }
            drop(permit);
        });
    }
}

/// 1 for true, 0 for false.
pub fn flag(on: bool) -> f64 {
    if on { 1.0 } else { 0.0 }
}

/// Whether `client` is connected now.
pub fn connected(client: &async_nats::Client) -> bool {
    client.connection_state() == async_nats::connection::State::Connected
}

// ---- the daemon ------------------------------------------------------------------------------------------------

static ANTS: Mutex<BTreeMap<UserId, Arc<Ant>>> = Mutex::new(BTreeMap::new());

/// The counters of `name` in this process, kept from its first use on.
pub fn ant(name: &UserId) -> Arc<Ant> {
    ANTS.locked().entry(name.clone()).or_default().clone()
}

/// JetStream storage usage by domain (empty for none) and store, with the limit when known.
type Stored = BTreeMap<(String, &'static str), (u64, Option<u64>)>;
static STORAGE: Mutex<Stored> = Mutex::new(BTreeMap::new());

/// Note that JetStream store `store` (`file` or `memory`) of `domain` holds `used` bytes of at most `limit`.
pub fn stored(domain: Option<&str>, store: &'static str, used: u64, limit: Option<u64>) {
    STORAGE.locked().insert((domain.unwrap_or_default().to_owned(), store), (used, limit));
}

/// Bytes of system memory available at the latest read, if any.
static MEMORY_AVAILABLE: Mutex<Option<u64>> = Mutex::new(None);

/// Note that `bytes` of system memory are available.
pub fn memory_available(bytes: u64) {
    *MEMORY_AVAILABLE.locked() = Some(bytes);
}

/// What one ant's daemon did and is doing.
#[derive(Default)]
pub struct Ant {
    /// Daemons serving the ant: the ant is reported while one is.
    serving: AtomicUsize,
    live: Mutex<Option<Live>>,
    prompts_in_flight: AtomicU64,
    prompts: AtomicU64,
    retries: AtomicU64,
    dead_letters: AtomicU64,
    tokens: AtomicU64,
    cost: Mutex<f64>,
}

/// An ant's connection and workload while it is connected.
struct Live {
    client: async_nats::Client,
    workload: Arc<Workload>,
}

/// An ant reported while alive ([`Ant::serving`]).
pub struct Serving(Arc<Ant>);

impl Drop for Serving {
    fn drop(&mut self) {
        self.0.serving.fetch_sub(1, Ordering::Relaxed);
    }
}

/// An ant's connection reported while alive ([`Ant::connected`]).
pub struct Connected(Arc<Ant>);

impl Drop for Connected {
    fn drop(&mut self) {
        *self.0.live.locked() = None;
    }
}

/// A prompt counted in flight while alive ([`Ant::prompting`]).
pub struct Prompting<'a>(&'a Ant);

impl Drop for Prompting<'_> {
    fn drop(&mut self) {
        self.0.prompts_in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Ant {
    /// Report the ant until the guard drops; until [`Ant::connected`], as not connected.
    pub fn serving(self: &Arc<Self>) -> Serving {
        self.serving.fetch_add(1, Ordering::Relaxed);
        Serving(self.clone())
    }

    /// Report `client`'s connection state and `workload` until the guard drops.
    pub fn connected(self: &Arc<Self>, client: async_nats::Client, workload: Arc<Workload>) -> Connected {
        *self.live.locked() = Some(Live { client, workload });
        Connected(self.clone())
    }

    /// Count a prompt, in flight until the guard drops.
    pub fn prompting(&self) -> Prompting<'_> {
        self.prompts.fetch_add(1, Ordering::Relaxed);
        self.prompts_in_flight.fetch_add(1, Ordering::Relaxed);
        Prompting(self)
    }

    /// Count a failed attempt handed back for redelivery.
    pub fn retried(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a message stored as a dead letter.
    pub fn buried(&self) {
        self.dead_letters.fetch_add(1, Ordering::Relaxed);
    }

    /// Count what a harness reported spending.
    pub fn spent(&self, tokens: u64, cost: f64) {
        self.tokens.fetch_add(tokens, Ordering::Relaxed);
        if cost > 0.0 {
            *self.cost.locked() += cost;
        }
    }

    fn is_connected(&self) -> bool {
        self.live.locked().as_ref().is_some_and(|live| connected(&live.client))
    }
}

/// The ants this process serves: healthy while it serves at least one and every one is connected.
pub struct Daemon;

impl Daemon {
    fn serving() -> Vec<(UserId, Arc<Ant>)> {
        let ants = ANTS.locked();
        ants.iter()
            .filter(|(_, ant)| ant.serving.load(Ordering::Relaxed) > 0)
            .map(|(n, a)| (n.clone(), a.clone()))
            .collect()
    }
}

impl Source for Daemon {
    fn healthy(&self) -> bool {
        let serving = Self::serving();
        !serving.is_empty() && serving.iter().all(|(_, ant)| ant.is_connected())
    }

    fn render(&self, out: &mut Exposition) {
        fn load(n: &AtomicU64) -> f64 {
            n.load(Ordering::Relaxed) as f64
        }
        fn workload(ant: &Ant, of: fn(&Workload) -> usize) -> f64 {
            ant.live.locked().as_ref().map_or(0.0, |live| of(&live.workload) as f64)
        }
        type Family = (&'static str, Kind, &'static str, fn(&Ant) -> f64);
        let families: [Family; 10] = [
            ("ants_nats_connected", Kind::Gauge, "Whether the ant's NATS connection is up.", |a| {
                flag(a.is_connected())
            }),
            ("ants_harnesses", Kind::Gauge, "Harness processes alive.", |a| workload(a, Workload::harnesses)),
            (
                "ants_memory_low",
                Kind::Gauge,
                "Whether memory is low, so idle harnesses stop at once and fewer run.",
                |a| workload(a, |w| usize::from(w.memory_low().is_some())),
            ),
            (
                "ants_messages_in_hand",
                Kind::Gauge,
                "Messages being handled: prompting, or waiting for their thread or a harness.",
                |a| workload(a, Workload::handling),
            ),
            ("ants_prompts_in_flight", Kind::Gauge, "Prompts running in a harness.", |a| load(&a.prompts_in_flight)),
            ("ants_prompts_total", Kind::Counter, "Prompts started.", |a| load(&a.prompts)),
            ("ants_retries_total", Kind::Counter, "Failed attempts handed back for redelivery.", |a| load(&a.retries)),
            ("ants_dead_letters_total", Kind::Counter, "Messages stored as dead letters.", |a| load(&a.dead_letters)),
            ("ants_spend_tokens_total", Kind::Counter, "Tokens the harnesses reported.", |a| load(&a.tokens)),
            (
                "ants_spend_cost_total",
                Kind::Counter,
                "Cost the harnesses reported, in their currency (usually USD).",
                |a| *a.cost.locked(),
            ),
        ];
        let serving = Self::serving();
        for (name, kind, help, read) in families {
            out.family(name, kind, help);
            for (ant, counters) in &serving {
                out.sample(name, &[("ant", ant.as_str())], read(counters));
            }
        }
        if let Some(bytes) = *MEMORY_AVAILABLE.locked() {
            let help = "Bytes of system memory available at the latest check of memory pressure.";
            out.single("ants_memory_available_bytes", Kind::Gauge, help, bytes as f64);
        }
        let storage = STORAGE.locked();
        let (bytes, limit) = ("ants_jetstream_bytes", "ants_jetstream_limit_bytes");
        out.family(bytes, Kind::Gauge, "Bytes the account stores in JetStream, by domain and store (file or memory).");
        for ((domain, store), (used, _)) in storage.iter() {
            out.sample(bytes, &[("domain", domain), ("store", store)], *used as f64);
        }
        out.family(limit, Kind::Gauge, "The most bytes a JetStream store takes, where known.");
        for ((domain, store), (_, max)) in storage.iter() {
            if let Some(max) = max {
                out.sample(limit, &[("domain", domain), ("store", store)], *max as f64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(bool);

    impl Source for Fixed {
        fn healthy(&self) -> bool {
            self.0
        }

        fn render(&self, out: &mut Exposition) {
            out.single("ants_up", Kind::Gauge, "Always 1.", 1.0);
        }
    }

    #[test]
    fn the_exposition_has_help_and_type_lines_escaped_labels_and_prometheus_numbers() {
        let mut out = Exposition::default();
        out.family("ants_x_total", Kind::Counter, "Things\nwith a \\ in.");
        out.sample("ants_x_total", &[("ant", "a\"b\\c\nd"), ("outcome", "ok")], 3.0);
        out.sample("ants_x_total", &[], 0.25);
        out.single("ants_y", Kind::Gauge, "Odd values.", f64::INFINITY);
        out.sample("ants_y", &[], f64::NEG_INFINITY);
        out.sample("ants_y", &[], f64::NAN);
        out.sample("ants_y", &[], 1e21);
        assert_eq!(
            out.into_string(),
            "# HELP ants_x_total Things\\nwith a \\\\ in.\n\
             # TYPE ants_x_total counter\n\
             ants_x_total{ant=\"a\\\"b\\\\c\\nd\",outcome=\"ok\"} 3\n\
             ants_x_total 0.25\n\
             # HELP ants_y Odd values.\n\
             # TYPE ants_y gauge\n\
             ants_y +Inf\n\
             ants_y -Inf\n\
             ants_y NaN\n\
             ants_y 1000000000000000000000\n"
        );
    }

    #[test]
    fn requests_route_to_health_metrics_or_nothing() {
        let (healthy, sick) = (Fixed(true), Fixed(false));
        let get = |path: &str, source: &dyn Source| route(&Method::GET, path, source);
        assert_eq!(get("/healthz", &healthy), (StatusCode::OK, "text/plain; charset=utf-8", "ok\n".to_owned()));
        assert_eq!(get("/healthz", &sick).0, StatusCode::SERVICE_UNAVAILABLE);
        let (status, content_type, body) = get("/metrics", &sick);
        assert_eq!((status, content_type), (StatusCode::OK, CONTENT_TYPE));
        assert_eq!(body, "# HELP ants_up Always 1.\n# TYPE ants_up gauge\nants_up 1\n");
        assert_eq!(route(&Method::HEAD, "/metrics", &healthy).0, StatusCode::OK);
        assert_eq!(route(&Method::POST, "/metrics", &healthy).0, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(get("/", &healthy).0, StatusCode::NOT_FOUND);
        assert_eq!(get("/metrics/", &healthy).0, StatusCode::NOT_FOUND);
    }

    #[test]
    fn an_ant_is_reported_while_served_with_its_counters() {
        let name: UserId = "metrics-unit".parse().unwrap();
        let ant = ant(&name);
        let rendered = || {
            let mut out = Exposition::default();
            Daemon.render(&mut out);
            out.into_string()
        };
        assert!(!rendered().contains("metrics-unit"), "not reported before it is served");
        let serving = ant.serving();
        let prompting = ant.prompting();
        ant.retried();
        ant.buried();
        ant.spent(120, 0.5);
        ant.spent(30, 0.25);
        let text = rendered();
        for line in [
            "ants_nats_connected{ant=\"metrics-unit\"} 0",
            "ants_harnesses{ant=\"metrics-unit\"} 0",
            "ants_memory_low{ant=\"metrics-unit\"} 0",
            "ants_prompts_in_flight{ant=\"metrics-unit\"} 1",
            "ants_prompts_total{ant=\"metrics-unit\"} 1",
            "ants_retries_total{ant=\"metrics-unit\"} 1",
            "ants_dead_letters_total{ant=\"metrics-unit\"} 1",
            "ants_spend_tokens_total{ant=\"metrics-unit\"} 150",
            "ants_spend_cost_total{ant=\"metrics-unit\"} 0.75",
        ] {
            assert!(text.lines().any(|l| l == line), "no {line:?} in\n{text}");
        }
        assert!(!Daemon.healthy() || !text.contains("ants_nats_connected{ant=\"metrics-unit\"} 0"));
        drop(prompting);
        assert!(rendered().contains("ants_prompts_in_flight{ant=\"metrics-unit\"} 0"));
        drop(serving);
        assert!(!rendered().contains("metrics-unit"), "not reported once it stopped");
        stored(Some("metrics-unit"), "file", 900, Some(1000));
        memory_available(4096);
        stored(Some("metrics-unit"), "memory", 5, None);
        let text = rendered();
        for line in [
            "ants_jetstream_bytes{domain=\"metrics-unit\",store=\"file\"} 900",
            "ants_jetstream_bytes{domain=\"metrics-unit\",store=\"memory\"} 5",
            "ants_jetstream_limit_bytes{domain=\"metrics-unit\",store=\"file\"} 1000",
        ] {
            assert!(text.lines().any(|l| l == line), "no {line:?} in\n{text}");
        }
        assert!(!text.contains("ants_jetstream_limit_bytes{domain=\"metrics-unit\",store=\"memory\"}"));
        // Other tests read memory too, so only the sample's presence is certain.
        assert!(text.lines().any(|l| l.starts_with("ants_memory_available_bytes ")), "{text}");
    }
}
