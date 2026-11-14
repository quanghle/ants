//! `ants webhook serve`: take events from outside services over HTTP and deliver each to the ant or channel its
//! webhook names.
//!
//! Each `[[webhook]]` of the config file ([`Config`]) is its own NATS user, with credentials from
//! `ants auth user NAME --webhook --to ANT` or `--channel CHANNEL` ([`ants_proto::Permissions::for_webhook`]): it
//! sends as itself, to its destination only. `POST <path>/<name>` with the event:
//! - is authenticated before anything else: an HMAC-SHA256 signature of the body, or a bearer token (401);
//! - is identified by an id from a header, a JSON field or the body's hash; an event already delivered answers
//!   200 with `"duplicate": true` and delivers nothing ([`WEBHOOKS_BUCKET`], and the stream's `Nats-Msg-Id`);
//! - becomes the text of a message to the ant, or of a channel post, on a new thread, with the mentions the config
//!   names, never taken from the event;
//! - answers 202 once the hub's stream stored it, else 503 with `Retry-After`: nothing is kept here.
//!
//! The server speaks plain HTTP, for a TLS-terminating reverse proxy in front of it.

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ants_daemon::bus;
use ants_proto::{
    CHANNELS_BUCKET, ChannelEntry, ChannelId, ChannelPost, Destination, Envelope, JsDomain, ThreadId, UserId,
    WEBHOOK_RETENTION, WEBHOOKS_BUCKET, subject, webhook_seen_key,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::DirectGetErrorKind;
use async_nats::jetstream::{self, kv, stream};
use data_encoding::{HEXLOWER, HEXLOWER_PERMISSIVE};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use ring::{digest, hmac};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{OnceCell, Semaphore, mpsc};
use tokio::task::JoinSet;

/// Where `ants webhook serve` listens unless told otherwise: loopback, behind the reverse proxy.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:8424";
const DEFAULT_PATH: &str = "/hooks";
const DEFAULT_TEXT: &str = "[webhook {webhook}] {id}\n\n{body}";
const DEFAULT_SIGNATURE_HEADER: &str = "x-hub-signature-256";
/// Largest body taken: the message must stay under the NATS payload limit once JSON-escaped.
const MAX_BODY: usize = 256 * 1024;
/// Most of a refused body read, and dropped, before answering.
const DRAIN: usize = 4 * MAX_BODY;
/// How long a request may take, body and delivery included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
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

/// The bucket of delivered events ([`WEBHOOKS_BUCKET`]), created when missing and the credentials allow it (a
/// human's): `ants init` creates it.
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(WEBHOOKS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: WEBHOOKS_BUCKET.to_owned(),
        description: "Events each webhook delivered, so retries deliver nothing new".to_owned(),
        history: 1,
        max_age: WEBHOOK_RETENTION,
        storage: stream::StorageType::File,
        ..Default::default()
    })
    .await
    .context("creating the webhooks bucket (with credentials, `ants init` as a human creates it)")
}

// ---- config ----------------------------------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    listen: Option<String>,
    path: Option<String>,
    nats: Option<String>,
    js_domain: Option<JsDomain>,
    #[serde(default, rename = "webhook")]
    webhooks: Vec<Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    name: UserId,
    creds: Option<PathBuf>,
    auth: AuthKind,
    secret_env: Option<String>,
    secret_file: Option<PathBuf>,
    signature_header: Option<String>,
    id: Option<String>,
    ant: Option<UserId>,
    channel: Option<ChannelId>,
    mention: Option<Vec<UserId>>,
    #[serde(default)]
    broadcast: bool,
    owner: Option<UserId>,
    text: Option<String>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
enum AuthKind {
    HmacSha256,
    Bearer,
}

/// The config file of `ants webhook serve`, checked.
pub struct Config {
    pub listen: Option<String>,
    /// Requests go to `<path>/<webhook>`.
    pub path: String,
    pub nats: Option<String>,
    pub js_domain: Option<JsDomain>,
    pub webhooks: Vec<Webhook>,
}

/// One `[[webhook]]`.
pub struct Webhook {
    pub name: UserId,
    pub creds: Option<PathBuf>,
    auth: Auth,
    id: IdSource,
    pub destination: Destination,
    mentions: Mentions,
    owner: Option<UserId>,
    text: Vec<Piece>,
}

enum Auth {
    /// `sha256=<hex>` of the body's HMAC-SHA256 in the header.
    Hmac { key: hmac::Key, header: HeaderName },
    /// `Authorization: Bearer <token>`.
    Bearer(String),
}

enum IdSource {
    Header(HeaderName),
    Json(String),
    BodySha256,
}

/// Whom a channel webhook's posts mention.
#[derive(Debug, PartialEq)]
enum Mentions {
    /// Nobody: the channel's messenger triages them.
    Nobody,
    Listed(Vec<UserId>),
    /// Every member when the event arrives.
    Members,
}

#[derive(Debug, PartialEq)]
enum Piece {
    Text(String),
    Body,
    Id,
    Webhook,
    Header(HeaderName),
    Json(String),
}

impl Config {
    /// Read and check the config file at `path`, and the credentials of each webhook against its destination.
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading webhook config {}", path.display()))?;
        let home = std::env::var_os("HOME").map(PathBuf::from).context("no home directory")?;
        let config = Self::parse(&text, &std::path::absolute(path)?, &home, |name| std::env::var(name).ok())?;
        for hook in &config.webhooks {
            hook.check_creds().with_context(|| format!("webhook config {}", path.display()))?;
        }
        Ok(config)
    }

    /// Parse the text of the config file at `path`: expand `~` to `home`, resolve relative paths against the file's
    /// directory and read each secret, from `env` or its file.
    fn parse(text: &str, path: &Path, home: &Path, env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let shown = path.display();
        let file: File = toml::from_str(text).with_context(|| format!("parsing webhook config {shown}"))?;
        let base = path.parent().unwrap_or(Path::new("."));
        let resolve = |p: &Path| match p.strip_prefix("~") {
            Ok(rest) => home.join(rest),
            Err(_) => base.join(p),
        };
        let path = file.path.as_deref().unwrap_or(DEFAULT_PATH).trim_end_matches('/').to_owned();
        if !path.is_empty() && !path.starts_with('/') {
            bail!("webhook config {shown}: path {path:?} must start with /");
        }
        if file.webhooks.is_empty() {
            bail!("webhook config {shown}: no [[webhook]] entries: nothing to serve");
        }
        let mut webhooks: Vec<Webhook> = Vec::with_capacity(file.webhooks.len());
        for entry in file.webhooks {
            let name = entry.name.clone();
            if webhooks.iter().any(|w| w.name == name) {
                bail!("webhook config {shown}: webhook {name} is listed twice");
            }
            let hook = Webhook::from_entry(entry, &resolve, &env)
                .with_context(|| format!("webhook config {shown}: webhook {name}"))?;
            webhooks.push(hook);
        }
        Ok(Self { listen: file.listen, path, nats: file.nats, js_domain: file.js_domain, webhooks })
    }
}

impl Webhook {
    fn from_entry(
        entry: Entry,
        resolve: &impl Fn(&Path) -> PathBuf,
        env: &impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        if entry.name.is_human() {
            bail!("the default human is no webhook");
        }
        let secret = match (&entry.secret_env, &entry.secret_file) {
            (Some(var), None) => env(var).with_context(|| format!("environment variable {var} is not set"))?,
            (None, Some(file)) => read_secret(&resolve(file))?,
            _ => bail!("give exactly one of secret_env and secret_file"),
        };
        let secret = secret.trim().to_owned();
        if secret.is_empty() {
            bail!("empty secret");
        }
        let auth = match entry.auth {
            AuthKind::HmacSha256 => {
                let header = entry.signature_header.as_deref().unwrap_or(DEFAULT_SIGNATURE_HEADER);
                let header = HeaderName::from_bytes(header.as_bytes())
                    .with_context(|| format!("signature_header {header:?}"))?;
                Auth::Hmac { key: hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()), header }
            }
            AuthKind::Bearer if entry.signature_header.is_some() => {
                bail!("signature_header goes with auth = \"hmac-sha256\" only")
            }
            AuthKind::Bearer => Auth::Bearer(secret),
        };
        let id = match entry.id.as_deref() {
            None | Some("body-sha256") => IdSource::BodySha256,
            Some(id) => match placeholder(id)? {
                Piece::Header(name) => IdSource::Header(name),
                Piece::Json(pointer) => IdSource::Json(pointer),
                _ => bail!("id {id:?}: expected header:NAME, json:/POINTER or body-sha256"),
            },
        };
        let destination = match (entry.ant, entry.channel) {
            (Some(ant), None) => Destination::Ant(ant),
            (None, Some(channel)) => Destination::Channel(channel),
            _ => bail!("give exactly one of ant and channel"),
        };
        let mentions = match (entry.mention, entry.broadcast) {
            (Some(_), true) => bail!("mention and broadcast exclude each other"),
            (None, false) => Mentions::Nobody,
            _ if matches!(destination, Destination::Ant(_)) => {
                bail!("mention and broadcast go with a channel only")
            }
            (Some(mentions), false) => Mentions::Listed(mentions),
            (None, true) => Mentions::Members,
        };
        let text = template(entry.text.as_deref().unwrap_or(DEFAULT_TEXT))?;
        let creds = entry.creds.as_deref().map(resolve);
        Ok(Self { name: entry.name, creds, auth, id, destination, mentions, owner: entry.owner, text })
    }

    /// Fail unless the credentials name the webhook, are no human's and may send to its destination.
    fn check_creds(&self) -> Result<()> {
        let Some(path) = &self.creds else { return Ok(()) };
        let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
        let claims = crate::humans::creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
        let name = &self.name;
        let issue = |what: &str| {
            let to = match &self.destination {
                Destination::Ant(ant) => format!("--to {ant}"),
                Destination::Channel(channel) => format!("--channel {channel}"),
            };
            anyhow!("credentials {} {what}; issue them with `ants auth user {name} --webhook {to}`", path.display())
        };
        if claims["name"].as_str() != Some(name.as_str()) {
            return Err(issue(&format!("are not webhook {name}'s")));
        }
        if crate::humans::may_publish(&claims, &format!("$KV.{}.>", ants_proto::HUMANS_BUCKET)) {
            return Err(issue("are a human's"));
        }
        if !crate::humans::may_publish(&claims, &self.destination.subject(name)) {
            return Err(issue("do not allow this destination"));
        }
        Ok(())
    }
}

fn read_secret(path: &Path) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(path).with_context(|| format!("secret_file {}", path.display()))?.permissions();
        if mode.mode() & 0o077 != 0 {
            bail!("secret_file {} is readable by others: chmod 600 it", path.display());
        }
    }
    std::fs::read_to_string(path).with_context(|| format!("reading secret_file {}", path.display()))
}

/// The pieces of a `text` template: `{body}`, `{id}`, `{webhook}`, `{header:NAME}` and `{json:/POINTER}` between
/// literal text; `{{` and `}}` are literal braces.
fn template(text: &str) -> Result<Vec<Piece>> {
    let mut pieces = Vec::new();
    let mut literal = String::new();
    let mut rest = text;
    while let Some(i) = rest.find(['{', '}']) {
        literal.push_str(&rest[..i]);
        rest = &rest[i..];
        if let Some(after) = rest.strip_prefix("{{").or_else(|| rest.strip_prefix("}}")) {
            literal.push_str(&rest[..1]);
            rest = after;
            continue;
        }
        if rest.starts_with('}') {
            bail!("text: a lone }}; write }}}} for a literal one");
        }
        let end = rest.find('}').context("text: a { without its }")?;
        let piece = placeholder(&rest[1..end]).context("text")?;
        if !literal.is_empty() {
            pieces.push(Piece::Text(std::mem::take(&mut literal)));
        }
        pieces.push(piece);
        rest = &rest[end + 1..];
    }
    literal.push_str(rest);
    if !literal.is_empty() {
        pieces.push(Piece::Text(literal));
    }
    Ok(pieces)
}

fn placeholder(name: &str) -> Result<Piece> {
    Ok(match name {
        "body" => Piece::Body,
        "id" => Piece::Id,
        "webhook" => Piece::Webhook,
        _ => {
            if let Some(header) = name.strip_prefix("header:") {
                Piece::Header(HeaderName::from_bytes(header.as_bytes()).with_context(|| format!("{{{name}}}"))?)
            } else if let Some(pointer) = name.strip_prefix("json:") {
                if !pointer.is_empty() && !pointer.starts_with('/') {
                    bail!("{{{name}}}: a JSON pointer starts with /");
                }
                Piece::Json(pointer.to_owned())
            } else {
                bail!("unknown placeholder {{{name}}}: expected body, id, webhook, header:NAME or json:/POINTER")
            }
        }
    })
}

// ---- requests --------------------------------------------------------------------------------------------------

/// A request refused, with its status and why.
struct Refusal(StatusCode, String);

fn refuse<T>(status: StatusCode, why: impl Into<String>) -> Result<T, Refusal> {
    Err(Refusal(status, why.into()))
}

/// An authenticated event, ready to deliver.
struct Event {
    /// Hash of its id, in its key and `Nats-Msg-Id`.
    hash: String,
    text: String,
}

impl Webhook {
    fn authorized_before_body(&self, headers: &HeaderMap) -> bool {
        match &self.auth {
            Auth::Hmac { .. } => true,
            Auth::Bearer(token) => headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .is_some_and(|given| crate::bridge::same(given.trim().as_bytes(), token.as_bytes())),
        }
    }

    fn authorized(&self, headers: &HeaderMap, body: &[u8]) -> bool {
        match &self.auth {
            Auth::Hmac { key, header } => {
                let signature = headers
                    .get(header)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().strip_prefix("sha256="))
                    .and_then(|hex| HEXLOWER_PERMISSIVE.decode(hex.as_bytes()).ok());
                signature.is_some_and(|signature| hmac::verify(key, body, &signature).is_ok())
            }
            Auth::Bearer(_) => true,
        }
    }

    /// The event in an authenticated request: its id and its text from the template.
    fn event(&self, headers: &HeaderMap, body: &[u8]) -> Result<Event, Refusal> {
        let wants_json = matches!(self.id, IdSource::Json(_)) || self.text.iter().any(|p| matches!(p, Piece::Json(_)));
        let parsed = match wants_json {
            true => match serde_json::from_slice::<Value>(body) {
                Ok(value) => Some(value),
                Err(e) => return refuse(StatusCode::BAD_REQUEST, format!("the body is no JSON: {e}")),
            },
            false => None,
        };
        let id = match &self.id {
            IdSource::BodySha256 => HEXLOWER.encode(digest::digest(&digest::SHA256, body).as_ref()),
            IdSource::Header(name) => header_text(headers, name),
            IdSource::Json(pointer) => {
                parsed.as_ref().and_then(|v| v.pointer(pointer)).map(json_text).unwrap_or_default()
            }
        };
        if id.is_empty() {
            return refuse(StatusCode::BAD_REQUEST, "the event has no id");
        }
        let mut text = String::new();
        for piece in &self.text {
            match piece {
                Piece::Text(s) => text.push_str(s),
                Piece::Body => match std::str::from_utf8(body) {
                    Ok(body) => text.push_str(body),
                    Err(_) => return refuse(StatusCode::UNSUPPORTED_MEDIA_TYPE, "the body is no UTF-8 text"),
                },
                Piece::Id => text.push_str(&id),
                Piece::Webhook => text.push_str(self.name.as_str()),
                Piece::Header(name) => text.push_str(&header_text(headers, name)),
                Piece::Json(pointer) => {
                    text.push_str(&parsed.as_ref().and_then(|v| v.pointer(pointer)).map(json_text).unwrap_or_default())
                }
            }
        }
        let hash = HEXLOWER.encode(digest::digest(&digest::SHA256, id.as_bytes()).as_ref())[..ID_HASH_LEN].to_owned();
        Ok(Event { hash, text })
    }
}

fn header_text(headers: &HeaderMap, name: &HeaderName) -> String {
    headers.get(name).map(|v| String::from_utf8_lossy(v.as_bytes()).trim().to_owned()).unwrap_or_default()
}

/// A JSON value as text: a string as itself, null as nothing, anything else as JSON.
fn json_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ---- server ----------------------------------------------------------------------------------------------------

/// What [`WEBHOOKS_BUCKET`] keeps of a delivered event.
#[derive(Serialize, Deserialize)]
struct Seen {
    thread: ThreadId,
    seq: u64,
}

/// A webhook's connection to the hub.
struct Bus {
    client: async_nats::Client,
    js: jetstream::Context,
    seen: kv::Store,
}

struct Hook {
    config: Webhook,
    bus: OnceCell<Bus>,
    last_warned: Mutex<Option<Instant>>,
}

struct Server {
    /// `<path>/`, before each webhook's name.
    prefix: String,
    hooks: HashMap<String, Arc<Hook>>,
    in_flight: Arc<Semaphore>,
}

/// Serve the webhooks of `config` on `listener` until a webhook's credentials fail. Each webhook connects to `nats`
/// as itself; until it is connected, its requests answer 503.
pub async fn serve(config: Config, nats: &str, js_domain: Option<&JsDomain>, listener: TcpListener) -> Result<()> {
    let hooks = config.webhooks.into_iter().map(|config| {
        let hook = Hook { config, bus: OnceCell::new(), last_warned: Mutex::default() };
        (hook.config.name.to_string(), Arc::new(hook))
    });
    let server = Arc::new(Server {
        prefix: format!("{}/", config.path),
        hooks: hooks.collect(),
        in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
    });
    let mut tasks = JoinSet::new();
    let (failed_tx, mut failed) = mpsc::unbounded_channel();
    for hook in server.hooks.values() {
        let (hook, nats, domain, failed_tx) = (hook.clone(), nats.to_owned(), js_domain.cloned(), failed_tx.clone());
        tasks.spawn(async move {
            if let Err(e) = connect(&hook, &nats, domain.as_ref()).await {
                let _ = failed_tx.send(e.context(format!("webhook {}", hook.config.name)));
            }
        });
    }
    loop {
        while tasks.try_join_next().is_some() {}
        let stream = tokio::select! {
            Some(e) = failed.recv() => return Err(e),
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a webhook connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let server = server.clone();
        tasks.spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let server = server.clone();
                async move { Ok::<_, Infallible>(server.handle(request).await) }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_TIMEOUT)
                .serve_connection(TokioIo::new(stream), service);
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "webhook connection");
            }
        });
    }
}

/// Connect `hook` as itself, and open the bucket of delivered events, waiting for both as long as it takes.
async fn connect(hook: &Hook, nats: &str, js_domain: Option<&JsDomain>) -> Result<()> {
    let name = &hook.config.name;
    let creds = hook.config.creds.as_deref();
    let pending = std::future::pending();
    let (client, mut js) =
        bus::connect_retrying(nats, creds, js_domain, name, "ants-webhook", pending).await?.context("stopped")?;
    js.set_timeout(ACK_TIMEOUT);
    let mut delay = Duration::from_secs(1);
    let seen = loop {
        match js.get_key_value(WEBHOOKS_BUCKET).await {
            Ok(seen) => break seen,
            Err(e) => {
                tracing::warn!(webhook = %name, error = %e, retry_in = ?delay, "no bucket {WEBHOOKS_BUCKET} yet: `ants init` creates it");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    };
    tracing::info!(webhook = %name, to = ?hook.config.destination, "webhook ready");
    let _ = hook.bus.set(Bus { client, js, seen });
    Ok(())
}

impl Server {
    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Ok(_permit) = self.in_flight.clone().try_acquire_owned() else {
            return unavailable("too many requests at once");
        };
        let outcome = tokio::time::timeout(REQUEST_TIMEOUT, self.accept(request)).await;
        match outcome {
            Ok(Ok(response)) => response,
            Ok(Err(Refusal(StatusCode::SERVICE_UNAVAILABLE, why))) => unavailable(&why),
            Ok(Err(Refusal(status, why))) => {
                let mut response = reply(status, &why);
                if status == StatusCode::METHOD_NOT_ALLOWED {
                    response.headers_mut().insert(header::ALLOW, HeaderValue::from_static("POST"));
                }
                response
            }
            Err(_) => unavailable("the request took too long"),
        }
    }

    async fn accept(&self, request: Request<Incoming>) -> Result<Response<Full<Bytes>>, Refusal> {
        let (parts, body) = request.into_parts();
        let hook = parts.uri.path().strip_prefix(&self.prefix).and_then(|name| self.hooks.get(name));
        let refused = match hook {
            None => Some(Refusal(StatusCode::NOT_FOUND, "no such webhook".to_owned())),
            Some(_) if parts.method != Method::POST => {
                Some(Refusal(StatusCode::METHOD_NOT_ALLOWED, "webhooks take POST only".to_owned()))
            }
            Some(hook) if !hook.config.authorized_before_body(&parts.headers) => {
                hook.warn_rejected();
                Some(Refusal(StatusCode::UNAUTHORIZED, "missing or wrong token".to_owned()))
            }
            Some(_) => None,
        };
        let body = read_body(body, refused.is_none()).await;
        if let Some(refused) = refused {
            return Err(refused);
        }
        let hook = hook.expect("refused without a webhook");
        let body = match body {
            Ok(Some(body)) => body,
            Ok(None) => {
                return refuse(StatusCode::PAYLOAD_TOO_LARGE, format!("bodies are limited to {MAX_BODY} bytes"));
            }
            Err(e) => return refuse(StatusCode::BAD_REQUEST, format!("reading the body: {e}")),
        };
        if !hook.config.authorized(&parts.headers, &body) {
            hook.warn_rejected();
            return refuse(StatusCode::UNAUTHORIZED, "missing or wrong signature");
        }
        let event = hook.config.event(&parts.headers, &body)?;
        let Some(bus) = hook.bus.get() else { return refuse(StatusCode::SERVICE_UNAVAILABLE, "not connected yet") };
        if bus.client.connection_state() != async_nats::connection::State::Connected {
            return refuse(StatusCode::SERVICE_UNAVAILABLE, "the hub is not reachable");
        }
        hook.deliver(bus, event).await.map_err(|e| {
            tracing::warn!(webhook = %hook.config.name, error = format!("{e:#}"), "delivering an event");
            Refusal(StatusCode::SERVICE_UNAVAILABLE, "the hub did not store the event".to_owned())
        })
    }
}

/// The body when `keep`, or None when it is longer than [`MAX_BODY`]. The rest of the body is read and dropped up
/// to [`DRAIN`], so the answer reaches the sender rather than a reset connection.
async fn read_body(mut body: Incoming, keep: bool) -> Result<Option<Vec<u8>>, hyper::Error> {
    let (mut kept, mut length) = (Vec::new(), 0);
    while let Some(frame) = body.frame().await {
        let Ok(data) = frame?.into_data() else { continue };
        length += data.len();
        if length > DRAIN {
            break;
        }
        if keep && length <= MAX_BODY {
            kept.extend_from_slice(&data);
        }
    }
    Ok((length <= MAX_BODY).then_some(kept))
}

impl Hook {
    /// Deliver `event` unless it was delivered already, and answer once the hub's stream stored it.
    async fn deliver(&self, bus: &Bus, event: Event) -> Result<Response<Full<Bytes>>> {
        let hook = &self.config;
        let key = webhook_seen_key(&hook.name, &event.hash);
        if let Some(seen) = self.seen(bus, &key).await? {
            return Ok(accepted(StatusCode::OK, Some(&seen.thread), true));
        }
        let (subject, payload, thread) = match &hook.destination {
            Destination::Ant(ant) => {
                let message = Envelope::new(hook.name.clone(), ant.clone(), event.text).with_owner(hook.owner.clone());
                (message.subject(), serde_json::to_vec(&message)?, message.thread_id)
            }
            Destination::Channel(channel) => {
                let mentions = match &hook.mentions {
                    Mentions::Nobody => Vec::new(),
                    Mentions::Listed(mentions) => mentions.clone(),
                    Mentions::Members => members(&bus.js, channel).await?,
                };
                let post = ChannelPost::new(channel.clone(), hook.name.clone(), event.text)
                    .with_owner(hook.owner.clone())
                    .from_webhook(mentions);
                (post.subject(), serde_json::to_vec(&post)?, post.thread_id)
            }
        };
        let message = jetstream::message::PublishMessage::build()
            .payload(payload.into())
            .message_id(format!("webhook.{}.{}", hook.name, event.hash));
        let ack = bus.js.send_publish(subject, message).await?.await.context("waiting for the hub to store it")?;
        if ack.duplicate {
            let thread = self.seen(bus, &key).await.ok().flatten().map(|seen| seen.thread);
            return Ok(accepted(StatusCode::OK, thread.as_ref(), true));
        }
        let seen = serde_json::to_vec(&Seen { thread: thread.clone(), seq: ack.sequence })?;
        for attempt in 1..=3 {
            match bus.seen.create(&key, seen.clone().into()).await {
                Ok(_) => break,
                Err(e) if e.kind() == kv::CreateErrorKind::AlreadyExists => break,
                Err(e) => {
                    tracing::warn!(webhook = %hook.name, error = %e, attempt, "recording a delivered event");
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                }
            }
        }
        tracing::info!(webhook = %hook.name, thread = %thread, "event delivered");
        Ok(accepted(StatusCode::ACCEPTED, Some(&thread), false))
    }

    async fn seen(&self, bus: &Bus, key: &str) -> Result<Option<Seen>> {
        let found = bus.seen.get(key).await.context("reading the webhooks bucket")?;
        Ok(found.and_then(|value| serde_json::from_slice(&value).ok()))
    }

    fn warn_rejected(&self) {
        let mut last = self.last_warned.lock().unwrap();
        if last.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
            *last = Some(Instant::now());
            tracing::warn!(webhook = %self.config.name, "rejected a request that failed authentication");
        }
    }
}

/// The members of `channel` now; none when it does not exist.
async fn members(js: &jetstream::Context, channel: &ChannelId) -> Result<Vec<UserId>> {
    let bucket = js.get_stream_no_info(format!("KV_{CHANNELS_BUCKET}")).await?;
    match bucket.direct_get_last_for_subject(subject::channel_entry(channel)).await {
        Ok(entry) if entry.payload.is_empty() => Ok(Vec::new()),
        Ok(entry) => {
            let entry: ChannelEntry = serde_json::from_slice(&entry.payload).context("reading the channel")?;
            Ok(entry.members)
        }
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).context("reading the channel"),
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
mod tests {
    use std::net::SocketAddr;

    use ants_proto::{ChannelEntry, Permissions, STREAM};
    use futures::TryStreamExt as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::auth::{issue_with, tests::Server};

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn parse(text: &str) -> Result<Config> {
        let env = |name: &str| (name == "HOOK_SECRET").then(|| "s3cret".to_owned());
        Config::parse(text, Path::new("/etc/ants/webhooks.toml"), Path::new("/home/q"), env)
    }

    fn error(text: &str) -> String {
        format!("{:#}", parse(text).err().expect("an error"))
    }

    const HOOK: &str = "[[webhook]]\nname = \"github\"\nauth = \"bearer\"\nsecret_env = \"HOOK_SECRET\"\n";

    #[test]
    fn the_config_names_one_destination_and_its_mentions_per_webhook() {
        let config = parse(&format!(
            "path = \"/in/\"\n{HOOK}ant = \"dev-a\"\ncreds = \"~/github.creds\"\nid = \"header:X-GitHub-Delivery\"\n\
             [[webhook]]\nname = \"ci\"\nauth = \"hmac-sha256\"\nsecret_env = \"HOOK_SECRET\"\nchannel = \"ops\"\n\
             broadcast = true\ncreds = \"ci.creds\"\n\
             [[webhook]]\nname = \"alerts\"\nauth = \"bearer\"\nsecret_env = \"HOOK_SECRET\"\nchannel = \"ops\"\n\
             mention = [\"dev-b\"]\nid = \"json:/alert/id\"\n"
        ))
        .unwrap();
        assert_eq!(config.path, "/in");
        let [github, ci, alerts] = &config.webhooks[..] else { panic!("three webhooks") };
        assert_eq!(github.destination, Destination::Ant(p("dev-a")));
        assert_eq!(github.creds.as_deref(), Some(Path::new("/home/q/github.creds")));
        assert_eq!(ci.creds.as_deref(), Some(Path::new("/etc/ants/ci.creds")));
        assert!(matches!(github.id, IdSource::Header(ref h) if h == "x-github-delivery"));
        assert!(matches!(ci.id, IdSource::BodySha256));
        assert!(matches!(alerts.id, IdSource::Json(ref p) if p == "/alert/id"));
        assert_eq!((&github.mentions, &ci.mentions), (&Mentions::Nobody, &Mentions::Members));
        assert_eq!(alerts.mentions, Mentions::Listed(vec![p("dev-b")]));

        for (extra, why) in [
            ("", "exactly one of ant and channel"),
            ("ant = \"dev-a\"\nchannel = \"ops\"", "exactly one of ant and channel"),
            ("ant = \"dev-a\"\nmention = [\"dev-b\"]", "with a channel only"),
            ("ant = \"dev-a\"\nbroadcast = true", "with a channel only"),
            ("channel = \"ops\"\nmention = []\nbroadcast = true", "exclude each other"),
            ("ant = \"dev-a\"\ntext = \"{payload}\"", "unknown placeholder {payload}"),
            ("ant = \"dev-a\"\ntext = \"{json:a}\"", "starts with /"),
            ("ant = \"dev-a\"\ntext = \"{body\"", "without its }"),
            ("ant = \"dev-a\"\nid = \"body\"", "expected header:NAME"),
            ("ant = \"dev-a\"\nsignature_header = \"X-Sig\"", "hmac-sha256\" only"),
            ("ant = \"dev-a\"\nsecret_file = \"s\"", "exactly one of secret_env and secret_file"),
            ("ant = \"dev-a\"\nurl = \"x\"", "unknown field `url`"),
        ] {
            let e = error(&format!("{HOOK}{extra}\n"));
            assert!(e.contains("webhook github") || e.contains("unknown field"), "{e}");
            assert!(e.contains(why), "{extra}: {e}");
        }
        let unset = HOOK.replace("HOOK_SECRET", "NOT_SET");
        assert!(error(&format!("{unset}ant = \"dev-a\"\n")).contains("NOT_SET is not set"));
        let human = HOOK.replace("github", "human");
        assert!(error(&format!("{human}ant = \"dev-a\"\n")).contains("the default human is no webhook"));
        assert!(error(&format!("{HOOK}ant = \"dev-a\"\n{HOOK}ant = \"dev-b\"\n")).contains("listed twice"));
        assert!(error("nats = \"nats://hub:4222\"\n").contains("nothing to serve"));
    }

    #[test]
    fn a_secret_file_must_be_private() {
        let dir = ants_test_support::scratch_dir("webhook-secret");
        let secret = dir.join("github.secret");
        std::fs::write(&secret, "s3cret\n").unwrap();
        let text =
            format!("[[webhook]]\nname = \"github\"\nauth = \"bearer\"\nsecret_file = {secret:?}\nant = \"dev-a\"\n");
        let path = dir.join("webhooks.toml");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o644)).unwrap();
            let e = Config::parse(&text, &path, &dir, |_| None).err().unwrap();
            assert!(format!("{e:#}").contains("readable by others"), "{e:#}");
            std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let config = Config::parse(&text, &path, &dir, |_| None).unwrap();
        assert!(matches!(&config.webhooks[0].auth, Auth::Bearer(token) if token == "s3cret"), "trimmed");
    }

    #[test]
    fn the_text_comes_from_the_template_and_the_event_id_from_its_source() {
        let config = parse(&format!(
            "{HOOK}ant = \"dev-a\"\nid = \"json:/id\"\n\
             text = \"{{{{{{webhook}}}}}} {{header:X-Event}} {{json:/repo/name}} {{json:/n}} {{json:/none}}|{{id}}\\n{{body}}\"\n"
        ))
        .unwrap();
        let hook = &config.webhooks[0];
        let mut headers = HeaderMap::new();
        headers.insert("x-event", HeaderValue::from_static("push"));
        let body = br#"{"id":42,"repo":{"name":"ants"},"n":[1]}"#;
        let event = hook.event(&headers, body).ok().unwrap();
        assert_eq!(event.text, format!("{{github}} push ants [1] |42\n{}", std::str::from_utf8(body).unwrap()));
        assert_eq!(event.hash.len(), ID_HASH_LEN);
        assert_eq!(hook.event(&headers, br#"{"id":42}"#).ok().unwrap().hash, event.hash, "the same id");

        let status = |body: &[u8]| hook.event(&headers, body).err().unwrap().0;
        assert_eq!(status(br#"{"repo":{}}"#), StatusCode::BAD_REQUEST, "no id");
        assert_eq!(status(b"not json"), StatusCode::BAD_REQUEST);
        let plain = parse(&format!("{HOOK}ant = \"dev-a\"\n")).unwrap();
        let event = plain.webhooks[0].event(&headers, b"ping").ok().unwrap();
        let id = HEXLOWER.encode(digest::digest(&digest::SHA256, b"ping").as_ref());
        assert_eq!(event.text, format!("[webhook github] {id}\n\nping"));
        assert_eq!(plain.webhooks[0].event(&headers, b"\xff").err().unwrap().0, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    // ---- over HTTP -------------------------------------------------------------------------------------------

    struct Answer {
        status: u16,
        headers: String,
        body: String,
    }

    impl Answer {
        fn json(&self) -> Value {
            serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.body))
        }
    }

    async fn request(addr: SocketAddr, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Answer {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut head = format!("{method} {path} HTTP/1.1\r\nHost: hooks\r\nConnection: close\r\n");
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        let (headers, body) = answer.split_once("\r\n\r\n").unwrap_or_else(|| panic!("{method} {path}: {answer:?}"));
        let status = headers.split(' ').nth(1).unwrap().parse().unwrap();
        Answer { status, headers: headers.to_lowercase(), body: body.to_owned() }
    }

    fn signed(body: &[u8]) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"s3cret");
        format!("sha256={}", HEXLOWER.encode(hmac::sign(&key, body).as_ref()))
    }

    /// `ants webhook serve` with the config at `path`, and where it listens.
    async fn start(path: &Path, nats: &str) -> (JoinHandle<Result<()>>, SocketAddr) {
        let config = Config::load(path).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let nats = nats.to_owned();
        (tokio::spawn(async move { serve(config, &nats, None, listener).await }), addr)
    }

    /// Send until the webhook is connected: 503 until then.
    async fn deliver(addr: SocketAddr, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Answer {
        for _ in 0..100 {
            let answer = request(addr, "POST", path, headers, body).await;
            if answer.status != 503 {
                return answer;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{path} never connected");
    }

    /// The hub with its stream and buckets, as `ants init` leaves it, with #ops of dev-a and dev-b, and a private
    /// secret file.
    async fn hub() -> Option<(Server, jetstream::Context, PathBuf)> {
        let server = Server::start()?;
        let (_, js) = server.connect(&UserId::human()).await;
        bus::init_stream(&js).await.unwrap();
        ensure_bucket(&js).await.unwrap();
        let channels = bus::ensure_channels(&js).await.unwrap();
        let mut ops = ChannelEntry::new("ops".parse().unwrap());
        ops.add_member(p("dev-a"));
        ops.add_member(p("dev-b"));
        channels.put(ops.key(), serde_json::to_vec(&ops).unwrap().into()).await.unwrap();
        let secret = server.dir().join("hooks.secret");
        std::fs::write(&secret, "s3cret").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        Some((server, js, secret))
    }

    fn creds(server: &Server, webhook: &UserId, to: Destination) -> PathBuf {
        let out = server.dir().join(format!("{webhook}.creds"));
        issue_with(&server.dir().join("auth"), webhook, Permissions::for_webhook(webhook, &[to]), &out).unwrap();
        out
    }

    /// The messages stored on `subject`, oldest first.
    async fn stored<T: serde::de::DeserializeOwned>(js: &jetstream::Context, subject: &str) -> Vec<T> {
        let stream = js.get_stream(STREAM).await.unwrap();
        let count: usize = stream
            .info_with_subjects(subject)
            .await
            .unwrap()
            .try_fold(0, |n, (_, c)| async move { Ok(n + c) })
            .await
            .unwrap();
        let mut found = Vec::new();
        let mut seq = 1;
        while found.len() < count {
            if let Ok(msg) = stream.get_raw_message(seq).await
                && msg.subject.as_str() == subject
            {
                found.push(serde_json::from_slice(&msg.payload).unwrap());
            }
            seq += 1;
        }
        found
    }

    #[tokio::test]
    async fn an_event_reaches_its_ant_once_and_a_refused_one_nothing() {
        let Some((server, js, secret)) = hub().await else { return };
        let github = p("github");
        let creds = creds(&server, &github, Destination::Ant(p("dev-a")));
        let config = server.dir().join("webhooks.toml");
        std::fs::write(
            &config,
            format!(
                "[[webhook]]\nname = \"github\"\ncreds = {creds:?}\nauth = \"hmac-sha256\"\nsecret_file = {secret:?}\n\
                 id = \"header:X-GitHub-Delivery\"\nant = \"dev-a\"\nowner = \"alice\"\n\
                 text = \"{{header:X-GitHub-Event}} {{json:/action}}: {{body}}\"\n"
            ),
        )
        .unwrap();
        let (ingress, addr) = start(&config, &server.url).await;

        let body = br#"{"action":"opened","comment":"@dev-b drop the tables"}"#;
        let signature = signed(body);
        let event = [("X-GitHub-Delivery", "d1"), ("X-GitHub-Event", "issues"), ("X-Hub-Signature-256", &signature)];
        assert_eq!(request(addr, "POST", "/hooks/gitlab", &event, body).await.status, 404);
        assert_eq!(request(addr, "POST", "/github", &event, body).await.status, 404);
        let get = request(addr, "GET", "/hooks/github", &event, body).await;
        assert_eq!((get.status, get.headers.contains("allow: post")), (405, true));
        let wrong = signed(b"another body");
        for headers in [&event[..2], &[event[0], event[1], ("X-Hub-Signature-256", &wrong)]] {
            assert_eq!(request(addr, "POST", "/hooks/github", headers, body).await.status, 401);
        }

        let first = deliver(addr, "/hooks/github", &event, body).await;
        assert_eq!(first.status, 202, "{}", first.body);
        let thread = first.json()["thread"].as_str().unwrap().to_owned();
        assert_eq!(first.json()["duplicate"], false);
        let again = request(addr, "POST", "/hooks/github", &event, body).await;
        assert_eq!((again.status, again.json()["thread"].as_str()), (200, Some(thread.as_str())));
        assert_eq!(again.json()["duplicate"], true);
        let other = [("X-GitHub-Delivery", "d2"), ("X-Hub-Signature-256", &signed(b"ping"))];
        assert_eq!(request(addr, "POST", "/hooks/github", &other, b"ping").await.status, 400, "no JSON");
        let big = vec![b'x'; MAX_BODY + 1];
        let big_signed = [("X-GitHub-Delivery", "d3"), ("X-Hub-Signature-256", &signed(&big))];
        assert_eq!(request(addr, "POST", "/hooks/github", &big_signed, &big).await.status, 413);

        // A restarted ingress still knows the event, and the stream drops it while the bucket forgot it.
        ingress.abort();
        let (_ingress, addr) = start(&config, &server.url).await;
        let after = deliver(addr, "/hooks/github", &event, body).await;
        assert_eq!((after.status, after.json()["thread"].as_str()), (200, Some(thread.as_str())));
        let (_, own) = bus::connect(&server.url, Some(&creds), None, &github, "test").await.unwrap();
        let seen = own.get_key_value(WEBHOOKS_BUCKET).await.unwrap();
        let hash = &HEXLOWER.encode(digest::digest(&digest::SHA256, b"d1").as_ref())[..ID_HASH_LEN];
        seen.delete(webhook_seen_key(&github, hash)).await.unwrap();
        let forgotten = request(addr, "POST", "/hooks/github", &event, body).await;
        assert_eq!((forgotten.status, forgotten.json()["duplicate"].as_bool()), (200, Some(true)));

        let messages: Vec<Envelope> = stored(&js, &subject::inbox(&p("dev-a"), &github)).await;
        let [message] = &messages[..] else { panic!("one message: {messages:?}") };
        assert_eq!((&message.from, &message.to, message.thread_id.as_str()), (&github, &p("dev-a"), thread.as_str()));
        assert_eq!(message.owner, Some(p("alice")));
        assert_eq!(message.text, format!("issues opened: {}", std::str::from_utf8(body).unwrap()));
    }

    #[tokio::test]
    async fn a_channel_webhook_mentions_nobody_the_listed_or_the_members() {
        let Some((server, js, secret)) = hub().await else { return };
        let ops: ChannelId = "ops".parse().unwrap();
        let mut config = String::new();
        for (name, mentions) in
            [("news", ""), ("alerts", "mention = [\"dev-b\", \"carol\"]"), ("ci", "broadcast = true")]
        {
            let creds = creds(&server, &p(name), Destination::Channel(ops.clone()));
            config.push_str(&format!(
                "[[webhook]]\nname = \"{name}\"\ncreds = {creds:?}\nauth = \"bearer\"\nsecret_file = {secret:?}\n\
                 channel = \"ops\"\ntext = \"{{body}}\"\n{mentions}\n"
            ));
        }
        let path = server.dir().join("webhooks.toml");
        std::fs::write(&path, config).unwrap();
        let (_ingress, addr) = start(&path, &server.url).await;

        let token = [("Authorization", "Bearer s3cret")];
        assert_eq!(request(addr, "POST", "/hooks/news", &[("Authorization", "Bearer nope")], b"x").await.status, 401);
        assert_eq!(request(addr, "POST", "/hooks/news", &[], b"x").await.status, 401);
        for name in ["news", "alerts", "ci"] {
            let answer = deliver(addr, &format!("/hooks/{name}"), &token, b"@dev-a the build is red").await;
            assert_eq!(answer.status, 202, "{name}: {}", answer.body);
        }
        let mentions = |name: &str| {
            let (js, subject) = (js.clone(), subject::chan(&ops, &p(name)));
            async move {
                let posts: Vec<ChannelPost> = stored(&js, &subject).await;
                let [post] = &posts[..] else { panic!("one post: {posts:?}") };
                assert!(post.webhook && post.text == "@dev-a the build is red" && post.owner.is_none());
                post.mentions.clone()
            }
        };
        assert!(mentions("news").await.is_empty(), "never parsed from the event");
        assert_eq!(mentions("alerts").await, [p("dev-b"), p("carol")]);
        assert_eq!(mentions("ci").await, [p("dev-a"), p("dev-b")]);
    }

    #[tokio::test]
    async fn without_the_hub_an_event_is_refused_for_a_retry() {
        let dir = ants_test_support::scratch_dir("webhook-down");
        let path = dir.join("webhooks.toml");
        std::fs::write(
            &path,
            format!("{HOOK}ant = \"dev-a\"\n").replace("secret_env = \"HOOK_SECRET\"", "secret_file = \"s\""),
        )
        .unwrap();
        std::fs::write(dir.join("s"), "s3cret").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(dir.join("s"), std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let port = ants_test_support::free_port();
        let (_ingress, addr) = start(&path, &format!("nats://127.0.0.1:{port}")).await;
        assert_eq!(request(addr, "POST", "/hooks/github", &[], b"x").await.status, 401, "checked first");
        let down = request(addr, "POST", "/hooks/github", &[("Authorization", "Bearer s3cret")], b"x").await;
        assert_eq!(down.status, 503);
        assert!(down.headers.contains(&format!("retry-after: {RETRY_AFTER}")), "{}", down.headers);
    }
}
