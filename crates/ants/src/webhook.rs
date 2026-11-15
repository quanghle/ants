//! `ants webhook serve`: take events from outside services over HTTP and deliver each to the ant or channel its
//! webhook names.
//!
//! Each `[[webhook]]` of the config file ([`Config`]) is its own NATS user, with credentials from
//! `ants auth user NAME --webhook --to ANT` or `--channel CHANNEL` ([`ants_proto::Permissions::for_webhook`]), or
//! with its entry and secret from `ants webhook add` ([`crate::webhook_add`]): it sends as itself, to its destination
//! only. `POST <path>/<name>` with the event:
//! - is authenticated before anything else: an HMAC-SHA256 signature of the body, a bearer token, or a token in a
//!   named header (401);
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

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use ants_daemon::direct_kv::DirectKv;
use ants_daemon::metrics::{self, Exposition, Kind};
use ants_daemon::schedule::Mentions;
use ants_daemon::{bus, owners};
use ants_proto::{
    CHANNELS_BUCKET, ChannelEntry, ChannelId, ChannelPost, Destination, Envelope, JsDomain, SERVING_EVERY, ThreadId,
    UserId, WEBHOOKS_BUCKET, subject, webhook_marker_key, webhook_seen_key,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use async_nats::jetstream::stream::DirectGetErrorKind;
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
use tokio::task::{AbortHandle, JoinSet};

use crate::notify;

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
/// human's), as `ants init` does.
#[cfg(test)]
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<jetstream::kv::Store> {
    if let Ok(store) = js.get_key_value(WEBHOOKS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(ants_daemon::hub::webhooks_config())
        .await
        .context("creating the webhooks bucket (with credentials, `ants init` as a human creates it)")
}

// ---- config ----------------------------------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    listen: Option<String>,
    metrics: Option<SocketAddr>,
    path: Option<String>,
    nats: Option<String>,
    js_domain: Option<JsDomain>,
    public_url: Option<String>,
    action_secret_env: Option<String>,
    action_secret_file: Option<PathBuf>,
    /// The time zone of the humans' quiet hours, unless one names its own.
    timezone: Option<String>,
    #[serde(default, rename = "webhook")]
    webhooks: Vec<Entry>,
    #[serde(default, rename = "human")]
    humans: Vec<notify::HumanEntry>,
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
    token_header: Option<String>,
    id: Option<String>,
    ant: Option<UserId>,
    channel: Option<ChannelId>,
    mention: Option<Vec<UserId>>,
    #[serde(default)]
    broadcast: bool,
    owner: Option<UserId>,
    text: Option<String>,
    allow: Option<Vec<String>>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
enum AuthKind {
    HmacSha256,
    Bearer,
    Token,
}

/// The config file of `ants webhook serve`, checked.
pub struct Config {
    pub listen: Option<String>,
    /// Where to serve health and metrics, if anywhere.
    pub metrics: Option<SocketAddr>,
    /// Requests go to `<path>/<webhook>`.
    pub path: String,
    pub nats: Option<String>,
    pub js_domain: Option<JsDomain>,
    pub webhooks: Vec<Webhook>,
    /// The humans to notify.
    pub humans: Vec<notify::Human>,
    /// Signs and checks approval links, with `public_url`.
    pub links: Option<notify::Links>,
}

/// One `[[webhook]]`.
#[derive(PartialEq)]
pub struct Webhook {
    pub name: UserId,
    pub creds: Option<PathBuf>,
    auth: Auth,
    id: IdSource,
    pub destination: Destination,
    mentions: Mentions,
    owner: Option<UserId>,
    text: Vec<Piece>,
    /// The tool rules the threads it starts are narrowed to; None for [`ants_proto::WEBHOOK_ALLOW`].
    allow: Option<Vec<String>>,
}

#[derive(PartialEq)]
enum Auth {
    /// The hex of the body's HMAC-SHA256, keyed with the secret, in the header, after `sha256=` or alone.
    Hmac { secret: String, header: HeaderName },
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// The token alone in the header, such as GitLab's `X-Gitlab-Token`.
    Token { secret: String, header: HeaderName },
}

#[derive(Debug, PartialEq)]
enum IdSource {
    Header(HeaderName),
    Json(String),
    BodySha256,
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
        let config = Self::read(path)?;
        for hook in &config.webhooks {
            hook.check_creds().with_context(|| format!("webhook config {}", path.display()))?;
        }
        for human in &config.humans {
            human.check_creds().with_context(|| format!("webhook config {}: human {}", path.display(), human.name))?;
        }
        Ok(config)
    }

    /// Read and check the config file at `path`, reading each secret, without looking at credentials.
    pub fn read(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading webhook config {}", path.display()))?;
        let home = std::env::home_dir().context("no home directory")?;
        Self::parse(&text, &std::path::absolute(path)?, &home, |name| std::env::var(name).ok())
    }

    /// Parse the text of the config file at `path`: expand `~` to `home`, resolve relative paths against the file's
    /// directory and read each secret, from `env` or its file.
    pub(crate) fn parse(text: &str, path: &Path, home: &Path, env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let shown = path.display();
        let file: File = toml::from_str(text).with_context(|| format!("parsing webhook config {shown}"))?;
        let resolve = resolver(path, home);
        let path = file.path.as_deref().unwrap_or(DEFAULT_PATH).trim_end_matches('/').to_owned();
        if !path.is_empty() && !path.starts_with('/') {
            bail!("webhook config {shown}: path {path:?} must start with /");
        }
        if file.webhooks.is_empty() && file.humans.is_empty() {
            bail!("webhook config {shown}: no [[webhook]] or [[human]] entries: nothing to serve");
        }
        if let Some(url) = &file.nats {
            bus::check_url(url).with_context(|| format!("webhook config {shown}"))?;
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
        let timezone = file
            .timezone
            .as_deref()
            .map(ants_daemon::schedule::time_zone)
            .transpose()
            .with_context(|| format!("webhook config {shown}"))?;
        let mut humans: Vec<notify::Human> = Vec::with_capacity(file.humans.len());
        for entry in file.humans {
            let human = notify::Human::from_entry(entry, timezone.as_ref(), &resolve, &env)
                .with_context(|| format!("webhook config {shown}: human"))?;
            let name = &human.name;
            if humans.iter().any(|h| &h.name == name) {
                bail!("webhook config {shown}: human {name} is listed twice");
            }
            if webhooks.iter().any(|w| &w.name == name) {
                bail!("webhook config {shown}: {name} is both a webhook and a human");
            }
            humans.push(human);
        }
        let secret = match (&file.action_secret_env, &file.action_secret_file) {
            (Some(var), None) => Some(
                env(var).with_context(|| format!("webhook config {shown}: environment variable {var} is not set"))?,
            ),
            (None, Some(file)) => Some(read_secret(&resolve(file)).with_context(|| format!("webhook config {shown}"))?),
            (None, None) => None,
            _ => bail!("webhook config {shown}: give at most one of action_secret_env and action_secret_file"),
        };
        let links = match (file.public_url, secret) {
            (None, None) => None,
            (Some(_), _) | (_, Some(_)) if humans.is_empty() => {
                bail!("webhook config {shown}: public_url and the action secret go with [[human]] entries")
            }
            (Some(url), Some(secret)) => {
                Some(notify::Links::new(&url, secret.trim()).with_context(|| format!("webhook config {shown}"))?)
            }
            (Some(_), None) => {
                bail!("webhook config {shown}: public_url needs action_secret_file or action_secret_env")
            }
            (None, Some(_)) => bail!("webhook config {shown}: the action secret needs public_url"),
        };
        let (listen, metrics, nats, js_domain) = (file.listen, file.metrics, file.nats, file.js_domain);
        Ok(Self { listen, metrics, path, nats, js_domain, webhooks, humans, links })
    }
}

/// Paths of the config file at `path` resolved: `~` expanded to `home`, relative ones against the file's directory.
fn resolver<'a>(path: &'a Path, home: &'a Path) -> impl Fn(&Path) -> PathBuf + 'a {
    let base = path.parent().unwrap_or(Path::new("."));
    move |p: &Path| match p.strip_prefix("~") {
        Ok(rest) => home.join(rest),
        Err(_) => base.join(p),
    }
}

/// Where a secret of the config file comes from.
#[derive(Debug, PartialEq, Eq)]
pub enum SecretSource {
    File(PathBuf),
    Env(String),
}

/// What the config file at `path` names, without reading a secret.
pub struct Outline {
    /// What each secret is for and where it comes from.
    pub secrets: Vec<(String, SecretSource)>,
    /// What each exec sink is for and the program it runs, a path resolved or a name to find on `PATH`.
    pub commands: Vec<(String, String)>,
    pub public_url: Option<String>,
}

/// The config file at `path` parsed, its secrets left unread.
pub fn outline(path: &Path) -> Result<Outline> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading webhook config {}", path.display()))?;
    let file: File = toml::from_str(&text).with_context(|| format!("parsing webhook config {}", path.display()))?;
    let home = std::env::home_dir().context("no home directory")?;
    let path = std::path::absolute(path)?;
    let resolve = resolver(&path, &home);
    let named = file
        .webhooks
        .iter()
        .map(|w| (format!("webhook {}", w.name), w.secret_env.as_deref(), w.secret_file.as_deref()))
        .chain(file.humans.iter().flat_map(notify::HumanEntry::secrets))
        .chain([("action secret".to_owned(), file.action_secret_env.as_deref(), file.action_secret_file.as_deref())]);
    let mut secrets = Vec::new();
    for (what, env, file) in named {
        if let Some(var) = env {
            secrets.push((what.clone(), SecretSource::Env(var.to_owned())));
        }
        if let Some(file) = file {
            secrets.push((what, SecretSource::File(resolve(file))));
        }
    }
    let commands = file
        .humans
        .iter()
        .flat_map(notify::HumanEntry::commands)
        .map(|(what, program)| (what, notify::resolve_program(program, &resolve)))
        .collect();
    Ok(Outline { secrets, commands, public_url: file.public_url })
}

/// What `ants webhook add` reads of a config file's text: where it serves, and the webhooks it lists.
pub(crate) struct Listing {
    pub listen: Option<String>,
    /// Requests go to `<path>/<webhook>`.
    pub path: String,
    pub public_url: Option<String>,
    pub js_domain: Option<JsDomain>,
    pub webhooks: Vec<UserId>,
}

impl Listing {
    pub(crate) fn parse(text: &str) -> Result<Self> {
        let file: File = toml::from_str(text)?;
        Ok(Self {
            listen: file.listen,
            path: file.path.as_deref().unwrap_or(DEFAULT_PATH).trim_end_matches('/').to_owned(),
            public_url: file.public_url,
            js_domain: file.js_domain,
            webhooks: file.webhooks.into_iter().map(|w| w.name).collect(),
        })
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
        if entry.signature_header.is_some() && !matches!(entry.auth, AuthKind::HmacSha256) {
            bail!("signature_header goes with auth = \"hmac-sha256\" only");
        }
        if entry.token_header.is_some() && !matches!(entry.auth, AuthKind::Token) {
            bail!("token_header goes with auth = \"token\" only");
        }
        let auth = match entry.auth {
            AuthKind::HmacSha256 => {
                let header = entry.signature_header.as_deref().unwrap_or(DEFAULT_SIGNATURE_HEADER);
                let header = HeaderName::from_bytes(header.as_bytes())
                    .with_context(|| format!("signature_header {header:?}"))?;
                Auth::Hmac { secret, header }
            }
            AuthKind::Bearer => Auth::Bearer(secret),
            AuthKind::Token => {
                let header = entry.token_header.as_deref().context("auth = \"token\" needs token_header")?;
                let header =
                    HeaderName::from_bytes(header.as_bytes()).with_context(|| format!("token_header {header:?}"))?;
                Auth::Token { secret, header }
            }
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
        if let Some(allow) = &entry.allow {
            ants_daemon::Allowlist::parse(allow)?;
        }
        let (owner, allow) = (entry.owner, entry.allow);
        Ok(Self { name: entry.name, creds, auth, id, destination, mentions, owner, text, allow })
    }

    fn check_creds(&self) -> Result<()> {
        check_creds("webhook", &self.name, self.creds.as_deref(), &self.destination)
    }
}

/// Fail unless the credentials at `path`, if any, name `name`, the user a webhook or schedule (`kind`) sends as, are
/// no human's and may send to `destination`.
pub(crate) fn check_creds(kind: &str, name: &UserId, path: Option<&Path>, destination: &Destination) -> Result<()> {
    let Some(path) = path else { return Ok(()) };
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let claims = crate::humans::creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
    let issue = |what: &str| {
        let to = match destination {
            Destination::Ant(ant) => format!("--to {ant}"),
            Destination::Channel(channel) => format!("--channel {channel}"),
        };
        anyhow!("credentials {} {what}; issue them with `ants auth user {name} --webhook {to}`", path.display())
    };
    if claims["name"].as_str() != Some(name.as_str()) {
        return Err(issue(&format!("are not {kind} {name}'s")));
    }
    if crate::humans::may_publish(&claims, &format!("$KV.{}.>", ants_proto::HUMANS_BUCKET)) {
        return Err(issue("are a human's"));
    }
    if !crate::humans::may_publish(&claims, &destination.subject(name)) {
        return Err(issue("do not allow this destination"));
    }
    Ok(())
}

pub(crate) fn read_secret(path: &Path) -> Result<String> {
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

#[cfg(test)]
impl Webhook {
    /// Whether a request with `headers` and `body` gets through authentication.
    pub(crate) fn accepts(&self, headers: &HeaderMap, body: &[u8]) -> bool {
        self.authorized_before_body(headers) && self.authorized(headers, body)
    }

    /// The hash of the id and the text of the event in an authenticated request.
    pub(crate) fn event_of(&self, headers: &HeaderMap, body: &[u8]) -> (String, String) {
        let event = self.event(headers, body).ok().expect("an event");
        (event.hash, event.text)
    }
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
            Auth::Token { secret, header } => headers
                .get(header)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|given| crate::bridge::same(given.trim().as_bytes(), secret.as_bytes())),
        }
    }

    fn authorized(&self, headers: &HeaderMap, body: &[u8]) -> bool {
        match &self.auth {
            Auth::Hmac { secret, header } => {
                let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
                let signature = headers
                    .get(header)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .map(|v| v.strip_prefix("sha256=").unwrap_or(v))
                    .and_then(|hex| HEXLOWER_PERMISSIVE.decode(hex.as_bytes()).ok());
                signature.is_some_and(|signature| hmac::verify(&key, body, &signature).is_ok())
            }
            Auth::Bearer(_) | Auth::Token { .. } => true,
        }
    }

    /// The event in an authenticated request: its id and its text from the template.
    fn event(&self, headers: &HeaderMap, body: &[u8]) -> Result<Event, Refusal> {
        let wants_json = matches!(self.id, IdSource::Json(_)) || self.text.iter().any(|p| matches!(p, Piece::Json(_)));
        let parsed = if wants_json {
            match serde_json::from_slice::<Value>(body) {
                Ok(value) => Some(value),
                Err(e) => return refuse(StatusCode::BAD_REQUEST, format!("the body is no JSON: {e}")),
            }
        } else {
            None
        };
        let id = match &self.id {
            IdSource::BodySha256 => HEXLOWER.encode(digest::digest(&digest::SHA256, body).as_ref()),
            IdSource::Header(name) => header_text(headers, name),
            IdSource::Json(pointer) => parsed.as_ref().and_then(|v| v.pointer(pointer)).map_or_default(json_text),
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
                    text.push_str(&parsed.as_ref().and_then(|v| v.pointer(pointer)).map_or_default(json_text));
                }
            }
        }
        let hash = HEXLOWER.encode(digest::digest(&digest::SHA256, id.as_bytes()).as_ref())[..ID_HASH_LEN].to_owned();
        Ok(Event { hash, text })
    }
}

fn header_text(headers: &HeaderMap, name: &HeaderName) -> String {
    headers.get(name).map_or_default(|v| String::from_utf8_lossy(v.as_bytes()).trim().to_owned())
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

/// A webhook's, or a schedule's, connection to the hub, as itself.
pub(crate) struct Bus {
    pub(crate) client: async_nats::Client,
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    /// Without stream info, which would list the events of every webhook.
    seen: DirectKv,
    /// What its marker holds: the version of ants this runs ([`ants_proto::Serving`]).
    marker: Vec<u8>,
    /// When it last put its marker.
    marked: Arc<Mutex<Option<Instant>>>,
    /// Puts the marker every [`SERVING_EVERY`].
    keeper: tokio::task::JoinHandle<()>,
}

impl Drop for Bus {
    fn drop(&mut self) {
        self.keeper.abort();
    }
}

/// An event to deliver, once.
pub(crate) struct Delivery<'a> {
    pub(crate) from: &'a UserId,
    pub(crate) destination: &'a Destination,
    pub(crate) mentions: &'a Mentions,
    pub(crate) owner: Option<&'a UserId>,
    /// The tool rules the thread is narrowed to ([`Envelope::allow`]).
    pub(crate) allow: Option<&'a [String]>,
    /// Its key in [`WEBHOOKS_BUCKET`] ([`webhook_seen_key`]).
    pub(crate) key: String,
    /// The stream's `Nats-Msg-Id` of the message.
    pub(crate) message_id: String,
    pub(crate) text: String,
}

/// The thread an event went to, if known, and whether it went there before.
pub(crate) struct Delivered {
    pub(crate) thread: Option<ThreadId>,
    pub(crate) duplicate: bool,
}

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
        self.routes.read().unwrap().clone()
    }
}

/// How [`serve`] reloads its config: on each trigger, with `load`.
pub struct Reload<L> {
    pub triggers: crate::reload::Triggers,
    pub load: L,
}

/// Serve the webhooks of `config` on `listener`, and notify its humans, until a webhook's or a human's credentials
/// fail. Each webhook and human connects as itself to the config's `nats`, else to `nats`; until it is connected, its
/// requests answer 503. Health and metrics are served on `config.metrics`, if given.
///
/// On each trigger of `reload` the config is loaded again: webhooks and humans newly listed start, those no longer
/// listed stop, those whose entry or credentials file changed start again, and the others carry on untouched, as
/// does the listener. A config that fails to load is logged and the one served stays.
pub async fn serve<L: FnMut() -> Result<Config>>(
    config: Config,
    nats: &str,
    js_domain: Option<&JsDomain>,
    listener: TcpListener,
    mut reload: Reload<L>,
) -> Result<()> {
    bus::check_url(nats)?;
    let mut tasks = JoinSet::new();
    let (failed_tx, mut failed) = mpsc::unbounded_channel();
    let mut running = Running::new(nats, js_domain, &config);
    let metrics_addr = config.metrics;
    let routes = running.apply(config, &mut tasks, &failed_tx);
    let server =
        Arc::new(Server { routes: RwLock::new(Arc::new(routes)), in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT)) });
    let _metrics = match metrics_addr {
        Some(addr) => Some(metrics::start(addr, server.clone()).await?),
        None => None,
    };
    loop {
        while tasks.try_join_next().is_some() {}
        let stream = tokio::select! {
            Some(e) = failed.recv() => return Err(e),
            trigger = reload.triggers.next() => {
                match (reload.load)() {
                    Ok(config) => {
                        let routes = running.apply(config, &mut tasks, &failed_tx);
                        *server.routes.write().unwrap() = Arc::new(routes);
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

        let mut hooks = HashMap::new();
        for webhook in config.webhooks {
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

        let mut humans = HashMap::new();
        for human in config.humans {
            let name = human.name.clone();
            let creds = crate::serve::creds_snapshot(human.creds.as_deref());
            match self.humans.remove(&name) {
                Some(old) if !hub_changed && !links_changed && old.served.config == human && old.creds == creds => {
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
}

/// Connect `hook` as itself, and open the bucket of delivered events, waiting for both as long as it takes.
async fn connect(hook: &Hook, nats: &str, js_domain: Option<&JsDomain>) -> Result<()> {
    let config = &hook.config;
    let bus = Bus::connect(&config.name, config.creds.as_deref(), nats, js_domain, "ants-webhook").await?;
    tracing::info!(webhook = %config.name, to = ?config.destination, "webhook ready");
    let _ = hook.bus.set(bus);
    Ok(())
}

impl Bus {
    /// Connect as `name`, and open the bucket of delivered events, waiting for both as long as it takes.
    pub(crate) async fn connect(
        name: &UserId,
        creds: Option<&Path>,
        nats: &str,
        js_domain: Option<&JsDomain>,
        client_name: &str,
    ) -> Result<Self> {
        let pending = std::future::pending();
        let (client, mut js) =
            bus::connect_retrying(nats, creds, js_domain, name, client_name, pending).await?.context("stopped")?;
        js.set_timeout(ACK_TIMEOUT);
        let seen = DirectKv::new(&js, js_domain, WEBHOOKS_BUCKET).await?;
        let mut delay = Duration::from_secs(1);
        loop {
            let error = match seen.exists(name).await {
                Ok(true) => break,
                Ok(false) => format!("no bucket {WEBHOOKS_BUCKET}"),
                Err(e) => format!("{e:#}"),
            };
            tracing::warn!(user = %name, error, retry_in = ?delay, "no bucket {WEBHOOKS_BUCKET} yet: `ants init` creates it");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
        let (marker, marked) = (crate::versions::serving().await, Arc::new(Mutex::new(None)));
        let keeping = (seen.clone(), webhook_marker_key(name), marker.clone(), marked.clone());
        let keeper = tokio::spawn(async move {
            let (seen, key, marker, marked) = keeping;
            let put = move || *marked.lock().unwrap() = Some(Instant::now());
            crate::versions::keep_putting(seen, key, marker, put).await;
        });
        Ok(Self { client, js, js_domain: js_domain.cloned(), seen, marker, marked, keeper })
    }

    /// Deliver `event` as a new thread unless it was delivered already, and return once the hub's stream stored it.
    pub(crate) async fn deliver(&self, event: Delivery<'_>) -> Result<Delivered> {
        let Delivery { from, destination, mentions, owner, allow, key, message_id, text } = event;
        let allow = allow.map(<[String]>::to_vec);
        if let Some(seen) = self.seen(&key).await? {
            return Ok(Delivered { thread: Some(seen.thread), duplicate: true });
        }
        self.mark(from).await?;
        let (subject, payload, thread) = match destination {
            Destination::Ant(ant) => {
                let message = Envelope::new(from.clone(), ant.clone(), text)
                    .with_owner(owner.cloned())
                    .with_allow(allow)
                    .from_webhook();
                if let Some(owner) = owner {
                    owners::recording(&self.js, self.js_domain.as_ref(), &message.thread_id, ant, owner).await;
                }
                (message.subject(), serde_json::to_vec(&message)?, message.thread_id)
            }
            Destination::Channel(channel) => {
                let mentions = match mentions {
                    Mentions::Nobody => Vec::new(),
                    Mentions::Listed(mentions) => mentions.clone(),
                    Mentions::Members => members(&self.js, channel).await?,
                };
                let post = ChannelPost::new(channel.clone(), from.clone(), text)
                    .with_owner(owner.cloned())
                    .with_allow(allow)
                    .from_webhook(mentions);
                (post.subject(), serde_json::to_vec(&post)?, post.thread_id)
            }
        };
        let message = jetstream::message::PublishMessage::build().payload(payload.into()).message_id(message_id);
        let ack = match self.js.send_publish(subject.clone(), message).await?.await {
            Ok(ack) => ack,
            Err(e) => return Err(bus::explain(&self.js, &subject, e).await).context("waiting for the hub to store it"),
        };
        if ack.duplicate {
            let thread = self.seen(&key).await.ok().flatten().map(|seen| seen.thread);
            return Ok(Delivered { thread, duplicate: true });
        }
        let seen = serde_json::to_vec(&Seen { thread: thread.clone(), seq: ack.sequence })?;
        for attempt in 1..=3 {
            match self.seen.create(&key, seen.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    tracing::warn!(user = %from, error = format!("{e:#}"), attempt, "recording a delivered event");
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                }
            }
        }
        Ok(Delivered { thread: Some(thread), duplicate: false })
    }

    /// Put the marker of `name` ([`webhook_marker_key`]), unless put in the last [`SERVING_EVERY`]: without it, daemons
    /// take its messages for an ant's, and answer it in its own inbox.
    async fn mark(&self, name: &UserId) -> Result<()> {
        if self.marked.lock().unwrap().is_some_and(|at| at.elapsed() < SERVING_EVERY) {
            return Ok(());
        }
        self.seen.put(webhook_marker_key(name), self.marker.clone()).await.context("putting the marker")?;
        *self.marked.lock().unwrap() = Some(Instant::now());
        Ok(())
    }

    async fn seen(&self, key: &str) -> Result<Option<Seen>> {
        let found = self.seen.get(key).await.context("reading the webhooks bucket")?;
        Ok(found.and_then(|value| serde_json::from_slice(&value).ok()))
    }
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
        let routes = self.routes();
        let name = parts.uri.path().strip_prefix(&routes.prefix);
        if let Some(answers) = routes.answers.as_ref().filter(|_| name == notify::APPROVAL_PATH.strip_prefix('/')) {
            if let Err(e) = read_body(body, false).await {
                return refuse(StatusCode::BAD_REQUEST, format!("reading the body: {e}"));
            }
            return Ok(answers.answer(&parts.method, parts.uri.query()).await);
        }
        let Some(hook) = name.and_then(|name| routes.hooks.get(name)) else {
            let _ = read_body(body, false).await;
            return refuse(StatusCode::NOT_FOUND, "no such webhook");
        };
        let outcome = hook.take(parts, body).await;
        hook.outcomes.count(&outcome);
        outcome
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
            Some(Refusal(StatusCode::UNAUTHORIZED, "missing or wrong token".to_owned()))
        } else {
            None
        };
        let body = read_body(body, refused.is_none()).await;
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
            return Ok(accepted(StatusCode::OK, delivered.thread.as_ref(), true));
        }
        if let Some(thread) = &delivered.thread {
            tracing::info!(webhook = %hook.name, %thread, "event delivered");
        }
        Ok(accepted(StatusCode::ACCEPTED, delivered.thread.as_ref(), false))
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
pub(crate) mod tests;
