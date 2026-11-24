//! The webhooks' config file: each `[[webhook]]` checked and resolved into a [`Webhook`], with its secret, id source
//! and text template, and the outline `ants doctor` reads of it.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::schedule::Mentions;
use ants_proto::{ChannelId, Destination, JsDomain, TrailName, TrailStart, UserId};
use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::BASE64;
use hyper::http::HeaderName;
use serde::Deserialize;

use super::{DEFAULT_PATH, DEFAULT_SIGNATURE_HEADER, DEFAULT_TEXT, DEFAULT_TOLERANCE, MAX_TOLERANCE, WEBHOOK_ID};
use crate::notify;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct File {
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

/// The argument of a run a webhook's event starts that carries the event's text.
pub(super) const TEXT_ARG: &str = "text";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    name: UserId,
    creds: Option<PathBuf>,
    auth: AuthKind,
    secret_env: Option<String>,
    secret_file: Option<PathBuf>,
    signature_header: Option<String>,
    token_header: Option<String>,
    tolerance: Option<String>,
    id: Option<String>,
    ant: Option<UserId>,
    channel: Option<ChannelId>,
    mention: Option<Vec<UserId>>,
    #[serde(default)]
    broadcast: bool,
    owner: Option<UserId>,
    text: Option<String>,
    allow: Option<Vec<String>>,
    trail: Option<TrailName>,
    args: Option<serde_json::Map<String, serde_json::Value>>,
    limits: Option<ants_daemon::trail::AskedLimits>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
enum AuthKind {
    HmacSha256,
    Bearer,
    Token,
    StandardWebhooks,
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
    pub(super) auth: Auth,
    pub(super) id: IdSource,
    pub destination: Destination,
    pub(super) mentions: Mentions,
    pub(super) owner: Option<UserId>,
    pub(super) text: Vec<Piece>,
    /// The tool rules the threads it starts are narrowed to; None for [`ants_proto::WEBHOOK_ALLOW`].
    pub(super) allow: Option<Vec<String>>,
    /// The saved trail each event starts, with the ant as its conductor, under the limits the entry gives and with
    /// its args, to which the event's text is added as `text`; None to prompt the ant with the text.
    pub trail: Option<TrailStart>,
}

#[derive(PartialEq)]
pub(super) enum Auth {
    /// The hex of the body's HMAC-SHA256, keyed with the secret, in the header, after `sha256=` or alone.
    Hmac { secret: String, header: HeaderName },
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// The token alone in the header, such as GitLab's `X-Gitlab-Token`.
    Token { secret: String, header: HeaderName },
    /// [Standard Webhooks](https://www.standardwebhooks.com), such as GitLab's signing token: `webhook-signature`
    /// lists `v1,<base64>` HMAC-SHA256 signatures of `<webhook-id>.<webhook-timestamp>.<body>`, keyed with the
    /// secret's base64 after `whsec_`, one of which must match, and the timestamp is within `tolerance` of now.
    Standard { key: Vec<u8>, tolerance: Duration },
}

#[derive(Debug, PartialEq)]
pub(super) enum IdSource {
    Header(HeaderName),
    Json(String),
    BodySha256,
}

#[derive(Debug, PartialEq)]
pub(super) enum Piece {
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
pub(super) fn resolver<'a>(path: &'a Path, home: &'a Path) -> impl Fn(&Path) -> PathBuf + 'a {
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
        if entry.tolerance.is_some() && !matches!(entry.auth, AuthKind::StandardWebhooks) {
            bail!("tolerance goes with auth = \"standard-webhooks\" only");
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
            AuthKind::StandardWebhooks => {
                let tolerance = match entry.tolerance.as_deref() {
                    None => DEFAULT_TOLERANCE,
                    Some(text) => {
                        let tolerance = ants_proto::duration::parse(text).map_err(|e| anyhow!(e))?;
                        if tolerance.is_zero() || tolerance > MAX_TOLERANCE {
                            bail!("tolerance {text:?}: from 1s to {}", ants_proto::duration::format(MAX_TOLERANCE));
                        }
                        tolerance
                    }
                };
                Auth::Standard { key: signing_key(&secret)?, tolerance }
            }
        };
        let id = match entry.id.as_deref() {
            None if matches!(auth, Auth::Standard { .. }) => IdSource::Header(WEBHOOK_ID),
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
        let trail = match entry.trail {
            None if entry.args.is_some() => bail!("args go with trail only"),
            None if entry.limits.is_some() => bail!("limits go with trail only"),
            None => None,
            Some(_) if !matches!(destination, Destination::Ant(_)) => {
                bail!("a webhook starting a trail names the ant conducting its runs, not a channel")
            }
            Some(_) if entry.allow.is_some() => {
                bail!("allow goes without trail: a trail's steps are narrowed as a webhook's threads are")
            }
            Some(_) if entry.args.as_ref().is_some_and(|args| args.contains_key(TEXT_ARG)) => {
                bail!("args.{TEXT_ARG} is the event's text: name the argument otherwise")
            }
            Some(trail) => {
                let args = serde_json::Value::Object(entry.args.unwrap_or_default());
                let limits = entry.limits.unwrap_or_default().limits().map_err(|e| anyhow!("limits: {e}"))?;
                Some(TrailStart { trail, args, limits })
            }
        };
        let (owner, allow) = (entry.owner, entry.allow);
        Ok(Self { name: entry.name, creds, auth, id, destination, mentions, owner, text, allow, trail })
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
    let claims =
        crate::people::humans::creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
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
    if crate::people::humans::writes_humans(&claims) {
        return Err(issue("are a human's"));
    }
    if !crate::people::humans::may_publish(&claims, &destination.subject(name)) {
        return Err(issue("do not allow this destination"));
    }
    Ok(())
}

/// The HMAC key of a Standard Webhooks signing secret: its base64, after `whsec_`.
pub(crate) fn signing_key(secret: &str) -> Result<Vec<u8>> {
    let encoded = secret.strip_prefix("whsec_").unwrap_or(secret);
    match BASE64.decode(encoded.as_bytes()) {
        Ok(key) if !key.is_empty() => Ok(key),
        _ => bail!("the secret is no Standard Webhooks signing secret: whsec_ and base64"),
    }
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
pub(super) fn template(text: &str) -> Result<Vec<Piece>> {
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

pub(super) fn placeholder(name: &str) -> Result<Piece> {
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
