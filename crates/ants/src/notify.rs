//! Notifications of `ants webhook serve`: the tool requests and escalations of the humans its config lists
//! (`[[human]]`), pushed to their phones and chats, with approve and reject buttons where the service has them.
//!
//! Each human is a NATS user of its own, with its human credentials, and takes its notifications from its durable
//! consumer [`notify_consumer`]: every ant's tool requests (`ants.approvals.<ant>`) and the escalations reaching its
//! inbox. Every process listing the human shares that consumer, so each request or escalation is notified once,
//! whichever daemon raised it, and the consumer starts with what arrives from its creation on. A request already
//! decided or expired, or an escalation older than [`ESCALATION_MAX_AGE`], is skipped; a sink that fails is logged
//! and the others still get the notice.
//!
//! A sink may be the push target of one device of the human (`device`): it gets no notice of what that device follows
//! as it comes, the human's inbox for an escalation and the tool requests for an approval, while the device's
//! [`ants_proto::Presence`] says it follows it ([`Present`]). A sink of no device gets every notice.
//!
//! With `public_url` and an action secret, an approval notice carries one link per verdict the request offers: the
//! request's ant, id and stream sequence, the verdict, the human and the request's expiry, signed with HMAC-SHA256
//! ([`Links`]). `GET` on a link only shows the request and a button that `POST`s to it, so a link preview decides
//! nothing; `POST` checks the signature and expiry and stores the decision with the human's credentials, exactly as
//! `ants tools approve` or `reject` does (the first decision wins). ntfy's buttons `POST` directly; Slack's and
//! Telegram's open the confirmation page.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{
    ApprovalRequest, DeviceId, Envelope, EscalationReason, Following, JsDomain, STREAM, UserId, Uuid, Verdict,
    notify_consumer, subject,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::{self, consumer, stream::Stream};
use chrono::{DateTime, TimeDelta, Utc};
use data_encoding::{HEXLOWER, HEXLOWER_PERMISSIVE};
use futures::StreamExt as _;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Bytes;
use hyper::http::{HeaderValue, Method, Request, Response, StatusCode, Uri, header};
use hyper_util::rt::TokioIo;
use ring::hmac;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::presence::Present;
use crate::tools;

/// Where approval links point, under the config's `path`. A webhook name starts with a letter or digit, so no
/// webhook is named like it.
pub const APPROVAL_PATH: &str = "/_approval";
/// How long one sink may take to take a notice.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the consumer waits for a notice to be sent before handing it out again.
const ACK_WAIT: Duration = Duration::from_secs(60);
const MAX_DELIVER: i64 = 3;
/// How long the consumer of a human no process lists any more stays.
const INACTIVE: Duration = Duration::from_hours(7 * 24);
/// Escalations older than this, as after a long outage, are not notified.
const ESCALATION_MAX_AGE: TimeDelta = TimeDelta::days(1);
/// Longest text taken from a request's input or an escalation.
const MAX_TEXT: usize = 1500;
/// How many notified stream sequences a human remembers, against a redelivery after a lost ack.
const RECENT: usize = 256;
/// Most of a sink's answer read, for its error.
const MAX_ANSWER: usize = 512;

// ---- config ----------------------------------------------------------------------------------------------------

fn yes() -> bool {
    true
}

/// One `[[human]]` of the webhook config file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanEntry {
    name: UserId,
    creds: Option<PathBuf>,
    #[serde(default = "yes")]
    approvals: bool,
    #[serde(default = "yes")]
    escalations: bool,
    ants: Option<Vec<UserId>>,
    #[serde(default)]
    notify: Vec<SinkEntry>,
}

/// One `[[human.notify]]`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SinkEntry {
    kind: SinkKind,
    url: Option<String>,
    topic: Option<String>,
    chat: Option<String>,
    secret_env: Option<String>,
    secret_file: Option<PathBuf>,
    /// The device of the human whose push target the sink is: none while that device follows what a notice is about.
    device: Option<DeviceId>,
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum SinkKind {
    Ntfy,
    Slack,
    Telegram,
}

/// A human to notify, checked.
pub struct Human {
    pub name: UserId,
    pub creds: Option<PathBuf>,
    /// The ants whose tool requests are notified: `None` for none, an empty list for every ant.
    approvals: Option<Vec<UserId>>,
    escalations: bool,
    sinks: Vec<Target>,
}

/// A sink, and the device of the human it is the push target of, if any. A sink of no device gets every notice.
#[derive(PartialEq, Eq)]
struct Target {
    device: Option<DeviceId>,
    sink: Sink,
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.device {
            Some(device) => write!(f, "{} of device {device}", self.sink.label()),
            None => f.write_str(&self.sink.label()),
        }
    }
}

/// Where a human's notices go. Its secrets are never logged: [`Sink::label`] names it.
#[derive(PartialEq, Eq)]
enum Sink {
    /// JSON published to the server `url` for `topic`, with `token` as bearer token.
    Ntfy { url: String, topic: String, token: Option<String> },
    /// A Slack incoming webhook, whose URL is the secret.
    Slack { url: String },
    /// A Telegram bot's `sendMessage` to `chat`, through the Bot API at `api`.
    Telegram { api: String, token: String, chat: String },
}

impl std::fmt::Debug for Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label())
    }
}

const NTFY: &str = "https://ntfy.sh";
const TELEGRAM: &str = "https://api.telegram.org";

impl Human {
    pub fn from_entry(
        entry: HumanEntry,
        resolve: &impl Fn(&Path) -> PathBuf,
        env: &impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        if entry.notify.is_empty() {
            bail!("no [[human.notify]] entries: nowhere to send");
        }
        let approvals = match (entry.approvals, entry.ants) {
            (false, Some(_)) => bail!("ants goes with approvals = true only"),
            (false, None) => None,
            (true, None) => Some(Vec::new()),
            (true, Some(ants)) if ants.is_empty() => bail!("ants = []: give the ants, or approvals = false"),
            (true, Some(ants)) => Some(ants),
        };
        if approvals.is_none() && !entry.escalations {
            bail!("approvals and escalations are both off: nothing to notify");
        }
        let sinks = entry
            .notify
            .into_iter()
            .enumerate()
            .map(|(i, sink)| {
                let device = sink.device;
                let sink = Sink::from_entry(sink, resolve, env).with_context(|| format!("notify #{}", i + 1))?;
                Ok(Target { device, sink })
            })
            .collect::<Result<Vec<_>>>()?;
        let creds = entry.creds.as_deref().map(resolve);
        Ok(Self { name: entry.name, creds, approvals, escalations: entry.escalations, sinks })
    }

    /// Fail unless the credentials are the human's.
    pub fn check_creds(&self) -> Result<()> {
        let Some(path) = &self.creds else { return Ok(()) };
        let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
        let claims = crate::humans::creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
        let name = &self.name;
        let human = crate::humans::may_publish(&claims, &format!("$KV.{}.>", ants_proto::HUMANS_BUCKET));
        if claims["name"].as_str() != Some(name.as_str()) || !human {
            bail!(
                "credentials {} are not human {name}'s; issue them with `ants auth user {name} --human`",
                path.display()
            );
        }
        if self.devices().next().is_some() && !crate::humans::may_publish(&claims, &subject::presence_of(name)) {
            bail!(
                "credentials {} predate presence, which sinks of a device need; issue them again with `ants auth \
                 user {name} --human`",
                path.display()
            );
        }
        Ok(())
    }

    /// The devices that sinks are push targets of.
    fn devices(&self) -> impl Iterator<Item = DeviceId> {
        self.sinks.iter().filter_map(|t| t.device)
    }

    /// The consumer's subjects: tool requests and the human's inbox, as configured.
    fn subjects(&self) -> Vec<String> {
        let approvals = self.approvals.is_some().then(|| subject::ALL_APPROVAL_REQUESTS.to_owned());
        let escalations = self.escalations.then(|| subject::inbox_of(&self.name));
        approvals.into_iter().chain(escalations).collect()
    }

    fn wants_request_of(&self, ant: &UserId) -> bool {
        self.approvals.as_ref().is_some_and(|ants| ants.is_empty() || ants.contains(ant))
    }
}

/// `url` checked as an http(s) URL without credentials or query, without its trailing `/`.
fn http_url(url: &str, what: &str) -> Result<String> {
    let uri: Uri = url.parse().with_context(|| format!("{what} {url:?}"))?;
    if !matches!(uri.scheme_str(), Some("https" | "http")) || uri.host().is_none_or(str::is_empty) {
        bail!("{what} {url:?}: expected https://host/...");
    }
    if uri.authority().is_some_and(|a| a.as_str().contains('@')) || uri.query().is_some() {
        bail!("{what} {url:?}: no user, password or query in it");
    }
    Ok(url.trim_end_matches('/').to_owned())
}

impl Sink {
    fn from_entry(
        entry: SinkEntry,
        resolve: &impl Fn(&Path) -> PathBuf,
        env: &impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let secret = match (&entry.secret_env, &entry.secret_file) {
            (Some(var), None) => Some(env(var).with_context(|| format!("environment variable {var} is not set"))?),
            (None, Some(file)) => Some(crate::webhook::read_secret(&resolve(file))?),
            (None, None) => None,
            _ => bail!("give at most one of secret_env and secret_file"),
        };
        let secret = match secret.map(|s| s.trim().to_owned()) {
            Some(s) if s.is_empty() => bail!("empty secret"),
            other => other,
        };
        let only = |what: &str, given: bool| match given {
            true => Err(anyhow!("{what} does not go with kind = {:?}", entry.kind)),
            false => Ok(()),
        };
        match entry.kind {
            SinkKind::Ntfy => {
                only("chat", entry.chat.is_some())?;
                let topic = entry.topic.context("ntfy needs topic")?;
                if topic.is_empty() || !topic.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
                    bail!("topic {topic:?}: letters, digits, - and _ only");
                }
                let url = http_url(entry.url.as_deref().unwrap_or(NTFY), "url")?;
                Ok(Self::Ntfy { url, topic, token: secret })
            }
            SinkKind::Slack => {
                only("topic", entry.topic.is_some())?;
                only("chat", entry.chat.is_some())?;
                if entry.url.is_some() {
                    bail!("the incoming webhook URL is a secret: give it with secret_file or secret_env, not url");
                }
                let url = secret.context("slack needs its incoming webhook URL in secret_file or secret_env")?;
                Ok(Self::Slack {
                    url: http_url(&url, "the incoming webhook URL").map_err(|_| {
                        anyhow!("the secret is no incoming webhook URL: expected https://hooks.slack.com/services/...")
                    })?,
                })
            }
            SinkKind::Telegram => {
                only("topic", entry.topic.is_some())?;
                let chat = entry.chat.context("telegram needs chat, the chat id")?;
                let token = secret.context("telegram needs the bot token in secret_file or secret_env")?;
                if token.contains(['/', '?', '#']) || token.chars().any(char::is_whitespace) {
                    bail!("the secret is no bot token");
                }
                let api = http_url(entry.url.as_deref().unwrap_or(TELEGRAM), "url")?;
                Ok(Self::Telegram { api, token, chat })
            }
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Ntfy { topic, .. } => format!("ntfy topic {topic}"),
            Self::Slack { .. } => "slack".to_owned(),
            Self::Telegram { chat, .. } => format!("telegram chat {chat}"),
        }
    }

    /// Where to `POST` `notice`, the bearer token if any, and the JSON body.
    fn request(&self, notice: &Notice) -> (String, Option<&str>, Value) {
        match self {
            Self::Ntfy { url, topic, token } => {
                let actions: Vec<Value> = notice
                    .buttons
                    .iter()
                    .map(|b| json!({ "action": "http", "label": b.label, "url": b.url, "method": "POST", "clear": true }))
                    .collect();
                let mut body =
                    json!({ "topic": topic, "title": notice.title, "message": notice.text, "tags": ["ant"] });
                if notice.urgent {
                    body["priority"] = json!(4);
                }
                if !actions.is_empty() {
                    body["actions"] = Value::Array(actions);
                }
                (url.clone(), token.as_deref(), body)
            }
            Self::Slack { url } => {
                let text = cut(&format!("*{}*\n{}", slack_escape(&notice.title), slack_escape(&notice.text)), 2900);
                let mut blocks = vec![json!({ "type": "section", "text": { "type": "mrkdwn", "text": text } })];
                if !notice.buttons.is_empty() {
                    let elements: Vec<Value> = notice
                        .buttons
                        .iter()
                        .map(|b| {
                            let mut button = json!({
                                "type": "button",
                                "text": { "type": "plain_text", "text": b.label },
                                "url": b.url,
                                "action_id": format!("ants-{}", b.verdict.as_str()),
                            });
                            button["style"] = json!(if b.verdict == Verdict::Reject { "danger" } else { "primary" });
                            button
                        })
                        .collect();
                    blocks.push(json!({ "type": "actions", "elements": elements }));
                }
                (url.clone(), None, json!({ "text": slack_escape(&notice.title), "blocks": blocks }))
            }
            Self::Telegram { api, token, chat } => {
                let mut body = json!({
                    "chat_id": chat,
                    "text": cut(&format!("{}\n\n{}", notice.title, notice.text), 4000),
                    "disable_web_page_preview": true,
                });
                if !notice.buttons.is_empty() {
                    let row: Vec<Value> =
                        notice.buttons.iter().map(|b| json!({ "text": b.label, "url": b.url })).collect();
                    body["reply_markup"] = json!({ "inline_keyboard": [row] });
                }
                (format!("{api}/bot{token}/sendMessage"), None, body)
            }
        }
    }
}

fn slack_escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// `text` cut to `max` characters, with `…` when cut.
fn cut(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((at, _)) => format!("{}…", &text[..at]),
        None => text.to_owned(),
    }
}

// ---- links -----------------------------------------------------------------------------------------------------

/// Signed approve and reject links under `public_url`, the address the reverse proxy gives the config's `path`.
pub struct Links {
    base: String,
    key: hmac::Key,
}

/// What one link decides.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Action {
    ant: UserId,
    id: Uuid,
    /// The request's stream sequence, where it is read back.
    seq: u64,
    verdict: Verdict,
    /// The human whose credentials store the decision, the one notified.
    by: UserId,
    /// The request's expiry, in Unix seconds.
    expires: i64,
}

#[derive(Debug, PartialEq, Eq)]
enum LinkError {
    Malformed,
    Forged,
    Expired,
}

fn parse_verdict(text: &str) -> Option<Verdict> {
    [Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject].into_iter().find(|v| v.as_str() == text)
}

impl Action {
    fn query(&self) -> String {
        format!(
            "ant={}&id={}&seq={}&verdict={}&by={}&exp={}",
            self.ant,
            self.id,
            self.seq,
            self.verdict.as_str(),
            self.by,
            self.expires
        )
    }

    /// What the signature covers: every field, in a fixed order, under a label of its own.
    fn signed(&self) -> String {
        format!("ants-approval\n{}", self.query().replace('&', "\n"))
    }
}

impl Links {
    pub fn new(public_url: &str, secret: &str) -> Result<Self> {
        let base = http_url(public_url, "public_url")?;
        if secret.len() < 16 {
            bail!("the action secret is too short: give at least 16 random characters");
        }
        Ok(Self { base, key: hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()) })
    }

    fn url(&self, action: &Action) -> String {
        let sig = HEXLOWER.encode(hmac::sign(&self.key, action.signed().as_bytes()).as_ref());
        format!("{}{APPROVAL_PATH}?{}&sig={sig}", self.base, action.query())
    }

    /// The action of a link's query, if its signature holds and it has not expired at `now`.
    fn verify(&self, query: &str, now: DateTime<Utc>) -> Result<Action, LinkError> {
        let mut fields = HashMap::new();
        for pair in query.split('&') {
            let (name, value) = pair.split_once('=').ok_or(LinkError::Malformed)?;
            if fields.insert(name, value).is_some() {
                return Err(LinkError::Malformed);
            }
        }
        let field = |name: &str| fields.get(name).copied().ok_or(LinkError::Malformed);
        let action = Action {
            ant: field("ant")?.parse().map_err(|_| LinkError::Malformed)?,
            id: field("id")?.parse().map_err(|_| LinkError::Malformed)?,
            seq: field("seq")?.parse().map_err(|_| LinkError::Malformed)?,
            verdict: parse_verdict(field("verdict")?).ok_or(LinkError::Malformed)?,
            by: field("by")?.parse().map_err(|_| LinkError::Malformed)?,
            expires: field("exp")?.parse().map_err(|_| LinkError::Malformed)?,
        };
        if fields.len() != 7 {
            return Err(LinkError::Malformed);
        }
        let sig = HEXLOWER_PERMISSIVE.decode(field("sig")?.as_bytes()).map_err(|_| LinkError::Forged)?;
        hmac::verify(&self.key, action.signed().as_bytes(), &sig).map_err(|_| LinkError::Forged)?;
        if now.timestamp() >= action.expires {
            return Err(LinkError::Expired);
        }
        Ok(action)
    }
}

// ---- notices ---------------------------------------------------------------------------------------------------

/// What a sink sends.
#[derive(Debug, PartialEq, Eq)]
struct Notice {
    title: String,
    text: String,
    buttons: Vec<Button>,
    urgent: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct Button {
    label: &'static str,
    url: String,
    verdict: Verdict,
}

fn label(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::AllowOnce => "Approve",
        Verdict::AllowAlways => "Always allow",
        Verdict::Reject => "Reject",
    }
}

/// The notice of `request`, stored at stream sequence `seq`, for `human`, with a button per verdict it offers when
/// there are `links`, and the allow rule `suggested` for it ([`tools::suggestion`]).
fn approval_notice(
    request: &ApprovalRequest,
    seq: u64,
    human: &UserId,
    links: Option<&Links>,
    suggested: Option<&tools::Suggestion>,
    now: DateTime<Utc>,
) -> Notice {
    let tool = &request.tool;
    let mut text = format!("{} ({})", tool.title, tool.kind);
    if !tool.locations.is_empty() {
        let _ = write!(text, "\nfiles: {}", tool.locations.join(", "));
    }
    if let Some(input) = &tool.input {
        let _ = write!(text, "\ninput: {}", cut(input, MAX_TEXT));
    }
    let id = tools::qualified(request);
    let _ = write!(
        text,
        "\nthread {}, request {id}, rejected in {} unless answered\nants tools approve {id} | ants tools reject {id}",
        request.thread_id,
        tools::left(request.expires_at, now),
    );
    if let Some(suggested) = suggested {
        let _ = write!(
            text,
            "\nallowed {} times before: `ants tools approve {id} --rule` also adds the allow rule {:?} to {}",
            suggested.approved_before, suggested.rule, request.ant
        );
    }
    let buttons = links
        .map(|links| {
            request
                .options
                .iter()
                .map(|&verdict| {
                    let action = Action {
                        ant: request.ant.clone(),
                        id: request.id,
                        seq,
                        verdict,
                        by: human.clone(),
                        expires: request.expires_at.timestamp(),
                    };
                    Button { label: label(verdict), url: links.url(&action), verdict }
                })
                .collect()
        })
        .unwrap_or_default();
    Notice { title: format!("{} asks to run a tool", request.ant), text, buttons, urgent: true }
}

/// The notice of an escalation `envelope` brings, if it brings one. The thread to answer on is the escalation's: an
/// expired tool request's notice comes on a thread of its own.
fn escalation_notice(envelope: &Envelope) -> Option<Notice> {
    let escalation = envelope.escalation.as_ref()?;
    let thread = &escalation.thread_id;
    let text = format!(
        "{}\n\nthread {thread}; answer with ants ask --thread {thread} @{} TEXT",
        cut(&envelope.text, MAX_TEXT),
        envelope.from,
    );
    let title = match escalation.reason {
        EscalationReason::Approval => format!("{}'s tool request expired unanswered", escalation.stuck),
        reason => format!("{} escalates: {} on {}", envelope.from, reason.as_str(), escalation.stuck),
    };
    Some(Notice { title, text, buttons: Vec::new(), urgent: false })
}

// ---- sending ---------------------------------------------------------------------------------------------------

fn tls() -> Result<TlsConnector> {
    static TLS: OnceLock<TlsConnector> = OnceLock::new();
    if let Some(tls) = TLS.get() {
        return Ok(tls.clone());
    }
    let tls = crate::bridge::tls_connector()?;
    Ok(TLS.get_or_init(|| tls).clone())
}

trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

/// `POST` `body` as JSON to `url`. Errors never name the URL, which may hold a secret.
async fn post_json(url: &str, bearer: Option<&str>, body: &Value) -> Result<()> {
    let uri: Uri = url.parse().map_err(|_| anyhow!("a malformed URL"))?;
    let https = uri.scheme_str() == Some("https");
    let host = uri.host().context("a URL without a host")?;
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    let authority = uri.authority().context("a URL without a host")?.as_str().to_owned();
    let tcp = TcpStream::connect((host.trim_start_matches('[').trim_end_matches(']'), port))
        .await
        .with_context(|| format!("connecting to {host}:{port}"))?;
    let io: Box<dyn Io> = match https {
        true => {
            let name = ServerName::try_from(host.trim_start_matches('[').trim_end_matches(']').to_owned())
                .context("the URL's host name")?;
            Box::new(tls()?.connect(name, tcp).await.context("TLS handshake")?)
        }
        false => Box::new(tcp),
    };
    let (mut send, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let path = uri.path_and_query().map_or("/", |p| p.as_str());
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header(header::HOST, authority)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::USER_AGENT, "ants");
    if let Some(token) = bearer {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = request.body(Full::new(Bytes::from(serde_json::to_vec(body)?)))?;
    let response = send.send_request(request).await.context("sending")?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let answer = Limited::new(response.into_body(), MAX_ANSWER * 8).collect().await;
    let answer = answer.map(|b| b.to_bytes()).unwrap_or_default();
    let answer = String::from_utf8_lossy(&answer[..answer.len().min(MAX_ANSWER)]).into_owned();
    bail!("answered {status}: {}", answer.trim())
}

/// Send `notice` to every sink of `human` at once, but those of the devices `away` leaves out; a failure is logged,
/// never fatal.
async fn send(human: &Human, notice: &Notice, away: impl Fn(&DeviceId) -> bool) {
    let targets = human.sinks.iter().filter(|t| t.device.as_ref().is_none_or(&away));
    let sends = targets.map(|Target { sink, .. }| async move {
        let (url, bearer, body) = sink.request(notice);
        let sent = tokio::time::timeout(SEND_TIMEOUT, post_json(&url, bearer, &body)).await;
        match sent {
            Ok(Ok(())) => tracing::debug!(human = %human.name, sink = sink.label(), "notified"),
            Ok(Err(e)) => tracing::warn!(human = %human.name, sink = sink.label(), "notifying: {e:#}"),
            Err(_) => tracing::warn!(human = %human.name, sink = sink.label(), "notifying: timed out"),
        }
    });
    futures::future::join_all(sends).await;
}

// ---- watching --------------------------------------------------------------------------------------------------

/// A human's connection to the hub.
struct HumanBus {
    client: async_nats::Client,
    js: jetstream::Context,
    stream: Stream,
    present: Present,
}

/// A human of the config, and its connection once made.
pub struct Notified {
    pub config: Human,
    bus: OnceCell<HumanBus>,
    recent: Mutex<VecDeque<u64>>,
}

impl Notified {
    pub fn new(config: Human) -> Self {
        Self { config, bus: OnceCell::new(), recent: Mutex::default() }
    }

    /// Whether its connection is made and up.
    pub fn connected(&self) -> bool {
        self.bus.get().is_some_and(|bus| ants_daemon::metrics::connected(&bus.client))
    }

    /// Whether the message at `seq` was notified, recording it if not.
    fn first_time(&self, seq: u64) -> bool {
        let mut recent = self.recent.lock().unwrap();
        if recent.contains(&seq) {
            return false;
        }
        if recent.len() == RECENT {
            recent.pop_front();
        }
        recent.push_back(seq);
        true
    }
}

/// Connect `human` as itself and notify it of what reaches its consumer, until the process ends. Fails only when the
/// connection does.
pub async fn run(
    human: Arc<Notified>,
    links: Option<Arc<Links>>,
    nats: String,
    js_domain: Option<JsDomain>,
) -> Result<()> {
    let name = &human.config.name;
    let creds = human.config.creds.as_deref();
    let (client, js) =
        bus::connect_retrying(&nats, creds, js_domain.as_ref(), name, "ants-notify", std::future::pending())
            .await?
            .context("stopped")?;
    let mut delay = Duration::from_secs(1);
    let stream = loop {
        match js.get_stream(STREAM).await {
            Ok(stream) => break stream,
            Err(e) => {
                tracing::warn!(human = %name, error = %e, retry_in = ?delay, "no stream {STREAM} yet: `ants init` creates it");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    };
    // Through a leaf node, a direct get of a missing bucket goes unanswered rather than finding none.
    if human.config.devices().next().is_some()
        && let Err(e) = crate::presence::ensure_bucket(&js).await
    {
        tracing::warn!(human = %name, "sinks of a device get every notice: {e:#}");
    }
    let present = Present::new(&js, js_domain.as_ref()).await?;
    let _ = human.bus.set(HumanBus { client, js, stream, present });
    let bus = human.bus.get().expect("just set");
    tracing::info!(human = %name, sinks = ?human.config.sinks, "notifying");
    let mut delay = Duration::from_secs(1);
    loop {
        match watch(&human, bus, links.as_deref()).await {
            Ok(()) => delay = Duration::from_secs(1),
            Err(e) => {
                tracing::warn!(human = %name, retry_in = ?delay, "notifications: {e:#}");
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
        tokio::time::sleep(delay).await;
    }
}

/// Take notifications from the human's consumer, created or updated first, until its messages end.
async fn watch(human: &Notified, bus: &HumanBus, links: Option<&Links>) -> Result<()> {
    let name = notify_consumer(&human.config.name);
    let consumer: consumer::PullConsumer = bus
        .stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(name.clone()),
            filter_subjects: human.config.subjects(),
            deliver_policy: consumer::DeliverPolicy::New,
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: ACK_WAIT,
            max_deliver: MAX_DELIVER,
            inactive_threshold: INACTIVE,
            ..Default::default()
        })
        .await
        .with_context(|| format!("creating consumer {name}"))?;
    let mut messages = consumer.messages().await.with_context(|| format!("reading consumer {name}"))?;
    while let Some(message) = messages.next().await {
        let message = match message {
            Ok(message) => message,
            Err(e) => {
                tracing::debug!(human = %human.config.name, error = %e, "notifications");
                continue;
            }
        };
        let seq = message.info().map(|i| i.stream_sequence).unwrap_or_default();
        if human.first_time(seq) {
            notify(human, bus, links, &message.subject, &message.payload, seq).await;
        }
        if let Err(e) = message.ack().await {
            tracing::warn!(human = %human.config.name, error = %e, "acking a notification");
        }
    }
    Ok(())
}

/// Notify `human` of the message at `seq` on `subject`, if it is a pending tool request it wants or an escalation.
async fn notify(human: &Notified, bus: &HumanBus, links: Option<&Links>, subject: &str, payload: &[u8], seq: u64) {
    let config = &human.config;
    let now = Utc::now();
    let escalation = subject.starts_with(subject::inbox_of(&config.name).trim_end_matches('*'));
    let notice = if escalation {
        match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) if envelope.check_subject(subject).is_ok() && is_recent(&envelope.id, now) => {
                escalation_notice(&envelope)
            }
            _ => None,
        }
    } else {
        let request = match serde_json::from_slice::<ApprovalRequest>(payload) {
            Ok(request) if request.check_subject(subject).is_ok() => request,
            _ => {
                tracing::debug!(%subject, seq, "skipping an unreadable or forged tool request");
                return;
            }
        };
        if !config.wants_request_of(&request.ant) || request.is_expired(now) {
            return;
        }
        match tools::state(&bus.js, &bus.stream, seq, &request, now).await {
            Ok(tools::State::Pending) => {
                let suggested = tools::suggestion(&bus.stream, seq, &request).await.unwrap_or_else(|e| {
                    tracing::warn!(human = %config.name, error = format!("{e:#}"), "counting earlier approvals");
                    None
                });
                Some(approval_notice(&request, seq, &config.name, links, suggested.as_ref(), now))
            }
            Ok(_) => None,
            Err(e) => {
                tracing::warn!(human = %config.name, error = format!("{e:#}"), "reading a tool request's state");
                Some(approval_notice(&request, seq, &config.name, links, None, now))
            }
        }
    };
    let Some(notice) = notice else { return };
    let what = if escalation { Following::Inbox } else { Following::Approvals };
    let reading = bus.present.following(&config.name, config.devices(), what).await;
    if !reading.is_empty() {
        tracing::debug!(human = %config.name, devices = ?reading, what = what.as_str(), "skipping devices reading");
    }
    send(config, &notice, |device| !reading.contains(device)).await;
}

/// Whether the message of id `id`, a UUIDv7 stamped when sent, is younger than [`ESCALATION_MAX_AGE`].
fn is_recent(id: &Uuid, now: DateTime<Utc>) -> bool {
    let sent = id.get_timestamp().and_then(|t| {
        let (secs, nanos) = t.to_unix();
        DateTime::from_timestamp(i64::try_from(secs).ok()?, nanos)
    });
    sent.is_none_or(|sent| now - sent < ESCALATION_MAX_AGE)
}

// ---- answering -------------------------------------------------------------------------------------------------

/// The approval links' end: the humans of the config by name, and the key their links are signed with.
pub struct Answers {
    pub humans: HashMap<UserId, Arc<Notified>>,
    pub links: Arc<Links>,
}

impl Answers {
    /// Answer a request on an approval link: `GET` shows it with a button to confirm, `POST` decides.
    pub async fn answer(&self, method: &Method, query: Option<&str>) -> Response<Full<Bytes>> {
        if method != Method::GET && method != Method::POST {
            let mut response =
                page(StatusCode::METHOD_NOT_ALLOWED, "Not allowed", "<p>Links take GET and POST only.</p>");
            response.headers_mut().insert(header::ALLOW, HeaderValue::from_static("GET, POST"));
            return response;
        }
        let action = match self.links.verify(query.unwrap_or(""), Utc::now()) {
            Ok(action) => action,
            Err(LinkError::Malformed) => {
                return page(StatusCode::BAD_REQUEST, "Bad link", "<p>The link is incomplete.</p>");
            }
            Err(LinkError::Forged) => {
                tracing::warn!("refused an approval link with a wrong signature");
                return page(StatusCode::FORBIDDEN, "Bad link", "<p>The link's signature does not hold.</p>");
            }
            Err(LinkError::Expired) => {
                let text = "<p>The request expired: its ant rejected it.</p>";
                return page(StatusCode::GONE, "Expired", text);
            }
        };
        let Some(human) = self.humans.get(&action.by) else {
            return page(StatusCode::NOT_FOUND, "Unknown human", "<p>No such human here any more.</p>");
        };
        let Some(bus) =
            human.bus.get().filter(|b| b.client.connection_state() == async_nats::connection::State::Connected)
        else {
            let mut response = page(StatusCode::SERVICE_UNAVAILABLE, "Try again", "<p>The hub is not reachable.</p>");
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
            return response;
        };
        match self.decide(&action, bus, method == Method::POST, query.unwrap_or("")).await {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!(human = %action.by, error = format!("{e:#}"), "answering an approval link");
                let mut response = page(StatusCode::SERVICE_UNAVAILABLE, "Try again", "<p>The hub did not answer.</p>");
                response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
                response
            }
        }
    }

    /// Show the request `action` names, or store its verdict when `post`. Errors are the hub's.
    async fn decide(&self, action: &Action, bus: &HumanBus, post: bool, query: &str) -> Result<Response<Full<Bytes>>> {
        let id = format!("{}/{}", action.ant, action.id);
        let request = match bus.stream.get_raw_message(action.seq).await {
            Ok(msg) if msg.subject.as_str() == subject::approvals(&action.ant) => {
                serde_json::from_slice::<ApprovalRequest>(&msg.payload).ok().filter(|r| r.id == action.id)
            }
            Ok(_) => None,
            Err(e) if e.kind() == jetstream::stream::RawMessageErrorKind::NoMessageFound => None,
            Err(e) => return Err(e).context("reading the request"),
        };
        let Some(request) = request else {
            return Ok(page(
                StatusCode::NOT_FOUND,
                "No such request",
                &format!("<p>No tool request {}.</p>", html(&id)),
            ));
        };
        let what = label(action.verdict);
        let details = details(&request);
        let now = Utc::now();
        if !post {
            let state = tools::state(&bus.js, &bus.stream, action.seq, &request, now).await?;
            let body = match state {
                tools::State::Pending => format!(
                    "{details}<form method=\"post\" action=\"?{}\"><button type=\"submit\">{what}</button></form>",
                    html(query)
                ),
                other => format!("{details}<p>{}</p>", html(&settled(&other))),
            };
            return Ok(page(StatusCode::OK, &format!("{what}: {id}?"), &body));
        }
        match tools::answer_request(&bus.js, &bus.stream, action.seq, &request, action.verdict, now).await {
            Ok(decision) => {
                tracing::info!(human = %action.by, request = id, verdict = decision.verdict.as_str(), "decided on a link");
                let body = format!("{details}<p>Stored: {}.</p>", html(tools::spelling(decision.verdict)));
                Ok(page(StatusCode::OK, &format!("{what}: {id}"), &body))
            }
            Err(e)
                if e.downcast_ref::<jetstream::context::PublishError>()
                    .is_some_and(|e| e.kind() != jetstream::context::PublishErrorKind::WrongLastSequence) =>
            {
                Err(e)
            }
            Err(e) => {
                let body = format!("{details}<p>{}</p>", html(&format!("{e:#}")));
                Ok(page(StatusCode::CONFLICT, &format!("Not stored: {id}"), &body))
            }
        }
    }
}

fn settled(state: &tools::State) -> String {
    match state {
        tools::State::Pending => "Pending.".to_owned(),
        tools::State::Decided(d) => format!("Already answered: {}.", tools::spelling(d.verdict)),
        tools::State::DecidedByAgent(by, d) => format!("Already answered by {by}: {}.", tools::spelling(d.verdict)),
        tools::State::Expired => "Expired: its ant rejected it.".to_owned(),
    }
}

fn details(request: &ApprovalRequest) -> String {
    let tool = &request.tool;
    let mut out = format!(
        "<p><b>{}</b> asks to run <b>{}</b> ({}) on thread {}.</p>",
        html(request.ant.as_str()),
        html(&tool.title),
        html(&tool.kind),
        html(request.thread_id.as_str())
    );
    if !tool.locations.is_empty() {
        let _ = write!(out, "<p>Files: {}</p>", html(&tool.locations.join(", ")));
    }
    if let Some(input) = &tool.input {
        let _ = write!(out, "<pre>{}</pre>", html(&cut(input, MAX_TEXT)));
    }
    out
}

fn html(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

fn page(status: StatusCode, title: &str, body: &str) -> Response<Full<Bytes>> {
    let text = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width, initial-scale=1\"><meta name=\"robots\" content=\"noindex\">\
         <title>{0}</title></head><body><h1>{0}</h1>{body}</body></html>",
        html(title)
    );
    let mut response = Response::new(Full::new(Bytes::from(text)));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    response
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use ants_proto::{Escalation, ThreadId, ToolCall};

    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn human(text: &str) -> Result<Human> {
        let entry: HumanEntry = toml::from_str(text)?;
        let env = |name: &str| match name {
            "SLACK_URL" => Some("https://hooks.slack.com/services/T0/B0/xyz\n".to_owned()),
            "BOT_TOKEN" => Some("123:abc".to_owned()),
            _ => None,
        };
        Human::from_entry(entry, &|p: &Path| Path::new("/etc/ants").join(p), &env)
    }

    fn error(text: &str) -> String {
        format!("{:#}", human(text).err().expect("an error"))
    }

    const NTFY_SINK: &str = "[[notify]]\nkind = \"ntfy\"\ntopic = \"alice-ants\"\n";

    #[test]
    fn a_human_has_sinks_and_what_to_be_notified_of() {
        let phone = DeviceId::generate();
        let alice = human(&format!(
            "name = \"alice\"\ncreds = \"alice.creds\"\nants = [\"dev-a\"]\n{NTFY_SINK}\
             [[notify]]\nkind = \"slack\"\nsecret_env = \"SLACK_URL\"\n\
             [[notify]]\nkind = \"telegram\"\nchat = \"-10042\"\nsecret_env = \"BOT_TOKEN\"\ndevice = \"{phone}\"\n"
        ))
        .unwrap();
        assert_eq!(alice.creds.as_deref(), Some(Path::new("/etc/ants/alice.creds")));
        assert_eq!(alice.subjects(), ["ants.approvals.*", "ants.inbox.alice.*"]);
        assert!(alice.wants_request_of(&p("dev-a")) && !alice.wants_request_of(&p("dev-b")));
        let ntfy = Sink::Ntfy { url: NTFY.to_owned(), topic: "alice-ants".to_owned(), token: None };
        let slack = Sink::Slack { url: "https://hooks.slack.com/services/T0/B0/xyz".to_owned() };
        let telegram = Sink::Telegram { api: TELEGRAM.to_owned(), token: "123:abc".to_owned(), chat: "-10042".into() };
        let sinks: Vec<_> = alice.sinks.iter().map(|t| &t.sink).collect();
        assert_eq!(sinks, [&ntfy, &slack, &telegram]);
        assert_eq!(alice.devices().collect::<Vec<_>>(), [phone], "telegram is the phone's push target");
        assert_eq!(
            format!("{:?}", alice.sinks),
            format!("[ntfy topic alice-ants, slack, telegram chat -10042 of device {phone}]")
        );

        let quiet =
            human(&format!("name = \"bob\"\napprovals = false\n{NTFY_SINK}url = \"http://ntfy.lan/\"\n")).unwrap();
        assert_eq!(quiet.subjects(), ["ants.inbox.bob.*"]);
        assert!(!quiet.wants_request_of(&p("dev-a")));
        assert_matches!(&quiet.sinks[0].sink, Sink::Ntfy { url, .. } if url == "http://ntfy.lan");
        let every = human(&format!("name = \"bob\"\nescalations = false\n{NTFY_SINK}")).unwrap();
        assert_eq!(every.subjects(), ["ants.approvals.*"]);
        assert!(every.wants_request_of(&p("dev-b")));

        for (text, why) in [
            ("name = \"bob\"\n".to_owned(), "nowhere to send"),
            (format!("name = \"bob\"\napprovals = false\nescalations = false\n{NTFY_SINK}"), "nothing to notify"),
            (format!("name = \"bob\"\napprovals = false\nants = [\"dev-a\"]\n{NTFY_SINK}"), "approvals = true only"),
            (format!("name = \"bob\"\nants = []\n{NTFY_SINK}"), "give the ants"),
            ("name = \"bob\"\n[[notify]]\nkind = \"ntfy\"\n".to_owned(), "ntfy needs topic"),
            (format!("name = \"bob\"\n{NTFY_SINK}chat = \"1\"\n"), "chat does not go with kind = Ntfy"),
            ("name = \"bob\"\n[[notify]]\nkind = \"ntfy\"\ntopic = \"a/b\"\n".to_owned(), "letters, digits"),
            (format!("name = \"bob\"\n{NTFY_SINK}url = \"ftp://x\"\n"), "expected https://host"),
            (format!("name = \"bob\"\n{NTFY_SINK}url = \"https://u:p@x\"\n"), "no user, password or query"),
            (format!("name = \"bob\"\n{NTFY_SINK}secret_env = \"UNSET\"\n"), "UNSET is not set"),
            (format!("name = \"bob\"\n{NTFY_SINK}device = \"phone\"\n"), "device"),
            ("name = \"bob\"\n[[notify]]\nkind = \"slack\"\n".to_owned(), "slack needs its incoming webhook URL"),
            (
                "name = \"bob\"\n[[notify]]\nkind = \"slack\"\nurl = \"https://hooks.slack.com/x\"\n".to_owned(),
                "not url",
            ),
            (
                "name = \"bob\"\n[[notify]]\nkind = \"slack\"\nsecret_env = \"BOT_TOKEN\"\n".to_owned(),
                "no incoming webhook",
            ),
            ("name = \"bob\"\n[[notify]]\nkind = \"telegram\"\nsecret_env = \"BOT_TOKEN\"\n".to_owned(), "needs chat"),
            ("name = \"bob\"\n[[notify]]\nkind = \"telegram\"\nchat = \"1\"\n".to_owned(), "needs the bot token"),
            ("name = \"bob\"\n[[notify]]\nkind = \"email\"\n".to_owned(), "unknown variant"),
        ] {
            assert!(error(&text).contains(why), "{text}: {}", error(&text));
        }
        let leaked = error("name = \"bob\"\n[[notify]]\nkind = \"slack\"\nsecret_env = \"BOT_TOKEN\"\n");
        assert!(!leaked.contains("123:abc"), "{leaked}");
    }

    fn links() -> Links {
        Links::new("https://hub.example/hooks/", "0123456789abcdef-secret").unwrap()
    }

    fn action() -> Action {
        Action {
            ant: p("dev-a"),
            id: Uuid::now_v7(),
            seq: 7,
            verdict: Verdict::AllowOnce,
            by: p("alice"),
            expires: Utc::now().timestamp() + 60,
        }
    }

    #[test]
    fn a_link_holds_only_as_signed_and_until_its_request_expires() {
        let (links, action) = (links(), action());
        let url = links.url(&action);
        let query = url.strip_prefix("https://hub.example/hooks/_approval?").expect(&url);
        assert_eq!(links.verify(query, Utc::now()), Ok(action.clone()));

        let later = DateTime::from_timestamp(action.expires, 0).unwrap();
        assert_eq!(links.verify(query, later), Err(LinkError::Expired));
        let other = Links::new("https://hub.example/hooks", "another-secret-of-length").unwrap();
        assert_eq!(other.verify(query, Utc::now()), Err(LinkError::Forged));
        for (from, to) in [
            ("verdict=allow_once", "verdict=allow_always"),
            ("by=alice", "by=bob"),
            ("seq=7", "seq=8"),
            ("ant=dev-a", "ant=dev-b"),
            (&format!("exp={}", action.expires), &format!("exp={}", action.expires + 3600)),
        ] {
            assert_eq!(links.verify(&query.replace(from, to), Utc::now()), Err(LinkError::Forged), "{to}");
        }
        let sig = query.rsplit_once("sig=").unwrap().1;
        assert_eq!(links.verify(&query.replace(sig, &sig.to_uppercase()), Utc::now()), Ok(action));
        for broken in [
            "",
            &query.replace("verdict=allow_once", "verdict=maybe"),
            &query.replace("&seq=7", ""),
            &format!("{query}&seq=7"),
            &format!("{query}&x=1"),
            &query.replace("sig=", "sig"),
        ] {
            assert_eq!(links.verify(broken, Utc::now()), Err(LinkError::Malformed), "{broken}");
        }
        assert!(Links::new("https://hub.example", "short").is_err());
        assert!(Links::new("hub.example", "0123456789abcdef-secret").is_err());
    }

    fn request() -> ApprovalRequest {
        let tool = ToolCall {
            kind: "execute".to_owned(),
            title: "rm -rf <build>".to_owned(),
            locations: vec!["src/a.rs".to_owned()],
            input: Some("{\"command\":\"rm -rf build\"}".to_owned()),
        };
        let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
        ApprovalRequest::new(p("dev-a"), ThreadId::generate(), tool, options, Duration::from_secs(600))
    }

    #[test]
    fn a_notice_carries_a_button_per_verdict_offered_in_each_sink_s_form() {
        let (request, links) = (request(), links());
        let notice = approval_notice(&request, 9, &p("alice"), Some(&links), None, Utc::now());
        assert_eq!(notice.title, "dev-a asks to run a tool");
        assert!(
            notice.text.contains("rm -rf <build> (execute)\nfiles: src/a.rs\ninput: {\"command\""),
            "{}",
            notice.text
        );
        assert!(notice.text.contains(&format!("ants tools approve dev-a/{}", request.id)), "{}", notice.text);
        let verdicts: Vec<_> = notice.buttons.iter().map(|b| (b.label, b.verdict)).collect();
        assert_eq!(
            verdicts,
            [("Approve", Verdict::AllowOnce), ("Always allow", Verdict::AllowAlways), ("Reject", Verdict::Reject)]
        );
        for button in &notice.buttons {
            let query = button.url.split_once('?').unwrap().1;
            let action = links.verify(query, Utc::now()).unwrap();
            assert_eq!((action.seq, action.verdict, &action.by), (9, button.verdict, &p("alice")));
        }
        assert!(approval_notice(&request, 9, &p("alice"), None, None, Utc::now()).buttons.is_empty(), "no links");
        assert!(!notice.text.contains("--rule"), "{}", notice.text);
        let suggested = tools::Suggestion { rule: "execute:rm -rf build".to_owned(), approved_before: 2 };
        let suggesting = approval_notice(&request, 9, &p("alice"), None, Some(&suggested), Utc::now());
        assert!(
            suggesting.text.ends_with(&format!(
                "\nallowed 2 times before: `ants tools approve dev-a/{} --rule` also adds the allow rule \
                 \"execute:rm -rf build\" to dev-a",
                request.id
            )),
            "{}",
            suggesting.text
        );

        let ntfy = Sink::Ntfy { url: NTFY.to_owned(), topic: "t".to_owned(), token: Some("tk".to_owned()) };
        let (url, bearer, body) = ntfy.request(&notice);
        assert_eq!((url.as_str(), bearer), (NTFY, Some("tk")));
        assert_eq!((body["topic"].as_str(), body["priority"].as_i64()), (Some("t"), Some(4)));
        assert_eq!(
            body["actions"][2],
            json!({
                "action": "http", "label": "Reject", "url": notice.buttons[2].url, "method": "POST", "clear": true
            })
        );

        let slack = Sink::Slack { url: "https://hooks.slack.com/s".to_owned() };
        let (url, bearer, body) = slack.request(&notice);
        assert_eq!((url.as_str(), bearer), ("https://hooks.slack.com/s", None));
        let section = body["blocks"][0]["text"]["text"].as_str().unwrap();
        assert!(section.starts_with("*dev-a asks to run a tool*\nrm -rf &lt;build&gt;"), "{section}");
        let buttons = &body["blocks"][1]["elements"];
        assert_eq!(
            (buttons[0]["url"].as_str(), buttons[0]["style"].as_str()),
            (Some(&*notice.buttons[0].url), Some("primary"))
        );
        assert_eq!(buttons[2]["style"], "danger");

        let telegram = Sink::Telegram { api: TELEGRAM.to_owned(), token: "1:x".to_owned(), chat: "42".to_owned() };
        let (url, _, body) = telegram.request(&notice);
        assert_eq!(url, "https://api.telegram.org/bot1:x/sendMessage");
        assert_eq!((body["chat_id"].as_str(), body["disable_web_page_preview"].as_bool()), (Some("42"), Some(true)));
        assert_eq!(
            body["reply_markup"]["inline_keyboard"][0][1],
            json!({ "text": "Always allow", "url": notice.buttons[1].url })
        );
        assert!(body["text"].as_str().unwrap().starts_with("dev-a asks to run a tool\n\nrm -rf <build>"));

        let plain = Envelope::new(p("dev-a"), p("alice"), "hello".to_owned());
        assert_eq!(escalation_notice(&plain), None);
        let escalation = Escalation::new(p("dev-a"), plain.thread_id.clone(), ants_proto::EscalationReason::TurnBudget);
        let escalated = plain.with_escalation(Some(escalation));
        let notice = escalation_notice(&escalated).unwrap();
        assert_eq!(notice.title, "dev-a escalates: turn_budget on dev-a");
        assert!(notice.text.starts_with("hello\n\nthread ") && notice.text.ends_with("@dev-a TEXT"), "{}", notice.text);
        let (_, _, body) = telegram.request(&notice);
        assert!(body.get("reply_markup").is_none());

        // An expired tool request's notice, on a thread of its own, is answered on the request's thread.
        let expired = Envelope::new(p("dev-a"), p("alice"), "[ants] Tool request expired".to_owned())
            .with_escalation(Some(Escalation::of_request(&request)));
        let notice = escalation_notice(&expired).unwrap();
        assert_eq!(notice.title, "dev-a's tool request expired unanswered");
        let answer = format!("thread {0}; answer with ants ask --thread {0} @dev-a TEXT", request.thread_id);
        assert!(notice.text.ends_with(&answer), "{}", notice.text);
    }

    #[test]
    fn texts_are_cut_and_escaped() {
        assert_eq!(cut("héllo", 3), "hél…");
        assert_eq!(cut("hé", 3), "hé");
        assert_eq!(html("<a href=\"x\">&'"), "&lt;a href=&quot;x&quot;&gt;&amp;&#39;");
        let old = Uuid::from_u128((1_000_000_000_u128 << 80) | (7 << 76) | (2 << 62));
        assert!(!is_recent(&old, Utc::now()) && is_recent(&Uuid::now_v7(), Utc::now()));
    }
}
