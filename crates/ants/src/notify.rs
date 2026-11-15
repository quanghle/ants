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
//! Every sink kind takes the same [`Notice`] through the same path, [`dispatch`]: [`Sink::request`] puts it in the
//! kind's form, a [`Delivery`] (a JSON `POST` for ntfy, Slack, Telegram and `http`, a command for `exec`), and
//! [`send_to`] delivers it within the sink's timeout, with errors that never name a URL, token or argument. An `exec`
//! sink gets the notice as JSON on its standard input, an `http` one the same JSON in a `POST` signed with
//! HMAC-SHA256 ([`notice_json`]).
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
use ants_daemon::direct_kv::{DirectKv, Lookup};
use ants_daemon::redact::Redactor;
use ants_proto::{
    ApprovalRequest, DeviceId, Envelope, Escalation, EscalationReason, Following, JsDomain, NOTIFY_BUCKET, NotifyPrefs,
    STREAM, ThreadId, UserId, Uuid, Verdict, notify_consumer, subject,
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
use jiff::tz::TimeZone;
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
/// How long one sink may take to take a notice, unless its `timeout` says otherwise.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest `timeout` of a sink, well within [`ACK_WAIT`].
const MAX_SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// The version of the JSON notice exec and http sinks get ([`notice_json`]).
const NOTICE_VERSION: u32 = 1;
/// Shortest signing secret of an http sink.
const MIN_SECRET: usize = 16;
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
    /// Whether an ant's reply reaching the human's inbox is notified.
    #[serde(default)]
    completions: bool,
    /// The daily window notices are held back in, such as `22:00-07:00`.
    quiet: Option<String>,
    /// The time zone `quiet` reads in, instead of the file's.
    timezone: Option<String>,
    /// What quiet hours and a snooze let through.
    #[serde(default)]
    bypass: Vec<Bypass>,
    #[serde(default)]
    notify: Vec<SinkEntry>,
}

/// What kind of notice quiet hours and a snooze let through, as a human's `bypass` lists them.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Bypass {
    Approvals,
    Escalations,
}

/// One `[[human.notify]]`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SinkEntry {
    kind: SinkKind,
    url: Option<String>,
    topic: Option<String>,
    chat: Option<String>,
    /// The program of an exec sink and its arguments, run without a shell.
    command: Option<Vec<String>>,
    secret_env: Option<String>,
    secret_file: Option<PathBuf>,
    /// The device of the human whose push target the sink is: none while that device follows what a notice is about.
    device: Option<DeviceId>,
    /// How long the sink may take, such as `20s`.
    timeout: Option<String>,
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum SinkKind {
    Ntfy,
    Slack,
    Telegram,
    Exec,
    Http,
}

impl HumanEntry {
    /// The secrets of its sinks: what each is for, its `secret_env` and its `secret_file`.
    pub fn secrets(&self) -> impl Iterator<Item = (String, Option<&str>, Option<&Path>)> {
        self.notify.iter().enumerate().map(|(i, sink)| {
            let what = format!("human {} notify #{}", self.name, i + 1);
            (what, sink.secret_env.as_deref(), sink.secret_file.as_deref())
        })
    }

    /// The programs of its exec sinks, as written: what each is for and the program.
    pub fn commands(&self) -> impl Iterator<Item = (String, &str)> {
        self.notify.iter().enumerate().filter_map(|(i, sink)| {
            let program = sink.command.as_ref()?.first()?;
            Some((format!("human {} notify #{}", self.name, i + 1), program.as_str()))
        })
    }
}

/// A human to notify, checked.
#[derive(PartialEq, Eq)]
pub struct Human {
    pub name: UserId,
    pub creds: Option<PathBuf>,
    /// The ants whose tool requests are notified: `None` for none, an empty list for every ant.
    approvals: Option<Vec<UserId>>,
    escalations: bool,
    completions: bool,
    quiet: Option<QuietHours>,
    bypass: Vec<Bypass>,
    sinks: Vec<Target>,
}

/// A daily window in a time zone, from `start` to `end` (minutes after midnight, `end` excluded), in which notices are
/// held back; one ending before it starts crosses midnight.
#[derive(Debug, PartialEq, Eq)]
struct QuietHours {
    start: u16,
    end: u16,
    zone: TimeZone,
    /// As written, for display.
    text: String,
}

impl QuietHours {
    /// `text`, `HH:MM-HH:MM`, read in `zone`.
    fn parse(text: &str, zone: TimeZone) -> Result<Self> {
        let minutes = |t: &str| -> Option<u16> {
            let (h, m) = t.trim().split_once(':')?;
            let (h, m): (u16, u16) = (h.parse().ok()?, m.parse().ok()?);
            (h < 24 && m < 60 && t.trim().len() == 5).then_some(h * 60 + m)
        };
        let window = text.split_once('-').and_then(|(start, end)| Some((minutes(start)?, minutes(end)?)));
        let Some((start, end)) = window else { bail!("quiet {text:?}: expected HH:MM-HH:MM, such as 22:00-07:00") };
        if start == end {
            bail!("quiet {text:?}: the window starts where it ends");
        }
        Ok(Self { start, end, zone, text: text.trim().to_owned() })
    }

    /// Whether `at` falls in the window, by the wall clock of its time zone.
    fn contains(&self, at: DateTime<Utc>) -> bool {
        let Ok(at) = jiff::Timestamp::from_second(at.timestamp()) else { return false };
        let time = self.zone.to_datetime(at);
        let now = u16::try_from(time.hour()).unwrap_or(0) * 60 + u16::try_from(time.minute()).unwrap_or(0);
        match self.start < self.end {
            true => self.start <= now && now < self.end,
            false => now >= self.start || now < self.end,
        }
    }
}

impl std::fmt::Display for QuietHours {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.text, self.zone.iana_name().unwrap_or("UTC"))
    }
}

/// A sink, and the device of the human it is the push target of, if any. A sink of no device gets every notice.
#[derive(PartialEq, Eq)]
struct Target {
    device: Option<DeviceId>,
    sink: Sink,
    /// How long the sink may take to take a notice.
    timeout: Duration,
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
    /// A program, with its arguments, run with the notice's JSON on its standard input.
    Exec { command: Vec<String> },
    /// The notice's JSON `POST`ed to `url`, signed with `secret`.
    Http { url: String, secret: String },
}

impl std::fmt::Debug for Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label())
    }
}

const NTFY: &str = "https://ntfy.sh";
const TELEGRAM: &str = "https://api.telegram.org";

impl Human {
    /// Check `entry`, reading its quiet hours in its own time zone, else `timezone` (the file's), else UTC.
    pub fn from_entry(
        entry: HumanEntry,
        timezone: Option<&TimeZone>,
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
        if approvals.is_none() && !entry.escalations && !entry.completions {
            bail!("approvals, escalations and completions are all off: nothing to notify");
        }
        let quiet = match (entry.quiet, entry.timezone) {
            (None, Some(_)) => bail!("timezone goes with quiet only"),
            (None, None) => None,
            (Some(quiet), zone) => {
                let zone = match zone {
                    Some(name) => ants_daemon::schedule::time_zone(&name)?,
                    None => timezone.cloned().unwrap_or(TimeZone::UTC),
                };
                Some(QuietHours::parse(&quiet, zone)?)
            }
        };
        let sinks = entry
            .notify
            .into_iter()
            .enumerate()
            .map(|(i, mut sink)| {
                let device = sink.device;
                let timeout = sink_timeout(sink.timeout.take()).with_context(|| format!("notify #{}", i + 1))?;
                let sink = Sink::from_entry(sink, resolve, env).with_context(|| format!("notify #{}", i + 1))?;
                Ok(Target { device, sink, timeout })
            })
            .collect::<Result<Vec<_>>>()?;
        let creds = entry.creds.as_deref().map(resolve);
        Ok(Self {
            name: entry.name,
            creds,
            approvals,
            escalations: entry.escalations,
            completions: entry.completions,
            quiet,
            bypass: entry.bypass,
            sinks,
        })
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

    /// What the human's quiet hours are and whether they hold at `now`, for display; None without them.
    pub fn quiet_hours(&self, now: DateTime<Utc>) -> Option<String> {
        let quiet = self.quiet.as_ref()?;
        let state = if quiet.contains(now) { "on now" } else { "off now" };
        let through: Vec<&str> = self
            .bypass
            .iter()
            .map(|b| match b {
                Bypass::Approvals => "approvals",
                Bypass::Escalations => "escalations",
            })
            .collect();
        let through = match through.is_empty() {
            true => "nothing".to_owned(),
            false => through.join(" and "),
        };
        Some(format!("quiet hours {quiet} ({state}); quiet hours and a snooze let {through} through"))
    }

    /// The consumer's subjects: tool requests and the human's inbox, as configured.
    fn subjects(&self) -> Vec<String> {
        let approvals = self.approvals.is_some().then(|| subject::ALL_APPROVAL_REQUESTS.to_owned());
        let escalations = (self.escalations || self.completions).then(|| subject::inbox_of(&self.name));
        approvals.into_iter().chain(escalations).collect()
    }

    fn wants_request_of(&self, ant: &UserId) -> bool {
        self.approvals.as_ref().is_some_and(|ants| ants.is_empty() || ants.contains(ant))
    }
}

/// The program of an exec sink as it is run: a path, one with a separator or starting with `~`, resolved as the
/// config file's paths are; a bare name left to be found on `PATH`.
pub fn resolve_program(program: &str, resolve: &impl Fn(&Path) -> PathBuf) -> String {
    match program.starts_with('~') || program.contains(std::path::is_separator) {
        true => resolve(Path::new(program)).to_string_lossy().into_owned(),
        false => program.to_owned(),
    }
}

/// A sink's `timeout`, [`SEND_TIMEOUT`] unless given.
fn sink_timeout(text: Option<String>) -> Result<Duration> {
    let Some(text) = text else { return Ok(SEND_TIMEOUT) };
    let timeout = ants_proto::duration::parse(&text).map_err(|e| anyhow!(e))?;
    if timeout.is_zero() || timeout > MAX_SEND_TIMEOUT {
        bail!("timeout {text:?}: from 1s to {}", ants_proto::duration::format(MAX_SEND_TIMEOUT));
    }
    Ok(timeout)
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
        if entry.kind != SinkKind::Exec {
            only("command", entry.command.is_some())?;
        }
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
            SinkKind::Exec => {
                only("url", entry.url.is_some())?;
                only("topic", entry.topic.is_some())?;
                only("chat", entry.chat.is_some())?;
                if secret.is_some() {
                    bail!(
                        "secret_file and secret_env do not go with kind = \"exec\": the command reads its own secrets"
                    );
                }
                let mut command = entry.command.context("exec needs command, the program and its arguments")?;
                let Some(program) = command.first_mut().filter(|p| !p.is_empty()) else {
                    bail!("command: give the program first, then its arguments");
                };
                *program = resolve_program(program, resolve);
                Ok(Self::Exec { command })
            }
            SinkKind::Http => {
                only("topic", entry.topic.is_some())?;
                only("chat", entry.chat.is_some())?;
                let url = entry.url.context("http needs url, where to POST notices")?;
                http_url(&url, "url")?;
                let secret = secret.context("http needs the signing secret in secret_file or secret_env")?;
                if secret.len() < MIN_SECRET {
                    bail!("the signing secret is too short: give at least {MIN_SECRET} random characters");
                }
                Ok(Self::Http { url, secret })
            }
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Ntfy { topic, .. } => format!("ntfy topic {topic}"),
            Self::Slack { .. } => "slack".to_owned(),
            Self::Telegram { chat, .. } => format!("telegram chat {chat}"),
            Self::Exec { command } => format!("exec {}", command[0]),
            Self::Http { url, .. } => {
                let host = url.parse::<Uri>().ok().and_then(|uri| uri.host().map(str::to_owned));
                format!("http {}", host.unwrap_or_default())
            }
        }
    }

    /// `notice` in the sink's form, and how it goes there.
    fn request(&self, notice: &Notice) -> Delivery {
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
                Delivery::post(url, token.as_deref(), &body)
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
                Delivery::post(url, None, &json!({ "text": slack_escape(&notice.title), "blocks": blocks }))
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
                Delivery::post(&format!("{api}/bot{token}/sendMessage"), None, &body)
            }
            Self::Exec { command } => {
                let input = format!("{}\n", notice_json(notice, Utc::now())).into_bytes();
                Delivery::Run { command: command.clone(), input }
            }
            Self::Http { url, secret } => {
                let now = Utc::now();
                let body = notice_json(notice, now).to_string().into_bytes();
                let timestamp = now.timestamp().to_string();
                let signature = signature(secret, &timestamp, &body);
                let headers = vec![(TIMESTAMP_HEADER, timestamp), (SIGNATURE_HEADER, signature)];
                Delivery::Post { url: url.clone(), headers, body }
            }
        }
    }
}

/// The header of an http sink's `POST` with its Unix time in seconds, part of what is signed.
const TIMESTAMP_HEADER: &str = "x-ants-timestamp";
/// The header of an http sink's `POST` with its signature, `sha256=` and the hex HMAC-SHA256 of [`signature`].
const SIGNATURE_HEADER: &str = "x-ants-signature";

/// The signature an http sink's `POST` of `body` at `timestamp` carries: `sha256=` and the lowercase hex HMAC-SHA256,
/// keyed with `secret`, of the timestamp's decimal digits, a `.` and the body.
fn signature(secret: &str, timestamp: &str, body: &[u8]) -> String {
    let mut context = hmac::Context::with_key(&hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()));
    context.update(timestamp.as_bytes());
    context.update(b".");
    context.update(body);
    format!("sha256={}", HEXLOWER.encode(context.sign().as_ref()))
}

/// How a notice goes to a sink. Holds the sink's secrets, so it has no `Debug`.
enum Delivery {
    /// `body`, JSON, `POST`ed to `url` with `headers`.
    Post { url: String, headers: Vec<(&'static str, String)>, body: Vec<u8> },
    /// `command` run with `input` on its standard input.
    Run { command: Vec<String>, input: Vec<u8> },
}

impl Delivery {
    fn post(url: &str, bearer: Option<&str>, body: &Value) -> Self {
        let headers = bearer.map(|token| ("authorization", format!("Bearer {token}")));
        Self::Post { url: url.to_owned(), headers: headers.into_iter().collect(), body: body.to_string().into_bytes() }
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
#[derive(PartialEq, Eq)]
pub struct Links {
    base: String,
    secret: String,
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
        Ok(Self { base, secret: secret.to_owned() })
    }

    fn key(&self) -> hmac::Key {
        hmac::Key::new(hmac::HMAC_SHA256, self.secret.as_bytes())
    }

    fn url(&self, action: &Action) -> String {
        let sig = HEXLOWER.encode(hmac::sign(&self.key(), action.signed().as_bytes()).as_ref());
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
        hmac::verify(&self.key(), action.signed().as_bytes(), &sig).map_err(|_| LinkError::Forged)?;
        if now.timestamp() >= action.expires {
            return Err(LinkError::Expired);
        }
        Ok(action)
    }
}

// ---- notices ---------------------------------------------------------------------------------------------------

/// What a sink sends, whatever its kind: each kind's form of it is [`Sink::request`]'s.
#[derive(Debug, PartialEq, Eq)]
struct Notice {
    about: About,
    /// The human notified.
    human: UserId,
    title: String,
    text: String,
    buttons: Vec<Button>,
    urgent: bool,
}

/// What a notice is about.
#[derive(Debug, PartialEq, Eq)]
enum About {
    /// A pending tool request, at stream sequence `seq`.
    Approval { request: Box<ApprovalRequest>, seq: u64 },
    /// An escalation reaching the human's inbox, in the message `id` from `from`.
    Escalation { id: Uuid, from: UserId, escalation: Box<Escalation> },
    /// An ant's reply `id` from `from` on `thread`, reaching the human's inbox.
    Completion { id: Uuid, from: UserId, thread: ThreadId },
    /// A test notice of `ants webhook notify-test`.
    Test { id: Uuid },
}

impl About {
    /// The thread the notice is about, which muting it holds back; none for a test.
    fn thread(&self) -> Option<&ThreadId> {
        match self {
            Self::Approval { request, .. } => Some(&request.thread_id),
            Self::Escalation { escalation, .. } => Some(&escalation.thread_id),
            Self::Completion { thread, .. } => Some(thread),
            Self::Test { .. } => None,
        }
    }

    /// The `bypass` entry letting it through quiet hours and a snooze, if one may.
    fn bypass(&self) -> Option<Bypass> {
        match self {
            Self::Approval { .. } => Some(Bypass::Approvals),
            Self::Escalation { .. } => Some(Bypass::Escalations),
            Self::Completion { .. } | Self::Test { .. } => None,
        }
    }
}

/// Why `about` is held back from `human` at `now`, given the holds `prefs` the human set, if it is: a muted thread
/// holds back every notice of it; quiet hours and a snooze each one the human's `bypass` does not let through.
fn held(human: &Human, about: &About, prefs: &NotifyPrefs, now: DateTime<Utc>) -> Option<&'static str> {
    if about.thread().is_some_and(|thread| prefs.is_muted(thread, now)) {
        return Some("muted thread");
    }
    if about.bypass().is_some_and(|kind| human.bypass.contains(&kind)) {
        return None;
    }
    if prefs.snoozed(now) {
        return Some("snoozed");
    }
    human.quiet.as_ref().is_some_and(|quiet| quiet.contains(now)).then_some("quiet hours")
}

/// `notice` as exec and http sinks get it, at `now`: version [`NOTICE_VERSION`] of the schema the README documents.
fn notice_json(notice: &Notice, now: DateTime<Utc>) -> Value {
    let links: Vec<Value> = notice
        .buttons
        .iter()
        .map(|b| json!({ "verdict": b.verdict.as_str(), "label": b.label, "url": b.url }))
        .collect();
    let mut json = json!({
        "version": NOTICE_VERSION,
        "human": notice.human,
        "title": notice.title,
        "text": notice.text,
        "urgent": notice.urgent,
        "sent_at": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "links": links,
    });
    let facts = match &notice.about {
        About::Approval { request, seq } => json!({
            "kind": "approval",
            "id": request.id,
            "ant": request.ant,
            "thread": request.thread_id,
            "request": tools::qualified(request),
            "seq": seq,
            "tool": request.tool,
            "options": request.options.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
            "expires_at": request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }),
        About::Escalation { id, from, escalation } => json!({
            "kind": "escalation",
            "id": id,
            "ant": from,
            "thread": escalation.thread_id,
            "reason": escalation.reason.as_str(),
            "stuck": escalation.stuck,
            "level": escalation.level,
        }),
        About::Completion { id, from, thread } => json!({
            "kind": "completion",
            "id": id,
            "ant": from,
            "thread": thread,
        }),
        About::Test { id } => json!({ "kind": "test", "id": id }),
    };
    if let (Value::Object(json), Value::Object(facts)) = (&mut json, facts) {
        json.extend(facts);
    }
    json
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

/// Redacts what notices carry, whatever the ant's `redact` setting: ntfy topics, chats and hooks are seen by more than
/// the bus is.
fn redactor() -> &'static Redactor {
    static REDACTOR: OnceLock<Redactor> = OnceLock::new();
    REDACTOR.get_or_init(|| Redactor::default().with_process_env())
}

/// `request` with its tool call redacted.
fn redacted(request: &ApprovalRequest) -> ApprovalRequest {
    let mut request = request.clone();
    redactor().redact_tool(&mut request.tool);
    request
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
    let request = &redacted(request);
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
    Notice {
        about: About::Approval { request: Box::new(request.clone()), seq },
        human: human.clone(),
        title: format!("{} asks to run a tool", request.ant),
        text,
        buttons,
        urgent: true,
    }
}

/// The notice of an escalation `envelope` brings, if it brings one. The thread to answer on is the escalation's: an
/// expired tool request's notice comes on a thread of its own.
fn escalation_notice(envelope: &Envelope) -> Option<Notice> {
    let escalation = envelope.escalation.as_ref()?;
    let thread = &escalation.thread_id;
    let text = format!(
        "{}\n\nthread {thread}; answer with ants ask --thread {thread} @{} TEXT",
        cut(&redactor().redact(&envelope.text), MAX_TEXT),
        envelope.from,
    );
    let title = match escalation.reason {
        EscalationReason::Approval => format!("{}'s tool request expired unanswered", escalation.stuck),
        reason => format!("{} escalates: {} on {}", envelope.from, reason.as_str(), escalation.stuck),
    };
    Some(Notice {
        about: About::Escalation { id: envelope.id, from: envelope.from.clone(), escalation: escalation.clone() },
        human: envelope.to.clone(),
        title,
        text,
        buttons: Vec::new(),
        urgent: false,
    })
}

/// The notice of an ant's reply `envelope`, if it is one: a message answering another (`in_reply_to`) that carries no
/// escalation, decision or mention and comes from no webhook.
fn completion_notice(envelope: &Envelope) -> Option<Notice> {
    let reply = envelope.in_reply_to.is_some()
        && envelope.escalation.is_none()
        && envelope.approval.is_none()
        && envelope.mention.is_none()
        && !envelope.webhook
        && envelope.from != envelope.to;
    if !reply {
        return None;
    }
    let thread = &envelope.thread_id;
    let text = format!(
        "{}\n\nthread {thread}; answer with ants ask --thread {thread} @{} TEXT",
        cut(&redactor().redact(&envelope.text), MAX_TEXT),
        envelope.from,
    );
    Some(Notice {
        about: About::Completion { id: envelope.id, from: envelope.from.clone(), thread: thread.clone() },
        human: envelope.to.clone(),
        title: format!("{} replied", envelope.from),
        text,
        buttons: Vec::new(),
        urgent: false,
    })
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

/// `method` on `url`, with `headers` and `body` as JSON if any: the status of the answer and the start of its body.
/// Errors never name the URL, which may hold a secret.
async fn fetch(
    method: Method,
    url: &str,
    headers: &[(&str, String)],
    body: Option<Vec<u8>>,
) -> Result<(StatusCode, String)> {
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
    let mut request =
        Request::builder().method(method).uri(path).header(header::HOST, authority).header(header::USER_AGENT, "ants");
    if body.is_some() {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    for (name, value) in headers {
        request = request.header(*name, value);
    }
    let response =
        send.send_request(request.body(Full::new(Bytes::from(body.unwrap_or_default())))?).await.context("sending")?;
    let status = response.status();
    let answer = Limited::new(response.into_body(), MAX_ANSWER * 8).collect().await;
    let answer = answer.map(|b| b.to_bytes()).unwrap_or_default();
    Ok((status, String::from_utf8_lossy(&answer[..answer.len().min(MAX_ANSWER)]).into_owned()))
}

/// `POST` `body`, JSON, to `url`. Errors never name the URL, which may hold a secret.
async fn post_json(url: &str, headers: &[(&str, String)], body: Vec<u8>) -> Result<()> {
    let (status, answer) = fetch(Method::POST, url, headers, Some(body)).await?;
    if status.is_success() {
        return Ok(());
    }
    bail!("answered {status}: {}", answer.trim())
}

/// Run `command` with `input` on its standard input, its output dropped and its errors on those of the process: an
/// error unless it exits with status 0 within `wait`. Past `wait` it is killed and reaped.
async fn run_command(command: &[String], input: Vec<u8>, wait: Duration) -> Result<()> {
    let (program, args) = command.split_first().context("no program")?;
    let mut child = tokio::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("starting the command")?;
    let mut stdin = child.stdin.take().context("no standard input")?;
    // A command that does not read its input may exit before taking it all.
    let feed = async move {
        let _ = tokio::io::AsyncWriteExt::write_all(&mut stdin, &input).await;
    };
    let ran = tokio::time::timeout(wait, async { tokio::join!(feed, child.wait()).1 }).await;
    match ran {
        Ok(status) => {
            let status = status.context("waiting for the command")?;
            if !status.success() {
                bail!("{status}");
            }
            Ok(())
        }
        Err(_) => {
            let _ = child.kill().await;
            bail!("no exit within {}s: killed", wait.as_secs())
        }
    }
}

/// `notice` sent to the sink of `target` within its timeout.
async fn send_to(target: &Target, notice: &Notice) -> Result<()> {
    let wait = target.timeout;
    match target.sink.request(notice) {
        Delivery::Post { url, headers, body } => {
            match tokio::time::timeout(wait, post_json(&url, &headers, body)).await {
                Ok(sent) => sent,
                Err(_) => bail!("no answer within {}s", wait.as_secs()),
            }
        }
        Delivery::Run { command, input } => run_command(&command, input, wait).await,
    }
}

/// Send a test notice to every sink of `human`, those of a device too, at once: each sink's label and how it went.
pub async fn test(human: &Human, machine: &str) -> Vec<(String, Result<()>)> {
    let notice = Notice {
        about: About::Test { id: Uuid::now_v7() },
        human: human.name.clone(),
        title: format!("ants test notice for {}", human.name),
        text: format!(
            "Sent by `ants webhook notify-test {}` on {machine}: tool requests and escalations reach this sink.",
            human.name
        ),
        buttons: Vec::new(),
        urgent: false,
    };
    let sent = dispatch(human, &notice, |_| true).await;
    sent.into_iter().map(|(target, sent)| (format!("{target:?}"), sent)).collect()
}

/// What `GET` on the approval links of `public_url` without a link's query answers, within `wait`: the status and the
/// start of the body.
pub async fn probe_links(public_url: &str, wait: Duration) -> Result<(StatusCode, String)> {
    let url = format!("{}{APPROVAL_PATH}", http_url(public_url, "public_url")?);
    match tokio::time::timeout(wait, fetch(Method::GET, &url, &[], None)).await {
        Ok(answer) => answer,
        Err(_) => bail!("no answer within {}s", wait.as_secs()),
    }
}

/// Send `notice` to every sink of `human` at once, but those of the devices `away` leaves out; a failure is logged,
/// never fatal.
async fn send(human: &Human, notice: &Notice, away: impl Fn(&DeviceId) -> bool) {
    for (target, sent) in dispatch(human, notice, |t| t.device.as_ref().is_none_or(&away)).await {
        match sent {
            Ok(()) => tracing::debug!(human = %human.name, sink = target.sink.label(), "notified"),
            Err(e) => tracing::warn!(human = %human.name, sink = target.sink.label(), "notifying: {e:#}"),
        }
    }
}

/// Send `notice` to the sinks of `human` that `wanted` keeps, all at once, each in its kind's form
/// ([`Sink::request`]) and within its timeout: each sink and how it went, a failure stopping no other.
async fn dispatch<'a>(
    human: &'a Human,
    notice: &Notice,
    wanted: impl Fn(&Target) -> bool,
) -> Vec<(&'a Target, Result<()>)> {
    let sends =
        human.sinks.iter().filter(|t| wanted(t)).map(|target| async move { (target, send_to(target, notice).await) });
    futures::future::join_all(sends).await
}

// ---- watching --------------------------------------------------------------------------------------------------

/// A human's connection to the hub.
struct HumanBus {
    client: async_nats::Client,
    js: jetstream::Context,
    stream: Stream,
    present: Present,
    /// The human's holds ([`NotifyPrefs`]); none when its credentials may not read them.
    prefs: Option<DirectKv>,
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
    let prefs = match crate::humans::Rights::from_creds(creds) {
        Ok(rights) if rights.may_publish(&subject::notify_prefs(name)) => {
            // Through a leaf node, a direct get of a missing bucket goes unanswered rather than finding none.
            if let Err(e) = crate::quiet::ensure_bucket(&js).await {
                tracing::warn!(human = %name, "{e:#}");
            }
            Some(DirectKv::new(&js, js_domain.as_ref(), NOTIFY_BUCKET).await?)
        }
        _ => {
            tracing::warn!(
                human = %name,
                "credentials from before snoozes and muted threads: every notice is sent; renew with `ants auth user \
                 {name} --human`"
            );
            None
        }
    };
    let _ = human.bus.set(HumanBus { client, js, stream, present, prefs });
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
    let inbox = subject.starts_with(subject::inbox_of(&config.name).trim_end_matches('*'));
    let notice = if inbox {
        match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) if envelope.check_subject(subject).is_ok() && is_recent(&envelope.id, now) => {
                match envelope.escalation.is_some() {
                    true if config.escalations => escalation_notice(&envelope),
                    false if config.completions => completion_notice(&envelope),
                    _ => None,
                }
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
    let prefs = holds(config, bus).await;
    if let Some(why) = held(config, &notice.about, &prefs, now) {
        // Dropped, not sent later: the message stays unread and a request pending, as for a notice never sent.
        tracing::info!(human = %config.name, why, title = notice.title, "held back a notice");
        return;
    }
    let what = if inbox { Following::Inbox } else { Following::Approvals };
    let reading = bus.present.following(&config.name, config.devices(), what).await;
    if !reading.is_empty() {
        tracing::debug!(human = %config.name, devices = ?reading, what = what.as_str(), "skipping devices reading");
    }
    send(config, &notice, |device| !reading.contains(device)).await;
}

/// The holds `human` set, read anew for each notice: none when they cannot be read, so a failure sends a notice too
/// many, never one too few.
async fn holds(human: &Human, bus: &HumanBus) -> NotifyPrefs {
    let Some(kv) = &bus.prefs else { return NotifyPrefs::default() };
    match kv.lookup(human.name.as_str()).await {
        Ok(Lookup::Value(value)) => serde_json::from_slice(&value).unwrap_or_else(|e| {
            tracing::warn!(human = %human.name, error = %e, "malformed snooze and muted threads: ignored");
            NotifyPrefs::default()
        }),
        Ok(Lookup::NoValue | Lookup::NoBucket) => NotifyPrefs::default(),
        Err(e) => {
            tracing::warn!(human = %human.name, error = format!("{e:#}"), "reading the snooze and muted threads");
            NotifyPrefs::default()
        }
    }
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
    let request = &redacted(request);
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
            "HOOK_SECRET" => Some("0123456789abcdef-hook".to_owned()),
            _ => None,
        };
        Human::from_entry(entry, None, &|p: &Path| Path::new("/etc/ants").join(p), &env)
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

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn quiet_hours_read_the_wall_clock_of_their_time_zone_across_midnight() {
        let night = human(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\n{NTFY_SINK}")).unwrap();
        let quiet = night.quiet.as_ref().unwrap();
        for (time, held) in [("21:59", false), ("22:00", true), ("23:59", true), ("00:00", true), ("06:59", true)] {
            assert_eq!(quiet.contains(at(&format!("2026-03-10T{time}:30Z"))), held, "{time} UTC");
        }
        assert!(!quiet.contains(at("2026-03-10T07:00:00Z")), "the end is not in the window");
        let day = QuietHours::parse("09:30-17:00", TimeZone::UTC).unwrap();
        assert!(day.contains(at("2026-03-10T09:30:00Z")) && !day.contains(at("2026-03-10T17:00:00Z")));
        assert!(!day.contains(at("2026-03-10T03:00:00Z")));

        // Tokyo is UTC+9 all year; New York moves to UTC-4 on 2026-03-08.
        let tokyo =
            human(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\ntimezone = \"Asia/Tokyo\"\n{NTFY_SINK}")).unwrap();
        let tokyo = tokyo.quiet.as_ref().unwrap();
        assert!(tokyo.contains(at("2026-03-10T14:00:00Z")), "23:00 in Tokyo");
        assert!(!tokyo.contains(at("2026-03-10T23:00:00Z")), "08:00 in Tokyo");
        let entry: HumanEntry =
            toml::from_str(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\n{NTFY_SINK}")).unwrap();
        let york = ants_daemon::schedule::time_zone("America/New_York").unwrap();
        let york = Human::from_entry(entry, Some(&york), &Path::to_path_buf, &|_: &str| None).unwrap();
        assert_eq!(
            york.quiet_hours(at("2026-03-07T03:30:00Z")).unwrap(),
            "quiet hours 22:00-07:00 America/New_York (on now); quiet hours and a snooze let nothing through",
            "22:30 EST, in the file's time zone"
        );
        assert!(!york.quiet.as_ref().unwrap().contains(at("2026-03-09T11:30:00Z")), "07:30 EDT");
        assert!(york.quiet.as_ref().unwrap().contains(at("2026-03-09T10:30:00Z")), "06:30 EDT");

        for (text, why) in [
            ("quiet = \"22:00\"", "expected HH:MM-HH:MM"),
            ("quiet = \"24:00-07:00\"", "expected HH:MM-HH:MM"),
            ("quiet = \"7:00-9:00\"", "expected HH:MM-HH:MM"),
            ("quiet = \"08:00-08:00\"", "starts where it ends"),
            ("timezone = \"Asia/Tokyo\"", "timezone goes with quiet only"),
            ("quiet = \"22:00-07:00\"\ntimezone = \"Mars/Base\"", "unknown time zone"),
            ("bypass = [\"completions\"]", "unknown variant"),
        ] {
            let text = format!("name = \"bob\"\n{text}\n{NTFY_SINK}");
            assert!(error(&text).contains(why), "{text}: {}", error(&text));
        }
        let only =
            human(&format!("name = \"bob\"\napprovals = false\nescalations = false\ncompletions = true\n{NTFY_SINK}"));
        assert_eq!(only.unwrap().subjects(), ["ants.inbox.bob.*"], "completions read the inbox");
        let none = format!("name = \"bob\"\napprovals = false\nescalations = false\n{NTFY_SINK}");
        assert!(error(&none).contains("nothing to notify"), "{}", error(&none));
    }

    #[test]
    fn a_mute_holds_back_everything_of_its_thread_and_a_snooze_or_quiet_hours_what_bypass_does_not_list() {
        let now = at("2026-03-10T12:00:00Z");
        let bob = human(&format!("name = \"bob\"\nbypass = [\"approvals\"]\n{NTFY_SINK}")).unwrap();
        let asked = request();
        let approval = About::Approval { request: Box::new(asked.clone()), seq: 1 };
        let other = ThreadId::generate();
        let escalation = About::Escalation {
            id: Uuid::now_v7(),
            from: p("dev-a"),
            escalation: Box::new(Escalation::new(p("dev-a"), other.clone(), ants_proto::EscalationReason::Asked)),
        };
        let completion = About::Completion { id: Uuid::now_v7(), from: p("dev-a"), thread: other };
        let mut prefs = NotifyPrefs::default();
        for about in [&approval, &escalation, &completion] {
            assert_eq!(held(&bob, about, &prefs, now), None, "nothing held");
        }

        prefs.snoozed_until = Some(now + TimeDelta::minutes(5));
        assert_eq!(held(&bob, &approval, &prefs, now), None, "approvals bypass");
        assert_eq!(held(&bob, &escalation, &prefs, now), Some("snoozed"));
        assert_eq!(held(&bob, &completion, &prefs, now), Some("snoozed"));
        let later = now + TimeDelta::minutes(5);
        assert_eq!(held(&bob, &completion, &prefs, later), None, "the snooze ended on its own");
        prefs.prune(later);
        assert_eq!(prefs, NotifyPrefs::default());

        prefs.muted.insert(asked.thread_id, now + TimeDelta::hours(1));
        assert_eq!(held(&bob, &approval, &prefs, now), Some("muted thread"), "whatever bypass says");
        assert_eq!(held(&bob, &completion, &prefs, now), None, "another thread");
        assert_eq!(held(&bob, &approval, &prefs, now + TimeDelta::hours(1)), None, "the mute ended");

        let night = human(&format!("name = \"bob\"\nquiet = \"11:00-13:00\"\nbypass = [\"escalations\"]\n{NTFY_SINK}"));
        let night = night.unwrap();
        let prefs = NotifyPrefs::default();
        assert_eq!(held(&night, &approval, &prefs, now), Some("quiet hours"));
        assert_eq!(held(&night, &escalation, &prefs, now), None, "escalations bypass");
        assert_eq!(held(&night, &completion, &prefs, now), Some("quiet hours"));
        assert_eq!(held(&night, &completion, &prefs, at("2026-03-10T13:00:00Z")), None);
    }

    #[test]
    fn a_reply_is_a_completion_and_nothing_else_is() {
        let (alice, dev_a) = (p("alice"), p("dev-a"));
        let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
        let done = asked.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
        let notice = completion_notice(&done).unwrap();
        assert_eq!((notice.title.as_str(), &notice.human, notice.urgent), ("dev-a replied", &alice, false));
        let thread = &asked.thread_id;
        assert_eq!(
            notice.text,
            format!("built\n\nthread {thread}; answer with ants ask --thread {thread} @dev-a TEXT")
        );
        let json = notice_json(&notice, at("2026-03-10T12:00:00Z"));
        assert_eq!(
            (&json["version"], &json["kind"], &json["id"], &json["ant"], &json["thread"]),
            (&json!(1), &json!("completion"), &json!(done.id), &json!("dev-a"), &json!(thread))
        );
        assert_eq!(json["links"], json!([]));

        let started = Envelope::new(dev_a.clone(), alice, "hello");
        assert!(completion_notice(&started).is_none(), "no reply");
        let escalated = done.clone().with_escalation(Some(Escalation::new(
            dev_a,
            thread.clone(),
            ants_proto::EscalationReason::Asked,
        )));
        assert!(completion_notice(&escalated).is_none(), "an escalation's notice is its own");
        let hooked = Envelope { webhook: true, ..done };
        assert!(completion_notice(&hooked).is_none());
    }

    /// Where a delivery `POST`s, its bearer token and its JSON body.
    fn posted(delivery: Delivery) -> (String, Option<String>, Value) {
        let Delivery::Post { url, headers, body } = delivery else { panic!("not a POST") };
        let bearer = headers.iter().find(|(name, _)| *name == "authorization");
        let bearer = bearer.map(|(_, value)| value.strip_prefix("Bearer ").unwrap().to_owned());
        (url, bearer, serde_json::from_slice(&body).unwrap())
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
    fn notices_are_redacted() {
        let secret = "ghp_0123456789abcdefghijABCDEFGHIJ";
        let mut request = request();
        request.tool.input = Some(format!("{{\"command\":\"git push https://x:{secret}@github.com/a/b\"}}"));
        let notice = approval_notice(&request, 9, &p("alice"), None, None, Utc::now());
        let json = notice_json(&notice, Utc::now());
        assert!(notice.text.contains("x:[redacted]@github.com"), "{}", notice.text);
        for shown in [notice.text, json.to_string(), details(&request)] {
            assert!(!shown.contains(secret), "{shown}");
        }

        let asked = Envelope::new(p("alice"), p("dev-a"), "build it");
        let done = asked.reply(p("dev-a"), format!("token {secret}"), ants_proto::Role::Human).unwrap();
        assert!(completion_notice(&done).unwrap().text.starts_with("token [redacted]\n"));
        let escalated = done.with_escalation(Some(Escalation::new(
            p("dev-a"),
            asked.thread_id,
            ants_proto::EscalationReason::Asked,
        )));
        assert!(escalation_notice(&escalated).unwrap().text.starts_with("token [redacted]\n"));
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
        let (url, bearer, body) = posted(ntfy.request(&notice));
        assert_eq!((url.as_str(), bearer.as_deref()), (NTFY, Some("tk")));
        assert_eq!((body["topic"].as_str(), body["priority"].as_i64()), (Some("t"), Some(4)));
        assert_eq!(
            body["actions"][2],
            json!({
                "action": "http", "label": "Reject", "url": notice.buttons[2].url, "method": "POST", "clear": true
            })
        );

        let slack = Sink::Slack { url: "https://hooks.slack.com/s".to_owned() };
        let (url, bearer, body) = posted(slack.request(&notice));
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
        let (url, _, body) = posted(telegram.request(&notice));
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
        let (_, _, body) = posted(telegram.request(&notice));
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

    #[test]
    fn an_old_style_config_sends_what_it_sent_before() {
        // The [[human]] example of the README before exec and http sinks, as it stood.
        let alice = human(
            "name = \"alice\"\ncreds = \"~/.config/ants/auth/alice.creds\"\nants = [\"dev-a\", \"dev-b\"]\n\
             approvals = true\nescalations = true\n\
             [[notify]]\nkind = \"ntfy\"\ntopic = \"alice-ants-7f3k\"\nsecret_env = \"BOT_TOKEN\"\n\
             [[notify]]\nkind = \"slack\"\nsecret_env = \"SLACK_URL\"\n\
             [[notify]]\nkind = \"telegram\"\nchat = \"123456789\"\nsecret_env = \"BOT_TOKEN\"\n\
             [[notify]]\nkind = \"ntfy\"\ntopic = \"alice-laptop-9c2e\"\ndevice = \"01968f7e-0000-7000-8000-000000000000\"\n",
        )
        .unwrap();
        assert!(alice.sinks.iter().all(|t| t.timeout == SEND_TIMEOUT));
        let notice = Notice {
            about: About::Test { id: Uuid::now_v7() },
            human: p("alice"),
            title: "dev-a asks".to_owned(),
            text: "a <b> & c".to_owned(),
            buttons: vec![Button {
                label: "Reject",
                url: "https://hub.example/x".to_owned(),
                verdict: Verdict::Reject,
            }],
            urgent: true,
        };
        let sent: Vec<_> = alice.sinks.iter().map(|t| posted(t.sink.request(&notice))).collect();
        let reject = json!({ "action": "http", "label": "Reject", "url": "https://hub.example/x", "method": "POST", "clear": true });
        assert_eq!(
            sent,
            [
                (
                    "https://ntfy.sh".to_owned(),
                    Some("123:abc".to_owned()),
                    json!({
                        "topic": "alice-ants-7f3k", "title": "dev-a asks", "message": "a <b> & c", "tags": ["ant"],
                        "priority": 4, "actions": [reject]
                    })
                ),
                (
                    "https://hooks.slack.com/services/T0/B0/xyz".to_owned(),
                    None,
                    json!({
                        "text": "dev-a asks",
                        "blocks": [
                            { "type": "section", "text": { "type": "mrkdwn", "text": "*dev-a asks*\na &lt;b&gt; &amp; c" } },
                            { "type": "actions", "elements": [{
                                "type": "button", "text": { "type": "plain_text", "text": "Reject" },
                                "url": "https://hub.example/x", "action_id": "ants-reject", "style": "danger"
                            }] }
                        ]
                    })
                ),
                (
                    "https://api.telegram.org/bot123:abc/sendMessage".to_owned(),
                    None,
                    json!({
                        "chat_id": "123456789", "text": "dev-a asks\n\na <b> & c", "disable_web_page_preview": true,
                        "reply_markup": { "inline_keyboard": [[{ "text": "Reject", "url": "https://hub.example/x" }]] }
                    })
                ),
                (
                    "https://ntfy.sh".to_owned(),
                    None,
                    json!({
                        "topic": "alice-laptop-9c2e", "title": "dev-a asks", "message": "a <b> & c", "tags": ["ant"],
                        "priority": 4, "actions": [reject]
                    })
                ),
            ]
        );
    }

    #[test]
    fn exec_and_http_sinks_are_checked() {
        let alice = human(
            "name = \"alice\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"bin/notify\", \"--to\", \"alice\"]\ntimeout = \"20s\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"notify-send\"]\n\
             [[notify]]\nkind = \"http\"\nurl = \"https://hooks.example/ants/\"\nsecret_env = \"HOOK_SECRET\"\n",
        )
        .unwrap();
        let sinks: Vec<_> = alice.sinks.iter().map(|t| (&t.sink, t.timeout)).collect();
        let exec = |command: &[&str]| Sink::Exec { command: command.iter().map(|&a| a.to_owned()).collect() };
        let http = Sink::Http { url: "https://hooks.example/ants/".to_owned(), secret: "0123456789abcdef-hook".into() };
        assert_eq!(
            sinks,
            [
                (&exec(&["/etc/ants/bin/notify", "--to", "alice"]), Duration::from_secs(20)),
                (&exec(&["notify-send"]), SEND_TIMEOUT),
                (&http, SEND_TIMEOUT),
            ]
        );
        assert_eq!(format!("{:?}", alice.sinks), "[exec /etc/ants/bin/notify, exec notify-send, http hooks.example]");
        for (text, why) in [
            ("[[notify]]\nkind = \"exec\"\n", "exec needs command"),
            ("[[notify]]\nkind = \"exec\"\ncommand = []\n", "give the program first"),
            ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\nurl = \"https://a\"\n", "url does not go with"),
            ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\nsecret_env = \"HOOK_SECRET\"\n", "reads its own"),
            ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\ntimeout = \"0\"\n", "from 1s to 30s"),
            ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\ntimeout = \"31s\"\n", "from 1s to 30s"),
            ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\ntimeout = \"10\"\n", "without a unit"),
            ("[[notify]]\nkind = \"ntfy\"\ntopic = \"t\"\ncommand = [\"x\"]\n", "command does not go with"),
            ("[[notify]]\nkind = \"http\"\nsecret_env = \"HOOK_SECRET\"\n", "http needs url"),
            ("[[notify]]\nkind = \"http\"\nurl = \"https://a/?k=v\"\nsecret_env = \"HOOK_SECRET\"\n", "no user"),
            ("[[notify]]\nkind = \"http\"\nurl = \"https://a\"\n", "needs the signing secret"),
            ("[[notify]]\nkind = \"http\"\nurl = \"https://a\"\nsecret_env = \"BOT_TOKEN\"\n", "too short"),
        ] {
            let text = format!("name = \"bob\"\n{text}");
            assert!(error(&text).contains(why), "{text}: {}", error(&text));
        }
    }

    #[test]
    fn exec_and_http_sinks_get_the_notice_as_versioned_json() {
        let (request, links) = (request(), links());
        let notice = approval_notice(&request, 9, &p("alice"), Some(&links), None, Utc::now());
        let now = Utc::now();
        let json = notice_json(&notice, now);
        let expires = request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        assert_eq!(
            json,
            json!({
                "version": 1,
                "kind": "approval",
                "id": request.id,
                "human": "alice",
                "ant": "dev-a",
                "thread": request.thread_id,
                "title": notice.title,
                "text": notice.text,
                "urgent": true,
                "sent_at": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                "request": format!("dev-a/{}", request.id),
                "seq": 9,
                "tool": {
                    "kind": "execute", "title": "rm -rf <build>", "locations": ["src/a.rs"],
                    "input": "{\"command\":\"rm -rf build\"}"
                },
                "options": ["allow_once", "allow_always", "reject"],
                "expires_at": expires,
                "links": [
                    { "verdict": "allow_once", "label": "Approve", "url": notice.buttons[0].url },
                    { "verdict": "allow_always", "label": "Always allow", "url": notice.buttons[1].url },
                    { "verdict": "reject", "label": "Reject", "url": notice.buttons[2].url },
                ],
            })
        );
        let unlinked = notice_json(&approval_notice(&request, 9, &p("alice"), None, None, now), now);
        assert_eq!(unlinked["links"], json!([]));

        let escalated = Envelope::new(p("dev-b"), p("alice"), "stuck".to_owned()).with_escalation(Some(
            Escalation::new(p("dev-a"), ThreadId::generate(), ants_proto::EscalationReason::TurnBudget),
        ));
        let json = notice_json(&escalation_notice(&escalated).unwrap(), now);
        let thread = &escalated.escalation.as_ref().unwrap().thread_id;
        assert_eq!(
            (&json["kind"], &json["id"], &json["human"], &json["ant"], &json["thread"]),
            (&json!("escalation"), &json!(escalated.id), &json!("alice"), &json!("dev-b"), &json!(thread))
        );
        assert_eq!(
            (&json["reason"], &json["stuck"], &json["level"]),
            (&json!("turn_budget"), &json!("dev-a"), &json!(0))
        );
        assert!(json.get("expires_at").is_none() && json["links"] == json!([]), "{json}");

        let http = Sink::Http { url: "https://hooks.example/a".to_owned(), secret: "0123456789abcdef-hook".into() };
        let Delivery::Post { url, headers, body } = http.request(&notice) else { panic!("not a POST") };
        assert_eq!(url, "https://hooks.example/a");
        let header = |name: &str| headers.iter().find(|(n, _)| *n == name).map(|(_, v)| v.clone()).unwrap();
        let timestamp = header("x-ants-timestamp");
        let signed = format!("{timestamp}.{}", String::from_utf8(body.clone()).unwrap());
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"0123456789abcdef-hook");
        let expected = HEXLOWER.encode(hmac::sign(&key, signed.as_bytes()).as_ref());
        assert_eq!(header("x-ants-signature"), format!("sha256={expected}"));
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["kind"], "approval");
    }

    /// An endpoint on loopback answering 200 to a `POST` signed with `secret` and 401 to others, handing over the
    /// JSON of each.
    #[cfg(unix)]
    async fn endpoint(secret: &'static str) -> (std::net::SocketAddr, tokio::sync::mpsc::UnboundedReceiver<Value>) {
        use hyper::body::Incoming;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = tx.clone();
                let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                    let tx = tx.clone();
                    async move {
                        let header = |name: &str| {
                            request.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned()
                        };
                        let (timestamp, signature) = (header("x-ants-timestamp"), header("x-ants-signature"));
                        let body = request.into_body().collect().await.unwrap().to_bytes();
                        let mut signed = format!("{timestamp}.").into_bytes();
                        signed.extend_from_slice(&body);
                        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
                        let sig = signature.strip_prefix("sha256=").and_then(|s| HEXLOWER.decode(s.as_bytes()).ok());
                        let fresh = timestamp.parse::<i64>().is_ok_and(|t| (Utc::now().timestamp() - t).abs() < 300);
                        let valid = fresh && sig.is_some_and(|sig| hmac::verify(&key, &signed, &sig).is_ok());
                        let status = if valid { StatusCode::OK } else { StatusCode::UNAUTHORIZED };
                        if valid {
                            let _ = tx.send(serde_json::from_slice::<Value>(&body).unwrap());
                        }
                        let mut response = Response::new(Full::new(Bytes::from_static(b"bad signature")));
                        *response.status_mut() = status;
                        Ok::<_, std::convert::Infallible>(response)
                    }
                });
                tokio::spawn(
                    hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service),
                );
            }
        });
        (addr, rx)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_and_http_sinks_take_the_notice_and_each_failure_is_reported() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = ants_test_support::scratch_dir("notify-exec-http").canonicalize().unwrap();
        let script = |name: &str, body: &str| {
            let path = dir.join(name);
            std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path.display().to_string()
        };
        let taken = dir.join("taken.json");
        let pid = dir.join("pid");
        let take = script("take", &format!("cat > {}", taken.display()));
        let fail = script("fail", "cat > /dev/null; echo failing >&2; exit 3");
        let hang = script("hang", &format!("echo $$ > {}\nexec sleep 30", pid.display()));
        let (good, mut posts) = endpoint("0123456789abcdef-hook").await;
        let (forged, _) = endpoint("another-secret-0123456789").await;
        let alice = human(&format!(
            "name = \"alice\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{take}\"]\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{fail}\", \"an argument\"]\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{hang}\"]\ntimeout = \"1s\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{}/missing\"]\n\
             [[notify]]\nkind = \"http\"\nurl = \"http://{good}/notices\"\nsecret_env = \"HOOK_SECRET\"\n\
             [[notify]]\nkind = \"http\"\nurl = \"http://{forged}/notices\"\nsecret_env = \"HOOK_SECRET\"\n",
            dir.display()
        ))
        .unwrap();

        let started = std::time::Instant::now();
        let results = test(&alice, "laptop").await;
        assert!(started.elapsed() < Duration::from_secs(5), "the sinks run at once, the hanging one for 1s");
        let shown: Vec<(&str, String)> = results
            .iter()
            .map(|(sink, sent)| (sink.as_str(), sent.as_ref().map_or_else(|e| format!("{e:#}"), |()| "ok".into())))
            .collect();
        let missing = format!("exec {}/missing", dir.display());
        assert_eq!(
            shown,
            [
                (&*format!("exec {take}"), "ok".to_owned()),
                (&*format!("exec {fail}"), "exit status: 3".to_owned()),
                (&*format!("exec {hang}"), "no exit within 1s: killed".to_owned()),
                (&*missing, "starting the command: No such file or directory (os error 2)".to_owned()),
                ("http 127.0.0.1", "ok".to_owned()),
                ("http 127.0.0.1", "answered 401 Unauthorized: bad signature".to_owned()),
            ]
        );
        let json: Value = serde_json::from_slice(&std::fs::read(&taken).unwrap()).unwrap();
        assert_eq!((&json["version"], &json["kind"], &json["human"]), (&json!(1), &json!("test"), &json!("alice")));
        assert!(json["text"].as_str().unwrap().contains("on laptop"), "{json}");
        let posted = tokio::time::timeout(Duration::from_secs(5), posts.recv()).await.unwrap().unwrap();
        assert_eq!((&posted["kind"], &posted["id"]), (&json!("test"), &json["id"]), "one notice, one id");
        // Killed and reaped: no process, not even a zombie, answers to its pid.
        let pid = std::fs::read_to_string(&pid).unwrap();
        let alive = std::process::Command::new("kill")
            .args(["-0", pid.trim()])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(!alive.success(), "the hanging command {} is gone", pid.trim());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
