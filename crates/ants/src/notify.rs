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
//!
//! Each human's connection also answers ants asking for the reactions to a message, in one queue group across every
//! process and human: only humans' credentials read the reactions bucket, and the answer lists them only for a message
//! the ant reads ([`ants_daemon::reactions::serve`]).
//!
//! A notice a snooze or quiet hours hold back is dropped. With `digest = true`, it is counted first in the human's
//! [`HeldNotices`] in [`NOTIFY_BUCKET`], with compare-and-set, by whichever process took it; once neither holds any
//! more, checked every [`DIGEST_EVERY`] and before the next notice sent, one process claims the count by deleting it
//! at the revision it read and sends one digest notice: the counts per kind and the tool requests still pending.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::direct_kv::{DirectKv, Lookup};
use ants_daemon::humans::{Registry, Watching};
use ants_daemon::redact::Redactor;
use ants_daemon::sync::MutexExt as _;
use ants_daemon::unserved::{Unserved, Watch};
use ants_proto::{
    ApprovalRequest, Away, DeviceId, Envelope, Escalation, EscalationReason, Following, HeldNotices, JsDomain,
    NOTIFY_BUCKET, NotifyPrefs, Question, Role, STREAM, ThreadId, UserId, Uuid, Verdict, held_key, notify_consumer,
    subject,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::{self, consumer, kv, stream::Stream};
use chrono::{DateTime, TimeDelta, Utc};
use data_encoding::{HEXLOWER, HEXLOWER_PERMISSIVE};
use futures::StreamExt as _;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::http::{HeaderValue, Method, Response, StatusCode, Uri, header};
use jiff::tz::TimeZone;
use ring::hmac;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::OnceCell;

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
/// How often a human's held notices are checked for a digest due, besides before each notice sent.
const DIGEST_EVERY: Duration = if cfg!(test) { Duration::from_millis(200) } else { Duration::from_secs(60) };
/// Most pending tool requests a digest's text lists.
const DIGEST_PENDING: usize = 10;
/// How many times a held notice is counted again when another process counted or claimed meanwhile.
const CONFLICTS: usize = 10;

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
    /// Whether a question an ant asks the human (ask_human) is notified: as escalations are, unless set.
    questions: Option<bool>,
    /// The daily window notices are held back in, such as `22:00-07:00`.
    quiet: Option<String>,
    /// The time zone `quiet` reads in, instead of the file's.
    timezone: Option<String>,
    /// What quiet hours and a snooze let through.
    #[serde(default)]
    bypass: Vec<Bypass>,
    /// The human who also gets the escalations and tool requests waiting for this one while it is away.
    stand_in: Option<UserId>,
    /// Whether one digest of the notices quiet hours or a snooze held back is sent when they end.
    #[serde(default)]
    digest: bool,
    #[serde(default)]
    notify: Vec<SinkEntry>,
}

/// What kind of notice quiet hours and a snooze let through, as a human's `bypass` lists them.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Bypass {
    Approvals,
    Escalations,
    Questions,
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
    questions: bool,
    quiet: Option<QuietHours>,
    bypass: Vec<Bypass>,
    /// Who gets what waits for the human while it is away, unless an ant names its own stand-in ([`relayed`]).
    stand_in: Option<UserId>,
    digest: bool,
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
        if self.start < self.end { self.start <= now && now < self.end } else { now >= self.start || now < self.end }
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
        let questions = entry.questions.unwrap_or(entry.escalations);
        if approvals.is_none() && !entry.escalations && !entry.completions && !questions {
            bail!("approvals, escalations, completions and questions are all off: nothing to notify");
        }
        if entry.stand_in.as_ref() == Some(&entry.name) {
            bail!("stand_in names the human itself");
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
            questions,
            quiet,
            bypass: entry.bypass,
            stand_in: entry.stand_in,
            digest: entry.digest,
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
                Bypass::Questions => "questions",
            })
            .collect();
        let through = if through.is_empty() { "nothing".to_owned() } else { through.join(" and ") };
        let digest = if self.digest { ", and a digest of the others follows" } else { "" };
        Some(format!("quiet hours {quiet} ({state}); quiet hours and a snooze let {through} through{digest}"))
    }

    /// The consumer's subjects: tool requests and the human's inbox, as configured.
    fn subjects(&self) -> Vec<String> {
        let approvals = self.approvals.is_some().then(|| subject::ALL_APPROVAL_REQUESTS.to_owned());
        let escalations =
            (self.escalations || self.completions || self.questions).then(|| subject::inbox_of(&self.name));
        approvals.into_iter().chain(escalations).collect()
    }

    fn wants_request_of(&self, ant: &UserId) -> bool {
        self.approvals.as_ref().is_some_and(|ants| ants.is_empty() || ants.contains(ant))
    }

    /// Whether `about` is a tool request relayed to the human as a stand-in that it is notified of anyway.
    fn gets_relayed_request(&self, about: &About) -> bool {
        let About::Escalation { from, escalation, .. } = about else { return false };
        escalation.reason == EscalationReason::Approval
            && away_of(from, escalation).is_some_and(|away| self.wants_request_of(&away.ant))
    }
}

/// The program of an exec sink as it is run: a path, one with a separator or starting with `~`, resolved as the
/// config file's paths are; a bare name left to be found on `PATH`.
pub fn resolve_program(program: &str, resolve: &impl Fn(&Path) -> PathBuf) -> String {
    if program.starts_with('~') || program.contains(std::path::is_separator) {
        resolve(Path::new(program)).to_string_lossy().into_owned()
    } else {
        program.to_owned()
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
        let only = |what: &str, given: bool| {
            if given { Err(anyhow!("{what} does not go with kind = {:?}", entry.kind)) } else { Ok(()) }
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
    /// The question `question` that `from` asks the human on `thread`, in the message `id`.
    Question { id: Uuid, from: UserId, thread: ThreadId, question: Box<Question> },
    /// A test notice of `ants webhook notify-test`.
    Test { id: Uuid },
    /// The messages `messages` the human sent, waiting for `ant`, which no daemon serves.
    Unserved { ant: UserId, messages: Vec<Uuid> },
    /// The digest `id` of the notices `held` back, with the tool requests `pending` when it was sent.
    Digest { id: Uuid, held: HeldNotices, pending: Vec<ApprovalRequest> },
}

impl About {
    /// The thread the notice is about, which muting it holds back; none for a test.
    fn thread(&self) -> Option<&ThreadId> {
        match self {
            Self::Approval { request, .. } => Some(&request.thread_id),
            Self::Escalation { escalation, .. } => Some(&escalation.thread_id),
            Self::Completion { thread, .. } | Self::Question { thread, .. } => Some(thread),
            Self::Test { .. } | Self::Unserved { .. } | Self::Digest { .. } => None,
        }
    }

    /// Its `kind` in the JSON notice.
    fn kind(&self) -> &'static str {
        match self {
            Self::Approval { .. } => "approval",
            Self::Escalation { .. } => "escalation",
            Self::Completion { .. } => "completion",
            Self::Question { .. } => "question",
            Self::Test { .. } => "test",
            Self::Unserved { .. } => "unserved",
            Self::Digest { .. } => "digest",
        }
    }

    /// The `bypass` entry letting it through quiet hours and a snooze, if one may.
    fn bypass(&self) -> Option<Bypass> {
        match self {
            Self::Approval { .. } => Some(Bypass::Approvals),
            Self::Escalation { .. } => Some(Bypass::Escalations),
            Self::Question { .. } => Some(Bypass::Questions),
            Self::Completion { .. } | Self::Test { .. } | Self::Unserved { .. } | Self::Digest { .. } => None,
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

/// Whether a snooze or quiet hours hold back notices of `human` at `now`, whatever its `bypass` lets through: while
/// they do, no digest is due.
fn holding(human: &Human, prefs: &NotifyPrefs, now: DateTime<Utc>) -> bool {
    prefs.snoozed(now) || human.quiet.as_ref().is_some_and(|quiet| quiet.contains(now))
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
        About::Escalation { id, from, escalation } => {
            let away = away_of(from, escalation);
            let mut facts = json!({
                "kind": "escalation",
                "id": id,
                "ant": away.map_or(from, |away| &away.ant),
                "thread": escalation.thread_id,
                "reason": escalation.reason.as_str(),
                "stuck": escalation.stuck,
                "level": escalation.level,
            });
            if let Some(away) = away {
                facts["stand_in_for"] = json!(away.human);
            }
            facts
        }
        About::Completion { id, from, thread } => json!({
            "kind": "completion",
            "id": id,
            "ant": from,
            "thread": thread,
        }),
        About::Question { id, from, thread, question } => json!({
            "kind": "question",
            "id": id,
            "ant": from,
            "thread": thread,
            "question": redactor().redact(&question.text),
            "options": question.options.iter().map(|o| redactor().redact(o)).collect::<Vec<_>>(),
            "free_text": question.free_text,
        }),
        About::Test { id } => json!({ "kind": "test", "id": id }),
        About::Unserved { ant, messages } => {
            json!({ "kind": "unserved", "id": messages.first(), "ant": ant, "messages": messages })
        }
        About::Digest { id, held, pending } => {
            let pending: Vec<Value> = pending
                .iter()
                .map(|request| {
                    json!({
                        "request": tools::qualified(request),
                        "ant": request.ant,
                        "thread": request.thread_id,
                        "title": request.tool.title,
                        "expires_at": request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    })
                })
                .collect();
            json!({
                "kind": "digest",
                "id": id,
                "since": held.since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                "counts": held.counts,
                "pending": pending,
            })
        }
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
    let buttons = links.map_or_default(|links| {
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
    });
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
/// expired tool request's notice comes on a thread of its own. One relayed by a human away ([`relayed`]) names that
/// human and the ant to answer, whose answer its text gives.
fn escalation_notice(envelope: &Envelope) -> Option<Notice> {
    let escalation = envelope.escalation.as_ref()?;
    if let Some(away) = away_of(&envelope.from, escalation) {
        let what = match escalation.reason {
            EscalationReason::Approval => format!("{} asks to run a tool", away.ant),
            reason => format!("{} escalates: {} on {}", away.ant, reason.as_str(), escalation.stuck),
        };
        return Some(Notice {
            about: About::Escalation { id: envelope.id, from: envelope.from.clone(), escalation: escalation.clone() },
            human: envelope.to.clone(),
            title: format!("for {} (away): {what}", away.human),
            text: notice_text(envelope, 2 * MAX_TEXT),
            buttons: Vec::new(),
            urgent: escalation.reason == EscalationReason::Approval,
        });
    }
    let thread = &escalation.thread_id;
    let text = format!(
        "{}\n\nthread {thread}; answer with ants ask --thread {thread} @{} TEXT",
        notice_text(envelope, MAX_TEXT),
        envelope.from,
    );
    let title = match escalation.reason {
        EscalationReason::Approval => format!("{}'s tool request expired unanswered", escalation.stuck),
        EscalationReason::Storage => "JetStream storage filling up".to_owned(),
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

/// What a notice shows of the text of `envelope`, cut at `max`: of a sealed message, only that it arrived.
fn notice_text(envelope: &Envelope, max: usize) -> String {
    match envelope.sealed {
        Some(_) => "a sealed message arrived".to_owned(),
        None => cut(&redactor().redact(&envelope.text), max),
    }
}

/// Whose escalation or tool request `escalation`, sent by `from`, relays to a stand-in: the human away, who sent it.
fn away_of<'a>(from: &UserId, escalation: &'a Escalation) -> Option<&'a Away> {
    escalation.away.as_ref().filter(|away| away.human == *from)
}

/// The message relaying to `human`'s stand-in the notice `notice`, held back from `human` by `why`, if it is to be
/// relayed: a pending tool request that waits for `human`, or an escalation an ant sent it, but not the notice of an
/// expired request nor one relayed already, and not what a muted thread holds back. The stand-in is the one the ant
/// named on it, else `human`'s own; it must be a human (`is_human`), as an ant would take the relay for a human's
/// message, and not `human`. The relay goes on the escalation's or request's thread, from `human` to the stand-in,
/// carrying the escalation (a request's as [`Escalation::of_request`] makes it) marked [`Escalation::away`].
fn relayed(human: &Human, notice: &Notice, why: &str, is_human: impl Fn(&UserId) -> bool) -> Option<Envelope> {
    if why == "muted thread" {
        return None;
    }
    let (ant, named, escalation) = match &notice.about {
        About::Approval { request, .. } if request.human.as_ref() == Some(&human.name) => {
            (&request.ant, request.stand_in.as_ref(), Escalation::of_request(request))
        }
        About::Escalation { from, escalation, .. }
            if escalation.away.is_none() && escalation.reason != EscalationReason::Approval =>
        {
            (from, escalation.stand_in.as_ref(), (**escalation).clone())
        }
        _ => return None,
    };
    let owner = &human.name;
    let valid = |user: &&UserId| *user != owner && is_human(user);
    let stand_in = named.filter(valid).or(human.stand_in.as_ref().filter(valid))?;
    let text = format!(
        "[ants] Relayed to {stand_in}, {owner}'s stand-in, while {owner} is away ({why}); an answer acts for \
         {owner}, who is told of it.\n\n{}\n\n{}",
        notice.title, notice.text
    );
    let escalation = Escalation { away: Some(Away { human: owner.clone(), ant: ant.clone() }), ..escalation };
    let thread = escalation.thread_id.clone();
    Some(Envelope::on_thread(thread, owner.clone(), stand_in.clone(), text).with_escalation(Some(escalation)))
}

/// The notice of an ant's reply `envelope`, if it is one: a message answering another (`in_reply_to`) that carries no
/// escalation, decision, mention or summary and comes from no webhook. The CLI that asked for a summary takes the
/// reply itself.
fn completion_notice(envelope: &Envelope) -> Option<Notice> {
    let reply = envelope.in_reply_to.is_some()
        && envelope.escalation.is_none()
        && envelope.approval.is_none()
        && envelope.mention.is_none()
        && envelope.summary.is_none()
        && !envelope.webhook
        && envelope.from != envelope.to;
    if !reply {
        return None;
    }
    let thread = &envelope.thread_id;
    let text = format!(
        "{}\n\nthread {thread}; answer with ants ask --thread {thread} @{} TEXT",
        notice_text(envelope, MAX_TEXT),
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

/// The notice of a question an ant asks (ask_human) in `envelope`, if it carries one: its text numbers the options,
/// and says how `ants answer` picks one or types an answer.
fn question_notice(envelope: &Envelope) -> Option<Notice> {
    let question = envelope.question.as_ref()?;
    let (id, thread) = (envelope.id, &envelope.thread_id);
    let typed = if question.free_text { " (an option's number, or a typed answer)" } else { "" };
    let text =
        format!("{}\n\nthread {thread}; answer with ants answer {id} NUMBER{typed}", notice_text(envelope, MAX_TEXT));
    let asked = cut(&redactor().redact(&question.text.split_whitespace().collect::<Vec<_>>().join(" ")), 100);
    Some(Notice {
        about: About::Question {
            id,
            from: envelope.from.clone(),
            thread: thread.clone(),
            question: Box::new(question.clone()),
        },
        human: envelope.to.clone(),
        title: format!("{} asks: {asked}", envelope.from),
        text,
        buttons: Vec::new(),
        urgent: false,
    })
}

/// The notice to `human` of its messages `found` waiting for an ant no daemon serves, at `now`.
fn unserved_notice(found: &Unserved, human: &UserId, now: DateTime<Utc>) -> Notice {
    let text = found.text(human, now);
    let text = redactor().redact(&text);
    Notice {
        about: About::Unserved { ant: found.ant.clone(), messages: found.messages.iter().map(|m| m.id).collect() },
        human: human.clone(),
        title: format!("messages wait for {}, which no daemon serves", found.ant),
        text: text.strip_prefix("[ants] ").unwrap_or(&text).to_owned(),
        buttons: Vec::new(),
        urgent: false,
    }
}

/// How `n` notices of `kind` read.
fn counted(kind: &str, n: u32) -> String {
    let (one, many) = match kind {
        "approval" => ("tool request", "tool requests"),
        "escalation" => ("escalation", "escalations"),
        "completion" => ("reply", "replies"),
        "question" => ("question", "questions"),
        _ => return format!("{n} {kind} notices"),
    };
    format!("{n} {}", if n == 1 { one } else { many })
}

/// The digest to `human` of the notices `held` back, with the tool requests `pending` at `now` (redacted, at most
/// [`DIGEST_PENDING`] in its text), its times on the wall clock of the human's quiet hours, else UTC.
fn digest_notice(human: &Human, held: HeldNotices, pending: &[ApprovalRequest], now: DateTime<Utc>) -> Notice {
    let zone = human.quiet.as_ref().map_or(TimeZone::UTC, |quiet| quiet.zone.clone());
    let since = jiff::Timestamp::from_second(held.since.timestamp())
        .map_or_default(|at| at.to_zoned(zone).strftime("%Y-%m-%d %H:%M %Z").to_string());
    let counts: Vec<String> = held.counts.iter().map(|(kind, n)| counted(kind, *n)).collect();
    let mut text = format!(
        "{} held back by quiet hours or a snooze since {since}; nothing was marked read: ants inbox shows the \
         messages.",
        counts.join(", ")
    );
    let pending: Vec<ApprovalRequest> = pending.iter().map(redacted).collect();
    if !pending.is_empty() {
        let _ = write!(
            text,
            "\n\n{} still pending:",
            counted("approval", u32::try_from(pending.len()).unwrap_or(u32::MAX))
        );
        for request in pending.iter().take(DIGEST_PENDING) {
            let _ = write!(
                text,
                "\n{} {} ({}), rejected in {} unless answered",
                tools::qualified(request),
                request.tool.title,
                request.tool.kind,
                tools::left(request.expires_at, now)
            );
        }
        if pending.len() > DIGEST_PENDING {
            let _ = write!(text, "\n…and {} more: ants tools list", pending.len() - DIGEST_PENDING);
        }
    }
    let total = held.total();
    Notice {
        title: format!("{total} {} held back", if total == 1 { "notice" } else { "notices" }),
        human: human.name.clone(),
        text,
        buttons: Vec::new(),
        urgent: !pending.is_empty(),
        about: About::Digest { id: Uuid::now_v7(), held, pending },
    }
}

// ---- sending ---------------------------------------------------------------------------------------------------

/// `method` on `url`, with `headers` and `body` as JSON if any: the status of the answer and the start of its body.
/// Errors never name the URL, which may hold a secret.
async fn fetch(
    method: Method,
    url: &str,
    headers: &[(&str, String)],
    body: Option<Vec<u8>>,
) -> Result<(StatusCode, String)> {
    let body = body.map(|body| ("application/json", body));
    let (status, answer) = crate::http::fetch(method, url, headers, body, MAX_ANSWER * 8).await?;
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
    /// The human's holds ([`NotifyPrefs`]).
    prefs: DirectKv,
    /// The humans of the team, kept current while `_watching` lives.
    humans: Registry,
    _watching: Watching,
    /// Where the notices held back are counted for a digest ([`HeldNotices`]); none without `digest`, or when the
    /// bucket cannot be created.
    held: Option<kv::Store>,
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
        let mut recent = self.recent.locked();
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
    // Through a leaf node, a direct get of a missing bucket goes unanswered rather than finding none.
    let bucket = crate::quiet::ensure_bucket(&js).await.inspect_err(|e| tracing::warn!(human = %name, "{e:#}"));
    let held = bucket.ok().filter(|_| human.config.digest);
    let prefs = DirectKv::new(&js, js_domain.as_ref(), NOTIFY_BUCKET).await?;
    let unserved = unserved_watch(&human.config, &js, js_domain.as_ref()).await;
    let humans = Registry::new(name.clone());
    let _watching = humans.watch(js.clone(), ants_daemon::humans::RETRY);
    let _ = human.bus.set(HumanBus { client, js, stream, present, prefs, humans, _watching, held });
    let bus = human.bus.get().expect("just set");
    tracing::info!(human = %name, sinks = ?human.config.sinks, "notifying");
    let mut unserved = Box::pin(tell_unserved(&human.config, bus, unserved));
    let mut reactions = Box::pin(answer_reactions(name, bus));
    let mut digests = Box::pin(digests(&human.config, bus));
    let mut delay = Duration::from_secs(1);
    loop {
        let watched = tokio::select! {
            watched = watch(&human, bus, links.as_deref()) => watched,
            never = &mut unserved => match never {},
            never = &mut reactions => match never {},
            never = &mut digests => match never {},
        };
        match watched {
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
        let seq = message.info().map_or_default(|i| i.stream_sequence);
        if human.first_time(seq) {
            notify(human, bus, links, &message.subject, &message.payload, seq).await;
        }
        if let Err(e) = message.ack().await {
            tracing::warn!(human = %human.config.name, error = %e, "acking a notification");
        }
    }
    Ok(())
}

/// Notify `human` of the message at `seq` on `subject`, if it is a pending tool request it wants, or an escalation,
/// question or reply it takes.
async fn notify(human: &Notified, bus: &HumanBus, links: Option<&Links>, subject: &str, payload: &[u8], seq: u64) {
    let config = &human.config;
    let now = Utc::now();
    let inbox = subject.starts_with(subject::inbox_of(&config.name).trim_end_matches('*'));
    let notice = if inbox {
        match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) if envelope.check_subject(subject).is_ok() && is_recent(&envelope.id, now) => {
                match (envelope.escalation.is_some(), envelope.question.is_some()) {
                    (true, _) if config.escalations => {
                        escalation_notice(&envelope).filter(|notice| !config.gets_relayed_request(&notice.about))
                    }
                    (false, true) if config.questions => question_notice(&envelope),
                    (false, false) if config.completions => completion_notice(&envelope),
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
        // Counted for the human's digest even when relayed to its stand-in: the digest tells what it missed.
        if why != "muted thread" {
            count_held(config, bus, notice.about.kind(), now).await;
        }
        relay(config, bus, &notice, why, seq).await;
        return;
    }
    digest(config, bus, &prefs, now).await;
    let what = if inbox { Following::Inbox } else { Following::Approvals };
    let reading = bus.present.following(&config.name, config.devices(), what).await;
    if !reading.is_empty() {
        tracing::debug!(human = %config.name, devices = ?reading, what = what.as_str(), "skipping devices reading");
    }
    send(config, &notice, |device| !reading.contains(device)).await;
}

/// Relay `notice`, held back from `human` by `why`, to its stand-in ([`relayed`]) when `human` is away: no device of
/// its sinks follows what the notice is about. The relay is stored once, whichever process of `human` relays it.
async fn relay(human: &Human, bus: &HumanBus, notice: &Notice, why: &str, seq: u64) {
    let Some(relay) = relayed(human, notice, why, |user| bus.humans.is_human(user)) else { return };
    let what = if matches!(notice.about, About::Approval { .. }) { Following::Approvals } else { Following::Inbox };
    let reading = bus.present.following(&human.name, human.devices(), what).await;
    if !reading.is_empty() {
        tracing::debug!(human = %human.name, devices = ?reading, "not away: no relay to the stand-in");
        return;
    }
    let id = format!("standin-{}-{seq}", human.name);
    match bus::store(&bus.js, relay.subject(), &relay, &id).await {
        Ok(_) => {
            tracing::info!(human = %human.name, stand_in = %relay.to, title = notice.title, "relayed to the stand-in");
        }
        Err(e) => tracing::warn!(
            human = %human.name,
            stand_in = %relay.to,
            error = format!("{e:#}"),
            "relaying to the stand-in"
        ),
    }
}

/// The watch of the messages `human` sends ([`unserved`]), when it takes escalations.
async fn unserved_watch(human: &Human, js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Option<Watch> {
    if !human.escalations {
        return None;
    }
    let name = &human.name;
    let roster = ants_daemon::mcp::jetstream_roster(js.clone());
    Watch::new(js, js_domain, name.clone(), Role::Human, roster)
        .await
        .inspect_err(|e| tracing::warn!(human = %name, error = format!("{e:#}"), "watching the messages sent"))
        .ok()
}

/// Tell `human`, through its sinks, of the messages it sent that keep waiting for an ant no daemon serves, as `watch`
/// finds them: those held back by a snooze or quiet hours wait for them to end.
async fn tell_unserved(human: &Human, bus: &HumanBus, watch: Option<Watch>) -> std::convert::Infallible {
    if let Some(mut watch) = watch {
        loop {
            for found in watch.next(|user| bus.humans.is_human(user)).await {
                let now = Utc::now();
                let notice = unserved_notice(&found, &human.name, now);
                if let Some(why) = held(human, &notice.about, &holds(human, bus).await, now) {
                    tracing::debug!(human = %human.name, why, title = notice.title, "holding back a notice");
                    continue;
                }
                if let Some(found) = watch.claim(found).await {
                    send(human, &unserved_notice(&found, &human.name, Utc::now()), |_| true).await;
                }
            }
        }
    }
    std::future::pending().await
}

/// Answer the ants asking for the reactions to the messages they read, with the credentials of `human`, which read
/// every reaction, while the process runs; `ants signer` alone changes an ant's reactions.
async fn answer_reactions(human: &UserId, bus: &HumanBus) -> std::convert::Infallible {
    let mut delay = Duration::from_secs(1);
    loop {
        let answering = ants_daemon::reactions::Answering::Lists;
        if let Err(e) = ants_daemon::reactions::serve(bus.client.clone(), bus.js.clone(), None, answering).await {
            tracing::warn!(human = %human, retry_in = ?delay, "answering reactions requests: {e:#}");
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

/// The notices held back from `human` and the revision they were read at, 0 when never stored: none when missing or
/// deleted, empty counts when malformed.
async fn load_held(kv: &kv::Store, human: &UserId) -> Result<(Option<HeldNotices>, u64)> {
    let Some(entry) = kv.entry(held_key(human)).await.context("reading the notices held back")? else {
        return Ok((None, 0));
    };
    if entry.operation != kv::Operation::Put {
        return Ok((None, entry.revision));
    }
    let held = serde_json::from_slice(&entry.value).unwrap_or_else(|e| {
        tracing::warn!(%human, error = %e, "malformed notices held back: starting over");
        HeldNotices::default()
    });
    Ok((Some(held), entry.revision))
}

/// Count a notice of `kind` held back from `human` at `now`, for its digest, on top of what other processes counted
/// meanwhile; a failure is logged.
async fn count_held(human: &Human, bus: &HumanBus, kind: &str, now: DateTime<Utc>) {
    let Some(kv) = &bus.held else { return };
    let name = &human.name;
    for _ in 0..CONFLICTS {
        let (held, revision) = match load_held(kv, name).await {
            Ok(loaded) => loaded,
            Err(e) => {
                tracing::warn!(human = %name, error = format!("{e:#}"), "counting a notice held back");
                return;
            }
        };
        let mut held = held.unwrap_or_default();
        held.add(kind, now);
        let Ok(value) = serde_json::to_vec(&held) else { return };
        match kv.update(held_key(name), value.into(), revision).await {
            Ok(_) => return,
            Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => {}
            Err(e) => {
                tracing::warn!(human = %name, error = %e, "counting a notice held back");
                return;
            }
        }
    }
    tracing::warn!(human = %name, "counting a notice held back: other processes kept changing the count");
}

/// Send `human` one digest of the notices held back, if any, once a snooze or quiet hours no longer hold at `now`
/// ([`holding`]), as `prefs` and its config say. The process whose delete of the count, at the revision it read, is
/// stored sends it, so one of those notifying the human does, at most once; the others find it gone or changed.
/// Nothing is marked read or decided.
async fn digest(human: &Human, bus: &HumanBus, prefs: &NotifyPrefs, now: DateTime<Utc>) {
    let Some(kv) = &bus.held else { return };
    if holding(human, prefs, now) {
        return;
    }
    let name = &human.name;
    let (held, revision) = match load_held(kv, name).await {
        Ok((Some(held), revision)) => (held, revision),
        Ok((None, _)) => return,
        Err(e) => {
            tracing::warn!(human = %name, error = format!("{e:#}"), "reading the notices held back");
            return;
        }
    };
    match kv.delete_expect_revision(held_key(name), Some(revision)).await {
        Ok(()) => {}
        Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => return,
        Err(e) => {
            tracing::warn!(human = %name, error = %e, "claiming the notices held back");
            return;
        }
    }
    if held.total() == 0 {
        return;
    }
    let pending = match human.approvals {
        None => Vec::new(),
        Some(_) => match tools::pending(&bus.js, &bus.stream, now).await {
            Ok(pending) => pending.into_iter().filter(|r| human.wants_request_of(&r.ant)).collect(),
            Err(e) => {
                tracing::warn!(human = %name, error = format!("{e:#}"), "reading the tool requests still pending");
                Vec::new()
            }
        },
    };
    let notice = digest_notice(human, held, &pending, now);
    tracing::info!(human = %name, title = notice.title, "sending the digest of the notices held back");
    send(human, &notice, |_| true).await;
}

/// Check every [`DIGEST_EVERY`] whether the digest of what was held back from `human` is due, and send it if so.
async fn digests(human: &Human, bus: &HumanBus) -> std::convert::Infallible {
    if bus.held.is_some() {
        loop {
            tokio::time::sleep(DIGEST_EVERY).await;
            let prefs = holds(human, bus).await;
            digest(human, bus, &prefs, Utc::now()).await;
        }
    }
    std::future::pending().await
}

/// The holds `human` set, read anew for each notice: none when they cannot be read, so a failure sends a notice too
/// many, never one too few.
async fn holds(human: &Human, bus: &HumanBus) -> NotifyPrefs {
    match bus.prefs.lookup(human.name.as_str()).await {
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
        match tools::answer_request(&bus.js, &bus.stream, action.seq, &request, action.verdict, &action.by, now).await {
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
        tools::State::DecidedByAnt(by, d) => format!("Already answered by {by}: {}.", tools::spelling(d.verdict)),
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
mod tests;
