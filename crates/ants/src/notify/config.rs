//! The `[[human]]` entries of the webhook config: each human's sinks, devices, snoozes and quiet hours, checked
//! and resolved into a [`Human`].

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{DeviceId, EscalationReason, UserId, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use hyper::http::Uri;
use jiff::tz::TimeZone;
use serde::Deserialize;

use super::notices::{About, away_of};
use super::{COMPLETION_WINDOW, MAX_COMPLETION_WINDOW, MAX_SEND_TIMEOUT, MIN_SECRET, SEND_TIMEOUT};

pub(super) fn yes() -> bool {
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
    /// How long a thread is quiet before its replies are notified, folded into one.
    completion_window: Option<String>,
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
pub(super) enum Bypass {
    Approvals,
    Escalations,
    Questions,
}

/// One `[[human.notify]]`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SinkEntry {
    pub(super) kind: SinkKind,
    pub(super) url: Option<String>,
    pub(super) topic: Option<String>,
    pub(super) chat: Option<String>,
    /// The program of an exec sink and its arguments, run without a shell.
    pub(super) command: Option<Vec<String>>,
    pub(super) secret_env: Option<String>,
    pub(super) secret_file: Option<PathBuf>,
    /// The device of the human whose push target the sink is: none while that device follows what a notice is about.
    device: Option<DeviceId>,
    /// How long the sink may take, such as `20s`.
    timeout: Option<String>,
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum SinkKind {
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
    pub(super) approvals: Option<Vec<UserId>>,
    pub(super) escalations: bool,
    pub(super) completions: bool,
    /// How long a thread is quiet before its replies are notified, folded into one: zero for each as it comes.
    pub(super) completion_window: Duration,
    pub(super) questions: bool,
    pub(super) quiet: Option<QuietHours>,
    pub(super) bypass: Vec<Bypass>,
    /// Who gets what waits for the human while it is away, unless an ant names its own stand-in ([`relayed`]).
    ///
    /// [`relayed`]: super::notices::relayed
    pub(super) stand_in: Option<UserId>,
    pub(super) digest: bool,
    pub(super) sinks: Vec<Target>,
}

/// A daily window in a time zone, from `start` to `end` (minutes after midnight, `end` excluded), in which notices are
/// held back; one ending before it starts crosses midnight.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct QuietHours {
    start: u16,
    end: u16,
    pub(super) zone: TimeZone,
    /// As written, for display.
    text: String,
}

impl QuietHours {
    /// `text`, `HH:MM-HH:MM`, read in `zone`.
    pub(super) fn parse(text: &str, zone: TimeZone) -> Result<Self> {
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
    pub(super) fn contains(&self, at: DateTime<Utc>) -> bool {
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
pub(super) struct Target {
    pub(super) device: Option<DeviceId>,
    pub(super) sink: Sink,
    /// How long the sink may take to take a notice.
    pub(super) timeout: Duration,
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
pub(super) enum Sink {
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

impl Sink {
    pub(super) fn from_entry(
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

    pub(super) fn label(&self) -> String {
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
}

pub(super) const NTFY: &str = "https://ntfy.sh";
pub(super) const TELEGRAM: &str = "https://api.telegram.org";

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
        let completion_window = match entry.completion_window {
            Some(_) if !entry.completions => bail!("completion_window goes with completions = true only"),
            text => completion_window(text)?,
        };
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
            completion_window,
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
        let claims =
            crate::people::humans::creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
        let name = &self.name;
        let human = crate::people::humans::writes_humans(&claims);
        if claims["name"].as_str() != Some(name.as_str()) || !human {
            bail!(
                "credentials {} are not human {name}'s; issue them with `ants auth user {name} --human`",
                path.display()
            );
        }
        Ok(())
    }

    /// The devices that sinks are push targets of.
    pub(super) fn devices(&self) -> impl Iterator<Item = DeviceId> {
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
    pub(super) fn subjects(&self) -> Vec<String> {
        let approvals = self.approvals.is_some().then(|| subject::ALL_APPROVAL_REQUESTS.to_owned());
        let escalations =
            (self.escalations || self.completions || self.questions).then(|| subject::inbox_of(&self.name));
        approvals.into_iter().chain(escalations).collect()
    }

    pub(super) fn wants_request_of(&self, ant: &UserId) -> bool {
        self.approvals.as_ref().is_some_and(|ants| ants.is_empty() || ants.contains(ant))
    }

    /// Whether `about` is a tool request relayed to the human as a stand-in that it is notified of anyway.
    pub(super) fn gets_relayed_request(&self, about: &About) -> bool {
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
pub(super) fn sink_timeout(text: Option<String>) -> Result<Duration> {
    let Some(text) = text else { return Ok(SEND_TIMEOUT) };
    let timeout = ants_proto::duration::parse(&text).map_err(|e| anyhow!(e))?;
    if timeout.is_zero() || timeout > MAX_SEND_TIMEOUT {
        bail!("timeout {text:?}: from 1s to {}", ants_proto::duration::format(MAX_SEND_TIMEOUT));
    }
    Ok(timeout)
}

/// A human's `completion_window`, [`COMPLETION_WINDOW`] unless given.
pub(super) fn completion_window(text: Option<String>) -> Result<Duration> {
    let Some(text) = text else { return Ok(COMPLETION_WINDOW) };
    let window = ants_proto::duration::parse(&text).map_err(|e| anyhow!(e))?;
    if window > MAX_COMPLETION_WINDOW {
        bail!("completion_window {text:?}: from 0 to {}", ants_proto::duration::format(MAX_COMPLETION_WINDOW));
    }
    Ok(window)
}

/// `url` checked as an http(s) URL without credentials or query, without its trailing `/`.
pub(super) fn http_url(url: &str, what: &str) -> Result<String> {
    let uri: Uri = url.parse().with_context(|| format!("{what} {url:?}"))?;
    if !matches!(uri.scheme_str(), Some("https" | "http")) || uri.host().is_none_or(str::is_empty) {
        bail!("{what} {url:?}: expected https://host/...");
    }
    if uri.authority().is_some_and(|a| a.as_str().contains('@')) || uri.query().is_some() {
        bail!("{what} {url:?}: no user, password or query in it");
    }
    Ok(url.trim_end_matches('/').to_owned())
}
