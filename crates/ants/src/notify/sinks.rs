//! Sending a [`Notice`] to a human's sinks: each sink kind's request ([`Delivery`]), sent within the sink's timeout
//! with errors that never name a URL, token or argument.

use std::time::Duration;

use ants_proto::{DeviceId, Uuid, Verdict};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use data_encoding::HEXLOWER;
use hyper::http::{Method, StatusCode};
use ring::hmac;
use serde_json::{Value, json};

use super::config::{Human, Sink, Target, http_url};
use super::notices::{About, Button, Notice, notice_json};
use super::{APPROVAL_PATH, MAX_ANSWER};

impl Sink {
    /// `notice` in the sink's form, and how it goes there.
    pub(super) fn request(&self, notice: &Notice) -> Delivery {
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
            Self::Slack { url } => Delivery::post(url, None, &slack_body(notice)),
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
pub(super) const TIMESTAMP_HEADER: &str = "x-ants-timestamp";
/// The header of an http sink's `POST` with its signature, `sha256=` and the hex HMAC-SHA256 of [`signature`].
pub(super) const SIGNATURE_HEADER: &str = "x-ants-signature";

/// The signature an http sink's `POST` of `body` at `timestamp` carries: `sha256=` and the lowercase hex HMAC-SHA256,
/// keyed with `secret`, of the timestamp's decimal digits, a `.` and the body.
pub(super) fn signature(secret: &str, timestamp: &str, body: &[u8]) -> String {
    let mut context = hmac::Context::with_key(&hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()));
    context.update(timestamp.as_bytes());
    context.update(b".");
    context.update(body);
    format!("sha256={}", HEXLOWER.encode(context.sign().as_ref()))
}

/// How a notice goes to a sink. Holds the sink's secrets, so it has no `Debug`.
pub(super) enum Delivery {
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

/// The Slack message of `notice`: its text, and its buttons below it.
fn slack_body(notice: &Notice) -> Value {
    let text = cut(&format!("*{}*\n{}", slack_escape(&notice.title), slack_escape(&notice.text)), 2900);
    let mut blocks = vec![json!({ "type": "section", "text": { "type": "mrkdwn", "text": text } })];
    if !notice.buttons.is_empty() {
        let elements: Vec<Value> = notice.buttons.iter().map(slack_button).collect();
        blocks.push(json!({ "type": "actions", "elements": elements }));
    }
    json!({ "text": slack_escape(&notice.title), "blocks": blocks })
}

fn slack_button(b: &Button) -> Value {
    let mut button = json!({
        "type": "button",
        "text": { "type": "plain_text", "text": b.label },
        "url": b.url,
        "action_id": format!("ants-{}", b.verdict.as_str()),
    });
    button["style"] = json!(if b.verdict == Verdict::Reject { "danger" } else { "primary" });
    button
}

fn slack_escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// `text` cut to `max` characters, with `…` when cut.
pub(super) fn cut(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((at, _)) => format!("{}…", &text[..at]),
        None => text.to_owned(),
    }
}

/// `method` on `url`, with `headers` and `body` as JSON if any: the status of the answer and the start of its body.
/// Errors never name the URL, which may hold a secret.
pub(super) async fn fetch(
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
pub(super) async fn post_json(url: &str, headers: &[(&str, String)], body: Vec<u8>) -> Result<()> {
    let (status, answer) = fetch(Method::POST, url, headers, Some(body)).await?;
    if status.is_success() {
        return Ok(());
    }
    bail!("answered {status}: {}", answer.trim())
}

/// Run `command` with `input` on its standard input, its output dropped and its errors on those of the process: an
/// error unless it exits with status 0 within `wait`. Past `wait` it is killed and reaped.
pub(super) async fn run_command(command: &[String], input: Vec<u8>, wait: Duration) -> Result<()> {
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
            // Fails only for a command already exited.
            let _ = child.kill().await;
            bail!("no exit within {}s: killed", wait.as_secs())
        }
    }
}

/// `notice` sent to the sink of `target` within its timeout.
pub(super) async fn send_to(target: &Target, notice: &Notice) -> Result<()> {
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
pub(super) async fn send(human: &Human, notice: &Notice, away: impl Fn(&DeviceId) -> bool) {
    for (target, sent) in dispatch(human, notice, |t| t.device.as_ref().is_none_or(&away)).await {
        match sent {
            Ok(()) => tracing::debug!(human = %human.name, sink = target.sink.label(), "notified"),
            Err(e) => tracing::warn!(human = %human.name, sink = target.sink.label(), "notifying: {e:#}"),
        }
    }
}

/// Send `notice` to the sinks of `human` that `wanted` keeps, all at once, each in its kind's form
/// ([`Sink::request`]) and within its timeout: each sink and how it went, a failure stopping no other.
pub(super) async fn dispatch<'a>(
    human: &'a Human,
    notice: &Notice,
    wanted: impl Fn(&Target) -> bool,
) -> Vec<(&'a Target, Result<()>)> {
    let sends =
        human.sinks.iter().filter(|t| wanted(t)).map(|target| async move { (target, send_to(target, notice).await) });
    futures::future::join_all(sends).await
}
