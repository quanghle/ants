//! Signed approve and reject links of approval notices ([`Links`]), and the pages that answer them ([`Answers`]).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use ants_client::text::spelling;
use ants_proto::{ApprovalRequest, UserId, Uuid, Verdict, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use data_encoding::{HEXLOWER, HEXLOWER_PERMISSIVE};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::http::{HeaderValue, Method, Response, StatusCode, header};
use ring::hmac;

use super::config::http_url;
use super::notices::{label, redacted};
use super::sinks::cut;
use super::{APPROVAL_PATH, HumanBus, MAX_TEXT, Notified};
use crate::tools;

/// Signed approve and reject links under `public_url`, the address the reverse proxy gives the config's `path`.
#[derive(PartialEq, Eq)]
pub struct Links {
    base: String,
    secret: String,
}

/// What one link decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Action {
    pub(super) ant: UserId,
    pub(super) id: Uuid,
    /// The request's stream sequence, where it is read back.
    pub(super) seq: u64,
    pub(super) verdict: Verdict,
    /// The human whose credentials store the decision, the one notified.
    pub(super) by: UserId,
    /// The request's expiry, in Unix seconds.
    pub(super) expires: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum LinkError {
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

    pub(super) fn url(&self, action: &Action) -> String {
        let sig = HEXLOWER.encode(hmac::sign(&self.key(), action.signed().as_bytes()).as_ref());
        format!("{}{APPROVAL_PATH}?{}&sig={sig}", self.base, action.query())
    }

    /// The action of a link's query, if its signature holds and it has not expired at `now`.
    pub(super) fn verify(&self, query: &str, now: DateTime<Utc>) -> Result<Action, LinkError> {
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
        let answer = tools::Answer { seq: action.seq, request: &request, verdict: action.verdict, by: &action.by, now };
        match tools::answer_request(&bus.js, &bus.stream, &answer).await {
            Ok(decision) => {
                tracing::info!(human = %action.by, request = id, verdict = decision.verdict.as_str(), "decided on a link");
                let body = format!("{details}<p>Stored: {}.</p>", html(spelling(decision.verdict)));
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

pub(super) fn settled(state: &tools::State) -> String {
    match state {
        tools::State::Pending => "Pending.".to_owned(),
        tools::State::Decided(d) => format!("Already answered: {}.", spelling(d.verdict)),
        tools::State::DecidedByAnt(by, d) => {
            format!("Already answered by {}: {}.", tools::deciders(by, d), spelling(d.verdict))
        }
        tools::State::Expired => "Expired: its ant rejected it.".to_owned(),
    }
}

pub(super) fn details(request: &ApprovalRequest) -> String {
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
    if let Some(always) = request.label(Verdict::AllowAlways) {
        let _ = write!(out, "<p>Allow always: {}</p>", html(always));
    }
    out
}

pub(super) fn html(text: &str) -> String {
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

pub(super) fn page(status: StatusCode, title: &str, body: &str) -> Response<Full<Bytes>> {
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
