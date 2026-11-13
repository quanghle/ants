//! `ants approve`: the tool requests personas escalate to the human, and the human's answers.
//!
//! Requests and decisions both live in the ANTS stream, so the pending ones are read back by subject: every request on
//! `ants.approvals.<persona>` that has not expired and has no decision on `ants.approvals.<persona>.<request id>`. An
//! expired request needs no answer: its persona already rejected it. The persona of a request is the one its subject
//! names, as credentials pin it there; a request whose body names another is skipped with a warning.
//!
//! Request ids are unique only per persona: any persona can publish a request reusing another's id. So a request is
//! written `<persona>/<id>`, and a bare id (or start of one) matching requests of several personas is refused with
//! the qualified ids to choose from.

use std::path::Path;

use ants_daemon::bus;
use ants_proto::{ApprovalDecision, ApprovalRequest, PersonaId, STREAM, Verdict, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt as _;

/// A verdict as typed on the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Answer {
    AllowOnce,
    AllowAlways,
    Reject,
}

impl From<Answer> for Verdict {
    fn from(answer: Answer) -> Self {
        match answer {
            Answer::AllowOnce => Verdict::AllowOnce,
            Answer::AllowAlways => Verdict::AllowAlways,
            Answer::Reject => Verdict::Reject,
        }
    }
}

/// The command-line spelling of a verdict.
fn spelling(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::AllowOnce => "allow-once",
        Verdict::AllowAlways => "allow-always",
        Verdict::Reject => "reject",
    }
}

fn options(request: &ApprovalRequest) -> String {
    request.options.iter().map(|v| spelling(*v)).collect::<Vec<_>>().join("|")
}

/// Where a request stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Pending,
    Decided(ApprovalDecision),
    /// No decision in time; the persona rejected it.
    Expired,
}

/// Every approval request in the stream, oldest first.
pub async fn requests(stream: &Stream) -> Result<Vec<ApprovalRequest>> {
    let mut requests = Vec::new();
    let mut from = 1;
    loop {
        let msg = match stream.get_first_raw_message_by_subject(subject::ALL_APPROVAL_REQUESTS, from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => return Ok(requests),
            Err(e) => return Err(e).context("reading approval requests"),
        };
        from = msg.sequence + 1;
        match serde_json::from_slice::<ApprovalRequest>(&msg.payload) {
            Ok(request) => match request.check_subject(&msg.subject) {
                Ok(()) => requests.push(request),
                Err(e) => tracing::warn!(seq = msg.sequence, error = %e, "skipping a request for another persona"),
            },
            Err(e) => tracing::warn!(seq = msg.sequence, error = %e, "skipping unreadable approval request"),
        }
    }
}

/// The stored decision on `request`, if any.
pub async fn decision(stream: &Stream, request: &ApprovalRequest) -> Result<Option<ApprovalDecision>> {
    let msg = match stream.get_last_raw_message_by_subject(&request.decision_subject()).await {
        Ok(msg) => msg,
        Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading the decision on request {}", request.id)),
    };
    match serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
        Ok(d) if d.request_id == request.id && d.persona == request.persona => Ok(Some(d)),
        // The persona ignores these too, so the request is still open.
        _ => {
            tracing::warn!(seq = msg.sequence, "ignoring a malformed decision");
            Ok(None)
        }
    }
}

pub async fn state(stream: &Stream, request: &ApprovalRequest, now: DateTime<Utc>) -> Result<State> {
    if let Some(decision) = decision(stream, request).await? {
        return Ok(State::Decided(decision));
    }
    Ok(if request.is_expired(now) { State::Expired } else { State::Pending })
}

/// The requests still waiting for the human at `now`, oldest first.
pub async fn pending(stream: &Stream, now: DateTime<Utc>) -> Result<Vec<ApprovalRequest>> {
    let mut pending = Vec::new();
    for request in requests(stream).await? {
        // Expired ones need no decision lookup.
        if !request.is_expired(now) && decision(stream, &request).await?.is_none() {
            pending.push(request);
        }
    }
    Ok(pending)
}

/// How a request is written on the command line: `<persona>/<id>`. Persona names and ids never contain `/`.
pub fn qualified(request: &ApprovalRequest) -> String {
    format!("{}/{}", request.persona, request.id)
}

/// What `ants approve` was given to pick a request: a start of its id, and the persona when written `<persona>/<id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub persona: Option<PersonaId>,
    pub id: String,
}

impl Query {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        let (persona, id) = match text.split_once('/') {
            Some((persona, id)) => (Some(persona.parse::<PersonaId>()?), id),
            None => (None, text),
        };
        let id = id.to_ascii_lowercase();
        if id.is_empty() {
            bail!("no request id given");
        }
        if !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
            bail!("{id} is not a request id: hex digits and dashes only");
        }
        Ok(Self { persona, id })
    }

    fn matches(&self, request: &ApprovalRequest) -> bool {
        self.persona.as_ref().is_none_or(|p| *p == request.persona) && request.id.to_string().starts_with(&self.id)
    }
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.persona {
            Some(persona) => write!(f, "{persona}/{}", self.id),
            None => f.write_str(&self.id),
        }
    }
}

/// The one request among `requests` that `query` picks. When several match, the error lists them qualified.
pub fn select(requests: Vec<ApprovalRequest>, query: &Query) -> Result<ApprovalRequest> {
    const LISTED: usize = 10;
    let mut found: Vec<_> = requests.into_iter().filter(|r| query.matches(r)).collect();
    match found.len() {
        0 => bail!("no approval request {query}"),
        1 => Ok(found.remove(0)),
        n => {
            let mut names = found.iter().take(LISTED).map(qualified).collect::<Vec<_>>().join(", ");
            if n > LISTED {
                names.push_str(&format!(" and {} more", n - LISTED));
            }
            bail!(
                "{n} approval requests match {query}: {names}; give more of the id or name the persona as <persona>/<id>"
            )
        }
    }
}

/// The one request `id` picks: a unique start of its id, optionally written `<persona>/<id>`.
pub async fn find(stream: &Stream, id: &str) -> Result<ApprovalRequest> {
    let query = Query::parse(id)?;
    select(requests(stream).await?, &query)
}

/// Answer the pending request `id` picks (see [`find`]), and wait until JetStream has stored the decision.
pub async fn answer(
    js: &jetstream::Context,
    stream: &Stream,
    id: &str,
    verdict: Verdict,
    now: DateTime<Utc>,
) -> Result<ApprovalDecision> {
    let request = find(stream, id).await?;
    match state(stream, &request, now).await? {
        State::Pending => {}
        State::Decided(d) => bail!("request {} was already answered: {}", qualified(&request), spelling(d.verdict)),
        State::Expired => {
            bail!("request {} expired at {}; {} rejected it", qualified(&request), request.expires_at, request.persona)
        }
    }
    if !request.options.contains(&verdict) {
        bail!("{} is not offered for request {}; choose {}", spelling(verdict), qualified(&request), options(&request));
    }
    let decision = request.decide(verdict);
    bus::publish_json(js, decision.subject(), &decision, None).await.context("publishing the decision")?;
    Ok(decision)
}

/// Time left before `expires_at`, such as `4m05s`.
fn left(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let secs = (expires_at - now).max(TimeDelta::zero()).num_seconds();
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        _ => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
    }
}

/// The first line of `text`, cut to `max` characters.
fn excerpt(text: &str, max: usize) -> String {
    let line = text.lines().next().unwrap_or("");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < text.len() {
        out.push('…');
    }
    out
}

/// One line of the pending list: `<persona>/<id>`, thread, tool, files, options and time left.
fn row(request: &ApprovalRequest, now: DateTime<Utc>) -> String {
    let files = if request.tool.locations.is_empty() { "-".to_owned() } else { request.tool.locations.join(",") };
    format!(
        "{}\t{}\t{}: {}\t{}\t{}\t{} left",
        qualified(request),
        request.thread_id,
        request.tool.kind,
        excerpt(&request.tool.title, 80),
        files,
        options(request),
        left(request.expires_at, now)
    )
}

/// The whole request, with its tool input and how to answer it.
fn details(request: &ApprovalRequest, state: &State, now: DateTime<Utc>) -> String {
    let mut out = format!(
        "request  {}\npersona  {}\nthread   {}\ntool     {}: {}\n",
        request.id, request.persona, request.thread_id, request.tool.kind, request.tool.title
    );
    for location in &request.tool.locations {
        out.push_str(&format!("file     {location}\n"));
    }
    if let Some(input) = &request.tool.input {
        out.push_str(&format!("input    {input}\n"));
    }
    out.push_str(&format!("asked    {}\n", request.requested_at.format("%Y-%m-%d %H:%M:%S")));
    match state {
        State::Pending => out.push_str(&format!(
            "expires  in {}\nanswer   ants approve {} {}",
            left(request.expires_at, now),
            qualified(request),
            options(request)
        )),
        State::Decided(d) => {
            out.push_str(&format!("answered {} at {}", spelling(d.verdict), d.decided_at.format("%Y-%m-%d %H:%M:%S")))
        }
        State::Expired => {
            out.push_str(&format!("expired  at {}, rejected", request.expires_at.format("%Y-%m-%d %H:%M:%S")))
        }
    }
    out
}

/// What `ants approve` was asked to do.
pub struct Args {
    pub id: Option<String>,
    pub answer: Option<Answer>,
    pub follow: bool,
    pub json: bool,
}

pub async fn run(url: &str, creds: Option<&Path>, domain: Option<&ants_proto::JsDomain>, args: Args) -> Result<()> {
    let (client, js) = bus::connect(url, creds, domain, &PersonaId::human(), "ants-approve").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let show = |request: &ApprovalRequest| -> Result<()> {
        if args.json {
            println!("{}", serde_json::to_string(request)?);
        } else {
            println!("{}", row(request, Utc::now()));
        }
        Ok(())
    };
    match (&args.id, args.answer) {
        (Some(id), Some(verdict)) => {
            let decision = answer(&js, &stream, id, verdict.into(), Utc::now()).await?;
            println!("answered request {}/{}: {}", decision.persona, decision.request_id, spelling(decision.verdict));
        }
        (Some(id), None) => {
            let request = find(&stream, id).await?;
            let now = Utc::now();
            let state = state(&stream, &request, now).await?;
            if args.json {
                println!("{}", serde_json::to_string(&request)?);
            } else {
                println!("{}", details(&request, &state, now));
            }
        }
        (None, _) => {
            // Subscribe before listing, so a request published in between is not missed.
            let mut live =
                if args.follow { Some(client.subscribe(subject::ALL_APPROVAL_REQUESTS).await?) } else { None };
            let listed = pending(&stream, Utc::now()).await?;
            if listed.is_empty() && !args.follow && !args.json {
                println!("no pending approval requests");
            }
            listed.iter().try_for_each(show)?;
            if let Some(live) = &mut live {
                while let Some(msg) = live.next().await {
                    let Ok(request) = serde_json::from_slice::<ApprovalRequest>(&msg.payload) else { continue };
                    if let Err(e) = request.check_subject(&msg.subject) {
                        tracing::warn!(error = %e, "skipping a request for another persona");
                    } else if !request.is_expired(Utc::now())
                        && !listed.iter().any(|r| r.id == request.id && r.persona == request.persona)
                    {
                        show(&request)?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_proto::{ThreadId, ToolCall};

    use super::*;
    use crate::auth::tests::Server;

    fn request(persona: &PersonaId, title: &str, offered: Vec<Verdict>, timeout: Duration) -> ApprovalRequest {
        let tool = ToolCall { kind: "execute".into(), title: title.into(), ..Default::default() };
        ApprovalRequest::new(persona.clone(), ThreadId::generate(), tool, offered, timeout)
    }

    fn titles(requests: &[ApprovalRequest]) -> Vec<&str> {
        requests.iter().map(|r| r.tool.title.as_str()).collect()
    }

    /// Runs as the human and dev-a with their own credentials, so it also checks their permissions.
    #[tokio::test]
    async fn pending_requests_are_listed_and_answered() {
        let Some(server) = Server::start() else { return };
        let dev_a: PersonaId = "dev-a".parse().unwrap();
        let (_, human) = server.connect(&PersonaId::human()).await;
        let stream = bus::ensure_stream(&human).await.unwrap();
        let (_, agent) = server.connect(&dev_a).await;
        let minute = Duration::from_secs(60);
        let push = request(&dev_a, "git push", vec![Verdict::AllowOnce, Verdict::AllowAlways], minute);
        let rm = request(&dev_a, "rm -rf build", vec![Verdict::AllowOnce], minute);
        let late = request(&dev_a, "curl example.com", vec![Verdict::AllowOnce], Duration::ZERO);
        for r in [&push, &rm, &late] {
            bus::publish_json(&agent, r.subject(), r, None).await.unwrap();
        }
        // dev-b publishes only on its own subject, so a request there naming dev-a is forged.
        let dev_b: PersonaId = "dev-b".parse().unwrap();
        let (_, intruder) = server.connect(&dev_b).await;
        let forged = request(&dev_a, "git push --force", vec![Verdict::AllowAlways], minute);
        bus::publish_json(&intruder, subject::approvals(&dev_b), &forged, None).await.unwrap();
        assert_eq!(titles(&pending(&stream, Utc::now()).await.unwrap()), ["git push", "rm -rf build"]);
        let err = answer(&human, &stream, &forged.id.to_string(), Verdict::AllowAlways, Utc::now()).await.unwrap_err();
        assert!(err.to_string().contains("no approval request"), "{err:#}");

        let id = push.id.to_string();
        let decision = answer(&human, &stream, &id[..id.len() - 4], Verdict::AllowAlways, Utc::now()).await.unwrap();
        assert_eq!((decision.request_id, decision.verdict), (push.id, Verdict::AllowAlways));
        let stored = stream.get_last_raw_message_by_subject(&push.decision_subject()).await.unwrap();
        assert_eq!(stored.subject.as_str(), format!("ants.approvals.dev-a.{}", push.id));
        assert_eq!(serde_json::from_slice::<ApprovalDecision>(&stored.payload).unwrap(), decision);
        assert_eq!(titles(&pending(&stream, Utc::now()).await.unwrap()), ["rm -rf build"]);

        let again = answer(&human, &stream, &id, Verdict::Reject, Utc::now()).await.unwrap_err();
        assert!(again.to_string().contains("already answered: allow-always"), "{again:#}");
        let expired = answer(&human, &stream, &late.id.to_string(), Verdict::AllowOnce, Utc::now()).await.unwrap_err();
        assert!(expired.to_string().contains("expired"), "{expired:#}");
        let unoffered =
            answer(&human, &stream, &rm.id.to_string(), Verdict::AllowAlways, Utc::now()).await.unwrap_err();
        assert!(unoffered.to_string().contains("choose allow-once|reject"), "{unoffered:#}");
        assert_eq!(super::decision(&stream, &rm).await.unwrap(), None, "an unoffered verdict publishes nothing");
        assert_eq!(state(&stream, &rm, rm.expires_at).await.unwrap(), State::Expired);
    }

    /// dev-b reuses the id of dev-a's request under its own name: a bare id is refused with both qualified ids, and
    /// `<persona>/<id>` answers the right one, on that persona's subject.
    #[tokio::test]
    async fn a_persona_qualified_id_picks_among_colliding_ids() {
        let Some(server) = Server::start() else { return };
        let (dev_a, dev_b): (PersonaId, PersonaId) = ("dev-a".parse().unwrap(), "dev-b".parse().unwrap());
        let (_, human) = server.connect(&PersonaId::human()).await;
        let stream = bus::ensure_stream(&human).await.unwrap();
        let minute = Duration::from_secs(60);
        let real = request(&dev_a, "git push", vec![Verdict::AllowOnce], minute);
        let mut copy = request(&dev_b, "rm -rf /", vec![Verdict::AllowOnce], minute);
        copy.id = real.id;
        for (persona, r) in [(&dev_a, &real), (&dev_b, &copy)] {
            let (_, agent) = server.connect(persona).await;
            bus::publish_json(&agent, r.subject(), r, None).await.unwrap();
        }

        let id = real.id.to_string();
        let err = answer(&human, &stream, &id, Verdict::AllowOnce, Utc::now()).await.unwrap_err();
        assert!(
            err.to_string().contains(&format!("2 approval requests match {id}: dev-a/{id}, dev-b/{id};")),
            "{err:#}"
        );

        let decision =
            answer(&human, &stream, &format!("dev-a/{}", &id[..8]), Verdict::AllowOnce, Utc::now()).await.unwrap();
        assert_eq!((&decision.persona, decision.request_id), (&dev_a, real.id));
        let stored = stream.get_last_raw_message_by_subject(&real.decision_subject()).await.unwrap();
        assert_eq!(stored.subject.as_str(), format!("ants.approvals.dev-a.{id}"));
        assert_eq!(titles(&pending(&stream, Utc::now()).await.unwrap()), ["rm -rf /"]);
        assert_eq!(find(&stream, &format!("dev-b/{id}")).await.unwrap(), copy);
    }

    #[test]
    fn a_query_names_a_known_persona_and_a_request_id() {
        let minute = Duration::from_secs(60);
        let dev_a: PersonaId = "dev-a".parse().unwrap();
        let known = vec![request(&dev_a, "git push", vec![], minute)];
        let id = known[0].id.to_string();
        let pick = |text: &str| Query::parse(text).and_then(|q| select(known.clone(), &q)).map_err(|e| e.to_string());
        assert_eq!(
            pick(&format!(" DEV-A/{} ", id.to_uppercase())).unwrap_err(),
            "invalid persona id \"DEV-A\": use 1-64 of a-z, 0-9 and '-', starting with a letter or digit"
        );
        assert_eq!(pick(&format!("dev-a/{}", id.to_uppercase())), Ok(known[0].clone()));
        assert_eq!(pick(&format!("dev-c/{id}")).unwrap_err(), format!("no approval request dev-c/{id}"));
        assert_eq!(pick("0000").unwrap_err(), "no approval request 0000");
        assert_eq!(pick("dev-a/").unwrap_err(), "no request id given");
        assert!(pick("dev-a/dev-b/0").unwrap_err().contains("is not a request id"));
        assert!(pick("/0").unwrap_err().contains("invalid persona id"));
    }

    #[test]
    fn time_left_reads_at_a_glance() {
        let now = Utc::now();
        let secs = |s| left(now + TimeDelta::seconds(s), now);
        assert_eq!([secs(-5), secs(45), secs(245), secs(3 * 3600 + 120)], ["0s", "45s", "4m05s", "3h02m"]);
    }
}
