//! `ants tools`: the tool requests ants escalate to the humans, and their answers.
//!
//! Requests and decisions both live in the ANTS stream, so the pending ones are read back by subject, without consuming
//! anything: every human sees every request on `ants.approvals.<ant>` that has not expired and has no decision on
//! `ants.approvals.<ant>.<request id>`. An expired request needs no answer: its ant already rejected it. The
//! ant of a request is the one its subject names, as credentials pin it there; a request whose body names another
//! is skipped with a warning.
//!
//! Any human decides, and the first decision wins: a decision is stored only while the request's subject holds none
//! ([`answer`]), so of two humans answering at once, the second is told the request was already answered. The ant
//! acts on the stored decision, not on the first it hears, since a refused publish still reaches subscribers.
//!
//! An agent of the ant's escalate_to may decide too: its decision travels as an envelope to the ant's inbox,
//! `ants.inbox.<ant>.<agent>`, where humans read it back. A request such a decision came to first, as the stream
//! ordered them and before the request expired, is decided too ([`State::DecidedByAgent`]), and a human's later answer
//! is refused. The chain is the ant's roster entry's, else the one kept in `ants_chains`, as the ant takes a
//! decision only from its own chain.
//!
//! Request ids are unique only per ant: any ant can publish a request reusing another's id. So a request is
//! written `<ant>/<id>`, and a bare id (or start of one) matching requests of several ants is refused with
//! the qualified ids to choose from.

use std::collections::HashMap;

use ants_daemon::chains;
use ants_proto::{
    ApprovalDecision, ApprovalRequest, CHAINS_BUCKET, Envelope, ROSTER_BUCKET, RosterEntry, STREAM, UserId, Uuid,
    Verdict, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt as _;

/// How a verdict is printed.
pub(crate) fn spelling(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::AllowOnce => "allow-once",
        Verdict::AllowAlways => "allow-always",
        Verdict::Reject => "reject",
    }
}

fn options(request: &ApprovalRequest) -> String {
    request.options.iter().map(|v| spelling(*v)).collect::<Vec<_>>().join("|")
}

/// The command giving `verdict` on `request`.
fn command(request: &ApprovalRequest, verdict: Verdict) -> String {
    let id = qualified(request);
    match verdict {
        Verdict::AllowOnce => format!("ants tools approve {id}"),
        Verdict::AllowAlways => format!("ants tools approve {id} --always"),
        Verdict::Reject => format!("ants tools reject {id}"),
    }
}

/// The commands answering `request` with the verdicts it offers, separated by ` | `.
fn commands(request: &ApprovalRequest) -> String {
    request.options.iter().map(|v| command(request, *v)).collect::<Vec<_>>().join(" | ")
}

/// Where a request stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Pending,
    /// A human's decision.
    Decided(ApprovalDecision),
    /// The decision of an agent of the ant's escalate_to, the one named.
    DecidedByAgent(UserId, ApprovalDecision),
    /// No decision in time; the ant rejected it.
    Expired,
}

/// Every approval request in the stream, oldest first, with its stream sequence.
pub async fn requests(stream: &Stream) -> Result<Vec<(u64, ApprovalRequest)>> {
    let mut requests = Vec::new();
    let mut from = 1;
    loop {
        let msg = match stream.get_first_raw_message_by_subject(subject::ALL_APPROVAL_REQUESTS, from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => return Ok(requests),
            Err(e) => return Err(e).context("reading tool requests"),
        };
        from = msg.sequence + 1;
        match serde_json::from_slice::<ApprovalRequest>(&msg.payload) {
            Ok(request) => match request.check_subject(&msg.subject) {
                Ok(()) => requests.push((msg.sequence, request)),
                Err(e) => tracing::warn!(seq = msg.sequence, error = %e, "skipping a request for another ant"),
            },
            Err(e) => tracing::warn!(seq = msg.sequence, error = %e, "skipping unreadable tool request"),
        }
    }
}

/// The escalate_to of `ant`, whose agents may decide its requests: its roster entry's, else the chain kept in
/// `ants_chains`, else none.
async fn chain_of(js: &jetstream::Context, ant: &UserId) -> Result<Vec<UserId>> {
    if let Ok(kv) = js.get_key_value(ROSTER_BUCKET).await
        && let Some(value) = kv.get(ant.as_str()).await.with_context(|| format!("reading {ant} in the roster"))?
    {
        match serde_json::from_slice::<RosterEntry>(&value).map_err(anyhow::Error::from).and_then(|entry| {
            entry.check_key(ant.as_str())?;
            Ok(entry)
        }) {
            Ok(entry) => return Ok(entry.escalate_to),
            Err(e) => tracing::warn!(%ant, error = %e, "skipping a roster entry"),
        }
    }
    match js.get_key_value(CHAINS_BUCKET).await {
        Ok(kv) => Ok(chains::read(&kv, ant).await?.unwrap_or_default()),
        Err(_) => Ok(Vec::new()),
    }
}

/// A decision an agent of the requester's escalate_to sent to its inbox.
#[derive(Debug, Clone)]
struct AgentDecision {
    by: UserId,
    decision: ApprovalDecision,
    sent_at: DateTime<Utc>,
}

impl AgentDecision {
    /// The decision as `request`'s ant takes it, if it came in time: a verdict not offered counts as a rejection.
    fn on(&self, request: &ApprovalRequest) -> Option<(UserId, ApprovalDecision)> {
        if self.sent_at >= request.expires_at {
            return None;
        }
        let decision =
            ApprovalDecision { decided_at: self.decision.decided_at, ..request.decide(self.decision.verdict) };
        Some((self.by.clone(), decision))
    }
}

/// The first decision on each request of `ant` that an agent of `chain` sent to its inbox, by request id, from
/// stream sequence `from` up to `until` (excluded), as far as the stream holds messages.
async fn agent_decisions(
    stream: &Stream,
    ant: &UserId,
    chain: &[UserId],
    from: u64,
    until: Option<u64>,
) -> Result<HashMap<Uuid, AgentDecision>> {
    let mut found = HashMap::new();
    if chain.is_empty() {
        return Ok(found);
    }
    let inbox = subject::inbox_of(ant);
    let mut next = from;
    while until.is_none_or(|until| next < until) {
        let msg = match stream.get_first_raw_message_by_subject(&inbox, next).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => break,
            Err(e) => return Err(e).with_context(|| format!("reading the inbox of {ant}")),
        };
        next = msg.sequence + 1;
        if until.is_some_and(|until| msg.sequence >= until) {
            break;
        }
        let Ok(envelope) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
        let Some(decision) = &envelope.approval else { continue };
        // The subject pins the sender; the ant takes decisions on its own requests from its own chain only.
        let ours = &envelope.to == ant && &decision.ant == ant;
        if ours && envelope.check_subject(&msg.subject).is_ok() && envelope.is_from_chain(chain) {
            found.entry(decision.request_id).or_insert_with(|| AgentDecision {
                by: envelope.from.clone(),
                decision: (**decision).clone(),
                sent_at: envelope.sent_at,
            });
        }
    }
    Ok(found)
}

/// The decision that counts on `request`, the first stored, if any.
pub async fn decision(stream: &Stream, request: &ApprovalRequest) -> Result<Option<ApprovalDecision>> {
    Ok(decisions(stream, request).await?.0)
}

/// The first stored decision on `request`, if any, and the sequence of the last message on its subject before it (0
/// for none).
async fn decisions(stream: &Stream, request: &ApprovalRequest) -> Result<(Option<ApprovalDecision>, u64)> {
    let mut last = 0;
    loop {
        let msg = match stream.get_first_raw_message_by_subject(&request.decision_subject(), last + 1).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => return Ok((None, last)),
            Err(e) => return Err(e).with_context(|| format!("reading the decision on request {}", request.id)),
        };
        last = msg.sequence;
        match serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
            Ok(d) if d.request_id == request.id && d.ant == request.ant => return Ok((Some(d), last)),
            // The ant ignores these too, so the request is still open.
            _ => tracing::warn!(seq = msg.sequence, "ignoring a malformed decision"),
        }
    }
}

/// Where `request`, stored at stream sequence `seq`, stands at `now`.
pub async fn state(
    js: &jetstream::Context,
    stream: &Stream,
    seq: u64,
    request: &ApprovalRequest,
    now: DateTime<Utc>,
) -> Result<State> {
    Ok(state_at(js, stream, seq, request, now).await?.0)
}

/// [`state`], and the sequence of the last message on the request's decision subject (0 for none).
async fn state_at(
    js: &jetstream::Context,
    stream: &Stream,
    seq: u64,
    request: &ApprovalRequest,
    now: DateTime<Utc>,
) -> Result<(State, u64)> {
    let (decision, last) = decisions(stream, request).await?;
    // An agent's decision counts only before a human's: the ant takes the first.
    let until = decision.is_some().then_some(last);
    let chain = chain_of(js, &request.ant).await?;
    let agent = agent_decisions(stream, &request.ant, &chain, seq + 1, until).await?;
    let state = match (agent.get(&request.id).and_then(|d| d.on(request)), decision) {
        (Some((by, decision)), _) => State::DecidedByAgent(by, decision),
        (None, Some(decision)) => State::Decided(decision),
        (None, None) if request.is_expired(now) => State::Expired,
        (None, None) => State::Pending,
    };
    Ok((state, last))
}

/// The requests still waiting for a decision at `now`, oldest first: neither expired nor decided by a human or an agent
/// of the ant's escalate_to.
pub async fn pending(js: &jetstream::Context, stream: &Stream, now: DateTime<Utc>) -> Result<Vec<ApprovalRequest>> {
    let mut undecided = Vec::new();
    for (seq, request) in requests(stream).await? {
        // Expired ones need no decision lookup.
        if !request.is_expired(now) && decision(stream, &request).await?.is_none() {
            undecided.push((seq, request));
        }
    }
    // One read of each ant's inbox, from its oldest undecided request on.
    let mut decided: HashMap<UserId, HashMap<Uuid, AgentDecision>> = HashMap::new();
    for (seq, request) in &undecided {
        if decided.contains_key(&request.ant) {
            continue;
        }
        let chain = chain_of(js, &request.ant).await?;
        let found = agent_decisions(stream, &request.ant, &chain, seq + 1, None).await?;
        decided.insert(request.ant.clone(), found);
    }
    Ok(undecided
        .into_iter()
        .map(|(_, request)| request)
        .filter(|r| decided.get(&r.ant).and_then(|d| d.get(&r.id)).and_then(|d| d.on(r)).is_none())
        .collect())
}

/// How a request is written on the command line: `<ant>/<id>`. Ant names and ids never contain `/`.
pub fn qualified(request: &ApprovalRequest) -> String {
    format!("{}/{}", request.ant, request.id)
}

/// What `ants tools` was given to pick a request: a start of its id, and the ant when written `<ant>/<id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub ant: Option<UserId>,
    pub id: String,
}

impl Query {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        let (ant, id) = match text.split_once('/') {
            Some((ant, id)) => (Some(ant.parse::<UserId>()?), id),
            None => (None, text),
        };
        let id = id.to_ascii_lowercase();
        if id.is_empty() {
            bail!("no request id given");
        }
        if !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
            bail!("{id} is not a request id: hex digits and dashes only");
        }
        Ok(Self { ant, id })
    }

    fn matches(&self, request: &ApprovalRequest) -> bool {
        self.ant.as_ref().is_none_or(|p| *p == request.ant) && request.id.to_string().starts_with(&self.id)
    }
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.ant {
            Some(ant) => write!(f, "{ant}/{}", self.id),
            None => f.write_str(&self.id),
        }
    }
}

/// The one request among `requests` that `query` picks. When several match, the error lists them qualified.
pub fn select(requests: Vec<ApprovalRequest>, query: &Query) -> Result<ApprovalRequest> {
    const LISTED: usize = 10;
    let mut found: Vec<_> = requests.into_iter().filter(|r| query.matches(r)).collect();
    match found.len() {
        0 => bail!("no tool request {query}"),
        1 => Ok(found.remove(0)),
        n => {
            let mut names = found.iter().take(LISTED).map(qualified).collect::<Vec<_>>().join(", ");
            if n > LISTED {
                names.push_str(&format!(" and {} more", n - LISTED));
            }
            bail!("{n} tool requests match {query}: {names}; give more of the id or name the ant as <ant>/<id>")
        }
    }
}

/// The one request `id` picks: a unique start of its id, optionally written `<ant>/<id>`. Returns it with its stream
/// sequence.
pub async fn find(stream: &Stream, id: &str) -> Result<(u64, ApprovalRequest)> {
    let query = Query::parse(id)?;
    let stored = requests(stream).await?;
    let request = select(stored.iter().map(|(_, r)| r.clone()).collect(), &query)?;
    let seq = stored.iter().find(|(_, r)| *r == request).map_or(0, |(seq, _)| *seq);
    Ok((seq, request))
}

/// Answer the pending request `id` picks (see [`find`]), and wait until JetStream has stored the decision. The
/// decision is stored only while the request has none, so another human's that came first wins over it.
pub async fn answer(
    js: &jetstream::Context,
    stream: &Stream,
    id: &str,
    verdict: Verdict,
    now: DateTime<Utc>,
) -> Result<ApprovalDecision> {
    let (seq, request) = find(stream, id).await?;
    answer_request(js, stream, seq, &request, verdict, now).await
}

/// Answer `request`, stored at stream sequence `seq`, as [`answer`] does.
pub async fn answer_request(
    js: &jetstream::Context,
    stream: &Stream,
    seq: u64,
    request: &ApprovalRequest,
    verdict: Verdict,
    now: DateTime<Utc>,
) -> Result<ApprovalDecision> {
    let last = check(js, stream, seq, request, verdict, now).await?;
    decide(js, stream, request, verdict, last).await
}

fn answered(request: &ApprovalRequest, decision: &ApprovalDecision) -> anyhow::Error {
    anyhow::anyhow!("request {} was already answered: {}", qualified(request), spelling(decision.verdict))
}

/// Check that `request`, stored at stream sequence `seq`, is pending and offers `verdict`. Returns the sequence of the
/// last message on its decision subject, for [`decide`].
async fn check(
    js: &jetstream::Context,
    stream: &Stream,
    seq: u64,
    request: &ApprovalRequest,
    verdict: Verdict,
    now: DateTime<Utc>,
) -> Result<u64> {
    let last = match state_at(js, stream, seq, request, now).await? {
        (State::Pending, last) => last,
        (State::Decided(d), _) => return Err(answered(request, &d)),
        (State::DecidedByAgent(by, d), _) => {
            bail!("request {} was already answered by {by}: {}", qualified(request), spelling(d.verdict))
        }
        (State::Expired, _) => {
            bail!("request {} expired at {}; {} rejected it", qualified(request), request.expires_at, request.ant)
        }
    };
    if !request.options.contains(&verdict) {
        bail!("{} is not offered for request {}; answer {}", spelling(verdict), qualified(request), commands(request));
    }
    Ok(last)
}

/// Store the decision `verdict` on `request` unless a message came on its decision subject after `last` ([`check`]).
async fn decide(
    js: &jetstream::Context,
    stream: &Stream,
    request: &ApprovalRequest,
    verdict: Verdict,
    last: u64,
) -> Result<ApprovalDecision> {
    let decision = request.decide(verdict);
    let message = jetstream::message::PublishMessage::build()
        .payload(serde_json::to_vec(&decision)?.into())
        .expected_last_subject_sequence(last);
    let stored = match js.send_publish(decision.subject(), message).await {
        Ok(ack) => ack.await,
        Err(e) => Err(e),
    };
    match stored {
        Ok(_) => Ok(decision),
        Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => match self::decision(stream, request).await? {
            Some(first) => Err(answered(request, &first)),
            None => Err(e).context("publishing the decision: the request changed meanwhile; try again"),
        },
        Err(e) => Err(e).context("publishing the decision"),
    }
}

/// Time left before `expires_at`, such as `4m05s`.
pub(crate) fn left(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> String {
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

/// One line of the pending list: `<ant>/<id>`, thread, tool, files, options and time left.
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
        "request  {}\nant      {}\nthread   {}\ntool     {}: {}\n",
        request.id, request.ant, request.thread_id, request.tool.kind, request.tool.title
    );
    for location in &request.tool.locations {
        out.push_str(&format!("file     {location}\n"));
    }
    if let Some(input) = &request.tool.input {
        out.push_str(&format!("input    {input}\n"));
    }
    out.push_str(&format!("asked    {}\n", request.requested_at.format("%Y-%m-%d %H:%M:%S")));
    match state {
        State::Pending => {
            out.push_str(&format!("expires  in {}\nanswer   {}", left(request.expires_at, now), commands(request)))
        }
        State::Decided(d) => {
            out.push_str(&format!("answered {} at {}", spelling(d.verdict), d.decided_at.format("%Y-%m-%d %H:%M:%S")))
        }
        State::DecidedByAgent(by, d) => out.push_str(&format!(
            "answered {} by {by} at {}",
            spelling(d.verdict),
            d.decided_at.format("%Y-%m-%d %H:%M:%S")
        )),
        State::Expired => {
            out.push_str(&format!("expired  at {}, rejected", request.expires_at.format("%Y-%m-%d %H:%M:%S")))
        }
    }
    out
}

/// `ants tools list`: the pending requests, then with `follow` new ones as they come, until interrupted.
pub async fn list(nats: &crate::Nats<'_>, follow: bool, json: bool) -> Result<()> {
    let (client, js) = nats.connect("ants-tools").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let print = |request: &ApprovalRequest| -> Result<()> {
        if json {
            println!("{}", serde_json::to_string(request)?);
        } else {
            println!("{}", row(request, Utc::now()));
        }
        Ok(())
    };
    // Subscribe before listing, so a request published in between is not missed.
    let mut live = if follow { Some(client.subscribe(subject::ALL_APPROVAL_REQUESTS).await?) } else { None };
    let listed = pending(&js, &stream, Utc::now()).await?;
    if listed.is_empty() && !follow && !json {
        println!("no pending tool requests");
    }
    listed.iter().try_for_each(print)?;
    if let Some(live) = &mut live {
        while let Some(msg) = live.next().await {
            let Ok(request) = serde_json::from_slice::<ApprovalRequest>(&msg.payload) else { continue };
            if let Err(e) = request.check_subject(&msg.subject) {
                tracing::warn!(error = %e, "skipping a request for another ant");
            } else if !request.is_expired(Utc::now())
                && !listed.iter().any(|r| r.id == request.id && r.ant == request.ant)
            {
                print(&request)?;
            }
        }
    }
    Ok(())
}

/// `ants tools show ID`: one request in full, with where it stands.
pub async fn show(nats: &crate::Nats<'_>, id: &str, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-tools").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let (seq, request) = find(&stream, id).await?;
    if json {
        println!("{}", serde_json::to_string(&request)?);
    } else {
        let now = Utc::now();
        let state = state(&js, &stream, seq, &request, now).await?;
        println!("{}", details(&request, &state, now));
    }
    Ok(())
}

/// `ants tools approve ID [--always]` and `ants tools reject ID`: answer one request with `verdict`.
pub async fn decide_request(nats: &crate::Nats<'_>, id: &str, verdict: Verdict) -> Result<()> {
    let (_, js) = nats.connect("ants-tools").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let decision = answer(&js, &stream, id, verdict, Utc::now()).await?;
    println!("answered request {}/{}: {}", decision.ant, decision.request_id, spelling(decision.verdict));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_daemon::bus;
    use ants_proto::{Role, ThreadId, ToolCall};

    use super::*;
    use crate::auth::tests::Server;

    fn request(ant: &UserId, title: &str, offered: Vec<Verdict>, timeout: Duration) -> ApprovalRequest {
        let tool = ToolCall { kind: "execute".into(), title: title.into(), ..Default::default() };
        ApprovalRequest::new(ant.clone(), ThreadId::generate(), tool, offered, timeout)
    }

    fn titles(requests: &[ApprovalRequest]) -> Vec<&str> {
        requests.iter().map(|r| r.tool.title.as_str()).collect()
    }

    /// Runs as the human and dev-a with their own credentials, so it also checks their permissions.
    #[tokio::test]
    async fn pending_requests_are_listed_and_answered() {
        let Some(server) = Server::start() else { return };
        let dev_a: UserId = "dev-a".parse().unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
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
        let dev_b: UserId = "dev-b".parse().unwrap();
        let (_, intruder) = server.connect(&dev_b).await;
        let forged = request(&dev_a, "git push --force", vec![Verdict::AllowAlways], minute);
        bus::publish_json(&intruder, subject::approvals(&dev_b), &forged, None).await.unwrap();
        assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["git push", "rm -rf build"]);
        let err = answer(&human, &stream, &forged.id.to_string(), Verdict::AllowAlways, Utc::now()).await.unwrap_err();
        assert!(err.to_string().contains("no tool request"), "{err:#}");

        let id = push.id.to_string();
        let decision = answer(&human, &stream, &id[..id.len() - 4], Verdict::AllowAlways, Utc::now()).await.unwrap();
        assert_eq!((decision.request_id, decision.verdict), (push.id, Verdict::AllowAlways));
        let stored = stream.get_last_raw_message_by_subject(&push.decision_subject()).await.unwrap();
        assert_eq!(stored.subject.as_str(), format!("ants.approvals.dev-a.{}", push.id));
        assert_eq!(serde_json::from_slice::<ApprovalDecision>(&stored.payload).unwrap(), decision);
        assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["rm -rf build"]);

        let again = answer(&human, &stream, &id, Verdict::Reject, Utc::now()).await.unwrap_err();
        assert!(again.to_string().contains("already answered: allow-always"), "{again:#}");
        let expired = answer(&human, &stream, &late.id.to_string(), Verdict::AllowOnce, Utc::now()).await.unwrap_err();
        assert!(expired.to_string().contains("expired"), "{expired:#}");
        let unoffered =
            answer(&human, &stream, &rm.id.to_string(), Verdict::AllowAlways, Utc::now()).await.unwrap_err();
        let rm_id = qualified(&rm);
        assert_eq!(
            unoffered.to_string(),
            format!(
                "allow-always is not offered for request {rm_id}; answer ants tools approve {rm_id} | \
                 ants tools reject {rm_id}"
            )
        );
        assert_eq!(super::decision(&stream, &rm).await.unwrap(), None, "an unoffered verdict publishes nothing");
        let (seq, _) = find(&stream, &rm.id.to_string()).await.unwrap();
        assert_eq!(state(&human, &stream, seq, &rm, rm.expires_at).await.unwrap(), State::Expired);
    }

    /// dev-b reuses the id of dev-a's request under its own name: a bare id is refused with both qualified ids, and
    /// `<ant>/<id>` answers the right one, on that ant's subject.
    #[tokio::test]
    async fn an_ant_qualified_id_picks_among_colliding_ids() {
        let Some(server) = Server::start() else { return };
        let (dev_a, dev_b): (UserId, UserId) = ("dev-a".parse().unwrap(), "dev-b".parse().unwrap());
        let (_, human) = server.connect(&UserId::human()).await;
        let stream = bus::ensure_stream(&human).await.unwrap();
        let minute = Duration::from_secs(60);
        let real = request(&dev_a, "git push", vec![Verdict::AllowOnce], minute);
        let mut copy = request(&dev_b, "rm -rf /", vec![Verdict::AllowOnce], minute);
        copy.id = real.id;
        for (ant, r) in [(&dev_a, &real), (&dev_b, &copy)] {
            let (_, agent) = server.connect(ant).await;
            bus::publish_json(&agent, r.subject(), r, None).await.unwrap();
        }

        let id = real.id.to_string();
        let err = answer(&human, &stream, &id, Verdict::AllowOnce, Utc::now()).await.unwrap_err();
        assert!(err.to_string().contains(&format!("2 tool requests match {id}: dev-a/{id}, dev-b/{id};")), "{err:#}");

        let decision =
            answer(&human, &stream, &format!("dev-a/{}", &id[..8]), Verdict::AllowOnce, Utc::now()).await.unwrap();
        assert_eq!((&decision.ant, decision.request_id), (&dev_a, real.id));
        let stored = stream.get_last_raw_message_by_subject(&real.decision_subject()).await.unwrap();
        assert_eq!(stored.subject.as_str(), format!("ants.approvals.dev-a.{id}"));
        assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["rm -rf /"]);
        assert_eq!(find(&stream, &format!("dev-b/{id}")).await.unwrap().1, copy);
    }

    /// Two humans with their own credentials see the same pending request; when both answer it, the first decision
    /// is the one stored and the second is refused, even when both checked it was pending before either answered.
    #[tokio::test]
    async fn every_human_sees_every_request_and_the_first_decision_wins() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a): (UserId, UserId, UserId) =
            ("alice".parse().unwrap(), "bob".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, a_js) = server.connect_as(&alice, Role::Human).await;
        let (_, b_js) = server.connect_as(&bob, Role::Human).await;
        let a_stream = bus::ensure_stream(&a_js).await.unwrap();
        let b_stream = bus::ensure_stream(&b_js).await.unwrap();
        let (agent_client, agent) = server.connect(&dev_a).await;
        let push = request(&dev_a, "git push", vec![Verdict::AllowOnce, Verdict::Reject], Duration::from_secs(60));
        let mut heard = agent_client.subscribe(push.decision_subject()).await.unwrap();
        agent_client.flush().await.unwrap();
        bus::publish_json(&agent, push.subject(), &push, None).await.unwrap();
        for (js, stream) in [(&a_js, &a_stream), (&b_js, &b_stream)] {
            assert_eq!(titles(&pending(js, stream, Utc::now()).await.unwrap()), ["git push"]);
        }

        let (seq, _) = find(&a_stream, &push.id.to_string()).await.unwrap();
        let a_last = check(&a_js, &a_stream, seq, &push, Verdict::AllowOnce, Utc::now()).await.unwrap();
        let b_last = check(&b_js, &b_stream, seq, &push, Verdict::Reject, Utc::now()).await.unwrap();
        let first = decide(&a_js, &a_stream, &push, Verdict::AllowOnce, a_last).await.unwrap();
        let late = decide(&b_js, &b_stream, &push, Verdict::Reject, b_last).await.unwrap_err();
        assert!(late.to_string().contains("already answered: allow-once"), "{late:#}");
        let again = answer(&b_js, &b_stream, &push.id.to_string(), Verdict::Reject, Utc::now()).await.unwrap_err();
        assert!(again.to_string().contains("already answered: allow-once"), "{again:#}");
        for (js, stream) in [(&a_js, &a_stream), (&b_js, &b_stream)] {
            assert_eq!(state(js, stream, seq, &push, Utc::now()).await.unwrap(), State::Decided(first.clone()));
            assert!(pending(js, stream, Utc::now()).await.unwrap().is_empty());
        }
        // The ant hears alice's decision, and acts on the one stored, which is alice's too.
        let msg = tokio::time::timeout(Duration::from_secs(5), heard.next()).await.unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<ApprovalDecision>(&msg.payload).unwrap(), first);
        let agent_stream = agent.get_stream_no_info(STREAM).await.unwrap();
        let stored = agent_stream.direct_get_last_for_subject(push.decision_subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<ApprovalDecision>(&stored.payload).unwrap(), first);
    }

    /// lead is in dev-a's escalate_to, kept in `ants_chains` as dev-a's roster entry expired; dev-c is not. Each sends
    /// decisions with its own credentials to dev-a's inbox.
    #[tokio::test]
    async fn a_request_an_agent_of_the_chain_decided_is_no_longer_pending() {
        let Some(server) = Server::start() else { return };
        let [dev_a, lead, dev_c]: [UserId; 3] = ["dev-a", "lead", "dev-c"].map(|p| p.parse().unwrap());
        let (_, human) = server.connect(&UserId::human()).await;
        let stream = bus::ensure_stream(&human).await.unwrap();
        bus::ensure_chains(&human).await.unwrap();
        let (_, agent) = server.connect(&dev_a).await;
        ants_daemon::chains::publish(&agent, &dev_a, std::slice::from_ref(&lead)).await.unwrap();
        let minute = Duration::from_secs(60);
        let offered = || vec![Verdict::AllowOnce, Verdict::Reject];
        let [push, rm, cat] =
            ["git push", "rm -rf build", "cat secrets"].map(|t| request(&dev_a, t, offered(), minute));
        for r in [&push, &rm, &cat] {
            bus::publish_json(&agent, r.subject(), r, None).await.unwrap();
        }
        let decide_as = async |js: &jetstream::Context, from: &UserId, r: &ApprovalRequest, verdict| {
            let envelope = Envelope::decision(
                r.thread_id.clone(),
                from.clone(),
                ApprovalDecision::new(r.id, r.ant.clone(), verdict),
            );
            bus::publish_json(js, envelope.subject(), &envelope, None).await.unwrap();
        };
        let (_, lead_js) = server.connect(&lead).await;
        let (_, intruder) = server.connect(&dev_c).await;
        decide_as(&lead_js, &lead, &push, Verdict::AllowOnce).await;
        decide_as(&intruder, &dev_c, &rm, Verdict::AllowOnce).await;
        answer(&human, &stream, &cat.id.to_string(), Verdict::Reject, Utc::now()).await.unwrap();
        decide_as(&lead_js, &lead, &cat, Verdict::AllowOnce).await;
        assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["rm -rf build"]);

        let err = answer(&human, &stream, &push.id.to_string(), Verdict::Reject, Utc::now()).await.unwrap_err();
        assert_eq!(err.to_string(), format!("request dev-a/{} was already answered by lead: allow-once", push.id));
        assert_eq!(super::decision(&stream, &push).await.unwrap(), None, "nothing stored for the human");
        let state_of = async |r: &ApprovalRequest| {
            let (seq, _) = find(&stream, &qualified(r)).await.unwrap();
            state(&human, &stream, seq, r, Utc::now()).await.unwrap()
        };
        let State::DecidedByAgent(by, decision) = state_of(&push).await else { panic!("decided by lead") };
        assert_eq!((by, decision.request_id, decision.verdict), (lead.clone(), push.id, Verdict::AllowOnce));
        let State::Decided(first) = state_of(&cat).await else { panic!("the human's came first") };
        assert_eq!(first.verdict, Verdict::Reject);
        assert_eq!(state_of(&rm).await, State::Pending, "dev-c is not in dev-a's chain");
    }

    #[test]
    fn a_query_names_a_known_ant_and_a_request_id() {
        let minute = Duration::from_secs(60);
        let dev_a: UserId = "dev-a".parse().unwrap();
        let known = vec![request(&dev_a, "git push", vec![], minute)];
        let id = known[0].id.to_string();
        let pick = |text: &str| Query::parse(text).and_then(|q| select(known.clone(), &q)).map_err(|e| e.to_string());
        assert_eq!(
            pick(&format!(" DEV-A/{} ", id.to_uppercase())).unwrap_err(),
            "invalid user id \"DEV-A\": use 1-64 of a-z, 0-9 and '-', starting with a letter or digit"
        );
        assert_eq!(pick(&format!("dev-a/{}", id.to_uppercase())), Ok(known[0].clone()));
        assert_eq!(pick(&format!("dev-c/{id}")).unwrap_err(), format!("no tool request dev-c/{id}"));
        assert_eq!(pick("0000").unwrap_err(), "no tool request 0000");
        assert_eq!(pick("dev-a/").unwrap_err(), "no request id given");
        assert!(pick("dev-a/dev-b/0").unwrap_err().contains("is not a request id"));
        assert!(pick("/0").unwrap_err().contains("invalid user id"));
    }

    #[test]
    fn time_left_reads_at_a_glance() {
        let now = Utc::now();
        let secs = |s| left(now + TimeDelta::seconds(s), now);
        assert_eq!([secs(-5), secs(45), secs(245), secs(3 * 3600 + 120)], ["0s", "45s", "4m05s", "3h02m"]);
    }
}
