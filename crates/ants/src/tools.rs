//! `ants tools`: the tool requests ants escalate to the humans, and their answers.
//!
//! Requests and decisions both live in the ANTS stream, so the pending ones are read back by subject, without consuming
//! anything: every human sees every request on `ants.approvals.<ant>` that has not expired and has no decision on
//! `ants.approvals.<ant>.<request id>`. An expired request needs no answer: its ant already rejected it. The
//! ant of a request is the one its subject names, as credentials pin it there; a request whose body names another
//! is skipped with a warning.
//!
//! Any human decides, and the first decision wins: a decision is stored only while the request's subject holds none
//! ([`answer_request`]), so of two humans answering at once, the second is told the request was already answered.
//! The ant acts on the stored decision, not on the first it hears, since a refused publish still reaches subscribers.
//!
//! An ant of the requester's escalate_to may decide too: its decision travels as an envelope to the requester's inbox,
//! `ants.inbox.<requester>.<decider>`, where humans read it back. A request such a decision came to first, as the stream
//! ordered them and before the request expired, is decided too ([`State::DecidedByAnt`]), and a human's later answer
//! is refused. The chain is the ant's roster entry's, else the one kept in `ants_chains`, as the ant takes a
//! decision only from its own chain.
//!
//! A human approving the same call of an ant again and again is shown the allow rule that would spare it
//! ([`suggestion`]): once [`SUGGEST_AFTER`] requests of the ant for the same rule ([`permission::rule_for`]) are
//! approved, counting the one at hand, as the stream holds them. `ants tools approve --rule` approves and adds the rule
//! to the ant's allow list in the config file, then reloads the daemon.
//!
//! Request ids are unique only per ant: any ant can publish a request reusing another's id. So a request is
//! written `<ant>/<id>`, and a bare id (or start of one) matching requests of several ants is refused with
//! the qualified ids to choose from.
//!
//! `ants tools list --follow` and `ants inbox --follow` follow the requests alike ([`follow`]); the inbox also shows
//! how each ends.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;

use ants_daemon::{bus, chains, config, permission};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, CHAINS_BUCKET, Envelope, Following, ROSTER_BUCKET, RosterEntry, STREAM, UserId,
    Uuid, Verdict, subject,
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
    /// The decision of an ant of the requester's escalate_to, the one named.
    DecidedByAnt(UserId, ApprovalDecision),
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

/// The escalate_to of `ant`, whose ants may decide its requests: its roster entry's, else the chain kept in
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

/// A decision an ant of the requester's escalate_to sent to its inbox.
#[derive(Debug, Clone)]
struct AntDecision {
    by: UserId,
    decision: ApprovalDecision,
    sent_at: DateTime<Utc>,
}

impl AntDecision {
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

/// The first decision on each request of `ant` that an ant of `chain` sent to its inbox, by request id, from
/// stream sequence `from` up to `until` (excluded), as far as the stream holds messages.
async fn ant_decisions(
    stream: &Stream,
    ant: &UserId,
    chain: &[UserId],
    from: u64,
    until: Option<u64>,
) -> Result<HashMap<Uuid, AntDecision>> {
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
            found.entry(decision.request_id).or_insert_with(|| AntDecision {
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
    // A chain ant's decision counts only before a human's: the ant takes the first.
    let until = decision.is_some().then_some(last);
    let chain = chain_of(js, &request.ant).await?;
    let by_chain = ant_decisions(stream, &request.ant, &chain, seq + 1, until).await?;
    let state = match (by_chain.get(&request.id).and_then(|d| d.on(request)), decision) {
        (Some((by, decision)), _) => State::DecidedByAnt(by, decision),
        (None, Some(decision)) => State::Decided(decision),
        (None, None) if request.is_expired(now) => State::Expired,
        (None, None) => State::Pending,
    };
    Ok((state, last))
}

/// The requests still waiting for a decision at `now`, oldest first: neither expired nor decided by a human or an ant
/// of the ant's escalate_to.
pub async fn pending(js: &jetstream::Context, stream: &Stream, now: DateTime<Utc>) -> Result<Vec<ApprovalRequest>> {
    Ok(pending_at(js, stream, now).await?.into_iter().map(|(_, request)| request).collect())
}

/// [`pending`], each with its stream sequence.
async fn pending_at(
    js: &jetstream::Context,
    stream: &Stream,
    now: DateTime<Utc>,
) -> Result<Vec<(u64, ApprovalRequest)>> {
    let mut undecided = Vec::new();
    for (seq, request) in requests(stream).await? {
        // Expired ones need no decision lookup.
        if !request.is_expired(now) && decision(stream, &request).await?.is_none() {
            undecided.push((seq, request));
        }
    }
    // One read of each ant's inbox, from its oldest undecided request on.
    let mut decided: HashMap<UserId, HashMap<Uuid, AntDecision>> = HashMap::new();
    for (seq, request) in &undecided {
        if decided.contains_key(&request.ant) {
            continue;
        }
        let chain = chain_of(js, &request.ant).await?;
        let found = ant_decisions(stream, &request.ant, &chain, seq + 1, None).await?;
        decided.insert(request.ant.clone(), found);
    }
    Ok(undecided
        .into_iter()
        .filter(|(_, r)| decided.get(&r.ant).and_then(|d| d.get(&r.id)).and_then(|d| d.on(r)).is_none())
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
                let _ = write!(names, " and {} more", n - LISTED);
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
#[cfg(test)]
pub async fn answer(
    js: &jetstream::Context,
    stream: &Stream,
    id: &str,
    verdict: Verdict,
    now: DateTime<Utc>,
) -> Result<ApprovalDecision> {
    let (seq, request) = find(stream, id).await?;
    answer_request(js, stream, seq, &request, verdict, &UserId::human(), now).await
}

/// Answer `request`, stored at stream sequence `seq`, as [`answer`] does, as the human `by`.
pub async fn answer_request(
    js: &jetstream::Context,
    stream: &Stream,
    seq: u64,
    request: &ApprovalRequest,
    verdict: Verdict,
    by: &UserId,
    now: DateTime<Utc>,
) -> Result<ApprovalDecision> {
    let last = check(js, stream, seq, request, verdict, now).await?;
    decide(js, stream, request, request.decide(verdict).by(Some(by.clone())), last).await
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
        (State::DecidedByAnt(by, d), _) => {
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

/// Store `decision` on `request` unless a message came on its decision subject after `last` ([`check`]).
async fn decide(
    js: &jetstream::Context,
    stream: &Stream,
    request: &ApprovalRequest,
    decision: ApprovalDecision,
    last: u64,
) -> Result<ApprovalDecision> {
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
        Err(e) => Err(bus::explain(js, &decision.subject(), e).await).context("publishing the decision"),
    }
}

/// How many approvals of requests of an ant for the same allow rule, the one at hand included, make the rule suggested.
pub const SUGGEST_AFTER: usize = 3;

/// An allow rule that would have spared the humans a request ([`suggestion`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    pub rule: String,
    /// How many earlier requests of the ant for the rule a human approved.
    pub approved_before: usize,
}

/// The allow rule fitting `request`, stored at stream sequence `seq`, if approving it makes at least
/// [`SUGGEST_AFTER`] approvals of requests of its ant for that rule. Earlier requests count when the stream still holds
/// them and a human's decision allowed them.
pub async fn suggestion(stream: &Stream, seq: u64, request: &ApprovalRequest) -> Result<Option<Suggestion>> {
    let Some(rule) = permission::rule_for(&request.tool) else { return Ok(None) };
    let requests = subject::approvals(&request.ant);
    let (mut approved_before, mut from) = (0, 1);
    while from < seq {
        let msg = match stream.get_first_raw_message_by_subject(&requests, from).await {
            Ok(msg) if msg.sequence < seq => msg,
            Ok(_) => break,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => break,
            Err(e) => return Err(e).with_context(|| format!("reading the tool requests of {}", request.ant)),
        };
        from = msg.sequence + 1;
        let Ok(earlier) = serde_json::from_slice::<ApprovalRequest>(&msg.payload) else { continue };
        let same = earlier.check_subject(&msg.subject).is_ok()
            && earlier.id != request.id
            && permission::rule_for(&earlier.tool).as_deref() == Some(rule.as_str());
        if same && decision(stream, &earlier).await?.is_some_and(|d| d.verdict != Verdict::Reject) {
            approved_before += 1;
        }
    }
    Ok((approved_before + 1 >= SUGGEST_AFTER).then_some(Suggestion { rule, approved_before }))
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
pub(crate) fn details(request: &ApprovalRequest, state: &State, now: DateTime<Utc>) -> String {
    let mut out = format!(
        "request  {}\nant      {}\nthread   {}\ntool     {}: {}\n",
        request.id, request.ant, request.thread_id, request.tool.kind, request.tool.title
    );
    for location in &request.tool.locations {
        let _ = writeln!(out, "file     {location}");
    }
    if let Some(input) = &request.tool.input {
        let _ = writeln!(out, "input    {input}");
    }
    let _ = writeln!(out, "asked    {}", request.requested_at.format("%Y-%m-%d %H:%M:%S"));
    let _ = match state {
        State::Pending => write!(out, "expires  in {}\nanswer   {}", left(request.expires_at, now), commands(request)),
        State::Decided(d) => {
            write!(out, "answered {} at {}", spelling(d.verdict), d.decided_at.format("%Y-%m-%d %H:%M:%S"))
        }
        State::DecidedByAnt(by, d) => {
            write!(out, "answered {} by {by} at {}", spelling(d.verdict), d.decided_at.format("%Y-%m-%d %H:%M:%S"))
        }
        State::Expired => write!(out, "expired  at {}, rejected", request.expires_at.format("%Y-%m-%d %H:%M:%S")),
    };
    out
}

/// What following the tool requests reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Update {
    /// A request waiting for a decision: pending when following started, or new since.
    Pending(ApprovalRequest),
    /// How a request reported pending ended: decided, or expired.
    Resolved(ApprovalRequest, State),
}

/// A request reported pending and not resolved yet.
struct Open {
    /// Its stream sequence, once read: a request heard live is looked up in the stream when its state is needed.
    seq: Option<u64>,
    request: ApprovalRequest,
    /// When to look again at a request that was still pending right after word of a decision on it came, as that
    /// word may come before the stream stores the decision.
    recheck: Option<DateTime<Utc>>,
}

/// How long after word of a decision a request still pending is looked at again.
const RECHECK: TimeDelta = TimeDelta::seconds(1);

/// Report the pending requests, then each new one and how each reported one ends, until the connection closes. Word
/// of a decision comes on the decision subjects (a human's) and in the inbox of each ant with a request reported (an
/// ant's of its escalate_to); a request is looked at again when one names it, and when it expires.
pub async fn follow(
    client: &async_nats::Client,
    js: &jetstream::Context,
    mut report: impl FnMut(&Update) -> Result<()>,
) -> Result<()> {
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    // A request heard live is stored after this.
    let stored = stream.cached_info().state.last_sequence;
    // Subscribe before listing, so a request or decision published in between is not missed.
    let mut asked = client.subscribe(subject::ALL_APPROVAL_REQUESTS).await.context("following tool requests")?;
    let mut decided = client.subscribe(subject::ALL_APPROVAL_DECISIONS).await.context("following decisions")?;
    let mut inboxes = futures::stream::SelectAll::new();
    let mut heard = std::collections::HashSet::new();
    let mut reported = std::collections::HashSet::new();
    let mut open: Vec<Open> = Vec::new();
    let mut fresh: Vec<_> =
        Box::pin(pending_at(js, &stream, Utc::now())).await?.into_iter().map(|(seq, r)| (Some(seq), r)).collect();
    loop {
        for (seq, request) in fresh.drain(..) {
            if !reported.insert((request.ant.clone(), request.id)) {
                continue;
            }
            if heard.insert(request.ant.clone()) {
                let inbox = subject::inbox_of(&request.ant);
                inboxes.push(client.subscribe(inbox).await.context("following decisions of chain ants")?);
            }
            report(&Update::Pending(request.clone()))?;
            open.push(Open { seq, request, recheck: None });
        }
        let wake = open.iter().map(|o| o.recheck.unwrap_or(o.request.expires_at)).min();
        let wake = async {
            match wake {
                Some(at) => tokio::time::sleep((at - Utc::now()).to_std().unwrap_or_default()).await,
                None => std::future::pending().await,
            }
        };
        let mut named = None;
        tokio::select! {
            msg = asked.next() => {
                let Some(msg) = msg else { return Ok(()) };
                match serde_json::from_slice::<ApprovalRequest>(&msg.payload) {
                    Ok(request) => match request.check_subject(&msg.subject) {
                        Err(e) => tracing::warn!(error = %e, "skipping a request for another ant"),
                        Ok(()) if !request.is_expired(Utc::now()) => fresh.push((None, request)),
                        Ok(()) => {}
                    },
                    Err(e) => tracing::warn!(error = %e, "skipping unreadable tool request"),
                }
            }
            msg = decided.next() => {
                let Some(msg) = msg else { return Ok(()) };
                if let Ok(d) = serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
                    named = Some((d.ant, d.request_id));
                }
            }
            Some(msg) = inboxes.next(), if !inboxes.is_empty() => {
                if let Ok(envelope) = serde_json::from_slice::<Envelope>(&msg.payload)
                    && let Some(d) = envelope.approval
                {
                    named = Some((d.ant, d.request_id));
                }
            }
            () = wake => {}
        }
        let now = Utc::now();
        let mut i = 0;
        while i < open.len() {
            let o = &mut open[i];
            let is_named = named.as_ref().is_some_and(|(ant, id)| *ant == o.request.ant && *id == o.request.id);
            let due = is_named || o.request.is_expired(now) || o.recheck.is_some_and(|at| at <= now);
            if !due {
                i += 1;
                continue;
            }
            o.recheck = None;
            match Box::pin(resolution(js, &stream, stored, o, now)).await {
                Ok(State::Pending) if o.request.is_expired(now) => {
                    // Only a request no longer in the stream: an expired one is pending no more.
                    tracing::debug!(request = %qualified(&o.request), "no longer following an expired request");
                    open.remove(i);
                }
                Ok(State::Pending) => {
                    if is_named {
                        o.recheck = Some(now + RECHECK);
                    }
                    i += 1;
                }
                Ok(state) => report(&Update::Resolved(open.remove(i).request, state))?,
                Err(e) if o.request.is_expired(now) => {
                    tracing::warn!(request = %qualified(&o.request), "not telling how the request ended: {e:#}");
                    open.remove(i);
                }
                Err(e) => {
                    tracing::warn!(request = %qualified(&o.request), "reading the state of the request: {e:#}");
                    i += 1;
                }
            }
        }
    }
}

/// Where `open` stands at `now`; a request heard live is looked up among those stored after sequence `stored`.
async fn resolution(
    js: &jetstream::Context,
    stream: &Stream,
    stored: u64,
    open: &mut Open,
    now: DateTime<Utc>,
) -> Result<State> {
    let seq = match open.seq {
        Some(seq) => seq,
        None => {
            let Some(seq) = seq_of(stream, &open.request, stored + 1).await? else { return Ok(State::Pending) };
            *open.seq.insert(seq)
        }
    };
    state(js, stream, seq, &open.request, now).await
}

/// The stream sequence of `request`, looked for from sequence `from` on, if stored.
async fn seq_of(stream: &Stream, request: &ApprovalRequest, mut from: u64) -> Result<Option<u64>> {
    loop {
        let msg = match stream.get_first_raw_message_by_subject(&request.subject(), from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading request {}", qualified(request))),
        };
        if serde_json::from_slice::<ApprovalRequest>(&msg.payload).is_ok_and(|r| r.id == request.id) {
            return Ok(Some(msg.sequence));
        }
        from = msg.sequence + 1;
    }
}

/// An update as `ants inbox --follow` shows it among replies: when, what, and how to answer a pending request.
pub fn render(update: &Update, now: DateTime<Utc>) -> String {
    let local = |at: DateTime<Utc>| at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string();
    match update {
        Update::Pending(r) => {
            let mut out = format!(
                "{}  {} asks to run a tool on thread {}: request {}\n  {}: {}\n",
                local(r.requested_at),
                r.ant,
                r.thread_id,
                qualified(r),
                r.tool.kind,
                excerpt(&r.tool.title, 200)
            );
            if !r.tool.locations.is_empty() {
                let _ = writeln!(out, "  files: {}", r.tool.locations.join(", "));
            }
            let _ = writeln!(out, "  expires in {}; answer with {}", left(r.expires_at, now), commands(r));
            out
        }
        Update::Resolved(r, state) => {
            let (at, how) = match state {
                State::Pending => (now, "is pending".to_owned()),
                State::Decided(d) => (d.decided_at, format!("was answered: {}", spelling(d.verdict))),
                State::DecidedByAnt(by, d) => (d.decided_at, format!("was answered by {by}: {}", spelling(d.verdict))),
                State::Expired => (r.expires_at, format!("expired unanswered: {} rejected it", r.ant)),
            };
            format!("{}  request {} of {} {how}\n", local(at), qualified(r), r.ant)
        }
    }
}

/// `ants tools list`: the pending requests, then with `follow` new ones as they come, until interrupted.
pub async fn list(nats: &crate::Nats<'_>, listing: crate::Listing) -> Result<()> {
    let crate::Listing { follow, json } = listing;
    let (client, js) = nats.connect("ants-tools").await?;
    let print = |request: &ApprovalRequest| -> Result<()> {
        if json {
            println!("{}", serde_json::to_string(request)?);
        } else {
            println!("{}", row(request, Utc::now()));
        }
        Ok(())
    };
    if !follow {
        let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
        let listed = pending(&js, &stream, Utc::now()).await?;
        if listed.is_empty() && !json {
            println!("no pending tool requests");
        }
        return listed.iter().try_for_each(print);
    }
    let follow = Box::pin(self::follow(&client, &js, |update| match update {
        Update::Pending(request) => print(request),
        Update::Resolved(..) => Ok(()),
    }));
    // Ctrl-C ends the following cleanly, so the device's presence goes at once.
    let follow = async {
        tokio::select! {
            followed = follow => followed,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    };
    let (me, device) = (&nats.me.id, nats.device);
    crate::presence::while_following(&js, nats.domain, me, device, Following::Approvals, follow).await
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

/// `ants tools approve ID [--always] [--rule]` and `ants tools reject ID`: answer one request with `verdict`. With
/// `rule`, the config file there, also add the allow rule fitting the request to its ant's allow list, then reload
/// the daemon serving the file; for an ant added from a template, whose entry may set no allow list, only answer.
pub async fn decide_request(nats: &crate::Nats<'_>, id: &str, verdict: Verdict, rule: Option<&Path>) -> Result<()> {
    let (_, js) = nats.connect("ants-tools").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let (seq, request) = find(&stream, id).await?;
    // An ant added from a template takes its allow rules from the template, and its entry may set no `allow`.
    let template = match rule {
        Some(path) => config::template_of(path, &request.ant)?,
        None => None,
    };
    // Checked before answering, so a rule that cannot be added leaves the request pending.
    let edit = match rule {
        Some(_) if template.is_some() => None,
        Some(path) => {
            let rule = permission::rule_for(&request.tool).with_context(|| {
                format!(
                    "no allow rule fits request {} exactly; answer without --rule, and widen the allow list of {} by \
                     hand if needed",
                    qualified(&request),
                    request.ant
                )
            })?;
            let text = config::with_allow_rule(path, &request.ant, &rule)?;
            Some((path, rule, text))
        }
        None => None,
    };
    let decision = answer_request(&js, &stream, seq, &request, verdict, &nats.me.id, Utc::now()).await?;
    println!("answered request {}/{}: {}", decision.ant, decision.request_id, spelling(decision.verdict));
    let ant = &request.ant;
    match edit {
        Some((path, rule, None)) => println!("{rule:?} is in the allow list of {ant} already in {}", path.display()),
        Some((path, rule, Some(text))) => {
            config::write(path, &text)?;
            println!("{rule:?} added to the allow list of {ant} in {}", path.display());
            reload_daemon(path, ant);
        }
        None if verdict == Verdict::Reject => {}
        None if template.is_some() => println!(
            "{ant} is added from template {}, whose allow rules it takes: no rule added to its entry; add one to the \
             template's allow or one of its allow_sets by hand",
            template.as_deref().unwrap_or_default()
        ),
        None => match suggestion(&stream, seq, &request).await {
            Ok(Some(s)) => println!("{}", suggestion_text(&request, &s)),
            Ok(None) => {}
            Err(e) => tracing::warn!(error = format!("{e:#}"), "counting earlier approvals"),
        },
    }
    Ok(())
}

/// What `ants tools approve` prints after approving `request`, for which `suggestion` came.
pub(crate) fn suggestion_text(request: &ApprovalRequest, suggestion: &Suggestion) -> String {
    let ant = &request.ant;
    format!(
        "{ant} was allowed this {} times: the allow rule {:?} in its allow list would spare asking; `ants tools \
         approve ID --rule` on the next such request adds it",
        suggestion.approved_before + 1,
        suggestion.rule
    )
}

/// Reload the daemon serving the config file at `path`, for `ant` to take its new allow list, or say how.
fn reload_daemon(path: &Path, ant: &UserId) {
    use crate::service::{Action, Outcome, Service, Services, failure, manual};
    match Services::local().apply(Service::Daemon, Action::Reload, path) {
        Outcome::Done(command) => {
            println!("daemon reloaded ({command}): {ant} restarts with the rule once its prompts in flight finish");
        }
        Outcome::NotRunning => println!("the daemon service is not running; {ant} has the rule once started"),
        outcome => println!(
            "{}reload `ants daemon` with SIGHUP, or its service ({}), for {ant} to take the rule",
            failure(outcome),
            manual(Service::Daemon, Action::Reload)
        ),
    }
}

#[cfg(test)]
mod tests;
