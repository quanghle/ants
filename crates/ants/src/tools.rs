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
//! is refused. The chain is the ant's colony entry's, else the one kept in `ants_chains`, as the ant takes a
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
//! A human's assistant decides for its human with such an envelope too, claiming to act for the human
//! (`on_behalf_of`): the requester holds the decision until the human consents to it, by confirming it (`ants assistant
//! confirm`) or by a pre-allow rule covering the call (`ants assistant allow`), as the daemon's `approvals` module
//! tells. A request with such a decision the human consented to before it expired is decided too, by the assistant for
//! the human ([`State::DecidedByAnt`] with the decision's `on_behalf_of` set); one still waiting for consent stays
//! pending, with its held decisions ([`held`]). The human's records are read as stored: only the human's credentials
//! write them, and the requester's daemon verifies them before it applies anything.
//!
//! `ants tools reject --note` sends the ant a note on the request's thread once the rejection is stored
//! ([`send_note`]), as ACP's answer to a permission request carries no message.
//!
//! `ants tools list --follow` and `ants inbox --follow` follow the requests alike ([`follow`]); the inbox also shows
//! how each ends.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;

use crate::connection::Nats;
use ants_daemon::records::chains;
use ants_daemon::{bus, config, permission};
use ants_proto::{
    Act, ActAnswer, ApprovalDecision, ApprovalRequest, AssistantGrant, CHAINS_BUCKET, COLONY_BUCKET, ColonyEntry,
    Confirmations, Envelope, Following, HUMANS_BUCKET, PreAllow, STREAM, UserId, Uuid, Verdict, assistant_grant_key,
    confirmations_key, pre_allow_key, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use chrono::{DateTime, TimeDelta, Utc};

mod following;

pub use following::{Update, follow, render};

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
    /// The decision of an ant of the requester's escalate_to, the one named; or of a human's assistant, named, for
    /// the human its `on_behalf_of` names, who consented to it.
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

/// The escalate_to of `ant`, whose ants may decide its requests: its colony entry's, else the chain kept in
/// `ants_chains`, else none.
async fn chain_of(js: &jetstream::Context, ant: &UserId) -> Result<Vec<UserId>> {
    if let Ok(kv) = js.get_key_value(COLONY_BUCKET).await
        && let Some(value) = kv.get(ant.as_str()).await.with_context(|| format!("reading {ant} in the colony"))?
    {
        match serde_json::from_slice::<ColonyEntry>(&value).map_err(anyhow::Error::from).and_then(|entry| {
            entry.check_key(ant.as_str())?;
            Ok(entry)
        }) {
            Ok(entry) => return Ok(entry.escalate_to),
            Err(e) => tracing::warn!(%ant, error = %e, "skipping a colony entry"),
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

/// A decision an assistant sent to the requester's inbox for its human, which the requester holds until the human
/// consents to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldAct {
    pub human: UserId,
    pub assistant: UserId,
    pub verdict: Verdict,
    pub sent_at: DateTime<Utc>,
}

impl HeldAct {
    /// The act as the human answers it.
    pub fn act(&self, request: &ApprovalRequest) -> Act {
        Act::Decision { ant: request.ant.clone(), request_id: request.id, verdict: self.verdict }
    }
}

/// The decisions in an ant's inbox on its requests, by request id.
#[derive(Debug, Default)]
struct InboxDecisions {
    /// The first of an ant of its escalate_to on each.
    chain: HashMap<Uuid, AntDecision>,
    /// The latest of each assistant for its human on each, sent before the request expired.
    held: HashMap<Uuid, Vec<HeldAct>>,
}

/// The decisions on the requests of `ant` sent to its inbox by an ant of `chain` or by an assistant for its human,
/// from stream sequence `from` up to `until` (excluded), as far as the stream holds messages.
async fn ant_decisions(
    stream: &Stream,
    ant: &UserId,
    chain: &[UserId],
    from: u64,
    until: Option<u64>,
) -> Result<InboxDecisions> {
    let mut found = InboxDecisions::default();
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
        let ours = &envelope.to == ant && &decision.ant == ant && envelope.check_subject(&msg.subject).is_ok();
        if !ours {
            continue;
        }
        if let Some(human) = &envelope.on_behalf_of {
            let held = found.held.entry(decision.request_id).or_default();
            held.retain(|h| !(h.human == *human && h.assistant == envelope.from));
            let (human, assistant) = (human.clone(), envelope.from.clone());
            held.push(HeldAct { human, assistant, verdict: decision.verdict, sent_at: envelope.sent_at });
        } else if envelope.is_from_chain(chain) {
            found.chain.entry(decision.request_id).or_insert_with(|| AntDecision {
                by: envelope.from.clone(),
                decision: (**decision).clone(),
                sent_at: envelope.sent_at,
            });
        }
    }
    Ok(found)
}

/// What a human stored of its assistant and its consent to the assistant's acts, unverified.
#[derive(Debug, Default)]
struct Consent {
    assistant: Option<UserId>,
    confirmations: Option<Confirmations>,
    pre_allow: Option<PreAllow>,
}

impl Consent {
    /// What `human` stored in the humans bucket `kv`; nothing for what is missing or malformed.
    async fn read(kv: Option<&jetstream::kv::Store>, human: &UserId) -> Result<Self> {
        let Some(kv) = kv else { return Ok(Self::default()) };
        let grant: Option<AssistantGrant> = record(kv, &assistant_grant_key(human)).await?;
        Ok(Self {
            assistant: grant.and_then(|g| g.assistant),
            confirmations: record(kv, &confirmations_key(human)).await?,
            pre_allow: record(kv, &pre_allow_key(human)).await?,
        })
    }

    /// Whether the human consents to `held` on `request`, by an answer or a pre-allow rule standing before the request
    /// expired; None while it has not answered and no rule covers it.
    fn answer(&self, request: &ApprovalRequest, held: &HeldAct) -> Option<ActAnswer> {
        if held.sent_at >= request.expires_at || self.assistant.as_ref() != Some(&held.assistant) {
            return Some(ActAnswer::Deny);
        }
        let confirmed = self.confirmations.as_ref().filter(|c| c.issued_at < request.expires_at);
        if let Some(answer) = confirmed.and_then(|c| c.answer(&held.assistant, &held.act(request))) {
            return Some(answer);
        }
        let rules = self.pre_allow.as_ref().filter(|r| r.assistant == held.assistant);
        let covered =
            rules.is_some_and(|r| r.rules_for(&request.ant).any(|rule| permission::covers(rule, &request.tool)));
        (covered && held.verdict != Verdict::AllowAlways).then_some(ActAnswer::Confirm)
    }
}

/// The record under `key` in the humans bucket `kv`; none when missing or malformed.
pub(crate) async fn record<T: serde::de::DeserializeOwned>(kv: &jetstream::kv::Store, key: &str) -> Result<Option<T>> {
    let value = kv.get(key).await.with_context(|| format!("reading {key} in {HUMANS_BUCKET}"))?;
    Ok(value.and_then(|v| serde_json::from_slice(&v).ok()))
}

/// Of `held`, the decisions on `request` assistants made for their humans, the one its human consented to, as a
/// decision by the assistant for the human; and those still waiting for consent.
async fn consented(
    js: &jetstream::Context,
    request: &ApprovalRequest,
    held: &[HeldAct],
) -> Result<(Option<(UserId, ApprovalDecision)>, Vec<HeldAct>)> {
    if held.is_empty() {
        return Ok((None, Vec::new()));
    }
    let kv = js.get_key_value(HUMANS_BUCKET).await.inspect_err(|e| tracing::debug!("{HUMANS_BUCKET} unread: {e}")).ok();
    let mut waiting = Vec::new();
    for act in held {
        match Consent::read(kv.as_ref(), &act.human).await?.answer(request, act) {
            Some(ActAnswer::Confirm) => {
                let decision =
                    request.decide(act.verdict).by(Some(act.human.clone())).on_behalf(Some(act.human.clone()));
                let decision = ApprovalDecision { decided_at: act.sent_at, ..decision };
                return Ok((Some((act.assistant.clone(), decision)), Vec::new()));
            }
            Some(ActAnswer::Deny) => {}
            None => waiting.push(act.clone()),
        }
    }
    Ok((None, waiting))
}

/// Who made `decision`, which `by` sent: `by`, or `by for <human>` when an assistant made it for its human.
pub(crate) fn deciders(by: &UserId, decision: &ApprovalDecision) -> String {
    match &decision.on_behalf_of {
        Some(human) => format!("{by} for {human}"),
        None => by.to_string(),
    }
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
    let (state, last, _) = standing(js, stream, seq, request, now).await?;
    Ok((state, last))
}

/// [`state_at`], and the decisions assistants made on a pending request for their humans that wait for consent.
async fn standing(
    js: &jetstream::Context,
    stream: &Stream,
    seq: u64,
    request: &ApprovalRequest,
    now: DateTime<Utc>,
) -> Result<(State, u64, Vec<HeldAct>)> {
    let (decision, last) = decisions(stream, request).await?;
    // A chain ant's decision counts only before a human's: the ant takes the first.
    let until = decision.is_some().then_some(last);
    let chain = chain_of(js, &request.ant).await?;
    let found = ant_decisions(stream, &request.ant, &chain, seq + 1, until).await?;
    if let Some((by, decision)) = found.chain.get(&request.id).and_then(|d| d.on(request)) {
        return Ok((State::DecidedByAnt(by, decision), last, Vec::new()));
    }
    if let Some(decision) = decision {
        return Ok((State::Decided(decision), last, Vec::new()));
    }
    let held = found.held.get(&request.id).map_or(&[][..], Vec::as_slice);
    let (state, held) = match consented(js, request, held).await? {
        (Some((by, decision)), _) => (State::DecidedByAnt(by, decision), Vec::new()),
        (None, _) if request.is_expired(now) => (State::Expired, Vec::new()),
        (None, held) => (State::Pending, held),
    };
    Ok((state, last, held))
}

/// The requests still waiting for a decision at `now`, oldest first: neither expired nor decided by a human, an ant
/// of the ant's escalate_to or an assistant for its human who consented.
pub async fn pending(js: &jetstream::Context, stream: &Stream, now: DateTime<Utc>) -> Result<Vec<ApprovalRequest>> {
    Ok(pending_at(js, stream, now).await?.into_iter().map(|(_, request, _)| request).collect())
}

/// The pending requests at `now` with decisions assistants made for their humans, which wait for the humans' consent.
pub async fn held(
    js: &jetstream::Context,
    stream: &Stream,
    now: DateTime<Utc>,
) -> Result<Vec<(ApprovalRequest, Vec<HeldAct>)>> {
    let pending = pending_at(js, stream, now).await?;
    Ok(pending.into_iter().filter(|(.., held)| !held.is_empty()).map(|(_, request, held)| (request, held)).collect())
}

/// [`pending`], each with its stream sequence and the decisions held on it.
async fn pending_at(
    js: &jetstream::Context,
    stream: &Stream,
    now: DateTime<Utc>,
) -> Result<Vec<(u64, ApprovalRequest, Vec<HeldAct>)>> {
    let mut undecided = Vec::new();
    for (seq, request) in requests(stream).await? {
        // Expired ones need no decision lookup.
        if !request.is_expired(now) && decision(stream, &request).await?.is_none() {
            undecided.push((seq, request));
        }
    }
    // One read of each ant's inbox, from its oldest undecided request on.
    let mut decided: HashMap<UserId, InboxDecisions> = HashMap::new();
    for (seq, request) in &undecided {
        if decided.contains_key(&request.ant) {
            continue;
        }
        let chain = chain_of(js, &request.ant).await?;
        let found = ant_decisions(stream, &request.ant, &chain, seq + 1, None).await?;
        decided.insert(request.ant.clone(), found);
    }
    let mut pending = Vec::new();
    for (seq, request) in undecided {
        let found = decided.get(&request.ant);
        if found.and_then(|d| d.chain.get(&request.id)).and_then(|d| d.on(&request)).is_some() {
            continue;
        }
        let held = found.and_then(|d| d.held.get(&request.id)).map_or(&[][..], Vec::as_slice);
        if let (None, held) = consented(js, &request, held).await? {
            pending.push((seq, request, held));
        }
    }
    Ok(pending)
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
    let by = decision.by.as_ref().map_or_default(|by| format!(" by {by}"));
    anyhow::anyhow!("request {} was already answered{by}: {}", qualified(request), spelling(decision.verdict))
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
            let by = deciders(&by, &d);
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
    if let Some(always) = request.label(Verdict::AllowAlways) {
        let _ = writeln!(out, "always   {always}");
    }
    let _ = writeln!(out, "asked    {}", request.requested_at.format("%Y-%m-%d %H:%M:%S"));
    let _ = match state {
        State::Pending => write!(out, "expires  in {}\nanswer   {}", left(request.expires_at, now), commands(request)),
        State::Decided(d) => {
            write!(out, "answered {} at {}", spelling(d.verdict), d.decided_at.format("%Y-%m-%d %H:%M:%S"))
        }
        State::DecidedByAnt(by, d) => {
            let (by, at) = (deciders(by, d), d.decided_at.format("%Y-%m-%d %H:%M:%S"));
            write!(out, "answered {} by {by} at {at}", spelling(d.verdict))
        }
        State::Expired => write!(out, "expired  at {}, rejected", request.expires_at.format("%Y-%m-%d %H:%M:%S")),
    };
    out
}

/// `ants tools list`: the pending requests, then with `follow` new ones as they come, until interrupted.
pub async fn list(nats: &Nats<'_>, listing: crate::connection::Listing) -> Result<()> {
    let crate::connection::Listing { follow, json } = listing;
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
        Update::Held(..) | Update::Resolved(..) => Ok(()),
    }));
    // Ctrl-C ends the following cleanly, so the device's presence goes at once.
    let follow = async {
        tokio::select! {
            followed = follow => followed,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    };
    let (me, device) = (&nats.me.id, nats.device);
    crate::people::presence::while_following(&js, nats.domain, me, device, Following::Approvals, follow).await
}

/// `ants tools show ID`: one request in full, with where it stands.
pub async fn show(nats: &Nats<'_>, id: &str, json: bool) -> Result<()> {
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

/// Send `note` on the thread of `request` to its ant as `me`'s message, as `ants ask --thread` does: as any human
/// message there, it steers the prompt running and renews the thread's turn budget. Returns the message, where it
/// was stored, and the warnings for an ant no daemon serves now.
pub async fn send_note(
    js: &jetstream::Context,
    domain: Option<&ants_proto::JsDomain>,
    me: &crate::people::humans::Me,
    request: &ApprovalRequest,
    note: String,
) -> Result<(Envelope, bus::Stored, Vec<String>)> {
    use crate::cmd::send;
    let envelope = send::ask_envelope(me, Some(request.thread_id.clone()), request.ant.clone(), note);
    let (warnings, _) = send::unserved(js, me, &envelope.to).await;
    let stored = send::deliver(js, domain, &envelope).await?;
    Ok((envelope, stored, warnings))
}

/// `ants tools approve ID [--always] [--rule]` and `ants tools reject ID [--note TEXT]`: answer one request with
/// `verdict`. With `rule`, the config file there, also add the allow rule fitting the request to its ant's allow list,
/// then reload the daemon serving the file; for an ant added from a template, whose entry may set no allow list, only
/// answer. With `note`, send it to the ant on the request's thread once the answer is stored ([`send_note`]); not when
/// the answer is refused.
pub async fn decide_request(
    nats: &Nats<'_>,
    id: &str,
    verdict: Verdict,
    rule: Option<&Path>,
    note: Option<String>,
) -> Result<()> {
    if note.as_deref().is_some_and(|n| n.trim().is_empty()) {
        bail!("the note is empty: give its text, or reject without --note");
    }
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
    let decision = match answer_request(&js, &stream, seq, &request, verdict, &nats.me.id, Utc::now()).await {
        Ok(decision) => decision,
        Err(e) if note.is_some() => bail!("{e:#}; the note was not sent"),
        Err(e) => return Err(e),
    };
    println!("answered request {}/{}: {}", decision.ant, decision.request_id, spelling(decision.verdict));
    let ant = &request.ant;
    if let Some(note) = note {
        let (envelope, stored, warnings) = send_note(&js, nats.domain, &nats.me, &request, note)
            .await
            .with_context(|| format!("request {} is rejected, but the note was not sent", qualified(&request)))?;
        warnings.iter().for_each(|w| tracing::warn!("{w}"));
        if stored == bus::Stored::Outbox {
            println!("{}", crate::cmd::send::OUTBOX);
        }
        println!("sent the note to {ant} on thread {} (message {})", envelope.thread_id, envelope.id);
    }
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
