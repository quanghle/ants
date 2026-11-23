//! Reading the tool requests and the decisions on them back from the ANTS stream: every request, where one stands,
//! the pending ones, and the one an id picks.

use std::collections::HashMap;
use std::fmt::Write as _;

use ants_daemon::bus::relayed::{self, GuardedKv as _, Read};
use ants_daemon::records::chains;
use ants_proto::{ApprovalDecision, ApprovalRequest, CHAINS_BUCKET, COLONY_BUCKET, ColonyEntry, UserId, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use chrono::{DateTime, Utc};

use super::State;
use super::assisted::{HeldAct, InboxDecisions, ant_decisions, consented};

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
        if relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer) {
            tracing::warn!(seq = msg.sequence, "skipping a tool request a stream relayed");
            continue;
        }
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
        && let Some(value) =
            kv.guarded_get(ant.as_str()).await.with_context(|| format!("reading {ant} in the colony"))?
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
            Ok(_) if relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer) => {
                tracing::warn!(seq = msg.sequence, "ignoring a decision a stream relayed");
            }
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
pub(super) async fn state_at(
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
pub(super) async fn standing(
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
pub(super) async fn pending_at(
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
