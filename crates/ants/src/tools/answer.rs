//! Answering a tool request: the decision stored only while the request has none, and the allow rule suggested to a
//! human approving the same call again and again.

use ants_client::text::spelling;
use ants_daemon::bus::relayed::{self, Read};
use ants_daemon::{bus, permission};
use ants_proto::{ApprovalDecision, ApprovalRequest, UserId, Verdict, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use chrono::{DateTime, Utc};

use super::State;
use super::assisted::deciders;
use super::requests::{decision, qualified, state_at};
use super::text::commands;

/// A human's answer to a tool request.
pub struct Answer<'a> {
    /// The stream sequence the request is stored at.
    pub seq: u64,
    pub request: &'a ApprovalRequest,
    pub verdict: Verdict,
    /// The human answering.
    pub by: &'a UserId,
    /// When: an expired request takes no answer.
    pub now: DateTime<Utc>,
}

/// Give `answer`, and wait until JetStream has stored the decision. The decision is stored only while the request has
/// none, so another human's that came first wins over it.
pub async fn answer_request(js: &jetstream::Context, stream: &Stream, answer: &Answer<'_>) -> Result<ApprovalDecision> {
    let Answer { request, verdict, by, .. } = *answer;
    let last = check(js, stream, answer).await?;
    decide(js, stream, request, request.decide(verdict).by(Some(by.clone())), last).await
}

fn answered(request: &ApprovalRequest, decision: &ApprovalDecision) -> anyhow::Error {
    let by = decision.by.as_ref().map_or_default(|by| format!(" by {by}"));
    anyhow::anyhow!("request {} was already answered{by}: {}", qualified(request), spelling(decision.verdict))
}

/// Check that the request of `answer` is pending and offers its verdict. Returns the sequence of the last message on
/// its decision subject, for [`decide`].
pub(super) async fn check(js: &jetstream::Context, stream: &Stream, answer: &Answer<'_>) -> Result<u64> {
    let Answer { seq, request, verdict, now, .. } = *answer;
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
        (State::Cancelled(at), _) => {
            bail!(
                "request {} is gone: the prompt of {} that asked was cancelled at {at}",
                qualified(request),
                request.ant
            )
        }
    };
    if !request.options.contains(&verdict) {
        bail!("{} is not offered for request {}; answer {}", spelling(verdict), qualified(request), commands(request));
    }
    Ok(last)
}

/// Store `decision` on `request` unless a message came on its decision subject after `last` ([`check`]).
pub(super) async fn decide(
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
        Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {
            match super::requests::decision(stream, request).await? {
                Some(first) => Err(answered(request, &first)),
                None => Err(e).context("publishing the decision: the request changed meanwhile; try again"),
            }
        }
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
        let same = !relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer)
            && earlier.check_subject(&msg.subject).is_ok()
            && earlier.id != request.id
            && permission::rule_for(&earlier.tool).as_deref() == Some(rule.as_str());
        if same && decision(stream, &earlier).await?.is_some_and(|d| d.verdict != Verdict::Reject) {
            approved_before += 1;
        }
    }
    Ok((approved_before + 1 >= SUGGEST_AFTER).then_some(Suggestion { rule, approved_before }))
}
