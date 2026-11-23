//! The decisions others than a human make on a tool request, sent to the requester's inbox: those of the ants of its
//! escalate_to, and those of the humans' assistants, which count once their humans consent to them.

use std::collections::HashMap;

use ants_daemon::bus::relayed::{self, GuardedKv as _, Read};
use ants_daemon::permission;
use ants_proto::{
    Act, ActAnswer, ApprovalDecision, ApprovalRequest, AssistantGrant, Confirmations, Envelope, HUMANS_BUCKET,
    PreAllow, UserId, Uuid, Verdict, assistant_grant_key, confirmations_key, pre_allow_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use async_nats::jetstream::stream::{LastRawMessageErrorKind, Stream};
use chrono::{DateTime, Utc};

/// A decision an ant of the requester's escalate_to sent to its inbox.
#[derive(Debug, Clone)]
pub(super) struct AntDecision {
    by: UserId,
    decision: ApprovalDecision,
    sent_at: DateTime<Utc>,
}

impl AntDecision {
    /// The decision as `request`'s ant takes it, if it came in time: a verdict not offered counts as a rejection.
    pub(super) fn on(&self, request: &ApprovalRequest) -> Option<(UserId, ApprovalDecision)> {
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
pub(super) struct InboxDecisions {
    /// The first of an ant of its escalate_to on each.
    pub(super) chain: HashMap<Uuid, AntDecision>,
    /// The latest of each assistant for its human on each, sent before the request expired.
    pub(super) held: HashMap<Uuid, Vec<HeldAct>>,
}

/// The decisions on the requests of `ant` sent to its inbox by an ant of `chain` or by an assistant for its human,
/// from stream sequence `from` up to `until` (excluded), as far as the stream holds messages.
pub(super) async fn ant_decisions(
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
        if relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer) {
            continue;
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
    let value = kv.guarded_get(key).await.with_context(|| format!("reading {key} in {HUMANS_BUCKET}"))?;
    Ok(value.and_then(|v| serde_json::from_slice(&v).ok()))
}

/// Of `held`, the decisions on `request` assistants made for their humans, the one its human consented to, as a
/// decision by the assistant for the human; and those still waiting for consent.
pub(super) async fn consented(
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
