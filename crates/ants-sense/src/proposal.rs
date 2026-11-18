//! Proposals (requirements ants-x39hk.6.5 and ants-x39hk.6.7): how the ant changes its persona ([`crate::persona`]) and
//! its skills ([`crate::skill`]). A call of `sense_identity` or `sense_skill` changes nothing yet: it writes a proposal,
//! an item of kind [`PROPOSAL`] holding the whole version proposed, its reason and the revision of its target it was
//! made from. A target has at most one pending proposal, a new one superseding it.
//!
//! A dream beginning [`CONFIRM_AFTER_HOURS`] or more after the proposal offers it as a finding
//! ([`crate::dream::proposals`]), and the ant decides it there with `sense_dream` ([`decide`]): applied confirms it,
//! putting its version into its target, rejected discards it and deferred leaves it pending, offered again by the next
//! dream. A proposal whose target changed meanwhile is confirmed no more. Every version stays in the target's revisions,
//! each with its reason. Proposals on a thread of untrusted origin, in answer to untrusted findings, or naming none of
//! the findings of the dream on whose thread they are made when its packet holds untrusted text, are refused
//! ([`crate::dream::untrusted`]).

use std::fmt::Write as _;

use ants_proto::provider::CallContext;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::dream::{self, Dream, Finding, Judgement, State, Verdict};
use crate::items::{Item, ItemId, Values};
use crate::store::{Items, StoreError, Write, read_modify_write};
use crate::tools::{Answer, MAX_REASON_CHARS};

/// The kind of a proposal's item.
pub const PROPOSAL: &str = "proposal";
/// The hours from a proposal to the dream that may confirm it, at least.
pub const CONFIRM_AFTER_HOURS: i64 = 12;

/// Where a proposal stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Awaiting the ant's verdict in a dream.
    Pending,
    /// Confirmed in a dream: its version is its target's.
    Confirmed,
    /// Rejected in a dream: discarded.
    Rejected,
    /// Replaced by a later proposal of the same target before a dream decided it.
    Superseded,
}

impl Status {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Confirmed => "confirmed",
            Self::Rejected => "rejected",
            Self::Superseded => "superseded",
        }
    }
}

/// When and in which dream the ant decided a proposal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decided {
    pub at: DateTime<Utc>,
    pub dream: u64,
    pub finding: String,
}

/// What a confirmed version of a target holds beside its content ([`content`]): the proposal confirmed, when, and
/// the reason it was proposed for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Confirmation {
    pub proposal: Option<ItemId>,
    pub confirmed: Option<DateTime<Utc>>,
    pub reason: String,
}

impl Confirmation {
    /// The confirmation `values`, a version of a target, hold; empty when they hold none.
    pub fn of(values: &Values) -> Self {
        serde_json::from_value(Value::Object(values.clone())).unwrap_or_default()
    }
}

/// A proposal, as its item holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Proposal {
    /// The item it changes: the persona or a skill.
    pub target: ItemId,
    /// The target's kind.
    pub kind: String,
    /// What it proposes, as the dream's finding states it.
    pub summary: String,
    /// The target's values as proposed, null for one removed.
    pub version: Values,
    /// The revision of the target it was made from, 0 for none yet.
    pub base: u64,
    pub reason: String,
    pub status: Status,
    /// Who proposed it, where and when, as ants told for the call.
    pub writer: CallContext,
    /// The finding it answers, if it names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<ItemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided: Option<Decided>,
}

impl Proposal {
    fn values(&self) -> Values {
        match serde_json::to_value(self) {
            Ok(Value::Object(values)) => values,
            _ => unreachable!("a proposal is an object"),
        }
    }

    /// When a dream may offer it and the ant confirm it: [`CONFIRM_AFTER_HOURS`] after it was made.
    pub fn ripe(&self) -> DateTime<Utc> {
        self.writer.at + Duration::hours(CONFIRM_AFTER_HOURS)
    }
}

/// A proposal and the item holding it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProposalItem {
    pub item: Item,
    pub proposal: Proposal,
}

impl ProposalItem {
    pub fn of(item: Item) -> Option<Self> {
        if item.kind != PROPOSAL {
            return None;
        }
        let proposal = serde_json::from_value(Value::Object(item.values.clone())).ok()?;
        Some(Self { item, proposal })
    }

    pub fn id(&self) -> &ItemId {
        &self.item.id
    }

    /// One line: its id, status, when it was made and when it can be confirmed, its reason.
    pub fn line(&self) -> String {
        let proposal = &self.proposal;
        let mut line = format!("Proposal {} ({}, made {}", self.id(), proposal.status.name(), time(proposal.writer.at));
        if proposal.status == Status::Pending {
            let _ = write!(line, ", confirmable in a dream from {}", time(proposal.ripe()));
        }
        if let Some(decided) = &proposal.decided {
            let _ = write!(line, ", decided {} in dream {}", time(decided.at), decided.dream);
        }
        let _ = write!(line, "): {}", proposal.reason);
        line
    }

    /// What a dream's finding says of it.
    pub fn finding(&self) -> String {
        let proposal = &self.proposal;
        let mut text = format!("{} Reason: {}", proposal.summary, proposal.reason);
        let _ = write!(
            text,
            "\nProposal {}, made {}: applied confirms it, rejected discards it, deferred keeps it for the next dream.",
            self.id(),
            time(proposal.writer.at)
        );
        if proposal.kind == crate::skill::SKILL {
            let _ = write!(text, " sense_skill proposal={} shows it in full.", self.id());
        }
        text
    }

    pub fn json(&self) -> Value {
        let mut json = Value::Object(self.item.values.clone());
        json["id"] = json!(self.id());
        json["revision"] = json!(self.item.revision);
        json
    }
}

/// `at` to the second, as a brief writes it.
pub fn time(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// The pending proposals, the oldest first.
pub async fn pending(items: &dyn Items) -> Result<Vec<ProposalItem>, StoreError> {
    let found = items.list_by(PROPOSAL, "status", Some(&json!(Status::Pending)), usize::MAX).await?;
    let mut pending: Vec<_> = found.into_iter().filter_map(ProposalItem::of).collect();
    pending.sort_by(|a, b| (a.proposal.writer.at, a.id()).cmp(&(b.proposal.writer.at, b.id())));
    Ok(pending)
}

/// The pending proposal of `target`, if any.
pub async fn pending_for(items: &dyn Items, target: &ItemId) -> Result<Option<ProposalItem>, StoreError> {
    Ok(pending(items).await?.into_iter().find(|found| &found.proposal.target == target))
}

/// The proposals of `target`, the newest first.
pub async fn of_target(items: &dyn Items, target: &ItemId) -> Result<Vec<ProposalItem>, StoreError> {
    let found = items.list_by(PROPOSAL, "target", Some(&json!(target)), usize::MAX).await?;
    let mut all: Vec<_> = found.into_iter().filter_map(ProposalItem::of).collect();
    all.sort_by(|a, b| (b.proposal.writer.at, b.id()).cmp(&(a.proposal.writer.at, a.id())));
    Ok(all)
}

/// Refuse a proposal made in `context` in answer to `finding`, if given, when it rests on untrusted text
/// ([`dream::untrusted`]) or names a finding no dream has.
pub(crate) async fn refuse_untrusted(
    items: &dyn Items,
    context: &CallContext,
    finding: Option<&str>,
) -> Result<(), Answer> {
    if let Some(finding) = finding
        && dream::finding(items, finding).await?.is_none()
    {
        return Err(Answer::error("not_found", format!("No dream has a finding {finding:?}.")));
    }
    match dream::untrusted(items, context, finding).await? {
        Some(why) => Err(Answer::error(
            "untrusted",
            format!("Refused: {why}; persona and skills change only on trusted ground."),
        )),
        None => Ok(()),
    }
}

/// Write `proposal`, made in `context`, superseding the pending proposal of its target in the same event. Returns it
/// and the id of the one it superseded.
pub async fn propose(items: &dyn Items, mut proposal: Proposal) -> Result<(ProposalItem, Option<ItemId>), StoreError> {
    proposal.version.insert("reason".into(), json!(proposal.reason));
    let (by, proposal) = (&proposal.writer.thread.to_string(), &proposal);
    read_modify_write(|| async move {
        let earlier = pending_for(items, &proposal.target).await?;
        let id = ItemId::new();
        let mut writes = vec![Write::Create { id: id.clone(), kind: PROPOSAL.into(), values: proposal.values() }];
        if let Some(earlier) = &earlier {
            let mut values = Values::new();
            values.insert("status".into(), json!(Status::Superseded));
            values.insert("superseded_by".into(), json!(id));
            writes.push(Write::Change { id: earlier.id().clone(), values, expected: Some(earlier.item.revision) });
        }
        let item = items.write(&writes, by).await?.swap_remove(0);
        let made = ProposalItem::of(item).ok_or_else(|| StoreError::Unavailable("unreadable proposal".into()))?;
        Ok((made, earlier.map(|earlier| earlier.id().clone())))
    })
    .await
}

/// Decide the proposal `finding` of dream `number` offers, as `verdict` says, at `now`: applied confirms it, putting
/// its version into its target, rejected discards it; the proposal, its target and the verdict on the dream are
/// written in one event. A proposal already out of the running is only rejected. Refused when it was made less than
/// [`CONFIRM_AFTER_HOURS`] before `now`, its target changed since, or, for the persona, the persona changed within
/// [`crate::persona::PERSONA_EVERY_HOURS`]. Returns the proposal as decided.
pub(crate) async fn decide(
    items: &dyn Items,
    number: u64,
    finding: &Finding,
    verdict: &Verdict,
    now: DateTime<Utc>,
) -> Result<ProposalItem, Answer> {
    let Some(id) = finding.items.first() else {
        return Err(Answer::error("not_found", format!("Finding {} names no proposal.", finding.id)));
    };
    read_modify_write(|| async move {
        let gone = || StoreError::NotFound(id.clone());
        let (dream, revision) =
            dream::get(items, number).await?.ok_or_else(|| StoreError::NotFound(Dream::id(number)))?;
        if dream.state == State::Done {
            return Ok(Err(Answer::error("done", format!("Dream {number} is done: its diary is written."))));
        }
        let found = items.get(id).await?.and_then(ProposalItem::of).ok_or_else(gone)?;
        let proposal = &found.proposal;
        let status = match verdict.verdict {
            Judgement::Applied => Status::Confirmed,
            Judgement::Rejected => Status::Rejected,
            Judgement::Deferred => return Ok(Ok(found)),
        };
        let mut judged = dream.clone();
        judged.verdicts.retain(|earlier| earlier.finding != verdict.finding);
        judged.verdicts.push(verdict.clone());
        let mut writes = vec![dream.change(&judged, revision)];
        let decided_here = proposal.decided.as_ref().is_some_and(|decided| decided.finding == finding.id);
        match (proposal.status, status) {
            (Status::Pending, _) => {}
            (current, wanted) if current == wanted && decided_here => return Ok(Ok(found)),
            (Status::Superseded | Status::Rejected, Status::Rejected) => {
                items.write(&writes, dream::BY).await?;
                return Ok(Ok(found));
            }
            (current, _) => {
                let mut message = format!("Proposal {id} is {} already", current.name());
                if let Some(by) = &proposal.superseded_by {
                    let _ = write!(message, ", by {by}");
                }
                message.push_str(": reject this finding.");
                return Ok(Err(Answer::error("decided", message)));
            }
        }
        if status == Status::Confirmed {
            if let Err(refused) = check(items, &found, now).await {
                return Ok(Err(refused));
            }
            let mut version = proposal.version.clone();
            version.insert("proposal".into(), json!(id));
            version.insert("confirmed".into(), json!(now));
            version.insert(crate::fork::INHERITED.into(), Value::Null);
            writes.push(Write::Put {
                id: proposal.target.clone(),
                kind: proposal.kind.clone(),
                values: version,
                expected: Some(proposal.base),
            });
        }
        let mut values = Values::new();
        values.insert("status".into(), json!(status));
        values.insert("decided".into(), json!(Decided { at: now, dream: number, finding: finding.id.clone() }));
        writes.push(Write::Change { id: id.clone(), values, expected: Some(found.item.revision) });
        let item = items.write(&writes, dream::BY).await?.pop().ok_or_else(gone)?;
        Ok(Ok(ProposalItem::of(item).ok_or_else(gone)?))
    })
    .await?
}

/// Refuse confirming `found` at `now` when it is not ripe, its target changed since it was made, or it would change
/// the persona twice within [`crate::persona::PERSONA_EVERY_HOURS`].
async fn check(items: &dyn Items, found: &ProposalItem, now: DateTime<Utc>) -> Result<(), Answer> {
    let (id, proposal) = (found.id(), &found.proposal);
    if now < proposal.ripe() {
        let message = format!(
            "Proposal {id} was made {}: it can be confirmed only from {}, in a later dream; defer it.",
            time(proposal.writer.at),
            time(proposal.ripe())
        );
        return Err(Answer::error("too_soon", message));
    }
    let target = items.get(&proposal.target).await?;
    let at = target.as_ref().map_or(0, |target| target.revision);
    if at != proposal.base {
        let message = format!(
            "Proposal {id} was made from revision {} of its target, now at revision {at}: reject it and propose again \
             from the current version.",
            proposal.base
        );
        return Err(Answer::error("stale", message));
    }
    if proposal.kind == crate::persona::PERSONA
        && let Some(confirmed) = target.as_ref().and_then(|target| Confirmation::of(&target.values).confirmed)
        && now < confirmed + Duration::hours(crate::persona::PERSONA_EVERY_HOURS)
    {
        let message = format!(
            "The persona changed {}: it changes at most once in {} hours; defer proposal {id}.",
            time(confirmed),
            crate::persona::PERSONA_EVERY_HOURS
        );
        return Err(Answer::error("too_soon", message));
    }
    Ok(())
}

/// The values of version `values` of a target with its revision's metadata removed: what a proposal compares.
pub fn content(values: &Values) -> Values {
    let mut content = values.clone();
    for meta in ["proposal", "confirmed", "reason", crate::fork::INHERITED] {
        content.remove(meta);
    }
    content.retain(|_, value| !value.is_null());
    content
}

/// Every version of `target` in order, the oldest first: its values after each revision, with the revision and when it
/// was written.
pub async fn versions(items: &dyn Items, target: &ItemId) -> Result<Vec<(u64, DateTime<Utc>, Values)>, StoreError> {
    let mut values = Values::new();
    let mut versions = Vec::new();
    for revision in items.history(target).await? {
        for (name, value) in revision.change.after {
            if value.is_null() {
                values.remove(&name);
            } else {
                values.insert(name, value);
            }
        }
        versions.push((revision.change.revision, revision.at, values.clone()));
    }
    Ok(versions)
}

/// The reason `given`, sanitized; refused when missing.
pub(crate) fn reason(given: Option<&str>) -> Result<String, Answer> {
    let given = given.map(str::trim);
    let sanitized =
        crate::sanitize::sanitize_title(given.unwrap_or_default(), crate::sanitize::Limit::Chars(MAX_REASON_CHARS));
    Ok(crate::memory::sanitized("reason", given, sanitized)?.text)
}

/// The finding `given`, if any.
pub(crate) fn finding(given: Option<&str>) -> Result<Option<String>, Answer> {
    match given.map(str::trim) {
        None => Ok(None),
        Some("") => Err(Answer::invalid("finding must be a finding's id, such as 3.1.")),
        Some(finding) => Ok(Some(finding.to_owned())),
    }
}

#[cfg(test)]
pub(crate) mod tests;
