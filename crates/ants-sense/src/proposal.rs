//! Proposals (requirements ants-x39hk.6.5 and ants-x39hk.6.7): how the ant changes its persona ([`crate::persona`]) and
//! its skills ([`crate::skill`]). A call of `sense_persona` or `sense_skill` changes nothing yet: it writes a proposal,
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
use crate::memory::invalid;
use crate::store::{Items, StoreError, Write};
use crate::tools::Answer;

/// The kind of a proposal's item.
pub const PROPOSAL: &str = "proposal";
/// The hours from a proposal to the dream that may confirm it, at least.
pub const CONFIRM_AFTER_HOURS: i64 = 12;
/// The most characters of a proposal's reason.
pub const MAX_REASON_CHARS: usize = 500;

/// How many times a write is tried again on items changed meanwhile.
const TRIES: usize = 3;

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
        && dream::finding(items, finding).await.map_err(unavailable)?.is_none()
    {
        return Err(Answer::error("not_found", format!("No dream has a finding {finding:?}.")));
    }
    match dream::untrusted(items, context, finding).await.map_err(unavailable)? {
        Some(why) => Err(Answer::error(
            "untrusted",
            format!("Refused: {why}; persona and skills change only on trusted ground."),
        )),
        None => Ok(()),
    }
}

/// Write `proposal`, made in `context`, superseding the pending proposal of its target in the same event. Returns it
/// and the id of the one it superseded.
pub async fn propose(items: &dyn Items, proposal: Proposal) -> Result<(ProposalItem, Option<ItemId>), StoreError> {
    let by = proposal.writer.thread.to_string();
    for _ in 0..TRIES {
        let earlier = pending_for(items, &proposal.target).await?;
        let id = ItemId::new();
        let mut writes = vec![Write::Create { id: id.clone(), kind: PROPOSAL.into(), values: proposal.values() }];
        if let Some(earlier) = &earlier {
            let mut values = Values::new();
            values.insert("status".into(), json!(Status::Superseded));
            values.insert("superseded_by".into(), json!(id));
            writes.push(Write::Change { id: earlier.id().clone(), values, expected: Some(earlier.item.revision) });
        }
        match items.write(&writes, &by).await {
            Ok(mut written) => {
                let item = written.swap_remove(0);
                let made =
                    ProposalItem::of(item).ok_or_else(|| StoreError::Unavailable("unreadable proposal".into()))?;
                return Ok((made, earlier.map(|earlier| earlier.id().clone())));
            }
            Err(StoreError::Conflict { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Err(StoreError::Unavailable("the proposals kept changing meanwhile".into()))
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
    for _ in 0..TRIES {
        let (mut dream, revision) =
            dream::get(items, number).await.map_err(unavailable)?.ok_or_else(|| gone(Dream::id(number)))?;
        if dream.state == State::Done {
            return Err(Answer::error("done", format!("Dream {number} is done: its diary is written.")));
        }
        let found =
            items.get(id).await.map_err(unavailable)?.and_then(ProposalItem::of).ok_or_else(|| gone(id.clone()))?;
        let proposal = &found.proposal;
        let status = match verdict.verdict {
            Judgement::Applied => Status::Confirmed,
            Judgement::Rejected => Status::Rejected,
            Judgement::Deferred => return Ok(found),
        };
        dream.verdicts.retain(|earlier| earlier.finding != verdict.finding);
        dream.verdicts.push(verdict.clone());
        let mut values = Values::new();
        values.insert("verdicts".into(), json!(dream.verdicts));
        let mut writes = vec![Write::Change { id: Dream::id(number), values, expected: Some(revision) }];
        let decided_here = proposal.decided.as_ref().is_some_and(|decided| decided.finding == finding.id);
        match (proposal.status, status) {
            (Status::Pending, _) => {}
            (current, wanted) if current == wanted && decided_here => return Ok(found),
            (Status::Superseded | Status::Rejected, Status::Rejected) => {
                write(items, &writes).await?;
                return Ok(found);
            }
            (current, _) => {
                let mut message = format!("Proposal {id} is {} already", current.name());
                if let Some(by) = &proposal.superseded_by {
                    let _ = write!(message, ", by {by}");
                }
                message.push_str(": reject this finding.");
                return Err(Answer::error("decided", message));
            }
        }
        if status == Status::Confirmed {
            check(items, &found, now).await?;
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
        match items.write(&writes, dream::BY).await {
            Ok(mut written) => {
                let item = written.pop().ok_or_else(|| gone(id.clone()))?;
                return ProposalItem::of(item).ok_or_else(|| gone(id.clone()));
            }
            Err(StoreError::Conflict { .. }) => {}
            Err(e) => return Err(unavailable(e)),
        }
    }
    Err(unavailable(StoreError::Unavailable(format!("dream {number} kept changing meanwhile"))))
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
    let target = items.get(&proposal.target).await.map_err(unavailable)?;
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
        && let Some(confirmed) = target.as_ref().and_then(|target| target.values.get("confirmed"))
        && let Ok(confirmed) = serde_json::from_value::<DateTime<Utc>>(confirmed.clone())
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

async fn write(items: &dyn Items, writes: &[Write]) -> Result<(), Answer> {
    items.write(writes, dream::BY).await.map(drop).map_err(unavailable)
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

fn gone(id: ItemId) -> Answer {
    unavailable(StoreError::NotFound(id))
}

pub(crate) fn unavailable(e: StoreError) -> Answer {
    Answer::error("unavailable", format!("Proposals are not available: {e}."))
}

/// The reason given in `arguments`, sanitized; refused when missing.
pub(crate) fn reason(arguments: &serde_json::Map<String, Value>) -> Result<String, Answer> {
    let given = match arguments.get("reason") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.trim()),
        Some(_) => return Err(invalid("reason must be a string.".into())),
    };
    let sanitized =
        crate::sanitize::sanitize_title(given.unwrap_or_default(), crate::sanitize::Limit::Chars(MAX_REASON_CHARS));
    Ok(crate::memory::sanitized("reason", given, sanitized)?.text)
}

/// The finding named in `arguments`, if any.
pub(crate) fn finding(arguments: &serde_json::Map<String, Value>) -> Result<Option<String>, Answer> {
    match arguments.get("finding") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !text.trim().is_empty() => Ok(Some(text.trim().to_owned())),
        Some(_) => Err(invalid("finding must be a finding's id, such as 3.1.".into())),
    }
}

#[cfg(test)]
pub(crate) mod tests;
