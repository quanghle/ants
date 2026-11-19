//! `sense_dream`: the ant's verdicts on the findings of its dream, on the dream thread, and finishing the dream. A
//! verdict on a proposal decides it ([`proposal::decide`]): applied confirms it, rejected discards it. One applying a
//! sage's [`TRAIL`] finding names the trail saved for it on the thread ([`Dream::trails`]).

use std::fmt::Write as _;

use ants_proto::TrailName;
use ants_proto::provider::{BoxFuture, ToolCall};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    BY, Dream, Finding, FindingKind, Judgement, MAX_DREAM_WRITES, MAX_FINDINGS, State, TRAIL, Verdict, close, get,
};
use crate::items::{Item, ItemId, Values};
use crate::memory::{MEMORY, Scope};
use crate::persona::persona_id;
use crate::proposal::{self, ProposalItem, Status};
use crate::sanitize::{Limit, sanitize_title};
use crate::store::{Items, StoreError, Write, read_modify_write};
use crate::tools::{Answer, MAX_REASON_CHARS};
use crate::{Sense, records, skill};

/// The JSON Schema of `sense_dream`'s arguments.
pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "verdicts": {
                "type": "array",
                "maxItems": MAX_FINDINGS,
                "items": {
                    "type": "object",
                    "properties": {
                        "finding": {"type": "string", "description": "The finding's id, such as 3.1."},
                        "verdict": {"enum": ["applied", "rejected", "deferred"]},
                        "reason": {"type": "string", "maxLength": MAX_REASON_CHARS},
                        "items": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "With applied: the ids of the memories or records written or voted \
                                            on, or of the proposal made, among them the item the finding asks for \
                                            (such as the self-model for no_self_model); none for a proposal, which \
                                            applied confirms.",
                        },
                        "trails": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "With applied: the trails saved with save_trail on this dream's thread \
                                            for the finding.",
                        },
                    },
                    "required": ["finding", "verdict", "reason"],
                },
            },
            "done": {"type": "boolean", "description": "Finish the dream: its diary is written."},
        },
    })
}

/// The arguments of a `sense_dream` call.
#[derive(Deserialize)]
struct Request {
    #[serde(default)]
    verdicts: Vec<GivenVerdict>,
    #[serde(default)]
    done: bool,
}

/// A verdict as a call gives it.
#[derive(Deserialize)]
struct GivenVerdict {
    finding: Option<String>,
    verdict: Option<Judgement>,
    reason: Option<String>,
    items: Option<Vec<String>>,
    trails: Option<Vec<String>>,
}

/// Answers a `sense_dream` call.
pub fn dream<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { dream_in(sense, call).await.unwrap_or_else(|refused| refused) })
}

async fn dream_in(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let context = &call.context;
    let items = sense.items(&context.ant).await?;
    let (dream, _) = super::last(&*items).await?.ok_or_else(|| {
        Answer::error("no_dream", "No dream yet: sense begins one once the ant has been idle a while.".into())
    })?;
    if dream.thread.as_ref() != Some(&context.thread) {
        let message = match &dream.thread {
            Some(thread) => format!("Dream {} is answered on its own thread, {thread}.", dream.number),
            None => format!("Dream {} has not woken the ant yet.", dream.number),
        };
        return Err(Answer::error("not_dream_thread", message));
    }
    if dream.state == State::Done {
        return Err(finished(&dream));
    }
    let Request { verdicts, done } = crate::tools::arguments(call)?;
    let verdicts = self::verdicts(&*items, &dream, verdicts).await?;
    if verdicts.is_empty() && !done {
        return Ok(show(&dream));
    }
    let now = Utc::now();
    let mut decided = Vec::new();
    for verdict in &verdicts {
        let Some(finding) = dream.findings.iter().find(|finding| finding.id == verdict.finding) else { continue };
        if finding.kind == FindingKind::Proposal && verdict.verdict != Judgement::Deferred {
            decided.push(proposal::decide(&*items, dream.number, finding, verdict, now).await?);
        }
    }
    let mut warning = String::new();
    if decided.iter().any(|found| found.proposal.kind == skill::SKILL && found.proposal.status == Status::Confirmed)
        && let Err(e) = skill::materialize_for(sense, &context.ant).await
    {
        let _ = write!(warning, "\nWarning: the skill files were not written; the next brief tries again: {e}.");
    }
    let (items, dream, verdicts, decided, warning) = (&*items, &dream, &verdicts, &decided, &warning);
    read_modify_write(|| async move {
        let gone = || StoreError::NotFound(Dream::id(dream.number));
        let (mut current, revision) = get(items, dream.number).await?.ok_or_else(gone)?;
        if current.state == State::Done {
            return Ok(Err(finished(&current)));
        }
        for verdict in verdicts {
            current.verdicts.retain(|earlier| earlier.finding != verdict.finding);
            current.verdicts.push(verdict.clone());
        }
        let writes = if done {
            close(current.clone(), revision, context, Utc::now()).to_vec()
        } else {
            let mut values = Values::new();
            values.insert("verdicts".into(), json!(current.verdicts));
            vec![Write::Change { id: Dream::id(current.number), values, expected: Some(revision) }]
        };
        let written = items.write(&writes, BY).await?;
        if done {
            current.state = State::Done;
            current.diary = written.first().map(|diary| diary.id.clone());
        }
        let mut answer = answered(&current, verdicts.len());
        for found in decided {
            let _ = write!(answer.text, "\n{}", found.line());
        }
        answer.text.push_str(warning);
        Ok(Ok(answer))
    })
    .await?
}

/// The verdicts `given` on findings of `dream`, checked.
async fn verdicts(items: &dyn Items, dream: &Dream, given: Vec<GivenVerdict>) -> Result<Vec<Verdict>, Answer> {
    if given.len() > MAX_FINDINGS {
        return Err(Answer::invalid(format!("At most {MAX_FINDINGS} verdicts at a time.")));
    }
    let at = Utc::now();
    let mut verdicts: Vec<Verdict> = Vec::new();
    for given in given {
        let text = |given: Option<String>| given.map(|text| text.trim().to_owned()).filter(|text| !text.is_empty());
        let finding = text(given.finding).ok_or_else(|| Answer::invalid("Each verdict names its finding."))?;
        let finding = finding.as_str();
        let Some(known) = dream.findings.iter().find(|known| known.id == finding) else {
            return Err(Answer::error("not_found", format!("Dream {} has no finding {finding:?}.", dream.number)));
        };
        let verdict = given.verdict.ok_or_else(|| {
            Answer::invalid(format!("Finding {finding}: the verdict is applied, rejected or deferred."))
        })?;
        let reason =
            text(given.reason).ok_or_else(|| Answer::invalid(format!("Finding {finding}: the reason is missing.")))?;
        let reason = sanitize_title(&reason, Limit::Chars(MAX_REASON_CHARS))
            .map_err(|refused| Answer::error("too_long", format!("Finding {finding}: the reason is {refused}.")))?
            .text;
        let mut named = Vec::new();
        for id in given.items.iter().flatten() {
            let id = id
                .trim()
                .parse::<ItemId>()
                .map_err(|_| Answer::invalid(format!("Finding {finding}: items are item ids.")))?;
            let item = items.get(&id).await?;
            named.push(item.ok_or_else(|| Answer::error("not_found", format!("Finding {finding}: no item {id}.")))?);
        }
        let trails = saved(dream, finding, verdict, given.trails.unwrap_or_default())?;
        let mut written: Vec<_> = named.iter().map(|item| item.id.clone()).collect();
        let trail = known.advice.as_ref().is_some_and(|advice| advice.kind == TRAIL);
        if known.kind == FindingKind::Proposal {
            written.clone_from(&known.items);
        } else if verdict == Judgement::Applied && trail && trails.is_empty() {
            return Err(Answer::invalid(format!(
                "Finding {finding} (trail): a trail finding applied names the trail saved for it in trails."
            )));
        } else if verdict == Judgement::Applied && written.is_empty() && trails.is_empty() {
            return Err(Answer::invalid(format!(
                "Finding {finding}: applied names the items written or voted on, the trails saved, or the proposal \
                 made."
            )));
        } else if verdict == Judgement::Applied
            && let Some(asked) = asked(known)
            && !named.iter().any(|item| asked.answered_by(item))
        {
            return Err(Answer::invalid(format!(
                "Finding {finding} ({}): applied names {}; none of the items given is.",
                known.kind, asked.text
            )));
        }
        verdicts.retain(|earlier| earlier.finding != finding);
        verdicts.push(Verdict { finding: finding.to_owned(), verdict, reason, items: written, trails, at });
    }
    Ok(verdicts)
}

/// The trails `given` as saved for `finding` of `dream` by a verdict of `verdict`, checked: each saved on the dream's
/// thread naming that finding or none ([`super::trail_saved`]), and only with [`Judgement::Applied`].
fn saved(dream: &Dream, finding: &str, verdict: Judgement, given: Vec<String>) -> Result<Vec<TrailName>, Answer> {
    if !given.is_empty() && verdict != Judgement::Applied {
        return Err(Answer::invalid(format!("Finding {finding}: trails are named only with applied.")));
    }
    let mut trails: Vec<TrailName> = Vec::new();
    for name in given {
        let name: TrailName =
            name.trim().parse().map_err(|_| Answer::invalid(format!("Finding {finding}: trails are trail names.")))?;
        let for_it = |saved: &&super::SavedTrail| saved.finding.as_deref().is_none_or(|saved| saved == finding);
        if !dream.trails.iter().filter(for_it).any(|saved| saved.name == name) {
            return Err(Answer::error(
                "not_found",
                format!("Finding {finding}: trail {name} was saved on this dream's thread for no such finding."),
            ));
        }
        if !trails.contains(&name) {
            trails.push(name);
        }
    }
    Ok(trails)
}

/// What an applied verdict on a finding names among its items, as the finding asks.
struct Asked {
    /// The records it asks written.
    records: Vec<ItemId>,
    /// The items a proposal answering it changes: the persona or a skill.
    targets: Vec<ItemId>,
    /// Whether a lesson of the practice answers it too: a memory, or a skill proposal.
    lessons: bool,
    /// What it asks, as a refusal tells.
    text: String,
}

impl Asked {
    fn answered_by(&self, item: &Item) -> bool {
        let proposed = ProposalItem::of(item.clone()).map(|found| found.proposal);
        self.records.contains(&item.id)
            || proposed.as_ref().is_some_and(|proposal| self.targets.contains(&proposal.target))
            || self.lessons && (item.kind == MEMORY || proposed.is_some_and(|proposal| proposal.kind == skill::SKILL))
    }
}

/// What an applied verdict on `finding` names among its items, by the finding's kind: for one on the identity, the
/// record or proposal it asks for; for one on a sage's practice, a lesson, the self-model or, for one about an ant
/// reviewed, its relationship record. None for a finding any item answers.
fn asked(finding: &Finding) -> Option<Asked> {
    let self_model = records::record_id(&Scope::Own)?;
    let relationship = || {
        let scope: Scope = finding.about()?.parse().ok()?;
        Some((records::record_id(&scope)?, scope))
    };
    let lessons = "a memory, the self-model or a skill proposal written";
    let (records, targets, lessons, text) = match finding.kind {
        FindingKind::NoPersona => {
            (vec![persona_id()], vec![persona_id()], false, "the persona proposal made".to_owned())
        }
        FindingKind::NoSelfModel | FindingKind::StaleSelfModel => {
            (vec![self_model], Vec::new(), false, "the self-model written".to_owned())
        }
        FindingKind::NoRelationship | FindingKind::NoReviewedRelationship => {
            let (record, scope) = relationship()?;
            (vec![record], Vec::new(), false, format!("the relationship record of {scope} written"))
        }
        FindingKind::Inherited if finding.items.contains(&self_model) => {
            (vec![self_model], Vec::new(), false, "the self-model written".to_owned())
        }
        FindingKind::Inherited if finding.items.contains(&persona_id()) => {
            (Vec::new(), vec![persona_id()], false, "the persona proposal made".to_owned())
        }
        FindingKind::Inherited => (Vec::new(), finding.items.clone(), false, "the skill proposal made".to_owned()),
        FindingKind::RejectedKind | FindingKind::UndoneFindings | FindingKind::UnlinkedFollowUps => {
            (vec![self_model], Vec::new(), true, lessons.to_owned())
        }
        FindingKind::UnansweredReviews | FindingKind::CostlyReviews => match relationship() {
            Some((record, scope)) => {
                (vec![record, self_model], Vec::new(), true, format!("the relationship record of {scope}, {lessons}"))
            }
            None => (vec![self_model], Vec::new(), true, lessons.to_owned()),
        },
        FindingKind::Contradiction
        | FindingKind::NeedsReview
        | FindingKind::Duplicates
        | FindingKind::Distill
        | FindingKind::Proposal
        | FindingKind::Sage
        | FindingKind::Vitals => return None,
    };
    Some(Asked { records, targets, lessons, text })
}

/// `dream`'s findings and the verdicts on them.
fn show(dream: &Dream) -> Answer {
    let mut text =
        format!("Dream {} ({}), {} of {MAX_DREAM_WRITES} writes made.", dream.number, dream.state, dream.writes_made());
    if dream.findings.is_empty() {
        text.push_str("\nNo findings: remember what the threads teach, then finish with done.");
    }
    for finding in &dream.findings {
        let _ = write!(text, "\n{}", super::packet::finding_line(finding, dream.number));
        if let Some(verdict) = dream.verdict(&finding.id) {
            let _ = write!(text, "\n  {}: {}{}", verdict.verdict, verdict.reason, verdict.named());
        }
    }
    if !dream.trails.is_empty() {
        let saved: Vec<_> = dream
            .trails
            .iter()
            .map(|saved| match &saved.finding {
                Some(finding) => format!("{} (for {finding})", saved.name),
                None => saved.name.to_string(),
            })
            .collect();
        let _ = write!(text, "\nSaved on this thread: {}.", saved.join(", "));
    }
    Answer { text, json: json(dream), is_error: false }
}

/// The answer after recording `recorded` verdicts on `dream`.
fn answered(dream: &Dream, recorded: usize) -> Answer {
    let open: Vec<_> = dream.findings.iter().filter(|finding| dream.verdict(&finding.id).is_none()).collect();
    let mut text = format!("Dream {}: {recorded} verdicts recorded", dream.number);
    if dream.state == State::Done {
        let diary = dream.diary.as_ref().map_or_else(String::new, |diary| format!(" in diary {diary}"));
        let _ = write!(text, "; done{diary}.");
        if !open.is_empty() {
            let _ = write!(text, " {} unanswered findings come again in the next dream.", open.len());
        }
    } else if open.is_empty() {
        text.push_str("; every finding is answered: finish with done.");
    } else {
        let ids: Vec<_> = open.iter().map(|finding| finding.id.as_str()).collect();
        let _ = write!(text, "; open: {}.", ids.join(", "));
    }
    Answer { text, json: json(dream), is_error: false }
}

fn json(dream: &Dream) -> Value {
    json!({
        "dream": dream.number,
        "state": dream.state,
        "findings": dream.findings,
        "verdicts": dream.verdicts,
        "writes": dream.writes_made(),
        "trails": dream.trails,
        "diary": dream.diary,
    })
}

fn finished(dream: &Dream) -> Answer {
    Answer::error("done", format!("Dream {} is done: its diary is written.", dream.number))
}
