//! The findings of a dream on the ant's identity ([`super`]): what it lacks for the work it did, gathered from its
//! sense data and thread records alone, never thread text, so never untrusted. Once the ant worked enough threads:
//!
//! - no persona after [`PERSONA_AFTER_THREADS`] threads, and none proposed;
//! - no self-model after [`SELF_MODEL_AFTER_THREADS`] threads;
//! - no relationship record of a human or ant who started [`RELATIONSHIP_AFTER_THREADS`] of its threads or more, one
//!   finding each;
//! - a self-model unchanged over [`STALE_ACTIVE_DAYS`] active days or more in which the ant worked with someone or in a
//!   repository it had not before;
//! - a persona, self-model or skill a clone still holds as inherited ([`crate::fork`]) after [`OWN_AFTER_THREADS`]
//!   threads of its own, and none proposed for it: to write anew as its own, or to keep by rejecting the finding.
//!
//! Each names the `sense_identity` or `sense_skill` call answering it. Its count steps coarsely ([`step`]): a rejected
//! one rests until the count doubles, not at every thread. The numbers are first guesses, to tune on real threads.

use std::collections::HashSet;

use ants_proto::UserId;
use ants_proto::provider::Starter;
use chrono::{DateTime, Utc};

use super::{FindingKind, Gathered};
use crate::fork::INHERITED;
use crate::items::{Item, ItemId};
use crate::memory::Scope;
use crate::skill::{SKILL, Skill};
use crate::store::{Items, StoreError};
use crate::thread::experience::{self, Tally};
use crate::thread::record;
use crate::{identity, persona, proposal, records};

/// The threads after which an ant without a persona is asked to write one.
pub const PERSONA_AFTER_THREADS: u32 = 5;
/// The threads after which an ant without a self-model is asked to write one.
pub const SELF_MODEL_AFTER_THREADS: u32 = 5;
/// The threads a human or ant started after which an ant without their relationship record is asked to write one.
pub const RELATIONSHIP_AFTER_THREADS: u32 = 3;
/// The active days a self-model stays unchanged, someone or somewhere new worked with meanwhile, before the ant is
/// asked to revise it.
pub const STALE_ACTIVE_DAYS: u32 = 30;
/// The threads a clone works after which it is asked to make what it inherited its own.
pub const OWN_AFTER_THREADS: u32 = 5;
/// The most of the people and repositories new to the ant a stale self-model's finding names.
const MAX_NAMED: usize = 5;

/// The findings on the identity of `ant` in `items`: the persona's, then the self-model's, then the relationships',
/// the most threads first, then the skills', by name.
pub(super) async fn gather(items: &dyn Items, ant: &UserId) -> Result<Vec<Gathered>, StoreError> {
    let Some(worked) = experience::experience(items, ant, usize::MAX).await? else { return Ok(Vec::new()) };
    let proposed = proposal::pending(items).await?.into_iter().map(|found| found.proposal.target).collect();
    let from = identity::card(items).await?.and_then(|card| card.cloned_from);
    let source = from.map_or_else(|| "its source".into(), |from| from.ant.to_string());
    let ground = Ground { threads: worked.threads, proposed, source };
    let mut found = Vec::new();
    found.extend(persona(items, &ground).await?);
    found.extend(self_model(items, ant, &ground).await?);
    found.extend(relationships(items, &worked.people).await?);
    found.extend(skills(items, &ground).await?);
    Ok(found)
}

/// What the findings rest on besides the items they are about.
struct Ground {
    /// The threads the ant worked.
    threads: u32,
    /// The targets of the pending proposals.
    proposed: Vec<ItemId>,
    /// The ant a clone was cloned from.
    source: String,
}

impl Ground {
    /// The finding that `item`, the `what` answered by `call`, is still as a clone inherited it, about `about`; none
    /// before [`OWN_AFTER_THREADS`], when no longer inherited or when a change of it is proposed.
    fn inherited(&self, about: &str, what: &str, item: &Item, call: &str) -> Option<Gathered> {
        let (threads, source) = (self.threads, &self.source);
        if threads < OWN_AFTER_THREADS || !item.values.contains_key(INHERITED) || self.proposed.contains(&item.id) {
            return None;
        }
        let text = format!(
            "The {what} is still as inherited on clone from {source}, after {threads} threads of the ant's own: write \
             it anew as the ant's own with {call}, or reject this finding to keep it as it is."
        );
        Some(on(FindingKind::Inherited, about, item, step(threads, OWN_AFTER_THREADS), text))
    }
}

async fn persona(items: &dyn Items, ground: &Ground) -> Result<Option<Gathered>, StoreError> {
    let call = "sense_identity, giving values, voice, boundaries, working_style and profile, a reason and this \
                finding's id as finding, then answer applied naming the proposal made";
    let threads = ground.threads;
    Ok(match persona::current(items).await?.filter(|item| !persona::text(&item.values).is_empty()) {
        Some(item) => ground.inherited("persona", "persona", &item, call),
        None if threads >= PERSONA_AFTER_THREADS && !ground.proposed.contains(&persona::persona_id()) => {
            let text = format!("No persona after {threads} threads: write one with {call}.");
            Some(subject(FindingKind::NoPersona, "persona", step(threads, PERSONA_AFTER_THREADS), text))
        }
        None => None,
    })
}

async fn self_model(items: &dyn Items, ant: &UserId, ground: &Ground) -> Result<Option<Gathered>, StoreError> {
    let call = |revision: Option<u64>| {
        let revision = revision.map_or_else(String::new, |revision| format!(", revision {revision}"));
        format!(
            "sense_identity, giving self_model (what the ant holds true of itself), a title{revision}, then answer \
             applied naming the record written"
        )
    };
    let threads = ground.threads;
    let Some(model) = records::self_model(items).await? else {
        if threads < SELF_MODEL_AFTER_THREADS {
            return Ok(None);
        }
        let text = format!("No self-model after {threads} threads: write one with {}.", call(None));
        return Ok(Some(subject(FindingKind::NoSelfModel, "self", step(threads, SELF_MODEL_AFTER_THREADS), text)));
    };
    let (item, revision) = (&model.item, model.item.revision);
    if model.record.inherited.is_some() {
        return Ok(ground.inherited("self", "self-model", item, &call(Some(revision))));
    }
    let since = item.updated.at;
    let Some(lately) = experience::experience_since(items, ant, since, usize::MAX).await? else { return Ok(None) };
    if lately.active_days < STALE_ACTIVE_DAYS {
        return Ok(None);
    }
    let known = known(items, since).await?;
    let new: Vec<_> =
        lately.people.iter().chain(&lately.repos).map(|tally| &tally.scope).filter(|s| !known.contains(*s)).collect();
    if new.is_empty() {
        return Ok(None);
    }
    let mut named: Vec<_> = new.iter().take(MAX_NAMED).map(ToString::to_string).collect();
    if new.len() > MAX_NAMED {
        named.push(format!("{} more", new.len() - MAX_NAMED));
    }
    let text = format!(
        "The self-model, revision {revision}, is unchanged over {} active days in which the ant worked with {} for \
         the first time: revise it with {}.",
        lately.active_days,
        named.join(", "),
        call(Some(revision))
    );
    let count = step(u32::try_from(new.len()).unwrap_or(u32::MAX), 1);
    Ok(Some(on(FindingKind::StaleSelfModel, "self", item, count, text)))
}

async fn relationships(items: &dyn Items, people: &[Tally]) -> Result<Vec<Gathered>, StoreError> {
    let people: Vec<_> = people.iter().filter(|tally| tally.threads >= RELATIONSHIP_AFTER_THREADS).collect();
    let scopes: Vec<_> = people.iter().map(|tally| tally.scope.clone()).collect();
    let recorded: Vec<_> = records::relationships(items, &scopes).await?.into_iter().map(|r| r.record.scope).collect();
    let unrecorded = people.into_iter().filter(|tally| !recorded.contains(&tally.scope));
    Ok(unrecorded
        .map(|tally| {
            let (who, n) = (&tally.scope, tally.threads);
            let text = format!(
                "No relationship record of {who}, who started {n} of the ant's threads: write one with \
                 sense_identity, giving relationship (what working together taught), who {who} and a title, then \
                 answer applied naming the record written."
            );
            subject(FindingKind::NoRelationship, &who.to_string(), step(n, RELATIONSHIP_AFTER_THREADS), text)
        })
        .collect())
}

async fn skills(items: &dyn Items, ground: &Ground) -> Result<Vec<Gathered>, StoreError> {
    let all = items.list(SKILL, usize::MAX).await?;
    let mut skills: Vec<_> = all.iter().filter_map(|item| Some((Skill::of(item)?.name, item))).collect();
    skills.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(skills
        .into_iter()
        .filter_map(|(name, item)| {
            let call = format!(
                "sense_skill, giving name {name}, content, a reason and this finding's id as finding, then answer \
                 applied naming the proposal made"
            );
            ground.inherited(&format!("{SKILL}:{name}"), &format!("skill {name}"), item, &call)
        })
        .collect())
}

/// A finding of `kind` about `about`, which has no item yet.
pub(super) fn subject(kind: FindingKind, about: &str, count: u32, text: String) -> Gathered {
    Gathered { kind, about: about.into(), items: Vec::new(), count: Some(count.into()), text, untrusted: false }
}

/// A finding of `kind` about `about`, held by `item`.
fn on(kind: FindingKind, about: &str, item: &Item, count: u32, text: String) -> Gathered {
    Gathered { items: vec![item.id.clone()], ..subject(kind, about, count, text) }
}

/// The people and repositories of the threads started by `at`.
async fn known(items: &dyn Items, at: DateTime<Utc>) -> Result<HashSet<Scope>, StoreError> {
    let mut known = HashSet::new();
    for record in record::records(items, usize::MAX).await?.into_iter().filter(|record| record.started <= at) {
        match record.origin.by {
            Starter::Human { user } => known.insert(Scope::Human(user)),
            Starter::Ant { user } => known.insert(Scope::Ant(user)),
            Starter::Webhook { .. } | Starter::Review { .. } | Starter::Provider { .. } => false,
        };
        known.extend(record.repo.and_then(|repo| format!("repo:{repo}").parse().ok()));
    }
    Ok(known)
}

/// `n` stepped down to the largest of `first` doubled any times that is at most `n`; `first` below it.
pub(super) fn step(n: u32, first: u32) -> u32 {
    let mut step = first.max(1);
    while step.saturating_mul(2) <= n {
        step *= 2;
    }
    step
}

#[cfg(test)]
mod tests {
    use super::step;

    #[test]
    fn counts_step_by_doubling() {
        let steps: Vec<_> = [0, 1, 2, 3, 4, 7, 8, 9].map(|n| step(n, 1)).into();
        assert_eq!(steps, [1, 1, 2, 2, 4, 4, 8, 8]);
        let steps: Vec<_> = [5, 9, 10, 19, 20, 40].map(|n| step(n, 5)).into();
        assert_eq!(steps, [5, 5, 10, 10, 20, 40]);
    }
}
