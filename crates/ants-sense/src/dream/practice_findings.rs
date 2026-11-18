//! The findings of a sage's dream on its practice of reviewing ([`super`]): how the findings it gave other ants fared,
//! gathered from the records of its reviews alone ([`ReviewRecord`]), never their text, so never untrusted:
//!
//! - a kind of finding rejected [`REJECTED_FINDINGS`] times in [`JUDGED_FINDINGS`] or more of the verdicts on it told
//!   over the last [`PRACTICE_DAYS`] active days, at least [`JUDGED_FINDINGS`] of them, one finding each;
//! - [`UNDONE_FINDINGS`] applied findings or more undone since, as told over the same days: a memory written for one
//!   voted wrong or stale, a proposal rejected;
//! - an ant reviewed within those days that left [`UNANSWERED_PERCENT`] percent or more of the findings of its last
//!   [`UNANSWERED_REVIEWS`] reviews with findings unanswered, its latest review left out, as the verdicts on it come
//!   with the next; one finding each;
//! - no relationship record of an ant reviewed [`RELATIONSHIP_AFTER_REVIEWS`] times or more, one finding each.
//!
//! Each names the answers open to the sage: a lesson with `sense_remember`, its self-model or a relationship record of
//! an ant with `sense_identity`, or a skill for how it reviews with `sense_skill`. Its count steps coarsely
//! ([`step`]). Applied, it rests as a rejected one does ([`super::Rested`]): the records it rests on change only with
//! the reviews to come. The numbers are first guesses, to tune on real reviews.

use std::collections::BTreeMap;

use ants_proto::UserId;
use chrono::{DateTime, Duration, Utc};

use super::identity_findings::{step, subject};
use super::{FindingKind, Gathered, Judgement, ReviewRecord, ReviewedFinding};
use crate::memory::{self, Scope};
use crate::records;
use crate::store::{Items, StoreError};

/// The days of the sage's active time over which the verdicts on its findings count.
pub const PRACTICE_DAYS: i64 = 14;
/// The fewest verdicts on a kind of finding over [`PRACTICE_DAYS`] before its rejected share counts.
pub const JUDGED_FINDINGS: u32 = 10;
/// The verdicts in every [`JUDGED_FINDINGS`] on a kind of finding that, rejected, make a finding of it.
pub const REJECTED_FINDINGS: u32 = 7;
/// The applied findings undone over [`PRACTICE_DAYS`] that make a finding.
pub const UNDONE_FINDINGS: u32 = 3;
/// The reviews of an ant whose findings' verdicts came over which its unanswered share counts.
pub const UNANSWERED_REVIEWS: usize = 3;
/// The percent of the findings of [`UNANSWERED_REVIEWS`] reviews an ant leaves unanswered that makes a finding.
pub const UNANSWERED_PERCENT: u32 = 50;
/// The reviews of an ant after which a sage without its relationship record is asked to write one.
pub const RELATIONSHIP_AFTER_REVIEWS: u32 = 3;
/// The most findings an undone findings' finding names.
const MAX_NAMED: usize = 5;

/// How a finding on the practice is answered, as its text tells.
const ANSWERS: &str = "a lesson with sense_remember, the self-model with sense_identity or a skill for how to review \
                       with sense_skill, then answer applied naming the item written, or reject this finding";

/// The findings on the practice of a sage that made `reviews`, the active days counted by `ages`: the rejected kinds,
/// by kind, then the undone findings, then the ants leaving findings unanswered, then the relationships, the most
/// reviewed first. None for an ant that reviewed nobody.
pub(super) async fn gather(
    items: &dyn Items,
    reviews: &[ReviewRecord],
    ages: &memory::Ages,
) -> Result<Vec<Gathered>, StoreError> {
    let recent = |at: Option<DateTime<Utc>>| at.is_some_and(|at| ages.since(at) < Duration::days(PRACTICE_DAYS));
    let mut found = rejected_kinds(reviews, &recent);
    found.extend(undone(reviews, &recent));
    found.extend(unanswered(reviews, &recent));
    found.extend(relationships(items, reviews).await?);
    Ok(found)
}

fn rejected_kinds(reviews: &[ReviewRecord], recent: &dyn Fn(Option<DateTime<Utc>>) -> bool) -> Vec<Gathered> {
    let mut kinds: BTreeMap<&str, (u32, u32)> = BTreeMap::new();
    let judged = reviews.iter().flat_map(|review| &review.findings).filter(|f| f.verdict.is_some() && recent(f.told));
    for finding in judged {
        let (judged, rejected) = kinds.entry(&finding.kind).or_default();
        *judged += 1;
        *rejected += u32::from(finding.verdict == Some(Judgement::Rejected));
    }
    kinds
        .into_iter()
        .filter(|(_, (judged, rejected))| {
            *judged >= JUDGED_FINDINGS && rejected * JUDGED_FINDINGS >= REJECTED_FINDINGS * judged
        })
        .map(|(kind, (judged, rejected))| {
            let text = format!(
                "Of the {judged} findings of kind {kind} judged by the ants reviewed over the last {PRACTICE_DAYS} \
                 active days, {rejected} were rejected: keep what that teaches of when such a finding is worth giving, \
                 as {ANSWERS}."
            );
            subject(FindingKind::RejectedKind, kind, step(rejected, REJECTED_FINDINGS), text)
        })
        .collect()
}

fn undone(reviews: &[ReviewRecord], recent: &dyn Fn(Option<DateTime<Utc>>) -> bool) -> Option<Gathered> {
    let undone = |finding: &&ReviewedFinding| {
        finding.verdict == Some(Judgement::Applied) && !finding.holds && recent(finding.told)
    };
    let undone: Vec<_> = reviews
        .iter()
        .flat_map(|review| review.findings.iter().filter(undone).map(move |finding| (&review.ant, finding)))
        .collect();
    let n = u32::try_from(undone.len()).unwrap_or(u32::MAX);
    if n < UNDONE_FINDINGS {
        return None;
    }
    let mut named: Vec<_> = undone
        .iter()
        .take(MAX_NAMED)
        .map(|(ant, finding)| format!("{} {} of {ant}", finding.id, finding.kind))
        .collect();
    if undone.len() > MAX_NAMED {
        named.push(format!("{} more", undone.len() - MAX_NAMED));
    }
    let text = format!(
        "{n} findings given other ants were applied and then undone, as told over the last {PRACTICE_DAYS} active \
         days (a memory written for one voted wrong or stale, a proposal rejected): {}. Keep what that teaches of the \
         findings that last, as {ANSWERS}.",
        named.join(", ")
    );
    Some(subject(FindingKind::UndoneFindings, "applied", step(n, UNDONE_FINDINGS), text))
}

fn unanswered(reviews: &[ReviewRecord], recent: &dyn Fn(Option<DateTime<Utc>>) -> bool) -> Vec<Gathered> {
    let mut by_ant: BTreeMap<&UserId, Vec<&ReviewRecord>> = BTreeMap::new();
    for review in reviews {
        by_ant.entry(&review.ant).or_default().push(review);
    }
    by_ant
        .into_iter()
        .filter_map(|(ant, mut reviews)| {
            reviews.sort_by_key(|review| review.dream);
            let (latest, earlier) = reviews.split_last()?;
            if !recent(Some(latest.received)) {
                return None;
            }
            let mut judged: Vec<_> =
                earlier.iter().rev().filter(|review| !review.findings.is_empty()).take(UNANSWERED_REVIEWS).collect();
            if judged.len() < UNANSWERED_REVIEWS {
                return None;
            }
            judged.reverse();
            let findings = judged.iter().flat_map(|review| &review.findings);
            let made = u32::try_from(findings.clone().count()).unwrap_or(u32::MAX);
            let left = u32::try_from(findings.filter(|f| f.verdict.is_none()).count()).unwrap_or(u32::MAX);
            if left * 100 < made * UNANSWERED_PERCENT {
                return None;
            }
            let who = Scope::Ant(ant.clone());
            let dreams: Vec<_> = judged.iter().map(|review| review.dream.to_string()).collect();
            let text = format!(
                "{who} left {left} of the {made} findings of the reviews of its dreams {} unanswered: keep what works \
                 in reviewing it in a relationship record with sense_identity, giving relationship, who {who} and a \
                 title, or as {ANSWERS}.",
                dreams.join(", ")
            );
            Some(subject(FindingKind::UnansweredReviews, &who.to_string(), step(left, 1), text))
        })
        .collect()
}

async fn relationships(items: &dyn Items, reviews: &[ReviewRecord]) -> Result<Vec<Gathered>, StoreError> {
    let mut counts: BTreeMap<&UserId, u32> = BTreeMap::new();
    for review in reviews {
        *counts.entry(&review.ant).or_default() += 1;
    }
    let mut reviewed: Vec<_> = counts.into_iter().filter(|(_, n)| *n >= RELATIONSHIP_AFTER_REVIEWS).collect();
    reviewed.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    let scopes: Vec<_> = reviewed.iter().map(|(ant, _)| Scope::Ant((*ant).clone())).collect();
    let recorded: Vec<_> = records::relationships(items, &scopes).await?.into_iter().map(|r| r.record.scope).collect();
    Ok(scopes
        .into_iter()
        .zip(reviewed)
        .filter(|(who, _)| !recorded.contains(who))
        .map(|(who, (_, n))| {
            let text = format!(
                "No relationship record of {who}, reviewed {n} times: write what works in reviewing it with \
                 sense_identity, giving relationship, who {who} and a title, then answer applied naming the record \
                 written; the reviews of {who} show it."
            );
            subject(FindingKind::NoReviewedRelationship, &who.to_string(), step(n, RELATIONSHIP_AFTER_REVIEWS), text)
        })
        .collect())
}
