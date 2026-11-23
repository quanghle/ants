//! Daily maintenance: what the daemon serving an ant does to its memories once a day, needing no ant ([`maintain`]).
//!
//! It rechecks citations ([`recheck_citations`]), then, in one event with the run's record (an item of kind
//! [`MAINTENANCE`]), at most [`MAX_CHANGES`] changes, each memory changed once:
//!
//! 1. retires ([`Status::Retired`]) the memories needing review that no change touched for [`RETIRE_AFTER_DAYS`];
//! 2. marks needs_review the active memories, not core, whose [`salience`] fell below [`DECAY_FLOOR`];
//! 3. demotes the core memories unused for [`CORE_IDLE_DAYS`];
//! 4. promotes to core the active memories of the ant's own (scope `self`, trust `self`, no episode) used at least
//!    [`CORE_USES`] times and last within [`CORE_IDLE_DAYS`], the most used first, while the core memories stay within
//!    [`CORE_BUDGET`].
//!
//! A memory is used when written, voted helpful or read in full ([`Memory::last_used`], [`Memory::uses`]). Its ages,
//! since its last use or change, count only the ant's active time ([`Ages`]): a dormant ant uses no memory.
//! Maintenance also clusters near duplicates: live memories but episodes, of one scope, alike from [`NEAR_DUPLICATE`]
//! ([`super::similarity`] of their titles and bodies) or with the same digest. The record keeps when it ran, each change with
//! its action and reason, and the clusters; its event keeps what they were before, so every run can be read back.
//! A review reads what is left to judge with [`needs_review`] and [`duplicates`].

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::recall::{HALF_LIFE_DAYS, HELPFUL_WEIGHT, MAX_HELPFUL, STALE_WEIGHT};
use super::{
    CORE_BUDGET, DEFAULT_IMPORTANCE, MEMORY, Memory, MemoryItem, MemoryKind, NEAR_DUPLICATE, Scope, Status, Trust,
    jaccard, recheck_citations, words,
};
use crate::identity::{self, Dormancy};
use crate::items::{ItemId, ItemValues, Values};
use crate::store::{Items, StoreError, Write, read_modify_write};

/// The kind of the item recording the last maintenance run ([`Maintenance`]).
pub const MAINTENANCE: &str = "maintenance";
/// The hours from one maintenance run to the next.
pub const MAINTENANCE_EVERY_HOURS: i64 = 24;
/// The [`salience`] under which an active memory is marked needs_review: a memory of importance 3, never voted on nor
/// read again, falls under it 90 days after it was written.
pub const DECAY_FLOOR: f64 = 0.125;
/// How much each day a memory was read in full adds to its [`salience`], up to [`MAX_ACCESS_DAYS`] days.
pub const ACCESS_WEIGHT: f64 = 0.25;
/// The most days of reading that add to a memory's [`salience`].
pub const MAX_ACCESS_DAYS: u32 = 8;
/// The days after which a memory needing review, untouched since, is retired.
pub const RETIRE_AFTER_DAYS: i64 = 90;
/// The uses ([`Memory::uses`]) from which a memory is promoted to core.
pub const CORE_USES: u32 = 5;
/// The days of disuse after which a core memory is demoted.
pub const CORE_IDLE_DAYS: i64 = 90;
/// The most memories a run changes: the others wait for the next one.
pub const MAX_CHANGES: usize = 100;
/// The most clusters of near duplicates a run records.
pub const MAX_CLUSTERS: usize = 50;

/// Who maintenance changes items for ([`crate::items::Stamp::by`]).
const BY: &str = "sense:maintenance";
/// The name of the record's item ([`ItemId::named`]).
const RECORD: &str = "daily";

/// What maintenance did to a memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Marked needs_review: its salience fell below [`DECAY_FLOOR`].
    Decayed,
    /// Retired: it needed review, untouched for [`RETIRE_AFTER_DAYS`].
    Retired,
    /// Made core by use.
    Promoted,
    /// No longer core: unused for [`CORE_IDLE_DAYS`].
    Demoted,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Decayed => "decayed",
            Self::Retired => "retired",
            Self::Promoted => "promoted",
            Self::Demoted => "demoted",
        })
    }
}

/// A change of a memory by maintenance, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Maintained {
    pub memory: ItemId,
    pub action: Action,
    pub reason: String,
}

/// A maintenance run, as its record holds it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Maintenance {
    pub ran: DateTime<Utc>,
    #[serde(default)]
    pub changes: Vec<Maintained>,
    /// The changes due past [`MAX_CHANGES`], left for the next run.
    #[serde(default)]
    pub left: usize,
    /// Clusters of near duplicates, each of at least two memories by id, up to [`MAX_CLUSTERS`].
    #[serde(default)]
    pub duplicates: Vec<Vec<ItemId>>,
    /// Citations of the ant's workdir rechecked, and memories they marked needs_review.
    #[serde(default)]
    pub citations_checked: usize,
    #[serde(default)]
    pub citations_marked: usize,
}

impl ItemValues for Maintenance {
    const KINDS: &[&str] = &[MAINTENANCE];
    const LABEL: &str = "maintenance record";
}

/// Ages up to a time in an ant's active time: the time but its dormancies ([`identity::Card::dormant`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Ages {
    now: DateTime<Utc>,
    /// The dormancies up to `now`, sorted, apart from each other.
    dormant: Vec<Dormancy>,
}

impl Ages {
    /// Ages up to `now`, without `dormancies`, which may overlap or go on past `now`.
    pub fn new(now: DateTime<Utc>, dormancies: impl IntoIterator<Item = Dormancy>) -> Self {
        let mut spans: Vec<_> = dormancies
            .into_iter()
            .map(|span| Dormancy { to: span.to.min(now), ..span })
            .filter(|span| span.from < span.to)
            .collect();
        spans.sort_by_key(|span| span.from);
        let mut dormant: Vec<Dormancy> = Vec::with_capacity(spans.len());
        for span in spans {
            match dormant.last_mut() {
                Some(last) if span.from <= last.to => last.to = last.to.max(span.to),
                _ => dormant.push(span),
            }
        }
        Self { now, dormant }
    }

    /// The active time from `since` to now; none when `since` is later.
    pub fn since(&self, since: DateTime<Utc>) -> Duration {
        let dormant: Duration =
            self.dormant.iter().map(|span| (span.to - span.from.max(since)).max(Duration::zero())).sum();
        (self.now - since - dormant).max(Duration::zero())
    }
}

/// The weight of `memory` against which it decays, its age taken from `ages`: its importance over
/// [`DEFAULT_IMPORTANCE`], times `0.5^(active days since last used / HALF_LIFE_DAYS)`, times `(1 + HELPFUL_WEIGHT ×
/// min(helpful, MAX_HELPFUL) + ACCESS_WEIGHT × min(days read, MAX_ACCESS_DAYS)) / (1 + STALE_WEIGHT × stale)`.
pub fn salience(memory: &Memory, ages: &Ages) -> f64 {
    let idle = days(ages.since(memory.last_used()));
    let importance = f64::from(memory.importance) / f64::from(DEFAULT_IMPORTANCE);
    let helpful = HELPFUL_WEIGHT * f64::from(memory.votes.helpful.min(MAX_HELPFUL));
    let read = ACCESS_WEIGHT * f64::from(memory.access.map_or(0, |access| access.days).min(MAX_ACCESS_DAYS));
    let stale = STALE_WEIGHT * f64::from(memory.votes.stale);
    importance * 0.5_f64.powf(idle / HALF_LIFE_DAYS) * (1.0 + helpful + read) / (1.0 + stale)
}

fn days(span: Duration) -> f64 {
    span.num_seconds().max(0) as f64 / 86_400.0
}

fn record_id() -> ItemId {
    ItemId::named(MAINTENANCE, RECORD)
}

/// The last maintenance run; none before the first.
pub async fn last_maintenance(items: &dyn Items) -> Result<Option<Maintenance>, StoreError> {
    Ok(record(items).await?.map(|(run, _)| run))
}

async fn record(items: &dyn Items) -> Result<Option<(Maintenance, u64)>, StoreError> {
    let Some(item) = items.get(&record_id()).await? else { return Ok(None) };
    Ok(Some((Maintenance::of(&item)?, item.revision)))
}

/// When maintenance is due next: [`MAINTENANCE_EVERY_HOURS`] after the last run; none before the first, due now.
pub async fn next_maintenance(items: &dyn Items) -> Result<Option<DateTime<Utc>>, StoreError> {
    Ok(last_maintenance(items).await?.map(|run| run.ran + Duration::hours(MAINTENANCE_EVERY_HOURS)))
}

/// The live memories needing review, the longest untouched first.
pub async fn needs_review(items: &dyn Items) -> Result<Vec<MemoryItem>, StoreError> {
    let found = items.list_by(MEMORY, "status", Some(&json!(Status::NeedsReview)), usize::MAX).await?;
    let mut found: Vec<_> = found
        .into_iter()
        .filter_map(MemoryItem::of)
        .filter(|found| found.memory.status == Status::NeedsReview)
        .collect();
    found.sort_by(|a, b| (&a.item.updated.at, a.id()).cmp(&(&b.item.updated.at, b.id())));
    Ok(found)
}

/// The clusters of near duplicates the last run found, each of the memories still live, at least two.
pub async fn duplicates(items: &dyn Items) -> Result<Vec<Vec<MemoryItem>>, StoreError> {
    let Some(run) = last_maintenance(items).await? else { return Ok(Vec::new()) };
    let mut clusters = Vec::new();
    for ids in run.duplicates {
        let mut cluster = Vec::new();
        for id in ids {
            if let Some(found) = items.get(&id).await?.and_then(MemoryItem::of).filter(|f| f.memory.status.live()) {
                cluster.push(found);
            }
        }
        if cluster.len() > 1 {
            clusters.push(cluster);
        }
    }
    Ok(clusters)
}

/// Run maintenance at `now` unless it ran within [`MAINTENANCE_EVERY_HOURS`], rechecking the citations of the
/// repository holding `workdir` first: see the module documentation. Returns the run, none when it was not due.
pub async fn maintain(
    items: &dyn Items,
    workdir: Option<&Path>,
    now: DateTime<Utc>,
) -> Result<Option<Maintenance>, StoreError> {
    if due_revision(items, now).await?.is_none() {
        return Ok(None);
    }
    let citations = match workdir {
        Some(workdir) => recheck_citations(items, workdir).await?,
        None => Default::default(),
    };
    let citations = &citations;
    read_modify_write(|| async move {
        let Some(revision) = due_revision(items, now).await? else { return Ok(None) };
        let memories: Vec<_> = items.list(MEMORY, usize::MAX).await?.into_iter().filter_map(MemoryItem::of).collect();
        let card = identity::card(items).await?;
        let ages = Ages::new(now, card.iter().flat_map(|card| card.dormant(now)));
        let (changes, writes, left) = decide(&memories, &ages);
        let run = Maintenance {
            ran: now,
            changes,
            left,
            duplicates: clusters(&memories),
            citations_checked: citations.checked,
            citations_marked: citations.marked,
        };
        let mut writes = writes;
        writes.push(Write::Put {
            id: record_id(),
            kind: MAINTENANCE.into(),
            values: run.values(),
            expected: Some(revision),
        });
        items.write(&writes, BY).await?;
        Ok(Some(run))
    })
    .await
}

/// The revision of the record when a run is due, 0 for none.
async fn due_revision(items: &dyn Items, now: DateTime<Utc>) -> Result<Option<u64>, StoreError> {
    Ok(match record(items).await? {
        None => Some(0),
        Some((run, revision)) => (run.ran + Duration::hours(MAINTENANCE_EVERY_HOURS) <= now).then_some(revision),
    })
}

/// The changes of `memories` due at the time of `ages`, each with its write, and how many are left past
/// [`MAX_CHANGES`].
fn decide(memories: &[MemoryItem], ages: &Ages) -> (Vec<Maintained>, Vec<Write>, usize) {
    let mut due: Vec<(&MemoryItem, Action, String)> = Vec::new();
    let idle_days = |memory: &Memory| ages.since(memory.last_used()).num_days();
    for found in memories {
        let memory = &found.memory;
        if memory.status == Status::NeedsReview {
            let untouched = ages.since(found.item.updated.at).num_days();
            if untouched >= RETIRE_AFTER_DAYS {
                due.push((found, Action::Retired, format!("needed review, untouched for {untouched} days")));
            }
        }
    }
    for found in memories {
        let memory = &found.memory;
        if memory.status == Status::Active && !memory.core {
            let salience = salience(memory, ages);
            if salience < DECAY_FLOOR {
                let reason =
                    format!("salience {salience:.3} fell below {DECAY_FLOOR}: unused for {} days", idle_days(memory));
                due.push((found, Action::Decayed, reason));
            }
        }
    }
    for found in memories {
        let memory = &found.memory;
        let changed = due.iter().any(|(changed, ..)| changed.id() == found.id());
        if memory.status.live() && memory.core && !changed && idle_days(memory) >= CORE_IDLE_DAYS {
            due.push((found, Action::Demoted, format!("core, unused for {} days", idle_days(memory))));
        }
    }
    let mut left = due.len().saturating_sub(MAX_CHANGES);
    due.truncate(MAX_CHANGES);

    let changed: BTreeSet<&ItemId> = due.iter().map(|(found, ..)| found.id()).collect();
    let mut core: usize = memories
        .iter()
        .filter(|found| found.memory.core && found.memory.status.live() && !changed.contains(found.id()))
        .map(|found| found.memory.core_bytes())
        .sum();
    let mut candidates: Vec<_> = memories
        .iter()
        .filter(|found| {
            let memory = &found.memory;
            memory.status == Status::Active
                && !memory.core
                && memory.scope == Scope::Own
                && memory.trust == Trust::Own
                && memory.kind != MemoryKind::Episode
                && memory.uses() >= CORE_USES
                && idle_days(memory) < CORE_IDLE_DAYS
                && !changed.contains(found.id())
        })
        .collect();
    candidates.sort_by(|a, b| {
        let key = |found: &MemoryItem| (Reverse(found.memory.uses()), Reverse(found.memory.last_used()));
        key(a).cmp(&key(b)).then_with(|| a.id().cmp(b.id()))
    });
    for found in candidates {
        let bytes = found.memory.core_bytes();
        if core + bytes > CORE_BUDGET {
            continue;
        }
        if due.len() >= MAX_CHANGES {
            left += 1;
            continue;
        }
        core += bytes;
        let reason = format!(
            "used {} times, last {} days ago; core memories take {core} of {CORE_BUDGET} bytes",
            found.memory.uses(),
            idle_days(&found.memory)
        );
        due.push((found, Action::Promoted, reason));
    }

    let writes = due
        .iter()
        .map(|(found, action, _)| {
            let (name, value) = match action {
                Action::Retired => ("status", json!(Status::Retired)),
                Action::Decayed => ("status", json!(Status::NeedsReview)),
                Action::Promoted => ("core", json!(true)),
                Action::Demoted => ("core", json!(false)),
            };
            let mut values = Values::new();
            values.insert(name.into(), value);
            Write::Change { id: found.id().clone(), values, expected: Some(found.item.revision) }
        })
        .collect();
    let changes = due
        .into_iter()
        .map(|(found, action, reason)| Maintained { memory: found.id().clone(), action, reason })
        .collect();
    (changes, writes, left)
}

/// The clusters of near duplicates among the live memories but episodes, each sorted, the clusters by their first.
fn clusters(memories: &[MemoryItem]) -> Vec<Vec<ItemId>> {
    let live: Vec<_> = memories
        .iter()
        .filter(|found| found.memory.status.live() && found.memory.kind != MemoryKind::Episode)
        .collect();
    let mut parent: Vec<usize> = (0..live.len()).collect();
    let mut digests: BTreeMap<(String, &str), usize> = BTreeMap::new();
    let mut scopes: BTreeMap<String, Vec<(usize, BTreeSet<String>)>> = BTreeMap::new();
    for (index, found) in live.iter().enumerate() {
        let memory = &found.memory;
        if let Some(first) = digests.insert((memory.scope.to_string(), &memory.digest), index) {
            join(&mut parent, first, index);
        }
        let text = format!("{}\n{}", memory.title, memory.body);
        scopes.entry(memory.scope.to_string()).or_default().push((index, words(&text)));
    }
    for group in scopes.values_mut() {
        group.sort_by_key(|(_, words)| words.len());
        for (i, (a, a_words)) in group.iter().enumerate() {
            for (b, b_words) in &group[i + 1..] {
                // Jaccard is at most the smaller set's size over the larger's: the rest are further apart.
                if (a_words.len() as f64) < NEAR_DUPLICATE * b_words.len() as f64 {
                    break;
                }
                if jaccard(a_words, b_words) >= NEAR_DUPLICATE {
                    join(&mut parent, *a, *b);
                }
            }
        }
    }
    let mut clusters: BTreeMap<usize, Vec<ItemId>> = BTreeMap::new();
    for (index, found) in live.iter().enumerate() {
        let root = root(&mut parent, index);
        clusters.entry(root).or_default().push(found.id().clone());
    }
    let mut clusters: Vec<_> = clusters.into_values().filter(|cluster| cluster.len() > 1).collect();
    for cluster in &mut clusters {
        cluster.sort();
    }
    clusters.sort();
    clusters.truncate(MAX_CLUSTERS);
    clusters
}

fn join(parent: &mut [usize], a: usize, b: usize) {
    let (a, b) = (root(parent, a), root(parent, b));
    parent[a.max(b)] = a.min(b);
}

fn root(parent: &mut [usize], mut index: usize) -> usize {
    while parent[index] != index {
        parent[index] = parent[parent[index]];
        index = parent[index];
    }
    index
}

#[cfg(test)]
mod tests;
