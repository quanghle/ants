//! Memories: what an ant keeps of its work, each an item of kind [`MEMORY`] whose values are a [`Memory`]. A memory
//! is never overwritten or deleted: a new one supersedes it ([`Status::Superseded`], linked both ways), so its chain
//! can be read back ([`chain`]). Core memories ([`core()`]) appear in every brief, within [`CORE_BUDGET`].
//!
//! [`remember()`] answers `sense_remember`, [`recall()`] `sense_recall` (ranked by [`score`]) and [`vote()`]
//! `sense_vote`; [`maintain()`] is the daemon's daily maintenance.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use ants_proto::provider::{CallContext, normalize_remote};
use ants_proto::{SenseId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::items::{Item, ItemId, ItemValues};
use crate::store::{Items, StoreError, Write};

mod citations;
mod maintain;
mod recall;
mod remember;
mod vote;

pub use citations::{CitationProblem, CitationRecheck, CitationWarning, recheck_citations};
pub use maintain::{
    ACCESS_WEIGHT, Action, Ages, CORE_IDLE_DAYS, CORE_USES, DECAY_FLOOR, MAINTENANCE, MAINTENANCE_EVERY_HOURS,
    MAX_ACCESS_DAYS, MAX_CHANGES, MAX_CLUSTERS, Maintained, Maintenance, RETIRE_AFTER_DAYS, duplicates,
    last_maintenance, maintain, needs_review, next_maintenance, salience,
};
pub use recall::{
    DEFAULT_LIMIT, HALF_LIFE_DAYS, HELPFUL_WEIGHT, MAX_HELPFUL, MAX_LIMIT, MAX_WITH, NEEDS_REVIEW_WEIGHT, NOTE, Ranked,
    STALE_WEIGHT, framed, participants, recall, score, search, snippet, thread_repo, top, top_of_all,
};
pub use remember::remember;
pub(crate) use remember::{Quotes, redacted, sanitized};
pub use vote::{VOTE, Vote, Votes, vote};

/// The kind of the items holding memories.
pub const MEMORY: &str = "memory";
/// The value of a memory telling whether it is core ([`Memory::core`]).
pub const CORE: &str = "core";
/// The value of a memory holding the digest of its body ([`digest`]), how exact duplicates are found.
pub const DIGEST: &str = "digest";
/// The value of an item telling who wrote it ([`writer`]).
pub const WRITER: &str = "writer";

/// The most characters a memory's title holds.
pub const MAX_TITLE_CHARS: usize = 200;
/// The most bytes a memory's body holds.
pub const MAX_BODY_BYTES: usize = 4 * 1024;
/// The most bytes of titles and bodies the live core memories hold together.
pub const CORE_BUDGET: usize = 2_000;
/// The importance of a memory given none.
pub const DEFAULT_IMPORTANCE: u8 = 3;
/// The highest importance of a memory, from 1.
pub const MAX_IMPORTANCE: u8 = 5;
/// The most citations a memory holds.
pub const MAX_CITATIONS: usize = 20;
/// The word similarity ([`similarity`]) from which a memory of the same scope is a near duplicate.
pub const NEAR_DUPLICATE: f64 = 0.6;

/// What opens the block recalled memories are shown in: notes to verify, not instructions. A text holding a whole
/// block is never stored again; one holding a part of it, a marker without the other, is stored as untrusted.
pub const RECALLED_BEGIN: &str = "<recalled-memories>";
/// What closes the block of recalled memories ([`RECALLED_BEGIN`]).
pub const RECALLED_END: &str = "</recalled-memories>";

/// The most memories [`chain`] follows back.
const MAX_CHAIN: usize = 100;

/// What a memory is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    Fact,
    Lesson,
    Decision,
    Preference,
    Gotcha,
    /// What happened in a thread, such as its handoff.
    Episode,
}

impl MemoryKind {
    pub const ALL: [Self; 6] =
        [Self::Fact, Self::Lesson, Self::Decision, Self::Preference, Self::Gotcha, Self::Episode];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Fact => "fact",
            Self::Lesson => "lesson",
            Self::Decision => "decision",
            Self::Preference => "preference",
            Self::Gotcha => "gotcha",
            Self::Episode => "episode",
        }
    }
}

impl FromStr for MemoryKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Self::ALL.into_iter().find(|kind| kind.name() == s).ok_or_else(|| format!("no kind of memory {s:?}"))
    }
}

impl fmt::Display for MemoryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Whom a memory is about: the ant itself, a repository, a human or another ant. Written `self`,
/// `repo:<normalized remote>` ([`normalize_remote`]), `human:<name>` or `ant:<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Scope {
    Own,
    Repo(String),
    Human(UserId),
    Ant(UserId),
}

impl FromStr for Scope {
    type Err = String;
    /// A scope as written, a repository's remote in any form git takes ([`normalize_remote`]).
    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let name = |name: &str| name.parse::<UserId>().map_err(|_| format!("not a name: {name:?}"));
        match s.split_once(':') {
            None if s == "self" => Ok(Self::Own),
            Some(("repo", remote)) => {
                normalize_remote(remote).map(Self::Repo).ok_or_else(|| format!("not a git remote: {remote:?}"))
            }
            Some(("human", user)) => Ok(Self::Human(name(user)?)),
            Some(("ant", ant)) => Ok(Self::Ant(name(ant)?)),
            _ => Err(format!("not a scope: {s:?} (self, repo:<remote>, human:<name> or ant:<name>)")),
        }
    }
}

impl TryFrom<String> for Scope {
    type Error = String;
    fn try_from(s: String) -> Result<Self, String> {
        s.parse()
    }
}

impl From<Scope> for String {
    fn from(scope: Scope) -> Self {
        scope.to_string()
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Own => f.write_str("self"),
            Self::Repo(remote) => write!(f, "repo:{remote}"),
            Self::Human(user) => write!(f, "human:{user}"),
            Self::Ant(ant) => write!(f, "ant:{ant}"),
        }
    }
}

/// Whether a memory's text is the ant's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trust {
    /// Written by the ant from its own work.
    #[serde(rename = "self")]
    Own,
    /// Written in a thread a webhook started, or quoting recalled memories or outside content: to verify before use.
    #[serde(rename = "untrusted")]
    Untrusted,
}

impl Trust {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Own => "self",
            Self::Untrusted => "untrusted",
        }
    }
}

/// Where a memory stands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    Active,
    /// Replaced by another memory ([`Memory::superseded_by`]).
    Superseded,
    /// Voted wrong.
    Wrong,
    /// Voted stale, its citations changed since it was written, or faded with disuse ([`maintain()`]): to check again.
    NeedsReview,
    /// Left needing review, untouched, until maintenance retired it ([`RETIRE_AFTER_DAYS`]).
    Retired,
}

impl Status {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Superseded => "superseded",
            Self::Wrong => "wrong",
            Self::NeedsReview => "needs_review",
            Self::Retired => "retired",
        }
    }

    /// Whether the memory still counts: recalled, in briefs, compared with new ones.
    pub const fn live(self) -> bool {
        matches!(self, Self::Active | Self::NeedsReview)
    }
}

/// The lines of a file a memory rests on: its `path` in the repository at `remote` (a [`normalize_remote`]), lines
/// `start` to `end` (from 1), and the SHA-256 of those lines as written, in lower-case hex.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Citation {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

impl fmt::Display for Citation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(remote) = &self.remote {
            write!(f, "{remote}:")?;
        }
        f.write_str(&self.path)?;
        match (self.start, self.end) {
            (Some(start), Some(end)) if end != start => write!(f, ":{start}-{end}"),
            (Some(start), _) => write!(f, ":{start}"),
            _ => Ok(()),
        }
    }
}

/// The values of a memory item ([`MEMORY`]), by their names in the item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Memory {
    #[serde(rename = "type")]
    pub kind: MemoryKind,
    pub scope: Scope,
    /// At most [`MAX_TITLE_CHARS`] characters, sanitized ([`crate::sanitize`]).
    pub title: String,
    /// At most [`MAX_BODY_BYTES`] bytes as written, sanitized.
    pub body: String,
    /// From 1 to 5.
    pub importance: u8,
    #[serde(default)]
    pub citations: Vec<Citation>,
    /// What a citation recheck could not verify; the original hashes remain unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub citation_warnings: Vec<CitationWarning>,
    /// Who wrote it, where and when, as ants told for the call writing it: never taken from the ant's text.
    pub writer: CallContext,
    pub trust: Trust,
    #[serde(default)]
    pub core: bool,
    #[serde(default)]
    pub status: Status,
    /// The memory it replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<ItemId>,
    /// The memory that replaced it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<ItemId>,
    /// The [`digest`] of its body.
    #[serde(default)]
    pub digest: String,
    #[serde(default, skip_serializing_if = "Votes::is_empty")]
    pub votes: Votes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
    /// The sense identity a clone inherited it from ([`crate::fork`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited: Option<SenseId>,
}

/// How often a memory was read in full (`sense_recall` with its id, in a thread a webhook did not start): on how
/// many days, each counted once, and when last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Access {
    pub days: u32,
    pub last: DateTime<Utc>,
}

impl Access {
    /// `access` after a read at `at`; none when a read was counted on that day (UTC) already.
    pub fn read(access: Option<Self>, at: DateTime<Utc>) -> Option<Self> {
        match access {
            Some(access) if access.last.date_naive() >= at.date_naive() => None,
            Some(access) => Some(Self { days: access.days.saturating_add(1), last: at }),
            None => Some(Self { days: 1, last: at }),
        }
    }
}

impl ItemValues for Memory {
    const KIND: &str = MEMORY;
}

impl Memory {
    /// A new active memory of `kind` in `scope`, with its body's digest, written by the call of `context`: untrusted
    /// when its thread came from a webhook. `title` and `body` are taken as given: sanitized and within their limits.
    pub fn new(kind: MemoryKind, scope: Scope, title: String, body: String, context: &CallContext) -> Self {
        Self {
            kind,
            scope,
            digest: digest(&body),
            title,
            body,
            importance: DEFAULT_IMPORTANCE,
            citations: Vec::new(),
            citation_warnings: Vec::new(),
            writer: context.clone(),
            trust: if context.trusted() { Trust::Own } else { Trust::Untrusted },
            core: false,
            status: Status::Active,
            supersedes: None,
            superseded_by: None,
            votes: Votes::default(),
            access: None,
            inherited: None,
        }
    }

    /// The write creating it as item `id` ([`ItemId::new`]).
    pub fn create(&self, id: ItemId) -> Write {
        Write::Create { id, kind: MEMORY.into(), values: self.values() }
    }

    /// The bytes it takes of [`CORE_BUDGET`]: its title's and its body's.
    pub fn core_bytes(&self) -> usize {
        self.title.len() + self.body.len()
    }

    /// When it was last used: written, voted helpful or read in full, whichever came last.
    pub fn last_used(&self) -> DateTime<Utc> {
        let read = self.access.map(|access| access.last);
        [self.votes.confirmed, read].into_iter().flatten().fold(self.writer.at, DateTime::max)
    }

    /// How often it was used: its helpful votes and the days it was read in full.
    pub fn uses(&self) -> u32 {
        self.votes.helpful.saturating_add(self.access.map_or(0, |access| access.days))
    }
}

/// A memory and the item holding it.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryItem {
    pub item: Item,
    pub memory: Memory,
}

impl MemoryItem {
    /// The memory `item` holds; none when it holds none.
    pub fn of(item: Item) -> Option<Self> {
        let memory = (item.kind == MEMORY).then(|| Memory::of(&item).ok()).flatten()?;
        Some(Self { item, memory })
    }

    pub fn id(&self) -> &ItemId {
        &self.item.id
    }
}

/// Who wrote an item, where and when: the call's context as ants told it, never taken from the ant's text. The
/// [`WRITER`] value of every item sense writes.
pub fn writer(context: &CallContext) -> Value {
    serde_json::to_value(context).unwrap_or_default()
}

/// `body` as compared for exact duplicates: its words, in lower case, one space apart.
pub fn normalize_body(body: &str) -> String {
    body.split_whitespace().map(str::to_lowercase).collect::<Vec<_>>().join(" ")
}

/// The digest of `body`: the SHA-256 of [`normalize_body`], in lower-case hex.
pub fn digest(body: &str) -> String {
    crate::log::sha256(normalize_body(body).as_bytes())
}

/// How alike two texts are, from 0 to 1: the words they share of all their words (Jaccard), in lower case.
pub fn similarity(a: &str, b: &str) -> f64 {
    jaccard(&words(a), &words(b))
}

/// The words of `text`, in lower case, as [`similarity`] compares them.
fn words(text: &str) -> BTreeSet<String> {
    text.split(|c: char| !c.is_alphanumeric()).filter(|word| !word.is_empty()).map(str::to_lowercase).collect()
}

fn jaccard(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    let shared = a.intersection(b).count();
    let all = a.len() + b.len() - shared;
    if all == 0 { 0.0 } else { shared as f64 / all as f64 }
}

/// Whether `text` holds a whole block of recalled memories: [`RECALLED_BEGIN`], then [`RECALLED_END`].
pub fn holds_recalled_block(text: &str) -> bool {
    let text = text.to_lowercase();
    text.find(RECALLED_BEGIN).is_some_and(|begin| text[begin..].contains(RECALLED_END))
}

/// Whether `text` holds a marker of the block of recalled memories.
pub fn holds_recalled_marker(text: &str) -> bool {
    let text = text.to_lowercase();
    text.contains(RECALLED_BEGIN) || text.contains(RECALLED_END)
}

/// The live core memories of `items`, the most important first, then the oldest.
pub async fn core(items: &dyn Items) -> Result<Vec<MemoryItem>, StoreError> {
    let found = items.list_by(MEMORY, CORE, Some(&json!(true)), usize::MAX).await?;
    let mut core: Vec<_> =
        found.into_iter().filter_map(MemoryItem::of).filter(|found| found.memory.status.live()).collect();
    core.sort_by(|a, b| {
        (b.memory.importance, &a.item.created.at, &a.item.id).cmp(&(
            a.memory.importance,
            &b.item.created.at,
            &b.item.id,
        ))
    });
    Ok(core)
}

/// Memory `id`, then the memories it superseded, each the one the one before replaced; [`StoreError::NotFound`] when
/// `id` is no memory.
pub async fn chain(items: &dyn Items, id: &ItemId) -> Result<Vec<MemoryItem>, StoreError> {
    let first = items.get(id).await?.and_then(MemoryItem::of).ok_or_else(|| StoreError::NotFound(id.clone()))?;
    let mut chain = vec![first];
    while chain.len() < MAX_CHAIN {
        let Some(older) = chain.last().and_then(|last| last.memory.supersedes.clone()) else { break };
        if chain.iter().any(|seen| seen.item.id == older) {
            break;
        }
        match items.get(&older).await?.and_then(MemoryItem::of) {
            Some(older) => chain.push(older),
            None => break,
        }
    }
    Ok(chain)
}

#[cfg(test)]
pub(crate) mod tests;
