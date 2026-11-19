//! Trail ratings: each conductor's judgement of whether a run of a saved trail served the task it was run for
//! ([`TrailRating`]), kept in [`TRAIL_RATINGS_BUCKET`], and the search ranking saved trails by them for a task
//! ([`Finder`]): the trails whose name, description or rated tasks match the task, ranked by one draw per trail from
//! Beta(1 + worked + partly/2, 1 + failed + partly/2) over its ratings on matching tasks, seeded by the thread and the
//! task, so the same question on a thread gets the same order and a new thread explores.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{Error, RunId, ThreadId, TrailName, UserId, tip_words, trail_rating_key};
#[cfg(doc)]
use crate::{TRAIL_RATINGS_BUCKET, script_sha256};

mod sample;
mod stem;

pub use stem::stem;

/// Most characters a rating's task has.
pub const MAX_RATING_TASK: usize = 300;
/// Most characters a rating's reason has.
pub const MAX_RATING_WHY: usize = 500;
/// The fewest words of a search ([`Query`]) a text shares to match it, or all of a search of fewer.
pub const RATING_MATCH_WORDS: usize = 2;
/// How long a rating weighs fully: an older one, or one of another script than its trail's current one, weighs half.
pub const RATING_FULL_WEIGHT: Duration = Duration::from_hours(90 * 24);
/// Trails a search finds unless asked for another number.
pub const FIND_TRAILS_LIMIT: usize = 5;
/// Most trails a search finds.
pub const MAX_FIND_TRAILS: usize = 20;
/// Most of a trail's latest ratings a [`RatingSummary`] keeps.
pub const LATEST_RATINGS: usize = 3;

/// Whether a run served the task it was run for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RatingOutcome {
    Worked,
    /// It served the task in part.
    Partly,
    Failed,
}

impl RatingOutcome {
    pub const ALL: [Self; 3] = [Self::Worked, Self::Partly, Self::Failed];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Worked => "worked",
            Self::Partly => "partly",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for RatingOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RatingOutcome {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::ALL
            .into_iter()
            .find(|outcome| outcome.as_str() == s)
            .ok_or_else(|| Error::InvalidRating(format!("no outcome {s:?}: use worked, partly or failed")))
    }
}

/// A conductor's rating in [`TRAIL_RATINGS_BUCKET`], under [`TrailRating::key`], of a saved trail its run took: one
/// per run and trail, rating the run again replacing it. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailRating {
    pub trail: TrailName,
    /// The ant whose run it rates.
    pub conductor: UserId,
    pub run: RunId,
    pub outcome: RatingOutcome,
    /// What the run was for.
    pub task: String,
    /// Why it worked or not.
    pub why: String,
    /// The [`script_sha256`] of the trail's script the run took.
    pub script: String,
    pub at: DateTime<Utc>,
}

impl TrailRating {
    /// Its key in [`TRAIL_RATINGS_BUCKET`] ([`trail_rating_key`]).
    pub fn key(&self) -> String {
        trail_rating_key(&self.trail, &self.conductor, &self.run)
    }

    /// Check what a reader takes from the bucket: the rating is stored under its own key and within the bounds of
    /// [`TrailRating::check`].
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key != self.key() {
            return Err(Error::ForeignRating { key: self.key(), at: key.to_owned() });
        }
        self.check()
    }

    /// Check the rating's bounds: a task of 1 to [`MAX_RATING_TASK`] characters and a reason of 1 to
    /// [`MAX_RATING_WHY`], neither blank, and a script hash of 64 lowercase hex digits.
    pub fn check(&self) -> Result<(), Error> {
        if self.task.trim().is_empty() || self.task.chars().count() > MAX_RATING_TASK {
            return Err(Error::InvalidRating(format!("a task has 1-{MAX_RATING_TASK} characters")));
        }
        if self.why.trim().is_empty() || self.why.chars().count() > MAX_RATING_WHY {
            return Err(Error::InvalidRating(format!("a reason has 1-{MAX_RATING_WHY} characters")));
        }
        if self.script.len() != 64 || !self.script.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return Err(Error::InvalidRating("a script hash is 64 lowercase hex digits".to_owned()));
        }
        Ok(())
    }

    /// Its weight at `now` for a trail whose current script has the hash `script`: half when older than
    /// [`RATING_FULL_WEIGHT`] or of another script, else whole.
    pub fn weight(&self, script: Option<&str>, now: DateTime<Utc>) -> f64 {
        let age = (now - self.at).to_std().unwrap_or_default();
        if age > RATING_FULL_WEIGHT || script != Some(self.script.as_str()) { 0.5 } else { 1.0 }
    }
}

/// The words a search looks for: those of [`tip_words`], each [`stem`]med.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Query {
    words: BTreeSet<String>,
}

impl Query {
    pub fn new(text: &str) -> Self {
        Self { words: stemmed(text) }
    }

    /// Whether it has no words, matching every text.
    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    /// Whether `text` shares at least [`RATING_MATCH_WORDS`] of its stemmed words, or all of them when it has fewer.
    pub fn matches(&self, text: &str) -> bool {
        let shared = stemmed(text).intersection(&self.words).count();
        shared >= RATING_MATCH_WORDS.min(self.words.len())
    }

    /// Whether a trail's name, its words split on `-` and `_`, with its description, matches.
    pub fn matches_trail(&self, name: &TrailName, description: &str) -> bool {
        self.matches(&format!("{name} {description}"))
    }

    /// The seed of the draws of a search for it on `thread`.
    fn seed(&self, thread: &ThreadId) -> [u8; 32] {
        let words = self.words.iter().map(String::as_str).collect::<Vec<_>>().join(" ");
        Sha256::new().chain_update(thread.as_str()).chain_update([0]).chain_update(words).finalize().into()
    }
}

fn stemmed(text: &str) -> BTreeSet<String> {
    tip_words(text).iter().map(|word| stem(word)).collect()
}

/// Ratings by outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub worked: u32,
    pub partly: u32,
    pub failed: u32,
}

impl Counts {
    pub fn add(&mut self, outcome: RatingOutcome) {
        match outcome {
            RatingOutcome::Worked => self.worked += 1,
            RatingOutcome::Partly => self.partly += 1,
            RatingOutcome::Failed => self.failed += 1,
        }
    }

    pub fn total(&self) -> u32 {
        self.worked + self.partly + self.failed
    }

    /// The share of the ratings that worked, a partly counting half; none without ratings.
    pub fn rate(&self) -> Option<f64> {
        let total = self.total();
        (total > 0).then(|| (f64::from(self.worked) + f64::from(self.partly) / 2.0) / f64::from(total))
    }
}

impl fmt::Display for Counts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} worked, {} partly, {} failed", self.worked, self.partly, self.failed)
    }
}

/// Whether a saved trail already serves a task: its description matches the task, and more of its ratings worked than
/// failed.
pub fn serves(description_matches: bool, counts: &Counts) -> bool {
    description_matches && counts.worked > counts.failed
}

/// A trail's ratings counted by outcome, with its [`LATEST_RATINGS`] latest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RatingSummary {
    #[serde(flatten)]
    pub counts: Counts,
    /// Latest first.
    pub latest: Vec<TrailRating>,
}

impl RatingSummary {
    pub fn add(&mut self, rating: TrailRating) {
        self.counts.add(rating.outcome);
        let at = self.latest.partition_point(|kept| (kept.at, &kept.run) > (rating.at, &rating.run));
        if at < LATEST_RATINGS {
            self.latest.insert(at, rating);
            self.latest.truncate(LATEST_RATINGS);
        }
    }
}

/// A search of the saved trails for a task, given the trails and then their ratings, which it folds as they come, so
/// it holds a summary per trail and no rating more. Pure: the time and the current scripts' hashes are given.
#[derive(Debug, Clone)]
pub struct Finder<T> {
    query: Query,
    now: DateTime<Utc>,
    trails: BTreeMap<TrailName, Evidence<T>>,
}

#[derive(Debug, Clone)]
struct Evidence<T> {
    item: T,
    script: Option<String>,
    described: bool,
    alpha: f64,
    beta: f64,
    matching: RatingSummary,
}

/// A trail a [`Finder`] found.
#[derive(Debug, Clone, PartialEq)]
pub struct Found<T> {
    pub name: TrailName,
    /// What the caller gave with the trail.
    pub item: T,
    /// Its draw, which ranks it.
    pub draw: f64,
    /// Its ratings on tasks matching the search.
    pub matching: RatingSummary,
}

impl<T> Found<T> {
    /// Whether no rating of it is on a task matching the search.
    pub fn exploring(&self) -> bool {
        self.matching.counts.total() == 0
    }
}

impl<T> Finder<T> {
    pub fn new(query: Query, now: DateTime<Utc>) -> Self {
        Self { query, now, trails: BTreeMap::new() }
    }

    /// Add a saved trail, `script` the hash of its current script, and `item` what to find it with.
    pub fn trail(&mut self, name: TrailName, description: &str, script: Option<String>, item: T) {
        let described = self.query.matches_trail(&name, description);
        let evidence = Evidence { item, script, described, alpha: 1.0, beta: 1.0, matching: RatingSummary::default() };
        self.trails.insert(name, evidence);
    }

    /// Fold in a rating of a trail added: one of a trail not added is left out, as of a trail since removed.
    pub fn rating(&mut self, rating: TrailRating) {
        let Some(evidence) = self.trails.get_mut(&rating.trail) else { return };
        if !self.query.matches(&rating.task) {
            return;
        }
        let weight = rating.weight(evidence.script.as_deref(), self.now);
        match rating.outcome {
            RatingOutcome::Worked => evidence.alpha += weight,
            RatingOutcome::Failed => evidence.beta += weight,
            RatingOutcome::Partly => {
                evidence.alpha += weight / 2.0;
                evidence.beta += weight / 2.0;
            }
        }
        evidence.matching.add(rating);
    }

    /// The trails matching the search, by name and description or by a rating's task, ranked by their draws for a
    /// search on `thread`, highest first, at most `limit`.
    pub fn rank(self, thread: &ThreadId, limit: usize) -> Vec<Found<T>> {
        let seed = self.query.seed(thread);
        let mut found: Vec<_> = self
            .trails
            .into_iter()
            .filter(|(_, evidence)| evidence.described || evidence.matching.counts.total() > 0)
            .map(|(name, evidence)| {
                let draw = draws(&seed, &name).beta(evidence.alpha, evidence.beta);
                Found { name, item: evidence.item, draw, matching: evidence.matching }
            })
            .collect();
        found.sort_by(|a, b| b.draw.total_cmp(&a.draw).then_with(|| a.name.cmp(&b.name)));
        found.truncate(limit);
        found
    }
}

/// The draws of `trail` from `seed`: a stream of its own, so adding or removing another trail leaves its draw as it
/// was.
fn draws(seed: &[u8; 32], trail: &TrailName) -> sample::SplitMix64 {
    let hash = Sha256::new().chain_update(seed).chain_update(trail.as_str()).finalize();
    let mut first = [0; 8];
    first.copy_from_slice(&hash[..8]);
    sample::SplitMix64::new(u64::from_be_bytes(first))
}

#[cfg(test)]
mod tests;
