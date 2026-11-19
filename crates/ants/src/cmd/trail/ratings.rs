//! The ratings conductors gave the saved trails their runs took, in [`TRAIL_RATINGS_BUCKET`]: counted beside each
//! trail in `ants trail list` and `show`, and beside each run in `ants trail runs` and `show RUN`. Humans purge a
//! trail's with the trail ([`purge`]).

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use ants_proto::{RatingOutcome, RatingSummary, RunId, TRAIL_RATINGS_BUCKET, TrailName, TrailRating, UserId};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use chrono::{DateTime, Utc};

use crate::colony::{short, since};
use crate::machines::latest_in;
use crate::text::excerpt;

/// The ratings of every trail, those `conductor` gave for one.
pub(crate) async fn read(js: &jetstream::Context, conductor: Option<&UserId>) -> Result<Vec<TrailRating>> {
    let filter = match conductor {
        Some(conductor) => format!("$KV.{TRAIL_RATINGS_BUCKET}.*.{conductor}.*"),
        None => format!("$KV.{TRAIL_RATINGS_BUCKET}.>"),
    };
    let found = latest_in(js, TRAIL_RATINGS_BUCKET, &filter).await?;
    Ok(found.iter().filter_map(|(key, payload)| parse(key, payload)).collect())
}

/// The ratings of trail `name`, the latest first.
pub(super) async fn of_trail(js: &jetstream::Context, name: &TrailName) -> Result<Option<RatingSummary>> {
    let found = latest_in(js, TRAIL_RATINGS_BUCKET, &format!("$KV.{TRAIL_RATINGS_BUCKET}.{name}.>")).await?;
    let ratings = found.iter().filter_map(|(key, payload)| parse(key, payload)).collect();
    Ok(summaries(ratings).remove(name))
}

/// The ratings of run `run`, `conductor`'s.
pub(super) async fn of_run(js: &jetstream::Context, conductor: &UserId, run: &RunId) -> Result<Vec<TrailRating>> {
    let filter = format!("$KV.{TRAIL_RATINGS_BUCKET}.*.{conductor}.{run}");
    let found = latest_in(js, TRAIL_RATINGS_BUCKET, &filter).await?;
    let mut ratings: Vec<TrailRating> = found.iter().filter_map(|(key, payload)| parse(key, payload)).collect();
    ratings.sort_by(|a, b| a.trail.cmp(&b.trail));
    Ok(ratings)
}

/// The ratings [`read`] reads, none when they do not read, as where the colony's buckets predate them: a listing
/// shows what else it has.
pub(crate) async fn read_or_none(js: &jetstream::Context, conductor: Option<&UserId>) -> Vec<TrailRating> {
    read(js, conductor).await.unwrap_or_else(|e| {
        tracing::warn!("not reading the trails' ratings: {e:#}");
        Vec::new()
    })
}

fn parse(key: &str, payload: &[u8]) -> Option<TrailRating> {
    let checked = serde_json::from_slice::<TrailRating>(payload)
        .map_err(anyhow::Error::from)
        .and_then(|rating| rating.check_key(key).map(|()| rating).map_err(anyhow::Error::from));
    checked.inspect_err(|e| tracing::warn!(key, "skipping a trail rating: {e:#}")).ok()
}

/// The ratings by trail, each counted with its latest.
pub(crate) fn summaries(ratings: Vec<TrailRating>) -> BTreeMap<TrailName, RatingSummary> {
    let mut summaries: BTreeMap<TrailName, RatingSummary> = BTreeMap::new();
    for rating in ratings {
        summaries.entry(rating.trail.clone()).or_default().add(rating);
    }
    summaries
}

/// The outcome each run's rating gave each trail.
pub(crate) fn of_runs(ratings: &[TrailRating]) -> HashMap<RunId, Vec<(TrailName, RatingOutcome)>> {
    let mut runs: HashMap<RunId, Vec<(TrailName, RatingOutcome)>> = HashMap::new();
    for rating in ratings {
        runs.entry(rating.run.clone()).or_default().push((rating.trail.clone(), rating.outcome));
    }
    for rated in runs.values_mut() {
        rated.sort_by(|(a, _), (b, _)| a.cmp(b));
    }
    runs
}

/// The cells `ants trail list` shows of a trail's ratings: worked/partly/failed, and the latest reason; `-` for none.
pub(super) fn cells(summary: Option<&RatingSummary>) -> [String; 2] {
    let Some(summary) = summary.filter(|summary| summary.counts.total() > 0) else {
        return ["-".to_owned(), "-".to_owned()];
    };
    let counts = summary.counts;
    let latest = summary.latest.first().map_or_else(|| "-".to_owned(), |rating| excerpt(&rating.why, 40));
    [format!("{}/{}/{}", counts.worked, counts.partly, counts.failed), latest]
}

/// The cell `ants trail runs` shows of a run's ratings: the outcome, or each trail's when it rated several; `-` for
/// none.
pub(super) fn rated_cell(rated: Option<&Vec<(TrailName, RatingOutcome)>>) -> String {
    match rated.map_or_default(Vec::as_slice) {
        [] => "-".to_owned(),
        [(_, outcome)] => outcome.to_string(),
        several => {
            let each: Vec<String> = several.iter().map(|(trail, outcome)| format!("{trail} {outcome}")).collect();
            each.join(", ")
        }
    }
}

/// The lines `ants trail show NAME` shows of a trail's ratings: the counts, then the latest, each beside its run.
pub(super) fn section(summary: &RatingSummary, now: DateTime<Utc>) -> String {
    let mut out = format!("ratings: {}\n", summary.counts);
    for rating in &summary.latest {
        let _ = writeln!(
            out,
            "  {} run {} by {} {} ago  task: {}  why: {}",
            rating.outcome,
            rating.run,
            rating.conductor,
            short(since(rating.at, now)),
            excerpt(&rating.task, 80),
            excerpt(&rating.why, 120)
        );
    }
    out
}

/// The lines `ants trail show RUN` shows of the run's ratings, a line per trail.
pub(super) fn of_run_section(ratings: &[TrailRating], now: DateTime<Utc>) -> String {
    let mut out = String::from("\nratings:\n");
    for rating in ratings {
        let _ = writeln!(
            out,
            "  {} {} {} ago  task: {}  why: {}",
            rating.trail,
            rating.outcome,
            short(since(rating.at, now)),
            rating.task,
            rating.why
        );
    }
    out
}

/// Purge the ratings of trail `name`; returns how many went.
pub(super) async fn purge(js: &jetstream::Context, name: &TrailName) -> Result<u64> {
    let stream = js
        .get_stream(format!("KV_{TRAIL_RATINGS_BUCKET}"))
        .await
        .context("opening ants_trail_ratings (`ants init` creates it)")?;
    let purged = stream.purge().filter(format!("$KV.{TRAIL_RATINGS_BUCKET}.{name}.>")).await?;
    Ok(purged.purged)
}

/// What `ants trail rm` adds to its line for the `purged` ratings it removed with the trail, before what it says of the
/// tips, if `tips` it says anything.
pub(super) fn purged(purged: u64, tips: bool) -> String {
    let and = if tips { "," } else { " and" };
    match purged {
        0 => String::new(),
        1 => format!("{and} its 1 rating"),
        n => format!("{and} its {n} ratings"),
    }
}
