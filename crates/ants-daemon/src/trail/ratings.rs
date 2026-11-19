//! The ratings conductors give their runs' saved trails, in [`TRAIL_RATINGS_BUCKET`]: each ant puts only its own, under
//! keys naming it, and reads every trail's by direct gets naming no subject; and the searches ranking the saved trails
//! by them ([`Trails::find`]). Nothing is cached: each search reads the bucket anew and folds it as it comes, as does
//! the reading of the colony's saved trails with their ratings counted for a sage's review packet ([`colony_rated`]).

use std::collections::BTreeMap;

use ants_proto::{
    Counts, Finder, Found, JsDomain, Query, RatingOutcome, SavedTrail, TRAIL_RATINGS_BUCKET, TRAILS_BUCKET, ThreadId,
    TrailName, TrailRating, TrailRun, UserId, script_sha256, serves,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::Utc;

use super::Trails;
use super::saved::SavedEntry;
use crate::records::direct_kv::DirectKv;

/// The ratings of the colony's trails. Cheap to clone.
#[derive(Clone)]
pub struct Ratings {
    ant: UserId,
    kv: DirectKv,
}

impl Ratings {
    /// The ratings in [`TRAIL_RATINGS_BUCKET`], put as `ant`, without a request: whether the bucket exists shows on the
    /// first read or write. `js_domain` is the domain `js` was made for ([`crate::bus::jetstream`]).
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: UserId) -> Result<Self> {
        Ok(Self { ant, kv: DirectKv::whole(js, js_domain, TRAIL_RATINGS_BUCKET).await?.past_relays() })
    }

    /// Store `rating`, one of this ant's runs, under its key, replacing an earlier rating of the run and trail.
    pub async fn put(&self, rating: &TrailRating) -> Result<()> {
        rating.check()?;
        if rating.conductor != self.ant {
            bail!("{} rates only its own runs, not {}'s", self.ant, rating.conductor);
        }
        let key = rating.key();
        self.kv.put(&key, serde_json::to_vec(rating)?).await.with_context(|| format!("storing rating {key}"))
    }

    /// The rating under `key`, if one is stored there and reads.
    pub async fn get(&self, key: &str) -> Result<Option<TrailRating>> {
        let Some(value) = self.kv.get(key).await? else { return Ok(None) };
        let rating: Option<TrailRating> = serde_json::from_slice(&value).ok();
        Ok(rating.filter(|rating| rating.check_key(key).is_ok()))
    }

    /// Hand `each` every rating of `trail`, or of every trail, that reads, as last put.
    pub async fn fold(&self, trail: Option<&TrailName>, mut each: impl FnMut(TrailRating)) -> Result<()> {
        let filter = trail.map_or_else(|| ">".to_owned(), |trail| format!("{trail}.>"));
        self.kv
            .scan(&filter, |key, value| match serde_json::from_slice::<TrailRating>(value) {
                Ok(rating) if rating.check_key(key).is_ok() => each(rating),
                _ => tracing::debug!(key, "skipping a trail rating that does not read"),
            })
            .await
            .context("reading the trail ratings")
    }
}

/// The colony's saved trails in [`TRAILS_BUCKET`], not this machine's local ones, each with its ratings counted by
/// outcome, those of trails no longer saved left out. Fails when either bucket cannot be read.
pub async fn colony_rated(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<Vec<(SavedTrail, Counts)>> {
    let mut trails = BTreeMap::new();
    DirectKv::whole(js, js_domain, TRAILS_BUCKET)
        .await?
        .scan(">", |key, value| match serde_json::from_slice::<SavedTrail>(value) {
            Ok(trail) if trail.check_key(key).is_ok() => {
                trails.insert(trail.name.clone(), (trail, Counts::default()));
            }
            _ => tracing::debug!(key, "skipping a saved trail that does not read"),
        })
        .await
        .context("reading the saved trails of the colony")?;
    DirectKv::whole(js, js_domain, TRAIL_RATINGS_BUCKET)
        .await?
        .scan(">", |key, value| match serde_json::from_slice::<TrailRating>(value) {
            Ok(rating) if rating.check_key(key).is_ok() => {
                if let Some((_, counts)) = trails.get_mut(&rating.trail) {
                    counts.add(rating.outcome);
                }
            }
            _ => tracing::debug!(key, "skipping a trail rating that does not read"),
        })
        .await
        .context("reading the trail ratings")?;
    Ok(trails.into_values().collect())
}

/// A saved trail a run's rating rated.
pub struct RatedTrail {
    pub trail: TrailName,
    /// The outcome of the rating of the run it replaced, if any.
    pub replaced: Option<RatingOutcome>,
    /// Whether the trail was saved from the run's script rather than taken by the run.
    pub saved_from: bool,
}

/// The saved trails `run` took, each with the [`script_sha256`] of the script its last leg of the trail ran, in the
/// order the route first took them.
pub(crate) fn route_trails(run: &TrailRun) -> Vec<(TrailName, String)> {
    let mut trails: Vec<(TrailName, String)> = Vec::new();
    for leg in &run.route {
        let Some(trail) = &leg.trail else { continue };
        let script = script_sha256(&leg.script);
        match trails.iter_mut().find(|(name, _)| name == trail) {
            Some(taken) => taken.1 = script,
            None => trails.push((trail.clone(), script)),
        }
    }
    trails
}

impl Trails {
    /// The ratings of the colony's trails, read and put under the ant's credentials.
    pub fn ratings(&self) -> &Ratings {
        &self.inner.ratings
    }

    /// Every saved trail the conductor may run, the colony's and the machine's.
    pub async fn saved_trails(&self) -> Result<Vec<SavedEntry>, String> {
        self.inner.saved.all().await
    }

    /// Rate each saved trail run `run` took, ended or stopped, and each the colony saved from its script, as
    /// `outcome` for `task`, because of `why`: one rating per trail, replacing an earlier rating of the run. A trail
    /// taken is rated of the script the run ran, one saved from the run of its script now.
    pub async fn rate(
        &self,
        run: &TrailRun,
        outcome: RatingOutcome,
        task: &str,
        why: &str,
    ) -> Result<Vec<RatedTrail>, String> {
        let at = Utc::now();
        let mut trails: Vec<(TrailName, String, bool)> =
            route_trails(run).into_iter().map(|(trail, script)| (trail, script, false)).collect();
        for saved in self.saved_trails().await? {
            if saved.saved_from.as_ref() == Some(&run.run) && !trails.iter().any(|(name, ..)| *name == saved.name) {
                trails.push((saved.name, saved.script, true));
            }
        }
        let ratings: Vec<(TrailRating, bool)> = trails
            .into_iter()
            .map(|(trail, script, saved_from)| {
                let rating = TrailRating {
                    trail,
                    conductor: self.inner.ant.clone(),
                    run: run.run.clone(),
                    outcome,
                    task: task.trim().to_owned(),
                    why: why.trim().to_owned(),
                    script,
                    at,
                };
                (rating, saved_from)
            })
            .collect();
        for (rating, _) in &ratings {
            rating.check().map_err(|e| e.to_string())?;
        }
        let store = &self.inner.ratings;
        let mut rated = Vec::new();
        for (rating, saved_from) in ratings {
            let key = rating.key();
            let earlier = store.get(&key).await.map_err(|e| format!("reading rating {key}: {e:#}"))?;
            store.put(&rating).await.map_err(|e| format!("{e:#}"))?;
            rated.push(RatedTrail {
                trail: rating.trail,
                replaced: earlier.map(|earlier| earlier.outcome),
                saved_from,
            });
        }
        Ok(rated)
    }

    /// The saved trails for `task`, ranked as a search on `thread` ranks them ([`Finder`]), at most `limit`.
    pub async fn find(&self, task: &str, thread: &ThreadId, limit: usize) -> Result<Vec<Found<SavedEntry>>, String> {
        let trails = self.saved_trails().await?;
        let mut finder = Finder::new(Query::new(task), Utc::now());
        for trail in trails {
            let (name, description, script) = (trail.name.clone(), trail.description.clone(), trail.script.clone());
            finder.trail(name, &description, Some(script), trail);
        }
        self.inner.ratings.fold(None, |rating| finder.rating(rating)).await.map_err(|e| format!("{e:#}"))?;
        Ok(finder.rank(thread, limit))
    }

    /// The saved trails that already serve `task` ([`serves`]): those whose name and description match it with more of
    /// their ratings on matching tasks worked than failed, ranked as a search on `thread` ranks them.
    pub async fn serving(&self, task: &str, thread: &ThreadId) -> Result<Vec<Found<SavedEntry>>, String> {
        let query = Query::new(task);
        let mut found = self.find(task, thread, usize::MAX).await?;
        found.retain(|found| serves(query.matches_trail(&found.name, &found.item.description), &found.matching.counts));
        Ok(found)
    }

    /// The saved trails whose name and description match `description` with more of all their ratings worked than
    /// failed, best first, at most `limit`, each with its counts: those a run of a script so described could take.
    pub async fn well_rated(&self, description: &str, limit: usize) -> Result<Vec<(TrailName, Counts)>, String> {
        let query = Query::new(description);
        let mut found = Vec::new();
        for trail in self.saved_trails().await? {
            if !query.matches_trail(&trail.name, &trail.description) {
                continue;
            }
            let mut counts = Counts::default();
            let read = self.inner.ratings.fold(Some(&trail.name), |rating| counts.add(rating.outcome)).await;
            read.map_err(|e| format!("{e:#}"))?;
            if serves(true, &counts) {
                found.push((trail.name, counts));
            }
        }
        found.sort_by(|(a, x), (b, y)| {
            let rate = |c: &Counts| c.rate().unwrap_or_default();
            rate(y).total_cmp(&rate(x)).then(y.total().cmp(&x.total())).then_with(|| a.cmp(b))
        });
        found.truncate(limit);
        Ok(found)
    }
}
