//! Rating the runs an ant conducts and finding the colony's saved trails by those ratings ([`crate::trail::Ratings`]):
//! `rate_trail` rates each saved trail a run took, or was saved from, for the task it served; `find_trails` ranks the
//! saved trails for a task by a draw from their ratings on matching tasks. A thread a webhook or schedule started
//! rates only the run reporting on it, the one its run's end invites a rating of, and the turn of that invitation
//! ([`ants_proto::Envelope::rating_of`]) that run alone; it finds trails as any thread does.

use std::fmt::Write as _;

use ants_proto::{
    FIND_TRAILS_LIMIT, Found, LATEST_RATINGS, MAX_FIND_TRAILS, MAX_RATING_TASK, MAX_RATING_WHY, Query, RatingOutcome,
    RunState, ThreadId, TrailRun,
};
use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::tools::{Tools, text};
use crate::trail::{RatedTrail, SavedEntry, age, clip};

/// Why rating and finding are refused when the ant conducts no runs.
const NO_TRAILS: &str = "this ant conducts no trails: the bus keeps no trail runs";
/// Why a rating is refused on an untrusted thread.
const UNTRUSTED: &str = "this thread is untrusted: a webhook or schedule started it, or the trail run whose step it \
                         answers; from it, only the run reporting on it is rated, and while an invitation to rate one \
                         is handled, only that one";
/// Most characters of a rating's reason `find_trails` quotes.
const MAX_REASON_SHOWN: usize = 200;

#[derive(Deserialize, schemars::JsonSchema)]
struct RateTrail {
    /// The id of a run this ant conducted, ended or stopped, as run_trail or the invitation to rate it gave it; a
    /// child run a trail() call started is rated by its own id.
    run: String,
    /// How the run served the task: `worked`, `partly` or `failed`.
    outcome: String,
    /// The task the run served, in a line, as later searches for such a task would name it: at most 300 characters.
    task: String,
    /// Why, concretely: what made it work, or what went wrong. At most 500 characters.
    why: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct FindTrails {
    /// The task, in a few words, such as `review rust changes`.
    query: String,
    /// How many trails to list: default 5, at most 20.
    #[serde(default)]
    limit: Option<usize>,
}

impl Hub {
    /// Rate run `args.run` as `thread` asks.
    async fn rate_trail(&self, thread: &ThreadId, args: RateTrail) -> Result<String, String> {
        let trails = self.trails.get().ok_or(NO_TRAILS)?;
        let current = self
            .handling(thread)
            .ok_or("no message is being handled on this thread; runs are rated only while one is")?;
        let run = args.run.trim().parse().map_err(|e| format!("run {:?}: {e}", args.run))?;
        let outcome: RatingOutcome = args.outcome.trim().parse().map_err(|e| format!("{e}"))?;
        let (task, why) = (args.task.trim(), args.why.trim());
        bounded("task", task, MAX_RATING_TASK)?;
        bounded("reason", why, MAX_RATING_WHY)?;
        let header = trails.run(&run).await?.ok_or_else(|| format!("{} conducts no run {run}", self.ant))?;
        if matches!(header.state, RunState::Running | RunState::Waiting) {
            return Err(format!("run {run} has not ended: a run is rated once it ends or is stopped"));
        }
        let uninvited = current.rating_of.as_ref().is_some_and(|invited| *invited != run);
        if (header.thread != *thread || uninvited) && !self.trusted(thread, &current).await? {
            return Err(UNTRUSTED.to_owned());
        }
        let rated = trails.rate(&header, outcome, task, why).await?;
        tracing::info!(%thread, %run, %outcome, trails = rated.len(), "trail run rated from a tool call");
        let mut out = rated_text(&header, outcome, task, &rated);
        if outcome == RatingOutcome::Worked && header.first_trail().is_none() && !rated.iter().any(|r| r.saved_from) {
            match trails.serving(task, thread).await {
                Ok(serving) => {
                    if let Some(trail) = serving.first() {
                        let _ = write!(
                            out,
                            "\nSaved trail {name} already serves such tasks ({}): run_trail(name: \"{name}\") next \
                             time.",
                            trail.matching.counts,
                            name = trail.name
                        );
                    } else if self.trusted(thread, &current).await? {
                        let _ = write!(
                            out,
                            "\nsave_trail(run: \"{run}\", name: …) saves its script for the colony: move the run's \
                             specific values into args, and have meta's description name the kind of task it serves."
                        );
                    }
                }
                Err(e) => tracing::warn!(%run, error = e, "finding the saved trails serving a rated task"),
            }
        }
        Ok(out)
    }

    /// The saved trails for the task `args.query` names, ranked for a search on `thread`.
    async fn find_trails(&self, thread: &ThreadId, args: FindTrails) -> Result<String, String> {
        let trails = self.trails.get().ok_or(NO_TRAILS)?;
        let query = args.query.trim();
        if Query::new(query).is_empty() {
            return Err("the query has no words to search for: name the task in a few words".to_owned());
        }
        let limit = args.limit.unwrap_or(FIND_TRAILS_LIMIT);
        if !(1..=MAX_FIND_TRAILS).contains(&limit) {
            return Err(format!("limit {limit}: from 1 to {MAX_FIND_TRAILS}"));
        }
        let found = trails.find(query, thread, limit).await?;
        // run_trail(script) and save_trail are refused on an untrusted thread.
        let trusted = match self.handling(thread) {
            Some(current) => self.trusted(thread, &current).await.unwrap_or(false),
            None => false,
        };
        Ok(found_text(query, &found, trusted, Utc::now()))
    }
}

fn bounded(what: &str, text: &str, max: usize) -> Result<(), String> {
    let chars = text.chars().count();
    if chars == 0 {
        return Err(format!("the {what} is empty"));
    }
    if chars > max {
        return Err(format!("the {what} has {chars} characters; a rating's {what} has at most {max}"));
    }
    Ok(())
}

/// What `rate_trail` tells of rating `run` as `outcome` for `task`, `rated` the trails it rated.
fn rated_text(run: &TrailRun, outcome: RatingOutcome, task: &str, rated: &[RatedTrail]) -> String {
    let mut out = if rated.is_empty() {
        format!("Run {} ran an inline script: an inline script has no saved trail to rate.", run.run)
    } else {
        let names: Vec<String> = rated
            .iter()
            .map(|r| if r.saved_from { format!("{} (saved from its script)", r.trail) } else { r.trail.to_string() })
            .collect();
        let plural = if rated.len() > 1 { "trails" } else { "trail" };
        format!("Rated run {} {outcome} for {plural} {} (task: {task:?}).", run.run, names.join(", "))
    };
    let replaced: Vec<(&RatedTrail, RatingOutcome)> =
        rated.iter().filter_map(|r| r.replaced.map(|before| (r, before))).collect();
    if let [(_, before), rest @ ..] = replaced.as_slice()
        && replaced.len() == rated.len()
        && rest.iter().all(|(_, other)| other == before)
    {
        let _ = write!(out, " It replaces the earlier rating ({before}).");
    } else if !replaced.is_empty() {
        let earlier: Vec<String> = replaced.iter().map(|(r, before)| format!("{} {before}", r.trail)).collect();
        let _ = write!(out, " It replaces the earlier ratings of {}.", earlier.join(", "));
    }
    if outcome != RatingOutcome::Worked {
        let _ = write!(out, "\nOther saved trails for this task: find_trails(query: {task:?}).");
    }
    out
}

/// What `find_trails` tells of the trails `found` for `query`, with the hint to write a script when `trusted`: a
/// description and a reason quoted as a task is, so their line breaks and control characters show escaped on one line.
fn found_text(query: &str, found: &[Found<SavedEntry>], trusted: bool, now: DateTime<Utc>) -> String {
    let mut out = if found.is_empty() {
        format!("No saved trail matches {query:?}.")
    } else {
        format!("Saved trails for {query:?}, ranked by a draw from each one's ratings on matching tasks:")
    };
    for (n, trail) in found.iter().enumerate() {
        let local = if trail.item.local { " (local to this machine)" } else { "" };
        let description = clip(trail.item.description.trim(), 300);
        let _ = write!(out, "\n{}. {}{local}: {description:?}", n + 1, trail.name);
        let saver = trail.item.saver.as_ref().map_or_default(|saver| format!("saved by {saver}; "));
        let counts = &trail.matching.counts;
        match counts.rate() {
            None => {
                let _ = write!(out, "\n   {saver}worth exploring: no rating on a matching task yet");
            }
            Some(rate) => {
                let total = counts.total();
                let ratings = if total == 1 { "rating" } else { "ratings" };
                let _ = write!(
                    out,
                    "\n   {saver}success {:.0}% over {total} {ratings} on matching tasks ({counts})",
                    rate * 100.0
                );
            }
        }
        for rating in trail.matching.latest.iter().take(LATEST_RATINGS) {
            let _ = write!(
                out,
                "\n   - {}, {} ago, {:?}: {:?}",
                rating.outcome,
                age(rating.at, now),
                rating.task,
                clip(&rating.why, MAX_REASON_SHOWN)
            );
        }
    }
    if trusted {
        out.push_str(
            "\nA script of this ant's own runs with run_trail(script: …); once it works, save_trail saves it for the \
             colony.",
        );
    }
    out
}

#[tool_router(router = ratings_router, vis = "pub(super)")]
impl Tools {
    #[tool(
        description = "Rate a trail run this ant conducted, once it ended or was stopped: `outcome` worked, partly or \
                       failed for `task`, the task it served in a line, because of `why`, a concrete reason. It rates \
                       each saved trail the run took and each saved from its script, one rating per run and trail: \
                       rating the run again replaces it. A child run a trail() call started is rated by its own id. \
                       Ants choosing a trail with find_trails draw on these ratings, so rate every run. On a thread a \
                       webhook or schedule started, only the run reporting on that thread is rated."
    )]
    async fn rate_trail(
        &self,
        Parameters(args): Parameters<RateTrail>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.rate_trail(&thread, args).await))
    }

    #[tool(
        description = "Find the colony's saved trails for a task before writing a trail-shaped script: `query` names \
                       the task in a few words. Lists at most `limit` (default 5, at most 20) of the trails whose name, \
                       description or rated tasks match, ranked by a draw from each one's ratings on matching tasks: \
                       better-rated trails come first more often, and those not yet rated still get tried. Each comes \
                       with who saved it, its success on matching tasks and its latest ratings. Run one with \
                       run_trail(name: …), then rate the run with rate_trail."
    )]
    async fn find_trails(
        &self,
        Parameters(args): Parameters<FindTrails>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.find_trails(&thread, args).await))
    }
}

#[cfg(test)]
mod tests;
