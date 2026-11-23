//! Trails as `ants trail` ([`crate::cmd::trail`]), `ants chat`, admin orders and `ants doctor` reach them: the saved
//! trails, the tips ants left on their steps, the ratings and the runs of trails, started, listed, shown, followed,
//! stopped and resumed.
//!
//! The saved trails are those of [`ants_proto::TRAILS_BUCKET`], which humans and ants put and remove, and those of
//! `~/.ants/trails/*.py` on this machine ([`saved`]); the tips are those of [`ants_proto::TRAIL_TIPS_BUCKET`], which
//! humans retire and purge with their trail ([`tips`]); the ratings are those of
//! [`ants_proto::TRAIL_RATINGS_BUCKET`], which conductors give their runs and humans purge with their trail
//! ([`ratings`]); the runs are the records conductors keep in
//! [`ants_proto::TRAIL_RUNS_BUCKET`], which humans read ([`runs`], [`follow`]), and humans ask a run's conductor to
//! start, stop and resume one ([`control`], [`start`]).

pub(crate) mod control;
pub(crate) mod follow;
pub(crate) mod ratings;
pub(crate) mod runs;
pub(crate) mod saved;
pub(crate) mod start;
#[cfg(test)]
mod tests;
pub(crate) mod tips;

use std::str::FromStr;

use ants_proto::{RunId, TipId, TrailLimits, TrailName, UserId, Uuid};
use anyhow::{Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use clap_complete::engine::ArgValueCandidates;

use crate::completions;
use crate::connection::Nats;
use crate::threads::thread;

/// `ants trail run`.
#[derive(clap::Args, Debug)]
pub(crate) struct RunArgs {
    /// A saved trail's name, or a file ending in .py.
    pub(crate) trail: start::Trail,
    /// The script's `args`, a JSON value.
    #[arg(long, value_name = "JSON", value_parser = start::json_args)]
    pub(crate) args: Option<serde_json::Value>,
    /// The conductor: the ant whose daemon runs the script and sends its steps.
    #[arg(long, value_name = "CONDUCTOR", add = ArgValueCandidates::new(completions::ants))]
    pub(crate) ant: Option<UserId>,
    /// The thread the run reports on, which the human owns: its id, a unique start of one, or `last`. A new thread by
    /// default.
    #[arg(long, value_name = "THREAD", add = ArgValueCandidates::new(completions::threads))]
    pub(crate) thread: Option<thread::Pick>,
    #[command(flatten)]
    pub(crate) limits: LimitArgs,
    /// Start without asking.
    #[arg(long, short = 'y')]
    pub(crate) yes: bool,
    /// Follow the run until it ends: its phase, steps sent, answered and queued, and its result or error. Exits 0 once
    /// it is done, non-zero once it has failed or stopped; interrupting stops the following, not the run.
    #[arg(long)]
    pub(crate) follow: bool,
    /// Print the started run as one JSON object (run, conductor, thread), and with --follow the run's record once it
    /// ends, rather than its steps.
    #[arg(long)]
    pub(crate) json: bool,
}

/// `ants trail tips`.
#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
pub(crate) struct TipsArgs {
    /// The trail's name.
    pub(crate) name: TrailName,
    /// Only the tips under this tip key, the `tips=` of the steps they were left on.
    #[arg(long, value_name = "KEY")]
    pub(crate) key: Option<String>,
    /// The retired tips too, after the live ones.
    #[arg(long)]
    pub(crate) all: bool,
    /// Retire the tip ID instead, which steps carry and searches find no more. Needs a human's credentials.
    #[arg(long, value_name = "ID", conflicts_with_all = ["key", "all"])]
    pub(crate) retire: Option<TipId>,
    /// With --retire, the tip's writer: needed only when tips of different writers share the id.
    #[arg(long, value_name = "ANT", requires = "retire")]
    pub(crate) writer: Option<UserId>,
}

/// What a run may spend, each limit at most its maximum ([`TrailLimits::MAX`]).
#[derive(clap::Args, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LimitArgs {
    /// Steps the run sends in all (50 by default, at most 500).
    #[arg(long, value_name = "N")]
    pub(crate) steps: Option<u32>,
    /// Steps waiting on an answer at once (4 by default, at most 16).
    #[arg(long, value_name = "N")]
    pub(crate) at_once: Option<u32>,
    /// Turns the run's steps spend in all, as ants' messages spend them (200 by default, at most 2000).
    #[arg(long, value_name = "N")]
    pub(crate) turns: Option<u32>,
    /// Time from the run's start to its end, stopped time included, such as 2h or 3d (24h by default, at most 7d).
    #[arg(long, value_name = "DUR", value_parser = ants_proto::duration::parse)]
    pub(crate) wall: Option<std::time::Duration>,
}

/// What a resumed run may spend: [`LimitArgs`], each limit left out keeping the run's own.
#[derive(clap::Args, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ResumeLimitArgs {
    /// Steps the run sends in all, at most 500; left out, the run's own limit.
    #[arg(long, value_name = "N")]
    pub(crate) steps: Option<u32>,
    /// Steps waiting on an answer at once, at most 16; left out, the run's own limit.
    #[arg(long, value_name = "N")]
    pub(crate) at_once: Option<u32>,
    /// Turns the run's steps spend in all, as ants' messages spend them, at most 2000; left out, the run's own limit.
    #[arg(long, value_name = "N")]
    pub(crate) turns: Option<u32>,
    /// Time from the run's start to its end, stopped time included, such as 2h or 3d, at most 7d; left out, the run's
    /// own limit.
    #[arg(long, value_name = "DUR", value_parser = ants_proto::duration::parse)]
    pub(crate) wall: Option<std::time::Duration>,
}

impl From<ResumeLimitArgs> for LimitArgs {
    fn from(ResumeLimitArgs { steps, at_once, turns, wall }: ResumeLimitArgs) -> Self {
        Self { steps, at_once, turns, wall }
    }
}

impl LimitArgs {
    pub(crate) fn given(&self) -> bool {
        self.steps.is_some() || self.at_once.is_some() || self.turns.is_some() || self.wall.is_some()
    }

    /// `base` with the limits given instead: refused when one is 0 or over its maximum.
    pub(crate) fn over(&self, base: TrailLimits) -> Result<TrailLimits> {
        let max = TrailLimits::MAX;
        let wall = self.wall.map(|wall| wall.as_secs());
        let given = [
            ("--steps", self.steps.map(u64::from), u64::from(max.steps), max.steps.to_string()),
            ("--at-once", self.at_once.map(u64::from), u64::from(max.concurrency), max.concurrency.to_string()),
            ("--turns", self.turns.map(u64::from), u64::from(max.turns), max.turns.to_string()),
            ("--wall", wall, max.wall_secs, ants_proto::duration::format(max.wall_time())),
        ];
        for (flag, value, most, shown) in given {
            match value {
                Some(0) => bail!("{flag} must be more than 0"),
                Some(value) if value > most => bail!("{flag} is over the most a run may take, {shown}"),
                _ => {}
            }
        }
        Ok(TrailLimits {
            steps: self.steps.unwrap_or(base.steps),
            concurrency: self.at_once.unwrap_or(base.concurrency),
            turns: self.turns.unwrap_or(base.turns),
            wall_secs: wall.unwrap_or(base.wall_secs),
            ..base
        })
    }
}

/// What `ants trail show` shows: a run, named by its id, a UUID; otherwise a saved trail, or a start of a run's id no
/// trail's name can be, as in capitals. `ants trail save` refuses a name reading as a UUID, so no trail hides behind a
/// run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Shown {
    Trail(TrailName),
    Run(RunId),
    Pick(runs::Pick),
}

impl FromStr for Shown {
    type Err = ants_proto::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if Uuid::try_parse(s).is_ok() {
            return s.to_ascii_lowercase().parse().map(Self::Run);
        }
        s.parse().map(Self::Trail).or_else(|e| s.parse().map(Self::Pick).map_err(|_| e))
    }
}

/// The saved trails `ants trail list` lists: the colony's, then this machine's, with their ratings.
pub(crate) async fn saved_trails(js: &jetstream::Context) -> Result<Vec<saved::Listed>> {
    let listed = saved::listing(saved::colony(js).await?, saved::local(&saved::local_dir()?));
    Ok(saved::rated(listed, &ratings::summaries(ratings::read_or_none(js, None).await)))
}

/// What `ants trail show` found.
pub(crate) enum Found {
    Trail(Box<saved::Found>),
    Run(RunId),
}

/// What `what` names: a run, by its id; else the saved trail of that name, the colony's before a local one; else, for
/// a `human`, the one run whose id starts so. Several runs' ids starting so are refused, naming them.
pub(crate) async fn find(js: &jetstream::Context, what: Shown, human: bool) -> Result<Found> {
    let name = match what {
        Shown::Run(run) => return Ok(Found::Run(run)),
        Shown::Pick(pick) => return Ok(Found::Run(runs::pick(js, &pick).await?.run)),
        Shown::Trail(name) => name,
    };
    let e = match saved::find(js, &saved::local_dir()?, &name).await {
        Ok(mut found) => {
            found.ratings = ratings::of_trail(js, &name).await.unwrap_or_else(|e| {
                tracing::warn!("not reading the ratings of trail {name}: {e:#}");
                None
            });
            return Ok(Found::Trail(Box::new(found)));
        }
        Err(e) => e,
    };
    let Some(pick) = name.as_str().parse::<runs::Pick>().ok().filter(|_| human) else { return Err(e) };
    let listed = runs::list(js, None).await?;
    if !listed.iter().any(|run| pick.starts(&run.run)) {
        return Err(e);
    }
    Ok(Found::Run(runs::picked(listed, &pick, "`ants trail runs` lists the runs kept")?.run))
}

/// The run `run` as `ants trail show RUN` prints it at `now`.
pub(crate) async fn run_shown(js: &jetstream::Context, run: &RunId, now: chrono::DateTime<Utc>) -> Result<String> {
    Ok(runs::render(&runs::detail(js, run).await?, now))
}

/// Refuse the credentials of an ant for `what`, which the server allows only a human.
pub(crate) fn human(nats: &Nats<'_>, what: &str) -> Result<()> {
    if !nats.me.role.is_human() {
        bail!("{what} needs a human's credentials; those of {} are an ant's", nats.me.id);
    }
    Ok(())
}

/// `rows` as columns two spaces apart, each as wide as its widest cell, without trailing blanks.
pub(crate) fn columns(rows: &[impl AsRef<[String]>]) -> String {
    let mut widths: Vec<usize> = Vec::new();
    for row in rows {
        let row = row.as_ref();
        widths.resize(widths.len().max(row.len()), 0);
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let cells: Vec<String> =
                row.as_ref().iter().zip(&widths).map(|(cell, &width)| format!("{cell:width$}")).collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect();
    lines.join("\n")
}
