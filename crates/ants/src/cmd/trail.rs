//! `ants trail`: save, list, show and remove saved trails, and start, list, show, follow, stop and resume the runs of
//! trails.
//!
//! The saved trails are those of [`ants_proto::TRAILS_BUCKET`], which humans put and remove, and those of
//! `~/.ants/trails/*.py` on this machine ([`saved`]); the runs are the records conductors keep in
//! [`ants_proto::TRAIL_RUNS_BUCKET`], which humans read ([`runs`], [`follow`]), and humans ask a run's conductor to
//! start, stop and resume one ([`control`], [`start`]).

pub(crate) mod control;
mod follow;
pub(crate) mod runs;
pub(crate) mod saved;
pub(crate) mod start;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::str::FromStr;

use ants_proto::{RunId, TrailLimits, TrailName, UserId, Uuid};
use anyhow::{Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::completions;
use crate::connection::Nats;
use crate::threads::thread;

#[derive(Subcommand, Debug)]
pub(crate) enum TrailCommand {
    /// Save the trail of FILE for the whole colony, under the name its meta gives or --name, replacing a saved trail of
    /// that name. The file is a Python script whose first statement is a literal `meta = {"name": ..., "description":
    /// ..., "phases": [...]}`; it is checked, not run. Needs a human's credentials.
    Save {
        /// The trail's script, a .py file.
        file: PathBuf,
        /// Save it under this name rather than its meta's: 1-64 of a-z, 0-9 and '-'.
        #[arg(long)]
        name: Option<TrailName>,
    },
    /// List the saved trails with their description, phases and source: the colony's, with who saved each and when,
    /// then those of ~/.ants/trails/*.py on this machine. Where both have a name, the colony's runs.
    List {
        /// One JSON object per trail.
        #[arg(long)]
        json: bool,
    },
    /// Show a saved trail's meta and script, the colony's before a local one of the same name; or, given a run's id
    /// (a UUID, as `ants trail runs` lists, or a unique start of one no trail is named), the run: its record, limits,
    /// pending calls and journal.
    Show {
        /// A trail's name, or a run's id.
        what: Shown,
        /// One JSON object.
        #[arg(long)]
        json: bool,
        /// Follow the run instead, as `ants trail run --follow` does, from its first step on.
        #[arg(long)]
        follow: bool,
    },
    /// Remove a trail the colony saved. A local trail in ~/.ants/trails stays: delete its file. Needs a human's
    /// credentials.
    Rm {
        /// The trail's name.
        name: TrailName,
    },
    /// Start a run of a trail: a saved one, the colony's before a local one of the same name, or the script of a .py
    /// file, checked here and sent as is. Prints what the run may spend and asks to confirm, unless --yes. The run's
    /// conductor, the ant whose daemon runs it, defaults to whom `ants ask` without @ant goes: the human's assistant,
    /// else the messenger. Needs a human's credentials.
    Run(RunArgs),
    /// Stop a run: it sends no further step and ignores the replies of the steps it waited on. Prints the steps still
    /// waiting on other ants, with the command withdrawing each from that ant's queue. Takes a run's id or a unique
    /// start of one. Needs a human's credentials.
    Stop {
        /// The run's id, or a unique start of one.
        run: runs::Pick,
    },
    /// Resume a stopped run, or one failed on a limit, from its stored record: the steps it waited on go out anew, on
    /// new threads. Limits given replace the run's own. Needs a human's credentials.
    Resume {
        /// The run's id, or a unique start of one.
        run: runs::Pick,
        #[command(flatten)]
        limits: LimitArgs,
        /// Follow the run then, as `ants trail run --follow` does.
        #[arg(long)]
        follow: bool,
    },
    /// List the runs of trails conductors recorded, oldest first: run id, trail, conductor, owner, state, phase, steps
    /// and turns spent of their limits, and when each started and ended. Needs a human's credentials.
    Runs {
        /// Only the runs this ant conducts.
        #[arg(long, value_name = "CONDUCTOR", add = ArgValueCandidates::new(completions::ants))]
        ant: Option<UserId>,
        /// One JSON object per run: its record, without the script.
        #[arg(long)]
        json: bool,
    },
}

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
    /// it is done, non-zero once it failed or stopped; interrupting stops the following, not the run.
    #[arg(long)]
    pub(crate) follow: bool,
    /// Print the started run as one JSON object (run, conductor, thread), and with --follow the run's record once it
    /// ends, rather than its steps.
    #[arg(long)]
    pub(crate) json: bool,
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

/// What `ants trail show` shows: a run, named by its id, a UUID; otherwise a saved trail. `ants trail save` refuses a
/// name reading as a UUID, so no trail hides behind a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Shown {
    Trail(TrailName),
    Run(RunId),
}

impl FromStr for Shown {
    type Err = ants_proto::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if Uuid::try_parse(s).is_ok() { s.parse().map(Self::Run) } else { s.parse().map(Self::Trail) }
    }
}

pub(crate) async fn run(nats: &Nats<'_>, command: TrailCommand) -> Result<()> {
    match command {
        TrailCommand::Save { file, name } => {
            human(nats, "saving a trail")?;
            let trail = saved::read(&file, name, &nats.me.id, Utc::now())?;
            let (_, js) = nats.connect("ants-cli").await?;
            match saved::put(&js, &trail).await? {
                Some(before) => println!(
                    "replaced trail {}, saved by {} {} ago",
                    trail.name,
                    before.saved_by,
                    crate::colony::short(crate::colony::since(before.saved_at, trail.saved_at))
                ),
                None => println!("saved trail {}", trail.name),
            }
        }
        TrailCommand::List { json } => {
            let (_, js) = nats.connect("ants-cli").await?;
            let listed = saved_trails(&js).await?;
            if json {
                for entry in &listed {
                    println!("{}", serde_json::to_string(entry)?);
                }
            } else if listed.is_empty() {
                println!("no saved trails");
            } else {
                println!("{}", saved::table(&listed, Utc::now()));
            }
        }
        TrailCommand::Show { what, json, follow } => {
            if matches!(what, Shown::Run(_)) {
                human(nats, "reading a trail's runs")?;
            }
            let (_, js) = nats.connect("ants-cli").await?;
            match find(&js, what, nats.me.role.is_human()).await? {
                Found::Trail(found) if follow => bail!("--follow follows a run; {} is a trail", found.trail.name),
                Found::Trail(found) if json => println!("{}", serde_json::to_string(&saved::Shown::from(&found))?),
                Found::Trail(found) => print!("{}", saved::render(&found, Utc::now())),
                Found::Run(run) => show_run(nats, &js, run, json, follow).await?,
            }
        }
        TrailCommand::Run(args) => start::run(nats, args, start::ask_terminal).await?,
        TrailCommand::Stop { run } => {
            human(nats, "stopping a run")?;
            let (client, js) = nats.connect("ants-cli").await?;
            let header = runs::pick(&js, &run).await?;
            print!("{}", control::stop(&client, &nats.me.id, &header).await?);
        }
        TrailCommand::Resume { run, limits, follow } => {
            human(nats, "resuming a run")?;
            let (client, js) = nats.connect("ants-cli").await?;
            let header = runs::pick(&js, &run).await?;
            let limits = if limits.given() { Some(limits.over(header.limits)?) } else { None };
            println!("{}", control::resume(&client, &nats.me.id, &header, limits).await?);
            if follow {
                follow::follow(&js, header, false).await?;
            }
        }
        TrailCommand::Rm { name } => {
            human(nats, "removing a trail")?;
            let (_, js) = nats.connect("ants-cli").await?;
            saved::remove(&js, &saved::local_dir()?, &name).await?;
            println!("removed trail {name}");
        }
        TrailCommand::Runs { ant, json } => {
            human(nats, "reading the runs of trails")?;
            let (_, js) = nats.connect("ants-cli").await?;
            let listed = runs::list(&js, ant.as_ref()).await?;
            if json {
                for run in &listed {
                    println!("{}", runs::header_json(run)?);
                }
            } else if listed.is_empty() {
                match ant {
                    Some(ant) => println!("no runs of trails conducted by {ant}"),
                    None => println!("no runs of trails"),
                }
            } else {
                println!("{}", runs::table(&listed, Utc::now()));
            }
        }
    }
    Ok(())
}

/// Show the run `run`, or with `follow` follow it.
async fn show_run(nats: &Nats<'_>, js: &jetstream::Context, run: RunId, json: bool, follow: bool) -> Result<()> {
    human(nats, "reading a trail's runs")?;
    if follow {
        return follow::follow(js, runs::find(js, &run).await?, json).await;
    }
    if json {
        println!("{}", serde_json::to_string(&runs::detail(js, &run).await?)?);
    } else {
        print!("{}", run_shown(js, &run, Utc::now()).await?);
    }
    Ok(())
}

/// The saved trails `ants trail list` lists: the colony's, then this machine's.
pub(crate) async fn saved_trails(js: &jetstream::Context) -> Result<Vec<saved::Listed>> {
    Ok(saved::listing(saved::colony(js).await?, saved::local(&saved::local_dir()?)))
}

/// What `ants trail show` found.
pub(crate) enum Found {
    Trail(saved::Found),
    Run(RunId),
}

/// What `what` names: a run, by its id; else the saved trail of that name, the colony's before a local one; else, for
/// a `human`, the one run whose id starts so.
pub(crate) async fn find(js: &jetstream::Context, what: Shown, human: bool) -> Result<Found> {
    let name = match what {
        Shown::Run(run) => return Ok(Found::Run(run)),
        Shown::Trail(name) => name,
    };
    let e = match saved::find(js, &saved::local_dir()?, &name).await {
        Ok(found) => return Ok(Found::Trail(found)),
        Err(e) => e,
    };
    let Some(pick) = name.as_str().parse::<runs::Pick>().ok().filter(|_| human) else { return Err(e) };
    let Ok(run) = runs::pick(js, &pick).await else { return Err(e) };
    Ok(Found::Run(run.run))
}

/// The run `run` as `ants trail show RUN` prints it at `now`.
pub(crate) async fn run_shown(js: &jetstream::Context, run: &RunId, now: chrono::DateTime<Utc>) -> Result<String> {
    Ok(runs::render(&runs::detail(js, run).await?, now))
}

/// Refuse the credentials of an ant for `what`, which the server allows only a human.
fn human(nats: &Nats<'_>, what: &str) -> Result<()> {
    if !nats.me.role.is_human() {
        bail!("{what} needs a human's credentials; those of {} are an ant's", nats.me.id);
    }
    Ok(())
}

/// `rows` as columns two spaces apart, each as wide as its widest cell, without trailing blanks.
fn columns<const N: usize>(rows: &[[String; N]]) -> String {
    let mut widths = [0; N];
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().zip(widths).map(|(cell, width)| format!("{cell:width$}")).collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect();
    lines.join("\n")
}
