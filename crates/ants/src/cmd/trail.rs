//! `ants trail`: save, list, show and remove saved trails, and list and show the runs of trails.
//!
//! The saved trails are those of [`ants_proto::TRAILS_BUCKET`], which humans put and remove, and those of
//! `~/.ants/trails/*.py` on this machine ([`saved`]); the runs are the records conductors keep in
//! [`ants_proto::TRAIL_RUNS_BUCKET`], which humans read ([`runs`]).

mod runs;
mod saved;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::str::FromStr;

use ants_proto::{RunId, TrailName, UserId, Uuid};
use anyhow::{Result, bail};
use chrono::Utc;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::completions;
use crate::connection::Nats;

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
    /// (a UUID, as `ants trail runs` lists), the run: its record, limits, pending calls and journal.
    Show {
        /// A trail's name, or a run's id.
        what: Shown,
        /// One JSON object.
        #[arg(long)]
        json: bool,
    },
    /// Remove a trail the colony saved. A local trail in ~/.ants/trails stays: delete its file. Needs a human's
    /// credentials.
    Rm {
        /// The trail's name.
        name: TrailName,
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
            let listed = saved::listing(saved::colony(&js).await?, saved::local(&saved::local_dir()?));
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
        TrailCommand::Show { what: Shown::Trail(name), json } => {
            let (_, js) = nats.connect("ants-cli").await?;
            let found = saved::find(&js, &saved::local_dir()?, &name).await?;
            if json {
                println!("{}", serde_json::to_string(&saved::Shown::from(&found))?);
            } else {
                print!("{}", saved::render(&found, Utc::now()));
            }
        }
        TrailCommand::Show { what: Shown::Run(run), json } => {
            human(nats, "reading a trail's runs")?;
            let (_, js) = nats.connect("ants-cli").await?;
            let detail = runs::detail(&js, &run).await?;
            if json {
                println!("{}", serde_json::to_string(&detail)?);
            } else {
                print!("{}", runs::render(&detail, Utc::now()));
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
