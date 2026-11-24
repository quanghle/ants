//! `ants trail`: save, list, show and remove saved trails, list and retire the tips ants left on their steps, and
//! start, list, show, follow, stop and resume the runs of trails ([`crate::trails`]).

use std::path::PathBuf;

use ants_proto::{RunId, TrailName, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::completions;
use crate::connection::Nats;
use crate::trails::{
    Found, LimitArgs, ResumeLimitArgs, RunArgs, Shown, TipsArgs, control, find, follow, ratings, run_shown, runs,
    saved, saved_trails, start, tips,
};

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
    /// List the tips ants left on a trail's steps, the live ones in the order steps carry them: id, tip key, writer,
    /// votes (+helpful ~stale !wrong), status, whether it was left on an older script than the one the colony saved,
    /// age and text. Works whether the trail is still saved or not.
    Tips {
        #[command(flatten)]
        tips: TipsArgs,
        /// One JSON object per tip: its record, votes, tally, status, score and older-script mark; with --retire, the
        /// retired tip's record.
        #[arg(long)]
        json: bool,
    },
    /// Remove a trail the colony saved, the tips ants left on its steps and the ratings its runs got. A local trail in
    /// ~/.ants/trails stays: delete its file. Needs a human's credentials.
    Rm {
        /// The trail's name.
        name: TrailName,
    },
    /// Start a run of a trail: a saved one, the colony's before a local one of the same name, or the script of a .py
    /// file, checked here and sent as is. Prints what the run may spend and asks to confirm, unless --yes. The run's
    /// conductor, the ant whose daemon runs it, defaults to where `ants ask` without @ant goes: the human's assistant,
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
        limits: ResumeLimitArgs,
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

pub(crate) async fn run(nats: &Nats<'_>, command: TrailCommand) -> Result<()> {
    match command {
        TrailCommand::Save { file, name } => {
            nats.me.must_be_human("saving a trail")?;
            let trail = saved::read(&file, name, &nats.me.id, Utc::now())?;
            let (_, js) = nats.connect("ants-cli").await?;
            match saved::put(&js, &trail).await? {
                Some(before) => println!(
                    "replaced trail {}, saved by {} {} ago",
                    trail.name,
                    before.saved_by,
                    crate::text::short(crate::text::since(before.saved_at, trail.saved_at))
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
            if matches!(what, Shown::Run(_) | Shown::Pick(_)) {
                nats.me.must_be_human("reading a trail's runs")?;
            }
            let (_, js) = nats.connect("ants-cli").await?;
            match find(&js, what, nats.me.role.is_human()).await? {
                Found::Trail(found) if follow => bail!("--follow follows a run; {} is a trail", found.trail.name),
                Found::Trail(found) if json => println!("{}", serde_json::to_string(&saved::Shown::from(&*found))?),
                Found::Trail(found) => print!("{}", saved::render(&found, Utc::now())),
                Found::Run(run) => show_run(nats, &js, run, json, follow).await?,
            }
        }
        TrailCommand::Run(args) => start::run(nats, args, crate::tty::confirm_on_stderr).await?,
        TrailCommand::Stop { run } => {
            nats.me.must_be_human("stopping a run")?;
            let (client, js) = nats.connect("ants-cli").await?;
            let header = runs::pick(&js, &run).await?;
            print!("{}", control::stop(&client, &nats.me.id, &header).await?.said(&header));
        }
        TrailCommand::Resume { run, limits, follow } => {
            nats.me.must_be_human("resuming a run")?;
            let (client, js) = nats.connect("ants-cli").await?;
            let header = runs::pick(&js, &run).await?;
            control::resumable(&header)?;
            let limits = LimitArgs::from(limits);
            let limits = if limits.given() { Some(limits.over(header.limits)?) } else { None };
            println!("{}", control::resume(&client, &nats.me.id, &header, limits).await?);
            if follow {
                follow::follow(&js, header, false).await?;
            }
        }
        TrailCommand::Tips { tips: args, json } => show_tips(nats, &args, json).await?,
        TrailCommand::Rm { name } => {
            nats.me.must_be_human("removing a trail")?;
            let (_, js) = nats.connect("ants-cli").await?;
            let purged = saved::remove(&js, &saved::local_dir()?, &name).await?;
            let rated = ratings::purged(purged.ratings, purged.tips > 0);
            let removed = if purged.trail { "removed trail" } else { "removed what was left of trail" };
            println!("{removed} {name}{rated}{}", tips::purged(purged.tips));
        }
        TrailCommand::Runs { ant, json } => {
            nats.me.must_be_human("reading the runs of trails")?;
            let (_, js) = nats.connect("ants-cli").await?;
            let listed = runs::list(&js, ant.as_ref()).await?;
            let rated = ratings::of_runs(&ratings::read_or_none(&js, ant.as_ref()).await);
            if json {
                for run in &listed {
                    println!("{}", runs::header_json(run, &rated)?);
                }
            } else if listed.is_empty() {
                match ant {
                    Some(ant) => println!("no runs of trails conducted by {ant}"),
                    None => println!("no runs of trails"),
                }
            } else {
                println!("{}", runs::table(&listed, &rated, Utc::now()));
            }
        }
    }
    Ok(())
}

/// List the tips of the trail `args` names, or retire the one `--retire` names.
async fn show_tips(nats: &Nats<'_>, args: &TipsArgs, json: bool) -> Result<()> {
    if args.retire.is_some() {
        nats.me.must_be_human("retiring a tip")?;
    }
    let (_, js) = nats.connect("ants-cli").await?;
    if let Some(id) = &args.retire {
        let retired = tips::retire(&js, &args.name, id, args.writer.as_ref(), &nats.me.id, Utc::now()).await?;
        if json {
            println!("{}", serde_json::to_string(&retired)?);
        } else {
            println!("{}", tips::retired(&retired, Utc::now()));
        }
        return Ok(());
    }
    let (board, script) = tips::listing(&js, &args.name).await?;
    let now = Utc::now();
    if json {
        for tip in &tips::listed(&board, args, script.as_deref(), now) {
            println!("{}", serde_json::to_string(&tips::Json::from(tip))?);
        }
    } else {
        print!("{}", tips::render(&board, args, script.as_deref(), now));
    }
    Ok(())
}

/// Show the run `run`, or with `follow` follow it.
async fn show_run(nats: &Nats<'_>, js: &jetstream::Context, run: RunId, json: bool, follow: bool) -> Result<()> {
    nats.me.must_be_human("reading a trail's runs")?;
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
