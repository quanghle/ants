//! `ants trail run`: what a run of a trail may spend, shown and confirmed, then the run asked of its conductor.

use std::fmt::Write as _;
use std::io::{BufRead as _, IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use ants_daemon::records::owners;
use ants_proto::{
    MAX_TRAIL_ARGS, MAX_TRAIL_SCRIPT, ThreadId, TrailAction, TrailLimits, TrailName, TrailOutcome, TrailSource, UserId,
};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use serde_json::{Value, json};

use super::{RunArgs, control, follow, runs, saved};
use crate::colony::{short, since};
use crate::connection::Nats;
use crate::threads::thread;

/// The trail `ants trail run` runs: a saved one, by name, or the script of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Trail {
    Saved(TrailName),
    File(PathBuf),
}

impl FromStr for Trail {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with(".py") {
            return Ok(Self::File(PathBuf::from(s)));
        }
        s.parse()
            .map(Self::Saved)
            .map_err(|_| "a saved trail's name (1-64 of a-z, 0-9 and '-') or a file ending in .py".to_owned())
    }
}

/// The value of `--args`: JSON.
pub(crate) fn json_args(s: &str) -> Result<Value, String> {
    serde_json::from_str(s).map_err(|e| format!("not JSON: {e}"))
}

/// What a run is to run, as [`summary`] shows it.
#[derive(Debug)]
pub(super) struct Plan {
    pub(super) name: String,
    /// Where the trail comes from, in words.
    pub(super) from: String,
    pub(super) description: String,
    pub(super) phases: Vec<String>,
    pub(super) source: TrailSource,
}

/// The plan of the script of the file `path`, checked here and sent inline.
pub(super) fn file_plan(path: &Path) -> Result<Plan> {
    let script = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    if script.len() > MAX_TRAIL_SCRIPT {
        bail!("{} has {} bytes; a trail takes at most {MAX_TRAIL_SCRIPT}", path.display(), script.len());
    }
    let meta = saved::checked(path, &script)?;
    Ok(Plan {
        name: meta.name,
        from: format!("the file {}, sent inline", path.display()),
        description: meta.description,
        phases: meta.phases,
        source: TrailSource::Script(script),
    })
}

/// The plan of the saved trail `found`: the colony's by name, a local one inline, as its conductor may run on another
/// machine.
pub(super) fn found_plan(found: saved::Found, now: chrono::DateTime<Utc>) -> Plan {
    let saved::Trail { name, description, phases, source, script } = found.trail;
    let (from, source) = match source {
        saved::Source::Colony { saved_by, saved_at } => (
            format!("saved for the colony by {saved_by} {} ago", short(since(saved_at, now))),
            TrailSource::Saved(name.clone()),
        ),
        saved::Source::Local { path } => {
            (format!("the local file {}, sent inline", path.display()), TrailSource::Script(script))
        }
    };
    Plan { name: name.to_string(), from, description, phases, source }
}

/// The limits in a line.
pub(super) fn limits_line(limits: &TrailLimits) -> String {
    format!(
        "{} steps, {} at once, {} turns, {} of wall time",
        limits.steps,
        limits.concurrency,
        limits.turns,
        ants_proto::duration::format(limits.wall_time())
    )
}

/// What `ants trail run` shows before the run starts: the trail, its conductor and thread, and what the run may spend.
pub(super) fn summary(plan: &Plan, conductor: &UserId, thread: Option<&ThreadId>, limits: &TrailLimits) -> String {
    let mut out = format!("trail {}, {}\n", plan.name, plan.from);
    if !plan.description.is_empty() {
        let _ = writeln!(out, "  {}", plan.description);
    }
    if !plan.phases.is_empty() {
        let _ = writeln!(out, "  phases: {}", plan.phases.join(", "));
    }
    let _ = match thread {
        Some(thread) => writeln!(out, "conductor {conductor}, reporting on thread {thread}"),
        None => writeln!(out, "conductor {conductor}, reporting on a new thread"),
    };
    let _ = write!(
        out,
        "the run may spend at most:\n  \
         steps:      {}, each also counted against the spending caps of the ant answering it\n  \
         at once:    {} steps\n  \
         turns:      {}\n  \
         wall time:  {}, stopped time included\n  \
         compute:    {} per resume of the script\n",
        limits.steps,
        limits.concurrency,
        limits.turns,
        ants_proto::duration::format(limits.wall_time()),
        ants_proto::duration::format(limits.compute()),
    );
    out
}

/// Ask `question` on the terminal: None without one.
pub(super) fn ask_terminal(question: &str) -> Result<Option<bool>> {
    if !std::io::stdin().is_terminal() {
        return Ok(None);
    }
    eprint!("{question} [y/N] ");
    std::io::stderr().flush()?;
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    Ok(Some(matches!(line.trim(), "y" | "Y" | "yes")))
}

/// `ants trail run`, confirming with `confirm` unless --yes: it answers None without a terminal to ask on.
pub(super) async fn run(
    nats: &Nats<'_>,
    args: RunArgs,
    confirm: impl FnOnce(&str) -> Result<Option<bool>>,
) -> Result<()> {
    let RunArgs { trail, args, ant, thread, limits, yes, follow, json } = args;
    super::human(nats, "starting a run")?;
    let limits = limits.over(TrailLimits::DEFAULT)?;
    let args = args.unwrap_or(Value::Null);
    let size = serde_json::to_vec(&args)?.len();
    if size > MAX_TRAIL_ARGS {
        bail!("--args has {size} bytes of JSON; a run takes at most {MAX_TRAIL_ARGS}");
    }
    let plan = match trail {
        Trail::File(path) => Ok(file_plan(&path)?),
        Trail::Saved(name) => Err(name),
    };
    let (client, js) = nats.connect("ants-cli").await?;
    let plan = match plan {
        Ok(plan) => plan,
        Err(name) => found_plan(saved::find(&js, &saved::local_dir()?, &name).await?, Utc::now()),
    };
    let conductor = match ant {
        Some(ant) => ant,
        None => crate::cmd::send::unaddressed(&js, &nats.me, nats.account.as_deref()).await.to,
    };
    let thread = thread::resolve(thread, nats, &js).await?;
    let summary = summary(&plan, &conductor, thread.as_ref(), &limits);
    if json {
        eprint!("{summary}");
    } else {
        print!("{summary}");
    }
    if !yes {
        match confirm("Start the run?")? {
            Some(true) => {}
            Some(false) => {
                println!("not started");
                return Ok(());
            }
            None => bail!("no terminal to confirm the run on: --yes starts it without asking"),
        }
    }
    let thread = thread.unwrap_or_else(ThreadId::generate);
    owners::recording(&js, nats.domain, &thread, &conductor, &nats.me.id).await;
    let action = TrailAction::Start { trail: plan.source, args, thread: thread.clone(), limits };
    let run = match control::ask(&client, &conductor, &nats.me.id, action, control::ANSWER_WAIT).await? {
        TrailOutcome::Started { run } => run,
        other => bail!("{conductor} answered {other:?}"),
    };
    if json {
        println!("{}", json!({ "run": run, "conductor": conductor, "thread": thread }));
    } else {
        println!("started run {run} on {conductor}, reporting on thread {thread}");
        if !follow {
            println!("`ants trail show {run} --follow` follows it, `ants trail stop {run}` stops it");
        }
    }
    if follow {
        follow::follow(&js, runs::find(&js, &run).await?, json).await?;
    }
    Ok(())
}
