//! `ants trail run`: what a run of a trail may spend, shown and confirmed, then the run asked of its conductor.

use std::fmt::Write as _;
use std::io::{BufRead, IsTerminal as _};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use ants_daemon::records::owners;
use ants_proto::{
    JsDomain, MAX_TRAIL_ARGS, MAX_TRAIL_SCRIPT, RunId, ThreadId, TrailAction, TrailLimits, TrailName, TrailOutcome,
    TrailSource, UserId,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::{Value, json};

use super::{RunArgs, control, follow, runs, saved};
use crate::connection::Nats;
use crate::text::one_line;
use crate::text::{short, since};
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

/// `args`, else null, refused over [`MAX_TRAIL_ARGS`] bytes of JSON, as `what` names them.
pub(crate) fn sized(args: Option<Value>, what: &str) -> Result<Value> {
    let args = args.unwrap_or(Value::Null);
    let size = serde_json::to_vec(&args)?.len();
    if size > MAX_TRAIL_ARGS {
        bail!("{what} has {size} bytes of JSON; a run takes at most {MAX_TRAIL_ARGS}");
    }
    Ok(args)
}

/// What a run is to run, as [`summary`] shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Plan {
    pub(crate) name: String,
    /// Where the trail comes from, in words.
    pub(crate) from: String,
    pub(crate) description: String,
    pub(crate) phases: Vec<String>,
    pub(crate) source: TrailSource,
}

/// The plan of the saved trail `name`: the colony's, else a local one ([`found_plan`]).
pub(crate) async fn saved_plan(js: &jetstream::Context, name: &TrailName) -> Result<Plan> {
    Ok(found_plan(saved::find(js, &saved::local_dir()?, name).await?, Utc::now()))
}

/// A run to ask its conductor for: what it runs and with what, the thread it reports on and what it may spend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Start {
    pub(crate) source: TrailSource,
    pub(crate) args: Value,
    pub(crate) conductor: UserId,
    pub(crate) thread: ThreadId,
    pub(crate) limits: TrailLimits,
}

impl Start {
    /// Ask the conductor, as human `me`, to start the run, which `me` owns: the owner record of the run's thread
    /// names `me` before the request goes, so that the recipients of its steps trust the owner they carry.
    pub(crate) async fn ask(
        &self,
        client: &async_nats::Client,
        js: &jetstream::Context,
        domain: Option<&JsDomain>,
        me: &UserId,
    ) -> Result<RunId> {
        let Self { source, args, conductor, thread, limits } = self;
        owners::recording(js, domain, thread, conductor, me).await;
        let action =
            TrailAction::Start { trail: source.clone(), args: args.clone(), thread: thread.clone(), limits: *limits };
        match control::ask(client, conductor, me, action, control::ANSWER_WAIT).await? {
            TrailOutcome::Started { run } => Ok(run),
            other => bail!("{conductor} answered {other:?}"),
        }
    }

    /// What `ants trail run` prints once the conductor started the run `run`.
    pub(crate) fn started(&self, run: &RunId) -> String {
        format!("started run {run} on {}, reporting on thread {}", self.conductor, self.thread)
    }
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
    let saved::Trail { name, description, phases, source, script, .. } = found.trail;
    let (from, source) = match source {
        saved::Source::Colony { saved_by, saved_at, .. } => (
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
pub(crate) fn limits_line(limits: &TrailLimits) -> String {
    format!(
        "{} steps, {} at once, {} turns, {} of wall time",
        limits.steps,
        limits.concurrency,
        limits.turns,
        ants_proto::duration::format(limits.wall_time())
    )
}

/// What `ants trail run` shows before the run starts: the trail, its conductor and thread, and what the run may spend.
pub(crate) fn summary(plan: &Plan, conductor: &UserId, thread: Option<&ThreadId>, limits: &TrailLimits) -> String {
    let mut out = format!("trail {}, {}\n", plan.name, plan.from);
    if !plan.description.is_empty() {
        let _ = writeln!(out, "  {}", one_line(&plan.description, usize::MAX));
    }
    if !plan.phases.is_empty() {
        let _ = writeln!(out, "  phases: {}", one_line(&plan.phases.join(", "), usize::MAX));
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

/// Ask `question` on the terminal, stdin and stderr: None without one on both.
pub(super) fn ask_terminal(question: &str) -> Result<Option<bool>> {
    let terminal = std::io::stdin().is_terminal() && std::io::stderr().is_terminal();
    ask(question, terminal, std::io::stdin().lock(), std::io::stderr())
}

/// Ask `question` on `output`, reading the answer from `input`: None unless both are a `terminal`.
pub(super) fn ask(
    question: &str,
    terminal: bool,
    mut input: impl BufRead,
    mut output: impl std::io::Write,
) -> Result<Option<bool>> {
    if !terminal {
        return Ok(None);
    }
    write!(output, "{question} [y/N] ")?;
    output.flush()?;
    let mut line = String::new();
    input.read_line(&mut line)?;
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
    let args = sized(args, "--args")?;
    let plan = match trail {
        Trail::File(path) => Ok(file_plan(&path)?),
        Trail::Saved(name) => Err(name),
    };
    let (client, js) = nats.connect("ants-cli").await?;
    let plan = match plan {
        Ok(plan) => plan,
        Err(name) => saved_plan(&js, &name).await?,
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
            Some(false) if json => bail!("not started"),
            Some(false) => {
                println!("not started");
                return Ok(());
            }
            None => bail!("no terminal to confirm the run on: --yes starts it without asking"),
        }
    }
    let thread = thread.unwrap_or_else(ThreadId::generate);
    let start = Start { source: plan.source, args, conductor, thread, limits };
    let run = start.ask(&client, &js, nats.domain, &nats.me.id).await?;
    let Start { conductor, thread, .. } = &start;
    if json {
        println!("{}", json!({ "run": run, "conductor": conductor, "thread": thread }));
    } else {
        println!("{}", start.started(&run));
        if !follow {
            println!("`ants trail show {run} --follow` follows it; `ants trail stop {run}` stops it");
        }
    }
    if follow {
        follow::follow(&js, runs::find(&js, &run).await?, json).await?;
    }
    Ok(())
}
