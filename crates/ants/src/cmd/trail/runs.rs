//! The runs of trails, as their conductors keep them in [`TRAIL_RUNS_BUCKET`]: a human reads every conductor's, by
//! batched direct gets.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::str::FromStr;

use ants_proto::provider::Starter;
use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, QueuedCall, RatingOutcome, RunId, RunState, StepStage, TRAIL_RUNS_BUCKET,
    ThreadId, TrailFunction, TrailLeg, TrailName, TrailRating, TrailRun, TrailSession, TrailStepThread, UserId, Uuid,
    parse_trail_run_key, trail_run_key, trail_session_key, trail_thread_steps_prefix,
};
use anyhow::{Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::colony::{short, since};
use crate::machines::{latest_in, latest_timed_in};
use crate::text::{after_colon, one_line};

/// The runs `conductor` keeps, else those of every conductor, oldest first.
pub(crate) async fn list(js: &jetstream::Context, conductor: Option<&UserId>) -> Result<Vec<TrailRun>> {
    let filter = match conductor {
        Some(conductor) => format!("$KV.{TRAIL_RUNS_BUCKET}.{conductor}.*"),
        None => format!("$KV.{TRAIL_RUNS_BUCKET}.*.*"),
    };
    let mut runs = headers(js, &filter).await?;
    runs.sort_by(|a, b| (a.started_at, a.run.as_str()).cmp(&(b.started_at, b.run.as_str())));
    Ok(runs)
}

/// The runs' headers among the entries on `filter`, the conductors' indexes left out.
async fn headers(js: &jetstream::Context, filter: &str) -> Result<Vec<TrailRun>> {
    let found = latest_in(js, TRAIL_RUNS_BUCKET, filter).await?;
    Ok(found.iter().filter(|(key, _)| parse_trail_run_key(key).is_some()).filter_map(|(k, v)| parse(k, v)).collect())
}

pub(super) fn parse<T: serde::de::DeserializeOwned>(key: &str, payload: &[u8]) -> Option<T> {
    serde_json::from_slice(payload).inspect_err(|e| tracing::warn!(key, "skipping a malformed run entry: {e}")).ok()
}

/// The outcome each run's rating gave each trail ([`super::ratings::of_runs`]).
pub(crate) type Rated = HashMap<RunId, Vec<(TrailName, RatingOutcome)>>;

/// A run's header as `ants trail runs --json` lists it: without its legs' scripts, which `ants trail show RUN --json`
/// gives, and with the outcome its ratings gave each trail, `rated`, when it has any.
pub(super) fn header_json(run: &TrailRun, rated: &Rated) -> Result<String> {
    let mut value = serde_json::to_value(run)?;
    if let Some(legs) = value.get_mut("route").and_then(Value::as_array_mut) {
        for leg in legs.iter_mut().filter_map(Value::as_object_mut) {
            leg.remove("script");
        }
    }
    if let Some(rated) = rated.get(&run.run)
        && let Some(object) = value.as_object_mut()
    {
        let ratings = rated.iter().map(|(trail, outcome)| serde_json::json!({"trail": trail, "outcome": outcome}));
        object.insert("ratings".to_owned(), Value::Array(ratings.collect()));
    }
    Ok(value.to_string())
}

/// A run as `ants trail show RUN` shows it: its header, the calls it waits on, its journal by call, and its child runs.
#[derive(Debug, Serialize)]
pub(super) struct Detail {
    pub(super) run: TrailRun,
    pub(super) pending: Vec<PendingCall>,
    pub(super) queued: Vec<QueuedCall>,
    pub(super) journal: Vec<JournalEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) children: Vec<TrailRun>,
    /// The conductor's ratings of the run, one per trail.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(super) ratings: Vec<TrailRating>,
}

/// The run `run`, of whichever conductor keeps it.
pub(super) async fn find(js: &jetstream::Context, run: &RunId) -> Result<TrailRun> {
    let mut found = headers(js, &format!("$KV.{TRAIL_RUNS_BUCKET}.*.{run}")).await?;
    match found.len() {
        0 => bail!("no run {run}: `ants trail runs` lists the runs kept"),
        1 => Ok(found.remove(0)),
        _ => {
            let conductors: Vec<&str> = found.iter().map(|r| r.conductor.as_str()).collect();
            bail!("run {run} is kept by several conductors: {}", conductors.join(", "))
        }
    }
}

/// A run as a human names it: by its id, or a unique start of one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pick(String);

impl FromStr for Pick {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        if s.is_empty() || s.len() > 36 || !s.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            return Err("a run's id, or a unique start of one, is hex digits and '-'".to_owned());
        }
        Ok(Self(s))
    }
}

impl Pick {
    /// Whether the id `run` starts with this one.
    pub(crate) fn starts(&self, run: &RunId) -> bool {
        run.as_str().starts_with(&self.0)
    }
}

impl std::fmt::Display for Pick {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The run `pick` names, of whichever conductor keeps it: the one of that id, else the one run whose id starts so.
pub(crate) async fn pick(js: &jetstream::Context, pick: &Pick) -> Result<TrailRun> {
    if Uuid::try_parse(&pick.0).is_ok() {
        return find(js, &pick.0.parse()?).await;
    }
    picked(list(js, None).await?, pick, "`ants trail runs` lists the runs kept")
}

/// The run of `conductor` that `pick` names: the one of that id, else its one run whose id starts so.
pub(crate) async fn pick_of(js: &jetstream::Context, conductor: &UserId, pick: &Pick) -> Result<TrailRun> {
    let listed = format!("`ants trail runs --ant {conductor}` lists its runs");
    picked(list(js, Some(conductor)).await?, pick, &listed)
}

/// The one run of `runs` whose id starts with `pick`; `listed` says where to find the runs when none does.
pub(super) fn picked(runs: Vec<TrailRun>, pick: &Pick, listed: &str) -> Result<TrailRun> {
    let mut found: Vec<TrailRun> = runs.into_iter().filter(|r| pick.starts(&r.run)).collect();
    match found.len() {
        0 => bail!("no run's id starts with {pick}: {listed}"),
        1 => Ok(found.remove(0)),
        n => {
            let ids: Vec<&str> = found.iter().take(5).map(|r| r.run.as_str()).collect();
            let more = if n > ids.len() { ", …" } else { "" };
            bail!("{n} runs' ids start with {pick} ({}{more}): give more of the id", ids.join(", "))
        }
    }
}

/// The run `run` with the calls it waits on, its journal and its child runs.
pub(super) async fn detail(js: &jetstream::Context, run: &RunId) -> Result<Detail> {
    let header = find(js, run).await?;
    let session = session(js, &header).await?;
    let calls = format!("$KV.{TRAIL_RUNS_BUCKET}.{}.call.*", trail_run_key(&header.conductor, run));
    let mut journal: Vec<JournalEntry> =
        latest_in(js, TRAIL_RUNS_BUCKET, &calls).await?.iter().filter_map(|(k, v)| parse(k, v)).collect();
    journal.sort_by_key(|entry| entry.call);
    let (pending, queued) = session.map_or_default(|s| (s.pending, s.queued));
    let mut children = list(js, Some(&header.conductor)).await?;
    children.retain(|child| child.parent.as_ref().is_some_and(|parent| parent.run == *run));
    let ratings = super::ratings::of_run(js, &header.conductor, run).await.unwrap_or_else(|e| {
        tracing::warn!("not reading the run's ratings: {e:#}");
        Vec::new()
    });
    Ok(Detail { run: header, pending, queued, journal, children, ratings })
}

/// The session the conductor of `run` keeps: the calls the run waits on and those queued. None before its first.
pub(crate) async fn session(js: &jetstream::Context, run: &TrailRun) -> Result<Option<TrailSession>> {
    let session = format!("$KV.{TRAIL_RUNS_BUCKET}.{}", trail_session_key(&run.conductor, &run.run));
    Ok(latest_in(js, TRAIL_RUNS_BUCKET, &session).await?.iter().find_map(|(k, v)| parse(k, v)))
}

/// The run's state in a word.
pub(crate) fn state(state: &RunState) -> &'static str {
    match state {
        RunState::Running => "running",
        RunState::Waiting => "waiting",
        RunState::Stopped { .. } => "stopped",
        RunState::Done { .. } => "done",
        RunState::Failed { .. } => "failed",
    }
}

pub(super) fn function(function: TrailFunction) -> &'static str {
    match function {
        TrailFunction::Agent => "agent",
        TrailFunction::Human => "human",
        TrailFunction::Trail => "trail",
        TrailFunction::Goto => "goto",
    }
}

fn ago(at: DateTime<Utc>, now: DateTime<Utc>) -> String {
    format!("{} ago", short(since(at, now)))
}

fn local(at: DateTime<Utc>) -> String {
    at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string()
}

/// The trail a run started, `inline` for a script given inline; its route once it went on by goto(), as in
/// `triage -> fix@patch`.
fn trail(run: &TrailRun) -> String {
    if run.route.len() > 1 {
        return run.route_text();
    }
    run.first_trail().map_or_else(|| "inline".to_owned(), ToString::to_string)
}

/// The runs in columns, with the outcome each one's ratings gave, `rated`.
pub(crate) fn table(runs: &[TrailRun], rated: &Rated, now: DateTime<Utc>) -> String {
    let header =
        ["RUN", "TRAIL", "CONDUCTOR", "OWNER", "STATE", "PHASE", "STEPS", "TURNS", "STARTED", "ENDED", "RATED"]
            .map(str::to_owned);
    let rows: Vec<[String; 11]> = std::iter::once(header)
        .chain(runs.iter().map(|r| {
            [
                r.run.to_string(),
                trail(r),
                r.conductor.to_string(),
                r.owner.as_ref().map_or_else(|| "-".to_owned(), ToString::to_string),
                state(&r.state).to_owned(),
                r.phase.as_deref().map_or_else(|| "-".to_owned(), |p| one_line(p, 30)),
                format!("{}/{}", r.steps, r.limits.steps),
                format!("{}/{}", r.turns, r.limits.turns),
                ago(r.started_at, now),
                r.ended_at.map_or_else(|| "-".to_owned(), |at| ago(at, now)),
                super::ratings::rated_cell(rated.get(&r.run)),
            ]
        }))
        .collect();
    super::columns(&rows)
}

/// What a call asks, in short: its label, else its prompt or questions, or the trail it runs.
pub(super) fn gist(args: &Value) -> String {
    if let Some(trail) = args.get("name").and_then(Value::as_str) {
        return one_line(&format!("trail {trail}"), 60);
    }
    let text = ["label", "prompt", "questions"]
        .iter()
        .find_map(|key| args.get(key))
        .map_or_else(|| args.to_string(), |value| value.as_str().map_or_else(|| value.to_string(), str::to_owned));
    one_line(&text, 60)
}

/// Where a goto() call asks to go: its trail, else the run's own, at its waypoint, else the trail's start.
pub(super) fn destination(args: &Value) -> String {
    let trail =
        args.get("trail").and_then(Value::as_str).map_or_else(|| "the same trail".to_owned(), |t| format!("trail {t}"));
    let at =
        args.get("at").and_then(Value::as_str).map_or_else(|| "its start".to_owned(), |at| format!("waypoint {at}"));
    one_line(&format!("{trail} at {at}"), 60)
}

/// Where `leg` entered its trail.
pub(super) fn entered(leg: &TrailLeg) -> String {
    let trail = leg.trail.as_ref().map_or_else(|| "the inline script".to_owned(), |t| format!("trail {t}"));
    match &leg.at {
        Some(at) => format!("{trail} at waypoint {at}"),
        None => format!("{trail} at its start"),
    }
}

/// Whom an agent call asks: the ant it names, else the capability, else the conductor itself.
fn whom(args: &Value) -> String {
    match (args.get("ant").and_then(Value::as_str), args.get("capability").and_then(Value::as_str)) {
        (Some(ant), _) => one_line(ant, 60),
        (None, Some(capability)) => format!("capability {}", one_line(capability, 60)),
        (None, None) => "-".to_owned(),
    }
}

/// Who started the run's thread.
fn starter(by: &Starter) -> String {
    match by {
        Starter::Human { user } => format!("human {user}"),
        Starter::Ant { user } => format!("ant {user}"),
        Starter::Webhook { user } => format!("webhook {user}, untrusted"),
        Starter::Review { user } => format!("ant {user}, asking a review, untrusted"),
        Starter::Provider { name } => format!("provider {name}"),
        Starter::Unknown => "someone of a kind not known, untrusted".to_owned(),
    }
}

/// A row per leg of a run's route, counted from 0: its trail, the waypoint it entered at, when it started, the steps
/// it sent, and the state it took after the first.
fn legs(route: &[TrailLeg]) -> Vec<[String; 6]> {
    route
        .iter()
        .enumerate()
        .map(|(at, leg)| {
            let input = if at == 0 { String::new() } else { format!("state {}", one_line(&leg.args.to_string(), 60)) };
            [
                at.to_string(),
                leg.trail.as_ref().map_or_else(|| "inline".to_owned(), ToString::to_string),
                leg.at.clone().unwrap_or_else(|| "start".to_owned()),
                format!("started {}", local(leg.started_at)),
                format!("steps {}", leg.steps),
                input,
            ]
        })
        .collect()
}

/// A row per child run: its id, its route, its state and the call it answers.
fn children(children: &[TrailRun]) -> Vec<[String; 4]> {
    children
        .iter()
        .map(|child| {
            let call = child.parent.as_ref().map_or_default(|parent| format!("call {}", parent.call));
            [child.run.to_string(), child.route_text(), state(&child.state).to_owned(), call]
        })
        .collect()
}

/// The run's header, limits, route, child runs, pending and queued calls and journal.
pub(super) fn render(detail: &Detail, now: DateTime<Utc>) -> String {
    let Detail { run: r, pending, queued, journal, children: kids, ratings } = detail;
    let mut out = match r.first_trail() {
        Some(trail) => format!("run {} of trail {trail}\n", r.run),
        None => format!("run {} of an inline script\n", r.run),
    };
    let owner = r.owner.as_ref().map_or_else(|| "no owner".to_owned(), |o| format!("owner {o}"));
    let channel = r.origin.channel.as_ref().map_or_default(|c| format!(" in #{c}"));
    let _ = writeln!(
        out,
        "conductor {}, {owner}, on thread {} started by {}{channel}",
        r.conductor,
        r.thread,
        starter(&r.origin.by)
    );
    if let Some(parent) = &r.parent {
        let _ = writeln!(out, "child run answering call {} of run {}", parent.call, parent.run);
    }
    if let Some(from) = &r.spends_from {
        let _ = writeln!(
            out,
            "started answering call {} of run {} of {}, spending from that run's grant of {} steps and {} turns",
            from.call, from.run, from.conductor, from.bounds.steps, from.bounds.turns
        );
    }
    let _ = match &r.state {
        RunState::Stopped { by } => writeln!(out, "state: stopped by {by}"),
        RunState::Done { result } => {
            writeln!(out, "state: done, result: {}", one_line(&result.to_string(), usize::MAX))
        }
        RunState::Failed { error, limit: Some(limit) } => {
            writeln!(out, "state: failed on its {limit} limit:{}", after_colon(error))
        }
        RunState::Failed { error, limit: None } => writeln!(out, "state: failed:{}", after_colon(error)),
        other => writeln!(out, "state: {}", state(other)),
    };
    if let Some(phase) = &r.phase {
        let _ = writeln!(out, "phase: {}", one_line(phase, usize::MAX));
    }
    let _ =
        write!(out, "started {} ({}), updated {}", local(r.started_at), ago(r.started_at, now), ago(r.updated_at, now));
    if let Some(at) = r.ended_at {
        let _ = write!(out, ", ended {} ({})", local(at), ago(at, now));
    }
    out.push('\n');
    let l = &r.limits;
    let _ = writeln!(
        out,
        "limits: steps {} of {}, turns {} of {}, {} steps at once, wall time {}, compute {} between suspensions, \
         session {} KiB",
        r.steps,
        l.steps,
        r.turns,
        l.turns,
        l.concurrency,
        ants_proto::duration::format(l.wall_time()),
        ants_proto::duration::format(l.compute()),
        l.session_bytes / 1024
    );
    let args = r.route.first().map_or(&Value::Null, |leg| &leg.args);
    if !args.is_null() {
        let _ = writeln!(out, "args: {}", one_line(&args.to_string(), 200));
    }
    let _ = writeln!(out, "\nroute: {}", r.route_text());
    out.push_str(&indented(&super::columns(&legs(&r.route))));
    if !kids.is_empty() {
        out.push_str("\nchild runs:\n");
        out.push_str(&indented(&super::columns(&children(kids))));
    }
    if !pending.is_empty() {
        out.push_str("\npending calls:\n");
        let rows: Vec<[String; 6]> = pending
            .iter()
            .map(|c| {
                [
                    c.call.to_string(),
                    function(c.function).to_owned(),
                    c.child().map_or_else(|| format!("to {}", c.to), |child| format!("run {child}")),
                    format!("sent {}", ago(c.sent_at, now)),
                    format!("thread {}", c.thread),
                    gist(&c.args),
                ]
            })
            .collect();
        out.push_str(&indented(&super::columns(&rows)));
    }
    if !queued.is_empty() {
        out.push_str("\nqueued calls:\n");
        let rows: Vec<[String; 3]> =
            queued.iter().map(|c| [c.call.to_string(), function(c.function).to_owned(), gist(&c.args)]).collect();
        out.push_str(&indented(&super::columns(&rows)));
    }
    if !journal.is_empty() {
        out.push_str("\njournal:\n");
        out.push_str(&indented(&super::columns(&answered(r, journal))));
    }
    if !ratings.is_empty() {
        out.push_str(&super::ratings::of_run_section(ratings, now));
    }
    out
}

/// A row per call of the journal of `run`: its function, whom it asked, what, and its answer.
fn answered(run: &TrailRun, journal: &[JournalEntry]) -> Vec<[String; 5]> {
    journal
        .iter()
        .map(|e| {
            let answer = match &e.answer {
                CallAnswer::Ok(value) => {
                    format!("ok: {}", one_line(&value.as_str().map_or_else(|| value.to_string(), str::to_owned), 40))
                }
                CallAnswer::Error(error) => format!("error: {}", one_line(error, 60)),
            };
            let whom = match e.function {
                TrailFunction::Agent => whom(&e.args),
                TrailFunction::Human => run.owner.as_ref().map_or_else(|| "-".to_owned(), ToString::to_string),
                TrailFunction::Trail | TrailFunction::Goto => run.conductor.to_string(),
            };
            let gist = if e.function == TrailFunction::Goto { destination(&e.args) } else { gist(&e.args) };
            [e.call.to_string(), function(e.function).to_owned(), whom, gist, answer]
        })
        .collect()
}

/// A step of a trail run: the script's call `call`, labelled `label`, sent to `to` on its own thread `thread`, at
/// `stage` since; as its conductor's status updates tell it ([`ants_proto::StatusUpdate::Step`]), or its records
/// ([`steps_on`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Step {
    pub(crate) run: RunId,
    pub(crate) conductor: UserId,
    pub(crate) call: u32,
    pub(crate) label: Option<String>,
    pub(crate) to: UserId,
    pub(crate) thread: ThreadId,
    pub(crate) stage: StepStage,
    /// When it started.
    pub(crate) at: DateTime<Utc>,
}

/// A step's thread as its conductor recorded it, with when.
pub(super) type Marked = (ThreadId, TrailStepThread, DateTime<Utc>);

/// The steps of the runs `conductor` conducts reporting on `thread`, by when each started, as its records in
/// [`TRAIL_RUNS_BUCKET`] tell them: the step threads it recorded under `thread`, each just before its step went out
/// ([`staged`]), read alone ([`ants_proto::trail_thread_step_key`]), with each run's header, journal and, while the
/// run goes on, the calls its session waits on.
pub(crate) async fn steps_on(js: &jetstream::Context, conductor: &UserId, thread: &ThreadId) -> Result<Vec<Step>> {
    let prefix = trail_thread_steps_prefix(conductor, thread);
    let filter = format!("$KV.{TRAIL_RUNS_BUCKET}.{prefix}*");
    let mut marked: Vec<Marked> = Vec::new();
    for (key, value, at) in latest_timed_in(js, TRAIL_RUNS_BUCKET, &filter).await? {
        let Some(step) = key.strip_prefix(&prefix).and_then(|step| step.parse::<ThreadId>().ok()) else { continue };
        if let Some(marker) = parse::<TrailStepThread>(&key, &value).filter(|m| m.run_thread == *thread) {
            marked.push((step, marker, at));
        }
    }
    let runs: BTreeSet<RunId> = marked.iter().map(|(_, marker, _)| marker.run.clone()).collect();
    let mut steps = Vec::new();
    for run in runs {
        let key = trail_run_key(conductor, &run);
        let header = format!("$KV.{TRAIL_RUNS_BUCKET}.{key}");
        let header: Option<TrailRun> =
            latest_in(js, TRAIL_RUNS_BUCKET, &header).await?.iter().find_map(|(k, v)| parse(k, v));
        let calls = format!("$KV.{TRAIL_RUNS_BUCKET}.{key}.call.*");
        let journal: Vec<JournalEntry> =
            latest_in(js, TRAIL_RUNS_BUCKET, &calls).await?.iter().filter_map(|(k, v)| parse(k, v)).collect();
        let going = header.as_ref().is_some_and(|h| matches!(h.state, RunState::Running | RunState::Waiting));
        let pending = if going {
            let session = format!("$KV.{TRAIL_RUNS_BUCKET}.{}", trail_session_key(conductor, &run));
            let session: Option<TrailSession> =
                latest_in(js, TRAIL_RUNS_BUCKET, &session).await?.iter().find_map(|(k, v)| parse(k, v));
            session.map(|s| s.pending)
        } else {
            Some(Vec::new())
        };
        let of_run = marked.iter().filter(|(_, marker, _)| marker.run == run).cloned().collect();
        steps.extend(staged(conductor, of_run, &journal, pending.as_deref()));
    }
    steps.sort_by_key(|s| (s.at, s.call));
    Ok(steps)
}

/// The steps of a run its conductor `conductor` recorded the threads of, `marked`, at the stage its records tell:
/// answered or failed as `journal` has its call's answer; dropped for a call sent anew on a thread recorded since;
/// else started while the run waits on the call on that thread, as `pending`, the calls its session waits on, has it
/// (not known: started), and dropped otherwise, as for a run ended or stopped. Each is labelled as its marker is, or,
/// for a marker recorded without a label, as its call's arguments in `journal` or `pending` tell.
pub(super) fn staged(
    conductor: &UserId,
    marked: Vec<Marked>,
    journal: &[JournalEntry],
    pending: Option<&[PendingCall]>,
) -> Vec<Step> {
    let latest = |call: u32| marked.iter().filter(|(_, m, _)| m.call == call).max_by_key(|(_, _, at)| *at);
    let steps = marked.iter().map(|(thread, marker, at)| {
        let answered = journal.iter().find(|e| e.call == marker.call);
        let waited = pending.map(|pending| pending.iter().find(|c| c.call == marker.call));
        let superseded = latest(marker.call).is_some_and(|(newest, ..)| newest != thread);
        let stage = match (answered, waited) {
            _ if superseded => StepStage::Dropped,
            (Some(entry), _) if matches!(entry.answer, CallAnswer::Error(_)) => StepStage::Failed,
            (Some(_), _) => StepStage::Answered,
            (None, None) => StepStage::Started,
            (None, Some(Some(call))) if call.thread == *thread => StepStage::Started,
            (None, Some(_)) => StepStage::Dropped,
        };
        let args = answered.map(|e| (e.function, &e.args)).or_else(|| {
            let call = pending?.iter().find(|c| c.call == marker.call)?;
            Some((call.function, &call.args))
        });
        let label = marker.label.clone().or_else(|| {
            let (function, args) = args?;
            match function {
                TrailFunction::Agent => args.get("label")?.as_str().map(str::to_owned),
                TrailFunction::Trail => args.get("name")?.as_str().map(|name| format!("trail {name}")),
                TrailFunction::Human | TrailFunction::Goto => None,
            }
        });
        Step {
            run: marker.run.clone(),
            conductor: conductor.clone(),
            call: marker.call,
            label,
            to: marker.to.clone(),
            thread: thread.clone(),
            stage,
            at: *at,
        }
    });
    steps.collect()
}

fn indented(text: &str) -> String {
    text.lines().map(|line| format!("  {line}\n")).collect()
}
