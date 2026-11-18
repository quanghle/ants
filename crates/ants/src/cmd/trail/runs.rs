//! The runs of trails, as their conductors keep them in [`TRAIL_RUNS_BUCKET`]: a human reads every conductor's, by
//! batched direct gets.

use std::fmt::Write as _;

use ants_proto::provider::Starter;
use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, QueuedCall, RunId, RunState, TRAIL_RUNS_BUCKET, TrailFunction, TrailRun,
    TrailSession, UserId, parse_trail_run_key, trail_run_key, trail_session_key,
};
use anyhow::{Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::colony::{short, since};
use crate::machines::latest_in;
use crate::text::excerpt;

/// The runs `conductor` keeps, else those of every conductor, oldest first.
pub(super) async fn list(js: &jetstream::Context, conductor: Option<&UserId>) -> Result<Vec<TrailRun>> {
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

fn parse<T: serde::de::DeserializeOwned>(key: &str, payload: &[u8]) -> Option<T> {
    serde_json::from_slice(payload).inspect_err(|e| tracing::warn!(key, "skipping a malformed run entry: {e}")).ok()
}

/// A run's header as `ants trail runs --json` lists it: without its script, which `ants trail show RUN --json` gives.
pub(super) fn header_json(run: &TrailRun) -> Result<String> {
    let mut value = serde_json::to_value(run)?;
    if let Some(fields) = value.as_object_mut() {
        fields.remove("script");
    }
    Ok(value.to_string())
}

/// A run as `ants trail show RUN` shows it: its header, the calls it waits on, and its journal by call.
#[derive(Debug, Serialize)]
pub(super) struct Detail {
    pub(super) run: TrailRun,
    pub(super) pending: Vec<PendingCall>,
    pub(super) queued: Vec<QueuedCall>,
    pub(super) journal: Vec<JournalEntry>,
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

/// The run `run` with the calls it waits on and its journal.
pub(super) async fn detail(js: &jetstream::Context, run: &RunId) -> Result<Detail> {
    let header = find(js, run).await?;
    let conductor = &header.conductor;
    let session = format!("$KV.{TRAIL_RUNS_BUCKET}.{}", trail_session_key(conductor, run));
    let session: Option<TrailSession> =
        latest_in(js, TRAIL_RUNS_BUCKET, &session).await?.iter().find_map(|(k, v)| parse(k, v));
    let calls = format!("$KV.{TRAIL_RUNS_BUCKET}.{}.call.*", trail_run_key(conductor, run));
    let mut journal: Vec<JournalEntry> =
        latest_in(js, TRAIL_RUNS_BUCKET, &calls).await?.iter().filter_map(|(k, v)| parse(k, v)).collect();
    journal.sort_by_key(|entry| entry.call);
    let (pending, queued) = session.map_or_default(|s| (s.pending, s.queued));
    Ok(Detail { run: header, pending, queued, journal })
}

/// The run's state in a word.
fn state(state: &RunState) -> &'static str {
    match state {
        RunState::Running => "running",
        RunState::Waiting => "waiting",
        RunState::Stopped { .. } => "stopped",
        RunState::Done { .. } => "done",
        RunState::Failed { .. } => "failed",
    }
}

fn function(function: TrailFunction) -> &'static str {
    match function {
        TrailFunction::Agent => "agent",
        TrailFunction::Human => "human",
    }
}

fn ago(at: DateTime<Utc>, now: DateTime<Utc>) -> String {
    format!("{} ago", short(since(at, now)))
}

fn local(at: DateTime<Utc>) -> String {
    at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string()
}

pub(super) fn table(runs: &[TrailRun], now: DateTime<Utc>) -> String {
    let header = ["RUN", "TRAIL", "CONDUCTOR", "OWNER", "STATE", "PHASE", "STEPS", "TURNS", "STARTED", "ENDED"]
        .map(str::to_owned);
    let rows: Vec<[String; 10]> = std::iter::once(header)
        .chain(runs.iter().map(|r| {
            [
                r.run.to_string(),
                r.trail.as_ref().map_or_else(|| "inline".to_owned(), ToString::to_string),
                r.conductor.to_string(),
                r.owner.as_ref().map_or_else(|| "-".to_owned(), ToString::to_string),
                state(&r.state).to_owned(),
                r.phase.as_deref().map_or_else(|| "-".to_owned(), |p| excerpt(p, 30)),
                format!("{}/{}", r.steps, r.limits.steps),
                format!("{}/{}", r.turns, r.limits.turns),
                ago(r.started_at, now),
                r.ended_at.map_or_else(|| "-".to_owned(), |at| ago(at, now)),
            ]
        }))
        .collect();
    super::columns(&rows)
}

/// What a call asks, in short: its label, else its prompt or questions.
fn gist(args: &Value) -> String {
    let text = ["label", "prompt", "questions"]
        .iter()
        .find_map(|key| args.get(key))
        .map_or_else(|| args.to_string(), |value| value.as_str().map_or_else(|| value.to_string(), str::to_owned));
    excerpt(&text, 60)
}

/// Whom an agent call asks: the ant it names, else the capability, else the conductor itself.
fn whom(args: &Value) -> String {
    match (args.get("ant").and_then(Value::as_str), args.get("capability").and_then(Value::as_str)) {
        (Some(ant), _) => ant.to_owned(),
        (None, Some(capability)) => format!("capability {capability}"),
        (None, None) => "-".to_owned(),
    }
}

/// Who started the run's thread.
fn starter(by: &Starter) -> String {
    match by {
        Starter::Human { user } => format!("human {user}"),
        Starter::Ant { user } => format!("ant {user}"),
        Starter::Webhook { user } => format!("webhook {user}, untrusted"),
        Starter::Provider { name } => format!("provider {name}"),
    }
}

/// The run's header, limits, pending and queued calls and journal.
pub(super) fn render(detail: &Detail, now: DateTime<Utc>) -> String {
    let Detail { run: r, pending, queued, journal } = detail;
    let mut out = match &r.trail {
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
    let _ = match &r.state {
        RunState::Stopped { by } => writeln!(out, "state: stopped by {by}"),
        RunState::Done { result } => writeln!(out, "state: done, result: {result}"),
        RunState::Failed { error } => writeln!(out, "state: failed: {error}"),
        other => writeln!(out, "state: {}", state(other)),
    };
    if let Some(phase) = &r.phase {
        let _ = writeln!(out, "phase: {phase}");
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
    if !r.args.is_null() {
        let _ = writeln!(out, "args: {}", excerpt(&r.args.to_string(), 200));
    }
    if !pending.is_empty() {
        out.push_str("\npending calls:\n");
        let rows: Vec<[String; 6]> = pending
            .iter()
            .map(|c| {
                [
                    c.call.to_string(),
                    function(c.function).to_owned(),
                    format!("to {}", c.to),
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
        let rows: Vec<[String; 5]> = journal
            .iter()
            .map(|e| {
                let answer = match &e.answer {
                    CallAnswer::Ok(value) => {
                        format!("ok: {}", excerpt(&value.as_str().map_or_else(|| value.to_string(), str::to_owned), 40))
                    }
                    CallAnswer::Error(error) => format!("error: {}", excerpt(error, 60)),
                };
                let whom = match e.function {
                    TrailFunction::Agent => whom(&e.args),
                    TrailFunction::Human => r.owner.as_ref().map_or_else(|| "-".to_owned(), ToString::to_string),
                };
                [e.call.to_string(), function(e.function).to_owned(), whom, gist(&e.args), answer]
            })
            .collect();
        out.push_str(&indented(&super::columns(&rows)));
    }
    out
}

fn indented(text: &str) -> String {
    text.lines().map(|line| format!("  {line}\n")).collect()
}
