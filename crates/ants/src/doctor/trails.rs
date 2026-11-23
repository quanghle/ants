//! Trails: whether a daemon here can start its `ants subprocess` worker, and the runs of trails stuck: failed on a
//! limit, stopped, waiting on ants gone from the colony, or past its wall time.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use ants_proto::{
    ColonyEntry, PendingCall, RunId, RunLimit, RunState, TRAIL_RUNS_BUCKET, TrailFunction, TrailLimits, TrailRun,
    subject,
};
use ants_trail::{Limits, Request, Response, Worker};
use chrono::{DateTime, Utc};

use super::{Check, Connected, NETWORK_TIMEOUT};
use crate::cmd::trail::runs;
use crate::service::{Service, Services};
use crate::text::one_line;
use crate::text::{short, since};

/// The most stuck runs listed, one check each.
pub(super) const MAX_LISTED: usize = 10;
/// How long past its wall time a run its conductor drives fails at most: a pass computing may hold it.
const OVERDUE_GRACE: Duration = Duration::from_mins(5);

/// The `ants` a daemon here starts as its trail worker: the one the daemon service installed under `home` runs, else
/// this one.
pub fn trail_worker(home: Option<PathBuf>) -> Option<PathBuf> {
    let unit = Services::under(home).unit(Service::Daemon);
    unit.and_then(|unit| unit.args.into_iter().next()).map(PathBuf::from).or_else(|| std::env::current_exe().ok())
}

/// Whether `program` starts as `ants subprocess` with the empty environment a daemon gives it, and answers within
/// `deadline` (a daemon allows [`ants_trail::GRACE`]) a request it refuses before running anything: a run of no
/// script under a limit of 0 steps.
pub(super) async fn check_worker(program: &Path, deadline: Duration) -> Check {
    let check = "trail worker";
    let shown = program.display();
    let probe = Request::Start {
        script: String::new(),
        entry: None,
        args: serde_json::Value::Null,
        limits: Limits { steps: 0, ..Limits::default() },
        clock: 0,
        seed: 0,
    };
    let fix = || {
        vec![format!(
            "env -i {shown} subprocess < /dev/null shows the worker's own error (a working worker answers \
             {{\"refused\": ...}}); install ants again at {shown}"
        )]
    };
    let started = Instant::now();
    match Worker::new(program).call(&probe, deadline).await {
        Ok(Response::Refused { .. }) => Check::ok(
            check,
            format!(
                "`{shown} subprocess` starts with an empty environment and answers in {}ms",
                started.elapsed().as_millis()
            ),
        ),
        Ok(_) => Check::fail(
            check,
            format!("`{shown} subprocess` ran a request under a limit of 0 steps, which a worker refuses"),
            fix(),
        ),
        Err(e) => Check::fail(check, format!("`{shown} subprocess`: {e}"), fix()),
    }
}

/// The runs of trails stuck ([`stuck`]), read with a human's credentials, which read every conductor's runs.
pub(super) async fn check_runs(connected: Option<&Connected>, now: DateTime<Utc>) -> Vec<Check> {
    let check = "trail runs";
    let Some(connected) = connected else { return vec![Check::skip(check, "the hub is not reached")] };
    if !connected.rights.may_publish(&format!("$JS.API.DIRECT.GET.{}", subject::kv_stream(TRAIL_RUNS_BUCKET))) {
        return vec![Check::skip(check, "checked with a human's credentials, which read every conductor's runs")];
    }
    let colony = match &connected.colony {
        Ok(colony) => colony,
        Err(why) => return vec![Check::skip(check, why.clone())],
    };
    let read = async {
        let listed = runs::list(&connected.js, None).await?;
        let mut pending = Vec::new();
        // Only a running run waits on ants: a waiting one waits on humans alone.
        for run in listed.iter().filter(|r| r.state == RunState::Running) {
            if let Some(session) = runs::session(&connected.js, run).await? {
                pending.push((run.run.clone(), session.pending));
            }
        }
        anyhow::Ok((listed, pending))
    };
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, read).await {
        Ok(Ok((listed, pending))) => stuck(&listed, &pending, colony, now),
        Ok(Err(e)) => vec![Check::skip(check, format!("no runs read: {e:#}"))],
        Err(_) => vec![Check::skip(check, "no runs read within 10s")],
    }
}

/// The runs of `listed` stuck, one check each with the command moving it on, given the calls `pending` each running
/// run waits on and the ants of `colony`: a run stopped, or failed on its steps, turns or wall time, which `ants trail
/// resume` continues (raising that limit) while its wall time may still grow; a run waiting on steps sent to ants not
/// in the colony, which `ants trail stop` stops; a run running or waiting past its wall time, which its conductor's
/// daemon does not drive, which `ants trail stop` stops; and a run whose conductor is not in the colony, which no
/// command moves on until a daemon serves the conductor again. A child run shows stopped only while its running parent
/// waits on it, and never failed: one stopped along with its parent resumes with it, a failed one's error answered its
/// parent's call, which only its parent's resume runs again, and one whose parent ended or went on answers nothing once
/// resumed.
pub(super) fn stuck(
    listed: &[TrailRun],
    pending: &[(RunId, Vec<PendingCall>)],
    colony: &[ColonyEntry],
    now: DateTime<Utc>,
) -> Vec<Check> {
    let check = "trail runs";
    let gone = |ant: &ants_proto::UserId| !colony.iter().any(|entry| entry.ant == *ant);
    let awaited = |run: &TrailRun| {
        let Some(link) = &run.parent else { return true };
        let calls = pending.iter().find(|(id, _)| *id == link.run).map_or_default(|(_, calls)| &calls[..]);
        calls.iter().any(|call| call.call == link.call && call.child().as_ref() == Some(&run.run))
    };
    let mut found: Vec<Check> = listed
        .iter()
        .filter_map(|run| {
            let name = match run.first_trail() {
                Some(trail) => format!("run {} of trail {trail}", run.run),
                None => format!("run {} of an inline script", run.run),
            };
            let elapsed = since(run.started_at, now);
            let ago = |at: Option<DateTime<Utc>>| short(since(at.unwrap_or(run.updated_at), now));
            let (detail, fix) = match &run.state {
                RunState::Stopped { by } => {
                    if !awaited(run) {
                        return None;
                    }
                    let wall = wall_past(&run.limits, elapsed)?;
                    let detail =
                        format!("{name}, conducted by {}, was stopped by {by} {} ago", run.conductor, ago(None));
                    (detail, format!("ants trail resume {}{wall}", run.run))
                }
                RunState::Failed { .. } if run.parent.is_some() => return None,
                RunState::Failed { error, .. } => {
                    let raise = raised(run, elapsed)?;
                    let error = error.lines().rfind(|line| !line.trim().is_empty()).unwrap_or_default();
                    let detail = format!(
                        "{name}, conducted by {}, failed {} ago: {}",
                        run.conductor,
                        ago(run.ended_at),
                        one_line(error, 160)
                    );
                    (detail, format!("ants trail resume {}{raise}", run.run))
                }
                RunState::Running | RunState::Waiting if gone(&run.conductor) => {
                    let detail = format!(
                        "{name} waits on its conductor {}, which is not in the colony: no daemon conducts it",
                        run.conductor
                    );
                    let fix = format!(
                        "start the daemon serving {}; `ants trail stop {}` needs it too",
                        run.conductor, run.run
                    );
                    return Some(Check::warn(check, detail, vec![fix]));
                }
                RunState::Running | RunState::Waiting if elapsed > run.limits.wall_time() + OVERDUE_GRACE => {
                    return Some(overdue(run, &name, elapsed));
                }
                RunState::Running => {
                    let calls = pending.iter().find(|(id, _)| *id == run.run).map_or_default(|(_, calls)| &calls[..]);
                    let mut away: Vec<&str> = calls
                        .iter()
                        .filter(|call| call.function == TrailFunction::Agent && gone(&call.to))
                        .map(|call| call.to.as_str())
                        .collect();
                    away.sort_unstable();
                    away.dedup();
                    if away.is_empty() {
                        return None;
                    }
                    let detail = format!(
                        "{name}, conducted by {}, waits on steps sent to {}, not in the colony: no daemon serves them \
                         to answer",
                        run.conductor,
                        away.join(", ")
                    );
                    return Some(Check::warn(check, detail, vec![format!("ants trail stop {}", run.run)]));
                }
                RunState::Waiting | RunState::Done { .. } => return None,
            };
            let fix = if gone(&run.conductor) {
                format!("{fix}, once a daemon serves {} again: it is not in the colony", run.conductor)
            } else {
                fix
            };
            Some(Check::warn(check, detail, vec![fix]))
        })
        .collect();
    if found.is_empty() {
        let unfinished = listed.iter().filter(|r| !r.finished()).count();
        let detail = match listed.len() {
            0 => "no runs of trails".to_owned(),
            n => format!("no run stuck: {n} kept, {unfinished} unfinished"),
        };
        return vec![Check::ok(check, detail)];
    }
    if found.len() > MAX_LISTED {
        let more = found.len() - MAX_LISTED;
        found.truncate(MAX_LISTED);
        found.push(Check::warn(check, format!("{more} more stuck"), vec!["ants trail runs".to_owned()]));
    }
    found
}

/// The check of `run`, named `name`, running or waiting `elapsed` after its start, past its wall time: its conductor's
/// daemon, in the colony, does not drive it.
fn overdue(run: &TrailRun, name: &str, elapsed: Duration) -> Check {
    let wall = run.limits.wall_time();
    let state = run.state.name();
    let detail = format!(
        "{name}, conducted by {}, is still {state} {} past its wall time of {}: the conductor's daemon does not drive it",
        run.conductor,
        short(elapsed.saturating_sub(wall)),
        ants_proto::duration::format(wall)
    );
    let fix = format!(
        "ants trail stop {}, or restart the daemon serving {}, which continues it from its records",
        run.run, run.conductor
    );
    Check::warn("trail runs", detail, vec![fix])
}

/// The flags of `ants trail resume` raising the limit failed `run` broke, its steps, turns or wall time, to twice it
/// at most its maximum, with the wall time `run` needs `elapsed` after its start; None when it broke none of them,
/// that limit is at its maximum already, or no wall time reaches that far.
fn raised(run: &TrailRun, elapsed: Duration) -> Option<String> {
    let (limits, max) = (&run.limits, TrailLimits::MAX);
    let doubled = |value: u32, most: u32| (value < most).then(|| value.saturating_mul(2).min(most));
    let limit = run.state.failed_on()?;
    let flag = limit.raised_by()?;
    if limit == RunLimit::Wall {
        let wall = wall_needed(limits.wall_time(), elapsed)?;
        return Some(format!(" {flag} {}", ants_proto::duration::format(wall)));
    }
    let needed = wall_past(limits, elapsed)?;
    let raised = match limit {
        RunLimit::Turns => doubled(limits.turns, max.turns),
        _ => doubled(limits.steps, max.steps),
    };
    raised.map(|value| format!(" {flag} {value}{needed}"))
}

/// The `--wall` flag a run under `limits` needs to resume `elapsed` after its start: none while its wall time is
/// ahead; None when no wall time reaches that far.
fn wall_past(limits: &TrailLimits, elapsed: Duration) -> Option<String> {
    if elapsed < limits.wall_time() {
        return Some(String::new());
    }
    wall_needed(limits.wall_time(), elapsed).map(|wall| format!(" --wall {}", ants_proto::duration::format(wall)))
}

/// `wall` doubled until it reaches past `elapsed`, at most [`TrailLimits::MAX`]'s; None when that does not.
fn wall_needed(wall: Duration, elapsed: Duration) -> Option<Duration> {
    let most = TrailLimits::MAX.wall_time();
    let mut raised = wall.max(Duration::from_secs(1));
    while raised <= elapsed && raised < most {
        raised = (raised * 2).min(most);
    }
    (raised > elapsed).then_some(raised)
}
