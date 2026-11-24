//! Tests of the doctor's trail checks: stuck runs and the commands moving them on, child runs, runs past their wall
//! time, every conductor's runs read, and the worker started as a daemon does.

use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    ColonyEntry, PendingCall, Role, RunLimit, RunState, TRAIL_RUNS_BUCKET, ThreadId, TrailFunction, TrailLeg,
    TrailLimits, TrailParent, TrailRun, TrailSession, Uuid, trail_session_key,
};
use ants_test_support::user as p;
use ants_test_support::{trail_leg, trail_run};
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::json;

#[cfg(unix)]
use super::super::trails::check_worker;
use super::super::trails::{MAX_LISTED, check_runs, stuck};
use super::*;
use crate::test_support::colony::entry;

/// A run of trail deploy that `conductor` conducts, in `state`, started `hours` before `now` and updated 30 minutes
/// before it.
fn record(conductor: &str, state: RunState, hours: i64, now: DateTime<Utc>) -> TrailRun {
    let started = now - TimeDelta::hours(hours);
    let leg = TrailLeg { script: "agent('build')\n".to_owned(), steps: 0, ..trail_leg(Some("deploy"), started) };
    TrailRun {
        route: vec![leg],
        owner: Some(p("alice")),
        origin: Origin { by: Starter::Human { user: p("alice") }, channel: None },
        steps: 2,
        turns: 7,
        updated_at: now - TimeDelta::minutes(30),
        ended_at: None,
        ..trail_run(&p(conductor), Some("deploy"), state, started)
    }
}

fn failed(error: &str, limit: Option<RunLimit>) -> RunState {
    RunState::Failed { error: error.to_owned(), limit }
}

/// A step a run sent to `to`, waiting on its answer.
fn step(to: &str) -> PendingCall {
    PendingCall {
        call: 1,
        function: TrailFunction::Agent,
        args: json!({"prompt": "build"}),
        to: p(to),
        thread: ThreadId::generate(),
        message: Uuid::now_v7(),
        sent_at: Utc::now(),
        sent: true,
        turn_budget: 8,
        retries: 0,
    }
}

fn colony(ants: &[&str], now: DateTime<Utc>) -> Vec<ColonyEntry> {
    ants.iter().map(|ant| entry(ant, 60, 2, now)).collect()
}

fn shown(checks: &[Check]) -> Vec<(Status, String, Vec<String>)> {
    checks.iter().map(|c| (c.status, c.detail.clone(), c.fix.clone())).collect()
}

fn warn(detail: String, fix: String) -> (Status, String, Vec<String>) {
    (Status::Warn, detail, vec![fix])
}

#[test]
fn runs_moving_on_or_over_for_good_are_not_stuck() {
    let now = Utc::now();
    let running = record("dev-a", RunState::Running, 1, now);
    let waiting_on_humans = record("dev-a", RunState::Waiting, 1, now);
    let done = record("dev-b", RunState::Done { result: json!("shipped") }, 2, now);
    let raised = record("dev-a", failed("Traceback:\nValueError: the run reached its limit of 50 steps", None), 2, now);
    let mut at_most = record("dev-a", failed("the run reached its limit of 500 steps", Some(RunLimit::Steps)), 2, now);
    at_most.limits.steps = TrailLimits::MAX.steps;
    let no_flag = record("dev-a", failed("TimeoutError: feed time limit exceeded", Some(RunLimit::Compute)), 2, now);
    let past_any_wall = record("dev-a", RunState::Stopped { by: p("alice") }, 8 * 24, now);
    let listed = [running.clone(), waiting_on_humans, done, raised, at_most, no_flag, past_any_wall];
    let pending = [(running.run, vec![step("dev-b"), step("dev-a")])];

    let checks = stuck(&listed, &pending, &colony(&["dev-a", "dev-b"], now), now);
    assert_eq!(shown(&checks), [(Status::Ok, "no run stuck: 7 kept, 3 unfinished".to_owned(), vec![])]);
    assert_eq!(shown(&stuck(&[], &[], &[], now)), [(Status::Ok, "no runs of trails".to_owned(), vec![])]);
}

#[test]
fn stuck_runs_name_the_command_resuming_or_stopping_each() {
    let now = Utc::now();
    let stopped = record("dev-a", RunState::Stopped { by: p("alice") }, 2, now);
    let turns = record("dev-a", failed("the run reached its limit of 200 turns", Some(RunLimit::Turns)), 3, now);
    let traceback = "Traceback:\n  agent('build')\nRuntimeError: the run reached its limit of 50 steps\n";
    let mut steps = record("dev-a", failed(traceback, Some(RunLimit::Steps)), 30, now);
    steps.ended_at = Some(now - TimeDelta::hours(1));
    let wall = record("dev-a", failed("the run ran past its wall time limit of 1d", Some(RunLimit::Wall)), 25, now);
    let orphaned = record("dev-a", RunState::Running, 1, now);
    let unconducted = record("dev-gone", RunState::Waiting, 1, now);
    let stopped_unconducted = record("dev-gone", RunState::Stopped { by: p("bob") }, 1, now);
    let listed = [&stopped, &turns, &steps, &wall, &orphaned, &unconducted, &stopped_unconducted].map(Clone::clone);
    let pending = [(orphaned.run.clone(), vec![step("dev-x"), step("dev-b"), step("dev-x")])];

    let checks = stuck(&listed, &pending, &colony(&["dev-a", "dev-b"], now), now);
    let name = |run: &TrailRun| format!("run {} of trail deploy", run.run);
    assert_eq!(
        shown(&checks),
        [
            warn(
                format!("{}, conducted by dev-a, was stopped by alice 30m ago", name(&stopped)),
                format!("ants trail resume {}", stopped.run)
            ),
            warn(
                format!("{}, conducted by dev-a, failed 30m ago: the run reached its limit of 200 turns", name(&turns)),
                format!("ants trail resume {} --turns 400", turns.run)
            ),
            warn(
                format!(
                    "{}, conducted by dev-a, failed 1h ago: RuntimeError: the run reached its limit of 50 steps",
                    name(&steps)
                ),
                format!("ants trail resume {} --steps 100 --wall 2d", steps.run)
            ),
            warn(
                format!(
                    "{}, conducted by dev-a, failed 30m ago: the run ran past its wall time limit of 1d",
                    name(&wall)
                ),
                format!("ants trail resume {} --wall 2d", wall.run)
            ),
            warn(
                format!(
                    "{}, conducted by dev-a, waits on steps sent to dev-x, not in the colony: no daemon serves them \
                     to answer",
                    name(&orphaned)
                ),
                format!("ants trail stop {}", orphaned.run)
            ),
            warn(
                format!(
                    "{} waits on its conductor dev-gone, which is not in the colony: no daemon conducts it",
                    name(&unconducted)
                ),
                format!("start the daemon serving dev-gone; `ants trail stop {}` needs it too", unconducted.run)
            ),
            warn(
                format!("{}, conducted by dev-gone, was stopped by bob 30m ago", name(&stopped_unconducted)),
                format!(
                    "ants trail resume {}, once a daemon serves dev-gone again: it is not in the colony",
                    stopped_unconducted.run
                )
            ),
        ]
    );

    let many: Vec<TrailRun> =
        (0..MAX_LISTED + 2).map(|_| record("dev-a", RunState::Stopped { by: p("alice") }, 1, now)).collect();
    let checks = stuck(&many, &[], &colony(&["dev-a"], now), now);
    assert_eq!(checks.len(), MAX_LISTED + 1);
    assert_eq!(shown(&checks[MAX_LISTED..]), [warn("2 more stuck".to_owned(), "ants trail runs".to_owned())]);
}

/// A child run of `parent` in `state`, answering its call `call`.
fn child(parent: &TrailRun, call: u32, state: RunState, now: DateTime<Utc>) -> TrailRun {
    let mut child = record("dev-a", state, 1, now);
    child.parent = Some(TrailParent { run: parent.run.clone(), call, origin_thread: parent.thread.clone() });
    child
}

/// The trail() call `call` a run waits on `child` to answer.
fn trail_call(call: u32, child: &TrailRun) -> PendingCall {
    let message = child.run.to_string().parse().unwrap();
    PendingCall { call, function: TrailFunction::Trail, to: child.conductor.clone(), message, ..step("dev-a") }
}

#[test]
fn child_runs_show_only_where_resuming_them_moves_their_parent_on() {
    let now = Utc::now();
    let stopped_by = |by: &str| RunState::Stopped { by: p(by) };
    let on_steps = || failed("the run reached its limit of 50 steps", Some(RunLimit::Steps));

    let parent = record("dev-a", stopped_by("alice"), 1, now);
    let with_parent = (1..=3).map(|call| child(&parent, call, stopped_by("alice"), now));
    let mut listed: Vec<TrailRun> = std::iter::once(parent.clone()).chain(with_parent).collect();
    let checks = stuck(&listed, &[], &colony(&["dev-a"], now), now);
    assert_eq!(
        shown(&checks),
        [warn(
            format!("run {} of trail deploy, conducted by dev-a, was stopped by alice 30m ago", parent.run),
            format!("ants trail resume {}", parent.run)
        )]
    );

    let failed_parent = record("dev-a", on_steps(), 1, now);
    let failed_child = child(&failed_parent, 1, on_steps(), now);
    let checks = stuck(&[failed_parent.clone(), failed_child], &[], &colony(&["dev-a"], now), now);
    assert_eq!(checks.len(), 1, "{checks:#?}");
    assert_eq!(checks[0].fix, [format!("ants trail resume {} --steps 100", failed_parent.run)]);

    let done = record("dev-a", RunState::Done { result: json!("shipped") }, 1, now);
    let went_on = record("dev-a", RunState::Running, 1, now);
    let waiting = record("dev-a", RunState::Waiting, 1, now);
    listed = vec![
        done.clone(),
        went_on.clone(),
        waiting.clone(),
        child(&done, 1, stopped_by("dev-a"), now),
        child(&done, 2, on_steps(), now),
        child(&went_on, 1, stopped_by("dev-a"), now),
        child(&went_on, 2, stopped_by("alice"), now),
        child(&went_on, 3, on_steps(), now),
        child(&waiting, 1, stopped_by("alice"), now),
        child(&record("dev-a", RunState::Running, 1, now), 1, stopped_by("alice"), now),
    ];
    let left_behind = &listed[5];
    let pending = [(went_on.run.clone(), vec![step("dev-a"), trail_call(1, &listed[3]), trail_call(4, left_behind)])];
    let checks = stuck(&listed, &pending, &colony(&["dev-a"], now), now);
    assert_eq!(shown(&checks), [(Status::Ok, "no run stuck: 10 kept, 7 unfinished".to_owned(), vec![])]);

    let awaited = &listed[6];
    let pending = [(went_on.run, vec![trail_call(2, awaited)])];
    let checks = stuck(&listed, &pending, &colony(&["dev-a"], now), now);
    assert_eq!(
        shown(&checks),
        [warn(
            format!("run {} of trail deploy, conducted by dev-a, was stopped by alice 30m ago", awaited.run),
            format!("ants trail resume {}", awaited.run)
        )]
    );
}

#[test]
fn a_run_running_or_waiting_past_its_wall_time_names_the_stop_or_the_restart_moving_it_on() {
    let now = Utc::now();
    let running = record("dev-a", RunState::Running, 3 * 24, now);
    let waiting = record("dev-a", RunState::Waiting, 26, now);
    let failing_now = record("dev-a", RunState::Running, 24, now);
    let listed = [&running, &waiting, &failing_now].map(Clone::clone);

    let checks = stuck(&listed, &[], &colony(&["dev-a"], now), now);
    let overdue = |run: &TrailRun, state: &str, past: &str| {
        warn(
            format!(
                "run {} of trail deploy, conducted by dev-a, is still {state} {past} past its wall time of 1d: the \
                 conductor's daemon does not drive it",
                run.run
            ),
            format!(
                "ants trail stop {}, or restart the daemon serving dev-a, which continues it from its records",
                run.run
            ),
        )
    };
    assert_eq!(shown(&checks), [overdue(&running, "running", "2d"), overdue(&waiting, "waiting", "2h")]);
}

/// Put `value` under `key` of [`TRAIL_RUNS_BUCKET`], as the conductor owning the key does.
async fn put(js: &async_nats::jetstream::Context, key: &str, value: &impl serde::Serialize) {
    let payload = serde_json::to_vec(value).unwrap();
    js.publish(format!("$KV.{TRAIL_RUNS_BUCKET}.{key}"), payload.into()).await.unwrap().await.unwrap();
}

#[tokio::test]
async fn the_runs_check_reads_every_conductor_s_runs_with_a_human_s_credentials() {
    let Some(server) = Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let rights = Rights::from_creds(Some(&creds)).unwrap();
    let now = Utc::now();
    let human = |ants: &[&str]| Connected { js: js.clone(), rights: rights.clone(), colony: Ok(colony(ants, now)) };

    let unread = check_runs(Some(&human(&["dev-a"])), now).await;
    assert_eq!(unread[0].status, Status::Skip, "no bucket before `ants init`: {unread:#?}");
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let served = human(&["dev-a", "dev-b"]);
    assert_eq!(shown(&check_runs(Some(&served), now).await), [(Status::Ok, "no runs of trails".to_owned(), vec![])]);

    let (_, a_js) = server.connect(&dev_a).await;
    let run = record("dev-a", RunState::Running, 1, now);
    put(&a_js, &run.key(), &run).await;
    let session = TrailSession {
        session: "opaque".to_owned(),
        pending: vec![step("dev-b")],
        queued: Vec::new(),
        anew: Vec::new(),
    };
    put(&a_js, &trail_session_key(&dev_a, &run.run), &session).await;
    let checks = check_runs(Some(&served), now).await;
    assert_eq!(shown(&checks), [(Status::Ok, "no run stuck: 1 kept, 1 unfinished".to_owned(), vec![])]);

    let checks = check_runs(Some(&human(&["dev-a"])), now).await;
    assert_eq!(checks.len(), 1, "{checks:#?}");
    assert_eq!((checks[0].status, checks[0].fix.clone()), (Status::Warn, vec![format!("ants trail stop {}", run.run)]));
    let detail = "waits on steps sent to dev-b, not in the colony: no daemon serves them to answer";
    assert!(checks[0].detail.ends_with(detail), "{}", checks[0]);

    let unread = Connected { colony: Err("no colony read: gone".to_owned()), ..human(&[]) };
    let skipped = (Status::Skip, "no colony read: gone".to_owned(), vec![]);
    assert_eq!(shown(&check_runs(Some(&unread), now).await), [skipped]);
    let ant_creds = server.creds_as(&dev_a, Role::Ant);
    let ant = Connected { js: a_js, rights: Rights::from_creds(Some(&ant_creds)).unwrap(), colony: Ok(Vec::new()) };
    let skipped = check_runs(Some(&ant), now).await;
    assert_eq!(
        shown(&skipped),
        [(Status::Skip, "checked with a human's credentials, which read every conductor's runs".to_owned(), vec![])]
    );
    assert_eq!(check_runs(None, now).await[0].detail, "the hub is not reached");
}

#[cfg(unix)]
#[tokio::test]
async fn the_worker_check_starts_the_worker_as_a_daemon_does() {
    let dir = ants_test_support::scratch_dir("doctor-worker").canonicalize().unwrap();
    let worker = |name: &str, body: &str| {
        let path = dir.join(name);
        ants_test_support::executable(&path, &format!("#!/bin/sh\n{body}\n"));
        path
    };
    // The ants binary cargo builds beside the test's, for the integration tests.
    let exe = std::env::current_exe().unwrap();
    let ants = exe.ancestors().skip(1).map(|dir| dir.join("ants")).find(|ants| ants.is_file());
    let ants = ants.expect("the ants binary is built beside the test binary, as `cargo test` builds it");
    // A daemon allows GRACE; a debug build started on a loaded machine may take longer.
    let deadline = Duration::from_mins(1);
    let check = check_worker(&ants, deadline).await;
    assert_eq!(check.status, Status::Ok, "{check}");
    let started = format!("`{} subprocess` starts with an empty environment and answers in ", ants.display());
    assert!(check.detail.starts_with(&started), "{check}");

    let old = worker("old", "echo \"error: unrecognized subcommand 'subprocess'\" >&2; exit 2");
    let check = check_worker(&old, deadline).await;
    assert_eq!(check.status, Status::Fail, "{check}");
    assert!(check.detail.ends_with("crashed (exit status: 2): error: unrecognized subcommand 'subprocess'"), "{check}");
    let fix = format!(
        "env -i {0} subprocess < /dev/null shows the worker's own error (a working worker answers {{\"refused\": \
         ...}}); install ants again at {0}",
        old.display()
    );
    assert_eq!(check.fix, [fix]);
    let check = check_worker(&dir.join("missing"), deadline).await;
    assert_eq!(check.status, Status::Fail, "{check}");
    assert!(check.detail.contains("cannot start the worker"), "{check}");
    std::fs::remove_dir_all(dir).unwrap();
}
