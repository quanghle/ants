use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    ColonyEntry, PendingCall, Role, RunId, RunLimit, RunState, TRAIL_RUNS_BUCKET, ThreadId, TrailFunction, TrailLeg,
    TrailLimits, TrailRun, TrailSession, UserId, Uuid, trail_session_key,
};
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::json;

use super::super::trails::{MAX_LISTED, check_runs, check_worker, stuck};
use super::*;
use crate::test_support::colony::entry;

fn p(id: &str) -> UserId {
    id.parse().unwrap()
}

/// A run of trail deploy that `conductor` conducts, in `state`, started `hours` before `now` and updated 30 minutes
/// before it.
fn record(conductor: &str, state: RunState, hours: i64, now: DateTime<Utc>) -> TrailRun {
    TrailRun {
        run: RunId::generate(),
        conductor: p(conductor),
        route: vec![TrailLeg {
            trail: Some("deploy".parse().unwrap()),
            at: None,
            script: "agent('build')\n".to_owned(),
            args: serde_json::Value::Null,
            first_call: 0,
            started_at: now - TimeDelta::hours(hours),
            steps: 0,
        }],
        owner: Some(p("alice")),
        thread: ThreadId::generate(),
        parent: None,
        origin: Origin { by: Starter::Human { user: p("alice") }, channel: None },
        limits: TrailLimits::default(),
        state,
        phase: None,
        steps: 2,
        turns: 7,
        started_at: now - TimeDelta::hours(hours),
        updated_at: now - TimeDelta::minutes(30),
        ended_at: None,
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
    let no_flag = record("dev-a", failed("TimeoutError: turn time limit exceeded", Some(RunLimit::Compute)), 2, now);
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
    let session = TrailSession { session: "opaque".to_owned(), pending: vec![step("dev-b")], queued: Vec::new() };
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
    use std::os::unix::fs::PermissionsExt as _;
    let dir = ants_test_support::scratch_dir("doctor-worker").canonicalize().unwrap();
    let worker = |name: &str, body: &str| {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    };
    // As `ants subprocess` does: refusing a limit of 0 steps, in an empty environment.
    let answering = worker(
        "answering",
        r#"[ "$1" = subprocess ] && [ -z "$HOME" ] && grep -q '"steps":0' && echo '{"refused":{"error":"no"}}'"#,
    );
    let check = check_worker(&answering).await;
    assert_eq!(check.status, Status::Ok, "{check}");
    let started = format!("`{} subprocess` starts with an empty environment and answers in ", answering.display());
    assert!(check.detail.starts_with(&started), "{check}");

    let old = worker("old", "echo \"error: unrecognized subcommand 'subprocess'\" >&2; exit 2");
    let check = check_worker(&old).await;
    assert_eq!(check.status, Status::Fail, "{check}");
    assert!(check.detail.ends_with("crashed (exit status: 2): error: unrecognized subcommand 'subprocess'"), "{check}");
    let fix = format!(
        "env -i {0} subprocess < /dev/null shows the worker's own error (a working worker answers {{\"refused\": \
         ...}}); install ants again at {0}",
        old.display()
    );
    assert_eq!(check.fix, [fix]);
    let check = check_worker(&dir.join("missing")).await;
    assert_eq!(check.status, Status::Fail, "{check}");
    assert!(check.detail.contains("cannot start the worker"), "{check}");
    std::fs::remove_dir_all(dir).unwrap();
}
