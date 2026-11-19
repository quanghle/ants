//! `ants trail run`, `stop`, `resume` and following a run.

use std::time::Duration;

use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, QueuedCall, Role, RunId, RunState, ThreadId, TrailAction, TrailFunction,
    TrailLeg, TrailLimits, TrailOutcome, TrailRequest, TrailRun, TrailRunIndex, TrailSession, TrailSource, UserId,
    Uuid, WaitingStep, subject, trail_runs_index_key,
};
use chrono::{TimeDelta, Utc};
use clap::Parser;
use futures::StreamExt as _;
use serde_json::json;

use super::super::follow::{Follower, Patience, Read, Records, UNREAD_STATUS, Unread, following};
use super::super::start::{self, Plan, Trail};
use super::super::{LimitArgs, RunArgs, TrailCommand, control, runs};
use super::{human, p, put, record, script, write};
use crate::cli::{Cli, Command};
use crate::connection::Nats;
use crate::test_support::Server;

fn parsed(args: &[&str]) -> Result<TrailCommand, clap::Error> {
    let cli = Cli::try_parse_from(["ants", "trail"].iter().chain(args))?;
    let Some(Command::Trail(command)) = cli.command else { panic!("not a trail command") };
    Ok(command)
}

fn run_args(args: &[&str]) -> RunArgs {
    let Ok(TrailCommand::Run(args)) = parsed(&[&["run"], args].concat()) else { panic!("not a run") };
    args
}

#[test]
fn run_takes_a_saved_trail_or_a_py_file_and_json_args() {
    let args = run_args(&["deploy", "--args", r#"{"branch": "main"}"#, "--ant", "lead", "--yes", "--steps", "9"]);
    assert_eq!(args.trail, Trail::Saved("deploy".parse().unwrap()));
    assert_eq!(args.args, Some(json!({"branch": "main"})));
    assert_eq!((args.ant, args.yes, args.limits.steps), (Some(p("lead")), true, Some(9)));
    assert_eq!(run_args(&["ship/it.py"]).trail, Trail::File("ship/it.py".into()));
    assert_eq!(run_args(&["ship.py", "--wall", "2h"]).limits.wall, Some(Duration::from_secs(7200)));
    let error = parsed(&["run", "deploy", "--args", "{branch"]).unwrap_err().to_string();
    assert!(error.contains("not JSON"), "{error}");
    assert!(parsed(&["run", "Not_A_Name"]).is_err());
    let Ok(TrailCommand::Stop { run }) = parsed(&["stop", "0190AB"]) else { panic!("not a stop") };
    assert_eq!(run.to_string(), "0190ab");
    assert!(parsed(&["stop", "deploy"]).is_err(), "a run's id is hex");
    let Ok(TrailCommand::Resume { limits, follow, .. }) = parsed(&["resume", "0190ab", "--turns", "400", "--follow"])
    else {
        panic!("not a resume")
    };
    assert_eq!((limits.turns, follow), (Some(400), true));
}

#[test]
fn limits_given_replace_the_base_within_their_maximum() {
    let base = TrailLimits { steps: 7, ..TrailLimits::DEFAULT };
    assert_eq!(LimitArgs::default().over(base).unwrap(), base);
    let given = LimitArgs { at_once: Some(2), wall: Some(Duration::from_secs(3600)), ..LimitArgs::default() };
    let limits = given.over(base).unwrap();
    assert_eq!((limits.steps, limits.concurrency, limits.wall_secs), (7, 2, 3600));
    assert_eq!(limits.compute_ms, base.compute_ms);
    let refused = |given: LimitArgs| given.over(base).unwrap_err().to_string();
    assert_eq!(refused(LimitArgs { steps: Some(0), ..LimitArgs::default() }), "--steps must be more than 0");
    let turns = LimitArgs { turns: Some(2001), ..LimitArgs::default() };
    assert_eq!(refused(turns), "--turns is over the most a run may take, 2000");
    let wall = LimitArgs { wall: Some(Duration::from_hours(8 * 24)), ..LimitArgs::default() };
    assert_eq!(refused(wall), "--wall is over the most a run may take, 7d");
}

#[test]
fn the_summary_shows_the_trail_and_what_the_run_may_spend() {
    let plan = Plan {
        name: "deploy".to_owned(),
        from: "the file deploy.py, sent inline".to_owned(),
        description: "Ship it".to_owned(),
        phases: vec!["build".to_owned(), "test".to_owned()],
        source: TrailSource::Script(script("deploy")),
    };
    let text = start::summary(&plan, &p("lead"), None, &TrailLimits::DEFAULT);
    for line in [
        "trail deploy, the file deploy.py, sent inline\n",
        "  Ship it\n  phases: build, test\n",
        "conductor lead, reporting on a new thread\n",
        "  steps:      50, each also counted",
        "  at once:    4 steps\n",
        "  turns:      200\n",
        "  wall time:  1d, stopped time included\n",
        "  compute:    10s per resume",
    ] {
        assert!(text.contains(line), "{line}: {text}");
    }
}

#[test]
fn stopping_lists_the_steps_left_with_other_ants_and_how_to_withdraw_them() {
    let (run, message) = (RunId::generate(), Uuid::now_v7());
    let text = control::stopped(&run, &p("lead"), &[WaitingStep { to: p("dev-a"), message }]);
    assert!(text.starts_with(&format!("stopped run {run} on lead: no further step goes out until")), "{text}");
    assert!(text.ends_with(&format!("  dev-a message {message}: ants withdraw dev-a {message}\n")), "{text}");
    assert!(!control::stopped(&run, &p("lead"), &[]).contains("withdraw"));
}

fn pending(call: u32, to: &str, function: TrailFunction) -> PendingCall {
    PendingCall {
        call,
        function,
        args: json!({"prompt": format!("step {call}")}),
        to: p(to),
        thread: ThreadId::generate(),
        message: Uuid::now_v7(),
        sent_at: Utc::now(),
        sent: true,
        turn_budget: 8,
        retries: 0,
    }
}

#[test]
fn a_follower_prints_each_change_of_a_run_once() {
    let mut follower = Follower::default();
    let mut header = record(&p("lead"), Some("deploy"), RunState::Running, Utc::now());
    assert_eq!(follower.header(&header), ["phase: build"]);
    assert!(follower.header(&header).is_empty());
    let queued = QueuedCall { call: 1, function: TrailFunction::Agent, args: json!({"prompt": "test it"}) };
    let pending0 = pending(0, "dev-a", TrailFunction::Agent);
    let mut session =
        TrailSession { session: String::new(), pending: vec![pending0], queued: vec![queued], anew: Vec::new() };
    assert_eq!(follower.session(&session), ["step 1 queued: test it", "step 0 sent to dev-a: step 0"]);
    assert!(follower.session(&session).is_empty());
    session.pending[0].retries = 1;
    let again = "step 0 asked again of dev-a for an answer matching its schema (1 of 2)";
    assert_eq!(follower.session(&session), [again]);
    session.pending[0].message = Uuid::now_v7();
    assert_eq!(follower.session(&session), ["step 0 sent anew to dev-a"]);
    session.queued.clear();
    session.pending.push(pending(1, "alice", TrailFunction::Human));
    assert_eq!(follower.session(&session), ["step 1 asks alice: step 1"]);
    let answered = JournalEntry {
        call: 0,
        function: TrailFunction::Agent,
        args: json!({}),
        answer: CallAnswer::Ok(json!({"built": true})),
    };
    assert_eq!(follower.entry(&answered).as_deref(), Some("step 0 answered: {…"));
    assert_eq!(follower.entry(&answered), None);
    let failed = JournalEntry { call: 1, answer: CallAnswer::Error("no".to_owned()), ..answered };
    assert_eq!(follower.entry(&failed).as_deref(), Some("step 1 failed: no"));
    assert!(follower.session(&session).is_empty(), "answered steps are done with");
    header.state = RunState::Waiting;
    header.phase = Some("test".to_owned());
    assert_eq!(follower.header(&header), ["phase: test", "waiting on humans' answers"]);
    header.state = RunState::Running;
    assert_eq!(follower.header(&header), ["running"]);
    let refused = JournalEntry {
        call: 2,
        function: TrailFunction::Goto,
        args: json!({"at": "build"}),
        answer: CallAnswer::Error("entered before".to_owned()),
    };
    let line = "call 2 to go to the same trail at waypoint build refused: entered before";
    assert_eq!(follower.entry(&refused).as_deref(), Some(line));
    let next =
        TrailLeg { trail: Some("release".parse().unwrap()), at: Some("review".to_owned()), ..header.route[0].clone() };
    header.route.push(next);
    assert_eq!(follower.header(&header), ["went on to trail release at waypoint review"]);
    assert!(follower.header(&header).is_empty());
}

#[test]
fn the_start_is_asked_only_with_a_terminal_on_stdin_and_stderr() {
    let mut asked = Vec::new();
    assert_eq!(start::ask("Start the run?", false, &b"y\n"[..], &mut asked).unwrap(), None);
    assert!(asked.is_empty());
    assert_eq!(start::ask("Start the run?", true, &b"y\n"[..], &mut asked).unwrap(), Some(true));
    assert_eq!(asked, b"Start the run? [y/N] ");
    assert_eq!(start::ask("Start the run?", true, &b"\n"[..], std::io::sink()).unwrap(), Some(false));
}

/// Records read in the order given, then unchanged.
struct Scripted(std::collections::VecDeque<anyhow::Result<Read>>);

impl Records for Scripted {
    async fn read(&mut self) -> anyhow::Result<Read> {
        self.0.pop_front().unwrap_or_else(|| Ok(Read::default()))
    }
}

fn header(run: &TrailRun) -> anyhow::Result<Read> {
    Ok(Read { header: Some(run.clone()), ..Read::default() })
}

const PATIENT: Patience = Patience { poll: Duration::from_millis(1), failed_reads: 3, quiet: Duration::from_secs(1) };

#[tokio::test]
async fn a_follow_ends_once_a_run_past_its_wall_time_goes_unchanged() {
    let mut run = record(&p("lead"), Some("deploy"), RunState::Running, Utc::now() - TimeDelta::hours(2));
    run.limits.wall_secs = 3600;
    let records = Scripted([header(&run)].into());
    let error = following(records, run.clone(), false, PATIENT).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "run {} is past its wall time of 1h and its record has not changed for 1s: no daemon of lead conducts it any more",
            run.run
        )
    );
    assert!(error.downcast_ref::<Unread>().is_none());

    let mut later = run.clone();
    later.limits.wall_secs = 86_400;
    let mut done = later.clone();
    done.state = RunState::Done { result: json!("shipped") };
    let records = Scripted([header(&later), Ok(Read::default()), Ok(Read::default()), header(&done)].into());
    following(records, later, false, PATIENT).await.expect("a run within its wall time is followed to its end");
}

#[tokio::test]
async fn a_follow_tries_failed_reads_again_before_giving_up_with_a_status_of_its_own() {
    let run = record(&p("lead"), Some("deploy"), RunState::Running, Utc::now());
    let mut done = run.clone();
    done.state = RunState::Done { result: json!("shipped") };
    let failed = || Err(anyhow::anyhow!("timed out"));
    let records = Scripted([failed(), failed(), header(&done)].into());
    following(records, run.clone(), true, PATIENT).await.expect("two failed reads are tried again");

    let records = Scripted([failed(), failed(), failed(), header(&done)].into());
    let error = following(records, run.clone(), false, PATIENT).await.unwrap_err();
    assert!(error.downcast_ref::<Unread>().is_some(), "{error}");
    assert_eq!(
        error.to_string(),
        format!(
            "gave up following run {0}: its records could not be read 3 times in a row: timed out; `ants trail show {0}` shows it",
            run.run
        )
    );
    assert_ne!(UNREAD_STATUS, 1);
}

/// Answer each request on `ant`'s control subject with what `answer` makes of it, as `ant`'s daemon does.
async fn conductor(server: &Server, ant: &UserId, answer: fn(&TrailRequest) -> TrailOutcome) {
    let (client, js) = server.connect(ant).await;
    let mut requests = client.subscribe(subject::trail(ant)).await.unwrap();
    // A flush only writes to the socket. A round trip on the same connection confirms the server processed the SUB.
    js.query_account().await.expect("confirming the conductor's subscription");
    tokio::spawn(async move {
        while let Some(message) = requests.next().await {
            let request: TrailRequest = serde_json::from_slice(&message.payload).unwrap();
            let reply = request.answer(answer(&request));
            client.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap();
        }
    });
}

#[tokio::test]
async fn run_confirms_asks_the_conductor_and_reports_its_refusal() {
    let Some(server) = Server::start() else { return };
    let dir = ants_test_support::scratch_dir("trail-run");
    let file = write(&dir, "deploy.py", &script("deploy"));
    let file = file.to_str().unwrap();
    let creds = server.creds_as(&p("alice"), Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    conductor(&server, &p("lead"), |request| match &request.action {
        TrailAction::Start { args, .. } if args["branch"] == "main" => TrailOutcome::Started { run: RunId::generate() },
        _ => TrailOutcome::Refused { error: "no such branch".to_owned() },
    })
    .await;

    let error = start::run(&nats, run_args(&[file, "--ant", "lead"]), |_| Ok(None)).await.unwrap_err();
    assert_eq!(error.to_string(), "no terminal to confirm the run on: --yes starts it without asking");
    start::run(&nats, run_args(&[file, "--ant", "lead"]), |_| Ok(Some(false))).await.unwrap();
    let declined = start::run(&nats, run_args(&[file, "--ant", "lead", "--json"]), |_| Ok(Some(false))).await;
    assert_eq!(declined.unwrap_err().to_string(), "not started");
    let main = run_args(&[file, "--ant", "lead", "--args", r#"{"branch": "main"}"#]);
    start::run(&nats, main, |_| Ok(Some(true))).await.unwrap();
    let error = start::run(&nats, run_args(&[file, "--ant", "lead", "--yes"]), |_| unreachable!()).await.unwrap_err();
    assert_eq!(error.to_string(), "lead refused: no such branch");
    let over = run_args(&[file, "--ant", "lead", "--yes", "--at-once", "17"]);
    let error = start::run(&nats, over, |_| unreachable!()).await.unwrap_err();
    assert_eq!(error.to_string(), "--at-once is over the most a run may take, 16");
    let bare = write(&dir, "bare.py", "agent('build')\n");
    let error = start::run(&nats, run_args(&[bare.to_str().unwrap(), "--yes"]), |_| unreachable!()).await.unwrap_err();
    assert!(format!("{error:#}").contains("meta"), "{error:#}");

    let creds = server.creds_as(&p("dev-a"), Role::Ant);
    let ant = Nats::new(&server.url, Some(&creds), None).unwrap();
    let error = start::run(&ant, run_args(&[file, "--yes"]), |_| unreachable!()).await.unwrap_err();
    assert!(error.to_string().starts_with("starting a run needs a human's credentials"), "{error}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn humans_stop_and_resume_runs_by_a_unique_start_of_their_id() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let js = human(&server, &alice).await;
    let (client, _) = server.connect_as(&alice, Role::Human).await;
    let lead = p("lead");
    let (_lead, lead_js) = server.connect(&lead).await;
    let now = Utc::now();
    let id = |s: &str| -> RunId { s.parse().unwrap() };
    let mut waiting = record(&lead, Some("deploy"), RunState::Waiting, now - TimeDelta::minutes(3));
    waiting.run = id("0190aaaa-0000-7000-8000-000000000001");
    let mut stopped = record(&lead, Some("deploy"), RunState::Stopped { by: alice.clone() }, now);
    stopped.run = id("0190aaab-0000-7000-8000-000000000002");
    let mut done = record(&lead, None, RunState::Done { result: json!("shipped") }, now);
    done.run = id("0190bbbb-0000-7000-8000-000000000003");
    for run in [&waiting, &stopped, &done] {
        put(&lead_js, &run.key(), run).await;
    }
    let index = TrailRunIndex { runs: vec![waiting.run.clone(), stopped.run.clone(), done.run.clone()] };
    put(&lead_js, &trail_runs_index_key(&lead), &index).await;

    let pick = |s: &str| s.parse::<runs::Pick>().unwrap();
    assert_eq!(runs::pick(&js, &pick("0190AAAA")).await.unwrap().run, waiting.run);
    assert_eq!(runs::pick(&js, &pick(done.run.as_str())).await.unwrap().run, done.run);
    let error = runs::pick(&js, &pick("0190aaa")).await.unwrap_err().to_string();
    assert!(
        error.starts_with("2 runs' ids start with 0190aaa (") && error.ends_with("): give more of the id"),
        "{error}"
    );
    let error = runs::pick(&js, &pick("0190c")).await.unwrap_err().to_string();
    assert_eq!(error, "no run's id starts with 0190c: `ants trail runs` lists the runs kept");

    let find = |s: &str| super::super::find(&js, s.parse().unwrap(), true);
    let error = find("0190aaa").await.err().unwrap().to_string();
    assert!(
        error.starts_with("2 runs' ids start with 0190aaa (") && error.ends_with("): give more of the id"),
        "{error}"
    );
    let error = find("0190c").await.err().unwrap().to_string();
    assert!(error.starts_with("no trail 0190c, either in the colony or in"), "{error}");
    for id in ["0190aaaa", "0190AAAA", "0190AAAA-0000-7000-8000-000000000001"] {
        let Ok(super::super::Found::Run(run)) = find(id).await else { panic!("{id} is no run") };
        assert_eq!(run, waiting.run, "{id}");
    }
    let error = find("0190AAA").await.err().unwrap().to_string();
    assert!(error.starts_with("2 runs' ids start with 0190aaa ("), "{error}");

    let error = control::stop(&client, &alice, &done).await.unwrap_err().to_string();
    assert_eq!(error, format!("run {} has ended: done", done.run));
    let offline = p("offline");
    let unanswered =
        control::ask(&client, &offline, &alice, TrailAction::Stop { run: waiting.run.clone() }, Duration::ZERO);
    assert_eq!(unanswered.await.unwrap_err().to_string(), "no daemon serving offline answered within 0s");
    conductor(&server, &lead, |request| match &request.action {
        TrailAction::Stop { run } => TrailOutcome::Stopped {
            run: run.clone(),
            waiting: vec![WaitingStep { to: "dev-a".parse().unwrap(), message: Uuid::nil() }],
        },
        TrailAction::Resume { run, limits: Some(limits) } if limits.steps == 60 => {
            TrailOutcome::Resumed { run: run.clone() }
        }
        TrailAction::Resume { run, .. } => TrailOutcome::Refused { error: format!("run {run} is not stopped") },
        TrailAction::Start { .. } => unreachable!("no run starts here"),
    })
    .await;
    let text = control::stop(&client, &alice, &waiting).await.unwrap();
    assert!(text.ends_with(&format!("  dev-a message {0}: ants withdraw dev-a {0}\n", Uuid::nil())), "{text}");
    let raised = LimitArgs { steps: Some(60), ..LimitArgs::default() }.over(stopped.limits).unwrap();
    let text = control::resume(&client, &alice, &stopped, Some(raised)).await.unwrap();
    let under = "under 60 steps, 4 at once, 200 turns, 1d of wall time";
    assert_eq!(text, format!("resumed run {} on lead {under}", stopped.run));
    let error = control::resume(&client, &alice, &waiting, None).await.unwrap_err().to_string();
    assert_eq!(error, format!("lead refused: run {} is not stopped", waiting.run));

    let creds = server.creds_as(&p("dev-a"), Role::Ant);
    let ant = Nats::new(&server.url, Some(&creds), None).unwrap();
    let error = super::super::run(&ant, TrailCommand::Stop { run: pick("0190aaaa") }).await.unwrap_err().to_string();
    assert!(error.starts_with("stopping a run needs a human's credentials"), "{error}");
}

#[tokio::test]
async fn a_stop_waiting_for_the_runs_pass_is_told_pending_and_an_unanswered_start_names_its_run() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let (client, _) = server.connect_as(&alice, Role::Human).await;
    let slow = p("slow");
    let now = Utc::now();
    let (computing, quick) = (record(&slow, None, RunState::Waiting, now), record(&slow, None, RunState::Waiting, now));
    let (conductor_client, js) = server.connect(&slow).await;
    let mut requests = conductor_client.subscribe(subject::trail(&slow)).await.unwrap();
    js.query_account().await.unwrap();
    let still = computing.run.clone();
    tokio::spawn(async move {
        while let Some(message) = requests.next().await {
            let request: TrailRequest = serde_json::from_slice(&message.payload).unwrap();
            let TrailAction::Stop { run } = &request.action else { continue };
            let mut outcomes = vec![TrailOutcome::Stopping { run: run.clone() }];
            if *run != still {
                outcomes.push(TrailOutcome::Stopped { run: run.clone(), waiting: Vec::new() });
            }
            for outcome in outcomes {
                let reply = request.answer(outcome);
                conductor_client.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap();
            }
        }
    });

    let text = control::stop_within(&client, &alice, &computing, Duration::from_millis(500)).await.unwrap();
    assert_eq!(text, control::stopping(&computing.run, &slow));
    assert!(text.contains("pending") && text.contains(&format!("`ants trail stop {}`", computing.run)), "{text}");
    let text = control::stop_within(&client, &alice, &quick, Duration::from_secs(5)).await.unwrap();
    assert_eq!(text, control::stopped(&quick.run, &slow, &[]), "the stop that took effect within the wait");

    let start = TrailAction::Start {
        trail: TrailSource::Saved("deploy".parse().unwrap()),
        args: json!(null),
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
    };
    let error = control::ask(&client, &p("offline"), &alice, start, Duration::ZERO).await.unwrap_err().to_string();
    assert!(
        error.starts_with("no daemon serving offline answered within 0s; a run it starts later is run "),
        "{error}"
    );
}
