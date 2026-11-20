//! The steps of runs, as a run's records tell their stage and label.

use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, RunId, RunState, StepStage, ThreadId, TrailFunction, TrailRun, TrailSession,
    TrailStepThread, Uuid, trail_call_key, trail_session_key, trail_thread_step_key,
};
use chrono::{TimeDelta, Utc};
use serde_json::json;

use super::super::runs;
use super::{human, p, put, record};
use crate::test_support::Server;

fn journal(call: u32, label: &str, answer: CallAnswer) -> JournalEntry {
    JournalEntry { call, function: TrailFunction::Agent, args: json!({"prompt": "do it", "label": label}), answer }
}

fn waited(call: u32, thread: &ThreadId) -> PendingCall {
    PendingCall {
        call,
        function: TrailFunction::Agent,
        args: json!({"prompt": "review", "label": "review"}),
        to: p("dev-b"),
        thread: thread.clone(),
        message: Uuid::now_v7(),
        sent_at: Utc::now(),
        sent: true,
        turn_budget: 8,
        retries: 0,
    }
}

#[test]
fn a_step_is_started_until_answered_failed_or_dropped_as_the_records_tell() {
    let (run, run_thread) = (RunId::generate(), ThreadId::generate());
    let threads: Vec<ThreadId> = (0..5).map(|_| ThreadId::generate()).collect();
    let now = Utc::now();
    let marked: Vec<runs::Marked> = [(0, 0), (1, 1), (2, 2), (2, 3), (3, 4)]
        .into_iter()
        .map(|(call, at)| {
            let marker =
                TrailStepThread { run: run.clone(), call, to: p("dev-b"), run_thread: run_thread.clone(), label: None };
            (threads[at].clone(), marker, now + TimeDelta::seconds(i64::try_from(at).unwrap()))
        })
        .collect();
    let answers =
        [journal(0, "build", CallAnswer::Ok(json!("built"))), journal(1, "test", CallAnswer::Error("no".into()))];
    let stages = |pending: Option<&[PendingCall]>| {
        let steps = runs::staged(&p("lead"), marked.clone(), &answers, pending);
        steps.into_iter().map(|s| (s.call, s.label, s.stage)).collect::<Vec<_>>()
    };
    let label = |l: &str| Some(l.to_owned());
    let going = [waited(2, &threads[3])];
    assert_eq!(
        stages(Some(&going)),
        [
            (0, label("build"), StepStage::Answered),
            (1, label("test"), StepStage::Failed),
            (2, label("review"), StepStage::Dropped),
            (2, label("review"), StepStage::Started),
            (3, None, StepStage::Dropped),
        ],
        "a call sent anew drops its older thread, and one the run no longer waits on is dropped"
    );
    let ended = stages(Some(&[]));
    assert_eq!(ended[3], (2, None, StepStage::Dropped), "an ended or stopped run waits on none");
    let unknown = stages(None);
    assert_eq!((unknown[3].2, unknown[4].2), (StepStage::Started, StepStage::Started), "a session not read");
}

#[test]
fn a_step_keeps_the_label_its_marker_carries_once_its_run_ended() {
    let (run, run_thread) = (RunId::generate(), ThreadId::generate());
    let (child, asked, older) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let now = Utc::now();
    let mark = |call: u32, to: &str, label: Option<&str>| TrailStepThread {
        run: run.clone(),
        call,
        to: p(to),
        run_thread: run_thread.clone(),
        label: label.map(str::to_owned),
    };
    let marked = vec![
        (child, mark(0, "lead", Some("trail echo")), now),
        (asked, mark(1, "dev-b", Some("review")), now + TimeDelta::seconds(1)),
        (older, mark(2, "dev-b", None), now + TimeDelta::seconds(2)),
    ];
    let echoed = JournalEntry {
        call: 0,
        function: TrailFunction::Trail,
        args: json!({"name": "echo", "args": {}}),
        answer: CallAnswer::Ok(json!("A")),
    };
    let child_failed = JournalEntry {
        call: 2,
        function: TrailFunction::Trail,
        args: json!({"name": "broken"}),
        answer: CallAnswer::Error("bad input".into()),
    };
    let steps = runs::staged(&p("lead"), marked, &[echoed, child_failed], Some(&[]));
    let shown: Vec<_> = steps.iter().map(|s| (s.call, s.to.as_str(), s.label.as_deref(), s.stage)).collect();
    assert_eq!(
        shown,
        [
            (0, "lead", Some("trail echo"), StepStage::Answered),
            (1, "dev-b", Some("review"), StepStage::Dropped),
            (2, "dev-b", Some("trail broken"), StepStage::Failed),
        ],
        "a trail() step reads back with its label and stage, an unanswered one of a stopped run keeps its label, and a \
         marker without one takes it from its call's arguments"
    );
}

#[tokio::test]
async fn humans_read_the_steps_a_conductor_recorded_on_the_thread_of_its_runs() {
    let Some(server) = Server::start() else { return };
    let js = human(&server, &p("alice")).await;
    let lead = p("lead");
    let (_lead, lead_js) = server.connect(&lead).await;
    let now = Utc::now();
    let running = record(&lead, Some("review"), RunState::Running, now);
    let mut done = record(&lead, Some("review"), RunState::Done { result: json!("ok") }, now);
    done.thread = running.thread.clone();
    let elsewhere = record(&lead, Some("review"), RunState::Running, now);
    for run in [&running, &done, &elsewhere] {
        put(&lead_js, &run.key(), run).await;
    }
    let (answered, waiting, left) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let other = ThreadId::generate();
    let mark = |run: &TrailRun, call: u32, to: &str| TrailStepThread {
        run: run.run.clone(),
        call,
        to: p(to),
        run_thread: run.thread.clone(),
        label: None,
    };
    put(&lead_js, &trail_thread_step_key(&lead, &running.thread, &answered), &mark(&running, 0, "dev-c")).await;
    put(&lead_js, &trail_thread_step_key(&lead, &running.thread, &waiting), &mark(&running, 1, "dev-b")).await;
    put(&lead_js, &trail_thread_step_key(&lead, &done.thread, &left), &mark(&done, 0, "dev-b")).await;
    put(&lead_js, &trail_thread_step_key(&lead, &elsewhere.thread, &other), &mark(&elsewhere, 0, "dev-b")).await;
    put(&lead_js, &trail_call_key(&lead, &running.run, 0), &journal(0, "build", CallAnswer::Ok(json!("built")))).await;
    let session = TrailSession {
        session: "opaque".to_owned(),
        pending: vec![waited(1, &waiting)],
        queued: Vec::new(),
        anew: Vec::new(),
    };
    put(&lead_js, &trail_session_key(&lead, &running.run), &session).await;

    let steps = runs::steps_on(&js, &lead, &running.thread).await.unwrap();
    let shown: Vec<_> = steps.iter().map(|s| (&s.thread, s.call, s.to.as_str(), s.label.as_deref(), s.stage)).collect();
    assert_eq!(
        shown,
        [
            (&answered, 0, "dev-c", Some("build"), StepStage::Answered),
            (&waiting, 1, "dev-b", Some("review"), StepStage::Started),
            (&left, 0, "dev-b", None, StepStage::Dropped),
        ]
    );
    assert!(steps.iter().all(|s| s.conductor == lead) && steps[0].run == running.run && steps[2].run == done.run);
    assert!(steps.windows(2).all(|w| w[0].at <= w[1].at), "by when each started");
    assert!(runs::steps_on(&js, &p("dev-a"), &running.thread).await.unwrap().is_empty(), "another conductor's");
}
