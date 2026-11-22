//! The runs conductors keep: `ants trail runs` and `ants trail show RUN`.

use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, QueuedCall, Role, RunId, RunState, ThreadId, TrailFunction, TrailLeg,
    TrailParent, TrailRun, TrailRunIndex, TrailSession, Uuid, trail_call_key, trail_runs_index_key, trail_session_key,
};
use chrono::{TimeDelta, Utc};
use serde_json::json;

use super::super::{Shown, TrailCommand, runs};
use super::{human, p, put, record, script};
use crate::connection::Nats;
use crate::test_support::Server;

#[test]
fn a_child_run_shows_the_call_it_answers_and_a_trail_call_its_child() {
    let now = Utc::now();
    let parent = record(&p("dev-a"), Some("release"), RunState::Waiting, now);
    let mut child = record(&p("dev-a"), Some("review"), RunState::Running, now);
    child.parent = Some(TrailParent { run: parent.run.clone(), call: 1, origin_thread: parent.thread.clone() });
    let call = PendingCall {
        call: 1,
        function: TrailFunction::Trail,
        args: json!({"name": "review", "args": {"pr": 7}}),
        to: p("dev-a"),
        thread: child.thread.clone(),
        message: child.run.as_str().parse().unwrap(),
        sent_at: now,
        sent: true,
        turn_budget: 0,
        retries: 0,
    };
    let shown = |run: &TrailRun, pending: Vec<PendingCall>, children: Vec<TrailRun>| {
        let detail = runs::Detail {
            run: run.clone(),
            pending,
            queued: Vec::new(),
            journal: Vec::new(),
            children,
            ratings: Vec::new(),
        };
        runs::render(&detail, now)
    };
    let text = shown(&child, Vec::new(), Vec::new());
    assert!(text.contains(&format!("\nchild run answering call 1 of run {}\n", parent.run)), "{text}");
    assert!(!text.contains("child runs:"), "{text}");
    let text = shown(&parent, vec![call], vec![child.clone()]);
    let row = format!("  1  trail  run {}  sent 0s ago  thread {}  trail review\n", child.run, child.thread);
    assert!(text.contains(&row) && !text.contains("child run answering"), "{text}");
    assert!(text.contains(&format!("\nchild runs:\n  {}  review  running  call 1\n", child.run)), "{text}");
}

#[test]
fn a_run_started_answering_a_step_shows_the_run_it_spends_from() {
    let now = Utc::now();
    let mut run = record(&p("dev-a"), None, RunState::Running, now);
    let from = RunId::generate();
    let bounds = ants_proto::StepBounds { steps: 3, turns: 20, until: now, route: vec!["deploy".parse().unwrap()] };
    run.spends_from = Some(ants_proto::SpendsFrom { conductor: p("lead"), run: from.clone(), call: 2, bounds });
    let detail = runs::Detail {
        run,
        pending: Vec::new(),
        queued: Vec::new(),
        journal: Vec::new(),
        children: Vec::new(),
        ratings: Vec::new(),
    };
    let text = runs::render(&detail, now);
    let line = format!(
        "\nstarted answering call 2 of run {from} of lead, spending from that run's grant of 3 steps and 20 turns\n"
    );
    assert!(text.contains(&line), "{text}");
}

#[test]
fn a_run_shows_the_legs_of_its_route_and_the_gotos_it_made() {
    let now = Utc::now();
    let mut run = record(&p("dev-a"), Some("triage"), RunState::Running, now);
    let next = TrailLeg {
        trail: Some("release".parse().unwrap()),
        local: false,
        at: Some("review".to_owned()),
        script: script("release"),
        args: json!({"version": "1.2"}),
        first_call: 2,
        started_at: now,
        steps: 1,
    };
    run.route.push(next);
    let goto = |args, answer| JournalEntry { call: 1, function: TrailFunction::Goto, args, answer };
    let refused = goto(json!({"trail": "triage"}), CallAnswer::Error("the run entered trail triage before".into()));
    let detail = runs::Detail {
        run: run.clone(),
        pending: Vec::new(),
        queued: Vec::new(),
        journal: vec![refused],
        children: Vec::new(),
        ratings: Vec::new(),
    };
    let text = runs::render(&detail, now);
    assert!(text.starts_with(&format!("run {} of trail triage\n", run.run)), "the trail it started: {text}");
    let (_, route) = text.split_once("\nroute: triage -> release@review\n").unwrap_or_else(|| panic!("{text}"));
    let legs: Vec<&str> = route.lines().take(2).collect();
    assert!(legs[0].starts_with("  0  triage   start   started ") && legs[0].ends_with("  steps 2"), "{text}");
    assert!(legs[1].starts_with("  1  release  review  started ") && legs[1].contains("  steps 1  "), "{text}");
    assert!(legs[1].ends_with("  state {\"version\":\"1.2\"}"), "{text}");
    assert!(text.contains("args: {\"branch\":\"main\"}\n"), "the run's input is its first leg's: {text}");
    let row = "  1  goto  dev-a  trail triage at its start  error: the run entered trail triage before\n";
    assert!(text.contains(row), "{text}");
    let table = runs::table(std::slice::from_ref(&run), &runs::Rated::default(), now);
    let row = table.lines().nth(1).unwrap_or_else(|| panic!("{table}"));
    assert!(
        row.starts_with(&format!("{}  triage -> release@review  dev-a  ", run.run)),
        "the trail it started: {table}"
    );
    let header =
        serde_json::from_str::<serde_json::Value>(&runs::header_json(&run, &runs::Rated::default()).unwrap()).unwrap();
    let route = header["route"].as_array().unwrap();
    assert!(route.len() == 2 && route.iter().all(|leg| leg.get("script").is_none()), "{header}");
    let inline = runs::render(&runs::Detail { run: record(&p("dev-a"), None, RunState::Running, now), ..detail }, now);
    assert!(inline.contains("\nroute: inline\n  0  inline  start  started "), "{inline}");
}

#[test]
fn a_run_failed_on_a_limit_shows_which() {
    let now = Utc::now();
    let shown = |state| {
        let run = record(&p("dev-a"), Some("release"), state, now);
        runs::render(
            &runs::Detail {
                run,
                pending: Vec::new(),
                queued: Vec::new(),
                journal: Vec::new(),
                children: Vec::new(),
                ratings: Vec::new(),
            },
            now,
        )
    };
    let error = "the run reached its limit of 200 turns".to_owned();
    let text = shown(RunState::Failed { error: error.clone(), limit: Some(ants_proto::RunLimit::Turns) });
    assert!(text.contains(&format!("\nstate: failed on its turns limit: {error}\n")), "{text}");
    let text = shown(RunState::Failed { error: error.clone(), limit: None });
    assert!(text.contains(&format!("\nstate: failed: {error}\n")), "{text}");
}

#[tokio::test]
async fn humans_list_and_show_the_runs_conductors_keep() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let js = human(&server, &alice).await;
    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    let ((_a, a_js), (_b, b_js)) = (server.connect(&dev_a).await, server.connect(&dev_b).await);
    let now = Utc::now();
    let waiting = record(&dev_a, Some("deploy"), RunState::Waiting, now - TimeDelta::minutes(5));
    let mut done = record(&dev_b, None, RunState::Done { result: json!("shipped") }, now - TimeDelta::hours(1));
    done.ended_at = Some(now - TimeDelta::minutes(30));
    put(&a_js, &waiting.key(), &waiting).await;
    put(&a_js, &trail_runs_index_key(&dev_a), &TrailRunIndex { runs: vec![waiting.run.clone()] }).await;
    let step = ThreadId::generate();
    let session = TrailSession {
        session: "opaque".to_owned(),
        pending: vec![PendingCall {
            call: 2,
            function: TrailFunction::Human,
            args: json!({"questions": "Ship it?"}),
            to: alice.clone(),
            thread: step.clone(),
            message: Uuid::now_v7(),
            sent_at: now - TimeDelta::minutes(1),
            sent: true,
            turn_budget: 8,
            retries: 0,
        }],
        queued: vec![QueuedCall { call: 3, function: TrailFunction::Agent, args: json!({"prompt": "announce"}) }],
        anew: Vec::new(),
    };
    put(&a_js, &trail_session_key(&dev_a, &waiting.run), &session).await;
    let built = JournalEntry {
        call: 0,
        function: TrailFunction::Agent,
        args: json!({"prompt": "build the release", "ant": "dev-c", "label": "build"}),
        answer: CallAnswer::Ok(json!("built")),
    };
    let tested = JournalEntry {
        call: 1,
        function: TrailFunction::Agent,
        args: json!({"prompt": "run the tests", "capability": "test"}),
        answer: CallAnswer::Error("tests failed".to_owned()),
    };
    put(&a_js, &trail_call_key(&dev_a, &waiting.run, 1), &tested).await;
    put(&a_js, &trail_call_key(&dev_a, &waiting.run, 0), &built).await;
    put(&b_js, &done.key(), &done).await;
    put(&b_js, &trail_session_key(&dev_b, &done.run), &session).await;

    assert_eq!(runs::list(&js, None).await.unwrap(), [done.clone(), waiting.clone()]);
    assert_eq!(runs::list(&js, Some(&dev_a)).await.unwrap(), std::slice::from_ref(&waiting));
    assert!(runs::list(&js, Some(&p("dev-c"))).await.unwrap().is_empty());
    let header =
        serde_json::from_str::<serde_json::Value>(&runs::header_json(&waiting, &runs::Rated::default()).unwrap())
            .unwrap();
    assert!(header["route"][0].get("script").is_none() && header["run"] == waiting.run.as_str(), "{header}");
    let table = runs::table(&[done.clone(), waiting.clone()], &runs::Rated::default(), now);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].starts_with("RUN") && lines[0].contains("ENDED") && lines[0].ends_with("RATED"), "{table}");
    for cell in [done.run.as_str(), "inline", "dev-b", "alice", "done", "2/", "1h ago", "30m ago"] {
        assert!(lines[1].contains(cell), "{cell}: {table}");
    }
    assert!(lines[2].contains("deploy") && lines[2].contains("waiting") && lines[2].contains("5m ago"), "{table}");

    let detail = runs::detail(&js, &waiting.run).await.unwrap();
    assert_eq!(detail.run, waiting);
    assert_eq!((detail.pending, detail.queued.clone()), (session.pending.clone(), session.queued.clone()));
    assert_eq!(detail.journal, [built, tested]);
    let detail = runs::detail(&js, &waiting.run).await.unwrap();
    let json = serde_json::to_value(&detail).unwrap();
    assert!(json.get("session").is_none() && json["journal"].as_array().unwrap().len() == 2, "{json}");
    assert!(detail.children.is_empty() && json.get("children").is_none(), "{json}");
    let text = runs::render(&detail, now);
    for line in [
        format!("run {} of trail deploy", waiting.run),
        format!("conductor dev-a, owner alice, on thread {} started by human alice", waiting.thread),
        "state: waiting".to_owned(),
        "phase: build".to_owned(),
        "args: {\"branch\":\"main\"}".to_owned(),
    ] {
        assert!(text.contains(&format!("{line}\n")), "{line}: {text}");
    }
    assert!(text.contains("limits: steps 2 of "), "{text}");
    assert!(text.contains(&format!("  2  human  to alice  sent 1m ago  thread {step}  Ship it?\n")), "{text}");
    assert!(text.contains("queued calls:\n  3  agent  announce\n"), "{text}");
    assert!(text.contains("  0  agent  dev-c            build          ok: built\n"), "{text}");
    assert!(text.contains("  1  agent  capability test  run the tests  error: tests failed\n"), "{text}");
    let text = runs::render(&runs::detail(&js, &done.run).await.unwrap(), now);
    assert!(text.starts_with(&format!("run {} of an inline script\nconductor dev-b", done.run)), "{text}");
    assert!(text.contains("state: done, result: \"shipped\"\n") && !text.contains("journal"), "{text}");
    assert!(!text.contains("pending calls") && !text.contains("queued calls"), "the calls of an ended run: {text}");

    let absent = RunId::generate();
    let error = runs::detail(&js, &absent).await.unwrap_err().to_string();
    assert_eq!(error, format!("no run {absent}: `ants trail runs` lists the runs kept"));
    let creds = server.creds_as(&dev_a, Role::Ant);
    let ant = Nats::new(&server.url, Some(&creds), None).unwrap();
    let error = super::super::run(&ant, TrailCommand::Runs { ant: None, json: false }).await.unwrap_err();
    assert!(error.to_string().starts_with("reading the runs of trails needs a human's"), "{error}");
    let show = TrailCommand::Show { what: Shown::Run(waiting.run), json: false, follow: false };
    assert!(super::super::run(&ant, show).await.is_err());
}
