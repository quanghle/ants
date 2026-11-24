//! Snapshots of trails: their admin orders, runs and child runs, ratings, control requests, limits and answers.

use super::*;

#[test]
fn admin_orders_of_trails() {
    snap("admin_op_stop_trail", &AdminOp::StopTrail { run: run() });
    let limits = TrailLimits { steps: 100, wall_secs: 2 * 24 * 3600, ..TrailLimits::DEFAULT };
    snap("admin_op_resume_trail", &AdminOp::ResumeTrail { run: run(), limits: Some(limits) });
}

#[test]
fn trail_child_runs() {
    let child = TrailRun {
        run: "019a7d2e-0000-7000-8000-000000000007".parse().unwrap(),
        conductor: user("planner"),
        route: vec![TrailLeg {
            trail: Some("review".parse().unwrap()),
            local: false,
            at: None,
            script: "await agent('review it', ant='dev-a')".to_owned(),
            args: serde_json::json!({"pr": 7}),
            first_call: 0,
            started_at: at(4),
            steps: 1,
        }],
        owner: Some(user("alice")),
        thread: other_thread(),
        parent: Some(TrailParent { run: run(), call: 2, origin_thread: thread() }),
        spends_from: None,
        grants: Vec::new(),
        origin: provider::Origin { by: provider::Starter::Human { user: user("alice") }, channel: Some(channel()) },
        work_branch: None,
        limits: TrailLimits { steps: 20, ..TrailLimits::DEFAULT },
        state: RunState::Running,
        phase: None,
        steps: 1,
        turns: 1,
        started_at: at(4),
        updated_at: at(5),
        ended_at: None,
    };
    snap("trail_run_child", &child);
    let call = PendingCall {
        call: 2,
        function: TrailFunction::Trail,
        args: serde_json::json!({"args": {"pr": 7}, "name": "review"}),
        to: user("planner"),
        thread: other_thread(),
        message: id(7),
        sent_at: at(4),
        sent: true,
        turn_budget: 0,
        retries: 0,
        asked: false,
    };
    assert_eq!(call.child(), Some(child.run), "a trail() call's message is its child run's id");
    snap("trail_child_call", &call);
}

/// A run that went from triage's start to release's review waypoint.
fn routed_run() -> TrailRun {
    TrailRun {
        run: run(),
        conductor: user("planner"),
        route: vec![
            TrailLeg {
                trail: Some("triage".parse().unwrap()),
                local: false,
                at: None,
                script: "await goto('release', at='review', state={'version': '1.2.0'})".to_owned(),
                args: serde_json::json!({"bug": 7}),
                first_call: 0,
                started_at: at(0),
                steps: 2,
            },
            TrailLeg {
                trail: Some("release".parse().unwrap()),
                local: false,
                at: Some("review".to_owned()),
                script: "async def review(state):\n    return await agent(state['version'], ant='dev-a')".to_owned(),
                args: serde_json::json!({"version": "1.2.0"}),
                first_call: 3,
                started_at: at(2),
                steps: 1,
            },
        ],
        owner: Some(user("alice")),
        thread: thread(),
        parent: None,
        spends_from: None,
        grants: Vec::new(),
        origin: provider::Origin { by: provider::Starter::Human { user: user("alice") }, channel: Some(channel()) },
        work_branch: Some(WorkBranch { name: format!("ants/dev-a/{}", other_thread()), by: user("dev-a") }),
        limits: TrailLimits { steps: 20, ..TrailLimits::DEFAULT },
        state: RunState::Done { result: serde_json::json!({"tag": "v1.2.0"}) },
        phase: Some("review".to_owned()),
        steps: 3,
        turns: 5,
        started_at: at(0),
        updated_at: at(9),
        ended_at: Some(at(9)),
    }
}

#[test]
fn trails() {
    snap(
        "envelope_step",
        &Envelope { from: user("planner"), to: user("dev-a"), ..plain_envelope().as_step(trail_step()) },
    );
    let saved = SavedTrail {
        name: "release".parse().unwrap(),
        description: "Cut a release".to_owned(),
        phases: vec!["build".to_owned(), "review".to_owned()],
        waypoints: vec!["build".to_owned(), "review".to_owned()],
        script: "agent('dev-a', 'build it')".to_owned(),
        saved_by: Saver::Human { user: user("alice") },
        saved_from: None,
        saved_at: at(0),
    };
    snap("saved_trail", &saved);
    snap(
        "saved_trail_by_ant",
        &SavedTrail { saved_by: Saver::Ant { user: user("dev-a") }, saved_from: Some(run()), ..saved },
    );
    let header = routed_run();
    assert_eq!(header.route_text(), "triage -> release@review");
    snap("trail_run", &header);
    let mut newer = serde_json::to_value(&header).unwrap();
    newer["paused_at"] = serde_json::json!("2025-01-01T12:05:00Z");
    assert_eq!(serde_json::from_value::<TrailRun>(newer).unwrap(), header, "unknown fields are additions");
    let states = [
        RunState::Running,
        RunState::Waiting,
        RunState::Stopped { by: user("alice") },
        RunState::Failed { error: "steps limit of 20 reached".to_owned(), limit: None },
    ];
    let runs: Vec<_> = states.into_iter().map(|state| TrailRun { state, ended_at: None, ..header.clone() }).collect();
    snap("trail_run_states", &runs);
    snap(
        "trail_session",
        &TrailSession {
            session: r#"{"monty":"dump"}"#.to_owned(),
            pending: vec![PendingCall {
                call: 3,
                function: TrailFunction::Agent,
                args: serde_json::json!(["dev-a", "review it"]),
                to: user("dev-a"),
                thread: other_thread(),
                message: id(5),
                sent_at: at(4),
                sent: true,
                turn_budget: 4,
                retries: 1,
                asked: false,
            }],
            queued: vec![QueuedCall { call: 4, function: TrailFunction::Human, args: serde_json::json!(["ship it?"]) }],
            anew: vec![2],
        },
    );
    let args = serde_json::json!(["dev-a", "build it"]);
    let answer = |call, answer| JournalEntry { call, function: TrailFunction::Agent, args: args.clone(), answer };
    let refused = JournalEntry {
        call: 3,
        function: TrailFunction::Goto,
        args: serde_json::json!({"state": 7, "trail": "triage"}),
        answer: CallAnswer::Error("the run entered trail triage at its start before".to_owned()),
    };
    snap(
        "journal_entries",
        &[
            answer(1, CallAnswer::Ok(serde_json::json!({"ok": true}))),
            answer(2, CallAnswer::Error("dev-a is not an ant".to_owned())),
            refused,
        ],
    );
    snap("trail_run_index", &TrailRunIndex { runs: vec![run()] });
    snap(
        "trail_tip",
        &TrailTip {
            trail: "release".parse().unwrap(),
            id: "k3f7q2ab".parse().unwrap(),
            writer: user("dev-a"),
            text: "Run the migrations before the smoke tests.".to_owned(),
            tip_key: Some("review".to_owned()),
            run: run(),
            script: script_sha256("agent('dev-a', 'build it')"),
            written_at: at(3),
            retired: Some(TipRetired { by: Some(user("alice")), at: at(9) }),
        },
    );
    snap("tip_retired", &TipRetired { by: Some(user("alice")), at: at(9) });
    snap(
        "tip_vote",
        &TipVote {
            voter: user("dev-b"),
            verdict: TipVerdict::Stale,
            reason: Some("the smoke tests now run the migrations".to_owned()),
            text: script_sha256("Run the migrations before the smoke tests."),
            script: Some(script_sha256("agent('dev-a', 'build it')")),
            at: at(5),
        },
    );
    let limits: TrailLimits = parse(r#"{"steps": 10}"#);
    assert_eq!(limits, TrailLimits { steps: 10, ..TrailLimits::DEFAULT }, "missing limits take their defaults");
}

#[test]
fn trail_ratings() {
    snap(
        "trail_rating",
        &TrailRating {
            trail: "release".parse().unwrap(),
            conductor: user("dev-a"),
            run: run(),
            outcome: RatingOutcome::Partly,
            task: "release 2.1 with the migration".to_owned(),
            why: "built and tagged, but the smoke tests needed a rerun".to_owned(),
            script: script_sha256("agent('dev-a', 'build it')"),
            untrusted: false,
            at: at(6),
        },
    );
}

#[test]
fn trail_control() {
    let request = |action| TrailRequest { id: id(1), ant: user("planner"), by: user("alice"), sent_at: at(0), action };
    snap(
        "trail_requests",
        &[
            request(TrailAction::Start {
                trail: TrailSource::Saved("release".parse().unwrap()),
                args: serde_json::json!({"version": "1.2.0"}),
                thread: thread(),
                limits: TrailLimits { steps: 20, ..TrailLimits::DEFAULT },
            }),
            request(TrailAction::Start {
                trail: TrailSource::Script("agent('dev-a', 'build it')".to_owned()),
                args: serde_json::Value::Null,
                thread: thread(),
                limits: TrailLimits::DEFAULT,
            }),
            request(TrailAction::Stop { run: run() }),
            request(TrailAction::Resume {
                run: run(),
                limits: Some(TrailLimits { steps: 40, ..TrailLimits::DEFAULT }),
            }),
        ],
    );
    let reply = |outcome| TrailReply { request_id: id(1), ant: user("planner"), outcome };
    snap(
        "trail_replies",
        &[
            reply(TrailOutcome::Started { run: run() }),
            reply(TrailOutcome::Stopped {
                run: run(),
                waiting: vec![WaitingStep { to: user("dev-a"), message: id(5) }],
            }),
            reply(TrailOutcome::Resumed { run: run() }),
            reply(TrailOutcome::Stopping { run: run() }),
            reply(TrailOutcome::Refused { error: "no saved trail release".to_owned() }),
        ],
    );
}

#[test]
fn trail_run_limits() {
    use ants_proto::RunLimit;
    let limits =
        [RunLimit::Steps, RunLimit::Turns, RunLimit::Wall, RunLimit::Compute, RunLimit::Session, RunLimit::Memory];
    let failed = limits.map(|limit| RunState::Failed { error: format!("past its {limit} limit"), limit: Some(limit) });
    snap("trail_run_failed_on_limits", &failed.to_vec());
}

#[test]
fn trail_run_spending_from_a_step() {
    let spends_from = SpendsFrom {
        conductor: user("lead"),
        run: "019a7d2e-0000-7000-8000-000000000009".parse().unwrap(),
        call: 4,
        bounds: StepBounds { steps: 6, turns: 40, until: at(9), route: vec!["deploy".parse().unwrap()] },
    };
    let grant = StepGrant {
        call: 2,
        thread: "019a7d2e-0000-7000-8000-00000000000a".parse().unwrap(),
        steps: 3,
        turns: 20,
        spent: TrailSpent { steps: 1, turns: 2, running: 1 },
    };
    let run = TrailRun { spends_from: Some(spends_from), grants: vec![grant], ..routed_run() };
    snap("trail_run_spending_from_a_step", &run);
}

#[test]
fn trail_answering() {
    let answering = TrailAnswering {
        conductor: user("lead"),
        run: "019a7d2e-0000-7000-8000-000000000009".parse().unwrap(),
        call: 4,
        runs: vec!["019a7d2e-0000-7000-8000-00000000000b".parse().unwrap()],
    };
    snap("trail_answering", &answering);
}
