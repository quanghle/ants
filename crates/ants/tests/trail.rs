//! Trail runs driven through real `ants subprocess` workers.

use ants_trail::{
    Answer, Call, Event, Function, JournalEntry, Limits, Progress, Run, RunError, RunState, Status, Worker,
};
use serde_json::{Value, json};

const META: &str = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": [\"one\"]}\n";

fn worker() -> Worker {
    Worker::new(env!("CARGO_BIN_EXE_ants"))
}

fn run(script: &str, args: Value, limits: Limits) -> Run {
    Run::with_clock(&format!("{META}{script}"), None, args, limits, 1_700_000_000, 7).unwrap()
}

fn prompts(calls: &[Call]) -> Vec<&str> {
    calls.iter().map(|call| call.args["prompt"].as_str().unwrap()).collect()
}

fn call<'a>(calls: &'a [Call], prompt: &str) -> &'a Call {
    calls.iter().find(|call| call.args["prompt"] == prompt).unwrap()
}

fn done(run: &Run) -> &Value {
    match run.status() {
        Status::Done { result } => result,
        other => panic!("not done: {other:?}"),
    }
}

fn failed(run: &Run) -> &str {
    match run.status() {
        Status::Failed { error } => error,
        other => panic!("not failed: {other:?}"),
    }
}

async fn answer(run: &mut Run, worker: &Worker, calls: &[Call], prompt: &str, value: Value) -> Progress {
    run.resolve(worker, call(calls, prompt).call, Answer::Ok(value)).await.unwrap()
}

const FAN_OUT: &str = r#"
phase("one")
async def item(name):
    first = await agent(f"first {name}")
    log(f"{name} got {first}")
    return await agent(f"then {first}", label=name)

await parallel(*[item(name) for name in args])
"#;

#[tokio::test]
async fn a_fan_out_moves_each_item_on_as_its_answer_arrives() {
    let worker = worker();
    let mut run = run(FAN_OUT, json!(["a", "b", "c"]), Limits::default());
    let started = run.start(&worker).await.unwrap();
    assert_eq!(prompts(&started.calls), ["first a", "first b", "first c"]);
    assert_eq!(started.events, [Event::Phase("one".to_owned())]);
    let mut calls = started.calls;

    let progress = answer(&mut run, &worker, &calls, "first c", json!("C")).await;
    assert_eq!(prompts(&progress.calls), ["then C"]);
    assert_eq!(progress.calls[0].args["label"], "c");
    assert_eq!(progress.events, [Event::Log("c got C".to_owned())]);
    calls.extend(progress.calls);
    let progress = answer(&mut run, &worker, &calls, "then C", json!("c done")).await;
    assert!(progress.calls.is_empty());

    run.answer(call(&calls, "first a").call, Answer::Ok(json!("A"))).unwrap();
    run.answer(call(&calls, "first b").call, Answer::Ok(json!("B"))).unwrap();
    let progress = run.resume(&worker).await.unwrap();
    assert_eq!(prompts(&progress.calls), ["then A", "then B"]);
    calls.extend(progress.calls);

    answer(&mut run, &worker, &calls, "then B", json!("b done")).await;
    assert_eq!(*run.status(), Status::Running);
    let last = answer(&mut run, &worker, &calls, "then A", json!("a done")).await;
    assert!(last.calls.is_empty() && last.cancelled.is_empty());
    assert_eq!(*done(&run), json!(["a done", "b done", "c done"]));
    assert_eq!(run.journal().len(), 6);
    assert_eq!(
        run.journal()[0],
        JournalEntry {
            call: call(&calls, "first c").call,
            function: Function::Agent,
            args: json!({"prompt": "first c"}),
            answer: Answer::Ok(json!("C")),
        }
    );
}

#[tokio::test]
async fn a_pipeline_moves_each_item_through_its_stages_without_a_barrier() {
    let script = r#"
async def draft(item):
    return await agent(f"draft {item}")

async def review(text):
    return await human([f"review {text}"])

await pipeline(args, draft, review)
"#;
    let worker = worker();
    let mut run = run(script, json!(["a", "b"]), Limits::default());
    let calls = run.start(&worker).await.unwrap().calls;
    assert_eq!(prompts(&calls), ["draft a", "draft b"]);

    let progress = answer(&mut run, &worker, &calls, "draft b", json!("B")).await;
    assert_eq!(progress.calls.len(), 1);
    assert_eq!(progress.calls[0].function, Function::Human);
    assert_eq!(progress.calls[0].args, json!({"questions": ["review B"]}));
    let review_b = progress.calls[0].call;

    let progress = answer(&mut run, &worker, &calls, "draft a", json!("A")).await;
    let review_a = progress.calls[0].call;
    run.resolve(&worker, review_a, Answer::Ok(json!("A ok"))).await.unwrap();
    run.resolve(&worker, review_b, Answer::Ok(json!("B ok"))).await.unwrap();
    assert_eq!(*done(&run), json!(["A ok", "B ok"]));
}

#[tokio::test]
async fn a_failing_step_raises_in_the_script() {
    let script = r#"
async def item(name):
    try:
        return await agent(name)
    except RuntimeError as e:
        return f"{name} failed: {e}"

await parallel(item("a"), item("b"))
"#;
    let worker = worker();
    let mut caught = run(script, json!(null), Limits::default());
    let calls = caught.start(&worker).await.unwrap().calls;
    caught.resolve(&worker, calls[0].call, Answer::Error("no such ant".to_owned())).await.unwrap();
    caught.resolve(&worker, calls[1].call, Answer::Ok(json!("fine"))).await.unwrap();
    assert_eq!(*done(&caught), json!(["a failed: no such ant", "fine"]));

    let uncaught = "await parallel(agent('a'), agent('b'))\n";
    let mut run = run(uncaught, json!(null), Limits::default());
    let calls = run.start(&worker).await.unwrap().calls;
    let progress = run.resolve(&worker, calls[1].call, Answer::Error("no such ant".to_owned())).await.unwrap();
    assert_eq!(progress.cancelled, [calls[0].call]);
    let error = failed(&run);
    // A gathered coroutine's traceback starts in it, here in the prelude's agent().
    assert!(error.contains("File \"<prelude>\", line 5, in agent"), "{error}");
    assert!(error.ends_with("RuntimeError: no such ant"), "{error}");
    assert!(matches!(run.answer(calls[0].call, Answer::Ok(json!(1))), Err(RunError::Ended)));
}

#[tokio::test]
async fn a_script_raising_fails_the_run_with_its_traceback() {
    let script = "x = await agent('a')\nraise ValueError(f'bad {x}')\n";
    let worker = worker();
    let mut run = run(script, json!(null), Limits::default());
    let calls = run.start(&worker).await.unwrap().calls;
    run.resolve(&worker, calls[0].call, Answer::Ok(json!(1))).await.unwrap();
    let error = failed(&run);
    assert!(error.starts_with("Traceback (most recent call last):"), "{error}");
    assert!(error.contains("File \"trail.py\", line 3, in <module>"), "{error}");
    assert!(error.ends_with("ValueError: bad 1"), "{error}");
}

#[tokio::test]
async fn a_run_continues_in_a_fresh_run_from_its_stored_state() {
    let worker = worker();
    let mut first = run(FAN_OUT, json!(["a", "b"]), Limits::default());
    let calls = first.start(&worker).await.unwrap().calls;
    let progress = answer(&mut first, &worker, &calls, "first a", json!("A")).await;
    let then_a = progress.calls[0].clone();
    let stored = serde_json::to_string(first.state()).unwrap();
    // Answered, its entry stored, but the host stopped before the run's state was stored again.
    let entry = first.answer(call(&calls, "first b").call, Answer::Ok(json!("B"))).unwrap();
    let mut journal: Vec<JournalEntry> = first.journal().to_vec();
    drop(first);
    journal.reverse();

    let state: RunState = serde_json::from_str(&stored).unwrap();
    let mut second = Run::restore(state, journal);
    assert_eq!(second.journal().last(), Some(&entry));
    assert_eq!(second.outstanding(), std::slice::from_ref(&then_a));
    let progress = second.resume(&worker).await.unwrap();
    assert_eq!(prompts(&progress.calls), ["then B"]);
    let then_b = progress.calls[0].call;
    second.resolve(&worker, then_a.call, Answer::Ok(json!(1))).await.unwrap();
    second.resolve(&worker, then_b, Answer::Ok(json!(2))).await.unwrap();
    assert_eq!(*done(&second), json!([1, 2]));
}

#[tokio::test]
async fn the_step_limit_raises_on_the_call_past_it() {
    let script = "for i in range(3):\n    await agent(str(i))\n";
    let worker = worker();
    let mut run = run(script, json!(null), Limits { steps: 2, ..Limits::default() });
    for _ in 0..2 {
        let calls = run.outstanding().to_vec();
        let progress = match calls.first() {
            Some(call) => run.resolve(&worker, call.call, Answer::Ok(json!(0))).await.unwrap(),
            None => run.start(&worker).await.unwrap(),
        };
        assert_eq!(progress.calls.len(), 1);
    }
    let call = run.outstanding()[0].call;
    run.resolve(&worker, call, Answer::Ok(json!(0))).await.unwrap();
    assert!(failed(&run).ends_with("RuntimeError: the run reached its limit of 2 steps"), "{}", failed(&run));
}

#[tokio::test]
async fn calls_past_the_concurrency_limit_wait_for_a_free_slot() {
    let worker = worker();
    let mut run = run(
        "await parallel(*[agent(str(i)) for i in range(5)])\n",
        json!(null),
        Limits { concurrency: 2, ..Limits::default() },
    );
    let mut calls = run.start(&worker).await.unwrap().calls;
    assert_eq!(prompts(&calls), ["0", "1"]);
    assert_eq!(run.queued().len(), 3);
    let mut answered = Vec::new();
    while let Some(next) = calls.pop() {
        let progress = run.resolve(&worker, next.call, Answer::Ok(json!(next.args["prompt"]))).await.unwrap();
        answered.push(next.args["prompt"].as_str().unwrap().to_owned());
        calls.extend(progress.calls);
        assert!(run.outstanding().len() <= 2);
    }
    assert_eq!(answered, ["1", "2", "3", "4", "0"]);
    assert_eq!(*done(&run), json!(["0", "1", "2", "3", "4"]));
}

#[tokio::test]
async fn compute_dump_memory_and_recursion_limits_fail_the_run() {
    let worker = worker();
    let cases = [
        (
            "while True:\n    pass\n",
            Limits { compute_ms: 300, ..Limits::default() },
            "TimeoutError: turn time limit exceeded",
        ),
        ("x = await agent('a')\n", Limits { dump_bytes: 1024, ..Limits::default() }, "passes the limit of 1024 bytes"),
        ("def f(n):\n    return f(n + 1)\nf(0)\n", Limits::default(), "RecursionError"),
    ];
    for (script, limits, error) in cases {
        let mut run = run(script, json!(null), limits);
        run.start(&worker).await.unwrap();
        assert!(failed(&run).contains(error), "{script}: {}", failed(&run));
    }

    let script = "x = []\nfor i in range(1000):\n    x.append('a' * 100000)\n";
    let mut run = run(script, json!(null), Limits { memory_bytes: 8 << 20, ..Limits::default() });
    run.start(&worker).await.unwrap();
    assert!(failed(&run).starts_with("MemoryError: memory limit exceeded"), "{}", failed(&run));
}

#[tokio::test]
async fn the_clock_is_the_runs_and_the_environment_empty() {
    let script = "import datetime, os\ntry:\n    os.getenv('PATH')\nexcept RuntimeError:\n    r = 'refused'\n[datetime.datetime.now().isoformat(), r]\n";
    let worker = worker();
    let mut run = run(script, json!(null), Limits::default());
    run.start(&worker).await.unwrap();
    assert_eq!(*done(&run), json!(["2023-11-14T22:13:20", "refused"]));
}

#[tokio::test]
async fn a_script_without_its_literal_meta_is_refused() {
    let refused = Run::new("x = 1\n", None, json!(null), Limits::default());
    assert!(matches!(refused, Err(RunError::Trail(ants_trail::TrailError::Meta(_)))));
    let limits = Limits { concurrency: 17, ..Limits::default() };
    assert!(matches!(Run::new(&format!("{META}x = 1\n"), None, json!(null), limits), Err(RunError::Limits(_))));
}

/// A run of [`FAN_OUT`] over a and b, with a's first step answered and both b's steps answered.
async fn half_way(worker: &Worker) -> Run {
    let mut run = run(FAN_OUT, json!(["a", "b"]), Limits::default());
    let calls = run.start(worker).await.unwrap().calls;
    let progress = answer(&mut run, worker, &calls, "first b", json!("B")).await;
    answer(&mut run, worker, &progress.calls, "then B", json!("b done")).await;
    answer(&mut run, worker, &calls, "first a", json!("A")).await;
    run
}

#[tokio::test]
async fn a_replay_rebuilds_the_session_from_the_journal() {
    let worker = worker();
    let run = half_way(&worker).await;
    let then_a = run.outstanding()[0].clone();
    assert_eq!(then_a.args["prompt"], "then A");

    let (mut replayed, progress) = Run::replay(run.state().clone(), run.journal().to_vec(), &worker).await.unwrap();
    assert_eq!(progress, Progress::default());
    assert_eq!(replayed.outstanding(), std::slice::from_ref(&then_a));
    replayed.resolve(&worker, then_a.call, Answer::Ok(json!("a done"))).await.unwrap();
    assert_eq!(*done(&replayed), json!(["a done", "b done"]));
}

#[tokio::test]
async fn a_replay_names_the_call_that_diverges_from_the_journal() {
    let worker = worker();
    let run = half_way(&worker).await;
    let mut journal = run.journal().to_vec();
    journal[1].args = json!({"prompt": "then X"});
    let Err(RunError::Diverged(error)) = Run::replay(run.state().clone(), journal, &worker).await else {
        panic!("a changed call diverges");
    };
    let call = run.journal()[1].call;
    assert_eq!(
        error,
        format!(
            r#"call {call} is agent({{"prompt":"then B","label":"b"}}), the journal has agent({{"prompt":"then X"}})"#
        )
    );

    let mut journal = run.journal().to_vec();
    journal.push(JournalEntry { call: 99, function: Function::Human, args: json!({}), answer: Answer::Ok(json!(1)) });
    let Err(RunError::Diverged(error)) = Run::replay(run.state().clone(), journal, &worker).await else {
        panic!("a call never made diverges");
    };
    assert_eq!(error, "the script no longer makes call 99 human({})");
}

const WAYPOINTS: &str = r#"meta = {"name": "fix", "description": "", "phases": [], "waypoints": ["triage", "patch"]}

async def triage(state):
    return await agent(f"triage {state}")

async def patch(state):
    plan = await agent(f"plan {state['bug']}")
    return await agent(f"patch {plan}")
"#;

fn entered(entry: Option<&str>, state: Value) -> Run {
    Run::with_clock(WAYPOINTS, entry, state, Limits::default(), 1_700_000_000, 7).unwrap()
}

#[tokio::test]
async fn a_trail_with_waypoints_starts_at_its_first() {
    let worker = worker();
    let mut run = entered(None, json!("a crash"));
    assert_eq!(run.state().entry(), Some("triage"));
    let calls = run.start(&worker).await.unwrap().calls;
    assert_eq!(prompts(&calls), ["triage a crash"]);
    run.resolve(&worker, calls[0].call, Answer::Ok(json!("a bug"))).await.unwrap();
    assert_eq!(*done(&run), json!("a bug"));
}

#[tokio::test]
async fn a_run_entered_at_a_waypoint_replays_from_its_journal() {
    let worker = worker();
    let mut run = entered(Some("patch"), json!({"bug": 7}));
    let calls = run.start(&worker).await.unwrap().calls;
    assert_eq!(prompts(&calls), ["plan 7"]);
    let then = answer(&mut run, &worker, &calls, "plan 7", json!("swap")).await.calls;
    assert_eq!(prompts(&then), ["patch swap"]);

    let stored: RunState = serde_json::from_str(&serde_json::to_string(run.state()).unwrap()).unwrap();
    let (mut replayed, progress) = Run::replay(stored, run.journal().to_vec(), &worker).await.unwrap();
    assert_eq!(progress, Progress::default());
    assert_eq!(replayed.outstanding(), then.as_slice());
    replayed.resolve(&worker, then[0].call, Answer::Ok(json!("patched"))).await.unwrap();
    assert_eq!(*done(&replayed), json!("patched"));
}

#[test]
fn a_run_entering_its_trail_at_no_waypoint_of_it_is_refused() {
    let refused = Run::new(WAYPOINTS, Some("review"), json!(null), Limits::default());
    assert!(matches!(&refused, Err(RunError::Entry(why)) if why.contains("no waypoint \"review\"")), "{refused:?}");
    let refused = Run::new(&format!("{META}x = 1\n"), Some("x"), json!(null), Limits::default());
    assert!(matches!(&refused, Err(RunError::Entry(why)) if why.contains("no waypoints")), "{refused:?}");
}
