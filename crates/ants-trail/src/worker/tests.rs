use serde_json::{Value, json};

use super::serve;
use crate::protocol::{Answer, Call, Event, Function, Limit, Limits, Request, Response};

const META: &str = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": []}\n";

fn start(script: &str, limits: Limits) -> Response {
    let request = Request::Start {
        script: format!("{META}{script}"),
        entry: None,
        args: json!({"items": ["a", "b", "c"]}),
        limits,
        clock: 1_700_000_000,
        seed: 7,
    };
    serve(&serde_json::to_string(&request).unwrap())
}

/// Starts `script`, a whole trail, at the waypoint `entry` with `state`.
fn enter(script: &str, entry: &str, state: Value) -> Response {
    let request = Request::Start {
        script: script.to_owned(),
        entry: Some(entry.to_owned()),
        args: state,
        limits: Limits::default(),
        clock: 1_700_000_000,
        seed: 7,
    };
    serve(&serde_json::to_string(&request).unwrap())
}

fn resume_with(response: &Response, answers: &[(u32, Answer)], limits: Limits, steps: u32) -> Response {
    let Response::Suspended { snapshot, .. } = response else { panic!("not suspended: {response:?}") };
    let request = Request::Resume { snapshot: snapshot.clone(), answers: answers.to_vec(), limits, seed: 7, steps };
    serve(&serde_json::to_string(&request).unwrap())
}

fn resume(response: &Response, answers: &[(u32, Answer)], steps: u32) -> Response {
    resume_with(response, answers, Limits::default(), steps)
}

fn calls(response: &Response) -> Vec<Call> {
    match response {
        Response::Suspended { calls, .. } => calls.clone(),
        other => panic!("not suspended: {other:?}"),
    }
}

fn result(response: Response) -> Value {
    match response {
        Response::Done { result, .. } => result,
        other => panic!("not done: {other:?}"),
    }
}

fn error(response: Response) -> String {
    failed(response).0
}

/// The error of a failed response, and the limit it failed on.
fn failed(response: Response) -> (String, Option<Limit>) {
    match response {
        Response::Failed { error, limit, .. } => (error, limit),
        other => panic!("not failed: {other:?}"),
    }
}

const FAN_OUT: &str = r#"
async def item(name):
    first = await agent(f"first {name}")
    return await agent(f"then {first}", label=name)

await parallel(*[item(name) for name in args["items"]])
"#;

#[test]
fn call_ids_do_not_depend_on_how_answers_are_batched() {
    let started = start(FAN_OUT, Limits::default());
    let first = calls(&started);
    assert_eq!(first.len(), 3);
    assert_eq!(first[0].function, Function::Agent);
    assert_eq!(first[0].args, json!({"prompt": "first a"}));

    let answers = [(first[2].call, Answer::Ok(json!("C"))), (first[0].call, Answer::Ok(json!("A")))];
    let batched = resume(&started, &answers, 3);
    let one = resume(&started, &answers[..1], 3);
    let two = resume(&one, &answers[1..], 4);
    let mut separate = calls(&one);
    separate.extend(calls(&two));
    assert_eq!(calls(&batched), separate);
    assert_eq!(separate[0].args, json!({"prompt": "then C", "label": "c"}));
    let Response::Suspended { pending, .. } = &batched else { unreachable!() };
    assert_eq!(pending.len(), 3);
}

#[test]
fn phase_log_and_print_are_events_in_order() {
    let script = "phase('scan')\nprint('a', 1)\nlog('b')\nprint('partial', end='')\n'end'\n";
    let Response::Done { result, events, .. } = start(script, Limits::default()) else { panic!() };
    assert_eq!(result, json!("end"));
    let logs = ["a 1", "b", "partial"].map(|s| Event::Log(s.to_owned()));
    assert_eq!(events[0], Event::Phase("scan".to_owned()));
    assert_eq!(events[1..], logs);
}

#[test]
fn the_clock_and_random_are_fixed_by_the_request() {
    let script = "import datetime, random, os\n[datetime.datetime.now().year, random.random(), os.urandom(4).hex()]\n";
    let first = result(start(script, Limits::default()));
    assert_eq!(first, result(start(script, Limits::default())));
    assert_eq!(first[0], json!(2023));
}

#[test]
fn other_os_calls_raise_in_the_script() {
    for (call, raised) in [
        ("open('/etc/hosts').read()", "PermissionError"),
        ("import os; os.getenv('HOME')", "RuntimeError"),
        ("import time; time.sleep(1)", "RuntimeError"),
        ("import socket", "ModuleNotFoundError"),
    ] {
        let script = format!("try:\n    {call}\n    r = 'ran'\nexcept Exception as e:\n    r = type(e).__name__\nr\n");
        assert_eq!(result(start(&script, Limits::default())), json!(raised), "{call}");
    }
}

#[test]
fn an_error_answer_raises_where_the_call_is_awaited() {
    let script = "try:\n    await agent('x')\nexcept RuntimeError as e:\n    r = str(e)\nr\n";
    let started = start(script, Limits::default());
    let call = calls(&started)[0].call;
    assert_eq!(result(resume(&started, &[(call, Answer::Error("no ant".to_owned()))], 1)), json!("no ant"));
}

#[test]
fn an_uncaught_error_fails_with_the_traceback_of_the_script() {
    let (error, limit) = failed(start("def f():\n    raise ValueError('bad')\nf()\n", Limits::default()));
    assert_eq!(limit, None, "no limit broken");
    assert!(error.contains("File \"trail.py\", line 4, in <module>"), "{error}");
    assert!(error.contains("File \"trail.py\", line 3, in f"), "{error}");
    assert!(error.ends_with("ValueError: bad"), "{error}");
}

#[test]
fn the_call_past_the_step_limit_raises() {
    let script = "for i in range(5):\n    try:\n        await agent(str(i))\n    except RuntimeError as e:\n        r = str(e)\n        break\nr\n";
    let limits = Limits { steps: 1, ..Limits::default() };
    let started = start(script, limits);
    let call = calls(&started)[0].call;
    let done = resume_with(&started, &[(call, Answer::Ok(json!(1)))], limits, 1);
    assert_eq!(result(done), json!("the run reached its limit of 1 steps"));

    let started = start("for i in range(5):\n    await agent(str(i))\n", limits);
    let call = calls(&started)[0].call;
    let (error, limit) = failed(resume_with(&started, &[(call, Answer::Ok(json!(1)))], limits, 1));
    assert!(error.ends_with("RuntimeError: the run reached its limit of 1 steps"), "{error}");
    assert_eq!(limit, Some(Limit::Steps), "the run failed on its steps");
}

#[test]
fn a_trail_call_is_a_step_counted_against_the_limit() {
    let script = "first = await trail('review', args={'pr': 7})\ntry:\n    await trail('lint')\nexcept RuntimeError as e:\n    r = str(e)\n[first, r]\n";
    let limits = Limits { steps: 1, ..Limits::default() };
    let started = start(script, limits);
    let first = calls(&started);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].function, Function::Trail);
    assert_eq!(first[0].args, json!({"name": "review", "args": {"pr": 7}}));
    let done = resume_with(&started, &[(first[0].call, Answer::Ok(json!("ok")))], limits, 1);
    assert_eq!(result(done), json!(["ok", "the run reached its limit of 1 steps"]));

    let started = start("await trail('lint')\n", Limits::default());
    assert_eq!(calls(&started)[0].args, json!({"name": "lint"}));
}

#[test]
fn a_goto_is_a_call_past_the_step_limit_and_its_state_is_bounded() {
    let script = "a = await agent('a')\ntry:\n    await goto('fix', state=a)\nexcept RuntimeError as e:\n    r = [str(e)]\ntry:\n    await goto(state='x' * 70000)\nexcept RuntimeError as e:\n    r.append(str(e))\nr\n";
    let limits = Limits { steps: 1, ..Limits::default() };
    let started = start(script, limits);
    let call = calls(&started)[0].call;
    let moved = resume_with(&started, &[(call, Answer::Ok(json!("A")))], limits, 1);
    let goto = calls(&moved);
    assert_eq!((goto[0].function, &goto[0].args), (Function::Goto, &json!({"trail": "fix", "state": "A"})));
    let done = resume_with(&moved, &[(goto[0].call, Answer::Error("no trail fix".to_owned()))], limits, 1);
    let oversized = "goto()'s state has 70002 bytes of JSON; a leg takes at most 65536";
    assert_eq!(result(done), json!(["no trail fix", oversized]));
}

#[test]
fn a_step_argument_json_cannot_hold_raises_type_error() {
    let error = error(start("await agent({1, 2})\n", Limits::default()));
    assert!(error.ends_with("TypeError: agent() takes JSON values: a set is not a JSON value"), "{error}");
}

#[test]
fn a_session_dumped_past_its_limit_fails() {
    let limits = Limits { dump_bytes: 1024, ..Limits::default() };
    let (error, limit) = failed(start("await agent('x')\n", limits));
    assert_eq!(limit, Some(Limit::Session));
    assert!(
        error.starts_with("the dumped session of ") && error.ends_with("passes the limit of 1024 bytes"),
        "{error}"
    );
}

#[test]
fn bad_requests_are_refused_and_foreign_snapshots_unrestorable() {
    assert!(matches!(serve("{"), Response::Refused { .. }));
    let started = start("await agent('x')\n", Limits::default());
    assert!(matches!(started, Response::Suspended { .. }));
    let limits = Limits::default();
    let request = json!({"resume": {"snapshot": {"nope": 1}, "answers": [], "limits": limits, "seed": 7, "steps": 0}});
    let response = serve(&request.to_string());
    assert!(matches!(&response, Response::Unrestorable { error } if error.starts_with("unreadable snapshot")));
}

const WAYPOINTS: &str = r#"meta = {"name": "fix", "description": "", "phases": [], "waypoints": ["triage", "patch"]}
PREFIX = "patch"

async def triage(state):
    return await agent("triage")

async def patch(state):
    found = await agent(f"{PREFIX} {state['bug']}")
    if found == "raise":
        raise ValueError(found)
    return [state, found]
"#;

#[test]
fn a_start_at_a_waypoint_runs_its_top_level_then_awaits_only_it_with_the_state() {
    let started = enter(WAYPOINTS, "patch", json!({"bug": 7}));
    let first = calls(&started);
    assert_eq!(first.iter().map(|c| &c.args).collect::<Vec<_>>(), [&json!({"prompt": "patch 7"})]);
    let done = resume(&started, &[(first[0].call, Answer::Ok(json!("fixed")))], 1);
    assert_eq!(result(done), json!([{"bug": 7}, "fixed"]));
    let raised = resume(&started, &[(first[0].call, Answer::Ok(json!("raise")))], 1);
    let error = error(raised);
    assert!(error.contains("File \"trail.py\", line 10, in patch"), "{error}");
}

#[test]
fn an_entry_the_meta_does_not_list_is_refused() {
    let plain = format!("{META}await agent('x')\n");
    for (script, entry) in [(WAYPOINTS, "review"), (plain.as_str(), "x")] {
        let response = enter(script, entry, json!(null));
        assert!(
            matches!(&response, Response::Refused { error } if error.contains(&format!("no waypoint {entry:?}"))),
            "{response:?}"
        );
    }
}
