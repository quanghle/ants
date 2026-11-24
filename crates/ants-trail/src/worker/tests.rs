//! Tests of the `ants subprocess` worker: call ids and labels, events, fixed clock and random, errors and tracebacks,
//! resumes under their own limits, the caps on output, notes, calls and steps, and bad requests.

use ants_proto::{CallAnswer, MAX_STEP_LABEL, RunLimit, TrailFunction};
use serde_json::{Value, json};

use super::{CALL_BYTES, MAX_ERROR, MAX_NOTE, MAX_RESULT, NOTE_BYTES, serve};
use crate::protocol::{Call, Event, Limits, Request, Response};

const META: &str = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": []}\n";

fn start(script: &str, limits: Limits) -> Response {
    start_as(script, limits, false)
}

/// Starts `script`, as a watcher's with `watcher`.
fn start_as(script: &str, limits: Limits, watcher: bool) -> Response {
    let request = Request::Start {
        script: format!("{META}{script}"),
        entry: None,
        args: json!({"items": ["a", "b", "c"]}),
        limits,
        clock: 1_700_000_000,
        seed: 7,
        watcher,
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
        watcher: false,
    };
    serve(&serde_json::to_string(&request).unwrap())
}

fn resume_with(response: &Response, answers: &[(u32, CallAnswer)], limits: Limits, steps: u32) -> Response {
    let Response::Suspended { snapshot, .. } = response else { panic!("not suspended: {response:?}") };
    let (snapshot, answers) = (snapshot.clone(), answers.to_vec());
    let watcher = calls(response).iter().any(|call| call.function.watches());
    let request = Request::Resume { snapshot, answers, limits, seed: 7, steps, watcher };
    serve(&serde_json::to_string(&request).unwrap())
}

fn resume(response: &Response, answers: &[(u32, CallAnswer)], steps: u32) -> Response {
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
fn failed(response: Response) -> (String, Option<RunLimit>) {
    match response {
        Response::Failed { error, limit, .. } => (error, limit),
        other => panic!("not failed: {other:?}"),
    }
}

const FAN_OUT: &str = r#"
async def item(name):
    first = await agent(f"first {name}")
    return await agent(f"then {first}", label=name, tips="then")

await parallel(*[item(name) for name in args["items"]])
"#;

#[test]
fn call_ids_do_not_depend_on_how_answers_are_batched() {
    let started = start(FAN_OUT, Limits::default());
    let first = calls(&started);
    assert_eq!(first.len(), 3);
    assert_eq!(first[0].function, TrailFunction::Agent);
    assert_eq!(first[0].args, json!({"prompt": "first a"}));

    let answers = [(first[2].call, CallAnswer::Ok(json!("C"))), (first[0].call, CallAnswer::Ok(json!("A")))];
    let batched = resume(&started, &answers, 3);
    let one = resume(&started, &answers[..1], 3);
    let two = resume(&one, &answers[1..], 4);
    let mut separate = calls(&one);
    separate.extend(calls(&two));
    assert_eq!(calls(&batched), separate);
    assert_eq!(separate[0].args, json!({"prompt": "then C", "label": "c", "tips": "then"}));
    let Response::Suspended { pending, .. } = &batched else { unreachable!() };
    assert_eq!(pending.len(), 3);
}

#[test]
fn an_agent_call_s_label_is_one_line_without_control_characters() {
    let script = "await parallel(agent('x', label='fix\\nstate: done\\x1b[8m'), agent('y', label='\\x07 \\n'))\n";
    let made = calls(&start(script, Limits::default()));
    assert_eq!(made[0].args, json!({"prompt": "x", "label": "fix state: done[8m"}));
    assert_eq!(made[1].args, json!({"prompt": "y"}));
    let long = format!("await agent('x', label='{}')\n", "x".repeat(MAX_STEP_LABEL + 1));
    let label = calls(&start(&long, Limits::default()))[0].args["label"].as_str().unwrap().to_owned();
    assert_eq!(label.chars().count(), MAX_STEP_LABEL + 1);
    assert!(label.ends_with('…'));
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
    assert_eq!(result(resume(&started, &[(call, CallAnswer::Error("no ant".to_owned()))], 1)), json!("no ant"));
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
fn a_timeout_or_memory_error_of_the_script_breaks_no_limit() {
    let (error, limit) = failed(start("raise TimeoutError('the review did not converge')\n", Limits::default()));
    assert!(error.ends_with("TimeoutError: the review did not converge"), "{error}");
    assert_eq!(limit, None);
    let (error, limit) = failed(start("raise MemoryError('too many files')\n", Limits::default()));
    assert!(error.ends_with("MemoryError: too many files"), "{error}");
    assert_eq!(limit, None);
    // Worded as Monty's own, raised at the top or in a function.
    let monty = "MemoryError('memory limit exceeded: 67108865 bytes > 67108864 bytes')";
    for script in [format!("raise {monty}\n"), format!("def f():\n    raise {monty}\nf()\n")] {
        let (error, limit) = failed(start(&script, Limits::default()));
        assert!(error.ends_with("MemoryError: memory limit exceeded: 67108865 bytes > 67108864 bytes"), "{error}");
        assert_eq!(limit, None, "{script}");
    }

    let limits = Limits { compute_ms: 200, ..Limits::default() };
    let (error, limit) = failed(start("while True:\n    pass\n", limits));
    assert!(error.contains("TimeoutError"), "{error}");
    assert_eq!(limit, Some(RunLimit::Compute));
    let limits = Limits { memory_bytes: 1024 * 1024, ..Limits::default() };
    let (error, limit) = failed(start("x = 'a' * (8 * 1024 * 1024)\n", limits));
    assert!(error.contains("MemoryError"), "{error}");
    assert_eq!(limit, Some(RunLimit::Memory));
}

#[test]
fn a_resume_computes_and_allocates_under_its_own_limits_not_those_of_the_dumped_session() {
    let script = "n = await agent('x')\nwhile n > 0:\n    n -= 1\nbig = 'a' * (8 * 1024 * 1024)\nlen(big)\n";
    let tight = Limits { compute_ms: 100, memory_bytes: 1024 * 1024, ..Limits::default() };
    let loose = Limits { compute_ms: Limits::MAX_COMPUTE_MS, ..Limits::default() };
    for started_under in [tight, loose] {
        let started = start(script, started_under);
        let call = calls(&started)[0].call;
        let answer = [(call, CallAnswer::Ok(json!(3_000_000)))];
        let (error, limit) = failed(resume_with(&started, &answer, tight, 1));
        assert_eq!(limit, Some(RunLimit::Compute), "{error}");
        let (error, limit) =
            failed(resume_with(&started, &answer, Limits { compute_ms: loose.compute_ms, ..tight }, 1));
        assert_eq!(limit, Some(RunLimit::Memory), "{error}");
        assert_eq!(result(resume_with(&started, &answer, loose, 1)), 8 * 1024 * 1024);
    }
}

#[test]
fn print_past_its_cap_fails_naming_it_and_breaks_no_limit() {
    let cap = format!("print() passes the {NOTE_BYTES} bytes print(), log() and phase() may write in one resume");
    for script in
        ["for i in range(100):\n    print('x' * 1000)\n", "for i in range(100):\n    print('x' * 1000)\n    log('.')\n"]
    {
        let (error, limit) = failed(start(script, Limits::default()));
        assert!(error.ends_with(&format!("RuntimeError: {cap}")), "{error}");
        assert_eq!(limit, None);
    }
}

#[test]
fn notes_are_cut_and_bounded_per_resume() {
    let Response::Done { events, .. } =
        start("log('x' * 20000)\nphase('y' * 5000)\nprint('z' * 6000)\n", Limits::default())
    else {
        panic!("not done");
    };
    let cut = |c: &str| format!("{}…", c.repeat(MAX_NOTE - '…'.len_utf8()));
    assert_eq!(events, [Event::Log(cut("x")), Event::Phase(cut("y")), Event::Log(cut("z"))]);

    let (error, limit) = failed(start("for i in range(10000):\n    log('a')\n", Limits::default()));
    assert!(
        error.ends_with(&format!(
            "RuntimeError: log() passes the {NOTE_BYTES} bytes print(), log() and phase() may write in one resume"
        )),
        "{error}"
    );
    assert_eq!(limit, None);

    // Each resume may write as much again.
    let script =
        "for i in range(12):\n    log('a' * 3000)\nawait agent('x')\nfor i in range(12):\n    log('b' * 3000)\n'end'\n";
    let started = start(script, Limits::default());
    let call = calls(&started)[0].call;
    let Response::Done { events, .. } = resume(&started, &[(call, CallAnswer::Ok(json!(1)))], 1) else {
        panic!("not done")
    };
    assert_eq!(events.len(), 12);
}

#[test]
fn the_calls_of_one_resume_are_bounded() {
    let script = "await parallel(*[agent('x' * 400000) for i in range(3)])\n";
    let Response::Failed { error, calls, limit, .. } = start(script, Limits::default()) else { panic!("not failed") };
    assert_eq!(calls.len(), 2, "the third passes the bound");
    let past = format!(
        "agent()'s arguments of 400013 bytes of JSON pass the {CALL_BYTES} bytes the calls of one resume may have"
    );
    assert!(error.ends_with(&format!("RuntimeError: {past}")), "{error}");
    assert_eq!(limit, None);
}

#[test]
fn a_result_too_large_to_store_fails_and_a_long_error_is_cut() {
    let (error, limit) = failed(start("'x' * 800000\n", Limits::default()));
    assert_eq!(error, format!("the trail's result has 800002 bytes of JSON; a result holds at most {MAX_RESULT}"));
    assert_eq!(limit, None);

    let (long, _) = failed(start("raise ValueError('x' * 100000)\n", Limits::default()));
    assert_eq!(long.len(), MAX_ERROR);
    assert!(long.ends_with("xx…"), "{long}");
}

#[test]
fn the_call_past_the_step_limit_raises() {
    let script = "for i in range(5):\n    try:\n        await agent(str(i))\n    except RuntimeError as e:\n        r = str(e)\n        break\nr\n";
    let limits = Limits { steps: 1, ..Limits::default() };
    let started = start(script, limits);
    let call = calls(&started)[0].call;
    let done = resume_with(&started, &[(call, CallAnswer::Ok(json!(1)))], limits, 1);
    assert_eq!(result(done), json!("the run reached its limit of 1 steps"));

    let started = start("for i in range(5):\n    await agent(str(i))\n", limits);
    let call = calls(&started)[0].call;
    let (error, limit) = failed(resume_with(&started, &[(call, CallAnswer::Ok(json!(1)))], limits, 1));
    assert!(error.ends_with("RuntimeError: the run reached its limit of 1 steps"), "{error}");
    assert_eq!(limit, Some(RunLimit::Steps), "the run failed on its steps");
}

#[test]
fn a_trail_call_is_a_step_counted_against_the_limit() {
    let script = "first = await trail('review', args={'pr': 7})\ntry:\n    await trail('lint')\nexcept RuntimeError as e:\n    r = str(e)\n[first, r]\n";
    let limits = Limits { steps: 1, ..Limits::default() };
    let started = start(script, limits);
    let first = calls(&started);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].function, TrailFunction::Trail);
    assert_eq!(first[0].args, json!({"name": "review", "args": {"pr": 7}}));
    let done = resume_with(&started, &[(first[0].call, CallAnswer::Ok(json!("ok")))], limits, 1);
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
    let moved = resume_with(&started, &[(call, CallAnswer::Ok(json!("A")))], limits, 1);
    let goto = calls(&moved);
    assert_eq!((goto[0].function, &goto[0].args), (TrailFunction::Goto, &json!({"trail": "fix", "state": "A"})));
    let done = resume_with(&moved, &[(goto[0].call, CallAnswer::Error("no trail fix".to_owned()))], limits, 1);
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
    assert_eq!(limit, Some(RunLimit::Session));
    assert!(
        error.starts_with("the dumped session of ") && error.ends_with("passes the limit of 1024 bytes"),
        "{error}"
    );
}

#[test]
fn bad_requests_are_refused_and_foreign_snapshots_unrestorable() {
    assert!(matches!(serve("{"), Response::Refused { error } if error.starts_with("unreadable request")));
    let started = start("await agent('x')\n", Limits::default());
    let Response::Suspended { snapshot, .. } = &started else { panic!("not suspended: {started:?}") };
    let unbounded = Limits { concurrency: 0, ..Limits::default() };
    let refused = start("await agent('x')\n", unbounded);
    assert!(matches!(&refused, Response::Refused { error } if error.contains("concurrency")), "{refused:?}");
    let refused = resume_with(&started, &[], unbounded, 0);
    assert!(matches!(&refused, Response::Refused { error } if error.contains("concurrency")), "{refused:?}");

    let resume = |snapshot: Value| {
        let limits = Limits::default();
        let request = json!({"resume": {"snapshot": snapshot, "answers": [], "limits": limits, "seed": 7, "steps": 1}});
        serve(&request.to_string())
    };
    let own: Value = serde_json::from_str(snapshot.get()).unwrap();
    assert!(matches!(resume(own.clone()), Response::Suspended { .. }), "the worker's own snapshot is restored");
    let mut other = own;
    let suspended = other.as_object_mut().and_then(|o| o.values_mut().next()).and_then(Value::as_object_mut).unwrap();
    let field = suspended.keys().next().unwrap().clone();
    suspended.remove(&field);
    let ended = serde_json::to_value(monty::RunProgress::Complete(monty_types::MontyObject::none())).unwrap();
    // A snapshot of no Monty session, one of another Monty version's form, lacking a field of this one's, and the form
    // of a session ended rather than suspended.
    for (foreign, why) in [
        (json!({"nope": 1}), "unreadable snapshot"),
        (other, "unreadable snapshot"),
        (ended, "the snapshot is not a suspended session"),
    ] {
        let response = resume(foreign.clone());
        assert!(
            matches!(&response, Response::Unrestorable { error } if error.starts_with(why)),
            "{foreign}: {response:?}"
        );
    }
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
    let done = resume(&started, &[(first[0].call, CallAnswer::Ok(json!("fixed")))], 1);
    assert_eq!(result(done), json!([{"bug": 7}, "fixed"]));
    let raised = resume(&started, &[(first[0].call, CallAnswer::Ok(json!("raise")))], 1);
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

#[test]
fn a_watchers_reads_suspend_it_and_their_answers_resume_it() {
    let script = r#"
async def main():
    ran = await command("pr-blocked 7")
    text = await read_file("STATUS")
    issue = await bd("show", "ants-1")
    kept = await thread()
    return {"output": ran["stdout"] + text + issue["status"] + kept["register"], "done": ran["status"] == 3}

await main()
"#;
    let first = start_as(script, Limits::default(), true);
    let made = calls(&first);
    assert_eq!(made.len(), 1);
    assert_eq!((made[0].function, &made[0].args), (TrailFunction::Command, &json!({"command": "pr-blocked 7"})));
    let answer = |value| [(made[0].call, CallAnswer::Ok(value))];
    let second = resume(&first, &answer(json!({"status": 3, "stdout": "ready\n"})), 0);
    let made = calls(&second);
    assert_eq!((made[0].function, &made[0].args), (TrailFunction::ReadFile, &json!({"path": "STATUS"})));
    let third = resume(&second, &[(made[0].call, CallAnswer::Ok(json!("green ")))], 0);
    let made = calls(&third);
    assert_eq!((made[0].function, &made[0].args), (TrailFunction::Bd, &json!({"args": ["show", "ants-1"]})));
    let fourth = resume(&third, &[(made[0].call, CallAnswer::Ok(json!({"status": "open"})))], 0);
    let made = calls(&fourth);
    assert_eq!((made[0].function, &made[0].args), (TrailFunction::Thread, &json!({})));
    let done = resume(&fourth, &[(made[0].call, CallAnswer::Ok(json!({"register": " goal"})))], 0);
    assert_eq!(result(done), json!({"output": "ready\ngreen open goal", "done": true}));
}

#[test]
fn a_watcher_prompts_no_ant_and_writes_no_note_and_a_run_reads_nothing() {
    let refused = |script: &str, watcher: bool| match start_as(script, Limits::default(), watcher) {
        Response::Failed { error, .. } => error,
        other => panic!("not refused: {other:?}"),
    };
    for (call, why) in [
        ("await agent('look')", "agent() would prompt an ant or start a run, which a watcher may not"),
        ("await human('ok?')", "human() would prompt an ant or start a run"),
        ("await trail('other')", "trail() would prompt an ant or start a run"),
        ("await goto(at='b')", "goto() would prompt an ant or start a run"),
        ("phase('one')", "phase() writes a note of the thread, which a watcher may not"),
        ("log('two')", "log() writes a note of the thread"),
    ] {
        let error = refused(call, true);
        assert!(error.contains(why), "{call}: {error}");
    }
    for call in ["await command('true')", "await read_file('a')", "await bd('ready')", "await thread()"] {
        let error = refused(call, false);
        assert!(
            error.contains("is a watcher's call: only a watcher's script reads the ant's state"),
            "{call}: {error}"
        );
    }
}
