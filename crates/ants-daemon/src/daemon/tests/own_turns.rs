//! The turns the ant's harness runs of its own between prompts, such as Claude Code's once a background command it
//! started ends: prompted on their thread with no outside message, the prompt taking the harness's turn, up to a cap
//! an hour.

use std::path::Path;

use ants_test_support::StubCli;
use serde_json::{Value, json};

use super::*;
use crate::harness::{Harnesses, Launch, ModelSettings, parse_definitions};

/// A line of Claude Code's stream-json on the main thread of session s.
fn event(mut event: Value) -> String {
    event["parent_tool_use_id"] = Value::Null;
    event["session_id"] = json!("s");
    event.to_string()
}

fn state(state: &str) -> String {
    event(json!({ "type": "system", "subtype": "session_state_changed", "state": state }))
}

fn init() -> String {
    event(json!({ "type": "system", "subtype": "init", "model": "claude-haiku-5-5", "mcp_servers": [] }))
}

fn say(id: &str, text: &str) -> String {
    let message = json!({ "id": id, "model": "claude-haiku-5-5", "role": "assistant",
        "content": [{ "type": "text", "text": text }] });
    event(json!({ "type": "assistant", "message": message }))
}

fn result(total: f64) -> String {
    let usage = json!({ "input_tokens": 4, "output_tokens": 20 });
    event(json!({ "type": "result", "subtype": "success", "is_error": false, "result": "", "stop_reason": "end_turn",
        "total_cost_usd": total, "usage": usage }))
}

/// The lines of a stub Claude Code 2.1.332 as it was seen to run a background command: the prompt's turn starts one,
/// running until file `done` in `dir` exists, and answers `Started the checks.`; once the command ended, Claude Code
/// tells so and runs a turn of its own answering `The checks passed.`.
fn backgrounding(dir: &Path) -> Vec<Vec<String>> {
    let (done, ended) = (dir.join("done"), dir.join("ended"));
    let (done, ended) = (done.display(), ended.display());
    let signed_in = json!({ "type": "control_response", "response": { "subtype": "success", "request_id": "ants-1",
        "response": { "account": { "email": "a@example.com", "apiProvider": "firstParty" } } } });
    let bash = json!({ "type": "tool_use", "id": "toolu_1", "name": "Bash",
        "input": { "command": "scripts/check.sh", "run_in_background": true } });
    let started = json!({ "type": "assistant", "message": { "id": "m1", "role": "assistant", "content": [bash] } });
    let running = json!([{ "type": "tool_result", "tool_use_id": "toolu_1", "is_error": false,
        "content": "Command running in background with ID: b1. You will be notified when it completes." }]);
    let notice = json!({ "type": "system", "subtype": "task_notification", "task_id": "b1", "tool_use_id": "toolu_1",
        "status": "completed", "output_file": "/tmp/b1.output",
        "summary": "Background command \"Run the checks\" completed (exit code 0)" });
    let turn = vec![
        state("running"),
        init(),
        event(started),
        format!("!bg while [ ! -f '{done}' ]; do sleep 0.05; done; : > '{ended}'"),
        event(json!({ "type": "user", "message": { "role": "user", "content": running } })),
        say("m2", "Started the checks."),
        result(0.1),
        state("idle"),
        format!("!sh while [ ! -f '{ended}' ]; do sleep 0.05; done"),
        event(notice),
        state("running"),
        init(),
        say("m3", "The checks passed."),
        result(0.2),
        state("idle"),
    ];
    vec![vec!["2.1.332 (Claude Code)".to_owned()], vec![signed_in.to_string()], turn]
}

/// The lines of a stub Claude Code signed in, whose first prompt's turn answers `Started.`, after which it runs
/// `turns` turns of its own, turn N once file `go<N>` in `dir` exists, answering `Own turn N.`; a second prompt's turn
/// answers `Done.`.
fn looping(dir: &Path, turns: usize) -> Vec<Vec<String>> {
    let signed_in = json!({ "type": "control_response", "response": { "subtype": "success", "request_id": "ants-1",
        "response": { "account": { "email": "a@example.com", "apiProvider": "firstParty" } } } });
    let answer = |id: &str, text: &str| vec![state("running"), init(), say(id, text), result(0.1), state("idle")];
    let mut first = answer("m0", "Started.");
    for n in 1..=turns {
        let go = dir.join(format!("go{n}"));
        first.push(format!("!sh while [ ! -f '{}' ]; do sleep 0.05; done", go.display()));
        first.extend(answer(&format!("m{n}"), &format!("Own turn {n}.")));
    }
    let version = vec!["2.1.332 (Claude Code)".to_owned()];
    vec![version, vec![signed_in.to_string()], first, answer("m-last", "Done.")]
}

/// A pool of dev-a's sessions run by `cli` as a claude harness in `dir`, the turns they run of their own going to
/// `bus`.
fn pool(bus: &Bus, cli: &StubCli, dir: &Path) -> Pool {
    let table = format!("[harnesses.claude-native]\nprotocol = \"claude\"\ncommand = {:?}", cli.path());
    let harness = Harnesses::builtin().define(parse_definitions(&table).unwrap()).unwrap().get("claude-native");
    let config = dir.display().to_string();
    let env = [("CLAUDE_CONFIG_DIR", config.as_str()), ("ANTHROPIC_API_KEY", ""), ("CLAUDE_CODE_OAUTH_TOKEN", "")];
    let env = env.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect();
    let launch =
        Launch::new(harness.unwrap(), dir.to_owned(), env, PermissionPolicy::allow_all(), ModelSettings::default())
            .with_own_turns(bus.own_turns_sink());
    Pool::new(launch, SessionStore::memory(), 2, IDLE)
}

#[tokio::test]
async fn an_ant_whose_turn_left_a_command_running_is_prompted_on_its_thread_once_the_harness_tells_it_ended() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let dir = ants_test_support::scratch_dir("own-turns").canonicalize().unwrap();
    let cli = StubCli::new(&dir, "claude");
    cli.script(None, &backgrounding(&dir));
    let pool = pool(&bus, &cli, &dir);
    let mut to_itself = bus.client.subscribe(subject::inbox(&bus.dev, &bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send(&t1, "Run the checks in the background.").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let started = next_envelope(&mut replies).await;
        assert_eq!((&started.thread_id, started.text.as_str()), (&t1, "Started the checks."));
        assert!(timeout(Duration::from_millis(300), to_itself.next()).await.is_err(), "no turn of the CLI's own yet");
        std::fs::write(dir.join("done"), "").unwrap();

        let woken = next_envelope(&mut to_itself).await;
        let why =
            "[ants] Background command \"Run the checks\" completed (exit code 0); its output is in /tmp/b1.output.";
        assert_eq!((&woken.thread_id, woken.text.as_str(), woken.queue), (&t1, why, true));
        let passed = next_envelope(&mut replies).await;
        assert_eq!((&passed.thread_id, passed.text.as_str()), (&t1, "The checks passed."));
        assert_eq!(passed.in_reply_to, Some(woken.id), "the answer to the message saying why");
        assert_eq!(cli.received(2).len(), 2, "nothing written for the CLI's own turn: the initialize and the prompt");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn the_turns_a_harness_runs_of_its_own_prompt_the_ant_only_up_to_the_cap_an_hour_the_rest_dropped() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.own_turns = super::super::own_turn::OwnTurns::capped(2);
    let dir = ants_test_support::scratch_dir("own-turns-capped").canonicalize().unwrap();
    let cli = StubCli::new(&dir, "claude");
    cli.script(None, &looping(&dir, 4));
    let pool = pool(&bus, &cli, &dir);
    let mut to_itself = bus.client.subscribe(subject::inbox(&bus.dev, &bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send(&t1, "Watch the build.").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let started = next_envelope(&mut replies).await;
        assert_eq!((&started.thread_id, started.text.as_str()), (&t1, "Started."));
        for n in 1..=2 {
            std::fs::write(dir.join(format!("go{n}")), "").unwrap();
            let woken = next_envelope(&mut to_itself).await;
            assert_eq!((&woken.thread_id, woken.queue), (&t1, true));
            let answer = next_envelope(&mut replies).await;
            assert_eq!(answer.text, format!("Own turn {n}."));
            assert_eq!(answer.in_reply_to, Some(woken.id));
        }
        for n in 3..=4 {
            std::fs::write(dir.join(format!("go{n}")), "").unwrap();
        }
        assert!(timeout(Duration::from_millis(500), to_itself.next()).await.is_err(), "no message past the cap");

        bus.send(&t1, "Is it done?").await;
        let done = next_envelope(&mut replies).await;
        assert_eq!((&done.thread_id, done.text.as_str()), (&t1, "Done."), "the dropped turns answer no one");
        assert_eq!(cli.received(2).len(), 3, "the initialize and the two prompts: none for the turns of its own");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let _ = std::fs::remove_dir_all(&dir);
}
