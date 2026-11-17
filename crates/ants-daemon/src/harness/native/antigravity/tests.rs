//! The antigravity translator against a stub of `agy`, its events as agy 1.5.9 prints them.

use std::assert_matches;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::v1::schema::{HttpHeader, McpServer, McpServerHttp, SessionId};
use ants_proto::{HarnessAgent, LimitKind, StatusUpdate, ThreadId};
use ants_test_support::{StubCli, scratch_dir};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::harness::{
    Harnesses, Launch, ModelSettings, Prompt, Readiness, Session, SignedOut, Spend, handshake, parse_definitions,
};
use crate::limits::breaker::StartFailed;
use crate::limits::overflow::ContextOverflow;
use crate::limits::ratelimit::Limited;
use crate::permission::{Allowlist, PermissionPolicy};

const RATE_LIMITED: &str = "agent executor error: generating and executing: Error 429, Message: Resource has been \
                            exhausted (e.g. check quota)., Status: RESOURCE_EXHAUSTED, Details: []";

fn init(id: &str) -> String {
    let init = json!({ "cwd": "/w", "tools": ["run_command", "call_mcp_tool"], "permission_mode": "request-review" });
    json!({ "event": "init", "conversation_id": id, "init": init }).to_string()
}

/// A step of conversation `id`, its `fields` over the common ones.
fn step(id: &str, index: u64, state: &str, step_type: &str, fields: Value) -> String {
    let mut step = json!({ "conversation_id": id, "step_index": index, "state": state, "step_type": step_type });
    step.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
    json!({ "event": "step_update", "step_update": step }).to_string()
}

fn usage(input: u64, output: u64) -> Value {
    json!({ "input_tokens": input, "output_tokens": output, "thinking_tokens": 7, "cache_read_tokens": 200,
            "total_tokens": input + output })
}

/// A reply streamed in one step: `delta` then the closing newline, with its usage.
fn reply(id: &str, index: u64, delta: &str) -> Vec<String> {
    vec![
        step(id, index, "ACTIVE", "agent_response", json!({ "text_delta": delta })),
        step(id, index, "DONE", "agent_response", json!({ "text_delta": "\n", "usage": usage(800, 27) })),
    ]
}

/// The `result` of a turn of `id`, its `fields` over the common ones; its usage is the conversation's.
fn result(id: &str, status: &str, response: &str, fields: Value) -> String {
    let mut result = json!({
        "conversation_id": id, "status": status, "response": response, "duration_seconds": 1.5, "num_turns": 3,
        "usage": usage(9000, 300),
    });
    result.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
    json!({ "event": "result", "result": result }).to_string()
}

fn lines(lines: &[&str]) -> Vec<String> {
    lines.iter().map(|&line| line.to_owned()).collect()
}

/// A harness of protocol antigravity run by a stub `agy` in a scratch directory, removed on drop.
struct Agy {
    dir: PathBuf,
    cli: StubCli,
    harness: Arc<Harness>,
}

impl Drop for Agy {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Agy {
    /// `[harnesses.agy]` of protocol antigravity, with `table` added.
    fn new(name: &str, table: &str) -> Self {
        let dir = scratch_dir(name).canonicalize().unwrap();
        let cli = StubCli::new(&dir, "agy");
        let text = format!(
            "[harnesses.agy]\ncommand = {:?}\nprotocol = \"antigravity\"\nsign_in = \"agy\"\n{table}",
            cli.path()
        );
        let harness = Harnesses::builtin().define(parse_definitions(&text).unwrap()).unwrap().get("agy").unwrap();
        Self { dir, cli, harness }
    }

    fn launch(&self, settings: ModelSettings) -> Launch {
        self.launch_with_key("", settings)
    }

    /// A launch with GEMINI_API_KEY `key` (none when empty), the daemon's left out.
    fn launch_with_key(&self, key: &str, settings: ModelSettings) -> Launch {
        let allow_all = PermissionPolicy::new(Allowlist::parse(&["*"]).unwrap(), None);
        let env = vec![(API_KEY.to_owned(), key.to_owned())];
        Launch::new(self.harness.clone(), self.dir.clone(), env, allow_all, settings)
    }

    /// Script run `run` as `agy --version`.
    fn version(&self, run: usize, version: &str) {
        self.cli.script(Some(run), &[vec![version.to_owned(), "!exit 0".to_owned()]]);
    }

    /// Script run `run` as a session: block 0 printed at its start, block N after its Nth prompt.
    fn session(&self, run: usize, blocks: &[Vec<String>]) {
        self.cli.script(Some(run), blocks);
    }

    /// The prompts run `run` read.
    fn prompts(&self, run: usize) -> Vec<Value> {
        self.cli.received(run).iter().map(|line| serde_json::from_str(line).unwrap()).collect()
    }

    /// The value run `run` got after `flag`.
    fn arg(&self, run: usize, flag: &str) -> Option<String> {
        let args = self.cli.args(run);
        args.iter().position(|arg| arg == flag).and_then(|at| args.get(at + 1).cloned())
    }
}

fn user(text: &str) -> Value {
    json!({ "event": "user", "message": { "content": text } })
}

#[tokio::test]
async fn an_agy_turn_streams_its_steps_and_runs_with_the_ant_settings() {
    let settings = r#"{"modelProvider":"gemini","permissions":{"allow":["command(git)"]}}"#;
    let table = format!("args = [\"--settings\", {settings:?}, \"--dangerously-skip-permissions\"]\nmode = \"strict\"");
    let agy = Agy::new("agy-turn", &table);
    agy.version(1, "1.5.9");
    let id = "c1";
    let command = json!({ "name": "run_command", "parameters": { "CommandLine": "touch x" } });
    let mut turn = vec![step(id, 0, "DONE", "user_input", json!({}))];
    turn.push(step(id, 1, "ACTIVE", "agent_response", json!({ "text_delta": "Hello" })));
    turn.push(step(id, 1, "DONE", "agent_response", json!({ "text_delta": "\n", "usage": usage(800, 27) })));
    turn.push(step(id, 2, "ACTIVE", "tool", json!({ "tool_name": "run_command", "tool_info": command })));
    let mut ran = command.clone();
    ran["output"] = "Command exited with code 0.".into();
    turn.push(step(id, 2, "DONE", "tool", json!({ "tool_name": "run_command", "tool_info": ran })));
    let edit = json!({ "name": "edit_file", "parameters": { "Path": "/w/a.txt" }, "error": { "type": "TOOL_ERROR", "message": "no" } });
    turn.push(step(id, 3, "ERROR", "tool", json!({ "tool_name": "edit_file", "tool_info": edit })));
    turn.extend(reply(id, 4, "there"));
    turn.push(result(id, "SUCCESS", "Hello\nthere\n", json!({})));
    agy.session(2, &[vec![init(id)], turn]);
    let thread: ThreadId = "t1".parse().unwrap();
    let ants = McpServerHttp::new("ants", "http://127.0.0.1:1/mcp")
        .headers(vec![HttpHeader::new("Authorization", "Bearer secret-token")]);
    let servers = vec![McpServer::Http(ants)];
    let settings = ModelSettings {
        model: Some("gemini-3.1-pro-high".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        ..Default::default()
    };
    let launch = agy
        .launch(settings)
        .with_mcp(Arc::new(move |_, _, http| {
            assert!(http, "agy takes HTTP MCP servers");
            servers.clone()
        }))
        .for_thread(&thread);

    let session = Session::start(&launch, None).await.unwrap();
    let named = HarnessAgent { name: "agy".to_owned(), version: "1.5.9".to_owned() };
    assert_eq!((session.agent(), session.id()), (Some(&named), SessionId::new(id)));
    let private = PathBuf::from(agy.arg(2, "--add-dir").expect("the session's directory added"));
    let written =
        |name: &str| -> Value { serde_json::from_str(&std::fs::read_to_string(private.join(name)).unwrap()).unwrap() };
    let mcp = json!({ "mcpServers": { "ants": {
        "serverUrl": "http://127.0.0.1:1/mcp", "headers": { "Authorization": "Bearer secret-token" }, "timeoutSeconds": 1860,
    } } });
    assert_eq!(written(".agents/mcp_config.json"), mcp);
    let merged = json!({
        "modelProvider": "gemini", "toolPermission": "strict", "permissions": { "allow": ["command(git)", "mcp(ants/*)"] },
    });
    assert_eq!(written("settings.json"), merged, "the definition's settings, its mode and ants' tools allowed");
    let mode = std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&private).unwrap().permissions());
    assert_eq!(mode & 0o777, 0o700);

    let (progress, mut status) = mpsc::channel(32);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let prompt = Prompt { progress: Some(progress), spend: Some(spend), ..Prompt::new("/go") };
    assert_eq!(session.run(prompt).await.unwrap().text, "Hello\nthere\n");
    session.close().await;

    let args = agy.cli.args(2);
    let protocol =
        ["--input-format", "stream-json", "--output-format", "stream-json", "--disable-slash-commands", "--settings"];
    assert_eq!(args[..6], protocol);
    assert_eq!(
        args[7..],
        [
            "--add-dir",
            &private.display().to_string(),
            "--model",
            "gemini-3.1-pro-high",
            "--effort",
            "high",
            "--dangerously-skip-permissions"
        ]
    );
    assert!(!args.iter().any(|arg| arg.contains("secret-token")), "{args:?}");
    assert_eq!(agy.cli.args(1), ["--version"]);
    for run in [1, 2] {
        assert_eq!(agy.cli.env(run, "AGY_CLI_DISABLE_AUTO_UPDATE").as_deref(), Some("true"));
    }
    assert_eq!(agy.cli.cwd(2), agy.dir);
    assert_eq!(agy.prompts(2), [user("/go")]);

    let mut updates = Vec::new();
    while let Ok(update) = status.try_recv() {
        updates.push(update);
    }
    let tool = |id: &str, title: &str, kind: &str, state: &str| StatusUpdate::Tool {
        id: id.to_owned(),
        title: Some(title.to_owned()),
        tool_kind: Some(kind.to_owned()),
        status: Some(state.to_owned()),
        paths: None,
    };
    assert!(updates.contains(&tool("agy-step-2", "touch x", "execute", "in_progress")), "{updates:?}");
    assert!(
        updates.iter().any(|u| matches!(u, StatusUpdate::Tool { id, status, .. } if id == "agy-step-2" && status.as_deref() == Some("completed"))),
        "{updates:?}"
    );
    assert!(
        updates.iter().any(|u| matches!(u, StatusUpdate::Tool { id, title, status, .. }
            if id == "agy-step-3" && title.as_deref() == Some("edit_file /w/a.txt") && status.as_deref() == Some("failed"))),
        "{updates:?}"
    );
    let mut spends = Vec::new();
    while let Ok(spend) = spent.try_recv() {
        spends.push(spend);
    }
    assert_eq!(spends, [Spend::Tokens(2 * 827)], "the turn's steps, not the conversation's total");
    for _ in 0..50 {
        if !private.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!private.exists(), "the session's directory goes with it");
}

#[tokio::test]
async fn agy_resumes_its_conversations_and_a_cancel_interrupts_it() {
    let agy = Agy::new("agy-resume", "");
    let launch = agy.launch(ModelSettings::default());
    agy.version(1, "1.5.9");
    agy.session(2, &[vec![init("c1")]]);
    let resumed = Session::start(&launch, Some(SessionId::new("c1"))).await.unwrap();
    assert_eq!(resumed.id(), SessionId::new("c1"));
    resumed.close().await;
    assert_eq!(agy.arg(2, "--conversation").as_deref(), Some("c1"));
    assert_eq!(agy.arg(2, "--add-dir"), None, "no MCP servers, no directory added");

    // agy opens a new conversation for an id it does not know; the session opens on it.
    agy.version(3, "1.5.9");
    agy.session(
        4,
        &[vec![init("c2")], vec![step("c2", 0, "ACTIVE", "agent_response", json!({ "text_delta": "Working" }))]],
    );
    agy.session(
        5,
        &[vec![init("c2")], [reply("c2", 3, "Again"), vec![result("c2", "SUCCESS", "Again\n", json!({}))]].concat()],
    );
    let session = Session::start(&launch, Some(SessionId::new("gone"))).await.unwrap();
    assert_eq!(session.id(), SessionId::new("c2"));
    assert_eq!(agy.cli.runs(), 4, "the new session is the conversation agy opened");

    let (stop, stopped) = oneshot::channel();
    let (progress, mut status) = mpsc::channel(16);
    let prompt = Prompt { stop: Some(stopped), progress: Some(progress), ..Prompt::new("go") };
    let cancel = async {
        status.recv().await;
        stop.send("Stopped by a human".to_owned()).unwrap();
    };
    let (cut, ()) = tokio::join!(session.run(prompt), cancel);
    let cut = cut.unwrap();
    assert_eq!((cut.text.as_str(), cut.cut.as_deref()), ("Working", Some("Stopped by a human")));
    assert!(session.is_alive(), "the prompt ended cancelled");
    assert_eq!(session.prompt("again").await.unwrap(), "Again\n");
    session.close().await;
    assert_eq!(agy.arg(5, "--conversation").as_deref(), Some("c2"), "started again on its conversation");
    assert_eq!(agy.prompts(5), [user("again")]);
}

#[tokio::test]
async fn a_signed_out_or_misconfigured_agy_fails_to_open_a_session() {
    let agy = Agy::new("agy-signin", "");
    let launch = agy.launch(ModelSettings::default());
    let refused = |error: &str| {
        let usage = json!({ "input_tokens": 0, "output_tokens": 0, "total_tokens": 0 });
        let result = json!({ "conversation_id": "", "status": "ERROR", "response": "", "error": error,
                             "num_turns": 0, "usage": usage, "finished_reason": "error" });
        json!({ "event": "result", "result": result }).to_string()
    };
    let signed_out = vec![
        refused("authentication failed or timed out"),
        "!stderr Error: authentication required. Run 'agy' to log in, then retry.".to_owned(),
        "!stderr error: authentication failed or timed out".to_owned(),
        "!exit 1".to_owned(),
    ];
    for run in [1, 3] {
        agy.version(run, "1.5.9");
        agy.session(run + 1, std::slice::from_ref(&signed_out));
    }
    let checked = handshake(&launch).await;
    assert_matches!(checked.readiness, Readiness::SignedOut { .. });
    assert_eq!(checked.sign_in.as_deref(), Some("agy"));
    let err = Session::start(&launch, None).await.err().expect("signed out");
    let signed_out = err.downcast_ref::<SignedOut>().expect("a sign-in error");
    assert_eq!((signed_out.harness.as_str(), signed_out.sign_in.as_deref()), ("agy", Some("agy")));

    agy.version(5, "1.5.9");
    let unknown = r#"invalid model selection (--model "nope" --effort ""): model nope is not recognized"#;
    agy.session(6, &[vec![refused(unknown), "!exit 1".to_owned()]]);
    let err = Session::start(&launch, None).await.err().expect("an unknown model");
    let failed = err.downcast_ref::<StartFailed>().expect("a start failure");
    assert!(failed.detail.contains("model nope is not recognized"), "{failed:?}");

    agy.version(7, "1.5.9");
    agy.session(8, &[lines(&["!stderr Error: --settings: unknown setting \"bogus\"", "!exit 1"])]);
    let Readiness::Failed { detail } = handshake(&launch).await.readiness else { panic!("a failing agy") };
    assert!(detail.contains("exited with exit status: 1: Error: --settings: unknown setting"), "{detail}");

    agy.version(9, "agy 1.5.8");
    let Readiness::Failed { detail } = handshake(&launch).await.readiness else { panic!("an old agy") };
    assert!(detail.contains("agy 1.5.8 is older than 1.5.9"), "{detail}");
    assert_eq!(agy.cli.runs(), 9, "no session of an old agy");

    agy.version(10, "1.5.9");
    agy.session(11, &[vec![init("c1")]]);
    assert_eq!(handshake(&launch).await.readiness, Readiness::Ready { agent: "agy 1.5.9".to_owned() });
}

#[tokio::test]
async fn agy_turn_errors_are_classified() {
    let agy = Agy::new("agy-limits", "");
    let launch = agy.launch(ModelSettings::default());
    let fatal = |error: &str, reported: Value| {
        vec![
            step("c1", 1, "DONE", "error_message", json!({})),
            result("c1", "ERROR", "", json!({ "error": error, "finished_reason": "error" })),
            format!("!stderr AGY_ERROR: {reported}"),
            "!exit 3".to_owned(),
        ]
    };
    let quota = "agent executor error: generating and executing: Error 429, Message: You exceeded your current \
                 quota, please check your plan and billing details.\nPlease retry in 7h3m2s., Status: RESOURCE_EXHAUSTED";
    let reported = json!({ "status": "RESOURCE_EXHAUSTED", "error_code": 429, "code_kind": "http", "retryable": true });
    agy.version(1, "1.5.9");
    agy.session(2, &[vec![init("c1")], fatal(RATE_LIMITED, reported.clone())]);
    agy.version(3, "1.5.9");
    agy.session(4, &[vec![init("c1")], fatal(quota, reported)]);
    for (kind, retry_after) in [(LimitKind::RateLimit, None), (LimitKind::Quota, Some(Duration::from_secs(25_382)))] {
        let session = Session::start(&launch, None).await.unwrap();
        let err = session.prompt("go").await.unwrap_err();
        let limited = err.downcast_ref::<Limited>().expect("a provider limit");
        assert_eq!((limited.kind, limited.retry_after), (kind, retry_after), "{err:#}");
        session.close().await;
    }

    // Too long for the model: once again in a new session, then given up.
    let overflow = "agent executor error: generating and executing: Error 400, Message: The input token count \
                    (1100000) exceeds the maximum number of tokens allowed (1048576)., Status: INVALID_ARGUMENT";
    let reported = json!({ "status": "INVALID_ARGUMENT", "error_code": 400 });
    agy.version(5, "1.5.9");
    agy.session(6, &[vec![init("c1")], fatal(overflow, reported.clone())]);
    agy.session(7, &[vec![init("c2")], fatal(overflow, reported)]);
    let session = Session::start(&launch, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    assert!(err.downcast_ref::<ContextOverflow>().is_some(), "{err:#}");
    assert_eq!(agy.cli.runs(), 7, "retried in a new session");
    assert!(agy.prompts(7)[0]["message"]["content"].as_str().unwrap().contains("go"));
}

#[tokio::test]
async fn a_retry_agy_recovered_from_is_no_failure() {
    let agy = Agy::new("agy-stale", "");
    let stale = format!("API error (attempt 1): {}", &RATE_LIMITED[RATE_LIMITED.find("Error 429").unwrap()..]);
    let failing = json!({ "error": stale, "finished_reason": "error" });
    let recovered = [
        vec![step("c1", 1, "DONE", "error_message", json!({}))],
        reply("c1", 2, "Hi"),
        vec![result("c1", "ERROR", "Hi\n", failing.clone())],
    ];
    // Denied tools end the turn without a reply.
    let denied = json!({ "error": stale, "denied_actions": [{ "action": "command", "display_name": "Bash" }] });
    let blocks = [
        vec![init("c1")],
        recovered.concat(),
        vec![result("c1", "ERROR", "", denied)],
        [vec![step("c1", 4, "DONE", "error_message", json!({}))], vec![result("c1", "ERROR", "", failing)]].concat(),
    ];
    agy.version(1, "1.5.9");
    agy.session(2, &blocks);
    let session = Session::start(&agy.launch(ModelSettings::default()), None).await.unwrap();
    assert_eq!(session.prompt("one").await.unwrap(), "Hi\n");
    assert_eq!(session.prompt("two").await.unwrap(), "");
    let err = session.prompt("three").await.unwrap_err();
    assert_matches!(err.downcast_ref::<Limited>(), Some(Limited { kind: LimitKind::RateLimit, .. }), "{err:#}");
    session.close().await;
}

#[tokio::test]
async fn an_api_key_refused_at_a_prompt_keeps_agy_signed_out_until_it_changes() {
    let agy = Agy::new("agy-refused", r#"args = ["--settings", '{"modelProvider": "gemini"}']"#);
    let invalid = "agent executor error: generating and executing: Error 400, Message: API key not valid. Please pass \
                   a valid API key., Status: INVALID_ARGUMENT";
    let reported = json!({ "status": "INVALID_ARGUMENT", "error_code": 400, "code_kind": "http", "retryable": false });
    let refused = vec![
        step("c1", 1, "DONE", "error_message", json!({})),
        result("c1", "ERROR", "", json!({ "error": invalid, "finished_reason": "error" })),
        format!("!stderr AGY_ERROR: {reported}"),
        "!exit 3".to_owned(),
    ];
    agy.version(1, "1.5.9");
    agy.session(2, &[vec![init("c1")], refused]);
    let revoked = agy.launch_with_key("AIza-revoked", ModelSettings::default());
    let session = Session::start(&revoked, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    assert!(err.downcast_ref::<SignedOut>().is_some(), "{err:#}");
    session.close().await;

    agy.version(3, "1.5.9");
    let Readiness::SignedOut { detail } = handshake(&revoked).await.readiness else {
        panic!("signed out although agy opens a session with the refused key")
    };
    assert!(detail.contains("API key not valid") && detail.contains("GEMINI_API_KEY unchanged since"), "{detail}");
    assert_eq!(agy.cli.runs(), 3, "no agy started for a session");
    agy.version(4, "1.5.9");
    agy.session(5, &[vec![init("c2")]]);
    let replaced = agy.launch_with_key("AIza-replaced", ModelSettings::default());
    assert_matches!(handshake(&replaced).await.readiness, Readiness::Ready { .. });
}

#[test]
fn agy_signs_in_with_the_api_key_where_its_settings_choose_gemini() {
    let agy = Agy::new("agy-provider", "");
    let home = agy.dir.join("home");
    let chosen = |args: &[&str]| {
        let mut harness = (*agy.harness).clone();
        harness.args = args.iter().map(|&arg| arg.to_owned()).collect();
        gemini_provider(&Cli::new(&harness, &[], &ModelSettings::default()), Some(&home))
    };
    assert!(!chosen(&[]), "a Google account by default");
    let own = home.join(".gemini/antigravity-cli/settings.json");
    std::fs::create_dir_all(own.parent().unwrap()).unwrap();
    std::fs::write(&own, r#"{"modelProvider": "gemini"}"#).unwrap();
    assert!(chosen(&[]));
    assert!(chosen(&["--settings", r#"{"toolPermission": "strict"}"#]));
    assert!(!chosen(&["--settings", r#"{"modelProvider": "other"}"#]), "the definition's settings win");
    let file = agy.dir.join("other.json");
    std::fs::write(&file, r#"{"modelProvider": "other"}"#).unwrap();
    assert!(!chosen(&["--settings", file.to_str().unwrap()]));
    assert!(chosen(&["--settings", "other.json"]), "relative to the session's directory, not read");
}

#[test]
fn an_agy_turn_ends_as_its_result_says() {
    let steps = Steps::default();
    let interrupted =
        json!({ "status": "ERROR", "response": "Hel", "error": "interrupted", "finished_reason": "interrupted" });
    assert_eq!(outcome(&interrupted, &steps, true), Outcome::Ended(StopReason::Cancelled));
    assert_eq!(outcome(&interrupted, &steps, false), Outcome::Failed("agy was interrupted: interrupted".to_owned()));
    let stale = json!({ "status": "ERROR", "response": "", "error": "API error (attempt 2): Error 503" });
    assert_eq!(outcome(&stale, &steps, false), Outcome::Ended(StopReason::EndTurn));
    let unanswered = Steps { unanswered: true, ..Steps::default() };
    assert_eq!(outcome(&stale, &unanswered, false), Outcome::Failed("API error (attempt 2): Error 503".to_owned()));
    let fatal = json!({ "status": "ERROR", "response": "partial", "error": "agent executor error: boom" });
    assert_eq!(outcome(&fatal, &steps, false), Outcome::Failed("agent executor error: boom".to_owned()));

    assert_eq!(failure("authentication failed or timed out", "").code, ErrorCode::AUTH_REQUIRED);
    assert_eq!(failure("Error 400, Message: API key not valid.", "").code, ErrorCode::AUTH_REQUIRED);
    assert!(failure("Your AI credits balance is too low to continue.", "").message.starts_with("quota exceeded: "));
}

#[test]
fn agy_definitions_are_checked_as_they_load() {
    let define = |table: &str| {
        let text = format!("[harnesses.agy]\ncommand = \"agy\"\nprotocol = \"antigravity\"\n{table}");
        parse_definitions(&text).and_then(|defs| Harnesses::builtin().define(defs)).map(|h| h.get("agy").unwrap())
    };
    for mode in MODES {
        define(&format!("mode = {mode:?}")).unwrap();
    }
    let refused = |table: &str| format!("{:#}", define(table).unwrap_err());
    let err = refused("mode = \"auto\"");
    assert!(
        err.contains(
            "protocol antigravity takes mode request-review, proceed-in-sandbox, always-proceed or strict, not auto"
        ),
        "{err}"
    );
    let err = refused("args = [\"-p\", \"hi\"]");
    assert!(err.contains("protocol antigravity sets agy's --p itself"), "{err}");
    let err = refused("[harnesses.agy.context]\nbig = { args = [\"--conversation=x\"] }");
    assert!(err.contains("sets agy's --conversation itself"), "{err}");
    let err = refused("args = [\"--settings={oops\"]");
    assert!(err.contains("--settings {oops: "), "{err}");
    assert!(refused("args = [\"--settings\"]").contains("--settings takes a value"));
    define("args = [\"--settings\", \"agy-settings.json\", \"--mode\", \"plan\"]").unwrap();
}
