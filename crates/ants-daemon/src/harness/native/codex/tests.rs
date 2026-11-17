//! The codex translator against a stub `codex app-server` playing the shapes codex 0.169.0 sends: observed live where
//! a signed-out server shows them (handshake, threads, a turn failing with 401), else as its JSON schema gives them.

use std::assert_matches;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_client_protocol::v1::schema::{EnvVariable, ImageContent, McpServer, McpServerStdio, SessionId, TextContent};
use ants_proto::{LimitKind, StatusUpdate, ThreadId};
use ants_test_support::{StubCli, scratch_dir};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::harness::native::Protocol;
use crate::harness::{
    Harnesses, Launch, ModelSettings, Prompt, Readiness, Session, SignedOut, Spend, Steer, Steering, fork, handshake,
    parse_definitions,
};
use crate::permission::{Allowlist, PermissionPolicy};
use crate::ratelimit::Limited;

const THREAD: &str = "01a1e32e-45fc-75d2-9997-0d34d7ac8d3d";
const TURN: &str = "01a1e32e-653e-78f2-aed1-059df0817cf7";

fn ok(id: u64, result: Value) -> String {
    json!({ "id": id, "result": result }).to_string()
}

fn error(id: u64, code: i64, message: &str) -> String {
    json!({ "error": { "code": code, "message": message }, "id": id }).to_string()
}

fn note(method: &str, params: Value) -> String {
    json!({ "method": method, "params": params, "emittedAtMs": 1_794_812_831_514_u64 }).to_string()
}

/// A notification of `TURN` on `THREAD`, with `params` added.
fn turn_note(method: &str, params: Value) -> String {
    on(TURN, method, params)
}

/// A notification of turn `turn` on `THREAD`, with `params` added.
fn on(turn: &str, method: &str, mut params: Value) -> String {
    params["threadId"] = THREAD.into();
    params["turnId"] = turn.into();
    note(method, params)
}

/// The blocks of the handshake of codex `version`, signed in to `account`: `initialize` (request 1), `initialized`
/// and `account/read` (request 2).
fn handshake_of(version: &str, account: Value) -> Vec<Vec<String>> {
    handshake_in(Path::new("/h/.codex"), version, account)
}

/// The blocks of [`handshake_of`] from a server whose `CODEX_HOME` is `home`.
fn handshake_in(home: &Path, version: &str, account: Value) -> Vec<Vec<String>> {
    let user_agent = format!("ants/{version} (Mac OS 27.2.1; arm64) rex/0.1.0-preview.14 (ants; 0.1.0)");
    let hello = json!({ "userAgent": user_agent, "codexHome": home, "platformFamily": "unix", "platformOs": "macos" });
    let status = json!({ "status": "disabled", "serverName": "m", "installationId": "i", "environmentId": null });
    let account = json!({ "account": account, "userId": null, "requiresOpenaiAuth": true, "workspaceRouting": null });
    vec![vec![], vec![ok(1, hello), note("remoteControl/status/changed", status)], vec![], vec![ok(2, account)]]
}

fn signed_in() -> Vec<Vec<String>> {
    handshake_of("0.169.0", json!({ "type": "apiKey" }))
}

/// The answer to request `id` opening thread `thread`, as `thread/start`, `thread/resume` and `thread/fork` give it.
fn opened(id: u64, thread: &str) -> String {
    let sandbox = json!({ "type": "workspaceWrite", "writableRoots": [], "networkAccess": false, "excludeTmpdirEnvVar": false, "excludeSlashTmp": false });
    let thread = json!({
        "id": thread, "sessionId": thread, "forkedFromId": null, "preview": "", "ephemeral": false,
        "modelProvider": "openai", "model": "gpt-6.1-sol", "reasoningEffort": null, "createdAt": 1_794_812_823,
        "updatedAt": 1_794_812_823, "status": { "type": "idle" }, "path": null, "cwd": "/w", "cliVersion": "0.169.0",
        "source": "vscode", "name": null, "turns": [],
    });
    let result = json!({ "thread": thread, "model": "gpt-6.1-sol", "modelProvider": "openai", "cwd": "/w", "approvalPolicy": "never", "sandbox": sandbox, "reasoningEffort": null });
    ok(id, result)
}

fn turn(id: &str, status: &str, error: Value) -> Value {
    json!({ "id": id, "rootTurnId": id, "items": [], "itemsView": "notLoaded", "status": status, "error": error, "startedAt": 1_794_812_831, "completedAt": null, "durationMs": null })
}

/// The answer to `turn/start`, request `id`, and the notifications starting the turn.
fn turn_started(id: u64, turn_id: &str) -> Vec<String> {
    vec![
        ok(id, json!({ "turn": turn(turn_id, "inProgress", Value::Null) })),
        note("thread/status/changed", json!({ "threadId": THREAD, "status": { "type": "active", "activeFlags": [] } })),
        note("turn/started", json!({ "threadId": THREAD, "turn": turn(turn_id, "inProgress", Value::Null) })),
    ]
}

fn turn_completed(turn_id: &str, status: &str, error: Value) -> String {
    note("turn/completed", json!({ "threadId": THREAD, "turn": turn(turn_id, status, error) }))
}

fn turn_error(message: &str, info: Value) -> Value {
    json!({ "message": message, "codexErrorInfo": info, "additionalDetails": null, "misalignment": null })
}

fn delta(item: &str, text: &str) -> String {
    turn_note("item/agentMessage/delta", json!({ "itemId": item, "delta": text }))
}

fn item(method: &str, item: Value) -> String {
    turn_note(method, json!({ "item": item, "startedAtMs": 1, "completedAtMs": 2 }))
}

fn tokens(total: u64, input: u64, output: u64) -> Value {
    json!({ "totalTokens": total, "inputTokens": input, "cachedInputTokens": 0, "cacheWriteInputTokens": 0, "outputTokens": output, "reasoningOutputTokens": 0 })
}

fn usage(total: Value, last: Value) -> String {
    let usage = json!({ "total": total, "last": last, "modelContextWindow": 272_000 });
    turn_note("thread/tokenUsage/updated", json!({ "tokenUsage": usage }))
}

/// A harness of protocol codex run by a stub CLI in a scratch directory, removed on drop.
struct Fixture {
    dir: PathBuf,
    cli: StubCli,
    harness: Arc<Harness>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Fixture {
    /// `[harnesses.native]` of protocol codex, with `table` added.
    fn new(name: &str, table: &str) -> Self {
        let dir = scratch_dir(name).canonicalize().unwrap();
        let cli = StubCli::new(&dir, "codex");
        let text = format!(
            "[harnesses.native]\ncommand = {:?}\nprotocol = \"codex\"\nsign_in = \"codex login\"\n{table}",
            cli.path()
        );
        let harness = Harnesses::builtin().define(parse_definitions(&text).unwrap()).unwrap().get("native").unwrap();
        Self { dir, cli, harness }
    }

    /// A launch working in the scratch directory, every tool allowed were the harness to ask, with the ants MCP
    /// server carrying its token.
    fn launch(&self, settings: ModelSettings) -> Launch {
        let allow_all = PermissionPolicy::new(Allowlist::parse(&["*"]).unwrap(), None);
        let launch = Launch::new(self.harness.clone(), self.dir.clone(), Vec::new(), allow_all, settings);
        let stdio = McpServerStdio::new("ants", "/bin/ants")
            .args(vec!["mcp".to_owned()])
            .env(vec![EnvVariable::new("ANTS_MCP_TOKEN", "secret")]);
        let servers = vec![McpServer::Stdio(stdio)];
        let thread: ThreadId = "t1".parse().unwrap();
        launch
            .with_mcp(Arc::new(move |_, _, http| {
                assert!(!http, "codex is given stdio MCP servers");
                servers.clone()
            }))
            .for_thread(&thread)
    }

    fn script(&self, run: Option<usize>, blocks: &[Vec<String>]) {
        self.cli.script(run, blocks);
    }

    /// What run `run` of the CLI read, as JSON.
    fn received(&self, run: usize) -> Vec<Value> {
        self.cli.received(run).iter().map(|line| serde_json::from_str(line).unwrap()).collect()
    }
}

fn mcp_config() -> Value {
    json!({ "command": "/bin/ants", "args": ["mcp"], "env": { "ANTS_MCP_TOKEN": "secret" }, "default_tools_approval_mode": "approve" })
}

fn first_turn() -> Vec<String> {
    let command = json!({
        "type": "commandExecution", "id": "c1", "pluginId": null, "scriptPath": null,
        "command": "/bin/zsh -lc 'cat README.md'", "cwd": "/w", "processId": null, "source": "agent",
        "status": "inProgress",
        "commandActions": [{ "type": "read", "command": "cat README.md", "name": "README.md", "path": "README.md" }],
        "aggregatedOutput": null, "exitCode": null, "durationMs": null,
    });
    let mut ran = command.clone();
    (ran["status"], ran["exitCode"], ran["aggregatedOutput"]) = ("completed".into(), 0.into(), "# ants\n".into());
    let ask = json!({ "type": "mcpToolCall", "id": "m1", "server": "ants", "tool": "ask_ant", "status": "inProgress", "arguments": { "to": "bob" }, "result": null, "error": null, "durationMs": null });
    let mut asked = ask.clone();
    (asked["status"], asked["error"]) = ("failed".into(), json!({ "message": "no ant bob" }));
    let edit = json!({ "type": "fileChange", "id": "f1", "status": "completed", "changes": [{ "path": "/w/a.rs", "kind": { "type": "update", "move_path": null }, "diff": "@@ -1 +1 @@\n-a\n+b\n" }] });
    let plan = json!({ "explanation": null, "plan": [{ "step": "Read", "status": "completed" }, { "step": "Answer", "status": "inProgress" }] });
    let user = json!({ "type": "userMessage", "id": "u1", "clientId": null, "content": [{ "type": "text", "text": "go", "text_elements": [] }] });
    let mut blocks = turn_started(4, TURN);
    blocks.extend([
        item("item/started", user.clone()),
        item("item/completed", user),
        turn_note("item/reasoning/summaryTextDelta", json!({ "itemId": "r1", "delta": "Looking", "summaryIndex": 0 })),
        item("item/completed", json!({ "type": "reasoning", "id": "r1", "summary": ["Looking"], "content": [] })),
        item("item/started", command),
        item("item/completed", ran),
        item("item/started", ask),
        item("item/completed", asked),
        item("item/completed", edit),
        turn_note("turn/plan/updated", plan),
        delta("a1", "Hello"),
        delta("a1", " there"),
        item(
            "item/completed",
            json!({ "type": "agentMessage", "id": "a1", "text": "Hello there", "phase": "commentary" }),
        ),
        item("item/completed", json!({ "type": "agentMessage", "id": "a2", "text": "Done.", "phase": "final_answer" })),
        usage(tokens(1500, 1200, 300), tokens(1500, 1200, 300)),
        note("turn/completed", json!({ "threadId": THREAD, "turn": turn("another", "completed", Value::Null) })),
        note("thread/status/changed", json!({ "threadId": THREAD, "status": { "type": "idle" } })),
        turn_completed(TURN, "completed", Value::Null),
    ]);
    blocks
}

#[tokio::test]
async fn a_codex_thread_streams_its_turns_through_the_translator() {
    let codex = Fixture::new("codex-prompt", "args = [\"-c\", \"model_provider=\\\"openai\\\"\"]");
    let second = [
        turn_started(5, "turn-2"),
        vec![
            on("turn-2", "thread/tokenUsage/updated", json!({ "tokenUsage": { "total": tokens(2000, 1600, 400), "last": tokens(500, 400, 100), "modelContextWindow": null } })),
            on("turn-2", "item/agentMessage/delta", json!({ "itemId": "a3", "delta": "Again" })),
            turn_completed("turn-2", "completed", Value::Null),
        ],
    ]
    .concat();
    codex.script(None, &[signed_in(), vec![vec![opened(3, THREAD)], first_turn(), second]].concat());
    let settings = ModelSettings {
        model: Some("gpt-6.1-sol".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        ..Default::default()
    };
    let session = Session::start(&codex.launch(settings), None).await.unwrap();
    assert_eq!(session.id(), SessionId::new(THREAD));
    assert_eq!(session.agent().map(|agent| (agent.name.as_str(), agent.version.as_str())), Some(("codex", "0.169.0")));
    let (progress, mut status) = mpsc::channel(256);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let prompt = Prompt { progress: Some(progress), spend: Some(spend.clone()), ..Prompt::new("go") };
    assert_eq!(session.run(prompt).await.unwrap().text, "Hello there\n\nDone.");
    let prompt = Prompt { spend: Some(spend), ..Prompt::new("again") };
    assert_eq!(session.run(prompt).await.unwrap().text, "Again");
    session.close().await;

    let mut updates = Vec::new();
    while let Ok(update) = status.try_recv() {
        updates.push(update);
    }
    assert!(updates.contains(&StatusUpdate::Thought { text: "Looking".to_owned() }), "{updates:?}");
    let tool = |want: &str| {
        updates.iter().find_map(|u| match u {
            StatusUpdate::Tool { id, title, paths, .. } if id == want => Some((title.clone(), paths.clone())),
            _ => None,
        })
    };
    assert_eq!(tool("c1"), Some((Some("Read README.md".to_owned()), Some(vec!["/w/README.md".to_owned()]))));
    assert_eq!(tool("m1").unwrap().0.as_deref(), Some("Tool: ants/ask_ant"));
    assert_eq!(tool("f1").unwrap().0.as_deref(), Some("Edit /w/a.rs"));
    assert!(tool("u1").is_none() && tool("a1").is_none(), "messages are no tool calls");
    assert!(updates.iter().any(|u| matches!(u, StatusUpdate::Plan { steps } if steps.len() == 2)), "{updates:?}");
    let mut spends = Vec::new();
    while let Ok(spend) = spent.try_recv() {
        spends.push(spend);
    }
    assert_eq!(spends, [Spend::Tokens(1500), Spend::Tokens(500)], "each turn's tokens, without cost");

    assert_eq!(codex.cli.args(1), ["app-server", "-c", "model_provider=\"openai\""]);
    let received = codex.received(1);
    assert!(received.iter().all(|message| message.get("jsonrpc").is_none()), "JSON-RPC without its version");
    assert_eq!(received[0]["method"], "initialize");
    assert_eq!(received[0]["params"]["clientInfo"]["name"], "ants");
    assert!(received[0]["params"]["capabilities"]["optOutNotificationMethods"].is_array());
    assert_eq!(received[1], json!({ "method": "initialized", "params": {} }));
    assert_eq!(received[2]["method"], "account/read");
    let start = &received[3];
    assert_eq!(start["method"], "thread/start");
    let params = &start["params"];
    assert_eq!(params["cwd"], json!(codex.dir));
    assert_eq!((&params["approvalPolicy"], &params["sandbox"]), (&json!("never"), &json!("workspace-write")));
    assert_eq!(params["model"], "gpt-6.1-sol");
    assert_eq!(params["config"], json!({ "model_reasoning_effort": "high", "mcp_servers.ants": mcp_config() }));
    assert!(!codex.cli.args(1).concat().contains("secret"), "the token is not on the command line");
    assert_eq!(received[4]["method"], "turn/start");
    assert_eq!(received[4]["params"]["threadId"], THREAD);
    assert_eq!(received[4]["params"]["input"], json!([{ "type": "text", "text": "go", "text_elements": [] }]));
}

#[tokio::test]
async fn codex_threads_resume_and_fork_with_their_permissions_and_mcp_servers() {
    let codex = Fixture::new("codex-sessions", "mode = \"full-access\"");
    codex.script(Some(1), &[signed_in(), vec![vec![opened(3, THREAD)]]].concat());
    codex.script(Some(2), &[signed_in(), vec![vec![opened(3, "forked")]]].concat());
    let no_rollout = error(3, -32600, "no rollout found for thread id 01a1e331-02d0-74d2-8faa-64eac0503b15");
    codex.script(Some(3), &[signed_in(), vec![vec![no_rollout]]].concat());
    let launch = codex.launch(ModelSettings::default());

    let resumed = Session::start(&launch, Some(SessionId::new(THREAD))).await.unwrap();
    assert_eq!(resumed.id(), SessionId::new(THREAD));
    resumed.close().await;
    let resume = &codex.received(1)[3];
    assert_eq!(resume["method"], "thread/resume");
    let params = &resume["params"];
    assert_eq!((&params["threadId"], &params["excludeTurns"]), (&json!(THREAD), &json!(true)));
    assert_eq!((&params["approvalPolicy"], &params["sandbox"]), (&json!("never"), &json!("danger-full-access")));
    assert_eq!(params["config"], json!({ "mcp_servers.ants": mcp_config() }), "a thread keeps no MCP servers");

    assert_eq!(fork(&launch, &SessionId::new(THREAD)).await.unwrap(), Some(SessionId::new("forked")));
    let forked = &codex.received(2)[3];
    assert_eq!((&forked["method"], &forked["params"]["threadId"]), (&json!("thread/fork"), &json!(THREAD)));
    assert_eq!(forked["params"]["sandbox"], "danger-full-access");

    let err = format!("{:#}", fork(&launch, &SessionId::new(THREAD)).await.unwrap_err());
    assert!(err.contains("no rollout found") && err.contains("once a turn started in it"), "{err}");
}

#[tokio::test]
async fn a_signed_out_codex_is_told_apart() {
    let codex = Fixture::new("codex-signin", "");
    let launch = codex.launch(ModelSettings::default());
    let signed_out = handshake_of("0.169.0", Value::Null);
    codex.script(Some(1), &signed_out);
    codex.script(Some(2), &signed_out);
    let unauthorized = turn_error(
        "unexpected status 401 Unauthorized: Missing bearer or basic authentication in header, url: https://api.openai.com/v1/responses",
        json!({ "httpConnectionFailed": { "httpStatusCode": 401 } }),
    );
    let reconnecting =
        turn_error("Reconnecting... 2/5", json!({ "responseStreamDisconnected": { "httpStatusCode": 401 } }));
    let retrying = turn_note("error", json!({ "error": reconnecting, "willRetry": true }));
    let failing = [turn_started(4, TURN), vec![retrying, turn_completed(TURN, "failed", unauthorized)]].concat();
    codex.script(Some(3), &[signed_in(), vec![vec![opened(3, THREAD)], failing]].concat());
    codex.script(Some(4), &[vec!["!stderr Error: thread 'main' panicked".to_owned(), "!exit 101".to_owned()]]);

    let checked = handshake(&launch).await;
    assert_matches!(checked.readiness, Readiness::SignedOut { .. });
    assert_eq!(checked.sign_in.as_deref(), Some("codex login"));
    let err = Session::start(&launch, None).await.err().expect("a signed-out session");
    assert!(err.downcast_ref::<SignedOut>().is_some(), "{err:#}");

    let session = Session::start(&launch, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    let signed_out = err.downcast_ref::<SignedOut>().expect("a 401 is a sign-in error");
    assert!(signed_out.detail.contains("401 Unauthorized"), "{err:#}");
    session.close().await;

    let Readiness::Failed { detail } = handshake(&launch).await.readiness else { panic!("a failing CLI") };
    assert!(detail.contains("exited with exit status: 101: Error: thread 'main' panicked"), "{detail}");
}

#[tokio::test]
async fn credentials_refused_at_a_prompt_keep_codex_signed_out_until_they_change() {
    let codex = Fixture::new("codex-refused", "");
    let launch = codex.launch(ModelSettings::default());
    let home = codex.dir.join("codex-home");
    std::fs::create_dir_all(&home).unwrap();
    let auth = home.join("auth.json");
    std::fs::write(&auth, r#"{"auth_mode":"apikey","OPENAI_API_KEY":"sk-revoked"}"#).unwrap();
    let signed_in = handshake_in(&home, "0.169.0", json!({ "type": "apiKey" }));
    let unauthorized = turn_error(
        "unexpected status 401 Unauthorized: Incorrect API key provided",
        json!({ "httpConnectionFailed": { "httpStatusCode": 401 } }),
    );
    let failing = [turn_started(4, TURN), vec![turn_completed(TURN, "failed", unauthorized)]].concat();
    codex.script(Some(1), &[signed_in.clone(), vec![vec![opened(3, THREAD)], failing]].concat());
    codex.script(None, &[signed_in, vec![vec![opened(3, THREAD)]]].concat());

    let session = Session::start(&launch, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    assert!(err.downcast_ref::<SignedOut>().is_some(), "{err:#}");
    session.close().await;

    let Readiness::SignedOut { detail } = handshake(&launch).await.readiness else {
        panic!("signed out although account/read still finds the refused key")
    };
    assert!(detail.contains("401 Unauthorized") && detail.contains("auth.json unchanged since"), "{detail}");
    assert!(codex.received(2).iter().all(|sent| sent["method"] != "thread/start"), "no thread opens");

    std::fs::write(&auth, r#"{"auth_mode":"apikey","OPENAI_API_KEY":"sk-new"}"#).unwrap();
    assert_matches!(handshake(&launch).await.readiness, Readiness::Ready { .. });
}

#[tokio::test]
async fn codex_limits_are_classified_with_their_reset() {
    let codex = Fixture::new("codex-limits", "");
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let snapshot = json!({ "rateLimits": {
        "limitId": "codex", "limitName": null, "normalModelSlug": null, "credits": null, "individualLimit": null,
        "spendControlReached": null, "planType": "plus", "rateLimitReachedType": "rate_limit_reached",
        "primary": { "usedPercent": 100.0, "windowDurationMins": 300, "resetsAt": now + 3600 },
        "secondary": { "usedPercent": 40.0, "windowDurationMins": 10080, "resetsAt": now + 86400 },
    } });
    let sparse = json!({ "rateLimits": { "limitId": "codex", "secondary": null, "primary": null, "planType": null } });
    let usage_limit = turn_error("You've hit your usage limit.", json!("usageLimitExceeded"));
    let quota = [
        turn_started(4, TURN),
        vec![
            note("account/rateLimits/updated", snapshot),
            note("account/rateLimits/updated", sparse),
            turn_completed(TURN, "failed", usage_limit),
        ],
    ]
    .concat();
    codex.script(Some(1), &[signed_in(), vec![vec![opened(3, THREAD)], quota]].concat());
    let too_many = turn_error("exceeded retry limit, last status: 429 Too Many Requests", json!("rateLimitExceeded"));
    let limited = [turn_started(4, TURN), vec![turn_completed(TURN, "failed", too_many)]].concat();
    codex.script(Some(2), &[signed_in(), vec![vec![opened(3, THREAD)], limited]].concat());
    codex.script(Some(3), &handshake_of("0.168.2", json!({ "type": "apiKey" })));
    let launch = codex.launch(ModelSettings::default());

    let session = Session::start(&launch, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    let limited = err.downcast_ref::<Limited>().expect("a provider limit");
    assert_eq!(limited.kind, LimitKind::Quota, "{err:#}");
    let retry_after = limited.retry_after.expect("the reset of the window used up");
    assert!(retry_after > Duration::from_secs(3500) && retry_after <= Duration::from_hours(1), "{retry_after:?}");
    session.close().await;

    let session = Session::start(&launch, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    let limited = err.downcast_ref::<Limited>().expect("a provider limit");
    assert_eq!((limited.kind, limited.retry_after), (LimitKind::RateLimit, None), "{err:#}");
    session.close().await;

    let err = format!("{:#}", Session::start(&launch, None).await.err().expect("an old codex"));
    assert!(err.contains("codex 0.168.2 is older than 0.169.0"), "{err}");
}

#[tokio::test]
async fn a_codex_turn_outgrowing_the_context_runs_again_in_a_new_thread() {
    let codex = Fixture::new("codex-overflow", "");
    let overflow = turn_error("Codex ran out of room in the model's context window.", json!("contextWindowExceeded"));
    let fresh = |method: &str, mut params: Value| {
        params["threadId"] = "fresh".into();
        note(method, params)
    };
    let retried = vec![
        ok(7, json!({ "turn": turn("turn-2", "inProgress", Value::Null) })),
        fresh("item/agentMessage/delta", json!({ "turnId": "turn-2", "itemId": "a1", "delta": "Fine" })),
        fresh("turn/completed", json!({ "turn": turn("turn-2", "completed", Value::Null) })),
    ];
    let blocks = [
        signed_in(),
        vec![
            vec![opened(3, THREAD)],
            [turn_started(4, TURN), vec![turn_completed(TURN, "failed", overflow)]].concat(),
            vec![ok(5, json!({ "status": "unsubscribed" }))],
            vec![opened(6, "fresh")],
            retried,
        ],
    ]
    .concat();
    codex.script(None, &blocks);
    let session = Session::start(&codex.launch(ModelSettings::default()), None).await.unwrap();
    assert_eq!(session.prompt("go").await.unwrap(), "Fine");
    assert_eq!(session.id(), SessionId::new("fresh"));
    session.close().await;
    let received = codex.received(1);
    let methods: Vec<_> = received.iter().map(|m| m["method"].clone()).collect();
    let want = [
        "initialize",
        "initialized",
        "account/read",
        "thread/start",
        "turn/start",
        "thread/unsubscribe",
        "thread/start",
        "turn/start",
    ];
    assert_eq!(methods, want);
    assert_eq!(received[5]["params"], json!({ "threadId": THREAD }), "the abandoned thread unloaded");
}

#[tokio::test]
async fn a_cancelled_codex_turn_is_interrupted() {
    let codex = Fixture::new("codex-cancel", "");
    let working = [turn_started(4, TURN), vec![delta("a1", "Working")]].concat();
    let interrupted = vec![
        ok(5, json!({})),
        note("thread/status/changed", json!({ "threadId": THREAD, "status": { "type": "idle" } })),
        turn_completed(TURN, "interrupted", Value::Null),
    ];
    codex.script(None, &[signed_in(), vec![vec![opened(3, THREAD)], working, interrupted]].concat());
    let session = Session::start(&codex.launch(ModelSettings::default()), None).await.unwrap();
    let (stop, stopped) = oneshot::channel();
    let (progress, mut status) = mpsc::channel(16);
    let prompt = Prompt { stop: Some(stopped), progress: Some(progress), ..Prompt::new("go") };
    let cancel = async {
        status.recv().await;
        stop.send("Stopped by a human".to_owned()).unwrap();
    };
    let (reply, ()) = tokio::join!(session.run(prompt), cancel);
    let reply = reply.unwrap();
    assert_eq!((reply.text.as_str(), reply.cut.as_deref()), ("Working", Some("Stopped by a human")));
    assert!(session.is_alive(), "codex ended the turn itself");
    session.close().await;
    let interrupt = json!({ "id": 5, "method": "turn/interrupt", "params": { "threadId": THREAD, "turnId": TURN } });
    assert_eq!(codex.received(1)[5], interrupt);
}

#[tokio::test]
async fn a_codex_turn_takes_steers_while_it_runs() {
    let codex = Fixture::new("codex-steer", "");
    let working = [turn_started(4, TURN), vec![delta("a1", "Working")]].concat();
    let steered =
        vec![ok(5, json!({ "turnId": TURN })), delta("a1", " on both"), turn_completed(TURN, "completed", Value::Null)];
    codex.script(None, &[signed_in(), vec![vec![opened(3, THREAD)], working, steered]].concat());
    let session = Session::start(&codex.launch(ModelSettings::default()), None).await.unwrap();
    let (ready, started) = oneshot::channel();
    let (steer_tx, steers) = mpsc::channel(1);
    let (progress, mut status) = mpsc::channel(16);
    let steering = Some(Steering { ready, steers });
    let prompt = Prompt { progress: Some(progress), steering, ..Prompt::new("go") };
    let steer = async {
        started.await.unwrap();
        status.recv().await;
        let (accepted, took) = oneshot::channel();
        steer_tx.send(Steer { text: "and this".to_owned(), accepted }).await.unwrap();
        took.await.unwrap()
    };
    let (reply, took) = tokio::join!(session.run(prompt), steer);
    assert_eq!((reply.unwrap().text, took), ("Working on both".to_owned(), true));
    session.close().await;
    let input = json!([{ "type": "text", "text": "and this", "text_elements": [] }]);
    let steer = json!({ "id": 5, "method": "turn/steer", "params": { "threadId": THREAD, "expectedTurnId": TURN, "input": input } });
    assert_eq!(codex.received(1)[5], steer);
}

#[tokio::test]
async fn codex_approval_requests_are_declined_without_asking_ants() {
    let codex = Fixture::new("codex-approval", "");
    let ask = |id: u64, method: &str, mut params: Value| {
        (params["threadId"], params["turnId"]) = (THREAD.into(), TURN.into());
        json!({ "id": id, "method": method, "params": params }).to_string()
    };
    let command = json!({ "itemId": "c1", "startedAtMs": 1, "kind": "command", "environmentId": "local", "command": "rm -rf build", "cwd": "/w", "reason": null });
    let file = json!({ "itemId": "f1", "startedAtMs": 1, "reason": "write outside", "grantRoot": "/" });
    let permissions = json!({ "itemId": "p1", "environmentId": "local", "startedAtMs": 1, "cwd": "/w", "reason": "network", "permissions": {} });
    let elicitation = json!({ "serverName": "ants", "mode": "form", "message": "Which ant?" });
    let declined = json!({ "type": "commandExecution", "id": "c1", "command": "rm -rf build", "cwd": "/w", "source": "agent", "status": "declined", "commandActions": [{ "type": "unknown", "command": "rm -rf build" }], "aggregatedOutput": null, "exitCode": null, "durationMs": null });
    let mut asking = turn_started(4, TURN);
    asking.extend([item("item/started", declined.clone()), ask(0, "item/commandExecution/requestApproval", command)]);
    let blocks = [
        signed_in(),
        vec![
            vec![opened(3, THREAD)],
            asking,
            vec![ask(1, "item/fileChange/requestApproval", file)],
            vec![ask(2, "item/permissions/requestApproval", permissions)],
            vec![ask(3, "mcpServer/elicitation/request", elicitation)],
            vec![ask(4, "something/new", json!({}))],
            vec![
                item("item/completed", declined),
                delta("a1", "Skipped"),
                turn_completed(TURN, "completed", Value::Null),
            ],
        ],
    ]
    .concat();
    codex.script(None, &blocks);
    // Every tool is allowed, were a request to reach ants.
    let session = Session::start(&codex.launch(ModelSettings::default()), None).await.unwrap();
    let (progress, mut status) = mpsc::channel(16);
    let reply = session.run(Prompt { progress: Some(progress), ..Prompt::new("clean up") }).await.unwrap();
    assert_eq!(reply.text, "Skipped");
    session.close().await;
    let answers = codex.received(1)[5..10].to_vec();
    assert_eq!(
        answers,
        [
            json!({ "id": 0, "result": { "decision": "decline" } }),
            json!({ "id": 1, "result": { "decision": "decline" } }),
            json!({ "id": 2, "result": { "permissions": {}, "scope": "turn" } }),
            json!({ "id": 3, "result": { "action": "decline", "content": null, "_meta": null } }),
            json!({ "id": 4, "error": { "code": -32601, "message": "something/new is not supported" } }),
        ]
    );
    let mut failed = false;
    while let Ok(update) = status.try_recv() {
        failed |= matches!(update, StatusUpdate::Tool { id, status, .. } if id == "c1" && status.as_deref() == Some("failed"));
    }
    assert!(failed, "the declined command failed");
}

#[test]
fn codex_modes_name_its_sandboxes() {
    let define = |table: &str| {
        let text = format!("[harnesses.native]\ncommand = \"codex\"\nprotocol = \"codex\"\n{table}");
        parse_definitions(&text).and_then(|defs| Harnesses::builtin().define(defs)).map(|h| h.get("native").unwrap())
    };
    for mode in ["workspace-write", "read-only", "full-access"] {
        assert_eq!(define(&format!("mode = {mode:?}")).unwrap().protocol(), Protocol::Codex);
    }
    let err = format!("{:#}", define("mode = \"on-failure\"").unwrap_err());
    assert!(
        err.contains("protocol codex takes mode workspace-write, read-only or full-access, not on-failure"),
        "{err}"
    );
    assert_eq!(permissions(None), Some(("never", "workspace-write")));
    assert_eq!(permissions(Some("read-only")), Some(("never", "read-only")));
}

#[test]
fn codex_prompts_carry_texts_and_images() {
    let prompt = [
        ContentBlock::Text(TextContent::new("look")),
        ContentBlock::Image(ImageContent::new("iVBORw0KGgo=", "image/png")),
    ];
    assert_eq!(
        input(&prompt),
        [
            json!({ "type": "text", "text": "look", "text_elements": [] }),
            json!({ "type": "image", "url": "data:image/png;base64,iVBORw0KGgo=" }),
        ]
    );
    assert_eq!(version("ants/0.171.0-alpha.7 (Mac OS 27.2.1; arm64)").as_deref(), Some("0.171.0-alpha.7"));
    assert!(!older("0.171.0-alpha.7") && !older("1.0.0") && older("0.168.9"));
}
