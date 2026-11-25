//! The claude protocol against a stub CLI playing the events Claude Code 2.1.325 was seen to send (trimmed).

use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{EnvVariable, McpServer, McpServerStdio, SessionId, TextContent};
use agent_client_protocol::{Agent, Channel, Client, ErrorCode};
use ants_proto::{HarnessAgent, LimitKind, PlanStep, StatusUpdate, ThreadId};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use super::failure::error;
use super::*;
use crate::harness::native::Protocol;
use crate::harness::native::tests::{StubHarness, run};
use crate::harness::{
    Launch, ModelSettings, Prompt, Readiness, Session, SignedOut, Spend, Steer, Steering, fork, handshake,
};
use crate::limits::breaker::StartFailed;
use crate::limits::ratelimit::Limited;
use crate::permission::{Allowlist, PermissionPolicy};

const VERSION: &str = "2.1.325 (Claude Code)";

/// The success response to control request `n` of a session.
fn ok(n: u64, response: Value) -> String {
    let response = json!({ "subtype": "success", "request_id": format!("ants-{n}"), "response": response });
    json!({ "type": "control_response", "response": response }).to_string()
}

/// The response to `initialize`, the first control request, of a CLI signed in to claude.ai.
fn signed_in() -> String {
    let account = json!({ "email": "a@example.com", "subscriptionType": "Claude Max", "apiProvider": "firstParty" });
    ok(1, json!({ "account": account, "claude_code_version": "2.1.325", "current_permission_mode": "default" }))
}

/// An event of the main thread of the turn.
fn event(mut event: Value) -> String {
    event["parent_tool_use_id"] = Value::Null;
    event["session_id"] = json!("s");
    event.to_string()
}

fn init() -> String {
    init_with(&[])
}

/// The `init` event of a Claude Code advertising `capabilities`.
fn init_with(capabilities: &[&str]) -> String {
    let servers = json!([{ "name": "ants", "status": "connected", "source": "dynamic" }]);
    event(json!({ "type": "system", "subtype": "init", "model": "claude-haiku-5-5", "mcp_servers": servers,
        "capabilities": capabilities }))
}

/// Claude Code reporting itself `state`, such as running or idle.
fn state(state: &str) -> String {
    event(json!({ "type": "system", "subtype": "session_state_changed", "state": state }))
}

fn stream(inner: Value) -> String {
    event(json!({ "type": "stream_event", "event": inner }))
}

fn message_start(id: &str) -> String {
    stream(json!({ "type": "message_start", "message": { "id": id, "model": "claude-haiku-5-5", "content": [] } }))
}

fn text(text: &str) -> String {
    stream(json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": text } }))
}

fn thinking(text: &str) -> String {
    stream(
        json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "thinking_delta", "thinking": text } }),
    )
}

/// The end of a model call whose context held `context` tokens, `output` of them its output.
fn message_delta(context: u64, output: u64) -> String {
    let usage = json!({ "input_tokens": 2, "cache_creation_input_tokens": 100,
        "cache_read_input_tokens": context - 102 - output, "output_tokens": output });
    stream(json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": usage }))
}

/// A block of assistant message `id`, which Claude Code sends one per line.
fn assistant(id: &str, block: Value) -> String {
    let message = json!({ "id": id, "model": "claude-haiku-5-5", "role": "assistant", "content": [block] });
    event(json!({ "type": "assistant", "message": message }))
}

fn tool_result(id: &str, is_error: bool) -> String {
    let content = json!([{ "type": "tool_result", "tool_use_id": id, "content": "done", "is_error": is_error }]);
    event(json!({ "type": "user", "message": { "role": "user", "content": content } }))
}

/// The result of a turn, the session having cost `total` so far, with `fields` over the usual ones.
fn result(total: f64, fields: Value) -> String {
    let since = json!({ "claude-haiku-5-5": { "inputTokens": 4, "outputTokens": 50, "cacheReadInputTokens": 1000,
        "cacheCreationInputTokens": 100, "thinkingTokens": 10, "costUSD": 0.01 } });
    let mut result = json!({
        "type": "result", "subtype": "success", "is_error": false, "result": "", "stop_reason": "end_turn",
        "terminal_reason": "completed", "session_id": "s", "total_cost_usd": total, "api_error_status": null,
        "modelUsage": { "claude-haiku-5-5": { "costUSD": total, "contextWindow": 1_000_000 } },
        "model_usage_since_last_result": since,
    });
    for (key, value) in fields.as_object().into_iter().flatten() {
        result[key] = value.clone();
    }
    result.to_string()
}

/// A turn failing as Claude Code ends a failed model call: its own message, then an error result.
fn failure(kind: &str, status: Option<u16>, message: &str, terminal: &str) -> Vec<String> {
    let content = json!([{ "type": "text", "text": message }]);
    let synthetic = json!({ "type": "assistant", "error": kind, "is_api_error_message": true,
        "api_error_status": status, "message": { "id": "e1", "model": "<synthetic>", "content": content } });
    let fields =
        json!({ "is_error": true, "result": message, "api_error_status": status, "terminal_reason": terminal });
    vec![init(), event(synthetic), result(0.0, fields)]
}

/// A harness of protocol claude run by a stub CLI.
struct Fixture(StubHarness);

impl std::ops::Deref for Fixture {
    type Target = StubHarness;

    fn deref(&self) -> &StubHarness {
        &self.0
    }
}

impl Fixture {
    /// `[harnesses.claude-native]` of protocol claude, with `table` added.
    fn new(name: &str, table: &str) -> Self {
        let table = format!("sign_in = \"claude login\"\n{table}");
        Self(StubHarness::new(name, "claude-native", Protocol::Claude, "claude", &table))
    }

    fn launch(&self, settings: ModelSettings) -> Launch {
        self.launch_with_key("", settings)
    }

    /// A launch with no credentials but ANTHROPIC_API_KEY `key` (none when empty), the ones of the daemon's
    /// environment and home left out.
    fn launch_with_key(&self, key: &str, settings: ModelSettings) -> Launch {
        let env = [
            ("CLAUDE_CONFIG_DIR", self.dir.to_str().unwrap()),
            ("ANTHROPIC_API_KEY", key),
            ("ANTHROPIC_AUTH_TOKEN", ""),
            ("CLAUDE_CODE_OAUTH_TOKEN", ""),
        ];
        self.0.launch(&env, settings)
    }

    /// Have run `run` (every run when None) print the version, then play `blocks` after each line it reads: a
    /// `--version` run reads none, a session's run its control requests and prompts.
    fn script(&self, run: Option<usize>, blocks: &[Vec<String>]) {
        let mut all = vec![vec![VERSION.to_owned()]];
        all.extend(blocks.iter().cloned());
        self.cli.script(run, &all);
    }
}

fn define(table: &str) -> anyhow::Result<Arc<Harness>> {
    crate::harness::native::tests::define("claude-native", Protocol::Claude, table)
}

/// The arguments every session's CLI gets first.
const FIRST_ARGS: [&str; 13] = [
    "-p",
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    "--verbose",
    "--include-partial-messages",
    "--replay-user-messages",
    "--thinking-display",
    "summarized",
    "--strict-mcp-config",
    "--permission-prompts",
    "none",
];

/// The events of a turn reading a file under `dir` with a todo list, then answering "Hello there".
fn reading_turn(dir: &std::path::Path) -> Vec<String> {
    let todos = json!({ "todos": [
        { "content": "Write a.txt", "status": "in_progress", "activeForm": "Writing a.txt" },
        { "content": "Edit a.txt", "status": "pending", "activeForm": "Editing a.txt" },
    ] });
    let read = json!({ "type": "tool_use", "id": "toolu_2", "name": "Read",
        "input": { "file_path": dir.join("src/a.rs"), "offset": 10 } });
    let subagent = json!({ "type": "assistant", "parent_tool_use_id": "toolu_9",
        "message": { "id": "msg_9", "content": [{ "type": "text", "text": "inner" }] } });
    let limit =
        json!({ "type": "rate_limit_event", "rate_limit_info": { "status": "allowed", "resetsAt": 1_794_895_200 } });
    vec![
        init(),
        message_start("msg_1"),
        thinking("Reading first"),
        assistant("msg_1", json!({ "type": "thinking", "thinking": "Reading first", "signature": "x" })),
        assistant("msg_1", json!({ "type": "tool_use", "id": "toolu_1", "name": "TodoWrite", "input": todos })),
        assistant("msg_1", read),
        message_delta(17_000, 30),
        tool_result("toolu_1", false),
        tool_result("toolu_2", false),
        // A subagent's message is not the reply.
        subagent.to_string(),
        message_start("msg_2"),
        text("Hello"),
        text(" there"),
        assistant("msg_2", json!({ "type": "text", "text": "Hello there" })),
        message_delta(18_000, 20),
        limit.to_string(),
        result(0.25, json!({ "result": "Hello there" })),
    ]
}

#[tokio::test]
async fn a_claude_session_streams_its_turn_and_reports_spend() {
    let table = "mode = \"acceptEdits\"\nargs = [\"--setting-sources\", \"project\"]\n\
                 env = { CLAUDE_CODE_ENABLE_TODO_TOOLS = \"0\" }";
    let native = Fixture::new("claude-turn", table);
    let added = ok(2, json!({ "added": ["ants"], "removed": [], "errors": {} }));
    native.script(None, &[vec![signed_in()], vec![added], reading_turn(&native.dir)]);
    let stdio = McpServerStdio::new("ants", "/bin/ants")
        .args(vec!["mcp".to_owned()])
        .env(vec![EnvVariable::new("ANTS_MCP_TOKEN", "secret")]);
    let servers = vec![McpServer::Stdio(stdio)];
    let thread: ThreadId = "t1".parse().unwrap();
    let settings = ModelSettings {
        model: Some("haiku".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        ..Default::default()
    };
    let launch = native
        .launch(settings)
        .with_mcp(Arc::new(move |session: &crate::harness::McpSession<'_>| {
            assert!(session.http, "Claude Code takes HTTP MCP servers");
            servers.clone()
        }))
        .for_thread(&thread);

    let session = Session::start(&launch, None).await.unwrap();
    let named = HarnessAgent { name: "Claude Code".to_owned(), version: "2.1.325".to_owned() };
    assert_eq!(session.agent(), Some(&named));
    let id = session.id().to_string();
    assert!(uuid::Uuid::parse_str(&id).is_ok(), "an id of the translator's choosing: {id}");
    let (reply, updates, spends) = run(&session, Prompt::new("go")).await;
    assert_eq!(reply.unwrap(), "Hello there");
    session.close().await;

    let thoughts: Vec<_> = updates.iter().filter(|u| matches!(u, StatusUpdate::Thought { .. })).collect();
    assert_eq!(thoughts, [&StatusUpdate::Thought { text: "Reading first".to_owned() }], "streamed once");
    let steps = vec![
        PlanStep { content: "Writing a.txt".to_owned(), status: "in_progress".to_owned() },
        PlanStep { content: "Edit a.txt".to_owned(), status: "pending".to_owned() },
    ];
    assert!(updates.contains(&StatusUpdate::Plan { steps }), "{updates:?}");
    let read = StatusUpdate::Tool {
        id: "toolu_2".to_owned(),
        title: Some("Read src/a.rs".to_owned()),
        tool_kind: Some("read".to_owned()),
        status: Some("in_progress".to_owned()),
        paths: Some(vec![native.dir.join("src/a.rs").display().to_string()]),
    };
    assert!(updates.contains(&read), "{updates:?}");
    let tools = updates.iter().filter(|u| matches!(u, StatusUpdate::Tool { .. })).count();
    assert_eq!(tools, 2, "the Read call and its end; TodoWrite is the plan: {updates:?}");
    assert!(!updates.iter().any(|u| matches!(u, StatusUpdate::Message { text } if text.contains("inner"))));
    assert_eq!(spends, [Spend::Cost { session: SessionId::new(id.clone()), total: 0.25 }, Spend::Tokens(1154)]);

    let own = [&id, "--model", "haiku", "--effort", "high", "--permission-mode", "acceptEdits"];
    let mut args: Vec<&str> = FIRST_ARGS.to_vec();
    args.push("--session-id");
    args.extend(own);
    args.extend(["--allowedTools=mcp__ants", "--setting-sources", "project"]);
    assert_eq!(native.cli.args(2), args, "the protocol's arguments, then the definition's");
    assert_eq!(native.cli.args(1), ["--version"]);
    assert!(!native.cli.args(2).concat().contains("secret"), "the MCP token is not on the command line");
    assert_eq!(native.cli.env(2, "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC").as_deref(), Some("1"));
    assert_eq!(native.cli.env(2, "CLAUDE_CODE_ENABLE_TODO_TOOLS").as_deref(), Some("0"), "the definition's env wins");
    let received = native.received(2);
    assert_eq!(received[0]["request"], json!({ "subtype": "initialize", "hooks": null }));
    let mcp = json!({ "ants": { "type": "stdio", "command": "/bin/ants", "args": ["mcp"],
        "env": { "ANTS_MCP_TOKEN": "secret" } } });
    assert_eq!(received[1]["request"], json!({ "subtype": "mcp_set_servers", "servers": mcp }));
    assert_eq!(received[2]["message"], json!({ "role": "user", "content": [{ "type": "text", "text": "go" }] }));
    assert_eq!((&received[2]["client_composed"], &received[2]["session_id"]), (&json!(true), &json!(id)));

    native.script(None, &[vec![signed_in()]]);
    let checked = handshake(&launch).await;
    assert_eq!(checked.readiness, Readiness::Ready { agent: "Claude Code 2.1.325".to_owned() });
    assert!(!native.cli.args(4).iter().any(|arg| arg.starts_with("--allowedTools")), "no MCP servers for a check");
}

#[tokio::test]
async fn a_claude_turn_takes_steers_and_ends_once_idle_with_each_replayed() {
    let native = Fixture::new("claude-steer", "");
    let replay = "!echo {\"isReplay\":true,";
    native.script(None, &[]);
    native.script(
        Some(2),
        &[
            vec![signed_in()],
            vec![replay.to_owned(), state("running"), init(), text("Working")],
            vec![result(0.1, json!({})), replay.to_owned(), text("On both"), result(0.2, json!({})), state("idle")],
        ],
    );
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();
    let (ready, started) = oneshot::channel();
    let (steer_tx, steers) = mpsc::channel(1);
    let (progress, mut status) = mpsc::channel(16);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let prompt = Prompt {
        progress: Some(progress),
        spend: Some(spend),
        steering: Some(Steering { ready, steers }),
        ..Prompt::new("go")
    };
    let steer = async {
        started.await.expect("Claude Code takes steers");
        status.recv().await;
        let (accepted, took) = oneshot::channel();
        steer_tx.send(Steer { text: "and this".to_owned(), accepted }).await.unwrap();
        took.await.unwrap()
    };
    let (reply, took) = tokio::join!(session.run(prompt), steer);
    assert_eq!((reply.unwrap().text, took), ("Working\n\nOn both".to_owned(), true), "one answer, both results");
    session.close().await;
    let mut spends = Vec::new();
    while let Ok(spend) = spent.try_recv() {
        spends.push(spend);
    }
    assert!(spends.contains(&Spend::Tokens(2 * 1154)), "the usage of both results: {spends:?}");
    let received = native.received(2);
    assert_eq!(received[2]["message"]["content"], json!([{ "type": "text", "text": "and this" }]));
    assert_ne!(received[1]["uuid"], received[2]["uuid"]);
}

#[tokio::test]
async fn claude_sessions_resume_and_fork_counting_only_their_own_cost() {
    let native = Fixture::new("claude-fork", "");
    let launch = native.launch(ModelSettings::default());
    native.script(None, &[]);
    native.script(Some(2), &[vec![signed_in()], vec![init(), text("Back"), result(0.5, json!({}))]]);
    // The fork starts at its parent's 1.5 and spends 0.25.
    native.script(Some(4), &[vec![signed_in()], vec![result(1.5, json!({ "num_turns": 0 }))]]);
    native.script(Some(6), &[vec![signed_in()], vec![init(), text("Forked"), result(1.75, json!({}))]]);
    native.script(Some(8), &[vec![signed_in()], vec![init(), text("Restarted"), result(2.5, json!({}))]]);

    let resumed = Session::start(&launch, Some(SessionId::new("p1"))).await.unwrap();
    assert_eq!(resumed.id(), SessionId::new("p1"));
    assert_eq!(run(&resumed, Prompt::new("again")).await.0.unwrap(), "Back");
    resumed.close().await;
    assert_eq!(native.cli.args(2)[13..15], ["--resume", "p1"]);

    let forked = fork(&launch, &SessionId::new("p1")).await.unwrap().expect("Claude Code forks");
    let args = native.cli.args(4);
    assert_eq!(args[13..17], ["--resume", "p1", "--fork-session", "--session-id"]);
    let (fork_id, base) = cost_base(&forked.0);
    assert_eq!(args[17], fork_id, "the fork's id chosen up front");
    assert!(uuid::Uuid::parse_str(fork_id).is_ok(), "{forked}");
    assert_eq!((forked.to_string(), base), (format!("{fork_id}~1500000"), 1.5), "the parent's cost carried in the id");
    let written = json!({ "type": "user", "message": { "role": "user", "content": [] }, "shouldQuery": false });
    assert_eq!(native.received(4)[1], written, "the fork kept without a turn");

    let clone = Session::start(&launch, Some(forked.clone())).await.unwrap();
    let (reply, _, spends) = run(&clone, Prompt::new("go")).await;
    assert_eq!(reply.unwrap(), "Forked");
    assert_eq!(spends[0], Spend::Cost { session: forked.clone(), total: 0.25 }, "not the parent's cost again");
    clone.close().await;
    assert_eq!(native.cli.args(6)[13..15], ["--resume", fork_id]);

    // A daemon started afresh knows the fork's base from its stored id alone.
    let stored = SessionId::new("6e2dbd29-0bc6-45cb-b047-9e7ca523eb90~1500000");
    let restarted = Session::start(&launch, Some(stored.clone())).await.unwrap();
    let (reply, _, spends) = run(&restarted, Prompt::new("again")).await;
    assert_eq!(reply.unwrap(), "Restarted");
    assert_eq!(spends[0], Spend::Cost { session: stored, total: 1.0 });
    restarted.close().await;
    assert_eq!(native.cli.args(8)[13..15], ["--resume", "6e2dbd29-0bc6-45cb-b047-9e7ca523eb90"]);
}

#[tokio::test]
async fn claude_sessions_load_the_ants_skills_as_a_plugin_once_one_is_confirmed() {
    let native = Fixture::new("claude-skills", "");
    let skills = native.dir.join(".sense/skills");
    let offered = Skills { dir: skills.clone(), plugin: native.dir.join(".sense") };
    let launch = native.launch(ModelSettings::default()).with_skills(offered);
    native.script(None, &[]);
    native.script(Some(2), &[vec![signed_in()]]);
    native.script(Some(4), &[vec![signed_in()]]);

    Session::start(&launch, Some(SessionId::new("p1"))).await.unwrap().close().await;
    assert!(!native.cli.args(2).contains(&"--plugin-dir".to_owned()), "no skill confirmed yet");
    std::fs::create_dir_all(skills.join("deploy")).unwrap();
    std::fs::write(skills.join("deploy/SKILL.md"), "---\nname: deploy\ndescription: Deploy.\n---\n").unwrap();
    Session::start(&launch, Some(SessionId::new("p1"))).await.unwrap().close().await;
    let args = native.cli.args(4);
    let plugin = native.dir.join(".sense").display().to_string();
    assert!(args.windows(2).any(|pair| pair == ["--plugin-dir", plugin.as_str()]), "{args:?}");
}

#[tokio::test]
async fn a_cancelled_claude_turn_is_interrupted_and_the_session_goes_on() {
    let native = Fixture::new("claude-cancel", "");
    let marker = json!([{ "type": "text", "text": "[Request interrupted by user]" }]);
    let aborted = json!({ "subtype": "error_during_execution", "is_error": true, "terminal_reason": "aborted_streaming",
        "stop_reason": null });
    let interrupted = vec![
        ok(2, json!({ "still_queued": [] })),
        event(json!({ "type": "user", "message": { "role": "user", "content": marker } })),
        result(0.0, aborted),
    ];
    let after = vec![init(), text("Next"), result(0.5, json!({}))];
    native.script(None, &[vec![signed_in()], vec![init(), text("Working")], interrupted, after]);
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();
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
    assert!(session.is_alive(), "Claude Code ended the turn itself");
    assert_eq!(session.prompt("more").await.unwrap(), "Next");
    session.close().await;
    let interrupt = json!({ "type": "control_request", "request_id": "ants-2", "request": { "subtype": "interrupt" } });
    assert_eq!(native.received(2)[2], interrupt);
}

/// The text blocks of a prompt saying `text`.
fn said(text: &str) -> Vec<ContentBlock> {
    vec![ContentBlock::Text(TextContent::new(text))]
}

/// A session of `fixture`'s CLI with the ant's `settings`, opened without the translator.
async fn open_session(fixture: &Fixture, settings: &ModelSettings) -> ClaudeSession {
    let claude = Claude::start(Cli::new(&fixture.harness, &[], settings)).await.unwrap();
    let open = Open { cwd: fixture.dir.clone(), mcp_servers: Vec::new(), sign_ins: None, skills: None };
    claude.new_session(open).await.unwrap()
}

#[tokio::test]
async fn a_cancel_drops_the_steers_queued_behind_the_turn_where_claude_code_can() {
    let fixture = Fixture::new("claude-cancel-queued", "");
    let marker = json!([{ "type": "text", "text": "[Request interrupted by user]" }]);
    let aborted = json!({ "subtype": "error_during_execution", "is_error": true, "terminal_reason": "aborted_streaming",
        "stop_reason": null });
    let dropped = |n: u64| ok(n, json!({ "still_queued": [], "cancelled": ["queued"] }));
    fixture.script(
        None,
        &[
            vec![signed_in()],
            vec![state("running"), init_with(&["interrupt_receipt_v1", CANCEL_QUEUED]), text("Working")],
            // The steer waits in Claude Code's queue.
            vec![],
            vec![
                dropped(2),
                event(json!({ "type": "user", "message": { "role": "user", "content": marker } })),
                result(0.0, aborted),
                state("idle"),
            ],
            vec![state("running"), init_with(&[CANCEL_QUEUED]), text("Next"), result(0.5, json!({})), state("idle")],
            // Interrupted before Claude Code started its turn, the prompt never runs.
            vec![state("running")],
            vec![dropped(3), state("idle")],
        ],
    );
    let session = open_session(&fixture, &ModelSettings::default()).await;
    let (ours, _client) = Channel::duplex();
    let prompted = async |cx: ConnectionTo<Client>| {
        let updates = Updates::new(cx, SessionId::new("s"));
        let steered = async {
            while !session.cancel_queued.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(session.steer(said("and this")).await?, "the running turn takes the steer");
            assert_eq!(session.steers.locked().pending.len(), 1);
            session.cancel().await;
            assert!(session.steers.locked().pending.is_empty(), "the dropped steer is not waited for");
            Ok::<_, Error>(())
        };
        let (done, steered) = tokio::join!(session.prompt(said("go"), updates.clone()), steered);
        steered?;
        assert_eq!(done?.stop_reason, StopReason::Cancelled);
        assert_eq!(session.prompt(said("more"), updates.clone()).await?.stop_reason, StopReason::EndTurn);

        let started = std::time::Instant::now();
        let early = async {
            while !session.steers.locked().open {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            session.cancel().await;
        };
        let (done, ()) = tokio::join!(session.prompt(said("again"), updates), early);
        assert_eq!(done?.stop_reason, StopReason::Cancelled);
        assert!(started.elapsed() < INTERRUPT_GRACE, "the prompt ended at once, its CLI not killed");
        Ok(())
    };
    Agent.v1().name("test").connect_with(ours, prompted).await.unwrap();
    let received = fixture.received(2);
    let interrupt =
        |n: u64| control_request(&format!("ants-{n}"), &json!({ "subtype": "interrupt", "cancel_queued": true }));
    assert_eq!(received[2]["message"]["content"], json!([{ "type": "text", "text": "and this" }]));
    assert_eq!(received[3], interrupt(2));
    assert_eq!(received[4]["message"]["content"], json!([{ "type": "text", "text": "more" }]));
    assert_eq!(received[6], interrupt(3));
}

#[tokio::test]
async fn a_claude_session_switches_model_and_reasoning_effort_as_it_runs() {
    let fixture = Fixture::new("claude-configure", "");
    let unknown = json!({ "subtype": "error", "request_id": "ants-4", "error": "Model 'opus-9' not found",
        "error_code": "not_found" });
    fixture.script(
        None,
        &[
            vec![signed_in()],
            vec![ok(2, json!({ "model": "claude-sonnet-5-5" }))],
            vec![ok(3, Value::Null)],
            vec![json!({ "type": "control_response", "response": unknown }).to_string()],
        ],
    );
    let settings = ModelSettings {
        model: Some("haiku".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        ..Default::default()
    };
    let session = open_session(&fixture, &settings).await;
    session.configure(Setting::Model, "sonnet").await.unwrap();
    session.configure(Setting::ReasoningEffort, "low").await.unwrap();
    let refused = session.configure(Setting::ReasoningEffort, "extreme").await.unwrap_err();
    assert!(refused.message.contains("takes reasoning_effort low, medium, high, xhigh, max, not extreme"), "{refused}");
    let unknown = session.configure(Setting::Model, "opus-9").await.unwrap_err();
    assert!(unknown.message.contains("set_model: Model 'opus-9' not found"), "{unknown}");
    drop(session);

    let received = fixture.received(2);
    assert_eq!(received[1]["request"], json!({ "subtype": "set_model", "model": "sonnet" }));
    let effort = json!({ "subtype": "apply_flag_settings", "settings": { "effortLevel": "low" } });
    assert_eq!(received[2]["request"], effort);
    assert_eq!(received[3]["request"], json!({ "subtype": "set_model", "model": "opus-9" }), "no unknown effort sent");
    let args = fixture.cli.args(2);
    assert!(args.windows(2).any(|pair| pair == ["--model", "haiku"]), "opened with the ant's model: {args:?}");
}

/// The answer of a session of a harness with `table`, on a launch with `allow` rules, to Claude Code's request to run
/// `touch x`, and the arguments the CLI got.
async fn approval(name: &str, table: &str, allow: &[&str]) -> (Value, Vec<String>) {
    let native = Fixture::new(name, table);
    let ask = json!({ "type": "control_request", "request_id": "c1", "request": { "subtype": "can_use_tool",
        "tool_name": "Bash", "input": { "command": "touch x" }, "tool_use_id": "toolu_1" } });
    let denied = event(json!({ "type": "system", "subtype": "permission_denied", "tool_name": "Write",
        "tool_use_id": "toolu_2", "message": "Claude requested permissions to use Write, but you haven't granted it yet." }));
    let turn = vec![init(), denied, ask.to_string()];
    native.script(None, &[vec![signed_in()], turn, vec![text("Done"), result(0.1, json!({}))]]);
    let policy = PermissionPolicy::new(Allowlist::parse(allow).unwrap(), None);
    let session = Session::start(&native.launch(ModelSettings::default()).with_policy(policy), None).await.unwrap();
    assert_eq!(session.prompt("clean up").await.unwrap(), "Done");
    session.close().await;
    let response = native.received(2)[2]["response"].clone();
    assert_eq!(response["request_id"], "c1");
    (response["response"].clone(), native.cli.args(2))
}

#[tokio::test]
async fn a_tool_request_of_claude_is_denied_unless_the_ant_asks_ants() {
    let (denied, args) = approval("claude-approval", "mode = \"default\"", &["*"]).await;
    assert_eq!(denied["behavior"], "deny", "though every tool is allowed");
    assert!(denied["message"].as_str().unwrap().contains("its permission settings allow"), "{denied}");
    assert_eq!(args[..FIRST_ARGS.len()], FIRST_ARGS);

    let asks = "mode = \"auto\"\ntools = \"ask\"";
    let (allowed, args) = approval("claude-approval-allowed", asks, &["execute"]).await;
    assert_eq!(allowed, json!({ "behavior": "allow", "updatedInput": { "command": "touch x" } }));
    let prompts = ["--permission-prompts", "host", "--permission-prompt-tool", "stdio"];
    assert_eq!(args[FIRST_ARGS.len() - 2..FIRST_ARGS.len() + 2], prompts);
    let (rejected, _) = approval("claude-approval-rejected", asks, &["read"]).await;
    assert_eq!(rejected["behavior"], "deny");
    assert!(rejected["message"].as_str().unwrap().contains("approvers did not allow"), "{rejected}");
}

#[tokio::test]
async fn claude_limit_errors_are_classified() {
    let native = Fixture::new("claude-limits", "");
    let launch = native.launch(ModelSettings::default());
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    let info = json!({ "status": "rejected", "resetsAt": now.as_secs() + 3 * 3600, "rateLimitType": "five_hour" });
    let mut session_limit =
        failure("rate_limit", Some(429), "You've hit your session limit · resets 3:45pm", "api_error");
    session_limit.insert(1, json!({ "type": "rate_limit_event", "rate_limit_info": info }).to_string());
    let capacity = "API Error: Server is temporarily limiting requests (not your usage limit)";
    let overloaded = "API Error: Repeated 529 Overloaded errors. The API is at capacity";
    let cases = [
        (session_limit, LimitKind::Quota, true),
        (failure("rate_limit", Some(429), capacity, "api_error"), LimitKind::RateLimit, false),
        (failure("overloaded", Some(529), overloaded, "api_error"), LimitKind::RateLimit, false),
        (failure("billing_error", Some(400), "Credit balance is too low", "api_error"), LimitKind::Quota, false),
    ];
    for (turn, kind, hinted) in cases {
        native.script(None, &[vec![signed_in()], turn]);
        let session = Session::start(&launch, None).await.unwrap();
        let err = session.prompt("go").await.unwrap_err();
        let limited = err.downcast_ref::<Limited>().expect("a provider limit");
        assert_eq!(limited.kind, kind, "{err:#}");
        assert_eq!(limited.retry_after.is_some_and(|wait| wait > Duration::from_hours(2)), hinted, "{err:#}");
        session.close().await;
    }
}

#[tokio::test]
async fn a_claude_prompt_outgrowing_the_context_runs_again_in_a_new_session() {
    let native = Fixture::new("claude-overflow", "");
    let overflow = failure("invalid_request", Some(400), "Prompt is too long", "prompt_too_long");
    native.script(None, &[]);
    native.script(Some(2), &[vec![signed_in()], overflow]);
    native.script(Some(3), &[vec![signed_in()], vec![init(), text("Fine"), result(0.1, json!({}))]]);
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();
    let first = session.id();
    assert_eq!(session.prompt("go").await.unwrap(), "Fine");
    assert_ne!(session.id(), first);
    let started = std::time::Instant::now();
    while !native.cli.ended(2) {
        assert!(started.elapsed() < Duration::from_secs(10), "the abandoned session's CLI still runs");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!native.cli.ended(3), "the new session's CLI runs on");
    session.close().await;
    assert_eq!(native.cli.runs(), 3, "a CLI for each session");
}

#[tokio::test]
async fn a_signed_out_claude_is_told_apart_from_a_failing_one() {
    let native = Fixture::new("claude-signin", "");
    let launch = native.launch(ModelSettings::default());
    let account = json!({ "account": { "tokenSource": "none", "apiProvider": "firstParty" } });
    native.script(None, &[vec![ok(1, account)]]);
    let checked = handshake(&launch).await;
    assert_matches!(checked.readiness, Readiness::SignedOut { .. });
    assert_eq!(checked.sign_in.as_deref(), Some("claude login"));

    let bad_key = failure("authentication_failed", Some(401), "Invalid API key · Fix external API key", "api_error");
    native.script(None, &[vec![signed_in()], bad_key]);
    let session = Session::start(&launch, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    assert!(err.downcast_ref::<SignedOut>().is_some(), "{err:#}");
    session.close().await;

    let in_use = "Error: Session ID 6e2dbd29-0bc6-45cb-b047-9e7ca523eb90 is already in use.";
    native.script(None, &[vec![format!("!stderr {in_use}"), "!exit 1".to_owned()]]);
    let err = Session::start(&launch, None).await.err().expect("a failing CLI");
    let failed = err.downcast_ref::<StartFailed>().expect("a startup failure");
    assert!(failed.detail.contains(&format!("exited with exit status: 1: {in_use}")), "{}", failed.detail);

    native.cli.script(None, &[["2.0.1 (Claude Code)"]]);
    let Readiness::Failed { detail } = handshake(&launch).await.readiness else { panic!("an old CLI") };
    assert!(detail.contains("Claude Code 2.0.1 is older than 2.1.325"), "{detail}");
}

#[tokio::test]
async fn an_api_key_refused_at_a_prompt_keeps_claude_signed_out_until_it_changes() {
    let native = Fixture::new("claude-refused", "");
    let bad_key = failure("authentication_failed", Some(401), "Invalid API key · Fix external API key", "api_error");
    native.script(None, &[vec![signed_in()], bad_key]);
    let revoked = native.launch_with_key("sk-ant-revoked", ModelSettings::default());
    let session = Session::start(&revoked, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    assert!(err.downcast_ref::<SignedOut>().is_some(), "{err:#}");
    session.close().await;

    native.script(None, &[vec![signed_in()]]);
    let Readiness::SignedOut { detail } = handshake(&revoked).await.readiness else {
        panic!("signed out although the session opens with the refused key")
    };
    assert!(detail.contains("Invalid API key") && detail.contains("ANTHROPIC_API_KEY unchanged since"), "{detail}");
    let replaced = native.launch_with_key("sk-ant-replaced", ModelSettings::default());
    assert_matches!(handshake(&replaced).await.readiness, Readiness::Ready { .. });
}

#[test]
fn claude_errors_reach_the_daemons_classifiers() {
    let harness = define("command = \"claude\"").unwrap();
    let classify = |error: &Error| crate::limits::ratelimit::classify(&harness, &error.to_string()).map(|l| l.kind);
    let failed = |message: &str, status: Option<u16>, terminal: &str, kind: Option<&str>| {
        let result =
            json!({ "is_error": true, "result": message, "api_error_status": status, "terminal_reason": terminal });
        error(&result, kind, None, None)
    };
    let login = failed("Not logged in · Please run /login", None, "api_error", Some("authentication_failed"));
    assert_eq!(login.code, ErrorCode::AUTH_REQUIRED);
    let expired = failed("Please run /login · API Error: 401 OAuth token has expired", Some(401), "api_error", None);
    assert_eq!(expired.code, ErrorCode::AUTH_REQUIRED);
    let weekly = failed("You've hit your weekly limit · resets Dec 1", Some(429), "api_error", Some("rate_limit"));
    assert_eq!(classify(&weekly), Some(LimitKind::Quota));
    let capacity = "API Error: Request rejected (429) · this may be a temporary capacity issue";
    assert_eq!(classify(&failed(capacity, Some(429), "api_error", Some("rate_limit"))), Some(LimitKind::RateLimit));
    let too_long = failed("Prompt is too long", Some(400), "prompt_too_long", Some("invalid_request"));
    assert!(crate::limits::overflow::overflows(&harness, &too_long.to_string()));
    let unknown = json!({ "is_error": true, "errors": ["No conversation found with session ID: x"] });
    let other = error(&unknown, None, None, None);
    assert_eq!(
        (other.code, other.message.as_str()),
        (ErrorCode::INTERNAL_ERROR, "No conversation found with session ID: x")
    );
}

#[test]
fn claude_definitions_are_checked() {
    let err = |table: &str| format!("{:#}", define(&format!("command = \"claude\"\n{table}")).unwrap_err());
    let mode = err("mode = \"ask\"");
    let modes = "default, manual, acceptEdits, plan, auto, dontAsk or bypassPermissions, not ask";
    assert!(mode.contains(&format!("protocol claude takes mode {modes}")), "{mode}");
    let own = err("args = [\"--permission-mode\", \"plan\"]");
    assert!(own.contains("protocol claude passes --permission-mode itself"), "{own}");
    let context = err("[harnesses.claude-native.context]\nx = { args = [\"--resume=abc\"] }");
    assert!(context.contains("passes --resume itself"), "{context}");
    for mode in MODES {
        assert!(define(&format!("command = \"claude\"\nmode = {mode:?}")).unwrap().bypasses_permissions());
    }
    assert!(define("command = \"claude\"\nargs = [\"--allowedTools=Read\", \"-p\"]").is_ok());
}

#[tokio::test]
async fn a_prompt_dropped_mid_turn_leaves_no_turn_running() {
    let fixture = Fixture::new("claude-dropped-prompt", "");
    fixture.script(None, &[vec![signed_in()], vec![init(), text("Hel")]]);
    let session = open_session(&fixture, &ModelSettings::default()).await;
    // The session is prompted without the translator, whose own task of a prompt nothing drops.
    let (ours, _client) = Channel::duplex();
    let dropped = async |cx: ConnectionTo<Client>| {
        let updates = Updates::new(cx, SessionId::new("s"));
        let mut prompt = Box::pin(session.prompt(said("hello"), updates));
        let turn_runs = async {
            while !session.steers.locked().open {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            done = &mut prompt => panic!("the turn has no result: {done:?}"),
            () = turn_runs => {}
        }
        drop(prompt);
        assert!(session.running.borrow().is_none(), "no turn to interrupt");
        assert!(session.shared.asking.locked().is_none(), "nowhere to send tool requests");
        assert!(!session.steer(said("more")).await?, "no turn takes a steer");
        Ok(())
    };
    Agent.v1().name("test").connect_with(ours, dropped).await.unwrap();
}

#[tokio::test]
async fn servers_of_mcp_config_in_args_join_the_ants_servers_of_every_session() {
    let mut native = Fixture::new("claude-mcp-config", "");
    let (first, second) = (native.dir.join("first.json"), native.dir.join("second.json"));
    let first_servers = json!({ "mcpServers": {
        "iconly": { "type": "http", "url": "https://old.example/mcp" },
        "ants": { "type": "http", "url": "https://fake.example/mcp" },
    } });
    std::fs::write(&first, first_servers.to_string()).unwrap();
    std::fs::write(&second, json!({ "mcpServers": { "docs": { "command": "docs-mcp" } } }).to_string()).unwrap();
    let inline = json!({ "mcpServers": { "iconly": { "type": "http", "url": "https://icons.example/mcp",
        "headers": { "Authorization": "Bearer x" } } } });
    let table = format!(
        "command = {:?}\nargs = [\"--mcp-config\", {:?}, {:?}, \"--mcp-config={}\", \"--allowedTools=mcp__iconly\"]",
        native.cli.path(),
        first.display().to_string(),
        second.display().to_string(),
        inline.to_string().replace('\\', "\\\\").replace('"', "\\\"")
    );
    native.0.harness = define(&table).unwrap();
    let added = ok(2, json!({ "added": [], "removed": [], "errors": {} }));
    native.script(None, &[vec![signed_in()], vec![added.clone()]]);
    native.script(Some(6), &[vec![signed_in()], vec![added], vec![result(0.0, json!({ "num_turns": 0 }))]]);
    let servers = vec![McpServer::Stdio(McpServerStdio::new("ants", "/bin/ants"))];
    let thread: ThreadId = "t1".parse().unwrap();
    let launch = native
        .launch(ModelSettings::default())
        .with_mcp(Arc::new(move |_: &crate::harness::McpSession<'_>| servers.clone()))
        .for_thread(&thread);

    Session::start(&launch, None).await.unwrap().close().await;
    Session::start(&launch, Some(SessionId::new("p1"))).await.unwrap().close().await;
    fork(&launch, &SessionId::new("p1")).await.unwrap().expect("Claude Code forks");

    let args = native.cli.args(2);
    assert!(!args.iter().any(|arg| arg.starts_with("--mcp-config") || arg.ends_with(".json")), "taken out: {args:?}");
    assert!(args.iter().any(|arg| arg == "--allowedTools=mcp__iconly"), "{args:?}");
    let from_args = json!({ "iconly": inline["mcpServers"]["iconly"], "docs": { "command": "docs-mcp" } });
    let mut with_ants = from_args.clone();
    with_ants["ants"] = json!({ "type": "stdio", "command": "/bin/ants", "args": [], "env": {} });
    let mut alone = from_args;
    alone["ants"] = first_servers["mcpServers"]["ants"].clone();
    for (run, servers) in [(2, &with_ants), (4, &with_ants), (6, &alone)] {
        let set = json!({ "subtype": "mcp_set_servers", "servers": servers });
        assert_eq!(native.received(run)[1]["request"], set, "run {run}: new, resumed, forked");
    }
}

#[tokio::test]
async fn a_bad_mcp_config_in_args_fails_the_start() {
    let scratch = ants_test_support::scratch_dir("claude-bad-mcp-config-files").canonicalize().unwrap();
    let bad_file = scratch.join("bad.json");
    std::fs::write(&bad_file, "{not json").unwrap();
    let bad_file = bad_file.display().to_string();
    let configs: [&[&str]; 8] = [
        &["--mcp-config", "{not json"],
        &["--mcp-config", "/nonexistent/mcp.json"],
        &["--mcp-config", &bad_file],
        &["--mcp-config", r#"{"servers": {}}"#],
        &["--mcp-config", r#"{"mcpServers": {"a": 1}}"#],
        &["--mcp-config"],
        &["--mcp-config="],
        &["--mcp-config", "--verbose"],
    ];
    for args in configs {
        let mut native = Fixture::new("claude-bad-mcp-config", "");
        native.0.harness = define(&format!("command = {:?}\nargs = {args:?}", native.cli.path())).unwrap();
        native.script(None, &[vec![signed_in()]]);
        let error = Session::start(&native.launch(ModelSettings::default()), None).await.err().unwrap();
        assert!(format!("{error:#}").contains("--mcp-config"), "{args:?}: {error:#}");
    }
    let _ = std::fs::remove_dir_all(scratch);
}

/// The command, quoted for a shell, starting `program` in a session of its own (`setsid`), as Claude Code runs a
/// command in the background.
fn setsid(program: &str) -> String {
    format!("perl -MPOSIX -e 'POSIX::setsid() or die; exec @ARGV' {program}")
}

/// A process this test started, killed when dropped, should the test fail before it is gone.
struct Killed(rustix::process::Pid);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = rustix::process::kill_process(self.0, rustix::process::Signal::KILL);
    }
}

/// The pid written to `file` in `dir`, once it is.
async fn written_pid(dir: &std::path::Path, file: &str) -> rustix::process::Pid {
    let path = dir.join(file);
    let mut pid = None;
    ants_test_support::eventually_within(Duration::from_secs(10), file, async || {
        pid = std::fs::read_to_string(&path).ok().and_then(|text| text.trim().parse().ok());
        pid.is_some()
    })
    .await;
    rustix::process::Pid::from_raw(pid.unwrap()).unwrap()
}

/// Wait for process `pid` to be gone, within the grace period of what a session left running and a second.
async fn gone(pid: rustix::process::Pid, what: &str) {
    let limit = crate::harness::native::leftovers::GRACE + Duration::from_secs(1);
    ants_test_support::eventually_every(Duration::from_millis(20), limit, what, async || {
        rustix::process::test_kill_process(pid).is_err()
    })
    .await;
}

#[tokio::test]
async fn a_closed_claude_session_ends_what_it_ran_in_the_background() {
    if !ants_test_support::have("perl") {
        return;
    }
    let native = Fixture::new("claude-leftovers", "");
    let pid = native.dir.join("child.pid");
    let background = format!("!bg echo $$ > '{}'; exec {}", pid.display(), setsid("sh -c 'while :; do sleep 1; done'"));
    native.script(None, &[vec![background, signed_in()]]);
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();
    let child = Killed(written_pid(&native.dir, "child.pid").await);
    assert!(native.cli.env(2, crate::harness::native::leftovers::TAG).is_some(), "the CLI's run is tagged");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rustix::process::test_kill_process(child.0).is_ok(), "the loop runs while the session does");
    session.close().await;
    gone(child.0, "the loop run in the background").await;
}

#[tokio::test]
async fn what_a_killed_claude_left_running_ends_once_its_session_is_released() {
    if !ants_test_support::have("perl") {
        return;
    }
    let native = Fixture::new("claude-killed-leftovers", "");
    let looping = crate::harness::native::leftovers::tests::looping();
    let program = std::iter::once(looping.get_program())
        .chain(looping.get_args())
        .map(|arg| format!("'{}'", arg.to_str().unwrap()))
        .collect::<Vec<_>>()
        .join(" ");
    let (cli, child) = (native.dir.join("cli.pid"), native.dir.join("child.pid"));
    let orphan = format!(
        "!sh echo $PPID > '{}'; ANTS_TEST_LOOP=1 {} > /dev/null 2>&1 & echo $! > '{}'",
        cli.display(),
        setsid(&program),
        child.display()
    );
    native.script(None, &[vec![orphan, signed_in()]]);
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();
    let cli = written_pid(&native.dir, "cli.pid").await;
    let child = Killed(written_pid(&native.dir, "child.pid").await);
    rustix::process::kill_process(cli, rustix::process::Signal::KILL).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(rustix::process::test_kill_process(child.0).is_ok(), "the loop outlives the CLI, reparented");
    session.close().await;
    gone(child.0, "the loop left running").await;
}
