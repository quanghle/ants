//! The copilot-sdk translator against a Copilot CLI server played here ([`Played`]) with the shapes Copilot CLI 1.0.112
//! sends: the definition's command is a relay script passing its stdio through two FIFOs, so the SDK speaks to the
//! played server as to `copilot --server --stdio`.

use std::assert_matches;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::v1::schema::{EnvVariable, McpServer, McpServerStdio, SessionId};
use ants_proto::{LimitKind, StatusUpdate, ThreadId};
use ants_test_support::scratch_dir;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::unix::pipe;
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::harness::{
    Harnesses, Launch, ModelSettings, Prompt, Readiness, Session, SignedOut, Spend, Steer, Steering, fork, handshake,
    parse_definitions,
};
use crate::limits::ratelimit::Limited;
use crate::permission::{Allowlist, PermissionPolicy};

/// How the played server answers a request: its result or error message, the notifications it sends next, and whether
/// it then closes its output, as a CLI exiting does.
struct Answer {
    result: Result<Value, String>,
    then: Vec<Value>,
    close: bool,
}

impl Answer {
    fn ok(result: Value) -> Self {
        Self { result: Ok(result), then: Vec::new(), close: false }
    }

    fn then(mut self, notifications: Vec<Value>) -> Self {
        self.then = notifications;
        self
    }
}

/// What a test plays beyond the defaults ([`default_answer`]): an answer to a request, by method and params.
type Script = Box<dyn FnMut(&str, &Value) -> Option<Answer> + Send>;

/// The requests the played server read, as method and params, over every run of the CLI.
type Requests = Arc<Mutex<Vec<(String, Value)>>>;

/// The answer of Copilot CLI 1.0.112, signed in, to the requests a test does not script.
fn default_answer(method: &str, params: &Value) -> Answer {
    Answer::ok(match method {
        "connect" => json!({ "ok": true, "protocolVersion": 3, "version": "1.0.112" }),
        "status.get" => json!({ "version": "1.0.112", "protocolVersion": 3 }),
        "auth.getStatus" => json!({ "isAuthenticated": true, "authType": "user", "login": "ant" }),
        "session.create" | "session.resume" => json!({ "sessionId": params["sessionId"], "mode": "interactive" }),
        "session.skills.reload" => json!({ "warnings": [], "errors": [] }),
        "sessions.fork" => json!({ "sessionId": "forked", "name": "go (fork)" }),
        "session.send" => json!({ "messageId": "u1" }),
        "session.detach" => json!({ "success": true }),
        _ => json!({}),
    })
}

/// A `session.event` notification of session `id`, with `fields` (`agentId`, `ephemeral`) added to the event.
fn event_of(id: &Value, kind: &str, data: Value, fields: &Value) -> Value {
    let mut event =
        json!({ "type": kind, "data": data, "id": kind, "timestamp": "2026-11-16T07:00:00Z", "parentId": null });
    if let (Some(event), Some(fields)) = (event.as_object_mut(), fields.as_object()) {
        event.extend(fields.clone());
    }
    json!({ "jsonrpc": "2.0", "method": "session.event", "params": { "sessionId": id, "event": event } })
}

fn event(id: &Value, kind: &str, data: Value) -> Value {
    event_of(id, kind, data, &json!({}))
}

/// The events ending a turn.
fn idle(id: &Value, aborted: bool) -> Vec<Value> {
    let data = if aborted { json!({ "aborted": true }) } else { json!({}) };
    vec![event(id, "assistant.idle", data.clone()), event(id, "session.idle", data)]
}

/// Read a `Content-Length` framed message; None at the end of the input.
async fn read_frame(reader: &mut BufReader<pipe::Receiver>) -> Option<Value> {
    let mut length = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        match line.trim().strip_prefix("Content-Length:") {
            Some(n) => length = n.trim().parse::<usize>().ok(),
            None if line.trim().is_empty() => break,
            None => {}
        }
    }
    let mut body = vec![0; length?];
    reader.read_exact(&mut body).await.ok()?;
    serde_json::from_slice(&body).ok()
}

async fn write_frame(writer: &mut pipe::Sender, message: &Value) {
    let body = message.to_string();
    let _ = writer.write_all(format!("Content-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await;
}

/// Serve run `run` of the CLI, until its input ends or an answer closes it.
async fn serve(dir: PathBuf, run: usize, script: Arc<Mutex<Script>>, requests: Requests) {
    let receiver = pipe::OpenOptions::new().open_receiver(dir.join(format!("in.{run}"))).unwrap();
    // The relay reads its output once it opened its input: nothing it writes is lost.
    let mut sender = loop {
        match pipe::OpenOptions::new().open_sender(dir.join(format!("out.{run}"))) {
            Ok(sender) => break sender,
            Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    };
    let mut reader = BufReader::new(receiver);
    while let Some(message) = read_frame(&mut reader).await {
        let (Some(id), Some(method)) = (message.get("id"), message["method"].as_str()) else { continue };
        let params = message.get("params").cloned().unwrap_or_default();
        requests.lock().unwrap().push((method.to_owned(), params.clone()));
        let answer = (script.lock().unwrap())(method, &params).unwrap_or_else(|| default_answer(method, &params));
        let response = match answer.result {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(message) => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": message } }),
        };
        write_frame(&mut sender, &response).await;
        for notification in &answer.then {
            write_frame(&mut sender, notification).await;
        }
        if answer.close {
            return;
        }
    }
}

/// A harness of protocol copilot-sdk whose command relays its stdio to a Copilot CLI server played here, in a scratch
/// directory removed on drop.
struct Played {
    dir: PathBuf,
    harness: Arc<Harness>,
    requests: Requests,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Played {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Played {
    /// `[harnesses.native]` of protocol copilot-sdk with `table` added, its CLI answering as `script` says.
    fn new(name: &str, table: &str, script: impl FnMut(&str, &Value) -> Option<Answer> + Send + 'static) -> Self {
        let dir = scratch_dir(name).canonicalize().unwrap();
        let relay = dir.join("copilot");
        let text = format!(
            "#!/bin/sh\nd='{}'\nn=$(($(cat \"$d/runs\" 2>/dev/null || echo 0) + 1))\necho $n > \"$d/runs\"\n\
             printf '%s\\n' \"$@\" > \"$d/args.$n\"\n[ -f \"$d/stderr\" ] && cat \"$d/stderr\" >&2 && exit 1\n\
             mkfifo \"$d/in.$n\" \"$d/out.$n\"\n\
             exec 3>\"$d/in.$n\"\ncat \"$d/out.$n\" 3>&- &\nexec cat >&3\n",
            dir.display()
        );
        std::fs::write(&relay, text).unwrap();
        std::fs::set_permissions(&relay, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        let definition = format!(
            "[harnesses.native]\ncommand = {relay:?}\nprotocol = \"copilot-sdk\"\nsign_in = \"copilot login\"\n{table}"
        );
        let harness = Harnesses::builtin().define(parse_definitions(&definition).unwrap()).unwrap().get("native");
        let requests = Requests::default();
        let script: Arc<Mutex<Script>> = Arc::new(Mutex::new(Box::new(script)));
        let (served, read) = (dir.clone(), requests.clone());
        let server = tokio::spawn(async move {
            let mut runs = tokio::task::JoinSet::new();
            let mut run = 0;
            loop {
                run += 1;
                while !served.join(format!("out.{run}")).exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                runs.spawn(serve(served.clone(), run, script.clone(), read.clone()));
            }
        });
        Self { dir, harness: harness.unwrap(), requests, server }
    }

    /// A launch of the harness working in the scratch directory, with the ant's `settings`.
    fn launch(&self, settings: ModelSettings) -> Launch {
        let allow_all = PermissionPolicy::new(Allowlist::parse(&["*"]).unwrap(), None);
        Launch::new(self.harness.clone(), self.dir.clone(), vec![], allow_all, settings)
    }

    /// The params of the requests of `method` read so far.
    fn requests(&self, method: &str) -> Vec<Value> {
        let requests = self.requests.lock().unwrap();
        requests.iter().filter(|(m, _)| m == method).map(|(_, params)| params.clone()).collect()
    }

    /// The arguments of run `run` of the CLI.
    fn args(&self, run: usize) -> Vec<String> {
        let args = std::fs::read_to_string(self.dir.join(format!("args.{run}"))).unwrap();
        args.lines().map(str::to_owned).collect()
    }
}

/// The events of a turn reading a file, the shapes of Copilot CLI 1.0.112: reasoning and a message streamed and then
/// sent whole, a tool call, a sub-agent's message, a message sent only whole, an event of a type the SDK does not know,
/// token use and cost.
fn reading(id: &Value) -> Vec<Value> {
    let ephemeral = json!({ "ephemeral": true });
    let mut events = vec![
        event_of(id, "session.usage_info", json!({ "tokenLimit": 136_000, "currentTokens": 12_710 }), &ephemeral),
        event(id, "assistant.reasoning_delta", json!({ "reasoningId": "r1", "deltaContent": "Reading" })),
        event(id, "assistant.reasoning", json!({ "reasoningId": "r1", "content": "Reading" })),
        event(id, "model.turn_started", json!({ "kind": "turn_started" })),
        event(id, "assistant.message_delta", json!({ "messageId": "a1", "deltaContent": "Let me look." })),
        event(id, "assistant.message", json!({ "messageId": "a1", "content": "Let me look.", "toolRequests": [] })),
        event(
            id,
            "tool.execution_start",
            json!({
                "toolCallId": "t1", "toolName": "view", "toolTitle": "View file", "arguments": { "path": "/w/a.md" }
            }),
        ),
        event(
            id,
            "tool.execution_complete",
            json!({ "toolCallId": "t1", "success": true, "result": { "content": "#" } }),
        ),
        event_of(
            id,
            "assistant.message_delta",
            json!({ "messageId": "s1", "deltaContent": "noise" }),
            &json!({ "agentId": "x" }),
        ),
        event(id, "assistant.message", json!({ "messageId": "a2", "content": "It is a heading." })),
        event(
            id,
            "assistant.usage",
            json!({
                "inputTokens": 13_930, "outputTokens": 71, "cacheReadTokens": 100, "reasoningTokens": 49, "cost": 0.33
            }),
        ),
        event(
            id,
            "session.usage_checkpoint",
            json!({ "totalNanoAiu": 1_776_500_000_u64, "totalPremiumRequests": 0.33 }),
        ),
    ];
    events.extend(idle(id, false));
    events
}

#[tokio::test]
async fn copilot_turns_stream_through_the_translator() {
    let extra = r#"'{"mcpServers": {"github": {"type": "http", "url": "https://api.example/mcp"}, "ants": {"command": "/old"}}}'"#;
    let table = format!(
        "args = [\"--log-level\", \"debug\", \"--excluded-tools\", \"bash\", \"read_bash,write_bash\", \
         \"--additional-mcp-config\", {extra}]\n[harnesses.native.context]\nlong = {{ args = [\"--context=long_context\"] }}"
    );
    let played = Played::new("copilot-prompt", &table, |method, params| {
        (method == "session.send").then(|| Answer::ok(json!({ "messageId": "u1" })).then(reading(&params["sessionId"])))
    });
    let stdio = McpServerStdio::new("ants", "/bin/ants")
        .args(vec!["mcp".to_owned()])
        .env(vec![EnvVariable::new("ANTS_MCP_TOKEN", "secret")]);
    let servers = vec![McpServer::Stdio(stdio)];
    let settings = ModelSettings {
        model: Some("claude-haiku-4.5".to_owned()),
        reasoning_effort: Some("low".to_owned()),
        context: Some("long".to_owned()),
        ..Default::default()
    };
    let thread: ThreadId = "t1".parse().unwrap();
    let launch = played
        .launch(settings)
        .with_mcp(Arc::new(move |_: &crate::harness::McpSession<'_>| servers.clone()))
        .for_thread(&thread);

    let session = Session::start(&launch, None).await.unwrap();
    let agent = session.agent().unwrap();
    assert_eq!((agent.name.as_str(), agent.version.as_str()), ("GitHub Copilot CLI", "1.0.112"));
    let (progress, mut status) = mpsc::channel(64);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let prompt = Prompt { progress: Some(progress), spend: Some(spend), ..Prompt::new("What is this?") };
    assert_eq!(session.run(prompt).await.unwrap().text, "Let me look.\n\nIt is a heading.");
    let id = session.id();
    session.close().await;

    let mut updates = Vec::new();
    while let Ok(update) = status.try_recv() {
        updates.push(update);
    }
    assert_eq!(updates.iter().filter(|u| matches!(u, StatusUpdate::Thought { .. })).count(), 1, "{updates:?}");
    let viewed = |u: &StatusUpdate| {
        matches!(u, StatusUpdate::Tool { id, title, paths: Some(paths), .. }
            if id == "t1" && title.as_deref() == Some("View file") && paths == &["/w/a.md"])
    };
    assert!(updates.iter().any(viewed), "{updates:?}");
    let mut spends = Vec::new();
    while let Ok(spend) = spent.try_recv() {
        spends.push(spend);
    }
    let cost = 1_776_500_000.0 * USD_PER_NANO_AIU;
    assert_eq!(spends, [Spend::Cost { session: id.clone(), total: cost }, Spend::Tokens(14_001)]);

    assert_eq!(played.args(1), [&SERVER_ARGS[..], &["--log-level", "debug"]].concat(), "no session settings or MCP");
    let created = &played.requests("session.create")[0];
    assert_eq!(created["sessionId"], json!(*id.0));
    let keys = ["model", "reasoningEffort", "contextTier", "excludedTools", "streaming", "enableSessionTelemetry"];
    let tools = json!(["bash", "read_bash", "write_bash"]);
    let expected = [json!("claude-haiku-4.5"), json!("low"), json!("long_context"), tools, json!(true), json!(false)];
    assert_eq!(keys.map(|key| created[key].clone()), expected);
    assert_eq!((&created["workingDirectory"], &created["requestPermission"]), (&json!(played.dir), &json!(true)));
    let mcp = json!({
        "github": { "type": "http", "url": "https://api.example/mcp" },
        "ants": { "type": "stdio", "command": "/bin/ants", "args": ["mcp"], "env": { "ANTS_MCP_TOKEN": "secret" } }
    });
    assert_eq!(created["mcpServers"], mcp);
    assert_eq!(played.requests("session.send")[0]["prompt"], "What is this?");

    let checked = handshake(&launch).await;
    assert_eq!(checked.readiness, Readiness::Ready { agent: "GitHub Copilot CLI 1.0.112".to_owned() });
}

#[tokio::test]
async fn copilot_sessions_resume_and_fork_by_id_counting_only_their_own_cost() {
    // The fork's accounting starts at its parent's 0.5 AI credits; it spends 0.25 more.
    let played = Played::new("copilot-sessions", "", |method, params| match method {
        "session.usage.getMetrics" if params["sessionId"] == "forked" => {
            Some(Answer::ok(json!({ "totalNanoAiu": 500_000_000_u64, "aiCreditsStatus": "complete" })))
        }
        "session.send" => {
            let id = &params["sessionId"];
            let checkpoint = json!({ "totalNanoAiu": 750_000_000_u64 });
            let mut events = vec![
                event(id, "assistant.message", json!({ "messageId": "a1", "content": "Forked" })),
                event(id, "session.usage_checkpoint", checkpoint),
            ];
            events.extend(idle(id, false));
            Some(default_answer(method, params).then(events))
        }
        _ => None,
    });
    let launch = played.launch(ModelSettings::default());
    let resumed = Session::start(&launch, Some(SessionId::new("s1"))).await.unwrap();
    assert_eq!(resumed.id(), SessionId::new("s1"));
    resumed.close().await;
    let forked = fork(&launch, &SessionId::new("s1")).await.unwrap().expect("the CLI forks");
    assert_eq!(forked, SessionId::new("forked~5000"), "the parent's cost carried in the id");
    assert_eq!(played.requests("sessions.fork"), [json!({ "sessionId": "s1" })]);

    // As a daemon started afresh would, from the stored id alone.
    let clone = Session::start(&launch, Some(forked.clone())).await.unwrap();
    let (spend, mut spent) = mpsc::unbounded_channel();
    let reply = clone.run(Prompt { spend: Some(spend), ..Prompt::new("go") }).await.unwrap();
    assert_eq!(reply.text, "Forked");
    clone.close().await;
    let cost = spent.try_recv().unwrap();
    let Spend::Cost { session, total } = cost else { panic!("a cost: {cost:?}") };
    assert_eq!(session, forked);
    assert!((total - 0.0025).abs() < 1e-12, "not the parent's cost again: {total}");
    let resumed: Vec<_> = played.requests("session.resume").iter().map(|r| r["sessionId"].clone()).collect();
    assert_eq!(resumed, [json!("s1"), json!("forked"), json!("forked")]);
}

#[tokio::test]
async fn a_cancelled_copilot_turn_is_aborted() {
    let played = Played::new("copilot-cancel", "", |method, params| {
        let id = &params["sessionId"];
        match method {
            "session.send" => Some(default_answer(method, params).then(vec![event(
                id,
                "assistant.message_delta",
                json!({ "messageId": "a1", "deltaContent": "Working" }),
            )])),
            "session.abort" => {
                let mut events = vec![event(id, "abort", json!({ "reason": "user_initiated" }))];
                events.extend(idle(id, true));
                Some(Answer::ok(json!({})).then(events))
            }
            _ => None,
        }
    });
    let session = Session::start(&played.launch(ModelSettings::default()), None).await.unwrap();
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
    assert!(session.is_alive(), "the CLI ended the turn itself");
    session.close().await;
    assert_eq!(played.requests("session.abort").len(), 1);
}

#[tokio::test]
async fn a_copilot_prompt_takes_steers_and_ends_at_the_idle_after_the_last() {
    let played = Played::new("copilot-steer", "", |method, params| {
        let id = &params["sessionId"];
        let said = |message: &str, text: &str| {
            event(id, "assistant.message_delta", json!({ "messageId": message, "deltaContent": text }))
        };
        let user = |message: &str| event(id, "user.message", json!({ "messageId": message, "content": "" }));
        match (method, params["mode"].as_str()) {
            ("session.send", Some("immediate")) => {
                // The steer comes as the first turn ends and runs as a turn of its own.
                let mut events = idle(id, false);
                events.extend([user("u2"), said("a2", "On both")]);
                events.extend(idle(id, false));
                Some(Answer::ok(json!({ "messageId": "u2" })).then(events))
            }
            ("session.send", _) => Some(default_answer(method, params).then(vec![user("u1"), said("a1", "Working")])),
            _ => None,
        }
    });
    let session = Session::start(&played.launch(ModelSettings::default()), None).await.unwrap();
    let (ready, started) = oneshot::channel();
    let (steer_tx, steers) = mpsc::channel(1);
    let (progress, mut status) = mpsc::channel(16);
    let prompt = Prompt { progress: Some(progress), steering: Some(Steering { ready, steers }), ..Prompt::new("go") };
    let steer = async {
        started.await.expect("the Copilot CLI takes steers");
        status.recv().await;
        let (accepted, took) = oneshot::channel();
        steer_tx.send(Steer { text: "and this".to_owned(), accepted }).await.unwrap();
        took.await.unwrap()
    };
    let (reply, took) = tokio::join!(session.run(prompt), steer);
    let reply = reply.unwrap();
    assert_eq!((reply.text.as_str(), reply.cut, took), ("Working\n\nOn both", None, true));
    session.close().await;
    assert_eq!(played.requests("session.send")[1]["prompt"], "and this");
}

#[tokio::test]
async fn copilot_permission_requests_are_answered_without_asking_ants() {
    let mut answered = 0;
    let played = Played::new("copilot-permissions", "", move |method, params| {
        let id = &params["sessionId"];
        let asked = |request: &str, managed: bool| {
            let ask = json!({
                "kind": "shell", "toolCallId": request, "fullCommandText": "rm -rf build",
                "managedApprovalRequired": managed
            });
            event(id, "permission.requested", json!({ "requestId": request, "permissionRequest": ask }))
        };
        match method {
            "session.send" => Some(default_answer(method, params).then(vec![asked("p1", false), asked("p2", true)])),
            "session.permissions.handlePendingPermissionRequest" => {
                answered += 1;
                let then = if answered == 2 { idle(id, false) } else { Vec::new() };
                Some(Answer::ok(json!({ "success": true })).then(then))
            }
            _ => None,
        }
    });
    // Every tool is allowed, were the requests to reach ants.
    let session = Session::start(&played.launch(ModelSettings::default()), None).await.unwrap();
    assert_eq!(session.prompt("clean up").await.unwrap(), "");
    session.close().await;
    let mut decisions: Vec<_> = played
        .requests("session.permissions.handlePendingPermissionRequest")
        .iter()
        .map(|a| (a["requestId"].clone(), a["result"]["kind"].clone()))
        .collect();
    decisions.sort_by_key(|(request, _)| request.to_string());
    assert_eq!(decisions, [(json!("p1"), json!("approve-once")), (json!("p2"), json!("reject"))]);
}

/// A `session.error` ending a turn.
fn failing(params: &Value, error: Value) -> Answer {
    let id = &params["sessionId"];
    let mut events = vec![event(id, "session.error", error)];
    events.extend(idle(id, false));
    default_answer("session.send", params).then(events)
}

/// A `session.error` of type `kind` and code `code`.
fn error(kind: &str, code: &str, status: u16, message: &str) -> Value {
    json!({ "errorType": kind, "errorCode": code, "statusCode": status, "message": message })
}

#[tokio::test]
async fn copilot_turn_errors_are_classified() {
    let errors = [
        (error("rate_limit", "user_model_rate_limited", 429, "Please wait 1 minute"), LimitKind::RateLimit),
        (error("rate_limit", "user_weekly_rate_limited", 429, "You've reached your weekly limit"), LimitKind::Quota),
        (error("quota", "quota_exceeded", 402, "You have no AI credits left"), LimitKind::Quota),
        (json!({ "errorType": "model_call", "statusCode": 429, "message": "Too many requests" }), LimitKind::RateLimit),
    ];
    let mut sent = errors.clone().into_iter().map(|(error, _)| error);
    let played = Played::new("copilot-limits", "", move |method, params| {
        (method == "session.send").then(|| failing(params, sent.next().unwrap()))
    });
    let launch = played.launch(ModelSettings::default());
    for (error, kind) in errors {
        let session = Session::start(&launch, None).await.unwrap();
        let err = session.prompt("go").await.unwrap_err();
        let limited = err.downcast_ref::<Limited>().unwrap_or_else(|| panic!("{error}: {err:#}"));
        assert_eq!(limited.kind, kind, "{error}: {err:#}");
        session.close().await;
    }
}

#[tokio::test]
async fn a_copilot_turn_outgrowing_the_context_runs_again_in_a_new_session() {
    let mut sends = 0;
    let played = Played::new("copilot-overflow", "", move |method, params| {
        if method != "session.send" {
            return None;
        }
        sends += 1;
        let message = "prompt token count of 140000 exceeds the limit of 136000";
        let overflow = json!({ "errorType": "context_limit", "message": message });
        if sends == 1 {
            return Some(failing(params, overflow));
        }
        let id = &params["sessionId"];
        let mut events = vec![event(id, "assistant.message", json!({ "messageId": "a1", "content": "Fine" }))];
        events.extend(idle(id, false));
        Some(default_answer(method, params).then(events))
    });
    let session = Session::start(&played.launch(ModelSettings::default()), None).await.unwrap();
    let first = session.id();
    assert_eq!(session.prompt("go").await.unwrap(), "Fine");
    assert_ne!(session.id(), first, "a new session");
    assert_eq!(played.requests("session.detach"), [json!({ "sessionId": *first.0 })], "the abandoned one detached");
    session.close().await;
    assert_eq!(played.requests("session.create").len(), 2);
}

#[tokio::test]
async fn a_signed_out_or_mismatched_copilot_cli_fails_to_start() {
    let mut runs = 0;
    let played = Played::new("copilot-start", "", move |method, _| match method {
        "connect" => {
            runs += 1;
            let protocol = if runs <= 2 { 3 } else { 4 };
            Some(Answer::ok(json!({ "ok": true, "protocolVersion": protocol, "version": "1.0.200" })))
        }
        "status.get" => Some(Answer::ok(json!({ "version": "1.0.200", "protocolVersion": 4 }))),
        "auth.getStatus" => Some(Answer::ok(json!({ "isAuthenticated": false, "statusMessage": "Not authenticated" }))),
        _ => None,
    });
    let launch = played.launch(ModelSettings::default());
    let checked = handshake(&launch).await;
    assert_matches!(checked.readiness, Readiness::SignedOut { .. });
    assert_eq!(checked.sign_in.as_deref(), Some("copilot login"));
    let err = Session::start(&launch, None).await.err().expect("a signed-out session");
    let signed_out = err.downcast_ref::<SignedOut>().expect("a sign-in error");
    assert_eq!(signed_out.sign_in.as_deref(), Some("copilot login"));

    let Readiness::Failed { detail } = handshake(&launch).await.readiness else { panic!("a CLI of another protocol") };
    assert!(detail.contains("Copilot CLI 1.0.200 speaks SDK protocol 4, github-copilot-sdk 1.0.29"), "{detail}");
}

#[tokio::test]
async fn a_copilot_cli_exiting_fails_with_its_stderr() {
    let played = Played::new("copilot-exit", "", |method, params| {
        let message =
            event(&params["sessionId"], "assistant.message_delta", json!({ "messageId": "a1", "deltaContent": "Wor" }));
        (method == "session.send").then(|| Answer { close: true, ..default_answer(method, params).then(vec![message]) })
    });
    let launch = played.launch(ModelSettings::default());
    let session = Session::start(&launch, None).await.unwrap();
    let err = session.prompt("go").await.unwrap_err();
    assert!(format!("{err:#}").contains("native closed its output"), "{err:#}");
    session.close().await;

    std::fs::write(played.dir.join("stderr"), "Error: Not logged in\n").unwrap();
    let err = Session::start(&launch, None).await.err().expect("an exit saying it is not logged in");
    assert!(err.downcast_ref::<SignedOut>().is_some(), "{err:#}");
}

#[test]
fn copilot_definitions_choose_tools_and_no_mode() {
    let define = |table: &str| {
        let text = format!("[harnesses.native]\ncommand = \"copilot\"\nprotocol = \"copilot-sdk\"\n{table}");
        parse_definitions(&text).and_then(|defs| Harnesses::builtin().define(defs)).map(|h| h.get("native").unwrap())
    };
    let refused = |table: &str| format!("{:#}", define(table).unwrap_err());
    assert!(refused("mode = \"autopilot\"").contains("protocol copilot-sdk takes no mode"));
    assert!(refused("args = [\"--acp\"]").contains("runs the CLI's server, which --acp"));
    assert!(refused("args = [\"--deny-tool\", \"shell(git push)\"]").contains("the CLI's server ignores --deny-tool"));
    assert!(refused("args = [\"--available-tools=*\"]").contains("takes builtin:*, mcp:* or custom:* rather than *"));
    assert!(
        refused("[harnesses.native.context]\nlong = { args = [\"--context\"] }").contains("context long: --context")
    );
    assert!(define("args = [\"--excluded-tools\", \"bash\"]").unwrap().bypasses_permissions());

    let mut args: Vec<String> = [
        "--available-tools",
        "view,mcp:*",
        "grep",
        "--log-level",
        "warn",
        "--context",
        "default",
        "--excluded-tools=bash",
        "--additional-mcp-config",
        r#"{"mcpServers": {"a": {"command": "a"}, "b": {"command": "b"}}}"#,
        "--additional-mcp-config=@servers.json",
    ]
    .map(str::to_owned)
    .into();
    let taken = SessionArgs::take(&mut args).unwrap();
    assert_eq!(args, ["--log-level", "warn"], "the CLI's own");
    let tools = |names: &[&str]| Some(names.iter().map(|&n| n.to_owned()).collect::<Vec<_>>());
    let expected = SessionArgs {
        available: tools(&["view", "mcp:*", "grep"]),
        excluded: tools(&["bash"]),
        context: Some("default".to_owned()),
        mcp: vec![
            r#"{"mcpServers": {"a": {"command": "a"}, "b": {"command": "b"}}}"#.to_owned(),
            "@servers.json".to_owned(),
        ],
    };
    assert_eq!(taken, expected);
    let dir = scratch_dir("copilot-mcp-config");
    let file = dir.join("servers.json");
    std::fs::write(&file, r#"{"mcpServers": {"b": {"command": "file"}}}"#).unwrap();
    let file_arg = format!("@{}", file.display());
    let taken = SessionArgs { mcp: vec![expected.mcp[0].clone(), file_arg.clone()], ..expected };
    let servers = taken.mcp_servers().unwrap();
    let expected = json!({ "a": { "type": "stdio", "command": "a" }, "b": { "type": "stdio", "command": "file" } });
    assert_eq!(Value::Object(servers), expected, "the later");
    std::fs::remove_dir_all(&dir).unwrap();
    for wrong in ["{", r#"{"servers": {}}"#, r#"{"mcpServers": {"a": 1}}"#] {
        let mut args = vec!["--additional-mcp-config".to_owned(), wrong.to_owned()];
        assert!(SessionArgs::take(&mut args).is_err(), "{wrong}");
    }
    let missing = SessionArgs { mcp: vec![file_arg], ..SessionArgs::default() };
    assert!(missing.mcp_servers().unwrap_err().contains("servers.json"));
}
