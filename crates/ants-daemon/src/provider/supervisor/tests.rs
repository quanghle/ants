use std::sync::Mutex;

use ants_proto::provider::jsonrpc::{Message, Request, Response};
use ants_proto::provider::{
    BriefReason, CallContext, Format, Origin, Output, Starter, Tool, ToolCall, ToolResult, method,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, DuplexStream};
use tokio::sync::Notify;

use super::*;
use crate::sync::MutexExt as _;

/// What a fake server answers a request with.
type Answer = fn(&Request) -> Value;

/// A provider run in this process: each run, by its number from 0, stays up for its lifetime once initialized, until
/// killed when that is none, and for no time past the lifetimes listed.
struct Fake {
    lifetimes: Vec<Option<Duration>>,
    answer: Answer,
    started: Mutex<Vec<Instant>>,
    kill: Notify,
}

impl Fake {
    fn new(lifetimes: Vec<Option<Duration>>, answer: Answer) -> Arc<Self> {
        Arc::new(Self { lifetimes, answer, started: Mutex::default(), kill: Notify::new() })
    }

    fn spawn(self: &Arc<Self>) -> Spawn {
        let fake = self.clone();
        Arc::new(move || {
            let run = {
                let mut started = fake.started.locked();
                started.push(Instant::now());
                started.len() - 1
            };
            let lifetime = fake.lifetimes.get(run).copied().unwrap_or(Some(Duration::ZERO));
            let (ours, theirs) = tokio::io::duplex(1 << 16);
            let (reader, writer) = tokio::io::split(ours);
            tokio::spawn(fake.clone().serve(theirs, lifetime));
            Ok(Spawned { reader: Box::new(reader), writer: Box::new(writer), process: None })
        })
    }

    async fn serve(self: Arc<Self>, io: DuplexStream, lifetime: Option<Duration>) {
        let (reader, mut writer) = tokio::io::split(io);
        let mut lines = BufReader::new(reader).lines();
        let mut deadline = None;
        loop {
            let up = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            let line = tokio::select! {
                line = lines.next_line() => line,
                () = up => break,
                () = self.kill.notified() => break,
            };
            let Ok(Some(line)) = line else { break };
            let Ok(Message::Request(request)) = Message::from_line(&line) else { continue };
            if request.method == method::INITIALIZE {
                deadline = lifetime.map(|lifetime| Instant::now() + lifetime);
            }
            let response = Response::new(request.id.clone(), Ok::<_, RpcError>((self.answer)(&request)));
            let _ = writer.write_all(Message::Response(response).to_line().as_bytes()).await;
        }
    }

    /// The whole seconds between its runs' starts.
    fn gaps(&self) -> Vec<u64> {
        self.started.locked().windows(2).map(|w| (w[1] - w[0]).as_secs()).collect()
    }
}

/// A provider of one tool, `echo`.
fn provider(request: &Request) -> Value {
    match request.method.as_str() {
        method::INITIALIZE => {
            let params: InitializeParams = request.params().unwrap();
            let declared = Capabilities { tools: true, ..Capabilities::NONE };
            let info = Info { name: "fake-own-name".into(), version: "1.0".into() };
            serde_json::to_value(params.accept(&[PROTOCOL_VERSION], info, declared).unwrap()).unwrap()
        }
        method::TOOLS_LIST => {
            let echo =
                Tool { name: "echo".into(), description: "Echo.".into(), input_schema: json!({"type": "object"}) };
            serde_json::to_value(ToolList { tools: vec![echo] }).unwrap()
        }
        method::TOOLS_CALL => {
            let call: ToolCall = request.params().unwrap();
            let result = ToolResult { output: Output::Text(format!("echo {}", call.name)), is_error: false };
            serde_json::to_value(result).unwrap()
        }
        _ => Value::Null,
    }
}

/// An MCP server of two pages of tools, one of them named so that ants cannot serve it.
fn mcp_server(request: &Request) -> Value {
    let params = request.params.clone().unwrap_or_default();
    match request.method.as_str() {
        "initialize" => json!({
            "protocolVersion": params["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "github", "version": "2.0"},
        }),
        "tools/list" if params["cursor"] == "2" => json!({"tools": [{"name": "get"}, {"name": "9lives"}]}),
        "tools/list" => json!({
            "tools": [{"name": "Search-Issues", "description": "Search.", "inputSchema": {"type": "object"}}],
            "nextCursor": "2",
        }),
        "tools/call" => {
            let text = format!("{} {}", params["name"], params["arguments"]);
            json!({"content": [{"type": "text", "text": text}]})
        }
        _ => Value::Null,
    }
}

fn call(name: &str) -> ToolCall {
    ToolCall {
        name: name.into(),
        arguments: json!({"q": "x"}).as_object().unwrap().clone(),
        context: CallContext {
            ant: "dev-a".parse().unwrap(),
            identity: "ab".into(),
            thread: "t-1".parse().unwrap(),
            origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
            harness: "copilot".into(),
            model: None,
            machine: "studio".into(),
            repo: None,
            at: chrono::Utc::now(),
        },
        format: Format::Text,
    }
}

fn brief() -> BriefRequest {
    BriefRequest { context: call("x").context, budget: 100, reason: BriefReason::Started }
}

async fn until(supervised: &Supervised, what: fn(&State) -> bool) {
    let mut state = supervised.shared.state.subscribe();
    tokio::time::timeout(Duration::from_secs(600), state.wait_for(what)).await.unwrap().unwrap();
}

fn names(tools: &ToolList) -> Vec<&str> {
    tools.tools.iter().map(|t| t.name.as_str()).collect()
}

#[tokio::test(start_paused = true)]
async fn restarts_back_off_from_a_second_to_a_minute_and_again_after_a_minute_up() {
    let lifetimes = [vec![Some(Duration::ZERO); 8], vec![Some(Duration::from_secs(61))]].concat();
    let fake = Fake::new(lifetimes, provider);
    let supervised = Supervised::with_spawn("fake", Protocol::Provider, fake.spawn());
    while fake.started.locked().len() < 11 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    supervised.stop();
    assert_eq!(fake.gaps(), [1, 2, 4, 8, 16, 32, 60, 60, 62, 2]);
}

#[tokio::test(start_paused = true)]
async fn tools_are_offered_while_up_and_calls_meanwhile_say_why() {
    let fake = Fake::new(vec![None, None], provider);
    let supervised = Arc::new(Supervised::with_spawn("fake", Protocol::Provider, fake.spawn()));
    let connected = crate::provider::connect(supervised.clone(), &Default::default()).await.unwrap();
    assert_eq!(connected.name(), "fake", "named as the config names it");
    assert_eq!(connected.capabilities(), Capabilities { tools: true, ..Capabilities::NONE });
    assert_eq!(names(&connected.list_tools().await.unwrap()), ["echo"]);
    assert_eq!(connected.call_tool(call("echo")).await.unwrap().output, Output::Text("echo echo".into()));

    fake.kill.notify_one();
    until(&supervised, |s| matches!(s, State::Down { .. })).await;
    assert_eq!(connected.list_tools().await.unwrap(), ToolList::default());
    let down = connected.call_tool(call("echo")).await.unwrap_err();
    assert_eq!(down.code, RpcError::UNAVAILABLE);
    assert_eq!(down.message, "provider unavailable: fake is down: closed its output; restarting in 1 s");

    until(&supervised, |s| matches!(s, State::Up(_))).await;
    assert_eq!(names(&connected.list_tools().await.unwrap()), ["echo"]);
    supervised.stop();
    until(&supervised, |s| matches!(s, State::Stopped)).await;
    let stopped = connected.call_tool(call("echo")).await.unwrap_err();
    assert_eq!(stopped.message, "provider unavailable: fake is stopped");
}

#[tokio::test(start_paused = true)]
async fn a_provider_down_at_connect_declares_what_it_may() {
    let spawn: Spawn = Arc::new(|| Err(std::io::Error::other("no such file")));
    let supervised = Arc::new(Supervised::with_spawn("memo", Protocol::Provider, spawn.clone()));
    let connected = crate::provider::connect(supervised.clone(), &Default::default()).await.unwrap();
    assert_eq!(connected.capabilities(), super::super::USES);
    let down = connected.brief(brief()).await.unwrap_err();
    assert_eq!(down.message, "provider unavailable: memo is down: could not start: no such file; restarting in 1 s");

    let mcp = Arc::new(Supervised::with_spawn("github", Protocol::Mcp, spawn));
    let connected = crate::provider::connect(mcp, &Default::default()).await.unwrap();
    assert_eq!(connected.capabilities(), Capabilities { tools: true, ..Capabilities::NONE });
}

#[tokio::test(start_paused = true)]
async fn an_mcp_server_serves_its_tools_alone() {
    let fake = Fake::new(vec![None], mcp_server);
    let supervised = Arc::new(Supervised::with_spawn("github", Protocol::Mcp, fake.spawn()));
    let connected = crate::provider::connect(supervised.clone(), &Default::default()).await.unwrap();
    assert_eq!((connected.name(), connected.agreed().provider.version.as_str()), ("github", "2.0"));
    assert_eq!(connected.capabilities(), Capabilities { tools: true, ..Capabilities::NONE });
    assert_eq!(names(&connected.list_tools().await.unwrap()), ["search_issues", "get"]);
    let result = connected.call_tool(call("search_issues")).await.unwrap();
    assert_eq!(result.output, Output::Text(r#""Search-Issues" {"q":"x"}"#.into()));
    assert_eq!(connected.brief(brief()).await.unwrap_err().code, RpcError::NOT_AGREED);
}

#[cfg(unix)]
#[tokio::test]
async fn a_provider_process_that_exits_is_started_again() {
    use std::collections::BTreeMap;

    use ants_test_support::{StubCli, scratch_dir};

    let dir = scratch_dir("provider-process");
    let stub = StubCli::new(&dir, "memo");
    let initialized = r#"{"jsonrpc":"2.0","id":1,"result":{"protocol_version":1,"provider":{"name":"memo","version":"1"},"capabilities":{"tools":true}}}"#;
    let listed = r#"{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}"#;
    stub.script(Some(1), &[vec![], vec![initialized], vec![listed, "!stderr boom", "!exit 3"]]);
    stub.script(None, &[vec![], vec![initialized]]);
    let definition = Definition {
        name: "memo".into(),
        command: stub.path().to_owned(),
        args: vec!["--stdio".into()],
        env: BTreeMap::from([("NOTES_DB".into(), "x.db".into())]),
        protocol: Protocol::Provider,
    };
    let supervised = Arc::new(Supervised::start(&definition));
    let connected = crate::provider::connect(supervised.clone(), &Default::default()).await.unwrap();
    assert_eq!(connected.list_tools().await.unwrap(), ToolList::default());
    until(&supervised, |s| matches!(s, State::Down { .. })).await;
    let down = connected.call_tool(call("x")).await.unwrap_err().message;
    let exited = "provider unavailable: memo is down: exited with exit status: 3: boom; restarting in ";
    // Off Linux, tokio learns of the exit from SIGCHLD, which another test's busy runtime may relay after the grace
    // given once the output closed.
    let closed = "provider unavailable: memo is down: closed its output; restarting in ";
    let raced = cfg!(not(target_os = "linux")) && down.starts_with(closed);
    assert!(down.starts_with(exited) || raced, "{down}");
    assert_eq!((stub.args(1), stub.env(1, "NOTES_DB")), (vec!["--stdio".to_owned()], Some("x.db".to_owned())));

    until(&supervised, |s| matches!(s, State::Up(_))).await;
    assert_eq!(stub.runs(), 2);
    supervised.stop();
    for _ in 0..100 {
        if stub.ended(2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(stub.ended(2), "stopping closes the provider's input");
    let _ = std::fs::remove_dir_all(dir);
}
