use std::assert_matches;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::ErrorCode;
use agent_client_protocol::v1::schema::{EnvVariable, HttpHeader, McpServer, McpServerHttp, McpServerStdio, SessionId};
use ants_proto::{HarnessAgent, LimitKind, StatusUpdate, ThreadId};
use ants_test_support::{StubCli, scratch_dir};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::harness::{
    Harnesses, Launch, Prompt, Readiness, Session, SignedOut, Spend, Steer, Steering, fork, handshake,
    parse_definitions,
};
use crate::limits::ratelimit::Limited;
use crate::permission::{Allowlist, PermissionPolicy};

/// A response of the stub CLI to request `id`.
fn ok(id: u64, result: Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string()
}

/// An error response of the stub CLI to request `id`.
fn error(id: u64, code: i64, message: &str, data: Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message, "data": data } }).to_string()
}

/// An update the stub CLI streams.
fn update(params: Value) -> String {
    json!({ "jsonrpc": "2.0", "method": "update", "params": params }).to_string()
}

fn message(text: &str) -> String {
    update(json!({ "kind": "message", "text": text }))
}

/// The answer to `initialize`, request 1.
fn hello() -> String {
    ok(1, json!({ "name": "stub-cli", "version": "1.2.3", "fork": true }))
}

/// The answer to `new`, request 2, opening session `id`.
fn opened(id: &str) -> String {
    ok(2, json!({ "id": id }))
}

fn end_turn(id: u64, tokens: u64) -> String {
    ok(id, json!({ "stop": "end_turn", "tokens": tokens }))
}

/// A harness of the stub protocol run by a stub CLI in a scratch directory, removed on drop.
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
    /// `[harnesses.native]` of protocol stub, with `table` added.
    fn new(name: &str, table: &str) -> Self {
        let dir = scratch_dir(name).canonicalize().unwrap();
        let cli = StubCli::new(&dir, "stub-cli");
        let text = format!(
            "[harnesses.native]\ncommand = {:?}\nprotocol = \"stub\"\nsign_in = \"stub-cli login\"\n{table}",
            cli.path()
        );
        let harness = Harnesses::builtin().define(parse_definitions(&text).unwrap()).unwrap().get("native").unwrap();
        Self { dir, cli, harness }
    }

    /// A launch of the harness working in the scratch directory, with `SHARED=ant` in the ant's environment and every
    /// tool allowed, were the harness to ask.
    fn launch(&self, settings: ModelSettings) -> Launch {
        let allow_all = PermissionPolicy::new(Allowlist::parse(&["*"]).unwrap(), None);
        let env = vec![("SHARED".to_owned(), "ant".to_owned())];
        Launch::new(self.harness.clone(), self.dir.clone(), env, allow_all, settings)
    }

    /// What run `run` of the CLI read, as JSON.
    fn received(&self, run: usize) -> Vec<Value> {
        self.cli.received(run).iter().map(|line| serde_json::from_str(line).unwrap()).collect()
    }
}

fn script(native: &Fixture, run: Option<usize>, blocks: &[Vec<String>]) {
    native.cli.script(run, blocks);
}

#[tokio::test]
async fn a_native_cli_answers_prompts_through_the_translator() {
    let native = Fixture::new("native-prompt", "args = [\"--def\"]\nenv = { DEF = \"def\", SHARED = \"def\" }");
    let usage = vec![
        update(json!({ "kind": "thought", "text": "hmm" })),
        message("Hello"),
        update(json!({ "kind": "tool", "id": "t1", "title": "Run tests" })),
        message(" there"),
        update(json!({ "kind": "cost", "usd": 0.25 })),
        end_turn(3, 42),
    ];
    script(&native, None, &[vec![], vec![hello()], vec![opened("n1")], usage]);
    let thread: ThreadId = "t1".parse().unwrap();
    let stdio = McpServerStdio::new("ants", "/bin/ants")
        .args(vec!["mcp".to_owned()])
        .env(vec![EnvVariable::new("ANTS_TOKEN", "secret")]);
    let servers = vec![McpServer::Stdio(stdio)];
    let launch = native
        .launch(ModelSettings::default())
        .with_mcp(Arc::new(move |session: &crate::harness::McpSession<'_>| {
            assert!(!session.http, "the stub CLI takes no HTTP MCP servers");
            servers.clone()
        }))
        .for_thread(&thread);

    let session = Session::start(&launch, None).await.unwrap();
    let named = HarnessAgent { name: "stub-cli".to_owned(), version: "1.2.3".to_owned() };
    assert_eq!((session.agent(), session.id()), (Some(&named), SessionId::new("n1")));
    let (progress, mut status) = mpsc::channel(16);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let prompt = Prompt { progress: Some(progress), spend: Some(spend), ..Prompt::new("go") };
    assert_eq!(session.run(prompt).await.unwrap().text, "Hello there");
    session.close().await;

    let mut updates = Vec::new();
    while let Ok(update) = status.try_recv() {
        updates.push(update);
    }
    assert!(updates.contains(&StatusUpdate::Thought { text: "hmm".to_owned() }), "{updates:?}");
    assert!(
        updates.iter().any(
            |u| matches!(u, StatusUpdate::Tool { id, title, .. } if id == "t1" && title.as_deref() == Some("Run tests"))
        ),
        "{updates:?}"
    );
    let mut spends = Vec::new();
    while let Ok(spend) = spent.try_recv() {
        spends.push(spend);
    }
    assert_eq!(spends, [Spend::Cost { session: SessionId::new("n1"), total: 0.25 }, Spend::Tokens(42)]);

    assert_eq!(native.cli.args(1), ["--stub", "--def"], "the protocol's arguments, then the definition's");
    assert_eq!(native.cli.env(1, "DEF").as_deref(), Some("def"));
    assert_eq!(native.cli.env(1, "SHARED").as_deref(), Some("ant"), "the ant's env over the definition's");
    let received = native.received(1);
    assert_eq!(received[1]["method"], "new");
    assert_eq!(received[1]["params"]["cwd"], json!(native.dir));
    let mcp = json!({ "ants": { "type": "stdio", "command": "/bin/ants", "args": ["mcp"], "env": { "ANTS_TOKEN": "secret" } } });
    assert_eq!(received[1]["params"]["mcp"], mcp);
    assert_eq!(received[2]["params"]["text"], "go");

    let checked = handshake(&launch).await;
    assert_eq!(checked.readiness, Readiness::Ready { agent: "stub-cli 1.2.3".to_owned() });
    assert_eq!(checked.sign_in.as_deref(), Some("stub-cli login"));
}

#[tokio::test]
async fn a_native_cli_that_steers_takes_messages_into_the_running_prompt() {
    let native = Fixture::new("native-steer", "");
    let hello = ok(1, json!({ "name": "stub-cli", "version": "1.2.3", "steer": true }));
    let steered = vec![ok(4, json!({ "accepted": true })), message("both"), end_turn(3, 7)];
    script(&native, None, &[vec![], vec![hello], vec![opened("n1")], vec![], steered]);
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();

    let (ready, started) = oneshot::channel();
    let (steer_tx, steers) = mpsc::channel(1);
    let prompt = Prompt { steering: Some(Steering { ready, steers }), ..Prompt::new("go") };
    let steer = async {
        started.await.expect("the harness advertises steering");
        let (accepted, took) = oneshot::channel();
        steer_tx.send(Steer { text: "and this".to_owned(), accepted }).await.unwrap();
        took.await.unwrap()
    };
    let (ran, took) = tokio::join!(session.run(prompt), steer);
    assert_eq!((ran.unwrap().text, took), ("both".to_owned(), true));
    session.close().await;
    let received = native.received(1);
    assert_eq!(
        (&received[3]["method"], &received[3]["params"]),
        (&json!("steer"), &json!({ "session": "n1", "text": "and this" }))
    );
}

#[tokio::test]
async fn native_sessions_resume_fork_and_run_the_ant_settings() {
    let native = Fixture::new("native-sessions", "mode = \"ask\"");
    script(&native, Some(1), &[vec![], vec![hello()], vec![opened("n1")]]);
    script(&native, Some(2), &[vec![], vec![hello()], vec![ok(2, json!({}))]]);
    script(&native, Some(3), &[vec![], vec![hello()], vec![ok(2, json!({ "id": "f1" }))]]);
    let settings =
        ModelSettings { model: Some("m1".to_owned()), reasoning_effort: Some("high".to_owned()), ..Default::default() };
    let launch = native.launch(settings);

    Session::start(&launch, None).await.unwrap().close().await;
    let received = native.received(1);
    assert_eq!(received[0]["params"]["mode"], "ask", "the definition's mode, the CLI's permission mode");
    assert_eq!((&received[1]["params"]["model"], &received[1]["params"]["effort"]), (&json!("m1"), &json!("high")));

    let resumed = Session::start(&launch, Some(SessionId::new("n1"))).await.unwrap();
    assert_eq!(resumed.id(), SessionId::new("n1"));
    resumed.close().await;
    assert_eq!(
        (&native.received(2)[1]["method"], &native.received(2)[1]["params"]["id"]),
        (&json!("load"), &json!("n1"))
    );

    assert_eq!(fork(&launch, &SessionId::new("n1")).await.unwrap(), Some(SessionId::new("f1")));
    assert_eq!(native.received(3)[1]["method"], "fork");
}

#[tokio::test]
async fn a_native_prompt_outgrowing_the_context_runs_again_in_a_new_session() {
    let native = Fixture::new("native-overflow", "");
    let overflow = error(3, -32603, "prompt too big", json!({ "kind": "overflow" }));
    let blocks = [
        vec![],
        vec![hello()],
        vec![opened("n1")],
        vec![overflow],
        vec![],
        vec![ok(4, json!({ "id": "n2" }))],
        vec![message("Fine"), end_turn(5, 1)],
    ];
    script(&native, None, &blocks);
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();
    // The stub plays the new session's answer only once it read the close.
    let reply = tokio::time::timeout(Duration::from_secs(30), session.prompt("go")).await.expect("the old one closed");
    assert_eq!(reply.unwrap(), "Fine");
    assert_eq!(session.id(), SessionId::new("n2"));
    session.close().await;
    let received = native.received(1);
    let methods: Vec<_> = received.iter().map(|m| m["method"].clone()).collect();
    assert_eq!(methods, ["initialize", "new", "prompt", "close", "new", "prompt"], "the abandoned session closed");
    assert_eq!(received[3]["params"], json!({ "session": "n1" }));
}

#[tokio::test]
async fn a_cancelled_native_prompt_ends_with_the_cli_interrupted() {
    let native = Fixture::new("native-cancel", "");
    let blocks = [
        vec![],
        vec![hello()],
        vec![opened("n1")],
        vec![message("Working")],
        vec![ok(3, json!({ "stop": "cancelled" }))],
    ];
    script(&native, None, &blocks);
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
    assert!(session.is_alive(), "the CLI ended the prompt itself");
    session.close().await;
    assert_eq!(native.received(1)[3], json!({ "jsonrpc": "2.0", "method": "cancel", "params": { "session": "n1" } }));
}

#[tokio::test]
async fn a_signed_out_native_cli_is_told_apart_from_a_failing_one() {
    let native = Fixture::new("native-signin", "");
    let launch = native.launch(ModelSettings::default());
    script(&native, Some(1), &[vec![], vec![error(1, -32000, "Please run stub-cli login", json!(null))]]);
    script(&native, Some(2), &[vec![], vec![error(1, -32000, "Please run stub-cli login", json!(null))]]);
    script(&native, Some(3), &[vec!["!stderr Error: Not logged in".to_owned(), "!exit 1".to_owned()]]);
    script(&native, Some(4), &[vec!["!stderr Segmentation fault".to_owned(), "!exit 2".to_owned()]]);

    let checked = handshake(&launch).await;
    assert_matches!(checked.readiness, Readiness::SignedOut { .. });
    assert_eq!(checked.sign_in.as_deref(), Some("stub-cli login"));
    let err = Session::start(&launch, None).await.err().expect("a signed-out session");
    let signed_out = err.downcast_ref::<SignedOut>().expect("a sign-in error");
    assert_eq!((signed_out.harness.as_str(), signed_out.sign_in.as_deref()), ("native", Some("stub-cli login")));

    let err = Session::start(&launch, None).await.err().expect("an exit saying it is not logged in");
    assert!(err.downcast_ref::<SignedOut>().is_some(), "{err:#}");
    let Readiness::Failed { detail } = handshake(&launch).await.readiness else { panic!("a failing CLI") };
    assert!(detail.contains("exited with exit status: 2: Segmentation fault"), "{detail}");
}

#[tokio::test]
async fn native_limit_errors_are_classified() {
    let native = Fixture::new("native-limits", "");
    let limited =
        error(3, -32603, "Too Many Requests", json!({ "kind": "rate_limit", "status": 429, "retry_after": 30 }));
    script(&native, Some(1), &[vec![], vec![hello()], vec![opened("n1")], vec![limited]]);
    let quota = error(3, -32603, "weekly allowance used", json!({ "kind": "quota" }));
    script(&native, Some(2), &[vec![], vec![hello()], vec![opened("n1")], vec![quota]]);
    let launch = native.launch(ModelSettings::default());
    for (kind, retry_after) in [(LimitKind::RateLimit, Some(Duration::from_secs(30))), (LimitKind::Quota, None)] {
        let session = Session::start(&launch, None).await.unwrap();
        let err = session.prompt("go").await.unwrap_err();
        let limited = err.downcast_ref::<Limited>().expect("a provider limit");
        assert_eq!((limited.kind, limited.retry_after), (kind, retry_after), "{err:#}");
        session.close().await;
    }
}

#[tokio::test]
async fn a_native_approval_request_is_denied_without_asking_ants() {
    let native = Fixture::new("native-approval", "");
    let ask = json!({ "jsonrpc": "2.0", "id": "a1", "method": "approve", "params": { "tool": "rm -rf build" } });
    let blocks =
        [vec![], vec![hello()], vec![opened("n1")], vec![ask.to_string()], vec![message("Skipped"), end_turn(3, 1)]];
    script(&native, None, &blocks);
    // Every tool is allowed, were the request to reach ants.
    let session = Session::start(&native.launch(ModelSettings::default()), None).await.unwrap();
    assert_eq!(session.prompt("clean up").await.unwrap(), "Skipped");
    session.close().await;
    assert_eq!(native.received(1)[3], json!({ "jsonrpc": "2.0", "id": "a1", "result": { "approved": false } }));
}

#[tokio::test]
async fn a_stopped_cli_has_its_grace_to_exit_then_is_killed() {
    let native = Fixture::new("native-stop", "");
    script(&native, Some(1), &[vec![]]);
    script(&native, Some(2), &[vec!["!sleep 30".to_owned()]]);
    let cli = Cli::new(&native.harness, &[], &ModelSettings::default());
    let mut exits = cli.spawn(["--stub"], Some(&native.dir)).unwrap();
    assert!(
        exits.stop(Duration::from_secs(10)).await.is_some_and(|status| status.success()),
        "at the end of its input"
    );
    let mut sleeps = cli.spawn(["--stub"], None).unwrap();
    let started = std::time::Instant::now();
    assert_eq!(sleeps.stop(Duration::from_millis(200)).await, None, "killed");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(native.cli.cwd(1), native.dir);
}

#[test]
fn a_native_protocol_runs_tools_unasked_restricted_by_its_own_settings() {
    let define = |table: &str| {
        let text = format!("[harnesses.native]\ncommand = \"x\"\n{table}");
        parse_definitions(&text).and_then(|defs| Harnesses::builtin().define(defs)).map(|h| h.get("native").unwrap())
    };
    let harness = define("protocol = \"stub\"\nmode = \"auto\"").unwrap();
    assert_eq!(harness.protocol(), Protocol::Stub);
    assert!(harness.bypasses_permissions(), "no tool request of a native protocol reaches ants");
    let unenforced = harness.unenforced(&["read".to_owned()], true).unwrap();
    assert!(unenforced.contains("harness native runs its tools unasked"), "{unenforced}");
    let fix = harness.gated_fix();
    assert!(fix.contains("native permission settings of [harnesses.native] (protocol stub"), "{fix}");

    let err = format!("{:#}", define("protocol = \"stub\"\nmode = \"default\"").unwrap_err());
    assert!(err.contains("harness native: protocol stub takes mode ask or auto, not default"), "{err}");
    assert!(format!("{:#}", define("protocol = \"vim\"").unwrap_err()).contains("unknown variant `vim`"));
    assert!(!define("protocol = \"acp\"\nmode = \"default\"").unwrap().bypasses_permissions());
    assert!(!Harnesses::builtin().get("copilot").unwrap().protocol().is_native(), "built-ins speak ACP");
}

#[tokio::test]
async fn json_rpc_matches_responses_and_routes_what_the_cli_sends() {
    let (client, server) = tokio::io::duplex(4096);
    let (client_read, client_write) = tokio::io::split(client);
    let (rpc, mut incoming) = Rpc::new(client_read, client_write, false);
    let (server_read, server_write) = tokio::io::split(server);
    let (mut lines, cli) = (JsonLines::new(server_read), JsonWriter::new(server_write));
    let serve = async {
        assert_eq!(lines.next().await.unwrap(), Some(json!({ "id": 1, "method": "one", "params": {} })));
        assert_eq!(lines.next().await.unwrap(), Some(json!({ "id": 2, "method": "two", "params": [] })));
        for message in [
            json!({ "id": 2, "result": "second" }),
            json!({ "method": "note", "params": 1 }),
            "not JSON-RPC".into(),
            json!({ "id": "q", "method": "ask", "params": {} }),
            json!({ "id": 1, "error": { "code": -1, "message": "no", "data": "why" } }),
        ] {
            cli.send(&message).await.unwrap();
        }
    };
    let (one, two, ()) = tokio::join!(rpc.request("one", json!({})), rpc.request("two", json!([])), serve);
    assert_eq!(two, Ok(json!("second")), "answered out of order");
    assert_eq!(one, Err(RpcError { code: -1, message: "no".to_owned(), data: Some(json!("why")) }));
    assert_eq!(incoming.recv().await, Some(Incoming::Notification { method: "note".to_owned(), params: json!(1) }));
    let asked = Incoming::Request { id: json!("q"), method: "ask".to_owned(), params: json!({}) };
    assert_eq!(incoming.recv().await, Some(asked));
    rpc.respond(json!("q"), json!(true)).await.unwrap();
    assert_eq!(lines.next().await.unwrap(), Some(json!({ "id": "q", "result": true })));

    drop((lines, cli));
    assert_eq!(incoming.recv().await, None, "the CLI's output ended");
    assert!(rpc.request("three", json!({})).await.unwrap_err().is_closed());
}

#[test]
fn native_errors_reach_the_daemons_classifiers() {
    let harness = Harnesses::builtin().get("copilot").unwrap();
    let classify = |error: Error| {
        crate::limits::ratelimit::classify(&harness, &error.to_string()).map(|l| (l.kind, l.retry_after))
    };
    let minutes = |n| Some(Duration::from_mins(n));
    assert_eq!(classify(rate_limited(Some(529), minutes(2), "Overloaded")), Some((LimitKind::RateLimit, minutes(2))));
    assert_eq!(classify(rate_limited(None, None, "slow down")), Some((LimitKind::RateLimit, None)));
    assert_eq!(classify(quota_exhausted(None, "plan exhausted")), Some((LimitKind::Quota, None)));
    assert_eq!(classify(quota_exhausted(minutes(1), "plan exhausted")), Some((LimitKind::RateLimit, minutes(1))));
    assert_eq!(classify(failed("tool failed")), None);
    assert!(crate::limits::overflow::overflows(&harness, &context_overflow("2M tokens").to_string()));
    assert_eq!(signed_out("token expired").code, ErrorCode::AUTH_REQUIRED);

    let http = McpServerHttp::new("ants", "http://127.0.0.1:1/mcp")
        .headers(vec![HttpHeader::new("Authorization", "Bearer t")]);
    assert_eq!(
        mcp_servers_json(&[McpServer::Http(http)]),
        json!({ "ants": { "type": "http", "url": "http://127.0.0.1:1/mcp", "headers": { "Authorization": "Bearer t" } } })
    );
}

#[test]
fn claude_codex_and_copilot_sdk_report_their_mcp_servers_and_no_other_protocol_does() {
    let reporting = [Protocol::Acp, Protocol::Claude, Protocol::Codex, Protocol::Antigravity, Protocol::CopilotSdk]
        .into_iter()
        .filter(|p| reports_mcp(*p))
        .collect::<Vec<_>>();
    assert_eq!(reporting, [Protocol::Claude, Protocol::Codex, Protocol::CopilotSdk]);
}

#[tokio::test]
async fn a_native_cli_reports_its_mcp_servers_and_signs_them_in_on_the_loopback() {
    let native = Fixture::new("native-mcp", "");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let browser = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tcp = tokio::io::BufReader::new(tcp);
        let mut line = String::new();
        tokio::io::AsyncBufReadExt::read_line(&mut tcp, &mut line).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(tcp.get_mut(), b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
            .await
            .unwrap();
        line
    });
    let servers = json!({ "servers": [
        { "name": "ants", "status": "connected", "tools": 3 },
        { "name": "github", "status": "needs_auth", "error": "sign in" },
    ] });
    let url = format!("https://auth.example/authorize?state=s&redirect_uri=http%3A%2F%2F127.0.0.1%3A{port}%2Fcb");
    let blocks =
        [vec![], vec![hello()], vec![opened("p1")], vec![ok(3, servers)], vec![ok(4, json!({ "url": url }))], vec![]];
    script(&native, None, &blocks);
    let thread: ThreadId = "t1".parse().unwrap();
    let stdio = McpServer::Stdio(McpServerStdio::new("ants", "/bin/ants"));
    let launch = native
        .launch(ModelSettings::default())
        .with_mcp(Arc::new(move |_: &crate::harness::McpSession<'_>| vec![stdio.clone()]))
        .for_thread(&thread);

    let host = crate::harness::open_mcp(&launch).await.unwrap().unwrap();
    let report = host.report(Duration::ZERO).await.unwrap();
    assert_eq!((report.harness.as_str(), report.protocol.as_str()), ("native", "stub"));
    let servers = report.servers.unwrap();
    assert_eq!(
        servers.iter().map(|s| (s.name.as_str(), s.status)).collect::<Vec<_>>(),
        [("ants", ants_proto::McpStatus::Connected), ("github", ants_proto::McpStatus::NeedsAuth)]
    );
    assert!(host.callback("github", &format!("http://127.0.0.1:{port}/cb?code=c")).await.is_err(), "not signing in");
    assert_eq!(host.sign_in("github").await.unwrap(), Some(url));
    for elsewhere in [format!("http://127.0.0.1:{port}/admin"), "http://example.com/cb".to_owned()] {
        assert!(host.callback("github", &elsewhere).await.is_err(), "{elsewhere}");
    }
    host.callback("github", &format!("http://localhost:{port}/cb?code=c&state=s")).await.unwrap();
    assert_eq!(browser.await.unwrap().trim(), "GET /cb?code=c&state=s HTTP/1.1");
    host.close().await;

    let received = native.received(1);
    let methods = received.iter().map(|m| m["method"].as_str().unwrap_or_default()).collect::<Vec<_>>();
    assert_eq!(methods[..4], ["initialize", "new", "mcp", "mcp_sign_in"]);
    assert_eq!(received[1]["params"]["mcp"]["ants"]["command"], "/bin/ants");
    assert_eq!(received[3]["params"], json!({ "session": "p1", "server": "github" }));

    let acp = Harnesses::builtin().get("copilot").unwrap();
    let policy = PermissionPolicy::new(Allowlist::parse(&["*"]).unwrap(), None);
    let acp = Launch::new(acp, native.dir.clone(), vec![], policy, ModelSettings::default());
    assert!(crate::harness::open_mcp(&acp).await.unwrap().is_none(), "ACP reports no MCP servers");
}
