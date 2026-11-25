//! Tests of native harness protocols behind the in-process ACP agent: prompts, steering, sessions, context overflow,
//! cancels, sign-in, limits, approvals, shutdown, JSON-RPC routing and MCP servers. The harness run by a stub CLI
//! ([`StubHarness`]) is the fixture of every protocol's tests.

use std::assert_matches;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    EnvVariable, HttpHeader, McpServerHttp, McpServerStdio, NewSessionRequest, PromptRequest, SessionId, TextContent,
};
use agent_client_protocol::{Channel, ErrorCode};
use ants_proto::{HarnessAgent, LimitKind, StatusUpdate, ThreadId};
use ants_test_support::{StubCli, scratch_dir};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use super::translator::Translator;
use super::*;
use crate::harness::{
    Harnesses, Launch, Prompt, Readiness, Session, SignedOut, Spend, Steer, SteerRequest, Steering, fork, handshake,
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

/// The harness a config file defines as `[harnesses.<name>]` of `protocol`, with `table` added.
pub(super) fn define(name: &str, protocol: Protocol, table: &str) -> anyhow::Result<Arc<Harness>> {
    let text = format!("[harnesses.{name}]\nprotocol = \"{protocol}\"\n{table}");
    Ok(Harnesses::builtin().define(parse_definitions(&text)?)?.get(name).unwrap())
}

/// A launch of `harness` working in `dir` with the ant's `env`, every tool allowed were the harness to ask.
pub(super) fn launch(harness: &Arc<Harness>, dir: &Path, env: &[(&str, &str)], settings: ModelSettings) -> Launch {
    let env = env.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect();
    Launch::new(harness.clone(), dir.to_owned(), env, PermissionPolicy::allow_all(), settings)
}

/// A harness of a native protocol run by a stub CLI in a scratch directory, removed on drop: what the tests of each
/// protocol build their own fixture on.
pub(super) struct StubHarness {
    pub dir: PathBuf,
    pub cli: StubCli,
    pub harness: Arc<Harness>,
}

impl Drop for StubHarness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl StubHarness {
    /// `[harnesses.<harness>]` of `protocol` in the scratch directory `name`, its command a stub CLI named `command`,
    /// with `table` added.
    pub(super) fn new(name: &str, harness: &str, protocol: Protocol, command: &str, table: &str) -> Self {
        let dir = scratch_dir(name).canonicalize().unwrap();
        let cli = StubCli::new(&dir, command);
        let harness = define(harness, protocol, &format!("command = {:?}\n{table}", cli.path())).unwrap();
        Self { dir, cli, harness }
    }

    /// A launch of the harness working in the scratch directory with the ant's `env`, every tool allowed were the
    /// harness to ask.
    pub(super) fn launch(&self, env: &[(&str, &str)], settings: ModelSettings) -> Launch {
        launch(&self.harness, &self.dir, env, settings)
    }

    /// What run `run` of the CLI read, as JSON.
    pub(super) fn received(&self, run: usize) -> Vec<Value> {
        self.cli.received(run).iter().map(|line| serde_json::from_str(line).unwrap()).collect()
    }
}

/// The command, quoted for a shell, starting `program` in a session of its own (`setsid`), as Claude Code runs a
/// command in the background.
pub(super) fn setsid(program: &str) -> String {
    format!("perl -MPOSIX -e 'POSIX::setsid() or die; exec @ARGV' {program}")
}

/// A process this test started, killed when dropped, should the test fail before it is gone.
pub(super) struct Killed(pub rustix::process::Pid);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = rustix::process::kill_process(self.0, rustix::process::Signal::KILL);
    }
}

/// The pid written to `file` in `dir`, once it is.
pub(super) async fn written_pid(dir: &Path, file: &str) -> rustix::process::Pid {
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
pub(super) async fn gone(pid: rustix::process::Pid, what: &str) {
    let limit = leftovers::GRACE + Duration::from_secs(1);
    ants_test_support::eventually_every(Duration::from_millis(20), limit, what, async || {
        rustix::process::test_kill_process(pid).is_err()
    })
    .await;
}

/// Run `prompt`, collecting its status updates and spend.
pub(super) async fn run(session: &Session, prompt: Prompt) -> (anyhow::Result<String>, Vec<StatusUpdate>, Vec<Spend>) {
    let (progress, mut status) = mpsc::channel(64);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let reply = session.run(Prompt { progress: Some(progress), spend: Some(spend), ..prompt }).await;
    let (mut updates, mut spends) = (Vec::new(), Vec::new());
    while let Ok(update) = status.try_recv() {
        updates.push(update);
    }
    while let Ok(spend) = spent.try_recv() {
        spends.push(spend);
    }
    (reply.map(|reply| reply.text), updates, spends)
}

/// A harness of the stub protocol run by a stub CLI.
struct Fixture(StubHarness);

impl std::ops::Deref for Fixture {
    type Target = StubHarness;

    fn deref(&self) -> &StubHarness {
        &self.0
    }
}

impl Fixture {
    /// `[harnesses.native]` of protocol stub, with `table` added.
    fn new(name: &str, table: &str) -> Self {
        let table = format!("sign_in = \"stub-cli login\"\n{table}");
        Self(StubHarness::new(name, "native", Protocol::Stub, "stub-cli", &table))
    }

    /// A launch of the harness with `SHARED=ant` in the ant's environment.
    fn launch(&self, settings: ModelSettings) -> Launch {
        self.0.launch(&[("SHARED", "ant")], settings)
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

/// The answer the stub CLI of a harness with `table` gets to its approval request, on a launch with `allow` rules.
async fn approval(name: &str, table: &str, allow: &[&str]) -> Value {
    let native = Fixture::new(name, table);
    let ask = json!({ "jsonrpc": "2.0", "id": "a1", "method": "approve", "params": { "tool": "rm -rf build" } });
    let blocks =
        [vec![], vec![hello()], vec![opened("n1")], vec![ask.to_string()], vec![message("Done"), end_turn(3, 1)]];
    script(&native, None, &blocks);
    let policy = PermissionPolicy::new(Allowlist::parse(allow).unwrap(), None);
    let launch = native.launch(ModelSettings::default()).with_policy(policy);
    let session = Session::start(&launch, None).await.unwrap();
    assert_eq!(session.prompt("clean up").await.unwrap(), "Done");
    session.close().await;
    native.received(1)[3].clone()
}

#[tokio::test]
async fn a_native_approval_request_is_denied_unless_the_ant_asks_ants() {
    let answer = |approved: bool| json!({ "jsonrpc": "2.0", "id": "a1", "result": { "approved": approved } });
    assert_eq!(approval("native-approval", "", &["*"]).await, answer(false), "denied, though every tool is allowed");
    let asks = "tools = \"ask\"";
    assert_eq!(approval("native-approval-allowed", asks, &["*"]).await, answer(true), "allowed by the allow rules");
    assert_eq!(approval("native-approval-rejected", asks, &["read"]).await, answer(false), "no approver to ask");
}

#[tokio::test]
async fn a_stopped_cli_has_its_grace_to_exit_then_is_killed() {
    let native = Fixture::new("native-stop", "");
    script(&native, Some(1), &[vec![]]);
    script(&native, Some(2), &[vec!["!sleep 30".to_owned()]]);
    let cli = Cli::new(&native.harness, &[], &ModelSettings::default());
    let (mut exits, stdin, _stdout) = cli.spawn(["--stub"], Some(&native.dir)).await.unwrap();
    drop(stdin);
    assert!(
        exits.stop(Duration::from_secs(10)).await.is_some_and(|status| status.success()),
        "at the end of its input"
    );
    let (mut sleeps, stdin, _stdout) = cli.spawn(["--stub"], None).await.unwrap();
    drop(stdin);
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
    assert!(harness.bypasses_permissions(), "no tool request of a native protocol reaches ants unless it asks");
    let unenforced = harness.unenforced(&["read".to_owned()], true).unwrap();
    assert!(unenforced.contains("harness native runs its tools unasked"), "{unenforced}");
    let fix = harness.gated_fix();
    assert!(fix.contains("set tools = \"ask\" on the ant or in [harnesses.native]"), "{fix}");
    let asking = define("protocol = \"stub\"\nmode = \"auto\"\ntools = \"ask\"").unwrap();
    assert!(!asking.bypasses_permissions(), "what the CLI would ask a human about reaches ants");
    assert_eq!(asking.unenforced(&["read".to_owned()], true), None);

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
    let acp = Launch::new(acp, native.dir.clone(), vec![], PermissionPolicy::allow_all(), ModelSettings::default());
    assert!(crate::harness::open_mcp(&acp).await.unwrap().is_none(), "ACP reports no MCP servers");
}

#[tokio::test]
async fn a_prompt_dropped_mid_turn_no_longer_counts_as_running() {
    let native = Fixture::new("native-dropped-prompt", "");
    let steering = ok(1, json!({ "name": "stub-cli", "version": "1.2.3", "steer": true }));
    let accepted = ok(4, json!({ "accepted": true }));
    script(&native, None, &[vec![], vec![steering], vec![opened("n1")], vec![], vec![accepted]]);
    let cli = Cli::new(&native.harness, &[], &ModelSettings::default());
    let translator = Arc::new(Translator::<stub::Stub>::new(cli));
    translator.clone().initialize(InitializeRequest::new(ProtocolVersion::V1)).await.unwrap();
    let session = translator.clone().new_session(NewSessionRequest::new(native.dir.clone())).await.unwrap().session_id;
    let said = |text: &str| vec![ContentBlock::Text(TextContent::new(text))];
    // The translator runs a prompt in a task of the connection, which nothing drops while it is open.
    let (ours, _client) = Channel::duplex();
    let dropped = async |cx: ConnectionTo<Client>| {
        let prompt = translator.clone().prompt(PromptRequest::new(session.clone(), said("hello")), cx);
        assert!(tokio::time::timeout(Duration::from_millis(200), prompt).await.is_err(), "the turn does not end");
        let steer = SteerRequest { session_id: session.clone(), prompt: said("more") };
        assert!(!translator.clone().steer(steer).await?.accepted, "no prompt runs to take a steer");
        Ok(())
    };
    Agent.v1().name("test").connect_with(ours, dropped).await.unwrap();
}

/// The lines of a stub CLI's turn starting `program` in the background in a session of its own, as Claude Code runs a
/// background command, its pid written to `file` once it is, and `program` again as a child of the CLI's own process
/// group, as an MCP server runs; the turn ends once the first wrote its pid.
fn in_the_background(file: &Path, program: &str) -> Vec<String> {
    let file = file.display();
    let own = format!(
        "perl -MPOSIX -e 'POSIX::setsid() or die; open my $f, \">\", shift or die; print $f \"$$\\n\"; close $f; exec @ARGV' '{file}' {program}"
    );
    vec![
        format!("!bg exec {own}"),
        format!("!bg exec {program}"),
        format!("!sh while [ ! -s '{file}' ]; do sleep 0.05; done"),
        end_turn(3, 1),
    ]
}

#[tokio::test]
async fn an_idle_harness_is_kept_while_its_last_turn_left_a_command_running_in_the_background() {
    if !ants_test_support::have("perl") {
        return;
    }
    let native = Fixture::new("native-background", "");
    let (first, second) = (native.dir.join("first.pid"), native.dir.join("second.pid"));
    let started = |run, file: &Path| {
        script(&native, Some(run), &[vec![], vec![hello()], vec![opened("n1")], in_the_background(file, "sleep 60")]);
    };
    started(1, &first);
    started(2, &second);
    let quiet = vec!["!bg exec sleep 60".to_owned(), end_turn(3, 1)];
    script(&native, Some(3), &[vec![], vec![hello()], vec![opened("n1")], quiet]);
    let launch = native.launch(ModelSettings::default());
    let pool = crate::pool::Pool::new(launch, crate::pool::SessionStore::memory(), 1, Duration::ZERO)
        .keeping_background_for(Some(Duration::from_mins(1)));
    let thread: ThreadId = "t1".parse().unwrap();

    pool.prompt(&thread, "check").await.unwrap();
    let check = Killed(written_pid(&native.dir, "first.pid").await);
    let now = std::time::Instant::now();
    assert_eq!(pool.evict_idle(now), 0, "kept while its background command runs");
    assert_eq!(pool.evict_idle(now + Duration::from_secs(50)), 0, "kept up to the limit");
    rustix::process::kill_process(check.0, rustix::process::Signal::KILL).unwrap();
    gone(check.0, "the background command").await;
    assert_eq!(pool.evict_idle(std::time::Instant::now()), 1, "stopped once the command ended");

    pool.prompt(&thread, "check again").await.unwrap();
    let check = Killed(written_pid(&native.dir, "second.pid").await);
    assert_eq!(pool.evict_idle(std::time::Instant::now()), 0, "kept while its background command runs");
    assert_eq!(pool.evict_idle(std::time::Instant::now() + Duration::from_mins(1)), 1, "stopped past the limit");
    gone(check.0, "the background command, ended with the harness").await;

    pool.prompt(&thread, "nothing in the background").await.unwrap();
    assert_eq!(native.cli.runs(), 3);
    assert_eq!(pool.evict_idle(std::time::Instant::now()), 1, "what runs in the CLI's own process group keeps none");
}

#[tokio::test]
async fn a_harness_working_in_the_background_is_the_last_stopped_to_stay_within_the_cap() {
    if !ants_test_support::have("perl") {
        return;
    }
    let native = Fixture::new("native-background-cap", "");
    let file = native.dir.join("check.pid");
    let checking = in_the_background(&file, "sleep 60");
    script(&native, Some(1), &[vec![], vec![hello()], vec![opened("n1")], checking]);
    script(&native, None, &[vec![], vec![hello()], vec![opened("n1")], vec![end_turn(3, 1)]]);
    let launch = native.launch(ModelSettings::default());
    let pool = crate::pool::Pool::new(launch, crate::pool::SessionStore::memory(), 2, Duration::from_hours(1));
    let [older, newer, third] = ["t1", "t2", "t3"].map(|thread| thread.parse::<ThreadId>().unwrap());

    pool.prompt(&older, "check").await.unwrap();
    let check = Killed(written_pid(&native.dir, "check.pid").await);
    pool.prompt(&newer, "quiet").await.unwrap();
    pool.prompt(&third, "quiet too").await.unwrap();
    ants_test_support::eventually_within(Duration::from_secs(10), "the newer harness stopped", async || {
        native.cli.ended(2)
    })
    .await;
    assert!(!native.cli.ended(1), "the harness working in the background goes on");
    assert!(rustix::process::test_kill_process(check.0).is_ok(), "and its check with it");
}
