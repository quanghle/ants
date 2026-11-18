//! The tools of the ant's provider: listed beside ants' own under its prefix, called with what ants knows of the call.

use std::sync::atomic::{AtomicBool, Ordering};

use ants_proto::provider::{
    BoxFuture, Capabilities, Format, Host, Info, InitializeParams, InitializeResult, Output, Provider, RpcError,
    Starter, Tool, ToolCall, ToolList, ToolResult,
};
use serde_json::json;

use super::*;
use crate::provider::context::Callers;
use crate::provider::origins::OriginStore;

/// A provider named `send` noting the calls of its tools, whose list fails while `down`: `note` answers the arguments
/// back, `message` would be served as ants' own `send_message`, `Bad` has an invalid name.
#[derive(Default)]
struct Fake {
    calls: StdMutex<Vec<ToolCall>>,
    down: AtomicBool,
}

impl Provider for Fake {
    fn initialize(
        &self,
        params: InitializeParams,
        _host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
        let info = Info { name: "send".to_owned(), version: "1".to_owned() };
        let tools = Capabilities { tools: true, ..Capabilities::NONE };
        Box::pin(std::future::ready(params.accept(&[1], info, tools)))
    }

    fn list_tools(&self) -> BoxFuture<'_, Result<ToolList, RpcError>> {
        if self.down.load(Ordering::Relaxed) {
            return Box::pin(std::future::ready(Err(RpcError::unavailable("down"))));
        }
        let tool = |name: &str| Tool {
            name: name.to_owned(),
            description: format!("the {name} tool"),
            input_schema: json!({"type": "object", "properties": {"x": {"type": "integer"}}}),
        };
        Box::pin(std::future::ready(Ok(ToolList { tools: vec![tool("note"), tool("message"), tool("Bad")] })))
    }

    fn call_tool(&self, call: ToolCall) -> BoxFuture<'_, Result<ToolResult, RpcError>> {
        let result = if self.down.load(Ordering::Relaxed) {
            Err(RpcError::unavailable("down"))
        } else {
            let output = match call.format {
                Format::Json => Output::Json(serde_json::Value::Object(call.arguments.clone())),
                Format::Text => Output::Text("noted".to_owned()),
            };
            Ok(ToolResult { output, is_error: call.arguments.contains_key("refuse") })
        };
        self.calls.locked().push(call);
        Box::pin(std::future::ready(result))
    }
}

async fn with_fake(mcp: &Mcp) -> Arc<Fake> {
    let fake = Arc::new(Fake::default());
    mcp.with_provider(crate::provider::connect(fake.clone()).await.expect("the fake connects"));
    fake
}

async fn names(client: &Client) -> Vec<String> {
    client.list_all_tools().await.unwrap().into_iter().map(|t| t.name.into_owned()).collect()
}

#[tokio::test]
async fn sense_tools_are_served_with_its_prefix_and_the_json_argument() {
    let home = home("sense");
    let (mcp, _sent) = start(&home).await;
    mcp.with_provider(crate::provider::connect(Arc::new(ants_sense::Sense::new())).await.unwrap());
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    mcp.settle_origin(&current, None).await.unwrap();
    let _turn = mcp.begin(&current);

    let tools = client.list_all_tools().await.unwrap();
    let served: Vec<_> = tools.iter().filter(|t| t.name.starts_with("sense_")).collect();
    let names: Vec<_> = served.iter().map(|t| t.name.as_ref()).collect();
    for name in ["sense_brief", "sense_remember", "sense_recall", "sense_handoff"] {
        assert!(names.contains(&name), "{names:?}");
    }
    assert!(served.iter().all(|t| t.input_schema["properties"]["json"]["type"] == "boolean"));
    assert!(tools.iter().any(|t| t.name == "send_message" && !t.input_schema["properties"].get("json").is_some()));

    let (ok, text) = call(&client, "sense_brief", json!({})).await;
    assert!(!ok, "{text}");
    assert!(text.starts_with("sense keeps no data here"), "{text}");
    let result =
        client.call_tool(CallToolRequestParams::new("sense_brief").with_arguments(json_object(json!({"json": true}))));
    let result = result.await.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(result.structured_content.unwrap()["error"], "unavailable");
}

/// The instructions the ants MCP server gives a session of `mcp`'s ant as it initializes.
async fn instructions(mcp: &Mcp) -> String {
    let (url, token) = http_server(mcp, &from_human().thread_id);
    let client = http_client(&url, &token).await;
    client.peer_info().and_then(|info| info.instructions.clone()).expect("instructions")
}

#[tokio::test]
async fn the_instructions_leave_memory_to_the_provider_and_tell_the_handoffs_apart() {
    let homes = [home("instructions"), home("instructions-sense"), home("instructions-other")];
    let (without, _sent) = start(&homes[0]).await;
    let text = instructions(&without).await;
    assert!(text.contains("handoff hands work to this ant itself on another thread"), "{text}");
    assert!(text.ends_with("Memory and task tools follow the ant's configured instructions."), "{text}");
    assert!(!text.contains("_* come from"), "{text}");

    let (sense, _sent) = start(&homes[1]).await;
    sense.with_provider(crate::provider::connect(Arc::new(ants_sense::Sense::new())).await.unwrap());
    let text = instructions(&sense).await;
    assert!(text.contains("The tools named sense_* come from sense"), "{text}");
    assert!(text.contains("sense_handoff, unlike handoff, sends nothing"), "{text}");
    assert!(!text.contains("configured instructions"), "{text}");

    let (other, _sent) = start(&homes[2]).await;
    with_fake(&other).await;
    let text = instructions(&other).await;
    assert!(text.contains("The tools named send_* come from send"), "{text}");
    assert!(!text.contains("sense_handoff") && !text.contains("configured instructions"), "{text}");
}

fn json_object(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    let serde_json::Value::Object(object) = value else { panic!("an object") };
    object
}

#[tokio::test]
async fn calls_carry_who_calls_where_and_when() {
    let home = home("context");
    let (mcp, mut sent) = start(&home).await;
    let fake = with_fake(&mcp).await;
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;

    let listed = names(&client).await;
    assert!(listed.contains(&"send_note".to_owned()), "{listed:?}");
    assert_eq!(listed.iter().filter(|n| *n == "send_message").count(), 1, "ants' own wins: {listed:?}");
    assert!(!listed.iter().any(|n| n.contains("Bad")), "{listed:?}");

    let (ok, text) = call(&client, "send_note", json!({"x": 1})).await;
    assert!(!ok && text.starts_with("[ants] send_note was not called: no message"), "{text}");
    assert!(fake.calls.locked().is_empty());

    mcp.settle_origin(&current, None).await.unwrap();
    let _turn = mcp.begin(&current);
    let before = chrono::Utc::now();
    let result = client
        .call_tool(CallToolRequestParams::new("send_note").with_arguments(json_object(json!({"x": 1, "json": true}))));
    let result = result.await.unwrap();
    assert_eq!((result.is_error, result.structured_content), (Some(false), Some(json!({"x": 1}))));
    let noted = fake.calls.locked().pop().unwrap();
    assert_eq!((noted.name.as_str(), noted.format), ("note", Format::Json));
    assert_eq!(noted.arguments, json_object(json!({"x": 1})), "the json argument is taken out");
    let context = noted.context;
    assert_eq!(
        (&context.ant, context.identity.as_str(), &context.thread),
        (&user("dev-a"), "id-1", &current.thread_id)
    );
    assert_eq!(context.origin.by, Starter::Human { user: UserId::human() });
    assert_eq!((context.harness.as_str(), context.model.as_deref(), context.machine.as_str()), ("test", None, "m1"));
    assert!(context.at >= before && context.trusted());

    let (ok, text) = call(&client, "send_note", json!({"refuse": true})).await;
    assert_eq!((ok, text.as_str()), (false, "noted"), "a refusal is the provider's own text");
    let (ok, text) = call(&client, "send_message", json!({"to": "dev-b", "text": "hi"})).await;
    assert!(ok, "{text}");
    assert_eq!(sent.recv().await.unwrap().to, user("dev-b"));
    assert_eq!(fake.calls.locked().len(), 1, "send_message is ants' own");
}

#[tokio::test]
async fn a_provider_down_lists_no_tools_and_answers_calls_with_why() {
    let home = home("down");
    let (mcp, _sent) = start(&home).await;
    let fake = with_fake(&mcp).await;
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    mcp.settle_origin(&current, None).await.unwrap();
    let _turn = mcp.begin(&current);

    fake.down.store(true, Ordering::Relaxed);
    let listed = names(&client).await;
    assert!(listed.contains(&"send_message".to_owned()) && !listed.iter().any(|n| n == "send_note"), "{listed:?}");
    let (ok, text) = call(&client, "send_note", json!({})).await;
    assert!(!ok);
    assert_eq!(text, "[ants] send_note did not reach send: provider unavailable: down.");

    fake.down.store(false, Ordering::Relaxed);
    assert!(names(&client).await.contains(&"send_note".to_owned()), "listed again once it is back");
}

#[tokio::test]
async fn calls_wait_for_the_ants_identity() {
    let home = home("noid");
    let (mcp, _sent) = start(&home).await;
    let fake = with_fake(&mcp).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    mcp.settle_origin(&current, None).await.unwrap();
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "send_note", json!({})).await;
    assert!(!ok);
    assert_eq!(text, "[ants] send_note was not called: the ant's identity record cannot be read.");
    assert!(fake.calls.locked().is_empty());
}

#[tokio::test]
async fn a_thread_keeps_its_origin_and_stays_untrusted_once_a_webhook_reached_it() {
    let home = home("origin");
    let (mcp, _sent) = start(&home).await;
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));
    let session =
        |thread: &ThreadId| drop(mcp.servers()(&crate::harness::McpSession::test(thread, Path::new("/"), true)));

    let asked = Envelope::new(user("dev-b"), user("dev-a"), "help");
    session(&asked.thread_id);
    let channel: ants_proto::ChannelId = "ops".parse().unwrap();
    mcp.settle_origin(&asked, Some(&channel)).await.unwrap();
    let mut human = Envelope::new(UserId::human(), user("dev-a"), "look");
    human.thread_id = asked.thread_id.clone();
    mcp.settle_origin(&human, None).await.unwrap();
    let origin = mcp.call_context(&asked.thread_id).await.unwrap().origin;
    assert_eq!((origin.by, origin.channel), (Starter::Ant { user: user("dev-b") }, Some(channel.clone())));

    let mut hook = Envelope::new(user("hook"), user("dev-a"), "build failed").from_webhook();
    hook.thread_id = asked.thread_id.clone();
    mcp.settle_origin(&hook, None).await.unwrap();
    mcp.settle_origin(&human, None).await.unwrap();
    let context = mcp.call_context(&asked.thread_id).await.unwrap();
    assert_eq!(
        (&context.origin.by, context.origin.channel.as_ref()),
        (&Starter::Webhook { user: user("hook") }, Some(&channel))
    );
    assert!(!context.trusted());
    assert_eq!(mcp.origin(&asked.thread_id), Some(context.origin));

    let unopened = from_human();
    mcp.settle_origin(&unopened, None).await.unwrap();
    let why = mcp.call_context(&unopened.thread_id).await.unwrap_err();
    assert_eq!(why, "no session of the thread was opened");
}

#[tokio::test]
async fn a_thread_keeps_its_stored_origin_across_a_restart() {
    let home = home("restart");
    let store = OriginStore::memory();
    let (mcp, _sent) = start(&home).await;
    mcp.with_origins(store.clone());
    let mut hook = Envelope::new(user("hook"), user("dev-a"), "build failed").from_webhook();
    let thread = hook.thread_id.clone();
    mcp.settle_origin(&hook, None).await.unwrap();
    let stored = store.get(&thread).await.unwrap().expect("stored once settled");
    assert_eq!(stored.by, Starter::Webhook { user: user("hook") });
    drop(mcp);

    let (mcp, _sent) = start(&home).await;
    mcp.with_origins(store.clone());
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));
    drop(mcp.servers()(&crate::harness::McpSession::test(&thread, Path::new("/"), true)));
    let context = mcp.call_context(&thread).await.unwrap();
    assert_eq!(context.origin, stored, "a call before the thread's next message");
    assert!(!context.trusted());

    drop(mcp);
    let (mcp, _sent) = start(&home).await;
    mcp.with_origins(store.clone());
    let mut human = Envelope::new(UserId::human(), user("dev-a"), "look");
    human.thread_id = thread.clone();
    mcp.settle_origin(&human, None).await.unwrap();
    assert_eq!(mcp.origin(&thread), Some(stored.clone()), "a human's message keeps it untrusted");
    assert_eq!(store.get(&thread).await.unwrap(), Some(stored));

    hook.thread_id = from_human().thread_id;
    let mut human = Envelope::new(UserId::human(), user("dev-a"), "look");
    human.thread_id = hook.thread_id.clone();
    mcp.settle_origin(&human, None).await.unwrap();
    mcp.settle_origin(&hook, None).await.unwrap();
    let downgraded = store.get(&hook.thread_id).await.unwrap().unwrap();
    assert_eq!(downgraded.by, Starter::Webhook { user: user("hook") }, "stored again once downgraded");
}

#[tokio::test]
async fn calls_carry_the_repository_the_thread_s_last_session_works_in() {
    let home = home("repo");
    let (mcp, _sent) = start(&home).await;
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));
    let current = from_human();
    mcp.settle_origin(&current, None).await.unwrap();
    let open = |dir: &Path| drop(mcp.servers()(&crate::harness::McpSession::test(&current.thread_id, dir, true)));

    open(Path::new("/"));
    assert_eq!(mcp.call_context(&current.thread_id).await.unwrap().repo, None, "outside a repository");
    std::fs::create_dir_all(home.join(".git")).unwrap();
    std::fs::create_dir_all(home.join("sub")).unwrap();
    std::fs::write(home.join(".git/config"), "[remote \"origin\"]\n\turl = git@github.com:Owner/Repo.git\n").unwrap();
    open(&home.join("sub"));
    let context = mcp.call_context(&current.thread_id).await.unwrap();
    assert_eq!(context.repo.as_deref(), Some("github.com/Owner/Repo"));
    let directories = mcp.workdirs();
    assert_eq!(directories.workdir(&context), Some(home.join("sub")));
    let other_ant = ants_proto::provider::CallContext { ant: user("dev-b"), ..context.clone() };
    assert_eq!(directories.workdir(&other_ant), None);
    let other_thread = ants_proto::provider::CallContext { thread: "unknown".parse().unwrap(), ..context };
    assert_eq!(directories.workdir(&other_thread), None);
    drop(mcp);
    assert_eq!(directories.workdir(&other_thread), None);
}

#[tokio::test]
async fn a_review_only_reads_and_messages_no_one() {
    let home = home("review");
    let (mcp, mut sent) = start(&home).await;
    let fake = with_fake(&mcp).await;
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));
    let review = Envelope::new(user("dev-b"), user("dev-a"), "# Review of dream 1 of dev-b")
        .with_allow(Some(ants_proto::REVIEW_ALLOW.map(str::to_owned).to_vec()));
    let (url, token) = http_server(&mcp, &review.thread_id);
    let client = http_client(&url, &token).await;
    mcp.settle_origin(&review, None).await.unwrap();
    let _turn = mcp.begin(&review);

    for (tool, args) in [
        ("send_message", json!({"to": "dev-b", "text": "hi"})),
        ("complete", json!({})),
        ("send_note", json!({"x": 1})),
    ] {
        let (ok, text) = call(&client, tool, args).await;
        assert!(!ok && text.starts_with(&format!("{tool} is not for a review")), "{text}");
    }
    assert!(sent.try_recv().is_err() && fake.calls.locked().is_empty(), "nothing went out");
    let (ok, text) = call(&client, "list_ants", json!({})).await;
    assert!(ok, "the ants tools that only read stay: {text}");
}
