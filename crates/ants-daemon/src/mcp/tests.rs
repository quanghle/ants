use std::path::PathBuf;
use std::sync::Mutex as StdMutex;

use agent_client_protocol::v1::schema::{
    AgentCapabilities, ContentBlock as AcpContent, ContentChunk, InitializeRequest, InitializeResponse,
    LoadSessionRequest, LoadSessionResponse, McpCapabilities, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, DynConnectTo};
use rmcp::RoleClient;
use rmcp::model::{CallToolRequestParams, ClientConfig};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use tokio::sync::mpsc;

use ants_proto::{ChannelAdminReply, MAX_TITLE};

use super::*;
use crate::harness::Launch;
use crate::pool::{Pool, SessionStore};

type Client = RunningService<RoleClient, ClientConfig>;

fn user(name: &str) -> UserId {
    name.parse().unwrap()
}

/// A fresh daemon home under the target directory, short enough for a socket path.
fn home(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/mcp-t").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// An MCP server for `dev-a` whose sent envelopes come out of the receiver.
async fn start(home: &Path) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    start_with_roster(home, Vec::new()).await
}

/// Like [`start`], with `roster` as the roster.
async fn start_with_roster(home: &Path, roster: Vec<RosterEntry>) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    start_with(home, roster, Arc::default(), Registry::default()).await
}

/// Thread notes in memory; a post's sequence is its place in the notes of its thread.
#[derive(Default)]
struct MemoryNotes(StdMutex<HashMap<ThreadId, Vec<NotesPost>>>);

impl NotesStore for MemoryNotes {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> BoxFuture<'a, Result<crate::notes::Notes>> {
        let posts = self.0.locked().get(thread).cloned().unwrap_or_default();
        Box::pin(async move { Ok(crate::notes::Notes { last_seq: posts.len() as u64, posts }) })
    }

    fn post<'a>(&'a self, post: &'a NotesPost, last_seq: u64) -> BoxFuture<'a, Result<bool>> {
        let mut all = self.0.locked();
        let notes = all.entry(post.thread_id.clone()).or_default();
        let stored = notes.len() as u64 == last_seq;
        if stored {
            notes.push(post.clone());
        }
        Box::pin(async move { Ok(stored) })
    }
}

/// Like [`start_with_roster`], with `notes` as the thread notes and `humans` as the humans.
async fn start_with(
    home: &Path,
    roster: Vec<RosterEntry>,
    notes: Arc<MemoryNotes>,
    humans: Registry,
) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let outbox: Outbox = Arc::new(move |envelope| {
        let _ = tx.send(envelope);
        Box::pin(async { Ok(()) })
    });
    let roster: Roster = Arc::new(move || {
        let roster = roster.clone();
        Box::pin(async move { Ok(roster) })
    });
    (Mcp::start(user("dev-a"), home, outbox, roster, notes, humans, None).await.unwrap(), rx)
}

fn listed(name: &str, capabilities: &[&str]) -> RosterEntry {
    let now = chrono::Utc::now();
    RosterEntry {
        ant: user(name),
        machine: "m1".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
        whoami: None,
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        version: ants_proto::ANTS_VERSION.to_owned(),
        perms: ants_proto::PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
    }
}

/// The human's message `dev-a` is handling on a fresh thread.
fn from_human() -> Envelope {
    Envelope::new(UserId::human(), user("dev-a"), "go")
}

async fn http_client(url: &str, token: &str) -> Client {
    let config = StreamableHttpClientTransportConfig::with_uri(url).auth_header(token);
    ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap()
}

/// The URL and token the sessions of `thread` get over HTTP.
fn http_server(mcp: &Mcp, thread: &ThreadId) -> (String, String) {
    let [McpServer::Http(server)] = &mcp.servers()(thread, Path::new("/"), true)[..] else { panic!("one HTTP server") };
    let token = server.headers[0].value.strip_prefix("Bearer ").unwrap().to_owned();
    (server.url.clone(), token)
}

async fn call(client: &Client, tool: &'static str, args: serde_json::Value) -> (bool, String) {
    let serde_json::Value::Object(args) = args else { panic!("arguments are an object") };
    let result = client.call_tool(CallToolRequestParams::new(tool).with_arguments(args)).await.unwrap();
    let text = result.content.iter().filter_map(|c| c.as_text()).map(|t| t.text.as_str()).collect();
    (result.is_error != Some(true), text)
}

#[tokio::test]
async fn send_message_follows_on_from_the_current_message() {
    let (mcp, mut sent) = start(&home("send")).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let tools: Vec<_> = client.list_all_tools().await.unwrap().into_iter().map(|t| t.name).collect();
    assert_eq!(tools.len(), 17, "{tools:?}");

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "hi"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let turn = mcp.begin(&current);
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "@dev-b", "text": "hi"})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.from, &out.to, &out.thread_id), (&user("dev-a"), &user("dev-b"), &current.thread_id));
    assert_eq!((out.turn_budget, out.in_reply_to), (current.turn_budget, None), "free after the human's message");

    let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "x", "thread": "new"})).await;
    assert!(ok);
    assert_ne!(sent.recv().await.unwrap().thread_id, current.thread_id);

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-a", "text": "me"})).await;
    assert!(!ok && text.contains("this ant"), "{text}");
    drop(turn);
    let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "late"})).await;
    assert!(!ok, "nothing goes out once the message is handled");
}

#[tokio::test]
async fn react_takes_the_current_message_or_a_named_one_and_removes() {
    let (mcp, _sent) = start(&home("react")).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(!ok && text.contains("not available"), "{text}");

    let stored: Arc<StdMutex<std::collections::BTreeSet<(Uuid, String)>>> = Arc::default();
    let kept = stored.clone();
    let (unread, silent) = (Uuid::now_v7(), Uuid::now_v7());
    mcp.with_reaction_asker(Arc::new(move |request: ants_proto::ReactionsRequest| {
        assert_eq!(request.ant.as_str(), "dev-a");
        let change = request.change.expect("a change");
        let mut stored = kept.locked();
        let entry = (request.message, change.reaction.as_str().to_owned());
        let outcome = if request.message == unread {
            Some(ants_proto::ReactionsOutcome::Unknown)
        } else if request.message == silent {
            None
        } else {
            let changed = if change.remove { stored.remove(&entry) } else { stored.insert(entry) };
            Some(ants_proto::ReactionsOutcome::Applied { changed })
        };
        Box::pin(async move { Ok(outcome) })
    }));
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(!ok && text.contains("name the message by message_id"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(ok && text == format!("Reacted eyes to message {}.", current.id), "{text}");
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(ok && text.starts_with("Already reacted eyes"), "{text}");
    let other = Uuid::now_v7();
    let args = serde_json::json!({"reaction": "check", "message_id": other.to_string()});
    assert!(call(&client, "react", args).await.0);
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "Eyes!"})).await;
    assert!(!ok, "{text}");
    let expected = std::collections::BTreeSet::from([(current.id, "eyes".to_owned()), (other, "check".to_owned())]);
    assert_eq!(*stored.locked(), expected);

    let remove = serde_json::json!({"reaction": "eyes", "remove": true});
    let (ok, text) = call(&client, "react", remove.clone()).await;
    assert!(ok && text.starts_with("Removed the reaction eyes"), "{text}");
    let (ok, text) = call(&client, "react", remove).await;
    assert!(ok && text.starts_with("No reaction eyes"), "{text}");
    assert_eq!(*stored.locked(), std::collections::BTreeSet::from([(other, "check".to_owned())]));

    let named = |id: Uuid| serde_json::json!({"reaction": "eyes", "message_id": id.to_string()});
    let (ok, text) = call(&client, "react", named(unread)).await;
    assert!(!ok && text.contains("none this ant reads") && text.contains("nothing changed"), "{text}");
    let (ok, text) = call(&client, "react", named(silent)).await;
    assert!(!ok && text.contains("ants signer"), "{text}");
}

#[tokio::test]
async fn list_reactions_shows_those_to_the_current_message_or_a_named_one() {
    let (mcp, _sent) = start(&home("list-reactions")).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({})).await;
    assert!(!ok && text.contains("not available"), "{text}");

    let (unread, silent) = (Uuid::now_v7(), Uuid::now_v7());
    let quiet = current.id;
    mcp.with_reaction_asker(Arc::new(move |request: ants_proto::ReactionsRequest| {
        assert!(request.change.is_none(), "only listing");
        let message = request.message;
        let outcome = if message == unread {
            Some(ants_proto::ReactionsOutcome::Unknown)
        } else if message == silent {
            None
        } else if message == quiet {
            Some(ants_proto::ReactionsOutcome::Listed { reactions: Vec::new() })
        } else {
            let reacted = |reaction: &str, by: &[&str]| ants_proto::Reacted {
                reaction: reaction.parse().unwrap(),
                by: by.iter().map(|b| b.parse().unwrap()).collect(),
            };
            let reactions = vec![reacted("check", &["dev-b"]), reacted("eyes", &["alice", "dev-b"])];
            Some(ants_proto::ReactionsOutcome::Listed { reactions })
        };
        Box::pin(async move { Ok(outcome) })
    }));
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({})).await;
    assert!(!ok && text.contains("name the message by message_id"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({})).await;
    assert!(ok && text == format!("No reactions to message {quiet}."), "{text}");
    let named = |id: Uuid| serde_json::json!({"message_id": id.to_string()});
    let other = Uuid::now_v7();
    let (ok, text) = call(&client, "list_reactions", named(other)).await;
    assert!(ok && text == format!("Reactions to message {other}: check by dev-b; eyes by alice, dev-b."), "{text}");
    let (ok, text) = call(&client, "list_reactions", named(unread)).await;
    assert!(ok && text.contains("none this ant reads"), "{text}");
    let (ok, text) = call(&client, "list_reactions", named(silent)).await;
    assert!(!ok && text.contains("ants signer") && text.contains("ants webhook serve"), "{text}");
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({"message_id": "nope"})).await;
    assert!(!ok && text.contains("message_id"), "{text}");
}

#[tokio::test]
async fn a_suggested_title_waits_for_the_turn_s_reply_and_goes_with_the_turn() {
    let (mcp, _sent) = start(&home("title")).await;
    let current = from_human();
    let thread = &current.thread_id;
    let (url, token) = http_server(&mcp, thread);
    let client = http_client(&url, &token).await;
    let suggest = |title: &str| call(&client, "suggest_thread_title", serde_json::json!({ "title": title }));
    let (ok, text) = suggest("Fix the build").await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let turn = mcp.begin(&current);
    for bad in ["  ", "two\nlines", &"x".repeat(MAX_TITLE + 1)] {
        let (ok, text) = suggest(bad).await;
        assert!(!ok && text.contains("invalid thread title"), "{text}");
    }
    assert!(suggest("First idea").await.0);
    let (ok, text) = suggest("  Fix the build ").await;
    assert!(ok && text.contains("\"Fix the build\""), "{text}");
    assert_eq!(mcp.take_title(thread).as_deref(), Some("Fix the build"), "the latest, trimmed");
    assert_eq!(mcp.take_title(thread), None, "taken once");
    assert!(suggest("Unsent").await.0);
    drop(turn);
    let _turn = mcp.begin(&current);
    assert_eq!(mcp.take_title(thread), None, "a suggestion ends with its turn");
}

#[tokio::test]
async fn ask_human_asks_the_thread_s_owner_without_spending_a_turn() {
    let humans = Registry::with(UserId::human(), &["alice"]);
    let (mcp, mut sent) = start_with(&home("ask-human"), Vec::new(), Arc::default(), humans).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "q").with_owner(Some(user("alice")));
    current.turn_budget = 0;
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let ask = |args: serde_json::Value| call(&client, "ask_human", args);
    let (ok, text) = ask(serde_json::json!({"question": "Ship?", "options": ["yes", "no"]})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = ask(serde_json::json!({"question": "Ship?", "options": ["yes"]})).await;
    assert!(!ok && text.contains("invalid question"), "{text}");
    let (ok, text) =
        ask(serde_json::json!({"question": " Ship? ", "options": ["yes", "not yet"], "free_text": false})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert!(text.contains(&out.id.to_string()) && text.contains("alice"), "{text}");
    assert_eq!((&out.to, &out.thread_id, out.turn_budget), (&user("alice"), &current.thread_id, 0));
    let question = out.asks().unwrap();
    assert_eq!((question.text.as_str(), question.free_text), ("Ship?", false));
    assert_eq!(out.text, question.render());
    assert_eq!(out.in_reply_to, None);

    let plain = Envelope::new(user("dev-b"), user("dev-a"), "q");
    let _turn = mcp.begin(&plain);
    let client = {
        let (url, token) = http_server(&mcp, &plain.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) =
        call(&client, "ask_human", serde_json::json!({"question": "Ship?", "options": ["yes", "no"]})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!(out.to, UserId::human(), "no owner: the default human");
    assert!(out.asks().unwrap().free_text, "typed answers by default");
}

/// A channel manager in memory: it applies requests to its channels as `ants channel serve` does, roster aside,
/// and records them; with `silent` it answers nothing.
struct MemoryAdmin {
    channels: StdMutex<Vec<ChannelEntry>>,
    requests: StdMutex<Vec<ChannelAdminRequest>>,
    silent: bool,
}

impl Admin for MemoryAdmin {
    fn channels(&self) -> BoxFuture<'_, Result<Vec<ChannelEntry>>> {
        let channels = self.channels.locked().clone();
        Box::pin(async move { Ok(channels) })
    }

    fn request(&self, request: ChannelAdminRequest) -> BoxFuture<'_, Result<Option<ChannelAdminReply>>> {
        self.requests.locked().push(request.clone());
        let reply = (!self.silent).then(|| {
            let mut channels = self.channels.locked();
            let outcome = match channels.iter_mut().find(|e| e.channel == request.channel) {
                None => ChannelAdminOutcome::Refused { refusal: ants_proto::Refusal::NoSuchChannel },
                Some(entry) => match entry.apply(&request.ant, &request.ops, &ants_proto::Humans::default()) {
                    Ok(changed) => ChannelAdminOutcome::Applied { entry: entry.clone(), revision: 2, changed },
                    Err(refusal) => ChannelAdminOutcome::Refused { refusal },
                },
            };
            ChannelAdminReply { request_id: request.id, outcome }
        });
        Box::pin(async move { Ok(reply) })
    }
}

#[tokio::test]
async fn manage_channel_asks_the_channel_manager_for_the_owner_s_changes() {
    let (mcp, _sent) = start(&home("chanadm")).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({})).await;
    assert!(!ok && text.contains("cannot be managed"), "{text}");

    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.owner = Some(user("dev-a"));
    let mut dev = ChannelEntry::new("dev".parse().unwrap());
    dev.owner = Some(user("lead"));
    let admin =
        Arc::new(MemoryAdmin { channels: StdMutex::new(vec![ops, dev]), requests: StdMutex::default(), silent: false });
    mcp.manage_channels(admin.clone());
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({})).await;
    assert!(ok && text.contains("#ops") && !text.contains("#dev"), "{text}");
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({"channel": "#dev"})).await;
    assert_eq!((ok, text.as_str()), (true, "#dev: members none; messenger messenger (default); owner lead."));
    assert!(admin.requests.locked().is_empty(), "showing asks nothing");

    let args = serde_json::json!({"channel": "#ops", "add": ["@dev-b", "dev-c"], "messenger": "dev-b"});
    let (ok, text) = call(&client, "manage_channel", args).await;
    assert!(ok && text.starts_with("Changed #ops: added dev-b; added dev-c; messenger dev-b."), "{text}");
    let request = admin.requests.locked()[0].clone();
    assert_eq!((&request.ant, &request.thread_id), (&user("dev-a"), &current.thread_id));
    let (ok, text) = call(&client, "manage_channel", serde_json::json!({"channel": "ops", "add": ["dev-b"]})).await;
    assert!(ok && text.starts_with("Nothing to change."), "{text}");

    let (ok, text) = call(&client, "manage_channel", serde_json::json!({"channel": "dev", "remove": ["x"]})).await;
    assert!(!ok && text.contains("#dev was not changed") && text.contains("lead"), "{text}");
    let both = serde_json::json!({"channel": "ops", "messenger": "dev-b", "clear_messenger": true});
    let (ok, text) = call(&client, "manage_channel", both).await;
    assert!(!ok && text.contains("not both"), "{text}");

    let silent = Arc::new(MemoryAdmin { channels: StdMutex::default(), requests: StdMutex::default(), silent: true });
    let (mcp, _sent) = start(&home("chanadm-silent")).await;
    mcp.manage_channels(silent);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) =
        call(&client, "manage_channel", serde_json::json!({"channel": "ops", "clear_messenger": true})).await;
    assert!(!ok && text.contains("no channel manager answered") && text.contains("ants channel"), "{text}");
}

#[tokio::test]
async fn messages_between_ants_spend_the_budget() {
    let (mcp, mut sent) = start(&home("budget")).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "q");
    current.turn_budget = 1;
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "dev-c", "text": "?"})).await;
    assert!(!ok && text.contains("no turns left"), "{text}");
    let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "fyi"})).await;
    assert!(ok);
    assert_eq!(sent.recv().await.unwrap().turn_budget, 0);

    current.turn_budget = 0;
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "more"})).await;
    assert!(!ok && text.contains("no turns left"), "{text}");
}

#[tokio::test]
async fn decide_approval_answers_a_request_from_the_chain() {
    let (mcp, mut sent) = start(&home("decide")).await;
    let tool = ants_proto::ToolCall {
        kind: "execute".to_owned(),
        title: "rm -rf target".to_owned(),
        locations: Vec::new(),
        input: None,
    };
    let request = ants_proto::ApprovalRequest::new(
        user("dev-b"),
        ThreadId::generate(),
        tool,
        vec![Verdict::AllowOnce, Verdict::Reject],
        Duration::from_secs(60),
    );
    let ants_proto::Hop::Chain { escalation, .. } =
        ants_proto::Escalation::of_request(&request).up(&[user("dev-a")], 3)
    else {
        panic!("a chain hop")
    };
    let asked = Envelope::on_thread(request.thread_id.clone(), user("dev-b"), user("dev-a"), "may it?")
        .with_owner(Some(UserId::human()))
        .with_escalation(Some(escalation));
    let client = {
        let (url, token) = http_server(&mcp, &asked.thread_id);
        http_client(&url, &token).await
    };

    let plain = Envelope::on_thread(asked.thread_id.clone(), user("dev-b"), user("dev-a"), "hi");
    let turn = mcp.begin(&plain);
    let (ok, text) = call(&client, "decide_approval", serde_json::json!({"verdict": "reject"})).await;
    assert!(!ok && text.contains("no approval request"), "{text}");
    drop(turn);

    let _turn = mcp.begin(&asked);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "unsure"})).await;
    assert!(!ok && text.contains("dev-b's approval request, which goes no higher"), "{text}");
    let (ok, text) = call(&client, "decide_approval", serde_json::json!({"verdict": "maybe"})).await;
    assert!(!ok && text.contains("unknown verdict"), "{text}");
    let (ok, text) = call(&client, "decide_approval", serde_json::json!({"verdict": "allow-once"})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.from, &out.to, &out.thread_id), (&user("dev-a"), &user("dev-b"), &request.thread_id));
    assert_eq!(out.owner, Some(UserId::human()));
    assert_eq!(out.chain_decision(&request, &[user("dev-a")]).unwrap().verdict, Verdict::AllowOnce);
    assert!(sent.try_recv().is_err(), "one decision");
}

#[tokio::test]
async fn registered_humans_are_humans_to_the_tools() {
    let humans = Registry::with(UserId::human(), &["alice"]);
    let (mcp, mut sent) = start_with(&home("humans"), Vec::new(), Arc::default(), humans).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "q").with_owner(Some(user("alice")));
    current.turn_budget = 0;
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "alice", "text": "?"})).await;
    assert!(!ok && text.contains("send_message reaches alice"), "{text}");
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "alice", "text": "stuck"})).await;
    assert!(ok, "a message to a human spends no turn: {text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((out.to.as_str(), out.turn_budget, &out.owner), ("alice", 0, &current.owner));

    // A registered human's message makes messages to ants free, and its owner goes on to threads started from it.
    let mut current = Envelope::new(user("alice"), user("dev-a"), "go").with_owner(Some(user("alice")));
    current.turn_budget = 1;
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) =
        call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "x", "thread": "new"})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_ne!(out.thread_id, current.thread_id);
    assert_eq!((out.turn_budget, &out.owner), (1, &current.owner));
}

#[tokio::test]
async fn requests_are_routed_by_roster_capabilities() {
    let mut billing = listed("dev-c", &["Rust", "billing"]);
    billing.whoami = Some("Maintain billing services.\nReview invoices.".to_owned());
    let mut docs = listed("docs", &[]);
    docs.activity = ants_proto::Activity::Working;
    docs.agent = Some(ants_proto::HarnessAgent { name: "copilot".to_owned(), version: "1.0.3".to_owned() });
    let mut busy = listed("dev-b", &["rust"]);
    busy.activity = ants_proto::Activity::Working;
    busy.handling = 1;
    let mut out = listed("ops", &["billing"]);
    let since = "2026-01-01T00:00:00Z".parse().unwrap();
    out.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: Some("copilot login".to_owned()) });
    let mut limited = listed("dev-q", &[]);
    let until = "2026-01-01T00:10:00Z".parse().unwrap();
    limited.limited = Some(ants_proto::HarnessLimit { kind: ants_proto::LimitKind::Quota, since, until });
    limited.memory_low = Some(ants_proto::MemoryLow { since, available: 512 << 20, max_harnesses: 1 });
    let roster = vec![listed("dev-a", &["rust"]), billing, out, busy, docs, limited];
    let (mcp, mut sent) = start_with_roster(&home("route"), roster).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;

    let (ok, text) = call(&client, "list_ants", serde_json::json!({})).await;
    assert!(
        ok && !text.contains("dev-a:")
            && text.contains("docs: no capabilities (copilot running copilot 1.0.3 on m1, working)"),
        "{text}"
    );
    assert!(text.contains("whoami (advertised):\nMaintain billing services.\nReview invoices."), "{text}");
    assert!(
        text.contains(
            "ops: billing (copilot on m1, signed out)\n  harness signed out since 2026-01-01T00:00:00Z: messages \
                 to ops wait unanswered until a human signs it in (`copilot login` on m1)"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "dev-q: no capabilities (copilot on m1, out-of-quota)\n  harness held back by a provider limit \
                 (out-of-quota): messages to dev-q wait until about 2026-01-01T00:10:00Z\n  memory low on m1 since \
                 2026-01-01T00:00:00Z: at most 1 harness runs at once, so messages may wait longer"
        ),
        "{text}"
    );
    let (ok, text) = call(&client, "list_ants", serde_json::json!({"capability": "billing"})).await;
    assert!(ok && text.starts_with("dev-c: Rust, billing (copilot on m1, idle)") && !text.contains("dev-b"), "{text}");
    assert!(text.contains("Maintain billing services.\nReview invoices."), "{text}");

    // dev-a has the capability too, but does not route to itself; dev-b comes before dev-c by name, but works.
    let (ok, text) =
        call(&client, "route", serde_json::json!({"capability": "RUST", "text": "fix it", "wait": false})).await;
    assert!(ok && text.starts_with("Routed to dev-c for RUST."), "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.to, out.text.as_str(), &out.thread_id), (&user("dev-c"), "fix it", &current.thread_id));

    let asking = tokio::spawn(async move {
        call(&client, "route", serde_json::json!({"capability": "billing", "text": "invoice?"})).await
    });
    let ask = sent.recv().await.unwrap();
    assert_eq!(ask.to, user("dev-c"), "not ops: idle, but signed out");
    let reply = ask.reply(user("dev-c"), "paid", Role::Ant).unwrap();
    mcp.hub.route(reply.clone()).await;
    let (ok, text) = asking.await.unwrap();
    let routed = format!("Routed to dev-c for billing. dev-c replied (message {}):\npaid", reply.id);
    assert!(ok && text == routed, "{text}");

    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "route", serde_json::json!({"capability": "go", "text": "?"})).await;
    assert!(!ok && text.contains("Capabilities there: billing, rust."), "{text}");
    assert!(sent.try_recv().is_err(), "nothing is sent without an ant to take it");
}

/// Looks ants up in `roster`.
fn lookup_in(roster: Vec<RosterEntry>) -> RosterLookup {
    Arc::new(move |ant| {
        let entry = roster.iter().find(|e| e.ant == ant).cloned();
        Box::pin(async move { Ok(entry) })
    })
}

#[tokio::test]
async fn request_and_retire_ant_are_offered_to_managers_and_request_returns_once_the_ant_is_in_the_roster() {
    let (mcp, _sent) = start(&home("request")).await;
    let thread = ThreadId::generate();
    let (url, token) = http_server(&mcp, &thread);
    let names = async |client: &Client| -> Vec<String> {
        client.list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect()
    };
    let offered = names(&http_client(&url, &token).await).await;
    assert!(!offered.contains(&"request_ant".to_owned()) && !offered.contains(&"retire_ant".to_owned()));

    let asked = Arc::new(StdMutex::new(Vec::<AntAsk>::new()));
    let retired = Arc::new(StdMutex::new(Vec::<RetireAsk>::new()));
    let (seen, seen_retired) = (asked.clone(), retired.clone());
    mcp.with_requester(Arc::new(move |ask: ManagerAsk| {
        let ask = match ask {
            ManagerAsk::Add(ask) => *ask,
            ManagerAsk::Retire(ask) => {
                seen_retired.locked().push(ask);
                return Box::pin(async { Ok("Retired dev-a-docs.".to_owned()) });
            }
        };
        seen.locked().push(ask.clone());
        Box::pin(async move {
            match ask.name.as_str() {
                "dev-a-docs" => Ok("Added dev-a-docs.".to_owned()),
                _ => Err("the signer refused it".to_owned()),
            }
        })
    }));
    mcp.with_lookup(lookup_in(vec![listed("dev-a-docs", &[])]));
    let client = http_client(&url, &token).await;
    let offered = names(&client).await;
    assert!(offered.contains(&"request_ant".to_owned()) && offered.contains(&"retire_ant".to_owned()));

    let args = serde_json::json!({
        "name": "dev-a-docs", "template": "worker", "reason": "docs",
        "overrides": {"model": "gpt-5", "prompt_timeout": "10m"},
    });
    let (ok, text) = call(&client, "request_ant", args).await;
    assert!(ok && text.starts_with("Added dev-a-docs.") && text.contains("is in the roster"), "{text}");
    let ask = asked.locked()[0].clone();
    assert_eq!(
        (ask.manager, ask.thread, ask.template, ask.wait),
        (user("dev-a"), thread.clone(), "worker".to_owned(), DEFAULT_REQUEST_WAIT)
    );
    assert_eq!(ask.request.model.as_deref(), Some("gpt-5"));
    assert_eq!(ask.request.prompt_timeout, Some(Duration::from_mins(10)));

    let args = serde_json::json!({"name": "dev-a-x", "template": "worker", "reason": "x", "timeout": "1m"});
    let (ok, text) = call(&client, "request_ant", args).await;
    assert!(!ok && text.contains("the signer refused it"), "{text}");
    assert_eq!(asked.locked()[1].wait, Duration::from_mins(1));
    let args = serde_json::json!({"name": "dev-a-y", "template": "worker", "reason": "y", "overrides": {"x": 1}});
    let (ok, text) = call(&client, "request_ant", args).await;
    assert!(!ok && text.contains("overrides"), "{text}");
    assert_eq!(asked.locked().len(), 2, "a malformed request goes nowhere");

    let (ok, text) = call(&client, "retire_ant", serde_json::json!({"name": " dev-a-docs "})).await;
    assert!(ok && text == "Retired dev-a-docs.", "{text}");
    let ask = retired.locked()[0].clone();
    assert_eq!((ask.manager, ask.thread, ask.name), (user("dev-a"), thread, user("dev-a-docs")));
}

#[test]
fn a_provider_limit_counts_only_while_it_outlasts_the_wait() {
    let now = chrono::Utc::now();
    let mut entry = listed("dev-q", &[]);
    let until = now + chrono::Duration::minutes(10);
    entry.limited = Some(ants_proto::HarnessLimit { kind: ants_proto::LimitKind::Quota, since: now, until });
    let ant = user("dev-q");
    let why = why_unserved(&ant, Some(&entry), now, Duration::ZERO).expect("held now");
    assert!(why.starts_with("the harness of dev-q (copilot on m1) is out of quota: messages to dev-q wait"), "{why}");
    assert!(why_unserved(&ant, Some(&entry), now, Duration::from_mins(5)).is_some(), "past a 5m ask");
    assert_eq!(why_unserved(&ant, Some(&entry), now, Duration::from_mins(30)), None, "over within a 30m ask");
    entry.limited = None;
    assert_eq!(why_unserved(&ant, Some(&entry), now, Duration::ZERO), None);
    entry.failing = Some(ants_proto::HarnessFailing { since: now, error: "not found".to_owned() });
    let why = why_unserved(&ant, Some(&entry), now, Duration::from_mins(30)).expect("held until fixed");
    assert!(why.starts_with("the harness of dev-q (copilot on m1) fails to start since"), "{why}");

    let since = "2026-01-01T00:00:00Z".parse().unwrap();
    entry.fallback = Some(ants_proto::ServingFallback { name: "claude".to_owned(), since });
    assert_eq!(why_unserved(&ant, Some(&entry), now, Duration::ZERO), None, "a fallback answers");
    assert!(
        describe(&[&entry]).contains(
            "\n  fallback claude answers messages to dev-q since 2026-01-01T00:00:00Z, until its own harness can again"
        ),
        "{}",
        describe(&[&entry])
    );
}

#[tokio::test]
async fn messages_to_an_ant_no_daemon_serves_say_so() {
    let mut stale = listed("dev-c", &["rust"]);
    stale.heartbeat_at -= chrono::Duration::minutes(10);
    let mut out = listed("ops", &["billing"]);
    let since = "2026-01-01T00:00:00Z".parse().unwrap();
    out.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: None });
    let roster = vec![listed("dev-a", &["rust"]), listed("dev-b", &["rust"]), stale, out];
    let humans = Registry::with(UserId::human(), &["alice"]);
    let (mcp, mut sent) = start_with(&home("unserved"), roster.clone(), Arc::default(), humans).await;
    mcp.with_lookup(lookup_in(roster));
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;

    let ask = |to: &str| serde_json::json!({"to": to, "text": "?"});
    let (ok, text) = call(&client, "ask_ant", ask("dev-bb")).await;
    assert!(!ok, "{text}");
    assert_eq!(
        text,
        "Not sent, as dev-bb is not in the roster: no daemon serves dev-bb now. Closest names in the roster: \
             dev-b, dev-c. send_message leaves a message waiting in the inbox of dev-bb instead."
    );
    let (ok, text) = call(&client, "ask_ant", ask("zed")).await;
    assert!(!ok && text.contains("now. In the roster: dev-b, dev-c, ops."), "{text}");
    let (ok, text) = call(&client, "ask_ant", ask("dev-c")).await;
    assert!(!ok && text.contains("dev-c is stale in the roster: the daemon on m1 last beat 10m"), "{text}");
    assert!(text.contains("Served ants sharing a capability of dev-c: dev-b."), "{text}");
    let (ok, text) = call(&client, "ask_ant", ask("ops")).await;
    assert!(
        !ok && text.contains(
            "the harness of ops (copilot on m1) is signed out since 2026-01-01T00:00:00Z: messages to ops wait \
                 unanswered until a human signs the harness in. No served ant shares a capability of ops"
        ),
        "{text}"
    );
    assert!(sent.try_recv().is_err(), "an ask no daemon can answer is not sent");

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-bb", "text": "hi"})).await;
    assert!(ok, "{text}");
    assert!(
        text.ends_with(
            "turns left). Warning: the message waits in the inbox of dev-bb, as dev-bb is not in the roster: no \
                 daemon serves dev-bb now. Closest names in the roster: dev-b, dev-c."
        ),
        "{text}"
    );
    assert_eq!(sent.recv().await.unwrap().to, user("dev-bb"), "sent all the same");
    for to in ["dev-b", "alice", "human"] {
        let (ok, text) = call(&client, "send_message", serde_json::json!({"to": to, "text": "hi"})).await;
        assert!(ok && !text.contains("Warning"), "{to}: {text}");
        assert_eq!(sent.recv().await.unwrap().to, user(to));
    }

    // A roster that cannot be read holds nothing back.
    let (mcp, mut sent) = start(&home("unserved-unread")).await;
    mcp.with_lookup(Arc::new(|_| Box::pin(async { Err(anyhow::anyhow!("no roster")) })));
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-x", "text": "hi"})).await;
    assert!(ok && !text.contains("Warning"), "{text}");
    assert_eq!(sent.recv().await.unwrap().to, user("dev-x"));
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "dev-x", "text": "?", "timeout": "1s"})).await;
    assert!(!ok && text.starts_with("no reply from dev-x within 1s"), "{text}");
    assert_eq!(sent.recv().await.unwrap().to, user("dev-x"));
}

#[tokio::test]
async fn notes_posts_spend_the_budget_then_the_thread_goes_to_its_owner() {
    let notes = Arc::new(MemoryNotes::default());
    let humans = Registry::with(UserId::human(), &["alice"]);
    let (mcp, mut sent) = start_with(&home("notes"), Vec::new(), notes.clone(), humans).await;
    let mut current =
        Envelope::new(user("dev-b"), user("dev-a"), "plan the release with me").with_owner(Some(user("alice")));
    current.turn_budget = 5;
    let thread = current.thread_id.clone();
    let client = {
        let (url, token) = http_server(&mcp, &thread);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "early"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "read_notes", serde_json::json!({})).await;
    assert!(ok && text.ends_with("has no notes yet."), "{text}");
    // dev-b posted already and left two turns, fewer than the message has.
    let theirs = NotesPost::spend(thread.clone(), user("dev-b"), "I take the docs", 3, None).unwrap();
    notes.0.locked().insert(thread.clone(), vec![theirs]);
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "I take the code"})).await;
    assert!(ok && text.contains("(1 turns left)"), "{text}");
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "ship friday?"})).await;
    assert!(ok && text.contains("; alice has the thread now"), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&user("dev-a"), &user("alice"), &thread));
    assert_eq!(notice.owner, Some(user("alice")));
    assert!(
        notice.text.contains("spent their turn budget") && notice.text.ends_with("a fresh budget."),
        "{}",
        notice.text
    );
    assert!(notice.text.contains("Last post from dev-a:\nship friday?"), "{}", notice.text);

    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "one more"})).await;
    assert!(!ok && text.contains("no turns left"), "{text}");
    assert!(sent.try_recv().is_err(), "the owner is told once");
    let (ok, text) = call(&client, "read_notes", serde_json::json!({"thread": thread.as_str()})).await;
    assert!(ok && text.starts_with(&format!("Notes of thread {thread}: 3 posts; 0 turns left.")), "{text}");
    assert!(text.contains("dev-b at ") && text.ends_with("ship friday?"), "{text}");

    // The human's post gives the notes a fresh budget, within the message's.
    notes.0.locked().get_mut(&thread).unwrap().push(NotesPost::new(thread.clone(), UserId::human(), "go"));
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "back to it"})).await;
    assert!(ok && text.contains("(4 turns left)"), "{text}");
}

/// An MCP server for `dev-a` escalating up `escalate_to`, whose sent envelopes come out of the receiver.
async fn start_escalating(home: &Path, escalate_to: &[&str]) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    start_escalating_with(home, escalate_to, Vec::new()).await
}

async fn start_escalating_with(
    home: &Path,
    escalate_to: &[&str],
    roster: Vec<RosterEntry>,
) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (outbox_tx, send_tx) = (tx.clone(), tx);
    let outbox: Outbox = Arc::new(move |envelope| {
        let _ = outbox_tx.send(envelope);
        Box::pin(async { Ok(()) })
    });
    let send: crate::escalation::Sender = Arc::new(move |envelope, _id| {
        let _ = send_tx.send(envelope);
        Box::pin(async { Ok(()) })
    });
    let roster: Roster = Arc::new(move || {
        let roster = roster.clone();
        Box::pin(async move { Ok(roster) })
    });
    let chain =
        crate::escalation::Chain { escalate_to: escalate_to.iter().map(|p| user(p)).collect(), ..Default::default() };
    let ledger = Arc::new(crate::caps::Ledger::memory(user("dev-a"), crate::caps::Caps::default()));
    let humans = Registry::default();
    let escalator = Escalator::new(user("dev-a"), chain, roster.clone(), humans.clone(), ledger, send);
    let notes = Arc::new(MemoryNotes::default());
    let mcp = Mcp::start(user("dev-a"), home, outbox, roster, notes, humans, Some(Arc::new(escalator)));
    (mcp.await.unwrap(), rx)
}

#[tokio::test]
async fn escalate_goes_up_the_chain_and_only_the_leader_sends_it_higher() {
    let (mcp, mut sent) = start_escalating(&home("escalate"), &["lead", "dev-b"]).await;
    let current = from_human();
    let thread = current.thread_id.clone();
    let client = {
        let (url, token) = http_server(&mcp, &thread);
        http_client(&url, &token).await
    };
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "the schema is unclear"})).await;
    assert!(ok && text.contains("to level 1: sent to lead, dev-b; lead leads"), "{text}");
    for to in ["lead", "dev-b"] {
        let envelope = sent.try_recv().unwrap();
        assert_eq!(envelope.to, user(to));
        let escalation = envelope.escalation.as_deref().unwrap();
        assert_eq!((escalation.level, escalation.reason), (1, EscalationReason::Asked));
        assert!(envelope.text.contains("dev-a asks for help: the schema is unclear"), "{}", envelope.text);
    }
    assert!(sent.try_recv().is_err());

    // On dev-c's escalation, led by lead, dev-a is a member: it neither escalates nor answers dev-c.
    let start = ants_proto::Escalation::new(user("dev-c"), thread.clone(), EscalationReason::TurnBudget);
    let ants_proto::Hop::Chain { escalation: theirs, .. } = start.up(&[user("lead"), user("dev-a")], 3) else {
        panic!("a chain")
    };
    let member = Envelope::on_thread(thread.clone(), user("dev-c"), user("dev-a"), "help dev-c")
        .with_escalation(Some(theirs.clone()));
    drop(_turn);
    let _turn = mcp.begin(&member);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "no idea"})).await;
    assert!(!ok && text.contains("only lead escalates the escalation of dev-c's thread"), "{text}");
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "try X"})).await;
    assert!(!ok && text.contains("only lead answers dev-c"), "{text}");
    assert!(sent.try_recv().is_err());

    // Led by dev-a, it goes up dev-a's chain, one level above, and dev-c hears of it.
    let other = ThreadId::generate();
    let start = ants_proto::Escalation::new(user("dev-c"), other.clone(), EscalationReason::TurnBudget);
    let ants_proto::Hop::Chain { escalation: led, .. } = start.up(&[user("dev-a")], 3) else { panic!("a chain") };
    let leading =
        Envelope::on_thread(other.clone(), user("dev-c"), user("dev-a"), "help dev-c").with_escalation(Some(led));
    let client = {
        let (url, token) = http_server(&mcp, &other);
        http_client(&url, &token).await
    };
    let _other = mcp.begin(&leading);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "beyond dev-a"})).await;
    assert!(ok && text.contains("to level 2: sent to lead, dev-b"), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!(notice.to, user("dev-c"));
    assert_eq!(notice.escalation.as_deref().map(|e| (e.level, &e.leader)), Some((2, &user("lead"))));
    let to: Vec<_> = std::iter::from_fn(|| sent.try_recv().ok()).map(|e| e.to).collect();
    assert_eq!(to, [user("lead"), user("dev-b")]);
}

#[tokio::test]
async fn escalate_on_a_thread_another_ant_created_goes_up_the_creator_s_chain() {
    let lead = RosterEntry { escalate_to: vec![user("boss")], ..listed("lead", &[]) };
    let (mcp, mut sent) = start_escalating_with(&home("escalate-creator"), &["dev-b"], vec![lead]).await;
    let current = Envelope::new(user("lead"), user("dev-a"), "fix it").with_creator(Some(user("lead")));
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(ok && text.contains("to level 1: sent to boss; boss leads"), "{text}");
    let envelope = sent.try_recv().unwrap();
    assert_eq!((&envelope.to, &envelope.creator), (&user("boss"), &Some(user("lead"))));
    let escalation = envelope.escalation.as_deref().unwrap();
    assert_eq!((escalation.level, &escalation.stuck), (1, &user("dev-a")));
    assert_eq!(escalation.visited, [user("dev-a"), user("lead"), user("boss")]);
    assert!(sent.try_recv().is_err(), "dev-a's own chain is not used");

    // A creator with no chain sends it to the owner, else the default human.
    let (mcp, mut sent) = start_escalating_with(&home("escalate-creator-none"), &["dev-b"], vec![]).await;
    let current = Envelope::new(user("lead"), user("dev-a"), "fix it").with_creator(Some(user("lead")));
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(ok && text.contains("to its owner human"), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!(notice.to, UserId::human());
    assert!(notice.text.contains("starts at lead, which created thread"), "{}", notice.text);
}

fn channels(entries: Vec<ChannelEntry>, posted: Arc<StdMutex<Vec<ChannelPost>>>) -> ChannelAccess {
    ChannelAccess {
        entry: Arc::new(move |channel| {
            let entry = entries.iter().find(|e| e.channel == channel).cloned();
            Box::pin(async move { Ok(entry) })
        }),
        post: Arc::new(move |post| {
            posted.locked().push(post);
            Box::pin(async { Ok(()) })
        }),
    }
}

#[tokio::test]
async fn post_channel_starts_a_thread_the_ant_created_where_it_may_post() {
    let (mcp, _sent) = start(&home("post-channel")).await;
    let current = from_human();
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let post = |channel: &str, text: &str| serde_json::json!({"channel": channel, "text": text});
    let (ok, text) = call(&client, "post_channel", post("ops", "hi")).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");
    let turn = mcp.begin(&current);
    let (ok, text) = call(&client, "post_channel", post("ops", "hi")).await;
    assert!(!ok && text.contains("channels are not served here"), "{text}");

    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(user("dev-a"));
    ops.add_member(user("dev-b"));
    ops.add_member(UserId::human());
    let mut dev = ChannelEntry::new("dev".parse().unwrap());
    dev.add_member(user("dev-b"));
    let mut triage = ChannelEntry::new("triage".parse().unwrap());
    triage.messenger = Some(user("dev-a"));
    let posted = Arc::new(StdMutex::new(Vec::new()));
    mcp.with_channels(channels(vec![ops, dev, triage], posted.clone()));

    let (ok, text) = call(&client, "post_channel", post("#ops", "@dev-b @human check the build")).await;
    assert!(ok && text.contains("that dev-a created (8 turns left); it wakes dev-b and notifies human."), "{text}");
    let got = posted.locked().pop().unwrap();
    assert_eq!((got.channel.as_str(), &got.from, got.in_reply_to), ("ops", &user("dev-a"), None));
    assert_eq!((&got.creator, &got.owner), (&Some(user("dev-a")), &None));
    assert_eq!(got.turn_budget, ants_proto::DEFAULT_TURN_BUDGET, "a human's message spends no turn");
    assert!(text.contains(&format!("starting thread {}", got.thread_id)) && got.thread_id != current.thread_id);

    let (ok, text) = call(&client, "post_channel", post("triage", "noted")).await;
    assert!(ok && text.contains("it wakes nobody"), "the messenger posts: {text}");
    let (ok, text) = call(&client, "post_channel", post("dev", "hi")).await;
    assert!(!ok && text.contains("dev-a is neither a member nor the messenger of #dev"), "{text}");
    let (ok, text) = call(&client, "post_channel", post("qa", "hi")).await;
    assert!(!ok && text.contains("there is no channel #qa"), "{text}");

    // Between ants, the post gets the turns the message leaves, and none left refuses it.
    drop(turn);
    let mut asked = Envelope::on_thread(current.thread_id.clone(), user("dev-b"), user("dev-a"), "post it");
    asked.turn_budget = 2;
    let turn = mcp.begin(&asked);
    let (ok, text) = call(&client, "post_channel", post("ops", "@dev-b go")).await;
    assert!(ok && text.contains("(1 turns left)"), "{text}");
    drop(turn);
    asked.turn_budget = 0;
    let _turn = mcp.begin(&asked);
    let (ok, text) = call(&client, "post_channel", post("ops", "@dev-b go")).await;
    assert!(!ok && text.contains("has no turns left"), "{text}");
    assert_eq!(posted.locked().len(), 2);
}

#[tokio::test]
async fn escalate_without_a_chain_goes_to_the_owner() {
    let (mcp, mut sent) = start_escalating(&home("escalate-owner"), &[]).await;
    let current = from_human();
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(ok && text.ends_with("to its owner human: dev-a has no escalate_to chain."), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!((notice.to, notice.in_reply_to), (UserId::human(), Some(current.id)));
}

#[tokio::test]
async fn http_needs_a_known_token() {
    let (mcp, _sent) = start(&home("auth")).await;
    let (url, _) = http_server(&mcp, &from_human().thread_id);
    let config = StreamableHttpClientTransportConfig::with_uri(url).auth_header("nope");
    let refused = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await;
    assert!(refused.is_err());
}

#[tokio::test]
async fn a_bad_timeout_is_refused_before_anything_is_sent() {
    let (mcp, mut sent) = start(&home("timeout")).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    for (timeout, why) in [("300", "without a unit"), ("0", "out of range"), ("31m", "at most 30m")] {
        for (tool, args) in [
            ("ask_ant", serde_json::json!({"to": "dev-b", "text": "?", "timeout": timeout})),
            ("route", serde_json::json!({"capability": "rust", "text": "?", "timeout": timeout})),
        ] {
            let (ok, text) = call(&client, tool, args).await;
            assert!(!ok && text.contains("timeout") && text.contains(why), "{tool} {timeout}: {text}");
        }
    }
    assert!(sent.try_recv().is_err(), "nothing sent");
}

#[tokio::test]
async fn ask_ant_gets_the_reply_while_the_inbox_consumer_holds_it() {
    let (mcp, mut sent) = start(&home("ask")).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let asking =
        tokio::spawn(async move { call(&client, "ask_ant", serde_json::json!({"to": "dev-b", "text": "6*7?"})).await });
    let ask = sent.recv().await.unwrap();
    assert_eq!((&ask.to, &ask.thread_id), (&user("dev-b"), &current.thread_id));

    // Someone else answering does not count.
    let mut forged = ask.reply(user("dev-c"), "13", Role::Ant).unwrap();
    forged.in_reply_to = Some(ask.id);
    mcp.hub.route(forged.clone()).await;
    assert!(!mcp.claim(&forged).await.unwrap(), "a forged reply is an ordinary message");

    let reply = ask.reply(user("dev-b"), "42", Role::Ant).unwrap();
    mcp.hub.route(reply.clone()).await;
    let (ok, text) = asking.await.unwrap();
    assert!(ok && text.ends_with("42"), "{text}");
    assert!(mcp.claim(&reply).await.unwrap(), "the consumer's copy is acked, not prompted");
    assert!(!mcp.claim(&reply).await.unwrap(), "the ask is settled");
}

#[tokio::test]
async fn the_consumer_can_deliver_the_reply_too() {
    let (mcp, mut sent) = start(&home("ask-js")).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let asking =
        tokio::spawn(async move { call(&client, "ask_ant", serde_json::json!({"to": "dev-b", "text": "?"})).await });
    let reply = sent.recv().await.unwrap().reply(user("dev-b"), "yes", Role::Ant).unwrap();
    assert!(mcp.claim(&reply).await.unwrap());
    assert!(asking.await.unwrap().1.ends_with("yes"));
    mcp.hub.route(reply).await;
}

#[tokio::test]
async fn a_late_reply_waits_in_check_inbox_across_restarts() {
    let dir = home("late");
    let (mcp, mut sent) = start(&dir).await;
    let current = from_human();
    let turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "dev-b", "text": "?", "timeout": "1s"})).await;
    assert!(!ok && text.contains("check_inbox"), "{text}");
    let ask = sent.recv().await.unwrap();
    drop((turn, client, mcp));

    // The reply comes after a restart: it is still known as a reply, not a new message.
    let (mcp, _sent) = start(&dir).await;
    let reply = ask.reply(user("dev-b"), "late answer", Role::Ant).unwrap();
    assert!(mcp.claim(&reply).await.unwrap());
    assert!(mcp.claim(&reply).await.is_ok_and(|claimed| !claimed), "kept once");
    drop(mcp);

    let (mcp, _sent) = start(&dir).await;
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    assert_eq!(mcp.late_on(&current.thread_id).await, 1);
    assert_eq!(mcp.late_on(&ThreadId::generate()).await, 0, "only its own thread's");
    let (ok, text) = call(&client, "check_inbox", serde_json::json!({})).await;
    assert!(ok && text.contains("From dev-b") && text.contains("late answer"), "{text}");
    assert_eq!(mcp.late_on(&current.thread_id).await, 0, "taken");
    let (_, text) = call(&client, "check_inbox", serde_json::json!({"all_threads": true})).await;
    assert_eq!(text, "No messages.");
}

#[tokio::test]
async fn the_socket_serves_the_tools_after_the_token() {
    let (mcp, mut sent) = start(&home("sock")).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let [McpServer::Stdio(server)] = &mcp.servers()(&current.thread_id, Path::new("/"), false)[..] else {
        panic!("stdio")
    };
    assert_eq!(server.args[..2], ["mcp", "--socket"]);
    let token = &server.env.iter().find(|e| e.name == TOKEN_ENV).unwrap().value;

    let mut stream = ipc::connect(Path::new(&server.args[2])).await.unwrap();
    stream.write_all(format!("{token}\n").as_bytes()).await.unwrap();
    let client = ClientConfig::default().serve(stream).await.unwrap();
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "human", "text": "done"})).await;
    assert!(ok, "{text}");
    assert!(sent.recv().await.unwrap().to.is_human());

    let mut stranger = ipc::connect(Path::new(&server.args[2])).await.unwrap();
    stranger.write_all(b"nope\n").await.unwrap();
    assert!(ClientConfig::default().serve(stranger).await.is_err());
}

/// Fake harnesses that note the MCP servers each session gets and, on a prompt, send the prompt's text to dev-b
/// through the HTTP one.
fn fake_pool(mcp: &Mcp, http: bool, seen: Arc<StdMutex<Vec<Vec<McpServer>>>>) -> Pool {
    let connect = Arc::new(move || {
        let (on_new, on_load, on_prompt) = (seen.clone(), seen.clone(), seen.clone());
        DynConnectTo::new(
            Agent
                .v1()
                .name("fake")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        let caps = AgentCapabilities::new()
                            .load_session(true)
                            .mcp_capabilities(McpCapabilities::new().http(http));
                        responder.respond(InitializeResponse::new(init.protocol_version).agent_capabilities(caps))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |new: NewSessionRequest, responder, _cx| {
                        let mut seen = on_new.locked();
                        seen.push(new.mcp_servers);
                        responder.respond(NewSessionResponse::new(format!("s{}", seen.len())))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |load: LoadSessionRequest, responder, _cx| {
                        on_load.locked().push(load.mcp_servers);
                        responder.respond(LoadSessionResponse::new())
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |prompt: PromptRequest, responder, cx| {
                        let AcpContent::Text(t) = &prompt.prompt[0] else { unreachable!() };
                        let server = on_prompt.locked().last().cloned().unwrap_or_default();
                        let answer = match server.first() {
                            Some(McpServer::Http(server)) => {
                                let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
                                let client = http_client(&server.url, token).await;
                                let args = serde_json::json!({"to": "dev-b", "text": t.text});
                                call(&client, "send_message", args).await.1
                            }
                            _ => "no tools".to_owned(),
                        };
                        cx.send_notification(SessionNotification::new(
                            prompt.session_id,
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(AcpContent::Text(TextContent::new(
                                answer,
                            )))),
                        ))?;
                        responder.respond(PromptResponse::new(StopReason::EndTurn))
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    let launch = Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_mcp(mcp.servers());
    Pool::new(launch, SessionStore::memory(), 1, Duration::from_secs(60))
}

#[tokio::test]
async fn sessions_get_the_tools_of_their_thread() {
    let (mcp, mut sent) = start(&home("pool")).await;
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let pool = fake_pool(&mcp, true, seen.clone());
    let (one, two) = (from_human(), from_human());

    for envelope in [&one, &two, &one] {
        let _turn = mcp.begin(envelope);
        let answer = pool.prompt(&envelope.thread_id, &envelope.text).await.unwrap();
        assert!(answer.starts_with("Sent to dev-b"), "{answer}");
        assert_eq!(sent.recv().await.unwrap().thread_id, envelope.thread_id);
    }
    let seen = seen.locked();
    assert_eq!(seen.len(), 3, "new, new, then load with the cap of one");
    let token = |servers: &[McpServer]| match servers {
        [McpServer::Http(http)] => http.headers[0].value.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(token(&seen[0]), token(&seen[2]), "a thread keeps its token across a load");
    assert_ne!(token(&seen[0]), token(&seen[1]));
}

#[tokio::test]
async fn harnesses_without_http_get_the_stdio_bridge() {
    let (mcp, _sent) = start(&home("stdio")).await;
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let pool = fake_pool(&mcp, false, seen.clone());
    let current = from_human();
    assert_eq!(pool.prompt(&current.thread_id, "x").await.unwrap(), "no tools");
    let seen = seen.locked();
    let [McpServer::Stdio(stdio)] = &seen[0][..] else { panic!("{:?}", seen[0]) };
    assert_eq!(stdio.command, std::env::current_exe().unwrap());
}

/// An attachments' store in memory, by handle.
type MemoryFiles = Arc<StdMutex<HashMap<AttachmentHandle, (Attachment, Vec<u8>)>>>;

fn memory_files(stored: MemoryFiles) -> FileAccess {
    let reader = stored.clone();
    FileAccess {
        put: Arc::new(move |handle, attachment, bytes| {
            stored.locked().insert(handle, (attachment, bytes.to_vec()));
            Box::pin(async { Ok(()) })
        }),
        get: Arc::new(move |handle, _| {
            let found = reader.locked().get(&handle).cloned();
            Box::pin(async move { found.ok_or_else(|| anyhow::anyhow!("attachment {handle} is not stored")) })
        }),
    }
}

/// A client for the sessions of `thread`, working in `dir`.
async fn client_in(mcp: &Mcp, thread: &ThreadId, dir: &Path) -> Client {
    let [McpServer::Http(server)] = &mcp.servers()(thread, dir, true)[..] else { panic!("one HTTP server") };
    let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
    http_client(&server.url, token).await
}

#[tokio::test]
async fn attach_sends_files_from_the_working_directory_only() {
    let home = home("attach");
    let (mcp, mut sent) = start(&home).await;
    let work = home.join("work");
    std::fs::create_dir_all(work.join("sub")).unwrap();
    std::fs::write(work.join("notes.txt"), "hello").unwrap();
    std::fs::write(work.join("sub/shot.png"), [0x89, b'P', b'N', b'G']).unwrap();
    std::fs::write(home.join("secret.txt"), "keep out").unwrap();
    std::fs::File::create(work.join("big.bin")).unwrap().set_len(ants_proto::MAX_ATTACHMENT_SIZE + 1).unwrap();
    let current = from_human();
    let _turn = mcp.begin(&current);
    let client = client_in(&mcp, &current.thread_id, &work).await;
    let send = |attach: serde_json::Value| serde_json::json!({"to": "dev-b", "text": "see", "attach": attach});

    let (ok, text) = call(&client, "send_message", send(serde_json::json!(["notes.txt"]))).await;
    assert!(!ok && text.contains("attachments are not available here; nothing sent"), "{text}");
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.clone());

    let (ok, text) = call(&client, "send_message", send(serde_json::json!(["notes.txt", "./sub/shot.png"]))).await;
    assert!(ok, "{text}");
    let out = sent.try_recv().unwrap();
    let got: Vec<_> = out.attachments.iter().map(|a| (a.name.as_str(), a.mime.as_str(), a.size)).collect();
    assert_eq!(got, [("notes.txt", "text/plain", 5), ("shot.png", "image/png", 4)]);
    let held = stored.locked().get(&out.attachment_handle(out.attachments[0].id)).cloned().unwrap();
    assert_eq!(held, (out.attachments[0].clone(), b"hello".to_vec()), "stored under the message's handle");

    #[cfg(unix)]
    std::os::unix::fs::symlink("../secret.txt", work.join("link")).unwrap();
    let nine = serde_json::json!(vec!["notes.txt"; 9]);
    for (attach, why) in [
        (serde_json::json!(["../secret.txt"]), "outside the working directory"),
        (serde_json::json!([home.join("secret.txt")]), "outside the working directory"),
        #[cfg(unix)]
        (serde_json::json!(["link"]), "outside the working directory"),
        (serde_json::json!(["sub"]), "no regular file"),
        (serde_json::json!(["notes.txt", "gone.txt"]), "cannot attach \"gone.txt\""),
        (serde_json::json!(["big.bin"]), "over the 8 MiB"),
        (nine, "at most 8 files"),
    ] {
        let (ok, text) = call(&client, "send_message", send(attach)).await;
        assert!(!ok && text.contains(why) && text.ends_with("nothing sent"), "{text}");
    }
    assert!(sent.try_recv().is_err(), "nothing sent");
    assert_eq!(stored.locked().len(), 2, "nothing stored");

    // Notes posts and channel posts carry them too, each under its own handle.
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "log", "attach": ["notes.txt"]})).await;
    assert!(ok, "{text}");
    let (ok, text) = call(&client, "read_notes", serde_json::json!({})).await;
    let handle = format!("dev-a.notes.{}.", current.thread_id);
    assert!(ok && text.contains("log\n📎 notes.txt (text/plain, 5 B) handle ") && text.contains(&handle), "{text}");
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(user("dev-a"));
    let posted = Arc::new(StdMutex::new(Vec::new()));
    mcp.with_channels(channels(vec![ops], posted.clone()));
    let post = serde_json::json!({"channel": "ops", "text": "shot", "attach": ["sub/shot.png"]});
    let (ok, text) = call(&client, "post_channel", post).await;
    assert!(ok, "{text}");
    let post = posted.locked().pop().unwrap();
    let held = stored.locked().get(&post.attachment_handle(post.attachments[0].id)).cloned().unwrap();
    assert_eq!(held.1, [0x89, b'P', b'N', b'G']);
}

#[tokio::test]
async fn attach_to_reply_names_the_files_the_turns_answer_carries() {
    let home = home("attach-to-reply");
    let (mcp, _sent) = start(&home).await;
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.clone());
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("a.txt"), "aa").unwrap();
    std::fs::write(work.join("b.txt"), "b").unwrap();
    std::fs::write(home.join("secret.txt"), "keep out").unwrap();
    let current = from_human();
    let thread = &current.thread_id;
    let client = client_in(&mcp, thread, &work).await;
    let attach =
        async |files: serde_json::Value| call(&client, "attach_to_reply", serde_json::json!({"attach": files})).await;
    let reply = current.reply(user("dev-a"), "done", Role::Human).unwrap();
    let handle_of = |id| reply.attachment_handle(id);

    let (ok, text) = attach(serde_json::json!(["a.txt"])).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let turn = mcp.begin(&current);
    let (ok, text) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok && text.contains("a.txt (text/plain, 2 B)"), "{text}");
    let (ok, text) = attach(serde_json::json!(["../secret.txt"])).await;
    assert!(!ok && text.contains("outside the working directory") && text.ends_with("nothing attached"), "{text}");
    assert!(stored.locked().is_empty(), "stored only as the answer is sent");
    let files = mcp.reply_files(thread, handle_of).await.unwrap();
    let got: Vec<_> = files.iter().map(|a| (a.name.as_str(), a.size)).collect();
    assert_eq!(got, [("a.txt", 2)], "the failed call left the earlier files");
    let held = stored.locked().get(&reply.attachment_handle(files[0].id)).cloned().unwrap();
    assert_eq!(held, (files[0].clone(), b"aa".to_vec()), "stored under the answer's handle");
    assert_eq!(mcp.reply_files(thread, handle_of).await.unwrap(), [], "taken by the answer");

    // A later call replaces them, an empty one clears them.
    let (ok, _) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok);
    let (ok, _) = attach(serde_json::json!(["b.txt"])).await;
    assert!(ok);
    let files = mcp.reply_files(thread, handle_of).await.unwrap();
    assert_eq!(files.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(), ["b.txt"]);
    let (ok, _) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok);
    let (ok, text) = attach(serde_json::json!([])).await;
    assert!(ok && text.contains("carries no files"), "{text}");
    assert_eq!(mcp.reply_files(thread, handle_of).await.unwrap(), []);

    // The files are read again as the answer is sent.
    let (ok, _) = attach(serde_json::json!(["b.txt"])).await;
    assert!(ok);
    std::fs::remove_file(work.join("b.txt")).unwrap();
    let why = mcp.reply_files(thread, handle_of).await.unwrap_err();
    assert!(why.contains("cannot attach \"b.txt\""), "{why}");

    // And forgotten with the turn.
    let (ok, _) = attach(serde_json::json!(["a.txt"])).await;
    assert!(ok);
    drop(turn);
    let _turn = mcp.begin(&current);
    assert_eq!(mcp.reply_files(thread, handle_of).await.unwrap(), []);
}

#[tokio::test]
async fn attach_redacts_secrets_in_text_files_only() {
    let home = home("attach-redact");
    let (mcp, mut sent) = start(&home).await;
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.clone());
    mcp.with_redactor(Arc::new(crate::redact::Redactor::default()));
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let token = format!("ghp_{}", "a1B2".repeat(9));
    std::fs::write(work.join(".env"), format!("GITHUB_TOKEN={token}\nLOG=info\n")).unwrap();
    let mut binary = vec![0xff, 0xfe];
    binary.extend_from_slice(token.as_bytes());
    std::fs::write(work.join("blob.bin"), &binary).unwrap();
    std::fs::write(work.join("plain.txt"), "nothing secret").unwrap();
    let current = from_human();
    let _turn = mcp.begin(&current);
    let client = client_in(&mcp, &current.thread_id, &work).await;

    let args = serde_json::json!({"to": "dev-b", "text": "see", "attach": [".env", "blob.bin", "plain.txt"]});
    let (ok, text) = call(&client, "send_message", args).await;
    assert!(ok, "{text}");
    let out = sent.try_recv().unwrap();
    let held: Vec<_> = out.attachments.iter().map(|a| stored.locked()[&out.attachment_handle(a.id)].clone()).collect();
    let env = b"GITHUB_TOKEN=[redacted]\nLOG=info\n";
    assert_eq!(held[0].1, env);
    let expected = crate::files::reference(".env", "application/octet-stream", env).unwrap();
    let reference = |a: &Attachment| (a.size, a.sha256.clone());
    assert_eq!(reference(&held[0].0), reference(&expected), "the reference is of the bytes stored");
    assert_eq!(held[1].1, binary, "not text: left alone");
    assert_eq!(held[2].1, b"nothing secret");
}

/// `read_attachment` reads an attachment of the message being handled as one stored when the hub stored that message,
/// so chunks still on their way from a leaf's outbox are waited for; any other as one stored when its id says.
#[tokio::test]
async fn read_attachment_waits_for_the_message_being_handled_as_its_stored_time_says() {
    let home = home("read-stored");
    let (mcp, _sent) = start(&home).await;
    let asked = Arc::new(StdMutex::new(Vec::new()));
    let seen = asked.clone();
    let files = FileAccess {
        put: Arc::new(|_, _, _| Box::pin(async { Ok(()) })),
        get: Arc::new(move |handle, stored| {
            seen.locked().push((handle.clone(), stored));
            Box::pin(async move { Err(anyhow::anyhow!("attachment {handle} is not stored")) })
        }),
    };
    mcp.with_files(files, home.clone());
    let current = Envelope::new(user("dev-b"), user("dev-a"), "files");
    let carried = current.attachment_handle(Uuid::now_v7());
    let other = current.attachment_handle(Uuid::now_v7());
    let stored = chrono::Utc::now();
    let turn = mcp.begin(&current).carrying(vec![carried.clone()], Some(stored));
    let client = client_in(&mcp, &current.thread_id, &home).await;
    for handle in [&carried, &other] {
        let (ok, _) = call(&client, "read_attachment", serde_json::json!({"handle": handle.to_string()})).await;
        assert!(!ok);
    }
    drop(turn);
    call(&client, "read_attachment", serde_json::json!({"handle": carried.to_string()})).await;
    assert_eq!(*asked.locked(), [(carried.clone(), Some(stored)), (other, None), (carried, None)]);
}

#[tokio::test]
async fn read_attachment_returns_images_and_text_and_saves_other_files() {
    let home = home("read-attachment");
    let (mcp, _sent) = start(&home).await;
    let stored = MemoryFiles::default();
    mcp.with_files(memory_files(stored.clone()), home.clone());
    let tree = home.join("tree");
    std::fs::create_dir_all(&tree).unwrap();
    let current = Envelope::new(user("dev-b"), user("dev-a"), "files");
    let _turn = mcp.begin(&current);
    let client = client_in(&mcp, &current.thread_id, &tree).await;
    let put = |envelope: &Envelope, name: &str, bytes: &[u8]| {
        let attachment = crate::files::reference(name, ants_proto::guess_mime(name), bytes).unwrap();
        let handle = envelope.attachment_handle(attachment.id);
        stored.locked().insert(handle.clone(), (attachment, bytes.to_vec()));
        handle.to_string()
    };
    let read = async |handle: &str| {
        let args = serde_json::json!({"handle": handle});
        let serde_json::Value::Object(args) = args else { unreachable!() };
        client.call_tool(CallToolRequestParams::new("read_attachment").with_arguments(args)).await.unwrap()
    };

    let image = read(&format!("`{}`", put(&current, "shot.png", b"png bytes"))).await;
    let [text, picture] = &image.content[..] else { panic!("{image:?}") };
    assert!(text.as_text().unwrap().text.contains("shot.png (image/png, 9 B)"));
    let picture = picture.as_image().unwrap();
    assert_eq!((picture.data.as_str(), picture.mime_type.as_str()), ("cG5nIGJ5dGVz", "image/png"));

    let (ok, text) =
        call(&client, "read_attachment", serde_json::json!({"handle": put(&current, "a.md", b"# hi")})).await;
    assert!(ok && text.ends_with("a.md (text/markdown, 4 B)\n# hi"), "{text}");

    // Other files are saved into the thread's directory, the same bytes once, different ones beside them.
    let handle = put(&current, "data.bin", &[0, 1, 2]);
    for _ in 0..2 {
        let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": handle})).await;
        assert!(ok && text.ends_with(&format!("saved as {}", tree.canonicalize().unwrap().join("data.bin").display())));
    }
    let other = put(&current, "data.bin", &[3]);
    let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": other})).await;
    assert!(ok && text.ends_with("data-2.bin"), "{text}");
    assert_eq!(std::fs::read(tree.join("data.bin")).unwrap(), [0, 1, 2]);
    assert_eq!(std::fs::read(tree.join("data-2.bin")).unwrap(), [3]);
    assert!(!home.join("data.bin").exists(), "not in the workdir");

    // Only a message the ant reads: one between two other ants is not.
    let theirs = Envelope::new(user("dev-b"), user("dev-c"), "private");
    let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": put(&theirs, "x.txt", b"x")})).await;
    assert!(!ok && text.contains("is on a message dev-a does not read"), "{text}");
    let unknown = current.attachment_handle(Uuid::now_v7()).to_string();
    let (ok, text) = call(&client, "read_attachment", serde_json::json!({"handle": unknown})).await;
    assert!(!ok && text.contains("is not stored"), "{text}");
    let (ok, _) = call(&client, "read_attachment", serde_json::json!({"handle": "nonsense"})).await;
    assert!(!ok);

    // A message with attachments lists them with their handles.
    let mut late = Envelope::on_thread(current.thread_id.clone(), user("dev-b"), user("dev-a"), "here");
    late.attachments = vec![crate::files::reference("log.txt", "text/plain", b"ok").unwrap()];
    let listed = super::shown(&late);
    let handle = late.attachment_handle(late.attachments[0].id);
    assert_eq!(listed, format!("here\n📎 log.txt (text/plain, 2 B) handle {handle}"));
}
