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

mod assistant;
mod attachments;
mod channels;
mod escalation;
mod messages;
mod replies;
mod routing;
mod serving;

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
    start_with_colony(home, Vec::new()).await
}

/// Like [`start`], with `colony` as the colony.
async fn start_with_colony(home: &Path, colony: Vec<ColonyEntry>) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    start_with(home, colony, Arc::default(), Registry::default()).await
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

/// Like [`start_with_colony`], with `notes` as the thread notes and `humans` as the humans.
async fn start_with(
    home: &Path,
    colony: Vec<ColonyEntry>,
    notes: Arc<MemoryNotes>,
    humans: Registry,
) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let outbox: Outbox = Arc::new(move |envelope| {
        let _ = tx.send(envelope);
        Box::pin(async { Ok(()) })
    });
    let colony: Colony = Arc::new(move || {
        let colony = colony.clone();
        Box::pin(async move { Ok(colony) })
    });
    (Mcp::start(user("dev-a"), home, outbox, colony, notes, humans, None).await.unwrap(), rx)
}

fn listed(name: &str, capabilities: &[&str]) -> ColonyEntry {
    let now = chrono::Utc::now();
    ColonyEntry {
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
        model: None,
        paused: None,
        drift: Vec::new(),
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
