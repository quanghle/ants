//! The conductor's daemon running trails ([`ants_daemon::trail`]) against a throwaway nats-server, with real
//! `ants subprocess` workers: the ants a run's steps go to are played by the test, which reads the steps the conductor
//! sends and hands it their replies, as its inbox would. Thread notes are kept in memory.

#![expect(clippy::disallowed_methods, reason = "tests read and hear as they please")]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::v1::schema::McpServer;
use ants_daemon::admin::restart::Restarts;
use ants_daemon::harness::McpSession;
use ants_daemon::hub;
use ants_daemon::mcp::Mcp;
use ants_daemon::people::humans::Registry;
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::stream::notes::{JetStreamNotes, Notes, NotesStore};
use ants_daemon::trail::{Conducting, StartRun, Trails};
use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    AdminOp, AdminOrder, AdminOutcome, AssistantGrant, ColonyEntry, DeviceId, Envelope, MACHINES_BUCKET, Machine,
    NotesPost, NoticeKind, OrderState, Pick, Role, RunId, RunLimit, RunState, StatusEvent, StatusUpdate, StepStage,
    TRAIL_RUNS_BUCKET, ThreadId, TrailAction, TrailLimits, TrailOutcome, TrailReply, TrailRequest, TrailRun,
    TrailRunIndex, TrailSession, TrailSource, TrailStart, TrailStepThread, UserId, WaitingStep, subject, trail_run_key,
    trail_runs_index_key, trail_session_key, trail_thread_step_key,
};
use ants_test_support::user as ant;
use ants_test_support::{NatsServer, eventually, eventually_within, scratch_dir};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use rmcp::model::{CallToolRequestParams, ClientConfig};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{RoleClient, ServiceExt as _};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[path = "trail_daemon/asked_first.rs"]
mod asked_first;
#[path = "trail_daemon/children.rs"]
mod children;
#[path = "trail_daemon/ends.rs"]
mod ends;
#[path = "trail_daemon/goto.rs"]
mod goto;
#[path = "trail_daemon/guide.rs"]
mod guide;
#[path = "trail_guide/host.rs"]
mod guide_host;
#[path = "trail_daemon/lifecycle.rs"]
mod lifecycle;
#[path = "trail_daemon/limits.rs"]
mod limits;
#[path = "trail_daemon/mcp.rs"]
mod mcp;
#[path = "trail_daemon/nested.rs"]
mod nested;
#[path = "trail_daemon/ratings.rs"]
mod ratings;
#[path = "trail_daemon/restore.rs"]
mod restore;
#[path = "trail_daemon/save_tools.rs"]
mod save_tools;
#[path = "trail_daemon/stored.rs"]
mod stored;
#[path = "trail_daemon/tip_tools.rs"]
mod tip_tools;
#[path = "trail_daemon/tips.rs"]
mod tips;
#[path = "trail_daemon/tips_carried.rs"]
mod tips_carried;
#[path = "trail_daemon/unanswered.rs"]
mod unanswered;
#[path = "trail_daemon/work_branch.rs"]
mod work_branch;

const META: &str = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": [\"one\"]}\n";

/// A conductor serving its runs, what it sends on `sent`: its steps, and the other messages it sends, which
/// [`Conductor::step`] puts aside in `told`, and in `invited` those inviting itself to rate a run that ended.
struct Conductor {
    trails: Trails,
    js: async_nats::jetstream::Context,
    client: async_nats::Client,
    sent: mpsc::UnboundedReceiver<Envelope>,
    told: Vec<Envelope>,
    invited: Vec<Envelope>,
    notes: Arc<MemoryNotes>,
    /// Set, the next message the conductor sends fails, and is not sent.
    failing: Arc<std::sync::atomic::AtomicBool>,
    serving: JoinHandle<()>,
    /// Whether the conductor is paused.
    pause: ants_daemon::pause::Pause,
}

/// Thread notes in memory.
#[derive(Default)]
struct MemoryNotes(Mutex<Vec<NotesPost>>);

impl NotesStore for MemoryNotes {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> BoxFuture<'a, anyhow::Result<Notes>> {
        let all = self.0.lock().unwrap();
        let posts: Vec<NotesPost> = all.iter().filter(|post| post.thread_id == *thread).cloned().collect();
        let notes = Notes { posts, last_seq: all.len() as u64 };
        Box::pin(async move { Ok(notes) })
    }

    fn post<'a>(&'a self, post: &'a NotesPost, last_seq: u64) -> BoxFuture<'a, anyhow::Result<bool>> {
        let mut all = self.0.lock().unwrap();
        let posted = all.len() as u64 == last_seq;
        if posted {
            all.push(post.clone());
        }
        Box::pin(async move { Ok(posted) })
    }
}

impl MemoryNotes {
    fn on(&self, thread: &ThreadId) -> Vec<NotesPost> {
        self.0.lock().unwrap().iter().filter(|post| post.thread_id == *thread).cloned().collect()
    }
}

impl Drop for Conductor {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

/// The buckets `ants init` creates, as the human.
async fn init(server: &NatsServer) {
    let (_, js) =
        ants_daemon::bus::connect(&server.url, None, None, &ants_test_support::human(), "init").await.unwrap();
    for config in
        [hub::trails_config(), hub::trail_runs_config(), hub::trail_tips_config(), hub::trail_ratings_config()]
    {
        js.create_stream(config).await.unwrap();
    }
}

/// Save `script` as the colony's trail `name`, as the human.
async fn save_for_colony(server: &NatsServer, name: &str, script: &str) {
    let (_, js) =
        ants_daemon::bus::connect(&server.url, None, None, &ants_test_support::human(), "save").await.unwrap();
    let trail = ants_proto::SavedTrail {
        name: name.parse().unwrap(),
        description: "a test".to_owned(),
        phases: Vec::new(),
        waypoints: Vec::new(),
        script: script.to_owned(),
        saved_by: ants_proto::Saver::Human { user: ants_test_support::human() },
        saved_from: None,
        saved_at: chrono::Utc::now(),
    };
    let kv = DirectKv::new(&js, None, ants_proto::TRAILS_BUCKET).await.unwrap();
    kv.put(name, serde_json::to_vec(&trail).unwrap()).await.unwrap();
}

/// The conductor `lead`'s daemon, its runs continued, with `worker` as the `ants` binary and `colony` as the colony.
async fn conductor(server: &NatsServer, worker: &Path, colony: Vec<ColonyEntry>) -> Conductor {
    conductor_full(server, worker, colony, Registry::new(ants_test_support::human()), None).await
}

/// [`conductor`], knowing the humans and their assistants' grants from `humans`.
async fn conductor_with(server: &NatsServer, worker: &Path, colony: Vec<ColonyEntry>, humans: Registry) -> Conductor {
    conductor_full(server, worker, colony, humans, None).await
}

/// Like [`conductor`], with the machine's saved trails in `trails_dir`.
async fn conductor_saving(
    server: &NatsServer,
    worker: &Path,
    colony: Vec<ColonyEntry>,
    trails_dir: Option<PathBuf>,
) -> Conductor {
    conductor_full(server, worker, colony, Registry::new(ants_test_support::human()), trails_dir).await
}

/// [`conductor`] with its humans and saved trails directory.
async fn conductor_full(
    server: &NatsServer,
    worker: &Path,
    colony: Vec<ColonyEntry>,
    humans: Registry,
    trails_dir: Option<PathBuf>,
) -> Conductor {
    conductor_refusing(server, worker, colony, humans, trails_dir, None).await
}

/// [`conductor_full`], failing to send any message to `refused`.
async fn conductor_refusing(
    server: &NatsServer,
    worker: &Path,
    colony: Vec<ColonyEntry>,
    humans: Registry,
    trails_dir: Option<PathBuf>,
    refused: Option<UserId>,
) -> Conductor {
    let lead = ant("lead");
    let pause = ants_daemon::pause::Pause::new(false);
    let (client, js) = ants_daemon::bus::connect(&server.url, None, None, &lead, "conductor").await.unwrap();
    let attempts = ants_daemon::bus::ensure_attempts(&js, None, &lead).await.unwrap();
    let (tx, sent) = mpsc::unbounded_channel();
    let failing = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fails = failing.clone();
    let outbox: ants_daemon::mcp::Outbox = Arc::new(move |envelope: Envelope| {
        let tx = tx.clone();
        let failed = fails.swap(false, std::sync::atomic::Ordering::SeqCst);
        let refused = refused.as_ref() == Some(&envelope.to);
        Box::pin(async move {
            if failed {
                anyhow::bail!("the bus is away");
            }
            if refused {
                anyhow::bail!("{} is unreachable", envelope.to);
            }
            tx.send(envelope).unwrap();
            Ok(())
        })
    });
    let notes = Arc::new(MemoryNotes::default());
    let colony = Arc::new(colony);
    let colony: ants_daemon::mcp::Colony = Arc::new(move || {
        let colony = colony.clone();
        Box::pin(async move { Ok(colony.to_vec()) })
    });
    let conducting = Conducting {
        js: js.clone(),
        js_domain: None,
        client: client.clone(),
        ant: lead,
        worker: worker.to_owned(),
        trails_dir,
        humans,
        colony,
        outbox,
        attempts,
        workers: ants_daemon::trail::DEFAULT_WORKERS,
        notes: notes.clone(),
        saves: ants_daemon::trail::SaveThrough::Bucket,
        pause: pause.clone(),
    };
    let trails = Trails::new(conducting).await.unwrap();
    trails.restore().await;
    let serving = tokio::spawn({
        let trails = trails.clone();
        async move { trails.serve().await }
    });
    Conductor { trails, js, client, sent, told: Vec::new(), invited: Vec::new(), notes, failing, serving, pause }
}

fn ants_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_ants"))
}

fn colony_entry(name: &str, capabilities: &[&str]) -> ColonyEntry {
    let now = chrono::Utc::now();
    serde_json::from_value(json!({
        "ant": name, "machine": "m", "instance": "i", "harness": "stub", "capabilities": capabilities,
        "started_at": now, "heartbeat_at": now, "activity": "idle", "handling": 0, "harnesses": 1,
        "version": "0", "perms": 1,
    }))
    .unwrap()
}

impl Conductor {
    async fn start(&self, script: &str, limits: TrailLimits) -> RunId {
        self.start_on(script, limits, ThreadId::generate()).await
    }

    /// Start a run of `script` reporting on `thread`.
    async fn start_on(&self, script: &str, limits: TrailLimits, thread: ThreadId) -> RunId {
        let human = ants_test_support::human();
        let start = StartRun {
            trail: TrailSource::Script(format!("{META}{script}")),
            args: Value::Null,
            owner: Some(human.clone()),
            origin: Origin { by: Starter::Human { user: human }, channel: None },
            thread,
            limits,
            spends_from: None,
        };
        self.trails.start(start).await.unwrap()
    }

    /// The next step the conductor sends.
    async fn step(&mut self) -> Envelope {
        loop {
            let sent = tokio::time::timeout(Duration::from_secs(30), self.sent.recv()).await.expect("a step").unwrap();
            if sent.trail_step.is_some() {
                return sent;
            }
            self.aside(sent);
        }
    }

    /// Put `sent`, no step, aside in `invited` if it invites the conductor to rate a run, else in `told`.
    fn aside(&mut self, sent: Envelope) {
        if sent.from == ant("lead") && sent.to == ant("lead") && sent.text.contains("rate_trail(run: ") {
            self.invited.push(sent);
        } else {
            self.told.push(sent);
        }
    }

    /// The next message inviting the conductor to rate a run.
    async fn invitation(&mut self) -> Envelope {
        loop {
            if !self.invited.is_empty() {
                return self.invited.remove(0);
            }
            let sent =
                tokio::time::timeout(Duration::from_secs(30), self.sent.recv()).await.expect("a message").unwrap();
            assert!(sent.trail_step.is_none(), "a step: {sent:?}");
            self.aside(sent);
        }
    }

    /// Whether the conductor sends no step for `wait`.
    async fn sends_no_step(&mut self, wait: Duration) -> bool {
        let until = tokio::time::Instant::now() + wait;
        while let Ok(sent) = tokio::time::timeout_at(until, self.sent.recv()).await {
            let sent = sent.unwrap();
            if sent.trail_step.is_some() {
                return false;
            }
            self.aside(sent);
        }
        true
    }

    /// The next message the conductor sends that is no step.
    async fn told(&mut self) -> Envelope {
        loop {
            if !self.told.is_empty() {
                return self.told.remove(0);
            }
            let sent =
                tokio::time::timeout(Duration::from_secs(30), self.sent.recv()).await.expect("a message").unwrap();
            assert!(sent.trail_step.is_none(), "a step: {sent:?}");
            self.aside(sent);
        }
    }

    /// The texts of the notes of `thread`, once `count` are there.
    async fn notes(&self, thread: &ThreadId, count: usize) -> Vec<String> {
        eventually("the notes are posted", async || self.notes.on(thread).len() >= count).await;
        self.notes.on(thread).into_iter().map(|post| post.text).collect()
    }

    /// `step`'s recipient replying `text`: true when the conductor took the reply.
    async fn reply(&self, step: &Envelope, text: &str) -> bool {
        let reply = step.reply(step.to.clone(), text, Role::Ant).unwrap();
        self.trails.take(&reply).await.unwrap()
    }

    async fn header(&self, run: &RunId) -> TrailRun {
        let runs = DirectKv::new(&self.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
        serde_json::from_slice(&runs.get(trail_run_key(&ant("lead"), run)).await.unwrap().unwrap()).unwrap()
    }

    async fn ended(&self, run: &RunId) -> RunState {
        let mut state = RunState::Running;
        eventually("the run ends", async || {
            state = self.header(run).await.state;
            state.finished()
        })
        .await;
        state
    }
}

/// The first answer on `answers` to the request `id` arriving within `wait`.
async fn answers_to(
    answers: &mut async_nats::Subscriber,
    id: ants_proto::Uuid,
    wait: Duration,
) -> Option<TrailOutcome> {
    let until = tokio::time::Instant::now() + wait;
    while let Ok(answer) = tokio::time::timeout_at(until, answers.next()).await {
        let answer: TrailReply = serde_json::from_slice(&answer.unwrap().payload).unwrap();
        if answer.request_id == id {
            return Some(answer.outcome);
        }
    }
    None
}

/// The next status event on `status` that tells a step.
async fn step_status(status: &mut async_nats::Subscriber) -> (StatusEvent, StepStage) {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), status.next()).await.expect("a status").unwrap();
        let event: StatusEvent = serde_json::from_slice(&event.payload).unwrap();
        if let StatusUpdate::Step { stage, .. } = event.update {
            return (event, stage);
        }
    }
}

impl Conductor {
    /// The stored session of `run`, ants_trail's [`ants_trail::Session`], as JSON.
    async fn session(&self, run: &RunId) -> (TrailSession, Value) {
        let runs = DirectKv::new(&self.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
        let stored = runs.get(trail_session_key(&ant("lead"), run)).await.unwrap().unwrap();
        let stored: TrailSession = serde_json::from_slice(&stored).unwrap();
        let state = serde_json::from_str(&stored.session).unwrap();
        (stored, state)
    }

    /// Store `state` as the session of `run`, as a daemon stopped since would find it.
    async fn put_session(&self, run: &RunId, mut stored: TrailSession, state: &Value) {
        stored.session = state.to_string();
        let runs = DirectKv::new(&self.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
        runs.put(trail_session_key(&ant("lead"), run), serde_json::to_vec(&stored).unwrap()).await.unwrap();
    }
}

/// Starts `script` with `lead`, answers its first step "A", and stops the daemon once the second, to ops, is stored
/// sent: the second step is returned.
async fn second_step_stored(lead: &mut Conductor, script: &str) -> (RunId, Envelope) {
    let run = lead.start(script, TrailLimits::default()).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, "A").await);
    let b = lead.step().await;
    assert_eq!(b.to, ant("ops"));
    eventually("the second step is stored sent", async || {
        lead.session(&run).await.0.pending.iter().any(|call| call.message == b.id && call.sent)
    })
    .await;
    lead.serving.abort();
    (run, b)
}

const TWO_STEPS: &str = "a = await agent(\"a\", ant=\"dev\")\nb = await agent(\"b\", ant=\"ops\")\na + b\n";

/// An MCP client of `lead`'s tools as a session of `thread`.
async fn mcp_client(mcp: &Mcp, thread: &ThreadId) -> RunningService<RoleClient, ClientConfig> {
    let session = McpSession { thread, dir: Path::new("/"), harness: "test", model: None, http: true };
    let [McpServer::Http(server)] = &mcp.servers()(&session)[..] else { panic!("one HTTP server") };
    let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
    let config = StreamableHttpClientTransportConfig::with_uri(server.url.as_str()).auth_header(token);
    ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap()
}

/// Call the tool `name` with `args`: whether it succeeded, and its text.
async fn call(client: &RunningService<RoleClient, ClientConfig>, name: &'static str, args: Value) -> (bool, String) {
    let Value::Object(args) = args else { panic!("arguments are an object") };
    let result = client.call_tool(CallToolRequestParams::new(name).with_arguments(args)).await.unwrap();
    let text = result.content.iter().filter_map(|c| c.as_text()).map(|t| t.text.as_str()).collect();
    (result.is_error != Some(true), text)
}

/// Call `run_trail` with `args`: whether it succeeded, and its text.
async fn run_trail(client: &RunningService<RoleClient, ClientConfig>, args: Value) -> (bool, String) {
    call(client, "run_trail", args).await
}

/// The MCP server of `lead`, conducting its runs with `trails`.
async fn lead_mcp(dir: &Path, js: &async_nats::jetstream::Context, trails: &Trails) -> Mcp {
    let outbox: ants_daemon::mcp::Outbox = Arc::new(|_| Box::pin(async { Ok(()) }));
    let colony: ants_daemon::mcp::Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let notes = Arc::new(JetStreamNotes::new(js.clone(), ant("lead")));
    let humans = Registry::new(ants_test_support::human());
    let mcp = Mcp::start(ant("lead"), &dir.join("home"), outbox, colony, notes, humans, None).await.unwrap();
    mcp.with_trails(trails.clone());
    mcp
}

/// The id of the run `run_trail` says it started.
fn started(text: &str) -> RunId {
    text.strip_prefix("Started run ").unwrap().split_once(' ').unwrap().0.parse().unwrap()
}
