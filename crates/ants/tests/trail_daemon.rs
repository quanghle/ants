//! The conductor's daemon running trails ([`ants_daemon::trail`]) against a throwaway nats-server, with real
//! `ants subprocess` workers: the ants a run's steps go to are played by the test, which reads the steps the conductor
//! sends and hands it their replies, as its inbox would. Thread notes are kept in memory.

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
    TrailSession, TrailSource, TrailStart, TrailStepThread, UserId, WaitingStep, subject, trail_run_key,
    trail_session_key, trail_thread_step_key,
};
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

#[path = "trail_daemon/children.rs"]
mod children;
#[path = "trail_daemon/goto.rs"]
mod goto;
#[path = "trail_daemon/ratings.rs"]
mod ratings;
#[path = "trail_daemon/save_tools.rs"]
mod save_tools;
#[path = "trail_daemon/tip_tools.rs"]
mod tip_tools;
#[path = "trail_daemon/tips.rs"]
mod tips;
#[path = "trail_daemon/tips_carried.rs"]
mod tips_carried;

const META: &str = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": [\"one\"]}\n";

fn ant(name: &str) -> UserId {
    name.parse().unwrap()
}

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
    serving: JoinHandle<()>,
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
    let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &UserId::human(), "init").await.unwrap();
    for config in
        [hub::trails_config(), hub::trail_runs_config(), hub::trail_tips_config(), hub::trail_ratings_config()]
    {
        js.create_key_value(config).await.unwrap();
    }
}

/// The conductor `lead`'s daemon, its runs continued, with `worker` as the `ants` binary and `colony` as the colony.
async fn conductor(server: &NatsServer, worker: &Path, colony: Vec<ColonyEntry>) -> Conductor {
    conductor_full(server, worker, colony, Registry::new(UserId::human()), None).await
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
    conductor_full(server, worker, colony, Registry::new(UserId::human()), trails_dir).await
}

/// [`conductor`] with its humans and saved trails directory.
async fn conductor_full(
    server: &NatsServer,
    worker: &Path,
    colony: Vec<ColonyEntry>,
    humans: Registry,
    trails_dir: Option<PathBuf>,
) -> Conductor {
    let lead = ant("lead");
    let (client, js) = ants_daemon::bus::connect(&server.url, None, None, &lead, "conductor").await.unwrap();
    let attempts = ants_daemon::bus::ensure_attempts(&js, None, &lead).await.unwrap();
    let (tx, sent) = mpsc::unbounded_channel();
    let outbox: ants_daemon::mcp::Outbox = Arc::new(move |envelope| {
        let tx = tx.clone();
        Box::pin(async move {
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
    };
    let trails = Trails::new(conducting).await.unwrap();
    trails.restore().await;
    let serving = tokio::spawn({
        let trails = trails.clone();
        async move { trails.serve().await }
    });
    Conductor { trails, js, client, sent, told: Vec::new(), invited: Vec::new(), notes, serving }
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
        let human = UserId::human();
        let start = StartRun {
            trail: TrailSource::Script(format!("{META}{script}")),
            args: Value::Null,
            owner: Some(human.clone()),
            origin: Origin { by: Starter::Human { user: human }, channel: None },
            thread,
            limits,
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

#[tokio::test]
async fn a_human_starts_a_run_fanning_out_to_two_ants_whose_replies_finish_it() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), vec![colony_entry("reviewer", &["review"])]).await;
    let mut answers = lead.client.subscribe(subject::trailed(&ant("lead"))).await.unwrap();
    let mut ask = async |action: TrailAction, wait: Duration| {
        let request = TrailRequest::new(ant("lead"), UserId::human(), action);
        lead.client.publish(request.subject(), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
        let answer = tokio::time::timeout(wait, answers.next()).await.ok()?.unwrap();
        let answer: TrailReply = serde_json::from_slice(&answer.payload).unwrap();
        assert_eq!(answer.request_id, request.id);
        Some(answer.outcome)
    };
    let missing = RunId::generate();
    let refused = loop {
        let stop = TrailAction::Stop { run: missing.clone() };
        if let Some(outcome) = ask(stop, Duration::from_millis(200)).await {
            break outcome;
        }
    };
    let TrailOutcome::Refused { error } = refused else { panic!("not refused: {refused:?}") };
    assert!(error.contains("conducts no run"), "{error}");
    let script = r#"
await parallel(agent("build it", ant="dev", tips="build"), agent("review it", capability="review"))
"#;
    let start = TrailAction::Start {
        trail: TrailSource::Script(format!("{META}{script}")),
        args: Value::Null,
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
    };
    let started = ask(start, Duration::from_secs(10)).await.expect("an answer");
    let TrailOutcome::Started { run } = started else { panic!("not started: {started:?}") };

    let (first, second) = (lead.step().await, lead.step().await);
    let mut steps = [first, second];
    steps.sort_by_key(|step| step.to.to_string());
    let [dev, reviewer] = steps;
    assert_eq!((dev.to.as_str(), reviewer.to.as_str()), ("dev", "reviewer"));
    assert_ne!(dev.thread_id, reviewer.thread_id, "each step has a thread of its own");
    assert_eq!(dev.text, "build it");
    let marks = dev.trail_step.as_ref().unwrap();
    assert_eq!(marks.run, run);
    assert_eq!(marks.tip_key.as_deref(), Some("build"), "the step carries its tip key");
    assert_eq!((&marks.trail, &marks.script), (&None, &None), "an inline script is no saved trail");
    assert_eq!(reviewer.trail_step.as_ref().unwrap().tip_key, None);
    assert_eq!(dev.owner, Some(UserId::human()), "the step's thread keeps the run's owner");

    let unrelated = Envelope::new(ant("dev"), ant("lead"), "hello");
    assert!(!lead.trails.take(&unrelated).await.unwrap(), "a message on no step's thread is prompted as ever");
    assert!(lead.reply(&reviewer, "looks good").await);
    assert!(lead.reply(&dev, "built").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["built", "looks good"]) });
    let header = lead.header(&run).await;
    assert_eq!((header.steps, header.turns), (2, 2), "each reply spent a turn of the run");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await, "no step went twice");
}

#[tokio::test]
async fn a_reply_not_matching_the_schema_is_asked_again_and_raises_after_two_retries() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
async def ask(prompt):
    try:
        return await agent(prompt, ant="dev", schema=schema)
    except RuntimeError as e:
        return str(e)
await parallel(ask("count"), ask("guess"))
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let (first, second) = (lead.step().await, lead.step().await);
    let (count, guess) = if first.text.starts_with("count") { (first, second) } else { (second, first) };
    assert!(count.text.contains("JSON Schema"), "{}", count.text);

    assert!(lead.reply(&count, "about seven").await);
    let again = lead.step().await;
    assert_eq!(again.thread_id, count.thread_id, "asked again on the step's thread");
    assert!(again.text.contains("no JSON"), "{}", again.text);
    assert!(lead.reply(&again, "Here:\n```json\n{\"n\": 7}\n```").await);

    let mut asked = guess;
    for _ in 0..2 {
        assert!(lead.reply(&asked, "{\"n\": \"many\"}").await);
        asked = lead.step().await;
        assert!(asked.text.contains("$.n"), "the error is named: {}", asked.text);
    }
    assert!(lead.reply(&asked, "{\"m\": 1}").await);
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    assert_eq!(result[0], json!({"n": 7}));
    let error = result[1].as_str().unwrap();
    assert!(error.contains("did not match the schema, asked 3 times") && error.contains(r#"has no "n""#), "{error}");
}

#[tokio::test]
async fn a_human_step_asks_the_runs_owner_and_its_answer_comes_later() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
await human([{"question": "Ship it?", "options": ["yes", "no"]}])
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let asked = lead.step().await;
    assert_eq!(asked.to, UserId::human());
    assert!(asked.asks().is_some(), "the step asks its questions as ask_human does");
    eventually("the run waits for its human", async || lead.header(&run).await.state == RunState::Waiting).await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    let answer = Envelope::answering(&asked, UserId::human(), vec![Pick::option(1)]).unwrap();
    assert!(lead.trails.take(&answer).await.unwrap());
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("yes") });
}

#[tokio::test]
async fn a_restarted_daemon_finishes_a_run_without_sending_a_step_again() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run =
        lead.start(r#"await parallel(agent("a", ant="dev"), agent("b", ant="ops"))"#, TrailLimits::default()).await;
    let (first, second) = (lead.step().await, lead.step().await);
    let (a, b) = if first.text == "a" { (first, second) } else { (second, first) };
    assert!(lead.reply(&a, "A").await);
    drop(lead);

    let mut again = conductor(&server, &ants_binary(), Vec::new()).await;
    assert!(again.sends_no_step(Duration::from_secs(2)).await, "no step is sent twice");
    assert!(again.reply(&a, "A again").await, "the answered step's later reply is not prompted");
    assert!(again.reply(&b, "B").await, "the restored run takes the reply of a step sent before the restart");
    assert_eq!(again.ended(&run).await, RunState::Done { result: json!(["A", "B"]) });
}

#[tokio::test]
async fn a_failing_worker_is_retried() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-worker");
    let worker = dir.join("worker");
    let failed = dir.join("failed");
    let wrapper = format!(
        "#!/bin/sh\nif [ ! -e {failed:?} ]; then touch {failed:?}; exit 3; fi\nexec {:?} \"$@\"\n",
        ants_binary()
    );
    std::fs::write(&worker, wrapper).unwrap();
    std::process::Command::new("chmod").arg("+x").arg(&worker).status().unwrap();

    let mut lead = conductor(&server, &worker, Vec::new()).await;
    let run = lead.start(r#"await agent("go", ant="dev")"#, TrailLimits::default()).await;
    let step = lead.step().await;
    assert!(failed.exists(), "the first worker failed");
    assert!(lead.reply(&step, "went").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("went") });
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_stopped_run_gives_up_its_steps_and_resumes_with_fresh_ones() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run =
        lead.start(r#"await parallel(agent("a", ant="dev"), agent("b", ant="ops"))"#, TrailLimits::default()).await;
    let (first, second) = (lead.step().await, lead.step().await);

    let human = UserId::human();
    let mut waiting = lead.trails.stop(&run, &human).await.unwrap();
    waiting.sort_by_key(|step| step.to.to_string());
    let messages: Vec<_> = waiting.iter().map(|step| (step.to.as_str(), step.message)).collect();
    let mut sent = vec![(first.to.as_str(), first.id), (second.to.as_str(), second.id)];
    sent.sort_unstable();
    assert_eq!(messages, sent, "the steps waited on are for the human to withdraw");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: human.clone() });
    let told = lead.told().await;
    assert_eq!((&told.to, &told.thread_id), (&human, &lead.header(&run).await.thread), "{told:?}");
    assert!(told.text.contains("was stopped by human") && told.text.contains("ants trail resume"), "{}", told.text);
    assert!(lead.reply(&first, "late").await, "a stopped step's reply is not prompted");
    assert!(lead.sends_no_step(Duration::from_secs(1)).await);
    assert!(lead.trails.stop(&run, &human).await.is_err(), "a run stops once");

    lead.trails.resume(&run, &human, None).await.unwrap();
    let (first, second) = (lead.step().await, lead.step().await);
    assert!(![&first, &second].iter().any(|step| step.id == messages[0].1 || step.id == messages[1].1));
    assert!(lead.reply(&first, &first.text.to_uppercase()).await);
    assert!(lead.reply(&second, &second.text.to_uppercase()).await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["A", "B"]) });
}

#[tokio::test]
async fn a_run_past_its_wall_time_fails() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let limits = TrailLimits { wall_secs: 1, ..TrailLimits::default() };
    let run = lead.start(r#"await agent("never answered", ant="dev")"#, limits).await;
    lead.step().await;
    let mut state = RunState::Running;
    eventually_within(Duration::from_secs(10), "the run fails", async || {
        state = lead.header(&run).await.state;
        state.finished()
    })
    .await;
    let RunState::Failed { error, limit } = state else { panic!("not failed: {state:?}") };
    assert!(error.contains("wall time"), "{error}");
    assert_eq!(limit, Some(RunLimit::Wall), "the record says which limit failed it");
    let told = lead.told().await;
    assert!(told.text.contains(" failed (1 steps, 0 turns): ") && told.text.contains(&error), "{}", told.text);
    assert!(told.text.contains("under raised limits"), "{}", told.text);

    let raised = TrailLimits { wall_secs: 3600, ..limits };
    lead.trails.resume(&run, &UserId::human(), Some(raised)).await.unwrap();
    let step = lead.step().await;
    assert!(lead.reply(&step, "answered").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("answered") }, "resumed past its wall time");
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

#[tokio::test]
async fn a_run_reports_on_its_thread_and_tells_its_owner_the_result() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let thread = ThreadId::generate();
    let mut status = lead.client.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
    let script = r#"
phase("one")
log("asking dev")
built = await agent("build it", ant="dev", label="build")
log("built")
{"built": built, "count": 2}
"#;
    let run = lead.start_on(script, TrailLimits::default(), thread.clone()).await;
    let step = lead.step().await;
    assert_eq!(step.origin_thread.as_ref(), Some(&thread), "the step's thread is spun off the run's");
    assert_eq!(step.passes_on(), Some(&thread), "the step passes the link to the run's thread on");

    let notes = lead.notes(&thread, 2).await;
    assert_eq!(notes[..2], [format!("Run {run} of trail t started: a test"), "phase: one\nlog: asking dev".into()]);
    assert_eq!(lead.notes.on(&thread)[0].title.as_deref(), Some("trail t: a test"), "the thread's title");
    assert!(lead.notes.on(&thread).iter().all(|note| note.from == ant("lead")));
    let (started, stage) = step_status(&mut status).await;
    assert_eq!((&started.ant, &started.thread_id, stage), (&ant("lead"), &thread, StepStage::Started));
    let StatusUpdate::Step { run: of, call, label, to, step_thread, .. } = started.update else { unreachable!() };
    let sent = step.trail_step.as_ref().unwrap();
    assert_eq!((of, call, label.as_deref()), (run.clone(), sent.call, Some("build")));
    assert_eq!((to, step_thread), (ant("dev"), step.thread_id.clone()));

    assert!(lead.reply(&step, "done").await);
    assert_eq!(step_status(&mut status).await.1, StepStage::Answered);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!({"built": "done", "count": 2}) });
    assert_eq!(lead.notes(&thread, 3).await[2], "log: built");
    let told = lead.told().await;
    assert_eq!((&told.from, &told.to, &told.thread_id), (&ant("lead"), &UserId::human(), &thread));
    assert_eq!(told.owner, Some(UserId::human()));
    let result = "```json\n{\n  \"built\": \"done\",\n  \"count\": 2\n}\n```";
    assert!(told.text.starts_with(&format!("Run {run}")), "{}", told.text);
    assert!(told.text.ends_with(&format!(" is done (1 steps, 1 turns):\n\n{result}")), "{}", told.text);
}

#[tokio::test]
async fn a_notice_on_a_step_thread_is_noted_on_the_run_thread_and_a_withdrawn_step_fails() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let thread = ThreadId::generate();
    let mut status = lead.client.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
    let script = r#"
async def go():
    try:
        return await agent("go", ant="dev")
    except RuntimeError as e:
        return str(e)
await go()
"#;
    let run = lead.start_on(script, TrailLimits::default(), thread.clone()).await;
    let step = lead.step().await;
    let notice = step.reply(ant("dev"), "[ants] dev is out of quota.", Role::Ant).unwrap().as_notice(NoticeKind::Quota);
    assert!(lead.trails.take(&notice).await.unwrap(), "a notice prompts no conductor");
    let call = step.trail_step.as_ref().unwrap().call;
    let noted = |kind: &str, text: &str| {
        format!("A {kind} notice from dev on the thread {} of step {call} to dev, run {run}:\n{text}", step.thread_id)
    };
    assert_eq!(lead.notes(&thread, 2).await[1], noted("quota", "[ants] dev is out of quota."));
    assert!(!lead.header(&run).await.finished(), "the step is still waited on");

    let human = Envelope::on_thread(step.thread_id.clone(), UserId::human(), ant("lead"), "how goes it?");
    assert!(!lead.trails.take(&human).await.unwrap(), "a human's message on a step's thread is prompted");

    let withdrawn = step.reply(ant("dev"), "[ants] Withdrawn.", Role::Ant).unwrap().as_notice(NoticeKind::Withdrawn);
    assert!(lead.trails.take(&withdrawn).await.unwrap());
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    assert!(result.as_str().unwrap().contains("dev never answered: the step's message was withdrawn"), "{result}");
    assert_eq!(lead.notes(&thread, 3).await[2], noted("withdrawn", "[ants] Withdrawn."));
    assert_eq!(step_status(&mut status).await.1, StepStage::Started);
    assert_eq!(step_status(&mut status).await.1, StepStage::Failed);
}

#[tokio::test]
async fn a_reply_after_the_run_ended_is_noted_on_its_thread_not_prompted() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let thread = ThreadId::generate();
    let run = lead.start_on(r#"await agent("a", ant="dev")"#, TrailLimits::default(), thread.clone()).await;
    let step = lead.step().await;
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    let marker = runs.get(trail_thread_step_key(&ant("lead"), &thread, &step.thread_id)).await.unwrap();
    let marker: TrailStepThread = serde_json::from_slice(&marker.expect("kept under the run's thread")).unwrap();
    let call = step.trail_step.as_ref().unwrap().call;
    assert_eq!((marker.run, marker.call, marker.run_thread), (run.clone(), call, thread.clone()));
    assert!(lead.reply(&step, "A").await);
    lead.ended(&run).await;
    let late = |text: &str| step.reply(ant("dev"), text, Role::Ant).unwrap();
    assert!(lead.trails.take(&late("A, again")).await.unwrap(), "a reply after the end prompts no conductor");
    let noted = |text: &str| {
        let (thread, call) = (&step.thread_id, step.trail_step.as_ref().unwrap().call);
        format!(
            "A reply from dev on the thread {thread} of step {call} to dev, which the run no longer waits on, run {run}:\n{text}"
        )
    };
    assert_eq!(lead.notes(&thread, 2).await[1], noted("A, again"));
    let plain = Envelope::on_thread(step.thread_id.clone(), ant("dev"), ant("lead"), "something else");
    assert!(!lead.trails.take(&plain).await.unwrap(), "a message replying to nothing is prompted");
    let other = late("not mine").reply(ant("ops"), "neither mine", Role::Ant).unwrap();
    assert!(!lead.trails.take(&other).await.unwrap(), "a reply of another ant on the step's thread is prompted");
    drop(lead);

    let again = conductor(&server, &ants_binary(), Vec::new()).await;
    assert!(again.trails.take(&late("A, once more")).await.unwrap(), "after a restart too");
    assert_eq!(again.notes(&thread, 1).await[0], noted("A, once more"));
}

impl Conductor {
    /// The stored session of `run`, ants_trail's run state, as JSON.
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

#[tokio::test]
async fn a_run_whose_session_the_worker_cannot_restore_is_replayed_from_its_journal() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    // A dump of another Monty version, which this one cannot read, and a session lost altogether.
    for snapshot in [json!({"ResolveFutures": {"dump_version": 99}}), Value::Null] {
        let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
        let (run, b) = second_step_stored(&mut lead, TWO_STEPS).await;
        let (stored, mut state) = lead.session(&run).await;
        assert!(!state["snapshot"].is_null());
        state["snapshot"] = snapshot.clone();
        lead.put_session(&run, stored, &state).await;
        drop(lead);

        let mut again = conductor(&server, &ants_binary(), Vec::new()).await;
        assert!(again.reply(&b, "B").await);
        assert_eq!(again.ended(&run).await, RunState::Done { result: json!("AB") }, "with {snapshot}");
        assert!(again.sends_no_step(Duration::from_millis(200)).await, "the replay sends no step again");
    }
}

#[tokio::test]
async fn a_replay_of_a_changed_script_fails_the_run_naming_the_call_that_differs() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let (run, b) = second_step_stored(&mut lead, TWO_STEPS).await;
    let (stored, mut state) = lead.session(&run).await;
    let script = state["script"].as_str().unwrap().replace(r#"agent("a""#, r#"agent("z""#);
    state["script"] = json!(script);
    state["snapshot"] = json!({"ResolveFutures": {"dump_version": 99}});
    lead.put_session(&run, stored, &state).await;
    drop(lead);

    let again = conductor(&server, &ants_binary(), Vec::new()).await;
    assert!(again.reply(&b, "B").await);
    let RunState::Failed { error, limit } = again.ended(&run).await else { panic!("not failed") };
    assert!(error.contains("diverges from the journal: call "), "{error}");
    assert!(error.contains(r#""prompt":"z""#) && error.contains(r#""prompt":"a""#), "{error}");
    assert_eq!(limit, None);
    let refused = again.trails.resume(&run, &UserId::human(), None).await.unwrap_err();
    assert!(refused.contains("failed, not on a limit"), "{refused}");
}

/// A user JWT for `name`, tagged `tag`, signed by `account` for the key `key`.
fn user_jwt(account: &nkeys::KeyPair, key: &nkeys::KeyPair, name: &UserId, tag: &str) -> String {
    use data_encoding::BASE64URL_NOPAD;
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let claims = json!({
        "sub": key.public_key(), "iss": account.public_key(), "name": name,
        "nats": { "type": "user", "tags": [tag] },
    });
    let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
    let signature = BASE64URL_NOPAD.encode(&account.sign(format!("{header}.{body}").as_bytes()).unwrap());
    format!("{header}.{body}.{signature}")
}

#[tokio::test]
async fn the_owners_assistant_answers_a_human_step_while_the_grant_names_it() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (human, pa) = (UserId::human(), ant("pa"));
    let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &human, "grant").await.unwrap();
    let bucket = js.create_key_value(ants_daemon::hub::humans_config()).await.unwrap();
    let (account, device) = (nkeys::KeyPair::new_account(), nkeys::KeyPair::new_user());
    let jwt = user_jwt(&account, &device, &human, &DeviceId::generate().tag());
    let mut grant = AssistantGrant::new(human.clone(), Some(pa.clone()), chrono::Utc::now(), jwt);
    grant.signature = data_encoding::BASE64URL_NOPAD.encode(&device.sign(&grant.signed()).unwrap());
    bucket.put(grant.key(), serde_json::to_vec(&grant).unwrap().into()).await.unwrap();
    let humans = Registry::new(human.clone()).verifying_grants(Some(account.public_key()));
    let _watching = humans.watch(js, Duration::from_secs(1));
    eventually("the grant is read", async || humans.assistant(&human).as_ref() == Some(&pa)).await;

    let mut lead = conductor_with(&server, &ants_binary(), Vec::new(), humans).await;
    let run =
        lead.start(r#"await human([{"question": "Ship it?", "options": ["yes", "no"]}])"#, TrailLimits::default());
    let run = run.await;
    let asked = lead.step().await;
    assert_eq!(asked.to, human);
    let answer = |by: &str, pick: u32| {
        Envelope::answering(&asked, ant(by), vec![Pick::option(pick)]).unwrap().on_behalf(Some(human.clone()))
    };
    assert!(!lead.trails.take(&answer("dev", 2)).await.unwrap(), "the grant names no other ant");
    assert!(lead.trails.take(&answer("pa", 1)).await.unwrap(), "the assistant answers for its human");
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("yes") });
}

#[tokio::test]
async fn a_schedule_s_firing_starts_its_saved_trail_once_on_its_thread() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-fired");
    std::fs::write(dir.join("count.py"), format!("{META}args[\"n\"] + 1\n")).unwrap();
    let lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let fire = |start: &TrailStart| {
        Envelope::new(ant("nightly"), ant("lead"), "Scheduled run of trail.")
            .with_owner(Some(UserId::human()))
            .from_webhook()
            .starting(Some(start.clone()))
    };
    let limits = TrailLimits { steps: 7, wall_secs: 3600, ..TrailLimits::DEFAULT };
    let start = TrailStart { trail: "count".parse().unwrap(), args: json!({"n": 1}), limits };
    let firing = fire(&start);

    let run = lead.trails.start_fired(&firing, &start).await.unwrap();
    assert_eq!(run.as_str(), firing.id.to_string(), "the run takes the firing's id");
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(2) });
    let header = lead.header(&run).await;
    assert_eq!((&header.thread, header.owner.as_ref()), (&firing.thread_id, Some(&UserId::human())));
    assert_eq!((header.trail(), header.limits), (Some(&start.trail), limits), "under the limits asked for");
    assert!(!header.origin.trusted(), "a schedule's run is as untrusted as a webhook's: {:?}", header.origin);
    assert_eq!(lead.trails.start_fired(&firing, &start).await.unwrap(), run, "delivered again");
    assert_eq!(lead.header(&run).await.started_at, header.started_at, "no run starts again");

    let missing = TrailStart { trail: "gone".parse().unwrap(), args: Value::Null, limits: TrailLimits::DEFAULT };
    let error = lead.trails.start_fired(&fire(&missing), &missing).await.unwrap_err();
    assert!(error.contains("no trail is saved as gone"), "{error}");
    std::fs::remove_dir_all(&dir).unwrap();
}

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
    let humans = Registry::new(UserId::human());
    let mcp = Mcp::start(ant("lead"), &dir.join("home"), outbox, colony, notes, humans, None).await.unwrap();
    mcp.with_trails(trails.clone());
    mcp
}

/// The id of the run `run_trail` says it started.
fn started(text: &str) -> RunId {
    text.strip_prefix("Started run ").unwrap().split_once(' ').unwrap().0.parse().unwrap()
}

#[tokio::test]
async fn run_trail_starts_a_saved_or_inline_trail_and_a_webhook_s_thread_runs_only_saved_ones() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-mcp");
    std::fs::create_dir_all(dir.join("saved")).unwrap();
    std::fs::write(dir.join("saved/count.py"), format!("{META}args[\"n\"] + 1\n")).unwrap();
    let lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.join("saved"))).await;
    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;

    let asked = Envelope::new(UserId::human(), ant("lead"), "count").with_owner(Some(UserId::human()));
    let _turn = mcp.begin(&asked);
    let client = mcp_client(&mcp, &asked.thread_id).await;
    let script = format!("{META}args[\"n\"] * 10\n");
    let (ok, text) = run_trail(&client, json!({"script": script, "args": {"n": 4}, "wait": true})).await;
    assert!(ok && text.contains("is done. Result:\n40\nrate_trail(run: "), "{text}");
    let (ok, text) = run_trail(&client, json!({"name": "count", "args": {"n": 4}})).await;
    assert!(ok && text.starts_with("Started run "), "{text}");
    let run = started(&text);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(5) });
    let header = lead.header(&run).await;
    assert_eq!((&header.thread, header.owner.as_ref()), (&asked.thread_id, Some(&UserId::human())));
    let (ok, text) = run_trail(&client, json!({"name": "count", "script": script})).await;
    assert!(!ok && text.contains("either `name` or `script`"), "{text}");
    let limits = json!({"steps": 9, "at_once": 2, "turns": 30, "wall": "2h"});
    let (ok, text) = run_trail(&client, json!({"name": "count", "args": {"n": 1}, "limits": limits})).await;
    assert!(ok, "{text}");
    let asked = TrailLimits { steps: 9, concurrency: 2, turns: 30, wall_secs: 7200, ..TrailLimits::DEFAULT };
    assert_eq!(lead.header(&started(&text)).await.limits, asked, "the run's record keeps the limits asked for");
    for (limits, why) in [
        (json!({"steps": 501}), "limits: steps is over the most a run may take, 500"),
        (json!({"wall": "8d"}), "limits: wall is over the most a run may take, 7d"),
        (json!({"at_once": 0}), "limits: at_once must be more than 0"),
    ] {
        let (ok, text) = run_trail(&client, json!({"name": "count", "limits": limits})).await;
        assert!(!ok && text.contains(why), "{text}");
    }

    let hooked = Envelope::new(ant("github"), ant("lead"), "an issue").from_webhook();
    let _turn = mcp.begin(&hooked);
    let client = mcp_client(&mcp, &hooked.thread_id).await;
    let (ok, text) = run_trail(&client, json!({"script": script, "args": {"n": 1}})).await;
    assert!(!ok && text.contains("only saved trails"), "{text}");
    let (ok, text) = run_trail(&client, json!({"name": "count", "args": {"n": 1}, "wait": true})).await;
    assert!(ok && text.contains("is done. Result:\n2\n"), "{text}");

    let idle = ThreadId::generate();
    let (ok, text) = run_trail(&mcp_client(&mcp, &idle).await, json!({"name": "count"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn trail_status_and_stop_trail_reach_only_the_runs_the_ant_conducts() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-mcp-status");
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;
    let asked = Envelope::new(UserId::human(), ant("lead"), "review").with_owner(Some(UserId::human()));
    let _turn = mcp.begin(&asked);
    let client = mcp_client(&mcp, &asked.thread_id).await;
    let script = format!("{META}phase(\"one\")\nawait agent(\"go\", ant=\"dev\", label=\"the go\")\n");
    let (ok, text) = run_trail(&client, json!({"script": script})).await;
    assert!(ok, "{text}");
    let run = started(&text);
    let step = lead.step().await;

    let (ok, status) = call(&client, "trail_status", json!({"run": run.as_str()})).await;
    assert!(ok, "{status}");
    for told in [": running.", "Spent 1 of 50 steps, 0 of 200 turns", "of 1d wall time", "to ant dev (the go)"] {
        assert!(status.contains(told), "{told:?} in {status}");
    }
    let (ok, listed) = call(&client, "trail_status", json!({})).await;
    assert!(ok && listed.contains(run.as_str()) && listed.contains("1 of 50 steps"), "{listed}");

    let mut other = lead.header(&run).await;
    (other.conductor, other.run) = (ant("dev"), RunId::generate());
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    runs.put(other.key(), serde_json::to_vec(&other).unwrap()).await.unwrap();
    for tool in ["trail_status", "stop_trail"] {
        let (ok, text) = call(&client, tool, json!({"run": other.run.as_str()})).await;
        assert!(!ok && text.contains(&format!("lead conducts no run {}", other.run)), "{tool}: {text}");
    }
    let (ok, text) = call(&client, "stop_trail", json!({"run": "nonsense"})).await;
    assert!(!ok && text.contains("nonsense"), "{text}");

    let (ok, text) = call(&client, "stop_trail", json!({"run": run.as_str()})).await;
    assert!(ok && text.starts_with(&format!("Stopped run {run}")), "{text}");
    assert!(text.contains(&format!("dev message {}", step.id)), "the step for a human to withdraw: {text}");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: ant("lead") });
    assert!(lead.told().await.text.contains("was stopped by lead"));
    let (ok, status) = call(&client, "trail_status", json!({"run": run.as_str()})).await;
    assert!(ok && status.contains("stopped by lead") && status.contains("ants trail resume"), "{status}");
    let (ok, text) = call(&client, "stop_trail", json!({"run": run.as_str()})).await;
    assert!(!ok && text.contains("already stopped"), "{text}");
    assert!(lead.reply(&step, "late").await, "a stopped step's reply is not prompted");
    assert!(lead.sends_no_step(Duration::from_millis(500)).await);

    lead.trails.resume(&run, &UserId::human(), None).await.unwrap();
    let step = lead.step().await;
    assert!(lead.reply(&step, "went").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("went") });
    let (ok, status) = call(&client, "trail_status", json!({"run": run.as_str()})).await;
    assert!(ok && status.contains(": done.") && status.contains("Result:\nwent\nrate_trail(run: "), "{status}");
    let (ok, listed) = call(&client, "trail_status", json!({})).await;
    assert!(ok && listed == "This ant conducts no run that has not ended.", "{listed}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The outcome of `order` once its machine's daemon recorded it.
async fn order_outcome(js: &async_nats::jetstream::Context, order: &AdminOrder) -> AdminOutcome {
    let machines = DirectKv::new(js, None, MACHINES_BUCKET).await.unwrap();
    let mut decided = None;
    eventually("the order is decided", async || {
        decided = machines.get(&order.outcome_key()).await.unwrap();
        decided.is_some()
    })
    .await;
    serde_json::from_slice(&decided.unwrap()).unwrap()
}

#[tokio::test]
async fn admin_orders_stop_and_resume_a_run_once_the_conductor_s_daemon_is_back() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &UserId::human(), "admin").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(r#"await agent("go", ant="dev")"#, TrailLimits::default()).await;
    let step = lead.step().await;

    let (account, key, alice) = (nkeys::KeyPair::new_account(), nkeys::KeyPair::new_user(), ant("alice"));
    let jwt = user_jwt(&account, &key, &alice, ants_proto::ADMIN_TAG);
    let m1: Machine = "m1".parse().unwrap();
    let order = |op| {
        let mut order = AdminOrder::new(alice.clone(), m1.clone(), ant("lead"), op, jwt.clone());
        order.signature = data_encoding::BASE64URL_NOPAD.encode(&key.sign(&order.signed()).unwrap());
        order
    };
    let publish = async |order: &AdminOrder| {
        let message = async_nats::jetstream::message::PublishMessage::build()
            .payload(serde_json::to_vec(order).unwrap().into())
            .message_id(order.id.to_string());
        js.send_publish(order.subject(), message).await.unwrap().await.unwrap();
    };

    // m1's daemon is away: the order waits in the stream.
    let stop = order(AdminOp::StopTrail { run: run.clone() });
    publish(&stop).await;
    // Back, it takes the order while the daemon serving lead connects, and applies it once lead conducts its runs.
    let restarts = Restarts::default();
    let _served = restarts.serve(&ant("lead"));
    let orders = ants_daemon::admin::Orders {
        machine: m1.clone(),
        nats: server.url.clone(),
        remote: true,
        accounts: vec![account.public_key()],
        config: None,
        creds: None,
        js_domain: None,
        restarts: restarts.clone(),
        providers: Default::default(),
        leaf: None,
        hold: ants_daemon::admin::held::Hold::new(Registry::new(UserId::human())),
    };
    let taking = tokio::spawn(ants_daemon::admin::serve(js.clone(), None, orders));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(lead.header(&run).await.state, RunState::Running, "not stopped before lead conducts its runs");
    let conducted = restarts.conduct(&ant("lead"), &lead.trails);
    let stopped = order_outcome(&js, &stop).await;
    assert_eq!((stopped.state, stopped.admin.as_ref()), (OrderState::Applied, Some(&alice)), "{stopped:?}");
    let waiting = vec![WaitingStep { to: ant("dev"), message: step.id }];
    let answer: TrailOutcome = serde_json::from_value(stopped.result.unwrap()).unwrap();
    assert_eq!(answer, TrailOutcome::Stopped { run: run.clone(), waiting }, "the steps for the admin to withdraw");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: alice.clone() });
    assert!(lead.told().await.text.contains("was stopped by alice"));

    let limits = TrailLimits { steps: 9, turns: 40, ..TrailLimits::DEFAULT };
    let resume = order(AdminOp::ResumeTrail { run: run.clone(), limits: Some(limits) });
    publish(&resume).await;
    let resumed = order_outcome(&js, &resume).await;
    assert_eq!(resumed.state, OrderState::Applied, "{resumed:?}");
    assert_eq!(
        serde_json::from_value::<TrailOutcome>(resumed.result.unwrap()).unwrap(),
        TrailOutcome::Resumed { run: run.clone() }
    );
    assert_eq!(lead.header(&run).await.limits, limits, "the run's record keeps the limits the order gave");
    let step = lead.step().await;
    assert!(lead.reply(&step, "went").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("went") });

    let refusals = [
        (AdminOp::StopTrail { run: run.clone() }, format!("run {run} has ended")),
        (AdminOp::ResumeTrail { run: run.clone(), limits: None }, format!("run {run} is done")),
        (AdminOp::StopTrail { run: RunId::generate() }, "lead conducts no run".to_owned()),
    ];
    for (op, why) in refusals {
        let refused = order(op);
        publish(&refused).await;
        let refused = order_outcome(&js, &refused).await;
        assert_eq!(refused.state, OrderState::Refused, "{refused:?}");
        assert!(refused.reason.as_deref().unwrap().contains(&why), "{why}: {refused:?}");
    }
    drop(conducted);
    taking.abort();
}
