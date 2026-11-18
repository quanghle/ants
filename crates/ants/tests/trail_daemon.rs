//! The conductor's daemon running trails ([`ants_daemon::trail`]) against a throwaway nats-server, with real
//! `ants subprocess` workers: the ants a run's steps go to are played by the test, which reads the steps the conductor
//! sends and hands it their replies, as its inbox would. Thread notes are kept in memory.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_daemon::people::humans::Registry;
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::stream::notes::{Notes, NotesStore};
use ants_daemon::trail::{Conducting, StartRun, Trails};
use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    ColonyEntry, Envelope, NotesPost, NoticeKind, Pick, Role, RunId, RunState, StatusEvent, StatusUpdate, StepStage,
    TRAIL_RUNS_BUCKET, ThreadId, TrailAction, TrailLimits, TrailOutcome, TrailReply, TrailRequest, TrailRun,
    TrailSource, UserId, subject, trail_run_key,
};
use ants_test_support::{NatsServer, eventually, eventually_within, scratch_dir};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const META: &str = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": [\"one\"]}\n";

fn ant(name: &str) -> UserId {
    name.parse().unwrap()
}

/// A conductor serving its runs, what it sends on `sent`: its steps, and the other messages it sends, which
/// [`Conductor::step`] puts aside in `told`.
struct Conductor {
    trails: Trails,
    js: async_nats::jetstream::Context,
    client: async_nats::Client,
    sent: mpsc::UnboundedReceiver<Envelope>,
    told: Vec<Envelope>,
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
    for config in [ants_daemon::hub::trails_config(), ants_daemon::hub::trail_runs_config()] {
        js.create_key_value(config).await.unwrap();
    }
}

/// The conductor `lead`'s daemon, its runs continued, with `worker` as the `ants` binary and `colony` as the colony.
async fn conductor(server: &NatsServer, worker: &Path, colony: Vec<ColonyEntry>) -> Conductor {
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
        trails_dir: None,
        humans: Registry::new(UserId::human()),
        colony,
        outbox,
        attempts,
        notes: notes.clone(),
    };
    let trails = Trails::new(conducting).await.unwrap();
    trails.restore().await;
    let serving = tokio::spawn({
        let trails = trails.clone();
        async move { trails.serve().await }
    });
    Conductor { trails, js, client, sent, told: Vec::new(), notes, serving }
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
            self.told.push(sent);
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
            self.told.push(sent);
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
            self.told.push(sent);
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
await parallel(agent("build it", ant="dev"), agent("review it", capability="review"))
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
    assert_eq!(dev.trail_step.as_ref().unwrap().run, run);
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
    let RunState::Failed { error } = state else { panic!("not failed: {state:?}") };
    assert!(error.contains("wall time"), "{error}");
    let told = lead.told().await;
    assert!(told.text.contains(" failed (1 steps, 0 turns): ") && told.text.contains(&error), "{}", told.text);
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
