//! The conductor's daemon running trails ([`ants_daemon::trail`]) against a throwaway nats-server, with real
//! `ants subprocess` workers: the ants a run's steps go to are played by the test, which reads the steps the conductor
//! sends and hands it their replies, as its inbox would.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ants_daemon::people::humans::Registry;
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::trail::{Conducting, StartRun, Trails};
use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    AssistantGrant, ColonyEntry, DeviceId, Envelope, Pick, Role, RunId, RunState, TRAIL_RUNS_BUCKET, ThreadId,
    TrailAction, TrailLimits, TrailOutcome, TrailReply, TrailRequest, TrailRun, TrailSession, TrailSource, UserId,
    subject, trail_run_key, trail_session_key,
};
use ants_test_support::{NatsServer, eventually, eventually_within, scratch_dir};
use futures::StreamExt as _;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const META: &str = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": [\"one\"]}\n";

fn ant(name: &str) -> UserId {
    name.parse().unwrap()
}

/// A conductor serving its runs, its sent steps on `sent`.
struct Conductor {
    trails: Trails,
    js: async_nats::jetstream::Context,
    client: async_nats::Client,
    sent: mpsc::UnboundedReceiver<Envelope>,
    serving: JoinHandle<()>,
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
    conductor_with(server, worker, colony, Registry::new(UserId::human())).await
}

/// [`conductor`], knowing the humans and their assistants' grants from `humans`.
async fn conductor_with(server: &NatsServer, worker: &Path, colony: Vec<ColonyEntry>, humans: Registry) -> Conductor {
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
        humans,
        colony,
        outbox,
        attempts,
    };
    let trails = Trails::new(conducting).await.unwrap();
    trails.restore().await;
    let serving = tokio::spawn({
        let trails = trails.clone();
        async move { trails.serve().await }
    });
    Conductor { trails, js, client, sent, serving }
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
        let human = UserId::human();
        let start = StartRun {
            trail: TrailSource::Script(format!("{META}{script}")),
            args: Value::Null,
            owner: Some(human.clone()),
            origin: Origin { by: Starter::Human { user: human }, channel: None },
            thread: ThreadId::generate(),
            limits,
        };
        self.trails.start(start).await.unwrap()
    }

    /// The next step the conductor sends.
    async fn step(&mut self) -> Envelope {
        tokio::time::timeout(Duration::from_secs(30), self.sent.recv()).await.expect("a step").unwrap()
    }

    /// Whether the conductor sends nothing for `wait`.
    async fn sends_nothing(&mut self, wait: Duration) -> bool {
        tokio::time::timeout(wait, self.sent.recv()).await.is_err()
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
    assert!(lead.sent.try_recv().is_err(), "no step went twice");
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
    assert!(again.sends_nothing(Duration::from_secs(2)).await, "no step is sent twice");
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
    assert!(lead.reply(&first, "late").await, "a stopped step's reply is not prompted");
    assert!(lead.sends_nothing(Duration::from_secs(1)).await);
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
        assert!(again.sent.try_recv().is_err(), "the replay sends no step again");
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
    let RunState::Failed { error } = again.ended(&run).await else { panic!("not failed") };
    assert!(error.contains("diverges from the journal: call "), "{error}");
    assert!(error.contains(r#""prompt":"z""#) && error.contains(r#""prompt":"a""#), "{error}");
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
