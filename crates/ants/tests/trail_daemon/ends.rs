//! A run's end: stored even when its first put fails, bounded to what its record holds, and told to its owner at least
//! once, under an id derived from the run and its end.

use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::{PendingCall, TrailRunIndex, trail_runs_index_key};

use super::*;

/// The most bytes of a run's record.
/// The most a worker lets a script return (ants-trail's MAX_RESULT).
const MAX_RESULT: usize = 768 * 1024;

/// Set the largest message the runs bucket takes: -1 for no limit beyond the server's.
async fn limit_runs(js: &async_nats::jetstream::Context, max: i32) {
    let stream = js.get_stream(format!("KV_{TRAIL_RUNS_BUCKET}")).await.unwrap();
    let mut config = stream.cached_info().config.clone();
    config.max_message_size = max;
    js.update_stream(config).await.unwrap();
}

/// The runs `lead`'s index lists.
async fn listed(js: &async_nats::jetstream::Context) -> Vec<RunId> {
    let kv = DirectKv::new(js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    let index = kv.get(trail_runs_index_key(&ant("lead"))).await.unwrap();
    index.map_or_default(|index| serde_json::from_slice::<TrailRunIndex>(&index).unwrap().runs)
}

/// List `run` in `lead`'s index again, as a daemon stopped before telling its end left it.
async fn relist(js: &async_nats::jetstream::Context, run: &RunId) {
    let kv = js.get_key_value(TRAIL_RUNS_BUCKET).await.unwrap();
    let index = TrailRunIndex { runs: vec![run.clone()] };
    kv.put(trail_runs_index_key(&ant("lead")), serde_json::to_vec(&index).unwrap().into()).await.unwrap();
}

impl Conductor {
    /// The next message telling the human the end of `run`.
    async fn end_message(&mut self, run: &RunId) -> Envelope {
        loop {
            let told = self.told().await;
            if told.to == UserId::human() && told.text.starts_with(&format!("Run {run} ")) {
                return told;
            }
        }
    }

    /// The messages telling the end of `run`, and those inviting a rating of it, sent within `wait`.
    async fn ends_sent(&mut self, run: &RunId, wait: Duration) -> (usize, usize) {
        assert!(self.sends_no_step(wait).await);
        let of = |message: &Envelope, prefix: &str| message.text.starts_with(&format!("{prefix}Run {run} "));
        let told = self.told.iter().filter(|told| of(told, "")).count();
        let invited = self.invited.iter().filter(|invited| of(invited, "[ants] ")).count();
        (told, invited)
    }
}

/// [`conductor`], whose outbox fails the first `fails` messages to the human that are no steps.
async fn conductor_failing(server: &NatsServer, fails: usize) -> (Conductor, Arc<AtomicUsize>) {
    let lead = ant("lead");
    let (client, js) = ants_daemon::bus::connect(&server.url, None, None, &lead, "conductor").await.unwrap();
    let attempts = ants_daemon::bus::ensure_attempts(&js, None, &lead).await.unwrap();
    let (tx, sent) = mpsc::unbounded_channel();
    let tried = Arc::new(AtomicUsize::new(0));
    let outbox: ants_daemon::mcp::Outbox = Arc::new({
        let tried = tried.clone();
        move |envelope: Envelope| {
            let tx = tx.clone();
            let owner = envelope.to == UserId::human() && envelope.trail_step.is_none();
            let failed = owner && tried.fetch_add(1, Ordering::SeqCst) < fails;
            Box::pin(async move {
                if failed {
                    anyhow::bail!("the outbox is down");
                }
                tx.send(envelope).unwrap();
                Ok(())
            })
        }
    });
    let notes = Arc::new(MemoryNotes::default());
    let colony: ants_daemon::mcp::Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let conducting = Conducting {
        js: js.clone(),
        js_domain: None,
        client: client.clone(),
        ant: lead,
        worker: ants_binary(),
        trails_dir: None,
        humans: Registry::new(UserId::human()),
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
    let conductor = Conductor { trails, js, client, sent, told: Vec::new(), invited: Vec::new(), notes, serving };
    (conductor, tried)
}

#[tokio::test]
async fn a_run_whose_end_is_not_stored_at_first_stores_it_on_a_retry_and_tells_it_once() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start("r = await agent(\"go\", ant=\"dev\")\nr * 150000\n", TrailLimits::default()).await;
    let step = lead.step().await;
    eventually("the step is stored sent", async || {
        let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
        let session = runs.get(trail_session_key(&ant("lead"), &run)).await.unwrap().unwrap();
        serde_json::from_slice::<TrailSession>(&session).unwrap().pending.iter().all(|call| call.sent)
    })
    .await;
    // The ended header, holding a result of 300000 bytes, is past what the bucket now takes.
    limit_runs(&lead.js, 200_000).await;
    assert!(lead.reply(&step, "ab").await);
    eventually("the run ends in memory", async || lead.trails.status(&run).await.unwrap().header.finished()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(lead.header(&run).await.state, RunState::Running, "its end is not stored");
    limit_runs(&lead.js, -1).await;

    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    assert_eq!(result.as_str().map(str::len), Some(300_000));
    let told = lead.end_message(&run).await;
    assert!(told.text.contains(" is done ("), "{}", told.text);
    lead.invitation().await;
    assert_eq!(lead.ends_sent(&run, Duration::from_secs(3)).await, (0, 0), "each is sent once");
    assert!(!listed(&lead.js).await.contains(&run));
    assert!(lead.trails.unfinished().is_empty(), "the run is forgotten");
}

#[tokio::test]
async fn a_run_s_result_or_error_past_its_record_fails_or_is_cut_and_its_owner_is_told() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start("phase(\"p\" * 1100000)\n\"x\" * 1100000\n", TrailLimits::default()).await;
    let RunState::Failed { error, limit } = lead.ended(&run).await else { panic!("not failed") };
    assert_eq!(error, format!("the trail's result has 1100002 bytes of JSON; a result holds at most {MAX_RESULT}"));
    assert_eq!(limit, None, "no raised limit holds a larger record");
    assert_eq!(lead.header(&run).await.phase.map(|phase| phase.chars().count()), Some(1000), "the phase is cut");
    let told = lead.end_message(&run).await;
    assert!(told.text.contains(" failed (") && told.text.contains("holds at most"), "{}", told.text);
    assert!(lead.invitation().await.text.contains(&run.to_string()));

    let raising = lead.start("raise ValueError(\"e\" * 1100000)\n", TrailLimits::default()).await;
    let RunState::Failed { error, .. } = lead.ended(&raising).await else { panic!("not failed") };
    // The worker cuts an error at 16 KiB, which the record holds.
    assert!(error.len() <= 16 * 1024 && error.len() > 15 * 1024, "the error is cut: {} bytes", error.len());
    assert!(error.ends_with('…'));
    lead.end_message(&raising).await;
    eventually("both runs are unlisted", async || listed(&lead.js).await.is_empty()).await;
}

#[tokio::test]
async fn a_child_run_s_result_past_its_record_raises_in_its_parent() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-end-child");
    std::fs::write(dir.join("big.py"), format!("{META}\"x\" * 1100000\n")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir)).await;
    let script = r#"
try:
    r = await trail("big")
except RuntimeError as e:
    r = str(e)
r
"#;
    let thread = ThreadId::generate();
    let run = lead.start_on(script, TrailLimits::default(), thread).await;
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    let expected =
        format!("trail big failed: the trail's result has 1100002 bytes of JSON; a result holds at most {MAX_RESULT}");
    assert_eq!(result, json!(expected));
    let session = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    let stored = session.get(trail_session_key(&ant("lead"), &run)).await.unwrap().unwrap();
    // The parent's session, last stored waiting on its call, names the child run.
    let pending = serde_json::from_slice::<TrailSession>(&stored).unwrap().pending;
    let child = pending.first().and_then(PendingCall::child).expect("a child run");
    assert!(matches!(lead.header(&child).await.state, RunState::Failed { limit: None, .. }));
    assert!(lead.end_message(&run).await.text.contains(" is done ("));
}

#[tokio::test]
async fn a_run_s_end_message_is_sent_again_until_stored_and_once_more_on_a_restart_under_one_id() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (mut lead, tried) = conductor_failing(&server, 1).await;
    let run = lead.start("1 + 1\n", TrailLimits::default()).await;
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(2) });
    let told = lead.end_message(&run).await;
    assert_eq!(tried.load(Ordering::SeqCst), 2, "sent again once the first failed");
    assert_eq!(told.id.get_version_num(), 7);
    let invited = lead.invitation().await;
    assert_ne!(told.id, invited.id);
    assert_eq!(lead.ends_sent(&run, Duration::from_millis(500)).await, (0, 0));
    eventually("the run is unlisted", async || !listed(&lead.js).await.contains(&run)).await;

    // A daemon stopped after storing the end and before telling it.
    let js = lead.js.clone();
    drop(lead);
    relist(&js, &run).await;
    let mut again = conductor(&server, &ants_binary(), Vec::new()).await;
    let resent = again.end_message(&run).await;
    assert_eq!((resent.id, &resent.text), (told.id, &told.text), "the same message, which the bus keeps once");
    assert_eq!(again.invitation().await.id, invited.id);
    assert_eq!(again.ends_sent(&run, Duration::from_millis(500)).await, (0, 0), "each sent once");
    eventually("the run is unlisted again", async || !listed(&again.js).await.contains(&run)).await;
}
