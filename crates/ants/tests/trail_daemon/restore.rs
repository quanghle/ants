//! Runs continued from their records: by a restarted conductor, at its start, or later for the runs it could not
//! continue then, tried again while it lives and on demand, for a reply to one of their steps or a stop; past a
//! failing or crashing worker; and replayed from their journal when their session does not restore.

use ants_proto::trail_runs_index_key;

use super::*;

/// The runs bucket, as the conductor `lead` reads and writes its keys.
async fn records(js: &async_nats::jetstream::Context) -> DirectKv {
    DirectKv::new(js, None, TRAIL_RUNS_BUCKET).await.unwrap()
}

impl Conductor {
    /// Wait until the stored session of `run` lists its steps as sent, `count` of them.
    async fn sent_steps(&self, run: &RunId, count: usize) {
        let runs = records(&self.js).await;
        let key = trail_session_key(&ant("lead"), run);
        eventually("the steps are stored as sent", async || {
            let Some(bytes) = runs.get(&key).await.unwrap() else { return false };
            let session: TrailSession = serde_json::from_slice(&bytes).unwrap();
            session.pending.len() == count && session.pending.iter().all(|call| call.sent)
        })
        .await;
    }

    fn holds(&self, run: &RunId) -> bool {
        self.trails.unfinished().iter().any(|header| header.run == *run)
    }
}

/// Replace the value of `key` with bytes that parse as no record, returning the value it held.
async fn spoil(runs: &DirectKv, key: &str) -> Vec<u8> {
    let kept = runs.get(key).await.unwrap().expect("a record");
    runs.put(key, b"not a record".to_vec()).await.unwrap();
    kept
}

#[tokio::test]
async fn a_conductor_that_could_not_read_its_index_at_start_continues_its_runs_once_it_can() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(r#"await agent("a", ant="dev")"#, TrailLimits::default()).await;
    let step = lead.step().await;
    lead.sent_steps(&run, 1).await;
    let runs = records(&lead.js).await;
    drop(lead);
    let index = trail_runs_index_key(&ant("lead"));
    let kept = spoil(&runs, &index).await;

    let again = conductor(&server, &ants_binary(), Vec::new()).await;
    assert!(!again.holds(&run));
    let reply = step.reply(step.to.clone(), "A", Role::Ant).unwrap();
    let e = again.trails.take(&reply).await.expect_err("the reply waits for its run, not acked as a late one");
    assert!(format!("{e:#}").contains("reading the trail runs to continue"), "{e:#}");

    runs.put(&index, kept).await.unwrap();
    eventually_within(Duration::from_secs(10), "the run is continued", async || again.holds(&run)).await;
    assert!(again.trails.take(&reply).await.unwrap(), "the reply delivered again answers the step");
    assert_eq!(again.ended(&run).await, RunState::Done { result: json!("A") });
}

#[tokio::test]
async fn a_run_whose_records_could_not_be_read_at_start_takes_a_reply_or_a_stop_once_they_can() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let mut started = Vec::new();
    for text in ["a", "b", "c"] {
        let run = lead.start(&format!(r#"await agent("{text}", ant="dev")"#), TrailLimits::default()).await;
        let step = lead.step().await;
        lead.sent_steps(&run, 1).await;
        started.push((run, step));
    }
    let runs = records(&lead.js).await;
    drop(lead);
    let [(answered, a), (fine, b), (stopped, c)] = <[_; 3]>::try_from(started).unwrap();
    let mut kept = Vec::new();
    for run in [&answered, &stopped] {
        let key = trail_session_key(&ant("lead"), run);
        kept.push((key.clone(), spoil(&runs, &key).await));
    }

    let again = conductor(&server, &ants_binary(), Vec::new()).await;
    assert!(again.holds(&fine) && !again.holds(&answered) && !again.holds(&stopped));
    let reply = a.reply(a.to.clone(), "A", Role::Ant).unwrap();
    let e = again.trails.take(&reply).await.expect_err("the reply waits for its run, not acked as a late one");
    assert!(format!("{e:#}").contains(&format!("continuing run {answered}")), "{e:#}");
    let human = ants_test_support::human();
    let e = again.trails.stop(&stopped, &human).await.expect_err("no stop while the run's records are unread");
    assert!(e.contains(&format!("continuing run {stopped}")), "{e}");

    for (key, value) in kept {
        runs.put(&key, value).await.unwrap();
    }
    assert!(again.trails.take(&reply).await.unwrap(), "the reply delivered again answers the step");
    assert_eq!(again.ended(&answered).await, RunState::Done { result: json!("A") });
    let waiting = again.trails.stop(&stopped, &human).await.unwrap();
    assert_eq!(waiting, [WaitingStep { to: c.to.clone(), message: c.id }], "the stop lists the step to withdraw");
    assert_eq!(again.header(&stopped).await.state, RunState::Stopped { by: human });
    assert!(again.reply(&b, "B").await);
    assert_eq!(again.ended(&fine).await, RunState::Done { result: json!("B") });
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
async fn a_worker_crashing_again_and_again_fails_the_run_and_tells_its_owner() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-crashing-worker");
    let worker = dir.join("worker");
    let tries = dir.join("tries");
    let wrapper = format!("#!/bin/sh\necho x >>{tries:?}\necho 'stack overflow' >&2\nkill -ABRT $$\n");
    std::fs::write(&worker, wrapper).unwrap();
    std::process::Command::new("chmod").arg("+x").arg(&worker).status().unwrap();

    let mut lead = conductor(&server, &worker, Vec::new()).await;
    let run = lead.start(r#"await agent("go", ant="dev")"#, TrailLimits::default()).await;
    let RunState::Failed { error, limit: None } = lead.ended(&run).await else { panic!("the run fails") };
    assert!(error.contains("crashed") && error.contains("stack overflow"), "{error}");
    assert!(error.ends_with("(3 times in a row)"), "{error}");
    assert_eq!(std::fs::read_to_string(&tries).unwrap().lines().count(), 3, "tried three times");
    let told = lead.told().await;
    assert!(
        told.text.contains(" failed (0 steps, 0 turns): ") && told.text.contains("stack overflow"),
        "{}",
        told.text
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

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
    let refused = again.trails.resume(&run, &ants_test_support::human(), None).await.unwrap_err();
    assert!(refused.contains("failed, not on a limit"), "{refused}");
}

#[tokio::test]
async fn a_fired_run_stored_but_not_listed_is_continued_by_its_firing_delivered_again() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-fired-unlisted");
    std::fs::write(dir.join("ask.py"), format!("{META}await agent(\"a\", ant=\"dev\")\n")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let start = TrailStart { trail: "ask".parse().unwrap(), args: Value::Null, limits: TrailLimits::DEFAULT };
    let fire = || {
        Envelope::new(ant("nightly"), ant("lead"), "Scheduled run of trail.")
            .with_owner(Some(ants_test_support::human()))
            .from_webhook()
            .starting(Some(start.clone()))
    };
    let firing = fire();
    let run = lead.trails.start_fired(&firing, &start).await.unwrap();
    let step = lead.step().await;
    eventually("the step is stored sent", async || {
        lead.session(&run).await.0.pending.iter().any(|call| call.message == step.id && call.sent)
    })
    .await;
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    drop(lead);
    // As a daemon stopped between storing the run's header and listing it leaves the index.
    let index = serde_json::to_vec(&TrailRunIndex { runs: Vec::new() }).unwrap();
    runs.put(trail_runs_index_key(&ant("lead")), index).await.unwrap();

    let mut again = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    assert!(again.trails.unfinished().is_empty(), "the index lists no run");
    assert_eq!(again.trails.start_fired(&firing, &start).await.unwrap(), run, "delivered again");
    assert_eq!(again.trails.unfinished().len(), 1, "the stored run is continued");
    assert!(again.sends_no_step(Duration::from_secs(2)).await, "its step is not sent again");
    let listed = runs.get(trail_runs_index_key(&ant("lead"))).await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<TrailRunIndex>(&listed).unwrap().runs, vec![run.clone()]);
    assert!(again.reply(&step, "A").await);
    assert_eq!(again.ended(&run).await, RunState::Done { result: json!("A") });

    // Listed with no header stored: the restarted daemon drops it, and its firing delivered again starts it anew.
    let lost = fire();
    let lost_run: RunId = lost.id.to_string().parse().unwrap();
    let index = serde_json::to_vec(&TrailRunIndex { runs: vec![lost_run.clone()] }).unwrap();
    runs.put(trail_runs_index_key(&ant("lead")), index).await.unwrap();
    drop(again);
    let mut third = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let listed = runs.get(trail_runs_index_key(&ant("lead"))).await.unwrap().unwrap();
    assert!(serde_json::from_slice::<TrailRunIndex>(&listed).unwrap().runs.is_empty(), "dropped from the index");
    assert_eq!(third.trails.start_fired(&lost, &start).await.unwrap(), lost_run);
    let step = third.step().await;
    assert!(third.reply(&step, "B").await);
    assert_eq!(third.ended(&lost_run).await, RunState::Done { result: json!("B") });
    std::fs::remove_dir_all(&dir).unwrap();
}
