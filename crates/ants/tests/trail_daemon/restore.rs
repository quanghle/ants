//! Runs a restarted conductor could not continue from their records at its start: tried again while it lives, and
//! on demand, for a reply to one of their steps or a stop.

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
    let human = UserId::human();
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
