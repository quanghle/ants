//! What a run stores before it acts: an answer's journal entry before the reply is taken, for the inbox to ack it, and
//! a step before it goes out, sent again under the same message id after a restart.

use ants_proto::trail_call_key;

use super::*;

/// Set the largest message the runs bucket takes: -1 for no limit beyond the server's.
async fn limit_runs(js: &async_nats::jetstream::Context, max: i32) {
    let stream = js.get_stream(format!("KV_{TRAIL_RUNS_BUCKET}")).await.unwrap();
    let mut config = stream.cached_info().config.clone();
    config.max_message_size = max;
    js.update_stream(config).await.unwrap();
}

#[tokio::test]
async fn a_reply_whose_journal_entry_is_not_stored_is_not_taken_and_is_taken_when_delivered_again() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(r#"await agent("go", ant="dev")"#, TrailLimits::default()).await;
    let step = lead.step().await;
    eventually("the step is stored sent", async || lead.session(&run).await.0.pending.iter().all(|call| call.sent))
        .await;
    let call = step.trail_step.as_ref().unwrap().call;
    let text = "y".repeat(2000);
    let reply = step.reply(ant("dev"), &text, Role::Ant).unwrap();

    // The journal entry, holding the reply's 2000 bytes, is past what the bucket now takes.
    limit_runs(&lead.js, 1000).await;
    assert!(
        lead.trails.take(&reply).await.is_err(),
        "a reply whose entry is not stored is left for the inbox to redeliver"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!lead.trails.status(&run).await.unwrap().header.finished(), "the run did not take the answer");
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    assert!(runs.get(trail_call_key(&ant("lead"), &run, call)).await.unwrap().is_none());
    limit_runs(&lead.js, -1).await;

    assert!(lead.trails.take(&reply).await.unwrap(), "the reply delivered again is taken");
    let entry = runs.get(trail_call_key(&ant("lead"), &run, call)).await.unwrap();
    assert!(entry.is_some(), "the journal entry is stored once the reply is taken");
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(text) });
}

#[tokio::test]
async fn a_step_stored_unsent_goes_out_after_a_restart_under_its_message_id() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let (run, b) = second_step_stored(&mut lead, TWO_STEPS).await;
    // As a daemon stopped between storing the step and marking it sent, which may have sent it or not.
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    let mut stored = lead.session(&run).await.0;
    stored.pending.iter_mut().for_each(|call| call.sent = false);
    runs.put(trail_session_key(&ant("lead"), &run), serde_json::to_vec(&stored).unwrap()).await.unwrap();
    drop(lead);

    let mut again = conductor(&server, &ants_binary(), Vec::new()).await;
    let resent = again.step().await;
    assert_eq!((resent.id, &resent.thread_id), (b.id, &b.thread_id), "the same message, which the bus keeps once");
    assert_eq!((&resent.to, &resent.text), (&b.to, &b.text));
    assert!(again.sends_no_step(Duration::from_millis(500)).await, "sent once");
    eventually("the step is stored sent", async || again.session(&run).await.0.pending.iter().all(|call| call.sent))
        .await;
    assert!(again.reply(&b, "B").await);
    assert_eq!(again.ended(&run).await, RunState::Done { result: json!("AB") });
    assert_eq!(again.header(&run).await.steps, 2, "the step sent again counts once");
}
