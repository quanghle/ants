//! The inbox loop: threads running concurrently and each in order, the pause, progress acks, steers, a stop and the
//! queue's places.

use super::*;

#[tokio::test]
async fn threads_run_concurrently_and_each_stays_in_order() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t2.clone(), "c".to_owned()), "t2 does not wait for t1");
        assert_eq!(log.locked().answered, ["c"], "b waits for the message before it on t1");
        until("c to be done", || bus.workload.handling() == 1).await;
        assert_eq!(bus.workload.activity(), ants_proto::Activity::Working);
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        until("the inbox to go idle", || bus.workload.activity() == ants_proto::Activity::Idle).await;
        assert_eq!(bus.workload.harnesses(), 2, "a harness per thread stays live");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_paused_ant_keeps_its_messages_queued_and_takes_no_prompt_until_resumed() {
    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.pause = crate::pause::Pause::new(true);
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let resume = || bus.pause.replace(&crate::pause::Pause::new(false));
    let pause = || bus.pause.replace(&crate::pause::Pause::new(true));
    let check = async {
        until("the colony to show the pause", || bus.workload.paused().is_some()).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(log.locked().prompted, 0, "paused from the start");
        let name = ants_proto::inbox_consumer(&bus.dev);
        let consumer = bus.stream.get_consumer::<jetstream::consumer::pull::Config>(&name).await;
        assert!(consumer.is_err(), "nothing pulled while paused");

        resume();
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "a".to_owned()));
        assert_eq!(bus.workload.paused(), None);
        bus.send(&t1, "hold b").await;
        until("b to start", || log.locked().prompted == 2).await;
        bus.send(&t1, "c").await;
        timeout(WAIT, async {
            while bus.consumer().await.num_ack_pending < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("c never came in hand");
        pause();
        until("the colony to show the pause", || bus.workload.paused().is_some()).await;
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold b".to_owned()), "a running prompt finishes");
        bus.send(&t2, "d").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(log.locked().prompted, 2, "neither the message in hand nor a new one starts");
        let consumer = bus.consumer().await;
        assert_eq!((consumer.num_pending, consumer.num_ack_pending), (1, 1), "d queued, c in hand: {consumer:?}");

        resume();
        let mut rest = [next_reply(&mut replies).await, next_reply(&mut replies).await];
        rest.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(rest, [(t1.clone(), "c".to_owned()), (t2.clone(), "d".to_owned())]);
        assert_eq!(log.locked().starts, 2, "a harness per thread, none restarted");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn messages_waiting_on_their_thread_hear_where_they_stand() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, _, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut statuses = bus.client.subscribe(subject::status(&bus.dev, &t1)).await.unwrap();
    bus.client.flush().await.unwrap();
    bus.send(&t1, "hold a").await;
    let (b, _) = bus.send(&t1, "b").await;
    let (c, _) = bus.send(&t1, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut queued = Vec::new();
        while queued.len() < 5 {
            let msg = timeout(WAIT, statuses.next()).await.expect("no status").unwrap();
            let event: StatusEvent = serde_json::from_slice(&msg.payload).unwrap();
            if let StatusUpdate::Queued { message, ahead } = event.update {
                queued.push((message, ahead));
            }
            if queued.len() == 2 {
                gate.add_permits(1);
            }
        }
        // As they arrive, then as the queue moves: b's turn comes, c waits behind it, then c's turn comes.
        assert_eq!(queued, [(b.id, 1), (c.id, 2), (b.id, 0), (c.id, 1), (c.id, 0)]);
        for _ in 0..3 {
            next_reply(&mut replies).await;
        }
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn inbox_consumers_from_before_sender_subjects_get_the_new_filter() {
    let Some((bus, _)) = Bus::start().await else { return };
    let name = ants_proto::inbox_consumer(&bus.dev);
    let old = jetstream::consumer::pull::Config {
        durable_name: Some(name.clone()),
        filter_subject: format!("ants.inbox.{}", bus.dev),
        ..Default::default()
    };
    bus.stream.create_consumer(old).await.unwrap();
    drop(crate::bus::inbox_consumer(&bus.stream, &bus.dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap());
    let mut consumer: PullConsumer = bus.stream.get_consumer(&name).await.unwrap();
    assert_eq!(consumer.info().await.unwrap().config.filter_subject, subject::inbox_of(&bus.dev));
}

#[tokio::test]
async fn ctrl_c_finishes_prompts_in_flight_and_a_restart_resumes() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;

    let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let stop_mid_prompt = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        ctrl_c.add_permits(1);
        gate.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), stop_mid_prompt);
    pool.shutdown().await;
    assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
    assert_eq!(log.locked().answered, ["hold a"], "b was handed back, not prompted");

    let pool = Pool::new(launch, store, 2, IDLE);
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().loaded, ["s1"], "the restarted daemon resumed the thread's session");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn progress_acks_keep_messages_in_hand_past_the_ack_wait() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));
    // A consumer left by a daemon configured with a longer ack wait takes the new one.
    drop(bus::inbox_consumer(&bus.stream, &bus.dev, ack_wait * 100, MAX_DELIVER, 4).await.unwrap());
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        assert_eq!(bus.consumer().await.config.ack_wait, ack_wait);
        tokio::time::sleep(ack_wait * 4).await;
        let consumer = bus.consumer().await;
        assert_eq!((consumer.num_redelivered, consumer.num_ack_pending), (0, 2), "neither message came back");
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_redelivery_of_a_message_in_hand_is_not_prompted_again() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send(&t1, "b").await;

    // Progress acks lost, as on a silent link: both messages come back while still in hand.
    let ack_wait = Duration::from_millis(300);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        timeout(WAIT, async {
            while bus.consumer().await.num_redelivered < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no redelivery");
        gate.add_permits(10);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        assert!(timeout(ack_wait * 3, replies.next()).await.is_err(), "no duplicate reply");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, Duration::from_secs(3600), ack_wait), check);
    assert_eq!(log.locked().answered, ["hold a", "b"]);
    assert_eq!(bus.unacked().await, 0, "acks of the first delivery count");
}

#[tokio::test]
async fn a_human_steers_a_running_prompt_ahead_of_the_queue_in_the_same_session() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;
    bus.send(&t1, "queued").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        steer(&bus, UserId::human(), &t1, "go left").await;
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.text, "partial\n\n[ants] human interrupted the prompt to steer dev-a (ants ask --steer).");
        let steered = next_envelope(&mut to_human).await;
        let note = "[ants] human interrupted the previous prompt on this thread with this message; tool calls it had \
                    in flight were cancelled, so check what they left before repeating any.";
        assert_eq!(steered.text, format!("s1: {note}\n\ngo left"), "the same session takes the steer first");
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "queued".to_owned()), "the queue waits");
        assert!(timeout(Duration::from_millis(300), to_human.next()).await.is_err(), "no escalation");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.cancelled, log.prompted, log.new_sessions), (1, 3, 1), "not retried");
        assert!(log.loaded.is_empty(), "the session goes on without a load");
    }
    assert_eq!(bus.unacked().await, 0);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty(), "no dead letter");
}

#[tokio::test]
async fn a_steer_with_no_prompt_running_or_from_an_ant_waits_its_turn() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    steer(&bus, UserId::human(), &t1, "idle").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "idle".to_owned()), "an ordinary prompt");
        bus.send(&t1, "stall").await;
        until("the prompt to run", || log.locked().prompted == 2).await;
        steer(&bus, dev_b.clone(), &t1, "from an ant").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(log.locked().cancelled, 0, "an ant does not steer");
        assert!(cancel(&bus, &t1).await);
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.text, "partial\n\n[ants] The prompt was cancelled by human (ants cancel).");
        assert_eq!(next_reply(&mut to_dev_b).await, (t1.clone(), "from an ant".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

/// Send `text` on `thread` from `from` as a steer.
async fn steer(bus: &Bus, from: UserId, thread: &ThreadId, text: &str) {
    let envelope = Envelope::on_thread(thread.clone(), from, bus.dev.clone(), text).steering();
    bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
}
