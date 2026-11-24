//! The inbox loop: threads running concurrently and each in order, the pause, progress acks, steers, a stop and the
//! queue's places.

use super::*;

#[tokio::test]
async fn threads_run_concurrently_and_each_stays_in_order() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "hold a").await;
    bus.send_ant(&t1, "b").await;
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

/// A thread whose prompt runs with more messages waiting than the window holds leaves room for the other threads: the
/// window widens by its messages past the first two, so another thread's message starts on a free harness.
#[tokio::test]
async fn one_threads_backlog_does_not_keep_another_threads_message_in_the_stream() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "hold a").await;
    let backlog: Vec<String> = (1..=2 * IN_HAND).map(|n| format!("b{n}")).collect();
    for text in &backlog {
        bus.send_ant(&t1, text).await;
    }
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t2.clone(), "c".to_owned()), "c does not wait for t1's backlog");
        assert_eq!(log.locked().answered, ["c"], "t1's prompt still runs");
        gate.add_permits(1);
        let mut t1_order = vec![next_reply(&mut replies).await.1];
        for _ in &backlog {
            t1_order.push(next_reply(&mut replies).await.1);
        }
        let expected: Vec<_> = std::iter::once("hold a".to_owned()).chain(backlog.iter().cloned()).collect();
        assert_eq!(t1_order, expected, "t1 stays in order");
        eventually_every(Duration::from_millis(20), WAIT, "the window to narrow back", async || {
            bus.consumer().await.config.max_ack_pending == IN_HAND
        })
        .await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_paused_ant_keeps_its_messages_queued_and_takes_no_prompt_until_resumed() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
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
        bus.send_ant(&t1, "c").await;
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
        // Its ack comes after its reply.
        eventually_every(Duration::from_millis(20), WAIT, "b is not acked", async || {
            bus.consumer().await.num_ack_pending == 1
        })
        .await;
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
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, _, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut statuses = bus.client.subscribe(subject::status(&bus.dev, &t1)).await.unwrap();
    bus.client.flush().await.unwrap();
    bus.send(&t1, "hold a").await;
    let (b, _) = bus.send_ant(&t1, "b").await;
    let (c, _) = bus.send_ant(&t1, "c").await;

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
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send_ant(&t1, "b").await;

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
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));
    // A consumer left by a daemon configured with a longer ack wait takes the new one.
    drop(bus::inbox_consumer(&bus.stream, &bus.dev, ack_wait * 100, MAX_DELIVER, 4).await.unwrap());
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send_ant(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        assert_eq!(bus.consumer().await.config.ack_wait, ack_wait);
        timeout(WAIT, async {
            while bus.consumer().await.num_ack_pending < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("both messages in hand");
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
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    bus.send_ant(&t1, "b").await;

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
async fn a_human_reply_automatically_steers_a_running_prompt_ahead_of_the_queue_in_the_same_session() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;
    let (queued, _) = bus.send_ant(&t1, "queued").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let (sent, _) = bus.send(&t1, "go left").await;
        let reply = next_envelope(&mut replies).await;
        assert_eq!(reply.text, "partial\n\n[ants] human interrupted the prompt to steer dev-a with a new message.");
        let steered = next_envelope(&mut replies).await;
        let note = "[ants] human interrupted the previous prompt on this thread with this message; tool calls it had \
                    in flight were cancelled, so check what they left before repeating any.";
        assert_eq!(steered.text, format!("s1: {note}\n\ngo left"), "the same session takes the steer first");
        assert_eq!(steered.in_reply_to, Some(sent.id));
        let reply = next_envelope(&mut replies).await;
        assert_eq!((reply.in_reply_to, reply.text.as_str()), (Some(queued.id), "s1: queued"), "the queue waits");
        assert!(timeout(Duration::from_millis(300), replies.next()).await.is_err(), "no escalation");
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

/// A human's message whose reply is stored, its ack lost, comes again while another prompt runs on its thread, as
/// after the hub was out of reach past the ack wait: it is acked on its turn, and the prompt goes on.
#[tokio::test]
async fn a_message_answered_before_interrupts_no_prompt_when_it_comes_again() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_ant(&t1, "hold b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let answered = Envelope::on_thread(t1.clone(), ants_test_support::human(), bus.dev.clone(), "a");
        let reply = answered.reply(bus.dev.clone(), "s1: a", Role::Human).unwrap();
        bus.marks.answered.add(&answered, &Reply::Message(Box::new(reply))).await.unwrap();
        let seq = bus::publish_json(&bus.js, answered.subject(), &answered, None).await.unwrap();
        // Behind the prompt, it is acked only after it.
        timeout(WAIT, async {
            while bus.consumer().await.num_ack_pending < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the message never came in hand");
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold b".to_owned()), "the prompt ran to its end");
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    let log = log.locked();
    assert_eq!((log.cancelled, log.prompted), (0, 1), "neither interrupted nor prompted again");
}

#[tokio::test]
async fn messages_join_a_prompt_whose_harness_takes_steers_and_the_last_gets_the_answer() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    log.locked().knobs.steers = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, _) = bus.send(&t1, "hold a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let (b, _) = bus.send(&t1, "b").await;
        until("b to join", || log.locked().steered.len() == 1).await;
        let (c, _) = bus.send(&t1, "c").await;
        until("c to join", || log.locked().steered.len() == 2).await;
        gate.add_permits(1);
        let mut got =
            [next_envelope(&mut replies).await, next_envelope(&mut replies).await, next_envelope(&mut replies).await];
        got.sort_by_key(|reply| [a.id, b.id, c.id].iter().position(|id| reply.in_reply_to == Some(*id)));
        let notice = |by: Uuid| {
            format!(
                "[ants] human steered the prompt answering this message with message {by}; the answer goes in reply to it."
            )
        };
        assert_eq!((got[0].in_reply_to, got[0].text.clone()), (Some(a.id), notice(b.id)));
        assert_eq!(got[0].notice, Some(ants_proto::NoticeKind::Steered));
        assert_eq!((got[1].in_reply_to, got[1].text.clone()), (Some(b.id), notice(c.id)));
        let joined = "[ants] human sent this message while the prompt ran, to steer it. The final answer goes in reply \
                      to this message, so it answers this message together with what came before it.";
        assert_eq!(got[2].in_reply_to, Some(c.id));
        assert_eq!(got[2].text, format!("s1: hold a\n+ {joined}\n\nb\n+ {joined}\n\nc"));
        assert_eq!(got[2].notice, None);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.prompted, log.cancelled), (1, 0), "one prompt took all three");
    }
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_steer_the_harness_refuses_waits_for_the_prompt_and_runs_next() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    log.locked().knobs.steers = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, _) = bus.send(&t1, "hold a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let (b, _) = bus.send(&t1, "refused").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        gate.add_permits(1);
        let first = next_envelope(&mut replies).await;
        assert_eq!((first.in_reply_to, first.text), (Some(a.id), "s1: hold a".to_owned()));
        let next = next_envelope(&mut replies).await;
        assert_eq!((next.in_reply_to, next.text), (Some(b.id), "s1: refused".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.prompted, log.cancelled, log.steered.len()), (2, 0, 0));
    }
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_message_with_no_prompt_running_or_an_ants_reply_waits_its_turn() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let t1 = ThreadId::generate();
    bus.send(&t1, "idle").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "idle".to_owned()), "an ordinary prompt");
        bus.send(&t1, "stall").await;
        until("the prompt to run", || log.locked().prompted == 2).await;
        let reply = Envelope {
            in_reply_to: Some(Uuid::new_v4()),
            ..Envelope::on_thread(t1.clone(), dev_b.clone(), bus.dev.clone(), "an answer")
        };
        bus::publish_json(&bus.js, reply.subject(), &reply, None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(log.locked().cancelled, 0, "an ant's reply does not steer");
        assert!(cancel(&bus, &t1).await);
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.text, "partial\n\n[ants] The prompt was cancelled by human (ants cancel).");
        until("the reply to be prompted", || log.locked().answered.iter().any(|text| text.ends_with("an answer")))
            .await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn an_ants_new_message_steers_a_running_prompt_as_a_humans_does() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let (sent, _) = bus.send_from(dev_b.clone(), &t1, "go right").await;
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(reply.text, "partial\n\n[ants] dev-b interrupted the prompt to steer dev-a with a new message.");
        let steered = next_envelope(&mut to_dev_b).await;
        assert_eq!(steered.in_reply_to, Some(sent.id));
        assert!(steered.text.starts_with("s1: [ants] dev-b interrupted the previous prompt"), "{}", steered.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().cancelled, 1);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_human_reply_marked_queue_waits_behind_the_running_prompt() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let mut statuses = bus.client.subscribe(subject::status(&bus.dev, &t1)).await.unwrap();
    bus.client.flush().await.unwrap();
    bus.send(&t1, "hold a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let later = Envelope {
            queue: true,
            ..Envelope::on_thread(t1.clone(), ants_test_support::human(), bus.dev.clone(), "b")
        };
        bus::publish_json(&bus.js, later.subject(), &later, None).await.unwrap();
        let ahead = timeout(WAIT, async {
            loop {
                let msg = statuses.next().await.unwrap();
                let event: StatusEvent = serde_json::from_slice(&msg.payload).unwrap();
                if let StatusUpdate::Queued { message, ahead } = event.update {
                    break (message, ahead);
                }
            }
        })
        .await
        .expect("no queued status");
        assert_eq!(ahead, (later.id, 1), "it waits behind the prompt running");
        assert_eq!(log.locked().cancelled, 0, "a queued reply does not interrupt");
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()), "the prompt finishes");
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_channel_post_a_steer_took_the_answer_from_gets_no_notice_post() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    log.locked().knobs.steers = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["human", "dev-a"]).await;
    let root = ChannelPost::new(ops.clone(), ants_test_support::human(), "@dev-a a\nhold: a");
    bus.post(&root).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the channel prompt to run", || log.locked().prompted == 1).await;
        let sent = ChannelPost::on_thread(ops, root.thread_id.clone(), ants_test_support::human(), "@dev-a b");
        let seq = bus.post(&sent).await;
        until("the post to join", || log.locked().steered.len() == 1).await;
        gate.add_permits(1);
        let answer = next_post(&mut replies).await;
        assert_eq!(answer.in_reply_to, Some(sent.id));
        assert!(answer.text.contains("\n+ [ants] human sent this message"), "{}", answer.text);
        assert!(answer.text.contains("hold: a"), "{}", answer.text);
        bus.until_posts_acked(seq).await;
        assert!(timeout(Duration::from_millis(300), replies.next()).await.is_err(), "no notice is posted");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 1);
}

#[tokio::test]
async fn a_human_channel_reply_automatically_steers_but_replies_of_ants_do_not() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["human", "dev-a", "dev-b"]).await;
    let root = ChannelPost::new(ops.clone(), "dev-b".parse().unwrap(), "@dev-a stall");
    bus.post(&root).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the channel prompt to run", || log.locked().prompted == 1).await;
        let queued = ChannelPost {
            in_reply_to: Some(root.id),
            ..ChannelPost::on_thread(ops.clone(), root.thread_id.clone(), "dev-b".parse().unwrap(), "@dev-a queued")
        };
        bus.post(&queued).await;
        let ignored =
            ChannelPost::on_thread(ops.clone(), root.thread_id.clone(), ants_test_support::human(), "not addressed");
        let seq = bus.post(&ignored).await;
        timeout(WAIT, async {
            loop {
                let info = bus.stream.consumer_info(ants_proto::chan_consumer(&bus.dev)).await.unwrap();
                if info.delivered.stream_sequence >= seq {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the other posts to arrive");
        assert_eq!(log.locked().cancelled, 0, "ant replies and unaddressed human posts do not interrupt");

        let sent = ChannelPost::on_thread(ops, root.thread_id.clone(), ants_test_support::human(), "@dev-a go left");
        let seq = bus.post(&sent).await;
        let partial = next_post(&mut replies).await;
        assert_eq!(partial.in_reply_to, Some(root.id));
        assert_eq!(partial.text, "partial\n\n[ants] human interrupted the prompt to steer dev-a with a new message.");
        let steered = next_post(&mut replies).await;
        assert_eq!(steered.in_reply_to, Some(sent.id), "the human reply goes ahead of the ant's queued post");
        assert!(steered.text.starts_with("s1: [ants] human interrupted the previous prompt"), "{}", steered.text);
        assert!(steered.text.contains("@dev-a go left"), "{}", steered.text);
        assert_eq!(next_post(&mut replies).await.in_reply_to, Some(queued.id), "the queued post still runs");
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let log = log.locked();
    assert_eq!((log.cancelled, log.prompted, log.new_sessions), (1, 3, 1));
    assert!(log.loaded.is_empty());
}
