//! A hub whose JetStream file store is full: replies and dead letters it refuses wait in hand, spending no attempt
//! and prompting no harness again, until it has room.

use super::*;

/// The hub's `max_file_store`.
const STORE: u64 = 16 << 20;
/// The stream that fills the store, besides the ants' own.
const FILLER: &str = "FILLER";

/// dev-a's side of a hub whose file store holds [`STORE`] bytes.
async fn start_small() -> Option<(Bus, async_nats::Subscriber)> {
    let server = Server::spawn_with(|dir| {
        format!("jetstream {{ store_dir: {:?}, max_file_store: {STORE}, max_memory_store: 8MB }}\n", dir.join("js"))
    })?;
    let url = server.url.clone();
    Some(Bus::connect(server, &url, None).await)
}

/// Fill the hub's file store until JetStream refuses a write for want of room.
async fn fill(js: &jetstream::Context) {
    let config = jetstream::stream::Config {
        name: FILLER.to_owned(),
        subjects: vec!["filler.>".to_owned()],
        ..Default::default()
    };
    js.create_stream(config).await.unwrap();
    for size in [100_000, 1_000, 50] {
        let payload: Vec<u8> = vec![b'x'; size];
        loop {
            let stored = match js.publish("filler.x", payload.clone().into()).await {
                Ok(ack) => ack.await.map(drop),
                Err(e) => Err(e),
            };
            if let Err(e) = stored {
                assert!(e.to_string().contains("insufficient resources"), "{e}");
                break;
            }
        }
    }
}

/// Free the hub's file store [`fill`] filled.
async fn free(js: &jetstream::Context) {
    js.delete_stream(FILLER).await.unwrap();
}

/// A reply the full store refuses waits in hand, the thread's next message unprompted meanwhile, and is stored once
/// the store has room, without prompting the harness again or spending a delivery attempt.
#[tokio::test]
async fn a_reply_refused_by_a_full_store_is_sent_once_there_is_room_unprompted_again() {
    let Some((bus, _)) = start_small().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let (held, seq) = bus.send(&thread, "hold a").await;
    bus.send(&thread, "b").await;
    // Core subscribers hear each refused publish too; what counts is what the stream stores.
    let stored = async || {
        let (subject, mut from, mut texts) = (subject::inbox(&UserId::human(), &bus.dev), 1, Vec::new());
        while let Ok(msg) = bus.stream.get_first_raw_message_by_subject(&subject, from).await {
            from = msg.sequence + 1;
            let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
            assert_eq!(reply.thread_id, thread);
            texts.push(reply.text.split_once(": ").expect("fake reply").1.to_owned());
        }
        texts
    };

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().prompted == 1).await;
        fill(&bus.js).await;
        gate.add_permits(1);
        until("the answer", || log.locked().answered.len() == 1).await;
        // Several retries of the store pass.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(stored().await.is_empty(), "no reply stored while full");
        assert_eq!(log.locked().prompted, 1, "neither prompted again nor the next message");
        let marker = bus.attempts.get(seq, &held).await.unwrap();
        assert!(matches!(marker, Some(crate::delivery::attempts::Marker::Started)), "no reply could be recorded");
        assert!(bus.answered.unsent(&held).await.is_some(), "the reply is kept on the machine");
        let consumer = bus.consumer().await;
        assert_eq!((consumer.num_redelivered, consumer.num_ack_pending), (0, 2), "both in hand, none redelivered");

        free(&bus.js).await;
        bus.until_acked(seq + 1).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(stored().await, ["hold a", "b"], "each answered once");
    assert_eq!(log.locked().answered, ["hold a", "b"], "each prompted once");
    assert_eq!(log.locked().prompted, 2);
    assert_eq!(bus.consumer().await.num_redelivered, 0, "no attempt spent");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert!(bus.answered.unsent(&held).await.is_none(), "forgotten once sent");
    assert_eq!(bus.unacked().await, 0);
}

/// A message whose last attempt fails while the store is full keeps its dead letter on the machine and waits in
/// hand; once the store has room the dead letter is stored, its owner told and the message terminated, the harness
/// not prompted again.
#[tokio::test]
async fn a_dead_letter_refused_by_a_full_store_is_buried_once_there_is_room() {
    let Some((bus, mut replies)) = start_small().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let (doomed, seq) = bus.send(&thread, "doomed a").await;
    let attempts = MAX_ATTEMPTS as usize;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        gate.add_permits(attempts - 1);
        until("the last attempt", || log.locked().doomed == attempts).await;
        fill(&bus.js).await;
        gate.add_permits(1);
        until("the last attempt to fail", || log.locked().doomed_failed == attempts).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let kept = bus.buried.get(&doomed).await.expect("the dead letter is kept on the machine");
        assert!(kept.dead.error.contains("doomed"), "{}", kept.dead.error);
        assert!(dead::list(&bus.stream).await.unwrap().is_empty(), "not stored while full");
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "nobody told yet");
        assert_eq!(bus.consumer().await.num_ack_pending, 1, "the message waits in hand");

        free(&bus.js).await;
        let notice = next_envelope(&mut replies).await;
        assert_eq!((&notice.thread_id, notice.in_reply_to), (&thread, Some(doomed.id)));
        assert!(notice.text.contains("ants dead replay"), "{}", notice.text);
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().doomed, attempts, "not prompted again");
    let letters = dead::list(&bus.stream).await.unwrap();
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(letters[0].1.stream_seq, seq);
    assert!(letters[0].1.error.contains("doomed"), "{}", letters[0].1.error);
    assert!(bus.buried.get(&doomed).await.is_none(), "forgotten once terminated");
    assert_eq!(bus.unacked().await, 0);
}

/// The texts stored in the hub's stream on `subject`, oldest first: core subscribers hear refused publishes too.
async fn stored_texts(bus: &Bus, subject: &str) -> Vec<String> {
    let (mut from, mut texts) = (1, Vec::new());
    while let Ok(msg) = bus.stream.get_first_raw_message_by_subject(subject, from).await {
        from = msg.sequence + 1;
        texts.push(serde_json::from_slice::<Envelope>(&msg.payload).unwrap().text);
    }
    texts
}

/// An answer whose escalation the full store refuses is kept on the machine: once the store has room, the retry
/// escalates and replies with it, the harness not prompted again.
#[tokio::test]
async fn an_answer_whose_escalation_a_full_store_refuses_is_sent_once_there_is_room_unprompted_again() {
    let Some((mut bus, _)) = start_small().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let thread = ThreadId::generate();
    let (held, seq) = bus.send_from(dev_b.clone(), &thread, "hold cost 2 stall").await;
    let (to_dev_b, to_human) = (subject::inbox(&dev_b, &bus.dev), subject::inbox(&UserId::human(), &bus.dev));
    let cap = "Thread cost cap reached (2.00 of 1.00 since a human's last message)";

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().prompted == 1).await;
        fill(&bus.js).await;
        gate.add_permits(1);
        until("the prompt to be cancelled", || log.locked().cancelled == 1).await;
        // Several retries of the store pass.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(stored_texts(&bus, &to_human).await.is_empty(), "no escalation stored while full");
        assert!(stored_texts(&bus, &to_dev_b).await.is_empty(), "no reply stored while full");
        let given = bus.answered.given(&held).await.expect("the answer is kept on the machine");
        assert_eq!(given.limit.as_deref(), Some(cap));
        assert_eq!(bus.consumer().await.num_redelivered, 0, "in hand, not redelivered");

        free(&bus.js).await;
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 1, "prompted once");
    assert_eq!(stored_texts(&bus, &to_dev_b).await, [format!("partial\n\n[ants] {cap}; the prompt was cancelled.")]);
    let escalations = stored_texts(&bus, &to_human).await;
    assert_eq!(escalations, [format!("{cap}; needs a decision. Last message from dev-b:\nhold cost 2 stall")]);
    assert!(bus.answered.given(&held).await.is_none(), "forgotten once the reply is recorded");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

/// A reply past the server's `max_payload` is cut to fit, with a line telling so, and sent once.
#[tokio::test]
async fn a_reply_past_max_payload_is_cut_to_fit_and_sent_once() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let max = bus::max_payload(&bus.client);
    // The envelope fits, the fake's reply, adding its session's name and the prompt's framing, does not.
    let text = "y".repeat(max - 4096);
    let (_, seq) = bus.send(&thread, &text).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut replies).await;
        assert_eq!(reply.thread_id, thread);
        assert!(
            reply.text.contains("[ants] This answer was cut to fit the bus's max_payload"),
            "{}",
            &reply.text[..100]
        );
        assert!(serde_json::to_vec(&reply).unwrap().len() <= max);
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 1, "prompted once");
    assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "one reply");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

/// The dead letter of a message near `max_payload` fits: stored, its envelope's text cut, and its owner told.
#[tokio::test]
async fn the_dead_letter_of_a_message_near_max_payload_is_stored_and_told() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let max = bus::max_payload(&bus.client);
    let text = format!("doomed {}", "z".repeat(max - 1024));
    let (doomed, seq) = bus.send(&thread, &text).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    gate.add_permits(MAX_ATTEMPTS as usize);
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert_eq!((&notice.thread_id, notice.in_reply_to), (&thread, Some(doomed.id)));
        assert!(notice.text.contains("ants dead replay"), "{}", &notice.text[..notice.text.len().min(400)]);
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().doomed, MAX_ATTEMPTS as usize);
    let letters = dead::list(&bus.stream).await.unwrap();
    assert_eq!(letters.len(), 1, "{letters:?}");
    let letter = &letters[0].1;
    assert_eq!(letter.stream_seq, seq);
    let raw = bus.stream.get_raw_message(letters[0].0).await.unwrap();
    assert!(raw.payload.len() <= max, "{} bytes", raw.payload.len());
    let envelope = letter.envelope.as_ref().expect("the envelope is kept, ready to replay");
    assert!(
        envelope.text.contains("[ants] Cut to fit the dead letter"),
        "{}",
        &envelope.text[envelope.text.len() - 200..]
    );
}
