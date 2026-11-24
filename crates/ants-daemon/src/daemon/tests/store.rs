//! A hub whose JetStream file store is full: replies, escalations and dead letters it refuses wait in hand, spending
//! no attempt and prompting no harness again, until it has room; and what is stored of a message near `max_payload`
//! fits.

use super::*;

/// The hub's `max_file_store`.
pub(super) const STORE: u64 = 16 << 20;
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
pub(super) async fn fill(js: &jetstream::Context) {
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
pub(super) async fn free(js: &jetstream::Context) {
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
        let (subject, mut from, mut texts) = (subject::inbox(&ants_test_support::human(), &bus.dev), 1, Vec::new());
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
        eventually_every(Duration::from_millis(20), WAIT, "the reply is kept on the machine", async || {
            bus.marks.answered.unsent(&held).await.is_some()
        })
        .await;
        // Several retries of the store pass.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(stored().await.is_empty(), "no reply stored while full");
        assert_eq!(log.locked().prompted, 1, "neither prompted again nor the next message");
        let marker = bus.marks.attempts.get(seq, &held).await.unwrap();
        assert!(matches!(marker, Some(crate::delivery::attempts::Marker::Started)), "no reply could be recorded");
        assert!(bus.marks.answered.unsent(&held).await.is_some(), "the reply is kept on the machine");
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
    assert!(bus.marks.answered.unsent(&held).await.is_none(), "forgotten once sent");
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
        let kept = bus.marks.buried.get(&doomed).await.expect("the dead letter is kept on the machine");
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
    assert!(bus.marks.buried.get(&doomed).await.is_none(), "forgotten once terminated");
    assert_eq!(bus.unacked().await, 0);
}

/// A message never to be answered met while the store is full waits in hand, spending no delivery, and is kept as a
/// dead letter once the store has room.
#[tokio::test]
async fn junk_met_while_the_store_is_full_is_a_dead_letter_once_there_is_room() {
    let Some((bus, _)) = start_small().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let junk = subject::inbox(&bus.dev, &ants_test_support::human());
    let seq = bus.js.publish(junk, "not an envelope".into()).await.unwrap().await.unwrap().sequence;
    fill(&bus.js).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let name = ants_proto::inbox_consumer(&bus.dev);
        let delivered = async {
            loop {
                if let Ok(mut consumer) = bus.stream.get_consumer::<jetstream::consumer::pull::Config>(&name).await
                    && consumer.info().await.unwrap().delivered.stream_sequence >= seq
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        timeout(WAIT, delivered).await.expect("the junk delivered");
        // Several retries of the store pass.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(dead::list(&bus.stream).await.unwrap().is_empty(), "not stored while full");
        let consumer = bus.consumer().await;
        assert_eq!((consumer.num_redelivered, consumer.num_ack_pending), (0, 1), "in hand, not redelivered");
        free(&bus.js).await;
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let letters = dead::list(&bus.stream).await.unwrap();
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(letters[0].1.stream_seq, seq);
    assert!(letters[0].1.error.contains("not an envelope"), "{}", letters[0].1.error);
    assert_eq!(log.locked().prompted, 0);
    assert_eq!(bus.consumer().await.num_redelivered, 0, "no delivery spent");
    assert_eq!(bus.unacked().await, 0);
}

/// The messages stored in the hub's stream on `subject`, oldest first: core subscribers hear refused publishes too.
async fn stored_on(bus: &Bus, subject: &str) -> Vec<Envelope> {
    let (mut from, mut stored) = (1, Vec::new());
    while let Ok(msg) = bus.stream.get_first_raw_message_by_subject(subject, from).await {
        from = msg.sequence + 1;
        stored.push(serde_json::from_slice(&msg.payload).unwrap());
    }
    stored
}

/// The texts of the messages stored in the hub's stream on `subject`, oldest first.
async fn stored_texts(bus: &Bus, subject: &str) -> Vec<String> {
    stored_on(bus, subject).await.into_iter().map(|stored| stored.text).collect()
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
    let (to_dev_b, to_human) =
        (subject::inbox(&dev_b, &bus.dev), subject::inbox(&ants_test_support::human(), &bus.dev));
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
        let given = bus.marks.answered.given(&held).await.expect("the answer is kept on the machine");
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
    assert!(bus.marks.answered.given(&held).await.is_none(), "forgotten once the reply is recorded");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

/// A prompt a second message joined, cut at a cap while the store is full, is not run again: the notice to the message
/// it started with and the answer to the one that joined it, with the title the turn suggested, are kept on the
/// machine, and sent once the store has room, the cap escalated once.
#[tokio::test]
async fn a_joined_prompt_a_full_store_refuses_the_outcome_of_is_settled_once_there_is_room_unprompted_again() {
    let Some((mut bus, _)) = start_small().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    let home = bus._server.dir().join("joined");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    let (launch, log, gate) = fake::launch();
    log.locked().knobs.steers = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let thread = ThreadId::generate();
    let (first, _) = bus.send_from(dev_b.clone(), &thread, "hold cost 2 stall").await;
    let (to_dev_b, to_human) =
        (subject::inbox(&dev_b, &bus.dev), subject::inbox(&ants_test_support::human(), &bus.dev));
    let cap = "Thread cost cap reached (2.00 of 1.00 since a human's last message)";

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().prompted == 1).await;
        let (joining, seq) = bus.send_from(dev_b.clone(), &thread, "b").await;
        until("b to join", || log.locked().steered.len() == 1).await;
        let title = serde_json::json!({"title": "Fix the build"});
        let suggested = call_tool(bus.mcp.as_ref().unwrap(), &thread, "suggest_thread_title", title).await;
        assert_ne!(suggested.is_error, Some(true), "{suggested:?}");
        fill(&bus.js).await;
        gate.add_permits(1);
        until("the prompt to be cancelled", || log.locked().cancelled == 1).await;
        eventually_every(
            Duration::from_millis(20),
            WAIT,
            "the notice and the answer are kept on the machine",
            async || {
                bus.marks.answered.unsent(&first).await.is_some() && bus.marks.answered.given(&joining).await.is_some()
            },
        )
        .await;
        // Several retries of the store pass.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(stored_on(&bus, &to_human).await.is_empty(), "no escalation stored while full");
        assert!(stored_on(&bus, &to_dev_b).await.is_empty(), "neither notice nor reply stored while full");
        assert_eq!(log.locked().prompted, 1, "neither message prompted again");
        assert!(bus.marks.answered.unsent(&first).await.is_some(), "the notice is kept on the machine");
        let given = bus.marks.answered.given(&joining).await.expect("the answer is kept on the machine");
        assert_eq!((given.limit.as_deref(), given.title.as_deref()), (Some(cap), Some("Fix the build")));

        free(&bus.js).await;
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
        joining
    };
    let ((), joining) = tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 1, "prompted once");
    let mut sent = stored_on(&bus, &to_dev_b).await;
    sent.sort_by_key(|reply| reply.in_reply_to != Some(first.id));
    let sent: Vec<_> =
        sent.iter().map(|reply| (reply.in_reply_to, reply.text.as_str(), reply.title.as_deref())).collect();
    let notice = format!(
        "[ants] dev-b steered the prompt answering this message with message {}; the answer goes in reply to it.",
        joining.id
    );
    let answer = format!("partial\n\n[ants] {cap}; the prompt was cancelled.");
    assert_eq!(
        sent,
        [(Some(first.id), notice.as_str(), None), (Some(joining.id), answer.as_str(), Some("Fix the build"))]
    );
    let escalations = stored_texts(&bus, &to_human).await;
    assert_eq!(escalations, [format!("{cap}; needs a decision. Last message from dev-b:\nb")]);
    assert!(bus.marks.answered.unsent(&first).await.is_none(), "forgotten once sent");
    assert!(bus.marks.answered.given(&joining).await.is_none(), "forgotten once the reply is recorded");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

/// What becomes of the file a turn attached to its answer before the retry makes the reply from the kept answer.
#[derive(PartialEq)]
enum Attached {
    /// Stored by the first try, in an attachments' store with room, and deleted from the working directory since.
    Stored,
    /// Refused by the full store, and still in the working directory.
    Refused,
    /// Refused by the full store, and deleted from the working directory since.
    RefusedAndGone,
}

/// The reply to a message whose turn attached `report.md` with `attach_to_reply` and whose prompt was cut at a cap
/// the full store refused the escalation of: made, once the store has room, from the answer kept on the machine, the
/// harness prompted once.
async fn reply_from_a_kept_answer_with_a_file(attached: Attached) -> Option<(Bus, Envelope)> {
    use jetstream::stream::StorageType;

    let (mut bus, _) = start_small().await?;
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    // In memory, the attachments' store has room while the file store is full.
    let storage = if attached == Attached::Stored { StorageType::Memory } else { StorageType::File };
    bus.js.create_stream(jetstream::stream::Config { storage, ..crate::hub::files_config() }).await.unwrap();
    let home = bus._server.dir().join("kept-files");
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("report.md"), "# done").unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await.unwrap();
    mcp.with_files(crate::mcp::jetstream_files(bus.js.clone()), home.clone());
    bus.mcp = Some(mcp);
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let thread = ThreadId::generate();
    let (held, seq) = bus.send_from(dev_b.clone(), &thread, "hold cost 2 stall").await;
    let to_dev_b = subject::inbox(&dev_b, &bus.dev);

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to start", || log.locked().prompted == 1).await;
        let files = serde_json::json!({"attach": ["report.md"]});
        let attach = call_tool_in(bus.mcp.as_ref().unwrap(), &thread, &work, "attach_to_reply", files).await;
        assert_ne!(attach.is_error, Some(true), "{attach:?}");
        fill(&bus.js).await;
        gate.add_permits(1);
        until("the prompt to be cancelled", || log.locked().cancelled == 1).await;
        eventually_every(Duration::from_millis(20), WAIT, "the answer is kept on the machine", async || {
            bus.marks.answered.given(&held).await.is_some()
        })
        .await;
        // Several retries of the store pass.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(stored_on(&bus, &to_dev_b).await.is_empty(), "no reply stored while full");
        let given = bus.marks.answered.given(&held).await.expect("the answer is kept on the machine");
        let files = given.files.expect("kept with the file the turn attached");
        assert_eq!(files.paths, ["report.md"]);
        let stored = files.stored.map(|stored| stored.files.into_iter().map(|file| file.name).collect::<Vec<_>>());
        assert_eq!(stored, (attached == Attached::Stored).then(|| vec!["report.md".to_owned()]));
        if attached != Attached::Refused {
            std::fs::remove_file(work.join("report.md")).unwrap();
        }

        free(&bus.js).await;
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 1, "prompted once");
    let [reply] = &stored_on(&bus, &to_dev_b).await[..] else { panic!("one reply") };
    assert!(bus.marks.answered.given(&held).await.is_none(), "forgotten once the reply is recorded");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
    let reply = reply.clone();
    Some((bus, reply))
}

/// The text of an answer cut at the thread's cost cap.
const CUT_AT_CAP: &str = "partial\n\n[ants] Thread cost cap reached (2.00 of 1.00 since a human's last message); the \
                          prompt was cancelled.";

/// The file `reply` carries, read from the attachments' store.
async fn file_of(bus: &Bus, reply: &Envelope) -> Vec<u8> {
    let [report] = &reply.attachments[..] else { panic!("{reply:?}") };
    assert_eq!((report.name.as_str(), report.mime.as_str()), ("report.md", "text/markdown"));
    crate::files::get(&bus.js, &reply.attachment_handle(report.id), Some(report)).await.unwrap().1
}

/// A reply made from a kept answer carries the files its turn attached as the failed try stored them: they are
/// neither read from the working directory nor stored again.
#[tokio::test]
async fn a_reply_made_from_a_kept_answer_carries_the_files_its_first_try_stored() {
    let Some((bus, reply)) = Box::pin(reply_from_a_kept_answer_with_a_file(Attached::Stored)).await else { return };
    assert_eq!(reply.text, CUT_AT_CAP);
    assert_eq!(file_of(&bus, &reply).await, b"# done");
}

/// A reply made from a kept answer whose files the first try could not store reads and stores them then.
#[tokio::test]
async fn a_reply_made_from_a_kept_answer_stores_the_files_its_first_try_could_not() {
    let Some((bus, reply)) = Box::pin(reply_from_a_kept_answer_with_a_file(Attached::Refused)).await else { return };
    assert_eq!(reply.text, CUT_AT_CAP);
    assert_eq!(file_of(&bus, &reply).await, b"# done");
}

/// A reply made from a kept answer whose files were never stored and are gone since says they are left out.
#[tokio::test]
async fn a_reply_made_from_a_kept_answer_says_its_files_are_left_out_when_they_are_gone() {
    let Some((_bus, reply)) = Box::pin(reply_from_a_kept_answer_with_a_file(Attached::RefusedAndGone)).await else {
        return;
    };
    assert!(reply.attachments.is_empty(), "{reply:?}");
    let left_out = format!("{CUT_AT_CAP}\n\n[ants] The files attached to this answer are left out: cannot attach");
    assert!(reply.text.starts_with(&left_out), "{}", reply.text);
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

/// An escalation quoting a message near `max_payload` fits: stored, its text cut with a line telling so.
#[tokio::test]
async fn an_escalation_quoting_a_message_near_max_payload_is_cut_to_fit() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let max = bus::max_payload(&bus.client);
    let thread = ThreadId::generate();
    let text = format!("cost 2 stall {}", "z".repeat(max - 400));
    let (_, seq) = bus.send_from(dev_b.clone(), &thread, &text).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let escalation = next_envelope(&mut to_human).await;
        assert_eq!(escalation.thread_id, thread);
        assert!(escalation.text.starts_with("Thread cost cap reached"), "{}", &escalation.text[..100]);
        let end = &escalation.text[escalation.text.len() - 200..];
        assert!(end.contains("[ants] This notice was cut to fit the bus's max_payload"), "{end}");
        assert!(serde_json::to_vec(&escalation).unwrap().len() <= max);
        let reply = next_envelope(&mut to_dev_b).await;
        assert!(reply.text.starts_with("partial"), "{}", reply.text);
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 1, "prompted once");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}
