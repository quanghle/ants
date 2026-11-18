//! Cancels, restarts and clone snapshots asked of the running ant, and an assistant's cancel or withdrawal for its
//! human.

use super::*;

#[tokio::test]
async fn a_human_cancels_a_running_prompt_and_the_sender_gets_what_it_wrote() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(dev_b.clone(), &t1, "stall").await;
    bus.send_from(dev_b.clone(), &t1, "after").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert!(!cancel(&bus, &ThreadId::generate()).await, "no prompt runs on another thread");
        until("the prompt to run", || log.locked().prompted == 1).await;
        assert!(cancel(&bus, &t1).await);
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!(reply.text, "partial\n\n[ants] The prompt was cancelled by human (ants cancel).");
        assert_eq!(next_reply(&mut to_dev_b).await, (t1.clone(), "after".to_owned()), "the thread goes on");
        assert!(!cancel(&bus, &t1).await, "nothing left to cancel");
        assert!(timeout(Duration::from_millis(300), to_human.next()).await.is_err(), "no escalation");
        let ends: Vec<_> = ends_on(&bus, &t1, 2).await.into_iter().map(|(_, end)| end.how).collect();
        assert_eq!(ends, [PromptEnd::Cancelled, PromptEnd::Replied]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().cancelled, 1);
    assert_eq!(log.locked().prompted, 2, "not retried");
    assert_eq!(bus.unacked().await, 0);
}

/// A clone's snapshot of dev-a is read once dev-a is paused and the prompt it runs ended: each thread it has a session
/// of, with the thread's history, and how its harness starts.
#[tokio::test]
async fn a_clone_s_snapshot_waits_for_the_pause_and_the_prompt_running() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold one").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let dev = bus.dev.clone();
        let _served = bus.restarts.serve(&dev);
        until("the held prompt to run", || log.locked().prompted == 1).await;
        until("the daemon to hear clones", || bus.restarts.hears_clones(&dev)).await;
        let mut snapshot = std::pin::pin!(bus.restarts.snapshot(&dev, "dev-c".parse().unwrap()));
        assert!(timeout(Duration::from_millis(500), &mut snapshot).await.is_err(), "not before the pause");
        bus.pause.replace(&crate::pause::Pause::new(true));
        assert!(timeout(Duration::from_millis(500), &mut snapshot).await.is_err(), "not while the prompt runs");
        gate.add_permits(1);
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "hold one".to_owned()));
        let snapshot = timeout(WAIT, snapshot).await.expect("read once the prompt ended").expect("served").unwrap();
        assert_eq!(snapshot.threads.iter().map(|copy| &copy.thread).collect::<Vec<_>>(), [&t1]);
        assert!(snapshot.threads[0].history.contains("hold one"), "{}", snapshot.threads[0].history);
        assert_eq!(snapshot.threads[0].stored.session.to_string(), "s1");
        assert!(snapshot.launch.is_some());
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

/// Have the daemon restart dev-a's harnesses for alice once it serves the inbox.
async fn restart(bus: &Bus, now: bool) -> Restarted {
    let (dev, alice) = (bus.dev.clone(), "alice".parse::<UserId>().unwrap());
    let _served = bus.restarts.serve(&dev);
    timeout(WAIT, async {
        while !bus.restarts.is_listening(&dev) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bus.restarts.restart(&dev, alice, now).await.expect("dev-a is served")
    })
    .await
    .expect("the daemon heard no restart")
}

#[tokio::test]
async fn a_restart_stops_idle_harnesses_and_working_ones_after_their_prompt() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "one").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // One thread after the other, so t2's session is s2.
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "one".to_owned()));
        bus.send(&t2, "hold two").await;
        until("the held prompt to run", || log.locked().prompted == 2).await;
        until("the first message to be handled", || bus.workload.handling() == 1).await;
        assert_eq!(restart(&bus, false).await, Restarted { stopped: 1, after_prompt: 1, cancelled: 0 });
        gate.add_permits(1);
        assert_eq!(next_reply(&mut to_human).await, (t2.clone(), "hold two".to_owned()), "the working prompt ends");
        bus.send(&t2, "again").await;
        assert_eq!(next_reply(&mut to_human).await, (t2.clone(), "again".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.starts, log.cancelled), (3, 0), "a new harness, no prompt cancelled");
        assert_eq!(log.loaded, ["s2"], "it resumes the session");
    }
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_restart_now_cancels_the_running_prompt_and_the_sender_gets_what_it_wrote() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut to_human = bus.replies().await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "stall").await;
    bus.send_ant(&t1, "after").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        assert_eq!(restart(&bus, true).await, Restarted { stopped: 0, after_prompt: 1, cancelled: 1 });
        let reply = next_envelope(&mut to_human).await;
        let notice = "[ants] The prompt was cancelled by alice to restart dev-a's harnesses (ants ant restart --now).";
        assert_eq!(reply.text, format!("partial\n\n{notice}"));
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "after".to_owned()), "the thread goes on");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    {
        let log = log.locked();
        assert_eq!((log.starts, log.cancelled, log.prompted), (2, 1, 2), "not retried, on a new harness");
        assert_eq!(log.loaded, ["s1"], "it resumes the session");
    }
    assert_eq!(bus.unacked().await, 0);
}

/// An assistant lists the queue, withdraws a queued message and cancels a prompt for its human on requests naming
/// it, the human told of each; the same requests from an ant the human's grant does not name go unanswered.
#[tokio::test]
async fn an_assistant_withdraws_and_cancels_for_its_human_and_no_other_ant_does() {
    use ants_proto::{AssistantRecord, CancelRequest, MessageMark};
    let Some((mut bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [alice, pa, pb] = ants(["alice", "pa", "pb"]);
    let device = crate::people::humans::TestDevice::default();
    bus.humans = Registry::with(UserId::human(), &["alice"]).verifying_grants(Some(device.account()));
    bus.account = Some(device.account());
    let mut grant =
        ants_proto::AssistantGrant::new(alice.clone(), Some(pa.clone()), chrono::Utc::now(), device.jwt(&alice));
    grant.signature = device.sign(&grant.signed());
    bus.humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    let devices = jetstream::kv::Config { bucket: ants_proto::DEVICES_BUCKET.to_owned(), ..Default::default() };
    let devices = bus.js.create_key_value(devices).await.unwrap();
    for ant in [&pa, &pb] {
        let record = AssistantRecord { jwt: device.assistant_jwt(ant, &alice) };
        devices.put(ants_proto::assistant_record_key(ant), serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    }
    let mut to_alice = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(alice.clone(), &t1, "stall").await;
    let (b, _) = bus.send_from(alice.clone(), &t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let listing = QueueRequest::new(bus.dev.clone(), pa.clone()).for_human(alice.clone());
        let listed = timeout(WAIT, async {
            loop {
                let answer = queue_with(&bus, listing.clone()).await;
                if answer.messages.len() == 2 {
                    return answer;
                }
            }
        });
        let listed = listed.await.expect("both in hand");
        assert_eq!((listed.messages[1].id, listed.messages[1].running), (b.id, false));
        let quiet = Duration::from_millis(600);
        for by in [&pb, &pa] {
            let forged = CancelRequest::new(bus.dev.clone(), t1.clone(), by.clone()).withdrawing(b.id);
            assert_eq!(ask_cancel(&bus, &forged, quiet).await, None, "a withdrawal for none is not answered");
        }
        let forged = CancelRequest::new(bus.dev.clone(), t1.clone(), pb.clone()).for_human(alice.clone());
        assert_eq!(ask_cancel(&bus, &forged.clone().withdrawing(b.id), quiet).await, None, "pb is not alice's");
        assert_eq!(ask_cancel(&bus, &forged, quiet).await, None);
        assert_eq!(log.locked().cancelled, 0);

        let request = CancelRequest::new(bus.dev.clone(), t1.clone(), pa.clone()).for_human(alice.clone());
        let withdrawn = ask_cancel(&bus, &request.clone().withdrawing(b.id), WAIT).await.unwrap();
        assert!(withdrawn.cancelled);
        assert!(matches!(mark_of(&bus, &b).await, Some(MessageMark::Withdrawn { by, on_behalf_of: Some(human), .. })
                if by == pa && human == alice));
        let told = next_envelope(&mut to_alice).await;
        assert!(told.text.contains("pa, as the assistant of alice, withdrew message"), "{}", told.text);
        assert!(ask_cancel(&bus, &request, WAIT).await.unwrap().cancelled);
        let mut texts = Vec::new();
        for _ in 0..3 {
            texts.push(next_envelope(&mut to_alice).await.text);
        }
        texts.sort();
        assert_eq!(
            texts,
            [
                "[ants] Withdrawn by pa for alice before dev-a was prompted with it.".to_owned(),
                format!(
                    "[ants] pa, as the assistant of alice, cancelled the prompt of dev-a on thread {t1} for alice."
                ),
                "partial\n\n[ants] The prompt was cancelled by pa, the assistant of alice.".to_owned(),
            ]
        );
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let done = |log: &fake::Log| (log.cancelled, log.prompted);
    assert_eq!(done(&log.locked()), (1, 1), "b never reached the harness");
    assert_eq!(bus.unacked().await, 0);
}
