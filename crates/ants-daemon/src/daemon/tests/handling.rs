//! Handling a message: retries, withdrawals before the prompt, and attempts cut off or recorded before a crash.

use super::*;

#[tokio::test]
async fn a_failed_message_is_retried_before_the_rest_of_its_thread() {
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send(&t1, "flaky a").await;
    bus.send_ant(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // The failed attempt may have run tools too, so the retry carries the warning.
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), warned("flaky a")));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, [warned("flaky a"), "b".to_owned()]);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_thread_is_told_started_by_its_first_message_only() {
    use ants_proto::provider::{EventKind, Origin, Starter};

    use crate::provider::events::{Lifecycle, tests};
    let Some((mut bus, _)) = Bus::start().await else { return };
    let provider = tests::Recording::taking_events();
    let outbox = tests::outbox_to(provider.clone()).await;
    let home = ants_test_support::scratch_dir("handling-lifecycle");
    bus.lifecycle = Lifecycle::open(&outbox, &bus.dev, "id-1", &home).await;
    let mut replies = bus.replies().await;
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&t1, "a").await;
    bus.send_ant(&t1, "b").await;
    bus.send_ant(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        for _ in 0..3 {
            next_reply(&mut replies).await;
        }
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    tests::took(&provider, 3).await;
    let started = |thread: &ThreadId, user: &str| {
        let user = user.parse().unwrap();
        let by = if thread == &t1 { Starter::Human { user } } else { Starter::Ant { user } };
        (Some(thread.clone()), EventKind::ThreadStarted { origin: Origin { by, channel: None } })
    };
    let mut told: Vec<_> =
        provider.got.locked().iter().map(|event| (event.thread.clone(), event.kind.clone())).collect();
    told.sort_by_key(|(thread, _)| thread.as_ref().map(|thread| u8::from(thread == &t2)));
    assert_eq!(told, [(None, EventKind::AntAdded), started(&t1, "human"), started(&t2, "dev-b")]);
    std::fs::remove_dir_all(home).unwrap();
}

/// Ask the daemon which messages it holds until it answers, as the daemon may not listen yet.
async fn queue(bus: &Bus) -> ants_proto::QueueReply {
    queue_with(bus, QueueRequest::new(bus.dev.clone(), UserId::human())).await
}

/// Withdraw `message` as the human, returning the mark found instead if the withdrawal lost.
async fn withdraw(bus: &Bus, message: &Envelope) -> Option<ants_proto::MessageMark> {
    crate::delivery::attempts::withdraw(&bus.markers, &bus.dev, &message.id, &UserId::human(), None).await.unwrap()
}

const WITHDRAWN: &str = "[ants] Withdrawn by human before dev-a was prompted with it (ants withdraw).";

#[tokio::test]
async fn a_queued_message_is_listed_and_withdrawn_while_the_running_prompt_goes_on() {
    use ants_proto::MessageMark;
    let Some((bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, a_seq) = bus.send(&t1, "hold a").await;
    let (b, b_seq) = bus.send_ant(&t1, "b").await;
    let (c, c_seq) = bus.send_ant(&t1, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let listed = timeout(WAIT, async {
            loop {
                let answer = queue(&bus).await;
                if answer.messages.len() == 3 {
                    return answer;
                }
            }
        })
        .await
        .expect("all three in hand");
        let at = |m: &QueuedMessage| (m.id, m.seq, m.running, m.ahead, m.from.clone(), m.thread_id.clone());
        let human = UserId::human();
        assert_eq!(
            listed.messages.iter().map(at).collect::<Vec<_>>(),
            [
                (a.id, a_seq, true, 0, human.clone(), t1.clone()),
                (b.id, b_seq, false, 1, b.from.clone(), t1.clone()),
                (c.id, c_seq, false, 2, c.from.clone(), t1.clone()),
            ]
        );
        let inbox = listed.inbox.expect("the inbox consumer's place");
        assert_eq!((inbox.delivered, inbox.undelivered), (c_seq, 0));

        assert_eq!(withdraw(&bus, &b).await, None, "b is withdrawn");
        assert!(
            matches!(withdraw(&bus, &b).await, Some(MessageMark::Withdrawn { by, .. }) if by == human),
            "withdrawn once"
        );
        assert!(
            matches!(withdraw(&bus, &a).await, Some(MessageMark::Started { .. })),
            "a running prompt is cancelled, not withdrawn"
        );
        gate.add_permits(1);
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        let notice = next_envelope(&mut replies).await;
        assert_eq!((notice.text.as_str(), notice.in_reply_to, &notice.thread_id), (WITHDRAWN, Some(b.id), &t1));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "c".to_owned()), "the thread goes on");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["hold a", "c"], "b never reached the harness");
    assert_eq!(bus.unacked().await, 0);
    assert!(matches!(mark_of(&bus, &a).await, Some(MessageMark::Finished { .. })));
    assert!(matches!(withdraw(&bus, &c).await, Some(MessageMark::Finished { .. })), "c is answered already");
}

#[tokio::test]
async fn a_message_awaiting_its_retry_is_listed_ahead_of_its_thread_and_withdrawn_before_it() {
    use ants_proto::MessageMark;
    let Some((mut bus, _)) = Bus::start().await else { return };
    let mut replies = bus.replies().await;
    // Long enough for the listing to find the message between its attempts.
    bus.retry_delay = Duration::from_secs(3);
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, a_seq) = bus.send(&t1, "doomed a").await;
    let (b, b_seq) = bus.send_ant(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the first attempt", || log.locked().doomed == 1).await;
        gate.add_permits(1);
        until("the first attempt to fail", || log.locked().doomed_failed == 1).await;
        let listed = timeout(WAIT, async {
            loop {
                let answer = queue(&bus).await;
                if answer.messages.first().is_some_and(|m| m.redelivery) {
                    return answer;
                }
            }
        })
        .await
        .expect("a listed as awaiting its retry");
        let at = |m: &QueuedMessage| (m.id, m.seq, m.running, m.ahead, m.redelivery);
        assert_eq!(
            listed.messages.iter().map(at).collect::<Vec<_>>(),
            [(a.id, a_seq, false, 0, true), (b.id, b_seq, false, 1, false)]
        );
        let inbox = listed.inbox.expect("the inbox consumer's place");
        assert_eq!((inbox.delivered, inbox.undelivered, inbox.ack_pending), (b_seq, 0, 2), "{inbox:?}");
        assert!(inbox.acked < a_seq, "{inbox:?}");
        assert!(listed.posts.is_some(), "the channel consumer's place");
        assert!(matches!(mark_of(&bus, &a).await, Some(MessageMark::Retrying { .. })));
        assert_eq!(withdraw(&bus, &a).await, None, "a awaits its retry, withdrawable");
        let notice = next_envelope(&mut replies).await;
        let withdrawn = "[ants] Withdrawn by human before dev-a retried it after a failed attempt (ants withdraw).";
        assert_eq!((notice.text.as_str(), notice.in_reply_to), (withdrawn, Some(a.id)));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()), "the thread goes on");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().doomed, 1, "the retry never reached the harness");
    assert!(matches!(mark_of(&bus, &a).await, Some(MessageMark::Withdrawn { .. })));
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_message_withdrawn_before_its_delivery_is_never_prompted() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (a, a_seq) = bus.send(&t1, "a").await;
    assert_eq!(withdraw(&bus, &a).await, None);
    bus.send(&t2, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut texts = HashMap::new();
        for _ in 0..2 {
            let reply = next_envelope(&mut replies).await;
            texts.insert(reply.thread_id, reply.text);
        }
        assert_eq!(texts[&t1], WITHDRAWN);
        assert!(texts[&t2].ends_with(": b"), "{}", texts[&t2]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    bus.until_acked(a_seq).await;
    assert_eq!(log.locked().answered, ["b"]);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_withdrawn_message_redelivered_after_a_crash_is_acked_unprompted() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (progress_every, ack_wait) = (Duration::from_millis(100), ack_wait(Duration::from_millis(100)));
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    bus.send(&t1, "hold a").await;
    let (b, _) = bus.send(&t1, "b").await;

    // Withdrawn while it waits in hand behind a, then the daemon stops mid-prompt: both are redelivered.
    let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let crash = async {
        until("the prompt to reach the harness", || log.locked().prompted == 1).await;
        assert_eq!(withdraw(&bus, &b).await, None);
        ctrl_c.add_permits(2);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), crash);
    drop(pool);

    let pool = Pool::new(launch, store, 2, IDLE);
    let check = async {
        // b, handed back at once, may come back before a.
        let mut texts = vec![next_envelope(&mut replies).await.text, next_envelope(&mut replies).await.text];
        texts.sort();
        assert_eq!(texts, [WITHDRAWN.to_owned(), format!("s1: {}", warned("hold a"))]);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(log.locked().answered, [warned("hold a")], "b never reached the harness");
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), replies.next()).await.is_err(), "one notice");
}

#[tokio::test]
async fn a_message_held_for_a_sign_in_can_be_withdrawn() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().signed_out = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, _) = bus.send(&t1, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert!(notice.text.contains("is not signed in"), "{}", notice.text);
        assert_eq!(withdraw(&bus, &a).await, None, "turned away unprompted, a is not started");
        log.locked().signed_out = false;
        assert_eq!(next_envelope(&mut replies).await.text, WITHDRAWN);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(log.locked().answered.is_empty());
    assert_eq!(bus.unacked().await, 0);
}

const WITHDRAWN_LIMITED: &str =
    "[ants] Withdrawn by human before dev-a retried it after holding it for a provider limit (ants withdraw).";
const WITHDRAWN_SIGNED_OUT: &str =
    "[ants] Withdrawn by human before dev-a retried it after holding it for a sign-in (ants withdraw).";

#[tokio::test]
async fn a_message_held_for_a_provider_limit_after_its_tools_ran_can_be_withdrawn() {
    use ants_proto::MessageMark;
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().limited = Some("API Error: 429 Too Many Requests, retry after 0.2s".to_owned());
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, _) = bus.send(&t1, "tools 1 a").await;
    bus.send(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // Each try runs a tool before meeting the limit again; held, a awaits the next one, withdrawable.
        timeout(WAIT, async {
            loop {
                if matches!(
                    mark_of(&bus, &a).await,
                    Some(MessageMark::Retrying { held: Some(Hold::ProviderLimit), .. })
                ) && withdraw(&bus, &a).await.is_none()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a withdrawn while held for the limit");
        log.locked().limited = None;
        let notice = next_envelope(&mut replies).await;
        assert_eq!((notice.text.as_str(), notice.in_reply_to), (WITHDRAWN_LIMITED, Some(a.id)));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()), "the thread goes on");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["b"], "a never got past the limit");
    assert!(matches!(mark_of(&bus, &a).await, Some(MessageMark::Withdrawn { held: Some(Hold::ProviderLimit), .. })));
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_message_cut_off_then_held_for_a_sign_in_can_be_withdrawn() {
    use ants_proto::MessageMark;
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (progress_every, ack_wait) = (Duration::from_millis(100), ack_wait(Duration::from_millis(100)));
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    let (a, _) = bus.send(&t1, "hold a").await;

    // The daemon stops mid-prompt; redelivered, a meets a signed-out harness and is held, its attempt cut off.
    let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let crash = async {
        until("the prompt to reach the harness", || log.locked().prompted == 1).await;
        ctrl_c.add_permits(2);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), crash);
    drop(pool);
    log.locked().signed_out = true;

    let pool = Pool::new(launch, store, 2, IDLE);
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert!(notice.text.contains("is not signed in"), "{}", notice.text);
        timeout(WAIT, async {
            while !matches!(mark_of(&bus, &a).await, Some(MessageMark::Retrying { .. })) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a held, awaiting a retry");
        assert_eq!(withdraw(&bus, &a).await, None, "held for the sign-in, a is withdrawable");
        log.locked().signed_out = false;
        let notice = next_envelope(&mut replies).await;
        assert_eq!((notice.text.as_str(), notice.in_reply_to), (WITHDRAWN_SIGNED_OUT, Some(a.id)));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert!(log.locked().answered.is_empty(), "a never reached the harness again");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_message_held_for_a_sign_in_after_its_tools_ran_can_be_withdrawn() {
    use ants_proto::MessageMark;
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (a, _) = bus.send(&t1, "tools 1 sign-out a").await;
    bus.send(&t1, "b").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // The harness signs out once a's tool ran: held, a awaits the sign-in, withdrawable.
        let notice = next_envelope(&mut replies).await;
        assert!(notice.text.contains("is not signed in"), "{}", notice.text);
        timeout(WAIT, async {
            while !matches!(mark_of(&bus, &a).await, Some(MessageMark::Retrying { held: Some(Hold::SignIn), .. })) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a held for the sign-in");
        let listed = queue(&bus).await;
        assert!(listed.messages.iter().any(|m| m.id == a.id), "a is listed: {listed:?}");
        assert_eq!(withdraw(&bus, &a).await, None, "held for the sign-in, a is withdrawable");
        log.locked().signed_out = false;
        let notice = next_envelope(&mut replies).await;
        assert_eq!((notice.text.as_str(), notice.in_reply_to), (WITHDRAWN_SIGNED_OUT, Some(a.id)));
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()), "the thread goes on");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 2, "a reached the harness once, and b");
    assert_eq!(log.locked().answered, ["b"]);
    assert!(matches!(mark_of(&bus, &a).await, Some(MessageMark::Withdrawn { held: Some(Hold::SignIn), .. })));
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_first_delivery_is_prompted_as_sent_and_leaves_no_marker() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (_, seq) = bus.send(&t1, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "a".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["a"]);
    assert!(!bus.marked(seq).await, "the marker is removed with the ack");
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_message_cut_off_mid_prompt_is_prompted_again_with_a_warning() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (progress_every, ack_wait) = (Duration::from_millis(100), ack_wait(Duration::from_millis(100)));
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let t1 = ThreadId::generate();
    let (_, seq) = bus.send(&t1, "hold a").await;

    // Stopped mid-prompt, as by a crash: the harness may have run tools, and the message was not acked.
    let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let crash = async {
        // Not just the harness's start: stopped before its session is stored, the thread would start afresh.
        until("the prompt to reach the harness", || log.locked().prompted == 1).await;
        ctrl_c.add_permits(2);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), crash);
    // Its harness is still held mid-prompt, so it is dropped rather than shut down.
    drop(pool);
    assert!(bus.marked(seq).await, "the attempt is marked as started");
    assert!(log.locked().answered.is_empty());

    let pool = Pool::new(launch, store, 2, IDLE);
    let check = async {
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), warned("hold a")));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    {
        let log = log.locked();
        assert_eq!(log.answered, [warned("hold a")]);
        assert_eq!(log.loaded, ["s1"], "the session resumes, with the history of the cut-off attempt");
    }
    assert!(!bus.marked(seq).await);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_reply_recorded_before_a_crash_is_stored_once_without_a_prompt() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    let (ask, seq) = bus.send(&t1, "a").await;

    // The crashed attempt recorded its reply and stored it, but never acked.
    let reply = ask.reply(bus.dev.clone(), "s1: done before", Role::Human).unwrap();
    bus.attempts.start(seq, &ask).await.unwrap();
    bus.attempts.replied(seq, &ask, &Reply::Message(Box::new(reply.clone()))).await.unwrap();
    bus::publish_json(&bus.js, reply.subject(), &reply, Some(&reply_id(&bus.dev, &ask))).await.unwrap();
    assert_eq!(next_reply(&mut replies).await, (t1.clone(), "done before".to_owned()));

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().starts, 0, "not prompted again");
    let state = bus.stream.get_info().await.unwrap().state;
    assert_eq!(state.messages, 2, "the message and one copy of its reply");
    assert!(!bus.marked(seq).await);
    assert_eq!(bus.unacked().await, 0);
}

/// While no daemon of the ant serves, the assistant of a human withdraws a message of the human's, or one it sent for
/// the human, by a record under a key of its own, which the daemon honours as it takes the message up, telling the
/// human; one recorded by an ant the grant does not name, or after the message was taken up, withdraws nothing.
#[tokio::test]
async fn a_withdrawal_an_assistant_records_while_no_daemon_serves_is_honoured_before_the_prompt() {
    use crate::delivery::attempts::{Recorded, record_withdrawal};
    use ants_proto::{AssistantRecord, MessageMark};
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
    let [t1, t2, t3, t4] = [(); 4].map(|()| ThreadId::generate());
    let (a, _) = bus.send_from(alice.clone(), &t1, "a").await;
    let (b, _) = bus.send_from(alice.clone(), &t2, "b").await;
    let (c, _) = bus.send_from(alice.clone(), &t3, "c").await;
    let mut for_alice = Envelope::on_thread(t4.clone(), pa.clone(), bus.dev.clone(), "d");
    for_alice.on_behalf_of = Some(alice.clone());
    bus::publish_json(&bus.js, for_alice.subject(), &for_alice, None).await.unwrap();

    let record = async |message: &Envelope, assistant: &UserId| {
        record_withdrawal(&bus.markers, &bus.dev, &message.id, assistant, &alice).await
    };
    assert_eq!(record(&a, &pa).await.unwrap(), Recorded::Holds);
    assert_eq!(record(&for_alice, &pa).await.unwrap(), Recorded::Holds);
    assert_eq!(record(&b, &pb).await.unwrap(), Recorded::Holds, "stored, but pb is not alice's assistant");
    assert_eq!(
        bus.attempts.take_up(&c).await.unwrap(),
        crate::delivery::attempts::TakenUp::Started(None),
        "c is taken up"
    );
    assert!(matches!(record(&c, &pa).await.unwrap(), Recorded::Marked(MessageMark::Started { .. })), "too late");

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut texts = Vec::new();
        for _ in 0..5 {
            texts.push(next_envelope(&mut to_alice).await.text);
        }
        let has = |part: &str| texts.iter().any(|t| t.contains(part));
        assert!(has("[ants] Withdrawn by pa for alice before dev-a was prompted with it."), "{texts:?}");
        assert!(has(&format!("pa, as the assistant of alice, withdrew message {} from the queue", for_alice.id)));
        assert!(!has(&format!("withdrew message {}", a.id)), "a's withdrawal notice went to alice: {texts:?}");
        assert!(has(": b") && has(": c"), "{texts:?}");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let mut answered = log.locked().answered.clone();
    answered.sort();
    assert_eq!(answered, ["b", "c"]);
    for message in [&a, &for_alice] {
        assert!(
            matches!(mark_of(&bus, message).await, Some(MessageMark::Withdrawn { by, on_behalf_of: Some(human), .. })
                if by == pa && human == alice)
        );
    }
    assert_eq!(bus.unacked().await, 0);
}
