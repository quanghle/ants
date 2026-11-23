//! Messages held while the harness is signed out, limited or failing to start, and the fallbacks answering meanwhile.

use super::*;

#[tokio::test]
async fn messages_wait_unspent_while_the_harness_is_signed_out() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().knobs.signed_out = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let mut statuses = bus.client.subscribe(subject::status_of(&bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let (_, seq) = bus.send(&t1, "a").await;
    bus.send(&t1, "b").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let signed_out = |notice: &Envelope| {
        assert_eq!(notice.from, bus.dev);
        assert!(notice.text.contains("harness test on") && notice.text.contains("is not signed in"), "{notice:?}");
        assert!(notice.text.contains("Sign test in on"), "{}", notice.text);
    };
    let check = async {
        let notice = next_envelope(&mut replies).await;
        signed_out(&notice);
        assert!([&t1, &t2].contains(&&notice.thread_id));
        loop {
            let msg = timeout(WAIT, statuses.next()).await.expect("no signed_out state").unwrap();
            let event: StatusEvent = serde_json::from_slice(&msg.payload).unwrap();
            if event.update == (StatusUpdate::State { state: State::SignedOut }) {
                break;
            }
        }
        let shown = bus.workload.signed_out().expect("the colony shows the harness signed out");
        assert_eq!(shown.sign_in, None, "the test harness names no sign-in command");
        // Several checks of the harness pass, telling nobody again and spending no attempt.
        let starts = log.locked().starts;
        until("checks of the harness", || log.locked().starts >= starts + 3).await;
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, 0);
        assert_eq!(log.locked().prompted, 0);

        log.locked().knobs.signed_out = false;
        let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
        answered.push(next_reply(&mut replies).await);
        answered.sort_by_key(|(_, text)| text.clone());
        let expected = [(t1.clone(), "a"), (t1.clone(), "b"), (t2.clone(), "c")].map(|(t, s)| (t, s.to_owned()));
        assert_eq!(answered, expected, "answered unwarned: no attempt started");
        assert_eq!(bus.workload.signed_out(), None, "signed in again: the colony no longer shows it");

        // Signed out again once it worked: a live harness turns the prompt away, and a human is told again.
        log.locked().knobs.signed_out = true;
        bus.send(&t1, "d").await;
        signed_out(&next_envelope(&mut replies).await);
        assert!(bus.workload.signed_out().is_some_and(|s| s.since > shown.since), "a new signed-out period");
        log.locked().knobs.signed_out = false;
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "d".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let mut answered = log.locked().answered.clone();
    answered.sort();
    assert_eq!(answered, ["a", "b", "c", "d"]);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_fallback_answers_while_the_harness_is_signed_out() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let (spare, spare_log, _) = fake::launch();
    let spare = spare.named("spare");
    log.locked().knobs.signed_out = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE).with_fallbacks(vec![("spare".to_owned(), spare)]);
    let thread = ThreadId::generate();
    let (_, seq) = bus.send(&thread, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert!(notice.text.contains("cannot answer with its own harness: harness test on"), "{}", notice.text);
        assert!(notice.text.contains("go to its fallback spare meanwhile, and back to the harness test"), "{notice:?}");
        let reply = next_envelope(&mut replies).await;
        assert_eq!(reply.thread_id, thread);
        assert!(
            reply.text.ends_with("a\n\n[ants] Answered by fallback spare, harness test being signed out."),
            "{reply:?}"
        );
        let shown = bus.workload.fallback().expect("the colony shows the fallback");
        assert_eq!(shown.name, "spare");
        assert!(bus.workload.signed_out().is_some(), "the harness itself is still signed out");
        bus.until_acked(seq).await;

        // The fallback turned away too: the message waits, the owner told once, until one answers.
        let mut passed_over = async |text: &str| {
            spare_log.locked().knobs.signed_out = true;
            bus.send(&thread, text).await;
            let notice = next_envelope(&mut replies).await;
            assert_eq!(notice.thread_id, thread);
            assert!(
                notice
                    .text
                    .starts_with("[ants] dev-a cannot answer with a fallback either: fallback spare is signed out"),
                "{}",
                notice.text
            );
            assert!(notice.text.contains("until harness test, signed out, answers again or a fallback"), "{notice:?}");
            let starts = spare_log.locked().starts;
            until("the fallback tried again", || spare_log.locked().starts > starts).await;
            assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
            spare_log.locked().knobs.signed_out = false;
            let reply = next_envelope(&mut replies).await;
            assert!(reply.text.contains(&format!("{text}\n\n[ants] Answered by fallback spare")), "{reply:?}");
        };
        passed_over("b").await;
        assert_eq!(bus.workload.fallback().map(|f| f.since), Some(shown.since), "the same outage");
        passed_over("b2").await;

        // Signed in again: the thread goes back to the ant's own harness, rebuilt from its history.
        log.locked().knobs.signed_out = false;
        until("the fallback stood down", || bus.workload.fallback().is_none()).await;
        bus.send(&thread, "c").await;
        let reply = next_envelope(&mut replies).await;
        assert!(reply.text.ends_with("\nc"), "answered by the ant's own harness, with no footer: {reply:?}");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(spare_log.locked().answered, ["a", "b", "b2"]);
    let answered = log.locked().answered.clone();
    let [rebuilt] = answered.as_slice() else { panic!("{answered:?}") };
    assert!(rebuilt.contains("ran in another harness") && rebuilt.ends_with('c'), "{rebuilt}");
    assert!(log.locked().loaded.is_empty(), "the fallback's session is not loaded in another harness");
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn messages_wait_unspent_while_the_harness_keeps_failing_to_start() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().knobs.broken = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (_, seq) = bus.send(&t1, "a").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // Failures on both threads open the breaker before either message runs out of attempts.
        let notice = next_envelope(&mut replies).await;
        assert_eq!(notice.from, bus.dev);
        assert!(notice.text.contains("ants harness check --harness test"), "{}", notice.text);
        let escalation = notice.escalation.as_ref().expect("pushed as an escalation");
        assert_eq!((escalation.reason, escalation.level), (EscalationReason::Harness, 0));
        let shown = bus.workload.failing().expect("the colony shows the harness failing");
        assert!(shown.error.contains("broken install"), "{}", shown.error);
        let redelivered = bus.consumer().await.num_redelivered;
        assert!(redelivered < 3, "{redelivered}");
        // Several checks of the harness fail, telling nobody again and spending no attempt.
        let starts = log.locked().starts;
        until("checks of the harness", || log.locked().starts >= starts + 3).await;
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, redelivered);
        assert!(bus.workload.failing().is_some_and(|f| f.since == shown.since), "the same failing period");

        log.locked().knobs.broken = false;
        let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
        answered.sort_by_key(|(_, text)| text.clone());
        assert_eq!(answered, [(t1.clone(), "a".to_owned()), (t2.clone(), "c".to_owned())]);
        assert_eq!(bus.workload.failing(), None, "started again: the colony no longer shows it");

        // Once messages get through, a lone failure is tried again as before, holding nothing.
        log.locked().knobs.broken = true;
        let t3 = ThreadId::generate();
        let starts = log.locked().starts;
        bus.send(&t3, "d").await;
        until("a failed start", || log.locked().starts > starts).await;
        log.locked().knobs.broken = false;
        assert_eq!(next_reply(&mut replies).await, (t3.clone(), "d".to_owned()));
        assert_eq!(bus.workload.failing(), None);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let mut answered = log.locked().answered.clone();
    answered.sort();
    assert_eq!(answered, ["a", "c", "d"]);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn messages_wait_unspent_while_the_daemon_is_out_of_open_files() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().knobs.broken_with = Some("Too many open files (os error 24)".to_owned());
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (_, seq) = bus.send(&t1, "a").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // Far more failed starts than open the breaker or spend every attempt.
        until("starts tried again", || log.locked().starts >= MAX_DELIVER as usize * 2).await;
        // Each backoff clears the hold until the next start meets it again.
        let shown = timeout(WAIT, async {
            loop {
                if let Some(shown) = bus.workload.limited() {
                    break shown;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the colony shows the hold");
        assert_eq!(shown.kind, LimitKind::OpenFiles);
        assert_eq!(bus.workload.failing(), None, "no harness breaker opens");
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "the hold tells nobody");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, 0);

        log.locked().knobs.broken_with = None;
        let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
        answered.sort_by_key(|(_, text)| text.clone());
        assert_eq!(answered, [(t1.clone(), "a".to_owned()), (t2.clone(), "c".to_owned())]);
        until("the hold cleared", || bus.workload.limited().is_none()).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn messages_wait_unspent_through_a_provider_rate_limit() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    log.locked().knobs.limited = Some("API Error: 429 Too Many Requests, retry after 0.2s".to_owned());
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (_, seq) = bus.send(&t1, "a").await;
    bus.send(&t1, "b").await;
    bus.send(&t2, "c").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("prompts tried again", || log.locked().prompted >= 4).await;
        // Each backoff clears the limit until the next try meets it again.
        let shown = timeout(WAIT, async {
            loop {
                if let Some(shown) = bus.workload.limited() {
                    break shown;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the colony shows the limit");
        assert_eq!(shown.kind, LimitKind::RateLimit);
        assert!(shown.until > shown.since);
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "a rate limit tells nobody");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, 0);

        log.locked().knobs.limited = None;
        let mut answered = vec![next_reply(&mut replies).await, next_reply(&mut replies).await];
        answered.push(next_reply(&mut replies).await);
        answered.sort_by_key(|(_, text)| text.clone());
        let expected = [(t1.clone(), "a"), (t1.clone(), "b"), (t2.clone(), "c")].map(|(t, s)| (t, s.to_owned()));
        assert_eq!(answered, expected, "answered unwarned: no attempt started");
        until("the limit cleared", || bus.workload.limited().is_none()).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn an_exhausted_quota_is_told_once_and_spends_no_attempt() {
    let Some((bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    // Copilot CLI's reply when the provider turns the prompt away, which is no answer.
    let error = "Error: Failed to get response from the AI model; retried 5 times (total retry wait time: 56.70 \
                     seconds) Last error: 429 You exceeded your current quota, please check your plan and billing \
                     details.";
    {
        let mut faked = log.locked();
        (faked.knobs.limited, faked.knobs.limited_reply) = (Some(error.to_owned()), true);
    }
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let thread = ThreadId::generate();
    let (_, seq) = bus.send(&thread, "a").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut replies).await;
        assert_eq!(notice.thread_id, thread);
        assert!(notice.text.contains("harness test on") && notice.text.contains("out of quota"), "{notice:?}");
        assert!(notice.text.contains("spending no attempts"), "{}", notice.text);
        assert_eq!(bus.workload.limited().map(|l| l.kind), Some(LimitKind::Quota));
        until("prompts tried again", || log.locked().prompted >= 3).await;
        assert!(timeout(Duration::from_millis(100), replies.next()).await.is_err(), "told once");
        assert!(!bus.marked(seq).await, "no attempt is recorded");
        assert_eq!(bus.consumer().await.num_redelivered, 0);

        log.locked().knobs.limited = None;
        assert_eq!(next_reply(&mut replies).await, (thread.clone(), "a".to_owned()));
        until("the limit cleared", || bus.workload.limited().is_none()).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered, ["a"]);
    assert!(dead::list(&bus.stream).await.unwrap().is_empty());
    assert_eq!(bus.unacked().await, 0);
}
