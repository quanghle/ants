//! What the server lets each role do: ants, humans and assistants.

use super::*;

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "every role checked in turn against one server")]
async fn credentials_confine_each_user_to_its_role() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));

    assert!(bus::connect(&server.url, None, None, &human, "anonymous").await.is_err(), "credentials are required");

    let (_, a_js) = server.connect(&dev_a).await;
    assert!(bus::open_stream(&a_js, &dev_a).await.is_err(), "only the human creates the stream");
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    // dev-a finds JetStream on connecting as the daemon does, without the stream's info.
    let a_creds = server.creds(&dev_a);
    let connecting = bus::connect_retrying(&server.url, Some(&a_creds), None, &dev_a, "test", std::future::pending());
    timeout(WAIT, connecting).await.expect("dev-a reaches JetStream").unwrap().expect("not stopped");

    let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    h_client.flush().await.unwrap();
    let ask = Envelope::new(human.clone(), dev_a.clone(), "hi");
    bus::publish(&h_js, &ask).await.unwrap();

    // dev-a serves its inbox the way the daemon does, and replies.
    let stream = bus::open_stream(&a_js, &dev_a).await.unwrap();
    let mut inbox = bus::inbox_consumer(&stream, &dev_a, WAIT, 5, 2).await.unwrap();
    let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
    let reply = ask.reply(dev_a.clone(), "hello", Role::Human).unwrap();
    bus::publish(&a_js, &reply).await.unwrap();
    msg.double_ack().await.unwrap();
    let got = timeout(WAIT, human_inbox.next()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&got.payload).unwrap(), reply);

    // dev-a cannot read dev-b's inbox, through a consumer or the stream.
    assert!(bus::inbox_consumer(&stream, &dev_b, WAIT, 5, 2).await.is_err());
    assert!(stream.get_raw_message(1).await.is_err());

    // dev-a keeps its sessions under its own keys, and cannot touch dev-b's.
    let thread: ThreadId = "t1".parse().unwrap();
    assert!(bus::ensure_sessions(&a_js, None, &dev_a).await.is_err(), "only the human creates the bucket");
    assert!(bus::ensure_attempts(&a_js, None, &dev_a).await.is_err(), "only the human creates the bucket");
    bus::init_own_buckets(&h_js).await.unwrap();
    let sessions = bus::ensure_sessions(&a_js, None, &dev_a).await.unwrap();
    sessions.put(session_key(&dev_a, &thread), "s1").await.unwrap();
    assert_eq!(sessions.get(session_key(&dev_a, &thread)).await.unwrap().as_deref(), Some(&b"s1"[..]));
    assert!(sessions.put(session_key(&dev_b, &thread), "s1").await.is_err());
    assert!(sessions.get(session_key(&dev_b, &thread)).await.is_err());
    // Except that it reads dev-b's daily spend, for pool caps, though it cannot write it.
    let day = ants_proto::daily_spend_key(&dev_b, "2026-11-13");
    assert_eq!(sessions.get(&day).await.unwrap(), None);
    assert!(sessions.put(&day, "{}").await.is_err());

    // So it does with the markers of its attempts.
    let attempts = bus::ensure_attempts(&a_js, None, &dev_a).await.unwrap();
    attempts.put(attempt_key(&dev_a, 1), "started").await.unwrap();
    assert_eq!(attempts.get(attempt_key(&dev_a, 1)).await.unwrap().as_deref(), Some(&b"started"[..]));
    attempts.delete(attempt_key(&dev_a, 1)).await.unwrap();
    assert_eq!(attempts.get(attempt_key(&dev_a, 1)).await.unwrap(), None);
    assert!(attempts.put(attempt_key(&dev_b, 1), "started").await.is_err());
    assert!(attempts.get(attempt_key(&dev_b, 1)).await.is_err());
    // A human withdraws a message waiting for dev-a, and reads what became of one, but no attempt marker; dev-a
    // marks only its own messages.
    let marks = DirectKv::new(&h_js, None, ants_proto::ATTEMPTS_BUCKET).await.unwrap();
    let (queued, taken) = (ants_proto::Uuid::now_v7(), ants_proto::Uuid::now_v7());
    let mark = |ant: &UserId, message| ants_proto::message_mark_key(ant, message);
    assert!(marks.create(mark(&dev_a, &queued), "withdrawn").await.unwrap());
    assert!(!attempts.create(mark(&dev_a, &queued), "started").await.unwrap(), "withdrawn first");
    assert!(attempts.create(mark(&dev_a, &taken), "started").await.unwrap());
    assert_eq!(marks.get(mark(&dev_a, &taken)).await.unwrap().as_deref(), Some(&b"started"[..]));
    assert!(marks.put(attempt_key(&dev_a, 2), "started").await.is_err());
    assert!(marks.get(attempt_key(&dev_a, 1)).await.is_err());
    assert!(attempts.create(mark(&dev_b, &queued), "started").await.is_err());
    // Watching the messages it sent for those that keep waiting, dev-a reads them back through its own reader and
    // what became of them, and marks those told of under its own keys; the human only under its own.
    let (sent, all) = (subject::inbox_from(&dev_a), jetstream::consumer::DeliverPolicy::All);
    let read_sent = async |ant: &UserId, filter: &str| {
        let (reader, mut read) = (ants_proto::sent_reader(ant), Vec::new());
        let reading = ants_daemon::stream::reading::Reading::Consumer;
        ants_daemon::stream::reading::read_back(&a_js, &reader, reading, filter, all, |seq, _, _, _| {
            read.push(seq);
            std::ops::ControlFlow::Continue(())
        })
        .await
        .map(|()| read)
    };
    assert_eq!(read_sent(&dev_a, &sent).await.unwrap().len(), 1, "its reply");
    assert!(read_sent(&dev_a, &subject::inbox_from(&dev_b)).await.is_err());
    assert!(read_sent(&dev_b, &subject::inbox_from(&dev_b)).await.is_err());
    assert_eq!(attempts.get(mark(&dev_b, &queued)).await.unwrap(), None);
    let told = |user: &UserId, message| ants_proto::unserved_key(user, message);
    assert!(attempts.create(told(&dev_a, &queued), "{}").await.unwrap());
    assert!(!attempts.create(told(&dev_a, &queued), "{}").await.unwrap(), "told once");
    assert!(attempts.create(told(&dev_b, &queued), "{}").await.is_err());
    assert!(marks.create(told(&human, &queued), "{}").await.unwrap());
    assert!(marks.create(told(&dev_a, &taken), "{}").await.is_err());
    assert!(marks.get(told(&dev_a, &queued)).await.is_err());

    // Nor list their keys, which a stream info request with a subjects filter would, as of the owners bucket.
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let filter = br#"{"subjects_filter":">"}"#;
    for bucket in [ants_proto::SESSIONS_BUCKET, ants_proto::ATTEMPTS_BUCKET, ants_proto::OWNERS_BUCKET] {
        let info = format!("$JS.API.STREAM.INFO.KV_{bucket}");
        a_raw.publish(info.clone(), filter[..].into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");
    }
    // Nor the subjects stored in the stream: who messages whom, approval requests and dead letters.
    let info = format!("$JS.API.STREAM.INFO.{STREAM}");
    a_raw.publish(info.clone(), filter[..].into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");

    // Nor speak for dev-b, or listen to others.
    a_raw.publish(subject::status(&dev_a, &thread), "ok".into()).await.unwrap();
    a_raw.publish(subject::status(&dev_b, &thread), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains("Publish to \"ants.status.dev-b.t1\""), "{v}");
    a_raw.publish(subject::approvals(&dev_b), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains("Publish to \"ants.approvals.dev-b\""), "{v}");
    a_raw.publish(subject::outbox(&subject::dead(&dev_a)).unwrap(), "ok".into()).await.unwrap();
    for spoof in [subject::dead(&dev_b), subject::outbox(&subject::dead(&dev_b)).unwrap()] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }
    // It sends only under its own name: not as the human, nor as dev-b to the human, nor as the hub's stream
    // republishing what dev-b sent.
    a_raw.publish(subject::inbox(&dev_a, &dev_a), "ok".into()).await.unwrap();
    let republished = format!("{}.stored.{dev_b}", inbox_prefix(&human));
    for spoof in [subject::inbox(&dev_b, &human), subject::inbox(&human, &dev_b), republished] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }
    // It posts to thread notes only under its own name too.
    let thread: ThreadId = "t2".parse().unwrap();
    a_raw.publish(subject::notes(&thread, &dev_a), "ok".into()).await.unwrap();
    for spoof in [subject::notes(&thread, &dev_b), subject::notes(&thread, &human)] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }
    // dev-a hears the human's decisions on its approval requests, but cannot decide them itself.
    let request = ants_proto::Uuid::now_v7();
    let mut decisions = a_raw.subscribe(subject::approval_decision(&dev_a, &request)).await.unwrap();
    a_raw.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"ants.approvals.dev-a.{request}\"")), "{v}");
    h_client.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
    let got = timeout(WAIT, decisions.next()).await.unwrap().unwrap();
    assert_eq!(&got.payload[..], b"allow");
    // It reads back the decision stored on its own request, and on no one else's.
    // The human's publish is not acknowledged, so the stream may store it a moment after dev-a hears it.
    let stored = timeout(WAIT, async {
        loop {
            match stream.direct_get_last_for_subject(subject::approval_decision(&dev_a, &request)).await {
                Ok(stored) => break stored,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(&stored.payload[..], b"allow");
    let of_b = format!("$JS.API.DIRECT.GET.{STREAM}.{}", subject::approval_decision(&dev_b, &request));
    a_raw.publish(of_b.clone(), "".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{of_b}\"")), "{v}");
    // Only a human asks dev-a to cancel a prompt; dev-a hears its own cancel requests, and answers as itself only.
    let mut cancels = a_raw.subscribe(subject::cancel_of(&dev_a)).await.unwrap();
    let mut answers = h_client.subscribe(subject::cancelled(&dev_a, &thread)).await.unwrap();
    h_client.flush().await.unwrap();
    a_raw.publish(subject::cancel(&dev_a, &thread), "self".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::cancel(&dev_a, &thread))), "{v}");
    h_client.publish(subject::cancel(&dev_a, &thread), "cancel".into()).await.unwrap();
    let got = timeout(WAIT, cancels.next()).await.unwrap().unwrap();
    assert_eq!(&got.payload[..], b"cancel");
    a_raw.publish(subject::cancelled(&dev_a, &thread), "done".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, answers.next()).await.unwrap().unwrap().payload[..], b"done");
    a_raw.publish(subject::cancelled(&dev_b, &thread), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::cancelled(&dev_b, &thread))), "{v}");
    let _sub = a_raw.subscribe(subject::cancel_of(&dev_b)).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Subscription to \"{}\"", subject::cancel_of(&dev_b))), "{v}");
    // Likewise only a human asks for dev-a's queue, and dev-a answers as itself only.
    let mut asks = a_raw.subscribe(subject::queue(&dev_a)).await.unwrap();
    let mut listed = h_client.subscribe(subject::queued(&dev_a)).await.unwrap();
    h_client.flush().await.unwrap();
    a_raw.publish(subject::queue(&dev_a), "self".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::queue(&dev_a))), "{v}");
    h_client.publish(subject::queue(&dev_a), "queue".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, asks.next()).await.unwrap().unwrap().payload[..], b"queue");
    a_raw.publish(subject::queued(&dev_a), "listed".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, listed.next()).await.unwrap().unwrap().payload[..], b"listed");
    a_raw.publish(subject::queued(&dev_b), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::queued(&dev_b))), "{v}");
    let _sub = a_raw.subscribe(subject::queue(&dev_b)).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Subscription to \"{}\"", subject::queue(&dev_b))), "{v}");
    // And it hears the requests naming their sender, an assistant's for its human, to it only.
    let _own = a_raw.subscribe(subject::cancels_by_of(&dev_a)).await.unwrap();
    let _own = a_raw.subscribe(subject::queues_by_of(&dev_a)).await.unwrap();
    for denied in [subject::cancels_by_of(&dev_b), subject::queues_by_of(&dev_b)] {
        let _sub = a_raw.subscribe(denied.clone()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
    }
    // Its own inbox it may watch live; a denial would be the next violation below.
    let _own = a_raw.subscribe(subject::inbox_of(&dev_a)).await.unwrap();
    let _own = a_raw.subscribe(subject::stored_inbox(&dev_a)).await.unwrap();
    for denied in
        [subject::inbox_of(&dev_b), subject::stored_inbox(&dev_b), "_INBOX.>".to_owned(), subject::ALL.to_owned()]
    {
        let _sub = a_raw.subscribe(denied.clone()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
    }
}

#[tokio::test]
async fn every_human_has_the_same_rights_and_reads_only_its_own_inbox() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    assert_eq!(
        (role(&alice, true), role(&alice, false), role(&UserId::human(), false)),
        (Role::Human, Role::Ant, Role::Human)
    );
    let (a_client, a_js) = server.connect_as(&alice, role(&alice, true)).await;
    let (b_client, b_js) = server.connect_as(&bob, role(&bob, true)).await;
    let (d_client, d_js) = server.connect(&dev_a).await;
    let (_, carol_js) = server.connect_as(&p("carol"), role(&p("carol"), false)).await;

    // Either human creates the stream and the buckets; a name issued without --human does not.
    assert!(bus::ensure_stream(&carol_js).await.is_err(), "carol is no human");
    assert!(crate::people::humans::ensure(&d_js).await.is_err(), "only a human creates the humans bucket");
    bus::ensure_stream(&a_js).await.unwrap();
    let b_stream = bus::ensure_stream(&b_js).await.unwrap();
    bus::ensure_colony(&b_js, ants_proto::COLONY_TTL).await.unwrap();
    bus::init_own_buckets(&a_js).await.unwrap();
    bus::init_own_buckets(&b_js).await.unwrap();
    bus::ensure_channels(&a_js).await.unwrap();
    let a_humans = crate::people::humans::ensure(&a_js).await.unwrap();
    let b_humans = crate::people::humans::ensure(&b_js).await.unwrap();

    // Each human writes the humans bucket; ants read it, but never write it.
    for (kv, human) in [(&a_humans, &alice), (&b_humans, &bob)] {
        let entry = ants_proto::HumanEntry::new(human.clone());
        kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    }
    let d_humans = crate::people::humans::ensure(&d_js).await.unwrap();
    let listed: ants_proto::HumanEntry = serde_json::from_slice(&d_humans.get("bob").await.unwrap().unwrap()).unwrap();
    assert_eq!((listed.check_key("bob"), listed.human), (Ok(()), bob.clone()));
    let keys: Vec<String> = d_humans.keys().await.unwrap().try_collect().await.unwrap();
    assert_eq!(keys, ["alice", "bob"]);
    let forged = serde_json::to_vec(&ants_proto::HumanEntry::new(dev_a.clone())).unwrap();
    assert!(d_humans.put("dev-a", forged.into()).await.is_err(), "an ant cannot list itself");
    assert!(d_humans.delete("alice").await.is_err());

    // Both watch all traffic and read dead letters.
    let mut a_all = a_client.subscribe(subject::ALL).await.unwrap();
    let mut b_all = b_client.subscribe(subject::ALL).await.unwrap();
    for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
        subscribed(js, human).await;
    }
    assert!(dead::list(&b_stream).await.unwrap().is_empty());

    // Each reads its replies in its own inbox, never in the other's.
    let (to_alice, to_bob) =
        (Envelope::new(dev_a.clone(), alice.clone(), "a"), Envelope::new(dev_a.clone(), bob.clone(), "b"));
    bus::publish(&d_js, &to_alice).await.unwrap();
    bus::publish(&d_js, &to_bob).await.unwrap();
    for all in [&mut a_all, &mut b_all] {
        assert_eq!(timeout(WAIT, all.next()).await.unwrap().unwrap().subject.as_str(), to_alice.subject());
    }
    for (js, human, other, sent) in [(&a_js, &alice, &bob, &to_alice), (&b_js, &bob, &alice, &to_bob)] {
        let theirs = async { crate::messages::inbox::Inbox::open(js, other).await?.unread(&Default::default()).await };
        assert!(theirs.await.is_err(), "{human} reads {other}'s inbox");
        let mut inbox = crate::messages::inbox::Inbox::open(js, human).await.unwrap();
        let unread = inbox.unread(&crate::messages::inbox::Select::default()).await.unwrap();
        assert_eq!(unread.as_slice(), std::slice::from_ref(sent));
    }

    // Either human decides dev-a's approval requests.
    let request = ants_proto::Uuid::now_v7();
    let mut decisions = d_client.subscribe(subject::approval_decision(&dev_a, &request)).await.unwrap();
    subscribed(&d_js, &dev_a).await;
    a_client.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
    a_client.flush().await.unwrap();
    b_client.publish(subject::approval_decision(&dev_a, &request), "reject".into()).await.unwrap();
    // Published on different connections, so heard in either order.
    let mut heard = Vec::new();
    for _ in 0..2 {
        heard.push(timeout(WAIT, decisions.next()).await.unwrap().unwrap().payload);
    }
    heard.sort();
    assert_eq!(heard, [&b"allow"[..], b"reject"]);
}

/// An assistant decides no approval request on its subject, and cancels no prompt, asks for no queue, withdraws no
/// queued message nor records an owner on the subjects and keys a human uses: its decisions wait in its messages for
/// its human's confirmation, and readers of the rest could not tell its acts from its human's. It does those on
/// subjects and keys naming it, and its human for an owner, which readers check against the grant. Its admin orders,
/// which name the human they are for, wait for the human's confirmation at the machine's daemon.
#[tokio::test]
async fn an_assistant_takes_no_act_readers_could_not_tell_from_its_human_s() {
    let Some(server) = Server::start() else { return };
    let (alice, pa) = (p("alice"), p("pa"));
    let creds = server.dir().join("pa-assistant.creds");
    let new = NewDevice { assistant_of: Some(alice.clone()), ..NewDevice::default() };
    let rights = Permissions::for_assistant(&pa, &alice);
    Issue::new(&server.dir().join("auth"), &pa, Kind::Assistant, rights).with(&new).into_file(&creds).unwrap();
    let (client, mut errors) = server.watched_with(&pa, &creds).await;
    for s in [
        "ants.approvals.dev-a.r1",
        "ants.cancel.dev-a.t1",
        "ants.queue.dev-a",
        "$KV.ants_attempts.dev-a.msg.m1",
        "$KV.ants_owners.t1.dev-a",
        "ants.cancel.dev-a.t1.pb",
        "ants.queue.dev-a.pb",
        "$KV.ants_owners.t1.dev-a.bob.pa",
        "$KV.ants_owners.t1.dev-a.alice.pb",
        "$KV.ants_attempts.dev-a.msg.m1.pb",
    ] {
        client.publish(s, "{}".into()).await.unwrap();
        let v = next_violation(&client, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{s}\"")), "{v}");
    }
    for s in ["ants.cancel.dev-a.t1.pa", "ants.queue.dev-a.pa", "$KV.ants_owners.t1.dev-a.alice.pa"] {
        client.publish(s, "{}".into()).await.unwrap();
    }
    client.publish("ants.queue.dev-a", "{}".into()).await.unwrap();
    let v = next_violation(&client, &mut errors).await;
    assert!(v.contains("Publish to \"ants.queue.dev-a\""), "only the plain subject is refused: {v}");
}

/// An assistant watches the team's traffic and keeps its own state as a human does, under its own name, and reads
/// its human's read markers but never moves them, nor sends as anyone else. Of another human it reads only the inbox
/// marker, as any ant does.
#[tokio::test]
async fn an_assistant_reads_its_human_s_marker_but_writes_only_its_own() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, pa) = (p("alice"), p("bob"), p("pa"));
    let (_a_client, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, Role::Human).await;
    let creds = server.dir().join("pa-assistant.creds");
    let new = NewDevice { assistant_of: Some(alice.clone()), ..NewDevice::default() };
    let rights = Permissions::for_assistant(&pa, &alice);
    Issue::new(&server.dir().join("auth"), &pa, Kind::Assistant, rights).with(&new).into_file(&creds).unwrap();
    let (pa_client, mut pa_js) = bus::connect(&server.url, Some(&creds), None, &pa, "test").await.unwrap();
    pa_js.set_timeout(Duration::from_secs(1));
    bus::ensure_stream(&a_js).await.unwrap();
    bus::init_own_buckets(&a_js).await.unwrap();

    let marker = |n: &str| serde_json::to_vec(&json!({ "seq": n })).unwrap();
    let a_read = crate::messages::inbox::ensure_bucket(&a_js).await.unwrap();
    a_read.put(alice.as_str(), marker("1").into()).await.unwrap();
    crate::messages::inbox::ensure_bucket(&b_js).await.unwrap().put(bob.as_str(), marker("2").into()).await.unwrap();
    let pa_read = crate::messages::inbox::ensure_bucket(&pa_js).await.unwrap();
    assert_eq!(pa_read.get(alice.as_str()).await.unwrap().unwrap(), marker("1"), "it reads its human's marker");
    assert_eq!(pa_read.get(bob.as_str()).await.unwrap().unwrap(), marker("2"), "another's inbox marker, as an ant");
    crate::messages::inbox::ensure_bucket(&b_js)
        .await
        .unwrap()
        .put(format!("{bob}.chan"), marker("5").into())
        .await
        .unwrap();
    assert!(pa_read.get(&format!("{bob}.chan")).await.is_err(), "but no other marker of another human");
    assert!(pa_read.put(alice.as_str(), marker("3").into()).await.is_err(), "nor moves it");
    pa_read.put(pa.as_str(), marker("4").into()).await.unwrap();
    assert_eq!(a_read.get(alice.as_str()).await.unwrap().unwrap(), marker("1"));

    // It watches all traffic, as a human does, and serves its own inbox, as an ant does.
    let mut all = pa_client.subscribe(subject::ALL).await.unwrap();
    subscribed(&pa_js, &pa).await;
    let to_pa = Envelope::new(alice.clone(), pa.clone(), "hello");
    bus::publish(&a_js, &to_pa).await.unwrap();
    assert_eq!(timeout(WAIT, all.next()).await.unwrap().unwrap().subject.as_str(), to_pa.subject());
    let mut inbox = crate::messages::inbox::Inbox::open(&pa_js, &pa).await.unwrap();
    assert!(inbox.unread(&crate::messages::inbox::Select::default()).await.is_ok());
    assert!(bus::publish(&pa_js, &Envelope::new(alice.clone(), bob.clone(), "as alice")).await.is_err());
    bus::publish(&pa_js, &Envelope::new(pa.clone(), alice.clone(), "as itself")).await.unwrap();
    let alice_s =
        async { crate::messages::inbox::Inbox::open(&pa_js, &alice).await?.unread(&Default::default()).await };
    assert!(alice_s.await.is_err(), "it reads no human's inbox");

    // Its daemon reads a thread back by direct gets, holding none of an ant's readers.
    let thread = to_pa.thread_id.clone();
    bus::publish(&pa_js, &Envelope::on_thread(thread.clone(), pa.clone(), alice.clone(), "hi")).await.unwrap();
    notes::publish(&pa_js, &NotesPost::new(thread.clone(), pa.clone(), "noted"), None).await.unwrap();
    let read = |reading| ants_daemon::stream::history::Thread {
        js: pa_js.clone(),
        ant: pa.clone(),
        thread: thread.clone(),
        channel: None,
        before: u64::MAX,
        reading,
        home: None,
    };
    let history = read(Reading::daemon(true)).history()(ants_daemon::stream::history::DEFAULT_BUDGET).await;
    for said in ["hello", "hi", "noted"] {
        assert!(history.contains(said), "{said} missing from {history}");
    }
    let history = read(Reading::Consumer).history()(ants_daemon::stream::history::DEFAULT_BUDGET).await;
    assert!(history.contains("could not be read back"), "{history}");

    // While no daemon of an ant answers, it withdraws a message for its human under a key naming it, which the ant's
    // daemon reads and honours before taking the message up.
    use ants_daemon::delivery::attempts::{Attempts, Recorded, TakenUp, record_withdrawal};
    let dev_a = p("dev-a");
    let message = Envelope::new(alice.clone(), dev_a.clone(), "later");
    let pa_attempts = DirectKv::new(&pa_js, None, ants_proto::ATTEMPTS_BUCKET).await.unwrap();
    let recorded = record_withdrawal(&pa_attempts, &dev_a, &message.id, &pa, &alice).await.unwrap();
    assert_eq!(recorded, Recorded::Holds);
    assert!(pa_attempts.put(ants_proto::message_mark_key(&dev_a, &message.id), "{}").await.is_err());
    let (_, d_js) = server.connect(&dev_a).await;
    let attempts = Attempts::new(bus::ensure_attempts(&d_js, None, &dev_a).await.unwrap(), dev_a.clone());
    assert_eq!(attempts.take_up(&message).await.unwrap(), TakenUp::Started(None));
    let (human, started) = attempts.withdrawal_by(&message, &pa).await.unwrap().expect("recorded before the take-up");
    assert_eq!(human, alice);
    assert!(attempts.withdraw_started(&message, &pa, &alice, started, None).await.unwrap());
    let recorded = record_withdrawal(&pa_attempts, &dev_a, &message.id, &pa, &alice).await.unwrap();
    assert!(matches!(recorded, Recorded::Marked(ants_proto::MessageMark::Withdrawn { .. })), "{recorded:?}");
}

#[tokio::test]
async fn only_a_human_creates_and_reads_its_own_notification_consumer() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let config = |of: &UserId| jetstream::consumer::pull::Config {
        durable_name: Some(ants_proto::notify_consumer(of)),
        filter_subjects: vec![subject::ALL_APPROVAL_REQUESTS.to_owned(), subject::inbox_of(of)],
        ..Default::default()
    };
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let a_stream = a_js.get_stream(STREAM).await.unwrap();
    let mine: jetstream::consumer::PullConsumer = a_stream.create_consumer(config(&alice)).await.unwrap();
    let one = jetstream::consumer::pull::Config { filter_subjects: vec![subject::inbox_of(&alice)], ..config(&alice) };
    a_stream.create_consumer::<jetstream::consumer::pull::Config>(one).await.unwrap();
    let (_, d_js) = server.connect(&dev_a).await;
    bus::publish_json(&d_js, subject::inbox(&alice, &dev_a), &Envelope::new(dev_a.clone(), alice.clone(), "hi"), None)
        .await
        .unwrap();
    let msg = mine.messages().await.unwrap().next().await.unwrap().unwrap();
    msg.ack().await.unwrap();

    let (_, b_js) = server.connect_as(&bob, Role::Human).await;
    let b_stream = b_js.get_stream(STREAM).await.unwrap();
    assert!(b_stream.create_consumer::<jetstream::consumer::pull::Config>(config(&alice)).await.is_err());
    let d_stream = d_js.get_stream_no_info(STREAM).await.unwrap();
    assert!(d_stream.create_consumer::<jetstream::consumer::pull::Config>(config(&dev_a)).await.is_err());
}

/// On a hub with a JetStream domain, the domain wildcards of a human's credentials (`$JS.hub.API.STREAM.CREATE.*`)
/// reach no further than its plain rights: the hub checks the subject it maps them to as well.
#[tokio::test]
async fn a_humans_domain_wildcards_reach_no_further_than_its_plain_rights() {
    let Some(hub) = Server::start_with("domain: hub", "") else { return };
    let (domain, human) = ("hub".parse::<JsDomain>().unwrap(), UserId::human());
    let creds = hub.creds_in(&human, Some(&domain));
    let (client, mut js) = bus::connect(&hub.url, Some(&creds), Some(&domain), &human, "test").await.unwrap();
    js.set_timeout(Duration::from_secs(1));
    bus::ensure_stream(&js).await.unwrap();
    let ask = |subject: &str, body: &str| {
        let request = client.request(subject.to_owned(), body.to_owned().into());
        async move { timeout(Duration::from_secs(1), request).await.ok().and_then(Result::ok) }
    };
    assert!(ask("$JS.hub.API.STREAM.INFO.ANTS", "").await.is_some(), "a right of the family answers");
    let config = r#"{"name":"FOO","subjects":["foo.>"]}"#;
    for (subject, body) in [
        ("$JS.hub.API.STREAM.CREATE.FOO", config),
        ("$JS.hub.API.STREAM.UPDATE.KV_ants_reactions", ""),
        ("$JS.hub.API.STREAM.INFO.KV_ants_identities", ""),
        ("$JS.hub.API.CONSUMER.CREATE.KV_ants_sessions", r#"{"stream_name":"KV_ants_sessions","config":{}}"#),
    ] {
        assert!(ask(subject, body).await.is_none(), "{subject} answered");
    }
    let streams: Vec<_> = js.streams().try_collect().await.unwrap();
    assert!(streams.iter().all(|info| info.config.name != "FOO"));
}

/// async-nats recreates an ordered consumer after a reconnect or missed heartbeats under a name, with its filter in the
/// create subject when it has one, after deleting the old one by name: the server takes both from a human's and an
/// ant's credentials, or their watches would stall.
#[tokio::test]
async fn ordered_consumers_of_a_bucket_are_recreated_under_a_name() {
    use jetstream::consumer::push::OrderedConfig;
    let Some(server) = Server::start() else { return };
    let (human, dev_a) = (UserId::human(), p("dev-a"));
    let (h_client, h_js) = server.connect_as(&human, Role::Human).await;
    h_js.create_key_value(ants_daemon::hub::channels_config()).await.unwrap();
    let (a_client, a_js) = server.connect_as(&dev_a, Role::Ant).await;
    let stream = format!("KV_{}", ants_proto::CHANNELS_BUCKET);
    for (who, client, js) in [("human", &h_client, &h_js), ("ant", &a_client, &a_js)] {
        let kv = js.get_stream_no_info(&stream).await.unwrap();
        let prefix = format!("$KV.{}.", ants_proto::CHANNELS_BUCKET);
        let named = [
            OrderedConfig { filter_subject: format!("{prefix}>"), ..Default::default() },
            OrderedConfig { filter_subjects: vec![format!("{prefix}a"), format!("{prefix}b")], ..Default::default() },
        ];
        for (n, config) in named.into_iter().enumerate() {
            let name = format!("{who}_{n}");
            let config = OrderedConfig { name: Some(name.clone()), deliver_subject: client.new_inbox(), ..config };
            kv.create_consumer(config).await.unwrap_or_else(|e| panic!("{who} creating {name}: {e}"));
            js.delete_consumer_from_stream(&name, &stream)
                .await
                .unwrap_or_else(|e| panic!("{who} deleting {name}: {e}"));
        }
    }
}

/// Every role that creates the notify, devices or humans bucket gets its stream info, so it finds the bucket as
/// `ants init` left it, rollups denied, and never sends the create its KV configuration no longer matches: a human
/// for all three, an ant for the humans, and the revoker putting a revocation record in the devices bucket.
#[tokio::test]
async fn the_buckets_ants_init_made_open_under_every_role_that_creates_them() {
    let Some(server) = Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let rights = crate::people::humans::Rights::from_creds(Some(&creds)).unwrap();
    ants_daemon::hub::init(&js, |s| rights.may_publish(s)).await.unwrap();

    crate::people::quiet::ensure_bucket(&js).await.unwrap();
    crate::people::humans::ensure(&js).await.unwrap();
    let own = crate::people::devices::Own::from_creds(Some(&creds)).unwrap().unwrap();
    crate::people::devices::publish(&js, &own).await.unwrap();
    let (_, ant_js) = server.connect_as(&dev_a, Role::Ant).await;
    crate::people::humans::ensure(&ant_js).await.unwrap();
    let team = server.dir().join("auth");
    crate::credentials::revoke::revoke(&team, None, &[nkeys::KeyPair::new_user().public_key()]).unwrap();
    assert_eq!(crate::people::devices::record_revocations(&server.url, None, &team).await.unwrap().len(), 1);

    for bucket in [ants_proto::NOTIFY_BUCKET, ants_proto::HUMANS_BUCKET, ants_proto::DEVICES_BUCKET] {
        let stream = js.get_stream(format!("KV_{bucket}")).await.unwrap();
        assert!(!stream.cached_info().config.allow_rollup, "{bucket} denies rollups");
    }
}

/// On a hub where `ants init` never ran, a human creating a bucket on first use creates it as `ants init` does: its
/// rollups denied, and a watched one with room for every daemon's watch. The revoker, which may not deny them, creates
/// no devices bucket and names `ants init`.
#[tokio::test]
async fn buckets_made_on_first_use_are_made_as_ants_init_makes_them() {
    use ants_daemon::hub::{MAX_CONSUMERS, Object};
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let team = server.dir().join("auth");
    crate::credentials::revoke::revoke(&team, None, &[nkeys::KeyPair::new_user().public_key()]).unwrap();
    let refused = crate::people::devices::record_revocations(&server.url, None, &team).await.unwrap_err();
    assert!(format!("{refused:#}").contains("`ants init`"), "{refused:#}");

    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    assert!(js.get_stream(Object::Devices.stream_name()).await.is_err(), "the revoker created no bucket");
    crate::people::quiet::ensure_bucket(&js).await.unwrap();
    crate::people::humans::ensure(&js).await.unwrap();
    let own = crate::people::devices::Own::from_creds(Some(&creds)).unwrap().unwrap();
    crate::people::devices::publish(&js, &own).await.unwrap();
    crate::webhook::ensure_bucket(&js).await.unwrap();
    bus::ensure_channels(&js).await.unwrap();
    bus::ensure_chains(&js).await.unwrap();
    bus::ensure_links(&js).await.unwrap();
    bus::init_own_buckets(&js).await.unwrap();

    for object in [
        Object::Notify,
        Object::Humans,
        Object::Devices,
        Object::Webhooks,
        Object::Channels,
        Object::Chains,
        Object::Links,
        Object::Sessions,
        Object::Attempts,
    ] {
        let config = js.get_stream(object.stream_name()).await.unwrap().cached_info().config.clone();
        assert!(!config.allow_rollup, "{object} denies rollups");
        assert_eq!(config.max_consumers == MAX_CONSUMERS, object.watched(), "{object}: {}", config.max_consumers);
    }
    assert_eq!(crate::people::devices::record_revocations(&server.url, None, &team).await.unwrap().len(), 1);
}
