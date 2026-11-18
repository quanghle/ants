//! Posting to channels and the channel manager's changes.

use super::*;

#[tokio::test]
async fn ants_post_to_channels_as_themselves_and_read_but_never_write_the_channels_bucket() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let ops: ChannelId = "ops".parse().unwrap();
    let (_h, h_js) = server.connect(&human).await;
    let stream = bus::ensure_stream(&h_js).await.unwrap();
    assert!(stream.cached_info().config.subjects.iter().any(|s| s == subject::ALL_CHANNELS));
    let (_a, a_js) = server.connect(&dev_a).await;

    // Only the human creates and writes the bucket; ants read and watch it.
    assert!(bus::ensure_channels(&a_js).await.is_err(), "only the human creates the bucket");
    let h_kv = bus::ensure_channels(&h_js).await.unwrap();
    let mut entry = ChannelEntry::new(ops.clone());
    entry.add_member(dev_a.clone());
    h_kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let a_kv = bus::ensure_channels(&a_js).await.unwrap();
    let read: ChannelEntry = serde_json::from_slice(&a_kv.get("ops").await.unwrap().unwrap()).unwrap();
    assert_eq!(read.check_key("ops"), Ok(()));
    assert_eq!(read, entry);
    let mut watch = a_kv.watch_with_history(">").await.unwrap();
    assert_eq!(timeout(WAIT, watch.next()).await.unwrap().unwrap().unwrap().key, "ops");
    entry.add_member(dev_b.clone());
    assert!(a_kv.put("ops", serde_json::to_vec(&entry).unwrap().into()).await.is_err(), "ants never write");
    assert!(a_kv.delete("ops").await.is_err());
    h_kv.delete("ops").await.unwrap();

    // Posts go on the poster's own subject only, in the hub's stream or its outbox form.
    let post = ChannelPost::new(ops.clone(), dev_a.clone(), "@dev-b ready?");
    bus::publish_json(&a_js, post.subject(), &post, None).await.unwrap();
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let _live = a_raw.subscribe(subject::ALL_CHANNELS).await.unwrap();
    a_raw.publish(subject::outbox(&subject::chan(&ops, &dev_a)).unwrap(), "ok".into()).await.unwrap();
    for spoof in [subject::chan(&ops, &dev_b), subject::chan(&ops, &human), "ants.out.chan.ops.dev-b".to_owned()] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }

    // dev-a's channel consumer and reader filter channel subjects only, as one filter: several would hide it.
    let consumer = |name: String, filter: &str| jetstream::consumer::pull::Config {
        durable_name: Some(name),
        filter_subject: filter.to_owned(),
        ..Default::default()
    };
    let a_stream = bus::open_stream(&a_js, &dev_a).await.unwrap();
    let chan: jetstream::consumer::PullConsumer =
        a_stream.create_consumer(consumer(chan_consumer(&dev_a), subject::ALL_CHANNELS)).await.unwrap();
    let msg = chan.messages().await.unwrap().next().await.unwrap().unwrap();
    let got: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!((got.check_subject(&msg.subject), &got), (Ok(()), &post));
    msg.double_ack().await.unwrap();
    let room = consumer(chan_reader(&dev_a), &subject::chan_room(&ops));
    a_js.create_consumer_on_stream(room, STREAM).await.unwrap();
    a_js.delete_consumer_from_stream(chan_reader(&dev_a), STREAM).await.unwrap();
    for name in [chan_consumer(&dev_a), chan_reader(&dev_a)] {
        assert!(a_stream.create_consumer(consumer(name.clone(), &subject::inbox_of(&dev_b))).await.is_err());
        let several = jetstream::consumer::pull::Config {
            durable_name: Some(name),
            filter_subjects: vec![subject::chan_room(&ops), subject::inbox_of(&dev_b)],
            ..Default::default()
        };
        assert!(a_stream.create_consumer(several).await.is_err());
    }
    assert!(a_stream.create_consumer(consumer(chan_consumer(&dev_b), subject::ALL_CHANNELS)).await.is_err());

    // What dev-a's daemon reads: the channels, and the thread of a post back for its prompt.
    h_kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let channels = Channels::watch(a_kv).await.unwrap();
    assert_eq!(channels.get(&ops), Some(&entry));
    bus::init_own_buckets(&h_js).await.unwrap();
    let note = post.reply(human.clone(), "noted", &[], &ants_proto::Humans::default()).unwrap();
    bus::publish_json(&h_js, note.subject(), &note, None).await.unwrap();
    let ask = post.reply(human.clone(), "@dev-a go", &[], &ants_proto::Humans::default()).unwrap();
    let seq = bus::publish_json(&h_js, ask.subject(), &ask, None).await.unwrap();
    let threads = Threads::new(a_js.clone(), dev_a.clone(), bus::ensure_sessions(&a_js, None, &dev_a).await.unwrap());
    assert_eq!(
        threads.context(&ask, seq, Earlier::Unseen, &ants_proto::Humans::default()).await.unwrap().posts,
        [note],
        "without dev-a's own post"
    );
    threads.seen(&ask.thread_id, seq).await.unwrap();
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one scenario against one server, each step building on the ones before")]
async fn the_channel_manager_applies_the_owner_s_changes_only_and_announces_them() {
    use ants_daemon::chanadm;
    use ants_proto::{ChannelAdminOutcome, ChannelAdminRequest, ChannelOp, Refusal};

    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b, manager) = (UserId::human(), p("dev-a"), p("dev-b"), UserId::channel_manager());
    let ops: ChannelId = "ops".parse().unwrap();
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let colony = bus::ensure_colony(&h_js, ants_proto::COLONY_TTL).await.unwrap();
    let channels = bus::ensure_channels(&h_js).await.unwrap();
    let mut entry = ChannelEntry::new(ops.clone());
    entry.owner = Some(dev_a.clone());
    h_kv_put(&channels, &entry).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    let now = chrono::Utc::now();
    let listed = ColonyEntry {
        ant: dev_b.clone(),
        machine: "m1".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: Vec::new(),
        whoami: None,
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        sage: None,
        version: ants_proto::ANTS_VERSION.to_owned(),
        perms: ants_proto::PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
        model: None,
        paused: None,
        drift: Vec::new(),
        mcp: false,
    };
    let b_colony = bus::ensure_colony(&b_js, ants_proto::COLONY_TTL).await.unwrap();
    b_colony.put("dev-b", serde_json::to_vec(&listed).unwrap().into()).await.unwrap();
    assert!(colony.get("dev-b").await.unwrap().is_some());

    assert!(issue(&server.dir().join("auth"), &manager, Role::Ant, None, &server.dir().join("x")).is_err());
    let creds = server.dir().join("channel-manager.creds");
    issue_channel_manager(&server.dir().join("auth"), None, &creds).unwrap();
    let (m_client, m_js) = bus::connect(&server.url, Some(&creds), None, &manager, "test").await.unwrap();
    let humans = ants_daemon::people::humans::Registry::new(human.clone());
    let _watching = humans.watch(m_js.clone(), ants_daemon::people::humans::RETRY);
    let colony_of = ants_daemon::mcp::jetstream_colony(m_js.clone());
    let behalf = ants_daemon::people::behalf::Records::new(m_js.clone(), None, None);
    let serving = tokio::spawn(chanadm::serve(m_client, m_js, colony_of, humans, behalf));

    let mut room = h_client.subscribe(subject::chan_room(&ops)).await.unwrap();
    subscribed(&h_js, &human).await;
    let (a_client, a_js) = server.connect(&dev_a).await;
    let a_admin = chanadm::nats_admin(a_client.clone(), a_js);
    let ask = |admin: &std::sync::Arc<dyn chanadm::Admin>, ant: &UserId, ops_: Vec<ChannelOp>| {
        let request = ChannelAdminRequest::new(ops.clone(), ant.clone(), ThreadId::generate(), ops_);
        let admin = admin.clone();
        async move { admin.request(request).await.unwrap().expect("an answer").outcome }
    };
    let refused = |outcome: ChannelAdminOutcome| match outcome {
        ChannelAdminOutcome::Refused { refusal } => refusal,
        applied => panic!("applied: {applied:?}"),
    };

    // The owner adds an ant in the colony; the change is stored and announced in the channel, mentioning nobody.
    let add_b = vec![ChannelOp::AddMember { ant: dev_b.clone() }];
    let first = ChannelAdminRequest::new(ops.clone(), dev_a.clone(), ThreadId::generate(), add_b.clone());
    let answer = a_admin.request(first.clone()).await.unwrap().expect("an answer");
    let ChannelAdminOutcome::Applied { entry: applied, changed, .. } = answer.outcome.clone() else {
        panic!("refused")
    };
    assert_eq!(
        (applied.members.as_slice(), changed.as_slice()),
        (std::slice::from_ref(&dev_b), ["added dev-b".to_owned()].as_slice())
    );
    let stored: ChannelEntry = serde_json::from_slice(&channels.get("ops").await.unwrap().unwrap()).unwrap();
    assert_eq!(stored, applied);
    let msg = timeout(WAIT, room.next()).await.unwrap().unwrap();
    let post: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!((post.check_subject(&msg.subject), &post.from), (Ok(()), &manager));
    assert!(post.text.contains("dev-a") && post.text.contains("added dev-b") && post.mentions.is_empty(), "{post:?}");

    // Ants outside the colony, other ants, and the former owner are refused.
    let add_c = vec![ChannelOp::AddMember { ant: p("dev-c") }];
    assert_eq!(refused(ask(&a_admin, &dev_a, add_c).await), Refusal::NotInColony(p("dev-c")));
    // A human needs no colony entry to be a member, but is never the messenger.
    let add_human = vec![ChannelOp::AddMember { ant: human.clone() }];
    let ChannelAdminOutcome::Applied { entry: with_human, changed, .. } = ask(&a_admin, &dev_a, add_human).await else {
        panic!("a human member refused")
    };
    assert_eq!((with_human.members, changed), (vec![dev_b.clone(), human.clone()], vec!["added human".to_owned()]));
    let human_messenger = vec![ChannelOp::SetMessenger { ant: human.clone() }];
    assert_eq!(refused(ask(&a_admin, &dev_a, human_messenger).await), Refusal::Human(human.clone()));
    let remove_human = vec![ChannelOp::RemoveMember { ant: human.clone() }];
    assert_matches!(ask(&a_admin, &dev_a, remove_human).await, ChannelAdminOutcome::Applied { .. });
    let (b_client, b_js) = server.connect(&dev_b).await;
    let b_admin = chanadm::nats_admin(b_client, b_js);
    let remove_b = vec![ChannelOp::RemoveMember { ant: dev_b.clone() }];
    assert_matches!(refused(ask(&b_admin, &dev_b, remove_b.clone()).await), Refusal::NotOwner { .. });
    entry = stored;
    entry.owner = Some(dev_b.clone());
    h_kv_put(&channels, &entry).await;
    assert_matches!(refused(ask(&a_admin, &dev_a, remove_b.clone()).await), Refusal::NotOwner { .. });
    assert_matches!(ask(&b_admin, &dev_b, remove_b).await, ChannelAdminOutcome::Applied { .. });
    // A request asked again gets its first answer, not a second change.
    assert_eq!(a_admin.request(first).await.unwrap(), Some(answer));

    // A request naming another ant's subject is denied, and one asking for the answer elsewhere goes unanswered.
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let foreign = ChannelAdminRequest::new(ops.clone(), dev_b.clone(), ThreadId::generate(), add_b.clone());
    a_raw.publish(foreign.subject(), serde_json::to_vec(&foreign).unwrap().into()).await.unwrap();
    assert!(next_violation(&a_raw, &mut errors).await.contains("ants.chanadm.ops.dev-b"));
    let mut b_inbox = h_client.subscribe(format!("{}.>", inbox_prefix(&dev_b))).await.unwrap();
    let forged = ChannelAdminRequest::new(ops.clone(), dev_a.clone(), ThreadId::generate(), add_b);
    let payload = serde_json::to_vec(&forged).unwrap();
    a_raw.publish_with_reply(forged.subject(), format!("{}.x", inbox_prefix(&dev_b)), payload.into()).await.unwrap();
    a_raw.flush().await.unwrap();
    assert!(timeout(Duration::from_millis(500), b_inbox.next()).await.is_err(), "no answer to dev-b's inbox");

    // The channel manager writes the channels bucket only, and posts as itself only.
    let (m_raw, mut errors) = server.watched_with(&manager, &creds).await;
    for denied in [subject::chan(&ops, &dev_a), "$KV.ants_colony.dev-a".to_owned(), subject::inbox_of(&dev_a)] {
        m_raw.publish(denied.clone(), "x".into()).await.unwrap();
        let v = next_violation(&m_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    let _sub = m_raw.subscribe(subject::ALL_CHANNELS).await.unwrap();
    assert!(next_violation(&m_raw, &mut errors).await.contains(subject::ALL_CHANNELS));
    serving.abort();
}

async fn h_kv_put(kv: &jetstream::kv::Store, entry: &ChannelEntry) {
    kv.put(entry.key(), serde_json::to_vec(entry).unwrap().into()).await.unwrap();
}
