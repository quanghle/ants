//! Credentials of what is neither ant nor human: webhooks, observers and hooks.

use super::*;

#[tokio::test]
async fn a_webhook_posts_as_itself_to_its_destinations_only_and_reads_no_message() {
    let Some(server) = Server::start() else { return };
    let (hook, dev_a, ops) = (p("github"), p("dev-a"), "ops".parse::<ChannelId>().unwrap());
    let creds = server.dir().join("github.creds");
    let destinations = [Destination::Ant(dev_a.clone()), Destination::Channel(ops.clone())];
    issue_with(
        &server.dir().join("auth"),
        &hook,
        crate::revoke::Kind::Webhook,
        Permissions::for_webhook(&hook, &destinations),
        &creds,
    )
    .unwrap();
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    crate::webhook::ensure_bucket(&h_js).await.unwrap();

    let (raw, mut errors) = server.watched_with(&hook, &creds).await;
    for allowed in [subject::inbox(&dev_a, &hook), subject::chan(&ops, &hook), "$KV.ants_webhooks.github.x".to_owned()]
    {
        raw.publish(allowed, "ok".into()).await.unwrap();
    }
    for denied in [
        subject::inbox(&p("dev-b"), &hook),
        subject::inbox(&dev_a, &p("dev-b")),
        subject::chan(&"dev".parse().unwrap(), &hook),
        "ants.out.inbox.dev-a.github".to_owned(),
        "$KV.ants_channels.ops".to_owned(),
        "$KV.ants_humans.github".to_owned(),
        "$KV.ants_webhooks.gitlab.x".to_owned(),
        subject::notes(&"t1".parse().unwrap(), &hook),
    ] {
        raw.publish(denied.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    for denied in [subject::ALL, "_INBOX.dev-a.>"] {
        let _sub = raw.subscribe(denied).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
    }
    // Nor does it list the subjects of the stream or the bucket: who messages whom, other webhooks' events.
    for info in ["$JS.API.STREAM.INFO.ANTS", "$JS.API.STREAM.INFO.KV_ants_webhooks"] {
        raw.publish(info, br#"{"subjects_filter":">"}"#[..].into()).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");
    }

    // Yet it finds JetStream on connecting, and records and reads its own events by key, as `ants webhook serve`.
    let connecting = bus::connect_retrying(&server.url, Some(&creds), None, &hook, "test", std::future::pending());
    let (_, mut hook_js) = timeout(WAIT, connecting).await.expect("github reaches JetStream").unwrap().unwrap();
    hook_js.set_timeout(Duration::from_secs(1));
    let seen = DirectKv::new(&hook_js, None, ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    assert!(seen.exists(&hook).await.unwrap());
    let key = ants_proto::webhook_seen_key(&hook, "e1");
    assert!(seen.create(&key, "first").await.unwrap());
    assert!(!seen.create(&key, "again").await.unwrap(), "an event is recorded once");
    assert_eq!(seen.get(&key).await.unwrap().as_deref(), Some(&b"first"[..]));
    seen.delete(&key).await.unwrap();
    assert!(seen.create(&key, "after").await.unwrap(), "a deleted record is no record");
    assert_eq!(seen.get(&key).await.unwrap().as_deref(), Some(&b"after"[..]));
    assert!(seen.create(ants_proto::webhook_seen_key(&p("gitlab"), "e1"), "x").await.is_err());
    assert!(seen.get(ants_proto::webhook_seen_key(&p("gitlab"), "e1")).await.is_err());

    // Its marker tells ants it is a webhook; they read no event it delivered, and put no marker of their own.
    raw.publish(ants_proto::webhook_marker_subject(&hook), "github".into()).await.unwrap();
    raw.flush().await.unwrap();
    let (_, a_js) = server.connect(&dev_a).await;
    let bucket = a_js.get_stream_no_info(format!("KV_{}", ants_proto::WEBHOOKS_BUCKET)).await.unwrap();
    let marker = bucket.direct_get_last_for_subject(ants_proto::webhook_marker_subject(&hook)).await.unwrap();
    assert_eq!(&marker.payload[..], b"github");
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    for denied in [
        "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.github.x".to_owned(),
        ants_proto::webhook_marker_subject(&dev_a),
    ] {
        a_raw.publish(denied.clone(), "".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
}

#[tokio::test]
async fn an_observer_reads_approvals_dead_letters_status_and_the_colony_and_publishes_nothing() {
    use ants_proto::Feed;
    use async_nats::jetstream::consumer;
    let Some(server) = Server::start() else { return };
    let (audit, dev_a, dev_b) = (p("audit"), p("dev-a"), p("dev-b"));
    let creds = server.dir().join("audit.creds");
    let team = server.dir().join("auth");
    issue_with(&team, &audit, crate::revoke::Kind::Observer, Permissions::for_observer(&audit), &creds).unwrap();
    let text = std::fs::read_to_string(&creds).unwrap();
    assert_eq!(ants_daemon::creds::Finding::from_creds(&text).unwrap().kind, ants_daemon::creds::Kind::Observer);
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    bus::ensure_colony(&h_js, Duration::from_secs(60)).await.unwrap();
    let (a, a_js) = server.connect(&dev_a).await;
    let (_, b_js) = server.connect(&dev_b).await;
    let request = ants_proto::Uuid::now_v7();
    let stored = [
        (&a_js, subject::approvals(&dev_a)),
        (&h_js, subject::approval_decision(&dev_a, &request)),
        (&a_js, subject::dead(&dev_a)),
        (&b_js, subject::inbox(&dev_a, &dev_b)),
        (&a_js, subject::notes(&ThreadId::generate(), &dev_a)),
    ];
    for (js, subject) in &stored {
        js.publish(subject.clone(), "event".into()).await.unwrap().await.unwrap();
    }
    a_js.get_key_value(ants_proto::COLONY_BUCKET).await.unwrap().put("dev-a", "{}".into()).await.unwrap();

    // It finds JetStream on connecting, reads each feed through its own consumer and watches the colony.
    let connecting = bus::connect_retrying(&server.url, Some(&creds), None, &audit, "test", std::future::pending());
    let (o, mut o_js) = timeout(WAIT, connecting).await.expect("audit reaches JetStream").unwrap().unwrap();
    o_js.set_timeout(Duration::from_secs(1));
    for (feed, want) in [
        (Feed::ApprovalRequests, subject::approvals(&dev_a)),
        (Feed::ApprovalDecisions, subject::approval_decision(&dev_a, &request)),
        (Feed::DeadLetters, subject::dead(&dev_a)),
    ] {
        let name = feed.consumer(&audit);
        let config = consumer::pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: feed.filter().to_owned(),
            ..Default::default()
        };
        let reader: consumer::PullConsumer = o_js.create_consumer_on_stream(config, STREAM).await.unwrap();
        let msgs: Vec<_> = reader.fetch().max_messages(10).messages().await.unwrap().try_collect().await.unwrap();
        let subjects: Vec<&str> = msgs.iter().map(|m| m.subject.as_str()).collect();
        assert_eq!(subjects, [want.as_str()], "{feed:?}");
        msgs[0].double_ack().await.unwrap();
        let again: consumer::PullConsumer = o_js.get_consumer_from_stream(&name, STREAM).await.unwrap();
        assert_eq!(again.cached_info().num_ack_pending, 0, "{feed:?}");
        o_js.delete_consumer_from_stream(&name, STREAM).await.unwrap();
    }
    let colony = o_js.get_key_value(ants_proto::COLONY_BUCKET).await.unwrap();
    assert_eq!(colony.get("dev-a").await.unwrap().as_deref(), Some(&b"{}"[..]));
    let mut watch = colony.watch_all().await.unwrap();
    a_js.get_key_value(ants_proto::COLONY_BUCKET).await.unwrap().put("dev-a", "{\"a\":1}".into()).await.unwrap();
    let entry = timeout(WAIT, watch.next()).await.expect("no colony entry").unwrap().unwrap();
    assert_eq!(entry.key, "dev-a");
    let mut status = o.subscribe(subject::ALL_STATUS).await.unwrap();
    // A flush only writes the subscription; a request on the same connection returns once the server has it.
    colony.get("dev-a").await.unwrap();
    let thread = ThreadId::generate();
    a.publish(subject::status(&dev_a, &thread), "{}".into()).await.unwrap();
    let heard = timeout(WAIT, status.next()).await.expect("no status").unwrap();
    assert_eq!(heard.subject.as_str(), subject::status(&dev_a, &thread));

    // It sends nothing, decides nothing, writes no bucket, and reads no conversation.
    let (raw, mut errors) = server.watched_with(&audit, &creds).await;
    for denied in [
        subject::inbox(&dev_a, &audit),
        subject::inbox(&dev_a, &dev_b),
        subject::outbox(&subject::inbox(&dev_a, &audit)).unwrap(),
        subject::notes(&thread, &audit),
        subject::chan(&"ops".parse().unwrap(), &audit),
        subject::approvals(&audit),
        subject::approval_decision(&dev_a, &request),
        subject::dead(&dev_a),
        subject::status(&audit, &thread),
        subject::cancel(&dev_a, &thread),
        subject::queue(&dev_a),
        format!("$KV.ants_attempts.{}", ants_proto::message_mark_key(&dev_a, &request)),
        subject::renew("dev-a"),
        "$KV.ants_colony.dev-a".to_owned(),
        "$KV.ants_humans.audit".to_owned(),
        "$JS.API.STREAM.INFO.ANTS".to_owned(),
        "$JS.API.STREAM.MSG.GET.ANTS".to_owned(),
        "$JS.API.DIRECT.GET.ANTS".to_owned(),
        format!("$JS.API.DIRECT.GET.ANTS.{}", subject::inbox(&dev_a, &dev_b)),
        // Consumers with another filter, or none in the subject, or another's name.
        format!("$JS.API.CONSUMER.CREATE.ANTS.{}.ants.inbox.>", Feed::DeadLetters.consumer(&audit)),
        "$JS.API.CONSUMER.CREATE.ANTS".to_owned(),
        format!("$JS.API.CONSUMER.CREATE.ANTS.{}", Feed::DeadLetters.consumer(&audit)),
        format!("$JS.API.CONSUMER.CREATE.ANTS.inbox-dev-a.{}", subject::inbox_of(&dev_a)),
        format!("$JS.API.CONSUMER.MSG.NEXT.ANTS.{}", ants_proto::inbox_consumer(&dev_a)),
        "$JS.API.CONSUMER.CREATE.KV_ants_sessions".to_owned(),
        "$JS.API.DIRECT.GET.KV_ants_presence.>".to_owned(),
        "$JS.API.STREAM.INFO.KV_ants_presence".to_owned(),
    ] {
        raw.publish(denied.clone(), "{}".into()).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    for denied in
        [subject::ALL, "ants.inbox.>", subject::ALL_NOTES, subject::ALL_CHANNELS, "ants.cancel.>", "_INBOX.dev-a.>"]
    {
        let _sub = raw.subscribe(denied).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
    }
}

#[tokio::test]
async fn a_hook_runs_once_per_dead_letter_from_where_its_consumer_left_off() {
    use ants_proto::Feed;
    use async_nats::jetstream::consumer;
    let Some(server) = Server::start() else { return };
    let (audit, dev_a) = (p("audit"), p("dev-a"));
    let creds = server.dir().join("audit.creds");
    issue_with(
        &server.dir().join("auth"),
        &audit,
        crate::revoke::Kind::Observer,
        Permissions::for_observer(&audit),
        &creds,
    )
    .unwrap();
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_, a_js) = server.connect(&dev_a).await;
    let connecting = bus::connect_retrying(&server.url, Some(&creds), None, &audit, "test", std::future::pending());
    let (_, o_js) = timeout(WAIT, connecting).await.expect("audit reaches JetStream").unwrap().unwrap();
    let out = server.dir().join("dead.jsonl");
    let hook = |out: &Path| crate::cmd::hook::Hook {
        event: crate::cmd::hook::Event::DeadLetter,
        command: vec!["sh".into(), "-c".into(), format!("cat >> '{}'", out.display())],
        timeout: Duration::from_secs(10),
        concurrency: 1,
    };
    let bury = |seq: u64, ant: &UserId| ants_proto::DeadLetter::of_payload(ant.clone(), seq, 6, "failed", b"x");
    let lines = async |want: usize| {
        loop {
            let text = std::fs::read_to_string(&out).unwrap_or_default();
            if text.lines().count() >= want {
                return text
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<ants_proto::DeadLetter>>();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };

    let running = tokio::spawn({
        let (o_js, audit, hook) = (o_js.clone(), audit.clone(), hook(&out));
        async move { hook.run(&o_js, &audit).await }
    });
    let reader = Feed::DeadLetters.consumer(&audit);
    while o_js.get_consumer_from_stream::<consumer::pull::Config, _, _>(&reader, STREAM).await.is_err() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // One naming another ant than its subject does is skipped.
    bus::publish_json(&a_js, subject::dead(&dev_a), &bury(1, &p("dev-b")), None).await.unwrap();
    bus::publish_json(&a_js, subject::dead(&dev_a), &bury(2, &dev_a), None).await.unwrap();
    let got = timeout(WAIT, lines(1)).await.expect("the hook ran");
    assert_eq!(got.iter().map(|d| d.stream_seq).collect::<Vec<_>>(), [2]);
    running.abort();
    let _ = running.await;
    // A dead letter the server delivers to the stopped reader's pull request, until it learns the reader is gone, waits
    // out the whole ack wait: the next is stored once no pull request waits and the hook's ack is in.
    timeout(WAIT, async {
        loop {
            let mut reader =
                o_js.get_consumer_from_stream::<consumer::pull::Config, _, _>(&reader, STREAM).await.unwrap();
            let info = reader.info().await.unwrap();
            if info.num_waiting == 0 && info.num_ack_pending == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the stopped reader's pull request and the hook's ack reach the server");

    // A dead letter stored while no hook runs is handed on when one starts again.
    bus::publish_json(&a_js, subject::dead(&dev_a), &bury(3, &dev_a), None).await.unwrap();
    let running = tokio::spawn({
        let (o_js, audit, hook) = (o_js.clone(), audit.clone(), hook(&out));
        async move { hook.run(&o_js, &audit).await }
    });
    let got = timeout(WAIT, lines(2)).await.expect("the hook ran again");
    assert_eq!(got.iter().map(|d| d.stream_seq).collect::<Vec<_>>(), [2, 3]);
    running.abort();
}
