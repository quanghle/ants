//! Tests of the service notifying humans: test notices, tool requests and escalations decided on a link, pushes
//! to the devices not following, held notices, folded replies and digests.

use super::*;

/// A notification service on loopback that takes every `POST`, handing over its path and JSON body.
async fn sink() -> (SocketAddr, mpsc::UnboundedReceiver<(String, Value)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let tx = tx.clone();
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let tx = tx.clone();
                async move {
                    let path = request.uri().path().to_owned();
                    let body = request.into_body().collect().await.unwrap().to_bytes();
                    let _ = tx.send((path, serde_json::from_slice(&body).unwrap()));
                    Ok::<_, Infallible>(reply(StatusCode::OK, "{}"))
                }
            });
            tokio::spawn(hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service));
        }
    });
    (addr, rx)
}

async fn next(posts: &mut mpsc::UnboundedReceiver<(String, Value)>) -> (String, Value) {
    tokio::time::timeout(Duration::from_secs(10), posts.recv()).await.expect("a notification").unwrap()
}

fn private(path: &Path, text: &str) {
    std::fs::write(path, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[tokio::test]
async fn a_test_notice_goes_to_every_sink_and_each_failure_is_reported() {
    let (sink, mut posts) = sink().await;
    let gone = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gone_addr = gone.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = gone.accept().await {
            let mut head = [0; 4096];
            let _ = stream.read(&mut head).await;
            let answer = "HTTP/1.1 404 Not Found\r\ncontent-length: 10\r\nconnection: close\r\n\r\nno_service";
            let _ = stream.write_all(answer.as_bytes()).await;
        }
    });
    let dir = ants_test_support::scratch_dir("webhook-notify-test");
    private(&dir.join("bot"), "123:abc\n");
    private(&dir.join("slack"), &format!("http://{gone_addr}/services/T0/B0/x\n"));
    let path = dir.join("webhooks.toml");
    let config = format!(
        "[[human]]\nname = \"alice\"\napprovals = false\n\
             [[human.notify]]\nkind = \"ntfy\"\ntopic = \"alerts\"\nurl = \"http://{sink}\"\n\
             [[human.notify]]\nkind = \"telegram\"\nchat = \"42\"\nurl = \"http://{sink}\"\nsecret_file = \"bot\"\n\
             [[human.notify]]\nkind = \"slack\"\nsecret_file = \"slack\"\n"
    );
    std::fs::write(&path, config).unwrap();

    let config = Config::read(&path).unwrap();
    let results = notify::test(&config.humans[0], "laptop").await;
    let shown: Vec<(&str, String)> = results
        .iter()
        .map(|(sink, sent)| (sink.as_str(), sent.as_ref().map_or_else(ToString::to_string, |()| "ok".into())))
        .collect();
    assert_eq!(
        shown,
        [
            ("ntfy topic alerts", "ok".to_owned()),
            ("telegram chat 42", "ok".to_owned()),
            ("slack", "answered 404 Not Found: no_service".to_owned()),
        ]
    );
    let mut got = [next(&mut posts).await, next(&mut posts).await];
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got[0].0, "/", "ntfy");
    assert_eq!(got[0].1["title"], "ants test notice for alice");
    assert_eq!(got[1].0, "/bot123:abc/sendMessage");
    assert!(got[1].1["text"].as_str().unwrap().contains("on laptop"), "{}", got[1].1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_human_is_notified_of_tool_requests_and_escalations_and_decides_on_a_link() {
    let Some((server, js, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (action_secret, bot_token) = (server.dir().join("action.secret"), server.dir().join("bot.token"));
    private(&action_secret, "0123456789abcdef-secret");
    private(&bot_token, "123:abc");
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "public_url = \"https://hub.example/hooks\"\naction_secret_file = {action_secret:?}\n\
                 [[human]]\nname = \"alice\"\ncreds = {creds:?}\n\
                 [[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"alice-ants\"\n\
                 [[human.notify]]\nkind = \"telegram\"\nurl = \"http://{sink}\"\nchat = \"42\"\nsecret_file = {bot_token:?}\n"
        ),
    )
    .unwrap();
    let (_ingress, addr) = start(&config, &server.url).await;
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let stream = own.get_stream(STREAM).await.unwrap();
    let consumer = ants_proto::notify_consumer(&alice);
    for _ in 0..100 {
        if stream.consumer_info(&consumer).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (_, ant) = server.connect(&dev_a).await;
    let tool = ants_proto::ToolCall {
        kind: "execute".to_owned(),
        title: "cargo publish".to_owned(),
        locations: Vec::new(),
        input: None,
    };
    let options = vec![ants_proto::Verdict::AllowOnce, ants_proto::Verdict::AllowAlways];
    let asked =
        ants_proto::ApprovalRequest::new(dev_a.clone(), ThreadId::generate(), tool, options, Duration::from_secs(600));
    ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();

    let mut sent = HashMap::new();
    for _ in 0..2 {
        let (path, body) = next(&mut posts).await;
        sent.insert(path, body);
    }
    let ntfy = &sent["/"];
    assert_eq!(
        (ntfy["topic"].as_str(), ntfy["title"].as_str()),
        (Some("alice-ants"), Some("dev-a asks to run a tool"))
    );
    let telegram = &sent["/bot123:abc/sendMessage"];
    assert_eq!(telegram["chat_id"], "42");
    let link = |i: usize| {
        let url = ntfy["actions"][i]["url"].as_str().unwrap();
        assert_eq!(telegram["reply_markup"]["inline_keyboard"][0][i]["url"].as_str(), Some(url));
        url.strip_prefix("https://hub.example").unwrap().to_owned()
    };
    let (approve, reject) = (link(0), link(2));
    assert!(approve.starts_with("/hooks/_approval?") && approve.contains("verdict=allow_once"), "{approve}");

    let shown = request(addr, "GET", &approve, &[], b"").await;
    assert_eq!(shown.status, 200, "{}", shown.body);
    assert!(shown.body.contains("cargo publish") && shown.body.contains("<form method=\"post\""), "{}", shown.body);
    let decisions = |js: jetstream::Context| {
        let subject = asked.decision_subject();
        async move { stored::<ants_proto::ApprovalDecision>(&js, &subject).await }
    };
    assert!(decisions(js.clone()).await.is_empty(), "showing decides nothing");
    let forged = approve.replace("verdict=allow_once", "verdict=allow_always");
    assert_eq!(request(addr, "POST", &forged, &[], b"").await.status, 403);
    assert_eq!(request(addr, "POST", "/hooks/_approval?ant=dev-a", &[], b"").await.status, 400);
    assert_eq!(request(addr, "PUT", &approve, &[], b"").await.status, 405);

    let approved = request(addr, "POST", &approve, &[], b"").await;
    assert_eq!(approved.status, 200, "{}", approved.body);
    let [decision] = &decisions(js.clone()).await[..] else { panic!("one decision") };
    assert_eq!((decision.request_id, decision.verdict), (asked.id, ants_proto::Verdict::AllowOnce));
    let late = request(addr, "POST", &reject, &[], b"").await;
    assert_eq!(late.status, 409, "the first answer stands: {}", late.body);
    assert_eq!(decisions(js.clone()).await.len(), 1);
    let settled = request(addr, "GET", &reject, &[], b"").await;
    assert!(settled.body.contains("Already answered") && !settled.body.contains("<form"), "{}", settled.body);

    let plain = Envelope::new(dev_a.clone(), alice.clone(), "done");
    ant.publish(plain.subject(), serde_json::to_vec(&plain).unwrap().into()).await.unwrap().await.unwrap();
    let replied =
        Envelope::new(alice.clone(), dev_a.clone(), "build it").reply(dev_a.clone(), "built", ants_proto::Role::Human);
    let replied = replied.unwrap();
    ant.publish(replied.subject(), serde_json::to_vec(&replied).unwrap().into()).await.unwrap().await.unwrap();
    let escalation =
        ants_proto::Escalation::new(dev_a.clone(), plain.thread_id.clone(), ants_proto::EscalationReason::Asked);
    let escalated = Envelope::new(dev_a.clone(), alice.clone(), "which database?").with_escalation(Some(escalation));
    ant.publish(escalated.subject(), serde_json::to_vec(&escalated).unwrap().into()).await.unwrap().await.unwrap();
    let (path, body) = next(&mut posts).await;
    assert_eq!(path, "/", "plain messages, and replies unless completions = true, are not notified");
    assert_eq!(body["title"], "dev-a escalates: asked on dev-a");
    assert!(body["message"].as_str().unwrap().starts_with("which database?"), "{body}");
    assert!(body.get("actions").is_none());

    question_is_pushed(&ant, &mut posts, Envelope::asking(plain.thread_id, dev_a, alice, which_database())).await;
}

fn which_database() -> ants_proto::Question {
    ants_proto::Question::new("Which database?", &["Postgres".to_owned(), "SQLite".to_owned()], true).unwrap()
}

/// Publish `asking` as `ant` and check that the ntfy sink gets its question.
async fn question_is_pushed(
    ant: &jetstream::Context,
    posts: &mut mpsc::UnboundedReceiver<(String, Value)>,
    asking: Envelope,
) {
    ant.publish(asking.subject(), serde_json::to_vec(&asking).unwrap().into()).await.unwrap().await.unwrap();
    let body = loop {
        // The escalation's telegram message may come first.
        let (path, body) = next(posts).await;
        if path == "/" {
            break body;
        }
    };
    assert_eq!(body["title"], "dev-a asks: Which database?");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("1. Postgres") && message.contains(&format!("ants answer {} NUMBER", asking.id)),
        "{body}"
    );
}

/// The topics of the next `n` notifications, with their titles, sorted.
async fn topics(posts: &mut mpsc::UnboundedReceiver<(String, Value)>, n: usize) -> Vec<(String, String)> {
    let mut got = Vec::new();
    for _ in 0..n {
        let (_, body) = next(posts).await;
        got.push((body["topic"].as_str().unwrap().to_owned(), body["title"].as_str().unwrap().to_owned()));
    }
    got.sort();
    got
}

#[tokio::test]
async fn a_device_following_what_a_notice_is_about_gets_no_push_and_the_others_do() {
    use crate::people::presence;
    use crate::test_support::presence::device;
    use ants_proto::Following;

    let Some((server, _, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (laptop, laptop_id) = device(&server, &alice, "laptop").await;
    let (_, phone_id) = device(&server, &alice, "phone").await;
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    let target = |topic: &str, device: String| {
        format!("[[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"{topic}\"\n{device}")
    };
    std::fs::write(
        &config,
        format!(
            "[[human]]\nname = \"alice\"\ncreds = {creds:?}\n{}{}{}",
            target("laptop", format!("device = \"{laptop_id}\"\n")),
            target("phone", format!("device = \"{phone_id}\"\n")),
            target("any", String::new()),
        ),
    )
    .unwrap();
    let (_ingress, _) = start(&config, &server.url).await;
    let stream = laptop.get_stream(STREAM).await.unwrap();
    for _ in 0..100 {
        if stream.consumer_info(&ants_proto::notify_consumer(&alice)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, ant) = server.connect(&dev_a).await;
    let escalate = async |text: &str| {
        let thread = ThreadId::generate();
        let escalation = ants_proto::Escalation::new(dev_a.clone(), thread, ants_proto::EscalationReason::Asked);
        let escalated = Envelope::new(dev_a.clone(), alice.clone(), text).with_escalation(Some(escalation));
        ant.publish(escalated.subject(), serde_json::to_vec(&escalated).unwrap().into()).await.unwrap().await.unwrap();
    };
    let ask = async || {
        let tool = ants_proto::ToolCall { kind: "execute".into(), title: "make".into(), ..Default::default() };
        let options = vec![ants_proto::Verdict::AllowOnce];
        let asked = ants_proto::ApprovalRequest::new(
            dev_a.clone(),
            ThreadId::generate(),
            tool,
            options,
            Duration::from_secs(600),
        );
        ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();
    };
    let pair = |topic: &str, title: &str| (topic.to_owned(), title.to_owned());
    let (escalated, asked) = ("dev-a escalates: asked on dev-a", "dev-a asks to run a tool");

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let following = presence::while_following(&laptop, None, &alice, Some(laptop_id), Following::Inbox, async move {
        _ = stop_rx.await;
    });
    let check = async {
        let present = presence::Present::new(&laptop, None).await.unwrap();
        for _ in 0..50 {
            if !present.following(&alice, [laptop_id], Following::Inbox).await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        escalate("which database?").await;
        assert_eq!(
            topics(&mut posts, 2).await,
            [pair("any", escalated), pair("phone", escalated)],
            "the laptop shows the escalation in its inbox"
        );
        ask().await;
        assert_eq!(
            topics(&mut posts, 3).await,
            [pair("any", asked), pair("laptop", asked), pair("phone", asked)],
            "the laptop follows no tool requests"
        );
        stop_tx.send(()).unwrap();
    };
    tokio::join!(following, check);
    escalate("and now?").await;
    assert_eq!(
        topics(&mut posts, 3).await,
        [pair("any", escalated), pair("laptop", escalated), pair("phone", escalated)],
        "the laptop stopped following"
    );
}

#[tokio::test]
async fn held_notices_are_dropped_unread_and_undecided_and_each_reply_is_notified_once() {
    let Some((server, js, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "[[human]]\nname = \"alice\"\ncreds = {creds:?}\ncompletions = true\ncompletion_window = \"0\"\nbypass = [\"escalations\"]\n\
                 [[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"alice\"\n"
        ),
    )
    .unwrap();
    // Two services listing alice, which share her consumer.
    let (_one, _) = start(&config, &server.url).await;
    let (_two, _) = start(&config, &server.url).await;
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let stream = own.get_stream(STREAM).await.unwrap();
    for _ in 0..100 {
        if stream.consumer_info(&ants_proto::notify_consumer(&alice)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, ant) = server.connect(&dev_a).await;
    let send = async |envelope: &Envelope| {
        ant.publish(envelope.subject(), serde_json::to_vec(envelope).unwrap().into()).await.unwrap().await.unwrap();
    };
    let reply = |thread: &ThreadId, text: &str| {
        let asked = Envelope::on_thread(thread.clone(), alice.clone(), dev_a.clone(), "go on");
        asked.reply(dev_a.clone(), text, ants_proto::Role::Human).unwrap()
    };
    let messages = async |posts: &mut mpsc::UnboundedReceiver<(String, Value)>, n: usize| {
        let mut got = Vec::new();
        for _ in 0..n {
            let (_, body) = next(posts).await;
            let first = body["message"].as_str().unwrap().lines().next().unwrap_or_default().to_owned();
            got.push((body["title"].as_str().unwrap().to_owned(), first));
        }
        got.sort();
        got
    };
    let replied = |text: &str| ("dev-a replied".to_owned(), text.to_owned());
    let hold = async |change: &(dyn Fn(&mut ants_proto::NotifyPrefs, chrono::DateTime<chrono::Utc>) + Sync)| {
        crate::people::quiet::update(&own, &alice, change).await.unwrap();
    };
    let (thread, muted) = (ThreadId::generate(), ThreadId::generate());

    send(&reply(&thread, "first")).await;
    send(&reply(&thread, "second")).await;
    assert_eq!(messages(&mut posts, 2).await, [replied("first"), replied("second")]);

    hold(&|prefs, now| prefs.snoozed_until = Some(now + chrono::TimeDelta::hours(1))).await;
    let tool = ants_proto::ToolCall { kind: "execute".into(), title: "make".into(), ..Default::default() };
    let options = vec![ants_proto::Verdict::AllowOnce];
    let asked =
        ants_proto::ApprovalRequest::new(dev_a.clone(), thread.clone(), tool, options, Duration::from_secs(600));
    ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();
    send(&reply(&thread, "while snoozed")).await;
    let escalation = ants_proto::Escalation::new(dev_a.clone(), thread.clone(), ants_proto::EscalationReason::Asked);
    send(&Envelope::new(dev_a.clone(), alice.clone(), "which database?").with_escalation(Some(escalation))).await;
    assert_eq!(
        messages(&mut posts, 1).await,
        [("dev-a escalates: asked on dev-a".to_owned(), "which database?".to_owned())],
        "escalations bypass the snooze"
    );

    hold(&|prefs, now| {
        prefs.snoozed_until = None;
        prefs.muted.insert(muted.clone(), now + chrono::TimeDelta::hours(1));
    })
    .await;
    send(&reply(&muted, "on the muted thread")).await;
    let escalation = ants_proto::Escalation::new(dev_a.clone(), muted.clone(), ants_proto::EscalationReason::Asked);
    send(&Envelope::new(dev_a.clone(), alice.clone(), "muted anyway").with_escalation(Some(escalation))).await;
    send(&reply(&thread, "elsewhere")).await;
    assert_eq!(messages(&mut posts, 1).await, [replied("elsewhere")], "a mute holds back escalations too");

    // A snooze whose deadline passed holds nothing back, with no one there to lift it.
    hold(&|prefs, now| prefs.snoozed_until = Some(now - chrono::TimeDelta::seconds(1))).await;
    send(&reply(&thread, "after the snooze")).await;
    assert_eq!(messages(&mut posts, 1).await, [replied("after the snooze")]);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), posts.recv()).await.is_err(),
        "nothing sent twice, nothing held sent later"
    );

    assert!(stored::<ants_proto::ApprovalDecision>(&js, &asked.decision_subject()).await.is_empty(), "undecided");
    let seq = stream.get_last_raw_message_by_subject(&asked.subject()).await.unwrap().sequence;
    let state = crate::tools::state(&own, &stream, seq, &asked, chrono::Utc::now()).await.unwrap();
    assert_matches!(state, crate::tools::State::Pending, "pending");
    if let Ok(read) = js.get_key_value(ants_proto::READ_BUCKET).await {
        assert!(read.entry(alice.as_str()).await.unwrap().is_none(), "nothing marked read");
    }
}

#[tokio::test]
async fn the_replies_on_a_thread_are_notified_once_it_is_quiet_folded_into_one() {
    let Some((server, _, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "[[human]]\nname = \"alice\"\ncreds = {creds:?}\ncompletions = true\ncompletion_window = \"1s\"\n\
                 [[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"alice\"\n"
        ),
    )
    .unwrap();
    let (_service, _) = start(&config, &server.url).await;
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let stream = own.get_stream(STREAM).await.unwrap();
    for _ in 0..100 {
        if stream.consumer_info(&ants_proto::notify_consumer(&alice)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, ant) = server.connect(&dev_a).await;
    let (busy, other) = (ThreadId::generate(), ThreadId::generate());
    for (thread, text) in [(&busy, "starting"), (&other, "other"), (&busy, "done")] {
        let asked = Envelope::on_thread(thread.clone(), alice.clone(), dev_a.clone(), "go on");
        let reply = asked.reply(dev_a.clone(), text, ants_proto::Role::Human).unwrap();
        ant.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap().await.unwrap();
    }
    let mut got = Vec::new();
    for _ in 0..2 {
        let (_, body) = next(&mut posts).await;
        let first = body["message"].as_str().unwrap().lines().next().unwrap_or_default().to_owned();
        got.push((body["title"].as_str().unwrap().to_owned(), first));
    }
    got.sort();
    let pair = |title: &str, text: &str| (title.to_owned(), text.to_owned());
    assert_eq!(got, [pair("dev-a replied", "other"), pair("dev-a replied (2 replies on the thread)", "done")]);
    assert!(tokio::time::timeout(Duration::from_millis(1500), posts.recv()).await.is_err(), "each thread once");
}

#[tokio::test]
async fn one_digest_of_what_a_snooze_held_back_follows_its_end_across_services() {
    let Some((server, _, _)) = hub().await else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let (sink, mut posts) = sink().await;
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "[[human]]\nname = \"alice\"\ncreds = {creds:?}\ncompletions = true\ncompletion_window = \"0\"\ndigest = true\n\
                 [[human.notify]]\nkind = \"ntfy\"\nurl = \"http://{sink}\"\ntopic = \"alice\"\n"
        ),
    )
    .unwrap();
    let (_one, _) = start(&config, &server.url).await;
    let (_two, _) = start(&config, &server.url).await;
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &alice, "test").await.unwrap();
    let stream = own.get_stream(STREAM).await.unwrap();
    for _ in 0..100 {
        if stream.consumer_info(&ants_proto::notify_consumer(&alice)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_, ant) = server.connect(&dev_a).await;
    let hold = async |change: &(dyn Fn(&mut ants_proto::NotifyPrefs, chrono::DateTime<chrono::Utc>) + Sync)| {
        crate::people::quiet::update(&own, &alice, change).await.unwrap();
    };
    let thread = ThreadId::generate();
    hold(&|prefs, now| prefs.snoozed_until = Some(now + chrono::TimeDelta::hours(1))).await;
    let tool = ants_proto::ToolCall { kind: "execute".into(), title: "make".into(), ..Default::default() };
    let asked = ants_proto::ApprovalRequest::new(
        dev_a.clone(),
        thread.clone(),
        tool,
        vec![ants_proto::Verdict::AllowOnce],
        Duration::from_secs(600),
    );
    ant.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();
    for text in ["one", "two"] {
        let question = Envelope::on_thread(thread.clone(), alice.clone(), dev_a.clone(), "go on");
        let reply = question.reply(dev_a.clone(), text, ants_proto::Role::Human).unwrap();
        ant.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap().await.unwrap();
    }

    let notify = own.get_key_value(ants_proto::NOTIFY_BUCKET).await.unwrap();
    let key = ants_proto::held_key(&alice);
    let mut counts = std::collections::BTreeMap::new();
    for _ in 0..100 {
        if let Some(value) = notify.get(&key).await.unwrap() {
            counts = serde_json::from_slice::<ants_proto::HeldNotices>(&value).unwrap().counts;
            if counts.values().sum::<u32>() == 3 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(counts, std::collections::BTreeMap::from([("approval".to_owned(), 1), ("completion".to_owned(), 2)]));
    assert!(tokio::time::timeout(Duration::from_millis(500), posts.recv()).await.is_err(), "held back while snoozed");

    hold(&|prefs, _| prefs.snoozed_until = None).await;
    let (_, digest) = next(&mut posts).await;
    assert_eq!(digest["title"], "3 notices held back");
    let text = digest["message"].as_str().unwrap();
    assert!(text.starts_with("1 tool request, 2 replies held back by quiet hours or a snooze since "), "{text}");
    assert!(text.contains(&format!("1 tool request still pending:\n{} make", crate::tools::qualified(&asked))));
    assert!(tokio::time::timeout(Duration::from_millis(1000), posts.recv()).await.is_err(), "one digest, sent once");
    assert!(notify.get(&key).await.unwrap().is_none(), "claimed");
    let seq = stream.get_last_raw_message_by_subject(&asked.subject()).await.unwrap().sequence;
    let state = crate::tools::state(&own, &stream, seq, &asked, chrono::Utc::now()).await.unwrap();
    assert_matches!(state, crate::tools::State::Pending, "still pending");
}
