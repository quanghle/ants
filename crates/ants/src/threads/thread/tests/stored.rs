//! Tests of threads read back from the hub: without consuming them, with their tool calls and requests, how each
//! run of a prompt ended, and sealed messages.

use super::*;

#[tokio::test]
async fn a_human_reads_threads_back_without_consuming_them() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it").with_owner(Some(alice.clone()));
    let reply = ask.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
    let note = NotesPost::new(ask.thread_id.clone(), alice.clone(), "remember the tests");
    let other = Envelope::new(alice.clone(), dev_a.clone(), "another thing");
    for (subject, payload) in [
        (ask.subject(), json(&ask)),
        (reply.subject(), json(&reply)),
        (note.subject(), json(&note)),
        (other.subject(), json(&other)),
    ] {
        js.publish(subject, payload.into()).await.unwrap().await.unwrap();
    }

    let mut threads = Threads::new(None);
    scan(&js, window(Duration::from_secs(3600)), |e| threads.add(&e)).await.unwrap();
    let (threads, _) = threads.finish(&Select::default(), &ThreadMarks::default());
    assert_eq!(threads.len(), 2);
    let built = threads.iter().find(|t| t.thread_id == ask.thread_id).unwrap();
    assert_eq!((built.messages, built.owner.as_ref()), (3, Some(&alice)));

    let mut shown = Vec::new();
    scan(&js, show_start(&ask.thread_id, None), |e| {
        if e.thread_id == ask.thread_id {
            shown.push(e.kind);
        }
    })
    .await
    .unwrap();
    assert_eq!(shown, [Kind::Message, Kind::Message, Kind::Note]);

    let stream = js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().clone();
    assert_eq!((stream.state.messages, stream.state.consumer_count), (4, 0));
}

#[tokio::test]
async fn a_thread_shows_its_ants_tool_calls_once_each_as_last_stored() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, ant_js) = server.connect_as(&dev_a, ants_proto::Role::Ant).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    js.publish(ask.subject(), json(&ask).into()).await.unwrap().await.unwrap();
    let started = Utc::now();
    let call = |id: &str, status: &str, duration_ms| {
        ants_proto::TimelineEntry::Tool(ants_proto::TimelineCall {
            id: id.to_owned(),
            tool_kind: Some("execute".to_owned()),
            title: format!("cargo {id}"),
            status: Some(status.to_owned()),
            started,
            duration_ms,
            paths: Vec::new(),
        })
    };
    let record = |entries| Timeline::new(dev_a.clone(), ask.thread_id.clone(), ask.id, entries, 0);
    for record in [
        record(vec![call("build", "in_progress", None)]),
        record(vec![call("build", "completed", Some(2100)), call("test", "failed", Some(840))]),
    ] {
        ant_js.publish(record.subject(), json(&record).into()).await.unwrap().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let reply = ask.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
    js.publish(reply.subject(), json(&reply).into()).await.unwrap().await.unwrap();
    let why = ants_proto::NoReplyWhy::ReplyToOwn;
    let end = record(vec![ants_proto::TimelineEntry::NoReply { why, reason: None, text: "noted".to_owned() }]);
    ant_js.publish(end.subject(), json(&end).into()).await.unwrap().await.unwrap();

    let shown = history(&js, &ask.thread_id, None, Activity::All, None).await.unwrap();
    let kinds: Vec<_> = shown.iter().map(|e| (e.kind, e.text.as_str())).collect();
    assert_eq!(
        kinds,
        [
            (Kind::Message, "build it"),
            (Kind::Tool, "cargo build"),
            (Kind::Tool, "cargo test"),
            (Kind::Message, "built"),
            (Kind::NoReply, "noted"),
        ]
    );
    let rendered = render(&shown[..3]);
    assert!(rendered.contains(" dev-a  Run(cargo build) · 2.1s\n    "), "{rendered}");
    assert!(rendered.contains("\n  build it\n  ⎿ "), "{rendered}");
    assert!(rendered.ends_with(" dev-a  Run(cargo test) · failed after 840ms\n"), "{rendered}");
    let line = serde_json::to_value(&shown[2]).unwrap();
    assert_eq!(line["tool"]["status"], "failed");
    assert_eq!(line["tool"]["duration_ms"], 840);
    assert_eq!(line["tool"]["message"], ask.id.to_string());
    let without = history(&js, &ask.thread_id, None, Activity::Ends, None).await.unwrap();
    let kinds: Vec<_> = without.iter().map(|e| e.kind).collect();
    assert_eq!(kinds, [Kind::Message, Kind::Message, Kind::NoReply], "--no-tools keeps the ends with no reply");
    let messages = history(&js, &ask.thread_id, None, Activity::None, None).await.unwrap();
    assert!(messages.iter().all(|e| e.kind == Kind::Message), "{messages:?}");
    assert!(!Kind::MESSAGES.contains(&Kind::Tool), "tool calls are not searched");
}

#[tokio::test]
async fn a_thread_shows_rejected_and_expired_tool_requests_once() {
    use ants_proto::TimelineRejection;

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, ant_js) = server.connect_as(&dev_a, ants_proto::Role::Ant).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    js.publish(ask.subject(), json(&ask).into()).await.unwrap().await.unwrap();
    let store = async |message, entries| {
        let record = Timeline::new(dev_a.clone(), ask.thread_id.clone(), message, entries, 0);
        ant_js.publish(record.subject(), json(&record).into()).await.unwrap().await.unwrap().sequence
    };
    let rejected = TimelineRejection {
        request: Uuid::now_v7(),
        kind: "execute".into(),
        title: "Run `cargo build`".into(),
        by: DecidedBy::Human,
        decider: Some(alice),
        via: Some(p("alice-assistant")),
    };
    let earlier = TimelineRejection { title: "old title".into(), ..rejected.clone() };
    store(Uuid::nil(), vec![TimelineEntry::Rejected(earlier)]).await;
    let seq = store(ask.id, vec![TimelineEntry::Rejected(rejected.clone())]).await;
    let chained = TimelineRejection {
        request: Uuid::now_v7(),
        kind: "read".into(),
        title: "Read src/main.rs".into(),
        by: DecidedBy::Chain,
        decider: Some(p("dev-b")),
        via: None,
    };
    let expired =
        TimelineRejection { request: Uuid::now_v7(), by: DecidedBy::Expiry, decider: None, ..chained.clone() };
    store(ask.id, vec![TimelineEntry::Rejected(chained.clone()), TimelineEntry::Rejected(expired.clone())]).await;

    let shown = history(&js, &ask.thread_id, None, Activity::All, None).await.unwrap();
    let kinds: Vec<_> = shown.iter().map(|e| e.kind).collect();
    assert_eq!(kinds, [Kind::Message, Kind::Rejected, Kind::Rejected, Kind::Rejected]);
    let text = render(&shown);
    assert!(text.contains("\n  build it\n  ⎿ "), "{text}");
    for (label, request, outcome) in [
        ("Run(cargo build)", rejected.request, "rejected by alice via alice-assistant"),
        ("Read(src/main.rs)", chained.request, "rejected by dev-b (approval chain)"),
        ("Read(src/main.rs)", expired.request, "expired"),
    ] {
        assert!(text.contains(&format!("dev-a  {label} · request {request} {outcome}\n")), "{text}");
    }

    let line = serde_json::to_value(&shown[1]).unwrap();
    assert_eq!(line["kind"], "rejected");
    assert_eq!(line["seq"], seq);
    assert_eq!(line["text"], rejected.title);
    assert_eq!(
        line["rejected"],
        serde_json::json!({
            "message": ask.id, "request": rejected.request, "tool_kind": "execute",
            "by": "human", "decider": "alice", "via": "alice-assistant"
        })
    );
    let line = serde_json::to_value(&shown[3]).unwrap();
    assert_eq!(
        line["rejected"],
        serde_json::json!({"message": ask.id, "request": expired.request, "tool_kind": "read", "by": "expiry"})
    );
    let without = history(&js, &ask.thread_id, None, Activity::Ends, None).await.unwrap();
    assert_eq!(without.len(), 1, "--no-tools omits rejections: {without:?}");
    assert!(!Kind::MESSAGES.contains(&Kind::Rejected));
}

#[tokio::test]
async fn a_thread_shows_how_each_run_of_a_prompt_ended() {
    use std::collections::BTreeMap;

    use ants_proto::{CallCount, PromptEnd, TimelineEnd};

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, ant_js) = server.connect_as(&dev_a, ants_proto::Role::Ant).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    js.publish(ask.subject(), json(&ask).into()).await.unwrap().await.unwrap();
    let failed = TimelineEnd {
        how: PromptEnd::Failed,
        duration_ms: 4200,
        tokens: 0,
        cost: 0.0,
        asked: 0,
        rebuilt: false,
        calls: BTreeMap::new(),
        sense_calls: 0,
    };
    let calls = [("execute", CallCount { calls: 3, failed: 1 }), ("other", CallCount { calls: 2, failed: 0 })];
    let replied = TimelineEnd {
        how: PromptEnd::Replied,
        duration_ms: 65_000,
        tokens: 1200,
        cost: 0.42,
        asked: 1,
        rebuilt: true,
        calls: calls.into_iter().map(|(kind, n)| (kind.to_owned(), n)).collect(),
        sense_calls: 2,
    };
    let mut seqs = Vec::new();
    for end in [failed, replied] {
        let record = Timeline::new(dev_a.clone(), ask.thread_id.clone(), ask.id, vec![TimelineEntry::Ended(end)], 0);
        seqs.push(ant_js.publish(record.subject(), json(&record).into()).await.unwrap().await.unwrap().sequence);
    }

    let shown = history(&js, &ask.thread_id, None, Activity::All, None).await.unwrap();
    let kinds: Vec<_> = shown.iter().map(|e| (e.kind, e.seq)).collect();
    assert_eq!(kinds, [(Kind::Message, 1), (Kind::Ended, seqs[0]), (Kind::Ended, seqs[1])]);
    let text = render(&shown);
    assert!(text.contains("\n  ⎿ "), "{text}");
    assert!(text.contains(" dev-a  ended: failed · 4.2s\n"), "{text}");
    let replied = " dev-a  ended: replied · 1m5s · 1200 tokens · $0.42 · calls execute 3 (1 failed), other 2 · 2 sense \
                   calls · 1 tool request asked · session rebuilt\n";
    assert!(text.ends_with(replied), "{text}");
    let line = serde_json::to_value(&shown[2]).unwrap();
    assert_eq!(line["kind"], "ended");
    assert_eq!(
        line["ended"],
        serde_json::json!({
            "message": ask.id, "how": "replied", "duration_ms": 65_000, "tokens": 1200, "cost": 0.42, "asked": 1,
            "rebuilt": true, "calls": {"execute": {"calls": 3, "failed": 1}, "other": {"calls": 2}}, "sense_calls": 2,
        })
    );
    let without = history(&js, &ask.thread_id, None, Activity::Ends, None).await.unwrap();
    assert_eq!(without.len(), 3, "--no-tools keeps the ends: {without:?}");
    let live = history(&js, &ask.thread_id, None, Activity::Calls, None).await.unwrap();
    assert_eq!(live.len(), 1, "{live:?}");
    assert!(!Kind::MESSAGES.contains(&Kind::Ended));
}

#[tokio::test]
async fn a_sealed_message_from_a_device_enrolled_mid_follow_opens_everywhere_it_is_read() {
    use ants_proto::Role;

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, bob) = (p("alice"), p("bob"));
    let enrolled = |name: &str| {
        let creds = server.dir().join(format!("alice-{name}.creds"));
        crate::test_support::issue(&server.dir().join("auth"), &alice, Role::Human, None, &creds).unwrap();
        creds
    };
    let desk = enrolled("desk");
    crate::connection::Nats::new(&server.url, Some(&desk), None, None).unwrap().connect("test").await.unwrap();
    let bob_creds = server.creds_as(&bob, Role::Human);
    let bob_nats = crate::connection::Nats::new(&server.url, Some(&bob_creds), None, None).unwrap();
    let (_, bob_js) = bob_nats.connect("test").await.unwrap();
    // One keyring per read, each loaded before the device was enrolled and learning it on its own.
    let load = async || crate::people::sealed::Keyring::load(&bob_nats, &bob_js).await.map(Arc::new);
    let (keyring, for_history, for_search) = (load().await, load().await, load().await);
    assert!(keyring.is_some());

    // Bob follows the inbox; meanwhile alice enrolls a second device and asks from it.
    let mut inbox = crate::messages::inbox::Inbox::open(&bob_js, &bob).await.unwrap();
    let select = crate::messages::inbox::Select { limit: Some(1), ..Default::default() };
    let mut followed = Vec::new();
    let follow = inbox.follow(
        &select,
        keyring.as_deref(),
        None,
        |e| {
            followed.push(crate::people::sealed::read(e.clone(), keyring.as_deref()));
            Ok(())
        },
        |_| Ok(()),
        tokio::time::sleep(Duration::from_secs(20)),
    );
    let asked = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let laptop = enrolled("laptop");
        let alice_nats = crate::connection::Nats::new(&server.url, Some(&laptop), None, None).unwrap();
        alice_nats.connect("test").await.unwrap();
        let words = vec!["@bob".to_owned(), "lunch at noon?".to_owned()];
        let args = crate::cmd::send::AskArgs { words, ..Default::default() };
        Box::pin(crate::cmd::send::ask(&alice_nats, args)).await.unwrap();
    };
    let (follow, ()) = tokio::join!(follow, asked);
    follow.unwrap();
    let [(e, state)] = followed.as_slice() else { panic!("not one message: {followed:?}") };
    assert_eq!((e.text.as_str(), *state), ("lunch at noon?", Some(ants_client::sealed::State::Opened)));

    // The thread's history and a search open it too.
    let shown = history(&bob_js, &e.thread_id, None, Activity::None, for_history).await.unwrap();
    let opened: Vec<_> = shown.iter().map(|e| (e.text.as_str(), e.sealed)).collect();
    assert_eq!(opened, [("lunch at noon?", Some(ants_client::sealed::State::Opened))]);
    let search = crate::threads::search::Search { grep: Some("lunch".into()), limit: 10, ..Default::default() };
    let (found, _) = search.run(&bob_js, Parser::opening(for_search)).await.unwrap();
    assert_eq!(found.iter().map(|e| e.text.as_str()).collect::<Vec<_>>(), ["lunch at noon?"]);
}
