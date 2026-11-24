//! Tests of picking a thread or a message: by a whole id, a start of one or the last, and where a read starts.

use super::*;

#[test]
fn show_starts_at_a_time_ordered_thread_s_start_or_reads_everything() {
    assert_matches!(show_start(&t("t1"), None), consumer::DeliverPolicy::All);
    assert_matches!(show_start(&ThreadId::generate(), None), consumer::DeliverPolicy::ByStartTime { .. });
    assert_matches!(show_start(&t("t1"), Some(Duration::from_secs(60))), consumer::DeliverPolicy::ByStartTime { .. });
    assert_matches!(window(Duration::ZERO), consumer::DeliverPolicy::All);
}

#[test]
fn a_pick_is_a_whole_uuid_a_start_of_an_id_or_last() {
    let id = ThreadId::generate();
    let pick = |s: &str| s.parse::<Pick>();
    assert_eq!(pick(&id.as_str().to_uppercase()).unwrap(), Pick::Id(id.clone()));
    assert_eq!(pick(&id.as_str()[..8]).unwrap(), Pick::Prefix(id.as_str()[..8].to_owned()));
    assert_eq!(pick("t1").unwrap(), Pick::Prefix("t1".to_owned()));
    assert_eq!(pick("LAST").unwrap(), Pick::Last);
    assert!(pick("no_such").is_err());
}

#[test]
fn a_start_of_an_id_picks_one_recent_thread_or_lists_them() {
    let alice = p("alice");
    let entry = |thread: &str, secs| Entry {
        from: Some(alice.clone()),
        to: Some(p("dev-a")),
        text: format!("on {thread}\nmore"),
        ..Entry::new(secs as u64, at(secs), Kind::Message, t(thread))
    };
    // The latest message of each thread, most recent first.
    let threads = [entry("ab12", 3), entry("ab1", 2), entry("cd34", 1)];
    assert_eq!(select(&threads, "ab1", &alice).unwrap(), Some(t("ab1")), "a whole id wins");
    assert_eq!(select(&threads, "c", &alice).unwrap(), Some(t("cd34")));
    assert_eq!(select(&threads, "ef", &alice).unwrap(), None);

    let several = select(&threads, "ab", &alice).unwrap_err().to_string();
    assert!(several.starts_with("2 threads alice took part in over the last 7 days start with ab"), "{several}");
    assert!(several.contains("\n  ab12  2023-11-14 22:13:23  alice -> dev-a: on ab12…\n  ab1  "), "{several}");
    assert!(!several.contains("cd34"), "{several}");

    // Among any user's threads, when none of alice's matches.
    let others = [entry("ef56", 5), entry("ef5", 4)];
    assert_eq!(select_any(&others, &threads, "ef5", &alice).unwrap(), t("ef5"), "a whole id wins");
    let several = select_any(&others, &threads, "ef", &alice).unwrap_err().to_string();
    assert!(several.starts_with("2 threads over the last 7 days start with ef, none of them with alice"));
    assert!(several.contains("\n  ef56 ") && !several.contains("ab12"), "{several}");
    let none = select_any(&[], &threads, "ef", &alice).unwrap_err().to_string();
    assert!(none.contains("starts with ef; give the whole id of an older one"), "{none}");
    assert!(none.contains("\n  ab12 ") && none.contains("\n  cd34 "), "{none}");
    let many: Vec<Entry> = (0..12).map(|i| entry(&format!("t{i}"), i)).collect();
    assert!(select_any(&[], &many, "x", &alice).unwrap_err().to_string().ends_with("\n  and 2 more"));
    assert!(select_any(&[], &[], "ab", &alice).unwrap_err().to_string().contains("give the whole thread id"));
}

#[test]
fn a_start_of_a_time_ordered_id_bounds_when_it_was_made() {
    let id = ThreadId::generate();
    let made = ants_daemon::channel::thread_start(&id).unwrap() + CLOCK_SLACK;
    for len in [1, 8, 9, 11, 13, 20, 36] {
        let (first, last) = made_within(&id.as_str()[..len]).unwrap();
        assert!(first <= made && made < last, "{len}");
    }
    let (first, last) = made_within(&id.as_str()[..8]).unwrap();
    assert_eq!(last.duration_since(first).unwrap(), Duration::from_millis(1 << 16));
    let (first, last) = made_within(&id.as_str()[..13]).unwrap();
    assert_eq!(last.duration_since(first).unwrap(), Duration::from_millis(1));
    let v4 = Uuid::new_v4().to_string();
    assert_eq!(made_within(&v4[..15]), None, "no version 7 UUID");
    assert_eq!(made_within("t1"), None);
    assert_eq!(made_within("0199a3b4c"), None, "no hyphen after 8 digits");
}

#[tokio::test]
async fn a_human_picks_its_threads_by_a_start_of_their_id_or_the_last() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::connection::Nats::new(&server.url, Some(&creds), None).unwrap();
    let js = nats.connect("test").await.unwrap().1;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let _ = js.create_stream(ants_daemon::hub::threads_config()).await;
    let resolve = |pick: &str| pick.parse::<Pick>().unwrap().resolve(&nats, &js);
    assert!(resolve("last").await.unwrap_err().to_string().contains("no message to or from alice"));

    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let posted = ChannelPost::new("ops".parse().unwrap(), alice.clone(), "deploy?");
    let others = Envelope::new(dev_a.clone(), dev_b.clone(), "between ants");
    let reply = asked.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
    // Two threads between ants made in the same millisecond a minute ago, so the same 13 characters start them.
    let millis = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - 60_000;
    let made = |tail: u128| t(&Uuid::from_u128((millis << 80) | (0x7 << 76) | (0x8 << 60) | tail).to_string());
    let twins = [0x1_0000, 0x2_0000].map(|tail| Envelope::on_thread(made(tail), dev_b.clone(), dev_a.clone(), "twin"));
    for (subject, payload) in [
        (asked.subject(), json(&asked)),
        (posted.subject(), json(&posted)),
        (reply.subject(), json(&reply)),
        (others.subject(), json(&others)),
        (twins[0].subject(), json(&twins[0])),
        (twins[1].subject(), json(&twins[1])),
    ] {
        js.publish(subject, payload.into()).await.unwrap().await.unwrap();
    }

    assert_eq!(resolve("last").await.unwrap(), asked.thread_id, "the reply is the latest of alice's");
    let start = |thread: &ThreadId| thread.as_str()[..thread.as_str().len() - 4].to_owned();
    assert_eq!(resolve(&start(&posted.thread_id)).await.unwrap(), posted.thread_id);
    assert_eq!(resolve(&start(&others.thread_id)).await.unwrap(), others.thread_id, "a thread between ants");
    assert_eq!(resolve(&start(&twins[1].thread_id)).await.unwrap(), twins[1].thread_id);
    let meta = crate::threads::thread_meta::open(&nats, &js).await.unwrap();
    let title = ants_proto::ThreadTitle::new(twins[1].thread_id.clone(), "Twin one", alice.clone()).unwrap();
    meta.put_title(&title).await.unwrap();
    let twin = resolve(&twins[0].thread_id.as_str()[..13]).await.unwrap_err().to_string();
    assert!(twin.starts_with("2 threads over the last 7 days start with"), "{twin}");
    assert!(twin.contains(&format!("\n  {} ", twins[1].thread_id)), "{twin}");
    assert!(twin.contains("  \"Twin one\"  dev-b -> dev-a: twin\n"), "titled where it has one: {twin}");
    assert!(twin.trim_end().ends_with("  dev-b -> dev-a: twin"), "untitled: {twin}");
    let unknown = resolve("ffff").await.unwrap_err().to_string();
    assert!(unknown.contains("no thread over the last 7 days starts with ffff"), "{unknown}");
    assert!(unknown.contains(&format!("\n  {} ", asked.thread_id)), "{unknown}");
    assert!(unknown.contains(&format!("\n  {} ", posted.thread_id)), "{unknown}");
    assert!(!unknown.contains(&format!("\n  {} ", others.thread_id)), "{unknown}");
    // A whole id needs no read, whoever's thread it is.
    assert_eq!(resolve(others.thread_id.as_str()).await.unwrap(), others.thread_id);

    let ant_creds = server.creds_as(&dev_a, ants_proto::Role::Ant);
    let ant = crate::connection::Nats::new(&server.url, Some(&ant_creds), None).unwrap();
    let refused = Pick::Last.resolve(&ant, &js).await.unwrap_err().to_string();
    assert!(refused.contains("give the whole thread id"), "{refused}");
}

#[tokio::test]
async fn a_human_picks_a_message_by_a_start_of_its_id() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let me = crate::people::humans::Me { id: alice.clone(), role: ants_proto::Role::Human, assistant_of: None };
    let resolve = |pick: &str| pick.parse::<MessagePick>().unwrap().resolve(&js, &me);

    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let reply = asked.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
    let posted = ChannelPost::new("ops".parse().unwrap(), alice.clone(), "deploy?");
    let others = Envelope::new(dev_a.clone(), dev_b.clone(), "between ants");
    // Messages made in the same millisecond, so the same 13 characters start their ids: two to alice a minute ago,
    // two between ants two minutes ago.
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
    let made = |millis: u128, tail: u128| Uuid::from_u128((millis << 80) | (0x7 << 76) | (0x8 << 60) | tail);
    let mine = [0x1_0000, 0x2_0000].map(|tail| Envelope { id: made(now - 60_000, tail), ..reply.clone() });
    let twins = [0x1_0000, 0x2_0000].map(|tail| Envelope { id: made(now - 120_000, tail), ..others.clone() });
    for (subject, payload) in [
        (asked.subject(), json(&asked)),
        (reply.subject(), json(&reply)),
        (posted.subject(), json(&posted)),
        (others.subject(), json(&others)),
        (mine[0].subject(), json(&mine[0])),
        (mine[1].subject(), json(&mine[1])),
        (twins[0].subject(), json(&twins[0])),
        (twins[1].subject(), json(&twins[1])),
    ] {
        js.publish(subject, payload.into()).await.unwrap().await.unwrap();
    }

    let ids: Vec<Uuid> = recent_messages(&js, &alice).await.unwrap().iter().filter_map(|e| e.id).collect();
    assert_eq!(ids, [mine[1].id, mine[0].id, posted.id, reply.id, asked.id], "alice's, most recent first");
    let start = |id: Uuid| id.to_string()[..32].to_owned();
    assert_eq!(resolve(&start(reply.id)).await.unwrap(), reply.id);
    assert_eq!(resolve(&start(posted.id).to_uppercase()).await.unwrap(), posted.id);
    assert_eq!(resolve(&start(others.id)).await.unwrap(), others.id, "a message between ants");
    assert_eq!(resolve(&start(twins[1].id)).await.unwrap(), twins[1].id);
    let several = resolve(&mine[0].id.to_string()[..13]).await.unwrap_err().to_string();
    assert!(several.starts_with("2 messages to or from alice over the last 7 days start with"), "{several}");
    assert!(several.contains(&format!("\n  {} ", mine[1].id)), "{several}");
    assert!(several.contains("dev-a -> alice: built"), "{several}");
    let twin = resolve(&twins[0].id.to_string()[..13]).await.unwrap_err().to_string();
    assert!(twin.contains("2 messages over the last 7 days start with"), "{twin}");
    assert!(twin.contains("none of them to or from alice"), "{twin}");
    let unknown = resolve("ffff").await.unwrap_err().to_string();
    assert!(unknown.contains("no message over the last 7 days starts with ffff"), "{unknown}");
    assert!(unknown.contains(&format!("\n  {} ", asked.id)) && !unknown.contains(&others.id.to_string()), "{unknown}");
    // A whole id needs no read, whoever's message it is.
    let whole = Uuid::now_v7();
    assert_eq!(resolve(&whole.to_string()).await.unwrap(), whole);

    let ant = crate::people::humans::Me { id: dev_a, role: ants_proto::Role::Ant, assistant_of: None };
    let start = MessagePick::Prefix(start(reply.id));
    let refused = start.resolve(&js, &ant).await.unwrap_err().to_string();
    assert!(refused.contains("give the whole message id"), "{refused}");
}
