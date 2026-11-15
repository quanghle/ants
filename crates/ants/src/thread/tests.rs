use std::assert_matches;

use ants_proto::{ApprovalRequest, Escalation, EscalationReason, ToolCall, Verdict};

use super::*;

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

fn t(s: &str) -> ThreadId {
    s.parse().unwrap()
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

/// The entries of `messages`, each a subject and a payload, stored a second apart.
fn entries(messages: &[(String, Vec<u8>)]) -> Vec<Entry> {
    let mut parser = Parser::default();
    let parsed = messages.iter().enumerate();
    parsed.filter_map(|(i, (subject, payload))| parser.entry(i as u64 + 1, at(i as i64), subject, payload)).collect()
}

#[test]
fn every_kind_of_stored_message_lands_on_its_thread() {
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let ask = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "fix it").with_owner(Some(alice));
    let reply = ask.reply(dev_a.clone(), "done\nsee the diff", ants_proto::Role::Human).unwrap();
    let post = ChannelPost::on_thread("ops".parse().unwrap(), t("t1"), dev_b.clone(), "@dev-a nice");
    let note = NotesPost::new(t("t1"), dev_a.clone(), "plan");
    let escalation = Escalation::new(dev_a.clone(), t("t1"), EscalationReason::TurnBudget);
    let up = Envelope::on_thread(t("t1"), dev_a.clone(), dev_b, "help").with_escalation(Some(escalation));
    let request = ApprovalRequest::new(
        dev_a.clone(),
        t("t1"),
        ToolCall { kind: "execute".into(), title: "rm -rf build".into(), ..Default::default() },
        vec![Verdict::AllowOnce],
        Duration::from_secs(60),
    );
    let decision = request.decide(Verdict::AllowOnce);
    let dead = DeadLetter::of_envelope(dev_a.clone(), 3, 5, "harness died", ask.clone());
    let unknown = ApprovalDecision::new(Uuid::now_v7(), dev_a, Verdict::Reject);
    let messages = [
        (ask.subject(), json(&ask)),
        (reply.subject(), json(&reply)),
        (post.subject(), json(&post)),
        (note.subject(), json(&note)),
        (up.subject(), json(&up)),
        (request.subject(), json(&request)),
        (decision.subject(), json(&decision)),
        (dead.subject(), json(&dead)),
        // A decision on a request not read, and a payload of no kind, are on no thread known.
        (unknown.subject(), json(&unknown)),
        ("ants.inbox.dev-a.alice".to_owned(), b"not json".to_vec()),
    ];
    let entries = entries(&messages);
    let kinds: Vec<Kind> = entries.iter().map(|e| e.kind).collect();
    let expected = [
        Kind::Message,
        Kind::Message,
        Kind::Post,
        Kind::Note,
        Kind::Escalation,
        Kind::Approval,
        Kind::Decision,
        Kind::Dead,
    ];
    assert_eq!(kinds, expected);
    assert!(entries.iter().all(|e| e.thread_id == t("t1") && !e.forged), "{entries:?}");
    assert_eq!(entries[1].route(), "dev-a -> alice");
    assert_eq!(entries[2].route(), "dev-b -> #ops");
    assert_eq!(entries[3].route(), "dev-a -> notes");
    assert!(entries[4].note.as_deref().unwrap().contains("turn_budget"), "{:?}", entries[4]);
    assert_eq!(entries[6].route(), "(a human) -> dev-a");
    assert!(entries[6].text.starts_with("allow-once on dev-a/"), "{}", entries[6].text);
    assert_eq!(entries[7].note.as_deref(), Some("after 5 deliveries: harness died"));

    assert_eq!(entries[0].id, Some(ask.id));
    assert_eq!((entries[2].id, entries[3].id), (Some(post.id), Some(note.id)));
    assert_eq!((entries[5].id, entries[7].id), (None, None), "a tool request and a dead letter");
    let mut entries = entries;
    let eyes = Reacted { reaction: "eyes".parse().unwrap(), by: vec![p("alice"), p("dev-a")] };
    entries[1].reactions = vec![eyes];
    let shown = render(&entries[..2]);
    assert!(shown.contains(&format!("message     alice -> dev-a (message {}): fix it\n", ask.id)), "{shown}");
    assert!(shown.ends_with(": done\n    see the diff\n    reacted eyes: alice, dev-a\n"), "{shown}");
}

#[test]
fn a_message_on_another_sender_s_subject_is_marked_forged() {
    let e = Envelope::on_thread(t("t1"), p("alice"), p("dev-a"), "hi");
    let entries = entries(&[("ants.inbox.dev-a.mallory".to_owned(), json(&e))]);
    assert!(entries[0].forged);
    assert!(render(&entries).contains(&format!("alice -> dev-a (forged) (message {}): hi", e.id)));
}

#[test]
fn threads_sum_up_their_users_owner_times_and_opening() {
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let one = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "first line\nmore");
    let two = Envelope::on_thread(t("t1"), dev_a, dev_b.clone(), "Deploy failed").with_owner(Some(alice));
    let other = ChannelPost::on_thread("ops".parse().unwrap(), t("t2"), dev_b.clone(), "unrelated");
    let three = two.reply(dev_b, "retrying", ants_proto::Role::Ant).unwrap();
    let messages = [
        (one.subject(), json(&one)),
        (two.subject(), json(&two)),
        (other.subject(), json(&other)),
        (three.subject(), json(&three)),
    ];
    let read = entries(&messages);
    let threads = |select: &Select| {
        let mut threads = Threads::new(select.grep.as_deref());
        read.iter().for_each(|e| threads.add(e));
        threads.finish(select, &ThreadMarks::default())
    };

    let (all, omitted) = threads(&Select::default());
    assert_eq!(omitted, 0);
    let ids: Vec<&str> = all.iter().map(|t| t.thread_id.as_str()).collect();
    assert_eq!(ids, ["t2", "t1"], "least recently active first");
    let t1 = &all[1];
    assert_eq!(t1.owner, Some(p("alice")));
    assert_eq!(t1.users.iter().map(UserId::as_str).collect::<Vec<_>>(), ["alice", "dev-a", "dev-b"]);
    assert_eq!((t1.first_at, t1.last_at, t1.messages), (at(0), at(3), 3));
    assert_eq!(t1.opening, "first line…");
    assert_eq!(all[0].channels.iter().map(ChannelId::as_str).collect::<Vec<_>>(), ["ops"]);

    let ids = |(threads, _): (Vec<Summary>, usize)| -> Vec<String> {
        threads.into_iter().map(|t| t.thread_id.to_string()).collect()
    };
    assert_eq!(ids(threads(&Select { ant: Some(p("alice")), ..Default::default() })), ["t1"]);
    assert_eq!(ids(threads(&Select { ant: Some(p("dev-b")), ..Default::default() })), ["t2", "t1"]);
    assert_eq!(ids(threads(&Select { owner: Some(p("alice")), ..Default::default() })), ["t1"]);
    assert!(ids(threads(&Select { owner: Some(p("bob")), ..Default::default() })).is_empty());
    let (limited, omitted) = threads(&Select { limit: Some(1), ..Default::default() });
    assert_eq!((limited[0].thread_id.as_str(), omitted), ("t1", 1));

    let (found, _) = threads(&Select { grep: Some("deploy FAILED".into()), ..Default::default() });
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].matched.as_deref(), Some("dev-a: Deploy failed"));
    let table = table(&found);
    assert!(table.starts_with("THREAD  OWNER  USERS"), "{table}");
    assert!(table.ends_with("\n    dev-a: Deploy failed"), "{table}");
}

#[test]
fn pinned_threads_list_last_and_archived_ones_only_with_a_message_since() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let on = |thread: &str| Envelope::on_thread(t(thread), alice.clone(), dev_a.clone(), thread);
    let messages: Vec<(String, Vec<u8>)> =
        ["t1", "t2", "t3", "t4", "t2"].map(on).iter().map(|e| (e.subject(), json(e))).collect();
    let read = entries(&messages);
    let mut marks = ThreadMarks::default();
    marks.pin(&t("t1"));
    marks.archive(&t("t2"), 3);
    marks.archive(&t("t3"), 3);
    let threads = |select: &Select| {
        let mut threads = Threads::new(None);
        read.iter().for_each(|e| threads.add(e));
        threads.finish(select, &marks)
    };
    let ids = |threads: &[Summary]| -> Vec<String> { threads.iter().map(|t| t.thread_id.to_string()).collect() };

    let (listed, _) = threads(&Select::default());
    assert_eq!(ids(&listed), ["t4", "t2", "t1"], "t3 archived with nothing since, t1 pinned last");
    let states: Vec<Option<&str>> = listed.iter().map(state).collect();
    assert_eq!(states, [None, Some("archived+new"), Some("pinned")]);
    let (limited, omitted) = threads(&Select { limit: Some(1), ..Default::default() });
    assert_eq!((ids(&limited), omitted), (vec!["t1".to_owned()], 2), "the pinned outlast the limit");
    let (pinned, _) = threads(&Select { pinned: true, ..Default::default() });
    assert_eq!(ids(&pinned), ["t1"]);
    let (archived, _) = threads(&Select { archived: true, ..Default::default() });
    assert_eq!(ids(&archived), ["t3", "t2"]);
    assert!(!archived[0].new_activity && archived[1].new_activity);

    let mut listed = listed;
    listed[1].title = Some("Release 1.2".into());
    let table = table(&listed);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].starts_with("THREAD  OWNER  USERS") && lines[0].contains("MSGS  STATE         TITLE"), "{table}");
    assert!(lines[2].contains("archived+new  Release 1.2  t2"), "{table}");
    assert!(lines[1].contains("-             -            t4"), "{table}");
    let json = serde_json::to_value(&listed[1]).unwrap();
    assert_eq!(
        (&json["title"], &json["archived"], &json["new_activity"]),
        (&"Release 1.2".into(), &true.into(), &true.into())
    );
    assert!(serde_json::to_value(&listed[0]).unwrap().get("pinned").is_none());
}

#[test]
fn threads_idle_since_the_cutoff_list_only_as_auto_archived() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let on = |thread: &str| Envelope::on_thread(t(thread), alice.clone(), dev_a.clone(), thread);
    let messages: Vec<(String, Vec<u8>)> =
        ["t1", "t2", "t3", "t4", "t5"].map(on).iter().map(|e| (e.subject(), json(e))).collect();
    let read = entries(&messages);
    let mut marks = ThreadMarks::default();
    marks.pin(&t("t1"));
    marks.archive(&t("t2"), 5);
    marks.kept.insert(t("t3"), at(10));
    let threads = |select: &Select| {
        let mut threads = Threads::new(None);
        read.iter().for_each(|e| threads.add(e));
        threads.finish(&Select { idle_before: Some(at(3)), ..select.clone() }, &marks).0
    };
    let ids = |threads: &[Summary]| -> Vec<String> { threads.iter().map(|t| t.thread_id.to_string()).collect() };

    let listed = threads(&Select::default());
    assert_eq!(ids(&listed), ["t3", "t5", "t1"], "t4 idle since at(3), t3 kept since, t1 pinned");
    let archived = threads(&Select { archived: true, ..Default::default() });
    assert_eq!(ids(&archived), ["t2", "t4"]);
    assert_eq!(archived.iter().map(state).collect::<Vec<_>>(), [Some("archived"), Some("auto-archived")]);
    assert!(table(&archived).lines().nth(2).unwrap().contains("auto-archived"));
    let json = serde_json::to_value(&archived[1]).unwrap();
    assert_eq!((&json["archived"], &json["auto_archived"]), (&true.into(), &true.into()));
    assert!(serde_json::to_value(&archived[0]).unwrap().get("auto_archived").is_none());
}

#[test]
fn an_escalation_reaching_a_human_stays_open_until_a_human_s_message_on_the_thread() {
    let (alice, dev_a, lead) = (p("alice"), p("dev-a"), p("lead"));
    let escalate = |thread: &str, to: &UserId, reason| {
        let escalation = Escalation::new(dev_a.clone(), t(thread), reason);
        Envelope::on_thread(t(thread), dev_a.clone(), to.clone(), "[ants] stuck").with_escalation(Some(escalation))
    };
    let messages: Vec<Envelope> = vec![
        escalate("t1", &alice, EscalationReason::TurnBudget),
        escalate("t2", &UserId::human(), EscalationReason::Approval),
        escalate("t3", &lead, EscalationReason::Cap),
        escalate("t4", &alice, EscalationReason::Asked),
        Envelope::on_thread(t("t4"), alice.clone(), dev_a.clone(), "go on"),
        Envelope::on_thread(t("t1"), dev_a.clone(), alice.clone(), "still here"),
        escalate("t1", &alice, EscalationReason::TurnBudget),
    ];
    let read = entries(&messages.iter().map(|e| (e.subject(), json(e))).collect::<Vec<_>>());
    let mut humans = Humans::default();
    humans.add("alice", ants_proto::HumanEntry::new(alice)).unwrap();
    let mut threads = Threads::new(None).escalated(humans);
    read.iter().for_each(|e| threads.add(e));
    let (open, _) = threads.finish(&Select { escalated: true, ..Default::default() }, &ThreadMarks::default());

    assert_eq!(open.iter().map(|t| t.thread_id.as_str()).collect::<Vec<_>>(), ["t1"]);
    let since = open[0].escalated.as_deref().unwrap();
    let first = "2023-11-14 22:13:20 dev-a -> alice: escalation level 0, turn_budget: dev-a stuck on t1";
    assert!(since.starts_with(first), "the first since a human's message: {since}");
    assert!(table(&open).ends_with(&format!("\n    {since}")), "{}", table(&open));
}

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
    let Some(server) = crate::auth::tests::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    ants_daemon::bus::init_stream(&js).await.unwrap();
    let me = crate::humans::Me { id: alice.clone(), role: ants_proto::Role::Human };
    let resolve = |pick: &str| pick.parse::<Pick>().unwrap().resolve(&js, &me);
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
    let twin = resolve(&twins[0].thread_id.as_str()[..13]).await.unwrap_err().to_string();
    assert!(twin.starts_with("2 threads over the last 7 days start with"), "{twin}");
    assert!(twin.contains(&format!("\n  {} ", twins[1].thread_id)), "{twin}");
    let unknown = resolve("ffff").await.unwrap_err().to_string();
    assert!(unknown.contains("no thread over the last 7 days starts with ffff"), "{unknown}");
    assert!(unknown.contains(&format!("\n  {} ", asked.thread_id)), "{unknown}");
    assert!(unknown.contains(&format!("\n  {} ", posted.thread_id)), "{unknown}");
    assert!(!unknown.contains(&format!("\n  {} ", others.thread_id)), "{unknown}");
    // A whole id needs no read, whoever's thread it is.
    assert_eq!(resolve(others.thread_id.as_str()).await.unwrap(), others.thread_id);

    let ant = crate::humans::Me { id: dev_a, role: ants_proto::Role::Ant };
    let refused = Pick::Last.resolve(&js, &ant).await.unwrap_err().to_string();
    assert!(refused.contains("give the whole thread id"), "{refused}");
}

#[tokio::test]
async fn a_human_reads_threads_back_without_consuming_them() {
    let Some(server) = crate::auth::tests::Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    ants_daemon::bus::init_stream(&js).await.unwrap();
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
