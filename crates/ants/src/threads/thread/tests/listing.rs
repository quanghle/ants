//! Tests of listing threads: suggested titles, summary requests left out, users, owner and times, pinned, archived,
//! done and auto-archived threads, and escalations left open.

use super::*;

#[test]
fn a_thread_keeps_the_first_title_an_ant_suggested_but_none_from_a_forged_message() {
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let ask = Envelope::on_thread(t("t1"), alice, dev_a.clone(), "fix it");
    let titled = |from: &UserId, title: &str| {
        let mut reply = ask.reply(from.clone(), "on it", ants_proto::Role::Human).unwrap();
        reply.title = Some(title.to_owned());
        reply
    };
    let (forged, first, second) = (titled(&dev_b, "Forged"), titled(&dev_a, "Fix it"), titled(&dev_b, "Later"));
    let messages = [
        (ask.subject(), json(&ask)),
        ("ants.inbox.alice.dev-a".to_owned(), json(&forged)),
        (first.subject(), json(&first)),
        (second.subject(), json(&second)),
    ];
    let read = entries(&messages);
    assert_eq!((read[1].forged, &read[1].suggested_title), (true, &None));
    let mut threads = Threads::new(None);
    read.iter().for_each(|e| threads.add(e));
    let (all, _) = threads.finish(&Select::default(), &ThreadMarks::default());
    assert_eq!(all[0].suggested, Some((dev_a, "Fix it".to_owned())));
}

#[test]
fn threads_opened_by_summary_requests_are_left_out_but_not_for_a_forged_one() {
    let (alice, dev_a, messenger) = (p("alice"), p("dev-a"), UserId::messenger());
    let work = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "fix it");
    let request = Some(SummaryRequest::thread(t("t1"), 1));
    let ask =
        Envelope { summary: request.clone(), ..Envelope::on_thread(t("t2"), alice.clone(), messenger.clone(), "x") };
    let tldr = Envelope { summary: request.clone(), ..ask.reply(messenger, "fixed", ants_proto::Role::Human).unwrap() };
    let forged = Envelope { summary: request, ..Envelope::on_thread(t("t3"), dev_a, alice, "hi") };
    let messages = [
        (work.subject(), json(&work)),
        (ask.subject(), json(&ask)),
        (tldr.subject(), json(&tldr)),
        ("ants.inbox.alice.dev-b".to_owned(), json(&forged)),
    ];
    let mut threads = Threads::new(None);
    entries(&messages).iter().for_each(|e| threads.add(e));
    let (listed, _) = threads.finish(&Select::default(), &ThreadMarks::default());
    let ids: Vec<&str> = listed.iter().map(|s| s.thread_id.as_str()).collect();
    assert_eq!(ids, ["t1", "t3"]);
}

#[test]
fn channel_and_notes_posts_suggest_titles_too_unless_forged() {
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let ops: ChannelId = "ops".parse().unwrap();
    let top = ChannelPost::new(ops.clone(), alice, "@dev-a the build broke");
    let thread = top.thread_id.clone();
    let reply = ChannelPost::on_thread(ops, thread.clone(), dev_a.clone(), "on it");
    let reply = ChannelPost { title: Some("Fix the build".to_owned()), ..reply };
    let forged = ChannelPost { from: dev_b.clone(), title: Some("Forged".to_owned()), ..reply.clone() };
    let note = NotesPost { title: Some("Plan the fix".to_owned()), ..NotesPost::new(thread.clone(), dev_b, "plan") };
    let messages = [
        (top.subject(), json(&top)),
        (reply.subject(), json(&forged)),
        (reply.subject(), json(&reply)),
        (note.subject(), json(&note)),
    ];
    let read = entries(&messages);
    let suggested: Vec<_> = read.iter().map(|e| (e.forged, e.suggested_title.as_deref())).collect();
    assert_eq!(suggested, [(false, None), (true, None), (false, Some("Fix the build")), (false, Some("Plan the fix"))]);
    let mut threads = Threads::new(None);
    read.iter().for_each(|e| threads.add(e));
    assert_eq!(threads.get(&thread).unwrap().suggested, Some((dev_a, "Fix the build".to_owned())));
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
fn only_messages_posts_and_notes_reopen_a_thread_and_done_ones_list_so() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let ask = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "build it");
    let tool = ToolCall { kind: "execute".into(), title: "cargo build".into(), ..Default::default() };
    let request = ApprovalRequest::new(dev_a, t("t1"), tool, vec![Verdict::AllowOnce], Duration::from_secs(60));
    let read = entries(&[(ask.subject(), json(&ask)), (request.subject(), json(&request))]);
    assert_eq!(read.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(last_reopening(&read), 1, "a tool request reopens nothing");
    assert_eq!(last_reopening(&read[1..]), 0);

    let mut threads = Threads::new(None);
    read.iter().for_each(|e| threads.add(e));
    let (mut listed, _) = threads.finish(&Select::default(), &ThreadMarks::default());
    assert_eq!(listed[0].last_reopening, 1);
    listed[0].done = Some(Done { by: alice, at: at(0), acting: Acting::Own });
    let mut pinned = listed[0].clone();
    pinned.pinned = true;
    let table = table(&[listed[0].clone(), pinned]);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].contains("MSGS  STATE"), "{table}");
    assert!(lines[1].contains("  done  ") && lines[2].contains("  pinned,done  "), "{table}");
    let json = serde_json::to_value(&listed[0]).unwrap();
    assert_eq!(json["done"]["by"], "alice", "{json}");
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
