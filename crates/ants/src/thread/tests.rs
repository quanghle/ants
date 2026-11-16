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
    let eyes = Reacted::new("eyes".parse().unwrap(), vec![p("alice"), p("dev-a")]);
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
fn a_question_shows_its_options_and_the_valid_answers_to_it() {
    let alice = p("alice");
    let options = ["yes".to_owned(), "not yet".to_owned()];
    let question = ants_proto::Question::new("Ship it?", &options, true).unwrap();
    let asked = Envelope::asking(t("t1"), p("dev-a"), alice.clone(), question.clone());
    let answer = |choice| Envelope::answering(&asked, alice.clone(), choice).unwrap();
    let picked = answer(ants_proto::Choice::Option(2));
    let typed = answer(ants_proto::Choice::Typed("after lunch".to_owned()));
    let mut wrong = answer(ants_proto::Choice::Option(1));
    wrong.answer.as_mut().unwrap().option = Some(3);
    let forged = answer(ants_proto::Choice::Option(1));
    let read = entries(&[
        (asked.subject(), json(&asked)),
        (picked.subject(), json(&picked)),
        (typed.subject(), json(&typed)),
        (wrong.subject(), json(&wrong)),
        ("ants.inbox.dev-a.mallory".to_owned(), json(&forged)),
    ]);
    assert_eq!(read[0].question.as_ref(), Some(&question));
    assert_eq!(read[1].answers(&read[0]), Some(Answered::Option(2, "not yet")));
    assert_eq!(read[2].answers(&read[0]), Some(Answered::Typed));
    assert_eq!((&read[3].answer, &read[4].answer), (&None, &None), "invalid or forged: ordinary messages");
    let by: Vec<Uuid> = answers_to(&read[0], &read).filter_map(|(e, _)| e.id).collect();
    assert_eq!(by, [picked.id, typed.id]);

    let shown = render(&read);
    assert!(shown.contains(&format!("dev-a -> alice (message {}): Ship it?\n", asked.id)), "{shown}");
    assert!(shown.contains("\n    1. yes\n    2. not yet\n"), "{shown}");
    assert!(shown.contains("\n    Answer with an option's number or in own words.\n"), "{shown}");
    assert!(shown.contains("\n    answered by alice: option 2, not yet\n"), "{shown}");
    assert!(shown.contains("\n    answered by alice, typed: after lunch\n"), "{shown}");
    assert!(!shown.contains("ants answer"), "answered: {shown}");
    let line = format!("alice -> dev-a [answers {} with option 2] (message {}): not yet", asked.id, picked.id);
    assert!(shown.contains(&line), "{shown}");
    assert!(shown.contains(&format!("[answers {} as typed]", asked.id)), "{shown}");
    let unanswered = render(&read[..1]);
    assert!(unanswered.contains(&format!("\n    (`ants answer {} N|TEXT` answers it)\n", asked.id)), "{unanswered}");
    let json = serde_json::to_value(&read[0]).unwrap();
    assert_eq!(json["question"]["options"][1], "not yet");
}

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
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
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
    let meta = crate::thread_meta::open(&nats, &js).await.unwrap();
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
    let ant = crate::Nats::new(&server.url, Some(&ant_creds), None).unwrap();
    let refused = Pick::Last.resolve(&ant, &js).await.unwrap_err().to_string();
    assert!(refused.contains("give the whole thread id"), "{refused}");
}

#[tokio::test]
async fn a_human_picks_a_message_by_a_start_of_its_id() {
    let Some(server) = crate::auth::tests::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let me = crate::humans::Me { id: alice.clone(), role: ants_proto::Role::Human, assistant_of: None };
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

    let ant = crate::humans::Me { id: dev_a, role: ants_proto::Role::Ant, assistant_of: None };
    let start = MessagePick::Prefix(start(reply.id));
    let refused = start.resolve(&js, &ant).await.unwrap_err().to_string();
    assert!(refused.contains("give the whole message id"), "{refused}");
}

#[tokio::test]
async fn a_human_reads_threads_back_without_consuming_them() {
    let Some(server) = crate::auth::tests::Server::start() else { return };
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
    let Some(server) = crate::auth::tests::Server::start() else { return };
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

    let shown = history(&js, &ask.thread_id, None, true, None).await.unwrap();
    let kinds: Vec<_> = shown.iter().map(|e| (e.kind, e.text.as_str())).collect();
    assert_eq!(
        kinds,
        [
            (Kind::Message, "build it"),
            (Kind::Tool, "cargo build"),
            (Kind::Tool, "cargo test"),
            (Kind::Message, "built")
        ]
    );
    let rendered = render(&shown[1..2]);
    assert!(rendered.contains("tool        dev-a [execute, completed, 2.1s]: cargo build"), "{rendered}");
    let line = serde_json::to_value(&shown[2]).unwrap();
    assert_eq!(line["tool"]["status"], "failed");
    assert_eq!(line["tool"]["duration_ms"], 840);
    assert_eq!(line["tool"]["message"], ask.id.to_string());
    let without = history(&js, &ask.thread_id, None, false, None).await.unwrap();
    assert!(without.iter().all(|e| e.kind == Kind::Message), "{without:?}");
    assert!(!<Kind as clap::ValueEnum>::value_variants().contains(&Kind::Tool), "tool calls are not searched");
}

#[tokio::test]
async fn a_sealed_message_from_a_device_enrolled_mid_follow_opens_everywhere_it_is_read() {
    use ants_proto::Role;

    let Some(server) = crate::auth::tests::Server::start() else { return };
    let (alice, bob) = (p("alice"), p("bob"));
    let enrolled = |name: &str| {
        let creds = server.dir().join(format!("alice-{name}.creds"));
        crate::auth::issue(&server.dir().join("auth"), &alice, Role::Human, None, &creds).unwrap();
        creds
    };
    let desk = enrolled("desk");
    crate::Nats::new(&server.url, Some(&desk), None).unwrap().connect("test").await.unwrap();
    let bob_creds = server.creds_as(&bob, Role::Human);
    let bob_nats = crate::Nats::new(&server.url, Some(&bob_creds), None).unwrap();
    let (_, bob_js) = bob_nats.connect("test").await.unwrap();
    // One keyring per read, each loaded before the device was enrolled and learning it on its own.
    let load = async || crate::sealed::Keyring::load(&bob_nats, &bob_js).await.map(Arc::new);
    let (keyring, for_history, for_search) = (load().await, load().await, load().await);
    assert!(keyring.is_some());

    // Bob follows the inbox; meanwhile alice enrolls a second device and asks from it.
    let mut inbox = crate::inbox::Inbox::open(&bob_js, &bob).await.unwrap();
    let select = crate::inbox::Select { limit: Some(1), ..Default::default() };
    let mut followed = Vec::new();
    let follow = inbox.follow(
        &select,
        keyring.as_deref(),
        None,
        |e| {
            followed.push(crate::sealed::read(e.clone(), keyring.as_deref()));
            Ok(())
        },
        tokio::time::sleep(Duration::from_secs(20)),
    );
    let asked = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let laptop = enrolled("laptop");
        let alice_nats = crate::Nats::new(&server.url, Some(&laptop), None).unwrap();
        alice_nats.connect("test").await.unwrap();
        let words = vec!["@bob".to_owned(), "lunch at noon?".to_owned()];
        let (sending, replies, input, attach) = Default::default();
        Box::pin(crate::cmd::send::ask(&alice_nats, None, sending, &replies, &input, &attach, words)).await.unwrap();
    };
    let (follow, ()) = tokio::join!(follow, asked);
    follow.unwrap();
    let [(e, state)] = followed.as_slice() else { panic!("not one message: {followed:?}") };
    assert_eq!((e.text.as_str(), *state), ("lunch at noon?", Some(crate::sealed::State::Opened)));

    // The thread's history and a search open it too.
    let shown = history(&bob_js, &e.thread_id, None, false, for_history).await.unwrap();
    let opened: Vec<_> = shown.iter().map(|e| (e.text.as_str(), e.sealed)).collect();
    assert_eq!(opened, [("lunch at noon?", Some(crate::sealed::State::Opened))]);
    let search = crate::search::Search { grep: Some("lunch".into()), limit: 10, ..Default::default() };
    let (found, _) = search.run(&bob_js, Parser::opening(for_search)).await.unwrap();
    assert_eq!(found.iter().map(|e| e.text.as_str()).collect::<Vec<_>>(), ["lunch at noon?"]);
}
