//! Tests of what is held back: quiet hours, mutes and snoozes, stand-ins, digests, folded replies and questions.

use super::*;

#[test]
fn quiet_hours_read_the_wall_clock_of_their_time_zone_across_midnight() {
    let night = human(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\n{NTFY_SINK}")).unwrap();
    let quiet = night.quiet.as_ref().unwrap();
    for (time, held) in [("21:59", false), ("22:00", true), ("23:59", true), ("00:00", true), ("06:59", true)] {
        assert_eq!(quiet.contains(at(&format!("2026-03-10T{time}:30Z"))), held, "{time} UTC");
    }
    assert!(!quiet.contains(at("2026-03-10T07:00:00Z")), "the end is not in the window");
    let day = QuietHours::parse("09:30-17:00", TimeZone::UTC).unwrap();
    assert!(day.contains(at("2026-03-10T09:30:00Z")) && !day.contains(at("2026-03-10T17:00:00Z")));
    assert!(!day.contains(at("2026-03-10T03:00:00Z")));

    // Tokyo is UTC+9 all year; New York moves to UTC-4 on 2026-03-08.
    let tokyo =
        human(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\ntimezone = \"Asia/Tokyo\"\n{NTFY_SINK}")).unwrap();
    let tokyo = tokyo.quiet.as_ref().unwrap();
    assert!(tokyo.contains(at("2026-03-10T14:00:00Z")), "23:00 in Tokyo");
    assert!(!tokyo.contains(at("2026-03-10T23:00:00Z")), "08:00 in Tokyo");
    let entry: HumanEntry = toml::from_str(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\n{NTFY_SINK}")).unwrap();
    let york = ants_daemon::schedule::time_zone("America/New_York").unwrap();
    let york = Human::from_entry(entry, Some(&york), &Path::to_path_buf, &|_: &str| None).unwrap();
    assert_eq!(
        york.quiet_hours(at("2026-03-07T03:30:00Z")).unwrap(),
        "quiet hours 22:00-07:00 America/New_York (on now); quiet hours and a snooze let nothing through",
        "22:30 EST, in the file's time zone"
    );
    assert!(!york.quiet.as_ref().unwrap().contains(at("2026-03-09T11:30:00Z")), "07:30 EDT");
    assert!(york.quiet.as_ref().unwrap().contains(at("2026-03-09T10:30:00Z")), "06:30 EDT");

    for (text, why) in [
        ("quiet = \"22:00\"", "expected HH:MM-HH:MM"),
        ("quiet = \"24:00-07:00\"", "expected HH:MM-HH:MM"),
        ("quiet = \"7:00-9:00\"", "expected HH:MM-HH:MM"),
        ("quiet = \"08:00-08:00\"", "starts where it ends"),
        ("timezone = \"Asia/Tokyo\"", "timezone goes with quiet only"),
        ("quiet = \"22:00-07:00\"\ntimezone = \"Mars/Base\"", "unknown time zone"),
        ("bypass = [\"completions\"]", "unknown variant"),
        ("completion_window = \"1m\"", "goes with completions = true only"),
        ("completions = true\ncompletion_window = \"2h\"", "from 0 to 1h"),
    ] {
        let text = format!("name = \"bob\"\n{text}\n{NTFY_SINK}");
        assert!(error(&text).contains(why), "{text}: {}", error(&text));
    }
    let only =
        human(&format!("name = \"bob\"\napprovals = false\nescalations = false\ncompletions = true\n{NTFY_SINK}"));
    assert_eq!(only.unwrap().subjects(), ["ants.inbox.bob.>"], "completions read the inbox");
    let none = format!(
        "name = \"bob\"\napprovals = false\nescalations = false\nmessages = false\nmentions = false\nruns = false\n\
         {NTFY_SINK}"
    );
    assert!(error(&none).contains("nothing to notify"), "{}", error(&none));
}

#[test]
fn a_mute_holds_back_everything_of_its_thread_and_a_snooze_or_quiet_hours_what_bypass_does_not_list() {
    let now = at("2026-03-10T12:00:00Z");
    let bob = human(&format!("name = \"bob\"\nbypass = [\"approvals\"]\n{NTFY_SINK}")).unwrap();
    let asked = request();
    let approval = About::Approval { request: Box::new(asked.clone()), seq: 1 };
    let other = ThreadId::generate();
    let escalation = About::Escalation {
        id: Uuid::now_v7(),
        from: p("dev-a"),
        escalation: Box::new(Escalation::new(p("dev-a"), other.clone(), ants_proto::EscalationReason::Asked)),
    };
    let completion = About::Completion { id: Uuid::now_v7(), from: p("dev-a"), thread: other, replies: 1 };
    let mut prefs = NotifyPrefs::default();
    for about in [&approval, &escalation, &completion] {
        assert_eq!(held(&bob, about, &prefs, now), None, "nothing held");
    }

    prefs.snoozed_until = Some(now + TimeDelta::minutes(5));
    assert_eq!(held(&bob, &approval, &prefs, now), None, "approvals bypass");
    assert_eq!(held(&bob, &escalation, &prefs, now), Some("snoozed"));
    assert_eq!(held(&bob, &completion, &prefs, now), Some("snoozed"));
    let later = now + TimeDelta::minutes(5);
    assert_eq!(held(&bob, &completion, &prefs, later), None, "the snooze ended on its own");
    prefs.prune(later);
    assert_eq!(prefs, NotifyPrefs::default());

    prefs.muted.insert(asked.thread_id, now + TimeDelta::hours(1));
    assert_eq!(held(&bob, &approval, &prefs, now), Some("muted thread"), "whatever bypass says");
    assert_eq!(held(&bob, &completion, &prefs, now), None, "another thread");
    assert_eq!(held(&bob, &approval, &prefs, now + TimeDelta::hours(1)), None, "the mute ended");

    let night = human(&format!("name = \"bob\"\nquiet = \"11:00-13:00\"\nbypass = [\"escalations\"]\n{NTFY_SINK}"));
    let night = night.unwrap();
    let prefs = NotifyPrefs::default();
    assert_eq!(held(&night, &approval, &prefs, now), Some("quiet hours"));
    assert_eq!(held(&night, &escalation, &prefs, now), None, "escalations bypass");
    assert_eq!(held(&night, &completion, &prefs, now), Some("quiet hours"));
    assert_eq!(held(&night, &completion, &prefs, at("2026-03-10T13:00:00Z")), None);
}

#[test]
fn a_notice_held_from_a_human_away_is_relayed_to_its_stand_in_who_is_told_whom_to_answer() {
    let alice = human(&format!("name = \"alice\"\nstand_in = \"bob\"\n{NTFY_SINK}")).unwrap();
    assert!(error(&format!("name = \"bob\"\nstand_in = \"bob\"\n{NTFY_SINK}")).contains("the human itself"));
    let is_human = |user: &UserId| ["alice", "bob", "carol"].contains(&user.as_str());
    let now = Utc::now();
    let asked = request().for_human(Some(p("alice")), None);
    let notice = approval_notice(&asked, 9, &p("alice"), None, None, now);

    let relay = relayed(&alice, &notice, "quiet hours", is_human).unwrap();
    assert_eq!((&relay.from, &relay.to, &relay.thread_id), (&p("alice"), &p("bob"), &asked.thread_id));
    assert!(
        relay.text.starts_with("[ants] Relayed to bob, alice's stand-in, while alice is away (quiet hours)"),
        "{}",
        relay.text
    );
    assert!(relay.text.contains(&format!("ants tools approve dev-a/{}", asked.id)), "{}", relay.text);
    let escalation = relay.escalation.as_deref().unwrap();
    assert_eq!(escalation.away, Some(Away { human: p("alice"), ant: p("dev-a") }));
    assert_eq!((escalation.reason, escalation.request_id), (EscalationReason::Approval, Some(asked.id)));

    let shown = escalation_notice(&relay).unwrap();
    assert_eq!(
        (shown.title.as_str(), &shown.human, shown.urgent),
        ("for alice (away): dev-a asks to run a tool", &p("bob"), true)
    );
    let json = notice_json(&shown, now);
    assert_eq!((&json["ant"], &json["stand_in_for"]), (&json!("dev-a"), &json!("alice")));
    let bob = human(&format!("name = \"bob\"\n{NTFY_SINK}")).unwrap();
    let carol = human(&format!("name = \"carol\"\nants = [\"dev-b\"]\n{NTFY_SINK}")).unwrap();
    assert!(bob.gets_relayed_request(&shown.about), "bob is notified of dev-a's requests anyway");
    assert!(!carol.gets_relayed_request(&shown.about));
    assert!(relayed(&bob, &shown, "quiet hours", is_human).is_none(), "a relay is relayed no further");

    let named = asked.clone().for_human(Some(p("alice")), Some(p("carol")));
    let notice = approval_notice(&named, 9, &p("alice"), None, None, now);
    assert_eq!(relayed(&alice, &notice, "snoozed", is_human).unwrap().to, p("carol"), "the ant's stand-in");
    let named = asked.clone().for_human(Some(p("alice")), Some(p("dev-b")));
    let notice = approval_notice(&named, 9, &p("alice"), None, None, now);
    assert_eq!(relayed(&alice, &notice, "snoozed", is_human).unwrap().to, p("bob"), "an ant stands in for no one");
    assert!(relayed(&alice, &notice, "muted thread", is_human).is_none());
    assert!(relayed(&bob, &notice, "snoozed", is_human).is_none(), "the request waits for another human");
    let unnamed = human(&format!("name = \"alice\"\n{NTFY_SINK}")).unwrap();
    let notice = approval_notice(&asked, 9, &p("alice"), None, None, now);
    assert!(relayed(&unnamed, &notice, "snoozed", is_human).is_none(), "no stand-in");

    let thread = ThreadId::generate();
    let stuck = Escalation {
        stand_in: Some(p("carol")),
        ..Escalation::new(p("dev-a"), thread.clone(), EscalationReason::Asked)
    };
    let escalated = Envelope::new(p("dev-a"), p("alice"), "which branch?").with_escalation(Some(stuck));
    let relay = relayed(&alice, &escalation_notice(&escalated).unwrap(), "snoozed", is_human).unwrap();
    assert_eq!((&relay.to, &relay.thread_id), (&p("carol"), &thread));
    let shown = escalation_notice(&relay).unwrap();
    assert_eq!(shown.title, "for alice (away): dev-a escalates: asked on dev-a");
    assert!(shown.text.contains(&format!("ants ask --thread {thread} @dev-a TEXT")), "{}", shown.text);
    let forged = Envelope { from: p("dev-b"), ..relay };
    assert_eq!(escalation_notice(&forged).unwrap().title, "dev-b escalates: asked on dev-a", "relayed by no one away");
    let expired = escalated.with_escalation(Some(Escalation::of_request(&asked)));
    assert!(relayed(&alice, &escalation_notice(&expired).unwrap(), "snoozed", is_human).is_none(), "nothing to answer");
}

#[test]
fn messages_mentions_and_run_ends_are_held_back_by_a_mute_a_snooze_and_quiet_hours_and_counted_for_the_digest() {
    let now = at("2026-03-10T12:00:00Z");
    let thread = ThreadId::generate();
    let message = About::Message { id: Uuid::now_v7(), from: p("bob"), thread: thread.clone() };
    let mention = About::Mention {
        id: Uuid::now_v7(),
        from: p("bob"),
        thread: thread.clone(),
        channel: "ops".parse().unwrap(),
        post: Uuid::now_v7(),
    };
    let run = About::Run {
        id: Uuid::now_v7(),
        from: p("lead"),
        thread: thread.clone(),
        run: "r1".parse().unwrap(),
        trail: None,
        state: ants_proto::RunEnd::Done,
    };
    let text =
        format!("name = \"alice\"\nquiet = \"11:00-13:00\"\nbypass = [\"escalations\", \"questions\"]\n{NTFY_SINK}");
    let night = human(&text).unwrap();
    let day = human(&format!("name = \"alice\"\nbypass = [\"escalations\", \"questions\"]\n{NTFY_SINK}")).unwrap();
    let prefs = NotifyPrefs::default();
    let mut muted = NotifyPrefs::default();
    muted.muted.insert(thread, now + TimeDelta::hours(1));
    let snoozed = NotifyPrefs { snoozed_until: Some(now + TimeDelta::minutes(5)), ..NotifyPrefs::default() };
    for about in [&message, &mention, &run] {
        assert_eq!(held(&day, about, &prefs, now), None, "{about:?}");
        assert_eq!(held(&night, about, &prefs, now), Some("quiet hours"), "no bypass lets it through");
        assert_eq!(held(&day, about, &snoozed, now), Some("snoozed"));
        assert_eq!(held(&day, about, &muted, now), Some("muted thread"));
    }

    let mut counts = HeldNotices::default();
    for about in [&message, &message, &mention, &run, &run] {
        counts.add(about.kind(), now);
    }
    let notice = digest_notice(&day, counts, &[], now);
    assert_eq!(notice.title, "5 notices held back");
    assert!(notice.text.starts_with("1 mention, 2 messages, 2 trail runs ended held back"), "{}", notice.text);
    assert_eq!(notice_json(&notice, now)["counts"], json!({ "mention": 1, "message": 2, "run": 2 }));
    assert_eq!((counted("run", 1), counted("message", 1)), ("1 trail run ended".to_owned(), "1 message".to_owned()));
}

#[test]
fn a_trail_runs_question_that_quiet_hours_held_back_is_pushed_once_they_end_unless_another_hold_holds_it() {
    let night = human(&format!("name = \"alice\"\nquiet = \"11:00-13:00\"\n{NTFY_SINK}")).unwrap();
    let (alice, lead) = (p("alice"), p("lead"));
    let question = ants_proto::Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], false).unwrap();
    let asking = Envelope::asking(ThreadId::generate(), lead, alice, question);
    let step = ants_proto::TrailStep {
        run: "r1".parse().unwrap(),
        call: 1,
        label: None,
        schema: None,
        trail: None,
        script: None,
        tip_key: None,
        bounds: None,
    };
    let stepped = asking.clone().as_step(step);
    let notice = question_notice(&stepped).unwrap();
    assert!(notice.about.is_run_question() && !question_notice(&asking).unwrap().about.is_run_question());
    assert_eq!(notice_json(&notice, Utc::now())["run"], json!("r1"));

    let notified = Notified::new(night);
    let (quiet, after) = (at("2026-03-10T12:00:00Z"), at("2026-03-10T13:00:00Z"));
    notified.keep_for_later(notice, 7);
    let mut prefs = NotifyPrefs::default();
    assert!(notified.due_later(&prefs, quiet).is_empty(), "quiet hours still hold");
    prefs.snoozed_until = Some(after + TimeDelta::minutes(5));
    let due = notified.due_later(&prefs, after);
    assert_eq!(due.iter().map(|(_, seq, why)| (*seq, *why)).collect::<Vec<_>>(), [(7, Some("snoozed"))]);
    assert!(notified.due_later(&NotifyPrefs::default(), after).is_empty(), "taken once");

    for seq in 0..70 {
        notified.keep_for_later(question_notice(&stepped).unwrap(), seq);
    }
    let due = notified.due_later(&NotifyPrefs::default(), after);
    assert_eq!(due.len(), LATER, "the oldest dropped");
    assert_eq!((due[0].1, due[0].2), (6, None));
}

#[test]
fn a_digest_is_due_once_neither_a_snooze_nor_quiet_hours_hold_whatever_bypass_lets_through() {
    let now = at("2026-03-10T12:00:00Z");
    let text = format!("name = \"bob\"\nquiet = \"11:00-13:00\"\nbypass = [\"approvals\"]\ndigest = true\n{NTFY_SINK}");
    let night = human(&text).unwrap();
    let mut prefs = NotifyPrefs::default();
    assert!(holding(&night, &prefs, now), "quiet hours, though approvals bypass them");
    let after = at("2026-03-10T13:00:00Z");
    assert!(!holding(&night, &prefs, after));
    prefs.snoozed_until = Some(after + TimeDelta::minutes(5));
    assert!(holding(&night, &prefs, after), "snoozed");
    assert!(!holding(&night, &prefs, after + TimeDelta::minutes(5)), "the snooze ended on its own");
    assert!(night.quiet_hours(now).unwrap().ends_with("let approvals through, and a digest of the others follows"));
    let day = human(&format!("name = \"bob\"\n{NTFY_SINK}")).unwrap();
    assert!(!day.digest && !holding(&day, &NotifyPrefs::default(), now), "off unless asked for");
}

#[test]
fn a_digest_counts_what_was_held_back_and_lists_the_requests_still_pending() {
    let now = at("2026-03-10T13:00:00Z");
    let mut held = HeldNotices::default();
    held.add("completion", at("2026-03-10T11:05:00Z"));
    held.add("approval", at("2026-03-10T11:30:00Z"));
    held.add("completion", at("2026-03-10T12:00:00Z"));
    assert_eq!((held.since, held.total()), (at("2026-03-10T11:05:00Z"), 3), "since the first");
    let night = human(&format!("name = \"bob\"\nquiet = \"11:00-13:00\"\ntimezone = \"Europe/Paris\"\n{NTFY_SINK}"));
    let night = night.unwrap();
    let mut pending: Vec<ApprovalRequest> = (0..12).map(|_| request()).collect();
    pending[0].tool.title = "deploy ghp_0123456789abcdefghijABCDEFGHIJ".to_owned();
    let notice = digest_notice(&night, held.clone(), &pending, now);
    assert_eq!((notice.title.as_str(), notice.urgent, notice.buttons.len()), ("3 notices held back", true, 0));
    let mut lines = notice.text.lines();
    assert_eq!(
        lines.next(),
        Some(
            "1 tool request, 2 replies held back by quiet hours or a snooze since 2026-03-10 12:05 CET; nothing was \
             marked read: ants inbox shows the messages."
        )
    );
    assert_eq!(lines.nth(1), Some("12 tool requests still pending:"));
    let first = lines.next().unwrap();
    assert!(first.starts_with(&tools::qualified(&pending[0])) && !first.contains("ghp_"), "redacted: {first}");
    assert_eq!(lines.clone().count(), DIGEST_PENDING, "nine more and how many are left");
    assert_eq!(lines.last(), Some("…and 2 more: ants tools list"));

    let json = notice_json(&notice, now);
    assert_eq!(
        (&json["kind"], &json["since"], &json["counts"]),
        (&json!("digest"), &json!("2026-03-10T11:05:00Z"), &json!({ "approval": 1, "completion": 2 }))
    );
    let listed = json["pending"].as_array().unwrap();
    assert_eq!(listed.len(), 12, "all of them");
    assert_eq!(listed[1]["request"], json!(tools::qualified(&pending[1])));
    assert_eq!((&listed[1]["ant"], &listed[1]["title"]), (&json!("dev-a"), &json!("rm -rf <build>")));
    assert!(json["id"].is_string() && json["links"] == json!([]), "{json}");

    let calm = digest_notice(&human(&format!("name = \"bob\"\n{NTFY_SINK}")).unwrap(), held, &[], now);
    assert!(!calm.urgent && !calm.text.contains("pending"), "{}", calm.text);
    assert!(calm.text.contains("since 2026-03-10 11:05 UTC"), "{}", calm.text);
}

#[test]
fn replies_on_a_thread_fold_into_one_notice_once_it_is_quiet_or_has_waited_five_windows() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let other = Envelope::new(alice, dev_a.clone(), "test it");
    let reply = |to: &Envelope, text: &str| completion_notice(&to.reply(dev_a.clone(), text, Role::Human).unwrap());
    let (window, start) = (Duration::from_secs(60), at("2026-03-10T12:00:00Z"));
    let mut replies = super::replies::Replies::default();
    replies.add(reply(&asked, "starting").unwrap(), 1, start);
    replies.add(reply(&other, "tested").unwrap(), 2, start + TimeDelta::seconds(30));
    replies.add(reply(&asked, "built").unwrap(), 3, start + TimeDelta::seconds(50));
    assert!(replies.due(start + TimeDelta::seconds(80), window).is_empty(), "both threads busy within the window");
    let due = replies.due(start + TimeDelta::seconds(90), window);
    assert_eq!(due.iter().map(|(n, seq)| (n.title.as_str(), *seq)).collect::<Vec<_>>(), [("dev-a replied", 2)]);
    let due = replies.due(start + TimeDelta::seconds(110), window);
    let [(notice, 3)] = &due[..] else { panic!("{due:?}") };
    assert_eq!(notice.title, "dev-a replied (2 replies on the thread)");
    assert!(notice.text.starts_with("built\n"));
    assert_eq!(notice_json(notice, start)["replies"], json!(2));

    for minute in 0..5 {
        replies.add(reply(&asked, "still going").unwrap(), 4, start + TimeDelta::minutes(minute));
    }
    assert!(replies.due(start + TimeDelta::seconds(299), window).is_empty());
    assert_eq!(replies.due(start + TimeDelta::minutes(5), window).len(), 1, "five windows after the first at most");
}

#[test]
fn a_question_is_its_own_notice_held_back_like_a_completion_unless_bypass_lists_it() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let options = ["Postgres".to_owned(), "SQLite".to_owned()];
    let question = ants_proto::Question::new(
        "Which  database\nfor token ghp_abcdefghijklmnopqrstuvwxyz0123456789?",
        &options,
        false,
    );
    let asking = Envelope::asking(ThreadId::generate(), dev_a.clone(), alice.clone(), question.unwrap());
    let notice = question_notice(&asking).unwrap();
    assert_eq!(
        (notice.title.as_str(), &notice.human, notice.urgent),
        ("dev-a asks: Which database for token [redacted]?", &alice, false)
    );
    let (id, thread) = (asking.id, &asking.thread_id);
    assert!(
        notice.text.ends_with(&format!("\n\nthread {thread}; answer with ants answer {id} NUMBER")),
        "{}",
        notice.text
    );
    assert!(notice.text.contains("1. Postgres") && !notice.text.contains("ghp_"), "{}", notice.text);
    let json = notice_json(&notice, at("2026-03-10T12:00:00Z"));
    assert_eq!(
        (&json["kind"], &json["id"], &json["ant"], &json["thread"]),
        (&json!("question"), &json!(id), &json!("dev-a"), &json!(thread))
    );
    let asked = &json["questions"][0];
    assert_eq!(
        (&asked["options"][0], &asked["multi_select"], &asked["free_text"]),
        (&json!({"label": "Postgres", "description": null}), &json!(false), &json!(false))
    );
    assert!(!asked["question"].as_str().unwrap().contains("ghp_"), "{json}");
    assert!(completion_notice(&asking).is_none() && question_notice(&Envelope::new(dev_a, alice, "hi")).is_none());
    assert_eq!(counted("question", 2), "2 questions");

    let now = at("2026-03-10T12:00:00Z");
    let night = human(&format!("name = \"bob\"\nquiet = \"11:00-13:00\"\n{NTFY_SINK}")).unwrap();
    let prefs = NotifyPrefs::default();
    assert_eq!(held(&night, &notice.about, &prefs, now), Some("quiet hours"));
    let through = human(&format!("name = \"bob\"\nquiet = \"11:00-13:00\"\nbypass = [\"questions\"]\n{NTFY_SINK}"));
    let through = through.unwrap();
    assert_eq!(held(&through, &notice.about, &prefs, now), None, "questions bypass");
    assert!(through.quiet_hours(now).unwrap().ends_with("let questions through"));
    let mut muted = NotifyPrefs::default();
    muted.muted.insert(thread.clone(), now + TimeDelta::hours(1));
    assert_eq!(held(&through, &notice.about, &muted, now), Some("muted thread"), "whatever bypass says");
    let only = "name = \"bob\"\napprovals = false\nescalations = false\nquestions = true\n";
    assert_eq!(
        human(&format!("{only}{NTFY_SINK}")).unwrap().subjects(),
        ["ants.inbox.bob.>"],
        "questions read the inbox"
    );
}
