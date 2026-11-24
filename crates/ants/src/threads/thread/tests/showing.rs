//! Tests of how a thread's entries show: messages and reactions, quoted replies, text someone else wrote, forged
//! messages, wakes, questions and answers, prompt ends and tool calls.

use super::*;

#[test]
fn a_thread_shows_its_messages_with_who_sent_them_to_whom_and_their_reactions() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let ask = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "fix it").with_owner(Some(alice));
    let reply = ask.reply(dev_a, "done\nsee the diff", ants_proto::Role::Human).unwrap();
    let mut entries = entries(&[(ask.subject(), json(&ask)), (reply.subject(), json(&reply))]);
    let eyes = Reacted::new("eyes".parse().unwrap(), vec![p("alice"), p("dev-a")]);
    entries[1].reactions = vec![eyes];
    let shown = render(&entries);
    assert!(shown.starts_with("● alice -> dev-a · "), "{shown}");
    assert!(shown.contains(&format!(" · message {}\n  fix it\n\n● dev-a -> alice · ", ask.id)), "{shown}");
    assert!(shown.ends_with("\n  done\n  see the diff\n  reacted eyes: alice, dev-a\n"), "{shown}");
}

#[test]
fn a_quoted_reply_shows_the_message_it_replies_to_before_its_text() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let done = Envelope::on_thread(t("t1"), dev_a.clone(), alice.clone(), "done, see the diff");
    let quote = ants_proto::Quote::new(dev_a.clone(), &done.text);
    let why = Envelope::on_thread(t("t1"), alice.clone(), dev_a, "why?").quoting(done.id, quote.clone());
    let post = ChannelPost::on_thread("ops".parse().unwrap(), t("t1"), alice, "@dev-a and?").quoting(done.id, quote);
    let entries = entries(&[(why.subject(), json(&why)), (post.subject(), json(&post))]);
    assert!(entries.iter().all(|e| e.quote == why.quote), "{entries:?}");
    let shown = render(&entries);
    assert!(shown.contains("\n  ↪ replying to dev-a: done, see the diff\n  why?\n"), "{shown}");
    assert!(shown.contains("\n  ↪ replying to dev-a: done, see the diff\n  @dev-a and?\n"), "{shown}");
}

#[test]
fn text_someone_else_wrote_shows_without_control_or_reordering_characters() {
    use crate::test_support::hostile::{HOSTILE, assert_printable};
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let ask = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), HOSTILE);
    let quote = ants_proto::Quote::new(alice.clone(), HOSTILE);
    let reply = Envelope::on_thread(t("t1"), dev_a.clone(), alice, HOSTILE).quoting(ask.id, quote);
    let post = ChannelPost::on_thread("ops".parse().unwrap(), t("t1"), dev_a.clone(), HOSTILE);
    let dead = DeadLetter::of_payload(dev_a, 7, 3, HOSTILE, HOSTILE.as_bytes());
    let read = entries(&[
        (ask.subject(), json(&ask)),
        (reply.subject(), json(&reply)),
        (post.subject(), json(&post)),
        (dead.subject(), json(&dead)),
    ]);
    let shown = render(&read);
    assert_printable(&shown);
    assert!(shown.contains("\n  [2J]0;pwned]52;c;cHduZWQ=spoof31m\n  line two\n"), "{shown}");

    let mut threads = Threads::new(Some("spoof"));
    read.iter().for_each(|e| threads.add(e));
    let (mut listed, _) =
        threads.finish(&Select { grep: Some("spoof".into()), ..Default::default() }, &ThreadMarks::default());
    listed[0].title = Some(HOSTILE.to_owned());
    let table = table(&listed);
    assert_printable(&table);
    assert_eq!(table.lines().count(), 3, "a header, the thread and the line matching: {table}");
}

#[test]
fn a_message_on_another_sender_s_subject_is_marked_forged() {
    let e = Envelope::on_thread(t("t1"), p("alice"), p("dev-a"), "hi");
    let entries = entries(&[("ants.inbox.dev-a.mallory".to_owned(), json(&e))]);
    assert!(entries[0].forged);
    let shown = render(&entries);
    assert!(shown.starts_with("● alice -> dev-a (forged) · "), "{shown}");
    assert!(shown.ends_with(&format!(" · message {}\n  hi\n", e.id)), "{shown}");
}

#[test]
fn a_provider_s_wake_shows_as_one_only_on_the_ant_s_own_inbox() {
    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    let wake = Envelope::on_thread(t("t1"), dev_a.clone(), dev_a.clone(), "standup").woken_by("sense");
    let claimed = Envelope::on_thread(t("t1"), dev_b, dev_a, "standup").woken_by("sense");
    let entries = entries(&[(wake.subject(), json(&wake)), (claimed.subject(), json(&claimed))]);
    assert_eq!((entries[0].provider.as_deref(), entries[1].provider.as_deref()), (Some("sense"), None));
    let shown = render(&entries);
    assert!(shown.starts_with("● dev-a -> dev-a [wake from sense] · "), "{shown}");
    assert!(shown.contains(&format!(" · message {}\n  standup\n\n● dev-b -> dev-a · ", wake.id)), "{shown}");
}

#[test]
fn a_question_shows_its_options_and_the_valid_answers_to_it() {
    let alice = p("alice");
    let options = ["yes".to_owned(), "not yet".to_owned()];
    let question = ants_proto::Question::new("Ship it?", &options, true).unwrap();
    let asked = Envelope::asking(t("t1"), p("dev-a"), alice.clone(), question.clone());
    let answer = |pick| Envelope::answering(&asked, alice.clone(), vec![pick]).unwrap();
    let picked = answer(ants_proto::Pick::option(2));
    let typed = answer(ants_proto::Pick::Typed("after lunch".to_owned()));
    let mut wrong = answer(ants_proto::Pick::option(1));
    wrong.answer.as_mut().unwrap().picks[0] = ants_proto::Pick::option(3);
    let forged = answer(ants_proto::Pick::option(1));
    let read = entries(&[
        (asked.subject(), json(&asked)),
        (picked.subject(), json(&picked)),
        (typed.subject(), json(&typed)),
        (wrong.subject(), json(&wrong)),
        ("ants.inbox.dev-a.mallory".to_owned(), json(&forged)),
    ]);
    assert_eq!(read[0].question.as_ref(), Some(&question.into()));
    let pick = |e: &Entry| e.answers(&read[0]).map(|a| a[0].pick.clone());
    assert_eq!(pick(&read[1]), Some(ants_proto::Pick::option(2)));
    assert_eq!(pick(&read[2]), Some(ants_proto::Pick::Typed("after lunch".to_owned())));
    assert_eq!((&read[3].answer, &read[4].answer), (&None, &None), "invalid or forged: ordinary messages");
    let by: Vec<Uuid> = answers_to(&read[0], &read).filter_map(|(e, _)| e.id).collect();
    assert_eq!(by, [picked.id, typed.id]);

    let shown = render(&read);
    assert!(shown.contains(&format!(" · message {}\n  Ship it?\n", asked.id)), "{shown}");
    assert!(shown.contains("\n  1. yes\n  2. not yet\n"), "{shown}");
    assert!(shown.contains("\n  Answer with an option's number or in own words.\n"), "{shown}");
    assert!(shown.contains("\n  answered by alice: option 2, not yet\n"), "{shown}");
    assert!(shown.contains("\n  answered by alice, typed: after lunch\n"), "{shown}");
    assert!(!shown.contains("ants answer"), "answered: {shown}");
    assert!(shown.contains(&format!("● alice -> dev-a [answers {} with option 2] · ", asked.id)), "{shown}");
    assert!(shown.contains(&format!(" · message {}\n  not yet\n", picked.id)), "{shown}");
    assert!(shown.contains(&format!("[answers {} as typed]", asked.id)), "{shown}");
    let unanswered = render(&read[..1]);
    assert!(unanswered.contains(&format!("\n  (`ants answer {} N|TEXT` answers it)\n", asked.id)), "{unanswered}");
    let json = serde_json::to_value(Shown::of(&read[0])).unwrap();
    assert_eq!(json["question"]["questions"][0]["options"][1]["label"], "not yet");
}

#[test]
fn an_answer_to_several_questions_shows_a_pick_for_each() {
    let alice = p("alice");
    let regions = ["eu".to_owned(), "us".to_owned(), "ap".to_owned()];
    let regions = ants_proto::Question::new("Which regions?", &regions, false).unwrap().multi().headed("Regions");
    let ship = ants_proto::Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
    let questions = ants_proto::Questions::new(vec![regions, ship]).unwrap();
    let asked = Envelope::asking(t("t1"), p("dev-a"), alice.clone(), questions);
    let picks = vec![ants_proto::Pick::Options(vec![1, 3]), ants_proto::Pick::Typed("after lunch".to_owned())];
    let answer = Envelope::answering(&asked, alice, picks).unwrap();
    let read = entries(&[(asked.subject(), json(&asked)), (answer.subject(), json(&answer))]);
    let shown = render(&read);
    assert!(
        shown.contains("\n  answered by alice: Regions options 1, eu and 3, ap; Q2 typed: after lunch\n"),
        "{shown}"
    );
    assert!(shown.contains(&format!("[answers {} with options 1, 3; typed]", asked.id)), "{shown}");
    let unanswered = render(&read[..1]);
    assert!(unanswered.contains(&format!("(`ants answer {} N,M N|'TEXT'` answers them)", asked.id)), "{unanswered}");
}

#[test]
fn a_prompt_ended_with_no_reply_shows_why_and_its_final_text() {
    use ants_proto::NoReplyWhy::{Completed, ReplyToOwn};

    let (dev_a, thread): (UserId, ThreadId) = (p("dev-a"), "t1".parse().unwrap());
    let at = DateTime::parse_from_rfc3339("2026-11-16T07:50:43Z").unwrap().to_utc();
    let record = |why, reason: Option<&str>, text: &str| {
        let end = ants_proto::TimelineEntry::NoReply { why, reason: reason.map(str::to_owned), text: text.to_owned() };
        Timeline { at, ..Timeline::new(dev_a.clone(), thread.clone(), Uuid::nil(), vec![end], 0) }
    };
    let entries = |record: &Timeline| timeline_entries(7, &record.subject(), &json(record));

    let own = entries(&record(ReplyToOwn, None, "Thanks, noted.\nNothing else."));
    assert_eq!(
        render(&own),
        "  ⎿ 2026-11-16 07:50:43 dev-a  no reply: reply to own message, answer not sent: Thanks, noted.\n      \
         Nothing else.\n"
    );
    let completed = entries(&record(Completed, Some("nothing to add"), ""));
    assert_eq!(render(&completed), "  ⎿ 2026-11-16 07:50:43 dev-a  no reply: completed (nothing to add)\n");
    let line = serde_json::to_value(&completed[0]).unwrap();
    assert_eq!((&line["kind"], &line["seq"]), (&"no_reply".into(), &7.into()));
    assert_eq!(
        line["no_reply"],
        serde_json::json!({"message": Uuid::nil(), "why": "completed", "reason": "nothing to add"})
    );
    let forged = record(Completed, None, "");
    assert!(timeline_entries(7, &subject::timeline(&thread, &p("dev-b")), &json(&forged)).is_empty());
}

#[test]
fn a_tool_call_shows_as_its_name_and_arguments() {
    let named = |name: &str, args: Option<&str>| (name.to_owned(), args.map(str::to_owned));
    assert_eq!(call_label(Some("execute"), "Run `cargo test`"), named("Run", Some("cargo test")));
    assert_eq!(call_label(Some("execute"), "ls -la"), named("Run", Some("ls -la")));
    assert_eq!(call_label(Some("execute"), ""), named("Run", None));
    assert_eq!(call_label(Some("read"), "Read src/x.rs"), named("Read", Some("src/x.rs")));
    assert_eq!(call_label(Some("read"), "src/x.rs"), named("Read", Some("src/x.rs")), "the kind names it");
    assert_eq!(call_label(Some("search"), "Grep TODO"), named("Grep", Some("TODO")));
    assert_eq!(call_label(Some("think"), "Think"), named("Think", None));
    assert_eq!(call_label(Some("other"), "browser_snapshot"), named("browser_snapshot", None));
    assert_eq!(call_label(None, "Read src/x.rs"), named("Read src/x.rs", None), "no kind: its title as it is");
}
