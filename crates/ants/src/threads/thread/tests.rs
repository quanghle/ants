//! Tests of `ants thread`: how its entries show (those of every kind of stored message are tested where they are made,
//! in ants-client), forged messages, titles, listing, picking by id, replies and forwards, tool calls and requests,
//! prompt ends, and sealed messages.

use std::assert_matches;

use ants_client::entry::last_reopening;
use ants_daemon::channel::CLOCK_SLACK;
use ants_daemon::records::reactions::Reacted;
use ants_proto::{
    Acting, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, DecidedBy, Envelope, Escalation, EscalationReason,
    Humans, NotesPost, SummaryRequest, ToolCall, Verdict,
};
use serde::Serialize;

use super::pick::{made_within, select, select_any};
use super::text::state;
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

#[tokio::test]
async fn ask_reply_to_answers_the_message_s_author_on_its_thread_quoting_it() {
    use crate::cmd::send::{AskArgs, PostArgs, ask, post};

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::connection::Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let built = asked.reply(dev_a.clone(), "built,\n  all green", ants_proto::Role::Human).unwrap();
    for e in [&asked, &built] {
        js.publish(e.subject(), json(e).into()).await.unwrap().await.unwrap();
    }

    let send = |reply_to: Uuid, thread: Option<Pick>, words: &[&str]| {
        let words = words.iter().map(|w| (*w).to_owned()).collect();
        Box::pin(ask(&nats, AskArgs { thread, reply_to: Some(MessagePick::Id(reply_to)), words, ..Default::default() }))
    };
    send(built.id, None, &["ship", "it"]).await.unwrap();
    let elsewhere = send(built.id, Some(Pick::Id(ThreadId::generate())), &["ship"]).await.unwrap_err().to_string();
    assert!(elsewhere.contains(&format!("is on thread {}, not ", asked.thread_id)), "{elsewhere}");
    let args = PostArgs {
        channel: "ops".parse().unwrap(),
        thread: None,
        reply_to: Some(MessagePick::Id(built.id)),
        queue: false,
        replies: Default::default(),
        input: Default::default(),
        attach: Default::default(),
        words: vec!["deploy?".to_owned()],
    };
    Box::pin(post(&nats, args)).await.unwrap();

    let shown = history(&js, &asked.thread_id, None, Activity::None, None).await.unwrap();
    let replied: Vec<_> = shown[2..].iter().map(|e| (e.to.clone(), e.channel.clone(), e.text.as_str())).collect();
    assert_eq!(replied, [(Some(dev_a.clone()), None, "ship it"), (None, Some("ops".parse().unwrap()), "deploy?")]);
    for e in &shown[2..] {
        assert_eq!(e.quote, Some(ants_proto::Quote::new(dev_a.clone(), "built, all green")), "{e:?}");
    }
}

#[tokio::test]
async fn ask_forward_hands_a_message_and_its_files_to_another_ant_on_a_thread_linked_to_its_own() {
    use crate::cmd::send::{AskArgs, ask};

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::connection::Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let mut built = asked.reply(dev_a.clone(), "built, all green", ants_proto::Role::Human).unwrap();
    let log = ants_daemon::files::reference("build.log", "text/plain", b"ok").unwrap();
    ants_daemon::files::put(&js, &built.attachment_handle(log.id), &log, b"ok").await.unwrap();
    built.attachments = vec![log];
    for e in [&asked, &built] {
        js.publish(e.subject(), json(e).into()).await.unwrap().await.unwrap();
    }

    let words = ["@dev-b", "ship", "it"].map(str::to_owned).to_vec();
    Box::pin(ask(&nats, AskArgs { forward: Some(MessagePick::Id(built.id)), words, ..Default::default() }))
        .await
        .unwrap();

    let sent = recent_messages(&js, &alice).await.unwrap().into_iter().find(|e| e.to.as_ref() == Some(&dev_b));
    let sent = sent.expect("a message to dev-b");
    assert_ne!(sent.thread_id, asked.thread_id, "on a new thread");
    assert_eq!(sent.origin.as_ref(), Some(&asked.thread_id), "linked to the source thread");
    assert!(sent.text.starts_with("ship it\n\nForwarded from dev-a on thread "), "{}", sent.text);
    assert!(sent.text.ends_with("> built, all green\n> 📎 build.log (attached)"), "{}", sent.text);
    let [file] = sent.attachments.as_slice() else { panic!("{:?}", sent.attachments) };
    assert!(file.handle.readable_by(&dev_b), "stored again for dev-b");
    let (_, bytes) =
        crate::messages::attachments::fetch(&js, &file.handle, Some(&file.attachment), None).await.unwrap();
    assert_eq!(bytes, b"ok");
}

#[tokio::test]
async fn ask_forward_thread_hands_its_kept_tldr_to_another_ant_and_refuses_without_one() {
    use crate::cmd::send::{AskArgs, ask};

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::connection::Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "fix the flaky test");
    js.publish(asked.subject(), json(&asked).into()).await.unwrap().await.unwrap();

    let send = || {
        let forward_thread = Some(Pick::Id(asked.thread_id.clone()));
        let words = ["@dev-b", "follow", "up"].map(str::to_owned).to_vec();
        Box::pin(ask(&nats, AskArgs { forward_thread, words, ..Default::default() }))
    };
    let refused = send().await.unwrap_err().to_string();
    assert!(refused.starts_with(&format!("no TL;DR of thread {} is kept", asked.thread_id)), "{refused}");

    let tldr = ants_proto::ThreadSummary {
        thread_id: asked.thread_id.clone(),
        text: "dev-a fixed the flaky test".to_owned(),
        by: p("messenger"),
        asker: alice.clone(),
        at: chrono::Utc::now(),
        upto: 1,
        tokens: 40,
    };
    crate::threads::thread_meta::open(&nats, &js).await.unwrap().put_summary(&tldr).await.unwrap();
    send().await.unwrap();

    let sent = recent_messages(&js, &alice).await.unwrap().into_iter().find(|e| e.to.as_ref() == Some(&dev_b));
    let sent = sent.expect("a message to dev-b");
    assert_ne!(sent.thread_id, asked.thread_id, "on a new thread");
    assert_eq!(sent.origin.as_ref(), Some(&asked.thread_id), "linked to the source thread");
    let head = format!("follow up\n\nForwarded thread {}, its TL;DR by messenger, ", asked.thread_id);
    assert!(sent.text.starts_with(&head), "{}", sent.text);
    assert!(sent.text.ends_with("UTC:\n> dev-a fixed the flaky test"), "{}", sent.text);
}

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

#[tokio::test]
async fn a_sealed_message_from_a_device_enrolled_mid_follow_opens_everywhere_it_is_read() {
    use ants_proto::Role;

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, bob) = (p("alice"), p("bob"));
    let enrolled = |name: &str| {
        let creds = server.dir().join(format!("alice-{name}.creds"));
        crate::auth::issue(&server.dir().join("auth"), &alice, Role::Human, None, &creds).unwrap();
        creds
    };
    let desk = enrolled("desk");
    crate::connection::Nats::new(&server.url, Some(&desk), None).unwrap().connect("test").await.unwrap();
    let bob_creds = server.creds_as(&bob, Role::Human);
    let bob_nats = crate::connection::Nats::new(&server.url, Some(&bob_creds), None).unwrap();
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
        let alice_nats = crate::connection::Nats::new(&server.url, Some(&laptop), None).unwrap();
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
