//! Tests of the entries stored messages are read into: every kind of stored message on its thread, forged messages,
//! a provider's wake, questions and the valid answers to them, suggested titles, daemon notices, sealed messages
//! without and with lookups, claims to act for a human, and what an ant's timeline records become.

use std::time::Duration;

use ants_proto::{
    ApprovalDecision, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, Escalation, EscalationReason, NoticeKind,
    Pick, Question, Reacted, Role, Sealed, Timeline, Timelines, ToolCall, Verdict,
};
use serde::Serialize;

use super::*;
use crate::entry::last_reopening;
use crate::p;
use crate::sealed::State;

fn t(s: &str) -> ThreadId {
    s.parse().unwrap()
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

/// The entries of `messages`, each a subject and a payload, stored a second apart, read with `lookups`.
fn entries_with(lookups: &dyn Lookups, messages: &[(String, Vec<u8>)]) -> Vec<Entry> {
    let mut parser = Parser::default();
    let stored = messages.iter().enumerate();
    stored
        .filter_map(|(i, (subject, payload))| parser.entry_with(lookups, i as u64 + 1, at(i as i64), subject, payload))
        .collect()
}

fn entries(messages: &[(String, Vec<u8>)]) -> Vec<Entry> {
    entries_with(&NoLookups, messages)
}

#[test]
fn every_kind_of_stored_message_lands_on_its_thread() {
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let ask = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "fix it").with_owner(Some(alice.clone()));
    let reply = ask.reply(dev_a.clone(), "done\nsee the diff", Role::Human).unwrap();
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
        ("ants.inbox.dev-a.alice".to_owned(), br#"{"stream":"ANTS","seq":2}"#.to_vec()),
        ("ants.status.dev-a.t1".to_owned(), b"{}".to_vec()),
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
    assert_eq!(Kind::MESSAGES.len(), 8);
    assert!(entries.iter().all(|e| e.thread_id == t("t1") && !e.forged), "{entries:?}");
    assert!(entries.iter().all(|e| !e.kind.on_timeline() && Kind::MESSAGES.contains(&e.kind)));
    assert_eq!(entries[1].route(), "dev-a -> alice");
    assert_eq!(entries[2].route(), "dev-b -> #ops");
    assert_eq!(entries[3].route(), "dev-a -> notes");
    assert!(entries[4].note.as_deref().unwrap().contains("turn_budget"), "{:?}", entries[4]);
    assert_eq!(entries[6].route(), "(a human) -> dev-a");
    assert!(entries[6].text.starts_with("allow-once on dev-a/"), "{}", entries[6].text);
    assert_eq!(entries[7].note.as_deref(), Some("after 5 deliveries: harness died"));
    assert_eq!(entries[0].owner, Some(alice));
    assert_eq!(last_reopening(&entries), 5, "no tool request, decision or dead letter reopens a thread");

    assert_eq!(entries[0].id, Some(ask.id));
    assert_eq!((entries[2].id, entries[3].id), (Some(post.id), Some(note.id)));
    assert_eq!((entries[5].id, entries[7].id), (None, None), "a tool request and a dead letter");

    let mut replied = entries[1].clone();
    replied.reactions = vec![Reacted::new("eyes".parse().unwrap(), vec![p("alice"), p("dev-a")])];
    let shown = serde_json::to_value(&replied).unwrap();
    assert_eq!((shown["kind"].as_str(), shown["seq"].as_u64()), (Some("message"), Some(2)));
    assert_eq!(shown["reactions"][0]["reaction"], "eyes");
    assert!(shown.get("forged").is_none() && shown.get("sealed").is_none() && shown.get("acting").is_none());
}

#[test]
fn a_kind_of_stored_message_is_named_in_any_case_and_none_of_a_timeline_s() {
    assert_eq!("post".parse(), Ok(Kind::Post));
    assert_eq!("Escalation".parse(), Ok(Kind::Escalation));
    for name in ["tool", "no reply", "rejected", "ended", "posts", ""] {
        assert!(name.parse::<Kind>().is_err(), "{name}");
    }
    let names: Vec<&str> = Kind::MESSAGES.iter().map(|kind| kind.as_str()).collect();
    assert_eq!(names, ["message", "escalation", "mention", "post", "note", "approval", "decision", "dead"]);
}

#[test]
fn a_message_on_another_sender_s_subject_is_marked_forged() {
    let e = Envelope::on_thread(t("t1"), p("alice"), p("dev-a"), "hi");
    let entries = entries(&[("ants.inbox.dev-a.mallory".to_owned(), json(&e))]);
    assert!(entries[0].forged);
    assert_eq!(entries[0].route(), "alice -> dev-a");
}

#[test]
fn a_provider_s_wake_is_one_only_on_the_ant_s_own_inbox() {
    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    let wake = Envelope::on_thread(t("t1"), dev_a.clone(), dev_a.clone(), "standup").woken_by("sense");
    let claimed = Envelope::on_thread(t("t1"), dev_b, dev_a, "standup").woken_by("sense");
    let entries = entries(&[(wake.subject(), json(&wake)), (claimed.subject(), json(&claimed))]);
    assert_eq!((entries[0].provider.as_deref(), entries[1].provider.as_deref()), (Some("sense"), None));
    assert_eq!((entries[0].note.as_deref(), entries[1].note.as_deref()), (Some("wake from sense"), None));
}

#[test]
fn a_watcher_s_wake_is_one_only_as_a_provider_s_wake_on_the_ant_s_own_inbox() {
    let dev_a = p("dev-a");
    let watcher = ants_proto::WatchWake {
        id: "rem-1".into(),
        title: "CI of #7".into(),
        done: false,
        output: Some("red".into()),
        cut: false,
        replaces: Some(Uuid::from_u128(1)),
    };
    let wake =
        Envelope::on_thread(t("t1"), dev_a.clone(), dev_a, "changed").woken_by("sense").watching(Some(watcher.clone()));
    let claimed = Envelope { from: p("dev-b"), ..wake.clone() };
    let unwoken = Envelope { provider: None, ..wake.clone() };
    let entries = entries(&[
        (wake.subject(), json(&wake)),
        (claimed.subject(), json(&claimed)),
        (unwoken.subject(), json(&unwoken)),
        // On another sender's subject: forged.
        ("ants.inbox.dev-a.mallory".to_owned(), json(&wake)),
    ]);
    assert_eq!(entries[0].watcher.as_deref(), Some(&watcher));
    assert!(entries[1..].iter().all(|entry| entry.watcher.is_none()), "{entries:?}");
}

#[test]
fn an_answer_counts_only_when_valid_and_to_a_question_read_before_it() {
    let alice = p("alice");
    let options = ["yes".to_owned(), "not yet".to_owned()];
    let question = Question::new("Ship it?", &options, true).unwrap();
    let asked = Envelope::asking(t("t1"), p("dev-a"), alice.clone(), question.clone());
    let answer = |pick| Envelope::answering(&asked, alice.clone(), vec![pick]).unwrap();
    let picked = answer(Pick::option(2));
    let typed = answer(Pick::Typed("after lunch".to_owned()));
    let mut wrong = answer(Pick::option(1));
    wrong.answer.as_mut().unwrap().picks[0] = Pick::option(3);
    let forged = answer(Pick::option(1));
    let read = entries(&[
        (asked.subject(), json(&asked)),
        (picked.subject(), json(&picked)),
        (typed.subject(), json(&typed)),
        (wrong.subject(), json(&wrong)),
        ("ants.inbox.dev-a.mallory".to_owned(), json(&forged)),
    ]);
    assert_eq!(read[0].question.as_ref(), Some(&question.into()));
    let pick = |e: &Entry| e.answers(&read[0]).map(|a| a[0].pick.clone());
    assert_eq!(pick(&read[1]), Some(Pick::option(2)));
    assert_eq!(pick(&read[2]), Some(Pick::Typed("after lunch".to_owned())));
    assert_eq!((&read[3].answer, &read[4].answer), (&None, &None), "invalid or forged: ordinary messages");
    let json = serde_json::to_value(&read[0]).unwrap();
    assert_eq!(json["question"]["questions"][0]["options"][1]["label"], "not yet");

    // An answer read before its question answers nothing.
    let early = entries(&[(picked.subject(), self::json(&picked)), (asked.subject(), self::json(&asked))]);
    assert_eq!(early[0].answer, None);
}

#[test]
fn messages_and_posts_suggest_titles_unless_forged() {
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let ops: ChannelId = "ops".parse().unwrap();
    let ask = Envelope::on_thread(t("t1"), alice, dev_a.clone(), "fix it");
    let mut titled = ask.reply(dev_a.clone(), "on it", Role::Human).unwrap();
    titled.title = Some("Fix it".to_owned());
    let forged = Envelope { from: dev_b.clone(), ..titled.clone() };
    let post = ChannelPost::on_thread(ops, t("t1"), dev_a, "on it");
    let post = ChannelPost { title: Some("Fix the build".to_owned()), ..post };
    let forged_post = ChannelPost { from: dev_b.clone(), title: Some("Forged".to_owned()), ..post.clone() };
    let note = NotesPost { title: Some("Plan the fix".to_owned()), ..NotesPost::new(t("t1"), dev_b, "plan") };
    let read = entries(&[
        (titled.subject(), json(&forged)),
        (titled.subject(), json(&titled)),
        (post.subject(), json(&forged_post)),
        (post.subject(), json(&post)),
        (note.subject(), json(&note)),
    ]);
    let suggested: Vec<_> = read.iter().map(|e| (e.forged, e.suggested_title.as_deref())).collect();
    let expected = [
        (true, None),
        (false, Some("Fix it")),
        (true, None),
        (false, Some("Fix the build")),
        (false, Some("Plan the fix")),
    ];
    assert_eq!(suggested, expected);
}

#[test]
fn a_daemon_s_notice_is_a_message_marked_so_unless_forged() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let mut notice = Envelope::on_thread(t("t1"), dev_a, alice, "the harness is signed out");
    notice.notice = Some(NoticeKind::SignedOut);
    let read = entries(&[(notice.subject(), json(&notice)), ("ants.inbox.alice.dev-b".to_owned(), json(&notice))]);
    assert_eq!((read[0].kind, read[0].notice), (Kind::Message, Some(NoticeKind::SignedOut)));
    assert_eq!(read[0].note.as_deref(), Some("notice: signed_out"));
    assert_eq!((read[1].forged, read[1].notice, read[1].note.as_deref()), (true, None, None));
}

/// A reader that opens every sealed message as `opened`, or makes `state` of it, and honours claims to act for
/// `alice` alone.
struct Device {
    state: State,
}

impl Lookups for Device {
    fn read(&self, envelope: Envelope) -> (Envelope, Option<State>) {
        if envelope.sealed.is_none() {
            return (envelope, None);
        }
        let mut read = sealed::unopened(envelope, self.state);
        if self.state == State::Opened {
            read.text = "the plan".to_owned();
            read.title = Some("Plans".to_owned());
        }
        (read, Some(self.state))
    }

    fn acting(&self, _from: &UserId, claim: Option<&UserId>) -> Acting {
        match claim {
            Some(human) if human.as_str() == "alice" => Acting::For(human.clone()),
            Some(human) => Acting::Forged(human.clone()),
            None => Acting::Own,
        }
    }
}

#[test]
fn a_sealed_message_is_an_entry_marked_sealed_and_opened_only_by_the_reader_s_lookups() {
    let (alice, bob) = (p("alice"), p("bob"));
    let mut sealed = Envelope::on_thread(t("t1"), bob.clone(), alice.clone(), "");
    let device = ants_proto::DeviceId::generate();
    sealed.sealed = Some(Box::new(Sealed { v: 1, device, keys: Vec::new(), body: "AAAA".into() }));
    sealed.title = Some("In the clear".to_owned());
    let plain = Envelope::on_thread(t("t1"), alice, bob, "ok");
    let messages = [(sealed.subject(), json(&sealed)), (plain.subject(), json(&plain))];

    let read = entries(&messages);
    assert_eq!((read[0].sealed, read[0].text.as_str()), (Some(State::Unreadable), "[sealed]"));
    assert_eq!((&read[0].suggested_title, read[0].matchable()), (&None, false));
    assert_eq!((read[1].sealed, read[1].matchable()), (None, true));
    assert_eq!(serde_json::to_value(&read[0]).unwrap()["sealed"], "unreadable");

    let opened = entries_with(&Device { state: State::Opened }, &messages);
    assert_eq!((opened[0].sealed, opened[0].text.as_str()), (Some(State::Opened), "the plan"));
    assert_eq!((&opened[0].suggested_title, opened[0].matchable()), (&None, true), "no ant seals");
    let forged = entries_with(&Device { state: State::Forged }, &messages);
    assert_eq!((forged[0].sealed, forged[0].text.as_str()), (Some(State::Forged), "[forged]"));

    let dead = DeadLetter::of_envelope(p("dev-a"), 3, 5, "harness died", sealed);
    let read = entries(&[(dead.subject(), json(&dead))]);
    assert_eq!(read[0].text, "[sealed, 3 bytes]");
}

#[test]
fn a_claim_to_act_for_a_human_is_forged_unless_the_reader_s_lookups_honour_it() {
    let (alice, pa, dev_a) = (p("alice"), p("pa"), p("dev-a"));
    let own = Envelope::on_thread(t("t1"), pa.clone(), dev_a.clone(), "mine");
    let for_alice = Envelope::on_thread(t("t1"), pa.clone(), dev_a.clone(), "hers").on_behalf(Some(alice.clone()));
    let for_bob = Envelope::on_thread(t("t1"), pa.clone(), dev_a, "his").on_behalf(Some(p("bob")));
    let post = ChannelPost::on_thread("ops".parse().unwrap(), t("t1"), pa, "posted");
    let post = ChannelPost { on_behalf_of: Some(alice.clone()), ..post };
    let messages = [
        (own.subject(), json(&own)),
        (for_alice.subject(), json(&for_alice)),
        (for_bob.subject(), json(&for_bob)),
        (post.subject(), json(&post)),
    ];
    let acting = |read: &[Entry]| read.iter().map(|e| e.acting.clone()).collect::<Vec<_>>();
    let forged = Acting::Forged(alice.clone());
    assert_eq!(acting(&entries(&messages)), [Acting::Own, forged.clone(), Acting::Forged(p("bob")), forged]);
    let checked = entries_with(&Device { state: State::Unreadable }, &messages);
    let honoured = Acting::For(alice);
    assert_eq!(acting(&checked), [Acting::Own, honoured.clone(), Acting::Forged(p("bob")), honoured]);
    assert_eq!(checked[1].route(), "alice via pa -> dev-a");
    assert_eq!(checked[2].sender(), "pa (forged: for bob)");
}

#[test]
fn an_ant_s_timeline_records_become_entries_of_its_tool_calls_and_prompt_ends() {
    let (dev_a, message) = (p("dev-a"), Uuid::now_v7());
    let record = serde_json::json!({
        "ant": "dev-a", "thread_id": "t1", "message": message, "seq": 1, "at": at(5),
        "entries": [
            { "type": "tool", "id": "c1", "title": "cargo test", "tool_kind": "execute", "status": "completed",
              "duration_ms": 2100, "started": at(3) },
            { "type": "no_reply", "why": "completed", "reason": "nothing to add", "text": "all green" },
        ],
    });
    let record: Timeline = serde_json::from_value(record).unwrap();
    let mut timelines = Timelines::default();
    assert!(timelines.add(&ants_proto::subject::timeline(&t("t1"), &dev_a), record));
    let tool = Entry::tool(7, &timelines.calls()[0]);
    assert_eq!((tool.kind, tool.seq, tool.stored_at), (Kind::Tool, 7, at(3)));
    assert_eq!((tool.from.as_ref(), tool.text.as_str()), (Some(&dev_a), "cargo test"));
    assert_eq!(tool.note.as_deref(), Some("execute, completed, 2.1s"));
    assert_eq!(tool.tool.as_ref().unwrap().message, message);
    let end = Entry::no_reply(7, &timelines.no_replies()[0]);
    assert_eq!((end.kind, end.text.as_str()), (Kind::NoReply, "all green"));
    assert_eq!(end.no_reply.as_ref().unwrap().reason.as_deref(), Some("nothing to add"));
    assert!(tool.kind.on_timeline() && end.kind.on_timeline() && !end.kind.reopens());
    let json = serde_json::to_value(&tool).unwrap();
    assert_eq!((json["kind"].as_str(), json["tool"]["call_id"].as_str()), (Some("tool"), Some("c1")));
}
