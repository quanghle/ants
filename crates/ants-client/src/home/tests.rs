//! Tests of what waits on a human, its home: the reasons a thread is on it and their order, what makes a thread stuck
//! or finished, the messages listed, threads marked done, snoozes and groups of tool requests.

use std::collections::HashSet;
use std::time::Duration;

use ants_proto::{
    Acting, ApprovalRequest, DeadLetter, Envelope, Escalation, EscalationReason, Mention, NoticeKind, Question,
    ReadMarker, Reminder, Reminders, ThreadId, ThreadSnoozes, ToolCall, UserId, Uuid, Verdict, subject,
};
use chrono::{DateTime, TimeDelta, Utc};

use super::*;
use crate::facts::Facts;
use crate::p;
use crate::parser::{Lookups, Parser};

/// The time `s` seconds into the tests.
fn at(s: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + s, 0).unwrap()
}

/// Honours `pb`'s claims to act for alice, whose assistant it is.
struct Assisting;

impl Lookups for Assisting {
    fn acting(&self, from: &UserId, claim: Option<&UserId>) -> Acting {
        match claim {
            Some(human) if from.as_str() == "pb" && human.as_str() == "alice" => Acting::For(human.clone()),
            Some(human) => Acting::Forged(human.clone()),
            None => Acting::Own,
        }
    }
}

/// What alice's client read, bob and alice the humans, each entry stored a second after the one before.
struct Home {
    me: UserId,
    parser: Parser,
    facts: Facts,
    threads: Vec<ThreadId>,
    marker: ReadMarker,
    done: HashSet<ThreadId>,
    requests: Vec<ApprovalRequest>,
    reminders: Reminders,
    snoozes: ThreadSnoozes,
    seq: u64,
}

impl Home {
    fn new() -> Self {
        Self {
            me: p("alice"),
            parser: Parser::default(),
            facts: Facts::default(),
            threads: Vec::new(),
            marker: ReadMarker::default(),
            done: HashSet::new(),
            requests: Vec::new(),
            reminders: Reminders::default(),
            snoozes: ThreadSnoozes::default(),
            seq: 0,
        }
    }

    /// Read what was stored on `subject`; its sequence.
    fn read(&mut self, subject: &str, payload: Vec<u8>) -> u64 {
        self.seq += 1;
        let entry = self.parser.entry_with(&Assisting, self.seq, at(self.seq as i64), subject, &payload).unwrap();
        if !self.threads.contains(&entry.thread_id) {
            self.threads.push(entry.thread_id.clone());
        }
        self.facts.add(&entry, &self.me);
        self.seq
    }

    fn message(&mut self, envelope: &Envelope) -> u64 {
        self.read(&envelope.subject(), serde_json::to_vec(envelope).unwrap())
    }

    fn say(&mut self, thread: &ThreadId, from: &str, to: &str, text: &str) -> u64 {
        self.message(&Envelope::on_thread(thread.clone(), p(from), p(to), text))
    }

    fn whys(&self, now: DateTime<Utc>) -> Vec<(Why, ThreadId)> {
        items(self, now).into_iter().map(|i| (i.why, i.thread)).collect()
    }
}

impl Source for Home {
    type Settled = ();

    fn me(&self) -> &UserId {
        &self.me
    }

    fn facts(&self) -> &Facts {
        &self.facts
    }

    fn threads(&self) -> impl Iterator<Item = &ThreadId> {
        self.threads.iter()
    }

    fn is_human(&self, user: &UserId) -> bool {
        matches!(user.as_str(), "alice" | "bob")
    }

    fn unread(&self, seq: u64) -> bool {
        !self.marker.is_read(seq)
    }

    fn done(&self, thread: &ThreadId) -> bool {
        self.done.contains(thread)
    }

    fn requests(&self, _: DateTime<Utc>) -> Vec<(ApprovalRequest, Option<()>)> {
        self.requests.iter().map(|r| (r.clone(), None)).collect()
    }

    fn reminders(&self) -> &Reminders {
        &self.reminders
    }

    fn snoozes(&self) -> &ThreadSnoozes {
        &self.snoozes
    }
}

fn request(ant: &str, thread: &ThreadId, title: &str, requested_at: DateTime<Utc>) -> ApprovalRequest {
    let tool = ToolCall { kind: "execute".into(), title: title.into(), ..Default::default() };
    let request =
        ApprovalRequest::new(p(ant), thread.clone(), tool, vec![Verdict::AllowOnce], Duration::from_secs(600));
    ApprovalRequest { requested_at, ..request }
}

fn asking(thread: &ThreadId, from: &str) -> Envelope {
    let question = Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], false).unwrap();
    Envelope::asking(thread.clone(), p(from), p("alice"), question)
}

fn escalation(thread: &ThreadId, from: &str) -> Envelope {
    let mut escalation = Envelope::on_thread(thread.clone(), p(from), p("alice"), "Out of turns");
    let reason = EscalationReason::TurnBudget;
    escalation.escalation = Some(Box::new(Escalation::new(p(from), thread.clone(), reason)));
    escalation
}

#[test]
fn a_thread_is_listed_once_under_its_most_pressing_reason_the_others_its_badges() {
    let mut home = Home::new();
    let [busy, chat] = [(); 2].map(|()| ThreadId::generate());
    let mention = Envelope {
        mention: Some(Mention { channel: "ops".parse().unwrap(), post: Uuid::now_v7(), author: None }),
        ..Envelope::on_thread(busy.clone(), p("bob"), p("alice"), "@alice look")
    };
    home.message(&mention);
    home.message(&Envelope { id: Uuid::now_v7(), text: "again".into(), ..mention.clone() });
    let asked = asking(&busy, "dev-a");
    let seq = home.message(&asked);
    home.say(&chat, "bob", "alice", "Lunch?");
    home.say(&chat, "bob", "alice", "Lunch!");
    home.requests.push(request("dev-a", &busy, "git push", at(0)));

    let found = items(&home, at(10));
    let shown: Vec<(Why, &ThreadId, &str)> = found.iter().map(|i| (i.why, &i.thread, i.text.as_str())).collect();
    assert_eq!(
        shown,
        [
            (Why::Approval, &busy, "dev-a asks to run execute: git push"),
            (Why::Question, &busy, "dev-a: Ship it?"),
            (Why::Message, &chat, "bob: Lunch?"),
        ]
    );
    assert_eq!((found[1].seq, found[1].question.as_ref().map(|q| q.0)), (Some(seq), Some(asked.id)));
    assert_eq!(found[1].also, [(Why::Mention, 2)], "the mentions are the question's badge");
    assert_eq!(Why::Mention.badge(2), "2 mentions");
    assert_eq!((found[2].count, found[2].seq), (2, Some(4)), "two unread, the first listed");

    home.marker.mark(4);
    home.marker.mark(5);
    assert_eq!(home.whys(at(10)).len(), 2, "read, the messages leave");
}

#[test]
fn a_thread_is_stuck_on_an_escalation_a_dead_letter_or_an_ant_unable_to_serve_until_a_human_speaks() {
    let mut home = Home::new();
    let [escalated, dead, quota] = [(); 3].map(|()| ThreadId::generate());
    home.message(&escalation(&escalated, "dev-a"));
    let sent = Envelope::on_thread(dead.clone(), p("alice"), p("dev-b"), "deploy");
    home.message(&sent);
    let letter = DeadLetter::of_envelope(p("dev-b"), 2, 5, "harness failed", sent);
    home.read(&subject::dead(&p("dev-b")), serde_json::to_vec(&letter).unwrap());
    let notice = Envelope {
        notice: Some(NoticeKind::Quota),
        ..Envelope::on_thread(quota.clone(), p("dev-c"), p("alice"), "[ants] out of quota")
    };
    home.message(&notice);
    let stuck = |home: &Home| {
        let found = home.whys(at(10)).into_iter().filter(|(why, _)| *why == Why::Stuck);
        found.map(|(_, thread)| thread).collect::<Vec<_>>()
    };
    assert_eq!(stuck(&home), [escalated.clone(), dead.clone(), quota.clone()]);
    assert_eq!(super::stuck(&home, &dead).map(|a| a.dead), Some(Some(3)), "on the dead letter");

    // The assistant's word for alice ends the escalation; the ant serving again ends the wait.
    home.message(&Envelope::on_thread(escalated, p("pb"), p("dev-a"), "go on").on_behalf(Some(p("alice"))));
    home.say(&quota, "dev-c", "alice", "back");
    assert_eq!(stuck(&home), std::slice::from_ref(&dead));
    home.say(&dead, "bob", "dev-b", "retry");
    assert!(stuck(&home).is_empty(), "bob is human");
}

#[test]
fn only_other_humans_messages_are_listed_and_an_ant_s_answer_finishes_a_thread_the_human_owns() {
    let mut home = Home::new();
    let [owned, theirs] = [(); 2].map(|()| ThreadId::generate());
    home.message(&Envelope::on_thread(owned.clone(), p("alice"), p("dev-a"), "deploy").with_owner(Some(p("alice"))));
    let answer = home.say(&owned, "dev-a", "alice", "deployed");
    home.say(&theirs, "dev-b", "alice", "fyi");
    home.message(&Envelope::on_thread(theirs.clone(), p("pb"), p("bob"), "hi bob").on_behalf(Some(p("alice"))));
    home.message(&Envelope::on_thread(theirs.clone(), p("pc"), p("alice"), "for carol").on_behalf(Some(p("carol"))));

    let found = items(&home, at(10));
    assert_eq!(found.len(), 1, "no ant's message, nor a claim not honoured: {found:?}");
    assert_eq!(
        (found[0].why, &found[0].text, found[0].seq),
        (Why::Finished, &"dev-a answered".to_owned(), Some(answer))
    );

    // Delegated on, the ant waits on another: not finished.
    home.say(&owned, "dev-a", "dev-b", "check it");
    assert!(home.whys(at(10)).is_empty());
    home.say(&owned, "dev-b", "dev-a", "fine");
    home.say(&owned, "dev-a", "alice", "checked");
    assert_eq!(home.whys(at(10)), [(Why::Finished, owned.clone())]);
    home.marker.mark(home.seq);
    assert!(home.whys(at(10)).is_empty(), "read, it leaves");
}

#[test]
fn an_answer_for_the_human_by_its_assistant_answers_its_question() {
    let mut home = Home::new();
    let thread = ThreadId::generate();
    let asked = asking(&thread, "dev-a");
    home.message(&asked);
    let forged =
        Envelope::answering(&asked, p("pc"), vec![ants_proto::Pick::option(1)]).unwrap().on_behalf(Some(p("alice")));
    home.message(&forged);
    assert_eq!(home.whys(at(10)), [(Why::Question, thread)], "pc is not her assistant");
    let answer =
        Envelope::answering(&asked, p("pb"), vec![ants_proto::Pick::option(1)]).unwrap().on_behalf(Some(p("alice")));
    home.message(&answer);
    assert!(home.whys(at(10)).is_empty());
}

#[test]
fn a_thread_marked_done_lists_only_its_tool_requests_and_reminder() {
    let mut home = Home::new();
    let thread = ThreadId::generate();
    home.message(&asking(&thread, "dev-a"));
    home.message(&escalation(&thread, "dev-a"));
    home.say(&thread, "bob", "alice", "Lunch?");
    home.requests.push(request("dev-a", &thread, "git push", at(0)));
    let reminder = Reminder { at: at(0), due: at(5), message: None, note: "look".into(), pushed: false };
    home.reminders.remind(&thread, reminder);
    assert_eq!(home.whys(at(10)).len(), 2, "the request, and the thread under its question");

    home.done.insert(thread.clone());
    let found = items(&home, at(10));
    let shown: Vec<(Why, &str)> = found.iter().map(|i| (i.why, i.text.as_str())).collect();
    assert_eq!(
        shown,
        [(Why::Approval, "dev-a asks to run execute: git push"), (Why::Reminder, &*format!("look · {thread}"))]
    );
}

#[test]
fn a_snooze_holds_until_something_new_arrives_and_an_ant_s_requests_on_a_thread_are_one_group() {
    let mut home = Home::new();
    let [snoozed, asked] = [(); 2].map(|()| ThreadId::generate());
    home.say(&snoozed, "bob", "alice", "Later");
    home.snoozes.snooze(&snoozed, at(1), at(1) + TimeDelta::hours(1));
    for (n, title) in [(1, "git push"), (2, "rm -rf x")] {
        home.requests.push(request("dev-a", &asked, title, at(n)));
    }
    home.requests.push(request("dev-b", &asked, "ls", at(3)));

    let found = items(&home, at(10));
    assert_eq!(found.len(), 2, "{found:?}");
    assert_eq!(found[0].text, "dev-a asks to run 2 tools, the oldest execute: git push");
    assert_eq!((found[0].count, found[0].id()), (2, Id::Requests(p("dev-a"), asked)));
    assert_eq!(found[0].of_kind().map(|r| r.len()), Some(2));
    assert_eq!(found[1].id(), Id::Request(p("dev-b"), home.requests[2].id));
    assert_eq!(super::snoozed(&home, at(10)).get(&snoozed), Some(&(at(1) + TimeDelta::hours(1))));

    home.say(&snoozed, "bob", "alice", "Now?");
    let found = home.whys(at(10));
    assert!(found.contains(&(Why::Message, snoozed.clone())), "the snooze ended: {found:?}");
    assert!(super::snoozed(&home, at(10)).is_empty());
}
