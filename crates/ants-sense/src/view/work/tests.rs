//! Tests of the open work view: findings, commitments, reminders and tasks listed without writing, no entry without
//! open work, and lists cut past their caps.

use std::sync::Arc;

use serde_json::{Value, json};

use super::*;
use crate::Sense;
use crate::dream::{Advice, ColonyTrails, Evidence, Finding, FindingKind, Judgement, Recipient, Start, Verdict};
use crate::testing::{context, events, human, sense, tool};
use crate::tools::Handler;

async fn created(sense: &Sense, handler: Handler, field: &str, arguments: Value) -> String {
    let answer = tool(sense, handler, arguments, context(human())).await;
    assert!(!answer.is_error, "{answer:?}");
    answer.json[field]["id"].as_str().unwrap().to_owned()
}

fn finding(id: &str, sage: bool) -> Finding {
    let advice = sage.then(|| Advice {
        kind: "lesson".into(),
        evidence: Evidence { files: vec!["src/lib.rs:3".into()], ..Evidence::default() },
        action: "Remember to rebase.".into(),
        follows: None,
        trail: None,
    });
    let kind = if sage { FindingKind::Sage } else { FindingKind::Distill };
    let text = format!("Finding {id}.");
    Finding { id: id.into(), kind, text, items: vec![], untrusted: false, key: None, count: None, advice, late: false }
}

fn verdict(finding: &str, verdict: Judgement) -> Verdict {
    let reason = format!("{verdict} for a reason.");
    Verdict { finding: finding.into(), verdict, reason, items: vec![], trails: vec![], at: context(human()).at }
}

fn entry<'a>(entries: &'a [MemoryEntry], key: &str) -> &'a str {
    let found = entries.iter().find(|entry| entry.key == key);
    &found.unwrap_or_else(|| panic!("no {key:?} in {entries:#?}")).content
}

#[tokio::test]
async fn the_view_lists_open_findings_commitments_reminders_and_tasks_writing_nothing() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let writer = context(human());
    let now = writer.at;
    let sage = Recipient::Sage("lead".parse().unwrap());
    let (threads, commits, vitals, trails) = (vec![], vec![], vec![], &ColonyTrails::UNREAD);
    let start = Start { at: now, recipient: sage, threads, commits, writer: &writer, vitals, trails };
    let begun = dream::begin(&*items, start).await.unwrap().unwrap();
    let mut answered = begun.clone();
    answered.state = State::Fallback;
    answered.findings = vec![finding("1.1", true), finding("1.2", false), finding("1.3", false)];
    answered.verdicts = vec![verdict("1.1", Judgement::Applied), verdict("1.2", Judgement::Deferred)];
    answered.review.as_mut().unwrap().late = vec![finding("1.4", true)];
    answered.held = vec![finding("1.5", false)];
    items.write(&[begun.change(&answered, 1)], "test").await.unwrap();
    let commitment = json!({"title": "Ship the parser", "promisee": "alice", "due": "2025-01-01T10:00:00Z"});
    let commitment = created(&sense, promise::promise, "promise", commitment).await;
    let reminder = json!({"title": "Check CI", "trigger": {"kind": "after", "seconds": 600}});
    let reminder = created(&sense, remind::remind, "reminder", reminder).await;
    let new = |title: &str, priority| json!({"action": "create", "title": title, "priority": priority});
    let deferred = json!({"action": "create", "title": "Write the docs", "priority": 3, "status": "deferred"});
    let later = created(&sense, task::task, "task", deferred).await;
    let first = created(&sense, task::task, "task", new("Fix the parser", 1)).await;
    let closed = created(&sense, task::task, "task", new("Done already", 0)).await;
    let claim = tool(&sense, task::task, json!({"action": "claim", "id": closed}), context(human())).await;
    let close = json!({"action": "close", "id": closed, "token": claim.json["token"]});
    assert!(!tool(&sense, task::task, close, context(human())).await.is_error);

    let logged = events(&kept).await.len();
    let entries = entries(&*items, now).await.unwrap();
    assert_eq!(events(&kept).await.len(), logged, "the view writes nothing");
    let keys: Vec<_> = entries.iter().map(|entry| entry.key.as_str()).collect();
    assert_eq!(keys, [FINDINGS, COMMITMENTS, REMINDERS, TASKS]);
    let findings = entry(&entries, FINDINGS);
    let head = "Dream 1 (fallback): 2 of 3 findings unanswered or deferred, the ant answering them with \
                sense_dream; 1 held unshown under the sage's answer, which the next dream offers first; 1 given \
                late by sage lead, which the next dream offers.\n";
    assert!(findings.starts_with(head), "{findings}");
    assert!(!findings.contains("1.1"), "an applied finding is not open: {findings}");
    assert!(findings.contains("- 1.2 (distill): Finding 1.2.\n  deferred: deferred for a reason.\n"), "{findings}");
    assert!(findings.contains("- 1.3 (distill): Finding 1.3.\n- 1.5 (distill): Finding 1.5.\n  Held unshown.\n- 1.4"));
    assert!(findings.ends_with("Suggested action: Remember to rebase.\n  Given late."), "{findings}");
    let commitments = entry(&entries, COMMITMENTS);
    assert!(commitments.starts_with(&format!("- {commitment} (revision 1, open, overdue): Ship the parser")));
    let reminders: Vec<_> = entry(&entries, REMINDERS).lines().collect();
    assert!(reminders[0].ends_with(&format!("; commitment {commitment}")), "{reminders:#?}");
    assert!(reminders[1].starts_with(&format!("- {reminder} (revision 1, upcoming): Check CI")), "{reminders:#?}");
    for (entry, kind) in entries.iter().zip(["findings", "commitments", "reminders", "tasks"]) {
        assert_eq!((entry.fields.section.as_deref(), entry.fields.kind.as_deref()), (Some("work"), Some(kind)));
    }
    let rows = &entries[2].fields.triggers;
    assert_eq!(rows.len(), reminders.len(), "a row for each line: {rows:#?}");
    assert_eq!(rows[0].commitment.as_deref(), Some(commitment.as_str()), "{rows:#?}");
    let row = &rows[1];
    assert_eq!((row.id.as_str(), row.revision, row.title.as_str()), (reminder.as_str(), Some(1), "Check CI"));
    assert_eq!((row.kind.as_str(), row.status.as_str()), ("reminder", "upcoming"));
    assert_eq!((row.trigger.as_deref(), row.last_fired, row.commitment.as_ref()), (Some("after 600 s"), None, None));
    assert_eq!(row.next_due, Some(now + chrono::Duration::seconds(600)));
    assert_eq!(row.thread, Some(context(human()).thread));
    assert!(entries.iter().filter(|entry| entry.key != REMINDERS).all(|entry| entry.fields.triggers.is_empty()));
    let tasks = entry(&entries, TASKS);
    let lines: Vec<_> = tasks.lines().collect();
    assert_eq!(lines.len(), 2, "{tasks}");
    assert!(lines[0].starts_with(&format!("- {first} P1 open: Fix the parser")), "{tasks}");
    assert!(lines[1].starts_with(&format!("- {later} P3 deferred: Write the docs")), "{tasks}");
}

#[tokio::test]
async fn an_ant_without_open_work_shows_no_entry_of_it() {
    let (_sense, kept) = sense().await;
    assert_eq!(entries(&*kept, Utc::now()).await.unwrap(), []);
}

#[test]
fn lists_past_their_caps_are_cut_and_counted() {
    let lines =
        |count: usize, bytes: usize| plain((0..count).map(|n| format!("- {n} {}", "x".repeat(bytes))).collect());
    let many = listed(TASKS, "tasks", String::new(), lines(LISTED + 5, 10)).unwrap().content;
    assert_eq!(many.lines().count(), LISTED + 1, "{many}");
    assert!(many.ends_with("\n5 more left out."), "{many}");
    let long = listed(TASKS, "tasks", "Head.".into(), lines(5, MEMORY_FILE_CAP / 3)).unwrap().content;
    assert!(long.len() <= MEMORY_FILE_CAP, "{}", long.len());
    assert!(long.starts_with("Head.\n- 0 "), "{}", &long[..20]);
    assert!(long.ends_with("\n3 more left out."), "{}", &long[long.len() - 20..]);
    assert_eq!(listed(TASKS, "tasks", "Head.".into(), lines(1, 10)).unwrap().content, "Head.\n- 0 xxxxxxxxxx");
    assert_eq!(listed(TASKS, "tasks", "Head.".into(), Vec::new()), None);
}

#[test]
fn the_rows_of_a_list_count_within_its_cap() {
    let row = |n: usize| TriggerRow {
        id: format!("reminder-{n}"),
        revision: Some(1),
        kind: "reminder".into(),
        title: "t".repeat(MEMORY_FILE_CAP / 8),
        status: "upcoming".into(),
        thread: None,
        trigger: Some("after 600 s".into()),
        next_due: None,
        last_fired: None,
        commitment: None,
        failing_checks: None,
        last_error: None,
    };
    let lines = (0..10).map(|n| (format!("- reminder-{n}: {}", "x".repeat(MEMORY_FILE_CAP / 8)), Some(row(n))));
    let entry = listed(REMINDERS, "reminders", String::new(), lines.collect()).unwrap();
    let shown = entry.content.lines().count() - 1;
    assert_eq!(entry.fields.triggers.len(), shown, "a row for each line shown");
    assert!((2..5).contains(&shown), "the rows take their share of the cap: {shown}");
    assert!(entry.content.ends_with(&format!("\n{} more left out.", 10 - shown)), "{}", entry.content);
    assert!(entry.content.len() + entry.fields.size() <= MEMORY_FILE_CAP);
}
