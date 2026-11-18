//! The ant's open work in sense's memories view ([`super`]), gathered only when the view is read: the findings of its
//! last dream unanswered or deferred, its sage's among them, then those its sage gave late, which the next dream offers
//! ([`crate::dream`]); its open commitments, overdue first ([`crate::promise`]); its active reminders, fired then
//! upcoming ([`crate::remind`]); and its tasks not closed, by priority then age ([`crate::task`]). Each is one entry
//! listing at most [`LISTED`] within [`MEMORY_FILE_CAP`] bytes, the cap of an entry of a memories read, then counting
//! those left out; none for an empty list. The ant answers its findings and keeps its work with its own tools: nothing
//! here asks the human reading it to act (requirement ants-x39hk.1).

use std::fmt::Write as _;

use ants_proto::MEMORY_FILE_CAP;
use ants_proto::provider::MemoryEntry;
use chrono::{DateTime, Utc};

use crate::dream::{self, State, finding_line};
use crate::store::{Items, StoreError};
use crate::task::{self, Status};
use crate::{promise, remind};

/// The key of the entry of the last dream's open findings.
pub const FINDINGS: &str = "open findings";
/// The key of the entry of the open commitments.
pub const COMMITMENTS: &str = "open commitments";
/// The key of the entry of the active reminders.
pub const REMINDERS: &str = "active reminders";
/// The key of the entry of the tasks not closed.
pub const TASKS: &str = "open tasks";

/// The most items an entry lists.
pub const LISTED: usize = 20;

/// Bytes of an entry kept for the line counting the items left out.
const TAIL: usize = 64;

/// The entries of the open work of the ant whose items are `items`, at `now`.
pub(super) async fn entries(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<MemoryEntry>, StoreError> {
    let [fired, upcoming] = remind::brief(items, now).await?;
    let tasks = task::list(items, usize::MAX).await?;
    let tasks = tasks.iter().filter(|found| found.task.status != Status::Closed).map(task::brief_line).collect();
    Ok([
        findings(items).await?,
        listed(COMMITMENTS, String::new(), promise::brief(items, now).await?.lines),
        listed(REMINDERS, String::new(), fired.lines.into_iter().chain(upcoming.lines).collect()),
        listed(TASKS, String::new(), tasks),
    ]
    .into_iter()
    .flatten()
    .collect())
}

/// The findings of the last dream unanswered or deferred, a deferral with its reason, then those its sage gave late;
/// none without any.
async fn findings(items: &dyn Items) -> Result<Option<MemoryEntry>, StoreError> {
    let Some((dream, _)) = dream::last(items).await? else { return Ok(None) };
    let mut lines = Vec::new();
    for finding in dream.open() {
        let mut line = finding_line(finding, dream.number);
        if let Some(verdict) = dream.verdict(&finding.id) {
            let _ = write!(line, "\n  {}: {}", verdict.verdict, verdict.reason);
        }
        lines.push(line);
    }
    let open = lines.len();
    let late = dream.review.iter().flat_map(|review| &review.late);
    lines.extend(late.map(|finding| format!("{}\n  Given late.", finding_line(finding, dream.number))));
    let answer = match dream.state {
        State::Sent => "the sage's answer awaited",
        State::Answered | State::Fallback => "the ant answering them with sense_dream",
        State::Done => "the next dream offering them again while they hold",
    };
    let mut head = format!(
        "Dream {} ({}): {open} of {} findings unanswered or deferred, {answer}",
        dream.number,
        dream.state,
        dream.findings.len()
    );
    if lines.len() > open {
        let sage = dream.sage.as_ref().map_or_else(|| "its sage".to_owned(), |sage| format!("sage {sage}"));
        let _ = write!(head, "; {} given late by {sage}, which the next dream offers", lines.len() - open);
    }
    head.push('.');
    Ok(listed(FINDINGS, head, lines))
}

/// The entry `key`: `head`, then the first of `lines` fitting [`LISTED`] lines and [`MEMORY_FILE_CAP`] bytes, then one
/// counting those left out; none without lines.
fn listed(key: &str, head: String, lines: Vec<String>) -> Option<MemoryEntry> {
    if lines.is_empty() {
        return None;
    }
    let total = lines.len();
    let mut content = head;
    let mut shown = 0;
    for line in lines.into_iter().take(LISTED) {
        if content.len() + 1 + line.len() > MEMORY_FILE_CAP - TAIL {
            break;
        }
        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str(&line);
        shown += 1;
    }
    if shown < total {
        if !content.is_empty() {
            content.push('\n');
        }
        let _ = write!(content, "{} more left out.", total - shown);
    }
    Some(MemoryEntry { key: key.to_owned(), content })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::*;
    use crate::Sense;
    use crate::dream::{Advice, Evidence, Finding, FindingKind, Judgement, Recipient, Start, Verdict};
    use crate::memory::tests::{context, events, human, sense, tool};
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
            follows_missing: false,
        });
        let kind = if sage { FindingKind::Sage } else { FindingKind::Distill };
        let text = format!("Finding {id}.");
        Finding { id: id.into(), kind, text, items: vec![], untrusted: false, key: None, count: None, advice }
    }

    fn verdict(finding: &str, verdict: Judgement) -> Verdict {
        let reason = format!("{verdict} for a reason.");
        Verdict { finding: finding.into(), verdict, reason, items: vec![], at: context(human()).at }
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
        let start =
            Start { at: now, recipient: sage, threads: vec![], commits: vec![], writer: &writer, vitals: vec![] };
        let begun = dream::begin(&*items, start).await.unwrap().unwrap();
        let mut answered = begun.clone();
        answered.state = State::Fallback;
        answered.findings = vec![finding("1.1", true), finding("1.2", false), finding("1.3", false)];
        answered.verdicts = vec![verdict("1.1", Judgement::Applied), verdict("1.2", Judgement::Deferred)];
        answered.review.as_mut().unwrap().late = vec![finding("1.4", true)];
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
                    sense_dream; 1 given late by sage lead, which the next dream offers.\n";
        assert!(findings.starts_with(head), "{findings}");
        assert!(!findings.contains("1.1"), "an applied finding is not open: {findings}");
        assert!(findings.contains("- 1.2 (distill): Finding 1.2.\n  deferred: deferred for a reason.\n"), "{findings}");
        assert!(findings.contains("- 1.3 (distill): Finding 1.3.\n- 1.4 (sage: lesson): Finding 1.4."), "{findings}");
        assert!(findings.ends_with("Suggested action: Remember to rebase.\n  Given late."), "{findings}");
        let commitments = entry(&entries, COMMITMENTS);
        assert!(commitments.starts_with(&format!("- {commitment} (revision 1, open, overdue): Ship the parser")));
        let reminders: Vec<_> = entry(&entries, REMINDERS).lines().collect();
        assert!(reminders[0].ends_with(&format!("; commitment {commitment}")), "{reminders:#?}");
        assert!(reminders[1].starts_with(&format!("- {reminder} (revision 1, upcoming): Check CI")), "{reminders:#?}");
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
        let lines = |count: usize, bytes: usize| (0..count).map(|n| format!("- {n} {}", "x".repeat(bytes))).collect();
        let many = listed(TASKS, String::new(), lines(LISTED + 5, 10)).unwrap().content;
        assert_eq!(many.lines().count(), LISTED + 1, "{many}");
        assert!(many.ends_with("\n5 more left out."), "{many}");
        let long = listed(TASKS, "Head.".into(), lines(5, MEMORY_FILE_CAP / 3)).unwrap().content;
        assert!(long.len() <= MEMORY_FILE_CAP, "{}", long.len());
        assert!(long.starts_with("Head.\n- 0 "), "{}", &long[..20]);
        assert!(long.ends_with("\n3 more left out."), "{}", &long[long.len() - 20..]);
        assert_eq!(listed(TASKS, "Head.".into(), lines(1, 10)).unwrap().content, "Head.\n- 0 xxxxxxxxxx");
        assert_eq!(listed(TASKS, "Head.".into(), Vec::new()), None);
    }
}
