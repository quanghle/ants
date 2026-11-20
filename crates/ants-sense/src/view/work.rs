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

/// The findings of the last dream unanswered or deferred, a deferral with its reason, then those its sage's answer
/// held unshown, then those its sage gave late; none without any.
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
    lines.extend(dream.held.iter().map(|finding| format!("{}\n  Held unshown.", finding_line(finding, dream.number))));
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
    if !dream.held.is_empty() {
        let _ = write!(
            head,
            "; {} held unshown under the sage's answer, which the next dream offers first",
            dream.held.len()
        );
    }
    if lines.len() > open + dream.held.len() {
        let sage = dream.sage.as_ref().map_or_else(|| "its sage".to_owned(), |sage| format!("sage {sage}"));
        let late = lines.len() - open - dream.held.len();
        let _ = write!(head, "; {late} given late by {sage}, which the next dream offers");
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
mod tests;
