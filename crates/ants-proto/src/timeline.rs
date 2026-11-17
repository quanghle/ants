//! Tool-call timelines: what an ant did while it worked on a message, stored compactly so a thread's history can show
//! it after the live status updates are gone.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, MAX_TOOL_PATHS, PlanStep, ThreadId, UserId, subject};

/// The most entries a [`Timeline`] record holds.
pub const MAX_TIMELINE_ENTRIES: usize = 50;
/// The most characters of a tool call's title a [`Timeline`] record keeps.
pub const MAX_TIMELINE_TITLE: usize = 200;
/// The most characters of each path of a tool call a [`Timeline`] record keeps.
pub const MAX_TIMELINE_PATH: usize = 256;
/// The most steps of a plan a [`Timeline`] record keeps, each cut to [`MAX_TIMELINE_TITLE`] characters.
pub const MAX_TIMELINE_STEPS: usize = 50;
/// The most tool calls of one prompt the records of its timeline hold: those past it are only counted
/// ([`Timeline::omitted`]).
pub const MAX_TIMELINE_CALLS: usize = 200;
/// How often at most a daemon stores a [`Timeline`] record while a prompt runs and calls changed.
pub const TIMELINE_EVERY: Duration = Duration::from_secs(30);

/// Part of the tool-call timeline of `ant` on a thread: the calls of the prompt that handled `message` that changed
/// since the last record, the prompt's plan as last reported, and whether it ended with no reply sent, stored on
/// [`subject::timeline`]. A record holds 1 to [`MAX_TIMELINE_ENTRIES`] entries, never a tool's input or output, message
/// text or thoughts, but for the final text of a prompt that ended with no reply sent ([`TimelineEntry::NoReply`]),
/// which no message carries. Calls are merged by message and call id, later records replacing earlier ones
/// ([`Timelines`]). Readers ignore unknown entry kinds and fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timeline {
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// The message whose prompt the record covers.
    pub message: Uuid,
    pub entries: Vec<TimelineEntry>,
    /// How many tool calls of the prompt were left out so far, past [`MAX_TIMELINE_CALLS`].
    #[serde(default, skip_serializing_if = "is_zero")]
    pub omitted: u32,
    pub at: DateTime<Utc>,
}

#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde's skip_serializing_if takes a reference")]
fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// One entry of a [`Timeline`] record, tagged by `type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TimelineEntry {
    Tool(TimelineCall),
    /// The prompt's plan as last reported, replacing any earlier one.
    Plan {
        steps: Vec<PlanStep>,
    },
    /// The prompt ended with no reply sent: why, the reason the ant gave to `complete` if any, and the turn's final text,
    /// which went to nobody.
    NoReply {
        why: NoReplyWhy,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        text: String,
    },
    /// An entry of a kind this version does not know, read as this and ignored: a reader older than a kind skips it
    /// (one before [`TimelineEntry::NoReply`] skips that). Stays the last variant.
    #[serde(other)]
    Unknown,
}

/// Why a prompt ended with no reply sent ([`TimelineEntry::NoReply`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoReplyWhy {
    /// The message was another ant's reply to a message this ant sent, so the answer is not sent back.
    ReplyToOwn,
    /// The ant ended the turn with the `complete` tool.
    Completed,
    /// A later message steered the prompt, whose answer went to that one; `reason` names it.
    Steered,
}

/// A tool call as a [`Timeline`] record holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineCall {
    /// The ACP tool call id, unique within the prompt.
    pub id: String,
    /// ACP tool kind: read, edit, execute and so on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_kind: Option<String>,
    /// At most [`MAX_TIMELINE_TITLE`] characters, redacted.
    #[serde(default)]
    pub title: String,
    /// ACP tool call status: pending, in_progress, completed or failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// When the daemon first heard of the call.
    pub started: DateTime<Utc>,
    /// How long the call ran, once it completed or failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// The paths of the call's ACP locations, at most [`MAX_TOOL_PATHS`] of at most [`MAX_TIMELINE_PATH`] characters,
    /// redacted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
}

impl TimelineCall {
    /// Whether the call ended: completed or failed.
    pub fn ended(&self) -> bool {
        matches!(self.status.as_deref(), Some("completed" | "failed"))
    }

    /// The call with its title and paths cut to what a record keeps.
    #[must_use]
    pub fn bounded(mut self) -> Self {
        cut(&mut self.title, MAX_TIMELINE_TITLE);
        self.paths.truncate(MAX_TOOL_PATHS);
        self.paths.iter_mut().for_each(|path| cut(path, MAX_TIMELINE_PATH));
        self
    }
}

impl TimelineEntry {
    /// The entry of a plan of `steps`, cut to what a record keeps.
    pub fn plan(steps: &[PlanStep]) -> Self {
        let steps = steps.iter().take(MAX_TIMELINE_STEPS).map(|step| {
            let mut content = step.content.clone();
            cut(&mut content, MAX_TIMELINE_TITLE);
            PlanStep { content, status: step.status.clone() }
        });
        Self::Plan { steps: steps.collect() }
    }
}

/// Cut `text` to `max` characters.
fn cut(text: &mut String, max: usize) {
    if let Some((at, _)) = text.char_indices().nth(max) {
        text.truncate(at);
    }
}

impl Timeline {
    pub fn new(ant: UserId, thread_id: ThreadId, message: Uuid, entries: Vec<TimelineEntry>, omitted: u32) -> Self {
        Self { ant, thread_id, message, entries, omitted, at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::timeline(&self.thread_id, &self.ant)
    }

    /// Check that the record arrived on its own [`Timeline::subject`]. Credentials pin each ant to its own timeline
    /// subjects, so a record naming another ant or thread is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser {
            kind: "timeline record",
            user: self.ant.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
    }
}

/// A tool call of a thread's timeline, merged from its records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedCall {
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// The message whose prompt made the call.
    pub message: Uuid,
    pub call: TimelineCall,
}

/// A prompt of a thread's timeline that ended with no reply sent ([`TimelineEntry::NoReply`]), as its latest record
/// has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedNoReply {
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// The message whose prompt ended so.
    pub message: Uuid,
    pub why: NoReplyWhy,
    pub reason: Option<String>,
    /// The turn's final text, which went to nobody.
    pub text: String,
    /// When the record saying so was made.
    pub at: DateTime<Utc>,
}

/// The tool calls, plans and ends with no reply of [`Timeline`] records, merged: fed in the order they were stored,
/// each call, plan and end is as its latest record has it. Calls are keyed by ant, message and call id, since a
/// channel post reaches several ants under one message id; plans and ends by ant and message.
#[derive(Debug, Clone, Default)]
pub struct Timelines {
    calls: Vec<MergedCall>,
    index: HashMap<(UserId, Uuid, String), usize>,
    plans: HashMap<(UserId, Uuid), Vec<PlanStep>>,
    omitted: HashMap<(UserId, Uuid), u32>,
    no_replies: Vec<MergedNoReply>,
}

impl Timelines {
    /// Merge `record`, stored on `subject`; a record forged for another ant or thread is dropped. Whether it was kept.
    pub fn add(&mut self, subject: &str, record: Timeline) -> bool {
        if record.check_subject(subject).is_err() {
            return false;
        }
        let Timeline { ant, thread_id, message, entries, omitted, at } = record;
        if omitted > 0 {
            self.omitted.insert((ant.clone(), message), omitted);
        }
        for entry in entries {
            match entry {
                TimelineEntry::Tool(call) => {
                    let key = (ant.clone(), message, call.id.clone());
                    if let Some(&i) = self.index.get(&key) {
                        self.calls[i].call = call;
                    } else {
                        self.index.insert(key, self.calls.len());
                        let (ant, thread_id) = (ant.clone(), thread_id.clone());
                        self.calls.push(MergedCall { ant, thread_id, message, call });
                    }
                }
                TimelineEntry::Plan { steps } => {
                    self.plans.insert((ant.clone(), message), steps);
                }
                TimelineEntry::NoReply { why, reason, text } => {
                    let (ant, thread_id) = (ant.clone(), thread_id.clone());
                    let end = MergedNoReply { ant, thread_id, message, why, reason, text, at };
                    match self.no_replies.iter_mut().find(|n| n.ant == end.ant && n.message == message) {
                        Some(had) => *had = end,
                        None => self.no_replies.push(end),
                    }
                }
                TimelineEntry::Unknown => {}
            }
        }
        true
    }

    /// Every call, in the order first stored.
    pub fn calls(&self) -> &[MergedCall] {
        &self.calls
    }

    /// Every prompt that ended with no reply sent, in the order first stored.
    pub fn no_replies(&self) -> &[MergedNoReply] {
        &self.no_replies
    }

    /// The plan of `ant` for `message` as last stored, if any.
    pub fn plan(&self, ant: &UserId, message: Uuid) -> Option<&[PlanStep]> {
        self.plans.get(&(ant.clone(), message)).map(Vec::as_slice)
    }

    /// How many calls of `ant` for `message` were left out of its records.
    pub fn omitted(&self, ant: &UserId, message: Uuid) -> u32 {
        self.omitted.get(&(ant.clone(), message)).copied().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.calls.is_empty() && self.plans.is_empty() && self.no_replies.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    fn call(id: &str, status: &str) -> TimelineEntry {
        TimelineEntry::Tool(TimelineCall {
            id: id.to_owned(),
            tool_kind: Some("execute".to_owned()),
            title: "cargo test".to_owned(),
            status: Some(status.to_owned()),
            started: DateTime::UNIX_EPOCH,
            duration_ms: None,
            paths: Vec::new(),
        })
    }

    #[test]
    fn records_skip_unknown_entries_and_fields() {
        let json = serde_json::json!({
            "ant": "dev-a", "thread_id": "t1", "message": Uuid::nil(), "at": DateTime::UNIX_EPOCH, "later": 1,
            "entries": [
                {"type": "tool", "id": "c1", "title": "ls", "started": DateTime::UNIX_EPOCH, "cost": 3},
                {"type": "diff", "path": "a.rs"},
            ],
        });
        let record: Timeline = serde_json::from_value(json).unwrap();
        assert_eq!(record.entries.len(), 2);
        assert!(matches!(&record.entries[0], TimelineEntry::Tool(c) if c.id == "c1" && !c.ended()));
        assert_eq!(record.entries[1], TimelineEntry::Unknown);
        assert_eq!(record.omitted, 0);
    }

    #[test]
    fn later_records_replace_calls_and_plans_and_forged_ones_are_dropped() {
        let t: ThreadId = "t1".parse().unwrap();
        let (m1, m2) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut timelines = Timelines::default();
        let first = Timeline::new(p("dev-a"), t.clone(), m1, vec![call("c1", "pending"), call("c2", "pending")], 0);
        let subject = first.subject();
        assert!(timelines.add(&subject, first));
        let plan = vec![PlanStep { content: "test".to_owned(), status: "pending".to_owned() }];
        let entries = vec![call("c1", "completed"), TimelineEntry::Plan { steps: plan.clone() }];
        assert!(timelines.add(&subject, Timeline::new(p("dev-a"), t.clone(), m1, entries, 3)));
        // The same call id in another prompt is another call.
        assert!(timelines.add(&subject, Timeline::new(p("dev-a"), t.clone(), m2, vec![call("c1", "failed")], 0)));
        let forged = Timeline::new(p("dev-b"), t, m1, vec![call("c1", "failed")], 0);
        assert!(!timelines.add(&subject, forged));

        let calls: Vec<_> = timelines
            .calls()
            .iter()
            .map(|c| (c.message, c.call.id.as_str(), c.call.status.as_deref().unwrap()))
            .collect();
        assert_eq!(calls, [(m1, "c1", "completed"), (m1, "c2", "pending"), (m2, "c1", "failed")]);
        assert_eq!(timelines.plan(&p("dev-a"), m1), Some(plan.as_slice()));
        assert_eq!(timelines.omitted(&p("dev-a"), m1), 3);
        assert_eq!(timelines.omitted(&p("dev-a"), m2), 0);
    }

    #[test]
    fn a_later_no_reply_of_a_prompt_replaces_the_earlier_one() {
        let t: ThreadId = "t1".parse().unwrap();
        let (m1, m2) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let end = |why, text: &str| TimelineEntry::NoReply { why, reason: None, text: text.to_owned() };
        let mut timelines = Timelines::default();
        let first = Timeline::new(p("dev-a"), t.clone(), m1, vec![end(NoReplyWhy::ReplyToOwn, "thanks")], 0);
        let subject = first.subject();
        assert!(timelines.add(&subject, first));
        assert!(!timelines.is_empty());
        let entries = vec![end(NoReplyWhy::Completed, "done")];
        assert!(timelines.add(&subject, Timeline::new(p("dev-a"), t.clone(), m1, entries, 0)));
        assert!(timelines.add(&subject, Timeline::new(p("dev-a"), t, m2, vec![end(NoReplyWhy::ReplyToOwn, "")], 0)));
        let ends: Vec<_> = timelines.no_replies().iter().map(|n| (n.message, n.why, n.text.as_str())).collect();
        assert_eq!(ends, [(m1, NoReplyWhy::Completed, "done"), (m2, NoReplyWhy::ReplyToOwn, "")]);
    }

    #[test]
    fn calls_and_plans_are_cut_to_what_a_record_keeps() {
        let TimelineEntry::Tool(call) = call("c1", "pending") else { unreachable!() };
        let call = TimelineCall { title: "é".repeat(MAX_TIMELINE_TITLE + 5), paths: vec!["p".repeat(300); 9], ..call };
        let call = call.bounded();
        assert_eq!(call.title, "é".repeat(MAX_TIMELINE_TITLE));
        assert_eq!(call.paths, vec!["p".repeat(MAX_TIMELINE_PATH); MAX_TOOL_PATHS]);
        let step = PlanStep { content: "x".repeat(300), status: "pending".to_owned() };
        let TimelineEntry::Plan { steps } = TimelineEntry::plan(&vec![step; 60]) else { unreachable!() };
        assert_eq!(steps.len(), MAX_TIMELINE_STEPS);
        assert!(steps.iter().all(|s| s.content.len() == MAX_TIMELINE_TITLE));
    }
}
