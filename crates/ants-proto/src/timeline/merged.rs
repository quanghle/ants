//! A thread's timeline as its readers show it ([`Timelines`]): the records of its prompts merged, each call, plan and
//! end as its latest record has it.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::{NoReplyWhy, Timeline, TimelineCall, TimelineEnd, TimelineEntry, TimelineRejection};
use crate::{PlanStep, ThreadId, UserId};

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

/// A tool request of a thread's timeline that was rejected ([`TimelineEntry::Rejected`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedRejection {
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// The message whose prompt asked for the tool.
    pub message: Uuid,
    pub rejection: TimelineRejection,
    /// When the record saying so was made.
    pub at: DateTime<Utc>,
}

/// How a prompt of a thread's timeline ended ([`TimelineEntry::Ended`]).
#[derive(Debug, Clone, PartialEq)]
pub struct MergedEnd {
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// The message whose prompt ended so.
    pub message: Uuid,
    pub end: TimelineEnd,
    /// When the record saying so was made.
    pub at: DateTime<Utc>,
}

/// The tool calls, plans, rejected tool requests, ends with no reply and prompt ends of [`Timeline`] records, merged:
/// fed in the order they were stored, each call, plan and end with no reply is as its latest record has it. Calls are
/// keyed by ant, message and call id, since a channel post reaches several ants under one message id; plans and ends
/// with no reply by ant and message; rejections by ant and request. Every prompt end is kept, a message prompted again
/// having one per run.
#[derive(Debug, Clone, Default)]
pub struct Timelines {
    calls: Vec<MergedCall>,
    index: HashMap<(UserId, Uuid, String), usize>,
    plans: HashMap<(UserId, Uuid), Vec<PlanStep>>,
    omitted: HashMap<(UserId, Uuid), u32>,
    no_replies: Vec<MergedNoReply>,
    rejections: Vec<MergedRejection>,
    ends: Vec<MergedEnd>,
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
                TimelineEntry::Rejected(rejection) => {
                    let request = rejection.request;
                    let (ant, thread_id) = (ant.clone(), thread_id.clone());
                    let rejected = MergedRejection { ant, thread_id, message, rejection, at };
                    let had =
                        self.rejections.iter_mut().find(|r| r.ant == rejected.ant && r.rejection.request == request);
                    match had {
                        Some(had) => *had = rejected,
                        None => self.rejections.push(rejected),
                    }
                }
                TimelineEntry::Ended(end) => {
                    let (ant, thread_id) = (ant.clone(), thread_id.clone());
                    self.ends.push(MergedEnd { ant, thread_id, message, end, at });
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

    /// Every tool request rejected, in the order first stored.
    pub fn rejections(&self) -> &[MergedRejection] {
        &self.rejections
    }

    /// How every prompt ended, in the order stored.
    pub fn ends(&self) -> &[MergedEnd] {
        &self.ends
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
        self.calls.is_empty()
            && self.plans.is_empty()
            && self.no_replies.is_empty()
            && self.rejections.is_empty()
            && self.ends.is_empty()
    }
}
