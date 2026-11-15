//! The tool-call timeline of each prompt, stored on `ants.timeline.<thread>.<ant>` ([`Timeline`]): best effort.
//!
//! While a prompt runs, its tool calls and plan are collected from the status updates the harness streams, redacted as
//! those are, and stored as records of what changed: at most one every [`TIMELINE_EVERY`] while anything changed, and
//! what is left when the prompt ends. A record is stored from a task of its own, so a slow or failed store never holds
//! up the harness or an ack; a failed one is logged and dropped.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use ants_proto::{
    MAX_TIMELINE_CALLS, MAX_TIMELINE_ENTRIES, PlanStep, StatusUpdate, ThreadId, Timeline, TimelineCall, TimelineEntry,
    UserId, subject,
};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[cfg(doc)]
use ants_proto::TIMELINE_EVERY;

use crate::bus;
use crate::redact::Redactor;

/// The tool calls and plan of one prompt, as far as they were not stored yet.
pub struct Collector<'a> {
    ant: &'a UserId,
    thread: &'a ThreadId,
    /// The message the prompt handles.
    message: Uuid,
    redactor: &'a Redactor,
    calls: Vec<TimelineCall>,
    index: HashMap<String, usize>,
    /// The calls changed since they were last stored, by index.
    changed: BTreeSet<usize>,
    plan: Option<Vec<PlanStep>>,
    plan_changed: bool,
    /// The calls past [`MAX_TIMELINE_CALLS`], only counted.
    left_out: HashSet<String>,
    left_out_changed: bool,
}

impl<'a> Collector<'a> {
    pub fn new(ant: &'a UserId, thread: &'a ThreadId, message: Uuid, redactor: &'a Redactor) -> Self {
        Self {
            ant,
            thread,
            message,
            redactor,
            calls: Vec::new(),
            index: HashMap::new(),
            changed: BTreeSet::new(),
            plan: None,
            plan_changed: false,
            left_out: HashSet::new(),
            left_out_changed: false,
        }
    }

    /// Take in an update the harness streamed at `now`, unredacted: tool calls and plans count, the rest not.
    pub fn push(&mut self, update: &StatusUpdate, now: DateTime<Utc>) {
        match update {
            StatusUpdate::Tool { id, title, tool_kind, status, paths } => {
                let Some(i) = self.call(id, now) else { return };
                let call = &mut self.calls[i];
                if let Some(title) = title {
                    call.title.clone_from(title);
                    self.redactor.redact_string(&mut call.title);
                }
                if tool_kind.is_some() {
                    call.tool_kind.clone_from(tool_kind);
                }
                if let Some(paths) = paths {
                    call.paths.clone_from(paths);
                    call.paths.iter_mut().for_each(|path| self.redactor.redact_string(path));
                }
                if status.is_some() {
                    call.status.clone_from(status);
                    if call.ended() && call.duration_ms.is_none() {
                        call.duration_ms = Some(u64::try_from((now - call.started).num_milliseconds()).unwrap_or(0));
                    }
                }
                // Redacted before it is cut, so no secret is cut short of being recognised.
                *call = std::mem::replace(call, empty_call(now)).bounded();
                self.changed.insert(i);
            }
            StatusUpdate::Plan { steps } => {
                let mut steps = steps.clone();
                steps.iter_mut().for_each(|step| self.redactor.redact_string(&mut step.content));
                self.plan = Some(steps);
                self.plan_changed = true;
            }
            _ => {}
        }
    }

    /// The index of the call `id`, added when new, first heard of `now`; None for one left out.
    fn call(&mut self, id: &str, now: DateTime<Utc>) -> Option<usize> {
        if let Some(&i) = self.index.get(id) {
            return Some(i);
        }
        if self.calls.len() >= MAX_TIMELINE_CALLS {
            self.left_out_changed |= self.left_out.insert(id.to_owned());
            return None;
        }
        self.index.insert(id.to_owned(), self.calls.len());
        self.calls.push(TimelineCall { id: id.to_owned(), ..empty_call(now) });
        Some(self.calls.len() - 1)
    }

    /// Whether anything changed since the last record.
    pub fn pending(&self) -> bool {
        !self.changed.is_empty() || self.plan_changed || self.left_out_changed
    }

    /// The next record of what changed, of at most [`MAX_TIMELINE_ENTRIES`] entries: the plan, then the calls in the
    /// order first heard of. None when nothing changed.
    pub fn take(&mut self) -> Option<Timeline> {
        let mut entries = Vec::new();
        if std::mem::take(&mut self.plan_changed)
            && let Some(plan) = &self.plan
        {
            entries.push(TimelineEntry::plan(plan));
        }
        while entries.len() < MAX_TIMELINE_ENTRIES
            && let Some(i) = self.changed.pop_first()
        {
            entries.push(TimelineEntry::Tool(self.calls[i].clone()));
        }
        // A record holds at least one entry: a new count of calls left out goes with the last call kept.
        if std::mem::take(&mut self.left_out_changed)
            && entries.is_empty()
            && let Some(last) = self.calls.last()
        {
            entries.push(TimelineEntry::Tool(last.clone()));
        }
        if entries.is_empty() {
            return None;
        }
        let omitted = u32::try_from(self.left_out.len()).unwrap_or(u32::MAX);
        Some(Timeline::new(self.ant.clone(), self.thread.clone(), self.message, entries, omitted))
    }
}

fn empty_call(now: DateTime<Utc>) -> TimelineCall {
    TimelineCall {
        id: String::new(),
        tool_kind: None,
        title: String::new(),
        status: None,
        started: now,
        duration_ms: None,
        paths: Vec::new(),
    }
}

/// Whether the credentials at `creds` let `ant` store its timeline ([`subject::timeline_from`]); without any, the
/// server judges. Credentials issued before timelines were stored do not: that is logged once, and none is stored.
pub fn allowed(creds: Option<&Path>, ant: &UserId) -> bool {
    let allowed = creds.is_none_or(|path| crate::creds::may_publish(path, &subject::timeline_from(ant)));
    if !allowed {
        tracing::warn!(
            "the credentials of {ant} do not allow storing tool-call timelines on {}: none is stored until they are \
             renewed (`ants doctor` names how) and the daemon restarted",
            subject::timeline_from(ant)
        );
    }
    allowed
}

/// Store `record` in the hub's stream, or out of its reach in the machine's outbox, from a task of its own. A failure
/// is logged, as a warning the first time.
pub fn store(js: &jetstream::Context, record: Timeline) {
    static WARNED: AtomicBool = AtomicBool::new(false);
    let js = js.clone();
    tokio::spawn(async move {
        let id = format!("timeline-{}", Uuid::now_v7());
        if let Err(e) = bus::store(&js, record.subject(), &record, &id).await {
            let error = format!("{e:#}");
            if WARNED.swap(true, Ordering::Relaxed) {
                tracing::debug!(thread = %record.thread_id, error, "dropping a tool-call timeline record");
            } else {
                tracing::warn!(thread = %record.thread_id, error, "dropping a tool-call timeline record");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use ants_proto::{MAX_TIMELINE_TITLE, MAX_TOOL_PATHS};
    use chrono::TimeDelta;

    use super::*;

    fn tool(id: &str, title: Option<&str>, status: Option<&str>) -> StatusUpdate {
        StatusUpdate::Tool {
            id: id.to_owned(),
            title: title.map(str::to_owned),
            tool_kind: Some("execute".to_owned()),
            status: status.map(str::to_owned),
            paths: None,
        }
    }

    fn calls(record: &Timeline) -> Vec<&TimelineCall> {
        record
            .entries
            .iter()
            .filter_map(|e| match e {
                TimelineEntry::Tool(call) => Some(call),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn calls_are_merged_redacted_timed_and_stored_once_per_change() {
        let (ant, thread): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let redactor = Redactor::default().with_env([("API_TOKEN", "tok-12345678")]);
        let mut timeline = Collector::new(&ant, &thread, Uuid::nil(), &redactor);
        let start = DateTime::UNIX_EPOCH;
        assert!(!timeline.pending() && timeline.take().is_none());

        timeline.push(&StatusUpdate::Message { text: "hi".to_owned() }, start);
        assert!(!timeline.pending(), "only tool calls and plans count");
        let long = format!("curl -H 'X-Token: tok-12345678' {}", "x".repeat(300));
        timeline.push(&tool("c1", Some(&long), Some("pending")), start);
        let paths = StatusUpdate::Tool {
            id: "c1".to_owned(),
            title: None,
            tool_kind: None,
            status: Some("in_progress".to_owned()),
            paths: Some(vec!["/work/tok-12345678/a.rs".to_owned(); 10]),
        };
        timeline.push(&paths, start + TimeDelta::milliseconds(5));
        let record = timeline.take().unwrap();
        assert_eq!((record.ant.as_str(), record.message, record.omitted), ("dev-a", Uuid::nil(), 0));
        assert_eq!(record.check_subject(&subject::timeline(&thread, &ant)), Ok(()));
        let [call] = calls(&record)[..] else { panic!("{record:?}") };
        assert!(call.title.starts_with("curl -H 'X-Token: [redacted]' x"), "{}", call.title);
        assert_eq!(call.title.chars().count(), MAX_TIMELINE_TITLE);
        assert_eq!(call.paths, vec!["/work/[redacted]/a.rs".to_owned(); MAX_TOOL_PATHS]);
        assert_eq!((call.status.as_deref(), call.started, call.duration_ms), (Some("in_progress"), start, None));
        assert!(!timeline.pending() && timeline.take().is_none(), "nothing changed since");

        timeline.push(&tool("c1", None, Some("completed")), start + TimeDelta::milliseconds(2100));
        let plan = vec![PlanStep { content: "use tok-12345678".to_owned(), status: "pending".to_owned() }];
        timeline.push(&StatusUpdate::Plan { steps: plan }, start);
        let record = timeline.take().unwrap();
        let TimelineEntry::Plan { steps } = &record.entries[0] else { panic!("{record:?}") };
        assert_eq!(steps[0].content, "use [redacted]");
        let [call] = calls(&record)[..] else { panic!("{record:?}") };
        assert_eq!((call.status.as_deref(), call.duration_ms), (Some("completed"), Some(2100)));
        assert!(call.title.starts_with("curl"), "a call keeps what did not change");
    }

    #[test]
    fn records_hold_at_most_their_entries_and_calls_past_the_cap_are_counted() {
        let (ant, thread): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let redactor = Redactor::default();
        let mut timeline = Collector::new(&ant, &thread, Uuid::nil(), &redactor);
        let now = Utc::now();
        for i in 0..MAX_TIMELINE_CALLS + 3 {
            timeline.push(&tool(&format!("c{i}"), Some("ls"), Some("pending")), now);
        }
        let mut records = Vec::new();
        while let Some(record) = timeline.take() {
            records.push(record);
        }
        assert_eq!(records.len(), MAX_TIMELINE_CALLS / MAX_TIMELINE_ENTRIES);
        assert!(records.iter().all(|r| r.entries.len() == MAX_TIMELINE_ENTRIES && r.omitted == 3));
        assert_eq!(calls(&records[0])[0].id, "c0");

        // A call left out stays out, and is counted once.
        timeline.push(&tool(&format!("c{MAX_TIMELINE_CALLS}"), None, Some("completed")), now);
        assert!(!timeline.pending());
        timeline.push(&tool("c-new", None, Some("pending")), now);
        let record = timeline.take().unwrap();
        assert_eq!(record.omitted, 4);
        assert_eq!(calls(&record)[0].id, format!("c{}", MAX_TIMELINE_CALLS - 1), "the count goes with the last call");
    }
}
