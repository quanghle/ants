//! The tool calls of threads, as the thread view's timeline shows them: those the ants make now, heard in their status
//! updates, and those stored on their timelines ([`ants_proto::Timeline`] records), merged so that each call shows
//! once with its latest status; and the prompts the ants ended with no reply sent, stored on their timelines. A stored record names the message whose prompt made a call, which a status update
//! does not: a call heard live is matched by its ant and ACP id, and by when it started, which the same daemon's clock
//! tells both ways.

use std::collections::HashMap;

use ants_proto::{StatusEvent, StatusUpdate, ThreadId, UserId, Uuid};
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::thread::{Entry, Kind};

/// How many calls of a thread are kept, the oldest dropped first.
const MAX_CALLS: usize = 1000;
/// How many threads' calls heard live are kept, those heard of least recently dropped first.
const MAX_THREADS: usize = 64;
/// How far apart a call heard live and a stored one with its ant and id may have started and still be the same call.
const SAME_CALL: TimeDelta = TimeDelta::minutes(2);

/// How the thread view shows the tool calls and the prompts ended with no reply, cycled by `T`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tools {
    /// Each call and each end with no reply on a line of its own.
    All,
    /// A line for each ant's turn between two messages, but for the one running, shown in full.
    #[default]
    Collapsed,
    /// Neither.
    None,
}

impl Tools {
    #[must_use]
    pub fn next(self) -> Self {
        match self {
            Self::All => Self::Collapsed,
            Self::Collapsed => Self::None,
            Self::None => Self::All,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Collapsed => "collapsed",
            Self::None => "none",
        }
    }
}

/// A tool call an ant made on a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub ant: UserId,
    /// The message whose prompt made the call, once a stored record tells.
    pub message: Option<Uuid>,
    /// The ACP tool call id, unique within its prompt.
    pub id: String,
    /// ACP tool kind: read, edit, execute and so on.
    pub kind: Option<String>,
    pub title: String,
    /// ACP tool call status: pending, in_progress, completed or failed.
    pub status: Option<String>,
    pub started: DateTime<Utc>,
    /// How long it ran, once it ended.
    pub duration: Option<TimeDelta>,
    /// The paths of its ACP locations.
    pub paths: Vec<String>,
    /// Heard in status updates and not stored since.
    pub live: bool,
}

impl Call {
    /// Whether it ended: completed or failed.
    pub fn ended(&self) -> bool {
        matches!(self.status.as_deref(), Some("completed" | "failed"))
    }
}

/// A prompt an ant ended with no reply sent, stored on its timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct End {
    pub ant: UserId,
    /// The message whose prompt ended so.
    pub message: Uuid,
    /// When its record was made.
    pub at: DateTime<Utc>,
    /// The reason the ant gave to `complete`, else the first line of the turn's final text, if any.
    pub why: Option<String>,
}

/// One thread's tool calls, by when they started, and its prompts ended with no reply, by when they ended.
#[derive(Debug, Default)]
pub struct Calls {
    calls: Vec<Call>,
    ends: Vec<End>,
    /// When a call was last heard of or stored.
    touched: Option<DateTime<Utc>>,
}

impl Calls {
    pub fn calls(&self) -> &[Call] {
        &self.calls
    }

    pub fn ends(&self) -> &[End] {
        &self.ends
    }

    /// Take a status update: a tool update starts a call or changes the one of its ant and id not ended yet.
    pub fn status(&mut self, event: &StatusEvent) {
        let StatusUpdate::Tool { id, title, tool_kind, status, paths } = &event.update else { return };
        self.touched = Some(event.at);
        let ended = matches!(status.as_deref(), Some("completed" | "failed"));
        let same = |c: &Call| c.ant == event.ant && c.id == *id;
        let at = self.calls.iter().rposition(|c| same(c) && !c.ended());
        let at = at.or_else(|| self.calls.iter().rposition(|c| same(c) && ended && c.live));
        let Some(at) = at else {
            let title = title.clone().or_else(|| tool_kind.clone()).unwrap_or_else(|| "a tool".to_owned());
            let mut call = Call {
                ant: event.ant.clone(),
                message: None,
                id: id.clone(),
                kind: tool_kind.clone(),
                title: one_line(&title),
                status: status.clone(),
                started: event.at,
                duration: None,
                paths: paths.clone().unwrap_or_default(),
                live: true,
            };
            if ended {
                call.duration = Some(TimeDelta::zero());
            }
            return self.insert(call);
        };
        let call = &mut self.calls[at];
        if let Some(title) = title {
            call.title = one_line(title);
        }
        if tool_kind.is_some() {
            call.kind.clone_from(tool_kind);
        }
        if let Some(paths) = paths {
            call.paths.clone_from(paths);
        }
        if status.is_some() {
            call.status.clone_from(status);
        }
        if ended && call.duration.is_none() {
            call.duration = Some((event.at - call.started).max(TimeDelta::zero()));
        }
    }

    /// Take a tool call stored on a timeline, an entry of kind tool: it replaces the call heard live with its ant and
    /// id that started about when it did, or the one stored before with its ant, message and id, but for the status of
    /// a call heard ended that the record does not have ended yet.
    pub fn stored(&mut self, entry: &Entry) {
        if let (Kind::NoReply, Some(end), Some(ant)) = (entry.kind, &entry.no_reply, &entry.from) {
            let why = end.reason.as_deref().or_else(|| entry.text.lines().map(str::trim).find(|l| !l.is_empty()));
            let end = End { ant: ant.clone(), message: end.message, at: entry.stored_at, why: why.map(one_line) };
            return self.end(end);
        }
        let (Kind::Tool, Some(tool), Some(ant)) = (entry.kind, &entry.tool, &entry.from) else { return };
        self.touched = Some(self.touched.map_or(entry.stored_at, |t| t.max(entry.stored_at)));
        let call = Call {
            ant: ant.clone(),
            message: Some(tool.message),
            id: tool.call_id.clone(),
            kind: tool.tool_kind.clone(),
            title: one_line(&entry.text),
            status: tool.status.clone(),
            started: entry.stored_at,
            duration: tool.duration_ms.and_then(|ms| TimeDelta::try_milliseconds(i64::try_from(ms).ok()?)),
            paths: tool.paths.clone(),
            live: false,
        };
        let same = |c: &Call| {
            c.ant == call.ant
                && c.id == call.id
                && match c.message {
                    Some(message) => Some(message) == call.message,
                    None => (c.started - call.started).abs() <= SAME_CALL,
                }
        };
        let Some(at) = self.calls.iter().rposition(same) else { return self.insert(call) };
        let had = &mut self.calls[at];
        if had.ended() && !call.ended() {
            had.message = call.message;
            had.live = false;
            return;
        }
        let started = had.started.min(call.started);
        *had = Call { started, ..call };
    }

    /// Keep `end` in order of time, replacing the one of its ant and message, dropping the oldest past [`MAX_CALLS`].
    fn end(&mut self, end: End) {
        self.touched = Some(self.touched.map_or(end.at, |t| t.max(end.at)));
        self.ends.retain(|e| e.ant != end.ant || e.message != end.message);
        let at = self.ends.partition_point(|e| e.at <= end.at);
        self.ends.insert(at, end);
        let over = self.ends.len().saturating_sub(MAX_CALLS);
        self.ends.drain(..over);
    }

    /// Keep `call` in order of start, dropping the oldest past [`MAX_CALLS`].
    fn insert(&mut self, call: Call) {
        let at = self.calls.partition_point(|c| c.started <= call.started);
        self.calls.insert(at, call);
        let over = self.calls.len().saturating_sub(MAX_CALLS);
        self.calls.drain(..over);
    }

    /// The paths the calls touched, the latest first, each once.
    pub fn paths(&self) -> Vec<&str> {
        let mut paths: Vec<&str> = Vec::new();
        for path in self.calls.iter().rev().flat_map(|c| &c.paths) {
            if !paths.contains(&path.as_str()) {
                paths.push(path);
            }
        }
        paths
    }
}

/// The tool calls of the threads heard of or opened.
#[derive(Debug, Default)]
pub struct Threads(HashMap<ThreadId, Calls>);

impl Threads {
    pub fn get(&self, thread: &ThreadId) -> Option<&Calls> {
        self.0.get(thread)
    }

    fn of(&mut self, thread: &ThreadId) -> &mut Calls {
        if !self.0.contains_key(thread) && self.0.len() >= MAX_THREADS {
            let stalest = self.0.iter().min_by_key(|(_, c)| c.touched).map(|(t, _)| t.clone());
            stalest.map(|t| self.0.remove(&t));
        }
        self.0.entry(thread.clone()).or_default()
    }

    /// Take a status update on any thread.
    pub fn status(&mut self, event: &StatusEvent) {
        if matches!(event.update, StatusUpdate::Tool { .. }) {
            self.of(&event.thread_id).status(event);
        }
    }

    /// Take a tool call or end with no reply stored on a timeline.
    pub fn stored(&mut self, entry: &Entry) {
        self.of(&entry.thread_id).stored(entry);
    }
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::live::tests::{at, user};
    use crate::thread::ToolCall;

    pub fn tool(thread: &ThreadId, ant: &str, id: &str, title: Option<&str>, status: &str, secs: i64) -> StatusEvent {
        let update = StatusUpdate::Tool {
            id: id.to_owned(),
            title: title.map(str::to_owned),
            tool_kind: Some("execute".to_owned()),
            status: Some(status.to_owned()),
            paths: (status == "in_progress").then(|| vec![format!("src/{id}.rs")]),
        };
        StatusEvent { at: at(secs), ..StatusEvent::new(user(ant), thread.clone(), update) }
    }

    /// The entry of a call stored on `ant`'s timeline for `message`, started at second `started`.
    pub fn stored(
        seq: u64,
        thread: &ThreadId,
        ant: &str,
        message: Uuid,
        id: &str,
        status: &str,
        started: i64,
    ) -> Entry {
        let mut entry = crate::live::tests::message(seq, thread, ant, "alice", "cargo test");
        entry.kind = Kind::Tool;
        entry.id = None;
        entry.to = None;
        entry.stored_at = at(started);
        entry.tool = Some(ToolCall {
            call_id: id.to_owned(),
            message,
            tool_kind: Some("execute".to_owned()),
            status: Some(status.to_owned()),
            duration_ms: Some(3100),
            paths: vec!["Cargo.toml".to_owned()],
        });
        entry
    }

    #[test]
    fn a_call_heard_live_runs_until_it_ends_and_keeps_its_title_and_paths() {
        let thread = ThreadId::generate();
        let mut calls = Calls::default();
        calls.status(&tool(&thread, "dev-a", "c1", Some("cargo   test"), "in_progress", 10));
        calls.status(&tool(&thread, "dev-a", "c2", None, "pending", 11));
        calls.status(&tool(&thread, "dev-a", "c1", None, "completed", 14));
        let c = calls.calls();
        assert_eq!(c.len(), 2);
        assert_eq!(
            (c[0].title.as_str(), c[0].ended(), c[0].duration),
            ("cargo test", true, Some(TimeDelta::seconds(4)))
        );
        assert_eq!(c[0].paths, ["src/c1.rs"], "a status without paths leaves them");
        assert_eq!((c[1].title.as_str(), c[1].ended()), ("execute", false), "untitled, the kind stands in");
        assert_eq!(calls.paths(), ["src/c1.rs"]);
        // The same id later is another call, as another prompt made it.
        calls.status(&tool(&thread, "dev-a", "c1", Some("again"), "in_progress", 30));
        assert_eq!(calls.calls().len(), 3);
    }

    #[test]
    fn a_stored_call_replaces_the_one_heard_live_once_and_never_goes_back_to_running() {
        let thread = ThreadId::generate();
        let (message, other) = (Uuid::new_v4(), Uuid::new_v4());
        let mut calls = Calls::default();
        calls.status(&tool(&thread, "dev-a", "c1", Some("cargo test"), "in_progress", 10));
        calls.stored(&stored(5, &thread, "dev-a", message, "c1", "in_progress", 10));
        assert_eq!(calls.calls().len(), 1);
        assert_eq!((calls.calls()[0].message, calls.calls()[0].live), (Some(message), false));
        calls.status(&tool(&thread, "dev-a", "c1", None, "completed", 13));
        calls.stored(&stored(6, &thread, "dev-a", message, "c1", "in_progress", 10));
        assert!(calls.calls()[0].ended(), "an older record does not undo the end heard live");
        calls.stored(&stored(7, &thread, "dev-a", message, "c1", "completed", 10));
        assert_eq!(calls.calls()[0].duration, Some(TimeDelta::milliseconds(3100)), "the record's duration wins");
        // Another prompt's call with the same id, or another ant's, is another call.
        calls.stored(&stored(8, &thread, "dev-a", other, "c1", "completed", 2));
        calls.stored(&stored(9, &thread, "dev-b", message, "c1", "completed", 11));
        let ants: Vec<_> = calls.calls().iter().map(|c| (c.ant.as_str(), c.started)).collect();
        assert_eq!(ants, [("dev-a", at(2)), ("dev-a", at(10)), ("dev-b", at(11))], "in order of start");
    }

    #[test]
    fn only_so_many_threads_are_kept() {
        let mut threads = Threads::default();
        let first = ThreadId::generate();
        threads.status(&tool(&first, "dev-a", "c1", None, "pending", 0));
        for n in 1..=MAX_THREADS {
            threads.status(&tool(&ThreadId::generate(), "dev-a", "c1", None, "pending", n.try_into().unwrap()));
        }
        assert!(threads.get(&first).is_none(), "the stalest went");
        assert_eq!(threads.0.len(), MAX_THREADS);
    }
}
