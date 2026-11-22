//! What a live view knows of the ants, apart from the terminal and the connection: the threads read from the stream,
//! each thread's live progress, the colony, the pending tool requests and the state of the connection, and a thread's
//! history as it loads.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ants_proto::{ApprovalRequest, ColonyEntry, StatusEvent, StatusUpdate, ThreadId, ThreadMarks, UserId, Uuid};
use chrono::{DateTime, Utc};

use crate::messages::progress;
use crate::threads::thread::{self, Entry, Summary, Threads};
use crate::tools::{HeldAct, Update};

/// The ants' live progress on a thread.
#[derive(Debug)]
pub struct Progress {
    pub status: progress::Status,
    pub at: DateTime<Utc>,
}

/// The connection to NATS and, through it, to the hub's JetStream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    Connected,
    /// Lost since then; the client reconnects on its own.
    Down(DateTime<Utc>),
    /// The server is connected, but the hub's JetStream has not answered since then, as through a leaf node whose
    /// uplink is down; the reads are tried again on their own.
    Unreached(DateTime<Utc>),
}

pub struct State {
    pub me: UserId,
    /// How far back the threads are read.
    pub since: Duration,
    threads: Threads,
    /// How the human marked threads, as last read: archived ones are left out of `order`.
    pub marks: ThreadMarks,
    /// How long a thread goes without a message before it counts as archived; None for never.
    pub auto_archive: Option<Duration>,
    /// The threads, most recently active first, but for the archived ones with no message since and the idle ones.
    pub order: Vec<ThreadId>,
    /// The archived threads with no message since and the idle ones, which lists leave out.
    hidden: HashSet<ThreadId>,
    pub progress: HashMap<ThreadId, Progress>,
    /// The messages waiting their turn on their thread, by id, with how many go before each, as the ants' daemons
    /// told; a message leaves once its turn comes.
    pub queued: HashMap<Uuid, usize>,
    pub colony: Vec<ColonyEntry>,
    pub colony_at: Option<DateTime<Utc>>,
    /// The pending tool requests, oldest first.
    pub approvals: Vec<ApprovalRequest>,
    /// The decisions assistants made on pending requests for their humans, waiting for consent, by ant and request.
    pub held: HashMap<(UserId, Uuid), Vec<HeldAct>>,
    pub link: Link,
    /// When the stream was last read.
    pub stored_at: Option<DateTime<Utc>>,
}

impl State {
    pub fn new(me: UserId, since: Duration) -> Self {
        Self {
            me,
            since,
            threads: Threads::new(None),
            marks: ThreadMarks::default(),
            auto_archive: None,
            order: Vec::new(),
            hidden: HashSet::new(),
            progress: HashMap::new(),
            queued: HashMap::new(),
            colony: Vec::new(),
            colony_at: None,
            approvals: Vec::new(),
            held: HashMap::new(),
            link: Link::Connected,
            stored_at: None,
        }
    }

    pub fn summary(&self, thread: &ThreadId) -> Option<&Summary> {
        self.threads.get(thread)
    }

    /// Take the messages read from the stream at `now`, ordering the threads again.
    pub fn stored(&mut self, entries: &[Entry], now: DateTime<Utc>) {
        for entry in entries {
            self.threads.add(entry);
        }
        self.stored_at = Some(now);
        self.reorder(now);
    }

    /// Order the threads again at `now`, as after the marks changed.
    pub fn reorder(&mut self, now: DateTime<Utc>) {
        let idle_before = thread::idle_before(self.auto_archive, now);
        self.hidden = self
            .threads
            .summaries()
            .filter(|s| self.marks.hides(&s.thread_id, s.last_seq, s.last_at, idle_before))
            .map(|s| s.thread_id.clone())
            .collect();
        let mut order: Vec<&Summary> =
            self.threads.summaries().filter(|s| !s.catch_up && !self.hidden.contains(&s.thread_id)).collect();
        order.sort_by(|a, b| (b.last_at, &b.thread_id).cmp(&(a.last_at, &a.thread_id)));
        self.order = order.into_iter().map(|s| s.thread_id.clone()).collect();
    }

    /// Whether lists leave `thread` out: archived with no message since, or idle.
    pub fn hidden(&self, thread: &ThreadId) -> bool {
        self.hidden.contains(thread)
    }

    pub fn status(&mut self, event: &StatusEvent, now: DateTime<Utc>) {
        if let StatusUpdate::Queued { message, ahead } = event.update {
            match ahead {
                0 => self.queued.remove(&message),
                ahead => self.queued.insert(message, ahead),
            };
        }
        let progress = self
            .progress
            .entry(event.thread_id.clone())
            .or_insert_with(|| Progress { status: progress::Status::new(Vec::new()), at: now });
        progress.status.apply(event);
        progress.at = now;
        if progress.status.line(usize::MAX).is_empty() {
            self.progress.remove(&event.thread_id);
        }
    }

    /// What the ants on `thread` are doing, in at most `width` characters, and when that was heard; None when none
    /// is busy there.
    pub fn progress_of(&self, thread: &ThreadId, width: usize) -> Option<(String, DateTime<Utc>)> {
        let progress = self.progress.get(thread)?;
        Some((progress.status.line(width), progress.at))
    }

    pub fn colony(&mut self, entries: Vec<ColonyEntry>, now: DateTime<Utc>) {
        self.colony = entries;
        self.colony_at = Some(now);
    }

    /// Take word from following the tool requests ([`crate::tools::follow`]) into the pending ones.
    pub fn approval(&mut self, update: &Update) {
        match update {
            Update::Pending(request) => {
                if !self.approvals.iter().any(|r| r.ant == request.ant && r.id == request.id) {
                    self.approvals.push(request.clone());
                }
            }
            Update::Held(request, held) if held.is_empty() => {
                self.held.remove(&(request.ant.clone(), request.id));
            }
            Update::Held(request, held) => {
                self.held.insert((request.ant.clone(), request.id), held.clone());
            }
            Update::Resolved(request, _) => {
                self.approvals.retain(|r| !(r.ant == request.ant && r.id == request.id));
                self.held.remove(&(request.ant.clone(), request.id));
            }
        }
    }

    /// Following the tool requests starts over: it lists the pending ones again.
    pub fn approvals_reset(&mut self) {
        self.approvals.clear();
        self.held.clear();
    }

    /// Take the connection's state at `now`: whether it is back after being lost, so what was missed meanwhile is to
    /// be read again.
    pub fn link(&mut self, connected: bool, now: DateTime<Utc>) -> bool {
        match (connected, self.link) {
            (false, Link::Connected) => {
                self.link = Link::Down(now);
                false
            }
            (false, Link::Unreached(since)) => {
                self.link = Link::Down(since);
                false
            }
            (true, Link::Down(_)) => {
                self.link = Link::Connected;
                true
            }
            _ => false,
        }
    }

    /// Take `e`, why `what` failed at `now`: when the hub's JetStream did not answer, the hub out of reach and None, as
    /// the header shows it and the read is tried again on its own; else the error to tell.
    pub fn failed(&mut self, what: &str, e: &anyhow::Error, now: DateTime<Utc>) -> Option<String> {
        if ants_daemon::bus::unanswered(e) {
            self.unreached(now);
            None
        } else if what.is_empty() {
            Some(format!("{e:#}"))
        } else {
            Some(format!("{what}: {e:#}"))
        }
    }

    /// Take a read the hub's JetStream did not answer at `now`, the server connected.
    pub fn unreached(&mut self, now: DateTime<Utc>) {
        if self.link == Link::Connected {
            self.link = Link::Unreached(now);
        }
    }

    /// Take a read the hub's JetStream answered: whether it is back after being out of reach, so what was missed
    /// meanwhile is to be read again.
    pub fn reached(&mut self) -> bool {
        let back = matches!(self.link, Link::Unreached(_));
        if back {
            self.link = Link::Connected;
        }
        back
    }
}

/// A thread's stored messages, as its history loads and messages on it are read from the stream meanwhile.
#[derive(Debug)]
pub struct History {
    pub thread: ThreadId,
    /// While loading, the messages on the thread read from the stream meanwhile, merged in once loaded.
    pub entries: Vec<Entry>,
    pub loading: bool,
}

impl History {
    /// The history of `thread`, to be loaded.
    pub fn new(thread: ThreadId) -> Self {
        Self { thread, entries: Vec::new(), loading: true }
    }

    /// Take the messages read from the stream: those on the thread after the last one held.
    pub fn stored(&mut self, entries: Vec<Entry>) {
        let after = self.entries.last().map_or(0, |e| e.seq);
        self.entries.extend(entries.into_iter().filter(|e| e.thread_id == self.thread && e.seq > after));
    }

    /// Take the history of `thread` read from the stream, if it is the one loading, merging in the messages read
    /// meanwhile; the read's error if it failed.
    pub fn loaded(&mut self, thread: &ThreadId, read: anyhow::Result<Vec<Entry>>) -> anyhow::Result<()> {
        if self.thread != *thread || !self.loading {
            return Ok(());
        }
        self.loading = false;
        let mut entries = read?;
        let after = entries.last().map_or(0, |e| e.seq);
        entries.extend(std::mem::take(&mut self.entries).into_iter().filter(|e| e.seq > after));
        self.entries = entries;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ApprovalDecision, Verdict};

    use super::*;
    use crate::test_support::live::{at, message, request, user};
    use crate::tools;

    fn state() -> State {
        State::new(user("alice"), Duration::from_hours(24))
    }

    #[test]
    fn threads_order_most_recent_first() {
        let (old, new) = (ThreadId::generate(), ThreadId::generate());
        let mut s = state();
        s.stored(&[message(1, &old, "alice", "dev-a", "old"), message(2, &new, "dev-a", "alice", "new")], at(5));
        assert_eq!(s.order, [new.clone(), old.clone()]);
        assert_eq!(s.stored_at, Some(at(5)));
        s.stored(&[message(3, &old, "dev-a", "alice", "again")], at(7));
        assert_eq!(s.order, [old.clone(), new]);
        assert_eq!(s.summary(&old).map(|s| s.last_seq), Some(3));
    }

    #[test]
    fn archived_and_idle_threads_are_left_out_until_a_message_is_stored_on_them() {
        let (archived, idle, kept, live) =
            (ThreadId::generate(), ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut s = state();
        s.marks.archive(&archived, 1);
        s.marks.kept.insert(kept.clone(), at(15));
        s.auto_archive = Some(Duration::from_secs(10));
        let read = [
            message(1, &archived, "alice", "dev-a", "done"),
            message(2, &idle, "alice", "dev-a", "quiet"),
            message(3, &kept, "alice", "dev-a", "kept"),
            message(20, &live, "alice", "dev-a", "busy"),
        ];
        s.stored(&read, at(20));
        assert_eq!(s.order, [live.clone(), kept.clone()], "idle since 10s ago or archived");
        assert!(s.hidden(&archived) && s.hidden(&idle) && !s.hidden(&kept) && !s.hidden(&live));
        let read = [message(21, &idle, "dev-a", "alice", "back"), message(22, &archived, "dev-a", "alice", "news")];
        s.stored(&read, at(22));
        assert_eq!(s.order, [archived.clone(), idle.clone(), live.clone(), kept]);
        assert!(!s.hidden(&archived) && !s.hidden(&idle));
        s.stored(&[], at(26));
        assert_eq!(s.order, [archived, idle, live], "kept until idle since being kept");
    }

    #[test]
    fn messages_read_while_a_history_loads_are_merged_once() {
        let thread = ThreadId::generate();
        let mut h = History::new(thread.clone());
        h.stored(vec![message(1, &ThreadId::generate(), "alice", "dev-a", "elsewhere")]);
        // Read meanwhile: one the history read has too, one after it.
        h.stored(vec![message(2, &thread, "dev-a", "alice", "two"), message(3, &thread, "dev-a", "alice", "3")]);
        let loaded = vec![message(1, &thread, "alice", "dev-a", "hi"), message(2, &thread, "dev-a", "alice", "two")];
        h.loaded(&thread, Ok(loaded)).unwrap();
        h.stored(vec![message(4, &thread, "dev-a", "alice", "four")]);
        assert!(!h.loading);
        assert_eq!(h.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2, 3, 4]);
        // A history read of another thread, or read again, is dropped.
        h.loaded(&ThreadId::generate(), Ok(Vec::new())).unwrap();
        h.loaded(&thread, Err(anyhow::anyhow!("late"))).unwrap();
        assert_eq!(h.entries.len(), 4);
    }

    #[test]
    fn a_failed_history_read_stops_loading_and_says_why() {
        let thread = ThreadId::generate();
        let mut h = History::new(thread.clone());
        h.stored(vec![message(2, &thread, "dev-a", "alice", "two")]);
        let e = h.loaded(&thread, Err(anyhow::anyhow!("no stream"))).unwrap_err();
        assert_eq!(e.to_string(), "no stream");
        assert!(!h.loading);
        assert_eq!(h.entries.len(), 1, "what was read meanwhile stays");
    }

    #[test]
    fn pending_requests_are_kept_once_until_resolved_or_followed_again() {
        let mut s = state();
        let (push, rm) = (request("dev-a", "git push", vec![Verdict::AllowOnce]), request("dev-b", "rm", vec![]));
        for update in [Update::Pending(push.clone()), Update::Pending(push.clone()), Update::Pending(rm.clone())] {
            s.approval(&update);
        }
        assert_eq!(s.approvals, [push.clone(), rm.clone()]);
        let held = HeldAct { human: user("alice"), assistant: user("pa"), verdict: Verdict::AllowOnce, sent_at: at(1) };
        s.approval(&Update::Held(push.clone(), vec![held.clone()]));
        assert_eq!(s.held[&(push.ant.clone(), push.id)], [held]);
        let decision = ApprovalDecision::new(push.id, push.ant.clone(), Verdict::Reject);
        s.approval(&Update::Resolved(push, tools::State::DecidedByAnt(user("lead"), decision)));
        assert_eq!(s.approvals, [rm]);
        assert!(s.held.is_empty(), "with the request resolved");
        s.approvals_reset();
        assert!(s.approvals.is_empty());
    }

    #[test]
    fn a_reconnection_is_told_once() {
        let mut s = state();
        assert!(!s.link(true, at(0)));
        assert!(!s.link(false, at(1)));
        assert!(!s.link(false, at(2)));
        assert_eq!(s.link, Link::Down(at(1)));
        assert!(s.link(true, at(3)));
        assert_eq!(s.link, Link::Connected);
        assert!(!s.link(true, at(4)));
    }

    #[test]
    fn the_hub_out_of_reach_is_told_apart_from_the_server_lost() {
        let mut s = state();
        assert!(!s.reached(), "already reached");
        s.unreached(at(1));
        s.unreached(at(2));
        assert_eq!(s.link, Link::Unreached(at(1)));
        assert!(!s.link(true, at(3)), "the server being connected does not bring the hub back");
        assert_eq!(s.link, Link::Unreached(at(1)));
        assert!(s.reached());
        assert_eq!(s.link, Link::Connected);
        s.unreached(at(4));
        assert!(!s.link(false, at(5)));
        assert_eq!(s.link, Link::Down(at(4)), "down since the hub was last reached");
        s.unreached(at(6));
        assert_eq!(s.link, Link::Down(at(4)), "a server lost is told over the hub out of reach");
        assert!(s.link(true, at(7)));
    }

    #[test]
    fn progress_shows_until_the_ants_go_idle() {
        let thread = ThreadId::generate();
        let mut s = state();
        let event = |update| StatusEvent::new(user("dev-a"), thread.clone(), update);
        s.status(&event(StatusUpdate::State { state: ants_proto::State::Working }), at(1));
        assert_eq!(s.progress_of(&thread, 80), Some(("dev-a: working".to_owned(), at(1))));
        s.status(&event(StatusUpdate::State { state: ants_proto::State::Idle }), at(2));
        assert_eq!(s.progress_of(&thread, 80), None);
    }

    #[test]
    fn a_message_counts_as_queued_until_its_turn_comes() {
        let thread = ThreadId::generate();
        let mut s = state();
        let message = Uuid::now_v7();
        let queued = |ahead| StatusEvent::new(user("dev-a"), thread.clone(), StatusUpdate::Queued { message, ahead });
        s.status(&queued(2), at(1));
        assert_eq!(s.queued.get(&message), Some(&2));
        s.status(&queued(1), at(2));
        assert_eq!(s.queued.get(&message), Some(&1));
        s.status(&queued(0), at(3));
        assert!(s.queued.is_empty());
    }
}
