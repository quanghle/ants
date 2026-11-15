//! What `ants dash` shows, and how keys change it, apart from the terminal and the connection: the threads read from
//! the stream, the selected one's history, each thread's live progress, the roster, the pending tool requests and the
//! one opened, and how fresh it all is. Keys and changes return the [`Action`]s the dashboard then performs.

use std::collections::HashMap;
use std::time::Duration;

use ants_proto::{ApprovalRequest, RosterEntry, StatusEvent, ThreadId, UserId, Verdict};
use chrono::{DateTime, TimeDelta, Utc};

use crate::progress;
use crate::thread::{Entry, Summary, Threads};
use crate::tools::{self, State, Update};

/// The list keys move in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    Threads,
    Approvals,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Up,
    Down,
    PageUp,
    PageDown,
    Home,
    End,
    Tab,
    Enter,
    Esc,
    /// Ctrl-C.
    Interrupt,
    Char(char),
}

/// What the dashboard does for a key or a change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Read the thread's history, dropping any read of another one.
    LoadHistory(ThreadId),
    /// Store the decision on the request, the first decision winning, as `ants tools approve` and `reject` do.
    Decide(Box<ApprovalRequest>, Verdict),
    /// Read again what may have missed changes: the tool requests, the roster, the selected thread.
    Refresh,
    Quit,
}

/// The selected thread's stored messages.
#[derive(Debug)]
pub struct History {
    pub thread: ThreadId,
    /// While loading, the messages on the thread read from the stream meanwhile, merged in once loaded.
    pub entries: Vec<Entry>,
    pub loading: bool,
    /// How many lines the view is scrolled up from the latest.
    pub scroll: usize,
}

/// A tool request opened in full.
#[derive(Debug)]
pub struct Detail {
    pub request: ApprovalRequest,
    pub state: State,
    /// The verdict asked for, waiting for its confirmation.
    pub confirm: Option<Verdict>,
    /// A decision is being stored.
    pub deciding: bool,
}

/// The ants' live progress on a thread.
#[derive(Debug)]
pub struct Progress {
    pub status: progress::Status,
    pub at: DateTime<Utc>,
}

/// The connection to NATS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    Connected,
    /// Lost since then; the client reconnects on its own.
    Down(DateTime<Utc>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub text: String,
    pub at: DateTime<Utc>,
    pub level: Level,
}

/// Whether a notice reports an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Error,
}

/// How long a notice shows.
const NOTICE_FOR: TimeDelta = TimeDelta::seconds(15);
/// How many lines PageUp and PageDown scroll.
const PAGE: usize = 10;

pub struct Model {
    pub me: UserId,
    /// How far back the threads are read.
    pub since: Duration,
    threads: Threads,
    /// The threads, most recently active first.
    pub order: Vec<ThreadId>,
    pub selected: Option<ThreadId>,
    pub history: Option<History>,
    pub progress: HashMap<ThreadId, Progress>,
    pub roster: Vec<RosterEntry>,
    pub roster_at: Option<DateTime<Utc>>,
    /// The pending tool requests, oldest first.
    pub approvals: Vec<ApprovalRequest>,
    /// The index of the selected request.
    pub approval: usize,
    pub focus: Pane,
    pub detail: Option<Detail>,
    pub link: Link,
    /// The stream sequence read up to, once the threads were read.
    pub stored_seq: Option<u64>,
    /// When the stream was last read.
    pub stored_at: Option<DateTime<Utc>>,
    pub notice: Option<Notice>,
}

impl Model {
    pub fn new(me: UserId, since: Duration) -> Self {
        Self {
            me,
            since,
            threads: Threads::new(None),
            order: Vec::new(),
            selected: None,
            history: None,
            progress: HashMap::new(),
            roster: Vec::new(),
            roster_at: None,
            approvals: Vec::new(),
            approval: 0,
            focus: Pane::Threads,
            detail: None,
            link: Link::Connected,
            stored_seq: None,
            stored_at: None,
            notice: None,
        }
    }

    pub fn summary(&self, thread: &ThreadId) -> Option<&Summary> {
        self.threads.get(thread)
    }

    pub fn notify(&mut self, text: impl Into<String>, level: Level, now: DateTime<Utc>) {
        self.notice = Some(Notice { text: text.into(), at: now, level });
    }

    /// The notice to show at `now`, if one is recent.
    pub fn notice_at(&self, now: DateTime<Utc>) -> Option<&Notice> {
        self.notice.as_ref().filter(|n| now - n.at < NOTICE_FOR)
    }

    /// Take the messages read from the stream up to sequence `last`. Selects the most recent thread when none is.
    pub fn stored(&mut self, entries: Vec<Entry>, last: u64, now: DateTime<Utc>) -> Option<Action> {
        for entry in &entries {
            self.threads.add(entry);
        }
        if let Some(history) = &mut self.history {
            let after = history.entries.last().map_or(0, |e| e.seq);
            history.entries.extend(entries.into_iter().filter(|e| e.thread_id == history.thread && e.seq > after));
        }
        self.stored_seq = Some(self.stored_seq.unwrap_or_default().max(last));
        self.stored_at = Some(now);
        let mut order: Vec<&Summary> = self.threads.summaries().collect();
        order.sort_by(|a, b| (b.last_at, &b.thread_id).cmp(&(a.last_at, &a.thread_id)));
        self.order = order.into_iter().map(|s| s.thread_id.clone()).collect();
        match (&self.selected, self.order.first()) {
            (None, Some(first)) => Some(self.select(first.clone())),
            _ => None,
        }
    }

    fn select(&mut self, thread: ThreadId) -> Action {
        self.selected = Some(thread.clone());
        self.history = Some(History { thread: thread.clone(), entries: Vec::new(), loading: true, scroll: 0 });
        Action::LoadHistory(thread)
    }

    /// Take the history of `thread` read from the stream, if still wanted.
    pub fn history_loaded(&mut self, thread: &ThreadId, read: anyhow::Result<Vec<Entry>>, now: DateTime<Utc>) {
        let Some(history) = self.history.as_mut().filter(|h| h.thread == *thread && h.loading) else { return };
        history.loading = false;
        match read {
            Ok(mut entries) => {
                let after = entries.last().map_or(0, |e| e.seq);
                entries.extend(std::mem::take(&mut history.entries).into_iter().filter(|e| e.seq > after));
                history.entries = entries;
            }
            Err(e) => self.notify(format!("reading thread {thread}: {e:#}"), Level::Error, now),
        }
    }

    /// Read the selected thread's history again.
    pub fn reload_history(&mut self) -> Option<Action> {
        let thread = self.selected.clone()?;
        Some(self.select(thread))
    }

    pub fn status(&mut self, event: &StatusEvent, now: DateTime<Utc>) {
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

    pub fn roster(&mut self, entries: Vec<RosterEntry>, now: DateTime<Utc>) {
        self.roster = entries;
        self.roster_at = Some(now);
    }

    /// Take word from following the tool requests ([`tools::follow`]).
    pub fn approval(&mut self, update: Update) {
        match update {
            Update::Pending(request) => {
                if !self.approvals.iter().any(|r| r.ant == request.ant && r.id == request.id) {
                    self.approvals.push(request);
                }
            }
            Update::Resolved(request, state) => {
                self.approvals.retain(|r| !(r.ant == request.ant && r.id == request.id));
                self.approval = self.approval.min(self.approvals.len().saturating_sub(1));
                if let Some(detail) = self.detail.as_mut().filter(|d| d.request == request) {
                    detail.state = state;
                    detail.confirm = None;
                }
            }
        }
    }

    /// Following the tool requests starts over: it lists the pending ones again.
    pub fn approvals_reset(&mut self) {
        self.approvals.clear();
        self.approval = 0;
    }

    /// Take the outcome of storing a decision: the line saying so, or why it was not stored.
    pub fn decided(&mut self, outcome: anyhow::Result<String>, now: DateTime<Utc>) {
        if let Some(detail) = &mut self.detail {
            detail.deciding = false;
        }
        match outcome {
            Ok(text) => {
                self.detail = None;
                self.notify(text, Level::Info, now);
            }
            Err(e) => self.notify(format!("{e:#}"), Level::Error, now),
        }
    }

    /// Take the connection's state at `now`: once it is back, what was missed meanwhile is read again.
    pub fn link(&mut self, connected: bool, now: DateTime<Utc>) -> Option<Action> {
        match (connected, self.link) {
            (false, Link::Connected) => {
                self.link = Link::Down(now);
                None
            }
            (true, Link::Down(_)) => {
                self.link = Link::Connected;
                self.notify("reconnected: reading what changed meanwhile", Level::Info, now);
                Some(Action::Refresh)
            }
            _ => None,
        }
    }

    pub fn key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Interrupt {
            return Some(Action::Quit);
        }
        if self.detail.is_some() {
            return self.detail_key(key, now);
        }
        match key {
            Key::Char('q') => Some(Action::Quit),
            Key::Tab => {
                self.focus = match self.focus {
                    Pane::Threads => Pane::Approvals,
                    Pane::Approvals => Pane::Threads,
                };
                None
            }
            Key::Char('R') => Some(Action::Refresh),
            Key::Up | Key::Char('k') => self.step(-1),
            Key::Down | Key::Char('j') => self.step(1),
            Key::Home => self.step(isize::MIN),
            Key::End => self.step(isize::MAX),
            Key::PageUp => {
                if let Some(history) = &mut self.history {
                    history.scroll = history.scroll.saturating_add(PAGE);
                }
                None
            }
            Key::PageDown => {
                if let Some(history) = &mut self.history {
                    history.scroll = history.scroll.saturating_sub(PAGE);
                }
                None
            }
            Key::Enter => match self.focus {
                Pane::Threads => self.reload_history(),
                Pane::Approvals => {
                    if let Some(request) = self.approvals.get(self.approval) {
                        let request = request.clone();
                        self.detail = Some(Detail { request, state: State::Pending, confirm: None, deciding: false });
                    }
                    None
                }
            },
            _ => None,
        }
    }

    /// Move the selection of the focused list by `by`, clamped to its ends.
    fn step(&mut self, by: isize) -> Option<Action> {
        let moved = |at: usize, len: usize| at.saturating_add_signed(by).min(len.saturating_sub(1));
        match self.focus {
            Pane::Approvals => {
                self.approval = moved(self.approval, self.approvals.len());
                None
            }
            Pane::Threads => {
                let at = self.selected.as_ref().and_then(|s| self.order.iter().position(|t| t == s));
                let to = self.order.get(at.map_or(0, |at| moved(at, self.order.len())))?.clone();
                (Some(&to) != self.selected.as_ref()).then(|| self.select(to))
            }
        }
    }

    fn detail_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let detail = self.detail.as_mut()?;
        if detail.deciding {
            return None;
        }
        if let Some(verdict) = detail.confirm.take() {
            if key == Key::Char('y') {
                detail.deciding = true;
                return Some(Action::Decide(Box::new(detail.request.clone()), verdict));
            }
            let id = tools::qualified(&detail.request);
            self.notify(format!("request {id} not answered"), Level::Info, now);
            return None;
        }
        let verdict = match key {
            Key::Esc | Key::Char('q') => {
                self.detail = None;
                return None;
            }
            Key::Char('a') => Verdict::AllowOnce,
            Key::Char('A') => Verdict::AllowAlways,
            Key::Char('r') => Verdict::Reject,
            _ => return None,
        };
        let id = tools::qualified(&detail.request);
        if detail.state != State::Pending {
            self.notify(format!("request {id} is answered already"), Level::Error, now);
        } else if !detail.request.options.contains(&verdict) {
            self.notify(format!("{} is not offered for request {id}", tools::spelling(verdict)), Level::Error, now);
        } else {
            detail.confirm = Some(verdict);
        }
        None
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use ants_proto::{ApprovalDecision, Envelope, StatusUpdate, ToolCall};

    use super::*;
    use crate::thread::Parser;

    pub fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    pub fn user(name: &str) -> UserId {
        name.parse().unwrap()
    }

    /// The entry of a message from `from` to `to` on `thread`, stored at sequence `seq`.
    pub fn message(seq: u64, thread: &ThreadId, from: &str, to: &str, text: &str) -> Entry {
        let envelope = Envelope { thread_id: thread.clone(), ..Envelope::new(user(from), user(to), text) };
        let subject = ants_proto::subject::inbox(&envelope.to, &envelope.from);
        let payload = serde_json::to_vec(&envelope).unwrap();
        Parser::default().entry(seq, at(seq.cast_signed()), &subject, &payload).unwrap()
    }

    pub fn request(ant: &str, title: &str, options: Vec<Verdict>) -> ApprovalRequest {
        let tool = ToolCall { kind: "execute".into(), title: title.into(), ..Default::default() };
        ApprovalRequest::new(user(ant), ThreadId::generate(), tool, options, Duration::from_mins(5))
    }

    fn model() -> Model {
        Model::new(user("alice"), Duration::from_hours(24))
    }

    #[test]
    fn keys_select_threads_most_recent_first_and_load_their_history() {
        let (old, new) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let read = vec![message(1, &old, "alice", "dev-a", "old"), message(2, &new, "dev-a", "alice", "new")];
        assert_eq!(m.stored(read, 2, at(5)), Some(Action::LoadHistory(new.clone())));
        assert_eq!(m.order, [new.clone(), old.clone()]);
        assert_eq!(m.key(Key::Down, at(6)), Some(Action::LoadHistory(old.clone())));
        assert_eq!(m.key(Key::Down, at(6)), None, "the last thread stays selected");
        assert_eq!(m.selected, Some(old.clone()));
        assert_eq!(m.key(Key::Char('k'), at(6)), Some(Action::LoadHistory(new.clone())));
        // Activity on the other thread moves it first; the selection stays on its thread.
        m.stored(vec![message(3, &old, "dev-a", "alice", "again")], 3, at(7));
        assert_eq!(m.order, [old, new.clone()]);
        assert_eq!(m.selected, Some(new));
        assert_eq!(m.key(Key::Char('q'), at(8)), Some(Action::Quit));
    }

    #[test]
    fn messages_read_while_a_history_loads_are_merged_once() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "hi")], 1, at(1));
        // Read meanwhile: one the history read has too, one after it.
        let meanwhile = vec![message(2, &thread, "dev-a", "alice", "two"), message(3, &thread, "dev-a", "alice", "3")];
        m.stored(meanwhile, 3, at(2));
        let loaded = vec![message(1, &thread, "alice", "dev-a", "hi"), message(2, &thread, "dev-a", "alice", "two")];
        m.history_loaded(&thread, Ok(loaded), at(3));
        m.stored(vec![message(4, &thread, "dev-a", "alice", "four")], 4, at(4));
        let history = m.history.as_ref().unwrap();
        assert!(!history.loading);
        assert_eq!(history.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2, 3, 4]);
        // A history read of a thread no longer selected is dropped.
        m.history_loaded(&ThreadId::generate(), Ok(Vec::new()), at(5));
        assert_eq!(m.history.as_ref().unwrap().entries.len(), 4);
    }

    #[test]
    fn a_decision_needs_details_shown_an_offered_verdict_and_a_confirmation() {
        let mut m = model();
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        m.approval(Update::Pending(push.clone()));
        m.approval(Update::Pending(push.clone()));
        assert_eq!(m.approvals.len(), 1);
        assert_eq!(m.key(Key::Char('a'), at(0)), None, "no decision from the list");
        m.key(Key::Tab, at(0));
        assert_eq!(m.key(Key::Enter, at(0)), None);
        assert_eq!(m.detail.as_ref().map(|d| &d.request), Some(&push));
        m.key(Key::Char('A'), at(1));
        assert!(
            m.notice_at(at(1))
                .is_some_and(|n| n.level == Level::Error && n.text.contains("allow-always is not offered"))
        );
        assert_eq!(m.detail.as_ref().unwrap().confirm, None);
        // Any key but y cancels.
        m.key(Key::Char('a'), at(2));
        assert_eq!(m.key(Key::Char('n'), at(2)), None);
        assert!(m.notice_at(at(2)).is_some_and(|n| n.text.contains("not answered")));
        m.key(Key::Char('r'), at(3));
        assert_eq!(m.key(Key::Char('y'), at(3)), Some(Action::Decide(Box::new(push), Verdict::Reject)));
        assert_eq!(m.key(Key::Char('y'), at(3)), None, "one decision at a time");
        m.decided(Err(anyhow::anyhow!("request was already answered: allow-once")), at(4));
        assert!(m.detail.is_some(), "a refused decision leaves the request open");
        m.key(Key::Char('a'), at(5));
        m.key(Key::Char('y'), at(5));
        m.decided(Ok("answered".into()), at(6));
        assert!(m.detail.is_none());
    }

    #[test]
    fn a_request_answered_elsewhere_while_open_takes_no_decision() {
        let mut m = model();
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        m.approval(Update::Pending(push.clone()));
        m.focus = Pane::Approvals;
        m.key(Key::Enter, at(0));
        m.key(Key::Char('a'), at(0));
        let decision = ApprovalDecision::new(push.id, push.ant.clone(), Verdict::Reject);
        let state = State::DecidedByAnt(user("lead"), decision);
        m.approval(Update::Resolved(push, state.clone()));
        assert!(m.approvals.is_empty());
        let detail = m.detail.as_ref().unwrap();
        assert_eq!((&detail.state, detail.confirm), (&state, None));
        assert_eq!(m.key(Key::Char('a'), at(1)), None);
        assert!(m.notice_at(at(1)).is_some_and(|n| n.text.contains("answered already")));
        assert!(m.notice_at(at(30)).is_none(), "notices fade");
    }

    #[test]
    fn a_reconnection_refreshes_once() {
        let mut m = model();
        assert_eq!(m.link(true, at(0)), None);
        assert_eq!(m.link(false, at(1)), None);
        assert_eq!(m.link(false, at(2)), None);
        assert_eq!(m.link, Link::Down(at(1)));
        assert_eq!(m.link(true, at(3)), Some(Action::Refresh));
        assert_eq!(m.link, Link::Connected);
    }

    #[test]
    fn progress_shows_until_the_ants_go_idle() {
        let thread = ThreadId::generate();
        let mut m = model();
        let event = |update| StatusEvent::new(user("dev-a"), thread.clone(), update);
        m.status(&event(StatusUpdate::State { state: ants_proto::State::Working }), at(1));
        assert_eq!(m.progress_of(&thread, 80), Some(("dev-a: working".to_owned(), at(1))));
        m.status(&event(StatusUpdate::State { state: ants_proto::State::Idle }), at(2));
        assert_eq!(m.progress_of(&thread, 80), None);
    }
}
