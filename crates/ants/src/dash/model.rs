//! What `ants dash` shows, and how keys change it, apart from the terminal and the connection: what is read live
//! ([`State`]), the selected thread and its history, the pending tool request selected or opened, and the latest
//! notice. Keys and changes return the [`Action`]s the dashboard then performs.

use ants_proto::{ApprovalRequest, ThreadId, Verdict};
use chrono::{DateTime, TimeDelta, Utc};

use crate::live::{self, History, State};
use crate::thread::Entry;
use crate::tools::{self, Update};

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

/// A tool request opened in full.
#[derive(Debug)]
pub struct Detail {
    pub request: ApprovalRequest,
    pub state: tools::State,
    /// The verdict asked for, waiting for its confirmation.
    pub confirm: Option<Verdict>,
    /// A decision is being stored.
    pub deciding: bool,
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
    pub live: State,
    pub selected: Option<ThreadId>,
    /// The selected thread's stored messages.
    pub history: Option<History>,
    /// How many lines the history is scrolled up from the latest.
    pub scroll: usize,
    /// The index of the selected request.
    pub approval: usize,
    pub focus: Pane,
    pub detail: Option<Detail>,
    pub notice: Option<Notice>,
}

impl Model {
    pub fn new(live: State) -> Self {
        Self {
            live,
            selected: None,
            history: None,
            scroll: 0,
            approval: 0,
            focus: Pane::Threads,
            detail: None,
            notice: None,
        }
    }

    pub fn notify(&mut self, text: impl Into<String>, level: Level, now: DateTime<Utc>) {
        self.notice = Some(Notice { text: text.into(), at: now, level });
    }

    /// The notice to show at `now`, if one is recent.
    pub fn notice_at(&self, now: DateTime<Utc>) -> Option<&Notice> {
        self.notice.as_ref().filter(|n| now - n.at < NOTICE_FOR)
    }

    /// Take the messages read from the stream. Selects the most recent thread when none is.
    pub fn stored(&mut self, entries: Vec<Entry>, now: DateTime<Utc>) -> Option<Action> {
        self.live.stored(&entries, now);
        if let Some(history) = &mut self.history {
            history.stored(entries);
        }
        match (&self.selected, self.live.order.first()) {
            (None, Some(first)) => Some(self.select(first.clone())),
            _ => None,
        }
    }

    fn select(&mut self, thread: ThreadId) -> Action {
        self.selected = Some(thread.clone());
        self.history = Some(History::new(thread.clone()));
        self.scroll = 0;
        Action::LoadHistory(thread)
    }

    /// Take the history of `thread` read from the stream, if still wanted.
    pub fn history_loaded(&mut self, thread: &ThreadId, read: anyhow::Result<Vec<Entry>>, now: DateTime<Utc>) {
        let Some(history) = &mut self.history else { return };
        if let Err(e) = history.loaded(thread, read) {
            self.notify(format!("reading thread {thread}: {e:#}"), Level::Error, now);
        }
    }

    /// Read the selected thread's history again.
    pub fn reload_history(&mut self) -> Option<Action> {
        let thread = self.selected.clone()?;
        Some(self.select(thread))
    }

    /// Take word from following the tool requests ([`tools::follow`]).
    pub fn approval(&mut self, update: Update) {
        self.live.approval(&update);
        if let Update::Resolved(request, state) = update {
            self.approval = self.approval.min(self.live.approvals.len().saturating_sub(1));
            if let Some(detail) = self.detail.as_mut().filter(|d| d.request == request) {
                detail.state = state;
                detail.confirm = None;
            }
        }
    }

    /// Following the tool requests starts over: it lists the pending ones again.
    pub fn approvals_reset(&mut self) {
        self.live.approvals_reset();
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
        if !self.live.link(connected, now) {
            return None;
        }
        self.notify("reconnected: reading what changed meanwhile", Level::Info, now);
        Some(Action::Refresh)
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
                if self.history.is_some() {
                    self.scroll = self.scroll.saturating_add(PAGE);
                }
                None
            }
            Key::PageDown => {
                if self.history.is_some() {
                    self.scroll = self.scroll.saturating_sub(PAGE);
                }
                None
            }
            Key::Enter => match self.focus {
                Pane::Threads => self.reload_history(),
                Pane::Approvals => {
                    if let Some(request) = self.live.approvals.get(self.approval) {
                        let request = request.clone();
                        let state = tools::State::Pending;
                        self.detail = Some(Detail { request, state, confirm: None, deciding: false });
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
                self.approval = moved(self.approval, self.live.approvals.len());
                None
            }
            Pane::Threads => {
                let order = &self.live.order;
                let at = self.selected.as_ref().and_then(|s| order.iter().position(|t| t == s));
                let to = order.get(at.map_or(0, |at| moved(at, order.len())))?.clone();
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
        match live::refusal(&detail.request, &detail.state, verdict) {
            Some(refusal) => self.notify(refusal, Level::Error, now),
            None => detail.confirm = Some(verdict),
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_proto::ApprovalDecision;

    use super::*;
    use crate::live::Link;
    use crate::live::tests::{at, message, request, user};

    fn model() -> Model {
        Model::new(State::new(user("alice"), Duration::from_hours(24)))
    }

    #[test]
    fn keys_select_threads_most_recent_first_and_load_their_history() {
        let (old, new) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let read = vec![message(1, &old, "alice", "dev-a", "old"), message(2, &new, "dev-a", "alice", "new")];
        assert_eq!(m.stored(read, at(5)), Some(Action::LoadHistory(new.clone())));
        assert_eq!(m.live.order, [new.clone(), old.clone()]);
        assert_eq!(m.key(Key::Down, at(6)), Some(Action::LoadHistory(old.clone())));
        assert_eq!(m.key(Key::Down, at(6)), None, "the last thread stays selected");
        assert_eq!(m.selected, Some(old.clone()));
        assert_eq!(m.key(Key::Char('k'), at(6)), Some(Action::LoadHistory(new.clone())));
        // Activity on the other thread moves it first; the selection stays on its thread.
        m.stored(vec![message(3, &old, "dev-a", "alice", "again")], at(7));
        assert_eq!(m.live.order, [old, new.clone()]);
        assert_eq!(m.selected, Some(new));
        assert_eq!(m.key(Key::Char('q'), at(8)), Some(Action::Quit));
    }

    #[test]
    fn messages_read_while_a_history_loads_are_merged_once() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "hi")], at(1));
        // Read meanwhile: one the history read has too, one after it.
        let meanwhile = vec![message(2, &thread, "dev-a", "alice", "two"), message(3, &thread, "dev-a", "alice", "3")];
        m.stored(meanwhile, at(2));
        let loaded = vec![message(1, &thread, "alice", "dev-a", "hi"), message(2, &thread, "dev-a", "alice", "two")];
        m.history_loaded(&thread, Ok(loaded), at(3));
        m.stored(vec![message(4, &thread, "dev-a", "alice", "four")], at(4));
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
        assert_eq!(m.live.approvals.len(), 1);
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
        let state = tools::State::DecidedByAnt(user("lead"), decision);
        m.approval(Update::Resolved(push, state.clone()));
        assert!(m.live.approvals.is_empty());
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
        assert_eq!(m.live.link, Link::Down(at(1)));
        assert_eq!(m.link(true, at(3)), Some(Action::Refresh));
        assert_eq!(m.live.link, Link::Connected);
    }
}
