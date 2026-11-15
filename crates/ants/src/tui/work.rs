//! The work view: every thread read, with the state the messages on it tell, those waiting on the human first.
//! What each thread's messages tell ([`Facts`]) also feeds home and the nav's counts.

use std::collections::HashMap;

use ants_proto::{ChannelId, EscalationReason, ThreadId, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};

use super::frame::{self, ago};
use super::model::{Data, Key, To};
use crate::thread::{Entry, Kind};

/// What a thread waits on, as its messages and the ants' progress tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadState {
    /// A tool request, an unread message to the human, or an ant's answer to the human's request.
    WaitingOnMe,
    /// Escalated to a human, or a message on it given up on, and no human said anything since.
    Stuck,
    /// An ant is busy on it.
    Working,
    /// An ant has a message on it it did not answer yet.
    WaitingOnAnt,
    Idle,
}

impl ThreadState {
    /// The states, as the nav lists them.
    pub const ALL: [Self; 5] = [Self::WaitingOnMe, Self::Working, Self::WaitingOnAnt, Self::Stuck, Self::Idle];

    pub fn label(self) -> &'static str {
        match self {
            Self::WaitingOnMe => "waiting on me",
            Self::Stuck => "stuck",
            Self::Working => "working",
            Self::WaitingOnAnt => "waiting on ant",
            Self::Idle => "idle",
        }
    }

    /// Where threads in this state sort, the most urgent first.
    fn rank(self) -> u8 {
        match self {
            Self::WaitingOnMe => 0,
            Self::Stuck => 1,
            Self::Working => 2,
            Self::WaitingOnAnt => 3,
            Self::Idle => 4,
        }
    }

    pub fn color(self) -> Color {
        match self {
            Self::WaitingOnMe => Color::Yellow,
            Self::Stuck => Color::Red,
            Self::Working => Color::Green,
            Self::WaitingOnAnt => Color::Cyan,
            Self::Idle => Color::DarkGray,
        }
    }
}

/// Something on a thread that may need the human: an escalation or a dead letter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub text: String,
}

/// A message on a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Said {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub from: UserId,
    pub to: UserId,
}

/// A message to the human, in its inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inboxed {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub kind: Kind,
    pub from: UserId,
    pub channel: Option<ChannelId>,
    pub text: String,
}

/// What a thread's messages tell of its state.
#[derive(Debug, Default)]
pub struct Fact {
    /// Each user's last message, post or note on it.
    sent: HashMap<UserId, u64>,
    /// Each user's last inbox message on it.
    received: HashMap<UserId, u64>,
    /// The last escalation to each user, but for tool requests, which [`crate::live::State::approvals`] holds.
    escalated: HashMap<UserId, Alert>,
    /// The last message given up on.
    dead: Option<Alert>,
    pub latest: Option<Said>,
    /// The messages to the human.
    pub inbox: Vec<Inboxed>,
}

/// Whether `entry` is in the inbox of `me`.
pub fn to(entry: &Entry, me: &UserId) -> bool {
    matches!(entry.kind, Kind::Message | Kind::Escalation | Kind::Mention) && entry.to.as_ref() == Some(me)
}

/// What the messages read tell of each thread.
#[derive(Debug, Default)]
pub struct Facts(HashMap<ThreadId, Fact>);

impl Facts {
    pub fn add(&mut self, entry: &Entry, me: &UserId) {
        if entry.forged {
            return;
        }
        let fact = self.0.entry(entry.thread_id.clone()).or_default();
        let alert = || Alert { seq: entry.seq, at: entry.stored_at, text: entry.text.clone() };
        if let Some(from) = &entry.from
            && matches!(entry.kind, Kind::Message | Kind::Post | Kind::Note)
        {
            fact.sent.insert(from.clone(), entry.seq);
        }
        match (entry.kind, &entry.from, &entry.to) {
            (Kind::Message, Some(from), Some(to)) => {
                fact.received.insert(to.clone(), entry.seq);
                fact.latest = Some(Said { seq: entry.seq, at: entry.stored_at, from: from.clone(), to: to.clone() });
            }
            (Kind::Escalation, _, Some(to)) if entry.reason != Some(EscalationReason::Approval) => {
                fact.escalated.insert(to.clone(), alert());
            }
            (Kind::Dead, _, _) => fact.dead = Some(alert()),
            _ => {}
        }
        if to(entry, me)
            && let Some(from) = &entry.from
        {
            fact.inbox.push(Inboxed {
                seq: entry.seq,
                at: entry.stored_at,
                kind: entry.kind,
                from: from.clone(),
                channel: entry.channel.clone(),
                text: entry.text.clone(),
            });
        }
    }

    pub fn get(&self, thread: &ThreadId) -> Option<&Fact> {
        self.0.get(thread)
    }
}

impl Data {
    fn last_human(&self, fact: &Fact) -> u64 {
        fact.sent.iter().filter(|(u, _)| self.is_human(u)).map(|(_, seq)| *seq).max().unwrap_or(0)
    }

    /// Why the thread is stuck: its latest escalation to a human or dead letter that no human said anything after.
    pub fn stuck(&self, thread: &ThreadId) -> Option<&Alert> {
        let fact = self.facts.get(thread)?;
        let after = self.last_human(fact);
        let escalated = fact.escalated.iter().filter(|(to, _)| self.is_human(to)).map(|(_, a)| a);
        escalated.chain(&fact.dead).filter(|a| a.seq > after).max_by_key(|a| a.seq)
    }

    /// Whether an ant has a message on the thread it did not answer yet.
    fn waiting_on_ant(&self, fact: &Fact) -> bool {
        fact.received.iter().any(|(u, seq)| !self.is_human(u) && fact.sent.get(u).is_none_or(|sent| sent < seq))
    }

    /// Whether an ant answered the human on a thread the human owns, and is done with it: the human's turn.
    pub fn finished(&self, thread: &ThreadId) -> bool {
        let (Some(summary), Some(fact)) = (self.live.summary(thread), self.facts.get(thread)) else { return false };
        let Some(latest) = &fact.latest else { return false };
        summary.owner.as_ref() == Some(self.me())
            && latest.to == *self.me()
            && !self.is_human(&latest.from)
            && !self.live.progress.contains_key(thread)
            && !self.waiting_on_ant(fact)
    }

    /// The human's unread messages on the thread.
    pub fn unread_on(&self, thread: &ThreadId) -> impl Iterator<Item = &Inboxed> {
        self.facts.get(thread).into_iter().flat_map(|f| &f.inbox).filter(|i| self.unread(i.seq))
    }

    pub fn state(&self, thread: &ThreadId) -> ThreadState {
        if self.stuck(thread).is_some() {
            return ThreadState::Stuck;
        }
        if self.live.approvals.iter().any(|r| r.thread_id == *thread)
            || self.unread_on(thread).next().is_some()
            || self.finished(thread)
        {
            return ThreadState::WaitingOnMe;
        }
        if self.live.progress.contains_key(thread) {
            return ThreadState::Working;
        }
        if self.facts.get(thread).is_some_and(|f| self.waiting_on_ant(f)) {
            return ThreadState::WaitingOnAnt;
        }
        ThreadState::Idle
    }

    /// The number of threads in each state.
    pub fn counts(&self) -> HashMap<ThreadState, usize> {
        let mut counts = HashMap::new();
        for thread in &self.live.order {
            *counts.entry(self.state(thread)).or_default() += 1;
        }
        counts
    }
}

/// Which threads the work view lists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    pub state: Option<ThreadState>,
    /// Only threads this ant or human is on.
    pub user: Option<UserId>,
    /// Only threads posted in this channel.
    pub channel: Option<ChannelId>,
}

impl Filter {
    fn title(&self) -> String {
        let mut parts = vec![self.state.map_or("work", ThreadState::label).to_owned()];
        parts.extend(self.user.as_ref().map(|u| format!("with {u}")));
        parts.extend(self.channel.as_ref().map(|c| format!("in #{c}")));
        parts.join(" ")
    }
}

#[derive(Debug)]
pub struct Page {
    pub filter: Filter,
    /// The selected thread, kept as threads move.
    selected: Option<ThreadId>,
}

impl Page {
    pub fn new(filter: Filter) -> Self {
        Self { filter, selected: None }
    }

    /// The threads listed with their states, the most urgent first, then the most recently active.
    pub fn rows(&self, data: &Data) -> Vec<(ThreadId, ThreadState)> {
        let mut rows: Vec<(ThreadId, ThreadState)> = data
            .live
            .order
            .iter()
            .filter(|t| {
                let summary = data.live.summary(t);
                let on = |user: &UserId| summary.is_some_and(|s| s.users.contains(user));
                let posted = |channel: &ChannelId| summary.is_some_and(|s| s.channels.contains(channel));
                self.filter.user.as_ref().is_none_or(on) && self.filter.channel.as_ref().is_none_or(posted)
            })
            .map(|t| (t.clone(), data.state(t)))
            .filter(|(_, s)| self.filter.state.is_none_or(|f| f == *s))
            .collect();
        rows.sort_by_key(|(_, s)| s.rank());
        rows
    }

    /// The selected thread.
    pub fn thread(&self, data: &Data) -> Option<ThreadId> {
        let rows = self.rows(data);
        rows.get(self.at(&rows)).map(|(t, _)| t.clone())
    }

    fn at(&self, rows: &[(ThreadId, ThreadState)]) -> usize {
        self.selected.as_ref().and_then(|s| rows.iter().position(|(t, _)| t == s)).unwrap_or(0)
    }

    pub fn key(&mut self, key: Key, data: &Data) -> Option<To> {
        let rows = self.rows(data);
        let at = self.at(&rows);
        let to = match key {
            Key::Enter => return rows.get(at).map(|(t, _)| To::Thread(t.clone())),
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at + 1,
            Key::PageUp => at.saturating_sub(10),
            Key::PageDown => at + 10,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        };
        self.selected = rows.get(to.min(rows.len().saturating_sub(1))).map(|(t, _)| t.clone());
        None
    }

    pub const KEYS: &str = "↑↓ thread · Enter open · Esc back";

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let rows = self.rows(data);
        let block = frame::block(format!(" {} ({}) ", self.filter.title(), rows.len()), focused);
        if rows.is_empty() {
            let text = if data.live.stored_at.is_some() { "no threads" } else { "reading the stream…" };
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }
        let me = data.me();
        let table: Vec<Row> = rows
            .iter()
            .map(|(thread, state)| {
                let summary = data.live.summary(thread);
                let ants: Vec<&str> =
                    summary.iter().flat_map(|s| &s.users).filter(|u| !data.is_human(u)).map(UserId::as_str).collect();
                let owner =
                    summary.and_then(|s| s.owner.as_ref()).map_or("-", |o| if o == me { "me" } else { o.as_str() });
                let age = summary.map_or_else(String::new, |s| ago(s.last_at, now));
                let doing = data.live.progress_of(thread, 60).map_or_else(String::new, |(line, _)| line);
                Row::new(vec![
                    Cell::from(state.label()).style(Style::new().fg(state.color())),
                    Cell::from(data.title(thread)),
                    Cell::from(ants.join(",")),
                    Cell::from(owner.to_owned()),
                    Cell::from(age),
                    Cell::from(doing),
                ])
            })
            .collect();
        let widths = [
            Constraint::Length(14),
            Constraint::Fill(3),
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Fill(2),
        ];
        let header = Row::new(["STATE", "THREAD", "ANTS", "OWNER", "AGE", "NOW"]).style(frame::dim());
        let table =
            Table::new(table, widths).header(header).block(block).row_highlight_style(frame::highlight(focused));
        let mut state = TableState::default().with_selected(Some(self.at(&rows)));
        frame.render_stateful_widget(table, area, &mut state);
    }
}

/// The line under the title of an open thread: its state, and why.
pub fn why(data: &Data, thread: &ThreadId) -> Line<'static> {
    let state = data.state(thread);
    let why = match state {
        ThreadState::Stuck => data.stuck(thread).map(|a| crate::excerpt(&a.text, 80)),
        ThreadState::WaitingOnMe if data.live.approvals.iter().any(|r| r.thread_id == *thread) => {
            Some("a tool request waits for a decision".to_owned())
        }
        ThreadState::WaitingOnMe => data.unread_on(thread).last().map(|i| format!("{}: {}", i.from, i.text)),
        _ => None,
    };
    let mut text = state.label().to_owned();
    if let Some(why) = why {
        text = format!("{text}: {}", crate::excerpt(&why, 100));
    }
    Line::styled(text, Style::new().fg(state.color()))
}

#[cfg(test)]
mod tests {
    use ants_proto::{Envelope, Escalation, StatusEvent, StatusUpdate, Verdict, subject};

    use super::*;
    use crate::live::tests::{at, message, request, user};
    use crate::thread::Parser;
    use crate::tui::model::tests::model;
    use crate::tui::model::{Model, Page as View};

    fn escalation(seq: u64, thread: &ThreadId, from: &str, to: &str, reason: EscalationReason) -> Entry {
        let x = Escalation::new(user(from), thread.clone(), reason);
        let envelope = Envelope {
            thread_id: thread.clone(),
            escalation: Some(Box::new(x)),
            ..Envelope::new(user(from), user(to), "help")
        };
        let payload = serde_json::to_vec(&envelope).unwrap();
        Parser::default().entry(seq, at(seq.cast_signed()), &subject::inbox(&user(to), &user(from)), &payload).unwrap()
    }

    fn busy(m: &mut Model, thread: &ThreadId) {
        let update = StatusUpdate::State { state: ants_proto::State::Working };
        m.data.live.status(&StatusEvent::new(user("dev-a"), thread.clone(), update), at(0));
    }

    fn state(m: &Model, thread: &ThreadId) -> ThreadState {
        m.data.state(thread)
    }

    #[test]
    fn a_thread_waits_on_an_ant_until_it_answers_and_on_the_human_until_read() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnAnt);
        busy(&mut m, &thread);
        assert_eq!(state(&m, &thread), ThreadState::Working);
        m.data.live.progress.clear();
        m.stored(vec![message(2, &thread, "dev-a", "alice", "deployed")], at(2));
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnMe, "an unread answer");
        m.data.marker.as_mut().unwrap().mark(2);
        assert_eq!(state(&m, &thread), ThreadState::Idle, "nobody owns it, so read means done");
    }

    #[test]
    fn an_owned_thread_an_ant_answered_waits_on_its_owner_even_once_read() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut opening = message(1, &thread, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(2, &thread, "dev-a", "alice", "deployed")], at(2));
        m.data.marker.as_mut().unwrap().mark(2);
        assert!(m.data.finished(&thread));
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnMe);
        m.stored(vec![message(3, &thread, "alice", "dev-a", "and the docs?")], at(3));
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnAnt);
    }

    #[test]
    fn an_escalation_to_a_human_is_stuck_until_a_human_speaks_and_approvals_wait_on_me() {
        let (thread, asked) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        m.stored(vec![escalation(2, &thread, "dev-a", "lead", EscalationReason::TurnBudget)], at(2));
        assert_ne!(state(&m, &thread), ThreadState::Stuck, "an escalation to an ant is the ants' business");
        m.stored(vec![escalation(3, &thread, "lead", "bob", EscalationReason::TurnBudget)], at(3));
        assert_eq!(state(&m, &thread), ThreadState::Stuck);
        assert_eq!(m.data.stuck(&thread).map(|a| a.seq), Some(3));
        m.stored(vec![message(4, &thread, "bob", "dev-a", "more turns")], at(4));
        assert_ne!(state(&m, &thread), ThreadState::Stuck);

        m.stored(vec![escalation(5, &asked, "dev-a", "alice", EscalationReason::Approval)], at(5));
        m.data.marker.as_mut().unwrap().mark(5);
        assert_eq!(state(&m, &asked), ThreadState::Idle, "a tool request counts while pending only");
        let push = ants_proto::ApprovalRequest {
            thread_id: asked.clone(),
            ..request("dev-a", "push", vec![Verdict::AllowOnce])
        };
        m.data.live.approval(&crate::tools::Update::Pending(push));
        assert_eq!(state(&m, &asked), ThreadState::WaitingOnMe);
    }

    #[test]
    fn the_work_view_lists_the_most_urgent_first_and_filters() {
        let (idle, mine, theirs) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.stored(
            vec![
                message(1, &mine, "dev-a", "alice", "done"),
                message(2, &theirs, "bob", "dev-b", "go"),
                message(3, &idle, "dev-b", "bob", "fyi"),
            ],
            at(4),
        );
        let page = Page::new(Filter::default());
        let rows: Vec<_> = page.rows(&m.data).into_iter().map(|(_, s)| s).collect();
        assert_eq!(rows, [ThreadState::WaitingOnMe, ThreadState::WaitingOnAnt, ThreadState::Idle]);
        let page = Page::new(Filter { user: Some(user("dev-b")), ..Filter::default() });
        assert_eq!(page.rows(&m.data).into_iter().map(|(t, _)| t).collect::<Vec<_>>(), [theirs.clone(), idle]);
        let counts = m.data.counts();
        assert_eq!((counts[&ThreadState::WaitingOnMe], counts.get(&ThreadState::Stuck)), (1, None));

        m.select(super::super::nav::Item::Work(Some(ThreadState::WaitingOnAnt)));
        let View::Work(page) = m.page() else { panic!("not the work view") };
        assert_eq!(page.rows(&m.data), [(theirs, ThreadState::WaitingOnAnt)]);
    }
}
