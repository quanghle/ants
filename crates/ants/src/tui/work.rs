//! The work view: every thread read, with the state the messages on it tell, those waiting on the human first.
//! What each thread's messages tell ([`Facts`]) also feeds home and the nav's counts.

use std::collections::HashMap;

use ants_proto::{ChannelId, EscalationReason, Question, ThreadDone, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState, Wrap};

use super::frame::{self, ago};
use super::interrupt::Intent;
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
    /// A human marked it done, and nothing was said on it since.
    Done,
}

impl ThreadState {
    /// The states, as the nav lists them.
    pub const ALL: [Self; 6] =
        [Self::WaitingOnMe, Self::Working, Self::WaitingOnAnt, Self::Stuck, Self::Idle, Self::Done];

    pub fn label(self) -> &'static str {
        match self {
            Self::WaitingOnMe => "waiting on me",
            Self::Stuck => "stuck",
            Self::Working => "working",
            Self::WaitingOnAnt => "waiting on ant",
            Self::Idle => "idle",
            Self::Done => "done",
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
            Self::Done => 5,
        }
    }

    pub fn color(self) -> Color {
        match self {
            Self::WaitingOnMe => Color::Yellow,
            Self::Stuck => Color::Red,
            Self::Working => Color::Green,
            Self::WaitingOnAnt => Color::Cyan,
            Self::Idle => Color::DarkGray,
            Self::Done => Color::Blue,
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

/// A question to the human no answer was read to yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asked {
    /// The id of the question's envelope, which an answer names.
    pub id: Uuid,
    pub at: DateTime<Utc>,
    pub from: UserId,
    pub question: Question,
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
    /// The turns left that its latest message, post or note carries.
    pub turns: Option<u32>,
    /// Who first messaged each user on it, in the order they were first messaged: who delegated to whom.
    pub asked: Vec<(UserId, UserId)>,
    /// The questions to the human no answer was read to yet.
    questions: Vec<Asked>,
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
            fact.turns = entry.turns.or(fact.turns);
        }
        match (entry.kind, &entry.from, &entry.to) {
            (Kind::Message, Some(from), Some(to)) => {
                if !fact.asked.iter().any(|(_, asked)| asked == to) {
                    fact.asked.push((from.clone(), to.clone()));
                }
                fact.received.insert(to.clone(), entry.seq);
                fact.latest = Some(Said { seq: entry.seq, at: entry.stored_at, from: from.clone(), to: to.clone() });
            }
            (Kind::Escalation, _, Some(to)) if entry.reason != Some(EscalationReason::Approval) => {
                fact.escalated.insert(to.clone(), alert());
            }
            (Kind::Dead, _, _) => fact.dead = Some(alert()),
            _ => {}
        }
        if let Some(answer) = &entry.answer {
            fact.questions.retain(|q| q.id != answer.question);
        }
        if to(entry, me)
            && let (Some(question), Some(id), Some(from)) = (&entry.question, entry.id, &entry.from)
        {
            fact.questions.push(Asked { id, at: entry.stored_at, from: from.clone(), question: question.clone() });
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

    /// The thread's shared done state, while nothing said on it since reopened it.
    pub fn done(&self, thread: &ThreadId) -> Option<&ThreadDone> {
        let last = self.live.summary(thread)?.last_reopening;
        self.cards.done.get(thread).filter(|d| d.holds(last))
    }

    /// Whether a question to the human on the thread has no answer yet.
    pub fn asks(&self, thread: &ThreadId) -> bool {
        self.question(thread).is_some()
    }

    /// The latest question to the human on the thread with no answer yet: the one the bar's `/answer` answers.
    pub fn question(&self, thread: &ThreadId) -> Option<&Asked> {
        self.facts.get(thread)?.questions.last()
    }

    /// Whether an ant answered the human on a thread the human owns and nobody marked done, and is done with it: the
    /// human's turn.
    pub fn finished(&self, thread: &ThreadId) -> bool {
        let (Some(summary), Some(fact)) = (self.live.summary(thread), self.facts.get(thread)) else { return false };
        let Some(latest) = &fact.latest else { return false };
        self.done(thread).is_none()
            && summary.owner.as_ref() == Some(self.me())
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
        if self.done(thread).is_some() {
            return ThreadState::Done;
        }
        if self.stuck(thread).is_some() {
            return ThreadState::Stuck;
        }
        if self.live.approvals.iter().any(|r| r.thread_id == *thread)
            || self.asks(thread)
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
            Key::Char('s') => return rows.get(at).map(|(t, _)| To::Interrupt(Intent::Steer, t.clone())),
            Key::Char('S') => return rows.get(at).map(|(t, _)| To::Tldr(t.clone())),
            Key::Char('x') | Key::Ctrl('x') => {
                return rows.get(at).map(|(t, _)| To::Interrupt(Intent::Cancel, t.clone()));
            }
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

    pub const KEYS: &str = "↑↓ thread · Enter open · s steer · x cancel · S TL;DR · Esc back";

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let rows = self.rows(data);
        let block = frame::block(format!(" {} ({}) ", self.filter.title(), rows.len()), focused);
        if rows.is_empty() {
            let text = if data.live.stored_at.is_some() { "no threads" } else { "reading the stream…" };
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }
        let at = self.at(&rows);
        let (list, detail) = if area.height >= DETAIL_HEIGHT * 2 {
            let [list, detail] = Layout::vertical([Constraint::Min(3), Constraint::Length(DETAIL_HEIGHT)]).areas(area);
            (list, Some(detail))
        } else {
            (area, None)
        };
        let table: Vec<Row> = rows.iter().map(|(thread, state)| row(data, thread, *state, now)).collect();
        let widths = [
            Constraint::Length(14),
            Constraint::Fill(3),
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(5),
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Fill(2),
        ];
        let header = Row::new(["STATE", "THREAD", "ANTS", "OWNER", "TURNS", "SPEND", "AGE", "NOW"]).style(frame::dim());
        let table =
            Table::new(table, widths).header(header).block(block).row_highlight_style(frame::highlight(focused));
        let mut state = TableState::default().with_selected(Some(at));
        frame.render_stateful_widget(table, list, &mut state);
        if let (Some(detail), Some((thread, _))) = (detail, rows.get(at)) {
            let block = frame::block(" Selected ".to_owned(), false);
            let width = usize::from(detail.width.saturating_sub(2));
            let paragraph = Paragraph::new(card(data, thread, now, width)).block(block).wrap(Wrap { trim: false });
            frame.render_widget(paragraph, detail);
        }
    }
}

/// How tall the selected thread's card is, borders included.
const DETAIL_HEIGHT: u16 = 7;

/// A cost, such as `$0.42`.
pub fn dollars(cost: f64) -> String {
    format!("${cost:.2}")
}

/// The thread's row in the work view.
fn row(data: &Data, thread: &ThreadId, state: ThreadState, now: DateTime<Utc>) -> Row<'static> {
    let me = data.me();
    let summary = data.live.summary(thread);
    let ants: Vec<&str> =
        summary.iter().flat_map(|s| &s.users).filter(|u| !data.is_human(u)).map(UserId::as_str).collect();
    let owner = summary.and_then(|s| s.owner.as_ref()).map_or("-", |o| if o == me { "me" } else { o.as_str() });
    let turns = data.facts.get(thread).and_then(|f| f.turns).map_or_else(|| "-".to_owned(), |t| t.to_string());
    let spend = data.cards.spent(thread).map_or_else(|| "-".to_owned(), |s| dollars(s.cost));
    let age = summary.map_or_else(String::new, |s| ago(s.last_at, now));
    Row::new(vec![
        Cell::from(state.label()).style(Style::new().fg(state.color())),
        Cell::from(data.title(thread)),
        Cell::from(ants.join(",")),
        Cell::from(owner.to_owned()),
        Cell::from(turns),
        Cell::from(spend),
        Cell::from(age),
        Cell::from(doing(data, thread, 60)),
    ])
}

/// What happens on the thread now, in at most `width` characters: a tool request or a question waiting for the
/// human, else the ants' progress.
pub fn doing(data: &Data, thread: &ThreadId, width: usize) -> String {
    if data.live.approvals.iter().any(|r| r.thread_id == *thread) {
        "approval".to_owned()
    } else if data.asks(thread) {
        "question".to_owned()
    } else {
        data.live.progress_of(thread, width).map_or_else(String::new, |(line, _)| line)
    }
}

/// The selected thread's card, in lines of `width`: its TL;DR, and who delegated to whom on it.
fn card(data: &Data, thread: &ThreadId, now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
    let label = |text: &'static str| Span::styled(text, frame::dim());
    let tldr = match data.cards.tldr.get(thread) {
        Some(t) => {
            let last = data.live.summary(thread).map_or(0, |s| s.last_reopening);
            let stale = if t.is_current(last) { "" } else { ", stale" };
            let about = format!(" ({}, {}{stale})", t.by, ago(t.at, now));
            let text = crate::excerpt(&t.text, (width * 3).saturating_sub(about.len() + 8));
            vec![Span::raw(text), Span::styled(about, frame::dim())]
        }
        None => vec![Span::styled("none kept: S or /tldr asks for one", frame::dim())],
    };
    let chain = chain(data, thread)
        .into_iter()
        .map(|path| {
            path.iter()
                .map(|user| match data.cards.spent_by(thread, user) {
                    Some(s) => format!("{user} {}", dollars(s.cost)),
                    None => user.to_string(),
                })
                .collect::<Vec<_>>()
                .join(" → ")
        })
        .collect::<Vec<_>>()
        .join(" · ");
    vec![Line::from([vec![label("TL;DR  ")], tldr].concat()), Line::from(vec![label("Chain  "), Span::raw(chain)])]
}

/// Who delegated to whom on the thread, as paths from its owner (else whoever spoke first) through each user first
/// messaged by the one before: the first path in full, each later one from where it branches off.
pub fn chain(data: &Data, thread: &ThreadId) -> Vec<Vec<UserId>> {
    let Some(fact) = data.facts.get(thread) else { return Vec::new() };
    let owner = data.live.summary(thread).and_then(|s| s.owner.clone());
    let Some(root) = owner.or_else(|| fact.asked.first().map(|(from, _)| from.clone())) else { return Vec::new() };
    let mut paths = Vec::new();
    let mut seen = vec![root.clone()];
    let mut stack = vec![vec![root]];
    while let Some(path) = stack.pop() {
        let last = path.last().expect("a path");
        let next: Vec<&UserId> =
            fact.asked.iter().filter(|(from, to)| from == last && !seen.contains(to)).map(|(_, to)| to).collect();
        if next.is_empty() {
            if path.len() > 1 {
                paths.push(path);
            }
            continue;
        }
        seen.extend(next.iter().map(|u| (*u).clone()));
        for (i, to) in next.into_iter().enumerate().rev() {
            let from = if i == 0 { path.clone() } else { vec![last.clone()] };
            stack.push([from, vec![to.clone()]].concat());
        }
    }
    paths
}

/// The line under the title of an open thread: its state, and why.
pub fn why(data: &Data, thread: &ThreadId) -> Line<'static> {
    let state = data.state(thread);
    let why = match state {
        ThreadState::Stuck => data.stuck(thread).map(|a| crate::excerpt(&a.text, 80)),
        ThreadState::WaitingOnMe if data.live.approvals.iter().any(|r| r.thread_id == *thread) => {
            Some("a tool request waits for a decision".to_owned())
        }
        ThreadState::WaitingOnMe if data.asks(thread) => Some("a question waits for an answer".to_owned()),
        ThreadState::Done => data.done(thread).map(|d| format!("by {}", d.by)),
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
    fn a_done_thread_stays_done_until_something_is_said_on_it() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut opening = message(1, &thread, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(2, &thread, "dev-a", "alice", "deployed")], at(2));
        m.data.marker.as_mut().unwrap().mark(2);
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnMe);
        m.data.cards.done.insert(thread.clone(), ThreadDone::new(thread.clone(), user("bob"), 2));
        assert_eq!(state(&m, &thread), ThreadState::Done, "done comes first, for every human");
        assert!(!m.data.finished(&thread), "nothing to review once done");
        assert_eq!(why(&m.data, &thread).to_string(), "done: by bob");
        m.stored(vec![message(3, &thread, "dev-a", "alice", "one more thing")], at(3));
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnMe, "reopened");
    }

    #[test]
    fn a_question_to_the_human_waits_on_it_until_answered() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut asked = message(1, &thread, "dev-a", "alice", "Which policy?");
        asked.question = Some(ants_proto::Question::new("Which policy?", &["a".into(), "b".into()], true).unwrap());
        m.stored(vec![asked.clone()], at(1));
        m.data.marker.as_mut().unwrap().mark(1);
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnMe, "read, but not answered");
        assert_eq!(doing(&m.data, &thread, 60), "question");
        let mut answer = message(2, &thread, "alice", "dev-a", "a");
        answer.answer = Some(ants_proto::Answer { question: asked.id.unwrap(), option: Some(1) });
        m.stored(vec![answer], at(2));
        assert_eq!(state(&m, &thread), ThreadState::WaitingOnAnt);
        assert_eq!(doing(&m.data, &thread, 60), "");
    }

    #[test]
    fn the_chain_follows_who_messaged_whom_first() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut opening = message(1, &thread, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(
            vec![
                opening,
                message(2, &thread, "dev-a", "dev-b", "is it read?"),
                message(3, &thread, "dev-b", "dev-a", "no"),
                message(4, &thread, "dev-a", "scout", "audit"),
                message(5, &thread, "scout", "reviewer", "review"),
                message(6, &thread, "dev-a", "alice", "done"),
            ],
            at(6),
        );
        let names = |paths: Vec<Vec<UserId>>| -> Vec<Vec<String>> {
            paths.into_iter().map(|p| p.iter().map(ToString::to_string).collect()).collect()
        };
        assert_eq!(
            names(chain(&m.data, &thread)),
            [vec!["alice", "dev-a", "dev-b"], vec!["dev-a", "scout", "reviewer"]]
        );
    }

    #[test]
    fn the_work_view_shows_turns_spend_and_the_selected_card() {
        use ants_daemon::caps::Spent;
        use ants_proto::ThreadSummary;
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let thread = ThreadId::generate();
        let mut m = model();
        let mut opening = message(1, &thread, "alice", "dev-a", "deploy the site");
        (opening.owner, opening.turns) = (Some(user("alice")), Some(8));
        let mut asked = message(2, &thread, "dev-a", "dev-b", "is it read?");
        asked.turns = Some(5);
        m.stored(vec![opening, asked], at(2));
        let spent = |cost| Spent { tokens: 100, cost };
        m.data.cards.spend.insert(thread.clone(), vec![(user("dev-a"), spent(0.38)), (user("dev-b"), spent(0.04))]);
        let tldr = ThreadSummary {
            thread_id: thread.clone(),
            text: "The migration lacks a default.".to_owned(),
            by: user("messenger"),
            asker: user("alice"),
            at: at(1),
            upto: 1,
            tokens: 10,
        };
        m.data.cards.tldr.insert(thread, tldr);
        let page = Page::new(Filter::default());
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
        terminal.draw(|f| page.draw(f, f.area(), &m.data, at(62), true)).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        let shown = cells.chunks(120).map(<[&str]>::concat).collect::<Vec<_>>().join("\n");
        assert!(shown.contains("TURNS SPEND"), "{shown}");
        assert!(shown.contains("me       5     $0.42   1m ago"), "{shown}");
        assert!(shown.contains("TL;DR  The migration lacks a default. (messenger, 1m ago, stale)"), "{shown}");
        assert!(shown.contains("Chain  alice → dev-a $0.38 → dev-b $0.04"), "{shown}");
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
