//! The ant view: one ant at a glance, as `ants ant show` prints it ([`crate::ant`]). Above, where and how it runs, as
//! its colony entry tells, beside what holds it back, what it does now on each thread (as `ants watch ANT` follows
//! it) and what it spent that UTC day; below, one pane at a time ([`Pane`]): its threads, the messages waiting for it,
//! which [`Io`] asks its daemon for every while (as `ants queue` does), its memories ([`super::memories`]), its MCP
//! servers ([`super::mcp`]), or its settings and the admin orders to it ([`settings`]).

use std::time::Duration;

use ants_daemon::caps::Spent;
use ants_proto::{ColonyEntry, JsDomain, ThreadId, UserId, Uuid};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState, Wrap};

use super::exec::Job;
use super::frame::{self, ago};
use super::mcp;
use super::memories;
use super::model::{Action, Data, Key, To};
use super::select::Selection;
use super::settings;
use super::work::{self, Filter};
use crate::live::maybe;
use crate::queue::{self, Waiting};

/// How often the shown ant's queue is asked for.
const READ_EVERY: Duration = Duration::from_secs(10);

/// What the lower part of the view shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    /// The threads the ant is on, as work lists them.
    Threads,
    /// The messages waiting for the ant.
    Queue,
    /// The ant's core and harness memories, read-only.
    Memories,
    /// The MCP servers of its harness, and their sign-in.
    Mcp,
    /// Its settings, and the admin orders to it.
    Settings,
}

impl Pane {
    pub const ALL: [Self; 5] = [Self::Threads, Self::Queue, Self::Memories, Self::Mcp, Self::Settings];

    fn label(self) -> &'static str {
        match self {
            Self::Threads => "threads",
            Self::Queue => "queue",
            Self::Memories => "memories",
            Self::Mcp => "mcp",
            Self::Settings => "settings",
        }
    }

    /// The pane of `panes` `by` panes on, round.
    fn moved(self, panes: &[Self], by: isize) -> Self {
        let at = panes.iter().position(|p| *p == self).unwrap_or(0);
        let len = panes.len().cast_signed();
        panes[(at.cast_signed() + by).rem_euclid(len).cast_unsigned()]
    }
}

/// The messages waiting for the ant, as last asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Queue {
    Asking,
    /// No daemon serving the ant answered.
    Unanswered,
    Failed(String),
    /// The messages listed, and how many more wait in the stream unlisted.
    Read(Vec<Waiting>, usize),
}

#[derive(Debug)]
pub struct Page {
    pub ant: UserId,
    pub pane: Pane,
    threads: work::Page,
    /// The selected message of the queue, kept as messages come and go.
    message: Selection<Uuid>,
    pub queue: Queue,
    pub memories: memories::Page,
    pub mcp: Box<mcp::Page>,
    pub settings: settings::Pane,
    /// Whether `r` asked to restart the ant's harnesses, which the next key confirms.
    armed: bool,
}

impl Page {
    pub const THREADS_KEYS: &str = "↑↓ thread · Enter open · x cancel · z snooze · S TL;DR · ←→ pane · \
         a ask · w work · r restart · p (un)pause · X retire · Esc back";
    pub const QUEUE_KEYS: &str =
        "↑↓ message · Enter open its thread · ←→ pane · a ask · w work · r restart · p (un)pause · X retire · Esc back";
    pub const ARMED_KEYS: &str = "y restart the ant's harnesses after their prompts · ! restart now, \
         cancelling the prompts · any other key keeps them";

    pub fn new(ant: UserId) -> Self {
        let threads = work::Page::new(Filter { user: Some(ant.clone()), ..Filter::default() });
        let (message, queue, memories) = (Selection::default(), Queue::Asking, memories::Page::new());
        let settings = settings::Pane::default();
        let mcp = Box::new(mcp::Page::new());
        Self { ant, pane: Pane::Threads, threads, message, queue, memories, mcp, settings, armed: false }
    }

    pub fn keys(&self) -> &'static str {
        match self.pane {
            _ if self.armed => Self::ARMED_KEYS,
            Pane::Threads => Self::THREADS_KEYS,
            Pane::Queue => Self::QUEUE_KEYS,
            Pane::Memories => memories::Page::KEYS,
            Pane::Mcp => self.mcp.keys(),
            Pane::Settings => self.settings.keys(),
        }
    }

    /// Take `key` into the change of a setting, or the MCP sign-in address, being written, if one is.
    pub fn write(&mut self, key: Key) -> Option<Option<super::model::Action>> {
        match self.pane {
            Pane::Settings => self.settings.write(key, &self.ant),
            Pane::Mcp => self.mcp.write(key),
            _ => None,
        }
    }

    fn waiting(&self) -> &[Waiting] {
        match &self.queue {
            Queue::Read(waiting, _) => waiting,
            _ => &[],
        }
    }

    fn messages(&self) -> Vec<Uuid> {
        self.waiting().iter().map(|m| m.id).collect()
    }

    /// The thread selected: on the threads pane, the selected one; on the queue's, the selected message's.
    pub fn thread(&self, data: &Data) -> Option<ThreadId> {
        match self.pane {
            Pane::Threads => self.threads.thread(data),
            Pane::Queue => self.waiting().get(self.message.at(&self.messages())).map(|m| m.thread_id.clone()),
            Pane::Memories | Pane::Mcp | Pane::Settings => None,
        }
    }

    pub fn key(&mut self, key: Key, data: &Data) -> Option<To> {
        let armed = std::mem::take(&mut self.armed);
        match key {
            Key::Char('r') => {
                self.armed = true;
                return None;
            }
            Key::Char(c @ ('y' | '!')) if armed => {
                let job = Job::Restart { ant: self.ant.clone(), now: c == '!' };
                return Some(To::Do(Action::Run(job, String::new())));
            }
            Key::Left | Key::Right => {
                self.pane = self.pane.moved(&self.panes(data), if key == Key::Left { -1 } else { 1 });
                match self.pane {
                    Pane::Memories => self.memories.shown(),
                    Pane::Mcp => self.mcp.shown(),
                    _ => {}
                }
                return None;
            }
            Key::Char('w') => return Some(To::Work(Filter { user: Some(self.ant.clone()), ..Filter::default() })),
            Key::Char('a') => return Some(To::Bar(format!("@{} ", self.ant))),
            Key::Char('c') => return Some(To::Credentials),
            Key::Char('X') => return Some(To::Bar(format!("/retire {}", self.ant))),
            Key::Char('p') => {
                let paused = data.live.colony.iter().any(|e| e.ant == self.ant && e.paused.is_some());
                let (ant, verb) = (self.ant.clone(), if paused { "resume" } else { "pause" });
                let job = Job::Pause { ant, paused: !paused };
                return Some(To::Do(Action::Run(job, format!("{verb} {}", self.ant))));
            }
            _ => {}
        }
        match self.pane {
            Pane::Threads => return self.threads.key(key, data),
            Pane::Settings => return self.settings.key(key, &self.ant),
            Pane::Memories => {
                self.memories.key(key);
                return None;
            }
            Pane::Mcp => return self.mcp.key(key).map(To::Do),
            Pane::Queue => {}
        }
        let messages = self.messages();
        let at = self.message.at(&messages);
        let to = match key {
            Key::Enter => return self.waiting().get(at).map(|m| To::Thread(m.thread_id.clone())),
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at + 1,
            Key::PageUp => at.saturating_sub(10),
            Key::PageDown => at + 10,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        };
        self.message.select(&messages, to);
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let entry = data.live.colony.iter().find(|e| e.ant == self.ant);
        let (mark, color) = super::nav::marker(&entry.into_iter().collect::<Vec<_>>(), now);
        let head = data.looks.head(&self.ant);
        let head_width = head.iter().flatten().map(Line::width).max().map_or(0, |w| w + 2);
        let width = (usize::from(area.width).saturating_sub(head_width) / 2).saturating_sub(2);
        let about = about(entry, now, width);
        let status = status(&self.ant, entry, data, now);
        let tall = about.len().max(status.len()).max(head.as_ref().map_or(0, Vec::len));
        let tall = u16::try_from(tall + 2).unwrap_or(u16::MAX);
        let [top, tabs, pane] =
            Layout::vertical([Constraint::Max(tall.min(area.height / 2)), Constraint::Length(1), Constraint::Min(3)])
                .areas(area);
        let head_width = u16::try_from(head_width).unwrap_or(u16::MAX);
        let [face, left, right] =
            Layout::horizontal([Constraint::Length(head_width), Constraint::Fill(1), Constraint::Fill(1)]).areas(top);
        let moved = data.looks.identity(&self.ant).and_then(|i| i.moved.as_ref());
        let title = match head {
            Some(head) => {
                frame.render_widget(Paragraph::new(head).block(frame::block(String::new(), false)), face);
                format!(" {mark} {} · {} ", self.ant, crate::ant::state(entry, moved, now))
            }
            None => format!(
                " {mark} {} {} · {} ",
                data.looks.face(&self.ant).content,
                self.ant,
                crate::ant::state(entry, moved, now)
            ),
        };

        let block = frame::block(title, focused).title_style(Style::new().fg(color));
        frame.render_widget(Paragraph::new(about).block(block).wrap(Wrap { trim: false }), left);
        let block = frame::block(" now ".to_owned(), false);
        frame.render_widget(Paragraph::new(status).block(block).wrap(Wrap { trim: false }), right);
        let threads = self.threads.rows(data).len();
        let mut spans = vec![Span::raw(" ")];
        for pane in self.panes(data) {
            let count = match pane {
                Pane::Threads => threads.to_string(),
                Pane::Queue => self.queued(),
                Pane::Memories => self.memories.count(),
                Pane::Mcp => self.mcp.count(),
                Pane::Settings => self.settings.count(),
            };
            let style = if pane == self.pane { frame::highlight(focused) } else { frame::dim() };
            spans.push(Span::styled(format!(" {} ({count}) ", pane.label()), style));
            spans.push(Span::raw(" "));
        }
        spans.push(Span::styled("←→", frame::dim()));
        frame.render_widget(Paragraph::new(Line::from(spans)), tabs);
        match self.pane {
            Pane::Threads => self.threads.draw(frame, pane, data, now, focused),
            Pane::Queue => self.draw_queue(frame, pane, data, now, focused),
            Pane::Memories => self.memories.draw(frame, pane, &self.ant, focused),
            Pane::Mcp => self.mcp.draw(frame, pane, &self.ant, focused),
            Pane::Settings => self.settings.draw(frame, pane, &self.ant, now, focused),
        }
    }

    /// The panes the view offers: the MCP one only while the ant's colony entry tells that its harness reports its
    /// MCP servers, or while it shows.
    fn panes(&self, data: &Data) -> Vec<Pane> {
        let mcp = self.pane == Pane::Mcp || data.live.colony.iter().any(|e| e.ant == self.ant && e.mcp);
        Pane::ALL.into_iter().filter(|p| *p != Pane::Mcp || mcp).collect()
    }

    /// How many messages wait, as the queue's tab counts them.
    fn queued(&self) -> String {
        match &self.queue {
            Queue::Read(waiting, more) => (waiting.len() + more).to_string(),
            _ => "?".to_owned(),
        }
    }

    fn draw_queue(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let block = frame::block(format!(" waiting for {} ", self.ant), focused);
        let text = match &self.queue {
            Queue::Asking => Some(format!("asking the daemon of {}…", self.ant)),
            Queue::Unanswered => Some(queue::unanswered(&self.ant)),
            Queue::Failed(e) => Some(format!("reading the queue: {e}")),
            Queue::Read(waiting, _) if waiting.is_empty() => Some(format!("no messages waiting for {}", self.ant)),
            Queue::Read(..) => None,
        };
        if let Some(text) = text {
            frame.render_widget(Paragraph::new(text).block(block).wrap(Wrap { trim: false }), area);
            return;
        }
        let waiting = self.waiting();
        let rows: Vec<Row> = waiting
            .iter()
            .map(|m| {
                let from = match &m.channel {
                    Some(channel) => format!("{} in #{channel}", m.from),
                    None => m.from.to_string(),
                };
                Row::new([
                    Cell::from(data.title(&m.thread_id)),
                    Cell::from(from),
                    Cell::from(ago(m.sent_at, now)),
                    Cell::from(queue::state(m)),
                ])
            })
            .collect();
        let widths = [Constraint::Fill(3), Constraint::Fill(1), Constraint::Length(8), Constraint::Fill(1)];
        let header = Row::new(["THREAD", "FROM", "AGE", "STATE"]).style(frame::dim());
        let mut block = block;
        if let Queue::Read(_, more) = &self.queue
            && *more > 0
        {
            block = block.title_bottom(format!(" and {more} more in the stream "));
        }
        let table = Table::new(rows, widths).header(header).block(block).row_highlight_style(frame::highlight(focused));
        let mut state = TableState::default().with_selected(Some(self.message.at(&self.messages())));
        frame.render_stateful_widget(table, area, &mut state);
    }
}

/// Where and how the ant runs, its labels lined up, in lines of at most `width` characters but for the whoami, which
/// wraps.
fn about(entry: Option<&ColonyEntry>, now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
    let Some(entry) = entry else {
        return vec![Line::styled("not in the colony: no daemon serves it now", frame::dim())];
    };
    let rows = crate::ant::about(entry, now);
    let label = rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    let mut lines = Vec::new();
    for (name, value) in rows {
        let text = if name == "whoami" { value } else { crate::excerpt(&value, width.saturating_sub(label + 1)) };
        for (n, part) in text.lines().enumerate() {
            let name = if n == 0 { name } else { "" };
            lines.push(Line::from(vec![
                Span::styled(format!("{name:<label$} "), frame::dim()),
                Span::raw(part.to_owned()),
            ]));
        }
    }
    lines
}

/// What holds the ant back, what it does now on each thread, and what it spent that UTC day.
fn status(ant: &UserId, entry: Option<&ColonyEntry>, data: &Data, now: DateTime<Utc>) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let moved = data.looks.identity(ant).and_then(|i| i.moved.as_ref());
    let holds = crate::ant::holds(ant, entry, moved, now);
    if holds.is_empty() {
        lines.push(Line::styled(format!("nothing holds {ant} back"), frame::dim()));
    }
    lines.extend(holds.into_iter().map(|h| Line::styled(h, Style::new().fg(Color::Yellow))));
    for asked in data.credentials.of(ant.as_str()) {
        lines.push(Line::styled(format!("{}: c answers", asked.describe()), Style::new().fg(Color::Yellow)));
    }
    let mut working: Vec<(&ThreadId, String, DateTime<Utc>)> = data
        .live
        .progress
        .iter()
        .filter_map(|(thread, progress)| Some((thread, progress.status.of(ant)?, progress.at)))
        .collect();
    working.sort_by_key(|w| std::cmp::Reverse(w.2));
    if working.is_empty() {
        lines.push(Line::styled("at work on no thread", frame::dim()));
    }
    for (thread, stand, at) in working {
        lines.push(Line::from(vec![
            Span::styled(format!("{stand} "), Style::new().fg(Color::Green)),
            Span::raw(format!("on {}", data.title(thread))),
            Span::styled(format!("  {}", ago(at, now)), frame::dim()),
        ]));
    }
    let spent =
        data.cards.today.as_ref().map(|today| {
            today.iter().find(|(a, _)| a == ant).map_or(Spent { tokens: 0, cost: 0.0 }, |(_, spent)| *spent)
        });
    let spent = match spent {
        Some(spent) => format!("{} · {} tokens", work::dollars(spent.cost), spent.tokens),
        None => "not read yet".to_owned(),
    };
    lines.push(Line::from(vec![Span::styled("spent today ", frame::dim()), Span::raw(spent)]));
    lines
}

/// What the shown ant's queue reads brought.
pub enum Event {
    Queue(UserId, Queue),
}

/// The reads of the shown ant's queue.
pub struct Io {
    js: jetstream::Context,
    domain: Option<JsDomain>,
    me: UserId,
    watched: Option<UserId>,
    every: tokio::time::Interval,
    read: Option<LocalBoxFuture<'static, Event>>,
}

impl Io {
    pub fn new(js: jetstream::Context, domain: Option<JsDomain>, me: UserId) -> Self {
        let every = tokio::time::interval(READ_EVERY);
        Self { js, domain, me, watched: None, every, read: None }
    }

    /// The next queue read. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        loop {
            tokio::select! {
                _ = self.every.tick() => {
                    if self.read.is_none() {
                        self.start_read();
                    }
                }
                event = maybe(&mut self.read) => {
                    self.read = None;
                    let Event::Queue(ant, _) = &event;
                    if self.watched.as_ref() == Some(ant) {
                        return event;
                    }
                }
            }
        }
    }

    /// Ask for the queue of the ant `shown` now and every while; with None, of none.
    pub fn watch(&mut self, shown: Option<UserId>) {
        if self.watched != shown {
            self.watched = shown;
            self.start_read();
        }
    }

    /// Ask again now.
    pub fn refresh(&mut self) {
        self.start_read();
    }

    fn start_read(&mut self) {
        let Some(ant) = self.watched.clone() else {
            self.read = None;
            return;
        };
        let (js, domain, me) = (self.js.clone(), self.domain.clone(), self.me.clone());
        self.read = Some(
            async move {
                let queue = match queue::read(&js, domain.as_ref(), &ant, &me).await {
                    Ok(Some((waiting, more))) => Queue::Read(waiting, more),
                    Ok(None) => Queue::Unanswered,
                    Err(e) => Queue::Failed(format!("{e:#}")),
                };
                Event::Queue(ant, queue)
            }
            .boxed_local(),
        );
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{
        AntModel, CoreMemories, McpReport, McpServerState, McpSignIn, McpStatus, Memories, MemoryFile, SignInNeeded,
        StatusEvent, StatusUpdate,
    };

    use super::*;
    use crate::test_support::colony::entry;
    use crate::test_support::live::{at, message, user};
    use crate::tui::model::{Action, Model};
    use crate::tui::nav;
    use crate::tui::test_support::model;

    fn screen(m: &Model, now: DateTime<Utc>) -> String {
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 36)).unwrap();
        terminal.draw(|f| crate::tui::frame::draw(f, m, now)).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        cells.chunks(140).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
    }

    fn waiting(thread: &ThreadId, from: &str, ahead: usize) -> Waiting {
        Waiting {
            id: Uuid::now_v7(),
            thread_id: thread.clone(),
            from: user(from),
            channel: None,
            sent_at: at(1),
            seq: 7,
            running: false,
            ahead,
            delivered: true,
            redelivery: false,
            retry: false,
            held: None,
            started: false,
            withdrawn_by: None,
            withdrawn_for: None,
        }
    }

    #[test]
    fn the_ant_view_shows_the_colony_entry_holds_activity_spend_and_threads() {
        let (busy, other) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut dev = entry("dev-a", 3600, 2, at(2));
        dev.model = AntModel::of(Some("gpt-5".to_owned()), Some("high".to_owned()), None);
        dev.signed_out = Some(SignInNeeded { since: at(1), sign_in: Some("copilot login".to_owned()) });
        m.data.live.colony(vec![dev], at(2));
        m.stored(
            vec![message(1, &busy, "alice", "dev-a", "deploy the site"), message(2, &other, "alice", "dev-b", "other")],
            at(2),
        );
        let tool = StatusUpdate::Tool {
            id: "t1".to_owned(),
            title: Some("cargo test".to_owned()),
            tool_kind: None,
            status: None,
            paths: None,
        };
        m.take(crate::live::Event::Status(StatusEvent::new(user("dev-a"), busy.clone(), tool)), at(2));
        m.data.cards.today = Some(vec![(user("dev-a"), Spent { tokens: 1234, cost: 0.42 })]);
        m.select(nav::Item::Ant(user("dev-a")));
        assert_eq!(m.page().name(), "ant");
        let shown = screen(&m, at(3));
        assert!(shown.contains("{.~.} dev-a · live, signed-out"), "without colour, the face: {shown}");
        assert!(shown.contains("machine      m1") && shown.contains("gpt-5, effort high"), "{shown}");
        assert!(shown.contains("capabilities rust, review"), "{shown}");
        assert!(shown.contains("harness copilot signed out for 2s"), "{shown}");
        assert!(shown.contains("running cargo test on deploy the site"), "{shown}");
        assert!(shown.contains("spent today $0.42 · 1234 tokens"), "{shown}");
        assert!(shown.contains("threads (1)") && shown.contains("queue (?)"), "{shown}");
        assert!(shown.contains(Page::THREADS_KEYS), "{shown}");
        assert!(!shown.contains("other"), "only the ant's threads: {shown}");

        m.focus = crate::tui::model::Focus::View;
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Open(busy)));
        m.key(Key::Esc, at(3));
        assert_eq!(m.page().name(), "ant");
        m.key(Key::Char('w'), at(3));
        assert_eq!(m.page().name(), "work");
        m.key(Key::Esc, at(3));
        m.key(Key::Char('a'), at(3));
        assert!(m.bar.focused && m.bar.text() == "@dev-a ", "a opens the bar on the ant");
        m.key(Key::Esc, at(3));
        m.focus = crate::tui::model::Focus::View;
        assert_eq!(m.key(Key::Char('r'), at(3)), None);
        assert!(screen(&m, at(3)).contains(Page::ARMED_KEYS), "r asks first");
        assert_eq!(m.key(Key::Char('y'), at(3)), Some(Action::Run(restart(false), String::new())));
        m.key(Key::Char('r'), at(3));
        assert_eq!(m.key(Key::Char('!'), at(3)), Some(Action::Run(restart(true), String::new())));
        m.key(Key::Char('r'), at(3));
        assert_eq!(m.key(Key::Char('n'), at(3)), None);
        assert_eq!(m.key(Key::Char('y'), at(3)), None, "any other key keeps the harnesses");
    }

    fn restart(now: bool) -> Job {
        Job::Restart { ant: user("dev-a"), now }
    }

    #[test]
    fn p_pauses_the_ant_or_resumes_it_when_paused() {
        let mut m = model();
        let mut dev = entry("dev-a", 60, 2, at(2));
        m.data.live.colony(vec![dev.clone()], at(2));
        m.select(nav::Item::Ant(user("dev-a")));
        m.focus = crate::tui::model::Focus::View;
        let pause = |paused| Some(Action::Run(Job::Pause { ant: user("dev-a"), paused }, String::new()));
        let sent = |action: Option<Action>| match action {
            Some(Action::Run(job, _)) => Some(Action::Run(job, String::new())),
            other => other,
        };
        assert_eq!(sent(m.key(Key::Char('p'), at(3))), pause(true));
        dev.paused = Some(ants_proto::Paused { since: at(3), by: None });
        m.data.live.colony(vec![dev], at(3));
        assert_eq!(sent(m.key(Key::Char('p'), at(4))), pause(false));
        assert!(screen(&m, at(4)).contains("paused"), "{}", screen(&m, at(4)));
    }

    #[test]
    fn the_queue_pane_lists_what_the_daemon_holds_and_opens_its_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.data.live.colony(vec![entry("dev-a", 60, 2, at(2))], at(2));
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(2));
        m.select(nav::Item::Ant(user("dev-a")));
        m.key(Key::Right, at(3));
        assert!(screen(&m, at(3)).contains("asking the daemon of dev-a…"));
        assert_eq!(m.ant_shown(), Some(user("dev-a")));
        m.ant_event(Event::Queue(user("dev-b"), Queue::Unanswered));
        m.ant_event(Event::Queue(user("dev-a"), Queue::Unanswered));
        assert!(screen(&m, at(3)).contains("no daemon serving dev-a answered within 5s"));
        let queued = vec![waiting(&thread, "alice", 0), waiting(&thread, "bob", 1)];
        m.ant_event(Event::Queue(user("dev-a"), Queue::Read(queued, 3)));
        let shown = screen(&m, at(3));
        assert!(shown.contains("queue (5)") && shown.contains("and 3 more in the stream"), "{shown}");
        assert!(shown.contains("next") && shown.contains("1 ahead"), "{shown}");
        assert!(shown.contains(Page::QUEUE_KEYS), "{shown}");
        m.key(Key::Down, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Open(thread)));
    }

    #[test]
    fn the_memories_pane_reads_them_once_shown_and_again_on_u() {
        let mut m = model();
        m.data.live.colony(vec![entry("dev-a", 60, 2, at(2))], at(2));
        m.select(nav::Item::Ant(user("dev-a")));
        assert_eq!(m.memories_wanted(), None, "not before the pane shows");
        m.key(Key::Left, at(3));
        assert_eq!(m.memories_wanted(), None, "not on the settings pane");
        m.key(Key::Left, at(3));
        assert!(screen(&m, at(3)).contains("asking the daemon of dev-a for its memories…"));
        assert_eq!(m.memories_wanted(), Some((user("dev-a"), 1)));
        let memories = Memories {
            core: CoreMemories { whoami: Some("A tester.".to_owned()), ..CoreMemories::default() },
            harness: Some("codex".to_owned()),
            workdir: Some("/w".to_owned()),
            files: vec![MemoryFile {
                path: "/w/AGENTS.md".to_owned(),
                size: 9,
                text: "Run it.\n".to_owned(),
                truncated: false,
            }],
            ..Memories::default()
        };
        m.memories_event(memories::Event(user("dev-a"), 1, Ok(memories)));
        let shown = screen(&m, at(3));
        assert!(shown.contains("memories (1)") && shown.contains("memories of dev-a · read-only"), "{shown}");
        assert!(shown.contains("A tester.") && shown.contains("── AGENTS.md (9 B)"), "{shown}");
        assert!(shown.contains(memories::Page::KEYS), "{shown}");

        m.focus = crate::tui::model::Focus::View;
        m.key(Key::Char('u'), at(3));
        assert_eq!(m.memories_wanted(), Some((user("dev-a"), 2)));
        m.memories_event(memories::Event(user("dev-a"), 1, Err("late".to_owned())));
        assert!(screen(&m, at(3)).contains("asking the daemon"), "an earlier asking's answer is dropped");
        m.memories_event(memories::Event(user("dev-a"), 2, Err("no admin role".to_owned())));
        assert!(screen(&m, at(3)).contains("reading the memories: no admin role"));
    }

    #[test]
    fn the_mcp_pane_reads_the_servers_and_signs_in_to_the_selected_one() {
        let mut m = model();
        m.data.live.colony(vec![entry("dev-a", 60, 2, at(2))], at(2));
        m.select(nav::Item::Ant(user("dev-a")));
        m.key(Key::Left, at(3));
        assert!(screen(&m, at(3)).contains("settings ("), "an ACP harness reports no MCP servers: no mcp tab");
        assert!(!screen(&m, at(3)).contains("mcp ("));
        m.key(Key::Left, at(3));
        assert!(screen(&m, at(3)).contains("memories of dev-a"));
        m.key(Key::Right, at(3));

        let acp = entry("dev-a", 60, 2, at(2));
        m.data.live.colony(vec![ColonyEntry { mcp: true, ..acp }], at(2));
        m.key(Key::Left, at(3));
        let wanted = m.mcp_wanted().unwrap();
        assert_eq!((wanted.ant, wanted.asked, wanted.sign), (user("dev-a"), 1, None));
        assert!(screen(&m, at(3)).contains("asking the daemon of dev-a for its MCP servers…"));
        let servers = vec![
            McpServerState::new("ants", McpStatus::Connected, None, Some("session")).with_tools(Some(12)),
            McpServerState::new("github", McpStatus::NeedsAuth, Some("unauthorized"), None),
        ];
        let report = McpReport {
            harness: "copilot".to_owned(),
            protocol: "copilot-sdk".to_owned(),
            servers: Some(servers),
            truncated: false,
            read_at: at(3),
        };
        m.mcp_event(mcp::Event::Read(user("dev-a"), 1, Ok(report)));
        let shown = screen(&m, at(3));
        assert!(shown.contains("mcp (2)") && shown.contains("12 tools") && shown.contains("needs_auth"), "{shown}");

        m.focus = crate::tui::model::Focus::View;
        m.key(Key::Down, at(3));
        m.key(Key::Char('s'), at(3));
        assert_eq!(m.mcp_wanted().unwrap().sign, Some((1, "github".to_owned(), None)));
        assert!(screen(&m, at(3)).contains("asking the harness to sign in…"));
        let url = "https://github.com/login/oauth/authorize?client_id=x".to_owned();
        let github = McpServerState::new("github", McpStatus::NeedsAuth, None, None);
        let signing = McpSignIn { server: github, authorization_url: Some(url.clone()), expires_at: Some(at(13)) };
        m.mcp_event(mcp::Event::Signed(user("dev-a"), 1, Ok(signing)));
        let shown = screen(&m, at(3));
        assert!(shown.contains(&url) && shown.contains("y copies it"), "{shown}");
        assert_eq!(m.key(Key::Char('y'), at(3)), Some(Action::Copy(url)));

        m.key(Key::Char('v'), at(3));
        let callback = "http://127.0.0.1:4100/callback?code=abc&state=r";
        for c in callback.chars() {
            m.key(Key::Char(c), at(3));
        }
        assert!(screen(&m, at(3)).contains(&format!("address: {callback}")));
        m.key(Key::Enter, at(3));
        assert_eq!(m.mcp_wanted().unwrap().sign, Some((2, "github".to_owned(), Some(callback.to_owned()))));
        m.mcp_event(mcp::Event::Signed(user("dev-a"), 1, Err("late".to_owned())));
        assert!(screen(&m, at(3)).contains("handing the address over…"), "an earlier sign-in's answer is dropped");
        let github = McpServerState::new("github", McpStatus::Connected, None, None).with_tools(Some(40));
        let signed = McpSignIn { server: github, authorization_url: None, expires_at: None };
        m.mcp_event(mcp::Event::Signed(user("dev-a"), 2, Ok(signed)));
        let shown = screen(&m, at(3));
        assert!(shown.contains("now connected") && shown.contains("40 tools"), "{shown}");
        assert_eq!(m.mcp_wanted().unwrap().sign, None);
    }
}
