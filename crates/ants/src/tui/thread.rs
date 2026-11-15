//! An open thread: its title, channels, state and the ants' progress on it, then its conversation as a timeline, the
//! latest at the bottom, beside a side panel with what else is known of it (its work card).
//!
//! The timeline interleaves, in the order stored, the thread's messages, posts, notes, escalations and tool requests
//! with the tool calls of its ants: those running now as their status updates tell ([`timeline::Calls::status`]), and
//! those stored on their timelines ([`timeline::Calls::stored`]), each placed before the first message stored after
//! it started. Messages between a human and an ant, and posts, are the top level; a message from one ant to another
//! nests a level under the sender, and the receiver's calls and reply nest under that message, unless the receiver
//! nests the sender already (a reply). Messages, posts and notes render as markdown. Opened from a search, the view
//! selects the message found and holds it at its top until moved. The thread's tool requests show as cards under the
//! conversation ([`approvals`]), a, A or r answering the oldest pending one and Enter showing it in full. Each message
//! shows the reactions to it under it, live, and `+` reacts to the one selected ([`react`]).

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Write as _;

use ants_proto::{ThreadId, UserId, Uuid, Verdict};
use chrono::{DateTime, Local, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::acts::{About, Change};
use super::frame::{self, ago};
use super::interrupt::Intent;
use super::model::{Action, Data, Key, Model, To};
use super::timeline::Call;
use super::{approvals, markdown, react, work};
use crate::live::History;
use crate::sealed;
use crate::thread::{Entry, Kind};

/// How deep delegations nest, deeper ones shown at this depth.
const MAX_DEPTH: usize = 4;
/// How wide the side panel is, shown only when the view is at least [`PANEL_FROM`] wide.
const PANEL_WIDTH: u16 = 34;
const PANEL_FROM: u16 = 96;
/// How many messages PgUp and PgDn move the selection.
const PAGE: isize = 10;
const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

#[derive(Debug)]
pub struct Page {
    pub history: History,
    /// The selected message's stream sequence; None follows the latest.
    selected: Option<u64>,
    /// The first line of the conversation drawn last, so that the selection moves within it.
    top: Cell<usize>,
    /// What the side panel shows of the thread besides its messages, once read.
    pub about: Option<About>,
    /// The message the thread was opened at, selected and held at the top of the view until a key moves it.
    at: Option<Uuid>,
}

impl Page {
    pub fn new(thread: ThreadId) -> Self {
        Self { history: History::new(thread), selected: None, top: Cell::new(0), about: None, at: None }
    }

    /// The thread, shown at `message` once read.
    pub fn at(thread: ThreadId, message: Uuid) -> Self {
        Self { at: Some(message), ..Self::new(thread) }
    }

    /// The sequence of the message the thread was opened at, once read.
    fn held(&self) -> Option<u64> {
        let at = self.at?;
        self.messages().find(|e| e.id == Some(at)).map(|e| e.seq)
    }

    pub const KEYS: &str = "↑↓ select · y copy · + react · p pin · m mute · e archive · d done · t tree · s steer · x cancel · a A r answer a tool request · End latest · Esc back";

    /// The messages shown, in the order stored.
    fn messages(&self) -> impl DoubleEndedIterator<Item = &Entry> {
        self.history.entries.iter().filter(|e| e.kind != Kind::Tool)
    }

    /// The ids of the messages shown, for the reactions to them.
    pub fn message_ids(&self) -> Vec<Uuid> {
        self.messages().filter_map(|e| e.id).collect()
    }

    /// The selected message, else the latest.
    fn selection(&self) -> Option<&Entry> {
        let mut messages = self.messages();
        match self.selected.or_else(|| self.held()) {
            Some(seq) => messages.find(|e| e.seq == seq),
            None => messages.last(),
        }
    }

    /// Move the selection `by` messages, down for a positive one; past the latest it follows the latest again.
    fn step(&mut self, by: isize) {
        let seqs: Vec<u64> = self.messages().map(|e| e.seq).collect();
        let at = self.selected.and_then(|s| seqs.iter().position(|seq| *seq == s)).unwrap_or(seqs.len());
        self.selected = seqs.get(at.saturating_add_signed(by).min(seqs.len())).copied();
    }

    /// The sequence of the thread's last message, post or note read; 0 for none.
    fn last_reopening(&self, data: &Data) -> u64 {
        let read = self.messages().filter(|e| e.kind.reopens()).map(|e| e.seq).max().unwrap_or(0);
        data.live.summary(&self.history.thread).map_or(read, |s| s.last_reopening.max(read))
    }

    /// How the human holds the thread at `now`.
    fn flags(&self, data: &Data, now: DateTime<Utc>) -> Flags {
        let thread = &self.history.thread;
        let marks = &data.live.marks;
        let idle_before = crate::thread::idle_before(data.live.auto_archive, now);
        let idle = data
            .live
            .summary(thread)
            .is_some_and(|s| idle_before.is_some_and(|before| marks.idle(thread, s.last_at, before)));
        let about = self.about.as_ref();
        Flags {
            pinned: marks.pinned.contains_key(thread),
            muted: about.and_then(|a| a.muted).is_some_and(|until| now < until),
            archived: marks.archived.contains_key(thread) || idle,
            done: about.and_then(|a| a.done.as_ref()).is_some_and(|d| d.holds(self.last_reopening(data))),
        }
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let (thread, last) = (self.history.thread.clone(), self.last_reopening(data));
        let change = |change| Some(To::Do(Action::Change(thread, change, last)));
        let flags = self.flags(data, now);
        if let Some(held) = self.held() {
            self.selected.get_or_insert(held);
        }
        if matches!(
            key,
            Key::Up | Key::Down | Key::Char('k' | 'j') | Key::PageUp | Key::PageDown | Key::Home | Key::End
        ) {
            self.at = None;
        }
        match key {
            Key::Up | Key::Char('k') => self.step(-1),
            Key::Down | Key::Char('j') => self.step(1),
            Key::PageUp => self.step(-PAGE),
            Key::PageDown => self.step(PAGE),
            Key::Home => {
                let first = self.messages().next().map(|e| e.seq);
                self.selected = first;
            }
            Key::End => self.selected = None,
            Key::Char('y') => return self.selection().map(|e| To::Do(Action::Copy(e.text.clone()))),
            Key::Char('+') => return self.selection().and_then(|e| e.id).map(To::React),
            Key::Char('p') => return change(Change::Pin(!flags.pinned)),
            Key::Char('m') => return change(Change::Mute(!flags.muted)),
            Key::Char('e') => return change(Change::Archive(!flags.archived)),
            Key::Char('d') => return change(Change::Done(!flags.done)),
            Key::Char('a' | 'A' | 'r') | Key::Enter => {
                let verdict = match key {
                    Key::Char('a') => Some(Verdict::AllowOnce),
                    Key::Char('A') => Some(Verdict::AllowAlways),
                    Key::Char('r') => Some(Verdict::Reject),
                    _ => None,
                };
                let pending = data.live.approvals.iter().filter(|r| r.thread_id == self.history.thread);
                let oldest = pending.min_by_key(|r| r.requested_at)?;
                return Some(To::Ask(oldest.clone(), verdict));
            }
            Key::Char('t') => return Some(To::Tree(self.history.thread.clone())),
            Key::Char('s') => return Some(To::Interrupt(Intent::Steer, self.history.thread.clone())),
            Key::Char('x') | Key::Ctrl('x') => return Some(To::Interrupt(Intent::Cancel, self.history.thread.clone())),
            _ => {}
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let data = &model.data;
        let thread = &self.history.thread;
        let summary = data.live.summary(thread);
        let mut title = format!(" {} ", data.title(thread));
        if let Some(s) = summary.filter(|s| !s.channels.is_empty()) {
            let channels: Vec<String> = s.channels.iter().map(|c| format!("#{c}")).collect();
            title = format!("{title}· {} ", channels.join(" "));
        }
        let (area, panel) = if area.width >= PANEL_FROM {
            let [left, right] = Layout::horizontal([Constraint::Min(20), Constraint::Length(PANEL_WIDTH)]).areas(area);
            (left, Some(right))
        } else {
            (area, None)
        };
        let area = draw_cards(frame, area, model, thread, now);
        let width = usize::from(area.width.saturating_sub(2));
        let mut lines = vec![work::why(data, thread)];
        if let Some((line, at)) = data.live.progress_of(thread, width.saturating_sub(16)) {
            lines.push(Line::styled(format!("▶ {line} ({})", ago(at, now)), Style::new().fg(Color::Green)));
        }
        if self.history.loading {
            lines.push(Line::styled("reading the thread…", frame::dim()));
        }
        let shown = Shown { data, now, busy: data.live.progress.contains_key(thread), width };
        let calls = data.calls.get(thread).map_or(&[][..], |c| c.calls());
        let rows = rows(&self.history.entries, calls, |u| data.is_human(u));
        let held = self.held();
        let selected = self.selected.or(held).and_then(|_| self.selection()).map(|e| e.seq);
        let mut range = None;
        for (at, (row, (first, rest))) in rows.iter().zip(glyphs(&rows)).enumerate() {
            if at > 0 && row.depth == 0 {
                lines.push(Line::raw(""));
            }
            let start = lines.len();
            shown.row(&mut lines, row, &self.history.entries, &first, &rest);
            if let Item::Message(e) = row.item
                && Some(e.seq) == selected
            {
                for line in &mut lines[start..] {
                    line.style = line.style.patch(frame::highlight(focused));
                }
                range = Some((start, lines.len()));
            }
        }
        let height = usize::from(area.height.saturating_sub(2));
        let bottom = lines.len().saturating_sub(height);
        let top = match range {
            None => bottom,
            Some((start, _)) if held.is_some() => start.min(bottom),
            Some((start, end)) => {
                let top = self.top.get().min(bottom);
                if start < top {
                    start
                } else if end > top + height {
                    end.saturating_sub(height).min(start)
                } else {
                    top
                }
            }
        };
        self.top.set(top);
        let paragraph = Paragraph::new(lines)
            .block(frame::block(title, focused))
            .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
        if let Some(panel) = panel {
            let lines = self.panel(data, calls, now, usize::from(PANEL_WIDTH.saturating_sub(4)));
            frame.render_widget(Paragraph::new(lines).block(frame::block(String::new(), false)), panel);
        }
    }

    /// The side panel: the thread's state, owner, ants and what each does, turns left, spend, age, origin, how the
    /// human holds it and the paths its tool calls touched.
    fn panel(&self, data: &Data, calls: &[Call], now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
        let thread = &self.history.thread;
        let summary = data.live.summary(thread);
        let label = |name: &str| Span::styled(format!("{name:<7}"), frame::dim());
        let field =
            |name: &str, value: String| Line::from(vec![label(name), Span::raw(cut(&value, width.saturating_sub(7)))]);
        let state = data.state(thread);
        let mut lines =
            vec![Line::from(vec![label("STATE"), Span::styled(state.label(), Style::new().fg(state.color()))])];
        let owner = summary.and_then(|s| s.owner.clone()).or_else(|| self.messages().find_map(|e| e.owner.clone()));
        lines.push(field("OWNER", owner.map_or_else(|| "none".to_owned(), |o| o.to_string())));
        let mut ants: Vec<&UserId> = summary.into_iter().flat_map(|s| &s.users).collect();
        ants.extend(self.messages().flat_map(|e| e.from.iter().chain(&e.to)));
        ants.extend(calls.iter().map(|c| &c.ant));
        ants.retain(|u| !data.is_human(u));
        ants.sort();
        ants.dedup();
        lines.push(Line::from(label("ANTS")));
        if ants.is_empty() {
            lines.push(Line::styled("  none", frame::dim()));
        }
        let progress = data.live.progress.get(thread);
        for ant in ants {
            match progress.and_then(|p| p.status.of(ant)) {
                Some(stand) => lines.push(Line::from(vec![
                    Span::raw(format!("  {ant} ")),
                    Span::styled(cut(&format!("◐ {stand}"), width.saturating_sub(ant.as_str().len() + 3)), busy()),
                ])),
                None => lines.push(Line::raw(format!("  {ant}"))),
            }
        }
        if let Some(turns) = self.messages().rev().find_map(|e| e.turns) {
            lines.push(field("TURNS", format!("{turns} left")));
        }
        if let Some(spend) = self.about.as_ref().and_then(|a| a.spend.as_ref()) {
            let (cost, tokens) = spend.iter().fold((0.0, 0), |(c, t), r| (c + r.spent.cost, t + r.spent.tokens));
            lines.push(field("SPEND", format!("${cost:.2} · {} tokens", count(tokens))));
        }
        let first = self.messages().next();
        if let Some(at) = summary.map(|s| s.first_at).or(first.map(|e| e.stored_at)) {
            lines.push(field("AGE", crate::roster::short(crate::roster::since(at, now))));
        }
        if let Some(first) = first {
            lines.push(field("FROM", format!("{} {}", origin(first, data), hm(first.stored_at))));
        }
        let flags = self.flags(data, now);
        let held: Vec<&str> =
            [(flags.pinned, "pinned"), (flags.muted, "muted"), (flags.archived, "archived"), (flags.done, "done")]
                .into_iter()
                .filter_map(|(on, name)| on.then_some(name))
                .collect();
        if !held.is_empty() {
            lines.push(field("HELD", held.join(" · ")));
        }
        let paths = data.calls.get(thread).map_or_default(super::timeline::Calls::paths);
        if !paths.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::from(label("FILES")));
            lines.extend(paths.into_iter().map(|p| Line::raw(format!("  {}", cut_start(p, width.saturating_sub(2))))));
        }
        lines
    }
}

/// Draw the cards of the tool requests of `thread` at the bottom of `area`, in at most half of it: the area left.
fn draw_cards(frame: &mut Frame, area: Rect, model: &Model, thread: &ThreadId, now: DateTime<Utc>) -> Rect {
    let width = usize::from(area.width.saturating_sub(2));
    let cards = approvals::cards(&model.data, Some(thread), now);
    let mut boxes = Vec::new();
    let mut height = 0;
    for (at, card) in cards.into_iter().enumerate() {
        let lines = approvals::lines(card, model.asking.as_ref(), at == 0, now, width);
        let tall = u16::try_from(lines.len()).unwrap_or(u16::MAX).saturating_add(2);
        if height + tall > area.height / 2 {
            break;
        }
        height += tall;
        boxes.push((card, lines, tall));
    }
    if boxes.is_empty() {
        return area;
    }
    let [rest, mut below] = Layout::vertical([Constraint::Min(3), Constraint::Length(height)]).areas(area);
    for (card, lines, tall) in boxes {
        let [this, next] = Layout::vertical([Constraint::Length(tall), Constraint::Min(0)]).areas(below);
        let style = if card.settled.is_some() { frame::dim() } else { Style::new().fg(Color::Yellow) };
        let block = ratatui::widgets::Block::bordered().title(approvals::title(card)).border_style(style);
        frame.render_widget(Paragraph::new(lines).block(block), this);
        below = next;
    }
    rest
}

/// How the human holds a thread.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Flags {
    pinned: bool,
    muted: bool,
    archived: bool,
    done: bool,
}

/// What a row of the conversation is.
#[derive(Debug, Clone, Copy)]
pub enum Item<'a> {
    Message(&'a Entry),
    Call(&'a Call),
}

/// A row of the conversation, and how deep it nests: 0 for the top level.
#[derive(Debug)]
pub struct Row<'a> {
    pub item: Item<'a>,
    pub depth: usize,
}

/// The conversation of `entries`, in the order stored, with `calls`, by when they started, each before the first
/// message stored after it started; `human` tells the humans.
pub fn rows<'a>(entries: &'a [Entry], calls: &'a [Call], human: impl Fn(&UserId) -> bool) -> Vec<Row<'a>> {
    let mut levels: HashMap<&UserId, usize> = HashMap::new();
    let level = |levels: &HashMap<&UserId, usize>, user: &UserId| levels.get(user).copied().unwrap_or(1);
    let mut calls = calls.iter().peekable();
    let mut rows = Vec::new();
    for entry in entries.iter().filter(|e| e.kind != Kind::Tool) {
        while let Some(call) = calls.next_if(|c| c.started < entry.stored_at) {
            rows.push(Row { item: Item::Call(call), depth: level(&levels, &call.ant) });
        }
        let ant = |user: &&UserId| !human(user);
        let depth = match (entry.kind, entry.from.as_ref(), entry.to.as_ref()) {
            (Kind::Message | Kind::Escalation, Some(from), Some(to)) if ant(&from) && ant(&to) => {
                let depth = level(&levels, from);
                // A reply to an ant nesting it leaves that ant where it was.
                if levels.get(to).is_none_or(|l| *l > depth) {
                    levels.insert(to, (depth + 1).min(MAX_DEPTH));
                }
                depth
            }
            (Kind::Note | Kind::Approval, Some(from), _) if ant(&from) => level(&levels, from),
            (Kind::Decision | Kind::Dead, _, Some(to)) if ant(&to) => level(&levels, to),
            _ => {
                for user in entry.from.iter().chain(&entry.to).filter(ant) {
                    levels.insert(user, 1);
                }
                0
            }
        };
        rows.push(Row { item: Item::Message(entry), depth });
    }
    rows.extend(calls.map(|call| Row { item: Item::Call(call), depth: level(&levels, &call.ant) }));
    rows
}

/// The tree drawn before each of `rows`: before its first line, and before the others.
fn glyphs(rows: &[Row]) -> Vec<(String, String)> {
    // Whether a row at each depth comes later, before any shallower one.
    let mut open = [false; MAX_DEPTH + 1];
    let mut out = vec![(String::new(), String::new()); rows.len()];
    for (at, row) in rows.iter().enumerate().rev() {
        let depth = row.depth.min(MAX_DEPTH);
        if depth > 0 {
            let (mut first, mut rest) = ("  ".to_owned(), "  ".to_owned());
            for on in &open[1..depth] {
                let bar = if *on { "│ " } else { "  " };
                first.push_str(bar);
                rest.push_str(bar);
            }
            first.push_str(if open[depth] { "├ " } else { "└ " });
            rest.push_str(if open[depth] { "│ " } else { "  " });
            out[at] = (first, rest);
        }
        open[depth] = true;
        open[depth + 1..].fill(false);
    }
    out
}

/// What the rows are drawn with.
struct Shown<'a> {
    data: &'a Data,
    now: DateTime<Utc>,
    /// Whether an ant is busy on the thread: a call not ended is running only then.
    busy: bool,
    width: usize,
}

impl Shown<'_> {
    /// Add the lines of `row`, which `first` and `rest` begin; `entries` are the thread's, for a question's answers.
    fn row(&self, lines: &mut Vec<Line<'static>>, row: &Row, entries: &[Entry], first: &str, rest: &str) {
        match row.item {
            Item::Call(call) => self.call(lines, call, first),
            Item::Message(e) if row.depth == 0 => self.block(lines, e, entries),
            Item::Message(e) => {
                let (head, style) = nested(e);
                let head = format!("{first}{} {head}", hm(e.stored_at));
                let under = format!("{rest}      ");
                let mut text = text(e, entries, self.width.saturating_sub(under.chars().count()));
                let room = self.width.saturating_sub(head.chars().count() + 2);
                let mut first = Line::styled(head, style);
                // A text of one line short enough goes on the line of the heading.
                if let [line] = &mut text[..]
                    && line.width() <= room
                {
                    first.spans.push(Span::raw("  "));
                    first.spans.append(&mut line.spans);
                    text.clear();
                }
                lines.push(first);
                lines.extend(text.into_iter().map(|line| indented(&under, line).patch_style(style)));
                lines.extend(self.reactions(e).map(|line| indented(&under, line)));
            }
        }
    }

    /// The line of the reactions to `e`, if any.
    fn reactions(&self, e: &Entry) -> Option<Line<'static>> {
        let reacted = self.data.reactions.get(&e.id?)?;
        react::line(reacted, self.data.me())
    }

    /// A message at the top level: who sent it to whom and when, then its text under it.
    fn block(&self, lines: &mut Vec<Line<'static>>, e: &Entry, entries: &[Entry]) {
        let from = e.from.as_ref().map_or("a human", UserId::as_str);
        let to = match (e.kind, &e.channel, &e.to) {
            (Kind::Post, Some(channel), _) => format!(" → #{channel}"),
            (Kind::Note, ..) => " → notes".to_owned(),
            (Kind::Approval, ..) => " → humans".to_owned(),
            (_, _, Some(to)) => format!(" → {to}"),
            _ => String::new(),
        };
        let from_style = if self.data.is_human(e.from.as_ref().unwrap_or(&UserId::human())) {
            Style::new().fg(Color::Cyan).bold()
        } else {
            Style::new().fg(Color::Magenta).bold()
        };
        let mut head = vec![
            Span::styled(format!("{from}{to}"), from_style),
            Span::styled(format!("  {}", hm(e.stored_at)), frame::dim()),
        ];
        let (what, style) = match e.kind {
            Kind::Escalation => {
                (format!("escalation: {}", e.note.as_deref().unwrap_or("")), Style::new().fg(Color::Red))
            }
            Kind::Mention => ("mention".to_owned(), frame::dim()),
            Kind::Approval => ("tool request".to_owned(), Style::new().fg(Color::Yellow)),
            Kind::Dead => (format!("gave up: {}", e.note.as_deref().unwrap_or("")), Style::new().fg(Color::Red)),
            _ if e.question.is_some() => ("asks".to_owned(), Style::new().fg(Color::Yellow)),
            _ => (String::new(), Style::new()),
        };
        if !what.is_empty() {
            head.push(Span::styled(format!("  {what}"), style));
        }
        if e.sealed.is_some() {
            head.push(Span::raw(format!("  {}", super::dm::LOCK)));
        }
        if e.forged || e.sealed == Some(sealed::State::Forged) {
            head.push(Span::styled("  forged", Style::new().fg(Color::Red).bold()));
        }
        lines.push(Line::from(head));
        lines.extend(text(e, entries, self.width.saturating_sub(2)).into_iter().map(|line| indented("  ", line)));
        lines.extend(self.reactions(e).map(|line| indented("  ", line)));
    }

    /// A tool call: when it started, its kind, its title, and how it ended or how long it has run.
    fn call(&self, lines: &mut Vec<Line<'static>>, call: &Call, first: &str) {
        let kind = call.kind.as_deref().unwrap_or("tool");
        let (status, style) = match call.status.as_deref() {
            Some("completed") => (format!("ok {}", took(call)), Style::new().fg(Color::Green)),
            Some("failed") => (format!("failed {}", took(call)), Style::new().fg(Color::Red)),
            _ if self.busy => {
                let elapsed = crate::roster::short(crate::roster::since(call.started, self.now));
                let spin = SPINNER[usize::try_from(self.now.timestamp()).unwrap_or(0) % SPINNER.len()];
                (format!("{spin} running {elapsed}"), busy())
            }
            Some(status) => (status.replace('_', " "), frame::dim()),
            None => ("started".to_owned(), frame::dim()),
        };
        let head = format!("{first}{} {kind:<7} ", hm(call.started));
        let room = self.width.saturating_sub(head.chars().count() + status.chars().count() + 2);
        lines.push(Line::from(vec![
            Span::styled(head, frame::dim()),
            Span::raw(format!("{:<room$}  ", cut(&call.title, room))),
            Span::styled(status, style),
        ]));
    }
}

/// How a message nested under another shows after its time: its route and what it is, and its style.
fn nested(e: &Entry) -> (String, Style) {
    let from = e.from.as_ref().map_or("a human", UserId::as_str);
    let to = e.to.as_ref().map_or("", UserId::as_str);
    let lock = if e.sealed.is_some() { format!(" {}", super::dm::LOCK) } else { String::new() };
    let forged = if e.forged || e.sealed == Some(sealed::State::Forged) { " (forged)" } else { "" };
    let forged = format!("{lock}{forged}");
    match e.kind {
        Kind::Note => (format!("{from} → notes{forged}"), Style::new()),
        Kind::Approval => (format!("{from} asks to run{forged}"), Style::new().fg(Color::Yellow)),
        Kind::Decision => (format!("{from} decided for {to}"), frame::dim()),
        Kind::Dead => (format!("{to} gave up"), Style::new().fg(Color::Red)),
        Kind::Escalation => (format!("{from} → {to} escalation{forged}"), Style::new().fg(Color::Red)),
        _ => (format!("{from} → {to}{forged}"), Style::new()),
    }
}

/// A message's text: a question's with its options numbered and its answers.
fn body(e: &Entry, entries: &[Entry]) -> String {
    if let Some(unopened) = super::dm::unopened(e.sealed) {
        return unopened.to_owned();
    }
    let Some(question) = &e.question else {
        return match (&e.note, e.kind) {
            (Some(note), Kind::Approval | Kind::Decision) => format!("{} [{note}]", e.text),
            _ => e.text.clone(),
        };
    };
    let mut text = question.text.clone();
    for (n, option) in question.options.iter().enumerate() {
        let _ = write!(text, "\n{} {option}", n + 1);
    }
    if question.free_text {
        text.push_str("\nor type an answer");
    }
    for (by, answered) in crate::thread::answers_to(e, entries) {
        text.push('\n');
        text.push_str(&crate::thread::answer_line(by, answered));
    }
    text
}

/// A message's text in lines at most `width` wide: as markdown for a message, post or note, else as it is.
fn text(e: &Entry, entries: &[Entry], width: usize) -> Vec<Line<'static>> {
    let text = body(e, entries);
    match e.kind {
        Kind::Message | Kind::Escalation | Kind::Mention | Kind::Post | Kind::Note if e.question.is_none() => {
            markdown::render(&text, width)
        }
        _ => {
            let mut lines = Vec::new();
            for line in text.lines() {
                push(&mut lines, line, "", Style::new(), width);
            }
            lines
        }
    }
}

/// `line` begun with `indent`, its style kept.
fn indented(indent: &str, line: Line<'static>) -> Line<'static> {
    let mut spans = vec![Span::raw(indent.to_owned())];
    spans.extend(line.spans);
    Line::from(spans).style(line.style)
}

/// Where the thread came from, as `first`, its first message read, tells: a channel post, a webhook or schedule
/// (which send alike), a human's ask, or an ant.
fn origin(first: &Entry, data: &Data) -> String {
    let from = first.from.as_ref().map_or("a human", UserId::as_str);
    match (&first.channel, first.kind) {
        (Some(channel), Kind::Post | Kind::Mention) => format!("#{channel} post"),
        _ if first.webhook => format!("webhook {from}"),
        _ if first.from.as_ref().is_none_or(|u| data.is_human(u)) => format!("ask by {from}"),
        _ => from.to_owned(),
    }
}

/// Add `line`, cut to `width` characters a line, those after the first begun with `rest`.
fn push(lines: &mut Vec<Line<'static>>, line: &str, rest: &str, style: Style, width: usize) {
    let mut chars = line.chars().peekable();
    lines.push(Line::styled(chars.by_ref().take(width.max(1)).collect::<String>(), style));
    let room = width.saturating_sub(rest.chars().count()).max(1);
    while chars.peek().is_some() {
        lines.push(Line::styled(format!("{rest}{}", chars.by_ref().take(room).collect::<String>()), style));
    }
}

fn busy() -> Style {
    Style::new().fg(Color::Yellow)
}

/// How long a call that ended took.
fn took(call: &Call) -> String {
    call.duration.and_then(|d| u64::try_from(d.num_milliseconds()).ok()).map_or_default(crate::thread::took)
}

/// The local hour and minute of `at`.
fn hm(at: DateTime<Utc>) -> String {
    at.with_timezone(&Local).format("%H:%M").to_string()
}

/// `text` in at most `width` characters, an ellipsis ending it when cut.
fn cut(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    let mut cut: String = text.chars().take(width.saturating_sub(1)).collect();
    cut.push('…');
    cut
}

/// `path` in at most `width` characters, an ellipsis beginning it when cut, so its file name shows.
fn cut_start(path: &str, width: usize) -> String {
    let count = path.chars().count();
    if count <= width {
        return path.to_owned();
    }
    let skip = count - width.saturating_sub(1);
    format!("…{}", path.chars().skip(skip).collect::<String>())
}

/// `n` tokens, short: `950`, `61k`, `1.2M`.
fn count(n: u64) -> String {
    match n {
        0..1000 => n.to_string(),
        1000..1_000_000 => format!("{}k", n / 1000),
        _ => format!("{}.{}M", n / 1_000_000, n % 1_000_000 / 100_000),
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{Answer, Question, ThreadDone};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::live::tests::{at, message};
    use crate::tui::model::tests::model;
    use crate::tui::timeline::tests::tool;

    /// A thread where alice asks dev-a, which asks dev-b, which answers it, then answers alice; each ant making a
    /// call before it speaks, dev-a one more after hearing back.
    fn delegated(thread: &ThreadId) -> (Vec<Entry>, Vec<ants_proto::StatusEvent>) {
        let entries = vec![
            message(1, thread, "alice", "dev-a", "deploy the site"),
            message(3, thread, "dev-a", "dev-b", "is jobs.retries read anywhere?"),
            message(5, thread, "dev-b", "dev-a", "only in worker/retry.rs"),
            message(7, thread, "dev-a", "alice", "deployed"),
        ];
        let calls = vec![
            tool(thread, "dev-a", "c1", Some("read migrations"), "completed", 2),
            tool(thread, "dev-b", "c2", Some("grep retries"), "completed", 4),
            tool(thread, "dev-a", "c3", Some("cargo test"), "in_progress", 6),
        ];
        (entries, calls)
    }

    fn screen(page: &Page, m: &Model, width: u16, now: DateTime<Utc>) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
        terminal.draw(|f| page.draw(f, f.area(), m, now, true)).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        cells.chunks(usize::from(width)).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn a_delegation_nests_the_other_ants_calls_and_reply_under_its_message() {
        let thread = ThreadId::generate();
        let mut m = model();
        let (entries, calls) = delegated(&thread);
        calls.iter().for_each(|c| m.data.calls.status(c));
        let rows = rows(&entries, m.data.calls.get(&thread).unwrap().calls(), |u| m.data.is_human(u));
        let shown: Vec<(String, usize)> = rows
            .iter()
            .map(|r| match r.item {
                Item::Message(e) => (e.text.clone(), r.depth),
                Item::Call(c) => (c.title.clone(), r.depth),
            })
            .collect();
        let expected = [
            ("deploy the site", 0),
            ("read migrations", 1),
            ("is jobs.retries read anywhere?", 1),
            ("grep retries", 2),
            ("only in worker/retry.rs", 2),
            ("cargo test", 1),
            ("deployed", 0),
        ];
        assert_eq!(shown, expected.map(|(t, d)| (t.to_owned(), d)));
        let first: Vec<String> = glyphs(&rows).into_iter().map(|(first, _)| first).collect();
        assert_eq!(first, ["", "  ├ ", "  ├ ", "  │ ├ ", "  │ └ ", "  └ ", ""]);
    }

    #[test]
    fn the_view_shows_the_timeline_with_running_calls_and_a_panel_beside_it() {
        let thread = ThreadId::generate();
        let mut m = model();
        let (entries, calls) = delegated(&thread);
        m.stored(entries[..3].to_vec(), at(5));
        calls.iter().for_each(|c| m.data.calls.status(c));
        let busy = ants_proto::StatusUpdate::State { state: ants_proto::State::Working };
        m.data
            .live
            .status(&ants_proto::StatusEvent::new(crate::live::tests::user("dev-a"), thread.clone(), busy), at(6));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries[..3].to_vec())).unwrap();
        page.about = Some(About { spend: Some(Vec::new()), ..About::default() });

        let shown = screen(&page, &m, 110, at(47));
        assert!(shown.contains("alice → dev-a"), "{shown}");
        assert!(shown.contains("  deploy the site"), "{shown}");
        assert!(shown.contains("read    read migrations") || shown.contains("execute read migrations"), "{shown}");
        assert!(shown.contains("ok 0ms"), "{shown}");
        assert!(shown.contains("dev-a → dev-b  is jobs.retries read anywhere?"), "{shown}");
        assert!(shown.contains("running 41s"), "a call not ended runs while its ant is busy: {shown}");
        for field in ["STATE", "OWNER", "ANTS", "dev-a ◐", "dev-b", "SPEND  $0.00 · 0 tokens", "FROM   ask by alice"]
        {
            assert!(shown.contains(field), "{field}: {shown}");
        }
        assert!(shown.contains("FILES") && shown.contains("src/c3.rs"), "{shown}");
        assert!(!screen(&page, &m, 80, at(47)).contains("STATE"), "no panel on a narrow view");
    }

    #[test]
    fn the_threads_tool_requests_show_as_cards_under_it_and_answer_to_a_and_enter() {
        let thread = ThreadId::generate();
        let mut m = model();
        let entries = vec![message(1, &thread, "alice", "dev-a", "deploy the site")];
        m.stored(entries.clone(), at(1));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let request = |command| {
            let request = crate::live::tests::request("dev-a", command, vec![Verdict::AllowOnce]);
            ants_proto::ApprovalRequest { thread_id: thread.clone(), ..request }
        };
        let (push, tag) = (request("git push"), request("git tag v1"));
        m.data.live.approvals = vec![push.clone(), tag, crate::live::tests::request("dev-b", "ls", vec![])];
        assert_eq!(page.key(Key::Char('A'), &m.data, at(2)), Some(To::Ask(push.clone(), Some(Verdict::AllowAlways))));
        assert_eq!(page.key(Key::Enter, &m.data, at(2)), Some(To::Ask(push.clone(), None)));

        let shown = screen(&page, &m, 100, at(2));
        assert!(shown.contains("deploy the site"), "{shown}");
        assert!(shown.contains("dev-a asks to run a tool") && shown.contains("execute: git push"), "{shown}");
        assert!(shown.contains("a allow · r reject · Enter details"), "the oldest takes the keys: {shown}");
        assert!(shown.contains("execute: git tag v1") && !shown.contains("execute: ls"), "{shown}");

        m.data.live.approvals.clear();
        assert_eq!(page.key(Key::Char('a'), &m.data, at(2)), None, "nothing pending, nothing to answer");
        m.data.settled.add(push, crate::tools::State::Expired, at(2));
        let shown = screen(&page, &m, 100, at(3));
        assert!(shown.contains("expired unanswered: dev-a rejected it"), "{shown}");
    }

    #[test]
    fn reactions_show_under_each_message_and_plus_reacts_or_takes_one_back() {
        use ants_daemon::reactions::Reacted;

        let thread = ThreadId::generate();
        let mut m = model();
        let entries = vec![
            message(1, &thread, "alice", "dev-a", "deploy the site"),
            message(2, &thread, "dev-a", "dev-b", "check the logs"),
        ];
        let (first, nested) = (entries[0].id.unwrap(), entries[1].id.unwrap());
        m.stored(entries.clone(), at(1));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let reacted = |name: &str, by: &[&str]| Reacted {
            reaction: name.parse().unwrap(),
            by: by.iter().map(|b| crate::live::tests::user(b)).collect(),
        };
        m.data.reactions.insert(first, vec![reacted("eyes", &["alice", "bob"]), reacted("shipit", &["dev-a"])]);
        m.data.reactions.insert(nested, vec![reacted("check", &["alice"])]);
        let shown = screen(&page, &m, 100, at(2));
        // A wide glyph's second cell shows as a space on the test screen.
        assert!(shown.contains("│  👀  2  :shipit: 1"), "{shown}");
        assert!(shown.contains("          ✅  1"), "under a nested message too: {shown}");

        m.pages.push(super::super::model::Page::Thread(page));
        m.key(Key::Home, at(2));
        assert_eq!(m.key(Key::Char('+'), at(2)), None);
        let footer = m.picking.as_ref().unwrap().line().to_string();
        assert!(footer.contains("👍 thumbsup") && footer.contains("Esc close"), "{footer}");
        for c in "eyes".chars() {
            m.key(Key::Char(c), at(2));
        }
        let eyes: ants_proto::Reaction = "eyes".parse().unwrap();
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::React(first, eyes.clone(), true)), "alice takes it back");
        assert!(m.picking.is_none());
        m.key(Key::Char('+'), at(2));
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::React(first, "thumbsup".parse().unwrap(), false)));

        m.read(crate::tui::reads::Read::Reacted(first, eyes, true, Ok(true)), at(3));
        let notice = m.notice_at(at(3)).map(|n| n.text.clone());
        assert_eq!(notice, Some(format!("removed eyes from message {first}")));
    }

    #[test]
    fn opened_from_a_search_the_message_found_is_selected_at_the_top_until_moved() {
        let thread = ThreadId::generate();
        let m = model();
        let entries: Vec<Entry> = (1..=30)
            .map(|n| {
                let (from, to) = if n % 2 == 0 { ("dev-a", "alice") } else { ("alice", "dev-a") };
                message(n, &thread, from, to, &format!("line {n}"))
            })
            .collect();
        let mut page = Page::at(thread.clone(), entries[4].id.unwrap());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let rows = |page: &Page| screen(page, &m, 80, at(40)).lines().map(str::to_owned).collect::<Vec<_>>();
        let shown = rows(&page);
        assert!(shown[1].starts_with("│alice → dev-a"), "{shown:#?}");
        assert_eq!(shown[2].trim_end_matches('│').trim_end(), "│  line 5");
        assert_eq!(page.key(Key::Char('y'), &m.data, at(40)), Some(To::Do(Action::Copy("line 5".to_owned()))));
        assert_eq!(rows(&page)[2], shown[2], "held until moved");
        page.key(Key::Up, &m.data, at(40));
        assert_eq!(rows(&page)[2].trim_end_matches('│').trim_end(), "│  line 4", "moving starts from it");
        page.key(Key::End, &m.data, at(40));
        assert!(rows(&page)[22].contains("line 30"), "the latest at the bottom");
    }

    #[test]
    fn messages_render_as_markdown_at_the_top_level_and_nested() {
        let thread = ThreadId::generate();
        let m = model();
        let mut entries = vec![
            message(1, &thread, "alice", "dev-a", "**done**:\n\n- one\n- two"),
            message(2, &thread, "dev-a", "dev-b", "check `retry.rs`"),
            message(3, &thread, "dev-b", "dev-a", "# Found\n\nin *two* places"),
        ];
        entries[2].kind = Kind::Note;
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let shown = screen(&page, &m, 80, at(40));
        let shown: Vec<&str> = shown.lines().map(|l| l.trim_end_matches('│').trim_end()).collect();
        let from = shown.iter().position(|l| l.contains("alice → dev-a")).unwrap();
        assert_eq!(shown[from + 1..from + 5], ["│  done:", "│", "│  • one", "│  • two"], "{shown:#?}");
        assert!(shown[from + 5].ends_with("dev-a → dev-b  check retry.rs"), "one line on its heading");
        assert!(shown[from + 6].ends_with("dev-b → notes"), "{shown:#?}");
        assert_eq!(shown[from + 7..from + 10], ["│            Found", "│", "│            in two places"]);
    }

    #[test]
    fn sealed_messages_show_a_lock_opened_or_why_not() {
        let thread = ThreadId::generate();
        let m = model();
        let sealed = |seq, from, to, text: &str, state| {
            let mut e = message(seq, &thread, from, to, text);
            e.sealed = Some(state);
            e
        };
        let entries = vec![
            sealed(1, "alice", "bob", "lunch at noon?", sealed::State::Opened),
            sealed(2, "bob", "alice", "[sealed]", sealed::State::Unreadable),
            sealed(3, "bob", "alice", "[forged]", sealed::State::Forged),
        ];
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let shown = screen(&page, &m, 80, at(40));
        let shown: Vec<&str> = shown.lines().map(|l| l.trim_end_matches('│').trim_end()).collect();
        let from = shown.iter().position(|l| l.contains("alice → bob")).unwrap();
        assert!(shown[from].ends_with("🔒"), "{shown:#?}");
        assert_eq!(shown[from + 1], "│  lunch at noon?");
        assert!(shown[from + 3].ends_with("🔒") && !shown[from + 3].contains("forged"), "{shown:#?}");
        assert_eq!(shown[from + 4], "│  [sealed] sent before this device had a key");
        assert!(shown[from + 6].contains("🔒") && shown[from + 6].ends_with("forged"), "{shown:#?}");
        assert!(shown[from + 7].starts_with("│  [forged] "), "{shown:#?}");
    }

    #[test]
    fn a_question_shows_its_options_and_answers() {
        let thread = ThreadId::generate();
        let mut asks = message(1, &thread, "dev-a", "alice", "Backfill strategy?");
        asks.question = Some(Question {
            text: "Backfill strategy?".to_owned(),
            options: vec!["default 0".to_owned(), "skip".to_owned()],
            free_text: true,
        });
        let mut answer = message(2, &thread, "alice", "dev-a", "skip");
        answer.answer = Some(Answer { question: asks.id.unwrap(), option: Some(2) });
        let entries = [asks, answer];
        assert_eq!(
            body(&entries[0], &entries),
            "Backfill strategy?\n1 default 0\n2 skip\nor type an answer\nanswered by alice: option 2, skip"
        );
    }

    #[test]
    fn keys_move_the_selection_copy_it_and_toggle_how_the_human_holds_the_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        let (entries, _) = delegated(&thread);
        m.stored(entries.clone(), at(7));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let data = &mut m.data;
        let copy = |text: &str| Some(To::Do(Action::Copy(text.to_owned())));
        assert_eq!(page.key(Key::Char('y'), data, at(8)), copy("deployed"), "the latest without a selection");
        page.key(Key::Up, data, at(8));
        page.key(Key::Char('k'), data, at(8));
        assert_eq!(page.key(Key::Char('y'), data, at(8)), copy("only in worker/retry.rs"));
        page.key(Key::Home, data, at(8));
        assert_eq!(page.key(Key::Char('y'), data, at(8)), copy("deploy the site"));
        page.key(Key::PageDown, data, at(8));
        assert_eq!(page.selected, None, "past the latest it follows the latest again");

        let change = |change| Some(To::Do(Action::Change(thread.clone(), change, 7)));
        for (key, on) in [('p', Change::Pin(true)), ('m', Change::Mute(true)), ('e', Change::Archive(true))] {
            assert_eq!(page.key(Key::Char(key), data, at(8)), change(on));
        }
        assert_eq!(page.key(Key::Char('d'), data, at(8)), change(Change::Done(true)));
        data.live.marks.pin(&thread);
        page.about = Some(About {
            muted: Some(at(100)),
            done: Some(ThreadDone::new(thread.clone(), crate::live::tests::user("bob"), 7)),
            ..About::default()
        });
        assert_eq!(page.key(Key::Char('p'), data, at(8)), change(Change::Pin(false)), "pressed again, undone");
        assert_eq!(page.key(Key::Char('m'), data, at(8)), change(Change::Mute(false)));
        assert_eq!(page.key(Key::Char('d'), data, at(8)), change(Change::Done(false)));
        assert_eq!(page.key(Key::Char('m'), data, at(200)), change(Change::Mute(true)), "the mute ended");
        data.live.marks.archive(&thread, 7);
        assert_eq!(page.key(Key::Char('e'), data, at(8)), change(Change::Archive(false)));
    }
}
