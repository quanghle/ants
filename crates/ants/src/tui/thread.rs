//! An open thread: its title, channels, state and the ants' progress on it, then its conversation as a timeline, the
//! latest at the bottom, beside a side panel with what else is known of it (its work card).
//!
//! The timeline interleaves, in the order stored, the thread's messages, posts, notes, escalations and tool requests
//! with the tool calls of its ants: those running now as their status updates tell ([`timeline::Calls::status`]), and
//! those stored on their timelines ([`timeline::Calls::stored`]), each placed before the first message stored after it
//! started. Messages between a human and an ant, and posts, are the top level; a message from one ant to another nests
//! a level under the sender, and the receiver's calls and reply nest under that message, unless the receiver nests the
//! sender already (a reply). Messages, posts and notes render as markdown, their attachments listed under them, images
//! drawn under their lines when on ([`images`]). Opened from a search, the view selects the message found and holds it
//! at its top until moved. The thread's tool requests show as cards under the conversation ([`approvals`]), a, A or r
//! answering the oldest pending one and Enter showing it in full; its questions after them ([`question`]), 1-9
//! answering the latest one waiting and Enter, once no tool request waits, focusing its card. Each message shows the
//! reactions to it under it, live, and `+` reacts to the one selected ([`react`]). `>` opens the command bar to reply
//! to the selected message, quoting it and @mentioning its author when an ant; a message quoting another shows
//! `↪ replying to <author>: <excerpt>` above its text.

use std::cell::{Cell, RefCell};

use ants_proto::{Quote, ThreadId, UserId, Uuid, Verdict};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::acts::{About, Change};
use super::exec::{Edit, Job, Quoted};
use super::model::{Action, Data, Key, Model, To};
use super::style::{self, ago};
use super::timeline::Call;
use super::{approvals, attach, catchup, images, question, work};
use crate::live::History;
use crate::messages::attachments::Listed;
use crate::threads::thread::{Entry, Kind};

mod conversation;

use conversation::{Item, Shown, busy, cut, glyphs, hm, rows, started, withdrawn_from, woken};

/// How wide the side panel is, shown only when the view is at least [`PANEL_FROM`] wide.
const PANEL_WIDTH: u16 = 34;
const PANEL_FROM: u16 = 96;

/// Where the conversation stands, as the keys left it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scroll {
    /// At the bottom, following the latest lines.
    Latest,
    /// From this line, which new messages leave where it is.
    At(usize),
    /// The selected message brought into view, from its last line when moving up, else from its first; then [`At`].
    ///
    /// [`At`]: Scroll::At
    Reveal { up: bool },
}

/// What the conversation drawn last laid out, for the keys that scroll it.
#[derive(Debug, Default)]
struct Laid {
    /// How many lines the view shows.
    height: usize,
    /// How many lines the conversation takes.
    lines: usize,
    /// Each message's sequence and the lines it takes, from its first to past its last.
    messages: Vec<(u64, usize, usize)>,
}

#[derive(Debug)]
pub struct Page {
    pub history: History,
    /// The selected message's stream sequence; None follows the latest.
    selected: Option<u64>,
    /// The first line of the conversation drawn last, so that the selection moves within it.
    top: Cell<usize>,
    /// Where the conversation stands.
    scroll: Cell<Scroll>,
    /// What the conversation drawn last laid out.
    laid: RefCell<Laid>,
    /// What the side panel shows of the thread besides its messages, once read.
    pub about: Option<About>,
    /// The message the thread was opened at, selected and held at the top of the view until a key moves it.
    at: Option<Uuid>,
    /// The attachment `o` opens of the message with them, by its sequence.
    mark: attach::Mark<u64>,
    /// The cursor on the question card the keys answer, while Enter has focused it.
    answering: Option<question::Cursor>,
}

impl Page {
    pub fn new(thread: ThreadId) -> Self {
        Self {
            history: History::new(thread),
            selected: None,
            top: Cell::new(0),
            scroll: Cell::new(Scroll::Latest),
            laid: RefCell::default(),
            about: None,
            at: None,
            mark: attach::Mark::default(),
            answering: None,
        }
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

    pub const KEYS: &str = "↑↓ select or scroll · PgUp PgDn scroll · Home first · v pager · i reply · > reply to selected · y copy · f forward · o open file · ] next file · + react · p pin · m mute · e archive · d done · u unread from selected · b remind of selected · z snooze · t tree · T tool calls · S TL;DR · x cancel · W withdraw · E withdraw to edit · a A r answer a tool request · n reject it with a note · 1-9 Enter answer a question · End latest · Esc back";

    /// The messages shown, in the order stored.
    fn messages(&self) -> impl DoubleEndedIterator<Item = &Entry> {
        self.history.entries.iter().filter(|e| !e.kind.on_timeline())
    }

    /// The ids of the messages shown, for the reactions to them.
    /// The id of the selected message (else the latest), if it has one.
    pub fn selected_id(&self) -> Option<Uuid> {
        self.selection().and_then(|e| e.id)
    }

    pub fn message_ids(&self) -> Vec<Uuid> {
        self.messages().filter_map(|e| e.id).collect()
    }

    /// The message selected, or the one the thread was opened at; none while the view follows the latest.
    pub fn chosen(&self) -> Option<&Entry> {
        let seq = self.selected.or_else(|| self.held())?;
        self.messages().find(|e| e.seq == seq)
    }

    /// The selected message, else the latest.
    fn selection(&self) -> Option<&Entry> {
        let mut messages = self.messages();
        match self.selected.or_else(|| self.held()) {
            Some(seq) => messages.find(|e| e.seq == seq),
            None => messages.last(),
        }
    }

    /// Where `>` leads: the bar open to reply to the selected message, quoting it, and @mentioning its author when an
    /// ant; none for a message with no id or author, or sealed and not opened.
    fn quote(&self, data: &Data) -> Option<To> {
        let e = self.selection().filter(|e| crate::tui::dm::unopened(e.sealed).is_none())?;
        let (id, from) = (e.id?, e.from.clone()?);
        let quoted = Quoted { id, quote: Quote::new(from.clone(), &e.text), sealed: e.sealed.is_some() };
        let mention = (!data.is_human(&from)).then_some(from);
        Some(To::Quote(self.history.thread.clone(), quoted, mention))
    }

    /// What `W` does, or `E` with `edit`: withdraw the selected message, the human's to an ant or its channel post
    /// waking ants, from the queue of each ant it waits for, putting its text and files back into the bar with `edit`;
    /// refused for one known started or withdrawn, and with `edit` for a post one of its ants started on already.
    fn withdrawal(&self, data: &Data, edit: bool) -> To {
        let me = data.me();
        let mine = |e: &&Entry| matches!(e.kind, Kind::Message | Kind::Post) && e.from.as_ref() == Some(me);
        let Some((e, id)) = self.selection().filter(mine).and_then(|e| Some((e, e.id?))) else {
            return To::Refused(format!("withdraw: select a message of {me} to an ant, or a post of {me} waking one"));
        };
        let entries = &self.history.entries;
        if data.withdrawn.contains(&id) {
            return To::Refused(format!("message {id} is withdrawn already"));
        }
        let ants = match (e.kind, e.to.clone()) {
            (Kind::Message, Some(ant)) if data.is_human(&ant) => {
                return To::Refused(format!(
                    "withdraw: message {id} is to {ant}, a human, not waiting in an ant's queue"
                ));
            }
            (Kind::Message, Some(ant)) => vec![ant],
            (Kind::Post, _) => woken(data, e),
            _ => Vec::new(),
        };
        if ants.is_empty() {
            return To::Refused(format!("withdraw: post {id} woke no ant"));
        }
        let queued = data.live.queued.contains_key(&id);
        let (began, waiting): (Vec<UserId>, Vec<UserId>) =
            ants.into_iter().partition(|ant| !queued && started(entries, ant, id));
        let waiting: Vec<UserId> = waiting.into_iter().filter(|ant| !withdrawn_from(entries, ant, id)).collect();
        if let Some(ant) = began.first().filter(|_| waiting.is_empty() || edit) {
            let what = if edit { "edit" } else { "withdraw" };
            return To::Refused(format!("{what} unavailable: {ant} started on message {id} already; x cancels it"));
        }
        if waiting.is_empty() {
            return To::Refused(format!("message {id} is withdrawn already"));
        }
        let edit = edit.then(|| Box::new(Edit { text: e.text.clone(), files: e.attachments.clone() }));
        To::Do(Action::Run(Job::Withdraw { ants: waiting, message: id, edit }, String::new()))
    }

    /// The message whose attachments `o` opens: the selected one, else the latest carrying any.
    fn attached(&self) -> Option<&Entry> {
        match self.selected.or_else(|| self.held()) {
            Some(_) => self.selection(),
            None => self.messages().rev().find(|e| !e.attachments.is_empty()),
        }
    }

    /// The attachment `o` opens.
    pub fn attachment(&self) -> Option<&Listed> {
        let e = self.attached()?;
        e.attachments.get(self.mark.at(e.seq, e.attachments.len()))
    }

    /// Move the selection `by` messages, down for a positive one, bringing it into view; past the latest it follows
    /// the latest again.
    fn step(&mut self, by: isize) {
        let seqs: Vec<u64> = self.messages().map(|e| e.seq).collect();
        let at = self.selected.and_then(|s| seqs.iter().position(|seq| *seq == s)).unwrap_or(seqs.len());
        self.selected = seqs.get(at.saturating_add_signed(by).min(seqs.len())).copied();
        self.scroll.set(match self.selected {
            None => Scroll::Latest,
            Some(_) => Scroll::Reveal { up: by < 0 },
        });
    }

    /// Scroll the conversation `by` lines, down for a positive one; at the bottom with nothing selected it follows the
    /// latest again.
    fn scroll_by(&self, by: isize) {
        let laid = self.laid.borrow();
        let bottom = laid.lines.saturating_sub(laid.height);
        let top = self.top.get().saturating_add_signed(by).min(bottom);
        self.top.set(top);
        self.scroll.set(if top == bottom && self.selected.is_none() { Scroll::Latest } else { Scroll::At(top) });
    }

    /// Up, or with `down` Down: scroll inside the selected message while it goes on past the view that way, else
    /// select the next message that way; with the selection out of view, select the message in view nearest that way.
    fn line(&mut self, down: bool) {
        let laid = self.laid.borrow();
        let (top, height) = (self.top.get(), laid.height);
        let selected = self.selected.and_then(|seq| laid.messages.iter().find(|m| m.0 == seq).copied());
        let shown = |&(_, start, end): &(u64, usize, usize)| start < top + height && end > top;
        let within =
            |m @ (_, start, end): (u64, usize, usize)| shown(&m) && if down { end > top + height } else { start < top };
        if selected.is_some_and(within) {
            drop(laid);
            return self.scroll_by(if down { 1 } else { -1 });
        }
        if selected.is_none_or(|m| !shown(&m)) && self.scroll.get() != Scroll::Latest {
            let mut in_view = laid.messages.iter().filter(|m| shown(m));
            let nearest = if down { in_view.next() } else { in_view.next_back() };
            if let Some(&(seq, ..)) = nearest {
                drop(laid);
                self.selected = Some(seq);
                self.scroll.set(Scroll::Reveal { up: !down });
                return;
            }
        }
        drop(laid);
        self.step(if down { 1 } else { -1 });
    }

    /// The cursor on the focused question card, while its question is still the one the keys answer.
    pub fn card(&self, data: &Data, now: DateTime<Utc>) -> Option<&question::Cursor> {
        let cursor = self.answering.as_ref()?;
        let (id, _) = question::keyed(data, &self.history.entries, now)?;
        (id == cursor.question).then_some(cursor)
    }

    /// Take `key` on the focused question card, if any: where it leads, if anywhere.
    pub fn card_key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<Option<To>> {
        if self.card(data, now).is_none() {
            self.answering = None;
            return None;
        }
        question::focused(&mut self.answering, key, &self.history.thread)
    }

    /// Type `text` pasted on the focused question card, if any: whether it took it.
    pub fn card_paste(&mut self, text: &str, data: &Data, now: DateTime<Utc>) -> bool {
        self.card(data, now).is_some() && self.answering.as_mut().is_some_and(|c| c.paste(text))
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
        let idle_before = crate::threads::thread::idle_before(data.live.auto_archive, now);
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
        ) && self.at.take().is_some()
        {
            self.scroll.set(Scroll::At(self.top.get()));
        }
        let half = isize::try_from(self.laid.borrow().height / 2).unwrap_or(isize::MAX).max(1);
        match key {
            Key::Up | Key::Char('k') => self.line(false),
            Key::Down | Key::Char('j') => self.line(true),
            Key::PageUp => self.scroll_by(-half),
            Key::PageDown => self.scroll_by(half),
            Key::Home => {
                let first = self.messages().next().map(|e| e.seq);
                self.selected = first;
                self.scroll.set(Scroll::Reveal { up: false });
            }
            Key::End => {
                self.selected = None;
                self.scroll.set(Scroll::Latest);
            }
            Key::Char('y') => return self.selection().map(|e| To::Do(Action::Copy(e.text.clone()))),
            Key::Char('v') => return self.selection().map(|e| To::Do(Action::Page(e.text.clone()))),
            Key::Char('+') => return self.selection().and_then(|e| e.id).map(To::React),
            Key::Char('>') => return self.quote(data),
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
                let Some(oldest) = pending.min_by_key(|r| r.requested_at) else {
                    if key == Key::Enter {
                        let keyed = question::keyed(data, &self.history.entries, now);
                        self.answering = keyed.map(|(id, asked)| question::Cursor::new(id, asked));
                    }
                    return None;
                };
                return Some(To::Ask(oldest.clone(), verdict));
            }
            Key::Char('n') => {
                let pending = data.live.approvals.iter().filter(|r| r.thread_id == self.history.thread);
                return pending.min_by_key(|r| r.requested_at).cloned().map(To::Note);
            }
            Key::Char('1'..='9') => {
                return match question::thread_key(key, data, &self.history.thread, &self.history.entries, now)? {
                    question::Keyed::To(to) => Some(*to),
                    question::Keyed::Focus(cursor) => {
                        self.answering = Some(cursor);
                        None
                    }
                };
            }
            Key::Char('t') => return Some(To::Tree(self.history.thread.clone())),
            Key::Char('z') => return Some(To::Snooze(self.history.thread.clone())),
            Key::Char('b') => {
                let message = self.selection().and_then(|e| e.id);
                return Some(To::Remind(self.history.thread.clone(), message));
            }
            Key::Char('u') => return Some(To::Unread(self.history.thread.clone(), self.selection().map(|e| e.seq))),
            Key::Char('S') => return Some(To::Tldr(self.history.thread.clone())),
            Key::Char('f') => return Some(To::Bar("/forward @".to_owned())),
            Key::Char('T') => return Some(To::Tools),
            Key::Char('o') => {
                let listed = self.attachment()?.clone();
                return Some(To::Do(Action::Run(Job::Open(Box::new(listed)), String::new())));
            }
            Key::Char(']') => {
                let (seq, count) = self.attached().map(|e| (e.seq, e.attachments.len()))?;
                self.mark.next(seq, count);
            }
            Key::Char('x') | Key::Ctrl('x') => return Some(To::Interrupt(self.history.thread.clone())),
            Key::Char('W') => return Some(self.withdrawal(data, false)),
            Key::Char('E') => return Some(self.withdrawal(data, true)),
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
        let cursor = self.card(data, now);
        let area = draw_cards(frame, area, model, thread, &self.history.entries, cursor, now);
        let block = style::block(title, focused);
        let inner = block.inner(area);
        let (outer, area) = (area, catchup::draw_tldr(frame, inner, model, thread, now));
        let width = usize::from(area.width);
        let mut lines = vec![work::why(data, thread)];
        if let Some((line, at)) = data.live.progress_of(thread, width.saturating_sub(16)) {
            lines.push(Line::styled(format!("▶ {line} ({})", ago(at, now)), Style::new().fg(Color::Green)));
        }
        if self.history.loading {
            lines.push(Line::styled("reading the thread…", style::dim()));
        }
        let marked = self.attached().map(|e| (e.seq, self.mark.at(e.seq, e.attachments.len())));
        let busy = data.live.progress.contains_key(thread);
        let shown = Shown { data, now, busy, width, marked, placed: RefCell::default() };
        let calls = data.calls.get(thread).map_or(&[][..], |c| c.calls());
        let ends = data.calls.get(thread).map_or(&[][..], |c| c.ends());
        let rows = rows(&self.history.entries, calls, ends, data.tools, busy, |u| data.is_human(u));
        let held = self.held();
        let selected = self.selected.or(held).and_then(|_| self.selection()).map(|e| e.seq);
        let mut range = None;
        let mut messages = Vec::new();
        for (at, (row, (first, rest))) in rows.iter().zip(glyphs(&rows)).enumerate() {
            if at > 0 && row.depth == 0 {
                lines.push(Line::raw(""));
            }
            let start = lines.len();
            shown.row(&mut lines, row, &self.history.entries, &first, &rest);
            if let Item::Message(e) = row.item {
                messages.push((e.seq, start, lines.len()));
                if Some(e.seq) == selected {
                    for line in &mut lines[start..] {
                        line.style = line.style.patch(style::highlight(focused));
                    }
                    range = Some((start, lines.len()));
                }
            }
        }
        let height = usize::from(area.height);
        let bottom = lines.len().saturating_sub(height);
        let top = match (range, self.scroll.get()) {
            (Some((start, _)), _) if held.is_some() => start.min(bottom),
            (_, Scroll::Latest) => bottom,
            (_, Scroll::At(top)) => top.min(bottom),
            (None, Scroll::Reveal { .. }) => self.top.get().min(bottom),
            (Some((start, end)), Scroll::Reveal { up }) => reveal(self.top.get(), (start, end), height, up).min(bottom),
        };
        if let Scroll::Reveal { .. } = self.scroll.get() {
            self.scroll.set(Scroll::At(top));
        }
        self.top.set(top);
        let newer = messages.iter().filter(|(_, start, _)| *start >= top + height).count();
        *self.laid.borrow_mut() = Laid { height, lines: lines.len(), messages };
        let block = match newer {
            0 => block,
            n => block.title_bottom(
                Line::styled(format!(" ↓ {n} newer · End shows them "), Style::new().fg(style::INFO)).right_aligned(),
            ),
        };
        frame.render_widget(block, outer);
        let paragraph = Paragraph::new(lines).scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
        images::draw(frame, &data.images, &shown.placed.borrow(), top, area);
        if let Some(panel) = panel {
            let lines = self.panel(data, calls, now, usize::from(PANEL_WIDTH.saturating_sub(4)));
            frame.render_widget(Paragraph::new(lines).block(style::block(String::new(), false)), panel);
        }
    }

    /// The side panel: the thread's state, owner, ants and what each does, turns left, spend, age, origin, how the
    /// human holds it and the paths its tool calls touched.
    fn panel(&self, data: &Data, calls: &[Call], now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
        let thread = &self.history.thread;
        let summary = data.live.summary(thread);
        let label = |name: &str| Span::styled(format!("{name:<7}"), style::dim());
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
            lines.push(Line::styled("  none", style::dim()));
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
            lines.push(field("AGE", crate::colony::short(crate::colony::since(at, now))));
        }
        if let Some(first) = first {
            lines.push(field("FROM", format!("{} {}", origin(first, data), hm(first.stored_at))));
        }
        let flags = self.flags(data, now);
        let snoozed = super::home::snoozed(data, now).contains_key(thread);
        let held: Vec<&str> = [
            (flags.pinned, "pinned"),
            (flags.muted, "muted"),
            (flags.archived, "archived"),
            (flags.done, "done"),
            (snoozed, "snoozed"),
        ]
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

/// Draw the cards of the tool requests and questions of `thread` at the bottom of `area`, in at most half of it, the
/// question card the keys answer under `cursor` while focused: the area left.
fn draw_cards(
    frame: &mut Frame,
    area: Rect,
    model: &Model,
    thread: &ThreadId,
    entries: &[Entry],
    cursor: Option<&question::Cursor>,
    now: DateTime<Utc>,
) -> Rect {
    let width = usize::from(area.width.saturating_sub(2));
    let cards = approvals::cards(&model.data, Some(thread), now);
    let cards = cards.into_iter().enumerate().map(|(at, card)| {
        let lines = approvals::lines(card, model.asking.as_ref(), at == 0, now, width);
        let style = if card.settled.is_some() { style::dim() } else { Style::new().fg(style::PENDING) };
        (approvals::title(card), lines, style)
    });
    // Enter shows a pending tool request before it focuses a question.
    let enter = !model.data.live.approvals.iter().any(|r| &r.thread_id == thread);
    let questions = question::cards(&model.data, entries, now);
    let questions = questions.iter().map(|card| (card.title(), card.lines(width, cursor, enter), card.style()));
    let mut boxes = Vec::new();
    let mut height = 0;
    for (title, lines, style) in cards.chain(questions) {
        let tall = u16::try_from(lines.len()).unwrap_or(u16::MAX).saturating_add(2);
        if height + tall > area.height / 2 {
            break;
        }
        height += tall;
        boxes.push((title, lines, style, tall));
    }
    if boxes.is_empty() {
        return area;
    }
    let [rest, mut below] = Layout::vertical([Constraint::Min(3), Constraint::Length(height)]).areas(area);
    for (title, lines, style, tall) in boxes {
        let [this, next] = Layout::vertical([Constraint::Length(tall), Constraint::Min(0)]).areas(below);
        let block = ratatui::widgets::Block::bordered().title(title).border_style(style);
        frame.render_widget(Paragraph::new(lines).block(block), this);
        below = next;
    }
    rest
}

/// The first line to show of a view `height` lines high, at `top` before, to bring into it the message on lines
/// `start..end`: whole if it fits, else from its last line when moving `up`, from its first otherwise; the view moving
/// no further than it has to.
fn reveal(top: usize, (start, end): (usize, usize), height: usize, up: bool) -> usize {
    if end - start <= height {
        if start < top {
            start
        } else if end > top + height {
            end - height
        } else {
            top
        }
    } else if up {
        if end > top && end <= top + height { top } else { end - height }
    } else if start >= top && start < top + height {
        top
    } else {
        start
    }
}

/// How the human holds a thread.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Flags {
    pinned: bool,
    muted: bool,
    archived: bool,
    done: bool,
}

/// Where the thread came from, as `first`, its first message read, tells: a channel post, a webhook or schedule
/// (which send alike), a provider's wake, a human's ask, or an ant.
fn origin(first: &Entry, data: &Data) -> String {
    let from = first.acting.label(first.from.as_ref().map_or("a human", UserId::as_str));
    match (&first.channel, first.kind) {
        (Some(channel), Kind::Post | Kind::Mention) => format!("#{channel} post"),
        _ if first.webhook => format!("webhook {from}"),
        _ if let Some(provider) = &first.provider => format!("{provider} wake"),
        _ if first.from.as_ref().is_none_or(|u| data.is_human(u)) => format!("ask by {from}"),
        _ => from,
    }
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
    use ants_proto::{Answer, Pick, Question, ThreadDone};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::conversation::body;
    use super::*;
    use crate::people::sealed;
    use crate::test_support::live::{at, message};
    use crate::tui::test_support::model;
    use crate::tui::test_support::tool;
    use crate::tui::timeline::Tools;

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
        crate::test_support::render::rendered(width, 24, |f| page.draw(f, f.area(), m, now, true))
    }

    #[test]
    fn a_delegation_nests_the_other_ants_calls_and_reply_under_its_message() {
        let thread = ThreadId::generate();
        let mut m = model();
        let (entries, calls) = delegated(&thread);
        calls.iter().for_each(|c| m.data.calls.status(c));
        let calls = m.data.calls.get(&thread).unwrap().calls();
        let rows = rows(&entries, calls, &[], Tools::All, false, |u| m.data.is_human(u));
        let shown: Vec<(String, usize)> = rows
            .iter()
            .map(|r| match &r.item {
                Item::Message(e) => (e.text.clone(), r.depth),
                Item::Call(c) => (c.title.clone(), r.depth),
                Item::End(e) => (e.ant.to_string(), r.depth),
                Item::Turn(t) => (t.ant.to_string(), r.depth),
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
        assert_eq!(first, ["", "  ⎿ ", "    ", "      ⎿ ", "        ", "    ", ""]);
    }

    #[test]
    fn a_prompt_ended_with_no_reply_shows_dim_where_it_ended() {
        let thread = ThreadId::generate();
        let mut m = model();
        let entries = vec![
            message(1, &thread, "dev-a", "dev-b", "check the logs"),
            message(3, &thread, "dev-b", "dev-a", "clean"),
        ];
        let end = ants_proto::TimelineEntry::NoReply {
            why: ants_proto::NoReplyWhy::ReplyToOwn,
            reason: None,
            text: "\nThanks, noted.\nMore.".to_owned(),
        };
        let user = crate::test_support::live::user;
        let record = ants_proto::Timeline {
            at: at(4),
            ..ants_proto::Timeline::new(user("dev-a"), thread.clone(), entries[1].id.unwrap(), vec![end], 0)
        };
        let ended =
            crate::threads::thread::timeline_entries(9, &record.subject(), &serde_json::to_vec(&record).unwrap());
        m.stored(entries.iter().cloned().chain(ended).collect(), at(5));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        assert_eq!(page.messages().count(), 2, "an end is no message");

        let shown = screen(&page, &m, 120, at(6));
        let ended =
            shown.lines().find(|l| l.contains("● dev-a  ended without a reply: Thanks, noted.")).unwrap_or_default();
        assert!(ended.split('│').nth(1).is_some_and(|l| l.ends_with(&hm(at(4)))), "{shown}");
        assert!(!shown.contains("More."), "{shown}");
    }

    #[test]
    fn a_message_waiting_its_turn_shows_how_many_go_before_it() {
        let thread = ThreadId::generate();
        let mut m = model();
        let entries = vec![
            message(1, &thread, "alice", "dev-a", "deploy the site"),
            message(2, &thread, "alice", "dev-a", "then the docs"),
        ];
        m.stored(entries.clone(), at(2));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries.clone())).unwrap();
        let queued = ants_proto::StatusUpdate::Queued { message: entries[1].id.unwrap(), ahead: 1 };
        let event = ants_proto::StatusEvent::new(crate::test_support::live::user("dev-a"), thread.clone(), queued);
        m.data.live.status(&event, at(3));
        let shown = screen(&page, &m, 100, at(4));
        let line = shown.lines().find(|l| l.contains("queued")).unwrap_or_default();
        assert!(line.contains("alice → dev-a") && line.contains("queued · 1 ahead"), "{shown}");
        assert_eq!(shown.matches("queued").count(), 1, "{shown}");
    }

    #[test]
    fn w_and_e_withdraw_a_waiting_message_of_the_human_to_an_ant_and_withdrawn_ones_are_marked() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut entries = vec![
            message(1, &thread, "alice", "dev-a", "deploy the site"),
            message(2, &thread, "dev-a", "alice", "deploying"),
            message(3, &thread, "alice", "dev-a", "then the docs"),
        ];
        let (first, last) = (entries[0].id.unwrap(), entries[2].id.unwrap());
        entries[1].in_reply_to = Some(first);
        m.stored(entries.clone(), at(3));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries.clone())).unwrap();
        let withdraw = |edit| {
            Some(To::Do(Action::Run(
                Job::Withdraw { ants: vec![crate::test_support::live::user("dev-a")], message: last, edit },
                String::new(),
            )))
        };
        assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), withdraw(None));
        let edit = Edit { text: "then the docs".into(), files: Vec::new() };
        assert_eq!(page.key(Key::Char('E'), &m.data, at(4)), withdraw(Some(Box::new(edit))));
        page.key(Key::Up, &m.data, at(4));
        page.key(Key::Up, &m.data, at(4));
        let refused = |why: &str| Some(To::Refused(why.to_owned()));
        assert_eq!(
            page.key(Key::Char('W'), &m.data, at(4)),
            refused("withdraw: select a message of alice to an ant, or a post of alice waking one")
        );
        page.key(Key::Up, &m.data, at(4));
        let started = format!("withdraw unavailable: dev-a started on message {first} already; x cancels it");
        assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), refused(&started));

        let mut notice =
            message(4, &thread, "dev-a", "alice", "[ants] Withdrawn by alice before dev-a was prompted with it");
        notice.in_reply_to = Some(last);
        notice.notice = Some(ants_proto::NoticeKind::Withdrawn);
        entries.push(notice);
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        page.key(Key::Up, &m.data, at(5));
        page.key(Key::Up, &m.data, at(5));
        assert_eq!(page.key(Key::Char('W'), &m.data, at(5)), refused(&format!("message {last} is withdrawn already")));
        m.data.withdrawn.insert(first);
        let shown = screen(&page, &m, 100, at(5));
        assert_eq!(shown.matches("  withdrawn").count(), 2, "{shown}");
    }

    #[test]
    fn w_and_e_withdraw_a_channel_post_of_the_human_from_the_ants_it_woke_that_have_not_started() {
        use ants_proto::{ChannelEntry, ChannelPost, Humans, NoticeKind};
        let user = crate::test_support::live::user;
        let (ops, mut m) = ("ops".parse::<ants_proto::ChannelId>().unwrap(), model());
        let mut channel = ChannelEntry::new(ops.clone());
        channel.members = ["alice", "dev-a", "dev-b"].map(user).to_vec();
        channel.messenger = Some(user("triage"));
        m.data.channels.push(channel);
        let parse = |seq: u64, post: &ChannelPost| {
            let payload = serde_json::to_vec(post).unwrap();
            crate::threads::thread::Parser::default()
                .entry(seq, at(seq.cast_signed()), &post.subject(), &payload)
                .unwrap()
        };
        let top = ChannelPost::new(ops.clone(), user("alice"), "@dev-a @dev-b @bob ship it");
        let thread = top.thread_id.clone();
        let shipping = top.reply(user("dev-a"), "shipping", &[], &Humans::default()).unwrap();
        let later = ChannelPost::on_thread(ops, thread.clone(), user("alice"), "and the docs");
        let mut entries = vec![parse(1, &top), parse(2, &shipping), parse(3, &later)];
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries.clone())).unwrap();
        let withdraw = |ants: &[&str], message, edit| {
            let ants = ants.iter().map(|a| user(a)).collect();
            Some(To::Do(Action::Run(Job::Withdraw { ants, message, edit }, String::new())))
        };
        let refused = |why: String| Some(To::Refused(why));
        assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), withdraw(&["triage"], later.id, None));
        page.key(Key::Home, &m.data, at(4));
        assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), withdraw(&["dev-b"], top.id, None));
        let edit = format!("edit unavailable: dev-a started on message {} already; x cancels it", top.id);
        assert_eq!(page.key(Key::Char('E'), &m.data, at(4)), refused(edit));

        let mut notice = message(4, &thread, "dev-b", "alice", "[ants] Withdrawn by alice before dev-b was prompted");
        (notice.in_reply_to, notice.notice) = (Some(top.id), Some(NoticeKind::Withdrawn));
        entries.push(notice);
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        page.key(Key::Home, &m.data, at(5));
        let started = format!("withdraw unavailable: dev-a started on message {} already; x cancels it", top.id);
        assert_eq!(page.key(Key::Char('W'), &m.data, at(5)), refused(started));
    }

    #[test]
    fn t_cycles_a_finished_turn_s_calls_from_one_line_to_each_to_none() {
        let thread = ThreadId::generate();
        let mut m = model();
        let entries = vec![
            message(1, &thread, "alice", "dev-a", "run the tests"),
            message(30, &thread, "dev-a", "alice", "two pass"),
        ];
        m.stored(entries.clone(), at(30));
        for (id, status, secs) in
            [("c1", "in_progress", 10), ("c1", "failed", 14), ("c2", "in_progress", 15), ("c2", "completed", 20)]
        {
            m.data.calls.status(&tool(&thread, "dev-a", id, Some(id), status, secs));
        }
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        m.pages.push(super::super::model::Page::Thread(page));
        let shown = |m: &Model| match m.pages.last() {
            Some(super::super::model::Page::Thread(page)) => screen(page, m, 100, at(40)),
            _ => unreachable!(),
        };
        assert!(shown(&m).contains("  ⎿ ● dev-a  Ran 2 commands · 10.0s · 1 failed"), "{}", shown(&m));

        assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::None)));
        assert_eq!(m.notice_at(at(40)).map(|n| n.text.as_str()), Some("tool calls: none"));
        let none = shown(&m);
        assert!(!none.contains("Run(c1)") && !none.contains("Ran 2") && none.contains("two pass"), "{none}");

        assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::All)));
        let all = shown(&m);
        assert!(all.contains("  ⎿ ● Run(c1) · failed after 4.0s") && all.contains("    ● Run(c2) · 5.0s"), "{all}");
        assert!(!all.contains("Ran 2"), "{all}");
        assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::Collapsed)));
    }

    #[test]
    fn a_running_turn_sums_its_calls_by_kind_and_lists_only_its_latest_three() {
        let thread = ThreadId::generate();
        let mut m = model();
        let entries = vec![message(1, &thread, "alice", "dev-a", "fix the build")];
        m.stored(entries.clone(), at(1));
        let calls = [
            ("c1", "read", "src/a.rs", "completed", 2),
            ("c2", "execute", "cargo build", "completed", 3),
            ("c3", "other", "browser_snapshot", "completed", 4),
            ("c4", "read", "Read src/b.rs", "completed", 5),
            ("c5", "execute", "Run `cargo test`", "in_progress", 6),
        ];
        for (id, kind, title, status, secs) in calls {
            let mut event = tool(&thread, "dev-a", id, Some(title), status, secs);
            if let ants_proto::StatusUpdate::Tool { tool_kind, .. } = &mut event.update {
                *tool_kind = Some(kind.to_owned());
            }
            m.data.calls.status(&event);
        }
        let busy = ants_proto::StatusUpdate::State { state: ants_proto::State::Working };
        m.data.live.status(
            &ants_proto::StatusEvent::new(crate::test_support::live::user("dev-a"), thread.clone(), busy),
            at(6),
        );
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();

        let shown = screen(&page, &m, 80, at(10));
        assert!(shown.contains("dev-a  Read 2 files, ran 2 commands, used 1 other tool · 8s"), "{shown}");
        assert!(shown.contains("      ⎿ ● browser_snapshot · "), "{shown}");
        assert!(shown.contains("        ● Read(src/b.rs) · "), "{shown}");
        assert!(shown.contains("Run(cargo test) · running 4s"), "{shown}");
        assert!(!shown.contains("src/a.rs") && !shown.contains("cargo build"), "only the latest three: {shown}");
    }

    #[test]
    fn the_view_shows_the_timeline_with_running_calls_and_a_panel_beside_it() {
        let thread = ThreadId::generate();
        let mut m = model();
        let (entries, calls) = delegated(&thread);
        m.stored(entries[..3].to_vec(), at(5));
        calls.iter().for_each(|c| m.data.calls.status(c));
        let busy = ants_proto::StatusUpdate::State { state: ants_proto::State::Working };
        m.data.live.status(
            &ants_proto::StatusEvent::new(crate::test_support::live::user("dev-a"), thread.clone(), busy),
            at(6),
        );
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries[..3].to_vec())).unwrap();
        page.about = Some(About { spend: Some(Vec::new()), ..About::default() });

        m.data.live.colony(vec![crate::test_support::colony::entry("dev-a", 3600, 2, at(5))], at(5));
        let shown = screen(&page, &m, 110, at(47));
        assert!(shown.contains("alice → dev-a"), "{shown}");
        assert!(shown.contains("    ● {.~.} dev-a → dev-b"), "an ant's face before its name: {shown}");
        assert!(shown.contains("  deploy the site"), "{shown}");
        assert!(shown.contains("dev-a → dev-b  is jobs.retries read anywhere?"), "{shown}");
        assert!(shown.contains("dev-a  Ran 1 command · 41s"), "the running turn on a line: {shown}");
        assert!(shown.contains("Run(cargo test) · running 41s"), "its latest call under it: {shown}");
        assert!(shown.contains("      ⎿ ● dev-b  Ran 1 command · 0ms "), "a finished turn on a line: {shown}");
        assert!(!shown.contains("read migrations"), "{shown}");
        m.data.tools = Tools::All;
        let shown = screen(&page, &m, 110, at(47));
        assert!(shown.contains("  ⎿ ● Run(read migrations) · 0ms "), "{shown}");
        m.data.tools = Tools::None;
        let shown = screen(&page, &m, 110, at(47));
        assert!(!shown.contains("Ran 1") && !shown.contains("running 41s"), "{shown}");
        assert!(shown.contains("only in worker/retry.rs"), "{shown}");
        m.data.tools = Tools::Collapsed;
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
            let request = crate::test_support::live::request("dev-a", command, vec![Verdict::AllowOnce]);
            ants_proto::ApprovalRequest { thread_id: thread.clone(), ..request }
        };
        let (push, tag) = (request("git push"), request("git tag v1"));
        m.data.live.approvals = vec![push.clone(), tag, crate::test_support::live::request("dev-b", "ls", vec![])];
        assert_eq!(page.key(Key::Char('A'), &m.data, at(2)), Some(To::Ask(push.clone(), Some(Verdict::AllowAlways))));
        assert_eq!(page.key(Key::Enter, &m.data, at(2)), Some(To::Ask(push.clone(), None)));
        assert_eq!(page.key(Key::Char('n'), &m.data, at(2)), Some(To::Note(push.clone())), "a note on the oldest");

        let shown = screen(&page, &m, 100, at(2));
        assert!(shown.contains("deploy the site"), "{shown}");
        assert!(shown.contains("dev-a asks to run a tool") && shown.contains("execute: git push"), "{shown}");
        assert!(
            shown.contains("a allow · r reject · n reject with a note · Enter details"),
            "the oldest takes the keys: {shown}"
        );
        assert!(shown.contains("execute: git tag v1") && !shown.contains("execute: ls"), "{shown}");

        m.data.live.approvals.clear();
        assert_eq!(page.key(Key::Char('a'), &m.data, at(2)), None, "nothing pending, nothing to answer");
        m.data.settled.add(push, crate::tools::State::Expired, at(2));
        let shown = screen(&page, &m, 100, at(3));
        assert!(shown.contains("expired unanswered: dev-a rejected it"), "{shown}");
    }

    #[test]
    fn enter_focuses_the_question_card_once_no_tool_request_waits_and_esc_leaves_it() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut asks = message(1, &thread, "dev-a", "alice", "Backfill?");
        let options = ["default every missing row to zero first".to_owned(), "skip".to_owned()];
        asks.question = Some(Question::new("Backfill?", &options, false).unwrap().into());
        let entries = vec![asks.clone()];
        m.stored(entries.clone(), at(1));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        m.pages.push(super::super::model::Page::Thread(page));
        let shown = |m: &Model| match m.pages.last() {
            Some(super::super::model::Page::Thread(page)) => screen(page, m, 40, at(2)),
            _ => unreachable!(),
        };
        let request = crate::test_support::live::request("dev-a", "git push", vec![Verdict::AllowOnce]);
        let push = ants_proto::ApprovalRequest { thread_id: thread.clone(), ..request };
        m.data.live.approvals = vec![push.clone()];
        let screen = shown(&m);
        assert!(
            screen.contains("│1-9 answer ") && !screen.contains("Enter choose"),
            "Enter takes the request: {screen}"
        );
        m.key(Key::Enter, at(2));
        assert!(m.asking.as_ref().is_some_and(|a| a.request == push), "the tool request first");
        m.key(Key::Esc, at(2));
        m.data.live.approvals.clear();

        let screen = shown(&m);
        assert!(screen.contains("│  1 default every missing row to zero │"), "one option a line: {screen}");
        assert!(screen.contains("│    first ") && screen.contains("│  2 skip "), "wrapped: {screen}");
        assert!(screen.contains("│1-9 answer · Enter choose"), "{screen}");
        assert_eq!(m.key(Key::Enter, at(2)), None);
        assert!(shown(&m).contains("│› 1 default every missing row to zero"), "{}", shown(&m));
        assert_eq!(m.key(Key::Esc, at(2)), None);
        assert_eq!(m.page().name(), "thread", "Esc leaves the card, not the thread");
        assert!(!shown(&m).contains("› 1"), "{}", shown(&m));

        m.key(Key::Enter, at(2));
        m.key(Key::Down, at(2));
        m.key(Key::Down, at(2));
        let job = Job::Answer { thread: thread.clone(), question: asks.id.unwrap(), picks: vec![Pick::option(2)] };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, String::new())), "no typed row below the last");
    }

    #[test]
    fn reactions_show_under_each_message_and_plus_reacts_or_takes_one_back() {
        use ants_daemon::records::reactions::Reacted;

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
        let reacted = |name: &str, by: &[&str]| {
            Reacted::new(name.parse().unwrap(), by.iter().map(|b| crate::test_support::live::user(b)).collect())
        };
        m.data.reactions.insert(first, vec![reacted("eyes", &["alice", "bob"]), reacted("shipit", &["dev-a"])]);
        m.data.reactions.insert(nested, vec![reacted("check", &["alice"])]);
        let shown = screen(&page, &m, 100, at(2));
        // A wide glyph's second cell shows as a space on the test screen.
        assert!(shown.contains("│  👀  2  :shipit: 1"), "{shown}");
        assert!(shown.contains("│      ✅  1"), "under a nested message too: {shown}");

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
        assert!(shown[1].starts_with("│❯ alice → dev-a"), "{shown:#?}");
        assert_eq!(shown[2].trim_end_matches('│').trim_end(), "│  line 5");
        assert_eq!(page.key(Key::Char('y'), &m.data, at(40)), Some(To::Do(Action::Copy("line 5".to_owned()))));
        assert_eq!(rows(&page)[2], shown[2], "held until moved");
        page.key(Key::Up, &m.data, at(40));
        assert_eq!(rows(&page)[2].trim_end_matches('│').trim_end(), "│  line 4", "moving starts from it");
        page.key(Key::End, &m.data, at(40));
        assert!(rows(&page)[22].contains("line 30"), "the latest at the bottom");
    }

    /// A thread of a short question, an answer drawn on 241 lines (its heading and 240 rows) and a short reply.
    fn tall(thread: &ThreadId) -> Page {
        let rows: Vec<String> = (1..=240).map(|n| format!("- row {n}.")).collect();
        let entries = vec![
            message(1, thread, "alice", "dev-a", "how did the deploy go?"),
            message(2, thread, "dev-a", "alice", &rows.join("\n")),
            message(3, thread, "alice", "dev-a", "thanks"),
        ];
        let mut page = Page::new(thread.clone());
        page.history.loaded(thread, Ok(entries)).unwrap();
        page
    }

    /// The rows of the tall answer `shown` shows.
    fn rows_shown(shown: &str) -> Vec<usize> {
        let rows = shown.split("row ").skip(1);
        rows.filter_map(|r| r.split_once('.').and_then(|(n, _)| n.parse().ok())).collect()
    }

    #[test]
    fn page_down_alone_brings_every_line_of_a_message_taller_than_the_view_into_it() {
        let thread = ThreadId::generate();
        let m = model();
        let mut page = tall(&thread);
        let draw =
            |page: &Page| crate::test_support::render::rendered(120, 30, |f| page.draw(f, f.area(), &m, at(9), true));
        draw(&page);
        page.key(Key::Home, &m.data, at(9));
        let mut seen = std::collections::BTreeSet::new();
        let mut shown = draw(&page);
        for _ in 0..40 {
            seen.extend(rows_shown(&shown));
            page.key(Key::PageDown, &m.data, at(9));
            let next = draw(&page);
            if next == shown {
                break;
            }
            shown = next;
        }
        assert_eq!(seen, (1..=240).collect(), "{shown}");
        assert!(shown.contains("thanks"), "{shown}");
        assert_eq!(page.chosen().map(|e| e.text.as_str()), Some("how did the deploy go?"), "PgDn keeps the selection");
        page.key(Key::PageUp, &m.data, at(9));
        let back = rows_shown(&draw(&page));
        assert!(back.contains(&220) && !back.contains(&240), "PgUp scrolls back by half a view: {back:?}");
    }

    #[test]
    fn up_and_down_scroll_inside_a_selected_message_taller_than_the_view_before_moving_on() {
        let thread = ThreadId::generate();
        let m = model();
        let mut page = tall(&thread);
        let draw =
            |page: &Page| crate::test_support::render::rendered(120, 30, |f| page.draw(f, f.area(), &m, at(9), true));
        draw(&page);
        page.key(Key::Home, &m.data, at(9));
        draw(&page);
        page.key(Key::Down, &m.data, at(9));
        let mut shown = draw(&page);
        assert_eq!(rows_shown(&shown).first(), Some(&1), "from its first line: {shown}");
        let answer = |page: &Page| page.chosen().is_some_and(|e| e.text.starts_with("- row 1."));
        let mut seen = std::collections::BTreeSet::new();
        while answer(&page) {
            seen.extend(rows_shown(&shown));
            page.key(Key::Down, &m.data, at(9));
            shown = draw(&page);
        }
        assert_eq!(seen, (1..=240).collect());
        assert_eq!(page.chosen().map(|e| e.text.as_str()), Some("thanks"));
        page.key(Key::Up, &m.data, at(9));
        let shown = draw(&page);
        assert!(answer(&page) && rows_shown(&shown).contains(&240), "from its last line: {shown}");
        page.key(Key::Up, &m.data, at(9));
        assert!(answer(&page), "Up scrolls inside it");
        let text = page.chosen().unwrap().text.clone();
        assert_eq!(page.key(Key::Char('v'), &m.data, at(9)), Some(To::Do(Action::Page(text))));
    }

    #[test]
    fn a_message_arriving_while_scrolled_up_leaves_the_view_and_shows_in_the_count() {
        let thread = ThreadId::generate();
        let m = model();
        let entries: Vec<Entry> =
            (1..=30).map(|n| message(n, &thread, "dev-a", "alice", &format!("line {n}"))).collect();
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let draw =
            |page: &Page| crate::test_support::render::rendered(80, 24, |f| page.draw(f, f.area(), &m, at(40), true));
        let newer = |shown: &str| {
            let border = shown.lines().last().unwrap_or_default();
            border
                .split_once("↓ ")
                .and_then(|(_, n)| n.split_once(" newer · End shows them"))
                .map(|(n, _)| n.to_owned())
        };
        assert_eq!(newer(&draw(&page)), None, "following the latest");
        page.key(Key::PageUp, &m.data, at(40));
        let before = draw(&page);
        let count: usize = newer(&before).unwrap().parse().unwrap();
        page.history.stored(vec![message(31, &thread, "dev-a", "alice", "line 31")]);
        let after = draw(&page);
        let body = |shown: &str| shown.lines().take(23).map(str::to_owned).collect::<Vec<_>>();
        assert_eq!(body(&after), body(&before), "the view stays");
        assert_eq!(newer(&after), Some((count + 1).to_string()), "{after}");
        page.key(Key::End, &m.data, at(40));
        let latest = draw(&page);
        assert!(latest.contains("line 31") && newer(&latest).is_none(), "{latest}");
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
        assert!(shown[from + 5].starts_with("│  ⎿ ● dev-a → dev-b  check retry.rs  "), "one line on its heading");
        assert!(shown[from + 6].starts_with("│      ⎿ ● dev-b → notes  "), "{shown:#?}");
        assert_eq!(shown[from + 7..from + 10], ["│          Found", "│", "│          in two places"]);
    }

    #[test]
    fn a_quoted_reply_shows_the_message_it_replies_to_above_its_text() {
        let thread = ThreadId::generate();
        let m = model();
        let mut reply = message(2, &thread, "alice", "dev-b", "why **that** one?");
        reply.quote = Some(Quote::new(crate::test_support::live::user("dev-a"), "picked dev-b for the deploy"));
        let entries = vec![message(1, &thread, "dev-a", "alice", "picked dev-b for the deploy"), reply];
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let shown = screen(&page, &m, 80, at(40));
        let shown: Vec<&str> = shown.lines().map(|l| l.trim_end_matches('│').trim_end()).collect();
        let from = shown.iter().position(|l| l.contains("alice → dev-b")).unwrap();
        let quoted = "│  ↪ replying to dev-a: picked dev-b for the deploy";
        assert_eq!(shown[from + 1..from + 3], [quoted, "│  why that one?"], "{shown:#?}");
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
        assert!(shown[from].contains("alice → bob  🔒 "), "{shown:#?}");
        assert_eq!(shown[from + 1], "│  lunch at noon?");
        assert!(shown[from + 3].contains("🔒") && !shown[from + 3].contains("forged"), "{shown:#?}");
        assert_eq!(shown[from + 4], "│  [sealed] sent before this device had a key");
        assert!(shown[from + 6].contains("🔒") && shown[from + 6].contains("  forged  "), "{shown:#?}");
        assert!(shown[from + 7].starts_with("│  [forged] "), "{shown:#?}");
    }

    #[test]
    fn a_question_shows_its_options_and_answers() {
        let thread = ThreadId::generate();
        let mut asks = message(1, &thread, "dev-a", "alice", "Backfill strategy?");
        let options = ["default 0".to_owned(), "skip".to_owned()];
        asks.question = Some(Question::new("Backfill strategy?", &options, true).unwrap().into());
        let mut answer = message(2, &thread, "alice", "dev-a", "skip");
        answer.answer = Some(Answer { question: asks.id.unwrap(), picks: vec![Pick::option(2)] });
        let entries = [asks, answer];
        assert_eq!(
            body(&entries[0], &entries),
            "Backfill strategy?\n1 default 0\n2 skip\nor type an answer\nanswered by alice: option 2, skip"
        );

        let mut asks = message(3, &thread, "dev-a", "alice", "two questions");
        let regions = ["eu".to_owned(), "us".to_owned()];
        let regions = Question::new("Which regions?", &regions, false).unwrap().multi().headed("Regions");
        let ship = Question::new("Ship it?", &options, false).unwrap();
        asks.question = Some(ants_proto::Questions::new(vec![regions, ship]).unwrap());
        assert_eq!(
            body(&asks, &[]),
            "Regions: Which regions?\n1 eu\n2 us\npick one or more\n\nQ2: Ship it?\n1 default 0\n2 skip"
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
        page.key(Key::End, data, at(8));
        assert_eq!(page.selected, None, "End follows the latest again");

        let change = |change| Some(To::Do(Action::Change(thread.clone(), change, 7)));
        for (key, on) in [('p', Change::Pin(true)), ('m', Change::Mute(true)), ('e', Change::Archive(true))] {
            assert_eq!(page.key(Key::Char(key), data, at(8)), change(on));
        }
        assert_eq!(page.key(Key::Char('d'), data, at(8)), change(Change::Done(true)));
        data.live.marks.pin(&thread);
        page.about = Some(About {
            muted: Some(at(100)),
            done: Some(ThreadDone::new(thread.clone(), crate::test_support::live::user("bob"), 7)),
            ..About::default()
        });
        assert_eq!(page.key(Key::Char('p'), data, at(8)), change(Change::Pin(false)), "pressed again, undone");
        assert_eq!(page.key(Key::Char('m'), data, at(8)), change(Change::Mute(false)));
        assert_eq!(page.key(Key::Char('d'), data, at(8)), change(Change::Done(false)));
        assert_eq!(page.key(Key::Char('m'), data, at(200)), change(Change::Mute(true)), "the mute ended");
        data.live.marks.archive(&thread, 7);
        assert_eq!(page.key(Key::Char('e'), data, at(8)), change(Change::Archive(false)));
    }

    #[test]
    fn attachments_list_under_their_message_and_o_opens_the_marked_one() {
        let thread = ThreadId::generate();
        let m = model();
        let mut sent = message(1, &thread, "dev-a", "alice", "the screenshots");
        let shot = |name: &str| ants_daemon::files::reference(name, "image/png", &[0; 1536]).unwrap();
        let envelope = ants_proto::Envelope::new(
            crate::test_support::live::user("dev-a"),
            crate::test_support::live::user("alice"),
            "x",
        );
        sent.attachments = crate::messages::attachments::listed(&[shot("before.png"), shot("after.png")], |id| {
            envelope.attachment_handle(id)
        });
        let entries = vec![sent, message(2, &thread, "alice", "dev-a", "thanks")];
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        let shown = screen(&page, &m, 80, at(3));
        // The paperclip takes two cells.
        assert!(shown.contains("│  📎  before.png (1.5 KiB) image/png ◂"), "{shown}");
        assert!(shown.contains("│  📎  after.png (1.5 KiB) image/png  "), "{shown}");

        let open = |page: &mut Page| match page.key(Key::Char('o'), &m.data, at(3)) {
            Some(To::Do(Action::Run(Job::Open(listed), text))) if text.is_empty() => listed.attachment.name,
            other => panic!("{other:?}"),
        };
        assert_eq!(open(&mut page), "before.png", "following the latest, the latest message with files");
        page.key(Key::Char(']'), &m.data, at(3));
        assert_eq!(open(&mut page), "after.png");
        assert!(screen(&page, &m, 80, at(3)).contains("after.png (1.5 KiB) image/png ◂"));
        page.key(Key::Char(']'), &m.data, at(3));
        assert_eq!(open(&mut page), "before.png", "past the last, the first");
        page.key(Key::Home, &m.data, at(3));
        page.key(Key::Down, &m.data, at(3));
        assert_eq!(page.key(Key::Char('o'), &m.data, at(3)), None, "the selected message has no files");
    }

    #[test]
    fn an_image_encoded_draws_under_its_line_and_the_others_keep_theirs() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut sent = message(1, &thread, "dev-a", "alice", "the screenshot");
        let png = crate::tui::test_support::png(40, 40);
        let shot = ants_daemon::files::reference("shot.png", "image/png", &png).unwrap();
        let log = ants_daemon::files::reference("build.log", "text/plain", b"ok").unwrap();
        let envelope = ants_proto::Envelope::new(
            crate::test_support::live::user("dev-a"),
            crate::test_support::live::user("alice"),
            "x",
        );
        sent.attachments = crate::messages::attachments::listed(&[shot, log], |id| envelope.attachment_handle(id));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(vec![sent.clone()])).unwrap();
        let before = screen(&page, &m, 80, at(3));

        m.data.images = images::Images::new(true);
        assert_eq!(images::wanted(&m), Vec::new(), "only for the open thread");
        let image = crate::tui::test_support::halfblocks(&png);
        m.data.images.insert(sent.attachments[0].handle.clone(), Some(image));
        let shown = screen(&page, &m, 80, at(3));
        let rows: Vec<&str> = shown.lines().collect();
        let line = rows.iter().position(|r| r.contains("shot.png")).unwrap();
        assert!(rows[line + 3].contains("build.log"), "two rows for the image:\n{shown}");
        assert_eq!(before.lines().filter(|r| r.contains("📎")).count(), 2);
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal.draw(|f| page.draw(f, f.area(), &m, at(3), true)).unwrap();
        let red = ratatui::style::Color::Rgb(200, 40, 40);
        let drawn: Vec<(u16, u16)> = (0..24)
            .flat_map(|y| (0..80).map(move |x| (x, y)))
            .filter(|&(x, y)| terminal.backend().buffer()[(x, y)].bg == red)
            .collect();
        let y = u16::try_from(line).unwrap() + 1;
        assert_eq!(drawn, [(3, y), (4, y), (5, y), (6, y), (3, y + 1), (4, y + 1), (5, y + 1), (6, y + 1)]);
    }
}
