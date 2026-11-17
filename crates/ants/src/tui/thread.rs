//! An open thread: its title, channels, state and the ants' progress on it, then its conversation as a timeline, the
//! latest at the bottom, beside a side panel with what else is known of it (its work card).
//!
//! The timeline interleaves, in the order stored, the thread's messages, posts, notes, escalations and tool requests
//! with the tool calls of its ants: those running now as their status updates tell ([`timeline::Calls::status`]), and
//! those stored on their timelines ([`timeline::Calls::stored`]), each placed before the first message stored after
//! it started. Messages between a human and an ant, and posts, are the top level; a message from one ant to another
//! nests a level under the sender, and the receiver's calls and reply nest under that message, unless the receiver
//! nests the sender already (a reply). Messages, posts and notes render as markdown, their attachments listed under
//! them, images drawn under their lines when on ([`images`]). Opened from a search, the view selects the message found
//! and holds it at its top until moved. The thread's tool requests show as cards under the conversation
//! ([`approvals`]), a, A or r answering the oldest pending one and Enter showing it in full. Each message shows the
//! reactions to it under it, live, and `+` reacts to the one selected ([`react`]). `>` opens the command bar to reply to
//! the selected message, quoting it and @mentioning its author when an ant; a message quoting another shows
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
use super::frame::{self, ago};
use super::model::{Action, Data, Key, Model, To};
use super::timeline::Call;
use super::{approvals, attach, catchup, images, question, work};
use crate::attachments::Listed;
use crate::live::History;
use crate::thread::{Entry, Kind};

mod conversation;

use conversation::{Item, Shown, busy, cut, glyphs, hm, rows, started, withdrawn_from, woken};

/// How wide the side panel is, shown only when the view is at least [`PANEL_FROM`] wide.
const PANEL_WIDTH: u16 = 34;
const PANEL_FROM: u16 = 96;
/// How many messages PgUp and PgDn move the selection.
const PAGE: isize = 10;

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
    /// The attachment `o` opens of the message with them, by its sequence.
    mark: attach::Mark<u64>,
}

impl Page {
    pub fn new(thread: ThreadId) -> Self {
        Self {
            history: History::new(thread),
            selected: None,
            top: Cell::new(0),
            about: None,
            at: None,
            mark: attach::Mark::default(),
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

    pub const KEYS: &str = "↑↓ select · i reply · > reply to selected · y copy · f forward · o open file · ] next file · + react · p pin · m mute · e archive · d done · u unread from selected · b remind of selected · z snooze · t tree · T tool calls · S TL;DR · x cancel · W withdraw · E withdraw to edit · a A r answer a tool request · End latest · Esc back";

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
                    return question::thread_key(key, data, &self.history.thread, &self.history.entries, now);
                };
                return Some(To::Ask(oldest.clone(), verdict));
            }
            Key::Char('1'..='9') => {
                return question::thread_key(key, data, &self.history.thread, &self.history.entries, now);
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
        let area = draw_cards(frame, area, model, thread, &self.history.entries, now);
        let block = frame::block(title, focused);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let area = catchup::draw_tldr(frame, inner, model, thread, now);
        let width = usize::from(area.width);
        let mut lines = vec![work::why(data, thread)];
        if let Some((line, at)) = data.live.progress_of(thread, width.saturating_sub(16)) {
            lines.push(Line::styled(format!("▶ {line} ({})", ago(at, now)), Style::new().fg(Color::Green)));
        }
        if self.history.loading {
            lines.push(Line::styled("reading the thread…", frame::dim()));
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
        let height = usize::from(area.height);
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
        let paragraph = Paragraph::new(lines).scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
        images::draw(frame, &data.images, &shown.placed.borrow(), top, area);
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

/// Draw the cards of the tool requests of `thread` at the bottom of `area`, in at most half of it: the area left.
fn draw_cards(
    frame: &mut Frame,
    area: Rect,
    model: &Model,
    thread: &ThreadId,
    entries: &[Entry],
    now: DateTime<Utc>,
) -> Rect {
    let width = usize::from(area.width.saturating_sub(2));
    let cards = approvals::cards(&model.data, Some(thread), now);
    let cards = cards.into_iter().enumerate().map(|(at, card)| {
        let lines = approvals::lines(card, model.asking.as_ref(), at == 0, now, width);
        let style = if card.settled.is_some() { frame::dim() } else { Style::new().fg(Color::Yellow) };
        (approvals::title(card), lines, style)
    });
    let questions = question::cards(&model.data, entries, now);
    let questions = questions.iter().map(|card| (card.title(), card.lines(width), card.style()));
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

/// How the human holds a thread.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Flags {
    pinned: bool,
    muted: bool,
    archived: bool,
    done: bool,
}

/// Where the thread came from, as `first`, its first message read, tells: a channel post, a webhook or schedule
/// (which send alike), a human's ask, or an ant.
fn origin(first: &Entry, data: &Data) -> String {
    let from = first.acting.label(first.from.as_ref().map_or("a human", UserId::as_str));
    match (&first.channel, first.kind) {
        (Some(channel), Kind::Post | Kind::Mention) => format!("#{channel} post"),
        _ if first.webhook => format!("webhook {from}"),
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
    use ants_proto::{Answer, Question, ThreadDone};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::conversation::body;
    use super::*;
    use crate::sealed;
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
        assert_eq!(first, ["", "  ├ ", "  ├ ", "  │ ├ ", "  │ └ ", "  └ ", ""]);
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
        let ended = crate::thread::timeline_entries(9, &record.subject(), &serde_json::to_vec(&record).unwrap());
        m.stored(entries.iter().cloned().chain(ended).collect(), at(5));
        let mut page = Page::new(thread.clone());
        page.history.loaded(&thread, Ok(entries)).unwrap();
        assert_eq!(page.messages().count(), 2, "an end is no message");

        let shown = screen(&page, &m, 100, at(6));
        assert!(shown.contains(&format!("{} dev-a ended without a reply: Thanks, noted.", hm(at(4)))), "{shown}");
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
            crate::thread::Parser::default().entry(seq, at(seq.cast_signed()), &post.subject(), &payload).unwrap()
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
        let turn = format!("{} dev-a 2 tool calls, 10.0s, 1 failed", hm(at(10)));
        assert!(shown(&m).contains(&turn), "{}", shown(&m));

        assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::None)));
        assert_eq!(m.notice_at(at(40)).map(|n| n.text.as_str()), Some("tool calls: none"));
        let none = shown(&m);
        assert!(!none.contains(" c1 ") && !none.contains("tool call") && none.contains("two pass"), "{none}");

        assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::All)));
        let all = shown(&m);
        assert!(all.contains("failed 4.0s") && all.contains("ok 5.0s") && !all.contains("tool calls"), "{all}");
        assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::Collapsed)));
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
        let face = format!("{} {{.~.}} dev-a → dev-b", hm(entries[1].stored_at));
        assert!(shown.contains(&face), "an ant's face before its name: {shown}");
        assert!(shown.contains("  deploy the site"), "{shown}");
        assert!(shown.contains("dev-a → dev-b  is jobs.retries read anywhere?"), "{shown}");
        assert!(shown.contains("running 41s"), "a call not ended runs while its ant is busy: {shown}");
        let turn = format!("│ ├ {} dev-b 1 tool call, 0ms ", hm(calls[1].at));
        assert!(shown.contains(&turn), "a finished turn on a line: {shown}");
        assert!(!shown.contains("read migrations"), "{shown}");
        m.data.tools = Tools::All;
        let shown = screen(&page, &m, 110, at(47));
        assert!(shown.contains("read    read migrations") || shown.contains("execute read migrations"), "{shown}");
        assert!(shown.contains("ok 0ms"), "{shown}");
        m.data.tools = Tools::None;
        let shown = screen(&page, &m, 110, at(47));
        assert!(!shown.contains("tool call") && !shown.contains("running 41s"), "{shown}");
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
        let reacted = |name: &str, by: &[&str]| {
            Reacted::new(name.parse().unwrap(), by.iter().map(|b| crate::test_support::live::user(b)).collect())
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
        sent.attachments =
            crate::attachments::listed(&[shot("before.png"), shot("after.png")], |id| envelope.attachment_handle(id));
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
        sent.attachments = crate::attachments::listed(&[shot, log], |id| envelope.attachment_handle(id));
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
