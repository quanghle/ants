//! An open thread: its title, channels, state and the ants' progress on it, then its conversation as a timeline, the
//! latest at the bottom, beside a side panel with what else is known of it (its work card).
//!
//! The timeline interleaves, in the order stored, the thread's messages, posts, notes, escalations and tool requests
//! with the tool calls of its ants: those running now as their status updates tell
//! ([`timeline::Calls::status`](super::timeline::Calls::status)), and those stored on their timelines
//! ([`timeline::Calls::stored`](super::timeline::Calls::stored)); and with the steps of the trail runs reporting on it
//! ([`super::steps`]), each placed before the first message stored after it started. The keys select a step as they
//! select a message, and Enter opens the step's thread. Messages between a human and an ant, and posts, are the top
//! level; a message from one ant to another nests a level under the sender, and the receiver's calls and reply nest
//! under that message, unless the receiver nests the sender already (a reply). Messages, posts and notes render as
//! markdown, their attachments listed under them, images drawn under their lines when on ([`images`](super::images)).
//! Opened from a search, the view selects the message found and holds it at its top until moved. The thread's tool
//! requests show as cards under the conversation ([`approvals`]), a, A or r answering the oldest pending one and Enter
//! showing it in full; its questions after them ([`question`]), 1-9 answering the latest one waiting and Enter, once no
//! tool request waits, focusing its card. Each message shows the reactions to it under it, live, and `+` reacts to the
//! one selected ([`Reads::react`](super::reads::Reads::react)). `>` opens the command bar to reply to the selected
//! message, quoting it and @mentioning its author when an ant; a message quoting another shows
//! `↪ replying to <author>: <excerpt>` above its text.

use std::cell::{Cell, RefCell};

use ants_client::attachments::Listed;
use ants_client::entry::{Entry, Kind};
use ants_proto::{ApprovalRequest, Quote, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};

use super::acts::{About, Change};
use super::exec::{Edit, Job, Quoted};
use super::model::{Action, Data, Key, To};
use super::{approvals, attach, question};
use crate::live::History;

mod cards;
mod conversation;
mod draw;
mod find;
mod panel;
mod pending;

pub use conversation::SPIN_MS;
use conversation::{started, withdrawn_from, woken};

/// How wide the side panel is, shown as [`super::fold`] has it.
const PANEL_WIDTH: u16 = 34;
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

/// A row the keys select: a message, by its stream sequence, or a step of a trail run reporting on the thread, by the
/// step's own thread.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Picked {
    Message(u64),
    Step(ThreadId),
}

/// A message or step, with the lines it takes in the conversation, from its first to past its last.
type Spot = (Picked, usize, usize);

/// What the conversation drawn last laid out, for the keys that scroll it.
#[derive(Debug, Default)]
struct Laid {
    /// How many lines the view shows.
    height: usize,
    /// How many lines the conversation takes.
    lines: usize,
    /// Each message and step, in order, with the lines it takes.
    rows: Vec<Spot>,
    /// The line of each match of the text found, in order.
    found: Vec<usize>,
}

#[derive(Debug)]
pub struct Page {
    pub history: History,
    /// The selected message, or step; None follows the latest.
    selected: Option<Picked>,
    /// The first line of the conversation drawn last, so that the selection moves within it.
    top: Cell<usize>,
    /// Where the conversation stands.
    scroll: Cell<Scroll>,
    /// What the conversation drawn last laid out.
    laid: RefCell<Laid>,
    /// Whether the conversation drawn last showed a spinner.
    pub spinning: Cell<bool>,
    /// What the side panel shows of the thread besides its messages, once read.
    pub about: Option<About>,
    /// The message the thread was opened at, selected and held at the top of the view until a key moves it.
    at: Option<Uuid>,
    /// The attachment `o` opens of the message with them, by its sequence.
    mark: attach::Mark<u64>,
    /// The cursor on the question card the keys answer, while Enter has focused it.
    answering: question::Focus,
    /// The waiting requests and questions listed individually, opened by Ctrl-L.
    listing: Option<pending::List>,
    /// The room the oldest pending tool request had on its card as last drawn, `(width, lines)`, which tells whether it
    /// showed whole there: no lines when its card was not drawn.
    card_room: Cell<Option<(usize, usize)>>,
    /// The text found in the conversation (Ctrl-F), while finding.
    find: Option<find::Find>,
}

impl Page {
    pub fn new(thread: ThreadId) -> Self {
        Self {
            history: History::new(thread),
            selected: None,
            top: Cell::new(0),
            scroll: Cell::new(Scroll::Latest),
            laid: RefCell::default(),
            spinning: Cell::new(false),
            about: None,
            at: None,
            mark: attach::Mark::default(),
            answering: question::Focus::default(),
            listing: None,
            card_room: Cell::new(None),
            find: None,
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

    /// The sequence of the message selected, else of the one the thread was opened at; none while a step is selected.
    fn picked(&self) -> Option<u64> {
        match &self.selected {
            Some(Picked::Message(seq)) => Some(*seq),
            Some(Picked::Step(_)) => None,
            None => self.held(),
        }
    }

    /// The message selected, or the one the thread was opened at; none while the view follows the latest.
    pub fn chosen(&self) -> Option<&Entry> {
        let seq = self.picked()?;
        self.messages().find(|e| e.seq == seq)
    }

    /// The selected message, else the latest; none while a step is selected.
    pub fn selection(&self) -> Option<&Entry> {
        match (self.picked(), &self.selected) {
            (Some(seq), _) => self.messages().find(|e| e.seq == seq),
            (None, Some(Picked::Step(_))) => None,
            (None, _) => self.messages().last(),
        }
    }

    /// The thread of the selected step of a trail run, which Enter opens.
    pub fn step_selected(&self) -> Option<&ThreadId> {
        match &self.selected {
            Some(Picked::Step(thread)) => Some(thread),
            _ => None,
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
    pub fn attached(&self) -> Option<&Entry> {
        match (&self.selected, self.held()) {
            (None, None) => self.messages().rev().find(|e| !e.attachments.is_empty()),
            _ => self.selection(),
        }
    }

    /// The attachment `o` opens.
    pub fn attachment(&self) -> Option<&Listed> {
        let e = self.attached()?;
        e.attachments.get(self.mark.at(e.seq, e.attachments.len()))
    }

    /// The oldest tool request waiting on the thread, which a, A, r, n and Enter answer.
    pub fn pending<'d>(&self, data: &'d Data) -> Option<&'d ApprovalRequest> {
        let pending = data.live.approvals.iter().filter(|r| r.thread_id == self.history.thread);
        pending.min_by_key(|r| r.requested_at)
    }

    /// Move the selection `by` messages or steps, down for a positive one, bringing it into view; past the latest it
    /// follows the latest again.
    fn step(&mut self, by: isize) {
        let rows = self.order();
        let at = self.selected.as_ref().and_then(|s| rows.iter().position(|row| row == s)).unwrap_or(rows.len());
        self.selected = rows.get(at.saturating_add_signed(by).min(rows.len())).cloned();
        self.scroll.set(match self.selected {
            None => Scroll::Latest,
            Some(_) => Scroll::Reveal { up: by < 0 },
        });
    }

    /// The messages and steps the keys select, in order: as drawn last when that showed steps, else the messages.
    fn order(&self) -> Vec<Picked> {
        let laid = self.laid.borrow();
        if laid.rows.iter().any(|(row, ..)| matches!(row, Picked::Step(_))) {
            return laid.rows.iter().map(|(row, ..)| row.clone()).collect();
        }
        self.messages().map(|e| Picked::Message(e.seq)).collect()
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

    pub fn mouse_scroll(&self, by: isize) {
        if !self.listing() {
            self.scroll_by(by);
        }
    }

    pub fn mouse_select(&mut self, seq: u64) {
        self.mouse_pick(Picked::Message(seq));
    }

    /// Select the step of a trail run on its own thread `thread`, as a click on its line does.
    pub fn mouse_select_step(&mut self, thread: ThreadId) {
        self.mouse_pick(Picked::Step(thread));
    }

    fn mouse_pick(&mut self, picked: Picked) {
        if !self.listing() {
            self.selected = Some(picked);
            self.at = None;
            self.answering.leave();
        }
    }

    pub fn mouse_question(&mut self, question: Uuid, at: usize, data: &Data, now: DateTime<Utc>) -> bool {
        if self.card(data, now).is_none() {
            self.key(Key::Enter, data, now);
        }
        self.answering.mouse_select(question, at)
    }

    #[cfg(test)]
    pub fn mouse_top(&self) -> usize {
        self.top.get()
    }

    #[cfg(test)]
    pub fn mouse_bottom(&self) -> usize {
        let laid = self.laid.borrow();
        laid.lines.saturating_sub(laid.height)
    }

    /// Select the message held at the top, else the one selected, else the latest, or the latest in view when scrolled
    /// up: Up from typing.
    pub fn select(&mut self) {
        if let Some(held) = self.held() {
            self.selected.get_or_insert(Picked::Message(held));
        } else if self.selected.is_none() {
            if self.scroll.get() == Scroll::Latest {
                self.selected = self.messages().last().map(|e| Picked::Message(e.seq));
            } else {
                self.line(false);
            }
        }
    }

    /// Whether a message or step is selected rather than the latest followed.
    pub fn selecting(&self) -> bool {
        self.selected.is_some()
    }

    /// Leave the selection, the view where it is, or following the latest when `latest`: back to typing.
    pub fn unselect(&mut self, latest: bool) {
        self.selected = None;
        self.at = None;
        if latest {
            self.scroll.set(Scroll::Latest);
        } else {
            self.scroll_by(0);
        }
    }

    /// Up, or with `down` Down: scroll inside the selected message while it goes on past the view that way, else
    /// select the next message or step that way; with the selection out of view, select the one in view nearest that
    /// way.
    fn line(&mut self, down: bool) {
        let laid = self.laid.borrow();
        let (top, height) = (self.top.get(), laid.height);
        let selected = self.selected.as_ref().and_then(|picked| laid.rows.iter().find(|m| m.0 == *picked));
        let shown = |m: &&Spot| m.1 < top + height && m.2 > top;
        let within = |m: &&Spot| shown(m) && if down { m.2 > top + height } else { m.1 < top };
        if selected.is_some_and(|m| within(&m)) {
            drop(laid);
            return self.scroll_by(if down { 1 } else { -1 });
        }
        if selected.is_none_or(|m| !shown(&m)) && self.scroll.get() != Scroll::Latest {
            let mut in_view = laid.rows.iter().filter(shown);
            let nearest = if down { in_view.next() } else { in_view.next_back() };
            if let Some((picked, ..)) = nearest {
                let picked = picked.clone();
                drop(laid);
                self.selected = Some(picked);
                self.scroll.set(Scroll::Reveal { up: !down });
                return;
            }
        }
        drop(laid);
        self.step(if down { 1 } else { -1 });
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
        if self.listing() {
            return self.waiting_key(key, data, now).flatten();
        }
        if key == Key::Ctrl('l') {
            self.toggle_waiting();
            return None;
        }
        let (thread, last) = (self.history.thread.clone(), self.last_reopening(data));
        let change = |change| Some(To::Do(Action::Change(thread, change, last)));
        let flags = self.flags(data, now);
        if let Some(held) = self.held() {
            self.selected.get_or_insert(Picked::Message(held));
        }
        if matches!(
            key,
            Key::Up
                | Key::Down
                | Key::Char('k' | 'j')
                | Key::PageUp
                | Key::PageDown
                | Key::Home
                | Key::End
                | Key::CtrlHome
                | Key::CtrlEnd
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
                let first = self.messages().next().map(|e| Picked::Message(e.seq));
                self.selected = first;
                self.scroll.set(Scroll::Reveal { up: false });
            }
            Key::End | Key::CtrlEnd => {
                self.selected = None;
                self.scroll.set(Scroll::Latest);
            }
            Key::CtrlHome => {
                self.top.set(0);
                self.scroll.set(Scroll::At(0));
            }
            Key::Char('y') => return self.selection().map(|e| To::Do(Action::Copy(e.text.clone()))),
            Key::Char('v') => return self.selection().map(|e| To::Do(Action::Page(e.text.clone()))),
            Key::Char('+') => return self.selection().and_then(|e| e.id).map(To::React),
            Key::Char('>') => return self.quote(data),
            Key::Enter if let Some(thread) = self.step_selected() => return Some(To::Thread(thread.clone())),
            Key::Char('p') => return change(Change::Pin(!flags.pinned)),
            Key::Char('m') => return change(Change::Mute(!flags.muted)),
            Key::Char('e') => return change(Change::Archive(!flags.archived)),
            Key::Char('d') => return change(Change::Done(!flags.done)),
            Key::Char('a' | 'A' | 'r') | Key::Enter => return self.request_key(key, data, now),
            Key::Char('n') => return self.pending(data).cloned().map(To::Note),
            // The question's card folds while a tool request waits: its keys answer the request's.
            Key::Char('1'..='9') if self.pending(data).is_none() => {
                let keyed = question::thread_key(key, data, &self.history.thread, &self.history.entries, now);
                return self.answering.keyed(keyed);
            }
            Key::Char('t') => return Some(To::Tree(self.history.thread.clone())),
            Key::Char('z') => return Some(To::Snooze(self.history.thread.clone())),
            Key::Char('b') => {
                let message = self.selection().and_then(|e| e.id);
                return Some(To::Remind(self.history.thread.clone(), message));
            }
            Key::Char('u') => return Some(To::Unread(self.history.thread.clone(), self.selection().map(|e| e.seq))),
            Key::Char('S') => return Some(To::Tldr(self.history.thread.clone())),
            Key::Char('f') => return Some(To::Forward),
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

    fn request_key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let Some(oldest) = self.pending(data) else {
            if key == Key::Enter {
                let keyed = question::keyed(data, &self.history.entries, now);
                self.answering.set(keyed.map(|(id, asked)| question::Cursor::new(id, asked)));
            }
            return None;
        };
        Some(approvals::keyed(oldest.clone(), key, self.card_room.get()))
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

#[cfg(test)]
mod tests;
