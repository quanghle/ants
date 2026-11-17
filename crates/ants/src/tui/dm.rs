//! A direct-message view with another human: the threads with messages between the CLI's human and that human, the
//! newest first, each with its latest message and the messages the human has not read, by its read marker. `n` writes
//! a message that starts a new thread, as `ants ask @human` does; `Enter` opens a thread. A DM is between two
//! humans: a group talks in a channel.

use std::collections::{BTreeSet, HashMap};

use ants_proto::{ThreadId, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};

use super::frame::{self, ago};
use super::model::{Action, Data, Key, To};
use super::select::Selection;
use crate::sealed::State;
use crate::thread::{Entry, Kind};

/// The lock marking a sealed message between two humans, and a thread with one.
pub const LOCK: &str = "🔒";

/// What a sealed message the device did not open reads in place of its text: one it has no key entry for was sent
/// before it had a key; None for one opened or not sealed.
pub fn unopened(sealed: Option<State>) -> Option<&'static str> {
    match sealed? {
        State::Opened => None,
        State::Unreadable => Some("[sealed] sent before this device had a key"),
        State::Forged => Some("[forged] does not open with the key of the device it names"),
    }
}

/// A message between the human and another user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Said {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub from: UserId,
    pub text: String,
    /// How many files are attached.
    pub attachments: usize,
    /// Whether it was sealed, and opened.
    pub sealed: Option<State>,
}

/// A thread's messages between the human and another user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dm {
    pub thread: ThreadId,
    pub latest: Said,
    /// The messages, by sequence.
    messages: BTreeSet<u64>,
    /// Those to the human.
    received: BTreeSet<u64>,
}

/// The threads with messages between the human and each other user, as read from the stream.
#[derive(Debug, Default)]
pub struct Dms(HashMap<UserId, HashMap<ThreadId, Dm>>);

impl Dms {
    /// Take a message read: an inbox message between `me` and another user, not forged, is a DM with that user.
    pub fn add(&mut self, entry: &Entry, me: &UserId) {
        let (Some(from), Some(to)) = (&entry.from, &entry.to) else { return };
        if entry.forged || entry.kind != Kind::Message || from == to {
            return;
        }
        let with = match (from == me, to == me) {
            (true, false) => to,
            (false, true) => from,
            _ => return,
        };
        let said = Said {
            seq: entry.seq,
            at: entry.stored_at,
            from: from.clone(),
            text: entry.text.clone(),
            attachments: entry.attachments.len(),
            sealed: entry.sealed,
        };
        let dm = self.0.entry(with.clone()).or_default().entry(entry.thread_id.clone()).or_insert_with(|| Dm {
            thread: entry.thread_id.clone(),
            latest: said.clone(),
            messages: BTreeSet::new(),
            received: BTreeSet::new(),
        });
        if said.seq >= dm.latest.seq {
            dm.latest = said;
        }
        dm.messages.insert(entry.seq);
        if to == me {
            dm.received.insert(entry.seq);
        }
    }

    /// The threads with messages between the human and `with`, the latest message newest first.
    pub fn with(&self, with: &UserId) -> Vec<&Dm> {
        let mut dms: Vec<&Dm> = self.0.get(with).into_iter().flat_map(HashMap::values).collect();
        dms.sort_by_key(|dm| std::cmp::Reverse(dm.latest.seq));
        dms
    }
}

/// How many messages on the thread the human has not read.
fn unread_on(data: &Data, dm: &Dm) -> usize {
    dm.received.iter().filter(|seq| data.unread(**seq)).count()
}

/// How many messages from `with` the human has not read.
pub fn unread(data: &Data, with: &UserId) -> usize {
    data.people.dms.with(with).into_iter().map(|dm| unread_on(data, dm)).sum()
}

/// What a message says, in a line: where every message of the view is rendered, a sealed one after a lock.
fn said(said: &Said, width: usize) -> String {
    let files = match said.attachments {
        0 => String::new(),
        n => format!(" 📎{n}"),
    };
    let text = unopened(said.sealed).map_or_else(|| said.text.replace('\n', " "), str::to_owned);
    let lock = if said.sealed.is_some() { format!("{LOCK} ") } else { String::new() };
    crate::excerpt(&format!("{lock}{}:{files} {text}", said.from), width)
}

/// The row of a direct thread in the list: its unread badge and title, how many messages it has and when the latest
/// came, then what that one says.
fn row(data: &Data, dm: &Dm, width: usize, now: DateTime<Utc>) -> ListItem<'static> {
    let unread = unread_on(data, dm);
    let badge = if unread == 0 { "   ".to_owned() } else { format!("{unread:>2} ") };
    let count = dm.messages.len();
    let plural = if count == 1 { "" } else { "s" };
    ListItem::new(vec![
        Line::from(vec![
            Span::styled(badge, Style::new().fg(Color::Yellow).bold()),
            Span::raw(data.title(&dm.thread)),
            Span::styled(format!("  {count} message{plural} · {}", ago(dm.latest.at, now)), frame::dim()),
        ]),
        Line::styled(format!("   {}", said(&dm.latest, width.saturating_sub(3))), frame::dim()),
    ])
}

/// A message to write to another human, on a new thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Draft {
    pub to: UserId,
    pub text: String,
}

#[derive(Debug)]
pub struct Page {
    /// The other human.
    pub with: UserId,
    /// The selected thread, kept on it as messages reorder the threads.
    selected: Selection<ThreadId>,
    /// The message being written, if any.
    draft: Option<String>,
}

impl Page {
    pub fn new(with: UserId) -> Self {
        Self { with, selected: Selection::default(), draft: None }
    }

    const KEYS: &str = "↑↓ thread · Enter open · n new message";
    const WRITING_KEYS: &str = "Enter send · Esc cancel";

    pub fn keys(&self) -> &'static str {
        if self.draft.is_some() { Self::WRITING_KEYS } else { Self::KEYS }
    }

    pub fn key(&mut self, key: Key, data: &Data) -> Option<To> {
        let dms = data.people.dms.with(&self.with);
        let threads: Vec<ThreadId> = dms.iter().map(|dm| dm.thread.clone()).collect();
        let at = self.selected.at(&threads);
        let to = match key {
            Key::Enter => return dms.get(at).map(|dm| To::Thread(dm.thread.clone())),
            Key::Char('n') => {
                self.draft = Some(String::new());
                return None;
            }
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at + 1,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        };
        self.selected.select(&threads, to);
        None
    }

    /// Take `key` into the message being written: None when none is, the key being the view's then. `Enter` sends a
    /// message that is not blank and `Esc` drops it.
    pub fn write(&mut self, key: Key) -> Option<Option<Action>> {
        let draft = self.draft.as_mut()?;
        match key {
            Key::Char(c) => draft.push(c),
            Key::Backspace => _ = draft.pop(),
            Key::Esc => self.draft = None,
            Key::Enter if draft.trim().is_empty() => {}
            Key::Enter => {
                let text = self.draft.take().unwrap_or_default();
                return Some(Some(Action::Send(Draft { to: self.with.clone(), text })));
            }
            _ => {}
        }
        Some(None)
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let [list, input] = Layout::vertical([Constraint::Min(3), Constraint::Length(3)]).areas(area);
        let state = match data.people.online(&self.with) {
            Some(true) => " · online",
            Some(false) => " · away",
            None => "",
        };
        let dms = data.people.dms.with(&self.with);
        let block = frame::block(format!(" {}{state} ", self.with), focused && self.draft.is_none());
        if dms.is_empty() {
            let text = if data.live.stored_at.is_some() {
                format!("no messages with {} yet: n writes one", self.with)
            } else {
                "reading the stream…".to_owned()
            };
            frame.render_widget(Paragraph::new(text).block(block), list);
        } else {
            let width = usize::from(list.width.saturating_sub(4));
            let rows: Vec<ListItem> = dms.iter().map(|dm| row(data, dm, width, now)).collect();
            let list_widget = List::new(rows).block(block).highlight_style(frame::highlight(focused));
            let selected = self.selected.at(&dms.iter().map(|dm| dm.thread.clone()).collect::<Vec<_>>());
            frame.render_stateful_widget(list_widget, list, &mut ListState::default().with_selected(Some(selected)));
        }
        let (text, style) = match &self.draft {
            Some(draft) => (format!("> {draft}_"), Style::new()),
            None => (format!("n writes to {}, starting a thread", self.with), frame::dim()),
        };
        let block = frame::block(String::new(), focused && self.draft.is_some());
        frame.render_widget(Paragraph::new(Line::styled(text, style)).block(block), input);
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ReadMarker;

    use super::*;
    use crate::live::tests::{at, message, user};
    use crate::tui::model::tests::model;
    use crate::tui::model::{Focus, Page as View};
    use crate::tui::nav;

    #[test]
    fn a_dm_lists_the_threads_between_the_two_humans_newest_first_with_unread_counted() {
        let (old, new, ant, theirs) =
            (ThreadId::generate(), ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut forged = message(6, &old, "bob", "alice", "forged");
        forged.forged = true;
        m.stored(
            vec![
                message(1, &old, "alice", "bob", "lunch?"),
                message(2, &old, "bob", "alice", "sure"),
                message(3, &ant, "alice", "dev-a", "deploy"),
                message(4, &new, "bob", "alice", "review my PR"),
                message(5, &theirs, "bob", "carol", "not alice's"),
                forged,
            ],
            at(5),
        );
        let dms = m.data.people.dms.with(&user("bob"));
        assert_eq!(dms.iter().map(|d| (&d.thread, d.latest.seq)).collect::<Vec<_>>(), [(&new, 4), (&old, 2)]);
        assert_eq!(dms[1].messages.len(), 2);
        assert_eq!(unread(&m.data, &user("bob")), 2);
        m.data.marker = Some(ReadMarker::default());
        m.data.marker.as_mut().unwrap().mark(2);
        assert_eq!(unread(&m.data, &user("bob")), 1, "read by the human's marker");
        assert_eq!(said(&dms[0].latest, 80), "bob: review my PR");
    }

    #[test]
    fn a_sealed_message_shows_after_a_lock_opened_or_why_not() {
        let thread = ThreadId::generate();
        let mut m = model();
        let sealed = |seq, text: &str, state| {
            let mut e = message(seq, &thread, "bob", "alice", text);
            e.sealed = Some(state);
            e
        };
        m.stored(vec![sealed(1, "lunch\nat noon?", State::Opened)], at(1));
        let latest = |m: &crate::tui::model::Model| said(&m.data.people.dms.with(&user("bob"))[0].latest, 80);
        assert_eq!(latest(&m), "🔒 bob: lunch at noon?");
        assert!(m.data.title(&thread).starts_with("🔒 lunch"), "{}", m.data.title(&thread));
        m.stored(vec![sealed(2, "[sealed]", State::Unreadable)], at(2));
        assert_eq!(latest(&m), "🔒 bob: [sealed] sent before this device had a key");
        m.stored(vec![sealed(3, "[forged]", State::Forged)], at(3));
        assert!(latest(&m).starts_with("🔒 bob: [forged] "), "{}", latest(&m));
    }

    #[test]
    fn selecting_a_person_opens_the_dm_whose_threads_open_and_where_a_message_is_written() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "bob", "alice", "hi")], at(1));
        m.select(nav::Item::Person(user("bob")));
        assert_eq!((m.page().name(), m.focus), ("dm", Focus::View));
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Open(thread)));
        m.key(Key::Esc, at(2));
        m.key(Key::Char('n'), at(2));
        for key in [Key::Char('q'), Key::Char('?'), Key::Char('x'), Key::Backspace, Key::Tab, Key::Char(' ')] {
            assert_eq!(m.key(key, at(2)), None, "{key:?} goes to the message being written");
        }
        assert!(!m.help && m.focus == Focus::View);
        let View::Dm(page) = m.page() else { panic!("not a DM") };
        assert_eq!(page.keys(), Page::WRITING_KEYS);
        let draft = Draft { to: user("bob"), text: "q? ".to_owned() };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Send(draft)));
        let View::Dm(page) = m.page() else { panic!("not a DM") };
        assert_eq!(page.keys(), Page::KEYS, "sent, so no longer written");

        m.key(Key::Char('n'), at(3));
        assert_eq!(m.key(Key::Enter, at(3)), None, "nothing to send");
        m.paste("see\r\nyou", at(3));
        let draft = Draft { to: user("bob"), text: "see you".to_owned() };
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Send(draft)), "a paste is written on one line");
        m.key(Key::Char('n'), at(3));
        m.key(Key::Esc, at(3));
        assert_eq!(m.key(Key::Char('q'), at(3)), Some(Action::Quit), "Esc dropped the message");
    }
}
