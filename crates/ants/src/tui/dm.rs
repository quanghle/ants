//! A direct-message view with another human: the threads with messages between the CLI's human and that human, the
//! newest first, but for those the human archived or left idle, each with its latest message and the messages the
//! human has not read, by its read marker. What is typed is written in the command bar, which writes to that human on a
//! new thread, sealed, as `ants ask @human` does; with a thread selected, `n` focuses it and `Enter` opens the thread. A
//! DM is between two humans: a group talks in a channel.

use std::collections::{BTreeSet, HashMap};

use ants_client::entry::{Entry, Kind};
use ants_proto::{ThreadId, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Paragraph;

use super::model::{Data, Key, Model, To};
use super::select::Selection;
use super::style;
use crate::text::ago;
use ants_client::sealed::State;

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

/// The threads with messages between the human and `with` shown, the latest message newest first: archived and idle
/// threads are left out.
fn shown<'d>(data: &'d Data, with: &UserId) -> Vec<&'d Dm> {
    data.people.dms.with(with).into_iter().filter(|dm| !data.live.hidden(&dm.thread)).collect()
}

/// How many messages on the thread the human has not read.
fn unread_on(data: &Data, dm: &Dm) -> usize {
    dm.received.iter().filter(|seq| data.unread(**seq)).count()
}

/// How many messages from `with` on the threads shown the human has not read.
pub fn unread(data: &Data, with: &UserId) -> usize {
    shown(data, with).into_iter().map(|dm| unread_on(data, dm)).sum()
}

/// What a message says, in a line: where every message of the view is rendered, a sealed one after a lock.
fn said(said: &Said, width: usize) -> String {
    let files = match said.attachments {
        0 => String::new(),
        n => format!(" 📎{n}"),
    };
    let text = unopened(said.sealed).map_or_else(|| said.text.replace('\n', " "), str::to_owned);
    let lock = if said.sealed.is_some() { format!("{LOCK} ") } else { String::new() };
    crate::text::excerpt(&format!("{lock}{}:{files} {text}", said.from), width)
}

/// The row of a direct thread in the list: its unread badge and title, how many messages it has and when the latest
/// came, then what that one says.
fn row(data: &Data, dm: &Dm, width: usize, now: DateTime<Utc>) -> Text<'static> {
    let unread = unread_on(data, dm);
    let badge = if unread == 0 { "   ".to_owned() } else { format!("{unread:>2} ") };
    let count = dm.messages.len();
    let plural = if count == 1 { "" } else { "s" };
    Text::from(vec![
        Line::from(vec![
            Span::styled(badge, Style::new().fg(Color::Yellow).bold()),
            Span::raw(data.title(&dm.thread)),
            Span::styled(format!("  {count} message{plural} · {}", ago(dm.latest.at, now)), style::dim()),
        ]),
        Line::styled(format!("   {}", said(&dm.latest, width.saturating_sub(3))), style::dim()),
    ])
}

#[derive(Debug)]
pub struct Page {
    /// The other human.
    pub with: UserId,
    /// The selected thread, kept on it as messages reorder the threads.
    selected: Selection<ThreadId>,
}

impl Page {
    pub fn new(with: UserId) -> Self {
        Self { with, selected: Selection::default() }
    }

    pub fn key(&mut self, key: Key, data: &Data, _: DateTime<Utc>) -> Option<To> {
        let dms = shown(data, &self.with);
        let threads: Vec<ThreadId> = dms.iter().map(|dm| dm.thread.clone()).collect();
        let at = self.selected.at(&threads);
        match key {
            Key::Enter => return dms.get(at).map(|dm| To::Thread(dm.thread.clone())),
            Key::Char('n') => return Some(To::Write),
            _ => _ = self.selected.step(key, &threads, None),
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let data = &model.data;
        let state = match data.people.online(&self.with) {
            Some(true) => " · online",
            Some(false) => " · away",
            None => "",
        };
        let dms = shown(data, &self.with);
        let block = style::block(format!(" {}{state} ", self.with), focused);
        if dms.is_empty() {
            let text = if data.live.stored_at.is_some() {
                format!("no messages with {} yet: type one", self.with)
            } else {
                style::READING.to_owned()
            };
            frame.render_widget(Paragraph::new(text).block(block), area);
        } else {
            let width = usize::from(area.width.saturating_sub(4));
            let rows: Vec<Text> = dms.iter().map(|dm| row(data, dm, width, now)).collect();
            let selected = self.selected.at(&dms.iter().map(|dm| dm.thread.clone()).collect::<Vec<_>>());
            let inner = block.inner(area);
            frame.render_widget(block, area);
            super::mouse::list(
                frame,
                inner,
                &model.ui.mouse,
                super::mouse::Viewport::Dm(self.with.clone()),
                rows,
                selected,
                focused,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ReadMarker;

    use super::*;
    use crate::test_support::live::{at, message, user};
    use crate::tui::exec::Job;
    use crate::tui::input::{Typed, exercise};
    use crate::tui::model::{Action, Focus};
    use crate::tui::nav;
    use crate::tui::test_support::model;

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
    fn archived_dm_threads_leave_the_list_and_its_unread_count() {
        let (old, new) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.data.live.marks.archive(&old, 2);
        m.stored(vec![message(1, &old, "bob", "alice", "lunch?"), message(2, &new, "bob", "alice", "PR")], at(2));
        let threads = |m: &crate::tui::model::Model| -> Vec<ThreadId> {
            shown(&m.data, &user("bob")).into_iter().map(|dm| dm.thread.clone()).collect()
        };
        assert_eq!(threads(&m), std::slice::from_ref(&new));
        assert_eq!(unread(&m.data, &user("bob")), 1);
        m.stored(vec![message(3, &old, "bob", "alice", "still on?")], at(3));
        assert_eq!(threads(&m), [old, new], "a message since brings it back");
        assert_eq!(unread(&m.data, &user("bob")), 3);
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
    fn selecting_a_person_opens_the_dm_whose_threads_open_and_whose_bar_writes_to_them() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "bob", "alice", "hi")], at(1));
        m.select(nav::Item::Person(user("bob")));
        assert_eq!((m.page().name(), m.focus), ("dm", Focus::View));
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Open(thread)));
        m.key(Key::Esc, at(2));
        let shown = crate::test_support::render::rendered(120, 32, |f| crate::tui::frame::draw(f, &m, at(2)));
        assert!(!shown.contains("n writes to"), "no input of its own: {shown}");
        assert_eq!(shown.matches("┌ > to bob on a new thread, sealed ─").count(), 1, "the bar writes to bob: {shown}");

        assert_eq!(m.key(Key::Char('n'), at(2)), None);
        assert_eq!(
            (m.ui.bar.text().as_str(), m.ui.bar.focused),
            ("n", true),
            "with nothing selected, a key is written"
        );
        m.key(Key::Ctrl('u'), at(2));
        m.key(Key::Esc, at(2));
        m.key(Key::Up, at(2));
        assert_eq!(m.key(Key::Char('n'), at(2)), None);
        assert!(m.ui.bar.focused && m.ui.bar.is_empty(), "with a thread selected, n focuses the bar");
        let want = exercise(|typed| match typed {
            Typed::Key(key) => _ = m.key(key, at(2)),
            Typed::Paste(text) => m.paste(text, at(2)),
        });
        let ask = Job::Ask { to: user("bob"), thread: None, text: want.to_owned(), quote: None };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(ask, want.to_owned())));
    }
}
