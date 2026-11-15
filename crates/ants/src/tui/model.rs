//! What `ants chat` shows, and how keys and what is read change it, apart from the terminal and the connection: what
//! is known of the ants ([`Data`]), the nav's selection, the views opened from it, the focus, the help and the latest
//! notice. Keys and changes return the [`Action`]s the UI then performs.

use ants_daemon::reactions::Reactions;
use ants_proto::{ApprovalRequest, ChannelEntry, Humans, ReadMarker, ThreadId, UserId, Uuid, Verdict};
use chrono::{DateTime, TimeDelta, Utc};

use super::command::Bar;
use super::exec::Job;
use super::reads::Read;
use super::work::{Facts, Filter};
use super::{channel, dm, home, nav, people, search, thread, work};
use crate::live::{self, State};
use crate::thread::Entry;

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
    Backspace,
    Delete,
    Left,
    Right,
    /// Alt-Enter: a new line in the command bar.
    Newline,
    /// Ctrl-C.
    Interrupt,
    /// Ctrl and another key, for the views to come (Ctrl-K, Ctrl-X).
    Ctrl(char),
    Char(char),
}

/// What the UI does for a key or a change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Read the thread's history, dropping any read of another one, and mark the human's messages on it read.
    Open(ThreadId),
    /// Mark the human's inbox messages on the thread read on all its devices, as `ants inbox --thread` does.
    MarkRead(ThreadId),
    /// Read the reactions to these messages, and again every while.
    Reactions(Vec<Uuid>),
    /// Send a message to another human on a new thread, as `ants ask @human` does.
    Send(dm::Draft),
    /// Read again what may have missed changes.
    Refresh,
    /// Run the command bar's job, for the bar's text.
    Run(Job, String),
    /// Store a decision on a tool request.
    Decide(ApprovalRequest, Verdict),
    /// Edit the command bar's text in the editor.
    Edit(String),
    /// Find the messages matching the search, replacing any search running.
    Search(crate::search::Search),
    Quit,
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

/// Everything read of the ants that the views show: what the live feed reads, what the UI reads besides, and what is
/// worked out of the messages read.
pub struct Data {
    pub live: State,
    /// The server, as the header names it.
    pub server: String,
    /// What each thread's messages tell of its state.
    pub facts: Facts,
    /// The top-level posts of each channel, and the replies on their threads.
    pub feeds: channel::Feeds,
    /// Which of the human's inbox messages are read, once read.
    pub marker: Option<ReadMarker>,
    /// The channels the human is a member of.
    pub channels: Vec<ChannelEntry>,
    /// The team's humans, once read.
    pub humans: Option<Humans>,
    /// The reactions to the messages watched.
    pub reactions: Reactions,
    /// Who of the other humans is online, and the messages with each.
    pub people: people::People,
}

impl Data {
    pub fn me(&self) -> &UserId {
        &self.live.me
    }

    /// Whether `user` is a human: as the humans bucket says once read, else only the CLI's human and `human`.
    pub fn is_human(&self, user: &UserId) -> bool {
        match &self.humans {
            Some(humans) => humans.contains(user) || user == self.me(),
            None => user.is_human() || user == self.me(),
        }
    }

    /// Whether the human's inbox message at `seq` is unread; not while the read marker is unknown.
    pub fn unread(&self, seq: u64) -> bool {
        self.marker.as_ref().is_some_and(|m| !m.is_read(seq))
    }

    /// The thread's title, else the title an ant suggested, else its opening line.
    pub fn title(&self, thread: &ThreadId) -> String {
        let Some(summary) = self.live.summary(thread) else { return thread.to_string() };
        summary
            .title
            .clone()
            .or_else(|| summary.suggested.as_ref().map(|(_, title)| title.clone()))
            .unwrap_or_else(|| summary.opening.clone())
    }
}

/// Whether keys move in the nav or in the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Nav,
    View,
}

/// A view, as the nav selects it or as opened from another.
#[derive(Debug)]
pub enum Page {
    Home(home::Page),
    Work(work::Page),
    Channel(channel::Page),
    Thread(thread::Page),
    Dm(dm::Page),
    Search(search::Page),
}

impl Page {
    /// The view the nav's `item` shows.
    fn of(item: &nav::Item) -> Self {
        match item {
            nav::Item::Home => Self::Home(home::Page::default()),
            nav::Item::Work(state) => Self::Work(work::Page::new(Filter { state: *state, ..Filter::default() })),
            nav::Item::Channel(channel) => Self::Channel(channel::Page::new(channel.clone())),
            nav::Item::Ant(user) => {
                Self::Work(work::Page::new(Filter { user: Some(user.clone()), ..Filter::default() }))
            }
            nav::Item::Person(user) => Self::Dm(dm::Page::new(user.clone())),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Home(_) => "home",
            Self::Work(_) => "work",
            Self::Channel(_) => "channel",
            Self::Thread(_) => "thread",
            Self::Dm(_) => "dm",
            Self::Search(_) => "search",
        }
    }
}

/// Where a view's key leads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum To {
    Thread(ThreadId),
    /// A thread, shown at a message on it.
    Message(ThreadId, Uuid),
    Work(Filter),
}

pub struct Model {
    pub data: Data,
    /// The nav's selection.
    pub nav: nav::Item,
    pub focus: Focus,
    /// The nav's view, then those opened from it, the shown one last; never empty.
    pub pages: Vec<Page>,
    pub help: bool,
    pub notice: Option<Notice>,
    pub bar: Bar,
}

impl Model {
    pub fn new(live: State, server: String) -> Self {
        let data = Data {
            live,
            server,
            facts: Facts::default(),
            feeds: channel::Feeds::default(),
            marker: None,
            channels: Vec::new(),
            humans: None,
            reactions: Reactions::new(),
            people: people::People::default(),
        };
        let nav = nav::Item::Home;
        Self {
            data,
            pages: vec![Page::of(&nav)],
            nav,
            focus: Focus::View,
            help: false,
            notice: None,
            bar: Bar::default(),
        }
    }

    pub fn page(&self) -> &Page {
        self.pages.last().expect("a view")
    }

    pub fn notify(&mut self, text: impl Into<String>, level: Level, now: DateTime<Utc>) {
        self.notice = Some(Notice { text: text.into(), at: now, level });
    }

    /// The notice to show at `now`, if one is recent.
    pub fn notice_at(&self, now: DateTime<Utc>) -> Option<&Notice> {
        self.notice.as_ref().filter(|n| now - n.at < NOTICE_FOR)
    }

    /// Take in what the live feed read or heard.
    pub fn take(&mut self, event: live::Event, now: DateTime<Utc>) -> Vec<Action> {
        match event {
            live::Event::Stored(Ok(entries)) => return self.stored(entries, now),
            live::Event::Stored(Err(e)) => self.notify(format!("{e:#}"), Level::Error, now),
            live::Event::Status(event) => self.data.live.status(&event, now),
            live::Event::Following => self.data.live.approvals_reset(),
            live::Event::Approval(update) => self.data.live.approval(&update),
            live::Event::FollowEnded(Ok(())) => {}
            live::Event::Decided(decided) => self.decided(decided),
            live::Event::FollowEnded(Err(e)) => {
                self.notify(format!("following the tool requests: {e:#}"), Level::Error, now);
            }
            live::Event::Roster(Ok(entries)) => self.data.live.roster(entries, now),
            live::Event::Roster(Err(e)) => self.notify(format!("reading the roster: {e:#}"), Level::Error, now),
            live::Event::History(thread, read) => {
                let page = self.pages.iter_mut().rev().find_map(|page| match page {
                    Page::Thread(page) if page.history.thread == thread => Some(page),
                    _ => None,
                });
                if let Some(page) = page
                    && let Err(e) = page.history.loaded(&thread, read)
                {
                    self.notify(format!("reading thread {thread}: {e:#}"), Level::Error, now);
                }
            }
        }
        Vec::new()
    }

    /// Take the messages read from the stream. Marks read the human's new messages on an open thread, and reads the
    /// reactions to a shown channel's new posts.
    pub fn stored(&mut self, entries: Vec<Entry>, now: DateTime<Utc>) -> Vec<Action> {
        let data = &mut self.data;
        data.live.stored(&entries, now);
        let me = data.live.me.clone();
        let mut posted = false;
        for entry in &entries {
            data.facts.add(entry, &me);
            data.people.dms.add(entry, &me);
            posted |= data.feeds.add(entry);
        }
        let mut actions = Vec::new();
        for page in &mut self.pages {
            if let Page::Thread(page) = page {
                let thread = page.history.thread.clone();
                if entries.iter().any(|e| e.thread_id == thread && work::to(e, &me)) {
                    actions.push(Action::MarkRead(thread));
                }
                page.history.stored(entries.clone());
            }
        }
        if posted && let Page::Channel(page) = self.page() {
            actions.push(Action::Reactions(self.data.feeds.watched(&page.channel)));
        }
        actions
    }

    /// Take what the UI read besides the live feed.
    pub fn read(&mut self, read: Read, now: DateTime<Utc>) {
        let data = &mut self.data;
        match read {
            Read::Marker(Ok(marker)) => data.marker = Some(marker),
            Read::Marker(Err(e)) => self.notify(format!("reading what was read: {e:#}"), Level::Error, now),
            Read::Directory(Ok((channels, humans))) => {
                let me = data.me().clone();
                data.channels = channels.into_iter().filter(|c| c.members.contains(&me)).collect();
                data.humans = Some(humans);
            }
            Read::Directory(Err(e)) => self.notify(format!("reading the channels: {e:#}"), Level::Error, now),
            Read::Marked(_, Ok(())) => {}
            Read::Marked(thread, Err(e)) => {
                self.notify(format!("marking thread {thread} read: {e:#}"), Level::Error, now);
            }
            Read::Reactions(reactions) => data.reactions = reactions,
        }
    }

    /// Take what was read of the other humans, or done for them.
    pub fn people(&mut self, event: people::Event, now: DateTime<Utc>) {
        match event {
            people::Event::Online(read) => {
                if let Some(text) = self.data.people.read(read) {
                    self.notify(text, Level::Error, now);
                }
            }
            people::Event::Sent(sent) => {
                let (text, level) = sent.notice();
                self.notify(text, level, now);
            }
        }
    }

    /// The open thread, to be read again from the start: its history is to be loaded.
    pub fn open_thread(&mut self) -> Option<ThreadId> {
        let Some(Page::Thread(page)) = self.pages.last_mut() else { return None };
        let thread = page.history.thread.clone();
        *page = thread::Page::new(thread.clone());
        Some(thread)
    }

    /// Take the connection's state at `now`: once it is back, what was missed meanwhile is read again.
    pub fn link(&mut self, connected: bool, now: DateTime<Utc>) -> Option<Action> {
        if !self.data.live.link(connected, now) {
            return None;
        }
        self.notify("reconnected: reading what changed meanwhile", Level::Info, now);
        Some(Action::Refresh)
    }

    pub fn key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Interrupt {
            return Some(Action::Quit);
        }
        if self.help {
            self.help = false;
            return None;
        }
        if !self.bar.focused
            && self.focus == Focus::View
            && let Some(Page::Dm(page)) = self.pages.last_mut()
            && let Some(action) = page.write(key)
        {
            return action;
        }
        if let Some(action) = self.bar_key(key, now) {
            return action;
        }
        match key {
            Key::Char('q') => Some(Action::Quit),
            Key::Char('/') => {
                self.bar.focused = true;
                if self.bar.is_empty() {
                    self.bar.set_text("/search ");
                }
                None
            }
            Key::Char('?') => {
                self.help = true;
                None
            }
            Key::Char('R') => Some(Action::Refresh),
            Key::Tab => {
                self.focus = match self.focus {
                    Focus::Nav => Focus::View,
                    Focus::View => Focus::Nav,
                };
                None
            }
            _ => match self.focus {
                Focus::Nav => self.nav_key(key, now),
                Focus::View => self.view_key(key, now),
            },
        }
    }

    fn nav_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let by = match key {
            Key::Enter => {
                self.focus = Focus::View;
                return None;
            }
            Key::Up | Key::Char('k') => -1,
            Key::Down | Key::Char('j') => 1,
            Key::Home => isize::MIN,
            Key::End => isize::MAX,
            _ => return None,
        };
        let items = nav::items(&self.data, now);
        let at = items.iter().position(|i| *i == self.nav).unwrap_or(0);
        let to = items.get(at.saturating_add_signed(by).min(items.len().saturating_sub(1)))?.clone();
        if to == self.nav {
            return None;
        }
        self.select(to)
    }

    /// Select `item` in the nav, showing its view.
    pub fn select(&mut self, item: nav::Item) -> Option<Action> {
        self.pages = vec![Page::of(&item)];
        self.nav = item;
        match self.page() {
            Page::Channel(page) => Some(Action::Reactions(self.data.feeds.watched(&page.channel))),
            _ => None,
        }
    }

    /// Run the search `query` asks for, listing its matches over the view; or why the query is refused.
    pub fn search(&mut self, query: String, now: DateTime<Utc>) -> Result<Action, String> {
        let search = search::parse(&query, now)?;
        for page in &mut self.pages {
            if let Page::Search(page) = page
                && page.found.is_none()
            {
                page.found = Some(Err("stopped for a later search".to_owned()));
            }
        }
        self.pages.push(Page::Search(search::Page::new(query, &search)));
        self.focus = Focus::View;
        Ok(Action::Search(search))
    }

    /// Take the matches of the search run last.
    pub fn found(&mut self, found: anyhow::Result<(Vec<Entry>, usize)>) {
        let page = self.pages.iter_mut().rev().find_map(|page| match page {
            Page::Search(page) if page.found.is_none() => Some(page),
            _ => None,
        });
        if let Some(page) = page {
            page.found(found);
        }
    }

    fn view_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Esc {
            if self.pages.len() > 1 {
                self.pages.pop();
            } else {
                self.focus = Focus::Nav;
            }
            return None;
        }
        let data = &self.data;
        let to = match self.pages.last_mut()? {
            Page::Home(page) => page.key(key, data, now),
            Page::Work(page) => page.key(key, data),
            Page::Channel(page) => page.key(key, data),
            Page::Thread(page) => page.key(key),
            Page::Dm(page) => page.key(key, data),
            Page::Search(page) => page.key(key),
        }?;
        match to {
            To::Thread(thread) => {
                self.pages.push(Page::Thread(thread::Page::new(thread.clone())));
                Some(Action::Open(thread))
            }
            To::Message(thread, message) => {
                self.pages.push(Page::Thread(thread::Page::at(thread.clone(), message)));
                Some(Action::Open(thread))
            }
            To::Work(filter) => {
                self.pages.push(Page::Work(work::Page::new(filter)));
                None
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use ants_proto::{HumanEntry, Verdict};

    use super::*;
    use crate::live::Link;
    use crate::live::tests::{at, message, request, user};
    use crate::tools::Update;

    /// The model of alice, with bob a human too and nothing read yet.
    pub fn model() -> Model {
        let mut m = Model::new(State::new(user("alice"), Duration::from_hours(24)), "hub:4222".to_owned());
        m.data.marker = Some(ReadMarker::default());
        let humans = ["alice", "bob"].map(|h| (h, HumanEntry::new(user(h))));
        m.data.humans = Some(Humans::from_entries(humans));
        m
    }

    #[test]
    fn tab_moves_between_nav_and_view_and_esc_goes_back() {
        let mut m = model();
        assert_eq!((m.focus, m.page().name()), (Focus::View, "home"));
        m.key(Key::Tab, at(0));
        assert_eq!(m.focus, Focus::Nav);
        m.key(Key::Char('j'), at(0));
        assert_eq!((&m.nav, m.page().name()), (&nav::Item::Work(None), "work"));
        m.key(Key::Enter, at(0));
        assert_eq!(m.focus, Focus::View);
        m.key(Key::Esc, at(0));
        assert_eq!(m.focus, Focus::Nav, "Esc on the nav's own view goes back to the nav");
        m.key(Key::Char('k'), at(0));
        m.key(Key::Up, at(0));
        assert_eq!(m.nav, nav::Item::Home, "the first entry stays selected");
    }

    #[test]
    fn help_shows_until_a_key_and_q_or_ctrl_c_quit() {
        let mut m = model();
        assert_eq!(m.key(Key::Char('?'), at(0)), None);
        assert!(m.help);
        assert_eq!(m.key(Key::Char('q'), at(0)), None, "a key closes the help first");
        assert!(!m.help);
        assert_eq!(m.key(Key::Char('q'), at(0)), Some(Action::Quit));
        m.key(Key::Char('?'), at(0));
        assert_eq!(m.key(Key::Interrupt, at(0)), Some(Action::Quit));
    }

    #[test]
    fn opening_a_thread_loads_it_marks_it_read_and_keeps_it_read_as_messages_come() {
        let (thread, other) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.select(nav::Item::Work(None));
        m.stored(
            vec![message(1, &thread, "alice", "dev-a", "deploy"), message(2, &other, "alice", "dev-b", "x")],
            at(1),
        );
        m.key(Key::Down, at(2));
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Open(thread.clone())));
        assert_eq!(m.page().name(), "thread");
        let loaded = vec![message(1, &thread, "alice", "dev-a", "deploy")];
        m.take(live::Event::History(thread.clone(), Ok(loaded)), at(3));
        let reply = message(3, &thread, "dev-a", "alice", "done");
        assert_eq!(m.stored(vec![reply], at(4)), [Action::MarkRead(thread.clone())]);
        assert!(m.stored(vec![message(4, &other, "dev-b", "alice", "elsewhere")], at(5)).is_empty());
        let Page::Thread(page) = m.page() else { panic!("not a thread") };
        assert_eq!(page.history.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 3]);
        assert_eq!(m.open_thread(), Some(thread), "a refresh reads the open thread again");
        m.key(Key::Esc, at(6));
        assert_eq!((m.page().name(), m.focus), ("work", Focus::View));
    }

    #[test]
    fn slash_searches_and_a_match_opens_its_thread_at_it() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.key(Key::Tab, at(0));
        assert_eq!(m.key(Key::Char('/'), at(0)), None);
        for c in "deploy since:soon".chars() {
            assert_eq!(m.key(Key::Char(c), at(0)), None, "keys go to the command bar");
        }
        assert_eq!(m.key(Key::Enter, at(0)), None);
        let error = m.bar.status.as_ref().map(|s| s.text.clone());
        assert!(error.is_some_and(|e| e.starts_with("invalid time \"soon\"")), "a refused query stays");
        assert_eq!(m.bar.text(), "/search deploy since:soon");
        m.key(Key::Ctrl('w'), at(0));
        let Some(Action::Search(search)) = m.key(Key::Enter, at(0)) else { panic!("no search") };
        assert_eq!(search.grep.as_deref(), Some("deploy"));
        assert_eq!((m.bar.focused, m.page().name(), m.focus), (false, "search", Focus::View));

        m.key(Key::Char('/'), at(1));
        m.key(Key::Char('d'), at(1));
        assert!(matches!(m.key(Key::Enter, at(1)), Some(Action::Search(_))));
        let Page::Search(earlier) = &m.pages[1] else { panic!("not a search") };
        assert_eq!(earlier.found, Some(Err("stopped for a later search".to_owned())));
        let found = message(1, &thread, "dev-a", "alice", "deployed");
        m.found(Ok((vec![found], 0)));
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Open(thread)));
        assert_eq!(m.page().name(), "thread");
        m.key(Key::Esc, at(3));
        assert_eq!(m.page().name(), "search", "Esc goes back to the matches");
        m.key(Key::Char('/'), at(4));
        m.key(Key::Esc, at(4));
        assert!(!m.bar.focused, "Esc leaves the bar");
        m.key(Key::Char('/'), at(4));
        assert_eq!((m.bar.focused, m.bar.text()), (true, "/search ".to_owned()), "a draft in the bar stays");
        m.key(Key::Esc, at(4));
        assert_eq!(m.key(Key::Char('q'), at(4)), Some(Action::Quit));
    }

    #[test]
    fn approvals_are_kept_once_until_following_starts_over() {
        let mut m = model();
        let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
        m.take(live::Event::Approval(Update::Pending(push.clone())), at(0));
        m.take(live::Event::Approval(Update::Pending(push.clone())), at(0));
        assert_eq!(m.data.live.approvals, [push]);
        m.take(live::Event::Following, at(1));
        assert!(m.data.live.approvals.is_empty(), "following again lists them anew");
    }

    #[test]
    fn a_reconnection_refreshes_once() {
        let mut m = model();
        assert_eq!(m.link(false, at(1)), None);
        assert_eq!(m.data.live.link, Link::Down(at(1)));
        assert_eq!(m.link(true, at(3)), Some(Action::Refresh));
        assert_eq!(m.link(true, at(4)), None);
        assert!(m.notice_at(at(4)).is_some_and(|n| n.text.starts_with("reconnected")));
    }

    #[test]
    fn the_humans_channels_are_those_it_is_a_member_of() {
        let mut m = model();
        let channel = |name: &str, members: &[&str]| ChannelEntry {
            channel: name.parse().unwrap(),
            members: members.iter().map(|m| user(m)).collect(),
            messenger: None,
            owner: None,
        };
        let channels = vec![channel("ops", &["alice", "dev-a"]), channel("sales", &["bob"])];
        m.read(Read::Directory(Ok((channels, Humans::default()))), at(0));
        assert_eq!(m.data.channels.iter().map(|c| c.channel.as_str()).collect::<Vec<_>>(), ["ops"]);
        assert!(!m.data.is_human(&user("bob")), "the humans read replace those known");
        assert!(m.data.is_human(&user("alice")) && m.data.is_human(&UserId::human()));
    }
}
