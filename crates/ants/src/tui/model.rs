//! What `ants chat` shows, and how keys and what is read change it, apart from the terminal and the connection: what
//! is known of the ants ([`Data`]), how the views show it ([`Ui`]), the nav's selection, the views opened from it,
//! the focus, the help and the latest notice. Keys and changes return the [`Action`]s the UI then performs.

use std::collections::{HashMap, HashSet};

use ants_daemon::records::reactions::Reactions;
use ants_proto::{
    ApprovalRequest, ChannelEntry, ChannelId, ChannelNotify, ChannelReadMarkers, Humans, Reaction, ReadMarker,
    TeamDirectory, ThreadId, UserId, Uuid, Verdict,
};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::text::Span;

use super::acts::Change;
use super::approvals::{self, Asking};
use super::command::{self, Bar};
use super::exec::{Job, Quoted};
use super::notices;
use super::reads::{Levels, Mark};
use super::work::{Facts, Filter};
use super::{
    ant, cards, catchup, channel, credentials, dm, faces, fold, held, home, images, nav, people, search, settings,
    steps, team, thread, timeline, tree, work,
};
use crate::live::State;

mod intake;
mod keys;
mod page;
mod prompt;
mod typing;

pub use prompt::{Overlay, Prompt};

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
    /// Ctrl-J, Shift-Enter or Alt-Enter: a new line in the command bar.
    Newline,
    /// Ctrl-C.
    Interrupt,
    /// Ctrl and another key: Ctrl-F, Ctrl-L, Ctrl-X and the like.
    Ctrl(char),
    /// Alt and a character: Alt-B, Alt-F and Alt-D in the command bar.
    Alt(char),
    /// Alt-Backspace: delete the word before the cursor in the command bar.
    AltBackspace,
    /// Ctrl-Left or Alt-Left: a word back in the command bar.
    WordLeft,
    /// Ctrl-Right or Alt-Right: a word on in the command bar.
    WordRight,
    /// Ctrl-Home: the thread's first message, the bar focused too.
    CtrlHome,
    /// Ctrl-End: the thread's latest message, the bar focused too.
    CtrlEnd,
    Char(char),
}

/// What the UI does for a key or a change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Read the thread's history, dropping any read of another one, and mark the human's messages on it read.
    Open(ThreadId),
    /// Mark the human's inbox messages on the thread read on all its devices, as `ants inbox --thread` does.
    MarkRead(ThreadId),
    /// Mark the human's inbox messages on the thread at these sequences unread again on all its devices, as `ants inbox
    /// --unread` does.
    MarkUnread(ThreadId, Vec<u64>),
    /// Mark the human's posts of the channel read on all its devices, as `ants channel read` does.
    MarkChannel(ChannelId, Mark),
    /// Read the reactions to these messages, and again every while.
    Reactions(Vec<Uuid>),
    /// Make a change to how the human holds the thread, the last message, post or note on which has this sequence;
    /// marking it done marks the human's inbox messages on it read too.
    Change(ThreadId, Change, u64),
    /// Copy the text to the terminal's clipboard.
    Copy(String),
    /// Hand the terminal back to the shell until job control resumes the process.
    Suspend,
    /// Read again what may have missed changes.
    Refresh,
    /// Run the command bar's job, for the bar's text.
    Run(Job, String),
    /// Store a decision on a tool request.
    Decide(Box<ApprovalRequest>, Verdict),
    /// Allow each of the tool requests once, in turn, as one batch.
    AllowAll(Vec<ApprovalRequest>),
    /// Edit the command bar's text in the editor.
    Edit(String),
    /// Show the text in the pager, else in the editor.
    Page(String),
    /// React to the message so, or with true take the reaction back, as `ants react` does.
    React(Uuid, Reaction, bool),
    /// Find the messages matching the search, replacing any search running.
    Search(crate::threads::search::Search),
    /// Cancel the prompt each ant runs on its thread, as `ants cancel` does.
    CancelSubtree(Vec<(UserId, ThreadId)>),
    /// Ask for a catch-up summary.
    Summarise(catchup::Ask),
    /// Answer a request for credentials, as `ants enroll approve`, `reject` or `renew` does.
    Credentials(credentials::Order),
    /// Answer an admin order or a decision of the human's assistant held for its consent, as `ants assistant confirm`
    /// does.
    Answer(held::Answer),
    /// Read an ant's settings, or send an order changing them, as `ants ant set` does.
    Settings(settings::Ask),
    /// Keep on the device how the thread view shows tool calls.
    KeepTools(timeline::Tools),
    /// Keep on the device the tab chosen in the channel's view.
    KeepChannelTab(ChannelId, channel::Tab),
    /// Keep on the device whether the nav and the side panel show.
    KeepFolds(fold::Folds),
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
/// How soon a second Ctrl-C quits, and a q after one closing an overlay does nothing.
const QUIT_WITHIN: TimeDelta = TimeDelta::seconds(1);

/// Everything read of the ants that the views show: what the live feed reads, what the UI reads besides, and what is
/// worked out of the messages read. It holds only what the bus and the store tell, the images of attachments among it,
/// and what the human did here that they are yet to tell (the messages withdrawn, the dead letters cleared); how the
/// UI shows it, and what is typed, is [`Ui`]'s.
pub struct Data {
    pub live: State,
    /// The server, as the header names it.
    pub server: String,
    /// What each thread's messages tell of its state.
    pub facts: Facts,
    /// The top-level posts of each channel, and the replies on their threads.
    pub feeds: channel::Feeds,
    /// What happened lately in each channel.
    pub activity: channel::Activity,
    /// Which of the human's inbox messages are read, once read.
    pub marker: Option<ReadMarker>,
    /// Which channel posts the human has read, once read; never without the credentials to.
    pub channel_reads: Option<ChannelReadMarkers>,
    /// The human's notify level of each channel not at the default, on all its devices.
    pub channel_levels: Levels,
    /// The channels the human is a member of.
    pub channels: Vec<ChannelEntry>,
    /// The team's humans, once read.
    pub humans: Option<Humans>,
    /// The team directory, once read; none on a bus without `ants_teams`.
    pub teams: Option<TeamDirectory>,
    /// The reactions to the messages watched.
    pub reactions: Reactions,
    /// The tool calls of the threads, heard live or read from their timelines.
    pub calls: timeline::Threads,
    /// The steps of the trail runs reporting on the threads, heard live or read from the runs' records.
    pub steps: steps::Steps,
    /// The tool requests settled while the UI runs.
    pub settled: approvals::Settled,
    /// Who of the other humans is online, and the messages with each.
    pub people: people::People,
    /// Who messaged whom on each thread, for the delegation trees.
    pub delegations: tree::Delegations,
    /// The threads' done states, TL;DRs and spend.
    pub cards: cards::Cards,
    /// The image attachments drawn in the thread view.
    pub images: images::Images,
    /// The credentials asked for, heard where the team's account key is.
    pub credentials: credentials::Heard,
    /// The admin orders of the human's assistant held for its consent, and its answers.
    pub held: held::Held,
    /// How the ants look.
    pub looks: faces::Looks,
    /// The messages the human withdrew here, marked before the ant's notice of each comes.
    pub withdrawn: HashSet<Uuid>,
    /// The dead letters replayed or dropped here, by stream sequence: read, though the stream holds them no more.
    pub dead_cleared: HashSet<u64>,
}

/// The state of the UI that the views read and mark beside [`Data`], when drawn or taking keys: where they were drawn,
/// how the thread and channel views show, the folds, the command bar with its drafts and history, what Up selected,
/// the notices and how the digest shows. None of it comes from the bus; the views themselves, the overlays, the nav
/// and the focus are the [`Model`]'s.
#[derive(Default)]
pub struct Ui {
    /// Where the last frame drew what the mouse can point at, and the viewports' scroll.
    pub mouse: super::mouse::Regions,
    /// How the thread view shows the tool calls, kept on the device.
    pub tools: timeline::Tools,
    /// The tab chosen in each channel's view, but the default, kept on the device.
    pub channel_tabs: HashMap<ChannelId, channel::Tab>,
    /// The drafts of the conversations, the bar holding that of the one shown.
    pub drafts: command::Drafts,
    /// The texts sent from the bar, which it recalls.
    pub history: command::History,
    /// The command bar.
    pub bar: Bar,
    /// Whether the nav and the thread's side panel show.
    pub fold: fold::Fold,
    /// The conversation whose message or post Up selected, the view's single keys acting on it; else, in a thread, a
    /// channel's feed or a DM, a printable key is written in the bar ([`Model::typing`]).
    pub selecting: Option<command::Conversation>,
    /// The latest notice: an error until a key is pressed, else for [`NOTICE_FOR`].
    pub notice: Option<Notice>,
    /// The notices of the run, the earliest first, [`notices::KEPT`] at most.
    pub notices: Vec<Notice>,
    /// How far the digest is scrolled, kept while it does not show.
    pub digest: catchup::Shown,
    /// Whether the thread view draws image attachments (`[cli]` `inline_images`).
    pub inline_images: bool,
}

impl Data {
    pub fn me(&self) -> &UserId {
        &self.live.me
    }

    /// The face of `user` and a space, to go before its name, when it is an ant: one with an identity record, or in
    /// the colony, and no human.
    pub fn face(&self, user: &UserId) -> Option<[Span<'static>; 2]> {
        let ant = self.looks.known(user) || self.live.colony.iter().any(|e| e.ant == *user);
        (ant && !self.is_human(user)).then(|| [self.looks.face(user), Span::raw(" ")])
    }

    /// Whether `user` is a human: as the humans bucket says once read, else only the CLI's human.
    pub fn is_human(&self, user: &UserId) -> bool {
        user == self.me() || self.humans.as_ref().is_some_and(|humans| humans.contains(user))
    }

    /// The human's notify level of `channel`.
    pub fn level(&self, channel: &ChannelId) -> ChannelNotify {
        self.channel_levels.get(channel).copied().unwrap_or_default()
    }

    /// Whether the human's inbox message at `seq` is unread; not while the read marker is unknown.
    pub fn unread(&self, seq: u64) -> bool {
        ants_client::inbox::unread(self.marker.as_ref(), seq)
    }

    /// The title a human set on the thread, else the title an ant suggested, else its opening line; after a lock
    /// when a sealed message is on it.
    pub fn title(&self, thread: &ThreadId) -> String {
        let summary = self.live.summary(thread);
        let title = self.cards.titles.get(thread).cloned().or_else(|| {
            let summary = summary?;
            let suggested = summary.suggested.as_ref().map(|(_, title)| title.clone());
            Some(summary.title.clone().or(suggested).unwrap_or_else(|| summary.opening.clone()))
        });
        let Some(title) = title else { return thread.to_string() };
        if summary.is_some_and(|s| s.sealed) { format!("{} {title}", super::dm::LOCK) } else { title }
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
    Team(team::Page),
    Thread(thread::Page),
    Dm(dm::Page),
    Search(search::Page),
    Tree(tree::Page),
    Ant(ant::Page),
    Credentials(credentials::Page),
    Held(held::Page),
}

impl Page {
    /// The view the nav's `item` shows.
    fn of(item: &nav::Item) -> Self {
        match item {
            nav::Item::Home => Self::Home(home::Page::default()),
            nav::Item::Work(state) => Self::Work(work::Page::new(Filter { state: *state, ..Filter::default() })),
            nav::Item::Activity => Self::Work(work::Page::new(Filter { activity: true, ..Filter::default() })),
            nav::Item::Channel(channel) => Self::Channel(channel::Page::new(channel.clone())),
            nav::Item::Team(team) => Self::Team(team::Page::new(team.clone())),
            nav::Item::Ant(user) => Self::Ant(ant::Page::new(user.clone())),
            nav::Item::Person(user) => Self::Dm(dm::Page::new(user.clone())),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Home(_) => "home",
            Self::Work(_) => "work",
            Self::Channel(_) => "channel",
            Self::Team(_) => "team",
            Self::Thread(_) => "thread",
            Self::Dm(_) => "dm",
            Self::Search(_) => "search",
            Self::Tree(_) => "tree",
            Self::Ant(_) => "ant",
            Self::Credentials(_) => "credentials",
            Self::Held(_) => "held for confirmation",
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
    /// Answer a pending tool request: with the verdict, at once; without, shown in full.
    Ask(ApprovalRequest, Option<Verdict>),
    /// The command bar open on a note rejecting a pending tool request.
    Note(ApprovalRequest),
    /// Allow each of the pending tool requests once, in turn, as one batch.
    AllowAll(Vec<ApprovalRequest>),
    /// Pick a reaction to the message.
    React(Uuid),
    /// A thread's delegation tree.
    Tree(ThreadId),
    /// An ant's view, over the view.
    Ant(UserId),
    /// A team's view, over the view.
    Team(UserId),
    /// Cancel the prompt running on a thread.
    Interrupt(ThreadId),
    /// The thread, with the command bar open to reply on it.
    Reply(ThreadId),
    /// The command bar open to reply on the thread to the message quoted, @mentioning its author when an ant.
    Quote(ThreadId, Quoted, Option<UserId>),
    /// Ask how long to keep the thread off home, or wake it if a snooze holds it off.
    Snooze(ThreadId),
    /// Ask when to be reminded of the thread, or of this message on it.
    Remind(ThreadId, Option<Uuid>),
    /// Clear the thread's reminder, done.
    Unremind(ThreadId),
    /// Mark the human's messages on the thread unread again: from the message at this sequence on, else the latest.
    Unread(ThreadId, Option<u64>),
    /// The command bar, open on this text.
    Bar(String),
    /// The command bar, open on `/forward @` to forward the thread to the ant typed.
    Forward,
    /// The command bar, focused on its draft for the view shown.
    Write,
    /// Ask for a new TL;DR of the thread.
    Tldr(ThreadId),
    /// The digest of what changed since the last `ants chat` on the device ended.
    Digest,
    /// Show the thread view's tool calls the next way.
    Tools,
    /// The credentials asked for.
    Credentials,
    /// The orders and decisions held for the human's consent.
    Held,
    /// Not another view: something to do.
    Do(Action),
    /// Nothing done, for the reason the status line tells.
    Refused(String),
}

pub struct Model {
    pub data: Data,
    pub ui: Ui,
    /// The nav's selection.
    pub nav: nav::Item,
    pub focus: Focus,
    /// The nav's view, then those opened from it, the shown one last; never empty.
    pub pages: Vec<Page>,
    /// What shows over the whole screen, the latest on top.
    pub overlays: Vec<Overlay>,
    /// The tool request whose decision is being stored, not shown in full: shown so, it is an overlay
    /// ([`Model::asking`]).
    storing: Option<Asking>,
    /// The prompt in the footer taking the keys.
    pub prompt: Option<Prompt>,
    /// The catch-up summaries: the span home's first line counts, the TL;DRs asked for, the digest.
    pub catchup: catchup::Catchup,
    /// Whether the two-line pane shows instead of the full view ([`super::compact`]).
    pub compact: bool,
    /// When Ctrl-C, nothing being open and the bar empty, asked for a second one to quit.
    pub quitting: Option<DateTime<Utc>>,
    /// When q last closed the help, the notices, the digest or a tool request, or came again soon after: a q that soon
    /// never quits.
    closed_by_q: Option<DateTime<Utc>>,
}

impl Model {
    pub fn new(live: State, server: String) -> Self {
        let data = Data {
            live,
            server,
            facts: Facts::default(),
            feeds: channel::Feeds::default(),
            activity: channel::Activity::default(),
            marker: None,
            channel_reads: None,
            channel_levels: Levels::new(),
            channels: Vec::new(),
            humans: None,
            teams: None,
            reactions: Reactions::new(),
            calls: timeline::Threads::default(),
            steps: steps::Steps::default(),
            settled: approvals::Settled::default(),
            people: people::People::default(),
            delegations: tree::Delegations::default(),
            cards: cards::Cards::default(),
            images: images::Images::default(),
            withdrawn: HashSet::new(),
            dead_cleared: HashSet::new(),
            credentials: credentials::Heard::default(),
            held: held::Held::default(),
            looks: faces::Looks::default(),
        };
        let nav = nav::Item::Home;
        Self {
            data,
            ui: Ui::default(),
            pages: vec![Page::of(&nav)],
            nav,
            focus: Focus::View,
            overlays: Vec::new(),
            storing: None,
            prompt: None,
            catchup: catchup::Catchup::default(),
            compact: false,
            quitting: None,
            closed_by_q: None,
        }
    }

    pub fn page(&self) -> &Page {
        self.pages.last().expect("a view")
    }

    /// Show `page` over the view, its lists scrolled afresh.
    pub fn push(&mut self, page: Page) {
        self.ui.mouse.forget(self.pages.len());
        self.pages.push(page);
    }

    /// Close the view shown, back to the one under it.
    pub fn pop(&mut self) {
        self.pages.pop();
        self.ui.mouse.forget(self.pages.len());
    }

    /// Show a notice, unless an error waits for a key and it is none, and keep it with the notices of the run.
    pub fn notify(&mut self, text: impl Into<String>, level: Level, now: DateTime<Utc>) {
        let notice = Notice { text: text.into(), at: now, level };
        if self.ui.notices.len() == notices::KEPT {
            self.ui.notices.remove(0);
        }
        self.ui.notices.push(notice.clone());
        if level == Level::Error || self.ui.notice.as_ref().is_none_or(|n| n.level != Level::Error) {
            self.ui.notice = Some(notice);
        }
    }

    /// Whether a Ctrl-C at `now` quits, the one before having asked for it.
    pub fn quitting_at(&self, now: DateTime<Utc>) -> bool {
        self.quitting.is_some_and(|at| now - at < QUIT_WITHIN)
    }

    /// The notice to show at `now`, if one is recent.
    pub fn notice_at(&self, now: DateTime<Utc>) -> Option<&Notice> {
        self.ui.notice.as_ref().filter(|n| n.level == Level::Error || now - n.at < NOTICE_FOR)
    }

    /// Read the reactions to the messages of the view shown: a channel's posts or a thread's messages.
    fn shown_reactions(&self) -> Option<Action> {
        match self.page() {
            Page::Channel(page) => Some(Action::Reactions(self.data.feeds.watched(&page.channel))),
            Page::Thread(page) => Some(Action::Reactions(page.message_ids())),
            _ => None,
        }
    }

    /// The thread shown, if one is.
    pub fn open(&self) -> Option<&ThreadId> {
        match self.page() {
            Page::Thread(page) => Some(&page.history.thread),
            _ => None,
        }
    }

    /// Mark read the channel posts the shown view shows, once which are read is known: every post of a channel's
    /// feed through its newest, or the posts of a thread. Marks them read here at once, so asks only once.
    pub fn mark_shown(&mut self) -> Option<Action> {
        let data = &mut self.data;
        let markers = data.channel_reads.as_mut()?;
        match self.pages.last()? {
            Page::Channel(page) => {
                let newest = data.feeds.newest(&page.channel)?;
                if markers.channels.get(&page.channel).is_some_and(|m| m.below >= newest) {
                    return None;
                }
                markers.mark_through(&page.channel, newest);
                Some(Action::MarkChannel(page.channel.clone(), Mark::Through(newest)))
            }
            Page::Thread(page) => {
                let (channel, mut seqs) = data.feeds.thread_posts(&page.history.thread)?;
                seqs.retain(|seq| !markers.is_read(channel, *seq));
                if seqs.is_empty() {
                    return None;
                }
                seqs.iter().for_each(|seq| markers.mark(channel, *seq));
                Some(Action::MarkChannel(channel.clone(), Mark::Posts(seqs)))
            }
            Page::Home(_)
            | Page::Work(_)
            | Page::Dm(_)
            | Page::Search(_)
            | Page::Tree(_)
            | Page::Ant(_)
            | Page::Team(_)
            | Page::Credentials(_)
            | Page::Held(_) => None,
        }
    }

    /// The open thread, to be read again from the start: its history is to be loaded.
    pub fn open_thread(&mut self) -> Option<ThreadId> {
        let Some(Page::Thread(page)) = self.pages.last_mut() else { return None };
        let thread = page.history.thread.clone();
        *page = thread::Page::new(thread.clone());
        Some(thread)
    }

    /// Select `item` in the nav, showing its view.
    pub fn select(&mut self, item: nav::Item) -> Option<Action> {
        self.ui.mouse.forget(0);
        self.pages = vec![Page::of(&item)];
        self.nav = item;
        self.shown_reactions()
    }

    /// Run the search `query` asks for, listing its matches over the view; or why the query is refused.
    pub fn search(&mut self, query: String, now: DateTime<Utc>) -> Result<Action, String> {
        let search = search::parse(&query, now)?;
        for page in self.pages_mut::<search::Page>().filter(|page| page.found.is_none()) {
            page.found = Some(Err("stopped for a later search".to_owned()));
        }
        self.push(Page::Search(search::Page::new(query, &search)));
        self.focus = Focus::View;
        Ok(Action::Search(search))
    }

    /// Show the delegation tree of `thread` over the view.
    pub fn tree(&mut self, thread: ThreadId) {
        self.push(Page::Tree(tree::Page::new(thread)));
        self.focus = Focus::View;
    }

    /// Whether the view drawn last shows a spinner that moves, so that the screen is drawn at its frame rate.
    pub fn spinning(&self) -> bool {
        self.data.looks.animates() && matches!(self.page(), Page::Thread(page) if page.spinning.get())
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ChannelPost, Verdict};

    use super::*;
    use crate::live::{self, Link};
    use crate::test_support::live::{at, message, request, user};
    use crate::tools::Update;
    use crate::tui::acts::Acted;
    use crate::tui::reads::Read;
    use crate::tui::test_support::{model, post};

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
    fn q_closes_the_help_without_quitting_however_fast_it_comes_again() {
        let mut m = model();
        assert_eq!(m.key(Key::Char('?'), at(0)), None);
        assert!(m.help().is_some());
        assert_eq!(m.key(Key::Char('q'), at(0)), None, "q closes the help first");
        assert!(m.help().is_none());
        let soon = at(0) + TimeDelta::milliseconds(600);
        assert_eq!(m.key(Key::Char('q'), soon), None, "a q right after does nothing");
        assert_eq!(m.key(Key::Char('q'), soon + TimeDelta::milliseconds(600)), None, "nor one right after that");
        assert_eq!(m.key(Key::Char('q'), at(3)), Some(Action::Quit));
    }

    #[test]
    fn ctrl_c_closes_then_empties_the_bar_then_quits_on_a_second_within_a_second() {
        let mut m = model();
        m.key(Key::Char('?'), at(0));
        assert_eq!(m.key(Key::Interrupt, at(0)), None, "Ctrl-C closes the help");
        assert!(m.help().is_none());
        m.ui.bar.set_text("draft");
        assert_eq!(m.key(Key::Char('q'), at(2)), None, "q quits with an empty bar only");
        assert_eq!(m.ui.bar.text(), "draft");
        assert_eq!(m.key(Key::Interrupt, at(3)), None);
        assert_eq!(m.ui.bar.text(), "", "the first Ctrl-C empties the bar");
        assert_eq!(m.key(Key::Interrupt, at(3) + TimeDelta::milliseconds(500)), None, "the second asks for a third");
        assert!(m.quitting_at(at(4)));
        assert!(!m.quitting_at(at(5)), "for a second");
        assert_eq!(m.key(Key::Interrupt, at(5)), None, "too late: it asks again");
        assert_eq!(m.key(Key::Interrupt, at(5) + TimeDelta::milliseconds(900)), Some(Action::Quit));
    }

    #[test]
    fn a_key_between_two_ctrl_c_keeps_the_ui_open() {
        let mut m = model();
        assert_eq!(m.key(Key::Interrupt, at(0)), None);
        m.key(Key::Down, at(0));
        assert_eq!(m.key(Key::Interrupt, at(0)), None);
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
        let first = loaded[0].id.unwrap();
        let shown = m.take(live::Event::History(thread.clone(), Ok(loaded)), at(3));
        assert_eq!(shown, [Action::Reactions(vec![first])], "the reactions to its messages are read");
        let reply = message(3, &thread, "dev-a", "alice", "done");
        let reactions = Action::Reactions(vec![first, reply.id.unwrap()]);
        assert_eq!(m.stored(vec![reply], at(4)), [Action::MarkRead(thread.clone()), reactions]);
        assert!(m.stored(vec![message(4, &other, "dev-b", "alice", "elsewhere")], at(5)).is_empty());
        let Page::Thread(page) = m.page() else { panic!("not a thread") };
        assert_eq!(page.history.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 3]);
        assert_eq!(m.open_thread(), Some(thread), "a refresh reads the open thread again");
        m.key(Key::Esc, at(6));
        assert_eq!((m.page().name(), m.focus), ("work", Focus::View));
    }

    #[test]
    fn u_marks_the_selected_message_and_later_ones_unread_and_closes_the_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.select(nav::Item::Work(None));
        let messages = vec![
            message(1, &thread, "alice", "dev-a", "deploy"),
            message(2, &thread, "dev-a", "alice", "deploying"),
            message(3, &thread, "dev-a", "alice", "done"),
        ];
        m.stored(messages.clone(), at(1));
        m.data.marker = Some(ReadMarker { below: 3, ..Default::default() });
        assert_eq!(m.key(Key::Char('u'), at(2)), Some(Action::MarkUnread(thread.clone(), vec![3])), "the latest");
        assert!(m.data.unread(3) && !m.data.unread(2));

        m.key(Key::Enter, at(3));
        m.take(live::Event::History(thread.clone(), Ok(messages)), at(3));
        m.key(Key::Up, at(4));
        m.key(Key::Up, at(4));
        assert_eq!(m.key(Key::Char('u'), at(4)), Some(Action::MarkUnread(thread, vec![2, 3])));
        assert_eq!(m.page().name(), "work", "the thread closes so as not to mark them read again");
        assert!(m.data.unread(2));
    }

    #[test]
    fn tool_calls_go_to_their_threads_timelines_and_what_was_done_shows() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        let call = crate::tui::test_support::stored(2, &thread, "dev-a", Uuid::new_v4(), "c1", "completed", 1);
        let loaded = vec![message(1, &thread, "alice", "dev-a", "deploy"), call.clone()];
        m.take(live::Event::History(thread.clone(), Ok(loaded)), at(2));
        m.stored(vec![call, message(3, &thread, "dev-a", "alice", "done")], at(3));
        let Page::Thread(page) = m.page() else { panic!("not a thread") };
        assert_eq!(page.history.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 3], "no call among them");
        assert_eq!(m.data.calls.get(&thread).map(|c| c.calls().len()), Some(1), "the call once");
        assert_eq!(m.open(), Some(&thread));

        let mut about = crate::tui::acts::About::default();
        about.marks.archive(&thread, 3);
        m.acts_event(Acted::About(thread.clone(), Ok(about.clone())), at(4));
        assert!(m.data.live.order.is_empty(), "archived, the thread leaves the work");
        let Page::Thread(page) = m.page() else { panic!("not a thread") };
        assert_eq!(page.about, Some(about));
        m.acts_event(Acted::Changed(thread.clone(), Change::Archive(true), Ok(true)), at(5));
        assert!(
            m.notice_at(at(5))
                .is_some_and(|n| n.text == "thread deploy archived; its messages and its work stay as they are")
        );
    }

    #[test]
    fn ctrl_f_finds_in_the_open_thread_and_elsewhere_opens_the_bar_on_search() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.key(Key::Tab, at(0));
        assert_eq!(m.key(Key::Ctrl('f'), at(0)), None);
        assert_eq!((m.ui.bar.focused, m.ui.bar.text()), (true, "/search ".to_owned()));
        m.key(Key::Esc, at(0));
        let messages =
            vec![message(1, &thread, "alice", "dev-a", "deploy"), message(2, &thread, "dev-a", "alice", "deployed")];
        m.stored(messages.clone(), at(1));
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        m.take(live::Event::History(thread, Ok(messages)), at(1));
        m.focus = Focus::View;
        let footer = |m: &Model| {
            let shown = crate::test_support::render::rendered(100, 20, |f| super::super::frame::draw(f, m, at(2)));
            shown.lines().last().unwrap_or_default().to_owned()
        };
        m.key(Key::Ctrl('f'), at(2));
        m.paste("depl", at(2));
        for c in "oy".chars() {
            assert_eq!(m.key(Key::Char(c), at(2)), None);
        }
        assert!(!m.ui.bar.focused, "the footer takes the text");
        assert!(footer(&m).starts_with(" find: deploy_ · 3 found"), "with the title: {}", footer(&m));
        m.key(Key::Enter, at(2));
        assert!(footer(&m).contains(" of 3"), "{}", footer(&m));
        m.key(Key::Esc, at(2));
        assert_eq!((m.page().name(), m.ui.bar.focused), ("thread", false), "Esc ends the find, the thread stays");
        assert!(!footer(&m).starts_with(" find"), "{}", footer(&m));
    }

    #[test]
    fn ctrl_f_searches_and_a_match_opens_its_thread_at_it() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.key(Key::Tab, at(0));
        assert_eq!(m.key(Key::Ctrl('f'), at(0)), None);
        for c in "deploy since:soon".chars() {
            assert_eq!(m.key(Key::Char(c), at(0)), None, "keys go to the command bar");
        }
        assert_eq!(m.key(Key::Enter, at(0)), None);
        let error = m.ui.bar.status.as_ref().map(|s| s.text.clone());
        assert!(error.is_some_and(|e| e.starts_with("invalid time \"soon\"")), "a refused query stays");
        assert_eq!(m.ui.bar.text(), "/search deploy since:soon");
        m.key(Key::Ctrl('w'), at(0));
        let Some(Action::Search(search)) = m.key(Key::Enter, at(0)) else { panic!("no search") };
        assert_eq!(search.grep.as_deref(), Some("deploy"));
        assert_eq!((m.ui.bar.focused, m.page().name(), m.focus), (false, "search", Focus::View));

        m.key(Key::Ctrl('f'), at(1));
        m.key(Key::Char('d'), at(1));
        assert!(matches!(m.key(Key::Enter, at(1)), Some(Action::Search(_))));
        let Page::Search(earlier) = &m.pages[1] else { panic!("not a search") };
        assert_eq!(earlier.found, Some(Err("stopped for a later search".to_owned())));
        let found = message(1, &thread, "dev-a", "alice", "deployed");
        m.search_event(Ok((vec![found], 0)));
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Open(thread)));
        assert_eq!(m.page().name(), "thread");
        m.key(Key::Esc, at(3));
        assert_eq!(m.page().name(), "search", "Esc goes back to the matches");
        m.key(Key::Ctrl('f'), at(4));
        m.key(Key::Esc, at(4));
        assert!(!m.ui.bar.focused, "Esc leaves the bar");
        m.key(Key::Ctrl('f'), at(4));
        assert_eq!((m.ui.bar.focused, m.ui.bar.text()), (true, "/search ".to_owned()), "a draft in the bar stays");
        m.key(Key::Esc, at(4));
        assert_eq!(m.key(Key::Char('q'), at(4)), None, "q keeps the draft");
    }

    #[test]
    fn a_shown_channel_or_thread_marks_its_posts_read_once() {
        let ops: ChannelId = "ops".parse().unwrap();
        let down = ChannelPost::new(ops.clone(), user("bob"), "the site is down");
        let lunch = ChannelPost::new(ops.clone(), user("bob"), "lunch?");
        let on = |text| ChannelPost::on_thread(ops.clone(), down.thread_id.clone(), user("dev-a"), text);
        let mut m = model();
        m.stored(vec![post(1, &down), post(2, &lunch), post(3, &on("looking"))], at(1));
        m.select(nav::Item::Channel(ops.clone()));
        assert_eq!(m.mark_shown(), None, "not before which posts are read is known");
        m.read(Read::ChannelMarkers(Ok(ChannelReadMarkers::default())), at(1));
        assert_eq!(m.mark_shown(), Some(Action::MarkChannel(ops.clone(), Mark::Through(3))));
        assert_eq!(m.mark_shown(), None, "marked here at once");

        m.data.channel_reads = Some(ChannelReadMarkers::default());
        m.pages.push(Page::Thread(thread::Page::new(down.thread_id.clone())));
        assert_eq!(m.mark_shown(), Some(Action::MarkChannel(ops.clone(), Mark::Posts(vec![1, 3]))));
        m.stored(vec![post(4, &on("fixed"))], at(2));
        assert_eq!(m.mark_shown(), Some(Action::MarkChannel(ops.clone(), Mark::Posts(vec![4]))));
        assert_eq!(m.mark_shown(), None);
        m.key(Key::Esc, at(3));
        assert_eq!(m.mark_shown(), Some(Action::MarkChannel(ops.clone(), Mark::Through(4))), "the feed shows #2");
        m.select(nav::Item::Home);
        m.stored(vec![post(5, &lunch)], at(4));
        assert_eq!(m.mark_shown(), None, "only what is shown");
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

    /// Reads the hub's JetStream does not answer, as through a leaf node whose uplink is down, show the hub out of
    /// reach rather than errors; the next stream read it answers brings it back and reads what was missed.
    #[test]
    fn the_hub_out_of_reach_shows_in_the_header_not_as_errors() {
        use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind, RequestError, RequestErrorKind};
        let unanswered = || {
            let e = GetStreamError::with_source(
                GetStreamErrorKind::Request,
                RequestError::new(RequestErrorKind::NoResponders),
            );
            anyhow::Error::from(e).context("opening bucket ants_channels")
        };
        let mut m = model();
        assert_eq!(m.link(true, at(1)), None);
        m.read(Read::Directory(Err(unanswered())), at(2));
        m.read(Read::Marker(Err(unanswered())), at(3));
        m.take(live::Event::Stored(Err(unanswered())), at(3));
        assert_eq!(m.data.live.link, Link::Unreached(at(2)));
        assert!(m.notice_at(at(3)).is_none(), "{:?}", m.notice_at(at(3)));
        assert_eq!(m.link(true, at(4)), None, "the server connected does not bring the hub back");
        let shown = format!("{}", crate::tui::style::link(m.data.live.link, at(9), ""));
        assert!(shown.contains("hub out of reach 7s, retrying"), "{shown}");

        assert!(m.take(live::Event::Stored(Ok(Vec::new())), at(6)).contains(&Action::Refresh));
        assert_eq!(m.data.live.link, Link::Connected);
        assert!(m.notice_at(at(6)).is_some_and(|n| n.text.starts_with("hub reached again")));
        assert!(!m.take(live::Event::Stored(Ok(Vec::new())), at(7)).contains(&Action::Refresh));
        m.read(Read::Directory(Err(anyhow::anyhow!("permissions violation"))), at(8));
        assert!(m.notice_at(at(8)).is_some_and(|n| n.text == "reading the channels: permissions violation"));
        assert_eq!(m.data.live.link, Link::Connected);
    }

    #[test]
    fn the_humans_channels_are_those_it_is_a_member_of() {
        let mut m = model();
        let channel = |name: &str, members: &[&str]| ChannelEntry {
            channel: name.parse().unwrap(),
            members: members.iter().map(|m| user(m)).collect(),
            messenger: None,
            owner: None,
            description: None,
        };
        let channels = vec![channel("ops", &["alice", "dev-a"]), channel("sales", &["bob"])];
        m.read(Read::Directory(Ok((channels, Humans::default()))), at(0));
        assert_eq!(m.data.channels.iter().map(|c| c.channel.as_str()).collect::<Vec<_>>(), ["ops"]);
        assert!(!m.data.is_human(&user("bob")), "the humans read replace those known");
        assert!(m.data.is_human(&user("alice")), "the model's own human");
    }
}
