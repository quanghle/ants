//! What `ants chat` shows, and how keys and what is read change it, apart from the terminal and the connection: what
//! is known of the ants ([`Data`]), the nav's selection, the views opened from it, the focus, the help and the latest
//! notice. Keys and changes return the [`Action`]s the UI then performs.

use ants_daemon::reactions::Reactions;
use ants_proto::{
    ApprovalRequest, ChannelEntry, ChannelId, ChannelReadMarkers, Humans, Reaction, ReadMarker, ThreadId, UserId, Uuid,
    Verdict,
};
use chrono::{DateTime, TimeDelta, Utc};

use super::acts::{Acted, Change};
use super::approvals::{self, Answer, Asking};
use super::command::{Bar, Tone};
use super::exec::Job;
use super::interrupt::{Confirm, Intent};
use super::react::{self, Pick, Picker};
use super::reads::{Mark, Read};
use super::work::{Facts, Filter};
use super::{cards, catchup, channel, dm, home, images, nav, people, search, snooze, thread, timeline, tree, work};
use crate::live::{self, State};
use crate::thread::{Entry, Kind};
use crate::tools::Update;

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
    /// Mark the human's posts of the channel read on all its devices, as `ants channel read` does.
    MarkChannel(ChannelId, Mark),
    /// Read the reactions to these messages, and again every while.
    Reactions(Vec<Uuid>),
    /// Make a change to how the human holds the thread, the last message, post or note on which has this sequence;
    /// marking it done marks the human's inbox messages on it read too.
    Change(ThreadId, Change, u64),
    /// Copy the text to the terminal's clipboard.
    Copy(String),
    /// Send a message to another human on a new thread, as `ants ask @human` does.
    Send(dm::Draft),
    /// Read again what may have missed changes.
    Refresh,
    /// Run the command bar's job, for the bar's text.
    Run(Job, String),
    /// Store a decision on a tool request.
    Decide(Box<ApprovalRequest>, Verdict),
    /// Edit the command bar's text in the editor.
    Edit(String),
    /// React to the message so, or with true take the reaction back, as `ants react` does.
    React(Uuid, Reaction, bool),
    /// Find the messages matching the search, replacing any search running.
    Search(crate::search::Search),
    /// Cancel the prompt each ant runs on its thread, as `ants cancel` does.
    CancelSubtree(Vec<(UserId, ThreadId)>),
    /// Ask for a catch-up summary.
    Summarise(catchup::Ask),
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
    /// Which channel posts the human has read, once read; never without the credentials to.
    pub channel_reads: Option<ChannelReadMarkers>,
    /// The channels the human is a member of.
    pub channels: Vec<ChannelEntry>,
    /// The team's humans, once read.
    pub humans: Option<Humans>,
    /// The reactions to the messages watched.
    pub reactions: Reactions,
    /// The tool calls of the threads, heard live or read from their timelines.
    pub calls: timeline::Threads,
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

    /// The thread's title, else the title an ant suggested, else its opening line; after a lock when a sealed message
    /// is on it.
    pub fn title(&self, thread: &ThreadId) -> String {
        let Some(summary) = self.live.summary(thread) else { return thread.to_string() };
        let title = summary
            .title
            .clone()
            .or_else(|| summary.suggested.as_ref().map(|(_, title)| title.clone()))
            .unwrap_or_else(|| summary.opening.clone());
        if summary.sealed { format!("{} {title}", super::dm::LOCK) } else { title }
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
    Tree(tree::Page),
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
            Self::Tree(_) => "tree",
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
    /// Answer a pending tool request: with the verdict, once confirmed; without, shown in full.
    Ask(ApprovalRequest, Option<Verdict>),
    /// Pick a reaction to the message.
    React(Uuid),
    /// A thread's delegation tree.
    Tree(ThreadId),
    /// Steer or cancel the prompt running on a thread.
    Interrupt(Intent, ThreadId),
    /// The thread, with the command bar open to reply on it.
    Reply(ThreadId),
    /// Ask how long to keep the thread off home.
    Snooze(ThreadId),
    /// The command bar, open on this text.
    Bar(String),
    /// Ask for a new TL;DR of the thread.
    Tldr(ThreadId),
    /// The digest of what changed since the last `ants chat` on the device ended.
    Digest,
    /// Not another view: something to do.
    Do(Action),
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
    /// The tool request being answered.
    pub asking: Option<Asking>,
    /// The reaction being picked.
    pub picking: Option<Picker>,
    /// The steer or cancel waiting for y while an ant runs a tool call.
    pub interrupting: Option<Confirm>,
    /// How long to snooze a thread from home, being asked.
    pub snoozing: Option<snooze::Prompt>,
    /// The catch-up summaries: the span home's first line counts, the TL;DRs asked for, the digest.
    pub catchup: catchup::Catchup,
    /// Whether the two-line pane shows instead of the full view ([`super::compact`]).
    pub compact: bool,
}

impl Model {
    pub fn new(live: State, server: String) -> Self {
        let data = Data {
            live,
            server,
            facts: Facts::default(),
            feeds: channel::Feeds::default(),
            marker: None,
            channel_reads: None,
            channels: Vec::new(),
            humans: None,
            reactions: Reactions::new(),
            calls: timeline::Threads::default(),
            settled: approvals::Settled::default(),
            people: people::People::default(),
            delegations: tree::Delegations::default(),
            cards: cards::Cards::default(),
            images: images::Images::default(),
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
            asking: None,
            picking: None,
            interrupting: None,
            snoozing: None,
            catchup: catchup::Catchup::default(),
            compact: false,
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
            live::Event::Status(event) => {
                self.data.live.status(&event, now);
                self.data.calls.status(&event);
            }
            live::Event::Following => self.data.live.approvals_reset(),
            live::Event::Approval(update) => self.approval(update, now),
            live::Event::Decided(outcome) => self.answered(outcome, now),
            live::Event::FollowEnded(Ok(())) => {}
            live::Event::FollowEnded(Err(e)) => {
                self.notify(format!("following the tool requests: {e:#}"), Level::Error, now);
            }
            live::Event::Roster(Ok(entries)) => self.data.live.roster(entries, now),
            live::Event::Roster(Err(e)) => self.notify(format!("reading the roster: {e:#}"), Level::Error, now),
            live::Event::History(thread, read) => {
                let read = read.map(|entries| self.tools(entries));
                for entry in read.iter().flatten() {
                    self.data.delegations.add(entry);
                }
                let page = self.pages.iter_mut().rev().find_map(|page| match page {
                    Page::Thread(page) if page.history.thread == thread => Some(page),
                    _ => None,
                });
                if let Some(page) = page
                    && let Err(e) = page.history.loaded(&thread, read)
                {
                    self.notify(format!("reading thread {thread}: {e:#}"), Level::Error, now);
                }
                if self.open() == Some(&thread) {
                    return self.shown_reactions().into_iter().collect();
                }
            }
        }
        Vec::new()
    }

    /// Read the reactions to the messages of the view shown: a channel's posts or a thread's messages.
    fn shown_reactions(&self) -> Option<Action> {
        match self.page() {
            Page::Channel(page) => Some(Action::Reactions(self.data.feeds.watched(&page.channel))),
            Page::Thread(page) => Some(Action::Reactions(page.message_ids())),
            _ => None,
        }
    }

    /// Take the messages read from the stream. Marks read the human's new messages on an open thread, and reads the
    /// reactions to a shown channel's new posts.
    pub fn stored(&mut self, entries: Vec<Entry>, now: DateTime<Utc>) -> Vec<Action> {
        let entries = self.tools(entries);
        let data = &mut self.data;
        data.live.stored(&entries, now);
        let me = data.live.me.clone();
        let mut posted = false;
        for entry in &entries {
            data.facts.add(entry, &me);
            data.people.dms.add(entry, &me);
            posted |= data.feeds.add(entry);
            data.delegations.add(entry);
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
        let shown = match self.page() {
            Page::Channel(_) => posted,
            Page::Thread(page) => entries.iter().any(|e| e.thread_id == page.history.thread),
            _ => false,
        };
        actions.extend(self.shown_reactions().filter(|_| shown));
        actions
    }

    /// Take the tool calls among `entries` into those of their threads: the other entries.
    fn tools(&mut self, entries: Vec<Entry>) -> Vec<Entry> {
        let (tools, entries): (Vec<Entry>, Vec<Entry>) = entries.into_iter().partition(|e| e.kind == Kind::Tool);
        for tool in &tools {
            self.data.calls.stored(tool);
        }
        entries
    }

    /// Take word from following the tool requests: a settled request keeps its card a while.
    fn approval(&mut self, update: Update, now: DateTime<Utc>) {
        self.data.live.approval(&update);
        if let Update::Resolved(request, state) = update {
            if let Some(asking) = &mut self.asking
                && !asking.settled(&request, &state)
            {
                self.asking = None;
                let how = approvals::how(&state, &request.ant);
                self.notify(format!("request {} {how}", crate::tools::qualified(&request)), Level::Info, now);
            }
            self.data.settled.add(request, state, now);
        }
    }

    /// Take the outcome of storing a decision: the line saying so, or why it was not stored, the first decision
    /// winning.
    fn answered(&mut self, outcome: anyhow::Result<String>, now: DateTime<Utc>) {
        let Some(asking) = self.asking.as_mut().filter(|a| a.storing) else {
            return self.decided(outcome);
        };
        asking.storing = false;
        asking.verdict = None;
        if outcome.is_ok() || !asking.full {
            self.asking = None;
        }
        match outcome {
            Ok(line) => self.notify(line, Level::Info, now),
            Err(e) => self.notify(format!("{e:#}"), Level::Error, now),
        }
    }

    /// Take `key` while a tool request is being answered.
    fn answer(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let asking = self.asking.as_mut()?;
        match asking.key(key) {
            Answer::Keep => {}
            Answer::Close(line) => {
                self.asking = None;
                if let Some(line) = line {
                    self.notify(line, Level::Info, now);
                }
            }
            Answer::Note(line) => self.notify(line, Level::Info, now),
            Answer::Refused(refusal) => self.notify(refusal, Level::Error, now),
            Answer::Decide(verdict) => {
                return self.asking.as_ref().map(|a| Action::Decide(Box::new(a.request.clone()), verdict));
            }
            Answer::Open(thread) => {
                self.asking = None;
                if self.open() != Some(&thread) {
                    self.focus = Focus::View;
                    return self.go(To::Thread(thread), now);
                }
            }
        }
        None
    }

    /// The thread shown, if one is.
    pub fn open(&self) -> Option<&ThreadId> {
        match self.page() {
            Page::Thread(page) => Some(&page.history.thread),
            _ => None,
        }
    }

    /// Take what was done to or read of the open thread besides its messages.
    pub fn acted(&mut self, acted: Acted, now: DateTime<Utc>) {
        match acted {
            Acted::About(thread, Ok(about)) => {
                if self.data.live.marks != about.marks {
                    self.data.live.marks.clone_from(&about.marks);
                    self.data.live.reorder(now);
                }
                match &about.done {
                    Some(done) => self.data.cards.done.insert(thread.clone(), done.clone()),
                    None => self.data.cards.done.remove(&thread),
                };
                for page in &mut self.pages {
                    if let Page::Thread(page) = page
                        && page.history.thread == thread
                    {
                        page.about = Some(about.clone());
                    }
                }
            }
            Acted::About(thread, Err(e)) => tracing::debug!(%thread, "reading the thread's marks: {e:#}"),
            Acted::Changed(thread, change, Ok(changed)) => {
                let line =
                    format!("thread {} {}", crate::excerpt(&self.data.title(&thread), 40), change.done_line(changed));
                self.notify(line, Level::Info, now);
            }
            Acted::Changed(thread, _, Err(e)) => {
                self.notify(format!("changing thread {thread}: {e:#}"), Level::Error, now);
            }
        }
    }

    /// Take what the UI read besides the live feed.
    pub fn read(&mut self, read: Read, now: DateTime<Utc>) {
        let data = &mut self.data;
        match read {
            Read::Marker(Ok(marker)) => data.marker = Some(marker),
            Read::Marker(Err(e)) => self.notify(format!("reading what was read: {e:#}"), Level::Error, now),
            Read::ChannelMarkers(Ok(markers)) | Read::ChannelMarked(_, Ok(markers)) => {
                data.channel_reads = Some(markers);
            }
            Read::ChannelMarkers(Err(e)) => {
                self.notify(format!("reading which channel posts were read: {e:#}"), Level::Error, now);
            }
            Read::ChannelMarked(channel, Err(e)) => {
                self.notify(format!("marking #{channel} read: {e:#}"), Level::Error, now);
            }
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
            Read::Reacted(message, reaction, remove, Ok(changed)) => {
                self.notify(react::done_line(message, &reaction, remove, changed), Level::Info, now);
            }
            Read::Reacted(message, reaction, _, Err(e)) => {
                self.notify(format!("reacting {reaction} to message {message}: {e:#}"), Level::Error, now);
            }
        }
    }

    /// Take what was read of the threads besides their messages.
    pub fn cards(&mut self, read: cards::Read, now: DateTime<Utc>) {
        if let Some(failed) = self.data.cards.take(read) {
            self.notify(failed, Level::Error, now);
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
            Page::Home(_) | Page::Work(_) | Page::Dm(_) | Page::Search(_) | Page::Tree(_) => None,
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
        if self.asking.as_ref().is_some_and(Asking::takes_keys) && !self.help {
            return self.answer(key, now);
        }
        if self.compact {
            return self.compact_key(key, now);
        }
        if self.help {
            self.help = false;
            return None;
        }
        if self.catchup.shown() {
            return self.digest_key(key);
        }
        if self.interrupting.is_some() {
            return self.confirm_key(key);
        }
        if self.snoozing.is_some() {
            return self.snooze_key(key, now);
        }
        if let Some(picker) = &mut self.picking {
            return match picker.key(key) {
                Pick::Typing => None,
                Pick::Close => {
                    self.picking = None;
                    None
                }
                Pick::React(message, reaction) => {
                    self.picking = None;
                    let reacted = self.data.reactions.get(&message).map_or(&[][..], Vec::as_slice);
                    let remove = react::reacted(reacted, self.data.me(), &reaction);
                    Some(Action::React(message, reaction, remove))
                }
            };
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
        self.shown_reactions()
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

    pub(super) fn view_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        if key == Key::Esc {
            if self.pages.len() > 1 {
                self.pages.pop();
                return self.shown_reactions();
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
            Page::Thread(page) => page.key(key, data, now),
            Page::Dm(page) => page.key(key, data),
            Page::Search(page) => page.key(key),
            Page::Tree(page) => page.key(key, data, now),
        }?;
        self.go(to, now)
    }

    /// Go where a view's key leads.
    fn go(&mut self, to: To, now: DateTime<Utc>) -> Option<Action> {
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
            To::React(message) => {
                self.picking = Some(Picker::new(message));
                None
            }
            To::Ask(request, verdict) => {
                match Asking::new(request, verdict) {
                    Ok(asking) => self.asking = Some(asking),
                    Err(refusal) => self.notify(refusal, Level::Error, now),
                }
                None
            }
            To::Tree(thread) => {
                self.tree(thread);
                None
            }
            To::Interrupt(intent, thread) => self.interrupt(intent, thread),
            To::Reply(thread) => {
                self.bar.focused = true;
                self.go(To::Thread(thread), now)
            }
            To::Snooze(thread) => {
                self.snooze(thread);
                None
            }
            To::Bar(text) => {
                self.bar.set_text(&text);
                self.bar.focused = true;
                None
            }
            To::Tldr(thread) => self.tldr(thread),
            To::Digest => self.home_digest(now),
            To::Do(Action::Run(Job::Open(listed), text)) => {
                self.bar.set_status(format!("opening {}…", listed.attachment.name), Tone::Busy);
                Some(Action::Run(Job::Open(listed), text))
            }
            To::Do(action @ Action::Run(..)) => {
                self.bar.set_status("running…", Tone::Busy);
                Some(action)
            }
            To::Do(action) => Some(action),
        }
    }

    /// Show the delegation tree of `thread` over the view.
    pub fn tree(&mut self, thread: ThreadId) {
        self.pages.push(Page::Tree(tree::Page::new(thread)));
        self.focus = Focus::View;
    }

    /// The root of the delegation tree shown, and the threads known of it, for its links and spend to be read.
    pub fn tree_shown(&self) -> Option<(ThreadId, Vec<ThreadId>)> {
        let Page::Tree(page) = self.page() else { return None };
        Some((page.root.clone(), self.data.delegations.threads(&page.root)))
    }

    /// Take in what the delegation trees' reads and cancels brought.
    pub fn tree_event(&mut self, event: tree::Event, now: DateTime<Utc>) {
        match event {
            tree::Event::Read(root, links, spend) => {
                for page in &mut self.pages {
                    if let Page::Tree(page) = page
                        && page.root == root
                    {
                        if let Some(links) = &links {
                            page.links.clone_from(links);
                        }
                        if spend.is_some() {
                            page.spend.clone_from(&spend);
                        }
                    }
                }
            }
            tree::Event::Cancelled(outcomes) => {
                let (text, level) = tree::notice(&outcomes);
                self.notify(text, level, now);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use ants_proto::{ChannelPost, HumanEntry, Verdict};

    use super::*;
    use crate::live::Link;
    use crate::live::tests::{at, message, request, user};
    use crate::tui::channel::tests::post;

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
    fn tool_calls_go_to_their_threads_timelines_and_what_was_done_shows() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        let call = crate::tui::timeline::tests::stored(2, &thread, "dev-a", Uuid::new_v4(), "c1", "completed", 1);
        let loaded = vec![message(1, &thread, "alice", "dev-a", "deploy"), call.clone()];
        m.take(live::Event::History(thread.clone(), Ok(loaded)), at(2));
        m.stored(vec![call, message(3, &thread, "dev-a", "alice", "done")], at(3));
        let Page::Thread(page) = m.page() else { panic!("not a thread") };
        assert_eq!(page.history.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 3], "no call among them");
        assert_eq!(m.data.calls.get(&thread).map(|c| c.calls().len()), Some(1), "the call once");
        assert_eq!(m.open(), Some(&thread));

        let mut about = crate::tui::acts::About::default();
        about.marks.archive(&thread, 3);
        m.acted(Acted::About(thread.clone(), Ok(about.clone())), at(4));
        assert!(m.data.live.order.is_empty(), "archived, the thread leaves the work");
        let Page::Thread(page) = m.page() else { panic!("not a thread") };
        assert_eq!(page.about, Some(about));
        m.acted(Acted::Changed(thread.clone(), Change::Archive(true), Ok(true)), at(5));
        assert!(
            m.notice_at(at(5))
                .is_some_and(|n| n.text == "thread deploy archived; its messages and its work stay as they are")
        );
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
