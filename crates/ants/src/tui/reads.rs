//! What `ants chat` reads besides the live feed ([`crate::live::Feed`]), every while and on demand: which of the
//! human's inbox messages and channel posts are read, the channels, their notify levels and the humans, and the reactions to the messages
//! shown, again at once as any of them changes; and marking a thread's messages and a channel's posts read, and
//! reacting to a message as `ants react` does.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use ants_daemon::reactions::{self, Change, Reactions};
use ants_proto::{
    ChannelEntry, ChannelId, ChannelNotify, ChannelReadMarkers, Humans, JsDomain, Reaction, ReadMarker, ThreadId,
    UserId, Uuid,
};
use anyhow::Result;
use async_nats::jetstream;
use futures::future::LocalBoxFuture;
use futures::stream::LocalBoxStream;
use futures::{FutureExt as _, StreamExt as _};

use super::model::Action;
use crate::channel::reads::ChannelReads;
use crate::inbox::{Inbox, Select};
use crate::live::maybe;

/// How often the read markers are read, for what other devices of the human marked read.
const MARKER_EVERY: Duration = Duration::from_secs(5);
/// How often the channels, their notify levels, the humans and the reactions are read.
const DIRECTORY_EVERY: Duration = Duration::from_secs(10);

type Directory = (Vec<ChannelEntry>, Humans);
/// The notify level of each channel not at the default.
pub type Levels = BTreeMap<ChannelId, ChannelNotify>;

/// Which posts of a channel to mark read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mark {
    /// Every post at or below this sequence.
    Through(u64),
    /// The posts at these sequences.
    Posts(Vec<u64>),
}

/// Something read.
pub enum Read {
    Marker(Result<ReadMarker>),
    /// Which channel posts are read.
    ChannelMarkers(Result<ChannelReadMarkers>),
    /// The channel's posts were marked read, or not: the markers as changed.
    ChannelMarked(ChannelId, Result<ChannelReadMarkers>),
    /// Every channel, and the team's humans.
    Directory(Result<Directory>),
    /// The human's notify levels of the channels.
    Levels(Levels),
    /// The thread's messages were marked read, or not.
    Marked(ThreadId, Result<()>),
    /// The thread's messages were marked unread again, or not.
    Unmarked(ThreadId, Result<()>),
    Reactions(Reactions),
    /// The human reacted to the message so, or took the reaction back: whether that changed anything.
    Reacted(Uuid, Reaction, bool, Result<bool>),
}

/// A reaction to make, or with true to take back.
type Reacting = (Uuid, Reaction, bool);

pub struct Reads {
    js: jetstream::Context,
    domain: Option<JsDomain>,
    me: UserId,
    markers: tokio::time::Interval,
    directories: tokio::time::Interval,
    marker: Option<LocalBoxFuture<'static, Result<ReadMarker>>>,
    directory: Option<LocalBoxFuture<'static, Result<Directory>>>,
    /// The threads to mark read, after the one in flight.
    to_mark: VecDeque<ThreadId>,
    marking: Option<LocalBoxFuture<'static, (ThreadId, Result<()>)>>,
    /// The messages to mark unread again, by thread, after those in flight.
    to_unmark: VecDeque<(ThreadId, Vec<u64>)>,
    unmarking: Option<LocalBoxFuture<'static, (ThreadId, Result<()>)>>,
    /// Whether the human's credentials may read and mark its channel read markers.
    channels: bool,
    channel_marker: Option<LocalBoxFuture<'static, Result<ChannelReadMarkers>>>,
    levels: Option<LocalBoxFuture<'static, Result<Levels>>>,
    /// The channel posts to mark read, after those in flight.
    channels_to_mark: VecDeque<(ChannelId, Mark)>,
    marking_channel: Option<LocalBoxFuture<'static, (ChannelId, Result<ChannelReadMarkers>)>>,
    /// The messages whose reactions are read.
    watched: Vec<Uuid>,
    reactions: Option<LocalBoxFuture<'static, Result<Reactions>>>,
    /// Every reaction added or removed, to read again those to the messages watched.
    changes: LocalBoxStream<'static, Change>,
    /// The reactions to make, after the one in flight.
    to_react: VecDeque<Reacting>,
    reacting: Option<LocalBoxFuture<'static, (Reacting, Result<bool>)>>,
}

impl Reads {
    /// What `me` reads; `channels`, whether its channel read markers too.
    pub fn new(js: jetstream::Context, domain: Option<JsDomain>, me: UserId, channels: bool) -> Self {
        Self {
            changes: changes(js.clone()),
            to_react: VecDeque::new(),
            reacting: None,
            js,
            domain,
            me,
            markers: tokio::time::interval(MARKER_EVERY),
            directories: tokio::time::interval(DIRECTORY_EVERY),
            marker: None,
            directory: None,
            to_mark: VecDeque::new(),
            marking: None,
            to_unmark: VecDeque::new(),
            unmarking: None,
            channels,
            channel_marker: None,
            levels: None,
            channels_to_mark: VecDeque::new(),
            marking_channel: None,
            watched: Vec::new(),
            reactions: None,
        }
    }

    /// The next thing read. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Read {
        loop {
            if self.marking.is_none()
                && let Some(thread) = self.to_mark.pop_front()
            {
                self.marking = Some(mark(self.js.clone(), self.me.clone(), thread).boxed_local());
            }
            if self.unmarking.is_none()
                && let Some((thread, seqs)) = self.to_unmark.pop_front()
            {
                self.unmarking = Some(unmark(self.js.clone(), self.me.clone(), thread, seqs).boxed_local());
            }
            if self.marking_channel.is_none()
                && let Some((channel, marks)) = self.channels_to_mark.pop_front()
            {
                let (js, me) = (self.js.clone(), self.me.clone());
                self.marking_channel = Some(mark_channel(js, me, channel, marks).boxed_local());
            }
            if self.reacting.is_none()
                && let Some(reacting) = self.to_react.pop_front()
            {
                self.reacting =
                    Some(react(self.js.clone(), self.domain.clone(), self.me.clone(), reacting).boxed_local());
            }
            tokio::select! {
                _ = self.markers.tick() => {
                    if self.marker.is_none() {
                        self.read_marker();
                    }
                    if self.channel_marker.is_none() {
                        self.read_channel_marker();
                    }
                }
                _ = self.directories.tick() => {
                    if self.directory.is_none() {
                        let js = self.js.clone();
                        self.directory = Some(directory(js).boxed_local());
                    }
                    if self.levels.is_none() {
                        self.read_levels();
                    }
                    if self.reactions.is_none() {
                        self.read_reactions();
                    }
                }
                read = maybe(&mut self.marker) => {
                    self.marker = None;
                    return Read::Marker(read);
                }
                read = maybe(&mut self.channel_marker) => {
                    self.channel_marker = None;
                    return Read::ChannelMarkers(read);
                }
                (channel, marked) = maybe(&mut self.marking_channel) => {
                    self.marking_channel = None;
                    return Read::ChannelMarked(channel, marked);
                }
                read = maybe(&mut self.directory) => {
                    self.directory = None;
                    return Read::Directory(read);
                }
                read = maybe(&mut self.levels) => {
                    self.levels = None;
                    match read {
                        Ok(levels) => return Read::Levels(levels),
                        Err(e) => tracing::debug!("reading the channels' notify levels: {e:#}"),
                    }
                }
                (thread, marked) = maybe(&mut self.marking) => {
                    self.marking = None;
                    // What is read changed: show it at once.
                    self.read_marker();
                    return Read::Marked(thread, marked);
                }
                (thread, unmarked) = maybe(&mut self.unmarking) => {
                    self.unmarking = None;
                    self.read_marker();
                    return Read::Unmarked(thread, unmarked);
                }
                Some(change) = self.changes.next() => {
                    if self.watched.contains(&change.message) {
                        self.read_reactions();
                    }
                }
                ((message, reaction, remove), changed) = maybe(&mut self.reacting) => {
                    self.reacting = None;
                    self.read_reactions();
                    return Read::Reacted(message, reaction, remove, changed);
                }
                read = maybe(&mut self.reactions) => {
                    self.reactions = None;
                    match read {
                        Ok(reactions) => return Read::Reactions(reactions),
                        Err(e) => tracing::debug!("reading the reactions: {e:#}"),
                    }
                }
            }
        }
    }

    fn read_marker(&mut self) {
        let (js, me) = (self.js.clone(), self.me.clone());
        self.marker = Some(async move { Ok(Inbox::open(&js, &me).await?.marker().await?.0) }.boxed_local());
    }

    fn read_channel_marker(&mut self) {
        if !self.channels {
            return;
        }
        let (js, me) = (self.js.clone(), self.me.clone());
        self.channel_marker =
            Some(async move { Ok(ChannelReads::open(&js, &me).await?.markers().await?.0) }.boxed_local());
    }

    fn read_levels(&mut self) {
        if !self.channels {
            return;
        }
        let (js, me) = (self.js.clone(), self.me.clone());
        self.levels = Some(async move { Ok(crate::quiet::prefs(&js, &me).await?.channels) }.boxed_local());
    }

    fn read_reactions(&mut self) {
        let (js, watched) = (self.js.clone(), self.watched.clone());
        self.reactions = Some(async move { reactions::load(&js, &watched).await }.boxed_local());
    }

    /// Mark the human's inbox messages on `thread` read, after those asked for before.
    pub fn mark_read(&mut self, thread: ThreadId) {
        if !self.to_mark.contains(&thread) {
            self.to_mark.push_back(thread);
        }
    }

    /// Mark the human's inbox messages at `seqs`, on `thread`, unread again on all its devices, after those asked for
    /// before.
    pub fn mark_unread(&mut self, thread: ThreadId, seqs: Vec<u64>) {
        self.to_unmark.push_back((thread, seqs));
    }

    /// Mark the human's posts of `channel` read on all its devices, after those asked for before; nothing without the
    /// credentials to.
    pub fn mark_channel(&mut self, channel: ChannelId, marks: Mark) {
        if self.channels {
            self.channels_to_mark.push_back((channel, marks));
        }
    }

    /// Read the reactions to `messages` now and every while, instead of those watched so far.
    pub fn watch_reactions(&mut self, messages: Vec<Uuid>) {
        self.watched = messages;
        self.read_reactions();
    }

    /// React `reaction` to `message`, or with `remove` take it back, after the reactions asked for before.
    pub fn react(&mut self, message: Uuid, reaction: Reaction, remove: bool) {
        self.to_react.push_back((message, reaction, remove));
    }

    /// Read everything again now.
    pub fn refresh(&mut self) {
        self.markers.reset_immediately();
        self.directories.reset_immediately();
        self.marker = None;
        self.channel_marker = None;
        self.directory = None;
        self.levels = None;
        self.reactions = None;
    }
}

/// Every reaction added or removed from now on; none when the bucket cannot be watched.
fn changes(js: jetstream::Context) -> LocalBoxStream<'static, Change> {
    futures::stream::once(async move { reactions::watch(&js).await })
        .filter_map(async |watch| watch.inspect_err(|e| tracing::debug!("watching the reactions: {e:#}")).ok())
        .flatten()
        .chain(futures::stream::pending())
        .boxed_local()
}

/// Make `reacting` as `me`, as `ants react` does.
async fn react(
    js: jetstream::Context,
    domain: Option<JsDomain>,
    me: UserId,
    reacting: Reacting,
) -> (Reacting, Result<bool>) {
    let (message, reaction, remove) = &reacting;
    let changed = if *remove {
        reactions::remove(&js, domain.as_ref(), &me, message, reaction).await
    } else {
        reactions::add(&js, domain.as_ref(), &me, message, reaction).await
    };
    (reacting, changed)
}

async fn directory(js: jetstream::Context) -> Result<Directory> {
    let channels = ants_daemon::channel::list(&crate::channel::open(&js).await?).await?;
    Ok((channels, crate::humans::read(&js).await?))
}

/// Mark the inbox messages of `me` on `thread` read, as `ants inbox --thread THREAD` does.
async fn mark(js: jetstream::Context, me: UserId, thread: ThreadId) -> (ThreadId, Result<()>) {
    let select = Select { thread: Some(thread.clone()), limit: None, peek: false, summaries: false };
    let marked = async { Inbox::open(&js, &me).await?.unread(&select).await.map(drop) }.await;
    (thread, marked)
}

/// Mark the inbox messages of `me` at `seqs`, on `thread`, unread again, as `ants inbox --unread` does.
async fn unmark(js: jetstream::Context, me: UserId, thread: ThreadId, seqs: Vec<u64>) -> (ThreadId, Result<()>) {
    let unmarked = async { Inbox::open(&js, &me).await?.unmark(&seqs).await }.await;
    (thread, unmarked)
}

/// Mark the posts of `channel` read for `me`, as `ants channel read` does: the markers as changed.
async fn mark_channel(
    js: jetstream::Context,
    me: UserId,
    channel: ChannelId,
    marks: Mark,
) -> (ChannelId, Result<ChannelReadMarkers>) {
    let marked = async {
        ChannelReads::open(&js, &me)
            .await?
            .change(|markers| match &marks {
                Mark::Through(seq) => markers.mark_through(&channel, *seq),
                Mark::Posts(seqs) => seqs.iter().for_each(|seq| markers.mark(&channel, *seq)),
            })
            .await
    }
    .await;
    (channel, marked)
}

impl super::model::Model {
    /// `u` on `thread`: mark the human's messages on it unread again on all its devices, those from the message at
    /// sequence `from` on, else (or with none from it on) the latest; here at once, not once the marker is read back.
    /// A thread view of it shown closes, so as not to mark them read again.
    pub fn mark_unread(
        &mut self,
        thread: ThreadId,
        from: Option<u64>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<Action> {
        let inbox: Vec<u64> =
            self.data.facts.get(&thread).map_or_else(Vec::new, |f| f.inbox.iter().map(|i| i.seq).collect());
        let mut seqs: Vec<u64> = inbox.iter().copied().filter(|seq| from.is_some_and(|from| *seq >= from)).collect();
        if seqs.is_empty() {
            seqs.extend(inbox.iter().max());
        }
        if seqs.is_empty() {
            let line = format!("no message to {} on this thread to mark unread", self.data.me());
            self.notify(line, super::model::Level::Error, now);
            return None;
        }
        if let Some(marker) = &mut self.data.marker {
            seqs.iter().for_each(|seq| marker.unmark(*seq));
        }
        if self.open() == Some(&thread) && self.pages.len() > 1 {
            self.pages.pop();
        }
        Some(Action::MarkUnread(thread, seqs))
    }
}
