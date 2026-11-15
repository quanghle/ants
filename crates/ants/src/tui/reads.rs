//! What `ants chat` reads besides the live feed ([`crate::live::Feed`]), every while and on demand: which of the
//! human's inbox messages and channel posts are read, the channels and the humans, and the reactions to the messages
//! shown; and marking a thread's messages and a channel's posts read.

use std::collections::VecDeque;
use std::time::Duration;

use ants_daemon::reactions::{self, Reactions};
use ants_proto::{ChannelEntry, ChannelId, ChannelReadMarkers, Humans, ReadMarker, ThreadId, UserId, Uuid};
use anyhow::Result;
use async_nats::jetstream;
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;

use crate::channel::reads::ChannelReads;
use crate::inbox::{Inbox, Select};
use crate::live::maybe;

/// How often the read markers are read, for what other devices of the human marked read.
const MARKER_EVERY: Duration = Duration::from_secs(5);
/// How often the channels, the humans and the reactions are read.
const DIRECTORY_EVERY: Duration = Duration::from_secs(10);

type Directory = (Vec<ChannelEntry>, Humans);

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
    /// The thread's messages were marked read, or not.
    Marked(ThreadId, Result<()>),
    Reactions(Reactions),
}

pub struct Reads {
    js: jetstream::Context,
    me: UserId,
    markers: tokio::time::Interval,
    directories: tokio::time::Interval,
    marker: Option<LocalBoxFuture<'static, Result<ReadMarker>>>,
    directory: Option<LocalBoxFuture<'static, Result<Directory>>>,
    /// The threads to mark read, after the one in flight.
    to_mark: VecDeque<ThreadId>,
    marking: Option<LocalBoxFuture<'static, (ThreadId, Result<()>)>>,
    /// Whether the human's credentials may read and mark its channel read markers.
    channels: bool,
    channel_marker: Option<LocalBoxFuture<'static, Result<ChannelReadMarkers>>>,
    /// The channel posts to mark read, after those in flight.
    channels_to_mark: VecDeque<(ChannelId, Mark)>,
    marking_channel: Option<LocalBoxFuture<'static, (ChannelId, Result<ChannelReadMarkers>)>>,
    /// The messages whose reactions are read.
    watched: Vec<Uuid>,
    reactions: Option<LocalBoxFuture<'static, Result<Reactions>>>,
}

impl Reads {
    /// What `me` reads; `channels`, whether its channel read markers too.
    pub fn new(js: jetstream::Context, me: UserId, channels: bool) -> Self {
        Self {
            js,
            me,
            markers: tokio::time::interval(MARKER_EVERY),
            directories: tokio::time::interval(DIRECTORY_EVERY),
            marker: None,
            directory: None,
            to_mark: VecDeque::new(),
            marking: None,
            channels,
            channel_marker: None,
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
            if self.marking_channel.is_none()
                && let Some((channel, marks)) = self.channels_to_mark.pop_front()
            {
                let (js, me) = (self.js.clone(), self.me.clone());
                self.marking_channel = Some(mark_channel(js, me, channel, marks).boxed_local());
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
                (thread, marked) = maybe(&mut self.marking) => {
                    self.marking = None;
                    // What is read changed: show it at once.
                    self.read_marker();
                    return Read::Marked(thread, marked);
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

    /// Read everything again now.
    pub fn refresh(&mut self) {
        self.markers.reset_immediately();
        self.directories.reset_immediately();
        self.marker = None;
        self.channel_marker = None;
        self.directory = None;
        self.reactions = None;
    }
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
