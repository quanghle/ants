//! What `ants chat` reads besides the live feed ([`crate::live::Feed`]), every while and on demand: which of the
//! human's inbox messages are read, the channels and the humans, and the reactions to the messages shown; and marking
//! a thread's messages read.

use std::collections::VecDeque;
use std::time::Duration;

use ants_daemon::reactions::{self, Reactions};
use ants_proto::{ChannelEntry, Humans, ReadMarker, ThreadId, UserId, Uuid};
use anyhow::Result;
use async_nats::jetstream;
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;

use crate::inbox::{Inbox, Select};
use crate::live::maybe;

/// How often the read marker is read, for what other devices of the human marked read.
const MARKER_EVERY: Duration = Duration::from_secs(5);
/// How often the channels, the humans and the reactions are read.
const DIRECTORY_EVERY: Duration = Duration::from_secs(10);

type Directory = (Vec<ChannelEntry>, Humans);

/// Something read.
pub enum Read {
    Marker(Result<ReadMarker>),
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
    /// The messages whose reactions are read.
    watched: Vec<Uuid>,
    reactions: Option<LocalBoxFuture<'static, Result<Reactions>>>,
}

impl Reads {
    pub fn new(js: jetstream::Context, me: UserId) -> Self {
        Self {
            js,
            me,
            markers: tokio::time::interval(MARKER_EVERY),
            directories: tokio::time::interval(DIRECTORY_EVERY),
            marker: None,
            directory: None,
            to_mark: VecDeque::new(),
            marking: None,
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
            tokio::select! {
                _ = self.markers.tick() => {
                    if self.marker.is_none() {
                        self.read_marker();
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
    let select = Select { thread: Some(thread.clone()), limit: None, peek: false };
    let marked = async { Inbox::open(&js, &me).await?.unread(&select).await.map(drop) }.await;
    (thread, marked)
}
