//! The in-hand window: how many messages the ant's inbox and channel consumers hand the daemon before their acks
//! (`max_ack_pending`). Messages on one thread run one at a time, so a thread's backlog filling a fixed window would
//! leave every other thread's messages in the stream, harnesses idle. Each consumer's window is therefore its base
//! ([`super::IN_HAND_PER_HARNESS`] per harness) widened by the messages it handed over that wait on their thread
//! behind its first [`THREAD_HEAD`], up to [`OVER_AT_MOST`] more.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ants_proto::UserId;
use anyhow::Result;
use async_nats::jetstream;
use futures::stream::{Stream, StreamExt as _};

use crate::bus;
use crate::sync::MutexExt as _;

use super::MAX_DELIVER;

/// The messages of a thread that count toward the window: the one running and the next.
pub(super) const THREAD_HEAD: usize = 2;
/// How far past its base each consumer's window widens at most, bounding the messages in hand and their progress acks.
pub(super) const OVER_AT_MOST: usize = 256;

/// Messages in hand that wait on their thread behind its first [`THREAD_HEAD`], by the consumer that handed them over.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Over {
    pub(super) inbox: usize,
    pub(super) posts: usize,
}

/// The window of the ant's inbox and channel consumers, and how to open and widen them.
pub(super) struct Window {
    stream: jetstream::stream::Stream<()>,
    ant: UserId,
    ack_wait: Duration,
    base: i64,
    /// Whether the channel consumer is pulled from, for the daemon to widen it.
    posts: AtomicBool,
    /// The `max_ack_pending` of the inbox and the channel consumer, as last set.
    set: Mutex<(i64, i64)>,
}

impl Window {
    pub(super) fn new(stream: jetstream::stream::Stream<()>, ant: UserId, ack_wait: Duration, base: i64) -> Self {
        Self { stream, ant, ack_wait, base, posts: AtomicBool::new(false), set: Mutex::new((base, base)) }
    }

    /// The ant's inbox consumer, with its channel consumer if `posts`, as one stream, each at the window as set.
    pub(super) async fn open(
        &self,
        posts: bool,
    ) -> Result<impl Stream<Item = <bus::Pull as Stream>::Item> + Unpin + use<>> {
        let (inbox, chan) = *self.set.locked();
        let messages = bus::inbox_consumer(&self.stream, &self.ant, self.ack_wait, MAX_DELIVER, inbox).await?;
        let posts = if posts {
            match bus::chan_consumer(&self.stream, &self.ant, self.ack_wait, MAX_DELIVER, chan).await {
                Ok(posts) => Some(posts),
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "serving the inbox without channel posts");
                    None
                }
            }
        } else {
            None
        };
        self.posts.store(posts.is_some(), Ordering::Relaxed);
        Ok(futures::stream::select(messages, futures::stream::iter(posts).flatten()))
    }

    /// Widen or narrow the consumers to fit `over`: what updates those whose window changed, none when neither did.
    pub(super) fn fit(&self, over: Over) -> Option<impl Future<Output = ()> + Send + use<'_>> {
        let width = |over: usize| self.base + i64::try_from(over.min(OVER_AT_MOST)).unwrap_or(0);
        let posts = self.posts.load(Ordering::Relaxed);
        let wanted = (width(over.inbox), if posts { width(over.posts) } else { self.base });
        let before = std::mem::replace(&mut *self.set.locked(), wanted);
        if before == wanted {
            return None;
        }
        Some(async move {
            let (ant, ack_wait) = (&self.ant, self.ack_wait);
            if before.0 != wanted.0 {
                self.update(bus::inbox_config(ant, ack_wait, MAX_DELIVER, wanted.0)).await;
            }
            if before.1 != wanted.1 {
                self.update(bus::chan_config(ant, ack_wait, MAX_DELIVER, wanted.1)).await;
            }
        })
    }

    async fn update(&self, config: jetstream::consumer::pull::Config) {
        let in_hand = config.max_ack_pending;
        match bus::update_consumer(&self.stream, config).await {
            Ok(()) => tracing::debug!(in_hand, "a consumer's window changed"),
            Err(e) => tracing::warn!(error = format!("{e:#}"), in_hand, "changing a consumer's window"),
        }
    }
}
