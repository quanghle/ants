//! Which channel posts a human has read, on any of its devices: its [`ChannelReadMarkers`] under [`channel_reads_key`]
//! in [`READ_BUCKET`](ants_proto::READ_BUCKET), beside its inbox's read marker, updated with compare-and-set so devices
//! marking at the same time each keep the other's marks. `ants channel read` marks the posts it prints, `ants channel
//! list` counts the unread ones, and `ants chat` does both as its views show them; each channel's notify level
//! ([`ChannelNotify`]) says which unread posts count.

use ants_daemon::bus::relayed::GuardedKv as _;
use ants_proto::{ChannelId, ChannelNotify, ChannelReadMarkers, Humans, ReadMarker, UserId, channel_reads_key};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::kv::{self, Operation};

use crate::cas;
use crate::people::humans::Me;

/// How many times a change is tried again after another device moved the markers first.
const CONFLICTS: usize = 16;

/// Whether the credentials of `nats` may read and write their human's channel read markers: a human's, never an
/// ant's.
pub fn allowed(nats: &crate::connection::Nats<'_>) -> bool {
    nats.me.role.is_human()
}

/// A human's channel read markers in the read bucket.
pub struct ChannelReads {
    kv: kv::Store,
    key: String,
}

impl ChannelReads {
    /// The channel read markers of `human`, creating the read bucket when missing.
    pub async fn open(js: &jetstream::Context, human: &UserId) -> Result<Self> {
        Ok(Self { kv: crate::messages::inbox::ensure_bucket(js).await?, key: channel_reads_key(human) })
    }

    /// The markers and their revision, 0 when there are none.
    pub async fn markers(&self) -> Result<(ChannelReadMarkers, u64)> {
        let Some(entry) = self.kv.guarded_entry(&self.key).await.context("reading the channel read markers")? else {
            return Ok((ChannelReadMarkers::default(), 0));
        };
        if entry.operation != Operation::Put {
            return Ok((ChannelReadMarkers::default(), entry.revision));
        }
        let markers = serde_json::from_slice(&entry.value).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "malformed channel read markers; starting over");
            ChannelReadMarkers::default()
        });
        Ok((markers, entry.revision))
    }

    /// Change the markers with `change`, on top of whatever other devices marked meanwhile; stores nothing when that
    /// changes nothing. The markers as changed.
    pub async fn change(&self, change: impl Fn(&mut ChannelReadMarkers)) -> Result<ChannelReadMarkers> {
        for _ in 0..CONFLICTS {
            let (mut markers, revision) = self.markers().await?;
            let had = markers.clone();
            change(&mut markers);
            if markers == had {
                return Ok(markers);
            }
            let stored = cas::update(&self.kv, &self.key, serde_json::to_vec(&markers)?, revision).await;
            if stored.context("marking channel posts read")? {
                return Ok(markers);
            }
        }
        bail!("marking channel posts read: the channel read markers kept moving")
    }
}

/// A channel's unread posts: those its notify level counts apart from the others, ants' chatter by default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unread {
    /// Those the channel's notify level counts: by default, those by a human or that @mention the human.
    pub posts: usize,
    /// Those of them that @mention the human.
    pub mentions: usize,
    /// The others, shown apart and dim.
    pub other: usize,
}

impl Unread {
    /// The unread ones of `posts`, each its sequence, author and the users it @mentions, by `marker` (none: nothing
    /// read), for `me` at notify level `level`, with `is_human` telling a human author from an ant. Its own posts are
    /// read.
    pub fn of<'a>(
        posts: impl IntoIterator<Item = (u64, &'a UserId, &'a [UserId])>,
        marker: Option<&ReadMarker>,
        me: &UserId,
        level: ChannelNotify,
        is_human: impl Fn(&UserId) -> bool,
    ) -> Self {
        let mut unread = Self::default();
        for (seq, from, mentions) in posts {
            if from == me || marker.is_some_and(|m| m.is_read(seq)) {
                continue;
            }
            let mentioned = mentions.contains(me);
            if level.counts(is_human(from), mentioned) {
                unread.posts += 1;
                unread.mentions += usize::from(mentioned);
            } else {
                unread.other += 1;
            }
        }
        unread
    }
}

/// The unread posts of `channel` by `markers`, for `reader` at notify level `level`, the bus's humans being
/// `humans`, but for those on the threads `archive` leaves out: only those after what the marker has read up to are
/// read from the stream.
pub async fn unread(
    js: &jetstream::Context,
    reader: &Me,
    humans: &Humans,
    markers: &ChannelReadMarkers,
    channel: &ChannelId,
    level: ChannelNotify,
    archive: Option<&super::Archive>,
) -> Result<Unread> {
    let marker = markers.channels.get(channel);
    let from = marker.map_or(0, |m| m.below + 1);
    let posts = super::read_from(js, reader, channel, from).await?;
    // A thread with an unread post has its later posts read too, so its last post is among those read.
    let hidden =
        archive.map_or_else(std::collections::HashSet::new, |a| a.hidden(posts.iter().map(|(seq, post)| (*seq, post))));
    let posts = posts
        .iter()
        .filter(|(_, post)| !hidden.contains(&post.thread_id))
        .map(|(seq, post)| (*seq, &post.from, post.mentions.as_slice()));
    Ok(Unread::of(posts, marker, &reader.id, level, |u| humans.contains(u)))
}
