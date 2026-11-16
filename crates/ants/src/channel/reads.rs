//! Which channel posts a human has read, on any of its devices: its [`ChannelReadMarkers`] under
//! [`channel_reads_key`] in [`READ_BUCKET`], beside its inbox's read marker, updated with compare-and-set so devices
//! marking at the same time each keep the other's marks. `ants channel read` marks the posts it prints, `ants channel
//! list` counts the unread ones, and `ants chat` does both as its views show them.

use ants_proto::{ChannelId, ChannelReadMarkers, Humans, ReadMarker, UserId, channel_reads_key};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::kv::{self, Operation, UpdateErrorKind};

use crate::humans::Me;

/// How many times a change is tried again after another device moved the markers first.
const CONFLICTS: usize = 16;

/// Whether the credentials of `nats` may read and write their human's channel read markers: a human's, never an
/// ant's.
pub fn allowed(nats: &crate::Nats<'_>) -> bool {
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
        Ok(Self { kv: crate::inbox::ensure_bucket(js).await?, key: channel_reads_key(human) })
    }

    /// The markers and their revision, 0 when there are none.
    pub async fn markers(&self) -> Result<(ChannelReadMarkers, u64)> {
        let Some(entry) = self.kv.entry(&self.key).await.context("reading the channel read markers")? else {
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
            match self.kv.update(&self.key, serde_json::to_vec(&markers)?.into(), revision).await {
                Ok(_) => return Ok(markers),
                Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => continue,
                Err(e) => return Err(e).context("marking channel posts read"),
            }
        }
        bail!("marking channel posts read: the channel read markers kept moving")
    }
}

/// A channel's unread posts: those by humans or that @mention the human apart from the other ants' chatter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unread {
    /// Those by a human, or that @mention the human.
    pub posts: usize,
    /// Those of them that @mention the human.
    pub mentions: usize,
    /// Those by an ant that do not @mention the human.
    pub ants: usize,
}

impl Unread {
    /// The unread ones of `posts`, each its sequence, author and the users it @mentions, by `marker` (none: nothing
    /// read), for `me`, with `is_human` telling a human author from an ant. Its own posts are read.
    pub fn of<'a>(
        posts: impl IntoIterator<Item = (u64, &'a UserId, &'a [UserId])>,
        marker: Option<&ReadMarker>,
        me: &UserId,
        is_human: impl Fn(&UserId) -> bool,
    ) -> Self {
        let mut unread = Self::default();
        for (seq, from, mentions) in posts {
            if from == me || marker.is_some_and(|m| m.is_read(seq)) {
                continue;
            }
            if mentions.contains(me) {
                unread.posts += 1;
                unread.mentions += 1;
            } else if is_human(from) {
                unread.posts += 1;
            } else {
                unread.ants += 1;
            }
        }
        unread
    }
}

/// The unread posts of `channel` by `markers`, for `reader`, the team's humans being `humans`: only those after what
/// the marker has read up to are read from the stream.
pub async fn unread(
    js: &jetstream::Context,
    reader: &Me,
    humans: &Humans,
    markers: &ChannelReadMarkers,
    channel: &ChannelId,
) -> Result<Unread> {
    let marker = markers.channels.get(channel);
    let from = marker.map_or(0, |m| m.below + 1);
    let posts = super::read_from(js, reader, channel, from).await?;
    let posts = posts.iter().map(|(seq, post)| (*seq, &post.from, post.mentions.as_slice()));
    Ok(Unread::of(posts, marker, &reader.id, |u| humans.contains(u)))
}
