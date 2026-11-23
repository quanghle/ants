//! A human's state shared by all its devices, each updated with compare-and-set: the inbox messages and channel posts
//! it read ([`ReadMarker`], [`ChannelReadMarkers`]) in [`crate::READ_BUCKET`], and the notifications it holds back
//! ([`NotifyPrefs`], [`HeldNotices`]) in [`crate::NOTIFY_BUCKET`].

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{ChannelId, ThreadId};
#[cfg(doc)]
use crate::{NOTIFY_BUCKET, READ_BUCKET, STREAM, channel_reads_key, held_key};

/// Which of a human's inbox messages, by [`STREAM`] sequence, the human has read: every one up to `below` but those
/// in `unread`, marked unread again, and those in `read` above it. Kept under the human's key in [`READ_BUCKET`] and shared by all its devices, which update it
/// with compare-and-set, so reading on one device marks read on all of them without consuming the message.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadMarker {
    /// Every inbox message of the human at or below this sequence is read.
    #[serde(default)]
    pub below: u64,
    /// Inbox messages above `below` read too, as when only one thread's were read.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub read: std::collections::BTreeSet<u64>,
    /// Inbox messages at or below `below` marked unread again.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub unread: std::collections::BTreeSet<u64>,
}

impl ReadMarker {
    pub fn is_read(&self, seq: u64) -> bool {
        (seq <= self.below && !self.unread.contains(&seq)) || self.read.contains(&seq)
    }

    /// Mark the message at `seq` read.
    pub fn mark(&mut self, seq: u64) {
        if seq > self.below {
            self.read.insert(seq);
        } else {
            self.unread.remove(&seq);
        }
    }

    /// Mark the message at `seq` unread again.
    pub fn unmark(&mut self, seq: u64) {
        if seq > self.below {
            self.read.remove(&seq);
        } else {
            self.unread.insert(seq);
        }
    }

    /// The first sequence a reader of the unread messages reads from: past `below`, or the first marked unread again.
    pub fn first_unread(&self) -> u64 {
        self.unread.first().copied().unwrap_or(self.below + 1).min(self.below + 1)
    }

    /// Move `below` past the read messages that follow it, given `inbox`, the sequences, in order, of every inbox
    /// message of the human stored after some sequence at or below `below`: what is read before the first unread one
    /// needs no entry of its own. Sequences of messages gone from the stream are skipped over as well.
    pub fn compact(&mut self, inbox: impl IntoIterator<Item = u64>) {
        for seq in inbox {
            if seq <= self.below {
                continue;
            }
            if !self.read.contains(&seq) {
                break;
            }
            self.below = seq;
        }
        let below = self.below;
        self.read.retain(|seq| *seq > below);
    }
}

/// Which channel posts, by [`STREAM`] sequence, a human has read: per channel, a [`ReadMarker`] over the sequences of
/// that channel's posts. Kept under [`channel_reads_key`] in [`READ_BUCKET`], apart from the human's inbox
/// [`ReadMarker`], so a CLI that knows only the inbox marker keeps both; shared by all the human's devices, which
/// update it with compare-and-set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelReadMarkers {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub channels: std::collections::BTreeMap<ChannelId, ReadMarker>,
}

impl ChannelReadMarkers {
    /// Whether the post of `channel` at `seq` is read.
    pub fn is_read(&self, channel: &ChannelId, seq: u64) -> bool {
        self.channels.get(channel).is_some_and(|m| m.is_read(seq))
    }

    /// Mark the post of `channel` at `seq` read.
    pub fn mark(&mut self, channel: &ChannelId, seq: u64) {
        if !self.is_read(channel, seq) {
            self.channels.entry(channel.clone()).or_default().mark(seq);
        }
    }

    /// Mark every post of `channel` at or below `seq` read.
    pub fn mark_through(&mut self, channel: &ChannelId, seq: u64) {
        let marker = self.channels.entry(channel.clone()).or_default();
        if seq > marker.below {
            marker.below = seq;
            marker.read.retain(|read| *read > seq);
            marker.unread.clear();
        }
    }

    /// Move the marker of `channel` past the read posts that follow it ([`ReadMarker::compact`]), given `posts`, the
    /// sequences, in order, of every post of the channel stored after some sequence at or below its `below`.
    pub fn compact(&mut self, channel: &ChannelId, posts: impl IntoIterator<Item = u64>) {
        if let Some(marker) = self.channels.get_mut(channel) {
            marker.compact(posts);
        }
    }
}

/// What a human holds back of its notifications, on all its devices: kept under the human's key in
/// [`NOTIFY_BUCKET`], updated with compare-and-set by `ants notify snooze|resume`, `ants thread mute|unmute` and
/// `ants channel notify`, and read by `ants webhook serve` before each notice and by `ants chat` and `ants channel
/// list` to count unread channel posts. Each hold ends at its deadline, with no one there to lift it; a channel's
/// notify level stays until changed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyPrefs {
    /// Until when every notice the human's config does not let through (`bypass`) is held back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snoozed_until: Option<DateTime<Utc>>,
    /// The muted threads, each until when: none of their notices are sent.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub muted: std::collections::BTreeMap<ThreadId, DateTime<Utc>>,
    /// The notify level of each channel not at the default ([`ChannelNotify::Humans`]).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub channels: std::collections::BTreeMap<ChannelId, ChannelNotify>,
}

/// Which unread posts of a channel count for a human, shown bold with their number, rather than apart and dim.
/// Posts @mentioning the human always count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelNotify {
    /// Every post.
    All,
    /// Posts by humans, and those @mentioning the human.
    #[default]
    Humans,
    /// Only posts @mentioning the human.
    Mentions,
}

impl ChannelNotify {
    pub const ALL: [Self; 3] = [Self::All, Self::Humans, Self::Mentions];

    /// Its name on the wire, as serde writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Humans => "humans",
            Self::Mentions => "mentions",
        }
    }

    /// How `ants chat` and `ants channel notify` describe it.
    pub fn describe(self) -> &'static str {
        match self {
            Self::All => "all posts",
            Self::Humans => "posts by humans and @mentions",
            Self::Mentions => "@mentions only",
        }
    }

    /// Whether a post by `from`, a human or not, that @mentions the reader or not, counts.
    pub fn counts(self, by_human: bool, mentions_me: bool) -> bool {
        match self {
            Self::All => true,
            Self::Humans => by_human || mentions_me,
            Self::Mentions => mentions_me,
        }
    }
}

impl std::fmt::Display for ChannelNotify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ChannelNotify {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|level| level.as_str() == s)
            .ok_or_else(|| format!("unknown notify level {s:?}: all, humans or mentions"))
    }
}

impl NotifyPrefs {
    /// The notify level of `channel`.
    pub fn channel(&self, channel: &ChannelId) -> ChannelNotify {
        self.channels.get(channel).copied().unwrap_or_default()
    }

    /// Set the notify level of `channel`, keeping none for the default.
    pub fn set_channel(&mut self, channel: &ChannelId, level: ChannelNotify) {
        if level == ChannelNotify::default() {
            self.channels.remove(channel);
        } else {
            self.channels.insert(channel.clone(), level);
        }
    }

    /// Whether a snooze holds at `now`.
    pub fn snoozed(&self, now: DateTime<Utc>) -> bool {
        self.snoozed_until.is_some_and(|until| now < until)
    }

    /// Whether `thread` is muted at `now`.
    pub fn is_muted(&self, thread: &ThreadId, now: DateTime<Utc>) -> bool {
        self.muted.get(thread).is_some_and(|until| now < *until)
    }

    /// Drop the holds that ended by `now`.
    pub fn prune(&mut self, now: DateTime<Utc>) {
        if !self.snoozed(now) {
            self.snoozed_until = None;
        }
        self.muted.retain(|_, until| now < *until);
    }
}

/// What `ants webhook serve` held back from a human, with `digest = true`, while a snooze or quiet hours held its
/// notices back: kept under [`held_key`] in [`NOTIFY_BUCKET`], counted with compare-and-set by every instance notifying
/// the human, and deleted with compare-and-set by the one instance sending the digest once the hold ended.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldNotices {
    /// When the first of them was held back.
    pub since: DateTime<Utc>,
    /// How many were held back of each kind of notice (`approval`, `escalation`, `completion`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub counts: std::collections::BTreeMap<String, u32>,
}

impl HeldNotices {
    /// Count one more notice of `kind`, held back at `now`.
    pub fn add(&mut self, kind: &str, now: DateTime<Utc>) {
        if self.counts.is_empty() {
            self.since = now;
        }
        let count = self.counts.entry(kind.to_owned()).or_default();
        *count = count.saturating_add(1);
    }

    /// How many were held back in all.
    pub fn total(&self) -> u32 {
        self.counts.values().fold(0, |sum, n| sum.saturating_add(*n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_marker_keeps_selective_reads_until_the_unread_before_them_are_read() {
        let mut marker = ReadMarker::default();
        // Inbox messages at 3, 5, 8 and 9; 5 and 9 are read first, as on one thread.
        for seq in [5, 9] {
            marker.mark(seq);
        }
        marker.compact([3, 5, 8, 9]);
        assert_eq!((marker.below, marker.read.iter().copied().collect::<Vec<_>>()), (0, vec![5, 9]));
        assert!(!marker.is_read(3) && marker.is_read(5) && !marker.is_read(8));
        marker.mark(3);
        marker.compact([3, 5, 8, 9]);
        assert_eq!((marker.below, marker.read.len()), (5, 1), "3 and 5 read, 8 not");
        // 8 left the stream before it was read: nothing holds the marker back any more.
        marker.compact([9]);
        assert_eq!(marker, ReadMarker { below: 9, ..ReadMarker::default() });
        marker.mark(2);
        assert!(marker.read.is_empty(), "already read");
        let json = serde_json::to_string(&marker).unwrap();
        assert_eq!(json, r#"{"below":9}"#);
        assert_eq!(serde_json::from_str::<ReadMarker>("{}").unwrap(), ReadMarker::default());
    }

    #[test]
    fn a_read_marker_marks_messages_unread_again_on_either_side_of_below() {
        let mut marker = ReadMarker { below: 9, read: [12].into(), ..ReadMarker::default() };
        assert_eq!(marker.first_unread(), 10);
        for seq in [5, 12] {
            marker.unmark(seq);
        }
        assert!(!marker.is_read(5) && !marker.is_read(12) && marker.is_read(4) && marker.is_read(9));
        assert_eq!(marker.first_unread(), 5, "a reader reads from the first marked unread again");
        marker.compact([10, 11]);
        assert!(!marker.is_read(5), "moving below on keeps what was marked unread");
        marker.mark(5);
        assert_eq!(marker, ReadMarker { below: 9, ..ReadMarker::default() });
        assert_eq!(marker.first_unread(), 10);
    }

    #[test]
    fn channel_read_markers_keep_a_marker_per_channel() {
        let (ops, dev) = ("ops".parse::<ChannelId>().unwrap(), "dev".parse::<ChannelId>().unwrap());
        let mut markers = ChannelReadMarkers::default();
        markers.mark(&ops, 7);
        markers.mark(&ops, 9);
        assert!(markers.is_read(&ops, 7) && !markers.is_read(&ops, 8) && !markers.is_read(&dev, 7));
        markers.compact(&ops, [7, 8, 9]);
        assert_eq!(
            markers.channels[&ops],
            ReadMarker { below: 7, read: [9].into(), ..ReadMarker::default() },
            "8 holds the marker back"
        );
        markers.mark_through(&ops, 8);
        assert_eq!(markers.channels[&ops], ReadMarker { below: 8, read: [9].into(), ..ReadMarker::default() });
        markers.mark_through(&ops, 9);
        markers.mark_through(&ops, 3);
        assert_eq!(markers.channels[&ops], ReadMarker { below: 9, ..ReadMarker::default() }, "never moves back");
        markers.compact(&dev, [1, 2]);
        assert!(!markers.channels.contains_key(&dev), "no marker for a channel with nothing read");
        assert_eq!(serde_json::to_string(&markers).unwrap(), r#"{"channels":{"ops":{"below":9}}}"#);
        assert_eq!(serde_json::from_str::<ChannelReadMarkers>("{}").unwrap(), ChannelReadMarkers::default());
    }
}
