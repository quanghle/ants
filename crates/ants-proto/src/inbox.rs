use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{ChannelId, ThreadId};
#[cfg(doc)]
use crate::{NOTIFY_BUCKET, READ_BUCKET, STREAM, channel_reads_key, held_key};

/// Which of a human's inbox messages, by [`STREAM`] sequence, the human has read: every one up to `below`, and those
/// in `read` above it. Kept under the human's key in [`READ_BUCKET`] and shared by all its devices, which update it
/// with compare-and-set, so reading on one device marks read on all of them without consuming the message.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadMarker {
    /// Every inbox message of the human at or below this sequence is read.
    #[serde(default)]
    pub below: u64,
    /// Inbox messages above `below` read too, as when only one thread's were read.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub read: std::collections::BTreeSet<u64>,
}

impl ReadMarker {
    pub fn is_read(&self, seq: u64) -> bool {
        seq <= self.below || self.read.contains(&seq)
    }

    /// Mark the message at `seq` read.
    pub fn mark(&mut self, seq: u64) {
        if seq > self.below {
            self.read.insert(seq);
        }
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

/// What a human holds back of its notifications for a while, on all its devices: kept under the human's key in
/// [`NOTIFY_BUCKET`], updated with compare-and-set by `ants notify snooze|resume` and `ants thread mute|unmute`, and
/// read by `ants webhook serve` before each notice. Each hold ends at its deadline, with no one there to lift it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyPrefs {
    /// Until when every notice the human's config does not let through (`bypass`) is held back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snoozed_until: Option<DateTime<Utc>>,
    /// The muted threads, each until when: none of their notices are sent.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub muted: std::collections::BTreeMap<ThreadId, DateTime<Utc>>,
}

impl NotifyPrefs {
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
        assert_eq!(marker, ReadMarker { below: 9, read: Default::default() });
        marker.mark(2);
        assert!(marker.read.is_empty(), "already read");
        let json = serde_json::to_string(&marker).unwrap();
        assert_eq!(json, r#"{"below":9}"#);
        assert_eq!(serde_json::from_str::<ReadMarker>("{}").unwrap(), ReadMarker::default());
    }

    #[test]
    fn channel_read_markers_keep_a_marker_per_channel() {
        let (ops, dev) = ("ops".parse::<ChannelId>().unwrap(), "dev".parse::<ChannelId>().unwrap());
        let mut markers = ChannelReadMarkers::default();
        markers.mark(&ops, 7);
        markers.mark(&ops, 9);
        assert!(markers.is_read(&ops, 7) && !markers.is_read(&ops, 8) && !markers.is_read(&dev, 7));
        markers.compact(&ops, [7, 8, 9]);
        assert_eq!(markers.channels[&ops], ReadMarker { below: 7, read: [9].into() }, "8 holds the marker back");
        markers.mark_through(&ops, 8);
        assert_eq!(markers.channels[&ops], ReadMarker { below: 8, read: [9].into() });
        markers.mark_through(&ops, 9);
        markers.mark_through(&ops, 3);
        assert_eq!(markers.channels[&ops], ReadMarker { below: 9, read: Default::default() }, "never moves back");
        markers.compact(&dev, [1, 2]);
        assert!(!markers.channels.contains_key(&dev), "no marker for a channel with nothing read");
        assert_eq!(serde_json::to_string(&markers).unwrap(), r#"{"channels":{"ops":{"below":9}}}"#);
        assert_eq!(serde_json::from_str::<ChannelReadMarkers>("{}").unwrap(), ChannelReadMarkers::default());
    }
}
