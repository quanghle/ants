//! The channels bucket as a daemon reads it: watched for changes, one entry loaded, or all of them listed.

use std::collections::HashMap;

use ants_proto::{ChannelEntry, ChannelId};
use anyhow::{Context as _, Result};
use async_nats::jetstream::kv;
use futures::{StreamExt as _, TryStreamExt as _};

use crate::bus::relayed::{self, GuardedKv as _};

/// The channels in the bucket, kept current by a watch. An entry whose channel disagrees with its key is skipped, and
/// so is a malformed one: neither is a channel.
#[derive(Default)]
pub struct Channels {
    entries: HashMap<ChannelId, ChannelEntry>,
    kv: Option<kv::Store>,
    updates: Option<relayed::Watch>,
}

impl Channels {
    /// The channels in `kv`, watched for changes. The watch starts first, so no change made while the current entries
    /// are read is missed.
    pub async fn watch(kv: kv::Store) -> Result<Self> {
        let updates = kv.guarded_watch(">").await.context("watching the channels")?;
        let mut channels = Self { entries: HashMap::new(), kv: Some(kv.clone()), updates: Some(updates) };
        let keys: Vec<String> = kv.keys().await.context("listing the channels")?.try_collect().await?;
        for key in keys {
            let value = kv.guarded_get(&key).await.with_context(|| format!("reading channel {key}"))?;
            channels.set(&key, value.as_deref());
        }
        Ok(channels)
    }

    pub fn get(&self, channel: &ChannelId) -> Option<&ChannelEntry> {
        self.entries.get(channel)
    }

    /// Read the entry of `channel` again, for a post that may have overtaken a change of it, as a post made right after
    /// its author was added to the channel does.
    pub async fn refresh(&mut self, channel: &ChannelId) {
        let Some(kv) = &self.kv else { return };
        match kv.guarded_get(channel.as_str()).await {
            Ok(value) => self.set(channel.as_str(), value.as_deref()),
            Err(e) => tracing::warn!(%channel, error = %e, "reading the channel"),
        }
    }

    /// Apply the next change of the bucket. Never resolves when nothing is watched.
    pub async fn changed(&mut self) {
        let Some(updates) = &mut self.updates else { return std::future::pending().await };
        match updates.next().await {
            Some(Ok(entry)) => {
                let value = (entry.operation == kv::Operation::Put).then_some(&entry.value[..]);
                self.set(&entry.key, value);
            }
            Some(Err(e)) => tracing::warn!(error = %e, "watching the channels"),
            None => {
                tracing::warn!("the watch of the channels ended; channel changes are seen after a restart");
                self.updates = None;
            }
        }
    }

    /// Set the channel of `key` to `value`, or remove it.
    fn set(&mut self, key: &str, value: Option<&[u8]>) {
        let Ok(channel) = key.parse::<ChannelId>() else {
            tracing::warn!(key, "skipping a channel entry under a key that is no channel id");
            return;
        };
        let Some(value) = value else {
            self.entries.remove(&channel);
            return;
        };
        match parse_entry(key, value) {
            Ok(entry) => {
                self.entries.insert(channel, entry);
            }
            Err(error) => {
                tracing::warn!(key, error, "skipping a channel entry");
                self.entries.remove(&channel);
            }
        }
    }
}

/// The entry `value` holds for the channel of `key`, or why it holds none: malformed, or naming another channel.
fn parse_entry(key: &str, value: &[u8]) -> Result<ChannelEntry, String> {
    let entry = serde_json::from_slice::<ChannelEntry>(value).map_err(|e| e.to_string())?;
    entry.check_key(key).map_err(|e| e.to_string())?;
    Ok(entry)
}

/// What is stored under a channel's key.
pub enum Stored {
    Missing,
    /// An entry for the key's channel, at a revision.
    Entry(ChannelEntry, u64),
    /// A malformed entry, or one naming another channel, at a revision: readers skip it.
    Unreadable(u64),
}

/// Read the entry of `channel` from the channels bucket `kv`, with its revision.
pub async fn load(kv: &kv::Store, channel: &ChannelId) -> Result<Stored> {
    let key = channel.as_str();
    let Some(found) = kv.guarded_entry(key).await.with_context(|| format!("reading channel {channel}"))? else {
        return Ok(Stored::Missing);
    };
    if found.operation != kv::Operation::Put {
        return Ok(Stored::Missing);
    }
    match parse_entry(key, &found.value) {
        Ok(entry) => Ok(Stored::Entry(entry, found.revision)),
        Err(error) => {
            tracing::warn!(key, error, "skipping a channel entry that is malformed or names another channel");
            Ok(Stored::Unreadable(found.revision))
        }
    }
}

/// Every channel in the channels bucket `kv`, by id. Entries readers skip are left out with a warning.
pub async fn list(kv: &kv::Store) -> Result<Vec<ChannelEntry>> {
    let mut keys: Vec<String> = kv.keys().await.context("listing the channels")?.try_collect().await?;
    keys.sort();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let Ok(channel) = key.parse::<ChannelId>() else {
            tracing::warn!(key, "skipping a channel entry under a key that is no channel id");
            continue;
        };
        if let Stored::Entry(entry, _) = load(kv, &channel).await? {
            entries.push(entry);
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::tests::ops;

    #[test]
    fn entries_under_another_key_or_malformed_are_no_channels() {
        let mut channels = Channels::default();
        let entry = ops();
        let json = serde_json::to_vec(&entry).unwrap();
        channels.set("ops", Some(&json));
        assert_eq!(channels.get(&entry.channel), Some(&entry));
        channels.set("dev", Some(&json));
        assert_eq!(channels.get(&"dev".parse().unwrap()), None, "an entry of ops under dev");
        channels.set("ops", Some(b"{"));
        assert_eq!(channels.get(&entry.channel), None, "malformed: the channel is gone");
        channels.set("ops", Some(&json));
        channels.set("ops", None);
        assert_eq!(channels.get(&entry.channel), None, "deleted");
    }
}
