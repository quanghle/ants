//! A change to a channel's entry as a human makes it: `ants channel create`, `set`, `add` and `remove`, and in `ants
//! chat` the channel view's members editor. The same checks refuse it, and the same compare-and-set update
//! ([`super::update`]) writes it.

use ants_proto::{ChannelEntry, ChannelId, Humans, UserId};
use anyhow::Result;
use async_nats::jetstream::{self, kv};

use crate::colony;

/// What to change of a channel's entry; what is not given stays.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Change {
    /// The members, replacing them all.
    pub members: Option<Vec<UserId>>,
    /// Members to add, ants or humans, after any replacing.
    pub add: Vec<UserId>,
    pub remove: Vec<UserId>,
    /// The messenger, or with none the default one.
    pub messenger: Option<Option<UserId>>,
    /// The owner, or with none no owner.
    pub owner: Option<Option<UserId>>,
    /// The description, or with none (or a blank one) no description.
    pub description: Option<Option<String>>,
}

/// What a change written did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changed {
    /// The entry stored.
    pub entry: ChannelEntry,
    /// The members added and removed, of those the change named.
    pub added: Vec<UserId>,
    pub removed: Vec<UserId>,
    /// What `ants channel` warns of: members named that already were members, or were none, and the ants named that
    /// no daemon serves now.
    pub warnings: Vec<String>,
}

impl Change {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Refuse a human as the messenger ([`super::check_messenger_not_human`]) or the owner
    /// ([`super::check_owner_not_human`]), and a description over [`ants_proto::MAX_CHANNEL_DESCRIPTION`] characters.
    pub fn check(&self, channel: &ChannelId, humans: &Humans) -> Result<()> {
        super::check_messenger_not_human(self.messenger.as_ref().and_then(Option::as_ref), humans)?;
        super::check_owner_not_human(self.owner.as_ref().and_then(Option::as_ref), humans)?;
        if let Some(description) = &self.description {
            ChannelEntry::new(channel.clone()).set_description(description.as_deref())?;
        }
        Ok(())
    }

    /// Make the change to `entry`, once checked ([`Change::check`]): the members added and those removed.
    pub fn apply(&self, entry: &mut ChannelEntry) -> (Vec<UserId>, Vec<UserId>) {
        if let Some(members) = &self.members {
            entry.members.clear();
            for member in members {
                entry.add_member(member.clone());
            }
        }
        let added = self.add.iter().filter(|u| entry.add_member((*u).clone())).cloned().collect();
        let removed = self.remove.iter().filter(|u| entry.remove_member(u)).cloned().collect();
        if let Some(messenger) = &self.messenger {
            entry.messenger.clone_from(messenger);
        }
        if let Some(owner) = &self.owner {
            entry.owner.clone_from(owner);
        }
        if let Some(description) = &self.description {
            let _ = entry.set_description(description.as_deref());
        }
        (added, removed)
    }

    /// Check the change and write it to the entry of `channel` in `kv`, the channels bucket, `humans` telling the
    /// humans from the ants.
    pub async fn write(
        &self,
        js: &jetstream::Context,
        kv: &kv::Store,
        channel: &ChannelId,
        humans: &Humans,
    ) -> Result<Changed> {
        self.check(channel, humans)?;
        let (entry, (added, removed)) = super::update(kv, channel, |entry| self.apply(entry)).await?;
        let mut warnings: Vec<String> = Vec::new();
        for user in self.add.iter().filter(|u| !added.contains(u)) {
            warnings.push(format!("{user} is already a member of #{channel}"));
        }
        for user in self.remove.iter().filter(|u| !removed.contains(u)) {
            warnings.push(format!("{user} is no member of #{channel}"));
        }
        let named = self.members.iter().flatten().chain(&added);
        let named = named.chain(self.messenger.iter().flatten()).chain(self.owner.iter().flatten());
        let ants: Vec<&UserId> = named.filter(|u| !humans.contains(u)).collect();
        warnings.extend(colony::unserved_warnings(js, &ants).await);
        Ok(Changed { entry, added, removed, warnings })
    }
}
