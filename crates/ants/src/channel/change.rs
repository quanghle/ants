//! A change to a channel's entry as a human makes it: `ants channel create`, `set`, `add` and `remove`, and in `ants
//! chat` the channel view's members editor and `/channel`. The same checks refuse it, and the same compare-and-set
//! update ([`super::update`]) writes it.

use ants_proto::{ChannelEntry, ChannelId, Humans, JsDomain, TeamDirectory, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream::{self, kv};

use crate::ants::colony;
use crate::people::humans::{self, Me};

/// What `ants channel create`, `set`, `add`, `remove` and `delete` do to a channel, and ants chat's `/channel` with
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Manage {
    /// Create the channel as the change gives it; fails when the channel exists.
    Create(Change),
    Change(Change),
    /// Delete the channel, its posts staying in the stream.
    Delete,
}

/// What a [`Manage`] did: the line `ants channel` prints, and what it warns of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Managed {
    pub line: String,
    pub warnings: Vec<String>,
}

impl Manage {
    /// Do it to `channel` as `me`, the teams read in JetStream domain `domain`.
    pub async fn run(
        &self,
        js: &jetstream::Context,
        me: &Me,
        domain: Option<&JsDomain>,
        channel: &ChannelId,
    ) -> Result<Managed> {
        let kv = super::open(js).await?;
        let (Self::Create(change) | Self::Change(change)) = self else {
            super::delete(&kv, channel).await?;
            let line = format!("deleted #{channel}; the posts to #{channel} stay in the stream");
            return Ok(Managed { line, warnings: Vec::new() });
        };
        let humans = humans::load(js, me).await;
        // A change removing members alone names no one a team could be.
        let removing = *change == Change { remove: change.remove.clone(), ..Change::default() };
        let teams = if removing { None } else { teams(js, domain).await };
        if let Self::Change(change) = self {
            let changed = change.write(js, &kv, channel, &humans, teams.as_ref()).await?;
            return Ok(Managed { line: super::describe(&changed.entry), warnings: changed.warnings });
        }
        change.check(channel, &humans, teams.as_ref())?;
        let mut entry = ChannelEntry::new(channel.clone());
        change.apply(&mut entry);
        super::create(&kv, &entry).await?;
        let named = entry.members.iter().chain(&entry.messenger).chain(&entry.owner);
        let ants: Vec<&UserId> = named.filter(|u| !humans.contains(u)).collect();
        let warnings = colony::unserved_warnings(js, &ants).await;
        Ok(Managed { line: format!("created {}", super::describe(&entry)), warnings })
    }
}

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

/// The team directory a change is checked against ([`Change::check`]): None when the bus keeps no teams, or, warned
/// of, when it cannot be read.
pub async fn teams(js: &jetstream::Context, js_domain: Option<&ants_proto::JsDomain>) -> Option<TeamDirectory> {
    crate::teams::read_of(js, js_domain).await.unwrap_or_else(|e| {
        tracing::warn!("not checking whether the users named are teams: {e:#}");
        None
    })
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
    /// ([`super::check_owner_not_human`]), a team of `teams` as a member, the messenger or the owner, and a
    /// description over [`ants_proto::MAX_CHANNEL_DESCRIPTION`] characters.
    pub fn check(&self, channel: &ChannelId, humans: &Humans, teams: Option<&TeamDirectory>) -> Result<()> {
        super::check_messenger_not_human(self.messenger.as_ref().and_then(Option::as_ref), humans)?;
        super::check_owner_not_human(self.owner.as_ref().and_then(Option::as_ref), humans)?;
        let named = self.members.iter().flatten().chain(&self.add);
        let mut named = named.chain(self.messenger.iter().flatten()).chain(self.owner.iter().flatten());
        if let Some((teams, team)) = teams.and_then(|teams| named.find(|u| teams.is_team(u)).map(|team| (teams, team)))
        {
            let lead = teams.lead_of(team).map_or("the team's lead".to_owned(), |lead| format!("its lead, {lead}"));
            bail!(
                "{team} is a team, which is no channel member: an @{team} in a post of #{channel} mentions {lead}, \
                 who has to be a member"
            );
        }
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
    /// humans from the ants and `teams` the teams.
    pub async fn write(
        &self,
        js: &jetstream::Context,
        kv: &kv::Store,
        channel: &ChannelId,
        humans: &Humans,
        teams: Option<&TeamDirectory>,
    ) -> Result<Changed> {
        self.check(channel, humans, teams)?;
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
