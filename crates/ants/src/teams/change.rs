//! A change of one team as a human writes it, from `ants chat`'s team editor: members added or removed, the lead
//! handed to another direct ant member or cleared, the owner and the description set or cleared, checked as `ants team add`,
//! `remove` and `set` check them and written by compare-and-set.

use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::teams::modify;
use ants_proto::{Humans, JsDomain, Team, TeamRefusal, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;

/// What to change of a team; what is left as none or empty stays.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Change {
    /// Ants of the bus or teams to add, each on no team.
    pub add: Vec<UserId>,
    /// Members to remove; removing the lead clears it.
    pub remove: Vec<UserId>,
    /// The new lead, one of the team's direct ant members, or none: the team then takes the lead of the nearest team
    /// above it with one.
    pub lead: Option<Option<UserId>>,
    /// The owner, a human, or none.
    pub owner: Option<Option<UserId>>,
    /// The description, or none.
    pub description: Option<Option<String>>,
}

/// What a change did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changed {
    pub added: Vec<UserId>,
    pub removed: Vec<UserId>,
    /// The team as written.
    pub team: Option<Team>,
}

impl Change {
    /// Refuse what is wrong with the change of `team` before anything is read of the bus: a human as a member, an
    /// owner who is no human of `humans`, a description too long or holding control characters.
    pub fn check(&self, team: &UserId, humans: &Humans) -> Result<()> {
        if self.add.is_empty()
            && self.remove.is_empty()
            && self.lead.is_none()
            && self.owner.is_none()
            && self.description.is_none()
        {
            bail!("nothing to change of team {team}");
        }
        for member in &self.add {
            super::check_not_human(member, humans)?;
        }
        if let Some(Some(owner)) = &self.owner {
            super::check_owner(owner, humans)?;
        }
        if let Some(Some(description)) = &self.description {
            Team::new(team.clone(), team.clone(), team.clone()).set_description(Some(description))?;
        }
        Ok(())
    }

    /// Write the change to `team` in the directory of `kv`, after [`Change::check`]: the members to add each an ant of
    /// the bus or a team, added before the lead is set, the members to remove after.
    pub async fn write(
        &self,
        js: &jetstream::Context,
        js_domain: Option<&JsDomain>,
        kv: &DirectKv,
        team: &UserId,
        humans: &Humans,
    ) -> Result<Changed> {
        self.check(team, humans)?;
        let known = super::known_ants(js, js_domain, &self.add.iter().collect::<Vec<_>>()).await?;
        modify(kv, |directory| {
            super::check_known(directory, &known, &self.add)?;
            let mut changed = Changed::default();
            for member in &self.add {
                if directory.add(team, member)? {
                    changed.added.push(member.clone());
                }
            }
            match &self.lead {
                Some(Some(lead)) => _ = directory.set_lead(team, lead)?,
                Some(None) => _ = directory.clear_lead(team)?,
                None => {}
            }
            let Some(entry) = directory.teams.iter_mut().find(|t| t.team == *team) else {
                return Err(TeamRefusal::NoSuchTeam { team: team.clone() });
            };
            if let Some(owner) = &self.owner {
                entry.owner.clone_from(owner);
            }
            if let Some(description) = &self.description {
                entry
                    .set_description(description.as_deref())
                    .map_err(|e| TeamRefusal::Invalid { why: e.to_string() })?;
            }
            for member in &self.remove {
                if directory.remove(team, member)? {
                    changed.removed.push(member.clone());
                }
            }
            changed.team = directory.team(team).cloned();
            Ok(changed)
        })
        .await
    }

    /// What the change did to `team`, in a line.
    pub fn line(&self, team: &UserId, changed: &Changed) -> String {
        let names = |users: &[UserId]| users.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ");
        let mut parts = Vec::new();
        if !changed.added.is_empty() {
            parts.push(format!("added {}", names(&changed.added)));
        }
        if !changed.removed.is_empty() {
            parts.push(format!("removed {}", names(&changed.removed)));
        }
        match &self.lead {
            Some(Some(lead)) => parts.push(format!("lead now {lead}")),
            Some(None) => parts.push("no lead of its own".to_owned()),
            None => {}
        }
        match &self.owner {
            Some(Some(owner)) => parts.push(format!("owner now {owner}")),
            Some(None) => parts.push("no owner".to_owned()),
            None => {}
        }
        match (&self.description, changed.team.as_ref().and_then(|t| t.description.as_ref())) {
            (Some(_), Some(_)) => parts.push("description changed".to_owned()),
            (Some(_), None) => parts.push("no description".to_owned()),
            (None, _) => {}
        }
        if parts.is_empty() {
            format!("nothing changed in team {team}")
        } else {
            format!("team {team}: {}", parts.join(", "))
        }
    }
}
