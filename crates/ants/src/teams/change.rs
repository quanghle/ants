//! The teams as a human writes them, from `ants team` and `ants chat`: a team created ([`Create`]) or deleted
//! ([`delete`]), and a change of one ([`Change`]): members added or removed, the lead handed to another direct ant
//! member or cleared, the owner and the description set or cleared. Each is checked before anything is read of the
//! bus and written by compare-and-set; [`tell`] then tells observers.

use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::teams::modify;
use ants_proto::{Humans, JsDomain, Team, TeamChange, TeamRefusal, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;

/// A team to create: led by `lead`, its first member, or without one by the lead of the team above it once it is put
/// under one; with the other members `members`, the owner `owner` and the description `description`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    pub team: UserId,
    /// An ant of the bus on no team.
    pub lead: Option<UserId>,
    /// Ants of the bus or teams, each on no team.
    pub members: Vec<UserId>,
    /// A human.
    pub owner: Option<UserId>,
    pub description: Option<String>,
}

impl Create {
    /// The team as `by` creates it, refusing what is wrong with it before anything is read of the bus: a human as its
    /// lead or a member, an owner who is no human of `humans`, a description too long or holding control characters.
    pub fn check(&self, by: &UserId, humans: &Humans) -> Result<Team> {
        let mut team = match &self.lead {
            Some(lead) => Team::new(self.team.clone(), lead.clone(), by.clone()),
            None => Team::unled(self.team.clone(), by.clone()),
        };
        team.set_description(self.description.as_deref())?;
        for member in self.lead.iter().chain(&self.members) {
            super::check_not_human(member, humans)?;
        }
        if let Some(owner) = &self.owner {
            super::check_owner(owner, humans)?;
        }
        team.owner.clone_from(&self.owner);
        team.members.extend(self.members.iter().filter(|m| Some(*m) != self.lead.as_ref()).cloned());
        Ok(team)
    }

    /// Create the team in the directory of `kv` as `by`, after [`Create::check`]: refused when a team, an ant or a
    /// human holds its name, or a member is neither an ant of the bus nor a team. The team, and the lines saying what
    /// its creation did.
    pub async fn write(
        &self,
        js: &jetstream::Context,
        js_domain: Option<&JsDomain>,
        kv: &DirectKv,
        by: &UserId,
        humans: &Humans,
    ) -> Result<(Team, Vec<String>)> {
        let created = self.check(by, humans)?;
        let known = super::known_ants(js, js_domain, &created.members.iter().collect::<Vec<_>>()).await?;
        super::check_team_name(js, js_domain, humans, &created.team).await?;
        let lines = modify(kv, |directory| {
            super::check_known(directory, &known, &created.members)?;
            if let Some(lead) = created.lead.as_ref().filter(|l| directory.is_team(l)) {
                return Err(TeamRefusal::NotLeadable { ant: lead.clone(), team: created.team.clone() });
            }
            directory.create(created.clone(), None)
        })
        .await?;
        Ok((created, lines))
    }
}

/// Delete `team` from the directory of `kv`: it leaves the team holding it, and its members are on no team. The lines
/// saying what it did.
pub async fn delete(kv: &DirectKv, team: &UserId) -> Result<Vec<String>> {
    modify(kv, |directory| directory.delete(team)).await
}

/// Tell observers what befell the teams of `changes`, which `by` wrote in the directory of `kv`, on `by`'s status, the
/// directory read back ([`ants_daemon::teams::tell`]): best effort, as all status is.
pub async fn tell(client: &async_nats::Client, by: &UserId, kv: &DirectKv, changes: &[(UserId, TeamChange)]) {
    if let Some(directory) = ants_daemon::teams::read_quickly(kv).await {
        ants_daemon::teams::tell(client, by, &directory, changes);
        if let Err(e) = client.flush().await {
            tracing::debug!(error = %e, "telling the team change");
        }
    }
}

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
