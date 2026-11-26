//! The teams as a human writes them, with no I/O: a team created ([`Create`]) and a change of one ([`Change`]): members
//! added or removed, the lead handed to another direct ant member or cleared, the owner and the description set or
//! cleared. Each is checked first against the humans of the bus ([`Create::check`], [`Change::check`]), then applied
//! to the directory as read, the ants of the bus known ([`create`], [`Change::apply`]); its writer stores the result by
//! compare-and-set.

use std::collections::BTreeSet;

use ants_proto::{Humans, Team, TeamDirectory, TeamRefusal, UserId};
use serde::{Deserialize, Serialize};

use super::TeamError;

/// A team to create: led by `lead`, its first member, or without one by the lead of the team above it once it is put
/// under one; with the other members `members`, the owner `owner` and the description `description`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Create {
    pub team: UserId,
    /// An ant of the bus on no team.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lead: Option<UserId>,
    /// Ants of the bus or teams, each on no team.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<UserId>,
    /// A human.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Create {
    /// The team as `by` creates it, refusing what is wrong with it before the directory is read: a human as its lead
    /// or a member, an owner who is no human of `humans`, a description too long or holding control characters.
    pub fn check(&self, by: &UserId, humans: &Humans) -> Result<Team, TeamError> {
        let mut team = match &self.lead {
            Some(lead) => Team::new(self.team.clone(), lead.clone(), by.clone()),
            None => Team::unled(self.team.clone(), by.clone()),
        };
        team.set_description(self.description.as_deref()).map_err(|e| TeamError::Invalid(e.to_string()))?;
        for member in self.lead.iter().chain(&self.members) {
            check_not_human(member, humans)?;
        }
        if let Some(owner) = &self.owner {
            check_owner(owner, humans)?;
        }
        team.owner.clone_from(&self.owner);
        team.members.extend(self.members.iter().filter(|m| Some(*m) != self.lead.as_ref()).cloned());
        Ok(team)
    }

    /// The names whose humans [`Create::check`] and [`check_name`] look at: the team's, its lead's, its members' and its
    /// owner's.
    pub fn names(&self) -> Vec<UserId> {
        let names = std::iter::once(&self.team).chain(&self.lead).chain(&self.members).chain(&self.owner);
        names.cloned().collect()
    }
}

/// Create `created`, checked ([`Create::check`]), in `directory`: refused when a team holds its name, a member is
/// neither an ant of `known` nor a team, or its lead is a team. The lines saying what its creation did.
pub fn create(
    directory: &mut TeamDirectory,
    created: &Team,
    known: &BTreeSet<UserId>,
) -> Result<Vec<String>, TeamRefusal> {
    check_known(directory, known, &created.members)?;
    if let Some(lead) = created.lead.as_ref().filter(|l| directory.is_team(l)) {
        return Err(TeamRefusal::NotLeadable { ant: lead.clone(), team: created.team.clone() });
    }
    directory.create(created.clone(), None)
}

/// What to change of a team; what is left as none or empty stays.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    /// Ants of the bus or teams to add, each on no team.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add: Vec<UserId>,
    /// Members to remove; removing the lead clears it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove: Vec<UserId>,
    /// The new lead, one of the team's direct ant members, or none: the team then takes the lead of the nearest team
    /// above it with one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lead: Option<Option<UserId>>,
    /// The owner, a human, or none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<Option<UserId>>,
    /// The description, or none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<Option<String>>,
}

/// What a change did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changed {
    pub added: Vec<UserId>,
    pub removed: Vec<UserId>,
    /// The team as written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<Team>,
}

impl Change {
    /// Refuse what is wrong with the change of `team` before the directory is read: nothing to change, a human as a
    /// member, an owner who is no human of `humans`, a description too long or holding control characters.
    pub fn check(&self, team: &UserId, humans: &Humans) -> Result<(), TeamError> {
        if self.add.is_empty()
            && self.remove.is_empty()
            && self.lead.is_none()
            && self.owner.is_none()
            && self.description.is_none()
        {
            return Err(TeamError::Invalid(format!("nothing to change of team {team}")));
        }
        for member in &self.add {
            check_not_human(member, humans)?;
        }
        if let Some(Some(owner)) = &self.owner {
            check_owner(owner, humans)?;
        }
        if let Some(Some(description)) = &self.description {
            Team::new(team.clone(), team.clone(), team.clone())
                .set_description(Some(description))
                .map_err(|e| TeamError::Invalid(e.to_string()))?;
        }
        Ok(())
    }

    /// The names whose humans [`Change::check`] looks at: the members to add and the owner.
    pub fn names(&self) -> Vec<UserId> {
        self.add.iter().chain(self.owner.iter().flatten()).cloned().collect()
    }

    /// Apply the change, checked ([`Change::check`]), to `team` in `directory`: the members to add each an ant of
    /// `known` or a team, added before the lead is set, the members to remove after.
    pub fn apply(
        &self,
        directory: &mut TeamDirectory,
        team: &UserId,
        known: &BTreeSet<UserId>,
    ) -> Result<Changed, TeamRefusal> {
        check_known(directory, known, &self.add)?;
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
            entry.set_description(description.as_deref()).map_err(|e| TeamRefusal::Invalid { why: e.to_string() })?;
        }
        for member in &self.remove {
            if directory.remove(team, member)? {
                changed.removed.push(member.clone());
            }
        }
        changed.team = directory.team(team).cloned();
        Ok(changed)
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

/// Refuse a member that is a human: humans are no team's members, at most a team's owner.
pub fn check_not_human(member: &UserId, humans: &Humans) -> Result<(), TeamError> {
    if humans.contains(member) {
        return Err(TeamError::Invalid(format!(
            "{member} is a human: a team's members are ants and teams, and a human at most its owner (`--owner`)"
        )));
    }
    Ok(())
}

/// Refuse an owner that is no human of `humans`.
pub fn check_owner(owner: &UserId, humans: &Humans) -> Result<(), TeamError> {
    if !humans.contains(owner) {
        return Err(TeamError::Invalid(format!(
            "{owner} is no human of the bus (`ants auth user {owner} --human`): a team's owner is a human"
        )));
    }
    Ok(())
}

/// Refuse `team` as the name of a new team when it is reserved, or an ant of `known` or a human of `humans` holds it:
/// teams share the names of ants and humans.
pub fn check_name(team: &UserId, humans: &Humans, known: &BTreeSet<UserId>) -> Result<(), TeamError> {
    if team.is_reserved() {
        return Err(TeamError::Invalid(format!("{team} is a reserved name; choose another name")));
    }
    if humans.contains(team) || known.contains(team) {
        return Err(TeamRefusal::NameTaken { name: team.clone() }.into());
    }
    Ok(())
}

/// Refuse the members of `members` that are neither teams of `directory` nor ants of `known`.
pub fn check_known(directory: &TeamDirectory, known: &BTreeSet<UserId>, members: &[UserId]) -> Result<(), TeamRefusal> {
    match members.iter().find(|m| !directory.is_team(m) && !known.contains(*m)) {
        Some(name) => Err(TeamRefusal::UnknownAnt { name: name.clone() }),
        None => Ok(()),
    }
}
