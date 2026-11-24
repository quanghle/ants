//! Teams: named groups of ants and teams, each with an ant lead, kept together in one [`TeamDirectory`] under
//! [`TEAM_DIRECTORY_KEY`] of [`TEAMS_BUCKET`]. Across the bus each ant and each team is a member of at most one team and
//! no team holds itself, so teams form a forest; a team's id shares the namespace of ants and humans, so `@backend`
//! may name a team wherever an ant is named, and a message to a team goes to its lead ([`Envelope::team`]).
//!
//! Humans write the directory by compare-and-set; ants read it, and change it only through `ants signer`
//! ([`TeamAdminRequest`]), within what a lead may change of the teams it manages ([`TeamDirectory::apply_for_ant`]).

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{Envelope, TEAMS_BUCKET};
use crate::{Error, MAX_TEAM_DESCRIPTION, UserId};

mod admin;

pub use admin::{TeamAdminOutcome, TeamAdminReply, TeamAdminRequest, TeamOp};

/// The key of the one entry of [`TEAMS_BUCKET`], the [`TeamDirectory`].
pub const TEAM_DIRECTORY_KEY: &str = "directory";

/// A team: a named group of ants and teams with a lead, as the [`TeamDirectory`] holds it.
///
/// Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Team {
    /// The team's id, of user id syntax, in the namespace of ants and humans.
    pub team: UserId,
    /// What the team is for, up to [`MAX_TEAM_DESCRIPTION`] characters, which each member's daemon adds to the first
    /// prompt of its sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The team's direct members, each an ant or a team (a member is a team when the directory holds a team of its
    /// name), in the order they were added.
    #[serde(default)]
    pub members: Vec<UserId>,
    /// The ant messages to the team go to, one of its direct ant members. None only once the lead was revoked, until a
    /// human names another: the team is then leaderless ([`Team::leaderless`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lead: Option<UserId>,
    /// The human the team answers to, if any; only a human sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The avatar's seed a human set, in place of the team's id ([`Team::avatar_seed`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    /// The avatar's colour a human set, one of [`crate::HUE_NAMES`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// Who created the team: a human, or the lead that created it beneath a team it manages.
    pub created_by: UserId,
    pub created_at: DateTime<Utc>,
}

impl Team {
    /// A team led by `lead`, its one member, created by `by` now.
    pub fn new(team: UserId, lead: UserId, by: UserId) -> Self {
        Self {
            team,
            description: None,
            members: vec![lead.clone()],
            lead: Some(lead),
            owner: None,
            avatar: None,
            color: None,
            created_by: by,
            created_at: Utc::now(),
        }
    }

    /// Whether the team has lost its lead to a revocation: messages to it are refused until a human names another.
    pub fn leaderless(&self) -> bool {
        self.lead.is_none()
    }

    pub fn is_member(&self, member: &UserId) -> bool {
        self.members.contains(member)
    }

    /// The seed of the team's avatar: the one a human set, else its id.
    pub fn avatar_seed(&self) -> &str {
        self.avatar.as_deref().unwrap_or(self.team.as_str())
    }

    /// Set the description to `text`, trimmed; none or a blank one clears it. Returns whether it changed; one longer
    /// than [`MAX_TEAM_DESCRIPTION`], or holding control characters but whitespace or characters reordering text
    /// ([`crate::shown`]), is refused, changing nothing.
    pub fn set_description(&mut self, text: Option<&str>) -> Result<bool, Error> {
        let text = text.map(str::trim).filter(|t| !t.is_empty());
        if let Some(count) = text.map(|t| t.chars().count()).filter(|n| *n > MAX_TEAM_DESCRIPTION) {
            return Err(Error::LongDescription(count));
        }
        if let Some(text) = text.filter(|t| !t.chars().all(crate::text::shown_in_lines)) {
            return Err(Error::InvalidDescription(text.to_owned()));
        }
        let text = text.map(str::to_owned);
        if self.description == text {
            return Ok(false);
        }
        self.description = text;
        Ok(true)
    }
}

/// Why a change of the teams is refused, or a directory read is broken.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum TeamRefusal {
    #[error("no team {team}")]
    NoSuchTeam { team: UserId },
    #[error("team {team} exists already")]
    TeamExists { team: UserId },
    #[error("the directory holds team {team} twice")]
    TeamTwice { team: UserId },
    #[error("{member} is on team {team} already; each ant and team is on one team at most")]
    OnTeam { member: UserId, team: UserId },
    #[error("{member} is on team {team} twice")]
    MemberTwice { member: UserId, team: UserId },
    #[error("team {team} would hold itself through team {member}")]
    HoldsItself { team: UserId, member: UserId },
    #[error("{member} is not a member of team {team}")]
    NotMember { member: UserId, team: UserId },
    #[error("{ant} cannot lead team {team}: the lead is one of its direct ant members")]
    NotLeadable { ant: UserId, team: UserId },
    #[error("{lead} leads team {team}; name another lead first (`ants team set {team} --lead ANT`)")]
    RemovesLead { lead: UserId, team: UserId },
    #[error("{ant} manages neither team {team} nor a team above it")]
    NotManager { ant: UserId, team: UserId },
    #[error("team {team} is not a member of team {parent}")]
    NotBeneath { team: UserId, parent: UserId },
    #[error("{name} is no ant of the bus")]
    UnknownAnt { name: UserId },
    #[error("the name {name} is taken by an ant or a human")]
    NameTaken { name: UserId },
    #[error("the bus keeps no ants_teams (`ants init` creates it)")]
    NoDirectory,
    #[error("the team directory is unreadable; only a human sets it again: {why}")]
    Unreadable { why: String },
    #[error("the team directory kept changing meanwhile; try again")]
    Busy,
    #[error("the request expired")]
    Expired,
    #[error("invalid request: {why}")]
    Invalid { why: String },
}

/// Every team of the bus, the one entry [`TEAM_DIRECTORY_KEY`] of [`TEAMS_BUCKET`], in the order teams were created.
///
/// Writers check the whole forest on every change ([`TeamDirectory::check`]) and store it by compare-and-set; readers
/// check it again, and take a directory breaking it as unreadable.
///
/// Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamDirectory {
    #[serde(default)]
    pub teams: Vec<Team>,
}

impl TeamDirectory {
    /// The directory `bytes` hold, checked ([`TeamDirectory::check`]).
    pub fn read(bytes: &[u8]) -> Result<Self, TeamRefusal> {
        let directory: Self =
            serde_json::from_slice(bytes).map_err(|e| TeamRefusal::Unreadable { why: e.to_string() })?;
        directory.check().map_err(|e| TeamRefusal::Unreadable { why: e.to_string() })?;
        Ok(directory)
    }

    /// Check the forest: no team twice, no member twice in a team nor on two teams, no team holding itself through its
    /// member teams, each lead a direct ant member of its team and each description in bounds. A leaderless team
    /// ([`Team::leaderless`]) breaks nothing here: it is broken for itself alone.
    pub fn check(&self) -> Result<(), TeamRefusal> {
        let mut ids = BTreeSet::new();
        for team in &self.teams {
            if !ids.insert(&team.team) {
                return Err(TeamRefusal::TeamTwice { team: team.team.clone() });
            }
        }
        let mut holder: HashMap<&UserId, &UserId> = HashMap::new();
        for team in &self.teams {
            if team.description.as_ref().is_some_and(|d| d.chars().count() > MAX_TEAM_DESCRIPTION) {
                return Err(TeamRefusal::Invalid { why: format!("the description of team {} is too long", team.team) });
            }
            for member in &team.members {
                if let Some(held) = holder.insert(member, &team.team) {
                    return Err(if *held == team.team {
                        TeamRefusal::MemberTwice { member: member.clone(), team: team.team.clone() }
                    } else {
                        TeamRefusal::OnTeam { member: member.clone(), team: held.clone() }
                    });
                }
            }
            if let Some(lead) = &team.lead
                && (!team.is_member(lead) || ids.contains(lead))
            {
                return Err(TeamRefusal::NotLeadable { ant: lead.clone(), team: team.team.clone() });
            }
        }
        for team in &self.teams {
            let mut at = &team.team;
            let mut steps = 0;
            while let Some(up) = holder.get(at) {
                steps += 1;
                if *up == &team.team || steps > self.teams.len() {
                    return Err(TeamRefusal::HoldsItself { team: team.team.clone(), member: at.clone() });
                }
                at = up;
            }
        }
        Ok(())
    }

    pub fn team(&self, id: &UserId) -> Option<&Team> {
        self.teams.iter().find(|t| t.team == *id)
    }

    fn team_mut(&mut self, id: &UserId) -> Result<&mut Team, TeamRefusal> {
        self.teams.iter_mut().find(|t| t.team == *id).ok_or_else(|| TeamRefusal::NoSuchTeam { team: id.clone() })
    }

    fn known(&self, id: &UserId) -> Result<&Team, TeamRefusal> {
        self.team(id).ok_or_else(|| TeamRefusal::NoSuchTeam { team: id.clone() })
    }

    pub fn is_team(&self, id: &UserId) -> bool {
        self.team(id).is_some()
    }

    /// The team holding `member`, an ant or a team, directly; None when it is on no team.
    pub fn holder(&self, member: &UserId) -> Option<&Team> {
        self.teams.iter().find(|t| t.is_member(member))
    }

    /// The teams holding `member`, the nearest first: its team, that team's, and so on to a top-level team.
    pub fn ancestors(&self, member: &UserId) -> Vec<&Team> {
        let mut chain: Vec<&Team> = Vec::new();
        let mut at = member;
        while let Some(team) = self.holder(at) {
            if chain.iter().any(|t| t.team == team.team) {
                break;
            }
            chain.push(team);
            at = &team.team;
        }
        chain
    }

    /// The top-level teams: those on no team, in order.
    pub fn roots(&self) -> Vec<&Team> {
        self.teams.iter().filter(|t| self.holder(&t.team).is_none()).collect()
    }

    /// The direct member teams of `team`, in order.
    pub fn subteams(&self, team: &Team) -> Vec<&Team> {
        team.members.iter().filter_map(|m| self.team(m)).collect()
    }

    /// The ants of `team` and of the teams beneath it, depth first in the order of members.
    pub fn ants_under(&self, team: &UserId) -> Vec<UserId> {
        let mut ants = Vec::new();
        let mut seen = BTreeSet::new();
        self.collect_ants(team, &mut ants, &mut seen);
        ants
    }

    fn collect_ants(&self, team: &UserId, ants: &mut Vec<UserId>, seen: &mut BTreeSet<UserId>) {
        let Some(team) = self.team(team) else { return };
        if !seen.insert(team.team.clone()) {
            return;
        }
        for member in &team.members {
            if self.is_team(member) {
                self.collect_ants(member, ants, seen);
            } else {
                ants.push(member.clone());
            }
        }
    }

    /// Whether `team` is `under` or beneath it, down the forest.
    pub fn is_beneath(&self, team: &UserId, under: &UserId) -> bool {
        team == under || self.ancestors(team).iter().any(|t| t.team == *under)
    }

    /// The teams `ant` leads, in order.
    pub fn led_by(&self, ant: &UserId) -> Vec<&Team> {
        self.teams.iter().filter(|t| t.lead.as_ref() == Some(ant)).collect()
    }

    /// Whether `ant` manages `team`: it leads the team or a team above it.
    pub fn manages(&self, ant: &UserId, team: &UserId) -> bool {
        let Some(own) = self.team(team) else { return false };
        own.lead.as_ref() == Some(ant) || self.ancestors(team).iter().any(|t| t.lead.as_ref() == Some(ant))
    }

    /// Whether `team` may take `member`: the member is on no team and, when a team, holds neither `team` nor a team
    /// above it.
    fn check_joins(&self, team: &UserId, member: &UserId) -> Result<(), TeamRefusal> {
        if let Some(held) = self.holder(member) {
            return Err(if held.team == *team {
                TeamRefusal::MemberTwice { member: member.clone(), team: team.clone() }
            } else {
                TeamRefusal::OnTeam { member: member.clone(), team: held.team.clone() }
            });
        }
        if self.is_team(member) && self.is_beneath(team, member) {
            return Err(TeamRefusal::HoldsItself { team: team.clone(), member: member.clone() });
        }
        Ok(())
    }

    /// Create `team`, which names its lead as one of its members: as a top-level team, or beneath `parent`. Every
    /// member is on no team before, and the lead an ant; a lead that is a direct member of `parent`, not its lead, moves
    /// down into the new team. Returns what changed.
    pub fn create(&mut self, team: Team, parent: Option<&UserId>) -> Result<Vec<String>, TeamRefusal> {
        let id = team.team.clone();
        if self.is_team(&id) {
            return Err(TeamRefusal::TeamExists { team: id });
        }
        let Some(lead) = team.lead.clone() else {
            return Err(TeamRefusal::Invalid { why: format!("team {id} needs a lead") });
        };
        if !team.is_member(&lead) || self.is_team(&lead) || lead == id {
            return Err(TeamRefusal::NotLeadable { ant: lead, team: id });
        }
        let mut next = self.clone();
        let mut changed = vec![format!("created team {id}, led by {lead}")];
        if let Some(parent) = parent {
            let from = next.team_mut(parent)?;
            if from.lead.as_ref() == Some(&lead) {
                return Err(TeamRefusal::RemovesLead { lead, team: parent.clone() });
            }
            if from.is_member(&lead) {
                from.members.retain(|m| *m != lead);
                changed.push(format!("moved {lead} from team {parent} to team {id}"));
            }
        }
        let mut seen = BTreeSet::new();
        for member in &team.members {
            if !seen.insert(member) {
                return Err(TeamRefusal::MemberTwice { member: member.clone(), team: id });
            }
            if *member == id {
                return Err(TeamRefusal::HoldsItself { team: id.clone(), member: id });
            }
            next.check_joins(&id, member)?;
        }
        next.teams.push(team);
        if let Some(parent) = parent {
            next.team_mut(parent)?.members.push(id.clone());
            changed.push(format!("added team {id} to team {parent}"));
        }
        next.check()?;
        *self = next;
        Ok(changed)
    }

    /// Delete `team`: it leaves the team holding it, and its members are on no team. Returns what changed.
    pub fn delete(&mut self, team: &UserId) -> Result<Vec<String>, TeamRefusal> {
        self.known(team)?;
        let mut changed = vec![format!("deleted team {team}")];
        if let Some(holder) = self.holder(team).map(|t| t.team.clone()) {
            self.team_mut(&holder)?.members.retain(|m| m != team);
            changed.push(format!("removed team {team} from team {holder}"));
        }
        self.teams.retain(|t| t.team != *team);
        Ok(changed)
    }

    /// Add `member`, an ant or a team on no team, to `team`. Returns whether it changed: false when already a member.
    pub fn add(&mut self, team: &UserId, member: &UserId) -> Result<bool, TeamRefusal> {
        if self.known(team)?.is_member(member) {
            return Ok(false);
        }
        if member == team {
            return Err(TeamRefusal::HoldsItself { team: team.clone(), member: member.clone() });
        }
        self.check_joins(team, member)?;
        self.team_mut(team)?.members.push(member.clone());
        Ok(true)
    }

    /// Remove `member` from `team`; the lead is never removed. Returns whether it changed: false when no member.
    pub fn remove(&mut self, team: &UserId, member: &UserId) -> Result<bool, TeamRefusal> {
        let own = self.known(team)?;
        if !own.is_member(member) {
            return Ok(false);
        }
        if own.lead.as_ref() == Some(member) {
            return Err(TeamRefusal::RemovesLead { lead: member.clone(), team: team.clone() });
        }
        self.team_mut(team)?.members.retain(|m| m != member);
        Ok(true)
    }

    /// Make `ant`, a direct ant member of `team`, its lead. Returns whether it changed.
    pub fn set_lead(&mut self, team: &UserId, ant: &UserId) -> Result<bool, TeamRefusal> {
        let own = self.known(team)?;
        if !own.is_member(ant) || self.is_team(ant) {
            return Err(TeamRefusal::NotLeadable { ant: ant.clone(), team: team.clone() });
        }
        if own.lead.as_ref() == Some(ant) {
            return Ok(false);
        }
        self.team_mut(team)?.lead = Some(ant.clone());
        Ok(true)
    }

    /// Rename the ant `from` to `to` wherever the directory names it, as a member and a lead. Returns whether it
    /// changed.
    pub fn rename_ant(&mut self, from: &UserId, to: &UserId) -> bool {
        let mut changed = false;
        for team in &mut self.teams {
            for member in team.members.iter_mut().filter(|m| *m == from) {
                *member = to.clone();
                changed = true;
            }
            if team.lead.as_ref() == Some(from) {
                team.lead = Some(to.clone());
            }
        }
        changed
    }

    /// Take the ant `ant`, retired or revoked, out of its team. A lead leaves its team leaderless, which only a
    /// revocation does: a retirement is refused before while the ant leads a team. Returns the team it left, if any.
    pub fn drop_ant(&mut self, ant: &UserId) -> Option<UserId> {
        let team = self.teams.iter_mut().find(|t| t.is_member(ant))?;
        team.members.retain(|m| m != ant);
        if team.lead.as_ref() == Some(ant) {
            team.lead = None;
        }
        Some(team.team.clone())
    }

    /// Apply the `ops` `ant` asks for on `team` ([`TeamAdminRequest`]): the ant must manage the team, leading it or a
    /// team above it. It may add an ant of the bus (`known_ant`, one with an identity record) or a team that is on no team, remove any member but
    /// the lead, hand the lead to another direct ant member, set or clear the description, create a team beneath
    /// `team` and delete a team that is a member of `team`; never set an owner or an avatar. Returns what changed, in
    /// order, empty when nothing did; on a refusal the directory is unchanged.
    pub fn apply_for_ant(
        &mut self,
        ant: &UserId,
        team: &UserId,
        ops: &[TeamOp],
        known_ant: impl Fn(&UserId) -> bool,
    ) -> Result<Vec<String>, TeamRefusal> {
        self.known(team)?;
        if !self.manages(ant, team) {
            return Err(TeamRefusal::NotManager { ant: ant.clone(), team: team.clone() });
        }
        let mut next = self.clone();
        let mut changed = Vec::new();
        for op in ops {
            match op {
                TeamOp::AddMember { member } => {
                    if !next.is_team(member) && !known_ant(member) {
                        return Err(TeamRefusal::UnknownAnt { name: member.clone() });
                    }
                    if next.add(team, member)? {
                        changed.push(format!("added {member} to team {team}"));
                    }
                }
                TeamOp::RemoveMember { member } => {
                    if next.remove(team, member)? {
                        changed.push(format!("removed {member} from team {team}"));
                    }
                }
                TeamOp::SetLead { ant: lead } => {
                    if next.set_lead(team, lead)? {
                        changed.push(format!("{lead} leads team {team}"));
                    }
                }
                TeamOp::SetDescription { description } => {
                    let set = next.team_mut(team)?.set_description(Some(description));
                    if set.map_err(|e| TeamRefusal::Invalid { why: e.to_string() })? {
                        changed.push(format!("described team {team}"));
                    }
                }
                TeamOp::ClearDescription => {
                    if next.team_mut(team)?.set_description(None).expect("clearing is valid") {
                        changed.push(format!("cleared the description of team {team}"));
                    }
                }
                TeamOp::CreateTeam { team: new, lead, description } => {
                    if !known_ant(lead) && next.holder(lead).is_none_or(|t| t.team != *team) {
                        return Err(TeamRefusal::UnknownAnt { name: lead.clone() });
                    }
                    let mut created = Team::new(new.clone(), lead.clone(), ant.clone());
                    created
                        .set_description(description.as_deref())
                        .map_err(|e| TeamRefusal::Invalid { why: e.to_string() })?;
                    changed.extend(next.create(created, Some(team))?);
                }
                TeamOp::DeleteTeam { team: gone } => {
                    next.known(gone)?;
                    if next.holder(gone).is_none_or(|t| t.team != *team) {
                        return Err(TeamRefusal::NotBeneath { team: gone.clone(), parent: team.clone() });
                    }
                    changed.extend(next.delete(gone)?);
                }
            }
        }
        next.check()?;
        *self = next;
        Ok(changed)
    }

    /// The note of `ant`'s team and its ancestors that its daemon adds to the first prompt of each session: each one's
    /// name, description, lead, owner and direct members, the nearest first, and whether the ant leads it. None when
    /// the ant is on no team.
    pub fn note_for(&self, ant: &UserId) -> Option<String> {
        use std::fmt::Write as _;
        let chain = self.ancestors(ant);
        let first = chain.first()?;
        let mut note = format!("[ants] You are a member of team {}", first.team);
        for up in &chain[1..] {
            let _ = write!(note, ", within team {}", up.team);
        }
        note.push('.');
        for team in &chain {
            let _ = write!(note, "\n\nTeam {}", team.team);
            if team.lead.as_ref() == Some(ant) {
                note.push_str(" (you lead it: messages to the team come to you; hand work on to its members)");
            }
            note.push(':');
            if let Some(description) = &team.description {
                let _ = write!(note, "\n{description}");
            }
            match &team.lead {
                Some(lead) => {
                    let _ = write!(note, "\n- lead: {lead}");
                }
                None => note.push_str("\n- lead: none (its lead was revoked)"),
            }
            if let Some(owner) = &team.owner {
                let _ = write!(note, "\n- owner: {owner} (a human)");
            }
            let members: Vec<String> = team
                .members
                .iter()
                .map(|m| if self.is_team(m) { format!("team {m}") } else { m.to_string() })
                .collect();
            let _ = write!(note, "\n- members: {}", members.join(", "));
        }
        Some(note)
    }
}

#[cfg(test)]
mod tests;
