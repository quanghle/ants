//! The teams as a human's client shows and writes them, with no I/O: the forest of teams ([`forest`]), a team's
//! members as a tree with each ant's state in the colony ([`tree`], [`ant_state`]), a team's lead ([`lead_label`]) and
//! the team an ant is on ([`team_line`]); the changes a human writes, checked before anything is read of the bus and
//! applied to the directory read ([`change`]); a directory breaking the rules repaired ([`repair`]); and why reading or
//! writing the directory failed ([`TeamError`]).
//!
//! The reads and writes themselves are [`crate::client::Client`]'s: the directory read with its revision
//! ([`Directory`]) and each change stored by compare-and-set.

use std::collections::BTreeSet;
use std::time::Duration;

use ants_proto::{COLONY_HEARTBEAT, ColonyEntry, Team, TeamDirectory, TeamRefusal, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Error;

pub mod change;
pub mod repair;

/// How long after its last heartbeat a colony entry goes stale: two heartbeats missed.
pub const STALE_AFTER: Duration = COLONY_HEARTBEAT.saturating_mul(2);

/// Whether `entry` was refreshed within [`STALE_AFTER`] of `now`. A heartbeat ahead of `now` counts as fresh.
pub fn is_live(entry: &ColonyEntry, now: DateTime<Utc>) -> bool {
    (now - entry.heartbeat_at).to_std().unwrap_or_default() <= STALE_AFTER
}

/// What the ant of `entry` is doing: `unserved` while its daemon cannot serve it, `paused` while an admin or its config
/// pauses it, `fallback` while a fallback answers for its harness, `signed-out` while its harness is, `failing` while
/// its harness keeps failing to start, `rate-limited` or `out-of-quota` while a provider limit holds its messages, else
/// its activity.
pub fn activity(entry: &ColonyEntry) -> &'static str {
    if entry.unserved.is_some() {
        return "unserved";
    }
    if entry.paused.is_some() {
        return "paused";
    }
    if entry.fallback.is_some() {
        return "fallback";
    }
    match (&entry.signed_out, &entry.failing, &entry.limited, entry.activity) {
        (Some(_), _, _, _) => "signed-out",
        (None, Some(_), _, _) => "failing",
        (None, None, Some(limited), _) => limited.kind.as_str(),
        (None, None, None, activity) => activity.as_str(),
    }
}

/// An ant's state in a few words, from its colony entry: `live, working`, `stale, idle`, or `not in the colony`.
pub fn ant_state(entry: Option<&ColonyEntry>, now: DateTime<Utc>) -> String {
    match entry {
        Some(e) => format!("{}, {}", if is_live(e, now) { "live" } else { "stale" }, activity(e)),
        None => "not in the colony".to_owned(),
    }
}

/// A team of the forest: how deep it is (0 for a top-level team), the team, and the team holding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Branch {
    pub depth: usize,
    pub team: Team,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<UserId>,
}

/// Every team of `directory`, depth first from the top-level teams, each under the team holding it.
pub fn forest(directory: &TeamDirectory) -> Vec<Branch> {
    fn under(directory: &TeamDirectory, team: &Team, depth: usize, out: &mut Vec<Branch>) {
        if out.iter().any(|b| b.team.team == team.team) {
            return;
        }
        let parent = directory.holder(&team.team).map(|t| t.team.clone());
        out.push(Branch { depth, team: team.clone(), parent });
        for sub in directory.subteams(team) {
            under(directory, sub, depth + 1, out);
        }
    }
    let mut out = Vec::new();
    for root in directory.roots() {
        under(directory, root, 0, &mut out);
    }
    out
}

/// A member of a team as its tree shows it: an ant, with whether it leads the team and its state in the colony, or a
/// team, with its lead and its own members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Member {
    Ant {
        name: UserId,
        lead: bool,
        state: String,
    },
    Team {
        name: UserId,
        /// Its lead, its own or, without one, that of the nearest team above it with one (`lead_from`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lead: Option<UserId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lead_from: Option<UserId>,
        members: Vec<Member>,
    },
}

impl Member {
    /// The ant's or the team's name.
    pub fn name(&self) -> &UserId {
        match self {
            Self::Ant { name, .. } | Self::Team { name, .. } => name,
        }
    }
}

/// The members of `team` as a tree, down through its member teams, each ant's state as `colony`, read at `now`,
/// tells it ([`ant_state`]).
pub fn tree(directory: &TeamDirectory, team: &Team, colony: &[ColonyEntry], now: DateTime<Utc>) -> Vec<Member> {
    members(directory, team, &|ant| ant_state(colony.iter().find(|e| e.ant == *ant), now))
}

/// The members of `team` as a tree, down through its member teams, each ant's state as `state` tells it.
pub fn members(directory: &TeamDirectory, team: &Team, state: &dyn Fn(&UserId) -> String) -> Vec<Member> {
    let mut seen = BTreeSet::from([team.team.clone()]);
    members_of(directory, team, state, &mut seen)
}

fn members_of(
    directory: &TeamDirectory,
    team: &Team,
    state: &dyn Fn(&UserId) -> String,
    seen: &mut BTreeSet<UserId>,
) -> Vec<Member> {
    let mut out = Vec::new();
    for member in &team.members {
        match directory.team(member) {
            Some(sub) if seen.insert(sub.team.clone()) => {
                let source = directory.lead_source(&sub.team);
                out.push(Member::Team {
                    name: sub.team.clone(),
                    lead: source.and_then(|t| t.lead.clone()),
                    lead_from: source.filter(|t| t.team != sub.team).map(|t| t.team.clone()),
                    members: members_of(directory, sub, state, seen),
                });
            }
            Some(_) => {}
            None => out.push(Member::Ant {
                name: member.clone(),
                lead: team.lead.as_ref() == Some(member),
                state: state(member),
            }),
        }
    }
    out
}

/// The team holding `ant` in a line: `backend`, `backend, within eng`, with `(lead)` when the ant leads it, `none` when
/// on no team.
pub fn team_line(directory: &TeamDirectory, ant: &UserId) -> String {
    let chain = directory.ancestors(ant);
    let Some(team) = chain.first() else { return "none".to_owned() };
    let mut line = team.team.to_string();
    if team.lead.as_ref() == Some(ant) {
        line.push_str(" (lead)");
    }
    for up in &chain[1..] {
        line.push_str(", within ");
        line.push_str(up.team.as_str());
    }
    line
}

/// The lead of `team` as shown: its own, `X (from team Y)` when it takes the lead of a team above it, `none` when
/// neither it nor any team above it has one.
pub fn lead_label(directory: &TeamDirectory, team: &Team) -> String {
    match directory.lead_source(&team.team) {
        Some(source) if source.team == team.team => source.lead.as_ref().map_or_default(UserId::to_string),
        Some(source) => {
            format!("{} (from team {})", source.lead.as_ref().map_or_default(UserId::to_string), source.team)
        }
        None => "none".to_owned(),
    }
}

/// A team as `ants team list --json` and `ants team show --json` give it: its record, the team holding it and, for
/// `show`, its members as a tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shown {
    #[serde(flatten)]
    pub team: Team,
    pub parent: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tree: Option<Vec<Member>>,
}

impl Shown {
    /// `team` of `directory`, without its tree.
    pub fn of(directory: &TeamDirectory, team: &Team) -> Self {
        Self { team: team.clone(), parent: directory.holder(&team.team).map(|t| t.team.clone()), tree: None }
    }
}

/// The team directory as read, with the revision of its entry, which a write expects: the stream sequence of the
/// key's last message, 0 for none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directory {
    pub directory: TeamDirectory,
    pub revision: u64,
}

/// Why a read or a change of the teams failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum TeamError {
    /// The teams refused it: the change breaks their rules, names an unknown ant or a taken name, or the directory
    /// read breaks the rules ([`TeamRefusal::Unreadable`]) or is kept by no bucket ([`TeamRefusal::NoDirectory`]).
    #[error("{0}")]
    Refused(TeamRefusal),
    /// What was asked for is wrong before anything is read of the bus, and why.
    #[error("{0}")]
    Invalid(String),
    /// Other writers changed the directory each time this one tried to store its change.
    #[error("the team directory kept changing meanwhile; try again")]
    Conflict,
    /// The read or the write itself failed.
    #[error("{0}")]
    Client(Error),
}

impl From<TeamRefusal> for TeamError {
    fn from(refusal: TeamRefusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<Error> for TeamError {
    fn from(error: Error) -> Self {
        Self::Client(error)
    }
}

#[cfg(test)]
mod tests;
