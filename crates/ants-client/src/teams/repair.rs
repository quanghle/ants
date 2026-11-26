//! A team directory breaking the rules ([`TeamDirectory::check`]) repaired ([`repair`]): each rule it breaks, and the
//! directory keeping them, taken from it by removing what breaks them, in directory order, the order teams were
//! created and members added:
//!
//! - a team listed again: the later entry goes, its members on no team;
//! - a member listed twice in a team, or in several teams: it stays where it came first, in the team that comes first
//!   in the directory, and leaves the others;
//! - a team holding itself, directly or through its member teams: the team of the cycle that comes first in the
//!   directory leaves the team holding it, which makes it a top-level team;
//! - a lead no longer a direct ant member of its team: the team is left without a lead of its own;
//! - a description over [`MAX_TEAM_DESCRIPTION`] characters: it is cut to that length.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use ants_proto::{MAX_TEAM_DESCRIPTION, TeamDirectory, UserId};
use serde::{Deserialize, Serialize};

/// A rule the directory broke, and what the repair removed for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "broken", rename_all = "snake_case")]
pub enum Broken {
    /// `team` was listed more than once; the later entry, of `members`, went.
    TeamTwice { team: UserId, members: Vec<UserId> },
    /// `member` was listed more than once in `team`; it stays listed once.
    MemberTwice { member: UserId, team: UserId },
    /// `member` was in `kept` and in `left`; it left `left`.
    OnTeams { member: UserId, kept: UserId, left: UserId },
    /// `team` held itself, through `holder` (itself when it was its own member); it left `holder`.
    HoldsItself { team: UserId, holder: UserId },
    /// `lead` led `team` without being one of its direct ant members; `team` has no lead of its own now.
    NotLeadable { lead: UserId, team: UserId },
    /// The description of `team` was over [`MAX_TEAM_DESCRIPTION`] characters; it was cut.
    DescriptionTooLong { team: UserId },
}

impl fmt::Display for Broken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TeamTwice { team, members } if members.is_empty() => {
                write!(f, "team {team} is listed more than once: its later entry goes")
            }
            Self::TeamTwice { team, members } => {
                let members = members.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ");
                write!(f, "team {team} is listed more than once: its later entry goes, with {members}")
            }
            Self::MemberTwice { member, team } => {
                write!(f, "{member} is listed more than once in team {team}: it stays listed once")
            }
            Self::OnTeams { member, kept, left } => {
                write!(f, "{member} is on teams {kept} and {left}: it stays on {kept}, the first, and leaves {left}")
            }
            Self::HoldsItself { team, holder } if team == holder => {
                write!(f, "team {team} is a member of itself: it leaves itself")
            }
            Self::HoldsItself { team, holder } => {
                write!(f, "team {team} holds itself through team {holder}: it leaves {holder}, and is a top-level team")
            }
            Self::NotLeadable { lead, team } => {
                write!(f, "{lead} leads team {team} but is no direct ant member of it: the team has no lead of its own")
            }
            Self::DescriptionTooLong { team } => write!(
                f,
                "the description of team {team} is over {MAX_TEAM_DESCRIPTION} characters: it is cut to \
                 {MAX_TEAM_DESCRIPTION}"
            ),
        }
    }
}

/// A directory as repaired: each rule the one read broke, in directory order, and the directory keeping the rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repaired {
    pub broken: Vec<Broken>,
    pub directory: TeamDirectory,
}

/// `directory`, read without its rules checked, repaired as the module tells: nothing broken when it keeps them, and
/// then it is unchanged.
pub fn repair(mut directory: TeamDirectory) -> Repaired {
    let mut broken = Vec::new();

    let mut ids = BTreeSet::new();
    directory.teams.retain(|team| {
        let first = ids.insert(team.team.clone());
        if !first {
            broken.push(Broken::TeamTwice { team: team.team.clone(), members: team.members.clone() });
        }
        first
    });

    let mut holder: BTreeMap<UserId, UserId> = BTreeMap::new();
    for team in &mut directory.teams {
        let id = team.team.clone();
        team.members.retain(|member| {
            let fix = match holder.get(member) {
                None if *member == id => Some(Broken::HoldsItself { team: id.clone(), holder: id.clone() }),
                None => None,
                Some(kept) if *kept == id => Some(Broken::MemberTwice { member: member.clone(), team: id.clone() }),
                Some(kept) => Some(Broken::OnTeams { member: member.clone(), kept: kept.clone(), left: id.clone() }),
            };
            match fix {
                None => {
                    holder.insert(member.clone(), id.clone());
                    true
                }
                Some(fix) => {
                    broken.push(fix);
                    false
                }
            }
        });
    }

    // A cycle's earliest team is the first of it met here: one met earlier would have broken it already.
    let order: Vec<UserId> = directory.teams.iter().map(|t| t.team.clone()).collect();
    for team in &order {
        let mut at = team;
        let mut seen = BTreeSet::new();
        while let Some(up) = holder.get(at) {
            if up == team {
                let up = holder.remove(team).expect("the team's holder");
                if let Some(held) = directory.teams.iter_mut().find(|t| t.team == up) {
                    held.members.retain(|m| m != team);
                }
                broken.push(Broken::HoldsItself { team: team.clone(), holder: up });
                break;
            }
            if !seen.insert(up) {
                break;
            }
            at = up;
        }
    }

    for team in &mut directory.teams {
        if let Some(lead) = &team.lead
            && (!team.members.contains(lead) || ids.contains(lead))
        {
            broken.push(Broken::NotLeadable { lead: lead.clone(), team: team.team.clone() });
            team.lead = None;
        }
        if let Some(description) = &mut team.description
            && description.chars().count() > MAX_TEAM_DESCRIPTION
        {
            *description = description.chars().take(MAX_TEAM_DESCRIPTION).collect();
            broken.push(Broken::DescriptionTooLong { team: team.team.clone() });
        }
    }
    Repaired { broken, directory }
}

#[cfg(test)]
mod tests {
    use ants_proto::Team;

    use super::*;
    use crate::p;

    fn team(id: &str, members: &[&str]) -> Team {
        let mut team = Team::unled(p(id), p("alice"));
        team.members = members.iter().map(|m| p(m)).collect();
        team
    }

    fn members(directory: &TeamDirectory) -> Vec<(String, Vec<String>)> {
        let names = |t: &Team| t.members.iter().map(ToString::to_string).collect();
        directory.teams.iter().map(|t| (t.team.to_string(), names(t))).collect()
    }

    fn of(teams: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        teams.iter().map(|(t, m)| ((*t).to_owned(), m.iter().map(|m| (*m).to_owned()).collect())).collect()
    }

    #[test]
    fn a_directory_keeping_the_rules_stays_as_it_is() {
        let directory = TeamDirectory { teams: vec![team("eng", &["dev-a", "web"]), team("web", &["dev-b"])] };
        directory.check().unwrap();
        assert_eq!(repair(directory.clone()), Repaired { broken: Vec::new(), directory });
    }

    #[test]
    fn a_member_on_several_teams_stays_on_the_first_and_a_team_listed_again_goes() {
        let directory = TeamDirectory {
            teams: vec![
                team("ops", &["dev-a", "dev-b", "dev-a"]),
                team("web", &["dev-b", "dev-c"]),
                team("ops", &["dev-d"]),
                team("api", &["dev-c", "dev-e"]),
            ],
        };
        let repaired = repair(directory);
        assert_eq!(
            repaired.broken,
            [
                Broken::TeamTwice { team: p("ops"), members: vec![p("dev-d")] },
                Broken::MemberTwice { member: p("dev-a"), team: p("ops") },
                Broken::OnTeams { member: p("dev-b"), kept: p("ops"), left: p("web") },
                Broken::OnTeams { member: p("dev-c"), kept: p("web"), left: p("api") },
            ]
        );
        let expected = of(&[("ops", &["dev-a", "dev-b"]), ("web", &["dev-c"]), ("api", &["dev-e"])]);
        assert_eq!(members(&repaired.directory), expected);
        repaired.directory.check().unwrap();
        assert_eq!(
            repaired.broken[2].to_string(),
            "dev-b is on teams ops and web: it stays on ops, the first, and leaves web"
        );
    }

    #[test]
    fn a_cycle_is_broken_where_its_first_team_is_held_and_a_team_leaves_itself() {
        let directory = TeamDirectory {
            teams: vec![
                team("eng", &["dev-a", "api"]),
                team("web", &["eng", "web"]),
                team("api", &["web", "ops"]),
                team("ops", &["dev-b"]),
            ],
        };
        let repaired = repair(directory);
        assert_eq!(
            repaired.broken,
            [
                Broken::HoldsItself { team: p("web"), holder: p("web") },
                Broken::HoldsItself { team: p("eng"), holder: p("web") },
            ]
        );
        let expected = of(&[("eng", &["dev-a", "api"]), ("web", &[]), ("api", &["web", "ops"]), ("ops", &["dev-b"])]);
        assert_eq!(members(&repaired.directory), expected);
        repaired.directory.check().unwrap();
        assert_eq!(
            repaired.broken[1].to_string(),
            "team eng holds itself through team web: it leaves web, and is a top-level team"
        );
    }

    #[test]
    fn a_lead_who_left_is_cleared_and_a_long_description_cut() {
        let mut ops = team("ops", &["dev-a"]);
        ops.lead = Some(p("dev-a"));
        ops.description = Some("x".repeat(MAX_TEAM_DESCRIPTION + 5));
        let mut web = team("web", &["dev-a", "dev-b"]);
        web.lead = Some(p("dev-a"));
        let mut eng = team("eng", &["ops"]);
        eng.lead = Some(p("ops"));
        let repaired = repair(TeamDirectory { teams: vec![ops, web, eng] });
        assert_eq!(
            repaired.broken,
            [
                Broken::OnTeams { member: p("dev-a"), kept: p("ops"), left: p("web") },
                Broken::DescriptionTooLong { team: p("ops") },
                Broken::NotLeadable { lead: p("dev-a"), team: p("web") },
                Broken::NotLeadable { lead: p("ops"), team: p("eng") },
            ]
        );
        let teams = &repaired.directory.teams;
        assert_eq!(
            (teams[0].lead.as_ref(), teams[1].lead.as_ref(), teams[2].lead.as_ref()),
            (Some(&p("dev-a")), None, None)
        );
        assert_eq!(teams[0].description.as_ref().map(|d| d.chars().count()), Some(MAX_TEAM_DESCRIPTION));
        repaired.directory.check().unwrap();
        let json = serde_json::to_value(&repaired.broken[2]).unwrap();
        assert_eq!(json, serde_json::json!({"broken": "not_leadable", "lead": "dev-a", "team": "web"}));
    }
}
