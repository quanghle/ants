//! An ant's escalation chain through the forest of teams: a team named in `escalate_to` stands for its lead, and an
//! ant on a team with no `escalate_to` escalates to the lead above it ([`chain`]).

use ants_proto::{TeamDirectory, UserId};

/// The ants `escalate_to`, the escalate_to of `ant`, names: each team stands for its lead
/// ([`TeamDirectory::lead_of`]), one with no lead in it nor above it left out with a warning, and `ant` itself and
/// duplicates left out. Without `teams`, `escalate_to` as it is.
pub fn named(ant: &UserId, escalate_to: &[UserId], teams: Option<&TeamDirectory>) -> Vec<UserId> {
    let Some(teams) = teams else { return escalate_to.to_vec() };
    let mut chain: Vec<UserId> = Vec::with_capacity(escalate_to.len());
    for member in escalate_to {
        let member = if teams.is_team(member) {
            match teams.lead_of(member) {
                Ok(lead) => lead,
                Err(e) => {
                    tracing::warn!(%ant, team = %member, error = %e, "leaving a team of escalate_to out of the chain");
                    continue;
                }
            }
        } else {
            member
        };
        if member != ant && !chain.contains(member) {
            chain.push(member.clone());
        }
    }
    chain
}

/// The chain of `ant`, whose escalate_to is `escalate_to`: the ants it names ([`named`]); with no escalate_to, for an
/// ant on a team of `teams`, the nearest lead above it other than itself, its team's own or inherited, else that of a
/// team above; else none.
pub fn chain(ant: &UserId, escalate_to: &[UserId], teams: Option<&TeamDirectory>) -> Vec<UserId> {
    if !escalate_to.is_empty() {
        return named(ant, escalate_to, teams);
    }
    let Some(teams) = teams else { return Vec::new() };
    let lead = teams.ancestors(ant).into_iter().filter_map(|t| t.lead.as_ref()).find(|lead| *lead != ant);
    lead.into_iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use ants_proto::Team;

    use super::*;

    fn p(id: &str) -> UserId {
        id.parse().unwrap()
    }

    /// eng (lead boss) holds backend (lead dev-a: dev-a, dev-b) and infra (no lead: dev-i); orphans has no lead
    /// (dev-o).
    fn forest() -> TeamDirectory {
        let mut teams = TeamDirectory::default();
        teams.create(Team::new(p("eng"), p("boss"), p("alice")), None).unwrap();
        let mut backend = Team::new(p("backend"), p("dev-a"), p("alice"));
        backend.members.push(p("dev-b"));
        teams.create(backend, Some(&p("eng"))).unwrap();
        let mut infra = Team::unled(p("infra"), p("alice"));
        infra.members.push(p("dev-i"));
        teams.create(infra, Some(&p("eng"))).unwrap();
        let mut orphans = Team::unled(p("orphans"), p("alice"));
        orphans.members.push(p("dev-o"));
        teams.create(orphans, None).unwrap();
        teams
    }

    #[test]
    fn a_team_in_escalate_to_stands_for_its_lead_never_the_ant_itself() {
        let teams = forest();
        let named = |ant: &str, to: &[&str]| {
            let to: Vec<UserId> = to.iter().map(|t| p(t)).collect();
            chain(&p(ant), &to, Some(&teams))
        };
        assert_eq!(named("dev-b", &["backend", "ops"]), [p("dev-a"), p("ops")]);
        assert_eq!(named("dev-b", &["infra"]), [p("boss")], "an unled team takes the lead above it");
        assert_eq!(named("dev-b", &["dev-a", "backend", "eng"]), [p("dev-a"), p("boss")], "no duplicates");
        assert_eq!(named("dev-a", &["backend", "eng"]), [p("boss")], "a lead never escalates to itself");
        assert_eq!(named("dev-b", &["orphans", "ops"]), [p("ops")], "a team with no lead is left out");
        assert_eq!(named("dev-a", &["backend"]), Vec::<UserId>::new(), "the chain is empty, not the forest's");
        let to = [p("backend")];
        assert_eq!(chain(&p("dev-b"), &to, None), to, "without the directory");
    }

    #[test]
    fn an_ant_on_a_team_with_no_escalate_to_escalates_to_the_nearest_lead_above_it() {
        let teams = forest();
        let of = |ant: &str| chain(&p(ant), &[], Some(&teams));
        assert_eq!(of("dev-b"), [p("dev-a")], "its team's lead");
        assert_eq!(of("dev-a"), [p("boss")], "a lead, the lead above it");
        assert_eq!(of("dev-i"), [p("boss")], "an unled team's, inherited");
        assert_eq!(of("boss"), Vec::<UserId>::new(), "the top lead goes to the owner");
        assert_eq!(of("dev-o"), Vec::<UserId>::new(), "no lead above it");
        assert_eq!(of("loner"), Vec::<UserId>::new(), "on no team");
        assert_eq!(chain(&p("dev-b"), &[], None), Vec::<UserId>::new());
    }
}
