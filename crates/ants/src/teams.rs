//! Teams as humans run them: the team directory read and written with a human's credentials (`ants team`), the
//! names teams share with ants and humans, a message to a team sent on to its lead (`ants ask @TEAM`), and what
//! retiring or revoking an ant does to its team.
//!
//! A retired or revoked ant leaves its team ([`drop_ant`]); a lead leaving leaves its team without a lead of its own,
//! so that it takes the lead of the nearest team above it with one. A team with no lead, nor any above it, refuses
//! messages, and `ants doctor` warns of it, until a human names a lead.

use std::collections::BTreeSet;
use std::time::Duration;

use ants_daemon::records::direct_kv::{DirectKv, Lookup};
use ants_daemon::teams;
use ants_proto::{ENROLL_INBOX, Humans, IDENTITIES_BUCKET, JsDomain, Team, TeamDirectory, TeamRefusal, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;

pub use ants_daemon::teams::open;

pub mod change;

/// How long a lookup that only adds a check ([`holds_team`]) waits for the hub.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

/// The team directory of `kv`: None when the bus keeps no `ants_teams`. Fails when the hub is out of reach, or the
/// directory breaks the rules ([`TeamRefusal::Unreadable`]).
pub async fn read(kv: &DirectKv) -> Result<Option<TeamDirectory>> {
    match teams::read(kv).await? {
        Some(read) => Ok(Some(read.directory?)),
        None => Ok(None),
    }
}

/// The team directory of `js`, read as [`read`] does.
pub async fn read_of(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<Option<TeamDirectory>> {
    read(&open(js, js_domain).await?).await
}

/// A team id or a member as given on the command line: `backend` or `@backend`.
pub fn parse_name(s: &str) -> Result<UserId, ants_proto::Error> {
    s.trim_prefix('@').parse()
}

/// Which of `names` are ants of the bus: those with an identity record in [`IDENTITIES_BUCKET`].
pub async fn known_ants(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    names: &[&UserId],
) -> Result<BTreeSet<UserId>> {
    let identities = DirectKv::whole(js, js_domain, IDENTITIES_BUCKET).await?;
    let mut known = BTreeSet::new();
    for name in names {
        if matches!(identities.lookup(name.as_str()).await?, Lookup::Value(_)) {
            known.insert((*name).clone());
        }
    }
    Ok(known)
}

/// Refuse `team` as the name of a new team when an ant (one with an identity record) or a human (of `humans`) holds
/// it, or it is reserved: teams share the names of ants and humans.
pub async fn check_team_name(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    humans: &Humans,
    team: &UserId,
) -> Result<()> {
    crate::auth::check_not_reserved(team)?;
    if humans.contains(team) || !known_ants(js, js_domain, &[team]).await?.is_empty() {
        return Err(TeamRefusal::NameTaken { name: team.clone() }.into());
    }
    Ok(())
}

/// Refuse a member that is a human: humans are no team's members, at most a team's owner.
pub fn check_not_human(member: &UserId, humans: &Humans) -> Result<()> {
    if humans.contains(member) {
        bail!("{member} is a human: a team's members are ants and teams, and a human at most its owner (`--owner`)");
    }
    Ok(())
}

/// Refuse an owner that is no human of `humans`.
pub fn check_owner(owner: &UserId, humans: &Humans) -> Result<()> {
    if !humans.contains(owner) {
        bail!("{owner} is no human of the bus (`ants auth user {owner} --human`): a team's owner is a human");
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

/// Whether a team of the bus holds `name`, the name of an ant or a human about to be issued credentials: false, with
/// the reason logged, when the directory cannot be read within a few seconds.
pub async fn holds_team(js: &jetstream::Context, js_domain: Option<&JsDomain>, name: &UserId) -> bool {
    match tokio::time::timeout(LOOKUP_TIMEOUT, read_of(js, js_domain)).await {
        Ok(Ok(directory)) => directory.is_some_and(|d| d.is_team(name)),
        Ok(Err(e)) => {
            tracing::debug!("not checking the team names: {e:#}");
            false
        }
        Err(_) => {
            tracing::debug!("not checking the team names: no answer within {LOOKUP_TIMEOUT:?}");
            false
        }
    }
}

/// Refuse `name` for an ant or a human when a team holds it ([`holds_team`]).
pub async fn check_no_team(js: &jetstream::Context, js_domain: Option<&JsDomain>, name: &UserId) -> Result<()> {
    if holds_team(js, js_domain, name).await {
        bail!("the name {name} is taken by a team (`ants team show {name}`); choose another name");
    }
    Ok(())
}

/// A JetStream context of short-lived credentials signed with the account key in `dir`, which read and write the team
/// directory ([`crate::auth::team_keeper_creds`]), connected to the server at `url` within `wait`.
pub async fn keeper(
    url: &str,
    js_domain: Option<&JsDomain>,
    dir: &std::path::Path,
    wait: Duration,
) -> Result<jetstream::Context> {
    ants_daemon::bus::check_url(url)?;
    let creds = crate::auth::team_keeper_creds(dir, js_domain)?;
    let connect = async_nats::ConnectOptions::with_credentials(&creds)?
        .name("ants-teams")
        .custom_inbox_prefix(ENROLL_INBOX)
        .connection_timeout(wait)
        .connect(url);
    let client = tokio::time::timeout(wait, connect).await.context("no answer from the server")??;
    let mut js = ants_daemon::bus::jetstream(client, js_domain);
    js.set_timeout(wait);
    Ok(js)
}

/// Take `ant`, retired or revoked, off its team in the directory of `kv`: a line saying so, None when it is on no team
/// or the bus keeps no `ants_teams`. A lead leaves its team without a lead of its own: it takes the lead of the
/// nearest team above it with one, should there be one.
pub async fn drop_ant(kv: &DirectKv, ant: &UserId) -> Result<Option<String>> {
    if teams::read(kv).await?.is_none() {
        return Ok(None);
    }
    teams::modify(kv, |directory| {
        let led = directory.led_by(ant).first().map(|t| t.team.clone());
        let Some(team) = directory.drop_ant(ant) else { return Ok(None) };
        if led.as_ref() != Some(&team) {
            return Ok(Some(format!("took {ant} off team {team}")));
        }
        Ok(Some(match directory.lead_source(&team) {
            Some(above) => format!(
                "took {ant} off team {team}, which has no lead of its own now: it takes {}, the lead of team {}",
                above.lead.as_ref().map_or_default(UserId::to_string),
                above.team
            ),
            None => format!(
                "took {ant} off team {team}, which has no lead now, nor has any team above it: messages to it are \
                 refused until a human names one (`ants team set {team} --lead ANT`)"
            ),
        }))
    })
    .await
}

/// [`drop_ant`] on the directory of `js`, a failure said in the line returned, with the command finishing it.
pub async fn drop_ant_or_say(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Option<String> {
    let dropped = async { drop_ant(&open(js, js_domain).await?, ant).await };
    match tokio::time::timeout(LOOKUP_TIMEOUT * 3, dropped).await {
        Ok(Ok(line)) => line,
        Ok(Err(e)) => Some(format!(
            "taking {ant} off its team failed ({e:#}): `ants team remove TEAM {ant}` does it (`ants team list` shows \
             the teams)"
        )),
        Err(_) => Some(format!(
            "taking {ant} off its team got no answer in time: `ants team remove TEAM {ant}` does it (`ants team list` \
             shows the teams)"
        )),
    }
}

/// The team holding `ant` as `ants ant show` names it: `backend`, `backend, within eng`, with `(lead)` when the ant
/// leads it, `none` when on no team.
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

/// A team as `ants team` prints it after a change: one line, then its description should it have one.
pub fn describe(directory: &TeamDirectory, team: &Team) -> String {
    let members: Vec<String> =
        team.members.iter().map(|m| if directory.is_team(m) { format!("team {m}") } else { m.to_string() }).collect();
    let owner = team.owner.as_ref().map_or_default(|o| format!("; owner {o}"));
    let within = directory.holder(&team.team).map_or_default(|t| format!(", within team {}", t.team));
    let description = team.description.as_ref().map_or_default(|d| format!("\ndescription: {d}"));
    format!(
        "team {}{within}: lead {}; members {}{owner}{description}",
        team.team,
        lead_label(directory, team),
        members.join(", ")
    )
}

/// The teams as a table, a forest: each top-level team, then the teams beneath it, indented under it. A DESCRIPTION
/// column, last, shows the start of each team's description, when any team has one.
pub fn table(directory: &TeamDirectory) -> String {
    let mut header = ["TEAM", "LEAD", "MEMBERS", "OWNER"].map(str::to_owned).to_vec();
    let described = directory.teams.iter().any(|t| t.description.is_some());
    if described {
        header.push("DESCRIPTION".to_owned());
    }
    let mut rows = vec![header];
    for (depth, team) in forest(directory) {
        let mut row = vec![
            format!("{}{}", "  ".repeat(depth), team.team),
            lead_label(directory, team),
            team.members.len().to_string(),
            team.owner.as_ref().map_or_else(|| "-".to_owned(), UserId::to_string),
        ];
        if described {
            row.push(team.description.as_deref().map_or_else(|| "-".to_owned(), |d| crate::text::excerpt(d, 40)));
        }
        rows.push(row);
    }
    crate::text::columns(&rows)
}

/// Every team of `directory`, depth first from the top-level teams, with its depth (0 for a top-level team).
pub fn forest(directory: &TeamDirectory) -> Vec<(usize, &Team)> {
    fn under<'a>(directory: &'a TeamDirectory, team: &'a Team, depth: usize, out: &mut Vec<(usize, &'a Team)>) {
        if out.iter().any(|(_, t)| t.team == team.team) {
            return;
        }
        out.push((depth, team));
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

/// A member of a team as `ants team show --json` gives it: an ant, with whether it leads the team and its state in
/// the colony, or a team, with its lead and its own members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
        #[serde(skip_serializing_if = "Option::is_none")]
        lead: Option<UserId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lead_from: Option<UserId>,
        members: Vec<Member>,
    },
}

/// The members of `team` as a tree, down through its member teams, each ant's state as `state` tells it.
pub fn members(directory: &TeamDirectory, team: &Team, state: &impl Fn(&UserId) -> String) -> Vec<Member> {
    let mut seen = BTreeSet::from([team.team.clone()]);
    members_of(directory, team, state, &mut seen)
}

fn members_of(
    directory: &TeamDirectory,
    team: &Team,
    state: &impl Fn(&UserId) -> String,
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

/// The members tree as `ants team show` prints it: one line each, indented by depth, each ant with its face
/// (`face`), the lead marked, and its state.
pub fn tree_lines(members: &[Member], face: &impl Fn(&UserId) -> String) -> Vec<String> {
    fn lines(members: &[Member], depth: usize, face: &impl Fn(&UserId) -> String, out: &mut Vec<String>) {
        let indent = "  ".repeat(depth + 1);
        for member in members {
            match member {
                Member::Ant { name, lead, state } => {
                    let lead = if *lead { " ★ lead" } else { "" };
                    out.push(format!("{indent}{}{lead}: {state}", face(name)));
                }
                Member::Team { name, lead, lead_from, members } => {
                    let lead = match (lead, lead_from) {
                        (Some(l), Some(from)) => format!("lead {l}, from team {from}"),
                        (Some(l), None) => format!("lead {l}"),
                        (None, _) => "no lead, nor above it".to_owned(),
                    };
                    out.push(format!("{indent}team {name} ({lead})"));
                    lines(members, depth + 1, face, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    lines(members, 0, face, &mut out);
    out
}

/// A team as `ants team list --json` and `ants team show --json` give it: its record, the team holding it and, for
/// `show`, its members as a tree.
#[derive(Debug, Serialize)]
pub struct Shown<'a> {
    #[serde(flatten)]
    pub team: &'a Team,
    pub parent: Option<&'a UserId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree: Option<Vec<Member>>,
}

impl<'a> Shown<'a> {
    pub fn of(directory: &'a TeamDirectory, team: &'a Team) -> Self {
        Self { team, parent: directory.holder(&team.team).map(|t| &t.team), tree: None }
    }
}

/// An ant's state as `ants team show` gives it, from its colony entry, as `ants ant show` says it.
pub fn state_of(entries: &[ants_proto::ColonyEntry], ant: &UserId, now: DateTime<Utc>) -> String {
    crate::ants::ant::state(entries.iter().find(|e| e.ant == *ant), None, now)
}

#[cfg(test)]
mod tests;
