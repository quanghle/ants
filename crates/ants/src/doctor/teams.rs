//! Teams: whether the team directory keeps its rules, each team has a lead, its own or one above it, each lead is live
//! in the colony, every member is an ant
//! of the bus or a team, each owner is a human and no team takes the name of an ant or a human; and whether `ants
//! signer` answers each ant's team changes.

use std::collections::BTreeSet;
use std::time::Duration;

use ants_daemon::config::Ant;
use ants_daemon::teams::keeper;
use ants_proto::{
    ColonyEntry, Humans, TEAMS_BUCKET, TeamAdminOutcome, TeamAdminRequest, TeamDirectory, TeamOp, TeamRefusal, UserId,
    subject,
};
use chrono::{DateTime, Utc};

use super::server::{Unreached, connect};
use super::{Check, Connected, NETWORK_TIMEOUT, Setup};

const CHECK: &str = "teams";

/// The team directory as the CLI's credentials read it ([`findings`]), with the ants of the bus and its humans.
pub(super) async fn check_teams(connected: Option<&Connected>, setup: &Setup, now: DateTime<Utc>) -> Vec<Check> {
    let Some(connected) = connected else { return vec![Check::skip(CHECK, "the hub is not reached")] };
    if !connected.rights.may_publish(&format!("$JS.API.DIRECT.GET.{}", subject::kv_stream(TEAMS_BUCKET))) {
        return vec![Check::skip(CHECK, "the credentials may not read the team directory")];
    }
    let domain = setup.js_domain.as_ref();
    let read = async {
        let Some(directory) = crate::teams::read_of(&connected.js, domain).await? else { return anyhow::Ok(None) };
        let names: Vec<&UserId> =
            directory.teams.iter().flat_map(|t| std::iter::once(&t.team).chain(&t.members)).collect();
        let known = crate::teams::known_ants(&connected.js, domain, &names).await?;
        let humans = crate::people::humans::read(&connected.js)
            .await
            .inspect_err(|e| tracing::debug!("not reading the humans: {e:#}"))
            .ok();
        Ok(Some((directory, known, humans)))
    };
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, read).await {
        Ok(Ok(Some((directory, known, humans)))) => {
            let colony = connected.colony.as_ref().ok().map(Vec::as_slice);
            findings(&directory, &known, humans.as_ref(), colony, now)
        }
        Ok(Ok(None)) => vec![Check::skip(CHECK, format!("the bus keeps no {TEAMS_BUCKET}"))],
        Ok(Err(e)) => match e.downcast_ref::<TeamRefusal>() {
            Some(refusal @ TeamRefusal::Unreadable { .. }) => vec![Check::fail(
                CHECK,
                refusal.to_string(),
                vec![format!(
                    "`ants team repair` shows each rule it breaks and `ants team repair --apply` writes it repaired, \
                     each member kept on the team that comes first; or a human deletes key `directory` of \
                     {TEAMS_BUCKET} to start over"
                )],
            )],
            _ => vec![Check::skip(CHECK, format!("no team directory read: {e:#}"))],
        },
        Err(_) => vec![Check::skip(CHECK, "no team directory read within 10s")],
    }
}

/// What is wrong with `directory`, which keeps the rules, one check each: a team with no lead, nor any team above it
/// (warn), a lead not live in `colony` when read (warn, at the team it leads of its own), an owner no human of `humans`
/// when read (warn), a member neither a team nor an ant of `known` (fail), a team taking the name of
/// an ant of `known` or a human of `humans` (fail); else one check telling it is sound.
pub(super) fn findings(
    directory: &TeamDirectory,
    known: &BTreeSet<UserId>,
    humans: Option<&Humans>,
    colony: Option<&[ColonyEntry]>,
    now: DateTime<Utc>,
) -> Vec<Check> {
    let mut checks = Vec::new();
    for team in &directory.teams {
        let id = &team.team;
        if known.contains(id) || humans.is_some_and(|h| h.contains(id)) {
            let kind = if known.contains(id) { "an ant" } else { "a human" };
            checks.push(Check::fail(
                CHECK,
                format!("the name {id} is both a team and {kind}: messages to {id} go to the team's lead"),
                vec![format!("ants team delete {id}, then create it again under another name")],
            ));
        }
        match &team.lead {
            None if directory.lead_source(id).is_none() => checks.push(Check::warn(
                CHECK,
                format!("team {id} has no lead, nor has any team above it: messages to it are refused"),
                vec![format!("ants team set {id} --lead ANT")],
            )),
            None => {}
            Some(lead) => {
                let live = colony.map(|c| c.iter().any(|e| e.ant == *lead && ants_daemon::colony::is_live(e, now)));
                if live == Some(false) {
                    checks.push(Check::warn(
                        CHECK,
                        format!("{lead}, the lead of team {id}, is not live in the colony: messages to {id} wait"),
                        vec![format!(
                            "serve {lead} (`ants colony` shows the ants served), or ants team set {id} --lead ANT"
                        )],
                    ));
                }
            }
        }
        if let (Some(owner), Some(humans)) = (&team.owner, humans)
            && !humans.contains(owner)
        {
            checks.push(Check::warn(
                CHECK,
                format!("{owner}, the owner of team {id}, is no human of the bus: no human answers for the team"),
                vec![format!("ants team set {id} --owner HUMAN, or ants team set {id} --no-owner")],
            ));
        }
        for member in team.members.iter().filter(|m| !directory.is_team(m) && !known.contains(*m)) {
            checks.push(Check::fail(
                CHECK,
                format!("team {id} holds {member}, neither an ant of the bus nor a team"),
                vec![format!("ants team remove {id} {member}")],
            ));
        }
    }
    if checks.is_empty() {
        let detail = match directory.teams.len() {
            0 => "no teams".to_owned(),
            1 => "1 team, with a lead, every member an ant of the bus or a team".to_owned(),
            n => format!(
                "{n} teams, each with a lead, its own or one above it, every member an ant of the bus or a team"
            ),
        };
        checks.push(Check::ok(CHECK, detail));
    }
    checks
}

/// Whether `ants signer` answers each ant's team changes, which its MCP manage_team makes: asked as the ant, where its
/// daemon connects, to change a team at stream sequence 0, which the signer answers without changing anything (as
/// changed, when the directory is written at all, else as no such team). An ant without credentials, on an open
/// server, asks no one: its daemon changes the teams itself.
pub(super) async fn check_team_changes(ants: &[Ant], setup: &Setup, wait: Duration) -> Vec<Check> {
    futures::future::join_all(ants.iter().map(|ant| check_ant_team_changes(ant, setup, wait))).await
}

async fn check_ant_team_changes(ant: &Ant, setup: &Setup, wait: Duration) -> Check {
    let check = format!("ant {} team changes", ant.name);
    let Some(creds) = &ant.creds else {
        return Check::ok(check, "no credentials, so an open server: the daemon changes the teams itself");
    };
    let url = ant.nats.as_deref().unwrap_or(&setup.nats);
    let client = match connect(url, Some(creds), &ant.name).await {
        Ok(client) => client,
        Err(Unreached::Refused(e) | Unreached::Unreachable(e) | Unreached::NoKey(e) | Unreached::Other(e)) => {
            return Check::skip(check, format!("not connected to {url} as {}: {e}", ant.name));
        }
    };
    let probe = TeamAdminRequest::new(
        ant.name.clone(),
        "doctor-probe".parse().expect("a team id"),
        vec![TeamOp::ClearDescription],
        0,
    );
    let signer = || {
        vec![
            "ants signer on the machine holding the account key (scripts/install-units.sh --signer runs it as a \
             service)"
                .to_owned(),
        ]
    };
    match keeper::request_within(&client, &probe, wait).await {
        Ok(Some(TeamAdminOutcome::Refused { refusal: TeamRefusal::NoDirectory })) => Check::warn(
            check,
            format!("ants signer answers, but the bus keeps no {TEAMS_BUCKET}: manage_team fails"),
            vec!["ants init".to_owned()],
        ),
        Ok(Some(TeamAdminOutcome::Refused {
            refusal: refusal @ (TeamRefusal::Expired | TeamRefusal::Unreadable { .. } | TeamRefusal::Invalid { .. }),
        })) => Check::warn(check, format!("ants signer refuses the ant's team changes: {refusal}"), Vec::new()),
        Ok(Some(_)) => Check::ok(check, "ants signer answers manage_team"),
        Ok(None) => Check::warn(
            check,
            format!("nothing answers its team changes within {}s: manage_team fails", wait.as_secs()),
            signer(),
        ),
        Err(e) => Check::skip(check, format!("asking as {}: {e:#}", ant.name)),
    }
}
