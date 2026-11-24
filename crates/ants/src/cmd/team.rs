//! `ants team`: create, change, list, show and delete teams, as a human writing the team directory by
//! compare-and-set.

use std::collections::BTreeSet;

use ants_daemon::teams::modify;
use ants_proto::{Team, TeamDirectory, TeamRefusal, UserId};
use anyhow::{Result, bail};
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::connection::Nats;
use crate::people::humans;
use crate::teams::{self, parse_name};
use crate::{colony, completions};

#[derive(Subcommand, Debug)]
pub(crate) enum TeamCommand {
    /// Create a team led by --lead, an ant, which is its first member; fails when a team, an ant or a human holds the
    /// name. Each ant and each team is on one team at most; `ants team add PARENT TEAM` puts a team under another.
    Create {
        /// The team: `backend` or `@backend`.
        #[arg(value_parser = parse_name)]
        team: UserId,
        /// The ant messages to the team go to: an ant of the bus on no team.
        #[arg(long, value_name = "ANT", value_parser = parse_name, add = ArgValueCandidates::new(completions::ants))]
        lead: UserId,
        /// Member, an ant or a team on no team (repeatable, or comma-separated).
        #[arg(long = "member", value_name = "NAME", value_delimiter = ',', value_parser = parse_name, add = ArgValueCandidates::new(completions::ants))]
        members: Vec<UserId>,
        /// The human the team answers to.
        #[arg(long, value_name = "HUMAN", value_parser = parse_name)]
        owner: Option<UserId>,
        /// What the team is for, up to 1000 characters: each member ant's sessions start with it.
        #[arg(long, value_name = "TEXT")]
        description: Option<String>,
    },
    /// Change the lead, the owner or the description of a team; what is not given stays.
    Set {
        /// The team: `backend` or `@backend`.
        #[arg(value_parser = parse_name, add = ArgValueCandidates::new(completions::teams))]
        team: UserId,
        /// The new lead, one of the team's direct ant members.
        #[arg(long, value_name = "ANT", value_parser = parse_name, add = ArgValueCandidates::new(completions::ants))]
        lead: Option<UserId>,
        /// The human the team answers to.
        #[arg(long, value_name = "HUMAN", value_parser = parse_name, conflicts_with = "no_owner")]
        owner: Option<UserId>,
        /// Leave the team without an owner.
        #[arg(long)]
        no_owner: bool,
        /// What the team is for, up to 1000 characters: sessions started from then on take it in, while running
        /// ones keep the one they started with.
        #[arg(long, value_name = "TEXT", conflicts_with = "no_description")]
        description: Option<String>,
        /// Leave the team without a description.
        #[arg(long)]
        no_description: bool,
    },
    /// Add members to a team: ants of the bus or teams, each on no team.
    Add {
        /// The team: `backend` or `@backend`.
        #[arg(value_parser = parse_name, add = ArgValueCandidates::new(completions::teams))]
        team: UserId,
        /// Ants or teams: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "NAME", value_parser = parse_name, add = ArgValueCandidates::new(completions::ants))]
        names: Vec<UserId>,
    },
    /// Remove members, ants or teams, from a team; its lead stays until `ants team set TEAM --lead ANT` names
    /// another.
    Remove {
        /// The team: `backend` or `@backend`.
        #[arg(value_parser = parse_name, add = ArgValueCandidates::new(completions::teams))]
        team: UserId,
        /// Ants or teams: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "NAME", value_parser = parse_name)]
        names: Vec<UserId>,
    },
    /// Delete a team: it leaves the team holding it, and its members are on no team.
    Delete {
        /// The team: `backend` or `@backend`.
        #[arg(value_parser = parse_name, add = ArgValueCandidates::new(completions::teams))]
        team: UserId,
    },
    /// List the teams as a forest, each team under the one holding it: lead, member count, owner and description.
    List {
        /// One JSON object per team, in the order listed, with the team holding it as `parent`.
        #[arg(long)]
        json: bool,
    },
    /// Show a team: the teams above it, its description, lead and owner, and its members as a tree down through its
    /// member teams, each ant with its state in the colony and the leads marked.
    Show {
        /// The team: `backend` or `@backend`.
        #[arg(value_parser = parse_name, add = ArgValueCandidates::new(completions::teams))]
        team: UserId,
        /// One JSON object: the team, its `parent`, and its members as a `tree`.
        #[arg(long)]
        json: bool,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: TeamCommand) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let kv = teams::open(&js, nats.domain).await?;
    match command {
        TeamCommand::List { json } => list(&kv, json).await,
        TeamCommand::Show { team, json } => show(&js, &kv, &team, json).await,
        command => {
            if !nats.me.role.is_human() {
                bail!(
                    "changing a team needs a human's credentials; those of {} are an ant's, which change the teams \
                     it leads through manage_team",
                    nats.me.id
                );
            }
            change(nats, &js, &kv, command).await
        }
    }
}

/// The directory as read, refusing a bus without `ants_teams`.
async fn directory(kv: &ants_daemon::records::direct_kv::DirectKv) -> Result<TeamDirectory> {
    Ok(teams::read(kv).await?.ok_or(TeamRefusal::NoDirectory)?)
}

async fn list(kv: &ants_daemon::records::direct_kv::DirectKv, json: bool) -> Result<()> {
    let directory = directory(kv).await?;
    if json {
        for (_, team) in teams::forest(&directory) {
            println!("{}", serde_json::to_string(&teams::Shown::of(&directory, team))?);
        }
    } else if directory.teams.is_empty() {
        println!("no teams");
    } else {
        println!("{}", teams::table(&directory));
    }
    Ok(())
}

async fn show(
    js: &async_nats::jetstream::Context,
    kv: &ants_daemon::records::direct_kv::DirectKv,
    id: &UserId,
    json: bool,
) -> Result<()> {
    let directory = directory(kv).await?;
    let Some(team) = directory.team(id) else { return Err(TeamRefusal::NoSuchTeam { team: id.clone() }.into()) };
    let entries = colony::list(js).await.inspect_err(|e| tracing::warn!("not reading the colony: {e:#}"));
    let entries = entries.unwrap_or_default();
    let now = chrono::Utc::now();
    let tree = teams::members(&directory, team, &|ant| teams::state_of(&entries, ant, now));
    if json {
        let shown = teams::Shown { tree: Some(tree), ..teams::Shown::of(&directory, team) };
        println!("{}", serde_json::to_string(&shown)?);
        return Ok(());
    }
    let faces = crate::avatars::Faces::stdout(crate::avatars::read_or_none(js).await);
    let above: Vec<String> = directory.ancestors(id).iter().map(|t| format!(", within team {}", t.team)).collect();
    println!("team {id}{}", above.concat());
    if let Some(description) = &team.description {
        println!("  description: {description}");
    }
    println!("  lead: {}", teams::lead_label(team));
    if let Some(owner) = &team.owner {
        println!("  owner: {owner}");
    }
    if team.leaderless() {
        println!("  {}", TeamRefusal::Leaderless { team: id.clone() });
    }
    println!("members:");
    for line in teams::tree_lines(&tree, &|ant| faces.named(ant)) {
        println!("{line}");
    }
    Ok(())
}

/// What a change of a team names, checked before writing: the ants among them, and the humans of the bus.
struct Named {
    known: BTreeSet<UserId>,
    humans: ants_proto::Humans,
}

impl Named {
    /// Read which of `names` are ants of the bus, refusing the humans among them.
    async fn read(nats: &Nats<'_>, js: &async_nats::jetstream::Context, names: &[&UserId]) -> Result<Self> {
        let humans = humans::load(js, &nats.me).await;
        for name in names {
            teams::check_not_human(name, &humans)?;
        }
        let known = teams::known_ants(js, nats.domain, names).await?;
        Ok(Self { known, humans })
    }
}

/// Every `ants team` subcommand that changes the directory, as the CLI's human.
async fn change(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    kv: &ants_daemon::records::direct_kv::DirectKv,
    command: TeamCommand,
) -> Result<()> {
    let me = nats.me.id.clone();
    let (id, warnings, added) = match command {
        TeamCommand::Create { team, lead, members, owner, description } => {
            let mut created = Team::new(team.clone(), lead.clone(), me);
            created.set_description(description.as_deref())?;
            let members: Vec<UserId> = members.into_iter().filter(|m| *m != lead).collect();
            let named: Vec<&UserId> = std::iter::once(&lead).chain(&members).collect();
            let checked = Named::read(nats, js, &named).await?;
            teams::check_team_name(js, nats.domain, &checked.humans, &team).await?;
            if let Some(owner) = &owner {
                teams::check_owner(owner, &checked.humans)?;
            }
            created.owner = owner;
            created.members.extend(members.iter().cloned());
            let all: Vec<UserId> = created.members.clone();
            modify(kv, |directory| {
                teams::check_known(directory, &checked.known, &all)?;
                if directory.is_team(&lead) {
                    return Err(TeamRefusal::NotLeadable { ant: lead.clone(), team: team.clone() });
                }
                directory.create(created.clone(), None)
            })
            .await?;
            (team, Vec::new(), all)
        }
        TeamCommand::Set { team, lead, owner, no_owner, description, no_description } => {
            set(nats, js, kv, &team, lead, (owner, no_owner), (description, no_description)).await?;
            (team, Vec::new(), Vec::new())
        }
        TeamCommand::Add { team, names } => {
            let checked = Named::read(nats, js, &names.iter().collect::<Vec<_>>()).await?;
            let added = modify(kv, |directory| {
                teams::check_known(directory, &checked.known, &names)?;
                let mut added = Vec::new();
                for name in &names {
                    if directory.add(&team, name)? {
                        added.push(name.clone());
                    }
                }
                Ok(added)
            })
            .await?;
            let warnings = names
                .iter()
                .filter(|n| !added.contains(n))
                .map(|n| format!("{n} is already a member of team {team}"))
                .collect();
            (team, warnings, added)
        }
        TeamCommand::Remove { team, names } => {
            let removed = modify(kv, |directory| {
                let mut removed = Vec::new();
                for name in &names {
                    if directory.remove(&team, name)? {
                        removed.push(name.clone());
                    }
                }
                Ok(removed)
            })
            .await?;
            let warnings = names
                .iter()
                .filter(|n| !removed.contains(n))
                .map(|n| format!("{n} is no member of team {team}"))
                .collect();
            (team, warnings, Vec::new())
        }
        TeamCommand::Delete { team } => {
            for line in modify(kv, |directory| directory.delete(&team)).await? {
                println!("{line}");
            }
            return Ok(());
        }
        TeamCommand::List { .. } | TeamCommand::Show { .. } => unreachable!("reads change nothing"),
    };
    for warning in warnings {
        tracing::warn!("{warning}");
    }
    let directory = directory(kv).await?;
    let ants: Vec<&UserId> = added.iter().filter(|a| !directory.is_team(a)).collect();
    for warning in colony::unserved_warnings(js, &ants).await {
        tracing::warn!("{warning}");
    }
    match directory.team(&id) {
        Some(team) => println!("{}", teams::describe(&directory, team)),
        None => println!("no team {id}"),
    }
    Ok(())
}

/// `ants team set`: change what is given of `team`.
async fn set(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    kv: &ants_daemon::records::direct_kv::DirectKv,
    team: &UserId,
    lead: Option<UserId>,
    (owner, no_owner): (Option<UserId>, bool),
    (description, no_description): (Option<String>, bool),
) -> Result<()> {
    if lead.is_none() && owner.is_none() && !no_owner && description.is_none() && !no_description {
        bail!("nothing to change: give --lead, --owner, --no-owner, --description or --no-description");
    }
    if let Some(description) = &description {
        Team::new(team.clone(), team.clone(), team.clone()).set_description(Some(description))?;
    }
    if let Some(owner) = &owner {
        teams::check_owner(owner, &humans::load(js, &nats.me).await)?;
    }
    let owner = (owner.is_some() || no_owner).then_some(owner);
    let description = (description.is_some() || no_description).then_some(description);
    modify(kv, |directory| {
        if let Some(lead) = &lead {
            directory.set_lead(team, lead)?;
        }
        let Some(entry) = directory.teams.iter_mut().find(|t| t.team == *team) else {
            return Err(TeamRefusal::NoSuchTeam { team: team.clone() });
        };
        if let Some(owner) = &owner {
            entry.owner.clone_from(owner);
        }
        if let Some(description) = &description {
            entry.set_description(description.as_deref()).map_err(|e| TeamRefusal::Invalid { why: e.to_string() })?;
        }
        Ok(())
    })
    .await
}
