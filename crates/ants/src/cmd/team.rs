//! `ants team`: create, change, list, show and delete teams, as a human writing the team directory by
//! compare-and-set, and telling observers what it changed on the human's status ([`ants_daemon::teams::tell`]).

use ants_proto::{TeamChange, TeamDirectory, TeamRefusal, UserId};
use anyhow::{Result, bail};
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::ants::colony;
use crate::completions;
use crate::connection::Nats;
use crate::people::humans;
use crate::teams::change::{Change, Create};
use crate::teams::{self, parse_name};

#[derive(Subcommand, Debug)]
pub(crate) enum TeamCommand {
    /// Create a team, led by --lead, an ant, which is its first member, or, without --lead, by the lead of the team
    /// above it once it is put under one; fails when a team, an ant or a human holds the name. Each ant and each team is on one team at most; `ants team add PARENT TEAM` puts a team under another.
    Create {
        /// The team: `backend` or `@backend`.
        #[arg(value_parser = parse_name)]
        team: UserId,
        /// The ant messages to the team go to: an ant of the bus on no team. Without it, the team takes the lead of
        /// the nearest team above it with one.
        #[arg(long, value_name = "ANT", value_parser = parse_name, add = ArgValueCandidates::new(completions::ants))]
        lead: Option<UserId>,
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
        #[arg(long, value_name = "ANT", value_parser = parse_name, add = ArgValueCandidates::new(completions::ants), conflicts_with = "no_lead")]
        lead: Option<UserId>,
        /// Leave the team without a lead of its own: it takes the lead of the nearest team above it with one.
        #[arg(long)]
        no_lead: bool,
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
    /// Remove members, ants or teams, from a team; removing its lead leaves it without a lead of its own, taking
    /// that of the nearest team above it with one.
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
    let (client, js) = nats.connect("ants-cli").await?;
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
            let changes = command.changes();
            change(nats, &js, &kv, command).await?;
            teams::change::tell(&client, &nats.me.id, &kv, &changes).await;
            Ok(())
        }
    }
}

impl TeamCommand {
    /// The team the command changes, and how.
    fn changes(&self) -> Vec<(UserId, TeamChange)> {
        match self {
            Self::Create { team, .. } => vec![(team.clone(), TeamChange::Created)],
            Self::Set { team, .. } | Self::Add { team, .. } | Self::Remove { team, .. } => {
                vec![(team.clone(), TeamChange::Changed)]
            }
            Self::Delete { team } => vec![(team.clone(), TeamChange::Deleted)],
            Self::List { .. } | Self::Show { .. } => Vec::new(),
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
    let faces = crate::ants::avatars::Faces::stdout(crate::ants::avatars::read_or_none(js).await);
    let above: Vec<String> = directory.ancestors(id).iter().map(|t| format!(", within team {}", t.team)).collect();
    println!("team {id}{}", above.concat());
    if let Some(description) = &team.description {
        println!("  description: {description}");
    }
    println!("  lead: {}", teams::lead_label(&directory, team));
    if let Some(owner) = &team.owner {
        println!("  owner: {owner}");
    }
    if let Err(refusal) = directory.lead_of(id) {
        println!("  {refusal}");
    }
    println!("members:");
    for line in teams::tree_lines(&tree, &|ant| faces.named(ant)) {
        println!("{line}");
    }
    Ok(())
}

/// Every `ants team` subcommand that changes the directory, as the CLI's human, written as `ants chat` writes it
/// ([`teams::change`]).
async fn change(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    kv: &ants_daemon::records::direct_kv::DirectKv,
    command: TeamCommand,
) -> Result<()> {
    let humans = humans::load(js, &nats.me).await;
    let write = async |team: &UserId, change: Change| change.write(js, nats.domain, kv, team, &humans).await;
    let (id, warnings, added) = match command {
        TeamCommand::Create { team, lead, members, owner, description } => {
            let create = Create { team, lead, members, owner, description };
            let (created, _) = create.write(js, nats.domain, kv, &nats.me.id, &humans).await?;
            (created.team, Vec::new(), created.members)
        }
        TeamCommand::Set { team, lead, no_lead, owner, no_owner, description, no_description } => {
            if lead.is_none() && !no_lead && owner.is_none() && !no_owner && description.is_none() && !no_description {
                bail!(
                    "nothing to change: give --lead, --no-lead, --owner, --no-owner, --description or --no-description"
                );
            }
            let change = Change {
                lead: (lead.is_some() || no_lead).then_some(lead),
                owner: (owner.is_some() || no_owner).then_some(owner),
                description: (description.is_some() || no_description).then_some(description),
                ..Change::default()
            };
            write(&team, change).await?;
            (team, Vec::new(), Vec::new())
        }
        TeamCommand::Add { team, names } => {
            let changed = write(&team, Change { add: names.clone(), ..Change::default() }).await?;
            let warnings = names
                .iter()
                .filter(|n| !changed.added.contains(n))
                .map(|n| format!("{n} is already a member of team {team}"))
                .collect();
            (team, warnings, changed.added)
        }
        TeamCommand::Remove { team, names } => {
            let changed = write(&team, Change { remove: names.clone(), ..Change::default() }).await?;
            let warnings = names
                .iter()
                .filter(|n| !changed.removed.contains(n))
                .map(|n| format!("{n} is no member of team {team}"))
                .collect();
            (team, warnings, Vec::new())
        }
        TeamCommand::Delete { team } => {
            for line in teams::change::delete(kv, &team).await? {
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
