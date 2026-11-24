//! The team tools: `list_teams` shows the forest of teams, or one team with its members and their colony state, and
//! `manage_team` changes a team the ant manages, its own or one beneath it, through `ants signer`
//! ([`crate::teams::keeper`]). A message to a team goes to its lead ([`Hub::addressed`]), a post's @mention of a team
//! mentions its lead ([`Hub::through_teams`]), and `list_ants` and `route`
//! keep to a team's ants, through its member teams, when given one ([`Hub::team_ants`]).

use std::fmt::Write as _;
use std::sync::Arc;

use ants_proto::{
    ChannelEntry, ChannelPost, ColonyEntry, TEAMADM_WAIT, TeamAdminOutcome, TeamDirectory, TeamOp, ThreadId, UserId,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::tools::{Tools, text};
use crate::colony;
use crate::teams::keeper::Changer;
use crate::teams::{READ_TIMEOUT, Read};

/// Reads the team directory: None when the bus keeps no teams.
pub type TeamReader = Arc<dyn Fn() -> BoxFuture<'static, Result<Option<Read>>> + Send + Sync>;

/// Changes a team for the ant: the team, the changes and the thread asking; None when no signer answered.
pub type TeamWriter = Arc<
    dyn Fn(UserId, Vec<TeamOp>, Option<ThreadId>) -> BoxFuture<'static, Result<Option<TeamAdminOutcome>>> + Send + Sync,
>;

/// What the team tools read and change the teams through.
#[derive(Clone)]
pub struct TeamAccess {
    pub read: TeamReader,
    pub change: TeamWriter,
}

/// Reads the directory of `changer` and changes it for `ant` through it: through `ants signer`, or as it would.
pub fn nats_teams(changer: Changer, ant: UserId) -> TeamAccess {
    let kv = changer.kv.clone();
    TeamAccess {
        read: Arc::new(move || {
            let kv = kv.clone();
            Box::pin(async move { crate::teams::read(&kv).await })
        }),
        change: Arc::new(move |team, ops, thread| {
            let (changer, ant) = (changer.clone(), ant.clone());
            Box::pin(async move { changer.change(&ant, &team, ops, thread).await })
        }),
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListTeams {
    /// The team to show, such as `backend`; omitted for every team.
    #[serde(default)]
    team: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ManageTeam {
    /// The team to change or show, such as `backend`: one this ant leads, or one beneath it. Omitted to list the teams
    /// this ant manages.
    #[serde(default)]
    team: Option<String>,
    /// A team to create as a member of `team`.
    #[serde(default)]
    create: Option<NewTeam>,
    /// Ants of the bus, or teams, each on no team yet, to make members.
    #[serde(default)]
    add: Vec<String>,
    /// Members to remove, ants or teams; a team removed keeps its members, and removing the lead clears it.
    #[serde(default)]
    remove: Vec<String>,
    /// A direct ant member to hand the lead of `team` to.
    #[serde(default)]
    lead: Option<String>,
    /// Clear the lead of `team`, which then takes the lead of the nearest team above it that has one.
    #[serde(default)]
    clear_lead: bool,
    /// What the team is for: up to 1000 characters, which each member's sessions start with.
    #[serde(default)]
    description: Option<String>,
    /// Clear the description.
    #[serde(default)]
    clear_description: bool,
    /// A team that is a member of `team` to delete; its members are on no team after.
    #[serde(default)]
    delete: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct NewTeam {
    /// The new team's name: 1-64 of a-z, 0-9 and '-', no ant's, human's or team's.
    team: String,
    /// Its lead, if any: an ant of the bus on no team, or a direct member of `team`, which moves down into it. Without
    /// one, the new team takes the lead of `team` (or of the nearest team above with one).
    #[serde(default)]
    lead: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

/// A name the tools take: an ant, a human or a team, with or without its `@`.
fn name(name: &str) -> Result<UserId, String> {
    name.trim().trim_start_matches('@').parse().map_err(|e| format!("{e}"))
}

impl Hub {
    /// The team directory as it stands, read within [`READ_TIMEOUT`]: None when teams are not served here or the bus
    /// keeps none.
    pub(super) async fn directory(&self) -> Result<Option<TeamDirectory>, String> {
        let Some(access) = self.teams.get() else { return Ok(None) };
        match tokio::time::timeout(READ_TIMEOUT, (access.read)()).await {
            Ok(Ok(None)) => Ok(None),
            Ok(Ok(Some(read))) => read.directory.map(Some).map_err(|e| e.to_string()),
            Ok(Err(e)) => Err(format!("cannot read the teams: {e:#}")),
            Err(_) => Err(format!("cannot read the teams: no answer within {}s", READ_TIMEOUT.as_secs())),
        }
    }

    /// The team directory, refused when there is none to read.
    async fn teams(&self) -> Result<TeamDirectory, String> {
        self.directory().await?.ok_or_else(|| "the bus keeps no teams (`ants init` makes their bucket)".to_owned())
    }

    /// Whom a message to `to` goes to, and the team it was addressed to: a team's lead (its own, else the nearest one
    /// above it), refused for a team with no lead in it nor above it, and for a team this ant leads, as a message to the ant itself is; any other name as it is. A directory that
    /// cannot be read holds no message back: the name is then taken for an ant's.
    pub(super) async fn addressed(&self, to: &str) -> Result<(UserId, Option<UserId>), String> {
        let to = name(to)?;
        if self.teams.get().is_none() || to == self.ant || self.humans.is_human(&to) {
            return Ok((to, None));
        }
        let directory = match self.directory().await {
            Ok(Some(directory)) => directory,
            Ok(None) => return Ok((to, None)),
            Err(e) => {
                tracing::debug!(%to, error = e, "taking the recipient for an ant");
                return Ok((to, None));
            }
        };
        if !directory.is_team(&to) {
            return Ok((to, None));
        }
        let lead = directory.lead_of(&to).map_err(|e| format!("nothing sent: {e}"))?.clone();
        if lead == self.ant {
            return Err(format!(
                "team {to} sends to its lead, this ant; hand the work on to its members (list_teams shows them), or \
                 call handoff to start work on another thread"
            ));
        }
        Ok((lead, Some(to)))
    }

    /// `post` to the channel of `entry` with each team it @mentions standing for the team's lead, who has to be a
    /// member ([`ChannelPost::through_teams`]). A directory that cannot be read holds no post back: its mentions then
    /// stay as they are.
    pub(super) async fn through_teams(&self, post: ChannelPost, entry: &ChannelEntry) -> Result<ChannelPost, String> {
        if self.teams.get().is_none() || post.mentions.is_empty() {
            return Ok(post);
        }
        match self.directory().await {
            Ok(Some(directory)) => post.through_teams(&directory, Some(entry)).map_err(|e| e.to_string()),
            Ok(None) => Ok(post),
            Err(e) => {
                tracing::debug!(channel = %entry.channel, error = e, "taking the post's mentions as they are");
                Ok(post)
            }
        }
    }

    /// The ants of team `team` and of the teams beneath it.
    pub(super) async fn team_ants(&self, team: &str) -> Result<Vec<UserId>, String> {
        let team = name(team)?;
        let directory = self.teams().await?;
        if !directory.is_team(&team) {
            return Err(format!("there is no team {team}; list_teams shows the teams"));
        }
        Ok(directory.ants_under(&team))
    }

    /// What `list_teams` shows: every team as a tree, or `team` with the teams above it.
    async fn list_teams(&self, team: Option<&str>) -> Result<String, String> {
        let directory = self.teams().await?;
        let colony = (self.colony)().await.unwrap_or_else(|e| {
            tracing::warn!(error = format!("{e:#}"), "reading the colony; members show as not in it");
            Vec::new()
        });
        let shown = Shown { directory: &directory, colony: &colony, ant: &self.ant, now: Utc::now() };
        let Some(team) = team.map(str::trim).filter(|t| !t.is_empty()) else {
            if directory.teams.is_empty() {
                return Ok("No team is on the bus; a human makes one with `ants team create`.".to_owned());
            }
            let mut text = shown.own();
            for root in directory.roots() {
                shown.tree(&root.team, 0, &mut text);
            }
            return Ok(text.trim_end().to_owned());
        };
        let team = name(team)?;
        if !directory.is_team(&team) {
            return Err(format!("there is no team {team}; list_teams without a team shows them all"));
        }
        let mut text = String::new();
        let above: Vec<String> = directory.ancestors(&team).iter().map(|t| format!("team {}", t.team)).collect();
        if !above.is_empty() {
            let _ = writeln!(text, "Within {}.", above.join(", within "));
        }
        shown.tree(&team, 0, &mut text);
        Ok(text.trim_end().to_owned())
    }

    /// What `manage_team` does: list the teams the ant manages, show one, or ask `ants signer` to change one, from
    /// a trusted `thread` only.
    async fn manage_team(&self, thread: &ThreadId, args: ManageTeam) -> Result<String, String> {
        let access = self.teams.get().ok_or("teams cannot be managed from this daemon")?;
        let Some(team) = args.team.as_deref().map(str::trim).filter(|t| !t.is_empty()) else {
            return self.managed().await;
        };
        let team = name(team)?;
        let ops = ops(&args)?;
        if ops.is_empty() {
            return self.list_teams(Some(team.as_str())).await;
        }
        let current = self
            .handling(thread)
            .ok_or("no message is being handled on this thread; teams are changed only while one is")?;
        if !self.trusted(thread, &current).await? {
            return Err("this thread is untrusted: a webhook or schedule started it, or the trail run whose step it \
                        answers; teams are changed only from trusted threads"
                .to_owned());
        }
        let outcome = (access.change)(team.clone(), ops, Some(thread.clone()))
            .await
            .map_err(|e| format!("team {team} was not changed: {e:#}"))?;
        let Some(outcome) = outcome else {
            return Err(format!(
                "no signer answered in {}s (start `ants signer`), so the change may not have been made; a human can \
                 make it with `ants team`",
                TEAMADM_WAIT.as_secs()
            ));
        };
        match outcome {
            TeamAdminOutcome::Applied { changed, .. } if changed.is_empty() => {
                Ok(format!("Nothing to change.\n{}", self.list_teams(Some(team.as_str())).await?))
            }
            TeamAdminOutcome::Applied { changed, .. } => {
                let shown = self.list_teams(Some(team.as_str())).await.unwrap_or_default();
                Ok(format!("Changed team {team}: {}.\n{shown}", changed.join("; ")).trim_end().to_owned())
            }
            TeamAdminOutcome::Changed => {
                Err(format!("team {team} was not changed: the teams kept changing meanwhile; try again"))
            }
            TeamAdminOutcome::Refused { refusal } => Err(format!("team {team} was not changed: {refusal}")),
        }
    }

    /// The teams the ant manages: those it leads and the teams beneath them.
    async fn managed(&self) -> Result<String, String> {
        let directory = self.teams().await?;
        let led = directory.led_by(&self.ant);
        if led.is_empty() {
            return Ok(format!(
                "{} leads no team, so it manages none; a human makes an ant a team's lead (`ants team set TEAM \
                 --lead ANT`).",
                self.ant
            ));
        }
        let lines: Vec<String> = led
            .iter()
            .map(|team| {
                let beneath: Vec<&str> = directory
                    .teams
                    .iter()
                    .filter(|t| t.team != team.team && directory.is_beneath(&t.team, &team.team))
                    .map(|t| t.team.as_str())
                    .collect();
                match beneath.as_slice() {
                    [] => format!("- team {}", team.team),
                    beneath => format!("- team {}, and beneath it: {}", team.team, beneath.join(", ")),
                }
            })
            .collect();
        Ok(format!("{} manages:\n{}", self.ant, lines.join("\n")))
    }
}

/// The changes `args` asks for, in order: the team created, the members added and removed, the lead, the description
/// and the team deleted.
fn ops(args: &ManageTeam) -> Result<Vec<TeamOp>, String> {
    let mut ops = Vec::new();
    if let Some(new) = &args.create {
        let description = new.description.clone().filter(|d| !d.trim().is_empty());
        let lead = new.lead.as_deref().map(name).transpose()?;
        ops.push(TeamOp::CreateTeam { team: name(&new.team)?, lead, description });
    }
    for added in &args.add {
        ops.push(TeamOp::AddMember { member: name(added)? });
    }
    for removed in &args.remove {
        ops.push(TeamOp::RemoveMember { member: name(removed)? });
    }
    match (&args.lead, args.clear_lead) {
        (Some(_), true) => return Err("give either lead or clear_lead, not both".to_owned()),
        (Some(lead), false) => ops.push(TeamOp::SetLead { ant: name(lead)? }),
        (None, true) => ops.push(TeamOp::ClearLead),
        (None, false) => {}
    }
    match (&args.description, args.clear_description) {
        (Some(_), true) => return Err("give either description or clear_description, not both".to_owned()),
        (Some(description), false) => ops.push(TeamOp::SetDescription { description: description.clone() }),
        (None, true) => ops.push(TeamOp::ClearDescription),
        (None, false) => {}
    }
    if let Some(deleted) = &args.delete {
        ops.push(TeamOp::DeleteTeam { team: name(deleted)? });
    }
    Ok(ops)
}

/// What `list_teams` draws the teams from.
struct Shown<'a> {
    directory: &'a TeamDirectory,
    colony: &'a [ColonyEntry],
    ant: &'a UserId,
    now: DateTime<Utc>,
}

impl Shown<'_> {
    /// The line naming the ant's own team, if it is on one.
    fn own(&self) -> String {
        match self.directory.holder(self.ant) {
            Some(team) if team.lead.as_ref() == Some(self.ant) => format!("{} leads team {}.\n", self.ant, team.team),
            Some(team) => format!("{} is on team {}.\n", self.ant, team.team),
            None => format!("{} is on no team.\n", self.ant),
        }
    }

    /// `team` at `depth`, with its description and members, the member teams drawn beneath it.
    fn tree(&self, team: &UserId, depth: usize, text: &mut String) {
        let Some(team) = self.directory.team(team) else { return };
        let header = if depth == 0 { String::new() } else { format!("{}- ", "  ".repeat(depth)) };
        let indent = "  ".repeat(depth + 1);
        let lead = match self.directory.lead_source(&team.team) {
            Some(from) if from.team == team.team => from.lead.as_ref().map_or_default(UserId::to_string),
            Some(from) => format!("{}, from team {}", from.lead.as_ref().map_or_default(UserId::to_string), from.team),
            None => "none, nor above it".to_owned(),
        };
        let owner = team.owner.as_ref().map_or_default(|owner| format!("; owner {owner}, a human"));
        let _ = writeln!(text, "{header}team {} (lead {lead}{owner})", team.team);
        if let Some(description) = &team.description {
            for line in description.lines() {
                let _ = writeln!(text, "{indent}| {line}");
            }
        }
        for member in &team.members {
            if self.directory.is_team(member) {
                self.tree(member, depth + 1, text);
                continue;
            }
            let led = if team.lead.as_ref() == Some(member) { " (lead)" } else { "" };
            let _ = writeln!(text, "{indent}- {member}{led}: {}", self.state(member));
        }
    }

    /// How the ant `ant` stands in the colony.
    fn state(&self, ant: &UserId) -> String {
        if ant == self.ant {
            return "this ant".to_owned();
        }
        let Some(entry) = self.colony.iter().find(|e| e.ant == *ant) else {
            return "not in the colony".to_owned();
        };
        let mut state = if !colony::is_live(entry, self.now) {
            "stale".to_owned()
        } else if entry.unserved.is_some() {
            "unserved".to_owned()
        } else if entry.paused.is_some() {
            "paused".to_owned()
        } else if entry.signed_out.is_some() && entry.fallback.is_none() {
            "signed out".to_owned()
        } else if entry.failing.is_some() {
            "failing".to_owned()
        } else if let Some(limited) = &entry.limited {
            limited.kind.as_str().to_owned()
        } else {
            entry.activity.as_str().to_owned()
        };
        let _ = write!(state, " on {}", entry.machine);
        state
    }
}

#[tool_router(router = teams_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "List the teams: named groups of ants and teams, forming a forest, each ant and team on \
        one team at most. Messages to a team go to its lead, a direct ant member, or without one to the lead of the \
        nearest team above it that has one. With `team`, only that team \
        and the teams beneath it. Each team shows its lead, its human owner, its description and its members, a \
        member ant with its state in the colony. send_message and ask_ant take a team in `to`, reaching its lead; \
        list_ants and route take `team` to keep to its ants, through its member teams.")]
    async fn list_teams(&self, Parameters(args): Parameters<ListTeams>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.list_teams(args.team.as_deref()).await))
    }

    #[tool(description = "Manage a team this ant leads, or one beneath it: `create` makes a team as a member of \
        `team` (its lead, if any, an ant on no team or a direct member of `team`, moving down into it; without one \
        it takes the lead of `team`), `add` makes ants of the bus or teams on no team members, `remove` takes \
        members out (a lead removed leaves its team without one), `lead` hands the lead to a direct ant member \
        (`clear_lead` clears it, the team then taking the nearest lead above it), `description` sets what the team is for (`clear_description` \
        clears it) and `delete` deletes a team that is a member of `team`. The changes apply in that order, all \
        or none. `ants signer` makes them, checking that this ant manages the team; the owner and the teams above \
        are a human's to change. Refused on a thread a webhook or schedule started. With only `team` it shows the \
        team; with nothing it lists the teams this ant manages.")]
    async fn manage_team(
        &self,
        Parameters(args): Parameters<ManageTeam>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.manage_team(&thread, args).await))
    }
}
