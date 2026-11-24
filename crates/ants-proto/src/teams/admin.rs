//! What an ant asks of `ants signer` to change the teams it manages ([`TeamAdminRequest`]): the directory is one entry
//! for every team, so no subject ties a change to a team, and ant credentials write nothing of [`TEAMS_BUCKET`]; the
//! signer writes for the ant, on the subject naming it, only what the ant may ([`TeamDirectory::apply_for_ant`]).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, MAX_TEAM_OPS, TEAMADM_TTL, TeamRefusal, ThreadId, UserId, subject};
#[cfg(doc)]
use crate::{TEAMADM_REPEAT, TEAMS_BUCKET, TeamDirectory};

/// One change an ant asks for in a [`TeamAdminRequest`], of the request's team.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TeamOp {
    /// Make `member`, an ant of the bus or a team, on no team either, a member.
    AddMember {
        member: UserId,
    },
    /// Remove `member`, an ant or a team, but not the lead.
    RemoveMember {
        member: UserId,
    },
    /// Hand the lead to `ant`, a direct ant member.
    SetLead {
        ant: UserId,
    },
    /// Clear the lead: the team then takes the lead of the nearest team above it that has one.
    ClearLead,
    SetDescription {
        description: String,
    },
    ClearDescription,
    /// Create the team `team` as a member of the request's team, led by `lead`, if given: an ant of the bus on no team,
    /// or a direct member of the request's team, which moves down into the new team. Without one, the new team takes
    /// the lead of the request's team, or of the nearest team above with one.
    CreateTeam {
        team: UserId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lead: Option<UserId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    /// Delete `team`, a member of the request's team; its members are on no team after.
    DeleteTeam {
        team: UserId,
    },
}

impl TeamOp {
    /// The ant the operation brings onto a team, if any: a member added (or a team, which the directory tells) or a new
    /// team's lead.
    pub fn ant_added(&self) -> Option<&UserId> {
        match self {
            Self::AddMember { member } => Some(member),
            Self::CreateTeam { lead, .. } => lead.as_ref(),
            Self::RemoveMember { .. }
            | Self::SetLead { .. }
            | Self::ClearLead
            | Self::SetDescription { .. }
            | Self::ClearDescription
            | Self::DeleteTeam { .. } => None,
        }
    }

    /// The team the operation creates, if any, whose name an ant, a human or a reserved name may hold already.
    pub fn team_created(&self) -> Option<&UserId> {
        match self {
            Self::CreateTeam { team, .. } => Some(team),
            _ => None,
        }
    }
}

/// An ant's request to change a team it manages, on [`subject::teamadm`]: core NATS, not stored. Only `ants signer`
/// answers, on [`subject::teamadm_reply`], which only the ant reads, and writes [`TEAMS_BUCKET`] for the ant only when
/// the directory is still the one the ant read (at stream sequence `last`), else answers
/// [`TeamAdminOutcome::Changed`]. The ant repeats the request with the same id while no answer arrives
/// ([`TEAMADM_REPEAT`]); the signer answers a repeat with its first answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamAdminRequest {
    pub id: Uuid,
    /// The ant asking, which must manage `team`.
    pub ant: UserId,
    /// The team changed.
    pub team: UserId,
    /// What to change, in order: 1 to [`MAX_TEAM_OPS`] operations.
    pub ops: Vec<TeamOp>,
    /// The stream sequence of the directory's last message as the ant read it, 0 for none.
    pub last: u64,
    /// The thread the ant was handling when it asked, for the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<ThreadId>,
    pub requested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl TeamAdminRequest {
    /// A request expiring [`TEAMADM_TTL`] from now.
    pub fn new(ant: UserId, team: UserId, ops: Vec<TeamOp>, last: u64) -> Self {
        let requested_at = Utc::now();
        let expires_at = requested_at + chrono::TimeDelta::from_std(TEAMADM_TTL).expect("a short duration");
        Self { id: Uuid::now_v7(), ant, team, ops, last, thread_id: None, requested_at, expires_at }
    }

    pub fn subject(&self) -> String {
        subject::teamadm(&self.ant)
    }

    pub fn reply_subject(&self) -> String {
        subject::teamadm_reply(&self.ant)
    }

    /// Check that the request arrived on its own [`TeamAdminRequest::subject`]. Credentials pin each ant to its own,
    /// while the body is whatever the publisher wrote, so a request naming another ant is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "team change request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// Why the request is malformed, if it is: no operation, or more than [`MAX_TEAM_OPS`].
    pub fn check_ops(&self) -> Result<(), TeamRefusal> {
        match self.ops.len() {
            0 => Err(TeamRefusal::Invalid { why: "no change asked for".to_owned() }),
            n if n > MAX_TEAM_OPS => Err(TeamRefusal::Invalid { why: format!("{n} changes; at most {MAX_TEAM_OPS}") }),
            _ => Ok(()),
        }
    }
}

/// The answer to a [`TeamAdminRequest`], on [`subject::teamadm_reply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamAdminReply {
    pub request_id: Uuid,
    pub ant: UserId,
    pub outcome: TeamAdminOutcome,
}

impl TeamAdminReply {
    pub fn subject(&self) -> String {
        subject::teamadm_reply(&self.ant)
    }

    /// Check that the answer arrived on its own [`TeamAdminReply::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "team change answer", user: self.ant.clone(), at: subject.to_owned() })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TeamAdminOutcome {
    /// The directory as stored at stream sequence `last` after the request, and what it changed; nothing was written
    /// when `changed` is empty.
    Applied {
        last: u64,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changed: Vec<String>,
    },
    /// The directory is no longer the one the ant read: nothing changed, and the ant reads it again.
    Changed,
    Refused {
        refusal: TeamRefusal,
    },
}
