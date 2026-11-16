use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::CHANADM_REPEAT;
use crate::{CHANADM_TTL, ChannelEntry, ChannelId, Error, MAX_CHANNEL_OPS, ThreadId, UserId, inbox_prefix, subject};

/// One change an owner asks for in a [`ChannelAdminRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ChannelOp {
    /// Make `ant`, an ant or a human despite the field's name, a member.
    AddMember { ant: UserId },
    /// Remove the member `ant`, an ant or a human.
    RemoveMember { ant: UserId },
    /// Make `ant` the channel's messenger; the default `messenger` clears it.
    SetMessenger { ant: UserId },
    /// Give the channel the default messenger again.
    ClearMessenger,
}

impl ChannelOp {
    /// The user the operation names, if any.
    pub fn ant(&self) -> Option<&UserId> {
        match self {
            Self::AddMember { ant } | Self::RemoveMember { ant } | Self::SetMessenger { ant } => Some(ant),
            Self::ClearMessenger => None,
        }
    }

    /// Whether the operation makes [`ChannelOp::ant`] a member or the messenger, so it must be a human or an ant in
    /// the colony.
    pub fn adds(&self) -> bool {
        matches!(self, Self::AddMember { .. } | Self::SetMessenger { .. })
    }
}

/// An ant's request to the channel manager for changes to a channel it owns, on [`subject::chanadm`]: core NATS, not
/// stored. The ant repeats it with the same id while no answer arrives ([`CHANADM_REPEAT`]); the manager answers a
/// repeat with its first answer. Only the requester's reply inboxes get the answer ([`ChannelAdminRequest::answers_on`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelAdminRequest {
    pub id: Uuid,
    pub channel: ChannelId,
    /// The ant asking, which must own the channel.
    pub ant: UserId,
    /// The thread the ant was handling when it asked, for the record.
    pub thread_id: ThreadId,
    /// What to change, in order: 1 to [`MAX_CHANNEL_OPS`] operations.
    pub ops: Vec<ChannelOp>,
    pub requested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ChannelAdminRequest {
    /// A request expiring [`CHANADM_TTL`] from now.
    pub fn new(channel: ChannelId, ant: UserId, thread_id: ThreadId, ops: Vec<ChannelOp>) -> Self {
        let requested_at = Utc::now();
        let expires_at = requested_at + chrono::TimeDelta::from_std(CHANADM_TTL).expect("a short duration");
        Self { id: Uuid::now_v7(), channel, ant, thread_id, ops, requested_at, expires_at }
    }

    pub fn subject(&self) -> String {
        subject::chanadm(&self.channel, &self.ant)
    }

    /// Check that the request arrived on its own [`ChannelAdminRequest::subject`]. Credentials pin each ant to its own
    /// subjects, while the body is whatever the publisher wrote, so a request naming another ant or channel is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "channel management request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// Whether `reply`, the reply subject the request came with, is one of the requester's own reply inboxes, where
    /// only its credentials (and its leaf's uplink) listen. Not the inbox messages its stream republishes there.
    pub fn answers_on(&self, reply: &str) -> bool {
        let prefix = format!("{}.", inbox_prefix(&self.ant));
        reply.strip_prefix(&prefix).is_some_and(|rest| !rest.is_empty() && !rest.starts_with("stored."))
    }

    /// Why the request is malformed, if it is: no operation, or more than [`MAX_CHANNEL_OPS`].
    pub fn check_ops(&self) -> Result<(), Refusal> {
        match self.ops.len() {
            0 => Err(Refusal::Invalid("no change asked for".to_owned())),
            n if n > MAX_CHANNEL_OPS => Err(Refusal::Invalid(format!("{n} changes; at most {MAX_CHANNEL_OPS}"))),
            _ => Ok(()),
        }
    }
}

/// The channel manager's answer to a [`ChannelAdminRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelAdminReply {
    pub request_id: Uuid,
    pub outcome: ChannelAdminOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ChannelAdminOutcome {
    /// The entry as stored at KV `revision` after the request, and what it changed; nothing changed and nothing was
    /// written when `changed` is empty.
    Applied {
        entry: ChannelEntry,
        revision: u64,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changed: Vec<String>,
    },
    Refused {
        refusal: Refusal,
    },
}

/// Why the channel manager did not apply a [`ChannelAdminRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "refusal", content = "detail", rename_all = "snake_case")]
pub enum Refusal {
    #[error("only the channel's owner may change it; {}", match owner {
        Some(owner) => format!("its owner is {owner}"),
        None => "it has no owner".to_owned(),
    })]
    NotOwner { owner: Option<UserId> },
    #[error("no such channel")]
    NoSuchChannel,
    #[error("the channel's entry is unreadable; a human has to set it again")]
    Unreadable,
    #[error("{0} is a human; the messenger is an ant")]
    Human(UserId),
    #[error("{0} is neither a human nor in the colony; only humans and ants in the colony may be added")]
    NotInColony(UserId),
    #[error("the request expired")]
    Expired,
    #[error("the channel kept changing meanwhile; try again")]
    Busy,
    #[error("invalid request: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn channel_admin_requests_match_their_subject_and_answer_the_requester_only() {
        let ops = vec![ChannelOp::AddMember { ant: p("dev-b") }, ChannelOp::ClearMessenger];
        let request = ChannelAdminRequest::new("ops".parse().unwrap(), p("lead"), "t1".parse().unwrap(), ops);
        assert_eq!(request.subject(), "ants.chanadm.ops.lead");
        assert_eq!(subject::parse_chanadm("ants.chanadm.ops.lead"), Some(("ops".parse().unwrap(), p("lead"))));
        assert_eq!(request.check_subject("ants.chanadm.ops.lead"), Ok(()));
        assert!(request.check_subject("ants.chanadm.ops.dev-a").is_err());
        assert!(request.check_subject("ants.chanadm.dev.lead").is_err());
        assert!(!request.is_expired(request.requested_at) && request.is_expired(request.requested_at + CHANADM_TTL));
        assert!(request.answers_on("_INBOX.lead.abc.1"));
        for forged in ["_INBOX.dev-a.abc", "_INBOX.lead.stored.dev-a", "_INBOX.lead.", "_INBOX.leader.x", "ants.x"] {
            assert!(!request.answers_on(forged), "{forged}");
        }
        assert_eq!(request.check_ops(), Ok(()));
        let none = ChannelAdminRequest { ops: Vec::new(), ..request.clone() };
        assert!(none.check_ops().is_err());
        let many = ChannelAdminRequest { ops: vec![ChannelOp::ClearMessenger; MAX_CHANNEL_OPS + 1], ..request.clone() };
        assert!(many.check_ops().is_err());

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["ops"][0], serde_json::json!({"op": "add_member", "ant": "dev-b"}));
        assert_eq!(json["ops"][1], serde_json::json!({"op": "clear_messenger"}));
        assert_eq!(serde_json::from_value::<ChannelAdminRequest>(json).unwrap(), request);
        let refused = ChannelAdminReply {
            request_id: request.id,
            outcome: ChannelAdminOutcome::Refused { refusal: Refusal::NotInColony(p("dev-z")) },
        };
        let json = serde_json::to_value(&refused).unwrap();
        assert_eq!(
            json["outcome"],
            serde_json::json!({"outcome": "refused", "refusal": {"refusal": "not_in_colony", "detail": "dev-z"}})
        );
        assert_eq!(serde_json::from_value::<ChannelAdminReply>(json).unwrap(), refused);
    }
}
