//! What an ant asks of `ants signer` to save a trail as its own or remove one it saved ([`SaveTrailRequest`]): a
//! trail's key is its name, which no subject ties to its saver, so ant credentials write nothing of
//! [`TRAILS_BUCKET`] and the signer writes for the ant, on the subject naming it, only what the ant may.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::TRAILS_BUCKET;
use crate::{Error, SavedTrail, TrailName, UserId, subject};

/// How often an ant repeats a [`SaveTrailRequest`] while no answer arrives; requests are core NATS, so one sent while
/// nothing answers is lost.
pub const SAVE_TRAIL_REPEAT: Duration = Duration::from_secs(2);
/// How long an ant waits for the answer to a [`SaveTrailRequest`] in all.
pub const SAVE_TRAIL_WAIT: Duration = Duration::from_secs(10);

/// An ant's request to save a trail as its own or remove one it saved, on [`subject::save_trail`]: core NATS, not
/// stored. Only `ants signer` answers, on [`subject::save_trail_reply`], which only the ant reads, writing
/// [`TRAILS_BUCKET`] for the ant only when the entry is still the one the ant checked and is absent or saved by the
/// ant. The ant repeats the request with the same id while no answer arrives ([`SAVE_TRAIL_REPEAT`]); a repeat of a
/// change made already is answered [`SaveTrailOutcome::Done`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaveTrailRequest {
    pub id: Uuid,
    /// The ant asking.
    pub ant: UserId,
    pub requested_at: DateTime<Utc>,
    pub change: TrailChange,
}

/// What a [`SaveTrailRequest`] changes in [`TRAILS_BUCKET`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TrailChange {
    /// Store `trail`, whose saver is the ant: as a new trail with no `last`, else over the ant's own trail of its name
    /// whose key's last message is at stream sequence `last`.
    Save {
        trail: SavedTrail,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last: Option<u64>,
    },
    /// Remove the ant's own trail `name`, whose key's last message is at stream sequence `last`.
    Remove { name: TrailName, last: u64 },
}

impl TrailChange {
    /// The name of the trail changed.
    pub fn name(&self) -> &TrailName {
        match self {
            Self::Save { trail, .. } => &trail.name,
            Self::Remove { name, .. } => name,
        }
    }
}

impl SaveTrailRequest {
    pub fn new(ant: UserId, change: TrailChange) -> Self {
        Self { id: Uuid::now_v7(), ant, requested_at: Utc::now(), change }
    }

    pub fn subject(&self) -> String {
        subject::save_trail(&self.ant)
    }

    pub fn reply_subject(&self) -> String {
        subject::save_trail_reply(&self.ant)
    }

    /// Check that the request arrived on its own [`SaveTrailRequest::subject`]. Credentials pin each ant to its own,
    /// while the body is whatever the publisher wrote, so a request naming another ant is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "trail save request", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// The answer to a [`SaveTrailRequest`], on [`subject::save_trail_reply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaveTrailReply {
    pub request_id: Uuid,
    pub ant: UserId,
    pub outcome: SaveTrailOutcome,
}

impl SaveTrailReply {
    pub fn subject(&self) -> String {
        subject::save_trail_reply(&self.ant)
    }

    /// Check that the answer arrived on its own [`SaveTrailReply::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "trail save answer", user: self.ant.clone(), at: subject.to_owned() })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum SaveTrailOutcome {
    /// The trail is saved, or removed.
    Done,
    /// The entry of the trail's name is no longer the one the ant checked: nothing changed.
    Changed,
    /// Nothing changed, and why: the trail is another's, or the request is not one an ant may make.
    Refused { why: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Saver, p};

    #[test]
    fn a_request_or_answer_on_another_ant_s_subject_is_forged() {
        let trail = SavedTrail {
            name: "deploy".parse().unwrap(),
            description: String::new(),
            phases: Vec::new(),
            waypoints: Vec::new(),
            script: "1".to_owned(),
            saved_by: Saver::Ant { user: p("dev-a") },
            saved_from: None,
            saved_at: Utc::now(),
        };
        let request = SaveTrailRequest::new(p("dev-a"), TrailChange::Save { trail, last: None });
        assert_eq!(request.subject(), "ants.savetrail.dev-a");
        assert!(request.check_subject("ants.savetrail.dev-a").is_ok());
        assert!(request.check_subject("ants.savetrail.dev-b").is_err());
        let reply = SaveTrailReply { request_id: request.id, ant: p("dev-a"), outcome: SaveTrailOutcome::Done };
        assert_eq!(reply.subject(), "_INBOX.dev-a.savetrail");
        assert!(reply.check_subject("_INBOX.dev-b.savetrail").is_err());
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["change"]["op"], "save");
        assert_eq!(serde_json::from_value::<SaveTrailRequest>(json).unwrap(), request);
    }
}
