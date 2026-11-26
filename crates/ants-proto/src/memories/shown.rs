//! What any human, with web credentials too, asks the daemon serving an ant for of its memories over core NATS
//! ([`MemoriesRequest`] on [`subject::memories`]), and the daemon's answer ([`MemoriesReply`] on
//! [`subject::memoried`]): the ant's core memories and its provider's memories view ([`MemoriesShown`]), capped as an
//! admin's read is. The harness's memory files and bd memories stay with that read ([`crate::AdminOp::ReadMemories`]),
//! which only an admin orders. Nothing of it is stored, so only the daemon serving the ant answers, while it does, and
//! a daemon whose config sets `[admin] remote = false` refuses it as it refuses every admin order.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{CoreMemories, ProviderMemories, Skipped};
use crate::{Error, UserId, subject};

/// A human asking the daemon serving `ant` for its memories, on [`subject::memories`] with core NATS. The daemon
/// answers with a [`MemoriesReply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoriesRequest {
    pub id: Uuid,
    pub ant: UserId,
    /// Who asks: a human, as only human credentials publish to [`subject::memories`].
    pub by: UserId,
    pub sent_at: DateTime<Utc>,
}

impl MemoriesRequest {
    pub fn new(ant: UserId, by: UserId) -> Self {
        Self { id: Uuid::now_v7(), ant, by, sent_at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::memories(&self.ant)
    }

    pub fn reply_subject(&self) -> String {
        subject::memoried(&self.ant)
    }

    /// Check that the request arrived on its own [`MemoriesRequest::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "memories request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn answer(&self, outcome: MemoriesOutcome) -> MemoriesReply {
        MemoriesReply { request_id: self.id, ant: self.ant.clone(), outcome }
    }
}

/// The answer of the daemon serving `ant` to a [`MemoriesRequest`], on [`subject::memoried`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoriesReply {
    pub request_id: Uuid,
    pub ant: UserId,
    #[serde(flatten)]
    pub outcome: MemoriesOutcome,
}

impl MemoriesReply {
    pub fn subject(&self) -> String {
        subject::memoried(&self.ant)
    }

    /// Check that the answer arrived on its own [`MemoriesReply::subject`], which credentials pin to the ant's own.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "memories answer", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// What came of a [`MemoriesRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum MemoriesOutcome {
    Read(MemoriesShown),
    Refused { error: String },
}

/// The memories of an ant that any human reads: its core memories and its provider's memories view, with what was
/// left out or cut short. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoriesShown {
    pub core: CoreMemories,
    /// The memories view of the ant's provider, when it serves one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderMemories>,
    /// What was found but left out or cut short, and why.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<Skipped>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn a_memories_request_and_its_answer_go_on_the_ant_s_subjects() {
        let request = MemoriesRequest::new(p("dev-a"), p("alice"));
        assert_eq!(
            (request.subject(), request.reply_subject()),
            ("ants.memories.dev-a".into(), "ants.memoried.dev-a".into())
        );
        assert_eq!(request.check_subject("ants.memories.dev-a"), Ok(()));
        assert!(request.check_subject("ants.memories.dev-b").is_err());
        let core = CoreMemories { whoami: Some("a tester".into()), ..CoreMemories::default() };
        let read = request.answer(MemoriesOutcome::Read(MemoriesShown { core, ..MemoriesShown::default() }));
        assert_eq!(read.check_subject("ants.memoried.dev-a"), Ok(()));
        assert!(read.check_subject("ants.memoried.dev-b").is_err());
        let json = serde_json::to_value(&read).unwrap();
        assert_eq!((json["outcome"].as_str(), json["core"]["whoami"].as_str()), (Some("read"), Some("a tester")));
        assert_eq!(serde_json::from_value::<MemoriesReply>(json).unwrap(), read);
        let refused = request.answer(MemoriesOutcome::Refused { error: "no".into() });
        let json = serde_json::to_value(&refused).unwrap();
        assert_eq!((json["outcome"].as_str(), json["error"].as_str()), (Some("refused"), Some("no")));
    }
}
