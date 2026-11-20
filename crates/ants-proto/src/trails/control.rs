//! A human asking a run's conductor to start, stop or resume it ([`TrailRequest`]), and its answer ([`TrailReply`]).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::TrailLimits;
#[cfg(doc)]
use crate::Envelope;
use crate::{Error, RunId, ThreadId, TrailName, UserId, subject};

/// A human asking `ant`'s daemon, the conductor of the run, to start, stop or resume one, on [`subject::trail`] with core
/// NATS. The daemon answers with a [`TrailReply`] on [`subject::trailed`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailRequest {
    pub id: Uuid,
    pub ant: UserId,
    /// Who asks: a human, as only human credentials publish to [`subject::trail`].
    pub by: UserId,
    pub sent_at: DateTime<Utc>,
    #[serde(flatten)]
    pub action: TrailAction,
}

/// What a [`TrailRequest`] asks of a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum TrailAction {
    /// Start a run of `trail` with `args`, reporting on `thread`, which the asking human owns: its owner record names
    /// the human for the conductor ([`crate::owner_key`]) before the request goes, so the steps' recipients trust it.
    /// The run takes the request's id, so a start asked again under the same id starts one run.
    Start {
        trail: TrailSource,
        #[serde(default)]
        args: Value,
        thread: ThreadId,
        #[serde(default)]
        limits: TrailLimits,
    },
    /// Stop a run: it sends no step until resumed, and the steps it waits on are withdrawn.
    Stop { run: RunId },
    /// Resume a stopped run, or one failed on a limit given `limits` raising it.
    Resume {
        run: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limits: Option<TrailLimits>,
    },
}

/// The run a schedule's message asks its recipient to start, as the run's conductor ([`Envelope::trail_start`]): the
/// saved trail `trail` with `args`, under `limits`, which the conductor refuses over [`TrailLimits::MAX`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailStart {
    pub trail: TrailName,
    #[serde(default)]
    pub args: Value,
    #[serde(default)]
    pub limits: TrailLimits,
}

/// The script a run starts: a saved trail's, by name, or one given inline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrailSource {
    Saved(TrailName),
    Script(String),
}

impl TrailRequest {
    pub fn new(ant: UserId, by: UserId, action: TrailAction) -> Self {
        Self { id: Uuid::now_v7(), ant, by, sent_at: Utc::now(), action }
    }

    pub fn subject(&self) -> String {
        subject::trail(&self.ant)
    }

    pub fn reply_subject(&self) -> String {
        subject::trailed(&self.ant)
    }

    /// Check that the request arrived on its own [`TrailRequest::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "trail request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn answer(&self, outcome: TrailOutcome) -> TrailReply {
        TrailReply { request_id: self.id, ant: self.ant.clone(), outcome }
    }
}

/// The answer of the conductor's daemon to a [`TrailRequest`], on [`subject::trailed`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailReply {
    pub request_id: Uuid,
    pub ant: UserId,
    #[serde(flatten)]
    pub outcome: TrailOutcome,
}

impl TrailReply {
    pub fn subject(&self) -> String {
        subject::trailed(&self.ant)
    }

    /// Check that the answer arrived on its own [`TrailReply::subject`], which credentials pin to the ant's own.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "trail answer", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// What came of a [`TrailRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TrailOutcome {
    Started {
        run: RunId,
    },
    /// The run stopped. Its steps to other ants that waited for an answer are for the asking human to withdraw
    /// ([`crate::message_mark_key`]), as an ant marks no other ant's messages; the conductor withdrew its own.
    Stopped {
        run: RunId,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        waiting: Vec<WaitingStep>,
    },
    Resumed {
        run: RunId,
    },
    /// The stop of a run whose script computes: it takes effect once that pass ends, and a [`TrailOutcome::Stopped`]
    /// answers the same request then. A stop asked again of the stopped run lists the steps still to withdraw.
    Stopping {
        run: RunId,
    },
    Refused {
        error: String,
    },
}

/// A step a stopped run waited on: its message, to `to`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingStep {
    pub to: UserId,
    pub message: Uuid,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::p;

    #[test]
    fn a_trail_request_and_its_answer_go_on_the_conductors_subjects() {
        let action = TrailAction::Start {
            trail: TrailSource::Saved("release".parse().unwrap()),
            args: json!({"version": "1.2"}),
            thread: ThreadId::generate(),
            limits: TrailLimits::DEFAULT,
        };
        let request = TrailRequest::new(p("planner"), p("alice"), action);
        assert_eq!(
            (request.subject(), request.reply_subject()),
            ("ants.trail.planner".into(), "ants.trailed.planner".into())
        );
        assert!(request.check_subject("ants.trail.dev-a").is_err());
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!((json["action"].as_str(), json["trail"]["saved"].as_str()), (Some("start"), Some("release")));
        let back: TrailRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, request);
        let stopped = request.answer(TrailOutcome::Stopped {
            run: RunId::generate(),
            waiting: vec![WaitingStep { to: p("dev-a"), message: Uuid::now_v7() }],
        });
        assert_eq!(stopped.check_subject("ants.trailed.planner"), Ok(()));
        let json = serde_json::to_value(&stopped).unwrap();
        assert_eq!(json["outcome"], "stopped");
        assert_eq!(serde_json::from_value::<TrailReply>(json).unwrap(), stopped);
    }
}
