use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::ToolCall;
use crate::{Error, Escalation, ThreadId, UserId, Verdict, subject};

/// The most paths a tool call carries, in [`StatusUpdate::Tool`] and in timeline records.
pub const MAX_TOOL_PATHS: usize = 8;

/// Where an ant stands on a thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// A prompt is running.
    Working,
    /// A tool request waits for the human's decision.
    WaitingApproval,
    /// The reply is stored; nothing runs on the thread.
    Idle,
    /// An attempt failed; it may be retried.
    Failed,
    /// The harness is signed out: the thread's messages wait for it to be signed in.
    SignedOut,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Working => "working",
            State::WaitingApproval => "waiting_approval",
            State::Idle => "idle",
            State::Failed => "failed",
            State::SignedOut => "signed_out",
        }
    }
}

/// Who settled a tool request ([`StatusUpdate::Decided`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecidedBy {
    /// A human, with the decision stored on the request's decision subject.
    Human,
    /// An ant of the requester's escalate_to, with `decide_approval`.
    Chain,
    /// Nobody: the request expired unanswered and was rejected.
    Expiry,
}

impl DecidedBy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Chain => "chain",
            Self::Expiry => "expiry",
        }
    }
}

/// One step of a harness's plan, as of its last [`StatusUpdate::Plan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    pub content: String,
    /// ACP plan entry status: pending, in_progress or completed.
    pub status: String,
}

/// What changed on a thread while an ant works on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StatusUpdate {
    State {
        state: State,
    },
    /// More of the reply text, as the harness streams it. Consecutive updates join up to the reply so far.
    Message {
        text: String,
    },
    /// More of the harness's reasoning, where it shares it.
    Thought {
        text: String,
    },
    /// A tool call started, or its title or status changed. Absent fields did not change.
    Tool {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// ACP tool kind, as in [`ToolCall::kind`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_kind: Option<String>,
        /// ACP tool call status: pending, in_progress, completed or failed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        /// The paths of the call's ACP locations, at most [`MAX_TOOL_PATHS`]; absent when there are none or they did
        /// not change.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        paths: Option<Vec<String>>,
    },
    /// The harness's whole plan, replacing the previous one.
    Plan {
        steps: Vec<PlanStep>,
    },
    /// Message `message` waits on the thread behind `ahead` others: the one in flight and those that came before it.
    /// Sent as it arrives and as the queue moves; `ahead` 0 means its turn came.
    Queued {
        message: Uuid,
        ahead: usize,
    },
    /// The tool request `<ant>/<request_id>` waits for a decision, as `ants tools` names it; sent just before the
    /// [`State::WaitingApproval`] state.
    Approval {
        request_id: Uuid,
    },
    /// The ant escalated what it could not settle: the message carrying `escalation` to `to`, an ant of a chain or the
    /// thread's owner (else the default human), is stored. Sent for each such message, reminders included; the
    /// message's text is not part of it.
    Escalated {
        to: UserId,
        escalation: Escalation,
    },
    /// The tool request `<ant>/<request_id>` was settled, `by` a human, an ant of the chain or its expiry (`decider`
    /// names the ant, or the human as its decision names it), and the ant goes on with `verdict`, a verdict the
    /// harness did not offer counting as a rejection. Sent just before the [`State::Working`] state, however the
    /// decision arrived.
    Decided {
        request_id: Uuid,
        verdict: Verdict,
        by: DecidedBy,
        /// The ant of the chain that decided, with [`DecidedBy::Chain`]; with [`DecidedBy::Human`], the human its
        /// [`ApprovalDecision::by`](crate::ApprovalDecision::by) names, as its client claims, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        decider: Option<UserId>,
    },
}

/// Live progress of an ant on a thread, published on [`subject::status`] with core NATS: never stored, so a
/// subscriber sees only what happens while it listens, and an update may be dropped. Readers should treat unknown
/// update types as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEvent {
    pub ant: UserId,
    pub thread_id: ThreadId,
    #[serde(flatten)]
    pub update: StatusUpdate,
    pub at: DateTime<Utc>,
}

impl StatusEvent {
    pub fn new(ant: UserId, thread_id: ThreadId, update: StatusUpdate) -> Self {
        Self { ant, thread_id, update, at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::status(&self.ant, &self.thread_id)
    }

    /// Check that the update arrived on its own [`StatusEvent::subject`]. Credentials pin each ant to its own
    /// status subjects, so an update naming another ant or thread is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser {
            kind: "status update",
            user: self.ant.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EscalationReason, Permissions, Role, p};

    #[test]
    fn status_events_round_trip_flat_and_name_their_subject() {
        let t: ThreadId = "t1".parse().unwrap();
        let state = StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::State { state: State::WaitingApproval });
        let json = serde_json::to_value(&state).unwrap();
        assert_eq!((json["type"].as_str(), json["state"].as_str()), (Some("state"), Some("waiting_approval")));
        assert_eq!(json["thread_id"], "t1");
        let tool = StatusEvent::new(
            p("dev-a"),
            t.clone(),
            StatusUpdate::Tool {
                id: "c1".into(),
                title: None,
                tool_kind: None,
                status: Some("completed".into()),
                paths: None,
            },
        );
        let json = serde_json::to_string(&tool).unwrap();
        assert!(json.contains(r#""type":"tool""#) && !json.contains("title") && !json.contains("paths"), "{json}");
        let queued = StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Queued { message: Uuid::nil(), ahead: 2 });
        let json = serde_json::to_value(&queued).unwrap();
        assert_eq!((json["type"].as_str(), json["ahead"].as_u64()), (Some("queued"), Some(2)));
        let approval = StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Approval { request_id: Uuid::nil() });
        let escalation = Escalation::new(p("dev-a"), t.clone(), EscalationReason::Asked);
        let escalated =
            StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Escalated { to: UserId::human(), escalation });
        let json = serde_json::to_value(&escalated).unwrap();
        assert_eq!((json["type"].as_str(), json["to"].as_str()), (Some("escalated"), Some("human")));
        assert_eq!(
            (json["escalation"]["stuck"].as_str(), json["escalation"]["reason"].as_str()),
            (Some("dev-a"), Some("asked"))
        );
        let decided = StatusEvent::new(
            p("dev-a"),
            t.clone(),
            StatusUpdate::Decided {
                request_id: Uuid::nil(),
                verdict: Verdict::AllowOnce,
                by: DecidedBy::Chain,
                decider: Some(p("lead")),
            },
        );
        let json = serde_json::to_value(&decided).unwrap();
        assert_eq!(
            (json["type"].as_str(), json["verdict"].as_str(), json["by"].as_str(), json["decider"].as_str()),
            (Some("decided"), Some("allow_once"), Some("chain"), Some("lead"))
        );
        for event in [state, tool, queued, approval, escalated, decided] {
            let json = serde_json::to_string(&event).unwrap();
            assert_eq!(serde_json::from_str::<StatusEvent>(&json).unwrap(), event);
        }

        let chunk = StatusEvent::new(p("dev-a"), t, StatusUpdate::Message { text: "hi".into() });
        assert_eq!(chunk.subject(), "ants.status.dev-a.t1");
        assert_eq!(chunk.check_subject("ants.status.dev-a.t1"), Ok(()));
        assert!(chunk.check_subject("ants.status.dev-a.t2").is_err(), "another thread");
        let forged = chunk.check_subject("ants.status.dev-b.t1").unwrap_err();
        assert_eq!(forged.to_string(), "status update of dev-a found at ants.status.dev-b.t1 (thread t1)");
        assert!(Permissions::for_user(&p("dev-a"), Role::Ant).publish.contains(&subject::status_of(&p("dev-a"))));
    }
}
