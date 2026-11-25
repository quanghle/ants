//! Live progress of an ant on a thread: the [`StatusEvent`] its daemon publishes on [`crate::subject::status`]
//! (`ants.status.<ant>.<thread>`, core NATS, never stored), which the CLI follows (`ants watch`): its [`State`], text,
//! tool calls, plan, tool requests and escalations ([`StatusUpdate`]); and what observers hear of the bus besides:
//! a trail run's end, an admin order settled and a team changed.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::ToolCall;
use crate::{
    AdminOutcome, Error, Escalation, Machine, OrderState, Questions, RunId, RunLimit, RunState, Team, TeamDirectory,
    ThreadId, TrailName, TrailRun, UserId, Verdict, subject,
};

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
    /// Its name on the wire, as serde writes it.
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
    /// Nobody: the prompt that asked was cancelled while the request waited, and the harness was told so.
    Cancelled,
}

impl DecidedBy {
    /// Its name on the wire, as serde writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Chain => "chain",
            Self::Expiry => "expiry",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Where a trail run's step stands ([`StatusUpdate::Step`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStage {
    /// The step's message is stored: the step started.
    Started,
    /// The step's recipient asked a human with ask_human: the step waits on the human, its recipient's answer after
    /// the human's being its reply.
    Asked,
    /// A reply answered the step's call.
    Answered,
    /// The step's call raised in the script: its replies never matched its schema, or its message was withdrawn.
    Failed,
    /// The run no longer waits on the step: the script gave it up, or the run ended or stopped first.
    Dropped,
}

impl StepStage {
    /// Its name on the wire, as serde writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Asked => "asked",
            Self::Answered => "answered",
            Self::Failed => "failed",
            Self::Dropped => "dropped",
        }
    }
}

/// How a trail run ended ([`StatusUpdate::RunEnded`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEnd {
    /// The script returned.
    Done,
    /// The script raised, or the run broke a limit or could not go on.
    Failed,
    /// A human, or its conductor, stopped it: a human may resume it.
    Stopped,
}

impl RunEnd {
    /// Its name on the wire, as serde writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
}

/// What makes a message the end of a trail run ([`crate::Envelope::trail_end`]): the run, the saved trail it started
/// on, and how it ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailEnd {
    pub run: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trail: Option<TrailName>,
    pub state: RunEnd,
}

impl TrailEnd {
    /// The end of `run`; none while it runs or waits.
    pub fn of(run: &TrailRun) -> Option<Self> {
        let state = match &run.state {
            RunState::Running | RunState::Waiting => return None,
            RunState::Done { .. } => RunEnd::Done,
            RunState::Failed { .. } => RunEnd::Failed,
            RunState::Stopped { .. } => RunEnd::Stopped,
        };
        Some(Self { run: run.run.clone(), trail: run.first_trail().cloned(), state })
    }
}

/// What befell a team ([`StatusUpdate::TeamChanged`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamChange {
    Created,
    /// Its members, lead, owner or description changed.
    Changed,
    Deleted,
}

impl TeamChange {
    /// Its name on the wire, as serde writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Changed => "changed",
            Self::Deleted => "deleted",
        }
    }
}

/// The most characters of a run's error a [`StatusUpdate::RunEnded`] carries.
pub const MAX_RUN_ERROR: usize = 500;

/// One step of a harness's plan, as of its last [`StatusUpdate::Plan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    pub content: String,
    /// ACP plan entry status ([`call_status`](crate::call_status)): pending, in_progress or completed.
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
    /// The ant asked `to`, a human, the questions `question` (ask_human): the message `message` carrying them is
    /// stored, and the answer comes later as a message from `to`. The message's text is not part of it.
    Asked {
        to: UserId,
        message: Uuid,
        question: Questions,
    },
    /// The tool request `<ant>/<request_id>` was settled, `by` a human, an ant of the chain, its expiry or the
    /// cancellation of the prompt that asked (`decider` names the ant, or the human as its decision names it), and the
    /// ant goes on with `verdict`, a verdict the harness did not offer counting as a rejection. Sent just before the
    /// [`State::Working`] state, however the decision arrived; a cancelled one, with the prompt ending, is followed by
    /// none of its own.
    Decided {
        request_id: Uuid,
        verdict: Verdict,
        by: DecidedBy,
        /// The ant of the chain that decided, with [`DecidedBy::Chain`]; with [`DecidedBy::Human`], the human its
        /// [`ApprovalDecision::by`](crate::ApprovalDecision::by) names, as its client claims, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        decider: Option<UserId>,
        /// With [`DecidedBy::Human`], the assistant that made the decision for the human `decider` names, which took
        /// effect as the human's once it confirmed it or a pre-allow rule it signed covered it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        via: Option<UserId>,
    },
    /// A step of the trail run `run`, which the ant conducts and which reports on the thread, started or ended: its
    /// script's call `call`, labelled `label` by the script, to `to` on the step's own thread `step_thread`.
    Step {
        run: RunId,
        call: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        to: UserId,
        step_thread: ThreadId,
        stage: StepStage,
    },
    /// A leg of the trail run `run`, which the ant conducts and which reports on the thread, started: the leg `leg` of
    /// its route, counted from 0, the first at the run's start and each next one a goto() led to, entering the saved
    /// trail `trail` (absent for an inline script) at the waypoint `waypoint` (absent for the trail's start).
    Leg {
        run: RunId,
        leg: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trail: Option<TrailName>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        waypoint: Option<String>,
    },
    /// The trail run `run`, which the ant conducts and which reports on the thread, ended `state`: started on the
    /// saved trail `trail` (absent for an inline script), as a child of the run `parent`, if one, having sent `steps`
    /// steps and spent `turns` turns. A failed run tells its `error`, cut to [`MAX_RUN_ERROR`] characters, and the
    /// `limit` it broke, if one; a stopped run who stopped it (`by`). Sent once, as the run ends or stops.
    RunEnded {
        run: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trail: Option<TrailName>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent: Option<RunId>,
        state: RunEnd,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<RunLimit>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        by: Option<UserId>,
        steps: u32,
        turns: u32,
    },
    /// The admin order `order` to the ant, on the machine `machine`, was settled `state` (applied, or refused for
    /// `reason`) by the daemon of the machine serving the ant: its op's kind `op`, sent by `admin`, for the human
    /// `on_behalf_of` when an assistant sent it. Sent on the thread named by the order's id once its outcome is
    /// stored; nothing of the op's arguments or result.
    OrderSettled {
        order: Uuid,
        machine: Machine,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        op: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        admin: Option<UserId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_behalf_of: Option<UserId>,
        state: OrderState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// The ant, or the human, changed the team `team`, on the thread named by the team: `change` tells whether it
    /// created, changed or deleted it; `entry` is the team as it stands after, absent once deleted, and `parent` the
    /// team holding it, if any.
    TeamChanged {
        team: UserId,
        change: TeamChange,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry: Option<Box<Team>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent: Option<UserId>,
    },
}

impl StatusUpdate {
    /// The end of `run`, done, failed or stopped; none while it runs or waits.
    pub fn run_ended(run: &TrailRun) -> Option<Self> {
        let (state, error, limit, by) = match &run.state {
            RunState::Running | RunState::Waiting => return None,
            RunState::Done { .. } => (RunEnd::Done, None, None, None),
            RunState::Failed { error, limit } => {
                (RunEnd::Failed, Some(error.chars().take(MAX_RUN_ERROR).collect()), *limit, None)
            }
            RunState::Stopped { by } => (RunEnd::Stopped, None, None, Some(by.clone())),
        };
        Some(Self::RunEnded {
            run: run.run.clone(),
            trail: run.route.first().and_then(|leg| leg.trail.clone()),
            parent: run.parent.as_ref().map(|parent| parent.run.clone()),
            state,
            error,
            limit,
            by,
            steps: run.steps,
            turns: run.turns,
        })
    }

    /// The thread an order's [`StatusUpdate::OrderSettled`] goes on, which its id names, and the update telling
    /// `outcome`; none while the order is pending or held.
    pub fn order_settled(outcome: &AdminOutcome) -> Option<(ThreadId, Self)> {
        if !matches!(outcome.state, OrderState::Applied | OrderState::Refused) {
            return None;
        }
        let thread = outcome.id.to_string().parse().expect("a UUID is a thread id");
        let update = Self::OrderSettled {
            order: outcome.id,
            machine: outcome.machine.clone(),
            op: outcome.op.as_ref().map(|op| op.kind().to_owned()),
            admin: outcome.admin.clone(),
            on_behalf_of: outcome.on_behalf_of.clone(),
            state: outcome.state,
            reason: outcome.reason.clone(),
        };
        Some((thread, update))
    }

    /// The thread a [`StatusUpdate::TeamChanged`] of `team` goes on, which the team names, and the update telling
    /// `change`, with the team as `directory`, read after, holds it.
    pub fn team_changed(directory: &TeamDirectory, team: &UserId, change: TeamChange) -> (ThreadId, Self) {
        let thread = team.as_str().parse().expect("a user id is a thread id");
        let entry = directory.team(team).filter(|_| change != TeamChange::Deleted).cloned().map(Box::new);
        let parent = directory.holder(team).map(|holder| holder.team.clone());
        (thread, Self::TeamChanged { team: team.clone(), change, entry, parent })
    }
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
            StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Escalated { to: crate::human(), escalation });
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
                via: None,
            },
        );
        let json = serde_json::to_value(&decided).unwrap();
        assert!(json.get("via").is_none(), "a decision of the decider itself names no assistant");
        assert_eq!(
            (json["type"].as_str(), json["verdict"].as_str(), json["by"].as_str(), json["decider"].as_str()),
            (Some("decided"), Some("allow_once"), Some("chain"), Some("lead"))
        );
        let ended = StatusEvent::new(
            p("dev-a"),
            t.clone(),
            StatusUpdate::RunEnded {
                run: "r1".parse().unwrap(),
                trail: None,
                parent: None,
                state: RunEnd::Failed,
                error: Some("boom".into()),
                limit: None,
                by: None,
                steps: 2,
                turns: 3,
            },
        );
        let json = serde_json::to_value(&ended).unwrap();
        assert_eq!((json["type"].as_str(), json["state"].as_str()), (Some("run_ended"), Some("failed")));
        assert!(json.get("trail").is_none() && json.get("by").is_none(), "{json}");
        for event in [state, tool, queued, approval, escalated, decided, ended] {
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

    #[test]
    fn an_order_settles_on_the_thread_its_id_names_and_a_team_changes_on_its_own() {
        let order = crate::AdminOrder::new(
            p("alice"),
            "m1".parse().unwrap(),
            p("dev-a"),
            crate::AdminOp::Restart { now: false },
            "jwt",
        );
        let held = AdminOutcome::of(&order, OrderState::Held, None, None);
        assert_eq!(StatusUpdate::order_settled(&held), None, "a held order is not settled");
        let refused = AdminOutcome::of(&order, OrderState::Refused, Some("no such ant".into()), None);
        let (thread, update) = StatusUpdate::order_settled(&refused).unwrap();
        assert_eq!(thread.as_str(), order.id.to_string());
        let json = serde_json::to_value(StatusEvent::new(p("dev-a"), thread, update)).unwrap();
        assert_eq!(
            (json["type"].as_str(), json["op"].as_str(), json["state"].as_str(), json["reason"].as_str()),
            (Some("order_settled"), Some("restart"), Some("refused"), Some("no such ant"))
        );

        let mut directory = TeamDirectory::default();
        directory.create(Team::new(p("eng"), p("lead"), p("alice")), None).unwrap();
        directory.create(Team::new(p("backend"), p("dev-a"), p("alice")), Some(&p("eng"))).unwrap();
        let (thread, update) = StatusUpdate::team_changed(&directory, &p("backend"), TeamChange::Created);
        let StatusUpdate::TeamChanged { entry: Some(entry), parent, .. } = &update else { panic!("{update:?}") };
        assert_eq!(
            (thread.as_str(), entry.lead.as_ref(), parent.as_ref()),
            ("backend", Some(&p("dev-a")), Some(&p("eng")))
        );
        directory.delete(&p("backend")).unwrap();
        let (_, update) = StatusUpdate::team_changed(&directory, &p("backend"), TeamChange::Deleted);
        let json = serde_json::to_value(StatusEvent::new(p("alice"), "backend".parse().unwrap(), update)).unwrap();
        assert_eq!((json["type"].as_str(), json["change"].as_str()), (Some("team_changed"), Some("deleted")));
        assert!(json.get("entry").is_none() && json.get("parent").is_none(), "{json}");
    }
}
