//! A trail's steps: the marks a step's message carries, and the grants a step gives a run started answering it.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(doc)]
use crate::{Envelope, NoticeKind, TRAIL_RUNS_BUCKET, TrailRun, TrailTip, script_sha256};
use crate::{RunId, ThreadId, TrailName, UserId};

/// What makes a message a trail's step: the run and call it serves ([`Envelope::trail_step`]). Its recipient answers
/// it with an ordinary reply on the step's thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailStep {
    pub run: RunId,
    /// The script's call the step answers, unique within the run.
    pub call: u32,
    /// What the script names the step, for showing it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The JSON Schema the answer is to match: the recipient answers with JSON matching it, and the conductor asks
    /// again, with the step on the same thread, for an answer that does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    /// The saved trail whose script made the call, which tips left answering the step go on; none for an inline script
    /// or a trail the colony does not save ([`TrailRun::tip_trail`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trail: Option<TrailName>,
    /// The [`script_sha256`] of that trail's script, as the run's leg ran it; none without a trail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    /// The call's tip key (`agent(..., tips=KEY)`), naming the tips ([`TrailTip::tip_key`]) the step carries first and
    /// a tip left answering it goes under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_key: Option<String>,
    /// The step's grant: what the runs its recipient starts with `run_trail` while answering it may spend together;
    /// none from a conductor that tells nothing of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounds: Option<StepBounds>,
}

/// What the runs started with `run_trail` while answering a step may spend together ([`TrailStep::bounds`]): the
/// step's grant, which its conductor took out of what the step's run, through its root's limits, had left as the step
/// went, and holds for the step until it is answered and those runs have ended. The recipient's daemon cuts each such
/// run to what the grant leaves after the runs it already started answering the step: the limits of those still
/// running, the spending of the others. What they spend goes back to the conductor ([`TrailSpent`]), charged to the
/// step's run and its ancestors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepBounds {
    /// Steps granted.
    pub steps: u32,
    /// Turns granted.
    pub turns: u32,
    /// When the step's run fails on its wall time.
    pub until: DateTime<Utc>,
    /// The saved trails on the routes of the step's run, its ancestors and the runs they spend from: a run started
    /// answering the step takes none of them, as no trail calls itself.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub route: Vec<TrailName>,
}

/// What the runs started with `run_trail` while answering a step spent in all so far, and how many of them still run
/// ([`Envelope::trail_spent`]): carried by the recipient's replies on the step's thread and by its
/// [`NoticeKind::RunSpent`] notice there as each such run ends or stops. The step's conductor charges the part it has
/// not charged yet to the step's run and its ancestors ([`StepGrant::spent`]), so a message told twice charges once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TrailSpent {
    pub steps: u32,
    pub turns: u32,
    /// The runs still running or waiting: neither ended nor stopped.
    pub running: u32,
}

/// A step's grant, as its conductor holds it in the header of the step's run ([`TrailRun::grants`]): what the runs
/// started answering the step may spend ([`StepBounds`]), and what they told they spent. It is held, less what was
/// spent, while the run waits on the step or while such runs still run, and given back otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepGrant {
    pub call: u32,
    /// The step's thread: a step sent anew, on resuming its run, goes on a new thread with a new grant.
    pub thread: ThreadId,
    /// Steps granted.
    #[serde(default)]
    pub steps: u32,
    /// Turns granted.
    #[serde(default)]
    pub turns: u32,
    /// What the runs answering the step told they spent, all charged, and how many of them still run.
    #[serde(default)]
    pub spent: TrailSpent,
}

/// The step a run started with `run_trail` while answering it spends from ([`TrailRun::spends_from`]): the run is
/// started, and resumed, within the step's grant ([`StepBounds`]), and what it spends goes back to the step's conductor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendsFrom {
    /// The step's conductor, under whose keys the step's run is kept.
    pub conductor: UserId,
    /// The step's run.
    pub run: RunId,
    /// The step's call.
    pub call: u32,
    /// The step's grant: the run, and its child runs, take none of the trails on its route.
    pub bounds: StepBounds,
}

/// The runs a recipient started with `run_trail` while answering one step, kept by the recipient's daemon in
/// [`TRAIL_RUNS_BUCKET`] under [`crate::trail_answering_key`] of the step's thread, so a restart still tells all they
/// spent. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailAnswering {
    /// The step's conductor.
    pub conductor: UserId,
    /// The step's run.
    pub run: RunId,
    /// The step's call.
    pub call: u32,
    /// The runs started answering it, in order.
    #[serde(default)]
    pub runs: Vec<RunId>,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{Envelope, p};

    #[test]
    fn a_message_is_no_step_unless_the_conductor_makes_it_one() {
        let message = Envelope::new(p("planner"), p("dev-a"), "build it");
        let json = serde_json::to_value(&message).unwrap();
        assert!(json.get("trail_step").is_none(), "a plain message serializes as before: {json}");
        let step = TrailStep {
            run: RunId::generate(),
            call: 1,
            label: None,
            schema: None,
            trail: None,
            script: None,
            tip_key: None,
            bounds: None,
        };
        let sent = message.as_step(step.clone());
        let back: Envelope = serde_json::from_value(serde_json::to_value(&sent).unwrap()).unwrap();
        assert_eq!(back.trail_step.as_deref(), Some(&step));
        let reply = sent.reply(p("dev-a"), "built", crate::Role::Ant).unwrap();
        assert_eq!(reply.trail_step, None);
        assert_eq!(sent.follow_up(p("planner"), p("dev-a"), "again").trail_step, None);
    }
}
