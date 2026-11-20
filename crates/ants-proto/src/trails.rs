//! Trails: scripts an ant, the conductor, runs to conduct other ants and humans, one step at a time. A step is a
//! message carrying its [`TrailStep`]; a saved trail is a [`SavedTrail`] in [`TRAILS_BUCKET`]; a run is kept in
//! [`TRAIL_RUNS_BUCKET`] under its conductor's keys: its [`TrailRun`], its [`TrailSession`], one [`JournalEntry`] per
//! answered call, the conductor's [`TrailRunIndex`] and a [`TrailStepThread`] per step's thread.
//!
//! A step goes on a thread of its own, which the conductor's [`TrailSession`] maps to the call
//! ([`PendingCall::thread`]). The reply resolving the call carries nothing of the step: the first message on that
//! thread from the step's recipient ([`PendingCall::to`]), or from the assistant of a human recipient answering for it,
//! to the conductor that carries no [`TrailStep`], no notice, no escalation and no run to rate answers it
//! ([`PendingCall::answered_by`]). Only the conductor's own records say which threads are steps, so no other user can
//! answer a call by naming it. A step the conductor sends itself tells its reply apart the same way: the step carries
//! its [`TrailStep`], the reply none, and the conductor's invitation to rate a run started on the step's thread names
//! the run ([`Envelope::rating_of`]).

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::provider::Origin;
use crate::questions::breaks_line;
#[cfg(doc)]
use crate::trail_thread_step_key;
use crate::{Envelope, Error, MAX_TITLE, RunId, ThreadId, TrailName, UserId, subject};
#[cfg(doc)]
use crate::{
    NoticeKind, TRAIL_RUNS_BUCKET, TRAILS_BUCKET, trail_call_key, trail_run_key, trail_runs_index_key,
    trail_session_key, trail_step_key,
};

mod ratings;
mod saving;
mod tips;

pub use ratings::{
    Counts, FIND_TRAILS_LIMIT, Finder, Found, LATEST_RATINGS, MAX_FIND_TRAILS, MAX_RATING_TASK, MAX_RATING_WHY, Query,
    RATING_FULL_WEIGHT, RATING_MATCH_WORDS, RatingOutcome, RatingSummary, TrailRating, serves, stem,
};
pub use saving::{SAVE_TRAIL_REPEAT, SAVE_TRAIL_WAIT, SaveTrailOutcome, SaveTrailReply, SaveTrailRequest, TrailChange};
pub use tips::{
    DEFAULT_TIPS_FOUND, MAX_LIVE_TIPS, MAX_TIP_REASON, MAX_TIP_TEXT, MAX_TIPS_BLOCK, MAX_TIPS_CARRIED, MAX_TIPS_FOUND,
    Ranked, Retirement, Similarity, TIP_HALF_LIFE, TIP_RELEVANCE, TIP_RELEVANT_WORDS, TIP_SIMILARITY, TIP_UNUSED,
    Tally, TipBoard, TipRetired, TipStatus, TipVerdict, TipVote, TrailTip, check_tip_key, script_sha256, search_words,
    tip_words,
};

/// Most bytes a trail's script has: a saved trail's and a run's.
pub const MAX_TRAIL_SCRIPT: usize = 256 * 1024;
/// Most bytes a run's arguments have, as JSON.
pub const MAX_TRAIL_ARGS: usize = 64 * 1024;
/// Most characters a saved trail's description has.
pub const MAX_TRAIL_DESCRIPTION: usize = 1000;
/// Most characters of a step's label ([`TrailStep::label`]), an ellipsis marking a cut aside.
pub const MAX_STEP_LABEL: usize = 200;
/// Most phases a saved trail names.
pub const MAX_TRAIL_PHASES: usize = 32;
/// Most waypoints a saved trail names.
pub const MAX_TRAIL_WAYPOINTS: usize = 32;
/// How long [`TRAIL_RUNS_BUCKET`] keeps a finished run, at least.
pub const TRAIL_RUN_RETENTION: Duration = Duration::from_hours(30 * 24);
/// How far ahead of a reader's clock a tip, a vote or a rating may be dated and still count: one dated further ahead
/// counts for nothing until then, so no date set ahead keeps a record from ageing.
pub const TRAIL_CLOCK_SKEW: chrono::TimeDelta = chrono::TimeDelta::minutes(1);

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

/// Who saved a trail: a human, with `ants trail save`, or an ant, with its `save_trail` tool.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Saver {
    Human { user: UserId },
    Ant { user: UserId },
}

impl Saver {
    pub fn user(&self) -> &UserId {
        match self {
            Self::Human { user } | Self::Ant { user } => user,
        }
    }
}

impl std::fmt::Display for Saver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Human { user } => write!(f, "{user}"),
            Self::Ant { user } => write!(f, "ant {user}"),
        }
    }
}

/// A saved trail in [`TRAILS_BUCKET`], under the key of its name. Humans put and delete any. An ant saves new ones and
/// replaces or removes only those it saved ([`Saver`]), through `ants signer` ([`SaveTrailRequest`]), as ant
/// credentials write none. Every ant and human reads them. Readers should treat unknown fields as additions, not
/// errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedTrail {
    pub name: TrailName,
    #[serde(default)]
    pub description: String,
    /// The phases the script reports, in order, for showing a run's progress.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phases: Vec<String>,
    /// The top-level async defs a run enters the trail at, the first its start; none for a trail entered only at its
    /// start.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waypoints: Vec<String>,
    /// The script's source text.
    pub script: String,
    /// The human or ant who saved it.
    pub saved_by: Saver,
    /// The run whose inline script an ant saved, conducted by that ant, so its record is `<saver>.<run>` in
    /// [`TRAIL_RUNS_BUCKET`]; none for a script saved as given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saved_from: Option<RunId>,
    pub saved_at: DateTime<Utc>,
}

impl SavedTrail {
    pub fn key(&self) -> &str {
        self.name.as_str()
    }

    /// Check what a reader takes from the bucket: the trail is stored under its own name and within the bounds of
    /// [`SavedTrail::check`].
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key != self.key() {
            return Err(Error::ForeignTrail { name: self.name.clone(), at: key.to_owned() });
        }
        self.check()
    }

    /// Check the trail's bounds: a name not reading as a run's id, a script of 1 to [`MAX_TRAIL_SCRIPT`] bytes, a
    /// description of one line of at most [`MAX_TRAIL_DESCRIPTION`] characters, at most [`MAX_TRAIL_PHASES`] phases,
    /// each one line of 1 to [`MAX_TITLE`] characters (no control characters in either), and at most
    /// [`MAX_TRAIL_WAYPOINTS`] waypoints, each a name of 1 to [`MAX_TITLE`] letters, digits and `_`.
    pub fn check(&self) -> Result<(), Error> {
        let invalid = |why: String| Err(Error::InvalidTrail { name: self.name.clone(), why });
        if Uuid::try_parse(self.name.as_str()).is_ok() {
            return invalid("a trail's name reads as a run's id, which `ants trail show` takes for a run".to_owned());
        }
        if self.script.trim().is_empty() || self.script.len() > MAX_TRAIL_SCRIPT {
            return invalid(format!("a script has 1-{MAX_TRAIL_SCRIPT} bytes"));
        }
        if self.description.chars().count() > MAX_TRAIL_DESCRIPTION || self.description.chars().any(breaks_line) {
            return invalid(format!("a description is one line of at most {MAX_TRAIL_DESCRIPTION} characters"));
        }
        if self.phases.len() > MAX_TRAIL_PHASES {
            return invalid(format!("a trail has at most {MAX_TRAIL_PHASES} phases"));
        }
        let bad_phase = |p: &str| p.trim().is_empty() || p.chars().any(breaks_line) || p.chars().count() > MAX_TITLE;
        if self.phases.iter().any(|p| bad_phase(p)) {
            return invalid(format!("a phase is one line of 1-{MAX_TITLE} characters"));
        }
        if self.waypoints.len() > MAX_TRAIL_WAYPOINTS {
            return invalid(format!("a trail has at most {MAX_TRAIL_WAYPOINTS} waypoints"));
        }
        let bad_waypoint =
            |w: &str| w.is_empty() || w.len() > MAX_TITLE || !w.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if self.waypoints.iter().any(|w| bad_waypoint(w)) {
            return invalid(format!("a waypoint is a name of 1-{MAX_TITLE} letters, digits and '_'"));
        }
        Ok(())
    }
}

/// What a run may spend, as its start asks and its [`TrailRun`] keeps. Missing fields take their defaults
/// ([`TrailLimits::DEFAULT`]); none may exceed [`TrailLimits::MAX`] ([`TrailLimits::check`]). Each step also counts
/// against its ant's own spending caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TrailLimits {
    /// Steps the run sends in all, re-asks for an answer matching a step's schema not counted.
    pub steps: u32,
    /// Steps waiting for an answer at once; the script's further calls wait their turn ([`TrailSession::queued`]).
    pub concurrency: u32,
    /// Turns the run's steps spend in all, as ant-to-ant messages spend them ([`Envelope::turn_budget`]).
    pub turns: u32,
    /// Seconds from the run's start to its end, stopped time included: a run still unfinished then fails.
    pub wall_secs: u64,
    /// Milliseconds the script computes for at most between two suspensions.
    pub compute_ms: u64,
    /// Most bytes of the dumped session ([`TrailSession::session`]).
    pub session_bytes: u32,
}

impl TrailLimits {
    /// What a run may spend when its start asks for no more. A step's answer spends a turn, a re-ask for an answer
    /// matching its schema another, and a step's ant that consults others on the step's thread more: four turns per
    /// step of the default steps.
    pub const DEFAULT: Self = Self {
        steps: 50,
        concurrency: 4,
        turns: 200,
        wall_secs: 24 * 3600,
        compute_ms: 10_000,
        session_bytes: 768 * 1024,
    };
    /// The most any run may spend. A run waiting for humans may take days, so a week of wall time, which bounds how
    /// long [`TRAIL_RUNS_BUCKET`] keeps a run's entries ([`TRAIL_RUN_RETENTION`]). The dumped session must leave room
    /// in its record, under NATS's 1 MiB payload, for the calls it waits on.
    pub const MAX: Self = Self {
        steps: 500,
        concurrency: 16,
        turns: 2000,
        wall_secs: 7 * 24 * 3600,
        compute_ms: 10_000,
        session_bytes: 768 * 1024,
    };

    pub fn wall_time(&self) -> Duration {
        Duration::from_secs(self.wall_secs)
    }

    pub fn compute(&self) -> Duration {
        Duration::from_millis(self.compute_ms)
    }

    /// Check each limit is at least 1 and at most its [`TrailLimits::MAX`].
    pub fn check(&self) -> Result<(), Error> {
        let max = Self::MAX;
        let limits = [
            ("steps", u64::from(self.steps), u64::from(max.steps)),
            ("concurrency", u64::from(self.concurrency), u64::from(max.concurrency)),
            ("turns", u64::from(self.turns), u64::from(max.turns)),
            ("wall_secs", self.wall_secs, max.wall_secs),
            ("compute_ms", self.compute_ms, max.compute_ms),
            ("session_bytes", u64::from(self.session_bytes), u64::from(max.session_bytes)),
        ];
        match limits.into_iter().find(|&(_, value, max)| value == 0 || value > max) {
            Some((limit, value, max)) => Err(Error::TrailLimit { limit, value, max }),
            None => Ok(()),
        }
    }
}

impl Default for TrailLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The limits a run's start sets, in a line: `50 steps, 4 at once, 200 turns, 1d of wall time`.
impl std::fmt::Display for TrailLimits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let wall = crate::duration::format(self.wall_time());
        write!(f, "{} steps, {} at once, {} turns, {wall} of wall time", self.steps, self.concurrency, self.turns)
    }
}

/// A run's header in [`TRAIL_RUNS_BUCKET`], under [`trail_run_key`]: what it runs, for whom and where, and how far it
/// got. Its conductor's daemon puts it at the start, as a leg starts and whenever its state or counters change; humans
/// read it. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailRun {
    pub run: RunId,
    /// The ant running the script, under whose keys the run is kept.
    pub conductor: UserId,
    /// The legs the run went, in order: the first the trail it started, each next one a goto() of the one before led
    /// to, the last the one it is on.
    pub route: Vec<TrailLeg>,
    /// The human the run answers to, the owner of the thread that started it: its steps' threads carry it on. None when
    /// that thread had none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The thread the run reports on, where its start was asked and its result goes; a child run's, a thread of its
    /// own spun off its parent's.
    pub thread: ThreadId,
    /// The run whose trail() call the run answers, as a child run of it: none for a run started otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<TrailParent>,
    /// The step whose recipient, the run's conductor, started the run with `run_trail` while answering it: the run
    /// spends from the step's run. None for a run started otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spends_from: Option<SpendsFrom>,
    /// The grants of its steps still held, or whose runs told some spending ([`StepGrant`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<StepGrant>,
    /// Where that thread came from, as the conductor's daemon settled it: a run from a webhook's thread is untrusted.
    pub origin: Origin,
    pub limits: TrailLimits,
    #[serde(flatten)]
    pub state: RunState,
    /// The phase the script reported last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Steps sent so far, re-asks not counted, its child runs' included.
    #[serde(default)]
    pub steps: u32,
    /// Turns the steps spent so far, its child runs' included.
    #[serde(default)]
    pub turns: u32,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
}

impl TrailRun {
    pub fn key(&self) -> String {
        crate::trail_run_key(&self.conductor, &self.run)
    }

    /// The leg the run is on: the last of its route.
    pub fn leg(&self) -> Option<&TrailLeg> {
        self.route.last()
    }

    /// The saved trail the run is on; none for a script given inline.
    pub fn trail(&self) -> Option<&TrailName> {
        self.leg().and_then(|leg| leg.trail.as_ref())
    }

    /// The trail the run is on when the colony saves it, which its steps' tips are on: none for an inline script, nor
    /// for a trail only the conductor's machine has ([`TrailLeg::local`]).
    pub fn tip_trail(&self) -> Option<&TrailName> {
        self.leg().filter(|leg| !leg.local).and_then(|leg| leg.trail.as_ref())
    }

    /// The saved trail the run started; none for a script given inline.
    pub fn first_trail(&self) -> Option<&TrailName> {
        self.route.first().and_then(|leg| leg.trail.as_ref())
    }

    /// The run's route in a line: each leg's [`TrailLeg::entry`], in order, joined by ` -> `, as in
    /// `triage -> fix@patch -> triage@report`.
    pub fn route_text(&self) -> String {
        self.route.iter().map(TrailLeg::entry).collect::<Vec<_>>().join(" -> ")
    }

    /// Whether the run is over: done or failed. A stopped run may still resume within its wall time.
    pub fn finished(&self) -> bool {
        self.state.finished()
    }
}

/// A leg of a run ([`TrailRun::route`]): its trail, entered at a waypoint with an input, its script pinned as the leg
/// started, so a saved trail changed or removed since changes no leg already started. A run enters each (trail,
/// waypoint) once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailLeg {
    /// The saved trail; none for a script given inline, which only a run's first leg is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trail: Option<TrailName>,
    /// Whether that trail is only the conductor's machine's, from its `~/.ants/trails`, the colony saving none of its
    /// name: its steps take no tips ([`TrailRun::tip_trail`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub local: bool,
    /// The waypoint the leg entered the trail at; none for a trail without waypoints, entered at its start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    pub script: String,
    /// The leg's input, JSON of at most [`MAX_TRAIL_ARGS`] bytes: the run's arguments on its first leg, the state of
    /// the goto() that led to it on the others.
    pub args: Value,
    /// The id of the leg's first call: a leg's calls are numbered past those of the legs before, so a call id is
    /// unique within the run and keys one journal entry ([`trail_call_key`]).
    #[serde(default)]
    pub first_call: u32,
    pub started_at: DateTime<Utc>,
    /// Steps sent during the leg, re-asks not counted, the steps of child runs it called included.
    #[serde(default)]
    pub steps: u32,
}

impl TrailLeg {
    /// Where the leg entered, as a route names it ([`leg_entry`]).
    pub fn entry(&self) -> String {
        leg_entry(self.trail.as_ref(), self.at.as_deref())
    }
}

/// Where a leg entered, as a route names it: its trail, `inline` for a script given inline, and `@` and the waypoint
/// when it entered at one: `fix@patch`.
pub fn leg_entry(trail: Option<&TrailName>, at: Option<&str>) -> String {
    let trail = trail.map_or("inline", TrailName::as_str);
    match at {
        Some(at) => format!("{trail}@{at}"),
        None => trail.to_owned(),
    }
}

/// The run a child run answers a trail() call of ([`TrailRun::parent`]). A child run has its parent's conductor, owner
/// and origin; its steps and turns count against its root run's limits, and it ends within its root's wall time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailParent {
    pub run: RunId,
    /// The parent's call the child's result answers.
    pub call: u32,
    /// The thread the child's thread was spun off, its root run's: its steps' threads link to it for the owner.
    pub origin_thread: ThreadId,
}

/// How far a run got ([`TrailRun::state`]), with its result or error once over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunState {
    /// The script computes, or waits for steps an ant answers.
    Running,
    /// The script waits only for steps humans answer.
    Waiting,
    /// A human, or its conductor, stopped it: it sends no step until a human resumes it, while its wall time runs on.
    Stopped { by: UserId },
    /// The script returned `result`.
    Done { result: Value },
    /// The script raised, or the run broke a limit or could not go on.
    Failed {
        error: String,
        /// The limit the run broke, failing on it: a run failed on one resumes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<RunLimit>,
    },
}

impl RunState {
    pub fn finished(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Failed { .. })
    }

    /// The limit a failed run broke.
    pub fn failed_on(&self) -> Option<RunLimit> {
        match self {
            Self::Failed { limit, .. } => *limit,
            _ => None,
        }
    }
}

/// Who answers a call of the script: an ant, by a step sent to it, a human, by a question on a step, or a child run of
/// a saved trail ([`TrailParent`]); or, for a goto(), the conductor, only with an error when it refuses to start the
/// leg the call asks for ([`TrailLeg`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrailFunction {
    /// `agent(prompt, ant=, capability=, schema=, label=, tips=)`: args `{"prompt", "ant"?, "capability"?, "schema"?,
    /// "label"?, "tips"?}`, the keyword arguments given as None left out.
    Agent,
    /// `human(questions)`: args `{"questions"}`.
    Human,
    /// `trail(name, args=None)`: args `{"name", "args"?}`, answered by a run of the trail saved as `name`.
    Trail,
    /// `goto(trail=None, at=None, state=None)`: args `{"trail"?, "at"?, "state"?}`. No step: the conductor ends the
    /// run's leg there and goes on in a run of the next leg, or answers the call with an error naming why not.
    Goto,
}

impl TrailFunction {
    /// The function's name in the script.
    pub fn name(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Human => "human",
            Self::Trail => "trail",
            Self::Goto => "goto",
        }
    }

    /// Whether a call of the function is a step, counted against the step and concurrency limits.
    pub fn is_step(self) -> bool {
        self != Self::Goto
    }
}

/// A run's suspended session in [`TRAIL_RUNS_BUCKET`], under [`trail_session_key`]: the interpreter's dumped state and
/// the calls it waits on. The conductor's daemon puts it after each suspension, before acking the answer that resumed
/// it, and only it reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailSession {
    /// The dumped session, opaque JSON of at most the run's [`TrailLimits::session_bytes`].
    pub session: String,
    /// The calls sent as steps, waiting for their answers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<PendingCall>,
    /// The calls made beyond the run's concurrency, in order, waiting to be sent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued: Vec<QueuedCall>,
    /// The calls whose steps went out before the run stopped or failed on a limit, to go out anew since its resume:
    /// counted against its steps once already, they count no step again. Each leaves the list once its new step goes
    /// out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub anew: Vec<u32>,
}

impl TrailSession {
    /// The pending call `message`, by `answerer`, answers, if any ([`PendingCall::answered_by`]).
    pub fn answered_by(&self, message: &Envelope, answerer: &UserId) -> Option<&PendingCall> {
        self.pending.iter().find(|call| call.answered_by(message, answerer))
    }
}

/// A call of the script sent as a step, waiting for its answer ([`TrailSession::pending`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCall {
    pub call: u32,
    pub function: TrailFunction,
    pub args: Value,
    /// The ant, or human, the step went to; for a trail() call, the conductor.
    pub to: UserId,
    /// The step's own thread; for a trail() call, the child run's thread.
    pub thread: ThreadId,
    /// The step's last message on that thread: the step, or its last re-ask; for a trail() call, the child run's id
    /// ([`PendingCall::child`]).
    pub message: Uuid,
    pub sent_at: DateTime<Utc>,
    /// Whether that message is published: the conductor's daemon puts the call before publishing its message, under the
    /// message's id, which the stream deduplicates, and marks it sent after, so its restarted daemon publishes again
    /// only what it never did. A stopped run's steps keep it; on resuming they go out anew, on new threads.
    #[serde(default)]
    pub sent: bool,
    /// The turns the step's thread had left as its last message on it went: what the step spent shows in the answer's
    /// [`Envelope::turn_budget`].
    #[serde(default)]
    pub turn_budget: u32,
    /// The re-asks for an answer matching the step's schema so far.
    #[serde(default)]
    pub retries: u32,
}

impl PendingCall {
    /// The child run answering the call, a trail() call.
    pub fn child(&self) -> Option<RunId> {
        (self.function == TrailFunction::Trail).then(|| self.message.to_string().parse().ok()).flatten()
    }

    /// Whether `message`, which passed [`Envelope::check_subject`], answers the call: `answerer` is the step's
    /// recipient, the message is on the step's thread, and it is neither a step, a notice ([`NoticeKind`]), an
    /// escalation nor an invitation to rate a run ([`Envelope::rating_of`]), as the conductor sends itself on the
    /// thread of a step to itself that a run started there reports on. `answerer` is the message's sender, or the human its [`Envelope::on_behalf_of`] names once the
    /// conductor honours the sender as that human's assistant, as for an answer to ask_human.
    pub fn answered_by(&self, message: &Envelope, answerer: &UserId) -> bool {
        message.thread_id == self.thread
            && *answerer == self.to
            && message.trail_step.is_none()
            && message.notice.is_none()
            && message.escalation.is_none()
            && message.rating_of.is_none()
    }
}

/// A call of the script waiting for room to be sent ([`TrailSession::queued`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedCall {
    pub call: u32,
    pub function: TrailFunction,
    pub args: Value,
}

/// An answered call in [`TRAIL_RUNS_BUCKET`], under [`trail_call_key`], put by the conductor's daemon before it acks
/// the answer: replaying the journal in call order rebuilds a session its dump no longer restores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalEntry {
    pub call: u32,
    pub function: TrailFunction,
    pub args: Value,
    pub answer: CallAnswer,
}

/// What a call returned to the script: `{"ok": <JSON>}`, or `{"error": "<message>"}`, which it raises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallAnswer {
    Ok(Value),
    Error(String),
}

/// The runs of a conductor not yet finished, in [`TRAIL_RUNS_BUCKET`] under [`trail_runs_index_key`], so its restarted
/// daemon finds them by reading its own keys alone. Its daemon adds a run before the run's first step and removes it
/// once finished.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailRunIndex {
    #[serde(default)]
    pub runs: Vec<RunId>,
}

/// A step's thread, as its conductor keeps it in [`TRAIL_RUNS_BUCKET`] under [`trail_step_key`] from before the step is
/// sent, so a message on it after the run ended, or after a restart, is still told to be the step's and reported on the
/// run's thread rather than prompting the conductor; and under [`trail_thread_step_key`], so a human reads the steps
/// of the runs on one thread by its keys alone. A trail() step, whose thread is its child run's, is kept under the
/// latter alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailStepThread {
    pub run: RunId,
    pub call: u32,
    /// The step's recipient ([`PendingCall::to`]).
    pub to: UserId,
    /// The run's thread ([`TrailRun::thread`]).
    pub run_thread: ThreadId,
    /// The step's label, as its `step` status update tells it ([`crate::StatusUpdate::Step`]): an agent() step's
    /// `label`, `trail NAME` for a trail() step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

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

/// The limit a failed run broke ([`RunState::Failed`]): those of [`TrailLimits`], and the memory of its script.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunLimit {
    /// A call past [`TrailLimits::steps`] raised in the script, which then failed.
    Steps,
    /// A step found none of [`TrailLimits::turns`] left to reserve.
    Turns,
    /// [`TrailLimits::wall_secs`] passed.
    Wall,
    /// The script computed past [`TrailLimits::compute_ms`] between two suspensions.
    Compute,
    /// The dumped session outgrew [`TrailLimits::session_bytes`], or its record.
    Session,
    /// The script's heap outgrew the memory a script has.
    Memory,
}

impl std::fmt::Display for RunLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Steps => "steps",
            Self::Turns => "turns",
            Self::Wall => "wall time",
            Self::Compute => "compute",
            Self::Session => "session size",
            Self::Memory => "memory",
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::p;

    fn saved() -> SavedTrail {
        SavedTrail {
            name: "release".parse().unwrap(),
            description: String::new(),
            phases: vec!["build".to_owned()],
            waypoints: Vec::new(),
            script: "agent('dev-a', 'build')".to_owned(),
            saved_by: Saver::Human { user: p("alice") },
            saved_from: None,
            saved_at: Utc::now(),
        }
    }

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

    #[test]
    fn only_the_recipients_reply_on_the_steps_thread_answers_a_call() {
        let step = Envelope::new(p("planner"), p("dev-a"), "build it").as_step(TrailStep {
            run: RunId::generate(),
            call: 1,
            label: None,
            schema: None,
            trail: None,
            script: None,
            tip_key: None,
            bounds: None,
        });
        let call = PendingCall {
            call: 1,
            function: TrailFunction::Agent,
            args: json!(["dev-a", "build it"]),
            to: p("dev-a"),
            thread: step.thread_id.clone(),
            message: step.id,
            sent_at: step.sent_at,
            sent: true,
            turn_budget: step.turn_budget,
            retries: 0,
        };
        let session =
            TrailSession { session: "{}".to_owned(), pending: vec![call], queued: Vec::new(), anew: Vec::new() };
        let reply = step.reply(p("dev-a"), "built", crate::Role::Ant).unwrap();
        assert_eq!(session.answered_by(&reply, &reply.from).map(|c| c.call), Some(1));
        let by_another = step.follow_up(p("dev-b"), p("planner"), "built too");
        let elsewhere = Envelope::new(p("dev-a"), p("planner"), "built");
        let notice = reply.clone().as_notice(crate::NoticeKind::SignedOut);
        let forged = reply.as_step(*step.trail_step.unwrap());
        for other in [by_another, elsewhere, notice, forged] {
            assert!(session.answered_by(&other, &other.from).is_none(), "{other:?}");
        }
    }

    #[test]
    fn a_self_step_is_told_from_its_reply() {
        let step = Envelope::new(p("planner"), p("planner"), "think").as_step(TrailStep {
            run: RunId::generate(),
            call: 2,
            label: None,
            schema: None,
            trail: None,
            script: None,
            tip_key: None,
            bounds: None,
        });
        let call = PendingCall {
            call: 2,
            function: TrailFunction::Agent,
            args: json!(null),
            to: p("planner"),
            thread: step.thread_id.clone(),
            message: step.id,
            sent_at: step.sent_at,
            sent: true,
            turn_budget: step.turn_budget,
            retries: 0,
        };
        assert!(!call.answered_by(&step, &step.from), "the step itself answers nothing");
        let invitation = Envelope {
            rating_of: Some(RunId::generate()),
            ..Envelope::on_thread(step.thread_id.clone(), p("planner"), p("planner"), "rate it")
        };
        assert!(!call.answered_by(&invitation, &invitation.from), "an invitation to rate a run answers nothing");
        let reply = step.reply(p("planner"), "thought", crate::Role::Ant).unwrap();
        assert!(call.answered_by(&reply, &reply.from));
    }

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

    #[test]
    fn a_journal_entry_has_the_shape_replay_reads() {
        let ok = JournalEntry {
            call: 1,
            function: TrailFunction::Human,
            args: json!(["ship?"]),
            answer: CallAnswer::Ok(json!("yes")),
        };
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            json!({"call": 1, "function": "human", "args": ["ship?"], "answer": {"ok": "yes"}})
        );
        let failed = JournalEntry { answer: CallAnswer::Error("no such ant".to_owned()), ..ok };
        assert_eq!(serde_json::to_value(&failed).unwrap()["answer"], json!({"error": "no such ant"}));
    }

    #[test]
    fn a_runs_state_sits_beside_its_other_fields() {
        let state = RunState::Failed { error: "steps limit reached".to_owned(), limit: None };
        let json = json!({"state": "failed", "error": "steps limit reached"});
        assert_eq!(serde_json::to_value(&state).unwrap(), json);
        assert!(state.finished() && !RunState::Stopped { by: p("alice") }.finished());
        let state = RunState::Failed { error: "out of turns".to_owned(), limit: Some(RunLimit::Turns) };
        let json = json!({"state": "failed", "error": "out of turns", "limit": "turns"});
        assert_eq!(serde_json::to_value(&state).unwrap(), json);
        assert_eq!(serde_json::from_value::<RunState>(json).unwrap().failed_on(), Some(RunLimit::Turns));
    }

    #[test]
    fn limits_hold_between_one_and_their_maximum() {
        assert_eq!(TrailLimits::default().check(), Ok(()));
        assert_eq!(TrailLimits::MAX.check(), Ok(()));
        let over = TrailLimits { concurrency: 17, ..TrailLimits::DEFAULT };
        assert_eq!(over.check(), Err(Error::TrailLimit { limit: "concurrency", value: 17, max: 16 }));
        assert!(TrailLimits { steps: 0, ..TrailLimits::DEFAULT }.check().is_err());
        assert!(TrailLimits::MAX.wall_time() >= TrailLimits::DEFAULT.wall_time());
    }

    #[test]
    fn a_saved_trail_is_checked_against_its_key_and_bounds() {
        let trail = saved();
        assert_eq!(trail.check_key("release"), Ok(()));
        assert!(matches!(trail.check_key("other"), Err(Error::ForeignTrail { .. })));
        let bad = [
            SavedTrail { script: " ".to_owned(), ..saved() },
            SavedTrail { script: "x".repeat(MAX_TRAIL_SCRIPT + 1), ..saved() },
            SavedTrail { description: "x".repeat(MAX_TRAIL_DESCRIPTION + 1), ..saved() },
            SavedTrail { description: "a\nb".to_owned(), ..saved() },
            SavedTrail { description: "a\u{1b}[8m".to_owned(), ..saved() },
            SavedTrail { phases: vec!["a\nb".to_owned()], ..saved() },
            SavedTrail { phases: vec!["a\u{1b}]0;title\u{7}".to_owned()], ..saved() },
            SavedTrail { phases: vec!["p".to_owned(); MAX_TRAIL_PHASES + 1], ..saved() },
            SavedTrail { waypoints: vec!["a b".to_owned()], ..saved() },
            SavedTrail { waypoints: vec!["w".to_owned(); MAX_TRAIL_WAYPOINTS + 1], ..saved() },
        ];
        for trail in bad {
            assert!(matches!(trail.check(), Err(Error::InvalidTrail { .. })), "{trail:?}");
        }
    }

    #[test]
    fn a_trail_name_reading_as_a_run_id_fails_the_check() {
        let trail = SavedTrail { name: RunId::generate().as_str().parse().unwrap(), ..saved() };
        let error = trail.check().unwrap_err().to_string();
        assert!(error.contains("reads as a run's id"), "{error}");
        let saver = Saver::Ant { user: p("dev-a") };
        assert_eq!((saver.to_string(), saver.user()), ("ant dev-a".to_owned(), &p("dev-a")));
        assert_eq!(Saver::Human { user: p("alice") }.to_string(), "alice");
    }
}
