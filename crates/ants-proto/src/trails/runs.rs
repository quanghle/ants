//! A run's records in [`TRAIL_RUNS_BUCKET`]: its header, limits and state, its session
//! and journal, its conductor's index of runs, and its steps' threads.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::{SpendsFrom, StepGrant};
use crate::provider::Origin;
#[cfg(doc)]
use crate::{
    Envelope, MAX_TRAIL_ARGS, TRAIL_RUN_RETENTION, TRAIL_RUNS_BUCKET, trail_call_key, trail_run_key,
    trail_runs_index_key, trail_session_key, trail_step_key, trail_thread_step_key,
};
use crate::{Error, RunId, ThreadId, TrailName, UserId};

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

    /// The state in a word: `running`, `waiting`, `stopped`, `done` or `failed`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Stopped { .. } => "stopped",
            Self::Done { .. } => "done",
            Self::Failed { .. } => "failed",
        }
    }

    /// Whether a resume continues a run in this state under `limits`, raised to `raised` when given: a stopped run, or
    /// one failed on a limit, but on its memory limit, which no resume raises, or on its session size limit unless
    /// `raised` raises it. Either would restore the same session, take the same answers and fail the same way.
    pub fn resumable(&self, limits: &TrailLimits, raised: Option<&TrailLimits>) -> Result<(), Unresumable<'_>> {
        match self {
            Self::Failed { limit: Some(RunLimit::Memory), .. } => Err(Unresumable::Broke(RunLimit::Memory)),
            Self::Failed { limit: Some(RunLimit::Session), .. }
                if !raised.is_some_and(|raised| raised.session_bytes > limits.session_bytes) =>
            {
                Err(Unresumable::Broke(RunLimit::Session))
            }
            Self::Stopped { .. } | Self::Failed { limit: Some(_), .. } => Ok(()),
            Self::Failed { error, limit: None } => Err(Unresumable::Failed(error)),
            Self::Done { .. } => Err(Unresumable::Done),
            Self::Running | Self::Waiting => Err(Unresumable::NotStopped),
        }
    }
}

/// Why no resume continues a run ([`RunState::resumable`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unresumable<'a> {
    /// The run is done.
    Done,
    /// The run failed, not on a limit, with this error.
    Failed(&'a str),
    /// The run failed on this limit, which the resume does not raise.
    Broke(RunLimit),
    /// The run is running or waiting.
    NotStopped,
}

impl Unresumable<'_> {
    /// Whether the run ended with nothing to resume: done, or failed on no limit.
    pub fn ended(&self) -> bool {
        matches!(self, Self::Done | Self::Failed(_))
    }
}

/// Who answers a call of the script: an ant, by a step sent to it, a human, by a question on a step, or a child run of
/// a saved trail ([`TrailParent`]); or, for a goto(), the conductor, only with an error when it refuses to start the
/// leg the call asks for ([`TrailLeg`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrailFunction {
    /// `agent(prompt, ant=, capability=, team=, schema=, label=, tips=)`: args `{"prompt", "ant"?, "capability"?,
    /// "team"?, "schema"?,
    /// "label"?, "tips"?}`, the keyword arguments given as None left out.
    Agent,
    /// `human(questions)`: args `{"questions"}`.
    Human,
    /// `trail(name, args=None)`: args `{"name", "args"?}`, answered by a run of the trail saved as `name`.
    Trail,
    /// `goto(trail=None, at=None, state=None)`: args `{"trail"?, "at"?, "state"?}`. No step: the conductor ends the
    /// run's leg there and goes on in a run of the next leg, or answers the call with an error naming why not.
    Goto,
    /// `command(command)` of a watcher: args `{"command"}`, answered with `{"status", "stdout"}` by the daemon running
    /// it as a command watcher's check runs, under the ant's tool allow rules.
    Command,
    /// `read_file(path)` of a watcher: args `{"path"}`, answered with the text of the file at `path` within the ant's
    /// working directory, None when there is none.
    ReadFile,
    /// `bd(*args)` of a watcher: args `{"args"}`, answered with what `bd <args> --json` prints, read: a read-only
    /// subcommand of bd alone.
    Bd,
    /// `thread(id=None)` of a watcher: args `{"id"?}`, answered with what the ant's sense keeps of the thread, the
    /// watcher's own without `id`: its resume register and its record.
    Thread,
}

impl TrailFunction {
    /// The function's name in the script.
    pub fn name(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Human => "human",
            Self::Trail => "trail",
            Self::Goto => "goto",
            Self::Command => "command",
            Self::ReadFile => "read_file",
            Self::Bd => "bd",
            Self::Thread => "thread",
        }
    }

    /// Whether a call of the function is a step, counted against the step and concurrency limits.
    pub fn is_step(self) -> bool {
        matches!(self, Self::Agent | Self::Human | Self::Trail)
    }

    /// Whether only a watcher's script may call the function: a read of the ant's state, which no run of a trail
    /// makes; a watcher's script may call no other.
    pub fn watches(self) -> bool {
        matches!(self, Self::Command | Self::ReadFile | Self::Bd | Self::Thread)
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
    /// Whether the step's recipient asked a human with ask_human ([`crate::NoticeKind::Asked`]): the step waits on the
    /// human, the recipient's answer after the human's answering it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub asked: bool,
}

impl PendingCall {
    /// The child run answering the call, a trail() call.
    pub fn child(&self) -> Option<RunId> {
        (self.function == TrailFunction::Trail).then(|| self.message.to_string().parse().ok()).flatten()
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

impl RunLimit {
    /// The flag of `ants trail resume` raising the limit: its steps, turns and wall time. None raises the compute
    /// limit, run again as it is, nor the memory limit; only a resume's own limits raise the session size limit
    /// ([`RunState::resumable`]).
    pub fn raised_by(self) -> Option<&'static str> {
        match self {
            Self::Steps => Some("--steps"),
            Self::Turns => Some("--turns"),
            Self::Wall => Some("--wall"),
            Self::Compute | Self::Session | Self::Memory => None,
        }
    }
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
}
