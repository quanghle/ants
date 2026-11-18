//! What the runner and an `ants subprocess` worker exchange: one JSON [`Request`] on the worker's stdin, one JSON
//! [`Response`] on its stdout. Also the calls, answers, events and journal entries a [`Run`](crate::Run) hands its host.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};

/// The limits of one run (ants-tybyn C9), checked by [`Limits::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Limits {
    /// Steps (agent and human calls) a run may make; the call past it raises in the script.
    pub steps: u32,
    /// Steps outstanding at once; the rest wait inside the run until one is answered.
    pub concurrency: u32,
    /// Compute time of one resume, in milliseconds.
    pub compute_ms: u64,
    /// Size of the dumped session, in bytes; a larger one fails the run.
    pub dump_bytes: usize,
    /// Memory the script's heap may use, in bytes; past it the script raises MemoryError.
    pub memory_bytes: usize,
    /// Depth of nested Python calls.
    pub recursion: usize,
}

impl Limits {
    pub const DEFAULT_STEPS: u32 = 50;
    pub const MAX_STEPS: u32 = 500;
    pub const DEFAULT_CONCURRENCY: u32 = 4;
    pub const MAX_CONCURRENCY: u32 = 16;
    pub const DEFAULT_COMPUTE_MS: u64 = 10_000;
    pub const MAX_COMPUTE_MS: u64 = 60_000;
    /// A run record, with its dumped session, fits one NATS message (max_payload 1 MiB).
    pub const MAX_DUMP_BYTES: usize = 768 * 1024;
    pub const DEFAULT_MEMORY_BYTES: usize = 64 * 1024 * 1024;
    pub const MAX_MEMORY_BYTES: usize = 512 * 1024 * 1024;
    pub const DEFAULT_RECURSION: usize = 200;
    /// What the worker thread's 16 MiB stack holds with room to spare.
    pub const MAX_RECURSION: usize = 1000;

    /// Refuses a limit of zero or one past its maximum.
    pub fn check(&self) -> Result<(), String> {
        let within = |name: &str, value: u64, max: u64| {
            if value == 0 || value > max {
                Err(format!("the {name} limit must be between 1 and {max}, not {value}"))
            } else {
                Ok(())
            }
        };
        within("steps", self.steps.into(), Self::MAX_STEPS.into())?;
        within("concurrency", self.concurrency.into(), Self::MAX_CONCURRENCY.into())?;
        within("compute (ms)", self.compute_ms, Self::MAX_COMPUTE_MS)?;
        within("dump size (bytes)", self.dump_bytes as u64, Self::MAX_DUMP_BYTES as u64)?;
        within("memory (bytes)", self.memory_bytes as u64, Self::MAX_MEMORY_BYTES as u64)?;
        within("recursion", self.recursion as u64, Self::MAX_RECURSION as u64)
    }

    /// The compute limit of one resume.
    #[must_use]
    pub fn compute(&self) -> Duration {
        Duration::from_millis(self.compute_ms)
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            steps: Self::DEFAULT_STEPS,
            concurrency: Self::DEFAULT_CONCURRENCY,
            compute_ms: Self::DEFAULT_COMPUTE_MS,
            dump_bytes: Self::MAX_DUMP_BYTES,
            memory_bytes: Self::DEFAULT_MEMORY_BYTES,
            recursion: Self::DEFAULT_RECURSION,
        }
    }
}

/// A host function a step calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Function {
    /// `agent(prompt, ant=, capability=, schema=, label=)`: args `{"prompt", "ant"?, "capability"?, "schema"?,
    /// "label"?}`, the keyword arguments given as None left out.
    Agent,
    /// `human(questions)`: args `{"questions"}`.
    Human,
    /// `trail(name, args=None)`: args `{"name", "args"?}`, answered by a run of the trail saved as `name`.
    Trail,
}

impl Function {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Human => "human",
            Self::Trail => "trail",
        }
    }
}

/// A call the script made that the host performs: a step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Call {
    /// Monty's id of the call, unique within the run.
    pub call: u32,
    pub function: Function,
    pub args: Value,
}

/// The host's answer to a call: its result, or an error raised in the script (as RuntimeError) where the call is
/// awaited.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Answer {
    Ok(Value),
    Error(String),
}

/// An answered call, as the journal keeps it: `{"call", "function", "args", "answer": {"ok": …} | {"error": …}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalEntry {
    pub call: u32,
    pub function: Function,
    pub args: Value,
    pub answer: Answer,
}

/// What the script reported without waiting: `phase(title)`, and `log(text)` or `print()` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    Phase(String),
    Log(String),
}

/// The one request an `ants subprocess` worker serves: `{"start": {…}}` or `{"resume": {…}}`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    /// Runs a script from its start, or from the waypoint `entry`.
    Start {
        script: String,
        /// The waypoint to enter: the script's top level runs, then the waypoint is awaited with `args`. None runs
        /// the script as it is, the start of a trail without waypoints.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry: Option<String>,
        args: Value,
        limits: Limits,
        /// The clock the script reads, in Unix seconds, fixed for the run.
        clock: i64,
        /// Seeds `random` and `os.urandom`.
        seed: u64,
    },
    /// Restores a suspended session and resolves its pending calls, one answer after the other in the given order,
    /// each followed by the script running until it waits again.
    Resume {
        snapshot: Box<RawValue>,
        answers: Vec<(u32, Answer)>,
        limits: Limits,
        seed: u64,
        /// Steps the run made so far, against `limits.steps`.
        steps: u32,
    },
}

/// The one response of a worker: `{"suspended": {…}}`, `{"done": {…}}`, `{"failed": {…}}`, `{"refused": {…}}` or
/// `{"unrestorable": {…}}`.
// Externally tagged: a snapshot, a raw JSON value, cannot be read through an internally tagged enum.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    /// Every live coroutine waits on a step.
    Suspended {
        /// The session, MontyRun's serde form (`RunProgress::ResolveFutures`) as JSON.
        snapshot: Box<RawValue>,
        /// The steps called during this request, in call order.
        calls: Vec<Call>,
        /// Every call the session still waits on, also earlier ones.
        pending: Vec<u32>,
        events: Vec<Event>,
    },
    /// The script ended; `result` is its last expression's value.
    Done {
        result: Value,
        /// The steps called during this request, in call order, which a replay checks against the journal.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        calls: Vec<Call>,
        events: Vec<Event>,
    },
    /// The script raised, or exceeded a limit: `error` is the Python traceback.
    Failed {
        error: String,
        /// As for [`Response::Done`].
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        calls: Vec<Call>,
        events: Vec<Event>,
    },
    /// The request could not be served: unreadable, or under limits the worker does not take.
    Refused { error: String },
    /// The snapshot of a resume is not a suspended session this build of Monty restores, as one another Monty
    /// version dumped: the session can be rebuilt only by replaying the run's journal.
    Unrestorable { error: String },
}

impl Response {
    /// The steps called during the request.
    #[must_use]
    pub fn calls(&self) -> &[Call] {
        match self {
            Self::Suspended { calls, .. } | Self::Done { calls, .. } | Self::Failed { calls, .. } => calls,
            Self::Refused { .. } | Self::Unrestorable { .. } => &[],
        }
    }
}
