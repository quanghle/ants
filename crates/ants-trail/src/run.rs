//! A run of a trail: the state machine its host drives, one worker request after the other.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::protocol::{Answer, Call, Event, JournalEntry, Limits, Request, Response};
use crate::runner::{self, Worker, WorkerError};
use crate::trail::{self, Meta, TrailError};

/// How a run stands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Status {
    /// Not started yet, or waiting on its steps.
    Running,
    /// The script ended with this result.
    Done { result: Value },
    /// The script raised, a limit stopped it, or its host failed it.
    Failed { error: String },
}

/// What one start or resume brought.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Progress {
    /// Steps for the host to perform now, then answer with [`Run::answer`].
    pub calls: Vec<Call>,
    /// Steps handed out before that the script no longer waits on, as the gather siblings of a step that raised, or
    /// all of them once the run ended: the host may abandon them; [`Run::answer`] refuses their answers.
    pub cancelled: Vec<u32>,
    /// What the script reported, in order.
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    Trail(#[from] TrailError),
    #[error("{0}")]
    Limits(String),
    /// The worker gave no response: the run is unchanged and the same call may be retried.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    #[error("the run has already started")]
    Started,
    #[error("the run has ended")]
    Ended,
    #[error("the run waits on no call {0}")]
    UnknownCall(u32),
    /// The worker refused the request: its snapshot is not one this build restores.
    #[error("the worker refused the run: {0}")]
    Refused(String),
    /// Replaying the journal, the script made other calls than the journal holds.
    #[error("the replay diverges from the journal: {0}")]
    Diverged(String),
}

/// Everything a run needs to continue in another process, but its journal (see [`Run::restore`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    script: String,
    args: Value,
    limits: Limits,
    clock: i64,
    seed: u64,
    meta: Meta,
    /// The suspended session; none before the start and after the end.
    snapshot: Option<Box<RawValue>>,
    /// Calls handed to the host and not answered yet.
    outstanding: Vec<Call>,
    /// Calls waiting for a concurrency slot.
    queued: Vec<Call>,
    /// Answered calls, in the order the session receives their answers.
    answered: Vec<u32>,
    /// How many of `answered` the session has received.
    sent: usize,
    steps: u32,
    status: Status,
}

impl RunState {
    #[must_use]
    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    #[must_use]
    pub fn status(&self) -> &Status {
        &self.status
    }

    #[must_use]
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}

/// A run of a trail. The host starts it, performs the calls it hands out, answers them and resumes it until it ends;
/// it stores [`Run::state`] after each start or resume, and each journal entry as it is answered, to continue it
/// elsewhere with [`Run::restore`].
#[derive(Debug, Clone)]
pub struct Run {
    state: RunState,
    /// In the order of `state.answered`.
    journal: Vec<JournalEntry>,
}

impl Run {
    /// A run of `script` reading the current time and a fresh seed.
    pub fn new(script: &str, args: Value, limits: Limits) -> Result<Self, RunError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let clock = i64::try_from(now.as_secs()).unwrap_or(i64::MAX);
        let seed = u64::from(now.subsec_nanos()) ^ (u64::from(std::process::id()) << 32) ^ now.as_secs();
        Self::with_clock(script, args, limits, clock, seed)
    }

    /// A run of `script` whose clock reads `clock` (Unix seconds) and whose `random` is seeded with `seed`.
    pub fn with_clock(script: &str, args: Value, limits: Limits, clock: i64, seed: u64) -> Result<Self, RunError> {
        limits.check().map_err(RunError::Limits)?;
        let meta = trail::check(script)?;
        let state = RunState {
            script: script.to_owned(),
            args,
            limits,
            clock,
            seed,
            meta,
            snapshot: None,
            outstanding: Vec::new(),
            queued: Vec::new(),
            answered: Vec::new(),
            sent: 0,
            steps: 0,
            status: Status::Running,
        };
        Ok(Self { state, journal: Vec::new() })
    }

    /// Continues a run from its stored state and journal, in any order. Journal entries the state does not know yet,
    /// stored before a crash of the host, count as answers not sent to the session yet.
    #[must_use]
    pub fn restore(mut state: RunState, journal: Vec<JournalEntry>) -> Self {
        let order = |call: u32| state.answered.iter().position(|&c| c == call).unwrap_or(usize::MAX);
        let mut journal = journal;
        journal.sort_by_key(|entry| (order(entry.call), entry.call));
        let known: HashSet<u32> = state.answered.iter().copied().collect();
        for entry in journal.iter().filter(|entry| !known.contains(&entry.call)) {
            state.answered.push(entry.call);
        }
        let answered: HashSet<u32> = journal.iter().map(|entry| entry.call).collect();
        state.answered.retain(|call| answered.contains(call));
        state.sent = state.sent.min(state.answered.len());
        state.outstanding.retain(|call| !answered.contains(&call.call));
        Self { state, journal }
    }

    /// Rebuilds the session from the journal alone, with no snapshot: runs the script again with the run's clock
    /// and seed and answers each call from the journal in order. Fails naming the first call the script makes
    /// otherwise than the journal holds.
    pub async fn replay(
        state: RunState,
        journal: Vec<JournalEntry>,
        worker: &Worker,
    ) -> Result<(Self, Progress), RunError> {
        let mut run = Self::restore(state, journal);
        let state = &run.state;
        let start = Request::Start {
            script: state.script.clone(),
            args: state.args.clone(),
            limits: state.limits,
            clock: state.clock,
            seed: state.seed,
        };
        let mut made = Vec::new();
        let mut response = worker.call(&start, runner::deadline(state.limits.compute(), 0)).await?;
        if let Response::Suspended { snapshot, calls, .. } = &response
            && !run.journal.is_empty()
        {
            made.extend(calls.iter().cloned());
            let steps = u32::try_from(calls.len()).unwrap_or(u32::MAX);
            let resume = Request::Resume {
                snapshot: snapshot.clone(),
                answers: run.journal.iter().map(|entry| (entry.call, entry.answer.clone())).collect(),
                limits: state.limits,
                seed: state.seed,
                steps,
            };
            response = worker.call(&resume, runner::deadline(state.limits.compute(), run.journal.len())).await?;
        }
        if let Response::Suspended { calls, .. } = &response {
            made.extend(calls.iter().cloned());
        }
        run.diverged(&made, &response)?;

        let state = &mut run.state;
        state.snapshot = None;
        state.status = Status::Running;
        state.sent = state.answered.len();
        state.steps = u32::try_from(made.len()).unwrap_or(u32::MAX);
        let known: HashSet<u32> = state.outstanding.iter().chain(&state.queued).map(|call| call.call).collect();
        let answered: HashSet<u32> = state.answered.iter().copied().collect();
        let fresh = made.into_iter().filter(|call| !known.contains(&call.call) && !answered.contains(&call.call));
        state.queued.extend(fresh);
        let mut progress = run.absorb(response)?;
        // Replay reports no events: the run reported them when it made them.
        progress.events.clear();
        Ok((run, progress))
    }

    /// Runs the script until it waits on its steps or ends.
    pub async fn start(&mut self, worker: &Worker) -> Result<Progress, RunError> {
        if self.started() {
            return Err(RunError::Started);
        }
        let state = &self.state;
        let request = Request::Start {
            script: state.script.clone(),
            args: state.args.clone(),
            limits: state.limits,
            clock: state.clock,
            seed: state.seed,
        };
        let response = worker.call(&request, runner::deadline(state.limits.compute(), 0)).await;
        self.respond(response)
    }

    /// Records the host's answer to a call it was handed, in the journal, and frees its concurrency slot. The script
    /// receives it at the next [`Run::resume`].
    pub fn answer(&mut self, call: u32, answer: Answer) -> Result<JournalEntry, RunError> {
        if self.state.status != Status::Running {
            return Err(RunError::Ended);
        }
        let index = self.state.outstanding.iter().position(|c| c.call == call).ok_or(RunError::UnknownCall(call))?;
        let Call { call, function, args } = self.state.outstanding.remove(index);
        self.state.answered.push(call);
        let entry = JournalEntry { call, function, args, answer };
        self.journal.push(entry.clone());
        Ok(entry)
    }

    /// Hands the script the answers it has not received yet, in the order they were given, and runs it until it
    /// waits again or ends. With none to hand, only fills free concurrency slots with queued calls. Starts a run
    /// not started yet.
    pub async fn resume(&mut self, worker: &Worker) -> Result<Progress, RunError> {
        if self.state.status != Status::Running {
            return Err(RunError::Ended);
        }
        if !self.started() {
            return self.start(worker).await;
        }
        let state = &self.state;
        let answers: Vec<(u32, Answer)> =
            self.journal[state.sent..].iter().map(|e| (e.call, e.answer.clone())).collect();
        let Some(snapshot) = state.snapshot.clone().filter(|_| !answers.is_empty()) else {
            let mut progress = Progress::default();
            self.dispatch(&mut progress);
            return Ok(progress);
        };
        let deadline = runner::deadline(state.limits.compute(), answers.len());
        let request = Request::Resume { snapshot, answers, limits: state.limits, seed: state.seed, steps: state.steps };
        let response = worker.call(&request, deadline).await;
        self.respond(response)
    }

    /// Records an answer and resumes the run: [`Run::answer`] then [`Run::resume`].
    pub async fn resolve(&mut self, worker: &Worker, call: u32, answer: Answer) -> Result<Progress, RunError> {
        self.answer(call, answer)?;
        self.resume(worker).await
    }

    /// Fails the run, as when its host cancels it. Returns the calls the host may abandon.
    pub fn fail(&mut self, error: impl Into<String>) -> Progress {
        let mut progress = Progress::default();
        if self.state.status == Status::Running {
            self.end(Status::Failed { error: error.into() }, &mut progress);
        }
        progress
    }

    #[must_use]
    pub fn state(&self) -> &RunState {
        &self.state
    }

    /// The answered calls, in the order the script receives them.
    #[must_use]
    pub fn journal(&self) -> &[JournalEntry] {
        &self.journal
    }

    #[must_use]
    pub fn status(&self) -> &Status {
        &self.state.status
    }

    /// The calls handed to the host and not answered yet.
    #[must_use]
    pub fn outstanding(&self) -> &[Call] {
        &self.state.outstanding
    }

    /// The calls waiting for a concurrency slot, not handed to the host yet.
    #[must_use]
    pub fn queued(&self) -> &[Call] {
        &self.state.queued
    }

    fn started(&self) -> bool {
        self.state.snapshot.is_some() || self.state.status != Status::Running
    }

    fn respond(&mut self, response: Result<Response, WorkerError>) -> Result<Progress, RunError> {
        let mut progress = Progress::default();
        match response {
            Ok(response) => {
                self.state.sent = self.state.answered.len();
                if let Response::Suspended { calls, .. } = &response {
                    self.state.steps += u32::try_from(calls.len()).unwrap_or(u32::MAX);
                    self.state.queued.extend(calls.iter().cloned());
                }
                return self.absorb(response);
            }
            Err(WorkerError::ComputeLimit(_)) => {
                let limit = self.state.limits.compute();
                self.end(
                    Status::Failed { error: format!("the trail ran past its compute limit of {limit:?}") },
                    &mut progress,
                );
            }
            Err(WorkerError::OutOfMemory) => {
                let limit = self.state.limits.memory_bytes;
                self.end(
                    Status::Failed { error: format!("the trail ran out of memory ({limit} bytes)") },
                    &mut progress,
                );
            }
            Err(e) => return Err(e.into()),
        }
        Ok(progress)
    }

    /// Takes in a response whose new calls are already queued.
    fn absorb(&mut self, response: Response) -> Result<Progress, RunError> {
        let mut progress = Progress::default();
        match response {
            Response::Suspended { snapshot, pending, events, .. } => {
                progress.events = events;
                let state = &mut self.state;
                state.snapshot = Some(snapshot);
                let waiting: HashSet<u32> = pending.iter().copied().collect();
                for call in state.outstanding.iter().filter(|call| !waiting.contains(&call.call)) {
                    progress.cancelled.push(call.call);
                }
                state.outstanding.retain(|call| waiting.contains(&call.call));
                state.queued.retain(|call| waiting.contains(&call.call));
                self.dispatch(&mut progress);
            }
            Response::Done { result, events } => {
                progress.events = events;
                self.end(Status::Done { result }, &mut progress);
            }
            Response::Failed { error, events } => {
                progress.events = events;
                self.end(Status::Failed { error }, &mut progress);
            }
            Response::Refused { error } => return Err(RunError::Refused(error)),
        }
        Ok(progress)
    }

    /// Hands out queued calls while concurrency slots are free.
    fn dispatch(&mut self, progress: &mut Progress) {
        let state = &mut self.state;
        let free =
            usize::try_from(state.limits.concurrency).unwrap_or(usize::MAX).saturating_sub(state.outstanding.len());
        let handed: Vec<Call> = state.queued.drain(..free.min(state.queued.len())).collect();
        state.outstanding.extend(handed.iter().cloned());
        progress.calls.extend(handed);
    }

    fn end(&mut self, status: Status, progress: &mut Progress) {
        let state = &mut self.state;
        progress.cancelled.extend(state.outstanding.drain(..).map(|call| call.call));
        state.queued.clear();
        state.snapshot = None;
        state.status = status;
    }

    /// Checks the calls a replay made against the journal and the calls the run knew.
    fn diverged(&self, made: &[Call], response: &Response) -> Result<(), RunError> {
        let describe = |call: &Call| format!("{}({})", call.function.name(), call.args);
        let pending: &[u32] = match response {
            Response::Suspended { pending, .. } => pending,
            Response::Refused { error } => return Err(RunError::Refused(error.clone())),
            _ => &[],
        };
        let expected = self.journal.iter().map(|e| Call { call: e.call, function: e.function, args: e.args.clone() });
        for want in expected.chain(self.state.outstanding.iter().cloned()).chain(self.state.queued.iter().cloned()) {
            match made.iter().find(|call| call.call == want.call) {
                None => {
                    return Err(RunError::Diverged(format!(
                        "the script no longer makes call {} {}",
                        want.call,
                        describe(&want)
                    )));
                }
                Some(call) if call != &want => {
                    let (want, made) = (describe(&want), describe(call));
                    return Err(RunError::Diverged(format!("call {} is {made}, the journal has {want}", call.call)));
                }
                Some(_) => {}
            }
        }
        if let Some(entry) = self.journal.iter().find(|entry| pending.contains(&entry.call)) {
            return Err(RunError::Diverged(format!("the script makes call {} only after its answer", entry.call)));
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::runner::GRACE;
    use crate::runner::tests::fake;

    fn run(limits: Limits) -> Run {
        let script = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": []}\nawait agent('a')\n";
        Run::with_clock(script, json!(null), limits, 0, 0).unwrap()
    }

    #[tokio::test]
    async fn a_crashed_worker_leaves_the_run_as_it_was() {
        let mut run = run(Limits::default());
        let crashed = run.start(&fake("run-crash", "kill -SEGV $$")).await;
        assert!(matches!(crashed, Err(RunError::Worker(WorkerError::Crashed { .. }))), "{crashed:?}");
        assert_eq!(*run.status(), Status::Running);
        assert!(!run.started());
    }

    #[tokio::test]
    async fn a_worker_killed_past_its_deadline_fails_the_run() {
        let mut run = run(Limits { compute_ms: 1, ..Limits::default() });
        let started = std::time::Instant::now();
        run.start(&fake("run-slow", "exec sleep 30")).await.unwrap();
        assert!(started.elapsed() < GRACE + Duration::from_secs(2));
        assert_eq!(*run.status(), Status::Failed { error: "the trail ran past its compute limit of 1ms".to_owned() });
        assert!(matches!(run.resume(&fake("run-slow", "exit 1")).await, Err(RunError::Ended)));
    }

    #[tokio::test]
    async fn a_worker_out_of_memory_fails_the_run() {
        let mut run = run(Limits::default());
        run.start(&fake("run-oom", "exit 65")).await.unwrap();
        let error = format!("the trail ran out of memory ({} bytes)", Limits::DEFAULT_MEMORY_BYTES);
        assert_eq!(*run.status(), Status::Failed { error });
    }
}
