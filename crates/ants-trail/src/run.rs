//! A run of a trail: the state machine its host drives, one worker request after the other.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use ants_proto::{CallAnswer, JournalEntry, RunLimit};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::protocol::{Call, Event, Limits, Request, Response};
use crate::runner::{self, Worker, WorkerError};
use crate::trail::{self, Meta, TrailError};
use crate::value::check_depth;

/// How a run stands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Status {
    /// Not started yet, or waiting on its steps.
    Running,
    /// The script ended with this result.
    Done { result: Value },
    /// The script raised, a limit stopped it, or its host failed it.
    Failed {
        error: String,
        /// The limit that stopped it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<RunLimit>,
    },
}

/// What one start or resume brought.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Progress {
    /// Calls for the host to perform now, steps and gotos, then answer with [`Run::answer`].
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
    /// The run enters its trail at no waypoint the trail has.
    #[error("{0}")]
    Entry(String),
    /// The run's args are nested deeper than [`crate::MAX_DEPTH`].
    #[error("the arguments are refused: {0}")]
    Args(String),
    /// The worker could not start, crashed, or could not be read: the run is unchanged and the same call may be
    /// retried. In [`Run::start`] and [`Run::resume`], a worker past its deadline or out of memory, or whose response
    /// is oversized or unreadable, fails the run instead.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    #[error("the run has already started")]
    Started,
    #[error("the run has ended")]
    Ended,
    #[error("the run waits on no call {0}")]
    UnknownCall(u32),
    /// The worker refused the request: unreadable to it, or under limits it does not take.
    #[error("the worker refused the run: {0}")]
    Refused(String),
    /// The run's session is not one this build restores: dumped by another Monty version, or missing. The run is
    /// unchanged; [`Run::replay`] rebuilds the session from the journal.
    #[error("the run's session cannot be restored: {0}")]
    Unrestorable(String),
    /// Replaying the journal, the script made other calls than the journal holds.
    #[error("the replay diverges from the journal: {0}")]
    Diverged(String),
}

/// Everything a run needs to continue in another process, but its journal (see [`Run::restore`]). A run is one leg of
/// a trail run: a goto() ends it, and the host goes on in a run of the next leg ([`Run::numbered_from`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    script: String,
    /// The waypoint the run enters its trail at; none for a trail without waypoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entry: Option<String>,
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
    /// The steps the session made.
    steps: u32,
    /// What the session's call ids are offset by: the first call id of the leg.
    first_call: u32,
    /// The call id past every call made.
    next_call: u32,
    status: Status,
}

impl Session {
    #[must_use]
    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    #[must_use]
    pub fn status(&self) -> &Status {
        &self.status
    }

    /// The waypoint the run enters its trail at; none for a trail without waypoints, entered at its start.
    #[must_use]
    pub fn entry(&self) -> Option<&str> {
        self.entry.as_deref()
    }

    #[must_use]
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The answered calls, in the order the session receives their answers.
    #[must_use]
    pub fn answered(&self) -> &[u32] {
        &self.answered
    }

    /// The id of the run's first call.
    #[must_use]
    pub fn first_call(&self) -> u32 {
        self.first_call
    }

    /// The call id past every call the run made: the first of a leg following it.
    #[must_use]
    pub fn next_call(&self) -> u32 {
        self.next_call
    }

    /// The state under `limits` instead, as when a run that failed on a limit continues under raised ones.
    pub fn with_limits(self, limits: Limits) -> Result<Self, RunError> {
        limits.check().map_err(RunError::Limits)?;
        Ok(Self { limits, ..self })
    }
}

/// A run of a trail. The host starts it, performs the calls it hands out, answers them and resumes it until it ends;
/// it stores [`Run::state`] after each start or resume, and each journal entry as it is answered, to continue it
/// elsewhere with [`Run::restore`].
#[derive(Debug, Clone)]
pub struct Run {
    state: Session,
    /// In the order of `state.answered`.
    journal: Vec<JournalEntry>,
}

impl Run {
    /// A run of `script` entering it at the waypoint `entry`, else at its start, with `args` as its input, reading
    /// the current time and a fresh seed.
    pub fn new(script: &str, entry: Option<&str>, args: Value, limits: Limits) -> Result<Self, RunError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let clock = i64::try_from(now.as_secs()).unwrap_or(i64::MAX);
        let seed = u64::from(now.subsec_nanos()) ^ (u64::from(std::process::id()) << 32) ^ now.as_secs();
        Self::with_clock(script, entry, args, limits, clock, seed)
    }

    /// A run of `script` as [`Run::new`] makes one, whose clock reads `clock` (Unix seconds) and whose `random` is
    /// seeded with `seed`. Refused when the script's meta lists no waypoint `entry`.
    pub fn with_clock(
        script: &str,
        entry: Option<&str>,
        args: Value,
        limits: Limits,
        clock: i64,
        seed: u64,
    ) -> Result<Self, RunError> {
        limits.check().map_err(RunError::Limits)?;
        check_depth(&args).map_err(RunError::Args)?;
        let meta = trail::check(script)?;
        let entry = meta.entry(entry).map_err(RunError::Entry)?.map(str::to_owned);
        let state = Session {
            script: script.to_owned(),
            entry,
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
            first_call: 0,
            next_call: 0,
            status: Status::Running,
        };
        Ok(Self { state, journal: Vec::new() })
    }

    /// The run, not started, with its call ids from `first` on: a leg following another, numbered from that one's
    /// [`Session::next_call`], so call ids are unique across a trail run's legs.
    #[must_use]
    pub fn numbered_from(mut self, first: u32) -> Self {
        self.state.first_call = first;
        self.state.next_call = first;
        self
    }

    /// Continues a run from its stored state and journal, in any order. Journal entries the state does not know yet,
    /// stored before a crash of the host, count as answers not sent to the session yet.
    #[must_use]
    pub fn restore(mut state: Session, journal: Vec<JournalEntry>) -> Self {
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

    /// Rebuilds the session from the journal alone, with no snapshot: runs the script again from the run's entry,
    /// with the run's input, clock and seed, and answers each call from the journal in order. Fails naming the first
    /// call the script makes otherwise than the journal holds.
    pub async fn replay(
        state: Session,
        journal: Vec<JournalEntry>,
        worker: &Worker,
    ) -> Result<(Self, Progress), RunError> {
        let mut run = Self::restore(state, journal);
        let (limits, seed) = (run.state.limits, run.state.seed);
        let start = Request::Start {
            script: run.state.script.clone(),
            entry: run.state.entry.clone(),
            args: run.state.args.clone(),
            limits,
            clock: run.state.clock,
            seed,
        };
        let mut response = worker.call(&start, runner::deadline(limits.compute(), 0)).await?;
        run.number(&mut response);
        let mut made = response.calls().to_vec();
        if let Response::Suspended { snapshot, calls, .. } = &response
            && !run.journal.is_empty()
        {
            let resume = Request::Resume {
                snapshot: snapshot.clone(),
                answers: run.answers(0),
                limits,
                seed,
                steps: steps(calls),
            };
            response = worker.call(&resume, runner::deadline(limits.compute(), run.journal.len())).await?;
            run.number(&mut response);
            made.extend(response.calls().iter().cloned());
        }
        run.diverged(&made, &response)?;

        let state = &mut run.state;
        state.snapshot = None;
        state.status = Status::Running;
        state.sent = state.answered.len();
        state.steps = steps(&made);
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
            entry: state.entry.clone(),
            args: state.args.clone(),
            limits: state.limits,
            clock: state.clock,
            seed: state.seed,
        };
        let response = worker.call(&request, runner::deadline(state.limits.compute(), 0)).await;
        self.respond(response)
    }

    /// Records the host's answer to a call it was handed, in the journal, and frees its concurrency slot. The script
    /// receives it at the next [`Run::resume`]; a value nested deeper than [`crate::MAX_DEPTH`] as an error instead.
    pub fn answer(&mut self, call: u32, answer: CallAnswer) -> Result<JournalEntry, RunError> {
        if self.state.status != Status::Running {
            return Err(RunError::Ended);
        }
        let answer = match answer {
            CallAnswer::Ok(value) => match check_depth(&value) {
                Ok(()) => CallAnswer::Ok(value),
                Err(e) => CallAnswer::Error(format!("the answer is refused: {e}")),
            },
            error @ CallAnswer::Error(_) => error,
        };
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
        let answers = self.answers(self.state.sent);
        let state = &self.state;
        if answers.is_empty() {
            let mut progress = Progress::default();
            self.dispatch(&mut progress);
            return Ok(progress);
        }
        let Some(snapshot) = state.snapshot.clone() else {
            return Err(RunError::Unrestorable("the run has no stored session".to_owned()));
        };
        let deadline = runner::deadline(state.limits.compute(), answers.len());
        let request = Request::Resume { snapshot, answers, limits: state.limits, seed: state.seed, steps: state.steps };
        let response = worker.call(&request, deadline).await;
        self.respond(response)
    }

    /// Records an answer and resumes the run: [`Run::answer`] then [`Run::resume`].
    pub async fn resolve(&mut self, worker: &Worker, call: u32, answer: CallAnswer) -> Result<Progress, RunError> {
        self.answer(call, answer)?;
        self.resume(worker).await
    }

    /// Fails the run, as when its host cancels it. Returns the calls the host may abandon.
    pub fn fail(&mut self, error: impl Into<String>) -> Progress {
        self.fail_on(None, error)
    }

    /// Fails the run on `limit`, as [`Run::fail`] does.
    pub fn fail_on(&mut self, limit: Option<RunLimit>, error: impl Into<String>) -> Progress {
        let mut progress = Progress::default();
        if self.state.status == Status::Running {
            self.end(Status::Failed { error: error.into(), limit }, &mut progress);
        }
        progress
    }

    #[must_use]
    pub fn state(&self) -> &Session {
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

    /// Whether the script ran: a run that made calls has, even with its session lost.
    fn started(&self) -> bool {
        let state = &self.state;
        state.snapshot.is_some() || state.status != Status::Running || state.next_call > state.first_call
    }

    /// Makes the session's call ids in `response` the run's, past those of its earlier legs.
    fn number(&mut self, response: &mut Response) {
        let first = self.state.first_call;
        if let Response::Suspended { pending, .. } = response {
            pending.iter_mut().for_each(|call| *call = call.saturating_add(first));
        }
        let (Response::Suspended { calls, .. } | Response::Done { calls, .. } | Response::Failed { calls, .. }) =
            response
        else {
            return;
        };
        for call in calls {
            call.call = call.call.saturating_add(first);
            self.state.next_call = self.state.next_call.max(call.call.saturating_add(1));
        }
    }

    /// The journal's answers from the `from`th on, by the session's call ids.
    fn answers(&self, from: usize) -> Vec<(u32, CallAnswer)> {
        let first = self.state.first_call;
        self.journal[from..].iter().map(|e| (e.call.saturating_sub(first), e.answer.clone())).collect()
    }

    fn respond(&mut self, response: Result<Response, WorkerError>) -> Result<Progress, RunError> {
        let mut progress = Progress::default();
        match response {
            Ok(Response::Refused { error }) => return Err(RunError::Refused(error)),
            Ok(Response::Unrestorable { error }) => return Err(RunError::Unrestorable(error)),
            Ok(mut response) => {
                self.number(&mut response);
                self.state.sent = self.state.answered.len();
                if let Response::Suspended { calls, .. } = &response {
                    self.state.steps += steps(calls);
                    self.state.queued.extend(calls.iter().cloned());
                }
                return self.absorb(response);
            }
            Err(WorkerError::ComputeLimit(_)) => {
                let most = self.state.limits.compute();
                let error = format!("the trail ran past its compute limit of {most:?}");
                self.end(Status::Failed { error, limit: Some(RunLimit::Compute) }, &mut progress);
            }
            Err(WorkerError::OutOfMemory) => {
                let most = self.state.limits.memory_bytes;
                let error = format!("the trail ran out of memory ({most} bytes)");
                self.end(Status::Failed { error, limit: Some(RunLimit::Memory) }, &mut progress);
            }
            // The same request would answer the same.
            Err(e @ (WorkerError::Oversized(_) | WorkerError::Protocol(_))) => {
                self.end(Status::Failed { error: e.to_string(), limit: None }, &mut progress);
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
            Response::Done { result, events, .. } => {
                progress.events = events;
                self.end(Status::Done { result }, &mut progress);
            }
            Response::Failed { error, events, limit, .. } => {
                progress.events = events;
                self.end(Status::Failed { error, limit }, &mut progress);
            }
            Response::Refused { error } => return Err(RunError::Refused(error)),
            Response::Unrestorable { error } => return Err(RunError::Unrestorable(error)),
        }
        Ok(progress)
    }

    /// Hands out queued steps while concurrency slots are free, and gotos at once.
    fn dispatch(&mut self, progress: &mut Progress) {
        let state = &mut self.state;
        let busy = state.outstanding.iter().filter(|call| call.function.is_step()).count();
        let mut free = usize::try_from(state.limits.concurrency).unwrap_or(usize::MAX).saturating_sub(busy);
        let mut handed = Vec::new();
        state.queued.retain(|call| {
            let step = call.function.is_step();
            if step && free == 0 {
                return true;
            }
            free -= usize::from(step);
            handed.push(call.clone());
            false
        });
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
            Response::Unrestorable { error } => return Err(RunError::Unrestorable(error.clone())),
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

/// The steps among `calls`.
fn steps(calls: &[Call]) -> u32 {
    u32::try_from(calls.iter().filter(|call| call.function.is_step()).count()).unwrap_or(u32::MAX)
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
        Run::with_clock(script, None, json!(null), limits, 0, 0).unwrap()
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
        let error = "the trail ran past its compute limit of 1ms".to_owned();
        assert_eq!(*run.status(), Status::Failed { error, limit: Some(RunLimit::Compute) });
        assert!(matches!(run.resume(&fake("run-slow", "exit 1")).await, Err(RunError::Ended)));
    }

    #[tokio::test]
    async fn a_worker_out_of_memory_fails_the_run() {
        let mut run = run(Limits::default());
        run.start(&fake("run-oom", "exit 65")).await.unwrap();
        let error = format!("the trail ran out of memory ({} bytes)", Limits::DEFAULT_MEMORY_BYTES);
        assert_eq!(*run.status(), Status::Failed { error, limit: Some(RunLimit::Memory) });
    }

    #[tokio::test]
    async fn an_unreadable_response_fails_the_run() {
        let mut run = run(Limits::default());
        run.start(&fake("run-garbled", "cat >/dev/null; echo nonsense")).await.unwrap();
        let Status::Failed { error, limit: None } = run.status() else { panic!("{:?}", run.status()) };
        assert!(error.starts_with("the worker's response is unreadable: "), "{error}");
    }
}
