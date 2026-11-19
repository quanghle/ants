//! The `ants subprocess` worker: serves one [`Request`] from stdin with one [`Response`] on stdout, then exits.

use std::borrow::Cow;
use std::io::{self, Read, Write};
use std::time::Instant;

use monty::{FunctionCall, OsCall, RunProgress};
use monty_types::{
    DateTimeSource, ExcType, ExtFunctionResult, MontyException, MontyObject, NameLookupResult, OsFunctionCall,
    OsPolicy, PrintWriter, PrintWriterCallback, ProcessTime, RandomSeed, RandomStart, ResourceLimits, ResourceTracker,
    SleepMode,
};
use serde_json::value::RawValue;

use crate::protocol::{Answer, Call, Event, Function, Limit, Limits, Request, Response};
use crate::trail;
use crate::value::{from_monty, to_monty};

/// The worker thread's stack: Monty recurses on the native stack, up to [`Limits::MAX_RECURSION`] Python frames.
const STACK_BYTES: usize = 16 * 1024 * 1024;
/// Bytes `print()`, `log()` and `phase()` may write in one resume, each note counting [`NOTE_COST`] on top of its
/// text; past them the call raises RuntimeError.
pub(crate) const NOTE_BYTES: usize = 64 * 1024;
/// What one note costs on top of its text: its JSON object, `{"log":""},`, and more.
const NOTE_COST: usize = 16;
/// Bytes of one note's text; a longer one is cut, ending with `…`.
pub(crate) const MAX_NOTE: usize = 4000;
/// Bytes of JSON the arguments of the calls of one resume may have, each call counting [`CALL_COST`] on top: what a
/// run's record holds of its waiting calls. Past them the call raises RuntimeError.
pub(crate) const CALL_BYTES: usize = 1024 * 1024;
/// What one call costs on top of its arguments: its id and function.
const CALL_COST: usize = 64;
/// Bytes of JSON a trail's result may have: what its session might, so that the run's record holds it.
pub(crate) const MAX_RESULT: usize = Limits::MAX_DUMP_BYTES;
/// Bytes of a failed script's error; a longer one is cut, ending with `…`.
pub(crate) const MAX_ERROR: usize = 16 * 1024;
/// Bytes of JSON a goto's state, the next leg's input, may have.
const MAX_STATE: usize = 64 * 1024;
/// The exit status of a worker that could not read its request or write its response, or whose thread panicked.
const FAILED: i32 = 70;

/// Whether this process was started as a worker, `ants subprocess`.
#[must_use]
pub fn asked() -> bool {
    std::env::args_os().nth(1).is_some_and(|arg| arg == "subprocess")
}

/// Serves the request on stdin and returns the exit status. Call it first in `main`, before any runtime starts.
#[must_use]
pub fn main() -> i32 {
    let worker = std::thread::Builder::new().name("trail".to_owned()).stack_size(STACK_BYTES).spawn(stdio);
    match worker.map(std::thread::JoinHandle::join) {
        Ok(Ok(Ok(()))) => 0,
        Ok(Ok(Err(e))) => {
            eprintln!("ants subprocess: {e}");
            FAILED
        }
        Ok(Err(_)) => FAILED,
        Err(e) => {
            eprintln!("ants subprocess: cannot start the worker thread: {e}");
            FAILED
        }
    }
}

fn stdio() -> io::Result<()> {
    let mut input = String::new();
    io::stdin().lock().read_to_string(&mut input)?;
    let response = serve(&input);
    drop(input);
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &response)?;
    out.flush()
}

/// Serves one request, as JSON.
pub(crate) fn serve(input: &str) -> Response {
    bounded(respond(input))
}

/// The most bytes of JSON the response to a request of `resumes` resumes (a start is one) takes, what the runner
/// reads at most: a session or a result, an error, and for each resume its calls, the ids of those waiting and its
/// notes, each byte of a note's text escaped as `\u00XX` at worst.
#[must_use]
pub(crate) fn response_bound(resumes: usize) -> usize {
    let once = 2 * Limits::MAX_DUMP_BYTES + 6 * MAX_ERROR;
    let each = 2 * CALL_BYTES + 6 * (NOTE_BYTES + NOTE_COST);
    resumes.max(1).saturating_mul(each).saturating_add(once)
}

/// Fails a script whose result is too large to store, and cuts a too long error.
fn bounded(response: Response) -> Response {
    match response {
        Response::Done { result, calls, events } => {
            let size = serde_json::to_vec(&result).map_or(usize::MAX, |result| result.len());
            if size <= MAX_RESULT {
                return Response::Done { result, calls, events };
            }
            let error = format!("the trail's result has {size} bytes of JSON; a result holds at most {MAX_RESULT}");
            Response::Failed { error, calls, events, limit: None }
        }
        Response::Failed { error, calls, events, limit } => {
            Response::Failed { error: cut(error, MAX_ERROR), calls, events, limit }
        }
        other => other,
    }
}

/// `text` cut to at most `most` bytes, ending with `…` if cut.
fn cut(mut text: String, most: usize) -> String {
    if text.len() <= most {
        return text;
    }
    let mut end = most - '…'.len_utf8();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push('…');
    text
}

fn respond(input: &str) -> Response {
    let request = match serde_json::from_str::<Request>(input) {
        Ok(request) => request,
        Err(e) => return Response::Refused { error: format!("unreadable request: {e}") },
    };
    match request {
        Request::Start { script, entry, args, limits, clock, seed } => {
            if let Err(error) = limits.check() {
                return Response::Refused { error };
            }
            let script = match entry {
                Some(entry) => match trail::enter(&script, &entry) {
                    Ok(program) => program,
                    Err(error) => return Response::Refused { error },
                },
                None => script,
            };
            arm(&limits);
            start(&script, &args, limits, clock, seed)
        }
        Request::Resume { snapshot, answers, limits, seed, steps } => {
            if let Err(error) = limits.check() {
                return Response::Refused { error };
            }
            arm(&limits);
            resume(&snapshot, answers, limits, seed, steps)
        }
    }
}

/// Caps the process's memory past the script's own limit: a script that allocates past the cap before Monty notices
/// ends the worker with [`monty_types::OOM_EXIT_CODE`].
fn arm(limits: &Limits) {
    // Fails only where the allocator is not installed: in unit tests, which do not reach the cap.
    let _ = monty_alloc::set_hard_limit(monty_types::memory_limit_with_headroom(Some(limits.memory_bytes), false));
}

fn start(script: &str, args: &serde_json::Value, limits: Limits, clock: i64, seed: u64) -> Response {
    let mut session = Session::new(limits, seed, 0);
    let run = match trail::compile(script) {
        Ok(run) => run.with_os_policy(policy(clock, seed)),
        Err(e) => return session.failed(e),
    };
    let tracker = ResourceTracker::new(ResourceLimits {
        max_turn_duration: Some(limits.compute()),
        max_memory: Some(limits.memory_bytes),
        max_recursion_depth: limits.recursion,
        ..ResourceLimits::default()
    });
    let inputs = vec![
        to_monty(args),
        MontyObject::function("__ants_agent", None),
        MontyObject::function("__ants_human", None),
        MontyObject::function("__ants_trail", None),
        MontyObject::function("__ants_goto", None),
        MontyObject::function("__ants_phase", None),
        MontyObject::function("__ants_log", None),
    ];
    let progress = run.start(inputs, tracker, session.writer());
    session.drive(progress).respond(session)
}

fn resume(snapshot: &RawValue, answers: Vec<(u32, Answer)>, limits: Limits, seed: u64, steps: u32) -> Response {
    let mut session = Session::new(limits, seed, steps);
    let mut waiting = match serde_json::from_str(snapshot.get()) {
        Ok(RunProgress::ResolveFutures(waiting)) => waiting,
        Ok(_) => return Response::Unrestorable { error: "the snapshot is not a suspended session".to_owned() },
        Err(e) => return Response::Unrestorable { error: format!("unreadable snapshot: {e}") },
    };
    for (call, answer) in answers {
        // A call of a coroutine that ended, as a gather sibling of a step that raised, waits no more.
        if !waiting.pending_call_ids().contains(&call) {
            continue;
        }
        let result = match answer {
            Answer::Ok(value) => ExtFunctionResult::Return(to_monty(&value)),
            Answer::Error(message) => {
                ExtFunctionResult::Error(MontyException::new(ExcType::RuntimeError, Some(message)))
            }
        };
        session.next_resume();
        let progress = waiting.resume(vec![(call, result)], session.writer());
        match session.drive(progress) {
            Outcome::Waiting(next) => waiting = *next,
            ended => return ended.respond(session),
        }
    }
    Outcome::Waiting(Box::new(waiting)).respond(session)
}

/// The clock, the timezone and the random seed are fixed by the request, so the same request makes the same calls.
fn policy(clock: i64, seed: u64) -> OsPolicy {
    OsPolicy {
        datetime: DateTimeSource::Fixed { unix_seconds: clock, microsecond: 0 },
        sleep: SleepMode::CallHost,
        process_time: ProcessTime::Zero,
        random_start: RandomStart::Seed(RandomSeed::Int(seed.into())),
        ..OsPolicy::default()
    }
}

enum Outcome {
    Waiting(Box<monty::ResolveFutures>),
    Done(serde_json::Value),
    Failed(String),
}

impl Outcome {
    fn respond(self, mut session: Session) -> Response {
        session.flush(true);
        let Session { events, calls, limits, limit, .. } = session;
        match self {
            Self::Done(result) => Response::Done { result, calls, events },
            Self::Failed(error) => Response::Failed { error, calls, events, limit },
            Self::Waiting(waiting) => {
                let pending = waiting.pending_call_ids().to_vec();
                let snapshot = match serde_json::to_string(&RunProgress::ResolveFutures(*waiting)) {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        let error = format!("cannot dump the session: {e}");
                        return Response::Failed { error, calls, events, limit: None };
                    }
                };
                let most = limits.dump_bytes;
                if snapshot.len() > most {
                    let size = snapshot.len();
                    return Response::Failed {
                        error: format!("the dumped session of {size} bytes passes the limit of {most} bytes"),
                        calls,
                        events,
                        limit: Some(Limit::Session),
                    };
                }
                match RawValue::from_string(snapshot) {
                    Ok(snapshot) => Response::Suspended { snapshot, calls, pending, events },
                    Err(e) => {
                        let error = format!("cannot dump the session: {e}");
                        Response::Failed { error, calls, events, limit: None }
                    }
                }
            }
        }
    }
}

/// What one request collects while it drives the script.
struct Session {
    limits: Limits,
    seed: u64,
    steps: u32,
    calls: Vec<Call>,
    /// Bytes the calls of the current resume cost, against [`CALL_BYTES`].
    called: usize,
    events: Vec<Event>,
    notes: Notes,
    /// When Monty last took over, starting the turn its compute limit applies to.
    turn: Instant,
    /// The limit the script broke, should it fail: one of time or memory it failed on, else the steps once a call
    /// past them raised.
    limit: Option<Limit>,
}

/// The notes of the current resume: what `print()` wrote not yet turned into events, and the bytes of notes written.
#[derive(Default)]
struct Notes {
    print: String,
    written: usize,
}

impl Notes {
    /// Counts `bytes` more written by `by`, refusing them past [`NOTE_BYTES`].
    fn write(&mut self, by: &str, bytes: usize) -> Result<(), MontyException> {
        let written = self.written.saturating_add(bytes);
        if written > NOTE_BYTES {
            return Err(MontyException::new(
                ExcType::RuntimeError,
                Some(format!("{by} passes the {NOTE_BYTES} bytes print(), log() and phase() may write in one resume")),
            ));
        }
        self.written = written;
        Ok(())
    }
}

impl PrintWriterCallback for Notes {
    fn stdout_write(&mut self, output: Cow<'_, str>) -> Result<(), MontyException> {
        self.write("print()", output.len() + NOTE_COST * output.matches('\n').count())?;
        self.print.push_str(&output);
        Ok(())
    }

    fn stdout_push(&mut self, end: char) -> Result<(), MontyException> {
        self.write("print()", end.len_utf8() + if end == '\n' { NOTE_COST } else { 0 })?;
        self.print.push(end);
        Ok(())
    }
}

impl Session {
    fn new(limits: Limits, seed: u64, steps: u32) -> Self {
        Self {
            limits,
            seed,
            steps,
            calls: Vec::new(),
            called: 0,
            events: Vec::new(),
            notes: Notes::default(),
            turn: Instant::now(),
            limit: None,
        }
    }

    /// Starts the turn Monty takes over for, lending it the writer of `print()`.
    fn writer(&mut self) -> PrintWriter<'_> {
        self.turn = Instant::now();
        PrintWriter::Callback(&mut self.notes)
    }

    /// Starts the resume of the next answer: its calls and notes are bounded anew.
    fn next_resume(&mut self) {
        self.called = 0;
        self.notes.written = 0;
    }

    /// The limit of Monty's that `exception`, ending the script, says it broke. Monty raises an uncatchable
    /// TimeoutError or MemoryError for its limits, but tells them from the script's own by nothing: the time limit
    /// is told by the time the turn took, the memory limit by Monty's message naming the request's limit.
    fn broke(&self, exception: &MontyException) -> Option<Limit> {
        match exception.exc_type() {
            ExcType::TimeoutError if self.turn.elapsed() >= self.limits.compute() => Some(Limit::Compute),
            ExcType::MemoryError => {
                let message = exception.message()?.strip_prefix("memory limit exceeded: ")?;
                let (used, limit) = message.strip_suffix(" bytes")?.split_once(" bytes > ")?;
                let (used, limit) = (used.parse::<usize>().ok()?, limit.parse::<usize>().ok()?);
                (limit == self.limits.memory_bytes && used > limit).then_some(Limit::Memory)
            }
            _ => None,
        }
    }

    fn failed(&mut self, exception: MontyException) -> Response {
        self.flush(true);
        let (calls, events) = (std::mem::take(&mut self.calls), std::mem::take(&mut self.events));
        Response::Failed { error: trail::render(exception), calls, events, limit: None }
    }

    /// Runs the script until it waits on its steps, ends or raises.
    fn drive(&mut self, mut progress: Result<RunProgress, MontyException>) -> Outcome {
        loop {
            progress = match progress {
                Err(e) => {
                    self.limit = self.broke(&e).or(self.limit);
                    return Outcome::Failed(trail::render(e));
                }
                Ok(RunProgress::Complete(result)) => {
                    return match from_monty(result.as_ref()) {
                        Ok(result) => Outcome::Done(result),
                        Err(e) => Outcome::Failed(format!("the trail's result is not JSON: {e}")),
                    };
                }
                Ok(RunProgress::ResolveFutures(waiting)) => return Outcome::Waiting(Box::new(waiting)),
                Ok(RunProgress::FunctionCall(call)) => self.call(call),
                Ok(RunProgress::OsCall(call)) => self.os(call),
                Ok(RunProgress::NameLookup(lookup)) => lookup.resume(NameLookupResult::Undefined, self.writer()),
            };
        }
    }

    fn call(&mut self, call: FunctionCall) -> Result<RunProgress, MontyException> {
        let function = match call.function_name.as_str() {
            "__ants_agent" => Some(Function::Agent),
            "__ants_human" => Some(Function::Human),
            "__ants_trail" => Some(Function::Trail),
            "__ants_goto" => Some(Function::Goto),
            "__ants_phase" | "__ants_log" => {
                let phase = call.function_name == "__ants_phase";
                let text = call.args.arg(0).and_then(|a| a.as_str()).unwrap_or_default().to_owned();
                let text = cut(text, MAX_NOTE);
                let by = if phase { "phase()" } else { "log()" };
                if let Err(e) = self.notes.write(by, text.len() + NOTE_COST) {
                    return call.resume(ExtFunctionResult::Error(e), self.writer());
                }
                self.flush(false);
                self.events.push(if phase { Event::Phase(text) } else { Event::Log(text) });
                return call.resume(MontyObject::none(), self.writer());
            }
            _ => None,
        };
        let result = match function {
            None => ExtFunctionResult::NotFound(call.function_name.clone()),
            Some(function) if function.is_step() && self.steps >= self.limits.steps => {
                self.limit = Some(Limit::Steps);
                ExtFunctionResult::Error(MontyException::new(
                    ExcType::RuntimeError,
                    Some(format!("the run reached its limit of {} steps", self.limits.steps)),
                ))
            }
            Some(function) => match call.args.arg(0).map(from_monty).transpose() {
                Ok(args) => {
                    let args = args.unwrap_or_default();
                    match oversized(function, &args).map_or_else(|| self.charge(function, &args), Err) {
                        Err(error) => ExtFunctionResult::Error(MontyException::new(ExcType::RuntimeError, Some(error))),
                        Ok(()) => {
                            self.steps += u32::from(function.is_step());
                            self.calls.push(Call { call: call.call_id, function, args });
                            return call.resume_pending(self.writer());
                        }
                    }
                }
                Err(e) => ExtFunctionResult::Error(MontyException::new(
                    ExcType::TypeError,
                    Some(format!("{}() takes JSON values: {e}", function.name())),
                )),
            },
        };
        call.resume(result, self.writer())
    }

    /// Counts a call's arguments against the [`CALL_BYTES`] of the current resume, refusing them past it.
    fn charge(&mut self, function: Function, args: &serde_json::Value) -> Result<(), String> {
        let size = serde_json::to_vec(args).map_or(usize::MAX, |args| args.len());
        let called = self.called.saturating_add(size).saturating_add(CALL_COST);
        if called > CALL_BYTES {
            let name = function.name();
            return Err(format!(
                "{name}()'s arguments of {size} bytes of JSON pass the {CALL_BYTES} bytes the calls of one resume may have"
            ));
        }
        self.called = called;
        Ok(())
    }

    /// Answers `os.urandom` from the seed; refuses every other OS call.
    fn os(&mut self, call: OsCall) -> Result<RunProgress, MontyException> {
        let result = match &call.function_call {
            OsFunctionCall::Urandom(args) => match usize::try_from(args.size) {
                Ok(size) if size <= 1024 * 1024 => {
                    ExtFunctionResult::Return(MontyObject::bytes(random(self.seed, call.call_id, size)))
                }
                _ => ExtFunctionResult::Error(MontyException::new(
                    ExcType::ValueError,
                    Some("urandom size too large".to_owned()),
                )),
            },
            other => other.no_handler_result(),
        };
        call.resume(result, self.writer())
    }

    /// Turns the complete lines `print()` wrote, and with `all` the rest too, into log events.
    fn flush(&mut self, all: bool) {
        let print = &mut self.notes.print;
        let end = if all { print.len() } else { print.rfind('\n').map_or(0, |i| i + 1) };
        if end == 0 {
            return;
        }
        let printed: String = print.drain(..end).collect();
        self.events.extend(printed.lines().map(|line| Event::Log(cut(line.to_owned(), MAX_NOTE))));
    }
}

/// Why a goto's arguments are refused: a state of more than [`MAX_STATE`] bytes of JSON.
fn oversized(function: Function, args: &serde_json::Value) -> Option<String> {
    let state = args.get("state").filter(|_| function == Function::Goto)?;
    let size = serde_json::to_vec(state).map_or(usize::MAX, |state| state.len());
    (size > MAX_STATE).then(|| format!("goto()'s state has {size} bytes of JSON; a leg takes at most {MAX_STATE}"))
}

/// Bytes from the run's seed and the call's id (splitmix64): the same call of the same run reads the same bytes.
fn random(seed: u64, call: u32, size: usize) -> Vec<u8> {
    let mut state = seed ^ u64::from(call).wrapping_mul(0xd1b5_4a32_d192_ed03);
    let mut bytes = Vec::with_capacity(size + 8);
    while bytes.len() < size {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        bytes.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    bytes.truncate(size);
    bytes
}

#[cfg(test)]
mod tests;
