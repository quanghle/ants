//! The `ants subprocess` worker: serves one [`Request`] from stdin with one [`Response`] on stdout, then exits.

use std::io::{self, Read, Write};

use monty::{FunctionCall, OsCall, RunProgress};
use monty_types::{
    DateTimeSource, ExcType, ExtFunctionResult, MontyException, MontyObject, NameLookupResult, OsFunctionCall,
    OsPolicy, PrintWriter, ProcessTime, RandomSeed, RandomStart, ResourceLimits, ResourceTracker, SleepMode,
};
use serde_json::value::RawValue;

use crate::protocol::{Answer, Call, Event, Function, Limits, Request, Response};
use crate::trail;
use crate::value::{from_monty, to_monty};

/// The worker thread's stack: Monty recurses on the native stack, up to [`Limits::MAX_RECURSION`] Python frames.
const STACK_BYTES: usize = 16 * 1024 * 1024;
/// `print()` output one request may collect; past it `print()` raises MemoryError.
const PRINT_BYTES: usize = 64 * 1024;
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
    let request = match serde_json::from_str::<Request>(input) {
        Ok(request) => request,
        Err(e) => return Response::Refused { error: format!("unreadable request: {e}") },
    };
    match request {
        Request::Start { script, args, limits, clock, seed } => {
            if let Err(error) = limits.check() {
                return Response::Refused { error };
            }
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
        MontyObject::function("__ants_phase", None),
        MontyObject::function("__ants_log", None),
    ];
    let progress = run.start(inputs, tracker, PrintWriter::CollectString(&mut session.print, Some(PRINT_BYTES)));
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
        let progress =
            waiting.resume(vec![(call, result)], PrintWriter::CollectString(&mut session.print, Some(PRINT_BYTES)));
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
        let Session { events, calls, limits, .. } = session;
        match self {
            Self::Done(result) => Response::Done { result, calls, events },
            Self::Failed(error) => Response::Failed { error, calls, events },
            Self::Waiting(waiting) => {
                let pending = waiting.pending_call_ids().to_vec();
                let snapshot = match serde_json::to_string(&RunProgress::ResolveFutures(*waiting)) {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        return Response::Failed { error: format!("cannot dump the session: {e}"), calls, events };
                    }
                };
                let limit = limits.dump_bytes;
                if snapshot.len() > limit {
                    let size = snapshot.len();
                    return Response::Failed {
                        error: format!("the dumped session of {size} bytes passes the limit of {limit} bytes"),
                        calls,
                        events,
                    };
                }
                match RawValue::from_string(snapshot) {
                    Ok(snapshot) => Response::Suspended { snapshot, calls, pending, events },
                    Err(e) => Response::Failed { error: format!("cannot dump the session: {e}"), calls, events },
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
    events: Vec<Event>,
    print: String,
}

impl Session {
    fn new(limits: Limits, seed: u64, steps: u32) -> Self {
        Self { limits, seed, steps, calls: Vec::new(), events: Vec::new(), print: String::new() }
    }

    fn failed(&mut self, exception: MontyException) -> Response {
        self.flush(true);
        let (calls, events) = (std::mem::take(&mut self.calls), std::mem::take(&mut self.events));
        Response::Failed { error: trail::render(exception), calls, events }
    }

    /// Runs the script until it waits on its steps, ends or raises.
    fn drive(&mut self, mut progress: Result<RunProgress, MontyException>) -> Outcome {
        loop {
            progress = match progress {
                Err(e) => return Outcome::Failed(trail::render(e)),
                Ok(RunProgress::Complete(result)) => {
                    return match from_monty(result.as_ref()) {
                        Ok(result) => Outcome::Done(result),
                        Err(e) => Outcome::Failed(format!("the trail's result is not JSON: {e}")),
                    };
                }
                Ok(RunProgress::ResolveFutures(waiting)) => return Outcome::Waiting(Box::new(waiting)),
                Ok(RunProgress::FunctionCall(call)) => self.call(call),
                Ok(RunProgress::OsCall(call)) => self.os(call),
                Ok(RunProgress::NameLookup(lookup)) => lookup.resume(
                    NameLookupResult::Undefined,
                    PrintWriter::CollectString(&mut self.print, Some(PRINT_BYTES)),
                ),
            };
        }
    }

    fn call(&mut self, call: FunctionCall) -> Result<RunProgress, MontyException> {
        let function = match call.function_name.as_str() {
            "__ants_agent" => Some(Function::Agent),
            "__ants_human" => Some(Function::Human),
            "__ants_trail" => Some(Function::Trail),
            "__ants_phase" | "__ants_log" => {
                let text = call.args.arg(0).and_then(|a| a.as_str()).unwrap_or_default().to_owned();
                self.flush(false);
                self.events.push(if call.function_name == "__ants_phase" {
                    Event::Phase(text)
                } else {
                    Event::Log(text)
                });
                return call
                    .resume(MontyObject::none(), PrintWriter::CollectString(&mut self.print, Some(PRINT_BYTES)));
            }
            _ => None,
        };
        let result = match function {
            None => ExtFunctionResult::NotFound(call.function_name.clone()),
            Some(_) if self.steps >= self.limits.steps => ExtFunctionResult::Error(MontyException::new(
                ExcType::RuntimeError,
                Some(format!("the run reached its limit of {} steps", self.limits.steps)),
            )),
            Some(function) => match call.args.arg(0).map(from_monty).transpose() {
                Ok(args) => {
                    self.steps += 1;
                    let args = args.unwrap_or_default();
                    self.calls.push(Call { call: call.call_id, function, args });
                    return call.resume_pending(PrintWriter::CollectString(&mut self.print, Some(PRINT_BYTES)));
                }
                Err(e) => ExtFunctionResult::Error(MontyException::new(
                    ExcType::TypeError,
                    Some(format!("{}() takes JSON values: {e}", function.name())),
                )),
            },
        };
        call.resume(result, PrintWriter::CollectString(&mut self.print, Some(PRINT_BYTES)))
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
        call.resume(result, PrintWriter::CollectString(&mut self.print, Some(PRINT_BYTES)))
    }

    /// Turns the complete lines `print()` wrote, and with `all` the rest too, into log events.
    fn flush(&mut self, all: bool) {
        let end = if all { self.print.len() } else { self.print.rfind('\n').map_or(0, |i| i + 1) };
        if end == 0 {
            return;
        }
        let printed: String = self.print.drain(..end).collect();
        self.events.extend(printed.lines().map(|line| Event::Log(line.to_owned())));
    }
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
