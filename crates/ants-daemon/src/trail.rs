//! Trails, as their conductor runs them: the daemon of the ant a run is started on runs its script, one worker
//! process per resume ([`ants_trail`]), and turns each call it makes into a step, a message on a thread of its own:
//! agent() to an ant, the conductor itself included, and human() to the run's owner as a question. The reply resolving
//! the step's call ([`PendingCall::answered_by`]) is taken here, not prompted to the conductor's harness, and acked
//! once its journal entry is stored. A run's records ([`store`]) let a restarted daemon continue it ([`load`]).
//!
//! This module alone names [`ants_trail`] in the daemon.

mod children;
mod control;
mod driver;
mod limits;
mod load;
mod report;
mod route;
mod saved;
mod schema;
mod steps;
mod store;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    Envelope, JsDomain, MAX_TRAIL_ARGS, MAX_TRAIL_SCRIPT, NoticeKind, PendingCall, QueuedCall, Role, RunId, RunState,
    StepStage, ThreadId, TrailFunction, TrailLeg, TrailLimits, TrailName, TrailRun, TrailSession, TrailSource,
    TrailStart, UserId, WaitingStep,
};
use ants_trail::{Answer, JournalEntry, Limits, Run, Worker};
use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::{Semaphore, mpsc};

use crate::mcp::{Colony, Outbox};
use crate::people::humans::Registry;
use crate::records::direct_kv::DirectKv;
use crate::stream::notes::NotesStore;
use report::Stray;
use saved::Saved;
use steps::Taken;
use store::Store;

pub use limits::AskedLimits;

/// How to write a trail, in brief, for the tool starting runs to teach.
pub const GUIDE: &str = ants_trail::GUIDE;

/// Workers computing at once across the process's runs, unless `[trails] workers` sets otherwise.
pub const DEFAULT_WORKERS: usize = 4;
/// Most workers `[trails] workers` may set.
pub const MAX_WORKERS: usize = 16;

static COMPUTING: OnceLock<Semaphore> = OnceLock::new();

/// The workers the process's runs compute in: as many as the first conductor started asked for.
fn computing() -> &'static Semaphore {
    COMPUTING.get_or_init(|| Semaphore::new(DEFAULT_WORKERS))
}

/// What a daemon conducts trails with.
pub struct Conducting {
    pub js: async_nats::jetstream::Context,
    pub js_domain: Option<JsDomain>,
    pub client: async_nats::Client,
    /// The conductor: the daemon's ant.
    pub ant: UserId,
    /// The `ants` binary, run as `ants subprocess` for each resume of a script.
    pub worker: PathBuf,
    /// The machine's saved trails, `~/.ants/trails`.
    pub trails_dir: Option<PathBuf>,
    pub humans: Registry,
    pub colony: Colony,
    pub outbox: Outbox,
    /// The conductor's attempts bucket, where it withdraws its own steps.
    pub attempts: DirectKv,
    /// Workers computing at once across the process's runs, 1 to [`MAX_WORKERS`]: the first conductor's count holds.
    pub workers: usize,
    /// The thread notes, where a run reports on its thread.
    pub notes: Arc<dyn NotesStore>,
}

/// A run to start: what the MCP tool, a schedule and a human's [`ants_proto::TrailRequest`] ask for.
#[derive(Debug, Clone)]
pub struct StartRun {
    pub trail: TrailSource,
    pub args: Value,
    /// The human the run answers to: the owner of the thread starting it. Its steps' threads carry it on, and its
    /// human() steps ask it (the colony's default human when none).
    pub owner: Option<UserId>,
    /// Where the starting thread came from: a run from an untrusted thread runs only saved trails, and its steps are
    /// as untrusted as that thread.
    pub origin: Origin,
    /// The thread the run reports on.
    pub thread: ThreadId,
    pub limits: TrailLimits,
}

/// How a run stands ([`Trails::status`]).
#[derive(Debug, Clone)]
pub struct RunStatus {
    pub header: TrailRun,
    /// The steps it waits on: none once it is not in memory, as after it ended.
    pub waiting: Vec<Waited>,
    /// The calls of its script waiting for room to go out as steps.
    pub queued: usize,
}

/// A step a run waits on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Waited {
    pub call: u32,
    pub function: TrailFunction,
    /// The ant, or human, the step went to.
    pub to: UserId,
    /// What the script named the step.
    pub label: Option<String>,
    /// Whether its message went out: a stopped run's steps go out anew on resuming.
    pub sent: bool,
}

impl Waited {
    fn of(call: &PendingCall) -> Self {
        let label = (call.function == TrailFunction::Agent)
            .then(|| steps::AgentArgs::of(&call.args).ok().and_then(|args| args.label))
            .flatten();
        Self { call: call.call, function: call.function, to: call.to.clone(), label, sent: call.sent }
    }
}

/// The runs of a conductor. Cheap to clone.
#[derive(Clone)]
pub struct Trails {
    inner: Arc<Inner>,
}

struct Inner {
    ant: UserId,
    client: async_nats::Client,
    worker: Worker,
    store: Store,
    saved: Saved,
    humans: Registry,
    colony: Colony,
    outbox: Outbox,
    attempts: DirectKv,
    notes: Arc<dyn NotesStore>,
    /// The runs not finished: running, waiting or stopped.
    runs: Mutex<HashMap<RunId, Arc<Slot>>>,
    /// The threads of steps, to the run waiting on them; or, once answered or given up, to the step's recipient, so
    /// a further reply of it is reported on the run's thread rather than prompted. A run's threads are forgotten as it
    /// ends: its [`ants_proto::TrailStepThread`]s tell its steps' threads from then on.
    threads: Mutex<HashMap<ThreadId, Step>>,
    wake: mpsc::UnboundedSender<RunId>,
    woken: tokio::sync::Mutex<mpsc::UnboundedReceiver<RunId>>,
}

#[derive(Clone)]
enum Step {
    Live(RunId),
    Retired { run: RunId, to: UserId },
}

/// A run in memory.
struct Slot {
    book: Mutex<Book>,
    /// Held while the script computes, so one pass at a time drives the run.
    engine: tokio::sync::Mutex<Run>,
    /// Held while the run's records are put, so a later state never loses to an earlier one.
    writes: tokio::sync::Mutex<()>,
}

/// What a run's records hold.
struct Book {
    header: TrailRun,
    /// The session as last stored: [`ants_trail::RunState`] as JSON.
    session: String,
    pending: Vec<PendingCall>,
    queued: Vec<QueuedCall>,
    /// Answers whose journal entries are stored, for the next pass to hand the script.
    answers: Vec<(u32, Answer)>,
    /// Whether the wall time passed with the run woken for it.
    late: bool,
    /// Whether a call waits for turns the steps of other runs of its tree hold.
    starved: bool,
}

impl Slot {
    fn new(engine: Run, book: Book) -> Arc<Self> {
        Arc::new(Self {
            book: Mutex::new(book),
            engine: tokio::sync::Mutex::new(engine),
            writes: tokio::sync::Mutex::new(()),
        })
    }

    fn book(&self) -> MutexGuard<'_, Book> {
        self.book.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn header(&self) -> TrailRun {
        self.book().header.clone()
    }
}

/// A leg to start: a run of `trail`'s script, entered at the waypoint `at`, else at its start.
struct Leg {
    trail: Option<TrailName>,
    script: String,
    at: Option<String>,
    args: Value,
    /// The id of the leg's first call: past those of the run's legs before ([`TrailLeg::first_call`]).
    first_call: u32,
}

impl Leg {
    /// The leg, checked to fit, as the run's route records it, its run, and its session as JSON.
    fn prepare(self, limits: &TrailLimits) -> Result<(TrailLeg, Run, String), String> {
        let Self { trail, script, at, args, first_call } = self;
        if script.len() > MAX_TRAIL_SCRIPT {
            return Err(format!("the script has {} bytes; a trail takes at most {MAX_TRAIL_SCRIPT}", script.len()));
        }
        let size = serde_json::to_vec(&args).map_or(usize::MAX, |args| args.len());
        if size > MAX_TRAIL_ARGS {
            return Err(format!("the arguments have {size} bytes of JSON; a run takes at most {MAX_TRAIL_ARGS}"));
        }
        let engine = Run::new(&script, at.as_deref(), args.clone(), engine_limits(limits))
            .map_err(|e| e.to_string())?
            .numbered_from(first_call);
        let session = serde_json::to_string(engine.state()).map_err(|e| e.to_string())?;
        let at = engine.state().entry().map(str::to_owned);
        let leg = TrailLeg { trail, at, script, args, first_call, started_at: Utc::now() };
        Ok((leg, engine, session))
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The engine's limits for a run under `limits`.
fn engine_limits(limits: &TrailLimits) -> Limits {
    Limits {
        steps: limits.steps,
        concurrency: limits.concurrency,
        compute_ms: limits.compute_ms,
        dump_bytes: usize::try_from(limits.session_bytes).unwrap_or(usize::MAX),
        ..Limits::default()
    }
}

impl Trails {
    pub async fn new(conducting: Conducting) -> Result<Self> {
        let Conducting {
            js,
            js_domain,
            client,
            ant,
            worker,
            trails_dir,
            humans,
            colony,
            outbox,
            attempts,
            workers,
            notes,
        } = conducting;
        COMPUTING.get_or_init(|| Semaphore::new(workers.clamp(1, MAX_WORKERS)));
        let store = Store::new(&js, js_domain.as_ref(), ant.clone()).await?;
        let saved = Saved::new(&js, js_domain.as_ref(), trails_dir).await;
        let (wake, woken) = mpsc::unbounded_channel();
        let inner = Inner {
            ant,
            client,
            worker: Worker::new(worker),
            store,
            saved,
            humans,
            colony,
            outbox,
            attempts,
            notes,
            runs: Mutex::default(),
            threads: Mutex::default(),
            wake,
            woken: tokio::sync::Mutex::new(woken),
        };
        Ok(Self { inner: Arc::new(inner) })
    }

    pub fn ant(&self) -> &UserId {
        &self.inner.ant
    }

    /// Whether `other` is a clone of these trails.
    pub fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Start a run: its records are stored and its first pass is due when this returns.
    pub async fn start(&self, start: StartRun) -> Result<RunId, String> {
        self.start_as(RunId::generate(), start).await
    }

    /// Start the run of the saved trail `start` that `firing`, a schedule's message to the conductor, asks for: on the
    /// firing's thread, answering to its owner, under the limits it asks for. The run takes the firing's id, so a firing
    /// delivered again starts none.
    pub async fn start_fired(&self, firing: &Envelope, start: &TrailStart) -> Result<RunId, String> {
        let run =
            firing.id.to_string().parse::<RunId>().map_err(|e| format!("the firing's id makes no run id: {e}"))?;
        if self.run(&run).await?.is_some() {
            return Ok(run);
        }
        let start = StartRun {
            trail: TrailSource::Saved(start.trail.clone()),
            args: start.args.clone(),
            owner: firing.owner.clone(),
            origin: crate::provider::context::origin(firing, None, &self.inner.humans.humans()),
            thread: firing.thread_id.clone(),
            limits: start.limits,
        };
        self.start_as(run, start).await
    }

    /// Run `run` as held or stored: None when the conductor has no such run.
    pub async fn run(&self, run: &RunId) -> Result<Option<TrailRun>, String> {
        let held = locked(&self.inner.runs).get(run).map(|slot| slot.header());
        match held {
            Some(header) => Ok(Some(header)),
            None => self.inner.store.header(run).await.map_err(|e| format!("reading the run: {e:#}")),
        }
    }

    /// How run `run` stands: refused when the conductor has no such run, as a run of another conductor is kept under
    /// that conductor's keys.
    pub async fn status(&self, run: &RunId) -> Result<RunStatus, String> {
        let held = locked(&self.inner.runs).get(run).cloned();
        if let Some(slot) = held {
            let book = slot.book();
            let waiting = book.pending.iter().map(Waited::of).collect();
            return Ok(RunStatus { header: book.header.clone(), waiting, queued: book.queued.len() });
        }
        match self.inner.store.header(run).await {
            Ok(Some(header)) => Ok(RunStatus { header, waiting: Vec::new(), queued: 0 }),
            Ok(None) => Err(format!("{} conducts no run {run}", self.inner.ant)),
            Err(e) => Err(format!("reading the run: {e:#}")),
        }
    }

    /// The runs not finished, running, waiting or stopped, oldest first.
    pub fn unfinished(&self) -> Vec<TrailRun> {
        let slots: Vec<Arc<Slot>> = locked(&self.inner.runs).values().cloned().collect();
        let mut runs: Vec<TrailRun> = slots.iter().map(|slot| slot.header()).collect();
        runs.sort_by(|a, b| (a.started_at, a.run.as_str()).cmp(&(b.started_at, b.run.as_str())));
        runs
    }

    async fn start_as(&self, run: RunId, start: StartRun) -> Result<RunId, String> {
        let StartRun { trail, args, owner, origin, thread, limits } = start;
        limits.check().map_err(|e| e.to_string())?;
        let (name, script) = match trail {
            TrailSource::Saved(name) => {
                let script = self.inner.saved.script(&name).await?;
                (Some(name), script)
            }
            TrailSource::Script(_) if !origin.trusted() => {
                return Err("a run started from a webhook's thread runs only saved trails".to_owned());
            }
            TrailSource::Script(script) => (None, script),
        };
        let (leg, engine, session) = Leg { trail: name, script, at: None, args, first_call: 0 }.prepare(&limits)?;
        let now = leg.started_at;
        let header = TrailRun {
            run,
            conductor: self.inner.ant.clone(),
            route: vec![leg],
            owner,
            thread,
            parent: None,
            origin,
            limits,
            state: RunState::Running,
            phase: None,
            steps: 0,
            turns: 0,
            started_at: now,
            updated_at: now,
            ended_at: None,
        };
        let run = header.run.clone();
        self.launch(header, engine, session).await.map_err(|e| format!("storing the run: {e:#}"))?;
        Ok(run)
    }

    /// Store the run of `header` and `engine`, its session `session`, and let its first pass come.
    async fn launch(&self, header: TrailRun, engine: Run, session: String) -> Result<()> {
        let run = header.run.clone();
        let started = header.clone();
        let meta = engine.state().meta().clone();
        let book = Book {
            header,
            session,
            pending: Vec::new(),
            queued: Vec::new(),
            answers: Vec::new(),
            late: false,
            starved: false,
        };
        let slot = Slot::new(engine, book);
        self.save_session(&slot).await?;
        self.save_header(&slot).await?;
        self.inner.store.list(&run, true).await?;
        self.report_start(&started, &meta).await;
        locked(&self.inner.runs).insert(run.clone(), slot);
        self.wake(&run);
        tracing::info!(run = %run, parent = ?started.parent.map(|parent| parent.run), "trail run started");
        Ok(())
    }

    /// Start a run a human asks for on `thread`, which the human owns.
    pub async fn start_for(
        &self,
        by: &UserId,
        trail: TrailSource,
        args: Value,
        thread: ThreadId,
        limits: TrailLimits,
    ) -> Result<RunId, String> {
        if !self.inner.humans.is_human(by) {
            return Err(format!("{by} is no human of the colony"));
        }
        let origin = Origin { by: Starter::Human { user: by.clone() }, channel: None };
        self.start(StartRun { trail, args, owner: Some(by.clone()), origin, thread, limits }).await
    }

    /// Stop a run for `by`: it sends no further step, ignores the replies of the steps it waited on and withdraws
    /// those it sent itself, and its child runs stop too. Returns the steps to other ants they waited on, for `by` to
    /// withdraw.
    pub async fn stop(&self, run: &RunId, by: &UserId) -> Result<Vec<WaitingStep>, String> {
        let slot = self.running(run).await?;
        let pending = {
            let mut book = slot.book();
            if let RunState::Stopped { by } = &book.header.state {
                return Err(format!("{by} already stopped run {run}"));
            }
            book.header.state = RunState::Stopped { by: by.clone() };
            book.header.updated_at = Utc::now();
            let pending = book.pending.clone();
            for call in book.pending.iter_mut().filter(|call| call.function != TrailFunction::Trail) {
                call.sent = false;
            }
            pending
        };
        let header = slot.header();
        let mut waiting = self.stop_children(&pending, by).await;
        for call in pending.iter().filter(|call| call.function != TrailFunction::Trail) {
            self.retire(run, call);
            if call.sent {
                self.report_step(&header, call, StepStage::Dropped);
            }
            if !call.sent || call.function == TrailFunction::Human {
                continue;
            }
            if call.to == self.inner.ant {
                let withdrawn =
                    crate::delivery::attempts::withdraw(&self.inner.attempts, &self.inner.ant, &call.message, by, None);
                if let Err(e) = withdrawn.await {
                    tracing::warn!(run = %run, call = call.call, error = format!("{e:#}"), "withdrawing a step");
                }
            } else {
                waiting.push(WaitingStep { to: call.to.clone(), message: call.message });
            }
        }
        let stored = async {
            self.save_session(&slot).await?;
            self.save_header(&slot).await
        };
        stored.await.map_err(|e| format!("storing the run: {e:#}"))?;
        self.report_end(&slot.header()).await;
        tracing::info!(run = %run, by = %by, "trail run stopped");
        Ok(waiting)
    }

    /// Resume a stopped run, or one failed on a limit, under `limits` when given: from its stored records, with the
    /// steps it waited on sent anew and its stopped child runs resumed.
    pub async fn resume(&self, run: &RunId, by: &UserId, limits: Option<TrailLimits>) -> Result<(), String> {
        let stored = |e: anyhow::Error| format!("reading the run: {e:#}");
        let held = locked(&self.inner.runs).get(run).cloned();
        if let Some(slot) = &held {
            // A pass in flight ends before the records are read again.
            drop(slot.engine.lock().await);
        }
        let header = self.inner.store.header(run).await.map_err(stored)?;
        let mut header = header.ok_or_else(|| format!("{} conducts no run {run}", self.inner.ant))?;
        match &header.state {
            RunState::Stopped { .. } => {}
            RunState::Failed { error } if error.contains("limit") => {}
            RunState::Failed { error } => return Err(format!("run {run} failed, not on a limit: {error}")),
            RunState::Done { .. } => return Err(format!("run {run} is done")),
            RunState::Running | RunState::Waiting => return Err(format!("run {run} is not stopped")),
        }
        if let Some(limits) = limits {
            limits.check().map_err(|e| e.to_string())?;
            header.limits = limits;
        }
        header.state = RunState::Running;
        header.ended_at = None;
        header.updated_at = Utc::now();
        let slot = self.load(header, true).await.map_err(stored)?;
        if let Some(held) = held {
            for call in &held.book().pending {
                self.retire(run, call);
            }
        }
        let saved = async {
            self.save_session(&slot).await?;
            self.save_header(&slot).await?;
            self.inner.store.list(run, true).await
        };
        saved.await.map_err(|e| format!("storing the run: {e:#}"))?;
        self.report_resumed(&slot.header(), by).await;
        locked(&self.inner.runs).insert(run.clone(), slot.clone());
        self.wake(run);
        tracing::info!(run = %run, by = %by, "trail run resumed");
        let (header, calls) = {
            let book = slot.book();
            (book.header.clone(), book.pending.clone())
        };
        self.resume_children(&header, &calls, by).await;
        Ok(())
    }

    /// Take `message` when it arrived on a step's thread: the reply answering the step's call, resolving it; a
    /// notice or escalation, or a further reply of the step's recipient once the run no longer waits on the step,
    /// reported on the run's thread. True when taken, to be acked without prompting the harness, once this returns.
    pub async fn take(&self, message: &Envelope) -> Result<bool> {
        let step = locked(&self.inner.threads).get(&message.thread_id).cloned();
        let answerer = self.answerer(message);
        let run = match step {
            None => return self.take_late(message, answerer).await,
            Some(Step::Retired { run, to }) => return Ok(self.take_retired(&run, &to, message, answerer).await),
            Some(Step::Live(run)) => run,
        };
        let Some(slot) = locked(&self.inner.runs).get(&run).cloned() else { return Ok(false) };
        let (header, pending) = {
            let book = slot.book();
            let pending = book.pending.iter().find(|call| call.thread == message.thread_id).cloned();
            (book.header.clone(), pending)
        };
        let Some(pending) = pending else { return Ok(false) };
        if !pending.answered_by(message, answerer) {
            if message.notice.is_none() && message.escalation.is_none() {
                return Ok(false);
            }
            let call = Some(pending.call);
            let stray = Stray { run: &run, run_thread: &header.thread, call, to: &pending.to, waited: true };
            self.report_stray(stray, message).await;
            let withdrawn = message.notice == Some(NoticeKind::Withdrawn)
                && message.from == pending.to
                && message.in_reply_to == Some(pending.message);
            if withdrawn {
                let error = format!("{} never answered: the step's message was withdrawn from its queue", pending.to);
                self.resolve(&slot, &header, &pending, Answer::Error(error), 0).await?;
            }
            return Ok(true);
        }
        let spent = match pending.function {
            TrailFunction::Agent => pending.turn_budget.saturating_sub(message.turn_budget),
            TrailFunction::Human | TrailFunction::Trail | TrailFunction::Goto => 0,
        };
        let answer = match steps::taken(&pending, message) {
            Taken::Answer(answer) => answer,
            Taken::Again(why) => match self.ask_again(&slot, &header, &pending, message, &why, spent).await? {
                None => return Ok(true),
                Some(answer) => answer,
            },
        };
        self.resolve(&slot, &header, &pending, answer, spent).await?;
        Ok(true)
    }

    /// Answer `pending`, a call of the run of `slot`, with `answer`, its step's thread having spent `spent` turns: its
    /// journal entry is stored when this returns, and the run woken to hand it to the script.
    async fn resolve(
        &self,
        slot: &Slot,
        header: &TrailRun,
        pending: &PendingCall,
        answer: Answer,
        spent: u32,
    ) -> Result<()> {
        let run = &header.run;
        let entry = JournalEntry {
            call: pending.call,
            function: steps::engine_function(pending.function),
            args: pending.args.clone(),
            answer,
        };
        self.inner.store.put_entry(run, &entry).await?;
        {
            let mut book = slot.book();
            book.pending.retain(|call| call.call != pending.call);
            book.header.turns = book.header.turns.saturating_add(spent);
            book.answers.push((entry.call, entry.answer.clone()));
        }
        self.charge(header, 0, spent);
        self.retire(run, pending);
        self.report_answered(header, pending, &entry.answer);
        self.wake(run);
        self.wake_starved(header);
        Ok(())
    }

    /// Take `message`, on the thread of a step to `to` the run `run` no longer waits on, when it is a notice, an
    /// escalation or a further reply of `to`, reporting it on the run's thread.
    async fn take_retired(&self, run: &RunId, to: &UserId, message: &Envelope, answerer: &UserId) -> bool {
        let reply = answerer == to && message.trail_step.is_none();
        if !reply && message.notice.is_none() && message.escalation.is_none() {
            return false;
        }
        let run_thread = locked(&self.inner.runs).get(run).map(|slot| slot.header().thread);
        if let Some(run_thread) = &run_thread {
            let step = self.inner.store.step(&message.thread_id).await;
            let call = step.ok().flatten().map(|step| step.call);
            let stray = Stray { run, run_thread, call, to, waited: false };
            self.report_stray(stray, message).await;
        }
        true
    }

    /// Take `message`, on a thread the conductor knows of no run in memory, when the thread is a step's, as its
    /// [`ants_proto::TrailStepThread`] tells, and the message a notice, an escalation, or a reply of the step's
    /// recipient: one arriving after the run ended, or after a restart, reported on the run's thread. Only a message
    /// that may be one, replying or answering, is looked up.
    async fn take_late(&self, message: &Envelope, answerer: &UserId) -> Result<bool> {
        let (notice, escalation) = (message.notice.is_some(), message.escalation.is_some());
        let replying = message.in_reply_to.is_some() || message.answer.is_some();
        if message.trail_step.is_some() || !(replying || notice || escalation) {
            return Ok(false);
        }
        let Some(step) = self.inner.store.step(&message.thread_id).await? else { return Ok(false) };
        if *answerer != step.to && !notice && !escalation {
            return Ok(false);
        }
        let stray =
            Stray { run: &step.run, run_thread: &step.run_thread, call: Some(step.call), to: &step.to, waited: false };
        self.report_stray(stray, message).await;
        Ok(true)
    }

    /// Who answers with `message`: the human its `on_behalf_of` names while the human's grant names the sender as its
    /// assistant, as an answer to ask_human counts for the human, else its sender. The inbox already cleared a claim
    /// the assistant's own credentials do not back ([`crate::people::behalf::Records::acting`]), so the claim stands
    /// for them here; the grant is checked again as it stands now.
    fn answerer<'a>(&self, message: &'a Envelope) -> &'a UserId {
        match &message.on_behalf_of {
            Some(human) if self.inner.humans.honours_assistant(human, &message.from, Some(human)) => human,
            _ => &message.from,
        }
    }

    /// Ask `pending`'s recipient again for a reply matching the step's schema, on the step's thread; None once asked,
    /// or the answer to give the call when the thread has no turns left for it.
    async fn ask_again(
        &self,
        slot: &Slot,
        header: &TrailRun,
        pending: &PendingCall,
        reply: &Envelope,
        why: &str,
        spent: u32,
    ) -> Result<Option<Answer>> {
        let schema = steps::AgentArgs::of(&pending.args).ok().and_then(|args| args.schema).unwrap_or_default();
        let again = match reply.reply(self.inner.ant.clone(), steps::again(why, &schema), Role::Ant) {
            Ok(again) => again.as_step(steps::step(header, pending)),
            Err(_) => {
                let error = format!("the reply did not match the schema, with no turns left to ask again: {why}");
                return Ok(Some(Answer::Error(error)));
            }
        };
        let update = |sent: bool| {
            let mut book = slot.book();
            if let Some(call) = book.pending.iter_mut().find(|call| call.call == pending.call) {
                call.message = again.id;
                call.sent_at = again.sent_at;
                call.turn_budget = again.turn_budget;
                call.retries = pending.retries + 1;
                call.sent = sent;
            }
            if !sent {
                book.header.turns = book.header.turns.saturating_add(spent + 1);
            }
        };
        update(false);
        self.charge(header, 0, spent + 1);
        self.save_session(slot).await?;
        (self.inner.outbox)(again.clone()).await?;
        update(true);
        self.save_ancestors(header).await?;
        self.save_session(slot).await?;
        self.save_header(slot).await?;
        tracing::info!(run = %header.run, call = pending.call, why, "trail step asked again");
        Ok(None)
    }

    /// Serve the conductor's runs, continued first ([`Trails::restore`]): hear requests and drive runs, never ending.
    pub async fn serve(&self) {
        let mut hearing = std::pin::pin!(self.hear());
        let mut driving = std::pin::pin!(self.drive_all());
        tokio::select! {
            () = &mut hearing => {}
            () = &mut driving => {}
        }
    }

    /// The run `run` in memory, not finished.
    async fn running(&self, run: &RunId) -> Result<Arc<Slot>, String> {
        if let Some(slot) = locked(&self.inner.runs).get(run).cloned() {
            return Ok(slot);
        }
        match self.inner.store.header(run).await {
            Ok(Some(header)) if header.finished() => Err(format!("run {run} has ended")),
            Ok(_) => Err(format!("{} conducts no run {run}", self.inner.ant)),
            Err(e) => Err(format!("reading the run: {e:#}")),
        }
    }

    fn wake(&self, run: &RunId) {
        // The receiver lives as long as the trails.
        let _ = self.inner.wake.send(run.clone());
    }

    /// Ignore further replies on `call`'s thread; a trail() call's is its child run's own.
    fn retire(&self, run: &RunId, call: &PendingCall) {
        if call.function == TrailFunction::Trail {
            return;
        }
        let retired = Step::Retired { run: run.clone(), to: call.to.clone() };
        locked(&self.inner.threads).insert(call.thread.clone(), retired);
    }

    /// Forget the threads of `run`, ended.
    fn forget(&self, run: &RunId) {
        locked(&self.inner.threads).retain(|_, step| match step {
            Step::Live(r) | Step::Retired { run: r, .. } => r != run,
        });
    }

    async fn save_session(&self, slot: &Slot) -> Result<()> {
        let _held = slot.writes.lock().await;
        let (run, session) = {
            let book = slot.book();
            let session = TrailSession {
                session: book.session.clone(),
                pending: book.pending.clone(),
                queued: book.queued.clone(),
            };
            (book.header.run.clone(), session)
        };
        let key = self.inner.store.session_key(&run);
        self.inner.store.put_bytes(key, serde_json::to_vec(&session)?).await
    }

    async fn save_header(&self, slot: &Slot) -> Result<()> {
        let _held = slot.writes.lock().await;
        let header = {
            let mut book = slot.book();
            book.header.updated_at = Utc::now();
            book.header.clone()
        };
        self.inner.store.put_header(&header).await
    }
}
