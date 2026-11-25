//! Trails, as their conductor runs them: the daemon of the ant a run is started on runs its script, one worker
//! process per resume ([`ants_trail`]), and turns each call it makes into a step, a message on a thread of its own:
//! agent() to an ant, the conductor itself included, and human() to the run's owner as a question. The reply resolving
//! the step's call (`steps::answered_by`) is taken here, not prompted to the conductor's harness, and acked
//! once its journal entry is stored. A run's records (`store`) let a restarted daemon continue it (`load`).
//!
//! This module and its submodules alone name [`ants_trail`] in the daemon.

mod answers;
mod asking;
mod children;
mod control;
mod driver;
mod invite;
pub mod keeper;
mod lifecycle;
mod limits;
mod load;
mod nested;
mod ratings;
mod report;
mod route;
mod saved;
mod schema;
mod steps;
mod store;
mod tips;
mod work;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use ants_proto::provider::Origin;
use ants_proto::{
    CallAnswer, JsDomain, PendingCall, QueuedCall, RunId, RunLimit, RunState, SpendsFrom, ThreadId, TrailFunction,
    TrailLimits, TrailRun, TrailSession, TrailSource, UserId,
};
use ants_trail::{Limits, Run, Worker};
use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::{Semaphore, mpsc};

use crate::mcp::{Colony, Outbox};
use crate::people::humans::Registry;
use crate::records::direct_kv::DirectKv;
use crate::stream::notes::NotesStore;
use lifecycle::Leg;
use saved::Saved;
use store::Store;

pub use driver::MAX_RECORD;
#[cfg(all(test, unix))]
pub(crate) use invite::invitation;
pub use invite::{EndWait, rating_hint};
pub use limits::AskedLimits;
pub use nested::{refused_on_route, within};
pub use ratings::{Rated, RatedTrail, Ratings, colony_rated};
pub(crate) use report::clip;
pub use saved::{PreparedSave, SaveFrom, SaveOutcome, SaveTrail, SavedEntry, SavedScript};
pub(crate) use saved::{age, from_run};
pub(crate) use steps::indented;
pub use tips::Tips;

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
    pub saves: SaveThrough,
    /// Whether the conductor is paused: then it starts no run and its runs' passes wait, sending no step.
    pub pause: crate::pause::Pause,
}

/// How a daemon saves and removes its ant's trails in [`ants_proto::TRAILS_BUCKET`], which ant credentials write
/// nothing of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveThrough {
    /// Asking `ants signer` ([`keeper`]).
    Signer,
    /// Writing the bucket as `ants signer` would: on a server without credentials, where nothing signs.
    Bucket,
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
    /// The step the run is started answering, with `run_trail` on the step's thread, `thread`: the run spends from the
    /// step's run, its limits cut to what the step's grant leaves after the runs already started answering it.
    pub spends_from: Option<SpendsFrom>,
    /// The branch the work so far is on as the run starts: that of `thread`, holding commits the repository's `HEAD`
    /// lacks, when the run is started with `run_trail` from a thread in a worktree. Its agent() steps name it to merge
    /// until a step's reply names another ([`ants_proto::TrailRun::work_branch`]).
    pub work_branch: Option<ants_proto::WorkBranch>,
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
        let label = (call.function == TrailFunction::Agent).then(|| call.function.label(&call.args)).flatten();
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
    tips: Tips,
    ratings: Ratings,
    humans: Registry,
    colony: Colony,
    outbox: Outbox,
    attempts: DirectKv,
    notes: Arc<dyn NotesStore>,
    pause: crate::pause::Pause,
    /// The team directory, naming the leads agent(team=) steps go to; None when it cannot be named.
    teams: Option<DirectKv>,
    /// The conductor's escalator, asking a human() step's question of the owner's assistant first; unset asks the
    /// owner ([`Trails::with_escalator`]).
    escalator: OnceLock<Arc<crate::escalation::Escalator>>,
    /// The runs not finished: running, waiting or stopped.
    runs: Mutex<HashMap<RunId, Arc<Slot>>>,
    /// The threads of steps, to the run waiting on them; or, once answered or given up, to the step's recipient, so
    /// a further reply of it is reported on the run's thread rather than prompted. A run's threads are forgotten as it
    /// ends: its [`ants_proto::TrailStepThread`]s tell its steps' threads from then on.
    threads: Mutex<HashMap<ThreadId, Step>>,
    /// The runs of the conductor's index not continued from their records yet ([`load`]).
    unrestored: Mutex<load::Unrestored>,
    wake: mpsc::UnboundedSender<RunId>,
    woken: tokio::sync::Mutex<mpsc::UnboundedReceiver<RunId>>,
    /// The run_trail calls waiting on the end of the runs they started ([`EndWait`]).
    waiters: Mutex<HashMap<RunId, tokio::sync::oneshot::Sender<TrailRun>>>,
    /// Held while a run answering a step is started or resumed within the step's grant.
    answering: tokio::sync::Mutex<()>,
    /// The runs being stopped, resumed or restored, one stop, resume or restoring of a run at a time
    /// ([`Trails::changing`]).
    changing: Mutex<HashMap<RunId, Arc<tokio::sync::Mutex<()>>>>,
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
    /// The session as last stored: [`ants_trail::Session`] as JSON.
    session: String,
    pending: Vec<PendingCall>,
    queued: Vec<QueuedCall>,
    /// The steps of the leg the run left by a goto(), still to give up once its stored header lists the next leg.
    left: Vec<PendingCall>,
    /// The calls whose steps counted against the run's steps before it stopped or failed on a limit and go out anew
    /// since its resume, counting no step again ([`TrailSession::anew`]).
    anew: Vec<u32>,
    /// Answers whose journal entries are stored, for the next pass to hand the script.
    answers: Vec<(u32, CallAnswer)>,
    /// The limit one of those answers raises for: a child run's, failed on it.
    broke: Option<RunLimit>,
    /// Whether the wall time passed with the run woken for it.
    late: bool,
    /// Whether a call waits for turns the steps of other runs of its tree hold.
    starved: bool,
    /// The run's workers that crashed in a row.
    crashes: u32,
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

/// The description in `script`'s meta, when the script is a trail.
pub(crate) fn script_description(script: &str) -> Option<String> {
    ants_trail::check(script).ok().map(|meta| meta.description)
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
            saves,
            pause,
        } = conducting;
        COMPUTING.get_or_init(|| Semaphore::new(workers.clamp(1, MAX_WORKERS)));
        let store = Store::new(&js, js_domain.as_ref(), ant.clone()).await?;
        let saved = Saved::new(&js, js_domain.as_ref(), trails_dir, ant.clone(), saves).await;
        let tips = Tips::new(&js, js_domain.as_ref()).await?;
        let ratings = Ratings::new(&js, js_domain.as_ref(), ant.clone()).await?;
        let teams = crate::teams::open(&js, js_domain.as_ref()).await.ok();
        let (wake, woken) = mpsc::unbounded_channel();
        let inner = Inner {
            ant,
            client,
            worker: Worker::new(worker),
            store,
            saved,
            tips,
            ratings,
            humans,
            colony,
            outbox,
            attempts,
            notes,
            pause,
            teams,
            escalator: OnceLock::new(),
            runs: Mutex::default(),
            threads: Mutex::default(),
            unrestored: Mutex::default(),
            wake,
            woken: tokio::sync::Mutex::new(woken),
            waiters: Mutex::default(),
            answering: tokio::sync::Mutex::new(()),
            changing: Mutex::default(),
        };
        Ok(Self { inner: Arc::new(inner) })
    }

    pub fn ant(&self) -> &UserId {
        &self.inner.ant
    }

    /// Run the saved trail `name` with `args` as a watcher's check, answering its reads with `read`: the script's
    /// result ([`ants_trail::watch::check`]).
    pub(crate) async fn check_watcher<'r>(
        &self,
        name: &str,
        args: serde_json::Value,
        read: impl Fn(
            ants_proto::TrailFunction,
            serde_json::Value,
        ) -> futures::future::BoxFuture<'r, ants_proto::CallAnswer>
        + Send
        + Sync,
    ) -> Result<serde_json::Value, String> {
        let name: ants_proto::TrailName = name.parse().map_err(|e| format!("no trail is named {name:?}: {e}"))?;
        let script = self.inner.saved.script(&name).await?.script;
        let computing = || async { computing().acquire().await.ok() };
        ants_trail::watch::check(&self.inner.worker, &script, args, read, computing).await
    }

    /// The tips of the colony's trails, read, left and voted on under the ant's credentials.
    pub fn tips(&self) -> &Tips {
        &self.inner.tips
    }

    /// Whether `other` is a clone of these trails.
    pub fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
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

    /// The runs held, by state, and the questions their `human()` steps sent wait on, for the metrics.
    pub fn measured(&self) -> crate::monitoring::metrics::Runs {
        let slots: Vec<Arc<Slot>> = locked(&self.inner.runs).values().cloned().collect();
        let mut runs = crate::monitoring::metrics::Runs::default();
        for slot in slots {
            let book = slot.book();
            match book.header.state {
                RunState::Running => runs.running += 1,
                RunState::Waiting => runs.waiting += 1,
                RunState::Stopped { .. } => runs.stopped += 1,
                RunState::Done { .. } | RunState::Failed { .. } => {}
            }
            runs.questions +=
                book.pending.iter().filter(|call| call.function == TrailFunction::Human && call.sent).count();
        }
        runs
    }

    /// Serve the conductor's runs, continued first ([`Trails::restore`]): hear requests and drive runs, never ending.
    pub async fn serve(&self) {
        let mut hearing = std::pin::pin!(self.hear());
        let mut driving = std::pin::pin!(self.drive_all());
        let mut restoring = std::pin::pin!(self.restore_again());
        tokio::select! {
            () = &mut hearing => {}
            () = &mut driving => {}
            () = &mut restoring => {}
        }
    }

    /// The run `run` in memory, not finished.
    async fn running(&self, run: &RunId) -> Result<Arc<Slot>, String> {
        if let Some(slot) = self.restored(run).await.map_err(|e| format!("{e:#}"))? {
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

    /// Run `body` as the one stop, resume or restoring of run `run` under way ([`Trails::changing`]): at once when the
    /// task runs it within one already, else after waiting for the one under way, if any, or None when `wait` is false
    /// and one is.
    async fn holding<T>(&self, run: &RunId, wait: bool, body: impl Future<Output = T>) -> Option<T> {
        let mut held = HOLDING.try_with(Clone::clone).unwrap_or_default();
        if held.contains(run) {
            return Some(body.await);
        }
        let _changing = self.changing(run, wait).await?;
        held.push(run.clone());
        Some(HOLDING.scope(held, body).await)
    }

    /// Wait for the stop, resume or restoring of run `run` under way, if any, or give up on it when `wait` is false,
    /// and hold off any other until the returned guard drops.
    async fn changing(&self, run: &RunId, wait: bool) -> Option<Changing> {
        let lock = locked(&self.inner.changing).entry(run.clone()).or_default().clone();
        let guard = if wait { lock.clone().lock_owned().await } else { lock.clone().try_lock_owned().ok()? };
        Some(Changing { trails: self.clone(), run: run.clone(), lock, guard: Some(guard) })
    }

    async fn save_session(&self, slot: &Slot) -> Result<()> {
        self.save_session_with(slot, None).await
    }

    /// Store the session of the run of `slot`, with `call` in place of the pending call of its id when given.
    async fn save_session_with(&self, slot: &Slot, call: Option<&PendingCall>) -> Result<()> {
        let _held = slot.writes.lock().await;
        let (run, session) = {
            let book = slot.book();
            let mut pending = book.pending.clone();
            if let Some(call) = call {
                pending
                    .iter_mut()
                    .filter(|pending| pending.call == call.call)
                    .for_each(|pending| *pending = call.clone());
            }
            let session = TrailSession {
                session: book.session.clone(),
                pending,
                queued: book.queued.clone(),
                anew: book.anew.clone(),
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

tokio::task_local! {
    /// The runs whose stop, resume or restoring the task runs within ([`Trails::holding`]).
    static HOLDING: Vec<RunId>;
}

/// A stop, resume or restoring of a run under way ([`Trails::changing`]).
struct Changing {
    trails: Trails,
    run: RunId,
    lock: Arc<tokio::sync::Mutex<()>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for Changing {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut changing = locked(&self.trails.inner.changing);
        // Held by the map and this alone: no other stop, resume or restoring of the run waits.
        if Arc::strong_count(&self.lock) == 2 {
            changing.remove(&self.run);
        }
    }
}
