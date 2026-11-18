//! Driving runs: each pass hands a run's script the answers taken since the last, lets it compute in a worker, sends
//! the steps it then waits on, and stores what came of it; a run's end is stored and reported here too.

use std::collections::HashSet;
use std::time::Duration;

use ants_proto::{
    DEFAULT_TURN_BUDGET, PendingCall, RunId, RunState, StepStage, ThreadId, TrailFunction, TrailRun, TrailStepThread,
};
use ants_trail::{Call, Event, Function, JournalEntry, Progress, Run, RunError, Status, WorkerError};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt as _};
use uuid::Uuid;

use super::{Answer, Slot, Step, Trails, load, locked, steps};

/// How long a pass that failed waits before the next, at first and at most.
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_mins(1);
/// Most bytes of a run's session record: NATS's 1 MiB payload, less room for its headers.
const MAX_RECORD: usize = 1024 * 1024 - 8 * 1024;

impl Trails {
    /// Drive each run woken, one pass at a time per run, and wake those whose wall time passed.
    pub(super) async fn drive_all(&self) {
        let mut woken = self.inner.woken.lock().await;
        let mut driving: HashSet<RunId> = HashSet::new();
        let mut again: HashSet<RunId> = HashSet::new();
        let mut drivers = FuturesUnordered::new();
        loop {
            let overdue = self.next_deadline();
            let sleep = async {
                match overdue {
                    Some(at) => tokio::time::sleep((at - Utc::now()).to_std().unwrap_or_default()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some(run) = woken.recv() => {
                    if driving.insert(run.clone()) {
                        drivers.push(self.drive(run));
                    } else {
                        again.insert(run);
                    }
                }
                Some(run) = drivers.next() => {
                    driving.remove(&run);
                    if again.remove(&run) {
                        driving.insert(run.clone());
                        drivers.push(self.drive(run));
                    }
                }
                () = sleep => self.wake_overdue(),
            }
        }
    }

    /// When the first run not woken for its wall time yet is due to fail.
    fn next_deadline(&self) -> Option<DateTime<Utc>> {
        let runs = locked(&self.inner.runs);
        runs.values().filter_map(|slot| deadline(&slot.book())).min()
    }

    fn wake_overdue(&self) {
        let now = Utc::now();
        let overdue: Vec<RunId> = locked(&self.inner.runs)
            .iter()
            .filter(|(_, slot)| {
                let mut book = slot.book();
                let overdue = deadline(&book).is_some_and(|at| at <= now);
                book.late |= overdue;
                overdue
            })
            .map(|(run, _)| run.clone())
            .collect();
        overdue.iter().for_each(|run| self.wake(run));
    }

    /// Pass over `run` until a pass succeeds, waiting longer after each that failed.
    fn drive(&self, run: RunId) -> BoxFuture<'_, RunId> {
        Box::pin(async move {
            let Some(slot) = locked(&self.inner.runs).get(&run).cloned() else { return run };
            let mut delay = RETRY_FIRST;
            while let Err(e) = self.pass(&slot).await {
                tracing::warn!(run = %run, error = format!("{e:#}"), retry_in = ?delay, "driving a trail run");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
            run
        })
    }

    async fn pass(&self, slot: &Slot) -> Result<()> {
        let mut engine = slot.engine.lock().await;
        loop {
            let answers = std::mem::take(&mut slot.book().answers);
            for (call, answer) in answers {
                if let Err(e) = engine.answer(call, answer) {
                    tracing::debug!(call, error = %e, "a trail answer the run no longer takes");
                }
            }
            let header = slot.header();
            if header.finished() {
                return Ok(());
            }
            let progress = if overdue(&header) {
                engine.fail(format!("the run ran past its wall time limit of {}", wall(&header)))
            } else if matches!(header.state, RunState::Stopped { .. }) {
                return Ok(());
            } else {
                self.compute(&mut engine).await?
            };
            self.progressed(slot, &progress).await;
            match engine.status().clone() {
                Status::Done { result } => return self.end(slot, RunState::Done { result }).await,
                Status::Failed { error } => return self.end(slot, RunState::Failed { error }).await,
                Status::Running => {}
            }
            let issued = self.issue(slot, &engine).await?;
            if !issued.answers.is_empty() {
                for entry in issued.answers {
                    engine.answer(entry.call, entry.answer)?;
                }
                continue;
            }
            if issued.out_of_turns {
                engine.fail(format!("the run reached its limit of {} turns", header.limits.turns));
                continue;
            }
            let session = serde_json::to_string(engine.state())?;
            let record =
                session.len() + slot.book().pending.iter().map(|call| call.args.to_string().len()).sum::<usize>();
            if record > MAX_RECORD {
                engine.fail(format!("the run's session outgrew the {MAX_RECORD} bytes its record holds"));
                continue;
            }
            {
                let mut book = slot.book();
                book.session = session;
                book.queued = load::queued(&engine);
                if !matches!(book.header.state, RunState::Stopped { .. }) {
                    book.header.state = waiting_or_running(&book.pending, &engine);
                }
            }
            self.save_session(slot).await?;
            self.save_header(slot).await?;
            return self.publish(slot).await;
        }
    }

    /// Let the script compute in a worker, once one of the daemon's is free: an error leaving the run unchanged is
    /// returned, for the pass to be tried again; any other fails the run. A session the worker cannot restore, as
    /// one another Monty version dumped, is rebuilt from the run's journal ([`Run::replay`]).
    async fn compute(&self, engine: &mut Run) -> Result<Progress> {
        let _computing = super::computing().acquire().await?;
        match engine.resume(&self.inner.worker).await {
            Ok(progress) => Ok(progress),
            Err(RunError::Worker(e)) => Err(anyhow!(e).context("running the trail's worker")),
            Err(RunError::Unrestorable(why)) => self.replay(engine, &why).await,
            Err(e) => Ok(engine.fail(e.to_string())),
        }
    }

    /// Rebuild `engine`'s session by running its script again from its start, with the run's clock and seed, and
    /// answering its calls from its journal, which holds every answered call of the run; a script making a call
    /// otherwise than its journal entry fails the run, naming the call.
    async fn replay(&self, engine: &mut Run, why: &str) -> Result<Progress> {
        tracing::info!(why, "replaying a trail run's journal to rebuild its session");
        let replayed = Run::replay(engine.state().clone(), engine.journal().to_vec(), &self.inner.worker).await;
        match replayed {
            Ok((replayed, progress)) => {
                *engine = replayed;
                Ok(progress)
            }
            Err(RunError::Worker(e @ (WorkerError::ComputeLimit(_) | WorkerError::OutOfMemory))) => {
                Ok(engine.fail(format!("replaying the run's journal: {e}")))
            }
            Err(RunError::Worker(e)) => Err(anyhow!(e).context("replaying the trail's journal")),
            Err(e) => Ok(engine.fail(e.to_string())),
        }
    }

    /// Take in what a computation reported, and give up the steps the script no longer waits on.
    async fn progressed(&self, slot: &Slot, progress: &Progress) {
        let (header, cancelled) = {
            let mut book = slot.book();
            for event in &progress.events {
                if let Event::Phase(phase) = event {
                    book.header.phase = Some(phase.clone());
                }
            }
            let (cancelled, pending) =
                std::mem::take(&mut book.pending).into_iter().partition(|call| progress.cancelled.contains(&call.call));
            book.pending = pending;
            (book.header.clone(), cancelled)
        };
        self.report_events(&header, &progress.events).await;
        for call in &cancelled {
            self.retire(&header.run, call);
            if call.sent {
                self.report_step(&header, call, StepStage::Dropped);
            }
            self.withdraw_own(&header, call).await;
        }
    }

    /// Withdraw `call`'s step when the conductor sent it itself.
    async fn withdraw_own(&self, header: &TrailRun, call: &PendingCall) {
        if !call.sent || call.to != self.inner.ant || call.function != TrailFunction::Agent {
            return;
        }
        let ant = &self.inner.ant;
        let withdrawn = crate::delivery::attempts::withdraw(&self.inner.attempts, ant, &call.message, ant, None);
        if let Err(e) = withdrawn.await {
            tracing::warn!(run = %header.run, call = call.call, error = format!("{e:#}"), "withdrawing a step");
        }
    }

    /// Store the end of a run, and forget it.
    async fn end(&self, slot: &Slot, state: RunState) -> Result<()> {
        let pending = {
            let mut book = slot.book();
            book.header.state = state;
            book.header.ended_at = Some(Utc::now());
            std::mem::take(&mut book.pending)
        };
        let header = slot.header();
        for call in &pending {
            self.withdraw_own(&header, call).await;
        }
        self.save_header(slot).await?;
        self.inner.store.list(&header.run, false).await?;
        locked(&self.inner.runs).remove(&header.run);
        self.forget(&header.run);
        for call in pending.iter().filter(|call| call.sent) {
            self.report_step(&header, call, StepStage::Dropped);
        }
        self.report_end(&slot.header()).await;
        Ok(())
    }

    /// Make a step of each call handed out that has none yet: a pending call, unsent, or, for a call no step can
    /// serve, its answer raising in the script, stored.
    async fn issue(&self, slot: &Slot, engine: &Run) -> Result<Issued> {
        let (header, mut turns) = {
            let book = slot.book();
            let reserved: u32 = book.pending.iter().map(reserved).sum();
            let left = book.header.limits.turns.saturating_sub(book.header.turns).saturating_sub(reserved);
            (book.header.clone(), left)
        };
        let mut issued = Issued::default();
        let mut colony = None;
        // A call answered while the script computed is outstanding in the engine until the next pass hands it in.
        let has_step = |call: &Call| {
            let book = slot.book();
            book.pending.iter().any(|pending| pending.call == call.call)
                || book.answers.iter().any(|(answered, _)| *answered == call.call)
        };
        for call in engine.outstanding().iter().filter(|call| !has_step(call)) {
            let to = match self.recipient(&header, call, &mut colony).await? {
                Ok(to) => to,
                Err(error) => {
                    issued.answers.push(self.raise(&header.run, call, error).await?);
                    continue;
                }
            };
            let turn_budget = match call.function {
                Function::Agent => DEFAULT_TURN_BUDGET.min(turns),
                Function::Human => DEFAULT_TURN_BUDGET,
            };
            if turn_budget == 0 {
                issued.out_of_turns = !slot.book().pending.iter().any(|pending| reserved(pending) > 0);
                continue;
            }
            if call.function == Function::Agent {
                turns -= turn_budget;
            }
            let pending = PendingCall {
                call: call.call,
                function: steps::function(call.function),
                args: call.args.clone(),
                to,
                thread: ThreadId::generate(),
                message: Uuid::now_v7(),
                sent_at: Utc::now(),
                sent: false,
                turn_budget,
                retries: 0,
            };
            if let Err(error) = steps::envelope(&header, &pending) {
                issued.answers.push(self.raise(&header.run, call, error).await?);
                continue;
            }
            locked(&self.inner.threads).insert(pending.thread.clone(), Step::Live(header.run.clone()));
            let mut book = slot.book();
            book.header.steps += 1;
            book.pending.push(pending);
        }
        Ok(issued)
    }

    /// Who answers `call`: for agent(), the ant it names, an ant of the colony with the capability it names, or else
    /// the conductor; for human(), the run's owner. An error for a call no one can answer.
    async fn recipient(
        &self,
        header: &TrailRun,
        call: &Call,
        colony: &mut Option<Vec<ants_proto::ColonyEntry>>,
    ) -> Result<Result<ants_proto::UserId, String>> {
        if call.function == Function::Human {
            let asked = crate::mcp::asked_questions(call.args.clone()).map(|_| ());
            return Ok(asked.map(|()| self.inner.humans.owner_or_default(header.owner.as_ref())));
        }
        let args = match steps::AgentArgs::of(&call.args) {
            Ok(args) => args,
            Err(e) => return Ok(Err(e)),
        };
        if let Some(ant) = &args.ant {
            let ant: ants_proto::UserId = match ant.parse() {
                Ok(ant) => ant,
                Err(e) => return Ok(Err(format!("agent() names no ant as {ant:?}: {e}"))),
            };
            if self.inner.humans.is_human(&ant) {
                return Ok(Err(format!("{ant} is a human: ask humans with human()")));
            }
            return Ok(Ok(ant));
        }
        let Some(capability) = &args.capability else { return Ok(Ok(self.inner.ant.clone())) };
        if colony.is_none() {
            *colony = Some((self.inner.colony)().await?);
        }
        let entries = colony.as_deref().unwrap_or_default();
        Ok(match crate::colony::pick(entries, capability, &args.prompt, &self.inner.ant, Utc::now()) {
            Some(entry) => Ok(entry.ant.clone()),
            None => Err(format!("no ant of the colony but the conductor has the capability {capability:?}")),
        })
    }

    /// Answer `call` with `error`, raising in the script, as its journal entry, stored.
    async fn raise(&self, run: &RunId, call: &Call, error: String) -> Result<JournalEntry> {
        let entry = JournalEntry {
            call: call.call,
            function: call.function,
            args: call.args.clone(),
            answer: Answer::Error(error),
        };
        self.inner.store.put_entry(run, &entry).await?;
        Ok(entry)
    }

    /// Publish the steps stored and not sent yet, then store them sent.
    async fn publish(&self, slot: &Slot) -> Result<()> {
        let (header, unsent) = {
            let book = slot.book();
            (book.header.clone(), book.pending.iter().filter(|call| !call.sent).cloned().collect::<Vec<_>>())
        };
        if unsent.is_empty() || matches!(header.state, RunState::Stopped { .. }) {
            return Ok(());
        }
        for call in &unsent {
            let message = steps::envelope(&header, call).map_err(|e| anyhow!(e))?;
            let thread = TrailStepThread {
                run: header.run.clone(),
                call: call.call,
                to: call.to.clone(),
                run_thread: header.thread.clone(),
            };
            self.inner.store.put_step(&call.thread, &thread).await?;
            (self.inner.outbox)(message).await?;
            if let Some(pending) = slot.book().pending.iter_mut().find(|pending| pending.message == call.message) {
                pending.sent = true;
            }
            self.report_step(&header, call, StepStage::Started);
        }
        self.save_session(slot).await
    }
}

#[derive(Default)]
struct Issued {
    /// Answers to calls no step can serve.
    answers: Vec<JournalEntry>,
    /// Whether a call waits for turns no step it waits on will give back.
    out_of_turns: bool,
}

/// The turns a pending step may still spend.
fn reserved(call: &PendingCall) -> u32 {
    match call.function {
        TrailFunction::Agent => call.turn_budget,
        TrailFunction::Human => 0,
    }
}

/// When the run of `book` fails for its wall time, unless woken for it already or ended.
fn deadline(book: &super::Book) -> Option<DateTime<Utc>> {
    let header = &book.header;
    if book.late || header.finished() {
        return None;
    }
    let wall = chrono::Duration::from_std(header.limits.wall_time()).ok()?;
    Some(header.started_at + wall)
}

/// Whether the run of `header` ran past its wall time.
fn overdue(header: &TrailRun) -> bool {
    chrono::Duration::from_std(header.limits.wall_time()).is_ok_and(|wall| header.started_at + wall <= Utc::now())
}

fn wall(header: &TrailRun) -> String {
    let secs = header.limits.wall_secs;
    match secs {
        s if s % 3600 == 0 => format!("{} hours", s / 3600),
        s if s % 60 == 0 => format!("{} minutes", s / 60),
        s => format!("{s} seconds"),
    }
}

/// Waiting when every step the run waits on is a human's.
fn waiting_or_running(pending: &[PendingCall], engine: &Run) -> RunState {
    let humans_only = !pending.is_empty()
        && pending.len() == engine.outstanding().len()
        && pending.iter().all(|call| call.function == TrailFunction::Human);
    if humans_only { RunState::Waiting } else { RunState::Running }
}
