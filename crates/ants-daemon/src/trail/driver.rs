//! Driving runs: each pass hands a run's script the answers taken since the last, lets it compute in a worker, sends
//! the steps it then waits on, and stores what came of it; a run's end is stored and reported here too.

use std::collections::HashSet;
use std::time::Duration;

use ants_proto::{
    AgentArgs, CallAnswer, DEFAULT_TURN_BUDGET, JournalEntry, PendingCall, RunId, RunLimit, RunState, StepStage,
    ThreadId, TrailFunction, TrailRun, TrailSession, TrailStepThread, lines, one_line,
};
use ants_trail::{Call, Event, Progress, Run, RunError, Status, WorkerError};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt as _};
use uuid::Uuid;

use super::report::clip;
use super::{Slot, Step, Trails, load, locked, steps};

/// How long a pass that failed waits before the next, at first and at most.
pub(super) const RETRY_FIRST: Duration = Duration::from_secs(1);
pub(super) const RETRY_MAX: Duration = Duration::from_mins(1);
/// Most bytes of a run's session record, and of its header: NATS's 1 MiB payload, less room for its headers.
pub const MAX_RECORD: usize = 1024 * 1024 - 8 * 1024;
/// Most characters of a run's phase, and of its error at its end when its record would not hold it whole.
const MAX_PHASE: usize = 1000;
const MAX_ERROR: usize = 8000;
/// Workers of a run that may crash in a row, the run unchanged, before it fails: a crash the same request brings
/// again, as a stack overflow's abort, would otherwise be tried until the run's wall time.
const MAX_CRASHES: u32 = 3;

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
        self.give_up_left(slot).await?;
        // What the run fails on should the script fail next: the limit an answer handed to it raises for, or the run
        // broke.
        let mut broke = None;
        loop {
            broke = broke.or(hand_answers(slot, &mut engine));
            let header = slot.header();
            if header.finished() {
                // Ended in memory, its end not stored yet.
                return self.close(slot).await;
            }
            let progress = if overdue(&header) {
                if matches!(engine.status(), Status::Running) {
                    broke = Some(RunLimit::Wall);
                }
                engine.fail(format!("the run ran past its wall time limit of {}", wall(&header)))
            } else if matches!(header.state, RunState::Stopped { .. }) {
                return Ok(());
            } else {
                self.compute(slot, &mut engine).await?
            };
            self.progressed(slot, &progress).await;
            match engine.status().clone() {
                Status::Done { result } => return self.end(slot, RunState::Done { result }).await,
                Status::Failed { error, limit } => {
                    let limit = limit.or(broke);
                    if limit.is_some() {
                        self.forget_refusals(&header.run, &engine).await?;
                    }
                    return self.end(slot, RunState::Failed { error, limit }).await;
                }
                Status::Running => {}
            }
            if self.take_gotos(slot, &mut engine).await? {
                continue;
            }
            let issued = self.issue(slot, &engine).await?;
            if !issued.answers.is_empty() {
                broke = issued.broke;
                for entry in issued.answers {
                    engine.answer(entry.call, entry.answer)?;
                }
                continue;
            }
            if issued.out_of_turns {
                broke = Some(RunLimit::Turns);
                engine.fail(format!("the run reached its limit of {} turns", header.limits.turns));
                continue;
            }
            let record = session_record(slot, &engine)?;
            // The record as save_session stores it: the session escaped once more, beside the calls it waits on.
            let size = serde_json::to_vec(&record)?.len();
            if size > MAX_RECORD {
                broke = Some(RunLimit::Session);
                engine.fail(format!(
                    "the run's session record outgrew its room: {size} bytes, past the {MAX_RECORD} it holds"
                ));
                continue;
            }
            return self.rest(slot, &engine, record, &header).await;
        }
    }

    /// Store the run of `slot` as it waits for the answers to the calls of `engine`, its session as in `record` and
    /// its ancestors as `header` names them, and tell of it.
    async fn rest(&self, slot: &Slot, engine: &Run, record: TrailSession, header: &TrailRun) -> Result<()> {
        {
            let mut book = slot.book();
            book.session = record.session;
            book.queued = record.queued;
            if !matches!(book.header.state, RunState::Stopped { .. }) {
                book.header.state = waiting_or_running(&book.pending, engine);
            }
        }
        self.save_ancestors(header).await?;
        self.save_session(slot).await?;
        self.save_header(slot).await?;
        self.publish(slot).await
    }

    /// Let the script compute in a worker, once one of the daemon's is free: an error leaving the run unchanged is
    /// returned, for the pass to be tried again, up to [`MAX_CRASHES`] crashes in a row; any other fails the run. A session the worker cannot restore, as
    /// one another Monty version dumped, is rebuilt from the run's journal ([`Run::replay`]).
    async fn compute(&self, slot: &Slot, engine: &mut Run) -> Result<Progress> {
        let _computing = super::computing().acquire().await?;
        match engine.resume(&self.inner.worker).await {
            Ok(progress) => {
                slot.book().crashes = 0;
                Ok(progress)
            }
            Err(RunError::Worker(e)) => retry_or_fail(slot, engine, e, "running the trail's worker"),
            Err(RunError::Unrestorable(why)) => self.replay(slot, engine, &why).await,
            Err(e) => Ok(engine.fail(e.to_string())),
        }
    }

    /// Rebuild `engine`'s session by running its script again from its start, with the run's clock and seed, and
    /// answering its calls from its journal, which holds every answered call of the run; a script making a call
    /// otherwise than its journal entry fails the run, naming the call.
    async fn replay(&self, slot: &Slot, engine: &mut Run, why: &str) -> Result<Progress> {
        tracing::info!(why, "replaying a trail run's journal to rebuild its session");
        let replayed = Run::replay(engine.state().clone(), engine.journal().to_vec(), &self.inner.worker).await;
        match replayed {
            Ok((replayed, progress)) => {
                slot.book().crashes = 0;
                *engine = replayed;
                Ok(progress)
            }
            Err(RunError::Worker(e @ (WorkerError::ComputeLimit(_) | WorkerError::OutOfMemory))) => {
                let limit = if e == WorkerError::OutOfMemory { RunLimit::Memory } else { RunLimit::Compute };
                Ok(engine.fail_on(Some(limit), format!("replaying the run's journal: {e}")))
            }
            Err(RunError::Worker(e @ (WorkerError::Oversized(_) | WorkerError::Protocol(_)))) => {
                Ok(engine.fail(format!("replaying the run's journal: {e}")))
            }
            Err(RunError::Worker(e)) => retry_or_fail(slot, engine, e, "replaying the trail's journal"),
            Err(e) => Ok(engine.fail(e.to_string())),
        }
    }

    /// Take in what a computation reported, and give up the steps the script no longer waits on.
    async fn progressed(&self, slot: &Slot, progress: &Progress) {
        let (header, cancelled) = {
            let mut book = slot.book();
            for event in &progress.events {
                if let Event::Phase(phase) = event {
                    book.header.phase = Some(clip(&one_line(phase, MAX_PHASE), MAX_PHASE));
                }
            }
            let (cancelled, pending): (Vec<PendingCall>, _) =
                std::mem::take(&mut book.pending).into_iter().partition(|call| progress.cancelled.contains(&call.call));
            book.pending = pending;
            (book.header.clone(), cancelled)
        };
        self.give_back_unsent(slot, &cancelled);
        for call in &cancelled {
            self.retire(&header.run, call);
        }
        self.report_events(&header, &progress.events).await;
        self.abandon(&header, &cancelled).await;
    }

    /// Give up `calls`, steps the run of `header` no longer waits on, their threads retired: their child runs stop, the
    /// steps sent are reported dropped, and those the conductor sent itself withdrawn.
    pub(super) async fn abandon(&self, header: &TrailRun, calls: &[PendingCall]) {
        self.stop_children(calls, &self.inner.ant).await;
        for call in calls {
            if call.sent {
                self.report_step(header, call, StepStage::Dropped);
            }
            self.withdraw_own(header, call).await;
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

    /// End the run of `slot` in `state`, bounded to what its record holds, then store and tell its end ([`Trails::close`]).
    /// The steps it counted and never sent are given back; an error is kept without control characters.
    async fn end(&self, slot: &Slot, mut state: RunState) -> Result<()> {
        if let RunState::Failed { error, .. } = &mut state {
            *error = lines(error);
        }
        let pending = slot.book().pending.clone();
        self.give_back_unsent(slot, &pending);
        {
            let mut book = slot.book();
            book.header.state = state;
            book.header.ended_at = Some(Utc::now());
            fit_end(&mut book.header);
        }
        self.close(slot).await
    }

    /// Store the end of the run of `slot`, ended in memory, and forget it: its child runs stop, and a child's end
    /// answers its parent's call first. The run_trail call waiting on it, if any, takes the end, else its conductor gets
    /// a message inviting a rating; its owner gets the end's message. An error leaves the run in memory, for its next
    /// pass to close it again: each step here holds when taken again.
    async fn close(&self, slot: &Slot) -> Result<()> {
        let (header, calls) = {
            let book = slot.book();
            (book.header.clone(), book.pending.clone())
        };
        self.stop_children(&calls, &self.inner.ant).await;
        self.answer_parent(&header).await?;
        for call in &calls {
            self.withdraw_own(&header, call).await;
        }
        self.save_ancestors(&header).await?;
        self.save_header(slot).await?;
        let pending = std::mem::take(&mut slot.book().pending);
        let header = slot.header();
        // Listed until the end is told, so that a restart tells it otherwise.
        self.tell_end(&header).await;
        self.inner.store.list(&header.run, false).await?;
        locked(&self.inner.runs).remove(&header.run);
        self.forget(&header.run);
        for call in pending.iter().filter(|call| call.sent) {
            self.report_step(&header, call, StepStage::Dropped);
        }
        self.wake_starved(&header);
        Ok(())
    }

    /// Tell the end of the run of `header`, stored: to the run_trail call waiting on it, else by the invitation to rate
    /// it, to its owner, and, for a run started answering a step, what the runs answering the step spent to the step's
    /// conductor. A message not stored is sent again, under the same id, until it is.
    async fn tell_end(&self, header: &TrailRun) {
        let mut invited = self.hand_end(header);
        let (mut told, mut spent) = (false, false);
        let mut delay = RETRY_FIRST;
        loop {
            invited = invited || self.invite(header).await;
            told = told || self.report_end(header).await;
            spent = spent || self.tell_spent(header).await;
            if invited && told && spent {
                return;
            }
            tracing::warn!(run = %header.run, retry_in = ?delay, "telling the end of a trail run");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RETRY_MAX);
        }
    }

    /// Make a step of each call handed out that has none yet: a pending call, unsent, or, for a call no step can
    /// serve, its answer raising in the script, stored. Steps and turns count against the run's tree's budget.
    async fn issue(&self, slot: &Slot, engine: &Run) -> Result<Issued> {
        let header = slot.header();
        let budget = self.budget(&header);
        let (mut steps, mut turns) = (budget.steps, budget.turns);
        let waiting = u32::try_from(slot.book().pending.len()).unwrap_or(u32::MAX);
        let (mut made, mut granted) = (0_u32, Vec::new());
        let mut issued = Issued::default();
        let mut colony = None;
        // A call answered while the script computed is outstanding in the engine until the next pass hands it in.
        let anew = |call: &Call| slot.book().anew.contains(&call.call);
        let has_step = |call: &Call| {
            let book = slot.book();
            book.pending.iter().any(|pending| pending.call == call.call)
                || book.answers.iter().any(|(answered, _)| *answered == call.call)
        };
        for call in engine.outstanding().iter().filter(|call| call.function.is_step() && !has_step(call)) {
            // A call sent anew on resuming counted its step already.
            let counted = anew(call);
            if !counted && steps >= budget.limits.steps {
                let error = format!("the run reached its limit of {} steps", budget.limits.steps);
                issued.answers.push(self.raise(&header.run, call, error).await?);
                issued.broke = Some(RunLimit::Steps);
                continue;
            }
            let to = if call.function == TrailFunction::Trail {
                match self.refuse_trail(&header, call).await {
                    None => Ok(self.inner.ant.clone()),
                    Some(error) => Err(error),
                }
            } else {
                self.recipient(&header, call, &mut colony).await?
            };
            let to = match to {
                Ok(to) => to,
                Err(error) => {
                    issued.answers.push(self.raise(&header.run, call, error).await?);
                    continue;
                }
            };
            let turn_budget = turn_budget_for(call.function, turns);
            if turn_budget == 0 && call.function == TrailFunction::Agent {
                if self.holds_turns(&header) {
                    slot.book().starved = true;
                } else {
                    issued.out_of_turns = true;
                }
                continue;
            }
            if call.function == TrailFunction::Agent {
                turns -= turn_budget;
            }
            let pending = pending_for(call, to, turn_budget);
            if call.function != TrailFunction::Trail {
                if let Err(error) = steps::envelope(&header, &pending, None) {
                    issued.answers.push(self.raise(&header.run, call, error).await?);
                    continue;
                }
                locked(&self.inner.threads).insert(pending.thread.clone(), Step::Live(header.run.clone()));
            }
            made += 1;
            if call.function == TrailFunction::Agent {
                granted.push((pending.call, pending.thread.clone()));
            }
            if counted {
                slot.book().pending.push(pending);
                continue;
            }
            steps += 1;
            {
                let mut book = slot.book();
                count_steps(&mut book.header, 1);
                book.pending.push(pending);
            }
            self.charge(&header, 1, 0);
        }
        if !granted.is_empty() {
            // Shared among the steps going out now and the calls held back for room, as many as may be out at once.
            let queued = u32::try_from(engine.queued().len()).unwrap_or(u32::MAX);
            let room = header.limits.concurrency.saturating_sub(waiting);
            self.grant(slot, &granted, room.min(made.saturating_add(queued)));
        }
        Ok(issued)
    }

    /// Who answers `call`: for agent(), the ant it names, an ant of the colony with the capability it names, the lead
    /// of the team it names, or else the conductor; for human(), the run's owner. An error for a call no one can answer.
    async fn recipient(
        &self,
        header: &TrailRun,
        call: &Call,
        colony: &mut Option<Vec<ants_proto::ColonyEntry>>,
    ) -> Result<Result<ants_proto::UserId, String>> {
        if call.function == TrailFunction::Human {
            let asked = crate::mcp::asked_questions(call.args.clone()).map(|_| ());
            return Ok(asked.map(|()| self.inner.humans.owner_or_default(header.owner.as_ref())));
        }
        let args = match AgentArgs::of(&call.args) {
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
        if let Some(team) = &args.team {
            return Ok(self.lead_of(team).await);
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

    /// The lead of `team`, which an agent(team=) step goes to: an error for no such team, a leaderless one, a bus
    /// keeping no teams or a directory that cannot be read now.
    async fn lead_of(&self, team: &str) -> Result<ants_proto::UserId, String> {
        let team: ants_proto::UserId = team.parse().map_err(|e| format!("agent() names no team as {team:?}: {e}"))?;
        let Some(kv) = &self.inner.teams else { return Err("the bus keeps no teams".to_owned()) };
        let Some(directory) = crate::teams::read_quickly(kv).await else {
            return Err(format!("the teams cannot be read, so team {team} has no lead to send to"));
        };
        directory.lead_of(&team).cloned().map_err(|e| e.to_string())
    }

    /// Answer `call` with `error`, raising in the script, as its journal entry, stored.
    pub(super) async fn raise(&self, run: &RunId, call: &Call, error: String) -> Result<JournalEntry> {
        let entry = JournalEntry {
            call: call.call,
            function: call.function,
            args: call.args.clone(),
            answer: CallAnswer::Error(error),
        };
        self.inner.store.put_entry(run, &entry).await?;
        Ok(entry)
    }

    /// Delete the journal entries of the calls the run `run`, `engine`, failed on a limit, refused since its session was
    /// stored, as one past its steps: those that session neither took an answer to, nor waits on, nor lists a step
    /// for. Resuming the run restores that session, whose script makes those calls anew, and a restart would take such
    /// an entry for the answer to its call sent anew. A call the session waits on keeps its entry though no step lists
    /// it, as a step answered before a resume, which stored the session it restored.
    async fn forget_refusals(&self, run: &RunId, engine: &Run) -> Result<()> {
        let store = &self.inner.store;
        let Some(stored) = store.session(run).await? else { return Ok(()) };
        let session: ants_trail::Session = serde_json::from_str(&stored.session)?;
        let pending = stored.pending.iter().map(|call| call.call);
        let mut kept: HashSet<u32> = session.answered().iter().copied().chain(pending).collect();
        kept.extend(Run::restore(session, Vec::new()).outstanding().iter().map(|call| call.call));
        for entry in engine.journal().iter().filter(|entry| !kept.contains(&entry.call)) {
            store.delete_entry(run, entry.call).await?;
        }
        Ok(())
    }

    /// Publish the steps stored and not sent yet, then store them sent. The agent() steps of a saved trail carry its
    /// tips, read once for them all.
    async fn publish(&self, slot: &Slot) -> Result<()> {
        let (header, unsent) = {
            let book = slot.book();
            (book.header.clone(), book.pending.iter().filter(|call| !call.sent).cloned().collect::<Vec<_>>())
        };
        if unsent.is_empty() || matches!(header.state, RunState::Stopped { .. }) {
            return Ok(());
        }
        let tips = if unsent.iter().any(|call| call.function == TrailFunction::Agent) {
            self.step_tips(&header).await
        } else {
            None
        };
        for call in &unsent {
            let thread = TrailStepThread {
                run: header.run.clone(),
                call: call.call,
                to: call.to.clone(),
                run_thread: header.thread.clone(),
                label: call.function.label(&call.args),
            };
            if call.function == TrailFunction::Trail {
                // A trail() step sends no message: its thread, the child run's, keeps no step key.
                self.inner.store.put_thread_step(&call.thread, &thread).await?;
                self.publish_child(slot, &header, call).await?;
                continue;
            }
            let message = steps::envelope(&header, call, tips.as_ref()).map_err(|e| anyhow!(e))?;
            let message = self.bounded(&header, message);
            self.inner.store.put_step(&call.thread, &thread).await?;
            (self.inner.outbox)(message).await?;
            {
                let mut book = slot.book();
                if let Some(pending) = book.pending.iter_mut().find(|pending| pending.message == call.message) {
                    pending.sent = true;
                }
                book.anew.retain(|anew| *anew != call.call);
            }
            self.report_step(&header, call, StepStage::Started);
        }
        self.save_session(slot).await
    }
}

impl Trails {
    /// Start the child run of `call`, a trail() call of the run of `slot`, then mark the call sent; a child that cannot
    /// start after all, as one of a trail removed since the call was issued, answers the call with why, its step given
    /// back.
    async fn publish_child(&self, slot: &Slot, header: &TrailRun, call: &PendingCall) -> Result<()> {
        if let Err(error) = self.start_child(header, call).await? {
            self.give_back_unsent(slot, std::slice::from_ref(call));
            return self.resolve(slot, header, call, CallAnswer::Error(error), 0).await;
        }
        let sent = {
            let mut book = slot.book();
            let pending = book.pending.iter_mut().find(|pending| pending.call == call.call && !pending.sent);
            let sent = pending.map(|pending| pending.sent = true).is_some();
            book.anew.retain(|anew| *anew != call.call);
            sent
        };
        if sent {
            self.report_step(header, call, StepStage::Started);
        }
        Ok(())
    }
}

#[derive(Default)]
struct Issued {
    /// Answers to calls no step can serve.
    answers: Vec<JournalEntry>,
    /// The limit one of those answers raises for: steps, for a call past them.
    broke: Option<RunLimit>,
    /// Whether a call waits for turns no step it waits on will give back.
    out_of_turns: bool,
}

/// The turns the step of a call to `function` may spend, of the `turns` the run's tree has left: none for a call
/// that prompts no ant and asks no human.
fn turn_budget_for(function: TrailFunction, turns: u32) -> u32 {
    match function {
        TrailFunction::Agent => DEFAULT_TURN_BUDGET.min(turns),
        TrailFunction::Human => DEFAULT_TURN_BUDGET,
        TrailFunction::Trail | TrailFunction::Goto => 0,
    }
}

/// The step of `call`, for `to` to answer within `turn_budget` turns, on a thread of its own and not sent yet.
fn pending_for(call: &Call, to: ants_proto::UserId, turn_budget: u32) -> PendingCall {
    PendingCall {
        call: call.call,
        function: call.function,
        args: call.args.clone(),
        to,
        thread: ThreadId::generate(),
        message: Uuid::now_v7(),
        sent_at: Utc::now(),
        sent: false,
        turn_budget,
        retries: 0,
    }
}

/// Count `steps` sent to the run of `header` and to the leg it is on.
pub(super) fn count_steps(header: &mut TrailRun, steps: u32) {
    header.steps = header.steps.saturating_add(steps);
    if let Some(leg) = header.route.last_mut() {
        leg.steps = leg.steps.saturating_add(steps);
    }
}

/// Give back `steps` counted for the run of `header` and the leg it is on, not sent after all.
pub(super) fn uncount_steps(header: &mut TrailRun, steps: u32) {
    header.steps = header.steps.saturating_sub(steps);
    if let Some(leg) = header.route.last_mut() {
        leg.steps = leg.steps.saturating_sub(steps);
    }
}

/// Hand back `e`, a worker's failure leaving `engine` unchanged, for the pass to be tried again; the run fails instead
/// on its [`MAX_CRASHES`]th worker crashing in a row.
fn retry_or_fail(slot: &Slot, engine: &mut Run, e: WorkerError, doing: &'static str) -> Result<Progress> {
    if matches!(e, WorkerError::Crashed { .. }) {
        let crashes = {
            let mut book = slot.book();
            book.crashes = book.crashes.saturating_add(1);
            book.crashes
        };
        if crashes >= MAX_CRASHES {
            return Ok(engine.fail(format!("{doing}: {e} ({crashes} times in a row)")));
        }
    }
    Err(anyhow!(e).context(doing))
}

/// The turns a pending step may still spend.
pub(super) fn reserved(call: &PendingCall) -> u32 {
    match call.function {
        TrailFunction::Agent => call.turn_budget,
        TrailFunction::Human | TrailFunction::Trail | TrailFunction::Goto => 0,
    }
}

/// Bound the end of the run of `header` to what its record holds: a result too large fails the run, naming its size,
/// and an error too large is cut, to [`MAX_ERROR`] characters, then, on a record near its room, as one whose route
/// filled it, to what fits, the run's phase dropped last.
fn fit_end(header: &mut TrailRun) {
    let size = |header: &TrailRun| serde_json::to_vec(header).map_or(usize::MAX, |record| record.len());
    if size(header) <= MAX_RECORD {
        return;
    }
    match &mut header.state {
        RunState::Done { result } => {
            let bytes = serde_json::to_vec(result).map_or(usize::MAX, |result| result.len());
            let error =
                format!("the run's result, {bytes} bytes as JSON, outgrew the {MAX_RECORD} bytes its record holds");
            header.state = RunState::Failed { error, limit: None };
        }
        RunState::Failed { error, .. } => *error = clip(error, MAX_ERROR),
        RunState::Running | RunState::Waiting | RunState::Stopped { .. } => {}
    }
    loop {
        let over = size(header).saturating_sub(MAX_RECORD);
        if over == 0 {
            return;
        }
        match &mut header.state {
            RunState::Failed { error, .. } if !error.is_empty() => {
                // Its characters cut in the share its bytes as JSON are over, and again while it does not fit.
                let chars = error.chars().count();
                let bytes = serde_json::to_vec(error).map_or(usize::MAX, |error| error.len());
                let keep = (chars.saturating_mul(bytes.saturating_sub(over)) / bytes).min(chars - 1);
                *error = if keep == 0 { String::new() } else { clip(error, keep) };
            }
            _ if header.phase.take().is_some() => {}
            _ => return,
        }
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

/// Hand `engine` the answers that arrived for the run of `slot` since its last pass: the limit one of them broke
/// the run on, if any.
fn hand_answers(slot: &Slot, engine: &mut Run) -> Option<RunLimit> {
    let (answers, raised) = {
        let mut book = slot.book();
        (std::mem::take(&mut book.answers), book.broke.take())
    };
    for (call, answer) in answers {
        if let Err(e) = engine.answer(call, answer) {
            tracing::debug!(call, error = %e, "a trail answer the run no longer takes");
        }
    }
    raised
}

/// The session record of the run of `slot` as `engine` waits: its calls asked anew pruned of those answered.
fn session_record(slot: &Slot, engine: &Run) -> Result<TrailSession> {
    let session = serde_json::to_string(engine.state())?;
    let (pending, anew) = {
        let mut book = slot.book();
        book.anew.retain(|call| engine.outstanding().iter().any(|c| c.call == *call));
        (book.pending.clone(), book.anew.clone())
    };
    Ok(TrailSession { session, pending, queued: load::queued(engine), anew })
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

#[cfg(test)]
mod tests {
    use ants_proto::TrailLeg;
    use ants_test_support::{trail_leg, trail_run};

    use super::*;

    /// A run of one leg ended in `state`, its leg's script padded for its record to have `size` bytes as it ran.
    fn ended(size: usize, state: RunState) -> TrailRun {
        let now = Utc::now();
        let leg = TrailLeg { script: String::new(), ..trail_leg(Some("deploy"), now) };
        let run = trail_run(&"lead".parse().unwrap(), Some("deploy"), RunState::Running, now);
        let mut header = TrailRun { route: vec![leg], ended_at: Some(now), ..run };
        let bare = serde_json::to_vec(&header).unwrap().len();
        header.route[0].script = "#".repeat(size - bare);
        assert_eq!(serde_json::to_vec(&header).unwrap().len(), size);
        header.state = state;
        header
    }

    fn size(header: &TrailRun) -> usize {
        serde_json::to_vec(header).unwrap().len()
    }

    #[test]
    fn an_end_that_fits_its_record_is_kept_whole() {
        let error = "e".repeat(MAX_ERROR * 2);
        let mut header = ended(1000, RunState::Failed { error: error.clone(), limit: None });
        fit_end(&mut header);
        assert_eq!(header.state, RunState::Failed { error, limit: None });
    }

    #[test]
    fn a_result_outgrowing_the_record_fails_the_run_naming_its_size() {
        let result = serde_json::Value::String("r".repeat(300 * 1024));
        let bytes = serde_json::to_vec(&result).unwrap().len();
        let mut header = ended(MAX_RECORD - 200 * 1024, RunState::Done { result });
        fit_end(&mut header);
        let error = format!("the run's result, {bytes} bytes as JSON, outgrew the {MAX_RECORD} bytes its record holds");
        assert_eq!(header.state, RunState::Failed { error, limit: None });
        assert!(size(&header) <= MAX_RECORD);
    }

    #[test]
    fn an_error_outgrowing_the_record_is_cut() {
        let error = "e".repeat(16 * 1024);
        let mut header = ended(MAX_RECORD - 10 * 1024, RunState::Failed { error, limit: Some(RunLimit::Steps) });
        fit_end(&mut header);
        let RunState::Failed { error, limit } = &header.state else { panic!("not failed") };
        assert_eq!((error.chars().count(), *limit), (MAX_ERROR, Some(RunLimit::Steps)));
        assert!(error.ends_with('…'));
        assert!(size(&header) <= MAX_RECORD);
    }

    #[test]
    fn an_end_with_a_phase_on_a_record_near_its_room_still_fits() {
        let error = "\u{1}".repeat(16 * 1024);
        let mut header = ended(MAX_RECORD - 2 * 1024, RunState::Failed { error, limit: None });
        header.phase = Some("p".repeat(MAX_PHASE));
        fit_end(&mut header);
        assert!(size(&header) <= MAX_RECORD, "{} bytes", size(&header));
        let RunState::Failed { error, .. } = &header.state else { panic!("not failed") };
        assert!(error.starts_with('\u{1}') && error.ends_with('…'), "what fits of the error is kept");
    }
}
