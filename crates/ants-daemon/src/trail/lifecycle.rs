//! A run's lifecycle: starting it, from a saved trail or a script, by a human, a schedule's firing or a step; stopping
//! it; and resuming it, stopped or failed on a limit.

use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    Envelope, MAX_TRAIL_ARGS, MAX_TRAIL_SCRIPT, PendingCall, RunId, RunLimit, RunState, StepStage, ThreadId,
    TrailFunction, TrailLeg, TrailLimits, TrailName, TrailRun, TrailSource, TrailStart, Unresumable, UserId,
    WaitingStep,
};
use ants_trail::Run;
use anyhow::Result;
use chrono::Utc;
use serde_json::Value;

use super::{Book, Slot, StartRun, Trails, children, driver, engine_limits, locked};

impl Trails {
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
        if locked(&self.inner.runs).contains_key(&run) {
            return Ok(run);
        }
        match self.inner.store.header(&run).await.map_err(|e| format!("reading the run: {e:#}"))? {
            Some(header) if header.finished() => return Ok(run),
            // Stored by a daemon stopped before it listed the run.
            Some(_) => {
                self.restore_run(&run).await.map_err(|e| format!("continuing the run: {e:#}"))?;
                return Ok(run);
            }
            None => {}
        }
        let start = StartRun {
            trail: TrailSource::Saved(start.trail.clone()),
            args: start.args.clone(),
            owner: firing.owner.clone(),
            origin: crate::provider::context::origin(firing, None, &self.inner.humans.humans()),
            thread: firing.thread_id.clone(),
            limits: start.limits,
            spends_from: None,
        };
        self.start_as(run, start).await
    }

    pub(super) async fn start_as(&self, run: RunId, start: StartRun) -> Result<RunId, String> {
        let StartRun { trail, args, owner, origin, thread, mut limits, spends_from } = start;
        if self.inner.pause.get().is_some() {
            let ant = &self.inner.ant;
            return Err(format!("{ant} is paused and starts no trail run: 'ants resume {ant}' resumes it"));
        }
        limits.check().map_err(|e| e.to_string())?;
        let _answering = match &spends_from {
            Some(_) => Some(self.inner.answering.lock().await),
            None => None,
        };
        let (name, script, local) = match trail {
            TrailSource::Saved(name) => {
                let found = self.inner.saved.script(&name).await?;
                (Some(name), found.script, found.local)
            }
            TrailSource::Script(_) if !origin.trusted() => {
                return Err("a run started from a webhook's thread runs only saved trails".to_owned());
            }
            TrailSource::Script(script) => (None, script, false),
        };
        if let Some(from) = &spends_from {
            limits = self.start_answering(&run, &thread, from, limits).await?;
        }
        let (leg, engine, session) =
            Leg { trail: name, local, script, at: None, args, first_call: 0 }.prepare(&limits)?;
        let now = leg.started_at;
        let header = TrailRun {
            run,
            conductor: self.inner.ant.clone(),
            route: vec![leg],
            owner,
            thread,
            parent: None,
            spends_from,
            grants: Vec::new(),
            origin,
            work_branch: None,
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
    pub(super) async fn launch(&self, header: TrailRun, engine: Run, session: String) -> Result<()> {
        let run = header.run.clone();
        let started = header.clone();
        let meta = engine.state().meta().clone();
        let book = Book {
            header,
            session,
            pending: Vec::new(),
            queued: Vec::new(),
            left: Vec::new(),
            anew: Vec::new(),
            answers: Vec::new(),
            broke: None,
            late: false,
            starved: false,
            crashes: 0,
        };
        let slot = Slot::new(engine, book);
        // Listed before its header is stored: a run listed with no header is dropped from the index at the next start.
        self.save_session(&slot).await?;
        self.inner.store.list(&run, true).await?;
        self.save_header(&slot).await?;
        self.report_start(&started, &meta).await;
        self.report_leg(&started);
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
        self.start(StartRun { trail, args, owner: Some(by.clone()), origin, thread, limits, spends_from: None }).await
    }

    /// Stop a run for `by`: it sends no further step, ignores the replies of the steps it waited on and withdraws
    /// those it sent itself, and its child runs stop too. Returns the steps to other ants they waited on, for `by` to
    /// withdraw. A pass in flight ends first, so none gives up steps of the run the stop does not list meanwhile.
    pub async fn stop(&self, run: &RunId, by: &UserId) -> Result<Vec<WaitingStep>, String> {
        self.holding(run, true, Box::pin(self.stop_held(run, by))).await.expect("waited for")
    }

    async fn stop_held(&self, run: &RunId, by: &UserId) -> Result<Vec<WaitingStep>, String> {
        let slot = self.running(run).await?;
        // Held to the end; a run's engine is locked before its children's, never after.
        let _engine = slot.engine.lock().await;
        let (header, pending) = {
            // Held from the stopped header's put to its taking effect in memory, so no other put stores the run
            // running meanwhile.
            let _held = slot.writes.lock().await;
            let (stopped, pending) = {
                let book = slot.book();
                if book.header.finished() {
                    return Err(format!("run {run} has ended"));
                }
                if let RunState::Stopped { by } = &book.header.state {
                    return Err(format!("{by} already stopped run {run}"));
                }
                let mut stopped = book.header.clone();
                stopped.state = RunState::Stopped { by: by.clone() };
                stopped.updated_at = Utc::now();
                // The steps never sent go out anew on resuming, counted then.
                driver::uncount_steps(&mut stopped, children::unsent(&book, &book.pending));
                (stopped, book.pending.clone())
            };
            self.inner.store.put_header(&stopped).await.map_err(|e| format!("storing the run: {e:#}"))?;
            let mut book = slot.book();
            let unsent = children::unsent(&book, &book.pending);
            driver::uncount_steps(&mut book.header, unsent);
            book.header.state = stopped.state;
            book.header.updated_at = stopped.updated_at;
            let header = book.header.clone();
            drop(book);
            self.give_back_above(&header, unsent);
            (header, pending)
        };
        let mut waiting = self.stop_steps(&header, &pending, by).await;
        self.hand_end(&slot.header());
        // The steps of a leg the run left by a goto() and has not given up yet, given up once the stored header lists
        // the next leg.
        let left = std::mem::take(&mut slot.book().left);
        waiting.extend(self.stop_steps(&header, &left, by).await);
        self.report_end(&slot.header()).await;
        self.tell_spent_stopped(slot.header());
        tracing::info!(run = %run, by = %by, "trail run stopped");
        Ok(waiting)
    }

    /// Give up `calls`, steps of the run of `header` stopped by `by`: their child runs stop, the replies to them are
    /// ignored and those the conductor sent itself withdrawn. Returns the steps to other ants, for `by` to withdraw.
    async fn stop_steps(&self, header: &TrailRun, calls: &[PendingCall], by: &UserId) -> Vec<WaitingStep> {
        let run = &header.run;
        let mut waiting = self.stop_children(calls, by).await;
        for call in calls.iter().filter(|call| call.function != TrailFunction::Trail) {
            self.retire(run, call);
            if call.sent {
                self.report_step(header, call, StepStage::Dropped);
            }
            if !call.sent || call.function == TrailFunction::Human {
                self.end_question(call).await;
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
        waiting
    }

    /// Resume a stopped run, or one failed on a limit, under `limits` when given: from its stored records, with the
    /// steps it waited on sent anew and its child runs stopped or failed on a limit resumed. A run started answering a
    /// step resumes within what the step's grant leaves after the other runs answering it.
    pub async fn resume(&self, run: &RunId, by: &UserId, limits: Option<TrailLimits>) -> Result<(), String> {
        self.holding(run, true, Box::pin(self.resume_held(run, by, limits))).await.expect("waited for")
    }

    async fn resume_held(&self, run: &RunId, by: &UserId, limits: Option<TrailLimits>) -> Result<(), String> {
        let stored = |e: anyhow::Error| format!("reading the run: {e:#}");
        let held = locked(&self.inner.runs).get(run).cloned();
        if let Some(slot) = &held {
            // A pass in flight ends before the records are read again.
            drop(slot.engine.lock().await);
        }
        let header = self.inner.store.header(run).await.map_err(stored)?;
        let mut header = header.ok_or_else(|| format!("{} conducts no run {run}", self.inner.ant))?;
        if let Err(why) = header.state.resumable(&header.limits, limits.as_ref()) {
            return Err(match why {
                Unresumable::Broke(RunLimit::Session) => format!(
                    "run {run} failed on its session size limit, which this resume does not raise: restoring the \
                     same session and taking the same answers, it would fail the same way"
                ),
                Unresumable::Broke(limit) => format!(
                    "run {run} failed on its {limit} limit, which no resume raises: restoring the same session and \
                     taking the same answers, it would fail the same way"
                ),
                Unresumable::Failed(error) => format!("run {run} failed, not on a limit: {error}"),
                Unresumable::Done => format!("run {run} is done"),
                Unresumable::NotStopped => format!("run {run} is not stopped"),
            });
        }
        if let Some(limits) = limits {
            limits.check().map_err(|e| e.to_string())?;
            header.limits = limits;
        }
        let _answering = match header.spends_from.clone() {
            Some(from) => {
                let held = self.inner.answering.lock().await;
                header.limits = self.resume_answering(&header, &from, header.limits).await?;
                Some(held)
            }
            None => None,
        };
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
            self.inner.store.list(run, true).await?;
            self.save_header(&slot).await?;
            self.save_ancestors(&slot.header()).await
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
}

/// A leg to start: a run of `trail`'s script, entered at the waypoint `at`, else at its start.
pub(super) struct Leg {
    pub(super) trail: Option<TrailName>,
    /// Whether the trail is only this machine's ([`TrailLeg::local`]).
    pub(super) local: bool,
    pub(super) script: String,
    pub(super) at: Option<String>,
    pub(super) args: Value,
    /// The id of the leg's first call: past those of the run's legs before ([`TrailLeg::first_call`]).
    pub(super) first_call: u32,
}

impl Leg {
    /// The leg, checked to fit, as the run's route records it, its run, and its session as JSON.
    pub(super) fn prepare(self, limits: &TrailLimits) -> Result<(TrailLeg, Run, String), String> {
        let Self { trail, local, script, at, args, first_call } = self;
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
        let leg = TrailLeg { trail, local, at, script, args, first_call, started_at: Utc::now(), steps: 0 };
        Ok((leg, engine, session))
    }
}
