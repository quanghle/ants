//! Answers to steps: the reply on a step's thread resolving its call, a re-ask for one matching the step's schema, and
//! messages on the thread of a step the run no longer waits on, or of a run no longer in memory.

use ants_proto::{
    AgentArgs, CallAnswer, Envelope, JournalEntry, PendingCall, Role, RunId, RunLimit, RunState, TipBoard,
    TrailFunction, TrailRun, UserId,
};
use anyhow::Result;
use chrono::Utc;

use super::report::Stray;
use super::steps::Taken;
use super::{Slot, Step, Trails, locked, nested, steps};

impl Trails {
    /// Take `message` when it arrived on a step's thread: the reply answering the step's call, resolving it; a
    /// notice or escalation, or a further reply of the step's recipient once the run no longer waits on the step,
    /// reported on the run's thread, a notice that the recipient does not answer the step failing the call
    /// (`steps::unanswered`). True when taken, to be acked without prompting the harness, once this returns.
    pub async fn take(&self, message: &Envelope) -> Result<bool> {
        let step = locked(&self.inner.threads).get(&message.thread_id).cloned();
        let answerer = self.answerer(message);
        let run = match step {
            None => return self.take_late(message, answerer).await,
            Some(Step::Retired { run, to }) => return self.take_retired(&run, &to, message, answerer).await,
            Some(Step::Live(run)) => run,
        };
        let Some(slot) = locked(&self.inner.runs).get(&run).cloned() else { return Ok(false) };
        let (header, pending) = {
            let book = slot.book();
            let pending = book.pending.iter().find(|call| call.thread == message.thread_id).cloned();
            (book.header.clone(), pending)
        };
        let Some(pending) = pending else { return Ok(false) };
        if !steps::answered_by(&pending, message, answerer) {
            if message.notice.is_none() && message.escalation.is_none() {
                return Ok(false);
            }
            self.store_told(&slot, pending.call, &pending.to, message).await?;
            let call = Some(pending.call);
            let stray = Stray { run: &run, run_thread: &header.thread, call, to: &pending.to, waited: true };
            self.report_stray(stray, message).await;
            if let Some(error) = steps::unanswered(&pending, message) {
                self.resolve(&slot, &header, &pending, CallAnswer::Error(error), 0).await?;
            } else if steps::asked(&pending, message) {
                self.waits_on_a_human(&slot, pending.call).await?;
            }
            return Ok(true);
        }
        // Charged in memory alone: what a message tells is charged once however often it is taken, and stored with the
        // answer, or the re-ask, it brings.
        self.take_told(&slot, pending.call, &pending.to, message);
        let header = slot.header();
        let spent = match pending.function {
            TrailFunction::Agent => pending.turn_budget.saturating_sub(message.turn_budget),
            TrailFunction::Human | TrailFunction::Trail | TrailFunction::Goto => 0,
            TrailFunction::Command | TrailFunction::ReadFile | TrailFunction::Bd | TrailFunction::Thread => 0,
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
    pub(super) async fn resolve(
        &self,
        slot: &Slot,
        header: &TrailRun,
        pending: &PendingCall,
        answer: CallAnswer,
        spent: u32,
    ) -> Result<()> {
        self.resolve_on(slot, header, pending, answer, spent, None).await
    }

    /// [`Trails::resolve`], with an answer raising for `broke`: the limit the run fails on should the script fail on
    /// it.
    pub(super) async fn resolve_on(
        &self,
        slot: &Slot,
        header: &TrailRun,
        pending: &PendingCall,
        answer: CallAnswer,
        spent: u32,
        broke: Option<RunLimit>,
    ) -> Result<()> {
        let run = &header.run;
        let entry = JournalEntry { call: pending.call, function: pending.function, args: pending.args.clone(), answer };
        self.inner.store.put_entry(run, &entry).await?;
        {
            let mut book = slot.book();
            book.pending.retain(|call| call.call != pending.call);
            book.header.turns = book.header.turns.saturating_add(spent);
            book.answers.push((entry.call, entry.answer.clone()));
            book.broke = book.broke.or(broke);
            nested::tidy(&mut book);
        }
        self.charge(header, 0, spent);
        self.retire(run, pending);
        // What the reply charged, stored before it is acked; one not stored now is stored by the run's next pass.
        let stored = async {
            self.save_ancestors(header).await?;
            self.save_header(slot).await
        };
        if let Err(e) = stored.await {
            tracing::warn!(run = %run, call = pending.call, error = format!("{e:#}"), "storing what a reply charged");
        }
        self.report_answered(header, pending, &entry.answer);
        self.wake(run);
        self.wake_starved(header);
        Ok(())
    }

    /// Mark the call `call` of the run of `slot` waiting on a human its step's recipient asked ([`steps::asked`]),
    /// stored with the run's session: the run waits on humans once every call it waits on does and none is queued.
    async fn waits_on_a_human(&self, slot: &Slot, call: u32) -> Result<()> {
        {
            let mut book = slot.book();
            let Some(pending) = book.pending.iter_mut().find(|pending| pending.call == call) else { return Ok(()) };
            if pending.asked {
                return Ok(());
            }
            pending.asked = true;
            if book.header.state == RunState::Running
                && book.queued.is_empty()
                && book.pending.iter().all(steps::on_a_human)
            {
                book.header.state = RunState::Waiting;
            }
        }
        self.save_session(slot).await?;
        self.save_header(slot).await
    }

    /// Take `message`, on the thread of a step to `to` the run `run` no longer waits on, when it is a notice, an
    /// escalation or a further reply of `to`, reporting it on the run's thread, and charging the run what it tells the
    /// runs answering the step spent.
    async fn take_retired(&self, run: &RunId, to: &UserId, message: &Envelope, answerer: &UserId) -> Result<bool> {
        // The conductor's invitation to rate a run started on the step's thread is no reply.
        let reply = answerer == to && message.trail_step.is_none() && message.rating_of.is_none();
        if !reply && message.notice.is_none() && message.escalation.is_none() {
            return Ok(false);
        }
        let Some(slot) = locked(&self.inner.runs).get(run).cloned() else { return Ok(true) };
        let call = self.inner.store.step(&message.thread_id).await.ok().flatten().map(|step| step.call);
        if let Some(call) = call {
            self.store_told(&slot, call, to, message).await?;
        }
        let run_thread = slot.header().thread;
        let stray = Stray { run, run_thread: &run_thread, call, to, waited: false };
        self.report_stray(stray, message).await;
        Ok(true)
    }

    /// Take `message`, on a thread the conductor knows of no run in memory, when the thread is a step's, as its
    /// [`ants_proto::TrailStepThread`] tells, and the message a notice, an escalation, or a reply of the step's
    /// recipient: one arriving after the run ended, or after a restart, reported on the run's thread. Only a message
    /// that may be one, replying or answering, is looked up. A message on a step of a run the daemon could not continue
    /// from its records yet is taken as the run, continued now, takes it; failing while the run cannot be.
    async fn take_late(&self, message: &Envelope, answerer: &UserId) -> Result<bool> {
        let (notice, escalation) = (message.notice.is_some(), message.escalation.is_some());
        let replying = message.in_reply_to.is_some() || message.answer.is_some();
        if message.trail_step.is_some() || !(replying || notice || escalation) {
            return Ok(false);
        }
        let Some(step) = self.inner.store.step(&message.thread_id).await? else { return Ok(false) };
        // A step of a run its daemon could not continue yet: taken once the run is, else tried again later.
        if self.unrestored(&step.run) && self.restored(&step.run).await?.is_some() {
            return Box::pin(self.take(message)).await;
        }
        if *answerer != step.to && !notice && !escalation {
            return Ok(false);
        }
        // After a restart, an answered step's thread is known from its record alone.
        let slot = locked(&self.inner.runs).get(&step.run).cloned();
        if let Some(slot) = slot {
            self.store_told(&slot, step.call, &step.to, message).await?;
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
    ) -> Result<Option<CallAnswer>> {
        let schema = AgentArgs::of(&pending.args).ok().and_then(|args| args.schema).unwrap_or_default();
        let Ok(again) = reply.reply(self.inner.ant.clone(), String::new(), Role::Ant) else {
            let error = format!("the reply did not match the schema, with no turns left to ask again: {why}");
            return Ok(Some(CallAnswer::Error(error)));
        };
        let step = steps::step(header, pending);
        let tips = self.step_tips(header).await;
        let tips = tips.and_then(|board| steps::retry_tips(&board, &step, Utc::now()));
        let again = Envelope { text: steps::again(why, &schema, tips.as_deref()), ..again }.as_step(step);
        let again = self.bounded(header, again);
        let asked = PendingCall {
            message: again.id,
            sent_at: again.sent_at,
            turn_budget: again.turn_budget,
            retries: pending.retries + 1,
            asked: false,
            sent: false,
            ..pending.clone()
        };
        // Stored unsent before it goes, from a copy: the run in memory changes once the re-ask went, so a reply
        // delivered again after a failure here is asked about once.
        self.save_session_with(slot, Some(&asked)).await?;
        if let Err(e) = (self.inner.outbox)(again.clone()).await {
            if let Err(e) = self.save_session(slot).await {
                tracing::warn!(run = %header.run, call = pending.call, error = format!("{e:#}"), "storing a trail run");
            }
            return Err(e);
        }
        {
            let mut book = slot.book();
            if let Some(call) = book.pending.iter_mut().find(|call| call.call == pending.call) {
                *call = PendingCall { sent: true, ..asked };
            }
            book.header.turns = book.header.turns.saturating_add(spent + 1);
        }
        self.charge(header, 0, spent + 1);
        // Gone already: one not stored now is stored by the run's next pass.
        let stored = async {
            self.save_ancestors(header).await?;
            self.save_session(slot).await?;
            self.save_header(slot).await
        };
        if let Err(e) = stored.await {
            tracing::warn!(run = %header.run, call = pending.call, error = format!("{e:#}"), "storing a re-ask");
        }
        tracing::info!(run = %header.run, call = pending.call, why, "trail step asked again");
        Ok(None)
    }

    /// The tips of the saved trail the run of `header` is on, for its steps: none for an inline script or a trail the
    /// colony does not save ([`TrailRun::tip_trail`]), nor, warned of, when they cannot be read, the steps then going
    /// without.
    pub(super) async fn step_tips(&self, header: &TrailRun) -> Option<TipBoard> {
        let trail = header.tip_trail()?;
        match self.inner.tips.board(trail).await {
            Ok(board) => Some(board),
            Err(e) => {
                let error = format!("{e:#}");
                tracing::warn!(run = %header.run, trail = %trail, error, "reading a trail's tips for its steps");
                None
            }
        }
    }
}
