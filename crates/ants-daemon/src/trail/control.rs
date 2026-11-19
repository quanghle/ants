//! The conductor's control subject ([`subject::trail`]): humans asking it to start, stop and resume runs. Admin orders
//! to stop and resume one ([`crate::admin::trail`]) are carried out the same way ([`Trails::act`]).

use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    RunId, RunState, TrailAction, TrailFunction, TrailOutcome, TrailReply, TrailRequest, UserId, WaitingStep, subject,
};
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;

use super::{StartRun, Trails, locked};

/// How long a failed subscription waits before the next try.
const RESUBSCRIBE: std::time::Duration = std::time::Duration::from_secs(5);
/// Most requests answered at once: a further one waits for one of those to be answered.
const ANSWERING: usize = 16;

impl Trails {
    /// Answer the requests on the conductor's control subject, never ending: several at once, so a stop waiting for
    /// a run's pass holds up no other request.
    pub(super) async fn hear(&self) {
        let subject = subject::trail(&self.inner.ant);
        // Held while a requested start is checked and stored, so one asked again under its request id starts one run.
        let starting = tokio::sync::Mutex::new(());
        loop {
            let mut requests = match self.inner.client.subscribe(subject.clone()).await {
                Ok(requests) => requests,
                Err(e) => {
                    tracing::warn!(error = %e, subject, "hearing trail requests");
                    tokio::time::sleep(RESUBSCRIBE).await;
                    continue;
                }
            };
            let mut answering = FuturesUnordered::new();
            loop {
                tokio::select! {
                    Some(()) = answering.next() => {}
                    message = requests.next(), if answering.len() < ANSWERING => match message {
                        Some(message) => answering.push(self.answer(message, &starting)),
                        None => break,
                    },
                }
            }
            while answering.next().await.is_some() {}
        }
    }

    async fn answer(&self, message: async_nats::Message, starting: &tokio::sync::Mutex<()>) {
        let request: TrailRequest = match serde_json::from_slice(&message.payload) {
            Ok(request) => request,
            Err(e) => {
                tracing::warn!(error = %e, "an unreadable trail request");
                return;
            }
        };
        let by = &request.by;
        let outcome = match request.check_subject(message.subject.as_str()) {
            Ok(()) if !self.inner.humans.is_human(by) => {
                TrailOutcome::Refused { error: format!("{by} is no human of the colony") }
            }
            Ok(()) => match &request.action {
                TrailAction::Start { .. } => {
                    let _one = starting.lock().await;
                    self.start_requested(&request).await
                }
                TrailAction::Stop { run } => {
                    if self.computing(run) {
                        self.reply(request.answer(TrailOutcome::Stopping { run: run.clone() })).await;
                    }
                    self.act(by, &request.action).await
                }
                TrailAction::Resume { .. } => self.act(by, &request.action).await,
            },
            Err(e) => TrailOutcome::Refused { error: e.to_string() },
        };
        self.reply(request.answer(outcome)).await;
    }

    async fn reply(&self, reply: TrailReply) {
        let published = match serde_json::to_vec(&reply) {
            Ok(payload) => self.inner.client.publish(reply.subject(), payload.into()).await.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(error) = published {
            tracing::warn!(error, request = %reply.request_id, "answering a trail request");
        }
    }

    /// Whether a pass of run `run` holds its engine: waits for a worker, computes or sends the steps it made.
    fn computing(&self, run: &RunId) -> bool {
        let slot = locked(&self.inner.runs).get(run).cloned();
        slot.is_some_and(|slot| slot.engine.try_lock().is_err())
    }

    /// Start the run `request` asks for under the request's id: when asked before, the run that request started.
    async fn start_requested(&self, request: &TrailRequest) -> TrailOutcome {
        let TrailAction::Start { trail, args, thread, limits } = &request.action else {
            return TrailOutcome::Refused { error: "no start was asked".to_owned() };
        };
        let by = &request.by;
        let run = match request.id.to_string().parse::<RunId>() {
            Ok(run) => run,
            Err(e) => return TrailOutcome::Refused { error: format!("the request's id makes no run id: {e}") },
        };
        match self.run(&run).await {
            Ok(Some(held)) if held.owner.as_ref() == Some(by) => return TrailOutcome::Started { run },
            Ok(Some(_)) => return TrailOutcome::Refused { error: format!("run {run} is another human's") },
            Ok(None) => {}
            Err(error) => return TrailOutcome::Refused { error },
        }
        let origin = Origin { by: Starter::Human { user: by.clone() }, channel: None };
        let start = StartRun {
            trail: trail.clone(),
            args: args.clone(),
            owner: Some(by.clone()),
            origin,
            thread: thread.clone(),
            limits: *limits,
            spends_from: None,
        };
        match self.start_as(run, start).await {
            Ok(run) => TrailOutcome::Started { run },
            Err(error) => TrailOutcome::Refused { error },
        }
    }

    /// Carry out `action` for `by`, a human: what a request on the control subject asks, or an admin's order. A stop of
    /// a stopped run lists the steps still to withdraw.
    pub(crate) async fn act(&self, by: &UserId, action: &TrailAction) -> TrailOutcome {
        let done = match action {
            TrailAction::Start { trail, args, thread, limits } => self
                .start_for(by, trail.clone(), args.clone(), thread.clone(), *limits)
                .await
                .map(|run| TrailOutcome::Started { run }),
            TrailAction::Stop { run } => match self.stop(run, by).await {
                Err(error) => self.still_waiting(run).ok_or(error),
                stopped => stopped,
            }
            .map(|waiting| TrailOutcome::Stopped { run: run.clone(), waiting }),
            TrailAction::Resume { run, limits } => {
                self.resume(run, by, *limits).await.map(|()| TrailOutcome::Resumed { run: run.clone() })
            }
        };
        done.unwrap_or_else(|error| TrailOutcome::Refused { error })
    }

    /// The steps to other ants that run `run`, stopped, waited on as it stopped, and those of its child runs: None
    /// when it is not stopped.
    fn still_waiting(&self, run: &RunId) -> Option<Vec<WaitingStep>> {
        let slot = locked(&self.inner.runs).get(run).cloned()?;
        let pending = {
            let book = slot.book();
            if !matches!(book.header.state, RunState::Stopped { .. }) {
                return None;
            }
            book.pending.clone()
        };
        let mut waiting = Vec::new();
        for call in pending {
            match call.function {
                TrailFunction::Agent if call.to != self.inner.ant => {
                    waiting.push(WaitingStep { to: call.to, message: call.message });
                }
                TrailFunction::Trail => {
                    waiting.extend(call.child().and_then(|child| self.still_waiting(&child)).unwrap_or_default());
                }
                _ => {}
            }
        }
        Some(waiting)
    }
}
