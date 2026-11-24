//! A human() step's question, asked of the run's owner as ask_human asks: of the owner's assistant first, when one
//! stands for it and can act ([`Escalator::ask_first`]). The assistant's answer on the step's thread, carrying the
//! question's escalation, answers the step for the owner; its pass-on, or no answer within escalate_after, sends the
//! question on to the owner, whose answer the step then takes. The step's end ends the question's escalation, so a
//! question answered or given up goes on to nobody.

use std::sync::Arc;

use ants_proto::{Envelope, EscalationReason, PendingCall, TrailFunction, UserId};
use anyhow::Result;

use super::Trails;
use crate::escalation::Escalator;

impl Trails {
    /// Ask the questions of human() steps through `escalator`, the conductor's: of the owner's assistant first. The
    /// first escalator set holds.
    pub fn with_escalator(&self, escalator: Arc<Escalator>) {
        let _ = self.inner.escalator.set(escalator);
    }

    /// Send `message`, the step of `call`: a human() step's question to the owner's assistant first, without the
    /// step's marker, as an ask_human question goes; else, and for any other step, to its recipient.
    pub(super) async fn send_step(&self, call: &PendingCall, message: Envelope) -> Result<()> {
        if call.function == TrailFunction::Human
            && let Some(escalator) = self.inner.escalator.get()
        {
            let asked = Envelope { trail_step: None, ..message.clone() };
            if let Some(assistant) = escalator.ask_first(&asked).await? {
                let (thread, human) = (&call.thread, &call.to);
                tracing::info!(%thread, %human, %assistant, "asked a human() step's question of the assistant first");
                return Ok(());
            }
        }
        (self.inner.outbox)(message).await
    }

    /// The human whose question, a human() step's asked of its assistant first, `message` answers: the assistant's
    /// message on the step's thread carrying the question's escalation it leads, while the human's grant names it.
    pub(super) fn assisted<'a>(&self, message: &'a Envelope) -> Option<&'a UserId> {
        let carried = message.escalation.as_deref()?;
        let human = carried.assistant_of.as_ref()?;
        let asked = carried.reason == EscalationReason::Question
            && carried.thread_id == message.thread_id
            && carried.stuck == self.inner.ant
            && carried.is_leader(&message.from);
        (asked && self.inner.humans.honours_assistant(human, &message.from, Some(human))).then_some(human)
    }

    /// Whether `message` is the escalator's to take: an assistant passing on a question asked of it first.
    pub(super) fn escalator_takes(&self, message: &Envelope) -> bool {
        self.inner.escalator.get().is_some_and(|escalator| escalator.passes_on(message))
    }

    /// End the escalation of the question of `call`, a human() step sent and no longer waited on, so that it goes on
    /// to nobody; failing to only warns.
    pub(super) async fn end_question(&self, call: &PendingCall) {
        if call.function != TrailFunction::Human || !call.sent {
            return;
        }
        let Some(escalator) = self.inner.escalator.get() else { return };
        if let Err(e) = escalator.ledger().reset(&call.thread).await {
            tracing::warn!(thread = %call.thread, error = format!("{e:#}"), "ending a human() step's question");
        }
    }
}
