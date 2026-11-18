//! Handling one message its thread's turn came for: taking it up unless withdrawn, answering it, and settling a
//! failed attempt as a retry, a hold or a dead letter.

use ants_proto::{Envelope, Hold, Role, State, ThreadId, UserId, WEBHOOK_ALLOW};
use anyhow::Result;
use async_nats::jetstream;

use crate::bus;
use crate::delivery::attempts::{Marker, TakenUp};
use crate::delivery::dead::Attempt;
use crate::harness::SignedOut;
use crate::limits::breaker::StartFailed;
use crate::limits::ratelimit::Limited;
use crate::monitoring::status;

use super::burial::Letter;
use super::delivery::reply_id;
use super::queue::{Place, Retry, queued_message};
use super::steering::Requeue;
use super::{InHand, Inbox, Incoming, MAX_ATTEMPTS, MAX_DELIVER, nak};

/// Who withdrew a message before the ant was prompted with it.
struct Withdrawal {
    by: UserId,
    /// The human an assistant withdrew it for.
    on_behalf_of: Option<UserId>,
    /// Whether the assistant recorded it under a key of its own, the human untold yet.
    recorded: bool,
    /// What the message was held for when it was withdrawn.
    held: Option<Hold>,
}

/// How handling a message ended.
pub(super) enum Handled {
    /// Answered, acked or buried.
    Done,
    /// Failed; its thread waits for the redelivery.
    Retry(Retry),
    /// The harness is signed out: the message, as it arrived, waits in hand for it to be signed in.
    SignedOut(Box<InHand>),
    /// A provider limit: the message, as it arrived, waits in hand for the backoff to pass.
    Limited(Box<InHand>),
    /// The harness keeps failing to start: the message, as it arrived, waits in hand for a check to start it.
    Broken(Box<InHand>),
    /// A fallback turned the message away: it goes, as it arrived, to the next one, or waits like the others.
    Failover(Box<InHand>),
    /// The message steered its thread's prompt, which did not take it or failed: it waits, as it arrived, for a
    /// prompt of its own, ahead of the others.
    Requeued(Box<InHand>),
}

impl Inbox<'_> {
    /// Answer one message. A failed attempt is redelivered after a delay, and the last one becomes a dead letter; one
    /// the harness failed for want of signing in, or for a provider limit, is no attempt, and is handed back to wait.
    /// Returns its thread, and how handling ended.
    pub(super) async fn handle(&self, mut incoming: Incoming, msg: jetstream::Message) -> (ThreadId, Handled) {
        let _working = self.workload.working();
        let arrived = incoming.clone();
        let envelope = &incoming.envelope;
        let thread = envelope.thread_id.clone();
        let (delivered, seq) = match msg.info() {
            Ok(info) => (info.delivered, info.stream_sequence),
            Err(e) => {
                self.bury_unanswerable(&msg, &format!("no delivery info: {e}")).await;
                return (thread, Handled::Done);
            }
        };
        self.thread_origin(&mut incoming, seq).await;
        self.sender_claims(&mut incoming, seq).await;
        self.owner_landed(&incoming, &msg).await;
        if self.settled_before(&incoming, &msg, seq).await {
            return (thread, Handled::Done);
        }
        let envelope = &incoming.envelope;
        let attempt = Attempt::of(delivered, MAX_ATTEMPTS);
        // Taken up before anything is done for it, unless a human withdrew it first (`ants withdraw`).
        let result = match self.take_up(envelope).await {
            Ok(Some(withdrawal)) => self.withdrawn(&incoming, &msg, seq, &withdrawal).await,
            Ok(None) => self.take(&mut incoming, &msg, seq, attempt).await,
            Err(e) => Err(e),
        };
        let Err(e) = result else {
            self.sign_in.settled();
            self.throttle.settled();
            self.breaker.settled();
            return (thread, Handled::Done);
        };
        if e.is::<Requeue>() {
            return (thread, Handled::Requeued(Box::new((arrived, msg))));
        }
        if self.passed_over(&e, &arrived).await {
            return (thread, Handled::Failover(Box::new((arrived, msg))));
        }
        match e.downcast_ref::<StartFailed>() {
            Some(failed) => {
                if let Some(hold) = self.breaker.failed(&thread, &self.redactor.redact(&failed.detail)) {
                    self.attempts.release(&arrived.envelope).await;
                    self.wait_for_repair(&arrived, failed, hold).await;
                    return (thread, Handled::Broken(Box::new((arrived, msg))));
                }
            }
            None => self.breaker.settled(),
        }
        if let Some(signed_out) = e.downcast_ref::<SignedOut>() {
            self.wait_for_sign_in(&arrived, signed_out).await;
            return (thread, Handled::SignedOut(Box::new((arrived, msg))));
        }
        self.sign_in.settled();
        if let Some(limited) = e.downcast_ref::<Limited>() {
            self.wait_out_limit(&arrived, limited).await;
            return (thread, Handled::Limited(Box::new((arrived, msg))));
        }
        self.throttle.settled();
        tracing::error!(%thread, error = format!("{e:#}"), "handling message");
        status::state(self.client, self.ant, &thread, State::Failed);
        // A failed last attempt, or an overdue one that failed to send its recorded reply: no delivery follows.
        if attempt != Attempt::Early {
            match self.bury(&incoming, &msg, &format!("{e:#}")).await {
                Ok(_) => {
                    self.attempts.forget(seq).await;
                    self.attempts.finish(&incoming.envelope).await;
                    return (thread, Handled::Done);
                }
                // The overdue delivery tries again.
                Err(bury) => tracing::warn!(error = format!("{bury:#}"), "dead-lettering"),
            }
        }
        // Withdrawable until its redelivery, unless that only sends the reply it stored again.
        if delivered < MAX_DELIVER
            && !matches!(self.attempts.get(seq, &incoming.envelope).await, Ok(Some(Marker::Replied(_))))
        {
            self.attempts.retrying(&incoming.envelope, None).await;
        }
        nak(&msg, Some(self.retry_delay)).await;
        self.metrics.retried();
        let retry = (delivered < MAX_DELIVER).then(|| Retry {
            seq,
            // Redelivery may lag behind the delay; past this the thread goes on without the message.
            until: tokio::time::Instant::now() + self.retry_delay * 3,
            queued: queued_message(&arrived, &msg, Place::Retry),
        });
        (thread, retry.map_or(Handled::Done, Handled::Retry))
    }

    /// Finish `incoming`, the message at stream sequence `seq`, if an earlier delivery of it settled it already: its
    /// reply or dead letter is stored. True if so.
    async fn settled_before(&self, incoming: &Incoming, msg: &jetstream::Message, seq: u64) -> bool {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        if self.answered.has(envelope).await {
            // Its ack failed, as while a leaf node's uplink is down; its reply is in the stream or on its way there.
            tracing::info!(%thread, "acking a message whose reply is stored already");
            match bus::double_ack(self.client, msg).await {
                Ok(()) => {
                    self.attempts.forget(seq).await;
                    self.attempts.finish(envelope).await;
                    self.answered.forget(envelope).await;
                }
                Err(e) => tracing::warn!(%thread, error = %e, "acking a message whose reply is stored"),
            }
            return true;
        }
        let Some(kept) = self.buried.get(envelope).await else { return false };
        // Its dead letter went to the machine's outbox while the hub was out of reach; the redelivery finishes it.
        tracing::info!(%thread, "burying a message whose dead letter is in the outbox");
        match self.bury_letter(incoming, msg, &kept.dead, Letter::Kept).await {
            Ok(()) => {
                self.attempts.forget(seq).await;
                self.attempts.finish(envelope).await;
            }
            Err(e) => tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering"),
        }
        true
    }

    /// Handle the message at stream sequence `seq`, taken up: answer it, or bury it when overdue. Marks it finished
    /// once it is acked or buried.
    async fn take(&self, incoming: &mut Incoming, msg: &jetstream::Message, seq: u64, attempt: Attempt) -> Result<()> {
        let marker = self.attempts.get(seq, &incoming.envelope).await?;
        if attempt == Attempt::Overdue && !matches!(marker, Some(Marker::Replied(_))) {
            let error = format!("no outcome after {MAX_ATTEMPTS} attempts: the daemon stopped or the ack wait ran out");
            match self.bury(incoming, msg, &error).await {
                Ok(_) => {
                    self.attempts.forget(seq).await;
                    self.attempts.finish(&incoming.envelope).await;
                }
                Err(e) => {
                    let thread = &incoming.envelope.thread_id;
                    tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering");
                }
            }
            return Ok(());
        }
        if self.arrived(incoming).await? {
            self.thread_started(incoming).await;
            Box::pin(self.answer(incoming, msg, seq, marker)).await?;
        } else {
            self.done(msg, seq).await?;
        }
        self.attempts.finish(&incoming.envelope).await;
        Ok(())
    }

    /// Mark `envelope` as started right before handling it ([`Attempts::take_up`]), unless it was withdrawn first: by
    /// a human (`ants withdraw`), by an assistant for its human through a daemon of the ant ([`Self::cancel_for`]), or
    /// by the assistant of the message's human, its sender or the one it was sent for, under a key of the assistant's
    /// own while no daemon answered ([`crate::delivery::attempts::record_withdrawal`]), honoured as a cancel request is: while the
    /// human's grant names the assistant.
    async fn take_up(&self, envelope: &Envelope) -> Result<Option<Withdrawal>> {
        let held = match self.attempts.take_up(envelope).await? {
            TakenUp::Withdrawn { by, on_behalf_of, held } => {
                return Ok(Some(Withdrawal { by, on_behalf_of, recorded: false, held }));
            }
            TakenUp::Started(held) => held,
        };
        let sender = Some(&envelope.from).filter(|from| self.humans.is_human(from));
        let mut humans: Vec<&UserId> = sender.into_iter().chain(envelope.on_behalf_of.as_ref()).collect();
        humans.dedup();
        for human in humans {
            let Some(assistant) = self.humans.assistant(human) else { continue };
            let Some((claimed, started)) = self.attempts.withdrawal_by(envelope, &assistant).await? else { continue };
            if &claimed != human {
                tracing::warn!(%assistant, %claimed, %human, "ignoring a withdrawal for a human the message is not of");
                continue;
            }
            if self.assisted(&assistant, Some(&claimed), "withdrawal").await.is_none() {
                continue;
            }
            if self.attempts.withdraw_started(envelope, &assistant, &claimed, started, held).await? {
                return Ok(Some(Withdrawal { by: assistant, on_behalf_of: Some(claimed), recorded: true, held }));
            }
            tracing::warn!(message = %envelope.id, "the mark of a message changed while honouring its withdrawal");
        }
        Ok(None)
    }

    /// Ack the message at stream sequence `seq`, which `withdrawal.by` withdrew before the ant was prompted with it, or
    /// retried it after a failed attempt or a hold for a sign-in or a provider limit, once a notice of it is stored on
    /// its thread, so `ants thread show` tells what became of it. The notice answers the sender, as a reply would,
    /// unless no reply can go to it: a webhook's goes to the thread's owner, and one to an ant out of turns, or to a
    /// channel post, to the human who withdrew it. The human an assistant withdrew a message for by a record of its own
    /// is told too, unless the notice goes to it.
    async fn withdrawn(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        withdrawal: &Withdrawal,
    ) -> Result<()> {
        let (by, on_behalf_of) = (&withdrawal.by, withdrawal.on_behalf_of.as_ref());
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        tracing::info!(%thread, %by, ?on_behalf_of, message = %envelope.id, "acking a withdrawn message unprompted");
        let ant = self.ant;
        // A failed attempt leaves its marker, to warn the retry.
        let before = match (withdrawal.held, self.attempts.get(seq, envelope).await) {
            (Some(held), _) => format!("before {ant} retried it after holding it for {held}"),
            (None, Ok(Some(_))) => format!("before {ant} retried it after a failed attempt"),
            (None, Ok(None) | Err(_)) => format!("before {ant} was prompted with it"),
        };
        let text = match on_behalf_of {
            None => format!("[ants] Withdrawn by {by} {before} (ants withdraw)."),
            Some(human) => format!("[ants] Withdrawn by {by} for {human} {before}."),
        };
        // The one who asked for the withdrawal: the human an assistant acted for.
        let by = on_behalf_of.unwrap_or(by);
        let to_human = |to: UserId| {
            let mut notice =
                envelope.follow_up(self.ant.clone(), to, text.clone()).as_notice(ants_proto::NoticeKind::Withdrawn);
            notice.in_reply_to = Some(envelope.id);
            notice
        };
        let notice = if incoming.post.is_some() {
            to_human(by.clone())
        } else if envelope.webhook {
            to_human(self.humans.human_owner(envelope.owner.as_ref()).unwrap_or_else(|| by.clone()))
        } else {
            match envelope.reply(self.ant.clone(), text.clone(), self.humans.sender_role(envelope)) {
                Ok(reply) => reply
                    .with_escalation(envelope.escalation.as_deref().cloned())
                    .as_notice(ants_proto::NoticeKind::Withdrawn),
                Err(_) => to_human(by.clone()),
            }
        };
        bus::store(self.js, notice.subject(), &notice, &reply_id(self.ant, envelope)).await?;
        if let Some(human) = on_behalf_of.filter(|human| withdrawal.recorded && **human != notice.to) {
            let (assistant, message) = (&withdrawal.by, envelope.id);
            let text = format!(
                "[ants] {assistant}, as the assistant of {human}, withdrew message {message} from the queue of {ant} \
                 on thread {thread} for {human}."
            );
            let told = Envelope::new(ant.clone(), human.clone(), text)
                .with_owner(Some(human.clone()))
                .as_notice(ants_proto::NoticeKind::Assistant);
            bus::store(self.js, told.subject(), &told, &format!("on-behalf-withdrawn-{message}")).await?;
        }
        self.done(msg, seq).await
    }

    /// Note the message the ant is about to answer with its escalator: a human's ends the thread's escalation and
    /// gives the thread a fresh allowance, and the escalation leader's answer gives the thread a fresh budget and
    /// allowance once per level ([`Escalator::arrived`]). The leader's reply that does so on a thread the ant got
    /// stuck on in a channel is answered in that channel, with the fresh budget: the thread's turns come from its posts.
    /// The message takes the tool rules its thread is narrowed to ([`Inbox::narrow`]).
    /// Returns whether to answer the message: a member's notice that it sent the ant's escalation higher is the
    /// daemon's alone ([`Escalator::took_climb`]), and so is the sage's answer to the review of a dream, unless its
    /// findings prompt the ant ([`crate::sense::Sense::review_answer`]).
    async fn arrived(&self, incoming: &mut Incoming) -> Result<bool> {
        let sender = self.humans.sender_role(&incoming.envelope);
        if let Some(sense) = self.sense.filter(|_| incoming.post.is_none() && sender == Role::Ant)
            && let Some(prompt) = sense.review_answer(self.ant, &mut incoming.envelope).await?
        {
            return Ok(prompt);
        }
        // Only a human asks for a summary.
        if !sender.is_human() {
            incoming.envelope.summary = None;
        }
        if incoming.post.is_none() && !sender.is_human() && self.escalator.took_climb(&incoming.envelope).await? {
            return Ok(false);
        }
        let renewed = self.escalator.arrived(&mut incoming.envelope, sender).await?;
        self.narrow(incoming, sender).await?;
        // Not a message from ask_ant or send_message: an ask_ant call waits for the answer in the leader's inbox.
        if renewed && incoming.post.is_none() && incoming.envelope.in_reply_to.is_some() {
            let pending = self.escalator.ledger().pending(&incoming.envelope.thread_id).await?;
            incoming.resume = pending.and_then(|p| p.channel);
        }
        Ok(true)
    }

    /// Set the tool rules of the message, and of its post, to those its thread is narrowed to: a human's message lifts
    /// the narrowing ([`Ledger::reset`]) and has none, a thread keeps the first rules a message brought it, and a
    /// webhook's message brings [`WEBHOOK_ALLOW`] when it has none of its own.
    async fn narrow(&self, incoming: &mut Incoming, sender: Role) -> Result<()> {
        let envelope = &mut incoming.envelope;
        let carried = envelope.allow.take();
        if !sender.is_human() {
            let carried = match carried {
                None if envelope.webhook => Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()),
                carried => carried,
            };
            envelope.allow = self.ledger.narrowed(&envelope.thread_id, carried).await?;
        }
        if let Some(posted) = &mut incoming.post {
            posted.post.allow.clone_from(&envelope.allow);
        }
        Ok(())
    }

    /// Tell the ant's provider of the thread of `incoming` when it has no session yet: a thread new to the ant, or one
    /// whose session expired. Told again, with the same id, when the message is handled again.
    async fn thread_started(&self, incoming: &Incoming) {
        let Some(lifecycle) = self.lifecycle else { return };
        let envelope = &incoming.envelope;
        if self.pool.has_session(&envelope.thread_id).await {
            return;
        }
        let origin = crate::provider::context::origin(envelope, incoming.channel().cloned(), &self.humans.humans());
        lifecycle.thread_started(&envelope.thread_id, &envelope.id.to_string(), origin).await;
    }

    /// Ack the message at stream sequence `seq`, which needs no reply.
    pub(super) async fn done(&self, msg: &jetstream::Message, seq: u64) -> Result<()> {
        bus::double_ack(self.client, msg).await?;
        self.attempts.forget(seq).await;
        Ok(())
    }
}
