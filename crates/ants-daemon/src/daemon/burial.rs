//! Dead letters: burying a message never to be answered, or whose attempts ran out, in the hub's stream or, out of
//! its reach, in the machine's outbox until the hub is back.

use ants_proto::DeadLetter;
use anyhow::Result;
use async_nats::jetstream;

use crate::bus;
use crate::delivery::dead::{self, Burial, Burying};

use super::holds::Stalled;
use super::{Inbox, Incoming, MAX_DELIVER};

/// A dead letter to bury: a new one, or one kept in the machine's outbox on an earlier delivery.
#[derive(Clone, Copy)]
pub(super) enum Letter {
    New,
    Kept,
}

impl Inbox<'_> {
    /// Dead-letter a message that is never to be prompted with, keeping only an excerpt of its payload, and terminate
    /// it. One whose dead letter the hub cannot take, its store full or out of reach, waits in hand until it can
    /// ([`Inbox::hold_junk`]), as does one whose dead letter went to the machine's outbox: it is counted then, and
    /// terminated once the hub's stream holds its dead letter.
    pub(super) async fn bury_unanswerable(&self, msg: &jetstream::Message, error: &str) {
        tracing::warn!(error, subject = %msg.subject, "dead-lettering a message without answering it");
        let kept = self.holds.junk.release(msg);
        let buried = if kept {
            self.bury_kept_junk(msg, error).await
        } else {
            dead::bury(self.js, self.ant, msg, None, error).await
        };
        match buried {
            Ok(burial) => {
                if !kept {
                    self.metrics.buried();
                }
                if burial == Burial::Outbox {
                    self.hold_junk(
                        msg,
                        error,
                        true,
                        Stalled::OutOfReach,
                        "the dead letter is in this machine's outbox",
                    );
                }
            }
            Err(e) => match self.stalled_by(&e).await {
                Some(why) => self.hold_junk(msg, error, kept, why, &format!("{e:#}")),
                None => {
                    // Never redelivered, dead letter or not.
                    tracing::warn!(error = format!("{e:#}"), "dead-lettering");
                    bus::term(msg).await;
                }
            },
        }
    }

    /// [`Inbox::bury_unanswerable`] for `msg` whose dead letter went to the machine's outbox before: once the hub is in
    /// reach, the letter its stream took from there is the one kept, and no other is stored ([`Burying::bury_kept`]).
    async fn bury_kept_junk(&self, msg: &jetstream::Message, error: &str) -> Result<Burial> {
        if !bus::in_reach(self.js).await {
            return Ok(Burial::Outbox);
        }
        let dead = dead::letter_for(self.ant, msg, None, error, bus::max_payload(self.client))?;
        Burying { js: self.js, humans: self.humans, escalator: None }.bury_kept(msg, &dead, None).await
    }

    /// Store `msg` as a dead letter, escalating it up the ant's chain (in the end to the thread's owner, else the
    /// default human), and terminate it. A channel post names its thread's owner ([`Inbox::admit_post`]), and its
    /// envelope does too. The dead letter of a channel post keeps its payload.
    pub(super) async fn bury(&self, incoming: &Incoming, msg: &jetstream::Message, error: &str) -> Result<()> {
        let envelope = incoming.post.is_none().then_some(&incoming.envelope);
        let dead = dead::letter_for(self.ant, msg, envelope, error, bus::max_payload(self.client))?;
        self.bury_letter(incoming, msg, &dead, Letter::New).await
    }

    /// Bury `msg` as `dead` ([`Burying::bury_letter`]), or, [`Letter::Kept`] in the machine's outbox on an earlier
    /// delivery, as the letter the hub's stream may hold already ([`Burying::bury_kept`]). A dead letter kept in the
    /// machine's outbox is noted in `buried`, so the redelivery buries it again, even in a restarted daemon; on the last delivery, with its
    /// ack subject, for [`Inbox::finish_buried`]. The answer or reply kept on the machine for the message's retry is
    /// forgotten: none follows.
    pub(super) async fn bury_letter(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        dead: &DeadLetter,
        letter: Letter,
    ) -> Result<()> {
        let post = incoming.post.as_ref().map(|posted| &posted.post);
        let burying = Burying { js: self.js, humans: self.humans, escalator: Some(self.escalator) };
        let buried = match letter {
            Letter::Kept => burying.bury_kept(msg, dead, post).await,
            Letter::New => burying.bury_letter(msg, dead, post).await,
        };
        let burial = match buried {
            Ok(burial) => burial,
            Err(e) => {
                // The hub cannot take it now: kept on the machine, it is buried once it can, the message held in hand
                // meanwhile ([`Inbox::stalled_by`]).
                if matches!(letter, Letter::New) && self.stalled_by(&e).await.is_some() {
                    self.keep_letter(incoming, msg, dead).await;
                }
                return Err(e);
            }
        };
        if matches!(letter, Letter::New) {
            self.metrics.buried();
        }
        match burial {
            Burial::Hub(_) => self.marks.buried.forget(&incoming.envelope).await,
            Burial::Outbox => self.keep_letter(incoming, msg, dead).await,
        }
        self.marks.answered.forget_kept(&incoming.envelope).await;
        Ok(())
    }

    /// Note `dead`, the dead letter of `msg`, in `buried`, so the message's next handling buries it again; on the last
    /// delivery with its ack subject, for [`Inbox::finish_buried`].
    async fn keep_letter(&self, incoming: &Incoming, msg: &jetstream::Message, dead: &DeadLetter) {
        let last = msg.info().is_ok_and(|info| info.delivered >= MAX_DELIVER);
        let kept = dead::Kept {
            dead: dead.clone(),
            post: incoming.post.as_ref().map(|posted| posted.post.clone()),
            last_delivery: msg.reply.as_ref().filter(|_| last).map(ToString::to_string),
        };
        if let Err(e) = self.marks.buried.add(&incoming.envelope, &kept).await {
            // The redelivery buries it with an error of its own, under the same message id.
            tracing::warn!(error = format!("{e:#}"), "noting the dead letter");
        }
    }

    /// Every `retry_delay`, finish the burial of each dead letter kept in the machine's outbox on its message's last
    /// delivery once the hub is in reach ([`dead::finish`]): no redelivery comes to do it. Runs until dropped.
    pub(super) async fn finish_buried(&self) {
        let mut every = tokio::time::interval(self.pacing.retry_delay);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            every.tick().await;
            for (message, kept) in self.marks.buried.undelivered().await {
                let seq = kept.dead.stream_seq;
                match dead::finish(self.js, &kept, self.humans).await {
                    Ok(Some(_)) => {
                        self.marks.attempts.forget(seq).await;
                        self.marks.buried.forget_id(message).await;
                    }
                    Ok(None) => tracing::debug!(stream_seq = seq, "the hub is out of reach; the dead letter waits"),
                    Err(e) => tracing::warn!(stream_seq = seq, error = format!("{e:#}"), "dead-lettering"),
                }
            }
        }
    }
}
