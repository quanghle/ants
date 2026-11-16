//! Dead letters: burying a message never to be answered, or whose attempts ran out, in the hub's stream or, out of
//! its reach, in the machine's outbox until the hub is back.

use ants_proto::DeadLetter;
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};

use crate::dead::{self, Burial};

use super::{Inbox, Incoming, MAX_DELIVER};

/// A dead letter to bury: a new one, or one kept in the machine's outbox on an earlier delivery.
#[derive(Clone, Copy)]
pub(super) enum Letter {
    New,
    Kept,
}

impl Inbox<'_> {
    /// Dead-letter a message that is never to be prompted with, keeping only an excerpt of its payload.
    pub(super) async fn bury_unanswerable(&self, msg: &jetstream::Message, error: &str) {
        tracing::warn!(error, subject = %msg.subject, "dead-lettering a message without answering it");
        match dead::bury(self.js, self.ant, msg, None, error).await {
            Ok(_) => self.metrics.buried(),
            Err(e) => {
                // Never redelivered, dead letter or not.
                tracing::warn!(error = format!("{e:#}"), "dead-lettering");
                if let Err(e) = msg.ack_with(AckKind::Term).await {
                    tracing::warn!(error = %e, "terminating the message");
                }
            }
        }
    }

    /// Store `msg` as a dead letter, escalating it up the ant's chain (in the end to the thread's owner, else the
    /// default human), and terminate it. A channel post names its thread's owner ([`Inbox::admit_post`]), and its
    /// envelope does too. The dead letter of a channel post keeps its payload.
    pub(super) async fn bury(&self, incoming: &Incoming, msg: &jetstream::Message, error: &str) -> Result<()> {
        let envelope = incoming.post.is_none().then_some(&incoming.envelope);
        let dead = dead::letter_for(self.ant, msg, envelope, error)?;
        self.bury_letter(incoming, msg, &dead, Letter::New).await
    }

    /// Bury `msg` as `dead` ([`dead::bury_letter`]), or, [`Letter::Kept`] in the machine's outbox on an earlier delivery,
    /// as the letter the hub's stream may hold already ([`dead::bury_kept`]). A dead letter kept in the machine's outbox is
    /// noted in `buried`, so the redelivery buries it again, even in a restarted daemon; on the last delivery, with its
    /// ack subject, for [`Inbox::finish_buried`].
    pub(super) async fn bury_letter(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        dead: &DeadLetter,
        letter: Letter,
    ) -> Result<()> {
        let post = incoming.post.as_ref().map(|posted| &posted.post);
        let escalator = Some(self.escalator);
        let burial = match letter {
            Letter::Kept => dead::bury_kept(self.js, msg, dead, post, self.humans, escalator).await?,
            Letter::New => {
                let burial = dead::bury_letter(self.js, msg, dead, post, self.humans, escalator).await?;
                self.metrics.buried();
                burial
            }
        };
        match burial {
            Burial::Hub(_) => self.buried.forget(&incoming.envelope).await,
            Burial::Outbox => {
                let last = msg.info().is_ok_and(|info| info.delivered >= MAX_DELIVER);
                let kept = dead::Kept {
                    dead: dead.clone(),
                    post: post.cloned(),
                    last_delivery: msg.reply.as_ref().filter(|_| last).map(ToString::to_string),
                };
                if let Err(e) = self.buried.add(&incoming.envelope, &kept).await {
                    // The redelivery buries it with an error of its own, under the same message id.
                    tracing::warn!(error = format!("{e:#}"), "noting the dead letter");
                }
            }
        }
        Ok(())
    }

    /// Every `retry_delay`, finish the burial of each dead letter kept in the machine's outbox on its message's last
    /// delivery once the hub is in reach ([`dead::finish`]): no redelivery comes to do it. Runs until dropped.
    pub(super) async fn finish_buried(&self) {
        let mut every = tokio::time::interval(self.retry_delay);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            every.tick().await;
            for (message, kept) in self.buried.undelivered().await {
                let seq = kept.dead.stream_seq;
                match dead::finish(self.js, &kept, self.humans).await {
                    Ok(Some(_)) => {
                        self.attempts.forget(seq).await;
                        self.buried.forget_id(message).await;
                    }
                    Ok(None) => tracing::debug!(stream_seq = seq, "the hub is out of reach; the dead letter waits"),
                    Err(e) => tracing::warn!(stream_seq = seq, error = format!("{e:#}"), "dead-lettering"),
                }
            }
        }
    }
}
