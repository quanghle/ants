use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::{ATTEMPTS_BUCKET, Envelope, message_mark_key};
use crate::{ChannelId, Error, ThreadId, UserId, subject};

/// A human asking the daemons of `ant` to cancel the prompt running on `thread_id`, on [`subject::cancel`] with
/// core NATS: never stored, so only daemons connected at the time hear it. Each one answers with a [`CancelReply`].
/// An assistant asks for its human on [`subject::cancel_by`], which names it, and may ask to withdraw a message
/// waiting on the thread instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub id: Uuid,
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// Who asks: a human on [`subject::cancel`], which only human credentials publish to; on [`subject::cancel_by`],
    /// the sender it names.
    pub by: UserId,
    pub sent_at: DateTime<Utc>,
    /// The human that `by`, its assistant, asks for. A daemon honours the request only while the human's grant names
    /// `by` (as for an [`Envelope::on_behalf_of`]), and tells the human of what it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
    /// A message waiting on the thread to withdraw ([`MessageMark::Withdrawn`]), rather than cancel the prompt running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdraw: Option<Uuid>,
}

impl CancelRequest {
    pub fn new(ant: UserId, thread_id: ThreadId, by: UserId) -> Self {
        Self { id: Uuid::now_v7(), ant, thread_id, by, sent_at: Utc::now(), on_behalf_of: None, withdraw: None }
    }

    /// The request, asked by its assistant `by` for `human`, on [`subject::cancel_by`].
    #[must_use]
    pub fn for_human(self, human: UserId) -> Self {
        Self { on_behalf_of: Some(human), ..self }
    }

    /// The request, asking to withdraw `message`, waiting on the thread, instead: on [`subject::cancel_by`] only, which
    /// no daemon that cancels the prompt instead listens to.
    #[must_use]
    pub fn withdrawing(self, message: Uuid) -> Self {
        Self { withdraw: Some(message), ..self }
    }

    /// [`subject::cancel_by`] for a request for a human or a withdrawal, else [`subject::cancel`].
    pub fn subject(&self) -> String {
        if self.on_behalf_of.is_some() || self.withdraw.is_some() {
            subject::cancel_by(&self.ant, &self.thread_id, &self.by)
        } else {
            subject::cancel(&self.ant, &self.thread_id)
        }
    }

    pub fn reply_subject(&self) -> String {
        subject::cancelled(&self.ant, &self.thread_id)
    }

    /// Check that the request arrived on its own [`CancelRequest::subject`], so that a request naming another ant or
    /// thread than its subject is not acted on.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser {
            kind: "cancel request",
            user: self.ant.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
    }

    /// A daemon's answer: whether it had a prompt running on the thread, which it then cancelled, or withdrew the
    /// message.
    pub fn answer(&self, cancelled: bool) -> CancelReply {
        let (ant, thread_id) = (self.ant.clone(), self.thread_id.clone());
        CancelReply { request_id: self.id, ant, thread_id, cancelled, mark: None }
    }

    /// A daemon's answer to a withdrawal it could not make: the mark the message had already.
    pub fn found(&self, mark: MessageMark) -> CancelReply {
        CancelReply { mark: Some(mark), ..self.answer(false) }
    }
}

/// The answer of one daemon of `ant` to a [`CancelRequest`], on [`subject::cancelled`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelReply {
    pub request_id: Uuid,
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// Whether a prompt was running on the thread and is now cancelled, its partial reply following on the thread;
    /// for a withdrawal, whether the message is withdrawn.
    pub cancelled: bool,
    /// For a withdrawal not made, the mark the message had already.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mark: Option<MessageMark>,
}

impl CancelReply {
    pub fn subject(&self) -> String {
        subject::cancelled(&self.ant, &self.thread_id)
    }

    /// Check that the answer arrived on its own [`CancelReply::subject`], which credentials pin to the ant's own.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser {
            kind: "cancel answer",
            user: self.ant.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
    }
}

/// A human asking the daemons of `ant` which messages they hold, on [`subject::queue`] with core NATS. Each one answers
/// with a [`QueueReply`]. An assistant asks for its human on [`subject::queue_by`], which names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRequest {
    pub id: Uuid,
    pub ant: UserId,
    /// Who asks: a human on [`subject::queue`], which only human credentials publish to; on [`subject::queue_by`], the
    /// sender it names.
    pub by: UserId,
    pub sent_at: DateTime<Utc>,
    /// The human that `by`, its assistant, asks for, as in [`CancelRequest::on_behalf_of`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
}

impl QueueRequest {
    pub fn new(ant: UserId, by: UserId) -> Self {
        Self { id: Uuid::now_v7(), ant, by, sent_at: Utc::now(), on_behalf_of: None }
    }

    /// The request, asked by its assistant `by` for `human`, on [`subject::queue_by`].
    #[must_use]
    pub fn for_human(self, human: UserId) -> Self {
        Self { on_behalf_of: Some(human), ..self }
    }

    /// [`subject::queue_by`] for a request for a human, else [`subject::queue`].
    pub fn subject(&self) -> String {
        match self.on_behalf_of {
            Some(_) => subject::queue_by(&self.ant, &self.by),
            None => subject::queue(&self.ant),
        }
    }

    pub fn reply_subject(&self) -> String {
        subject::queued(&self.ant)
    }

    /// Check that the request arrived on its own [`QueueRequest::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "queue request", user: self.ant.clone(), at: subject.to_owned() })
    }

    /// A daemon's answer: the messages it holds, and where the ant's inbox and channel consumers stand.
    pub fn answer(
        &self,
        messages: Vec<QueuedMessage>,
        inbox: Option<ConsumerPlace>,
        posts: Option<ConsumerPlace>,
    ) -> QueueReply {
        QueueReply {
            request_id: self.id,
            ant: self.ant.clone(),
            messages,
            delivered: inbox.map(|place| place.delivered),
            undelivered: inbox.map(|place| place.undelivered),
            inbox,
            posts,
        }
    }
}

/// The answer of one daemon of `ant` to a [`QueueRequest`], on [`subject::queued`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReply {
    pub request_id: Uuid,
    pub ant: UserId,
    /// The messages the daemon holds, in flight or waiting their turn on their thread, by stream sequence.
    pub messages: Vec<QueuedMessage>,
    /// The stream sequence of the last message the ant's inbox consumer delivered: later inbox messages wait in the
    /// stream. None when the consumer's info could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered: Option<u64>,
    /// How many inbox messages wait in the stream, not delivered yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub undelivered: Option<u64>,
    /// Where the ant's inbox consumer stands, `delivered` and `undelivered` included, which readers older than it
    /// take instead. None when its info could not be read, or from a daemon older than it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox: Option<ConsumerPlace>,
    /// Where the ant's channel consumer, of every channel post, stands. None when the daemon serves no channel posts,
    /// its info could not be read, or the daemon is older than it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posts: Option<ConsumerPlace>,
}

/// Where one of an ant's consumers stands, by stream sequence, as a [`QueueReply`] tells.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerPlace {
    /// Every message up to this one is acked.
    pub acked: u64,
    /// The last message delivered: later ones wait in the stream.
    pub delivered: u64,
    /// How many wait in the stream, not delivered yet.
    pub undelivered: u64,
    /// How many delivered messages are not acked: in a daemon's hands, or back with the server awaiting redelivery
    /// (after a failed attempt, or from a daemon that stopped).
    pub ack_pending: u64,
}

impl QueueReply {
    pub fn subject(&self) -> String {
        subject::queued(&self.ant)
    }

    /// Check that the answer arrived on its own [`QueueReply::subject`], which credentials pin to the ant's own.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "queue answer", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// A message to an ant waiting to be answered, as [`QueueReply`] lists it and `ants queue` shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedMessage {
    /// The message's [`Envelope::id`], or a channel post's id.
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    /// The channel, for a channel post.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    pub sent_at: DateTime<Utc>,
    /// Its stream sequence.
    pub seq: u64,
    /// Whether the daemon is handling it now.
    pub running: bool,
    /// How many messages of its thread go before it: 0 for the one running, or next.
    pub ahead: usize,
    /// Whether it awaits redelivery after a failed attempt, which started it: the rest of its thread waits for it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub redelivery: bool,
}

/// What became of a message to an ant, in [`ATTEMPTS_BUCKET`] under its [`message_mark_key`]. The daemon creates the
/// mark [`MessageMark::Started`] right before handling the message, and a human creates it
/// [`MessageMark::Withdrawn`] (`ants withdraw`), or the daemon for an assistant's [`CancelRequest::withdraw`]: both
/// create the key only if it has no value, so the first wins and the other sees what it found. A daemon acks a withdrawn message without prompting with it, and puts
/// [`MessageMark::Finished`] once it acked a message it started. Marks expire with the stream's messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MessageMark {
    Started {
        thread_id: ThreadId,
        at: DateTime<Utc>,
    },
    Finished {
        thread_id: ThreadId,
        at: DateTime<Utc>,
    },
    Withdrawn {
        by: UserId,
        at: DateTime<Utc>,
        /// The human `by`, its assistant, withdrew the message for.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_behalf_of: Option<UserId>,
    },
}
