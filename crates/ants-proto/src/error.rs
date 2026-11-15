#[cfg(doc)]
use crate::subject;
use crate::{ChannelId, MAX_TITLE, ThreadId, UserId};

/// Errors from parsing ants identifiers and building replies.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("invalid {kind} {value:?}: use 1-64 of a-z, 0-9 and '-', starting with a letter or digit")]
    InvalidId { kind: &'static str, value: String },
    #[error("thread {0} has no turns left")]
    BudgetExhausted(ThreadId),
    #[error("envelope from {from} to {to} arrived on {subject}")]
    WrongSubject { subject: String, from: UserId, to: UserId },
    /// A body naming another user than the key or subject it was stored under, which credentials pin.
    #[error("{kind} of {user} found at {at}")]
    ForeignUser { kind: &'static str, user: UserId, at: String },
    #[error("notes post from {from} on thread {thread} arrived on {subject}")]
    WrongNotesSubject { subject: String, from: UserId, thread: ThreadId },
    #[error("channel post from {from} in channel {channel} arrived on {subject}")]
    WrongChannelSubject { subject: String, from: UserId, channel: ChannelId },
    /// A body naming another channel than the key it was stored under.
    #[error("{kind} of channel {channel} found at {at}")]
    ForeignChannel { kind: &'static str, channel: ChannelId, at: String },
    #[error("invalid machine name {0:?}: use 1-64 of letters, digits, '-' and '_', starting with a letter or digit")]
    InvalidMachine(String),
    #[error("invalid thread title {0:?}: use one line of 1-{MAX_TITLE} characters")]
    InvalidTitle(String),
    #[error("invalid question: {0}")]
    InvalidQuestion(String),
    #[error("invalid answer: {0}")]
    InvalidAnswer(String),
    /// A renewal request naming other credentials than the subject it arrived on, or no credentials renewable over
    /// NATS ([`subject::renewable_name`]).
    #[error("renewal request of {name:?} found at {at}")]
    ForeignRenewal { name: String, at: String },
    #[error(
        "invalid device encryption key {0:?}: use the public key of an nkeys xkey, X followed by 55 base32 characters"
    )]
    InvalidDeviceKey(String),
}
