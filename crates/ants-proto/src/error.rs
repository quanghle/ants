//! The [`Error`] of building and checking what travels: an invalid id, machine name, title, question, answer or
//! attachment, a body naming another user or channel than the subject or key it came on, and a thread with no turns
//! left.

#[cfg(doc)]
use crate::subject;
use crate::{ChannelId, MAX_CHANNEL_DESCRIPTION, MAX_TITLE, ThreadId, TrailName, UserId};

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
    #[error("a channel's description of {0} characters is too long: at most {MAX_CHANNEL_DESCRIPTION}")]
    LongDescription(usize),
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
    #[error("invalid attachment {name:?}: {reason}")]
    InvalidAttachment { name: String, reason: &'static str },
    #[error("{0} attachments: a message carries at most {max}", max = crate::MAX_ATTACHMENTS)]
    TooManyAttachments(usize),
    #[error("invalid attachment handle {0:?}: use <from>.<inbox|notes|chan>.<to, thread or channel>.<id>")]
    InvalidAttachmentHandle(String),
    #[error("invalid sense id {0:?}: use a ULID, 26 of uppercase Crockford base32 starting with 0-7")]
    InvalidSenseId(String),
    /// A saved trail found under another key than its name.
    #[error("trail {name} found at {at}")]
    ForeignTrail { name: TrailName, at: String },
    #[error("invalid trail {name}: {why}")]
    InvalidTrail { name: TrailName, why: String },
    #[error("trail limit {limit} of {value} is out of range: use 1-{max}")]
    TrailLimit { limit: &'static str, value: u64, max: u64 },
    #[error("invalid tip id {0:?}: use 8 of a-z and 2-7")]
    InvalidTipId(String),
    /// A trail tip or vote found under another key than its own.
    #[error("{kind} {key} found at {at}")]
    ForeignTip { kind: &'static str, key: String, at: String },
    #[error("invalid tip: {0}")]
    InvalidTip(String),
    #[error("invalid tip vote: {0}")]
    InvalidTipVote(String),
}
