//! Where a sense identity's log is stored ([`EventLog`]), and a store in memory ([`MemoryLog`]).

use std::future::Future;
use std::sync::Mutex;

use ants_proto::SenseId;
use chrono::Utc;

use super::Stored;

/// The store of every sense identity's log, one subject each: the daemon's JetStream stream, or [`MemoryLog`].
/// Sequences are the store's, increasing across all identities, so an identity's messages need not have consecutive
/// ones; 0 stands for none.
pub trait EventLog {
    /// Store `payload` as the next message of `sense`'s log, if its last message is still at sequence `expected_last`
    /// (0: none), and return the sequence it is stored at; else [`AppendError::Conflict`], naming the last.
    fn append(
        &self,
        sense: &SenseId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, AppendError>> + Send;

    /// Up to `limit` messages of `sense`'s log stored after sequence `after`, in order: fewer when the store answers
    /// with fewer at once, none past the last.
    fn read_after(
        &self,
        sense: &SenseId,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Stored>, LogError>> + Send;

    /// The sequence of the last message of `sense`'s log; 0 for none.
    fn last_sequence(&self, sense: &SenseId) -> impl Future<Output = Result<u64, LogError>> + Send;

    /// What tells this store from another holding the same identities at the same sequences, such as the stream
    /// created again: a cache of a log built from another incarnation is to be built again.
    fn incarnation(&self) -> &str;
}

/// Why the store did not take or give messages: of which kind, set where the error is made, and in what words.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct LogError {
    pub kind: LogErrorKind,
    pub message: String,
}

/// The kinds of [`LogError`]. Only an unreachable store lets a write wait for it ([`crate::store::is_offline`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogErrorKind {
    /// The store did not answer: no connection, disconnected, timed out, no responders.
    Unreachable,
    /// The store, the signer or the credentials refused the message, such as another config, a permission or a
    /// signer failing, or other writers kept appending.
    Refused,
    /// What the log holds, or what is kept of it here, does not check out: a message missing or malformed, an event
    /// that could not be verified, or the chain status not kept.
    Integrity,
    /// The store is another incarnation than the one read before, such as the stream created again.
    Incarnation,
}

impl LogError {
    pub fn new(kind: LogErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    pub fn unreachable(message: impl Into<String>) -> Self {
        Self::new(LogErrorKind::Unreachable, message)
    }

    pub fn refused(message: impl Into<String>) -> Self {
        Self::new(LogErrorKind::Refused, message)
    }

    pub fn integrity(message: impl Into<String>) -> Self {
        Self::new(LogErrorKind::Integrity, message)
    }

    pub fn incarnation(message: impl Into<String>) -> Self {
        Self::new(LogErrorKind::Incarnation, message)
    }
}

/// Why the store did not append a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AppendError {
    /// The identity's last message is not the one the writer expected: another writer appended since.
    #[error("the log's last message is at sequence {last}, not the one expected")]
    Conflict { last: u64 },
    #[error(transparent)]
    Failed(#[from] LogError),
}

/// An [`EventLog`] in memory, for tests.
pub struct MemoryLog {
    incarnation: String,
    /// The messages, and the last sequence used, which no message removed gives back.
    messages: Mutex<(Vec<(SenseId, Stored)>, u64)>,
}

impl MemoryLog {
    pub fn new(incarnation: &str) -> Self {
        Self { incarnation: incarnation.to_owned(), messages: Mutex::new((Vec::new(), 0)) }
    }

    /// Replace the payload of the message at `sequence`, or with `None` remove the message, as no stream allows.
    pub fn rewrite(&self, sequence: u64, payload: Option<Vec<u8>>) {
        let messages = &mut self.messages.lock().unwrap().0;
        let at = messages.iter().position(|(_, m)| m.sequence == sequence).expect("no message at that sequence");
        match payload {
            Some(payload) => messages[at].1.payload = payload,
            None => drop(messages.remove(at)),
        }
    }

    fn last(messages: &[(SenseId, Stored)], sense: &SenseId) -> u64 {
        messages.iter().rev().find(|(id, _)| id == sense).map_or(0, |(_, m)| m.sequence)
    }
}

impl Default for MemoryLog {
    fn default() -> Self {
        Self::new("memory")
    }
}

impl EventLog for MemoryLog {
    fn append(
        &self,
        sense: &SenseId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, AppendError>> + Send {
        let (messages, used) = &mut *self.messages.lock().unwrap();
        let last = Self::last(messages, sense);
        let appended = if last == expected_last {
            *used += 1;
            messages.push((sense.clone(), Stored { sequence: *used, stored_at: Utc::now(), payload }));
            Ok(*used)
        } else {
            Err(AppendError::Conflict { last })
        };
        std::future::ready(appended)
    }

    fn read_after(
        &self,
        sense: &SenseId,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Stored>, LogError>> + Send {
        let messages = self.messages.lock().unwrap();
        let found = messages.0.iter().filter(|(id, m)| id == sense && m.sequence > after).take(limit);
        std::future::ready(Ok(found.map(|(_, m)| m.clone()).collect()))
    }

    fn last_sequence(&self, sense: &SenseId) -> impl Future<Output = Result<u64, LogError>> + Send {
        std::future::ready(Ok(Self::last(&self.messages.lock().unwrap().0, sense)))
    }

    fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

impl<T: EventLog + Sync> EventLog for &T {
    fn append(
        &self,
        sense: &SenseId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, AppendError>> + Send {
        (**self).append(sense, payload, expected_last)
    }

    fn read_after(
        &self,
        sense: &SenseId,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Stored>, LogError>> + Send {
        (**self).read_after(sense, after, limit)
    }

    fn last_sequence(&self, sense: &SenseId) -> impl Future<Output = Result<u64, LogError>> + Send {
        (**self).last_sequence(sense)
    }

    fn incarnation(&self) -> &str {
        (**self).incarnation()
    }
}
