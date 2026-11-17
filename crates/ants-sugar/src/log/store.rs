//! Where a sugar identity's log is stored ([`EventLog`]), and a store in memory ([`MemoryLog`]).

use std::future::Future;
use std::sync::Mutex;

use ants_proto::SugarId;
use chrono::Utc;

use super::Stored;

/// The store of every sugar identity's log, one subject each: the daemon's JetStream stream, or [`MemoryLog`].
/// Sequences are the store's, increasing across all identities, so an identity's messages need not have consecutive
/// ones; 0 stands for none.
pub trait EventLog {
    /// Store `payload` as the next message of `sugar`'s log, if its last message is still at sequence `expected_last`
    /// (0: none), and return the sequence it is stored at; else [`AppendError::Conflict`], naming the last.
    fn append(
        &self,
        sugar: &SugarId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, AppendError>> + Send;

    /// Up to `limit` messages of `sugar`'s log stored after sequence `after`, in order: fewer when the store answers
    /// with fewer at once, none past the last.
    fn read_after(
        &self,
        sugar: &SugarId,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Stored>, LogError>> + Send;

    /// The sequence of the last message of `sugar`'s log; 0 for none.
    fn last_sequence(&self, sugar: &SugarId) -> impl Future<Output = Result<u64, LogError>> + Send;

    /// What tells this store from another holding the same identities at the same sequences, such as the stream
    /// created again: a cache of a log built from another incarnation is to be built again.
    fn incarnation(&self) -> &str;
}

/// Why the store did not take or give messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct LogError(pub String);

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
    messages: Mutex<(Vec<(SugarId, Stored)>, u64)>,
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

    fn last(messages: &[(SugarId, Stored)], sugar: &SugarId) -> u64 {
        messages.iter().rev().find(|(id, _)| id == sugar).map_or(0, |(_, m)| m.sequence)
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
        sugar: &SugarId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, AppendError>> + Send {
        let (messages, used) = &mut *self.messages.lock().unwrap();
        let last = Self::last(messages, sugar);
        let appended = if last == expected_last {
            *used += 1;
            messages.push((sugar.clone(), Stored { sequence: *used, stored_at: Utc::now(), payload }));
            Ok(*used)
        } else {
            Err(AppendError::Conflict { last })
        };
        std::future::ready(appended)
    }

    fn read_after(
        &self,
        sugar: &SugarId,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Stored>, LogError>> + Send {
        let messages = self.messages.lock().unwrap();
        let found = messages.0.iter().filter(|(id, m)| id == sugar && m.sequence > after).take(limit);
        std::future::ready(Ok(found.map(|(_, m)| m.clone()).collect()))
    }

    fn last_sequence(&self, sugar: &SugarId) -> impl Future<Output = Result<u64, LogError>> + Send {
        std::future::ready(Ok(Self::last(&self.messages.lock().unwrap().0, sugar)))
    }

    fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

impl<T: EventLog + Sync> EventLog for &T {
    fn append(
        &self,
        sugar: &SugarId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, AppendError>> + Send {
        (**self).append(sugar, payload, expected_last)
    }

    fn read_after(
        &self,
        sugar: &SugarId,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Stored>, LogError>> + Send {
        (**self).read_after(sugar, after, limit)
    }

    fn last_sequence(&self, sugar: &SugarId) -> impl Future<Output = Result<u64, LogError>> + Send {
        (**self).last_sequence(sugar)
    }

    fn incarnation(&self) -> &str {
        (**self).incarnation()
    }
}
