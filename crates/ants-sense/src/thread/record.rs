//! What sense records of each thread of the ant from the thread's lifecycle events, never from what its messages said:
//! a [`Record`], an item of kind [`KIND`] named after the thread ([`ItemId::named`]), which sense alone writes
//! ([`BY`]), as it does the card. The thread's start makes it, with who started it, the repository the ant works in
//! and when; a later start, the thread's session gone, notes only when, and so do the thread marked done and its
//! session expiring. Each event changes the record once and a repeat nothing, so a thread takes about two events of
//! the log, its start and its end, none per prompt. An end of a thread sense never saw start records nothing.
//!
//! A clone inherits no record ([`crate::fork::KEPT`]); an export carries them with the rest of the log.

use ants_proto::ThreadId;
use ants_proto::provider::{Event, EventKind, Origin};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::identity::BY;
use crate::items::{ItemId, ItemValues};
use crate::store::{Items, StoreError, read_modify_write};

/// The kind of a thread's record.
pub const KIND: &str = "thread";
/// How many ids of the events last applied a record keeps, so a repeated event changes nothing.
const EVENTS_KEPT: usize = 8;

/// A thread's record, as sense keeps it: never written by the ant. Its revisions and their times are the item's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub thread: ThreadId,
    /// Who started the thread, and from which channel: the origin of its first start.
    pub origin: Origin,
    /// The repository of the ant's working directory when the thread first started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// When the thread first started.
    pub started: DateTime<Utc>,
    /// When it last started again, its session gone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restarted: Option<DateTime<Utc>>,
    /// When it was last marked done.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub done: Option<DateTime<Utc>>,
    /// When its session last expired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expired: Option<DateTime<Utc>>,
    /// The ids of the events last applied, the latest last.
    pub events: Vec<Uuid>,
}

impl ItemValues for Record {
    const KIND: &str = KIND;
}

impl Record {
    /// The record of `thread` started by `event`, from `origin` in `repo`.
    fn started(thread: &ThreadId, origin: &Origin, repo: Option<&String>, event: &Event) -> Self {
        Self {
            thread: thread.clone(),
            origin: origin.clone(),
            repo: repo.cloned(),
            started: event.at,
            restarted: None,
            done: None,
            expired: None,
            events: vec![event.id],
        }
    }

    /// Apply the lifecycle event `event`, unless applied already.
    fn apply(&mut self, event: &Event) {
        if self.events.contains(&event.id) {
            return;
        }
        match event.kind {
            EventKind::ThreadStarted { .. } => self.restarted = Some(event.at),
            EventKind::ThreadDone => self.done = Some(event.at),
            EventKind::ThreadExpired => self.expired = Some(event.at),
            _ => return,
        }
        self.events.push(event.id);
        let over = self.events.len().saturating_sub(EVENTS_KEPT);
        self.events.drain(..over);
    }
}

/// Whether events of `kind` change a thread's record.
pub fn touches_record(kind: &EventKind) -> bool {
    matches!(kind, EventKind::ThreadStarted { .. } | EventKind::ThreadDone | EventKind::ThreadExpired)
}

/// The record of `thread` in `items`; none before sense saw it start.
pub async fn record(items: &dyn Items, thread: &ThreadId) -> Result<Option<Record>, StoreError> {
    items.get(&ItemId::named(KIND, thread.as_str())).await?.as_ref().map(Record::of).transpose()
}

/// The records in `items`, the last changed first, up to `limit`.
pub async fn records(items: &dyn Items, limit: usize) -> Result<Vec<Record>, StoreError> {
    items.list(KIND, limit).await?.iter().map(Record::of).collect()
}

/// Apply the lifecycle event `event` to the record of its thread in `items`, made by the thread's start: the record as
/// it is then; none when the event is of no thread, or of one sense never saw start.
pub async fn record_event(items: &dyn Items, event: &Event) -> Result<Option<Record>, StoreError> {
    let Some(thread) = event.thread.as_ref().filter(|_| touches_record(&event.kind)) else { return Ok(None) };
    let id = ItemId::named(KIND, thread.as_str());
    read_modify_write(|| async {
        let item = items.get(&id).await?;
        let current = item.as_ref().map(Record::of).transpose()?;
        let record = match (&current, &event.kind) {
            (Some(current), _) => {
                let mut record = current.clone();
                record.apply(event);
                if record == *current {
                    return Ok(Some(record));
                }
                record
            }
            (None, EventKind::ThreadStarted { origin, repo }) => Record::started(thread, origin, repo.as_ref(), event),
            (None, _) => return Ok(None),
        };
        let expected = item.map_or(0, |item| item.revision);
        Record::of(&items.put(&id, KIND, record.values(), Some(expected), BY).await?).map(Some)
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ants_proto::provider::{BoxFuture, Provider, RpcError, Starter};
    use ants_proto::{SenseId, UserId};
    use chrono::TimeZone as _;

    use super::*;
    use crate::Sense;
    use crate::cache::Cache;
    use crate::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
    use crate::store::{Store, Stores};

    const THREAD: &str = "019a7d2c-0000-7000-8000-000000000001";

    /// The items of every ant: the same each time, or else what `error` makes.
    struct Kept(Result<Arc<dyn Items>, fn() -> StoreError>);

    impl Stores for Kept {
        fn items<'a>(&'a self, _ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
            Box::pin(std::future::ready(self.0.clone().map_err(|error| error())))
        }
    }

    async fn sense() -> (Sense, Arc<dyn Items>) {
        let verifier = AccountVerifier::new(vec![], NoRevocations);
        let ant = "dev-a".parse().unwrap();
        let log =
            AntLog::new(MemoryLog::default(), Unsigned, verifier, SenseId::new(), ant, "m1".parse().unwrap(), None);
        let store = Store::open(log.unwrap(), Cache::in_memory().unwrap()).await.unwrap();
        let items: Arc<dyn Items> = Arc::new(tokio::sync::Mutex::new(store));
        (Sense::with_stores(Arc::new(Kept(Ok(items.clone())))), items)
    }

    fn at(minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 1, 1, 10, minute, 0).unwrap()
    }

    fn origin(user: &str) -> Origin {
        Origin { by: Starter::Human { user: user.parse().unwrap() }, channel: None }
    }

    fn event(id: u128, minute: u32, kind: EventKind) -> Event {
        let thread = Some(THREAD.parse().unwrap());
        Event {
            id: Uuid::from_u128(id),
            ant: "dev-a".parse().unwrap(),
            identity: "0f1e".into(),
            thread,
            at: at(minute),
            kind,
        }
    }

    fn started(id: u128, minute: u32, user: &str, repo: &str) -> Event {
        event(id, minute, EventKind::ThreadStarted { origin: origin(user), repo: Some(repo.to_owned()) })
    }

    #[tokio::test]
    async fn each_event_changes_the_record_of_its_thread_once_and_a_repeat_nothing() {
        let (sense, items) = sense().await;
        let id = ItemId::named(KIND, THREAD);
        let revision = async || items.get(&id).await.unwrap().map_or(0, |item| item.revision);
        let told = [
            started(1, 0, "alice", "github.com/o/r"),
            event(2, 10, EventKind::ThreadExpired),
            started(3, 20, "bob", "github.com/o/other"),
            event(4, 30, EventKind::ThreadDone),
        ];
        for (event, revisions) in told.iter().zip(1..) {
            sense.event(event.clone()).await.unwrap();
            assert_eq!(revision().await, revisions, "{:?}", event.kind);
            // As when the message starting the thread is handled again.
            sense.event(Event { at: event.at + chrono::Duration::minutes(1), ..event.clone() }).await.unwrap();
            assert_eq!(revision().await, revisions, "a repeat of {:?}", event.kind);
        }

        let thread = THREAD.parse().unwrap();
        let record = record(&*items, &thread).await.unwrap().unwrap();
        let expected = Record {
            thread,
            origin: origin("alice"),
            repo: Some("github.com/o/r".to_owned()),
            started: at(0),
            restarted: Some(at(20)),
            done: Some(at(30)),
            expired: Some(at(10)),
            events: (1..=4).map(Uuid::from_u128).collect(),
        };
        assert_eq!(record, expected, "the first start kept");
        let item = items.get(&id).await.unwrap().unwrap();
        assert_eq!((item.kind.as_str(), item.updated.by.as_str()), (KIND, BY));
        assert_eq!(records(&*items, 10).await.unwrap(), [expected]);
    }

    #[tokio::test]
    async fn an_end_of_a_thread_sense_never_saw_start_records_nothing() {
        let (sense, items) = sense().await;
        sense.event(event(1, 0, EventKind::ThreadDone)).await.unwrap();
        sense.event(event(2, 0, EventKind::ThreadExpired)).await.unwrap();
        sense.event(Event { thread: None, ..started(3, 0, "alice", "github.com/o/r") }).await.unwrap();
        sense.event(event(4, 0, EventKind::SessionLost)).await.unwrap();
        assert_eq!(records(&*items, 10).await.unwrap(), []);
    }

    #[tokio::test]
    async fn a_thread_event_waits_while_the_data_of_the_ant_is_not_open_and_goes_where_none_is_kept() {
        let not_open = || StoreError::Unavailable("the host connection is not up yet".into());
        let sense = Sense::with_stores(Arc::new(Kept(Err(not_open))));
        let refused = sense.event(started(1, 0, "alice", "github.com/o/r")).await.unwrap_err();
        assert_eq!(refused.code, RpcError::unavailable("").code, "told again");

        let not_kept = || StoreError::NotKept("no host connection".into());
        let sense = Sense::with_stores(Arc::new(Kept(Err(not_kept))));
        sense.event(started(1, 0, "alice", "github.com/o/r")).await.unwrap();
    }
}
