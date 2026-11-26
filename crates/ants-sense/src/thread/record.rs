//! What sense records of each thread of the ant from the thread's lifecycle events, never from what its messages said:
//! a [`Record`], an item of kind [`KIND`] named after the thread ([`ItemId::named`]), which sense alone writes
//! ([`BY`]), as it does the card. The thread's start makes it, with who started it, the repository the ant works in
//! and when; a later start, the thread's session gone, notes only when, and so do the thread marked done and its
//! session expiring. Each event changes the record once and a repeat nothing, so a thread takes about two events of
//! the log, its start and its end, and one more for each prompt ending with a plan the harness reported other than
//! the record's ([`EventKind::ThreadPlanned`]): the plan the brief shows ([`Plan`]), kept here rather than in the
//! resume register, which the ant replaces by compare-and-set and a write of sense's would refuse. An end, or a plan,
//! of a thread sense never saw start records nothing.
//!
//! A clone inherits no record ([`crate::fork::KEPT`]); an export carries them with the rest of the log.

use std::fmt::Write as _;

use ants_proto::provider::{Event, EventKind, Origin};
use ants_proto::{MAX_TIMELINE_STEPS, MAX_TIMELINE_TITLE, PlanStep, ThreadId};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::identity::BY;
use crate::items::{ItemId, ItemValues};
use crate::sanitize::{Limit, sanitize_title};
use crate::store::{Items, StoreError, read_modify_write};

/// The kind of a thread's record.
pub const KIND: &str = "thread";
/// How many ids of the events last applied a record keeps, so a repeated event changes nothing.
const EVENTS_KEPT: usize = 8;
/// The most characters of a plan step's status.
const STATUS_CHARS: usize = 20;

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
    /// The plan the harness last reported on the thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<Plan>,
    /// The ids of the events last applied, the latest last.
    pub events: Vec<Uuid>,
}

/// The plan a harness last reported on a thread, as a prompt ended with it: its steps, sanitized, and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub steps: Vec<PlanStep>,
    pub at: DateTime<Utc>,
}

impl Plan {
    /// The plan of `steps` reported at `at`: at most [`MAX_TIMELINE_STEPS`], each sanitized and cut, the blank ones
    /// left out.
    fn of(steps: &[PlanStep], at: DateTime<Utc>) -> Self {
        let clean = |text: &str, max: usize| {
            let text: String = text.trim().chars().take(max).collect();
            sanitize_title(&text, Limit::Chars(max)).map_or(text, |clean| clean.text.trim().to_owned())
        };
        let steps = steps
            .iter()
            .map(|step| PlanStep {
                content: clean(&step.content, MAX_TIMELINE_TITLE),
                status: clean(&step.status, STATUS_CHARS),
            })
            .filter(|step| !step.content.is_empty())
            .take(MAX_TIMELINE_STEPS)
            .collect();
        Self { steps, at }
    }

    /// The plan as brief text: a line a step, `[x]` done, `[>]` in progress, `[ ]` pending.
    pub fn text(&self) -> String {
        let at = self.at.to_rfc3339_opts(SecondsFormat::Secs, true);
        let mut out = format!("Plan the harness last reported ({at}):\n");
        for step in &self.steps {
            let mark = match step.status.as_str() {
                "completed" => "[x]",
                "in_progress" => "[>]",
                _ => "[ ]",
            };
            let _ = writeln!(out, "- {mark} {}", step.content);
        }
        out
    }
}

impl ItemValues for Record {
    const KINDS: &[&str] = &[KIND];
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
            plan: None,
            events: vec![event.id],
        }
    }

    /// Apply the lifecycle event `event`, unless applied already.
    fn apply(&mut self, event: &Event) {
        if self.events.contains(&event.id) {
            return;
        }
        match &event.kind {
            EventKind::ThreadStarted { .. } => self.restarted = Some(event.at),
            EventKind::ThreadDone => self.done = Some(event.at),
            EventKind::ThreadExpired => self.expired = Some(event.at),
            EventKind::ThreadPlanned { steps } => {
                let plan = Plan::of(steps, event.at);
                // The same plan reported again changes nothing, not even the events applied.
                if self.plan.as_ref().is_some_and(|kept| kept.steps == plan.steps) || plan.steps.is_empty() {
                    return;
                }
                self.plan = Some(plan);
            }
            _ => return,
        }
        self.events.push(event.id);
        let over = self.events.len().saturating_sub(EVENTS_KEPT);
        self.events.drain(..over);
    }
}

/// Whether events of `kind` change a thread's record.
pub fn touches_record(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::ThreadStarted { .. }
            | EventKind::ThreadDone
            | EventKind::ThreadExpired
            | EventKind::ThreadPlanned { .. }
    )
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

    use crate::log::MemoryLog;
    use crate::store::Stores;

    const THREAD: &str = "019a7d2c-0000-7000-8000-000000000001";

    /// The items of every ant: the same each time, or else what `error` makes.
    struct Kept(Result<Arc<dyn Items>, fn() -> StoreError>);

    impl Stores for Kept {
        fn items<'a>(&'a self, _ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
            Box::pin(std::future::ready(self.0.clone().map_err(|error| error())))
        }
    }

    async fn sense() -> (Sense, Arc<dyn Items>) {
        let items: Arc<dyn Items> =
            Arc::new(crate::testing::store(MemoryLog::default(), SenseId::new(), "dev-a".parse().unwrap()).await);
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
            plan: None,
            events: (1..=4).map(Uuid::from_u128).collect(),
        };
        assert_eq!(record, expected, "the first start kept");
        let item = items.get(&id).await.unwrap().unwrap();
        assert_eq!((item.kind.as_str(), item.updated.by.as_str()), (KIND, BY));
        assert_eq!(records(&*items, 10).await.unwrap(), [expected]);
    }

    fn step(content: &str, status: &str) -> PlanStep {
        PlanStep { content: content.to_owned(), status: status.to_owned() }
    }

    fn planned(id: u128, minute: u32, steps: Vec<PlanStep>) -> Event {
        event(id, minute, EventKind::ThreadPlanned { steps })
    }

    #[tokio::test]
    async fn a_plan_reported_otherwise_replaces_the_one_kept_and_the_same_plan_changes_nothing() {
        let (sense, items) = sense().await;
        let id = ItemId::named(KIND, THREAD);
        let revision = async || items.get(&id).await.unwrap().map_or(0, |item| item.revision);
        sense.event(planned(1, 0, vec![step("Read the code", "pending")])).await.unwrap();
        assert_eq!(revision().await, 0, "no record of a thread sense never saw start");

        sense.event(started(2, 1, "alice", "github.com/o/r")).await.unwrap();
        let first = vec![step("Read the code", "completed"), step("Write\u{202e} the fix", "in_progress")];
        sense.event(planned(3, 2, first.clone())).await.unwrap();
        assert_eq!(revision().await, 2);
        sense.event(planned(4, 3, first)).await.unwrap();
        assert_eq!(revision().await, 2, "the same plan from the next prompt changes nothing");
        sense.event(planned(5, 4, vec![step("  ", "pending")])).await.unwrap();
        assert_eq!(revision().await, 2, "a plan of blank steps is none");

        let thread = THREAD.parse().unwrap();
        let plan = record(&*items, &thread).await.unwrap().unwrap().plan.unwrap();
        let steps = vec![step("Read the code", "completed"), step("Write the fix", "in_progress")];
        assert_eq!(plan, Plan { steps, at: at(2) }, "sanitized");
        assert_eq!(
            plan.text(),
            "Plan the harness last reported (2025-01-01T10:02:00Z):\n- [x] Read the code\n- [>] Write the fix\n"
        );

        let next = vec![step("Ship it", "pending")];
        sense.event(planned(6, 5, next.clone())).await.unwrap();
        assert_eq!(revision().await, 3);
        let plan = record(&*items, &thread).await.unwrap().unwrap().plan.unwrap();
        assert_eq!(plan, Plan { steps: next, at: at(5) });
        let register = items.get(&ItemId::named(super::super::CHECKPOINT, THREAD)).await.unwrap();
        assert_eq!(register, None, "the resume register is the ant's alone");
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
