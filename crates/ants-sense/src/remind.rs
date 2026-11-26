//! Durable reminder scheduling. Preparing a firing persists its identity before a wake is attempted; only a
//! confirmed delivery advances its schedule. A replacement daemon therefore retries the same wake identity.
//! Commitment reminders are automatically renewed through their due time: their expiry is 30 days after the later
//! of creation and the commitment's due time, and explicit renewal never shortens that due-anchored lifetime.
//! `source_context` and `workdir_machine` preserve creation provenance; `writer` follows the latest tool mutation.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ants_proto::ThreadId;
use ants_proto::provider::{BoxFuture, CallContext, ToolCall, TriggerRow};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::brief::Listing;
use crate::items::{Item, ItemId, ItemValues, changed, readable, stamp};
use crate::store::{Items, StoreError, Write, read_modify_write};
use crate::{Answer, Sense};

mod condition;
mod tool;
mod trigger;
pub mod watch;

pub use condition::{CommandDigest, Condition, FileEvent, IssueState, PrState, SenseEvent};
pub use trigger::Trigger;
pub use watch::{Watch, Watched};

/// Whether sense `event` holds at `now` among `items`, of item `id` alone when given: the observation of a
/// [`Condition::Sense`].
pub async fn sense_condition(
    items: &dyn Items,
    event: SenseEvent,
    id: Option<&ItemId>,
    now: DateTime<Utc>,
) -> Result<bool, StoreError> {
    match event {
        SenseEvent::CommitmentOverdue => crate::promise::overdue(items, id, now).await,
        SenseEvent::LeaseExpired => crate::task::lease_expired(items, id, now).await,
    }
}

pub const KIND: &str = "reminder";
/// The most reminders active at once, watchers and commitments' included.
pub const MAX_ACTIVE: usize = 50;
pub const CONDITION_DAYS: i64 = 30;
const SCHEDULER: &str = "sense:scheduler";
/// The tool call listing the reminders, which the brief's sections of them end with when cut.
const LIST: &str = "sense_remind action=list";

/// Why a reminder is not set or changed: the ant's quota of active reminders, a trigger giving no time to fire at, or
/// its store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RemindError {
    #[error("at most {MAX_ACTIVE} reminders and watchers may be active")]
    TooMany,
    #[error("{0}")]
    Trigger(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// The refusal of a reminder: `too_many` or `invalid_trigger`, else as its store refuses.
impl From<RemindError> for Answer {
    fn from(error: RemindError) -> Self {
        match error {
            RemindError::TooMany => {
                let message = format!(
                    "At most {MAX_ACTIVE} reminders may be active, watchers and commitments' included: drop one or \
                     resolve a commitment first."
                );
                Self::error("too_many", message)
            }
            RemindError::Trigger(why) => Self::error("invalid_trigger", why),
            RemindError::Store(error) => error.into(),
        }
    }
}

/// A reminder's error met by the scheduler, a stored trigger giving no time counting as sense's data unavailable.
impl From<RemindError> for StoreError {
    fn from(error: RemindError) -> Self {
        match error {
            RemindError::Store(error) => error,
            error => Self::Unavailable(error.to_string()),
        }
    }
}

/// A reminder, as its item keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reminder {
    pub title: String,
    /// The thread its firings wake.
    pub thread: Option<ThreadId>,
    pub trigger: Trigger,
    pub status: Status,
    /// When it is next due, delivered again or checked; none once it fires no more.
    pub next_due: Option<DateTime<Utc>>,
    pub first_due: Option<DateTime<Utc>>,
    /// How many times it fired, numbering its firings.
    #[serde(default)]
    pub number: u64,
    /// How many times its current occurrence fired: its first delivery, then the refires.
    #[serde(default)]
    pub occurrence_firings: u64,
    /// The context of the latest tool call changing it.
    pub writer: CallContext,
    /// The context of the tool call creating it.
    pub source_context: Option<CallContext>,
    /// When it expires unless renewed, a pending firing delivered still.
    pub expires: Option<DateTime<Utc>>,
    /// The condition checks in a row that saw no change, which slow the checks.
    pub unchanged_checks: Option<u64>,
    /// The working directory of the thread creating it, on machine `workdir_machine`.
    pub workdir: Option<PathBuf>,
    pub workdir_machine: Option<String>,
    /// The commitment it reminds of.
    pub promise: Option<ItemId>,
    /// The firing persisted before its wake, until its delivery is confirmed.
    pub pending: Option<Firing>,
    pub delivered: Option<Delivered>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    /// Whether postponed: its condition is not checked again before it fires.
    #[serde(default, with = "crate::items::flag")]
    pub snoozed: bool,
    /// What the last check of its condition saw, of a command only the digest of its exit status and stdout.
    pub observation: Option<Value>,
    /// When the latest condition check was reserved.
    pub check_started: Option<DateTime<Utc>>,
    /// A watcher's checks in a row that failed, its command refused or past its time among them.
    pub failed_checks: Option<u64>,
    /// Why a watcher's latest check failed, while its checks keep failing.
    pub check_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Active,
    /// Fired for good, waiting for an acknowledgement.
    Exhausted,
    Acknowledged,
    Dropped,
    Expired,
    /// A watcher whose check reported it done, retired after its last firing.
    Done,
}

impl Status {
    pub fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Exhausted => "exhausted",
            Self::Acknowledged => "acknowledged",
            Self::Dropped => "dropped",
            Self::Expired => "expired",
            Self::Done => "done",
        }
    }
}

/// A firing whose delivery was confirmed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delivered {
    pub id: String,
    pub at: DateTime<Utc>,
}

impl ItemValues for Reminder {
    const KINDS: &[&str] = &[KIND];
}

impl Reminder {
    /// A new active reminder of `trigger`, first due at `first_due`, made by the call of `writer` from the thread's
    /// working directory `workdir`.
    pub fn new(
        title: String,
        thread: ThreadId,
        trigger: Trigger,
        first_due: DateTime<Utc>,
        expires: DateTime<Utc>,
        writer: &CallContext,
        workdir: Option<&Path>,
    ) -> Self {
        Self {
            title,
            thread: Some(thread),
            unchanged_checks: (trigger.condition().is_some() || trigger.watch().is_some()).then_some(0),
            trigger,
            status: Status::Active,
            next_due: Some(first_due),
            first_due: Some(first_due),
            number: 0,
            occurrence_firings: 0,
            writer: writer.clone(),
            source_context: Some(writer.clone()),
            expires: Some(expires),
            workdir: workdir.map(|workdir| PathBuf::from(workdir.to_string_lossy().into_owned())),
            workdir_machine: workdir.map(|_| writer.machine.clone()),
            promise: None,
            pending: None,
            delivered: None,
            acknowledged_at: None,
            snoozed: false,
            observation: None,
            check_started: None,
            failed_checks: None,
            check_error: None,
        }
    }

    /// Whether it expired: past its expiry with no firing pending.
    pub fn expired(&self, now: DateTime<Utc>) -> bool {
        self.pending.is_none() && self.expires.is_some_and(|at| at <= now)
    }

    /// The condition still to be checked before its occurrence fires: none once it fired or was postponed.
    pub fn condition(&self) -> Option<&Condition> {
        let unfired = self.pending.is_none() && self.occurrence_firings == 0 && !self.snoozed;
        self.trigger.condition().filter(|_| unfired)
    }

    /// The watcher to check: none for another trigger, while a firing of it is pending, or once it is not active.
    pub fn watch(&self) -> Option<&Watch> {
        self.trigger.watch().filter(|_| self.pending.is_none() && self.status == Status::Active)
    }

    /// The digest of a watcher's latest successful check.
    pub fn digest(&self) -> Option<&str> {
        self.observation.as_ref()?.get("digest")?.as_str()
    }

    /// The context its condition is checked in: that of its creation, in the thread it wakes.
    pub fn source(&self) -> Option<CallContext> {
        let thread = self.thread.clone()?;
        Some(CallContext { thread, ..self.source_context.clone().unwrap_or_else(|| self.writer.clone()) })
    }

    fn renewed_expiry(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, RemindError> {
        let due = self.promise.as_ref().and(self.first_due).unwrap_or(now);
        add(now.max(due), Duration::days(CONDITION_DAYS))
    }

    /// End the occurrence fired: due at the trigger's next, else fired for good.
    fn finish_occurrence(&mut self, now: DateTime<Utc>) -> Result<(), StoreError> {
        match self.trigger.next(now)? {
            Some(next) => {
                self.next_due = Some(next);
                self.occurrence_firings = 0;
            }
            None => {
                self.next_due = None;
                self.status = Status::Exhausted;
            }
        }
        Ok(())
    }

    fn drop_it(&mut self) {
        self.pending = None;
        self.next_due = None;
        self.status = Status::Dropped;
    }
}

/// One due trigger. A pending delivery has no condition to re-evaluate.
#[derive(Debug, Clone, PartialEq)]
pub struct Due {
    pub id: ItemId,
    pub at: DateTime<Utc>,
    pub condition: Option<Condition>,
    /// The watcher to check; none for another trigger or a pending delivery.
    pub watch: Option<Watch>,
    pub observation: Option<Value>,
    pub workdir: Option<PathBuf>,
}

/// A watcher's check reserved before it runs, as a [`ConditionCheck`] is. Its result applies only to this revision
/// ([`complete_watch`]).
#[derive(Debug, Clone, PartialEq)]
pub struct WatchCheck {
    pub id: ItemId,
    pub revision: u64,
    pub at: DateTime<Utc>,
    pub watch: Watch,
    /// The digest of the latest successful check; none before the first.
    pub digest: Option<String>,
    /// The context to check in ([`Reminder::source`]).
    pub source: Option<CallContext>,
    pub workdir: Option<PathBuf>,
    /// The machine whose `workdir` it is.
    pub workdir_machine: Option<String>,
}

/// What a watcher's check found ([`complete_watch`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checked {
    /// It ran: the digest of its result, and whether it reported the watcher done.
    Seen { digest: String, done: bool },
    /// It could not run or failed, for this reason.
    Failed(String),
}

/// A condition probe reserved before running external commands. Results apply only to this revision.
#[derive(Debug, Clone, PartialEq)]
pub struct ConditionCheck {
    pub id: ItemId,
    pub revision: u64,
    pub at: DateTime<Utc>,
    pub condition: Condition,
    pub observation: Option<Value>,
    /// The context to check in ([`Reminder::source`]).
    pub source: Option<CallContext>,
    pub workdir: Option<PathBuf>,
    /// The machine whose `workdir` it is.
    pub workdir_machine: Option<String>,
}

/// A durable wake request. The daemon must use `id` as its idempotency key on every delivery attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Firing {
    pub id: String,
    pub reminder: ItemId,
    pub text: String,
    pub thread: Option<ThreadId>,
    pub reason: String,
    pub at: DateTime<Utc>,
    /// The firing it replaces: the same watcher's last, or the firing a refire follows, whose wake is withdrawn while
    /// it still waits in the queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces: Option<String>,
    /// Whether it is a watcher's last, its check having reported it done: its delivery retires the watcher.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub last: bool,
}

impl Firing {
    /// Whether it is a watcher's.
    pub fn watched(&self) -> bool {
        self.last || self.reason.starts_with(WATCH_REASON)
    }
}

/// How the reason of a watcher's firing starts.
pub const WATCH_REASON: &str = "the watcher";

pub fn schema() -> Value {
    tool::schema()
}

pub fn remind<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { tool::handle(sense, call).await.unwrap_or_else(|answer| answer) })
}

/// Active reminders, including pending and exhausted-but-unacknowledged one-shot reminders, soonest first.
pub async fn active(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<(Item, Reminder)>, StoreError> {
    let mut reminders = readable(items.list(KIND, usize::MAX).await?, Reminder::of);
    reminders.retain(|(_, reminder)| {
        (reminder.status == Status::Active && !reminder.expired(now)) || reminder.status == Status::Exhausted
    });
    reminders.sort_by_key(|(item, reminder)| (reminder.next_due.unwrap_or(DateTime::<Utc>::MAX_UTC), item.id.clone()));
    Ok(reminders)
}

/// The brief's lines of the active reminders: those fired, then those upcoming.
pub(crate) async fn brief(items: &dyn Items, now: DateTime<Utc>) -> Result<[Listing; 2], StoreError> {
    let [fired, upcoming] = rows(items, now).await?;
    let lines = |rows: Vec<(String, TriggerRow)>| rows.into_iter().map(|(line, _)| line).collect();
    Ok([Listing { lines: lines(fired), more: LIST }, Listing { lines: lines(upcoming), more: LIST }])
}

/// The active reminders, those fired then those upcoming, each by its line of the brief and its row of the memories
/// view.
pub(crate) async fn rows(items: &dyn Items, now: DateTime<Utc>) -> Result<[Vec<(String, TriggerRow)>; 2], StoreError> {
    let (mut fired, mut upcoming) = (Vec::new(), Vec::new());
    for (item, reminder) in active(items, now).await? {
        let is_fired = reminder.occurrence_firings > 0;
        let row = (line(&item, &reminder), row(&item, &reminder));
        if is_fired {
            fired.push(row);
        } else {
            upcoming.push(row);
        }
    }
    Ok([fired, upcoming])
}

/// The brief's line of the active `reminder` its `item` holds.
fn line(item: &Item, reminder: &Reminder) -> String {
    let is_fired = reminder.occurrence_firings > 0;
    let mut line = format!(
        "- {} (revision {}, {}): {}",
        item.id,
        item.revision,
        if is_fired { "fired" } else { "upcoming" },
        reminder.title
    );
    if is_fired {
        let pending = reminder.pending.as_ref().map(|firing| (&firing.id, firing.at));
        if let Some((id, at)) = pending.or_else(|| reminder.delivered.as_ref().map(|firing| (&firing.id, firing.at))) {
            let _ = write!(line, "; firing {id} at {}", stamp(at));
        }
        line.push_str(if pending.is_some() { "; delivery pending" } else { "; awaiting acknowledgement" });
    }
    if let Some(next) = reminder.next_due {
        let _ = write!(line, "; next {}", stamp(next));
    }
    if !is_fired {
        let _ = write!(line, "; trigger {}", json!(reminder.trigger));
    }
    if reminder.trigger.watch().is_some() {
        if let Some(delivered) = &reminder.delivered {
            let _ = write!(line, "; last fired {}", stamp(delivered.at));
        }
        if let Some(failed) = reminder.failed_checks {
            let why = reminder.check_error.as_deref().unwrap_or("unknown");
            let _ = write!(line, "; {failed} checks failed in a row, the latest because {why}");
        }
    }
    let _ = write!(line, "; thread {}", reminder.thread.as_ref().map_or("", ThreadId::as_str));
    if let Some(promise) = &reminder.promise {
        let _ = write!(line, "; commitment {promise}");
    }
    line
}

/// The memories view's row of the active `reminder` its `item` holds.
fn row(item: &Item, reminder: &Reminder) -> TriggerRow {
    let status = match (reminder.occurrence_firings > 0, reminder.pending.is_some()) {
        (false, _) => "upcoming",
        (true, true) => "delivery_pending",
        (true, false) => "awaiting_acknowledgement",
    };
    let watcher = reminder.trigger.watch().is_some();
    let last_fired = reminder.pending.as_ref().map(|firing| firing.at).or(reminder.delivered.as_ref().map(|d| d.at));
    TriggerRow {
        id: item.id.to_string(),
        revision: Some(item.revision),
        kind: if watcher { "watcher" } else { KIND }.to_owned(),
        title: reminder.title.clone(),
        status: status.to_owned(),
        thread: reminder.thread.clone(),
        trigger: Some(reminder.trigger.summary()),
        next_due: reminder.next_due,
        last_fired,
        commitment: reminder.promise.as_ref().map(ToString::to_string),
        failing_checks: reminder.failed_checks.filter(|_| watcher),
        last_error: reminder.check_error.clone().filter(|_| watcher),
    }
}

/// The ant's active watchers, as `ants doctor` reads them ([`watch::WatcherStatus`]).
pub async fn watcher_status(items: &dyn Items, now: DateTime<Utc>) -> Result<watch::WatcherStatus, StoreError> {
    let watchers = active(items, now)
        .await?
        .into_iter()
        .filter_map(|(item, reminder)| {
            let runs = reminder.trigger.watch()?.run.runs();
            Some(watch::WatcherState {
                id: item.id,
                title: reminder.title,
                runs,
                failed_checks: reminder.failed_checks.unwrap_or_default(),
                error: reminder.check_error,
            })
        })
        .collect();
    Ok(watch::WatcherStatus { checked_at: now, watchers })
}

/// The next scheduler deadline, including condition expiry and a pending delivery left by a previous daemon.
pub async fn next_due(items: &dyn Items, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, StoreError> {
    Ok(schedule(items, now).await?.into_iter().map(|due| due.at).min())
}

/// Triggers ready to deliver or check, sorted by deadline and id.
pub async fn due(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<Due>, StoreError> {
    Ok(schedule(items, now).await?.into_iter().filter(|due| due.at <= now).collect())
}

/// Persisted deliveries still awaiting confirmation, independent of the current clock.
pub async fn pending_work(items: &dyn Items) -> Result<Vec<Firing>, StoreError> {
    let reminders = readable(items.list(KIND, usize::MAX).await?, Reminder::of);
    let mut firings: Vec<_> = reminders.into_iter().filter_map(|(_, reminder)| reminder.pending).collect();
    firings.sort_by_key(|firing| (firing.at, firing.id.clone()));
    Ok(firings)
}

async fn schedule(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<Due>, StoreError> {
    let mut due = Vec::new();
    for (item, reminder) in readable(list_active(items).await?, Reminder::of) {
        let at = if reminder.pending.is_some() {
            Some(now)
        } else {
            match (reminder.next_due, reminder.expires) {
                (Some(at), Some(expires)) => Some(at.min(expires)),
                (at, _) => at,
            }
        };
        let Some(at) = at else { continue };
        due.push(Due {
            id: item.id,
            at,
            condition: reminder.condition().cloned(),
            watch: reminder.watch().cloned(),
            observation: reminder.observation,
            workdir: reminder.workdir,
        });
    }
    due.sort_by_key(|due| (due.at, due.id.clone()));
    Ok(due)
}

/// Persist a due time firing before waking the ant, its `reason` noting when the occurrence was due if it fires
/// later than [`crate::when::LATE`]. Returning a pending firing never allocates another number. A condition's first
/// firing must instead pass through [`condition_checked`] or [`condition_observed`].
pub async fn prepare_firing(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
    reason: &str,
) -> Result<Option<Firing>, StoreError> {
    read_modify_write(|| async move {
        let item = get(items, id).await?;
        let reminder = Reminder::of(&item)?;
        if let Some(firing) = reminder.pending {
            return Ok(Some(firing));
        }
        if reminder.status != Status::Active {
            return Ok(None);
        }
        if reminder.expired(now) {
            expire(items, &item, reminder).await?;
            return Ok(None);
        }
        if reminder.next_due.is_none_or(|at| at > now) || reminder.condition().is_some() || reminder.watch().is_some() {
            return Ok(None);
        }
        // An occurrence's first firing past its time says so, as a schedule's does; its refires come late by design.
        let late = reminder.next_due.filter(|_| reminder.occurrence_firings == 0);
        let reason = match late.and_then(|due| crate::when::late_note(due, now)) {
            Some(note) => format!("{reason} {note}"),
            None => reason.to_owned(),
        };
        prepare(items, &item, reminder, now, &reason).await.map(Some)
    })
    .await
}

/// Confirm an actual successful wake. A retry of the same confirmation is a no-op; an older id cannot confirm a
/// newer firing. Initial delivery is followed by at most three refires, each an hour after successful delivery and
/// replacing the firing before it ([`Firing::replaces`]).
pub async fn confirm_delivered(
    items: &dyn Items,
    id: &ItemId,
    firing_id: &str,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    read_modify_write(|| async move {
        let item = get(items, id).await?;
        let mut reminder = Reminder::of(&item)?;
        let Some(firing) = reminder.pending.take() else { return Ok(()) };
        if firing.id != firing_id {
            return Err(StoreError::Unavailable(format!("firing {firing_id} is not pending for reminder {id}")));
        }
        reminder.delivered = Some(Delivered { id: firing.id, at: now });
        if let Some(watch) = reminder.trigger.watch() {
            reminder.occurrence_firings = 0;
            if firing.last {
                reminder.next_due = None;
                reminder.status = Status::Done;
            } else {
                reminder.next_due = Some(add(now, watch.next_check(0))?);
            }
        } else if reminder.occurrence_firings < 4 {
            reminder.next_due = Some(add(now, Duration::hours(1))?);
        } else {
            reminder.finish_occurrence(now)?;
        }
        change(items, &item, &reminder).await.map(drop)
    })
    .await
}

/// Record a condition check without changing its prior observation.
pub async fn condition_checked(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
    matched: bool,
    reason: &str,
) -> Result<Option<Firing>, StoreError> {
    condition_observed(items, id, now, matched, reason, None).await
}

/// Record a condition check and, when supplied, its next bounded baseline (for file changes, for example).
/// A command condition's observation is only `{"digest": hex}` of its exit status and stdout
/// ([`CommandDigest`]): any other is discarded, so its output is never kept.
pub async fn condition_observed(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
    matched: bool,
    reason: &str,
    observation: Option<Value>,
) -> Result<Option<Firing>, StoreError> {
    let Some(check) = reserve_condition(items, id, now).await? else {
        return Ok(Reminder::of(&get(items, id).await?)?.pending);
    };
    complete_condition(items, &check, now, matched, reason, observation).await
}

/// Fence and reserve one external probe, persisting its next allowed check before any command runs.
pub async fn reserve_condition(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
) -> Result<Option<ConditionCheck>, StoreError> {
    read_modify_write(|| async move {
        let item = get(items, id).await?;
        let mut reminder = Reminder::of(&item)?;
        if reminder.pending.is_some() || reminder.status != Status::Active {
            return Ok(None);
        }
        if reminder.expired(now) {
            expire(items, &item, reminder).await?;
            return Ok(None);
        }
        if reminder.next_due.is_none_or(|at| at > now) {
            return Ok(None);
        }
        let Some(condition) = reminder.condition().cloned() else {
            return Ok(None);
        };
        reminder.next_due = Some(add(now, interval(&condition, reminder.unchanged_checks.unwrap_or_default()))?);
        reminder.check_started = Some(now);
        let reserved = change(items, &item, &reminder).await?;
        Ok(Some(ConditionCheck {
            id: reserved.id,
            revision: reserved.revision,
            at: now,
            condition,
            source: reminder.source(),
            observation: reminder.observation,
            workdir: reminder.workdir,
            workdir_machine: reminder.workdir_machine,
        }))
    })
    .await
}

/// Apply a reserved probe's result. Postponement, acknowledgement or a newer reservation invalidates the probe.
pub async fn complete_condition(
    items: &dyn Items,
    check: &ConditionCheck,
    now: DateTime<Utc>,
    matched: bool,
    reason: &str,
    observation: Option<Value>,
) -> Result<Option<Firing>, StoreError> {
    let item = get(items, &check.id).await?;
    if item.revision != check.revision {
        return Ok(None);
    }
    let mut reminder = Reminder::of(&item)?;
    if reminder.status != Status::Active {
        return Ok(None);
    }
    if reminder.expired(now) {
        return match expire(items, &item, reminder).await {
            Err(StoreError::Conflict { .. }) => Ok(None),
            result => result.map(|_| None),
        };
    }
    reminder.check_started = None;
    let mut unchanged = true;
    let kept = match &check.condition {
        Condition::Command { .. } => observation.filter(condition::digest_only),
        _ => observation,
    };
    if let Some(observation) = kept {
        if observation.to_string().len() > 4096 {
            return Err(StoreError::Unavailable("condition observation exceeds 4096 bytes".into()));
        }
        unchanged = check.observation.as_ref() == Some(&observation);
        reminder.observation = Some(observation);
    }
    if matched {
        return match prepare(items, &item, reminder, now, reason).await {
            Err(StoreError::Conflict { .. }) => Ok(None),
            result => result.map(Some),
        };
    }
    let checks = if unchanged { reminder.unchanged_checks.unwrap_or_default().saturating_add(1) } else { 0 };
    reminder.unchanged_checks = Some(checks);
    reminder.next_due = Some(add(check.at, interval(&check.condition, checks))?);
    match change(items, &item, &reminder).await {
        Err(StoreError::Conflict { .. }) => Ok(None),
        result => result.map(|_| None),
    }
}

/// Fence and reserve a watcher's check, persisting its next allowed check before it runs, as
/// [`reserve_condition`] does for a condition.
pub async fn reserve_watch(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
) -> Result<Option<WatchCheck>, StoreError> {
    read_modify_write(|| async move {
        let item = get(items, id).await?;
        let mut reminder = Reminder::of(&item)?;
        if reminder.expired(now) && reminder.status == Status::Active {
            expire(items, &item, reminder).await?;
            return Ok(None);
        }
        let Some(watch) = reminder.watch().cloned() else { return Ok(None) };
        if reminder.next_due.is_none_or(|at| at > now) {
            return Ok(None);
        }
        reminder.next_due = Some(add(now, watch.next_check(reminder.unchanged_checks.unwrap_or_default()))?);
        reminder.check_started = Some(now);
        let reserved = change(items, &item, &reminder).await?;
        Ok(Some(WatchCheck {
            id: reserved.id,
            revision: reserved.revision,
            at: now,
            watch,
            digest: reminder.digest().map(str::to_owned),
            source: reminder.source(),
            workdir: reminder.workdir,
            workdir_machine: reminder.workdir_machine,
        }))
    })
    .await
}

/// Apply a reserved watcher check's result: a result whose digest differs from the last check's fires, but on the
/// first check unless the watcher asks for it ([`Watch::first`]); one reporting the watcher done fires a last time
/// whatever its digest. A failed check fires nothing. A change since the reservation invalidates the check.
pub async fn complete_watch(
    items: &dyn Items,
    check: &WatchCheck,
    now: DateTime<Utc>,
    checked: Checked,
) -> Result<Option<Firing>, StoreError> {
    let item = get(items, &check.id).await?;
    if item.revision != check.revision {
        return Ok(None);
    }
    let mut reminder = Reminder::of(&item)?;
    if reminder.status != Status::Active {
        return Ok(None);
    }
    if reminder.expired(now) {
        return match expire(items, &item, reminder).await {
            Err(StoreError::Conflict { .. }) => Ok(None),
            result => result.map(|_| None),
        };
    }
    reminder.check_started = None;
    let (fire, changed) = match checked {
        Checked::Seen { digest, done } => {
            reminder.failed_checks = None;
            reminder.check_error = None;
            let changed = check.digest.as_deref() != Some(digest.as_str());
            let fire = done || (changed && (check.digest.is_some() || check.watch.first));
            reminder.observation = Some(json!({"digest": digest}));
            (fire.then_some(done), changed)
        }
        Checked::Failed(why) => {
            reminder.failed_checks = Some(reminder.failed_checks.unwrap_or_default().saturating_add(1));
            let why =
                crate::sanitize::sanitize(&why, crate::sanitize::Lines::Single, crate::sanitize::Limit::Chars(500))
                    .map_or_else(|_| "the check failed".to_owned(), |clean| clean.text);
            reminder.check_error = Some(why);
            (None, false)
        }
    };
    let checks = if changed { 0 } else { reminder.unchanged_checks.unwrap_or_default().saturating_add(1) };
    reminder.unchanged_checks = Some(checks);
    if let Some(done) = fire {
        let reason = if done {
            format!("{WATCH_REASON} reported it is done: this is its last firing")
        } else {
            format!("{WATCH_REASON}'s result changed")
        };
        let replaces = reminder.delivered.as_ref().map(|delivered| delivered.id.clone());
        return match prepare_as(items, &item, reminder, now, &reason, replaces, done).await {
            Err(StoreError::Conflict { .. }) => Ok(None),
            result => result.map(Some),
        };
    }
    reminder.next_due = Some(add(check.at, check.watch.next_check(checks))?);
    match change(items, &item, &reminder).await {
        Err(StoreError::Conflict { .. }) => Ok(None),
        result => result.map(|_| None),
    }
}

fn interval(condition: &Condition, unchanged: u64) -> Duration {
    Duration::minutes(if unchanged >= 10 {
        30
    } else if condition.remote() {
        5
    } else {
        1
    })
}

async fn prepare(
    items: &dyn Items,
    item: &Item,
    reminder: Reminder,
    now: DateTime<Utc>,
    reason: &str,
) -> Result<Firing, StoreError> {
    // A refire replaces the occurrence's firing before it, withdrawn if it still waits in the ant's queue.
    let replaces = reminder.delivered.as_ref().filter(|_| reminder.occurrence_firings > 0).map(|d| d.id.clone());
    prepare_as(items, item, reminder, now, reason, replaces, false).await
}

/// [`prepare`] a firing replacing the firing `replaces`, the last of its watcher with `last`.
async fn prepare_as(
    items: &dyn Items,
    item: &Item,
    mut reminder: Reminder,
    now: DateTime<Utc>,
    reason: &str,
    replaces: Option<String>,
    last: bool,
) -> Result<Firing, StoreError> {
    let number = reminder
        .number
        .checked_add(1)
        .ok_or_else(|| StoreError::Unavailable("reminder firing number exhausted".into()))?;
    let reason = crate::sanitize::sanitize(reason, crate::sanitize::Lines::Single, crate::sanitize::Limit::Chars(2000))
        .map_err(|error| StoreError::Unavailable(format!("firing reason: {error}")))?
        .text;
    let firing = Firing {
        id: format!("{}:{number}", item.id),
        reminder: item.id.clone(),
        text: reminder.title.clone(),
        thread: reminder.thread.clone(),
        reason,
        at: now,
        replaces,
        last,
    };
    reminder.number = number;
    reminder.occurrence_firings += 1;
    reminder.pending = Some(firing.clone());
    reminder.next_due = Some(now);
    reminder.snoozed = false;
    change(items, item, &reminder).await?;
    Ok(firing)
}

/// The creation of the reminder of commitment `promise`, due at `due`, made by the call of `writer` from the
/// thread's working directory `workdir`: renewed through its due time ([`Reminder::renewed_expiry`]).
pub(crate) fn for_commitment(
    promise: &ItemId,
    title: &str,
    due: DateTime<Utc>,
    writer: &CallContext,
    workdir: Option<&Path>,
    now: DateTime<Utc>,
) -> Result<Write, RemindError> {
    let expires = add(now.max(due), Duration::days(CONDITION_DAYS))?;
    let trigger = Trigger::At { at: due };
    let mut reminder = Reminder::new(title.to_owned(), writer.thread.clone(), trigger, due, expires, writer, workdir);
    reminder.promise = Some(promise.clone());
    let id = ItemId::named("promise-reminder", promise.as_str());
    Ok(Write::Create { id, kind: KIND.into(), values: reminder.values() })
}

/// The change dropping reminder `id` with its commitment, if it is a reminder.
pub(crate) async fn dropped(items: &dyn Items, id: &ItemId) -> Result<Option<Write>, StoreError> {
    let Some(item) = items.get(id).await?.filter(|item| item.kind == KIND) else { return Ok(None) };
    let mut reminder = Reminder::of(&item)?;
    reminder.drop_it();
    change_to(&item, &reminder).map(Some)
}

/// Create reminder-containing batches under a shared revision guard, enforcing the ant-wide active-trigger cap.
pub(crate) async fn write_creations(
    items: &dyn Items,
    writes: &[Write],
    by: &str,
    now: DateTime<Utc>,
) -> Result<Vec<Item>, RemindError> {
    let (mut added, mut removed) = (0, 0);
    for write in writes {
        match write {
            Write::Create { kind, .. } if kind == KIND => added += 1,
            Write::Change { id, values, .. } => {
                if let Some(mut item) = items.get(id).await?.filter(|item| item.kind == KIND) {
                    let counted = |reminder: &Reminder| reminder.status == Status::Active && !reminder.expired(now);
                    let before = counted(&Reminder::of(&item)?);
                    for (name, value) in values {
                        match value {
                            Value::Null => item.values.remove(name),
                            value => item.values.insert(name.clone(), value.clone()),
                        };
                    }
                    let after = counted(&Reminder::of(&item)?);
                    added += usize::from(!before && after);
                    removed += usize::from(before && !after);
                }
            }
            _ => {}
        }
    }
    if added == 0 {
        return Ok(items.write(writes, by).await?);
    }
    let guard = &ItemId::named(QUOTA, "active");
    // Only a conflict on the quota is decided again: one on the writes given is the caller's to answer.
    read_modify_write(|| async move {
        let quota = items.get(guard).await?;
        let active = readable(list_active(items).await?, Reminder::of);
        let count = active.iter().filter(|(_, reminder)| !reminder.expired(now)).count();
        if count.saturating_sub(removed) + added > MAX_ACTIVE {
            return Ok(Err(RemindError::TooMany));
        }
        let revision = quota.as_ref().map_or(0, |item| item.revision);
        let mut batch = writes.to_vec();
        batch.push(Write::Put {
            id: guard.clone(),
            kind: QUOTA.into(),
            values: Quota { generation: revision + 1 }.values(),
            expected: Some(revision),
        });
        match items.write(&batch, by).await {
            Ok(mut items) => {
                items.pop();
                Ok(Ok(items))
            }
            Err(StoreError::Conflict { expected, current }) if current.id != *guard => {
                Ok(Err(StoreError::Conflict { expected, current }.into()))
            }
            Err(e) => Err(e),
        }
    })
    .await?
}

/// The kind of the guard of the writes adding active reminders.
const QUOTA: &str = "reminder-quota";

/// The guard serializing the writes adding active reminders: each such write counts a generation more.
#[derive(Serialize, Deserialize)]
struct Quota {
    generation: u64,
}

impl ItemValues for Quota {
    const KINDS: &[&str] = &[QUOTA];
}

/// The changes resuming the reminders paused while the ant was dormant, from `since` to `until`: each active one
/// unexpired at `since` expiring that much later, so a dormancy uses up none of its life.
pub(crate) async fn resumed(
    items: &dyn Items,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
) -> Result<Vec<Write>, StoreError> {
    let paused = until - since;
    if paused <= Duration::zero() {
        return Ok(Vec::new());
    }
    let mut writes = Vec::new();
    for (item, mut reminder) in readable(list_active(items).await?, Reminder::of) {
        let Some(expires) = reminder.expires.filter(|at| *at > since) else { continue };
        reminder.expires = Some(add(expires, paused)?);
        writes.push(change_to(&item, &reminder)?);
    }
    Ok(writes)
}

/// The items of the reminders with status active.
async fn list_active(items: &dyn Items) -> Result<Vec<Item>, StoreError> {
    items.list_by(KIND, "status", Some(&json!(Status::Active)), usize::MAX).await
}

pub(crate) async fn get(items: &dyn Items, id: &ItemId) -> Result<Item, StoreError> {
    items.get(id).await?.filter(|item| item.kind == KIND).ok_or_else(|| StoreError::NotFound(id.clone()))
}

async fn expire(items: &dyn Items, item: &Item, mut reminder: Reminder) -> Result<Item, StoreError> {
    reminder.status = Status::Expired;
    reminder.next_due = None;
    change(items, item, &reminder).await
}

/// The change of `item` into `reminder`, at the item's revision: its values that differ, null for those removed.
fn change_to(item: &Item, reminder: &Reminder) -> Result<Write, StoreError> {
    let values = changed(&Reminder::of(item)?.values(), reminder.values());
    Ok(Write::Change { id: item.id.clone(), values, expected: Some(item.revision) })
}

/// The scheduler's change of `item` into `reminder`.
async fn change(items: &dyn Items, item: &Item, reminder: &Reminder) -> Result<Item, StoreError> {
    Ok(items.write_confirmed(&[change_to(item, reminder)?], SCHEDULER).await?.remove(0))
}

fn add(at: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>, RemindError> {
    at.checked_add_signed(duration).ok_or_else(|| RemindError::Trigger("The reminder's time is out of range.".into()))
}

#[cfg(test)]
mod tests;
