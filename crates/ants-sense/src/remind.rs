//! Durable reminder scheduling. Preparing a firing persists its identity before a wake is attempted; only a
//! confirmed delivery advances its schedule. A replacement daemon therefore retries the same wake identity.
//! Commitment reminders are automatically renewed through their due time: their expiry is 30 days after the later
//! of creation and the commitment's due time, and explicit renewal never shortens that due-anchored lifetime.
//! `source_context` and `workdir_machine` preserve creation provenance; `writer` follows the latest tool mutation.

use std::path::PathBuf;

use ants_proto::ThreadId;
use ants_proto::provider::{BoxFuture, ToolCall};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::items::{Item, ItemId, Values};
use crate::store::{Items, StoreError, Write};
use crate::{Answer, Sense};

mod tool;
mod trigger;

pub use trigger::Trigger;

pub const KIND: &str = "reminder";
pub const MAX_ACTIVE: usize = 50;
pub const CONDITION_DAYS: i64 = 30;
const RETRIES: usize = 8;
const SCHEDULER: &str = "sense:scheduler";

/// One due trigger. A pending delivery has no condition to re-evaluate.
#[derive(Debug, Clone, PartialEq)]
pub struct Due {
    pub id: ItemId,
    pub at: DateTime<Utc>,
    pub condition: Option<Value>,
    pub observation: Option<Value>,
    pub workdir: Option<PathBuf>,
}

/// A condition probe reserved before running external commands. Results apply only to this revision.
#[derive(Debug, Clone, PartialEq)]
pub struct ConditionCheck {
    pub id: ItemId,
    pub revision: u64,
    pub at: DateTime<Utc>,
    pub condition: Value,
    pub observation: Option<Value>,
    pub workdir: Option<PathBuf>,
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
}

pub fn schema() -> Value {
    tool::schema()
}

pub fn remind<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { tool::handle(sense, call).await.unwrap_or_else(|answer| answer) })
}

/// Active reminders, including pending and exhausted-but-unacknowledged one-shot reminders, soonest first.
pub async fn active(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<Item>, StoreError> {
    let mut reminders = items.list(KIND, usize::MAX).await?;
    reminders
        .retain(|item| (item.text("status") == "active" && !expired(item, now)) || item.text("status") == "exhausted");
    reminders.sort_by_key(|item| (date(item, "next_due").unwrap_or(DateTime::<Utc>::MAX_UTC), item.id.clone()));
    Ok(reminders)
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
    let mut firings = Vec::new();
    for item in items.list(KIND, usize::MAX).await? {
        if let Some(firing) = pending(&item)? {
            firings.push(firing);
        }
    }
    firings.sort_by_key(|firing| (firing.at, firing.id.clone()));
    Ok(firings)
}

async fn schedule(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<Due>, StoreError> {
    let mut due = Vec::new();
    for item in items.list_by(KIND, "status", Some(&json!("active")), usize::MAX).await? {
        let pending = pending(&item)?;
        let at = if pending.is_some() {
            Some(now)
        } else {
            match (date(&item, "next_due"), date(&item, "expires")) {
                (Some(at), Some(expires)) => Some(at.min(expires)),
                (at, _) => at,
            }
        };
        let Some(at) = at else { continue };
        let condition = if pending.is_none() && number(&item, "occurrence_firings") == 0 && !snoozed(&item) {
            trigger(&item)?.condition().cloned()
        } else {
            None
        };
        due.push(Due {
            id: item.id,
            at,
            condition,
            observation: item.values.get("observation").cloned(),
            workdir: item.values.get("workdir").and_then(Value::as_str).map(PathBuf::from),
        });
    }
    due.sort_by_key(|due| (due.at, due.id.clone()));
    Ok(due)
}

/// Persist a due time firing before waking the ant. Returning a pending firing never allocates another number.
/// A condition's first firing must instead pass through [`condition_checked`] or [`condition_observed`].
pub async fn prepare_firing(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
    reason: &str,
) -> Result<Option<Firing>, StoreError> {
    for _ in 0..RETRIES {
        let item = get(items, id).await?;
        if let Some(firing) = pending(&item)? {
            return Ok(Some(firing));
        }
        if item.text("status") != "active" {
            return Ok(None);
        }
        if expired(&item, now) {
            match expire(items, &item).await {
                Err(StoreError::Conflict { .. }) => continue,
                result => return result.map(|_| None),
            }
        }
        if date(&item, "next_due").is_none_or(|at| at > now) {
            return Ok(None);
        }
        if trigger(&item)?.condition().is_some() && number(&item, "occurrence_firings") == 0 && !snoozed(&item) {
            return Ok(None);
        }
        match prepare(items, &item, now, reason, Values::new()).await {
            Err(StoreError::Conflict { .. }) => continue,
            result => return result.map(Some),
        }
    }
    Err(busy())
}

/// Confirm an actual successful wake. A retry of the same confirmation is a no-op; an older id cannot confirm a
/// newer firing. Initial delivery is followed by at most three refires, each an hour after successful delivery.
pub async fn confirm_delivered(
    items: &dyn Items,
    id: &ItemId,
    firing_id: &str,
    now: DateTime<Utc>,
) -> Result<(), StoreError> {
    for _ in 0..RETRIES {
        let item = get(items, id).await?;
        let Some(firing) = pending(&item)? else { return Ok(()) };
        if firing.id != firing_id {
            return Err(StoreError::Unavailable(format!("firing {firing_id} is not pending for reminder {id}")));
        }
        let mut values = values(json!({"pending":null,"delivered":{"id":firing_id,"at":now}}));
        if number(&item, "occurrence_firings") < 4 {
            values.insert("next_due".into(), json!(add(now, Duration::hours(1))?));
        } else {
            finish_occurrence(&item, now, &mut values)?;
        }
        match change(items, &item, values, SCHEDULER, true).await {
            Err(StoreError::Conflict { .. }) => continue,
            result => return result.map(drop),
        }
    }
    Err(busy())
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
/// Never pass command output: command observations are deliberately discarded.
pub async fn condition_observed(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
    matched: bool,
    reason: &str,
    observation: Option<Value>,
) -> Result<Option<Firing>, StoreError> {
    let Some(check) = reserve_condition(items, id, now).await? else {
        return pending(&get(items, id).await?);
    };
    complete_condition(items, &check, now, matched, reason, observation).await
}

/// Fence and reserve one external probe, persisting its next allowed check before any command runs.
pub async fn reserve_condition(
    items: &dyn Items,
    id: &ItemId,
    now: DateTime<Utc>,
) -> Result<Option<ConditionCheck>, StoreError> {
    for _ in 0..RETRIES {
        let item = get(items, id).await?;
        if pending(&item)?.is_some() || item.text("status") != "active" {
            return Ok(None);
        }
        if expired(&item, now) {
            match expire(items, &item).await {
                Err(StoreError::Conflict { .. }) => continue,
                result => return result.map(|_| None),
            }
        }
        if date(&item, "next_due").is_none_or(|at| at > now) {
            return Ok(None);
        }
        if number(&item, "occurrence_firings") != 0 || snoozed(&item) {
            return Ok(None);
        }
        let trigger = trigger(&item)?;
        let Some(condition) = trigger.condition() else {
            return Ok(None);
        };
        let next = add(now, interval(condition, number(&item, "unchanged_checks")))?;
        match change(items, &item, values(json!({"next_due":next,"check_started":now})), SCHEDULER, true).await {
            Err(StoreError::Conflict { .. }) => continue,
            Ok(reserved) => {
                return Ok(Some(ConditionCheck {
                    id: reserved.id,
                    revision: reserved.revision,
                    at: now,
                    condition: condition.clone(),
                    observation: item.values.get("observation").cloned(),
                    workdir: item.values.get("workdir").and_then(Value::as_str).map(PathBuf::from),
                }));
            }
            Err(error) => return Err(error),
        }
    }
    Err(busy())
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
    if item.revision != check.revision || item.text("status") != "active" {
        return Ok(None);
    }
    if expired(&item, now) {
        return match expire(items, &item).await {
            Err(StoreError::Conflict { .. }) => Ok(None),
            result => result.map(|_| None),
        };
    }
    let mut changed = values(json!({"check_started":null}));
    let mut unchanged = true;
    if let Some(observation) = observation
        && check.condition["kind"] != "command"
    {
        if observation.to_string().len() > 4096 {
            return Err(StoreError::Unavailable("condition observation exceeds 4096 bytes".into()));
        }
        unchanged = check.observation.as_ref() == Some(&observation);
        changed.insert("observation".into(), observation);
    }
    if matched {
        return match prepare(items, &item, now, reason, changed).await {
            Err(StoreError::Conflict { .. }) => Ok(None),
            result => result.map(Some),
        };
    }
    let checks = if unchanged { number(&item, "unchanged_checks").saturating_add(1) } else { 0 };
    changed.insert("unchanged_checks".into(), checks.into());
    changed.insert("next_due".into(), json!(add(check.at, interval(&check.condition, checks))?));
    match change(items, &item, changed, SCHEDULER, true).await {
        Err(StoreError::Conflict { .. }) => Ok(None),
        result => result.map(|_| None),
    }
}

fn interval(condition: &Value, unchanged: u64) -> Duration {
    Duration::minutes(if unchanged >= 10 {
        30
    } else if matches!(condition["kind"].as_str(), Some("gh_pr" | "gh_workflow" | "bd")) {
        5
    } else {
        1
    })
}

async fn prepare(
    items: &dyn Items,
    item: &Item,
    now: DateTime<Utc>,
    reason: &str,
    mut changed: Values,
) -> Result<Firing, StoreError> {
    let number = number(item, "number")
        .checked_add(1)
        .ok_or_else(|| StoreError::Unavailable("reminder firing number exhausted".into()))?;
    let reason = crate::sanitize::sanitize(reason, crate::sanitize::Lines::Single, crate::sanitize::Limit::Chars(2000))
        .map_err(|error| StoreError::Unavailable(format!("firing reason: {error}")))?
        .text;
    let firing = Firing {
        id: format!("{}:{number}", item.id),
        reminder: item.id.clone(),
        text: item.text("title").to_owned(),
        thread: item.text("thread").parse().ok(),
        reason,
        at: now,
    };
    changed.insert("number".into(), number.into());
    changed.insert("occurrence_firings".into(), (self::number(item, "occurrence_firings") + 1).into());
    changed.insert("pending".into(), json!(firing));
    changed.insert("next_due".into(), json!(now));
    changed.insert("snoozed".into(), Value::Null);
    change(items, item, changed, SCHEDULER, true).await?;
    Ok(firing)
}

pub(crate) fn promise_creation(id: &ItemId, promise: &Values, now: DateTime<Utc>) -> Result<Write, StoreError> {
    let reminder = ItemId::named("promise-reminder", id.as_str());
    let due = promise
        .get("due")
        .and_then(Value::as_str)
        .and_then(|due| due.parse::<DateTime<Utc>>().ok())
        .ok_or_else(|| StoreError::Unavailable("commitment reminder requires a due time".into()))?;
    Ok(Write::Create {
        id: reminder,
        kind: KIND.into(),
        values: values(json!({
            "title": promise.get("title"), "thread": promise.get("source_thread"), "promise": id,
            "writer":promise.get(crate::memory::WRITER),"source_context":promise.get(crate::memory::WRITER),
            "trigger":{"kind":"at","at":promise.get("due")}, "next_due":promise.get("due"),"first_due":promise.get("due"),
            "status":"active","number":0,"occurrence_firings":0,
            "expires":add(now.max(due),Duration::days(CONDITION_DAYS))?
        })),
    })
}

/// Create reminder-containing batches under a shared revision guard, enforcing the ant-wide active-trigger cap.
pub(crate) async fn write_creations(
    items: &dyn Items,
    writes: &[Write],
    by: &str,
    now: DateTime<Utc>,
) -> Result<Vec<Item>, StoreError> {
    let (mut added, mut removed) = (0, 0);
    for write in writes {
        match write {
            Write::Create { kind, .. } if kind == KIND => added += 1,
            Write::Change { id, values, .. } => {
                if let Some(item) = items.get(id).await?.filter(|item| item.kind == KIND) {
                    let before = item.text("status") == "active" && !expired(&item, now);
                    let status = values.get("status").and_then(Value::as_str).unwrap_or_else(|| item.text("status"));
                    let expires = values
                        .get("expires")
                        .and_then(Value::as_str)
                        .and_then(|at| at.parse::<DateTime<Utc>>().ok())
                        .or_else(|| date(&item, "expires"));
                    let after = status == "active" && expires.is_none_or(|at| at > now);
                    added += usize::from(!before && after);
                    removed += usize::from(before && !after);
                }
            }
            _ => {}
        }
    }
    if added == 0 {
        return items.write(writes, by).await;
    }
    let guard = ItemId::named("reminder-quota", "active");
    for _ in 0..RETRIES {
        let quota = items.get(&guard).await?;
        let count = items
            .list_by(KIND, "status", Some(&json!("active")), usize::MAX)
            .await?
            .iter()
            .filter(|item| !expired(item, now))
            .count();
        if count.saturating_sub(removed) + added > MAX_ACTIVE {
            return Err(StoreError::Unavailable(format!("at most {MAX_ACTIVE} active reminders are permitted")));
        }
        let revision = quota.as_ref().map_or(0, |item| item.revision);
        let mut batch = writes.to_vec();
        batch.push(Write::Put {
            id: guard.clone(),
            kind: "reminder-quota".into(),
            values: values(json!({"generation":revision + 1})),
            expected: Some(revision),
        });
        match items.write(&batch, by).await {
            Err(StoreError::Conflict { current, .. }) if current.id == guard => continue,
            Ok(mut items) => {
                items.pop();
                return Ok(items);
            }
            result => return result,
        }
    }
    Err(busy())
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
    for item in items.list_by(KIND, "status", Some(&json!("active")), usize::MAX).await? {
        let Some(expires) = date(&item, "expires").filter(|at| *at > since) else { continue };
        let values = values(json!({"expires": add(expires, paused)?}));
        writes.push(Write::Change { id: item.id, values, expected: Some(item.revision) });
    }
    Ok(writes)
}

pub(crate) async fn get(items: &dyn Items, id: &ItemId) -> Result<Item, StoreError> {
    items.get(id).await?.filter(|item| item.kind == KIND).ok_or_else(|| StoreError::NotFound(id.clone()))
}

pub(crate) fn values(value: Value) -> Values {
    value.as_object().expect("object values").clone()
}

pub(crate) fn date(item: &Item, name: &str) -> Option<DateTime<Utc>> {
    item.text(name).parse().ok()
}

pub(crate) fn renewed_expiry(item: &Item, now: DateTime<Utc>) -> Result<DateTime<Utc>, StoreError> {
    let due = item.values.get("promise").and_then(|_| date(item, "first_due")).unwrap_or(now);
    add(now.max(due), Duration::days(CONDITION_DAYS))
}

fn number(item: &Item, name: &str) -> u64 {
    item.values.get(name).and_then(Value::as_u64).unwrap_or_default()
}

fn snoozed(item: &Item) -> bool {
    item.values.get("snoozed").and_then(Value::as_bool).unwrap_or_default()
}

fn pending(item: &Item) -> Result<Option<Firing>, StoreError> {
    item.values
        .get("pending")
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|error| StoreError::Unavailable(format!("invalid pending reminder firing: {error}")))
        })
        .transpose()
}

fn trigger(item: &Item) -> Result<Trigger, StoreError> {
    serde_json::from_value(item.values.get("trigger").cloned().unwrap_or_default())
        .map_err(|error| StoreError::Unavailable(format!("invalid reminder trigger: {error}")))
}

fn expired(item: &Item, now: DateTime<Utc>) -> bool {
    !item.values.contains_key("pending") && date(item, "expires").is_some_and(|at| at <= now)
}

async fn expire(items: &dyn Items, item: &Item) -> Result<Item, StoreError> {
    change(items, item, values(json!({"status":"expired","next_due":null})), SCHEDULER, true).await
}

pub(crate) fn finish_occurrence(item: &Item, now: DateTime<Utc>, values: &mut Values) -> Result<(), StoreError> {
    match trigger(item)?.next(now)? {
        Some(next) => {
            values.insert("next_due".into(), json!(next));
            values.insert("occurrence_firings".into(), 0.into());
        }
        None => {
            values.insert("next_due".into(), Value::Null);
            values.insert("status".into(), "exhausted".into());
        }
    }
    Ok(())
}

pub(crate) async fn change(
    items: &dyn Items,
    item: &Item,
    values: Values,
    by: &str,
    confirmed: bool,
) -> Result<Item, StoreError> {
    let writes = [Write::Change { id: item.id.clone(), values, expected: Some(item.revision) }];
    let mut changed =
        if confirmed { items.write_confirmed(&writes, by).await? } else { items.write(&writes, by).await? };
    Ok(changed.remove(0))
}

fn add(at: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>, StoreError> {
    at.checked_add_signed(duration).ok_or_else(|| StoreError::Unavailable("reminder time is out of range".into()))
}

fn busy() -> StoreError {
    StoreError::Unavailable("reminder changed repeatedly; retry the operation".into())
}

#[cfg(test)]
mod tests;
