//! Thread-held leases whose fencing token is the claim event's confirmed stream sequence.

use ants_proto::ThreadId;
use chrono::{DateTime, Duration, Utc};

use super::*;

const LEASE_MINUTES: i64 = 5;
const RECLAIM_GRACE_MINUTES: i64 = 10;

/// A claim's durable metadata. The token is read from the history event at `claim_revision`, not invented locally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub thread: ThreadId,
    pub expires_at: DateTime<Utc>,
    pub claim_revision: u64,
}

/// The real stream sequence of a task's current claim, or none when unclaimed. Pending/offline sequences never fence.
pub async fn fencing_token(items: &dyn Items, task: &TaskItem) -> Result<Option<u64>, StoreError> {
    let Some(lease) = &task.task.lease else { return Ok(None) };
    let token = items.history(task.id()).await?.into_iter().find_map(|revision| {
        if revision.change.revision != lease.claim_revision
            || revision.change.id != *task.id()
            || revision.change.kind != TASK
            || revision.change.by != lease.thread.as_str()
            || revision.sequence == 0
        {
            return None;
        }
        let claimed: Lease = serde_json::from_value(revision.change.after.get("lease")?.clone()).ok()?;
        (claimed.thread == lease.thread && claimed.claim_revision == revision.change.revision)
            .then_some(revision.sequence)
    });
    token
        .map(Some)
        .ok_or_else(|| StoreError::Unavailable(format!("Task {} has no confirmed claim event sequence.", task.id())))
}

pub(super) async fn shown(items: &dyn Items, task: TaskItem) -> Result<Answer, Answer> {
    let token = fencing_token(items, &task).await.map_err(refused)?;
    let mut answer = super::shown(task);
    if let Some(token) = token {
        answer.json["token"] = json!(token);
        let _ = write!(answer.text, "; claim token {token}");
    }
    Ok(answer)
}

pub(super) async fn claim(
    items: &dyn Items,
    graph: &Graph,
    current: &TaskItem,
    call: &ToolCall,
) -> Result<Answer, Answer> {
    let now = call.context.at;
    if let Some(lease) = &current.task.lease {
        if lease.thread == call.context.thread && now < lease.expires_at {
            return shown(items, current.clone()).await;
        }
        if lease.thread != call.context.thread
            && now.signed_duration_since(lease.expires_at) <= Duration::minutes(RECLAIM_GRACE_MINUTES)
        {
            return Err(Answer::error(
                "lease_held",
                format!(
                    "Task {} is held by thread {} until {}, with a further 10-minute reclaim grace.",
                    current.id(),
                    lease.thread,
                    lease.expires_at.to_rfc3339()
                ),
            ));
        }
    } else if current.task.status != Status::Open {
        return Err(Answer::error("not_ready", format!("Task {} is not open.", current.id())));
    }
    if !graph.unblocked(&current.task) {
        return Err(Answer::error("not_ready", format!("Task {} has open blocking prerequisites.", current.id())));
    }
    let mut next = current.task.clone();
    next.status = Status::InProgress;
    next.lease = Some(Lease {
        thread: call.context.thread.clone(),
        expires_at: expiry(now)?,
        claim_revision: current
            .item
            .revision
            .checked_add(1)
            .ok_or_else(|| Answer::invalid("Task revision overflow."))?,
    });
    let writes = graph.writes(Write::Change {
        id: current.id().clone(),
        values: next.values(),
        expected: Some(current.item.revision),
    });
    let item = items.write_confirmed(&writes, call.context.thread.as_str()).await.map_err(refused)?.remove(0);
    shown(items, TaskItem::of(item).expect("claimed task")).await
}

pub(super) async fn authorize(
    items: &dyn Items,
    current: &TaskItem,
    token: Option<u64>,
    call: &ToolCall,
) -> Result<(), Answer> {
    let token = token.ok_or_else(|| Answer::error("token_required", "The claim token is required.".into()))?;
    let invalid = || {
        Answer::error("invalid_token", format!("No matching claim token for task {} and this thread.", current.id()))
    };
    let lease = current.task.lease.as_ref().ok_or_else(invalid)?;
    if lease.thread != call.context.thread || fencing_token(items, current).await.map_err(refused)? != Some(token) {
        return Err(invalid());
    }
    if call.context.at >= lease.expires_at {
        return Err(Answer::error(
            "lease_expired",
            format!("Task {} has an expired lease; claim again.", current.id()),
        ));
    }
    Ok(())
}

pub(super) async fn change(
    items: &dyn Items,
    current: &TaskItem,
    request: &Request,
    call: &ToolCall,
) -> Result<Answer, Answer> {
    authorize(items, current, request.token, call).await?;
    let mut next = current.task.clone();
    match request.action {
        Action::Heartbeat => {
            let lease = next.lease.as_mut().expect("authorized lease");
            lease.expires_at = lease.expires_at.max(expiry(call.context.at)?);
        }
        Action::Release => {
            next.status = Status::Open;
            next.lease = None;
        }
        Action::Close => {
            next.status = Status::Closed;
            next.lease = None;
        }
        _ => unreachable!("lease mutation"),
    }
    let writes =
        [Write::Change { id: current.id().clone(), values: next.values(), expected: Some(current.item.revision) }];
    let item = items.write_confirmed(&writes, call.context.thread.as_str()).await.map_err(refused)?.remove(0);
    shown(items, TaskItem::of(item).expect("lease change")).await
}

fn expiry(now: DateTime<Utc>) -> Result<DateTime<Utc>, Answer> {
    now.checked_add_signed(Duration::minutes(LEASE_MINUTES)).ok_or_else(|| Answer::invalid("Lease time overflow."))
}
