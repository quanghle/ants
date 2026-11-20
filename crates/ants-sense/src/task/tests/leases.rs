use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ants_proto::provider::{Output, Provider};
use chrono::{DateTime, Duration, Utc};

use super::*;
use crate::log::{AppendError, LogError, Stored};

async fn at(sense: &Sense, thread: &str, elapsed: Duration, arguments: Value) -> Answer {
    let mut call = call(thread, arguments);
    call.context.at += elapsed;
    answer(sense, &call).await
}

fn token(answer: &Answer) -> u64 {
    assert!(!answer.is_error, "{answer:?}");
    answer.json["token"].as_u64().expect("claim token")
}

#[tokio::test]
async fn task_claim_uses_the_claim_event_sequence_and_heartbeat_keeps_that_fence() {
    let (sense, items) = setup().await;
    let id = create(&sense, "ship", 1).await;
    items.create("memory", Values::new(), "other").await.unwrap();
    let claim = invoke(&sense, json!({"action": "claim", "id": id, "revision": 1})).await;
    let fence = token(&claim);
    assert_eq!(claim.json["task"]["revision"], 2);
    assert_ne!(fence, 2, "stream sequence is not item revision");
    let history = items.history(&id).await.unwrap();
    assert_eq!(fence, history[1].sequence);
    assert_eq!(claim.json["task"]["lease"]["claim_revision"], 2);
    assert_eq!(claim.json["task"]["lease"]["thread"], "t1");
    assert_eq!(claim.json["task"]["lease"]["expires_at"], "2025-01-01T12:05:00Z");
    assert!(ready(&*items, 100).await.unwrap().is_empty());
    assert_eq!(claimed(&*items, &"t1".parse().unwrap(), 10).await.unwrap().len(), 1);
    assert!(claimed(&*items, &"t2".parse().unwrap(), 10).await.unwrap().is_empty());
    let heartbeat =
        at(&sense, "t1", Duration::minutes(4), json!({"action": "heartbeat", "id": id, "token": fence, "revision": 2}))
            .await;
    assert_eq!(token(&heartbeat), fence);
    assert_eq!(heartbeat.json["task"]["lease"]["expires_at"], "2025-01-01T12:09:00Z");
    assert_eq!(heartbeat.json["task"]["revision"], 3);
    let repeated = at(&sense, "t1", Duration::minutes(4), json!({"action": "claim", "id": id})).await;
    assert_eq!(token(&repeated), fence);
    let stepped_back = invoke(&sense, json!({"action": "heartbeat", "id": id, "token": fence})).await;
    assert_eq!(token(&stepped_back), fence);
    assert_eq!(stepped_back.json["task"]["lease"]["expires_at"], "2025-01-01T12:09:00Z");
    let shown = invoke(&sense, json!({"action": "show", "id": id})).await;
    assert_eq!(token(&shown), fence);
    assert_eq!(items.history(&id).await.unwrap().len(), 3, "idempotent claim and show append nothing");
    let lease = list(&*items, 10).await.unwrap().remove(0);
    assert_eq!(fencing_token(&*items, &lease).await.unwrap(), Some(fence));
}

#[tokio::test]
async fn task_invalid_or_missing_tokens_and_status_bypasses_never_write() {
    let (sense, items) = setup().await;
    let id = create(&sense, "ship", 1).await;
    let claim = invoke(&sense, json!({"action": "claim", "id": id})).await;
    let fence = token(&claim);
    for action in ["heartbeat", "release", "close"] {
        for (thread, given) in [("t1", Some(fence + 1)), ("t2", Some(fence)), ("t1", None)] {
            let mut args = json!({"action": action, "id": id});
            if let Some(given) = given {
                args["token"] = json!(given);
            }
            let refused = at(&sense, thread, Duration::minutes(1), args).await;
            assert!(refused.is_error, "{refused:?}");
            assert_eq!(items.history(&id).await.unwrap().len(), 2);
        }
        let stale = invoke(&sense, json!({"action": action, "id": id, "token": fence, "revision": 1})).await;
        assert_eq!(stale.json["error"], "conflict");
    }
    for args in [
        json!({"action": "update", "id": id, "revision": 2, "title": "stolen"}),
        json!({"action": "update", "id": id, "revision": 2, "token": fence + 1, "title": "stolen"}),
        json!({"action": "update", "id": id, "revision": 2, "token": fence, "status": "open"}),
        json!({"action": "update", "id": id, "revision": 2, "token": fence, "status": "closed"}),
        json!({"action": "create", "title": "closed", "status": "closed"}),
        json!({"action": "create", "title": "unleased", "status": "in_progress"}),
        json!({"action": "claim", "id": id, "thread": "t2"}),
    ] {
        let refused = invoke(&sense, args).await;
        assert!(refused.is_error, "{refused:?}");
        assert_eq!(items.history(&id).await.unwrap().len(), 2);
    }
    let updated =
        invoke(&sense, json!({"action": "update", "id": id, "revision": 2, "token": fence, "title": "ship safely"}))
            .await;
    assert_eq!(token(&updated), fence);
    let release = invoke(&sense, json!({"action": "release", "id": id, "token": fence, "revision": 3})).await;
    assert!(!release.is_error, "{release:?}");
    assert_eq!(release.json["task"]["status"], "open");
    assert_eq!(release.json["task"]["lease"], Value::Null);
    assert_eq!(ready(&*items, 10).await.unwrap().len(), 1);
    let claim = at(&sense, "t2", Duration::minutes(1), json!({"action": "claim", "id": id})).await;
    let next_fence = token(&claim);
    assert_ne!(next_fence, fence);
    let stale = invoke(&sense, json!({"action": "close", "id": id, "token": fence})).await;
    assert_eq!(stale.json["error"], "invalid_token");
    assert_eq!(items.history(&id).await.unwrap().len(), 5);
    let close = at(&sense, "t2", Duration::minutes(2), json!({"action": "close", "id": id, "token": next_fence})).await;
    assert!(!close.is_error, "{close:?}");
    assert_eq!(close.json["task"]["status"], "closed");
    assert_eq!(close.json["task"]["lease"], Value::Null);
    let repeated =
        at(&sense, "t2", Duration::minutes(2), json!({"action": "close", "id": id, "token": next_fence})).await;
    assert_eq!(repeated.json["error"], "invalid_token");
    assert_eq!(items.history(&id).await.unwrap().len(), 6);
}

#[tokio::test]
async fn task_cross_thread_reclaim_requires_strictly_more_than_ten_minutes_after_expiry() {
    let (sense, items) = setup().await;
    let id = create(&sense, "ship", 1).await;
    let claim = invoke(&sense, json!({"action": "claim", "id": id})).await;
    let first = token(&claim);
    for elapsed in [Duration::minutes(4), Duration::minutes(5), Duration::minutes(15)] {
        let denied = at(&sense, "t2", elapsed, json!({"action": "claim", "id": id})).await;
        assert_eq!(denied.json["error"], "lease_held");
    }
    let expired =
        at(&sense, "t1", Duration::minutes(5), json!({"action": "heartbeat", "id": id, "token": first})).await;
    assert_eq!(expired.json["error"], "lease_expired");
    assert_eq!(items.history(&id).await.unwrap().len(), 2);
    let stale_snapshot = Graph::load(&*items).await.unwrap();
    let claim =
        at(&sense, "t2", Duration::minutes(15) + Duration::nanoseconds(1), json!({"action": "claim", "id": id})).await;
    let second = token(&claim);
    assert_ne!(second, first);
    assert_eq!(claim.json["task"]["lease"]["claim_revision"], 3);
    let mut before_expiry = call("t1", json!({"action": "heartbeat", "id": id, "token": first}));
    before_expiry.context.at += Duration::minutes(4);
    let stale_request = Request::parse(&before_expiry).unwrap();
    let lost = lease::change(&*items, &stale_snapshot.0[&id], &stale_request, &before_expiry).await.unwrap_err();
    assert_eq!(lost.json["error"], "conflict", "a prevalidated old lease cannot overwrite a reclaim");
    for action in ["heartbeat", "release", "close"] {
        let stale = at(&sense, "t1", Duration::minutes(16), json!({"action": action, "id": id, "token": first})).await;
        assert_eq!(stale.json["error"], "invalid_token");
    }
    assert_eq!(items.history(&id).await.unwrap().len(), 3);
    let current = list(&*items, 10).await.unwrap().remove(0);
    assert_eq!(fencing_token(&*items, &current).await.unwrap(), Some(items.history(&id).await.unwrap()[2].sequence));
    let own = at(&sense, "t2", Duration::minutes(21), json!({"action": "claim", "id": id})).await;
    assert_ne!(token(&own), second, "the owner may reacquire an expired lease without the cross-thread grace");
    let stale_owner =
        at(&sense, "t2", Duration::minutes(21), json!({"action": "heartbeat", "id": id, "token": second})).await;
    assert_eq!(stale_owner.json["error"], "invalid_token");
    assert_eq!(items.history(&id).await.unwrap().len(), 4);
}

#[tokio::test]
async fn task_only_ready_tasks_can_be_claimed_and_cas_guards_racing_claims_and_blocker_reopens() {
    let (sense, items) = setup().await;
    let blocker = create(&sense, "blocker", 2).await;
    let id = create(&sense, "ship", 1).await;
    let updated = invoke(
        &sense,
        json!({"action": "update", "id": id, "revision": 1, "dependencies": [{"kind": "blocks", "id": blocker}]}),
    )
    .await;
    assert!(!updated.is_error, "{updated:?}");
    let denied = invoke(&sense, json!({"action": "claim", "id": id})).await;
    assert_eq!(denied.json["error"], "not_ready");
    for kind in ["memory", "promise", "reminder", "checkpoint", "handoff", "card"] {
        let fake = items.create(kind, items.get(&id).await.unwrap().unwrap().values, "t1").await.unwrap();
        let denied = invoke(&sense, json!({"action": "claim", "id": fake.id})).await;
        assert_eq!(denied.json["error"], "not_found");
        assert_eq!(items.history(&fake.id).await.unwrap().len(), 1);
    }
    for status in ["blocked", "deferred"] {
        let created = invoke(&sense, json!({"action": "create", "title": status, "status": status})).await;
        let denied = invoke(&sense, json!({"action": "claim", "id": created.json["task"]["id"]})).await;
        assert_eq!(denied.json["error"], "not_ready");
    }
    let claim = invoke(&sense, json!({"action": "claim", "id": blocker})).await;
    let closed = invoke(&sense, json!({"action": "close", "id": blocker, "token": token(&claim)})).await;
    assert!(!closed.is_error, "{closed:?}");
    let denied = invoke(&sense, json!({"action": "claim", "id": blocker})).await;
    assert_eq!(denied.json["error"], "not_ready");
    let graph = Graph::load(&*items).await.unwrap();
    let reopened = invoke(&sense, json!({"action": "update", "id": blocker, "revision": 3, "status": "open"})).await;
    assert!(!reopened.is_error, "{reopened:?}");
    let stale_claim = lease::claim(&*items, &graph, &graph.0[&id], &call("t1", json!({}))).await.unwrap_err();
    assert_eq!(stale_claim.json["error"], "conflict");
    assert_eq!(items.history(&id).await.unwrap().len(), 2, "a blocker reopen fences the stale readiness snapshot");
    let first = Graph::load(&*items).await.unwrap();
    let second = Graph::load(&*items).await.unwrap();
    let won = lease::claim(&*items, &first, &first.0[&blocker], &call("t1", json!({}))).await.unwrap();
    assert!(token(&won) > 0);
    let lost = lease::claim(&*items, &second, &second.0[&blocker], &call("t2", json!({}))).await.unwrap_err();
    assert_eq!(lost.json["error"], "conflict");
    assert_eq!(items.history(&blocker).await.unwrap().len(), 5);
}

/// A log out of reach while `online` is not set, counting its reads.
#[derive(Default)]
struct SwitchLog {
    log: MemoryLog,
    online: Arc<AtomicBool>,
    drop_after_append: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
}

impl EventLog for SwitchLog {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, last: u64) -> Result<u64, AppendError> {
        if !self.online.load(Ordering::SeqCst) {
            return Err(LogError::unreachable("hub unavailable").into());
        }
        let result = self.log.append(sense, payload, last).await;
        if self.drop_after_append.swap(false, Ordering::SeqCst) {
            self.online.store(false, Ordering::SeqCst);
        }
        result
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if !self.online.load(Ordering::SeqCst) {
            return Err(LogError::unreachable("hub unavailable"));
        }
        self.log.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        if !self.online.load(Ordering::SeqCst) {
            return Err(LogError::unreachable("hub unavailable"));
        }
        self.log.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        self.log.incarnation()
    }
}

#[tokio::test]
async fn task_offline_claims_never_queue_and_acknowledged_claims_survive_post_append_disconnect() {
    let online = Arc::new(AtomicBool::new(true));
    let drop_after_append = Arc::new(AtomicBool::new(false));
    let (sense, items) = setup_log(SwitchLog {
        online: online.clone(),
        drop_after_append: drop_after_append.clone(),
        ..SwitchLog::default()
    })
    .await;
    let id = create(&sense, "ship", 1).await;
    online.store(false, Ordering::SeqCst);
    let denied = invoke(&sense, json!({"action": "claim", "id": id})).await;
    assert_eq!(denied.json["error"], "unavailable");
    assert_eq!(items.pending().await.unwrap(), 0);
    assert_eq!(items.history(&id).await.unwrap().len(), 1);
    online.store(true, Ordering::SeqCst);
    drop_after_append.store(true, Ordering::SeqCst);
    let claim = invoke(&sense, json!({"action": "claim", "id": id})).await;
    let fence = token(&claim);
    assert!(!online.load(Ordering::SeqCst));
    assert_eq!(items.history(&id).await.unwrap()[1].sequence, fence);
    assert_eq!(items.pending().await.unwrap(), 0);
    for action in ["heartbeat", "release", "close"] {
        let denied = at(&sense, "t1", Duration::minutes(1), json!({"action": action, "id": id, "token": fence})).await;
        assert_eq!(denied.json["error"], "unavailable");
        assert_eq!(items.pending().await.unwrap(), 0);
        assert_eq!(items.history(&id).await.unwrap().len(), 2);
    }
    let current = items.get(&id).await.unwrap().unwrap();
    let mut provisional = Task::held(&current).unwrap();
    provisional.lease = Some(Lease {
        thread: "t2".parse().unwrap(),
        expires_at: "2025-01-01T12:25:00Z".parse::<DateTime<Utc>>().unwrap(),
        claim_revision: current.revision + 1,
    });
    let pending = items.change(&id, provisional.values(), Some(current.revision), "t2").await.unwrap();
    assert_eq!(items.history(&id).await.unwrap().last().unwrap().sequence, 0);
    assert!(fencing_token(&*items, &TaskItem::of(pending).unwrap()).await.is_err(), "pending seq0 cannot fence");
}

#[tokio::test]
async fn a_task_call_syncs_the_log_once_and_is_refused_offline() {
    let (online, reads) = (Arc::new(AtomicBool::new(true)), Arc::new(AtomicUsize::new(0)));
    let (sense, items) =
        setup_log(SwitchLog { online: online.clone(), reads: reads.clone(), ..SwitchLog::default() }).await;
    for arguments in [json!({"action": "create", "title": "ship"}), json!({"action": "ready"})] {
        let before = reads.load(Ordering::SeqCst);
        let result = sense.call_tool(call("t1", arguments)).await.unwrap();
        assert!(!result.is_error, "{result:?}");
        assert_eq!(reads.load(Ordering::SeqCst) - before, 1, "one sync a call");
    }
    online.store(false, Ordering::SeqCst);
    for arguments in [json!({"action": "create", "title": "offline"}), json!({"action": "ready"})] {
        let result = sense.call_tool(call("t1", arguments)).await.unwrap();
        let Output::Json(json) = result.output else { panic!("a JSON answer") };
        assert!(result.is_error && json["error"] == "unavailable", "{json}");
    }
    assert_eq!(items.pending().await.unwrap(), 0, "nothing queued offline");
}
