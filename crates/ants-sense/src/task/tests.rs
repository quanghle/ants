//! Tests of personal tasks: external references, the ready list, relationship validation, expected revisions, and
//! compare-and-set guards on the graph.

use std::sync::Arc;

use ants_proto::SenseId;
use ants_proto::provider::CallContext;

use super::*;
use crate::log::{EventLog, MemoryLog};
use crate::testing::{self, context, fixture, human, sense};

mod leases;

fn call(thread: &str, arguments: Value) -> ToolCall {
    let context =
        CallContext { thread: thread.parse().unwrap(), model: None, machine: "m1".into(), ..context(human()) };
    testing::call("task", arguments, context)
}

/// The answer to `call` as a provider call gets it, the items caught up with the log as it starts.
async fn answer(sense: &Sense, call: &ToolCall) -> Answer {
    sense.changing(&call.context.ant, task(sense, call)).await
}

async fn invoke(sense: &Sense, args: Value) -> Answer {
    answer(sense, &call("t1", args)).await
}

async fn create(sense: &Sense, title: &str, priority: u8) -> ItemId {
    let answer = invoke(sense, json!({"action": "create", "title": title, "priority": priority})).await;
    assert!(!answer.is_error, "{answer:?}");
    serde_json::from_value(answer.json["task"]["id"].clone()).unwrap()
}

#[tokio::test]
async fn task_external_references_cannot_inject_lines_into_briefs() {
    let (sense, _) = sense().await;
    let answer =
        invoke(&sense, json!({"action":"create","title":"Ship parser","external_ref":"bd:team-\u{200b}1\nnext"})).await;
    assert!(!answer.is_error, "{answer:?}");
    assert_eq!(answer.json["task"]["external_ref"], "bd:team-1 next");
    let invalid = invoke(&sense, json!({"action":"create","title":"Empty reference","external_ref":"\u{200b}"})).await;
    assert_eq!(invalid.json["error"], "invalid_arguments");
}

#[tokio::test]
async fn task_ready_is_kind_filtered_open_unblocked_and_sorted_before_limiting() {
    let (sense, items) = sense().await;
    let oldest = create(&sense, "oldest", 2).await;
    let urgent = create(&sense, "urgent", 0).await;
    let newer = create(&sense, "newer", 2).await;
    let blocked = create(&sense, "blocked", 0).await;
    let answer = invoke(
        &sense,
        json!({"action": "update", "id": blocked, "revision": 1, "dependencies": [{"kind": "blocks", "id": oldest}]}),
    )
    .await;
    assert!(!answer.is_error, "{answer:?}");
    for kind in ["memory", "promise", "reminder", "checkpoint", "handoff", "card"] {
        let task = items.get(&urgent).await.unwrap().unwrap();
        items.create(kind, task.values, "t1").await.unwrap();
    }
    let ready_ids: Vec<_> = ready(&*items, 2).await.unwrap().iter().map(|task| task.id().clone()).collect();
    assert_eq!(ready_ids, [urgent.clone(), oldest.clone()]);
    assert_eq!(ready(&*items, 100).await.unwrap().last().unwrap().id(), &newer);
    for status in ["blocked", "deferred"] {
        let answer =
            invoke(&sense, json!({"action": "create", "title": status, "status": status, "priority": 0})).await;
        assert!(!answer.is_error, "{answer:?}");
    }
    let active = create(&sense, "in_progress", 0).await;
    let claim = invoke(&sense, json!({"action": "claim", "id": active})).await;
    assert!(!claim.is_error, "{claim:?}");
    let closed = create(&sense, "closed", 0).await;
    let claim = invoke(&sense, json!({"action": "claim", "id": closed})).await;
    assert!(!claim.is_error, "{claim:?}");
    let close = invoke(&sense, json!({"action": "close", "id": closed, "token": claim.json["token"]})).await;
    assert!(!close.is_error, "{close:?}");
    assert_eq!(ready(&*items, 100).await.unwrap().len(), 3);
    let claim = invoke(&sense, json!({"action": "claim", "id": oldest})).await;
    assert!(!claim.is_error, "{claim:?}");
    let closed = invoke(&sense, json!({"action": "close", "id": oldest, "token": claim.json["token"]})).await;
    assert!(!closed.is_error, "{closed:?}");
    assert!(ready(&*items, 100).await.unwrap().iter().any(|task| task.id() == &blocked));
    assert_eq!(list(&*items, 100).await.unwrap().len(), 8);
}

#[tokio::test]
async fn task_relationships_validate_types_targets_duplicates_parentage_and_cycles_without_writes() {
    let (sense, items) = sense().await;
    let a = create(&sense, "a", 2).await;
    let b = create(&sense, "b", 2).await;
    let c = create(&sense, "c", 2).await;
    let memory = items.create("memory", Values::new(), "t1").await.unwrap();
    for deps in [
        json!([{"kind": "blocks", "id": a}]),
        json!([{"kind": "blocks", "id": ItemId::new()}]),
        json!([{"kind": "blocks", "id": memory.id}]),
        json!([{"kind": "unknown", "id": b}]),
        json!([{"kind": "related", "id": b}, {"kind": "related", "id": b}]),
        json!([{"kind": "parent-child", "id": b}, {"kind": "parent-child", "id": c}]),
    ] {
        let answer = invoke(&sense, json!({"action": "update", "id": a, "revision": 1, "dependencies": deps})).await;
        assert!(answer.is_error, "{answer:?}");
        assert_eq!(items.history(&a).await.unwrap().len(), 1);
    }
    let edge = invoke(
        &sense,
        json!({"action": "update", "id": a, "revision": 1, "dependencies": [{"kind": "blocks", "id": b}]}),
    )
    .await;
    assert!(!edge.is_error, "{edge:?}");
    let edge = invoke(
        &sense,
        json!({"action": "update", "id": b, "revision": 1, "dependencies": [{"kind": "parent-child", "id": c}]}),
    )
    .await;
    assert!(!edge.is_error, "{edge:?}");
    let cycle = invoke(
        &sense,
        json!({"action": "update", "id": c, "revision": 1, "dependencies": [{"kind": "discovered-from", "id": a}]}),
    )
    .await;
    assert_eq!(cycle.json["error"], "invalid_arguments");
    assert!(cycle.text.contains("cycle"));
    assert_eq!(items.history(&c).await.unwrap().len(), 1);
    assert!(ready(&*items, 100).await.unwrap().iter().any(|task| task.id() == &b), "parent does not block");
    let related = invoke(
        &sense,
        json!({"action": "update", "id": c, "revision": 1, "dependencies": [{"kind": "related", "id": a}]}),
    )
    .await;
    assert!(!related.is_error, "{related:?}");
    let duplicate = invoke(
        &sense,
        json!({"action": "update", "id": a, "revision": 2, "dependencies": [{"kind": "related", "id": c}]}),
    )
    .await;
    assert_eq!(duplicate.json["error"], "invalid_arguments");
    assert_eq!(items.history(&a).await.unwrap().len(), 2);
}

#[tokio::test]
async fn task_expected_revisions_preserve_external_references_and_reject_stale_writes() {
    let (sense, items) = sense().await;
    let id = create(&sense, "first", 4).await;
    let missing = invoke(&sense, json!({"action": "update", "id": id, "title": "wrong"})).await;
    assert_eq!(missing.json["error"], "revision_required");
    let edit = invoke(
        &sense,
        json!({"action": "update", "id": id, "revision": 1, "external_ref": "bd:team-1", "body": "more"}),
    )
    .await;
    assert!(!edit.is_error, "{edit:?}");
    let stale = invoke(&sense, json!({"action": "update", "id": id, "revision": 1, "title": "wrong"})).await;
    assert_eq!(stale.json["error"], "conflict");
    assert_eq!(stale.json["task"]["revision"], 2);
    let edit = invoke(&sense, json!({"action": "update", "id": id, "revision": 2, "title": "next"})).await;
    assert_eq!(edit.json["task"]["external_ref"], "bd:team-1");
    let edit = invoke(&sense, json!({"action": "update", "id": id, "revision": 3, "external_ref": null})).await;
    assert_eq!(edit.json["task"]["external_ref"], Value::Null);
    assert_eq!(items.history(&id).await.unwrap().len(), 4);
    let noop = invoke(&sense, json!({"action": "update", "id": id, "revision": 4, "title": "next"})).await;
    assert!(!noop.is_error);
    assert_eq!(items.history(&id).await.unwrap().len(), 4, "no-op is not an event");
    for args in [
        json!({"action": "create", "title": "bad", "priority": -1}),
        json!({"action": "create", "title": "bad", "priority": 5}),
        json!({"action": "create", "title": "bad", "status": "done"}),
        json!({"action": "create", "title": " "}),
        json!({"action": "ready", "limit": 0}),
        json!({"action": "ready", "status": "closed"}),
    ] {
        assert!(invoke(&sense, args).await.is_error);
    }
}

#[tokio::test]
async fn task_graph_cas_guards_prevent_two_valid_snapshots_from_committing_a_cycle() {
    let (sense, items) = sense().await;
    let a = create(&sense, "a", 2).await;
    let b = create(&sense, "b", 2).await;
    let first = Graph::load(&*items).await.unwrap();
    let second = Graph::load(&*items).await.unwrap();
    let mut next_a = first.0[&a].task.clone();
    next_a.dependencies.push(Dependency { kind: DependencyKind::Blocks, id: b.clone() });
    let mut next_b = second.0[&b].task.clone();
    next_b.dependencies.push(Dependency { kind: DependencyKind::Blocks, id: a.clone() });
    first.validate(&a, &next_a).unwrap();
    second.validate(&b, &next_b).unwrap();
    let a_writes = first.writes(Write::Change { id: a, values: next_a.values(), expected: Some(1) });
    let b_writes = second.writes(Write::Change { id: b.clone(), values: next_b.values(), expected: Some(1) });
    items.write(&a_writes, "t1").await.unwrap();
    assert!(matches!(items.write(&b_writes, "t2").await, Err(StoreError::Conflict { .. })));
    assert_eq!(items.history(&b).await.unwrap().len(), 1, "the failed batch is atomic");
}
