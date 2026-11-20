use ants_proto::provider::CallContext;

use super::*;
use crate::testing::{self, context, human, sense};

fn call(arguments: Value) -> ToolCall {
    let context = CallContext { thread: "t1".parse().unwrap(), model: None, machine: "m1".into(), ..context(human()) };
    testing::call("promise", arguments, context)
}

async fn create(sense: &Sense, title: &str, due: &str) -> ItemId {
    let result = promise(sense, &call(json!({"title":title,"promisee":"alice","due":due}))).await;
    assert!(!result.is_error, "{result:?}");
    result.json["promise"]["id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn open_commitments_are_overdue_first_and_resolution_is_terminal() {
    let (sense, items) = sense().await;
    let future = create(&sense, "future", "2025-01-02T00:00:00Z").await;
    let overdue = create(&sense, "overdue", "2025-01-01T00:00:00Z").await;
    let now = call(json!({})).context.at;
    let listed = open(&*items, now).await.unwrap();
    assert_eq!(listed.iter().map(|(item, _)| &item.id).collect::<Vec<_>>(), [&overdue, &future]);
    assert_eq!(listed[0].1.source_thread.as_str(), "t1");
    for (action, state) in [("keep", "kept"), ("break", "broken"), ("release", "released")] {
        let id = create(&sense, action, "2025-01-03T00:00:00Z").await;
        let result = promise(&sense, &call(json!({"action":action,"id":id}))).await;
        assert_eq!(result.json["promise"]["values"]["status"], state);
        let again = promise(&sense, &call(json!({"action":"keep","id":id}))).await;
        assert_eq!(again.json["error"], "closed");
    }
    assert_eq!(open(&*items, now).await.unwrap().len(), 2);
}

#[tokio::test]
async fn renegotiation_retains_old_terms_and_atomically_links_the_replacement() {
    let (sense, items) = sense().await;
    let id = create(&sense, "first terms", "2025-01-02T00:00:00Z").await;
    let result = promise(
        &sense,
        &call(json!({"action":"renegotiate","id":id,"due":"2025-01-03T00:00:00+00:00","reason":"more time"})),
    )
    .await;
    assert!(!result.is_error, "{result:?}");
    let next: ItemId = result.json["promise"]["id"].as_str().unwrap().parse().unwrap();
    let replacement = items.get(&next).await.unwrap().unwrap();
    assert_eq!(replacement.text("status"), "open");
    assert_eq!(replacement.values["history"][0]["due"], "2025-01-02T00:00:00Z");
    assert_eq!(replacement.values["history"][0]["reason"], "more time");
    let previous = items.get(&id).await.unwrap().unwrap();
    assert_eq!(previous.text("status"), "renegotiated");
    assert_eq!(previous.values["replacement"], json!(next));
    assert_eq!(replacement.values["supersedes"], json!(id));
    assert_eq!(
        items.history(&id).await.unwrap().last().unwrap().sequence,
        items.history(&next).await.unwrap()[0].sequence
    );
}

#[tokio::test]
async fn invalid_terms_and_stale_revisions_change_nothing() {
    let (sense, items) = sense().await;
    for args in [
        json!({"title":"x","promisee":"","due":"2025-01-01T00:00:00Z"}),
        json!({"title":"x","promisee":"alice","due":"tomorrow"}),
        json!({"title":"x","promisee":"alice","due":3}),
    ] {
        assert!(promise(&sense, &call(args)).await.is_error);
    }
    assert!(items.list(KIND, 100).await.unwrap().is_empty());
    let id = create(&sense, "first", "2025-01-02T00:00:00Z").await;
    let current = items.get(&id).await.unwrap().unwrap();
    let result = promise(&sense, &call(json!({"action":"keep","id":id,"revision":2}))).await;
    assert_eq!(result.json["error"], "conflict");
    assert_eq!(result.json["current"], json!(current));
    assert_eq!(result.json["promise"], json!(current));
    assert_eq!(items.get(&id).await.unwrap().unwrap(), current);
}

#[tokio::test]
async fn stale_revisions_return_the_current_commitment_even_after_resolution() {
    let (sense, items) = sense().await;
    let id = create(&sense, "first", "2025-01-02T00:00:00Z").await;
    let kept = promise(&sense, &call(json!({"action":"keep","id":id,"revision":1}))).await;
    assert!(!kept.is_error, "{kept:?}");
    let current = items.get(&id).await.unwrap().unwrap();
    assert_eq!(current.text("status"), "kept");
    for action in ["keep", "release", "renegotiate", "show"] {
        let result = promise(&sense, &call(json!({"action":action,"id":id,"revision":1}))).await;
        assert_eq!(result.json["error"], "conflict", "{result:?}");
        assert_eq!(result.json["current"], json!(current));
        assert_eq!(result.json["promise"], json!(current));
    }
    let closed = promise(&sense, &call(json!({"action":"keep","id":id,"revision":current.revision}))).await;
    assert_eq!(closed.json["error"], "closed");
    assert_eq!(items.get(&id).await.unwrap().unwrap(), current);
}

#[tokio::test]
async fn automatic_reminders_remain_schedulable_until_a_commitment_due_in_forty_days() {
    let (sense, items) = sense().await;
    let now = call(json!({})).context.at;
    let due = now + chrono::Duration::days(40);
    let id = create(&sense, "long commitment", &due.to_rfc3339()).await;
    let commitment = Promise::of(&items.get(&id).await.unwrap().unwrap()).unwrap();
    let reminder = commitment.reminder.unwrap();
    let expiry = due + chrono::Duration::days(30);
    let item = items.get(&reminder).await.unwrap().unwrap();
    assert_eq!(crate::remind::Reminder::of(&item).unwrap().expires, Some(expiry));

    let mut renew = call(json!({"action":"renew","id":reminder}));
    renew.context.at = now + chrono::Duration::days(1);
    let renewed = crate::remind::remind(&sense, &renew).await;
    assert!(!renewed.is_error, "{renewed:?}");
    assert_eq!(renewed.json["reminder"]["values"]["expires"], json!(expiry));
    assert_eq!(crate::remind::next_due(&*items, now + chrono::Duration::days(31)).await.unwrap(), Some(due));
    assert!(
        crate::remind::prepare_firing(&*items, &reminder, due - chrono::Duration::seconds(1), "not due")
            .await
            .unwrap()
            .is_none()
    );
    let firing = crate::remind::prepare_firing(&*items, &reminder, due, "commitment overdue").await.unwrap().unwrap();
    assert_eq!(firing.id, format!("{reminder}:1"));
}
