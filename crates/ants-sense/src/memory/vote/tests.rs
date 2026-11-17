use std::sync::Arc;

use ants_proto::provider::{CallContext, Format, Output, Starter};
use serde_json::json;

use super::*;
use crate::memory::tests::{Kept, context, human, lesson, memory, ok, sense, tool};
use crate::store::Items;

fn in_thread(thread: &str) -> CallContext {
    CallContext { thread: thread.parse().unwrap(), ..context(human()) }
}

const OTHER: &str = "019a7d2c-0000-7000-8000-000000000002";

async fn cast(sense: &Sense, arguments: serde_json::Value, context: CallContext) -> Answer {
    let answer = tool(sense, vote, arguments, context).await;
    assert!(!answer.is_error, "{answer:?}");
    answer
}

async fn ballot(store: &Arc<Kept>, id: &ItemId, thread: &str) -> crate::items::Item {
    let items: Arc<dyn Items> = store.clone();
    items.get(&ItemId::named(VOTE, &format!("{id}/{thread}"))).await.unwrap().unwrap()
}

#[tokio::test]
async fn a_stale_vote_marks_a_memory_to_review_and_a_helpful_one_clears_it() {
    let (sense, store) = sense().await;
    let id = ok(&sense, lesson("Port", "Deploy uses 8080")).await;
    let thread = context(human()).thread.to_string();

    let answer = cast(&sense, json!({"id": id, "vote": "stale", "reason": "port\tmoved"}), context(human())).await;
    let expected =
        json!({"id": id, "vote": "stale", "replaced": null, "status": "needs_review", "votes": {"stale": 1}});
    for (name, value) in expected.as_object().unwrap() {
        assert_eq!(&answer.json[name], value, "{name}");
    }
    let Output::Text(text) = answer.render(Format::Text).output else { panic!("text") };
    assert!(text.starts_with(&format!("Voted stale on {id} \"Port\": 0 helpful, 1 stale, 0 wrong.")), "{text}");
    assert!(text.contains("Marked needs_review"), "{text}");
    let stored = memory(&store, &id).await;
    assert_eq!((stored.status, stored.votes.stale), (Status::NeedsReview, 1));
    let voted = ballot(&store, &id, &thread).await;
    assert_eq!((voted.kind.as_str(), voted.text("vote"), voted.text("reason")), (VOTE, "stale", "port moved"));
    assert_eq!(voted.values[WRITER], writer(&context(human())));

    let answer = cast(&sense, json!({"id": id, "vote": "helpful"}), context(human())).await;
    assert_eq!((&answer.json["replaced"], &answer.json["status"]), (&json!("stale"), &json!("active")));
    let stored = memory(&store, &id).await;
    assert_eq!((stored.status, stored.votes.helpful, stored.votes.stale), (Status::Active, 1, 0));
    assert_eq!(stored.votes.confirmed, Some(context(human()).at));
    let voted = ballot(&store, &id, &thread).await;
    assert_eq!((voted.revision, voted.text("vote"), voted.values.get("reason")), (2, "helpful", None));
}

#[tokio::test]
async fn each_thread_counts_once() {
    let (sense, store) = sense().await;
    let id = ok(&sense, lesson("Port", "Deploy uses 8080")).await;
    cast(&sense, json!({"id": id, "vote": "helpful"}), context(human())).await;
    let again = cast(&sense, json!({"id": id, "vote": "helpful"}), context(human())).await;
    assert_eq!(again.json["votes"]["helpful"], 1);
    assert!(again.text.contains("counts once"), "{}", again.text);
    let other = cast(&sense, json!({"id": id, "vote": "helpful"}), in_thread(OTHER)).await;
    assert_eq!(other.json["votes"]["helpful"], 2);
    assert_eq!(memory(&store, &id).await.votes.helpful, 2);
}

#[tokio::test]
async fn a_wrong_vote_retires_the_memory() {
    let (sense, store) = sense().await;
    let id = ok(&sense, lesson("Port", "Deploy uses 8080")).await;
    let answer = cast(&sense, json!({"id": id, "vote": "wrong"}), context(human())).await;
    assert_eq!((&answer.json["status"], &answer.json["votes"]), (&json!("wrong"), &json!({"wrong": 1})));
    assert!(answer.text.contains("Retired as wrong"), "{}", answer.text);
    assert_eq!(memory(&store, &id).await.status, Status::Wrong);
    let again = tool(&sense, vote, json!({"id": id, "vote": "helpful"}), in_thread(OTHER)).await;
    assert_eq!(again.json["error"], "conflict");
    assert!(again.text.contains("is wrong"), "{}", again.text);
}

#[tokio::test]
async fn votes_are_refused_on_unknown_memories_bad_arguments_and_webhook_threads() {
    let (sense, store) = sense().await;
    let id = ok(&sense, lesson("Port", "Deploy uses 8080")).await;
    let hook = context(Starter::Webhook { user: "hook".parse().unwrap() });
    let cases = [
        (json!({"id": ItemId::new(), "vote": "helpful"}), context(human()), "not_found"),
        (json!({"vote": "helpful"}), context(human()), "invalid"),
        (json!({"id": "nope", "vote": "helpful"}), context(human()), "invalid"),
        (json!({"id": id, "vote": "meh"}), context(human()), "invalid"),
        (json!({"id": id}), context(human()), "invalid"),
        (json!({"id": id, "vote": "wrong", "reason": "r".repeat(501)}), context(human()), "too_long"),
        (json!({"id": id, "vote": "wrong"}), hook, "untrusted"),
    ];
    for (arguments, context, code) in cases {
        let answer = tool(&sense, vote, arguments.clone(), context).await;
        assert!(answer.is_error, "{arguments}");
        assert_eq!(answer.json["error"], code, "{arguments}");
    }
    let stored = memory(&store, &id).await;
    assert_eq!((stored.status, stored.votes), (Status::Active, Votes::default()));
}
