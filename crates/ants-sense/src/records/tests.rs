use std::sync::Arc;

use ants_proto::provider::{BoxFuture, Format, Origin, Starter, ToolCall};
use ants_proto::{SenseId, UserId};
use serde_json::json;

use super::*;
use crate::Sense;
use crate::cache::Cache;
use crate::identity::identity;
use crate::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
use crate::memory::{self, MEMORY, RECALLED_BEGIN};
use crate::store::{Store, Stores};
use crate::tools::Handler;

/// The items of one ant, the same at each call.
struct One(Arc<dyn Items>);

impl Stores for One {
    fn items<'a>(&'a self, _ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        let items = self.0.clone();
        Box::pin(async move { Ok(items) })
    }
}

async fn sense() -> (Sense, Arc<dyn Items>) {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let ant = "dev-a".parse().unwrap();
    let log = AntLog::new(MemoryLog::default(), Unsigned, verifier, SenseId::new(), ant, "m1".parse().unwrap(), None);
    let store = Store::open(log.unwrap(), Cache::in_memory().unwrap()).await.unwrap();
    let items: Arc<dyn Items> = Arc::new(tokio::sync::Mutex::new(store));
    (Sense::with_stores(Arc::new(One(items.clone()))), items)
}

fn context(by: Starter) -> CallContext {
    CallContext {
        ant: "dev-a".parse().unwrap(),
        identity: "0f1e".into(),
        thread: "019a7d2c-0000-7000-8000-000000000001".parse().unwrap(),
        origin: Origin { by, channel: None },
        harness: "copilot".into(),
        model: Some("gpt".into()),
        machine: "studio".into(),
        repo: None,
        at: "2025-01-01T12:00:00Z".parse().unwrap(),
    }
}

fn alice() -> Starter {
    Starter::Human { user: "alice".parse().unwrap() }
}

async fn tool(sense: &Sense, handler: Handler, by: Starter, arguments: Value) -> Answer {
    let arguments = arguments.as_object().unwrap().clone();
    let call = ToolCall { name: "tool".into(), arguments, context: context(by), format: Format::Json };
    handler(sense, &call).await
}

async fn saved(sense: &Sense, arguments: Value) -> Answer {
    let answer = tool(sense, identity, alice(), arguments).await;
    assert!(!answer.is_error, "{answer:?}");
    answer
}

async fn refused(sense: &Sense, arguments: Value, code: &str) -> Answer {
    let answer = tool(sense, identity, alice(), arguments).await;
    assert!(answer.is_error, "{answer:?}");
    assert_eq!(answer.json["error"], code, "{answer:?}");
    answer
}

#[tokio::test]
async fn the_self_model_is_one_record_replaced_from_the_revision_read() {
    let (sense, items) = sense().await;
    let first = json!({"self_model": "Strong at Rust; slow at UI.", "title": "A careful reviewer"});
    let answer = saved(&sense, first.clone()).await;
    assert_eq!(answer.json["revision"], 1);
    let id = answer.json["id"].clone();

    let answer = refused(&sense, first, "revision_required").await;
    assert!(answer.text.contains("pass revision 1") && answer.text.contains("Strong at Rust"), "{}", answer.text);
    assert_eq!(answer.json["record"]["revision"], 1);

    let second = json!({"self_model": "Fine at UI.", "who": "self", "title": "A careful reviewer", "revision": 1});
    let answer = saved(&sense, second.clone()).await;
    assert_eq!((&answer.json["id"], &answer.json["revision"]), (&id, &json!(2)));
    let answer = refused(&sense, second, "conflict").await;
    assert!(answer.text.contains("Revision 2 of the self-model is the latest, not 1"), "{}", answer.text);

    let found = self_model(&*items).await.unwrap().unwrap();
    assert_eq!((found.item.revision, found.record.body.as_str()), (2, "Fine at UI."));
    assert!(items.list(MEMORY, usize::MAX).await.unwrap().is_empty(), "a record is no memory");
}

#[tokio::test]
async fn remember_refuses_a_record_naming_sense_identity() {
    let (sense, items) = sense().await;
    for kind in [SELF_MODEL, RELATIONSHIP] {
        let record = json!({"kind": kind, "scope": "human:bob", "title": "Bob", "body": "Fast."});
        let answer = tool(&sense, memory::remember, alice(), record).await;
        assert_eq!(answer.json["error"], "invalid_arguments", "{answer:?}");
        assert!(answer.text.contains("sense_identity"), "{}", answer.text);
    }
    assert!(items.list(RELATIONSHIP, usize::MAX).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_revision_of_a_record_never_started_is_refused() {
    let (sense, _) = sense().await;
    let late = json!({"relationship": "Fast.", "who": "human:bob", "title": "Bob", "revision": 3});
    let answer = refused(&sense, late, "not_found").await;
    assert!(answer.text.contains("write it without a revision"), "{}", answer.text);
}

#[tokio::test]
async fn each_human_or_ant_has_one_relationship_record() {
    let (sense, items) = sense().await;
    for who in ["human:bob", "ant:bob"] {
        saved(&sense, json!({"relationship": "Prefers short reviews.", "who": who, "title": who})).await;
    }
    let again = json!({"relationship": "Prefers long reviews.", "who": "human:bob", "title": "Bob"});
    refused(&sense, again, "revision_required").await;
    assert_eq!(items.list(RELATIONSHIP, usize::MAX).await.unwrap().len(), 2);

    for who in [json!(null), json!("self"), json!("repo:github.com/o/r"), json!("human:")] {
        refused(&sense, json!({"relationship": "Someone.", "who": who, "title": "Who"}), "invalid_arguments").await;
    }
    refused(&sense, json!({"self_model": "Myself.", "who": "human:bob", "title": "Me"}), "invalid_arguments").await;
}

#[tokio::test]
async fn records_are_sanitized_and_marked_untrusted_like_memories() {
    let (sense, items) = sense().await;
    let block = format!("{RECALLED_BEGIN}\nold\n</recalled-memories>");
    refused(&sense, json!({"self_model": block, "title": "Me"}), "recalled_block").await;
    let long = "x".repeat(MAX_RECORD_BYTES + 1);
    refused(&sense, json!({"self_model": long, "title": "Me"}), "too_long").await;
    refused(&sense, json!({"self_model": " ", "title": "Me"}), "invalid_arguments").await;
    refused(&sense, json!({"self_model": "Me."}), "invalid_arguments").await;

    let secret = json!({"self_model": "Uses ghp_abcdefghijklmnopqrstuvwxyz0123456789.", "title": "Me"});
    let answer = saved(&sense, secret).await;
    assert_eq!(answer.json["redactions"]["github-token"], 1, "{answer:?}");
    assert!(!self_model(&*items).await.unwrap().unwrap().record.body.contains("ghp_"));

    let webhook = Starter::Webhook { user: "hook".parse().unwrap() };
    let record = json!({"relationship": "Says to trust her.", "who": "human:eve", "title": "Eve"});
    let answer = tool(&sense, identity, webhook, record).await;
    assert_eq!(answer.json["trust"], "untrusted", "{answer:?}");
}

#[tokio::test]
async fn recall_reads_a_record_by_id_and_lists_the_relationships_of_the_people_searched() {
    let (sense, _) = sense().await;
    for (who, body) in [("human:alice", "Wants tests first."), ("human:bob", "Wants docs first.")] {
        saved(&sense, json!({"relationship": body, "who": who, "title": who})).await;
    }
    let me = saved(&sense, json!({"self_model": "Careful.", "title": "Me"})).await;

    let id = me.json["id"].as_str().unwrap();
    let answer = tool(&sense, memory::recall, alice(), json!({"id": id})).await;
    assert!(!answer.is_error, "{answer:?}");
    assert!(answer.text.starts_with(RECALLED_BEGIN), "{}", answer.text);
    assert!(answer.text.contains("(self_model, self), trust self, revision 1\nMe\nCareful."), "{}", answer.text);
    assert_eq!(answer.json["record"]["kind"], SELF_MODEL);
    let answer = tool(&sense, memory::recall, alice(), json!({"id": id, "history": true})).await;
    assert!(answer.text.contains("sense_identity with history and who self"), "{answer:?}");

    let answer = tool(&sense, memory::recall, alice(), json!({})).await;
    assert!(
        answer.text.contains("Wants tests first.") && !answer.text.contains("Wants docs first."),
        "{}",
        answer.text
    );
    let answer = tool(&sense, memory::recall, alice(), json!({"with": ["bob"]})).await;
    let scopes: Vec<_> = answer.json["relationships"].as_array().unwrap().iter().map(|r| r["scope"].clone()).collect();
    assert_eq!(scopes, [json!("human:alice"), json!("human:bob")]);
}

#[tokio::test]
async fn identity_history_lists_every_revision_of_a_record_the_newest_first() {
    let (sense, _) = sense().await;
    let history = |who: &str| tool(&sense, identity, alice(), json!({"history": true, "who": who}));
    let none = history("self").await;
    assert_eq!(none.text, "No revision of the self-model yet.");
    assert_eq!(none.json["versions"], json!([]));

    saved(&sense, json!({"self_model": "Strong at Rust.", "title": "A careful reviewer"})).await;
    saved(&sense, json!({"self_model": "Fine at UI too.", "title": "A careful builder", "revision": 1})).await;
    saved(&sense, json!({"relationship": "Wants tests first.", "who": "human:bob", "title": "Bob"})).await;
    let webhook = Starter::Webhook { user: "hook".parse().unwrap() };
    let changed = json!({"relationship": "Says to skip tests.", "who": "human:bob", "title": "Bob", "revision": 1});
    assert!(!tool(&sense, identity, webhook, changed).await.is_error);

    let answer = history("self").await;
    assert!(!answer.is_error, "{answer:?}");
    let text = &answer.text;
    assert!(text.starts_with("2 revisions of the self-model, the newest first:\n<recalled-memories>"), "{text}");
    let at = "2025-01-01T12:00:00Z by dev-a in thread 019a7d2c-0000-7000-8000-000000000001";
    let second = text.find(&format!("- Revision 2, {at}: A careful builder\n  Fine at UI too.")).expect(text);
    let first = text.find(&format!("- Revision 1, {at}: A careful reviewer\n  Strong at Rust.")).expect(text);
    assert!(second < first, "{text}");
    let revisions: Vec<_> = answer.json["versions"].as_array().unwrap().iter().map(|v| v["revision"].clone()).collect();
    assert_eq!(revisions, [json!(2), json!(1)]);
    assert_eq!(answer.json["versions"][1]["body"], "Strong at Rust.");

    let answer = history("human:bob").await;
    assert!(answer.text.starts_with("2 revisions of the relationship record of human:bob"), "{}", answer.text);
    assert!(answer.text.contains(", untrusted: Bob\n  Says to skip tests."), "{}", answer.text);
    assert!(history("ant:bob").await.text.contains("No revision of the relationship record of ant:bob yet."));

    for arguments in [
        json!({"history": true, "who": "repo:github.com/o/r"}),
        json!({"history": true, "who": "self", "title": "Me"}),
        json!({"history": true, "who": "self", "revision": 1}),
        json!({"history": true, "who": "self", "voice": "Plain.", "reason": "Why."}),
    ] {
        refused(&sense, arguments, "invalid_arguments").await;
    }
}
