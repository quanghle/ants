use std::collections::HashMap;
use std::sync::Arc;

use ants_proto::provider::{BoxFuture, CallContext, Event, EventKind, Format, Origin, Provider, Starter, ToolCall};
use ants_proto::{SenseId, UserId};
use chrono::Utc;
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;
use crate::Sense;
use crate::cache::Cache;
use crate::items::{ItemId, Values};
use crate::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
use crate::memory::{Memory, VOTE};
use crate::records::RELATIONSHIP;
use crate::store::{Store, Stores};
use crate::thread::{CHECKPOINT, HANDOFF};
use crate::{promise, remind, task, thread};

const SOURCE: &str = "dev-a";
const CLONE: &str = "dev-b";

/// The items of the ants sense keeps, each in a log of its own.
struct Kept(HashMap<UserId, Arc<dyn Items>>);

impl Stores for Kept {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        let items = self.0.get(ant).cloned().ok_or_else(|| StoreError::NotKept(ant.to_string()));
        Box::pin(std::future::ready(items))
    }
}

async fn items(ant: &str) -> Arc<dyn Items> {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let log = AntLog::new(
        MemoryLog::default(),
        Unsigned,
        verifier,
        SenseId::new(),
        ant.parse().unwrap(),
        "m1".parse().unwrap(),
        None,
    );
    Arc::new(tokio::sync::Mutex::new(Store::open(log.unwrap(), Cache::in_memory().unwrap()).await.unwrap()))
}

/// sense keeping the source and the clone, as the daemon serving both does.
async fn sense() -> (Sense, Arc<dyn Items>, Arc<dyn Items>) {
    let (source, clone) = (items(SOURCE).await, items(CLONE).await);
    let kept = HashMap::from([(SOURCE.parse().unwrap(), source.clone()), (CLONE.parse().unwrap(), clone.clone())]);
    (Sense::with_stores(Arc::new(Kept(kept))), source, clone)
}

fn context(ant: &str) -> CallContext {
    CallContext {
        ant: ant.parse().unwrap(),
        identity: "0f1e".into(),
        thread: "019a7d2c-0000-7000-8000-000000000001".parse().unwrap(),
        origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
        harness: "copilot".into(),
        model: None,
        machine: "studio".into(),
        repo: None,
        at: Utc::now(),
    }
}

/// The answer of the source's tool `name` to `arguments`, which must not be refused.
async fn call(sense: &Sense, name: &str, arguments: Value) -> Value {
    call_as(sense, SOURCE, name, arguments).await
}

/// The answer of `ant`'s tool `name` to `arguments`, which must not be refused.
async fn call_as(sense: &Sense, ant: &str, name: &str, arguments: Value) -> Value {
    let context = context(ant);
    let arguments = arguments.as_object().unwrap().clone();
    let result = sense.call_tool(ToolCall { name: name.into(), arguments, context, format: Format::Json }).await;
    let result = result.unwrap();
    assert!(!result.is_error, "{result:?}");
    match result.output {
        ants_proto::provider::Output::Json(json) => json,
        output => panic!("{output:?}"),
    }
}

fn cloned(from: &str, at: chrono::DateTime<Utc>) -> Event {
    let kind = EventKind::AntCloned { from: from.parse().unwrap(), from_identity: "0f1e".into() };
    Event { id: Uuid::now_v7(), ant: CLONE.parse().unwrap(), identity: "1a2b".into(), thread: None, at, kind }
}

async fn count(items: &dyn Items, kind: &str) -> usize {
    items.list(kind, usize::MAX).await.unwrap().len()
}

fn id(answer: &Value) -> ItemId {
    answer["id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn a_clone_keeps_memories_and_the_self_model_marked_inherited_and_drops_the_rest() {
    let (sense, source, clone) = sense().await;
    let old = id(&call(&sense, "remember", json!({"kind": "fact", "title": "Old", "body": "Lexer is slow."})).await);
    let new = json!({"kind": "fact", "title": "New", "body": "Lexer is fast now.", "supersedes": old});
    let new = id(&call(&sense, "remember", new).await);
    call(&sense, "vote", json!({"id": new, "vote": "helpful"})).await;
    call(&sense, "remember", json!({"kind": "self_model", "title": "Me", "body": "Careful."})).await;
    let bob = json!({"kind": "relationship", "scope": "human:bob", "title": "Bob", "body": "Reviews fast."});
    call(&sense, "remember", bob).await;
    call(&sense, "checkpoint", json!({"goal": "Ship"})).await;
    call(&sense, "handoff", json!({"done": ["Lexer"]})).await;
    call(&sense, "task", json!({"action": "create", "title": "Parser"})).await;
    call(&sense, "promise", json!({"title": "Notes", "promisee": "alice", "due": "2099-01-01T00:00:00Z"})).await;
    call(&sense, "remind", json!({"title": "Check", "trigger": {"kind": "after", "seconds": 3600}})).await;
    let context = context(SOURCE);
    let kind = EventKind::ThreadStarted { origin: context.origin, repo: None };
    let (id, ant, at) = (Uuid::now_v7(), SOURCE.parse().unwrap(), Utc::now());
    sense.event(Event { id, ant, identity: "0f1e".into(), thread: Some(context.thread), at, kind }).await.unwrap();

    sense.event(cloned(SOURCE, Utc::now())).await.unwrap();

    let from = source.sense().await;
    let memories = clone.list(MEMORY, usize::MAX).await.unwrap();
    assert_eq!(memories.len(), 3, "the two facts and the handoff's episode");
    for item in &memories {
        let mut values = source.get(&item.id).await.unwrap().unwrap().values;
        values.insert(INHERITED.into(), json!(from));
        assert_eq!(item.values, values, "copied as it is, marked inherited");
        assert_eq!(Memory::of(item).unwrap().inherited.as_ref(), Some(&from));
    }
    let recalled = call_as(&sense, CLONE, "recall", json!({"id": new})).await;
    assert_eq!(recalled["memory"][INHERITED], json!(from));
    let new = Memory::of(&clone.get(&new).await.unwrap().unwrap()).unwrap();
    assert_eq!((new.supersedes, new.votes.helpful), (Some(old), 1), "links and counts hold");
    let model = crate::records::self_model(&*clone).await.unwrap().unwrap();
    assert_eq!((model.record.body.as_str(), model.record.inherited.as_ref()), ("Careful.", Some(&from)));
    for kind in [RELATIONSHIP, CHECKPOINT, HANDOFF, task::TASK, promise::KIND, remind::KIND, VOTE, thread::record::KIND]
    {
        assert!(count(&*source, kind).await > 0, "{kind}");
        assert_eq!(count(&*clone, kind).await, 0, "{kind}");
    }
    let card = crate::identity::card(&*clone).await.unwrap().unwrap();
    assert_eq!(card.cloned_from.unwrap().ant.as_str(), SOURCE);
    assert_ne!(card.sense, from);
}

#[tokio::test]
async fn the_clone_takes_the_source_as_it_was_when_cloned() {
    let (sense, source, clone) = sense().await;
    let before = id(&call(&sense, "remember", json!({"kind": "fact", "title": "Before", "body": "Kept."})).await);
    call(&sense, "remember", json!({"kind": "self_model", "title": "Me", "body": "Then."})).await;
    let at = Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    call(&sense, "remember", json!({"kind": "fact", "title": "After", "body": "Not kept."})).await;
    call(&sense, "remember", json!({"kind": "self_model", "title": "Me", "body": "Now.", "revision": 1})).await;

    sense.event(cloned(SOURCE, at)).await.unwrap();

    let memories = clone.list(MEMORY, usize::MAX).await.unwrap();
    assert_eq!(memories.iter().map(|item| &item.id).collect::<Vec<_>>(), [&before]);
    let model = crate::records::self_model(&*clone).await.unwrap().unwrap();
    assert_eq!(model.record.body, "Then.");
    assert_eq!(crate::records::self_model(&*source).await.unwrap().unwrap().record.body, "Now.");
}

#[tokio::test]
async fn a_fork_tried_again_copies_only_what_the_clone_lacks() {
    let (sense, source, clone) = sense().await;
    call(&sense, "remember", json!({"kind": "fact", "title": "One", "body": "First fact."})).await;
    call(&sense, "remember", json!({"kind": "self_model", "title": "Theirs", "body": "The source's."})).await;
    let own = json!({"kind": "self_model", "title": "Mine", "body": "The clone's own."});
    call_as(&sense, CLONE, "remember", own).await;

    assert_eq!(fork(&*source, &*clone, Utc::now()).await.unwrap(), 1, "the clone's own self-model stays");
    assert_eq!(fork(&*source, &*clone, Utc::now()).await.unwrap(), 0);
    let model = crate::records::self_model(&*clone).await.unwrap().unwrap();
    assert_eq!((model.record.title.as_str(), model.record.inherited), ("Mine", None));

    let event = cloned(SOURCE, Utc::now());
    sense.event(event.clone()).await.unwrap();
    call(&sense, "remember", json!({"kind": "fact", "title": "Two", "body": "Second fact."})).await;
    sense.event(Event { at: Utc::now(), ..event }).await.unwrap();
    assert_eq!(count(&*clone, MEMORY).await, 1, "an event applied is not forked again");
}

#[tokio::test]
async fn a_clone_of_an_ant_sense_does_not_keep_gets_its_card_alone() {
    let (sense, _, clone) = sense().await;
    sense.event(cloned("dev-x", Utc::now())).await.unwrap();
    let card = crate::identity::card(&*clone).await.unwrap().unwrap();
    assert_eq!(card.cloned_from.unwrap().ant.as_str(), "dev-x");
    assert_eq!(count(&*clone, MEMORY).await, 0);
}

#[tokio::test]
async fn a_record_the_clone_writes_anew_is_no_longer_inherited() {
    let (sense, source, clone) = sense().await;
    call(&sense, "remember", json!({"kind": "self_model", "title": "Me", "body": "Careful."})).await;
    fork(&*source, &*clone, Utc::now()).await.unwrap();
    let bolder = json!({"kind": "self_model", "title": "Me", "body": "Bolder.", "revision": 1});
    call_as(&sense, CLONE, "remember", bolder).await;
    let model = crate::records::self_model(&*clone).await.unwrap().unwrap();
    assert_eq!((model.item.revision, model.record.inherited), (2, None));
}

#[tokio::test]
async fn a_clone_keeps_the_persona_and_the_skills_but_not_their_proposals() {
    let (sense, source, clone) = sense().await;
    let mut persona = Values::new();
    persona.insert("voice".into(), json!("Plain."));
    let mut skill = Values::new();
    skill.insert("name".into(), json!("release"));
    skill.insert("description".into(), json!("Cut a release."));
    skill.insert("files".into(), json!({"SKILL.md": "---\nname: release\ndescription: Cut a release.\n---\n"}));
    let writes = [
        Write::Create { id: crate::persona::persona_id(), kind: PERSONA.into(), values: persona },
        Write::Create { id: crate::skill::skill_id("release"), kind: SKILL.into(), values: skill },
    ];
    source.write(&writes, "test").await.unwrap();
    call(&sense, "persona", json!({"voice": "Loud.", "reason": "Why not."})).await;

    sense.event(cloned(SOURCE, Utc::now())).await.unwrap();

    let from = source.sense().await;
    let persona = crate::persona::current(&*clone).await.unwrap().unwrap();
    assert_eq!((persona.text("voice"), &persona.values[INHERITED]), ("Plain.", &json!(from)));
    let skills = crate::skill::skills(&*clone).await.unwrap();
    assert_eq!(skills.iter().map(|skill| skill.name.as_str()).collect::<Vec<_>>(), ["release"]);
    assert_eq!(count(&*source, crate::proposal::PROPOSAL).await, 1);
    assert_eq!(count(&*clone, crate::proposal::PROPOSAL).await, 0, "proposals are the source's");
}
