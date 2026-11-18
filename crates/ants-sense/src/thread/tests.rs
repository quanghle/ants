use std::sync::Arc;

use ants_proto::provider::{Format, Origin, Starter};
use ants_proto::{SenseId, UserId};

use super::*;
use crate::cache::Cache;
use crate::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
use crate::memory::WRITER;
use crate::store::{Store, Stores};

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

fn call(name: &str, thread: &str, arguments: Value) -> ToolCall {
    ToolCall {
        name: name.to_owned(),
        arguments: arguments.as_object().unwrap().clone(),
        context: CallContext {
            ant: "dev-a".parse().unwrap(),
            identity: "0f1e".into(),
            thread: thread.parse().unwrap(),
            origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
            harness: "copilot".into(),
            model: Some("gpt".into()),
            machine: "studio".into(),
            repo: None,
            at: "2025-01-01T12:00:00Z".parse().unwrap(),
        },
        format: Format::Json,
    }
}

async fn checkpointed(sense: &Sense, thread: &str, arguments: Value) -> Answer {
    checkpoint(sense, &call("checkpoint", thread, arguments)).await
}

async fn handed_over(sense: &Sense, thread: &str, arguments: Value) -> Answer {
    handoff(sense, &call("handoff", thread, arguments)).await
}

fn thread(name: &str) -> ThreadId {
    name.parse().unwrap()
}

fn strings(entries: &[&str]) -> Vec<String> {
    entries.iter().map(|entry| (*entry).to_owned()).collect()
}

#[tokio::test]
async fn a_checkpoint_starts_the_register_then_replaces_it_only_from_its_revision() {
    let (sense, items) = sense().await;
    let first = json!({"goal": "ship", "step": "tests", "blockers": ["ci"], "branch": "main", "questions": ["why"]});
    let answer = checkpointed(&sense, "t1", first).await;
    assert!(!answer.is_error, "{answer:?}");
    assert_eq!(answer.json["register"]["revision"], 1);
    let first = register(&*items, &thread("t1")).await.unwrap().unwrap();
    assert_eq!((first.goal.as_deref(), first.step.as_deref(), first.next), (Some("ship"), Some("tests"), None));
    assert_eq!(
        (first.blockers, first.questions, first.branch.as_deref()),
        (strings(&["ci"]), strings(&["why"]), Some("main"))
    );

    let unread = checkpointed(&sense, "t1", json!({"goal": "other"})).await;
    assert_eq!((unread.is_error, &unread.json["error"]), (true, &json!("revision_required")));
    assert_eq!(unread.json["register"]["goal"], "ship");
    assert!(unread.text.contains("Goal: ship"), "{}", unread.text);
    let stale = checkpointed(&sense, "t1", json!({"goal": "other", "revision": 2})).await;
    assert_eq!((&stale.json["error"], &stale.json["register"]["revision"]), (&json!("conflict"), &json!(1)));
    let missing = checkpointed(&sense, "t2", json!({"goal": "other", "revision": 3})).await;
    assert_eq!(missing.json["error"], "not_found");

    let replaced = checkpointed(&sense, "t1", json!({"goal": "ship it", "next": "merge", "revision": 1})).await;
    assert!(!replaced.is_error, "{replaced:?}");
    let now = register(&*items, &thread("t1")).await.unwrap().unwrap();
    assert_eq!((now.revision, now.goal.as_deref(), now.next.as_deref()), (2, Some("ship it"), Some("merge")));
    assert_eq!((now.step, now.blockers, now.branch), (None, vec![], None), "replaced whole");
    assert_eq!(register(&*items, &thread("t2")).await.unwrap(), None);
}

#[tokio::test]
async fn a_handoff_keeps_the_work_and_an_episode_memory_written_by_the_call_s_context() {
    let (sense, items) = sense().await;
    let arguments = json!({
        "done": ["wrote the parser"],
        "in_flight": ["the lexer"],
        "next": ["tests"],
        "promises": ["tell alice"],
        "decisions": ["no regex"],
        "files": ["src/parse.rs"],
        "thread": "t9",
        "writer": {"ant": "mallory"},
    });
    let mut webhook = call("handoff", "t1", arguments);
    webhook.context.origin.by = Starter::Webhook { user: "hook".parse().unwrap() };
    let answer = handoff(&sense, &webhook).await;
    assert!(!answer.is_error, "{answer:?}");
    let id: ItemId = answer.json["handoff"]["id"].as_str().unwrap().parse().unwrap();
    let item = items.get(&id).await.unwrap().unwrap();
    let handoff = Handoff::of(&item).unwrap();
    assert_eq!((handoff.thread.as_str(), handoff.acknowledged, item.created.by.as_str()), ("t1", None, "t1"));
    assert_eq!((handoff.work.done, handoff.work.files), (strings(&["wrote the parser"]), strings(&["src/parse.rs"])));
    let writer = &item.values[WRITER];
    assert_eq!(
        (&writer["ant"], &writer["machine"], &writer["thread"]),
        (&json!("dev-a"), &json!("studio"), &json!("t1"))
    );
    assert_eq!(writer["origin"]["by"], json!({"kind": "webhook", "user": "hook"}));

    let episode = items.get(&handoff.episode.unwrap()).await.unwrap().unwrap();
    let memory = crate::memory::Memory::of(&episode).unwrap();
    assert_eq!((memory.kind, memory.trust), (MemoryKind::Episode, crate::memory::Trust::Untrusted));
    assert_eq!(memory.title, "Handoff of thread t1: wrote the parser");
    assert!(memory.body.contains("In flight:\n- the lexer\n"), "{}", memory.body);
    assert_eq!((json!(memory.writer), memory.status), (writer.clone(), crate::memory::Status::Active));
    assert_eq!((memory.scope.to_string(), memory.importance, memory.core), ("self".into(), 3, false));
    let (episode, handoff) = (items.history(&episode.id).await.unwrap(), items.history(&item.id).await.unwrap());
    assert_eq!(episode[0].sequence, handoff[0].sequence, "one event writes both");

    let empty = handed_over(&sense, "t1", json!({"done": [" "]})).await;
    assert_eq!(empty.json["error"], "empty");
}

#[tokio::test]
async fn handoffs_show_until_a_checkpoint_on_their_thread_or_naming_them_acknowledges_them() {
    let (sense, items) = sense().await;
    let mut ids = Vec::new();
    for thread in ["t1", "t2", "t3"] {
        let answer = handed_over(&sense, thread, json!({"next": [format!("finish {thread}")]})).await;
        ids.push(answer.json["handoff"]["id"].as_str().unwrap().to_owned());
    }
    let open = async |items: &dyn Items| {
        let open = unacknowledged(items, 10).await.unwrap();
        open.into_iter().map(|handoff| handoff.thread.to_string()).collect::<Vec<_>>()
    };
    assert_eq!(open(&*items).await, ["t3", "t2", "t1"]);

    let on_t1 = checkpointed(&sense, "t1", json!({"goal": "go on"})).await;
    assert_eq!(on_t1.json["acknowledged"], json!([ids[0]]));
    assert_eq!(open(&*items).await, ["t3", "t2"]);
    let item = items.get(&ids[0].parse().unwrap()).await.unwrap().unwrap();
    assert_eq!(Handoff::of(&item).unwrap().acknowledged.unwrap().thread.as_str(), "t1");

    let episode = items.list("memory", 10).await.unwrap()[0].id.to_string();
    for unknown in [ItemId::new().to_string(), episode] {
        let refused = checkpointed(&sense, "t4", json!({"acknowledges": [unknown]})).await;
        assert_eq!(refused.json["error"], "not_found");
    }
    assert_eq!(register(&*items, &thread("t4")).await.unwrap(), None, "nothing written");
    let invalid = checkpointed(&sense, "t4", json!({"acknowledges": ["nope"]})).await;
    assert_eq!(invalid.json["error"], "invalid_arguments");

    let naming = checkpointed(&sense, "t4", json!({"goal": "review", "acknowledges": [ids[1], ids[0]]})).await;
    assert_eq!(naming.json["acknowledged"], json!([ids[1]]), "t1's already acknowledged");
    assert!(naming.text.contains(&format!("Acknowledged handoffs: {}.", ids[1])), "{}", naming.text);
    assert_eq!(open(&*items).await, ["t3"]);
}

#[tokio::test]
async fn a_checkpoint_changing_nothing_shows_the_register_and_every_unacknowledged_handoff() {
    let (sense, items) = sense().await;
    let empty = checkpointed(&sense, "t1", json!({})).await;
    assert!(!empty.is_error, "{empty:?}");
    assert_eq!((&empty.json["register"], &empty.json["handoffs"]), (&Value::Null, &json!([])));
    assert!(empty.text.contains("No unacknowledged handoffs."), "{}", empty.text);
    assert_eq!(register(&*items, &thread("t1")).await.unwrap(), None, "nothing written");

    checkpointed(&sense, "t1", json!({"goal": "ship", "blockers": ["ci"]})).await;
    let mut episodes = Vec::new();
    for thread in ["t1", "t2"] {
        let answer = handed_over(&sense, thread, json!({"next": [format!("finish {thread}")]})).await;
        episodes.push(answer.json["handoff"]["episode"].as_str().unwrap().to_owned());
    }
    for arguments in [json!({}), json!({"revision": 1, "goal": " ", "acknowledges": []})] {
        let read = checkpointed(&sense, "t1", arguments).await;
        assert!(!read.is_error, "{read:?}");
        assert_eq!((&read.json["register"]["goal"], &read.json["register"]["revision"]), (&json!("ship"), &json!(1)));
        assert_eq!(read.json["handoffs"].as_array().unwrap().len(), 2);
        assert!(read.text.starts_with("Resume register, revision 1") && read.text.contains("- ci"), "{}", read.text);
        for episode in &episodes {
            assert!(read.text.contains(&format!("Episode memory: {episode}")), "{}", read.text);
        }
    }
    assert_eq!(unacknowledged(&*items, 10).await.unwrap().len(), 2, "nothing acknowledged");
    assert_eq!(register(&*items, &thread("t1")).await.unwrap().unwrap().revision, 1, "nothing written");
}

#[tokio::test]
async fn texts_pass_the_sanitizer_and_their_limits() {
    let (sense, items) = sense().await;
    let token = format!("ghp_{}", "a1B2c3D4e5".repeat(4).get(..36).unwrap());
    let arguments = json!({
        "goal": format!("use {token}\u{202E} now"),
        "branch": "feat\nx",
        "blockers": ["", "  "],
        "thread": "t9",
    });
    let answer = checkpointed(&sense, "t1", arguments).await;
    assert!(answer.text.contains("Redacted 1 secret."), "{}", answer.text);
    let register = register(&*items, &thread("t1")).await.unwrap().unwrap();
    assert_eq!(register.goal.as_deref(), Some("use [redacted:github-token] now"));
    assert_eq!(
        (register.branch.as_deref(), register.blockers, register.thread.as_str()),
        (Some("feat x"), vec![], "t1")
    );

    let long = checkpointed(&sense, "t2", json!({"goal": "x".repeat(TEXT_MAX + 1)})).await;
    assert_eq!(long.json["error"], "too_long");
    let many = handed_over(&sense, "t2", json!({"done": vec!["x"; ENTRIES_MAX + 1]})).await;
    assert_eq!(many.json["error"], "too_long");
    let typed = handed_over(&sense, "t2", json!({"done": "x"})).await;
    assert_eq!(typed.json["error"], "invalid_arguments");
    let numbered = checkpointed(&sense, "t2", json!({"revision": "1"})).await;
    assert_eq!(numbered.json["error"], "invalid_arguments");

    let handed = handed_over(&sense, "t2", json!({"done": [format!("pushed with {token}")]})).await;
    assert!(handed.text.starts_with("Handoff ") && handed.text.ends_with("Redacted 1 secret."), "{}", handed.text);
    let id: ItemId = handed.json["handoff"]["id"].as_str().unwrap().parse().unwrap();
    let handoff = Handoff::of(&items.get(&id).await.unwrap().unwrap()).unwrap();
    assert_eq!(handoff.work.done, ["pushed with [redacted:github-token]"]);
    let episode = crate::memory::Memory::of(&items.get(&handoff.episode.unwrap()).await.unwrap().unwrap()).unwrap();
    assert!(!episode.title.contains("ghp_") && !episode.body.contains("ghp_"));
}

#[test]
fn long_texts_are_cut_on_character_boundaries() {
    assert_eq!(truncate_chars("ééé", 3), "ééé");
    assert_eq!(truncate_chars("éééé", 3), "éé…");
    let cut = truncate_bytes(&"é".repeat(10), 8);
    assert!(cut.len() <= 8 && cut.ends_with('…'), "{cut}");
}
