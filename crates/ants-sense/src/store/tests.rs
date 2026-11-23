//! Tests of the store: revisions and expected ones, past values, raced writes, items of known ids, listing and search,
//! cache rebuilds, atomic writes, writes whose callers stopped waiting, and how sense reaches an ant's items.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::{SenseId, UserId};
use serde_json::{Value, json};

use super::*;
use crate::log::{AccountVerifier, MemoryLog, NoRevocations, Stored, Unsigned};

mod offline;

/// A log counting the messages read from it.
struct Counted<'a>(&'a MemoryLog, AtomicUsize);

impl<'a> Counted<'a> {
    fn new(log: &'a MemoryLog) -> Self {
        Self(log, AtomicUsize::new(0))
    }

    fn read(&self) -> usize {
        self.1.swap(0, Ordering::Relaxed)
    }
}

impl EventLog for Counted<'_> {
    fn append(
        &self,
        sense: &SenseId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, crate::log::AppendError>> + Send {
        self.0.append(sense, payload, expected_last)
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        let read = self.0.read_after(sense, after, limit).await?;
        self.1.fetch_add(read.len(), Ordering::Relaxed);
        Ok(read)
    }

    fn last_sequence(&self, sense: &SenseId) -> impl Future<Output = Result<u64, LogError>> + Send {
        self.0.last_sequence(sense)
    }

    fn incarnation(&self) -> &str {
        self.0.incarnation()
    }
}

type Verify = AccountVerifier<NoRevocations>;

fn ant_log<L: EventLog>(log: L, sense: &SenseId) -> AntLog<L, Unsigned, Verify> {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    AntLog::new(log, Unsigned, verifier, sense.clone(), dev_a(), "m1".parse().unwrap(), None).unwrap()
}

async fn open<L: EventLog>(log: L, sense: &SenseId, home: Option<&Path>) -> Store<L, Unsigned, Verify> {
    let cache = match home {
        Some(home) => Cache::open(home).unwrap(),
        None => Cache::in_memory().unwrap(),
    };
    Store::open(ant_log(log, sense), cache).await.unwrap()
}

fn dev_a() -> UserId {
    "dev-a".parse().unwrap()
}

fn values(value: Value) -> Values {
    value.as_object().unwrap().clone()
}

fn note(title: &str) -> Values {
    values(json!({ "title": title, "body": format!("about {title}") }))
}

/// The titles of the items of kind `note`, sorted.
fn titles<L: EventLog>(store: &Store<L, Unsigned, Verify>) -> Vec<String> {
    let mut titles: Vec<_> =
        store.list("note", 100).unwrap().iter().map(|item| item.text("title").to_owned()).collect();
    titles.sort();
    titles
}

#[tokio::test]
async fn each_change_takes_the_next_revision_and_an_expected_one_must_be_current() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    let item = store.create("note", note("one"), "thread-1").await.unwrap();
    assert_eq!((item.revision, item.text("title"), item.created.by.as_str()), (1, "one", "thread-1"));
    let item = store.change(&item.id, values(json!({ "body": "two" })), Some(1), "thread-2").await.unwrap();
    assert_eq!((item.revision, item.text("body"), item.updated.by.as_str()), (2, "two", "thread-2"));
    assert_eq!(item.created.by, "thread-1");

    let refused = store.change(&item.id, values(json!({ "body": "stale" })), Some(1), "t").await.unwrap_err();
    let StoreError::Conflict { expected: 1, current } = refused else { panic!("{refused:?}") };
    assert_eq!(*current, item, "the current item comes back");
    let last = log.last_sequence(&sense).await.unwrap();
    let same = store.change(&item.id, values(json!({ "body": "two" })), Some(2), "t").await.unwrap();
    assert_eq!((same, log.last_sequence(&sense).await.unwrap()), (item.clone(), last), "nothing changed");
    let item = store.change(&item.id, values(json!({ "body": null, "core": true })), None, "t").await.unwrap();
    assert_eq!((item.revision, item.values.get("body"), &item.values["core"]), (3, None, &json!(true)));
    let unknown = store.change(&ItemId::new(), note("x"), None, "t").await.unwrap_err();
    assert!(matches!(unknown, StoreError::NotFound(_)), "{unknown:?}");
}

#[tokio::test]
async fn changes_keep_values_before_and_after_so_past_states_can_be_told() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    let pause = || std::thread::sleep(std::time::Duration::from_millis(2));
    let before = Utc::now();
    pause();
    let first = store.create("note", note("one"), "t").await.unwrap();
    pause();
    let between = Utc::now();
    pause();
    let second = store.change(&first.id, values(json!({ "body": "changed" })), Some(1), "u").await.unwrap();

    let history = store.history(&first.id).unwrap();
    let changes: Vec<_> = history.iter().map(|revision| &revision.change).collect();
    assert_eq!((changes[0].revision, &changes[0].before, &changes[0].after), (1, &None, &note("one")));
    let (was, became) = (values(json!({ "body": "about one" })), values(json!({ "body": "changed" })));
    assert_eq!((changes[1].revision, &changes[1].before, &changes[1].after), (2, &Some(was), &became));
    let stored = log.read_after(&sense, history[1].sequence - 1, 1).await.unwrap().remove(0).event().unwrap();
    assert_eq!((stored.kind.as_str(), &stored.data["before"]), (ITEM_EVENT, &json!({ "body": "about one" })));

    assert_eq!(store.at(&first.id, before).unwrap(), None, "not created yet");
    assert_eq!(store.at(&second.id, between).unwrap(), Some(first));
    assert_eq!(store.at(&second.id, Utc::now()).unwrap(), Some(second));
}

#[tokio::test]
async fn a_change_raced_by_another_writer_is_decided_again_on_what_it_wrote() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let (mut a, mut b) = (open(&log, &sense, None).await, open(&log, &sense, None).await);
    let item = a.create("note", note("one"), "a").await.unwrap();
    b.sync().await.unwrap();
    assert_eq!(b.get(&item.id).unwrap(), Some(item.clone()));
    let by_a = a.change(&item.id, values(json!({ "body": "by a" })), Some(1), "a").await.unwrap();

    // b has not read a's change: the log refuses b's append, and b finds the item at another revision.
    let refused = b.change(&item.id, values(json!({ "body": "by b" })), Some(1), "b").await.unwrap_err();
    let StoreError::Conflict { current, .. } = refused else { panic!("{refused:?}") };
    assert_eq!(*current, by_a);
    let created = a.create("note", note("two"), "a").await.unwrap();
    let by_b = b.change(&item.id, values(json!({ "title": "by b" })), None, "b").await.unwrap();
    assert_eq!((by_b.revision, by_b.text("body"), by_b.text("title")), (3, "by a", "by b"));
    assert_eq!(b.get(&created.id).unwrap(), Some(created), "what a wrote first applied");
    a.sync().await.unwrap();
    assert_eq!(a.get(&item.id).unwrap(), Some(by_b));
}

#[tokio::test]
async fn an_item_of_a_known_id_is_created_once_then_changed_from_the_revision_expected() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let (mut a, mut b) = (open(&log, &sense, None).await, open(&log, &sense, None).await);
    let id = ItemId::named("note", "only");
    let missing = a.put(&id, "note", note("one"), Some(1), "a").await.unwrap_err();
    assert!(matches!(missing, StoreError::NotFound(_)), "{missing:?}");
    let created = a.put(&id, "note", note("one"), Some(0), "a").await.unwrap();
    assert_eq!((&created.id, created.revision, created.kind.as_str()), (&id, 1, "note"));

    // b has not read a's creation: it finds the item when the log refuses its append.
    let refused = b.put(&id, "note", note("two"), Some(0), "b").await.unwrap_err();
    let StoreError::Conflict { expected: 0, current } = refused else { panic!("{refused:?}") };
    assert_eq!(*current, created);
    let changed = b.put(&id, "note", note("two"), Some(1), "b").await.unwrap();
    assert_eq!((changed.revision, changed.text("title")), (2, "two"));
    let anyway = a.put(&id, "note", values(json!({ "body": null })), None, "a").await.unwrap();
    assert_eq!((anyway.revision, anyway.values.get("body")), (3, None));
}

#[tokio::test]
async fn items_are_listed_by_a_value_or_its_absence() {
    let log = MemoryLog::default();
    let mut store = open(&log, &SenseId::new(), None).await;
    let one = store.create("note", values(json!({ "thread": "t1" })), "t").await.unwrap();
    let two = store.create("note", values(json!({ "thread": "t2", "done": true })), "t").await.unwrap();
    store.create("other", values(json!({ "thread": "t1" })), "t").await.unwrap();
    let ids = |items: Vec<Item>| items.into_iter().map(|item| item.id).collect::<Vec<_>>();
    let list = |name, value: Value| store.list_by("note", name, Some(&value), 10).unwrap();
    assert_eq!(ids(list("thread", json!("t1"))), std::slice::from_ref(&one.id));
    assert_eq!(ids(store.list_by("note", "done", None, 10).unwrap()), std::slice::from_ref(&one.id));
    assert_eq!(ids(list("thread\"", json!("t2"))), std::slice::from_ref(&two.id));
    assert_eq!(ids(list("done", json!(true))), [two.id]);
    assert_eq!(list("done", json!("true")), []);
    assert_eq!(list("thread", json!("t3")), []);
}

#[tokio::test]
async fn the_cache_is_built_again_unless_built_from_this_log_where_it_catches_up() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let home = ants_test_support::scratch_dir("sense-cache");
    let counted = Counted::new(&log);

    let mut store = open(&counted, &sense, Some(&home)).await;
    for title in ["one", "two"] {
        store.create("note", note(title), "t").await.unwrap();
    }
    drop(store);
    counted.read();
    let store = open(&counted, &sense, Some(&home)).await;
    assert_eq!((titles(&store), counted.read()), (vec!["one".to_owned(), "two".to_owned()], 1), "checked, kept");
    drop(store);

    // Behind: another writer appended; the cache reads only that.
    let mut other = open(&log, &sense, None).await;
    other.create("note", note("three"), "t").await.unwrap();
    let store = open(&counted, &sense, Some(&home)).await;
    assert_eq!((titles(&store).len(), counted.read()), (3, 2));
    drop(store);

    // The same messages in another incarnation of the store: built again from the start.
    let again = MemoryLog::new("again");
    for stored in log.read_after(&sense, 0, 10).await.unwrap() {
        let last = again.last_sequence(&sense).await.unwrap();
        again.append(&sense, stored.payload, last).await.unwrap();
    }
    let counted_again = Counted::new(&again);
    let store = open(&counted_again, &sense, Some(&home)).await;
    assert_eq!((titles(&store).len(), counted_again.read()), (3, 3));
    drop(store);

    // A log not holding what the cache was built from, in the same incarnation: built again.
    let other_log = MemoryLog::new("again");
    let mut store = open(&other_log, &sense, None).await;
    store.create("note", note("elsewhere"), "t").await.unwrap();
    let store = open(&other_log, &sense, Some(&home)).await;
    assert_eq!(titles(&store), ["elsewhere"]);
    drop(store);

    // Another sense identity: built again from its log.
    let store = open(&log, &SenseId::new(), Some(&home)).await;
    assert_eq!(titles(&store), Vec::<String>::new());
    drop(store);

    // A corrupt cache file: replaced, built again.
    std::fs::write(crate::home::Home::new(&home).cache(), b"not a database at all, not even close to one").unwrap();
    let store = open(&counted, &sense, Some(&home)).await;
    assert_eq!((titles(&store).len(), counted.read()), (3, 3));
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn search_finds_items_by_words_the_best_matches_first() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    let titled = values(json!({ "title": "Rust lifetimes", "body": "borrows" }));
    let titled = store.create("note", titled, "t").await.unwrap();
    let bodied = values(json!({ "title": "Cooking", "body": "a rusty pan, then rust again: rust everywhere" }));
    let bodied = store.create("note", bodied, "t").await.unwrap();
    store.create("card", values(json!({ "title": "rust" })), "t").await.unwrap();
    store.create("note", values(json!({ "title": "unrelated" })), "t").await.unwrap();

    let found = |store: &Store<_, _, _>, text: &str, kind: Option<&str>| -> Vec<ItemId> {
        store.search(text, kind, 10).unwrap().into_iter().map(|hit| hit.item.id).collect()
    };
    assert_eq!(found(&store, "RUST", Some("note")), [titled.id.clone(), bodied.id.clone()]);
    assert_eq!(found(&store, "rust", None).len(), 3);
    assert_eq!(found(&store, "lifetime borrowed", None), std::slice::from_ref(&titled.id), "stemmed, any word");
    assert_eq!(found(&store, "\"rust\" AND (NEAR lifetimes* -", Some("note")).len(), 2, "no FTS5 syntax");
    assert_eq!(found(&store, " ;-) ", None), []);
    let hits = store.search("rust", Some("note"), 1).unwrap();
    assert!(hits.len() == 1 && hits[0].score > 0.0, "{hits:?}");

    store.change(&titled.id, values(json!({ "title": "Go", "body": null })), None, "t").await.unwrap();
    assert_eq!(found(&store, "lifetimes", None), []);
    assert_eq!(found(&store, "go", None), [titled.id]);
}

/// The items of dev-a, in a log in memory.
struct Memory;

impl Stores for Memory {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        Box::pin(async move {
            if *ant != dev_a() {
                return Err(StoreError::Unavailable(format!("no store for {ant}")));
            }
            let log: &'static MemoryLog = Box::leak(Box::default());
            let store = Store::open(ant_log(log, &SenseId::new()), Cache::in_memory()?).await?;
            Ok(Arc::new(tokio::sync::Mutex::new(store)) as Arc<dyn Items>)
        })
    }
}

#[tokio::test]
async fn sense_reaches_an_ant_s_items_through_the_stores_it_is_given() {
    let none = crate::Sense::new().items(&dev_a()).await.err().unwrap();
    assert!(matches!(none, StoreError::NotKept(_)), "{none:?}");
    let sense = crate::Sense::with_stores(Arc::new(Memory));
    let items = sense.items(&dev_a()).await.unwrap();
    let item = items.create("note", note("one"), "t").await.unwrap();
    assert_eq!(items.get(&item.id).await.unwrap(), Some(item.clone()));
    assert_eq!(items.search("one", None, 5).await.unwrap()[0].item, item);
    assert!(sense.items(&"dev-b".parse().unwrap()).await.is_err());
}

#[tokio::test]
async fn a_write_changes_several_items_in_one_event_all_or_none() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    let one = store.create("note", note("one"), "t").await.unwrap();
    let two = ItemId::new();
    let link = values(json!({ "next": two }));
    let writes = [
        Write::Create { id: two.clone(), kind: "note".into(), values: note("two") },
        Write::Change { id: one.id.clone(), values: link.clone(), expected: Some(1) },
    ];
    let written = store.write(&writes, "t").await.unwrap();
    assert_eq!((&written[0].id, written[1].revision, log.last_sequence(&sense).await.unwrap()), (&two, 2, 2));
    let stored = log.read_after(&sense, 1, 1).await.unwrap().remove(0).event().unwrap();
    assert_eq!((stored.kind.as_str(), stored.data["changes"].as_array().unwrap().len()), (ITEMS_EVENT, 2));
    assert_eq!(store.list_by("note", "next", Some(&json!(two)), 10).unwrap(), [written[1].clone()]);
    assert_eq!(store.history(&one.id).unwrap()[1].sequence, store.history(&two).unwrap()[0].sequence);

    let stale = [
        Write::Create { id: ItemId::new(), kind: "note".into(), values: note("three") },
        Write::Change { id: one.id.clone(), values: note("stale"), expected: Some(1) },
    ];
    let refused = store.write(&stale, "t").await.unwrap_err();
    assert!(matches!(refused, StoreError::Conflict { expected: 1, .. }), "{refused:?}");
    assert_eq!((titles(&store), log.last_sequence(&sense).await.unwrap()), (vec!["one".into(), "two".into()], 2));

    // A batch of which one change does not follow on its item changes nothing, for every reader.
    let mut other = ant_log(&log, &sense);
    let three = Change::create("note", note("three"), "t");
    let skipping =
        Change { revision: 5, before: Some(Values::new()), ..Change::of(&written[1], note("x"), "t").unwrap() };
    other.append(ITEMS_EVENT, json!({ "changes": [three, skipping] })).await.unwrap();
    store.sync().await.unwrap();
    assert_eq!(titles(&store), ["one", "two"]);
}

#[tokio::test]
async fn a_write_whose_caller_stopped_waiting_once_appended_is_cached_by_the_next_call() {
    let (log, sense) = (MemoryLog::default(), SenseId::new());
    let home = ants_test_support::scratch_dir("sense-stopped");
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let ant_log =
        AntLog::new(&log, Unsigned, verifier, sense.clone(), dev_a(), "m1".parse().unwrap(), Some(home.clone()));
    let mut store = Store::open(ant_log.unwrap(), Cache::open(&home).unwrap()).await.unwrap();
    let earlier_write = store.cache().hold_writes().await;
    {
        // Polled once: appended, then waiting for the cache's earlier write to end.
        let write = store.create("note", note("one"), "t");
        tokio::select! {
            biased;
            _ = write => panic!("the write did not wait for the cache"),
            () = std::future::ready(()) => {}
        }
    }
    drop(earlier_write);
    assert_eq!(log.last_sequence(&sense).await.unwrap(), 1, "appended");
    store.sync().await.unwrap();
    assert_eq!(titles(&store), ["one"]);
    store.create("note", note("two"), "t").await.unwrap();
    assert_eq!(titles(&store), ["one", "two"]);
    std::fs::remove_dir_all(&home).unwrap();
}
