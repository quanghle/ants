use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::{SugarId, UserId};
use serde_json::{Value, json};

use super::*;
use crate::log::{AccountVerifier, MemoryLog, NoRevocations, Stored, Unsigned};

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
        sugar: &SugarId,
        payload: Vec<u8>,
        expected_last: u64,
    ) -> impl Future<Output = Result<u64, crate::log::AppendError>> + Send {
        self.0.append(sugar, payload, expected_last)
    }

    async fn read_after(&self, sugar: &SugarId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        let read = self.0.read_after(sugar, after, limit).await?;
        self.1.fetch_add(read.len(), Ordering::Relaxed);
        Ok(read)
    }

    fn last_sequence(&self, sugar: &SugarId) -> impl Future<Output = Result<u64, LogError>> + Send {
        self.0.last_sequence(sugar)
    }

    fn incarnation(&self) -> &str {
        self.0.incarnation()
    }
}

type Verify = AccountVerifier<NoRevocations>;

fn ant_log<L: EventLog>(log: L, sugar: &SugarId) -> AntLog<L, Unsigned, Verify> {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    AntLog::new(log, Unsigned, verifier, sugar.clone(), dev_a(), "m1".parse().unwrap(), None).unwrap()
}

async fn open<L: EventLog>(log: L, sugar: &SugarId, home: Option<&Path>) -> Store<L, Unsigned, Verify> {
    let cache = match home {
        Some(home) => Cache::open(home).unwrap(),
        None => Cache::in_memory().unwrap(),
    };
    Store::open(ant_log(log, sugar), cache).await.unwrap()
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
    let (log, sugar) = (MemoryLog::default(), SugarId::new());
    let mut store = open(&log, &sugar, None).await;
    let item = store.create("note", note("one"), "thread-1").await.unwrap();
    assert_eq!((item.revision, item.text("title"), item.created.by.as_str()), (1, "one", "thread-1"));
    let item = store.change(&item.id, values(json!({ "body": "two" })), Some(1), "thread-2").await.unwrap();
    assert_eq!((item.revision, item.text("body"), item.updated.by.as_str()), (2, "two", "thread-2"));
    assert_eq!(item.created.by, "thread-1");

    let refused = store.change(&item.id, values(json!({ "body": "stale" })), Some(1), "t").await.unwrap_err();
    let StoreError::Conflict { expected: 1, current } = refused else { panic!("{refused:?}") };
    assert_eq!(*current, item, "the current item comes back");
    let last = log.last_sequence(&sugar).await.unwrap();
    let same = store.change(&item.id, values(json!({ "body": "two" })), Some(2), "t").await.unwrap();
    assert_eq!((same, log.last_sequence(&sugar).await.unwrap()), (item.clone(), last), "nothing changed");
    let item = store.change(&item.id, values(json!({ "body": null, "core": true })), None, "t").await.unwrap();
    assert_eq!((item.revision, item.values.get("body"), &item.values["core"]), (3, None, &json!(true)));
    let unknown = store.change(&ItemId::new(), note("x"), None, "t").await.unwrap_err();
    assert!(matches!(unknown, StoreError::NotFound(_)), "{unknown:?}");
}

#[tokio::test]
async fn changes_keep_values_before_and_after_so_past_states_can_be_told() {
    let (log, sugar) = (MemoryLog::default(), SugarId::new());
    let mut store = open(&log, &sugar, None).await;
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
    let stored = log.read_after(&sugar, history[1].sequence - 1, 1).await.unwrap().remove(0).event().unwrap();
    assert_eq!((stored.kind.as_str(), &stored.data["before"]), (ITEM_EVENT, &json!({ "body": "about one" })));

    assert_eq!(store.at(&first.id, before).unwrap(), None, "not created yet");
    assert_eq!(store.at(&second.id, between).unwrap(), Some(first));
    assert_eq!(store.at(&second.id, Utc::now()).unwrap(), Some(second));
}

#[tokio::test]
async fn a_change_raced_by_another_writer_is_decided_again_on_what_it_wrote() {
    let (log, sugar) = (MemoryLog::default(), SugarId::new());
    let (mut a, mut b) = (open(&log, &sugar, None).await, open(&log, &sugar, None).await);
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
async fn the_cache_is_built_again_unless_built_from_this_log_where_it_catches_up() {
    let (log, sugar) = (MemoryLog::default(), SugarId::new());
    let home = ants_test_support::scratch_dir("sugar-cache");
    let counted = Counted::new(&log);

    let mut store = open(&counted, &sugar, Some(&home)).await;
    for title in ["one", "two"] {
        store.create("note", note(title), "t").await.unwrap();
    }
    drop(store);
    counted.read();
    let store = open(&counted, &sugar, Some(&home)).await;
    assert_eq!((titles(&store), counted.read()), (vec!["one".to_owned(), "two".to_owned()], 1), "checked, kept");
    drop(store);

    // Behind: another writer appended; the cache reads only that.
    let mut other = open(&log, &sugar, None).await;
    other.create("note", note("three"), "t").await.unwrap();
    let store = open(&counted, &sugar, Some(&home)).await;
    assert_eq!((titles(&store).len(), counted.read()), (3, 2));
    drop(store);

    // The same messages in another incarnation of the store: built again from the start.
    let again = MemoryLog::new("again");
    for stored in log.read_after(&sugar, 0, 10).await.unwrap() {
        let last = again.last_sequence(&sugar).await.unwrap();
        again.append(&sugar, stored.payload, last).await.unwrap();
    }
    let counted_again = Counted::new(&again);
    let store = open(&counted_again, &sugar, Some(&home)).await;
    assert_eq!((titles(&store).len(), counted_again.read()), (3, 3));
    drop(store);

    // A log not holding what the cache was built from, in the same incarnation: built again.
    let other_log = MemoryLog::new("again");
    let mut store = open(&other_log, &sugar, None).await;
    store.create("note", note("elsewhere"), "t").await.unwrap();
    let store = open(&other_log, &sugar, Some(&home)).await;
    assert_eq!(titles(&store), ["elsewhere"]);
    drop(store);

    // Another sugar identity: built again from its log.
    let store = open(&log, &SugarId::new(), Some(&home)).await;
    assert_eq!(titles(&store), Vec::<String>::new());
    drop(store);

    // A corrupt cache file: replaced, built again.
    std::fs::write(crate::cache::cache_file(&home), b"not a database at all, not even close to one").unwrap();
    let store = open(&counted, &sugar, Some(&home)).await;
    assert_eq!((titles(&store).len(), counted.read()), (3, 3));
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn search_finds_items_by_words_the_best_matches_first() {
    let (log, sugar) = (MemoryLog::default(), SugarId::new());
    let mut store = open(&log, &sugar, None).await;
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
            let store = Store::open(ant_log(log, &SugarId::new()), Cache::in_memory()?).await?;
            Ok(Arc::new(tokio::sync::Mutex::new(store)) as Arc<dyn Items>)
        })
    }
}

#[tokio::test]
async fn sugar_reaches_an_ant_s_items_through_the_stores_it_is_given() {
    let none = crate::Sugar::new().items(&dev_a()).await.err().unwrap();
    assert!(matches!(none, StoreError::Unavailable(_)), "{none:?}");
    let sugar = crate::Sugar::with_stores(Arc::new(Memory));
    let items = sugar.items(&dev_a()).await.unwrap();
    let item = items.create("note", note("one"), "t").await.unwrap();
    assert_eq!(items.get(&item.id).await.unwrap(), Some(item.clone()));
    assert_eq!(items.search("one", None, 5).await.unwrap()[0].item, item);
    assert!(sugar.items(&"dev-b".parse().unwrap()).await.is_err());
}
