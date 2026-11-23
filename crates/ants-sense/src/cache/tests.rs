//! Tests of the cache: searches are words only, a cache of another schema or unreadable is replaced, pending writes
//! prevent replacing it, applying nothing new writes nothing, and writes commit off the caller's thread.

use super::*;

#[test]
fn a_search_is_words_only() {
    assert_eq!(
        match_query("Rust's \"lifetimes\" AND (NEAR x*) rust"),
        Some(r#""Rust" OR "s" OR "lifetimes" OR "AND" OR "NEAR" OR "x" OR "rust""#.into())
    );
    assert_eq!(match_query("  -- ;) "), None);
    assert_eq!(match_query(&"a ".repeat(100)), Some("\"a\"".into()));
    assert_eq!(
        match_query(&(0..100).map(|n| format!("w{n} ")).collect::<String>()).unwrap().matches(" OR ").count(),
        31
    );
}

#[tokio::test]
async fn a_cache_of_another_schema_or_unreadable_is_replaced_with_an_empty_one() {
    let home = ants_test_support::scratch_dir("sense-cache-schema");
    let cache = Cache::open(&home).unwrap();
    let sense = SenseId::new();
    cache.reset(&sense, "one").await.unwrap();
    drop(cache);
    let built = Cache::open(&home).unwrap().built().unwrap();
    assert_eq!(built, Some(Built { sense, incarnation: "one".into(), position: None }), "kept");

    Connection::open(Home::new(&home).cache())
        .unwrap()
        .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
        .unwrap();
    assert_eq!(Cache::open(&home).unwrap().built().unwrap(), None);
    std::fs::write(Home::new(&home).cache(), vec![0xff; 4096]).unwrap();
    assert_eq!(Cache::open(&home).unwrap().built().unwrap(), None);
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn pending_writes_prevent_replacing_a_corrupt_missing_or_different_log_cache() {
    let home = ants_test_support::scratch_dir("sense-cache-pending");
    let cache = Cache::open(&home).unwrap();
    let sense = SenseId::new();
    cache.reset(&sense, "one").await.unwrap();
    let change = Change::create("note", serde_json::json!({"title":"local"}).as_object().unwrap().clone(), "t");
    let pending = Pending::new(ITEM_EVENT, serde_json::to_value(change).unwrap(), "m1".parse().unwrap());
    cache.enqueue(&pending).await.unwrap();
    assert!(cache.reset(&sense, "two").await.is_err());
    drop(cache);
    let file = Home::new(&home).cache();
    std::fs::write(&file, b"corrupt with pending writes").unwrap();
    assert!(Cache::open(&home).is_err());
    assert_eq!(std::fs::read(&file).unwrap(), b"corrupt with pending writes");
    std::fs::remove_file(&file).unwrap();
    assert!(Cache::open(&home).is_err());
    assert!(!file.exists(), "a pending cache is not silently replaced");
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn applying_nothing_where_the_cache_was_built_to_writes_nothing() {
    let home = ants_test_support::scratch_dir("sense-cache-nothing");
    let cache = Cache::open(&home).unwrap();
    cache.reset(&SenseId::new(), "one").await.unwrap();
    let tip = crate::log::Mark { index: 1, sha256: "00".into() };
    let position = Position { sequence: 1, tip, events: 1, bytes: 10, broken: None, ant: None };
    cache.apply(&[], Some(&position)).await.unwrap();
    let written = cache.db().total_changes();
    cache.apply(&[], Some(&position)).await.unwrap();
    assert_eq!(cache.db().total_changes(), written, "nothing written");
    let further = Position { sequence: 2, ..position };
    cache.apply(&[], Some(&further)).await.unwrap();
    assert_eq!(cache.built().unwrap().unwrap().position, Some(further), "a position moved on is kept");
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn writes_commit_off_the_caller_s_thread() {
    let home = ants_test_support::scratch_dir("sense-cache-thread");
    let cache = Cache::open(&home).unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let noted = committed.clone();
    let hook = move || {
        noted.lock().unwrap().push(std::thread::current().id());
        false
    };
    cache.db().commit_hook(Some(hook)).unwrap();
    let sense = SenseId::new();
    cache.reset(&sense, "one").await.unwrap();
    let tip = crate::log::Mark { index: 1, sha256: "00".into() };
    let position = Position { sequence: 1, tip, events: 1, bytes: 10, broken: None, ant: None };
    cache.apply(&[], Some(&position)).await.unwrap();
    cache.set_context(&serde_json::json!({"machine": "m1"})).await.unwrap();
    let change = Change::create("note", serde_json::json!({"title":"local"}).as_object().unwrap().clone(), "t");
    let pending = Pending::new(ITEM_EVENT, serde_json::to_value(change).unwrap(), "m1".parse().unwrap());
    cache.enqueue(&pending).await.unwrap();
    let committed = committed.lock().unwrap().clone();
    assert_eq!(committed.len(), 4, "each write committed");
    assert!(!committed.contains(&std::thread::current().id()), "no write committed on the caller's thread");
    assert_eq!(cache.get(&pending.revisions().unwrap()[0].change.id).unwrap().unwrap().text("title"), "local");
    std::fs::remove_dir_all(home).unwrap();
}
