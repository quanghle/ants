//! Tests of the cache: searches are words only, a cache of another schema or unreadable is replaced, pending writes
//! prevent replacing it, and applying nothing new writes nothing.

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

#[test]
fn a_cache_of_another_schema_or_unreadable_is_replaced_with_an_empty_one() {
    let home = ants_test_support::scratch_dir("sense-cache-schema");
    let mut cache = Cache::open(&home).unwrap();
    let sense = SenseId::new();
    cache.reset(&sense, "one").unwrap();
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

#[test]
fn pending_writes_prevent_replacing_a_corrupt_missing_or_different_log_cache() {
    let home = ants_test_support::scratch_dir("sense-cache-pending");
    let mut cache = Cache::open(&home).unwrap();
    let sense = SenseId::new();
    cache.reset(&sense, "one").unwrap();
    let change = Change::create("note", serde_json::json!({"title":"local"}).as_object().unwrap().clone(), "t");
    let pending = Pending::new(ITEM_EVENT, serde_json::to_value(change).unwrap(), "m1".parse().unwrap());
    cache.enqueue(&pending).unwrap();
    assert!(cache.reset(&sense, "two").is_err());
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

#[test]
fn applying_nothing_where_the_cache_was_built_to_writes_nothing() {
    let home = ants_test_support::scratch_dir("sense-cache-nothing");
    let mut cache = Cache::open(&home).unwrap();
    cache.reset(&SenseId::new(), "one").unwrap();
    let tip = crate::log::Mark { index: 1, sha256: "00".into() };
    let position = Position { sequence: 1, tip, events: 1, bytes: 10, broken: None, ant: None };
    cache.apply(&[], Some(&position)).unwrap();
    let written = cache.db.total_changes();
    cache.apply(&[], Some(&position)).unwrap();
    assert_eq!(cache.db.total_changes(), written, "nothing written");
    let further = Position { sequence: 2, ..position };
    cache.apply(&[], Some(&further)).unwrap();
    assert_eq!(cache.built().unwrap().unwrap().position, Some(further), "a position moved on is kept");
    std::fs::remove_dir_all(home).unwrap();
}
