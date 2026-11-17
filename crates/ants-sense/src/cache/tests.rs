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

    Connection::open(cache_file(&home)).unwrap().pragma_update(None, "user_version", SCHEMA_VERSION + 1).unwrap();
    assert_eq!(Cache::open(&home).unwrap().built().unwrap(), None);
    std::fs::write(cache_file(&home), vec![0xff; 4096]).unwrap();
    assert_eq!(Cache::open(&home).unwrap().built().unwrap(), None);
    std::fs::remove_dir_all(&home).unwrap();
}
