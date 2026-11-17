use super::*;

#[test]
fn a_record_round_trips_and_names_its_key() {
    let ant = crate::p("dev-a");
    let born = "2026-11-15T10:00:00Z".parse().unwrap();
    let record = AntIdentity::new(ant, "0123456789abcdef0123456789abcdef".into(), born);
    let json = serde_json::to_string(&record).unwrap();
    assert_eq!(
        json,
        r#"{"ant":"dev-a","id":"0123456789abcdef0123456789abcdef","born":"2026-11-15T10:00:00Z","prompts":0}"#
    );
    assert_eq!(serde_json::from_str::<AntIdentity>(&json).unwrap(), record);
    assert_eq!(record.check_key("dev-a"), Ok(()));
    assert!(record.check_key("dev-b").is_err());
}

#[test]
fn stages_grow_with_prompts() {
    let stages = [0, 9, 10, 99, 100, 999, 1000].map(Stage::of);
    use Stage::*;
    assert_eq!(stages, [Callow, Callow, Worker, Worker, Veteran, Veteran, Elder]);
}
