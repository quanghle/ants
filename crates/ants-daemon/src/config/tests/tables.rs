//! The tables reaching every ant: storage, memory pressure and trails, and the schedules.

use super::*;

#[test]
fn the_storage_table_reaches_every_ant() {
    let loaded = parse_all(
        "[storage]\nwarn_percent = 90\nmax_file_store = \"10GB\"\nmax_memory_store = 1024\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"",
    )
    .unwrap();
    let settings = storage::Settings { warn_percent: 90, max_file_store: Some(10 << 30), max_memory_store: Some(1024) };
    assert_eq!(loaded.storage, settings);
    assert_eq!(loaded.ants.len(), 3, "dev-a, the messenger and the sage");
    assert!(loaded.ants.iter().all(|ant| ant.storage == settings));
    assert_eq!(config(loaded.ants[0].clone()).unwrap().storage.jetstream, settings);
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    assert!(err("[storage]\nwarn_percent = 100").contains("warn_percent must be below 100"));
    assert!(err("[storage]\nmax_file_store = \"10 parsecs\"").contains("invalid size"));
    assert!(err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nstorage = {}").contains("unknown field"));
}

#[test]
fn the_memory_pressure_table_reaches_every_ant() {
    let loaded = parse_all(
        "[memory_pressure]\nmin_available = \"10%\"\nmax_harnesses = 2\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"",
    )
    .unwrap();
    let settings = pressure::Settings { min_available: pressure::Threshold::Percent(10), max_harnesses: 2 };
    assert!(loaded.ants.iter().all(|ant| ant.memory_pressure == settings));
    assert_eq!(config(loaded.ants[0].clone()).unwrap().pooling.memory_pressure, settings);
    let default = parse_all("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    assert_eq!(default.ants[0].memory_pressure, pressure::Settings::default(), "on unless turned off");
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    assert!(err("[memory_pressure]\nmax_harnesses = 0").contains("max_harnesses must be at least 1"));
    assert!(err("[memory_pressure]\nmin_available = \"100%\"").contains("invalid share"));
}

#[test]
fn the_trails_table_caps_the_workers_of_every_ant() {
    let loaded = parse_all("[trails]\nworkers = 2\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    assert!(loaded.ants.iter().all(|ant| ant.trail_workers == 2));
    assert_eq!(config(loaded.ants[0].clone()).unwrap().pooling.trail_workers, 2);
    let default = parse_all("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    assert_eq!(default.ants[0].trail_workers, crate::trail::DEFAULT_WORKERS);
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    assert!(err("[trails]\nworkers = 0").contains("[trails] workers is 0; it takes 1 to 16"));
    assert!(err("[trails]\nworkers = 17").contains("[trails] workers is 17; it takes 1 to 16"));
    assert!(err("[trails]\nthreads = 2").contains("unknown field `threads`"));
}

#[test]
fn schedules_take_the_top_level_server_and_unique_names() {
    let loaded = parse_all(
        "nats = \"wss://hub.example/nats\"\njs_domain = \"hub\"\n\
             [[schedule]]\nname = \"nightly\"\ncreds = \"~/auth/nightly.creds\"\ncron = \"0 3 * * *\"\n\
             ant = \"dev-a\"\nowner = \"alice\"\ntext = \"Audit.\"\n\
             [[schedule]]\nname = \"pulse\"\nnats = \"nats://127.0.0.1:4222\"\njs_domain = \"own\"\nevery = \"15m\"\n\
             channel = \"ops\"\nmention = [\"dev-b\"]\ntext = \"Stuck?\"\n",
    )
    .unwrap();
    let [nightly, pulse] = &loaded.schedules[..] else { panic!("two schedules") };
    let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
    assert_eq!(nightly.creds, Some(root.join("auth/nightly.creds")));
    assert_eq!(nightly.nats.as_deref(), Some("wss://hub.example/nats"));
    assert_eq!(nightly.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    assert_eq!(nightly.owner.as_ref().map(UserId::as_str), Some("alice"));
    assert_eq!(pulse.nats.as_deref(), Some("nats://127.0.0.1:4222"));
    assert_eq!(pulse.js_domain.as_ref().map(JsDomain::as_str), Some("own"));
    assert_eq!(pulse.mentions, schedule::Mentions::Listed(vec!["dev-b".parse().unwrap()]));

    let err = |text: &str| format!("{:#}", parse_all(text).unwrap_err());
    let one = "[[schedule]]\nname = \"nightly\"\ncron = \"@daily\"\nant = \"dev-a\"\ntext = \"t\"\n";
    assert!(err(&format!("{one}{one}")).contains("schedule nightly is listed twice"));
    let ant = format!("[[ant]]\nname = \"nightly\"\nworkdir = \".\"\n{one}");
    assert!(err(&ant).contains("schedule nightly has the name of an ant"));
    let reserved = one.replace("nightly", "system-sage");
    assert!(err(&reserved).contains("system-sage is reserved for a built-in ant"));
    let bad = err(&one.replace("@daily", "0 25 * * *"));
    assert!(bad.contains("config.toml: schedule nightly: cron \"0 25 * * *\": hour"), "{bad}");
    let bad = err(&format!("timezone = \"Mars/Olympus_Mons\"\n{one}"));
    assert!(bad.contains("config.toml: timezone \"Mars/Olympus_Mons\": unknown time zone"), "{bad}");
    let bad = err(&format!("{one}timezone = \"Europe/Londn\"\n"));
    assert!(bad.contains("schedule nightly: timezone \"Europe/Londn\": unknown time zone"), "{bad}");

    let zoned = |text: &str| parse_all(text).unwrap().schedules[0].when.to_string();
    assert_eq!(zoned(one), "cron \"@daily\"", "UTC by default");
    let tokyo = format!("timezone = \"Asia/Tokyo\"\n{one}");
    assert_eq!(zoned(&tokyo), "cron \"@daily\" in Asia/Tokyo", "the top-level default");
    assert_eq!(zoned(&format!("{tokyo}timezone = \"UTC\"\n")), "cron \"@daily\"", "the schedule's own");
}
