//! Fixtures of the colony: the entries ants keep in it.

use ants_proto::ColonyEntry;
use chrono::{DateTime, Utc};

/// The entry of `ant` on m1, started `started` seconds before `now` and last beating `beat` seconds before it.
pub(crate) fn entry(ant: &str, started: i64, beat: i64, now: DateTime<Utc>) -> ColonyEntry {
    ColonyEntry {
        capabilities: vec!["rust".to_owned(), "review".to_owned()],
        workdir: Some("/src/app".to_owned()),
        started_at: now - chrono::Duration::seconds(started),
        heartbeat_at: now - chrono::Duration::seconds(beat),
        ..ants_test_support::colony_entry(ant, "m1")
    }
}
