//! Fixtures of the colony: the entries ants keep in it.

use ants_proto::ColonyEntry;
use chrono::{DateTime, Utc};

/// The entry of `ant` on m1, started `started` seconds before `now` and last beating `beat` seconds before it.
pub(crate) fn entry(ant: &str, started: i64, beat: i64, now: DateTime<Utc>) -> ColonyEntry {
    ColonyEntry {
        ant: ant.parse().unwrap(),
        machine: "m1".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: vec!["rust".to_owned(), "review".to_owned()],
        whoami: None,
        profile: None,
        sense_unkept: None,
        workdir: Some("/src/app".to_owned()),
        started_at: now - chrono::Duration::seconds(started),
        heartbeat_at: now - chrono::Duration::seconds(beat),
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        sage: None,
        version: ants_proto::ANTS_VERSION.to_owned(),
        perms: ants_proto::PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        unserved: None,
        fallback: None,
        memory_low: None,
        model: None,
        paused: None,
        drift: Vec::new(),
        mcp: false,
    }
}
