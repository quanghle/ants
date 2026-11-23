//! Sample members of a colony: a user by name ([`user`]) and the colony entry of an ant a daemon serves
//! ([`colony_entry`]), for a test to change the fields it is about in.

use ants_proto::{ANTS_VERSION, Activity, ColonyEntry, PERMISSIONS_VERSION, UserId};

/// The user named `name`.
pub fn user(name: &str) -> UserId {
    name.parse().expect("a user name")
}

/// The entry of `ant` served now on `machine` by this version's daemon, run `run-1` with the harness `copilot`: idle,
/// with no capabilities and nothing else set.
pub fn colony_entry(ant: &str, machine: &str) -> ColonyEntry {
    let now = chrono::Utc::now();
    ColonyEntry {
        ant: user(ant),
        machine: machine.to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: Vec::new(),
        whoami: None,
        profile: None,
        sense_unkept: None,
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        sage: None,
        version: ANTS_VERSION.to_owned(),
        perms: PERMISSIONS_VERSION,
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
