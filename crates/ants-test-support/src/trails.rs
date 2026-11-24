//! Sample trail records: a run of one leg ([`trail_run`]) for a test to change the fields it is about in.

use ants_proto::provider::{Origin, Starter};
use ants_proto::{RunId, RunState, ThreadId, TrailLeg, TrailLimits, TrailRun, UserId};
use chrono::{DateTime, Utc};
use serde_json::Value;

/// A leg of `trail`, of an inline script for none, entered at its start at `at`: its script `meta = {}`, its
/// arguments null, one step made.
pub fn trail_leg(trail: Option<&str>, at: DateTime<Utc>) -> TrailLeg {
    TrailLeg {
        trail: trail.map(|trail| trail.parse().expect("a trail name")),
        local: false,
        at: None,
        script: "meta = {}".to_owned(),
        args: Value::Null,
        first_call: 0,
        started_at: at,
        steps: 1,
    }
}

/// A run of one leg ([`trail_leg`]) in `state` that `conductor` conducts for the colony's human, who started it on a
/// thread of its own at `at`, under the default limits: one step and one turn spent, updated at `at`, and ended then
/// once finished.
pub fn trail_run(conductor: &UserId, trail: Option<&str>, state: RunState, at: DateTime<Utc>) -> TrailRun {
    let ended_at = state.finished().then_some(at);
    TrailRun {
        run: RunId::generate(),
        conductor: conductor.clone(),
        route: vec![trail_leg(trail, at)],
        owner: Some(crate::human()),
        thread: ThreadId::generate(),
        parent: None,
        spends_from: None,
        grants: Vec::new(),
        origin: Origin { by: Starter::Human { user: crate::human() }, channel: None },
        work_branch: None,
        limits: TrailLimits::default(),
        state,
        phase: None,
        steps: 1,
        turns: 1,
        started_at: at,
        updated_at: at,
        ended_at,
    }
}
