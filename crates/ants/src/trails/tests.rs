//! Tests of `ants trail`: the shared fixtures, and its commands, one submodule each.

mod ratings;
mod records;
mod run;
mod saves;
mod steps;
mod terminal;
mod tips;

use std::path::Path;

use ants_proto::provider::{Origin, Starter};
use ants_proto::{Role, RunState, TRAIL_RUNS_BUCKET, TrailLeg, TrailRun, UserId};
use ants_test_support::user as p;
use ants_test_support::{trail_leg, trail_run};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::json;

use crate::connection::Nats;
use crate::test_support::Server;

fn script(name: &str) -> String {
    format!(
        "meta = {{\"name\": \"{name}\", \"description\": \"Ship {name}\", \"phases\": [\"build\", \"test\"]}}\n\
         agent(\"build {name}\")\n"
    )
}

fn write(dir: &Path, file: &str, text: &str) -> std::path::PathBuf {
    let path = dir.join(file);
    std::fs::write(&path, text).unwrap();
    path
}

/// The JetStream context of a human's connection, which creates the colony's missing buckets.
async fn human(server: &Server, user: &UserId) -> jetstream::Context {
    let creds = server.creds_as(user, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None, None).unwrap();
    nats.connect("test").await.unwrap().1
}

fn record(conductor: &UserId, trail: Option<&str>, state: RunState, started: chrono::DateTime<Utc>) -> TrailRun {
    let script = script(trail.unwrap_or("inline"));
    let leg = TrailLeg { script, args: json!({"branch": "main"}), steps: 2, ..trail_leg(trail, started) };
    TrailRun {
        route: vec![leg],
        owner: Some(p("alice")),
        origin: Origin { by: Starter::Human { user: p("alice") }, channel: None },
        phase: Some("build".to_owned()),
        steps: 2,
        turns: 7,
        ended_at: None,
        ..trail_run(conductor, trail, state, started)
    }
}

/// Put `value` under `key` of [`TRAIL_RUNS_BUCKET`], as the conductor owning the key does.
async fn put(js: &jetstream::Context, key: &str, value: &impl serde::Serialize) {
    let payload = serde_json::to_vec(value).unwrap();
    js.publish(format!("$KV.{TRAIL_RUNS_BUCKET}.{key}"), payload.into()).await.unwrap().await.unwrap();
}
