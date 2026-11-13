//! The roster against a real nats-server: entries appear, refresh, leave on a clean stop and expire after a crash.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ants_daemon::bus;
use ants_daemon::roster::{self, Registration, Workload};
use ants_proto::{Activity, PersonaId, RosterEntry};
use ants_test_support::NatsServer as Server;
use async_nats::jetstream::kv;

fn entry(persona: &str) -> RosterEntry {
    let now = chrono::Utc::now();
    RosterEntry {
        persona: persona.parse().unwrap(),
        machine: "test".to_owned(),
        harness: "copilot".to_owned(),
        capabilities: vec!["rust".to_owned()],
        whoami: Some("Maintain the Rust backend.".to_owned()),
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: None,
        handling: None,
        harnesses: None,
    }
}

async fn read(kv: &kv::Store, persona: &str) -> Option<RosterEntry> {
    kv.get(persona).await.unwrap().map(|v| serde_json::from_slice(&v).unwrap())
}

/// Poll until `persona`'s entry is gone, or fail after `limit`.
async fn wait_gone(kv: &kv::Store, persona: &str, limit: Duration) -> Duration {
    let start = Instant::now();
    while read(kv, persona).await.is_some() {
        assert!(start.elapsed() < limit, "{persona} still listed after {limit:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    start.elapsed()
}

#[tokio::test]
async fn entries_refresh_leave_and_expire() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &PersonaId::human(), "test").await.unwrap();
    let ttl = Duration::from_secs(2);
    let kv = bus::ensure_roster(&js, ttl).await.unwrap();
    let beat = Duration::from_millis(300);

    // Heartbeats outlive the TTL, then a clean stop removes the entry at once.
    let a = Registration::start(kv.clone(), entry("dev-a"), beat).await.unwrap();
    let first = read(&kv, "dev-a").await.expect("dev-a registered");
    assert_eq!(first.capabilities, ["rust"]);
    assert_eq!(first.whoami.as_deref(), Some("Maintain the Rust backend."));
    tokio::time::sleep(ttl + Duration::from_secs(1)).await;
    let later = read(&kv, "dev-a").await.expect("heartbeats keep dev-a listed");
    assert!(later.heartbeat_at > first.heartbeat_at);
    assert_eq!(later.started_at, first.started_at);
    assert_eq!(later.whoami, first.whoami);
    a.deregister().await;
    assert!(read(&kv, "dev-a").await.is_none());

    // A daemon that dies just stops beating; its entry expires after the TTL.
    let b = Registration::start(kv.clone(), entry("dev-b"), beat).await.unwrap();
    drop(b);
    assert!(read(&kv, "dev-b").await.is_some());
    let gone_after = wait_gone(&kv, "dev-b", ttl * 3).await;
    assert!(gone_after >= ttl - Duration::from_millis(500), "expired too early: {gone_after:?}");

    // An existing bucket keeps its max age.
    let reopened = bus::ensure_roster(&js, Duration::from_secs(60)).await.unwrap();
    assert_eq!(reopened.status().await.unwrap().max_age(), ttl);
}

#[tokio::test]
async fn listing_gives_every_entry_by_persona() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &PersonaId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    assert!(roster::list_entries(&kv).await.unwrap().is_empty());

    let beat = Duration::from_secs(10);
    let _b = Registration::start(kv.clone(), entry("dev-b"), beat).await.unwrap();
    let _a = Registration::start(kv.clone(), entry("dev-a"), beat).await.unwrap();
    kv.put("junk", "not an entry".into()).await.unwrap();
    let listed: Vec<_> = roster::list_entries(&kv).await.unwrap().into_iter().map(|e| e.persona.to_string()).collect();
    assert_eq!(listed, ["dev-a", "dev-b"]);
}

/// Poll until `persona`'s entry shows `activity`, or fail after `limit`.
async fn wait_activity(kv: &kv::Store, persona: &str, activity: Activity, limit: Duration) -> RosterEntry {
    let start = Instant::now();
    loop {
        let entry = read(kv, persona).await.expect("listed");
        if entry.activity == Some(activity) {
            return entry;
        }
        assert!(start.elapsed() < limit, "{persona} not {activity:?} after {limit:?}: {entry:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn turning_working_or_idle_shows_before_the_next_beat() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &PersonaId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    let workload = Arc::new(Workload::default());
    workload.set_harnesses(1);
    let beat = Duration::from_secs(60);
    let _a = Registration::start_with(kv.clone(), entry("dev-a"), beat, Some(workload.clone())).await.unwrap();
    let first = read(&kv, "dev-a").await.unwrap();
    assert_eq!((first.activity, first.handling, first.harnesses), (Some(Activity::Idle), Some(0), Some(1)));

    let working = workload.working();
    let busy = wait_activity(&kv, "dev-a", Activity::Working, Duration::from_secs(5)).await;
    assert_eq!(busy.handling, Some(1));
    drop(working);
    wait_activity(&kv, "dev-a", Activity::Idle, Duration::from_secs(5)).await;
}
