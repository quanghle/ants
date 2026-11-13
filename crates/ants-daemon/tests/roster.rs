//! The roster against a real nats-server: entries appear, refresh, leave on a clean stop and expire after a crash.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ants_daemon::bus;
use ants_daemon::roster::{self, Registration};
use ants_proto::{PersonaId, RosterEntry};
use async_nats::jetstream::kv;

struct Server {
    child: Child,
    store: PathBuf,
    url: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.store);
    }
}

impl Server {
    /// None when nats-server is not installed.
    fn start() -> Option<Self> {
        if Command::new("nats-server").arg("--version").output().is_err() {
            eprintln!("nats-server not on PATH; skipping");
            return None;
        }
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let store = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("roster-js-{port}"));
        let child = Command::new("nats-server")
            .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"])
            .arg(&store)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self { child, store, url: format!("nats://127.0.0.1:{port}") };
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Some(server);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("nats-server did not start");
    }
}

fn entry(persona: &str) -> RosterEntry {
    let now = chrono::Utc::now();
    RosterEntry {
        persona: persona.parse().unwrap(),
        machine: "test".to_owned(),
        harness: "copilot".to_owned(),
        capabilities: vec!["rust".to_owned()],
        workdir: None,
        started_at: now,
        heartbeat_at: now,
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
    let (_client, js) = bus::connect(&server.url, None, &PersonaId::human(), "test").await.unwrap();
    let ttl = Duration::from_secs(2);
    let kv = bus::ensure_roster(&js, ttl).await.unwrap();
    let beat = Duration::from_millis(300);

    // Heartbeats outlive the TTL, then a clean stop removes the entry at once.
    let a = Registration::start(kv.clone(), entry("dev-a"), beat).await.unwrap();
    let first = read(&kv, "dev-a").await.expect("dev-a registered");
    assert_eq!(first.capabilities, ["rust"]);
    tokio::time::sleep(ttl + Duration::from_secs(1)).await;
    let later = read(&kv, "dev-a").await.expect("heartbeats keep dev-a listed");
    assert!(later.heartbeat_at > first.heartbeat_at);
    assert_eq!(later.started_at, first.started_at);
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
    let (_client, js) = bus::connect(&server.url, None, &PersonaId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    assert!(roster::list_entries(&kv).await.unwrap().is_empty());

    let beat = Duration::from_secs(10);
    let _b = Registration::start(kv.clone(), entry("dev-b"), beat).await.unwrap();
    let _a = Registration::start(kv.clone(), entry("dev-a"), beat).await.unwrap();
    kv.put("junk", "not an entry".into()).await.unwrap();
    let listed: Vec<_> = roster::list_entries(&kv).await.unwrap().into_iter().map(|e| e.persona.to_string()).collect();
    assert_eq!(listed, ["dev-a", "dev-b"]);
}
