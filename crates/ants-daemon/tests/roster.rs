//! The roster against a real nats-server: entries appear, refresh, leave on a clean stop and expire after a crash.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ants_daemon::bus;
use ants_daemon::roster::{self, Registration, Workload};
use ants_proto::{Activity, RosterEntry, SignInNeeded, UserId};
use ants_test_support::NatsServer as Server;
use async_nats::jetstream::kv;

fn entry(ant: &str) -> RosterEntry {
    entry_on(ant, "test")
}

/// An entry of a daemon run of its own on `machine`.
fn entry_on(ant: &str, machine: &str) -> RosterEntry {
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let now = chrono::Utc::now();
    RosterEntry {
        ant: ant.parse().unwrap(),
        machine: machine.to_owned(),
        instance: Some(format!("run-{}", RUNS.fetch_add(1, Ordering::Relaxed))),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: vec!["rust".to_owned()],
        whoami: Some("Maintain the Rust backend.".to_owned()),
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: None,
        handling: None,
        harnesses: None,
        escalate_to: Vec::new(),
        version: None,
        perms: None,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
    }
}

async fn read(kv: &kv::Store, ant: &str) -> Option<RosterEntry> {
    kv.get(ant).await.unwrap().map(|v| serde_json::from_slice(&v).unwrap())
}

/// Poll until `ant`'s entry is gone, or fail after `limit`.
async fn wait_gone(kv: &kv::Store, ant: &str, limit: Duration) -> Duration {
    let start = Instant::now();
    while read(kv, ant).await.is_some() {
        assert!(start.elapsed() < limit, "{ant} still listed after {limit:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    start.elapsed()
}

#[tokio::test]
async fn entries_refresh_leave_and_expire() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
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
async fn listing_gives_every_entry_by_ant() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    assert!(roster::list_entries(&kv).await.unwrap().is_empty());

    let beat = Duration::from_secs(10);
    let _b = Registration::start(kv.clone(), entry("dev-b"), beat).await.unwrap();
    let _a = Registration::start(kv.clone(), entry("dev-a"), beat).await.unwrap();
    kv.put("junk", "not an entry".into()).await.unwrap();
    let listed: Vec<_> = roster::list_entries(&kv).await.unwrap().into_iter().map(|e| e.ant.to_string()).collect();
    assert_eq!(listed, ["dev-a", "dev-b"]);
}

/// Poll until `ant`'s entry shows `activity`, or fail after `limit`.
async fn wait_activity(kv: &kv::Store, ant: &str, activity: Activity, limit: Duration) -> RosterEntry {
    let start = Instant::now();
    loop {
        let entry = read(kv, ant).await.expect("listed");
        if entry.activity == Some(activity) {
            return entry;
        }
        assert!(start.elapsed() < limit, "{ant} not {activity:?} after {limit:?}: {entry:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn turning_working_or_idle_shows_before_the_next_beat() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
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

/// Poll until `ant`'s entry shows `signed_out`, or fail after `limit`.
async fn wait_signed_out(kv: &kv::Store, ant: &str, signed_out: Option<&SignInNeeded>, limit: Duration) {
    let start = Instant::now();
    loop {
        let entry = read(kv, ant).await.expect("listed");
        if entry.signed_out.as_ref() == signed_out {
            return;
        }
        assert!(start.elapsed() < limit, "{ant} not signed out as {signed_out:?} after {limit:?}: {entry:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_signed_out_harness_shows_until_it_signs_in_before_the_next_beat() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    let workload = Arc::new(Workload::default());
    let beat = Duration::from_secs(60);
    let _a = Registration::start_with(kv.clone(), entry("dev-a"), beat, Some(workload.clone())).await.unwrap();
    assert_eq!(read(&kv, "dev-a").await.unwrap().signed_out, None);

    let out = SignInNeeded { since: chrono::Utc::now(), sign_in: Some("copilot login".to_owned()) };
    workload.set_signed_out(Some(out.clone()));
    wait_signed_out(&kv, "dev-a", Some(&out), Duration::from_secs(5)).await;
    workload.set_signed_out(None);
    wait_signed_out(&kv, "dev-a", None, Duration::from_secs(5)).await;
}

/// The built-in messenger waits while another daemon serves the messenger, on this machine or another, takes over once
/// that one stops, and of two daemons starting at once only one goes ahead.
#[tokio::test]
async fn the_builtin_messenger_waits_for_another_daemons_messenger() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    let every = Duration::from_millis(100);

    let mut here = entry_on("messenger", "here");
    roster::wait_for_turn(&kv, &mut here, every).await.unwrap();
    assert_eq!(read(&kv, "messenger").await.map(|e| e.machine).as_deref(), Some("here"), "nobody served it");
    roster::wait_for_turn(&kv, &mut here, every).await.unwrap();
    assert_eq!(read(&kv, "messenger").await.unwrap().instance, here.instance, "its own entry");
    let mut neighbour = entry_on("messenger", "here");
    let blocked = tokio::time::timeout(every * 5, roster::wait_for_turn(&kv, &mut neighbour, every)).await;
    assert!(blocked.is_err(), "started while another daemon on the same machine serves the messenger");
    kv.delete("messenger").await.unwrap();

    let there = Registration::start(kv.clone(), entry_on("messenger", "there"), Duration::from_secs(10)).await.unwrap();
    let waiting = tokio::spawn({
        let kv = kv.clone();
        async move { roster::wait_for_turn(&kv, &mut here, every).await }
    });
    tokio::time::sleep(every * 5).await;
    assert!(!waiting.is_finished(), "started while another machine serves the messenger");
    assert_eq!(read(&kv, "messenger").await.unwrap().machine, "there");
    there.deregister().await;
    tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("took over").unwrap().unwrap();
    assert_eq!(read(&kv, "messenger").await.unwrap().machine, "here");
    kv.delete("messenger").await.unwrap();

    let racing = |machine: &str| {
        let (kv, mut entry) = (kv.clone(), entry_on("messenger", machine));
        tokio::spawn(async move { roster::wait_for_turn(&kv, &mut entry, every).await })
    };
    let (a, b) = (racing("a"), racing("b"));
    tokio::time::sleep(every * 5).await;
    assert!(a.is_finished() != b.is_finished(), "exactly one of two daemons starting at once goes ahead");
    a.abort();
    b.abort();
}

/// The roster leaves a marker when an entry expires, so a daemon waiting for its turn takes over within about a second
/// of the expiry, without looking at the roster in between.
#[tokio::test]
async fn a_waiting_daemon_takes_over_as_soon_as_the_entry_expires() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    let ttl = Duration::from_secs(2);
    let kv = bus::ensure_roster(&js, ttl).await.unwrap();
    assert!(ants_daemon::hub::has_limit_markers(&kv.stream.cached_info().config), "nats-server 2.11 or later");

    let there = Registration::start(kv.clone(), entry_on("messenger", "there"), Duration::from_secs(60)).await;
    let put_at = Instant::now();
    drop(there.unwrap());
    let mut here = entry_on("messenger", "here");
    let every = Duration::from_secs(60);
    tokio::time::timeout(ttl * 3, roster::wait_for_turn(&kv, &mut here, every)).await.expect("took over").unwrap();
    let took = put_at.elapsed();
    assert!(took >= ttl - Duration::from_millis(500), "took over before the entry expired: {took:?}");
    assert!(took <= ttl + Duration::from_millis(1500), "took over {took:?} after the put");
    assert_eq!(read(&kv, "messenger").await.unwrap().instance, here.instance);
}

/// Poll until `registration` is lost, or fail after `limit`.
async fn wait_lost(registration: &Registration, limit: Duration) {
    tokio::time::timeout(limit, registration.lost()).await.expect("handed over");
    assert!(registration.is_lost());
}

/// A held messenger entry is handed over as soon as another daemon puts the entry, such as one serving a listed
/// messenger, and its daemon then leaves that daemon's entry alone; the built-in messenger takes over again once that
/// daemon stops.
#[tokio::test]
async fn the_builtin_messenger_hands_over_to_a_messenger_started_later() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    let every = Duration::from_millis(100);
    // No heartbeat comes during the test: the watch of the entry does it all.
    let beat = Duration::from_secs(60);

    let mut builtin = entry_on("messenger", "here");
    let revision = roster::wait_for_turn(&kv, &mut builtin, every).await.unwrap();
    let held = Registration::hold(kv.clone(), builtin.clone(), revision, beat, None).await.unwrap();

    // Deleted under it, the entry is put back at once.
    kv.delete("messenger").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(read(&kv, "messenger").await.map(|e| e.instance), Some(builtin.instance.clone()));
    assert!(!held.is_lost());

    // A listed messenger, here on the same machine, puts its entry regardless.
    let listed = entry_on("messenger", "here");
    let explicit = Registration::start(kv.clone(), listed.clone(), Duration::from_secs(10)).await.unwrap();
    wait_lost(&held, Duration::from_secs(2)).await;
    held.deregister().await;
    assert_eq!(read(&kv, "messenger").await.unwrap().instance, listed.instance, "left the other daemon's entry");

    // Once the listed messenger stops, the built-in messenger, waiting again, takes over.
    let mut again = entry_on("messenger", "here");
    let waiting = tokio::spawn({
        let kv = kv.clone();
        async move { roster::wait_for_turn(&kv, &mut again, every).await.map(|_| again) }
    });
    tokio::time::sleep(every * 5).await;
    assert!(!waiting.is_finished(), "started while the listed messenger serves");
    explicit.deregister().await;
    let again = tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("took over").unwrap().unwrap();
    assert_eq!(read(&kv, "messenger").await.unwrap().instance, again.instance);
}

/// Of two daemons that each put the messenger's entry in turn, only the one whose put is the latest keeps serving:
/// one whose entry went away and was put by another daemon (as after its heartbeats stopped for the TTL) finds that
/// entry at its next put and hands over.
#[tokio::test]
async fn of_two_builtin_messengers_only_the_latest_put_serves() {
    let Some(server) = Server::start() else { return };
    let (_client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    let kv = bus::ensure_roster(&js, Duration::from_secs(30)).await.unwrap();
    let (every, beat) = (Duration::from_millis(100), Duration::from_millis(200));

    let mut a = entry_on("messenger", "here");
    let stale = roster::wait_for_turn(&kv, &mut a, every).await.unwrap();
    kv.delete("messenger").await.unwrap();
    let mut b = entry_on("messenger", "here");
    let revision = roster::wait_for_turn(&kv, &mut b, every).await.unwrap();
    let b_held = Registration::hold(kv.clone(), b.clone(), revision, beat, None).await.unwrap();

    let first = read(&kv, "messenger").await.unwrap();

    let a_held = Registration::hold(kv.clone(), a, stale, beat, None).await.unwrap();
    assert!(a_held.is_lost(), "its entry was put by another daemon");
    a_held.deregister().await;
    tokio::time::sleep(beat * 3).await;
    assert!(!b_held.is_lost());
    let later = read(&kv, "messenger").await.unwrap();
    assert_eq!(later.instance, b.instance);
    assert!(later.heartbeat_at > first.heartbeat_at, "its heartbeats go on");
}
