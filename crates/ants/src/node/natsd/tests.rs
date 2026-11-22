use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::NatsServer;
use ants_test_support::{free_port, have, scratch_dir};
use anyhow::{Result, bail};

use super::managed::{Managed, address, alive, lock_path, probe, recorded, signal};
use super::*;

/// Supervision quick enough for tests.
const FAST: Policy = Policy {
    backoff: Duration::from_millis(50),
    max_backoff: Duration::from_millis(200),
    healthy: Duration::from_mins(1),
    max_restarts: 3,
    stop_grace: Duration::from_secs(5),
    orphan_grace: Duration::from_secs(5),
    probe_every: Duration::from_millis(50),
};

/// A nats-server configuration listening on `port`, its JetStream store and log in `dir`.
fn server_conf(dir: &Path, port: u16) -> PathBuf {
    let conf = dir.join("nats.conf");
    let (store, log) = (dir.join("js"), dir.join("nats.log"));
    std::fs::write(
        &conf,
        format!("listen: 127.0.0.1:{port}\njetstream {{ store_dir: {store:?} }}\nlog_file: {log:?}\n"),
    )
    .unwrap();
    conf
}

fn settings(command: impl Into<PathBuf>, config: &Path) -> NatsServer {
    NatsServer { command: command.into(), config: config.to_owned(), ready_timeout: Duration::from_secs(10) }
}

fn url(port: u16) -> String {
    format!("nats://127.0.0.1:{port}")
}

/// An executable shell script `name` in `dir` running `body`.
fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// A server started on `conf`, ready, and its process id.
async fn started(settings: &NatsServer, port: u16) -> (Managed, u32) {
    let managed = starting(settings, port).await.unwrap().unwrap();
    let pid = managed.pid().unwrap();
    (managed, pid)
}

/// [`start`] on `port`. Another process of the machine can listen on the port a moment, and a process another test
/// spawns holds a copy of the lock a moment: it tries again then.
async fn starting(settings: &NatsServer, port: u16) -> Result<Option<Managed>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match start(settings, &url(port), FAST, std::future::pending()).await {
            Err(e)
                if ["a server already listens", "another ants daemon"].iter().any(|m| e.to_string().starts_with(m))
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            started => return started,
        }
    }
}

/// Whether the lock of `conf` is free, or soon: a process another test spawns holds a copy of it a moment.
async fn lock_free(conf: &Path) -> bool {
    let free = || std::fs::File::open(lock_path(conf)).unwrap().try_lock().is_ok();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !free() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok()
}

fn recorded_server(conf: &Path) -> Option<u32> {
    recorded(&std::fs::read_to_string(lock_path(conf)).unwrap()).1
}

/// Wait, up to 10s, for `done`.
async fn until(mut done: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("in time");
}

#[test]
fn the_address_is_local() {
    assert_eq!(address("nats://127.0.0.1:4222").unwrap(), ("127.0.0.1".to_owned(), 4222));
    assert_eq!(address("tls://user:pw@localhost").unwrap(), ("localhost".to_owned(), 4222));
    assert_eq!(address("[::1]:5222").unwrap(), ("::1".to_owned(), 5222));
    assert_eq!(address("nats://127.0.0.2:7,nats://hub:4222").unwrap(), ("127.0.0.2".to_owned(), 7));
    let remote = address("nats://hub.example:4222").unwrap_err().to_string();
    assert!(remote.contains("needs the daemon to connect to it locally"), "{remote}");
    assert!(address("ws://localhost:8080").is_err());
}

#[test]
fn backoff_doubles_up_to_its_cap() {
    let waits: Vec<_> = (1..=7).map(|n| Policy::DEFAULT.backoff(n).as_secs()).collect();
    assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30]);
}

/// The server runs once ready, recorded in its lock; stopping it reaps it and releases the lock.
#[tokio::test]
async fn a_server_starts_ready_and_stops_reaped() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-start");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let (managed, pid) = started(&settings("nats-server", &conf), port).await;
    assert!(probe("127.0.0.1", port).await);
    assert_eq!(recorded_server(&conf), Some(pid));
    assert!(std::fs::File::open(lock_path(&conf)).unwrap().try_lock().is_err(), "held while it runs");
    managed.stop().await;
    assert!(!alive(pid), "reaped");
    assert!(lock_free(&conf).await);
    assert_eq!(recorded_server(&conf), None);
    std::fs::remove_dir_all(dir).unwrap();
}

/// What JetStream stored stays across a stop and a start.
#[tokio::test]
async fn jetstream_data_survives_a_restart() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-persist");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let settings = settings("nats-server", &conf);
    let (managed, _) = started(&settings, port).await;
    let js = async_nats::jetstream::new(async_nats::connect(url(port)).await.unwrap());
    let kv = js
        .create_key_value(async_nats::jetstream::kv::Config { bucket: "kept".into(), ..Default::default() })
        .await
        .unwrap();
    kv.put("key", "value".into()).await.unwrap();
    drop((kv, js));
    managed.stop().await;

    let (managed, _) = started(&settings, port).await;
    let js = async_nats::jetstream::new(async_nats::connect(url(port)).await.unwrap());
    let kv = js.get_key_value("kept").await.unwrap();
    assert_eq!(kv.get("key").await.unwrap().as_deref(), Some(&b"value"[..]));
    drop((kv, js));
    managed.stop().await;
    std::fs::remove_dir_all(dir).unwrap();
}

/// A second daemon on the same configuration is refused before starting anything.
#[tokio::test]
async fn a_second_owner_starts_nothing() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-twice");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let (managed, _) = started(&settings("nats-server", &conf), port).await;
    let ran = dir.join("ran");
    let stub = script(&dir, "stub", &format!("touch {ran:?}\nexec sleep 30"));
    let e = start(&settings(&stub, &conf), &url(port), FAST, std::future::pending()).await.err().unwrap();
    let expected =
        format!("another ants daemon (pid {}) runs the nats-server of {}", std::process::id(), conf.display());
    assert_eq!(e.to_string(), expected);
    assert!(!ran.exists());
    managed.stop().await;
    std::fs::remove_dir_all(dir).unwrap();
}

/// No server starts while another listens at the address; the lock is released.
#[tokio::test]
async fn an_occupied_port_starts_nothing() {
    let dir = scratch_dir("natsd-occupied");
    let other = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = other.local_addr().unwrap().port();
    let conf = server_conf(&dir, port);
    let ran = dir.join("ran");
    let stub = script(&dir, "stub", &format!("touch {ran:?}\nexec sleep 30"));
    let e = start(&settings(&stub, &conf), &url(port), FAST, std::future::pending()).await.err().unwrap();
    assert!(e.to_string().starts_with(&format!("a server already listens on 127.0.0.1:{port};")), "{e}");
    assert!(!ran.exists());
    assert!(lock_free(&conf).await);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_missing_executable_is_named() {
    let dir = scratch_dir("natsd-missing");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let missing = dir.join("no-nats-server");
    let e = starting(&settings(&missing, &conf), port).await.err().unwrap();
    assert_eq!(
        e.to_string(),
        format!("{} not found: install nats-server, or set [nats_server] command to its path", missing.display())
    );
    assert!(lock_free(&conf).await);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A server exiting at start, on an invalid configuration, is an error with its exit status.
#[tokio::test]
async fn an_invalid_configuration_fails_with_the_exit_status() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-invalid");
    let port = free_port();
    let conf = dir.join("nats.conf");
    std::fs::write(&conf, "listen: {{{\n").unwrap();
    let e = starting(&settings("nats-server", &conf), port).await.err().unwrap();
    let e = e.to_string();
    assert!(e.contains("exited before accepting connections (exit status: 1)"), "{e}");
    assert!(lock_free(&conf).await);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A server not ready in time is stopped and reaped.
#[tokio::test]
async fn a_server_not_ready_in_time_is_stopped() {
    let dir = scratch_dir("natsd-timeout");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let pid_file = dir.join("pid");
    let stub = script(&dir, "stub", &format!("echo $$ > {pid_file:?}\nexec sleep 30"));
    // Long enough for the stub to have started, even when the system checks a new script first.
    let settings = NatsServer { ready_timeout: Duration::from_secs(2), ..settings(&stub, &conf) };
    let e = starting(&settings, port).await.err().unwrap();
    let e = e.to_string();
    assert!(e.contains(&format!("did not accept connections on 127.0.0.1:{port} within 2s")), "{e}");
    let pid: u32 = std::fs::read_to_string(pid_file).unwrap().trim().parse().unwrap();
    assert!(!alive(pid), "stopped and reaped");
    assert!(lock_free(&conf).await);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A server exiting unasked starts again, ready, recorded in the lock.
#[tokio::test]
async fn an_unexpected_exit_restarts_the_server() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-restart");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let (mut managed, first) = started(&settings("nats-server", &conf), port).await;
    signal(first, libc::SIGKILL).unwrap();
    let again = async {
        until(async || recorded_server(&conf).is_some_and(|pid| pid != first) && probe("127.0.0.1", port).await).await;
    };
    tokio::select! {
        e = managed.supervise() => panic!("gave up: {e:#}"),
        () = again => {}
    }
    let second = managed.pid().unwrap();
    assert_ne!(second, first);
    assert_eq!(recorded_server(&conf), Some(second));
    managed.stop().await;
    assert!(!alive(second));
    std::fs::remove_dir_all(dir).unwrap();
}

/// A server that keeps exiting is given up on after the restarts the policy allows, and serving stops with the error.
#[tokio::test]
async fn a_server_exiting_again_and_again_is_given_up() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-give-up");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let (runs, once) = (dir.join("runs"), dir.join("once"));
    let stub = script(
        &dir,
        "stub",
        &format!("echo run >> {runs:?}\n[ -e {once:?} ] && exit 3\ntouch {once:?}\nexec nats-server \"$@\""),
    );
    let (managed, pid) = started(&settings(&stub, &conf), port).await;
    signal(pid, libc::SIGKILL).unwrap();
    let serving = run_serving(managed, std::future::pending::<Result<()>>());
    let e = tokio::time::timeout(Duration::from_secs(10), serving).await.unwrap().unwrap_err().to_string();
    assert!(e.contains("kept exiting: 3 restarts in a row"), "{e}");
    assert_eq!(std::fs::read_to_string(&runs).unwrap().lines().count(), 4, "started once and restarted 3 times");
    assert!(lock_free(&conf).await);
    std::fs::remove_dir_all(dir).unwrap();
}

/// The server stops only once serving is done.
#[tokio::test]
async fn the_server_stops_after_serving() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-order");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let (managed, pid) = started(&settings("nats-server", &conf), port).await;
    let serving = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok((alive(pid), probe("127.0.0.1", port).await))
    };
    assert_eq!(run_serving(managed, serving).await.unwrap(), (true, true), "running while serving");
    assert!(!alive(pid), "stopped after");
    let (managed, pid) = started(&settings("nats-server", &conf), port).await;
    let failing = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        bail!("serving failed, the server running: {}", alive(pid))
    };
    let e = run_serving::<()>(managed, failing).await.unwrap_err();
    assert_eq!(e.to_string(), "serving failed, the server running: true");
    assert!(!alive(pid), "stopped after a failure too");
    std::fs::remove_dir_all(dir).unwrap();
}

/// A leaf reload of the managed server's configuration reaches that server: SIGHUP keeps it running, SIGTERM has it
/// started again.
#[tokio::test]
async fn leaf_reloads_reach_the_managed_server() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-apply");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let (mut managed, first) = started(&settings("nats-server", &conf), port).await;
    let done = format!("kill -HUP {first} (the nats-server ants daemon runs)");
    assert_eq!(apply(&conf, Action::Reload), Outcome::Done(done));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(alive(first) && probe("127.0.0.1", port).await);
    assert_eq!(managed.pid(), Some(first));

    let done = format!("kill -TERM {first} (the nats-server ants daemon runs, which starts it again)");
    assert_eq!(apply(&conf, Action::Restart), Outcome::Done(done));
    let again = async {
        until(async || recorded_server(&conf).is_some_and(|pid| pid != first) && probe("127.0.0.1", port).await).await;
    };
    tokio::select! {
        e = managed.supervise() => panic!("gave up: {e:#}"),
        () = again => {}
    }
    assert_ne!(managed.pid(), Some(first));
    managed.stop().await;
    std::fs::remove_dir_all(dir).unwrap();
}

/// A server of the configuration a previous daemon left running is stopped before a new one starts; another process
/// the lock names is left alone.
#[tokio::test]
async fn a_server_left_running_is_stopped_first() {
    if !have("nats-server") {
        return;
    }
    let dir = scratch_dir("natsd-orphan");
    let port = free_port();
    let conf = server_conf(&dir, port);
    let settings = settings("nats-server", &conf);

    let mut other = std::process::Command::new("sleep").arg("30").spawn().unwrap();
    std::fs::write(lock_path(&conf), format!("daemon=1\nserver={}\n", other.id())).unwrap();
    let (managed, _) = started(&settings, port).await;
    assert!(alive(other.id()), "not a server of the configuration");
    managed.stop().await;
    other.kill().unwrap();
    other.wait().unwrap();

    let mut orphan = std::process::Command::new("nats-server").arg("-c").arg(&conf).spawn().unwrap();
    let left = orphan.id();
    // Reaped as soon as it exits, as its parent, init, would.
    let reaper = std::thread::spawn(move || orphan.wait().unwrap());
    until(async || probe("127.0.0.1", port).await).await;
    std::fs::write(lock_path(&conf), format!("daemon=1\nserver={left}\n")).unwrap();
    let (managed, pid) = started(&settings, port).await;
    assert_ne!(pid, left);
    assert!(!alive(left), "stopped first");
    reaper.join().unwrap();
    managed.stop().await;
    std::fs::remove_dir_all(dir).unwrap();
}
