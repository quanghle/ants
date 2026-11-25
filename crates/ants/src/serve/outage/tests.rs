//! Tests of what the outage ERROR says runs the server: the daemon's `[nats_server]`, the nats-server service
//! installed with its start command, or nothing known for an address elsewhere; what carries the uplink to the hub
//! whose JetStream does not answer: the HTTPS bridge or Dev Tunnel service, or nothing known; and a daemon's connection to a
//! throwaway nats-server stopped past the threshold logging one ERROR naming the service's start command, then an INFO.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_test_support::{NatsServer, free_port, scratch_dir};

use super::*;
use crate::service::Ran;

/// The services under `home`, with the nats-server service, if installed, not running.
fn stopped(home: &Path) -> Services {
    Services::answering(home, |command| {
        let (ok, stdout) = match command {
            "id -u" => (true, "501\n"),
            c if c.starts_with("launchctl print") => (true, "\tstate = not running\n"),
            _ => (false, ""),
        };
        Ran { ok, stdout: stdout.to_owned(), stderr: String::new() }
    })
}

/// The services under `home`, with the service `label` (`ants.nats`, …), if installed, running.
fn running(home: &Path, label: &'static str) -> Services {
    Services::answering(home, move |command| {
        let (ok, stdout) = match command {
            "id -u" => (true, "501\n"),
            c if c == format!("launchctl print gui/501/{label}") => (true, "\tstate = running\n\tpid = 4242\n"),
            c if c.starts_with("launchctl print") => (true, "\tstate = not running\n"),
            c if c.starts_with("systemctl --user is-active") && c.ends_with(&label.replace('.', "-")) => (true, ""),
            _ => (false, ""),
        };
        Ran { ok, stdout: stdout.to_owned(), stderr: String::new() }
    })
}

/// A home with the services `labels` (`ants.nats`, …) installed.
fn home_with(labels: &[&str]) -> PathBuf {
    let home = scratch_dir("outage");
    for label in labels {
        let unit = if cfg!(target_os = "macos") {
            home.join(format!("Library/LaunchAgents/{label}.plist"))
        } else {
            home.join(format!(".config/systemd/user/{}.service", label.replace('.', "-")))
        };
        std::fs::create_dir_all(unit.parent().unwrap()).unwrap();
        std::fs::write(unit, "").unwrap();
    }
    home
}

fn home_with_the_service() -> PathBuf {
    home_with(&["ants.nats"])
}

/// The command starting the service `label` (`ants.nats`, …).
fn start_of(label: &str) -> String {
    if cfg!(target_os = "macos") {
        format!("launchctl kickstart gui/501/{label}")
    } else {
        format!("systemctl --user start {}", label.replace('.', "-"))
    }
}

fn start_command() -> String {
    start_of("ants.nats")
}

#[test]
fn names_the_daemon_s_own_server_the_service_installed_or_nothing() {
    let home = home_with_the_service();
    let local = "nats://127.0.0.1:4222";
    let managed = Managed { url: local.to_owned(), config: PathBuf::from("/c/ants.toml") };
    let told = what_runs(local, Some(&managed), &mut stopped(&home)).unwrap();
    assert!(told.contains("[nats_server] of /c/ants.toml"), "{told}");

    let service = format!("its service is installed and not running: start it with `{}`", start_command());
    for url in [local, "nats://localhost:4222", "tls://[::1]:4222", "nats://127.0.0.1:4222,nats://127.0.0.1:4223"] {
        assert_eq!(what_runs(url, None, &mut stopped(&home)), Some(service.clone()), "{url}");
    }
    let elsewhere = "tls://hub.example.com:4222";
    assert_eq!(what_runs(elsewhere, Some(&managed), &mut stopped(&home)), None, "not this machine's");
    let bare = scratch_dir("outage");
    assert_eq!(what_runs(local, None, &mut stopped(&bare)), None, "no service installed");
    std::fs::remove_dir_all(&home).unwrap();
    std::fs::remove_dir_all(&bare).unwrap();
}

#[test]
fn names_the_uplink_s_service_or_nothing_for_ants_doctor_to_check() {
    let local = "nats://127.0.0.1:4222";
    let bare = scratch_dir("outage");
    assert_eq!(what_uplinks(local, &mut stopped(&bare)), None, "no uplink service: a direct uplink");
    for label in ["ants.bridge", "ants.devtunnel"] {
        let home = home_with(&["ants.nats", label]);
        let told = what_uplinks(local, &mut stopped(&home));
        let start = start_of(label);
        assert_eq!(told, Some(format!("the uplink's service is installed and not running: start it with `{start}`")));
        let told = what_uplinks(local, &mut running(&home, label)).unwrap();
        assert!(told.starts_with("the uplink's service runs: see its log, or restart it with `"), "{told}");
        assert!(told.contains(label) || told.contains(&label.replace('.', "-")), "{told}");
        assert_eq!(what_uplinks("tls://hub.example.com:4222", &mut stopped(&home)), None, "not this machine's");
        std::fs::remove_dir_all(&home).unwrap();
    }
    std::fs::remove_dir_all(&bare).unwrap();
}

/// What is logged, as fmt writes it.
#[derive(Clone, Default)]
struct Logged(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logged {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logged {
    /// The lines at `level` naming `url`.
    fn lines(&self, level: &str, url: &str) -> Vec<String> {
        let text = String::from_utf8(self.0.lock().unwrap().clone()).unwrap();
        text.lines().filter(|l| l.contains(level) && l.contains(url)).map(str::to_owned).collect()
    }
}

#[tokio::test]
async fn a_server_stopped_past_the_threshold_gets_one_error_naming_its_service_then_an_info() {
    let Some(mut server) = NatsServer::start_on(free_port()) else { return };
    let logged = Logged::default();
    let writer = logged.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .with_max_level(tracing::Level::INFO)
        .finish();
    let _logging = tracing::subscriber::set_default(subscriber);
    let home = home_with_the_service();
    let services = home.clone();
    let after = Duration::from_millis(500);
    let _watching = bus::outage::watch(after, move |url, _| what_runs(url, None, &mut stopped(&services)));
    let ant: ants_proto::UserId = "dev-a".parse().unwrap();
    let connecting = bus::connect_retrying(&server.url, None, None, &ant, "test", std::future::pending());
    let (client, _) = connecting.await.unwrap().unwrap();

    server.stop();
    let errored = async {
        while logged.lines("ERROR", &server.url).is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), errored).await.expect("no ERROR logged");
    tokio::time::sleep(after * 3).await;
    let errors = logged.lines("ERROR", &server.url);
    assert_eq!(errors.len(), 1, "one ERROR per outage: {errors:#?}");
    assert!(errors[0].contains(&format!("start it with `{}`", start_command())), "{}", errors[0]);
    assert!(errors[0].contains("out of reach for over 500ms"), "{}", errors[0]);

    server.start_again();
    let answered = async {
        while logged.lines("INFO", &server.url).iter().all(|l| !l.contains("answers again")) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(20), answered).await.expect("no INFO on reconnect");
    drop(client);
    std::fs::remove_dir_all(&home).unwrap();
}
