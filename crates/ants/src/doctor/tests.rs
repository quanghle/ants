//! Tests of `ants doctor`: the hub, versions, harnesses, shared settings, sense, leaf, keys, tunnels, bridges, webhook,
//! services, channels and config checks, and how a check renders.

mod ants;
mod channels;
mod config_file;
mod credentials;
mod leaf_node;
mod server;
mod services;
mod teams;
mod trails;
mod transports;

use ants_daemon::config::Ant;
use ants_daemon::monitoring::storage;
use ants_daemon::monitoring::storage::Usage;
use ants_daemon::secrets::creds::Seed;
use ants_daemon::secrets::keystore;
use ants_daemon::secrets::keystore::Store as _;
use ants_daemon::{bus, hub};
use ants_proto::{Machine, PERMISSIONS_VERSION, Role, STREAM, UserId};

use super::colony::{avatars, drift, harness_versions};
use super::credentials::{check_key, check_xkey};
use super::server::{check_store_account, consumer_limit, consumers, storage_check, supported_server};
use super::transports::leafnode_info;
use super::*;
use crate::ants::avatars::{Depth, Faces};
use crate::credentials::{backup, issued};
#[cfg(unix)]
use crate::node::bridge;
use crate::node::leaf;
use crate::node::members::EnrollApprover;
use crate::release::versions;
#[cfg(unix)]
use crate::service::{Action, Service};
use crate::test_support::Server;

fn setup(server: &Server, creds: Option<PathBuf>) -> Setup {
    Setup {
        nats: server.url.clone(),
        creds,
        js_domain: None,
        human: None,
        config: server.dir().join("no-config.toml"),
        leaf_config: server.dir().join("no-leaf.conf"),
        hub_config: server.dir().join("no-nats.conf"),
        harnesses: false,
        team: server.dir().join("no-team"),
        webhooks: server.dir().join("no-webhooks.toml"),
        home: None,
        worker: None,
    }
}

fn named<'a>(checks: &'a [Check], name: &str) -> Vec<&'a Check> {
    checks.iter().filter(|c| c.check == name).collect()
}

#[test]
fn a_check_renders_on_one_line_with_its_fixes_indented() {
    let check = Check::fail("hub", "stream ANTS is missing", vec!["ants init".to_owned()]);
    assert_eq!(check.to_string(), "fail  hub: stream ANTS is missing\n      fix: ants init");
    assert_eq!(Check::ok("server", "connected").to_string(), "ok    server: connected");
    assert_eq!(
        serde_json::to_string(&check).unwrap(),
        r#"{"check":"hub","status":"fail","detail":"stream ANTS is missing","fix":["ants init"]}"#
    );
    assert_eq!(
        serde_json::to_string(&Check::skip("leaf", "none")).unwrap(),
        r#"{"check":"leaf","status":"skip","detail":"none"}"#
    );
}

#[cfg(unix)]
/// The unit files scripts/install-units.sh writes for `args`, installed under `home` for this platform's
/// manager; false where it has none.
fn install_units(home: &Path, args: &[&str]) -> bool {
    let leaf = home.join("leaf.conf");
    let mut all = vec!["--nats-server", "/bin/sh", "--no-daemon", "--nats-conf", leaf.to_str().unwrap()];
    all.extend(args);
    install_units_as(home, &all)
}

#[cfg(unix)]
/// As [`install_units`], with `args` alone.
fn install_units_as(home: &Path, args: &[&str]) -> bool {
    let dest = match std::env::consts::OS {
        "macos" => home.join("Library/LaunchAgents"),
        "linux" => home.join(".config/systemd/user"),
        _ => return false,
    };
    let out = std::process::Command::new("sh")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/install-units.sh"))
        .arg("--out")
        .arg(&dest)
        .args(args)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    true
}

#[cfg(unix)]
/// The services under `home`, every one running.
fn running(home: &Path) -> Services {
    Services::answering(home, |command| {
        let stdout = match command {
            "id -u" => "501\n",
            c if c.starts_with("launchctl print") => "\tstate = running\n\tpid = 7\n",
            _ => "",
        };
        crate::service::Ran { ok: true, stdout: stdout.to_owned(), stderr: String::new() }
    })
}

#[cfg(unix)]
fn one<'a>(checks: &'a [Check], name: &str) -> &'a Check {
    let found = named(checks, name);
    assert_eq!(found.len(), 1, "one {name} check: {checks:#?}");
    found[0]
}
