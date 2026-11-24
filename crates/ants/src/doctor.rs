//! `ants doctor`: check a machine's ants setup end to end, reporting each check with the command that fixes it.
//!
//! It changes nothing: the hub's stream and buckets are looked up ([`hub::check`]), not created when missing as the
//! first connection of other commands does, and only the requests the credentials
//! allow are made, so none just times out. Every network check has a timeout; a harness handshake has its own.
//!
//! [`hub::check`]: ants_daemon::hub::check

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config;
use ants_daemon::harness::{self, Harnesses};
use ants_daemon::secrets::creds;
use ants_proto::{ColonyEntry, JsDomain, UserId};
use async_nats::jetstream;
use serde::Serialize;

use self::ants::{
    check_ants, check_bypassed, check_host, check_machine_name, check_mcp_sockets, check_open_files, check_reactions,
    check_sense_kept, check_sense_logs, check_shared_workdirs, check_trail_saves,
};
use self::credentials::{check_backup, check_cli_creds};
use self::leaf_node::{Leaf, check_leaf};
use self::server::{check_resolver, check_storage, check_store_account, reach};
use self::services::{check_services, check_webhooks};
pub use self::trails::trail_worker;
use self::trails::{check_runs, check_worker};
use self::transports::{check_transports, installed_transports, same_address};
use self::web::check_web_listeners;
use crate::auth;
use crate::people::humans::{Me, Rights};
use crate::release::update;
use crate::service::Services;
use crate::webhook;

mod ants;
mod colony;
mod credentials;
mod leaf_node;
mod server;
mod services;
mod trails;
mod transports;
mod web;

/// How long a network check may take.
const NETWORK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Warn,
    Fail,
    /// The check could not run, for the reason its detail gives.
    Skip,
}

/// The outcome of one check, with the commands fixing a warning or a failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Check {
    pub check: String,
    pub status: Status,
    pub detail: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fix: Vec<String>,
}

impl Check {
    fn new(check: impl Into<String>, status: Status, detail: impl Into<String>, fix: Vec<String>) -> Self {
        Self { check: check.into(), status, detail: detail.into(), fix }
    }

    fn ok(check: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(check, Status::Ok, detail, Vec::new())
    }

    fn skip(check: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(check, Status::Skip, detail, Vec::new())
    }

    fn warn(check: impl Into<String>, detail: impl Into<String>, fix: Vec<String>) -> Self {
        Self::new(check, Status::Warn, detail, fix)
    }

    fn fail(check: impl Into<String>, detail: impl Into<String>, fix: Vec<String>) -> Self {
        Self::new(check, Status::Fail, detail, fix)
    }
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self.status {
            Status::Ok => "ok",
            Status::Warn => "warn",
            Status::Fail => "fail",
            Status::Skip => "skip",
        };
        write!(f, "{status:<5} {}: {}", self.check, self.detail)?;
        for fix in &self.fix {
            write!(f, "\n      fix: {fix}")?;
        }
        Ok(())
    }
}

/// What to check: the connection the CLI resolved, and the machine's files.
pub struct Setup {
    pub nats: String,
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// Whom the CLI acts as without credentials.
    pub human: Option<UserId>,
    /// The config file of the machine's ants.
    pub config: PathBuf,
    /// The leaf node configuration of `ants leaf config`.
    pub leaf_config: PathBuf,
    /// The local hub's nats-server configuration of `ants setup`, whose service is checked when it is there and the
    /// leaf node configuration is not.
    pub hub_config: PathBuf,
    /// Whether to start each harness for an ACP handshake.
    pub harnesses: bool,
    /// The team directory of `ants auth`, whose system account key, when it is there, asks the hub how it keeps
    /// pushed accounts.
    pub team: PathBuf,
    /// The config file of `ants webhook serve`, checked when it is there.
    pub webhooks: PathBuf,
    /// The home directory whose installed services are checked; None for none.
    pub home: Option<PathBuf>,
    /// The `ants` a daemon here starts as its trail worker, `ants subprocess` ([`trail_worker`]); None leaves it
    /// unchecked.
    pub worker: Option<PathBuf>,
}

/// Run every check, in order: the config file, the CLI's credentials, the server, JetStream, the hub's stream and
/// buckets, the versions of ants running across the team, the JetStream storage, each ant's credentials, harness,
/// workdir and reactions responder, the trail worker and the runs of trails stuck, the hub's account resolver and store
/// account, the team keys' backup, the leaf node and outbox, the installed services, the WebSocket listeners' origins, the
/// webhooks' config file and notification sinks, and the Dev Tunnel and HTTPS bridge services installed.
pub async fn run(setup: &Setup) -> Vec<Check> {
    let now = auth::now().unwrap_or_default();
    let mut checks = Vec::new();
    let loaded = check_config(&setup.config, &mut checks);
    let identity = check_cli_creds(setup, now, &mut checks);
    let leaf = Leaf::read(&setup.leaf_config);
    let ants = loaded.as_ref().map_or_default(|l| l.ants.as_slice());
    // The nats-server configuration `ants daemon` runs itself, from `[nats_server]`.
    let managed = loaded.as_ref().and_then(|l| l.nats_server.as_ref()).map(|n| n.config.as_path());
    let reach = reach(setup, identity.as_ref(), leaf.as_ref().and_then(|l| l.as_ref().ok()), managed);
    let ants_checked = async {
        match &loaded {
            Some(loaded) => check_ants(&loaded.ants, &loaded.harnesses, setup, now).await,
            None => vec![Check::skip("ants", "no config file read: no ants to check")],
        }
    };
    let through_leaf =
        leaf.as_ref().and_then(|l| l.as_ref().ok()).is_some_and(|l| same_address(&setup.nats, &l.listen));
    let resolver = check_resolver(setup, through_leaf);
    let reactions = check_reactions(ants, setup, NETWORK_TIMEOUT);
    let trail_saves = check_trail_saves(ants, setup, NETWORK_TIMEOUT);
    let update = check_update(loaded.as_ref().map_or_default(|l| l.auto_update));
    let worker = async {
        match (&loaded, &setup.worker) {
            (None, _) => Check::skip("trail worker", "no config file read: no daemon here conducts runs"),
            (Some(loaded), _) if loaded.ants.is_empty() => {
                Check::skip("trail worker", "no ants served here: no daemon here conducts runs")
            }
            (Some(_), None) => Check::skip("trail worker", "no ants executable found to start as the worker"),
            (Some(_), Some(program)) => check_worker(program, ants_trail::GRACE).await,
        }
    };
    let ((reached, connected), ants_checked, resolver, reactions, trail_saves, update, worker) =
        tokio::join!(reach, ants_checked, resolver, reactions, trail_saves, update, worker);
    checks.extend(update);
    checks.extend(reached);
    let settings = loaded.as_ref().map_or_default(|l| l.storage);
    checks.extend(check_storage(connected.as_ref(), setup, settings).await);
    checks.extend(ants_checked);
    let hostname = ants_daemon::colony::machine().await;
    let host = loaded.as_ref().map(|l| &l.host);
    checks.extend(host.map(|host| check_host(host, ants, &hostname, now)));
    let colony = connected.as_ref().and_then(|c| c.colony.as_ref().ok()).map_or_default(Vec::as_slice);
    // Without a config file read, the host settings an ant described by flags goes by.
    let unread;
    let kept_host = match host {
        Some(host) => host,
        None => {
            unread = config::Summary::load(&setup.config).map_or_default(|s| s.host);
            &unread
        }
    };
    checks.extend(check_sense_kept(kept_host, ants, &hostname, colony, chrono::Utc::now()));
    checks.extend(host.and_then(|host| check_machine_name(host, &hostname)));
    checks.extend(check_shared_workdirs(ants, &setup.config));
    checks.extend(check_mcp_sockets(ants, &setup.config));
    checks.extend(check_open_files(ants, ants_daemon::limits::files::reachable()));
    checks.extend(check_sense_logs(ants));
    checks.extend(reactions);
    checks.extend(trail_saves);
    if let Some(loaded) = &loaded {
        let webhooks = setup.webhooks.exists().then(|| webhook::Config::read(&setup.webhooks).ok()).flatten();
        checks.extend(check_bypassed(loaded, webhooks.as_ref().map_or_default(|c| c.webhooks.as_slice())));
    }
    checks.push(worker);
    checks.extend(check_runs(connected.as_ref(), chrono::Utc::now()).await);
    checks.push(resolver);
    // After the CLI's connection, which has the hub load the team's account if no member connected yet.
    checks.push(check_store_account(setup, through_leaf).await);
    checks.extend(check_backup(&setup.team));
    let human = identity.as_ref().filter(|i| i.finding.is_some()).map(|i| &i.me).filter(|me| me.role.is_human());
    checks.extend(check_leaf(setup, leaf, ants, human, connected.as_ref(), managed, now).await);
    let (services, transports) = {
        let mut services = Services::under(setup.home.clone());
        (check_services(setup, &mut services, managed), installed_transports(&mut services))
    };
    checks.extend(services);
    checks.extend(check_web_listeners(setup, managed));
    checks.extend(check_webhooks(&setup.webhooks).await);
    checks.extend(check_transports(transports, now).await);
    checks
}

/// Whether `a` and `b` name the same file: their canonical paths when both are there, else their absolute paths.
pub(crate) fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => std::path::absolute(a).ok().is_some_and(|a| std::path::absolute(b).is_ok_and(|b| a == b)),
    }
}

/// Who the CLI connects as.
struct Identity {
    me: Me,
    rights: Rights,
    /// None without credentials.
    finding: Option<creds::Finding>,
}

/// A connection to the server, as [`Identity`].
struct Connected {
    js: jetstream::Context,
    rights: Rights,
    /// The colony's entries, else why they are unread.
    colony: Result<Vec<ColonyEntry>, String>,
}

fn check_config(path: &Path, checks: &mut Vec<Check>) -> Option<config::Loaded> {
    if !path.exists() {
        checks.push(Check::skip("config", format!("no config file at {}: no ants on this machine", path.display())));
        return None;
    }
    match config::load(path) {
        Ok(loaded) => {
            let names: Vec<&str> = loaded.ants.iter().map(|ant| ant.name.as_str()).collect();
            let elsewhere: String = loaded
                .elsewhere
                .iter()
                .map(|left| {
                    format!(
                        "; the built-in {name}, without credentials at {} here, is left to another machine, unless \
                         served with credentials kept elsewhere: ants daemon --config {} --ant {name} --creds CREDS",
                        left.creds.display(),
                        path.display(),
                        name = left.ant.name,
                    )
                })
                .collect();
            if names.is_empty() && loaded.host.creds.is_none() {
                let detail = format!(
                    "{} lists no ants to serve here, as on a machine where only humans run the CLI{elsewhere}",
                    path.display()
                );
                checks.push(Check::ok("config", detail));
            } else if names.is_empty() {
                let detail = format!("{} lists no ants{elsewhere}", path.display());
                let fix = format!("add an [[ant]] to {}, or copy a built-in ant's credentials here", path.display());
                checks.push(Check::fail("config", format!("{detail}: nothing to serve here"), vec![fix]));
            } else {
                let detail = format!("{} lists {}{elsewhere}", path.display(), names.join(", "));
                checks.push(Check::ok("config", detail));
            }
            Some(loaded)
        }
        Err(e) => {
            let mut fix = vec![format!("correct {}; `ants daemon --config {0}` refuses it as it is", path.display())];
            if harness::Unset::caused(&e) {
                let builtin = Harnesses::builtin().names().collect::<Vec<_>>().join(", ");
                fix.insert(
                    0,
                    format!(
                        "add `harness = \"NAME\"` at the top of {}, NAME a built-in harness ({builtin}) or a \
                         [harnesses.NAME] table; `ants setup` lists the ones installed here",
                        path.display()
                    ),
                );
            }
            checks.push(Check::fail("config", format!("{e:#}"), fix));
            None
        }
    }
}

/// Whether a release newer than this ants is out, asked of GitHub only when the config file's `auto_update` is on.
async fn check_update(mode: config::AutoUpdate) -> Option<Check> {
    if mode == config::AutoUpdate::Off {
        return None;
    }
    let running = update::Version::running();
    let latest = tokio::time::timeout(NETWORK_TIMEOUT * 2, update::resolve(None, false)).await;
    let release = match latest {
        Err(_) => return Some(Check::skip("update", "GitHub did not answer in time")),
        Ok(Err(e)) => return Some(Check::skip("update", format!("finding the latest release: {e:#}"))),
        Ok(Ok(release)) => release,
    };
    Some(match release.version().filter(|latest| *latest > running) {
        Some(latest) => Check::warn(
            "update",
            format!("ants {latest} is available (running {running}; auto_update = \"{}\")", mode.as_str()),
            vec!["ants update".to_owned()],
        ),
        None => Check::ok("update", format!("ants {running} is the latest release")),
    })
}

#[cfg(test)]
mod tests;
