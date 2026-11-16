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
use ants_daemon::creds;
use ants_daemon::harness::{self, Harnesses};
use ants_proto::JsDomain;
use async_nats::jetstream;
use serde::Serialize;

use self::ants::{check_ants, check_bypassed, check_host, check_machine_name, check_reactions, check_shared_workdirs};
use self::credentials::{check_backup, check_cli_creds};
use self::leaf_node::{Leaf, check_leaf};
use self::server::{check_resolver, check_storage, reach};
use self::services::{check_services, check_webhooks};
use self::transports::{check_transports, installed_transports, same_address};
use crate::auth;
use crate::humans::{Me, Rights};
use crate::service::Services;
use crate::webhook;

mod ants;
mod colony;
mod credentials;
mod leaf_node;
mod server;
mod services;
mod transports;

/// How long a network check may take.
pub const NETWORK_TIMEOUT: Duration = Duration::from_secs(5);

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
}

/// Run every check, in order: the config file, the CLI's credentials, the server, JetStream, the hub's stream and
/// buckets, the versions of ants running across the team, the JetStream storage, each ant's credentials, harness,
/// workdir and reactions responder, the hub's account resolver, the team keys' backup, the leaf node and outbox, the installed services, the
/// webhooks' config file and notification sinks, and the Dev Tunnel and HTTPS bridge services installed.
pub async fn run(setup: &Setup) -> Vec<Check> {
    let now = auth::now().unwrap_or_default();
    let mut checks = Vec::new();
    let loaded = check_config(&setup.config, &mut checks);
    let identity = check_cli_creds(setup, now, &mut checks);
    let leaf = Leaf::read(&setup.leaf_config);
    let ants = loaded.as_ref().map_or_default(|l| l.ants.as_slice());
    let reach = reach(setup, identity.as_ref(), leaf.as_ref().and_then(|l| l.as_ref().ok()));
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
    let ((reached, connected), ants_checked, resolver, reactions) =
        tokio::join!(reach, ants_checked, resolver, reactions);
    checks.extend(reached);
    let settings = loaded.as_ref().map_or_default(|l| l.storage);
    checks.extend(check_storage(connected.as_ref(), setup, settings).await);
    checks.extend(ants_checked);
    if let Some(loaded) = &loaded {
        let hostname = ants_daemon::colony::machine().await;
        checks.push(check_host(&loaded.host, ants, &hostname, now));
        checks.extend(check_machine_name(&loaded.host, &hostname));
    }
    checks.extend(check_shared_workdirs(ants, &setup.config));
    checks.extend(reactions);
    if let Some(loaded) = &loaded {
        let webhooks = setup.webhooks.exists().then(|| webhook::Config::read(&setup.webhooks).ok()).flatten();
        checks.extend(check_bypassed(loaded, webhooks.as_ref().map_or_default(|c| c.webhooks.as_slice())));
    }
    checks.push(resolver);
    checks.extend(check_backup(&setup.team));
    let human = identity.as_ref().map(|i| &i.me).filter(|me| me.role.is_human() && !me.id.is_human());
    checks.extend(check_leaf(setup, leaf, ants, human, connected.as_ref(), now).await);
    let (services, transports) = {
        let mut services = Services::under(setup.home.clone());
        (check_services(setup, &mut services), installed_transports(&mut services))
    };
    checks.extend(services);
    checks.extend(check_webhooks(&setup.webhooks).await);
    checks.extend(check_transports(transports, now).await);
    checks
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
}

fn check_config(path: &Path, checks: &mut Vec<Check>) -> Option<config::Loaded> {
    if !path.exists() {
        checks.push(Check::skip("config", format!("no config file at {}: no ants on this machine", path.display())));
        return None;
    }
    match config::load(path) {
        Ok(loaded) => {
            let names: Vec<&str> = loaded.ants.iter().map(|ant| ant.name.as_str()).collect();
            checks.push(Check::ok("config", format!("{} lists {}", path.display(), names.join(", "))));
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

#[cfg(test)]
mod tests;
