//! A machine's host connection: one NATS connection per daemon serving a whole config file, with the machine's host
//! credentials (`ants auth host`), apart from its ants' own. It keeps the machine's [`MachineRecord`] in
//! [`MACHINES_BUCKET`] current: the ants the daemon serves, put on start and whenever they change, and never deleted,
//! so an ant stays addressable on its machine after its roster entry expired. It also takes the admin orders to the
//! machine's ants and records their outcomes ([`crate::admin`]). Without host credentials, where the ants have their
//! own, the connection stays off and the daemon serves its ants as before.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ants_proto::{JsDomain, MACHINES_BUCKET, Machine, MachineRecord, UserId, host_inbox_prefix};
use anyhow::{Context as _, Result, bail};
use tokio::sync::watch;

use crate::creds::{self, Kind};
use crate::direct_kv::DirectKv;
use crate::{admin, bus, keystore, roster};

const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// What a host connection connects with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Host {
    /// The machine's name; None for the one its credentials name, else the first label of its host name.
    pub machine: Option<Machine>,
    pub nats: String,
    /// The host credentials; None to connect as no one, to a server without authentication.
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// The config's `[admin] remote`: whether admin orders may be carried out, else each is refused.
    pub remote_admin: bool,
}

static OFF_TOLD: AtomicBool = AtomicBool::new(false);

impl Host {
    /// The host connection of a daemon whose ants connect with `ant_creds`: with `creds` if there are any, as no one
    /// when no ant has credentials either, else none, said once per process.
    pub fn new(
        machine: Option<Machine>,
        nats: String,
        creds: Option<PathBuf>,
        js_domain: Option<JsDomain>,
        mut ant_creds: impl Iterator<Item = bool>,
    ) -> Option<Self> {
        if creds.is_none() && ant_creds.any(|has| has) {
            if !OFF_TOLD.swap(true, Ordering::Relaxed) {
                let named = machine.as_ref().map_or_else(|| "MACHINE".to_owned(), ToString::to_string);
                tracing::info!(
                    "no host credentials: the machine's record of its ants and its admin orders stay off; issue \
                     them with `ants auth host {named}` and name them in the config's [host] table"
                );
            }
            return None;
        }
        Some(Self { machine, nats, creds, js_domain, remote_admin: true })
    }
}

/// The machine `host` stands for: its `machine`, which must be the one its credentials name if they name one, else
/// theirs, else the first label of `hostname`.
pub fn machine_of(machine: Option<&Machine>, creds: Option<&Path>, hostname: &str) -> Result<Machine> {
    let named = match creds {
        Some(path) => match creds::check(path)?.kind {
            Kind::Host { machine } => Some(machine),
            _ => bail!("{} are no host credentials; issue them with `ants auth host MACHINE`", path.display()),
        },
        None => None,
    };
    match (machine, named) {
        (Some(set), Some(named)) if *set != named => {
            bail!("the [host] table names machine {set}, its credentials {named}")
        }
        (Some(set), _) => Ok(set.clone()),
        (None, Some(named)) => Ok(named),
        (None, None) => Ok(Machine::from_hostname(hostname)),
    }
}

/// The record of `machine` serving `ants`, the built-in messenger left out.
pub fn record(machine: &Machine, hostname: &str, ants: &[UserId]) -> MachineRecord {
    let mut ants: Vec<UserId> = ants.iter().filter(|ant| **ant != UserId::messenger()).cloned().collect();
    ants.sort();
    ants.dedup();
    MachineRecord {
        machine: machine.clone(),
        ants,
        hostname: hostname.to_owned(),
        version: ants_proto::ANTS_VERSION.to_owned(),
        updated_at: chrono::Utc::now(),
    }
}

/// Connect as `host`, keep its machine's record current with `ants` and take the admin orders to the machine's ants
/// ([`admin::serve`]), until `ants` closes. Fails only on what waiting cannot mend: credentials that do not load or
/// name another machine.
pub async fn run(host: Host, ants: watch::Receiver<Vec<UserId>>) -> Result<()> {
    let hostname = roster::machine().await;
    let machine = machine_of(host.machine.as_ref(), host.creds.as_deref(), &hostname)?;
    let accounts = host.creds.as_deref().map(creds::account).transpose()?.into_iter().collect();
    let Some(js) = connect(&host, &machine).await? else { return Ok(()) };
    let orders = admin::Orders { machine: machine.clone(), remote: host.remote_admin, accounts };
    tokio::select! {
        recorded = keep_record(&js, &host, &machine, &hostname, ants) => recorded,
        () = admin::serve(js.clone(), host.js_domain.clone(), orders) => Ok(()),
    }
}

/// Keep the record of `machine` current with `ants`, until `ants` closes.
async fn keep_record(
    js: &async_nats::jetstream::Context,
    host: &Host,
    machine: &Machine,
    hostname: &str,
    mut ants: watch::Receiver<Vec<UserId>>,
) -> Result<()> {
    let kv = DirectKv::new(js, host.js_domain.as_ref(), MACHINES_BUCKET).await?;
    let mut delay = RETRY_FIRST;
    loop {
        let value = serde_json::to_vec(&record(machine, hostname, &ants.borrow_and_update()))?;
        match kv.put(machine.as_str(), value).await {
            Ok(()) => {
                delay = RETRY_FIRST;
                tracing::debug!(%machine, "recorded the machine's ants");
                if ants.changed().await.is_err() {
                    return Ok(());
                }
            }
            Err(e) => {
                tracing::warn!(
                    %machine, error = format!("{e:#}"), retry_in = ?delay,
                    "recording the machine's ants failed; `ants init` creates the {MACHINES_BUCKET} bucket"
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    changed = ants.changed() => if changed.is_err() { return Ok(()) },
                }
                delay = (delay * 2).min(RETRY_MAX);
            }
        }
    }
}

/// The JetStream context of a connection as `host`, retried while the server is not reachable; None when nothing
/// waits for it any longer.
async fn connect(host: &Host, machine: &Machine) -> Result<Option<async_nats::jetstream::Context>> {
    bus::check_url(&host.nats)?;
    let options = || -> Result<async_nats::ConnectOptions> {
        let options = async_nats::ConnectOptions::new()
            .name(format!("ants host {machine}"))
            .custom_inbox_prefix(host_inbox_prefix(machine))
            .event_callback(|event| async move {
                match event {
                    async_nats::Event::ServerError(e) => tracing::warn!("host connection: {e}"),
                    event => tracing::debug!("host connection: {event}"),
                }
            });
        match &host.creds {
            Some(path) => {
                creds::warn(path);
                bus::authenticate(options, path, keystore::os())
            }
            None => Ok(options),
        }
    };
    let mut delay = RETRY_FIRST;
    loop {
        match options()?.connect(host.nats.as_str()).await {
            Ok(client) => return Ok(Some(bus::jetstream(client, host.js_domain.as_ref()))),
            Err(e) if bus::worth_retrying(&e) => {
                tracing::warn!(%machine, error = %e, retry_in = ?delay, "host connection: NATS not reachable yet");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
            Err(e) => return Err(e).context("connecting the host connection to NATS"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(name: &str) -> Machine {
        name.parse().unwrap()
    }

    #[test]
    fn the_machine_is_the_table_s_else_the_host_name_s() {
        assert_eq!(machine_of(Some(&m("box")), None, "laptop.local").unwrap(), m("box"));
        assert_eq!(machine_of(None, None, "laptop.local").unwrap(), m("laptop"));
    }

    #[test]
    fn host_credentials_are_needed_unless_no_ant_has_any() {
        let nats = || "nats://localhost:4222".to_owned();
        assert!(Host::new(None, nats(), None, None, [true, false].into_iter()).is_none());
        let anonymous = Host::new(None, nats(), None, None, [false].into_iter()).unwrap();
        assert_eq!(anonymous.creds, None);
        let creds = Some(PathBuf::from("host.creds"));
        assert_eq!(Host::new(None, nats(), creds.clone(), None, [true].into_iter()).unwrap().creds, creds);
    }

    #[test]
    fn the_record_lists_the_served_ants_once_and_sorted_without_the_messenger() {
        let ants: Vec<UserId> = ["dev-b", "dev-a", "dev-b"].iter().map(|a| a.parse().unwrap()).collect();
        let record = record(&m("box"), "box.local", &[ants, vec![UserId::messenger()]].concat());
        assert_eq!(record.ants, ["dev-a".parse::<UserId>().unwrap(), "dev-b".parse().unwrap()]);
        assert_eq!((record.key(), record.hostname.as_str()), ("box", "box.local"));
    }
}
