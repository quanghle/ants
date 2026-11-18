//! A machine's host connection: one NATS connection per daemon serving a whole config file, with the machine's host
//! credentials (`ants auth host`), apart from its ants' own. It keeps the machine's [`MachineRecord`] in
//! [`MACHINES_BUCKET`] current: the ants the daemon serves, put on start and whenever they change, and never deleted,
//! so an ant stays addressable on its machine after its colony entry expired. It also takes the admin orders to the
//! machine's ants and records their outcomes ([`crate::admin`]), reading the humans' consent to their assistants'
//! orders from [`ants_proto::HUMANS_BUCKET`] ([`crate::admin::held`]), and keeps the shared settings of the machine's
//! ants ([`crate::profile::shared`]), creating those still missing from the machine's config. Without host credentials,
//! where the ants have their own, the connection stays off and the daemon serves its ants as before, sense keeping none
//! of their data ([`uncredentialed`]). A daemon serving one ant (`ants daemon --ant`) makes no host connection, but
//! sense a link of its own with the host credentials, carrying its logs alone ([`run_sense`]).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ants_proto::{
    DEVICES_BUCKET, IDENTITIES_BUCKET, JsDomain, MACHINES_BUCKET, Machine, MachineRecord, UserId, host_inbox_prefix,
};
use anyhow::{Context as _, Result, bail};
use tokio::sync::watch;

use crate::profile::shared::{self, Shared};
use crate::records::direct_kv::DirectKv;
use crate::secrets::creds::{self, Kind};
use crate::secrets::keystore;
use crate::sense::{Hosting, Link};
use crate::{admin, bus, colony};

const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// What a host connection connects with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Host {
    /// The machine's name; None for the one its credentials name, else `fallback`.
    pub machine: Option<Machine>,
    /// The config's top-level `machine`, if it is a machine name: the machine's name where neither `machine` nor the
    /// credentials name one; None for the first label of the host name then.
    pub fallback: Option<Machine>,
    pub nats: String,
    /// The host credentials; None to connect as no one, to a server without authentication.
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// The config's `[admin] remote`: whether admin orders may be carried out, else each is refused.
    pub remote_admin: bool,
    /// The config file the daemon serves whole, which admin orders edit; none refuses those.
    pub config: Option<admin::ConfigFile>,
    /// The ants the process serves, which restart their harnesses on an admin order.
    pub restarts: admin::restart::Restarts,
    /// The machine's leaf node, whose uplink an add order renews to carry the ant added.
    pub leaf: Option<admin::LeafNode>,
}

static OFF_TOLD: AtomicBool = AtomicBool::new(false);

impl Host {
    /// The host connection of a daemon whose ants connect with `ant_creds`: with `creds` if there are any, as no one
    /// when no ant has credentials either, else none, said once per process.
    pub fn new(
        machine: Option<Machine>,
        fallback: Option<Machine>,
        nats: String,
        creds: Option<PathBuf>,
        js_domain: Option<JsDomain>,
        ant_creds: impl Iterator<Item = bool>,
    ) -> Option<Self> {
        let named = machine.as_ref().or(fallback.as_ref()).map_or_else(|| "MACHINE".to_owned(), ToString::to_string);
        let host = Self::quietly(machine, fallback, nats, creds, js_domain, ant_creds);
        if host.is_none() && !OFF_TOLD.swap(true, Ordering::Relaxed) {
            tracing::info!(
                "no host credentials: the machine's record of its ants and its admin orders stay off; issue them \
                 with `ants auth host {named}` and name them in the config's [host] table"
            );
        }
        host
    }

    /// [`Self::new`], saying nothing when there is none, as for sense's link of its own ([`run_sense`]), where each ant
    /// using sense says so as it starts.
    pub fn quietly(
        machine: Option<Machine>,
        fallback: Option<Machine>,
        nats: String,
        creds: Option<PathBuf>,
        js_domain: Option<JsDomain>,
        mut ant_creds: impl Iterator<Item = bool>,
    ) -> Option<Self> {
        if creds.is_none() && ant_creds.any(|has| has) {
            return None;
        }
        Some(Self {
            machine,
            fallback,
            nats,
            creds,
            js_domain,
            remote_admin: true,
            config: None,
            restarts: admin::restart::Restarts::default(),
            leaf: None,
        })
    }
}

/// Why sense keeps none of the data of the ants of `machine`, a machine without host credentials whose ants connect
/// with their own, and how to mend it.
pub fn uncredentialed(machine: &Machine) -> String {
    format!(
        "no host credentials on {machine}, which sense writes its logs with on a server with authentication: issue \
         them with `ants auth host {machine}`, or set provider = \"none\" for an ant meant to go without"
    )
}

/// The machine `host` stands for: its `machine`, which must be the one its credentials name if they name one, else
/// theirs, else `otherwise`.
pub fn machine_of(machine: Option<&Machine>, creds: Option<&Path>, otherwise: Machine) -> Result<Machine> {
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
        (None, None) => Ok(otherwise),
    }
}

/// The record of `machine` serving `ants`, the built-in messenger and sage left out.
pub fn record(machine: &Machine, hostname: &str, ants: &[UserId]) -> MachineRecord {
    let builtin = [UserId::messenger(), UserId::sage()];
    let mut ants: Vec<UserId> = ants.iter().filter(|ant| !builtin.contains(ant)).cloned().collect();
    ants.sort();
    ants.dedup();
    MachineRecord {
        machine: machine.clone(),
        ants,
        hostname: hostname.to_owned(),
        version: ants_proto::ANTS_VERSION.to_owned(),
        updated_at: chrono::Utc::now(),
        drift: std::collections::BTreeMap::new(),
    }
}

/// Connect as `host`, keep its machine's record current with `ants` and take the admin orders to the machine's ants
/// ([`admin::serve`]), until `ants` closes. Fails only on what waiting cannot mend: credentials that do not load or
/// name another machine.
pub async fn run(host: Host, ants: watch::Receiver<Vec<UserId>>) -> Result<()> {
    Box::pin(run_keeping(host, ants, None)).await
}

/// [`run`], sense reaching the data of the process's ants over the connection once up, when `sense` is given
/// ([`crate::sense`]).
pub async fn run_keeping(host: Host, ants: watch::Receiver<Vec<UserId>>, sense: Option<Hosting>) -> Result<()> {
    let Some((js, hostname, machine, accounts)) = link(&host).await? else { return Ok(()) };
    if let Some(sense) = &sense {
        sense.up(Link::new(js.clone(), host.js_domain.clone(), accounts.clone(), machine.clone()));
    }
    let humans = crate::people::humans::Registry::new(UserId::human())
        .verifying_grants(crate::people::humans::account(host.creds.as_deref()));
    let _watching = humans.watch(js.clone(), crate::people::humans::RETRY);
    let orders = admin::Orders {
        machine: machine.clone(),
        nats: host.nats.clone(),
        remote: host.remote_admin,
        accounts: accounts.clone(),
        config: host.config.clone(),
        creds: host.creds.clone(),
        js_domain: host.js_domain.clone(),
        restarts: host.restarts.clone(),
        leaf: host.leaf.clone(),
        hold: admin::held::Hold::new(humans),
    };
    let shared = host.config.as_ref().map(|c| c.shared.clone());
    let polled = ants.clone();
    tokio::select! {
        recorded = keep_record(&js, &host, &machine, &hostname, ants, shared.clone()) => recorded,
        () = admin::serve(js.clone(), host.js_domain.clone(), orders) => Ok(()),
        kept = keep_shared(&js, &host, &machine, &accounts, shared, polled) => kept,
    }
}

/// Connect as `host` for sense alone, reaching the data of the process's ants over the connection once up
/// ([`crate::sense`]), with none of a host connection's other duties: no machine record, admin orders or shared
/// settings. Runs until dropped; fails only on what waiting cannot mend, as [`run`] does.
pub async fn run_sense(host: Host, sense: Hosting) -> Result<()> {
    let Some((js, _, machine, accounts)) = link(&host).await? else { return Ok(()) };
    sense.up(Link::new(js, host.js_domain.clone(), accounts, machine));
    std::future::pending().await
}

/// The connection as `host` ([`connect`]), with the machine's host name, the machine it stands for ([`machine_of`])
/// and the accounts of its credentials.
async fn link(host: &Host) -> Result<Option<(async_nats::jetstream::Context, String, Machine, Vec<String>)>> {
    let hostname = colony::machine().await;
    let otherwise = host.fallback.clone().unwrap_or_else(|| Machine::from_hostname(&hostname));
    let machine = machine_of(host.machine.as_ref(), host.creds.as_deref(), otherwise)?;
    let accounts: Vec<String> = host.creds.as_deref().map(creds::account).transpose()?.into_iter().collect();
    let js = connect(host, &machine).await?;
    Ok(js.map(|js| (js, hostname, machine, accounts)))
}

/// Keep `shared` current with the shared settings of `ants`, verified against `accounts` ([`shared::refresh`]), until
/// `ants` closes; with no `shared`, nothing.
async fn keep_shared(
    js: &async_nats::jetstream::Context,
    host: &Host,
    machine: &Machine,
    accounts: &[String],
    shared: Option<Shared>,
    mut ants: watch::Receiver<Vec<UserId>>,
) -> Result<()> {
    let Some(shared) = shared else { return std::future::pending().await };
    let identities = DirectKv::new(js, host.js_domain.as_ref(), IDENTITIES_BUCKET).await?;
    let devices = DirectKv::new(js, host.js_domain.as_ref(), DEVICES_BUCKET).await?;
    let bucket = shared::Bucket { identities: &identities, devices: &devices, accounts };
    loop {
        let listed = ants.borrow_and_update().clone();
        shared::refresh(bucket, Some(machine), &shared, &listed).await;
        shared.polled();
        tokio::select! {
            () = tokio::time::sleep(shared::POLL_EVERY) => {}
            changed = ants.changed() => if changed.is_err() { return Ok(()) },
        }
    }
}

/// Keep the record of `machine` current with `ants` and the drift `shared` tells, until `ants` closes.
async fn keep_record(
    js: &async_nats::jetstream::Context,
    host: &Host,
    machine: &Machine,
    hostname: &str,
    mut ants: watch::Receiver<Vec<UserId>>,
    shared: Option<Shared>,
) -> Result<()> {
    let kv = DirectKv::new(js, host.js_domain.as_ref(), MACHINES_BUCKET).await?;
    let mut changes = shared.as_ref().map(Shared::subscribe);
    let mut delay = RETRY_FIRST;
    loop {
        let mut record = record(machine, hostname, &ants.borrow_and_update());
        if let Some(changes) = &mut changes {
            changes.mark_unchanged();
        }
        if let Some(shared) = &shared {
            record.drift = shared.drift().into_iter().filter(|(ant, _)| record.ants.contains(ant)).collect();
        }
        let value = serde_json::to_vec(&record)?;
        match kv.put(machine.as_str(), value).await {
            Ok(()) => {
                delay = RETRY_FIRST;
                tracing::debug!(%machine, "recorded the machine's ants");
                let shared_changed = async {
                    match &mut changes {
                        Some(changes) => changes.changed().await.is_ok(),
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    changed = ants.changed() => if changed.is_err() { return Ok(()) },
                    _ = shared_changed => {}
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
    fn the_machine_is_the_table_s_else_the_one_given() {
        assert_eq!(machine_of(Some(&m("box")), None, m("dev-vm1")).unwrap(), m("box"));
        assert_eq!(machine_of(None, None, m("dev-vm1")).unwrap(), m("dev-vm1"));
    }

    #[test]
    fn host_credentials_are_needed_unless_no_ant_has_any() {
        let nats = || "nats://localhost:4222".to_owned();
        assert!(Host::new(None, None, nats(), None, None, [true, false].into_iter()).is_none());
        let anonymous = Host::new(None, None, nats(), None, None, [false].into_iter()).unwrap();
        assert_eq!(anonymous.creds, None);
        let creds = Some(PathBuf::from("host.creds"));
        assert_eq!(Host::new(None, None, nats(), creds.clone(), None, [true].into_iter()).unwrap().creds, creds);
    }

    #[test]
    fn the_record_lists_the_served_ants_once_and_sorted_without_the_builtin_ones() {
        let ants: Vec<UserId> = ["dev-b", "dev-a", "dev-b"].iter().map(|a| a.parse().unwrap()).collect();
        let record = record(&m("box"), "box.local", &[ants, vec![UserId::messenger(), UserId::sage()]].concat());
        assert_eq!(record.ants, ["dev-a".parse::<UserId>().unwrap(), "dev-b".parse().unwrap()]);
        assert_eq!((record.key(), record.hostname.as_str()), ("box", "box.local"));
    }
}
