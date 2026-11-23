//! `ants daemon`: which ants to serve, from the config file and the command line, and serving them together.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ants_daemon::admin::restart::Restarts;
use ants_daemon::admin::{ConfigFile, LeafNode};
use ants_daemon::config::HostSettings;
use ants_daemon::host::Host;
use ants_daemon::profile::shared::{Shared, Via};
use ants_daemon::provider::Providers;
use ants_daemon::schedule::Schedule;
use ants_daemon::{Config, Stop};
use ants_proto::{JsDomain, Machine, UserId};
use anyhow::{Result, bail};
use futures::stream::{FuturesUnordered, StreamExt as _};
use tokio::task::JoinHandle;
use tracing::Instrument as _;

use self::running::{Links, Running, Schedules, reconcile};
use crate::reload::Triggers;
use crate::scheduler;

pub(crate) use self::running::creds_snapshot;
pub use self::selection::{AntOptions, DaemonArgs, Listed, ants, ants_on_reload, config_file, configs};

mod running;
mod selection;

/// What `ants daemon` serves: its ants' configs and its schedules, ready to run.
pub struct Served {
    pub configs: Vec<Config>,
    pub schedules: Vec<Schedule>,
    pub metrics: Option<SocketAddr>,
    /// The machine's host connection, with the ants to record as the machine's ([`host`]).
    pub host: Option<(Host, Vec<UserId>)>,
    /// Sense's link of its own, without a host connection ([`sense_link`]); None with one, or no ant using sense.
    pub sense: Option<SenseLink>,
    /// The shared settings `configs` were overlaid with: kept current by the host connection, else, without one, by
    /// reading them as the ants ([`shared::follow`]).
    ///
    /// [`shared::follow`]: ants_daemon::profile::shared::follow
    pub shared: Option<Shared>,
    /// The machine's name, which shared settings created without a host connection name as their writer; None for
    /// the first label of the host name.
    pub machine: Option<Machine>,
}

impl Served {
    /// Where to read the shared settings of the ants served without a host connection: each server the ants connect
    /// to, as the first of them there. None with a host connection, which reads them, or no shared settings to keep.
    fn vias(&self) -> Option<Vec<Via>> {
        if self.host.is_some() || self.shared.is_none() {
            return None;
        }
        let mut vias: Vec<Via> = Vec::new();
        for config in self.configs.iter().filter(|c| c.presence.builtin != Some(ants_proto::BuiltinAnt::Messenger)) {
            match vias
                .iter_mut()
                .find(|v| v.nats == config.connection.nats_url && v.js_domain == config.connection.js_domain)
            {
                Some(via) => via.ants.push(config.ant.clone()),
                None => vias.push(Via {
                    nats: config.connection.nats_url.clone(),
                    creds: config.connection.creds.clone(),
                    js_domain: config.connection.js_domain.clone(),
                    user: config.ant.clone(),
                    ants: vec![config.ant.clone()],
                }),
            }
        }
        Some(vias)
    }
}

/// The host connection of a daemon serving `configs` as `settings` describe it, `nats` and `js_domain` standing for
/// what the file does not set ([`Host::new`]), and admin orders editing the file `config`.
pub fn host(
    settings: Option<HostSettings>,
    configs: &[Config],
    nats: &str,
    js_domain: Option<&JsDomain>,
    config: Option<ConfigFile>,
) -> Option<(Host, Vec<UserId>)> {
    let fallback = settings.as_ref()?.fallback();
    let HostSettings { creds, machine, nats: file_nats, js_domain: file_domain, remote_admin, .. } = settings?;
    let nats = file_nats.unwrap_or_else(|| nats.to_owned());
    let domain = file_domain.or(js_domain.cloned());
    let mut host =
        Host::new(machine, fallback, nats, creds, domain, configs.iter().map(|c| c.connection.creds.is_some()))?;
    host.remote_admin = remote_admin;
    host.config = config;
    Some((host, configs.iter().map(|c| c.ant.clone()).collect()))
}

/// What sense reaches its ants' data over in a daemon without a host connection ([`sense_link`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SenseLink {
    /// A link of its own with the machine's host credentials, carrying none of a host connection's other duties
    /// ([`ants_daemon::host::run_sense`]).
    Own(Box<Host>),
    /// None: sense keeps none of the ants' data, for this reason ([`ants_daemon::host::uncredentialed`]).
    Off(String),
}

/// What sense reaches the data of `configs` over in a daemon whose host connection is `host`: None with one, which
/// carries it, or no ant using sense; else a link of its own as `settings` describe the machine's host connection,
/// `nats` and `js_domain` standing for what the file does not set, where the rule of [`Host::new`] makes one, and
/// otherwise none, why named after the machine as `hostname` stands for it.
pub fn sense_link(
    host: Option<&Host>,
    settings: Option<HostSettings>,
    configs: &[Config],
    nats: &str,
    js_domain: Option<&JsDomain>,
    hostname: &str,
) -> Option<SenseLink> {
    if host.is_some() || !configs.iter().any(|c| matches!(c.provider, ants_daemon::provider::Setting::Sense)) {
        return None;
    }
    let settings = settings.unwrap_or_default();
    let (fallback, named) = (settings.fallback(), settings.machine_or(hostname));
    let HostSettings { creds, machine, nats: file_nats, js_domain: file_domain, .. } = settings;
    let nats = file_nats.unwrap_or_else(|| nats.to_owned());
    let domain = file_domain.or(js_domain.cloned());
    let ant_creds = configs.iter().map(|c| c.connection.creds.is_some());
    Some(match Host::quietly(machine, fallback, nats, creds, domain, ant_creds) {
        Some(host) => SenseLink::Own(Box::new(host)),
        None => SenseLink::Off(ants_daemon::host::uncredentialed(&named)),
    })
}

/// How long [`run`] waits at start for the first read of the shared settings, by the host connection or without one,
/// to serve the ants with them rather than restart them once read.
const SHARED_FIRST_READ: Duration = Duration::from_secs(5);

/// The machine's leaf node, at ~/.config/ants/leaf.conf, whose uplink an add order renews to carry the ant added
/// ([`crate::node::leaf::uplink::renew_and_reconnect`]); none without a home directory.
pub fn leaf_node() -> Option<LeafNode> {
    let conf = crate::node::leaf::default_path().ok()?;
    Some(LeafNode {
        conf,
        uplink: crate::node::leaf::read_uplink,
        renew: crate::node::leaf::uplink::renew_and_reconnect,
        shrink: |conf, asked, shrunk| crate::node::leaf::uplink::shrink_and_reconnect(conf, Some(asked), shrunk),
    })
}

/// How [`run`] ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    Stopped,
    /// The executable was replaced and every ant stopped, for the new one to serve them.
    Replaced,
}

/// Serve every ant until `shutdown` (Ctrl-C or SIGTERM: [`stop_signal`]), each over its own NATS connection, and run
/// the schedules alongside, each over its own as well. An ant that fails is reported and the others keep running; it is
/// an error only when none is left. On SIGHUP, once the config file changed and settled, or at once after an admin
/// order edited it (`triggers`), `reload` lists the ants and schedules again and the daemon serves those
/// ([`reconcile`], [`Schedules::reconcile`]); an empty list leaves no ants running once they drain, and a list it
/// cannot read is reported and what runs stays as it is. Health and metrics are served on `served.metrics`, if given,
/// until the daemon stops; a reload does not move them. Once `replaced` the ants stop as on SIGTERM, finishing their
/// prompts in flight, and the schedules at once.
pub async fn run(
    served: Served,
    reload: impl Fn() -> Result<Served>,
    mut triggers: Triggers,
    replaced: impl Future<Output = ()>,
    shutdown: impl Future<Output = ()>,
) -> Result<Ended> {
    let metrics = served.metrics;
    let _metrics = serve_metrics(metrics, Arc::new(ants_daemon::monitoring::metrics::Daemon)).await?;
    let mut running = HashMap::new();
    // The ants served, which the host connection's admin orders restart, and their providers, which they reach.
    let (restarts, providers) = (Restarts::default(), Providers::default());
    let serve = |ant| serve(ant, restarts.clone(), providers.clone());
    let mut links = Links::new(providers.clone());
    links.reconcile(hosted(served.host.clone(), &restarts, &providers), Some(&served));
    let served = after_first_read(served, &reload, &mut links, &restarts, &providers).await;
    providers.reconcile(served.configs.iter().map(|c| (&c.ant, &c.provider)));
    serve_sense(&providers, &served.configs);
    let mut tasks: FuturesUnordered<_> = reconcile(&mut running, served.configs).into_iter().map(serve).collect();
    let mut schedules = Schedules::default();
    schedules.reconcile(served.schedules, run_schedule);
    tokio::pin!(shutdown, replaced);
    let (mut stopping, mut served, mut errors, mut ended) = (false, 0, Vec::new(), Ended::Stopped);
    while !tasks.is_empty() {
        tokio::select! {
            Some((ant, result)) = tasks.next() => {
                match result {
                    Ok(()) => served += 1,
                    Err(e) => {
                        tracing::error!(%ant, error = format!("{e:#}"), "ant stopped");
                        errors.push(e);
                    }
                }
                let next = running.remove(&ant).and_then(|r| r.next);
                if let Some(config) = next.filter(|_| !stopping) {
                    tracing::info!(%ant, "serving the ant again with its listed entry");
                    tasks.push(serve(Running::insert(&mut running, config)));
                }
            }
            () = &mut shutdown, if !stopping => {
                stopping = true;
                schedules.reconcile(Vec::new(), run_schedule);
                links.reconcile(None, None);
            }
            () = &mut replaced, if !stopping => {
                tracing::info!("stopping every ant as on SIGTERM, finishing the prompts in flight");
                (stopping, ended) = (true, Ended::Replaced);
                schedules.reconcile(Vec::new(), run_schedule);
                links.reconcile(None, None);
                for r in running.values_mut() {
                    r.next = None;
                    r.ask_to_stop();
                }
            }
            trigger = triggers.next(), if !stopping => match reload() {
                Ok(served) => {
                    tracing::info!(on = trigger.describe(), "config reloaded");
                    if served.metrics != metrics {
                        tracing::warn!("the metrics address changed; it takes effect on a restart");
                    }
                    links.reconcile(hosted(served.host.clone(), &restarts, &providers), Some(&served));
                    providers.reconcile(served.configs.iter().map(|c| (&c.ant, &c.provider)));
                    serve_sense(&providers, &served.configs);
                    tasks.extend(reconcile(&mut running, served.configs).into_iter().map(serve));
                    schedules.reconcile(served.schedules, run_schedule);
                }
                Err(e) => tracing::error!(
                    on = trigger.describe(),
                    error = format!("{e:#}"),
                    "reload failed; serving the ants as before"
                ),
            },
        }
    }
    outcome(ended, served, errors)
}

/// Serve the sense data of every ant of `configs` using sense among `providers` before any of them starts, so an event
/// of one ant about another, such as a clone's about its source, finds the other's data however their starts
/// interleave: an ant sense is not serving then is one whose data this process keeps not at all, and its events are
/// acked unread.
fn serve_sense(providers: &Providers, configs: &[Config]) {
    for config in configs {
        if let Some(sense) = providers.sense_of(&config.provider) {
            sense.serve(&config.ant, &config.storage.home, config.connection.creds.clone());
        }
    }
}

/// `wanted`, with its host connection restarting the ants on the admin orders `restarts` passes on and reaching their
/// `providers`.
fn hosted(
    wanted: Option<(Host, Vec<UserId>)>,
    restarts: &Restarts,
    providers: &Providers,
) -> Option<(Host, Vec<UserId>)> {
    wanted.map(|(host, ants)| (Host { restarts: restarts.clone(), providers: providers.clone(), ..host }, ants))
}

/// What [`run`] serves at start: what `reload` lists once the shared settings were first read, within
/// [`SHARED_FIRST_READ`]; `served` as it is without shared settings, when they could not be read yet, or when the
/// reload failed.
async fn after_first_read(
    served: Served,
    reload: &impl Fn() -> Result<Served>,
    links: &mut Links,
    restarts: &Restarts,
    providers: &Providers,
) -> Served {
    let Some(shared) = served.shared.clone() else {
        return served;
    };
    if !shared.wait_polled(SHARED_FIRST_READ).await {
        tracing::warn!("the shared settings could not be read yet; serving the config's until they are");
        return served;
    }
    match reload() {
        Ok(again) => {
            links.reconcile(hosted(again.host.clone(), restarts, providers), Some(&again));
            again
        }
        Err(e) => {
            tracing::error!(error = format!("{e:#}"), "reload with the shared settings failed; serving the config's");
            served
        }
    }
}

/// Wait for Ctrl-C or SIGTERM, forever when they cannot be waited for. Listening from its first poll on.
pub async fn stop_signal() {
    if let Err(e) = ants_daemon::daemon::stop_signal().await {
        tracing::warn!(error = %e, "waiting for Ctrl-C or SIGTERM");
        std::future::pending::<()>().await;
    }
}

/// How [`run`] ends once every ant stopped, `served` of them without an error: as `ended`, unless none was served
/// and some failed.
fn outcome(ended: Ended, served: usize, mut errors: Vec<anyhow::Error>) -> Result<Ended> {
    match (ended, served, errors.len()) {
        (Ended::Replaced, _, _) => Ok(Ended::Replaced),
        (_, 0, 1) => Err(errors.remove(0)),
        (_, 0, n) if n > 0 => bail!("none of the {n} ants could be served"),
        (ended, _, _) => Ok(ended),
    }
}

/// Serve one ant until it stops, reporting which, restarting its harnesses on the admin orders `restarts` passes on,
/// with its provider among `providers`.
fn serve(
    (config, stop): (Config, Stop),
    restarts: Restarts,
    providers: Providers,
) -> impl Future<Output = (UserId, Result<()>)> {
    let ant = config.ant.clone();
    let span = tracing::info_span!("ant", name = %ant);
    async move { (ant, ants_daemon::run_until(config, &stop, &restarts, &providers).await) }.instrument(span)
}

/// Run `schedule` in a task of its own until aborted.
fn run_schedule(schedule: Schedule) -> JoinHandle<()> {
    let name = schedule.name.clone();
    let span = tracing::info_span!("schedule", name = %name);
    tokio::spawn(
        async move {
            if let Err(e) = scheduler::run(schedule).await {
                tracing::error!(schedule = %name, error = format!("{e:#}"), "schedule stopped");
            }
        }
        .instrument(span),
    )
}

/// Serve health and metrics from `source` on `addr`, if given, for as long as the result is kept.
pub async fn serve_metrics(
    addr: Option<SocketAddr>,
    source: Arc<dyn ants_daemon::monitoring::metrics::Source>,
) -> Result<Option<ants_daemon::monitoring::metrics::Listening>> {
    match addr {
        Some(addr) => Ok(Some(ants_daemon::monitoring::metrics::start(addr, source).await?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests;
