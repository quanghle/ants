//! What `ants daemon` logs once the NATS server its connections go to, or the JetStream behind it, stays out of reach
//! ([`bus::outage`]): what runs the server, or the leaf node's uplink, there, so the ERROR names what to start.

use std::path::PathBuf;

use ants_daemon::bus;
use ants_daemon::bus::outage::Of;

use crate::service::{Action, Service, Services, State};

/// The nats-server the daemon runs from its config file's `[nats_server]`.
pub(crate) struct Managed {
    /// The address its ants connect to it at.
    pub url: String,
    /// The config file.
    pub config: PathBuf,
}

/// Watch the daemon's connections and heartbeats ([`bus::outage::watch`]) for as long as the result is kept, after
/// [`bus::outage::OUT_OF_REACH`] naming what runs the server out of reach ([`what_runs`]), or the uplink to the
/// JetStream that does not answer through it ([`what_uplinks`]).
pub(crate) fn watch(managed: Option<Managed>) -> bus::outage::Watching {
    bus::outage::watch(bus::outage::OUT_OF_REACH, move |url, of| match of {
        Of::Server => what_runs(url, managed.as_ref(), &mut Services::local()),
        Of::JetStream => what_uplinks(url, &mut Services::local()),
    })
}

/// What runs the NATS server at `url` and what to do about it: the daemon itself, from its `[nats_server]`
/// (`managed`); else, for an address on this machine, the nats-server service installed among `services`, with the
/// command starting it when it is not running; else nothing known.
pub(crate) fn what_runs(url: &str, managed: Option<&Managed>, services: &mut Services) -> Option<String> {
    if let Some(managed) = managed.filter(|m| m.url == url) {
        return Some(format!(
            "`ants daemon` runs it from the [nats_server] of {} and starts it again after it exits: see this log for \
             why it does not answer",
            managed.config.display()
        ));
    }
    if !on_this_machine(url) {
        return None;
    }
    match services.status(Service::Nats) {
        State::Stopped(start) => Some(format!("its service is installed and not running: start it with `{start}`")),
        State::Running => {
            let restart = services.by_hand(Service::Nats, Action::Restart).unwrap_or_default();
            Some(format!("its service runs yet does not answer: see its log, or restart it with `{restart}`"))
        }
        State::Unknown(command, e) => Some(format!("finding its service failed: {command}: {e}")),
        State::Unsupported | State::NotInstalled | State::Other(_) => None,
    }
}

/// What carries the uplink of the leaf node at `url` to the hub, whose JetStream does not answer through it: for an
/// address on this machine, the Dev Tunnel or HTTPS bridge service installed among `services`, with the command
/// starting it when it is not running; else nothing known, for `ants doctor` to check.
pub(crate) fn what_uplinks(url: &str, services: &mut Services) -> Option<String> {
    if !on_this_machine(url) {
        return None;
    }
    let mut running = None;
    for service in [Service::Bridge, Service::DevTunnel] {
        match services.status(service) {
            State::Stopped(start) => {
                return Some(format!("the uplink's service is installed and not running: start it with `{start}`"));
            }
            State::Running => running = running.or(Some(service)),
            State::Unknown(command, e) => return Some(format!("finding the uplink's service failed: {command}: {e}")),
            State::Unsupported | State::NotInstalled | State::Other(_) => {}
        }
    }
    let restart = services.by_hand(running?, Action::Restart).unwrap_or_default();
    Some(format!(
        "the uplink's service runs: see its log, or restart it with `{restart}`; `ants doctor` checks the leaf node and \
         its uplink"
    ))
}

/// Whether each server of `url`, a comma-separated list too, is on a loopback address.
fn on_this_machine(url: &str) -> bool {
    url.split(',').all(|one| {
        let address = one.split_once("://").map_or(one, |(_, rest)| rest);
        let host_port = address.split(['/', '?', '#']).next().unwrap_or_default();
        let host = match host_port.strip_prefix('[') {
            Some(v6) => v6.split(']').next().unwrap_or_default(),
            None => host_port.rsplit_once(':').map_or(host_port, |(host, _)| host),
        };
        host == "localhost" || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
    })
}

#[cfg(all(test, unix))]
mod tests;
