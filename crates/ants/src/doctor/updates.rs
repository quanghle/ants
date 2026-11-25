//! The update check, while the config file's `auto_update` is on: whether a release newer than this ants is out, as
//! GitHub lists the stable ones, and where this machine stands to the version the bus runs, the one `ants signer`
//! records in `ants_machines` (`_account`), read with the CLI's credentials, with the release `auto_update = "install"`
//! would take.

use std::cmp::Ordering;

use ants_daemon::config::AutoUpdate;
use ants_proto::{MACHINES_BUCKET, Machine, subject};

use super::{Check, Connected, NETWORK_TIMEOUT};
use crate::release::update::{self, Decision, Version};

const CHECK: &str = "update";

/// The version the bus runs: the one `ants signer` recorded running on `machine`.
pub(super) struct Bus {
    pub(super) version: Version,
    pub(super) machine: Machine,
}

/// The latest stable release, asked of GitHub only while `mode` is on: None while off, else its version or why it is
/// unknown.
pub(super) async fn latest_release(mode: AutoUpdate) -> Option<Result<Version, String>> {
    if mode == AutoUpdate::Off {
        return None;
    }
    let latest = tokio::time::timeout(NETWORK_TIMEOUT * 2, update::resolve(None, false)).await;
    Some(match latest {
        Err(_) => Err("GitHub did not answer in time".to_owned()),
        Ok(Err(e)) => Err(format!("finding the latest release: {e:#}")),
        Ok(Ok(release)) => release.version().ok_or_else(|| format!("release tag {} is no version", release.tag_name)),
    })
}

/// The update check of `mode` ([`findings`]) once the latest release is `latest`, reading the version the bus runs
/// over `connected`; none while `auto_update` is off.
pub(super) async fn check_update(
    mode: AutoUpdate,
    latest: Option<Result<Version, String>>,
    connected: Option<&Connected>,
) -> Option<Check> {
    let latest = latest?;
    Some(findings(mode, &Version::running(), latest, bus(connected).await))
}

/// The version the bus runs, as the CLI's credentials read the record of `ants signer`: None when no signer recorded
/// one, else why it is unread.
async fn bus(connected: Option<&Connected>) -> Result<Option<Bus>, String> {
    let Some(connected) = connected else { return Err("the hub is not reached".to_owned()) };
    if !connected.rights.may_publish(&format!("$JS.API.DIRECT.GET.{}", subject::kv_stream(MACHINES_BUCKET))) {
        return Err(format!("the credentials may not read {MACHINES_BUCKET}"));
    }
    match tokio::time::timeout(NETWORK_TIMEOUT, crate::ants::machines::holder(&connected.js)).await {
        Err(_) => Err(format!("{MACHINES_BUCKET} unread within 5s")),
        Ok(Err(e)) => Err(format!("{e:#}")),
        Ok(Ok(holder)) => {
            Ok(holder.and_then(|h| Some(Bus { version: Version::parse(&h.version)?, machine: h.machine })))
        }
    }
}

/// What `mode` makes of this machine running `running`, the latest stable release `latest` and the bus `bus`: a
/// warning of a newer release, or of the machine running behind or ahead of the bus, each with the command moving it
/// on, else ok; skipped when neither the latest release nor the bus's version tells anything.
pub(super) fn findings(
    mode: AutoUpdate,
    running: &Version,
    latest: Result<Version, String>,
    bus: Result<Option<Bus>, String>,
) -> Check {
    let mut said = Vec::new();
    let newer = latest.as_ref().ok().filter(|latest| *latest > running);
    match (&latest, newer) {
        (_, Some(latest)) => {
            said.push(format!("ants {latest} is available (running {running}; auto_update = \"{}\")", mode.as_str()));
        }
        (Ok(_), None) => said.push(format!("ants {running} is the latest release")),
        (Err(e), None) => said.push(format!("the latest release is unknown: {e}")),
    }
    let known = bus.as_ref().ok().and_then(Option::as_ref);
    let mut fix = Vec::new();
    match &bus {
        Ok(Some(Bus { version, machine })) => {
            let runs = format!("the bus runs ants {version} (the version `ants signer` runs, on {machine})");
            match running.cmp(version) {
                Ordering::Less => {
                    said.push(format!("{runs}: this machine runs behind it"));
                    fix.push(format!("ants update --version {version}"));
                }
                Ordering::Greater => {
                    said.push(format!(
                        "{runs}: this machine runs ahead of it, and a release before 1.0 may change the wire"
                    ));
                    fix.push(format!("ants update --version {running} on {machine}, where `ants signer` runs"));
                }
                Ordering::Equal if newer.is_some() => {
                    said.push(format!("{runs}, as this machine does"));
                    let then = match mode {
                        AutoUpdate::Install => "auto_update = \"install\" then takes it here",
                        _ => "then `ants update` here",
                    };
                    fix.push(format!("ants update on {machine}, where `ants signer` runs, first; {then}"));
                }
                Ordering::Equal => said.push(format!("{runs}, as this machine does")),
            }
        }
        Ok(None) => said
            .push(format!("the version the bus runs is unknown: no `ants signer` recorded one in {MACHINES_BUCKET}")),
        Err(e) => said.push(format!("the version the bus runs is unread: {e}")),
    }
    if known.is_none() && newer.is_some() {
        fix.push("ants update".to_owned());
    }
    if mode == AutoUpdate::Install
        && let Ok(latest) = &latest
    {
        match update::decide(mode, running, latest, known.map(|bus| &bus.version)) {
            Decision::Install(target) => said.push(format!("auto_update = \"install\" takes ants {target}")),
            Decision::BusUnknown => {
                said.push("auto_update = \"install\" takes none while the version the bus runs is unknown".to_owned());
            }
            Decision::Nothing | Decision::Warn => {}
        }
    }
    let detail = said.join("; ");
    if !fix.is_empty() {
        Check::warn(CHECK, detail, fix)
    } else if latest.is_err() && known.is_none() {
        Check::skip(CHECK, detail)
    } else {
        Check::ok(CHECK, detail)
    }
}
