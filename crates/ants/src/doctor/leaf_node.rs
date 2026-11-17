//! The machine's leaf node: its configuration, uplink and system account credentials, whether it listens, and the
//! hub's sourcing of its outbox.

use std::path::{Path, PathBuf};

use ants_daemon::config::Ant;
use ants_daemon::secrets::creds::{self, Kind};
use ants_proto::{INTAKE_STREAM, JsDomain, Machine, Role, STREAM, UserId};

use super::credentials::check_creds;
use super::services::start_server;
use super::transports::same_address;
use super::{Check, Connected, NETWORK_TIMEOUT, Setup};
use crate::humans::Me;
use crate::{auth, leaf, outbox};

/// The machine's leaf node, as `ants leaf config` wrote its configuration.
pub(super) struct Leaf {
    pub(super) path: PathBuf,
    text: String,
    pub(super) machine: Machine,
    uplink: PathBuf,
    pub(super) listen: String,
    /// The hub's JetStream domain, as the uplink credentials carry it.
    pub(super) hub_domain: Option<JsDomain>,
}

impl Leaf {
    /// None without a configuration at `path`.
    pub(super) fn read(path: &Path) -> Option<anyhow::Result<Self>> {
        if !path.exists() {
            return None;
        }
        Some((|| {
            let text = std::fs::read_to_string(path)?;
            let (machine, uplink) = leaf::read_uplink(path)?;
            let listen = leaf::read_listen(path)?;
            let hub_domain = creds::check(&uplink).ok().and_then(|f| f.js_domain);
            Ok(Self { path: path.to_owned(), text, machine, uplink, listen, hub_domain })
        })())
    }
}

/// The leaf node's system account credentials, which carry the revocations pushed to the hub.
pub(super) fn check_leaf_system(
    setup: &Setup,
    leaf: &Leaf,
    uplink: &anyhow::Result<creds::Finding>,
    generic: String,
    now: u64,
) -> Check {
    let path = setup.leaf_config.display();
    let machine = &leaf.machine;
    let system = auth::leaf_system_creds(&leaf.uplink);
    let rewrite = format!("ants leaf config {machine} --uplink URL ... (picks up {})", system.display());
    if !system.is_file() {
        let detail = format!(
            "no system account credentials at {}: revocations pushed to the hub do not reach this leaf",
            system.display()
        );
        let issue = uplink.as_ref().ok().and_then(|f| f.renew().into_iter().next()).unwrap_or(generic);
        Check::warn("leaf system uplink", detail, vec![issue, rewrite])
    } else if !leaf.text.contains(&*system.to_string_lossy()) {
        let detail =
            format!("{path} does not use {}: revocations pushed to the hub do not reach this leaf", system.display());
        Check::warn("leaf system uplink", detail, vec![rewrite])
    } else {
        match creds::check(&system) {
            Ok(finding) if finding.kind == (Kind::LeafSystem { machine: machine.clone() }) => {
                check_creds("leaf system uplink", &system, &finding, now)
            }
            Ok(finding) => Check::fail(
                "leaf system uplink",
                format!("{} are the credentials of {}", system.display(), finding.holder()),
                vec![generic],
            ),
            Err(e) => Check::fail("leaf system uplink", format!("{e:#}"), vec![generic]),
        }
    }
}

/// The leaf node: its configuration, uplink and system account credentials, whether it listens and the CLI connects
/// to it, and whether the hub takes from its outbox what the machine's ants and humans send. `managed` is the
/// nats-server configuration the daemon runs itself ([`start_server`]).
pub(super) async fn check_leaf(
    setup: &Setup,
    leaf: Option<anyhow::Result<Leaf>>,
    ants: &[Ant],
    human: Option<&Me>,
    connected: Option<&Connected>,
    managed: Option<&Path>,
    now: u64,
) -> Vec<Check> {
    let path = setup.leaf_config.display();
    let leaf = match leaf {
        None => return vec![Check::skip("leaf", format!("no leaf node configuration at {path}"))],
        Some(Err(e)) => {
            let fix = "ants leaf config MACHINE --uplink URL".to_owned();
            return vec![Check::fail("leaf", format!("{e:#}"), vec![fix])];
        }
        Some(Ok(leaf)) => leaf,
    };
    let machine = &leaf.machine;
    let mut checks = vec![Check::ok("leaf", format!("{path}: machine {machine}, listening on {}", leaf.listen))];

    let uplink = creds::check(&leaf.uplink);
    let generic =
        format!("ants auth leaf {machine} --ant ANT... --human HUMAN..., then ants leaf config {machine} ...");
    let (check, members) = check_uplink(setup, &leaf, &uplink, ants, human, &generic, now);
    checks.push(check);
    checks.push(check_leaf_system(setup, &leaf, &uplink, generic, now));
    checks.push(check_listening(setup, &leaf, managed).await);
    checks.push(check_outbox(machine, members.as_deref(), uplink.as_ref().ok(), connected).await);
    checks
}

/// The leaf node's uplink credentials, which carry the rights of the ants and the human connecting through the leaf
/// node, with the members they carry.
fn check_uplink(
    setup: &Setup,
    leaf: &Leaf,
    uplink: &anyhow::Result<creds::Finding>,
    ants: &[Ant],
    human: Option<&Me>,
    generic: &str,
    now: u64,
) -> (Check, Option<Vec<(UserId, Role)>>) {
    let machine = &leaf.machine;
    let finding = match uplink {
        Err(e) => return (Check::fail("leaf uplink", format!("{e:#}"), vec![generic.to_owned()]), None),
        Ok(finding) => finding,
    };
    let (carried, humans) = match &finding.kind {
        Kind::Leaf { machine: m, ants: carried, humans, .. } if m == machine => (carried, humans),
        _ => {
            let detail = format!(
                "{} are the credentials of {}, not the uplink of {machine}",
                leaf.uplink.display(),
                finding.holder()
            );
            return (Check::fail("leaf uplink", detail, vec![generic.to_owned()]), None);
        }
    };
    // The ants and the human connecting through the leaf node, the daemon's default server.
    let through = |url: Option<&str>| same_address(url.unwrap_or(leaf::DEFAULT_LISTEN), &leaf.listen);
    let missing: Vec<&UserId> = ants
        .iter()
        .filter(|ant| through(ant.nats.as_deref()))
        .map(|ant| &ant.name)
        .chain(human.filter(|_| through(Some(&setup.nats))).map(|me| &me.id))
        .filter(|user| !carried.contains(user) && !humans.contains(user))
        .collect();
    let members = Some(auth::members(carried, humans));
    if missing.is_empty() {
        return (check_creds("leaf uplink", &leaf.uplink, finding, now), members);
    }
    let mut renewed = finding.clone();
    if let Kind::Leaf { ants, humans, .. } = &mut renewed.kind {
        for user in &missing {
            if human.is_some_and(|me| &me.id == *user) {
                humans.push((*user).clone());
            } else {
                ants.push((*user).clone());
            }
        }
    }
    let names: Vec<&str> = missing.iter().map(|u| u.as_str()).collect();
    let detail = format!(
        "{} carry no rights for {} of this machine: what they send does not cross the uplink",
        leaf.uplink.display(),
        names.join(", ")
    );
    (Check::fail("leaf uplink", detail, renewed.renew()), members)
}

/// Whether the leaf node listens, and where the CLI connects.
async fn check_listening(setup: &Setup, leaf: &Leaf, managed: Option<&Path>) -> Check {
    let listening = tokio::time::timeout(NETWORK_TIMEOUT, tokio::net::TcpStream::connect(leaf.listen.as_str())).await;
    match listening {
        Ok(Ok(_)) if same_address(&setup.nats, &leaf.listen) => {
            Check::ok("leaf node", format!("listening on {}, where the CLI connects", leaf.listen))
        }
        Ok(Ok(_)) => Check::warn(
            "leaf node",
            format!("listening on {}, but the CLI connects to {}", leaf.listen, setup.nats),
            vec![format!("nats = \"nats://{}\" in [cli] of {}", leaf.listen, setup.config.display())],
        ),
        failed => {
            let error = match failed {
                Ok(Err(e)) => e.to_string(),
                _ => format!("no answer within {}s", NETWORK_TIMEOUT.as_secs()),
            };
            let start = start_server(setup, &leaf.path, managed);
            Check::fail("leaf node", format!("nothing listens on {}: {error}", leaf.listen), vec![start])
        }
    }
}

/// Whether the hub sources the outbox of `machine` and takes from it what `members` send ([`outbox::sourcing`]).
pub(super) async fn check_outbox(
    machine: &Machine,
    members: Option<&[(UserId, Role)]>,
    uplink: Option<&creds::Finding>,
    connected: Option<&Connected>,
) -> Check {
    let check = "outbox";
    let Some(connected) = connected else { return Check::skip(check, "the hub is not reached") };
    let Some(members) = members.filter(|m| !m.is_empty()) else {
        return Check::skip(check, "the uplink credentials name no ants or humans of the machine");
    };
    let rights = &connected.rights;
    let infos = [format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"), format!("$JS.API.STREAM.INFO.{STREAM}")];
    if !infos.iter().all(|s| rights.may_publish(s)) {
        return Check::skip(check, "only a human's credentials get the stream info telling it");
    }
    let admit = uplink
        .and_then(|f| f.renew().into_iter().nth(1))
        .unwrap_or_else(|| format!("ants leaf outbox {machine} --ant ANT... --human HUMAN..."));
    let sourcing = tokio::time::timeout(NETWORK_TIMEOUT * 2, outbox::sourcing(&connected.js, machine, members)).await;
    match sourcing {
        Err(_) => Check::warn(check, "no stream info within 10s", vec![admit]),
        Ok(Err(e)) => Check::warn(check, format!("{e:#}"), vec![admit]),
        Ok(Ok(sourcing)) => match (sourcing.hearing, sourcing.unadmitted.as_slice()) {
            (outbox::Hearing::Failed(e), _) => {
                Check::warn(check, format!("the hub does not source the outbox of {machine}: {e}"), vec![admit])
            }
            (_, [_, ..]) => {
                let names: Vec<&str> = sourcing.unadmitted.iter().map(UserId::as_str).collect();
                let detail = format!(
                    "the hub's stream takes nothing from the outbox of {machine} for {}: what they send while the \
                     uplink is down stays there",
                    names.join(", ")
                );
                Check::warn(check, detail, vec![admit])
            }
            (outbox::Hearing::Silent, []) => Check::warn(
                check,
                format!(
                    "the hub sources the outbox of {machine} but has not heard from it since its nats-server started: \
                     the leaf node is not connected, or the hub lacks the feature flag below"
                ),
                vec![
                    admit,
                    "feature_flags { js_api_reply_v2: true } in the hub's nats-server configuration, then restart it"
                        .to_owned(),
                ],
            ),
            (outbox::Hearing::Stale(ago), []) => Check::warn(
                check,
                format!(
                    "the hub last heard from the outbox of {machine} {}s ago: the uplink is down; the hub goes on \
                     within seconds of the leaf node's return",
                    ago.as_secs()
                ),
                vec![admit],
            ),
            (outbox::Hearing::Heard, []) => {
                let names: Vec<&str> = members.iter().map(|(user, _)| user.as_str()).collect();
                Check::ok(check, format!("the hub takes from the outbox of {machine} for {}", names.join(", ")))
            }
        },
    }
}
