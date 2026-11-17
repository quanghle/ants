//! The machine's ants: their credentials and harnesses, the host settings, shared workdirs, reactions responders and
//! which webhooks bypass their approvals.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Ant};
use ants_daemon::harness::{self, Harnesses, Readiness};
use ants_daemon::records::reactions;
use ants_daemon::secrets::creds::{self, Kind};
use ants_daemon::secrets::keystore;
use ants_daemon::{Launch, PermissionPolicy};
use ants_proto::{MACHINES_BUCKET, Machine, ReactionChange, ReactionsRequest, ReactionsResponder, Uuid};

use super::credentials::{check_creds, check_key};
use super::server::{Unreached, connect};
use super::{Check, Setup};
use crate::webhook;

/// Each ant's credentials, and, unless told not to, whether its harness is installed and signed in: one handshake per
/// harness and environment, all at once.
pub(super) async fn check_ants(ants: &[Ant], harnesses: &Harnesses, setup: &Setup, now: u64) -> Vec<Check> {
    let config = setup.config.display();
    // The harness's name, the ant's environment, the harness and its launch.
    type Started = (String, Vec<(String, String)>, std::sync::Arc<ants_daemon::Harness>, Launch);
    let mut launches: Vec<Started> = Vec::new();
    let mut picked = Vec::with_capacity(ants.len());
    for ant in ants {
        let name = ant.harness.as_deref().unwrap_or_default();
        let env: Vec<(String, String)> = ant.env.clone().into_iter().collect();
        let index = match launches.iter().position(|(n, e, ..)| n == name && *e == env) {
            Some(index) => Some(index),
            None => harnesses.get(name).ok().map(|harness| {
                let cwd = Some(ant.workdir.clone())
                    .filter(|dir| dir.is_dir())
                    .or_else(|| std::env::current_dir().ok())
                    .unwrap_or_else(|| PathBuf::from("/"));
                let launch =
                    Launch::new(harness.clone(), cwd, env.clone(), PermissionPolicy::reject_all(), Default::default());
                launches.push((name.to_owned(), env, harness, launch));
                launches.len() - 1
            }),
        };
        picked.push(index);
    }
    let handshakes = if setup.harnesses {
        futures::future::join_all(launches.iter().map(|(.., launch)| harness::handshake(launch))).await
    } else {
        Vec::new()
    };
    let mut checks = Vec::new();
    for (ant, index) in ants.iter().zip(picked) {
        checks.push(check_ant_creds(ant, now));
        if let Some(path) = &ant.creds {
            checks.extend(check_key(&format!("ant {} key", ant.name), path, vec![issue(ant)], keystore::os()));
        }
        let check = format!("ant {} harness", ant.name);
        let (handshake, installed) = match index {
            _ if !setup.harnesses => {
                checks.push(Check::skip(check, "--no-harness"));
                continue;
            }
            Some(index) => (&handshakes[index], &launches[index].2),
            None => {
                checks.push(Check::fail(check, "unknown harness", vec![format!("correct {config}")]));
                continue;
            }
        };
        let harness = &handshake.harness;
        let recheck = format!("ants harness check --harness {harness} --config {config}");
        checks.push(match &handshake.readiness {
            Readiness::Ready { agent } => Check::ok(check, format!("{harness}: {agent}, signed in")),
            Readiness::Missing => {
                let command = installed.command().display();
                let detail = format!("{harness}: {command} is not installed (not found on PATH)");
                let file = installed.file().map_or_else(|| config.to_string(), |file| file.display().to_string());
                let fix = format!("install {command}, or set the `command` of [harnesses.{harness}] in {file}");
                Check::fail(check, detail, vec![fix])
            }
            Readiness::SignedOut { detail } => {
                let sign_in = handshake.sign_in.clone().unwrap_or_else(|| format!("sign {harness} in"));
                Check::fail(check, format!("{harness} is signed out: {detail}"), vec![sign_in, recheck])
            }
            Readiness::Failed { detail } => {
                Check::fail(check, format!("{harness} ACP handshake: {detail}"), vec![recheck])
            }
        });
    }
    checks
}

/// The commands issuing the credentials of `ant` again.
pub(super) fn issue(ant: &Ant) -> String {
    let name = &ant.name;
    let domain = ant.js_domain.as_ref().map_or_default(|d| format!(" --js-domain {d}"));
    format!("ants auth user {name}{domain} on the machine holding the team keys, or ants add {name}")
}

/// An ant's credentials: an ant's, or an assistant's, of its name, issued for the JetStream domain it uses.
pub(super) fn check_ant_creds(ant: &Ant, now: u64) -> Check {
    let check = format!("ant {} credentials", ant.name);
    let name = &ant.name;
    let issue = issue(ant);
    let Some(path) = &ant.creds else {
        return Check::skip(check, "none in the config file: the ant connects as no one");
    };
    let finding = match creds::check(path) {
        Ok(finding) => finding,
        Err(e) => return Check::fail(check, format!("{e:#}"), vec![issue]),
    };
    if !matches!(finding.kind, Kind::Ant | Kind::Assistant { .. }) || finding.name != name.as_str() {
        let detail = format!("{} are the credentials of {}, not of ant {name}", path.display(), finding.holder());
        return Check::fail(check, detail, vec![issue]);
    }
    if let Some(used) = &ant.js_domain
        && finding.js_domain.as_ref() != Some(used)
    {
        let detail = format!("{} were not issued for JetStream domain {used}, which the ant uses", path.display());
        return Check::fail(check, detail, vec![issue]);
    }
    check_creds(&check, path, &finding, now)
}

/// The machine's host credentials ([`ants_daemon::host`]): a warning when the ants have credentials and the host
/// none, which keeps the machine's record of its ants and its admin orders off; the `[host]` table's `machine`, else
/// the top-level one if it is a machine name, else `hostname`'s first label names the machine then.
pub(super) fn check_host(host: &config::HostSettings, ants: &[Ant], hostname: &str, now: u64) -> Check {
    let check = "host credentials";
    let machine = host.machine_or(hostname);
    let issue = || {
        vec![format!(
            "ants auth host {machine} on the machine holding the team keys, then copy them to \
             ~/.config/ants/auth/host.creds here, or name them in `creds` of [host]"
        )]
    };
    let Some(path) = &host.creds else {
        if ants.iter().all(|ant| ant.creds.is_none()) {
            return Check::skip(check, "none, as no ant has any: the daemon records the machine's ants as no one");
        }
        let detail = format!(
            "none: the daemon does not record the machine's ants in {MACHINES_BUCKET}, nor take their admin orders"
        );
        return Check::warn(check, detail, issue());
    };
    let finding = match creds::check(path) {
        Ok(finding) => finding,
        Err(e) => return Check::fail(check, format!("{e:#}"), issue()),
    };
    match ants_daemon::host::machine_of(host.machine.as_ref(), Some(path), machine.clone()) {
        Ok(_) => check_creds(check, path, &finding, now),
        Err(e) => Check::fail(check, format!("{e:#}"), issue()),
    }
}

/// A warning when the config file's top-level `machine`, which names the machine in its ants' colony entries, is no
/// machine name ([`Machine`]) and the `[host]` table names none, so that host credentials, the machine's record and
/// admin orders name the machine as its credentials do, else as `hostname`'s first label.
pub(super) fn check_machine_name(host: &config::HostSettings, hostname: &str) -> Option<Check> {
    let top = host.top_machine.as_deref().filter(|_| host.machine.is_none() && host.fallback().is_none())?;
    let detail = format!(
        "machine {top:?} names the ants' colony entries, but holds characters a machine name cannot, so host \
         credentials, {MACHINES_BUCKET} and admin orders name the machine {} unless its host credentials name \
         another",
        Machine::from_hostname(hostname)
    );
    let fix = format!(
        "machine = \"{}\" at the top of the config file (letters, digits, '-' and '_', starting with a letter or \
         digit), or under [host] to keep the colony's name",
        Machine::from_hostname(&top.replace('.', "-"))
    );
    Some(Check::warn("machine name", detail, vec![fix]))
}

/// A warning for each ant whose threads run at once in one git work tree, worktrees turned off, where they may
/// clobber each other's edits.
pub(super) fn check_shared_workdirs(ants: &[Ant], config: &Path) -> Vec<Check> {
    ants.iter()
        .filter(|ant| ants_daemon::worktrees::shares_git_workdir(&ant.workdir, ant.max_harnesses, ant.worktrees))
        .map(|ant| {
            let detail = format!(
                "up to {} threads at once edit the git work tree of {}",
                ant.max_harnesses,
                ant.workdir.display()
            );
            let fix = format!(
                "worktrees = true in the [[ant]] named {} of {}, giving each thread its own git worktree",
                ant.name,
                config.display()
            );
            Check::warn(format!("ant {} workdir", ant.name), detail, vec![fix])
        })
        .collect()
}

/// Whether something answers each ant's reactions requests, which its MCP list_reactions and react make, and which
/// kind of process does: asked as the ant, where its daemon connects, about the nil id, which no ant reads, so the
/// answers are unknown and nothing changes. An ant without credentials, on an open server, asks no one: its daemon
/// answers them itself.
pub(super) async fn check_reactions(ants: &[Ant], setup: &Setup, wait: Duration) -> Vec<Check> {
    futures::future::join_all(ants.iter().map(|ant| check_ant_reactions(ant, setup, wait))).await
}

pub(super) async fn check_ant_reactions(ant: &Ant, setup: &Setup, wait: Duration) -> Check {
    let check = format!("ant {} reactions", ant.name);
    let Some(creds) = &ant.creds else {
        return Check::ok(
            check,
            "no credentials, so an open server: the daemon answers list_reactions and react itself",
        );
    };
    let url = ant.nats.as_deref().unwrap_or(&setup.nats);
    let client = match connect(url, Some(creds), &ant.name).await {
        Ok(client) => client,
        Err(Unreached::Refused(e) | Unreached::Unreachable(e) | Unreached::NoKey(e) | Unreached::Other(e)) => {
            return Check::skip(check, format!("not connected to {url} as {}: {e}", ant.name));
        }
    };
    let ask = async |request: ReactionsRequest| reactions::request_within(&client, &request, wait).await;
    let remove = ReactionChange::new("eyes".parse().expect("a reaction"), true);
    let (listed, changed) = tokio::join!(
        ask(ReactionsRequest::new(ant.name.clone(), Uuid::nil())),
        ask(ReactionsRequest::change(ant.name.clone(), Uuid::nil(), remove)),
    );
    let (listed, changed) = match (listed, changed) {
        (Ok(listed), Ok(changed)) => (listed, changed),
        (Err(e), _) | (_, Err(e)) => return Check::skip(check, format!("asking as {}: {e:#}", ant.name)),
    };
    let within = format!("within {}s", wait.as_secs());
    let signer = "ants signer on the machine holding the account key (scripts/install-units.sh --signer runs it as a \
                  service)"
        .to_owned();
    match (listed, changed) {
        (listed, Some(changed)) => {
            let mut by = vec![responder(changed.responder)];
            by.extend(listed.map(|l| responder(l.responder)).filter(|l| !by.contains(l)));
            Check::ok(check, format!("{} answers list_reactions and react", by.join(" and ")))
        }
        (Some(listed), None) => {
            let detail = format!(
                "{} answers list_reactions, but nothing answers react {within}: the ant's react fails",
                responder(listed.responder)
            );
            Check::warn(check, detail, vec![signer])
        }
        (None, None) => {
            let detail = format!("nothing answers its reactions requests {within}: list_reactions and react fail");
            let webhook = "or, for list_reactions only, ants webhook serve with a [[human]] entry".to_owned();
            Check::warn(check, detail, vec![signer, webhook])
        }
    }
}

/// The kind of process that answered a reactions request, as a user-facing name.
pub(super) fn responder(responder: ReactionsResponder) -> &'static str {
    match responder {
        ReactionsResponder::Signer => "ants signer",
        ReactionsResponder::Webhook => "ants webhook serve",
    }
}

/// A warning for each ant whose harness runs its tools unasked while the ant gates them: with allow rules or tool
/// approvals by its `escalate_to` ants, or as the destination of schedules or webhooks narrowing the threads they
/// start.
pub(super) fn check_bypassed(loaded: &config::Loaded, webhooks: &[webhook::Webhook]) -> Vec<Check> {
    let mut checks = Vec::new();
    for ant in &loaded.ants {
        let Some(harness) = ant.harness.as_deref().and_then(|name| loaded.harnesses.get(name).ok()) else { continue };
        if !harness.bypasses_permissions() {
            continue;
        }
        let check = format!("ant {} tool permissions", ant.name);
        let fix = vec![harness.gated_fix()];
        let allow = if ant.allow_tools { vec!["*".to_owned()] } else { ant.allow.clone() };
        if let Some(detail) = harness.unenforced(&allow, !ant.escalate_to.is_empty()) {
            checks.push(Check::warn(check.clone(), detail, fix.clone()));
        }
        let to_ant = |destination: &ants_proto::Destination| matches!(destination, ants_proto::Destination::Ant(to) if *to == ant.name);
        let narrowing: Vec<String> = loaded
            .schedules
            .iter()
            .filter(|s| to_ant(&s.destination) && s.allow.as_deref().is_none_or(|a| !harness::allows_all(a)))
            .map(|s| format!("schedule {}", s.name))
            .chain(
                webhooks
                    .iter()
                    .filter(|w| to_ant(&w.destination) && w.narrows())
                    .map(|w| format!("webhook {}", w.name)),
            )
            .collect();
        if !narrowing.is_empty() {
            let detail = format!(
                "harness {harness} runs its tools unasked, so the tool rules {} narrow its threads to do not apply",
                narrowing.join(", ")
            );
            checks.push(Check::warn(check, detail, fix));
        }
    }
    checks
}
