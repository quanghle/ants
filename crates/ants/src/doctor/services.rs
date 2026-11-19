//! The services installed on the machine, and the webhooks' config file with its notification sinks.

use std::path::Path;

use hyper::StatusCode;

use super::{Check, NETWORK_TIMEOUT, Setup, Status, same_file};
use crate::notify;
use crate::service::{Service, Services, State};
use crate::webhook::{self, SecretSource};

/// The daemon service of the config file, the nats-server service of the leaf node configuration, and that of the
/// local hub's configuration when there is no leaf node one, as scripts/install-units.sh installs them. A nats-server
/// configuration `managed`, the `[nats_server]` config of the config file, is run by the daemon instead: its check
/// follows the daemon service, and an ants-nats service running it too fails it.
pub(super) fn check_services(setup: &Setup, services: &mut Services, managed: Option<&Path>) -> Vec<Check> {
    let daemon = ("daemon service", Service::Daemon, setup.config.as_path(), "--config");
    let nats = ("leaf node service", Service::Nats, setup.leaf_config.as_path(), "--nats-conf");
    // `ants setup` runs a local hub only on a machine with no leaf node, from the same unit.
    let hub = ("hub service", Service::Nats, setup.hub_config.as_path(), "--nats-conf");
    let hub = (setup.hub_config.exists() && !setup.leaf_config.exists()).then_some(hub);
    let other = managed
        .filter(|m| !same_file(m, &setup.leaf_config) && !same_file(m, &setup.hub_config))
        .map(|m| ("nats-server service", Service::Nats, m, "--nats-conf"));
    // Only a machine with the webhooks' config file serves them.
    let webhook = ("webhook service", Service::Webhook, setup.webhooks.as_path(), "--webhook --webhook-config");
    let webhook = setup.webhooks.exists().then_some(webhook);
    [Some(daemon), Some(nats), hub, other, webhook]
        .into_iter()
        .flatten()
        .map(|(check, service, file, flag)| {
            if service == Service::Nats && managed.is_some_and(|m| same_file(m, file)) {
                return check_managed(setup, services, check, file);
            }
            let install = format!("scripts/install-units.sh {flag} {}", file.display());
            let file_shown = file.display();
            if !file.exists() {
                return Check::skip(check, format!("no {file_shown}"));
            }
            match services.state(service, file) {
                State::Unsupported => Check::skip(check, "no service manager ants installs units for here"),
                State::NotInstalled => Check::warn(check, format!("none installed to run {file_shown}"), vec![install]),
                State::Other(unit) => {
                    Check::warn(check, format!("{} runs another file than {file_shown}", unit.display()), vec![install])
                }
                State::Stopped(start) => {
                    Check::fail(check, format!("installed for {file_shown}, not running"), vec![start])
                }
                State::Running => Check::ok(check, format!("running {file_shown}")),
                State::Unknown(command, e) => Check::warn(check, format!("{command}: {e}"), vec![install]),
            }
        })
        .collect()
}

/// The check `check` of the nats-server running `conf` that `ants daemon` runs from the config file's `[nats_server]`:
/// as the daemon service runs, failed when an ants-nats service runs `conf` too.
fn check_managed(setup: &Setup, services: &mut Services, check: &str, conf: &Path) -> Check {
    let config = setup.config.display();
    let shown = conf.display();
    let table = format!("[nats_server] of {config}");
    if !conf.exists() {
        let fix = format!("write {shown} (`ants leaf config`), or correct config in {table}");
        return Check::fail(check, format!("no {shown}, the configuration {table} names"), vec![fix]);
    }
    if let State::Running | State::Stopped(_) = services.state(Service::Nats, conf) {
        let mut fix: Vec<String> = services.removal(Service::Nats).into_iter().collect();
        fix.push(format!("or remove {table}"));
        let detail =
            format!("two owners of {shown}: an ants-nats service runs it, and `ants daemon` too, from {table}");
        return Check::fail(check, detail, fix);
    }
    let install = format!("scripts/install-units.sh --managed-nats --config {config}");
    let managed = format!("{shown} is run by `ants daemon` ({table})");
    match services.state(Service::Daemon, &setup.config) {
        State::Running => Check::ok(check, format!("{managed}, whose service runs")),
        State::Stopped(start) => Check::fail(check, format!("{managed}, whose service is not running"), vec![start]),
        State::NotInstalled => Check::warn(check, format!("{managed}; no daemon service installed"), vec![install]),
        State::Other(unit) => Check::warn(
            check,
            format!("{managed}; {} runs another config file than {config}", unit.display()),
            vec![install],
        ),
        State::Unsupported => Check::skip(check, format!("{managed}; no service manager ants installs units for here")),
        State::Unknown(command, e) => Check::warn(check, format!("{managed}; {command}: {e}"), vec![install]),
    }
}

/// The command starting the nats-server running `conf`, for a check finding it down: the daemon's, when it runs `conf`
/// from `[nats_server]` (the config file's `managed`), else the nats-server service's.
pub(super) fn start_server(setup: &Setup, conf: &Path, managed: Option<&Path>) -> String {
    let config = setup.config.display();
    if managed.is_some_and(|m| same_file(m, conf)) {
        return match Services::local().state(Service::Daemon, &setup.config) {
            State::Stopped(start) => start,
            State::Running => format!(
                "the log of `ants daemon` tells why the nats-server of [nats_server] in {config} is down \
                 (~/Library/Logs/ants/daemon.log on macOS, journalctl --user -u ants-daemon on Linux)"
            ),
            _ => format!(
                "ants daemon --config {config} starts it ([nats_server]), or scripts/install-units.sh --managed-nats \
                 to run the daemon as a service"
            ),
        };
    }
    match Services::local().state(Service::Nats, conf) {
        State::Stopped(start) => start,
        _ => format!("nats-server -c {}, or scripts/install-units.sh to run it as a service", conf.display()),
    }
}

/// The webhooks' config file at `path`, when it is there: its secret files, present and private to their owner, the
/// file itself as `ants webhook serve` reads it, the programs of its exec sinks, and whether `public_url` reaches it.
pub(super) async fn check_webhooks(path: &Path) -> Vec<Check> {
    if !path.exists() {
        return Vec::new();
    }
    let shown = path.display();
    let refused = || format!("correct {shown}; `ants webhook serve --config {shown}` refuses it as it is");
    let outline = match webhook::outline(path) {
        Ok(outline) => outline,
        Err(e) => return vec![Check::fail("webhooks", format!("{e:#}"), vec![refused()])],
    };
    let mut checks = Vec::new();
    let mut unset = Vec::new();
    for (what, source) in &outline.secrets {
        match source {
            SecretSource::File(file) => {
                let fix = format!("write the secret into {} (mode 0600)", file.display());
                match private_file(file, fix) {
                    Ok(_) => {}
                    Err(problem) => checks.push(problem.named("webhook secrets", what, Status::Fail)),
                }
            }
            SecretSource::Env(var) if std::env::var_os(var).is_none() => unset.push((what, var)),
            SecretSource::Env(_) => {}
        }
    }
    let files = outline.secrets.iter().filter(|(_, s)| matches!(s, SecretSource::File(_))).count();
    if checks.is_empty() && files > 0 {
        checks.push(Check::ok("webhook secrets", format!("{files} secret files of {shown}, each with mode 0600")));
    }
    for (what, var) in &unset {
        let detail = format!("{what}: {var} is not set here; only the environment of `ants webhook serve` may hold it");
        checks.push(Check::warn("webhook secrets", detail, vec![format!("export {var}, or name a secret_file")]));
    }
    if checks.iter().any(|c| c.status != Status::Ok) {
        checks.push(Check::skip("webhooks", format!("{shown} not read: its secrets are not all readable here")));
    } else {
        checks.push(match webhook::Config::read(path) {
            Ok(config) => {
                let humans: Vec<&str> = config.humans.iter().map(|h| h.name.as_str()).collect();
                let hooks = config.webhooks.len();
                match humans.as_slice() {
                    [] => Check::ok("webhooks", format!("{shown} lists {hooks} webhooks and no human to notify")),
                    _ => Check::ok(
                        "webhooks",
                        format!(
                            "{shown} lists {hooks} webhooks and notifies {}; `ants webhook notify-test HUMAN` tries \
                             each sink",
                            humans.join(", ")
                        ),
                    ),
                }
            }
            Err(e) => Check::fail("webhooks", format!("{e:#}"), vec![refused()]),
        });
    }
    checks.extend(outline.commands.iter().map(|(what, program)| check_command(what, program)));
    if let Some(public_url) = &outline.public_url {
        checks.push(check_public_url(public_url).await);
    }
    checks
}

/// Whether the program of an exec sink is there to run: an executable file at its path, or one on `PATH` here.
pub(super) fn check_command(what: &str, program: &str) -> Check {
    let check = "notify commands";
    let executable = |path: &Path| {
        let Ok(metadata) = std::fs::metadata(path) else { return false };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        metadata.is_file()
    };
    if program.contains(std::path::is_separator) {
        return if executable(Path::new(program)) {
            Check::ok(check, format!("{what}: {program} is executable"))
        } else {
            Check::fail(
                check,
                format!("{what}: {program} is no executable file"),
                vec![format!("put the program at {program} and chmod +x it, or correct command")],
            )
        };
    }
    let path = std::env::var_os("PATH").unwrap_or_default();
    match std::env::split_paths(&path).map(|dir| dir.join(program)).find(|p| executable(p)) {
        Some(found) => Check::ok(check, format!("{what}: {program} is {} here", found.display())),
        None => Check::warn(
            check,
            format!("{what}: {program} is not on PATH here"),
            vec![format!("give the full path of {program} in command")],
        ),
    }
}

/// Whether `public_url`, the approval links' address, reaches `ants webhook serve`: its page for a link without a
/// query.
pub(super) async fn check_public_url(public_url: &str) -> Check {
    let check = "public_url";
    let url = format!("{}{}", public_url.trim_end_matches('/'), notify::APPROVAL_PATH);
    let fix = || {
        vec![format!("route {public_url} to `ants webhook serve` (its listen address and path), or correct public_url")]
    };
    match notify::probe_links(public_url, NETWORK_TIMEOUT).await {
        Ok((StatusCode::BAD_REQUEST, page)) if page.contains("The link is incomplete.") => {
            Check::ok(check, format!("{url} answers as `ants webhook serve` does"))
        }
        Ok((status, _)) => {
            Check::warn(check, format!("{url} answered {status}, not as `ants webhook serve` does"), fix())
        }
        Err(e) => Check::warn(check, format!("{url}: {e:#}; approval links from notices may not open"), fix()),
    }
}

/// Why a secret file does not do: its check's detail and fix, and how bad it is.
pub(super) struct Problem {
    detail: String,
    fix: String,
    /// The file is there, readable by others.
    exposed: bool,
}

impl Problem {
    /// The check `check` of the file holding `what`: `exposed` warns or fails as `status` says.
    pub(super) fn named(self, check: &str, what: &str, exposed: Status) -> Check {
        let status = if self.exposed { exposed } else { Status::Fail };
        Check::new(check, status, format!("{what}: {}", self.detail), vec![self.fix])
    }
}

/// The modification time of `file`, a regular file only its owner may read; else what to do about it, `write`
/// when it is missing.
pub(super) fn private_file(file: &Path, write: String) -> Result<std::fs::Metadata, Problem> {
    let shown = file.display();
    let metadata = match std::fs::metadata(file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => return Err(Problem { detail: format!("{shown} is not a file"), fix: write, exposed: false }),
        Err(e) => return Err(Problem { detail: format!("{shown}: {e}"), fix: write, exposed: false }),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            let detail = format!("{shown} has mode {mode:04o}, readable by others");
            return Err(Problem { detail, fix: format!("chmod 600 {shown}"), exposed: true });
        }
    }
    Ok(metadata)
}
