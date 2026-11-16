//! `ants setup`: bring up ants on a single machine, step by step, each step left alone when already done: the
//! harness of a new config file ([`pick`]), a local nats-server with JetStream and its service, the hub's stream and
//! buckets (`ants init`), the config file with the messenger and optionally one ant, the harness check, the machine's
//! host credentials, the daemon service, the shell completions, and last `ants doctor`. It never overwrites a file: one already there is reported
//! and kept.

mod pick;

use std::fmt::Write as _;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Ant, Summary};
use ants_daemon::harness;
use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};

use crate::cmd::harness::HarnessPick;
use crate::completions::{self, Shell};
use crate::service::{self, Action, Installed, Outcome, Service, Services, State, Values};
use crate::{Connection, Nats, auth, doctor};

/// How long to wait for a nats-server just started to listen.
const LISTEN_WAIT: Duration = Duration::from_secs(10);

#[derive(clap::Args, Debug)]
pub struct SetupArgs {
    /// Also list this ant in the config file (lowercase letters, digits and '-'), working in --workdir.
    #[arg(long, value_name = "NAME", requires = "workdir")]
    pub ant: Option<UserId>,
    /// The working directory of --ant.
    #[arg(long, value_name = "DIR", requires = "ant")]
    pub workdir: Option<PathBuf>,
    /// The harness of a new config file, its top-level `harness`: built in (`copilot`, `claude`, `codex`,
    /// `antigravity`) or defined under `[harnesses.NAME]` (default: the only built-in one installed, else asked for
    /// on a terminal).
    #[arg(long)]
    pub harness: Option<String>,
    /// Config file of the machine's ants (default: ~/.ants/config.toml).
    #[arg(long, env = "ANTS_CONFIG")]
    pub config: Option<PathBuf>,
    /// nats-server binary for its service (default: nats-server on PATH).
    #[arg(long, value_name = "PATH")]
    pub nats_server: Option<PathBuf>,
    /// Install and start no services; print how to start nats-server instead.
    #[arg(long)]
    pub no_services: bool,
    /// Skip starting the harness for an ACP handshake.
    #[arg(long)]
    pub no_harness: bool,
    /// Install no shell completions.
    #[arg(long)]
    pub no_completions: bool,
    /// Only say what each step would do; write, start and check nothing.
    #[arg(long)]
    pub dry_run: bool,
}

/// What came of a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mark {
    Done,
    /// Already done before.
    Exists,
    Skip,
    Fail,
}

/// The steps' outcomes, printed as they come.
struct Report {
    dry_run: bool,
    failed: usize,
}

impl Report {
    fn step(&mut self, mark: Mark, step: &str, detail: impl std::fmt::Display) {
        let mark = match mark {
            Mark::Done if self.dry_run => "would",
            Mark::Done => "done",
            Mark::Exists => "exists",
            Mark::Skip => "skip",
            Mark::Fail => {
                self.failed += 1;
                "fail"
            }
        };
        println!("{mark:<6} {step}: {detail}");
    }
}

/// Run the steps of `ants setup`, then `ants doctor`; fail when a step or a check fails.
pub async fn run(args: &SetupArgs, connection: &Connection<'_>) -> Result<()> {
    let home = std::env::home_dir().context("no home directory")?;
    let config_path = match &args.config {
        Some(path) => std::path::absolute(path)?,
        None => config::default_path()?,
    };
    let leaf_conf = auth::default_dir()?.with_file_name("leaf.conf");
    let nats_conf = auth::default_dir()?.with_file_name("nats.conf");
    let mut report = Report { dry_run: args.dry_run, failed: 0 };
    let harness = pick_harness(&mut report, args, &config_path)?;
    let mut services = Services::local();
    let values = || -> Result<Values> {
        let nats_server = match &args.nats_server {
            Some(path) => std::path::absolute(path)?,
            None => on_path("nats-server").unwrap_or_default(),
        };
        let path = std::env::var("PATH").unwrap_or_default();
        Ok(Values { ants: std::env::current_exe()?, nats_server, path })
    };

    let listen = local_listen(connection.nats);
    let mut started = false;
    match &listen {
        None => report.step(
            Mark::Skip,
            "nats-server",
            format!("{} is not on this machine: no local nats-server set up", connection.nats),
        ),
        Some(_) if leaf_conf.exists() => report.step(
            Mark::Skip,
            "nats-server",
            format!("the leaf node of {} is the machine's nats-server", leaf_conf.display()),
        ),
        Some(listen) => {
            nats_config(&mut report, &nats_conf, listen, &home);
            started = nats_service(&mut report, args, &mut services, &nats_conf, listen, values).await;
        }
    }
    if started && !args.dry_run {
        wait_listening(listen.as_deref().unwrap_or_default()).await;
    }

    init(&mut report, args, connection, listen.as_deref()).await;
    let appended = config_file(&mut report, args, harness.as_deref(), connection, &config_path);
    harness_check(&mut report, args, harness, &config_path).await;
    host_creds(&mut report, args, connection, &config_path).await;
    daemon_service(&mut report, args, &mut services, &config_path, appended, values);
    shell_completions(&mut report, args, &home);

    if args.dry_run {
        println!("would run ants doctor");
        return Ok(());
    }
    println!("ants doctor:");
    let webhooks = config_path.with_file_name("webhooks.toml");
    let setup = doctor::Setup {
        nats: connection.nats.to_owned(),
        creds: connection.creds.map(Path::to_owned),
        js_domain: connection.js_domain.cloned(),
        config: config_path,
        leaf_config: leaf_conf,
        hub_config: nats_conf,
        harnesses: false,
        team: auth::default_dir()?,
        webhooks,
        home: Some(home.clone()),
    };
    let checks = Box::pin(doctor::run(&setup)).await;
    for check in &checks {
        println!("{check}");
    }
    let doctor_failed = checks.iter().filter(|c| c.status == doctor::Status::Fail).count();
    match (report.failed, doctor_failed) {
        (0, 0) => Ok(()),
        (0, n) => bail!("{n} of {} checks failed", checks.len()),
        (n, _) => bail!("{n} setup steps failed"),
    }
}

/// The harness of a new config file at `config`: --harness, else the only built-in one installed, else one asked for
/// on a terminal; None for a file there. Fails, listing the harnesses, when none can be picked.
fn pick_harness(report: &mut Report, args: &SetupArgs, config: &Path) -> Result<Option<String>> {
    let step = "harness choice";
    if args.harness.is_some() || config.exists() {
        return Ok(args.harness.clone());
    }
    let detected = pick::detect(&std::env::var_os("PATH").unwrap_or_default());
    let (mut input, mut output) = (std::io::stdin().lock(), std::io::stdout());
    let terminal = input.is_terminal();
    let prompt: Option<(&mut dyn std::io::BufRead, &mut dyn std::io::Write)> =
        terminal.then_some((&mut input, &mut output));
    match pick::choose(&detected, prompt) {
        Ok((name, how)) => {
            report.step(Mark::Done, step, how);
            Ok(Some(name))
        }
        Err(e) => {
            report.step(Mark::Fail, step, format!("{e:#}"));
            bail!("no harness for {}: install one or pass --harness NAME", config.display())
        }
    }
}

/// Write the nats-server configuration at `path`, unless there is one: listening on `listen`, with JetStream
/// keeping its data in ~/.ants/jetstream.
fn nats_config(report: &mut Report, path: &Path, listen: &str, home: &Path) {
    if path.exists() {
        return report.step(Mark::Exists, "nats-server config", format!("{} kept", path.display()));
    }
    let store = home.join(".ants/jetstream");
    let text = format!(
        "# The machine's local nats-server for ants, written by `ants setup`: JetStream on, listening on this machine \
         only.\nlisten: {}\njetstream {{\n  store_dir: {}\n}}\n",
        quoted(listen),
        quoted(&store.to_string_lossy())
    );
    let written = if report.dry_run { Ok(()) } else { write_new(path, &text) };
    match written {
        Ok(()) => report.step(
            Mark::Done,
            "nats-server config",
            format!("{} (JetStream in {})", path.display(), store.display()),
        ),
        Err(e) => report.step(Mark::Fail, "nats-server config", format!("{e:#}")),
    }
}

/// Install and start the service running nats-server with `conf`, unless a server listens on `listen` already.
/// Returns whether it started one.
async fn nats_service(
    report: &mut Report,
    args: &SetupArgs,
    services: &mut Services,
    conf: &Path,
    listen: &str,
    values: impl Fn() -> Result<Values>,
) -> bool {
    let step = "nats-server service";
    let up = listening(listen).await;
    if args.no_services {
        if up {
            report.step(Mark::Skip, step, format!("--no-services; a server listens on {listen}"));
        } else {
            report.step(Mark::Skip, step, format!("--no-services; start one: nats-server -c {}", conf.display()));
        }
        return false;
    }
    let state = services.state(Service::Nats, conf);
    if state == State::Running {
        report.step(Mark::Exists, step, format!("running {}", conf.display()));
        return false;
    }
    if up {
        report.step(
            Mark::Skip,
            step,
            format!("a server listens on {listen} already; none installed for {}", conf.display()),
        );
        return false;
    }
    let values = match values() {
        Ok(values) if values.nats_server.as_os_str().is_empty() => {
            report.step(
                Mark::Fail,
                step,
                "nats-server not found on PATH (install it: https://docs.nats.io/running-a-nats-service/introduction/installation, or give --nats-server)",
            );
            return false;
        }
        Ok(values) => values,
        Err(e) => {
            report.step(Mark::Fail, step, format!("{e:#}"));
            return false;
        }
    };
    installed(report, step, services.install(Service::Nats, conf, &values, args.dry_run), Service::Nats)
}

/// Report what [`Services::install`] did; returns whether it started the service.
fn installed(report: &mut Report, step: &str, installed: Installed, service: Service) -> bool {
    match installed {
        Installed::New(unit, command) => {
            report.step(Mark::Done, step, format!("installed {} and started it ({command})", unit.display()));
            true
        }
        Installed::Started(command) => {
            report.step(Mark::Done, step, format!("installed already, started: {command}"));
            true
        }
        Installed::Running => {
            report.step(Mark::Exists, step, "installed and running");
            false
        }
        Installed::Other(unit) => {
            let install = match service {
                Service::Daemon => "scripts/install-units.sh --config FILE",
                Service::Nats => "scripts/install-units.sh --nats-conf FILE",
                _ => "scripts/install-units.sh",
            };
            report.step(
                Mark::Skip,
                step,
                format!("{} runs another file; left alone (switch: {install})", unit.display()),
            );
            false
        }
        Installed::Unsupported => {
            report.step(Mark::Skip, step, "no service manager ants installs units for here");
            false
        }
        Installed::Failed(command, e) => {
            report.step(Mark::Fail, step, format!("`{command}` failed: {e}"));
            false
        }
    }
}

/// Create the hub's stream and buckets, as `ants init` does.
async fn init(report: &mut Report, args: &SetupArgs, connection: &Connection<'_>, listen: Option<&str>) {
    let step = "ants init";
    if args.dry_run {
        return report.step(Mark::Done, step, format!("stream and buckets on {}", connection.nats));
    }
    if let Some(listen) = listen
        && !listening(listen).await
    {
        return report.step(Mark::Skip, step, format!("no server listens on {listen}; run `ants init` once one does"));
    }
    let done = async {
        let nats = Nats::new(connection.nats, connection.creds, connection.js_domain)?;
        crate::init_hub(&nats).await
    };
    match done.await {
        Ok(lines) => report.step(Mark::Done, step, lines.join("\n       ")),
        Err(e) => report.step(Mark::Fail, step, format!("{e:#}")),
    }
}

/// Issue the machine's host credentials where the daemon finds them, `host-<machine>.creds` in the team directory,
/// when the config file's ants have credentials, the team's keys are here and the daemon finds none.
async fn host_creds(report: &mut Report, args: &SetupArgs, connection: &Connection<'_>, config: &Path) {
    let step = "host credentials";
    let summary = match Summary::load(config) {
        Ok(summary) => summary,
        Err(e) => return report.step(Mark::Skip, step, format!("{e:#}")),
    };
    if let Some(creds) = &summary.host.creds {
        return report.step(Mark::Exists, step, creds.display());
    }
    if summary.ants.iter().all(|(_, creds)| creds.is_none()) {
        return report.step(Mark::Skip, step, "the ants connect as no one, and so does the machine's host");
    }
    let hostname = ants_daemon::colony::machine().await;
    let machine = summary.host.machine_or(&hostname);
    let dir = match auth::default_dir() {
        Ok(dir) if auth::holds_team(&dir) => dir,
        _ => {
            let hint = format!(
                "run `ants auth host {machine}` on the machine holding the team keys and copy the file to \
                 ~/.config/ants/auth/host.creds here, for the daemon to record the machine's ants"
            );
            return report.step(Mark::Skip, step, hint);
        }
    };
    let out = dir.join(format!("{}.creds", ants_proto::host_user(&machine)));
    if args.dry_run {
        return report.step(Mark::Done, step, format!("issue {}", out.display()));
    }
    match auth::issue_host(&dir, &machine, connection.js_domain, &out) {
        Ok(()) => report.step(Mark::Done, step, out.display()),
        Err(e) => report.step(Mark::Fail, step, format!("{e:#}")),
    }
}

/// Write the config file, unless there is one: the messenger, `harness` and the server given, and the ant of --ant.
/// To a file there, only add the ant, when not listed; one naming no harness for its ants fails the step. Returns
/// whether it changed the file.
fn config_file(
    report: &mut Report,
    args: &SetupArgs,
    harness: Option<&str>,
    connection: &Connection<'_>,
    path: &Path,
) -> bool {
    let step = "config";
    let shown = path.display();
    let summary = if path.exists() {
        match Summary::load(path) {
            Ok(summary) => Some(summary),
            Err(e) => {
                report.step(Mark::Fail, step, format!("{e:#}"));
                return false;
            }
        }
    } else {
        None
    };
    let entry = match (&args.ant, &args.workdir) {
        (Some(name), Some(workdir)) => match ant_entry(name, workdir, connection.nats, summary.as_ref(), harness) {
            Ok(entry) => Some((name, entry)),
            Err(e) => {
                report.step(Mark::Fail, step, format!("ant {name}: {e:#}"));
                return false;
            }
        },
        _ => None,
    };
    let Some(summary) = summary else {
        let mut text = String::from("# The ants of this machine (docs/config.md), written by `ants setup`.\n");
        if let Some(harness) = harness {
            let _ = writeln!(text, "harness = {}", quoted(harness));
        }
        if connection.nats != crate::add::DEFAULT_NATS {
            let _ = writeln!(text, "nats = {}", quoted(connection.nats));
        }
        text.push_str("messenger = true\n");
        if let Some((_, entry)) = &entry {
            text.push('\n');
            text.push_str(entry);
        }
        let listed = match &entry {
            Some((name, _)) => format!("the messenger and {name}"),
            None => "the messenger".to_owned(),
        };
        let written = if report.dry_run { Ok(()) } else { write_new(path, &text) };
        match written {
            Ok(()) => report.step(
                Mark::Done,
                step,
                format!("{shown} serving {listed} on harness {}", harness.unwrap_or("none")),
            ),
            Err(e) => report.step(Mark::Fail, step, format!("{e:#}")),
        }
        return !report.dry_run;
    };
    if let Some(harness) = args.harness.as_ref().filter(|h| summary.harness.as_ref() != Some(*h)) {
        report.step(Mark::Skip, step, format!("{shown} kept: its harness stays, not set to {harness}"));
    }
    let Some((name, entry)) = entry else {
        match config::load(path) {
            Err(e) if harness::Unset::caused(&e) => {
                let installed = pick::installed(&pick::detect(&std::env::var_os("PATH").unwrap_or_default()));
                let installed = match installed.as_slice() {
                    [] => "none of the built-in ones".to_owned(),
                    names => names.join(", "),
                };
                report.step(Mark::Fail, step, format!("{shown} kept: {e:#}; installed here: {installed}"));
            }
            _ => report.step(Mark::Exists, step, format!("{shown} kept")),
        }
        return false;
    };
    if summary.ants.iter().any(|(ant, _)| ant == name) {
        report.step(Mark::Exists, step, format!("{shown} lists {name} already; its entry kept"));
        return false;
    }
    let appended = if report.dry_run { Ok(()) } else { config::append(path, &entry) };
    match appended {
        Ok(()) => report.step(Mark::Done, step, format!("{shown}: {name} added")),
        Err(e) => report.step(Mark::Fail, step, format!("{e:#}")),
    }
    !report.dry_run
}

/// The `[[ant]]` entry of `name` working in `workdir`, checked as the daemon will check it: running the top-level
/// harness of the file there, else `harness`, that of a new file.
fn ant_entry(
    name: &UserId,
    workdir: &Path,
    nats: &str,
    summary: Option<&Summary>,
    harness: Option<&str>,
) -> Result<String> {
    let workdir = workdir.canonicalize().with_context(|| format!("workdir {}", workdir.display()))?;
    let ant = Ant::new(name.clone(), workdir);
    let harnesses = summary.map_or_default(|s| s.harnesses.clone());
    let harness = match summary {
        Some(summary) => summary.harness.clone(),
        None => harness.map(str::to_owned),
    };
    Ant { harness, ..ant.clone() }.into_config(nats, &harnesses)?;
    config::entry(&ant)
}

/// Start `harness`, else the one of the config file, for an ACP handshake, as `ants harness check` does.
async fn harness_check(report: &mut Report, args: &SetupArgs, harness: Option<String>, config: &Path) {
    let step = "harness";
    if args.no_harness {
        return report.step(Mark::Skip, step, "--no-harness");
    }
    if args.dry_run {
        return report.step(Mark::Done, step, "ants harness check");
    }
    let pick = HarnessPick { harness, config: Some(config.to_owned()) };
    let checked = async {
        let harness = pick.resolve()?;
        let agent = harness::check(&harness).await?;
        anyhow::Ok(format!("{harness}: {agent}"))
    };
    match checked.await {
        Ok(agent) => report.step(Mark::Done, step, agent),
        Err(e) => report.step(Mark::Fail, step, format!("{e:#}")),
    }
}

/// Install and start the daemon service of the config file; reload it when running and the file changed.
fn daemon_service(
    report: &mut Report,
    args: &SetupArgs,
    services: &mut Services,
    config: &Path,
    changed: bool,
    values: impl Fn() -> Result<Values>,
) {
    let step = "daemon service";
    if args.no_services {
        return report.step(
            Mark::Skip,
            step,
            format!("--no-services; run it: ants daemon --config {}", config.display()),
        );
    }
    let values = match values() {
        Ok(values) => values,
        Err(e) => return report.step(Mark::Fail, step, format!("{e:#}")),
    };
    match services.install(Service::Daemon, config, &values, args.dry_run) {
        Installed::Running if changed => match services.apply(Service::Daemon, Action::Reload, config) {
            Outcome::Done(command) => report.step(Mark::Done, step, format!("running; reloaded: {command}")),
            outcome => report.step(
                Mark::Fail,
                step,
                format!("{}reload it: {}", service::failure(outcome), service::manual(Service::Daemon, Action::Reload)),
            ),
        },
        outcome => _ = installed(report, step, outcome, Service::Daemon),
    }
}

/// Install the completions of the login shell.
fn shell_completions(report: &mut Report, args: &SetupArgs, home: &Path) {
    let step = "completions";
    if args.no_completions {
        return report.step(Mark::Skip, step, "--no-completions");
    }
    let Some(shell) = Shell::login() else {
        return report.step(Mark::Skip, step, "$SHELL is not bash, zsh or fish; see `ants completions --help`");
    };
    match completions::install(shell, home, args.dry_run) {
        Ok(install) if install.installed == completions::Installed::Unchanged => {
            report.step(Mark::Exists, step, install);
        }
        Ok(install) => report.step(Mark::Done, step, install),
        Err(e) => report.step(Mark::Fail, step, format!("{e:#}")),
    }
}

/// The address a local nats-server for the URL `url` listens on: its host and port, when a loopback `nats://` one.
fn local_listen(url: &str) -> Option<String> {
    let rest = url.strip_prefix("nats://").or_else(|| (!url.contains("://")).then_some(url))?;
    let authority = rest.split(['/', '?', ',']).next()?;
    let authority = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(']') || authority.starts_with('[') => (host, port.parse::<u16>().ok()?),
        _ => (authority, 4222),
    };
    let host = match host {
        "localhost" | "127.0.0.1" => "127.0.0.1",
        "[::1]" | "::1" => "[::1]",
        _ => return None,
    };
    Some(format!("{host}:{port}"))
}

/// Whether something accepts connections on `listen`.
async fn listening(listen: &str) -> bool {
    let connect = tokio::net::TcpStream::connect(listen);
    matches!(tokio::time::timeout(Duration::from_secs(1), connect).await, Ok(Ok(_)))
}

async fn wait_listening(listen: &str) {
    let deadline = tokio::time::Instant::now() + LISTEN_WAIT;
    while !listening(listen).await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The first file named `name` on PATH.
fn on_path(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?).map(|dir| dir.join(name)).find(|p| p.is_file())
}

/// `text` as a quoted string, for nats-server configuration and TOML alike.
fn quoted(text: &str) -> String {
    serde_json::Value::String(text.to_owned()).to_string()
}

/// Write `text` to a new file at `path`; fails when there is one.
fn write_new(path: &Path, text: &str) -> Result<()> {
    use std::io::Write as _;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(text.as_bytes()).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_local_server_listens_on_the_loopback_address_of_the_url() {
        assert_eq!(local_listen("nats://127.0.0.1:4222").as_deref(), Some("127.0.0.1:4222"));
        assert_eq!(local_listen("nats://localhost").as_deref(), Some("127.0.0.1:4222"));
        assert_eq!(local_listen("nats://me:pw@localhost:4333").as_deref(), Some("127.0.0.1:4333"));
        assert_eq!(local_listen("nats://[::1]:4222").as_deref(), Some("[::1]:4222"));
        assert_eq!(local_listen("127.0.0.1:5000").as_deref(), Some("127.0.0.1:5000"));
        assert_eq!(local_listen("nats://hub.example:4222"), None);
        assert_eq!(local_listen("tls://127.0.0.1:4222"), None);
    }
}
