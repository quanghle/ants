//! `ants setup`: bring up ants on a single machine, step by step, each step left alone when already done: the
//! harness of a new config file ([`pick`]), a local nats-server with JetStream and its service ([`nats`]), the hub's
//! stream and buckets (`ants init`), the config file with the messenger and optionally one ant ([`config`]), the
//! harness check, the machine's host credentials, the daemon service, the shell completions, and last `ants doctor`.
//! It never overwrites a file: one already there is reported and kept.
//!
//! With `--web` the local nats-server also takes the page of `ants web`: its configuration, the one file a re-run adds
//! to, gets a loopback WebSocket listener for that page's origin alone ([`web`]).
//!
//! With `--managed-nats` the daemon is the nats-server's only owner: the new config file's `[nats_server]` names its
//! configuration, no nats-server service is installed, and the hub's stream and buckets wait for the daemon service
//! to start it.

mod config;
mod nats;
mod pick;
mod web;

use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};

use ants_daemon::config::Summary;
use ants_daemon::harness;
use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};

use crate::cmd::harness::HarnessPick;
use crate::completions::{self, Shell};
use crate::connection::{Connection, Nats};
use crate::node::natsd::bundled;
use crate::service::{self, Action, Installed, Outcome, Service, Services, Values};
use crate::{auth, doctor};

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
    /// nats-server binary for its service, or with --managed-nats the `command` of `[nats_server]` (default: the one
    /// beside the ants executable, in ~/.ants/bin or on PATH, else the pinned release downloaded into ~/.ants/bin).
    #[arg(long, value_name = "PATH")]
    pub nats_server: Option<PathBuf>,
    /// Have `ants daemon` run the local nats-server: the new config file gets a `[nats_server]` table naming its
    /// configuration, and no nats-server service is installed, the daemon service being its only owner.
    #[arg(long)]
    pub managed_nats: bool,
    /// Also take the page of `ants web`: add a WebSocket listener on port 4281 of the local nats-server's loopback
    /// host to its configuration, for pages of the origin `ants web` serves (http://127.0.0.1:4280) only, and
    /// restart a server running it. On a leaf node, `ants leaf config --web` does.
    #[arg(long)]
    pub web: bool,
    /// Install and start no services; print how to start nats-server, or with --managed-nats the daemon, instead.
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

/// What --managed-nats came to.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Managed {
    /// Not asked for, or not done: the config file there has no `[nats_server]` for it.
    Off,
    /// The new config file's `[nats_server]` is to name this nats-server configuration.
    New(PathBuf),
    /// The config file there names it in its `[nats_server]` already.
    Kept,
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

/// Where `ants setup` writes and looks.
struct Paths {
    home: PathBuf,
    /// The config file of the machine's ants.
    config: PathBuf,
    /// The configuration of the machine's leaf node, its nats-server when there is one.
    leaf_conf: PathBuf,
    /// The configuration of the local nats-server set up otherwise.
    nats_conf: PathBuf,
}

/// What the steps share: the report they add to, the flags, the server, the machine's services and the paths.
struct Steps<'a> {
    report: Report,
    args: &'a SetupArgs,
    connection: &'a Connection<'a>,
    services: Services,
    paths: &'a Paths,
}

/// Run the steps of `ants setup`, then `ants doctor`; fail when a step or a check fails.
pub async fn run(connection: &Connection<'_>, args: SetupArgs) -> Result<()> {
    let home = std::env::home_dir().context("no home directory")?;
    let config = match &args.config {
        Some(path) => std::path::absolute(path)?,
        None => ants_daemon::config::default_path()?,
    };
    let leaf_conf = crate::node::leaf::default_path()?;
    let nats_conf = auth::default_dir()?.with_file_name("nats.conf");
    let paths = Paths { home, config, leaf_conf, nats_conf };
    let report = Report { dry_run: args.dry_run, failed: 0 };
    let mut steps = Steps { report, args: &args, connection, services: Services::local(), paths: &paths };
    let harness = steps.pick_harness()?;

    let listen = nats::local_listen(connection.nats);
    let mut started = false;
    let mut managed = Managed::Off;
    match &listen {
        None if args.managed_nats => {
            steps.report.step(
                Mark::Fail,
                "nats-server",
                format!("{} is not on this machine, and --managed-nats runs a local nats-server", connection.nats),
            );
            bail!(
                "--managed-nats needs a server on this machine (nats://127.0.0.1:PORT): give --nats, or drop \
                 --managed-nats"
            );
        }
        None => {
            steps.report.step(
                Mark::Skip,
                "nats-server",
                format!("{} is not on this machine: no local nats-server set up", connection.nats),
            );
            steps.web_elsewhere();
        }
        Some(listen) if args.managed_nats => {
            let conf = if paths.leaf_conf.exists() {
                steps.leaf_node();
                &paths.leaf_conf
            } else {
                steps.nats_config(listen);
                steps.web(listen).await;
                &paths.nats_conf
            };
            managed = steps.managed_nats(conf, listen).await?;
        }
        Some(_) if paths.leaf_conf.exists() => steps.leaf_node(),
        Some(listen) => {
            steps.nats_config(listen);
            steps.web(listen).await;
            started = steps.nats_service(listen).await;
        }
    }
    if started && !args.dry_run {
        nats::wait_listening(listen.as_deref().unwrap_or_default()).await;
    }

    // The daemon starts a managed nats-server: only once its service runs is there one to set up.
    let after_daemon = managed != Managed::Off;
    if !after_daemon {
        steps.init(listen.as_deref(), false).await;
    }
    let appended = steps.config_file(harness.as_deref(), &managed);
    steps.harness_check(harness).await;
    steps.host_creds().await;
    let restart = matches!(managed, Managed::New(_));
    let daemon_up = steps.daemon_service(appended, restart);
    if after_daemon {
        if daemon_up && !args.dry_run {
            nats::wait_listening(listen.as_deref().unwrap_or_default()).await;
        }
        steps.init(listen.as_deref(), true).await;
    }
    steps.shell_completions();

    if args.dry_run {
        println!("would run ants doctor");
        return Ok(());
    }
    let unstarted = steps.unstarted(listen.as_deref(), after_daemon).await;
    Box::pin(steps.doctor_checks(unstarted)).await
}

impl Steps<'_> {
    /// What the units of the services are written with.
    fn values(&self) -> Result<Values> {
        let nats_server = match &self.args.nats_server {
            Some(path) => std::path::absolute(path)?,
            None => bundled::find().unwrap_or_default(),
        };
        let path = std::env::var("PATH").unwrap_or_default();
        Ok(Values { ants: std::env::current_exe()?, nats_server, path })
    }

    /// The harness of a new config file: --harness, else the only built-in one installed, else one asked for on a
    /// terminal; None for a file there. Fails, listing the harnesses, when none can be picked.
    fn pick_harness(&mut self) -> Result<Option<String>> {
        let config = &self.paths.config;
        let step = "harness choice";
        if self.args.harness.is_some() || config.exists() {
            return Ok(self.args.harness.clone());
        }
        let detected = pick::detect(&std::env::var_os("PATH").unwrap_or_default());
        let (mut input, mut output) = (std::io::stdin().lock(), std::io::stdout());
        let terminal = input.is_terminal();
        let prompt: Option<(&mut dyn std::io::BufRead, &mut dyn std::io::Write)> =
            terminal.then_some((&mut input, &mut output));
        match pick::choose(&detected, prompt) {
            Ok((name, how)) => {
                self.report.step(Mark::Done, step, how);
                Ok(Some(name))
            }
            Err(e) => {
                self.report.step(Mark::Fail, step, format!("{e:#}"));
                bail!("no harness for {}: install one or pass --harness NAME", config.display())
            }
        }
    }

    /// Create the hub's stream and buckets, as `ants init` does; with `managed`, on the server the daemon of the config
    /// file runs.
    async fn init(&mut self, listen: Option<&str>, managed: bool) {
        let connection = self.connection;
        let step = "ants init";
        if self.args.dry_run {
            return self.report.step(Mark::Done, step, format!("stream and buckets on {}", connection.nats));
        }
        if let Some(listen) = listen
            && !nats::listening(listen).await
        {
            let then = if !managed {
                "run `ants init` once one does".to_owned()
            } else if self.args.no_services {
                let config = self.paths.config.display();
                format!("start `ants daemon --config {config}`, which runs it, then `ants init`")
            } else {
                "the daemon service starts it (its log tells why not), then run `ants init`".to_owned()
            };
            return self.report.step(Mark::Skip, step, format!("no server listens on {listen}; {then}"));
        }
        let done = async {
            let nats = Nats::new(connection.nats, connection.creds, connection.js_domain)?;
            crate::cmd::init::hub(&nats, false).await
        };
        match done.await {
            Ok(lines) => self.report.step(Mark::Done, step, lines.join("\n       ")),
            Err(e) => self.report.step(Mark::Fail, step, format!("{e:#}")),
        }
    }

    /// Issue the machine's host credentials where the daemon finds them, `host-<machine>.creds` in the team directory,
    /// when the config file's ants have credentials, the team's keys are here and the daemon finds none.
    async fn host_creds(&mut self) {
        let (connection, config) = (self.connection, &self.paths.config);
        let step = "host credentials";
        let summary = match Summary::load(config) {
            Ok(summary) => summary,
            Err(e) => return self.report.step(Mark::Skip, step, format!("{e:#}")),
        };
        if let Some(creds) = &summary.host.creds {
            return self.report.step(Mark::Exists, step, creds.display());
        }
        if summary.ants.iter().all(|(_, creds)| creds.is_none()) {
            return self.report.step(Mark::Skip, step, "the ants connect as no one, and so does the machine's host");
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
                return self.report.step(Mark::Skip, step, hint);
            }
        };
        let out = dir.join(format!("{}.creds", ants_proto::host_user(&machine)));
        if self.args.dry_run {
            return self.report.step(Mark::Done, step, format!("issue {}", out.display()));
        }
        match auth::issue_host(&dir, &machine, connection.js_domain, &out) {
            Ok(()) => self.report.step(Mark::Done, step, out.display()),
            Err(e) => self.report.step(Mark::Fail, step, format!("{e:#}")),
        }
    }

    /// Start `harness`, else the one of the config file, for an ACP handshake, as `ants harness check` does.
    async fn harness_check(&mut self, harness: Option<String>) {
        let step = "harness";
        if self.args.no_harness {
            return self.report.step(Mark::Skip, step, "--no-harness");
        }
        if self.args.dry_run {
            return self.report.step(Mark::Done, step, "ants harness check");
        }
        let pick = HarnessPick { harness, config: Some(self.paths.config.clone()) };
        let checked = async {
            let harness = pick.resolve()?;
            let agent = harness::check(&harness).await?;
            anyhow::Ok(format!("{harness}: {agent}"))
        };
        match checked.await {
            Ok(agent) => self.report.step(Mark::Done, step, agent),
            Err(e) => self.report.step(Mark::Fail, step, format!("{e:#}")),
        }
    }

    /// Install and start the daemon service of the config file; when running and the file changed, reload it, or
    /// restart it with `restart`, for what only a start reads (`[nats_server]`). Returns whether it runs.
    fn daemon_service(&mut self, changed: bool, restart: bool) -> bool {
        let config = &self.paths.config;
        let step = "daemon service";
        if self.args.no_services {
            self.report.step(
                Mark::Skip,
                step,
                format!("--no-services; run it: ants daemon --config {}", config.display()),
            );
            return false;
        }
        let values = match self.values() {
            Ok(values) => values,
            Err(e) => {
                self.report.step(Mark::Fail, step, format!("{e:#}"));
                return false;
            }
        };
        let (action, done) = if restart { (Action::Restart, "restarted") } else { (Action::Reload, "reloaded") };
        match self.services.install(Service::Daemon, config, &values, self.args.dry_run) {
            Installed::Running if changed => match self.services.apply(Service::Daemon, action, config) {
                Outcome::Done(command) => {
                    self.report.step(Mark::Done, step, format!("running; {done}: {command}"));
                    true
                }
                outcome => {
                    let manual = service::manual(Service::Daemon, action);
                    let how = format!(
                        "{}{} it: {manual}",
                        service::failure(outcome),
                        if restart { "restart" } else { "reload" }
                    );
                    self.report.step(Mark::Fail, step, how);
                    false
                }
            },
            Installed::Running => {
                self.report.step(Mark::Exists, step, "installed and running");
                true
            }
            outcome => self.installed(step, outcome, Service::Daemon),
        }
    }

    /// Report what [`Services::install`] did; returns whether it started the service.
    fn installed(&mut self, step: &str, installed: Installed, service: Service) -> bool {
        match installed {
            Installed::New(unit, command) => {
                self.report.step(Mark::Done, step, format!("installed {} and started it ({command})", unit.display()));
                true
            }
            Installed::Started(command) => {
                self.report.step(Mark::Done, step, format!("installed already, started: {command}"));
                true
            }
            Installed::Running => {
                self.report.step(Mark::Exists, step, "installed and running");
                false
            }
            Installed::Other(unit) => {
                let install = match service {
                    Service::Daemon => "scripts/install-units.sh --config FILE",
                    Service::Nats => "scripts/install-units.sh --nats-conf FILE",
                    _ => "scripts/install-units.sh",
                };
                self.report.step(
                    Mark::Skip,
                    step,
                    format!("{} runs another file; left alone (switch: {install})", unit.display()),
                );
                false
            }
            Installed::Unsupported => {
                self.report.step(Mark::Skip, step, "no service manager ants installs units for here");
                false
            }
            Installed::Failed(command, e) => {
                self.report.step(Mark::Fail, step, format!("`{command}` failed: {e}"));
                false
            }
        }
    }

    /// Install the completions of the login shell.
    fn shell_completions(&mut self) {
        let step = "completions";
        if self.args.no_completions {
            return self.report.step(Mark::Skip, step, "--no-completions");
        }
        let Some(shell) = Shell::login() else {
            return self.report.step(
                Mark::Skip,
                step,
                "$SHELL is not bash, zsh or fish; see `ants completions --help`",
            );
        };
        match completions::install(shell, &self.paths.home, self.args.dry_run) {
            Ok(install) if install.installed == completions::Installed::Unchanged => {
                self.report.step(Mark::Exists, step, install);
            }
            Ok(install) => self.report.step(Mark::Done, step, install),
            Err(e) => self.report.step(Mark::Fail, step, format!("{e:#}")),
        }
    }

    /// The address of the local server --no-services left to start, when it does not listen yet, and the command that
    /// starts it: the daemon of the config file for a `managed` one, else nats-server on the leaf node or local hub
    /// configuration.
    async fn unstarted(&self, listen: Option<&str>, managed: bool) -> Option<(String, String)> {
        let Paths { config, leaf_conf, nats_conf, .. } = self.paths;
        let listen = listen.filter(|_| self.args.no_services)?;
        if nats::listening(listen).await {
            return None;
        }
        let start = if managed {
            format!("ants daemon --config {}", config.display())
        } else {
            format!("nats-server -c {}", if leaf_conf.exists() { leaf_conf } else { nats_conf }.display())
        };
        Some((listen.to_owned(), start))
    }

    /// Run `ants doctor` on the config file and the leaf node and local hub configurations; fail when a check failed,
    /// or a step before. `unstarted` is the address and start command of a server --no-services left to start, not
    /// listening yet ([`left_to_start`]).
    async fn doctor_checks(&self, unstarted: Option<(String, String)>) -> Result<()> {
        println!("ants doctor:");
        let (connection, paths) = (self.connection, self.paths);
        let setup = doctor::Setup {
            nats: connection.nats.to_owned(),
            creds: connection.creds.map(Path::to_owned),
            js_domain: connection.js_domain.cloned(),
            config: paths.config.clone(),
            leaf_config: paths.leaf_conf.clone(),
            hub_config: paths.nats_conf.clone(),
            harnesses: false,
            team: auth::default_dir()?,
            webhooks: paths.config.with_file_name("webhooks.toml"),
            worker: doctor::trail_worker(Some(paths.home.clone())),
            home: Some(paths.home.clone()),
        };
        let mut checks = Box::pin(doctor::run(&setup)).await;
        if let Some((listen, start)) = unstarted {
            left_to_start(&mut checks, &listen, &start);
        }
        for check in &checks {
            println!("{check}");
        }
        let doctor_failed = checks.iter().filter(|c| c.status == doctor::Status::Fail).count();
        match (self.report.failed, doctor_failed) {
            (0, 0) => Ok(()),
            (0, n) => bail!("{n} of {} checks failed", checks.len()),
            (n, _) => bail!("{n} setup steps failed"),
        }
    }
}

/// Skip the checks of `checks` failing as no server listens on `listen`, one --no-services left to start with
/// `start`: the setup did what it was asked, and `ants doctor` checks the server once it runs.
fn left_to_start(checks: &mut [doctor::Check], listen: &str, start: &str) {
    for check in checks {
        if check.status == doctor::Status::Fail && ["server", "leaf node"].contains(&check.check.as_str()) {
            check.status = doctor::Status::Skip;
            check.detail = format!(
                "no server listens on {listen} yet: --no-services leaves it to start ({start}); run `ants doctor` \
                 once it runs"
            );
            check.fix.clear();
        }
    }
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

    pub(super) fn managed_args(nats_server: Option<&str>) -> SetupArgs {
        SetupArgs {
            ant: None,
            workdir: None,
            harness: None,
            config: None,
            nats_server: nats_server.map(PathBuf::from),
            managed_nats: true,
            web: false,
            no_services: false,
            no_harness: true,
            no_completions: true,
            dry_run: false,
        }
    }

    /// The paths of a setup under `home`, a scratch directory.
    pub(super) fn paths(home: &Path) -> Paths {
        Paths {
            home: home.to_owned(),
            config: home.join("config.toml"),
            leaf_conf: home.join("leaf.conf"),
            nats_conf: home.join("nats.conf"),
        }
    }

    /// The steps of a setup with `args` on `paths`, at the default server, nothing reported yet.
    pub(super) fn steps<'a>(args: &'a SetupArgs, paths: &'a Paths) -> Steps<'a> {
        const CONNECTION: Connection<'static> =
            Connection { nats: crate::cli::DEFAULT_NATS, creds: None, js_domain: None };
        let report = Report { dry_run: false, failed: 0 };
        Steps { report, args, connection: &CONNECTION, services: Services::local(), paths }
    }

    #[test]
    fn a_server_left_to_start_fails_no_check() {
        use doctor::Status::{Fail, Skip};
        let check = |check: &str, status, detail: &str| doctor::Check {
            check: check.to_owned(),
            status,
            detail: detail.to_owned(),
            fix: vec!["a fix".to_owned()],
        };
        let mut checks = vec![
            check("server", Fail, "nats://127.0.0.1:4222 does not answer"),
            check("leaf node", Fail, "nothing listens on 127.0.0.1:4222"),
            check("config", Fail, "unreadable"),
            check("hub", Skip, "JetStream does not answer"),
        ];
        left_to_start(&mut checks, "127.0.0.1:4222", "ants daemon --config /c.toml");
        let statuses: Vec<_> = checks.iter().map(|c| c.status).collect();
        assert_eq!(statuses, [Skip, Skip, Fail, Skip]);
        assert_eq!(
            checks[0].detail,
            "no server listens on 127.0.0.1:4222 yet: --no-services leaves it to start (ants daemon --config \
             /c.toml); run `ants doctor` once it runs"
        );
        assert!(checks[0].fix.is_empty() && checks[1].fix.is_empty());
        assert_eq!(checks[2].fix, ["a fix"], "a failure of its own");
    }
}
