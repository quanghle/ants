//! The steps of `ants setup` about the machine's nats-server: its configuration, its executable, and who runs it, the
//! ants-nats service or with `--managed-nats` the daemon.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config;
use anyhow::{Result, bail};

use super::{Managed, Mark, Steps};
use crate::doctor;
use crate::node::conf::quote as quoted;
use crate::node::natsd::bundled;
use crate::service::{self, Action, Service, State, Values};

/// How to get nats-server, when not found and not downloaded.
const NATS_INSTALL: &str =
    "install it: https://docs.nats.io/running-a-nats-service/introduction/installation, or give --nats-server";

/// How long to wait for a nats-server just started to listen.
const LISTEN_WAIT: Duration = Duration::from_secs(10);

impl Steps<'_> {
    /// Write the local nats-server's configuration, unless there is one: listening on `listen`, with JetStream keeping
    /// its data in ~/.ants/jetstream.
    pub(super) fn nats_config(&mut self, listen: &str) {
        let (path, home) = (&self.paths.nats_conf, &self.paths.home);
        if path.exists() {
            return self.report.step(Mark::Exists, "nats-server config", format!("{} kept", path.display()));
        }
        let store = home.join(".ants/jetstream");
        let text = format!(
            "# The machine's local nats-server for ants, written by `ants setup`: JetStream on, listening on this \
             machine only.\nlisten: {}\njetstream {{\n  store_dir: {}\n}}\n",
            quoted(listen),
            quoted(&store.to_string_lossy())
        );
        let written = if self.report.dry_run { Ok(()) } else { super::write_new(path, &text) };
        match written {
            Ok(()) => self.report.step(
                Mark::Done,
                "nats-server config",
                format!("{} (JetStream in {})", path.display(), store.display()),
            ),
            Err(e) => self.report.step(Mark::Fail, "nats-server config", format!("{e:#}")),
        }
    }

    /// Report the machine's leaf node as its nats-server, none being set up beside it, and with --web whether it takes
    /// the page of `ants web` ([`Steps::web_on_leaf`]).
    pub(super) fn leaf_node(&mut self) {
        let leaf_conf = &self.paths.leaf_conf;
        let how = format!("the leaf node of {} is the machine's nats-server", leaf_conf.display());
        self.report.step(Mark::Skip, "nats-server", how);
        self.web_on_leaf();
    }

    /// Install and start the service running nats-server with the local configuration, unless a server listens on
    /// `listen` already. Returns whether it started one.
    pub(super) async fn nats_service(&mut self, listen: &str) -> bool {
        let conf = &self.paths.nats_conf;
        let step = "nats-server service";
        let up = listening(listen).await;
        if self.args.no_services {
            if up {
                self.report.step(Mark::Skip, step, format!("--no-services; a server listens on {listen}"));
            } else {
                self.report.step(
                    Mark::Skip,
                    step,
                    format!("--no-services; start one: nats-server -c {}", conf.display()),
                );
            }
            return false;
        }
        let state = self.services.state(Service::Nats, conf);
        if state == State::Running {
            self.report.step(Mark::Exists, step, format!("running {}", conf.display()));
            return false;
        }
        if up {
            self.report.step(
                Mark::Skip,
                step,
                format!("a server listens on {listen} already; none installed for {}", conf.display()),
            );
            return false;
        }
        let Some(nats_server) = self.nats_server_binary().await else {
            return false;
        };
        let values = match self.values() {
            Ok(values) => Values { nats_server, ..values },
            Err(e) => {
                self.report.step(Mark::Fail, step, format!("{e:#}"));
                return false;
            }
        };
        let installed = self.services.install(Service::Nats, conf, &values, self.args.dry_run);
        self.installed(step, installed, Service::Nats)
    }

    /// With --managed-nats, check that the daemon of the config file can be the only owner of the nats-server running
    /// `conf`, listening on `listen`: what the config file's `[nats_server]` is to name. Fails when an ants-nats
    /// service runs `conf`, or, for a new config file, when a server listens on `listen` already, which the daemon
    /// would not replace.
    pub(super) async fn managed_nats(&mut self, conf: &Path, listen: &str) -> Result<Managed> {
        let config = &self.paths.config;
        let step = "nats-server service";
        let (shown, config_shown) = (conf.display(), config.display());
        if let State::Running | State::Stopped(_) = self.services.state(Service::Nats, conf) {
            let remove = self.services.removal(Service::Nats).unwrap_or_default();
            self.report.step(
                Mark::Fail,
                step,
                format!(
                    "an ants-nats service runs {shown}, which --managed-nats has the daemon run: remove the service \
                     ({remove}), or drop --managed-nats"
                ),
            );
            bail!("two owners for the nats-server of {shown}: the ants-nats service and the daemon");
        }
        if config.exists() {
            let table = config::load(config).map(|loaded| loaded.nats_server.map(|n| n.config));
            let to_add = format!("[nats_server] config = {}", quoted(&conf.to_string_lossy()));
            return Ok(match table {
                Ok(Some(named)) if doctor::same_file(&named, conf) => {
                    let how = format!("none; the daemon of {config_shown} runs {shown} ([nats_server])");
                    self.report.step(Mark::Exists, step, how);
                    Managed::Kept
                }
                Ok(Some(named)) => {
                    let how = format!("[nats_server] of {config_shown} names {}, not {shown}; kept", named.display());
                    self.report.step(Mark::Skip, step, how);
                    Managed::Off
                }
                Ok(None) => {
                    let how = format!(
                        "{config_shown} kept, without [nats_server]: add `{to_add}` to it, then restart the daemon \
                         service ({})",
                        service::manual(Service::Daemon, Action::Restart)
                    );
                    self.report.step(Mark::Fail, step, how);
                    Managed::Off
                }
                Err(e) => {
                    self.report.step(Mark::Skip, step, format!("{config_shown} does not load: {e:#}"));
                    Managed::Off
                }
            });
        }
        if listening(listen).await {
            self.report.step(
                Mark::Fail,
                step,
                format!(
                    "a server listens on {listen} already, and the daemon starts none while another runs: stop it, or \
                     drop --managed-nats"
                ),
            );
            bail!("--managed-nats: a server listens on {listen} already");
        }
        self.nats_server_binary().await;
        let how = if self.args.no_services {
            format!("--no-services; `ants daemon --config {config_shown}` runs nats-server -c {shown} ([nats_server])")
        } else {
            format!("none; the daemon service runs nats-server -c {shown} ([nats_server] of {config_shown})")
        };
        self.report.step(Mark::Skip, step, how);
        Ok(Managed::New(conf.to_owned()))
    }

    /// The nats-server executable: --nats-server, else the one found ([`bundled::find`]), else the pinned release
    /// downloaded into ~/.ants/bin; none when the download failed.
    async fn nats_server_binary(&mut self) -> Option<PathBuf> {
        let step = "nats-server binary";
        if let Some(path) = &self.args.nats_server {
            return std::path::absolute(path).ok();
        }
        if let Some(found) = bundled::find() {
            return Some(found);
        }
        let (version, dry_run) = (bundled::version(), self.report.dry_run);
        let downloaded = async {
            let dir = bundled::dir()?;
            if dry_run {
                return Ok(dir.join(bundled::EXE));
            }
            bundled::download(&dir).await
        };
        match downloaded.await {
            Ok(exe) => {
                self.report.step(Mark::Done, step, format!("nats-server {version} into {}", exe.display()));
                Some(exe)
            }
            Err(e) => {
                let how = format!("none found {}, and the download failed: {e:#} ({NATS_INSTALL})", bundled::LOOKED_UP);
                self.report.step(Mark::Fail, step, how);
                None
            }
        }
    }
}

/// The address a local nats-server for the URL `url` listens on: its host and port, when a loopback `nats://` one.
pub(super) fn local_listen(url: &str) -> Option<String> {
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
pub(super) async fn listening(listen: &str) -> bool {
    let connect = tokio::net::TcpStream::connect(listen);
    matches!(tokio::time::timeout(Duration::from_secs(1), connect).await, Ok(Ok(_)))
}

pub(super) async fn wait_listening(listen: &str) {
    let deadline = tokio::time::Instant::now() + LISTEN_WAIT;
    while !listening(listen).await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::super::config::nats_server_table;
    #[cfg(unix)]
    use super::super::tests::{managed_args, paths, steps};
    use super::*;
    #[cfg(unix)]
    use crate::service::Services;

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

    #[cfg(unix)]
    #[tokio::test]
    async fn the_daemon_owns_the_managed_nats_server_alone() {
        let home = ants_test_support::scratch_dir("setup-managed-owner").canonicalize().unwrap();
        let paths = paths(&home);
        let (conf, config) = (&paths.leaf_conf, &paths.config);
        std::fs::write(conf, "listen: 127.0.0.1:4222\n").unwrap();
        let services = || {
            Services::answering(&home, |command| {
                let stdout = match command {
                    "id -u" => "501\n",
                    c if c.starts_with("launchctl print") => "\tstate = running\n\tpid = 7\n",
                    _ => "",
                };
                service::Ran { ok: true, stdout: stdout.to_owned(), stderr: String::new() }
            })
        };
        let free_listen = format!("127.0.0.1:{}", ants_test_support::free_port());
        let busy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let busy_listen = busy.local_addr().unwrap().to_string();
        let args = managed_args(Some("/bin/sh"));
        let mut steps = steps(&args, &paths);
        let mut owner = async |listen: &str| {
            steps.services = services();
            steps.managed_nats(conf, listen).await.map_err(|e| e.to_string())
        };

        assert_eq!(owner(&free_listen).await, Ok(Managed::New(conf.clone())), "a new config file names it");
        let listens = owner(&busy_listen).await.unwrap_err();
        assert_eq!(listens, format!("--managed-nats: a server listens on {busy_listen} already"));

        std::fs::write(config, "harness = \"copilot\"\nmessenger = true\n").unwrap();
        assert_eq!(owner(&busy_listen).await, Ok(Managed::Off), "a config file there is not edited");
        assert_eq!(std::fs::read_to_string(config).unwrap(), "harness = \"copilot\"\nmessenger = true\n");
        std::fs::write(config, format!("harness = \"copilot\"\nmessenger = true\n{}", nats_server_table(conf, None)))
            .unwrap();
        assert_eq!(owner(&busy_listen).await, Ok(Managed::Kept), "the daemon of the file there runs it");

        let unit = if cfg!(target_os = "macos") {
            home.join("Library/LaunchAgents/ants.nats.plist")
        } else {
            home.join(".config/systemd/user/ants-nats.service")
        };
        std::fs::create_dir_all(unit.parent().unwrap()).unwrap();
        let runs = format!("ExecStart=\"/bin/nats-server\" -c \"{0}\"\n<string>{0}</string>\n", conf.display());
        std::fs::write(&unit, runs).unwrap();
        let two = owner(&free_listen).await.unwrap_err();
        assert_eq!(
            two,
            format!("two owners for the nats-server of {}: the ants-nats service and the daemon", conf.display())
        );
        drop(owner);
        assert_eq!(steps.report.failed, 3, "the busy address, the file without the table, the ants-nats service");
        drop(busy);
        std::fs::remove_dir_all(&home).unwrap();
    }
}
