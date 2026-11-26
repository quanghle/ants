//! The step of `ants setup --web`: the loopback WebSocket listener the local nats-server takes the page of `ants web`
//! on.

use std::time::Duration;

use super::nats::{listening, wait_listening};
use super::{Mark, Steps};
use crate::node::conf::quote as quoted;
use crate::node::natsd;
use crate::node::web as page;
use crate::service::{Action, Outcome};

/// The step of --web.
const WEB_STEP: &str = "web listener";

impl Steps<'_> {
    /// With --web, have the local nats-server, whose clients connect to `listen`, take the page of `ants web`: add the
    /// loopback WebSocket listener to its configuration ([`Steps::web_listener`]) and restart a server listening
    /// already, through the service or the daemon running the configuration, as nats-server reloads no change of a
    /// WebSocket listener. One not started yet reads the file at its start.
    pub(super) async fn web(&mut self, listen: &str) {
        let conf = &self.paths.nats_conf;
        if !self.args.web || !self.web_listener(listen) || !listening(listen).await {
            return;
        }
        let by_hand = format!("stop it and run `nats-server -c {}`", conf.display());
        match natsd::apply(conf, Action::Restart) {
            Outcome::Done(command) => {
                self.report.step(Mark::Done, WEB_STEP, format!("nats-server restarted to listen there ({command})"));
                // The restart is under way: the old server may still listen.
                tokio::time::sleep(Duration::from_millis(500)).await;
                wait_listening(listen).await;
            }
            Outcome::Failed(command, e) => self.report.step(
                Mark::Fail,
                WEB_STEP,
                format!("`{command}` failed ({e}): restart the nats-server on {listen} to listen there ({by_hand})"),
            ),
            Outcome::NotRunning | Outcome::Unmanaged => self.report.step(
                Mark::Skip,
                WEB_STEP,
                format!(
                    "neither a service nor a daemon here runs the server on {listen}: restart it to listen there \
                     ({by_hand})"
                ),
            ),
        }
    }

    /// Add the loopback WebSocket listener for the page of `ants web` to the local nats-server's configuration, whose
    /// clients connect to `listen`, unless it has one for that page's origin; a listener for other origins is kept, and
    /// fails the step. Returns whether it changed the file.
    fn web_listener(&mut self, listen: &str) -> bool {
        let conf = &self.paths.nats_conf;
        let (shown, origin) = (conf.display(), page::origin());
        let at = match page::listen(listen) {
            Ok(at) => at,
            Err(e) => {
                self.report.step(Mark::Fail, WEB_STEP, format!("{e:#}"));
                return false;
            }
        };
        let added = format!("{shown}: ws://{at} takes pages of {origin} only, as `ants web` serves it");
        let text = match std::fs::read_to_string(conf) {
            Ok(text) => text,
            // The file a dry run did not write.
            Err(_) if self.report.dry_run => {
                self.report.step(Mark::Done, WEB_STEP, added);
                return false;
            }
            Err(e) => {
                self.report.step(Mark::Fail, WEB_STEP, format!("reading {shown}: {e}"));
                return false;
            }
        };
        match page::add(&text, at, &origin) {
            page::Added::There => {
                self.report.step(Mark::Exists, WEB_STEP, format!("{shown} takes pages of {origin} already"));
                false
            }
            page::Added::Other(origins) => {
                let takes = match origins.as_slice() {
                    [] => "any origin".to_owned(),
                    origins => origins.join(", "),
                };
                let how = format!(
                    "{shown} has a websocket listener already, taking pages of {takes}; kept: add {} to its \
                     allowed_origins and restart nats-server",
                    quoted(&origin)
                );
                self.report.step(Mark::Fail, WEB_STEP, how);
                false
            }
            page::Added::New(_) if self.report.dry_run => {
                self.report.step(Mark::Done, WEB_STEP, added);
                false
            }
            page::Added::New(text) => match std::fs::write(conf, text) {
                Ok(()) => {
                    self.report.step(Mark::Done, WEB_STEP, added);
                    true
                }
                Err(e) => {
                    self.report.step(Mark::Fail, WEB_STEP, format!("writing {shown}: {e}"));
                    false
                }
            },
        }
    }

    /// With --web and a server on another machine: nothing here to add a listener to.
    pub(super) fn web_elsewhere(&mut self) {
        let nats = self.connection.nats;
        if self.args.web {
            let how = format!(
                "{nats} is not on this machine: --web adds a listener to a local nats-server; a hub names the page's \
                 origin in its websocket block (docs/transport.md)"
            );
            self.report.step(Mark::Fail, WEB_STEP, how);
        }
    }

    /// With --web on a machine whose nats-server is its leaf node: whether it takes the page of `ants web`, which
    /// `ants leaf config --web` writes, that file being its to write.
    pub(super) fn web_on_leaf(&mut self) {
        let leaf_conf = &self.paths.leaf_conf;
        if !self.args.web {
            return;
        }
        let (shown, origin) = (leaf_conf.display(), page::origin());
        let taken = std::fs::read_to_string(leaf_conf)
            .is_ok_and(|text| page::Listener::read(&text).is_some_and(|listener| listener.takes(&origin)));
        if taken {
            self.report.step(Mark::Exists, WEB_STEP, format!("{shown} takes pages of {origin} already"));
        } else {
            let how = format!(
                "the leaf node of {shown} is the machine's nats-server: run `ants leaf config MACHINE --uplink URL \
                 --web`"
            );
            self.report.step(Mark::Fail, WEB_STEP, how);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{managed_args, paths, steps};
    use super::super::{Report, SetupArgs};
    use super::*;

    /// `ants setup --web` on a machine whose nats-server an `ants daemon` manages: the listener is added, the server
    /// restarted through the daemon ([`natsd::apply`]), as nats-server takes no new WebSocket listener on a reload,
    /// and the listener then takes pages of the origin of `ants web` alone; a second run changes nothing. Skipped
    /// without nats-server, and while the listener's fixed port is taken, as by the machine's own server.
    #[cfg(unix)]
    #[tokio::test]
    async fn web_restarts_a_managed_server_whose_new_listener_takes_the_page_of_ants_web_only() {
        use ants_daemon::config::NatsServer;
        use ants_test_support::{eventually_within, free_port, have};

        const TAKEN: &str = "HTTP/1.1 101 Switching Protocols";
        const REFUSED: &str = "HTTP/1.1 403 Forbidden";
        let at = page::listen("127.0.0.1:4222").unwrap();
        if !have("nats-server") {
            return;
        }
        if std::net::TcpListener::bind(at).is_err() {
            eprintln!("skipped: {at}, the listener's port, is taken");
            return;
        }
        let home = ants_test_support::scratch_dir("setup-web-managed").canonicalize().unwrap();
        let paths = paths(&home);
        let listen = format!("127.0.0.1:{}", free_port());
        let args = SetupArgs { web: true, ..managed_args(None) };
        let mut steps = steps(&args, &paths);
        steps.nats_config(&listen);
        let settings = NatsServer {
            command: "nats-server".into(),
            config: paths.nats_conf.clone(),
            ready_timeout: Duration::from_secs(10),
        };
        let policy = natsd::Policy { backoff: Duration::from_millis(50), ..natsd::Policy::DEFAULT };
        let managed = natsd::start(&settings, &format!("nats://{listen}"), policy, std::future::pending())
            .await
            .unwrap()
            .expect("a server started");
        assert!(page::Listener::read(&std::fs::read_to_string(&paths.nats_conf).unwrap()).is_none());
        assert!(!listening(&at.to_string()).await, "no listener before");

        let addr = at.to_string();
        let served = natsd::run_serving(managed, async {
            steps.web(&listen).await;
            eventually_within(Duration::from_secs(10), "the restarted server listens for pages", async || {
                listening(&addr).await
            })
            .await;
            let answers = tokio::task::spawn_blocking({
                let addr = addr.clone();
                move || {
                    [Some(page::origin().as_str()), Some("http://localhost:4280"), Some("https://evil.example"), None]
                        .map(|origin| page::upgrade(&addr, origin))
                }
            })
            .await?;
            steps.web(&listen).await;
            Ok(answers)
        })
        .await
        .unwrap();
        assert_eq!(served, [TAKEN, REFUSED, REFUSED, TAKEN], "the page's origin, others refused, the CLI's none");
        assert_eq!(steps.report.failed, 0);
        let block = page::block(at, &page::origin());
        assert_eq!(std::fs::read_to_string(&paths.nats_conf).unwrap().matches(&block).count(), 1, "added once");
        std::fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn web_adds_the_listener_once_and_a_second_run_changes_nothing() {
        let home = ants_test_support::scratch_dir("setup-web").canonicalize().unwrap();
        let paths = paths(&home);
        let (conf, leaf_conf) = (&paths.nats_conf, &paths.leaf_conf);
        let listen = "127.0.0.1:4222";
        let args = SetupArgs { web: true, ..managed_args(None) };
        let mut steps = steps(&args, &paths);
        steps.report.dry_run = true;
        steps.nats_config(listen);
        assert!(!steps.web_listener(listen));
        assert!(!conf.exists(), "a dry run writes nothing");

        steps.report = Report { dry_run: false, failed: 0 };
        steps.nats_config(listen);
        let plain = std::fs::read_to_string(conf).unwrap();
        assert!(steps.web_listener(listen), "the first run adds it");
        let first = std::fs::read_to_string(conf).unwrap();
        let block = page::block("127.0.0.1:4281".parse().unwrap(), "http://127.0.0.1:4280");
        assert_eq!(first, format!("{plain}{block}"));

        steps.nats_config(listen);
        assert!(!steps.web_listener(listen), "the second run changes nothing");
        assert_eq!(std::fs::read_to_string(conf).unwrap(), first);
        assert_eq!(steps.report.failed, 0);

        let open = format!("{plain}websocket {{\n  port: 8080\n  no_tls: true\n}}\n");
        std::fs::write(conf, &open).unwrap();
        assert!(!steps.web_listener(listen));
        assert_eq!(std::fs::read_to_string(conf).unwrap(), open, "another listener is kept");
        assert_eq!(steps.report.failed, 1);

        std::fs::write(leaf_conf, &plain).unwrap();
        steps.web_on_leaf();
        assert_eq!(steps.report.failed, 2, "a leaf node's listener is `ants leaf config --web`'s to write");
        std::fs::write(leaf_conf, &first).unwrap();
        steps.web_on_leaf();
        assert_eq!(std::fs::read_to_string(leaf_conf).unwrap(), first);
        assert_eq!(steps.report.failed, 2);
        std::fs::remove_dir_all(&home).unwrap();
    }
}
