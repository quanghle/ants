//! Test helpers shared by the test suites of this crate that need its own code: a nats-server trusting a fresh team,
//! a hub for leaf nodes and uplink credentials. Those needing none of it are in `ants_test_support`.
//! Its submodules hold the fixtures the tests of several modules share.

pub(crate) mod colony;
pub(crate) mod github;
pub(crate) mod live;
pub(crate) mod orders;
pub(crate) mod presence;
pub(crate) mod render;
pub(crate) mod webhook;

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{JsDomain, Machine, Role, UserId, inbox_prefix};
use ants_test_support::NatsServer;
pub(crate) use ants_test_support::eventually;
use async_nats::{Client, Event, jetstream};
use tokio::sync::mpsc;

use crate::auth::{SERVER_CONF, init, issue, issue_leaf, members, role};
use crate::node::members::EnrollApprover;

/// A nats-server trusting a fresh team, whose keys are in its directory.
pub(crate) struct Server(NatsServer);

impl std::ops::Deref for Server {
    type Target = NatsServer;
    fn deref(&self) -> &NatsServer {
        &self.0
    }
}

impl std::ops::DerefMut for Server {
    fn deref_mut(&mut self) -> &mut NatsServer {
        &mut self.0
    }
}

impl Server {
    /// None when nats-server is not installed.
    pub(crate) fn start() -> Option<Self> {
        Self::start_with("", "")
    }

    /// A server trusting a fresh team, with `jetstream` added to its JetStream block and `extra` to its
    /// configuration.
    pub(crate) fn start_with(jetstream: &str, extra: &str) -> Option<Self> {
        NatsServer::spawn_with(|dir| {
            init(&dir.join("auth")).unwrap();
            let store = dir.join("js");
            format!("jetstream {{ store_dir: {store:?}\n{jetstream} }}\ninclude \"auth/{SERVER_CONF}\"\n{extra}")
        })
        .map(Self)
    }

    /// A server trusting a fresh team on the layout from before the store account: the team's account holds
    /// JetStream itself ([`crate::auth::write_server_conf_before_store`]).
    pub(crate) fn start_before_store() -> Option<Self> {
        NatsServer::spawn_with(|dir| {
            init(&dir.join("auth")).unwrap();
            crate::auth::write_server_conf_before_store(&dir.join("auth")).unwrap();
            format!("jetstream {{ store_dir: {:?} }}\ninclude \"auth/{SERVER_CONF}\"\n", dir.join("js"))
        })
        .map(Self)
    }

    pub(crate) fn creds(&self, user: &UserId) -> PathBuf {
        self.creds_in(user, None)
    }

    /// Credentials of `user` issued for `domain`, as `ants auth user` without `--human` does.
    pub(crate) fn creds_in(&self, user: &UserId, domain: Option<&JsDomain>) -> PathBuf {
        let suffix = domain.map_or_default(|d| format!("-{d}"));
        let out = self.dir().join(format!("{user}{suffix}.creds"));
        issue(&self.dir().join("auth"), user, role(user, false), domain, &out).unwrap();
        out
    }

    pub(crate) async fn connect(&self, user: &UserId) -> (Client, jetstream::Context) {
        self.connect_as(user, role(user, false)).await
    }

    /// Credentials of `user` issued in `role`.
    pub(crate) fn creds_as(&self, user: &UserId, role: Role) -> PathBuf {
        let creds = self.dir().join(format!("{user}-{role:?}.creds"));
        issue(&self.dir().join("auth"), user, role, None, &creds).unwrap();
        creds
    }

    /// A connection with credentials of `user` issued in `role`.
    pub(crate) async fn connect_as(&self, user: &UserId, role: Role) -> (Client, jetstream::Context) {
        let creds = self.creds_as(user, role);
        let (client, mut js) = bus::connect(&self.url, Some(&creds), None, user, "test").await.unwrap();
        // Denied JetStream requests only time out.
        js.set_timeout(Duration::from_secs(1));
        (client, js)
    }

    /// A connection that reports the server errors it gets, such as permission violations.
    pub(crate) async fn watched(&self, user: &UserId) -> (Client, mpsc::UnboundedReceiver<String>) {
        self.watched_with(user, &self.creds(user)).await
    }

    /// A connection with `creds` that reports the server errors it gets.
    pub(crate) async fn watched_with(&self, user: &UserId, creds: &Path) -> (Client, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let client = async_nats::ConnectOptions::with_credentials_file(creds)
            .await
            .unwrap()
            .custom_inbox_prefix(inbox_prefix(user))
            .event_callback(move |event| {
                let tx = tx.clone();
                async move {
                    if let Event::ServerError(e) = event {
                        let _ = tx.send(e.to_string());
                    }
                }
            })
            .connect(&self.url)
            .await
            .unwrap();
        (client, rx)
    }
}

/// A hub with the JetStream domain `hub` trusting a fresh team, taking leaf nodes on `leaf_port` (see
/// [`ants_test_support::free_port`]), with `extra` added to its configuration.
pub(crate) fn spawn_hub(leaf_port: u16, extra: &str) -> Option<NatsServer> {
    NatsServer::spawn_with(|dir| {
        init(&dir.join("auth")).unwrap();
        format!(
            "jetstream {{ store_dir: {:?}, domain: hub }}\ninclude \"auth/{}\"\n\
                 leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n{extra}\n",
            dir.join("js"),
            SERVER_CONF
        )
    })
}

/// Issue `out`, uplink credentials of `machine` for the hub's `domain`, for as many ants as they may carry; returns
/// the ants and the error refusing one more.
pub(crate) fn uplink_for_most_ants(
    team: &Path,
    machine: &Machine,
    domain: Option<&JsDomain>,
    out: &Path,
) -> (Vec<UserId>, String) {
    let mut ants = Vec::new();
    loop {
        ants.push(format!("dev-{}", ants.len()).parse().unwrap());
        if let Err(e) = issue_leaf(team, machine, &members(&ants, &[]), &[], EnrollApprover::Without, domain, out) {
            ants.pop();
            return (ants, e.to_string());
        }
    }
}
