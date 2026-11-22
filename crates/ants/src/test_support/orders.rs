//! Fixtures of admin orders: the host connection of a machine, and `ants signer` answering orders.

use std::time::Duration;

use ants_daemon::admin::ConfigFile;
use ants_daemon::admin::restart::Restarts;
use ants_daemon::host::{self, Host};
use ants_proto::UserId;
use anyhow::Result;
use async_nats::jetstream;

use crate::auth::issue_host;
use crate::machines;
use crate::test_support::Server;

/// How long a test waits for a daemon to decide an order. An order delivered to a pull request that went silent comes
/// again after a few seconds, and a loaded test machine may take several more; a decided order ends the wait.
pub(crate) const WAIT: Duration = Duration::from_secs(20);

pub(crate) fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

/// The host connection of m1 serving dev-a, as a daemon whose config sets `[admin] remote` to `remote` runs it,
/// serving the file `config`, once it recorded its ants; dropping the sender stops it.
pub(crate) async fn host_of_m1(
    server: &Server,
    js: &jetstream::Context,
    remote: bool,
    config: Option<ConfigFile>,
) -> (tokio::sync::watch::Sender<Vec<UserId>>, tokio::task::JoinHandle<Result<()>>) {
    host_of_m1_serving(server, js, remote, config, Restarts::default()).await
}

/// [`host_of_m1`], of a process serving the ants `restarts` holds.
pub(crate) async fn host_of_m1_serving(
    server: &Server,
    js: &jetstream::Context,
    remote: bool,
    config: Option<ConfigFile>,
    restarts: Restarts,
) -> (tokio::sync::watch::Sender<Vec<UserId>>, tokio::task::JoinHandle<Result<()>>) {
    let creds = server.dir().join("host-m1.creds");
    issue_host(&server.dir().join("auth"), &"m1".parse().unwrap(), None, &creds).unwrap();
    let (url, creds) = (server.url.clone(), Some(creds));
    let host = Host {
        machine: None,
        fallback: None,
        nats: url,
        creds,
        js_domain: None,
        remote_admin: remote,
        config,
        restarts,
        providers: Default::default(),
        leaf: None,
    };
    let (ants, watched) = tokio::sync::watch::channel(vec![p("dev-a")]);
    let task = tokio::spawn(host::run(host, watched));
    tokio::time::timeout(WAIT, async {
        while machines::machines_of(js, &p("dev-a")).await.unwrap_or_default().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the host records its ants");
    (ants, task)
}

/// How long an order the signer answers may take: the daemon asks again every 5s.
pub(crate) const SIGNED: Duration = Duration::from_secs(30);

/// Run `body` while `ants signer` answers over `server` for the team of `server.dir()/auth`, once it listens: a
/// request sent before would wait for the daemon to ask again.
pub(crate) async fn with_signer<T>(server: &Server, body: impl Future<Output = T>) -> T {
    with_signer_at(&server.url, server.dir(), None, body).await
}

/// [`with_signer`] over the server at `url` of JetStream domain `js_domain`, for the team of `dir/auth`.
pub(crate) async fn with_signer_at<T>(
    url: &str,
    dir: &std::path::Path,
    js_domain: Option<&ants_proto::JsDomain>,
    body: impl Future<Output = T>,
) -> T {
    let config = dir.join("signer.toml");
    std::fs::write(&config, "").unwrap();
    let options = crate::signing::signer::Options {
        config: &config,
        approval_timeout: SIGNED,
        reconnect: SIGNED * 3,
        tick: Duration::from_millis(200),
    };
    let team = dir.join("auth");
    let serving = crate::signing::signer::serve(url, &team, js_domain, &options, |_, _| {}, std::future::pending());
    tokio::select! {
        r = serving => panic!("the signer stopped: {r:?}"),
        out = async { Box::pin(signer_listens(url, &team, js_domain)).await; body.await } => out,
    }
}

/// Wait until the signer of the team in `team` listens for ordered enrollment requests over the server at `url`: a
/// request without one gets "no responders", one the signer hears goes unanswered (an empty request is ignored).
async fn signer_listens(url: &str, team: &std::path::Path, js_domain: Option<&ants_proto::JsDomain>) {
    let creds = crate::auth::signer_creds(team, js_domain).unwrap();
    let options = async_nats::ConnectOptions::with_credentials(&creds).unwrap();
    let client = options.custom_inbox_prefix(ants_proto::ENROLL_INBOX).connect(url).await.unwrap();
    let probe = ants_proto::subject::enroll(&p("signer-probe"));
    crate::test_support::eventually("the signer listens", async || {
        let request = client.request(probe.clone(), Default::default());
        tokio::time::timeout(Duration::from_millis(200), request).await.is_err()
    })
    .await;
}
