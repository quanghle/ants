//! [`AdminOp::ReadMcp`] and [`AdminOp::McpSignIn`]: the MCP servers of the ant's harness ([`McpReport`]) and their
//! OAuth sign-in ([`McpSignIn`]).
//!
//! The daemon serving the ant's inbox carries them out ([`serve`]) with a session of the ant's harness opened for them
//! alone ([`crate::harness::open_mcp`]), whatever its threads do: started with the ant's environment and settings, the
//! MCP servers its sessions get, and stopped once read. A sign-in keeps its session until the browser came back, or
//! for [`MCP_SIGN_IN_FOR`]: to the harness's loopback listener when the browser runs on the ant's machine, else as the
//! address it came back to, handed over by a sign-in order with a `callback`. The harness keeps what the sign-in got
//! where it keeps its credentials, for every session of it to come.
//!
//! An order that waited longer than [`MCP_FRESH`] in the stream is refused as stale; one for an ant whose inbox the
//! daemon does not serve now, as while it connects, is refused.

use std::collections::HashMap;
use std::time::Duration;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{
    AdminOrder, AdminOutcome, MCP_FRESH, MCP_SIGN_IN_FOR, McpReport, McpServerState, McpSignIn, McpStatus, UserId,
};
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};

use super::{Orders, applied, refused};
use crate::harness::{Launch, McpHost, open_mcp, unreported_mcp};

/// How long the servers have to start before they are read.
const SETTLE: Duration = Duration::from_secs(15);

/// How long a server has to connect once the browser came back.
const CONNECT: Duration = Duration::from_secs(10);

/// How often a sign-in waiting for the browser checks whether it came back to the loopback listener.
const SIGNED_IN_POLL: Duration = Duration::from_secs(2);

/// The most reads under way at once.
const MAX_READS: usize = 4;

/// An MCP order asked of the daemon serving an ant.
#[derive(Debug)]
pub enum Request {
    Read(oneshot::Sender<Result<McpReport>>),
    SignIn { server: String, callback: Option<String>, done: oneshot::Sender<Result<McpSignIn>> },
}

/// The MCP orders of an ant, heard while its daemon serves its inbox
/// ([`crate::admin::restart::Restarts::listen_mcp`]).
pub struct Listening {
    pub(crate) restarts: super::restart::Restarts,
    pub(crate) ant: UserId,
    pub(crate) tx: mpsc::UnboundedSender<Request>,
    pub(crate) rx: mpsc::UnboundedReceiver<Request>,
}

impl Listening {
    async fn next(&mut self) -> Request {
        match self.rx.recv().await {
            Some(request) => request,
            None => std::future::pending().await,
        }
    }
}

pub(super) async fn apply(
    orders: &Orders,
    order: &AdminOrder,
    sign_in: Option<(&str, Option<&str>)>,
    stored_at: DateTime<Utc>,
) -> AdminOutcome {
    let waited = Utc::now() - stored_at;
    if waited > MCP_FRESH {
        return refused(
            order,
            &format!(
                "stale: it waited {}s for the machine's daemon, which reads MCP servers only while online; ask again",
                waited.num_seconds()
            ),
        );
    }
    let answer = match sign_in {
        None => ask(orders, order, Request::Read).await,
        Some((server, callback)) => {
            let (server, callback) = (server.to_owned(), callback.map(str::to_owned));
            ask(orders, order, |done| Request::SignIn { server, callback, done }).await
        }
    };
    match answer {
        Ok(result) => applied(order, Some(result)),
        Err(reason) => refused(order, &reason),
    }
}

/// Ask the daemon serving the ant of `order`, the result as JSON or the reason for refusing it.
async fn ask<T: serde::Serialize>(
    orders: &Orders,
    order: &AdminOrder,
    request: impl FnOnce(oneshot::Sender<Result<T>>) -> Request,
) -> Result<serde_json::Value, String> {
    let (done, answer) = oneshot::channel();
    match orders.restarts.mcp(&order.ant, request(done)) {
        None => return Err(format!("machine {} serves no ant {}", orders.machine, order.ant)),
        Some(false) => return Err(format!("ant {} serves no inbox now; ask again once it is online", order.ant)),
        Some(true) => {}
    }
    match answer.await {
        Ok(Ok(result)) => serde_json::to_value(result).map_err(|e| format!("writing the result: {e}")),
        Ok(Err(e)) => Err(format!("{e:#}")),
        Err(_) => Err(format!("ant {} stopped serving its inbox; ask again", order.ant)),
    }
}

/// Carry out the MCP orders `listening` hears with sessions of the harness `launch` gives, for as long as the daemon
/// serves the ant's inbox.
pub async fn serve(mut listening: Listening, launch: impl Fn() -> Launch) {
    let mut reads = tokio::task::JoinSet::new();
    let mut signing: HashMap<String, Signing> = HashMap::new();
    loop {
        let request = tokio::select! {
            request = listening.next() => request,
            Some(_) = reads.join_next() => continue,
        };
        signing.retain(|_, signing| !signing.task.is_finished());
        match request {
            Request::Read(done) if reads.len() >= MAX_READS => {
                let _ = done.send(Err(anyhow!("{MAX_READS} reads of the MCP servers are under way; ask again")));
            }
            Request::Read(mut done) => {
                let launch = launch();
                reads.spawn(async move {
                    tokio::select! {
                        read = read(&launch) => {
                            let _ = done.send(read);
                        }
                        () = done.closed() => {}
                    }
                });
            }
            Request::SignIn { server, callback: Some(callback), done } => match signing.get(&server) {
                Some(waiting) => {
                    if let Err(mpsc::error::SendError((_, done))) = waiting.callbacks.send((callback, done)) {
                        let _ = done.send(Err(anyhow!("the sign-in to {server} ended; sign in again")));
                    }
                }
                None => {
                    let _ = done.send(Err(anyhow!(
                        "no sign-in to {server} waits for the browser: sign in first, or read the servers, as the \
                         sign-in may have ended once the browser came back"
                    )));
                }
            },
            Request::SignIn { server, callback: None, done } => {
                let (callbacks, heard) = mpsc::unbounded_channel();
                let task = tokio::spawn(sign_in(launch(), server.clone(), done, heard));
                signing.insert(server, Signing { callbacks, task: Aborting(task) });
            }
        }
    }
}

/// A sign-in waiting for the browser: where the addresses it came back to go.
struct Signing {
    callbacks: mpsc::UnboundedSender<Callback>,
    task: Aborting,
}

type Callback = (String, oneshot::Sender<Result<McpSignIn>>);

/// A task aborted once dropped, with the harness it holds.
struct Aborting(tokio::task::JoinHandle<()>);

impl Aborting {
    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl Drop for Aborting {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn read(launch: &Launch) -> Result<McpReport> {
    let Some(host) = open_mcp(launch).await? else { return Ok(unreported_mcp(launch)) };
    let report = host.report(SETTLE).await;
    host.close().await;
    report
}

/// Sign in to `server` with a session of `launch`'s harness, answering `done` with where to, and keep the session
/// while the browser has not come back, to the loopback or as one of `callbacks`.
async fn sign_in(
    launch: Launch,
    server: String,
    done: oneshot::Sender<Result<McpSignIn>>,
    mut callbacks: mpsc::UnboundedReceiver<Callback>,
) {
    let host = match open_mcp(&launch).await {
        Ok(Some(host)) => host,
        Ok(None) => {
            let report = unreported_mcp(&launch);
            let why =
                format!("harness {} (protocol {}) reports no MCP servers to sign in", report.harness, report.protocol);
            let _ = done.send(Err(anyhow!(why)));
            return;
        }
        Err(e) => {
            let _ = done.send(Err(e));
            return;
        }
    };
    let started = start(&host, &server).await;
    let waiting = matches!(&started, Ok(signing) if signing.authorization_url.is_some());
    tracing::info!(
        server,
        waiting,
        error = started.as_ref().err().map(|e| format!("{e:#}")),
        "signing in to an MCP server"
    );
    let _ = done.send(started);
    if waiting {
        wait(&host, &server, &mut callbacks).await;
    }
    host.close().await;
}

/// Start the sign-in to `server`, telling where to open in a browser if the harness asks.
async fn start(host: &McpHost, server: &str) -> Result<McpSignIn> {
    let state = state(&host.settled(SETTLE).await?, server)?;
    if state.status == McpStatus::Connected {
        return Ok(McpSignIn { server: state, authorization_url: None, expires_at: None });
    }
    match host.sign_in(server).await? {
        Some(url) => {
            let expires_at = Some(Utc::now() + MCP_SIGN_IN_FOR);
            Ok(McpSignIn { server: state, authorization_url: Some(url), expires_at })
        }
        None => {
            Ok(McpSignIn { server: connected(host, server, CONNECT).await?, authorization_url: None, expires_at: None })
        }
    }
}

/// Wait for the browser to come back from signing in to `server`, for [`MCP_SIGN_IN_FOR`].
async fn wait(host: &McpHost, server: &str, callbacks: &mut mpsc::UnboundedReceiver<Callback>) {
    let until = tokio::time::sleep(MCP_SIGN_IN_FOR.to_std().unwrap_or_default());
    tokio::pin!(until);
    loop {
        tokio::select! {
            () = &mut until => {
                tracing::info!(server, "the browser did not come back from signing in to an MCP server");
                return;
            }
            Some((callback, done)) = callbacks.recv() => {
                let signed_in = async {
                    host.callback(server, &callback).await?;
                    connected(host, server, CONNECT).await
                };
                let signed_in = signed_in.await.map(|state| McpSignIn { server: state, authorization_url: None, expires_at: None });
                let finished = matches!(&signed_in, Ok(signing) if signing.server.status == McpStatus::Connected);
                let _ = done.send(signed_in);
                if finished {
                    return;
                }
            }
            () = tokio::time::sleep(SIGNED_IN_POLL) => {
                if host.servers().await.is_ok_and(|servers| is_connected(&servers, server)) {
                    tracing::info!(server, "signed in to an MCP server");
                    return;
                }
            }
        }
    }
}

/// `server`, once connected or `within` passed.
async fn connected(host: &McpHost, server: &str, within: Duration) -> Result<McpServerState> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let servers = host.servers().await?;
        if is_connected(&servers, server) || tokio::time::Instant::now() >= deadline {
            return state(&servers, server);
        }
        tokio::time::sleep(SIGNED_IN_POLL.min(within)).await;
    }
}

fn is_connected(servers: &[McpServerState], server: &str) -> bool {
    servers.iter().any(|s| s.name == server && s.status == McpStatus::Connected)
}

fn state(servers: &[McpServerState], server: &str) -> Result<McpServerState> {
    match servers.iter().find(|s| s.name == server) {
        Some(state) => Ok(state.clone()),
        None if servers.is_empty() => bail!("the harness reports no MCP server {server}, nor any other"),
        None => {
            let names = servers.iter().map(|s| s.name.as_str()).collect::<Vec<_>>().join(", ");
            bail!("the harness reports no MCP server {server}, only {names}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::restart::Restarts;
    use crate::harness::{Harnesses, ModelSettings};
    use crate::permission::{Allowlist, PermissionPolicy};

    #[tokio::test]
    async fn mcp_orders_reach_the_daemon_serving_the_ant_s_inbox() {
        let (restarts, dev_a): (Restarts, UserId) = (Restarts::default(), "dev-a".parse().unwrap());
        let read = || {
            let (done, answer) = oneshot::channel();
            (Request::Read(done), answer)
        };
        assert_eq!(restarts.mcp(&dev_a, read().0), None, "not served");
        let _served = restarts.serve(&dev_a);
        assert_eq!(restarts.mcp(&dev_a, read().0), Some(false), "no inbox served");

        let policy = PermissionPolicy::new(Allowlist::parse(&["*"]).unwrap(), None);
        let harness = Harnesses::builtin().get("copilot").unwrap();
        let launch = Launch::new(harness, std::env::temp_dir(), vec![], policy, ModelSettings::default());
        let serving = tokio::spawn(serve(restarts.listen_mcp(&dev_a), move || launch.clone()));
        let (request, answer) = read();
        assert_eq!(restarts.mcp(&dev_a, request), Some(true));
        let report = answer.await.unwrap().unwrap();
        assert_eq!((report.harness.as_str(), report.protocol.as_str(), report.servers), ("copilot", "acp", None));

        let sign_in = |callback: Option<&str>| {
            let (done, answer) = oneshot::channel();
            let callback = callback.map(str::to_owned);
            assert_eq!(
                restarts.mcp(&dev_a, Request::SignIn { server: "github".to_owned(), callback, done }),
                Some(true)
            );
            answer
        };
        let early = sign_in(Some("http://127.0.0.1:1/")).await.unwrap().unwrap_err();
        assert!(format!("{early}").contains("sign in first"), "{early}");
        let unreported = sign_in(None).await.unwrap().unwrap_err();
        assert!(format!("{unreported}").contains("reports no MCP servers"), "{unreported}");

        serving.abort();
        let _ = serving.await;
        assert_eq!(restarts.mcp(&dev_a, read().0), Some(false), "no longer heard");
    }
}
