//! `ants bridge`: carry NATS connections over WebSocket.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Subcommand;

use crate::node::bridge;
use crate::release::upgrade;
use crate::serve::serve_metrics;

#[derive(Subcommand, Debug)]
pub(crate) enum BridgeCommand {
    /// On the hub, behind the TLS-terminating reverse proxy: open a connection to the upstream (the hub's leafnode
    /// port) for each session `ants bridge connect` starts.
    Serve {
        /// Address for the reverse proxy.
        #[arg(long, default_value = bridge::DEFAULT_SERVE_LISTEN)]
        listen: String,
        /// Where sessions connect to: the hub's leafnode port.
        #[arg(long, default_value = bridge::DEFAULT_UPSTREAM)]
        upstream: String,
        /// Bearer token every request must carry. Without it, anyone reaching the bridge may open sessions, which
        /// NATS authentication then checks as for any leaf node.
        #[arg(long, env = bridge::TOKEN_ENV, hide_env_values = true)]
        token: Option<String>,
        /// File holding the bearer token, read at start, instead of --token or ANTS_BRIDGE_TOKEN: keeps it out of the
        /// command line and environment.
        #[arg(long, value_name = "FILE", conflicts_with = "token")]
        token_file: Option<PathBuf>,
        /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464
        /// (default: off).
        #[arg(long, value_name = "ADDR")]
        metrics: Option<SocketAddr>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
    /// On a worker: listen locally and carry each connection to the hub's `ants bridge serve`. The machine's leaf
    /// node then uses the uplink nats-leaf://127.0.0.1:7422.
    #[expect(rustdoc::broken_intra_doc_links, reason = "clap help text: brackets mark optional parts")]
    Connect {
        /// The bridge's URL, https://host[:port]/path, as the hub's reverse proxy mounts it.
        url: String,
        /// Local address for the leaf node's uplink; loopback only.
        #[arg(long, default_value = bridge::DEFAULT_CONNECT_LISTEN)]
        listen: String,
        /// Bearer token, as given to `ants bridge serve`.
        #[arg(long, env = bridge::TOKEN_ENV, hide_env_values = true)]
        token: Option<String>,
        /// File holding the bearer token, read at start, instead of --token or ANTS_BRIDGE_TOKEN: keeps it out of the
        /// command line and environment.
        #[arg(long, value_name = "FILE", conflicts_with = "token")]
        token_file: Option<PathBuf>,
        /// Extra request header, `Name: value` (repeatable), for a gateway in front of the hub.
        #[arg(long = "header", value_name = "HEADER")]
        headers: Vec<String>,
        /// HTTP CONNECT forward proxy, http://[user:password@]host:port.
        #[arg(long, value_name = "URL")]
        proxy: Option<String>,
        /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464
        /// (default: off).
        #[arg(long, value_name = "ADDR")]
        metrics: Option<SocketAddr>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
}

pub(crate) async fn run(command: BridgeCommand) -> Result<()> {
    match command {
        BridgeCommand::Serve { listen, upstream, token, token_file, metrics, upgrade } => {
            let token = bridge::token(token, token_file.as_deref())?;
            let addr = bridge::listen_addr(&listen, bridge::Listen::Any)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            let server = bridge::Server::new(upstream.clone(), token);
            let _metrics = serve_metrics(metrics, server.traffic()).await?;
            tracing::info!("bridge on http://{addr}, sessions connect to {upstream}");
            upgrade::or_exit(server.run(listener), &upgrade).await
        }
        BridgeCommand::Connect { url, listen, token, token_file, headers, proxy, metrics, upgrade } => {
            let token = bridge::token(token, token_file.as_deref())?;
            let remote = bridge::Remote::new(&url, token.as_deref(), &headers, proxy.as_deref())?;
            let addr = bridge::listen_addr(&listen, bridge::Listen::LoopbackOnly)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            let _metrics = serve_metrics(metrics, remote.traffic()).await?;
            tracing::info!("bridging {addr} to {url}; leaf uplink: nats-leaf://{addr}");
            upgrade::or_exit(remote.run(listener), &upgrade).await
        }
    }
}
