//! The two ways into the MCP server of an ant: streamable HTTP on a loopback port, each request carrying the bearer
//! token of its session, and a local socket whose connections start with the token, which `ants mcp` bridges stdio
//! to ([`bridge_stdio`]).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::ThreadId;
use anyhow::{Context as _, Result};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::http::{Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use rmcp::ServiceExt as _;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpListener;

use super::Hub;
use super::tools::Tools;
use crate::ipc;

/// Bridge stdin and stdout to the MCP socket of a daemon, as `ants mcp` does for harnesses without HTTP MCP. Ends when
/// the daemon closes the connection.
pub async fn bridge_stdio(socket: &Path, token: &str) -> Result<()> {
    let stream = ipc::connect(socket).await.with_context(|| format!("connecting to {}", socket.display()))?;
    let (mut from_daemon, mut to_daemon) = tokio::io::split(stream);
    to_daemon.write_all(format!("{token}\n").as_bytes()).await?;
    let up = async {
        tokio::io::copy(&mut tokio::io::stdin(), &mut to_daemon).await?;
        if !ipc::HALF_CLOSE {
            return Ok(());
        }
        // The harness closed stdin: let the daemon finish and close its side.
        to_daemon.shutdown().await?;
        std::future::pending::<std::io::Result<()>>().await
    };
    let down = async {
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut from_daemon, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::select! {
        r = up => r?,
        r = down => r?,
    }
    Ok(())
}

/// The thread an HTTP request comes from, after its token checked out.
#[derive(Clone)]
pub(super) struct Caller(pub(super) ThreadId);

pub(super) async fn serve_http(listener: TcpListener, hub: Arc<Hub>) {
    let factory_hub = hub.clone();
    let service = StreamableHttpService::new(
        move || Ok(Tools::new(factory_hub.clone(), None)),
        Arc::new(NeverSessionManager::default()),
        StreamableHttpServerConfig::default().with_legacy_session_mode(false),
    );
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!(error = %e, "accepting an MCP connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (service, hub) = (service.clone(), hub.clone());
        tokio::spawn(async move {
            let handle = hyper::service::service_fn(move |mut request: Request<Incoming>| {
                let mut service = service.clone();
                let caller = bearer(&request).and_then(|token| hub.caller(token));
                async move {
                    let Some(thread) = caller else {
                        let body = Full::new(Bytes::from_static(b"unknown or missing bearer token")).boxed();
                        let mut response = Response::new(body);
                        *response.status_mut() = StatusCode::UNAUTHORIZED;
                        return Ok(response);
                    };
                    request.extensions_mut().insert(Caller(thread));
                    tower_service::Service::call(&mut service, request).await
                }
            });
            if let Err(e) =
                hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), handle).await
            {
                tracing::debug!(error = %e, "MCP connection");
            }
        });
    }
}

fn bearer<B>(request: &Request<B>) -> Option<&str> {
    request.headers().get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")
}

/// Each connection starts with the session's token on a line of its own, then carries MCP as JSON lines.
pub(super) async fn serve_socket(mut listener: ipc::Listener, hub: Arc<Hub>) {
    loop {
        let stream = match listener.accept().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::warn!(error = %e, "accepting an MCP socket connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let hub = hub.clone();
        tokio::spawn(async move {
            let (read, write) = tokio::io::split(stream);
            let mut read = BufReader::new(read);
            let mut line = String::new();
            if (&mut read).take(256).read_line(&mut line).await.is_err() {
                return;
            }
            let Some(thread) = hub.caller(line.trim_end()) else {
                tracing::warn!("MCP socket connection with an unknown token");
                return;
            };
            match Tools::new(hub, Some(thread)).serve((read, write)).await {
                Ok(service) => {
                    let _ = service.waiting().await;
                }
                Err(e) => tracing::debug!(error = %e, "MCP socket session"),
            }
        });
    }
}
