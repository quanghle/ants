//! The MCP servers of an ant's harness, read from a session opened for them alone: the harness starts with the ant's
//! environment and settings, opens a session with the MCP servers the ant's sessions get, reports how each stands and
//! signs them in with OAuth, and stops once closed. Only native protocols reporting them do: not ACP's, nor
//! Antigravity's.

use std::collections::HashMap;
use std::time::Duration;

use agent_client_protocol::Error;
use ants_proto::{MCP_SERVERS_CAP, McpReport, McpServerState, McpStatus};
use anyhow::{Context as _, anyhow, bail, ensure};
use chrono::Utc;

use super::Launch;
use super::native::{self, is_loopback};
use super::sign_in::HANDSHAKE_TIMEOUT;
use crate::sync::MutexExt as _;

/// The longest a request to the harness about its MCP servers may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the servers are read while some still start.
const SETTLE_POLL: Duration = Duration::from_millis(500);

/// The report of `launch`'s harness when its protocol reports no MCP servers.
pub fn unreported_mcp(launch: &Launch) -> McpReport {
    McpReport {
        harness: launch.harness.name.clone(),
        protocol: launch.harness.protocol.name().to_owned(),
        servers: None,
        truncated: false,
        read_at: Utc::now(),
    }
}

/// A session of `launch`'s harness held open for its MCP servers, with the servers the sessions of `launch`'s thread
/// get; None when the harness's protocol reports none, or runs in-process.
pub async fn open_mcp(launch: &Launch) -> anyhow::Result<Option<McpHost>> {
    let Some(env) = &launch.process_env else { return Ok(None) };
    let servers_of = launch.clone();
    let servers: native::ServersFor = Box::new(move |http| servers_of.mcp_servers(http));
    let Some(open) = native::mcp_host(&launch.harness, env, &launch.settings, launch.cwd.clone(), servers) else {
        return Ok(None);
    };
    let host = tokio::time::timeout(HANDSHAKE_TIMEOUT, open)
        .await
        .map_err(|_| anyhow!("{} opened no session within {HANDSHAKE_TIMEOUT:?}", launch.harness.name))?
        .map_err(|e| failure(&e))?;
    Ok(Some(McpHost { host, report: unreported_mcp(launch), redirects: std::sync::Mutex::default() }))
}

/// A session held open for its MCP servers ([`open_mcp`]); the harness stops once it is closed and dropped.
pub struct McpHost {
    host: Box<dyn native::McpHost>,
    report: McpReport,
    /// Where the browser of each sign-in started comes back to, by server.
    redirects: std::sync::Mutex<HashMap<String, Option<Redirect>>>,
}

impl McpHost {
    /// The servers, once none is starting or `within` passed.
    pub async fn settled(&self, within: Duration) -> anyhow::Result<Vec<McpServerState>> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let servers = self.servers().await?;
            if !servers.iter().any(|s| s.status == McpStatus::Pending) || tokio::time::Instant::now() >= deadline {
                return Ok(servers);
            }
            tokio::time::sleep(SETTLE_POLL).await;
        }
    }

    /// The servers and how each stands, as reported once settled within `within`.
    pub async fn report(&self, within: Duration) -> anyhow::Result<McpReport> {
        let mut servers = self.settled(within).await?;
        let truncated = servers.len() > MCP_SERVERS_CAP;
        servers.truncate(MCP_SERVERS_CAP);
        Ok(McpReport { servers: Some(servers), truncated, read_at: Utc::now(), ..self.report.clone() })
    }

    /// The servers and how each stands now.
    pub async fn servers(&self) -> anyhow::Result<Vec<McpServerState>> {
        call(self.host.servers()).await
    }

    /// Start signing in to `server`: the address to open in a browser, or none when it needs no sign-in.
    pub async fn sign_in(&self, server: &str) -> anyhow::Result<Option<String>> {
        let url = call(self.host.sign_in(server)).await?;
        let redirect = url.as_deref().and_then(redirect_of);
        self.redirects.locked().insert(server.to_owned(), redirect);
        Ok(url)
    }

    /// Hand the harness `callback`, where the browser came back to after the sign-in to `server` started: an address
    /// on this machine's loopback, on the listener of the sign-in.
    pub async fn callback(&self, server: &str, callback: &str) -> anyhow::Result<()> {
        let Some(redirect) = self.redirects.locked().get(server).cloned() else {
            bail!("no sign-in to {server} waits for the browser");
        };
        let came_back = Redirect::parse(callback).context("the callback address")?;
        ensure!(is_loopback(&came_back.host), "the callback address {callback} is not on the loopback");
        if let Some(redirect) = redirect {
            ensure!(came_back.matches(&redirect), "the callback address {callback} is not where the sign-in returns");
        }
        call(self.host.callback(server, callback)).await
    }

    pub async fn close(self) {
        if tokio::time::timeout(CALL_TIMEOUT, self.host.close()).await.is_err() {
            tracing::warn!(harness = self.report.harness, "closing a session read for its MCP servers timed out");
        }
    }
}

async fn call<T>(request: impl Future<Output = Result<T, Error>>) -> anyhow::Result<T> {
    match tokio::time::timeout(CALL_TIMEOUT, request).await {
        Ok(done) => done.map_err(|e| failure(&e)),
        Err(_) => bail!("the harness did not answer within {CALL_TIMEOUT:?}"),
    }
}

fn failure(e: &Error) -> anyhow::Error {
    match &e.data {
        Some(data) => anyhow!("{}: {data}", e.message),
        None => anyhow!("{}", e.message),
    }
}

/// An `http` address's host, port and path.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Redirect {
    host: String,
    port: u16,
    path: String,
}

impl Redirect {
    fn parse(url: &str) -> anyhow::Result<Self> {
        let uri: hyper::Uri = url.parse()?;
        ensure!(uri.scheme_str() == Some("http"), "{url} is not an http address");
        let host = uri.host().context("no host")?.trim_start_matches('[').trim_end_matches(']').to_ascii_lowercase();
        Ok(Self { host, port: uri.port_u16().unwrap_or(80), path: uri.path().to_owned() })
    }

    /// Whether an address the browser came back to is on the listener of `redirect`: its port, and its path or one
    /// below it. `localhost` and the loopback addresses are told apart only by the port.
    fn matches(&self, redirect: &Self) -> bool {
        let under = self.path.strip_prefix(redirect.path.trim_end_matches('/'));
        self.port == redirect.port && under.is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    }
}

/// Where the browser comes back to from the sign-in at `url`: its `redirect_uri`, if on the loopback.
fn redirect_of(url: &str) -> Option<Redirect> {
    let query = url.split_once('?')?.1.split('#').next()?;
    let value = query.split('&').find_map(|pair| pair.strip_prefix("redirect_uri="))?;
    let redirect = Redirect::parse(&percent_decoded(value)?).ok()?;
    is_loopback(&redirect.host).then_some(redirect)
}

/// A query value decoded: `%XX` escapes and `+` for space.
fn percent_decoded(value: &str) -> Option<String> {
    let (bytes, mut decoded) = (value.as_bytes(), Vec::with_capacity(value.len()));
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
                decoded.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' => {
                decoded.push(b' ');
                i += 1;
            }
            byte => {
                decoded.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_callback_must_come_back_to_the_listener_of_the_sign_in() {
        let url = "https://auth.example/authorize?client_id=c&redirect_uri=http%3A%2F%2F127.0.0.1%3A5123%2Fcallback%2Fx1&state=s";
        let redirect = redirect_of(url).unwrap();
        assert_eq!(redirect, Redirect { host: "127.0.0.1".to_owned(), port: 5123, path: "/callback/x1".to_owned() });
        let came_back = |callback: &str| Redirect::parse(callback).unwrap().matches(&redirect);
        assert!(came_back("http://127.0.0.1:5123/callback/x1?code=a&state=s"));
        assert!(came_back("http://localhost:5123/callback/x1?code=a"));
        assert!(!came_back("http://127.0.0.1:5124/callback/x1?code=a"));
        assert!(!came_back("http://127.0.0.1:5123/callback/x10?code=a"));
        assert!(!came_back("http://127.0.0.1:5123/admin"));
        let root = redirect_of("https://a/x?redirect_uri=http://127.0.0.1:9/").unwrap();
        assert!(Redirect::parse("http://127.0.0.1:9/?code=a").unwrap().matches(&root));
        assert_eq!(redirect_of("https://a/x?redirect_uri=https%3A%2F%2Fexample.com%2Fcb"), None);
        assert!(Redirect::parse("https://127.0.0.1:9/").is_err());
    }
}
