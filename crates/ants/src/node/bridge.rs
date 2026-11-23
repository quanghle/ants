//! `ants bridge`: carry a TCP connection, a leaf node's uplink, over plain HTTP(S) requests, for networks whose
//! proxies block WebSocket.
//!
//! The worker runs `ants bridge connect <url>`, which listens on `127.0.0.1:7422`, so its leaf node's uplink is
//! `nats-leaf://127.0.0.1:7422`, as with `devtunnel connect`. Each local connection becomes a session of
//! `ants bridge serve` on the hub, behind its TLS-terminating reverse proxy, which opens a TCP connection to the hub's
//! leafnode port. Bytes go up in POST bodies and come down in long-polled GET responses, each request tagged with the
//! byte offset it starts at, so a request lost or repeated on the way is retried without losing or doubling bytes.
//! NATS authentication runs inside the stream unchanged; an optional bearer token keeps strangers from opening
//! sessions at all.
//!
//! Requests, under the URL's path:
//! - `POST <path>/open`: connects to the upstream, answers the new session's id.
//! - `POST <path>/s/<id>?off=N`: the body continues the upstream bytes at offset N; 204.
//! - `GET <path>/s/<id>?off=N`: everything before N arrived; answers the downstream bytes from N, waiting up to
//!   [`POLL`] for some (an empty body when none came), or 410 once the upstream closed and all of it was taken.
//! - `DELETE <path>/s/<id>`: closes the session.

mod connect;
mod serve;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ants_daemon::monitoring::metrics::{self, Exposition, Kind};
use anyhow::{Context as _, Result, bail};

pub use connect::{Carried, Probe, Remote};
pub use serve::Server;

/// Where `ants bridge connect` listens: where a leaf node's `nats-leaf://` uplink goes by default.
pub const DEFAULT_CONNECT_LISTEN: &str = "127.0.0.1:7422";
/// Where `ants bridge serve` listens, for the reverse proxy in front of it.
pub const DEFAULT_SERVE_LISTEN: &str = "127.0.0.1:8423";
/// The hub's leafnode port.
pub const DEFAULT_UPSTREAM: &str = "127.0.0.1:7422";
/// Environment variable of the bearer token both ends may share.
pub const TOKEN_ENV: &str = "ANTS_BRIDGE_TOKEN";

/// How long a GET waits for downstream bytes; below the idle timeouts of common proxies (60 s).
const POLL: Duration = Duration::from_secs(20);
/// A session without requests for this long is closed. The worker gives up retrying after as long.
const IDLE: Duration = Duration::from_secs(60);
/// The most bytes in one request or response body.
const CHUNK: usize = 128 * 1024;
/// Time for one request, apart from a GET's wait.
const REQUEST: Duration = Duration::from_secs(30);

/// What either end of the bridge carried, for `--metrics`.
#[derive(Debug, Default)]
pub struct Traffic {
    sessions: AtomicU64,
    opened: AtomicU64,
    failed: AtomicU64,
    up: AtomicU64,
    down: AtomicU64,
}

impl Traffic {
    fn opened(&self) {
        self.opened.fetch_add(1, Ordering::Relaxed);
        self.sessions.fetch_add(1, Ordering::Relaxed);
    }

    fn closed(&self) {
        self.sessions.fetch_sub(1, Ordering::Relaxed);
    }

    fn failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    fn up(&self, n: usize) {
        self.up.fetch_add(n as u64, Ordering::Relaxed);
    }

    fn down(&self, n: usize) {
        self.down.fetch_add(n as u64, Ordering::Relaxed);
    }
}

/// The bridge has no NATS connection of its own: healthy while it runs.
impl metrics::Source for Traffic {
    fn healthy(&self) -> bool {
        true
    }

    fn render(&self, out: &mut Exposition) {
        let load = |n: &AtomicU64| n.load(Ordering::Relaxed) as f64;
        out.single("ants_bridge_sessions", Kind::Gauge, "Bridge sessions open.", load(&self.sessions));
        out.single("ants_bridge_sessions_total", Kind::Counter, "Bridge sessions opened.", load(&self.opened));
        out.single(
            "ants_bridge_failures_total",
            Kind::Counter,
            "Sessions that failed: the upstream unreachable (serve), or the session not opened or broken (connect).",
            load(&self.failed),
        );
        out.family(
            "ants_bridge_bytes_total",
            Kind::Counter,
            "Bytes carried, up towards the hub's upstream or down from it.",
        );
        out.sample("ants_bridge_bytes_total", &[("direction", "up")], load(&self.up));
        out.sample("ants_bridge_bytes_total", &[("direction", "down")], load(&self.down));
    }
}

/// The bearer token: `token` (from --token or [`TOKEN_ENV`]), else the content of `file`, trimmed, which must not be
/// empty.
pub fn token(token: Option<String>, file: Option<&Path>) -> Result<Option<String>> {
    let Some(file) = file else { return Ok(token) };
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading the bridge token from {}", file.display()))?;
    let token = text.trim();
    if token.is_empty() {
        bail!("{}: no bridge token in the file", file.display());
    }
    Ok(Some(token.to_owned()))
}

/// Which addresses [`listen_addr`] takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Listen {
    Any,
    LoopbackOnly,
}

/// `addr` parsed, and checked to be loopback with [`Listen::LoopbackOnly`].
pub fn listen_addr(addr: &str, listen: Listen) -> Result<SocketAddr> {
    let parsed: SocketAddr = addr.parse().with_context(|| format!("--listen {addr}: expected host:port"))?;
    if listen == Listen::LoopbackOnly && !parsed.ip().is_loopback() {
        bail!("--listen {addr}: the bridge serves this machine only; use a loopback address");
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests;
