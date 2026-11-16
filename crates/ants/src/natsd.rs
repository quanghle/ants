//! The nats-server `ants daemon` starts and supervises itself when the config file has a `[nats_server]` table: one
//! child process running an existing configuration, owned through a lock file beside that configuration, ready before
//! the daemon connects, started again when it exits unasked, and stopped once the ants are drained ([`start`],
//! [`run_serving`]). Leaf node reloads reach it through [`apply`]. Unix only.

use std::path::Path;
use std::time::Duration;

use crate::service::{Action, Outcome, Service, Services};

#[cfg(unix)]
mod managed;

#[cfg(unix)]
pub use managed::{run_serving, start};

/// The timing of a managed nats-server's supervision.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(unix), allow(dead_code))]
pub struct Policy {
    /// The wait before restarting the server after an exit, doubled for each restart in a row.
    pub backoff: Duration,
    /// The longest wait before a restart.
    pub max_backoff: Duration,
    /// How long the server runs for the restarts in a row to count from 0 again.
    pub healthy: Duration,
    /// The restarts in a row, without a run of [`Policy::healthy`], after which supervision gives up.
    pub max_restarts: u32,
    /// How long a stop waits for the server to exit after SIGTERM before SIGKILL.
    pub stop_grace: Duration,
    /// The same, for a server a previous daemon left running.
    pub orphan_grace: Duration,
    /// How often readiness is checked while the server starts.
    pub probe_every: Duration,
}

impl Policy {
    /// Restarts after 1s, 2s, 4s … up to 30s; 5 in a row without a minute's run and supervision gives up.
    pub const DEFAULT: Policy = Policy {
        backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(30),
        healthy: Duration::from_mins(1),
        max_restarts: 5,
        stop_grace: Duration::from_secs(30),
        orphan_grace: Duration::from_secs(10),
        probe_every: Duration::from_millis(200),
    };

    /// The wait before restart number `n` (from 1) in a row.
    #[cfg(unix)]
    fn backoff(&self, n: u32) -> Duration {
        self.backoff.saturating_mul(1 << n.saturating_sub(1).min(20)).min(self.max_backoff)
    }
}

/// The managed nats-server of a platform without one.
#[cfg(not(unix))]
pub enum Managed {}

#[cfg(not(unix))]
pub async fn start(
    _settings: &ants_daemon::config::NatsServer,
    _nats_url: &str,
    _policy: Policy,
    _stop: impl Future<Output = ()>,
) -> anyhow::Result<Option<Managed>> {
    anyhow::bail!(
        "[nats_server] is supported on macOS and Linux only: remove it from the config file and run nats-server as a \
         service of its own"
    )
}

#[cfg(not(unix))]
pub async fn run_serving<T>(managed: Managed, _serving: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    match managed {}
}

/// `action` on the leaf node or hub running the configuration `conf`: on the nats-server an `ants daemon` runs from it
/// ([`start`]) when it holds that configuration's lock, the server running; on the service running it otherwise
/// ([`Services::apply`]). A reload is SIGHUP; a restart is SIGTERM, after which the daemon starts the server again.
pub fn apply(conf: &Path, action: Action) -> Outcome {
    #[cfg(unix)]
    if let Some(pid) = managed::server_of(conf) {
        return managed::signal_server(pid, action);
    }
    Services::local().apply(Service::Nats, action, conf)
}

#[cfg(all(test, unix))]
mod tests;
