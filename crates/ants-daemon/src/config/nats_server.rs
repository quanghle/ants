//! The `[nats_server]` table of the config file: the local nats-server `ants daemon` starts and supervises itself,
//! in place of a separately installed service ([`NatsServer`]).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Result, bail};
use serde::Deserialize;

use super::resolve;

/// How long [`NatsServer::ready_timeout`] is when the table sets none.
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The `[nats_server]` table, as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NatsServerSection {
    command: Option<PathBuf>,
    config: PathBuf,
    #[serde(default, deserialize_with = "optional_duration")]
    ready_timeout: Option<Duration>,
}

fn optional_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    ants_proto::duration::deserialize(d).map(Some)
}

/// The nats-server a daemon serving the whole config file starts before connecting its ants and stops after they
/// finished: one process, running an existing nats-server configuration (the machine's leaf node, or the hub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsServer {
    /// The nats-server executable: a path, resolved like the file's other paths, or a bare name looked up on PATH;
    /// the default `nats-server` is looked up beside the ants executable, then in `~/.ants/bin`, then on PATH.
    pub command: PathBuf,
    /// Its configuration file, run as `nats-server -c FILE`, resolved like the file's other paths.
    pub config: PathBuf,
    /// How long to wait for it to accept NATS connections once started (default [`DEFAULT_READY_TIMEOUT`]).
    pub ready_timeout: Duration,
}

impl NatsServerSection {
    /// The table with its paths resolved against `base` and `home`, checked.
    pub(super) fn settle(self, base: &Path, home: &Path) -> Result<NatsServer> {
        let command = match self.command {
            Some(command) if command.as_os_str().is_empty() => bail!("[nats_server] command is empty"),
            Some(command) if command.components().count() > 1 => resolve(&command, base, home),
            Some(command) => command,
            None => PathBuf::from("nats-server"),
        };
        if self.config.as_os_str().is_empty() {
            bail!("[nats_server] config is empty");
        }
        let ready_timeout = self.ready_timeout.unwrap_or(DEFAULT_READY_TIMEOUT);
        if ready_timeout.is_zero() {
            bail!("[nats_server] ready_timeout must be above 0");
        }
        Ok(NatsServer { command, config: resolve(&self.config, base, home), ready_timeout })
    }
}
