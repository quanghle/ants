//! What the daemon serving an ant verified of its sense log, kept in the ant's home ([`ChainStatus`] in
//! [`Home::status`]), for `ants doctor`, whose credentials do not reach the log, to report.

use std::io;
use std::path::Path;

use ants_proto::{SenseId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Break, Chain, Mark};
use crate::home::Home;

/// The bytes of an ant's sense log past which `ants doctor` and the brief warn: 32 MiB.
pub const SIZE_LIMIT: u64 = 32 << 20;

/// `bytes` for people, in MiB with one decimal, and exactly.
pub fn mib(bytes: u64) -> String {
    format!("{:.1} MiB ({bytes} bytes)", bytes as f64 / f64::from(1 << 20))
}

/// What the daemon serving an ant last verified of its sense log: the furthest event, which a later read expects to
/// find unchanged, the first break, if any, and the log's size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainStatus {
    pub sense: SenseId,
    /// The incarnation of the store read ([`super::EventLog::incarnation`]).
    pub incarnation: String,
    pub checked_at: DateTime<Utc>,
    /// The events read that verified.
    pub events: u64,
    /// The bytes of the messages read ([`Chain::bytes`]).
    #[serde(default)]
    pub bytes: u64,
    /// The furthest event verified, now or before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seen: Option<Mark>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broken: Option<Break>,
    /// The name of the ant the log's events are of by then ([`Chain::named`]): an ant added under another name with
    /// this home lists it among its earlier names ([`crate::identity::take_over`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ant: Option<UserId>,
}

impl ChainStatus {
    /// The status of `chain`, read from a store of `incarnation`, now.
    pub fn of(chain: &Chain, incarnation: &str) -> Self {
        Self {
            sense: chain.sense().clone(),
            incarnation: incarnation.to_owned(),
            checked_at: Utc::now(),
            events: chain.events(),
            bytes: chain.bytes(),
            seen: chain.seen().cloned(),
            broken: chain.broken().cloned(),
            ant: chain.named().cloned(),
        }
    }
}

/// The [`ChainStatus`] kept in `home`; none when there is none.
pub fn read_status(home: &Path) -> io::Result<Option<ChainStatus>> {
    let text = match std::fs::read(Home::new(home).status()) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    serde_json::from_slice(&text).map(Some).map_err(io::Error::other)
}

/// Keep `status` in `home`, replacing the file whole.
pub fn write_status(home: &Path, status: &ChainStatus) -> io::Result<()> {
    crate::home::write(&Home::new(home).status(), &serde_json::to_vec_pretty(status).map_err(io::Error::other)?)
}
