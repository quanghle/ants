//! What the daemon serving an ant verified of its sense log, kept in the ant's home ([`ChainStatus`]), for `ants doctor`,
//! whose credentials do not reach the log, to report.

use std::io;
use std::path::{Path, PathBuf};

use ants_proto::SenseId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Break, Chain, Mark};

/// The directory of an ant's home holding its sense files: its identity's id, the cache of its log and the
/// [`ChainStatus`].
pub const SENSE_DIR: &str = ".sense";
/// The file of [`SENSE_DIR`] holding the [`ChainStatus`].
pub const CHAIN_FILE: &str = "chain.json";

/// What the daemon serving an ant last verified of its sense log: the furthest event, which a later read expects to
/// find unchanged, and the first break, if any.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainStatus {
    pub sense: SenseId,
    /// The incarnation of the store read ([`super::EventLog::incarnation`]).
    pub incarnation: String,
    pub checked_at: DateTime<Utc>,
    /// The events read that verified.
    pub events: u64,
    /// The furthest event verified, now or before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seen: Option<Mark>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broken: Option<Break>,
}

impl ChainStatus {
    /// The status of `chain`, read from a store of `incarnation`, now.
    pub fn of(chain: &Chain, incarnation: &str) -> Self {
        Self {
            sense: chain.sense().clone(),
            incarnation: incarnation.to_owned(),
            checked_at: Utc::now(),
            events: chain.events(),
            seen: chain.seen().cloned(),
            broken: chain.broken().cloned(),
        }
    }
}

/// Where the [`ChainStatus`] of the ant whose home is `home` is kept.
pub fn status_file(home: &Path) -> PathBuf {
    home.join(SENSE_DIR).join(CHAIN_FILE)
}

/// The [`ChainStatus`] kept in `home`; none when there is none.
pub fn read_status(home: &Path) -> io::Result<Option<ChainStatus>> {
    let text = match std::fs::read(status_file(home)) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    serde_json::from_slice(&text).map(Some).map_err(io::Error::other)
}

/// Keep `status` in `home`, replacing the file whole.
pub fn write_status(home: &Path, status: &ChainStatus) -> io::Result<()> {
    let file = status_file(home);
    let dir = file.parent().expect("a file in a directory");
    std::fs::create_dir_all(dir)?;
    let beside = dir.join(format!("{CHAIN_FILE}.new"));
    std::fs::write(&beside, serde_json::to_vec_pretty(status).map_err(io::Error::other)?)?;
    std::fs::rename(&beside, &file)
}
