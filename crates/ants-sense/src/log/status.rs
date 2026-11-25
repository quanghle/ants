//! What the daemon serving an ant verified of its sense log, kept in the ant's home ([`ChainStatus`] in
//! [`Home::status`]), for `ants doctor`, whose credentials do not reach the log, to report.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use ants_proto::{SenseId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

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

/// The [`ChainStatus`] kept in `home`, the latest an [`AntLog`](super::AntLog) of this process keeps there when still
/// being written in the background; none when there is none. What is there but no status, as a power loss may leave
/// it ([`write_status`]), fails as [`io::ErrorKind::InvalidData`].
pub fn read_status(home: &Path) -> io::Result<Option<ChainStatus>> {
    let file = Home::new(home).status();
    if let Some(keeping) = writes().get(&file) {
        return Ok(Some(keeping.latest.clone()));
    }
    let text = match std::fs::read(file) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    serde_json::from_slice(&text).map(Some).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Keep `status` in `home`, replacing the file whole, synced to disk neither: it is written again as often as an event
/// is appended, and a status a power loss leaves out of date or empty is written anew at the next read of the log.
pub fn write_status(home: &Path, status: &ChainStatus) -> io::Result<()> {
    write(&Home::new(home).status(), status)
}

/// A status being written by [`keep_status`]: the latest kept, and whether it is still to write after the one being
/// written, if any.
struct Keeping {
    latest: ChainStatus,
    unwritten: bool,
}

/// The statuses [`keep_status`] writes, by file, there while a writer of the file runs.
static KEEPING: LazyLock<Mutex<HashMap<PathBuf, Keeping>>> = LazyLock::new(Mutex::default);
/// Told each time a file leaves [`KEEPING`].
static WRITTEN: Notify = Notify::const_new();

fn writes() -> MutexGuard<'static, HashMap<PathBuf, Keeping>> {
    KEEPING.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Keep `status` in `home` as [`write_status`] does, in the runtime's blocking pool, never on the caller's thread: a
/// status kept while the one before is written replaces any other still to write, so only the latest is written
/// next. A status that cannot be written is logged, and written again at the next change ([`read_status`] meanwhile
/// reads it); none is written once `home` is gone. Outside a runtime it is written at once.
pub(super) fn keep_status(home: &Path, status: ChainStatus) {
    let file = Home::new(home).status();
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        if let Err(e) = write(&file, &status) {
            tracing::warn!(file = %file.display(), error = %e, "the sense chain status is not kept");
        }
        return;
    };
    let mut kept = writes();
    let keeping = Keeping { latest: status, unwritten: true };
    match kept.entry(file.clone()) {
        Entry::Occupied(mut writing) => {
            *writing.get_mut() = keeping;
            return;
        }
        Entry::Vacant(idle) => idle.insert(keeping),
    };
    drop(kept);
    let home = home.to_owned();
    runtime.spawn_blocking(move || {
        loop {
            let status = {
                let mut kept = writes();
                let Some(keeping) = kept.get_mut(&file).filter(|keeping| keeping.unwritten) else {
                    kept.remove(&file);
                    WRITTEN.notify_waiters();
                    return;
                };
                keeping.unwritten = false;
                keeping.latest.clone()
            };
            // A home removed meanwhile, as with its ant, is not made again.
            if home.is_dir()
                && let Err(e) = write(&file, &status)
            {
                tracing::warn!(file = %file.display(), error = %e, "the sense chain status is not kept");
            }
        }
    });
}

/// Wait for the statuses an [`AntLog`](super::AntLog) kept in `home` so far to be written.
pub async fn status_written(home: &Path) {
    let file = Home::new(home).status();
    loop {
        let mut written = std::pin::pin!(WRITTEN.notified());
        written.as_mut().enable();
        if !writes().contains_key(&file) {
            return;
        }
        written.await;
    }
}

fn write(file: &Path, status: &ChainStatus) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(status).map_err(io::Error::other)?;
    crate::home::write_unsynced(file, &bytes)
}
