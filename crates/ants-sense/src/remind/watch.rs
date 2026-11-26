//! What a watcher runs on its interval ([`Watched`]): a command, or a saved trail in a one-shot worker. The daemon
//! keeps the digest of each check's result, never the result itself, and fires on each change of it
//! ([`super::complete_watch`]).

use std::io;
use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use ants_proto::ThreadId;

use crate::home::Home;
use crate::items::ItemId;
use crate::store::{Items, StoreError};

use super::condition::MAX_CONDITION_BYTES;

/// The shortest interval between a watcher's checks, and its default.
pub const MIN_INTERVAL_SECONDS: u64 = 60;
/// The longest interval between a watcher's checks: a day.
pub const MAX_INTERVAL_SECONDS: u64 = 24 * 60 * 60;
/// The interval a watcher's checks back off to after [`UNCHANGED_BEFORE_BACKOFF`] unchanged checks, unless it gives
/// its own.
pub const DEFAULT_MAX_INTERVAL_SECONDS: u64 = 30 * 60;
/// The checks in a row seeing no change, or failing, after which a watcher backs off to its longest interval.
pub const UNCHANGED_BEFORE_BACKOFF: u64 = 10;
/// The exit status of a command watcher's check reporting the watcher done: it fires a last time and retires.
pub const DONE_STATUS: i32 = 3;
/// The time a trail watcher's check takes at most, by default and at most.
pub const DEFAULT_TRAIL_SECONDS: u64 = 30;
pub const MAX_TRAIL_SECONDS: u64 = 5 * 60;
/// The most bytes of JSON of a trail watcher's args.
pub const MAX_ARGS_BYTES: usize = 64 * 1024;
/// The most bytes of a check's output a wake passes on.
pub const MAX_OUTPUT_BYTES: usize = ants_proto::MAX_WATCH_OUTPUT;

/// What a watcher runs on each check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Watched {
    /// A shell command the ant's tool allow rules permit, in its workdir, under a 30 s timeout: its exit status and
    /// stdout are its result, exit status [`DONE_STATUS`] reporting the watcher done.
    Command { command: String },
    /// The saved trail `name`, run with `args` in a one-shot worker for at most `limit_seconds`: its script returns
    /// `{"output": str, "done": bool}`.
    Trail {
        name: String,
        #[serde(default, skip_serializing_if = "Value::is_null")]
        args: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_seconds: Option<u64>,
    },
}

/// A watcher: what it runs, how often, and whether its first check fires.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Watch {
    pub run: Watched,
    /// Seconds between checks while they see changes, at least [`MIN_INTERVAL_SECONDS`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_seconds: Option<u64>,
    /// Seconds between checks once [`UNCHANGED_BEFORE_BACKOFF`] in a row saw no change, at least the interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_interval_seconds: Option<u64>,
    /// Whether the first check fires; otherwise it sets the result the next ones are compared with.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub first: bool,
}

impl Watch {
    pub(crate) fn validate(&self) -> Result<(), String> {
        match &self.run {
            Watched::Command { command } => text("run.command", command)?,
            Watched::Trail { name, args, limit_seconds } => {
                text("run.name", name)?;
                let size = serde_json::to_vec(args).map_or(usize::MAX, |args| args.len());
                if size > MAX_ARGS_BYTES {
                    return Err(format!(
                        "run.args has {size} bytes of JSON; a watcher takes at most {MAX_ARGS_BYTES}."
                    ));
                }
                if let Some(seconds) = limit_seconds
                    && !(1..=MAX_TRAIL_SECONDS).contains(seconds)
                {
                    return Err(format!("run.limit_seconds must be between 1 and {MAX_TRAIL_SECONDS}."));
                }
            }
        }
        let interval = self.interval();
        if !(MIN_INTERVAL_SECONDS..=MAX_INTERVAL_SECONDS).contains(&interval) {
            return Err(format!("interval_seconds must be between {MIN_INTERVAL_SECONDS} and {MAX_INTERVAL_SECONDS}."));
        }
        let most = self.max_interval();
        if !(interval..=MAX_INTERVAL_SECONDS).contains(&most) {
            return Err(format!("max_interval_seconds must be between interval_seconds and {MAX_INTERVAL_SECONDS}."));
        }
        Ok(())
    }

    /// Seconds between checks seeing changes.
    pub fn interval(&self) -> u64 {
        self.interval_seconds.unwrap_or(MIN_INTERVAL_SECONDS)
    }

    /// Seconds between checks seeing none for a while.
    pub fn max_interval(&self) -> u64 {
        self.max_interval_seconds.unwrap_or_else(|| DEFAULT_MAX_INTERVAL_SECONDS.max(self.interval()))
    }

    /// The wait before the next check, after `unchanged` checks in a row saw no change.
    pub fn next_check(&self, unchanged: u64) -> Duration {
        let seconds = if unchanged >= UNCHANGED_BEFORE_BACKOFF { self.max_interval() } else { self.interval() };
        Duration::seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
    }

    /// The most a trail watcher's check takes; none for a command's, which takes its 30 s.
    pub fn trail_limit(&self) -> Option<std::time::Duration> {
        match &self.run {
            Watched::Trail { limit_seconds, .. } => {
                Some(std::time::Duration::from_secs(limit_seconds.unwrap_or(DEFAULT_TRAIL_SECONDS)))
            }
            Watched::Command { .. } => None,
        }
    }
}

fn text(name: &str, value: &str) -> Result<(), String> {
    super::condition::text(name, value).map_err(|e| e.replace("condition.", ""))
}

/// The digest of a check's result, as the watcher keeps it: of the command's exit status and stdout, or of the trail's
/// output.
pub fn digest(status: Option<i32>, output: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    let kind = status.map_or_else(|| "trail".to_owned(), |status| format!("exit {status}"));
    let digest = Sha256::new().chain_update(kind).chain_update([0]).chain_update(output).finalize();
    data_encoding::HEXLOWER.encode(&digest)
}

pub(super) fn schema() -> Value {
    let text = json!({"type":"string","minLength":1,"maxLength":MAX_CONDITION_BYTES});
    json!({
        "type":"object","additionalProperties":false,"required":["kind","run"],
        "properties":{
            "kind":{"const":"watch"},
            "run":{"oneOf":[
                {"type":"object","additionalProperties":false,"required":["kind","command"],
                 "properties":{"kind":{"const":"command"},"command":text}},
                {"type":"object","additionalProperties":false,"required":["kind","name"],
                 "properties":{"kind":{"const":"trail"},"name":text,"args":{},
                     "limit_seconds":{"type":"integer","minimum":1,"maximum":MAX_TRAIL_SECONDS}}}
            ]},
            "interval_seconds":{"type":"integer","minimum":MIN_INTERVAL_SECONDS,"maximum":MAX_INTERVAL_SECONDS},
            "max_interval_seconds":{"type":"integer","minimum":MIN_INTERVAL_SECONDS,"maximum":MAX_INTERVAL_SECONDS},
            "first":{"type":"boolean"}
        }
    })
}

/// The ant's active watchers as the daemon serving it last checked them, kept in its home for `ants doctor`, whose
/// credentials do not reach sense data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatcherStatus {
    pub checked_at: DateTime<Utc>,
    pub watchers: Vec<WatcherState>,
}

/// One active watcher in a [`WatcherStatus`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatcherState {
    pub id: ItemId,
    pub title: String,
    /// What it runs: its command, or `trail NAME`.
    pub runs: String,
    /// Its checks in a row that failed.
    #[serde(default)]
    pub failed_checks: u64,
    /// Why the latest of them failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Watched {
    /// What it runs, in a line: its command, or `trail NAME`.
    pub fn runs(&self) -> String {
        match self {
            Self::Command { command } => command.clone(),
            Self::Trail { name, .. } => format!("trail {name}"),
        }
    }
}

/// The [`WatcherStatus`] kept in `home`; none when there is none.
pub fn read_status(home: &Path) -> io::Result<Option<WatcherStatus>> {
    let text = match std::fs::read(Home::new(home).watchers()) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    serde_json::from_slice(&text).map(Some).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Keep `status` in `home`, replacing the file whole, unsynced: it is written again after each check.
pub fn write_status(home: &Path, status: &WatcherStatus) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(status).map_err(io::Error::other)?;
    crate::home::write_unsynced(&Home::new(home).watchers(), &bytes)
}

/// What a trail watcher's `thread()` reads of `thread`: its resume register, as its last checkpoint left it, and its
/// record; each null when it has none.
pub async fn thread_state(items: &dyn Items, thread: &ThreadId) -> Result<Value, StoreError> {
    let register = crate::thread::register(items, thread).await?;
    let record = crate::thread::record::record(items, thread).await?;
    Ok(json!({"register": register, "record": record}))
}
