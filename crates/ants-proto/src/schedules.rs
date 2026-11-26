//! What an [`AdminOp::ReadSchedules`] order returns in its outcome's `result`: the `[[schedule]]` entries of the
//! config file the daemon of the order's machine serves, each with its timetable, destination, next firing and the
//! last one due ([`Schedules`]), as `ants schedule list` reads them on that machine. Whether a firing went out the
//! reader tells from the keys of the webhooks bucket, which it reads itself.
//!
//! A machine's daemon answers such an order only while it is online: one that waited longer than [`SCHEDULES_FRESH`]
//! in the stream for it is refused as stale. The outcome stays in a KV bucket, so it lists at most
//! [`SCHEDULES_CAP`] schedules, counting those left out.

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::AdminOp;
use crate::{Destination, UserId};

/// How long a read of schedules may wait in the stream for its machine's daemon before it is refused as stale.
pub const SCHEDULES_FRESH: TimeDelta = TimeDelta::minutes(2);

/// The most schedules an outcome lists.
pub const SCHEDULES_CAP: usize = 256;

/// The schedules of a machine's config file. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schedules {
    /// The first [`SCHEDULES_CAP`] schedules, in the config file's order.
    pub schedules: Vec<ListedSchedule>,
    /// How many schedules were left out past [`SCHEDULES_CAP`].
    pub left_out: usize,
}

/// One schedule of a config file, listed at the time of the read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedSchedule {
    pub name: UserId,
    /// Its timetable, as `cron "0 3 * * 1-5" in Europe/London` or `every 4h`.
    pub when: String,
    pub to: Destination,
    /// The saved trail its firings start, if they start one.
    pub trail: Option<String>,
    pub catch_up: bool,
    /// Its next firing; none once it fires no more.
    pub next: Option<DateTime<Utc>>,
    /// Its last firing due within the webhooks bucket's retention; none before any.
    pub last_due: Option<DateTime<Utc>>,
}
