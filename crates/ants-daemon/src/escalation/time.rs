//! Times and durations in milliseconds, as the ledger keeps deadlines.

use std::time::Duration;

pub(super) fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

pub(super) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
