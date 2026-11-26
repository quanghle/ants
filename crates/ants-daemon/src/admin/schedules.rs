//! [`AdminOp::ReadSchedules`]: list the `[[schedule]]` entries of the config file the daemon serves ([`Schedules`]),
//! whatever ant the order names, as `ants schedule list` lists them on the machine. Like a read of memories, it is
//! answered only while the daemon is online: an order that waited longer than [`SCHEDULES_FRESH`] is refused as stale.

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, SCHEDULES_CAP, SCHEDULES_FRESH, Schedules};
use chrono::{DateTime, Utc};

use super::{NO_CONFIG, Orders, applied, refused};
use crate::schedule::Schedule;

pub(super) fn apply(orders: &Orders, order: &AdminOrder, stored_at: DateTime<Utc>) -> AdminOutcome {
    let now = Utc::now();
    let waited = now - stored_at;
    if waited > SCHEDULES_FRESH {
        return refused(
            order,
            &format!(
                "stale: it waited {}s for the machine's daemon, which reads schedules only while online; ask again",
                waited.num_seconds()
            ),
        );
    }
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    match crate::config::load(&config.path) {
        Ok(loaded) => applied(order, Some(serde_json::to_value(listed(&loaded.schedules, now)).expect("serializes"))),
        Err(e) => refused(order, &format!("{e:#}")),
    }
}

/// `schedules` listed at `now`, the first [`SCHEDULES_CAP`] of them.
fn listed(schedules: &[Schedule], now: DateTime<Utc>) -> Schedules {
    Schedules {
        schedules: schedules.iter().take(SCHEDULES_CAP).map(|schedule| schedule.listed(now)).collect(),
        left_out: schedules.len().saturating_sub(SCHEDULES_CAP),
    }
}
