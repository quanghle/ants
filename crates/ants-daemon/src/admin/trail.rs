//! [`AdminOp::StopTrail`] and [`AdminOp::ResumeTrail`]: stop or resume a trail run the ant conducts. The daemon serving
//! the ant carries the order out as it answers a human's request on the ant's control subject ([`Trails::act`]), the
//! admin in the human's place, and the outcome's `result` is that answer ([`TrailOutcome`]): applied once stopped, with
//! the steps left in other ants' queues, or resumed; refused with the conductor's reason.
//!
//! An order waits in the stream while the machine's daemon is away, and is taken once it is back, often before the
//! daemon serving the ant conducts its runs again: an order the process serves the ant for, without its inbox served
//! yet, as while the ant connects, waits for its runs up to [`CONDUCTING_WAIT`], and one to an ant the process does not
//! serve, as before the process started serving its ants, up to [`SERVED_WAIT`]. One to an ant whose daemon serves its
//! inbox conducting no runs, as the bus keeps none, is refused at once.

use std::time::Duration;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, TrailAction, TrailOutcome, UserId};

use super::restart::Restarts;
use super::{Orders, applied, refused};
use crate::trail::Trails;

/// How long an order waits for the daemon serving the ant to conduct its runs.
pub const CONDUCTING_WAIT: Duration = Duration::from_mins(1);
/// How long an order waits for the process to serve the ant.
pub const SERVED_WAIT: Duration = Duration::from_secs(10);
/// How often the waiting order looks again.
const LOOK_AGAIN: Duration = Duration::from_millis(100);

/// The trail runs of an ant conducted, reached by the orders about them ([`Restarts::conduct`]).
pub struct Conducted {
    pub(super) restarts: Restarts,
    pub(super) ant: UserId,
    pub(super) trails: Trails,
}

/// How a process stands with the trail runs of an ant ([`Restarts::conductor`]).
pub(super) enum Conductor {
    /// It serves no such ant.
    Unserved,
    /// The daemon serving it does not serve its inbox yet.
    Starting,
    /// The daemon serving it serves its inbox, conducting no runs.
    NoTrails,
    Ready(Trails),
}

/// Carry out `action`, what `order` asks of a run of its ant.
pub(super) async fn apply(orders: &Orders, order: &AdminOrder, action: TrailAction) -> AdminOutcome {
    let trails = match conducting(orders, &order.ant).await {
        Ok(trails) => trails,
        Err(why) => return refused(order, &why),
    };
    match trails.act(&order.admin, &action).await {
        TrailOutcome::Refused { error } => refused(order, &error),
        outcome => applied(order, Some(serde_json::to_value(outcome).expect("an outcome serializes"))),
    }
}

/// The trail runs of `ant`, once the daemon serving it conducts them.
async fn conducting(orders: &Orders, ant: &UserId) -> Result<Trails, String> {
    let since = tokio::time::Instant::now();
    loop {
        match orders.restarts.conductor(ant) {
            Conductor::Ready(trails) => return Ok(trails),
            Conductor::NoTrails => return Err(format!("{ant} conducts no trail runs: the bus keeps none")),
            Conductor::Unserved if since.elapsed() >= SERVED_WAIT => {
                return Err(format!("machine {} serves no ant {ant}", orders.machine));
            }
            Conductor::Starting if since.elapsed() >= CONDUCTING_WAIT => {
                let waited = ants_proto::duration::format(CONDUCTING_WAIT);
                return Err(format!("the daemon serving {ant} did not conduct its trail runs within {waited}"));
            }
            Conductor::Unserved | Conductor::Starting => tokio::time::sleep(LOOK_AGAIN).await,
        }
    }
}
