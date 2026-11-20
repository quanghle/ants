//! What of an ant's sense data the colony sees: the profile of its confirmed persona, in which the ant tells other ants
//! and the messenger what it is good for, published in its colony entry, and why sense keeps none of its data, where
//! it keeps none. The profile is read before the ant's first colony entry and then with each read of the ant's
//! schedule ([`super::scheduler`]), which wakes after each change sense makes of the ant's items.

use std::time::Duration;

use ants_proto::UserId;
use ants_sense::Sense;
use ants_sense::store::Items;
use tokio::sync::watch;

use crate::colony::Workload;

/// The longest wait for the profile before the ant's first colony entry, which goes without it past that.
const FIRST_READ: Duration = Duration::from_secs(2);

/// Put in `workload` the profile, which `ant`'s colony entry shows, of the ant's confirmed persona in `items`; a read
/// that fails keeps the profile last read.
pub(super) async fn profile(items: &dyn Items, ant: &UserId, workload: &Workload) {
    match ants_sense::persona::profile(items).await {
        Ok(profile) => workload.set_profile(profile),
        Err(error) => tracing::debug!(%ant, %error, "keeping the profile last read until the next read"),
    }
}

/// Put in `workload` the profile of `ant`'s confirmed persona before its first colony entry, from the items `sense`
/// serves of the ant from its cache, waiting at most [`FIRST_READ`]: past that, or when the read fails, the entry goes
/// without it until the schedule's first read.
pub(super) async fn first_profile(sense: &Sense, ant: &UserId, workload: &Workload) {
    let read = async {
        match sense.items(ant).await {
            Ok(items) => profile(&*items, ant, workload).await,
            Err(error) => tracing::debug!(%ant, %error, "the first colony entry goes without the profile"),
        }
    };
    if tokio::time::timeout(FIRST_READ, read).await.is_err() {
        tracing::debug!(%ant, "the first colony entry goes without the profile, which took too long to read");
    }
}

/// Keep `workload`'s reason why sense keeps none of `ant`'s data, which `ant`'s colony entry shows, the one `why`
/// follows, warning of each reason once as it first shows. Runs until dropped.
pub(super) async fn unkept(mut why: watch::Receiver<Option<String>>, ant: &UserId, workload: &Workload) {
    let mut warned = None;
    loop {
        let current = why.borrow_and_update().clone();
        if let Some(reason) = current.as_ref().filter(|_| current != warned) {
            tracing::warn!(%ant, "sense keeps none of the ant's data, which is served without them: {reason}");
            warned.clone_from(&current);
        }
        workload.set_sense_unkept(current);
        if why.changed().await.is_err() {
            return std::future::pending().await;
        }
    }
}

#[cfg(test)]
mod tests;
