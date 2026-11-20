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
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use ants_proto::SenseId;
    use ants_proto::provider::BoxFuture;

    use ants_sense::items::Values;
    use ants_sense::log::MemoryLog;
    use ants_sense::persona::{PERSONA, PROFILE, persona_id};
    use ants_sense::store::{StoreError, Stores, Write};
    use ants_sense::testing::{self, One};
    use ants_test_support::eventually_within;
    use serde_json::json;

    use super::*;

    fn ant() -> UserId {
        "dev-profile".parse().unwrap()
    }

    async fn items() -> Arc<dyn Items> {
        Arc::new(testing::store(MemoryLog::default(), SenseId::new(), ant()).await)
    }

    /// Confirm a persona holding `profile`, as a dream does.
    async fn confirm(items: &dyn Items, profile: &str) {
        let mut values = Values::new();
        values.insert("voice".into(), json!("Plain."));
        values.insert(PROFILE.into(), json!(profile));
        let put = Write::Put { id: persona_id(), kind: PERSONA.into(), values, expected: None };
        items.write(&[put], "test").await.unwrap();
    }

    /// Counts the warnings logged while it is the default subscriber.
    struct Warnings(Arc<AtomicUsize>);

    impl tracing::Subscriber for Warnings {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn why_sense_keeps_none_of_the_data_shows_in_the_entry_and_warns_once_each() {
        let warnings = Arc::new(AtomicUsize::new(0));
        let _logged = tracing::subscriber::set_default(Warnings(warnings.clone()));
        let off = || Some("no host credentials on m1".to_owned());
        let (why, followed) = watch::channel(off());
        let (dev, workload) = (ant(), Workload::default());
        let shown = async |want: Option<String>, warned: usize| {
            let shows = async || workload.sense_unkept() == want && warnings.load(Ordering::Relaxed) == warned;
            eventually_within(Duration::from_secs(5), &format!("{want:?}, warned {warned} times"), shows).await;
        };
        let checked = async {
            shown(off(), 1).await;
            why.send_replace(None);
            shown(None, 1).await;
            why.send_replace(off());
            shown(off(), 1).await;
            let stopped = Some("the host connection stopped".to_owned());
            why.send_replace(stopped.clone());
            shown(stopped, 2).await;
        };
        tokio::select! {
            () = unkept(followed, &dev, &workload) => unreachable!("follows until dropped"),
            () = checked => {}
        }
    }

    /// Never serves the items.
    struct Stuck;

    impl Stores for Stuck {
        fn items<'a>(&'a self, _: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_entry_shows_the_profile_read_in_time() {
        let (items, dev, workload) = (items().await, ant(), Workload::default());
        let sense = Sense::with_stores(Arc::new(One(items.clone())));
        first_profile(&sense, &dev, &workload).await;
        assert_eq!(workload.profile(), None, "no persona confirmed");
        confirm(&*items, "Good for Rust daemons.").await;
        first_profile(&sense, &dev, &workload).await;
        assert_eq!(workload.profile().as_deref(), Some("Good for Rust daemons."));

        let (workload, started) = (Workload::default(), tokio::time::Instant::now());
        first_profile(&Sense::with_stores(Arc::new(Stuck)), &dev, &workload).await;
        assert_eq!((workload.profile(), started.elapsed()), (None, FIRST_READ), "the entry waits no longer");
        first_profile(&Sense::new(), &dev, &workload).await;
        assert_eq!(workload.profile(), None, "sense keeping no data");
    }
}
