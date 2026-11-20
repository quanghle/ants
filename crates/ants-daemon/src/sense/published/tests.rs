use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ants_proto::SenseId;
use ants_proto::provider::BoxFuture;
use ants_sense::cache::Cache;
use ants_sense::items::Values;
use ants_sense::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
use ants_sense::persona::{PERSONA, PROFILE, persona_id};
use ants_sense::store::{Store, StoreError, Stores, Write};
use ants_test_support::eventually_within;
use serde_json::json;

use super::*;

struct One(Arc<dyn Items>);

impl Stores for One {
    fn items<'a>(&'a self, _: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}

fn ant() -> UserId {
    "dev-profile".parse().unwrap()
}

async fn items() -> Arc<dyn Items> {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let log = AntLog::new(MemoryLog::default(), Unsigned, verifier, SenseId::new(), ant(), "m1".parse().unwrap(), None)
        .unwrap();
    Arc::new(tokio::sync::Mutex::new(Store::open(log, Cache::in_memory().unwrap()).await.unwrap()))
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
