//! What of an ant's sense data the colony sees: the profile of its confirmed persona, in which the ant tells other ants
//! and the messenger what it is good for, published in its colony entry, and why sense keeps none of its data, where
//! it keeps none.

use ants_proto::{COLONY_HEARTBEAT, UserId};
use ants_sense::Sense;
use tokio::sync::watch;

use crate::colony::Workload;

/// Keep `workload`'s profile, which `ant`'s colony entry shows, that of the ant's confirmed persona: read now and after
/// each change a call to `sense` makes of the ant's items, and a heartbeat after a read that failed, the profile last
/// read staying meanwhile. Runs until dropped.
pub(super) async fn profile(sense: &Sense, ant: &UserId, workload: &Workload) {
    // Subscribe first: a persona confirmed during a read must wake the following wait.
    let mut changes = sense.changes(ant);
    loop {
        match async { ants_sense::persona::profile(&*sense.items(ant).await?).await }.await {
            Ok(profile) => workload.set_profile(profile),
            Err(error) => {
                tracing::debug!(%ant, %error, "reading the ant's profile again in a heartbeat");
                tokio::time::sleep(COLONY_HEARTBEAT).await;
                continue;
            }
        }
        if changes.changed().await.is_err() {
            return std::future::pending().await;
        }
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
    use ants_proto::provider::{BoxFuture, CallContext, Format, Origin, Provider as _, Starter, ToolCall};
    use ants_sense::cache::Cache;
    use ants_sense::items::Values;
    use ants_sense::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
    use ants_sense::persona::{PERSONA, PROFILE, persona_id};
    use ants_sense::store::{Items, Store, StoreError, Stores, Write};
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
        let log =
            AntLog::new(MemoryLog::default(), Unsigned, verifier, SenseId::new(), ant(), "m1".parse().unwrap(), None)
                .unwrap();
        Arc::new(tokio::sync::Mutex::new(Store::open(log, Cache::in_memory().unwrap()).await.unwrap()))
    }

    /// Confirm a persona holding `profile`, as a dream does, and tell `sense` so through a call that writes: the
    /// register of a thread of its own.
    async fn confirm(sense: &Sense, items: &dyn Items, profile: Option<&str>) {
        let mut values = Values::new();
        values.insert("voice".into(), json!("Plain."));
        values.insert(PROFILE.into(), profile.map_or(serde_json::Value::Null, |profile| json!(profile)));
        let put = Write::Put { id: persona_id(), kind: PERSONA.into(), values, expected: None };
        items.write(&[put], "test").await.unwrap();
        let context = CallContext {
            ant: ant(),
            identity: "0f1e".into(),
            thread: format!("profile-{}", uuid::Uuid::new_v4().simple()).parse().unwrap(),
            origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
            harness: "copilot".into(),
            model: None,
            machine: "m1".into(),
            repo: None,
            at: chrono::Utc::now(),
        };
        let register = json!({"goal": format!("Publish the profile {profile:?}"), "next": "Watch the colony entry"});
        let arguments = register.as_object().unwrap().clone();
        let noted = ToolCall { name: "checkpoint".into(), arguments, context, format: Format::Json };
        assert!(!sense.call_tool(noted).await.unwrap().is_error);
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

    #[tokio::test]
    async fn the_confirmed_profile_is_published_until_the_persona_drops_it() {
        let items = items().await;
        let sense = Sense::with_stores(Arc::new(One(items.clone())));
        confirm(&sense, &*items, Some("Good for Rust daemons.")).await;
        let (dev, workload) = (ant(), Workload::default());
        let published = async |want: Option<&str>| {
            let shows = async || workload.profile().as_deref() == want;
            eventually_within(Duration::from_secs(5), &format!("the profile {want:?}"), shows).await;
        };
        let checked = async {
            published(Some("Good for Rust daemons.")).await;
            confirm(&sense, &*items, None).await;
            published(None).await;
            confirm(&sense, &*items, Some("Good for reviews.")).await;
            published(Some("Good for reviews.")).await;
        };
        tokio::select! {
            () = profile(&sense, &dev, &workload) => unreachable!("publishes until dropped"),
            () = checked => {}
        }
    }
}
