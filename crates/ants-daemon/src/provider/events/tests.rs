//! Tests of lifecycle events told to providers: order per ant, acks and retries under the same id, a capped queue kept
//! across restarts, renames, and providers without events.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::provider::{
    Ack, Capabilities, Host, InitializeParams, InitializeResult, Provider, Rebuilt, WakeRequest, Woken,
};

use super::*;

/// A provider recording the events it takes, failing the first `failing` calls.
#[derive(Default)]
pub(crate) struct Recording {
    pub(crate) got: Mutex<Vec<Event>>,
    failing: AtomicUsize,
    events: bool,
}

impl Recording {
    pub(crate) fn taking_events() -> Arc<Self> {
        Arc::new(Self { events: true, ..Self::default() })
    }

    fn failing(n: usize) -> Arc<Self> {
        Arc::new(Self { events: true, failing: AtomicUsize::new(n), ..Self::default() })
    }

    pub(crate) fn kinds(&self) -> Vec<EventKind> {
        self.got.locked().iter().map(|event| event.kind.clone()).collect()
    }
}

impl Provider for Recording {
    fn initialize(
        &self,
        params: InitializeParams,
        _host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
        let declared = Capabilities { events: self.events, tools: true, ..Capabilities::NONE };
        let info = ants_proto::provider::Info { name: "recording".to_owned(), version: "0".to_owned() };
        Box::pin(std::future::ready(params.accept(&[ants_proto::provider::PROTOCOL_VERSION], info, declared)))
    }

    fn event(&self, event: Event) -> BoxFuture<'_, Result<Ack, RpcError>> {
        let failed = self.failing.try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1)).is_ok();
        self.got.locked().push(event);
        let result = if failed { Err(RpcError::unavailable("down")) } else { Ok(Ack {}) };
        Box::pin(std::future::ready(result))
    }
}

struct NoWake;

impl Host for NoWake {
    fn wake(&self, _request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
        Box::pin(std::future::ready(Err(RpcError::unavailable("no"))))
    }
}

/// An outbox delivering to `provider`, retrying at once.
pub(crate) async fn outbox_to(provider: Arc<Recording>) -> Outbox {
    let params = InitializeParams::new(super::super::client(), super::super::USES);
    let connected = Connected::connect(provider, params, Arc::new(NoWake)).await.unwrap();
    let resolve: Resolve = Arc::new(move |_| Box::pin(std::future::ready(Some(connected.clone()))));
    Outbox::with_retry(resolve, Duration::from_millis(1), Duration::from_millis(4))
}

/// An outbox whose provider is never connected.
fn outbox_to_none() -> Outbox {
    Outbox::with_retry(Arc::new(|_| Box::pin(std::future::ready(None))), Duration::from_millis(1), Duration::ZERO)
}

fn p(name: &str) -> UserId {
    name.parse().unwrap()
}

fn t(name: &str) -> ThreadId {
    name.parse().unwrap()
}

/// Wait until `provider` took `n` events.
pub(crate) async fn took(provider: &Recording, n: usize) {
    for _ in 0..1_000 {
        if provider.got.locked().len() >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("the provider took {} events, not {n}", provider.got.locked().len());
}

fn queued(home: &Path) -> Vec<Event> {
    std::fs::read_to_string(queue_file(home))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn each_ant_s_events_arrive_in_order_and_leave_the_queue_once_acked() {
    let dir = ants_test_support::scratch_dir("events-order");
    let provider = Recording::taking_events();
    let outbox = outbox_to(provider.clone()).await;
    let (a, b) = (dir.join("a"), dir.join("b"));
    let rebuilt = |reason| EventKind::SessionRebuilt { reason };
    for (n, kind) in
        [EventKind::SessionLost, rebuilt(Rebuilt::Lost), rebuilt(Rebuilt::Overflow)].into_iter().enumerate()
    {
        let thread = t(&format!("t{n}"));
        outbox.push(&a, event(Uuid::new_v4(), &p("dev-a"), "ida", Some(&thread), kind)).await;
        outbox.push(&b, event(Uuid::new_v4(), &p("dev-b"), "idb", None, EventKind::ThreadExpired)).await;
    }
    took(&provider, 6).await;
    let got = provider.got.locked().clone();
    let of = |ant: &str| got.iter().filter(|e| e.ant == p(ant)).map(|e| e.thread.clone()).collect::<Vec<_>>();
    assert_eq!(of("dev-a"), [Some(t("t0")), Some(t("t1")), Some(t("t2"))]);
    assert_eq!(of("dev-b").len(), 3);
    for _ in 0..100 {
        if outbox.pending(&p("dev-a")).await.is_empty() && outbox.pending(&p("dev-b")).await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(!queue_file(&a).exists() && !queue_file(&b).exists(), "no event left once acked");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_failed_event_is_sent_again_with_its_id_before_the_next() {
    let dir = ants_test_support::scratch_dir("events-retry");
    let provider = Recording::failing(2);
    let outbox = outbox_to(provider.clone()).await;
    let first = event(Uuid::new_v4(), &p("dev-a"), "ida", None, EventKind::AntAdded);
    outbox.push(&dir, first.clone()).await;
    outbox.push(&dir, event(Uuid::new_v4(), &p("dev-a"), "ida", None, EventKind::AntRetired)).await;
    took(&provider, 4).await;
    let ids: Vec<Uuid> = provider.got.locked().iter().map(|e| e.id).collect();
    assert_eq!(ids[..3], [first.id; 3], "tried until acked, the same id each time");
    assert_eq!(provider.kinds()[3], EventKind::AntRetired, "then the next");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn events_kept_while_no_provider_is_connected_are_told_after_a_restart() {
    let dir = ants_test_support::scratch_dir("events-restart");
    let down = outbox_to_none();
    let ant = p("dev-a");
    down.push(&dir, event(Uuid::new_v4(), &ant, "ida", None, EventKind::AntAdded)).await;
    down.push(&dir, event(Uuid::new_v4(), &ant, "ida", Some(&t("t1")), EventKind::ThreadExpired)).await;
    let kept = queued(&dir);
    assert_eq!(kept.len(), 2, "stored before the push returns");

    let provider = Recording::taking_events();
    let restarted = outbox_to(provider.clone()).await;
    restarted.open(&ant, &dir).await;
    took(&provider, 2).await;
    assert_eq!(*provider.got.locked(), kept, "the same events, ids and times");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_queue_past_its_cap_drops_its_oldest() {
    let dir = ants_test_support::scratch_dir("events-cap");
    let outbox = outbox_to_none();
    let ant = p("dev-a");
    let first = event(Uuid::new_v4(), &ant, "ida", None, EventKind::AntAdded);
    let mut lines = serde_json::to_string(&first).unwrap() + "\n";
    for _ in 1..CAP {
        lines +=
            &(serde_json::to_string(&event(Uuid::new_v4(), &ant, "ida", None, EventKind::ThreadDone)).unwrap() + "\n");
    }
    std::fs::create_dir_all(dir.join(DIR)).unwrap();
    std::fs::write(queue_file(&dir), lines).unwrap();
    outbox.push(&dir, event(Uuid::new_v4(), &ant, "ida", None, EventKind::AntRetired)).await;
    let kept = queued(&dir);
    assert_eq!(kept.len(), CAP);
    assert!(kept.iter().all(|e| e.id != first.id), "the oldest went");
    assert_eq!(kept.last().unwrap().kind, EventKind::AntRetired);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_provider_without_events_gets_no_lifecycle() {
    let dir = ants_test_support::scratch_dir("events-not-agreed");
    let outbox = outbox_to(Arc::new(Recording::default())).await;
    assert!(Lifecycle::open(&outbox, &p("dev-a"), "ida", &dir).await.is_none());
    assert!(!dir.join(DIR).exists(), "nothing written");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn an_ant_is_told_added_once_retired_once_stopped_and_revived_once_served_again() {
    let dir = ants_test_support::scratch_dir("events-ant");
    let provider = Recording::taking_events();
    let outbox = outbox_to(provider.clone()).await;
    let ant = p("dev-a");
    let lifecycle = Lifecycle::open(&outbox, &ant, "ida", &dir).await.unwrap();
    lifecycle.stopped(RETIRED_WAIT).await;
    Lifecycle::open(&outbox, &ant, "ida", &dir).await.unwrap();
    took(&provider, 1).await;
    assert_eq!(provider.kinds(), [EventKind::AntAdded], "added once; no retirement unmarked");

    mark_retired(&dir).unwrap();
    lifecycle.stopped(RETIRED_WAIT).await;
    assert_eq!(provider.kinds()[1..], [EventKind::AntRetired], "acked before the stop ends");
    lifecycle.stopped(RETIRED_WAIT).await;
    Lifecycle::open(&outbox, &ant, "ida", &dir).await.unwrap();
    took(&provider, 3).await;
    assert_eq!(provider.kinds(), [EventKind::AntAdded, EventKind::AntRetired, EventKind::AntRevived]);
    assert!(!dir.join(DIR).join(RETIRED).exists());

    // Retired while no daemon served it: told on its revival, before it.
    mark_retired(&dir).unwrap();
    Lifecycle::open(&outbox, &ant, "ida", &dir).await.unwrap();
    took(&provider, 5).await;
    assert_eq!(provider.kinds()[3..], [EventKind::AntRetired, EventKind::AntRevived]);
    assert!(provider.got.locked().iter().all(|e| e.ant == ant && e.identity == "ida" && e.thread.is_none()));

    // A new identity in the same home, as after `ants auth revoke`, is an ant added.
    Lifecycle::open(&outbox, &ant, "idb", &dir).await.unwrap();
    took(&provider, 6).await;
    assert_eq!(provider.got.locked()[5].kind, EventKind::AntAdded);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A stop waits a bounded time for an unreachable provider to take the retirement, which stays queued.
#[tokio::test]
async fn a_stop_waits_a_bounded_time_for_the_retirement_to_be_acked() {
    let dir = ants_test_support::scratch_dir("events-retired-wait");
    let ant = p("dev-a");
    let lifecycle = Lifecycle {
        ant: ant.clone(),
        identity: "ida".to_owned(),
        home: dir.clone(),
        outbox: outbox_to_none(),
        workdir: None,
    };
    mark_retired(&dir).unwrap();
    let started = Instant::now();
    lifecycle.stopped(Duration::from_millis(50)).await;
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert_eq!(queued(&dir).iter().map(|e| e.kind.clone()).collect::<Vec<_>>(), [EventKind::AntRetired]);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn an_ant_that_arrived_is_told_of_how_not_as_added() {
    let dir = ants_test_support::scratch_dir("events-arrived");
    let cloned = EventKind::AntCloned { from: p("dev-a"), from_identity: "ida".to_owned() };
    arrived(&dir, &p("dev-b"), "idb", vec![cloned.clone()]).unwrap();
    let provider = Recording::taking_events();
    let outbox = outbox_to(provider.clone()).await;
    Lifecycle::open(&outbox, &p("dev-b"), "idb", &dir).await.unwrap();
    took(&provider, 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(provider.kinds(), [cloned]);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn events_queued_under_an_earlier_name_are_told_of_the_renamed_ant() {
    let dir = ants_test_support::scratch_dir("events-renamed");
    let old = [EventKind::ThreadExpired, EventKind::SessionLost];
    let lines: String = old
        .iter()
        .map(|kind| serde_json::to_string(&event(Uuid::new_v4(), &p("dev-a"), "ida", Some(&t("t1")), kind.clone())))
        .map(|line| line.unwrap() + "\n")
        .collect();
    std::fs::create_dir_all(dir.join(DIR)).unwrap();
    std::fs::write(queue_file(&dir), lines).unwrap();
    std::fs::create_dir_all(dir.join(crate::sense::DIR)).unwrap();
    std::fs::write(dir.join(crate::sense::DIR).join("names"), "dev-a\n").unwrap();
    let provider = Recording::taking_events();
    let outbox = outbox_to(provider.clone()).await;
    Lifecycle::open(&outbox, &p("dev-b"), "idb", &dir).await.unwrap();
    took(&provider, 3).await;
    let got = provider.got.locked().clone();
    assert_eq!(
        got.iter().map(|e| e.kind.clone()).collect::<Vec<_>>(),
        [old[0].clone(), old[1].clone(), EventKind::AntAdded]
    );
    assert!(got.iter().all(|e| e.ant == p("dev-b")), "{got:?}");
    assert_eq!(got[0].identity, "ida", "an event keeps the identity it happened to");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_thread_start_told_again_keeps_its_id() {
    let id = |message| derived(&["dev-a", "t1", message, "thread_started"]);
    assert_eq!(id("m1"), id("m1"));
    assert_ne!(id("m1"), id("m2"));
}
