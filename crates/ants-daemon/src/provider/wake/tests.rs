use std::sync::atomic::{AtomicBool, Ordering};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::DEFAULT_TURN_BUDGET;
use ants_proto::provider::{Capabilities, InitializeParams, InitializeResult, PROTOCOL_VERSION, Provider};
use ants_test_support::NatsServer;

use super::*;
use crate::bus;
use crate::pool::Stored;

/// What a wake sent: the envelopes, as the outbox got them.
type Sent = Arc<Mutex<Vec<Envelope>>>;

struct Setup {
    _server: NatsServer,
    js: jetstream::Context,
    sent: Sent,
    /// Fails the next send once it stored the message, as a daemon stopping before it records the wake sent.
    fail_after_storing: Arc<AtomicBool>,
    sessions: SessionStore,
    /// A thread the ant has a session for.
    thread: ThreadId,
}

impl Setup {
    async fn start() -> Option<Self> {
        let server = NatsServer::start()?;
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        let sessions = SessionStore::memory();
        let thread = ThreadId::generate();
        let stored = Stored { session: SessionId::new("s1"), harness: "fake".to_owned(), cloned: false };
        sessions.put(&thread, stored).await.unwrap();
        let sent = Sent::default();
        let fail_after_storing = Arc::new(AtomicBool::new(false));
        Some(Self { _server: server, js, sent, fail_after_storing, sessions, thread })
    }

    /// A waker of `ant` for provider `clock`, sending through the stream.
    async fn waker(&self, ant: &UserId) -> Waker {
        let (js, sent, fail) = (self.js.clone(), self.sent.clone(), self.fail_after_storing.clone());
        let outbox: Outbox = Arc::new(move |envelope: Envelope| {
            let (js, sent, fail) = (js.clone(), sent.clone(), fail.clone());
            Box::pin(async move {
                bus::publish(&js, &envelope).await?;
                sent.locked().push(envelope);
                if fail.swap(false, Ordering::SeqCst) {
                    anyhow::bail!("stopped");
                }
                Ok(())
            })
        });
        let marks = bus::ensure_attempts(&self.js, None, ant).await.unwrap();
        Waker::new(ant.clone(), "clock", outbox, self.sessions.clone(), marks, Owners::new(self.js.clone()))
    }

    fn sent(&self) -> Vec<Envelope> {
        self.sent.locked().clone()
    }

    async fn stored(&self) -> u64 {
        bus::ensure_stream(&self.js).await.unwrap().info().await.unwrap().state.messages
    }
}

fn p(name: &str) -> UserId {
    name.parse().unwrap()
}

fn request(ant: &UserId, thread: Option<&ThreadId>, id: &str) -> WakeRequest {
    WakeRequest { ant: ant.clone(), thread: thread.cloned(), text: "standup in 5".to_owned(), id: id.to_owned() }
}

#[tokio::test]
async fn a_wake_on_a_thread_of_the_ant_is_a_message_to_it_from_its_provider() {
    let Some(setup) = Setup::start().await else { return };
    let ant = p("wake-dev");
    let waker = setup.waker(&ant).await;
    let woken = waker.wake("clock", request(&ant, Some(&setup.thread), "r1")).await.unwrap();
    assert_eq!(woken.thread, setup.thread);
    let [sent] = setup.sent().try_into().unwrap();
    assert_eq!((&sent.from, &sent.to, &sent.thread_id), (&ant, &ant, &setup.thread));
    assert_eq!((sent.wake_provider(), sent.text.as_str()), (Some("clock"), "standup in 5"));
    assert_eq!(sent.turn_budget, DEFAULT_TURN_BUDGET, "a fresh budget, none of the thread's");
    assert_eq!(setup.stored().await, 1);
}

#[tokio::test]
async fn a_wake_without_a_thread_starts_one() {
    let Some(setup) = Setup::start().await else { return };
    let ant = p("wake-new");
    let waker = setup.waker(&ant).await;
    let woken = waker.wake("clock", request(&ant, None, "r1")).await.unwrap();
    assert_ne!(woken.thread, setup.thread);
    let [sent] = setup.sent().try_into().unwrap();
    assert_eq!((&sent.thread_id, sent.owner.as_ref()), (&woken.thread, None));
}

#[tokio::test]
async fn a_repeated_wake_is_sent_once_and_names_the_same_thread() {
    let Some(setup) = Setup::start().await else { return };
    let ant = p("wake-again");
    let waker = setup.waker(&ant).await;
    let first = waker.wake("clock", request(&ant, None, "r1")).await.unwrap();
    let again = waker.wake("clock", request(&ant, None, "r1")).await.unwrap();
    assert_eq!(again, first);
    let restarted = setup.waker(&ant).await;
    assert_eq!(restarted.wake("clock", request(&ant, None, "r1")).await.unwrap(), first, "remembered in the bucket");
    assert_eq!(setup.sent().len(), 1);
    let other = waker.wake("clock", request(&ant, None, "r2")).await.unwrap();
    assert_ne!(other, first, "another id wakes it again");
    assert_eq!(setup.sent().len(), 2);
}

#[tokio::test]
async fn a_wake_not_recorded_sent_is_sent_again_under_its_id() {
    let Some(setup) = Setup::start().await else { return };
    let ant = p("wake-retry");
    let waker = setup.waker(&ant).await;
    setup.fail_after_storing.store(true, Ordering::SeqCst);
    let failed = waker.wake("clock", request(&ant, Some(&setup.thread), "r1")).await.unwrap_err();
    assert_eq!(failed.code, RpcError::UNAVAILABLE);
    let woken = waker.wake("clock", request(&ant, Some(&setup.thread), "r1")).await.unwrap();
    assert_eq!(woken.thread, setup.thread);
    let [first, second] = setup.sent().try_into().unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(setup.stored().await, 1, "the stream keeps one");
    waker.wake("clock", request(&ant, Some(&setup.thread), "r1")).await.unwrap();
    assert_eq!(setup.sent().len(), 2, "sent once recorded");
}

#[tokio::test]
async fn a_wake_the_ant_does_not_take_is_refused() {
    let Some(setup) = Setup::start().await else { return };
    let ant = p("wake-refused");
    let waker = setup.waker(&ant).await;
    let refused = |request: WakeRequest, provider: &'static str| {
        let waker = &waker;
        async move { waker.wake(provider, request).await.unwrap_err().code }
    };
    let invalid = RpcError::INVALID_PARAMS;
    assert_eq!(refused(request(&ant, None, "r1"), "sense").await, invalid, "another provider's ant");
    let foreign = ThreadId::generate();
    let unknown = refused(request(&ant, Some(&foreign), "r1"), "clock").await;
    assert_eq!(unknown, RpcError::UNKNOWN_THREAD, "no thread of the ant");
    assert_eq!(refused(WakeRequest { text: " \n".to_owned(), ..request(&ant, None, "r1") }, "clock").await, invalid);
    let long = "x".repeat(MAX_WAKE_TEXT + 1);
    assert_eq!(refused(WakeRequest { text: long, ..request(&ant, None, "r1") }, "clock").await, invalid);
    assert_eq!(refused(request(&ant, None, ""), "clock").await, invalid);
    assert!(setup.sent().is_empty());
}

#[tokio::test]
async fn an_ant_is_woken_at_most_so_often_an_hour() {
    let Some(setup) = Setup::start().await else { return };
    let ant = p("wake-often");
    let waker = setup.waker(&ant).await;
    for n in 0..WAKES_PER_HOUR {
        waker.wake("clock", request(&ant, None, &n.to_string())).await.unwrap();
    }
    let refused = waker.wake("clock", request(&ant, None, "over")).await.unwrap_err();
    assert_eq!(refused.code, RpcError::UNAVAILABLE);
    waker.wake("clock", request(&ant, None, "0")).await.expect("a repeat counts for nothing");
    assert_eq!(setup.sent().len(), WAKES_PER_HOUR);
}

/// A provider that keeps the host ants hands it, and uses wake.
#[derive(Default)]
struct Clock {
    host: Mutex<Option<Arc<dyn Host>>>,
}

impl Provider for Clock {
    fn initialize(
        &self,
        params: InitializeParams,
        host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
        *self.host.locked() = Some(host);
        let info = ants_proto::provider::Info { name: "clock".to_owned(), version: "1".to_owned() };
        let capabilities = Capabilities { wake: true, ..Capabilities::NONE };
        Box::pin(std::future::ready(params.accept(&[PROTOCOL_VERSION], info, capabilities)))
    }
}

#[tokio::test]
async fn a_provider_wakes_only_the_ants_served_here() {
    let Some(setup) = Setup::start().await else { return };
    let clock = Arc::new(Clock::default());
    let served = Wakers::default();
    super::super::connect(clock.clone(), &served).await.expect("connects");
    let host = clock.host.locked().clone().unwrap();
    let ant = p("wake-served");
    let unserved = host.wake(request(&ant, None, "r1")).await.unwrap_err();
    assert_eq!(unserved.code, RpcError::UNAVAILABLE);
    let serving = register(&served, setup.waker(&ant).await);
    let woken = host.wake(request(&ant, None, "r1")).await.unwrap();
    assert_eq!(setup.sent()[0].thread_id, woken.thread);
    drop(serving);
    assert_eq!(host.wake(request(&ant, None, "r2")).await.unwrap_err().code, RpcError::UNAVAILABLE);
}
