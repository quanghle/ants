use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::SenseId;
use ants_proto::provider::{BoxFuture, CallContext, Format, Origin, Provider as _, Starter, ToolCall, Woken};
use ants_sense::cache::Cache;
use ants_sense::items::ItemId;
use ants_sense::log::{
    AccountVerifier, AntLog, AppendError, EventLog, LogError, MemoryLog, NoRevocations, Stored, Unsigned,
};
use ants_sense::store::{Store, Stores};
use serde_json::{Value, json};

use super::*;
use crate::sync::MutexExt as _;

mod dreaming;
mod probes;

const DAY: Duration = Duration::from_hours(24);

struct One {
    items: Arc<dyn Items>,
    reads: AtomicUsize,
}

impl Stores for One {
    fn items<'a>(&'a self, _: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(self.items.clone()) })
    }
}

#[derive(Clone, Default)]
struct SharedLog(Arc<MemoryLog>);

impl EventLog for SharedLog {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected: u64) -> Result<u64, AppendError> {
        self.0.append(sense, payload, expected).await
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.0.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.0.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        self.0.incarnation()
    }
}

async fn open(log: SharedLog, sense: SenseId) -> Arc<dyn Items> {
    let log = AntLog::new(
        log,
        Unsigned,
        AccountVerifier::new(vec![], NoRevocations),
        sense,
        ant(),
        "m1".parse().unwrap(),
        None,
    )
    .unwrap();
    Arc::new(tokio::sync::Mutex::new(Store::open(log, Cache::in_memory().unwrap()).await.unwrap()))
}

async fn setup() -> (Arc<Sense>, Arc<One>) {
    let items = open(SharedLog::default(), SenseId::new()).await;
    let stores = Arc::new(One { items, reads: AtomicUsize::new(0) });
    (Arc::new(Sense::with_stores(stores.clone())), stores)
}

fn ant() -> UserId {
    "dev-scheduler".parse().unwrap()
}

fn settings() -> Settings {
    Settings {
        ant: ant(),
        workdir: std::env::current_dir().unwrap(),
        machine: "m1".into(),
        env: BTreeMap::new(),
        allow: Vec::new(),
        dreams: None,
    }
}

fn call(arguments: Value) -> ToolCall {
    ToolCall {
        name: "remind".into(),
        arguments: arguments.as_object().unwrap().clone(),
        context: CallContext {
            ant: ant(),
            identity: "0f1e".into(),
            thread: "scheduler-thread".parse().unwrap(),
            origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
            harness: "copilot".into(),
            model: None,
            machine: "m1".into(),
            repo: None,
            at: Utc::now(),
        },
        format: Format::Json,
    }
}

async fn reminder(items: &dyn Items, id: &ItemId) -> remind::Reminder {
    remind::Reminder::of(&items.get(id).await.unwrap().unwrap()).unwrap()
}

async fn create(sense: &Sense, trigger: Value) -> ItemId {
    let result = sense.call_tool(call(json!({"title":"scheduled work","trigger":trigger}))).await.unwrap();
    assert!(!result.is_error, "{result:?}");
    let ants_proto::provider::Output::Json(result) = result.output else { panic!("requested JSON") };
    result["reminder"]["id"].as_str().unwrap().parse().unwrap()
}

#[derive(Default)]
struct Wakes {
    requests: Mutex<Vec<WakeRequest>>,
    errors: Mutex<VecDeque<RpcError>>,
}

impl Host for Wakes {
    fn wake(&self, request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
        Box::pin(async move {
            self.requests.locked().push(request.clone());
            if let Some(error) = self.errors.locked().pop_front() {
                return Err(error);
            }
            Ok(Woken { thread: request.thread.unwrap_or_else(ants_proto::ThreadId::generate) })
        })
    }
}

async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn an_empty_schedule_sleeps_until_maintenance_or_a_tool_changes_it() {
    let (sense, stores) = setup().await;
    let host = Arc::new(Wakes::default());
    let scheduler = start(sense.clone(), host.clone(), settings());
    settle().await;
    let reads = stores.reads.load(Ordering::SeqCst);
    tokio::time::advance(DAY - Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(stores.reads.load(Ordering::SeqCst), reads, "no trigger deadline means no polling before maintenance");
    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    settle().await;
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].id, format!("{id}:1"));
    assert_eq!(requests[0].thread.as_ref().unwrap().as_str(), "scheduler-thread");
    assert!(requests[0].text.contains("scheduled work") && requests[0].text.contains("reminder time reached"));
    scheduler.stop().await;
    let reads = stores.reads.load(Ordering::SeqCst);
    create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    settle().await;
    // The call's own: as it starts, caught up with the log, and as it writes.
    assert_eq!(stores.reads.load(Ordering::SeqCst), reads + 2, "stopped scheduler no longer reads");
    assert_eq!(host.requests.locked().len(), 1);
}

fn lifecycle(kind: ants_proto::provider::EventKind) -> ants_proto::provider::Event {
    ants_proto::provider::Event {
        id: uuid::Uuid::new_v4(),
        ant: ant(),
        identity: "0f1e".into(),
        thread: None,
        at: Utc::now(),
        kind,
    }
}

/// A retired ant's reminders fire nothing while it is dormant; its revival fires those due meanwhile.
#[tokio::test(start_paused = true)]
async fn a_dormant_ant_s_reminders_wait_for_its_revival() {
    use ants_proto::provider::EventKind;
    let (sense, _stores) = setup().await;
    sense.event(lifecycle(EventKind::AntAdded)).await.unwrap();
    sense.event(lifecycle(EventKind::AntRetired)).await.unwrap();
    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    let host = Arc::new(Wakes::default());
    let scheduler = start(sense.clone(), host.clone(), settings());
    settle().await;
    tokio::time::advance(DAY).await;
    settle().await;
    assert!(host.requests.locked().is_empty(), "nothing fires while dormant");
    sense.event(lifecycle(EventKind::AntRevived)).await.unwrap();
    settle().await;
    let requests = host.requests.locked().clone();
    assert_eq!(requests.iter().map(|request| request.id.clone()).collect::<Vec<_>>(), [format!("{id}:1")]);
    scheduler.stop().await;
}

#[tokio::test(start_paused = true)]
async fn failed_and_throttled_deliveries_retry_without_advancing_or_spinning() {
    let (sense, stores) = setup().await;
    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    let host = Arc::new(Wakes::default());
    host.errors
        .locked()
        .extend([RpcError::unavailable("disconnected"), RpcError::unavailable("woken 60 times within the hour")]);
    let scheduler = start(sense.clone(), host.clone(), settings());
    settle().await;
    let fired = reminder(&*stores.items, &id).await;
    assert_eq!(fired.number, 1);
    assert!(fired.delivered.is_none());
    assert_eq!(remind::pending_work(&*stores.items).await.unwrap()[0].id, format!("{id}:1"));
    create(&sense, json!({"kind":"after","seconds":3600})).await;
    settle().await;
    assert_eq!(host.requests.locked().len(), 1, "a change does not bypass retry backoff");
    tokio::time::advance(RETRY).await;
    settle().await;
    assert_eq!(host.requests.locked().len(), 2);
    tokio::time::advance(RETRY).await;
    settle().await;
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| request.id == format!("{id}:1")));
    assert!(remind::pending_work(&*stores.items).await.unwrap().is_empty());
    assert!(reminder(&*stores.items, &id).await.delivered.is_some());
    scheduler.stop().await;
}

#[tokio::test]
async fn takeover_delivers_persisted_pending_work_before_new_due_work() {
    let log = SharedLog::default();
    let identity = SenseId::new();
    let first = open(log.clone(), identity.clone()).await;
    let sense = Sense::with_stores(Arc::new(One { items: first.clone(), reads: AtomicUsize::new(0) }));
    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    let pending = remind::prepare_firing(&*first, &id, Utc::now(), "original cause").await.unwrap().unwrap();
    let newer = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    drop(sense);
    drop(first);
    let takeover = open(log, identity).await;
    let host = Wakes::default();
    let next = cycle(&Sense::new(), &*takeover, &host, &settings(), Utc::now()).await.unwrap().unwrap();
    let requests = host.requests.locked().clone();
    assert_eq!(
        requests.iter().map(|request| request.id.as_str()).collect::<Vec<_>>(),
        [pending.id, format!("{newer}:1")]
    );
    assert!(requests[0].text.contains("original cause"));
    assert!(next > Utc::now());
    assert!(remind::pending_work(&*takeover).await.unwrap().is_empty());
}

#[tokio::test]
async fn only_an_unknown_source_thread_falls_back_to_a_new_thread_with_the_same_id() {
    let (sense, stores) = setup().await;
    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    let host = Wakes::default();
    host.errors.locked().push_back(RpcError::new(RpcError::UNKNOWN_THREAD, "no session"));
    cycle(&sense, &*stores.items, &host, &settings(), Utc::now()).await.unwrap();
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].thread.is_some() && requests[1].thread.is_none());
    assert_eq!(requests[0].id, requests[1].id);
    assert_eq!(requests[1].id, format!("{id}:1"));

    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    // An invalid parameter is no unknown thread, however worded.
    host.errors
        .locked()
        .push_back(RpcError::invalid_params("thread scheduler-thread is no thread of ant dev-scheduler"));
    assert!(cycle(&sense, &*stores.items, &host, &settings(), Utc::now()).await.is_err());
    assert_eq!(host.requests.locked().len(), 3);
    assert_eq!(remind::pending_work(&*stores.items).await.unwrap()[0].id, format!("{id}:1"));
}

#[tokio::test(start_paused = true)]
async fn a_missing_host_stays_quiet_without_polling() {
    let scheduler = start(Arc::new(Sense::new()), Arc::new(Wakes::default()), settings());
    settle().await;
    tokio::time::advance(DAY).await;
    settle().await;
    assert!(scheduler.0.lock().await.as_ref().is_some_and(|task| !task.is_finished()));
    scheduler.stop().await;
}

#[tokio::test]
async fn a_reminder_without_a_source_thread_wakes_on_a_new_thread() {
    let (sense, stores) = setup().await;
    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    stores.items.change(&id, json!({"thread":null}).as_object().unwrap().clone(), None, "test").await.unwrap();
    let host = Wakes::default();
    cycle(&sense, &*stores.items, &host, &settings(), Utc::now()).await.unwrap();
    assert!(host.requests.locked()[0].thread.is_none());
    assert!(remind::pending_work(&*stores.items).await.unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn maintenance_rechecks_citations_on_start_then_daily_without_trigger_polling() {
    use ants_sense::memory::{Citation, Memory, MemoryKind, Scope, Status};

    let root = ants_test_support::scratch_dir("scheduler-maintenance");
    std::fs::create_dir(root.join(".git")).unwrap();
    std::fs::write(root.join(".git/config"), "[remote \"origin\"]\nurl = https://github.com/ants/citations\n").unwrap();
    std::fs::write(root.join("source"), "changed").unwrap();
    let (sense, stores) = setup().await;
    let status = async |id: &ItemId| Memory::of(&stores.items.get(id).await.unwrap().unwrap()).unwrap().status;
    let mut memory = Memory::new(
        MemoryKind::Fact,
        Scope::Ant(ant()),
        "source".into(),
        "cited source".into(),
        &call(json!({})).context,
    );
    memory.citations = vec![Citation {
        path: "source".into(),
        start: Some(1),
        end: Some(1),
        remote: Some("github.com/ants/citations".into()),
        hash: Some("ab".repeat(32)),
    }];
    let first = ItemId::new();
    stores.items.write(&[memory.create(first.clone())], "test").await.unwrap();
    let settings = || Settings { workdir: root.clone(), ..settings() };
    let scheduler = start(sense.clone(), Arc::new(Wakes::default()), settings());
    for _ in 0..100 {
        if status(&first).await == Status::NeedsReview {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(status(&first).await, Status::NeedsReview);
    let run = memory::last_maintenance(&*stores.items).await.unwrap().unwrap();
    assert_eq!((run.citations_checked, run.citations_marked), (1, 1));
    let second = ItemId::new();
    stores.items.write(&[memory.create(second.clone())], "test").await.unwrap();
    let reads = stores.reads.load(Ordering::SeqCst);
    tokio::time::advance(DAY - Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(stores.reads.load(Ordering::SeqCst), reads, "no minute polling");
    assert_eq!(status(&second).await, Status::Active);
    scheduler.stop().await;
    let day_later = run.ran + chrono::Duration::hours(24);
    cycle(&sense, &*stores.items, &Wakes::default(), &settings(), day_later).await.unwrap();
    assert_eq!(status(&second).await, Status::NeedsReview);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn a_takeover_keeps_the_last_maintenance_run() {
    let log = SharedLog::default();
    let identity = SenseId::new();
    let first = open(log.clone(), identity.clone()).await;
    let now = Utc::now();
    let next = cycle(&Sense::new(), &*first, &Wakes::default(), &settings(), now).await.unwrap().unwrap();
    let ran = memory::last_maintenance(&*first).await.unwrap().unwrap().ran;
    assert_eq!(next, ran + chrono::Duration::hours(24), "an empty schedule sleeps until maintenance");
    drop(first);
    let takeover = open(log, identity).await;
    let hour_later = now + chrono::Duration::hours(1);
    let next = cycle(&Sense::new(), &*takeover, &Wakes::default(), &settings(), hour_later).await.unwrap();
    assert_eq!(memory::last_maintenance(&*takeover).await.unwrap().unwrap().ran, ran, "not again within a day");
    assert_eq!(next, Some(ran + chrono::Duration::hours(24)));
    let day_later = ran + chrono::Duration::hours(24);
    cycle(&Sense::new(), &*takeover, &Wakes::default(), &settings(), day_later).await.unwrap();
    assert!(memory::last_maintenance(&*takeover).await.unwrap().unwrap().ran >= day_later);
}

#[tokio::test(start_paused = true)]
async fn waiting_subscribes_before_read_and_honors_deadlines() {
    let (sender, mut changes) = watch::channel(());
    sender.send_replace(());
    assert!(wait(&mut changes, None).await, "change during read is retained");
    let before = tokio::time::Instant::now();
    assert!(wait(&mut changes, Some(Utc::now() + chrono::Duration::hours(1))).await);
    assert!(before.elapsed() >= Duration::from_secs(3599));
}
