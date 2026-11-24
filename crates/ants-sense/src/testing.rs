//! What the tests of sense, and of the daemon's sense module, share: an ant's items kept in memory ([`store`]),
//! sense keeping them as the items of every ant ([`fixture`]) or of the ants named ([`ByAnt`]), and calls of its tools ([`call`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::provider::{BoxFuture, CallContext, Format, Origin, Starter, ToolCall};
use ants_proto::{SenseId, UserId};
use serde_json::Value;

use crate::Sense;
use crate::cache::Cache;
use crate::log::{
    AccountVerifier, AntLog, AppendError, Event, EventLog, LogError, MemoryLog, NoRevocations, Stored, Unsigned,
};
use crate::store::{Items, Shared, Store, StoreError, Stores};
use crate::tools::{Answer, Handler};

/// The items of an ant over a log of `L`, unsigned and verified against no account.
pub type Kept<L = MemoryLog> = Shared<L, Unsigned, AccountVerifier<NoRevocations>>;

/// What a test outside sense makes a proposal with.
pub mod proposal {
    pub use crate::proposal::{Decided, Proposal, ProposalItem, Status, propose};
}

/// The one store of every ant, counting how often it is asked for.
pub struct One {
    pub items: Arc<dyn Items>,
    reads: AtomicUsize,
}

impl One {
    pub fn new(items: Arc<dyn Items>) -> Self {
        Self { items, reads: AtomicUsize::new(0) }
    }

    /// How many times an ant's items were asked for.
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl Stores for One {
    fn items<'a>(&'a self, _: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(Ok(self.items.clone())))
    }
}

/// The items of the ants sense keeps, and none of any other.
pub struct ByAnt(pub HashMap<UserId, Arc<dyn Items>>);

impl Stores for ByAnt {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        let items = self.0.get(ant).cloned().ok_or_else(|| StoreError::NotKept(ant.to_string()));
        Box::pin(std::future::ready(items))
    }
}

/// A log in memory its clones share, so a store opened again reads what another wrote.
#[derive(Clone, Default)]
pub struct SharedLog(pub Arc<MemoryLog>);

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

/// The items of `ant` under sense id `sense` in `log`, on machine `m1`, cached in memory.
pub async fn store<L: EventLog>(log: L, sense: SenseId, ant: UserId) -> Kept<L> {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let log = AntLog::new(log, Unsigned, verifier, sense, ant, "m1".parse().unwrap(), None).unwrap();
    Shared::new(Store::open(log, Cache::in_memory().unwrap()).await.unwrap())
}

/// Sense keeping the items of `dev-a` in `log` as those of every ant, and those items.
pub async fn fixture<L: EventLog + Send + Sync + 'static>(log: L) -> (Sense, Arc<Kept<L>>) {
    let items = Arc::new(store(log, SenseId::new(), "dev-a".parse().unwrap()).await);
    (Sense::with_stores(Arc::new(One::new(items.clone()))), items)
}

/// [`fixture`] over a log of its own.
pub async fn sense() -> (Sense, Arc<Kept>) {
    fixture(MemoryLog::default()).await
}

/// The events of the log of `store`.
pub async fn events<L: EventLog>(store: &Kept<L>) -> Vec<Event> {
    let store = store.lock().await;
    let sense = store.log().chain().sense().clone();
    let stored = store.log().log().read_after(&sense, 0, 1_000).await.unwrap();
    stored.iter().map(|stored| stored.event().unwrap()).collect()
}

/// The context of a call of `dev-a` on a thread `by` started.
pub fn context(by: Starter) -> CallContext {
    CallContext {
        ant: "dev-a".parse().unwrap(),
        identity: "0f1e".into(),
        thread: "019a7d2c-0000-7000-8000-000000000001".parse().unwrap(),
        origin: Origin { by, channel: None },
        harness: "copilot".into(),
        model: Some("gpt".into()),
        machine: "studio".into(),
        repo: None,
        at: "2025-01-01T12:00:00Z".parse().unwrap(),
    }
}

/// The human `alice`.
pub fn human() -> Starter {
    Starter::Human { user: "alice".parse().unwrap() }
}

/// A call of tool `name` in `context` with `arguments`, a JSON object, answered as JSON.
pub fn call(name: &str, arguments: Value, context: CallContext) -> ToolCall {
    let arguments = arguments.as_object().unwrap().clone();
    ToolCall { name: name.to_owned(), arguments, context, format: Format::Json }
}

/// The answer of `handler`, as JSON, to a call of `context` with `arguments`.
pub async fn tool(sense: &Sense, handler: Handler, arguments: Value, context: CallContext) -> Answer {
    handler(sense, &call("tool", arguments, context)).await
}
