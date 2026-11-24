//! sense: ants' built-in identity, memory and work provider. It runs inside the daemon serving each ant and implements
//! the same provider interface ([`Provider`]) that external providers serve over the provider protocol, so the daemon
//! talks to it as to any other provider.
//!
//! sense asks nothing of humans: no approvals, gates, review queues or notifications.

#![warn(unnameable_types)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use ants_proto::UserId;
use ants_proto::provider::{
    Ack, AntRef, BoxFuture, Brief, BriefRequest, CallContext, Capabilities, Event, EventKind, Export, Host,
    ImportRequest, Info, InitializeParams, InitializeResult, MemoriesView, PROTOCOL_VERSION, Provider, RpcError,
    ToolCall, ToolList, ToolResult,
};

pub(crate) mod brief;
pub(crate) mod cache;
pub mod dream;
pub mod export;
pub(crate) mod fork;
pub mod home;
pub mod identity;
pub mod items;
pub mod log;
pub mod memory;
pub mod persona;
pub(crate) mod promise;
pub(crate) mod proposal;
pub(crate) mod records;
pub mod remind;
pub(crate) mod sanitize;
pub mod skill;
pub mod store;
pub(crate) mod task;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
pub(crate) mod thread;
mod tools;
pub(crate) mod view;
pub mod when;

pub use tools::{Answer, DreamWrite, MAX_TOOLS, TOOLS, ToolSpec};
pub use view::REMINDERS;

use store::{Items, StoreError, Stores};

/// The provider name sense answers initialize with, the prefix of its tools' names (`sense_recall`).
pub const NAME: &str = "sense";

/// The protocol versions sense speaks.
pub const VERSIONS: &[u32] = &[PROTOCOL_VERSION];

/// What sense implements: its tools, the brief, lifecycle events, which write each ant's card, the memories view, which
/// shows humans each ant's identity and memories (`view`), and exports, which take each ant's log and skill files to
/// another bus ([`export`]).
pub const CAPABILITIES: Capabilities =
    Capabilities { tools: true, context: true, events: true, wake: true, memories: true, export: true };

/// The built-in provider. One serves every ant of a daemon, each call naming its ant.
#[derive(Default)]
pub struct Sense {
    stores: Option<Arc<dyn Stores>>,
    workdirs: Option<Arc<dyn Workdirs>>,
    ants: Mutex<HashMap<UserId, Arc<Ant>>>,
}

/// What sense keeps of an ant between its calls.
#[derive(Default)]
struct Ant {
    /// Sent when a call changed the ant's items, waking its schedule.
    changed: tokio::sync::watch::Sender<()>,
    /// Taken by each write on a dream thread, counted toward [`dream::MAX_DREAM_WRITES`], from the check of the cap
    /// until counted, so concurrent writes cannot pass the cap together.
    dream_writes: Arc<tokio::sync::Mutex<()>>,
    /// The zone its reminders' times of day are read in ([`Sense::read_times_in`]); UTC when None.
    timezone: Mutex<Option<when::TimeZone>>,
}

/// Directories supplied by the daemon, never taken from a tool's arguments.
pub trait Workdirs: Send + Sync {
    fn workdir(&self, context: &CallContext) -> Option<PathBuf>;
}

impl Sense {
    /// sense keeping no data: its tools that need some refuse.
    pub fn new() -> Self {
        Self::default()
    }

    /// sense reaching each ant's data through `stores`.
    pub fn with_stores(stores: Arc<dyn Stores>) -> Self {
        Self { stores: Some(stores), ..Self::default() }
    }

    #[must_use]
    pub fn with_workdirs(mut self, workdirs: Arc<dyn Workdirs>) -> Self {
        self.workdirs = Some(workdirs);
        self
    }

    pub fn workdir(&self, context: &CallContext) -> Option<PathBuf> {
        self.workdirs.as_ref()?.workdir(context)
    }

    /// The home of `ant`, where its skill files are kept ([`skill::materialize`]); none when the stores know none.
    pub fn home(&self, ant: &UserId) -> Option<PathBuf> {
        self.stores.as_ref()?.home(ant)
    }

    fn ant(&self, ant: &UserId) -> Arc<Ant> {
        self.ants.lock().unwrap_or_else(std::sync::PoisonError::into_inner).entry(ant.clone()).or_default().clone()
    }

    /// Read the times of day of the reminders `ant` makes in `zone`, as the config file serving it says: its
    /// top-level `timezone`, else UTC.
    pub fn read_times_in(&self, ant: &UserId, zone: when::TimeZone) {
        *self.ant(ant).timezone.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(zone);
    }

    /// The zone the times of day of the reminders `ant` makes are read in ([`Sense::read_times_in`]).
    pub fn timezone(&self, ant: &UserId) -> when::TimeZone {
        let zone = self.ant(ant).timezone.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        zone.unwrap_or(when::TimeZone::UTC)
    }

    /// The changes of the items of `ant`. Subscribe before reading triggers, so a change during the
    /// read cannot be missed.
    pub fn changes(&self, ant: &UserId) -> tokio::sync::watch::Receiver<()> {
        self.ant(ant).changed.subscribe()
    }

    /// Notify the ant's schedule of writes, including those made outside provider calls.
    pub fn changed(&self, ant: &UserId) {
        self.ant(ant).changed.send_replace(());
    }

    /// The items of `ant`, served from the cache: the provider call reading them caught them up with the log as it
    /// started ([`Sense::synced`]).
    pub async fn items(&self, ant: &UserId) -> Result<Arc<dyn Items>, StoreError> {
        match &self.stores {
            Some(stores) => stores.items(ant).await,
            None => Err(StoreError::NotKept("no stores".into())),
        }
    }

    /// Whether the link reaching the log of `ant` waits to be tried again ([`Stores::link_retrying`]).
    fn link_retrying(&self, ant: &UserId) -> bool {
        self.stores.as_ref().is_some_and(|stores| stores.link_retrying(ant))
    }

    /// The items of `ant` caught up with its log, as a provider call starts ([`Stores::synced`]).
    pub async fn synced(&self, ant: &UserId) -> Result<Arc<dyn Items>, StoreError> {
        match &self.stores {
            Some(stores) => stores.synced(ant).await,
            None => Err(StoreError::NotKept("no stores".into())),
        }
    }

    /// Run `call`, a tool call on the items of `ant`, once they caught up with its log, and wake the ant's schedule if
    /// the call changed them: its log or the writes waiting for it. A log failing other than by being out of reach
    /// fails the call, its reads no longer meeting the log.
    async fn changing(&self, ant: &UserId, call: impl Future<Output = Answer>) -> Answer {
        async fn written(items: &dyn Items) -> (u64, usize) {
            (items.log_bytes().await, items.pending().await.unwrap_or_default())
        }
        let items = match self.synced(ant).await {
            Ok(items) => items,
            Err(StoreError::NotKept(_)) => return call.await,
            Err(error) => return error.into(),
        };
        let before = written(&*items).await;
        let output = call.await;
        if written(&*items).await != before {
            self.changed(ant);
        }
        output
    }

    async fn record_event(&self, event: Event) -> Result<Ack, RpcError> {
        let (card, thread) = (identity::touches_card(&event.kind), thread::record::touches_record(&event.kind));
        if !card && !thread {
            return Ok(Ack {});
        }
        let recorded = async {
            let items = self.synced(&event.ant).await?;
            if thread {
                return thread::record::record_event(&*items, &event).await.map(drop);
            }
            if let EventKind::AntCloned { from, .. } = &event.kind {
                self.fork(&*items, from, &event).await?;
            }
            identity::record_event(&*items, &event).await?;
            // A dormancy starting or ending pauses or resumes the ant's reminders.
            self.changed(&event.ant);
            Ok(())
        };
        match recorded.await {
            Ok(()) => Ok(Ack {}),
            // Between retries of the link, data kept none of now may be kept moments later: the event is told again.
            Err(StoreError::NotKept(why)) if self.link_retrying(&event.ant) => {
                Err(RpcError::unavailable(&format!("the event waits for sense's link: {why}")))
            }
            // Sense keeps no data of the ant here at all. Data only not open yet is unavailable instead: the event is
            // told again.
            Err(StoreError::NotKept(_)) => Ok(Ack {}),
            Err(e) => Err(RpcError::unavailable(&e.to_string())),
        }
    }

    /// Fork the identity of `from`, which `event` cloned, into `clone`'s: before the card notes the event, so the
    /// event delivered again after a failed fork forks again, and not once it has. Nothing to fork when sense keeps no
    /// data of `from`, unless its link waits to be tried again.
    async fn fork(&self, clone: &dyn Items, from: &UserId, event: &Event) -> Result<(), StoreError> {
        if identity::card(clone).await?.is_some_and(|card| card.events.contains(&event.id)) {
            return Ok(());
        }
        let source = match self.synced(from).await {
            Ok(source) => source,
            Err(StoreError::NotKept(why)) if self.link_retrying(from) => {
                return Err(StoreError::Unavailable(format!("the fork waits for sense's link: {why}")));
            }
            Err(StoreError::NotKept(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        if source.sense().await == clone.sense().await {
            return Ok(());
        }
        fork::fork(&*source, clone, event.at).await.map(drop)
    }

    async fn export_of(&self, ant: AntRef) -> Result<Export, RpcError> {
        let stores = self.stores.as_ref().ok_or_else(|| RpcError::unavailable("sense keeps no data here"))?;
        skill::materialize_for(self, &ant.ant)
            .await
            .map_err(|e| RpcError::unavailable(&format!("writing the skill files: {e}")))?;
        let export = stores.export(&ant.ant).await.map_err(|e| RpcError::unavailable(&e.to_string()))?;
        let data = serde_json::to_value(export).map_err(|e| RpcError::internal(&e.to_string()))?;
        Ok(Export { provider: NAME.to_owned(), data })
    }

    async fn import_into(&self, request: ImportRequest) -> Result<Ack, RpcError> {
        if request.export.provider != NAME {
            return Err(RpcError::invalid_params(&format!("an export of {}, not of sense", request.export.provider)));
        }
        let export: export::SenseExport = serde_json::from_value(request.export.data)
            .map_err(|e| RpcError::invalid_params(&format!("not an export of sense: {e}")))?;
        let stores = self.stores.as_ref().ok_or_else(|| RpcError::unavailable("sense keeps no data here"))?;
        match stores.import(&request.ant, export).await {
            Ok(()) => {
                self.changed(&request.ant);
                Ok(Ack {})
            }
            Err(StoreError::Import(e @ export::ImportError::Invalid(_))) => {
                Err(RpcError::invalid_params(&e.to_string()))
            }
            Err(e) => Err(RpcError::unavailable(&e.to_string())),
        }
    }

    /// The brief of the session starting as `request` tells, noting the session on the ant's card first; empty when
    /// sense keeps no data of the ant.
    async fn brief_for(&self, request: BriefRequest) -> Result<Brief, RpcError> {
        let context = &request.context;
        let composed = async {
            let items = self.synced(&context.ant).await?;
            let card = identity::record_session(&*items, context).await?;
            if let Some(workdir) = self.workdir(context) {
                memory::recheck_citations(&*items, &workdir).await?;
            }
            // Files failing to be written are written again at the next brief.
            let _ = skill::materialize_for(self, &context.ant).await;
            brief::compose(&*items, context, Some(&card), request.budget as usize).await
        };
        match composed.await {
            Ok(text) => Ok(Brief { text }),
            Err(StoreError::NotKept(_)) => Ok(Brief::default()),
            Err(e) => Err(RpcError::unavailable(&e.to_string())),
        }
    }
}

impl std::fmt::Debug for Sense {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sense").field("stores", &self.stores.is_some()).finish()
    }
}

fn ready<T: Send + 'static>(value: T) -> BoxFuture<'static, T> {
    Box::pin(std::future::ready(value))
}

impl Provider for Sense {
    fn initialize(
        &self,
        params: InitializeParams,
        _host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
        let info = Info { name: NAME.to_owned(), version: env!("CARGO_PKG_VERSION").to_owned() };
        ready(params.accept(VERSIONS, info, CAPABILITIES))
    }

    fn list_tools(&self) -> BoxFuture<'_, Result<ToolList, RpcError>> {
        ready(Ok(ToolList { tools: TOOLS.iter().map(ToolSpec::tool).collect() }))
    }

    fn call_tool(&self, call: ToolCall) -> BoxFuture<'_, Result<ToolResult, RpcError>> {
        let Some(spec) = TOOLS.iter().find(|spec| spec.name == call.name) else {
            return ready(Err(RpcError::invalid_params(&format!("sense has no tool {:?}", call.name))));
        };
        Box::pin(async move {
            let answer = self.changing(&call.context.ant, async {
                if spec.dream.is_some() {
                    dream::counted(self, spec, &call).await
                } else {
                    (spec.handler)(self, &call).await
                }
            });
            Ok(answer.await.render(call.format))
        })
    }

    fn brief(&self, request: BriefRequest) -> BoxFuture<'_, Result<Brief, RpcError>> {
        Box::pin(self.brief_for(request))
    }

    fn event(&self, event: Event) -> BoxFuture<'_, Result<Ack, RpcError>> {
        Box::pin(self.record_event(event))
    }

    fn memories(&self, ant: AntRef) -> BoxFuture<'_, Result<MemoriesView, RpcError>> {
        Box::pin(view::memories(self, ant))
    }

    fn export(&self, ant: AntRef) -> BoxFuture<'_, Result<Export, RpcError>> {
        Box::pin(self.export_of(ant))
    }

    fn import(&self, request: ImportRequest) -> BoxFuture<'_, Result<Ack, RpcError>> {
        Box::pin(self.import_into(request))
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll, Waker};

    use ants_proto::provider::{
        CallContext, Capability, Connected, Format, Origin, Output, Starter, WakeRequest, Woken,
    };

    use super::*;

    fn ready<T>(future: impl Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("sense waits"),
        }
    }

    struct Ants;

    impl Host for Ants {
        fn wake(&self, _request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
            unreachable!("sense does not wake ants")
        }
    }

    fn connect() -> Connected {
        let params = InitializeParams::new(Info { name: "ants".into(), version: "0".into() }, Capabilities::ALL);
        ready(Connected::connect(Arc::new(Sense::new()), params, Arc::new(Ants))).unwrap()
    }

    fn call(name: &str, format: Format) -> ToolCall {
        ToolCall {
            name: name.to_owned(),
            arguments: serde_json::Map::new(),
            context: CallContext {
                ant: "dev-a".parse().unwrap(),
                identity: "0f1e".into(),
                thread: "019a7d2c-0000-7000-8000-000000000001".parse().unwrap(),
                origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
                harness: "copilot".into(),
                model: None,
                machine: "studio".into(),
                repo: None,
                at: "2025-01-01T12:00:00Z".parse().unwrap(),
            },
            format,
        }
    }

    #[test]
    fn sense_agrees_on_its_tools_brief_and_events() {
        let sense = connect();
        assert_eq!(sense.name(), NAME);
        assert!(sense.has(Capability::Tools) && sense.has(Capability::Context) && sense.has(Capability::Events));
        assert!(sense.has(Capability::Wake) && sense.has(Capability::Memories) && sense.has(Capability::Export));
        let tools = ready(sense.list_tools()).unwrap().tools;
        let names: Vec<_> = tools.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "brief",
                "remember",
                "recall",
                "vote",
                "checkpoint",
                "handoff",
                "task",
                "promise",
                "remind",
                "identity",
                "skill",
                "dream"
            ]
        );
        let request = BriefRequest {
            context: call("brief", Format::Text).context,
            budget: 8_000,
            reason: ants_proto::provider::BriefReason::Started,
        };
        assert_eq!(ready(sense.brief(request)).unwrap(), Brief::default());
    }

    #[test]
    fn tools_of_a_sense_keeping_no_data_refuse_as_text_or_json() {
        let sense = connect();
        let text = ready(sense.call_tool(call("brief", Format::Text))).unwrap();
        assert!(text.is_error);
        let Output::Text(text) = text.output else { panic!("asked for text") };
        assert!(text.starts_with("sense keeps no data here"), "{text}");
        let json = ready(sense.call_tool(call("brief", Format::Json))).unwrap();
        assert!(json.is_error);
        let Output::Json(json) = json.output else { panic!("asked for JSON") };
        assert_eq!(json["error"], "unavailable");
        let unknown = ready(sense.call_tool(call("dance", Format::Text))).unwrap_err();
        assert_eq!(unknown.code, RpcError::INVALID_PARAMS);
    }
}
