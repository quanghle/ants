//! sense: ants' built-in identity, memory and work provider. It runs inside the daemon serving each ant and implements
//! the same provider interface ([`Provider`]) that external providers serve over the provider protocol, so the daemon
//! talks to it as to any other provider.
//!
//! sense asks nothing of humans: no approvals, gates, review queues or notifications.

use std::path::PathBuf;
use std::sync::Arc;

use ants_proto::UserId;
use ants_proto::provider::{
    Ack, BoxFuture, Brief, BriefRequest, CallContext, Capabilities, Event, Host, Info, InitializeParams,
    InitializeResult, PROTOCOL_VERSION, Provider, RpcError, ToolCall, ToolList, ToolResult,
};

pub mod brief;
pub mod cache;
pub mod identity;
pub mod items;
pub mod log;
pub mod memory;
pub mod promise;
pub mod records;
pub mod remind;
pub mod sanitize;
pub mod store;
pub mod task;
pub mod thread;
mod tools;

pub use tools::{Answer, MAX_TOOLS, TOOLS, ToolSpec};

use store::{Items, StoreError, Stores};

/// The provider name sense answers initialize with, the prefix of its tools' names (`sense_recall`).
pub const NAME: &str = "sense";

/// The protocol versions sense speaks.
pub const VERSIONS: &[u32] = &[PROTOCOL_VERSION];

/// What sense implements: its tools, the brief and lifecycle events, which write each ant's card.
pub const CAPABILITIES: Capabilities =
    Capabilities { tools: true, context: true, events: true, wake: true, ..Capabilities::NONE };

/// The built-in provider. One serves every ant of a daemon, each call naming its ant.
pub struct Sense {
    stores: Option<Arc<dyn Stores>>,
    workdirs: Option<Arc<dyn Workdirs>>,
    changed: tokio::sync::watch::Sender<()>,
}

/// Directories supplied by the daemon, never taken from a tool's arguments.
pub trait Workdirs: Send + Sync {
    fn workdir(&self, context: &CallContext) -> Option<PathBuf>;
}

impl Default for Sense {
    fn default() -> Self {
        Self { stores: None, workdirs: None, changed: tokio::sync::watch::channel(()).0 }
    }
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

    /// Subscribe before reading triggers, so a change during the read cannot be missed.
    pub fn changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// The items of `ant`.
    pub async fn items(&self, ant: &UserId) -> Result<Arc<dyn Items>, StoreError> {
        match &self.stores {
            Some(stores) => stores.items(ant).await,
            None => Err(StoreError::NotKept("no stores".into())),
        }
    }

    /// The identity card of `ant`; none before its first use of sense.
    pub async fn card(&self, ant: &UserId) -> Result<Option<identity::Card>, StoreError> {
        identity::card(&*self.items(ant).await?).await
    }

    async fn record_event(&self, event: Event) -> Result<Ack, RpcError> {
        if !identity::touches_card(&event.kind) {
            return Ok(Ack {});
        }
        let recorded = match self.items(&event.ant).await {
            Ok(items) => identity::record_event(&*items, &event).await.map(drop),
            Err(e) => Err(e),
        };
        match recorded {
            Ok(()) | Err(StoreError::NotKept(_)) => Ok(Ack {}),
            Err(e) => Err(RpcError::unavailable(&e.to_string())),
        }
    }

    /// The brief of the session starting as `request` tells, noting the session on the ant's card first; empty when
    /// sense keeps no data of the ant.
    async fn brief_for(&self, request: BriefRequest) -> Result<Brief, RpcError> {
        let context = &request.context;
        let composed = async {
            let items = self.items(&context.ant).await?;
            let card = identity::record_session(&*items, context).await?;
            if let Some(workdir) = self.workdir(context) {
                memory::recheck_citations(&*items, &workdir).await?;
            }
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
            let format = call.format;
            let answer = (spec.handler)(self, &call).await;
            if !answer.is_error {
                self.changed.send_replace(());
            }
            Ok(answer.render(format))
        })
    }

    fn brief(&self, request: BriefRequest) -> BoxFuture<'_, Result<Brief, RpcError>> {
        Box::pin(self.brief_for(request))
    }

    fn event(&self, event: Event) -> BoxFuture<'_, Result<Ack, RpcError>> {
        Box::pin(self.record_event(event))
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
        assert!(sense.has(Capability::Wake));
        let tools = ready(sense.list_tools()).unwrap().tools;
        let names: Vec<_> = tools.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(
            names,
            ["brief", "remember", "recall", "vote", "checkpoint", "handoff", "task", "promise", "remind"]
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
        assert!(text.starts_with("The brief is not available: sense keeps no data here"), "{text}");
        let json = ready(sense.call_tool(call("brief", Format::Json))).unwrap();
        assert!(json.is_error);
        let Output::Json(json) = json.output else { panic!("asked for JSON") };
        assert_eq!(json["error"], "unavailable");
        let unknown = ready(sense.call_tool(call("dance", Format::Text))).unwrap_err();
        assert_eq!(unknown.code, RpcError::INVALID_PARAMS);
    }
}
