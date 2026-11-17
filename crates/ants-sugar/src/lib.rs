//! sugar: ants' built-in identity, memory and work provider. It runs inside the daemon serving each ant and implements
//! the same provider interface ([`Provider`]) that external providers serve over the provider protocol, so the daemon
//! talks to it as to any other provider.
//!
//! sugar asks nothing of humans: no approvals, gates, review queues or notifications.

use std::sync::Arc;

use ants_proto::provider::{
    BoxFuture, Brief, BriefRequest, Capabilities, Host, Info, InitializeParams, InitializeResult, PROTOCOL_VERSION,
    Provider, RpcError, ToolCall, ToolList, ToolResult,
};

pub mod sanitize;
mod tools;

pub use tools::{Answer, MAX_TOOLS, TOOLS, ToolSpec};

/// The provider name sugar answers initialize with, the prefix of its tools' names (`sugar_recall`).
pub const NAME: &str = "sugar";

/// The protocol versions sugar speaks.
pub const VERSIONS: &[u32] = &[PROTOCOL_VERSION];

/// What sugar implements: its tools and the brief.
pub const CAPABILITIES: Capabilities = Capabilities { tools: true, context: true, ..Capabilities::NONE };

/// The built-in provider. One serves every ant of a daemon, each call naming its ant.
#[derive(Debug, Default)]
pub struct Sugar {}

impl Sugar {
    pub fn new() -> Self {
        Self::default()
    }
}

fn ready<T: Send + 'static>(value: T) -> BoxFuture<'static, T> {
    Box::pin(std::future::ready(value))
}

impl Provider for Sugar {
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
            return ready(Err(RpcError::invalid_params(&format!("sugar has no tool {:?}", call.name))));
        };
        ready(Ok((spec.handler)(self, &call).render(call.format)))
    }

    fn brief(&self, _request: BriefRequest) -> BoxFuture<'_, Result<Brief, RpcError>> {
        ready(Ok(Brief::default()))
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
            Poll::Pending => panic!("sugar waits"),
        }
    }

    struct Ants;

    impl Host for Ants {
        fn wake(&self, _request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
            unreachable!("sugar does not wake ants")
        }
    }

    fn connect() -> Connected {
        let params = InitializeParams::new(Info { name: "ants".into(), version: "0".into() }, Capabilities::ALL);
        ready(Connected::connect(Arc::new(Sugar::new()), params, Arc::new(Ants))).unwrap()
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
                at: "2025-01-01T12:00:00Z".parse().unwrap(),
            },
            format,
        }
    }

    #[test]
    fn sugar_agrees_on_its_tools_and_brief() {
        let sugar = connect();
        assert_eq!(sugar.name(), NAME);
        assert!(sugar.has(Capability::Tools) && sugar.has(Capability::Context));
        assert!(!sugar.has(Capability::Wake) && !sugar.has(Capability::Events));
        let tools = ready(sugar.list_tools()).unwrap().tools;
        let names: Vec<_> = tools.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(names, ["brief", "remember", "recall", "vote", "checkpoint", "handoff"]);
        let request = BriefRequest {
            context: call("brief", Format::Text).context,
            budget: 8_000,
            reason: ants_proto::provider::BriefReason::Started,
        };
        assert_eq!(ready(sugar.brief(request)).unwrap(), Brief::default());
    }

    #[test]
    fn tools_not_built_yet_say_so_as_text_or_json() {
        let sugar = connect();
        let text = ready(sugar.call_tool(call("recall", Format::Text))).unwrap();
        assert!(text.is_error);
        assert_eq!(text.output, Output::Text("sugar_recall is not available yet.".into()));
        let json = ready(sugar.call_tool(call("recall", Format::Json))).unwrap();
        assert!(json.is_error);
        let Output::Json(json) = json.output else { panic!("asked for JSON") };
        assert_eq!(json["error"], "not_available");
        let unknown = ready(sugar.call_tool(call("dance", Format::Text))).unwrap_err();
        assert_eq!(unknown.code, RpcError::INVALID_PARAMS);
    }
}
