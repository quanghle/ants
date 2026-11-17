//! The provider interface in process: [`Provider`], which built-in sense implements and an external provider's client
//! serves over the protocol, and [`Host`], what ants serves a provider. [`Connected`] is a provider past initialize,
//! calling it only for the capabilities agreed.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use super::{
    Ack, Agreed, AntRef, Brief, BriefRequest, Capabilities, Capability, Event, Export, ImportRequest, InitializeParams,
    InitializeResult, MemoriesView, NegotiationError, RpcError, ToolCall, ToolList, ToolResult, WakeRequest, Woken,
    method, negotiate,
};

/// The future each method of the interface returns.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A provider, one method per protocol method. Every method but [`Provider::initialize`] fails with
/// [`RpcError::METHOD_NOT_FOUND`] unless implemented: a provider implements those of the capabilities it declares.
pub trait Provider: Send + Sync {
    /// Answer ants' initialize ([`InitializeParams::accept`]), keeping `host` to call back on.
    fn initialize(
        &self,
        params: InitializeParams,
        host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>>;

    fn list_tools(&self) -> BoxFuture<'_, Result<ToolList, RpcError>> {
        not_found(method::TOOLS_LIST)
    }

    fn call_tool(&self, _call: ToolCall) -> BoxFuture<'_, Result<ToolResult, RpcError>> {
        not_found(method::TOOLS_CALL)
    }

    fn brief(&self, _request: BriefRequest) -> BoxFuture<'_, Result<Brief, RpcError>> {
        not_found(method::BRIEF)
    }

    fn event(&self, _event: Event) -> BoxFuture<'_, Result<Ack, RpcError>> {
        not_found(method::EVENT)
    }

    fn memories(&self, _ant: AntRef) -> BoxFuture<'_, Result<MemoriesView, RpcError>> {
        not_found(method::MEMORIES)
    }

    fn export(&self, _ant: AntRef) -> BoxFuture<'_, Result<Export, RpcError>> {
        not_found(method::EXPORT)
    }

    fn import(&self, _request: ImportRequest) -> BoxFuture<'_, Result<Ack, RpcError>> {
        not_found(method::IMPORT)
    }
}

/// What ants serves a provider: the requests a provider makes of ants.
pub trait Host: Send + Sync {
    /// Wake an ant with a message from the provider.
    fn wake(&self, request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>>;
}

fn not_found<T: Send + 'static>(method: &str) -> BoxFuture<'static, Result<T, RpcError>> {
    let error = RpcError::method_not_found(method);
    Box::pin(std::future::ready(Err(error)))
}

/// A provider past initialize, with what was agreed: it calls the provider only for the capabilities both sides
/// declared, refusing the others with [`RpcError::NOT_AGREED`], and passes on the provider's requests to ants the
/// same way. Cheap to clone.
#[derive(Clone)]
pub struct Connected {
    provider: Arc<dyn Provider>,
    agreed: Arc<Agreed>,
}

impl fmt::Debug for Connected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connected").field("agreed", &self.agreed).finish_non_exhaustive()
    }
}

impl Connected {
    /// Initialize `provider` with `params`, handing it `host` to call back on, and agree with its answer
    /// ([`negotiate`]).
    pub async fn connect(
        provider: Arc<dyn Provider>,
        params: InitializeParams,
        host: Arc<dyn Host>,
    ) -> Result<Self, NegotiationError> {
        let gate = Arc::new(HostGate { host, agreed: OnceLock::new() });
        let result = provider.initialize(params.clone(), gate.clone()).await.map_err(NegotiationError::Refused)?;
        let agreed = negotiate(&params, result)?;
        let _ = gate.agreed.set(agreed.capabilities);
        Ok(Self { provider, agreed: Arc::new(agreed) })
    }

    /// The provider's name, which prefixes the names its tools are served under.
    pub fn name(&self) -> &str {
        &self.agreed.provider.name
    }

    pub fn agreed(&self) -> &Agreed {
        &self.agreed
    }

    pub fn capabilities(&self) -> Capabilities {
        self.agreed.capabilities
    }

    pub fn has(&self, capability: Capability) -> bool {
        self.agreed.capabilities.has(capability)
    }

    fn agreed_for(&self, capability: Capability) -> Result<&dyn Provider, RpcError> {
        if self.has(capability) { Ok(self.provider.as_ref()) } else { Err(RpcError::not_agreed(capability)) }
    }

    pub async fn list_tools(&self) -> Result<ToolList, RpcError> {
        self.agreed_for(Capability::Tools)?.list_tools().await
    }

    pub async fn call_tool(&self, call: ToolCall) -> Result<ToolResult, RpcError> {
        self.agreed_for(Capability::Tools)?.call_tool(call).await
    }

    pub async fn brief(&self, request: BriefRequest) -> Result<Brief, RpcError> {
        self.agreed_for(Capability::Context)?.brief(request).await
    }

    pub async fn event(&self, event: Event) -> Result<Ack, RpcError> {
        self.agreed_for(Capability::Events)?.event(event).await
    }

    pub async fn memories(&self, ant: AntRef) -> Result<MemoriesView, RpcError> {
        self.agreed_for(Capability::Memories)?.memories(ant).await
    }

    pub async fn export(&self, ant: AntRef) -> Result<Export, RpcError> {
        self.agreed_for(Capability::Export)?.export(ant).await
    }

    pub async fn import(&self, request: ImportRequest) -> Result<Ack, RpcError> {
        self.agreed_for(Capability::Export)?.import(request).await
    }
}

/// The host a provider gets: ants' own, for the capabilities agreed once initialize is done.
struct HostGate {
    host: Arc<dyn Host>,
    agreed: OnceLock<Capabilities>,
}

impl Host for HostGate {
    fn wake(&self, request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
        if !self.agreed.get().is_some_and(|agreed| agreed.wake) {
            return Box::pin(std::future::ready(Err(RpcError::not_agreed(Capability::Wake))));
        }
        self.host.wake(request)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::provider::Info;

    /// Poll `future` to its end: the futures here never wait.
    fn ready<T>(future: impl Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("the future waits"),
        }
    }

    fn info(name: &str) -> Info {
        Info { name: name.to_owned(), version: "0.1.0".to_owned() }
    }

    /// A provider of tools that wakes ants, keeping the host it was handed.
    #[derive(Default)]
    struct Waking {
        host: Mutex<Option<Arc<dyn Host>>>,
    }

    impl Provider for Waking {
        fn initialize(
            &self,
            params: InitializeParams,
            host: Arc<dyn Host>,
        ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
            *self.host.lock().unwrap() = Some(host);
            let declared = Capabilities { tools: true, wake: true, ..Capabilities::NONE };
            Box::pin(std::future::ready(params.accept(&[1], info("waking"), declared)))
        }

        fn list_tools(&self) -> BoxFuture<'_, Result<ToolList, RpcError>> {
            Box::pin(std::future::ready(Ok(ToolList::default())))
        }
    }

    impl Waking {
        fn wake(&self) -> Result<Woken, RpcError> {
            let host = self.host.lock().unwrap().clone().expect("initialized");
            let request = WakeRequest { ant: "dev-a".parse().unwrap(), thread: None, text: "x".into(), id: "1".into() };
            ready(host.wake(request))
        }
    }

    struct Ants;

    impl Host for Ants {
        fn wake(&self, _request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
            Box::pin(std::future::ready(Ok(Woken { thread: "t-1".parse().unwrap() })))
        }
    }

    fn connect(provider: &Arc<Waking>, uses: Capabilities) -> Connected {
        let params = InitializeParams::new(info("ants"), uses);
        ready(Connected::connect(provider.clone(), params, Arc::new(Ants))).unwrap()
    }

    #[test]
    fn calls_go_through_only_for_capabilities_agreed() {
        let provider = Arc::new(Waking::default());
        let connected = connect(&provider, Capabilities { tools: true, context: true, ..Capabilities::NONE });
        assert_eq!(connected.name(), "waking");
        assert_eq!(connected.capabilities(), Capabilities { tools: true, ..Capabilities::NONE });
        assert_eq!(ready(connected.list_tools()), Ok(ToolList::default()));
        let context = ready(connected.memories(AntRef { ant: "dev-a".parse().unwrap(), identity: "ab".into() }));
        assert_eq!(context.unwrap_err().code, RpcError::NOT_AGREED);
        assert_eq!(provider.wake().unwrap_err().code, RpcError::NOT_AGREED, "ants did not declare wake");
    }

    #[test]
    fn a_method_declared_but_not_implemented_is_not_found() {
        let provider = Arc::new(Waking::default());
        let connected = connect(&provider, Capabilities::ALL);
        assert_eq!(provider.wake().unwrap().thread.as_str(), "t-1");
        let call = ToolCall {
            name: "x".into(),
            arguments: serde_json::Map::new(),
            context: crate::provider::CallContext {
                ant: "dev-a".parse().unwrap(),
                identity: "ab".into(),
                thread: "t-1".parse().unwrap(),
                origin: crate::provider::Origin {
                    by: crate::provider::Starter::Human { user: "alice".parse().unwrap() },
                    channel: None,
                },
                harness: "copilot".into(),
                model: None,
                machine: "studio".into(),
                repo: None,
                at: chrono::Utc::now(),
            },
            format: crate::provider::Format::Text,
        };
        assert_eq!(ready(connected.call_tool(call)).unwrap_err().code, RpcError::METHOD_NOT_FOUND);
    }

    #[test]
    fn a_refused_initialize_connects_nothing() {
        struct Refusing;
        impl Provider for Refusing {
            fn initialize(
                &self,
                params: InitializeParams,
                _host: Arc<dyn Host>,
            ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
                Box::pin(std::future::ready(params.accept(&[99], info("refusing"), Capabilities::ALL)))
            }
        }
        let params = InitializeParams::new(info("ants"), Capabilities::ALL);
        let refused = ready(Connected::connect(Arc::new(Refusing), params, Arc::new(Ants))).unwrap_err();
        assert!(matches!(refused, NegotiationError::Refused(e) if e.code == RpcError::UNSUPPORTED_VERSION));
    }
}
