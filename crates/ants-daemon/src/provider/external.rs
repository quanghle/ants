//! The client of an external provider speaking the provider protocol over its stdio ([`ants_proto::provider`]): each
//! method a request, the provider's wake requests passed on to ants.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ants_proto::provider::jsonrpc::{Request, Response};
use ants_proto::provider::{
    Ack, AntRef, BoxFuture, Brief, BriefRequest, Event, Export, Host, ImportRequest, InitializeParams,
    InitializeResult, MemoriesView, Provider, RpcError, ToolCall, ToolList, ToolResult, WakeRequest, method,
};
use tokio::io::{AsyncRead, AsyncWrite};

use super::link::{Link, Serve};

/// How long the provider has to answer initialize.
pub(super) const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the provider has to answer any other request.
pub(super) const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// An external provider, spoken to over its output `reader` and input `writer`, and the link to it.
pub(super) fn client(
    name: &str,
    reader: impl AsyncRead + Send + Unpin + 'static,
    writer: impl AsyncWrite + Send + Unpin + 'static,
) -> (Arc<dyn Provider>, Arc<Link>) {
    let host = Arc::new(OnceLock::<Arc<dyn Host>>::new());
    let serve: Serve = {
        let host = host.clone();
        Arc::new(move |request: Request| {
            let host = host.clone();
            Box::pin(async move {
                let result = match request.method.as_str() {
                    method::WAKE => match (host.get(), request.params::<WakeRequest>()) {
                        (Some(host), Ok(wake)) => host.wake(wake).await,
                        (None, _) => Err(RpcError::unavailable("ants is not connected to the provider yet")),
                        (_, Err(e)) => Err(e),
                    },
                    other => Err(RpcError::method_not_found(other)),
                };
                Response::new(request.id, result)
            })
        })
    };
    let link = Arc::new(Link::new(name, reader, writer, serve));
    (Arc::new(External { link: link.clone(), host }), link)
}

struct External {
    link: Arc<Link>,
    host: Arc<OnceLock<Arc<dyn Host>>>,
}

impl Provider for External {
    fn initialize(
        &self,
        params: InitializeParams,
        host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
        let _ = self.host.set(host);
        Box::pin(async move { self.link.request(method::INITIALIZE, &params, INITIALIZE_TIMEOUT).await })
    }

    fn list_tools(&self) -> BoxFuture<'_, Result<ToolList, RpcError>> {
        Box::pin(self.link.request(method::TOOLS_LIST, &(), CALL_TIMEOUT))
    }

    fn call_tool(&self, call: ToolCall) -> BoxFuture<'_, Result<ToolResult, RpcError>> {
        Box::pin(async move { self.link.request(method::TOOLS_CALL, &call, CALL_TIMEOUT).await })
    }

    fn brief(&self, request: BriefRequest) -> BoxFuture<'_, Result<Brief, RpcError>> {
        Box::pin(async move { self.link.request(method::BRIEF, &request, CALL_TIMEOUT).await })
    }

    fn event(&self, event: Event) -> BoxFuture<'_, Result<Ack, RpcError>> {
        Box::pin(async move { self.link.request(method::EVENT, &event, CALL_TIMEOUT).await })
    }

    fn memories(&self, ant: AntRef) -> BoxFuture<'_, Result<MemoriesView, RpcError>> {
        Box::pin(async move { self.link.request(method::MEMORIES, &ant, CALL_TIMEOUT).await })
    }

    fn export(&self, ant: AntRef) -> BoxFuture<'_, Result<Export, RpcError>> {
        Box::pin(async move { self.link.request(method::EXPORT, &ant, CALL_TIMEOUT).await })
    }

    fn import(&self, request: ImportRequest) -> BoxFuture<'_, Result<Ack, RpcError>> {
        Box::pin(async move { self.link.request(method::IMPORT, &request, CALL_TIMEOUT).await })
    }
}
