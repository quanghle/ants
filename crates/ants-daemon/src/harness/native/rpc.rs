//! A JSON-RPC 2.0 client over JSON lines, for a CLI serving its protocol on stdio (such as `codex app-server`): requests
//! matched with their responses by id, notifications both ways, and the CLI's own requests to answer.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};

use super::io::{JsonLines, JsonWriter};
use crate::sync::MutexExt as _;

/// What the CLI sends on its own: a notification, or a request [`Rpc::respond`] answers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Incoming {
    Notification { method: String, params: Value },
    Request { id: Value, method: String, params: Value },
}

/// An error response, or the connection's end before the response.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

/// The code of an [`RpcError`] for a request the connection ended before its response.
pub(crate) const CLOSED: i64 = -32099;

impl RpcError {
    fn closed() -> Self {
        Self { code: CLOSED, message: "the connection closed".to_owned(), data: None }
    }

    /// Whether the connection ended before the response, as when the CLI exited: then
    /// [`super::io::Process::exited`] tells why.
    pub(crate) fn is_closed(&self) -> bool {
        self.code == CLOSED
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.code)?;
        if let Some(data) = &self.data {
            write!(f, ": {data}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RpcError {}

type Pending = HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>;

/// The response to a request sent ([`Rpc::send_request`]).
pub(crate) struct Response(oneshot::Receiver<Result<Value, RpcError>>);

impl Response {
    /// Wait for it: the request's result, or its error, the connection's end included.
    pub(crate) async fn wait(self) -> Result<Value, RpcError> {
        self.0.await.unwrap_or_else(|_| Err(RpcError::closed()))
    }
}

/// A JSON-RPC client. Dropping it stops reading.
pub(crate) struct Rpc<W> {
    writer: JsonWriter<W>,
    /// Requests awaiting their response; None once the input ended.
    pending: Arc<Mutex<Option<Pending>>>,
    next_id: AtomicU64,
    /// Whether messages carry `"jsonrpc": "2.0"`, which some CLIs leave out.
    versioned: bool,
    reader: tokio::task::JoinHandle<()>,
}

impl<W> Drop for Rpc<W> {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl<W: AsyncWrite + Unpin> Rpc<W> {
    /// Speak JSON-RPC with a CLI over `reader`, its output, and `writer`, its input, putting `"jsonrpc": "2.0"` in
    /// every message when `versioned`. Request ids count from 1. What the CLI sends on its own comes in on the
    /// receiver, which ends with its output.
    pub(crate) fn new(
        reader: impl AsyncRead + Unpin + Send + 'static,
        writer: W,
        versioned: bool,
    ) -> (Self, mpsc::UnboundedReceiver<Incoming>) {
        let pending = Arc::new(Mutex::new(Some(Pending::new())));
        let (tx, rx) = mpsc::unbounded_channel();
        let reader = tokio::spawn(read(JsonLines::new(reader), pending.clone(), tx));
        let rpc = Self { writer: JsonWriter::new(writer), pending, next_id: AtomicU64::new(1), versioned, reader };
        (rpc, rx)
    }

    /// Send a request and wait for its response.
    pub(crate) async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        self.send_request(method, params).await?.wait().await
    }

    /// Send a request, its response to be waited for later, or not at all.
    pub(crate) async fn send_request(&self, method: &str, params: Value) -> Result<Response, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        match self.pending.locked().as_mut() {
            Some(pending) => pending.insert(id, tx),
            None => return Err(RpcError::closed()),
        };
        let sent = self.send(json!({ "id": id, "method": method, "params": params })).await;
        if sent.is_err() {
            if let Some(pending) = self.pending.locked().as_mut() {
                pending.remove(&id);
            }
            return Err(RpcError::closed());
        }
        Ok(Response(rx))
    }

    /// Send a notification.
    pub(crate) async fn notify(&self, method: &str, params: Value) -> std::io::Result<()> {
        self.send(json!({ "method": method, "params": params })).await
    }

    /// Answer the CLI's request `id` with `result`.
    pub(crate) async fn respond(&self, id: Value, result: Value) -> std::io::Result<()> {
        self.send(json!({ "id": id, "result": result })).await
    }

    /// Answer the CLI's request `id` with an error.
    pub(crate) async fn respond_error(&self, id: Value, code: i64, message: &str) -> std::io::Result<()> {
        self.send(json!({ "id": id, "error": { "code": code, "message": message } })).await
    }

    async fn send(&self, mut message: Value) -> std::io::Result<()> {
        if self.versioned
            && let Some(object) = message.as_object_mut()
        {
            object.insert("jsonrpc".to_owned(), "2.0".into());
        }
        self.writer.send(&message).await
    }
}

/// Route what the CLI writes until its output ends, then fail the requests left waiting.
async fn read(
    mut lines: JsonLines<impl AsyncRead + Unpin>,
    pending: Arc<Mutex<Option<Pending>>>,
    incoming: mpsc::UnboundedSender<Incoming>,
) {
    loop {
        let message = match lines.next().await {
            Ok(Some(Value::Object(message))) => message,
            Ok(Some(other)) => {
                tracing::warn!(message = %other, "skipping a JSON-RPC message that is no object");
                continue;
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(error = %e, "reading JSON-RPC");
                break;
            }
        };
        route(message, &pending, &incoming);
    }
    pending.locked().take();
}

fn route(
    mut message: Map<String, Value>,
    pending: &Mutex<Option<Pending>>,
    incoming: &mpsc::UnboundedSender<Incoming>,
) {
    let params = message.remove("params").unwrap_or(Value::Null);
    match (message.remove("id"), message.remove("method")) {
        (Some(id), Some(Value::String(method))) => {
            super::io::forward(incoming, Incoming::Request { id, method, params });
        }
        (None, Some(Value::String(method))) => super::io::forward(incoming, Incoming::Notification { method, params }),
        (Some(id), None) => {
            let waiting = id.as_u64().and_then(|id| pending.locked().as_mut()?.remove(&id));
            let Some(waiting) = waiting else {
                tracing::warn!(%id, "skipping a JSON-RPC response to no request");
                return;
            };
            let result = match message.remove("error") {
                Some(error) => Err(RpcError {
                    code: error.get("code").and_then(Value::as_i64).unwrap_or_default(),
                    message: error.get("message").and_then(Value::as_str).unwrap_or_default().to_owned(),
                    data: error.get("data").cloned(),
                }),
                None => Ok(message.remove("result").unwrap_or(Value::Null)),
            };
            let _ = waiting.send(result);
        }
        _ => tracing::warn!(?message, "skipping a JSON-RPC message of no kind"),
    }
}
