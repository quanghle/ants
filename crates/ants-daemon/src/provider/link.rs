//! A JSON-RPC 2.0 link over an external provider's stdio, one [`Message`] per line: ants' requests matched with their
//! responses by id, the provider's requests served, its notifications logged and dropped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::provider::BoxFuture;
use ants_proto::provider::RpcError;
use ants_proto::provider::jsonrpc::{Message, Notification, Request, RequestId, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio::sync::{mpsc, oneshot, watch};

use crate::sync::MutexExt as _;

/// What answers the provider's requests.
pub(super) type Serve = Arc<dyn Fn(Request) -> BoxFuture<'static, Response> + Send + Sync>;

/// The longest line read; a longer one ends the link.
const MAX_LINE: usize = 16 << 20;

type Pending = Arc<Mutex<HashMap<i64, oneshot::Sender<Response>>>>;

/// One provider's stdio, as JSON-RPC. Dropping it closes the provider's stdin.
pub(super) struct Link {
    name: String,
    out: mpsc::UnboundedSender<String>,
    pending: Pending,
    next: AtomicI64,
    closed: watch::Receiver<Option<String>>,
    reader: tokio::task::JoinHandle<()>,
}

impl Link {
    /// Speak to the provider `name` reading its output from `reader` and writing to its input on `writer`, its
    /// requests answered by `serve`.
    pub(super) fn new(
        name: &str,
        reader: impl AsyncRead + Send + Unpin + 'static,
        writer: impl AsyncWrite + Send + Unpin + 'static,
        serve: Serve,
    ) -> Self {
        let (out, lines) = mpsc::unbounded_channel();
        let pending = Pending::default();
        let (closed_tx, closed) = watch::channel(None);
        tokio::spawn(write(writer, lines));
        let reader = tokio::spawn(read(name.to_owned(), reader, out.downgrade(), pending.clone(), serve, closed_tx));
        Self { name: name.to_owned(), out, pending, next: AtomicI64::new(1), closed, reader }
    }

    /// Call `method` with `params` and wait up to `wait` for its result.
    pub(super) async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &impl Serialize,
        wait: Duration,
    ) -> Result<T, RpcError> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let request = Request::new(RequestId::Number(id), method, params)
            .map_err(|e| RpcError::internal(&format!("writing the params of {method}: {e}")))?;
        let (answer, answered) = oneshot::channel();
        self.pending.locked().insert(id, answer);
        if self.closed.borrow().is_some() || self.out.send(Message::Request(request).to_line()).is_err() {
            self.pending.locked().remove(&id);
            return Err(self.gone());
        }
        match tokio::time::timeout(wait, answered).await {
            Ok(Ok(response)) => response.into_result(),
            Ok(Err(_)) => Err(self.gone()),
            Err(_) => {
                self.pending.locked().remove(&id);
                let why = format!("{} did not answer {method} within {} s", self.name, wait.as_secs());
                Err(RpcError::unavailable(&why))
            }
        }
    }

    /// Send the notification `method` with `params`.
    pub(super) fn notify(&self, method: &str, params: &impl Serialize) {
        match Notification::new(method, params) {
            Ok(notification) => {
                let _ = self.out.send(Message::Notification(notification).to_line());
            }
            Err(e) => tracing::warn!(provider = self.name, error = %e, "not sending {method}"),
        }
    }

    /// Wait for the provider's output to end; how it did.
    pub(super) async fn closed(&self) -> String {
        let mut closed = self.closed.clone();
        match closed.wait_for(Option::is_some).await {
            Ok(why) => why.clone().unwrap_or_default(),
            Err(_) => "closed its output".to_owned(),
        }
    }

    fn gone(&self) -> RpcError {
        let why = self.closed.borrow().clone().unwrap_or_else(|| "closed its input".to_owned());
        RpcError::unavailable(&format!("{} {why}", self.name))
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// Write `lines` to the provider's input until the link drops or the input closes.
async fn write(mut writer: impl AsyncWrite + Unpin, mut lines: mpsc::UnboundedReceiver<String>) {
    while let Some(line) = lines.recv().await {
        if writer.write_all(line.as_bytes()).await.is_err() || writer.flush().await.is_err() {
            break;
        }
    }
}

/// Read the provider's output to its end, passing responses to their requests and answering its requests with
/// `serve` on `out`, a line that is no message dropped; then say how it ended on `closed`, failing the requests still
/// waiting.
async fn read(
    name: String,
    reader: impl AsyncRead + Unpin,
    out: mpsc::WeakUnboundedSender<String>,
    pending: Pending,
    serve: Serve,
    closed: watch::Sender<Option<String>>,
) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    let why = loop {
        line.clear();
        match (&mut reader).take(MAX_LINE as u64 + 1).read_until(b'\n', &mut line).await {
            Ok(0) => break "closed its output".to_owned(),
            Ok(_) if line.len() > MAX_LINE => break format!("wrote a line over {} MiB", MAX_LINE >> 20),
            Ok(_) => {}
            Err(e) => break format!("could not be read: {e}"),
        }
        let text = String::from_utf8_lossy(&line);
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        match Message::from_line(text) {
            Ok(Message::Response(response)) => {
                let waiting = match &response.id {
                    Some(RequestId::Number(id)) => pending.locked().remove(id),
                    _ => None,
                };
                match waiting {
                    Some(waiting) => {
                        let _ = waiting.send(response);
                    }
                    None => tracing::debug!(provider = name, id = ?response.id, "dropping a response to no request"),
                }
            }
            Ok(Message::Request(request)) => {
                let (serve, out) = (serve.clone(), out.clone());
                tokio::spawn(async move {
                    let response = serve(request).await;
                    if let Some(out) = out.upgrade() {
                        let _ = out.send(Message::Response(response).to_line());
                    }
                });
            }
            Ok(Message::Notification(notification)) => {
                tracing::debug!(provider = name, method = notification.method, "dropping a notification");
            }
            Err(e) => {
                tracing::warn!(provider = name, error = %e, "dropping a line of the provider that is no message");
            }
        }
    };
    closed.send_replace(Some(why));
    pending.locked().clear();
}
