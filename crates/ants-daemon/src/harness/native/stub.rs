//! A test protocol over JSON-RPC, for the stub CLI of `ants-test-support` ([`ants_test_support::StubCli`]) to script:
//! it runs one CLI per connection, as `codex app-server` does, and uses the helpers every protocol has.
//!
//! The CLI answers `initialize` with `{name, version, fork, steer}`, `new` and `fork` with `{id}`, `load` with anything,
//! `prompt` with `{stop, tokens}` and `steer`, which adds `{session, text}` to the running prompt, with `{accepted}`,
//! failing as [`mapped`] reads. It notifies `update` with `{kind, text}` (`message`,
//! `thought`), `{kind: "tool", id, title}` or `{kind: "cost", usd}`, and may ask `approve`, which is denied. `cancel`
//! and `close` are notified to it. `mcp` is answered with `{servers}`, each an [`McpServerState`], and `mcp_sign_in`
//! with `{url}`, the address to sign in at, if any; the browser's return is requested on the loopback.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{ContentBlock, PromptResponse, StopReason, ToolCall, Usage};
use ants_proto::McpServerState;
use anyhow::bail;
use serde_json::{Value, json};
use tokio::process::ChildStdin;
use tokio::sync::{mpsc, oneshot};

use super::{
    Cli, Harness, Incoming, Info, Native, NativeSession, Open, Process, Rpc, RpcError, Updates, context_overflow,
    deny_approval, failed, mcp_servers_json, prompt_text, quota_exhausted, rate_limited, signed_out,
};
use crate::sync::MutexExt as _;

pub(crate) struct Stub {
    shared: Arc<Shared>,
    info: Info,
    router: tokio::task::JoinHandle<()>,
}

struct Shared {
    cli: Cli,
    rpc: Rpc<ChildStdin>,
    process: tokio::sync::Mutex<Process>,
    /// Where the CLI's updates go: the prompt running.
    current: Mutex<Option<Updates>>,
    /// Asks of the router to tell once it routed what the CLI sent before ([`Shared::routed`]).
    flushes: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl Drop for Stub {
    fn drop(&mut self) {
        // The router holds the connection, and with it the CLI's stdin.
        self.router.abort();
    }
}

impl Shared {
    async fn request(&self, method: &str, params: Value) -> Result<Value, Error> {
        match self.rpc.request(method, params).await {
            Ok(result) => Ok(result),
            Err(e) if e.is_closed() => Err(self.process.lock().await.exited().await),
            Err(e) => Err(mapped(&e)),
        }
    }

    /// Wait for the router to have routed what the CLI sent before the last response read: the updates of a prompt
    /// come before its response, and must reach the prompt before it ends.
    async fn routed(&self) {
        let (flushed, done) = oneshot::channel();
        if self.flushes.send(flushed).is_ok() {
            let _ = done.await;
        }
    }
}

/// The error of a request the CLI failed: -32000 for signed out, else by `data.kind` (`rate_limit` with `status` and
/// `retry_after` seconds, `quota`, `overflow`).
fn mapped(e: &RpcError) -> Error {
    let data = e.data.clone().unwrap_or_default();
    let retry_after = data["retry_after"].as_u64().map(Duration::from_secs);
    match (e.code, data["kind"].as_str()) {
        (-32000, _) => signed_out(&e.message),
        (_, Some("rate_limit")) => {
            let status = data["status"].as_u64().and_then(|status| u16::try_from(status).ok());
            rate_limited(status, retry_after, &e.message)
        }
        (_, Some("quota")) => quota_exhausted(retry_after, &e.message),
        (_, Some("overflow")) => context_overflow(&e.message),
        _ => failed(&e.message),
    }
}

/// Send the CLI's updates to the prompt running, and deny what it asks.
async fn route(
    mut incoming: mpsc::UnboundedReceiver<Incoming>,
    mut flushes: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    shared: Arc<Shared>,
) {
    loop {
        // What the CLI sent first, then the flushes: a flush answers once all the CLI sent before is routed.
        let message = tokio::select! {
            biased;
            message = incoming.recv() => match message {
                Some(message) => message,
                None => break,
            },
            Some(flushed) = flushes.recv() => {
                let _ = flushed.send(());
                continue;
            }
        };
        match message {
            Incoming::Notification { method, params } if method == "update" => {
                let Some(updates) = shared.current.locked().clone() else { continue };
                let text = params["text"].as_str().unwrap_or_default();
                match params["kind"].as_str() {
                    Some("message") => updates.message(text),
                    Some("thought") => updates.thought(text),
                    Some("tool") => {
                        let id = params["id"].as_str().unwrap_or_default();
                        updates.tool_call(ToolCall::new(id.to_owned(), params["title"].as_str().unwrap_or_default()));
                    }
                    Some("cost") => updates.usage(0, 0, params["usd"].as_f64()),
                    _ => {}
                }
            }
            Incoming::Request { id, method, params } if method == "approve" => {
                deny_approval(&shared.cli, &params.to_string());
                let _ = shared.rpc.respond(id, json!({ "approved": false })).await;
            }
            other => tracing::warn!(?other, "the stub CLI sent something unknown"),
        }
    }
}

impl Native for Stub {
    type Session = StubSession;

    fn validate(harness: &Harness) -> anyhow::Result<()> {
        if let Some(mode) = harness.mode.as_deref().filter(|mode| !["ask", "auto"].contains(mode)) {
            bail!("protocol stub takes mode ask or auto, not {mode}");
        }
        Ok(())
    }

    async fn start(cli: Cli) -> Result<Self, Error> {
        let mut process = cli.spawn(["--stub"], None)?;
        let (stdin, stdout) = process.take_io().ok_or_else(|| failed("no stdio"))?;
        let (rpc, incoming) = Rpc::new(stdout, stdin, true);
        let mode = cli.mode().map(str::to_owned);
        let (flush_tx, flushes) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            cli,
            rpc,
            process: tokio::sync::Mutex::new(process),
            current: Mutex::default(),
            flushes: flush_tx,
        });
        let router = tokio::spawn(route(incoming, flushes, shared.clone()));
        let stub = |hello: &Value| Info {
            name: hello["name"].as_str().unwrap_or("stub").to_owned(),
            version: hello["version"].as_str().unwrap_or("0").to_owned(),
            load: true,
            fork: hello["fork"].as_bool().unwrap_or_default(),
            images: false,
            http_mcp: false,
            steer: hello["steer"].as_bool().unwrap_or_default(),
        };
        match shared.request("initialize", json!({ "mode": mode })).await {
            Ok(hello) => Ok(Self { info: stub(&hello), shared, router }),
            Err(e) => {
                router.abort();
                Err(e)
            }
        }
    }

    fn info(&self) -> Info {
        self.info.clone()
    }

    const MCP: bool = true;

    async fn new_session(&self, open: Open) -> Result<StubSession, Error> {
        let cli = &self.shared.cli;
        let params = json!({
            "cwd": open.cwd,
            "mcp": mcp_servers_json(&open.mcp_servers),
            "model": cli.model(),
            "effort": cli.effort(),
        });
        let opened = self.shared.request("new", params).await?;
        Ok(self.session(opened["id"].as_str().unwrap_or_default()))
    }

    async fn load_session(&self, id: &str, open: Open) -> Result<StubSession, Error> {
        let params = json!({ "id": id, "cwd": open.cwd, "mcp": mcp_servers_json(&open.mcp_servers) });
        self.shared.request("load", params).await?;
        Ok(self.session(id))
    }

    async fn fork_session(&self, id: &str, open: Open) -> Result<StubSession, Error> {
        let forked = self.shared.request("fork", json!({ "id": id, "cwd": open.cwd })).await?;
        Ok(self.session(forked["id"].as_str().unwrap_or_default()))
    }
}

impl Stub {
    fn session(&self, id: &str) -> StubSession {
        StubSession { id: id.to_owned(), shared: self.shared.clone() }
    }
}

pub(crate) struct StubSession {
    id: String,
    shared: Arc<Shared>,
}

impl NativeSession for StubSession {
    fn id(&self) -> String {
        self.id.clone()
    }

    async fn prompt(&self, prompt: Vec<ContentBlock>, updates: Updates) -> Result<PromptResponse, Error> {
        *self.shared.current.locked() = Some(updates);
        let params = json!({ "session": self.id, "text": prompt_text(&prompt) });
        let done = self.shared.request("prompt", params).await;
        self.shared.routed().await;
        self.shared.current.locked().take();
        let done = done?;
        let stop = if done["stop"] == "cancelled" { StopReason::Cancelled } else { StopReason::EndTurn };
        let tokens = done["tokens"].as_u64().unwrap_or_default();
        Ok(PromptResponse::new(stop).usage(Usage::new(tokens, tokens, 0)))
    }

    async fn mcp_servers(&self) -> Result<Vec<McpServerState>, Error> {
        let listed = self.shared.request("mcp", json!({ "session": self.id })).await?;
        serde_json::from_value(listed["servers"].clone()).map_err(|e| failed(format!("the stub CLI's servers: {e}")))
    }

    async fn mcp_sign_in(&self, server: &str) -> Result<Option<String>, Error> {
        let started = self.shared.request("mcp_sign_in", json!({ "session": self.id, "server": server })).await?;
        Ok(started["url"].as_str().map(str::to_owned))
    }

    async fn steer(&self, prompt: Vec<ContentBlock>) -> Result<bool, Error> {
        let params = json!({ "session": self.id, "text": prompt_text(&prompt) });
        let steered = self.shared.request("steer", params).await?;
        Ok(steered["accepted"].as_bool().unwrap_or_default())
    }

    async fn cancel(&self) {
        if let Err(e) = self.shared.rpc.notify("cancel", json!({ "session": self.id })).await {
            tracing::warn!(error = %e, "cancelling the stub CLI's prompt");
        }
    }

    async fn close(&self) {
        if let Err(e) = self.shared.rpc.notify("close", json!({ "session": self.id })).await {
            tracing::warn!(error = %e, "closing the stub CLI's session");
        }
    }
}
