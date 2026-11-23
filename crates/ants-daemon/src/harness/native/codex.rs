//! Codex's app server (`protocol = "codex"`): `codex app-server`, JSON-RPC over stdio without the `"jsonrpc"` member,
//! one server per ACP connection hosting its threads.
//!
//! The handshake is `initialize`, `initialized` and `account/read`, which says whether an account is signed in.
//! Sessions are threads (`thread/start`, `thread/resume`, `thread/fork`), each opened with the definition's mode as an
//! approval policy and sandbox, the ant's model and reasoning effort, and the session's MCP servers as per-thread
//! config (`mcp_servers.<name>`), none of which a thread keeps when resumed or forked. A prompt is a turn
//! (`turn/start`, `turn/steer` to add a message to it, `turn/interrupt` to cancel) whose item and token-usage
//! notifications stream as ACP updates until `turn/completed`. Approval requests of the server are declined at once,
//! unless the ant asks ants about its tools: then its threads run with approval policy `on-request`, and the server's
//! requests to run a command or change files go to ants ([`super::approve`]). A closed session unsubscribes from its thread (`thread/unsubscribe`), which the server then unloads.

mod items;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{
    ContentBlock, PromptResponse, StopReason, ToolCallUpdate, ToolCallUpdateFields, ToolKind,
};
use ants_proto::{MCP_SERVERS_CAP, McpServerState, McpStatus};
use anyhow::bail;
use serde_json::{Map, Value, json};
use tokio::process::ChildStdin;
use tokio::sync::mpsc;

use self::items::{Tokens, Turn, input};
use super::io::older;
use super::{
    Cli, Credentials, Harness, Incoming, Info, Native, NativeSession, Open, Process, Rpc, Setting, Updates, approve,
    context_overflow, deny_approval, failed, mcp_servers_json, quota_exhausted, rate_limited, signed_out,
};
use crate::sync::MutexExt as _;

/// The first version speaking the app server as the translator does, the one it was verified with.
const MIN_VERSION: [u64; 3] = [0, 169, 0];

/// The definition's `mode` values, each with the approval policy and sandbox mode of the threads it opens; the first
/// is the default. None asks: the sandbox is the restriction, unless the ant asks ants about its tools
/// ([`ASKING_POLICY`]).
const MODES: [(&str, &str, &str); 3] = [
    ("workspace-write", "never", "workspace-write"),
    ("read-only", "never", "read-only"),
    ("full-access", "never", "danger-full-access"),
];

/// The approval policy of the threads of an ant asking ants about its tools: the model asks to run what the sandbox
/// would stop.
const ASKING_POLICY: &str = "on-request";

/// Notifications the translator reads nothing of, which the server is asked not to send.
const UNREAD: [&str; 7] = [
    "remoteControl/status/changed",
    "skills/changed",
    "thread/started",
    "item/commandExecution/outputDelta",
    "item/fileChange/patchUpdated",
    "item/mcpToolCall/progress",
    "turn/diff/updated",
];

pub(crate) struct Codex {
    shared: Arc<Shared>,
    info: Info,
    /// The server's `CODEX_HOME`, where it keeps its credentials (`auth.json`) unless in the OS keyring.
    home: Option<PathBuf>,
    router: tokio::task::JoinHandle<()>,
}

impl Drop for Codex {
    fn drop(&mut self) {
        // The router holds the connection, and with it the server's stdin.
        self.router.abort();
    }
}

/// A notification of a thread: its method and params.
type Notification = (String, Value);

struct Shared {
    cli: Cli,
    rpc: Rpc<ChildStdin>,
    process: tokio::sync::Mutex<Process>,
    /// Where the notifications of each thread running a prompt go, by thread id.
    threads: Mutex<HashMap<String, mpsc::UnboundedSender<Notification>>>,
    /// Where the approval requests of each thread running a prompt go, when the ant asks ants about its tools.
    asking: Mutex<HashMap<String, Updates>>,
    /// The latest rate-limit snapshot of each limit, merged from the sparse updates.
    limits: Mutex<HashMap<String, Map<String, Value>>>,
}

impl Shared {
    async fn request(&self, method: &str, params: Value) -> Result<Value, Error> {
        match self.rpc.request(method, params).await {
            Ok(result) => Ok(result),
            Err(e) if e.is_closed() => Err(self.exited().await),
            Err(e) if e.message.starts_with("no rollout found") => Err(failed(format!(
                "codex {method}: {}; codex keeps a thread once a turn has started in it",
                e.message
            ))),
            Err(e) => Err(failed(format!("codex {method}: {}", e.message))),
        }
    }

    /// The error of the server's output ending.
    async fn exited(&self) -> Error {
        self.process.lock().await.exited().await
    }

    fn notified(&self, method: &str, params: Value) {
        match method {
            "account/rateLimits/updated" => self.merge_limits(&params["rateLimits"]),
            "warning" | "configWarning" => {
                let text = params.get("message").or_else(|| params.get("summary")).unwrap_or(&Value::Null);
                tracing::warn!(harness = self.cli.name(), %text, "codex: {method}");
            }
            "mcpServer/startupStatus/updated" if params["status"] == "failed" => {
                let (server, error) = (&params["name"], &params["error"]);
                tracing::warn!(harness = self.cli.name(), %server, %error, "codex: an MCP server failed to start");
            }
            _ => {}
        }
        let Some(thread) = params["threadId"].as_str() else { return };
        if let Some(tx) = self.threads.locked().get(thread) {
            super::io::forward(tx, (method.to_owned(), params));
        }
    }

    /// Answer a request of the server: an approval to run a command or change files as ants decide
    /// ([`approval`]), another or a question for a human declined, anything else refused.
    async fn asked(&self, id: Value, method: &str, params: &Value) {
        if let Some(call) = approval(method, params) {
            let updates = params["threadId"].as_str().and_then(|thread| self.asking.locked().get(thread).cloned());
            let allowed = approve(&self.cli, updates, call, &format!("{method}: {}", asked_what(params))).await;
            let decision = match (method.starts_with("item/"), allowed) {
                (true, true) => json!({ "decision": "accept" }),
                (true, false) => json!({ "decision": "decline" }),
                (false, true) => json!({ "decision": "approved" }),
                (false, false) => {
                    json!({ "decision": { "denied": { "rejection": "the ant's tool permissions did not allow it" } } })
                }
            };
            if let Err(e) = self.rpc.respond(id, decision).await {
                tracing::debug!(error = %e, "answering codex");
            }
            return;
        }
        let decline = match method {
            "item/permissions/requestApproval" => json!({ "permissions": {}, "scope": "turn" }),
            "mcpServer/elicitation/request" => json!({ "action": "decline", "content": null, "_meta": null }),
            "item/tool/requestUserInput" => json!({ "answers": {} }),
            "item/tool/call" => json!({ "contentItems": [], "success": false }),
            _ => {
                tracing::warn!(harness = self.cli.name(), method, "codex asked something unknown");
                if let Err(e) = self.rpc.respond_error(id, -32601, &format!("{method} is not supported")).await {
                    tracing::debug!(error = %e, "answering codex");
                }
                return;
            }
        };
        deny_approval(&self.cli, &format!("{method}: {}", asked_what(params)));
        if let Err(e) = self.rpc.respond(id, decline).await {
            tracing::debug!(error = %e, "answering codex");
        }
    }

    fn merge_limits(&self, snapshot: &Value) {
        let Some(snapshot) = snapshot.as_object() else { return };
        let id = snapshot.get("limitId").and_then(Value::as_str).unwrap_or_default().to_owned();
        let mut limits = self.limits.locked();
        let merged = limits.entry(id).or_default();
        for (key, value) in snapshot.iter().filter(|(_, value)| !value.is_null()) {
            merged.insert(key.clone(), value.clone());
        }
    }

    /// How long until the limits reached reset, from the latest rate-limit snapshots: the last reset of a window used
    /// up.
    fn retry_after(&self) -> Option<Duration> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let limits = self.limits.locked();
        let resets = limits
            .values()
            .flat_map(|snapshot| ["primary", "secondary"].map(|window| snapshot.get(window)))
            .flatten()
            .filter(|window| window["usedPercent"].as_f64().is_some_and(|used| used >= 100.0))
            .filter_map(|window| window["resetsAt"].as_u64());
        resets.max().filter(|&at| at > now).map(|at| Duration::from_secs(at - now))
    }

    /// The error of a failed turn, classified by its `codexErrorInfo`.
    fn turn_error(&self, error: &Value) -> Error {
        let message = error["message"].as_str().unwrap_or("the turn failed");
        let detail = match error["additionalDetails"].as_str().filter(|details| !details.is_empty()) {
            Some(details) => format!("{message} ({details})"),
            None => message.to_owned(),
        };
        let (kind, status) = match &error["codexErrorInfo"] {
            Value::String(kind) => (kind.as_str(), None),
            Value::Object(info) => match info.iter().next() {
                Some((kind, fields)) => {
                    (kind.as_str(), fields["httpStatusCode"].as_u64().and_then(|status| u16::try_from(status).ok()))
                }
                None => ("other", None),
            },
            _ => ("other", None),
        };
        match (kind, status) {
            ("contextWindowExceeded", _) => context_overflow(detail),
            ("usageLimitExceeded", _) => quota_exhausted(self.retry_after(), detail),
            ("rateLimitExceeded", _) | (_, Some(429)) => rate_limited(Some(429), self.retry_after(), detail),
            ("serverOverloaded" | "flexUnavailable" | "policyCapacityUnavailable", _) => {
                rate_limited(status, None, detail)
            }
            ("unauthorized", _) | (_, Some(401)) => signed_out(detail),
            _ => failed(detail),
        }
    }
}

/// What the server's request `params` asks about, for the log.
fn asked_what(params: &Value) -> String {
    let what = ["command", "reason", "message", "tool"].iter().find_map(|key| match &params[key] {
        Value::String(text) => Some(text.clone()),
        Value::Array(words) => Some(words.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" ")),
        _ => None,
    });
    what.unwrap_or_default()
}

/// The tool call an approval request `method` of the server asks to run, when it asks to run a command or change
/// files: the item it reported, by id.
fn approval(method: &str, params: &Value) -> Option<ToolCallUpdate> {
    let (kind, title) = match method {
        "item/commandExecution/requestApproval" | "execCommandApproval" => (ToolKind::Execute, asked_what(params)),
        "item/fileChange/requestApproval" | "applyPatchApproval" => {
            let reason = params["reason"].as_str().filter(|reason| !reason.is_empty());
            (
                ToolKind::Edit,
                reason.map_or_else(|| "Change files".to_owned(), |reason| format!("Change files: {reason}")),
            )
        }
        _ => return None,
    };
    let id = params["itemId"].as_str().or_else(|| params["callId"].as_str()).unwrap_or("approval");
    let fields = ToolCallUpdateFields::new().kind(kind).title(title).raw_input(params.clone());
    Some(ToolCallUpdate::new(id.to_owned(), fields))
}

/// Route what the server sends: notifications to the prompts running, requests answered, those asking ants off the
/// loop. Once its output ends, the prompts running end too.
async fn route(mut incoming: mpsc::UnboundedReceiver<Incoming>, shared: Arc<Shared>) {
    while let Some(message) = incoming.recv().await {
        match message {
            Incoming::Notification { method, params } => shared.notified(&method, params),
            Incoming::Request { id, method, params } if shared.cli.asks() && approval(&method, &params).is_some() => {
                let shared = shared.clone();
                tokio::spawn(async move { shared.asked(id, &method, &params).await });
            }
            Incoming::Request { id, method, params } => shared.asked(id, &method, &params).await,
        }
    }
    shared.threads.locked().clear();
}

/// The version in the user agent `initialize` answers, `<client>/<version> (<os>) ...`.
fn version(user_agent: &str) -> Option<String> {
    let version = user_agent.split_whitespace().next()?.split_once('/')?.1;
    version.starts_with(|c: char| c.is_ascii_digit()).then(|| version.to_owned())
}

/// The approval policy and sandbox mode of `mode`, None when it is no mode of the protocol.
fn permissions(mode: Option<&str>) -> Option<(&'static str, &'static str)> {
    let mode = mode.unwrap_or(MODES[0].0);
    MODES.iter().find(|(name, ..)| *name == mode).map(|&(_, policy, sandbox)| (policy, sandbox))
}

impl Native for Codex {
    type Session = CodexSession;

    fn validate(harness: &Harness) -> anyhow::Result<()> {
        if let Some(mode) = harness.mode.as_deref().filter(|&mode| permissions(Some(mode)).is_none()) {
            let modes = MODES.map(|(name, ..)| name);
            bail!("protocol codex takes mode {}, not {mode}", crate::harness::one_of(&modes));
        }
        Ok(())
    }

    async fn start(cli: Cli) -> Result<Self, Error> {
        let (process, stdin, stdout) = cli.spawn(["app-server"], None)?;
        let (rpc, incoming) = Rpc::new(stdout, stdin, false);
        let shared = Arc::new(Shared {
            cli,
            rpc,
            process: tokio::sync::Mutex::new(process),
            threads: Mutex::default(),
            asking: Mutex::default(),
            limits: Mutex::default(),
        });
        let router = tokio::spawn(route(incoming, shared.clone()));
        let info = Info {
            name: "codex".to_owned(),
            version: String::new(),
            load: true,
            fork: true,
            images: true,
            http_mcp: false,
            steer: true,
        };
        let mut codex = Self { shared, info, home: None, router };
        (codex.info.version, codex.home) = codex.handshake().await?;
        Ok(codex)
    }

    fn info(&self) -> Info {
        self.info.clone()
    }

    const MCP: bool = true;

    const ASKS: bool = true;

    fn credentials(&self) -> Option<Credentials> {
        let path = self.home.as_ref()?.join("auth.json");
        let stored = std::fs::read(&path).ok()?;
        Some(Credentials::new(path.display().to_string(), &[&stored]))
    }

    async fn new_session(&self, open: Open) -> Result<CodexSession, Error> {
        self.offer_skills(&open).await?;
        let started = self.shared.request("thread/start", self.thread(&open, None)).await?;
        self.session(&started)
    }

    async fn load_session(&self, id: &str, open: Open) -> Result<CodexSession, Error> {
        self.offer_skills(&open).await?;
        let resumed = self.shared.request("thread/resume", self.thread(&open, Some(id))).await?;
        self.session(&resumed)
    }

    async fn fork_session(&self, id: &str, open: Open) -> Result<CodexSession, Error> {
        self.offer_skills(&open).await?;
        let forked = self.shared.request("thread/fork", self.thread(&open, Some(id))).await?;
        self.session(&forked)
    }
}

impl Codex {
    /// Initialize the server and check an account is signed in: the server's version and `CODEX_HOME`.
    async fn handshake(&self) -> Result<(String, Option<PathBuf>), Error> {
        let client = json!({ "name": "ants", "title": "ants", "version": env!("CARGO_PKG_VERSION") });
        let capabilities = json!({ "experimentalApi": false, "optOutNotificationMethods": UNREAD });
        let hello =
            self.shared.request("initialize", json!({ "clientInfo": client, "capabilities": capabilities })).await?;
        let user_agent = hello["userAgent"].as_str().unwrap_or_default();
        let Some(version) = version(user_agent) else {
            return Err(failed(format!("codex app-server named no version in its user agent {user_agent:?}")));
        };
        if older(&version, MIN_VERSION) {
            let [major, minor, patch] = MIN_VERSION;
            return Err(failed(format!(
                "codex {version} is older than {major}.{minor}.{patch}, the first version protocol codex runs"
            )));
        }
        if self.shared.rpc.notify("initialized", json!({})).await.is_err() {
            return Err(self.shared.exited().await);
        }
        let account = self.shared.request("account/read", json!({ "refreshToken": false })).await?;
        if account["account"].is_null() && account["requiresOpenaiAuth"] == true {
            return Err(signed_out("codex has no account signed in"));
        }
        Ok((version, hello["codexHome"].as_str().map(PathBuf::from)))
    }

    /// Make the skills `open` offers the server's extra skill roots, for the threads it opens from then on.
    async fn offer_skills(&self, open: &Open) -> Result<(), Error> {
        if let Some(skills) = &open.skills {
            self.shared.request("skills/extraRoots/set", json!({ "extraRoots": [skills.dir] })).await?;
        }
        Ok(())
    }

    /// The params opening a thread (`thread/start`), or resuming or forking thread `id`, with the definition's
    /// permissions, the ant's model and effort, and the session's MCP servers, whose tools run unasked.
    fn thread(&self, open: &Open, id: Option<&str>) -> Value {
        let cli = &self.shared.cli;
        let (policy, sandbox) = permissions(cli.mode()).unwrap_or(("never", "workspace-write"));
        let policy = if cli.asks() { ASKING_POLICY } else { policy };
        let mut config = Map::new();
        if let Some(effort) = cli.effort() {
            config.insert("model_reasoning_effort".to_owned(), effort.into());
        }
        if let Value::Object(servers) = mcp_servers_json(&open.mcp_servers) {
            for (name, server) in servers {
                let mut entry = match server["type"].as_str() {
                    Some("http") => json!({ "url": server["url"], "http_headers": server["headers"] }),
                    _ => json!({ "command": server["command"], "args": server["args"], "env": server["env"] }),
                };
                entry["default_tools_approval_mode"] = "approve".into();
                config.insert(format!("mcp_servers.{name}"), entry);
            }
        }
        let mut params = json!({ "cwd": open.cwd, "approvalPolicy": policy, "sandbox": sandbox, "config": config });
        if let Some(model) = cli.model() {
            params["model"] = model.into();
        }
        if let Some(id) = id {
            params["threadId"] = id.into();
            params["excludeTurns"] = true.into();
        }
        params
    }

    fn session(&self, opened: &Value) -> Result<CodexSession, Error> {
        let Some(id) = opened["thread"]["id"].as_str() else {
            return Err(failed(format!("codex opened a thread without an id: {opened}")));
        };
        Ok(CodexSession {
            id: id.to_owned(),
            shared: self.shared.clone(),
            turn: Mutex::default(),
            overrides: Mutex::default(),
            tokens: Mutex::default(),
        })
    }
}

/// The turn a session runs, if any.
#[derive(Default)]
struct Running {
    /// Its id, once `turn/start` answered.
    id: Option<String>,
    /// Whether it was cancelled.
    cancelled: bool,
}

pub(crate) struct CodexSession {
    id: String,
    shared: Arc<Shared>,
    turn: Mutex<Running>,
    /// The model and reasoning effort selected since the thread opened, by `turn/start` param.
    overrides: Mutex<Map<String, Value>>,
    /// The thread's token totals at the end of the last turn.
    tokens: Mutex<Option<Tokens>>,
}

/// An MCP server as codex's `mcpServerStatus/list` reports it.
fn mcp_server(server: &Value) -> McpServerState {
    let runtime = server["runtimeStatus"].as_str();
    let status = match runtime {
        Some("connected") => McpStatus::Connected,
        Some("notStarted" | "starting") => McpStatus::Pending,
        Some("authenticationRequired") => McpStatus::NeedsAuth,
        Some("failed" | "cancelled") => McpStatus::Failed,
        Some("disabled") => McpStatus::Disabled,
        _ => McpStatus::Unknown,
    };
    let error = server["toolsError"].as_str().or(runtime.filter(|&r| r == "cancelled"));
    let tools = server["tools"].as_object().map(Map::len);
    super::mcp_server_state(server, status, error, None, tools)
}

/// Stop routing a thread's notifications to its prompt.
struct Unroute<'a>(&'a Shared, &'a str);

impl Drop for Unroute<'_> {
    fn drop(&mut self) {
        self.0.threads.locked().remove(self.1);
        self.0.asking.locked().remove(self.1);
    }
}

impl CodexSession {
    async fn interrupt(&self, turn: &str) {
        let params = json!({ "threadId": self.id, "turnId": turn });
        if let Err(e) = self.shared.rpc.request("turn/interrupt", params).await {
            tracing::debug!(error = %e, "interrupting a codex turn");
        }
    }
}

impl NativeSession for CodexSession {
    fn id(&self) -> String {
        self.id.clone()
    }

    async fn mcp_servers(&self) -> Result<Vec<McpServerState>, Error> {
        let (mut servers, mut cursor) = (Vec::new(), Value::Null);
        loop {
            let params = json!({ "threadId": self.id, "detail": "toolsAndAuthOnly", "cursor": cursor });
            let page = self.shared.request("mcpServerStatus/list", params).await?;
            servers.extend(page["data"].as_array().into_iter().flatten().map(mcp_server));
            cursor = page["nextCursor"].clone();
            if cursor.is_null() || servers.len() >= MCP_SERVERS_CAP {
                return Ok(servers);
            }
        }
    }

    async fn mcp_sign_in(&self, server: &str) -> Result<Option<String>, Error> {
        let params = json!({ "name": server, "threadId": self.id });
        let login = self.shared.request("mcpServer/oauth/login", params).await?;
        Ok(login["authorizationUrl"].as_str().filter(|url| !url.is_empty()).map(str::to_owned))
    }

    async fn prompt(&self, prompt: Vec<ContentBlock>, updates: Updates) -> Result<PromptResponse, Error> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.shared.threads.locked().insert(self.id.clone(), tx);
        self.shared.asking.locked().insert(self.id.clone(), updates.clone());
        let _unroute = Unroute(&self.shared, &self.id);
        *self.turn.locked() = Running::default();
        let mut params = json!({ "threadId": self.id, "input": input(&prompt) });
        for (key, value) in self.overrides.locked().iter() {
            params[key] = value.clone();
        }
        let started = self.shared.request("turn/start", params).await?;
        let Some(id) = started["turn"]["id"].as_str() else {
            return Err(failed(format!("codex started a turn without an id: {started}")));
        };
        let cancelled = {
            let mut running = self.turn.locked();
            running.id = Some(id.to_owned());
            running.cancelled
        };
        if cancelled {
            self.interrupt(id).await;
        }
        let known = *self.tokens.locked();
        let mut turn = Turn::new(id, updates, known);
        let done = loop {
            let Some((method, params)) = rx.recv().await else {
                return Err(self.shared.exited().await);
            };
            if let Some(done) = turn.handle(&method, params) {
                break done;
            }
        };
        self.turn.locked().id = None;
        if let Some(latest) = turn.latest {
            *self.tokens.locked() = Some(latest);
        }
        let stop = match done["status"].as_str() {
            Some("interrupted") => StopReason::Cancelled,
            Some("failed") => return Err(self.shared.turn_error(&done["error"])),
            _ => StopReason::EndTurn,
        };
        Ok(PromptResponse::new(stop).usage(turn.usage()))
    }

    async fn steer(&self, prompt: Vec<ContentBlock>) -> Result<bool, Error> {
        let turn = {
            let running = self.turn.locked();
            running.id.clone().filter(|_| !running.cancelled)
        };
        let Some(turn) = turn else { return Ok(false) };
        let params = json!({ "threadId": self.id, "expectedTurnId": turn, "input": input(&prompt) });
        match self.shared.rpc.request("turn/steer", params).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_closed() => Err(self.shared.exited().await),
            Err(e) => {
                tracing::debug!(error = %e, "codex refused a steer, as of a turn ending");
                Ok(false)
            }
        }
    }

    async fn cancel(&self) {
        let turn = {
            let mut running = self.turn.locked();
            running.cancelled = true;
            running.id.clone()
        };
        if let Some(turn) = turn {
            self.interrupt(&turn).await;
        }
    }

    async fn close(&self) {
        if let Err(e) = self.shared.request("thread/unsubscribe", json!({ "threadId": self.id })).await {
            tracing::warn!(harness = self.shared.cli.name(), error = %e, "unsubscribing from a codex thread");
        }
    }

    async fn configure(&self, setting: Setting, value: &str) -> Result<(), Error> {
        let key = match setting {
            Setting::Model => "model",
            Setting::ReasoningEffort => "effort",
        };
        self.overrides.locked().insert(key.to_owned(), value.into());
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;
