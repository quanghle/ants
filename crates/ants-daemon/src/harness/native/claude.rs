//! Claude Code's stream-json (`protocol = "claude"`): one `claude -p --input-format stream-json --output-format
//! stream-json` process per session, kept for its life, taking a user message per prompt on stdin and answering with
//! the events of the turn, a `result` ending each.
//!
//! The translator chooses a new session's id (`--session-id`), resumes one with `--resume` and forks one with
//! `--resume <parent> --fork-session --session-id <new>`. A fork's totals start at its parent's, so its ACP id carries
//! that base ([`based_id`]). A session's process gets its MCP servers on stdin
//! (`mcp_set_servers`, with `--strict-mcp-config`) and their tools allowed (`--allowedTools=mcp__<server>`), the
//! definition's `mode` as `--permission-mode`, and `--permission-prompts none`, so whatever Claude Code would ask a
//! human about is denied rather than waited on. Cancel is Claude Code's `interrupt`; the process is killed when the
//! turn does not end within [`INTERRUPT_GRACE`].

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{
    ContentBlock, Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus, PromptResponse, StopReason, ToolCall,
    ToolCallLocation, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind, Usage,
};
use ants_proto::{McpServerState, McpStatus};
use anyhow::bail;
use serde_json::{Value, json};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{mpsc, oneshot, watch};

use super::{
    Cli, Credentials, Harness, Info, JsonLines, JsonWriter, Native, NativeSession, Open, Process, Updates, based_id,
    context_overflow, cost_base, deny_approval, failed, mcp_servers_json, quota_exhausted, rate_limited, signed_out,
};
use crate::sync::MutexExt as _;

/// The first Claude Code version the protocol was verified with.
const MIN_VERSION: [u64; 3] = [2, 1, 325];

/// The permission modes `mode` may name (`--permission-mode`); `manual` is Claude Code's other name for `default`.
const MODES: &[&str] = &["default", "manual", "acceptEdits", "plan", "auto", "dontAsk", "bypassPermissions"];

/// The reasoning efforts Claude Code takes (`--effort`); it ignores any other.
const EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max"];

/// Arguments the protocol sets itself, which a definition's `args` may not.
const OWN_ARGS: &[&str] = &[
    "--input-format",
    "--output-format",
    "--session-id",
    "-r",
    "--resume",
    "-c",
    "--continue",
    "--fork-session",
    "--no-session-persistence",
    "--permission-mode",
    "--permission-prompt-tool",
    "--permission-prompts",
];

/// Environment of the CLI unless the definition or the ant set it: no telemetry, error reports, auto-updates and other
/// traffic a session does not need, and the TodoWrite tool, whose todo lists stream as the agent's plan.
const ENV: &[(&str, &str)] = &[
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_ENABLE_TODO_TOOLS", "1"),
    ("CLAUDE_CODE_ENABLE_TASKS", "0"),
    ("CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS", "1"),
];

/// The environment variables Claude Code reads an API key or a token from.
const CREDENTIAL_VARIABLES: [&str; 3] = ["ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN"];

/// How long an interrupted turn has to end before the CLI is killed.
const INTERRUPT_GRACE: Duration = Duration::from_secs(10);

/// How long a closed session's CLI has to exit, writing its cost totals to the transcript, before it is killed.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// What a tool request Claude Code asks about is answered with.
const DENIED: &str = "Denied: this harness runs only the tools its permission settings allow.";

/// The starts of Claude Code's messages of an exhausted plan, credit or spend allowance.
const USAGE_LIMITS: &[&str] = &[
    "You've hit your",
    "You've reached your",
    "You're out of usage credits",
    "You're out of extra usage",
    "Your org is out of usage",
    "Your seat type doesn't include",
    "Your usage allocation has been disabled",
    "Your group's usage limit is set to $0",
    "Credit balance is too low",
    "requires usage credits",
];

/// The starts of Claude Code's messages of a conversation outgrowing the model's context window.
const OVERFLOWS: &[&str] = &["Prompt is too long", "Context limit reached", "Context exceeds", "Request too large"];

pub(crate) struct Claude {
    cli: Cli,
    version: String,
}

/// How a session's process opens it.
enum How {
    New(String),
    Load(String),
    Fork { parent: String, id: String },
}

impl How {
    fn id(&self) -> &str {
        match self {
            Self::New(id) | Self::Load(id) | Self::Fork { id, .. } => id,
        }
    }
}

impl Native for Claude {
    type Session = ClaudeSession;

    fn validate(harness: &Harness) -> anyhow::Result<()> {
        if let Some(mode) = harness.mode.as_deref().filter(|mode| !MODES.contains(mode)) {
            bail!("protocol claude takes mode {}, not {mode}", MODES.join(", "));
        }
        let context = harness.context.values().flat_map(|value| &value.args);
        for arg in harness.args.iter().chain(context) {
            let own =
                |flag: &&&str| arg == **flag || arg.strip_prefix(**flag).is_some_and(|rest| rest.starts_with('='));
            if let Some(flag) = OWN_ARGS.iter().find(own) {
                bail!("protocol claude passes {flag} itself; remove it from the arguments");
            }
        }
        Ok(())
    }

    async fn start(cli: Cli) -> Result<Self, Error> {
        let version = version(&cli).await?;
        Ok(Self { cli, version })
    }

    fn info(&self) -> Info {
        Info {
            name: "Claude Code".to_owned(),
            version: self.version.clone(),
            load: true,
            fork: true,
            images: true,
            http_mcp: true,
            steer: true,
        }
    }

    const MCP: bool = true;

    /// The API key and tokens in the environment and the login in `.credentials.json` of the configuration directory,
    /// which Claude Code keeps there unless in the macOS keychain.
    fn credentials(&self) -> Option<Credentials> {
        let (mut places, mut parts) = (Vec::new(), Vec::new());
        for name in CREDENTIAL_VARIABLES {
            if let Some(value) = self.cli.var(name) {
                places.push(name.to_owned());
                parts.push(value.into_bytes());
            }
        }
        let home = self.cli.var("HOME").map(PathBuf::from).or_else(std::env::home_dir);
        let dir =
            self.cli.var("CLAUDE_CONFIG_DIR").map(PathBuf::from).or_else(|| home.map(|home| home.join(".claude")));
        if let Some(file) = dir.map(|dir| dir.join(".credentials.json"))
            && let Ok(stored) = std::fs::read(&file)
        {
            places.push(file.display().to_string());
            parts.push(stored);
        }
        let parts: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        (!places.is_empty()).then(|| Credentials::new(places.join(" and "), &parts))
    }

    async fn new_session(&self, open: Open) -> Result<ClaudeSession, Error> {
        self.open(How::New(uuid::Uuid::new_v4().to_string()), open, 0.0).await
    }

    async fn load_session(&self, id: &str, open: Open) -> Result<ClaudeSession, Error> {
        let (id, base) = cost_base(id);
        self.open(How::Load(id.to_owned()), open, base).await
    }

    async fn fork_session(&self, id: &str, open: Open) -> Result<ClaudeSession, Error> {
        let how = How::Fork { parent: cost_base(id).0.to_owned(), id: uuid::Uuid::new_v4().to_string() };
        let session = self.open(how, open, 0.0).await?;
        session.keep().await?;
        Ok(session)
    }
}

/// The version of the CLI, from `--version`; fails for one older than [`MIN_VERSION`].
async fn version(cli: &Cli) -> Result<String, Error> {
    let mut command = tokio::process::Command::new(cli.program());
    command.arg("--version").envs(cli.env()).stdin(Stdio::null()).kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    let program = cli.program().display();
    let output = match tokio::time::timeout(Duration::from_secs(30), command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Err(failed(format!("{}: starting {program}: {e}", cli.name()))),
        Err(_) => return Err(failed(format!("{}: {program} --version did not answer within 30s", cli.name()))),
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let version = text.split_whitespace().next().unwrap_or_default();
    let parts: Vec<u64> = version.split('.').map_while(|part| part.parse().ok()).collect();
    if parts.len() != 3 {
        return Err(failed(format!(
            "{}: {program} --version printed {:?}, not a Claude Code version",
            cli.name(),
            text.trim()
        )));
    }
    if parts[..] < MIN_VERSION[..] {
        let [major, minor, patch] = MIN_VERSION;
        return Err(failed(format!(
            "{}: Claude Code {version} is older than {major}.{minor}.{patch}, the first version protocol claude runs",
            cli.name()
        )));
    }
    Ok(version.to_owned())
}

impl Claude {
    async fn open(&self, how: How, open: Open, base: f64) -> Result<ClaudeSession, Error> {
        if let Some(effort) = self.cli.effort().filter(|effort| !EFFORTS.contains(effort)) {
            return Err(failed(format!(
                "{}: Claude Code takes reasoning_effort {}, not {effort}",
                self.cli.name(),
                EFFORTS.join(", ")
            )));
        }
        let servers = mcp_servers_json(&open.mcp_servers);
        let mut cli = self.cli.clone();
        for (name, value) in ENV {
            cli.env.entry((*name).to_owned()).or_insert_with(|| (*value).to_owned());
        }
        let mut process = cli.spawn(self.args(&how, &servers), Some(&open.cwd))?;
        let (stdin, stdout) = process.take_io().ok_or_else(|| failed("no stdio"))?;
        let (events, received) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            cli: self.cli.clone(),
            writer: JsonWriter::new(stdin),
            process: tokio::sync::Mutex::new(Some(process)),
            pending: Mutex::new(Some(HashMap::new())),
            requests: AtomicU64::new(0),
        });
        let reader = tokio::spawn(read(JsonLines::new(stdout), shared.clone(), events));
        let session = ClaudeSession {
            id: how.id().to_owned(),
            cwd: open.cwd,
            shared,
            reader: Some(reader),
            events: tokio::sync::Mutex::new(received),
            running: watch::channel(None).0,
            turns: AtomicU64::new(0),
            interrupted: AtomicBool::new(false),
            spent: Mutex::new(Spent { base, ..Spent::default() }),
            steers: Mutex::default(),
        };
        let hello = session.shared.control(json!({ "subtype": "initialize", "hooks": null })).await?;
        if no_credentials(&hello["account"]) {
            return Err(signed_out(
                "Claude Code has neither a login nor an API key (Not logged in · Please run /login)",
            ));
        }
        if let Some(servers) = servers.as_object().filter(|servers| !servers.is_empty()) {
            let set = session.shared.control(json!({ "subtype": "mcp_set_servers", "servers": servers })).await?;
            if let Some(errors) = set["errors"].as_object().filter(|errors| !errors.is_empty()) {
                let errors: Vec<String> = errors.iter().map(|(name, error)| format!("{name}: {error}")).collect();
                return Err(failed(format!("{}: MCP servers refused: {}", self.cli.name(), errors.join("; "))));
            }
        }
        Ok(session)
    }

    /// The protocol's arguments for a session opened `how`, with MCP servers `servers`.
    fn args(&self, how: &How, servers: &Value) -> Vec<String> {
        let mut args: Vec<String> = [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--replay-user-messages",
            "--thinking-display",
            "summarized",
            "--permission-prompts",
            "none",
            "--strict-mcp-config",
        ]
        .map(str::to_owned)
        .into();
        match how {
            How::New(id) => args.extend(["--session-id".to_owned(), id.clone()]),
            How::Load(id) => args.extend(["--resume".to_owned(), id.clone()]),
            How::Fork { parent, id } => {
                args.extend(["--resume", parent, "--fork-session", "--session-id", id].map(str::to_owned));
            }
        }
        let settings =
            [("--model", self.cli.model()), ("--effort", self.cli.effort()), ("--permission-mode", self.cli.mode())];
        for (flag, value) in settings {
            if let Some(value) = value {
                args.extend([flag.to_owned(), value.to_owned()]);
            }
        }
        // Last, as the flag takes a list.
        for name in servers.as_object().into_iter().flat_map(serde_json::Map::keys) {
            let name: String =
                name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' }).collect();
            args.push(format!("--allowedTools=mcp__{name}"));
        }
        args
    }
}

/// Whether the `account` of Claude Code's `initialize` answer has no credentials: no login, no API key, and the
/// Anthropic API as provider (Bedrock, Vertex and Foundry bring their own).
fn no_credentials(account: &Value) -> bool {
    account["tokenSource"] == "none"
        && account["apiKeySource"].as_str().is_none_or(|source| source == "none")
        && account["apiProvider"].as_str().is_none_or(|provider| provider == "firstParty")
}

/// What a session's process and its reader share.
struct Shared {
    cli: Cli,
    writer: JsonWriter<ChildStdin>,
    /// Taken when the session closes.
    process: tokio::sync::Mutex<Option<Process>>,
    /// Control requests awaiting their response, by id; None once the CLI's output ended.
    pending: Mutex<Option<HashMap<String, oneshot::Sender<Value>>>>,
    requests: AtomicU64,
}

impl Shared {
    /// Send control request `request` and await its response.
    async fn control(&self, request: Value) -> Result<Value, Error> {
        let id = format!("ants-{}", self.requests.fetch_add(1, Ordering::Relaxed) + 1);
        let (tx, rx) = oneshot::channel();
        let open = self.pending.locked().as_mut().map(|pending| pending.insert(id.clone(), tx)).is_some();
        if !open {
            return Err(self.exited().await);
        }
        let subtype = request["subtype"].as_str().unwrap_or_default().to_owned();
        let message = json!({ "type": "control_request", "request_id": id, "request": request });
        if self.writer.send(&message).await.is_err() {
            return Err(self.exited().await);
        }
        let Ok(response) = rx.await else { return Err(self.exited().await) };
        if response["subtype"] == "success" {
            Ok(response["response"].clone())
        } else {
            Err(failed(format!("{}: {subtype}: {}", self.cli.name(), response["error"].as_str().unwrap_or("failed"))))
        }
    }

    /// Answer a control request of the CLI: deny a tool request, refuse anything else.
    async fn answer(&self, message: &Value) {
        let (id, request) = (&message["request_id"], &message["request"]);
        let response = if request["subtype"] == "can_use_tool" {
            let tool = request["tool_name"].as_str().unwrap_or("a tool");
            deny_approval(&self.cli, &format!("{tool} {}", request["input"]));
            json!({ "subtype": "success", "request_id": id, "response": { "behavior": "deny", "message": DENIED } })
        } else {
            let subtype = request["subtype"].as_str().unwrap_or_default();
            json!({ "subtype": "error", "request_id": id, "error": format!("Unsupported control request subtype: {subtype}") })
        };
        if let Err(e) = self.writer.send(&json!({ "type": "control_response", "response": response })).await {
            tracing::debug!(error = %e, "answering a control request of Claude Code");
        }
    }

    /// The error of the CLI's output ending.
    async fn exited(&self) -> Error {
        match self.process.lock().await.as_mut() {
            Some(process) => process.exited().await,
            None => failed(format!("{} stopped", self.cli.name())),
        }
    }
}

/// Read the CLI's output: responses to control requests to whoever awaits them, its own control requests answered,
/// everything else to `events`.
async fn read(mut lines: JsonLines<ChildStdout>, shared: Arc<Shared>, events: mpsc::UnboundedSender<Value>) {
    loop {
        let message = match lines.next().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(harness = shared.cli.name(), error = %e, "reading Claude Code");
                break;
            }
        };
        match message["type"].as_str() {
            Some("control_response") => {
                let response = &message["response"];
                let id = response["request_id"].as_str().unwrap_or_default();
                let waiting = shared.pending.locked().as_mut().and_then(|pending| pending.remove(id));
                if let Some(waiting) = waiting {
                    let _ = waiting.send(response.clone());
                }
            }
            Some("control_request") => shared.answer(&message).await,
            Some("control_cancel_request") => {}
            _ => super::io::forward(&events, message),
        }
    }
    shared.pending.locked().take();
}

pub(crate) struct ClaudeSession {
    id: String,
    cwd: PathBuf,
    shared: Arc<Shared>,
    reader: Option<tokio::task::JoinHandle<()>>,
    events: tokio::sync::Mutex<mpsc::UnboundedReceiver<Value>>,
    /// The turn running, by number.
    running: watch::Sender<Option<u64>>,
    turns: AtomicU64,
    interrupted: AtomicBool,
    spent: Mutex<Spent>,
    steers: Mutex<Steers>,
}

/// The messages steering the turn running ([`NativeSession::steer`]), written to stdin as it runs. Claude Code takes
/// one between model calls, or after the turn's `result` as a turn of its own, replaying it as it does
/// (`--replay-user-messages`). The prompt ends once the session is idle (`session_state_changed`), or at a `result`
/// from a Claude Code that does not report its state, and only once each steer was replayed.
#[derive(Debug, Default)]
struct Steers {
    /// Whether a turn runs that takes steers: until it ends.
    open: bool,
    /// The uuids of the messages written but not replayed.
    pending: HashSet<String>,
}

/// What the session reports of its context window and cost.
#[derive(Debug, Default)]
struct Spent {
    /// The cost Claude Code carried over from the session's parent, which its totals include.
    base: f64,
    /// The session's cost so far as reported.
    cost: f64,
    /// Tokens in the context window as of the last model call, and the window's size.
    used: u64,
    size: u64,
}

impl Drop for ClaudeSession {
    fn drop(&mut self) {
        let reader = self.reader.take();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            reader.inspect(tokio::task::JoinHandle::abort);
            return;
        };
        let shared = self.shared.clone();
        let running = self.running.borrow().is_some();
        // Claude Code writes its cost totals to the transcript as it exits, which resuming restores: let it.
        runtime.spawn(async move {
            // Writing fails only once the CLI is gone.
            if running {
                let interrupt = json!({ "type": "control_request", "request_id": "ants-close", "request": { "subtype": "interrupt" } });
                let _ = shared.writer.send(&interrupt).await;
            }
            let _ = shared.writer.close().await;
            let process = shared.process.lock().await.take();
            if let Some(mut process) = process {
                process.stop(STOP_GRACE).await;
            }
            reader.inspect(tokio::task::JoinHandle::abort);
        });
    }
}

impl ClaudeSession {
    /// Have Claude Code write the forked session's transcript, which it does at the first message, without a turn:
    /// an empty message that queries nothing. Its result tells what the fork's totals start at.
    async fn keep(&self) -> Result<(), Error> {
        let mut events = self.events.lock().await;
        let message = json!({ "type": "user", "message": { "role": "user", "content": [] }, "shouldQuery": false });
        if self.shared.writer.send(&message).await.is_err() {
            return Err(self.shared.exited().await);
        }
        let result = loop {
            match events.recv().await {
                Some(event) if event["type"] == "result" => break event,
                Some(_) => {}
                None => return Err(self.shared.exited().await),
            }
        };
        if result["is_error"] == true {
            return Err(failed(format!("{}: forking: {}", self.shared.cli.name(), error_text(&result, None))));
        }
        self.spent.locked().base = result["total_cost_usd"].as_f64().unwrap_or_default();
        Ok(())
    }

    /// The user message of `prompt`, as `uuid`.
    fn message(&self, prompt: &[ContentBlock], uuid: &str) -> Value {
        json!({
            "type": "user",
            "message": { "role": "user", "content": content(prompt) },
            "parent_tool_use_id": null,
            "session_id": self.id,
            "uuid": uuid,
            "client_composed": true,
        })
    }

    async fn turn(
        &self,
        events: &mut mpsc::UnboundedReceiver<Value>,
        prompt: &[ContentBlock],
        updates: &Updates,
    ) -> Result<PromptResponse, Error> {
        let message = self.message(prompt, &uuid::Uuid::new_v4().to_string());
        if self.shared.writer.send(&message).await.is_err() {
            return Err(self.shared.exited().await);
        }
        self.steers.locked().open = true;
        let mut turn = Turn::new(&self.shared.cli, &self.cwd, updates);
        let (mut counts, mut last, mut states) = ([0; 5], None, false);
        let result = loop {
            let Some(event) = events.recv().await else { return Err(self.shared.exited().await) };
            let idle = match (event["type"].as_str(), event["subtype"].as_str()) {
                (Some("user"), _) if event["isReplay"] == true => {
                    self.steers.locked().pending.remove(event["uuid"].as_str().unwrap_or_default());
                    continue;
                }
                (Some("system"), Some("session_state_changed")) => {
                    states = true;
                    event["state"] == "idle"
                }
                _ => {
                    let Some(result) = turn.event(event) else { continue };
                    counts = std::array::from_fn(|i| counts[i] + token_counts(&result)[i]);
                    last = Some(result);
                    turn.parted = true;
                    !states
                }
            };
            if !idle {
                continue;
            }
            let Some(result) = last.take() else { continue };
            let mut steers = self.steers.locked();
            if steers.pending.is_empty() || self.interrupted.load(Ordering::Relaxed) {
                steers.open = false;
                break result;
            }
            last = Some(result);
        };
        let (used, size, cost) = {
            let mut spent = self.spent.locked();
            let total = result["total_cost_usd"].as_f64().unwrap_or_default();
            spent.cost = spent.cost.max(total - spent.base);
            spent.size = window(&result, turn.model.as_deref()).unwrap_or(spent.size);
            if turn.used > 0 {
                spent.used = turn.used;
            }
            (spent.used, spent.size, spent.cost)
        };
        updates.usage(used, size, Some(cost));
        let stop = ended(&result, &turn, self.interrupted.load(Ordering::Relaxed))?;
        Ok(PromptResponse::new(stop).usage(tokens(counts)))
    }
}

impl NativeSession for ClaudeSession {
    fn id(&self) -> String {
        based_id(&self.id, self.spent.locked().base)
    }

    async fn mcp_servers(&self) -> Result<Vec<McpServerState>, Error> {
        let status = self.shared.control(json!({ "subtype": "mcp_status" })).await?;
        let servers = status["mcpServers"].as_array().map_or(&[][..], Vec::as_slice);
        Ok(servers.iter().map(mcp_server).collect())
    }

    async fn mcp_sign_in(&self, server: &str) -> Result<Option<String>, Error> {
        let auth = self.shared.control(json!({ "subtype": "mcp_authenticate", "serverName": server })).await?;
        Ok(auth["authUrl"].as_str().filter(|url| !url.is_empty()).map(str::to_owned))
    }

    async fn mcp_callback(&self, server: &str, callback: &str) -> Result<(), Error> {
        let request = json!({ "subtype": "mcp_oauth_callback_url", "serverName": server, "callbackUrl": callback });
        self.shared.control(request).await.map(drop)
    }

    async fn prompt(&self, prompt: Vec<ContentBlock>, updates: Updates) -> Result<PromptResponse, Error> {
        let mut events = self.events.lock().await;
        // Whatever came between turns, such as a background task's notice, belongs to no prompt.
        while events.try_recv().is_ok() {}
        let turn = self.turns.fetch_add(1, Ordering::Relaxed) + 1;
        self.interrupted.store(false, Ordering::Relaxed);
        *self.steers.locked() = Steers::default();
        self.running.send_replace(Some(turn));
        let done = self.turn(&mut events, &prompt, &updates).await;
        self.steers.locked().open = false;
        self.running.send_replace(None);
        done
    }

    async fn steer(&self, prompt: Vec<ContentBlock>) -> Result<bool, Error> {
        let uuid = uuid::Uuid::new_v4().to_string();
        {
            let mut steers = self.steers.locked();
            if !steers.open || self.interrupted.load(Ordering::Relaxed) {
                return Ok(false);
            }
            steers.pending.insert(uuid.clone());
        }
        if self.shared.writer.send(&self.message(&prompt, &uuid)).await.is_err() {
            self.steers.locked().pending.remove(&uuid);
            return Err(self.shared.exited().await);
        }
        Ok(true)
    }

    async fn cancel(&self) {
        let Some(turn) = *self.running.borrow() else { return };
        self.interrupted.store(true, Ordering::Relaxed);
        let mut running = self.running.subscribe();
        let interrupt = self.shared.control(json!({ "subtype": "interrupt" }));
        if let Ok(Err(e)) = tokio::time::timeout(INTERRUPT_GRACE, interrupt).await {
            tracing::warn!(harness = self.shared.cli.name(), error = %e, "interrupting Claude Code");
        }
        if tokio::time::timeout(INTERRUPT_GRACE, running.wait_for(|now| *now != Some(turn))).await.is_err() {
            tracing::warn!(
                harness = self.shared.cli.name(),
                "Claude Code did not end the interrupted turn; killing it"
            );
            if let Some(process) = self.shared.process.lock().await.as_mut() {
                process.stop(Duration::ZERO).await;
            }
        }
    }
}

/// An MCP server as Claude Code's `mcp_status` reports it.
fn mcp_server(server: &Value) -> McpServerState {
    let status = match server["status"].as_str() {
        Some("connected") => McpStatus::Connected,
        Some("pending") => McpStatus::Pending,
        Some("needs-auth") => McpStatus::NeedsAuth,
        Some("failed") => McpStatus::Failed,
        Some("disabled") => McpStatus::Disabled,
        _ => McpStatus::Unknown,
    };
    let name = server["name"].as_str().unwrap_or_default();
    McpServerState::new(name, status, server["error"].as_str(), server["scope"].as_str())
        .with_tools(server["tools"].as_array().map(Vec::len))
}

/// A prompt's content as Claude Code takes it: text, and images as base64 image blocks.
fn content(prompt: &[ContentBlock]) -> Vec<Value> {
    prompt
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(json!({ "type": "text", "text": text.text })),
            ContentBlock::Image(image) => Some(json!({
                "type": "image",
                "source": { "type": "base64", "media_type": image.mime_type, "data": image.data },
            })),
            ContentBlock::ResourceLink(link) => {
                Some(json!({ "type": "text", "text": format!("[@{}]({})", link.name, link.uri) }))
            }
            _ => None,
        })
        .collect()
}

/// One turn's events, as they stream to the daemon.
struct Turn<'a> {
    cli: &'a Cli,
    cwd: &'a Path,
    updates: &'a Updates,
    /// The model of the turn, from its `init` event.
    model: Option<String>,
    /// Messages streamed as deltas, by id, whose whole blocks are not sent again.
    streamed: HashSet<String>,
    /// TodoWrite calls, sent as the plan rather than as tool calls.
    todos: HashSet<String>,
    /// Tokens in the context window as of the turn's last model call.
    used: u64,
    /// The kind of error of the turn's last assistant message, such as `rate_limit`.
    error: Option<String>,
    /// Claude Code's own message of the turn, such as an error's.
    synthetic: Option<String>,
    /// The last rate limit state reported.
    limit: Option<Value>,
    /// Whether the reply said anything yet.
    said: bool,
    /// Whether a steer's turn followed a `result`, its reply then parted from what came before by a blank line.
    parted: bool,
}

impl<'a> Turn<'a> {
    fn new(cli: &'a Cli, cwd: &'a Path, updates: &'a Updates) -> Self {
        Self {
            cli,
            cwd,
            updates,
            model: None,
            streamed: HashSet::new(),
            todos: HashSet::new(),
            used: 0,
            error: None,
            synthetic: None,
            limit: None,
            said: false,
            parted: false,
        }
    }

    /// Take an event of the CLI; the turn's result when it ends it.
    fn event(&mut self, event: Value) -> Option<Value> {
        // Subagents' own messages stay inside their Task call.
        if !event["parent_tool_use_id"].is_null() {
            return None;
        }
        match event["type"].as_str() {
            Some("stream_event") => self.stream(&event["event"]),
            Some("assistant") => self.assistant(&event),
            Some("user") => self.tool_results(&event["message"]["content"]),
            Some("system") => self.system(&event),
            Some("rate_limit_event") => self.limit = Some(event["rate_limit_info"].clone()),
            Some("result") => return Some(event),
            _ => {}
        }
        None
    }

    fn stream(&mut self, event: &Value) {
        match event["type"].as_str() {
            Some("message_start") => {
                if let Some(id) = event["message"]["id"].as_str() {
                    self.streamed.insert(id.to_owned());
                }
            }
            Some("content_block_delta") => {
                let delta = &event["delta"];
                match (delta["type"].as_str(), delta["text"].as_str(), delta["thinking"].as_str()) {
                    (Some("text_delta"), Some(text), _) if !text.is_empty() => self.say(text),
                    (Some("thinking_delta"), _, Some(thinking)) if !thinking.is_empty() => {
                        self.updates.thought(thinking);
                    }
                    _ => {}
                }
            }
            Some("message_delta") => {
                let usage = &event["usage"];
                let tokens =
                    ["input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens"];
                self.used = tokens.iter().filter_map(|key| usage[key].as_u64()).sum();
            }
            _ => {}
        }
    }

    fn assistant(&mut self, event: &Value) {
        let message = &event["message"];
        if let Some(kind) = event["error"].as_str() {
            self.error = Some(kind.to_owned());
        }
        let synthetic = message["model"] == "<synthetic>";
        let streamed = message["id"].as_str().is_some_and(|id| self.streamed.contains(id));
        for block in message["content"].as_array().into_iter().flatten() {
            match (block["type"].as_str(), block["text"].as_str()) {
                (Some("text"), Some(text)) if synthetic => self.synthetic = Some(text.to_owned()),
                (Some("text"), Some(text)) if !streamed && !text.is_empty() => self.say(text),
                (Some("thinking"), _) if !streamed => {
                    if let Some(thinking) = block["thinking"].as_str().filter(|thinking| !thinking.is_empty()) {
                        self.updates.thought(thinking);
                    }
                }
                (Some("tool_use"), _) => self.tool_use(block),
                _ => {}
            }
        }
    }

    fn say(&mut self, text: &str) {
        if std::mem::take(&mut self.parted) && self.said {
            self.updates.message("\n\n");
        }
        self.said = true;
        self.updates.message(text);
    }

    fn tool_use(&mut self, block: &Value) {
        let (Some(id), Some(name)) = (block["id"].as_str(), block["name"].as_str()) else { return };
        let input = &block["input"];
        if name == "TodoWrite" {
            self.todos.insert(id.to_owned());
            self.updates.plan(plan(input));
            return;
        }
        let (kind, title) = describe(name, input, self.cwd);
        let path = ["file_path", "notebook_path", "path"].iter().find_map(|key| input[key].as_str());
        let line = input["offset"].as_u64().and_then(|line| u32::try_from(line.max(1)).ok());
        let locations = path.map(|path| ToolCallLocation::new(path).line(line)).into_iter().collect();
        let call =
            ToolCall::new(id.to_owned(), title).kind(kind).status(ToolCallStatus::InProgress).raw_input(input.clone());
        self.updates.tool_call(call.locations(locations));
    }

    fn tool_results(&self, content: &Value) {
        for block in content.as_array().into_iter().flatten().filter(|block| block["type"] == "tool_result") {
            let Some(id) = block["tool_use_id"].as_str().filter(|id| !self.todos.contains(*id)) else { continue };
            let status = if block["is_error"] == true { ToolCallStatus::Failed } else { ToolCallStatus::Completed };
            self.updates.tool_update(ToolCallUpdate::new(id.to_owned(), ToolCallUpdateFields::new().status(status)));
        }
    }

    fn system(&mut self, event: &Value) {
        match event["subtype"].as_str() {
            Some("init") => {
                self.model = event["model"].as_str().map(str::to_owned);
                let failing = event["mcp_servers"].as_array().into_iter().flatten().filter(|s| s["status"] == "failed");
                for server in failing {
                    tracing::warn!(harness = self.cli.name(), server = %server["name"], "Claude Code failed to start an MCP server");
                }
            }
            Some("permission_denied") => {
                let tool = event["tool_name"].as_str().unwrap_or("a tool");
                deny_approval(self.cli, &format!("{tool}: {}", event["message"].as_str().unwrap_or_default()));
            }
            Some("api_retry") => {
                tracing::info!(harness = self.cli.name(), error = %event["error"], status = %event["error_status"], "Claude Code retries a model call");
            }
            _ => {}
        }
    }
}

/// What a tool call of Claude Code's tool `name` with `input` is shown as.
fn describe(name: &str, input: &Value, cwd: &Path) -> (ToolKind, String) {
    let text = |key: &str| input[key].as_str().unwrap_or_default();
    let path = |key: &str| {
        let path = Path::new(text(key));
        path.strip_prefix(cwd).unwrap_or(path).display().to_string()
    };
    let (kind, title) = match name {
        "Bash" | "PowerShell" => (ToolKind::Execute, text("command").to_owned()),
        "Read" => (ToolKind::Read, format!("Read {}", path("file_path"))),
        "Write" => (ToolKind::Edit, format!("Write {}", path("file_path"))),
        "Edit" | "MultiEdit" => (ToolKind::Edit, format!("Edit {}", path("file_path"))),
        "NotebookEdit" => (ToolKind::Edit, format!("Edit {}", path("notebook_path"))),
        "Glob" => (ToolKind::Search, format!("Find {}", text("pattern"))),
        "Grep" => (ToolKind::Search, format!("Search {}", text("pattern"))),
        "ToolSearch" => (ToolKind::Search, format!("Find tools {}", text("query"))),
        "WebFetch" => (ToolKind::Fetch, format!("Fetch {}", text("url"))),
        "WebSearch" => (ToolKind::Fetch, format!("Search the web for {}", text("query"))),
        "Task" | "Agent" => (ToolKind::Think, text("description").to_owned()),
        "EnterPlanMode" | "ExitPlanMode" => (ToolKind::SwitchMode, String::new()),
        "Skill" => (ToolKind::Other, format!("Load skill {}", text("skill"))),
        _ => (ToolKind::Other, String::new()),
    };
    let title = if title.trim().is_empty() { name.to_owned() } else { title };
    (kind, title)
}

/// The plan of a TodoWrite call: its todos, the one in progress by its active form.
fn plan(input: &Value) -> Plan {
    let entries = input["todos"].as_array().into_iter().flatten().map(|todo| {
        let content = todo["content"].as_str().unwrap_or_default();
        let (status, content) = match todo["status"].as_str() {
            Some("in_progress") => (PlanEntryStatus::InProgress, todo["activeForm"].as_str().unwrap_or(content)),
            Some("completed") => (PlanEntryStatus::Completed, content),
            _ => (PlanEntryStatus::Pending, content),
        };
        PlanEntry::new(content, PlanEntryPriority::Medium, status)
    });
    Plan::new(entries.collect())
}

/// The context window of the turn's model, from its result.
fn window(result: &Value, model: Option<&str>) -> Option<u64> {
    let usage = result["modelUsage"].as_object()?;
    let own = model.and_then(|model| usage.get(model)).and_then(|usage| usage["contextWindow"].as_u64());
    own.or_else(|| usage.values().filter_map(|usage| usage["contextWindow"].as_u64()).max())
}

/// The tokens a turn used, every model's: what changed since the last result, else the turn's main calls; as input,
/// output, cache read, cache write and thought tokens.
fn token_counts(result: &Value) -> [u64; 5] {
    let per_model = result["model_usage_since_last_result"].as_object().filter(|models| !models.is_empty());
    let [input, output, read, write, thought] = match per_model {
        Some(models) => {
            ["inputTokens", "outputTokens", "cacheReadInputTokens", "cacheCreationInputTokens", "thinkingTokens"]
                .map(|key| models.values().filter_map(|usage| usage[key].as_u64()).sum())
        }
        None => {
            let usage = &result["usage"];
            let keys = ["input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"];
            let [input, output, read, write] = keys.map(|key| usage[key].as_u64().unwrap_or_default());
            [input, output, read, write, usage["output_tokens_details"]["thinking_tokens"].as_u64().unwrap_or_default()]
        }
    };
    [input, output, read, write, thought]
}

/// The usage of [`token_counts`] `counts`.
fn tokens([input, output, read, write, thought]: [u64; 5]) -> Usage {
    Usage::new(input + output + read + write, input, output)
        .cached_read_tokens(read)
        .cached_write_tokens(write)
        .thought_tokens(thought)
}

/// How a turn ended, from its result: a stop reason, or the error the daemon classifies.
fn ended(result: &Value, turn: &Turn<'_>, interrupted: bool) -> Result<StopReason, Error> {
    let terminal = result["terminal_reason"].as_str().unwrap_or_default();
    if interrupted || matches!(terminal, "aborted_streaming" | "aborted_tools") {
        return Ok(StopReason::Cancelled);
    }
    if result["subtype"] == "error_max_turns" {
        return Ok(StopReason::MaxTurnRequests);
    }
    if result["is_error"] != true {
        return Ok(match result["stop_reason"].as_str() {
            Some("max_tokens") => StopReason::MaxTokens,
            Some("refusal") => StopReason::Refusal,
            _ => StopReason::EndTurn,
        });
    }
    Err(error(result, turn.error.as_deref(), turn.synthetic.as_deref(), turn.limit.as_ref()))
}

/// The error of a failed turn's `result`, its last assistant message having been of error `kind` (such as
/// `rate_limit`) and Claude Code's own message `synthetic`, with the last rate limit state `limit`.
fn error(result: &Value, kind: Option<&str>, synthetic: Option<&str>, limit: Option<&Value>) -> Error {
    let text = error_text(result, synthetic);
    let kind = kind.unwrap_or_default();
    let status = result["api_error_status"].as_u64();
    if matches!(kind, "authentication_failed" | "oauth_org_not_allowed") || text.to_lowercase().contains("run /login") {
        return signed_out(text);
    }
    let rejected = limit.is_some_and(|limit| limit["status"] == "rejected");
    if matches!(kind, "billing_error" | "account_on_hold")
        || rejected
        || USAGE_LIMITS.iter().any(|start| text.contains(start))
    {
        return quota_exhausted(limit.and_then(resets), text);
    }
    if kind == "rate_limit" || status == Some(429) {
        // Claude Code says a capacity limit is "not your usage limit", which would read as an exhausted quota.
        return rate_limited(Some(429), None, text.replace(" (not your usage limit)", ""));
    }
    if kind == "overloaded" || status == Some(529) {
        return rate_limited(Some(529), None, text);
    }
    if result["terminal_reason"] == "prompt_too_long" || OVERFLOWS.iter().any(|start| text.contains(start)) {
        return context_overflow(text);
    }
    failed(text)
}

/// What a failed turn's `result` says: its message, else its errors, else Claude Code's own message.
fn error_text(result: &Value, synthetic: Option<&str>) -> String {
    if let Some(text) = result["result"].as_str().filter(|text| !text.is_empty()) {
        return text.to_owned();
    }
    let errors: Vec<&str> = result["errors"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
    if !errors.is_empty() {
        return errors.join("; ");
    }
    synthetic.map_or_else(|| format!("Claude Code ended the turn with {}", result["subtype"]), str::to_owned)
}

/// How long until a rejected rate limit resets.
fn resets(limit: &Value) -> Option<Duration> {
    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(limit["resetsAt"].as_u64()?);
    at.duration_since(SystemTime::now()).ok()
}

#[cfg(all(test, unix))]
mod tests;
