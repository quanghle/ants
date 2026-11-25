//! Claude Code's stream-json (`protocol = "claude"`): one `claude -p --input-format stream-json --output-format
//! stream-json` process per session, kept for its life, taking a user message per prompt on stdin and answering with
//! the events of the turn, a `result` ending each.
//!
//! The translator chooses a new session's id (`--session-id`), resumes one with `--resume` and forks one with
//! `--resume <parent> --fork-session --session-id <new>`. A fork's totals start at its parent's, so its ACP id carries
//! that base ([`based_id`]). A session's process gets its MCP servers on stdin (`mcp_set_servers`, with
//! `--strict-mcp-config`): the servers of the definition's `--mcp-config` and the ants ones, only the ants ones with
//! their tools allowed (`--allowedTools=mcp__<server>`). It gets the
//! definition's `mode` as `--permission-mode`, and `--permission-prompts none`, so whatever Claude Code would ask a
//! human about is denied rather than waited on; when the ant asks ants about its tools, `--permission-prompts host
//! --permission-prompt-tool stdio` instead, so it comes as a `can_use_tool` control request, sent to ants
//! ([`super::approve`]). Cancel is Claude Code's `interrupt`, dropping the steers queued behind the turn where Claude
//! Code can ([`CANCEL_QUEUED`]); the process is killed when the turn does not end within [`INTERRUPT_GRACE`]. Another
//! model is selected with `set_model` and another reasoning effort with `apply_flag_settings` (`effortLevel`).
//!
//! Between prompts Claude Code runs turns of its own, as when a background command it started ends after the turn
//! that started it: each is reported as it starts ([`control::Own`]) and its events are kept for the prompt taking it
//! ([`NativeSession::take_own_turn`]), which streams and answers with them, a prompt coming first waiting for it to
//! end. The events of a turn the report refuses are dropped, a prompt coming first still waiting for it to end.

mod control;
mod failure;
mod turn;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{ContentBlock, PromptResponse, StopReason};
use ants_proto::McpServerState;
use anyhow::bail;
use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, watch};

use self::control::{OWN_TURN, Shared, control_request, read};
use self::failure::{ended, error_text};
use self::turn::{Turn, token_counts, window};
use super::io::{older, probe_version};
use super::turn::{Spent, Tokens};
use super::{
    Cli, Credentials, Harness, Info, JsonLines, JsonWriter, Native, NativeSession, Open, Setting, Updates, based_id,
    cost_base, failed, is_flag, mcp_servers_json, signed_out,
};
use crate::harness::Skills;
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

/// The capability of `system/init` saying `interrupt` takes `cancel_queued`, which drops the user messages queued
/// behind the turn rather than running each as a turn of its own after it.
const CANCEL_QUEUED: &str = "interrupt_cancel_queued_v1";

/// How long an interrupted turn has to end before the CLI is killed.
const INTERRUPT_GRACE: Duration = Duration::from_secs(10);

/// How long a closed session's CLI has to exit, writing its cost totals to the transcript, before it is killed.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// The most turns the CLI ran of its own whose events are kept for the prompts yet to take them.
const STASHED: usize = 8;

pub(crate) struct Claude {
    cli: Cli,
    version: String,
    /// The servers of the definition's `--mcp-config`, which `mcp_set_servers` would otherwise drop.
    mcp: Map<String, Value>,
    /// Whether the CLI's interrupt takes `cancel_queued` ([`CANCEL_QUEUED`]), as the last `system/init` of any of its
    /// sessions said.
    cancel_queued: Arc<AtomicBool>,
}

/// Take `--mcp-config` out of `args` (repeatable, each taking one or more values, `--mcp-config VALUE...` or
/// `=VALUE`, a value inline JSON or a file of it) and read its servers, a later value's over an earlier one's.
fn take_mcp_config(args: &mut Vec<String>) -> Result<Map<String, Value>, String> {
    let mut servers = Map::new();
    let mut rest = Vec::new();
    let mut taken = std::mem::take(args).into_iter().peekable();
    while let Some(arg) = taken.next() {
        let values = if let Some(value) = arg.strip_prefix("--mcp-config=") {
            if value.is_empty() {
                return Err("--mcp-config takes JSON or a file of it".to_owned());
            }
            vec![value.to_owned()]
        } else if arg == "--mcp-config" {
            let mut values = Vec::new();
            while let Some(next) = taken.next_if(|next| !next.starts_with('-')) {
                values.push(next);
            }
            if values.is_empty() {
                return Err("--mcp-config takes JSON or a file of it".to_owned());
            }
            values
        } else {
            rest.push(arg);
            continue;
        };
        for value in values {
            let read = if value.trim_start().starts_with('{') {
                mcp_config(&value)?
            } else {
                std::fs::read_to_string(&value)
                    .map_err(|e| format!("--mcp-config: reading {value}: {e}"))
                    .and_then(|text| mcp_config(&text).map_err(|e| format!("{e} in {value}")))?
            };
            servers.extend(read);
        }
    }
    *args = rest;
    Ok(servers)
}

/// The servers of an MCP configuration, as `--mcp-config` takes it: `{"mcpServers": {NAME: SERVER}}`.
fn mcp_config(text: &str) -> Result<Map<String, Value>, String> {
    let config: Value = serde_json::from_str(text).map_err(|e| format!("--mcp-config: {e}"))?;
    let Some(Value::Object(servers)) = config.get("mcpServers") else {
        return Err("--mcp-config takes an object of servers in mcpServers".to_owned());
    };
    if let Some((name, _)) = servers.iter().find(|(_, server)| !server.is_object()) {
        return Err(format!("--mcp-config: server {name} is no object"));
    }
    Ok(servers.clone())
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
            bail!("protocol claude takes mode {}, not {mode}", crate::harness::one_of(MODES));
        }
        let context = harness.context.values().flat_map(|value| &value.args);
        for arg in harness.args.iter().chain(context) {
            if let Some(flag) = OWN_ARGS.iter().find(|flag| is_flag(arg, flag)) {
                bail!("protocol claude passes {flag} itself; remove it from the arguments");
            }
        }
        Ok(())
    }

    async fn start(mut cli: Cli) -> Result<Self, Error> {
        let mcp = take_mcp_config(&mut cli.args).map_err(|e| failed(format!("{}: {e}", cli.name())))?;
        let version = version(&cli).await?;
        Ok(Self { cli, version, mcp, cancel_queued: Arc::default() })
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

    const ASKS: bool = true;

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
    let output = probe_version(cli).await?;
    let text = String::from_utf8_lossy(&output.stdout);
    let version = text.split_whitespace().next().unwrap_or_default();
    if version.split('.').map_while(|part| part.parse::<u64>().ok()).count() != 3 {
        return Err(failed(format!(
            "{}: {} --version printed {:?}, not a Claude Code version",
            cli.name(),
            cli.program().display(),
            text.trim()
        )));
    }
    if older(version, MIN_VERSION) {
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
        if let Some(effort) = self.cli.effort() {
            check_effort(&self.cli, effort)?;
        }
        let servers = mcp_servers_json(&open.mcp_servers);
        let cli = ENV.iter().fold(self.cli.clone(), |cli, (name, value)| cli.with_env_default(name, value));
        let (process, stdin, stdout) =
            cli.spawn(self.args(&how, &servers, open.skills.as_ref()), Some(&open.cwd)).await?;
        let (events, received) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            cli: self.cli.clone(),
            cwd: open.cwd.clone(),
            asking: Mutex::default(),
            writer: JsonWriter::new(stdin),
            process: tokio::sync::Mutex::new(Some(process)),
            pending: Mutex::new(Some(HashMap::new())),
            requests: AtomicU64::new(0),
            own: Mutex::default(),
            own_turns: open.own_turns.clone(),
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
            spent: Mutex::new(Spent::from(base)),
            steers: Mutex::default(),
            cancel_queued: self.cancel_queued.clone(),
            stash: Mutex::default(),
        };
        let hello = session.shared.control(json!({ "subtype": "initialize", "hooks": null })).await?;
        if no_credentials(&hello["account"]) {
            return Err(signed_out(
                "Claude Code has neither a login nor an API key (Not logged in · Please run /login)",
            ));
        }
        let mut all = self.mcp.clone();
        all.extend(servers.as_object().cloned().unwrap_or_default());
        if !all.is_empty() {
            let servers = all;
            let set = session.shared.control(json!({ "subtype": "mcp_set_servers", "servers": servers })).await?;
            if let Some(errors) = set["errors"].as_object().filter(|errors| !errors.is_empty()) {
                let errors: Vec<String> = errors.iter().map(|(name, error)| format!("{name}: {error}")).collect();
                return Err(failed(format!("{}: MCP servers refused: {}", self.cli.name(), errors.join("; "))));
            }
        }
        Ok(session)
    }

    /// The protocol's arguments for a session opened `how`, with MCP servers `servers` and `skills`.
    fn args(&self, how: &How, servers: &Value, skills: Option<&Skills>) -> Vec<String> {
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
            "--strict-mcp-config",
        ]
        .map(str::to_owned)
        .into();
        let prompts: &[&str] = if self.cli.asks() {
            &["--permission-prompts", "host", "--permission-prompt-tool", "stdio"]
        } else {
            &["--permission-prompts", "none"]
        };
        args.extend(prompts.iter().map(|arg| (*arg).to_owned()));
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
        if let Some(skills) = skills {
            args.extend(["--plugin-dir".to_owned(), skills.plugin.display().to_string()]);
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

/// Fails unless Claude Code takes reasoning effort `effort`.
fn check_effort(cli: &Cli, effort: &str) -> Result<(), Error> {
    if EFFORTS.contains(&effort) {
        return Ok(());
    }
    Err(failed(format!("{}: Claude Code takes reasoning_effort {}, not {effort}", cli.name(), EFFORTS.join(", "))))
}

/// The control request interrupting the turn, dropping the user messages queued behind it when `cancel_queued`.
fn interrupt(cancel_queued: bool) -> Value {
    if cancel_queued {
        json!({ "subtype": "interrupt", "cancel_queued": true })
    } else {
        json!({ "subtype": "interrupt" })
    }
}

/// Whether the `account` of Claude Code's `initialize` answer has no credentials: no login, no API key, and the
/// Anthropic API as provider (Bedrock, Vertex and Foundry bring their own).
fn no_credentials(account: &Value) -> bool {
    account["tokenSource"] == "none"
        && account["apiKeySource"].as_str().is_none_or(|source| source == "none")
        && account["apiProvider"].as_str().is_none_or(|provider| provider == "firstParty")
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
    /// Whether the interrupt drops the steers queued behind the turn ([`Claude::cancel_queued`]).
    cancel_queued: Arc<AtomicBool>,
    stash: Mutex<Stash>,
}

/// The turns the CLI ran of its own between prompts, as the session's events hold them, each after a line of
/// [`OWN_TURN`] naming it: kept for the prompt taking it ([`NativeSession::take_own_turn`]).
#[derive(Debug, Default)]
struct Stash {
    /// The turn whose events come next, until it ends.
    current: Option<String>,
    /// The events of each turn no prompt took yet, oldest first, at most [`STASHED`].
    turns: VecDeque<(String, VecDeque<Value>)>,
}

/// The messages steering the turn running ([`NativeSession::steer`]), written to stdin as it runs. Claude Code takes
/// one between model calls, or after the turn's `result` as a turn of its own, replaying it as it does
/// (`--replay-user-messages`). The prompt ends once the session is idle (`session_state_changed`), or at a `result`
/// from a Claude Code that does not report its state, and only once each steer was replayed. A cancel ends it at the
/// interrupted turn's end, the steers not taken in dropped with it where Claude Code can ([`CANCEL_QUEUED`]).
#[derive(Debug, Default)]
struct Steers {
    /// Whether a turn runs that takes steers: until it ends.
    open: bool,
    /// The uuids of the messages written but not replayed.
    pending: HashSet<String>,
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
        let cancel_queued = self.cancel_queued.load(Ordering::Relaxed);
        // Claude Code writes its cost totals to the transcript as it exits, which resuming restores: let it.
        runtime.spawn(async move {
            // Writing fails only once the CLI is gone.
            if running {
                let interrupt = control_request("ants-close", &interrupt(cancel_queued));
                let _ = shared.writer.send(&interrupt).await;
            }
            let mut process = shared.process.lock().await.take();
            // What Claude Code runs in the background is reparented once it exits, which closing its stdin makes it do.
            if let Some(process) = process.as_mut() {
                process.walk().await;
            }
            let _ = shared.writer.close().await;
            if let Some(mut process) = process {
                process.stop(STOP_GRACE).await;
            }
            reader.inspect(tokio::task::JoinHandle::abort);
        });
    }
}

/// Ends the session's turn when its prompt ends, however it does: its tool requests then go nowhere, it takes no more
/// steers and no turn runs.
struct Ended<'a>(&'a ClaudeSession);

impl Drop for Ended<'_> {
    fn drop(&mut self) {
        self.0.shared.asking.locked().take();
        self.0.steers.locked().open = false;
        self.0.running.send_replace(None);
    }
}

impl ClaudeSession {
    /// Have Claude Code write the forked session's transcript, which it does at the first message, without a turn:
    /// an empty message that queries nothing. Its result tells what the fork's totals start at.
    async fn keep(&self) -> Result<(), Error> {
        let mut events = self.events.lock().await;
        let _prompting = self.shared.prompting();
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

    /// Keep what the CLI sent between prompts in the stash: the events of each turn it ran of its own, the rest
    /// dropped, such as a background task's notice or a turn the daemon refused. Waits for a turn of its own it still
    /// runs to end, unless it is `own`, which the prompt takes as it runs.
    async fn settle(&self, events: &mut mpsc::UnboundedReceiver<Value>, own: Option<&str>) -> Result<(), Error> {
        loop {
            let event = match events.try_recv() {
                Ok(event) => event,
                Err(_) => {
                    let current = self.stash.locked().current.clone();
                    if current.is_none() || current.as_deref() == own {
                        return Ok(());
                    }
                    match events.recv().await {
                        Some(event) => event,
                        None => return Err(self.shared.exited().await),
                    }
                }
            };
            let mut stash = self.stash.locked();
            if event["type"] == OWN_TURN {
                let id = event["id"].as_str().unwrap_or_default().to_owned();
                if event["dropped"] != true {
                    if stash.turns.len() >= STASHED {
                        stash.turns.pop_front();
                    }
                    stash.turns.push_back((id.clone(), VecDeque::new()));
                }
                stash.current = Some(id);
                continue;
            }
            let Some(current) = stash.current.clone() else { continue };
            if self.shared.ends_turn(&event) {
                stash.current = None;
            }
            if let Some((_, turn)) = stash.turns.iter_mut().rev().find(|(id, _)| *id == current) {
                turn.push_back(event);
            }
        }
    }

    /// The events of turn `id` the CLI ran of its own, so far, taken from the stash; None when it holds none.
    fn take(&self, id: &str) -> Option<VecDeque<Value>> {
        let mut stash = self.stash.locked();
        let at = stash.turns.iter().position(|(stashed, _)| stashed == id)?;
        if stash.current.as_deref() == Some(id) {
            stash.current = None;
        }
        stash.turns.remove(at).map(|(_, events)| events)
    }

    /// Run a prompt: with `prompt` as its message, else taking turn `own` the CLI runs of its own, should it hold it.
    async fn run(&self, own: Option<&str>, prompt: &[ContentBlock], updates: Updates) -> Result<PromptResponse, Error> {
        let mut events = self.events.lock().await;
        let turn = self.turns.fetch_add(1, Ordering::Relaxed) + 1;
        self.interrupted.store(false, Ordering::Relaxed);
        *self.steers.locked() = Steers::default();
        self.running.send_replace(Some(turn));
        *self.shared.asking.locked() = Some(updates.clone());
        let _ended = Ended(self);
        let _prompting = self.shared.prompting();
        self.settle(&mut events, own).await?;
        let taken = own.and_then(|id| self.take(id));
        if taken.is_none() {
            // A turn of its own the CLI runs first: the message would steer it.
            self.settle(&mut events, None).await?;
            if self.interrupted.load(Ordering::Relaxed) {
                return Ok(PromptResponse::new(StopReason::Cancelled));
            }
        }
        self.turn(&mut events, taken, prompt, &updates).await
    }

    /// Run a turn, prompted with `prompt` unless `taken` holds the events so far of the turn the CLI runs of its own.
    async fn turn(
        &self,
        events: &mut mpsc::UnboundedReceiver<Value>,
        taken: Option<VecDeque<Value>>,
        prompt: &[ContentBlock],
        updates: &Updates,
    ) -> Result<PromptResponse, Error> {
        let mut taken = match taken {
            Some(taken) => taken,
            None => {
                let message = self.message(prompt, &uuid::Uuid::new_v4().to_string());
                if self.shared.writer.send(&message).await.is_err() {
                    return Err(self.shared.exited().await);
                }
                VecDeque::new()
            }
        };
        self.steers.locked().open = true;
        let mut turn = Turn::new(&self.shared.cli, &self.cwd, updates);
        let (mut counts, mut last, mut states) = (Tokens::default(), None, false);
        let result = loop {
            let event = match taken.pop_front() {
                Some(event) => event,
                None => match events.recv().await {
                    Some(event) => event,
                    None => return Err(self.shared.exited().await),
                },
            };
            let idle = match (event["type"].as_str(), event["subtype"].as_str()) {
                (Some("user"), _) if event["isReplay"] == true => {
                    self.steers.locked().pending.remove(event["uuid"].as_str().unwrap_or_default());
                    continue;
                }
                (Some("system"), Some("init")) => {
                    let capabilities = event["capabilities"].as_array().map_or(&[][..], Vec::as_slice);
                    self.cancel_queued.store(capabilities.iter().any(|c| c == CANCEL_QUEUED), Ordering::Relaxed);
                    turn.event(event);
                    continue;
                }
                (Some("system"), Some("session_state_changed")) => {
                    states = true;
                    event["state"] == "idle"
                }
                _ => {
                    let Some(result) = turn.event(event) else { continue };
                    counts.add(token_counts(&result));
                    last = Some(result);
                    turn.part += 1;
                    !states
                }
            };
            if !idle {
                continue;
            }
            let Some(result) = last.take() else {
                // An interrupt dropping the queued messages dropped the prompt's own before its turn started.
                if self.interrupted.load(Ordering::Relaxed) {
                    return Ok(PromptResponse::new(StopReason::Cancelled));
                }
                continue;
            };
            let mut steers = self.steers.locked();
            if steers.pending.is_empty() || self.interrupted.load(Ordering::Relaxed) {
                steers.open = false;
                break result;
            }
            last = Some(result);
        };
        let spent = {
            let mut spent = self.spent.locked();
            let total = result["total_cost_usd"].as_f64().unwrap_or_default();
            spent.cost = Some(spent.cost.unwrap_or_default().max(total - spent.base));
            spent.size = window(&result, turn.model.as_deref()).unwrap_or(spent.size);
            if turn.used > 0 {
                spent.used = turn.used;
            }
            *spent
        };
        spent.report(updates);
        let stop = ended(&result, &turn, self.interrupted.load(Ordering::Relaxed))?;
        // The total counts the cache's tokens too.
        let total = counts.input + counts.output + counts.cache_read + counts.cache_write;
        Ok(PromptResponse::new(stop).usage(counts.usage(total)))
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
        self.run(None, &prompt, updates).await
    }

    async fn take_own_turn(
        &self,
        id: &str,
        prompt: Vec<ContentBlock>,
        updates: Updates,
    ) -> Result<PromptResponse, Error> {
        self.run(Some(id), &prompt, updates).await
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
        let cancel_queued = self.cancel_queued.load(Ordering::Relaxed);
        let interrupt = self.shared.control(interrupt(cancel_queued));
        match tokio::time::timeout(INTERRUPT_GRACE, interrupt).await {
            // The steers written but not taken in are dropped, so none is replayed.
            Ok(Ok(_)) if cancel_queued => self.steers.locked().pending.clear(),
            Ok(Err(e)) => tracing::warn!(harness = self.shared.cli.name(), error = %e, "interrupting Claude Code"),
            _ => {}
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

    async fn configure(&self, setting: Setting, value: &str) -> Result<(), Error> {
        let request = match setting {
            Setting::Model => json!({ "subtype": "set_model", "model": value }),
            Setting::ReasoningEffort => {
                check_effort(&self.shared.cli, value)?;
                json!({ "subtype": "apply_flag_settings", "settings": { "effortLevel": value } })
            }
        };
        self.shared.control(request).await.map(drop)
    }
}

/// An MCP server as Claude Code's `mcp_status` reports it.
fn mcp_server(server: &Value) -> McpServerState {
    let status = super::mcp_status(server["status"].as_str());
    let tools = server["tools"].as_array().map(Vec::len);
    super::mcp_server_state(server, status, server["error"].as_str(), server["scope"].as_str(), tools)
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

#[cfg(all(test, unix))]
mod tests;
