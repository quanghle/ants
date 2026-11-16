//! Antigravity CLI's headless stream-json (`protocol = "antigravity"`): `agy --input-format stream-json
//! --output-format stream-json`, one process per session. The process prints `init` with the conversation id when it
//! starts, then runs a turn for each `user` line it reads, streaming `step_update` events and ending the turn with one
//! `result`. A session resumes with `--conversation <id>`; agy cannot fork one.
//!
//! agy takes per-run settings only as one `--settings` (the last given wins) and MCP servers only from files, so each
//! session gets a private directory holding its settings (the definition's own `--settings` merged in) and, given to
//! agy with `--add-dir`, `.agents/mcp_config.json` with ants' MCP servers; the directory goes with the session.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use agent_client_protocol::v1::schema::{
    ContentBlock, PromptResponse, StopReason, ToolCall, ToolCallLocation, ToolCallStatus, ToolCallUpdate,
    ToolCallUpdateFields, ToolKind, Usage,
};
use agent_client_protocol::{Error, ErrorCode};
use anyhow::bail;
use serde_json::{Value, json};
use tokio::process::ChildStdin;
use tokio::sync::{mpsc, oneshot};

use super::{
    Cli, Harness, Info, JsonLines, JsonWriter, Native, NativeSession, Open, Process, Updates, context_overflow,
    deny_approval, failed, mcp_servers_json, prompt_text, quota_exhausted, rate_limited, signed_out,
};
use crate::sync::MutexExt as _;

/// The environment variable that, `true` (not `1`), keeps agy from replacing itself with a newer release as it runs.
const AUTO_UPDATE: &str = "AGY_CLI_DISABLE_AUTO_UPDATE";

/// The oldest agy whose headless stream-json this speaks.
const OLDEST: [u64; 3] = [1, 5, 9];

/// agy's tool permission modes (its `toolPermission` setting), which `mode` names.
const MODES: &[&str] = &["request-review", "proceed-in-sandbox", "always-proceed", "strict"];

/// Flags of agy the protocol sets or that would change how it speaks, refused in a definition's arguments.
const OWN_FLAGS: &[&str] = &[
    "input-format",
    "output-format",
    "conversation",
    "continue",
    "c",
    "print",
    "p",
    "prompt",
    "prompt-interactive",
    "i",
    "print-timeout",
];

/// How long an ants MCP server has to connect, and each of its tool calls to answer: `ask_ant` waits up to 30 minutes.
const MCP_TIMEOUT_SECS: u64 = 31 * 60;

/// How a `result` error begins that agy keeps reporting after a retry of the turn succeeded, and on every later turn.
const STALE_RETRY: &str = "API error (attempt ";

/// The error of a `result` agy prints instead of `init` when it is not signed in.
const NOT_SIGNED_IN: &str = "authentication failed or timed out";

/// How long `agy --version` may take.
const VERSION_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Antigravity {
    cli: Cli,
    version: String,
    /// The new conversation a resume found no conversation for and agy opened instead, kept for the new session the
    /// daemon opens next, with what it was opened with.
    spare: Mutex<Option<(Open, AgySession)>>,
}

impl Native for Antigravity {
    type Session = AgySession;

    fn validate(harness: &Harness) -> anyhow::Result<()> {
        if let Some(mode) = harness.mode.as_deref().filter(|mode| !MODES.contains(mode)) {
            bail!("protocol antigravity takes mode {}, not {mode}", crate::harness::one_of(MODES));
        }
        let lists = std::iter::once(&harness.args).chain(harness.context.values().map(|value| &value.args));
        for args in lists {
            if let Some(own) = args.iter().filter_map(|arg| flag(arg)).find(|(name, _)| OWN_FLAGS.contains(name)) {
                bail!("protocol antigravity sets agy's --{} itself; remove it from the arguments", own.0);
            }
            match split_settings(args) {
                Err(e) => bail!("{e}"),
                Ok((_, Some(given))) if given.trim_start().starts_with('{') => {
                    if let Err(e) = inline_settings(&given) {
                        bail!("--settings {given}: {e}");
                    }
                }
                Ok(_) => {}
            }
        }
        Ok(())
    }

    async fn start(mut cli: Cli) -> Result<Self, Error> {
        cli.env.entry(AUTO_UPDATE.to_owned()).or_insert_with(|| "true".to_owned());
        let version = version(&cli).await?;
        Ok(Self { cli, version, spare: Mutex::default() })
    }

    fn info(&self) -> Info {
        Info {
            name: "agy".to_owned(),
            version: self.version.clone(),
            load: true,
            fork: false,
            images: false,
            http_mcp: true,
        }
    }

    async fn new_session(&self, open: Open) -> Result<AgySession, Error> {
        let spare = self.spare.locked().take();
        match spare {
            Some((opened, session)) if opened == open => Ok(session),
            _ => AgySession::open(&self.cli, open, None).await,
        }
    }

    async fn load_session(&self, id: &str, open: Open) -> Result<AgySession, Error> {
        let session = AgySession::open(&self.cli, open.clone(), Some(id)).await?;
        if session.id == id {
            return Ok(session);
        }
        // agy opens a new conversation when it has none of the id, saying so only on stderr.
        tracing::debug!(
            harness = self.cli.name(),
            requested = id,
            opened = session.id,
            "agy opened a new conversation"
        );
        *self.spare.locked() = Some((open, session));
        Err(Error::new(ErrorCode::RESOURCE_NOT_FOUND, format!("agy has no conversation {id}")))
    }
}

/// agy's version, which `agy --version` prints, if it is one this speaks to.
async fn version(cli: &Cli) -> Result<String, Error> {
    let bare = Cli { args: Vec::new(), ..cli.clone() };
    let run = tokio::time::timeout(VERSION_TIMEOUT, bare.command(["--version"]).output()).await;
    let output = match run {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Err(failed(format!("{}: starting {}: {e}", cli.name(), cli.program().display()))),
        Err(_) => {
            return Err(failed(format!("{}: agy --version did not answer within {VERSION_TIMEOUT:?}", cli.name())));
        }
    };
    let printed = String::from_utf8_lossy(&output.stdout);
    let found = printed.split_whitespace().find_map(|word| {
        let parts: Option<Vec<u64>> = word.split('.').map(|part| part.parse().ok()).collect();
        parts.filter(|parts| parts.len() >= 2).map(|parts| (word.to_owned(), parts))
    });
    let Some((version, parts)) = found else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(failed(format!(
            "{}: agy --version printed no version: {} {}",
            cli.name(),
            printed.trim(),
            stderr.trim()
        )));
    };
    if parts.as_slice() < OLDEST.as_slice() {
        let [major, minor, patch] = OLDEST;
        return Err(failed(format!(
            "agy {version} is older than {major}.{minor}.{patch}, the first whose headless stream-json protocol \
             antigravity speaks; update agy (agy update)"
        )));
    }
    Ok(version)
}

/// A flag of agy's command line, with or without its `=value`: agy takes `-name` and `--name`.
fn flag(arg: &str) -> Option<(&str, Option<&str>)> {
    let name = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-')).filter(|name| !name.is_empty())?;
    Some(match name.split_once('=') {
        Some((name, value)) => (name, Some(value)),
        None => (name, None),
    })
}

/// `args` without their `--settings`, and the value of the last one, which agy would take.
fn split_settings(args: &[String]) -> Result<(Vec<String>, Option<String>), String> {
    let (mut kept, mut given) = (Vec::new(), None);
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match flag(arg) {
            Some(("settings", Some(value))) => given = Some(value.to_owned()),
            Some(("settings", None)) => given = Some(args.next().ok_or("--settings takes a value")?.clone()),
            _ => kept.push(arg.clone()),
        }
    }
    Ok((kept, given))
}

/// Settings given inline: a JSON object.
fn inline_settings(given: &str) -> Result<serde_json::Map<String, Value>, String> {
    match serde_json::from_str(given) {
        Ok(Value::Object(settings)) => Ok(settings),
        Ok(_) => Err("not a JSON object".to_owned()),
        Err(e) => Err(e.to_string()),
    }
}

/// The settings a session of `cli` runs with: those of the definition's last `--settings`, inline or a file (relative
/// to the session's directory), the definition's `mode` as agy's `toolPermission`, and rules allowing the tools of
/// ants' MCP servers. With the definition's arguments left once `--settings` is taken out.
fn settings(cli: &Cli, open: &Open) -> Result<(Vec<String>, Value), Error> {
    let invalid = |e: String| failed(format!("{}: {e}", cli.name()));
    let (args, given) = split_settings(cli.args()).map_err(invalid)?;
    let mut settings = match &given {
        None => Ok(serde_json::Map::new()),
        Some(inline) if inline.trim_start().starts_with('{') => inline_settings(inline),
        Some(file) => std::fs::read_to_string(open.cwd.join(file))
            .map_err(|e| e.to_string())
            .and_then(|text| inline_settings(&text)),
    }
    .map_err(|e| invalid(format!("--settings {}: {e}", given.as_deref().unwrap_or_default())))?;
    if let Some(mode) = cli.mode() {
        settings.insert("toolPermission".to_owned(), mode.into());
    }
    let servers = mcp_servers_json(&open.mcp_servers);
    let names = servers.as_object().into_iter().flat_map(serde_json::Map::keys);
    let rules: Vec<Value> = names.map(|name| format!("mcp({name}/*)").into()).collect();
    if !rules.is_empty() {
        let permissions = settings.entry("permissions").or_insert_with(|| json!({}));
        let allow =
            permissions.as_object_mut().map(|permissions| permissions.entry("allow").or_insert_with(|| json!([])));
        let Some(allow) = allow.and_then(Value::as_array_mut) else {
            return Err(invalid("--settings: permissions.allow is not a list".to_owned()));
        };
        for rule in rules {
            if !allow.contains(&rule) {
                allow.push(rule);
            }
        }
    }
    Ok((args, Value::Object(settings)))
}

/// ants' MCP servers as agy's `mcp_config.json` has them.
fn mcp_config(open: &Open) -> Value {
    let mut config = serde_json::Map::new();
    for (name, server) in mcp_servers_json(&open.mcp_servers).as_object().into_iter().flatten() {
        let mut entry = match server["type"].as_str() {
            Some("http") => json!({ "serverUrl": server["url"], "headers": server["headers"] }),
            _ => json!({ "command": server["command"], "args": server["args"], "env": server["env"] }),
        };
        entry["timeoutSeconds"] = MCP_TIMEOUT_SECS.into();
        config.insert(name.clone(), entry);
    }
    json!({ "mcpServers": config })
}

/// A directory only the daemon's user reads, removed when dropped: a session's settings and MCP servers, which carry
/// the ant's token.
struct Private(PathBuf);

impl Private {
    fn new(cli: &Cli) -> Result<Self, Error> {
        let dir = std::env::temp_dir().join(format!("ants-agy-{}", uuid::Uuid::new_v4().simple()));
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(&dir).map_err(|e| failed(format!("{}: creating {}: {e}", cli.name(), dir.display())))?;
        Ok(Self(dir))
    }

    /// Write `value` to `name` in it.
    fn write(&self, cli: &Cli, name: &str, value: &Value) -> Result<PathBuf, Error> {
        let path = self.0.join(name);
        let written = path
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| std::fs::write(&path, value.to_string()));
        written.map_err(|e| failed(format!("{}: writing {}: {e}", cli.name(), path.display())))?;
        Ok(path)
    }
}

impl Drop for Private {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            tracing::debug!(error = %e, dir = %self.0.display(), "removing a session's agy settings");
        }
    }
}

/// A session: one agy process on its conversation, started again on it after a cancel or a failure ended it.
pub(crate) struct AgySession {
    id: String,
    /// The CLI with the definition's arguments but `--settings`.
    cli: Cli,
    /// The protocol's arguments.
    launch: Vec<String>,
    cwd: PathBuf,
    running: tokio::sync::Mutex<Option<Running>>,
    /// Ends the prompt running.
    cancel: Mutex<Option<oneshot::Sender<()>>>,
    _private: Private,
}

/// A running agy, and the events it prints.
struct Running {
    process: Process,
    /// The denied actions its results listed so far: each lists those of the process's earlier turns too.
    denied: usize,
    writer: JsonWriter<ChildStdin>,
    events: mpsc::UnboundedReceiver<Value>,
    reader: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl AgySession {
    /// Start agy in a new conversation, or on conversation `resume`.
    async fn open(cli: &Cli, open: Open, resume: Option<&str>) -> Result<Self, Error> {
        let private = Private::new(cli)?;
        let (args, settings) = settings(cli, &open)?;
        let settings = private.write(cli, "settings.json", &settings)?;
        let mut launch: Vec<String> = [
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--disable-slash-commands",
            "--settings",
        ]
        .map(str::to_owned)
        .into();
        launch.push(settings.display().to_string());
        if !open.mcp_servers.is_empty() {
            private.write(cli, ".agents/mcp_config.json", &mcp_config(&open))?;
            launch.extend(["--add-dir".to_owned(), private.0.display().to_string()]);
        }
        for (name, value) in [("--model", cli.model()), ("--effort", cli.effort())] {
            if let Some(value) = value {
                launch.extend([name.to_owned(), value.to_owned()]);
            }
        }
        let cli = Cli { args, ..cli.clone() };
        let (running, id) = run(&cli, &launch, &open.cwd, resume).await?;
        Ok(Self {
            id,
            cli,
            launch,
            cwd: open.cwd,
            running: tokio::sync::Mutex::new(Some(running)),
            cancel: Mutex::default(),
            _private: private,
        })
    }
}

/// Start agy with `launch` in `cwd`, on conversation `resume` if given, and read its `init`: the process, and the id of
/// the conversation it opened.
async fn run(cli: &Cli, launch: &[String], cwd: &Path, resume: Option<&str>) -> Result<(Running, String), Error> {
    let mut args = launch.to_vec();
    if let Some(id) = resume {
        args.extend(["--conversation".to_owned(), id.to_owned()]);
    }
    let mut process = cli.spawn(&args, Some(cwd))?;
    let (stdin, stdout) = process.take_io().ok_or_else(|| failed("no stdio"))?;
    let mut lines = JsonLines::new(stdout);
    loop {
        let event = match lines.next().await {
            Ok(Some(event)) => event,
            Ok(None) | Err(_) => return Err(process.exited().await),
        };
        match event["event"].as_str() {
            Some("init") => {
                let id = event["conversation_id"].as_str().unwrap_or_default().to_owned();
                let (tx, events) = mpsc::unbounded_channel();
                let reader = tokio::spawn(async move {
                    while let Ok(Some(event)) = lines.next().await {
                        if tx.send(event).is_err() {
                            break;
                        }
                    }
                });
                return Ok((Running { process, denied: 0, writer: JsonWriter::new(stdin), events, reader }, id));
            }
            // A failure to start: not signed in, an unknown model or agent.
            Some("result") => {
                let _ = process.exited().await;
                return Err(failure(event["result"]["error"].as_str().unwrap_or_default(), &process.stderr()));
            }
            _ => tracing::debug!(harness = cli.name(), ?event, "agy printed something before init"),
        }
    }
}

/// The error of a turn or start agy failed with `error`, classified by it and the `AGY_ERROR` line of `stderr`.
fn failure(error: &str, stderr: &str) -> Error {
    let reported = stderr
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("AGY_ERROR: "))
        .and_then(|line| serde_json::from_str::<Value>(line).ok())
        .unwrap_or_default();
    let (code, status) = (reported["error_code"].as_u64(), reported["status"].as_str());
    let lower = error.to_lowercase();
    let signed_out_error = error.contains(NOT_SIGNED_IN)
        || error.contains("API key not valid")
        || stderr.lines().any(|line| line.starts_with("Error: authentication required"))
        || code == Some(401);
    if signed_out_error {
        return signed_out(error);
    }
    if lower.contains("exceeds the maximum number of tokens allowed") {
        return context_overflow(error);
    }
    if lower.contains("credits balance is too low") || lower.contains("daily quota") {
        return quota_exhausted(None, error);
    }
    if code == Some(429) || status == Some("RESOURCE_EXHAUSTED") || lower.contains("error 429") {
        return rate_limited(Some(429), None, error);
    }
    if error.is_empty() {
        return failed(format!("agy failed: {stderr}"));
    }
    failed(error)
}

/// What a turn's steps came to.
#[derive(Default)]
struct Steps {
    /// Tokens: input, output, thinking, cached input read, total.
    usage: [u64; 5],
    /// The tool calls seen.
    tools: HashSet<String>,
    /// An API error the turn has not answered after.
    unanswered: bool,
}

impl Steps {
    fn step(&mut self, step: &Value, updates: &Updates) {
        let usage = &step["usage"];
        let counts = ["input_tokens", "output_tokens", "thinking_tokens", "cache_read_tokens", "total_tokens"];
        for (sum, name) in self.usage.iter_mut().zip(counts) {
            *sum += usage[name].as_u64().unwrap_or_default();
        }
        match step["step_type"].as_str() {
            Some("agent_response") => {
                if let Some(text) = step["text_delta"].as_str().filter(|text| !text.is_empty()) {
                    updates.message(text);
                }
                if step["state"] != "ACTIVE" {
                    self.unanswered = false;
                }
            }
            Some("error_message") => self.unanswered = true,
            Some("tool" | "subagent") => self.tool(step, updates),
            _ => {}
        }
    }

    /// A tool or subagent step: a tool call when first seen, else what changed of it.
    fn tool(&mut self, step: &Value, updates: &Updates) {
        let id = format!("agy-step-{}", step["step_index"]);
        let status = match step["state"].as_str() {
            Some("ACTIVE") => ToolCallStatus::InProgress,
            Some("ERROR") => ToolCallStatus::Failed,
            _ => ToolCallStatus::Completed,
        };
        let info = &step["tool_info"];
        let output = match (&info["output"], &info["error"]["message"]) {
            (_, Value::String(error)) => Some(json!({ "error": error })),
            (Value::String(output), _) => Some(output.clone().into()),
            _ => None,
        };
        if self.tools.insert(id.clone()) {
            updates.tool_call(described(id, step).status(status).raw_output(output));
        } else {
            updates.tool_update(ToolCallUpdate::new(id, ToolCallUpdateFields::new().status(status).raw_output(output)));
        }
    }

    fn usage(&self) -> Usage {
        let [input, output, thinking, cached, total] = self.usage;
        Usage::new(total, input, output).thought_tokens(thinking).cached_read_tokens(cached)
    }
}

/// The tool call of a tool or subagent step.
fn described(id: String, step: &Value) -> ToolCall {
    let tool = step["tool_name"].as_str().unwrap_or("tool");
    let parameters = &step["tool_info"]["parameters"];
    let path = parameters["Path"].as_str();
    let kind = match tool {
        "read_file" => ToolKind::Read,
        "write_file" | "edit_file" => ToolKind::Edit,
        "run_command" => ToolKind::Execute,
        "search_web" => ToolKind::Fetch,
        _ => ToolKind::Other,
    };
    let title = match (tool, path) {
        ("run_command", _) => parameters["CommandLine"].as_str().unwrap_or(tool).to_owned(),
        ("call_mcp_tool", _) => format!(
            "{}/{}",
            parameters["ServerName"].as_str().unwrap_or_default(),
            parameters["ToolName"].as_str().unwrap_or_default()
        ),
        (_, Some(path)) => format!("{tool} {path}"),
        _ if step["step_type"] == "subagent" => {
            let subagents = step["subagent_info"]["subagents"].as_array().into_iter().flatten();
            let roles: Vec<&str> = subagents.filter_map(|subagent| subagent["role"].as_str()).collect();
            format!("{tool} {}", roles.join(", ")).trim_end().to_owned()
        }
        _ => tool.to_owned(),
    };
    let input = match step["step_type"].as_str() {
        Some("subagent") => step["subagent_info"].clone(),
        _ => parameters.clone(),
    };
    let call = ToolCall::new(id, title).kind(kind).raw_input((!input.is_null()).then_some(input));
    match path {
        Some(path) => call.locations(vec![ToolCallLocation::new(path)]),
        None => call,
    }
}

/// How a turn ended, by its `result`.
#[derive(Debug, PartialEq)]
enum Outcome {
    Ended(StopReason),
    Failed(String),
}

/// How the turn whose steps came to `steps` ended, by its `result`, `interrupted` telling whether it was cancelled.
fn outcome(result: &Value, steps: &Steps, interrupted: bool) -> Outcome {
    let error = result["error"].as_str().unwrap_or_default();
    if result["finished_reason"] == "interrupted" {
        if interrupted {
            return Outcome::Ended(StopReason::Cancelled);
        }
        return Outcome::Failed(format!("agy was interrupted: {error}"));
    }
    // After a retry that succeeded, agy reports the turn, and every later one, as failed with the retry's error.
    let stale = error.starts_with(STALE_RETRY) && !steps.unanswered;
    if result["status"] == "SUCCESS" || stale {
        return Outcome::Ended(StopReason::EndTurn);
    }
    Outcome::Failed(error.to_owned())
}

impl NativeSession for AgySession {
    fn id(&self) -> String {
        self.id.clone()
    }

    async fn prompt(&self, prompt: Vec<ContentBlock>, updates: Updates) -> Result<PromptResponse, Error> {
        let (cancel, cancelled) = oneshot::channel();
        *self.cancel.locked() = Some(cancel);
        let done = self.prompted(&prompt_text(&prompt), &updates, cancelled).await;
        self.cancel.locked().take();
        done
    }

    async fn cancel(&self) {
        if let Some(cancel) = self.cancel.locked().take() {
            let _ = cancel.send(());
        }
    }
}

impl AgySession {
    /// Run a turn of `text`, starting agy again on the conversation if a cancel or failure ended it.
    async fn prompted(
        &self,
        text: &str,
        updates: &Updates,
        cancelled: oneshot::Receiver<()>,
    ) -> Result<PromptResponse, Error> {
        let mut slot = self.running.lock().await;
        if slot.is_none() {
            let (running, id) = run(&self.cli, &self.launch, &self.cwd, Some(&self.id)).await?;
            if id != self.id {
                return Err(failed(format!("agy did not resume conversation {}", self.id)));
            }
            *slot = Some(running);
        }
        let Some(running) = slot.as_mut() else { return Err(failed("agy is not running")) };
        let (done, keep) = self.turn(running, text, updates, cancelled).await;
        if !keep {
            // Ended or ending: started again on the conversation for the next prompt.
            slot.take();
        }
        done
    }

    /// Run a turn on `running`: its response, and whether the process serves the next one.
    async fn turn(
        &self,
        running: &mut Running,
        text: &str,
        updates: &Updates,
        mut cancelled: oneshot::Receiver<()>,
    ) -> (Result<PromptResponse, Error>, bool) {
        if cancelled.try_recv().is_ok() {
            return (Ok(PromptResponse::new(StopReason::Cancelled)), true);
        }
        let line = json!({ "event": "user", "message": { "content": text } });
        if running.writer.send(&line).await.is_err() {
            return (Err(running.process.exited().await), false);
        }
        let (mut steps, mut listening, mut interrupted) = (Steps::default(), true, false);
        loop {
            let event = tokio::select! {
                event = running.events.recv() => event,
                asked = &mut cancelled, if listening => {
                    listening = false;
                    if asked.is_ok() {
                        interrupted = true;
                        // agy ends the turn on SIGINT and exits; where it cannot be sent, it is stopped.
                        if !running.process.interrupt() {
                            running.process.stop(Duration::ZERO).await;
                        }
                    }
                    continue;
                }
            };
            let Some(event) = event else {
                if interrupted {
                    return (Ok(PromptResponse::new(StopReason::Cancelled).usage(steps.usage())), false);
                }
                return (Err(running.process.exited().await), false);
            };
            match event["event"].as_str() {
                Some("step_update") => steps.step(&event["step_update"], updates),
                Some("result") => return self.finish(running, &event["result"], &steps, interrupted).await,
                _ => tracing::debug!(harness = self.cli.name(), ?event, "an agy event not translated"),
            }
        }
    }

    /// The response to a turn that ended with `result`, and whether the process serves the next turn.
    async fn finish(
        &self,
        running: &mut Running,
        result: &Value,
        steps: &Steps,
        interrupted: bool,
    ) -> (Result<PromptResponse, Error>, bool) {
        // agy denies at once what its permission settings would ask a human about, and ends the turn.
        let denials = result["denied_actions"].as_array().map_or(&[][..], Vec::as_slice);
        for denied in denials.iter().skip(running.denied) {
            let (action, name) = (denied["action"].as_str(), denied["display_name"].as_str());
            deny_approval(&self.cli, &format!("{} ({})", action.unwrap_or("tool"), name.unwrap_or_default()));
        }
        running.denied = running.denied.max(denials.len());
        match outcome(result, steps, interrupted) {
            Outcome::Ended(stop) => (Ok(PromptResponse::new(stop).usage(steps.usage())), !interrupted),
            Outcome::Failed(error) => {
                // A failed turn ends agy, which says why on stderr too.
                let _ = running.process.exited().await;
                (Err(failure(&error, &running.process.stderr())), false)
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests;
