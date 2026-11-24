//! Antigravity CLI's headless stream-json (`protocol = "antigravity"`): `agy --input-format stream-json
//! --output-format stream-json`, one process per session. The process prints `init` with the conversation id when it
//! starts, then runs a turn for each `user` line it reads, streaming `step_update` events and ending the turn with one
//! `result`. A session resumes with `--conversation <id>`; agy cannot fork one.
//!
//! agy takes per-run settings only as one `--settings` (the last given wins), and MCP servers, hooks and skills only
//! from files, so each session gets a private directory holding its settings (the definition's own `--settings` merged
//! in) and, given to agy with `--add-dir`, `.agents/mcp_config.json` with ants' MCP servers, `.agents/hooks.json` with
//! the hook that steers the running turn ([`Steers`]) and `.agents/skills` with copies of the ant's skills; the
//! directory goes with the session.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use agent_client_protocol::v1::schema::{
    ContentBlock, PromptResponse, StopReason, ToolCall, ToolCallLocation, ToolCallStatus, ToolKind, Usage,
};
use agent_client_protocol::{Error, ErrorCode};
use anyhow::bail;
use serde_json::{Value, json};
use tokio::process::ChildStdin;
use tokio::sync::{mpsc, oneshot};

use super::io::{older, probe_version};
use super::turn::{Tokens, ToolCalls};
use super::{
    Cli, Credentials, Harness, Info, JsonLines, JsonWriter, Native, NativeSession, Open, Process, Updates,
    context_overflow, deny_approval, failed, mcp_servers_json, prompt_text, quota_exhausted, rate_limited, signed_out,
};
use crate::sync::MutexExt as _;

/// The environment variable that, `true` (not `1`), keeps agy from replacing itself with a newer release as it runs.
const AUTO_UPDATE: &str = "AGY_CLI_DISABLE_AUTO_UPDATE";

/// The oldest agy whose headless stream-json this speaks.
const OLDEST: [u64; 3] = [1, 5, 15];

/// agy's tool permission modes (its `toolPermission` setting), which `mode` names.
const MODES: &[&str] = &["request-review", "proceed-in-sandbox", "always-proceed", "strict"];

/// Flags of agy the protocol sets or that would change how it speaks, refused in a definition's arguments.
const OWN_FLAGS: &[&str] = &[
    "input-format",
    "output-format",
    "conversation",
    "resume",
    "r",
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

/// The error of a `result` agy prints instead of `init` when it is not signed in.
const NOT_SIGNED_IN: &str = "authentication failed or timed out";

/// The environment variable agy reads a Gemini API key from, used with model provider `gemini`.
const API_KEY: &str = "GEMINI_API_KEY";

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

    async fn start(cli: Cli) -> Result<Self, Error> {
        let cli = cli.with_env_default(AUTO_UPDATE, "true");
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
            // The hook taking steers in is a POSIX shell command.
            steer: cfg!(unix),
        }
    }

    /// The Gemini API key in the environment, where agy runs with it; a Google account's login, in the keychain, is
    /// not read.
    fn credentials(&self) -> Option<Credentials> {
        let key = self.cli.var(API_KEY)?;
        let home = self.cli.var("HOME").map(PathBuf::from).or_else(std::env::home_dir);
        gemini_provider(&self.cli, home.as_deref()).then(|| Credentials::new(API_KEY, &[key.as_bytes()]))
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
    let output = probe_version(cli).await?;
    let printed = String::from_utf8_lossy(&output.stdout);
    let found = printed.split_whitespace().find(|word| {
        let parts: Option<Vec<u64>> = word.split('.').map(|part| part.parse().ok()).collect();
        parts.is_some_and(|parts| parts.len() >= 2)
    });
    let Some(version) = found else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(failed(format!(
            "{}: agy --version printed no version: {} {}",
            cli.name(),
            printed.trim(),
            stderr.trim()
        )));
    };
    if older(version, OLDEST) {
        let [major, minor, patch] = OLDEST;
        return Err(failed(format!(
            "agy {version} is older than {major}.{minor}.{patch}, the first whose headless stream-json protocol \
             antigravity speaks; update agy (agy update)"
        )));
    }
    Ok(version.to_owned())
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

/// Whether agy's settings choose model provider `gemini`, which signs in with [`API_KEY`]: those of the definition's
/// `--settings` over agy's own `settings.json` in `home`. A settings file relative to the session's directory, not
/// read here, counts as choosing it.
fn gemini_provider(cli: &Cli, home: Option<&Path>) -> bool {
    let read = |path: &Path| std::fs::read_to_string(path).ok().and_then(|text| inline_settings(&text).ok());
    let given = match split_settings(cli.args()) {
        Ok((_, Some(inline))) if inline.trim_start().starts_with('{') => inline_settings(&inline).ok(),
        Ok((_, Some(file))) if Path::new(&file).is_relative() => return true,
        Ok((_, Some(file))) => read(Path::new(&file)),
        _ => None,
    };
    let own = || home.and_then(|home| read(&home.join(".gemini/antigravity-cli/settings.json")));
    let provider = |settings: serde_json::Map<String, Value>| settings.get("modelProvider").cloned();
    given.and_then(provider).or_else(|| own().and_then(provider)).is_some_and(|provider| provider == "gemini")
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

/// The command of agy's `PreInvocation` hook, run before each model call with the call's JSON on stdin: it moves the
/// steer file in `dir` of the call's conversation away, so the translator finds it gone ([`take_back`]), and prints
/// it, injecting its steps into the turn; else `{}`, injecting none.
#[cfg(unix)]
fn steer_hook(dir: &Path) -> String {
    let dir = dir.display().to_string().replace('\'', r"'\''");
    format!(
        r#"d='{dir}'; id=$(sed -n 's/.*"conversationId": *"\([0-9A-Za-z-]*\)".*/\1/p'); f="$d/steer-$id.json"; if [ -n "$id" ] && mv -f "$f" "$f.$$" 2>/dev/null; then cat "$f.$$"; rm -f "$f.$$"; else echo '{{}}'; fi"#
    )
}

/// Write `steers` to the steer file `file` for agy's hook to inject ([`steer_hook`]), whole or not at all.
fn write_steers(file: &Path, steers: &[String]) -> std::io::Result<()> {
    let steps: Vec<Value> = steers.iter().map(|text| json!({ "userMessage": text })).collect();
    let written = file.with_extension("json.new");
    std::fs::write(&written, json!({ "injectSteps": steps }).to_string())?;
    std::fs::rename(written, file)
}

/// Take the steer file `file` back from agy's hook, removing it: whether the hook had not taken it.
fn take_back(file: &Path) -> bool {
    let back = file.with_extension("json.back");
    let untaken = std::fs::rename(file, &back).is_ok();
    if untaken {
        let _ = std::fs::remove_file(back);
    }
    untaken
}

/// Copy directory `from` to `to`: its files and directories, not what its symbolic links point to.
fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let target = to.join(entry.file_name());
        if kind.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else if kind.is_file() {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// A directory only the daemon's user reads, removed when dropped: a session's settings and MCP servers, which carry
/// the ant's token, its hook, steers and skills.
struct Private(PathBuf);

impl Private {
    fn new(cli: &Cli) -> Result<Self, Error> {
        let dir = std::env::temp_dir().join(format!("ants-agy-{}", uuid::Uuid::new_v4().simple()));
        #[cfg_attr(not(unix), expect(unused_mut, reason = "only Unix sets a mode"))]
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

    /// The file agy's hook takes the steers of conversation `id` from.
    fn steer_file(&self, id: &str) -> PathBuf {
        self.0.join(format!("steer-{id}.json"))
    }

    /// Copy the skills of `skills`, each a directory holding a `SKILL.md`, to `.agents/skills` in it, replacing those
    /// an earlier start copied: agy loads no skill through a symbolic link. A copy failing leaves the skill to the
    /// brief.
    fn copy_skills(&self, cli: &Cli, skills: &Path) {
        let to = self.0.join(".agents/skills");
        if let Err(e) = std::fs::remove_dir_all(&to)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(harness = cli.name(), error = %e, dir = %to.display(), "removing agy's skills");
            return;
        }
        let entries = std::fs::read_dir(skills).into_iter().flatten().flatten();
        for skill in entries.filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir())) {
            if !skill.path().join("SKILL.md").is_file() {
                continue;
            }
            if let Err(e) = copy_dir(&skill.path(), &to.join(skill.file_name())) {
                tracing::warn!(harness = cli.name(), error = %e, skill = %skill.path().display(), "copying a skill for agy");
            }
        }
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
    steers: Mutex<Steers>,
    /// The ant's skills, copied for each start of agy.
    skills: Option<PathBuf>,
    private: Private,
}

/// The messages steering the prompt running ([`NativeSession::steer`]), in the session's steer file
/// ([`Private::steer_file`]), which agy's `PreInvocation` hook ([`steer_hook`]) takes before agy's next model call,
/// injecting them into the turn as user input. A turn ending before the hook took them runs them as a turn of its own,
/// the prompt ending after it.
#[derive(Debug, Default)]
struct Steers {
    /// Whether a prompt runs that takes steers: until it ends or is cancelled.
    open: bool,
    /// Those written to the steer file, unless the hook took them since.
    written: Vec<String>,
    /// Whether the last write of the steer file failed, leaving `written` out of it.
    unwritten: bool,
}

impl Steers {
    /// The steers agy has not taken, taken back from the steer file `file`.
    fn reclaim(&mut self, file: &Path) -> Vec<String> {
        let written = std::mem::take(&mut self.written);
        let unwritten = std::mem::take(&mut self.unwritten);
        if take_back(file) || unwritten { written } else { Vec::new() }
    }
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
        }
        #[cfg(unix)]
        {
            let hook = json!({ "type": "command", "command": steer_hook(&private.0), "timeout": 5 });
            private.write(cli, ".agents/hooks.json", &json!({ "ants": { "PreInvocation": [hook] } }))?;
        }
        let skills = open.skills.map(|skills| skills.dir);
        if let Some(skills) = &skills {
            private.copy_skills(cli, skills);
        }
        if cfg!(unix) || !open.mcp_servers.is_empty() || skills.is_some() {
            launch.extend(["--add-dir".to_owned(), private.0.display().to_string()]);
        }
        for (name, value) in [("--model", cli.model()), ("--effort", cli.effort())] {
            if let Some(value) = value {
                launch.extend([name.to_owned(), value.to_owned()]);
            }
        }
        let cli = cli.with_args(args);
        let (running, id) = run(&cli, &launch, &open.cwd, resume).await?;
        Ok(Self {
            id,
            cli,
            launch,
            cwd: open.cwd,
            running: tokio::sync::Mutex::new(Some(running)),
            cancel: Mutex::default(),
            steers: Mutex::default(),
            skills,
            private,
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
    let (mut process, stdin, stdout) = cli.spawn(&args, Some(cwd))?;
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
                        super::io::forward(&tx, event);
                    }
                });
                return Ok((Running { process, denied: 0, writer: JsonWriter::new(stdin), events, reader }, id));
            }
            // A failure to start: not signed in, an unknown model or agent.
            Some("result") => {
                // Waited on for all of its stderr, which the failure is classified by.
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
    /// The tokens its steps counted, and their total as agy counts it.
    tokens: Tokens,
    total: u64,
    /// The tool calls seen.
    tools: ToolCalls,
}

impl Steps {
    fn step(&mut self, step: &Value, updates: &Updates) {
        let count = |name: &str| step["usage"][name].as_u64().unwrap_or_default();
        self.tokens.add(Tokens {
            input: count("input_tokens"),
            output: count("output_tokens"),
            cache_read: count("cache_read_tokens"),
            cache_write: 0,
            thought: count("thinking_tokens"),
        });
        self.total += count("total_tokens");
        match step["step_type"].as_str() {
            Some("agent_response") => {
                if let Some(text) = step["text_delta"].as_str().filter(|text| !text.is_empty()) {
                    updates.message(text);
                }
            }
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
        self.tools.report(updates, described(id, step), status, output);
    }

    fn usage(&self) -> Usage {
        self.tokens.usage(self.total)
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

/// How a turn ended, by its `result`, `interrupted` telling whether it was cancelled.
fn outcome(result: &Value, interrupted: bool) -> Outcome {
    let error = result["error"].as_str().unwrap_or_default();
    if result["finished_reason"] == "interrupted" {
        if interrupted {
            return Outcome::Ended(StopReason::Cancelled);
        }
        return Outcome::Failed(format!("agy was interrupted: {error}"));
    }
    if result["status"] == "SUCCESS" {
        return Outcome::Ended(StopReason::EndTurn);
    }
    Outcome::Failed(error.to_owned())
}

/// The user line of a turn of `text`.
fn user(text: &str) -> Value {
    json!({ "event": "user", "message": { "content": text } })
}

impl NativeSession for AgySession {
    fn id(&self) -> String {
        self.id.clone()
    }

    async fn prompt(&self, prompt: Vec<ContentBlock>, updates: Updates) -> Result<PromptResponse, Error> {
        let (cancel, cancelled) = oneshot::channel();
        *self.cancel.locked() = Some(cancel);
        self.steers.locked().open = true;
        let _ended = Ended(self);
        self.prompted(&prompt_text(&prompt), &updates, cancelled).await
    }

    async fn steer(&self, prompt: Vec<ContentBlock>) -> Result<bool, Error> {
        let mut steers = self.steers.locked();
        if !steers.open {
            return Ok(false);
        }
        let file = self.private.steer_file(&self.id);
        let mut written = steers.reclaim(&file);
        written.push(prompt_text(&prompt));
        if let Err(e) = write_steers(&file, &written) {
            tracing::warn!(harness = self.cli.name(), error = %e, file = %file.display(), "writing a steer for agy");
            // Refused, it runs after the prompt; those before it run as a turn of the prompt.
            written.pop();
            *steers = Steers { open: false, unwritten: !written.is_empty(), written };
            return Ok(false);
        }
        steers.written = written;
        Ok(true)
    }

    async fn cancel(&self) {
        self.steers.locked().open = false;
        if let Some(cancel) = self.cancel.locked().take() {
            let _ = cancel.send(());
        }
    }
}

/// Ends the session's prompt however it ends: it takes no more steers, and those agy did not take are dropped.
struct Ended<'a>(&'a AgySession);

impl Drop for Ended<'_> {
    fn drop(&mut self) {
        self.0.cancel.locked().take();
        *self.0.steers.locked() = Steers::default();
        take_back(&self.0.private.steer_file(&self.0.id));
    }
}

impl AgySession {
    /// Run a turn of `text`, starting agy again on the conversation, with the skills as they are now, if a cancel or
    /// failure ended it.
    async fn prompted(
        &self,
        text: &str,
        updates: &Updates,
        cancelled: oneshot::Receiver<()>,
    ) -> Result<PromptResponse, Error> {
        let mut slot = self.running.lock().await;
        if slot.is_none() {
            if let Some(skills) = &self.skills {
                self.private.copy_skills(&self.cli, skills);
            }
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
        if running.writer.send(&user(text)).await.is_err() {
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
            let result = match event["event"].as_str() {
                Some("step_update") => {
                    steps.step(&event["step_update"], updates);
                    continue;
                }
                Some("result") => &event["result"],
                _ => {
                    tracing::debug!(harness = self.cli.name(), ?event, "an agy event not translated");
                    continue;
                }
            };
            self.denials(running, result);
            let stop = match outcome(result, interrupted) {
                Outcome::Ended(stop) => stop,
                Outcome::Failed(error) => {
                    // A failed turn ends agy, which says why on stderr too.
                    let _ = running.process.exited().await;
                    return (Err(failure(&error, &running.process.stderr())), false);
                }
            };
            let untaken = if interrupted { Vec::new() } else { self.untaken() };
            if untaken.is_empty() {
                return (Ok(PromptResponse::new(stop).usage(steps.usage())), !interrupted);
            }
            if running.writer.send(&user(&untaken.join("\n\n"))).await.is_err() {
                return (Err(running.process.exited().await), false);
            }
        }
    }

    /// Warn of the actions agy denied by `result`: it denies at once what its permission settings would ask a human
    /// about, and ends the turn.
    fn denials(&self, running: &mut Running, result: &Value) {
        let denials = result["denied_actions"].as_array().map_or(&[][..], Vec::as_slice);
        for denied in denials.iter().skip(running.denied) {
            let (action, name) = (denied["action"].as_str(), denied["display_name"].as_str());
            deny_approval(&self.cli, &format!("{} ({})", action.unwrap_or("tool"), name.unwrap_or_default()));
        }
        running.denied = running.denied.max(denials.len());
    }

    /// At the end of a turn, the steers agy did not take into it, which then run as a turn of their own; with none,
    /// the prompt takes no more.
    fn untaken(&self) -> Vec<String> {
        let mut steers = self.steers.locked();
        let untaken = steers.reclaim(&self.private.steer_file(&self.id));
        if untaken.is_empty() {
            steers.open = false;
        }
        untaken
    }
}

#[cfg(all(test, unix))]
mod tests;
