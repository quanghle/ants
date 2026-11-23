//! The events of a Claude Code turn as session updates: the reply and thoughts, tool calls with their titles and
//! kinds, todo lists as the plan, and the tokens and context window its result counts.

use std::collections::HashSet;
use std::path::Path;

use agent_client_protocol::v1::schema::{
    Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus, ToolCall, ToolCallLocation, ToolCallStatus, ToolCallUpdate,
    ToolCallUpdateFields, ToolKind,
};
use serde_json::Value;

use crate::harness::native::turn::{Reply, Tokens};
use crate::harness::native::{Cli, Updates, deny_approval};

/// One turn's events, as they stream to the daemon.
pub(super) struct Turn<'a> {
    cli: &'a Cli,
    cwd: &'a Path,
    updates: &'a Updates,
    /// The model of the turn, from its `init` event.
    pub(super) model: Option<String>,
    /// The reply: messages streamed as deltas, by id, are not sent again whole.
    reply: Reply,
    /// TodoWrite calls, sent as the plan rather than as tool calls.
    todos: HashSet<String>,
    /// Tokens in the context window as of the turn's last model call.
    pub(super) used: u64,
    /// The kind of error of the turn's last assistant message, such as `rate_limit`.
    pub(super) error: Option<String>,
    /// Claude Code's own message of the turn, such as an error's.
    pub(super) synthetic: Option<String>,
    /// The last rate limit state reported.
    pub(super) limit: Option<Value>,
    /// The results so far: a steer's turn follows one, its reply then parted from what came before by a blank line.
    pub(super) part: u64,
}

impl<'a> Turn<'a> {
    pub(super) fn new(cli: &'a Cli, cwd: &'a Path, updates: &'a Updates) -> Self {
        Self {
            cli,
            cwd,
            updates,
            model: None,
            reply: Reply::default(),
            todos: HashSet::new(),
            used: 0,
            error: None,
            synthetic: None,
            limit: None,
            part: 0,
        }
    }

    /// Take an event of the CLI; the turn's result when it ends it.
    pub(super) fn event(&mut self, event: Value) -> Option<Value> {
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
                    self.reply.streams(id);
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
        let streamed = message["id"].as_str().is_some_and(|id| self.reply.streamed(id));
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
        self.reply.say(self.updates, &self.part.to_string(), text);
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
pub(super) fn describe(name: &str, input: &Value, cwd: &Path) -> (ToolKind, String) {
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
pub(super) fn window(result: &Value, model: Option<&str>) -> Option<u64> {
    let usage = result["modelUsage"].as_object()?;
    let own = model.and_then(|model| usage.get(model)).and_then(|usage| usage["contextWindow"].as_u64());
    own.or_else(|| usage.values().filter_map(|usage| usage["contextWindow"].as_u64()).max())
}

/// The tokens a turn used, every model's: what changed since the last result, else the turn's main calls.
pub(super) fn token_counts(result: &Value) -> Tokens {
    let per_model = result["model_usage_since_last_result"].as_object().filter(|models| !models.is_empty());
    let [input, output, cache_read, cache_write, thought] = match per_model {
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
    Tokens { input, output, cache_read, cache_write, thought }
}
