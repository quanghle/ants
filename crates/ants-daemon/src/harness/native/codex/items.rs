//! A codex turn's items and notifications as session updates: the reply and reasoning, tool calls with their titles,
//! kinds and outcomes, the plan and the tokens the thread counts.

use std::path::Path;

use agent_client_protocol::v1::schema::{
    ContentBlock, Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus, ToolCall, ToolCallLocation, ToolCallStatus,
    ToolKind, Usage,
};
use serde_json::{Value, json};

use crate::harness::native::Updates;
use crate::harness::native::turn::{self, Reply, ToolCalls};

/// A prompt's blocks as the `input` of `turn/start`: texts, and images as data URLs.
pub(super) fn input(prompt: &[ContentBlock]) -> Vec<Value> {
    let text = |text: &str| json!({ "type": "text", "text": text, "text_elements": [] });
    prompt
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(t) => Some(text(&t.text)),
            ContentBlock::Image(image) if image.data.is_empty() => {
                image.uri.as_ref().map(|uri| json!({ "type": "image", "url": uri }))
            }
            ContentBlock::Image(image) => {
                Some(json!({ "type": "image", "url": format!("data:{};base64,{}", image.mime_type, image.data) }))
            }
            ContentBlock::ResourceLink(link) => Some(text(&link.uri)),
            _ => None,
        })
        .collect()
}

/// Token counts of a thread, as `thread/tokenUsage/updated` breaks them down, with the total the server counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Tokens {
    total: u64,
    counts: turn::Tokens,
}

impl Tokens {
    fn of(breakdown: &Value) -> Self {
        let count = |key: &str| breakdown[key].as_u64().unwrap_or_default();
        let counts = turn::Tokens {
            input: count("inputTokens"),
            output: count("outputTokens"),
            cache_read: count("cachedInputTokens"),
            cache_write: 0,
            thought: count("reasoningOutputTokens"),
        };
        Self { total: count("totalTokens"), counts }
    }

    fn minus(self, before: Self) -> Self {
        Self { total: self.total.saturating_sub(before.total), counts: self.counts.minus(before.counts) }
    }
}

/// A turn running, turning the notifications of its thread into updates.
pub(super) struct Turn {
    id: String,
    updates: Updates,
    /// The reply and reasoning: an agent message or reasoning item whose text came as deltas is not sent again.
    reply: Reply,
    /// The tool calls announced.
    tools: ToolCalls,
    /// The thread's token totals before the turn, and the latest.
    before: Option<Tokens>,
    pub(super) latest: Option<Tokens>,
}

impl Turn {
    pub(super) fn new(id: &str, updates: Updates, before: Option<Tokens>) -> Self {
        Self { id: id.to_owned(), updates, reply: Reply::default(), tools: ToolCalls::default(), before, latest: None }
    }

    /// Handle a notification of the thread: the turn, once `turn/completed` ends it.
    pub(super) fn handle(&mut self, method: &str, mut params: Value) -> Option<Value> {
        if params["turnId"].as_str().is_some_and(|turn| turn != self.id) {
            return None;
        }
        let item = params["itemId"].as_str().unwrap_or_default();
        let delta = params["delta"].as_str().unwrap_or_default();
        match method {
            // An empty piece does not count as streaming: the message is then said whole.
            "item/agentMessage/delta" if !delta.is_empty() => self.reply.delta(&self.updates, item, delta),
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                self.reply.thought_delta(&self.updates, item, delta);
            }
            "item/reasoning/summaryPartAdded" if params["summaryIndex"].as_u64().is_some_and(|i| i > 0) => {
                self.updates.thought("\n\n");
            }
            "item/started" => self.started(&params["item"]),
            "item/completed" => self.completed(&params["item"]),
            "turn/plan/updated" => self.updates.plan(plan(&params["plan"])),
            "thread/tokenUsage/updated" => self.used(&params["tokenUsage"]),
            "error" => tracing::debug!(error = %params["error"], retrying = %params["willRetry"], "codex"),
            "model/rerouted" => tracing::info!(from = %params["fromModel"], to = %params["toModel"], "codex rerouted"),
            "turn/completed" if params["turn"]["id"] == self.id.as_str() => return Some(params["turn"].take()),
            _ => {}
        }
        None
    }

    fn started(&mut self, item: &Value) {
        if let Some(call) = tool_call(item) {
            self.tools.started(&self.updates, call.status(ToolCallStatus::InProgress));
        }
    }

    fn completed(&mut self, item: &Value) {
        let id = item["id"].as_str().unwrap_or_default();
        match item["type"].as_str() {
            Some("agentMessage") => {
                let text = item["text"].as_str().unwrap_or_default();
                // Said once, whatever came of it before or comes after.
                if !text.is_empty() && !self.reply.streamed(id) {
                    self.reply.delta(&self.updates, id, text);
                }
            }
            Some("reasoning") => {
                let parts = item["summary"].as_array().filter(|parts| !parts.is_empty()).or(item["content"].as_array());
                let text: Vec<&str> = parts.into_iter().flatten().filter_map(Value::as_str).collect();
                if !text.is_empty() {
                    self.reply.whole_thought(&self.updates, id, text.join("\n\n"));
                }
            }
            _ => {
                let Some(call) = tool_call(item) else { return };
                let (status, output) = outcome(item);
                self.tools.report(&self.updates, call, status, output);
            }
        }
    }

    fn used(&mut self, usage: &Value) {
        let (total, last) = (Tokens::of(&usage["total"]), Tokens::of(&usage["last"]));
        self.before.get_or_insert_with(|| total.minus(last));
        self.latest = Some(total);
        if let Some(window) = usage["modelContextWindow"].as_u64() {
            self.updates.usage(last.total, window, None);
        }
    }

    /// The tokens the turn used, their total as the server counts it.
    pub(super) fn usage(&self) -> Option<Usage> {
        let used = self.latest?.minus(self.before.unwrap_or_default());
        Some(used.counts.usage(used.total))
    }
}

fn plan(steps: &Value) -> Plan {
    let entries = steps.as_array().into_iter().flatten().map(|step| {
        let status = match step["status"].as_str() {
            Some("completed") => PlanEntryStatus::Completed,
            Some("inProgress") => PlanEntryStatus::InProgress,
            _ => PlanEntryStatus::Pending,
        };
        PlanEntry::new(step["step"].as_str().unwrap_or_default(), PlanEntryPriority::Medium, status)
    });
    Plan::new(entries.collect())
}

/// A path of an item, relative ones taken from `cwd`.
fn location(cwd: Option<&str>, path: &str) -> ToolCallLocation {
    match cwd {
        Some(cwd) => ToolCallLocation::new(Path::new(cwd).join(path)),
        None => ToolCallLocation::new(path),
    }
}

/// The tool call an item of the thread is, if it is one: commands, file changes, MCP and dynamic tool calls, web
/// searches, images and sub-agents.
fn tool_call(item: &Value) -> Option<ToolCall> {
    let id = item["id"].as_str().unwrap_or_default().to_owned();
    let text = |key: &str| item[key].as_str().unwrap_or_default();
    let call = match item["type"].as_str()? {
        "commandExecution" => {
            let (kind, title, locations) = command(item);
            let input = json!({ "command": item["command"], "cwd": item["cwd"] });
            ToolCall::new(id, title).kind(kind).locations(locations).raw_input(input)
        }
        "fileChange" => {
            let changes = item["changes"].as_array().map_or_default(Vec::as_slice);
            let mut paths: Vec<&str> = changes.iter().filter_map(|change| change["path"].as_str()).collect();
            let title = if paths.is_empty() { "Edit".to_owned() } else { format!("Edit {}", paths.join(", ")) };
            paths.extend(changes.iter().filter_map(|change| change["kind"]["move_path"].as_str()));
            let locations = paths.into_iter().map(ToolCallLocation::new).collect();
            ToolCall::new(id, title).kind(ToolKind::Edit).locations(locations)
        }
        "mcpToolCall" => {
            ToolCall::new(id, format!("Tool: {}/{}", text("server"), text("tool"))).raw_input(item["arguments"].clone())
        }
        "dynamicToolCall" => {
            let name = item["namespace"]
                .as_str()
                .map_or_else(|| text("tool").to_owned(), |ns| format!("{ns}/{}", text("tool")));
            ToolCall::new(id, format!("Tool: {name}")).raw_input(item["arguments"].clone())
        }
        "webSearch" => {
            let action = &item["action"];
            let title = match action["type"].as_str() {
                Some("openPage") => format!("Open {}", action["url"].as_str().unwrap_or_default()),
                Some("findInPage") => format!(
                    "Find {} in {}",
                    action["pattern"].as_str().unwrap_or_default(),
                    action["url"].as_str().unwrap_or_default()
                ),
                _ if !text("query").is_empty() => format!("Search the web for {}", text("query")),
                _ => "Search the web".to_owned(),
            };
            ToolCall::new(id, title).kind(ToolKind::Fetch)
        }
        "imageView" => ToolCall::new(id, format!("View {}", text("path")))
            .kind(ToolKind::Read)
            .locations(vec![ToolCallLocation::new(text("path"))]),
        "imageGeneration" => ToolCall::new(id, "Image generation"),
        "collabAgentToolCall" => ToolCall::new(id, format!("Agent: {}", text("tool"))),
        _ => return None,
    };
    Some(call)
}

/// The kind, title and paths of a command, from the actions codex parsed it into: reads, listings and searches, else
/// the command itself.
fn command(item: &Value) -> (ToolKind, String, Vec<ToolCallLocation>) {
    let command = item["command"].as_str().unwrap_or_default();
    let cwd = item["cwd"].as_str();
    let (mut kind, mut titles, mut locations) = (ToolKind::Execute, Vec::new(), Vec::new());
    for action in item["commandActions"].as_array().into_iter().flatten() {
        let path = action["path"].as_str();
        let (this, title) = match action["type"].as_str() {
            Some("read") => {
                locations.extend(path.map(|path| location(cwd, path)));
                (ToolKind::Read, format!("Read {}", action["name"].as_str().or(path).unwrap_or(command)))
            }
            Some("listFiles") => (ToolKind::Search, format!("List {}", path.unwrap_or("."))),
            Some("search") => {
                let title = match (action["query"].as_str(), path) {
                    (Some(query), Some(path)) => format!("Search {query} in {path}"),
                    (Some(query), None) => format!("Search {query}"),
                    _ => format!("Search {}", action["command"].as_str().unwrap_or(command)),
                };
                (ToolKind::Search, title)
            }
            _ => (ToolKind::Execute, action["command"].as_str().unwrap_or(command).to_owned()),
        };
        kind = this;
        titles.push(title);
    }
    if titles.is_empty() {
        titles.push(command.to_owned());
    }
    (kind, titles.join(", "), locations)
}

/// How a completed tool item ended, and what it gave back worth reporting.
fn outcome(item: &Value) -> (ToolCallStatus, Option<Value>) {
    let failed = match item["status"].as_str() {
        Some("failed" | "declined") => true,
        _ => !item["error"].is_null() || item["result"]["isError"] == true || item["success"] == false,
    };
    let status = if failed { ToolCallStatus::Failed } else { ToolCallStatus::Completed };
    let output = match item["type"].as_str() {
        Some("commandExecution") => Some(json!({ "exitCode": item["exitCode"], "status": item["status"] })),
        Some("mcpToolCall") if !item["error"].is_null() => Some(json!({ "error": item["error"] })),
        _ => None,
    };
    (status, output)
}
