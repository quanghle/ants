//! An MCP server over its stdio as a provider of tools alone: initialize, then `tools/list` and `tools/call`, each tool
//! served under a name ants takes ([`tool_name`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ants_proto::provider::jsonrpc::{Request, Response};
use ants_proto::provider::{
    BoxFuture, Capabilities, Format, Host, Info, InitializeParams, InitializeResult, Output, PROTOCOL_VERSION,
    Provider, RpcError, Tool, ToolCall, ToolList, ToolResult, valid_tool_name,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncWrite};

use super::external::{CALL_TIMEOUT, INITIALIZE_TIMEOUT};
use super::link::{Link, Serve};
use crate::sync::MutexExt as _;

/// The MCP version ants asks for.
const MCP_VERSION: &str = "2025-06-18";

/// The most pages of tools read.
const MAX_PAGES: usize = 100;

/// An MCP server named `name`, spoken to over its output `reader` and input `writer`, and the link to it.
pub(super) fn client(
    name: &str,
    reader: impl AsyncRead + Send + Unpin + 'static,
    writer: impl AsyncWrite + Send + Unpin + 'static,
) -> (Arc<dyn Provider>, Arc<Link>) {
    let serve: Serve = Arc::new(|request: Request| {
        let result = match request.method.as_str() {
            "ping" => Ok(json!({})),
            other => Err(RpcError::method_not_found(other)),
        };
        Box::pin(std::future::ready(Response::new(request.id, result)))
    });
    let link = Arc::new(Link::new(name, reader, writer, serve));
    let server = McpServer { name: name.to_owned(), link: link.clone(), names: Mutex::default() };
    (Arc::new(server), link)
}

struct McpServer {
    name: String,
    link: Arc<Link>,
    /// The server's name of each tool, by the name ants serves it under.
    names: Mutex<HashMap<String, String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Initialized {
    #[serde(default)]
    capabilities: Map<String, Value>,
    server_info: Option<ServerInfo>,
}

#[derive(Deserialize)]
struct ServerInfo {
    version: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    #[serde(default)]
    tools: Vec<McpTool>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpTool {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default = "object")]
    input_schema: Value,
}

fn object() -> Value {
    json!({"type": "object"})
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Called {
    #[serde(default)]
    content: Vec<Value>,
    structured_content: Option<Value>,
    #[serde(default)]
    is_error: bool,
}

impl Provider for McpServer {
    fn initialize(
        &self,
        params: InitializeParams,
        _host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
        Box::pin(async move {
            let client = &params.client;
            let ours = json!({
                "protocolVersion": MCP_VERSION,
                "capabilities": {},
                "clientInfo": {"name": client.name, "version": client.version},
            });
            let initialized: Initialized = self.link.request("initialize", &ours, INITIALIZE_TIMEOUT).await?;
            self.link.notify("notifications/initialized", &());
            let tools = initialized.capabilities.contains_key("tools");
            let capabilities = Capabilities { tools, ..Capabilities::NONE };
            let version = initialized.server_info.and_then(|info| info.version).unwrap_or_default();
            params.accept(&[PROTOCOL_VERSION], Info { name: self.name.clone(), version }, capabilities)
        })
    }

    fn list_tools(&self) -> BoxFuture<'_, Result<ToolList, RpcError>> {
        Box::pin(async move {
            let mut listed = Vec::new();
            let mut cursor = None;
            for _ in 0..MAX_PAGES {
                let params = match &cursor {
                    Some(cursor) => json!({"cursor": cursor}),
                    None => Value::Null,
                };
                let page: Page = self.link.request("tools/list", &params, CALL_TIMEOUT).await?;
                listed.extend(page.tools);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            let mut names = HashMap::new();
            let mut tools = Vec::new();
            for tool in listed {
                let Some(name) = tool_name(&tool.name).filter(|name| !names.contains_key(name)) else {
                    tracing::warn!(
                        provider = self.name,
                        tool = tool.name,
                        "not serving a tool whose name ants cannot take"
                    );
                    continue;
                };
                names.insert(name.clone(), tool.name);
                tools.push(Tool { name, description: tool.description, input_schema: tool.input_schema });
            }
            *self.names.locked() = names;
            Ok(ToolList { tools })
        })
    }

    fn call_tool(&self, call: ToolCall) -> BoxFuture<'_, Result<ToolResult, RpcError>> {
        Box::pin(async move {
            if self.names.locked().is_empty() {
                self.list_tools().await?;
            }
            let name = self.names.locked().get(&call.name).cloned();
            let Some(name) = name else {
                return Err(RpcError::invalid_params(&format!("no tool {} of {}", call.name, self.name)));
            };
            let params = json!({"name": name, "arguments": call.arguments});
            let called: Called = self.link.request("tools/call", &params, CALL_TIMEOUT).await?;
            Ok(result(called, call.format))
        })
    }
}

/// The name ants serves the MCP tool `name` under: lowercase, a character other than a letter, a digit or `_` as `_`;
/// none when that is no tool name ([`valid_tool_name`]).
fn tool_name(name: &str) -> Option<String> {
    let name: String = name
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| if c.is_ascii_lowercase() || c.is_ascii_digit() { c } else { '_' })
        .collect();
    valid_tool_name(&name).then_some(name)
}

/// The result of a call that answered `called`, asked for in `format`: its structured content when JSON was asked for
/// and it has some, else the text of its content.
fn result(called: Called, format: Format) -> ToolResult {
    let is_error = called.is_error;
    if let (Format::Json, Some(json)) = (format, called.structured_content.clone()) {
        return ToolResult { output: Output::Json(json), is_error };
    }
    let mut parts: Vec<String> = called
        .content
        .iter()
        .map(|block| match block.get("type").and_then(Value::as_str) {
            Some("text") => block.get("text").and_then(Value::as_str).unwrap_or_default().to_owned(),
            Some(kind) => format!("[{kind} content]"),
            None => "[content]".to_owned(),
        })
        .collect();
    if parts.is_empty()
        && let Some(json) = called.structured_content
    {
        parts.push(json.to_string());
    }
    ToolResult { output: Output::Text(parts.join("\n")), is_error }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_names_are_taken_as_ants_takes_them() {
        assert_eq!(tool_name("search").as_deref(), Some("search"));
        assert_eq!(tool_name("Get-Issue.v2").as_deref(), Some("get_issue_v2"));
        assert_eq!(tool_name("2fa"), None);
        assert_eq!(tool_name(&"x".repeat(33)), None);
    }

    #[test]
    fn results_are_text_unless_json_is_asked_for_and_given() {
        let called = || Called {
            content: vec![json!({"type": "text", "text": "one"}), json!({"type": "image", "data": "…"})],
            structured_content: Some(json!({"n": 1})),
            is_error: true,
        };
        let text = result(called(), Format::Text);
        assert_eq!(text, ToolResult { output: Output::Text("one\n[image content]".into()), is_error: true });
        assert_eq!(result(called(), Format::Json).output, Output::Json(json!({"n": 1})));
        let bare = Called { content: Vec::new(), structured_content: Some(json!([1])), is_error: false };
        assert_eq!(result(bare, Format::Text).output, Output::Text("[1]".into()));
    }
}
