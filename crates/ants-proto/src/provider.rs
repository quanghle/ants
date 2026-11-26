//! The provider protocol: how ants talks to the system holding an ant's identity, memories and work, its provider.
//!
//! An external provider is a process speaking JSON-RPC 2.0 ([`jsonrpc`]) over its standard input and output, one
//! message per line: each line is one JSON object, UTF-8, without a newline inside it (the compact form of
//! `serde_json` never has one), and no batches. The provider writes its logs to its standard error. Built-in sense
//! implements the same [`Provider`] interface in process, so switching an ant between providers changes nothing else.
//!
//! ants opens with [`method::INITIALIZE`]: it offers the protocol versions it speaks and the [`Capabilities`] it
//! uses, the provider answers with the version it chose, its name and its own capabilities, and both sides then use
//! only the capabilities both declared ([`negotiate`]). Each other method belongs to one capability:
//!
//! | Method | Direction | Capability | Params | Result |
//! |---|---|---|---|---|
//! | [`method::INITIALIZE`] | ants → provider | | [`InitializeParams`] | [`InitializeResult`] |
//! | [`method::TOOLS_LIST`] | ants → provider | tools | none | [`ToolList`] |
//! | [`method::TOOLS_CALL`] | ants → provider | tools | [`ToolCall`] | [`ToolResult`] |
//! | [`method::BRIEF`] | ants → provider | context | [`BriefRequest`] | [`Brief`] |
//! | [`method::EVENT`] | ants → provider | events | [`Event`] | [`Ack`] |
//! | [`method::MEMORIES`] | ants → provider | memories | [`AntRef`] | [`MemoriesView`] |
//! | [`method::EXPORT`] | ants → provider | export | [`AntRef`] | [`Export`] |
//! | [`method::IMPORT`] | ants → provider | export | [`ImportRequest`] | [`Ack`] |
//! | [`method::WAKE`] | provider → ants | wake | [`WakeRequest`] | [`Woken`] |
//!
//! A provider never reads an ant's messages, prompts or replies: it sees only its own tool calls, the lifecycle
//! events, and what the ant passes it.

use std::time::Duration;

mod calls;
mod handshake;
mod interface;
pub mod jsonrpc;

pub use calls::{
    Ack, AntRef, Brief, BriefReason, BriefRequest, CallContext, Event, EventKind, Export, ImportRequest, MemoriesView,
    MemoryEntry, MemoryFields, Origin, Output, Rebuilt, Starter, Tool, ToolCall, ToolList, ToolResult, TriggerRow,
    WakeRequest, Woken, normalize_remote, remote_url,
};
pub use handshake::{
    Agreed, Capabilities, Capability, Info, InitializeParams, InitializeResult, NegotiationError, choose_version,
    negotiate,
};
pub use interface::{BoxFuture, Connected, Host, Provider};
pub use jsonrpc::RpcError;

/// The provider protocol version this build speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// The byte budget of a brief unless the config sets another.
pub const BRIEF_BUDGET: u32 = 8_000;

/// How long ants waits for a brief before leaving it out.
pub const BRIEF_TIMEOUT: Duration = Duration::from_secs(5);

/// How long ants remembers a wake request's id, a repeat within it waking the ant once.
pub const WAKE_DEDUP: Duration = Duration::from_hours(7 * 24);

/// The most bytes the text of a wake request may have, as a webhook's body: well within a bus message.
pub const MAX_WAKE_TEXT: usize = 256 * 1024;

/// The argument ants adds to every provider tool it serves: `true` asks for the answer as JSON, else it comes as
/// brief text ([`take_format`]).
pub const JSON_ARGUMENT: &str = "json";

/// The JSON-RPC method names of the protocol.
pub mod method {
    pub const INITIALIZE: &str = "initialize";
    pub const TOOLS_LIST: &str = "tools/list";
    pub const TOOLS_CALL: &str = "tools/call";
    pub const BRIEF: &str = "brief";
    pub const EVENT: &str = "event";
    pub const MEMORIES: &str = "memories";
    pub const EXPORT: &str = "export";
    pub const IMPORT: &str = "import";
    pub const WAKE: &str = "wake";
}

/// The answer format a tool call asks for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    /// Brief text, for the ant to read.
    #[default]
    Text,
    Json,
}

/// Whether `name` can name a provider, the prefix of its tools' names: 1-32 of `a-z`, `0-9` and `-`, starting with a
/// letter.
pub fn valid_provider_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && name.starts_with(|c: char| c.is_ascii_lowercase())
}

/// Whether `name` can name a provider's tool: 1-32 of `a-z`, `0-9` and `_`, starting with a letter.
pub fn valid_tool_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && name.starts_with(|c: char| c.is_ascii_lowercase())
}

/// The name ants serves `tool` of `provider` under: the provider's name, `_`, the tool's (`sense_recall`).
pub fn served_name(provider: &str, tool: &str) -> String {
    format!("{provider}_{tool}")
}

/// `schema`, a tool's input schema, with the [`JSON_ARGUMENT`] ants adds to every tool it serves.
pub fn with_json_argument(mut schema: serde_json::Value) -> serde_json::Value {
    if let Some(object) = schema.as_object_mut() {
        object.entry("type").or_insert_with(|| "object".into());
        let properties = object.entry("properties").or_insert_with(|| serde_json::Value::Object(Default::default()));
        if let Some(properties) = properties.as_object_mut() {
            properties.insert(
                JSON_ARGUMENT.to_owned(),
                serde_json::json!({"type": "boolean", "description": "Answer as JSON instead of brief text."}),
            );
        }
    }
    schema
}

/// The format the [`JSON_ARGUMENT`] of a served tool's `arguments` asks for, taking the argument out of them.
pub fn take_format(arguments: &mut serde_json::Map<String, serde_json::Value>) -> Format {
    match arguments.remove(JSON_ARGUMENT) {
        Some(serde_json::Value::Bool(true)) => Format::Json,
        _ => Format::Text,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn names_are_checked_and_prefixed() {
        assert!(valid_provider_name("sense") && valid_provider_name("my-notes2"));
        assert!(!valid_provider_name("") && !valid_provider_name("Sense") && !valid_provider_name("2x"));
        assert!(!valid_provider_name("a_b") && !valid_provider_name(&"a".repeat(33)));
        assert!(valid_tool_name("recall") && valid_tool_name("check_point2"));
        assert!(!valid_tool_name("_x") && !valid_tool_name("re-call") && !valid_tool_name(""));
        assert_eq!(served_name("sense", "recall"), "sense_recall");
    }

    #[test]
    fn the_json_argument_is_added_and_taken() {
        let schema = with_json_argument(json!({"type": "object", "properties": {"q": {"type": "string"}}}));
        assert_eq!(schema["properties"]["q"], json!({"type": "string"}));
        assert_eq!(schema["properties"]["json"]["type"], "boolean");
        assert_eq!(with_json_argument(json!({}))["type"], "object");

        let mut arguments = json!({"q": "x", "json": true}).as_object().unwrap().clone();
        assert_eq!(take_format(&mut arguments), Format::Json);
        assert_eq!(arguments, *json!({"q": "x"}).as_object().unwrap());
        assert_eq!(take_format(&mut arguments), Format::Text);
        let mut not_json = json!({"json": "yes"}).as_object().unwrap().clone();
        assert_eq!(take_format(&mut not_json), Format::Text);
        assert!(not_json.is_empty());
    }
}
