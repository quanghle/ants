//! The tools of the ant's provider ([`crate::provider`]), served beside ants' own under the provider's name as prefix
//! ([`served_name`]), each with the `json` argument ([`with_json_argument`]), so no harness needs configuring for them.
//! Each call carries who calls, where and when as ants knows it ([`CallContext`]), never as the ant's text claims: the
//! ant and its identity record's id ([`Callers`]), the thread and where it came from (its [`origin`], kept per thread
//! as [`settled`]), the harness and model running the session, the machine serving the ant, and the time; a brief
//! request takes the same ([`Mcp::caller`], [`Mcp::origin`]).
//!
//! The provider's list is asked for at each `tools/list`, so a provider going down or coming back shows in the lists
//! taken after it: the HTTP server keeps no session to tell a harness of a change. A provider whose list fails or takes
//! over [`LIST_TIMEOUT`] serves no tools meanwhile, ants' own still served; a tool whose name is not valid or would
//! take one of ants' own is left out, logged once. A call the provider does not answer within [`CALL_TIMEOUT`] fails.

use std::collections::HashSet;
use std::time::Duration;

use ants_proto::provider::{
    CallContext, Capability, Connected, Origin, Output, ToolCall, ToolResult, served_name, take_format,
    valid_tool_name, with_json_argument,
};
use ants_proto::{ChannelId, Envelope, ThreadId};
use rmcp::model::{CallToolResult, ContentBlock, JsonObject};

use super::tools::{Tools, own_names, text};
use super::{Hub, Mcp, Sessions};
use crate::provider::context::{CANNOT_READ, Caller, Callers, origin, settled};
use crate::sync::MutexExt as _;

/// How long a `tools/list` waits for the provider's tools before listing ants' own alone.
pub const LIST_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a call of a provider's tool waits for its answer.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// What the hub keeps of the provider's tools.
#[derive(Default)]
pub(super) struct Served {
    /// Those listed last, as served.
    tools: Vec<rmcp::model::Tool>,
    /// The names of the provider's tools left out, each logged once.
    refused: HashSet<String>,
}

impl Mcp {
    /// Take `callers` for who the calls of the provider's tools and its brief requests are for; until then those calls
    /// are refused, the identity record taken as one that cannot be read.
    pub fn with_callers(&self, callers: Callers) {
        let _ = self.hub.callers.set(callers);
    }

    /// Who a provider call now is for: an error saying why when the ant's identity record is absent or cannot be read.
    pub async fn caller(&self) -> Result<Caller, String> {
        self.hub.who_calls().await
    }

    /// Where `thread` came from, as its messages handled since the daemon started tell ([`Mcp::begin`]).
    pub fn origin(&self, thread: &ThreadId) -> Option<Origin> {
        self.hub.sessions.locked().origins.get(thread).cloned()
    }

    /// Who calls the provider on `thread` now, where and when, as a call of one of its tools carries it: an error
    /// saying why when ants does not know it all, as when the ant's identity record cannot be read or with no turn on
    /// the thread since the daemon started.
    pub async fn call_context(&self, thread: &ThreadId) -> Result<CallContext, String> {
        self.hub.call_context(thread).await
    }
}

impl Hub {
    async fn who_calls(&self) -> Result<Caller, String> {
        match self.callers.get() {
            Some(callers) => callers.caller().await,
            None => Err(CANNOT_READ.to_owned()),
        }
    }

    async fn call_context(&self, thread: &ThreadId) -> Result<CallContext, String> {
        let caller = self.who_calls().await?;
        let sessions = self.sessions.locked();
        let origin = sessions.origins.get(thread).ok_or("no message on the thread was handled since ants started")?;
        let (harness, model) = sessions.running.get(thread).ok_or("no session of the thread was opened")?;
        Ok(caller.context(thread.clone(), origin.clone(), harness, model.as_deref()))
    }

    /// Keep where the thread of `envelope`, a message it handles, came from ([`origin`], [`settled`]).
    pub(super) fn note_origin(&self, sessions: &mut Sessions, envelope: &Envelope) {
        let now = origin(envelope, None, &self.humans.humans());
        let earlier = sessions.origins.remove(&envelope.thread_id);
        sessions.origins.insert(envelope.thread_id.clone(), settled(earlier, now));
    }
}

/// Keep `channel` as the channel of `thread`, a channel thread, unless it has one.
pub(super) fn note_channel(sessions: &mut Sessions, thread: &ThreadId, channel: &ChannelId) {
    if let Some(origin) = sessions.origins.get_mut(thread) {
        origin.channel.get_or_insert_with(|| channel.clone());
    }
}

impl Tools {
    /// The provider's tools as served now: none without a provider of tools, or when its list fails.
    pub(super) async fn provider_tools(&self) -> Vec<rmcp::model::Tool> {
        let Some(provider) = self.hub.provider.get().filter(|p| p.has(Capability::Tools)) else { return Vec::new() };
        let list = match tokio::time::timeout(LIST_TIMEOUT, provider.list_tools()).await {
            Ok(Ok(list)) => list,
            Ok(Err(e)) => {
                tracing::warn!(provider = provider.name(), error = %e, "listing no provider tools");
                return Vec::new();
            }
            Err(_) => {
                tracing::warn!(provider = provider.name(), "listing no provider tools: the provider did not answer");
                return Vec::new();
            }
        };
        let mut served = self.hub.served.locked();
        let tools: Vec<_> =
            list.tools.into_iter().filter_map(|tool| serve(provider, tool, &mut served.refused)).collect();
        served.tools.clone_from(&tools);
        tools
    }

    /// The provider's tool served as `name`, as listed last.
    pub(super) fn provider_tool(&self, name: &str) -> Option<rmcp::model::Tool> {
        self.hub.served.locked().tools.iter().find(|t| t.name == name).cloned()
    }

    /// The provider and the name of its tool that `name` serves, if it serves one: never one of ants' own names.
    pub(super) fn provider_of<'a>(&self, name: &'a str) -> Option<(&Connected, &'a str)> {
        let provider = self.hub.provider.get()?;
        let tool = name.strip_prefix(provider.name())?.strip_prefix('_')?;
        (valid_tool_name(tool) && !own_names().contains(name)).then_some((provider, tool))
    }

    /// Call `tool` of `provider` on `thread` with `arguments`, as served under `name`.
    pub(super) async fn call_provider(
        &self,
        provider: &Connected,
        name: &str,
        tool: &str,
        mut arguments: JsonObject,
        thread: &ThreadId,
    ) -> CallToolResult {
        let format = take_format(&mut arguments);
        let context = match self.hub.call_context(thread).await {
            Ok(context) => context,
            Err(why) => return text(Err(format!("[ants] {name} was not called: {why}."))),
        };
        let call = ToolCall { name: tool.to_owned(), arguments, context, format };
        match tokio::time::timeout(CALL_TIMEOUT, provider.call_tool(call)).await {
            Ok(Ok(result)) => answer(result),
            Ok(Err(e)) => text(Err(format!("[ants] {name} did not reach {}: {}.", provider.name(), e.message))),
            Err(_) => text(Err(format!(
                "[ants] {} did not answer {name} within {} s.",
                provider.name(),
                CALL_TIMEOUT.as_secs()
            ))),
        }
    }
}

/// `tool` of `provider` as served, or none when its name is not valid or would take one of ants' own, or its schema is
/// not an object: logged once, kept in `refused`.
fn serve(
    provider: &Connected,
    tool: ants_proto::provider::Tool,
    refused: &mut HashSet<String>,
) -> Option<rmcp::model::Tool> {
    let name = served_name(provider.name(), &tool.name);
    let schema = match with_json_argument(tool.input_schema) {
        serde_json::Value::Object(schema) => Some(schema),
        _ => None,
    };
    let why = match schema {
        _ if !valid_tool_name(&tool.name) => "its name is not valid",
        _ if own_names().contains(&name) => "an ants tool has its name",
        None => "its input schema is not an object",
        Some(schema) => return Some(rmcp::model::Tool::new(name, tool.description, schema)),
    };
    if refused.insert(name.clone()) {
        tracing::warn!(provider = provider.name(), tool = %name, why, "leaving a provider tool out");
    }
    None
}

/// The provider's answer as the harness gets it: text, or JSON as structured content.
fn answer(result: ToolResult) -> CallToolResult {
    match (result.output, result.is_error) {
        (Output::Text(text), false) => CallToolResult::success(vec![ContentBlock::text(text)]),
        (Output::Text(text), true) => CallToolResult::error(vec![ContentBlock::text(text)]),
        (Output::Json(json), false) => CallToolResult::structured(json),
        (Output::Json(json), true) => CallToolResult::structured_error(json),
    }
}
