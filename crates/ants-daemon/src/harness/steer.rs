//! Steering a running prompt: ants' `_ants/steer` extension to ACP, which adds a message to the prompt running in a
//! session as the user's next input to its turn, for an agent that advertises it in its capabilities' `_meta`.

use agent_client_protocol::v1::schema::{AgentCapabilities, ContentBlock, SessionId};
use agent_client_protocol::{JsonRpcRequest, JsonRpcResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The key of the agent capabilities' `_meta` whose `true` advertises `_ants/steer`.
const META_KEY: &str = "ants/steer";

/// Add `prompt` to the prompt running in session `session_id`. The agent answers whether it took the message into
/// the running turn; it refuses with no prompt running, or one too far along to take it, and the message then waits
/// for a prompt of its own.
#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcRequest)]
#[request(method = "_ants/steer", response = SteerResponse)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SteerRequest {
    pub session_id: SessionId,
    pub prompt: Vec<ContentBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcResponse)]
pub(crate) struct SteerResponse {
    /// Whether the running prompt took the message: its answer is then the prompt's.
    pub accepted: bool,
}

/// `capabilities` advertising `_ants/steer`.
pub(crate) fn advertise(capabilities: AgentCapabilities) -> AgentCapabilities {
    let mut meta = capabilities.meta.clone().unwrap_or_default();
    meta.insert(META_KEY.to_owned(), Value::Bool(true));
    capabilities.meta(meta)
}

/// Whether an agent of `capabilities` takes `_ants/steer`.
pub(crate) fn advertised(capabilities: &AgentCapabilities) -> bool {
    capabilities.meta.as_ref().and_then(|meta| meta.get(META_KEY)).and_then(Value::as_bool).unwrap_or(false)
}
