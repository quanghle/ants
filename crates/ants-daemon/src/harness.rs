//! ACP client side: start harnesses over stdio, keep their sessions, and run prompts through them.

mod catalog;
mod configure;
mod connection;
mod definition;
mod launch;
mod mcp;
mod mcp_sign_in;
pub(crate) mod native;
mod observe;
mod process;
mod session;
mod sign_in;
mod skills;
mod steer;

pub use catalog::{BUILTIN_HARNESSES, Harnesses, Unset, parse_definitions};
pub(crate) use definition::validate_env;
pub use definition::{ContextValue, Harness, allows_all};
pub use launch::{Launch, McpServers, McpSession};
pub use mcp::{McpHost, open_mcp, unreported_mcp};
pub use mcp_sign_in::{McpSignIn, McpSignIns};
pub use native::Protocol;
use process::one_of;
pub use process::{ModelSettings, on_path};
pub use session::{
    DEFAULT_MAX_TOOL_CALLS, DEFAULT_PROMPT_TIMEOUT, Prompt, PromptLimits, Reply, Session, Spend, Steer, Steering, fork,
    probe, prompt_once,
};
pub use sign_in::{Handshake, Readiness, SignedOut, check, handshake};
pub(crate) use steer::{SteerRequest, SteerResponse, advertise as advertise_steering};

// The tests of the native translators take these through this module.
#[cfg(all(test, unix))]
use crate::limits::breaker::StartFailed;
#[cfg(all(test, unix))]
use ants_proto::PlanStep;

#[cfg(test)]
mod tests;
