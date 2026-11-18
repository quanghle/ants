//! Admin orders to an ant and the commands sending them: sending an order and listing them, pausing, resuming and
//! restarting an ant, stopping and resuming a trail run it conducts, and reading its settings, memories and MCP
//! servers, which its machine's daemon answers.

pub(crate) mod mcp;
pub(crate) mod memories;
pub(crate) mod orders;
pub(crate) mod pause;
pub(crate) mod restart;
pub(crate) mod settings;
pub(crate) mod trail;
