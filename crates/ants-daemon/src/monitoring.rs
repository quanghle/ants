//! What a daemon reports while it works: live progress and the tool-call timeline of each prompt, health and metrics
//! over HTTP, and the JetStream storage the ants depend on.

pub mod metrics;
pub(crate) mod status;
pub mod storage;
pub(crate) mod timeline;
