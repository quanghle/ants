//! Persona daemon: consumes a persona's durable inbox and answers each message through an ACP harness.

pub mod bus;
pub mod config;
pub mod daemon;
pub mod harness;
pub mod pool;
pub mod workspace;

pub use daemon::{Config, run};
pub use harness::{HarnessKind, Launch, PermissionPolicy};
