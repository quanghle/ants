//! Persona daemon: consumes a persona's durable inbox and answers each message through an ACP harness.

pub mod bus;
pub mod daemon;
pub mod harness;
pub mod workspace;

pub use daemon::{Config, run};
pub use harness::{HarnessKind, PermissionPolicy};
