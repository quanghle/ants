//! Persona daemon: consumes a persona's durable inbox and answers each message through an ACP harness.

pub mod approvals;
pub mod attempts;
pub mod bus;
pub mod config;
pub mod daemon;
pub mod dead;
pub mod harness;
pub mod mcp;
pub mod permission;
pub mod plan;
pub mod pool;
pub mod roster;
pub mod workspace;

pub use daemon::{Config, run};
pub use harness::{HarnessKind, Launch};
pub use permission::{Allowlist, Approver, PermissionPolicy};
