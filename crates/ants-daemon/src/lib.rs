//! Ant daemon: consumes an ant's durable inbox and answers each message through an ACP harness.

#![cfg_attr(
    all(test, target_os = "macos"),
    allow(linker_messages, reason = "a test binary's unwind tables outgrow what ld64 encodes compactly (16 MB)")
)]
#![expect(
    rustdoc::private_intra_doc_links,
    reason = "read with --document-private-items, where the links to private items resolve"
)]
#![cfg_attr(test, allow(clippy::disallowed_methods, reason = "tests read and hear as they please"))]

// An ant served: its inbox loop, the harness sessions it prompts and the worktrees they work in, the tools of those
// harnesses, and what a thread committed.
pub mod checkouts;
pub mod daemon;
pub mod harness;
mod ipc;
pub mod mcp;
pub mod permission;
pub mod pool;
mod publish;
pub mod worktrees;

// The config file and what it sets up.
pub mod config;
pub mod schedule;

// The hub, the machine and the colony, and the admin orders to a machine's ants.
pub mod admin;
pub mod bus;
pub mod clone;
pub mod colony;
pub mod files;
pub mod host;
pub mod hub;
mod pause;

// Beyond one ant: escalations, tool requests escalated to humans, and channels.
mod approvals;
pub mod chanadm;
pub mod channel;
pub mod escalation;

pub mod delivery;
pub mod limits;
pub mod monitoring;
pub mod people;
pub mod profile;
pub mod provider;
pub mod records;
pub mod secrets;
pub mod sense;
pub mod stream;
pub mod trail;

pub mod sync;
#[cfg(test)]
mod test_support;

pub use daemon::{Config, Stop, run, run_until};
pub use harness::{Harness, Harnesses, Launch};
pub use permission::{Allowlist, Approver, PermissionPolicy};
