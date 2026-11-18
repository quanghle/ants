//! Trails: Python scripts that orchestrate ants, run by [Monty](https://github.com/pydantic/monty) in one-shot
//! `ants subprocess` workers. No NATS: the daemon drives a [`Run`], performs the calls it hands out and stores its
//! [`RunState`] and journal.
//!
//! A run's session lives between steps as a snapshot, Monty's serde form as JSON. Each start or resume spawns a
//! fresh worker ([`Worker`]) with an empty environment, under the run's compute limit; the worker restores the
//! snapshot, runs the script until every live coroutine waits on a step, dumps it again and exits.

mod protocol;
mod run;
mod runner;
mod trail;
mod value;
pub mod worker;

pub use protocol::{Answer, Call, Event, Function, JournalEntry, Limits, Request, Response};
pub use run::{Progress, Run, RunError, RunState, Status};
pub use runner::{GRACE, Worker, WorkerError, deadline};
pub use trail::{Meta, SCRIPT_NAME, TrailError, check};

/// The trail API and patterns in short, as tool descriptions serve them; contrib/skills/ants-trails/SKILL.md is the
/// full guide.
pub const GUIDE: &str = include_str!("guide.md");

/// The global allocator of a binary serving [`worker::main`]: it counts live bytes, which Monty's memory limit
/// reads, and ends a worker past its hard cap with [`monty_types::OOM_EXIT_CODE`].
pub use monty_alloc::LimitedAllocator as Allocator;
