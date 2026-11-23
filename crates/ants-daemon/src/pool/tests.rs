//! Tests of the harness pool against fake harnesses, one submodule for each behaviour.

use std::sync::Mutex;

use ants_proto::provider::{BriefReason, Origin, Starter};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::fake::Sessions;
use super::*;
use crate::harness::{PromptLimits, Spend};
use crate::limits::overflow::ContextOverflow;
use crate::provider::brief::Briefing;
use crate::provider::brief::fake::{Answer, Fake};
use crate::provider::context::Caller;

mod briefs;
mod cap;
mod limits;
mod restart;
mod sessions;
mod worktrees;

const IDLE: Duration = Duration::from_secs(60);

fn fake_pool(max_live: usize, sessions: Sessions) -> (Pool, Arc<Mutex<fake::Log>>, Arc<Semaphore>) {
    let (launch, log, gate) = fake::launch_with(sessions);
    (Pool::new(launch, SessionStore::memory(), max_live, IDLE), log, gate)
}

fn thread(n: u8) -> ThreadId {
    format!("t{n}").parse().unwrap()
}

/// Let spawned harness tasks run until they block.
async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

fn with_history(text: &str) -> Prompt {
    let history = crate::stream::history::from_fn(|budget| async move { format!("HISTORY {budget}") });
    Prompt { history: Some(history), ..Prompt::new(text) }
}
