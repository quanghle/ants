//! The branch the work of a run so far is on. With worktrees, each step works in a checkout of its own, from
//! the repository's `HEAD`, and its reply names the branch of the step's thread when that holds commits `HEAD` lacks
//! ([`Branch::holds_line`]); the latest such reply to an agent() step sets the run's [`WorkBranch`], as the branch of
//! the thread a run is started from with `run_trail` does at its start, and the agent() steps sent after it name that
//! branch to merge.

use ants_proto::{Envelope, PendingCall, TrailFunction, TrailRun, WorkBranch};

use super::{Slot, Trails};
use crate::worktrees::Branch;

impl Trails {
    /// Note on the run of `slot` the branch `reply`, answering `pending`, an agent() step, tells the step's work is on;
    /// stored with the header the answer stores.
    pub(super) fn note_work_branch(&self, slot: &Slot, pending: &PendingCall, reply: &Envelope) {
        if pending.function != TrailFunction::Agent {
            return;
        }
        let Some(name) = Branch::held_in(&reply.text) else { return };
        let branch = WorkBranch { name: name.to_owned(), by: pending.to.clone() };
        slot.book().header.work_branch = Some(branch);
    }
}

/// The `[ants]` line an agent() step of `run` ends with when the work so far is on a branch.
pub(super) fn work_line(run: &TrailRun) -> Option<String> {
    let WorkBranch { name, by } = run.work_branch.as_ref()?;
    Some(format!(
        "[ants] The work so far is on branch {name}, in the repository of {by}: this step works in a checkout of its \
         own, so merge {name} to build on it."
    ))
}
