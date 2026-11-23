//! Whether an ant's threads get a git worktree each.

use std::path::Path;

use crate::process;

/// Whether each thread of an ant gets its own git worktree: as the config key `worktrees` says, and where it is unset
/// when threads may run at once (more than one harness) in a workdir inside a git work tree that git does not ignore
/// (such as a build output directory).
pub async fn per_thread(workdir: &Path, max_harnesses: usize, worktrees: Option<bool>) -> bool {
    match worktrees {
        Some(on) => on,
        None => max_harnesses > 1 && in_git_work_tree(workdir) && !ignored_by_git(workdir).await,
    }
}

/// Whether threads of an ant run at once in one git work tree, where they may clobber each other's edits: more than one
/// harness, worktrees turned off, and the workdir inside a git work tree.
pub fn shares_git_workdir(workdir: &Path, max_harnesses: usize, worktrees: Option<bool>) -> bool {
    worktrees == Some(false) && max_harnesses > 1 && in_git_work_tree(workdir)
}

/// Whether git ignores `dir`, or cannot tell: only `git check-ignore` finding no rule for it says it does not.
async fn ignored_by_git(dir: &Path) -> bool {
    process::exit_code("git", dir, ["check-ignore", "--quiet", "."]).await != Some(1)
}

/// Whether `dir` is inside a git work tree, judged by a `.git` directory or file in it or above it without running
/// git.
fn in_git_work_tree(dir: &Path) -> bool {
    std::path::absolute(dir).is_ok_and(|dir| dir.ancestors().any(|dir| dir.join(".git").exists()))
}
