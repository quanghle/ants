//! The worktrees of the ant cloned, added again for the clone, and the git commands that do it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{ThreadId, UserId};
use anyhow::{Context as _, Result};

use super::files::copy;
use super::undo::Done;
use crate::clone::ThreadCopy;

/// Longest a git command of a worktree copy may take.
const GIT_WAIT: Duration = Duration::from_mins(2);

/// A worktree of the ant cloned, as it was before its home was copied.
pub(super) struct Tree {
    pub(super) thread: ThreadId,
    from: PathBuf,
    /// The repository's git directory.
    common: PathBuf,
    head: String,
    /// A commit of its uncommitted changes (`git stash create`), if it has any.
    changes: Option<String>,
    untracked: Vec<PathBuf>,
}

/// The worktrees under `home` of the threads of `copies`.
pub(super) async fn worktrees(home: &Path, copies: &[ThreadCopy]) -> Result<Vec<Tree>> {
    let mut trees = Vec::new();
    for copy in copies {
        let from = home.join("worktrees").join(copy.thread.as_str());
        if !from.join(".git").is_file() {
            continue;
        }
        let head = git(&from, &["rev-parse", "HEAD"]).await?.trim().to_owned();
        let changes = git(&from, &["stash", "create"]).await?.trim().to_owned();
        let untracked = git(&from, &["ls-files", "--others", "--exclude-standard", "-z"]).await?;
        let common = git(&from, &["rev-parse", "--path-format=absolute", "--git-common-dir"]).await?;
        trees.push(Tree {
            thread: copy.thread.clone(),
            common: PathBuf::from(common.trim()),
            head,
            changes: Some(changes).filter(|c| !c.is_empty()),
            untracked: untracked.split('\0').filter(|f| !f.is_empty()).map(PathBuf::from).collect(),
            from,
        });
    }
    Ok(trees)
}

impl Tree {
    /// Where the clone's session of the thread works in the worktree added again in `new_home`: at the place the
    /// session cloned worked at, in `was`, in this one.
    pub(super) fn session_dir(&self, new_home: &Path, was: &Path) -> PathBuf {
        let to = new_home.join("worktrees").join(self.thread.as_str());
        match was.strip_prefix(&self.from) {
            Ok(within) => to.join(within),
            Err(_) => to,
        }
    }

    /// Add the worktree again in `new_home`, the home of `new` copied from `src_home`, on its own branch: the branch.
    pub(super) async fn add(&self, src_home: &Path, new_home: &Path, new: &UserId, done: &mut Done) -> Result<String> {
        let repo = match self.common.strip_prefix(src_home) {
            Ok(inside) => new_home.join(inside),
            Err(_) => self.common.clone(),
        };
        let to = new_home.join("worktrees").join(self.thread.as_str());
        let branch = format!("ants/{new}/{}", self.thread);
        let path = to.to_str().context("the worktree path is not UTF-8")?;
        git(&repo, &["worktree", "add", "--quiet", "--no-track", "-b", &branch, path, &self.head]).await?;
        done.worktrees.push((repo, to.clone(), branch.clone()));
        if let Some(changes) = &self.changes {
            git(&to, &["stash", "apply", "--quiet", changes]).await?;
        }
        for file in &self.untracked {
            let target = to.join(file);
            if let Some(dir) = target.parent() {
                std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            }
            copy(&self.from.join(file), &target)?;
        }
        Ok(branch)
    }
}

pub(super) async fn git(dir: &Path, args: &[&str]) -> Result<String> {
    crate::worktrees::run("git", dir, args, &[("GIT_TERMINAL_PROMPT", "0")], Some(GIT_WAIT)).await
}
