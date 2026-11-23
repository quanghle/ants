//! The branches of an ant's threads: the commits they hold, where a new one starts, the remote they are pushed to and
//! what they are described with.

use std::time::Duration;

use ants_proto::{ThreadId, UserId};
use anyhow::{Context as _, Result};

use super::{Base, FETCH_TIMEOUT, Worktrees};
use crate::process::{git, remote_git};

/// A thread's branch holding commits not in the repository's `HEAD`, nor in the remote's default branch with
/// [`Base::Upstream`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Branch {
    pub thread: ThreadId,
    pub name: String,
    pub commits: usize,
    /// The remote's default branch the commits are not in either, with [`Base::Upstream`], such as `origin/main`.
    pub upstream: Option<String>,
}

impl Branch {
    /// How many commits the branch holds that the repository's `HEAD` does not, in words.
    pub fn commits_text(&self) -> String {
        match self.commits {
            1 => "1 commit".to_owned(),
            n => format!("{n} commits"),
        }
    }

    /// What the commits are not in, in words.
    pub fn outside(&self) -> String {
        match &self.upstream {
            Some(upstream) => format!("the repository's HEAD or {upstream}"),
            None => "the repository's HEAD".to_owned(),
        }
    }
}

impl Worktrees {
    /// The threads of the ant with a branch in the repository.
    pub(super) async fn branched(&self) -> Vec<ThreadId> {
        let prefix = format!("refs/heads/ants/{}/", self.ant);
        let Ok(refs) = git(&self.repo, ["for-each-ref", "--format=%(refname)", &prefix]).await else {
            return Vec::new();
        };
        refs.lines().filter_map(|name| name.strip_prefix(&prefix)?.parse().ok()).collect()
    }

    /// The thread's branch, when it holds commits not in the repository's `HEAD`, nor with [`Base::Upstream`] in the
    /// remote's default branch as last fetched. None too when git fails, as when the branch does not exist.
    pub async fn ahead(&self, thread: &ThreadId) -> Option<Branch> {
        match self.count(thread).await {
            Ok(branch) => Some(branch).filter(|b| b.commits > 0),
            Err(e) => {
                tracing::debug!(%thread, error = format!("{e:#}"), "counting the commits of the thread's branch");
                None
            }
        }
    }

    /// The thread's branch, with how many commits it holds that are not in the repository's `HEAD`, nor with
    /// [`Base::Upstream`] in the remote's default branch as last fetched.
    pub(super) async fn count(&self, thread: &ThreadId) -> Result<Branch> {
        let name = self.branch(thread);
        let upstream = match self.base {
            Base::Head => None,
            Base::Upstream => match self.remote().await {
                Some(remote) => self.upstream(&remote).await,
                None => None,
            },
        };
        let tip = format!("refs/heads/{name}");
        let args = ["rev-list", "--count", &tip, "--not", "HEAD"].into_iter().chain(upstream.as_deref());
        let commits = git(&self.repo, args).await?.trim().parse().context("counting commits")?;
        let upstream = upstream.map(|u| shorten(&u).to_owned());
        Ok(Branch { thread: thread.clone(), name, commits, upstream })
    }

    /// The repository's default remote: the remote of the current branch's upstream, else `origin` when there is one.
    pub async fn remote(&self) -> Option<String> {
        if let Ok(current) = git(&self.repo, ["symbolic-ref", "--quiet", "--short", "HEAD"]).await
            && let Ok(remote) = git(&self.repo, ["config", "--get", &format!("branch.{}.remote", current.trim())]).await
            && !matches!(remote.trim(), "" | ".")
        {
            return Some(remote.trim().to_owned());
        }
        let remotes = git(&self.repo, ["remote"]).await.ok()?;
        remotes.lines().any(|r| r == "origin").then(|| "origin".to_owned())
    }

    /// The default branch of `remote` as last fetched, its `HEAD`, else the upstream of the current branch, as a full
    /// ref name such as `refs/remotes/origin/main`.
    async fn upstream(&self, remote: &str) -> Option<String> {
        for name in [format!("refs/remotes/{remote}/HEAD"), "@{upstream}".to_owned()] {
            if let Ok(full) = git(&self.repo, ["rev-parse", "--verify", "--quiet", "--symbolic-full-name", &name]).await
                && !full.trim().is_empty()
            {
                return Some(full.trim().to_owned());
            }
        }
        None
    }

    /// Where a new branch starts: `HEAD`, or with [`Base::Upstream`] the default branch of the default remote once
    /// fetched, else `HEAD` with a warning.
    pub(super) async fn start(&self, thread: &ThreadId) -> String {
        if self.base == Base::Head {
            return "HEAD".to_owned();
        }
        match self.fetched_upstream().await {
            Ok(upstream) => {
                tracing::info!(%thread, %upstream, "starting the thread's branch from the remote's default branch");
                upstream
            }
            Err(e) => {
                tracing::warn!(%thread, error = format!("{e:#}"), "starting the thread's branch from HEAD instead");
                "HEAD".to_owned()
            }
        }
    }

    /// The default branch of the default remote, once fetched.
    async fn fetched_upstream(&self) -> Result<String> {
        let remote = self.remote().await.context("the repository has no default remote")?;
        let _fetching = self.repository.fetching.lock().await;
        let fetch = ["fetch", "--quiet", "--no-tags", &remote];
        remote_git(&self.repo, fetch, FETCH_TIMEOUT).await?;
        self.upstream(&remote).await.with_context(|| {
            format!(
                "remote {remote} has no HEAD (git remote set-head {remote} --auto) and the current branch no upstream"
            )
        })
    }

    /// Whether `branch` is at the commit `remote` had it at when last pushed or fetched.
    pub async fn pushed(&self, branch: &Branch, remote: &str) -> bool {
        let tip = |name: String| async move { git(&self.repo, ["rev-parse", "--verify", "--quiet", &name]).await.ok() };
        let local = tip(format!("refs/heads/{}", branch.name)).await;
        local.is_some() && local == tip(format!("refs/remotes/{remote}/{}", branch.name)).await
    }

    /// Push `branch` to `remote` (`git push <remote> <branch>`), prompting for no credentials and failing after
    /// `timeout`.
    pub async fn push(&self, branch: &Branch, remote: &str, timeout: Duration) -> Result<()> {
        remote_git(&self.repo, ["push", "--quiet", remote, &branch.name], timeout).await.map(drop)
    }

    /// What the thread's branch was described with ([`Worktrees::describe`]), if anything.
    pub async fn description(&self, thread: &ThreadId) -> Option<String> {
        let key = format!("branch.{}.description", self.branch(thread));
        let description = git(&self.repo, ["config", "--get", &key]).await.ok()?;
        Some(description.trim().to_owned()).filter(|d| !d.is_empty())
    }

    /// Describe the thread's branch with `text` (`branch.<branch>.description`), unless it has a description already or
    /// does not exist.
    pub async fn describe(&self, thread: &ThreadId, text: &str) -> Result<()> {
        let branch = self.branch(thread);
        if self.description(thread).await.is_some() || !self.has_branch(&branch).await {
            return Ok(());
        }
        let _alone = self.repository.changes.alone().await;
        git(&self.repo, ["config", &format!("branch.{branch}.description"), text]).await.map(drop)
    }

    pub(super) async fn has_branch(&self, branch: &str) -> bool {
        git(&self.repo, ["rev-parse", "--verify", "--quiet", &format!("refs/heads/{branch}")]).await.is_ok()
    }

    /// Delete `branch`, whatever commits it holds.
    pub(super) async fn delete_branch(&self, branch: &str) -> Result<()> {
        git(&self.repo, ["branch", "--delete", "--force", "--quiet", branch]).await.map(drop)
    }
}

/// `ref_name` without its `refs/remotes/` or `refs/heads/` prefix, as git shows it.
fn shorten(ref_name: &str) -> &str {
    ref_name.strip_prefix("refs/remotes/").or_else(|| ref_name.strip_prefix("refs/heads/")).unwrap_or(ref_name)
}

/// The branch of `ant`'s worktree of `thread`.
pub(crate) fn branch_name(ant: &UserId, thread: &ThreadId) -> String {
    format!("ants/{ant}/{thread}")
}
