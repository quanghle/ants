//! A git worktree per thread, so the threads of one ant running at once do not edit and build in the same checkout.
//!
//! Each thread works in `<home>/worktrees/<thread>`, a worktree of the repository the ant's workdir is in, on its own
//! branch `ants/<ant>/<thread>` started from the repository's `HEAD`, or with [`Base::Upstream`] from the default
//! branch of its default remote, fetched first ([`Worktrees::with_base`]). A branch rather than a detached `HEAD` keeps
//! what the thread committed reachable after its worktree is removed, and two worktrees never need the same branch.
//! The worktree is created on the thread's first prompt, reused whenever its harness starts again (so session/load
//! sees the same directory) and removed once the thread's session expired from the session store, along with its
//! branch unless that holds commits not merged into the repository's `HEAD` (nor, with [`Base::Upstream`], into the
//! remote's default branch). Worktrees left behind once the option is turned off are removed the same way
//! ([`Worktrees::left_behind`]), no new one being created. A branch holding such commits is reported
//! ([`Worktrees::ahead`], [`Pruned::kept`]), so its work is not forgotten.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use std::time::Duration;

use ants_proto::{ThreadId, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use tokio::sync::Mutex;

/// Longest a git fetch for a thread's new branch may take.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(60);

/// Where a thread's new branch starts: the ant config key `worktree_base`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Base {
    /// The repository's `HEAD`.
    #[default]
    Head,
    /// The default branch of the repository's default remote, fetched first: the remote's `HEAD`, else the upstream of
    /// the current branch. `HEAD`, with a warning, when there is no remote, the fetch fails or neither is known.
    Upstream,
}

impl Base {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::Upstream => "upstream",
        }
    }
}

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

/// What a [`Worktrees::prune`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Pruned {
    /// How many worktrees were removed.
    pub removed: usize,
    /// The branches of the removed worktrees kept for the commits they hold.
    pub kept: Vec<Branch>,
}

pub struct Worktrees {
    ant: UserId,
    /// Where git is run for the repository: the top level of its main worktree, or its git directory for worktrees left
    /// behind.
    repo: PathBuf,
    /// Where the workdir is inside the repository, so a thread works at the same place in its own worktree.
    within: PathBuf,
    /// A directory per thread.
    dir: PathBuf,
    /// One change to the worktrees at a time, so a thread's worktree is never removed while it is being set up.
    changing: Mutex<()>,
    /// Where a thread's new branch starts.
    base: Base,
}

impl Worktrees {
    /// The worktrees of the ant's threads under `home`, of the repository `workdir` is in. None, with a warning, when
    /// it is in none or git cannot be run: the threads then share the workdir.
    pub async fn find(ant: &UserId, workdir: &Path, home: &Path) -> Option<Self> {
        match git(workdir, ["rev-parse", "--show-toplevel", "--show-prefix"]).await {
            Ok(out) => {
                let mut lines = out.lines();
                let repo = PathBuf::from(lines.next().unwrap_or_default());
                let within = Path::new(lines.next().unwrap_or_default()).components().collect();
                tracing::info!(repo = %repo.display(), "giving each thread its own git worktree");
                let dir = home.join("worktrees");
                Some(Self { ant: ant.clone(), repo, within, dir, changing: Mutex::new(()), base: Base::Head })
            }
            Err(e) => {
                tracing::warn!(
                    workdir = %workdir.display(),
                    error = format!("{e:#}"),
                    "workdir is not in a git repository; its threads share it"
                );
                None
            }
        }
    }

    /// The worktrees left under `home` by the ant's threads, to be removed as their sessions expire while no new one is
    /// created. None when there are none, or, with a warning, when none of them is in a repository. The repository is
    /// found from the worktrees, not the workdir, which may have changed since.
    pub async fn left_behind(ant: &UserId, home: &Path) -> Option<Self> {
        let dir = home.join("worktrees");
        let threads = threads_in(&dir);
        if threads.is_empty() {
            return None;
        }
        for thread in &threads {
            let path = dir.join(thread.as_str());
            // Checked first, as git would otherwise look for a repository around the home.
            if !path.join(".git").is_file() {
                continue;
            }
            if let Ok(common) = git(&path, ["rev-parse", "--git-common-dir"]).await {
                let repo = path.join(common.trim());
                tracing::info!(
                    worktrees = %dir.display(),
                    "removing the git worktrees left by threads once their sessions expire"
                );
                let within = PathBuf::new();
                return Some(Self { ant: ant.clone(), repo, within, dir, changing: Mutex::new(()), base: Base::Head });
            }
        }
        tracing::warn!(worktrees = %dir.display(), "keeping the thread worktrees left in no git repository");
        None
    }

    /// The same, starting a thread's new branch from `base`, which also tells what the commits of a branch are compared
    /// with ([`Worktrees::ahead`]).
    #[must_use]
    pub fn with_base(self, base: Base) -> Self {
        Self { base, ..self }
    }

    /// The directory the thread works in, in its worktree, which is created when missing.
    pub async fn checkout(&self, thread: &ThreadId) -> Result<PathBuf> {
        let _changing = self.changing.lock().await;
        let path = self.path(thread);
        if !path.join(".git").is_file() {
            self.add(thread, &path).await?;
            tracing::info!(%thread, worktree = %path.display(), "created the thread's git worktree");
        }
        let cwd = if self.within.as_os_str().is_empty() { path } else { path.join(&self.within) };
        if !cwd.is_dir() {
            bail!("{} is not in the thread's worktree", self.within.display());
        }
        Ok(cwd)
    }

    /// Remove the worktree of each thread `expired` tells, one at a time, and tell how many were removed and which
    /// branches were kept. A thread whose worktree is being set up waits for the removal to end, and `expired` is asked
    /// with no change going on.
    pub async fn prune(&self, mut expired: impl AsyncFnMut(&ThreadId) -> bool) -> Pruned {
        let mut pruned = Pruned::default();
        for thread in self.threads() {
            let _changing = self.changing.lock().await;
            if !expired(&thread).await {
                continue;
            }
            match self.remove(&thread).await {
                Ok(kept) => {
                    pruned.removed += 1;
                    pruned.kept.extend(kept);
                }
                Err(e) => tracing::warn!(%thread, error = format!("{e:#}"), "removing the thread's git worktree"),
            }
        }
        pruned
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
    async fn count(&self, thread: &ThreadId) -> Result<Branch> {
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
    async fn start(&self, thread: &ThreadId) -> String {
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
        let fetch = ["fetch", "--quiet", "--no-tags", &remote];
        remote_git(&self.repo, fetch, FETCH_TIMEOUT).await?;
        self.upstream(&remote).await.with_context(|| {
            format!(
                "remote {remote} has no HEAD (git remote set-head {remote} --auto) and the current branch no upstream"
            )
        })
    }

    /// The repository the worktrees belong to.
    pub fn repo(&self) -> &Path {
        &self.repo
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
        if self.description(thread).await.is_some()
            || git(&self.repo, ["rev-parse", "--verify", "--quiet", &format!("refs/heads/{branch}")]).await.is_err()
        {
            return Ok(());
        }
        git(&self.repo, ["config", &format!("branch.{branch}.description"), text]).await.map(drop)
    }

    /// The threads with a worktree.
    pub fn threads(&self) -> Vec<ThreadId> {
        threads_in(&self.dir)
    }

    fn path(&self, thread: &ThreadId) -> PathBuf {
        self.dir.join(thread.as_str())
    }

    fn branch(&self, thread: &ThreadId) -> String {
        format!("ants/{}/{thread}", self.ant)
    }

    async fn add(&self, thread: &ThreadId, path: &Path) -> Result<()> {
        // Left over from an add that failed half-way: it is no worktree, and in the way of a new one.
        if path.exists() {
            std::fs::remove_dir_all(path).with_context(|| format!("removing {}", path.display()))?;
        }
        std::fs::create_dir_all(&self.dir).with_context(|| format!("creating {}", self.dir.display()))?;
        // Forget worktrees whose directory is gone, which git would not add again.
        git(&self.repo, ["worktree", "prune"]).await?;
        let branch = self.branch(thread);
        let exists =
            git(&self.repo, ["rev-parse", "--verify", "--quiet", &format!("refs/heads/{branch}")]).await.is_ok();
        let path = path.as_os_str();
        if exists {
            git(&self.repo, [OsStr::new("worktree"), "add".as_ref(), "--quiet".as_ref(), path, branch.as_ref()])
                .await?;
        } else {
            let start = self.start(thread).await;
            // No upstream for the branch, which starting from a remote branch would otherwise set.
            let args = ["worktree", "add", "--quiet", "--no-track", "-b", &branch].map(OsStr::new);
            git(&self.repo, args.into_iter().chain([path, OsStr::new(&start)])).await?;
        }
        Ok(())
    }

    /// Remove the thread's worktree, and its branch unless that holds commits not in the repository's `HEAD`: then the
    /// branch kept.
    async fn remove(&self, thread: &ThreadId) -> Result<Option<Branch>> {
        let path = self.path(thread);
        let removed =
            git(&self.repo, [OsStr::new("worktree"), "remove".as_ref(), "--force".as_ref(), path.as_ref()]).await;
        if let Err(e) = removed {
            if path.join(".git").exists() {
                return Err(e);
            }
            std::fs::remove_dir_all(&path).with_context(|| format!("removing {}", path.display()))?;
        }
        git(&self.repo, ["worktree", "prune"]).await?;
        let counted = self.count(thread).await;
        let kept = counted.as_ref().ok().filter(|b| b.commits > 0).cloned();
        match &kept {
            Some(branch) => {
                tracing::info!(%thread, branch = branch.name, commits = branch.commits, "keeping the thread's branch");
            }
            None => {
                let branch = self.branch(thread);
                let mut args = vec!["branch", "--delete", "--quiet"];
                // Known to hold nothing new, the branch goes even when started from a remote branch HEAD lacks; should
                // counting have failed, git still refuses to delete a branch with commits not merged into HEAD.
                if counted.is_ok() {
                    args.push("--force");
                }
                args.push(&branch);
                if let Err(e) = git(&self.repo, args).await {
                    tracing::info!(%thread, %branch, error = format!("{e:#}"), "keeping the thread's branch");
                }
            }
        }
        tracing::info!(%thread, worktree = %path.display(), "removed the git worktree of the expired thread");
        Ok(kept)
    }
}

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
    let status = tokio::process::Command::new("git")
        .args(["check-ignore", "--quiet", "."])
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    !status.is_ok_and(|status| status.code() == Some(1))
}

/// Whether `dir` is inside a git work tree, judged by a `.git` directory or file in it or above it without running
/// git.
fn in_git_work_tree(dir: &Path) -> bool {
    std::path::absolute(dir).is_ok_and(|dir| dir.ancestors().any(|dir| dir.join(".git").exists()))
}

/// The threads with a directory in `dir`.
fn threads_in(dir: &Path) -> Vec<ThreadId> {
    let Ok(entries) = std::fs::read_dir(dir) else { return Vec::new() };
    let mut threads: Vec<ThreadId> =
        entries.flatten().filter_map(|entry| entry.file_name().to_str()?.parse().ok()).collect();
    threads.sort();
    threads
}

/// `ref_name` without its `refs/remotes/` or `refs/heads/` prefix, as git shows it.
fn shorten(ref_name: &str) -> &str {
    ref_name.strip_prefix("refs/remotes/").or_else(|| ref_name.strip_prefix("refs/heads/")).unwrap_or(ref_name)
}

/// Run git in `dir` and return what it printed, or fail with what it printed on stderr.
async fn git<I, S>(dir: &Path, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run("git", dir, args, &[], None).await
}

/// Run git in `dir` to reach a remote, as [`git`] does, prompting for no credentials and failing after `timeout`.
pub(crate) async fn remote_git<I, S>(dir: &Path, args: I, timeout: Duration) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run("git", dir, args, &[("GIT_TERMINAL_PROMPT", "0")], Some(timeout)).await
}

/// Run `program` in `dir` with `env` added, and return what it printed, or fail with what it printed on stderr, or
/// after `timeout`, killing it.
pub(crate) async fn run<P, I, S>(
    program: P,
    dir: &Path,
    args: I,
    env: &[(&str, &str)],
    timeout: Option<Duration>,
) -> Result<String>
where
    P: AsRef<OsStr>,
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let program = program.as_ref();
    let args: Vec<S> = args.into_iter().collect();
    let shown = || {
        let words = std::iter::once(program).chain(args.iter().map(AsRef::as_ref));
        words.map(OsStr::to_string_lossy).collect::<Vec<_>>().join(" ")
    };
    let output = tokio::process::Command::new(program)
        .args(&args)
        .envs(env.iter().copied())
        .current_dir(dir)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output();
    let output = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, output)
            .await
            .map_err(|_| anyhow::anyhow!("{}: no answer within {}", shown(), ants_proto::duration::format(timeout)))?,
        None => output.await,
    }
    .with_context(|| format!("running {}", shown()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            bail!("{}: {}", shown(), output.status);
        }
        bail!("{}: {stderr}", shown());
    }
    String::from_utf8(output.stdout).with_context(|| format!("{}: output is no UTF-8", shown()))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;
    use crate::test_support::{commit, repo, run};

    fn ant() -> UserId {
        "dev-a".parse().unwrap()
    }

    fn thread(name: &str) -> ThreadId {
        name.parse().unwrap()
    }

    #[tokio::test]
    async fn each_thread_gets_its_own_worktree_and_keeps_it() {
        let Some(repo) = repo("worktrees-own") else { return };
        let home = repo.parent().unwrap().join("home");
        let worktrees = Worktrees::find(&ant(), &repo.join("sub"), &home).await.unwrap();
        let a = worktrees.checkout(&thread("a")).await.unwrap();
        let b = worktrees.checkout(&thread("b")).await.unwrap();
        assert_eq!((a.clone(), b.clone()), (home.join("worktrees/a/sub"), home.join("worktrees/b/sub")));
        assert_eq!(std::fs::read_to_string(a.join("file")).unwrap(), "one");
        assert_eq!(run(&a, &["branch", "--show-current"]).trim(), "ants/dev-a/a");
        std::fs::write(a.join("file"), "edited").unwrap();
        assert_eq!(worktrees.checkout(&thread("a")).await.unwrap(), a);
        assert_eq!(std::fs::read_to_string(a.join("file")).unwrap(), "edited", "the worktree is reused");
        assert_eq!(std::fs::read_to_string(b.join("file")).unwrap(), "one");
        assert_eq!(std::fs::read_to_string(repo.join("sub/file")).unwrap(), "one");
        assert_eq!(worktrees.threads(), [thread("a"), thread("b")]);
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn expired_threads_lose_their_worktree_and_merged_branch() {
        let Some(repo) = repo("worktrees-prune") else { return };
        let home = repo.parent().unwrap().join("home");
        let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
        let (a, b, c) = (thread("a"), thread("b"), thread("c"));
        for t in [&a, &b, &c] {
            worktrees.checkout(t).await.unwrap();
        }
        std::fs::write(home.join("worktrees/a/sub/file"), "uncommitted").unwrap();
        commit(&home.join("worktrees/b"), "work of b");
        let ahead = Branch { thread: b.clone(), name: "ants/dev-a/b".to_owned(), commits: 1, upstream: None };
        assert_eq!(worktrees.ahead(&b).await, Some(ahead.clone()));
        assert_eq!(worktrees.ahead(&a).await, None, "uncommitted work is no commit");
        assert_eq!(worktrees.ahead(&thread("d")).await, None, "no branch");
        let pruned = worktrees.prune(async |t: &ThreadId| *t != c).await;
        assert_eq!(pruned, Pruned { removed: 2, kept: vec![ahead] });
        assert_eq!(worktrees.threads(), std::slice::from_ref(&c));
        let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
        assert_eq!(branches.lines().collect::<Vec<_>>(), ["ants/dev-a/b", "ants/dev-a/c"], "unmerged work is kept");
        assert_eq!(run(&repo, &["worktree", "list", "--porcelain"]).matches("worktree ").count(), 2);
        let again = worktrees.checkout(&b).await.unwrap();
        assert_eq!(
            run(&again, &["log", "-1", "--format=%s"]).trim(),
            "work of b",
            "a kept branch is checked out again"
        );
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn worktrees_left_behind_are_found_from_their_repository() {
        let Some(repo) = repo("worktrees-left") else { return };
        let home = repo.parent().unwrap().join("home");
        assert!(Worktrees::left_behind(&ant(), &home).await.is_none(), "no worktrees");
        let worktrees = Worktrees::find(&ant(), &repo.join("sub"), &home).await.unwrap();
        let (a, b) = (thread("a"), thread("b"));
        worktrees.checkout(&a).await.unwrap();
        worktrees.checkout(&b).await.unwrap();
        // Not a worktree: skipped when looking for the repository, and never mistaken for a thread of the enclosing one.
        std::fs::remove_file(home.join("worktrees/a/.git")).unwrap();
        let moved = repo.parent().unwrap().join("moved");
        std::fs::rename(&repo, &moved).unwrap();
        run(&moved, &["worktree", "repair", home.join("worktrees/b").to_str().unwrap()]);
        let left = Worktrees::left_behind(&ant(), &home).await.unwrap();
        assert_eq!(left.prune(async |_: &ThreadId| true).await, Pruned { removed: 2, kept: Vec::new() });
        assert!(left.threads().is_empty());
        assert_eq!(run(&moved, &["worktree", "list", "--porcelain"]).matches("worktree ").count(), 1);
        let branches = run(&moved, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
        assert_eq!(branches, "", "merged branches are deleted");
        std::fs::remove_dir_all(moved.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_missing_or_broken_worktree_is_created_again() {
        let Some(repo) = repo("worktrees-broken") else { return };
        let home = repo.parent().unwrap().join("home");
        let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
        let a = thread("a");
        let path = worktrees.checkout(&a).await.unwrap();
        std::fs::remove_dir_all(&path).unwrap();
        assert_eq!(worktrees.checkout(&a).await.unwrap(), path, "a registered worktree whose directory is gone");
        std::fs::remove_file(path.join(".git")).unwrap();
        assert_eq!(worktrees.checkout(&a).await.unwrap(), path, "a directory that is no worktree");
        assert_eq!(std::fs::read_to_string(path.join("sub/file")).unwrap(), "one");
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn a_workdir_outside_a_work_tree_is_shared() {
        if !ants_test_support::have("git") {
            return;
        }
        let dir = ants_test_support::scratch_dir("worktrees-bare").canonicalize().unwrap();
        run(&dir, &["init", "--quiet", "--bare"]);
        assert!(Worktrees::find(&ant(), &dir, &dir.join("home")).await.is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_repository_without_commits_fails_the_checkout() {
        if !ants_test_support::have("git") {
            return;
        }
        let dir = ants_test_support::scratch_dir("worktrees-empty").canonicalize().unwrap();
        run(&dir, &["init", "--quiet"]);
        let worktrees = Worktrees::find(&ant(), &dir, &dir.join(".home")).await.unwrap();
        assert!(worktrees.checkout(&thread("a")).await.is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn worktrees_default_on_for_concurrent_threads_in_a_git_workdir() {
        let Some(repo) = repo("worktrees-default") else { return };
        let sub = repo.join("sub");
        let outside = std::env::temp_dir();
        assert!(per_thread(&sub, 2, None).await, "unset, concurrent threads in a git workdir");
        assert!(!per_thread(&sub, 1, None).await, "unset, one thread at a time");
        assert!(!per_thread(Path::new("/"), 4, None).await, "unset, no git work tree");
        assert!(!per_thread(&sub, 2, Some(false)).await, "turned off");
        assert!(per_thread(&sub, 1, Some(true)).await && per_thread(&outside, 1, Some(true)).await, "turned on");
        std::fs::create_dir(repo.join("build")).unwrap();
        std::fs::write(repo.join(".gitignore"), "/build\n").unwrap();
        assert!(!per_thread(&repo.join("build"), 2, None).await, "unset, a workdir git ignores");
        let scratch = ants_test_support::scratch_dir("worktrees-default-scratch");
        assert!(!per_thread(&scratch, 4, None).await, "unset, the scratch dirs of tests in ants' ignored target/");
        std::fs::remove_dir_all(scratch).unwrap();
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[test]
    fn concurrent_threads_share_a_git_workdir_only_with_worktrees_turned_off() {
        let dir = ants_test_support::scratch_dir("worktrees-shared");
        std::fs::create_dir_all(dir.join(".git")).unwrap();
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        assert!(shares_git_workdir(&dir.join("sub"), 2, Some(false)));
        assert!(!shares_git_workdir(&dir.join("sub"), 2, None));
        assert!(!shares_git_workdir(&dir.join("sub"), 2, Some(true)));
        assert!(!shares_git_workdir(&dir.join("sub"), 1, Some(false)));
        std::fs::remove_dir(dir.join(".git")).unwrap();
        std::fs::write(dir.join(".git"), "gitdir: elsewhere").unwrap();
        assert!(shares_git_workdir(&dir, 4, Some(false)), "a worktree's .git is a file");
        assert!(!shares_git_workdir(Path::new("/"), 4, Some(false)));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A bare repository `remote.git` beside `repo`, its `origin` with `main` pushed, the remote's `HEAD` set unless
    /// `head` is false, and a clone `other` of it.
    fn with_remote(repo: &Path, head: bool) -> (PathBuf, PathBuf) {
        let dir = repo.parent().unwrap();
        let remote = dir.join("remote.git");
        run(dir, &["init", "--quiet", "--bare", "--initial-branch=main", remote.to_str().unwrap()]);
        run(repo, &["remote", "add", "origin", remote.to_str().unwrap()]);
        if head {
            run(repo, &["push", "--quiet", "origin", "main"]);
            run(repo, &["remote", "set-head", "origin", "main"]);
        } else {
            // Since git 2.48 a fetch otherwise sets the remote's HEAD.
            run(repo, &["config", "remote.origin.followRemoteHEAD", "never"]);
            run(repo, &["push", "--quiet", "origin", "main"]);
        }
        run(dir, &["clone", "--quiet", remote.to_str().unwrap(), dir.join("other").to_str().unwrap()]);
        (remote, dir.join("other"))
    }

    #[tokio::test]
    async fn an_upstream_base_starts_the_branch_from_the_fetched_remote_default_branch() {
        let Some(repo) = repo("worktrees-upstream") else { return };
        let home = repo.parent().unwrap().join("home");
        let (_, other) = with_remote(&repo, true);
        commit(&other, "upstream work");
        run(&other, &["push", "--quiet", "origin", "main"]);
        let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap().with_base(Base::Upstream);
        assert_eq!(worktrees.remote().await.as_deref(), Some("origin"));
        let (a, b) = (thread("a"), thread("b"));
        let path = worktrees.checkout(&a).await.unwrap();
        assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "upstream work");
        assert_eq!(run(&repo, &["log", "-1", "--format=%s"]).trim(), "first", "the workdir's checkout is untouched");
        assert!(
            Command::new("git")
                .args(["config", "branch.ants/dev-a/a.remote"])
                .current_dir(&repo)
                .status()
                .is_ok_and(|s| !s.success())
        );
        assert_eq!(worktrees.ahead(&a).await, None, "the upstream commit is no commit of the thread");
        commit(&path, "work of a");
        let ahead = worktrees.ahead(&a).await.unwrap();
        assert_eq!((ahead.commits, ahead.upstream.as_deref()), (1, Some("origin/main")));
        assert_eq!(ahead.outside(), "the repository's HEAD or origin/main");
        worktrees.checkout(&b).await.unwrap();
        let pruned = worktrees.prune(async |_: &ThreadId| true).await;
        assert_eq!(pruned, Pruned { removed: 2, kept: vec![ahead] });
        let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
        assert_eq!(branches.trim(), "ants/dev-a/a", "a branch with only upstream commits is deleted");
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn an_upstream_base_falls_back_to_head() {
        let Some(repo) = repo("worktrees-upstream-fallback") else { return };
        let home = repo.parent().unwrap().join("home");
        commit(&repo, "local work");
        let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap().with_base(Base::Upstream);
        let path = worktrees.checkout(&thread("a")).await.unwrap();
        assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "local work", "no remote");

        let (remote, other) = with_remote(&repo, false);
        commit(&other, "upstream work");
        run(&other, &["push", "--quiet", "origin", "main"]);
        let path = worktrees.checkout(&thread("b")).await.unwrap();
        let shown = "the remote has no HEAD and main no upstream";
        assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "local work", "{shown}");

        run(&repo, &["branch", "--quiet", "--set-upstream-to=origin/main"]);
        let path = worktrees.checkout(&thread("c")).await.unwrap();
        assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "upstream work", "main's upstream instead");

        std::fs::remove_dir_all(&remote).unwrap();
        let path = worktrees.checkout(&thread("d")).await.unwrap();
        assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "local work", "the fetch fails");
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }
}
