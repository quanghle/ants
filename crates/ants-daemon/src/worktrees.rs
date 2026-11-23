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
//!
//! A repository with no commit has nothing to start a branch from: its threads work in the workdir, shared, until it
//! has one. From then on each new thread gets its worktree, while the threads that started in the workdir stay there,
//! where their sessions are: the pool asks for no worktree for a thread whose stored session worked in the workdir
//! (`crate::pool`).
//!
//! Ants of one daemon may share a repository. Git does not take a `git worktree prune` beside a `git worktree add` on
//! one repository (the prune removes the add's half-made registration), so the removals, prunes and branch deletions
//! of a repository go alone across the daemon, by its git common directory. Adds do run beside each other,
//! [`ADDS_AT_ONCE`] at a time: git takes the longer for one the more worktrees the repository has, which a burst of
//! new threads of many ants would otherwise wait for one after the other. An add that fails anyway, as with another
//! process's git racing it, is tried once more, alone, and a branch made by an add that failed is deleted; an add git
//! does not finish within [`ADD_TIMEOUT`] is stopped, and failed. Fetches of one repository go one at a time, apart
//! from those changes, so they do not clash on the remote's refs while other ants' worktrees need not wait for the
//! remote. A branch of the ant left without a worktree, as by a daemon killed while adding one, is deleted once its
//! thread has no session, unless it holds commits.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};

use ants_proto::{ThreadId, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use crate::sync::MutexExt as _;

/// Longest a git fetch for a thread's new branch may take.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(60);

/// Longest one `git worktree add` may take, its checkout and the repository's `post-checkout` hook included.
pub const ADD_TIMEOUT: Duration = Duration::from_secs(120);

/// How many `git worktree add` run at once in one repository, across the daemon's ants.
pub const ADDS_AT_ONCE: u32 = 4;

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

/// A thread's worktree that could not be made, the context of the error: the thread's prompt does not run, rather than
/// run in the workdir live threads share, and its message is tried again as after a failed attempt. Nothing ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorktreeFailed;

impl std::fmt::Display for WorktreeFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("creating the thread's git worktree")
    }
}

impl std::error::Error for WorktreeFailed {}

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
    /// The ant's workdir, where threads work while the repository has no commit.
    workdir: PathBuf,
    /// Whether the repository was seen with a commit: looked for at each new thread until there is one.
    born: AtomicBool,
    /// A directory per thread.
    dir: PathBuf,
    /// One change to the worktrees at a time, so a thread's worktree is never removed while it is being set up.
    changing: Mutex<()>,
    /// What orders the changes to the repository's worktrees and branches, and its fetches, shared by every ant of the
    /// daemon working in it ([`repository`]). Its [`Changes`] are taken after [`Worktrees::changing`], never before.
    repository: Arc<Repository>,
    /// Where a thread's new branch starts.
    base: Base,
    /// Longest one `git worktree add` may take.
    add_timeout: Duration,
}

impl Worktrees {
    /// The worktrees of the ant's threads under `home`, of the repository `workdir` is in. None, with a warning, when
    /// it is in none or git cannot be run: the threads then share the workdir. So do, with a warning, those starting
    /// while the repository has no commit to start a branch from ([`Worktrees::checkout`]).
    pub async fn find(ant: &UserId, workdir: &Path, home: &Path) -> Option<Self> {
        match git(workdir, ["rev-parse", "--show-toplevel", "--show-prefix"]).await {
            Ok(out) => {
                let mut lines = out.lines();
                let repo = PathBuf::from(lines.next().unwrap_or_default());
                let within = Path::new(lines.next().unwrap_or_default()).components().collect();
                let born = has_commit(workdir).await;
                if born {
                    tracing::info!(repo = %repo.display(), "giving each thread its own git worktree");
                } else {
                    tracing::warn!(
                        workdir = %workdir.display(),
                        "the workdir's git repository has no commits yet; threads share the workdir until it has one"
                    );
                }
                let dir = home.join("worktrees");
                let common = match git(workdir, ["rev-parse", "--git-common-dir"]).await {
                    Ok(common) => workdir.join(common.trim()),
                    Err(e) => {
                        tracing::warn!(error = format!("{e:#}"), "finding the repository's git directory");
                        repo.join(".git")
                    }
                };
                let worktrees = Self::new(ant, repo, within, dir, &common);
                Some(Self { workdir: workdir.to_owned(), born: AtomicBool::new(born), ..worktrees })
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
                let common = repo.clone();
                return Some(Self::new(ant, repo, PathBuf::new(), dir, &common));
            }
        }
        tracing::warn!(worktrees = %dir.display(), "keeping the thread worktrees left in no git repository");
        None
    }

    fn new(ant: &UserId, repo: PathBuf, within: PathBuf, dir: PathBuf, common: &Path) -> Self {
        Self {
            ant: ant.clone(),
            repo,
            within,
            workdir: PathBuf::new(),
            born: AtomicBool::new(true),
            dir,
            changing: Mutex::new(()),
            repository: repository(common),
            base: Base::Head,
            add_timeout: ADD_TIMEOUT,
        }
    }

    /// The same, starting a thread's new branch from `base`, which also tells what the commits of a branch are compared
    /// with ([`Worktrees::ahead`]).
    #[must_use]
    pub fn with_base(self, base: Base) -> Self {
        Self { base, ..self }
    }

    /// The directory the thread works in, in its worktree, which is created when missing. The workdir instead, shared,
    /// while the repository has no commit: whoever asks keeps a thread that started there in it.
    pub async fn checkout(&self, thread: &ThreadId) -> Result<PathBuf> {
        let _changing = self.changing.lock().await;
        let path = self.path(thread);
        if !path.join(".git").is_file() {
            if self.unborn().await {
                tracing::info!(%thread, workdir = %self.workdir.display(), "the thread works in the shared workdir");
                return Ok(self.workdir.clone());
            }
            let began = Instant::now();
            self.add(thread, &path).await?;
            tracing::info!(
                %thread,
                worktree = %path.display(),
                took_ms = began.elapsed().as_millis(),
                "created the thread's git worktree"
            );
        }
        let cwd = if self.within.as_os_str().is_empty() { path } else { path.join(&self.within) };
        if !cwd.is_dir() {
            bail!("{} is not in the thread's worktree", self.within.display());
        }
        Ok(cwd)
    }

    /// Whether the repository still has no commit, looked for until there is one.
    async fn unborn(&self) -> bool {
        if self.born.load(Ordering::Relaxed) {
            return false;
        }
        if !has_commit(&self.repo).await {
            return true;
        }
        self.born.store(true, Ordering::Relaxed);
        tracing::info!(
            repo = %self.repo.display(),
            "the repository has a commit now: giving each new thread its own git worktree"
        );
        false
    }

    /// Remove the worktree of each thread `expired` tells, one at a time, and tell how many were removed and which
    /// branches were kept. A thread whose worktree is being set up waits for the removal to end, and `expired` is asked
    /// with no change going on. A branch of the ant without a worktree that holds no commits goes too once `expired`
    /// tells its thread: one left by an add cut short, as when the daemon was killed.
    pub async fn prune(&self, mut expired: impl AsyncFnMut(&ThreadId) -> bool) -> Pruned {
        let mut pruned = Pruned::default();
        let threads = self.threads();
        for thread in &threads {
            let _changing = self.changing.lock().await;
            if !expired(thread).await {
                continue;
            }
            match self.remove(thread).await {
                Ok(kept) => {
                    pruned.removed += 1;
                    pruned.kept.extend(kept);
                }
                Err(e) => tracing::warn!(%thread, error = format!("{e:#}"), "removing the thread's git worktree"),
            }
        }
        for thread in self.branched().await.into_iter().filter(|t| !threads.contains(t)) {
            let _changing = self.changing.lock().await;
            // Counted first, so a branch kept for its commits is never taken for an expired thread again.
            if self.path(&thread).exists() || !self.count(&thread).await.is_ok_and(|b| b.commits == 0) {
                continue;
            }
            if !expired(&thread).await {
                continue;
            }
            let branch = self.branch(&thread);
            let _alone = self.repository.changes.alone().await;
            match git(&self.repo, ["branch", "--delete", "--force", "--quiet", &branch]).await {
                Ok(_) => tracing::info!(%thread, %branch, "deleted the thread's branch left without a worktree"),
                Err(e) => tracing::warn!(%thread, %branch, error = format!("{e:#}"), "deleting the thread's branch"),
            }
        }
        pruned
    }

    /// The threads of the ant with a branch in the repository.
    async fn branched(&self) -> Vec<ThreadId> {
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
        let _fetching = self.repository.fetching.lock().await;
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
        let _alone = self.repository.changes.alone().await;
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
        branch_name(&self.ant, thread)
    }

    /// Create the thread's worktree at `path`, on its branch, made when missing. Tried again once failed, alone, after
    /// forgetting the worktrees whose directory is gone; a branch the add made is deleted when that fails too.
    async fn add(&self, thread: &ThreadId, path: &Path) -> Result<()> {
        let branch = self.branch(thread);
        let existed = self.has_branch(&branch).await;
        // Fetched before the repository is taken, so other ants' worktrees need not wait for the remote.
        let start = if existed { None } else { Some(self.start(thread).await) };
        let start = start.as_deref().unwrap_or("HEAD");
        let first = {
            let _adding = self.repository.changes.adding().await;
            self.try_add(path, &branch, (!existed).then_some(start)).await
        };
        let Err(first) = first else { return Ok(()) };
        tracing::info!(%thread, error = format!("{first:#}"), "creating the thread's git worktree again");
        // Alone, as the prune would take the half-made worktrees of other adds for gone.
        let _alone = self.repository.changes.alone().await;
        let retried = async {
            // Forget worktrees whose directory is gone, as what the failed add left, which git would not add again.
            if path.exists() {
                std::fs::remove_dir_all(path).with_context(|| format!("removing {}", path.display()))?;
            }
            git(&self.repo, ["worktree", "prune"]).await?;
            let exists = self.has_branch(&branch).await;
            self.try_add(path, &branch, (!exists).then_some(start)).await
        };
        let Err(e) = retried.await else { return Ok(()) };
        if path.exists() {
            let _ = std::fs::remove_dir_all(path);
        }
        if !existed && self.has_branch(&branch).await {
            // Left by the failed add, the branch would otherwise outlive the thread: no worktree tells of it.
            let _ = git(&self.repo, ["worktree", "prune"]).await;
            if let Err(e) = git(&self.repo, ["branch", "--delete", "--force", "--quiet", &branch]).await {
                tracing::warn!(%thread, %branch, error = format!("{e:#}"), "deleting the branch of a failed worktree");
            }
        }
        Err(e)
    }

    /// One attempt at [`Worktrees::add`]: on `branch`, or a new branch started from `new` when given. Fails when git
    /// does not finish within [`Worktrees::add_timeout`].
    async fn try_add(&self, path: &Path, branch: &str, new: Option<&str>) -> Result<()> {
        // Left over from an add that failed half-way: it is no worktree, and in the way of a new one.
        if path.exists() {
            std::fs::remove_dir_all(path).with_context(|| format!("removing {}", path.display()))?;
        }
        std::fs::create_dir_all(&self.dir).with_context(|| format!("creating {}", self.dir.display()))?;
        let path = path.as_os_str();
        let args: Vec<&OsStr> = match new {
            None => vec!["worktree".as_ref(), "add".as_ref(), "--quiet".as_ref(), path, branch.as_ref()],
            Some(start) => {
                // No upstream for the branch, which starting from a remote branch would otherwise set. Forced, which
                // spares git looking through every worktree of the repository for one with the branch checked out:
                // none has a branch this add makes.
                let args = ["worktree", "add", "--quiet", "--force", "--no-track", "-b", branch].map(OsStr::new);
                args.into_iter().chain([path, OsStr::new(start)]).collect()
            }
        };
        run("git", &self.repo, args, &[], Some(self.add_timeout)).await.map(drop)
    }

    async fn has_branch(&self, branch: &str) -> bool {
        git(&self.repo, ["rev-parse", "--verify", "--quiet", &format!("refs/heads/{branch}")]).await.is_ok()
    }

    /// Remove the thread's worktree, and its branch unless that holds commits not in the repository's `HEAD`: then the
    /// branch kept.
    async fn remove(&self, thread: &ThreadId) -> Result<Option<Branch>> {
        let path = self.path(thread);
        let _alone = self.repository.changes.alone().await;
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

/// What orders the git commands of the daemon's ants in one repository.
#[derive(Default)]
struct Repository {
    /// The changes to its worktrees and branches.
    changes: Changes,
    /// One fetch at a time from its remote, never taken while changing it.
    fetching: Mutex<()>,
}

/// The changes to one repository's worktrees and branches: [`ADDS_AT_ONCE`] adds at a time, any other change alone.
/// Whoever asked first goes first: a removal waits for the adds going on, and the adds asked for after it wait for it.
struct Changes(Semaphore);

impl Default for Changes {
    fn default() -> Self {
        Self(Semaphore::new(ADDS_AT_ONCE as usize))
    }
}

impl Changes {
    /// Held while adding a worktree.
    async fn adding(&self) -> Option<SemaphorePermit<'_>> {
        self.0.acquire().await.ok()
    }

    /// Held for a change that no other may run beside.
    async fn alone(&self) -> Option<SemaphorePermit<'_>> {
        self.0.acquire_many(ADDS_AT_ONCE).await.ok()
    }
}

/// The [`Repository`] whose git common directory is `common`, the same for every ant of the daemon.
fn repository(common: &Path) -> Arc<Repository> {
    static REPOSITORIES: LazyLock<std::sync::Mutex<HashMap<PathBuf, Weak<Repository>>>> =
        LazyLock::new(Default::default);
    let common = common.canonicalize().unwrap_or_else(|_| common.to_owned());
    let mut repositories = REPOSITORIES.locked();
    repositories.retain(|_, repository| repository.strong_count() > 0);
    if let Some(repository) = repositories.get(&common).and_then(Weak::upgrade) {
        return repository;
    }
    let repository = Arc::new(Repository::default());
    repositories.insert(common, Arc::downgrade(&repository));
    repository
}

/// Whether the repository `dir` is in has a commit at its `HEAD`.
async fn has_commit(dir: &Path) -> bool {
    git(dir, ["rev-parse", "--verify", "--quiet", "HEAD^{commit}"]).await.is_ok()
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

/// The branch of `ant`'s worktree of `thread`.
pub(crate) fn branch_name(ant: &UserId, thread: &ThreadId) -> String {
    format!("ants/{ant}/{thread}")
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
    async fn ants_sharing_a_repository_check_out_and_prune_at_once() {
        let Some(repo) = repo("worktrees-shared-repo") else { return };
        let root = repo.parent().unwrap();
        let mut all = Vec::new();
        for i in 0..24 {
            let ant: UserId = format!("dev-{i}").parse().unwrap();
            let workdir = if i % 2 == 0 { repo.clone() } else { repo.join("sub") };
            all.push(Worktrees::find(&ant, &workdir, &root.join(format!("home-{i}"))).await.unwrap());
        }
        let threads: Vec<ThreadId> = (0..3).map(|j| thread(&format!("t{j}"))).collect();
        let checkouts = all.iter().flat_map(|w| threads.iter().map(move |t| w.checkout(t)));
        for dir in futures::future::join_all(checkouts).await {
            let dir = dir.unwrap();
            assert!(dir.join("file").is_file() || dir.join("sub/file").is_file(), "{}", dir.display());
        }
        let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
        assert_eq!(branches.lines().count(), 72);
        let pruned = futures::future::join_all(all.iter().map(|w| w.prune(async |_: &ThreadId| true))).await;
        assert!(pruned.iter().all(|p| *p == Pruned { removed: 3, kept: Vec::new() }), "{pruned:?}");
        assert_eq!(run(&repo, &["branch", "--list", "ants/*"]), "");
        assert_eq!(run(&repo, &["worktree", "list", "--porcelain"]).matches("worktree ").count(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Make `script` the `post-checkout` hook of `repo`, which git runs at the end of each `git worktree add`.
    fn hook(repo: &Path, script: &str) {
        let hook = repo.join(".git/hooks/post-checkout");
        std::fs::write(&hook, format!("#!/bin/sh\n{script}\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[tokio::test]
    async fn a_failed_checkout_leaves_no_branch() {
        let Some(repo) = repo("worktrees-failed") else { return };
        let home = repo.parent().unwrap().join("home");
        hook(&repo, "exit 1");
        let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
        assert!(worktrees.checkout(&thread("a")).await.is_err());
        assert_eq!(run(&repo, &["branch", "--list", "ants/*"]), "", "the branch the add made is gone");
        assert!(!home.join("worktrees/a").exists());
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_add_git_does_not_end_in_time_fails_and_leaves_nothing() {
        let Some(repo) = repo("worktrees-slow") else { return };
        let home = repo.parent().unwrap().join("home");
        hook(&repo, "sleep 30");
        let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
        let hurried = Worktrees { add_timeout: Duration::from_secs(2), ..worktrees };
        let e = hurried.checkout(&thread("a")).await.unwrap_err();
        assert!(format!("{e:#}").contains("no answer within 2s"), "{e:#}");
        assert_eq!(run(&repo, &["branch", "--list", "ants/*"]), "", "the branch the add made is gone");
        assert!(!home.join("worktrees/a").exists());
        std::fs::remove_file(repo.join(".git/hooks/post-checkout")).unwrap();
        let worktrees = Worktrees { add_timeout: ADD_TIMEOUT, ..hurried };
        assert_eq!(worktrees.checkout(&thread("a")).await.unwrap(), home.join("worktrees/a"), "tried again later");
        std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn adds_run_beside_each_other_and_any_other_change_alone() {
        use futures::FutureExt as _;
        let changes = Changes::default();
        let adding = futures::future::join_all((0..ADDS_AT_ONCE).map(|_| changes.adding())).await;
        assert!(changes.adding().now_or_never().is_none(), "no more adds at once");
        let mut alone = std::pin::pin!(changes.alone());
        assert!(futures::poll!(&mut alone).is_pending(), "not beside an add");
        drop(adding);
        assert!(changes.adding().now_or_never().is_none(), "an add asked for later waits for the change");
        let held = alone.await;
        assert!(changes.adding().now_or_never().is_none() && changes.alone().now_or_never().is_none());
        drop(held);
        assert!(changes.adding().now_or_never().is_some());
    }

    #[tokio::test]
    async fn ants_of_one_repository_share_its_changes() {
        let Some(repo) = repo("worktrees-one-repository") else { return };
        let root = repo.parent().unwrap();
        let a = Worktrees::find(&ant(), &repo, &root.join("home-a")).await.unwrap();
        let b = Worktrees::find(&"dev-b".parse().unwrap(), &repo.join("sub"), &root.join("home-b")).await.unwrap();
        assert!(Arc::ptr_eq(&a.repository, &b.repository));
        let _alone = a.repository.changes.alone().await;
        let waiting = tokio::time::timeout(Duration::from_millis(300), b.checkout(&thread("t"))).await;
        assert!(waiting.is_err(), "no add beside another ant's removal");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_branch_left_without_a_worktree_goes_once_its_thread_has_no_session() {
        let Some(repo) = repo("worktrees-orphans") else { return };
        let home = repo.parent().unwrap().join("home");
        let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
        // As left by a daemon killed between git making the branch and the worktree.
        for t in ["a", "b", "c"] {
            run(&repo, &["branch", &format!("ants/dev-a/{t}")]);
        }
        run(&repo, &["branch", "ants/dev-b/a"]);
        run(&repo, &["switch", "--quiet", "ants/dev-a/c"]);
        commit(&repo, "work of c");
        run(&repo, &["switch", "--quiet", "main"]);
        let mut asked = Vec::new();
        let pruned = worktrees
            .prune(async |t: &ThreadId| {
                asked.push(t.clone());
                *t != thread("b")
            })
            .await;
        assert_eq!(pruned, Pruned::default(), "no worktree removed");
        assert_eq!(asked, [thread("a"), thread("b")], "a branch with commits is not taken for an expired thread");
        let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
        assert_eq!(
            branches.lines().collect::<Vec<_>>(),
            ["ants/dev-a/b", "ants/dev-a/c", "ants/dev-b/a"],
            "a's goes; b's thread has a session, c's holds a commit, and another ant's is not this ant's"
        );
        let path = worktrees.checkout(&thread("b")).await.unwrap();
        assert_eq!(run(&path, &["branch", "--show-current"]).trim(), "ants/dev-a/b", "the branch is checked out");
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
    async fn a_repository_without_commits_is_shared_until_it_has_one() {
        if !ants_test_support::have("git") {
            return;
        }
        let dir = ants_test_support::scratch_dir("worktrees-empty").canonicalize().unwrap();
        let (repo, home) = (dir.join("repo"), dir.join("home"));
        std::fs::create_dir_all(repo.join("sub")).unwrap();
        std::fs::write(repo.join("sub/file"), "one").unwrap();
        run(&repo, &["init", "--quiet", "--initial-branch=main"]);
        let top = Worktrees::find(&ant(), &repo, &home).await.unwrap();
        let below = Worktrees::find(&"dev-b".parse().unwrap(), &repo.join("sub"), &dir.join("home-b")).await.unwrap();
        let (a, b, c) = (thread("a"), thread("b"), thread("c"));
        assert_eq!(top.checkout(&a).await.unwrap(), repo, "an unborn HEAD");
        assert_eq!(below.checkout(&a).await.unwrap(), repo.join("sub"), "below the top level");
        assert!(!home.join("worktrees").exists() && run(&repo, &["branch", "--list", "ants/*"]).is_empty());
        assert_eq!(top.prune(async |_: &ThreadId| true).await, Pruned::default());

        run(&repo, &["add", "."]);
        commit(&repo, "first");
        assert_eq!(top.checkout(&b).await.unwrap(), home.join("worktrees/b"), "a new thread, once there is a commit");
        assert_eq!(top.checkout(&c).await.unwrap(), home.join("worktrees/c"));
        assert_eq!(top.threads(), [b, c]);
        assert_eq!(below.checkout(&thread("d")).await.unwrap(), dir.join("home-b/worktrees/d/sub"));
        assert_eq!(top.checkout(&a).await.unwrap(), home.join("worktrees/a"), "only its stored session keeps it out");
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
    async fn ants_sharing_a_repository_fetch_one_at_a_time() {
        let Some(repo) = repo("worktrees-upstream-shared") else { return };
        let root = repo.parent().unwrap().to_owned();
        let (_, other) = with_remote(&repo, true);
        commit(&other, "upstream work");
        run(&other, &["push", "--quiet", "origin", "main"]);
        let mut all = Vec::new();
        for i in 0..16 {
            let ant: UserId = format!("dev-{i}").parse().unwrap();
            let home = root.join(format!("home-{i}"));
            all.push(Worktrees::find(&ant, &repo, &home).await.unwrap().with_base(Base::Upstream));
        }
        let t = thread("t");
        let paths = futures::future::join_all(all.iter().map(|w| w.checkout(&t))).await;
        for path in paths {
            let path = path.unwrap();
            assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "upstream work", "{}", path.display());
        }
        std::fs::remove_dir_all(root).unwrap();
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
