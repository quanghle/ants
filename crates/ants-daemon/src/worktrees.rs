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
//! where their sessions are.
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

use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ants_proto::{ThreadId, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use tokio::sync::Mutex;

use self::repository::{Repository, repository};
use crate::process::{git, run};
use crate::sync::MutexExt as _;

mod branches;
mod policy;
mod repository;

pub use self::branches::Branch;
pub(crate) use self::branches::branch_name;
pub use self::policy::{per_thread, shares_git_workdir};

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
    /// The threads that started in the workdir, the repository having no commit then, and stay there.
    sharing: std::sync::Mutex<HashSet<ThreadId>>,
    /// A directory per thread.
    dir: PathBuf,
    /// One change to the worktrees at a time, so a thread's worktree is never removed while it is being set up.
    changing: Mutex<()>,
    /// What orders the changes to the repository's worktrees and branches, and its fetches, shared by every ant of the
    /// daemon working in it ([`repository::repository`]). Its [`repository::Changes`] are taken after
    /// [`Worktrees::changing`], never before.
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
            sharing: std::sync::Mutex::default(),
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
    /// for a thread starting while the repository has no commit, and from then on for that thread.
    pub async fn checkout(&self, thread: &ThreadId) -> Result<PathBuf> {
        let _changing = self.changing.lock().await;
        let path = self.path(thread);
        if !path.join(".git").is_file() {
            if self.shares(thread).await {
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

    /// Whether the thread works in the workdir: it started there, or starts now and the repository still has no commit.
    async fn shares(&self, thread: &ThreadId) -> bool {
        if !self.born.load(Ordering::Relaxed) {
            if !has_commit(&self.repo).await {
                if self.sharing.locked().insert(thread.clone()) {
                    tracing::info!(%thread, workdir = %self.workdir.display(), "the thread works in the shared workdir");
                }
                return true;
            }
            self.born.store(true, Ordering::Relaxed);
            tracing::info!(
                repo = %self.repo.display(),
                sharing = self.sharing.locked().len(),
                "the repository has a commit now: giving each new thread its own git worktree"
            );
        }
        self.sharing.locked().contains(thread)
    }

    /// The threads working in the workdir, as the repository had no commit when they started.
    pub fn sharing(&self) -> Vec<ThreadId> {
        let mut threads: Vec<ThreadId> = self.sharing.locked().iter().cloned().collect();
        threads.sort();
        threads
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
            match self.delete_branch(&branch).await {
                Ok(()) => tracing::info!(%thread, %branch, "deleted the thread's branch left without a worktree"),
                Err(e) => tracing::warn!(%thread, %branch, error = format!("{e:#}"), "deleting the thread's branch"),
            }
        }
        pruned
    }

    /// The repository the worktrees belong to.
    pub fn repo(&self) -> &Path {
        &self.repo
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
            if let Err(e) = self.delete_branch(&branch).await {
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

/// Whether the repository `dir` is in has a commit at its `HEAD`.
async fn has_commit(dir: &Path) -> bool {
    git(dir, ["rev-parse", "--verify", "--quiet", "HEAD^{commit}"]).await.is_ok()
}

/// The threads with a directory in `dir`.
fn threads_in(dir: &Path) -> Vec<ThreadId> {
    let Ok(entries) = std::fs::read_dir(dir) else { return Vec::new() };
    let mut threads: Vec<ThreadId> =
        entries.flatten().filter_map(|entry| entry.file_name().to_str()?.parse().ok()).collect();
    threads.sort();
    threads
}

#[cfg(test)]
mod tests;
