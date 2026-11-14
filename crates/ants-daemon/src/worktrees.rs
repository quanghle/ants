//! A git worktree per thread, so the threads of one ant running at once do not edit and build in the same checkout.
//!
//! Each thread works in `<home>/worktrees/<thread>`, a worktree of the repository the ant's workdir is in, on its own
//! branch `ants/<ant>/<thread>` started from the repository's `HEAD`. A branch rather than a detached `HEAD` keeps
//! what the thread committed reachable after its worktree is removed, and two worktrees never need the same branch.
//! The worktree is created on the thread's first prompt, reused whenever its harness starts again (so session/load
//! sees the same directory) and removed once the thread's session expired from the session store, along with its
//! branch unless that holds commits not merged into the repository's `HEAD`.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use ants_proto::{ThreadId, UserId};
use anyhow::{Context as _, Result, bail};
use tokio::sync::Mutex;

pub struct Worktrees {
    ant: UserId,
    /// Top level of the main worktree of the repository the workdir is in.
    repo: PathBuf,
    /// Where the workdir is inside the repository, so a thread works at the same place in its own worktree.
    within: PathBuf,
    /// A directory per thread.
    dir: PathBuf,
    /// One change to the worktrees at a time, so a thread's worktree is never removed while it is being set up.
    changing: Mutex<()>,
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
                Some(Self { ant: ant.clone(), repo, within, dir, changing: Mutex::new(()) })
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

    /// Remove the worktree of each thread `expired` tells, one at a time, and return how many were removed. A thread
    /// whose worktree is being set up waits for the removal to end, and `expired` is asked with no change going on.
    pub async fn prune(&self, mut expired: impl AsyncFnMut(&ThreadId) -> bool) -> usize {
        let mut removed = 0;
        for thread in self.threads() {
            let _changing = self.changing.lock().await;
            if !expired(&thread).await {
                continue;
            }
            match self.remove(&thread).await {
                Ok(()) => removed += 1,
                Err(e) => tracing::warn!(%thread, error = format!("{e:#}"), "removing the thread's git worktree"),
            }
        }
        removed
    }

    /// The threads with a worktree.
    pub fn threads(&self) -> Vec<ThreadId> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else { return Vec::new() };
        let mut threads: Vec<ThreadId> =
            entries.flatten().filter_map(|entry| entry.file_name().to_str()?.parse().ok()).collect();
        threads.sort();
        threads
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
            let args = ["worktree", "add", "--quiet", "-b", &branch].map(OsStr::new);
            git(&self.repo, args.into_iter().chain([path, OsStr::new("HEAD")])).await?;
        }
        Ok(())
    }

    async fn remove(&self, thread: &ThreadId) -> Result<()> {
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
        let branch = self.branch(thread);
        if let Err(e) = git(&self.repo, ["branch", "--delete", "--quiet", &branch]).await {
            tracing::info!(%thread, %branch, error = format!("{e:#}"), "keeping the thread's branch");
        }
        tracing::info!(%thread, worktree = %path.display(), "removed the git worktree of the expired thread");
        Ok(())
    }
}

/// Run git in `dir` and return what it printed, or fail with what it printed on stderr.
async fn git<I, S>(dir: &Path, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args: Vec<S> = args.into_iter().collect();
    let shown = || args.iter().map(|a| a.as_ref().to_string_lossy()).collect::<Vec<_>>().join(" ");
    let output = tokio::process::Command::new("git")
        .args(&args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("running git {}", shown()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {}: {}", shown(), stderr.trim());
    }
    String::from_utf8(output.stdout).with_context(|| format!("git {}: output is no UTF-8", shown()))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::process::Command;

    use super::*;

    /// A repository with one commit and a `sub` directory, or None when git is missing.
    pub(crate) fn repo(name: &str) -> Option<PathBuf> {
        if !ants_test_support::have("git") {
            return None;
        }
        let dir = ants_test_support::scratch_dir(name).canonicalize().unwrap();
        let repo = dir.join("repo");
        std::fs::create_dir_all(repo.join("sub")).unwrap();
        std::fs::write(repo.join("sub/file"), "one").unwrap();
        run(&repo, &["init", "--quiet", "--initial-branch=main"]);
        run(&repo, &["add", "."]);
        commit(&repo, "first");
        Some(repo)
    }

    pub(crate) fn run(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git").args(args).current_dir(dir).output().unwrap();
        assert!(output.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&output.stderr));
        String::from_utf8(output.stdout).unwrap()
    }

    fn commit(dir: &Path, message: &str) {
        run(dir, &["-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "--allow-empty", "-am", message]);
    }

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
        let removed = worktrees.prune(async |t: &ThreadId| *t != c).await;
        assert_eq!(removed, 2);
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
}
