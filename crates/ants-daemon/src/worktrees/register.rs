//! A thread's worktree on a new branch, registered with the repository as `git worktree add` registers one, without
//! git reading every worktree the repository has first: `git worktree add` takes about 0.15 ms longer for each, which
//! thousands of threads in one repository would pay at each new one.
//!
//! The registration is the files git writes in `<common dir>/worktrees/<name>`: `gitdir`, naming the worktree's `.git`
//! file, `commondir`, `HEAD` on the new branch and `locked` while it is made, so no `git worktree prune` takes it for
//! gone. One `git checkout -b` then makes the branch, checks it out and runs the repository's `post-checkout` hook, as
//! `git worktree add` does, and the worktree's `.git` file is written last: a worktree whose making was cut short has
//! none, and is made again, its registration replaced. Git reads such a worktree as any other (`git worktree list`,
//! `remove`, `prune`). A repository whose refs are not files (reftable), with per-worktree config or a sparse checkout,
//! which `git worktree add` copies, gets its worktrees from `git worktree add` instead, as does any on Windows
//! ([`by_hand`]).

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};

use super::Worktrees;
use crate::process::{git, run};

/// What git reads as a worktree cut short while being made, as by a daemon killed: `git worktree prune` leaves it.
const LOCKED: &str = "initializing";

impl Worktrees {
    /// Make the worktree `path` of the thread on its new branch `branch`, started from `start`. Nothing is left of it
    /// but the branch when that fails, or does not end within [`Worktrees::add_timeout`].
    pub(super) async fn register(&self, path: &Path, branch: &str, start: &str) -> Result<()> {
        let commit = format!("{start}^{{commit}}");
        let commit = git(&self.repo, ["rev-parse", "--verify", "--end-of-options", &commit]).await?;
        std::fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
        let path = path.canonicalize().with_context(|| format!("finding {}", path.display()))?;
        let admin = match self.registration(&path, branch) {
            Ok(admin) => admin,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&path);
                return Err(e);
            }
        };
        let made = async {
            let checkout = ["checkout", "--quiet", "--no-track", "-b", branch, commit.trim()];
            let dirs = [arg("--git-dir=", &admin), arg("--work-tree=", &path)];
            let args = dirs.into_iter().chain(checkout.map(Into::into));
            run("git", &path, args, &[], Some(self.add_timeout)).await?;
            std::fs::remove_file(admin.join("locked")).context("unlocking the worktree")?;
            std::fs::write(path.join(".git"), format!("gitdir: {}\n", admin.display()))
                .with_context(|| format!("writing {}", path.join(".git").display()))
        };
        let made = made.await;
        if made.is_err() {
            let _ = std::fs::remove_dir_all(&admin);
            let _ = std::fs::remove_dir_all(&path);
        }
        made
    }

    /// Register the worktree `path` on `branch`, locked, in the first directory of `<common dir>/worktrees` free under
    /// a name git would give it, and return that directory.
    fn registration(&self, path: &Path, branch: &str) -> Result<PathBuf> {
        let worktrees = self.common.join("worktrees");
        std::fs::create_dir_all(&worktrees).with_context(|| format!("creating {}", worktrees.display()))?;
        let name = path.file_name().context("a worktree's directory has a name")?.to_string_lossy();
        let mut admin = PathBuf::new();
        for i in 0.. {
            admin = worktrees.join(if i == 0 { name.to_string() } else { format!("{name}{i}") });
            match std::fs::create_dir(&admin) {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e).with_context(|| format!("creating {}", admin.display())),
            }
        }
        let write = |file: &str, text: String| {
            let file = admin.join(file);
            std::fs::write(&file, text).with_context(|| format!("writing {}", file.display()))
        };
        let written = write("gitdir", format!("{}\n", path.join(".git").display()))
            .and_then(|()| write("locked", format!("{LOCKED}\n")))
            .and_then(|()| write("commondir", "../..\n".to_owned()))
            .and_then(|()| write("HEAD", format!("ref: refs/heads/{branch}\n")))
            .and_then(|()| std::fs::create_dir(admin.join("refs")).context("creating the worktree's refs"));
        if let Err(e) = written {
            let _ = std::fs::remove_dir_all(&admin);
            return Err(e);
        }
        Ok(admin)
    }

    /// Forget the registrations of the worktree `path`, which has no `.git` file, left by its making cut short: those
    /// under a name git would give it that tell of `path`, locked or not.
    pub(super) fn forget_cut_short(&self, path: &Path) {
        let Some(name) = path.file_name().map(|name| name.to_string_lossy()) else { return };
        let git_file = path.join(".git");
        let worktrees = self.common.join("worktrees");
        for i in 0.. {
            let admin = worktrees.join(if i == 0 { name.to_string() } else { format!("{name}{i}") });
            if !admin.is_dir() {
                return;
            }
            let Ok(gitdir) = std::fs::read_to_string(admin.join("gitdir")) else { continue };
            if Path::new(gitdir.trim()) == git_file {
                tracing::info!(registration = %admin.display(), "forgetting a worktree whose making was cut short");
                if let Err(e) = std::fs::remove_dir_all(&admin) {
                    tracing::warn!(registration = %admin.display(), error = %e, "forgetting a cut short worktree");
                }
            }
        }
    }
}

/// Whether the worktrees of the repository `repo` is in, whose git directory is `git_dir`, are registered by hand
/// rather than by `git worktree add`: unless its refs are not files, it has per-worktree config or a sparse checkout,
/// or git cannot tell. Never on Windows, where git writes the paths of a registration its own way.
pub(super) async fn by_hand(repo: &Path, git_dir: &Path) -> bool {
    if cfg!(windows) {
        return false;
    }
    let refs = git(repo, ["config", "--default=files", "extensions.refStorage"]).await;
    let per_worktree = git(repo, ["config", "--type=bool", "--default=false", "extensions.worktreeConfig"]).await;
    refs.is_ok_and(|refs| refs.trim().eq_ignore_ascii_case("files"))
        && per_worktree.is_ok_and(|on| on.trim() == "false")
        && !git_dir.join("info/sparse-checkout").exists()
}

/// `flag` followed by `path`, as one argument.
fn arg(flag: &str, path: &Path) -> std::ffi::OsString {
    let mut arg = std::ffi::OsString::from(flag);
    arg.push(path);
    arg
}
