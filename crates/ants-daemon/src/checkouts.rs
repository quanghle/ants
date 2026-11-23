//! The checkouts a review works in (requirement ants-x39hk.12.4): when a message from an ant narrowed to a review
//! ([`ants_proto::REVIEW_ALLOW`]) arrives, the daemon checks out each commit its packet names
//! ([`Envelope::review_commits`]) under `<home>/reviews/<thread>`, detached, one directory per commit, and the
//! review's session works there, so `read:*` covers the checkouts and nothing else. Each commit is fetched, at depth
//! one, into a git directory of the review's own (`.store`), from the sender's repository when the same one is on this
//! machine, else from `https://<repo>`; one that cannot be fetched is left out, the prompt saying so. The checkouts go
//! once the prompt answering the message ends ([`Prepared`]), and any left from an earlier run when the daemon starts.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{Envelope, ReviewCommit, ThreadId};
use anyhow::{Result, bail};
use tokio::time::Instant;

use crate::worktrees::FETCH_TIMEOUT;

/// The most commits a review checks out.
pub const MAX_CHECKOUTS: usize = 16;
/// How long checking out a review's commits may take in all.
const PREPARE_WITHIN: Duration = Duration::from_secs(180);
/// How long a git command on the review's own git directory may take.
const GIT_TIMEOUT: Duration = Duration::from_secs(60);
/// The review's git directory, beside its checkouts.
const STORE: &str = ".store";

/// The reviews' checkouts under an ant's home.
#[derive(Debug, Clone)]
pub struct Checkouts {
    dir: PathBuf,
}

impl Checkouts {
    /// The checkouts under `home`.
    pub fn new(home: &Path) -> Self {
        Self { dir: home.join("reviews") }
    }

    /// Remove the checkouts left by an earlier run: a review still to answer checks them out again.
    pub fn clear(&self) {
        if let Err(e) = std::fs::remove_dir_all(&self.dir)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(dir = %self.dir.display(), error = %e, "removing the review checkouts left behind");
        }
    }

    /// Where the session of `thread` works: its review's directory, while it has one.
    pub fn workdir(&self, thread: &ThreadId) -> Option<PathBuf> {
        Some(self.review_dir(thread)).filter(|dir| dir.is_dir())
    }

    /// Check out the commits `envelope`, a message from an ant narrowed to a review, names, and tell where.
    pub async fn prepare(&self, envelope: &Envelope) -> Prepared {
        let thread = &envelope.thread_id;
        let dir = self.review_dir(thread);
        let mut prepared = Prepared { dir: dir.clone(), note: String::new() };
        let store = match init(&dir).await {
            Ok(store) => Some(store),
            Err(e) => {
                tracing::warn!(%thread, error = format!("{e:#}"), "setting up the review's checkouts");
                None
            }
        };
        let deadline = Instant::now() + PREPARE_WITHIN;
        let mut done: BTreeMap<&str, Result<PathBuf, &'static str>> = BTreeMap::new();
        let mut lines = Vec::new();
        for commit in &envelope.review_commits {
            let at = match done.get(commit.commit.as_str()) {
                Some(at) => at.clone(),
                None => {
                    let at = match &store {
                        None => Err("the review's git directory could not be set up"),
                        Some(_) if !is_commit_id(&commit.commit) => Err("not a full commit id"),
                        Some(_) if done.len() >= MAX_CHECKOUTS => Err("past the most commits a review checks out"),
                        Some(store) => check_out(store, &dir, commit, deadline).await.map_err(|e| {
                            tracing::warn!(%thread, commit = commit.commit, error = format!("{e:#}"), "checking out");
                            "its repository could not be reached or does not hold it"
                        }),
                    };
                    done.insert(&commit.commit, at.clone());
                    at
                }
            };
            let place = match &commit.thread {
                Some(thread) => format!("the worktree of thread {thread}"),
                None => "the workdir".to_owned(),
            };
            let at = match at {
                Ok(path) => path.display().to_string(),
                Err(why) => format!("no checkout available, {why}"),
            };
            lines.push(format!("- {} at {}, in {place}: {at}", commit.repo, commit.commit));
        }
        prepared.note = if lines.is_empty() {
            "## Checkouts\n\nNone: the review names no commits.".to_owned()
        } else {
            format!(
                "## Checkouts\n\nThe commits above are checked out for this review, detached, each in a directory of \
                 its own under {}; read code only there.\n\n{}",
                dir.display(),
                lines.join("\n")
            )
        };
        let checked_out = done.values().filter(|at| at.is_ok()).count();
        tracing::info!(%thread, checked_out, commits = done.len(), "checked out the review's commits");
        prepared
    }

    /// The directory of the checkouts of the review on `thread`, there or not.
    pub(crate) fn review_dir(&self, thread: &ThreadId) -> PathBuf {
        self.dir.join(thread.as_str())
    }
}

/// A review's checkouts, removed when dropped.
#[derive(Debug)]
#[must_use]
pub struct Prepared {
    dir: PathBuf,
    note: String,
}

impl Prepared {
    /// What the prompt tells of the checkouts: where each commit is, or why it is not.
    pub fn note(&self) -> &str {
        &self.note
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => tracing::info!(dir = %self.dir.display(), "removed the review's checkouts"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(dir = %self.dir.display(), error = %e, "removing the review's checkouts"),
        }
    }
}

/// Create the review directory `dir` with its git directory, unless there, and return the git directory.
async fn init(dir: &Path) -> Result<PathBuf> {
    let store = dir.join(STORE);
    if !store.join("HEAD").is_file() {
        std::fs::create_dir_all(dir)?;
        let args = [OsStr::new("init"), "--bare".as_ref(), "--quiet".as_ref(), store.as_os_str()];
        crate::worktrees::run("git", dir, args, &[], Some(GIT_TIMEOUT)).await?;
    }
    Ok(store)
}

/// Check `commit` out in `dir`, fetching it into `store` first unless there, and return where.
async fn check_out(store: &Path, dir: &Path, commit: &ReviewCommit, deadline: Instant) -> Result<PathBuf> {
    let id = commit.commit.as_str();
    let path = dir.join(&id[..12]);
    if path.join(".git").is_file() {
        return Ok(path);
    }
    if !holds(store, id).await {
        fetch(store, commit, deadline).await?;
    }
    let args = [OsStr::new("worktree"), "add".as_ref(), "--quiet".as_ref(), "--detach".as_ref(), path.as_ref()];
    // Large files stay pointers: the review's git directory has no remote to fetch them from.
    git(store, args.into_iter().chain([OsStr::new(id)]), &[("GIT_LFS_SKIP_SMUDGE", "1")], GIT_TIMEOUT).await?;
    Ok(path)
}

/// Fetch `commit` into `store` from the first of its sources that has it, within `deadline`.
async fn fetch(store: &Path, commit: &ReviewCommit, deadline: Instant) -> Result<()> {
    let mut failed = Vec::new();
    for source in sources(commit) {
        let left = deadline.saturating_duration_since(Instant::now()).min(FETCH_TIMEOUT);
        if left.is_zero() {
            failed.push("no time left".to_owned());
            break;
        }
        let args = ["fetch", "--quiet", "--no-tags", "--depth", "1", &source, &commit.commit];
        match git(store, args, &[("GIT_TERMINAL_PROMPT", "0")], left).await {
            Ok(()) if holds(store, &commit.commit).await => return Ok(()),
            Ok(()) => failed.push(format!("{source}: the fetch brought no commit")),
            Err(e) => failed.push(format!("{e:#}")),
        }
    }
    if failed.is_empty() {
        bail!("no source to fetch {} from", commit.repo);
    }
    bail!("{}", failed.join("; "))
}

/// Where `commit` may be fetched from: the sender's repository, when the same one is on this machine, then the
/// repository's remote over HTTPS, when its name makes a URL.
fn sources(commit: &ReviewCommit) -> Vec<String> {
    let local = commit.local.as_deref().filter(|local| {
        let path = Path::new(local);
        path.is_absolute()
            && path.is_dir()
            && crate::provider::context::repository(path).as_deref() == Some(commit.repo.as_str())
    });
    let remote = is_remote_name(&commit.repo).then(|| format!("https://{}", commit.repo));
    local.map(str::to_owned).into_iter().chain(remote).collect()
}

/// Whether `store` holds commit `id`.
async fn holds(store: &Path, id: &str) -> bool {
    git(store, ["cat-file", "-e", &format!("{id}^{{commit}}")], &[], GIT_TIMEOUT).await.is_ok()
}

/// Run git on `store`, named explicitly, as a bare git directory must be where git finds no bare one on its own.
async fn git<I, S>(store: &Path, args: I, env: &[(&str, &str)], timeout: Duration) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let named = [OsStr::new("--git-dir"), store.as_os_str()];
    let args: Vec<_> =
        named.into_iter().map(OsStr::to_owned).chain(args.into_iter().map(|a| a.as_ref().to_owned())).collect();
    crate::worktrees::run("git", store, args, env, Some(timeout)).await.map(drop)
}

/// Whether `id` is a full commit id: 40 or 64 lowercase hexadecimal digits.
fn is_commit_id(id: &str) -> bool {
    matches!(id.len(), 40 | 64) && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Whether `repo`, a normalized remote, names a host and path safe to put in an HTTPS URL.
fn is_remote_name(repo: &str) -> bool {
    let Some((host, path)) = repo.split_once('/') else { return false };
    let host_ok = !host.is_empty() && host.bytes().all(|b| b.is_ascii_alphanumeric() || b"-.".contains(&b));
    let segment_ok = |segment: &str| {
        !matches!(segment, "" | "." | "..")
            && segment.bytes().all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
    };
    host_ok && !host.starts_with(['-', '.']) && path.split('/').all(segment_ok)
}

#[cfg(test)]
mod tests;
