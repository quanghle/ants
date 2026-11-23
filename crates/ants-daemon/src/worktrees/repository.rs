//! The order of the git commands the daemon's ants run in one repository they share.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};

use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use super::ADDS_AT_ONCE;
use crate::sync::MutexExt as _;

/// What orders the git commands of the daemon's ants in one repository.
#[derive(Default)]
pub(super) struct Repository {
    /// The changes to its worktrees and branches.
    pub(super) changes: Changes,
    /// One fetch at a time from its remote, never taken while changing it.
    pub(super) fetching: Mutex<()>,
}

/// The changes to one repository's worktrees and branches: [`ADDS_AT_ONCE`] adds at a time, any other change alone.
/// Whoever asked first goes first: a removal waits for the adds going on, and the adds asked for after it wait for it.
pub(super) struct Changes(Semaphore);

impl Default for Changes {
    fn default() -> Self {
        Self(Semaphore::new(ADDS_AT_ONCE as usize))
    }
}

impl Changes {
    /// Held while adding a worktree.
    pub(super) async fn adding(&self) -> Option<SemaphorePermit<'_>> {
        self.0.acquire().await.ok()
    }

    /// Held for a change that no other may run beside.
    pub(super) async fn alone(&self) -> Option<SemaphorePermit<'_>> {
        self.0.acquire_many(ADDS_AT_ONCE).await.ok()
    }
}

/// The [`Repository`] whose git common directory is `common`, the same for every ant of the daemon.
pub(super) fn repository(common: &Path) -> Arc<Repository> {
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
