//! What a clone did so far ([`Done`]), and undoing it on a failure.

use std::path::PathBuf;

use ants_proto::{AdminOrder, ThreadId};

use super::worktrees::git;
use crate::pool::SessionStore;
use crate::profile::identity::Identity;
use crate::records::direct_kv::DirectKv;

/// What a clone did so far, to undo on a failure.
#[derive(Default)]
pub(super) struct Done {
    /// The ant cloned was paused by the clone, to resume.
    pub(super) paused: bool,
    pub(super) home: Option<PathBuf>,
    /// The worktrees added: the repository's git directory, the worktree and its branch.
    pub(super) worktrees: Vec<(PathBuf, PathBuf, String)>,
    /// The credentials and key files written beside the host credentials.
    pub(super) files: Vec<PathBuf>,
    /// The key `ants signer` signed.
    pub(super) key: Option<String>,
    pub(super) sessions: Option<(SessionStore, Vec<ThreadId>)>,
    pub(super) identity: Option<Identity>,
    /// The bucket the clone's shared settings were written to.
    pub(super) shared: Option<(DirectKv, String)>,
}

impl Done {
    pub(super) async fn undo(self, order: &AdminOrder) {
        let warn = |what: &str, e: anyhow::Error| {
            tracing::warn!(order = %order.id, error = format!("{e:#}"), "undoing a clone: {what}");
        };
        if let Some((kv, key)) = self.shared
            && let Err(e) = kv.delete(&key).await
        {
            warn("deleting the shared settings", e);
        }
        if let Some(identity) = self.identity
            && let Err(e) = identity.delete().await
        {
            warn("deleting the identity record", e);
        }
        if let Some((store, threads)) = self.sessions {
            for thread in threads {
                if let Err(e) = store.delete(&thread).await {
                    warn("deleting a session", e);
                }
            }
        }
        for file in self.files {
            if let Err(e) = std::fs::remove_file(&file)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn("removing a file", e.into());
            }
        }
        for (repo, tree, branch) in self.worktrees.into_iter().rev() {
            let tree = tree.to_string_lossy().into_owned();
            if let Err(e) = git(&repo, &["worktree", "remove", "--force", &tree]).await {
                warn("removing a worktree", e);
            }
            if let Err(e) = git(&repo, &["branch", "-D", &branch]).await {
                warn("deleting a branch", e);
            }
        }
        if let Some(home) = self.home
            && let Err(e) = std::fs::remove_dir_all(&home)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn("removing the home", e.into());
        }
    }
}
