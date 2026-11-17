//! Copying the home of the ant cloned, and writing each thread's history into the clone's.

use std::path::Path;

use ants_proto::UserId;
use anyhow::{Context as _, Result};

use super::plan::Plan;
use super::undo::Done;
use super::worktrees::{Tree, worktrees};
use crate::clone::{Snapshot, ThreadCopy};

/// What of a home a clone does not copy: its worktrees and history are made again, its answered and buried messages,
/// its MCP endpoint and its lifecycle events ([`crate::provider::events`]) are its own.
const NOT_COPIED: [&str; 7] =
    ["worktrees", "history", "answered", "buried", "mcp.json", "mcp.sock", crate::provider::events::DIR];

/// Copy the home of the ant cloned to the clone's, with the history of each thread of `snapshot`, and add its
/// worktrees again for `new`: the worktrees, and the branches added.
pub(super) async fn copy_files(
    plan: &Plan,
    snapshot: &Snapshot,
    new: &UserId,
    done: &mut Done,
) -> Result<(Vec<Tree>, Vec<String>)> {
    let (src_home, new_home) = (&plan.src_home, &plan.new_home);
    let trees = worktrees(src_home, &snapshot.threads).await?;
    done.home = Some(new_home.clone());
    copy_home(src_home, new_home)?;
    write_histories(new_home, &snapshot.threads)?;
    let mut branches = Vec::new();
    for tree in &trees {
        branches.push(tree.add(src_home, new_home, new, done).await?);
    }
    Ok((trees, branches))
}

/// Write the history of each thread of `copies` in `home`, the clone's.
fn write_histories(home: &Path, copies: &[ThreadCopy]) -> Result<()> {
    for copy in copies {
        let file = crate::stream::history::cloned::path(home, &copy.thread);
        if let Some(dir) = file.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        std::fs::write(&file, &copy.history).with_context(|| format!("writing {}", file.display()))?;
    }
    Ok(())
}

/// Copy the home `from` to `to`, all but [`NOT_COPIED`]: directories, regular files and symbolic links.
fn copy_home(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
    let entries = match std::fs::read_dir(from) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", from.display())),
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("reading {}", from.display()))?;
        if NOT_COPIED.iter().any(|name| entry.file_name() == *name) {
            continue;
        }
        copy(&entry.path(), &to.join(entry.file_name()))?;
    }
    Ok(())
}

/// Copy the directory, regular file or symbolic link `from` to `to`; anything else is skipped.
pub(super) fn copy(from: &Path, to: &Path) -> Result<()> {
    let kind = std::fs::symlink_metadata(from).with_context(|| format!("reading {}", from.display()))?.file_type();
    if kind.is_dir() {
        std::fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
        for entry in std::fs::read_dir(from).with_context(|| format!("reading {}", from.display()))? {
            let entry = entry.with_context(|| format!("reading {}", from.display()))?;
            copy(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else if kind.is_file() {
        std::fs::copy(from, to).with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
    } else if kind.is_symlink() {
        #[cfg(unix)]
        {
            let target = std::fs::read_link(from).with_context(|| format!("reading {}", from.display()))?;
            std::os::unix::fs::symlink(&target, to).with_context(|| format!("linking {}", to.display()))?;
        }
    }
    Ok(())
}
