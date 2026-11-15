//! When `ants daemon` and `ants webhook serve` reload their config file: on SIGHUP, at once after an admin order
//! edited it ([`Triggers::edits`]) or the shared settings of an ant changed ([`Triggers::watch_shared`]), and once an edit of the file, or of the daemon's drop-in harness files, settled. The file is polled, its modification time, length and inode
//! compared every [`CHECK_EVERY`] on the process's own runtime, with no watcher thread, and so are the drop-in files
//! ([`ants_daemon::config::dropin_files`]) listed by name; a change counts once two checks in a row find them the
//! same.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use tokio::sync::{Notify, watch};

/// How often the config file is checked for a change.
pub const CHECK_EVERY: Duration = Duration::from_secs(3);

/// What tells one file at a path from another: a file replaced by a rename has another inode, one rewritten in place
/// another modification time or length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileId {
    dev: u64,
    ino: u64,
    modified: Option<SystemTime>,
    pub len: u64,
    pub executable: bool,
}

impl FileId {
    pub fn of(path: &Path) -> std::io::Result<Self> {
        let meta = std::fs::metadata(path)?;
        #[cfg(unix)]
        let (dev, ino, executable) = {
            use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
            (meta.dev(), meta.ino(), meta.permissions().mode() & 0o111 != 0)
        };
        #[cfg(not(unix))]
        let (dev, ino, executable) = (0, 0, true);
        Ok(Self { dev, ino, modified: meta.modified().ok(), len: meta.len(), executable })
    }
}

/// The config file, or none at its path, and the drop-in files, by path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Found {
    file: Option<FileId>,
    dropins: Vec<(PathBuf, FileId)>,
}

/// A config file, and a directory of drop-in files, checked for changes.
#[derive(Debug)]
struct Watched {
    path: PathBuf,
    dropins: Option<PathBuf>,
    /// The files as last loaded.
    loaded: Found,
    /// The files as the last check found them.
    seen: Found,
}

impl Watched {
    fn new(path: PathBuf, dropins: Option<PathBuf>) -> Self {
        let mut watched = Self { path, dropins, loaded: Found::default(), seen: Found::default() };
        watched.loading();
        watched
    }

    fn found(&self) -> Found {
        let dropins = self.dropins.as_deref().and_then(|dir| ants_daemon::config::dropin_files(dir).ok());
        let dropins = dropins
            .unwrap_or_default()
            .into_iter()
            .filter_map(|path| FileId::of(&path).ok().map(|id| (path, id)))
            .collect();
        Found { file: FileId::of(&self.path).ok(), dropins }
    }

    /// Whether the files are other than the ones loaded and unchanged since the last check: an edit still being
    /// written waits for a later check. The files found count as loaded then.
    fn settled(&mut self) -> bool {
        let now = self.found();
        let settled = now != self.loaded && now == self.seen;
        self.seen = now;
        if settled {
            self.loaded.clone_from(&self.seen);
        }
        settled
    }

    /// Count the files found now as loaded, as a reload by SIGHUP reads them.
    fn loading(&mut self) {
        self.loaded = self.found();
        self.seen.clone_from(&self.loaded);
    }
}

/// Why to reload.
#[derive(Debug, PartialEq, Eq)]
pub enum Trigger {
    Hangup,
    /// The config file, or a drop-in file, changed and settled.
    Changed,
    /// An admin order edited the config file.
    Edited,
    /// The shared settings of an ant changed ([`ants_daemon::shared`]).
    Shared,
}

impl Trigger {
    pub fn describe(&self) -> &'static str {
        match self {
            Trigger::Hangup => "SIGHUP",
            Trigger::Changed => "the config changed",
            Trigger::Edited => "an admin order edited the config",
            Trigger::Shared => "the shared settings of an ant changed",
        }
    }
}

/// SIGHUP, and changes of the config file and its drop-in files.
pub struct Triggers {
    hangup: Hangup,
    file: Watched,
    checks: tokio::time::Interval,
    edited: Arc<Notify>,
    shared: Option<watch::Receiver<u64>>,
}

impl Triggers {
    /// Listen for SIGHUP and check the file at `path`, and the drop-in files of the directory `dropins`, every
    /// `every`.
    pub fn new(path: PathBuf, dropins: Option<PathBuf>, every: Duration) -> Result<Self> {
        let mut checks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Ok(Self {
            hangup: Hangup::new()?,
            file: Watched::new(path, dropins),
            checks,
            edited: Arc::default(),
            shared: None,
        })
    }

    /// What to notify once an edit of the config file is written, to reload it at once.
    pub fn edits(&self) -> Arc<Notify> {
        Arc::clone(&self.edited)
    }

    /// Reload as well once the shared settings `changes` tells of change ([`ants_daemon::shared::Shared::subscribe`]).
    pub fn watch_shared(&mut self, mut changes: watch::Receiver<u64>) {
        changes.mark_unchanged();
        self.shared = Some(changes);
    }

    /// Wait for the next reason to reload. Cancel-safe, for `tokio::select!`.
    pub async fn next(&mut self) -> Trigger {
        loop {
            tokio::select! {
                () = self.hangup.recv() => {
                    self.file.loading();
                    return Trigger::Hangup;
                }
                () = self.edited.notified() => {
                    self.file.loading();
                    return Trigger::Edited;
                }
                changed = shared_changed(self.shared.as_mut()) => if changed.is_err() {
                    self.shared = None;
                } else {
                    return Trigger::Shared;
                },
                _ = self.checks.tick() => if self.file.settled() {
                    tracing::info!(file = %self.file.path.display(), "the config changed");
                    return Trigger::Changed;
                },
            }
        }
    }
}

/// The next change `changes` tells of; with none, never.
async fn shared_changed(changes: Option<&mut watch::Receiver<u64>>) -> Result<(), watch::error::RecvError> {
    match changes {
        Some(changes) => changes.changed().await,
        None => std::future::pending().await,
    }
}

/// SIGHUP, which `systemctl reload` sends.
struct Hangup(#[cfg(unix)] tokio::signal::unix::Signal);

impl Hangup {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use anyhow::Context as _;
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self(signal(SignalKind::hangup()).context("listening for SIGHUP")?))
        }
        #[cfg(not(unix))]
        Ok(Self())
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        if self.0.recv().await.is_some() {
            return;
        }
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_edit_counts_once_it_settled_and_once_only() {
        let dir = ants_test_support::scratch_dir("reload-watch");
        let path = dir.join("config.toml");
        std::fs::write(&path, "a = 1\n").unwrap();
        let mut file = Watched::new(path.clone(), None);
        assert!(!file.settled(), "nothing changed");

        std::fs::write(&path, "a = 1\nb = 2\n").unwrap();
        assert!(!file.settled(), "first seen");
        std::fs::write(&path, "a = 1\nb = 2\nc = 3\n").unwrap();
        assert!(!file.settled(), "still changing");
        assert!(file.settled(), "unchanged since the last check");
        assert!(!file.settled(), "loaded already");

        std::fs::remove_file(&path).unwrap();
        assert!(!file.settled());
        assert!(file.settled(), "a removed file counts too, for its load to fail");
        let staged = dir.join(".config.toml.new");
        std::fs::write(&staged, "a = 2\n").unwrap();
        std::fs::rename(&staged, &path).unwrap();
        file.loading();
        assert!(!file.settled() && !file.settled(), "a reload by SIGHUP read it");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_new_or_edited_dropin_file_counts_as_a_change() {
        let dir = ants_test_support::scratch_dir("reload-dropins");
        let path = dir.join("config.toml");
        let dropins = dir.join("harnesses.d");
        std::fs::write(&path, "").unwrap();
        let mut file = Watched::new(path, Some(dropins.clone()));
        std::fs::create_dir(&dropins).unwrap();
        std::fs::write(dropins.join("notes.txt"), "").unwrap();
        assert!(!file.settled() && !file.settled(), "an empty directory and other files change nothing");
        std::fs::write(dropins.join("codex.toml"), "[harnesses.codex]\ncommand = \"codex-acp\"\n").unwrap();
        assert!(!file.settled(), "first seen");
        assert!(file.settled(), "a new file");
        std::fs::write(dropins.join("codex.toml"), "[harnesses.codex]\ncommand = \"codex-acp2\"\n").unwrap();
        assert!(!file.settled() && file.settled(), "an edited file");
        std::fs::remove_file(dropins.join("codex.toml")).unwrap();
        assert!(!file.settled() && file.settled(), "a removed file");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_settled_edit_triggers_a_reload() {
        let dir = ants_test_support::scratch_dir("reload-trigger");
        let path = dir.join("webhooks.toml");
        std::fs::write(&path, "a = 1\n").unwrap();
        let mut triggers = Triggers::new(path.clone(), None, Duration::from_millis(20)).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), triggers.next()).await.is_err(), "no change");
        std::fs::write(&path, "a = 2\nb = 3\n").unwrap();
        let trigger = tokio::time::timeout(Duration::from_secs(5), triggers.next()).await.expect("a reload");
        assert_eq!(trigger, Trigger::Changed);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
