//! When `ants daemon` and `ants webhook serve` reload their config file: on SIGHUP, and once an edit of the file
//! settled. The file is polled, its modification time, length and inode compared every [`CHECK_EVERY`] on the
//! process's own runtime, with no watcher thread; a change counts once two checks in a row find the file the same.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context as _, Result};

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

/// A config file checked for changes.
#[derive(Debug)]
struct Watched {
    path: PathBuf,
    /// The file as last loaded, or none at the path.
    loaded: Option<FileId>,
    /// The file as the last check found it.
    seen: Option<FileId>,
}

impl Watched {
    fn new(path: PathBuf) -> Self {
        let now = FileId::of(&path).ok();
        Self { path, loaded: now.clone(), seen: now }
    }

    /// Whether the file is another than the one loaded and unchanged since the last check: an edit still being
    /// written waits for a later check. The file found counts as loaded then.
    fn settled(&mut self) -> bool {
        let now = FileId::of(&self.path).ok();
        let settled = now != self.loaded && now == self.seen;
        self.seen = now;
        if settled {
            self.loaded.clone_from(&self.seen);
        }
        settled
    }

    /// Count the file at the path now as loaded, as a reload by SIGHUP reads it.
    fn loading(&mut self) {
        self.loaded = FileId::of(&self.path).ok();
        self.seen.clone_from(&self.loaded);
    }
}

/// Why to reload.
#[derive(Debug, PartialEq, Eq)]
pub enum Trigger {
    Hangup,
    /// The config file changed and settled.
    Changed,
}

impl Trigger {
    pub fn describe(&self) -> &'static str {
        match self {
            Trigger::Hangup => "SIGHUP",
            Trigger::Changed => "the config file changed",
        }
    }
}

/// SIGHUP, and changes of the config file.
pub struct Triggers {
    hangup: Hangup,
    file: Watched,
    checks: tokio::time::Interval,
}

impl Triggers {
    /// Listen for SIGHUP and check the file at `path` every `every`.
    pub fn new(path: PathBuf, every: Duration) -> Result<Self> {
        let mut checks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Ok(Self { hangup: Hangup::new()?, file: Watched::new(path), checks })
    }

    /// Wait for the next reason to reload. Cancel-safe, for `tokio::select!`.
    pub async fn next(&mut self) -> Trigger {
        loop {
            tokio::select! {
                () = self.hangup.recv() => {
                    self.file.loading();
                    return Trigger::Hangup;
                }
                _ = self.checks.tick() => if self.file.settled() {
                    tracing::info!(file = %self.file.path.display(), "the config file changed");
                    return Trigger::Changed;
                },
            }
        }
    }
}

/// SIGHUP, which `systemctl reload` sends.
struct Hangup(#[cfg(unix)] tokio::signal::unix::Signal);

impl Hangup {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
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
        std::future::pending().await
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
        let mut file = Watched::new(path.clone());
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

    #[tokio::test]
    async fn a_settled_edit_triggers_a_reload() {
        let dir = ants_test_support::scratch_dir("reload-trigger");
        let path = dir.join("webhooks.toml");
        std::fs::write(&path, "a = 1\n").unwrap();
        let mut triggers = Triggers::new(path.clone(), Duration::from_millis(20)).unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), triggers.next()).await.is_err(), "no change");
        std::fs::write(&path, "a = 2\nb = 3\n").unwrap();
        let trigger = tokio::time::timeout(Duration::from_secs(5), triggers.next()).await.expect("a reload");
        assert_eq!(trigger, Trigger::Changed);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
