//! When `ants daemon` and `ants webhook serve` reload their config file: on SIGHUP, at once after an admin order
//! edited it ([`Triggers::edits`]) or the shared settings of an ant changed ([`Triggers::watch_shared`]), and once an edit of the file, or of the daemon's drop-in harness files, settled. The file is polled, its modification time, length and inode
//! compared every [`CHECK_EVERY`] on the process's own runtime, with no watcher thread, and so are the drop-in files
//! ([`ants_daemon::config::dropin_files`]) listed by name, the daemon's credentials files of the built-in ants
//! ([`Triggers::watch_creds`]) and the machine's host credentials files the config's lookup tries
//! ([`Triggers::host_creds`]); a change counts once two checks in a row find them the same.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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

/// The config file, or none at its path, the drop-in files, by path, and the credentials files, or none at each path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Found {
    file: Option<FileId>,
    dropins: Vec<(PathBuf, FileId)>,
    creds: Vec<Option<FileId>>,
    host: Vec<Candidate>,
}

/// A file at a path, or none there.
type Candidate = (PathBuf, Option<FileId>);

/// The machine's host credentials files a load of the config looked for
/// ([`ants_daemon::config::HostSettings::creds_candidates`]), each as found then, for the next check to watch from;
/// clones share them.
#[derive(Debug, Clone, Default)]
pub struct HostCreds(Arc<Mutex<Option<Vec<Candidate>>>>);

impl HostCreds {
    /// Watch `candidates` from now on, as they are now, a load of the config having just looked for them.
    pub fn watch(&self, candidates: Vec<PathBuf>) {
        let found = candidates
            .into_iter()
            .map(|path| {
                let id = FileId::of(&path).ok();
                (path, id)
            })
            .collect();
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(found);
    }

    fn take(&self) -> Option<Vec<Candidate>> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
    }
}

/// A config file, a directory of drop-in files, and credentials files, checked for changes.
#[derive(Debug)]
struct Watched {
    path: PathBuf,
    dropins: Option<PathBuf>,
    creds: Vec<PathBuf>,
    host: HostCreds,
    /// The files as last loaded.
    loaded: Found,
    /// The files as the last check found them.
    seen: Found,
}

impl Watched {
    fn new(path: PathBuf, dropins: Option<PathBuf>) -> Self {
        let (creds, loaded, seen) = (Vec::new(), Found::default(), Found::default());
        let mut watched = Self { path, dropins, creds, host: HostCreds::default(), loaded, seen };
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
        let creds = self.creds.iter().map(|path| FileId::of(path).ok()).collect();
        let host = self.loaded.host.iter().map(|(path, _)| (path.clone(), FileId::of(path).ok())).collect();
        Found { file: FileId::of(&self.path).ok(), dropins, creds, host }
    }

    /// Watch the host credentials files a load of the config looked for since the last check, as it found them.
    fn rebase_host(&mut self) {
        if let Some(host) = self.host.take() {
            self.seen.host.clone_from(&host);
            self.loaded.host = host;
        }
    }

    /// Why to reload, where the files are other than the ones loaded and unchanged since the last check: an edit
    /// still being written waits for a later check. The files found count as loaded then.
    fn settled(&mut self) -> Option<Trigger> {
        self.rebase_host();
        let now = self.found();
        let settled = now != self.loaded && now == self.seen;
        let config = (&now.file, &now.dropins) != (&self.loaded.file, &self.loaded.dropins);
        self.seen = now;
        if !settled {
            return None;
        }
        self.loaded.clone_from(&self.seen);
        Some(if config { Trigger::Changed } else { Trigger::Credentials })
    }

    /// Count the files found now as loaded, as a reload by SIGHUP reads them.
    fn loading(&mut self) {
        self.rebase_host();
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
    /// The shared settings of an ant changed ([`ants_daemon::profile::shared`]).
    Shared,
    /// Credentials files of the built-in ants or of the machine's host came, went or changed, and settled
    /// ([`Triggers::watch_creds`], [`Triggers::host_creds`]).
    Credentials,
}

impl Trigger {
    pub fn describe(&self) -> &'static str {
        match self {
            Trigger::Hangup => "SIGHUP",
            Trigger::Changed => "the config changed",
            Trigger::Edited => "an admin order edited the config",
            Trigger::Shared => "the shared settings of an ant changed",
            Trigger::Credentials => "the credentials of a built-in ant or the host changed",
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

    /// Reload as well once the shared settings `changes` tells of change ([`ants_daemon::profile::shared::Shared::subscribe`]).
    pub fn watch_shared(&mut self, mut changes: watch::Receiver<u64>) {
        changes.mark_unchanged();
        self.shared = Some(changes);
    }

    /// Reload as well once one of the credentials files `creds`, those of the built-in ants
    /// ([`ants_daemon::config::builtins::creds_files`]), comes, goes or changes and settles, as which built-in ants the
    /// machine serves, and with what credentials, depends on them.
    pub fn watch_creds(&mut self, creds: Vec<PathBuf>) {
        self.file.creds = creds;
        self.file.loading();
    }

    /// Where each load of the config tells the host credentials files it looked for ([`HostCreds::watch`]), to reload
    /// as well once one of them comes, goes or changes and settles, as the built-in ants the machine serves depend on
    /// whether the server takes credentials, and the host connection on them.
    pub fn host_creds(&self) -> HostCreds {
        self.file.host.clone()
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
                _ = self.checks.tick() => if let Some(trigger) = self.file.settled() {
                    if trigger == Trigger::Changed {
                        tracing::info!(file = %self.file.path.display(), "the config changed");
                    }
                    return trigger;
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
        assert!(file.settled().is_none(), "nothing changed");

        std::fs::write(&path, "a = 1\nb = 2\n").unwrap();
        assert!(file.settled().is_none(), "first seen");
        std::fs::write(&path, "a = 1\nb = 2\nc = 3\n").unwrap();
        assert!(file.settled().is_none(), "still changing");
        assert!(file.settled().is_some(), "unchanged since the last check");
        assert!(file.settled().is_none(), "loaded already");

        std::fs::remove_file(&path).unwrap();
        assert!(file.settled().is_none());
        assert!(file.settled().is_some(), "a removed file counts too, for its load to fail");
        let staged = dir.join(".config.toml.new");
        std::fs::write(&staged, "a = 2\n").unwrap();
        std::fs::rename(&staged, &path).unwrap();
        file.loading();
        assert!(file.settled().is_none() && file.settled().is_none(), "a reload by SIGHUP read it");
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
        assert!(
            file.settled().is_none() && file.settled().is_none(),
            "an empty directory and other files change nothing"
        );
        std::fs::write(dropins.join("codex.toml"), "[harnesses.codex]\ncommand = \"codex-acp\"\n").unwrap();
        assert!(file.settled().is_none(), "first seen");
        assert!(file.settled().is_some(), "a new file");
        std::fs::write(dropins.join("codex.toml"), "[harnesses.codex]\ncommand = \"codex-acp2\"\n").unwrap();
        assert!(file.settled().is_none() && file.settled().is_some(), "an edited file");
        std::fs::remove_file(dropins.join("codex.toml")).unwrap();
        assert!(file.settled().is_none() && file.settled().is_some(), "a removed file");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_built_in_ants_credentials_coming_going_or_changing_count_as_such() {
        let dir = ants_test_support::scratch_dir("reload-creds");
        let path = dir.join("config.toml");
        std::fs::write(&path, "").unwrap();
        let (messenger, sage) = (dir.join("messenger.creds"), dir.join("sage.creds"));
        std::fs::write(&sage, "old").unwrap();
        let mut file = Watched::new(path.clone(), None);
        file.creds = vec![messenger.clone(), sage.clone()];
        file.loading();
        assert_eq!((file.settled(), file.settled()), (None, None), "nothing changed");
        std::fs::write(&messenger, "issued").unwrap();
        assert_eq!((file.settled(), file.settled()), (None, Some(Trigger::Credentials)), "copied in");
        std::fs::write(&sage, "renewed").unwrap();
        assert_eq!((file.settled(), file.settled()), (None, Some(Trigger::Credentials)), "replaced");
        std::fs::remove_file(&messenger).unwrap();
        assert_eq!((file.settled(), file.settled()), (None, Some(Trigger::Credentials)), "removed");
        std::fs::write(&path, "harness = \"copilot\"\nhuman = \"human\"\n").unwrap();
        std::fs::remove_file(&sage).unwrap();
        assert_eq!((file.settled(), file.settled()), (None, Some(Trigger::Changed)), "with the config, a change of it");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_host_credentials_looked_for_count_coming_going_or_changing_and_not_a_new_lookup() {
        let dir = ants_test_support::scratch_dir("reload-host-creds");
        let path = dir.join("config.toml");
        std::fs::write(&path, "").unwrap();
        let (host, named) = (dir.join("host.creds"), dir.join("host-box.creds"));
        let mut file = Watched::new(path, None);
        let looked = file.host.clone();
        looked.watch(vec![host.clone(), named.clone()]);
        assert_eq!((file.settled(), file.settled()), (None, None), "neither exists");
        std::fs::write(&named, "issued").unwrap();
        assert_eq!((file.settled(), file.settled()), (None, Some(Trigger::Credentials)), "the fallback copied in");
        std::fs::write(&host, "issued").unwrap();
        assert_eq!((file.settled(), file.settled()), (None, Some(Trigger::Credentials)), "host.creds copied in");
        looked.watch(vec![host.clone(), dir.join("host-other.creds")]);
        assert_eq!((file.settled(), file.settled()), (None, None), "a lookup elsewhere read what it found");
        std::fs::remove_file(&named).unwrap();
        assert_eq!((file.settled(), file.settled()), (None, None), "no longer looked for");
        std::fs::remove_file(&host).unwrap();
        assert_eq!((file.settled(), file.settled()), (None, Some(Trigger::Credentials)), "removed");
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
        triggers.watch_creds(vec![dir.join("sage.creds")]);
        std::fs::write(dir.join("sage.creds"), "").unwrap();
        let trigger = tokio::time::timeout(Duration::from_secs(5), triggers.next()).await.expect("a reload");
        assert_eq!(trigger, Trigger::Credentials);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
