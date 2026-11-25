//! This daemon's record of the descendants it walked of its CLIs' runs not released, so that a daemon started after
//! it died ends them ([`super::sweep_at_start`]): on macOS, which hides the environment of the system's own programs,
//! and with it their tag, how a `zsh` loop reparented as its CLI and the daemon died is found. The record is the file
//! `<daemon pid>.<daemon start>` in `ants-<uid>/leftovers` of the temporary directory, which goes with the processes
//! at a reboot, a line `<pid> <start>` per process, removed once the daemon released every run.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, Weak};

use super::unix::{DAEMON, Daemon, Proc, WATCHES, os};
use crate::sync::MutexExt as _;

/// The directory of the records.
const DIR: &str = "leftovers";

/// The record this daemon wrote last, so as not to write it again unchanged; locked while one is written.
static WRITTEN: Mutex<Option<String>> = Mutex::new(None);

/// The directory of the records in the temporary directory, if it serves.
pub(super) fn dir() -> Option<PathBuf> {
    in_temp(&std::env::temp_dir())
}

/// `ants-<uid>/leftovers` in `temp`, made mode 0700 where missing; None, logged once, where it cannot be made, or
/// another user owns it or others may enter it.
pub(super) fn in_temp(temp: &Path) -> Option<PathBuf> {
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
    static REFUSED: std::sync::Once = std::sync::Once::new();
    let uid = rustix::process::getuid().as_raw();
    let base = temp.join(format!("ants-{uid}"));
    let dir = base.join(DIR);
    let checked = std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir).and_then(|()| {
        for dir in [&base, &dir] {
            let meta = std::fs::symlink_metadata(dir)?;
            if !meta.is_dir() || meta.uid() != uid || meta.mode() & 0o077 != 0 {
                return Err(io::Error::other(format!(
                    "{} is no directory of this user's alone (mode {:o})",
                    dir.display(),
                    meta.mode() & 0o777
                )));
            }
        }
        Ok(())
    });
    match checked {
        Ok(()) => Some(dir),
        Err(e) => {
            REFUSED.call_once(|| {
                tracing::warn!(
                    dir = %dir.display(),
                    error = %e,
                    "keeping no record of what harness CLIs leave running: a daemon started after this one died may leave some running"
                );
            });
            None
        }
    }
}

/// Write this daemon's record anew: the descendants walked of its runs not released.
pub(super) fn save() {
    let mut written = WRITTEN.locked();
    let text = text(&recorded());
    if written.as_deref() == Some(text.as_str()) {
        return;
    }
    let Some(dir) = dir() else { return };
    match write(&dir, *DAEMON, &text) {
        Ok(()) => *written = Some(text),
        Err(e) => tracing::debug!(error = %e, "writing the record of what harness CLIs left running"),
    }
}

/// The descendants walked of this daemon's runs not released.
fn recorded() -> Vec<Proc> {
    let watches: Vec<_> = {
        let mut watches = WATCHES.locked();
        watches.retain(|watch| watch.strong_count() > 0);
        watches.iter().filter_map(Weak::upgrade).collect()
    };
    watches.iter().flat_map(|watch| watch.locked().tree.clone()).collect()
}

/// A record of `processes`.
pub(super) fn text(processes: &[Proc]) -> String {
    processes.iter().map(|process| format!("{} {}\n", process.pid, process.start)).collect()
}

/// The file of `daemon`'s record.
fn file(dir: &Path, daemon: Daemon) -> PathBuf {
    dir.join(format!("{}.{}", daemon.pid, daemon.start))
}

/// Write `text` as `daemon`'s record in `dir`, replacing the one there; empty, remove it.
pub(super) fn write(dir: &Path, daemon: Daemon, text: &str) -> io::Result<()> {
    let path = file(dir, daemon);
    if text.is_empty() {
        return match std::fs::remove_file(&path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
    }
    let new = dir.join(format!("{}.{}.new", daemon.pid, daemon.start));
    std::fs::write(&new, text)?;
    std::fs::rename(&new, &path)
}

/// The daemon whose record `name` names, written or being written.
fn daemon_of(name: &str) -> Option<Daemon> {
    let name = name.strip_suffix(".new").unwrap_or(name);
    let (pid, start) = name.split_once('.')?;
    Some(Daemon { pid: pid.parse().ok()?, start: start.parse().ok()? })
}

/// The processes still running that the records in `dir` of daemons no longer running name, and those records' files.
pub(super) fn of_daemons_gone(dir: &Path) -> (Vec<Proc>, Vec<PathBuf>) {
    let table = os::table();
    let running: HashSet<Daemon> =
        table.iter().map(|process| Daemon { pid: process.pid, start: process.start }).collect();
    let (mut recorded, mut files) = (Vec::new(), Vec::new());
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let Some(daemon) = entry.file_name().to_str().and_then(daemon_of) else { continue };
        if daemon == *DAEMON || running.contains(&daemon) {
            continue;
        }
        let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
        for line in text.lines() {
            let Some((pid, start)) = line.split_once(' ') else { continue };
            let (Ok(pid), Ok(start)) = (pid.parse::<i32>(), start.parse::<u64>()) else { continue };
            recorded.extend(table.iter().filter(|process| process.pid == pid && process.start == start));
        }
        files.push(entry.path());
    }
    (recorded, files)
}

/// Remove the records `files`, their processes ended.
pub(super) fn remove(files: &[PathBuf]) {
    for file in files {
        let _ = std::fs::remove_file(file);
    }
}
