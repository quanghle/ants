//! Moving services onto a new binary: `ants daemon`, `ants channel serve`, `ants webhook serve` and `ants bridge`
//! notice their executable was replaced, as `cargo install` does, and exit with [`EXIT_CODE`] for the service manager
//! to start the new one.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use crate::reload::FileId;

/// Exit status once the executable was replaced (`EX_TEMPFAIL`). The systemd units restart on it
/// (`RestartForceExitStatus`); launchd's `KeepAlive` restarts on any unsuccessful exit.
const EXIT_CODE: i32 = 75;

#[derive(clap::Args, Debug, Clone)]
pub struct UpgradeArgs {
    /// Check this often whether the ants executable was replaced (as by `cargo install`) and, once the new one is in
    /// place, exit with status 75 for the service manager to start it (a duration such as 30s or 5m; 0 never checks).
    #[arg(long, value_name = "DURATION", env = "ANTS_UPGRADE_CHECK", default_value = "30s",
          value_parser = ants_proto::duration::parse)]
    pub upgrade_check: Duration,
}

/// The executable this process runs, as found at its path at start. On Linux the running executable's own path ends
/// in ` (deleted)` once it was replaced, so the path is looked up once and the file found there later compared.
#[derive(Debug)]
struct Exe {
    path: PathBuf,
    running: FileId,
    /// What the last check found at the path, when another file than the running one.
    seen: Option<FileId>,
}

impl Exe {
    fn new(path: PathBuf) -> std::io::Result<Self> {
        let running = FileId::of(&path)?;
        Ok(Self { path, running, seen: None })
    }

    /// Whether another file replaced the running one, unchanged since the last check, non-empty and executable: an
    /// install still writing it, or a path without a file, waits for a later check.
    fn replaced(&mut self) -> bool {
        let now = FileId::of(&self.path).ok().filter(|now| *now != self.running);
        let settled = now.is_some() && now == self.seen;
        self.seen = now;
        settled && self.seen.as_ref().is_some_and(|now| now.executable && now.len > 0)
    }
}

/// Wait until the executable was replaced, checking every `every`; never, when `every` is zero or the executable
/// cannot be found.
pub async fn replaced(every: Duration) {
    if every.is_zero() {
        return std::future::pending().await;
    }
    let mut exe = match std::env::current_exe().and_then(Exe::new) {
        Ok(exe) => exe,
        Err(e) => {
            tracing::warn!(error = %e, "finding the executable; not checking for upgrades");
            return std::future::pending().await;
        }
    };
    let mut checks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        checks.tick().await;
        if exe.replaced() {
            tracing::info!(executable = %exe.path.display(), "the executable was replaced by a new one");
            return;
        }
    }
}

/// Run `serving` until it ends, or [`exit`] at once, as SIGTERM stops it, once the executable was replaced.
pub async fn or_exit(serving: impl Future<Output = Result<()>>, args: &UpgradeArgs) -> Result<()> {
    tokio::select! {
        result = serving => result,
        () = replaced(args.upgrade_check) => exit(),
    }
}

/// Exit with [`EXIT_CODE`] for the service manager to start the new executable.
pub fn exit() -> ! {
    tracing::info!(
        status = EXIT_CODE,
        "exiting for the service manager to start the new executable; started by hand, start it again"
    );
    crate::messages::progress::flush_logs();
    std::process::exit(EXIT_CODE)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn write(
        path: &Path,
        content: &str,
        #[cfg_attr(not(unix), expect(unused_variables, reason = "only Unix sets a mode"))] mode: u32,
    ) {
        std::fs::write(path, content).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
    }

    #[test]
    fn a_file_renamed_over_the_executable_counts_once_it_settled() {
        let dir = ants_test_support::scratch_dir("upgrade-rename");
        let path = dir.join("ants");
        write(&path, "old", 0o755);
        let mut exe = Exe::new(path.clone()).unwrap();
        assert!(!exe.replaced(), "nothing changed");

        std::fs::remove_file(&path).unwrap();
        assert!(!exe.replaced(), "no file at the path");
        let staged = dir.join(".ants-new");
        write(&staged, "new binary", 0o755);
        std::fs::rename(&staged, &path).unwrap();
        assert!(!exe.replaced(), "first seen");
        assert!(exe.replaced(), "unchanged since the last check");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_file_still_changing_or_not_executable_does_not_count() {
        let dir = ants_test_support::scratch_dir("upgrade-in-place");
        let path = dir.join("ants");
        write(&path, "old", 0o755);
        let mut exe = Exe::new(path.clone()).unwrap();

        write(&path, "new, half", 0o755);
        assert!(!exe.replaced());
        write(&path, "new, whole", 0o755);
        assert!(!exe.replaced(), "changed since the last check");
        #[cfg(unix)]
        {
            write(&path, "new, whole, not executable", 0o644);
            assert!(!exe.replaced());
            assert!(!exe.replaced(), "not executable");
            write(&path, "", 0o755);
            assert!(!exe.replaced());
            assert!(!exe.replaced(), "empty");
        }
        write(&path, "new, done", 0o755);
        assert!(!exe.replaced());
        assert!(exe.replaced());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
