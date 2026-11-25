//! Open files: the soft limit on them a daemon runs with, raised at its start to the hard one, what its config needs,
//! and a harness failing for want of them.
//!
//! An ant takes about [`PER_ANT`] open files idle, each live harness about [`PER_HARNESS`] more. A service manager's
//! soft limit is low (launchd's 256, systemd's 1024), so a daemon raises its soft limit to its hard one at start, on
//! macOS at most `kern.maxfilesperproc`. A harness that still fails for want of open files is a hold of the machine
//! ([`LimitKind::OpenFiles`]): its message waits in hand, spending no attempt, and is tried again after a backoff.

use ants_proto::LimitKind;

use crate::limits::ratelimit::Limited;

/// Open files an ant takes while idle: its connection, sense store and logs.
pub const PER_ANT: u64 = 4;
/// Open files each live harness takes: its pipes and, on macOS, the kqueue its exit is waited for with
/// (`crate::process::wait`).
pub const PER_HARNESS: u64 = if cfg!(target_os = "macos") { 4 } else { 3 };
/// Open files beyond those of the ants and harnesses: the daemon's own, a managed nats-server's, a burst of git.
pub const MARGIN: u64 = 64;

/// A process's limit on open files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    /// The soft limit, enforced; None for unlimited.
    pub soft: Option<u64>,
    /// The hard limit the soft one may be raised to; None for unlimited.
    pub hard: Option<u64>,
}

/// The open files a daemon serving ants that each run at most the given `max_harnesses` needs, about.
pub fn needed(max_harnesses: impl IntoIterator<Item = usize>) -> u64 {
    let per_ant = |max: usize| PER_ANT + PER_HARNESS * u64::try_from(max).unwrap_or(u64::MAX);
    max_harnesses.into_iter().map(per_ant).fold(MARGIN, u64::saturating_add)
}

/// Whether `limit` lets a process open `needed` files.
pub fn suffices(limit: Limit, needed: u64) -> bool {
    limit.soft.is_none_or(|soft| soft >= needed)
}

/// What [`raise`] found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Raised {
    /// The limit the process started with.
    pub before: Limit,
    /// The limit it runs with now.
    pub now: Limit,
    /// Why raising the soft limit failed, if it did.
    pub error: Option<String>,
}

/// Raise the soft limit on open files ([`raise`]) and log the limit the daemon runs with, warning when it is below
/// `needed`, what its ants need.
pub fn raise_for(needed: u64) {
    let Some(raised) = raise() else { return };
    let (before, soft, hard) = (show(raised.before.soft), show(raised.now.soft), show(raised.now.hard));
    if let Some(error) = &raised.error {
        tracing::warn!(soft, hard, error, "raising the open-file limit");
    }
    if suffices(raised.now, needed) {
        tracing::info!(soft, hard, before, needed, "open-file limit");
    } else {
        tracing::warn!(
            soft,
            hard,
            needed,
            "open-file limit below what the ants need: harness starts may fail and their messages wait; raise the hard \
             limit (LimitNOFILE= in a systemd unit, HardResourceLimits NumberOfFiles in a launchd plist, ulimit -Hn)"
        );
    }
}

/// The limit on open files of this process; None where there is none to read (Windows).
#[cfg(unix)]
pub fn limit() -> Option<Limit> {
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    Some(Limit { soft: limit.current, hard: limit.maximum })
}

/// The limit on open files of this process; None where there is none to read (Windows).
#[cfg(not(unix))]
pub fn limit() -> Option<Limit> {
    None
}

/// Raise the soft limit on open files of this process to the hard one, on macOS at most `kern.maxfilesperproc`. None
/// where there is no limit (Windows).
#[cfg(unix)]
pub fn raise() -> Option<Raised> {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let before = limit()?;
    let target = target(before.hard, max_per_process());
    if before.soft.is_none() || before.soft >= target && target.is_some() {
        return Some(Raised { before, now: before, error: None });
    }
    let set = |soft| setrlimit(Resource::Nofile, Rlimit { current: soft, maximum: before.hard });
    // Older macOS refuses more than OPEN_MAX.
    let result = set(target).or_else(|e| match target {
        Some(target) if cfg!(target_os = "macos") && target > OPEN_MAX => set(Some(OPEN_MAX)),
        _ => Err(e),
    });
    let error = result.err().map(|e| e.to_string());
    let now = getrlimit(Resource::Nofile);
    Some(Raised { before, now: Limit { soft: now.current, hard: now.maximum }, error })
}

/// The limit on open files a daemon started as this process was would run with, its soft limit raised ([`raise`]).
/// None where there is no limit (Windows).
#[cfg(unix)]
pub fn reachable() -> Option<Limit> {
    let limit = limit()?;
    let target = target(limit.hard, max_per_process());
    let soft = if limit.soft.is_none() || limit.soft >= target && target.is_some() { limit.soft } else { target };
    Some(Limit { soft, ..limit })
}

/// The limit on open files a daemon started as this process was would run with. None where there is no limit
/// (Windows).
#[cfg(not(unix))]
pub fn reachable() -> Option<Limit> {
    None
}

/// Raise the soft limit on open files of this process to the hard one. None where there is no limit (Windows).
#[cfg(not(unix))]
pub fn raise() -> Option<Raised> {
    None
}

/// macOS's `OPEN_MAX`, the most open files older releases let a soft limit reach.
#[cfg(unix)]
const OPEN_MAX: u64 = 10_240;

/// The soft limit to raise to: the hard limit, at most the system's limit per process.
#[cfg(any(unix, test))]
fn target(hard: Option<u64>, per_process: Option<u64>) -> Option<u64> {
    match (hard, per_process) {
        (Some(hard), Some(cap)) => Some(hard.min(cap)),
        (hard, None) => hard,
        (None, cap) => cap,
    }
}

/// The most open files the system lets one process have: `kern.maxfilesperproc` on macOS.
#[cfg(target_os = "macos")]
fn max_per_process() -> Option<u64> {
    let mut value: libc::c_int = 0;
    let mut len = size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated and `value` is a writable c_int of `len` bytes, the most sysctlbyname writes.
    let status = unsafe {
        libc::sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            (&raw mut value).cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (status == 0 && len == size_of::<libc::c_int>()).then(|| u64::try_from(value).ok()).flatten()
}

/// The most open files the system lets one process have: the hard limit says it, elsewhere.
#[cfg(all(unix, not(target_os = "macos")))]
fn max_per_process() -> Option<u64> {
    None
}

/// A limit as a log or a check shows it.
pub fn show(limit: Option<u64>) -> String {
    limit.map_or_else(|| "unlimited".to_owned(), |n| n.to_string())
}

/// Whether `text`, an error, says the process or the system ran out of open files (EMFILE, ENFILE).
pub fn exhausted(text: &str) -> bool {
    text.to_lowercase().contains("too many open files")
}

/// The hold of the machine that `e`, an error from `harness`, is when it says the process ran out of open files.
pub fn held(harness: &str, e: &anyhow::Error) -> Option<Limited> {
    let detail = format!("{e:#}");
    exhausted(&detail).then(|| Limited {
        harness: harness.to_owned(),
        kind: LimitKind::OpenFiles,
        retry_after: None,
        detail,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_files_per_ant_and_harness() {
        assert_eq!(needed([]), MARGIN);
        assert_eq!(needed(std::iter::repeat_n(8, 50)), MARGIN + 50 * (PER_ANT + 8 * PER_HARNESS));
        let limit = |soft| Limit { soft, hard: None };
        assert!(!suffices(limit(Some(256)), needed(std::iter::repeat_n(8, 50))));
        assert!(suffices(limit(Some(4096)), needed(std::iter::repeat_n(8, 50))));
        assert!(suffices(limit(None), u64::MAX));
    }

    #[test]
    fn raises_to_the_hard_limit_within_the_system_cap() {
        assert_eq!(target(Some(1 << 20), Some(61_440)), Some(61_440));
        assert_eq!(target(Some(4096), Some(61_440)), Some(4096));
        assert_eq!(target(None, Some(61_440)), Some(61_440));
        assert_eq!(target(Some(4096), None), Some(4096));
        assert_eq!(target(None, None), None);
    }

    #[test]
    fn running_out_of_open_files_holds_the_message() {
        let e = anyhow::anyhow!("copilot session: Internal error: \"Too many open files (os error 24)\"");
        let copilot = held("copilot", &e).unwrap();
        assert_eq!(
            (copilot.kind, copilot.retry_after, copilot.harness.as_str()),
            (LimitKind::OpenFiles, None, "copilot")
        );
        let system = anyhow::Error::new(std::io::Error::other("Too many open files in system"));
        assert!(held("h", &system).is_some());
        assert!(held("h", &anyhow::anyhow!("copilot session: not installed")).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn raises_the_soft_limit() {
        let raised = raise().unwrap();
        assert_eq!(raised.error, None, "{raised:?}");
        assert!(raised.now.soft >= raised.before.soft || raised.now.soft.is_none(), "{raised:?}");
        assert_eq!(limit(), Some(raised.now));
    }
}
