//! What a native CLI leaves running: the processes it started, Claude Code's background commands among them, which
//! run in process groups of their own and outlive it, as a lease keeper's `bd heartbeat` loop would.
//!
//! On Unix the CLI's descendants are walked while it runs, last as it is released ([`Leftovers::walk`]), and each run
//! of a CLI is tagged in its environment ([`TAG`]: the daemon's process and the run's serial), which what it starts
//! inherits. Released ([`Leftovers::release`]: the session closed, the CLI exited or was killed), the descendants
//! walked and every process of the daemon's user tagged with a run that is gone get SIGTERM, to their process group
//! where they lead one, then SIGKILL after [`unix::GRACE`]. A run is gone once released, or once its daemon's process
//! is: the daemon's start sweeps those too ([`sweep_at_start`]), so what was reparented when the CLI or the daemon died
//! first ends there at the latest. A run of another daemon still running is never swept, nor the daemon itself and its
//! ancestors. macOS hides the environment of its own programs (`sh`, `sleep`, `perl`) from other processes, so there
//! only processes of other programs are found by their tag.
//!
//! On Windows each CLI runs in a job object that kills what is left in it once closed, at the release.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(all(test, unix))]
pub(super) mod tests;
#[cfg(windows)]
mod windows;

use tokio::process::{Child, Command};

#[cfg(all(test, unix))]
pub(crate) use unix::GRACE;
#[cfg(unix)]
pub(crate) use unix::TAG;

/// What a run of a native CLI leaves to end at its release: on Unix its tag and the descendants walked, on Windows
/// its job object. Dropped unreleased, it is released at once, blocking for up to [`unix::GRACE`] on Unix.
pub(crate) struct Leftovers {
    /// The harness's name, to log with what ends.
    name: String,
    /// The CLI's process ID, once started.
    #[cfg(unix)]
    pid: Option<u32>,
    /// None once released.
    #[cfg(unix)]
    run: Option<unix::Run>,
    #[cfg(unix)]
    tree: Vec<unix::Proc>,
    #[cfg(windows)]
    job: Option<windows::Job>,
}

impl Leftovers {
    /// Ready `command`, the CLI of harness `name`, to be released with what it starts: on Unix tagged with a new run.
    pub(crate) fn tag(name: &str, command: &mut Command) -> Self {
        #[cfg(unix)]
        let run = {
            let run = unix::Run::new();
            command.env(TAG, run.tag());
            run
        };
        #[cfg(not(unix))]
        let _ = command;
        Self {
            name: name.to_owned(),
            #[cfg(unix)]
            pid: None,
            #[cfg(unix)]
            run: Some(run),
            #[cfg(unix)]
            tree: Vec::new(),
            #[cfg(windows)]
            job: None,
        }
    }

    /// Follow `child`, the CLI started: on Windows in a job object of its own.
    pub(crate) fn follow(&mut self, child: &Child) {
        #[cfg(unix)]
        {
            self.pid = child.id();
        }
        #[cfg(windows)]
        {
            self.job = windows::Job::of(&self.name, child);
        }
    }

    /// Walk the CLI's descendants while it still runs, adding them to those to end at the release.
    pub(crate) async fn walk(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.pid.and_then(|pid| i32::try_from(pid).ok())
            && let Ok(walked) = tokio::task::spawn_blocking(move || unix::descendants(pid)).await
        {
            unix::merge(&mut self.tree, walked);
        }
    }

    /// [`Self::walk`], blocking, where no runtime runs.
    pub(crate) fn walk_blocking(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.pid.and_then(|pid| i32::try_from(pid).ok()) {
            unix::merge(&mut self.tree, unix::descendants(pid));
        }
    }

    /// End what the CLI left running, now it ended: the descendants walked and, on Unix, what is tagged with a run
    /// that is gone, this one included.
    pub(crate) async fn release(self) {
        #[cfg(unix)]
        {
            let mut leftovers = self;
            if let Some(run) = leftovers.run.take() {
                drop(run);
                let (name, tree) = (std::mem::take(&mut leftovers.name), std::mem::take(&mut leftovers.tree));
                let _ = tokio::task::spawn_blocking(move || unix::end(&name, &tree)).await;
            }
        }
        #[cfg(not(unix))]
        drop(self);
    }
}

impl Drop for Leftovers {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(run) = self.run.take() {
            self.walk_blocking();
            drop(run);
            unix::end(&self.name, &self.tree);
        }
    }
}

/// End, in the background, what runs of native CLIs left running whose daemon is gone, as the daemon starts; once per
/// process.
pub(crate) fn sweep_at_start() {
    #[cfg(unix)]
    {
        static SWEPT: std::sync::Once = std::sync::Once::new();
        SWEPT.call_once(|| {
            tokio::task::spawn_blocking(|| unix::end("", &[]));
        });
    }
}

#[cfg(unix)]
mod unix {
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{LazyLock, Mutex};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal};

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) use self::other as os;
    #[cfg(target_os = "linux")]
    pub(super) use super::linux as os;
    #[cfg(target_os = "macos")]
    pub(super) use super::macos as os;
    use crate::sync::MutexExt as _;

    /// The environment variable tagging a run of a native CLI and what it starts: `<pid>.<start>.<serial>`, the
    /// daemon's process ID and start time, and the run's serial in it.
    pub(crate) const TAG: &str = "ANTS_SESSION";

    /// How long a process left running has to exit once sent SIGTERM before it is sent SIGKILL.
    pub(crate) const GRACE: Duration = Duration::from_secs(2);

    /// How often the processes sent SIGTERM are looked for while they have their grace.
    const POLL: Duration = Duration::from_millis(50);

    /// The most of a process's command line logged as it is ended.
    const COMMAND_HEAD: usize = 120;

    /// A process as the process table lists it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Proc {
        pub pid: i32,
        pub ppid: i32,
        pub pgid: i32,
        pub uid: u32,
        /// When it started, in a unit of the platform's: with the pid, which process it is.
        pub start: u64,
    }

    /// A daemon's process: its pid and start time.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub(super) struct Daemon {
        pub pid: i32,
        pub start: u64,
    }

    /// A run of a native CLI: its daemon's process and its serial there.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Tag {
        pub daemon: Daemon,
        pub serial: u64,
    }

    impl Tag {
        pub(super) fn parse(value: &str) -> Option<Self> {
            let mut parts = value.splitn(3, '.');
            let pid = parts.next()?.parse().ok()?;
            let start = parts.next()?.parse().ok()?;
            let serial = parts.next()?.parse().ok()?;
            Some(Self { daemon: Daemon { pid, start }, serial })
        }

        pub(super) fn value(self) -> String {
            format!("{}.{}.{}", self.daemon.pid, self.daemon.start, self.serial)
        }
    }

    /// This daemon's process.
    pub(super) static DAEMON: LazyLock<Daemon> = LazyLock::new(|| {
        let pid = rustix::process::getpid().as_raw_nonzero().get();
        Daemon { pid, start: os::process(pid).map_or(0, |me| me.start) }
    });

    static SERIAL: AtomicU64 = AtomicU64::new(1);

    /// The serials of this daemon's runs not released.
    static LIVE: Mutex<BTreeSet<u64>> = Mutex::new(BTreeSet::new());

    /// A run of a native CLI, live until dropped.
    #[derive(Debug)]
    pub(super) struct Run(u64);

    impl Run {
        pub(super) fn new() -> Self {
            let serial = SERIAL.fetch_add(1, Ordering::Relaxed);
            LIVE.locked().insert(serial);
            Self(serial)
        }

        pub(super) fn tag(&self) -> String {
            Tag { daemon: *DAEMON, serial: self.0 }.value()
        }
    }

    impl Drop for Run {
        fn drop(&mut self) {
            LIVE.locked().remove(&self.0);
        }
    }

    /// Whether the run `tag` names is gone, its daemon running as one of `running`.
    fn gone(tag: Tag, running: &HashSet<Daemon>) -> bool {
        if tag.daemon == *DAEMON { !LIVE.locked().contains(&tag.serial) } else { !running.contains(&tag.daemon) }
    }

    /// Add the processes of `walked` to `tree`, unless already in it.
    pub(super) fn merge(tree: &mut Vec<Proc>, walked: Vec<Proc>) {
        for process in walked {
            if !tree.iter().any(|known| known.pid == process.pid && known.start == process.start) {
                tree.push(process);
            }
        }
    }

    /// The processes descended from `root`.
    pub(super) fn descendants(root: i32) -> Vec<Proc> {
        let table = os::table();
        let mut children: HashMap<i32, Vec<&Proc>> = HashMap::new();
        for process in &table {
            children.entry(process.ppid).or_default().push(process);
        }
        let (mut found, mut seen) = (Vec::new(), HashSet::from([root]));
        let mut next = vec![root];
        while let Some(pid) = next.pop() {
            for &child in children.get(&pid).into_iter().flatten() {
                if seen.insert(child.pid) {
                    found.push(*child);
                    next.push(child.pid);
                }
            }
        }
        found
    }

    /// The processes to end: those of `tree` still running and those of the daemon's user tagged with a run that is
    /// gone, neither the daemon nor its ancestors.
    pub(super) fn leftovers(tree: &[Proc]) -> Vec<Proc> {
        let table = os::table();
        let uid = rustix::process::getuid().as_raw();
        let running: HashSet<Daemon> =
            table.iter().map(|process| Daemon { pid: process.pid, start: process.start }).collect();
        let by_pid: HashMap<i32, &Proc> = table.iter().map(|process| (process.pid, process)).collect();
        let mut spared = HashSet::from([DAEMON.pid]);
        let mut pid = DAEMON.pid;
        while let Some(parent) = by_pid.get(&pid).map(|process| process.ppid).filter(|&ppid| spared.insert(ppid)) {
            pid = parent;
        }
        let mut buffer = Vec::new();
        table
            .iter()
            .filter(|process| process.uid == uid && process.pid > 1 && !spared.contains(&process.pid))
            .filter(|process| {
                tree.iter().any(|walked| walked.pid == process.pid && walked.start == process.start)
                    || os::tag(process.pid, &mut buffer)
                        .and_then(|tag| Tag::parse(&tag))
                        .is_some_and(|tag| gone(tag, &running))
            })
            .copied()
            .collect()
    }

    /// End the processes left running ([`leftovers`]), those of `tree` among them, logging each with harness `name`
    /// (none when sweeping at the daemon's start): SIGTERM to each, to its process group where it leads one, then
    /// SIGKILL to those still running after [`GRACE`].
    pub(super) fn end(name: &str, tree: &[Proc]) {
        let ending = leftovers(tree);
        if ending.is_empty() {
            return;
        }
        let mut buffer = Vec::new();
        for process in &ending {
            let mut command = os::command(process.pid, &mut buffer);
            if let Some((end, _)) = command.char_indices().nth(COMMAND_HEAD) {
                command.truncate(end);
            }
            if name.is_empty() {
                tracing::info!(
                    pid = process.pid,
                    command,
                    "ending a process left by a harness session of a daemon gone"
                );
            } else {
                tracing::info!(
                    harness = name,
                    pid = process.pid,
                    command,
                    "ending a process left by a harness session"
                );
            }
            signal(process, Signal::TERM);
        }
        let deadline = Instant::now() + GRACE;
        let mut running = ending;
        loop {
            running.retain(|process| os::process(process.pid).is_some_and(|now| now.start == process.start));
            if running.is_empty() || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(POLL);
        }
        for process in &running {
            tracing::debug!(pid = process.pid, "killing a process left by a harness session, still running");
            signal(process, Signal::KILL);
        }
    }

    /// Send `signal` to `process` and, where it leads one, its process group, unless the daemon's.
    fn signal(process: &Proc, signal: Signal) {
        let Some(pid) = Pid::from_raw(process.pid) else { return };
        if process.pgid == process.pid && process.pgid != rustix::process::getpgrp().as_raw_nonzero().get() {
            let _ = rustix::process::kill_process_group(pid, signal);
        }
        let _ = rustix::process::kill_process(pid, signal);
    }

    /// Where the process table is not read: nothing is found left running.
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    mod other {
        use super::Proc;

        pub(super) fn table() -> Vec<Proc> {
            Vec::new()
        }

        pub(super) fn process(_: i32) -> Option<Proc> {
            None
        }

        pub(super) fn tag(_: i32, _: &mut Vec<u8>) -> Option<String> {
            None
        }

        pub(super) fn command(_: i32, _: &mut Vec<u8>) -> String {
            String::new()
        }
    }
}
