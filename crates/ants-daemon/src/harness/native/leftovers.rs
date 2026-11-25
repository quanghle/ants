//! What a native CLI leaves running: the processes it started, Claude Code's background commands among them, which
//! run in process groups of their own and outlive it, as a lease keeper's `bd heartbeat` loop would.
//!
//! On Unix the CLI's descendants are walked while it runs: at the end of each of its turns ([`Runs::turn_ended`]) and
//! last as it is released ([`Leftovers::walk`]). Each run of a CLI is tagged in its environment ([`TAG`]: the daemon's
//! process and the run's serial), which what it starts inherits. Released ([`Leftovers::release`]: the session closed,
//! the CLI exited or was killed), the descendants walked and every process of the daemon's user tagged with a run that
//! is gone get SIGTERM, to their process group where they lead one, then SIGKILL after [`unix::GRACE`]. A run is gone
//! once released, or once its daemon's process is: the daemon's start sweeps those too ([`sweep_at_start`]), with the
//! descendants a daemon gone walked, which it kept a record of ([`record`]), so what was reparented when the CLI or the
//! daemon died first ends there at the latest. A run of another daemon still running is never swept, nor the daemon
//! itself and its ancestors. macOS hides the environment of its own programs (`sh`, `sleep`, `perl`) from other
//! processes, so there only processes of other programs are found by their tag, and the others by a walk made before
//! the CLI died.
//!
//! The end of a turn also tells what the turn left running in the background ([`Background`]): the processes descended
//! from the CLI, or tagged with its run, outside the CLI's process group, which its MCP servers stay in.
//!
//! On Windows each CLI runs in a job object that kills what is left in it once closed, at the release. The end of a
//! turn lists the processes in it, but the CLI and console hosts, not running as the turn began or that the turn before
//! left running (`job::Turns`): what the turn left running in the background.

#[cfg(any(windows, test))]
mod job;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(unix)]
mod record;
#[cfg(all(test, unix))]
pub(super) mod tests;
#[cfg(windows)]
mod windows;

#[cfg(any(unix, windows))]
use std::sync::{Arc, Mutex, Weak};

use tokio::process::{Child, Command};

use crate::harness::background::Background;
#[cfg(any(unix, windows))]
use crate::sync::MutexExt as _;

/// The most of a process's command line logged as it is ended or told as left running in the background.
#[cfg(any(unix, windows))]
const COMMAND_HEAD: usize = 120;

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
    /// The CLI's process and the descendants walked, shared with the [`Runs`] it is watched by.
    #[cfg(unix)]
    watch: Arc<Mutex<unix::Watch>>,
    /// Shared with the [`Runs`] it is watched by.
    #[cfg(windows)]
    job: Option<Arc<windows::Job>>,
}

impl Leftovers {
    /// Ready `command`, the CLI of harness `name`, to be released with what it starts: on Unix tagged with a new run.
    pub(crate) fn tag(name: &str, command: &mut Command) -> Self {
        #[cfg(unix)]
        let (run, watch) = {
            let run = unix::Run::new();
            command.env(TAG, run.tag());
            let watch = unix::Watch::new(run.tag());
            (run, watch)
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
            watch,
            #[cfg(windows)]
            job: None,
        }
    }

    /// Follow `child`, the CLI started: on Windows in a job object of its own.
    pub(crate) fn follow(&mut self, child: &Child) {
        #[cfg(unix)]
        {
            self.pid = child.id();
            self.watch.locked().follow(self.pid);
        }
        #[cfg(windows)]
        {
            self.job = windows::Job::of(&self.name, child).map(Arc::new);
        }
    }

    /// Have `runs` walk the CLI's descendants at the end of each of its turns ([`Runs::turn_ended`]).
    pub(crate) fn watched_by(&self, runs: &Runs) {
        #[cfg(unix)]
        runs.watches.locked().push(Arc::downgrade(&self.watch));
        #[cfg(windows)]
        if let Some(job) = &self.job {
            runs.jobs.locked().push(Arc::downgrade(job));
        }
        #[cfg(not(any(unix, windows)))]
        let _ = runs;
    }

    /// Walk the CLI's descendants while it still runs, adding them to those to end at the release.
    pub(crate) async fn walk(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.pid.and_then(|pid| i32::try_from(pid).ok()) {
            let watch = self.watch.clone();
            let walking = move || unix::merge(&mut watch.locked().tree, unix::descendants(pid));
            let _ = tokio::task::spawn_blocking(walking).await;
        }
    }

    /// [`Self::walk`], blocking, where no runtime runs.
    pub(crate) fn walk_blocking(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.pid.and_then(|pid| i32::try_from(pid).ok()) {
            unix::merge(&mut self.watch.locked().tree, unix::descendants(pid));
        }
    }

    /// End what the CLI left running, now it ended: the descendants walked and, on Unix, what is tagged with a run
    /// that is gone, this one included.
    pub(crate) async fn release(self) {
        #[cfg(unix)]
        {
            let mut leftovers = self;
            if let Some(run) = leftovers.run.take() {
                let (name, watch) = (std::mem::take(&mut leftovers.name), leftovers.watch.clone());
                let _ = tokio::task::spawn_blocking(move || unix::release(&name, run, &watch)).await;
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
            unix::release(&self.name, run, &self.watch);
        }
    }
}

/// The runs of the CLI processes started for one harness, shared by the clones of its [`super::Cli`]: what each
/// started is walked at the end of each turn ([`Runs::turn_ended`]).
#[derive(Debug, Clone, Default)]
pub(crate) struct Runs {
    #[cfg(unix)]
    watches: Arc<Mutex<Vec<Weak<Mutex<unix::Watch>>>>>,
    #[cfg(windows)]
    jobs: Arc<Mutex<Vec<Weak<windows::Job>>>>,
    #[cfg(windows)]
    turns: Arc<Mutex<job::Turns>>,
}

impl Runs {
    /// As a turn began: on Windows, what a CLI's job object holds is noted, what starts in it from now on being the
    /// turn's (`job::Turns`).
    pub(crate) fn turn_started(&self) {
        #[cfg(windows)]
        {
            let jobs: Vec<_> = self.jobs.locked().iter().filter_map(Weak::upgrade).collect();
            for job in jobs {
                job.turn_started();
            }
        }
    }

    /// As a turn ended: walk the descendants of each CLI still running, so that its release ends those it left running
    /// even once it died first, and record them for a daemon started after this one died ([`record`]). Returns what
    /// the turn left running in the background, outside the CLI's process group; on Windows, what its CLIs' job
    /// objects hold that the turn started or the turn before left running (`job::Turns`).
    pub(crate) async fn turn_ended(&self) -> Vec<Background> {
        #[cfg(unix)]
        {
            let watches: Vec<_> = {
                let mut watches = self.watches.locked();
                watches.retain(|watch| watch.strong_count() > 0);
                watches.iter().filter_map(Weak::upgrade).collect()
            };
            if watches.is_empty() {
                return Vec::new();
            }
            let walking = move || {
                let background = unix::turn_ended(&watches);
                drop(watches);
                record::save();
                background
            };
            tokio::task::spawn_blocking(walking).await.unwrap_or_default()
        }
        #[cfg(windows)]
        {
            let jobs: Vec<_> = {
                let mut jobs = self.jobs.locked();
                jobs.retain(|job| job.strong_count() > 0);
                jobs.iter().filter_map(Weak::upgrade).collect()
            };
            let turns = self.turns.clone();
            let listing = move || {
                let listed = jobs.iter().map(|job| job.listed()).collect();
                drop(jobs);
                turns.locked().ended(listed)
            };
            tokio::task::spawn_blocking(listing).await.unwrap_or_default()
        }
        #[cfg(not(any(unix, windows)))]
        Vec::new()
    }
}

/// Whether process `pid`, started at `start` in the platform's unit, still runs; false where the process table is not
/// read.
pub(crate) fn runs(pid: i32, start: u64) -> bool {
    #[cfg(unix)]
    {
        unix::os::process(pid).is_some_and(|process| process.start == start)
    }
    #[cfg(windows)]
    {
        windows::process(pid).is_some_and(|process| process.start == start)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (pid, start);
        false
    }
}

/// `command` cut to its first [`COMMAND_HEAD`] characters.
#[cfg(any(unix, windows))]
fn head(mut command: String) -> String {
    if let Some((end, _)) = command.char_indices().nth(COMMAND_HEAD) {
        command.truncate(end);
    }
    command
}

/// End, in the background, what runs of native CLIs left running whose daemon is gone, as the daemon starts: what is
/// tagged with their runs and the descendants such a daemon recorded walking them ([`record`]); once per process.
pub(crate) fn sweep_at_start() {
    #[cfg(unix)]
    {
        static SWEPT: std::sync::Once = std::sync::Once::new();
        SWEPT.call_once(|| {
            tokio::task::spawn_blocking(|| {
                let Some(dir) = record::dir() else { return unix::end("", &[]) };
                let (recorded, records) = record::of_daemons_gone(&dir);
                unix::end("", &recorded);
                record::remove(&records);
            });
        });
    }
}

#[cfg(unix)]
mod unix {
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, LazyLock, Mutex, Weak};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal};

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) use self::other as os;
    #[cfg(target_os = "linux")]
    pub(super) use super::linux as os;
    #[cfg(target_os = "macos")]
    pub(super) use super::macos as os;
    use crate::harness::background::Background;
    use crate::sync::MutexExt as _;

    /// The environment variable tagging a run of a native CLI and what it starts: `<pid>.<start>.<serial>`, the
    /// daemon's process ID and start time, and the run's serial in it.
    pub(crate) const TAG: &str = "ANTS_SESSION";

    /// How long a process left running has to exit once sent SIGTERM before it is sent SIGKILL.
    pub(crate) const GRACE: Duration = Duration::from_secs(2);

    /// How often the processes sent SIGTERM are looked for while they have their grace.
    const POLL: Duration = Duration::from_millis(50);

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

    impl Proc {
        /// Whether it is `other`'s process, by pid and start.
        pub(super) fn is(&self, other: &Proc) -> bool {
            self.pid == other.pid && self.start == other.start
        }
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

    /// The runs of this daemon not released, for its record ([`super::record`]).
    pub(super) static WATCHES: Mutex<Vec<Weak<Mutex<Watch>>>> = Mutex::new(Vec::new());

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

    /// A run of a native CLI as walked: its tag, the CLI's process once listed, and its descendants walked, those to
    /// end at the release.
    #[derive(Debug)]
    pub(super) struct Watch {
        pub tag: String,
        pub cli: Option<Proc>,
        pub tree: Vec<Proc>,
    }

    impl Watch {
        /// The run tagged `tag`, kept in this daemon's record while not released.
        pub(super) fn new(tag: String) -> Arc<Mutex<Self>> {
            let watch = Arc::new(Mutex::new(Self { tag, cli: None, tree: Vec::new() }));
            let mut watches = WATCHES.locked();
            watches.retain(|watch| watch.strong_count() > 0);
            watches.push(Arc::downgrade(&watch));
            watch
        }

        /// Follow the CLI, process `pid` once started, by pid and start: a walk of a pid since reused finds nothing.
        pub(super) fn follow(&mut self, pid: Option<u32>) {
            self.cli = pid.and_then(|pid| i32::try_from(pid).ok()).and_then(os::process);
        }
    }

    /// End what the run `watch` of harness `name` left running, `run` released first ([`end`]), and drop it from this
    /// daemon's record.
    pub(super) fn release(name: &str, run: Run, watch: &Mutex<Watch>) {
        let tree = {
            let mut watch = watch.locked();
            watch.cli = None;
            std::mem::take(&mut watch.tree)
        };
        drop(run);
        end(name, &tree);
        super::record::save();
    }

    /// Whether the run `tag` names is gone, its daemon running as one of `running`.
    pub(super) fn gone(tag: Tag, running: &HashSet<Daemon>) -> bool {
        if tag.daemon == *DAEMON { !LIVE.locked().contains(&tag.serial) } else { !running.contains(&tag.daemon) }
    }

    /// Add the processes of `walked` to `tree`, unless already in it.
    pub(super) fn merge(tree: &mut Vec<Proc>, walked: Vec<Proc>) {
        for process in walked {
            if !tree.iter().any(|known| known.is(&process)) {
                tree.push(process);
            }
        }
    }

    /// The processes descended from `root`.
    pub(super) fn descendants(root: i32) -> Vec<Proc> {
        descendants_in(&os::table(), root)
    }

    /// The processes of `table` descended from `root`.
    fn descendants_in(table: &[Proc], root: i32) -> Vec<Proc> {
        let mut children: HashMap<i32, Vec<&Proc>> = HashMap::new();
        for process in table {
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

    /// At a turn's end: for each run of `watches` whose CLI still runs, its descendants walked, added to its tree in
    /// place of those no longer running. Returns the processes of the daemon's user descended from such a CLI, or
    /// tagged with its run, outside the CLI's process group: what the turn left running in the background.
    pub(super) fn turn_ended(watches: &[Arc<Mutex<Watch>>]) -> Vec<Background> {
        let table = os::table();
        let uid = rustix::process::getuid().as_raw();
        let listed = |process: &Proc| table.iter().any(|listed| listed.is(process));
        let mut buffer = Vec::new();
        let mut tags: Option<HashMap<i32, String>> = None;
        let mut background = Vec::new();
        for watch in watches {
            let mut watch = watch.locked();
            let Some(cli) = watch.cli.filter(|cli| listed(cli)) else { continue };
            watch.tree.retain(|process| listed(process));
            let walked = descendants_in(&table, cli.pid);
            let tags = tags.get_or_insert_with(|| {
                let ours = table.iter().filter(|process| process.uid == uid && process.pid > 1);
                ours.filter_map(|process| Some((process.pid, os::tag(process.pid, &mut buffer)?))).collect()
            });
            for process in &table {
                let started = walked.iter().any(|walked| walked.is(process))
                    || tags.get(&process.pid).is_some_and(|tag| *tag == watch.tag);
                if started && process.uid == uid && process.pid != cli.pid && process.pgid != cli.pgid {
                    background.push(Background {
                        pid: process.pid,
                        start: process.start,
                        command: command(process.pid, &mut buffer),
                    });
                }
            }
            merge(&mut watch.tree, walked);
        }
        background
    }

    /// The command line of process `pid`, at most [`super::COMMAND_HEAD`] characters of it.
    fn command(pid: i32, buffer: &mut Vec<u8>) -> String {
        super::head(os::command(pid, buffer))
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
                tree.iter().any(|walked| walked.is(process))
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
            let command = command(process.pid, &mut buffer);
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
