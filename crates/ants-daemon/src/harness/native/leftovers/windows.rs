//! A native CLI's job object on Windows, which kills the processes left in it once closed, and lists those it holds
//! as a turn ends.

use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::sync::Mutex;

use tokio::process::Child;
use windows_sys::Win32::Foundation::{ERROR_MORE_DATA, FILETIME, GetLastError, WAIT_TIMEOUT};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_BASIC_PROCESS_ID_LIST,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicProcessIdList, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject,
};
use windows_sys::Win32::System::Threading::{
    GetProcessTimes, OpenProcess, PROCESS_NAME_WIN32, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    QueryFullProcessImageNameW, WaitForSingleObject,
};

use super::job::Listed;
use crate::harness::background::Background;
use crate::sync::MutexExt as _;

/// A job object holding a CLI and what it starts, ended when dropped (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`).
pub(super) struct Job {
    handle: OwnedHandle,
    /// The CLI's process ID.
    cli: i32,
    /// The processes it held as the last turn began, by pid and start.
    began: Mutex<Vec<(i32, u64)>>,
}

impl Job {
    /// A job object of its own for `child`, the CLI of harness `name`; None, logged, where it cannot be made.
    pub(super) fn of(name: &str, child: &Child) -> Option<Self> {
        let made = Self::handle().and_then(|handle| {
            let process = child.raw_handle().ok_or("the CLI exited")?;
            let cli = child.id().and_then(|pid| i32::try_from(pid).ok()).ok_or("the CLI exited")?;
            // SAFETY: both handles are open: the job's held by `handle`, the process's by `child`.
            if unsafe { AssignProcessToJobObject(handle.as_raw_handle(), process) } == 0 {
                return Err("assigning the CLI to it failed");
            }
            Ok(Self { handle, cli, began: Mutex::default() })
        });
        match made {
            Ok(job) => Some(job),
            Err(e) => {
                tracing::warn!(harness = name, error = e, "no job object for the CLI: what it starts may outlive it");
                None
            }
        }
    }

    /// A new job object's handle, set to kill what it holds once closed.
    fn handle() -> Result<OwnedHandle, &'static str> {
        // SAFETY: no security attributes nor name are given.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err("creating it failed");
        }
        // SAFETY: the handle was just created and is owned by nothing else.
        let job = unsafe { OwnedHandle::from_raw_handle(handle) };
        // SAFETY: the structure is plain integers, for which zero is a value.
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let size = u32::try_from(std::mem::size_of_val(&limits)).map_err(|_| "its limits do not fit")?;
        // SAFETY: the information is the structure its class names, of `size` bytes.
        let set = unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                size,
            )
        };
        if set == 0 {
            return Err("setting it to kill what it holds once closed failed");
        }
        Ok(job)
    }

    /// Note the processes it holds as a turn begins.
    pub(super) fn turn_started(&self) {
        let began = self.ids().into_iter().filter_map(|pid| Some((pid, open(pid)?.1))).collect();
        *self.began.locked() = began;
    }

    /// The job as a turn ends: its CLI, the processes it held as the turn began, and those it holds now that run.
    pub(super) fn listed(&self) -> Listed {
        let now = self.ids().into_iter().filter_map(process).collect();
        Listed { cli: self.cli, began: self.began.locked().clone(), now }
    }

    /// The IDs of the processes it holds; none where they cannot be listed.
    fn ids(&self) -> Vec<i32> {
        let header =
            std::mem::offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList) / std::mem::size_of::<usize>();
        let mut capacity = 64;
        for _ in 0..4 {
            // Words, so that the structure starting it is aligned.
            let mut buffer = vec![0usize; header + capacity];
            let Ok(size) = u32::try_from(std::mem::size_of_val(buffer.as_slice())) else { break };
            // SAFETY: the buffer, aligned for the structure, holds `size` bytes.
            let listed = unsafe {
                QueryInformationJobObject(
                    self.handle.as_raw_handle(),
                    JobObjectBasicProcessIdList,
                    buffer.as_mut_ptr().cast(),
                    size,
                    std::ptr::null_mut(),
                )
            } != 0;
            // SAFETY: the buffer, zeroed first, holds the structure, its list of at least one ID included.
            let list = unsafe { &*buffer.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() };
            let (assigned, in_list) = (list.NumberOfAssignedProcesses as usize, list.NumberOfProcessIdsInList as usize);
            // SAFETY: no other call of this thread came in between.
            if !listed && unsafe { GetLastError() } != ERROR_MORE_DATA {
                break;
            }
            if !listed || assigned > capacity {
                capacity = assigned.max(capacity) + 16;
                continue;
            }
            let ids = buffer.get(header..header + in_list.min(capacity)).unwrap_or_default();
            return ids.iter().filter_map(|&id| i32::try_from(id).ok()).collect();
        }
        Vec::new()
    }
}

/// Process `pid` if it runs: its start (its creation time, in 100 ns since 1601) and the path of its executable.
pub(super) fn process(pid: i32) -> Option<Background> {
    let (process, start) = open(pid)?;
    Some(Background { pid, start, command: super::head(image(&process)) })
}

/// Process `pid` opened, if it runs, and its start.
fn open(pid: i32) -> Option<(OwnedHandle, u64)> {
    let id = u32::try_from(pid).ok()?;
    // SAFETY: the handle is not inherited, and the call takes no pointer.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE, 0, id) };
    if handle.is_null() {
        return None;
    }
    // SAFETY: the handle was just opened and is owned by nothing else.
    let process = unsafe { OwnedHandle::from_raw_handle(handle) };
    // SAFETY: the handle is open, with the right to wait on it; a wait of 0 ms only tells whether it exited.
    if unsafe { WaitForSingleObject(process.as_raw_handle(), 0) } != WAIT_TIMEOUT {
        return None;
    }
    let mut times = [FILETIME::default(); 4];
    let [created, exited, kernel, user] = &mut times;
    // SAFETY: the handle is open, with the right to query it, and each time points to a FILETIME.
    if unsafe { GetProcessTimes(process.as_raw_handle(), created, exited, kernel, user) } == 0 {
        return None;
    }
    let start = (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime);
    Some((process, start))
}

/// The path of the executable `process` runs; empty where it cannot be read.
fn image(process: &OwnedHandle) -> String {
    let mut path = [0u16; 1024];
    let mut size = u32::try_from(path.len()).unwrap_or(0);
    // SAFETY: the handle is open, with the right to query it, and the buffer holds `size` characters.
    let read = unsafe {
        QueryFullProcessImageNameW(process.as_raw_handle(), PROCESS_NAME_WIN32, path.as_mut_ptr(), &mut size)
    };
    let read = usize::try_from(size).ok().filter(|_| read != 0).and_then(|size| path.get(..size));
    read.map_or_default(String::from_utf16_lossy)
}

#[cfg(test)]
mod tests {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use super::super::{Leftovers, Runs};

    /// The environment variable making [`a_cli_starting_a_loop`] start its loop.
    const CLI: &str = "ANTS_TEST_JOB_CLI";

    /// The environment variable making [`a_loop`] loop.
    const LOOP: &str = "ANTS_TEST_JOB_LOOP";

    /// A run of the ignored test `test` of this module, with no input nor output.
    fn this(test: &str) -> Command {
        let test = format!("{}::{test}", module_path!().split_once("::").unwrap().1);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", &test, "--ignored", "--quiet", "--test-threads=1"]);
        command.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        command
    }

    /// Not a test: a CLI leaving a loop ([`a_loop`]) running, run by
    /// [`what_a_turn_leaves_in_the_job_is_told_and_ends_at_the_release`].
    #[test]
    #[ignore = "run by the test of what a turn leaves in a job object"]
    fn a_cli_starting_a_loop() {
        if std::env::var_os(CLI).is_none() {
            return;
        }
        let mut looping = this("a_loop").env(LOOP, "1").spawn().unwrap();
        let _ = looping.wait();
    }

    /// Not a test: a loop running until killed, started by [`a_cli_starting_a_loop`].
    #[test]
    #[ignore = "run by the test of what a turn leaves in a job object"]
    fn a_loop() {
        while std::env::var_os(LOOP).is_some() {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    #[tokio::test]
    async fn what_a_turn_leaves_in_the_job_is_told_and_ends_at_the_release() {
        let runs = Runs::default();
        runs.turn_started();
        let mut command = tokio::process::Command::from(this("a_cli_starting_a_loop"));
        command.env(CLI, "1").kill_on_drop(true);
        let mut leftovers = Leftovers::tag("job", &mut command);
        let child = command.spawn().unwrap();
        leftovers.follow(&child);
        leftovers.watched_by(&runs);
        let cli = i32::try_from(child.id().unwrap()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let left = loop {
            let left = runs.turn_ended().await;
            if !left.is_empty() || Instant::now() > deadline {
                break left;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let [looping] = left.as_slice() else { panic!("the loop alone is left running: {left:?}") };
        assert_ne!(looping.pid, cli);
        assert!(looping.command.ends_with(".exe"), "{looping:?}");
        assert!(looping.runs());

        runs.turn_started();
        assert_eq!(runs.turn_ended().await, left, "what the turn before left running still counts");

        leftovers.release().await;
        let deadline = Instant::now() + Duration::from_secs(30);
        while looping.runs() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!looping.runs(), "the job ends the loop as it is released");
        drop(child);
    }
}
