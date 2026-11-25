//! A native CLI's job object on Windows, which kills the processes left in it once closed.

use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};

use tokio::process::Child;
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation, SetInformationJobObject,
};

/// A job object holding a CLI and what it starts, ended when dropped (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`).
pub(super) struct Job(OwnedHandle);

impl Job {
    /// A job object of its own for `child`, the CLI of harness `name`; None, logged, where it cannot be made.
    pub(super) fn of(name: &str, child: &Child) -> Option<Self> {
        let made = Self::new().and_then(|job| {
            let process = child.raw_handle().ok_or("the CLI exited")?;
            // SAFETY: both handles are open: the job's held by `job`, the process's by `child`.
            if unsafe { AssignProcessToJobObject(job.0.as_raw_handle(), process) } == 0 {
                return Err("assigning the CLI to it failed");
            }
            Ok(job)
        });
        match made {
            Ok(job) => Some(job),
            Err(e) => {
                tracing::warn!(harness = name, error = e, "no job object for the CLI: what it starts may outlive it");
                None
            }
        }
    }

    fn new() -> Result<Self, &'static str> {
        // SAFETY: no security attributes nor name are given.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err("creating it failed");
        }
        // SAFETY: the handle was just created and is owned by nothing else.
        let job = Self(unsafe { OwnedHandle::from_raw_handle(handle) });
        // SAFETY: the structure is plain integers, for which zero is a value.
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let size = u32::try_from(std::mem::size_of_val(&limits)).map_err(|_| "its limits do not fit")?;
        // SAFETY: the information is the structure its class names, of `size` bytes.
        let set = unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle(),
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
}
