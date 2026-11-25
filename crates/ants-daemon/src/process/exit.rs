//! A child's exit on macOS, told by a kqueue of its own (`EVFILT_PROC`, `NOTE_EXIT`) rather than by SIGCHLD, which
//! tokio wakes the wait of every child at.

use std::io;
use std::os::fd::OwnedFd;
use std::process::ExitStatus;
use std::time::Duration;

use rustix::event::kqueue::{Event, EventFilter, EventFlags, ProcessEvents, kevent, kqueue};
use rustix::process::Pid;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::process::Child;

/// Wait for `child` to exit, its stdin closed first: its exit status, or None for tokio's own wait to take over, when
/// no kqueue watches it or it cannot be reaped yet once one told of its exit.
pub(super) async fn wait(child: &mut Child) -> io::Result<Option<ExitStatus>> {
    drop(child.stdin.take());
    if let Some(status) = child.try_wait()? {
        return Ok(Some(status));
    }
    let Some(pid) = child.id() else { return Ok(None) };
    let exit = match watch(pid) {
        Ok(exit) => exit,
        Err(e) => {
            tracing::debug!(pid, error = %e, "waiting for a child by SIGCHLD: no kqueue watches it");
            return Ok(None);
        }
    };
    // It may have exited before the watch began.
    if let Some(status) = child.try_wait()? {
        return Ok(Some(status));
    }
    if exit.readable().await.is_err() {
        return Ok(None);
    }
    child.try_wait()
}

/// A kqueue readable once process `pid` exited.
fn watch(pid: u32) -> io::Result<AsyncFd<OwnedFd>> {
    let pid = Pid::from_raw(pid.cast_signed()).ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let queue = kqueue()?;
    let exit = Event::new(
        EventFilter::Proc { pid, flags: ProcessEvents::EXIT },
        EventFlags::ADD | EventFlags::ONESHOT,
        std::ptr::null_mut(),
    );
    let mut none: [Event; 0] = [];
    // SAFETY: the event names a process, no file descriptor that would have to outlive the kqueue.
    unsafe { kevent(&queue, &[exit], &mut none, Some(Duration::ZERO)) }?;
    // SAFETY: the kqueue is open, owned by the AsyncFd, and closed only as it drops.
    Ok(unsafe { AsyncFd::register_with_interest(queue, Interest::READABLE) }?)
}
