//! The start of a child process and the wait for it that the daemon and the trail runner share: each starts in the
//! blocking pool ([`spawn`]), and on macOS, where tokio learns of a child's exit by SIGCHLD alone and so wakes the
//! wait of every child at each child's exit, a kqueue of its own tells of each child's, so each wait wakes once, as
//! with Linux's pidfds ([`wait`]).

#[cfg(target_os = "macos")]
mod exit;

use std::io;
use std::process::ExitStatus;

use tokio::process::{Child, Command};

/// Start `command` in the blocking pool rather than on the calling thread, which the fork and exec of the program
/// would hold for the time of the system call: a burst of sessions, each starting git, would otherwise hold the
/// runtime's one thread. The child is the runtime's, waited for and killed on drop as any other.
pub async fn spawn(mut command: Command) -> io::Result<Child> {
    tokio::task::spawn_blocking(move || command.spawn()).await.map_err(io::Error::other)?
}

/// Wait for `child` to exit, its stdin closed first, as tokio's `Child::wait` does, woken on macOS by this child's
/// exit alone.
pub async fn wait(child: &mut Child) -> io::Result<ExitStatus> {
    #[cfg(target_os = "macos")]
    if let Some(status) = exit::wait(child).await? {
        return Ok(status);
    }
    child.wait().await
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::future::Future as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_child_starts_off_the_runtime_thread() {
        use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd};

        use rustix::event::{PollFd, PollFlags, Timespec};

        // The child, before its exec, tells it began and waits for the runtime's thread to answer: a start holding
        // that thread would wait out the child's 10 seconds and fail.
        let (began, tell) = rustix::pipe::pipe().unwrap();
        let (hear, answer) = rustix::pipe::pipe().unwrap();
        let (tell_fd, hear_fd) = (tell.as_raw_fd(), hear.as_raw_fd());
        let mut command = Command::new("true");
        // SAFETY: the hook makes system calls alone, allocating nothing, on descriptors the parent keeps open until
        // the child started.
        unsafe {
            command.pre_exec(move || {
                let (tell, hear) = (BorrowedFd::borrow_raw(tell_fd), BorrowedFd::borrow_raw(hear_fd));
                rustix::io::write(tell, b"b")?;
                let mut fds = [PollFd::new(&hear, PollFlags::IN)];
                if rustix::event::poll(&mut fds, Some(&Timespec { tv_sec: 10, tv_nsec: 0 }))? == 0 {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                Ok(())
            });
        }
        let answering = tokio::spawn(async move {
            // Blocking the runtime's thread: the start goes on elsewhere or not at all.
            rustix::io::read(began.as_fd(), &mut [0; 1]).unwrap();
            rustix::io::write(answer.as_fd(), b"a").unwrap();
        });
        let mut child = spawn(command).await.expect("the child started off the runtime's thread");
        assert!(wait(&mut child).await.unwrap().success());
        answering.await.unwrap();
        drop((tell, hear));
    }

    #[tokio::test]
    async fn a_wait_wakes_at_the_exit_of_its_own_child_alone() {
        let mut child = Command::new("sleep").arg("30").kill_on_drop(true).spawn().unwrap();
        let pid = child.id().unwrap().to_string();
        let polls = Arc::new(AtomicUsize::new(0));
        let polled = polls.clone();
        let waiting = tokio::spawn(async move {
            let mut waited = std::pin::pin!(wait(&mut child));
            std::future::poll_fn(|cx| {
                polled.fetch_add(1, Ordering::Relaxed);
                waited.as_mut().poll(cx)
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        for _ in 0..20 {
            let mut other = Command::new("true").spawn().unwrap();
            assert!(wait(&mut other).await.unwrap().success());
        }
        let woken = polls.load(Ordering::Relaxed);
        assert!(woken <= 2, "the wait was polled {woken} times as 20 other children exited");
        let mut kill = Command::new("kill").args(["-KILL", &pid]).spawn().unwrap();
        assert!(wait(&mut kill).await.unwrap().success());
        let status = tokio::time::timeout(Duration::from_secs(10), waiting).await.unwrap().unwrap().unwrap();
        assert!(!status.success());
    }
}
