//! The wait for a child process that the daemon and the trail runner share: on macOS, where tokio learns of a
//! child's exit by SIGCHLD alone and so wakes the wait of every child at each child's exit, a kqueue of its own tells
//! of each child's, so each wait wakes once, as with Linux's pidfds.

#[cfg(target_os = "macos")]
mod exit;

use std::io;
use std::process::ExitStatus;

use tokio::process::Child;

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

    use tokio::process::Command;

    use super::*;

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
