//! Running a program, git above all, for what it prints: every process the daemon starts and waits for outside its
//! harnesses and trails. Each starts in the blocking pool ([`spawn`]), and every child of the daemon is waited for as
//! [`wait`] does.

#[cfg(target_os = "macos")]
mod exit;

use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::process::{ExitStatus, Output, Stdio};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio::process::{Child, Command};

/// Run git in `dir` and return what it printed, or fail with what it printed on stderr.
pub(crate) async fn git<I, S>(dir: &Path, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run("git", dir, args, &[], None).await
}

/// Run git in `dir` to reach a remote, as [`git`] does, prompting for no credentials and failing after `timeout`.
pub(crate) async fn remote_git<I, S>(dir: &Path, args: I, timeout: Duration) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run("git", dir, args, &[("GIT_TERMINAL_PROMPT", "0")], Some(timeout)).await
}

/// Run `program` in `dir` with `env` added, and return what it printed, or fail with what it printed on stderr, or
/// after `timeout`, killing it.
pub(crate) async fn run<P, I, S>(
    program: P,
    dir: &Path,
    args: I,
    env: &[(&str, &str)],
    timeout: Option<Duration>,
) -> Result<String>
where
    P: AsRef<OsStr>,
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let program = program.as_ref();
    let args: Vec<S> = args.into_iter().collect();
    let shown = || {
        let words = std::iter::once(program).chain(args.iter().map(AsRef::as_ref));
        words.map(OsStr::to_string_lossy).collect::<Vec<_>>().join(" ")
    };
    let mut command = Command::new(program);
    command
        .args(&args)
        .envs(env.iter().copied())
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = output_of(command);
    let output = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, output)
            .await
            .map_err(|_| anyhow::anyhow!("{}: no answer within {}", shown(), ants_proto::duration::format(timeout)))?,
        None => output.await,
    }
    .with_context(|| format!("running {}", shown()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        if stderr.is_empty() {
            bail!("{}: {}", shown(), output.status);
        }
        bail!("{}: {stderr}", shown());
    }
    String::from_utf8(output.stdout).with_context(|| format!("{}: output is no UTF-8", shown()))
}

/// The exit code of `program` run in `dir`, what it prints dropped. None when it cannot be run or a signal ended it.
pub(crate) async fn exit_code<I, S>(program: &str, dir: &Path, args: I) -> Option<i32>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.args(args).current_dir(dir).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    wait(&mut spawn(command).await.ok()?).await.ok()?.code()
}

/// Start `command` in the blocking pool rather than on the calling thread, which the fork and exec of the program
/// would hold for the time of the system call: a burst of sessions, each starting git, would otherwise hold the
/// runtime's one thread. The child is the runtime's, waited for and killed on drop as any other.
pub(crate) async fn spawn(mut command: Command) -> io::Result<Child> {
    tokio::task::spawn_blocking(move || command.spawn()).await.map_err(io::Error::other)?
}

/// Run `command` for its exit status and what it printed, as tokio's `Command::output` does, started as [`spawn`]
/// does and waited for as [`wait`] does.
pub(crate) async fn output(mut command: Command) -> io::Result<Output> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    output_of(command).await
}

/// [`output`], the stdio of `command` as it set it.
async fn output_of(command: Command) -> io::Result<Output> {
    async fn read(pipe: Option<impl AsyncRead + Unpin>) -> io::Result<Vec<u8>> {
        let mut read = Vec::new();
        if let Some(mut pipe) = pipe {
            pipe.read_to_end(&mut read).await?;
        }
        Ok(read)
    }
    let mut child = spawn(command).await?;
    let (stdout, stderr) = (child.stdout.take(), child.stderr.take());
    let (status, stdout, stderr) = tokio::try_join!(wait(&mut child), read(stdout), read(stderr))?;
    Ok(Output { status, stdout, stderr })
}

/// Wait for `child` to exit, its stdin closed first, as tokio's `Child::wait` does. On macOS, where tokio learns of a
/// child's exit by SIGCHLD alone and so wakes the wait of every child at each child's exit, a kqueue of its own tells
/// of this child's ([`exit`]): each wait wakes once, as with Linux's pidfds.
pub(crate) async fn wait(child: &mut Child) -> io::Result<ExitStatus> {
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
    async fn a_wait_wakes_at_the_exit_of_its_own_child_alone() {
        let mut sleeping = Command::new("sleep");
        sleeping.arg("30").kill_on_drop(true);
        let mut child = spawn(sleeping).await.unwrap();
        let pid = rustix::process::Pid::from_raw(child.id().unwrap().cast_signed()).unwrap();
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
        let dir = std::env::temp_dir();
        for _ in 0..20 {
            assert_eq!(exit_code("true", &dir, std::iter::empty::<&str>()).await, Some(0));
        }
        assert_eq!(output(Command::new("true")).await.unwrap().status.code(), Some(0));
        let woken = polls.load(Ordering::Relaxed);
        assert!(woken <= 2, "the wait was polled {woken} times as 21 other children exited");
        rustix::process::kill_process(pid, rustix::process::Signal::KILL).unwrap();
        let status = tokio::time::timeout(Duration::from_secs(10), waiting).await.unwrap().unwrap().unwrap();
        assert!(!status.success());
    }
}
