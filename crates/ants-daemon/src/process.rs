//! Running a program, git above all, for what it prints: every process the daemon starts and waits for outside its
//! harnesses and trails. Each starts in the blocking pool ([`spawn`]).

use std::ffi::OsStr;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};

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
    let mut command = tokio::process::Command::new(program);
    command
        .args(&args)
        .envs(env.iter().copied())
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = async { spawn(command).await?.wait_with_output().await };
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
    let mut command = tokio::process::Command::new(program);
    command.args(args).current_dir(dir).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    spawn(command).await.ok()?.wait().await.ok()?.code()
}

/// Start `command` in the blocking pool rather than on the calling thread, which the fork and exec of the program
/// would hold for the time of the system call: a burst of sessions, each starting git, would otherwise hold the
/// runtime's one thread. The child is the runtime's, waited for and killed on drop as any other.
pub(crate) async fn spawn(mut command: tokio::process::Command) -> std::io::Result<tokio::process::Child> {
    tokio::task::spawn_blocking(move || command.spawn()).await.map_err(std::io::Error::other)?
}
