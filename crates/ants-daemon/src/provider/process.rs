//! An external provider's process: its command started in a process group of its own, its stderr logged, and its end
//! told with the last of that.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use super::definition::Definition;
use crate::sync::MutexExt as _;

/// The longest line of stderr logged; the rest of a longer one is logged as lines of its own.
const STDERR_LINE: usize = 1024;

/// How long a process has to exit on its own once its stdin closes before it is killed.
pub(super) const EXIT_GRACE: Duration = Duration::from_secs(1);

/// A provider's running process. Once dropped, it has [`EXIT_GRACE`] to exit, its stdin closed by then, before it
/// and what it started are killed.
pub(super) struct Process {
    child: Option<Child>,
    pid: Option<u32>,
    /// The last line it wrote to stderr.
    last: Arc<Mutex<String>>,
    stderr: Option<tokio::task::JoinHandle<()>>,
}

/// Start the provider `definition` defines, working in the daemon's directory.
pub(super) fn spawn(definition: &Definition) -> std::io::Result<(Process, ChildStdout, ChildStdin)> {
    let mut command = Command::new(&definition.command);
    command
        .args(&definition.args)
        .envs(&definition.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    let mut child = command.spawn()?;
    let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Err(std::io::Error::other("no stdio"));
    };
    let last = Arc::new(Mutex::new(String::new()));
    let stderr = child.stderr.take().map(|stderr| tokio::spawn(log(stderr, definition.name.clone(), last.clone())));
    let process = Process { pid: child.id(), child: Some(child), last, stderr };
    Ok((process, stdout, stdin))
}

/// Log the lines of a provider's stderr, keeping the last, to its end.
async fn log(stderr: impl AsyncRead + Unpin, name: String, last: Arc<Mutex<String>>) {
    let mut reader = BufReader::new(stderr);
    let mut line = Vec::new();
    loop {
        line.clear();
        match (&mut reader).take(STDERR_LINE as u64).read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let text = String::from_utf8_lossy(&line);
        let text = text.trim_end();
        if text.is_empty() {
            continue;
        }
        tracing::info!(provider = name, "stderr: {text}");
        text.clone_into(&mut last.locked());
    }
}

impl Process {
    /// Wait for it to exit; how it did, with the last line of its stderr.
    pub(super) async fn exited(&mut self) -> String {
        let Some(child) = self.child.as_mut() else { return std::future::pending().await };
        let how = match crate::process::wait(child).await {
            Ok(status) => format!("exited with {status}"),
            Err(e) => format!("could not be waited for: {e}"),
        };
        self.child = None;
        if let Some(stderr) = self.stderr.as_mut() {
            let _ = tokio::time::timeout(EXIT_GRACE, stderr).await;
        }
        let last = self.last.locked();
        if last.is_empty() { how } else { format!("{how}: {last}") }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else { return };
        if matches!(child.try_wait(), Ok(Some(_))) {
            return;
        }
        let pid = self.pid;
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if tokio::time::timeout(EXIT_GRACE, crate::process::wait(&mut child)).await.is_err() {
                        kill(pid, &mut child);
                    }
                });
            }
            Err(_) => kill(pid, &mut child),
        }
    }
}

/// Kill `child` and, on Unix, the process group it leads; either is already gone when a kill fails.
fn kill(pid: Option<u32>, child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = pid.and_then(|pid| rustix::process::Pid::from_raw(pid.cast_signed())) {
        let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
    }
    #[cfg(not(unix))]
    let _ = pid;
    let _ = child.start_kill();
}
