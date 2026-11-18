//! Runs one request in a fresh `ants subprocess` worker, with an empty environment and under a deadline.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::protocol::{Request, Response};

/// Time a worker gets on top of its compute limit: to start (the first start of a new binary may take seconds on
/// macOS), to restore its session and to dump it.
pub const GRACE: Duration = Duration::from_secs(5);
/// Most stderr kept from a worker that crashed.
const STDERR_TAIL: usize = 4096;

/// A worker program: the `ants` binary, whose `subprocess` mode serves one request.
#[derive(Debug, Clone)]
pub struct Worker {
    program: PathBuf,
}

/// Why a worker gave no response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkerError {
    #[error("cannot start the worker: {0}")]
    Spawn(String),
    /// Killed past its deadline, the compute limit plus [`GRACE`].
    #[error("the worker ran past its deadline of {0:?}")]
    ComputeLimit(Duration),
    /// Ended by its allocator's hard cap.
    #[error("the worker ran out of memory")]
    OutOfMemory,
    /// Ended by a signal, as a stack overflow's abort, or with an unexpected status.
    #[error("the worker crashed ({status}): {stderr}")]
    Crashed { status: String, stderr: String },
    /// Exited normally with no readable response.
    #[error("the worker's response is unreadable: {0}")]
    Protocol(String),
}

impl Worker {
    /// The worker at `program`, as `std::env::current_exe()` for the daemon.
    #[must_use]
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self { program: program.into() }
    }

    /// Serves `request` in a new worker, killed once `deadline` passes.
    pub async fn call(&self, request: &Request, deadline: Duration) -> Result<Response, WorkerError> {
        let input = serde_json::to_vec(request).map_err(|e| WorkerError::Spawn(e.to_string()))?;
        let mut child = Command::new(&self.program)
            .arg("subprocess")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| WorkerError::Spawn(format!("{}: {e}", self.program.display())))?;
        let (Some(mut stdin), Some(mut stdout), Some(mut stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            return Err(WorkerError::Spawn("the worker's stdio is not piped".to_owned()));
        };
        let exchange = async {
            let write = async {
                // A worker that dies early closes its stdin: its status tells why, not the broken pipe.
                let _ = stdin.write_all(&input).await;
                drop(stdin);
            };
            let (mut out, mut err) = (Vec::new(), Vec::new());
            let ((), read, _) = tokio::join!(write, stdout.read_to_end(&mut out), stderr.read_to_end(&mut err));
            let status = child.wait().await;
            (status, read.map(|_| out), err)
        };
        let Ok((status, out, err)) = tokio::time::timeout(deadline, exchange).await else {
            // The exchange future, dropped, no longer borrows the child.
            let _ = child.kill().await;
            return Err(WorkerError::ComputeLimit(deadline));
        };
        let status = status.map_err(|e| WorkerError::Protocol(e.to_string()))?;
        classify(status, &out.map_err(|e| e.to_string()), &err)
    }
}

/// How long a worker may take for a request that resolves `answers` calls: the compute limit applies to each.
#[must_use]
pub fn deadline(compute: Duration, answers: usize) -> Duration {
    compute.saturating_mul(u32::try_from(answers.max(1)).unwrap_or(u32::MAX)).saturating_add(GRACE)
}

fn classify(status: ExitStatus, out: &Result<Vec<u8>, String>, err: &[u8]) -> Result<Response, WorkerError> {
    if status.code() == Some(monty_types::OOM_EXIT_CODE) {
        return Err(WorkerError::OutOfMemory);
    }
    if !status.success() {
        let stderr = String::from_utf8_lossy(&err[err.len().saturating_sub(STDERR_TAIL)..]).trim().to_owned();
        return Err(WorkerError::Crashed { status: status.to_string(), stderr });
    }
    let out = out.as_ref().map_err(|e| WorkerError::Protocol(e.clone()))?;
    serde_json::from_slice(out).map_err(|e| WorkerError::Protocol(e.to_string()))
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Where fake workers go: beside the test binary, in the target directory.
    fn dir() -> PathBuf {
        let exe = std::env::current_exe().unwrap();
        exe.parent().unwrap().join("ants-trail-runner")
    }

    /// A fake worker: a shell script.
    pub(crate) fn fake(name: &str, body: &str) -> Worker {
        let dir = dir();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        Worker::new(path)
    }

    fn request() -> Request {
        Request::Start {
            script: String::new(),
            args: serde_json::Value::Null,
            limits: crate::Limits::default(),
            clock: 0,
            seed: 0,
        }
    }

    async fn call(worker: &Worker, deadline: Duration) -> Result<Response, WorkerError> {
        worker.call(&request(), deadline).await
    }

    #[tokio::test]
    async fn outcomes_are_classified() {
        let ok = fake(
            "ok",
            r#"[ "$1" = subprocess ] && [ -z "$HOME" ] && cat >/dev/null && echo '{"done":{"result":1,"events":[]}}'"#,
        );
        assert!(matches!(call(&ok, Duration::from_secs(10)).await, Ok(Response::Done { .. })));

        let oom = fake("oom", "exit 65");
        assert!(matches!(call(&oom, Duration::from_secs(10)).await, Err(WorkerError::OutOfMemory)));

        let crash = fake("crash", "echo 'stack overflow' >&2; kill -ABRT $$");
        let Err(WorkerError::Crashed { stderr, .. }) = call(&crash, Duration::from_secs(10)).await else {
            panic!("a crash is a crash");
        };
        assert_eq!(stderr, "stack overflow");

        let garbled = fake("garbled", "echo nonsense");
        assert!(matches!(call(&garbled, Duration::from_secs(10)).await, Err(WorkerError::Protocol(_))));

        let missing = Worker::new(dir().join("no-such-worker"));
        assert!(matches!(call(&missing, Duration::from_secs(10)).await, Err(WorkerError::Spawn(_))));
    }

    #[tokio::test]
    async fn a_worker_past_its_deadline_is_killed() {
        let slow = fake("slow", "exec sleep 30");
        let started = std::time::Instant::now();
        assert!(matches!(call(&slow, Duration::from_millis(300)).await, Err(WorkerError::ComputeLimit(_))));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn the_deadline_grows_with_the_answers() {
        let compute = Duration::from_secs(10);
        assert_eq!(deadline(compute, 0), compute + GRACE);
        assert_eq!(deadline(compute, 3), 3 * compute + GRACE);
    }
}
