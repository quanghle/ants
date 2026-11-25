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
    /// Its exit status or its response could not be read.
    #[error("cannot read the worker: {0}")]
    Io(String),
    /// Exited normally with a response that is no [`Response`]: the same request would bring it again.
    #[error("the worker's response is unreadable: {0}")]
    Protocol(String),
    /// Wrote more than any response to the request takes, in bytes; the rest is not read.
    #[error("the worker's response passes the {0} bytes a response to its request takes")]
    Oversized(usize),
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
        let most = crate::worker::response_bound(match request {
            Request::Start { .. } => 1,
            Request::Resume { answers, .. } => answers.len(),
        });
        let mut command = Command::new(&self.program);
        command
            .arg("subprocess")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = ants_process::spawn(command)
            .await
            .map_err(|e| WorkerError::Spawn(format!("{}: {e}", self.program.display())))?;
        let (Some(mut stdin), Some(stdout), Some(mut stderr)) =
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
            // Past the bound the pipe closes, which ends a worker still writing.
            let read = async move {
                let mut out = Vec::new();
                stdout
                    .take(u64::try_from(most).unwrap_or(u64::MAX).saturating_add(1))
                    .read_to_end(&mut out)
                    .await
                    .map(|_| out)
            };
            let tail = async {
                let (mut tail, mut buf) = (Vec::new(), [0; 4096]);
                while let Ok(n @ 1..) = stderr.read(&mut buf).await {
                    tail.extend_from_slice(&buf[..n]);
                    if tail.len() > 2 * STDERR_TAIL {
                        tail.drain(..tail.len() - STDERR_TAIL);
                    }
                }
                tail
            };
            let ((), read, err) = tokio::join!(write, read, tail);
            let status = ants_process::wait(&mut child).await;
            (status, read, err)
        };
        let Ok((status, out, err)) = tokio::time::timeout(deadline, exchange).await else {
            // The exchange future, dropped, no longer borrows the child.
            let _ = child.kill().await;
            return Err(WorkerError::ComputeLimit(deadline));
        };
        if out.as_ref().is_ok_and(|out| out.len() > most) {
            return Err(WorkerError::Oversized(most));
        }
        let status = status.map_err(|e| WorkerError::Io(e.to_string()))?;
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
    let out = out.as_ref().map_err(|e| WorkerError::Io(e.clone()))?;
    serde_json::from_slice(out).map_err(|e| WorkerError::Protocol(e.to_string()))
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::*;

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
        ants_test_support::executable(&path, &format!("#!/bin/sh\n{body}\n"));
        Worker::new(path)
    }

    fn request() -> Request {
        Request::Start {
            script: String::new(),
            entry: None,
            args: serde_json::Value::Null,
            limits: crate::Limits::default(),
            clock: 0,
            seed: 0,
            watcher: false,
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

    #[tokio::test]
    async fn a_response_past_its_bound_is_refused_and_the_largest_valid_one_read() {
        let flood = fake("flood", "cat >/dev/null; exec yes");
        let started = std::time::Instant::now();
        let most = crate::worker::response_bound(1);
        assert_eq!(call(&flood, Duration::from_secs(30)).await.unwrap_err(), WorkerError::Oversized(most));
        assert!(started.elapsed() < Duration::from_secs(10));

        // A session at its limit, the calls of one resume at theirs, notes at theirs, escaped at worst.
        let session = format!("[\"{}\"]", "s".repeat(crate::Limits::MAX_DUMP_BYTES - 4));
        let args = serde_json::json!({"prompt": "p".repeat(crate::worker::CALL_BYTES - 64 - 13)});
        let calls = vec![crate::Call { call: u32::MAX, function: ants_proto::TrailFunction::Agent, args }];
        let events = vec![crate::Event::Log("\u{1}".repeat(crate::worker::NOTE_BYTES - 16))];
        let snapshot = serde_json::value::RawValue::from_string(session).unwrap();
        let largest = Response::Suspended { snapshot, calls, pending: vec![u32::MAX], events };
        let path = dir().join("largest.json");
        std::fs::write(&path, serde_json::to_vec(&largest).unwrap()).unwrap();
        let read = fake("largest", &format!("cat >/dev/null; cat '{}'", path.display()));
        assert!(matches!(call(&read, Duration::from_secs(30)).await, Ok(Response::Suspended { .. })));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn the_wait_for_a_worker_wakes_at_its_exit_alone() {
        use std::future::Future as _;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let pid_file = dir().join("waiting.pid");
        let _ = std::fs::remove_file(&pid_file);
        let waiting = fake(
            "waiting",
            &format!(
                "cat >/dev/null; echo $$ > '{}.new'; mv '{0}.new' '{0}'; exec sleep 30 >&- 2>&-",
                pid_file.display()
            ),
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let polled = polls.clone();
        let calling = tokio::spawn(async move {
            let mut called = std::pin::pin!(call(&waiting, Duration::from_secs(60)));
            std::future::poll_fn(|cx| {
                polled.fetch_add(1, Ordering::Relaxed);
                called.as_mut().poll(cx)
            })
            .await
        });
        let pid = loop {
            if let Ok(pid) = std::fs::read_to_string(&pid_file) {
                break pid.trim().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        // Its stdout and stderr closed, the worker's wait is all the call waits for.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let before = polls.load(Ordering::Relaxed);
        for _ in 0..20 {
            let mut other = Command::new("true").spawn().unwrap();
            assert!(ants_process::wait(&mut other).await.unwrap().success());
        }
        let woken = polls.load(Ordering::Relaxed) - before;
        assert!(woken <= 1, "the call was polled {woken} times as 20 other children exited");
        let mut kill = Command::new("kill").args(["-KILL", &pid]).spawn().unwrap();
        assert!(ants_process::wait(&mut kill).await.unwrap().success());
        let called = tokio::time::timeout(Duration::from_secs(10), calling).await.unwrap().unwrap();
        assert!(matches!(called, Err(WorkerError::Crashed { .. })), "{called:?}");
    }

    #[test]
    fn the_deadline_grows_with_the_answers() {
        let compute = Duration::from_secs(10);
        assert_eq!(deadline(compute, 0), compute + GRACE);
        assert_eq!(deadline(compute, 3), 3 * compute + GRACE);
    }
}
