//! A native CLI's process and the JSON lines it speaks on stdio.

use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::Error;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use super::Cli;
use crate::sync::MutexExt as _;

/// The last bytes of a process's stderr, and the task reading it.
type Stderr = (Arc<Mutex<VecDeque<u8>>>, tokio::task::JoinHandle<()>);

/// The most of its stderr a process keeps, to name in the error of its exit.
const STDERR_TAIL: usize = 16 * 1024;

/// How many reads of a process's stderr in a row may fail before it is no longer read.
const STDERR_FAILURES: u32 = 10;

/// How long a process left behind has to exit on its own once its stdin closes before it is killed.
const EXIT_GRACE: Duration = Duration::from_secs(1);

/// The longest line read; a longer one fails the input.
const MAX_LINE: usize = 64 * 1024 * 1024;

impl Cli {
    /// The CLI's command: its executable, with `protocol_args` before the definition's arguments and those of the ant's
    /// `context`, the definition's environment with the ant's over it, added to the daemon's. Its stdio is piped, it
    /// runs without a shell, in a process group of its own on Unix (so a stop reaches what it starts) and without a
    /// console window on Windows, and it is killed when dropped.
    pub(crate) fn command<I, S>(&self, protocol_args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut command = Command::new(&self.program);
        command
            .args(protocol_args)
            .args(&self.args)
            .envs(&self.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(windows)]
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
        command
    }

    /// Start the CLI ([`Self::command`]) with `protocol_args`, working in `cwd`, else in the daemon's directory.
    pub(crate) fn spawn<I, S>(&self, protocol_args: I, cwd: Option<&Path>) -> Result<Process, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut command = self.command(protocol_args);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        let mut child = command
            .spawn()
            .map_err(|e| super::failed(format!("{}: starting {}: {e}", self.name(), self.program.display())))?;
        let stderr = child.stderr.take().map(|stderr| {
            let tail = Arc::new(Mutex::new(VecDeque::new()));
            let task = tokio::spawn(drain(stderr, self.name().to_owned(), tail.clone()));
            (tail, task)
        });
        Ok(Process {
            name: self.name().to_owned(),
            pid: child.id(),
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            child: Some(child),
            stderr,
        })
    }
}

/// How long a CLI's `--version` has to answer.
const VERSION_TIMEOUT: Duration = Duration::from_secs(30);

/// Run the CLI's `--version`, without the definition's arguments: what it printed.
pub(crate) async fn probe_version(cli: &Cli) -> Result<std::process::Output, Error> {
    let bare = Cli { args: Vec::new(), ..cli.clone() };
    let program = cli.program().display();
    match tokio::time::timeout(VERSION_TIMEOUT, bare.command(["--version"]).output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(super::failed(format!("{}: starting {program}: {e}", cli.name()))),
        Err(_) => {
            Err(super::failed(format!("{}: {program} --version did not answer within {VERSION_TIMEOUT:?}", cli.name())))
        }
    }
}

/// Whether dotted `version` is older than `oldest`, comparing the leading numbers of its first three parts (split at
/// `.` and `-`), a missing one counting as 0.
pub(crate) fn older(version: &str, oldest: [u64; 3]) -> bool {
    let parts = version.split(['.', '-']).take(3).map(|part| {
        let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
        digits.parse::<u64>().unwrap_or_default()
    });
    let mut numbers = [0; 3];
    for (number, part) in numbers.iter_mut().zip(parts) {
        *number = part;
    }
    numbers < oldest
}

/// Log the lines of a process's stderr and keep their tail, reading to its end: a pipe closed early would fail the
/// process's next write. Bytes that are not UTF-8 are replaced; a line longer than [`STDERR_TAIL`] is taken in pieces.
async fn drain(stderr: impl AsyncRead + Unpin, name: String, tail: Arc<Mutex<VecDeque<u8>>>) {
    let mut reader = BufReader::new(stderr);
    let mut line = Vec::new();
    let mut failures = 0;
    loop {
        line.clear();
        let read = (&mut reader).take(STDERR_TAIL as u64).read_until(b'\n', &mut line).await;
        match read {
            Ok(0) => break,
            Ok(_) => failures = 0,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                failures += 1;
                if failures >= STDERR_FAILURES {
                    tracing::debug!(harness = name, error = %e, "giving up reading stderr");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if line.is_empty() {
            continue;
        }
        let text = String::from_utf8_lossy(&line);
        let text = text.trim_end_matches(['\n', '\r']);
        tracing::debug!(harness = name, "stderr: {text}");
        let mut tail = tail.locked();
        tail.extend(text.bytes().chain(std::iter::once(b'\n')));
        let excess = tail.len().saturating_sub(STDERR_TAIL);
        tail.drain(..excess);
    }
}

/// A running native CLI. Dropped while it runs, its stdin closes, and unless it exits within a second it is killed with
/// what it started.
pub(crate) struct Process {
    name: String,
    pid: Option<u32>,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<Stderr>,
}

impl Process {
    /// Its stdin and stdout, to speak to it through; taken once.
    pub(crate) fn take_io(&mut self) -> Option<(ChildStdin, ChildStdout)> {
        Some((self.stdin.take()?, self.stdout.take()?))
    }

    /// The last of what it wrote to stderr.
    pub(crate) fn stderr(&self) -> String {
        let Some((tail, _)) = &self.stderr else { return String::new() };
        let bytes: Vec<u8> = tail.locked().iter().copied().collect();
        String::from_utf8_lossy(&bytes).trim().to_owned()
    }

    /// Interrupt it as Ctrl-C does, with SIGINT; false where no signal can be sent, as on Windows.
    pub(crate) fn interrupt(&self) -> bool {
        #[cfg(unix)]
        if let Some(pid) = self.pid.and_then(|pid| rustix::process::Pid::from_raw(pid.cast_signed())) {
            return rustix::process::kill_process(pid, rustix::process::Signal::INT).is_ok();
        }
        false
    }

    /// Wait for it to exit, as it does once its output ended, and describe how it did with the last of its stderr: the
    /// error to answer with when its output ends early. Its stderr may say it is not signed in, which the daemon
    /// recognizes.
    pub(crate) async fn exited(&mut self) -> Error {
        let status = match self.child.as_mut() {
            Some(child) => tokio::time::timeout(EXIT_GRACE, child.wait()).await.ok().and_then(Result::ok),
            None => None,
        };
        if let Some((_, task)) = self.stderr.as_mut() {
            // What the stderr reader kept so far serves when it does not finish in time.
            let _ = tokio::time::timeout(EXIT_GRACE, task).await;
        }
        let how = match status {
            Some(status) => format!("exited with {status}"),
            None => "closed its output".to_owned(),
        };
        let stderr = self.stderr();
        if stderr.is_empty() {
            super::failed(format!("{} {how}", self.name))
        } else {
            super::failed(format!("{} {how}: {stderr}", self.name))
        }
    }

    /// Close its stdin and wait up to `grace` for it to exit, then kill it and what it started. Its exit status, if it
    /// exited on its own.
    pub(crate) async fn stop(&mut self, grace: Duration) -> Option<ExitStatus> {
        self.stdin = None;
        let child = self.child.as_mut()?;
        let status = tokio::time::timeout(grace, child.wait()).await.ok().and_then(Result::ok);
        if status.is_none() {
            kill(self.pid, child);
            let _ = child.wait().await;
        }
        status
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stdin = None;
        let Some(mut child) = self.child.take() else { return };
        if matches!(child.try_wait(), Ok(Some(_))) {
            return;
        }
        let pid = self.pid;
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if tokio::time::timeout(EXIT_GRACE, child.wait()).await.is_err() {
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

/// Pass `event`, read of a CLI, on to its reader: the session or prompt it is for. A reader gone means that ended, so
/// what the CLI still prints for it is dropped, as every translator does.
pub(crate) fn forward<T>(events: &tokio::sync::mpsc::UnboundedSender<T>, event: T) {
    if events.send(event).is_err() {
        tracing::debug!("dropping a CLI event: the session or prompt it is for ended");
    }
}

/// JSON values read one per line.
pub(crate) struct JsonLines<R> {
    reader: BufReader<R>,
    line: Vec<u8>,
}

impl<R: AsyncRead + Unpin> JsonLines<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self { reader: BufReader::new(reader), line: Vec::new() }
    }

    /// The next value, skipping blank lines and, logged, lines that are not JSON; None at the end of the input.
    pub(crate) async fn next(&mut self) -> io::Result<Option<Value>> {
        loop {
            self.line.clear();
            if !self.read_line().await? {
                return Ok(None);
            }
            let line = self.line.trim_ascii();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice(line) {
                Ok(value) => return Ok(Some(value)),
                Err(e) => tracing::warn!(error = %e, line = %String::from_utf8_lossy(line), "skipping a line not JSON"),
            }
        }
    }

    /// Read a line into `self.line`; false at the end of the input.
    async fn read_line(&mut self) -> io::Result<bool> {
        loop {
            let buf = self.reader.fill_buf().await?;
            if buf.is_empty() {
                return Ok(!self.line.is_empty());
            }
            let (part, done) = match buf.iter().position(|&b| b == b'\n') {
                Some(end) => (&buf[..=end], true),
                None => (buf, false),
            };
            if self.line.len() + part.len() > MAX_LINE {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "a line longer than 64 MiB"));
            }
            self.line.extend_from_slice(part);
            let used = part.len();
            self.reader.consume(used);
            if done {
                return Ok(true);
            }
        }
    }
}

/// Writes JSON values one per line; shared by the tasks writing to one CLI.
pub(crate) struct JsonWriter<W> {
    /// None once closed.
    writer: tokio::sync::Mutex<Option<W>>,
}

impl<W: AsyncWrite + Unpin> JsonWriter<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self { writer: tokio::sync::Mutex::new(Some(writer)) }
    }

    /// Write `value` as one line and flush it.
    pub(crate) async fn send(&self, value: &impl Serialize) -> io::Result<()> {
        let mut line = serde_json::to_vec(value)?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        let writer = writer.as_mut().ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "closed"))?;
        writer.write_all(&line).await?;
        writer.flush().await
    }

    /// Close the output, which a CLI reading prompts until the end of its input takes as the end of the session: the
    /// writer is dropped, as shutting a child's stdin down does not close it.
    pub(crate) async fn close(&self) -> io::Result<()> {
        let Some(mut writer) = self.writer.lock().await.take() else { return Ok(()) };
        writer.shutdown().await
    }
}

/// The longest a request on a loopback listener ([`get_loopback`]) may take.
const LOOPBACK_TIMEOUT: Duration = Duration::from_secs(30);

/// Request `url`, on a CLI's loopback listener, as a browser coming back to it would; fails unless the listener
/// answers with a success or a redirect.
pub(crate) async fn get_loopback(url: &str) -> anyhow::Result<()> {
    use anyhow::Context as _;
    let uri: hyper::Uri = url.parse().context("the address")?;
    let host = uri.host().context("the address names no host")?;
    anyhow::ensure!(uri.scheme_str() == Some("http") && is_loopback(host), "{url} is not on this machine's loopback");
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let path = uri.path_and_query().map_or("/", hyper::http::uri::PathAndQuery::as_str);
    let authority = uri.authority().map_or(host, hyper::http::uri::Authority::as_str);
    let request = async {
        let mut tcp = tokio::net::TcpStream::connect((host, uri.port_u16().unwrap_or(80))).await?;
        let head = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: ants\r\nConnection: close\r\n\r\n");
        tcp.write_all(head.as_bytes()).await?;
        let mut status = Vec::new();
        let mut reader = BufReader::new(tcp);
        reader.read_until(b'\n', &mut status).await?;
        io::Result::Ok(String::from_utf8_lossy(&status).into_owned())
    };
    let status = tokio::time::timeout(LOOPBACK_TIMEOUT, request).await.context("timed out")?.context("requesting")?;
    let code = status.split_whitespace().nth(1).and_then(|code| code.parse::<u16>().ok());
    anyhow::ensure!(code.is_some_and(|code| (200..400).contains(&code)), "answered {}", status.trim());
    Ok(())
}

/// Whether `host` (an address or a name) is this machine's loopback.
pub(crate) fn is_loopback(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost") || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stderr_is_read_to_its_end_past_bytes_not_utf8() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let tail = Arc::new(Mutex::new(VecDeque::new()));
        let task = tokio::spawn(drain(reader, "cli".to_owned(), tail.clone()));
        writer.write_all(b"first\n\xff\xfe not UTF-8\n").await.unwrap();
        for n in 0..1000 {
            writer.write_all(format!("line {n}\n").as_bytes()).await.expect("stderr still read");
        }
        drop(writer);
        task.await.unwrap();
        let tail: Vec<u8> = tail.locked().iter().copied().collect();
        let tail = String::from_utf8(tail).unwrap();
        assert!(tail.starts_with("first\n\u{fffd}\u{fffd} not UTF-8\nline 0\n"), "{tail}");
        assert!(tail.ends_with("line 999\n"), "{tail}");
    }
}
