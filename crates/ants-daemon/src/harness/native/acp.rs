//! A harness speaking ACP itself, to which the daemon's ACP client speaks on stdio without a translator: its process
//! started, waited for and released as a native CLI's ([`Process`]), so what it left running, such as the commands it
//! ran in the background in process groups of their own, ends with it.
//!
//! Its connection ended, its stdin is closed and it has [`EXIT_GRACE`] to exit, as ACP's stdio transport has it, then
//! SIGTERM to its process group and [`TERMINATE_GRACE`] more before it is killed with what it started.
//!
//! As it answers a prompt, its turn ended, its descendants are walked as a native CLI's are ([`Turns`]), and the answer
//! lists what the turn left running in the background ([`crate::harness::background`]). The turns it runs of its own
//! between prompts are reported and kept for the prompts taking them ([`own_turns`]).

mod own_turns;

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::v1::schema::PromptResponse;
use agent_client_protocol::{Agent, Channel, Client, ConnectTo, ConnectionDriver, DynConnectTo, Lines};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::oneshot;

use self::own_turns::OwnTurns;
use super::leftovers::Runs;
use super::{Cli, Process};
use crate::harness::background;
use crate::sync::MutexExt as _;

/// How long a harness whose stdin closed has to exit on its own.
const EXIT_GRACE: Duration = Duration::from_secs(1);
/// How long a harness sent SIGTERM has to exit before it is killed.
const TERMINATE_GRACE: Duration = Duration::from_secs(2);
/// How long the connection to a harness that exited has to end on its own.
const CLOSE_GRACE: Duration = Duration::from_secs(1);
/// How long the end of a harness's output waits for its exit to be told ([`lines`]).
const SETTLE_LIMIT: Duration = Duration::from_secs(5);

/// What the daemon's ACP client connects to for `cli`, a harness speaking ACP.
pub(super) fn connect(cli: Cli) -> DynConnectTo<Client> {
    DynConnectTo::new(Acp(cli))
}

/// A harness speaking ACP, not started yet.
struct Acp(Cli);

impl ConnectTo<Client> for Acp {
    async fn connect_to(self, client: impl ConnectTo<Agent>) -> agent_client_protocol::Result<()> {
        let (channel, driver) = self.into_channel_and_future();
        let relay = ConnectTo::<Client>::connect_to(channel, client);
        match driver {
            Some(driver) => futures::try_join!(relay, driver).map(drop),
            None => relay.await,
        }
    }

    /// The channel to the harness and the work driving it, its process started on the calling thread:
    /// [`crate::harness::Launch::transport`] calls this in the blocking pool.
    fn into_channel_and_future(self) -> (Channel, Option<ConnectionDriver>) {
        let Self(cli) = self;
        let started = Process::start_here(cli.name(), cli.program(), cli.command(std::iter::empty::<&str>()));
        let (process, stdin, stdout) = match started {
            Ok(started) => started,
            Err(error) => {
                let (channel, peer) = Channel::duplex();
                let driver = ConnectionDriver::new(async move {
                    drop(peer);
                    Err(error)
                });
                return (channel, Some(driver));
            }
        };
        process.watched_by(cli.runs());
        let turns = Turns::new(cli.runs());
        let own = cli.own_turns().map(|report| Arc::new(OwnTurns::new(cli.name(), report)));
        let (settled, settling) = oneshot::channel();
        let lines = lines(stdin, stdout, turns, own, settling);
        let (channel, transport) = ConnectTo::<Client>::into_channel_and_future(lines);
        let Some(transport) = transport else { unreachable!("a line transport drives its own I/O") };
        let (finish, finishing) = oneshot::channel();
        let name = cli.name().to_owned();
        let driver =
            ConnectionDriver::with_finish(supervise(name, process, transport, finishing, settled), move || {
                let _ = finish.send(());
            });
        (channel, Some(driver))
    }
}

/// The JSON-RPC lines of a harness's stdio, its `turns` followed, and those of its `own`, if it reports them. The end
/// of its output waits, up to [`SETTLE_LIMIT`], until `settling` tells that its exit, if it exited, was told: a harness
/// failing at its start then fails its connection with why, before the client sees only its output closed.
fn lines(
    stdin: ChildStdin,
    stdout: ChildStdout,
    turns: Turns,
    own: Option<Arc<OwnTurns>>,
    settling: oneshot::Receiver<()>,
) -> Lines<
    impl futures::Sink<String, Error = io::Error> + Send + 'static,
    impl futures::Stream<Item = io::Result<String>> + Send + 'static,
> {
    let state = (BufReader::new(stdout).lines(), turns.clone(), own.clone(), settling);
    let incoming = futures::stream::unfold(state, |(mut lines, turns, own, settling)| async move {
        let line = match own_turns::next(own.as_deref(), &mut lines).await.transpose() {
            Some(Ok(line)) => Ok(turns.received(line).await),
            Some(Err(e)) => Err(e),
            None => {
                let _ = tokio::time::timeout(SETTLE_LIMIT, settling).await;
                return None;
            }
        };
        Some((line, (lines, turns, own, settling)))
    });
    let outgoing = futures::sink::unfold((stdin, turns, own), |(mut stdin, turns, own), line: String| async move {
        turns.sent(&line);
        if own.as_ref().is_none_or(|own| own.sent(&line)) {
            let mut line = line.into_bytes();
            line.push(b'\n');
            stdin.write_all(&line).await?;
            stdin.flush().await?;
        }
        Ok::<_, io::Error>((stdin, turns, own))
    });
    Lines::new(Box::pin(outgoing), Box::pin(incoming))
}

/// The turns of a harness: the prompts sent to it not answered yet, by their JSON-RPC ids, and the runs of its
/// process, walked as each turn ends ([`Runs::turn_ended`]).
#[derive(Clone)]
struct Turns {
    prompts: Arc<Mutex<Vec<Value>>>,
    runs: Runs,
}

impl Turns {
    fn new(runs: &Runs) -> Self {
        Self { prompts: Arc::default(), runs: runs.clone() }
    }

    /// Note `line`, sent to the harness, should it prompt it: a turn starts.
    fn sent(&self, line: &str) {
        if !line.contains("\"session/prompt\"") {
            return;
        }
        let Ok(message) = serde_json::from_str::<Value>(line) else { return };
        if message.get("method").and_then(Value::as_str) == Some("session/prompt")
            && let Some(id) = message.get("id")
        {
            self.prompts.locked().push(id.clone());
            self.runs.turn_started();
        }
    }

    /// `line`, received from the harness. Answering a prompt, it ends a turn: the process's descendants are walked,
    /// and the answer lists what the turn left running in the background.
    async fn received(&self, line: String) -> String {
        // An answer names no method, as a request or a notification of the harness does.
        if self.prompts.locked().is_empty() || line.contains("\"method\"") {
            return line;
        }
        let Ok(mut message) = serde_json::from_str::<Value>(&line) else { return line };
        let answered = {
            let mut prompts = self.prompts.locked();
            let index = message.get("id").and_then(|id| prompts.iter().position(|prompt| prompt == id));
            index.map(|index| prompts.swap_remove(index)).is_some()
        };
        if !answered {
            return line;
        }
        let left = self.runs.turn_ended().await;
        let Some(result) = message.get_mut("result").filter(|_| !left.is_empty()) else { return line };
        let Ok(response) = serde_json::from_value::<PromptResponse>(result.clone()) else { return line };
        let Ok(listed) = serde_json::to_value(background::with(response, &left)) else { return line };
        *result = listed;
        message.to_string()
    }
}

/// Drive `transport`, the lines of the process of harness `name`, until either ends, then stop the harness and end
/// what it left running. Asked to finish (`finishing`), the harness's descendants are walked before its stdin closes.
async fn supervise(
    name: String,
    mut process: Process,
    mut transport: ConnectionDriver,
    mut finishing: oneshot::Receiver<()>,
    settled: oneshot::Sender<()>,
) -> agent_client_protocol::Result<()> {
    let mut asked = false;
    let exited = loop {
        tokio::select! {
            result = &mut transport => break Err(result),
            status = process.wait() => break Ok(status),
            finish = &mut finishing, if !asked => {
                asked = true;
                // A finish handle dropped is no request to finish.
                if finish.is_ok() {
                    process.walk().await;
                    let _ = transport.request_finish();
                }
            }
        }
    };
    let closed = match exited {
        Ok(status) => return ended(&name, process, status, transport, settled).await,
        Err(closed) => closed,
    };
    if !asked {
        process.walk().await;
    }
    // Its stdin closes with the transport.
    drop(transport);
    if let Err(e) = closed {
        // Writing to a harness that failed at its start fails the transport first: its exit says why.
        return match tokio::time::timeout(EXIT_GRACE, process.wait()).await {
            Ok(Some(status)) if !status.success() => Err(process.exited().await),
            _ => Err(e),
        };
    }
    match tokio::time::timeout(EXIT_GRACE, process.wait()).await {
        Ok(status) => {
            process.stop(Duration::ZERO).await;
            match status {
                Some(status) if !status.success() => Err(process.exited().await),
                _ => Ok(()),
            }
        }
        // Ended at ants' request, it did not fail, whatever its exit status.
        Err(_) => {
            tracing::debug!(harness = name, "the harness did not exit once its stdin closed: stopping it");
            let grace = if process.terminate() { TERMINATE_GRACE } else { Duration::ZERO };
            process.stop(grace).await;
            Ok(())
        }
    }
}

/// End the connection to `process`, of harness `name`, that exited with `status` while `transport` still ran: what it
/// left running ends, and the connection fails with its stderr, unless it exited successfully, when the connection
/// has [`CLOSE_GRACE`] to end on its own.
async fn ended(
    name: &str,
    mut process: Process,
    status: Option<std::process::ExitStatus>,
    transport: ConnectionDriver,
    settled: oneshot::Sender<()>,
) -> agent_client_protocol::Result<()> {
    if !status.is_some_and(|status| status.success()) {
        return Err(process.exited().await);
    }
    process.stop(Duration::ZERO).await;
    let _ = settled.send(());
    tokio::time::timeout(CLOSE_GRACE, transport).await.unwrap_or_else(|_| {
        tracing::debug!(harness = name, "the harness exited, its connection still open: closing it");
        Ok(())
    })
}

#[cfg(all(test, unix))]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use agent_client_protocol::v1::schema::PromptResponse;
    use agent_client_protocol::{ConnectTo as _, ConnectionDriver};
    use ants_test_support::scratch_dir;
    use rustix::process::{Pid, Signal, kill_process, test_kill_process};
    use serde_json::Value;

    use super::super::leftovers::{GRACE, TAG};
    use super::super::tests::{define, launch};
    use super::super::{Cli, Process, Protocol};
    use super::Turns;
    use crate::harness::{ModelSettings, Readiness, handshake};
    use crate::sync::MutexExt as _;

    /// A process the test started, killed when dropped, should the test fail before it is gone.
    struct Killed(Pid);

    impl Drop for Killed {
        fn drop(&mut self) {
            let _ = kill_process(self.0, Signal::KILL);
        }
    }

    /// The driver of the transport to an ACP harness running `script` in `sh`, started in the blocking pool as
    /// [`crate::harness::Launch::transport`] starts it, and its channel, to keep open as a client does.
    async fn start(name: &str, script: &str) -> (agent_client_protocol::Channel, ConnectionDriver) {
        let harness = define(name, Protocol::Acp, &format!("command = \"sh\"\nargs = [\"-c\", {script:?}]")).unwrap();
        let cli = Cli::new(&harness, &[], &ModelSettings::default());
        let started = tokio::task::spawn_blocking(move || super::connect(cli).into_channel_and_future());
        let (channel, driver) = started.await.unwrap();
        (channel, driver.unwrap())
    }

    /// The pid written to `file`, once it is.
    async fn written_pid(file: &Path) -> Killed {
        let mut pid = None;
        ants_test_support::eventually_within(Duration::from_secs(10), "the pid written", async || {
            pid = std::fs::read_to_string(file).ok().and_then(|text| text.trim().parse().ok());
            pid.is_some()
        })
        .await;
        Killed(Pid::from_raw(pid.unwrap()).unwrap())
    }

    /// Wait for process `pid` to be gone, within the grace period of what a harness left running and a second.
    async fn gone(pid: Pid, what: &str) {
        ants_test_support::eventually_every(
            Duration::from_millis(20),
            GRACE + Duration::from_secs(1),
            what,
            async || test_kill_process(pid).is_err(),
        )
        .await;
    }

    #[tokio::test]
    async fn an_acp_harness_finished_ends_what_it_ran_in_the_background() {
        if !ants_test_support::have("perl") {
            return;
        }
        let dir = scratch_dir("acp-leftovers");
        let (pid, tag) = (dir.join("child.pid"), dir.join("tag"));
        // In a session of its own, as a harness runs a command in the background; `sh`, whose environment macOS hides,
        // so that only the walk of the harness's descendants finds it there.
        let background = "perl -MPOSIX -e 'POSIX::setsid() or die; exec @ARGV' sh -c 'while :; do sleep 1; done'";
        let script = format!(
            "echo \"${TAG}\" > '{}'; {background} < /dev/null > /dev/null 2>&1 & echo $! > '{}'; cat > /dev/null",
            tag.display(),
            pid.display()
        );
        let (_channel, mut driver) = start("acp-leftovers", &script).await;
        let child = written_pid(&pid).await;
        assert!(!std::fs::read_to_string(&tag).unwrap().trim().is_empty(), "the harness's run is tagged");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(test_kill_process(child.0).is_ok(), "the loop runs while the harness does");
        assert!(driver.request_finish());
        driver.await.expect("the harness exits once its stdin closes");
        gone(child.0, "the loop run in the background").await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn an_acp_harness_answering_a_prompt_lists_what_its_turn_left_running() {
        if !ants_test_support::have("perl") {
            return;
        }
        let dir = scratch_dir("acp-turn");
        let pid = dir.join("child.pid");
        let background = "perl -MPOSIX -e 'POSIX::setsid() or die; exec @ARGV' sh -c 'while :; do sleep 1; done'";
        let script =
            format!("{background} < /dev/null > /dev/null 2>&1 & echo $! > '{}'; cat > /dev/null", pid.display());
        let harness =
            define("acp-turn", Protocol::Acp, &format!("command = \"sh\"\nargs = [\"-c\", {script:?}]")).unwrap();
        let cli = Cli::new(&harness, &[], &ModelSettings::default());
        let started = Process::start_here(cli.name(), cli.program(), cli.command(std::iter::empty::<&str>()));
        let (process, _stdin, _stdout) = started.unwrap();
        process.watched_by(cli.runs());
        let turns = Turns::new(cli.runs());
        let child = written_pid(&pid).await;

        let other = r#"{"jsonrpc":"2.0","id":6,"result":{}}"#;
        assert_eq!(turns.received(other.to_owned()).await, other, "no prompt is pending");
        let mut left = Vec::new();
        ants_test_support::eventually_within(Duration::from_secs(10), "the loop listed", async || {
            turns.sent(r#"{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"s1","prompt":[]}}"#);
            assert_eq!(turns.received(other.to_owned()).await, other, "no answer to the prompt");
            let answer = turns.received(r#"{"jsonrpc":"2.0","id":7,"result":{"stopReason":"end_turn"}}"#.to_owned());
            let answer: Value = serde_json::from_str(&answer.await).unwrap();
            let response: PromptResponse = serde_json::from_value(answer["result"].clone()).unwrap();
            left = crate::harness::background::of(&response);
            // The loop, with the `sleep` it runs.
            left.iter().any(|left| left.pid == child.0.as_raw_nonzero().get())
        })
        .await;
        assert!(turns.prompts.locked().is_empty(), "the prompts answered");

        drop(process);
        gone(child.0, "the loop run in the background").await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn what_an_acp_harness_that_failed_left_running_ends_with_it() {
        let dir = scratch_dir("acp-failed-leftovers");
        let pid = dir.join("child.pid");
        let looping = super::super::leftovers::tests::looping();
        let program = std::iter::once(looping.get_program())
            .chain(looping.get_args())
            .map(|arg| format!("'{}'", arg.to_str().unwrap()))
            .collect::<Vec<_>>()
            .join(" ");
        let script = format!(
            "ANTS_TEST_LOOP=1 {program} < /dev/null > /dev/null 2>&1 & echo $! > '{}'; echo boom >&2; exit 3",
            pid.display()
        );
        let (_channel, driver) = start("acp-failed-leftovers", &script).await;
        let child = written_pid(&pid).await;
        let error = driver.await.expect_err("the harness failed");
        assert!(error.to_string().contains("exited with exit status: 3: boom"), "{error}");
        gone(child.0, "the loop left running").await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn an_acp_harness_exiting_at_its_start_is_told_by_its_stderr() {
        let dir = scratch_dir("acp-signed-out");
        let script = "echo 'Error: No authentication information found.' >&2; exit 1";
        let signed_out =
            define("acp-signed-out", Protocol::Acp, &format!("command = \"sh\"\nargs = [\"-c\", {script:?}]"));
        let launched = launch(&signed_out.unwrap(), &dir, &[], ModelSettings::default());
        let readiness = handshake(&launched).await.readiness;
        let Readiness::SignedOut { detail } = readiness else { panic!("not signed out: {readiness:?}") };
        assert!(detail.contains("exited with exit status: 1: Error: No authentication information found."), "{detail}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn an_acp_harness_ignoring_the_end_of_its_input_is_stopped() {
        let (_channel, mut driver) = start("acp-stubborn", "trap '' TERM; while :; do sleep 1; done").await;
        assert!(driver.request_finish());
        let started = std::time::Instant::now();
        driver.await.expect("stopped at ants' request, the harness did not fail");
        let took = started.elapsed();
        assert!(
            took >= super::EXIT_GRACE + super::TERMINATE_GRACE,
            "SIGTERM ignored, killed after its grace: {took:?}"
        );
        assert!(took < Duration::from_secs(10), "{took:?}");
    }
}
