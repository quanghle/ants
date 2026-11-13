//! End to end against a real nats-server: `ants ask --wait` -> `ants daemon` -> harness over ACP -> reply.
//!
//! The harness is this test binary itself: started through a `copilot` symlink on the daemon's PATH, it serves ACP on
//! stdio and answers every prompt with `stub: <prompt>`.

use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use agent_client_protocol::v1::schema::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, Stdio as AcpStdio};
use ants_test_support::{NatsServer, have, scratch_dir};

const ANTS: &str = env!("CARGO_BIN_EXE_ants");
const WAIT: Duration = Duration::from_secs(60);

fn main() {
    let me = std::env::args().next().unwrap_or_default();
    if Path::new(&me).file_name().is_some_and(|name| name == "copilot") {
        return stub_harness();
    }
    let tests: [(&str, fn()); 1] = [("ask_gets_the_reply_of_a_daemon_harness", ask_gets_the_reply_of_a_daemon_harness)];
    let filters: Vec<String> = std::env::args().skip(1).filter(|a| !a.starts_with('-')).collect();
    for (name, test) in tests {
        if filters.is_empty() || filters.iter().any(|f| name.contains(f.as_str())) {
            test();
            println!("test {name} ... ok");
        }
    }
}

fn ask_gets_the_reply_of_a_daemon_harness() {
    if !have("bd") {
        return;
    }
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("e2e");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::os::unix::fs::symlink(std::env::current_exe().unwrap(), bin.join("copilot")).unwrap();
    let path = std::env::join_paths(
        std::iter::once(bin).chain(std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())),
    )
    .unwrap();

    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command
    };
    let init = run(ants(&["init"]));
    assert!(init.status.success(), "ants init: {}", String::from_utf8_lossy(&init.stderr));

    let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
    let home = dir.join("home");
    let daemon = ants(&["daemon", "--persona", "dev-a"])
        .arg("--home")
        .arg(&home)
        .arg("--workdir")
        .arg(&dir)
        .env("PATH", &path)
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    let daemon = Killed(daemon);
    let daemon_log = || std::fs::read_to_string(dir.join("daemon.log")).unwrap_or_default();

    let ask = run(ants(&["ask", "--wait", "@dev-a", "hello", "there"]));
    let stdout = String::from_utf8_lossy(&ask.stdout);
    assert!(ask.status.success(), "ants ask: {}\ndaemon log:\n{}", String::from_utf8_lossy(&ask.stderr), daemon_log());
    assert!(stdout.starts_with("thread "), "{stdout}");
    assert!(stdout.contains("dev-a: stub: hello there\n"), "{stdout}\ndaemon log:\n{}", daemon_log());

    let inbox = run(ants(&["inbox"]));
    assert!(inbox.status.success(), "ants inbox: {}", String::from_utf8_lossy(&inbox.stderr));
    assert!(!String::from_utf8_lossy(&inbox.stdout).contains("stub:"), "ask --wait marked the reply read");

    // Service managers stop it with SIGTERM: a clean stop, as on Ctrl-C.
    let mut daemon = daemon;
    let term = Command::new("kill").arg("-TERM").arg(daemon.0.id().to_string()).status().unwrap();
    assert!(term.success());
    let start = Instant::now();
    let status = loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            break status;
        }
        assert!(start.elapsed() < WAIT, "daemon still running after SIGTERM\ndaemon log:\n{}", daemon_log());
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "daemon after SIGTERM: {status}\ndaemon log:\n{}", daemon_log());
    assert!(daemon_log().contains("stopped"), "daemon log:\n{}", daemon_log());

    drop(daemon);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Run `command` to completion, killing it after [`WAIT`].
fn run(mut command: Command) -> Output {
    let mut child = command.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let start = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if start.elapsed() > WAIT {
            let _ = child.kill();
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.wait_with_output().unwrap()
}

/// A child process killed on drop, so a failed assertion does not leave it running.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// ACP over stdio: every prompt is answered with `stub: <prompt>`.
fn stub_harness() {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let served = runtime.block_on(
        Agent
            .v1()
            .name("stub")
            .on_receive_request(
                async move |init: InitializeRequest, responder, _cx| {
                    responder.respond(
                        InitializeResponse::new(init.protocol_version).agent_capabilities(AgentCapabilities::new()),
                    )
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |_: NewSessionRequest, responder, _cx| responder.respond(NewSessionResponse::new("stub")),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |prompt: PromptRequest, responder, cx| {
                    let text: String = prompt
                        .prompt
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text(t) => Some(t.text.as_str()),
                            _ => None,
                        })
                        .collect();
                    let reply = ContentBlock::Text(TextContent::new(format!("stub: {text}")));
                    cx.send_notification(SessionNotification::new(
                        prompt.session_id,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(reply)),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .connect_to(AcpStdio::new()),
    );
    if let Err(e) = served {
        eprintln!("stub harness: {e}");
        std::process::exit(1);
    }
}
