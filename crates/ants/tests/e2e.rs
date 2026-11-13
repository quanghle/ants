//! End to end against a real nats-server: `ants ask --wait` -> `ants daemon` -> harness over ACP -> reply.
//!
//! The harness is this test binary itself: started through a `copilot` symlink on the daemon's PATH, it serves ACP on
//! stdio and answers every prompt with `stub: <prompt>`.

use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use agent_client_protocol::v1::schema::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SessionConfigKind, SessionConfigOption,
    SessionConfigSelectOption, SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason, TextContent,
};
use agent_client_protocol::{Agent, Stdio as AcpStdio};
use ants_test_support::{NatsServer, scratch_dir};

const ANTS: &str = env!("CARGO_BIN_EXE_ants");
const WAIT: Duration = Duration::from_secs(60);

fn main() {
    let me = std::env::args().next().unwrap_or_default();
    if Path::new(&me).file_name().is_some_and(|name| name == "copilot") {
        return stub_harness();
    }
    let tests: [(&str, fn()); 2] = [
        ("ask_gets_the_reply_of_a_daemon_harness", ask_gets_the_reply_of_a_daemon_harness),
        ("custom_whoami_and_tools_reach_the_harness", custom_whoami_and_tools_reach_the_harness),
    ];
    let filters: Vec<String> = std::env::args().skip(1).filter(|a| !a.starts_with('-')).collect();
    for (name, test) in tests {
        if filters.is_empty() || filters.iter().any(|f| name.contains(f.as_str())) {
            test();
            println!("test {name} ... ok");
        }
    }
}

fn ask_gets_the_reply_of_a_daemon_harness() {
    exercise_daemon(false);
}

fn custom_whoami_and_tools_reach_the_harness() {
    exercise_daemon(true);
}

fn exercise_daemon(custom_tools: bool) {
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("e2e");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::os::unix::fs::symlink(std::env::current_exe().unwrap(), bin.join("copilot")).unwrap();
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command
    };
    let init = run(ants(&["init"]));
    assert!(init.status.success(), "ants init: {}", String::from_utf8_lossy(&init.stderr));

    let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
    let home = dir.join("home");
    let mut command = ants(&["daemon"]);
    if custom_tools {
        let config = dir.join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[[persona]]\nname = \"dev-a\"\nworkdir = {dir:?}\nhome = {home:?}\n\
                 whoami = \"Maintain the Rust backend.\"\n\
                 model = \"stub-model\"\nreasoning_effort = \"high\"\ncontext = \"long_context\"\n\
                 memory = \"Use the notes tool.\"\ntasks = \"Use the ticket tool.\"\n\
                 env = {{ ANTS_TEST_TOOL_DB = \"custom.db\", BD_DB = \"unmanaged.db\", BD_REMOTE = \"\" }}\n"
            ),
        )
        .unwrap();
        command.arg("--config").arg(config);
    } else {
        command.args(["--persona", "dev-a"]).arg("--home").arg(&home).arg("--workdir").arg(&dir);
    }
    let daemon = command
        .env("PATH", &bin)
        .env_remove("BD_DB")
        .env_remove("BD_REMOTE")
        .env_remove("ANTS_TEST_TOOL_DB")
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
    let expected = if custom_tools {
        "dev-a: stub: [ants persona dev-a]\nMaintain the Rust backend.\n\n\
         [ants memory]\nUse the notes tool.\n\n[ants tasks]\nUse the ticket tool.\n\n\
         hello there\ncustom.db; BD_DB=unmanaged.db; BD_REMOTE=\n\
         model=stub-model; effort=high; context=long_context\n"
    } else {
        "dev-a: stub: hello there\n"
    };
    assert!(stdout.contains(expected), "{stdout}\ndaemon log:\n{}", daemon_log());
    assert!(!home.join(".bd").exists(), "ants must not initialize a tool workspace");

    let roster = run(ants(&["roster", "--json"]));
    assert!(roster.status.success(), "ants roster: {}", String::from_utf8_lossy(&roster.stderr));
    let entry: serde_json::Value = serde_json::from_slice(&roster.stdout).unwrap();
    assert_eq!(entry["persona"], "dev-a");
    if custom_tools {
        assert_eq!(entry["whoami"], "Maintain the Rust backend.");
        assert!(!String::from_utf8_lossy(&roster.stdout).contains("Use the notes tool."));
    } else {
        assert!(entry.get("whoami").is_none());
    }

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
    let options = std::sync::Arc::new(std::sync::Mutex::new(vec![
        SessionConfigOption::select(
            "model",
            "Model",
            "stub-default",
            ["stub-default", "stub-model"]
                .into_iter()
                .map(|value| SessionConfigSelectOption::new(value, value))
                .collect::<Vec<_>>(),
        ),
        SessionConfigOption::select(
            "reasoning_effort",
            "Reasoning effort",
            "low",
            ["low", "high"].into_iter().map(|value| SessionConfigSelectOption::new(value, value)).collect::<Vec<_>>(),
        ),
    ]));
    let on_new = options.clone();
    let on_set = options.clone();
    let args: Vec<_> = std::env::args().collect();
    let context = args
        .windows(2)
        .find(|args| args[0] == "--context")
        .map(|args| args[1].clone())
        .unwrap_or_else(|| "unset".to_owned());
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
                async move |_: NewSessionRequest, responder, _cx| {
                    responder.respond(NewSessionResponse::new("stub").config_options(on_new.lock().unwrap().clone()))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: SetSessionConfigOptionRequest, responder, _cx| {
                    let mut options = on_set.lock().unwrap();
                    let option = options.iter_mut().find(|option| option.id == request.config_id).unwrap();
                    let SessionConfigKind::Select(select) = &mut option.kind else { unreachable!() };
                    select.current_value = request.value.as_value_id().unwrap().clone();
                    responder.respond(SetSessionConfigOptionResponse::new(options.clone()))
                },
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
                    let mut reply = format!("stub: {text}");
                    if let Ok(db) = std::env::var("ANTS_TEST_TOOL_DB") {
                        reply.push_str(&format!(
                            "\n{db}; BD_DB={}; BD_REMOTE={}",
                            std::env::var("BD_DB").unwrap(),
                            std::env::var("BD_REMOTE").unwrap()
                        ));
                        let options = options.lock().unwrap();
                        let selected: Vec<_> = options
                            .iter()
                            .map(|option| {
                                let SessionConfigKind::Select(select) = &option.kind else { unreachable!() };
                                select.current_value.0.as_ref()
                            })
                            .collect();
                        reply.push_str(&format!("\nmodel={}; effort={}; context={context}", selected[0], selected[1]));
                    }
                    let reply = ContentBlock::Text(TextContent::new(reply));
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
