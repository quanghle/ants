//! End to end against a real nats-server: `ants ask --wait` -> `ants daemon` -> harness over ACP -> reply.
//!
//! The harness is this test binary itself: started through a `copilot` symlink on the daemon's PATH, or a
//! `stub-adapter` one a config file defines as a harness, it serves ACP on stdio and answers every prompt with
//! `stub: <prompt>`.

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
    if Path::new(&me).file_name().is_some_and(|name| name == "copilot" || name == "stub-adapter") {
        return stub_harness();
    }
    let tests: [(&str, fn()); 5] = [
        ("ask_gets_the_reply_of_a_daemon_harness", ask_gets_the_reply_of_a_daemon_harness),
        ("custom_whoami_and_tools_reach_the_harness", custom_whoami_and_tools_reach_the_harness),
        ("channels_are_managed_posted_to_and_read", channels_are_managed_posted_to_and_read),
        ("the_builtin_messenger_hands_over_to_a_listed_one", the_builtin_messenger_hands_over_to_a_listed_one),
        (
            "a_configured_adapter_serves_ants_the_messenger_and_the_cli",
            a_configured_adapter_serves_ants_the_messenger_and_the_cli,
        ),
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
                "messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = {dir:?}\nhome = {home:?}\n\
                 whoami = \"Maintain the Rust backend.\"\n\
                 model = \"stub-model\"\nreasoning_effort = \"high\"\ncontext = \"long_context\"\n\
                 memory = \"Use the notes tool.\"\ntasks = \"Use the ticket tool.\"\n\
                 env = {{ ANTS_TEST_TOOL_DB = \"custom.db\", BD_DB = \"unmanaged.db\", BD_REMOTE = \"\" }}\n"
            ),
        )
        .unwrap();
        command.arg("--config").arg(config);
    } else {
        command.env("ANTS_ANT", "dev-a").arg("--home").arg(&home).arg("--workdir").arg(&dir);
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
        "dev-a: stub: [ants role dev-a]\nMaintain the Rust backend.\n\n\
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
    assert_eq!(entry["ant"], "dev-a");
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

fn channels_are_managed_posted_to_and_read() {
    use std::io::BufRead as _;

    let Some(server) = NatsServer::start() else { return };
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command
    };
    let ok = |args: &[&str]| {
        let out = run(ants(args));
        let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        assert!(out.status.success(), "ants {args:?}: {stderr}");
        (stdout.into_owned(), stderr.into_owned())
    };
    ok(&["init"]);

    let (created, warned) = ok(&["channel", "create", "#ops", "--member", "dev-a,dev-b", "--messenger", "triage"]);
    assert_eq!(created, "created #ops: members dev-a,dev-b; messenger triage\n");
    assert!(warned.contains("dev-a is not in the roster"), "{warned}");
    let again = run(ants(&["channel", "create", "ops"]));
    assert!(!again.status.success(), "a channel is created once");
    assert!(String::from_utf8_lossy(&again.stderr).contains("exists"));
    ok(&["channel", "create", "dev"]);
    assert_eq!(
        ok(&["channel", "add", "ops", "@dev-c", "dev-a"]).0,
        "#ops: members dev-a,dev-b,dev-c; messenger triage\n"
    );
    assert_eq!(ok(&["channel", "remove", "ops", "dev-a"]).0, "#ops: members dev-b,dev-c; messenger triage\n");
    assert_eq!(
        ok(&["channel", "set", "ops", "--default-messenger"]).0,
        "#ops: members dev-b,dev-c; messenger messenger (default)\n"
    );
    assert!(!run(ants(&["channel", "add", "qa", "dev-a"])).status.success(), "no such channel");
    assert_eq!(
        ok(&["channel", "set", "ops", "--owner", "lead"]).0,
        "#ops: members dev-b,dev-c; messenger messenger (default); owner lead\n"
    );
    assert!(!run(ants(&["channel", "set", "ops", "--owner", "human"])).status.success(), "no human owner");
    let rows: Vec<Vec<String>> =
        ok(&["channel", "list"]).0.lines().map(|l| l.split_whitespace().map(str::to_owned).collect()).collect();
    assert_eq!(
        rows,
        [
            vec!["CHANNEL", "MEMBERS", "MESSENGER", "OWNER"],
            vec!["#dev", "-", "messenger", "(default)", "-"],
            vec!["#ops", "dev-b,dev-c", "messenger", "(default)", "lead"]
        ]
    );

    let (posted, warned) = ok(&["post", "#ops", "@dev-c", "and", "@dev-z,", "look"]);
    let thread = posted.strip_prefix("thread ").and_then(|t| t.strip_suffix('\n')).expect(&posted).to_owned();
    assert!(warned.contains("@dev-z is no member of #ops"), "{warned}");
    assert!(!warned.contains("@dev-c"), "{warned}");

    // `--follow` prints the posts on the thread as they come.
    let mut waiting = Command::new(ANTS);
    waiting.args(["--nats", &server.url, "post", "--follow", "--timeout", "30s", "ops", "anyone?"]);
    let mut waiting =
        Killed(waiting.env_remove("ANTS_CREDS").stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap());
    let mut lines = std::io::BufReader::new(waiting.0.stdout.take().unwrap()).lines();
    let waited = lines.next().unwrap().unwrap().strip_prefix("thread ").unwrap().to_owned();
    ok(&["post", "--thread", &waited, "ops", "still", "here"]);
    assert_eq!(lines.next().unwrap().unwrap(), "human: still here");
    drop(waiting);

    ok(&["post", "ops", "--thread", &thread, "any", "news?"]);
    let read = ok(&["channel", "read", "ops"]).0;
    let expected = [
        format!("thread {thread}"),
        "human: @dev-c and @dev-z, look".to_owned(),
        "    ".to_owned(),
        "human: any news?".to_owned(),
        String::new(),
        format!("thread {waited}"),
        "human: anyone?".to_owned(),
        "    ".to_owned(),
        "human: still here".to_owned(),
    ];
    let mut at = 0;
    for part in &expected {
        let found = read[at..].find(part.as_str()).unwrap_or_else(|| panic!("{part:?} after {at} in:\n{read}"));
        at += found + part.len();
    }
    let one = ok(&["channel", "read", "#ops", "--thread", &thread]).0;
    assert!(one.contains("any news?") && !one.contains("anyone?"), "{one}");
    let last = ok(&["channel", "read", "ops", "--limit", "1"]).0;
    assert!(last.contains("anyone?") && !last.contains("any news?"), "{last}");

    // Nobody serves the messenger: a `--wait` gives up at the timeout, with an error.
    let asked = run(ants(&["ask", "--wait", "--timeout", "1s", "anyone", "there?"]));
    let (stdout, stderr) = (String::from_utf8_lossy(&asked.stdout), String::from_utf8_lossy(&asked.stderr));
    let thread = stdout.strip_prefix("thread ").and_then(|t| t.strip_suffix('\n')).expect(&stdout);
    assert_eq!(asked.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains(&format!("no reply on thread {thread} within 1s")), "{stderr}");

    assert_eq!(ok(&["channel", "delete", "dev"]).0, "deleted #dev; the posts to #dev stay in the stream\n");
    assert!(!ok(&["channel", "list"]).0.contains("#dev"));
    drop(server);
}

/// A daemon serving the built-in messenger hands it over to a daemon started later with a listed messenger, on the same
/// machine, and serves it again once that daemon stops.
fn the_builtin_messenger_hands_over_to_a_listed_one() {
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("handover");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::os::unix::fs::symlink(std::env::current_exe().unwrap(), bin.join("copilot")).unwrap();
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command
    };
    assert!(run(ants(&["init"])).status.success());
    let log = |name: &str| std::fs::read_to_string(dir.join(format!("{name}.log"))).unwrap_or_default();
    let logs = || format!("builtin log:\n{}\nlisted log:\n{}", log("builtin"), log("listed"));
    let daemon = |name: &str, config: &str| {
        let (path, home) = (dir.join(format!("{name}.toml")), dir.join(name));
        std::fs::write(&path, config).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        let log = std::fs::File::create(dir.join(format!("{name}.log"))).unwrap();
        let mut command = ants(&["daemon", "--config"]);
        command.arg(path).env("HOME", home).env("PATH", &bin).env("RUST_LOG", "info");
        Killed(command.stdout(Stdio::null()).stderr(log).spawn().unwrap())
    };
    let wait_for = |name: &str, text: &str, times: usize| {
        let start = Instant::now();
        while log(name).matches(text).count() < times {
            assert!(start.elapsed() < WAIT, "{name} log lacks {text:?} {times} times\n{}", logs());
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let served_from = || {
        let roster = run(ants(&["roster", "--json"]));
        let entry: serde_json::Value = serde_json::from_slice(&roster.stdout).unwrap();
        assert_eq!(entry["ant"], "messenger");
        entry["workdir"].as_str().unwrap().to_owned()
    };
    let ask = |text: &str| {
        let ask = run(ants(&["ask", "--wait", text]));
        let stdout = String::from_utf8_lossy(&ask.stdout);
        assert!(stdout.contains("messenger: stub:") && stdout.contains(text), "{stdout}\n{}", logs());
    };

    let _builtin = daemon("builtin", "harness = \"copilot\"\n");
    wait_for("builtin", "serving inbox", 1);
    let listed = daemon("listed", &format!("[[ant]]\nname = \"messenger\"\nworkdir = {dir:?}\n"));
    wait_for("listed", "serving inbox", 1);
    wait_for("builtin", "another daemon took over the ant", 1);
    assert_eq!(served_from(), dir.canonicalize().unwrap().display().to_string(), "{}", logs());
    ask("first");

    let mut listed = listed;
    assert!(Command::new("kill").arg("-TERM").arg(listed.0.id().to_string()).status().unwrap().success());
    listed.0.wait().unwrap();
    wait_for("builtin", "serving inbox", 2);
    assert!(served_from().ends_with(".ants/messenger"), "{}", logs());
    ask("second");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

fn a_configured_adapter_serves_ants_the_messenger_and_the_cli() {
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("adapter");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::os::unix::fs::symlink(std::env::current_exe().unwrap(), bin.join("stub-adapter")).unwrap();
    let (user_home, home) = (dir.join("user"), dir.join("dev-a"));
    std::fs::create_dir_all(&user_home).unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            "harness = \"stubby\"\n\
             [harnesses.stubby]\ncommand = \"bin/stub-adapter\"\nargs = [\"--flag\", \"two words\"]\n\
             env = {{ ANTS_TEST_ADAPTER = \"1\", OVERRIDDEN = \"definition\" }}\n\
             [harnesses.strict]\ncommand = \"bin/stub-adapter\"\nmode = \"default\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = {dir:?}\nhome = {home:?}\nenv = {{ OVERRIDDEN = \"ant\" }}\n"
        ),
    )
    .unwrap();
    // Neither the daemon nor the CLI finds the adapter on PATH: the definition's path, relative to the file, does.
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command
            .args(["--nats", &server.url])
            .args(args)
            .env_remove("ANTS_CREDS")
            .env_remove("ANTS_CONFIG")
            .env_remove("ANTS_TEST_ADAPTER")
            .env("HOME", &user_home)
            .env("PATH", "/usr/bin:/bin");
        command
    };
    let stdout = |output: &Output| String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = |output: &Output| String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(run(ants(&["init"])).status.success());

    let check = run(ants(&["harness", "check", "--config", config.to_str().unwrap()]));
    assert!(check.status.success(), "ants harness check: {}", stderr(&check));
    assert!(stdout(&check).starts_with("stubby: "), "{}", stdout(&check));
    let prompt = run(ants(&["harness", "prompt", "--config", config.to_str().unwrap(), "hi"]));
    assert!(prompt.status.success(), "ants harness prompt: {}", stderr(&prompt));
    assert!(stdout(&prompt).contains("stub: hi\nargs=--flag two words; OVERRIDDEN=definition"), "{}", stdout(&prompt));
    let strict = run(ants(&["harness", "prompt", "--config", config.to_str().unwrap(), "--harness", "strict", "hi"]));
    assert!(!strict.status.success());
    assert!(stderr(&strict).contains("does not offer session mode default"), "{}", stderr(&strict));
    let unknown = run(ants(&["harness", "check", "--config", config.to_str().unwrap(), "--harness", "nope"]));
    assert!(!unknown.status.success());
    assert!(stderr(&unknown).contains("unknown harness \"nope\""), "{}", stderr(&unknown));

    let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
    let mut command = ants(&["daemon", "--config", config.to_str().unwrap()]);
    let _daemon = Killed(command.env("RUST_LOG", "info").stdout(Stdio::null()).stderr(log).spawn().unwrap());
    let daemon_log = || std::fs::read_to_string(dir.join("daemon.log")).unwrap_or_default();
    let ask = |args: &[&str], expected: &str| {
        let ask = run(ants(&[&["ask", "--wait"], args].concat()));
        assert!(stdout(&ask).contains(expected), "{}{}\ndaemon log:\n{}", stdout(&ask), stderr(&ask), daemon_log());
    };
    ask(&["@dev-a", "hello"], "dev-a: stub: hello\nargs=--flag two words; OVERRIDDEN=ant");
    ask(&["hello"], "messenger: stub: hello\nargs=--flag two words; OVERRIDDEN=definition");

    let workdir = dir.to_str().unwrap();
    let add = |harness: &str| {
        let out = dir.join("dev-b.creds");
        let mut command = ants(&["add", "dev-b", "--config", config.to_str().unwrap(), "--workdir", workdir]);
        command.arg("--harness").arg(harness).arg("--out").arg(out);
        run(command)
    };
    let unknown = add("nope");
    assert!(!unknown.status.success());
    assert!(stderr(&unknown).contains("unknown harness \"nope\""), "{}", stderr(&unknown));
    let added = add("stubby");
    assert!(added.status.success(), "ants add: {}", stderr(&added));
    let text = std::fs::read_to_string(&config).unwrap();
    let dev_b = text.split("name = \"dev-b\"").nth(1).unwrap_or_default();
    assert!(dev_b.contains("harness = \"stubby\"\n"), "{text}");
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
    let adapter = std::env::var("ANTS_TEST_ADAPTER").is_ok().then(|| {
        let overridden = std::env::var("OVERRIDDEN").unwrap_or_default();
        format!("\nargs={}; OVERRIDDEN={overridden}", args[1..].join(" "))
    });
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
                    if let Some(adapter) = &adapter {
                        reply.push_str(adapter);
                    }
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
