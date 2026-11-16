//! End to end against a real nats-server: `ants ask --wait` -> `ants daemon` -> harness over ACP -> reply.
//!
//! The harness is this test binary itself: started through a `copilot` symlink on the daemon's PATH, or a
//! `stub-adapter` one a config file defines as a harness, it serves ACP on stdio and answers every prompt with
//! `stub: <prompt>`.

#![allow(clippy::too_many_lines)]

use std::fmt::Write as _;
use std::io::{Read as _, Write as _};
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
use ants_daemon::sync::MutexExt as _;
use ants_test_support::{NatsServer, free_port, scratch_dir};

const ANTS: &str = env!("CARGO_BIN_EXE_ants");
const WAIT: Duration = Duration::from_secs(60);

fn main() {
    let me = std::env::args().next().unwrap_or_default();
    if Path::new(&me).file_stem().is_some_and(|name| name == "copilot" || name == "stub-adapter") {
        return stub_harness();
    }
    let tests: [(&str, fn()); 8] = [
        ("ask_gets_the_reply_of_a_daemon_harness", ask_gets_the_reply_of_a_daemon_harness),
        ("a_daemon_exits_once_its_executable_was_replaced", a_daemon_exits_once_its_executable_was_replaced),
        ("custom_whoami_and_tools_reach_the_harness", custom_whoami_and_tools_reach_the_harness),
        ("channels_are_managed_posted_to_and_read", channels_are_managed_posted_to_and_read),
        ("files_are_attached_listed_and_saved", files_are_attached_listed_and_saved),
        ("the_builtin_messenger_hands_over_to_a_listed_one", the_builtin_messenger_hands_over_to_a_listed_one),
        (
            "a_configured_adapter_serves_ants_the_messenger_and_the_cli",
            a_configured_adapter_serves_ants_the_messenger_and_the_cli,
        ),
        (
            "setup_brings_a_machine_up_once_and_completions_name_its_ants_channels_and_threads",
            setup_brings_a_machine_up_once_and_completions_name_its_ants_channels_and_threads,
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

/// Make this test binary, the stub harness, available as `name` in `bin`.
fn link_self(bin: &Path, name: &str) {
    let exe = std::env::current_exe().unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(exe, bin.join(name)).unwrap();
    #[cfg(not(unix))]
    std::fs::copy(exe, bin.join(name).with_extension(std::env::consts::EXE_EXTENSION)).unwrap();
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
    link_self(&bin, "copilot");
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command
    };
    let init = run(ants(&["init"]));
    assert!(init.status.success(), "ants init: {}", String::from_utf8_lossy(&init.stderr));

    let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
    let home = dir.join("home");
    let metrics = format!("127.0.0.1:{}", free_port());
    let mut command = ants(&["daemon"]);
    if custom_tools {
        let config = dir.join("config.toml");
        std::fs::write(
            &config,
            format!(
                "messenger = false\nharness = \"copilot\"\nmetrics = {metrics:?}\n\
                 [[ant]]\nname = \"dev-a\"\nworkdir = {dir:?}\nhome = {home:?}\n\
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
        command.arg("--harness").arg("copilot");
        command.arg("--metrics").arg(&metrics);
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
    let mut said = stdout.lines();
    let thread = said.next().and_then(|l| l.strip_prefix("thread ")).expect(&stdout).to_owned();
    let message = said.next().and_then(|l| l.strip_prefix("message ")).expect(&stdout).to_owned();
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

    let colony = run(ants(&["colony", "--json"]));
    assert!(colony.status.success(), "ants colony: {}", String::from_utf8_lossy(&colony.stderr));
    let entry: serde_json::Value = serde_json::from_slice(&colony.stdout).unwrap();
    assert_eq!(entry["ant"], "dev-a");
    if custom_tools {
        assert_eq!(entry["whoami"], "Maintain the Rust backend.");
        assert!(!String::from_utf8_lossy(&colony.stdout).contains("Use the notes tool."));
    } else {
        assert!(entry.get("whoami").is_none());
    }

    let shown = run(ants(&["ant", "show", "dev-a"]));
    let text = String::from_utf8_lossy(&shown.stdout);
    // The ant's face goes before its name.
    let first = text.lines().next().unwrap_or_default();
    assert!(first.contains(" dev-a: live") && text.contains("holds:\n  none"), "{text}");
    assert!(text.contains("no messages waiting") && text.contains(&thread[..8]), "{text}");
    let shown = run(ants(&["ant", "show", "dev-a", "--json"]));
    assert!(shown.status.success(), "ants ant show: {}", String::from_utf8_lossy(&shown.stderr));
    let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(shown["colony"]["ant"], "dev-a", "{shown}");
    assert_eq!(shown["queue"], serde_json::json!([]), "{shown}");
    assert_eq!(shown["threads"][0]["thread_id"], thread.as_str(), "{shown}");
    if custom_tools {
        let model = serde_json::json!({"model": "stub-model", "reasoning_effort": "high", "context": "long_context"});
        assert_eq!(shown["colony"]["model"], model, "{shown}");
    }

    // dev-a answered the human's thread: finished work waits on the human.
    let status = run(ants(&["status", "--json"]));
    assert!(status.status.success(), "ants status: {}", String::from_utf8_lossy(&status.stderr));
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(
        status["home"],
        serde_json::json!({"approve": 0, "question": 0, "stuck": 0, "message": 0, "mention": 0, "finished": 1})
    );
    assert!(status["working"].is_array() && status["spend_today"].is_number(), "{status}");
    let tmux = run(ants(&["status", "--tmux"]));
    let tmux = String::from_utf8_lossy(&tmux.stdout);
    assert!(tmux.starts_with("⚑1 ◐") && tmux.contains(" $"), "{tmux}");

    // A reaction is no message: it wakes no ant.
    let react = |args: &[&str]| {
        let out = run(ants(&[&["react"], args].concat()));
        assert!(out.status.success(), "ants react: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    assert_eq!(react(&[&message, "eyes"]), format!("reacted eyes to message {message}\n"));
    assert_eq!(react(&[&message, "eyes"]), format!("already reacted eyes to message {message}\n"));
    react(&[&message, "check"]);
    assert_eq!(react(&[&message, "--remove", "check"]), format!("removed check from message {message}\n"));
    let shown = run(ants(&["thread", "show", &thread]));
    let shown = String::from_utf8_lossy(&shown.stdout);
    assert!(shown.contains(&format!("(message {message}): hello there\n    reacted eyes: human\n")), "{shown}");
    std::thread::sleep(Duration::from_millis(500));

    let (status, health) = get(&metrics, "/healthz");
    assert_eq!((status, health.as_str()), (200, "ok\n"), "daemon log:\n{}", daemon_log());
    let (status, exposition) = get(&metrics, "/metrics");
    assert_eq!(status, 200);
    for line in [
        "# TYPE ants_nats_connected gauge",
        "ants_nats_connected{ant=\"dev-a\"} 1",
        "# TYPE ants_prompts_total counter",
        "ants_prompts_total{ant=\"dev-a\"} 1",
        "ants_prompts_in_flight{ant=\"dev-a\"} 0",
    ] {
        assert!(exposition.lines().any(|l| l == line), "{line} in:\n{exposition}");
    }
    assert_eq!(get(&metrics, "/nothing").0, 404);

    if !custom_tools {
        // A reply with no prompt running is an ordinary message on the thread.
        let ask = run(ants(&["ask", "--wait", "--thread", &thread, "@dev-a", "go", "left"]));
        let stdout = String::from_utf8_lossy(&ask.stdout);
        assert!(ask.status.success(), "ants ask: {}", String::from_utf8_lossy(&ask.stderr));
        assert!(stdout.starts_with(&format!("thread {thread}\n")), "{stdout}");
        assert!(stdout.contains("dev-a: stub: go left\n"), "{stdout}\ndaemon log:\n{}", daemon_log());

        // A message from a file, or written in an editor, keeps its lines.
        let file = dir.join("message.md");
        std::fs::write(&file, "first line\n\n  indented\nlast\n").unwrap();
        let ask = run(ants(&["ask", "--wait", "@dev-a", "--file", file.to_str().unwrap()]));
        let stdout = String::from_utf8_lossy(&ask.stdout);
        assert!(stdout.contains("dev-a: stub: first line\n\n  indented\nlast\n"), "{stdout}\n{}", daemon_log());
        #[cfg(unix)]
        {
            let editor = dir.join("editor");
            std::fs::write(&editor, "#!/bin/sh\nprintf 'from the\\n  editor\\n' > \"$1\"\n").unwrap();
            std::fs::set_permissions(&editor, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
            let mut edit = ants(&["ask", "--wait", "--edit", "@dev-a"]);
            edit.env_remove("VISUAL").env("EDITOR", &editor);
            let ask = run(edit);
            let stdout = String::from_utf8_lossy(&ask.stdout);
            assert!(stdout.contains("dev-a: stub: from the\n  editor\n"), "{stdout}\n{}", daemon_log());
            let mut cancel = ants(&["ask", "--edit", "@dev-a"]);
            cancel.env("VISUAL", "true");
            let cancelled = run(cancel);
            assert!(!cancelled.status.success() && cancelled.stdout.is_empty(), "{cancelled:?}");
            assert!(String::from_utf8_lossy(&cancelled.stderr).contains("the message is empty: nothing sent"));
        }
    }

    let inbox = run(ants(&["inbox"]));
    assert!(inbox.status.success(), "ants inbox: {}", String::from_utf8_lossy(&inbox.stderr));
    assert!(!String::from_utf8_lossy(&inbox.stdout).contains("stub:"), "ask --wait marked the reply read");
    let unread = run(ants(&["inbox", "--unread", "last"]));
    assert!(String::from_utf8_lossy(&unread.stdout).contains("marked 1 message to"), "{unread:?}");
    let inbox = run(ants(&["inbox"]));
    assert!(String::from_utf8_lossy(&inbox.stdout).contains("stub:"), "the latest reply is unread again: {inbox:?}");

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
    assert!(warned.contains("dev-a is not in the colony"), "{warned}");
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
    // A human member of a channel sees its unread posts, its own counting as read.
    ok(&["channel", "add", "dev", "human"]);
    ok(&["post", "dev", "hello"]);
    let rows: Vec<Vec<String>> =
        ok(&["channel", "list"]).0.lines().map(|l| l.split_whitespace().map(str::to_owned).collect()).collect();
    assert_eq!(rows[0].last().map(String::as_str), Some("UNREAD"));
    assert_eq!(rows[1], ["#dev", "human", "messenger", "(default)", "-", "0"]);
    assert_eq!(rows[2].last().map(String::as_str), Some("-"), "not a member of #ops");

    let (posted, warned) = ok(&["post", "#ops", "@dev-c", "and", "@dev-z,", "look"]);
    let thread = posted.lines().next().and_then(|t| t.strip_prefix("thread ")).expect(&posted).to_owned();
    assert!(warned.contains("@dev-z is no member of #ops"), "{warned}");
    assert!(!warned.contains("@dev-c"), "{warned}");

    // `--follow` prints the posts on the thread as they come.
    let mut waiting = Command::new(ANTS);
    waiting.args(["--nats", &server.url, "post", "--follow", "--timeout", "30s", "ops", "anyone?"]);
    let mut waiting =
        Killed(waiting.env_remove("ANTS_CREDS").stdout(Stdio::piped()).stderr(Stdio::null()).spawn().unwrap());
    let mut lines = std::io::BufReader::new(waiting.0.stdout.take().unwrap()).lines();
    let waited = lines.next().unwrap().unwrap().strip_prefix("thread ").unwrap().to_owned();
    assert!(lines.next().unwrap().unwrap().starts_with("message "));
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

    // A post from stdin keeps its lines.
    let mut piped =
        Killed(ants(&["post", "ops", "--file", "-"]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap());
    piped.0.stdin.take().unwrap().write_all(b"@dev-b first\n  second\n").unwrap();
    let mut posted = String::new();
    piped.0.stdout.take().unwrap().read_to_string(&mut posted).unwrap();
    assert!(piped.0.wait().unwrap().success(), "{posted}");
    let multiline = posted.lines().next().and_then(|t| t.strip_prefix("thread ")).expect(&posted).to_owned();
    let read = ok(&["channel", "read", "ops", "--thread", &multiline]).0;
    // Indented under its first line.
    assert!(read.contains("human: @dev-b first\n      second\n"), "{read}");

    // Nobody serves the messenger: a `--wait` gives up at the timeout, with an error, a reaction meanwhile being no
    // reply.
    let mut asking = ants(&["ask", "--wait", "--timeout", "3s", "anyone", "there?"]);
    let mut asking = Killed(asking.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap());
    let mut said = std::io::BufReader::new(asking.0.stdout.take().unwrap()).lines();
    let thread = said.next().unwrap().unwrap().strip_prefix("thread ").unwrap().to_owned();
    let message = said.next().unwrap().unwrap().strip_prefix("message ").unwrap().to_owned();
    ok(&["react", &message, "eyes"]);
    let mut stderr = String::new();
    asking.0.stderr.take().unwrap().read_to_string(&mut stderr).unwrap();
    assert_eq!(asking.0.wait().unwrap().code(), Some(1), "{stderr}");
    assert!(stderr.contains(&format!("no reply on thread {thread} within 3s")), "{stderr}");
    assert!(stderr.contains("messenger is not in the colony: no daemon serves messenger now"), "{stderr}");
    assert!(stderr.contains("--wait may not end until one does"), "{stderr}");

    assert_eq!(ok(&["channel", "delete", "dev"]).0, "deleted #dev; the posts to #dev stay in the stream\n");
    assert!(!ok(&["channel", "list"]).0.contains("#dev"));
    drop(server);
}

/// `--attach` stores files before the message carrying them, `ants thread show`, `ants channel read` and `ants notes` list
/// them with their handles, and `ants attachment get` saves one as stored, once.
fn files_are_attached_listed_and_saved() {
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("attach");
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command.current_dir(&dir);
        command
    };
    let ok = |args: &[&str]| {
        let out = run(ants(args));
        let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        assert!(out.status.success(), "ants {args:?}: {stderr}");
        stdout.into_owned()
    };
    let failed = |args: &[&str]| {
        let out = run(ants(args));
        assert!(!out.status.success(), "ants {args:?} succeeded");
        assert!(out.stdout.is_empty(), "nothing sent: {}", String::from_utf8_lossy(&out.stdout));
        String::from_utf8_lossy(&out.stderr).into_owned()
    };
    ok(&["init"]);
    let shot: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(dir.join("shot.png"), &shot).unwrap();
    std::fs::write(dir.join("notes.txt"), b"line\n").unwrap();

    let asked = ok(&["ask", "@dev-a", "--attach", "shot.png", "--attach", "notes.txt", "look"]);
    let said: Vec<&str> = asked.lines().collect();
    let thread = said[0].strip_prefix("thread ").expect(&asked);
    let handles: Vec<&str> = said[2..].iter().map(|l| l.strip_prefix("attachment ").expect(&asked)).collect();
    assert_eq!(handles.len(), 2, "{asked}");
    assert!(handles.iter().all(|h| h.starts_with("human.inbox.dev-a.")), "{asked}");
    let shown = ok(&["thread", "show", thread]);
    assert!(shown.contains(&format!("\n    📎 shot.png (293 KiB) {}\n", handles[0])), "{shown}");
    assert!(shown.contains(&format!("\n    📎 notes.txt (5 B) {}\n", handles[1])), "{shown}");
    let json: serde_json::Value = serde_json::from_str(ok(&["thread", "show", thread, "--json"]).trim()).unwrap();
    let listed = &json["attachments"][0];
    assert_eq!(
        (&listed["handle"], &listed["mime"], &listed["size"]),
        (&handles[0].into(), &"image/png".into(), &300_000.into())
    );

    let missing = failed(&["ask", "@dev-a", "--attach", "notes.txt", "--attach", "missing.png", "lost"]);
    assert!(missing.contains("missing.png"), "{missing}");
    let nine: Vec<&str> = ["--attach", "notes.txt"].repeat(9);
    let many = failed(&[&["ask", "@dev-a"], &nine[..], &["too many"]].concat());
    assert!(many.contains("9 files to attach: a message carries at most 8"), "{many}");
    assert!(failed(&["notes", thread, "--attach", "notes.txt"]).contains("give TEXT"));

    std::fs::create_dir(dir.join("out")).unwrap();
    let saved = ok(&["attachment", "get", handles[0], "-o", "out"]);
    assert_eq!(saved, "saved shot.png (293 KiB, image/png) to out/shot.png\n");
    assert_eq!(std::fs::read(dir.join("out/shot.png")).unwrap(), shot);
    let again = failed(&["attachment", "get", handles[0], "-o", "out"]);
    assert!(again.contains("out/shot.png exists: not overwriting it"), "{again}");
    let mut here = ants(&["attachment", "get", handles[1]]);
    std::fs::create_dir(dir.join("here")).unwrap();
    here.current_dir(dir.join("here"));
    assert!(run(here).status.success());
    assert_eq!(std::fs::read(dir.join("here/notes.txt")).unwrap(), b"line\n");

    let posted = ok(&["post", "ops", "--attach", "notes.txt", "see", "the", "notes"]);
    let posted = posted.lines().find_map(|l| l.strip_prefix("attachment ")).expect(&posted).to_owned();
    assert!(posted.starts_with("human.chan.ops."), "{posted}");
    let read = ok(&["channel", "read", "ops"]);
    assert!(read.contains(&format!("    📎 notes.txt (5 B) {posted}\n")), "{read}");
    let noted = ok(&["notes", thread, "--attach", "shot.png", "the", "plan"]);
    let noted = noted.lines().find_map(|l| l.strip_prefix("attachment ")).expect(&noted).to_owned();
    assert!(noted.starts_with(&format!("human.notes.{thread}.")), "{noted}");
    let notes = ok(&["notes", thread]);
    assert!(notes.contains(&format!("the plan\n  📎 shot.png (293 KiB) {noted}\n")), "{notes}");
    ok(&["attachment", "get", &noted, "-o", "out/plan.png"]);
    assert_eq!(std::fs::read(dir.join("out/plan.png")).unwrap(), shot);
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A daemon serving the built-in messenger hands it over to a daemon started later with a listed messenger, on the same
/// machine, and serves it again once that daemon stops.
/// `cargo install` renames the new executable over the old one: the daemon stops as on SIGTERM and exits with status
/// 75, which the service units restart on.
fn a_daemon_exits_once_its_executable_was_replaced() {
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("upgrade");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    link_self(&bin, "copilot");
    let installed = bin.join("ants");
    std::fs::copy(ANTS, &installed).unwrap();
    let log = || std::fs::read_to_string(dir.join("daemon.log")).unwrap_or_default();
    let mut daemon = Command::new(&installed);
    daemon
        .args(["--nats", &server.url, "daemon", "--ant", "dev-a", "--harness", "copilot", "--upgrade-check", "1s"])
        .arg("--workdir")
        .arg(&dir)
        .arg("--home")
        .arg(dir.join("home"))
        .env_remove("ANTS_CREDS")
        .env_remove("ANTS_CONFIG")
        .env_remove("ANTS_UPGRADE_CHECK")
        .env("PATH", &bin)
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(dir.join("daemon.log")).unwrap());
    let mut daemon = Killed(daemon.spawn().unwrap());
    let start = Instant::now();
    while !log().contains("serving inbox") {
        assert!(start.elapsed() < WAIT, "daemon log:\n{}", log());
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_millis(1500));
    assert!(daemon.0.try_wait().unwrap().is_none(), "exited unreplaced\ndaemon log:\n{}", log());

    let staged = bin.join(".ants-new");
    std::fs::copy(ANTS, &staged).unwrap();
    std::fs::rename(&staged, &installed).unwrap();
    let start = Instant::now();
    let status = loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            break status;
        }
        assert!(start.elapsed() < WAIT, "still running\ndaemon log:\n{}", log());
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(status.code(), Some(75), "daemon log:\n{}", log());
    let log = log();
    assert!(log.contains("the executable was replaced") && log.contains("stopped"), "{log}");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

fn the_builtin_messenger_hands_over_to_a_listed_one() {
    let Some(server) = NatsServer::start() else { return };
    let dir = scratch_dir("handover");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    link_self(&bin, "copilot");
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
        let colony = run(ants(&["colony", "--json"]));
        let entry: serde_json::Value = serde_json::from_slice(&colony.stdout).unwrap();
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
    let listed =
        daemon("listed", &format!("[[ant]]\nname = \"messenger\"\nworkdir = {dir:?}\nharness = \"copilot\"\n"));
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
    link_self(&bin, "stub-adapter");
    let (user_home, home) = (dir.join("user"), dir.join("dev-a"));
    std::fs::create_dir_all(&user_home).unwrap();
    let config = dir.join("config.toml");
    let original = format!(
        "harness = \"stubby\"\n\
         [harnesses.stubby]\ncommand = \"bin/stub-adapter\"\nargs = [\"--flag\", \"two words\"]\n\
         env = {{ ANTS_TEST_ADAPTER = \"1\", OVERRIDDEN = \"definition\" }}\n\
         [harnesses.strict]\ncommand = \"bin/stub-adapter\"\nmode = \"default\"\n\
         [[ant]]\nname = \"dev-a\"\nworkdir = {dir:?}\nhome = {home:?}\nenv = {{ OVERRIDDEN = \"ant\" }}\n"
    );
    std::fs::write(&config, &original).unwrap();
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

    // SIGHUP reloads the file: dev-c newly listed starts, dev-a changed restarts, the messenger carries on.
    let wait_for = |text: &str, times: usize| {
        let start = Instant::now();
        while daemon_log().matches(text).count() < times {
            assert!(start.elapsed() < WAIT, "daemon log lacks {text:?} {times} times:\n{}", daemon_log());
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let hangup =
        || assert!(Command::new("kill").arg("-HUP").arg(_daemon.0.id().to_string()).status().unwrap().success());
    let listed = |text: &str| {
        let changed = original.replace("OVERRIDDEN = \"ant\"", "OVERRIDDEN = \"changed\"");
        std::fs::write(&config, format!("{changed}{text}")).unwrap();
    };
    wait_for("serving inbox", 2);
    listed(&format!("[[ant]]\nname = \"dev-c\"\nworkdir = {dir:?}\n"));
    hangup();
    wait_for("config reloaded", 1);
    ask(&["@dev-c", "hi"], "dev-c: stub: hi\nargs=--flag two words; OVERRIDDEN=definition");
    wait_for("serving inbox", 4);
    ask(&["@dev-a", "hi"], "dev-a: stub: hi\nargs=--flag two words; OVERRIDDEN=changed");
    let restarts: Vec<_> =
        daemon_log().lines().filter(|l| l.contains("restarting the ant")).map(str::to_owned).collect();
    assert!(restarts.len() == 1 && restarts[0].contains("dev-a"), "{}", daemon_log());
    assert_eq!(daemon_log().matches("serving inbox").count(), 4, "{}", daemon_log());

    // A file that does not parse leaves the ants served as they are.
    std::fs::write(&config, "[[ant]\n").unwrap();
    hangup();
    wait_for("reload failed", 1);
    ask(&["@dev-c", "again"], "dev-c: stub: again");

    // An ant no longer listed stops and leaves the colony.
    listed("");
    hangup();
    wait_for("config reloaded", 2);
    wait_for("stopping the ant no longer listed", 1);
    let start = Instant::now();
    while stdout(&run(ants(&["colony", "--json"]))).contains("\"dev-c\"") {
        assert!(start.elapsed() < WAIT, "dev-c still in the colony\ndaemon log:\n{}", daemon_log());
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `ants setup` against a running server, in a scratch home and without services, twice: the second run keeps what
/// the first wrote. Completion then reads the server from the config file setup wrote.
fn setup_brings_a_machine_up_once_and_completions_name_its_ants_channels_and_threads() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("setup");
    let workdir = home.join("work");
    std::fs::create_dir_all(&workdir).unwrap();
    // A PATH with codex-acp the only built-in harness on it.
    let bin = home.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::write(bin.join(if cfg!(windows) { "codex-acp.exe" } else { "codex-acp" }), "").unwrap();
    let path = std::env::var_os("PATH").unwrap_or_default();
    let builtins = ["copilot", "claude-agent-acp", "codex-acp", "agy"];
    let others: Vec<_> = std::env::split_paths(&path)
        .filter(|dir| {
            !builtins.iter().any(|command| ants_daemon::harness::on_path(command.as_ref(), dir.as_os_str()).is_some())
        })
        .collect();
    let none = std::env::join_paths(&others).unwrap();
    let codex = std::env::join_paths(std::iter::once(bin).chain(others)).unwrap();
    let ants_on = |path: &std::ffi::OsStr, args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(args).env("HOME", &home).env("PATH", path).env("SHELL", "/bin/bash").current_dir(&home);
        for var in ["ANTS_CREDS", "ANTS_CONFIG", "ANTS_NATS_URL", "ANTS_JS_DOMAIN", "XDG_DATA_HOME", "XDG_CONFIG_HOME"]
        {
            command.env_remove(var);
        }
        command
    };
    let ants = |args: &[&str]| ants_on(&codex, args);

    let other = home.join("other.toml");
    let out =
        run(ants_on(&none, &["--nats", &server.url, "setup", "--no-services", "--config", other.to_str().unwrap()]));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!out.status.success() && !other.exists(), "{stdout}");
    assert!(stdout.starts_with("fail   harness choice: no built-in harness is installed here"), "{stdout}");
    assert!(stdout.contains("npm install -g @agentclientprotocol/codex-acp; sign in: codex-acp login"), "{stdout}");

    let setup = |ant: &str| {
        let workdir = workdir.to_str().unwrap();
        let args =
            ["--nats", &server.url, "setup", "--no-services", "--no-harness", "--ant", ant, "--workdir", workdir];
        let out = run(ants(&args));
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(out.status.success(), "{stdout}{}", String::from_utf8_lossy(&out.stderr));
        stdout
    };

    let first = setup("dev");
    let config = home.join(".ants/config.toml");
    for line in [
        "done   harness choice: codex, the only built-in harness installed here",
        "done   nats-server config",
        "done   ants init",
        "done   config",
        "done   completions",
        "ok    config",
    ] {
        assert!(first.contains(line), "{line:?} in:\n{first}");
    }
    let written = std::fs::read_to_string(&config).unwrap();
    assert!(written.contains("\nharness = \"codex\"\n"), "{written}");
    assert!(written.contains(&format!("nats = \"{}\"", server.url)), "{written}");
    assert!(written.contains("messenger = true\n") && written.contains("name = \"dev\""), "{written}");
    assert!(std::fs::read_to_string(home.join(".config/ants/nats.conf")).unwrap().contains("jetstream {"));
    let script = home.join(".local/share/bash-completion/completions/ants");
    assert!(std::fs::read_to_string(&script).unwrap().contains("ANTS_COMPLETE=\"bash\""));

    let again = setup("dev");
    for line in ["exists nats-server config", "exists config", "exists completions"] {
        assert!(again.contains(line), "{line:?} in:\n{again}");
    }
    assert_eq!(std::fs::read_to_string(&config).unwrap(), written, "a re-run keeps the config file");
    assert!(setup("qa").contains("done   config"), "a new ant is added");
    assert!(std::fs::read_to_string(&config).unwrap().starts_with(&written));

    let out = run(ants(&["--nats", &server.url, "channel", "create", "ops"]));
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let out = run(ants(&["--nats", &server.url, "ask", "@dev", "hello", "there"]));
    let asked = String::from_utf8_lossy(&out.stdout);
    let thread = asked.lines().next().and_then(|t| t.strip_prefix("thread ")).expect(&asked).to_owned();
    let message = asked.lines().nth(1).and_then(|m| m.strip_prefix("message ")).expect(&asked).to_owned();

    let complete = |words: &[&str]| {
        let mut command = ants(&["--"]);
        command.args(words).env("ANTS_COMPLETE", "bash").env("_CLAP_IFS", "\n");
        command.env("_CLAP_COMPLETE_INDEX", (words.len() - 1).to_string());
        let out = run(command);
        assert!(out.stderr.is_empty(), "{}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).lines().map(str::to_owned).collect::<Vec<_>>()
    };
    assert_eq!(complete(&["ants", "post", "o"]), ["ops"]);
    assert_eq!(complete(&["ants", "ask", "@"]), ["@dev", "@messenger", "@qa"]);
    assert_eq!(complete(&["ants", "thread", "show", &thread[..8]]), [thread.as_str()]);
    assert_eq!(complete(&["ants", "thread", "show", "la"]), ["last"]);
    assert_eq!(complete(&["ants", "react", &thread, "e"]), ["eyes"]);
    assert_eq!(complete(&["ants", "react", &message[..8]]), [message.as_str()]);
    let out = run(ants(&["--nats", &server.url, "react", &message[..13], "eyes"]));
    let (reacted, error) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert_eq!(reacted.trim(), format!("reacted eyes to message {message}"), "{error}");
    drop(server);
    assert!(complete(&["ants", "post", "o"]).is_empty(), "no server, no candidates");
    let _ = std::fs::remove_dir_all(&home);
}

/// Run `command` to completion, killing it after [`WAIT`].
/// `GET path` on `addr` over a raw connection: the status and body.
fn get(addr: &str, path: &str) -> (u16, String) {
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n").unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    let (head, body) = answer.split_once("\r\n\r\n").unwrap_or_else(|| panic!("GET {path}: {answer:?}"));
    (head.split(' ').nth(1).unwrap().parse().unwrap(), body.to_owned())
}

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
    let context =
        args.windows(2).find(|args| args[0] == "--context").map_or_else(|| "unset".to_owned(), |args| args[1].clone());
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
                    responder.respond(NewSessionResponse::new("stub").config_options(on_new.locked().clone()))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: SetSessionConfigOptionRequest, responder, _cx| {
                    let mut options = on_set.locked();
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
                        let _ = write!(
                            reply,
                            "\n{db}; BD_DB={}; BD_REMOTE={}",
                            std::env::var("BD_DB").unwrap(),
                            std::env::var("BD_REMOTE").unwrap()
                        );
                        let options = options.locked();
                        let selected: Vec<_> = options
                            .iter()
                            .map(|option| {
                                let SessionConfigKind::Select(select) = &option.kind else { unreachable!() };
                                select.current_value.0.as_ref()
                            })
                            .collect();
                        let _ = write!(reply, "\nmodel={}; effort={}; context={context}", selected[0], selected[1]);
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
