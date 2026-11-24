//! A flood of messages on one ant: each answered exactly once, a withdrawn one never.
//!
//! By default a small flood that runs in seconds. `ANTS_STRESS=N` sends N times as many messages, each prompt taking
//! 300 ms, and kills the daemon (SIGKILL) twice while the backlog drains, restarting it: the messages in hand are
//! then redelivered after the ack wait (60 s), so a stress run takes minutes.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use ants_test_support::NatsServer;

use super::{Fixture, count_tokens, run, wait_answered};

/// Sent at once, at most this many `ants ask` at a time.
const BATCH: usize = 32;

pub fn a_flood_of_messages_is_answered_once_each() {
    let Some(server) = NatsServer::start() else { return };
    let stress: usize = std::env::var("ANTS_STRESS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let scale = stress.max(1);
    let fixture = Fixture::new("flood", &server, "copilot");
    let dir = &fixture.dir;
    let ants = |args: &[&str]| fixture.ants(args);
    let init = run(ants(&["init"]));
    assert!(init.status.success(), "ants init: {}", String::from_utf8_lossy(&init.stderr));

    let start_daemon = |run: usize| {
        let mut command = ants(&["daemon", "--harness", "copilot", "--human", "human", "--max-harnesses", "4"]);
        command
            .arg("--home")
            .arg(dir.join("home"))
            .arg("--workdir")
            .arg(dir)
            .env("ANTS_ANT", "dev-a")
            .env("PATH", &fixture.bin)
            .env("ANTS_TEST_PROMPT_DELAY_MS", if stress > 0 { "300" } else { "20" });
        fixture.spawn_daemon(&mut command, &format!("daemon.{run}.log"))
    };
    let logs = || {
        let mut all = String::new();
        for run in 1..=3 {
            if let Ok(log) = std::fs::read_to_string(dir.join(format!("daemon.{run}.log"))) {
                let _ = write!(all, "--- daemon run {run}\n{}", tail(&log, 40));
            }
        }
        all
    };
    let mut daemon = start_daemon(1);
    let warm = run(ants(&["ask", "--wait", "--timeout", "60s", "@dev-a", "warmup"]));
    assert!(warm.status.success(), "ants ask: {}\n{}", String::from_utf8_lossy(&warm.stderr), logs());

    // One message on each of many threads, past max_harnesses.
    let sent = send_all(&ants, (0..40 * scale).map(|i| vec!["@dev-a".to_owned(), format!("tok-t{i:05}")]).collect());
    let one = sent[0].1.clone();
    // Many on one thread: two in three steer the running prompt, the others queue behind it.
    let on_one = (0..30 * scale)
        .map(|i| {
            let mut args = vec!["--thread".to_owned(), one.clone()];
            if i % 3 == 0 {
                args.push("--queue".to_owned());
            }
            args.extend(["@dev-a".to_owned(), format!("tok-s{i:05}")]);
            args
        })
        .collect();
    let sent: Vec<_> = sent.into_iter().chain(send_all(&ants, on_one)).collect();
    let token_of: HashMap<&str, &str> =
        sent.iter().map(|(token, _, message)| (message.as_str(), token.as_str())).collect();

    // Withdraw some of those still waiting; only those the CLI says it withdrew must stay unanswered.
    let queue = run(ants(&["queue", "dev-a", "--json"]));
    let waiting: Vec<String> = String::from_utf8_lossy(&queue.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|row| row["running"] != true)
        .filter_map(|row| row["id"].as_str().map(str::to_owned))
        .filter(|id| token_of.contains_key(id.as_str()))
        .collect();
    let mut withdrawn = HashSet::new();
    for id in waiting.iter().rev().take(5 * scale) {
        let out = run(ants(&["withdraw", "dev-a", id]));
        if out.status.success() && String::from_utf8_lossy(&out.stdout).starts_with("withdrew") {
            withdrawn.insert(token_of[id.as_str()].to_owned());
        }
    }
    if stress > 0 {
        assert!(!withdrawn.is_empty(), "nothing withdrawn of {} waiting\n{}", waiting.len(), logs());
        for run in [2, 3] {
            std::thread::sleep(Duration::from_secs(2));
            daemon.kill();
            daemon = start_daemon(run);
        }
    }

    // Every message not withdrawn is answered, then nothing more comes for it.
    let expected: Vec<&str> =
        sent.iter().map(|(token, ..)| token.as_str()).filter(|token| !withdrawn.contains(*token)).collect();
    let limit = Duration::from_secs(60 + 300 * stress as u64);
    wait_answered(limit, Duration::from_secs(1), &expected, || answers(&ants), logs);
    std::thread::sleep(Duration::from_secs(2));
    let answered = answers(&ants);
    let twice: Vec<_> = answered.iter().filter(|(_, n)| **n > 1).collect();
    assert!(twice.is_empty(), "answered more than once: {twice:?}\n{}", logs());
    let despite: Vec<_> = withdrawn.iter().filter(|token| answered.contains_key(*token)).collect();
    assert!(despite.is_empty(), "answered though withdrawn: {despite:?}\n{}", logs());
    assert!(daemon.running(), "the daemon exited\n{}", logs());
}

/// Run `ants ask` with each of `asks`' arguments, [`BATCH`] at a time; each message's token, thread and id.
fn send_all(ants: &impl Fn(&[&str]) -> Command, asks: Vec<Vec<String>>) -> Vec<(String, String, String)> {
    let mut sent = Vec::new();
    for batch in asks.chunks(BATCH) {
        let children: Vec<_> = batch
            .iter()
            .map(|args| {
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                let mut command = ants(&[&["ask"], &args[..]].concat());
                (
                    args.last().unwrap().to_string(),
                    command.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap(),
                )
            })
            .collect();
        for (token, child) in children {
            let Output { status, stdout, stderr } = child.wait_with_output().unwrap();
            let stdout = String::from_utf8_lossy(&stdout);
            assert!(status.success(), "ants ask {token}: {}", String::from_utf8_lossy(&stderr));
            let field = |name: &str| stdout.lines().find_map(|line| line.strip_prefix(name)).expect(&stdout).to_owned();
            sent.push((token, field("thread "), field("message ")));
        }
    }
    sent
}

/// How many of dev-a's replies carry each token.
fn answers(ants: &impl Fn(&[&str]) -> Command) -> HashMap<String, usize> {
    let mut search = ants(&["thread", "search", "--from", "dev-a", "--since", "1h", "--limit", "100000", "--json"]);
    count_tokens(&mut search, Some("dev-a"), |text| {
        text.get(..10).filter(|token| {
            token[4..5].chars().all(|c| c == 't' || c == 's') && token[5..].bytes().all(|b| b.is_ascii_digit())
        })
    })
}

pub fn tail(text: &str, lines: usize) -> String {
    let all: Vec<_> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n") + "\n"
}
