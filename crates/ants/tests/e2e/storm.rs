//! Channel storms: human posts @mentioning every member of a channel, ants and the human, and posts mentioning nobody
//! that the built-in messenger triages. The stub echoes each prompt, mentions included, so every ant reply mentions
//! everyone again: the thread's turn budget ends each storm, every member is woken, the messenger triages each
//! unmentioned post once, the human gets one mention notice per post mentioning it, and `ants channel read` prints
//! every post.
//!
//! By default 6 ants and one storm of each kind. `ANTS_STRESS=N` serves 6 + 6N ants and runs 1 + N storms of each.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ants_proto::{DEFAULT_TURN_BUDGET, UserId, parse_mentions};
use ants_test_support::{NatsServer, scratch_dir};

use super::{ANTS, Killed, link_self, run};

#[expect(clippy::too_many_lines, reason = "one scenario: the daemon, the storms, then what they did")]
pub fn a_channel_storm_ends_at_the_turn_budget() {
    let Some(server) = NatsServer::start() else { return };
    let stress: usize = std::env::var("ANTS_STRESS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let members = 6 + 6 * stress;
    let storms = 1 + stress;
    let dir = scratch_dir("storm");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    link_self(&bin, "copilot");
    let ants = |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &server.url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command.env("HOME", &dir);
        command
    };
    let ok = |args: &[&str]| {
        let out = run(ants(args));
        assert!(out.status.success(), "ants {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    ok(&["init"]);

    let names: Vec<String> = (0..members).map(|i| format!("s{i}")).collect();
    let mut config = "messenger = true\nsage = \"none\"\nharness = \"copilot\"\n".to_owned();
    for name in &names {
        let home = dir.join("homes").join(name);
        std::fs::create_dir_all(&home).unwrap();
        let _ =
            write!(config, "\n[[ant]]\nname = \"{name}\"\nworkdir = {home:?}\nhome = {home:?}\nmax_harnesses = 2\n");
    }
    std::fs::write(dir.join("config.toml"), config).unwrap();
    let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
    let _daemon = Killed(
        ants(&["daemon"])
            .arg("--config")
            .arg(dir.join("config.toml"))
            .env("PATH", &bin)
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .env("ANTS_TEST_PROMPT_DELAY_MS", "20")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    let read_log = || std::fs::read_to_string(dir.join("daemon.log")).unwrap_or_default();
    let logs = || super::flood::tail(&read_log(), 60);
    // The messenger serves once it holds its colony entry; both answering means both serve.
    for to in [&["@s0", "warmup"][..], &["warmup"]] {
        let warm = run(ants(&[&["ask", "--wait", "--timeout", "60s"][..], to].concat()));
        assert!(warm.status.success(), "ants ask: {}\n{}", String::from_utf8_lossy(&warm.stderr), logs());
    }

    let all: Vec<String> = names.iter().map(|n| format!("@{n}")).collect();
    ok(&["channel", "create", "#storm", "--member", &format!("{},human", names.join(","))]);
    let storm = format!("{} @human storm", all.join(" "));
    let thread_of = |posted: String| posted.lines().next().and_then(|t| t.strip_prefix("thread ")).unwrap().to_owned();
    let mentioned: Vec<String> = (0..storms).map(|_| thread_of(ok(&["post", "#storm", &storm]))).collect();
    let triaged: Vec<String> = (0..storms).map(|_| thread_of(ok(&["post", "#storm", "who", "can", "help?"]))).collect();

    // Quiet: no prompt started and no escalation sent for a while.
    let activity = || {
        let log = read_log();
        log.matches("prompting thread=").count() + log.matches("escalated thread=").count()
    };
    let limit = Duration::from_secs(120 + 600 * stress as u64);
    let start = Instant::now();
    let (mut last, mut quiet) = (activity(), 0);
    while quiet < 3 {
        assert!(start.elapsed() < limit, "the storms went on past {limit:?}\n{}", logs());
        std::thread::sleep(Duration::from_secs(1));
        let now = activity();
        quiet = if now == last { quiet + 1 } else { 0 };
        last = now;
    }
    let took = start.elapsed();

    let search = |filter: &[&str]| -> Vec<serde_json::Value> {
        let mut args = vec!["thread", "search", "--since", "1h", "--limit", "100000", "--json"];
        args.extend_from_slice(filter);
        // Read as it comes: the replies echo whole prompts, past what a pipe holds.
        let out = ants(&args).output().unwrap();
        assert!(out.status.success(), "ants {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).lines().map(|line| serde_json::from_str(line).unwrap()).collect()
    };
    let posts = search(&["--channel", "storm", "--type", "post"]);
    let to_human = search(&["--to", "human"]);
    let on = |entries: &[serde_json::Value], thread: &str, kind: &str| -> Vec<serde_json::Value> {
        entries.iter().filter(|e| e["thread_id"] == thread && e["kind"] == kind).cloned().collect()
    };
    let log = read_log();
    let prompted = |ant: &str, thread: &str| {
        let (who, what) = (format!("ant{{name={ant}}}"), format!("prompting thread={thread} "));
        log.lines().filter(|line| line.contains(&who) && line.contains(&what)).count()
    };
    let human: UserId = "human".parse().unwrap();
    let mut summary = HashMap::new();
    for thread in mentioned.iter().chain(&triaged) {
        let by_ants: Vec<_> = on(&posts, thread, "post").into_iter().filter(|p| p["from"] != "human").collect();
        // The storm ends: however the replies race, no member or messenger posts more often than the thread has
        // turns. TODO(ants-bpu3b): racing replies spend the same turn; bound the posts by the budget once fixed.
        let most = DEFAULT_TURN_BUDGET as usize * (members + 1);
        assert!(!by_ants.is_empty() && by_ants.len() <= most, "{} ant posts on {thread}\n{}", by_ants.len(), logs());
        // Every member post spent from the same budget, so the storm ran out of turns and escalated to the human.
        // TODO(ants-l9309): each woken member escalates again on every post it cannot answer; bound it once fixed.
        let escalations = on(&to_human, thread, "escalation").len();
        assert!(escalations >= 1, "no escalation on {thread}\n{}", logs());
        let mentioning = by_ants.iter().filter(|p| parse_mentions(p["text"].as_str().unwrap()).contains(&human));
        assert_eq!(on(&to_human, thread, "mention").len(), mentioning.count(), "one notice per post mentioning");
        let prompts: usize = names.iter().map(|ant| prompted(ant, thread)).sum();
        summary.insert(thread.clone(), (by_ants.len(), prompts, escalations));
    }
    for thread in &mentioned {
        for ant in &names {
            assert!(prompted(ant, thread) >= 1, "{ant} was not woken on {thread}\n{}", logs());
        }
        assert_eq!(prompted("system-messenger", thread), 0, "a post mentioning members bypasses triage");
    }
    for thread in &triaged {
        assert_eq!(prompted("system-messenger", thread), 1, "one triage of {thread}\n{}", logs());
        let answered = on(&posts, thread, "post").iter().any(|p| p["from"] == "system-messenger");
        assert!(answered, "the messenger's triage is posted on {thread}");
    }
    let printed = ants(&["channel", "read", "storm", "--peek"]).output().unwrap();
    assert!(printed.status.success(), "ants channel read: {}", String::from_utf8_lossy(&printed.stderr));
    let printed = String::from_utf8_lossy(&printed.stdout);
    let lines = printed.lines().filter(|line| {
        let line = line.trim_start();
        line.len() > 20
            && line.as_bytes()[4] == b'-'
            && line[20..]
                .split_once(": ")
                .is_some_and(|(from, _)| !from.contains(' ') && posts.iter().any(|p| p["from"] == from))
    });
    assert_eq!(lines.count(), posts.len(), "ants channel read prints every post:\n{printed}");
    eprintln!(
        "storm: {members} members, {} posts in {took:?}; per thread (ant posts, member prompts, escalations to the \
         human): mentioning all {:?}, triaged {:?}",
        posts.len(),
        mentioned.iter().map(|t| summary[t]).collect::<Vec<_>>(),
        triaged.iter().map(|t| summary[t]).collect::<Vec<_>>(),
    );
}
