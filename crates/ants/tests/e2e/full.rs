//! A hub whose JetStream file store fills up: the daemon warns the human past `[storage] warn_percent`, `ants ask`
//! is refused while the store is full, replies that cannot be stored leave their messages unacked, and once the store
//! has room again every message accepted before it filled is answered exactly once, none a dead letter.
//!
//! The store stays full for less than a message's five attempts, 30 s apart. The test takes about 45 s: the daemon
//! retries a failed reply 30 s later, and first reads the usage 30 s after starting.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ants_test_support::{NatsServer, scratch_dir};
use async_nats::jetstream;

use super::{ANTS, Killed, link_self, run};

/// The hub's `max_file_store`.
const STORE: u64 = 16 << 20;
/// Messages accepted before the store fills, each answered once it has room again.
const ACCEPTED: usize = 6;
/// The stream that fills the store, besides the ants' own.
const FILLER: &str = "FILLER";

#[expect(clippy::too_many_lines, reason = "one scenario: fill, refuse, free, then the answers")]
pub fn a_full_hub_answers_once_space_returns() {
    let Some(server) = NatsServer::spawn_with(|dir| {
        format!("jetstream {{ store_dir: {:?}, max_file_store: {STORE}, max_memory_store: 8MB }}\n", dir.join("js"))
    }) else {
        return;
    };
    let dir = scratch_dir("full");
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
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        format!(
            "messenger = false\nsage = \"none\"\nharness = \"copilot\"\n\
             [storage]\nwarn_percent = 60\nmax_file_store = {STORE}\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = {dir:?}\nhome = {:?}\n",
            dir.join("home")
        ),
    )
    .unwrap();
    let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
    let daemon = ants(&["daemon"])
        .arg("--config")
        .arg(&config)
        .env("PATH", &bin)
        .env("ANTS_TEST_PROMPT_DELAY_MS", "3000")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    let daemon = Killed(daemon);
    let logs = || super::flood::tail(&std::fs::read_to_string(dir.join("daemon.log")).unwrap_or_default(), 40);
    let warm = run(ants(&["ask", "--wait", "--timeout", "60s", "@dev-a", "warmup"]));
    assert!(warm.status.success(), "ants ask: {}\n{}", String::from_utf8_lossy(&warm.stderr), logs());

    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let js = runtime.block_on(async { jetstream::new(async_nats::connect(&server.url).await.unwrap()) });
    let filler = || {
        let config = jetstream::stream::Config {
            name: FILLER.to_owned(),
            subjects: vec!["filler.>".to_owned()],
            ..Default::default()
        };
        runtime.block_on(async { js.create_stream(config).await.unwrap() });
    };
    filler();

    // Accepted while there is room, then the store fills before their prompts end.
    let tokens: Vec<String> = (0..ACCEPTED).map(|i| format!("tok-f{i:05}")).collect();
    for token in &tokens {
        let out = run(ants(&["ask", "@dev-a", token]));
        assert!(out.status.success(), "ants ask {token}: {}", String::from_utf8_lossy(&out.stderr));
    }
    for size in [100_000, 1_000, 50] {
        let refused = runtime.block_on(fill(&js, size, u64::MAX));
        assert!(refused.as_deref().is_some_and(|e| e.contains("insufficient resources")), "{refused:?}");
    }
    let refused = run(ants(&["ask", "@dev-a", "tok-f99999"]));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success() && stderr.contains("insufficient resources"), "ants ask on a full hub: {stderr}");

    // Each reply fails to store and leaves its message unacked, with the daemon idle meanwhile.
    let cpu = cpu_seconds(daemon.0.id());
    let full = Instant::now();
    let failed = |log: &str| log.matches("handling message").count();
    while failed(&std::fs::read_to_string(dir.join("daemon.log")).unwrap_or_default()) < ACCEPTED {
        assert!(full.elapsed() < Duration::from_secs(30), "replies not refused\n{}", logs());
        std::thread::sleep(Duration::from_millis(500));
    }
    std::thread::sleep(Duration::from_secs(3));
    if let (Some(before), Some(after)) = (cpu, cpu_seconds(daemon.0.id())) {
        let busy = after - before;
        assert!(busy < full.elapsed().as_secs_f64() / 2.0, "the daemon spun {busy:.1} s on a full store\n{}", logs());
    }
    let early = answers(&replies(&ants));
    assert!(tokens.iter().all(|token| !early.contains_key(token)), "answered on a full store: {early:?}");

    // Room again, though past the warning share: the daemon's first reading of the usage, 30 s after it started,
    // warns the human, and every accepted message is answered once, on its retry.
    runtime.block_on(async { js.delete_stream(FILLER).await.unwrap() });
    filler();
    let filled = runtime.block_on(fill(&js, 100_000, STORE * 7 / 10 / 100_000));
    assert!(filled.is_none(), "refused short of full: {filled:?}");
    let freed = Instant::now();
    let warning = "JetStream file store of the hub";
    loop {
        let replies = replies(&ants);
        let answered = answers(&replies);
        let missing: Vec<_> = tokens.iter().filter(|token| !answered.contains_key(*token)).collect();
        let warned = replies.iter().find(|text| text.contains(warning));
        if let Some(warned) = warned {
            assert!(warned.contains("Using most: FILLER") && warned.contains("max_file_store"), "{warned}");
            if missing.is_empty() {
                break;
            }
        }
        assert!(
            freed.elapsed() < Duration::from_secs(90),
            "unanswered once freed: {missing:?}, warned: {}\n{}",
            warned.is_some(),
            logs()
        );
        std::thread::sleep(Duration::from_secs(1));
    }
    std::thread::sleep(Duration::from_secs(2));
    let answered = answers(&replies(&ants));
    let twice: Vec<_> = answered.iter().filter(|(_, n)| **n > 1).collect();
    assert!(twice.is_empty(), "answered more than once: {twice:?}\n{}", logs());
    assert!(!answered.contains_key("tok-f99999"), "answered though refused");
    let dead = run(ants(&["dead", "list"]));
    let dead = String::from_utf8_lossy(&dead.stdout);
    assert_eq!(dead.trim(), "no dead letters", "{}", logs());
    let mut daemon = daemon;
    assert!(daemon.0.try_wait().unwrap().is_none(), "the daemon exited\n{}", logs());
}

/// Publish up to `count` messages of `size` bytes into the filler stream; JetStream's error for the first one it
/// refuses, or None when it took them all.
async fn fill(js: &jetstream::Context, size: usize, count: u64) -> Option<String> {
    let payload = vec![b'x'; size];
    for _ in 0..count {
        let ack = match js.publish("filler.x", payload.clone().into()).await {
            Ok(ack) => ack.await,
            Err(e) => Err(e),
        };
        if let Err(e) = ack {
            return Some(e.to_string());
        }
    }
    None
}

/// The texts of dev-a's replies.
fn replies(ants: &impl Fn(&[&str]) -> Command) -> Vec<String> {
    let search =
        ants(&["thread", "search", "--from", "dev-a", "--since", "1h", "--limit", "10000", "--json"]).output().unwrap();
    assert!(search.status.success(), "ants thread search: {}", String::from_utf8_lossy(&search.stderr));
    String::from_utf8_lossy(&search.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|entry| entry["text"].as_str().map(str::to_owned))
        .collect()
}

/// How many of `replies` carry each token.
fn answers(replies: &[String]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for text in replies {
        let mut seen = String::new();
        for (at, _) in text.match_indices("tok-f") {
            let Some(token) = text.get(at..at + 10) else { continue };
            if token[5..].bytes().all(|b| b.is_ascii_digit()) && !seen.contains(token) {
                let _ = write!(seen, "{token} ");
                *counts.entry(token.to_owned()).or_insert(0) += 1;
            }
        }
    }
    counts
}

/// The CPU time process `pid` used so far, where `ps` tells it.
fn cpu_seconds(pid: u32) -> Option<f64> {
    let out = Command::new("ps").args(["-o", "time=", "-p", &pid.to_string()]).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let (days, clock) = text.trim().split_once('-').unwrap_or(("0", text.trim()));
    let seconds = clock.split(':').try_fold(0.0, |total, part| part.parse::<f64>().ok().map(|n| total * 60.0 + n))?;
    Some(days.parse::<f64>().ok()? * 86_400.0 + seconds)
}
