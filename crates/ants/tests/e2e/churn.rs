//! Reloads under traffic: ants listed and unlisted, a busy ant's settings changed and the file read half written, by
//! SIGHUP while messages flow; each message answered exactly once once every ant is listed again, and `/healthz` at
//! 200 throughout, an ant a reload starts left out of health until it connects.
//!
//! By default a few seconds of churn under light traffic. `ANTS_STRESS=N` churns N times as long, with more senders and
//! prompts of 200 ms.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ants_daemon::sync::MutexExt as _;
use ants_test_support::{NatsServer, free_port, scratch_dir};

use super::{ANTS, Killed, get, link_self, run};

/// Ants listed and unlisted besides dev-a, which stays listed.
const CHURNED: usize = 3;

#[expect(clippy::too_many_lines, reason = "one scenario: the daemon, the senders, the churn, then the answers")]
pub fn reloads_under_traffic_lose_no_message() {
    if !cfg!(unix) {
        return; // SIGHUP
    }
    let Some(server) = NatsServer::start() else { return };
    let stress: u64 = std::env::var("ANTS_STRESS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let dir = scratch_dir("churn");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    link_self(&bin, "copilot");
    let url = server.url.clone();
    let ants = move |args: &[&str]| {
        let mut command = Command::new(ANTS);
        command.args(["--nats", &url]).args(args).env_remove("ANTS_CREDS").env_remove("ANTS_CONFIG");
        command
    };
    let init = run(ants(&["init"]));
    assert!(init.status.success(), "ants init: {}", String::from_utf8_lossy(&init.stderr));

    let config = dir.join("config.toml");
    let metrics = format!("127.0.0.1:{}", free_port());
    let render = |listed: &[bool], generation: usize| {
        let mut text = format!("messenger = false\nsage = \"none\"\nharness = \"copilot\"\nmetrics = {metrics:?}\n");
        let names =
            std::iter::once("dev-a".to_owned()).chain((0..CHURNED).filter(|i| listed[*i]).map(|i| format!("c-{i}")));
        for name in names {
            let home = dir.join("homes").join(&name);
            std::fs::create_dir_all(&home).unwrap();
            let _ =
                write!(text, "\n[[ant]]\nname = \"{name}\"\nworkdir = {home:?}\nhome = {home:?}\nmax_harnesses = 2\n");
            if name == "dev-a" {
                let _ = writeln!(text, "whoami = \"generation {generation}\"");
            }
        }
        text
    };
    let mut listed = [true; CHURNED];
    std::fs::write(&config, render(&listed, 0)).unwrap();
    let log = std::fs::File::create(dir.join("daemon.log")).unwrap();
    let daemon = ants(&["daemon"])
        .arg("--config")
        .arg(&config)
        .env("PATH", &bin)
        .env("ANTS_TEST_PROMPT_DELAY_MS", if stress > 0 { "200" } else { "20" })
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    let pid = daemon.id().to_string();
    let mut daemon = Killed(daemon);
    let logs = || super::flood::tail(&std::fs::read_to_string(dir.join("daemon.log")).unwrap_or_default(), 60);
    let warm = run(ants(&["ask", "--wait", "--timeout", "60s", "@dev-a", "warmup"]));
    assert!(warm.status.success(), "ants ask: {}\n{}", String::from_utf8_lossy(&warm.stderr), logs());

    // Senders, each to dev-a or one of the churned ants in turn, whether listed or not.
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(Mutex::new(Vec::new()));
    let pause = Duration::from_millis(if stress > 0 { 50 } else { 200 });
    let senders: Vec<_> = (0..2 + 2 * stress)
        .map(|sender| {
            let (stop, sent, ants) = (stop.clone(), sent.clone(), ants.clone());
            std::thread::spawn(move || {
                let mut n = 0;
                while !stop.load(Ordering::Relaxed) {
                    let to = if n % 2 == 0 { "dev-a".to_owned() } else { format!("c-{}", n % CHURNED) };
                    let token = format!("tok-{sender}x{n:05}");
                    let out = run(ants(&["ask", &format!("@{to}"), &token]));
                    assert!(out.status.success(), "ants ask: {}", String::from_utf8_lossy(&out.stderr));
                    sent.locked().push(token);
                    n += 1;
                    std::thread::sleep(pause);
                }
            })
        })
        .collect();

    // Health probes, every one while the churn starts ants and stops them answered 200.
    let unhealthy = Arc::new(Mutex::new(Vec::new()));
    let prober = {
        let (stop, unhealthy, metrics) = (stop.clone(), unhealthy.clone(), metrics.clone());
        std::thread::spawn(move || {
            let mut probes = 0;
            while !stop.load(Ordering::Relaxed) {
                let (status, body) = get(&metrics, "/healthz");
                if status != 200 {
                    unhealthy.locked().push(format!("{status} {body}"));
                }
                probes += 1;
                std::thread::sleep(Duration::from_millis(50));
            }
            probes
        })
    };

    // Churn: one ant toggled per reload, dev-a's whoami changed every third, and every file written in place in
    // pieces with a second SIGHUP in the middle, so some reloads read it half written and are refused.
    let hup = || assert!(Command::new("kill").args(["-HUP", &pid]).status().unwrap().success());
    let until = Instant::now() + Duration::from_secs(6 + 30 * stress);
    let mut generation = 0;
    let mut reloads = 0;
    while Instant::now() < until {
        listed[reloads % CHURNED] ^= true;
        if reloads % 3 == 0 {
            generation += 1;
        }
        write_in_pieces(&config, &render(&listed, generation), &hup);
        hup();
        reloads += 1;
        std::thread::sleep(Duration::from_millis(150));
        assert!(alive(&mut daemon), "the daemon exited\n{}", logs());
    }
    stop.store(true, Ordering::Relaxed);
    for sender in senders {
        sender.join().unwrap();
    }
    let probes = prober.join().unwrap();
    let unhealthy = unhealthy.locked().clone();
    assert!(unhealthy.is_empty(), "{} of {probes} health probes failed: {unhealthy:?}\n{}", unhealthy.len(), logs());
    std::fs::write(&config, render(&[true; CHURNED], generation)).unwrap();
    hup();

    let sent = sent.locked().clone();
    let limit = Duration::from_secs(120 + 600 * stress);
    let start = Instant::now();
    let mut answered = answers(&ants);
    while sent.iter().any(|token| !answered.contains_key(token)) {
        let missing: Vec<_> = sent.iter().filter(|token| !answered.contains_key(*token)).take(10).collect();
        assert!(start.elapsed() < limit, "unanswered after {limit:?}, such as {missing:?}\n{}", logs());
        std::thread::sleep(Duration::from_secs(1));
        answered = answers(&ants);
    }
    std::thread::sleep(Duration::from_secs(2));
    let twice: Vec<_> = answers(&ants).into_iter().filter(|(_, n)| *n > 1).collect();
    assert!(twice.is_empty(), "answered more than once: {twice:?}\n{}", logs());
    let log = std::fs::read_to_string(dir.join("daemon.log")).unwrap();
    assert!(log.contains("reload failed; serving the ants as before"), "no reload read a half-written file: {reloads}");
    assert!(alive(&mut daemon), "the daemon exited\n{}", logs());
}

fn alive(daemon: &mut Killed) -> bool {
    daemon.0.try_wait().unwrap().is_none()
}

/// Write `text` over `path` in place, 64 bytes at a time, calling `midway` halfway and leaving the file half written
/// a while after.
fn write_in_pieces(path: &Path, text: &str, midway: &impl Fn()) {
    let mut file = std::fs::File::create(path).unwrap();
    let half = text.len() / 2 / 64;
    for (i, piece) in text.as_bytes().chunks(64).enumerate() {
        file.write_all(piece).unwrap();
        file.flush().unwrap();
        if i == half {
            midway();
            std::thread::sleep(Duration::from_millis(50));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// How many replies, by any ant, carry each token.
fn answers(ants: &impl Fn(&[&str]) -> Command) -> HashMap<String, usize> {
    let search = ants(&["thread", "search", "--since", "1h", "--limit", "100000", "--json"]).output().unwrap();
    assert!(search.status.success(), "ants thread search: {}", String::from_utf8_lossy(&search.stderr));
    let mut counts = HashMap::new();
    for line in String::from_utf8_lossy(&search.stdout).lines() {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        if matches!(entry["from"].as_str(), None | Some("human")) {
            continue;
        }
        let text = entry["text"].as_str().unwrap_or_default();
        let tokens: HashSet<&str> = text
            .match_indices("tok-")
            .filter_map(|(at, _)| {
                let rest = &text[at + 4..];
                let (sender, n) = rest.split_once('x')?;
                let n = n.get(..5)?;
                ((1..=3).contains(&sender.len())
                    && sender.bytes().all(|b| b.is_ascii_digit())
                    && n.bytes().all(|b| b.is_ascii_digit()))
                .then(|| &text[at..at + 4 + sender.len() + 1 + 5])
            })
            .collect();
        for token in tokens {
            *counts.entry(token.to_owned()).or_insert(0) += 1;
        }
    }
    counts
}
