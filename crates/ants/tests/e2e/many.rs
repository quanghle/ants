//! Many ants on one daemon in git workdirs with worktrees on, half of them each in a repository of its own, the other
//! half sharing one: every thread started at once gets its own worktree and its session's brief, every message is
//! answered exactly once, and once the threads' sessions are gone a restarted daemon removes every worktree and branch.
//!
//! By default 12 ants with 3 threads each. `ANTS_STRESS=N` serves 12×N ants with 3+N threads each.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ants_proto::{SESSIONS_BUCKET, UserId};
use ants_test_support::{NatsServer, have, wait_until};

use super::{Fixture, count_tokens, run, wait_answered};

/// Sent at once, at most this many `ants ask` at a time.
const BATCH: usize = 32;

#[expect(clippy::too_many_lines, reason = "one scenario: the ants, their threads, the answers, then the cleanup")]
pub fn many_ants_in_worktrees_answer_once_and_clean_up() {
    if !have("git") {
        return;
    }
    let Some(server) = NatsServer::start() else { return };
    let stress: usize = std::env::var("ANTS_STRESS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let (count, threads) = (12 * stress.max(1), 3 + stress);
    let fixture = Fixture::new("many", &server, "copilot");
    let dir = &fixture.dir;
    let path = std::env::join_paths(
        std::iter::once(fixture.bin.clone())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())),
    )
    .unwrap();
    let ants = |args: &[&str]| fixture.ants(args);
    let init = run(ants(&["init"]));
    assert!(init.status.success(), "ants init: {}", String::from_utf8_lossy(&init.stderr));

    let names: Vec<String> = (0..count).map(|i| format!("m-{i:03}")).collect();
    let mut config = "messenger = false\nsage = \"none\"\nharness = \"copilot\"\n".to_owned();
    let shared = dir.join("repos").join("shared");
    make_repo(&shared);
    let mut repos = vec![shared.clone()];
    for (i, name) in names.iter().enumerate() {
        let home = dir.join("homes").join(name);
        let repo = if i % 2 == 0 {
            let own = dir.join("repos").join(name);
            make_repo(&own);
            repos.push(own.clone());
            own
        } else {
            shared.clone()
        };
        std::fs::create_dir_all(&home).unwrap();
        let _ = write!(
            config,
            "\n[[ant]]\nname = \"{name}\"\nworkdir = {repo:?}\nhome = {home:?}\nmax_harnesses = {threads}\n\
             worktrees = true\n"
        );
    }
    std::fs::write(dir.join("config.toml"), config).unwrap();
    let start_daemon = |run: usize| {
        let mut command = ants(&["daemon", "--config"]);
        command.arg(dir.join("config.toml")).env("PATH", &path).env("ANTS_TEST_PROMPT_DELAY_MS", "20");
        fixture.spawn_daemon(&mut command, &format!("daemon.{run}.log"))
    };
    let logs = |run: usize| {
        super::flood::tail(&std::fs::read_to_string(dir.join(format!("daemon.{run}.log"))).unwrap_or_default(), 60)
    };
    let daemon = start_daemon(1);
    let limit = Duration::from_secs(60 + 60 * stress as u64);
    wait_until(limit, Duration::from_millis(200), || {
        let all = served(&ants) == count;
        all.then_some(()).ok_or_else(|| format!("not every ant in the colony\n{}", logs(1)))
    });

    // A new thread per message, every ant's threads at once.
    let asks: Vec<(String, String)> = names
        .iter()
        .enumerate()
        .flat_map(|(i, name)| (0..threads).map(move |j| (name.clone(), format!("tok-m{:05}", i * 100 + j))))
        .collect();
    let mut sent = Vec::new();
    for batch in asks.chunks(BATCH) {
        let children: Vec<_> = batch
            .iter()
            .map(|(name, token)| {
                let mut ask = ants(&["ask", &format!("@{name}"), token]);
                (name, token, ask.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap())
            })
            .collect();
        for (name, token, child) in children {
            let out = child.wait_with_output().unwrap();
            let stdout = String::from_utf8_lossy(&out.stdout);
            assert!(out.status.success(), "ants ask {token}: {}", String::from_utf8_lossy(&out.stderr));
            let thread = stdout.lines().find_map(|l| l.strip_prefix("thread ")).expect(&stdout).to_owned();
            sent.push((name.clone(), token.clone(), thread));
        }
    }

    let tokens: Vec<&str> = sent.iter().map(|(_, token, _)| token.as_str()).collect();
    wait_answered(limit, Duration::from_secs(1), &tokens, || answers(&ants), || logs(1));
    std::thread::sleep(Duration::from_secs(2));
    let twice: Vec<_> = answers(&ants).into_iter().filter(|(_, n)| *n > 1).collect();
    assert!(twice.is_empty(), "answered more than once: {twice:?}\n{}", logs(1));
    let worktree = |name: &str, thread: &str| dir.join("homes").join(name).join("worktrees").join(thread);
    let shared: Vec<_> =
        sent.iter().filter(|(name, _, thread)| !worktree(name, thread).join(".git").is_file()).collect();
    assert!(shared.is_empty(), "threads without a worktree of their own: {shared:?}\n{}", logs(1));
    assert!(daemon.running(), "the daemon exited\n{}", logs(1));
    let log = std::fs::read_to_string(dir.join("daemon.1.log")).unwrap();
    let unbriefed = log.lines().filter(|line| line.contains("the provider sent no brief")).count();
    assert_eq!(unbriefed, 0, "sessions started without the brief\n{}", logs(1));

    // With the sessions gone, the next daemon removes every worktree, and every branch, none holding commits.
    daemon.kill();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &UserId::human(), "e2e").await.unwrap();
        let sessions = js.get_key_value(SESSIONS_BUCKET).await.unwrap();
        for (name, _, thread) in &sent {
            sessions.delete(format!("{name}.{thread}")).await.unwrap();
        }
    });
    let daemon = start_daemon(2);
    let start = Instant::now();
    loop {
        let left: Vec<_> = sent.iter().filter(|(name, _, thread)| worktree(name, thread).exists()).collect();
        let branches: HashSet<String> = repos.iter().flat_map(|repo| branches(repo)).collect();
        if left.is_empty() && branches.is_empty() {
            break;
        }
        assert!(
            start.elapsed() < limit,
            "{} worktrees and {} branches left after {limit:?}, such as {:?} {:?}\n{}",
            left.len(),
            branches.len(),
            left.first(),
            branches.iter().next(),
            logs(2)
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    drop(daemon);
}

/// A git repository at `path` with one commit of a few files.
fn make_repo(path: &Path) {
    std::fs::create_dir_all(path.join("src")).unwrap();
    for i in 0..5 {
        std::fs::write(path.join("src").join(format!("f{i}.txt")), format!("file {i}\n")).unwrap();
    }
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .args(["-c", "user.name=e2e", "-c", "user.email=e2e@example.com", "-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(path)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    };
    git(&["init", "-q"]);
    git(&["add", "-A"]);
    git(&["commit", "-qm", "init"]);
}

/// The branches of ants' threads in `repo`.
fn branches(repo: &Path) -> Vec<String> {
    let out = Command::new("git").args(["branch", "--list", "ants/*"]).current_dir(repo).output().unwrap();
    String::from_utf8_lossy(&out.stdout).lines().map(|l| l.trim().to_owned()).filter(|l| !l.is_empty()).collect()
}

/// How many of the ants named `m-…` are in the colony.
fn served(ants: &impl Fn(&[&str]) -> Command) -> usize {
    let colony = ants(&["colony", "--json"]).output().unwrap();
    String::from_utf8_lossy(&colony.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|entry| entry["ant"].as_str().is_some_and(|ant| ant.starts_with("m-")))
        .count()
}

/// How many replies, by any ant, carry each token.
fn answers(ants: &impl Fn(&[&str]) -> Command) -> HashMap<String, usize> {
    let mut search = ants(&["thread", "search", "--since", "1h", "--limit", "100000", "--json"]);
    count_tokens(&mut search, None, |text| {
        text.get(..10).filter(|token| token.starts_with("tok-m") && token[5..].bytes().all(|b| b.is_ascii_digit()))
    })
}
