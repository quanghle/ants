//! A leaf node whose uplink to the hub flaps under traffic: messages sent from the hub and from the leaf to an ant
//! served behind the leaf are each answered exactly once, none a dead letter, once the uplink stays up.
//!
//! By default a few seconds of flapping. `ANTS_STRESS=N` flaps N times as long with prompts of 200 ms, and cuts the
//! uplink once for longer than the ack wait (60 s), so a stress run takes minutes.

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ants_daemon::sync::MutexExt as _;
use ants_test_support::{NatsServer, ants_command, free_port, scratch_dir, wait_until};

use super::{ANTS, Daemon, Killed, count_tokens, link_self, run, wait_answered};

#[expect(clippy::too_many_lines, reason = "one scenario: the hub, the leaf, the senders, the flaps, then the answers")]
pub fn a_flapping_uplink_loses_no_message() {
    let stress: u64 = std::env::var("ANTS_STRESS").ok().and_then(|n| n.parse().ok()).unwrap_or(0);
    let dir = scratch_dir("leaf-flap");
    let team = dir.join("team");
    let users = Users(dir.clone());
    let base = |args: &[&str]| users.base(args);
    let ok = |command: Command| {
        let out = run(command);
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        out
    };
    let hub_leafnodes = free_port();
    let Some(hub) = NatsServer::spawn_with(|hub_dir| {
        ok(base(&["auth", "init"]));
        std::fs::copy(team.join("server.conf"), hub_dir.join("server.conf")).unwrap();
        format!(
            "jetstream {{ store_dir: {:?}, domain: hub }}\ninclude \"server.conf\"\n\
             leafnodes {{ listen: \"127.0.0.1:{hub_leafnodes}\", no_advertise: true }}\n\
             feature_flags {{ js_api_reply_v2: true }}\n",
            hub_dir.join("js")
        )
    }) else {
        return;
    };
    let path = |path: &PathBuf| path.to_str().unwrap().to_owned();
    for (who, human) in [("alice", true), ("bob", true), ("dev-a", false)] {
        let out = path(&dir.join(format!("{who}.creds")));
        ok(base(
            &["auth", "user", who, "--out", &out, if human { "--human" } else { "--ant" }][..if human { 6 } else { 5 }],
        ));
    }
    let as_user = |who: &str, url: &str, args: &[&str]| users.user(who, url, args);
    ok(as_user("alice", &hub.url, &["init"]));
    let leaf_creds = path(&team.join("leaf-m1.creds"));
    ok(base(&["auth", "leaf", "m1", "--ant", "dev-a", "--human", "bob", "--out", &leaf_creds]));

    let proxy = Proxy::start(hub_leafnodes);
    let leaf_port = free_port();
    std::fs::create_dir_all(dir.join("leaf")).unwrap();
    let leaf_conf = path(&dir.join("leaf").join("leaf.conf"));
    let uplink = format!("nats-leaf://127.0.0.1:{}", proxy.port);
    let listen = format!("127.0.0.1:{leaf_port}");
    ok(base(&["leaf", "config", "m1", "--uplink", &uplink, "--listen", &listen, "--out", &leaf_conf]));
    let mut leaf = Command::new("nats-server");
    leaf.args(["-c", &leaf_conf]).stdout(Stdio::null()).stderr(Stdio::null());
    let _leaf = Killed(leaf.spawn().unwrap());
    let leaf_url = format!("nats://{listen}");
    // The leaf node connects to the hub in the background.
    wait_until(Duration::from_secs(30), Duration::from_millis(500), || {
        let out = run(as_user("alice", &hub.url, &["leaf", "outbox", "m1", "--leaf-config", &leaf_conf]));
        if out.status.success() {
            return Ok(());
        }
        Err(format!("ants leaf outbox: {}", String::from_utf8_lossy(&out.stderr)))
    });

    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    link_self(&bin, "copilot");
    let home = dir.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let mut daemon =
        as_user("dev-a", &leaf_url, &["daemon", "--harness", "copilot", "--human", "human", "--provider", "none"]);
    daemon
        .arg("--home")
        .arg(&home)
        .arg("--workdir")
        .arg(&home)
        .env("ANTS_ANT", "dev-a")
        .env("PATH", &bin)
        .env("ANTS_TEST_PROMPT_DELAY_MS", if stress > 0 { "200" } else { "20" });
    let daemon = Daemon::spawn(&mut daemon, dir.join("daemon.log"));
    let logs = || super::flood::tail(&daemon.log(), 60);
    for (who, url) in [("alice", &hub.url), ("bob", &leaf_url)] {
        let warm = run(as_user(who, url, &["ask", "--wait", "--timeout", "50s", "@dev-a", "warmup"]));
        assert!(warm.status.success(), "{who}'s warmup: {}\n{}", String::from_utf8_lossy(&warm.stderr), logs());
    }

    // alice sends from the hub, bob from the leaf, through every flap.
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(Mutex::new(Vec::new()));
    let senders: Vec<_> = [("a", "alice", hub.url.clone()), ("b", "bob", leaf_url)]
        .into_iter()
        .map(|(tag, who, url)| {
            let (stop, sent, users) = (stop.clone(), sent.clone(), users.clone());
            std::thread::spawn(move || {
                let mut n = 0;
                while !stop.load(Ordering::Relaxed) {
                    let token = format!("tok-{tag}x{n:05}");
                    let out = run(users.user(who, &url, &["ask", "@dev-a", &token]));
                    assert!(out.status.success(), "{who}'s ants ask: {}", String::from_utf8_lossy(&out.stderr));
                    sent.locked().push(token);
                    n += 1;
                    std::thread::sleep(Duration::from_millis(150));
                }
            })
        })
        .collect();

    let until = Instant::now() + Duration::from_secs(10 + 100 * stress);
    let mut long = (stress > 0).then_some(Instant::now() + Duration::from_secs(50 * stress));
    let mut flap = 0u64;
    while Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1500 + 500 * (flap % 4)));
        proxy.cut();
        let down = match long {
            Some(at) if Instant::now() > at => {
                long = None;
                Duration::from_secs(75)
            }
            _ => Duration::from_millis(500 + 700 * (flap % 3)),
        };
        std::thread::sleep(down);
        proxy.restore();
        flap += 1;
        assert!(daemon.running(), "the daemon exited\n{}", logs());
    }
    stop.store(true, Ordering::Relaxed);
    for sender in senders {
        sender.join().unwrap();
    }

    let sent = sent.locked().clone();
    let limit = Duration::from_secs(180 + 600 * stress);
    let answers = || answers(&users, &hub.url);
    let tokens: Vec<&str> = sent.iter().map(String::as_str).collect();
    wait_answered(limit, Duration::from_secs(2), &tokens, answers, logs);
    std::thread::sleep(Duration::from_secs(2));
    let twice: Vec<_> = answers().into_iter().filter(|(_, n)| *n > 1).collect();
    assert!(twice.is_empty(), "answered more than once: {twice:?}\n{}", logs());
    let dead = as_user("alice", &hub.url, &["thread", "search", "--since", "1h", "--type", "dead", "--json"])
        .output()
        .unwrap();
    let dead = String::from_utf8_lossy(&dead.stdout);
    assert!(dead.trim().is_empty(), "dead letters after {flap} flaps:\n{dead}\n{}", logs());
    assert!(proxy.cuts.load(Ordering::Relaxed) > 0, "the uplink never flapped");
    assert!(daemon.running(), "the daemon exited\n{}", logs());
}

/// The `ants` commands of a team whose auth directory and users' credentials are in a directory.
#[derive(Clone)]
struct Users(PathBuf);

impl Users {
    /// `ants` with the team's auth directory, for the hub's domain.
    fn base(&self, args: &[&str]) -> Command {
        let mut command = ants_command(ANTS);
        command.args(args).env("ANTS_AUTH_DIR", self.0.join("team")).env("ANTS_JS_DOMAIN", "hub");
        command
    }

    /// `ants` as `who`, through the server at `url`.
    fn user(&self, who: &str, url: &str, args: &[&str]) -> Command {
        let mut command = self.base(args);
        command.env("ANTS_NATS_URL", url).env("ANTS_CREDS", self.0.join(format!("{who}.creds")));
        command
    }
}

/// How many of dev-a's replies carry each token, as alice finds them on the hub.
fn answers(users: &Users, hub: &str) -> HashMap<String, usize> {
    let search = ["thread", "search", "--since", "1h", "--limit", "100000", "--json"];
    count_tokens(&mut users.user("alice", hub, &search), Some("dev-a"), |text| {
        let token = text.get(..11)?;
        let rest = token.as_bytes();
        (matches!(rest[4], b'a' | b'b') && rest[5] == b'x' && rest[6..].iter().all(u8::is_ascii_digit)).then_some(token)
    })
}

/// A TCP proxy in front of the hub's leafnode port: [`Proxy::cut`] drops every connection through it and refuses new
/// ones until [`Proxy::restore`].
struct Proxy {
    port: u16,
    up: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<TcpStream>>>,
    cuts: AtomicU64,
}

impl Proxy {
    fn start(target: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let up = Arc::new(AtomicBool::new(true));
        let conns = Arc::new(Mutex::new(Vec::new()));
        let (accepting, held) = (up.clone(), conns.clone());
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if !accepting.load(Ordering::Relaxed) {
                    continue;
                }
                let Ok(server) = TcpStream::connect(("127.0.0.1", target)) else { continue };
                held.locked().extend([client.try_clone().unwrap(), server.try_clone().unwrap()]);
                pump(client.try_clone().unwrap(), server.try_clone().unwrap());
                pump(server, client);
            }
        });
        Self { port, up, conns, cuts: AtomicU64::new(0) }
    }

    fn cut(&self) {
        self.up.store(false, Ordering::Relaxed);
        self.cuts.fetch_add(1, Ordering::Relaxed);
        for conn in self.conns.locked().drain(..) {
            let _ = conn.shutdown(Shutdown::Both);
        }
    }

    fn restore(&self) {
        self.up.store(true, Ordering::Relaxed);
    }
}

/// Copy `from` to `to` until either closes, then close both.
fn pump(mut from: TcpStream, mut to: TcpStream) {
    std::thread::spawn(move || {
        let mut buf = [0; 16 * 1024];
        while let Ok(n) = from.read(&mut buf) {
            if n == 0 || to.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        let _ = from.shutdown(Shutdown::Both);
        let _ = to.shutdown(Shutdown::Both);
    });
}
