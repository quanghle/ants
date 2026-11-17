//! Test helpers shared by the ants crates (dev-dependency only): external tools, a throwaway nats-server and a scripted
//! stub CLI ([`StubCli`]).
//!
//! A test that needs `nats-server` skips when the tool is not on PATH, unless [`REQUIRE_TOOLS`] is set to `1`
//! (CI sets it), where it fails instead, so a broken install cannot pass as skipped tests. Under [`JS_ACK_FC_V2`]`=1`
//! every test nats-server uses the JetStream ack and flow control subjects nats-server 2.16 uses by default.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

/// Environment variable that turns a missing tool into a test failure.
pub const REQUIRE_TOOLS: &str = "ANTS_REQUIRE_TOOLS";

/// Environment variable that, set to `1`, turns on nats-server's feature flag `js_ack_fc_v2` in every test server:
/// acks and flow control then go to the v2 subjects `$JS.ACK.<domain>.<account hash>.…` and `$JS.FC.<domain>.…`, the
/// default from nats-server 2.16.
pub const JS_ACK_FC_V2: &str = "ANTS_TEST_JS_ACK_FC_V2";

/// Whether `tool` runs. When it does not, the caller skips its test, or this panics under [`REQUIRE_TOOLS`]`=1`.
pub fn have(tool: &str) -> bool {
    if Command::new(tool).arg("--version").output().is_ok() {
        return true;
    }
    assert!(std::env::var(REQUIRE_TOOLS).as_deref() != Ok("1"), "{tool} is not on PATH and {REQUIRE_TOOLS}=1");
    eprintln!("{tool} not on PATH; skipping");
    false
}

/// The directory under `target/` holding the files of tests.
fn test_tmp() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/test-tmp")
}

/// A fresh directory under `target/`, unique to this process and call. The caller removes it.
pub fn scratch_dir(name: &str) -> PathBuf {
    static DIRS: AtomicUsize = AtomicUsize::new(0);
    let n = DIRS.fetch_add(1, Ordering::Relaxed);
    let dir = test_tmp().join(format!("{name}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The ports [`free_port`] hands out: below the ephemeral ports of Linux (from 32768) and macOS (from 49152), which
/// port-0 binds and nats-servers listening on port -1 take.
const RESERVED_PORTS: std::ops::Range<u16> = 20_000..32_000;

/// A TCP port of 127.0.0.1 nothing listens on, for a server that must start after its client or for an address
/// where nothing answers. No other call, in this test process or another, gets it until this process exits: each
/// port handed out stays locked by a file under `target/test-tmp/ports`. Port-0 binds, taking ephemeral ports, miss it too.
pub fn free_port() -> u16 {
    static PORTS: Mutex<(u16, Vec<File>)> = Mutex::new((0, Vec::new()));
    let mut ports = PORTS.lock().unwrap_or_else(PoisonError::into_inner);
    let (next, held) = &mut *ports;
    let span = RESERVED_PORTS.end - RESERVED_PORTS.start;
    if *next == 0 {
        use std::hash::{BuildHasher as _, Hasher as _};
        // Random, away from the ports of other test processes and of a run just before, whose servers may still be
        // stopping.
        let random = std::hash::RandomState::new().build_hasher().finish();
        *next = RESERVED_PORTS.start + u16::try_from(random % u64::from(span)).unwrap();
    }
    let locks = test_tmp().join("ports");
    std::fs::create_dir_all(&locks).unwrap();
    for _ in 0..span {
        let port = *next;
        *next = if port + 1 == RESERVED_PORTS.end { RESERVED_PORTS.start } else { port + 1 };
        let lock = File::options().create(true).truncate(false).write(true).open(locks.join(port.to_string())).unwrap();
        // Held by another test process, or by this one through another handle.
        if lock.try_lock().is_err() {
            continue;
        }
        // A listener bound to a wildcard address does not keep another from binding 127.0.0.1 alone.
        let refused = std::net::TcpStream::connect(("127.0.0.1", port)).is_err();
        if refused && std::net::TcpListener::bind(("0.0.0.0", port)).is_ok() {
            held.push(lock);
            return port;
        }
    }
    panic!("no free port left in {RESERVED_PORTS:?}");
}

/// Whether [`JS_ACK_FC_V2`] asks for v2 ack and flow control subjects.
fn ack_fc_v2() -> bool {
    std::env::var(JS_ACK_FC_V2).as_deref() == Ok("1")
}

/// `config` with feature flag `js_ack_fc_v2` on, in its `feature_flags` block if it has one: a second block would
/// replace the first.
fn with_ack_fc_v2(config: &str) -> String {
    const BLOCK: &str = "feature_flags {";
    match config.find(BLOCK) {
        Some(at) => {
            let at = at + BLOCK.len();
            format!("{}\n  js_ack_fc_v2: true\n{}", &config[..at], &config[at..])
        }
        None => format!("{config}\nfeature_flags {{ js_ack_fc_v2: true }}\n"),
    }
}

/// A nats-server with JetStream on a port of its choosing, its files in a scratch directory; stopped and removed on
/// drop.
pub struct NatsServer {
    child: Child,
    dir: PathBuf,
    pub url: String,
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl NatsServer {
    /// None when nats-server is not installed (see [`have`]).
    pub fn start() -> Option<Self> {
        Self::start_with(|_| String::new())
    }

    /// Like [`start`](Self::start), with configuration lines from `extra`, which gets the server's directory to put
    /// included files in.
    pub fn start_with(extra: impl FnOnce(&Path) -> String) -> Option<Self> {
        Self::spawn_with(|dir| format!("jetstream {{ store_dir: {:?} }}\n{}", dir.join("js"), extra(dir)))
    }

    /// Like [`start`](Self::start), listening on `port` of 127.0.0.1 (see [`free_port`]), for a client started first.
    pub fn start_on(port: u16) -> Option<Self> {
        Self::spawn(&format!("127.0.0.1:{port}"), |dir| format!("jetstream {{ store_dir: {:?} }}\n", dir.join("js")))
    }

    /// A nats-server whose whole configuration, JetStream included, comes from `conf`, which gets the server's
    /// directory; only the listen address and ports file are added. None when nats-server is not installed.
    pub fn spawn_with(conf: impl FnOnce(&Path) -> String) -> Option<Self> {
        // The server picks its own port and reports it in the ports file, so parallel tests cannot collide.
        Self::spawn("127.0.0.1:-1", conf)
    }

    fn spawn(listen: &str, conf: impl FnOnce(&Path) -> String) -> Option<Self> {
        if !have("nats-server") {
            return None;
        }
        let dir = scratch_dir("nats");
        let path = dir.join("nats.conf");
        let mut config = format!("listen: {listen}\nports_file_dir: {dir:?}\n{}", conf(&dir));
        if ack_fc_v2() {
            config = with_ack_fc_v2(&config);
        }
        std::fs::write(&path, config).unwrap();
        let (child, url) = launch(&dir);
        Some(Self { child, dir, url })
    }

    /// Stop the server as a container's stop does (SIGTERM), letting it shut down gracefully, keeping its files for
    /// [`start_again`](Self::start_again).
    pub fn stop(&mut self) {
        let status = Command::new("kill").arg("-TERM").arg(self.child.id().to_string()).status().unwrap();
        assert!(status.success());
        self.child.wait().unwrap();
    }

    /// Start the server [`stop`](Self::stop)ped again on the same configuration and files. A server that picked its
    /// own port may get another one: [`url`](Self::url) follows.
    pub fn start_again(&mut self) {
        (self.child, self.url) = launch(&self.dir);
    }

    /// The server's directory, removed with it.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The server's process id, to signal it (SIGHUP reloads its configuration).
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
}

/// Start nats-server on the configuration in `dir`; the child and its client URL, from its ports file.
fn launch(dir: &Path) -> (Child, String) {
    let mut child = Command::new("nats-server")
        .arg("-c")
        .arg(dir.join("nats.conf"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let ports = dir.join(format!("nats-server_{}.ports", child.id()));
    for _ in 0..100 {
        let url = std::fs::read_to_string(&ports).ok().and_then(|json| {
            let ports: serde_json::Value = serde_json::from_str(&json).ok()?;
            Some(ports["nats"][0].as_str()?.to_owned())
        });
        if let Some(url) = url {
            return (child, url);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("nats-server did not start");
}

/// A scripted stub of a CLI speaking JSON lines on stdio, such as a native harness protocol: a POSIX shell script that
/// plays blocks of lines from a script file and records what it gets.
///
/// Each start of the stub is a run, numbered from 1. A run plays the script `<stub>.script.<run>` if there is one, else
/// `<stub>.script`: block 0 when it starts, then block N after reading its Nth line of input, until its input ends. A
/// line of a block is printed to stdout as it is, except `!stderr TEXT`, printed to stderr, `!echo TEXT`, which prints
/// the line just read with its first character replaced by TEXT, `!sleep SECONDS`, and `!exit CODE`, which ends the
/// run. Blocks are separated by `---` lines. The run records its input lines, its
/// arguments, its environment and its working directory beside the stub ([`StubCli::received`], [`StubCli::args`],
/// [`StubCli::env`], [`StubCli::cwd`]), and the end of its input ([`StubCli::ended`]).
#[cfg(unix)]
pub struct StubCli {
    path: PathBuf,
}

#[cfg(unix)]
const STUB_CLI: &str = r#"#!/bin/sh
stub=$0
run=$(( $(cat "$stub.runs" 2>/dev/null || echo 0) + 1 ))
echo "$run" > "$stub.runs"
script="$stub.script.$run"
[ -f "$script" ] || script="$stub.script"
log="$stub.log.$run"
: > "$log"
for arg in "$@"; do printf '%s\n' "$arg"; done > "$stub.args.$run"
env > "$stub.env.$run"
pwd > "$stub.cwd.$run"
play() {
    awk -v want="$1" '/^---$/ { n++; next } n == want' "$script" > "$stub.block.$run"
    while IFS= read -r line; do
        case "$line" in
        '!exit '*) exit "${line#!exit }" ;;
        '!stderr '*) printf '%s\n' "${line#!stderr }" >&2 ;;
        '!echo '*) printf '%s%s\n' "${line#!echo }" "${input#?}" ;;
        '!sleep '*) sleep "${line#!sleep }" ;;
        *) printf '%s\n' "$line" ;;
        esac
    done < "$stub.block.$run"
}
n=0
play 0
while IFS= read -r input; do
    printf '%s\n' "$input" >> "$log"
    n=$((n + 1))
    play "$n"
done
: > "$stub.ended.$run"
"#;

#[cfg(unix)]
impl StubCli {
    /// A stub named `name` in `dir`, with an empty script.
    pub fn new(dir: &Path, name: &str) -> Self {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join(name);
        std::fs::write(&path, STUB_CLI).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let stub = Self { path };
        std::fs::write(stub.file("script"), "").unwrap();
        stub
    }

    /// Set the blocks run `run` plays, or every run without a script of its own when None.
    pub fn script<L: AsRef<str>>(&self, run: Option<usize>, blocks: &[impl AsRef<[L]>]) {
        let text: Vec<String> = blocks
            .iter()
            .map(|block| block.as_ref().iter().map(|line| format!("{}\n", line.as_ref())).collect())
            .collect();
        let file = match run {
            Some(run) => format!("script.{run}"),
            None => "script".to_owned(),
        };
        std::fs::write(self.file(&file), text.join("---\n")).unwrap();
    }

    /// The stub's executable.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How many times it was started.
    pub fn runs(&self) -> usize {
        std::fs::read_to_string(self.file("runs")).map_or(0, |n| n.trim().parse().unwrap())
    }

    /// The lines of input run `run` read.
    pub fn received(&self, run: usize) -> Vec<String> {
        self.lines(&format!("log.{run}"))
    }

    /// The arguments run `run` was started with.
    pub fn args(&self, run: usize) -> Vec<String> {
        self.lines(&format!("args.{run}"))
    }

    /// The value of the environment variable `name` in run `run`.
    pub fn env(&self, run: usize, name: &str) -> Option<String> {
        let prefix = format!("{name}=");
        self.lines(&format!("env.{run}")).into_iter().find_map(|line| line.strip_prefix(&prefix).map(str::to_owned))
    }

    /// Whether the input of run `run` ended, as it does when the process starting the stub closes its stdin.
    pub fn ended(&self, run: usize) -> bool {
        self.file(&format!("ended.{run}")).exists()
    }

    /// The working directory of run `run`.
    pub fn cwd(&self, run: usize) -> PathBuf {
        self.lines(&format!("cwd.{run}")).concat().into()
    }

    fn file(&self, suffix: &str) -> PathBuf {
        let mut name = self.path.as_os_str().to_owned();
        name.push(format!(".{suffix}"));
        name.into()
    }

    fn lines(&self, suffix: &str) -> Vec<String> {
        std::fs::read_to_string(self.file(suffix)).unwrap_or_default().lines().map(str::to_owned).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn a_stub_cli_plays_a_block_per_line_read() {
        use std::io::Write as _;
        let dir = scratch_dir("stub-cli");
        let stub = StubCli::new(&dir, "cli");
        stub.script(None, &[&["ready"][..], &["!stderr oops", "one"], &["!exit 3"]]);
        stub.script(Some(2), &[["second"]]);
        let run = |input: &str| {
            let mut child = Command::new(stub.path())
                .arg("--flag")
                .env("STUB_VAR", "set")
                .current_dir(&dir)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
            child.wait_with_output().unwrap()
        };
        let first = run("a\nb\nc\n");
        assert_eq!(String::from_utf8_lossy(&first.stdout), "ready\none\n");
        assert_eq!(String::from_utf8_lossy(&first.stderr), "oops\n");
        assert_eq!(first.status.code(), Some(3), "ended by the third block");
        assert_eq!(stub.received(1), ["a", "b"]);
        assert_eq!(String::from_utf8_lossy(&run("x\n").stdout), "second\n", "its own script");
        assert_eq!(stub.runs(), 2);
        assert_eq!(stub.args(1), ["--flag"]);
        assert_eq!(stub.env(1, "STUB_VAR").as_deref(), Some("set"));
        assert_eq!(stub.cwd(1).canonicalize().unwrap(), dir.canonicalize().unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_free_port_is_handed_out_once_and_locked_against_other_processes() {
        let (a, b) = (free_port(), free_port());
        assert_ne!(a, b);
        assert!(RESERVED_PORTS.contains(&a) && RESERVED_PORTS.contains(&b), "{a} {b}");
        let lock = File::open(test_tmp().join("ports").join(a.to_string())).unwrap();
        assert!(lock.try_lock().is_err(), "another process cannot take port {a}");
    }

    #[test]
    fn the_ack_flag_joins_the_feature_flags_block_a_config_has() {
        let config = with_ack_fc_v2("jetstream {}\nfeature_flags { js_api_reply_v2: true }\n");
        assert_eq!(config.matches("feature_flags").count(), 1, "{config}");
        assert!(config.contains("js_ack_fc_v2: true") && config.contains("js_api_reply_v2: true"), "{config}");
        assert!(with_ack_fc_v2("jetstream {}\n").ends_with("feature_flags { js_ack_fc_v2: true }\n"));
    }
}
