//! Test helpers shared by the ants crates (dev-dependency only): external tools and a throwaway nats-server.
//!
//! A test that needs `nats-server` skips when the tool is not on PATH, unless [`REQUIRE_TOOLS`] is set to `1`
//! (CI sets it), where it fails instead, so a broken install cannot pass as skipped tests. Under [`JS_ACK_FC_V2`]`=1`
//! every test nats-server uses the JetStream ack and flow control subjects nats-server 2.16 uses by default.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// A fresh directory under `target/`, unique to this process and call. The caller removes it.
pub fn scratch_dir(name: &str) -> PathBuf {
    static DIRS: AtomicUsize = AtomicUsize::new(0);
    let n = DIRS.fetch_add(1, Ordering::Relaxed);
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/test-tmp")
        .join(format!("{name}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A TCP port of 127.0.0.1 that was free a moment ago, for a server that must start after its client.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
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
        let child = Command::new("nats-server")
            .arg("-c")
            .arg(&path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let ports = dir.join(format!("nats-server_{}.ports", child.id()));
        let mut server = Self { child, dir, url: String::new() };
        for _ in 0..100 {
            let url = std::fs::read_to_string(&ports).ok().and_then(|json| {
                let ports: serde_json::Value = serde_json::from_str(&json).ok()?;
                Some(ports["nats"][0].as_str()?.to_owned())
            });
            if let Some(url) = url {
                server.url = url;
                return Some(server);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("nats-server did not start");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ack_flag_joins_the_feature_flags_block_a_config_has() {
        let config = with_ack_fc_v2("jetstream {}\nfeature_flags { js_api_reply_v2: true }\n");
        assert_eq!(config.matches("feature_flags").count(), 1, "{config}");
        assert!(config.contains("js_ack_fc_v2: true") && config.contains("js_api_reply_v2: true"), "{config}");
        assert!(with_ack_fc_v2("jetstream {}\n").ends_with("feature_flags { js_ack_fc_v2: true }\n"));
    }
}
