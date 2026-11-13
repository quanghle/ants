//! Test helpers shared by the ants crates (dev-dependency only): external tools and a throwaway nats-server.
//!
//! A test that needs `nats-server` or `bd` skips when the tool is not on PATH, unless [`REQUIRE_TOOLS`] is set to `1`
//! (CI sets it), where it fails instead, so a broken install cannot pass as skipped tests.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Environment variable that turns a missing tool into a test failure.
pub const REQUIRE_TOOLS: &str = "ANTS_REQUIRE_TOOLS";

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

    /// A nats-server whose whole configuration, JetStream included, comes from `conf`, which gets the server's
    /// directory; only the listen address and ports file are added. None when nats-server is not installed.
    pub fn spawn_with(conf: impl FnOnce(&Path) -> String) -> Option<Self> {
        if !have("nats-server") {
            return None;
        }
        let dir = scratch_dir("nats");
        let path = dir.join("nats.conf");
        // The server picks its own port and reports it in the ports file, so parallel tests cannot collide.
        let config = format!("listen: 127.0.0.1:-1\nports_file_dir: {dir:?}\n{}", conf(&dir));
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
