//! The managed nats-server's process: its owner lock, start, readiness, supervision and stop.

use std::fmt::Write as _;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use ants_daemon::config::NatsServer;
use anyhow::{Context as _, Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep, sleep_until, timeout};

use super::Policy;
use crate::service::{Action, Outcome};

/// Where the daemon reaches its managed nats-server, from its NATS URL `url` (the first, of several): the host and port
/// of a `nats://` or `tls://` URL, or one without a scheme, which must be this machine.
pub(super) fn address(url: &str) -> Result<(String, u16)> {
    let first = url.split(',').next().unwrap_or_default().trim();
    let rest = match first.split_once("://") {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("nats") || scheme.eq_ignore_ascii_case("tls") => rest,
        Some((scheme, _)) => bail!(
            "a managed nats-server needs the daemon to connect to it with nats:// or tls://, not {scheme}:// ({first})"
        ),
        None => first,
    };
    let rest = rest.split(['/', '?']).next().unwrap_or_default();
    let rest = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    let (host, port) = match rest.strip_prefix('[') {
        Some(v6) => match v6.split_once(']') {
            Some((host, "")) => (host, None),
            Some((host, port)) => (host, Some(port.strip_prefix(':').unwrap_or(port))),
            None => bail!("invalid NATS URL {first}"),
        },
        None => match rest.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (rest, None),
        },
    };
    let port = match port {
        Some(port) => port.parse().map_err(|_| anyhow::anyhow!("invalid port in the NATS URL {first}"))?,
        None => 4222,
    };
    let local =
        host.eq_ignore_ascii_case("localhost") || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
    if !local {
        bail!(
            "a managed nats-server needs the daemon to connect to it locally: {first} is not on this machine \
             (localhost, 127.0.0.1 or ::1); set the NATS URL (--nats, ANTS_NATS_URL) to the server's local address"
        );
    }
    Ok((host.to_owned(), port))
}

/// `host:port`, with brackets around an IPv6 host.
pub(super) fn host_port(host: &str, port: u16) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

/// The owner lock of the nats-server configuration `conf`: the file beside it, `<conf>.lock`, of its real path where
/// it can be found.
pub(super) fn lock_path(conf: &Path) -> PathBuf {
    let mut path = std::fs::canonicalize(conf).unwrap_or_else(|_| conf.to_owned()).into_os_string();
    path.push(".lock");
    path.into()
}

/// The daemon and server process ids a lock file records.
pub(super) fn recorded(text: &str) -> (Option<u32>, Option<u32>) {
    let pid = |key: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
            .and_then(|pid| pid.trim().parse::<u32>().ok())
            .filter(|&pid| pid > 0 && i32::try_from(pid).is_ok())
    };
    (pid("daemon"), pid("server"))
}

/// How long one readiness check waits for the server's INFO line.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// The nats-server a daemon started from `[nats_server]`: owned through its configuration's lock until
/// [`Managed::stop`]ped.
pub struct Managed {
    settings: NatsServer,
    host: String,
    port: u16,
    policy: Policy,
    lock: Lock,
    /// The server process, until it exits or is stopped.
    child: Option<Child>,
    /// When the server last started.
    since: Instant,
    /// The restarts in a row since the last run of [`Policy::healthy`].
    restarts: u32,
}

/// Start the nats-server `settings` describe, for a daemon connecting to it at `nats_url`, and wait until it accepts
/// connections. The configuration's lock is taken first, so a second daemon cannot start a second server on it; a
/// server a previous daemon killed with SIGKILL left running is stopped; and none starts while a server already
/// listens at the address. None when `stop` came first, the server stopped again.
pub async fn start(
    settings: &NatsServer,
    nats_url: &str,
    policy: Policy,
    stop: impl Future<Output = ()>,
) -> Result<Option<Managed>> {
    let (host, port) = address(nats_url)?;
    let (lock, left) = Lock::take(&settings.config)?;
    if let Some(pid) = left {
        stop_orphan(pid, &settings.config, policy.orphan_grace).await;
    }
    if listens(&host, port).await {
        bail!(
            "a server already listens on {}; [nats_server] starts none while another runs: stop it (an ants.nats / \
             ants-nats service?) or remove [nats_server]",
            host_port(&host, port)
        );
    }
    let mut managed = Managed {
        settings: settings.clone(),
        host,
        port,
        policy,
        lock,
        child: None,
        since: Instant::now(),
        restarts: 0,
    };
    match managed.launch(stop).await {
        Ok(true) => Ok(Some(managed)),
        Ok(false) => {
            managed.stop().await;
            Ok(None)
        }
        Err(e) => {
            managed.stop().await;
            Err(e)
        }
    }
}

/// Run `serving` while supervising `managed` ([`Managed::supervise`]), then stop the server: once `serving` is done,
/// whatever its outcome, or at once when supervision gives up, `serving` dropped and the error returned.
pub async fn run_serving<T>(mut managed: Managed, serving: impl Future<Output = Result<T>>) -> Result<T> {
    let outcome = tokio::select! {
        outcome = serving => outcome,
        e = managed.supervise() => Err(e),
    };
    managed.stop().await;
    outcome
}

impl Managed {
    /// The server's process id, while it runs.
    #[cfg(test)]
    pub(super) fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    /// Start the server again each time it exits, after [`Policy::backoff`], until it exited
    /// [`Policy::max_restarts`] times in a row without running [`Policy::healthy`]: the error then.
    pub async fn supervise(&mut self) -> anyhow::Error {
        loop {
            if let Some(child) = self.child.as_mut() {
                let pid = child.id();
                let status = child.wait().await;
                self.child = None;
                self.lock.record(None);
                match status {
                    Ok(status) => tracing::warn!(pid, %status, "nats-server exited"),
                    Err(e) => tracing::warn!(pid, error = %e, "waiting for nats-server"),
                }
                if self.since.elapsed() >= self.policy.healthy {
                    self.restarts = 0;
                }
            }
            if self.restarts >= self.policy.max_restarts {
                let e = anyhow!(
                    "{} kept exiting: {} restarts in a row, none running {:?}; gave up",
                    self.command(),
                    self.restarts,
                    self.policy.healthy
                );
                tracing::error!(error = format!("{e:#}"), "giving up on nats-server");
                return e;
            }
            self.restarts += 1;
            let backoff = self.policy.backoff(self.restarts);
            sleep(backoff).await;
            tracing::info!(attempt = self.restarts, after = ?backoff, "restarting nats-server");
            if let Err(e) = self.launch(std::future::pending()).await {
                tracing::warn!(error = format!("{e:#}"), "nats-server did not start again");
            }
        }
    }

    /// Stop the server, if it runs, and release the configuration's lock.
    pub async fn stop(mut self) {
        self.halt().await;
    }

    /// `nats-server -c CONF`, as run.
    fn command(&self) -> String {
        format!("{} -c {}", self.settings.command.display(), self.settings.config.display())
    }

    /// Start the server and wait until it is ready: false when `stop` came first.
    async fn launch(&mut self, stop: impl Future<Output = ()>) -> Result<bool> {
        let child = spawn(&self.settings)?;
        let pid = child.id();
        tracing::info!(pid, config = %self.settings.config.display(), "nats-server started");
        self.since = Instant::now();
        self.lock.record(pid);
        let child = self.child.insert(child);
        let (limit, every) = (self.settings.ready_timeout, self.policy.probe_every);
        match ready(child, &self.host, self.port, limit, every, stop).await {
            Readiness::Ready => {
                tracing::info!(pid, address = host_port(&self.host, self.port), "nats-server ready");
                Ok(true)
            }
            Readiness::Stopped => Ok(false),
            Readiness::Exited(status) => {
                self.child = None;
                self.lock.record(None);
                let status = status.map_or_else(|e| format!("waiting for it: {e}"), |s| s.to_string());
                bail!(
                    "{} exited before accepting connections ({status}): its log, on the daemon's standard error \
                     unless its configuration sets log_file, says why",
                    self.command()
                )
            }
            Readiness::TimedOut => {
                self.halt().await;
                bail!(
                    "{} did not accept connections on {} within {limit:?} ([nats_server] ready_timeout); stopped it",
                    self.command(),
                    host_port(&self.host, self.port),
                )
            }
        }
    }

    /// Stop the server, if it runs: SIGTERM, then SIGKILL after [`Policy::stop_grace`]; and reap it.
    async fn halt(&mut self) {
        let Some(child) = self.child.as_mut() else { return };
        let pid = child.id();
        tracing::info!(pid, "stopping nats-server");
        if let Some(pid) = pid {
            let _ = signal(pid, libc::SIGTERM);
        }
        let status = match timeout(self.policy.stop_grace, child.wait()).await {
            Ok(status) => status,
            Err(_) => {
                tracing::warn!(pid, grace = ?self.policy.stop_grace, "nats-server did not stop in time; killing it");
                let _ = child.start_kill();
                child.wait().await
            }
        };
        match status {
            Ok(status) => tracing::info!(pid, %status, "nats-server stopped"),
            Err(e) => tracing::warn!(pid, error = %e, "waiting for nats-server to stop"),
        }
        self.child = None;
        self.lock.record(None);
    }
}

/// `nats-server -c CONF`, in a process group of its own so Ctrl-C at a terminal reaches the daemon alone, which stops
/// it once the ants are drained.
fn spawn(settings: &NatsServer) -> Result<Child> {
    let mut command = Command::new(super::bundled::resolve(&settings.command));
    command.arg("-c").arg(&settings.config).stdin(Stdio::null()).process_group(0).kill_on_drop(true);
    #[cfg(target_os = "linux")]
    {
        let parent = std::process::id();
        // SAFETY: the closure runs in the forked child before exec and calls only async-signal-safe functions
        // (prctl, getppid), allocating nothing on success.
        unsafe {
            command.pre_exec(move || {
                // The server gets SIGTERM when the daemon dies, even by SIGKILL; unless the daemon died already.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM as libc::c_ulong) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() as u32 != parent {
                    return Err(std::io::Error::other("the daemon exited"));
                }
                Ok(())
            });
        }
    }
    command.spawn().map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound if settings.command == Path::new(super::bundled::DEFAULT) => anyhow!(
            "nats-server not found {}: `ants setup` downloads it, or set [nats_server] command to its path",
            super::bundled::LOOKED_UP
        ),
        std::io::ErrorKind::NotFound => anyhow!(
            "{} not found: install nats-server, or set [nats_server] command to its path",
            settings.command.display()
        ),
        _ => anyhow!(e).context(format!("starting {} -c {}", settings.command.display(), settings.config.display())),
    })
}

enum Readiness {
    Ready,
    Stopped,
    Exited(std::io::Result<ExitStatus>),
    TimedOut,
}

/// Wait for the server `child` to accept NATS connections at `host`:`port`, checking every `every` until `limit`: or
/// for it to exit, or for `stop`.
async fn ready(
    child: &mut Child,
    host: &str,
    port: u16,
    limit: Duration,
    every: Duration,
    stop: impl Future<Output = ()>,
) -> Readiness {
    let deadline = Instant::now() + limit;
    tokio::pin!(stop);
    loop {
        let check = async {
            let ready = probe(host, port).await;
            if !ready {
                sleep(every).await;
            }
            ready
        };
        tokio::select! {
            status = child.wait() => return Readiness::Exited(status),
            () = &mut stop => return Readiness::Stopped,
            () = sleep_until(deadline) => return Readiness::TimedOut,
            ready = check => if ready {
                return Readiness::Ready;
            },
        }
    }
}

/// Whether a NATS server answers at `host`:`port`: accepts a connection and sends its INFO line, as it does before any
/// authentication.
pub(super) async fn probe(host: &str, port: u16) -> bool {
    let check = async {
        let stream = TcpStream::connect((host, port)).await.ok()?;
        let mut line = String::new();
        BufReader::new(stream.take(64 * 1024)).read_line(&mut line).await.ok()?;
        Some(line.starts_with("INFO "))
    };
    timeout(PROBE_TIMEOUT, check).await.ok().flatten().unwrap_or(false)
}

/// Whether something accepts TCP connections at `host`:`port`.
async fn listens(host: &str, port: u16) -> bool {
    matches!(timeout(PROBE_TIMEOUT, TcpStream::connect((host, port))).await, Ok(Ok(_)))
}

/// The lock file of a nats-server configuration, held while a daemon owns that configuration's server; it records
/// the daemon's and the server's process ids.
struct Lock(File);

impl Lock {
    /// The lock of the configuration `conf`, and the server process id it recorded, of a previous owner.
    fn take(conf: &Path) -> Result<(Lock, Option<u32>)> {
        let path = lock_path(conf);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let owner = std::fs::read_to_string(&path).ok().and_then(|text| recorded(&text).0);
                let pid = owner.map_or_default(|pid| format!(" (pid {pid})"));
                bail!("another ants daemon{pid} runs the nats-server of {}", conf.display());
            }
            Err(TryLockError::Error(e)) => return Err(e).with_context(|| format!("locking {}", path.display())),
        }
        let mut text = String::new();
        (&file).read_to_string(&mut text).with_context(|| format!("reading {}", path.display()))?;
        let lock = Lock(file);
        lock.record(None);
        Ok((lock, recorded(&text).1))
    }

    /// Record this daemon and its server `server`, while it runs.
    fn record(&self, server: Option<u32>) {
        let mut text = format!("daemon={}\n", std::process::id());
        if let Some(pid) = server {
            let _ = writeln!(text, "server={pid}");
        }
        let mut file = &self.0;
        let written = file.set_len(0).and_then(|()| file.rewind()).and_then(|()| file.write_all(text.as_bytes()));
        if let Err(e) = written {
            tracing::warn!(error = %e, "recording the nats-server process in its lock file");
        }
    }
}

/// The nats-server process an `ants daemon` runs from the configuration `conf`: the server its lock records, when a
/// daemon holds that lock and the server is alive.
pub(super) fn server_of(conf: &Path) -> Option<u32> {
    let file = File::open(lock_path(conf)).ok()?;
    match file.try_lock() {
        Err(TryLockError::WouldBlock) => {}
        Ok(()) | Err(TryLockError::Error(_)) => return None,
    }
    let mut text = String::new();
    (&file).read_to_string(&mut text).ok()?;
    recorded(&text).1.filter(|&pid| alive(pid))
}

/// `action` on the managed nats-server `pid`.
pub(super) fn signal_server(pid: u32, action: Action) -> Outcome {
    let (number, name, done) = match action {
        Action::Reload => (libc::SIGHUP, "HUP", "the nats-server ants daemon runs"),
        Action::Restart => (libc::SIGTERM, "TERM", "the nats-server ants daemon runs, which starts it again"),
        Action::Start => return Outcome::Done(format!("none: ants daemon runs the nats-server, pid {pid}")),
    };
    let command = format!("kill -{name} {pid}");
    match signal(pid, number) {
        Ok(()) => Outcome::Done(format!("{command} ({done})")),
        Err(e) => Outcome::Failed(command, e.to_string()),
    }
}

/// Send the process `pid` the signal `number`; never a process group, `pid` being above 0.
pub(super) fn signal(pid: u32, number: libc::c_int) -> std::io::Result<()> {
    let pid = libc::pid_t::try_from(pid).ok().filter(|&pid| pid > 0).ok_or(std::io::ErrorKind::InvalidInput)?;
    // SAFETY: kill(2) takes plain integers; a positive pid names one process.
    if unsafe { libc::kill(pid, number) } == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

/// Whether the process `pid` exists.
pub(super) fn alive(pid: u32) -> bool {
    match signal(pid, 0) {
        Ok(()) => true,
        Err(e) => e.raw_os_error() == Some(libc::EPERM),
    }
}

/// Stop the server `pid` a previous daemon left running, killed with SIGKILL before it could stop it: when that
/// process runs the configuration `conf`, as its command line says. SIGTERM, then SIGKILL after `grace`.
async fn stop_orphan(pid: u32, conf: &Path, grace: Duration) {
    if pid == std::process::id() || !alive(pid) || !runs(pid, conf) {
        return;
    }
    tracing::warn!(pid, config = %conf.display(), "stopping the nats-server a previous ants daemon left running");
    let _ = signal(pid, libc::SIGTERM);
    if gone(pid, grace).await {
        return;
    }
    tracing::warn!(pid, ?grace, "the nats-server left running did not stop in time; killing it");
    let _ = signal(pid, libc::SIGKILL);
    gone(pid, Duration::from_secs(5)).await;
}

/// Whether the command line of the process `pid` names the file `conf`.
fn runs(pid: u32, conf: &Path) -> bool {
    let Some(conf) = conf.to_str() else { return false };
    std::process::Command::new("ps")
        .args(["-ww", "-o", "command=", "-p", &pid.to_string()])
        .stderr(Stdio::null())
        .output()
        .is_ok_and(|out| out.status.success() && String::from_utf8_lossy(&out.stdout).contains(conf))
}

/// Whether the process `pid` is gone within `limit`.
async fn gone(pid: u32, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while alive(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
    true
}
