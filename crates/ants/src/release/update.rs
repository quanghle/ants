//! `ants update`: replace the running ants executable with a release published on GitHub, the archive for this
//! target checked against the release's `SHA256SUMS`, then renamed over the executable at once. The services running
//! it move onto the new one through their upgrade check ([`crate::release::upgrade`]).

use std::cmp::Ordering;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::AutoUpdate;
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use crate::node::natsd::bundled;
use crate::service::{Action, Service, Services};

/// The repository publishing the releases, `owner/name`.
const REPO: &str = "quanghle/ants";
const API_URL: &str = "https://api.github.com";
/// Exit status of `ants update --check` when a newer release is out.
const AVAILABLE_EXIT_CODE: i32 = 10;
/// Longest answer of the GitHub API read.
const MAX_ANSWER: usize = 1024 * 1024;
/// Largest archive downloaded.
const MAX_ARCHIVE: usize = 256 * 1024 * 1024;
const API_TIMEOUT: Duration = Duration::from_secs(30);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_mins(10);
const SMOKE_TIMEOUT: Duration = Duration::from_secs(30);
const SUMS: &str = "SHA256SUMS";
/// How often a daemon with `auto_update` polls the releases, give or take [`POLL_JITTER`].
const POLL_EVERY: Duration = Duration::from_hours(24);
/// The spread of the polls around [`POLL_EVERY`], which keeps a team's machines from polling together.
const POLL_JITTER: Duration = Duration::from_hours(2);
/// Longest wait for the first poll after the daemon starts.
const FIRST_POLL: Duration = Duration::from_mins(10);
/// Wait after a first failed poll, doubled after each further one up to [`POLL_EVERY`].
const RETRY_AFTER: Duration = Duration::from_hours(1);
/// The services running the ants executable, which restart onto a new one.
const SERVICES: [Service; 4] = [Service::Daemon, Service::Webhook, Service::Bridge, Service::BridgeServe];

#[derive(clap::Args, Debug)]
pub(crate) struct UpdateArgs {
    /// Only tell whether a newer release is out: exit status 0 when up to date, 10 when one is.
    #[arg(long)]
    check: bool,
    /// Install this release (a tag such as v0.2.0) rather than the latest, even when not newer.
    #[arg(long, value_name = "TAG")]
    version: Option<String>,
    /// Consider pre-releases too.
    #[arg(long)]
    pre: bool,
}

/// A release's version: `vMAJOR.MINOR.PATCH[-PRE][+BUILD]`, ordered as semantic versions are (build ignored).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Version {
    core: [u64; 3],
    pre: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Ident {
    // Before `Alpha`: numeric identifiers sort below alphanumeric ones.
    Num(u64),
    Alpha(String),
}

impl Version {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let text = text.strip_prefix('v').unwrap_or(text);
        let text = text.split_once('+').map_or(text, |(version, _)| version);
        let (core, pre) = match text.split_once('-') {
            Some((core, pre)) => (core, Some(pre)),
            None => (text, None),
        };
        let mut numbers = core.split('.').map(|n| n.parse::<u64>().ok());
        let core = [numbers.next()??, numbers.next()??, numbers.next()??];
        if numbers.next().is_some() {
            return None;
        }
        let pre = match pre {
            Some(pre) => pre
                .split('.')
                .map(|id| match id.parse() {
                    _ if id.is_empty() => None,
                    Ok(n) => Some(Ident::Num(n)),
                    Err(_) => Some(Ident::Alpha(id.to_owned())),
                })
                .collect::<Option<_>>()?,
            None => Vec::new(),
        };
        Some(Self { core, pre })
    }

    /// The version of this executable.
    pub(crate) fn running() -> Self {
        Self::parse(env!("CARGO_PKG_VERSION")).expect("the package version is a semantic version")
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.core.cmp(&other.core).then_with(|| match (self.pre.is_empty(), other.pre.is_empty()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => self.pre.cmp(&other.pre),
        })
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [major, minor, patch] = self.core;
        write!(f, "v{major}.{minor}.{patch}")?;
        for (i, id) in self.pre.iter().enumerate() {
            f.write_str(if i == 0 { "-" } else { "." })?;
            match id {
                Ident::Num(n) => write!(f, "{n}")?,
                Ident::Alpha(s) => f.write_str(s)?,
            }
        }
        Ok(())
    }
}

/// A release as the GitHub API lists it.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Release {
    pub(crate) tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
    #[serde(default)]
    assets: Vec<Asset>,
}

#[derive(Debug, Clone, Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
}

impl Release {
    pub(crate) fn version(&self) -> Option<Version> {
        Version::parse(&self.tag_name)
    }
}

/// The release target this executable was built for, as the release archives name it; none for a target without
/// releases.
pub(crate) fn target() -> Option<&'static str> {
    if cfg!(all(target_arch = "x86_64", target_os = "linux", target_env = "gnu")) {
        Some("x86_64-unknown-linux-gnu")
    } else if cfg!(all(target_arch = "aarch64", target_os = "linux", target_env = "gnu")) {
        Some("aarch64-unknown-linux-gnu")
    } else if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
        Some("aarch64-apple-darwin")
    } else if cfg!(all(target_arch = "x86_64", target_os = "windows", target_env = "msvc")) {
        Some("x86_64-pc-windows-msvc")
    } else {
        None
    }
}

/// The name of the archive of `tag` for `target`, as the release workflow packs it.
fn archive_name(tag: &str, target: &str) -> String {
    let extension = if target.contains("windows") { "zip" } else { "tar.gz" };
    format!("ants-{tag}-{target}.{extension}")
}

/// The archive of `release` for `target` and the release's `SHA256SUMS`.
fn assets<'a>(release: &'a Release, target: &str) -> Result<(&'a Asset, &'a Asset)> {
    let name = archive_name(&release.tag_name, target);
    let find = |name: &str| release.assets.iter().find(|a| a.name == name);
    let archive = find(&name).with_context(|| format!("release {} has no {name}", release.tag_name))?;
    let sums = find(SUMS).with_context(|| format!("release {} has no {SUMS}", release.tag_name))?;
    Ok((archive, sums))
}

/// The SHA-256 digest `sums`, a `sha256sum` listing, gives for the file `name`.
fn expected_digest(sums: &str, name: &str) -> Result<Vec<u8>> {
    let line = sums
        .lines()
        .filter_map(|line| line.split_once(char::is_whitespace))
        .find(|(_, file)| file.trim_start().trim_start_matches('*') == name);
    let (digest, _) = line.with_context(|| format!("{SUMS} lists no {name}"))?;
    let digest = data_encoding::HEXLOWER_PERMISSIVE
        .decode(digest.as_bytes())
        .ok()
        .filter(|d| d.len() == 32)
        .with_context(|| format!("{SUMS} gives a malformed digest for {name}"))?;
    Ok(digest)
}

/// Check `bytes`, the file `name`, against the digest `sums` lists for it.
pub(crate) fn verify(bytes: &[u8], name: &str, sums: &str) -> Result<()> {
    let expected = expected_digest(sums, name)?;
    let actual = ring::digest::digest(&ring::digest::SHA256, bytes);
    if actual.as_ref() != expected.as_slice() {
        bail!(
            "{name} does not match its SHA-256 digest in {SUMS} (expected {}, got {})",
            data_encoding::HEXLOWER.encode(&expected),
            data_encoding::HEXLOWER.encode(actual.as_ref())
        );
    }
    Ok(())
}

/// Why an update may not replace the executable `exe`, with the command to run instead: it was installed by `cargo
/// install` into `cargo_bin`.
fn refusal(exe: &Path, cargo_bin: Option<&Path>) -> Option<String> {
    let cargo_bin = cargo_bin?;
    exe.parent().is_some_and(|dir| dir == cargo_bin).then(|| {
        format!(
            "{} was installed by cargo install; update it the same way: cargo install --locked --git \
             https://github.com/{REPO} ants",
            exe.display()
        )
    })
}

/// `$CARGO_HOME/bin`, else `~/.cargo/bin`, as found on disk.
fn cargo_bin() -> Option<PathBuf> {
    let home =
        std::env::var_os("CARGO_HOME").map(PathBuf::from).or_else(|| Some(std::env::home_dir()?.join(".cargo")))?;
    home.join("bin").canonicalize().ok()
}

async fn api<T: serde::de::DeserializeOwned>(path: &str) -> Result<T> {
    let url = format!("{API_URL}/repos/{REPO}{path}");
    let headers = [("accept", "application/vnd.github+json".to_owned())];
    let body = tokio::time::timeout(API_TIMEOUT, crate::http::download(&url, &headers, MAX_ANSWER))
        .await
        .context("GitHub did not answer in time")?
        .with_context(|| format!("asking GitHub for the releases of {REPO}"))?;
    serde_json::from_slice(&body).with_context(|| format!("GitHub's answer for {path}"))
}

/// The release tagged `tag`, else the newest one, a pre-release only if `pre`.
pub(crate) async fn resolve(tag: Option<&str>, pre: bool) -> Result<Release> {
    if let Some(tag) = tag {
        let tag = if tag.starts_with('v') { tag.to_owned() } else { format!("v{tag}") };
        return api(&format!("/releases/tags/{tag}")).await.with_context(|| format!("finding release {tag}"));
    }
    let releases: Vec<Release> = api("/releases?per_page=100").await?;
    newest(releases, pre).with_context(|| format!("no release of {REPO} is published"))
}

/// The newest of `releases` that is published, a pre-release only if `pre`.
fn newest(releases: Vec<Release>, pre: bool) -> Option<Release> {
    releases
        .into_iter()
        .filter(|r| !r.draft && (pre || !r.prerelease))
        .filter_map(|r| Some((r.version()?, r)))
        .max_by(|(a, _), (b, _)| a.cmp(b))
        .map(|(_, r)| r)
}

/// The executable to replace, its real path: refused when `cargo install` put it there or this target has no
/// releases.
pub(crate) fn executable() -> Result<PathBuf> {
    let exe = std::env::current_exe().and_then(|exe| exe.canonicalize()).context("finding the ants executable")?;
    if let Some(refused) = refusal(&exe, cargo_bin().as_deref()) {
        bail!("{refused}");
    }
    if target().is_none() {
        bail!(
            "no release is built for this platform ({} {}); build from source: cargo install --locked --git \
             https://github.com/{REPO} ants",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
    }
    Ok(exe)
}

/// A directory for the files of an update or download, removed when dropped.
pub(crate) struct Scratch(PathBuf);

impl Scratch {
    /// A new, empty directory at `path`, anything there before removed.
    pub(crate) fn create(path: PathBuf) -> std::io::Result<Self> {
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }

    /// One beside the executable `exe`, for an update.
    fn beside(exe: &Path) -> Result<Self> {
        let dir = exe.parent().context("the executable's directory")?;
        let scratch = dir.join(format!(".ants-update-{}", std::process::id()));
        match Self::create(scratch.clone()) {
            Ok(scratch) => Ok(scratch),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                let command: Vec<String> =
                    std::iter::once(exe.display().to_string()).chain(std::env::args().skip(1)).collect();
                let how = if cfg!(windows) { "from an administrator's prompt:" } else { "as root: sudo" };
                bail!("{} is not writable; run the update {how} {}", dir.display(), command.join(" "))
            }
            Err(e) => Err(e).with_context(|| format!("creating {}", scratch.display())),
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Install `release` in place of `exe`: download its archive for this target, check it against `SHA256SUMS`, unpack
/// it, run the binary with `--version`, then rename it over `exe`. Leaves `exe` as it was on any failure. The
/// nats-server the archive holds goes beside `exe` first, unless one as new is there: returned when it went.
pub(crate) async fn install(release: &Release, exe: &Path) -> Result<Option<NatsServer>> {
    let target = target().context("no release is built for this platform")?;
    let (archive, sums) = assets(release, target)?;
    let scratch = Scratch::beside(exe)?;
    let fetch = async |asset: &Asset, limit| {
        tokio::time::timeout(DOWNLOAD_TIMEOUT, crate::http::download(&asset.browser_download_url, &[], limit))
            .await
            .with_context(|| format!("downloading {} took too long", asset.name))?
            .with_context(|| format!("downloading {}", asset.name))
    };
    let sums = fetch(sums, MAX_ANSWER).await?;
    let bytes = fetch(archive, MAX_ARCHIVE).await?;
    verify(&bytes, &archive.name, &String::from_utf8_lossy(&sums))?;
    let packed = scratch.0.join(&archive.name);
    std::fs::write(&packed, &bytes).with_context(|| format!("writing {}", packed.display()))?;
    let unpacked = unpack(&packed, &scratch.0).await?;
    let binary = unpacked.join(exe_name());
    let staged = scratch.0.join(exe_name());
    std::fs::rename(&binary, &staged).with_context(|| format!("{} holds no {}", archive.name, exe_name()))?;
    prepare(&staged, exe)?;
    smoke_test(&staged, release).await?;
    let nats_server = stage_nats_server(&unpacked, &scratch.0, exe).await?;
    if let Some(nats_server) = &nats_server {
        replace(&nats_server.staged, &nats_server.path)?;
    }
    replace(&staged, exe)?;
    Ok(nats_server)
}

/// The nats-server of a release, put beside the ants executable.
#[derive(Debug)]
pub(crate) struct NatsServer {
    staged: PathBuf,
    /// Where it went.
    pub(crate) path: PathBuf,
    /// The version of the one it replaced, none when there was none.
    was: Option<Version>,
    version: Version,
}

impl fmt::Display for NatsServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "nats-server {} at {}", self.version, self.path.display())?;
        match &self.was {
            Some(was) => write!(f, " (was {was})"),
            None => Ok(()),
        }
    }
}

/// Stage in `scratch` the nats-server of the release unpacked in `unpacked`, to go beside `exe`: none when the
/// archive holds none, or the one there is as new or does not tell its version.
async fn stage_nats_server(unpacked: &Path, scratch: &Path, exe: &Path) -> Result<Option<NatsServer>> {
    let bundled = unpacked.join(bundled::EXE);
    if !bundled.is_file() {
        return Ok(None);
    }
    let path = exe.with_file_name(bundled::EXE);
    let was = if path.exists() {
        match bundled::installed_version(&path).await {
            Ok(was) => Some(was),
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "keeping the nats-server beside ants");
                return Ok(None);
            }
        }
    } else {
        None
    };
    let staged = scratch.join(bundled::EXE);
    std::fs::rename(&bundled, &staged).with_context(|| format!("staging {}", staged.display()))?;
    prepare(&staged, &path)?;
    let version = bundled::installed_version(&staged).await.context("the release's nats-server failed its check")?;
    if was.as_ref().is_some_and(|was| *was >= version) {
        return Ok(None);
    }
    Ok(Some(NatsServer { staged, path, was, version }))
}

fn exe_name() -> &'static str {
    if cfg!(windows) { "ants.exe" } else { "ants" }
}

/// Unpack the archive `packed` into `into` with the system's `tar` (bsdtar on Windows, which reads zip too): the
/// directory holding its files.
pub(crate) async fn unpack(packed: &Path, into: &Path) -> Result<PathBuf> {
    let output = tokio::process::Command::new("tar")
        .arg(if cfg!(windows) { "-xf" } else { "-xzf" })
        .arg(packed)
        .arg("-C")
        .arg(into)
        .kill_on_drop(true)
        .output()
        .await
        .context("running tar to unpack the release")?;
    if !output.status.success() {
        bail!(
            "tar failed to unpack the release ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let name = packed.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    let dir = name.trim_end_matches(".zip").trim_end_matches(".tar.gz");
    Ok(into.join(dir))
}

/// Give `staged` the permissions of `exe`, executable, and flush it to disk.
pub(crate) fn prepare(staged: &Path, exe: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(exe).map_or(0o755, |m| m.permissions().mode()) | 0o111;
        std::fs::set_permissions(staged, std::fs::Permissions::from_mode(mode & 0o7777))?;
    }
    #[cfg(not(unix))]
    let _ = exe;
    std::fs::File::open(staged).and_then(|f| f.sync_all()).with_context(|| format!("flushing {}", staged.display()))
}

/// Run `staged --version`, which must name the version of `release`.
async fn smoke_test(staged: &Path, release: &Release) -> Result<()> {
    let run = tokio::process::Command::new(staged).arg("--version").kill_on_drop(true).output();
    let output = tokio::time::timeout(SMOKE_TIMEOUT, run)
        .await
        .context("the new binary did not answer --version in time")?
        .context("running the new binary")?;
    let printed = String::from_utf8_lossy(&output.stdout);
    let version = printed.trim().strip_prefix("ants ").unwrap_or_default();
    let tag = release.tag_name.trim_start_matches('v');
    if !output.status.success() || version.is_empty() || !tag.starts_with(version) {
        bail!(
            "the new binary failed its check: `ants --version` gave {:?} ({}), not {}",
            printed.trim(),
            output.status,
            release.tag_name
        );
    }
    Ok(())
}

/// Rename `staged` over `exe` in one step. Windows does not replace a running executable, so one there is first
/// renamed aside to `<exe>.old`, which `remove_old` deletes on a later run.
fn replace(staged: &Path, exe: &Path) -> Result<()> {
    if cfg!(windows) && exe.exists() {
        let old = old_path(exe);
        match std::fs::remove_file(&old) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => bail!("removing {} left by the last update, still running: {e}", old.display()),
        }
        std::fs::rename(exe, &old).with_context(|| format!("renaming {} aside", exe.display()))?;
        if let Err(e) = std::fs::rename(staged, exe) {
            if let Err(restoring) = std::fs::rename(&old, exe) {
                tracing::warn!(error = %restoring, "restoring {} from {}", exe.display(), old.display());
            }
            return Err(e).with_context(|| format!("replacing {}", exe.display()));
        }
        return Ok(());
    }
    std::fs::rename(staged, exe).with_context(|| format!("replacing {}", exe.display()))
}

fn old_path(exe: &Path) -> PathBuf {
    let mut old = exe.as_os_str().to_owned();
    old.push(".old");
    PathBuf::from(old)
}

/// Delete the executables an earlier update renamed aside, ants and nats-server, once nothing runs them.
#[cfg(windows)]
pub(crate) fn remove_old() {
    if let Ok(exe) = std::env::current_exe() {
        // Each fails while it still runs, or when none is left; the next start tries again.
        let _ = std::fs::remove_file(old_path(&exe));
        let _ = std::fs::remove_file(old_path(&exe.with_file_name(bundled::EXE)));
    }
}

/// What a daemon does about a newer release, polling the stable releases about daily while `mode` is not off: warns
/// of it (`check`), or installs it (`install`), up to the version the bus runs ([`ants_daemon::host::bus_version`]),
/// for the daemon's upgrade check, every `upgrade_check`, to restart onto it. Both say when this machine runs behind
/// or ahead of the bus. Never downgrades; backs off after a failure. Never returns.
pub(crate) async fn auto(mode: AutoUpdate, upgrade_check: Duration) {
    if mode == AutoUpdate::Off {
        return std::future::pending().await;
    }
    let mut wait = random_below(FIRST_POLL);
    let mut retry = RETRY_AFTER;
    loop {
        tokio::time::sleep(wait).await;
        match poll(mode, upgrade_check).await {
            Ok(true) => return std::future::pending().await,
            Ok(false) => {
                retry = RETRY_AFTER;
                wait = POLL_EVERY - POLL_JITTER / 2 + random_below(POLL_JITTER);
            }
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), retry_in = ?retry, "checking for a newer release of ants");
                (wait, retry) = (retry, backoff(retry));
            }
        }
    }
}

/// The wait after another failed poll, having waited `retry` after the last one.
fn backoff(retry: Duration) -> Duration {
    retry.saturating_mul(2).min(POLL_EVERY)
}

/// What a poll does about the releases.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    /// Nothing: off, or no newer release to act on.
    Nothing,
    /// Warn of the newer release (`check`).
    Warn,
    /// Install this release (`install`).
    Install(Version),
    /// Install nothing, as the version the bus runs is not known (`install`).
    BusUnknown,
}

/// What `mode` does once the latest stable release is `latest`, this executable `running` and the bus `bus`: nothing
/// (off, or not newer), warn (check), or install the latest release, but none newer than the bus's (install).
fn decide(mode: AutoUpdate, running: &Version, latest: &Version, bus: Option<&Version>) -> Decision {
    match (mode, bus) {
        (AutoUpdate::Off, _) => Decision::Nothing,
        _ if latest <= running => Decision::Nothing,
        (AutoUpdate::Check, _) => Decision::Warn,
        (AutoUpdate::Install, None) => Decision::BusUnknown,
        (AutoUpdate::Install, Some(bus)) if bus <= running => Decision::Nothing,
        (AutoUpdate::Install, Some(bus)) => Decision::Install(latest.min(bus).clone()),
    }
}

/// Where this machine, running `running`, stands to the bus running `bus`, when apart.
fn apart(running: &Version, bus: &Version) -> Option<String> {
    match running.cmp(bus) {
        Ordering::Less => {
            Some(format!("this machine runs ants {running}, behind the bus's {bus} (the version `ants signer` runs)"))
        }
        Ordering::Greater => Some(format!(
            "this machine runs ants {running}, ahead of the bus's {bus} (the version `ants signer` runs): a release \
             before 1.0 may change the wire, so update the hub first"
        )),
        Ordering::Equal => None,
    }
}

/// Poll the releases once, doing what `mode` says of a newer one: whether one was installed.
async fn poll(mode: AutoUpdate, upgrade_check: Duration) -> Result<bool> {
    let latest = resolve(None, false).await?;
    let version = latest.version().with_context(|| format!("release tag {} is not a version", latest.tag_name))?;
    let running = Version::running();
    let bus = ants_daemon::host::bus_version().and_then(|v| Version::parse(&v));
    if let Some(note) = bus.as_ref().and_then(|bus| apart(&running, bus)) {
        tracing::warn!("{note}");
    }
    match decide(mode, &running, &version, bus.as_ref()) {
        Decision::Nothing => {
            tracing::debug!(%running, latest = %version, bus = ?bus.map(|b| b.to_string()), "nothing to install");
            Ok(false)
        }
        Decision::Warn => {
            tracing::warn!(
                "ants {version} is available (running {running}); `ants update` installs it, or auto_update = \
                 \"install\" in the config file"
            );
            Ok(false)
        }
        Decision::BusUnknown => {
            tracing::warn!(
                "ants {version} is available (running {running}), but auto_update = \"install\" installs nothing \
                 while the version the bus runs is not known: it is read with the machine's host credentials \
                 (`ants auth host`) from the record `ants signer` puts"
            );
            Ok(false)
        }
        Decision::Install(target) => {
            let release = if target == version { latest } else { resolve(Some(&target.to_string()), false).await? };
            let version = target;
            let exe = executable()?;
            let nats_server = install(&release, &exe).await?;
            tracing::info!(executable = %exe.display(), "installed ants {version} (was {running})");
            if let Some(nats_server) = nats_server {
                tracing::info!("installed {nats_server}");
            }
            if upgrade_check.is_zero() {
                tracing::warn!("--upgrade-check is 0: the daemon runs ants {running} until restarted");
            }
            Ok(true)
        }
    }
}

/// A random duration below `bound`.
fn random_below(bound: Duration) -> Duration {
    use std::hash::BuildHasher as _;
    let random = std::collections::hash_map::RandomState::new().hash_one(std::time::Instant::now());
    let bound = u64::try_from(bound.as_millis()).unwrap_or(u64::MAX).max(1);
    Duration::from_millis(random % bound)
}

/// `ants update`.
pub(crate) async fn run(args: UpdateArgs) -> Result<()> {
    let running = Version::running();
    let release = resolve(args.version.as_deref(), args.pre).await?;
    let version = release.version().with_context(|| format!("release tag {} is not a version", release.tag_name))?;
    if args.check {
        if version > running {
            println!("ants {version} is available (running {running}); `ants update` installs it");
            std::process::exit(AVAILABLE_EXIT_CODE);
        }
        println!("ants {running} is up to date (latest release {version})");
        return Ok(());
    }
    if args.version.is_none() && version <= running {
        println!("ants {running} is up to date (latest release {version})");
        return Ok(());
    }
    let exe = executable()?;
    let nats_server = install(&release, &exe).await?;
    println!("installed ants {version} at {} (was {running})", exe.display());
    if let Some(nats_server) = &nats_server {
        println!("installed {nats_server}");
    }
    let nats_server = nats_server.map(|n| n.path);
    for line in services_note(&mut Services::local(), nats_server.as_deref()) {
        println!("{line}");
    }
    Ok(())
}

/// What becomes of the services running ants: each installed one restarts onto the new executable through its upgrade
/// check, or, with the check off, by hand with the command given; and of the nats-server service running
/// `nats_server`, the nats-server just installed, which runs the old one until restarted.
fn services_note(services: &mut Services, nats_server: Option<&Path>) -> Vec<String> {
    let mut lines = vec![
        "services running ants (ants daemon, webhook serve, bridge, channel serve, signer) restart onto it within \
         their --upgrade-check interval (30s by default); restart any started by hand or with --upgrade-check 0"
            .to_owned(),
    ];
    for service in SERVICES {
        let Some(unit) = services.unit(service) else { continue };
        let every = unit.flag("--upgrade-check").or_else(|| unit.env("ANTS_UPGRADE_CHECK"));
        let off = every.and_then(|e| ants_proto::duration::parse(e).ok()).is_some_and(|d| d.is_zero());
        if off && let Some(restart) = services.by_hand(service, Action::Restart) {
            lines.push(format!("an installed service checks for no upgrade; restart it: {restart}"));
        }
    }
    let runs = |unit: &crate::service::Unit| {
        unit.args
            .first()
            .zip(nats_server)
            .is_some_and(|(program, new)| crate::doctor::same_file(Path::new(program), new))
    };
    if services.unit(Service::Nats).is_some_and(|unit| runs(&unit))
        && let Some(restart) = services.by_hand(Service::Nats, Action::Restart)
    {
        lines.push(format!("the nats-server service runs the old nats-server until restarted: {restart}"));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(text: &str) -> Version {
        Version::parse(text).unwrap()
    }

    fn release(tag: &str, prerelease: bool, assets: &[&str]) -> Release {
        Release {
            tag_name: tag.to_owned(),
            draft: false,
            prerelease,
            assets: assets
                .iter()
                .map(|name| Asset { name: (*name).to_owned(), browser_download_url: format!("https://x/{name}") })
                .collect(),
        }
    }

    #[test]
    fn versions_order_as_semantic_versions() {
        let ordered = ["v0.1.0", "v0.2.0-alpha", "v0.2.0-alpha.1", "v0.2.0-alpha.beta", "v0.2.0-rc.2", "v0.2.0-rc.10"];
        for pair in ordered.windows(2) {
            assert!(v(pair[0]) < v(pair[1]), "{} < {}", pair[0], pair[1]);
        }
        assert!(v("v0.2.0-rc.10") < v("0.2.0"));
        assert!(v("v0.10.0") > v("v0.9.9"));
        assert_eq!(v("v1.2.3+build.5"), v("1.2.3"));
        assert_eq!(v("v0.2.0-rc.1").to_string(), "v0.2.0-rc.1");
        for bad in ["", "v1", "v1.2", "v1.2.3.4", "v1.x.3", "v1.2.3-", "v1.2.3-a..b", "latest"] {
            assert!(Version::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn auto_update_acts_on_a_newer_release_only_and_backs_off() {
        let (running, newer, older) = (v("v0.2.0"), v("v0.3.0"), v("v0.1.0"));
        assert_eq!(decide(AutoUpdate::Off, &running, &newer, Some(&newer)), Decision::Nothing);
        assert_eq!(decide(AutoUpdate::Check, &running, &newer, None), Decision::Warn);
        assert_eq!(decide(AutoUpdate::Install, &running, &newer, Some(&newer)), Decision::Install(newer.clone()));
        for mode in [AutoUpdate::Off, AutoUpdate::Check, AutoUpdate::Install] {
            let bus = Some(&newer);
            assert_eq!(decide(mode, &running, &running, bus), Decision::Nothing, "{mode:?}");
            assert_eq!(decide(mode, &running, &older, bus), Decision::Nothing, "{mode:?} never downgrades");
        }
        let mut retry = RETRY_AFTER;
        let waits: Vec<_> = (0..7)
            .map(|_| {
                let wait = retry;
                retry = backoff(retry);
                wait.as_secs() / 3600
            })
            .collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 24, 24]);
        assert!(random_below(FIRST_POLL) < FIRST_POLL);
    }

    /// On a bus whose signer runs 0.4.0, install does not take 0.5.0 until the signer runs it; a machine behind the bus
    /// catches up to it, and one ahead installs nothing; check warns of 0.5.0 all the same.
    #[test]
    fn install_goes_up_to_the_version_the_bus_runs_and_check_says_where_the_machine_stands() {
        let (v3, v4, v5) = (v("v0.3.0"), v("v0.4.0"), v("v0.5.0"));
        assert_eq!(decide(AutoUpdate::Install, &v4, &v5, Some(&v4)), Decision::Nothing, "the bus runs 0.4.0");
        assert_eq!(decide(AutoUpdate::Install, &v4, &v5, Some(&v5)), Decision::Install(v5.clone()), "then 0.5.0");
        assert_eq!(decide(AutoUpdate::Install, &v3, &v5, Some(&v4)), Decision::Install(v4.clone()), "up to the bus");
        assert_eq!(decide(AutoUpdate::Install, &v5, &v5, Some(&v4)), Decision::Nothing, "ahead of the bus");
        assert_eq!(decide(AutoUpdate::Install, &v4, &v5, None), Decision::BusUnknown);
        assert_eq!(decide(AutoUpdate::Check, &v4, &v5, Some(&v4)), Decision::Warn);
        assert_eq!(apart(&v4, &v4), None);
        assert!(apart(&v3, &v4).unwrap().contains("ants v0.3.0, behind the bus's v0.4.0"), "{:?}", apart(&v3, &v4));
        assert!(apart(&v5, &v4).unwrap().contains("ants v0.5.0, ahead of the bus's v0.4.0"), "{:?}", apart(&v5, &v4));
    }

    #[test]
    fn the_newest_published_release_wins_pre_releases_only_when_asked() {
        let mut draft = release("v9.0.0", false, &[]);
        draft.draft = true;
        let releases = vec![
            release("v0.1.0", false, &[]),
            release("v0.3.0-rc.1", true, &[]),
            release("v0.2.0", false, &[]),
            release("nightly", true, &[]),
            draft,
        ];
        assert_eq!(newest(releases.clone(), false).unwrap().tag_name, "v0.2.0");
        assert_eq!(newest(releases, true).unwrap().tag_name, "v0.3.0-rc.1");
        assert!(newest(vec![], false).is_none());
    }

    #[test]
    fn the_archive_for_the_target_is_picked_with_the_checksums() {
        let tag = "v0.1.0";
        let names: Vec<String> = ["aarch64-apple-darwin", "x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"]
            .iter()
            .map(|t| archive_name(tag, t))
            .chain([SUMS.to_owned()])
            .collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let full = release(tag, false, &names);
        let (archive, sums) = assets(&full, "x86_64-pc-windows-msvc").unwrap();
        assert_eq!((archive.name.as_str(), sums.name.as_str()), ("ants-v0.1.0-x86_64-pc-windows-msvc.zip", SUMS));
        let (archive, _) = assets(&full, "aarch64-apple-darwin").unwrap();
        assert_eq!(archive.name, "ants-v0.1.0-aarch64-apple-darwin.tar.gz");

        let missing = assets(&full, "aarch64-unknown-linux-gnu").unwrap_err().to_string();
        assert!(missing.contains("no ants-v0.1.0-aarch64-unknown-linux-gnu.tar.gz"), "{missing}");
        let unsummed = release(tag, false, &names[..1]);
        assert!(assets(&unsummed, "aarch64-apple-darwin").unwrap_err().to_string().contains(SUMS));
    }

    #[test]
    fn an_archive_is_checked_against_its_listed_digest() {
        let bytes = b"archive bytes";
        let digest = data_encoding::HEXLOWER.encode(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref());
        let other = "0".repeat(64);
        let sums = format!("{other}  ants-other.tar.gz\n{digest}  ants-a.tar.gz\n{other} *ants-b.zip\n");
        verify(bytes, "ants-a.tar.gz", &sums).unwrap();
        let mismatch = verify(bytes, "ants-b.zip", &sums).unwrap_err().to_string();
        assert!(mismatch.contains("does not match"), "{mismatch}");
        let unlisted = verify(bytes, "ants-c.zip", &sums).unwrap_err().to_string();
        assert!(unlisted.contains("lists no ants-c.zip"), "{unlisted}");
        let malformed = verify(bytes, "x", "abc  x\n").unwrap_err().to_string();
        assert!(malformed.contains("malformed"), "{malformed}");
        assert_eq!(expected_digest(&sums.to_uppercase().replace("ANTS-A.TAR.GZ", "a"), "a").unwrap().len(), 32);
    }

    #[test]
    fn an_executable_from_cargo_install_is_refused() {
        let cargo_bin = Path::new("/home/me/.cargo/bin");
        let refused = refusal(&cargo_bin.join("ants"), Some(cargo_bin)).unwrap();
        assert!(refused.contains("cargo install --locked --git https://github.com/quanghle/ants ants"), "{refused}");
        assert!(refusal(Path::new("/home/me/.local/bin/ants"), Some(cargo_bin)).is_none());
        assert!(refusal(&cargo_bin.join("ants"), None).is_none());
    }

    #[test]
    fn an_unwritable_directory_is_refused_and_a_replacement_is_whole() {
        let dir = ants_test_support::scratch_dir("update-replace");
        let exe = dir.join(exe_name());
        std::fs::write(&exe, "old").unwrap();
        let scratch = Scratch::beside(&exe).unwrap();
        let staged = scratch.0.join(exe_name());
        std::fs::write(&staged, "new").unwrap();
        prepare(&staged, &exe).unwrap();
        replace(&staged, &exe).unwrap();
        assert_eq!(std::fs::read_to_string(&exe).unwrap(), "new");
        drop(scratch);
        let left: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(left.len(), if cfg!(windows) { 2 } else { 1 }, "only the executable stays: {left:?}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(std::fs::metadata(&exe).unwrap().permissions().mode() & 0o111, 0o111);
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
            let refused = Scratch::beside(&exe).map(|_| ());
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            // Root writes anywhere.
            if let Err(refused) = refused {
                let refused = refused.to_string();
                assert!(refused.contains("is not writable") && refused.contains("sudo"), "{refused}");
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_bundled_nats_server_goes_beside_ants_unless_one_as_new_is_there() {
        let dir = ants_test_support::scratch_dir("update-nats-server");
        let (unpacked, scratch, bin) = (dir.join("unpacked"), dir.join("scratch"), dir.join("bin"));
        for d in [&unpacked, &scratch, &bin] {
            std::fs::create_dir_all(d).unwrap();
        }
        let exe = bin.join("ants");
        let fake = |path: &Path, version: &str| {
            ants_test_support::executable(path, &format!("#!/bin/sh\necho 'nats-server: {version}'\n"));
        };
        let staged = async || {
            fake(&unpacked.join("nats-server"), "v2.15.3");
            stage_nats_server(&unpacked, &scratch, &exe).await.unwrap()
        };

        let new = staged().await.unwrap();
        assert_eq!(
            (new.path.clone(), new.was.clone(), new.version.to_string()),
            (bin.join("nats-server"), None, "v2.15.3".into())
        );
        fake(&bin.join("nats-server"), "v2.14.9");
        assert_eq!(staged().await.unwrap().was, Some(v("v2.14.9")), "an older one is replaced");
        fake(&bin.join("nats-server"), "v2.15.3");
        assert!(staged().await.is_none(), "one as new stays");
        fake(&bin.join("nats-server"), "v2.16.0");
        assert!(staged().await.is_none(), "a newer one stays");
        std::fs::write(bin.join("nats-server"), "#!/bin/sh\nexit 1\n").unwrap();
        assert!(staged().await.is_none(), "one not telling its version stays");
        std::fs::remove_file(unpacked.join("nats-server")).unwrap();
        assert!(stage_nats_server(&unpacked, &scratch, &exe).await.unwrap().is_none(), "an archive without one");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
