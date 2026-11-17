//! The nats-server bundled with ants: the release pinned in `nats-server.sha256`, packed beside `ants` in each release
//! archive, or downloaded into `~/.ants/bin` by `ants setup` ([`download`]). The default command `nats-server` is
//! found beside the ants executable, then in `~/.ants/bin`, then on PATH ([`find`]).

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};

use crate::release::update::{self, Scratch, Version};

/// The pinned release's archives with their SHA-256 digests, a `sha256sum` listing.
const PINNED: &str = include_str!("../../../nats-server.sha256");
const RELEASES: &str = "https://github.com/nats-io/nats-server/releases/download";
/// Largest archive downloaded.
const MAX_ARCHIVE: usize = 64 * 1024 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_mins(10);
const VERSION_TIMEOUT: Duration = Duration::from_secs(30);

/// The default nats-server command, a bare name [`find`] looks up.
#[cfg(unix)]
pub(crate) const DEFAULT: &str = "nats-server";
/// The file name of the nats-server executable.
pub(crate) const EXE: &str = if cfg!(windows) { "nats-server.exe" } else { "nats-server" };

/// The platform of this OS and architecture, as nats-server releases name it.
fn platform() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Some("darwin-arm64"),
        ("macos", "x86_64") => Some("darwin-amd64"),
        ("linux", "x86_64") => Some("linux-amd64"),
        ("linux", "aarch64") => Some("linux-arm64"),
        ("windows", "x86_64") => Some("windows-amd64"),
        _ => None,
    }
}

/// The archive `pinned` lists for `platform`, with the version it holds.
fn archive<'a>(pinned: &'a str, platform: &str) -> Option<(&'a str, &'a str)> {
    let suffix = format!("-{platform}");
    pinned.lines().filter(|line| !line.starts_with('#')).filter_map(|line| line.split_whitespace().nth(1)).find_map(
        |name| {
            let stem = name.strip_suffix(".tar.gz").or_else(|| name.strip_suffix(".zip"))?;
            let version = stem.strip_suffix(&suffix)?.strip_prefix("nats-server-")?;
            Some((name, version))
        },
    )
}

/// The version of the pinned nats-server, such as `v2.15.3`.
pub(crate) fn version() -> &'static str {
    archive(PINNED, platform().unwrap_or("linux-amd64")).map_or("", |(_, version)| version)
}

/// Where `ants setup` downloads nats-server: `~/.ants/bin`.
pub(crate) fn dir() -> Result<PathBuf> {
    Ok(std::env::home_dir().context("no home directory")?.join(".ants").join("bin"))
}

/// The directory of the ants executable, its real path.
fn exe_dir() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let exe = exe.canonicalize().unwrap_or(exe);
    exe.parent().map(Path::to_owned)
}

/// The nats-server executable: beside the ants executable, else in `~/.ants/bin`, else on PATH.
pub(crate) fn find() -> Option<PathBuf> {
    search(&[exe_dir(), dir().ok()], std::env::var_os("PATH").as_deref())
}

/// The first nats-server executable in `dirs`, then in the directories of `path`.
fn search(dirs: &[Option<PathBuf>], path: Option<&OsStr>) -> Option<PathBuf> {
    let on_path = path.into_iter().flat_map(std::env::split_paths);
    dirs.iter().flatten().cloned().chain(on_path).map(|dir| dir.join(EXE)).find(|exe| exe.is_file())
}

/// The program to run for the nats-server command `command`: the default one as [`find`] finds it, any other as
/// given.
#[cfg(unix)]
pub(crate) fn resolve(command: &Path) -> PathBuf {
    if command != Path::new(DEFAULT) {
        return command.to_owned();
    }
    find().unwrap_or_else(|| command.to_owned())
}

/// Where the default nats-server is looked for, for messages.
pub(crate) const LOOKED_UP: &str = "beside the ants executable, in ~/.ants/bin or on PATH";

/// Download the pinned nats-server for this platform into `dir`, its archive checked against the pinned digest and
/// the executable against the pinned version: the path of the executable.
pub(crate) async fn download(dir: &Path) -> Result<PathBuf> {
    let platform = platform().with_context(|| {
        format!("nats-server publishes no release for {} {}", std::env::consts::OS, std::env::consts::ARCH)
    })?;
    let (name, version) = archive(PINNED, platform).with_context(|| format!("no nats-server pinned for {platform}"))?;
    let url = format!("{RELEASES}/{version}/{name}");
    let bytes = tokio::time::timeout(DOWNLOAD_TIMEOUT, crate::http::download(&url, &[], MAX_ARCHIVE))
        .await
        .with_context(|| format!("downloading {name} took too long"))?
        .with_context(|| format!("downloading {url}"))?;
    update::verify(&bytes, name, PINNED)?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let scratch = Scratch::create(dir.join(format!(".nats-server-{}", std::process::id())))
        .with_context(|| format!("creating a directory in {}", dir.display()))?;
    let packed = scratch.path().join(name);
    std::fs::write(&packed, &bytes).with_context(|| format!("writing {}", packed.display()))?;
    let staged = update::unpack(&packed, scratch.path()).await?.join(EXE);
    let exe = dir.join(EXE);
    update::prepare(&staged, &exe)?;
    let found = installed_version(&staged).await?;
    if found.to_string() != version {
        bail!("{name} holds nats-server {found}, not {version}");
    }
    std::fs::rename(&staged, &exe).with_context(|| format!("moving nats-server to {}", exe.display()))?;
    Ok(exe)
}

/// The version `exe --version` prints (`nats-server: v2.15.3`).
pub(crate) async fn installed_version(exe: &Path) -> Result<Version> {
    let run = tokio::process::Command::new(exe).arg("--version").kill_on_drop(true).output();
    let output = tokio::time::timeout(VERSION_TIMEOUT, run)
        .await
        .with_context(|| format!("{} did not answer --version in time", exe.display()))?
        .with_context(|| format!("running {} --version", exe.display()))?;
    let printed = String::from_utf8_lossy(&output.stdout);
    let version = printed.trim().strip_prefix("nats-server: ").and_then(Version::parse);
    match version {
        Some(version) if output.status.success() => Ok(version),
        _ => bail!("`{} --version` gave {:?} ({})", exe.display(), printed.trim(), output.status),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pinned_archive_of_each_platform_names_its_version() {
        let pinned = "# comment\n\
                      00  nats-server-v2.15.3-linux-amd64.tar.gz\n\
                      11  nats-server-v2.16.0-RC.1-windows-amd64.zip\n";
        assert_eq!(archive(pinned, "linux-amd64"), Some(("nats-server-v2.15.3-linux-amd64.tar.gz", "v2.15.3")));
        assert_eq!(
            archive(pinned, "windows-amd64"),
            Some(("nats-server-v2.16.0-RC.1-windows-amd64.zip", "v2.16.0-RC.1"))
        );
        assert_eq!(archive(pinned, "linux-arm64"), None);
    }

    #[test]
    fn every_release_target_has_a_pinned_nats_server_of_one_version() {
        let versions: Vec<_> = ["darwin-arm64", "linux-amd64", "linux-arm64", "windows-amd64"]
            .map(|platform| archive(PINNED, platform).unwrap().1)
            .to_vec();
        assert!(versions.iter().all(|v| *v == version() && Version::parse(v).is_some()), "{versions:?}");
    }

    #[test]
    fn the_default_nats_server_is_found_beside_ants_then_in_the_download_directory_then_on_path() {
        let root = ants_test_support::scratch_dir("bundled-search");
        let [beside, downloaded, on_path] = ["beside", "downloaded", "path"].map(|name| root.join(name));
        for dir in [&beside, &downloaded, &on_path] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let path = std::env::join_paths([&on_path]).unwrap();
        let dirs = [Some(beside.clone()), Some(downloaded.clone())];
        assert_eq!(search(&dirs, Some(&path)), None);
        for dir in [&on_path, &downloaded, &beside] {
            std::fs::write(dir.join(EXE), "").unwrap();
            assert_eq!(search(&dirs, Some(&path)), Some(dir.join(EXE)));
        }
        assert_eq!(search(&[None, None], Some(&path)), Some(on_path.join(EXE)));
        std::fs::remove_dir_all(&root).unwrap();
    }
}
