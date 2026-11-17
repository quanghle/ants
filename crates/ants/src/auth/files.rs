//! Credentials and seed files: reading and writing `.creds` files, seeds and other secrets.

use std::fs;
use std::io::Write as _;
use std::path::Path;

use ants_daemon::secrets::creds::Seed;
use anyhow::{Context as _, Result, anyhow};
use nkeys::{KeyPair, XKey};

/// The user JWT and key of the `.creds` file at `path`, whose seed is in the file or in the OS credential store.
pub fn read_creds(path: &Path) -> Result<(String, KeyPair)> {
    ants_daemon::secrets::creds::load(path, ants_daemon::secrets::keystore::os())
}

/// The user JWT of a `.creds` file's text: the line after the `BEGIN NATS USER JWT` marker.
pub fn creds_jwt(creds: &str) -> Option<&str> {
    creds_block(creds, "BEGIN NATS USER JWT")
}

pub(super) fn creds_block<'a>(creds: &'a str, marker: &str) -> Option<&'a str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains(marker))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
}

/// Write the `.creds` file of `jwt` and the seed of `user` to `out`, readable by its owner only.
pub fn write_creds(out: &Path, jwt: &str, user: &KeyPair) -> Result<()> {
    write_secret(out, &creds(jwt, &seed(user)?))
}

/// Write `user`'s seed alone to `out`, readable by its owner only.
pub fn write_seed(out: &Path, user: &KeyPair) -> Result<()> {
    write_secret(out, &format!("{}\n", seed(user)?))
}

/// Read a seed written by [`write_seed`].
pub fn read_seed(path: &Path) -> Result<KeyPair> {
    let seed = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    KeyPair::from_seed(seed.trim()).map_err(|e| anyhow!("{}: {e}", path.display()))
}

pub(super) fn creds(jwt: &str, seed: &str) -> String {
    ants_daemon::secrets::creds::text(jwt, seed)
}

/// The text of a `.creds` file holding `jwt` and `seed`, and the seed of the encryption key `xkey` if given.
pub(crate) fn creds_with(jwt: &str, seed: &str, xkey: Option<&XKey>) -> Result<String> {
    let mut text = creds(jwt, seed);
    if let Some(xkey) = xkey {
        text += &ants_daemon::secrets::creds::xkey_block(Seed::File(&xseed(xkey)?));
    }
    Ok(text)
}

pub(crate) fn xseed(xkey: &XKey) -> Result<String> {
    xkey.seed().map_err(|e| anyhow!("{e}"))
}

pub(crate) fn seed(key: &KeyPair) -> Result<String> {
    key.seed().map_err(|e| anyhow!("{e}"))
}

pub(super) fn load(path: &Path) -> Result<KeyPair> {
    let seed =
        fs::read_to_string(path).with_context(|| format!("reading {} (run `ants auth init`)", path.display()))?;
    KeyPair::from_seed(seed.trim()).map_err(|e| anyhow!("{}: {e}", path.display()))
}

/// Replace the `.creds` file at `path` with that of `jwt` and the seed of `user`, and of the encryption key `xkey` if
/// given, at once: written beside it, readable by its owner only, then renamed over it, so what reads it finds either
/// the earlier credentials or the new ones.
pub fn replace_creds(path: &Path, jwt: &str, user: &KeyPair, xkey: Option<&XKey>) -> Result<()> {
    replace_secret(path, &creds_with(jwt, &seed(user)?, xkey)?)
}

/// Replace the file at `path` with `contents` at once: written beside it, readable by its owner only, then renamed
/// over it.
pub(crate) fn replace_secret(path: &Path, contents: &str) -> Result<()> {
    let name = path.file_name().with_context(|| format!("{} names no file", path.display()))?;
    let mut temporary = name.to_owned();
    temporary.push(format!(".{}.tmp", std::process::id()));
    let temporary = path.with_file_name(temporary);
    let written = write_secret(&temporary, contents)
        .and_then(|()| fs::rename(&temporary, path).with_context(|| format!("replacing {}", path.display())));
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    written
}

/// Write a file only its owner can read, replacing any earlier one.
pub(crate) fn write_secret(path: &Path, contents: &str) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path).with_context(|| format!("writing {}", path.display()))?;
    // `mode` applies only on creation; tighten a file that already existed too.
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}
