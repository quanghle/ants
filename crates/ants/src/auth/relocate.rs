//! `ants auth move-stores`: the one-time move of a server's streams, buckets and object stores from the bus's account,
//! which held JetStream before the store account, into the store account.
//!
//! JetStream keeps an account's streams in `<store_dir>/jetstream/<account>/streams/<stream>`, each with its messages,
//! sequences, timestamps and consumers. With the server stopped, moving those directories under the store account
//! keeps all of it, which sourcing each stream into a new one would not: sequence numbers (and so KV revisions),
//! timestamps and consumers' ack floors.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};

use super::preloaded_accounts;

/// What [`move_stores`] moved.
#[derive(Debug, PartialEq, Eq)]
pub struct Moved {
    /// The streams moved, by name.
    pub streams: Vec<String>,
    /// Those of them that replaced a stream of the same name in the store account.
    pub replaced: Vec<String>,
    /// The bus's account's directory, when left in place as it holds more than streams.
    pub left: Option<PathBuf>,
}

/// Move the streams the bus's account holds in the JetStream `store_dir` of a stopped server into the store account,
/// the accounts as `server_conf` (a `server.conf` of `ants auth init`) preloads them. A stream the store account holds
/// already, created by a start on the store account's layout before the move, fails the move unless `replace`
/// discards it.
pub fn move_stores(store_dir: &Path, server_conf: &Path, replace: bool) -> Result<Moved> {
    let text = fs::read_to_string(server_conf).with_context(|| format!("reading {}", server_conf.display()))?;
    let (account, store) = preloaded_accounts(&text).with_context(|| server_conf.display().to_string())?;
    let Some(store) = store else {
        bail!(
            "{} preloads no store account: run `ants auth init` on the auth directory to add it, and use the \
             server.conf it writes",
            server_conf.display()
        );
    };
    let jetstream = store_dir.join("jetstream");
    if !jetstream.is_dir() {
        bail!(
            "{} holds no JetStream data (no jetstream directory): give the store_dir of the server's jetstream block",
            store_dir.display()
        );
    }
    let from = jetstream.join(&account);
    let to = jetstream.join(&store);
    let streams = streams(&from.join("streams"))?;
    if !to.exists() {
        if from.exists() {
            fs::rename(&from, &to).with_context(|| format!("moving {} to {}", from.display(), to.display()))?;
        }
        return Ok(Moved { streams, replaced: Vec::new(), left: None });
    }
    let target = to.join("streams");
    let replaced: Vec<String> = streams.iter().filter(|s| target.join(s).exists()).cloned().collect();
    if !replace && !replaced.is_empty() {
        bail!(
            "the store account already holds {}, created since the server started on the store account's layout: \
             pass --replace to discard them for the bus's account's",
            replaced.join(", ")
        );
    }
    fs::create_dir_all(&target).with_context(|| format!("creating {}", target.display()))?;
    for stream in &streams {
        let (old, new) = (from.join("streams").join(stream), target.join(stream));
        if replaced.contains(stream) {
            fs::remove_dir_all(&new).with_context(|| format!("removing {}", new.display()))?;
        }
        fs::rename(&old, &new).with_context(|| format!("moving {} to {}", old.display(), new.display()))?;
    }
    let _ = fs::remove_dir(from.join("streams"));
    let left = (from.exists() && fs::remove_dir(&from).is_err()).then_some(from);
    Ok(Moved { streams, replaced, left })
}

/// The streams in `dir`, by name; none when it is not there.
fn streams(dir: &Path) -> Result<Vec<String>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", dir.display())),
    };
    let mut streams = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("reading {}", dir.display()))?;
        if entry.file_type()?.is_dir() {
            streams.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    streams.sort();
    Ok(streams)
}

#[cfg(test)]
mod tests;
