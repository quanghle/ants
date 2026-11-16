//! [`AdminOp::Retire`]: the `ants retire` flow for an ant of the config file the daemon serves, templated or not. Its
//! `[[ant]]` table goes, so the reload stops it, its prompts in flight ending; once it left the colony, or after
//! [`STOP_WAIT`], `ants signer` revokes its key on the order ([`super::ask`]) and its credentials are deleted, with
//! any seed of theirs in the OS credential store. Its home stays. The order taken again after the table went, its
//! outcome not stored, finds the credentials left beside the host credentials and goes on from the revocation.
//!
//! On a machine running a leaf node, the request carries its uplink credentials too, and the signer, once it revoked
//! the key, renews them without the ant, which the leaf node then connects its uplink with ([`shrink`]). The ant is
//! retired all the same when that fails, the outcome's `uplink` saying how to drop it from the uplink.

use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, COLONY_BUCKET, EnrollReply, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use serde_json::json;

use super::add::{check_uplink, leaf_uplink};
use super::ask::{SIGNER_WAIT, Uplink, ask};
use super::{LeafNode, NO_CONFIG, Orders, applied, refused};
use crate::config;
use crate::creds::{self, Finding, Seed};
use crate::direct_kv::{DirectKv, Lookup};
use crate::keystore;

/// How long the daemon waits for the ant to leave the colony before revoking its key, as `ants retire` does.
const STOP_WAIT: Duration = Duration::from_mins(5);
const STOP_POLL: Duration = Duration::from_secs(1);

pub(super) async fn apply(js: &jetstream::Context, orders: &Orders, order: &AdminOrder) -> AdminOutcome {
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    let Some(host_creds) = &orders.creds else {
        return refused(order, "the machine has no host credentials to ask `ants signer` with");
    };
    let ant = &order.ant;
    let unlisted = match unlist(&config.path, host_creds, ant) {
        Ok(unlisted) => unlisted,
        Err(e) => return refused(order, &format!("{e:#}")),
    };
    if unlisted.listed {
        config.edited.notify_one();
    }
    let stopped = stopped(js, orders, ant, STOP_WAIT).await;
    let home = unlisted.home.display().to_string();
    let Some(key) = unlisted.key else {
        return applied(
            order,
            Some(json!({ "stopped": stopped, "revoked": [], "home": home, "note": "no credentials to revoke" })),
        );
    };
    // The retirement does not wait on the leaf node: without its configuration, the uplink is left as it is.
    let leaf = leaf_uplink(orders.leaf.as_ref()).and_then(|leaf| {
        leaf.map(|(leaf, uplink)| {
            let (asked, _) = creds::load(&uplink.creds, keystore::os())
                .with_context(|| format!("the uplink credentials of the leaf node {}", uplink.machine))?;
            Ok((leaf, uplink, asked))
        })
        .transpose()
    });
    let (leaf, leaf_error) = match leaf {
        Ok(leaf) => (leaf, None),
        Err(e) => (None, Some(format!("{e:#}"))),
    };
    let uplink = leaf.as_ref().map(|(_, uplink, _)| uplink);
    let (revoked, push_error, shrunk) = match ask(js, host_creds, uplink, order, ant, &key, SIGNER_WAIT).await {
        Ok(EnrollReply::Retired { revoked, push_error, leaf_jwt, leaf_error: why, .. }) => {
            (revoked, push_error, leaf_jwt.ok_or(why))
        }
        Ok(EnrollReply::Rejected { reason, .. }) => {
            let reason = reason.as_deref().unwrap_or("no reason given");
            return refused(order, &format!("`ants signer` refused to revoke {key}: {reason}; {}", hint(&key)));
        }
        Ok(EnrollReply::Approved { .. }) => {
            return refused(order, &format!("`ants signer` answered with an approval; {}", hint(&key)));
        }
        Err(e) => return refused(order, &format!("{e:#}; {}", hint(&key))),
    };
    if let Some(creds) = &unlisted.creds
        && let Err(e) = forget(creds)
    {
        tracing::warn!(ant = %ant, error = format!("{e:#}"), "deleting the credentials of a retired ant");
    }
    let uplink = match (leaf, leaf_error) {
        (Some((leaf, uplink, asked)), _) => Some(shrink(leaf, &uplink, &orders.accounts, ant, asked, shrunk).await),
        (None, Some(e)) => Some(format!("the uplink of the machine's leaf node is left as it is: {e}")),
        (None, None) => None,
    }
    .filter(|said| !said.is_empty());
    let creds = unlisted.creds.map(|c| c.display().to_string());
    let mut result = json!({
        "stopped": stopped,
        "revoked": revoked,
        "push_error": push_error,
        "creds": creds,
        "home": home,
    });
    if let Some(uplink) = uplink {
        result["uplink"] = uplink.into();
    }
    applied(order, Some(result))
}

/// Have the leaf node `leaf` use `shrunk`, the uplink credentials of `uplink` the signer renewed without `ant` from
/// `asked`, after checking them, or say why it did not renew them: what came of it, said in the order's outcome; empty
/// when they did not carry the ant.
async fn shrink(
    leaf: &LeafNode,
    uplink: &Uplink,
    accounts: &[String],
    ant: &UserId,
    asked: String,
    shrunk: Result<String, Option<String>>,
) -> String {
    let machine = &uplink.machine;
    let remedy = format!(
        "until a human runs `ants auth leaf {machine} --remove-ant {ant}` where the account key is and `ants leaf \
         outbox {machine} --remove-ant {ant}`, they keep {ant}'s rights"
    );
    let shrunk = match shrunk {
        Ok(shrunk) => shrunk,
        Err(None) => return String::new(),
        Err(Some(why)) => {
            return format!("the uplink credentials of {machine} were left as they are ({why}): {remedy}");
        }
    };
    if let Err(e) = check_uplink(&shrunk, accounts, uplink) {
        return format!("the uplink credentials of {machine} were left as they are ({e:#}): {remedy}");
    }
    let (apply, conf) = (leaf.shrink, leaf.conf.clone());
    match tokio::task::spawn_blocking(move || apply(&conf, &asked, &shrunk)).await.map_err(anyhow::Error::from) {
        Ok(Ok(Some(said))) => format!("the uplink credentials of {machine} were renewed without {ant}: {said}"),
        Ok(Ok(None)) => {
            format!("the uplink credentials of {machine} changed meanwhile and were left as they are: {remedy}")
        }
        Ok(Err(e)) | Err(e) => {
            format!("renewing the uplink credentials of {machine} without {ant} failed: {e:#}; {remedy}")
        }
    }
}

fn hint(key: &str) -> String {
    format!(
        "the ant is unlisted; its key is revoked with `ants auth revoke --key {key} --earlier` where the account key is"
    )
}

struct Unlisted {
    /// Whether the config listed the ant, so the table went now.
    listed: bool,
    creds: Option<PathBuf>,
    key: Option<String>,
    home: PathBuf,
}

/// Remove `ant` from the config at `path`: where its credentials are, their key and its home. An ant unlisted already,
/// its credentials left beside the host credentials `host_creds`, is retired from those.
fn unlist(path: &Path, host_creds: &Path, ant: &UserId) -> Result<Unlisted> {
    let loaded = config::load(path)?;
    let default_home = || -> Result<PathBuf> { Ok(config::home_dir()?.join(".ants").join(ant.as_str())) };
    let Some(entry) = loaded.ants.into_iter().find(|a| a.name == *ant) else {
        let left = host_creds.parent().map(|dir| dir.join(format!("{ant}.creds"))).filter(|c| c.exists());
        let Some(creds) = left else { bail!("config {} lists no ant {ant}", path.display()) };
        let key = key_of(&creds, ant)?;
        return Ok(Unlisted { listed: false, creds: Some(creds), key: Some(key), home: default_home()? });
    };
    if entry.builtin {
        bail!("{ant} is built in: it is turned off in the config, not retired");
    }
    let key = entry.creds.as_deref().map(|creds| key_of(creds, ant)).transpose()?;
    config::write(path, &without_mark(&config::without_ant(path, ant)?))?;
    let home = match entry.home {
        Some(home) => home,
        None => default_home()?,
    };
    Ok(Unlisted { listed: true, creds: entry.creds, key, home })
}

/// `text` without the comments of an add or clone order ([`super::add::is_mark`]) left above no table, as of the table cut
/// below one, and the blank lines that left at its end.
fn without_mark(text: &str) -> String {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let mut out = String::with_capacity(text.len());
    let mut cut = false;
    for (i, line) in lines.iter().enumerate() {
        if super::add::is_mark(line) && lines.get(i + 1).is_none_or(|next| next.trim() != "[[ant]]") {
            cut = true;
            continue;
        }
        out.push_str(line);
    }
    if cut {
        while out.ends_with("\n\n") {
            out.pop();
        }
    }
    out
}

/// The key of `ant`'s credentials at `creds`.
fn key_of(creds: &Path, ant: &UserId) -> Result<String> {
    let text = std::fs::read_to_string(creds).with_context(|| format!("reading credentials {}", creds.display()))?;
    let finding = Finding::from_creds(&text).with_context(|| format!("credentials {}", creds.display()))?;
    if finding.name != ant.as_str() {
        bail!("{} are the credentials of {}, not of {ant}", creds.display(), finding.name);
    }
    Ok(finding.key)
}

/// Wait up to `wait` for `ant` to leave the colony, as its daemon stopping it removes its entry; whether it did.
async fn stopped(js: &jetstream::Context, orders: &Orders, ant: &UserId, wait: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        match DirectKv::new(js, orders.js_domain.as_ref(), COLONY_BUCKET).await {
            Ok(colony) => match colony.lookup(ant.as_str()).await {
                Ok(Lookup::NoValue | Lookup::NoBucket) => return true,
                Ok(Lookup::Value(_)) => {}
                Err(e) => tracing::warn!(ant = %ant, error = format!("{e:#}"), "reading the colony"),
            },
            Err(e) => tracing::warn!(ant = %ant, error = format!("{e:#}"), "reading the colony"),
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(STOP_POLL).await;
    }
}

/// Delete the credentials file `path`, and the seeds it keeps in the OS credential store.
fn forget(path: &Path) -> Result<()> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    for seed in [creds::seed_of(&text), creds::xkey_of(&text)] {
        if let Some(Seed::Stored(key)) = seed {
            keystore::os().delete(key).with_context(|| format!("deleting the seed of {key}"))?;
        }
    }
    std::fs::remove_file(path).with_context(|| format!("removing {}", path.display()))
}
