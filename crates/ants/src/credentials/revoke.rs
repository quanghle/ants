//! Revoking issued credentials before they expire.
//!
//! Revocations live in [`REVOKED`]; the account JWT in `server.conf` carries them as NATS revocations (a user key and
//! a time: JWTs of that key issued at or before it are refused), and [`push`](super::push::push) sends that account
//! JWT to a running server through the system account, which closes the revoked users' connections at once. The
//! update reaches every leaf node connected to that server through a system account uplink (`ants leaf config`),
//! which applies it too and keeps it across restarts.

use std::fs;
use std::path::Path;

use ants_daemon::config::registration::Grant;
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use super::issued::{ISSUED, Issued, Kind, issued, latest, record};
use crate::auth;

/// The revocations in the auth directory.
pub const REVOKED: &str = "revoked.json";

/// One revoked user key, an entry of [`REVOKED`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revoked {
    pub key: String,
    /// Seconds since the epoch: JWTs of the key issued at or before it are refused.
    pub revoked_at: u64,
    /// The name its credentials carry, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// When the latest credentials known for the key expire; none when they never do, or are unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

impl Revoked {
    /// Whether every known credential of the key has expired at `now`, so the revocation is no longer needed.
    fn expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now)
    }
}

/// The revocations of `dir`, oldest first.
pub fn revoked(dir: &Path) -> Result<Vec<Revoked>> {
    let path = dir.join(REVOKED);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("reading {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn write_revoked(dir: &Path, revoked: &[Revoked]) -> Result<()> {
    let path = dir.join(REVOKED);
    let mut text = serde_json::to_string_pretty(revoked)?;
    text.push('\n');
    fs::write(&path, text).with_context(|| format!("writing {}", path.display()))
}

/// Revoke every key the registry of `dir` knows for `name`, and each of `keys`, then write `server.conf` again ([`apply`]). Returns the keys revoked, some perhaps already expired;
/// fails without a change when `name` has no known key.
pub fn revoke(dir: &Path, name: Option<&str>, keys: &[String]) -> Result<Vec<Revoked>> {
    let now = auth::now()?;
    let issued = issued(dir)?;
    let mut targets: Vec<Revoked> = Vec::new();
    let add = |targets: &mut Vec<Revoked>, key: &str, name: Option<&str>, expires_at: Option<u64>| {
        match targets.iter_mut().find(|t| t.key == key) {
            // Of several credentials of one key, the revocation lasts until the last of them expires.
            Some(known) => known.expires_at = known.expires_at.zip(expires_at).map(|(a, b)| a.max(b)),
            None => targets.push(Revoked {
                key: key.to_owned(),
                revoked_at: now,
                name: name.map(str::to_owned),
                expires_at,
            }),
        }
    };
    if let Some(name) = name {
        for one in issued.iter().filter(|i| i.name == name) {
            add(&mut targets, &one.key, Some(name), one.expires_at);
        }
        if targets.is_empty() {
            bail!(
                "no credentials of {name} known in {}; revoke them by public key with --key U...",
                dir.join(ISSUED).display()
            );
        }
    }
    for key in keys {
        auth::check_user_key(key)?;
        let known: Vec<&Issued> = issued.iter().filter(|i| i.key == *key).collect();
        match known.as_slice() {
            [] => add(&mut targets, key, None, None),
            known => {
                for one in known {
                    add(&mut targets, key, Some(&one.name), one.expires_at);
                }
            }
        }
    }
    let mut all = revoked(dir)?;
    all.retain(|r| !targets.iter().any(|t| t.key == r.key));
    all.extend(targets.iter().cloned());
    write_revoked(dir, &all)?;
    apply_at(dir, now)?;
    if let Some(name) = name {
        withdraw_approvals(dir, &issued, name, now)?;
    }
    Ok(targets)
}

/// Revoke by name, as [`revoke`] does, every assistant of `human` ([`Issued::assistant_of`]) holding a live key: its
/// dedicated assistant, which acts for no one once its human is revoked whole. Returns the keys revoked.
pub fn revoke_assistants(dir: &Path, human: &str) -> Result<Vec<Revoked>> {
    let now = auth::now()?;
    let gone: Vec<String> = revoked(dir)?.into_iter().map(|r| r.key).collect();
    let mut names: Vec<String> = issued(dir)?
        .into_iter()
        .filter(|i| i.kind == Kind::Assistant && i.assistant_of.as_ref().is_some_and(|of| of.as_str() == human))
        .filter(|i| !gone.contains(&i.key) && i.expires_at.is_none_or(|exp| exp > now))
        .map(|i| i.name)
        .collect();
    names.sort_unstable();
    names.dedup();
    let mut all = Vec::new();
    for name in names {
        all.extend(revoke(dir, Some(&name), &[])?);
    }
    Ok(all)
}

/// Withdraw at `now` the approval grants of the keys of `name` in the registry `issued` of `dir`: append again the
/// latest line of each key a human's approval registered, marked [`Issued::withdrawn`]. Its GitHub binding stays.
fn withdraw_approvals(dir: &Path, issued: &[Issued], name: &str, now: u64) -> Result<()> {
    let approved = |i: &&&Issued| matches!(i.grant, Some(Grant::Approval(_))) && i.withdrawn.is_none();
    for one in latest(issued).iter().filter(|i| i.name == name).filter(approved) {
        record(dir, &Issued { withdrawn: Some(now), ..(*one).clone() })?;
    }
    Ok(())
}

/// Drop the revocations whose credentials have all expired, and write the `server.conf` of `dir` with the rest.
/// Returns the public keys and JWTs of the accounts to push, in order ([`auth::write_server_conf`]).
pub fn apply(dir: &Path) -> Result<[auth::AccountJwt; 3]> {
    apply_at(dir, auth::now()?)
}

/// [`apply`] at `now`, in seconds since the epoch.
fn apply_at(dir: &Path, now: u64) -> Result<[auth::AccountJwt; 3]> {
    let mut all = revoked(dir)?;
    let before = all.len();
    all.retain(|r| !r.expired(now));
    if all.len() != before {
        write_revoked(dir, &all)?;
    }
    auth::write_server_conf(dir, &revocations(dir, all)?)
}

/// The accounts' revocations of `revoked`: a key the registry of `dir` records as a leaf node's store account uplink
/// ([`Kind::LeafStore`]) in the store account, one it records as a leaf node's system account uplink
/// ([`Kind::LeafSystem`]) in the system account, one it records otherwise in the team's, and one it does not know in
/// all three.
fn revocations(dir: &Path, revoked: Vec<Revoked>) -> Result<auth::Revocations> {
    let issued = issued(dir)?;
    let mut revocations = auth::Revocations::default();
    for r in revoked {
        let kinds: Vec<Kind> = issued.iter().filter(|i| i.key == r.key).map(|i| i.kind).collect();
        let known = !kinds.is_empty();
        let in_account = |of: &dyn Fn(Kind) -> bool| !known || kinds.iter().any(|&kind| of(kind));
        if in_account(&|kind| kind == Kind::LeafStore) {
            revocations.store.insert(r.key.clone(), r.revoked_at);
        }
        if in_account(&|kind| kind == Kind::LeafSystem) {
            revocations.system.insert(r.key.clone(), r.revoked_at);
        }
        if in_account(&|kind| !matches!(kind, Kind::LeafStore | Kind::LeafSystem)) {
            revocations.team.insert(r.key, r.revoked_at);
        }
    }
    Ok(revocations)
}

#[cfg(test)]
mod tests;
