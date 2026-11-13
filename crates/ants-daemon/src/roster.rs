//! The persona's entry in the roster, kept alive by heartbeats.

use std::time::Duration;

use ants_proto::{PersonaId, ROSTER_HEARTBEAT, ROSTER_TTL, RosterEntry};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, kv};
use chrono::Utc;
use futures::TryStreamExt as _;
use tokio::task::JoinHandle;

use crate::bus;
use crate::daemon::Config;

/// A persona listed in the roster. Dropping it stops the heartbeats, so the entry expires;
/// [`Registration::deregister`] removes it at once.
pub struct Registration {
    kv: kv::Store,
    key: String,
    heartbeat: JoinHandle<()>,
}

/// List the persona of `config` in the roster and keep its entry fresh until the registration ends.
pub async fn register(js: &jetstream::Context, config: &Config) -> Result<Registration> {
    let kv = bus::ensure_roster(js, ROSTER_TTL).await?;
    let now = Utc::now();
    let entry = RosterEntry {
        persona: config.persona.clone(),
        machine: machine().await,
        harness: config.harness.to_string(),
        capabilities: config.capabilities.clone(),
        workdir: Some(config.workdir.display().to_string()),
        started_at: now,
        heartbeat_at: now,
    };
    Registration::start(kv, entry, ROSTER_HEARTBEAT).await
}

impl Registration {
    /// Put `entry` now, then again every `every`. The first put must succeed; later failures are logged and retried
    /// on the next beat, so a brief outage costs nothing as long as it is shorter than the bucket's max age.
    pub async fn start(kv: kv::Store, mut entry: RosterEntry, every: Duration) -> Result<Self> {
        let key = entry.key().to_owned();
        put(&kv, &entry).await.with_context(|| format!("registering {key} in the roster"))?;
        let beat_kv = kv.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(every);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticks.tick().await;
            loop {
                ticks.tick().await;
                entry.heartbeat_at = Utc::now();
                if let Err(e) = put(&beat_kv, &entry).await {
                    tracing::warn!(persona = %entry.persona, error = format!("{e:#}"), "roster heartbeat");
                }
            }
        });
        Ok(Self { kv, key, heartbeat })
    }

    /// Stop the heartbeats and remove the entry.
    pub async fn deregister(self) {
        self.heartbeat.abort();
        if let Err(e) = self.kv.delete(&self.key).await {
            tracing::warn!(persona = %self.key, error = %e, "leaving the roster; the entry expires on its own");
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

/// The entries in the roster, by persona. An entry removed while listing is left out, and a malformed one is skipped
/// with a warning.
pub async fn list_entries(kv: &kv::Store) -> Result<Vec<RosterEntry>> {
    let mut keys: Vec<String> = kv.keys().await.context("listing the roster")?.try_collect().await?;
    keys.sort();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(value) = kv.get(&key).await.with_context(|| format!("reading {key} in the roster"))? else { continue };
        match serde_json::from_slice(&value) {
            Ok(entry) => entries.push(entry),
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed roster entry"),
        }
    }
    Ok(entries)
}

/// Whether `entry` advertises `capability`. Tags compare whole, ignoring case and surrounding space.
pub fn has_capability(entry: &RosterEntry, capability: &str) -> bool {
    let capability = capability.trim();
    entry.capabilities.iter().any(|c| c.trim().eq_ignore_ascii_case(capability))
}

/// The persona a request needing `capability` goes to: of the entries other than `asking` that advertise it, the
/// first by persona id, so the same roster always gives the same answer.
pub fn pick<'a>(entries: &'a [RosterEntry], capability: &str, asking: &PersonaId) -> Option<&'a RosterEntry> {
    entries
        .iter()
        .filter(|e| &e.persona != asking && has_capability(e, capability))
        .min_by(|a, b| a.persona.cmp(&b.persona))
}

async fn put(kv: &kv::Store, entry: &RosterEntry) -> Result<()> {
    kv.put(entry.key(), serde_json::to_vec(entry)?.into()).await?;
    Ok(())
}

/// Host name of this machine, as `hostname` prints it.
async fn machine() -> String {
    let output = tokio::process::Command::new("hostname").output().await;
    match output {
        Ok(out) if out.status.success() => {
            let name = String::from_utf8_lossy(&out.stdout).trim().to_owned();
            if !name.is_empty() {
                return name;
            }
        }
        _ => {}
    }
    std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")).unwrap_or_else(|_| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(persona: &str, capabilities: &[&str]) -> RosterEntry {
        let now = Utc::now();
        RosterEntry {
            persona: persona.parse().unwrap(),
            machine: "m".to_owned(),
            harness: "copilot".to_owned(),
            capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
            workdir: None,
            started_at: now,
            heartbeat_at: now,
        }
    }

    #[test]
    fn pick_is_the_first_other_persona_with_the_whole_tag() {
        let entries = [
            entry("ops", &["rust-ops"]),
            entry("dev-c", &[" Rust "]),
            entry("dev-b", &["rust"]),
            entry("dev-a", &["rust"]),
        ];
        let me: PersonaId = "dev-a".parse().unwrap();
        let picked = |capability| pick(&entries, capability, &me).map(|e| e.persona.as_str());
        assert_eq!(picked("rust"), Some("dev-b"));
        assert_eq!(picked("RUST "), Some("dev-b"));
        assert_eq!(picked("rust-ops"), Some("ops"));
        assert_eq!(picked("ops"), None, "tags match whole, not in part");
        assert_eq!(pick(&entries, "rust", &"dev-b".parse().unwrap()).map(|e| e.persona.as_str()), Some("dev-a"));
    }
}
