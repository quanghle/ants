//! The saved trails a run names: colony-wide in [`TRAILS_BUCKET`], which humans put, else in the machine's
//! `~/.ants/trails/<name>.py`. The bucket wins a name both have, so every conductor of the colony runs the same
//! trail by that name.

use std::path::PathBuf;

use ants_proto::{JsDomain, SavedTrail, TRAILS_BUCKET, TrailName, script_sha256};
use async_nats::jetstream;

use crate::records::direct_kv::DirectKv;

pub(super) struct Saved {
    kv: Option<DirectKv>,
    dir: Option<PathBuf>,
}

/// A saved trail, as a search for trails reads it.
#[derive(Debug, Clone)]
pub struct SavedEntry {
    pub name: TrailName,
    pub description: String,
    /// The [`script_sha256`] of its script.
    pub script: String,
    /// Who saved it; none for a trail local to this machine.
    pub saver: Option<String>,
    /// Whether it is this machine's, in `~/.ants/trails`, the colony having none of its name.
    pub local: bool,
}

impl Saved {
    pub(super) async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>, dir: Option<PathBuf>) -> Self {
        let kv = match DirectKv::whole(js, js_domain, TRAILS_BUCKET).await {
            Ok(kv) => Some(kv),
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "reading no saved trails of the colony");
                None
            }
        };
        Self { kv, dir }
    }

    /// The script of the trail saved as `name`.
    pub(super) async fn script(&self, name: &TrailName) -> Result<String, String> {
        let mut unread = None;
        if let Some(kv) = &self.kv {
            match kv.get(name.as_str()).await {
                Ok(Some(value)) => {
                    let saved: SavedTrail =
                        serde_json::from_slice(&value).map_err(|e| format!("reading saved trail {name}: {e}"))?;
                    saved.check_key(name.as_str()).map_err(|e| e.to_string())?;
                    return Ok(saved.script);
                }
                Ok(None) => {}
                Err(e) => unread = Some(format!("{e:#}")),
            }
        }
        if let Some(dir) = &self.dir {
            let path = dir.join(format!("{name}.py"));
            match tokio::fs::read_to_string(&path).await {
                Ok(script) => return Ok(script),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("reading {}: {e}", path.display())),
            }
        }
        Err(match unread {
            Some(e) => format!("no trail is saved as {name} (the saved trails of the colony are unread: {e})"),
            None => format!("no trail is saved as {name}"),
        })
    }

    /// Every saved trail, the colony's then this machine's whose names the colony has not, each by name. Fails when the
    /// colony's are unread; a local trail that is unread or no trail is left out.
    pub(super) async fn all(&self) -> Result<Vec<SavedEntry>, String> {
        let mut all = Vec::new();
        if let Some(kv) = &self.kv {
            kv.scan(">", |key, value| match serde_json::from_slice::<SavedTrail>(value) {
                Ok(trail) if trail.check_key(key).is_ok() => all.push(SavedEntry {
                    saver: Some(saver(&trail)),
                    script: script_sha256(&trail.script),
                    name: trail.name,
                    description: trail.description,
                    local: false,
                }),
                _ => tracing::debug!(key, "skipping a saved trail that does not read"),
            })
            .await
            .map_err(|e| format!("the saved trails of the colony are unread: {e:#}"))?;
        }
        if let Some(dir) = &self.dir {
            all.extend(local(dir, &all).await);
        }
        all.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(all)
    }
}

/// Who saved `trail`, as a search for trails shows it.
fn saver(trail: &SavedTrail) -> String {
    trail.saved_by.to_string()
}

/// The trails of `dir`, `<name>.py` files that are trails, but for those of a name in `colony`.
async fn local(dir: &std::path::Path, colony: &[SavedEntry]) -> Vec<SavedEntry> {
    let mut found = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else { return found };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "py") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|stem| stem.to_str()?.parse::<TrailName>().ok()) else { continue };
        if colony.iter().any(|trail| trail.name == name) {
            continue;
        }
        let Ok(script) = tokio::fs::read_to_string(&path).await else { continue };
        let Some(description) = super::script_description(&script) else { continue };
        found.push(SavedEntry { name, description, script: script_sha256(&script), saver: None, local: true });
    }
    found
}
