//! The saved trails a run names: colony-wide in [`TRAILS_BUCKET`], which humans put, else in the machine's
//! `~/.ants/trails/<name>.py`. The bucket wins a name both have, so every conductor of the colony runs the same
//! trail by that name.

use std::path::PathBuf;

use ants_proto::{JsDomain, SavedTrail, TRAILS_BUCKET, TrailName};
use async_nats::jetstream;

use crate::records::direct_kv::DirectKv;

pub(super) struct Saved {
    kv: Option<DirectKv>,
    dir: Option<PathBuf>,
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
}
