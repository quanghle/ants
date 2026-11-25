//! The clone's records on the bus: its sessions, its threads' origins, its identity record and its shared settings.

use std::path::{Path, PathBuf};

use ants_proto::{AntIdentity, ClonedFrom, SharedSettings, UserId};
use anyhow::Result;
use chrono::Utc;
use nkeys::KeyPair;
use serde_json::{Map, Value, json};

use super::Cloning;
use super::plan::Plan;
use super::undo::Done;
use super::worktrees::Tree;
use crate::clone::{Snapshot, ThreadCopy};
use crate::pool::{SessionStore, Stored};
use crate::profile::identity::{self, Identity};
use crate::profile::shared;
use crate::provider::origins::OriginStore;
use crate::{bus, harness};

impl Cloning<'_> {
    /// Store the clone's sessions ([`store_sessions`]), its threads' origins and its identity record, with its
    /// credentials: the threads forked, those that start over, and the record.
    pub(super) async fn store(
        &self,
        plan: &Plan,
        snapshot: &Snapshot,
        trees: &[Tree],
        done: &mut Done,
    ) -> Result<(Vec<String>, Vec<String>, AntIdentity)> {
        let (new, domain) = (self.new, self.orders.js_domain.as_ref());
        let (_client, new_js) = bus::connect(&self.orders.nats, Some(&plan.creds_path), domain, new, "clone").await?;
        let kv = bus::ensure_sessions(&new_js, domain, new).await?;
        let store = SessionStore::kv(kv.clone(), new.clone());
        let (forked, restart) = store_sessions(&store, snapshot, trees, &plan.new_home, &plan.workdir, done).await?;
        store_origins(&OriginStore::kv(kv, new.clone()), snapshot, done).await?;

        let identity = Identity::new(&new_js, domain, new).await?;
        let record = born(new, &self.order.ant, snapshot.identity.as_ref())?;
        done.identity = Some(identity.clone());
        identity.replace(&record).await?;
        Ok((forked, restart, record))
    }

    /// Write the clone's shared settings, `fields`, signed with its credentials `jwt` and `key`.
    pub(super) async fn share(
        &self,
        fields: Map<String, Value>,
        jwt: &str,
        key: &KeyPair,
        done: &mut Done,
    ) -> Result<()> {
        let mut record = SharedSettings::new(self.new.clone(), fields, self.orders.machine.clone(), None);
        done.shared = Some((self.bucket.identities.clone(), record.key()));
        let signer = shared::Signer::new(jwt, key.clone());
        shared::write(self.bucket.identities, &mut record, &signer, None).await?;
        self.config.shared.store(record);
        Ok(())
    }
}

/// Store in `store`, the clone's, a session for each thread of `snapshot`, forked in the thread's worktree among
/// `trees`, added again in `new_home`, else in `workdir`, or marked to start over, with the directory it works in: the
/// threads forked, and those that start over.
async fn store_sessions(
    store: &SessionStore,
    snapshot: &Snapshot,
    trees: &[Tree],
    new_home: &Path,
    workdir: &Path,
    done: &mut Done,
) -> Result<(Vec<String>, Vec<String>)> {
    let (mut forked, mut restart) = (Vec::new(), Vec::new());
    for copy in &snapshot.threads {
        let cwd = match trees.iter().find(|t| t.thread == copy.thread) {
            Some(tree) => tree.session_dir(new_home, copy.stored.cwd.as_deref()),
            None => workdir.to_owned(),
        };
        let stored = session(snapshot.launch.as_ref(), copy, cwd).await;
        if stored.cloned {
            restart.push(copy.thread.to_string());
        } else {
            forked.push(copy.thread.to_string());
        }
        done.sessions.get_or_insert_with(|| (store.clone(), Vec::new())).1.push(copy.thread.clone());
        store.put(&copy.thread, stored).await?;
    }
    Ok((forked, restart))
}

/// Store in `store`, the clone's, where each thread of `snapshot` came from, so that the clone carries on with the
/// origin each thread settled on rather than settling it again from its first message there.
async fn store_origins(store: &OriginStore, snapshot: &Snapshot, done: &mut Done) -> Result<()> {
    for (thread, origin) in &snapshot.origins {
        done.origins.get_or_insert_with(|| (store.clone(), Vec::new())).1.push(thread.clone());
        store.put(thread, origin).await?;
    }
    Ok(())
}

/// The identity record of `new`, a clone of `src`, whose record is `src_identity`.
fn born(new: &UserId, src: &UserId, src_identity: Option<&AntIdentity>) -> Result<AntIdentity> {
    let mut record = identity::born(new)?;
    if let Some(src_identity) = src_identity {
        record.prompts = src_identity.prompts;
        record.cloned_from = Some(ClonedFrom { ant: src.clone(), id: src_identity.id.clone(), at: Utc::now() });
    }
    Ok(record)
}

pub(super) fn record_summary(record: &AntIdentity) -> Value {
    json!({ "id": record.id, "prompts": record.prompts, "cloned_from": record.cloned_from })
}

/// The session of the thread of `copy` for the clone: forked with `launch` in `cwd` if its harness can, else marked to
/// start over.
async fn session(launch: Option<&harness::Launch>, copy: &ThreadCopy, cwd: PathBuf) -> Stored {
    let restart = Stored { cloned: true, cwd: Some(cwd.clone()), ..copy.stored.clone() };
    let Some(launch) = launch else { return restart };
    if launch.harness_name() != copy.stored.harness {
        return restart;
    }
    match harness::fork(&launch.clone().in_dir(cwd.clone()), &copy.stored.session).await {
        Ok(Some(session)) => Stored { session, harness: copy.stored.harness.clone(), cwd: Some(cwd), cloned: false },
        Ok(None) => restart,
        Err(e) => {
            tracing::warn!(thread = %copy.thread, error = format!("{e:#}"), "forking a session for a clone");
            restart
        }
    }
}
