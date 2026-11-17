//! [`AdminOp::Clone`]: add an ant under a new name, a copy of the order's ant with its home, sessions and thread
//! history, to the config file the daemon serves. The ant cloned is paused (unless it was already), and the daemon
//! serving its inbox reads a [`Snapshot`] once its last prompt ended; then its home is copied (no worktrees, no thread
//! history, no answered or buried messages, no lifecycle events), each thread's history written into the clone's home
//! ([`crate::stream::history::cloned`]), each worktree added again on the branch `ants/<new>/<thread>` from the same commit,
//! with its uncommitted changes and untracked files, the clone's key signed by `ants signer` on the order, each session
//! forked when the harness can (else marked to start over, [`crate::stream::history::Rebuild::Cloned`]) and stored under the
//! clone's keys, and its identity record written: a new id and birth, the prompts of the ant cloned and where it came
//! from ([`ants_proto::ClonedFrom`]), and the clone's provider is to be told it was cloned ([`events::arrived`]). Where
//! the hub keeps shared settings ([`crate::profile::shared`]), the clone gets its own record of them: those of the ant
//! cloned, with the order's settings over them. Its `[[ant]]` table, a copy of the cloned ant's with its shared
//! settings and the order's settings over it, goes last, which starts it on the reload, and the ant cloned resumes. A
//! failure undoes what was done, the signed key aside, which no one holds once the credentials are deleted. A comment
//! above the table names the order, so that the order taken again after the table was written is applied without
//! cloning twice.
//!
//! Not cloned: the messages queued for the ant, its channel memberships, references and spend counters.

use std::path::Path;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::provider::EventKind;
use ants_proto::{AdminOrder, AdminOutcome, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use serde_json::{Map, Value, json};

use super::add::{leaf_uplink, renew};
use super::{ConfigFile, NO_CONFIG, Orders, applied, refused};
#[cfg(doc)]
use crate::clone::Snapshot;
use crate::config;
use crate::profile::shared::{self, Bucket, Found};
use crate::provider::events;

mod files;
mod keys;
mod pause;
mod plan;
mod store;
mod undo;
mod worktrees;

use files::copy_files;
use pause::resume;
use plan::{Plan, plan};
use store::record_summary;
use undo::Done;

pub(super) async fn apply(
    js: &jetstream::Context,
    bucket: Bucket<'_>,
    orders: &Orders,
    order: &AdminOrder,
    new: &UserId,
    settings: &Map<String, Value>,
) -> AdminOutcome {
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    let Some(host_creds) = &orders.creds else {
        return refused(order, "the machine has no host credentials to ask `ants signer` with");
    };
    let mut done = Done::default();
    let cloning = Cloning { js, bucket, orders, config, host_creds, order, new, settings };
    let result = cloning.run(&mut done).await;
    let (paused, key) = (done.paused, done.key.take());
    if result.is_err() {
        done.undo(order).await;
    }
    if paused {
        resume(config, &order.ant);
    }
    match result {
        Ok(None) => applied(order, None),
        Ok(Some(result)) => {
            config.edited.notify_one();
            applied(order, Some(result))
        }
        Err(e) => match key {
            Some(key) => refused(
                order,
                &format!(
                    "{e:#}; all undone, but the clone's key {key} that `ants signer` signed stays valid, held by no \
                     one, until `ants auth revoke --key {key}` where the account key is"
                ),
            ),
            None => refused(order, &format!("{e:#}; all undone")),
        },
    }
}

/// A clone order being carried out, with what it is carried out with.
struct Cloning<'a> {
    js: &'a jetstream::Context,
    bucket: Bucket<'a>,
    orders: &'a Orders,
    config: &'a ConfigFile,
    /// The machine's host credentials, which `ants signer` is asked with.
    host_creds: &'a Path,
    order: &'a AdminOrder,
    new: &'a UserId,
    settings: &'a Map<String, Value>,
}

impl Cloning<'_> {
    /// Carry out the clone of the order's ant as `new`, noting in `done` what to undo: what it did, none when the
    /// config shows it done already.
    async fn run(&self, done: &mut Done) -> Result<Option<Value>> {
        let (src, path, new) = (&self.order.ant, &self.config.path, self.new);
        let src_shared = match shared::read(self.bucket, src).await? {
            Found::Record(record, _) => Some(Some(record.fields)),
            Found::None(_) | Found::Malformed(..) | Found::Unverified(..) => Some(None),
            Found::NoBucket => None,
        };
        let Some(plan) = plan(path, self.host_creds, self.order, new, self.settings, src_shared)? else {
            return Ok(None);
        };
        if !self.orders.restarts.hears_clones(src) {
            bail!("{} serves no inbox of {src} to clone", self.orders.machine);
        }
        let leaf = leaf_uplink(self.orders.leaf.as_ref())?;
        let snapshot = self.snapshot(done).await?;
        let (trees, branches) = copy_files(&plan, &snapshot, new, done).await?;
        let signed = self.sign(leaf, &plan, done).await?;
        let (forked, restart, record) = self.store(&plan, &snapshot, &trees, done).await?;
        if let Some(from) = &record.cloned_from {
            let cloned = EventKind::AntCloned { from: from.ant.clone(), from_identity: from.id.clone() };
            events::arrived(&plan.new_home, new, &record.id, vec![cloned])?;
        }

        let Plan { table, workdir, new_home, creds_path, shared, .. } = plan;
        if workdir.starts_with(&new_home) {
            std::fs::create_dir_all(&workdir).with_context(|| format!("creating {}", workdir.display()))?;
        }
        // Before listing the ant, so its first connection finds the uplink carrying it.
        let uplink = match signed.renewal {
            Some((leaf, machine, renewed)) => Some(renew(leaf, &machine, new, renewed).await),
            None => None,
        };
        if let Some(fields) = shared {
            self.share(fields, &signed.jwt, &signed.key, done).await?;
        }
        config::append(path, &table)?;
        if let Err(e) = std::fs::remove_file(&signed.seed_path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %signed.seed_path.display(), error = %e, "removing the key of a cloned ant");
        }
        let mut result = json!({
            "creds": creds_path,
            "key": signed.public_key,
            "home": new_home,
            "threads": snapshot.threads.len(),
            "forked": forked,
            "restarted": restart,
            "worktrees": branches,
            "identity": record_summary(&record),
        });
        if let Some(uplink) = uplink {
            result["uplink"] = Value::String(uplink);
        }
        Ok(Some(result))
    }
}
