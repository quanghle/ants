//! Forking an identity on clone (requirement ants-x39hk.6.4): when ants clones an ant, the clone's sense identity, a
//! new one with its own id, takes a copy of what its source knew as the clone was made. It keeps the items of the
//! kinds in [`KEPT`], each under its own id, so links between them hold, and each marked [`INHERITED`] from the
//! source's identity; it drops every other kind: tasks, commitments, reminders, relationship records, resume
//! registers, handoffs, votes, persona and skill proposals, dreams and the card, the clone's own being made from the
//! clone event.

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::identity;
use crate::memory::MEMORY;
use crate::persona::PERSONA;
use crate::records::SELF_MODEL;
use crate::skill::SKILL;
use crate::store::{Items, StoreError, Write};

/// The value of an item a clone inherited: the sense id of the identity it was copied from.
pub const INHERITED: &str = "inherited";
/// The kinds of items a clone inherits: memories, self-model, persona and skills.
pub const KEPT: &[&str] = &[MEMORY, SELF_MODEL, PERSONA, SKILL];

/// The most items one event of the clone's log copies.
const BATCH_ITEMS: usize = 100;
/// About the most bytes of values one event of the clone's log copies.
const BATCH_BYTES: usize = 256 * 1024;

/// Copy into `clone` the items of the kinds in [`KEPT`] in `source` as they were at `at`, when the clone was made,
/// marked [`INHERITED`]; those `clone` holds already, copied by an earlier try or written since, are left alone, so a
/// fork tried again copies only what it has not. Returns how many items it copied.
pub async fn fork(source: &dyn Items, clone: &dyn Items, at: DateTime<Utc>) -> Result<usize, StoreError> {
    let from = source.sense().await;
    let (mut batch, mut bytes, mut forked) = (Vec::new(), 0, 0);
    for kind in KEPT {
        for item in source.list(kind, usize::MAX).await? {
            let item = if item.updated.at <= at { Some(item) } else { source.at(&item.id, at).await? };
            let Some(item) = item else { continue };
            if clone.get(&item.id).await?.is_some() {
                continue;
            }
            let mut values = item.values;
            values.insert(INHERITED.into(), json!(from));
            bytes += serde_json::to_vec(&values).map_or(0, |encoded| encoded.len());
            batch.push(Write::Create { id: item.id, kind: item.kind, values });
            if batch.len() >= BATCH_ITEMS || bytes >= BATCH_BYTES {
                forked += clone.write(&std::mem::take(&mut batch), identity::BY).await?.len();
                bytes = 0;
            }
        }
    }
    if !batch.is_empty() {
        forked += clone.write(&batch, identity::BY).await?.len();
    }
    Ok(forked)
}

#[cfg(test)]
mod tests;
