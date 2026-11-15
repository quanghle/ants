//! [`AdminOp::Set`]: change fields of the ant's settings, all or none, each only if it holds the value the order
//! expects ([`Edit::change`]).
//!
//! - The ant's shared settings ([`SHARED_FIELDS`]) change in their record ([`crate::shared`]), by compare-and-set, each
//!   set whole: a field the record leaves out holds what the machine's config gives it while the ant has no record.
//!   Where the machine's config sets such a field too, it gets the new value as well, so the machine shows no drift.
//! - Every other field changes in the ant's `[[ant]]` table of the config file the daemon serves, as do all of them
//!   where the hub has no bucket for shared settings.
//!
//! The file as edited, served with the shared settings as changed, must load before either is written; the file is
//! written over the old one after the record ([`Edit::save`]), and the daemon reloads at once ([`ConfigFile::edited`],
//! [`crate::shared::Shared::subscribe`]). An order taken again after its changes were written, its outcome not stored,
//! finds its fields holding their new values and is applied without writing again.

#[cfg(doc)]
use ants_proto::{AdminOp, SHARED_FIELDS};
use ants_proto::{AdminOrder, AdminOutcome, FieldChange, IDENTITIES_BUCKET, SharedSettings, is_shared_field};
use anyhow::{Result, bail};

use super::{ConfigFile, NO_CONFIG, Orders, applied, refused};
use crate::config::edit::{Edit, needed};
use crate::direct_kv::DirectKv;
use crate::shared::{self, Found, Roles};

/// How often a change of the shared settings is tried while they keep changing meanwhile.
const TRIES: usize = 3;
/// The server an overlaid ant is checked with when its table names none: any address that parses.
const ANY_NATS: &str = "nats://127.0.0.1:4222";

/// Carry out the order, refusing it when its changes do not apply; fails, for the order to be taken again, only when
/// the shared settings cannot be read or written.
pub(super) async fn apply(
    identities: &DirectKv,
    orders: &Orders,
    order: &AdminOrder,
    changes: &[FieldChange],
) -> Result<AdminOutcome> {
    let Some(config) = &orders.config else { return Ok(refused(order, NO_CONFIG)) };
    let shares = changes.iter().any(|c| c.field.first().is_some_and(|key| is_shared_field(key)));
    let edited = if shares {
        edit_shared(identities, config, orders, order, changes).await?
    } else {
        edit(config, order, changes)
    };
    Ok(match edited {
        Ok(written) => {
            if written {
                config.edited.notify_one();
            }
            applied(order, None)
        }
        Err(e) => refused(order, &format!("{e:#}")),
    })
}

/// Make `changes` to the ant's table in the config file, returning whether the file was written.
fn edit(config: &ConfigFile, order: &AdminOrder, changes: &[FieldChange]) -> Result<bool> {
    let mut file = Edit::open(&config.path)?;
    if !file.change(&order.ant, changes)? {
        return Ok(false);
    }
    file.save()?;
    Ok(true)
}

/// Make `changes` to the ant's shared settings and its table in the config file: the outer result fails when the
/// record cannot be read or written, the inner one with why the changes do not apply, else holds whether the file was
/// written.
async fn edit_shared(
    identities: &DirectKv,
    config: &ConfigFile,
    orders: &Orders,
    order: &AdminOrder,
    changes: &[FieldChange],
) -> Result<Result<bool>> {
    for _ in 0..TRIES {
        let found = shared::read(identities, &order.ant).await?;
        if matches!(found, Found::NoBucket) {
            return Ok(edit(config, order, changes));
        }
        match prepare(config, orders, order, changes, found) {
            Err(e) => return Ok(Err(e)),
            Ok(Prepared { record, last, file }) => {
                if let Some(record) = record {
                    if !identities.update(record.key(), serde_json::to_vec(&record)?, last).await? {
                        tracing::debug!(ant = %order.ant, "the shared settings changed meanwhile; trying again");
                        continue;
                    }
                    tracing::info!(ant = %order.ant, admin = %order.admin, "changed the ant's shared settings");
                    config.shared.store(record);
                }
                return Ok(match file {
                    Some(file) => file.save().map(|_| true),
                    None => Ok(false),
                });
            }
        }
    }
    Ok(Err(anyhow::anyhow!("the ant's shared settings kept changing meanwhile; send the order again")))
}

/// What a settings order changes: the shared settings to store, when they change, over the stream sequence they were
/// read at, and the config file to write, when it changes.
struct Prepared {
    record: Option<SharedSettings>,
    last: u64,
    file: Option<Edit>,
}

fn prepare(
    config: &ConfigFile,
    orders: &Orders,
    order: &AdminOrder,
    changes: &[FieldChange],
    found: Found,
) -> Result<Prepared> {
    let ant = &order.ant;
    let mut file = Edit::open(&config.path)?;
    let loaded = file.check()?;
    let Some(listed) = loaded.ants.iter().find(|a| a.name == *ant) else {
        bail!("{} lists no ant {ant}", config.path.display());
    };
    let (before, last) = match found {
        Found::Record(record, last) => (Some(record.fields), last),
        Found::Malformed(_, last) | Found::None(last) => (None, last),
        Found::NoBucket => bail!("there is no bucket {IDENTITIES_BUCKET} for the ant's shared settings"),
    };
    let mut fields = before.clone().unwrap_or_else(|| shared::local_fields(listed));
    let mut file_changes = Vec::new();
    for change in changes {
        let Some(key) = change.field.first().filter(|key| is_shared_field(key)) else {
            file_changes.push(change.clone());
            continue;
        };
        if change.field.len() != 1 {
            bail!("{} is one of the ant's shared settings, set whole: set {key}", change.field.join("."));
        }
        if needed(change, fields.get(key))? {
            match &change.value {
                Some(value) => fields.insert(key.clone(), value.clone()),
                None => fields.remove(key),
            };
        }
        if let Some(set) = file.get(ant, &change.field)? {
            file_changes.push(FieldChange {
                field: change.field.clone(),
                expected: Some(set),
                expected_digest: None,
                value: change.value.clone(),
            });
        }
    }
    let file_changed = !file_changes.is_empty() && file.change(ant, &file_changes)?;
    let loaded = file.check()?;
    let Some(listed) = loaded.ants.into_iter().find(|a| a.name == *ant) else {
        bail!("{} lists no ant {ant}", config.path.display());
    };
    let roles = Roles::of(&fields)?;
    roles.overlay(listed).into_config(ANY_NATS, &loaded.harnesses)?;
    let record = (before.as_ref() != Some(&fields)).then(|| SharedSettings {
        ant: ant.clone(),
        fields,
        machine: orders.machine.clone(),
        by: Some(order.admin.clone()),
        updated_at: chrono::Utc::now(),
    });
    Ok(Prepared { record, last, file: file_changed.then_some(file) })
}
