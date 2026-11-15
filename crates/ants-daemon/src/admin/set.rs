//! [`AdminOp::Set`]: change fields of the ant's `[[ant]]` table in the config file the daemon serves, all or none,
//! each only if it holds the value the order expects ([`Edit::change`]). The file as edited must load before it is
//! written over the old one ([`Edit::save`]), and the daemon reloads it at once ([`ConfigFile::edited`]). An order
//! taken again after its edit was written, its outcome not stored, finds its fields holding their new values and is
//! applied without writing again.

use std::path::Path;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, FieldChange, UserId};
use anyhow::Result;

#[cfg(doc)]
use super::ConfigFile;
use super::{NO_CONFIG, Orders, applied, refused};
use crate::config::edit::Edit;

pub(super) fn apply(orders: &Orders, order: &AdminOrder, changes: &[FieldChange]) -> AdminOutcome {
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    match edit(&config.path, &order.ant, changes) {
        Ok(written) => {
            if written {
                config.edited.notify_one();
            }
            applied(order, None)
        }
        Err(e) => refused(order, &format!("{e:#}")),
    }
}

/// Make `changes` to `ant`'s table in the config file at `path`, returning whether the file was written.
fn edit(path: &Path, ant: &UserId, changes: &[FieldChange]) -> Result<bool> {
    let mut file = Edit::open(path)?;
    if !file.change(ant, changes)? {
        return Ok(false);
    }
    file.save()?;
    Ok(true)
}
