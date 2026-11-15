//! [`AdminOp::Pause`] and [`AdminOp::Resume`]: set `paused = true` in the ant's `[[ant]]` table of the config file
//! the daemon serves, or remove it, and have the daemon reload the file at once ([`ConfigFile::edited`]), which pauses
//! or resumes the running ant in place ([`crate::pause`]). The admin whose order paused it is noted for the roster
//! ([`ConfigFile::pausers`]). An ant already paused, or not paused, is left as it is, and the order applied.

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, UserId};
use anyhow::Result;
use serde_json::Value;

#[cfg(doc)]
use super::ConfigFile;
use super::{NO_CONFIG, Orders, applied, refused};
use crate::config::edit::Edit;

pub(super) fn apply(orders: &Orders, order: &AdminOrder, paused: bool) -> AdminOutcome {
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    let written = match edit(&config.path, &order.ant, paused) {
        Ok(written) => written,
        Err(e) => return refused(order, &format!("{e:#}")),
    };
    if !paused {
        config.pausers.take(&order.ant);
    } else if written {
        config.pausers.paused(&order.ant, &order.admin);
    }
    if written {
        config.edited.notify_one();
    }
    applied(order, None)
}

/// Pause or resume `ant` in the config file at `path`, returning whether the file was written.
pub(super) fn edit(path: &std::path::Path, ant: &UserId, paused: bool) -> Result<bool> {
    let mut file = Edit::open(path)?;
    let field = ["paused".to_owned()];
    if file.get(ant, &field)?.and_then(|v| v.as_bool()).unwrap_or(false) == paused {
        return Ok(false);
    }
    file.set(ant, &field, paused.then_some(&Value::Bool(true)))?;
    file.save()?;
    Ok(true)
}
