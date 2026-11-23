//! The uplink credentials of a machine's leaf node: renewed to carry an ant added, shrunk to drop one retired, and
//! taken up by the node connecting its uplink again.

use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::auth;
use crate::credentials::enroll;
use crate::node::natsd;
use crate::service::{Action, Outcome, Service};

/// The configuration of the machine's leaf node, at `path`, or at ~/.config/ants/leaf.conf when it exists, and the
/// node.
pub(crate) fn leaf_node(path: Option<&Path>) -> Result<Option<(PathBuf, enroll::Leaf)>> {
    let path = match path {
        Some(path) => path.to_owned(),
        None => {
            let default = super::default_path()?;
            if !default.exists() {
                return Ok(None);
            }
            default
        }
    };
    let (machine, creds) = super::read_uplink(&path)?;
    Ok(Some((path, enroll::Leaf { machine, creds })))
}

/// Replace the JWT of the uplink credentials of `leaf` by `renewed`, unless it would drop something they allow (they
/// changed while the request waited). Returns whether it replaced it.
pub(crate) fn renew_uplink(leaf: &enroll::Leaf, renewed: &str) -> Result<bool> {
    let (current, key) = auth::read_creds(&leaf.creds)?;
    if !auth::allows_all(renewed, &current)? {
        return Ok(false);
    }
    auth::write_creds(&leaf.creds, renewed, &key)?;
    Ok(true)
}

/// Renew the uplink credentials of the leaf node configured at `conf` with `renewed` ([`renew_uplink`]) and have the
/// node connect its uplink again with them, by a reload keeping local connections ([`super::reconnect_uplink`]): what
/// came of it, or None when the credentials changed meanwhile and were left as they are. Blocks while the leaf node's
/// service reloads.
pub(crate) fn renew_and_reconnect(conf: &Path, renewed: &str) -> Result<Option<String>> {
    renew_and_apply(conf, renewed, |action| natsd::apply(conf, action))
}

/// [`renew_and_reconnect`], `apply` reloading the leaf node.
pub(crate) fn renew_and_apply(
    conf: &Path,
    renewed: &str,
    apply: impl FnOnce(Action) -> Outcome,
) -> Result<Option<String>> {
    replace_and_apply(conf, None, renewed, apply)
}

/// [`renew_and_reconnect`] with `shrunk`, the uplink JWT `asked` renewed without a retired ant: the credentials take it
/// while they still hold `asked`, or allow nothing it does not.
pub(crate) fn shrink_and_reconnect(conf: &Path, asked: Option<&str>, shrunk: &str) -> Result<Option<String>> {
    shrink_and_apply(conf, asked, shrunk, |action| natsd::apply(conf, action))
}

/// [`shrink_and_reconnect`], `apply` reloading the leaf node.
pub(crate) fn shrink_and_apply(
    conf: &Path,
    asked: Option<&str>,
    shrunk: &str,
    apply: impl FnOnce(Action) -> Outcome,
) -> Result<Option<String>> {
    replace_and_apply(conf, asked, shrunk, apply)
}

fn replace_and_apply(
    conf: &Path,
    asked: Option<&str>,
    renewed: &str,
    apply: impl FnOnce(Action) -> Outcome,
) -> Result<Option<String>> {
    let (machine, creds) = super::read_uplink(conf)?;
    let (current, key) = auth::read_creds(&creds)?;
    let replaced = if asked == Some(current.as_str()) {
        auth::write_creds(&creds, renewed, &key)?;
        true
    } else {
        renew_uplink(&enroll::Leaf { machine, creds }, renewed)?
    };
    if !replaced {
        return Ok(None);
    }
    let said = super::reconnect_uplink(conf, apply).unwrap_or_else(|e| {
        format!(
            "{e:#}; a human restarts the leaf node to use them ({})",
            crate::service::manual(Service::Nats, Action::Restart)
        )
    });
    Ok(Some(said))
}
