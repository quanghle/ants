//! Pausing the ant cloned while its snapshot is read, and resuming it after.

use std::time::Duration;

use ants_proto::UserId;
use anyhow::{Context as _, Result, anyhow, bail};

use super::Cloning;
use super::undo::Done;
use crate::admin::ConfigFile;
use crate::clone::Snapshot;

/// How long the daemon serving the ant waits for its prompts to end and reads its snapshot: past the default
/// `prompt_timeout`.
const SNAPSHOT_WAIT: Duration = Duration::from_mins(35);

impl Cloning<'_> {
    /// Pause the ant cloned, unless it was already, and have the daemon serving its inbox read its snapshot once its
    /// last prompt ended.
    pub(super) async fn snapshot(&self, done: &mut Done) -> Result<Snapshot> {
        let (src, new, machine) = (&self.order.ant, self.new, &self.orders.machine);
        if crate::admin::pause::edit(&self.config.path, src, true)? {
            done.paused = true;
            self.config.pausers.paused(src, &self.order.admin);
            self.config.edited.notify_one();
        }
        let reading = tokio::time::timeout(SNAPSHOT_WAIT, self.orders.restarts.snapshot(src, new.clone()));
        let snapshot = self
            .turn
            .aside(reading)
            .await
            .map_err(|_| anyhow!("{src}'s prompts did not end within {}", ants_proto::duration::format(SNAPSHOT_WAIT)))?
            .with_context(|| format!("{machine} stopped serving the inbox of {src}"))?
            .with_context(|| format!("reading {src}'s sessions and threads"))?;
        if let Some(moved) = snapshot.identity.as_ref().and_then(|identity| identity.moved.as_ref()) {
            bail!(
                "{src} was moved to another bus by {} at {}: it is cloned where it is served now",
                moved.by,
                moved.at
            );
        }
        if snapshot.new_identity.is_some() {
            bail!("{new} has an identity record already: a name once used is not cloned into");
        }
        Ok(snapshot)
    }
}

/// Resume `ant`, paused by the clone.
pub(super) fn resume(config: &ConfigFile, ant: &UserId) {
    match crate::admin::pause::edit(&config.path, ant, false) {
        Ok(written) => {
            config.pausers.take(ant);
            if written {
                config.edited.notify_one();
            }
        }
        Err(e) => tracing::error!(ant = %ant, error = format!("{e:#}"), "resuming an ant after cloning it"),
    }
}
