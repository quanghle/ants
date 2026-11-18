//! Built-in sense (crate `ants-sense`) as the daemon runs it, the one place the daemon reaches it. What any provider
//! gets, sense gets through [`crate::provider`] like the others: its tools, briefs, events, wake requests and export.
//! The rest is sense's alone, behind [`Sense`]: the data it keeps of the ants a process serves ([`stores`]), over their
//! event logs in the hub's stream ([`log`]); each ant's schedule of reminders, memory maintenance and dreams
//! ([`scheduler`]); its sage's answers to the reviews of its dreams; the skills it offers the ant's harness; and the
//! workdirs of the ant's threads, through which it reaches the ant's files ([`workdirs`]). A home an ant takes over
//! keeps the sense data in it ([`take_over`]).

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use ants_proto::provider::Provider;
use ants_proto::{AntIdentity, Envelope, UserId};
use anyhow::{Context as _, Result};

pub mod log;
mod scheduler;
mod stores;
mod workdirs;

pub use ants_sense::log::SENSE_DIR as DIR;
pub use ants_sense::memory::{normalize_remote, remote_url};
pub use ants_sense::{CAPABILITIES, NAME};
pub(crate) use scheduler::{Daemon, Dreaming, Serving};
pub use stores::{Hosting, Link};

use crate::daemon::Config;
use crate::profile::identity::Identity;
use crate::provider::Setting;
use stores::SenseStores;

/// Built-in sense of a process: one serves all its ants whose setting names sense, each call naming its ant.
pub struct Sense {
    stores: Arc<SenseStores>,
    provider: Arc<ants_sense::Sense>,
}

impl Sense {
    fn new() -> Self {
        let stores = SenseStores::new();
        let provider = ants_sense::Sense::with_stores(stores.clone()).with_workdirs(Arc::new(workdirs::Directories));
        Self { stores, provider: Arc::new(provider) }
    }

    /// Serve the sense data of `ant`, whose home is `home` and whose credentials are at `creds`, if any
    /// ([`SenseStores::serve`]).
    pub fn serve(&self, ant: &UserId, home: &Path, creds: Option<PathBuf>) {
        self.stores.serve(ant, home, creds);
    }

    /// The daemon of `ant`, served, read its identity `record`, which names its sense id: None when it has none
    /// ([`SenseStores::identify`]).
    pub(crate) fn identify(&self, ant: &UserId, record: Option<Identity>) {
        self.stores.identify(ant, record);
    }

    /// Note on the card of `ant` the birth and the prompt count its identity `record` holds, which a record made again
    /// for it, as after a revoke and re-add, starts from: a call's context carries neither.
    pub(crate) async fn grown(&self, ant: &UserId, record: &AntIdentity) -> Result<()> {
        let items = self.provider.items(ant).await?;
        Ok(stores::note_growth(ant, &*items, record).await?)
    }

    /// A host connection starts, over which sense reaches the logs of the ants served ([`Hosting`]).
    pub fn hosting(&self) -> Hosting {
        self.stores.hosting()
    }

    /// The provider registered for the ants whose setting names sense.
    pub fn provider(&self) -> Arc<dyn Provider> {
        self.provider.clone()
    }

    /// Run the schedule of the ant `config` serves on `machine` until the guard drops, its dreams taking from the
    /// daemon what `dreaming` gives. Start only after the ant holds its colony registration and its wake service is
    /// installed.
    pub(crate) fn schedule(&self, config: &Config, machine: &str, dreaming: Arc<dyn Dreaming>) -> Serving {
        scheduler::serve(self.provider.clone(), config, machine, dreaming)
    }

    /// Take `envelope`, a message from an ant to `ant`, if it is its sage's answer to the review of a dream: whether
    /// the ant is to answer it, none when it is no such answer ([`scheduler::review_answer`]).
    pub(crate) async fn review_answer(&self, ant: &UserId, envelope: &mut Envelope) -> Result<Option<bool>> {
        let items = self.provider.items(ant).await?;
        scheduler::review_answer(&*items, envelope).await
    }

    /// The directory of the skills sense offers the harness of the ant whose home is `home`.
    pub fn skills(&self, home: &Path) -> PathBuf {
        ants_sense::export::skills_dir(home)
    }

    /// Resolve the file access of `ant` through `dirs`, the workdirs of its threads, until the guard drops.
    pub(crate) fn workdirs(&self, ant: &UserId, dirs: crate::mcp::Directories) -> workdirs::Serving {
        workdirs::serve(ant, Arc::new(dirs))
    }
}

/// Built-in sense of the process.
pub fn built_in() -> &'static Sense {
    static BUILT_IN: LazyLock<Sense> = LazyLock::new(Sense::new);
    &BUILT_IN
}

/// Built-in sense of the process when `setting` names it.
pub fn of(setting: &Setting) -> Option<&'static Sense> {
    matches!(setting, Setting::Sense).then(built_in)
}

/// List among the earlier names of `ant`, whose home is `home`, the name the sense data there were last of, when
/// another, so the ant keeps those data: that name, if listed.
pub fn take_over(home: &Path, ant: &UserId) -> Result<Option<UserId>> {
    ants_sense::identity::take_over(home, ant).with_context(|| {
        format!("listing the earlier name of {ant} in {}", ants_sense::identity::names_file(home).display())
    })
}

/// Sense keeping no data, its tools that need some refusing.
#[cfg(test)]
pub(crate) fn keeping_none() -> Arc<dyn Provider> {
    Arc::new(ants_sense::Sense::new())
}

#[cfg(test)]
mod tests;
