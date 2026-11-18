//! Built-in sense (crate `ants-sense`) as the daemon runs it, the one place the daemon reaches it. What any provider
//! gets, sense gets through [`crate::provider`] like the others: its tools, briefs, events, wake requests and export.
//! The rest is sense's alone, behind [`Sense`]: the data it keeps of the ants a process serves ([`stores`]), over their
//! event logs in the hub's stream ([`log`]); each ant's schedule of reminders, memory maintenance and dreams
//! ([`scheduler`]); its sage's answers to the reviews of its dreams; the skills it offers the ant's harness; the
//! workdirs of the ant's threads, through which it reaches the ant's files ([`workdirs`]); and what its colony entry
//! shows of it: the profile the ant publishes, or why sense keeps none of its data ([`published`]). A home an ant
//! takes over keeps the sense data in it ([`take_over`]).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ants_proto::provider::Provider;
use ants_proto::{AntIdentity, Envelope, UserId};
use ants_sense::store::{Items, StoreError};
use anyhow::{Context as _, Result};

pub mod log;
mod published;
mod scheduler;
mod stores;
mod workdirs;

pub use ants_sense::home::DIR;
pub use ants_sense::{CAPABILITIES, NAME};
pub(crate) use scheduler::{Daemon, Dreaming, Serving};
pub use stores::{Hosting, Link};

use crate::daemon::Config;
use crate::profile::identity::Identity;
use crate::provider::wake::Wakers;
use stores::SenseStores;

/// Built-in sense of a process ([`crate::provider::Providers::sense`]): one serves all its ants whose setting names
/// sense, each call naming its ant.
pub struct Sense {
    stores: Arc<SenseStores>,
    provider: Arc<ants_sense::Sense>,
    workdirs: workdirs::Directories,
    /// The ants of the process its schedules wake.
    wakers: Wakers,
}

impl Sense {
    /// Sense waking the ants `wakers` lists.
    pub(crate) fn new(wakers: Wakers) -> Self {
        let (stores, workdirs) = (SenseStores::new(), workdirs::Directories::default());
        let provider = ants_sense::Sense::with_stores(stores.clone()).with_workdirs(Arc::new(workdirs.clone()));
        Self { stores, provider: Arc::new(provider), workdirs, wakers }
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
        let items = self.provider.synced(ant).await?;
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
    /// daemon what `dreaming` gives, and each of its reads of the ant's items putting the profile of the ant's
    /// confirmed persona in `workload` ([`published::profile`]). Start only after the ant holds its colony registration
    /// and its wake service is installed.
    pub(crate) fn schedule(
        &self,
        config: &Config,
        machine: &str,
        dreaming: Arc<dyn Dreaming>,
        workload: Arc<crate::colony::Workload>,
    ) -> Serving {
        scheduler::serve(self.provider.clone(), self.wakers.clone(), config, machine, dreaming, workload)
    }

    /// Put in `workload` the profile of `ant`'s confirmed persona before the ant's first colony entry, which then
    /// shows it, as the schedule's reads keep it current after ([`published::first_profile`]).
    pub(crate) async fn first_profile(&self, ant: &UserId, workload: &crate::colony::Workload) {
        published::first_profile(&self.provider, ant, workload).await;
    }

    /// The items of `ant`, synced; none when sense keeps none of its data, as the ant is still served without them.
    /// Data kept but out of reach is an error, so that a message waits for its effects.
    async fn kept(&self, ant: &UserId) -> Result<Option<Arc<dyn Items>>> {
        match self.provider.synced(ant).await {
            Ok(items) => Ok(Some(items)),
            Err(StoreError::NotKept(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Take `envelope`, a message from an ant to `ant`, if it is its sage's answer to the review of a dream: whether
    /// the ant is to answer it, none when it is no such answer or sense keeps none of the ant's data
    /// ([`scheduler::review_answer`]).
    pub(crate) async fn review_answer(&self, ant: &UserId, envelope: &mut Envelope) -> Result<Option<bool>> {
        let Some(items) = self.kept(ant).await? else { return Ok(None) };
        scheduler::review_answer(&*items, envelope).await
    }

    /// Note in the sense data of `sage` the review message `envelope` that arrived for it from an ant, if sense keeps
    /// them ([`scheduler::review_received`]).
    pub(crate) async fn review_received(&self, sage: &UserId, envelope: &Envelope) -> Result<()> {
        let Some(items) = self.kept(sage).await? else { return Ok(()) };
        scheduler::review_received(&*items, envelope).await
    }

    /// Note in the sense data of `sage` its answer `text` to the review message `request`, with what its thread `spent`
    /// when known, if sense keeps them ([`scheduler::review_given`]).
    pub(crate) async fn review_given(
        &self,
        sage: &UserId,
        request: &Envelope,
        text: &str,
        spent: Option<crate::limits::caps::Spent>,
    ) -> Result<()> {
        let Some(items) = self.kept(sage).await? else { return Ok(()) };
        // Saturates on a cost out of range, and a negative one counts as none.
        let spent = spent.map(|spent| ants_sense::dream::ReviewSpend {
            tokens: spent.tokens,
            cost_micros: (spent.cost * 1e6).round() as u64,
        });
        scheduler::review_given(&*items, request, text, spent).await
    }

    /// Keep what `workload` puts in `ant`'s colony entry of why sense keeps none of its data current, until dropped
    /// ([`published::unkept`]); the schedule keeps its profile current ([`Sense::schedule`]).
    pub(crate) async fn publish(&self, ant: &UserId, workload: &crate::colony::Workload) {
        published::unkept(self.stores.why_unkept(), ant, workload).await;
    }

    /// The process keeps none of its ants' data, for the reason `why`, as it makes no host connection and sense no link
    /// of its own; with None, it may again ([`SenseStores::unkept`]). Each ant's colony entry and the daemon's log say
    /// so.
    pub fn unkept(&self, why: Option<String>) {
        self.stores.unkept(why);
    }

    /// The skills sense offers the harness of the ant whose home is `home`: its skills directory, in the Claude Code
    /// plugin of its sense files.
    pub fn skills(&self, home: &Path) -> crate::harness::Skills {
        let home = ants_sense::home::Home::new(home);
        crate::harness::Skills { dir: home.skills(), plugin: home.dir().to_owned() }
    }

    /// Resolve the file access of `ant` through `dirs`, the workdirs of its threads, until the guard drops.
    pub(crate) fn workdirs(&self, ant: &UserId, dirs: crate::mcp::Directories) -> workdirs::Serving {
        workdirs::serve(&self.workdirs, ant, Arc::new(dirs))
    }
}

/// List among the earlier names of `ant`, whose home is `home`, the name the sense data there were last of, when
/// another, so the ant keeps those data: that name, if listed.
pub fn take_over(home: &Path, ant: &UserId) -> Result<Option<UserId>> {
    ants_sense::identity::take_over(home, ant).with_context(|| {
        format!("listing the earlier name of {ant} in {}", ants_sense::home::Home::new(home).names().display())
    })
}

#[cfg(test)]
impl Sense {
    /// What the record of the review of dream `dream` of `ant` in the sense data of `sage` says its thread spent, none
    /// when it says nothing. Panics when sense keeps no such record.
    pub(crate) async fn review_spent(
        &self,
        sage: &UserId,
        ant: &UserId,
        dream: u64,
    ) -> Option<crate::limits::caps::Spent> {
        let items = self.kept(sage).await.unwrap().expect("sense keeps the sage's data");
        let record = ants_sense::dream::review_record(&*items, ant, dream).await.unwrap().expect("the review's record");
        assert!(record.answered.is_some(), "the review answered: {record:?}");
        let spent = record.spent?;
        Some(crate::limits::caps::Spent { tokens: spent.tokens, cost: spent.cost_micros as f64 / 1e6 })
    }
}

/// Sense keeping no data, its tools that need some refusing.
#[cfg(test)]
pub(crate) fn keeping_none() -> Arc<dyn Provider> {
    Arc::new(ants_sense::Sense::new())
}

#[cfg(test)]
mod tests;
