//! The data sugar keeps of the ants a process serves ([`SugarStores`]): each ant's items, opened on first use from the
//! sugar id in its home ([`ants_sugar::identity`]), its event log in [`ants_proto::SUGAR_STREAM`] over the machine's
//! host connection and its cache in its home. Without a host connection sugar keeps nothing; while one connects, its
//! data is unavailable.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use ants_proto::provider::BoxFuture;
use ants_proto::{JsDomain, Machine, UserId};
use ants_sugar::cache::Cache;
use ants_sugar::identity;
use ants_sugar::log::{AccountVerifier, AntLog};
use ants_sugar::store::{Items, Store, StoreError, Stores};
use anyhow::{Context as _, Result};
use async_nats::jetstream;

use crate::sugar_log::{AntSigner, DeviceRevocations, JetStreamLog};

/// The items of the ants a process serves, reached over its host connection.
#[derive(Default)]
pub struct SugarStores {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    host: Host,
    /// Counts the host connections started, so one that ended after the next started leaves the next's state alone.
    generation: u64,
    ants: HashMap<UserId, Served>,
}

#[derive(Default)]
enum Host {
    #[default]
    Off,
    Connecting,
    Up(Arc<Link>),
}

/// An ant served, and its items once opened.
struct Served {
    home: PathBuf,
    creds: Option<PathBuf>,
    items: Arc<tokio::sync::Mutex<Option<Opened>>>,
}

struct Opened {
    link: Arc<Link>,
    items: Arc<dyn Items>,
}

/// A host connection, over which sugar reaches its logs.
pub struct Link {
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    /// The accounts whose ants sign events ([`AccountVerifier`]); none on a server without authentication.
    accounts: Vec<String>,
    machine: Machine,
    log: tokio::sync::OnceCell<JetStreamLog>,
}

impl Link {
    /// The host connection `js`, made for `js_domain`, of `machine`, whose credentials are of `accounts`.
    pub fn new(js: jetstream::Context, js_domain: Option<JsDomain>, accounts: Vec<String>, machine: Machine) -> Self {
        Self { js, js_domain, accounts, machine, log: tokio::sync::OnceCell::new() }
    }
}

/// A host connection starting, from [`SugarStores::hosting`] until dropped: sugar's data is unavailable until it is
/// [`Hosting::up`], then reached over it.
pub struct Hosting {
    stores: Arc<SugarStores>,
    generation: u64,
}

impl Hosting {
    /// The host connection is up as `link`.
    pub fn up(&self, link: Link) {
        let mut state = self.stores.state();
        if state.generation == self.generation {
            state.host = Host::Up(Arc::new(link));
        }
    }
}

impl Drop for Hosting {
    fn drop(&mut self) {
        let mut state = self.stores.state();
        if state.generation == self.generation {
            state.host = Host::Off;
            state.close();
        }
    }
}

impl State {
    /// Let go of the items opened, and of the host connection they hold.
    fn close(&mut self) {
        for served in self.ants.values_mut() {
            served.items = Arc::default();
        }
    }
}

impl SugarStores {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A host connection starts, replacing any before.
    pub fn hosting(self: &Arc<Self>) -> Hosting {
        let mut state = self.state();
        state.generation += 1;
        state.host = Host::Connecting;
        state.close();
        Hosting { stores: self.clone(), generation: state.generation }
    }

    /// The process serves `ant`, whose home is `home` and whose credentials are at `creds`, if any. An ant served from
    /// the same home before under another name is no longer.
    pub fn serve(&self, ant: &UserId, home: &Path, creds: Option<PathBuf>) {
        let mut state = self.state();
        state.ants.retain(|other, served| other == ant || served.home != home);
        if let Some(served) = state.ants.get(ant)
            && served.home == home
            && served.creds == creds
        {
            return;
        }
        let served = Served { home: home.to_owned(), creds, items: Arc::default() };
        state.ants.insert(ant.clone(), served);
    }
}

/// The stores of the daemon's process.
pub fn stores() -> &'static Arc<SugarStores> {
    static STORES: OnceLock<Arc<SugarStores>> = OnceLock::new();
    STORES.get_or_init(SugarStores::new)
}

impl Stores for SugarStores {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        Box::pin(async move {
            let (items, home, creds, link) = {
                let state = self.state();
                let served = state.ants.get(ant).ok_or_else(|| StoreError::NotKept(format!("{ant} is not served")))?;
                let link = match &state.host {
                    Host::Off => return Err(StoreError::NotKept("no host connection".into())),
                    Host::Connecting => {
                        return Err(StoreError::Unavailable("the host connection is not up yet".into()));
                    }
                    Host::Up(link) => link.clone(),
                };
                (served.items.clone(), served.home.clone(), served.creds.clone(), link)
            };
            let mut opened = items.lock().await;
            if let Some(opened) = opened.as_ref().filter(|opened| Arc::ptr_eq(&opened.link, &link)) {
                return Ok(opened.items.clone());
            }
            *opened = None;
            let items = open(ant, &home, creds.as_deref(), &link)
                .await
                .map_err(|e| StoreError::Unavailable(format!("opening the sugar data of {ant}: {e:#}")))?;
            *opened = Some(Opened { link, items: items.clone() });
            Ok(items)
        })
    }
}

/// The items of `ant`, whose home is `home`, its sugar id minted there first if it has none, signed with its
/// credentials at `creds`, over `link`.
async fn open(ant: &UserId, home: &Path, creds: Option<&Path>, link: &Link) -> Result<Arc<dyn Items>> {
    let sugar = identity::sugar_id(home).with_context(|| format!("reading {}", identity::id_file(home).display()))?;
    let log = link.log.get_or_try_init(|| JetStreamLog::open(&link.js)).await?.clone();
    let signer = AntSigner::load(ant, creds, &link.accounts)?;
    let revocations = DeviceRevocations::new(&link.js, link.js_domain.as_ref()).await?;
    let verifier = AccountVerifier::new(link.accounts.clone(), revocations);
    let log = AntLog::new(log, signer, verifier, sugar, ant.clone(), link.machine.clone(), Some(home.to_owned()))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let store = Store::open(log, Cache::open(home)?).await?;
    Ok(Arc::new(tokio::sync::Mutex::new(store)))
}

#[cfg(test)]
mod tests;
