//! The data sense keeps of the ants a process serves ([`SenseStores`]): each ant's items, opened on first use from the
//! sense id in its home ([`ants_sense::identity`]), its event log in [`ants_proto::SENSE_STREAM`] over the machine's
//! host connection and its cache in its home. Existing caches remain available while disconnected, including after
//! process restart. One retry worker per host connection drains durable local outboxes after reconnect. An ant's sense
//! data travel to another bus by export and import ([`ants_sense::export`]); a home awaiting the import opens none of
//! them until it lands.
//!
//! The ant's identity record names its sense id ([`ants_proto::AntIdentity::sense`]), so its homes on every machine
//! open the same sense identity: the id of a home is written to a record naming none, the first writer winning, a home
//! without one takes the record's, an id is minted only when neither has one, and a home and a record naming different
//! ones are refused ([`SenseStores::identify`]). The card keeps the birth and the prompt count the record holds, which a
//! record made again for the ant, as after a revoke and re-add, starts from ([`note_growth`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::provider::BoxFuture;
use ants_proto::{AntIdentity, JsDomain, Machine, SenseId, UserId};
use ants_sense::cache::Cache;
use ants_sense::export::{self, SenseExport};
use ants_sense::home::Home;
use ants_sense::identity::{self, Growth};
use ants_sense::log::{
    AccountVerifier, AntLog, AppendError, Event, EventLog, LogError, LogErrorKind, Revocations, Signature, Signer,
    Stored, Verifier, VerifyError,
};
use ants_sense::store::{Items, Store, StoreError, Stores, is_offline};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use serde::{Deserialize, Serialize};

use super::log::{AntSigner, DeviceRevocations, JetStreamLog};
use crate::profile::identity::Identity;

/// The items of the ants a process serves, reached over its host connection.
#[derive(Default)]
pub struct SenseStores {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    host: Host,
    /// Counts the host connections started, so one that ended after the next started leaves the next's state alone.
    generation: u64,
    ants: HashMap<UserId, Served>,
    route: Route,
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
    /// A digest of the credentials as they were when served: the items, signed with them, open again once they change.
    signer: Option<u64>,
    items: Arc<tokio::sync::Mutex<Option<Opened>>>,
    named: Named,
}

/// What names the sense id of an ant served besides its home.
#[derive(Clone, Default)]
enum Named {
    /// Its daemon has not read its identity record yet: its items open only under the id its home keeps.
    #[default]
    Awaited,
    /// No identity record, as without an identities bucket: the home's id, minted there when it has none.
    Home,
    /// Its identity record, which the home's id is settled against on the next opening.
    Record(Arc<Identity>),
    /// The home's id is the one the record names.
    Settled,
}

type Opened = Arc<dyn Items>;
type Route = Arc<Mutex<Option<Arc<Link>>>>;

#[derive(Serialize, Deserialize)]
struct Context {
    machine: Machine,
    accounts: Vec<String>,
}

/// A host connection, over which sense reaches its logs.
pub struct Link {
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    /// The accounts whose ants sign events ([`AccountVerifier`]); none on a server without authentication.
    accounts: Vec<String>,
    machine: Machine,
    log: tokio::sync::OnceCell<JetStreamLog>,
    revocations: tokio::sync::OnceCell<DeviceRevocations>,
}

impl Link {
    /// The host connection `js`, made for `js_domain`, of `machine`, whose credentials are of `accounts`.
    pub fn new(js: jetstream::Context, js_domain: Option<JsDomain>, accounts: Vec<String>, machine: Machine) -> Self {
        Self {
            js,
            js_domain,
            accounts,
            machine,
            log: tokio::sync::OnceCell::new(),
            revocations: tokio::sync::OnceCell::new(),
        }
    }
}

/// A host connection starting, from [`SenseStores::hosting`] until dropped. Existing stores keep serving their
/// caches until [`Hosting::up`] supplies a connection, then follow that connection without losing queued writes.
pub struct Hosting {
    stores: Arc<SenseStores>,
    generation: u64,
    retry: Option<tokio::task::JoinHandle<()>>,
}

impl Hosting {
    /// The host connection is up as `link`.
    pub fn up(&self, link: Link) {
        let mut state = self.stores.state();
        if state.generation == self.generation {
            let link = Arc::new(link);
            *route(&state.route) = Some(link.clone());
            state.host = Host::Up(link);
            self.stores.changed.notify_one();
        }
    }
}

impl Drop for Hosting {
    fn drop(&mut self) {
        if let Some(retry) = &self.retry {
            retry.abort();
        }
        let mut state = self.stores.state();
        if state.generation == self.generation {
            state.host = Host::Off;
            *route(&state.route) = None;
        }
    }
}

impl SenseStores {
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
        *route(&state.route) = None;
        let generation = state.generation;
        drop(state);
        let weak = Arc::downgrade(self);
        let retry = tokio::runtime::Handle::try_current().ok().map(|runtime| {
            runtime.spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    let Some(stores) = weak.upgrade() else { break };
                    if stores.state().generation != generation {
                        break;
                    }
                    tokio::select! {
                        _ = tick.tick() => {}
                        _ = stores.changed.notified() => {}
                    }
                    if stores.state().generation != generation {
                        break;
                    }
                    stores.retry_pending().await;
                }
            })
        });
        Hosting { stores: self.clone(), generation, retry }
    }

    /// The process serves `ant`, whose home is `home` and whose credentials are at `creds`, if any. An ant served from
    /// the same home before under another name is no longer; one served again with other credentials, as after a
    /// revoke and re-add, signs with the new ones.
    pub fn serve(&self, ant: &UserId, home: &Path, creds: Option<PathBuf>) {
        let signer = creds.as_deref().and_then(digest);
        let mut state = self.state();
        state.ants.retain(|other, served| other == ant || served.home != home);
        if let Some(served) = state.ants.get(ant)
            && served.home == home
            && served.creds == creds
            && served.signer == signer
        {
            return;
        }
        let served = Served { home: home.to_owned(), creds, signer, items: Arc::default(), named: Named::Awaited };
        state.ants.insert(ant.clone(), served);
        self.changed.notify_one();
    }

    /// The daemon of `ant`, served, read its identity `record`, None when it has none, as without an identities bucket.
    /// Until then a home without a sense id opens nothing: the home's id is settled against the record on the next
    /// opening of its items, written to it when it names none, a home without one taking the record's or, when the
    /// record names none either, a new one written to the record first; a home and a record naming different ones are
    /// refused, the items opened before closed. Without a record, the home's id is minted when it has none.
    pub fn identify(&self, ant: &UserId, record: Option<Identity>) {
        if let Some(served) = self.state().ants.get_mut(ant) {
            served.named = record.map_or(Named::Home, |record| Named::Record(Arc::new(record)));
        }
    }

    /// The home's id of `ant` is settled against the record in `named`, if `items` are still the ones served.
    fn settled(&self, ant: &UserId, items: &Arc<tokio::sync::Mutex<Option<Opened>>>) {
        if let Some(served) = self.state().ants.get_mut(ant)
            && Arc::ptr_eq(&served.items, items)
        {
            served.named = Named::Settled;
        }
    }

    async fn retry_pending(&self) {
        let served: Vec<_> = self
            .state()
            .ants
            .iter()
            .map(|(ant, served)| (ant.clone(), served.home.clone(), served.items.clone()))
            .collect();
        for (ant, home, opened) in served {
            let items = opened.lock().await.clone();
            if let Some(items) = items {
                if items.pending().await.is_ok_and(|count| count > 0) {
                    let _ = tokio::time::timeout(Duration::from_secs(15), items.sync()).await;
                }
            } else if Home::new(&home).has_pending_writes() {
                let _ = tokio::time::timeout(Duration::from_secs(15), self.items(&ant)).await;
            }
        }
    }
}

fn digest(file: &Path) -> Option<u64> {
    let mut hasher = std::hash::DefaultHasher::new();
    std::hash::Hash::hash(&std::fs::read(file).ok()?, &mut hasher);
    Some(std::hash::Hasher::finish(&hasher))
}

/// What [`SenseStores`] holds of an ant served.
struct Of {
    items: Arc<tokio::sync::Mutex<Option<Opened>>>,
    home: PathBuf,
    creds: Option<PathBuf>,
    link: Option<Arc<Link>>,
    route: Route,
    named: Named,
}

impl SenseStores {
    fn of(&self, ant: &UserId) -> Result<Of, StoreError> {
        let state = self.state();
        let served = state.ants.get(ant).ok_or_else(|| StoreError::NotKept(format!("{ant} is not served")))?;
        let link = match &state.host {
            Host::Off | Host::Connecting => None,
            Host::Up(link) => Some(link.clone()),
        };
        let (items, home, creds) = (served.items.clone(), served.home.clone(), served.creds.clone());
        Ok(Of { items, home, creds, link, route: state.route.clone(), named: served.named.clone() })
    }
}

impl Stores for SenseStores {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        Box::pin(async move {
            let Of { items: slot, home, creds, link, route, named } = self.of(ant)?;
            let files = Home::new(&home);
            if files.awaits_import() {
                return Err(StoreError::Unavailable(format!(
                    "the sense data of {ant} await their import: `ants ant import FILE --provider-only` imports them, \
                     removing {} starts afresh",
                    files.importing().display()
                )));
            }
            let mut opened = slot.lock().await;
            let mut current = opened.clone();
            if let Some(items) = &current {
                match items.sync().await {
                    Err(StoreError::Log(error))
                        if error.kind == LogErrorKind::Incarnation && items.pending().await? == 0 =>
                    {
                        *opened = None;
                        current = None;
                    }
                    // The items remain readable, including when a conflict requires explicit reconciliation.
                    Ok(()) | Err(StoreError::Log(_)) | Err(StoreError::Unavailable(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            let items = match current {
                Some(items) => items,
                None => {
                    if link.is_none() && !files.cache().exists() {
                        return Err(match self.state().host {
                            Host::Connecting => StoreError::Unavailable("the host connection is not up yet".into()),
                            _ => StoreError::NotKept("no host connection".into()),
                        });
                    }
                    if let Named::Record(record) = &named {
                        take_named(ant, &home, record).await?;
                    }
                    let items =
                        open(ant, &home, creds.as_deref(), link.as_deref(), route, matches!(named, Named::Home))
                            .await
                            .map_err(|e| StoreError::Unavailable(format!("opening the sense data of {ant}: {e:#}")))?;
                    *opened = Some(items.clone());
                    items
                }
            };
            if let Named::Record(record) = &named
                && link
                    .is_some_and(|link| link.js.client().connection_state() == async_nats::connection::State::Connected)
            {
                match settle(ant, &home, record, &*items).await {
                    Ok(true) => self.settled(ant, &slot),
                    // Settled on a later opening, the record unread meanwhile.
                    Ok(false) => {}
                    Err(refused) => {
                        *opened = None;
                        return Err(refused);
                    }
                }
            }
            Ok(items)
        })
    }

    fn home(&self, ant: &UserId) -> Option<PathBuf> {
        self.of(ant).ok().map(|of| of.home)
    }

    fn export<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<SenseExport, StoreError>> {
        Box::pin(async move {
            let items = self.items(ant).await?;
            let Of { home, link, .. } = self.of(ant)?;
            let link = link.ok_or_else(|| StoreError::Unavailable("no host connection".into()))?;
            let messages = items.messages().await?;
            let skills = export::skill_files(&home)
                .map_err(|e| StoreError::Unavailable(format!("reading the skill files of {ant}: {e}")))?;
            Ok(export::pack(items.sense().await, ant.clone(), &messages, &link.accounts, skills)?)
        })
    }

    fn import<'a>(&'a self, ant: &'a UserId, export: SenseExport) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let Of { items, home, creds, link, route, .. } = self.of(ant)?;
            let link = link.ok_or_else(|| StoreError::Unavailable("no host connection".into()))?;
            let mut opened = items.lock().await;
            *opened = None;
            import(ant, &home, creds.as_deref(), &link, route, &export).await?;
            drop(opened);
            self.items(ant).await.map(drop)
        })
    }
}

/// Take `export` in as the sense data of `ant`, whose home is `home`, signed with its credentials at `creds`, over
/// `link`: under the export's sense id, which the home keeps unless it keeps another, refused then.
async fn import(
    ant: &UserId,
    home: &Path,
    creds: Option<&Path>,
    link: &Link,
    route: Route,
    export: &SenseExport,
) -> Result<(), StoreError> {
    let unavailable = |e: anyhow::Error| StoreError::Unavailable(format!("{e:#}"));
    let sense = &export.manifest.sense;
    match identity::read_id(home).map_err(|e| unavailable(e.into()))? {
        Some(id) if id != *sense => {
            return Err(StoreError::Unavailable(format!(
                "the home of {ant} keeps sense identity {id}, and the export is of {sense}"
            )));
        }
        Some(_) => {}
        None => identity::keep_id(home, sense).map_err(|e| unavailable(e.into()))?,
    }
    if Home::new(home).has_pending_writes() {
        return Err(StoreError::Unavailable(format!("changes of {ant} wait to be appended to its log here")));
    }
    let earlier = identity::earlier_names(home).map_err(|e| unavailable(e.into()))?;
    let log = link.log.get_or_try_init(|| JetStreamLog::open(&link.js)).await.map_err(|e| unavailable(e.into()))?;
    let signer = HostSigner {
        signer: AntSigner::load(ant, creds, &link.accounts).map_err(unavailable)?,
        ant: ant.clone(),
        accounts: link.accounts.clone(),
    };
    let incarnation = log.incarnation().to_owned();
    let log = HostLog { route: route.clone(), incarnation, accounts: link.accounts.clone() };
    let machine = link.machine.clone();
    let mut log =
        AntLog::new(log, signer, HostVerifier(route), sense.clone(), ant.clone(), machine, Some(home.into()))?
            .with_earlier_names(earlier);
    let imported = export::import(&mut log, export).await?;
    imported.write_skills(home).map_err(|e| unavailable(e.into()))?;
    export::import_done(home).map_err(|e| unavailable(e.into()))
}

/// Have the home of `ant`, `home`, keep a sense id when it keeps none: the one its identity `record` names, or a new
/// one the record then names, unless another daemon named one first. Nothing when it keeps one, which [`settle`]
/// settles against the record once opened.
async fn take_named(ant: &UserId, home: &Path, record: &Identity) -> Result<(), StoreError> {
    let unavailable = |e: anyhow::Error| StoreError::Unavailable(format!("naming the sense identity of {ant}: {e:#}"));
    if identity::read_id(home).map_err(|e| unavailable(e.into()))?.is_some() || Home::new(home).has_pending_writes() {
        return Ok(());
    }
    let minted = SenseId::new();
    let named = record.name_sense(&minted, None).await.map_err(unavailable)?;
    let sense = named.and_then(|record| record.sense).unwrap_or(minted);
    identity::keep_id(home, &sense).map_err(|e| unavailable(e.into()))
}

/// Settle the sense id of `items`, those of `ant` whose home is `home`, against its identity `record`, naming it there
/// when the record names none, the record then starting from the growth the card kept of an earlier one, as after a
/// revoke and re-add, and noting the record's on the card: whether it is settled, false when the record or the card
/// cannot be read; refused when the record names another sense id, with how to settle that.
async fn settle(ant: &UserId, home: &Path, record: &Identity, items: &dyn Items) -> Result<bool, StoreError> {
    let sense = items.sense().await;
    let unsettled = |e: &dyn std::fmt::Display| {
        tracing::warn!(%ant, error = format!("{e:#}"), "naming the sense identity in the ant's identity record");
        Ok(false)
    };
    let earlier = match identity::card(items).await {
        Ok(card) => card.and_then(|card| card.growth).map(|growth| (growth.born, growth.prompts)),
        Err(e) => return unsettled(&e),
    };
    let stored = match record.name_sense(&sense, earlier).await {
        Ok(stored) => stored,
        Err(e) => return unsettled(&e),
    };
    match stored.as_ref().and_then(|stored| stored.sense.as_ref()) {
        Some(named) if *named != sense => Err(StoreError::Unavailable(format!(
            "the home of {ant} keeps sense identity {sense} and its identity record names {named}: removing {} takes \
             the record's, `ants auth revoke {ant}` and adding it again keeps the home's",
            Home::new(home).id().display()
        ))),
        _ => {
            if let Some(stored) = &stored
                && let Err(e) = note_growth(ant, items, stored).await
            {
                tracing::warn!(%ant, error = format!("{e:#}"), "noting the ant's growth on its card");
            }
            Ok(true)
        }
    }
}

/// Note on the card of `ant` in `items` the birth and the prompt count its identity `record` holds, when the record
/// names their sense id, so a record made again for the ant starts from them ([`settle`]).
pub(super) async fn note_growth(ant: &UserId, items: &dyn Items, record: &AntIdentity) -> Result<(), StoreError> {
    if record.sense.as_ref() != Some(&items.sense().await) {
        return Ok(());
    }
    identity::record_growth(items, ant, Growth { born: record.born, prompts: record.prompts }).await.map(drop)
}

/// The items of `ant`, whose home is `home`, under the sense id it keeps, minted there first if it has none when
/// `mint`, signed with its credentials at `creds`, over `link`; its events under the earlier names its home lists are
/// its own.
async fn open(
    ant: &UserId,
    home: &Path,
    creds: Option<&Path>,
    link: Option<&Link>,
    route: Route,
    mint: bool,
) -> Result<Arc<dyn Items>> {
    let sense = match identity::read_id(home)? {
        Some(sense) => sense,
        None if Home::new(home).has_pending_writes() => {
            anyhow::bail!("sense identity missing while pending writes remain; cache left untouched");
        }
        None if mint => identity::sense_id(home)?,
        None => anyhow::bail!("waiting for the daemon of {ant} to read its identity record, which names its sense id"),
    };
    let earlier =
        identity::earlier_names(home).with_context(|| format!("reading {}", Home::new(home).names().display()))?;
    let cache = Cache::open(home)?;
    let remote = match link {
        Some(link) if link.js.client().connection_state() == async_nats::connection::State::Connected => {
            match link.log.get_or_try_init(|| JetStreamLog::open(&link.js)).await {
                Ok(log) => Some(log.incarnation().to_owned()),
                Err(error) if is_offline(&error) => None,
                Err(error) => return Err(error.into()),
            }
        }
        _ => None,
    };
    let (context, incarnation) = match (link, remote) {
        (Some(link), Some(incarnation)) => {
            let context = Context { machine: link.machine.clone(), accounts: link.accounts.clone() };
            (context, incarnation)
        }
        _ => {
            let context = cache.context()?.context("no cached sense connection context")?;
            let context: Context = serde_json::from_value(context)?;
            let built = cache.built()?.context("sense cache has not been built from a log")?;
            (context, built.incarnation)
        }
    };
    let saved_context = serde_json::to_value(&context)?;
    let signer = HostSigner {
        signer: AntSigner::load(ant, creds, &context.accounts)?,
        ant: ant.clone(),
        accounts: context.accounts.clone(),
    };
    let log = HostLog { route: route.clone(), incarnation, accounts: context.accounts };
    let verifier = HostVerifier(route);
    let log = AntLog::new(log, signer, verifier, sense, ant.clone(), context.machine.clone(), Some(home.to_owned()))
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .with_earlier_names(earlier);
    let store = Store::open(log, cache).await?.with_machine(context.machine);
    store.cache().set_context(&saved_context)?;
    Ok(Arc::new(tokio::sync::Mutex::new(store)))
}

fn route(route: &Route) -> std::sync::MutexGuard<'_, Option<Arc<Link>>> {
    route.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn connected(route_: &Route) -> Result<Arc<Link>, LogError> {
    let link = route(route_).clone().ok_or_else(|| LogError::unreachable("hub unavailable: no host connection"))?;
    if link.js.client().connection_state() != async_nats::connection::State::Connected {
        return Err(LogError::unreachable("hub unavailable: host disconnected"));
    }
    Ok(link)
}

struct HostLog {
    route: Route,
    incarnation: String,
    accounts: Vec<String>,
}

impl HostLog {
    async fn log(&self) -> Result<JetStreamLog, LogError> {
        let link = connected(&self.route)?;
        if link.accounts != self.accounts {
            return Err(LogError::refused("host authentication accounts changed; cached sense writes retained"));
        }
        let log = link.log.get_or_try_init(|| JetStreamLog::open(&link.js)).await?;
        if log.incarnation() != self.incarnation {
            return Err(LogError::incarnation("sense stream incarnation changed; cached sense writes retained"));
        }
        Ok(log.clone())
    }
}

impl EventLog for HostLog {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        self.log().await?.append(sense, payload, expected_last).await
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.log().await?.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.log().await?.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

struct HostSigner {
    signer: AntSigner,
    ant: UserId,
    accounts: Vec<String>,
}

impl Signer for HostSigner {
    fn sign(&self, bytes: &[u8]) -> Result<Option<Signature>, String> {
        let signature = self.signer.sign(bytes)?;
        if self.accounts.is_empty() {
            return Ok(signature);
        }
        let signature = signature.ok_or("no signing credentials")?;
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default();
        let user = ants_proto::verify_user_jwt(&signature.credentials, self.accounts.iter().map(String::as_str), now)
            .map_err(|error| error.to_string())?;
        if user.id != self.ant {
            return Err(format!("credentials belong to {}, not {}", user.id, self.ant));
        }
        user.check_signature(bytes, &signature.signature).map_err(|error| error.to_string())?;
        Ok(Some(signature))
    }
}

struct HostVerifier(Route);

impl Verifier for HostVerifier {
    async fn verify(&self, event: &Event, stored_at: chrono::DateTime<chrono::Utc>) -> Result<(), VerifyError> {
        let link = connected(&self.0).map_err(|error| VerifyError::Unavailable(error.to_string()))?;
        AccountVerifier::new(link.accounts.clone(), HostRevocations(link)).verify(event, stored_at).await
    }
}

struct HostRevocations(Arc<Link>);

impl Revocations for HostRevocations {
    async fn revoked_at(&self, key: &str) -> Result<Option<u64>, String> {
        self.0
            .revocations
            .get_or_try_init(|| DeviceRevocations::new(&self.0.js, self.0.js_domain.as_ref()))
            .await
            .map_err(|error| format!("{error:#}"))?
            .revoked_at(key)
            .await
    }
}

#[cfg(test)]
mod tests;
