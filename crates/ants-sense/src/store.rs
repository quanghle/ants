//! An ant's items, written to its log and read from its cache ([`Store`]), and how sense's tools reach the store of
//! the ant they serve ([`Stores`], [`Items`]).

use std::sync::Arc;

use ants_proto::provider::BoxFuture;
use ants_proto::{Machine, SenseId, UserId};
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::cache::{Cache, CacheError, Hit, Pending};
use crate::export::SenseExport;
use crate::items::{Batch, Change, ITEM_EVENT, ITEMS_EVENT, Item, ItemId, Revision, Values};
use crate::log::{
    APPEND_TRIES, AntLog, Break, Checked, EventLog, LogError, LogErrorKind, Position, Signer, Stored, Verifier,
};
use crate::thread::experience::Experience;

/// Why a store did not answer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("no item {0}")]
    NotFound(ItemId),
    /// A change expected another revision of the item than its current one, given.
    #[error("item {} is at revision {}, not {expected}", current.id, current.revision)]
    Conflict { expected: u64, current: Box<Item> },
    /// Other writers kept changing the item between a read of it and the write decided on it, [`TRIES`] times
    /// ([`read_modify_write`]).
    #[error("item {0} kept changing meanwhile")]
    Contended(ItemId),
    #[error("sense's data is not available: {0}")]
    Unavailable(String),
    /// No daemon connection has opened the ant's log here, and no existing cache can serve it.
    #[error("sense keeps no data here: {0}")]
    NotKept(String),
    #[error("the log: {0}")]
    Log(#[from] LogError),
    #[error("the cache: {0}")]
    Cache(#[from] CacheError),
    #[error("{0}")]
    Import(#[from] crate::export::ImportError),
}

/// The items of an ant's sense identity, projected from its verified log into a cache. Normally the log accepts a
/// change before the cache applies it. During a connectivity failure, a durable outbox accepts decided changes and
/// overlays them on the cached projection; reconnect appends them in order, preserving each batch and revision.
/// Concurrent remote changes that invalidate queued changes are reported and the affected writes remain queued.
pub struct Store<L, S, V> {
    log: AntLog<L, S, V>,
    cache: Cache,
    machine: Machine,
}

impl<L: EventLog, S: Signer, V: Verifier> Store<L, S, V> {
    /// The items of `log`, cached in `cache`: kept when built from this log up to where it still matches, else
    /// emptied and built again from the log's start; then caught up with the log ([`Store::sync`]).
    pub async fn open(mut log: AntLog<L, S, V>, mut cache: Cache) -> Result<Self, StoreError> {
        let sense = log.chain().sense().clone();
        let incarnation = log.log().incarnation().to_owned();
        let built =
            cache.built().ok().flatten().filter(|built| built.sense == sense && built.incarnation == incarnation);
        let position = match built {
            Some(built) => match holds(log.log(), &built).await {
                Ok(true) => built.position,
                Err(error) if is_offline(&error) => built.position,
                Err(error) => return Err(error.into()),
                Ok(false) => {
                    cache.reset(&sense, &incarnation)?;
                    None
                }
            },
            _ => {
                cache.reset(&sense, &incarnation)?;
                None
            }
        };
        log.resume(position);
        let mut store = Self { log, cache, machine: "offline".parse().expect("a machine name") };
        match store.sync().await {
            Err(StoreError::Log(error)) if is_offline(&error) => {}
            Err(StoreError::Unavailable(_)) if store.cache.pending_conflict()?.is_some() => {}
            result => result?,
        }
        Ok(store)
    }

    /// Identify locally queued changes before the hub supplies their event stamps.
    #[must_use]
    pub fn with_machine(mut self, machine: Machine) -> Self {
        self.machine = machine;
        self
    }

    pub fn log(&self) -> &AntLog<L, S, V> {
        &self.log
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Catch up with the verified log, then append queued local changes in order. A conflict retains the affected
    /// write and those after it, returning an error rather than overwriting either side.
    pub async fn sync(&mut self) -> Result<(), StoreError> {
        self.catch_up().await?;
        self.flush().await
    }

    async fn catch_up(&mut self) -> Result<(), StoreError> {
        loop {
            let read = match self.log.read_next().await {
                Ok(read) => read,
                Err(e) => {
                    self.realign();
                    return Err(e.into());
                }
            };
            let Some(events) = read else { return self.apply(&[]) };
            self.apply(&events)?;
        }
    }

    /// Create an item of `kind` with `values`, for `by` ([`crate::items::Stamp::by`]).
    pub async fn create(&mut self, kind: &str, values: Values, by: &str) -> Result<Item, StoreError> {
        let write = Write::Create { id: ItemId::new(), kind: kind.to_owned(), values };
        Ok(self.write(std::slice::from_ref(&write), by).await?.remove(0))
    }

    /// The item of `kind`, created with `values` for `by` when there is none, with whether it was: for kinds of which
    /// an ant has one, such as its card. Two writers creating it at once create one, the second finding the first's.
    pub async fn single(&mut self, kind: &str, values: Values, by: &str) -> Result<(Item, bool), StoreError> {
        let change = Change::create(kind, values, by);
        let item = self
            .commit(|cache| match cache.list(kind, 1)?.pop() {
                Some(item) => Ok(vec![Err(item)]),
                None => Ok(vec![Ok(change.clone())]),
            })
            .await?
            .remove(0);
        let created = item.id == change.id;
        Ok((item, created))
    }

    /// Set `values` of item `id`, null removing one, for `by`: refused when `expected` is given and the item is at
    /// another revision ([`StoreError::Conflict`], with the item). Changing nothing appends nothing.
    pub async fn change(
        &mut self,
        id: &ItemId,
        values: Values,
        expected: Option<u64>,
        by: &str,
    ) -> Result<Item, StoreError> {
        let write = Write::Change { id: id.clone(), values, expected };
        Ok(self.write(std::slice::from_ref(&write), by).await?.remove(0))
    }

    /// Set `values` of item `id`, creating it of `kind` with them when missing, for `by`: refused when `expected` is
    /// given and the item is at another revision, 0 standing for no item ([`StoreError::Conflict`] with the item, or
    /// [`StoreError::NotFound`] for an item expected but missing). For an item of a known id ([`ItemId::named`]).
    pub async fn put(
        &mut self,
        id: &ItemId,
        kind: &str,
        values: Values,
        expected: Option<u64>,
        by: &str,
    ) -> Result<Item, StoreError> {
        let write = Write::Put { id: id.clone(), kind: kind.to_owned(), values, expected };
        Ok(self.write(std::slice::from_ref(&write), by).await?.remove(0))
    }

    /// Make `writes`, each of another item, at once, for `by`: one event of the log, so all of them apply or none
    /// does. Refused as a whole when one is ([`Store::change`]); a change changing nothing appends nothing. Returns
    /// the items as the writes leave them, in order.
    pub async fn write(&mut self, writes: &[Write], by: &str) -> Result<Vec<Item>, StoreError> {
        for (i, write) in writes.iter().enumerate() {
            if writes[..i].iter().any(|other| other.id() == write.id()) {
                return Err(StoreError::Unavailable(format!("item {} written twice at once", write.id())));
            }
        }
        self.commit(|cache| writes.iter().map(|write| write.decide(cache, by)).collect()).await
    }

    /// A write requiring a hub acknowledgment, never accepted into the offline outbox.
    pub async fn write_confirmed(&mut self, writes: &[Write], by: &str) -> Result<Vec<Item>, StoreError> {
        for (index, write) in writes.iter().enumerate() {
            if writes[..index].iter().any(|other| other.id() == write.id()) {
                return Err(StoreError::Unavailable(format!("item {} written twice at once", write.id())));
            }
        }
        self.sync().await?;
        self.check_integrity()?;
        self.commit_inner(|cache| writes.iter().map(|write| write.decide(cache, by)).collect(), false).await
    }

    /// Every message of the log, as stored, for an export ([`crate::export`]): once caught up, with no change waiting to
    /// be appended and the chain whole.
    pub async fn messages(&mut self) -> Result<Vec<Stored>, StoreError> {
        self.sync().await?;
        if self.pending()? > 0 {
            return Err(StoreError::Unavailable("changes wait to be appended to the log".into()));
        }
        self.check_chain()?;
        Ok(self.log.messages().await?)
    }

    pub fn pending(&self) -> Result<usize, StoreError> {
        Ok(self.cache.pending_count()?)
    }

    pub fn get(&self, id: &ItemId) -> Result<Option<Item>, StoreError> {
        Ok(self.cache.get(id)?)
    }

    /// The items of `kind`, the last changed first, up to `limit`.
    pub fn list(&self, kind: &str, limit: usize) -> Result<Vec<Item>, StoreError> {
        Ok(self.cache.list(kind, limit)?)
    }

    /// The items of `kind` whose value `name` is `value`, or which have no value `name` when `value` is none, the last
    /// changed first, up to `limit`.
    pub fn list_by(
        &self,
        kind: &str,
        name: &str,
        value: Option<&Value>,
        limit: usize,
    ) -> Result<Vec<Item>, StoreError> {
        Ok(self.cache.list_by(kind, name, value, limit)?)
    }

    /// [`Cache::experience`].
    pub fn experience(&self, own: &[UserId], top: usize) -> Result<Option<Experience>, StoreError> {
        Ok(self.cache.experience(own, top)?)
    }

    /// The items matching `text` best ([`Cache::search`]).
    pub fn search(&self, text: &str, kind: Option<&str>, limit: usize) -> Result<Vec<Hit>, StoreError> {
        Ok(self.cache.search(text, kind, limit)?)
    }

    /// The changes of item `id`, in order. Unacknowledged local changes have sequence 0, never a fencing token.
    pub fn history(&self, id: &ItemId) -> Result<Vec<Revision>, StoreError> {
        Ok(self.cache.history(id)?)
    }

    /// Item `id` as it was at time `at`; none when it did not exist yet.
    pub fn at(&self, id: &ItemId, at: DateTime<Utc>) -> Result<Option<Item>, StoreError> {
        Ok(self.cache.at(id, at)?)
    }

    /// Append the changes `next` decides on the cache, each a change or the item it leaves unchanged, as one event,
    /// and return the items they leave; decided again on what another writer appended first.
    async fn commit(
        &mut self,
        next: impl FnMut(&Cache) -> Result<Vec<Result<Change, Item>>, StoreError>,
    ) -> Result<Vec<Item>, StoreError> {
        self.commit_inner(next, true).await
    }

    async fn commit_inner(
        &mut self,
        mut next: impl FnMut(&Cache) -> Result<Vec<Result<Change, Item>>, StoreError>,
        queue: bool,
    ) -> Result<Vec<Item>, StoreError> {
        if self.pending()? > 0 {
            match self.sync().await {
                Err(StoreError::Log(error)) if queue && is_offline(&error) => {
                    // Trying the oldest write preserves order and rechecks the signer before accepting more.
                    match self.flush().await {
                        Err(StoreError::Log(error)) if is_offline(&error) => {}
                        result => result?,
                    }
                }
                result => result?,
            }
        }
        for _ in 0..APPEND_TRIES {
            let decided = next(&self.cache)?;
            let changes: Vec<&Change> = decided.iter().filter_map(|decided| decided.as_ref().ok()).collect();
            let (kind, data) = match changes[..] {
                [] => return Ok(decided.into_iter().filter_map(Result::err).collect()),
                [change] => (ITEM_EVENT, serde_json::to_value(change)),
                _ => (ITEMS_EVENT, serde_json::to_value(Batch { changes: changes.into_iter().cloned().collect() })),
            };
            let mut pending = Pending::new(kind, data.map_err(CacheError::from)?, self.machine.clone());
            pending.guards = decided
                .iter()
                .filter_map(|item| item.as_ref().err())
                .map(|item| (item.id.clone(), item.revision))
                .collect();
            if self.pending()? > 0 {
                self.check_integrity()?;
                self.cache.enqueue(&pending)?;
                return decided.into_iter().map(|decided| self.applied(decided)).collect();
            }
            match self.log.try_append(kind, pending.data.clone()).await {
                Ok(Ok(appended)) => {
                    self.apply(std::slice::from_ref(&appended))?;
                    return decided.into_iter().map(|decided| self.applied(decided)).collect();
                }
                Ok(Err(missed)) => {
                    self.apply(&missed)?;
                }
                Err(e) => {
                    self.realign();
                    if queue && is_offline(&e) {
                        self.check_integrity()?;
                        self.cache.enqueue(&pending)?;
                        return decided.into_iter().map(|decided| self.applied(decided)).collect();
                    }
                    return Err(e.into());
                }
            }
        }
        Err(StoreError::Unavailable(format!("other writers kept appending to sense identity {}", self.sense())))
    }

    async fn flush(&mut self) -> Result<(), StoreError> {
        if self.pending()? == 0 {
            return Ok(());
        }
        self.check_chain()?;
        let mut conflicts = 0;
        while let Some(pending) = self.cache.pending()?.first().cloned() {
            if self.cache.pending_ready()? == 0
                && let Some(reason) = self.cache.pending_conflict()?
            {
                return Err(StoreError::Unavailable(reason));
            }
            match self.log.try_append(&pending.kind, pending.data.clone()).await {
                Ok(Ok(appended)) => self.apply(std::slice::from_ref(&appended))?,
                Ok(Err(missed)) => {
                    self.apply(&missed)?;
                    self.check_chain()?;
                    conflicts += 1;
                    if conflicts >= APPEND_TRIES {
                        return Err(StoreError::Unavailable(
                            "other writers kept appending; pending writes retained".into(),
                        ));
                    }
                }
                Err(error) => {
                    self.realign();
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    async fn refresh(&mut self) -> Result<(), StoreError> {
        match self.sync().await {
            Err(StoreError::Log(error)) if is_offline(&error) => Ok(()),
            result => result,
        }
    }

    fn check_integrity(&self) -> Result<(), StoreError> {
        self.check_chain()?;
        if let Some(reason) = self.cache.pending_conflict()? {
            return Err(StoreError::Unavailable(reason));
        }
        Ok(())
    }

    fn check_chain(&self) -> Result<(), StoreError> {
        if let Some(broken) = self.log.chain().broken() {
            return Err(StoreError::Unavailable(format!(
                "sense log chain is broken at event {}: {}",
                broken.index, broken.reason
            )));
        }
        Ok(())
    }

    /// The item `decided` leaves, as the cache has it once applied.
    fn applied(&self, decided: Result<Change, Item>) -> Result<Item, StoreError> {
        let change = match decided {
            Ok(change) => change,
            Err(unchanged) => return Ok(unchanged),
        };
        let item = self.cache.get(&change.id)?.filter(|item| item.revision == change.revision);
        item.ok_or_else(|| StoreError::Unavailable(format!("the change of item {} did not apply", change.id)))
    }

    fn sense(&self) -> &SenseId {
        self.log.chain().sense()
    }

    /// Apply `events`, just read, to the cache, with how far the log was read.
    fn apply(&mut self, events: &[Checked]) -> Result<(), StoreError> {
        let position = self.log.chain().position();
        self.cache.apply(events, position.as_ref()).map_err(|e| {
            self.realign();
            e.into()
        })
    }

    /// Read the log again from where the cache was built to, after what was read past it failed to apply or was lost.
    fn realign(&mut self) {
        let position = self.cache.built().ok().flatten().and_then(|built| built.position);
        self.log.resume(position);
    }
}

/// Only an unreachable store permits accepting a local write. Other failures must remain visible.
pub fn is_offline(error: &LogError) -> bool {
    error.kind == LogErrorKind::Unreachable
}

/// One change of a [`Store::write`].
#[derive(Debug, Clone, PartialEq)]
pub enum Write {
    /// Create item `id`, a new id ([`ItemId::new`]) minted by the caller, so other writes can name it.
    Create { id: ItemId, kind: String, values: Values },
    /// [`Store::change`].
    Change { id: ItemId, values: Values, expected: Option<u64> },
    /// [`Store::put`].
    Put { id: ItemId, kind: String, values: Values, expected: Option<u64> },
}

impl Write {
    pub fn id(&self) -> &ItemId {
        match self {
            Self::Create { id, .. } | Self::Change { id, .. } | Self::Put { id, .. } => id,
        }
    }

    /// Its change, decided on `cache`, or the item it leaves unchanged.
    fn decide(&self, cache: &Cache, by: &str) -> Result<Result<Change, Item>, StoreError> {
        match self {
            Self::Create { id, kind, values } => {
                if cache.get(id)?.is_some() {
                    return Err(StoreError::Unavailable(format!("item {id} exists already")));
                }
                Ok(Ok(Change { id: id.clone(), ..Change::create(kind, values.clone(), by) }))
            }
            Self::Change { id, values, expected } => {
                let current = cache.get(id)?.ok_or_else(|| StoreError::NotFound(id.clone()))?;
                if let Some(expected) = expected
                    && *expected != current.revision
                {
                    return Err(StoreError::Conflict { expected: *expected, current: Box::new(current) });
                }
                Ok(Change::of(&current, values.clone(), by).ok_or(current))
            }
            Self::Put { id, kind, values, expected } => match cache.get(id)? {
                None if expected.unwrap_or_default() == 0 => {
                    Ok(Ok(Change { id: id.clone(), ..Change::create(kind, values.clone(), by) }))
                }
                None => Err(StoreError::NotFound(id.clone())),
                Some(current) if expected.is_some_and(|expected| expected != current.revision) => {
                    Err(StoreError::Conflict { expected: expected.unwrap_or_default(), current: Box::new(current) })
                }
                Some(current) => Ok(Change::of(&current, values.clone(), by).ok_or(current)),
            },
        }
    }
}

/// Whether the log in `log` still holds the message the cache `built` was last built from, unchanged.
async fn holds(log: &impl EventLog, built: &crate::cache::Built) -> Result<bool, LogError> {
    let Some(Position { sequence, tip, .. }) = &built.position else { return Ok(true) };
    if log.last_sequence(&built.sense).await? < *sequence {
        return Ok(false);
    }
    let read = log.read_after(&built.sense, sequence - 1, 1).await?;
    Ok(read.first().is_some_and(|stored| stored.sequence == *sequence && stored.sha256() == tip.sha256))
}

/// How many times a read-modify-write of items is made before it gives up ([`read_modify_write`]). Each write of one
/// is appended to the log in turn by [`APPEND_TRIES`] attempts of its own, decided again on what others appended.
pub const TRIES: usize = 8;

/// Read items, decide on them and write the change at the revisions read, by `attempt`: made again from the reading
/// while the write is refused as another writer changed an item meanwhile ([`StoreError::Conflict`]), [`TRIES`] times
/// at most, then given up as [`StoreError::Contended`]. An attempt changing several items writes them at once
/// ([`Items::write`]), so all of them change or none.
pub async fn read_modify_write<T, F>(mut attempt: impl FnMut() -> F) -> Result<T, StoreError>
where
    F: Future<Output = Result<T, StoreError>>,
{
    let mut tries = 1;
    loop {
        match attempt().await {
            Err(StoreError::Conflict { current, .. }) if tries >= TRIES => {
                return Err(StoreError::Contended(current.id));
            }
            Err(StoreError::Conflict { .. }) => tries += 1,
            result => return result,
        }
    }
}

/// An ant's items as sense's tools reach them, each call taking its turn: a [`Store`] behind a lock.
pub trait Items: Send + Sync {
    /// [`Store::create`].
    fn create<'a>(&'a self, kind: &'a str, values: Values, by: &'a str) -> BoxFuture<'a, Result<Item, StoreError>>;
    /// [`Store::single`].
    fn single<'a>(
        &'a self,
        kind: &'a str,
        values: Values,
        by: &'a str,
    ) -> BoxFuture<'a, Result<(Item, bool), StoreError>>;
    /// [`Store::change`].
    fn change<'a>(
        &'a self,
        id: &'a ItemId,
        values: Values,
        expected: Option<u64>,
        by: &'a str,
    ) -> BoxFuture<'a, Result<Item, StoreError>>;
    /// [`Store::put`].
    fn put<'a>(
        &'a self,
        id: &'a ItemId,
        kind: &'a str,
        values: Values,
        expected: Option<u64>,
        by: &'a str,
    ) -> BoxFuture<'a, Result<Item, StoreError>>;
    /// [`Store::write`].
    fn write<'a>(&'a self, writes: &'a [Write], by: &'a str) -> BoxFuture<'a, Result<Vec<Item>, StoreError>>;
    /// Like [`Items::write`], but never queue a write without a hub acknowledgment.
    fn write_confirmed<'a>(&'a self, writes: &'a [Write], by: &'a str) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        self.write(writes, by)
    }
    /// Accepted local writes that have not yet been acknowledged by the hub.
    fn pending(&self) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(async { Ok(0) })
    }
    /// The acknowledged stream sequence of an exact revision, never an offline placeholder.
    fn confirmed_sequence<'a>(
        &'a self,
        id: &'a ItemId,
        revision: u64,
    ) -> BoxFuture<'a, Result<Option<u64>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .history(id)
                .await?
                .iter()
                .find(|entry| entry.change.revision == revision && entry.sequence != 0)
                .map(|entry| entry.sequence))
        })
    }
    fn get<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Option<Item>, StoreError>>;
    /// [`Store::list`].
    fn list<'a>(&'a self, kind: &'a str, limit: usize) -> BoxFuture<'a, Result<Vec<Item>, StoreError>>;
    /// [`Store::list_by`].
    fn list_by<'a>(
        &'a self,
        kind: &'a str,
        name: &'a str,
        value: Option<&'a Value>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Item>, StoreError>>;
    /// [`Store::search`].
    fn search<'a>(
        &'a self,
        text: &'a str,
        kind: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Hit>, StoreError>>;
    /// [`Store::experience`].
    fn experience<'a>(&'a self, own: &'a [UserId], top: usize)
    -> BoxFuture<'a, Result<Option<Experience>, StoreError>>;
    /// [`Store::history`].
    /// Cached history, including pending revisions with sequence 0; never requires another network round trip.
    fn history<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Vec<Revision>, StoreError>>;
    /// [`Store::at`].
    fn at<'a>(&'a self, id: &'a ItemId, at: DateTime<Utc>) -> BoxFuture<'a, Result<Option<Item>, StoreError>>;
    /// [`Store::sync`].
    fn sync(&self) -> BoxFuture<'_, Result<(), StoreError>>;
    /// The sense identity whose items these are.
    fn sense(&self) -> BoxFuture<'_, SenseId>;
    /// The first break of the log read so far ([`crate::log::Chain::broken`]).
    fn broken(&self) -> BoxFuture<'_, Option<Break>>;
    /// The bytes of the log read so far ([`crate::log::Chain::bytes`]).
    fn log_bytes(&self) -> BoxFuture<'_, u64>;
    /// The ant's earlier names, whose events in the log are its own ([`crate::log::Chain::earlier`]).
    fn earlier_names(&self) -> BoxFuture<'_, Vec<UserId>>;
    /// [`Store::messages`].
    fn messages(&self) -> BoxFuture<'_, Result<Vec<Stored>, StoreError>>;
}

impl<L, S, V> Items for tokio::sync::Mutex<Store<L, S, V>>
where
    L: EventLog + Send + Sync,
    S: Signer + Send + Sync,
    V: Verifier + Send + Sync,
{
    fn create<'a>(&'a self, kind: &'a str, values: Values, by: &'a str) -> BoxFuture<'a, Result<Item, StoreError>> {
        Box::pin(async move { self.lock().await.create(kind, values, by).await })
    }

    fn single<'a>(
        &'a self,
        kind: &'a str,
        values: Values,
        by: &'a str,
    ) -> BoxFuture<'a, Result<(Item, bool), StoreError>> {
        Box::pin(async move { self.lock().await.single(kind, values, by).await })
    }

    fn change<'a>(
        &'a self,
        id: &'a ItemId,
        values: Values,
        expected: Option<u64>,
        by: &'a str,
    ) -> BoxFuture<'a, Result<Item, StoreError>> {
        Box::pin(async move { self.lock().await.change(id, values, expected, by).await })
    }

    fn put<'a>(
        &'a self,
        id: &'a ItemId,
        kind: &'a str,
        values: Values,
        expected: Option<u64>,
        by: &'a str,
    ) -> BoxFuture<'a, Result<Item, StoreError>> {
        Box::pin(async move { self.lock().await.put(id, kind, values, expected, by).await })
    }

    fn write<'a>(&'a self, writes: &'a [Write], by: &'a str) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(async move { self.lock().await.write(writes, by).await })
    }

    fn write_confirmed<'a>(&'a self, writes: &'a [Write], by: &'a str) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(async move { self.lock().await.write_confirmed(writes, by).await })
    }

    fn pending(&self) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(async move { self.lock().await.pending() })
    }

    fn confirmed_sequence<'a>(
        &'a self,
        id: &'a ItemId,
        revision: u64,
    ) -> BoxFuture<'a, Result<Option<u64>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .lock()
                .await
                .history(id)?
                .iter()
                .find(|entry| entry.change.revision == revision && entry.sequence != 0)
                .map(|entry| entry.sequence))
        })
    }

    fn get<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(async move {
            let mut store = self.lock().await;
            store.refresh().await?;
            store.get(id)
        })
    }

    fn list<'a>(&'a self, kind: &'a str, limit: usize) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(async move {
            let mut store = self.lock().await;
            store.refresh().await?;
            store.list(kind, limit)
        })
    }

    fn list_by<'a>(
        &'a self,
        kind: &'a str,
        name: &'a str,
        value: Option<&'a Value>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(async move {
            let mut store = self.lock().await;
            store.refresh().await?;
            store.list_by(kind, name, value, limit)
        })
    }

    fn search<'a>(
        &'a self,
        text: &'a str,
        kind: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Hit>, StoreError>> {
        Box::pin(async move {
            let mut store = self.lock().await;
            store.refresh().await?;
            store.search(text, kind, limit)
        })
    }

    fn experience<'a>(
        &'a self,
        own: &'a [UserId],
        top: usize,
    ) -> BoxFuture<'a, Result<Option<Experience>, StoreError>> {
        Box::pin(async move {
            let mut store = self.lock().await;
            store.refresh().await?;
            store.experience(own, top)
        })
    }

    fn history<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Vec<Revision>, StoreError>> {
        Box::pin(async move { self.lock().await.history(id) })
    }

    fn at<'a>(&'a self, id: &'a ItemId, at: DateTime<Utc>) -> BoxFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(async move { self.lock().await.at(id, at) })
    }

    fn sync(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.lock().await.sync().await })
    }

    fn sense(&self) -> BoxFuture<'_, SenseId> {
        Box::pin(async move { self.lock().await.sense().clone() })
    }

    fn broken(&self) -> BoxFuture<'_, Option<Break>> {
        Box::pin(async move { self.lock().await.log().chain().broken().cloned() })
    }

    fn log_bytes(&self) -> BoxFuture<'_, u64> {
        Box::pin(async move { self.lock().await.log().chain().bytes() })
    }

    fn earlier_names(&self) -> BoxFuture<'_, Vec<UserId>> {
        Box::pin(async move { self.lock().await.log().chain().earlier().to_vec() })
    }

    fn messages(&self) -> BoxFuture<'_, Result<Vec<Stored>, StoreError>> {
        Box::pin(async move { self.lock().await.messages().await })
    }
}

/// What finds the items of each ant a daemon serves, opening the ant's [`Store`] on first use: the daemon provides it
/// ([`crate::Sense::with_stores`]), as it holds the ant's home, credentials and connection.
pub trait Stores: Send + Sync {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>>;

    /// The export of `ant`'s sense data, to take to another bus ([`crate::export`]).
    fn export<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<SenseExport, StoreError>> {
        Box::pin(std::future::ready(Err(StoreError::NotKept(format!("no export of {ant}")))))
    }

    /// The home of `ant`, where its skill files are kept ([`crate::skill::materialize`]); none when not known.
    fn home(&self, ant: &UserId) -> Option<std::path::PathBuf> {
        let _ = ant;
        None
    }

    /// Take in `export` as `ant`'s sense data ([`crate::export::import`]).
    fn import<'a>(&'a self, ant: &'a UserId, export: SenseExport) -> BoxFuture<'a, Result<(), StoreError>> {
        drop(export);
        Box::pin(std::future::ready(Err(StoreError::NotKept(format!("no import of {ant}")))))
    }
}

#[cfg(test)]
mod tests;
