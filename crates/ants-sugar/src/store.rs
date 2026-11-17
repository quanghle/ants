//! An ant's items, written to its log and read from its cache ([`Store`]), and how sugar's tools reach the store of
//! the ant they serve ([`Stores`], [`Items`]).

use std::sync::Arc;

use ants_proto::provider::BoxFuture;
use ants_proto::{SugarId, UserId};
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::cache::{Cache, CacheError, Hit};
use crate::items::{Batch, Change, ITEM_EVENT, ITEMS_EVENT, Item, ItemId, Revision, Values};
use crate::log::{APPEND_TRIES, AntLog, Checked, EventLog, LogError, Position, Signer, Verifier};

/// Why a store did not answer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("no item {0}")]
    NotFound(ItemId),
    /// A change expected another revision of the item than its current one, given.
    #[error("item {} is at revision {}, not {expected}", current.id, current.revision)]
    Conflict { expected: u64, current: Box<Item> },
    #[error("sugar's data is not available: {0}")]
    Unavailable(String),
    /// sugar keeps no data of the ant here, as no daemon connection reaches its log: nothing is to be written.
    #[error("sugar keeps no data here: {0}")]
    NotKept(String),
    #[error("the log: {0}")]
    Log(#[from] LogError),
    #[error("the cache: {0}")]
    Cache(#[from] CacheError),
}

/// The items of an ant's sugar identity: each change appended to its log first ([`AntLog`]), then applied to its
/// cache ([`Cache`]), which every read reads. A change is decided on the cache caught up with the log: when another
/// writer appended since, the log refuses the append, and the change is decided again on what that writer appended.
pub struct Store<L, S, V> {
    log: AntLog<L, S, V>,
    cache: Cache,
}

impl<L: EventLog, S: Signer, V: Verifier> Store<L, S, V> {
    /// The items of `log`, cached in `cache`: kept when built from this log up to where it still matches, else
    /// emptied and built again from the log's start; then caught up with the log ([`Store::sync`]).
    pub async fn open(mut log: AntLog<L, S, V>, mut cache: Cache) -> Result<Self, StoreError> {
        let sugar = log.chain().sugar().clone();
        let incarnation = log.log().incarnation().to_owned();
        let built =
            cache.built().ok().flatten().filter(|built| built.sugar == sugar && built.incarnation == incarnation);
        let position = match built {
            Some(built) if holds(log.log(), &built).await? => built.position,
            _ => {
                cache.reset(&sugar, &incarnation)?;
                None
            }
        };
        log.resume(position);
        let mut store = Self { log, cache };
        store.sync().await?;
        Ok(store)
    }

    pub fn log(&self) -> &AntLog<L, S, V> {
        &self.log
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Apply to the cache what the log holds past what the cache was built from.
    pub async fn sync(&mut self) -> Result<(), StoreError> {
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

    /// The items matching `text` best ([`Cache::search`]).
    pub fn search(&self, text: &str, kind: Option<&str>, limit: usize) -> Result<Vec<Hit>, StoreError> {
        Ok(self.cache.search(text, kind, limit)?)
    }

    /// The changes of item `id`, in log order.
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
        mut next: impl FnMut(&Cache) -> Result<Vec<Result<Change, Item>>, StoreError>,
    ) -> Result<Vec<Item>, StoreError> {
        for _ in 0..APPEND_TRIES {
            let decided = next(&self.cache)?;
            let changes: Vec<&Change> = decided.iter().filter_map(|decided| decided.as_ref().ok()).collect();
            let (kind, data) = match changes[..] {
                [] => return Ok(decided.into_iter().filter_map(Result::err).collect()),
                [change] => (ITEM_EVENT, serde_json::to_value(change)),
                _ => (ITEMS_EVENT, serde_json::to_value(Batch { changes: changes.into_iter().cloned().collect() })),
            };
            match self.log.try_append(kind, data.map_err(CacheError::from)?).await {
                Ok(Ok(appended)) => {
                    self.apply(std::slice::from_ref(&appended))?;
                    return decided.into_iter().map(|decided| self.applied(decided)).collect();
                }
                Ok(Err(missed)) => self.apply(&missed)?,
                Err(e) => {
                    self.realign();
                    return Err(e.into());
                }
            }
        }
        Err(StoreError::Unavailable(format!("other writers kept appending to sugar identity {}", self.sugar())))
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

    fn sugar(&self) -> &SugarId {
        self.log.chain().sugar()
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
    if log.last_sequence(&built.sugar).await? < *sequence {
        return Ok(false);
    }
    let read = log.read_after(&built.sugar, sequence - 1, 1).await?;
    Ok(read.first().is_some_and(|stored| stored.sequence == *sequence && stored.sha256() == tip.sha256))
}

/// An ant's items as sugar's tools reach them, each call taking its turn: a [`Store`] behind a lock.
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
    /// [`Store::history`].
    fn history<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Vec<Revision>, StoreError>>;
    /// [`Store::at`].
    fn at<'a>(&'a self, id: &'a ItemId, at: DateTime<Utc>) -> BoxFuture<'a, Result<Option<Item>, StoreError>>;
    /// [`Store::sync`].
    fn sync(&self) -> BoxFuture<'_, Result<(), StoreError>>;
    /// The sugar identity whose items these are.
    fn sugar(&self) -> BoxFuture<'_, SugarId>;
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

    fn get<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(async move { self.lock().await.get(id) })
    }

    fn list<'a>(&'a self, kind: &'a str, limit: usize) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(async move { self.lock().await.list(kind, limit) })
    }

    fn list_by<'a>(
        &'a self,
        kind: &'a str,
        name: &'a str,
        value: Option<&'a Value>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(async move { self.lock().await.list_by(kind, name, value, limit) })
    }

    fn search<'a>(
        &'a self,
        text: &'a str,
        kind: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Hit>, StoreError>> {
        Box::pin(async move { self.lock().await.search(text, kind, limit) })
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

    fn sugar(&self) -> BoxFuture<'_, SugarId> {
        Box::pin(async move { self.lock().await.sugar().clone() })
    }
}

/// What finds the items of each ant a daemon serves, opening the ant's [`Store`] on first use: the daemon provides it
/// ([`crate::Sugar::with_stores`]), as it holds the ant's home, credentials and connection.
pub trait Stores: Send + Sync {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>>;
}

#[cfg(test)]
mod tests;
