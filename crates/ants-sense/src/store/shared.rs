//! A store its ant's calls share ([`Shared`]): turns at the log for writes and catch-ups, reads served from the cache.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, PoisonError};
use std::time::Instant;

use ants_proto::provider::BoxFuture;
use ants_proto::{SenseId, UserId};
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{Items, Store, StoreError, Write};
use crate::cache::{Cache, CacheError, Hit};
use crate::dream::ReviewTally;
use crate::items::{Item, ItemId, Revision, Values};
use crate::log::{Break, EventLog, Signer, Stored, Verifier};
use crate::thread::calls::Calls;
use crate::thread::experience::Experience;

/// What a store's log told so far, as the last turn at it left it.
#[derive(Default)]
struct Seen {
    broken: Option<Break>,
    bytes: u64,
    earlier: Vec<UserId>,
}

/// A [`Store`] its ant's calls share ([`Items`]). A write or a catch-up takes its turn at the log ([`Shared::lock`]),
/// held across the log's round trips; a read serves the cache without one, so it waits for no other call's round
/// trip, and takes a turn only to append writes left waiting for the log.
pub struct Shared<L, S, V> {
    store: tokio::sync::Mutex<Store<L, S, V>>,
    cache: Arc<Cache>,
    sense: SenseId,
    seen: std::sync::Mutex<Seen>,
}

/// A turn at the log of a [`Shared`] store: what the log told is kept as the turn ends.
pub struct Turn<'a, L, S, V> {
    store: tokio::sync::MutexGuard<'a, Store<L, S, V>>,
    seen: &'a std::sync::Mutex<Seen>,
}

impl<L, S, V> Deref for Turn<'_, L, S, V> {
    type Target = Store<L, S, V>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl<L, S, V> DerefMut for Turn<'_, L, S, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

impl<L, S, V> Drop for Turn<'_, L, S, V> {
    fn drop(&mut self) {
        let chain = self.store.log.chain();
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        seen.bytes = chain.bytes();
        if seen.broken.as_ref() != chain.broken() {
            seen.broken = chain.broken().cloned();
        }
        if seen.earlier != chain.earlier() {
            seen.earlier = chain.earlier().to_vec();
        }
    }
}

impl<L, S, V> Shared<L, S, V> {
    pub fn new(store: Store<L, S, V>) -> Self {
        let chain = store.log.chain();
        let seen = Seen { broken: chain.broken().cloned(), bytes: chain.bytes(), earlier: chain.earlier().to_vec() };
        Self {
            cache: store.cache.clone(),
            sense: chain.sense().clone(),
            seen: std::sync::Mutex::new(seen),
            store: tokio::sync::Mutex::new(store),
        }
    }

    /// The store, once the turns asked for before have ended.
    pub async fn lock(&self) -> Turn<'_, L, S, V> {
        Turn { store: self.store.lock().await, seen: &self.seen }
    }

    fn seen(&self) -> std::sync::MutexGuard<'_, Seen> {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<L: EventLog, S: Signer, V: Verifier> Shared<L, S, V> {
    /// What `read` makes of the cache, the writes left waiting for the log appended first when it can be reached.
    async fn read<T>(&self, read: impl FnOnce(&Cache) -> Result<T, CacheError>) -> Result<T, StoreError> {
        if self.cache.read(Cache::pending_count).await? > 0 {
            self.lock().await.replay().await?;
        }
        Ok(self.cache.read(read).await?)
    }
}

impl<L, S, V> Items for Shared<L, S, V>
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
        Box::pin(async move { Ok(self.cache.read(Cache::pending_count).await?) })
    }

    fn confirmed_sequence<'a>(
        &'a self,
        id: &'a ItemId,
        revision: u64,
    ) -> BoxFuture<'a, Result<Option<u64>, StoreError>> {
        Box::pin(async move {
            let history = self.cache.read(|cache| cache.history(id)).await?;
            Ok(history
                .iter()
                .find(|entry| entry.change.revision == revision && entry.sequence != 0)
                .map(|entry| entry.sequence))
        })
    }

    fn get<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(self.read(|cache| cache.get(id)))
    }

    fn list<'a>(&'a self, kind: &'a str, limit: usize) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(self.read(move |cache| cache.list(kind, limit)))
    }

    fn list_by<'a>(
        &'a self,
        kind: &'a str,
        name: &'a str,
        value: Option<&'a Value>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Item>, StoreError>> {
        Box::pin(self.read(move |cache| cache.list_by(kind, name, value, limit)))
    }

    fn search<'a>(
        &'a self,
        text: &'a str,
        kind: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Hit>, StoreError>> {
        Box::pin(self.read(move |cache| cache.search(text, kind, limit)))
    }

    fn experience<'a>(
        &'a self,
        own: &'a [UserId],
        since: Option<DateTime<Utc>>,
        top: usize,
    ) -> BoxFuture<'a, Result<Option<Experience>, StoreError>> {
        Box::pin(self.read(move |cache| cache.experience(own, since, top)))
    }

    fn review_tally(&self, top: usize) -> BoxFuture<'_, Result<Option<ReviewTally>, StoreError>> {
        Box::pin(self.read(move |cache| cache.review_tally(top)))
    }

    fn calls(&self, since: DateTime<Utc>) -> BoxFuture<'_, Result<Vec<Calls>, StoreError>> {
        Box::pin(self.read(move |cache| cache.calls(since)))
    }

    fn history<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Vec<Revision>, StoreError>> {
        Box::pin(async move { Ok(self.cache.read(|cache| cache.history(id)).await?) })
    }

    fn at<'a>(&'a self, id: &'a ItemId, at: DateTime<Utc>) -> BoxFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(async move { Ok(self.cache.read(|cache| cache.at(id, at)).await?) })
    }

    fn sync(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.lock().await.sync().await })
    }

    fn sync_since(&self, asked: Instant) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.lock().await.sync_since(asked).await })
    }

    fn last_sync(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.lock().await.last_sync() })
    }

    fn sense(&self) -> BoxFuture<'_, SenseId> {
        Box::pin(std::future::ready(self.sense.clone()))
    }

    fn broken(&self) -> BoxFuture<'_, Option<Break>> {
        Box::pin(std::future::ready(self.seen().broken.clone()))
    }

    fn log_bytes(&self) -> BoxFuture<'_, u64> {
        Box::pin(std::future::ready(self.seen().bytes))
    }

    fn earlier_names(&self) -> BoxFuture<'_, Vec<UserId>> {
        Box::pin(std::future::ready(self.seen().earlier.clone()))
    }

    fn messages(&self) -> BoxFuture<'_, Result<Vec<Stored>, StoreError>> {
        Box::pin(async move { self.lock().await.messages().await })
    }
}
