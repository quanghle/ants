//! An ant's items as sense's tools reach them ([`Items`]).

use std::time::Instant;

use ants_proto::provider::BoxFuture;
use ants_proto::{SenseId, UserId};
use chrono::{DateTime, Utc};
use serde_json::Value;

#[cfg(doc)]
use super::{Shared, Store};
use super::{StoreError, Write};
use crate::cache::Hit;
use crate::dream::ReviewTally;
use crate::items::{Item, ItemId, Revision, Values};
use crate::log::{Break, Stored};
use crate::thread::calls::Calls;
use crate::thread::experience::Experience;

/// An ant's items as sense's tools reach them: a [`Store`] its calls share ([`Shared`]).
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
    fn write_confirmed<'a>(&'a self, writes: &'a [Write], by: &'a str) -> BoxFuture<'a, Result<Vec<Item>, StoreError>>;
    /// Accepted local writes that have not yet been acknowledged by the hub.
    fn pending(&self) -> BoxFuture<'_, Result<usize, StoreError>>;
    /// The acknowledged stream sequence of an exact revision, never an offline placeholder.
    fn confirmed_sequence<'a>(
        &'a self,
        id: &'a ItemId,
        revision: u64,
    ) -> BoxFuture<'a, Result<Option<u64>, StoreError>>;
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
    fn experience<'a>(
        &'a self,
        own: &'a [UserId],
        since: Option<DateTime<Utc>>,
        top: usize,
    ) -> BoxFuture<'a, Result<Option<Experience>, StoreError>>;
    /// [`Store::review_tally`].
    fn review_tally(&self, top: usize) -> BoxFuture<'_, Result<Option<ReviewTally>, StoreError>>;
    /// [`Store::calls`].
    fn calls(&self, since: DateTime<Utc>) -> BoxFuture<'_, Result<Vec<Calls>, StoreError>>;
    /// [`Store::history`].
    /// Cached history, including pending revisions with sequence 0; never requires another network round trip.
    fn history<'a>(&'a self, id: &'a ItemId) -> BoxFuture<'a, Result<Vec<Revision>, StoreError>>;
    /// [`Store::at`].
    fn at<'a>(&'a self, id: &'a ItemId, at: DateTime<Utc>) -> BoxFuture<'a, Result<Option<Item>, StoreError>>;
    /// [`Store::sync`].
    fn sync(&self) -> BoxFuture<'_, Result<(), StoreError>>;
    /// [`Store::sync_since`].
    fn sync_since(&self, asked: Instant) -> BoxFuture<'_, Result<(), StoreError>>;
    /// [`Store::last_sync`].
    fn last_sync(&self) -> BoxFuture<'_, Result<(), StoreError>>;
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
