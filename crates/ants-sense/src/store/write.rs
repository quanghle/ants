//! The changes of one write of an ant's items ([`Write`]) and a write decided again while others change what it
//! read ([`read_modify_write`]).

use super::StoreError;
use crate::cache::Cache;
use crate::items::{Change, Item, ItemId, Values};

/// One change of a [`Store::write`](super::Store::write).
#[derive(Debug, Clone, PartialEq)]
pub enum Write {
    /// Create item `id`, a new id ([`ItemId::new`]) minted by the caller, so other writes can name it.
    Create { id: ItemId, kind: String, values: Values },
    /// [`Store::change`](super::Store::change).
    Change { id: ItemId, values: Values, expected: Option<u64> },
    /// [`Store::put`](super::Store::put).
    Put { id: ItemId, kind: String, values: Values, expected: Option<u64> },
}

impl Write {
    pub fn id(&self) -> &ItemId {
        match self {
            Self::Create { id, .. } | Self::Change { id, .. } | Self::Put { id, .. } => id,
        }
    }

    /// Its change, decided on `cache`, or the item it leaves unchanged.
    pub(super) fn decide(&self, cache: &Cache, by: &str) -> Result<Result<Change, Item>, StoreError> {
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

/// How many times a read-modify-write of items is made before it gives up ([`read_modify_write`]). Each write of one
/// is appended to the log in turn by [`APPEND_TRIES`](crate::log::APPEND_TRIES) attempts of its own, decided again on what others appended.
pub const TRIES: usize = 8;

/// Read items, decide on them and write the change at the revisions read, by `attempt`: made again from the reading
/// while the write is refused as another writer changed an item meanwhile ([`StoreError::Conflict`]), [`TRIES`] times
/// at most, then given up as [`StoreError::Contended`]. An attempt changing several items writes them at once
/// ([`Items::write`](super::Items::write)), so all of them change or none.
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
