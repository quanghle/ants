//! sense's data as items: memories, the identity card, checkpoints and the like, each a set of values of one kind
//! ([`Item`]). Each change of an item is an event of the ant's log ([`ITEM_EVENT`], holding a [`Change`]) carrying the
//! item's next revision and the changed values as they were and became, so an item's state at a past time can be
//! told from its changes ([`state_at`]) and a change can be made only from the revision it expects.

use std::fmt;
use std::str::FromStr;

use ants_proto::Machine;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::store::StoreError;

/// The kind of the log events changing an item, whose data is a [`Change`].
pub const ITEM_EVENT: &str = "item";
/// The kind of the log events changing several items at once, whose data is a [`Batch`]: all its changes apply, or,
/// when one does not follow on its item, none.
pub const ITEMS_EVENT: &str = "items";
/// The value of an item searched first ([`crate::cache::Cache::search`]).
pub const TITLE: &str = "title";
/// The value of an item searched after its title.
pub const BODY: &str = "body";

/// An item's values by name; null stands for none.
pub type Values = Map<String, Value>;

/// The alphabet of a ULID: Crockford's base32.
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// The id of an item: a ULID minted by the writer creating it, so no log position is needed to name it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ItemId(String);

impl ItemId {
    /// A new id: a ULID of the time now, with the random bits of a UUID v7.
    pub fn new() -> Self {
        Self::of(Uuid::now_v7().as_u128())
    }

    /// The id of the item of `kind` named `name`, the same for every writer: for an item there is one of per name,
    /// such as a thread's resume register, so writers creating it at once create the same item.
    pub fn named(kind: &str, name: &str) -> Self {
        let digest = Sha256::new().chain_update(kind).chain_update([0]).chain_update(name).finalize();
        Self::of(u128::from_be_bytes(digest[..16].try_into().expect("16 bytes")))
    }

    fn of(value: u128) -> Self {
        Self((0..26).map(|i| char::from(CROCKFORD[((value >> (125 - 5 * i)) & 31) as usize])).collect())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ItemId {
    fn default() -> Self {
        Self::new()
    }
}

/// Why a text is no item id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not an item id: {0:?}")]
pub struct InvalidItemId(pub String);

impl FromStr for ItemId {
    type Err = InvalidItemId;
    /// A ULID, in either case.
    fn from_str(s: &str) -> Result<Self, InvalidItemId> {
        let id = s.to_ascii_uppercase();
        let valid = id.len() == 26 && id.bytes().all(|b| CROCKFORD.contains(&b)) && id.as_bytes()[0] <= b'7';
        if valid { Ok(Self(id)) } else { Err(InvalidItemId(s.to_owned())) }
    }
}

impl TryFrom<String> for ItemId {
    type Error = InvalidItemId;
    fn try_from(s: String) -> Result<Self, InvalidItemId> {
        s.parse()
    }
}

impl From<ItemId> for String {
    fn from(id: ItemId) -> Self {
        id.0
    }
}

impl fmt::Display for ItemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// When a change was made, by whom and on which machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stamp {
    /// The writer's clock as it wrote the change's event.
    pub at: DateTime<Utc>,
    /// Who the change was made for, as the writer tells, such as a thread.
    pub by: String,
    pub machine: Machine,
}

/// An item as its last change left it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Item {
    pub id: ItemId,
    /// What the item is, such as `memory` or `card`, which tells how to read its values.
    pub kind: String,
    /// 1 on creation, one more on each change.
    pub revision: u64,
    /// Its values, none null.
    pub values: Values,
    pub created: Stamp,
    pub updated: Stamp,
}

impl Item {
    /// The value `name` if it is a string, else nothing.
    pub fn text(&self, name: &str) -> &str {
        self.values.get(name).and_then(Value::as_str).unwrap_or_default()
    }
}

/// The values of `after` that differ from `before`, null for one removed: the change of an item holding `before`
/// into one holding `after`.
pub fn changed(before: &Values, after: Values) -> Values {
    let removed: Vec<_> =
        before.keys().filter(|name| !after.contains_key(*name)).map(|name| (name.clone(), Value::Null)).collect();
    let changed = after.into_iter().filter(|(name, value)| before.get(name).unwrap_or(&Value::Null) != value);
    changed.chain(removed).collect()
}

/// What an item of one kind holds, as a struct its values are read into and written from.
pub trait ItemValues: Serialize + DeserializeOwned {
    /// The kinds of the items holding it.
    const KINDS: &'static [&'static str];
    /// What it is called in the error naming an item unreadable as one: its first kind, unless it says otherwise.
    const LABEL: &'static str = Self::KINDS[0];

    /// What `item` holds; unavailable when its values are no such struct.
    fn of(item: &Item) -> Result<Self, StoreError> {
        serde_json::from_value(Value::Object(item.values.clone()))
            .map_err(|e| StoreError::Unavailable(format!("unreadable {} {}: {e}", Self::LABEL, item.id)))
    }

    /// What `item` holds; none for an item of another kind or unreadable as one.
    fn held(item: &Item) -> Option<Self> {
        Self::KINDS.contains(&item.kind.as_str()).then(|| Self::of(item).ok()).flatten()
    }

    /// The values of an item holding it.
    fn values(&self) -> Values {
        match serde_json::to_value(self) {
            Ok(Value::Object(values)) => values,
            _ => unreachable!("an item holds a struct"),
        }
    }
}

/// What `read` makes of each of `found`, beside its item. One it cannot read is left out with a warning naming it,
/// so a malformed item hides itself alone from a listing.
pub(crate) fn readable<T, E: std::fmt::Display>(
    found: Vec<Item>,
    read: impl Fn(&Item) -> Result<T, E>,
) -> Vec<(Item, T)> {
    let read = |item: Item| match read(&item) {
        Ok(read) => Some((item, read)),
        Err(error) => {
            tracing::warn!(item = %item.id, kind = %item.kind, "left an unreadable item out: {error}");
            None
        }
    };
    found.into_iter().filter_map(read).collect()
}

/// A flag kept only while set: null when unset.
pub(crate) mod flag {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(flag: &bool, serializer: S) -> Result<S::Ok, S::Error> {
        if *flag { serializer.serialize_bool(true) } else { serializer.serialize_none() }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
        Option::<bool>::deserialize(deserializer).map(Option::unwrap_or_default)
    }
}

/// `at` as values keep a time: RFC 3339 in UTC, its fraction of a second only when it has one.
pub fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// One change of one item, the data of an [`ITEM_EVENT`]: its creation, or new values for some of its values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub id: ItemId,
    pub kind: String,
    /// The revision the item has after the change.
    pub revision: u64,
    /// Who the change is made for ([`Stamp::by`]).
    pub by: String,
    /// The changed values as they were, null for one the item did not have; none on creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Values>,
    /// The changed values as they become, null for one removed.
    pub after: Values,
}

impl Change {
    /// The creation of a new item of `kind` with `values`, leaving out null ones.
    pub fn create(kind: &str, values: Values, by: &str) -> Self {
        let after = values.into_iter().filter(|(_, value)| !value.is_null()).collect();
        Self { id: ItemId::new(), kind: kind.to_owned(), revision: 1, by: by.to_owned(), before: None, after }
    }

    /// The change of `item` setting `values`, null removing one; none when it would change nothing.
    pub fn of(item: &Item, values: Values, by: &str) -> Option<Self> {
        let (mut before, mut after) = (Values::new(), Values::new());
        for (name, value) in values {
            let was = item.values.get(&name).cloned().unwrap_or_default();
            if was != value {
                before.insert(name.clone(), was);
                after.insert(name, value);
            }
        }
        (!after.is_empty()).then(|| Self {
            id: item.id.clone(),
            kind: item.kind.clone(),
            revision: item.revision + 1,
            by: by.to_owned(),
            before: Some(before),
            after,
        })
    }

    /// The item after this change, made as `stamp` says, of `current`, the item before it: a change that does not
    /// follow on it, such as one of another revision than the next, changes nothing and says why.
    pub fn apply(&self, current: Option<&Item>, stamp: Stamp) -> Result<Item, String> {
        let mut item = match (current, &self.before) {
            (None, None) if self.revision == 1 => Item {
                id: self.id.clone(),
                kind: self.kind.clone(),
                revision: 0,
                values: Values::new(),
                created: stamp.clone(),
                updated: stamp.clone(),
            },
            (Some(item), Some(_)) if item.revision + 1 == self.revision && item.kind == self.kind => item.clone(),
            (Some(item), _) => {
                return Err(format!("revision {} of item {} at revision {}", self.revision, self.id, item.revision));
            }
            (None, _) => return Err(format!("revision {} of item {}, unknown", self.revision, self.id)),
        };
        for (name, value) in &self.after {
            match value {
                Value::Null => item.values.remove(name),
                value => item.values.insert(name.clone(), value.clone()),
            };
        }
        item.revision = self.revision;
        item.updated = stamp;
        Ok(item)
    }
}

/// The changes of several items made at once, the data of an [`ITEMS_EVENT`]: each of another item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Batch {
    pub changes: Vec<Change>,
}

/// A change of an item, as its event in the log has it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Revision {
    /// The stream sequence of the event.
    pub sequence: u64,
    pub at: DateTime<Utc>,
    pub machine: Machine,
    pub change: Change,
}

impl Revision {
    pub fn stamp(&self) -> Stamp {
        Stamp { at: self.at, by: self.change.by.clone(), machine: self.machine.clone() }
    }
}

/// The state at time `at` of the item whose changes, in log order, are `history`: the state after the changes before
/// the first one written after `at`, so a writer's clock stepping back never mixes later changes in; none when it did
/// not exist yet.
pub fn state_at(history: &[Revision], at: DateTime<Utc>) -> Option<Item> {
    let cut = history.iter().position(|revision| revision.at > at).unwrap_or(history.len());
    history[..cut]
        .iter()
        .fold(None, |item, revision| revision.change.apply(item.as_ref(), revision.stamp()).ok().or(item))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn values(value: Value) -> Values {
        value.as_object().unwrap().clone()
    }

    fn stamp(at: &str) -> Stamp {
        Stamp { at: at.parse().unwrap(), by: "t".into(), machine: "m1".parse().unwrap() }
    }

    #[test]
    fn ids_are_ulids_read_in_either_case() {
        let id = ItemId::new();
        assert_eq!(id.as_str().to_ascii_lowercase().parse::<ItemId>(), Ok(id.clone()));
        assert_eq!(serde_json::from_value::<ItemId>(json!(id.as_str())).unwrap(), id);
        assert!("8ZZZZZZZZZZZZZZZZZZZZZZZZZ".parse::<ItemId>().is_err());
        assert!("01ARZ3NDEKTSV4RRFFQ69G5FAI".parse::<ItemId>().is_err(), "I is no Crockford digit");
        let named = ItemId::named("checkpoint", "t1");
        assert_eq!(named.as_str().parse::<ItemId>(), Ok(named.clone()));
        assert_eq!(named, ItemId::named("checkpoint", "t1"));
        assert_ne!(named, ItemId::named("checkpoint", "t2"));
        assert_ne!(named, ItemId::named("handoff", "t1"));
    }

    #[test]
    fn a_change_keeps_only_what_it_changes_as_it_was_and_became() {
        let create = Change::create("memory", values(json!({ "title": "a", "body": "b", "gone": null })), "t");
        let item = create.apply(None, stamp("2025-01-01T00:00:00Z")).unwrap();
        assert_eq!((item.revision, &item.values), (1, &values(json!({ "title": "a", "body": "b" }))));
        let change = Change::of(&item, values(json!({ "title": "a", "body": "c", "core": true })), "u").unwrap();
        assert_eq!(change.before, Some(values(json!({ "body": "b", "core": null }))));
        assert_eq!(change.after, values(json!({ "body": "c", "core": true })));
        let changed = change.apply(Some(&item), stamp("2025-01-02T00:00:00Z")).unwrap();
        assert_eq!((changed.revision, changed.text("body"), &changed.created), (2, "c", &item.created));
        assert_eq!(Change::of(&changed, values(json!({ "body": "c", "other": null })), "u"), None);
        let removed = Change::of(&changed, values(json!({ "core": null })), "u").unwrap();
        assert!(!removed.apply(Some(&changed), stamp("2025-01-03T00:00:00Z")).unwrap().values.contains_key("core"));
        assert!(change.apply(Some(&changed), stamp("2025-01-03T00:00:00Z")).is_err(), "revision 2 again");
        assert!(change.apply(None, stamp("2025-01-03T00:00:00Z")).is_err(), "of no item");
    }
}
