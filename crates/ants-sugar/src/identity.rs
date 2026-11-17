//! An ant's sugar identity: its id ([`SugarId`]), minted when the ant first uses sugar and kept in its home
//! ([`ID_FILE`]) and in its card ([`Card`]), the item sugar alone writes, from the ant's lifecycle events
//! ([`record_event`]) and the start of each of its sessions ([`record_session`]). The id never changes: not on a
//! rename, a change of harness or model, a move, a retirement or a revival; a clone gets its own.

use std::io;
use std::path::{Path, PathBuf};

use ants_proto::provider::{CallContext, Event, EventKind};
use ants_proto::{SugarId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::cache::CacheError;
use crate::items::{Item, Values};
use crate::log::{APPEND_TRIES, SUGAR_DIR};
use crate::store::{Items, StoreError};

/// The file of [`SUGAR_DIR`] holding the ant's sugar id.
pub const ID_FILE: &str = "id";
/// The kind of the card's item; an ant has one.
pub const CARD: &str = "card";
/// Whom sugar's own changes are made for ([`crate::items::Stamp::by`]): the card's only writer.
pub const BY: &str = "sugar";
/// How many ids of the lifecycle events last applied the card keeps, so a repeated event changes nothing.
const EVENTS_KEPT: usize = 16;

/// Where the sugar id of the ant whose home is `home` is kept.
pub fn id_file(home: &Path) -> PathBuf {
    home.join(SUGAR_DIR).join(ID_FILE)
}

/// The sugar id kept in `home`; none when there is none.
pub fn read_id(home: &Path) -> io::Result<Option<SugarId>> {
    let text = match std::fs::read_to_string(id_file(home)) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    text.trim().parse().map(Some).map_err(io::Error::other)
}

/// The sugar id kept in `home`, minted and kept there, written whole, when there is none.
pub fn sugar_id(home: &Path) -> io::Result<SugarId> {
    if let Some(id) = read_id(home)? {
        return Ok(id);
    }
    let id = SugarId::new();
    let file = id_file(home);
    let dir = file.parent().expect("a file in a directory");
    std::fs::create_dir_all(dir)?;
    let beside = dir.join(format!("{ID_FILE}.new"));
    std::fs::write(&beside, format!("{id}\n"))?;
    std::fs::rename(&beside, &file)?;
    Ok(id)
}

/// The ant's identity card, as sugar keeps it: never written by the ant. Its revisions and their times are the item's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Card {
    pub sugar: SugarId,
    /// The ant's current name.
    pub name: UserId,
    /// Its earlier names, the oldest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub earlier_names: Vec<UserId>,
    /// The id of its current ants identity record ([`ants_proto::AntIdentity::id`]).
    #[serde(default)]
    pub identity: String,
    /// Every ants identity id linked to it, in the order they were.
    #[serde(default)]
    pub identities: Vec<String>,
    /// When the sugar identity was made: the ant's addition or cloning, else its first use of sugar.
    pub born: DateTime<Utc>,
    /// The harnesses its sessions ran in, the first used first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub harnesses: Vec<String>,
    /// The models its sessions ran, the first used first; a harness's default is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
    /// The machines that served its sessions, the first first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub machines: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloned_from: Option<ClonedFrom>,
    /// False from its retirement to its revival or addition again.
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<DateTime<Utc>>,
    /// When it was added again with another ants identity, as after a revoke and re-add: until the brief told it so
    /// ([`returned_told`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returned_at: Option<DateTime<Utc>>,
    /// When it last moved to another machine or bus.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moved_at: Option<DateTime<Utc>>,
    /// The ids of the lifecycle events last applied, the latest last.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<Uuid>,
}

/// The ant a clone was made of, and its ants identity id then.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClonedFrom {
    pub ant: UserId,
    pub identity: String,
}

impl Card {
    /// The card of `sugar`, the identity of `ant`, made at `at`, linked to no ants identity yet.
    fn new(sugar: SugarId, ant: UserId, at: DateTime<Utc>) -> Self {
        Self {
            sugar,
            name: ant,
            earlier_names: Vec::new(),
            identity: String::new(),
            identities: Vec::new(),
            born: at,
            harnesses: Vec::new(),
            models: Vec::new(),
            machines: Vec::new(),
            cloned_from: None,
            active: true,
            retired_at: None,
            returned_at: None,
            moved_at: None,
            events: Vec::new(),
        }
    }

    /// Whether the ant came back under another ants identity and the brief has not told it yet.
    pub fn returned(&self) -> bool {
        self.returned_at.is_some()
    }

    fn of(item: &Item) -> Result<Self, StoreError> {
        Ok(serde_json::from_value(Value::Object(item.values.clone())).map_err(CacheError::from)?)
    }

    fn values(&self) -> Result<Values, StoreError> {
        match serde_json::to_value(self).map_err(CacheError::from)? {
            Value::Object(values) => Ok(values),
            _ => unreachable!("a card is an object"),
        }
    }

    /// The values setting `before`, the card's values, to this card's: null for each it no longer has.
    fn patch(&self, before: &Values) -> Result<Values, StoreError> {
        let mut values = self.values()?;
        for name in before.keys() {
            values.entry(name.clone()).or_insert(Value::Null);
        }
        Ok(values)
    }

    /// Make `identity` the current ants identity, linked to the card.
    fn link(&mut self, identity: &str) {
        if identity.is_empty() {
            return;
        }
        identity.clone_into(&mut self.identity);
        add(&mut self.identities, identity);
    }

    /// Apply the lifecycle event `event`, unless applied already.
    fn apply(&mut self, event: &Event) {
        if self.events.contains(&event.id) {
            return;
        }
        let others = self.identities.iter().any(|linked| *linked != event.identity);
        match &event.kind {
            EventKind::AntAdded if others => {
                (self.active, self.retired_at, self.returned_at) = (true, None, Some(event.at));
            }
            EventKind::AntAdded => {}
            EventKind::AntRenamed { from } => {
                for name in [&self.name, from] {
                    if *name != event.ant && !self.earlier_names.contains(name) {
                        self.earlier_names.push(name.clone());
                    }
                }
                self.name = event.ant.clone();
            }
            EventKind::AntMoved => self.moved_at = Some(event.at),
            EventKind::AntCloned { from, from_identity } => {
                self.cloned_from = Some(ClonedFrom { ant: from.clone(), identity: from_identity.clone() });
            }
            EventKind::AntRetired => (self.active, self.retired_at) = (false, Some(event.at)),
            EventKind::AntRevived => (self.active, self.retired_at) = (true, None),
            _ => return,
        }
        self.link(&event.identity);
        self.events.push(event.id);
        let over = self.events.len().saturating_sub(EVENTS_KEPT);
        self.events.drain(..over);
    }

    /// Note a session started as `context` says: its identity, harness, model and machine.
    fn session(&mut self, context: &CallContext) {
        self.link(&context.identity);
        add(&mut self.harnesses, &context.harness);
        if let Some(model) = &context.model {
            add(&mut self.models, model);
        }
        add(&mut self.machines, &context.machine);
    }
}

fn add(list: &mut Vec<String>, value: &str) {
    if !value.is_empty() && !list.iter().any(|known| known == value) {
        list.push(value.to_owned());
    }
}

/// Whether events of `kind` change the card: those of the ant's lifecycle, not of its threads and sessions.
pub fn touches_card(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::AntAdded
            | EventKind::AntRenamed { .. }
            | EventKind::AntMoved
            | EventKind::AntCloned { .. }
            | EventKind::AntRetired
            | EventKind::AntRevived
    )
}

/// The card in `items`; none before the ant's first use of sugar.
pub async fn card(items: &dyn Items) -> Result<Option<Card>, StoreError> {
    items.list(CARD, 1).await?.first().map(Card::of).transpose()
}

/// Apply the lifecycle event `event` to the card in `items`, made first when there is none: the card as it is then.
pub async fn record_event(items: &dyn Items, event: &Event) -> Result<Card, StoreError> {
    update(items, &event.ant, event.at, |card| card.apply(event)).await
}

/// Note on the card in `items`, made first when there is none, the session starting as `context` says.
pub async fn record_session(items: &dyn Items, context: &CallContext) -> Result<Card, StoreError> {
    update(items, &context.ant, context.at, |card| card.session(context)).await
}

/// Note that the brief told the ant of its return ([`Card::returned`]).
pub async fn returned_told(items: &dyn Items, ant: &UserId) -> Result<Card, StoreError> {
    update(items, ant, Utc::now(), |card| card.returned_at = None).await
}

/// The card in `items` after `change`, made for `ant` at `at` first when there is none. A change made by another
/// writer in between is changed again.
async fn update(
    items: &dyn Items,
    ant: &UserId,
    at: DateTime<Utc>,
    change: impl Fn(&mut Card),
) -> Result<Card, StoreError> {
    for _ in 0..APPEND_TRIES {
        let Some(item) = items.list(CARD, 1).await?.pop() else {
            let mut card = Card::new(items.sugar().await, ant.clone(), at);
            change(&mut card);
            let (item, created) = items.single(CARD, card.values()?, BY).await?;
            if created {
                return Card::of(&item);
            }
            continue;
        };
        let current = Card::of(&item)?;
        let mut card = current.clone();
        change(&mut card);
        if card == current {
            return Ok(card);
        }
        match items.change(&item.id, card.patch(&item.values)?, Some(item.revision), BY).await {
            Ok(item) => return Card::of(&item),
            Err(StoreError::Conflict { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Err(StoreError::Unavailable("other writers kept changing the card".into()))
}

#[cfg(test)]
mod tests;
