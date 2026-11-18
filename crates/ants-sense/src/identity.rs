//! An ant's sense identity: its id ([`SenseId`]), minted when the ant first uses sense and kept in its home
//! ([`Home::id`]) and in its card ([`Card`]), the item sense alone writes, from the ant's lifecycle events
//! ([`record_event`]) and the start of each of its sessions ([`record_session`]). The id never changes: not on a
//! rename, a change of harness or model, a move, a retirement or a revival; a clone gets its own. A renamed ant's home
//! lists its earlier names ([`Home::names`]), whose events in its log are its own ([`crate::log::Chain`]): an import
//! under another name lists the name before ([`note_renamed`]), and so does an ant added under another name with the
//! home of an earlier one ([`take_over`]). A card made anew, as on another bus, starts with the names listed.
//!
//! A retirement makes the identity dormant ([`Card::active`] false), keeping all its data: its commitments stay open
//! and its reminders paused, the daemon firing none, until a revival or an addition under another ants identity
//! makes it active again, its reminders' expiry put off by the time it was dormant. The card keeps its last
//! dormancies ([`Card::dormancies`]), so maintenance counts only its active time in the ages of its memories.
//!
//! `sense_identity` ([`identity`]) shows the card whole with the ant's persona, self-model, relationship records and
//! skills, and is the one tool changing the persona, the self-model and the relationship records.

use std::io;
use std::path::Path;

use ants_proto::provider::{CallContext, Event, EventKind};
use ants_proto::{SenseId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::cache::CacheError;
use crate::home::Home;
use crate::items::{Item, Values};
use crate::log::read_status;
use crate::remind;
use crate::store::{Items, StoreError, Write, read_modify_write};

mod tool;

pub use tool::{identity, schema};

/// The kind of the card's item; an ant has one.
pub const CARD: &str = "card";
/// Whom sense's own changes are made for ([`crate::items::Stamp::by`]): the card's only writer.
pub const BY: &str = "sense";
/// How many ids of the lifecycle events last applied the card keeps, so a repeated event changes nothing.
const EVENTS_KEPT: usize = 16;
/// How many of its last dormancies the card keeps.
const DORMANCIES_KEPT: usize = 16;

/// The sense id kept in `home`; none when there is none.
pub fn read_id(home: &Path) -> io::Result<Option<SenseId>> {
    let text = match std::fs::read_to_string(Home::new(home).id()) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    text.trim().parse().map(Some).map_err(io::Error::other)
}

/// The sense id kept in `home`, minted and kept there, written whole, when there is none.
pub fn sense_id(home: &Path) -> io::Result<SenseId> {
    if let Some(id) = read_id(home)? {
        return Ok(id);
    }
    let id = SenseId::new();
    keep_id(home, &id)?;
    Ok(id)
}

/// Keep `id` as the sense id in `home`, written whole.
pub fn keep_id(home: &Path, id: &SenseId) -> io::Result<()> {
    crate::home::write(&Home::new(home).id(), format!("{id}\n").as_bytes())
}

/// The earlier names listed in `home`, the oldest first; none when it lists none.
pub fn earlier_names(home: &Path) -> io::Result<Vec<UserId>> {
    let text = match std::fs::read_to_string(Home::new(home).names()) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.trim().parse().map_err(io::Error::other))
        .collect()
}

/// List `from` among the earlier names in `home`, the home of an ant renamed from it, unless listed already: the file
/// written whole.
pub fn note_renamed(home: &Path, from: &UserId) -> io::Result<()> {
    let mut names = earlier_names(home)?;
    if names.contains(from) {
        return Ok(());
    }
    names.push(from.clone());
    crate::home::write(
        &Home::new(home).names(),
        names.iter().map(|name| format!("{name}\n")).collect::<String>().as_bytes(),
    )
}

/// Let `ant` take over `home`, as when an ant is added under a new name with the home of an earlier one: the name the
/// home's sense log was last read under, as its chain status tells ([`crate::log::ChainStatus::ant`]), listed among the
/// earlier names of `ant` ([`note_renamed`]) and returned when it differs; none when the home holds no sense data.
pub fn take_over(home: &Path, ant: &UserId) -> io::Result<Option<UserId>> {
    let Some(sense) = read_id(home)? else { return Ok(None) };
    let named = read_status(home)?.filter(|status| status.sense == sense).and_then(|status| status.ant);
    match named {
        Some(named) if named != *ant => {
            note_renamed(home, &named)?;
            Ok(Some(named))
        }
        _ => Ok(None),
    }
}

/// The ant's identity card, as sense keeps it: never written by the ant. Its revisions and their times are the item's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Card {
    pub sense: SenseId,
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
    /// When the sense identity was made: the ant's addition or cloning, else its first use of sense.
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
    /// False while dormant: from its retirement to its revival or addition again.
    pub active: bool,
    /// When its dormancy started: kept past its return until the brief told it so ([`returned_told`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<DateTime<Utc>>,
    /// When it came back from dormancy, or was added again with another ants identity, as after a revoke and re-add:
    /// until the brief told it so ([`returned_told`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returned_at: Option<DateTime<Utc>>,
    /// When it last moved to another machine or bus.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moved_at: Option<DateTime<Utc>>,
    /// The ids of the lifecycle events last applied, the latest last.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<Uuid>,
    /// The dormancies it came back from, the latest last, up to [`DORMANCIES_KEPT`]: unlike [`Card::retired_at`],
    /// kept after the brief told it of its return.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dormancies: Vec<Dormancy>,
    /// The growth its ants identity record held as last told ([`record_growth`]), which a record made again for the
    /// ant, as after a revoke and re-add, starts from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub growth: Option<Growth>,
}

/// The birth of an ants identity record and the prompts the ant completed since ([`ants_proto::AntIdentity::born`],
/// [`ants_proto::AntIdentity::prompts`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Growth {
    pub born: DateTime<Utc>,
    pub prompts: u64,
}

/// A time an identity was dormant: from its retirement to the event ending its dormancy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dormancy {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

/// The ant a clone was made of, and its ants identity id then.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClonedFrom {
    pub ant: UserId,
    pub identity: String,
}

impl Card {
    /// The card of `sense`, the identity of `ant`, once named `earlier`, made at `at`, linked to no ants identity yet.
    fn new(sense: SenseId, ant: UserId, earlier: Vec<UserId>, at: DateTime<Utc>) -> Self {
        let mut earlier_names = Vec::new();
        for name in earlier {
            if name != ant && !earlier_names.contains(&name) {
                earlier_names.push(name);
            }
        }
        Self {
            sense,
            name: ant,
            earlier_names,
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
            dormancies: Vec::new(),
            growth: None,
        }
    }

    /// Its dormancies until `now`: those it came back from and, while dormant, the current one.
    pub fn dormant(&self, now: DateTime<Utc>) -> impl Iterator<Item = Dormancy> {
        let current = self.retired_at.filter(|_| !self.active).map(|from| Dormancy { from, to: now });
        self.dormancies.iter().copied().chain(current)
    }

    /// Whether the ant came back from dormancy or under another ants identity and the brief has not told it yet.
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

    /// Note that the ant is named `ant` now, its name before an earlier one when it differs.
    fn named(&mut self, ant: &UserId) {
        if self.name == *ant {
            return;
        }
        let before = std::mem::replace(&mut self.name, ant.clone());
        if !self.earlier_names.contains(&before) {
            self.earlier_names.push(before);
        }
        self.earlier_names.retain(|name| name != ant);
    }

    /// Apply the lifecycle event `event`, unless applied already.
    fn apply(&mut self, event: &Event) {
        if self.events.contains(&event.id) {
            return;
        }
        let others = self.identities.iter().any(|linked| *linked != event.identity);
        match &event.kind {
            EventKind::AntAdded if others => self.wake(event.at),
            EventKind::AntAdded => {}
            EventKind::AntRenamed { from } => self.named(from),
            EventKind::AntMoved => self.moved_at = Some(event.at),
            EventKind::AntCloned { from, from_identity } => {
                self.cloned_from = Some(ClonedFrom { ant: from.clone(), identity: from_identity.clone() });
            }
            EventKind::AntRetired if self.active => {
                (self.active, self.retired_at, self.returned_at) = (false, Some(event.at), None);
            }
            EventKind::AntRetired => {}
            EventKind::AntRevived if !self.active => self.wake(event.at),
            EventKind::AntRevived => {}
            _ => return,
        }
        self.named(&event.ant);
        self.link(&event.identity);
        self.events.push(event.id);
        let over = self.events.len().saturating_sub(EVENTS_KEPT);
        self.events.drain(..over);
    }

    /// Make the identity active again at `at`, its dormancy, if any, ended then.
    fn wake(&mut self, at: DateTime<Utc>) {
        if let Some(from) = self.retired_at.filter(|_| !self.active) {
            self.dormancies.push(Dormancy { from, to: at });
            let over = self.dormancies.len().saturating_sub(DORMANCIES_KEPT);
            self.dormancies.drain(..over);
        }
        (self.active, self.returned_at) = (true, Some(at));
    }

    /// Note `growth`, keeping the earlier birth and the larger count of it and the one noted, so a growth told late
    /// never undoes a later one.
    fn grew(&mut self, growth: Growth) {
        let noted = self.growth.get_or_insert(growth);
        (noted.born, noted.prompts) = (noted.born.min(growth.born), noted.prompts.max(growth.prompts));
    }

    /// Note a session started as `context` says: its identity, harness, model and machine.
    fn session(&mut self, context: &CallContext) {
        self.named(&context.ant);
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

/// The card in `items`; none before the ant's first use of sense.
pub async fn card(items: &dyn Items) -> Result<Option<Card>, StoreError> {
    items.list(CARD, 1).await?.first().map(Card::of).transpose()
}

/// Apply the lifecycle event `event` to the card in `items`, made first when there is none: the card as it is then.
/// An event ending the ant's dormancy resumes its reminders in the same write ([`remind::resumed`]).
pub async fn record_event(items: &dyn Items, event: &Event) -> Result<Card, StoreError> {
    let apply = |card: &mut Card| card.apply(event);
    read_modify_write(|| async move {
        let Some(item) = items.list(CARD, 1).await?.pop() else {
            return update(items, &event.ant, event.at, apply).await;
        };
        let current = Card::of(&item)?;
        let mut card = current.clone();
        card.apply(event);
        let Some(since) = current.retired_at.filter(|_| !current.active && card.active) else {
            return update(items, &event.ant, event.at, apply).await;
        };
        let mut writes = remind::resumed(items, since, event.at).await?;
        writes.push(Write::Change {
            id: item.id.clone(),
            values: card.patch(&item.values)?,
            expected: Some(item.revision),
        });
        Card::of(&items.write(&writes, BY).await?.pop().expect("the card written last"))
    })
    .await
}

/// Note on the card in `items`, made first when there is none, the session starting as `context` says.
pub async fn record_session(items: &dyn Items, context: &CallContext) -> Result<Card, StoreError> {
    update(items, &context.ant, context.at, |card| card.session(context)).await
}

/// Note on the card in `items`, made first when there is none, the growth of the ant's identity record, as its daemon
/// tells: written only when it grew.
pub async fn record_growth(items: &dyn Items, ant: &UserId, growth: Growth) -> Result<Card, StoreError> {
    update(items, ant, Utc::now(), |card| card.grew(growth)).await
}

/// Note that the brief told the ant of its return ([`Card::returned`]), and so of its dormancy when it is active.
pub async fn returned_told(items: &dyn Items, ant: &UserId) -> Result<Card, StoreError> {
    update(items, ant, Utc::now(), |card| {
        card.returned_at = None;
        if card.active {
            card.retired_at = None;
        }
    })
    .await
}

/// The card in `items` after `change`, made for `ant` at `at` first when there is none, with the earlier names of the
/// ant's log. A change made by another writer in between is changed again.
async fn update(
    items: &dyn Items,
    ant: &UserId,
    at: DateTime<Utc>,
    change: impl Fn(&mut Card),
) -> Result<Card, StoreError> {
    let change = &change;
    read_modify_write(|| async move {
        let item = match items.list(CARD, 1).await?.pop() {
            Some(item) => item,
            None => {
                let mut card = Card::new(items.sense().await, ant.clone(), items.earlier_names().await, at);
                change(&mut card);
                let (item, created) = items.single(CARD, card.values()?, BY).await?;
                if created {
                    return Card::of(&item);
                }
                item
            }
        };
        let current = Card::of(&item)?;
        let mut card = current.clone();
        change(&mut card);
        if card == current {
            return Ok(card);
        }
        Card::of(&items.change(&item.id, card.patch(&item.values)?, Some(item.revision), BY).await?)
    })
    .await
}

#[cfg(test)]
mod tests;
