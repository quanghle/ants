//! An ant's items as its log left them, kept in SQLite in the ant's home ([`Home::cache`]), with FTS5 over their
//! titles and bodies ([`Cache::search`]), so reading them reads no log. The cache is never the source of truth: it
//! holds what was built from the log of one sense identity in one incarnation of its store, up to a position
//! ([`Built`]). An offline outbox also keeps accepted local writes durably until the log acknowledges them. Its pending
//! projection is separate from the confirmed projection; a conflicting remote revision never discards a local write. A
//! pending marker ([`Home::pending`]) prevents replacing an unreadable cache that may hold unacknowledged writes.

use std::io;
use std::path::{Path, PathBuf};

use ants_proto::{Machine, SenseId, UserId};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::Value;

use crate::dream::ReviewTally;
use crate::home::Home;
use crate::items::{BODY, Batch, Change, ITEM_EVENT, ITEMS_EVENT, Item, ItemId, Revision, TITLE, state_at};
use crate::log::{Checked, Position};
use crate::thread::calls::{self, Calls};
use crate::thread::experience::{self, Experience};

/// The schema's version, in `PRAGMA user_version`: a cache of another is built again.
const SCHEMA_VERSION: i64 = 3;
/// SQLite's page cache, in KiB: the cache is small and read little.
const PAGE_CACHE_KIB: i64 = 512;
/// The words of a search looked for, at most.
const MAX_TERMS: usize = 32;

const SCHEMA: &str = "
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE items (
    rowid INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    item TEXT NOT NULL
);
CREATE INDEX items_by_kind ON items (kind, updated_at);
CREATE TABLE changes (
    sequence INTEGER NOT NULL,
    item TEXT NOT NULL,
    entry TEXT NOT NULL,
    PRIMARY KEY (sequence, item)
) WITHOUT ROWID;
CREATE INDEX changes_by_item ON changes (item, sequence);
CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT NOT NULL UNIQUE, entry TEXT NOT NULL);
CREATE TABLE confirmed_items (id TEXT PRIMARY KEY, item TEXT NOT NULL) WITHOUT ROWID;
CREATE VIRTUAL TABLE items_text USING fts5 (
    title, body, content = '', contentless_delete = 1, tokenize = 'porter unicode61 remove_diacritics 2'
);
";

/// Why the cache did not answer.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Unusable(String),
}

/// What a cache was built from: the log of `sense` in the store's `incarnation`, read up to `position` (none before
/// any message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Built {
    pub sense: SenseId,
    pub incarnation: String,
    pub position: Option<Position>,
}

/// An item found by [`Cache::search`], and how well it matches: higher is better.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub item: Item,
    pub score: f64,
}

/// An ant's items, as built from its log.
pub struct Cache {
    db: Connection,
    pending_file: Option<PathBuf>,
}

mod outbox;
pub(crate) use outbox::Pending;

impl Cache {
    /// The cache kept in `home`, created empty when missing. Unreadable caches are replaced only when no pending
    /// marker protects local writes; otherwise opening fails and leaves the files untouched.
    pub fn open(home: &Path) -> Result<Self, CacheError> {
        let home = Home::new(home);
        let file = home.cache();
        if !file.exists() && home.has_pending_writes() {
            return Err(CacheError::Unusable("cache missing while pending sense writes may remain".into()));
        }
        std::fs::create_dir_all(home.dir())?;
        match Connection::open(&file).map_err(CacheError::from).and_then(Self::ready) {
            Ok(mut cache) => {
                cache.pending_file = Some(home.pending());
                Ok(cache)
            }
            Err(error) if home.has_pending_writes() => Err(CacheError::Unusable(format!(
                "{error}; pending sense writes may remain in {}; cache left untouched",
                file.display()
            ))),
            Err(_) => {
                for suffix in ["", "-journal", "-wal", "-shm"] {
                    match std::fs::remove_file(format!("{}{suffix}", file.display())) {
                        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
                        _ => {}
                    }
                }
                let mut cache = Self::ready(Connection::open(&file)?)?;
                cache.pending_file = Some(home.pending());
                Ok(cache)
            }
        }
    }

    /// An empty cache in memory, kept nowhere.
    pub fn in_memory() -> Result<Self, CacheError> {
        Self::ready(Connection::open_in_memory()?)
    }

    fn ready(db: Connection) -> Result<Self, CacheError> {
        db.pragma_update(None, "cache_size", -PAGE_CACHE_KIB)?;
        db.pragma_update(None, "synchronous", "FULL")?;
        let version: i64 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
        match version {
            0 => {
                db.execute_batch(&format!("BEGIN; {SCHEMA} PRAGMA user_version = {SCHEMA_VERSION}; COMMIT;"))?;
            }
            SCHEMA_VERSION => {
                let check: String = db.pragma_query_value(None, "quick_check", |row| row.get(0))?;
                if check != "ok" {
                    return Err(CacheError::Unusable(format!("corrupt: {check}")));
                }
            }
            other => return Err(CacheError::Unusable(format!("schema version {other}"))),
        }
        Ok(Self { db, pending_file: None })
    }

    /// What the cache was built from; none when it was never built.
    pub fn built(&self) -> Result<Option<Built>, CacheError> {
        let (Some(sense), Some(incarnation)) = (meta(&self.db, "sense")?, meta(&self.db, "incarnation")?) else {
            return Ok(None);
        };
        let sense = sense.parse().map_err(|_| CacheError::Unusable(format!("sense identity {sense:?}")))?;
        let position = meta(&self.db, "position")?.map(|position| serde_json::from_str(&position)).transpose()?;
        Ok(Some(Built { sense, incarnation, position }))
    }

    /// Opaque connection context retained with the cache, for opening it before the hub is reachable.
    pub fn context(&self) -> Result<Option<Value>, CacheError> {
        meta(&self.db, "connection")?.map(|value| Ok(serde_json::from_str(&value)?)).transpose()
    }

    pub fn set_context(&self, value: &Value) -> Result<(), CacheError> {
        self.db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('connection', ?1)",
            [serde_json::to_string(value)?],
        )?;
        Ok(())
    }

    /// Empty the cache, to build it again from the start of the log of `sense` in the store's `incarnation`.
    pub fn reset(&mut self, sense: &SenseId, incarnation: &str) -> Result<(), CacheError> {
        if !self.pending()?.is_empty() {
            return Err(CacheError::Unusable("pending sense writes prevent replacing the cache's log".into()));
        }
        let tx = self.db.transaction()?;
        tx.execute_batch(
            "DELETE FROM meta WHERE key != 'connection'; DELETE FROM items; DELETE FROM changes; DELETE FROM items_text;",
        )?;
        set_meta(&tx, "sense", Some(sense.as_str()))?;
        set_meta(&tx, "incarnation", Some(incarnation))?;
        tx.commit()?;
        Ok(())
    }

    /// Apply `events`, read from the log in order, and record that the log was read up to `position`, at once. An
    /// event that is no item's change, or does not follow on the item's last ([`Change::apply`]), changes nothing; nor
    /// does a batch ([`ITEMS_EVENT`]) of which one change does not, or which changes an item twice.
    pub fn apply(&mut self, events: &[Checked], position: Option<&Position>) -> Result<(), CacheError> {
        let tx = self.db.transaction()?;
        let pending = outbox::pending(&tx)?;
        if !pending.is_empty() {
            outbox::restore(&tx)?;
        }
        for checked in events {
            let changes = match checked.event.kind.as_str() {
                ITEM_EVENT => serde_json::from_value::<Change>(checked.event.data.clone()).map(|change| vec![change]),
                ITEMS_EVENT => serde_json::from_value::<Batch>(checked.event.data.clone()).map(|batch| batch.changes),
                _ => continue,
            };
            let Ok(changes) = changes else { continue };
            let at = checked
                .event
                .data
                .get(outbox::AT)
                .and_then(Value::as_str)
                .and_then(|at| at.parse::<DateTime<Utc>>().ok())
                .unwrap_or(checked.event.at);
            let revisions = changes.into_iter().map(|change| Revision {
                sequence: checked.stored.sequence,
                at,
                machine: checked.event.machine.clone(),
                change,
            });
            apply(&tx, revisions.collect())?;
            if let Some(operation) = checked.event.data.get(outbox::OPERATION).and_then(Value::as_str)
                && let Some(entry) = pending.iter().find(|entry| entry.operation == operation)
            {
                if entry.kind != checked.event.kind || entry.data != checked.event.data {
                    return Err(CacheError::Unusable(format!(
                        "remote event differs from pending sense write {operation}; local write retained"
                    )));
                }
                tx.execute("DELETE FROM outbox WHERE operation = ?1", [operation])?;
            }
        }
        if !pending.is_empty() {
            outbox::overlay(&tx)?;
        }
        set_meta(&tx, "position", position.map(serde_json::to_string).transpose()?.as_deref())?;
        tx.commit()?;
        self.clear_pending_marker()?;
        Ok(())
    }

    pub fn get(&self, id: &ItemId) -> Result<Option<Item>, CacheError> {
        get(&self.db, id)
    }

    /// The items of `kind`, the last changed first, up to `limit`.
    pub fn list(&self, kind: &str, limit: usize) -> Result<Vec<Item>, CacheError> {
        let mut query = self.db.prepare("SELECT item FROM items WHERE kind = ?1 ORDER BY updated_at DESC LIMIT ?2")?;
        let rows = query.query_map(params![kind, limit_of(limit)], |row| row.get::<_, String>(0))?;
        rows.map(|item| Ok(serde_json::from_str(&item?)?)).collect()
    }

    /// The items of `kind` whose value `name` is `value`, or which have no value `name` when `value` is none, the last
    /// changed first, up to `limit`.
    pub fn list_by(
        &self,
        kind: &str,
        name: &str,
        value: Option<&Value>,
        limit: usize,
    ) -> Result<Vec<Item>, CacheError> {
        let path = format!("$.values.\"{}\"", name.replace('"', ""));
        let mut query = self.db.prepare(
            "SELECT item FROM items WHERE kind = ?1 AND json_extract(item, ?2) IS json_extract(?3, '$')
             ORDER BY updated_at DESC LIMIT ?4",
        )?;
        let value = value.map(Value::to_string);
        let rows = query.query_map(params![kind, path, value, limit_of(limit)], |row| row.get::<_, String>(0))?;
        rows.map(|item| Ok(serde_json::from_str(&item?)?)).collect()
    }

    /// The experience of the ant whose names are `own`, since `since` if given ([`crate::thread::experience`]).
    pub fn experience(
        &self,
        own: &[UserId],
        since: Option<DateTime<Utc>>,
        top: usize,
    ) -> Result<Option<Experience>, CacheError> {
        experience::count(&self.db, own, since, top)
    }

    /// The tally of the reviews the items record, with the `top` kinds and ants ([`crate::dream::review_tally`]).
    pub fn review_tally(&self, top: usize) -> Result<Option<ReviewTally>, CacheError> {
        crate::dream::reviews::count(&self.db, top)
    }

    /// The calls of sense's tools the ant's threads made since `since`, pending writes too ([`crate::thread::calls`]).
    pub fn calls(&self, since: DateTime<Utc>) -> Result<Vec<Calls>, CacheError> {
        let pending = self.pending()?.into_iter().take(self.pending_ready()?);
        let pending = pending.map(|pending| pending.revisions()).collect::<Result<Vec<_>, _>>()?;
        calls::count(&self.db, &pending, since)
    }

    /// The items, of `kind` if given, whose title or body holds any word of `text`, the best matches first (BM25, a
    /// title's words weighing more), up to `limit`. `text` is taken as words only, never as an FTS5 query.
    pub fn search(&self, text: &str, kind: Option<&str>, limit: usize) -> Result<Vec<Hit>, CacheError> {
        let Some(query) = match_query(text) else { return Ok(Vec::new()) };
        let mut found = self.db.prepare(
            "SELECT items.item, bm25(items_text, 2.0, 1.0) AS rank
             FROM items_text JOIN items ON items.rowid = items_text.rowid
             WHERE items_text MATCH ?1 AND (?2 IS NULL OR items.kind = ?2)
             ORDER BY rank LIMIT ?3",
        )?;
        let rows = found.query_map(params![query, kind, limit_of(limit)], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
        })?;
        rows.map(|row| {
            let (item, rank) = row?;
            Ok(Hit { item: serde_json::from_str(&item)?, score: -rank })
        })
        .collect()
    }

    /// The changes of item `id`, confirmed first and pending after them with sequence 0.
    pub fn history(&self, id: &ItemId) -> Result<Vec<Revision>, CacheError> {
        let mut query = self.db.prepare("SELECT entry FROM changes WHERE item = ?1 ORDER BY sequence")?;
        let rows = query.query_map([id.as_str()], |row| row.get::<_, String>(0))?;
        let mut revisions: Vec<Revision> =
            rows.map(|revision| Ok(serde_json::from_str(&revision?)?)).collect::<Result<_, CacheError>>()?;
        let applied = meta(&self.db, "pending_applied")?.and_then(|value| value.parse::<usize>().ok());
        for pending in self.pending()?.into_iter().take(applied.unwrap_or(usize::MAX)) {
            revisions.extend(pending.revisions()?.into_iter().filter(|revision| &revision.change.id == id));
        }
        Ok(revisions)
    }

    /// Item `id` as it was at time `at` ([`state_at`]); none when it did not exist yet.
    pub fn at(&self, id: &ItemId, at: DateTime<Utc>) -> Result<Option<Item>, CacheError> {
        Ok(state_at(&self.history(id)?, at))
    }
}

fn limit_of(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

fn sequence_of(sequence: u64) -> Result<i64, CacheError> {
    i64::try_from(sequence).map_err(|_| CacheError::Unusable(format!("sequence {sequence}")))
}

fn meta(db: &Connection, key: &str) -> Result<Option<String>, CacheError> {
    Ok(db.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| row.get(0)).optional()?)
}

fn set_meta(tx: &Transaction<'_>, key: &str, value: Option<&str>) -> Result<(), CacheError> {
    match value {
        Some(value) => tx.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)", [key, value])?,
        None => tx.execute("DELETE FROM meta WHERE key = ?1", [key])?,
    };
    Ok(())
}

fn get(db: &Connection, id: &ItemId) -> Result<Option<Item>, CacheError> {
    let item: Option<String> =
        db.query_row("SELECT item FROM items WHERE id = ?1", [id.as_str()], |row| row.get(0)).optional()?;
    Ok(item.map(|item| serde_json::from_str(&item)).transpose()?)
}

/// Apply the changes of `revisions`, of one event, to their items, keeping them in the items' histories: none when one
/// does not follow on its item's last change or two change the same item.
fn apply(tx: &Transaction<'_>, revisions: Vec<Revision>) -> Result<(), CacheError> {
    let mut items = Vec::with_capacity(revisions.len());
    for revision in &revisions {
        if revisions.iter().filter(|other| other.change.id == revision.change.id).count() > 1 {
            return Ok(());
        }
        let current = get(tx, &revision.change.id)?;
        let Ok(item) = revision.change.apply(current.as_ref(), revision.stamp()) else { return Ok(()) };
        items.push(item);
    }
    for (item, revision) in items.into_iter().zip(revisions) {
        put(tx, &item, &revision)?;
    }
    Ok(())
}

fn put(tx: &Transaction<'_>, item: &Item, revision: &Revision) -> Result<(), CacheError> {
    put_item(tx, item)?;
    tx.execute(
        "INSERT INTO changes (sequence, item, entry) VALUES (?1, ?2, ?3)",
        params![sequence_of(revision.sequence)?, item.id.as_str(), serde_json::to_string(revision)?],
    )?;
    Ok(())
}

fn put_item(tx: &Transaction<'_>, item: &Item) -> Result<(), CacheError> {
    let updated_at = item.updated.at.to_rfc3339_opts(SecondsFormat::Micros, true);
    let rowid: i64 = tx.query_row(
        "INSERT INTO items (id, kind, revision, updated_at, item) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (id) DO UPDATE SET revision = ?3, updated_at = ?4, item = ?5
         RETURNING rowid",
        params![item.id.as_str(), item.kind, sequence_of(item.revision)?, updated_at, serde_json::to_string(item)?],
        |row| row.get(0),
    )?;
    tx.execute("DELETE FROM items_text WHERE rowid = ?1", [rowid])?;
    tx.execute(
        "INSERT INTO items_text (rowid, title, body) VALUES (?1, ?2, ?3)",
        params![rowid, item.text(TITLE), item.text(BODY)],
    )?;
    Ok(())
}

/// `text` as an FTS5 query for any of its words, each quoted, so nothing in it reads as FTS5 syntax; none without
/// words.
fn match_query(text: &str) -> Option<String> {
    let mut terms: Vec<&str> = Vec::new();
    for word in text.split(|c: char| !c.is_alphanumeric()).filter(|word| !word.is_empty()) {
        if terms.len() == MAX_TERMS {
            break;
        }
        if !terms.contains(&word) {
            terms.push(word);
        }
    }
    (!terms.is_empty()).then(|| terms.iter().map(|word| format!("\"{word}\"")).collect::<Vec<_>>().join(" OR "))
}

#[cfg(test)]
mod tests;
