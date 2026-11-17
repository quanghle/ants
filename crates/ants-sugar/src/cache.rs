//! An ant's items as its log left them, kept in SQLite in the ant's home ([`CACHE_FILE`] of [`SUGAR_DIR`]), with
//! FTS5 over their titles and bodies ([`Cache::search`]), so reading them reads no log. The cache is never the source
//! of truth: it holds what was built from the log of one sugar identity in one incarnation of its store, up to a
//! position ([`Built`]), and is emptied and built again from the log ([`crate::store::Store::open`]) whenever it is
//! missing, unreadable, of another identity or incarnation, or does not match the log.

use std::io;
use std::path::{Path, PathBuf};

use ants_proto::SugarId;
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::items::{BODY, Change, ITEM_EVENT, Item, ItemId, Revision, TITLE, state_at};
use crate::log::{Checked, Position, SUGAR_DIR};

/// The file of [`SUGAR_DIR`] holding the cache.
pub const CACHE_FILE: &str = "cache.db";

/// The schema's version, in `PRAGMA user_version`: a cache of another is built again.
const SCHEMA_VERSION: i64 = 1;
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
    sequence INTEGER PRIMARY KEY,
    item TEXT NOT NULL,
    entry TEXT NOT NULL
);
CREATE INDEX changes_by_item ON changes (item, sequence);
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

/// What a cache was built from: the log of `sugar` in the store's `incarnation`, read up to `position` (none before
/// any message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Built {
    pub sugar: SugarId,
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
}

/// Where the cache of the ant whose home is `home` is kept.
pub fn cache_file(home: &Path) -> PathBuf {
    home.join(SUGAR_DIR).join(CACHE_FILE)
}

impl Cache {
    /// The cache kept in `home`, created empty when missing; one that cannot be read, is corrupt or of another schema
    /// is replaced with an empty one.
    pub fn open(home: &Path) -> Result<Self, CacheError> {
        let file = cache_file(home);
        std::fs::create_dir_all(file.parent().expect("a file in a directory"))?;
        match Connection::open(&file).map_err(CacheError::from).and_then(Self::ready) {
            Ok(cache) => Ok(cache),
            Err(_) => {
                for suffix in ["", "-journal", "-wal", "-shm"] {
                    match std::fs::remove_file(format!("{}{suffix}", file.display())) {
                        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e.into()),
                        _ => {}
                    }
                }
                Self::ready(Connection::open(&file)?)
            }
        }
    }

    /// An empty cache in memory, kept nowhere.
    pub fn in_memory() -> Result<Self, CacheError> {
        Self::ready(Connection::open_in_memory()?)
    }

    fn ready(db: Connection) -> Result<Self, CacheError> {
        db.pragma_update(None, "cache_size", -PAGE_CACHE_KIB)?;
        db.pragma_update(None, "synchronous", "NORMAL")?;
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
        Ok(Self { db })
    }

    /// What the cache was built from; none when it was never built.
    pub fn built(&self) -> Result<Option<Built>, CacheError> {
        let (Some(sugar), Some(incarnation)) = (meta(&self.db, "sugar")?, meta(&self.db, "incarnation")?) else {
            return Ok(None);
        };
        let sugar = sugar.parse().map_err(|_| CacheError::Unusable(format!("sugar identity {sugar:?}")))?;
        let position = meta(&self.db, "position")?.map(|position| serde_json::from_str(&position)).transpose()?;
        Ok(Some(Built { sugar, incarnation, position }))
    }

    /// Empty the cache, to build it again from the start of the log of `sugar` in the store's `incarnation`.
    pub fn reset(&mut self, sugar: &SugarId, incarnation: &str) -> Result<(), CacheError> {
        let tx = self.db.transaction()?;
        tx.execute_batch("DELETE FROM meta; DELETE FROM items; DELETE FROM changes; DELETE FROM items_text;")?;
        set_meta(&tx, "sugar", Some(sugar.as_str()))?;
        set_meta(&tx, "incarnation", Some(incarnation))?;
        tx.commit()?;
        Ok(())
    }

    /// Apply `events`, read from the log in order, and record that the log was read up to `position`, at once. An
    /// event that is no item's change, or does not follow on the item's last ([`Change::apply`]), changes nothing.
    pub fn apply(&mut self, events: &[Checked], position: Option<&Position>) -> Result<(), CacheError> {
        let tx = self.db.transaction()?;
        for checked in events {
            if checked.event.kind == ITEM_EVENT
                && let Ok(change) = serde_json::from_value::<Change>(checked.event.data.clone())
            {
                let revision = Revision {
                    sequence: checked.stored.sequence,
                    at: checked.event.at,
                    machine: checked.event.machine.clone(),
                    change,
                };
                apply(&tx, revision)?;
            }
        }
        set_meta(&tx, "position", position.map(serde_json::to_string).transpose()?.as_deref())?;
        tx.commit()?;
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

    /// The changes of item `id` kept, in log order.
    pub fn history(&self, id: &ItemId) -> Result<Vec<Revision>, CacheError> {
        let mut query = self.db.prepare("SELECT entry FROM changes WHERE item = ?1 ORDER BY sequence")?;
        let rows = query.query_map([id.as_str()], |row| row.get::<_, String>(0))?;
        rows.map(|revision| Ok(serde_json::from_str(&revision?)?)).collect()
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

/// Apply `revision`'s change to its item, keeping it in the item's history; nothing when it does not follow on the
/// item's last change.
fn apply(tx: &Transaction<'_>, revision: Revision) -> Result<(), CacheError> {
    let current = get(tx, &revision.change.id)?;
    let Ok(item) = revision.change.apply(current.as_ref(), revision.stamp()) else { return Ok(()) };
    let updated_at = item.updated.at.to_rfc3339_opts(SecondsFormat::Micros, true);
    let rowid: i64 = tx.query_row(
        "INSERT INTO items (id, kind, revision, updated_at, item) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (id) DO UPDATE SET revision = ?3, updated_at = ?4, item = ?5
         RETURNING rowid",
        params![item.id.as_str(), item.kind, sequence_of(item.revision)?, updated_at, serde_json::to_string(&item)?],
        |row| row.get(0),
    )?;
    tx.execute("DELETE FROM items_text WHERE rowid = ?1", [rowid])?;
    tx.execute(
        "INSERT INTO items_text (rowid, title, body) VALUES (?1, ?2, ?3)",
        params![rowid, item.text(TITLE), item.text(BODY)],
    )?;
    tx.execute(
        "INSERT INTO changes (sequence, item, entry) VALUES (?1, ?2, ?3)",
        params![sequence_of(revision.sequence)?, item.id.as_str(), serde_json::to_string(&revision)?],
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
