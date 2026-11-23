//! The cache's offline outbox: local writes the log has not yet acknowledged ([`Pending`]), kept in the `outbox`
//! table and applied over the confirmed items, which `confirmed_items` keeps a copy of while any are pending. A
//! rebuild restores that copy and overlays the outbox again; the first entry a remote revision invalidates stops the
//! overlay and is recorded as the pending conflict instead of being dropped.

use serde::{Deserialize, Serialize};

use super::*;

pub(crate) const OPERATION: &str = "_sense_operation";
pub(super) const AT: &str = "_sense_at";

/// A decided atomic write, not a log event: only the hub assigns its sequence and chain position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Pending {
    pub operation: String,
    pub kind: String,
    pub data: Value,
    pub at: DateTime<Utc>,
    pub machine: Machine,
    #[serde(default)]
    pub guards: Vec<(ItemId, u64)>,
}

impl Pending {
    pub fn new(kind: &str, mut data: Value, machine: Machine) -> Self {
        let operation = uuid::Uuid::now_v7().to_string();
        let at = Utc::now();
        data[OPERATION] = Value::String(operation.clone());
        data[AT] = Value::String(at.to_rfc3339_opts(SecondsFormat::Nanos, true));
        Self { operation, kind: kind.to_owned(), data, at, machine, guards: Vec::new() }
    }

    pub fn revisions(&self) -> Result<Vec<Revision>, CacheError> {
        if self.data.get(OPERATION).and_then(Value::as_str) != Some(self.operation.as_str()) {
            return Err(CacheError::Unusable("pending write's operation identity differs".into()));
        }
        let changes = match self.kind.as_str() {
            ITEM_EVENT => vec![serde_json::from_value(self.data.clone())?],
            ITEMS_EVENT => serde_json::from_value::<Batch>(self.data.clone())?.changes,
            _ => return Err(CacheError::Unusable("pending write is not an item change".into())),
        };
        Ok(changes
            .into_iter()
            .map(|change| Revision { sequence: 0, at: self.at, machine: self.machine.clone(), change })
            .collect())
    }
}

impl Cache {
    pub(crate) fn pending(&self) -> Result<Vec<Pending>, CacheError> {
        pending(&self.db())
    }

    pub(crate) fn pending_count(&self) -> Result<usize, CacheError> {
        count(&self.db())
    }

    pub(crate) fn pending_conflict(&self) -> Result<Option<String>, CacheError> {
        meta(&self.db(), "pending_conflict")
    }

    pub(crate) fn pending_ready(&self) -> Result<usize, CacheError> {
        let db = self.db();
        Ok(meta(&db, "pending_applied")?.and_then(|value| value.parse().ok()).unwrap_or(count(&db)?))
    }

    pub(crate) async fn enqueue(&self, entry: &Pending) -> Result<(), CacheError> {
        let entry = entry.clone();
        self.write(move |db, pending_file| {
            // The marker prevents a corrupt cache from being discarded with acknowledged local writes inside.
            if let Some(path) = pending_file {
                let file = std::fs::File::create(path)?;
                file.sync_all()?;
                std::fs::File::open(path.parent().expect("cache directory"))?.sync_all()?;
            }
            // Accepted, the write is the outbox's alone until the log holds it: its commit syncs.
            db.pragma_update(None, "synchronous", "FULL")?;
            let queued = queue(db, &entry);
            db.pragma_update(None, "synchronous", "NORMAL")?;
            queued
        })
        .await
    }
}

fn queue(db: &mut Connection, entry: &Pending) -> Result<(), CacheError> {
    let tx = db.transaction()?;
    if count(&tx)? == 0 {
        snapshot(&tx)?;
    }
    tx.execute(
        "INSERT INTO outbox (operation, entry) VALUES (?1, ?2)",
        params![entry.operation, serde_json::to_string(entry)?],
    )?;
    apply_pending(&tx, entry)?;
    set_meta(&tx, "pending_applied", Some(&count(&tx)?.to_string()))?;
    tx.commit()?;
    Ok(())
}

/// Remove the pending marker at `path`, if any, once the outbox in `db` is empty.
pub(super) fn clear_pending_marker(db: &Connection, path: Option<&Path>) -> Result<(), CacheError> {
    if let Some(path) = path
        && count(db)? == 0
    {
        match std::fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
    }
    Ok(())
}

fn count(db: &Connection) -> Result<usize, CacheError> {
    let count: i64 = db.query_row("SELECT count(*) FROM outbox", [], |row| row.get(0))?;
    usize::try_from(count).map_err(|_| CacheError::Unusable("pending write count is too large".into()))
}

pub(super) fn pending(db: &Connection) -> Result<Vec<Pending>, CacheError> {
    let mut query = db.prepare("SELECT entry FROM outbox ORDER BY id")?;
    let rows = query.query_map([], |row| row.get::<_, String>(0))?;
    rows.map(|entry| Ok(serde_json::from_str(&entry?)?)).collect()
}

fn snapshot(tx: &Transaction<'_>) -> Result<(), CacheError> {
    tx.execute_batch("DELETE FROM confirmed_items; INSERT INTO confirmed_items SELECT id, item FROM items;")?;
    Ok(())
}

pub(super) fn restore(tx: &Transaction<'_>) -> Result<(), CacheError> {
    let items = {
        let mut query = tx.prepare("SELECT item FROM confirmed_items")?;
        let rows = query.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|item| Ok(serde_json::from_str::<Item>(&item?)?)).collect::<Result<Vec<_>, CacheError>>()?
    };
    tx.execute_batch("DELETE FROM items; DELETE FROM items_text;")?;
    for item in items {
        put_item(tx, &item)?;
    }
    Ok(())
}

pub(super) fn overlay(tx: &Transaction<'_>) -> Result<(), CacheError> {
    let entries = pending(tx)?;
    if entries.is_empty() {
        tx.execute("DELETE FROM confirmed_items", [])?;
    } else {
        snapshot(tx)?;
    }
    set_meta(tx, "pending_conflict", None)?;
    let mut applied = 0;
    for entry in entries {
        match apply_pending(tx, &entry) {
            Err(CacheError::Unusable(reason)) => {
                set_meta(tx, "pending_conflict", Some(&reason))?;
                break;
            }
            result => result?,
        }
        applied += 1;
    }
    set_meta(tx, "pending_applied", Some(&applied.to_string()))?;
    Ok(())
}

fn apply_pending(tx: &Transaction<'_>, entry: &Pending) -> Result<(), CacheError> {
    for (id, revision) in &entry.guards {
        if get(tx, id)?.is_none_or(|current| current.revision != *revision) {
            return Err(CacheError::Unusable(format!(
                "pending sense write {} conflicts with remote guard item {}; local write retained",
                entry.operation, id
            )));
        }
    }
    let revisions = entry.revisions()?;
    let mut items = Vec::new();
    for revision in &revisions {
        let current = get(tx, &revision.change.id)?;
        let item = revision.change.apply(current.as_ref(), revision.stamp()).map_err(|_| {
            CacheError::Unusable(format!(
                "pending sense write {} conflicts with remote item {}; local write retained",
                entry.operation, revision.change.id
            ))
        })?;
        items.push(item);
    }
    for item in items {
        put_item(tx, &item)?;
    }
    Ok(())
}
