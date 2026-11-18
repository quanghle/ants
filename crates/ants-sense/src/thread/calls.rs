//! The calls of sense's tools the ant's threads made, counted by week from the changes the cache holds, nothing
//! written. Sense keeps no count of calls by tool, the vital signs of a dream counting them all
//! ([`crate::dream::Signs::sense_calls`]): a call that wrote made one event of the log, its changes made by the thread
//! ([`crate::items::Change::by`]), and the items it changed tell which tool it was. A call writing nothing leaves no
//! trace: `sense_brief`, a search of `sense_recall`, a read of the other tools, or a call changing nothing.
//! `sense_recall` reading a memory in full counts once a day a memory, as it notes the read ([`crate::memory::Access`]);
//! `sense_checkpoint` acknowledging handoffs counts once, for its register. Sense writes `sense_dream`'s verdicts
//! itself, so they count none here: the dreams keep them, with when each was given.

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike as _, Duration, NaiveDate, Utc};
use rusqlite::{Connection, named_params};

use super::{CHECKPOINT, HANDOFF};
use crate::cache::CacheError;
use crate::identity::BY;
use crate::items::Revision;
use crate::memory::{MEMORY, VOTE};
use crate::proposal::PROPOSAL;
use crate::records::{RELATIONSHIP, SELF_MODEL};
use crate::skill::SKILL;
use crate::store::{Items, StoreError};
use crate::{promise, remind, task};

/// The tools a call that wrote is told of, in the order of sense's tool list.
pub const TOOLS: [&str; 10] =
    ["remember", "recall", "vote", "checkpoint", "handoff", "task", "promise", "remind", "identity", "skill"];

/// The calls of one tool that wrote in one week.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Calls {
    /// The week's Monday (UTC).
    pub week: NaiveDate,
    /// The tool, without its `sense_` prefix: one of [`TOOLS`].
    pub tool: &'static str,
    pub calls: u32,
}

/// The changes the ant's threads made, not sense's own writers, the latest first: each one's event, time and item
/// kind, whether it made the item, whether it only noted a read, and what a proposal is of.
const CHANGES: &str = "
SELECT sequence, json_extract(entry, '$.at'), json_extract(entry, '$.change.kind'),
    json_type(entry, '$.change.before') IS NULL,
    NOT EXISTS (SELECT 1 FROM json_each(entry, '$.change.after') WHERE key <> 'access'),
    CASE WHEN json_type(entry, '$.change.after.kind') = 'text' THEN json_extract(entry, '$.change.after.kind') END
FROM changes
WHERE json_extract(entry, '$.change.by') <> :sense AND json_extract(entry, '$.change.by') NOT LIKE :sense || ':%'
ORDER BY sequence DESC";

/// One change of an item by a thread.
struct Changed {
    kind: String,
    created: bool,
    /// Whether it changed only the item's [`crate::memory::Access`].
    read: bool,
    /// The kind of the target of a proposal.
    of: Option<String>,
}

impl Changed {
    fn of(revision: &Revision) -> Self {
        let change = &revision.change;
        Self {
            kind: change.kind.clone(),
            created: change.before.is_none(),
            read: change.after.keys().all(|name| name == "access"),
            of: change.after.get("kind").and_then(|kind| kind.as_str()).map(ToOwned::to_owned),
        }
    }
}

/// The calls the ant's threads made in `items` since `since`, by week and tool, the earliest week first.
pub async fn calls(items: &dyn Items, since: DateTime<Utc>) -> Result<Vec<Calls>, StoreError> {
    items.calls(since).await
}

/// The Monday (UTC) of the week of `at`.
pub fn week(at: DateTime<Utc>) -> NaiveDate {
    let day = at.date_naive();
    day - Duration::days(day.weekday().num_days_from_monday().into())
}

/// The calls counted from the changes of `db` and the writes `pending` there, each a call's changes, since `since`
/// ([`calls`]).
pub(crate) fn count(
    db: &Connection,
    pending: &[Vec<Revision>],
    since: DateTime<Utc>,
) -> Result<Vec<Calls>, CacheError> {
    let mut events: Vec<(DateTime<Utc>, Vec<Changed>)> = Vec::new();
    let mut query = db.prepare(CHANGES)?;
    let mut rows = query.query(named_params! {":sense": BY})?;
    let mut last = None;
    while let Some(row) = rows.next()? {
        let Ok(at) = row.get::<_, String>(1)?.parse::<DateTime<Utc>>() else { continue };
        if at < since {
            break;
        }
        let changed = Changed { kind: row.get(2)?, created: row.get(3)?, read: row.get(4)?, of: row.get(5)? };
        let sequence: i64 = row.get(0)?;
        match events.last_mut() {
            Some((_, changes)) if last == Some(sequence) => changes.push(changed),
            _ => events.push((at, vec![changed])),
        }
        last = Some(sequence);
    }
    for revisions in pending {
        if let Some(first) = revisions.first().filter(|first| first.at >= since) {
            events.push((first.at, revisions.iter().map(Changed::of).collect()));
        }
    }
    let mut counted: BTreeMap<(NaiveDate, usize), u32> = BTreeMap::new();
    for (at, changes) in &events {
        if let Some(tool) = tool(changes).and_then(|tool| TOOLS.iter().position(|name| *name == tool)) {
            *counted.entry((week(*at), tool)).or_default() += 1;
        }
    }
    Ok(counted.into_iter().map(|((week, tool), calls)| Calls { week, tool: TOOLS[tool], calls }).collect())
}

/// The tool whose call made the `changes` of one event; none for an event of no tool's, such as a handoff
/// acknowledged apart from its checkpoint's register.
fn tool(changes: &[Changed]) -> Option<&'static str> {
    let has = |kind: &str| changes.iter().any(|changed| changed.kind == kind);
    Some(if changes.iter().any(|changed| changed.kind == HANDOFF && changed.created) {
        "handoff"
    } else if has(CHECKPOINT) {
        "checkpoint"
    } else if has(HANDOFF) {
        return None;
    } else if has(VOTE) {
        "vote"
    } else if has(promise::KIND) {
        "promise"
    } else if has(task::TASK) {
        "task"
    } else if has(remind::KIND) {
        "remind"
    } else if has(SELF_MODEL) || has(RELATIONSHIP) {
        "identity"
    } else if let Some(proposal) = changes.iter().find(|changed| changed.kind == PROPOSAL && changed.created) {
        if proposal.of.as_deref() == Some(SKILL) { "skill" } else { "identity" }
    } else if has(MEMORY) {
        if changes.iter().all(|changed| changed.read) { "recall" } else { "remember" }
    } else {
        return None;
    })
}
