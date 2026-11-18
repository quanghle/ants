//! The reviews a sage made: sense keeps a record of each in the sage's data ([`ReviewRecord`]), an item of kind
//! [`REVIEW`] named after the ant reviewed and its dream ([`ItemId::named`]), which sense alone writes ([`BY`]), as it
//! does the thread records ([`crate::thread::record`]). The review message makes it ([`review_received`]): the ant, its
//! dream and the thread. The sage's answer gives its findings by kind ([`review_answered`]), read as the ant's daemon
//! reads it ([`super::parse`]) and numbered as the ant numbers them, `<dream>.<n>`; the first answer stays. Each later
//! review message of the ant tells what is new in the verdicts and outcomes of the sage's earlier findings
//! ([`ants_proto::ReviewOutcomes`]), which go to the records of the reviews that made them. A record holds the kinds,
//! verdicts and outcomes alone: no text of the ant's threads, nor of the findings. Each message changes the records
//! once and a repeat nothing.
//!
//! The sage's tally is counted from the records by a query of the cache each time it is asked ([`review_tally`]): the
//! findings made, applied, rejected, deferred and unanswered, and of the applied how many still hold, in all, by kind
//! and by ant. The sage's identity shows it: the brief, `sense_identity` and the memories view. An ant that reviewed
//! nobody has none. A clone inherits no record ([`crate::fork::KEPT`]).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ants_proto::{FindingOutcome, FindingVerdict, ReviewOutcomes, ThreadId, UserId};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, ToSql, named_params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Judgement, parse};
use crate::cache::CacheError;
use crate::identity::BY;
use crate::items::{Item, ItemId, Values, values_of};
use crate::store::{Items, StoreError, Write, read_modify_write};

/// The kind of a review's record.
pub const REVIEW: &str = "review";

/// A review a sage made, as its sense keeps it: never written by the sage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewRecord {
    /// The ant reviewed.
    pub ant: UserId,
    /// The number of its dream reviewed.
    pub dream: u64,
    /// The thread the review was asked and answered on.
    pub thread: ThreadId,
    /// When the review message arrived.
    pub received: DateTime<Utc>,
    /// When the sage answered it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answered: Option<DateTime<Utc>>,
    /// The findings of the answer, in their order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<ReviewedFinding>,
}

/// A finding of a sage's answer to a review, and what the ant made of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewedFinding {
    /// `<dream>.<n>`, as the ant answers it.
    pub id: String,
    /// Its kind, in the sage's words.
    pub kind: String,
    /// The ant's last verdict on it, once told; none while unanswered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Judgement>,
    /// Applied, whether what the ant wrote for it still holds.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub holds: bool,
    /// When its verdict or outcome was last told.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub told: Option<DateTime<Utc>>,
}

impl ReviewRecord {
    /// The record `item` holds.
    pub fn of(item: &Item) -> Result<Self, StoreError> {
        Ok(serde_json::from_value(Value::Object(item.values.clone())).map_err(CacheError::from)?)
    }

    /// The values of its item.
    pub fn values(&self) -> Values {
        values_of(self)
    }

    fn new(ant: &UserId, dream: u64, thread: &ThreadId, at: DateTime<Utc>) -> Self {
        Self { ant: ant.clone(), dream, thread: thread.clone(), received: at, answered: None, findings: Vec::new() }
    }

    /// Apply `told` at `at`: the ant's verdict on one of its findings, and whether it still holds.
    fn told(&mut self, told: &FindingOutcome, at: DateTime<Utc>) {
        let Some(finding) = self.findings.iter_mut().find(|finding| finding.id == told.finding) else { return };
        let verdict = Some(match told.verdict {
            FindingVerdict::Applied => Judgement::Applied,
            FindingVerdict::Rejected => Judgement::Rejected,
            FindingVerdict::Deferred => Judgement::Deferred,
        });
        let holds = told.holds && told.verdict == FindingVerdict::Applied;
        if (finding.verdict, finding.holds) != (verdict, holds) {
            (finding.verdict, finding.holds, finding.told) = (verdict, holds, Some(at));
        }
    }
}

/// The id of the record of the review of dream `dream` of `ant`.
pub fn review_id(ant: &UserId, dream: u64) -> ItemId {
    ItemId::named(REVIEW, &format!("{ant}.{dream}"))
}

/// The record of the review of dream `dream` of `ant` in `items`; none before its message arrived.
pub async fn review_record(items: &dyn Items, ant: &UserId, dream: u64) -> Result<Option<ReviewRecord>, StoreError> {
    items.get(&review_id(ant, dream)).await?.as_ref().map(ReviewRecord::of).transpose()
}

/// The records in `items`, the last changed first, up to `limit`.
pub async fn review_records(items: &dyn Items, limit: usize) -> Result<Vec<ReviewRecord>, StoreError> {
    items.list(REVIEW, limit).await?.iter().map(ReviewRecord::of).collect()
}

/// Note in `items`, a sage's, the review message from `ant` on `thread` arriving at `at`, which `told` says it asks of
/// the ant's dream and what it tells of the sage's earlier findings: the review's record made, and the verdicts and
/// outcomes applied to the records of the earlier reviews. Whether it changed any record: a message told again
/// changes none.
pub async fn review_received(
    items: &dyn Items,
    ant: &UserId,
    thread: &ThreadId,
    told: &ReviewOutcomes,
    at: DateTime<Utc>,
) -> Result<bool, StoreError> {
    let mut earlier: BTreeMap<u64, Vec<&FindingOutcome>> = BTreeMap::new();
    for outcome in &told.findings {
        let number = outcome.finding.split_once('.').and_then(|(number, _)| number.parse::<u64>().ok());
        if let Some(number) = number.filter(|number| *number < told.dream) {
            earlier.entry(number).or_default().push(outcome);
        }
    }
    let earlier = &earlier;
    read_modify_write(|| async move {
        let mut writes = Vec::new();
        let id = review_id(ant, told.dream);
        if items.get(&id).await?.is_none() {
            let values = ReviewRecord::new(ant, told.dream, thread, at).values();
            writes.push(Write::Put { id, kind: REVIEW.into(), values, expected: Some(0) });
        }
        for (number, outcomes) in earlier {
            let id = review_id(ant, *number);
            let Some(item) = items.get(&id).await? else { continue };
            let mut record = ReviewRecord::of(&item)?;
            let before = record.clone();
            for outcome in outcomes {
                record.told(outcome, at);
            }
            if record != before {
                writes.push(Write::Put {
                    id,
                    kind: REVIEW.into(),
                    values: record.values(),
                    expected: Some(item.revision),
                });
            }
        }
        if writes.is_empty() {
            return Ok(false);
        }
        items.write(&writes, BY).await?;
        Ok(true)
    })
    .await
}

/// Note in `items`, a sage's, its answer `text` at `at` to the review of dream `dream` of `ant` on `thread`: its
/// findings, as the ant's daemon reads them ([`parse`]), by kind; none when it holds none readable. Whether it changed
/// the record: once answered, a review keeps its first answer.
pub async fn review_answered(
    items: &dyn Items,
    ant: &UserId,
    thread: &ThreadId,
    dream: u64,
    text: &str,
    at: DateTime<Utc>,
) -> Result<bool, StoreError> {
    let findings: Vec<_> = parse(text)
        .map_or_default(|parsed| parsed.findings)
        .into_iter()
        .zip(1..)
        .map(|((_, advice), n)| ReviewedFinding {
            id: format!("{dream}.{n}"),
            kind: advice.kind,
            verdict: None,
            holds: false,
            told: None,
        })
        .collect();
    let findings = &findings;
    let id = &review_id(ant, dream);
    read_modify_write(|| async move {
        let item = items.get(id).await?;
        let mut record = match &item {
            Some(item) => ReviewRecord::of(item)?,
            None => ReviewRecord::new(ant, dream, thread, at),
        };
        if record.answered.is_some() {
            return Ok(false);
        }
        record.answered = Some(at);
        record.findings.clone_from(findings);
        let expected = item.map_or(0, |item| item.revision);
        items.put(id, REVIEW, record.values(), Some(expected), BY).await?;
        Ok(true)
    })
    .await
}

/// The findings of a sage, or of one kind or for one ant, and what the ants made of them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct FindingTally {
    pub made: u32,
    pub applied: u32,
    /// The applied that still hold.
    pub holding: u32,
    pub rejected: u32,
    pub deferred: u32,
    pub unanswered: u32,
}

impl FindingTally {
    /// Count a finding, with the last `verdict` on it, applied with what still `holds`.
    pub(crate) fn add(&mut self, verdict: Option<Judgement>, holds: bool) {
        self.made += 1;
        match verdict {
            Some(Judgement::Applied) => {
                self.applied += 1;
                self.holding += u32::from(holds);
            }
            Some(Judgement::Rejected) => self.rejected += 1,
            Some(Judgement::Deferred) => self.deferred += 1,
            None => self.unanswered += 1,
        }
    }

    pub(crate) fn plus(self, other: &Self) -> Self {
        Self {
            made: self.made + other.made,
            applied: self.applied + other.applied,
            holding: self.holding + other.holding,
            rejected: self.rejected + other.rejected,
            deferred: self.deferred + other.deferred,
            unanswered: self.unanswered + other.unanswered,
        }
    }

    /// The counts, on one line.
    pub fn counts(&self) -> String {
        format!(
            "{} made, {} applied ({} still holding), {} rejected, {} deferred, {} unanswered",
            self.made, self.applied, self.holding, self.rejected, self.deferred, self.unanswered
        )
    }
}

/// A sage's tally of its reviews, from their records ([`review_tally`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewTally {
    pub reviews: u32,
    /// The ants reviewed.
    pub ants: u32,
    /// When the last review arrived, to the millisecond.
    pub last: DateTime<Utc>,
    /// The findings of every review.
    pub findings: FindingTally,
    /// The kinds of the most findings, the most first.
    pub by_kind: Vec<KindTally>,
    /// The ants of the most reviews, the most first, then the latest.
    pub by_ant: Vec<AntTally>,
}

/// The findings of one kind of a [`ReviewTally`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KindTally {
    pub kind: String,
    pub findings: FindingTally,
}

/// The reviews of one ant of a [`ReviewTally`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AntTally {
    pub ant: UserId,
    pub reviews: u32,
    /// When its last review arrived, to the millisecond.
    pub last: DateTime<Utc>,
    pub findings: FindingTally,
}

/// The tally of the reviews `items`, a sage's, records, with the `top` kinds and ants of the most findings and reviews;
/// none before any review.
pub async fn review_tally(items: &dyn Items, top: usize) -> Result<Option<ReviewTally>, StoreError> {
    items.review_tally(top).await
}

/// The review records of the cache's items, and their findings, each with its kind, its verdict, null while
/// unanswered, and whether it holds.
const RECORDS: &str = "
WITH reviews AS (
    SELECT
        json_extract(item, '$.values.ant') AS ant,
        julianday(json_extract(item, '$.values.received')) AS received,
        json_extract(item, '$.values.findings') AS findings
    FROM items WHERE kind = :kind
),
findings AS (
    SELECT
        reviews.ant AS ant,
        json_extract(finding.value, '$.kind') AS kind,
        json_extract(finding.value, '$.verdict') AS verdict,
        ifnull(json_extract(finding.value, '$.holds'), 0) AS holds
    FROM reviews, json_each(ifnull(reviews.findings, '[]')) AS finding
)";

/// The [`FindingTally`] of the rows of `findings` a query groups, as six columns.
const TALLY: &str = "
    count(findings.kind),
    ifnull(sum(findings.verdict = 'applied'), 0),
    ifnull(sum(findings.verdict = 'applied' AND findings.holds), 0),
    ifnull(sum(findings.verdict = 'rejected'), 0),
    ifnull(sum(findings.verdict = 'deferred'), 0),
    count(findings.kind) - count(findings.verdict)";

const TOTALS: &str = "
SELECT
    (SELECT count(*) FROM reviews),
    (SELECT count(DISTINCT ant) FROM reviews),
    (SELECT strftime('%Y-%m-%dT%H:%M:%fZ', max(received)) FROM reviews),
    {tally}
FROM findings";

const BY_KIND: &str = "
SELECT findings.kind, {tally} FROM findings
GROUP BY findings.kind ORDER BY count(*) DESC, findings.kind LIMIT :top";

const BY_ANT: &str = "
SELECT reviewed.ant, reviewed.reviews, strftime('%Y-%m-%dT%H:%M:%fZ', reviewed.last), {tally}
FROM (SELECT ant, count(*) AS reviews, max(received) AS last FROM reviews GROUP BY ant) AS reviewed
LEFT JOIN findings ON findings.ant = reviewed.ant
GROUP BY reviewed.ant ORDER BY reviewed.reviews DESC, reviewed.last DESC, reviewed.ant LIMIT :top";

/// The [`FindingTally`] in the six columns of `row` from `at`.
fn tally_at(row: &rusqlite::Row<'_>, at: usize) -> rusqlite::Result<FindingTally> {
    Ok(FindingTally {
        made: row.get(at)?,
        applied: row.get(at + 1)?,
        holding: row.get(at + 2)?,
        rejected: row.get(at + 3)?,
        deferred: row.get(at + 4)?,
        unanswered: row.get(at + 5)?,
    })
}

/// The tally counted from the review records of the items of `db` ([`review_tally`]).
pub(crate) fn count(db: &Connection, top: usize) -> Result<Option<ReviewTally>, CacheError> {
    let query = |sql: &str| format!("{RECORDS}{}", sql.replace("{tally}", TALLY));
    let totals = |row: &rusqlite::Row<'_>| {
        Ok((row.get::<_, u32>(0)?, row.get::<_, u32>(1)?, row.get::<_, Option<String>>(2)?, tally_at(row, 3)?))
    };
    let (reviews, ants, last, findings) = db.query_row(&query(TOTALS), named_params! {":kind": REVIEW}, totals)?;
    let Some(last) = last.and_then(|last| last.parse().ok()).filter(|_| reviews > 0) else { return Ok(None) };
    let params: &[(&str, &dyn ToSql)] = &[(":kind", &REVIEW), (":top", &i64::try_from(top).unwrap_or(i64::MAX))];
    let mut by_kind = Vec::new();
    let mut kinds = db.prepare(&query(BY_KIND))?;
    for row in kinds.query_map(params, |row| Ok((row.get::<_, String>(0)?, tally_at(row, 1)?)))? {
        let (kind, findings) = row?;
        by_kind.push(KindTally { kind, findings });
    }
    let mut by_ant = Vec::new();
    let mut ants_query = db.prepare(&query(BY_ANT))?;
    let rows = ants_query.query_map(params, |row| {
        Ok((row.get::<_, String>(0)?, row.get(1)?, row.get::<_, String>(2)?, tally_at(row, 3)?))
    })?;
    for row in rows {
        let (ant, reviews, last, findings) = row?;
        if let (Ok(ant), Ok(last)) = (ant.parse(), last.parse()) {
            by_ant.push(AntTally { ant, reviews, last, findings });
        }
    }
    Ok(Some(ReviewTally { reviews, ants, last, findings, by_kind, by_ant }))
}

/// The lines of `tally` the sage's identity shows: its reviews and their findings, then by kind and by ant.
pub fn tally_lines(tally: &ReviewTally) -> String {
    let plural = |count: u32, noun: &str| if count == 1 { format!("1 {noun}") } else { format!("{count} {noun}s") };
    let mut lines = format!(
        "Reviews of other ants: {} of {}, the last {}; findings {}.",
        plural(tally.reviews, "review"),
        plural(tally.ants, "ant"),
        tally.last.format("%Y-%m-%d"),
        tally.findings.counts()
    );
    if !tally.by_kind.is_empty() {
        let kinds: Vec<_> =
            tally.by_kind.iter().map(|kind| format!("{} {}", kind.kind, kind.findings.counts())).collect();
        let _ = write!(lines, "\nFindings by kind, the most first: {}.", kinds.join("; "));
    }
    if !tally.by_ant.is_empty() {
        let ants: Vec<_> = tally
            .by_ant
            .iter()
            .map(|ant| {
                let (reviews, last) = (plural(ant.reviews, "review"), ant.last.format("%Y-%m-%d"));
                format!("{}, {reviews}, the last {last}: {}", ant.ant, ant.findings.counts())
            })
            .collect();
        let _ = write!(lines, "\nReviews by ant, the most first: {}.", ants.join("; "));
    }
    lines
}
