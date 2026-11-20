//! What an ant has done, counted from the records of its threads of work ([`super::record`], [`work_records`]) by a
//! query of the cache each time it is asked: no counter to keep in step, nothing written. The brief's identity, the
//! growth report, a dream's packet and its findings on the identity show it.
//!
//! - The threads of work are those of the records, one per thread sense saw start, but for the threads of dreams and
//!   reviews: those a dream names, its own and the review thread its sage answered on, those an ant asking a review
//!   started and those a review record names, of the ant's reviews of others. The done ones, those marked done at
//!   least once.
//! - The active days are the UTC days on which a thread started, started again or was marked done. A record keeps
//!   only the first start and the last start again and done of its thread, so a thread worked over many days counts
//!   three of them at most; a session expiring is no activity.
//! - The people are the humans and other ants who started its threads: not the ant itself, under any of its names,
//!   handing work to itself on another thread, nor webhooks and providers.
//! - The repositories are those of the ant's working directory when each thread first started.
//!
//! Each of the people and repositories counts its threads and their last activity, a start, a start again or a done;
//! the most threads come first, then the latest.
//!
//! Counted since a time ([`experience_since`]), as a dream's packet counts it, only the activity since then counts: the
//! threads started, started again or marked done since, the done ones those marked done since, and the days of that
//! activity alone.

use std::collections::HashSet;

use ants_proto::UserId;
use ants_proto::provider::Starter;
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, ToSql, named_params};
use serde::Serialize;
use serde_json::Value;

use super::record::{self, KIND, Record};
use crate::cache::CacheError;
use crate::dream::{DREAM, REVIEW};
use crate::items::Values;
use crate::memory::Scope;
use crate::store::{Items, StoreError};

/// What an ant has done, from its thread records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Experience {
    pub threads: u32,
    /// The threads marked done.
    pub done: u32,
    pub active_days: u32,
    /// The humans and ants who started its threads, `human:<name>` or `ant:<name>`, the most threads first.
    pub people: Vec<Tally>,
    /// The repositories its threads started in, `repo:<remote>`, the most threads first.
    pub repos: Vec<Tally>,
}

/// One of the people or repositories of an [`Experience`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Tally {
    pub scope: Scope,
    pub threads: u32,
    /// The last activity of those threads, to the millisecond.
    pub last: DateTime<Utc>,
}

/// The thread records of the cache's items, their times as Julian days, each with its last activity: those active since
/// `:since`, an RFC 3339 time or null for all, with only the times since then; those of work alone ([`work_records`]),
/// the dreams of kind `:dream` and review records of kind `:review` naming the others.
const RECORDS: &str = "
WITH bounds AS (SELECT ifnull(julianday(:since), 0) AS since),
others AS (
    SELECT json_extract(item, '$.values.thread') AS thread FROM items WHERE kind IN (:dream, :review)
    UNION SELECT json_extract(item, '$.values.review.thread') FROM items WHERE kind = :dream
),
stored AS (
    SELECT
        json_extract(item, '$.values.origin.by.kind') AS starter,
        json_extract(item, '$.values.origin.by.user') AS user,
        json_extract(item, '$.values.repo') AS repo,
        julianday(json_extract(item, '$.values.started')) AS started,
        julianday(json_extract(item, '$.values.restarted')) AS restarted,
        julianday(json_extract(item, '$.values.done')) AS done
    FROM items WHERE kind = :kind
        AND NOT ifnull(json_extract(item, '$.values.origin.by.kind') = 'review', 0)
        AND json_extract(item, '$.values.thread') NOT IN (SELECT thread FROM others WHERE thread IS NOT NULL)
),
records AS (
    SELECT
        starter,
        user,
        repo,
        CASE WHEN started >= since THEN started END AS started,
        CASE WHEN restarted >= since THEN restarted END AS restarted,
        CASE WHEN done >= since THEN done END AS done
    FROM stored, bounds WHERE max(started, ifnull(restarted, 0), ifnull(done, 0)) >= since
),
active AS (SELECT *, max(ifnull(started, 0), ifnull(restarted, 0), ifnull(done, 0)) AS last FROM records)";

const TOTALS: &str = "
SELECT count(*), count(done), (
    SELECT count(DISTINCT day) FROM (
        SELECT date(started) AS day FROM records
        UNION ALL SELECT date(restarted) FROM records
        UNION ALL SELECT date(done) FROM records
    )
) FROM records";

/// The most frequent values of `{scope}`, an expression over `active` that is null for a record not counted.
const TALLIES: &str = "
SELECT scope, count(*) AS threads, strftime('%Y-%m-%dT%H:%M:%fZ', max(last)) AS latest
FROM (SELECT {scope} AS scope, last FROM active) WHERE scope IS NOT NULL
GROUP BY scope ORDER BY threads DESC, max(last) DESC, scope LIMIT :top";

/// The scope of a thread's starter, `:own` the JSON array of the ant's names.
const PEOPLE: &str =
    "CASE WHEN starter = 'human' OR (starter = 'ant' AND user NOT IN (SELECT value FROM json_each(:own)))
    THEN starter || ':' || user END";

const REPOS: &str = "'repo:' || repo";

/// The experience of `ant` in `items`, with the `top` people and repositories it worked with most; none before any
/// record of its threads of work.
pub async fn experience(items: &dyn Items, ant: &UserId, top: usize) -> Result<Option<Experience>, StoreError> {
    counted(items, ant, None, top).await
}

/// The experience of `ant` in `items` since `since`, counting only the activity since then ([`experience`]); none when
/// no thread of work was active since.
pub async fn experience_since(
    items: &dyn Items,
    ant: &UserId,
    since: DateTime<Utc>,
    top: usize,
) -> Result<Option<Experience>, StoreError> {
    counted(items, ant, Some(since), top).await
}

async fn counted(
    items: &dyn Items,
    ant: &UserId,
    since: Option<DateTime<Utc>>,
    top: usize,
) -> Result<Option<Experience>, StoreError> {
    let mut own = items.earlier_names().await;
    own.push(ant.clone());
    items.experience(&own, since, top).await
}

/// The records of the ant's threads of work in `items`, the last changed first: not those of its dreams, nor of
/// reviews, as [`experience`] counts them.
pub async fn work_records(items: &dyn Items) -> Result<Vec<Record>, StoreError> {
    let thread = |values: &Values| values.get("thread").and_then(Value::as_str).map(str::to_owned);
    let mut others = HashSet::new();
    for item in items.list(DREAM, usize::MAX).await? {
        others.extend(thread(&item.values));
        others.extend(item.values.get("review").and_then(Value::as_object).and_then(thread));
    }
    others.extend(items.list(REVIEW, usize::MAX).await?.iter().filter_map(|item| thread(&item.values)));
    let records = record::records(items, usize::MAX).await?.into_iter();
    let work = |record: &Record| {
        !matches!(record.origin.by, Starter::Review { .. }) && !others.contains(record.thread.as_str())
    };
    Ok(records.filter(work).collect())
}

/// The experience counted from the items of `db`, `own` the ant's names, since `since` if given ([`experience`]).
pub(crate) fn count(
    db: &Connection,
    own: &[UserId],
    since: Option<DateTime<Utc>>,
    top: usize,
) -> Result<Option<Experience>, CacheError> {
    let since = since.map(|since| since.to_rfc3339_opts(SecondsFormat::Millis, true));
    let base = named_params! {":kind": KIND, ":since": since, ":dream": DREAM, ":review": REVIEW};
    let totals = |row: &rusqlite::Row<'_>| Ok((row.get(0)?, row.get(1)?, row.get(2)?));
    let (threads, done, active_days) = db.query_row(&format!("{RECORDS}{TOTALS}"), base, totals)?;
    if threads == 0 {
        return Ok(None);
    }
    let (own, top) = (serde_json::to_string(own)?, i64::try_from(top).unwrap_or(i64::MAX));
    let people = tallies(db, PEOPLE, &[base, named_params! {":own": own, ":top": top}].concat())?;
    let repos = tallies(db, REPOS, &[base, named_params! {":top": top}].concat())?;
    Ok(Some(Experience { threads, done, active_days, people, repos }))
}

/// The tallies of [`TALLIES`] for `scope`; a row whose scope or time does not parse is left out.
fn tallies(db: &Connection, scope: &str, params: &[(&str, &dyn ToSql)]) -> Result<Vec<Tally>, CacheError> {
    let mut query = db.prepare(&format!("{RECORDS}{}", TALLIES.replace("{scope}", scope)))?;
    let rows = query.query_map(params, |row| Ok((row.get::<_, String>(0)?, row.get(1)?, row.get::<_, String>(2)?)))?;
    let mut tallies = Vec::new();
    for row in rows {
        let (scope, threads, last) = row?;
        if let (Ok(scope), Ok(last)) = (scope.parse(), last.parse()) {
            tallies.push(Tally { scope, threads, last });
        }
    }
    Ok(tallies)
}

#[cfg(test)]
mod tests {
    use ants_proto::SenseId;
    use ants_proto::provider::{Event, EventKind, Origin, Starter};
    use chrono::TimeZone as _;
    use uuid::Uuid;

    use super::*;

    use crate::log::MemoryLog;

    use crate::thread::record::record_event;

    fn day(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 1, day, hour, 0, 0).unwrap()
    }

    fn name(name: &str) -> UserId {
        name.parse().unwrap()
    }

    fn started(by: Starter, repo: Option<&str>) -> EventKind {
        EventKind::ThreadStarted { origin: Origin { by, channel: None }, repo: repo.map(Into::into) }
    }

    fn tally(scope: &str, threads: u32, last: DateTime<Utc>) -> Tally {
        Tally { scope: scope.parse().unwrap(), threads, last }
    }

    #[tokio::test]
    async fn the_experience_counts_the_threads_their_days_and_who_started_them_where() {
        let items = crate::testing::store(MemoryLog::default(), SenseId::new(), name("dev-a")).await;
        assert_eq!(experience(&items, &name("dev-a"), 3).await.unwrap(), None, "no record, no experience");

        let (alice, r, s) = (|| Starter::Human { user: name("alice") }, Some("github.com/o/r"), Some("github.com/o/s"));
        let told = [
            (1, day(1, 9), started(alice(), r)),
            (1, day(2, 9), EventKind::ThreadDone),
            (2, day(3, 9), started(alice(), r)),
            (2, day(3, 18), started(alice(), r)),
            (3, day(4, 9), started(Starter::Ant { user: name("dev-b") }, s)),
            (4, day(4, 10), started(Starter::Ant { user: name("dev-a") }, r)),
            (5, day(5, 9), started(Starter::Webhook { user: name("hook") }, None)),
            (5, day(6, 9), EventKind::ThreadExpired),
            (6, day(5, 10), started(Starter::Provider { name: "sense".into() }, s)),
            (7, day(5, 12), started(Starter::Human { user: name("bob") }, None)),
        ];
        for (id, (thread, at, kind)) in (1..).zip(told) {
            let thread = Some(format!("019a7d2c-0000-7000-8000-{thread:012}").parse().unwrap());
            let event =
                Event { id: Uuid::from_u128(id), ant: name("dev-a"), identity: "0f1e".into(), thread, at, kind };
            record_event(&items, &event).await.unwrap().unwrap();
        }
        let expected = Experience {
            threads: 7,
            done: 1,
            active_days: 5,
            people: vec![
                tally("human:alice", 2, day(3, 18)),
                tally("human:bob", 1, day(5, 12)),
                tally("ant:dev-b", 1, day(4, 9)),
            ],
            repos: vec![tally("repo:github.com/o/r", 3, day(4, 10)), tally("repo:github.com/o/s", 2, day(5, 10))],
        };
        assert_eq!(experience(&items, &name("dev-a"), 3).await.unwrap().unwrap(), expected);
        let first = items.experience(&[name("dev-a")], None, 1).await.unwrap().unwrap();
        assert_eq!((first.people, first.repos), (expected.people[..1].to_vec(), expected.repos[..1].to_vec()));
        let renamed = items.experience(&[name("dev-b"), name("dev-a")], None, 3).await.unwrap().unwrap();
        assert_eq!(renamed.people, expected.people[..2], "dev-b, an earlier name of the ant, is no other");

        let done_since = experience_since(&items, &name("dev-a"), day(2, 9), 3).await.unwrap().unwrap();
        assert_eq!(
            (done_since.threads, done_since.done, done_since.active_days),
            (7, 1, 4),
            "thread 1, started before, counts for its done alone"
        );
        let expected = Experience {
            threads: 5,
            done: 0,
            active_days: 2,
            people: vec![tally("human:bob", 1, day(5, 12)), tally("ant:dev-b", 1, day(4, 9))],
            repos: vec![tally("repo:github.com/o/s", 2, day(5, 10)), tally("repo:github.com/o/r", 1, day(4, 10))],
        };
        let since = experience_since(&items, &name("dev-a"), day(4, 0), 3).await.unwrap();
        assert_eq!(since.unwrap(), expected, "threads 1 and 2 were active before");
        let after = experience_since(&items, &name("dev-a"), day(6, 0), 3).await.unwrap();
        assert_eq!(after, None, "a session expiring is no activity");
    }
}
