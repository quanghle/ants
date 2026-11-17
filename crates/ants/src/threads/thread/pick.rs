//! Which thread a command means ([`Pick`]): the last one of the CLI's user, or a whole id or a unique start of one
//! among the threads of the CLI's user or, for a time-ordered id, of any user.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_daemon::channel::CLOCK_SLACK;
use ants_daemon::stream::reading::{self, Reading};
use ants_proto::{ThreadId, UserId, Uuid, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::LastRawMessageErrorKind;
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, Utc};

use super::entry::Entry;
use super::parser::Parser;
use super::text::time;
use super::{LATE, LISTED, RECENT, window};
use crate::connection::Nats;
use crate::text::excerpt;
use crate::threads::thread_meta;

/// What a THREAD argument or `--thread` picks: a whole thread id (a UUID), taken as given; a start of one (or a whole
/// id of another form), among the threads the CLI's user took part in lately ([`RECENT`]), else among any user's; or
/// `last`, the thread of the latest message to or from that user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pick {
    Id(ThreadId),
    Prefix(String),
    Last,
}

impl std::str::FromStr for Pick {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<Self> {
        let text = text.trim().to_ascii_lowercase();
        if text == "last" {
            return Ok(Self::Last);
        }
        if Uuid::try_parse(&text).is_ok_and(|id| id.hyphenated().to_string() == text) {
            return Ok(Self::Id(text.parse()?));
        }
        match text.parse::<ThreadId>() {
            Ok(_) => Ok(Self::Prefix(text)),
            Err(_) => bail!("{text} is no thread id, start of one or `last`"),
        }
    }
}

impl Pick {
    /// The thread this picks for the CLI's user, reading the stream with `js` unless given a whole thread id. Only a
    /// human's credentials get stream messages by sequence, so only a human picks by a start of an id or `last`. The
    /// threads an error lists show their titles where the credentials may read them.
    pub async fn resolve(self, nats: &Nats<'_>, js: &jetstream::Context) -> Result<ThreadId> {
        let me = &nats.me;
        let prefix = match self {
            Self::Id(thread) => return Ok(thread),
            Self::Prefix(prefix) => Some(prefix),
            Self::Last => None,
        };
        if !me.role.is_human() {
            bail!("the credentials of {} are an ant's, which read no thread back: give the whole thread id", me.id);
        }
        let Some(prefix) = prefix else { return last(js, &me.id).await };
        let own = recent(js, &me.id, window(RECENT)).await?;
        let unpicked = match select(&own, &prefix, &me.id) {
            Ok(Some(thread)) => return Ok(thread),
            Ok(None) => {
                let any = any_recent(js, &prefix).await?;
                match select_any(&any, &own, &prefix, &me.id) {
                    Ok(thread) => return Ok(thread),
                    Err(unpicked) => unpicked.titled(nats, js).await,
                }
            }
            Err(unpicked) => unpicked.titled(nats, js).await,
        };
        Err(anyhow!(unpicked))
    }
}

/// [`Pick::resolve`] for an optional pick.
pub async fn resolve(pick: Option<Pick>, nats: &Nats<'_>, js: &jetstream::Context) -> Result<Option<ThreadId>> {
    match pick {
        Some(pick) => Ok(Some(pick.resolve(nats, js).await?)),
        None => Ok(None),
    }
}

/// The subjects of the messages `user` took part in: its inbox both ways, its channel posts and its notes posts.
pub(super) fn own_subjects(user: &UserId) -> [String; 4] {
    [subject::inbox_of(user), subject::inbox_from(user), subject::chan_from(user), subject::notes_from(user)]
}

/// The thread of the latest message to or from `user` ([`own_subjects`]), however old, other than a summary
/// request's or reply's: when the last of a subject is one, the latest of the last [`RECENT`] if any.
pub(super) async fn last(js: &jetstream::Context, user: &UserId) -> Result<ThreadId> {
    let stream = js.get_stream(ants_proto::STREAM).await.context("opening the ANTS stream")?;
    let mut parser = Parser::default();
    let mut latest: Option<Entry> = None;
    let mut catch_up = false;
    for filter in own_subjects(user) {
        let msg = match stream.get_last_raw_message_by_subject(&filter).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => continue,
            Err(e) => return Err(e).with_context(|| format!("reading the last message on {filter}")),
        };
        let stored = DateTime::<Utc>::from(SystemTime::from(msg.time));
        let entry = parser.entry(msg.sequence, stored, &msg.subject, &msg.payload).filter(|e| !e.forged);
        catch_up |= entry.as_ref().is_some_and(|e| e.summary.is_some());
        let entry = entry.filter(|e| e.summary.is_none());
        if let Some(entry) = entry.filter(|e| latest.as_ref().is_none_or(|l| l.seq < e.seq)) {
            latest = Some(entry);
        }
    }
    if catch_up && let Some(recent) = recent(js, user, window(RECENT)).await?.into_iter().next() {
        return Ok(recent.thread_id);
    }
    latest.map(|e| e.thread_id).ok_or_else(|| anyhow!("no message to or from {user} in the stream, so no last thread"))
}

/// The latest message of each thread `user` took part in ([`own_subjects`]) in the last [`RECENT`], most recent first.
pub async fn recent_threads(js: &jetstream::Context, user: &UserId) -> Result<Vec<Entry>> {
    recent(js, user, window(RECENT)).await
}

/// The latest message of each thread `user` took part in ([`own_subjects`]) stored from `start` on, most recent first,
/// summary requests and replies left out.
pub(super) async fn recent(
    js: &jetstream::Context,
    user: &UserId,
    start: consumer::DeliverPolicy,
) -> Result<Vec<Entry>> {
    let mut parser = Parser::default();
    let mut latest: HashMap<ThreadId, Entry> = HashMap::new();
    for filter in own_subjects(user) {
        reading::read_back(js, "", Reading::Gets, &filter, start, |seq, stored, subject, payload| {
            let entry = parser.entry(seq, stored.into(), subject, payload).filter(|e| !e.forged && e.summary.is_none());
            if let Some(entry) = entry {
                match latest.get(&entry.thread_id) {
                    Some(kept) if kept.seq > entry.seq => {}
                    _ => _ = latest.insert(entry.thread_id.clone(), entry),
                }
            }
            ControlFlow::Continue(())
        })
        .await?;
    }
    let mut threads: Vec<Entry> = latest.into_values().collect();
    threads.sort_by_key(|e| std::cmp::Reverse(e.seq));
    Ok(threads)
}

/// The latest message of each thread with an id starting with `prefix` stored in the last [`RECENT`], whoever took
/// part in it, most recent first.
pub(super) async fn any_recent(js: &jetstream::Context, prefix: &str) -> Result<Vec<Entry>> {
    let mut threads: HashMap<ThreadId, Entry> = HashMap::new();
    scan_made(js, prefix, |entry| {
        if entry.thread_id.as_str().starts_with(prefix) {
            threads.insert(entry.thread_id.clone(), entry);
        }
    })
    .await?;
    let mut threads: Vec<Entry> = threads.into_values().collect();
    threads.sort_by_key(|e| std::cmp::Reverse(e.seq));
    Ok(threads)
}

/// Hand `take` the entry of every message, not forged, stored in the last [`RECENT`] when an id starting with `prefix`
/// may have been made, oldest first. For a start of a time-ordered id, only the messages stored while such an id was
/// made, give or take the clocks' skew ([`CLOCK_SLACK`]) and up to [`LATE`] after, are read: a thread's first message,
/// or the message with such an id, is.
pub(super) async fn scan_made(js: &jetstream::Context, prefix: &str, mut take: impl FnMut(Entry)) -> Result<()> {
    let now = SystemTime::now();
    let floor = now.checked_sub(RECENT).unwrap_or(SystemTime::UNIX_EPOCH);
    let (from, to) = match made_within(prefix) {
        Some((first, last)) => (first.checked_sub(CLOCK_SLACK).unwrap_or(first).max(floor), (last + LATE).min(now)),
        None => (floor, now),
    };
    if from > to {
        return Ok(());
    }
    let mut parser = Parser::default();
    let start = consumer::DeliverPolicy::ByStartTime { start_time: from.into() };
    reading::read_back(js, "", Reading::Gets, subject::ALL, start, |seq, stored, subject, payload| {
        if stored > to {
            return ControlFlow::Break(());
        }
        if let Some(entry) = parser.entry(seq, stored.into(), subject, payload).filter(|e| !e.forged) {
            take(entry);
        }
        ControlFlow::Continue(())
    })
    .await
}

/// The span in which a time-ordered (version 7 UUID) thread id starting with `prefix` was made, from the time its
/// first 12 hex digits carry; None when no such id starts so, or the start leaves the time open.
pub(super) fn made_within(prefix: &str) -> Option<(SystemTime, SystemTime)> {
    let bytes = prefix.as_bytes();
    if bytes.get(14).is_some_and(|&b| b != b'7') {
        return None;
    }
    let mut digits = String::new();
    for (i, &b) in bytes.iter().take(13).enumerate() {
        match (i, b) {
            (8, b'-') => {}
            (8, _) => return None,
            (_, b) if b.is_ascii_hexdigit() => digits.push(b as char),
            _ => return None,
        }
    }
    if digits.is_empty() {
        return None;
    }
    let millis = |pad: char| {
        let padded: String = digits.chars().chain(std::iter::repeat(pad)).take(12).collect();
        u64::from_str_radix(&padded, 16).ok()
    };
    let at = |ms: u64| SystemTime::UNIX_EPOCH + Duration::from_millis(ms);
    Some((at(millis('0')?), at(millis('f')? + 1)))
}

/// Threads among `threads` whose id is `prefix`, else those starting with it.
pub(super) fn matching<'a>(threads: &'a [Entry], prefix: &str) -> Vec<&'a Entry> {
    match threads.iter().find(|e| e.thread_id.as_str() == prefix) {
        Some(exact) => vec![exact],
        None => threads.iter().filter(|e| e.thread_id.as_str().starts_with(prefix)).collect(),
    }
}

/// Why a start of an id picks no thread: what to say, and the threads to list beneath, with their titles if known.
#[derive(Debug)]
pub(super) struct Unpicked<'a> {
    why: String,
    listed: Vec<&'a Entry>,
    titles: HashMap<ThreadId, String>,
}

impl<'a> Unpicked<'a> {
    fn new(why: String, listed: impl IntoIterator<Item = &'a Entry>) -> Self {
        Self { why, listed: listed.into_iter().collect(), titles: HashMap::new() }
    }

    /// The same, with the titles of the threads listed, read as `ants thread list` reads them; none where the
    /// credentials may not read them, which a request would only wait on.
    async fn titled(self, nats: &Nats<'_>, js: &jetstream::Context) -> String {
        if self.listed.is_empty() {
            return self.to_string();
        }
        let shown = self.listed.iter().take(LISTED).map(|e| &e.thread_id);
        let titles = match thread_meta::open(nats, js).await {
            Ok(meta) => meta.titles(shown).await,
            Err(e) => Err(e),
        };
        let titles = titles.unwrap_or_else(|e| {
            tracing::debug!("listing threads without their titles: {e:#}");
            HashMap::new()
        });
        Self { titles: titles.into_iter().map(|(thread, title)| (thread, title.title)).collect(), ..self }.to_string()
    }
}

impl std::fmt::Display for Unpicked<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.listed.is_empty() {
            return f.write_str(&self.why);
        }
        write!(f, "{}:\n{}", self.why, listing(self.listed.iter().copied(), &self.titles))
    }
}

/// The one thread among `threads` `user` took part in (the latest message of each, most recent first) whose id is
/// `prefix`, else starts with it; None when none does. Several are listed.
pub(super) fn select<'a>(threads: &'a [Entry], prefix: &str, user: &UserId) -> Result<Option<ThreadId>, Unpicked<'a>> {
    let found = matching(threads, prefix);
    match found.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(one.thread_id.clone())),
        _ => Err(Unpicked::new(
            format!(
                "{} threads {user} took part in over the last {} days start with {prefix}; give more of the id",
                found.len(),
                RECENT.as_secs() / 86_400,
            ),
            found,
        )),
    }
}

/// The one thread among `threads` (any user's, the latest message of each, most recent first) whose id is `prefix`,
/// else starts with it, for a `user` who took part in none of them. None lists `own`, the threads `user` took part
/// in, and several list those found.
pub(super) fn select_any<'a>(
    threads: &'a [Entry],
    own: &'a [Entry],
    prefix: &str,
    user: &UserId,
) -> Result<ThreadId, Unpicked<'a>> {
    let found = matching(threads, prefix);
    let days = RECENT.as_secs() / 86_400;
    match found.as_slice() {
        [one] => Ok(one.thread_id.clone()),
        [] if own.is_empty() => Err(Unpicked::new(
            format!("no thread over the last {days} days starts with {prefix}: give the whole thread id"),
            [],
        )),
        [] => Err(Unpicked::new(
            format!(
                "no thread over the last {days} days starts with {prefix}; give the whole id of an older one, or pick \
                 one of the most recent {user} took part in"
            ),
            own,
        )),
        _ => Err(Unpicked::new(
            format!(
                "{} threads over the last {days} days start with {prefix}, none of them with {user}; give more of the id",
                found.len()
            ),
            found,
        )),
    }
}

/// Threads as an error lists them, one per line: id, time of the latest message, the thread's title in `titles` if
/// any, and the route and first line of that message.
pub(super) fn listing<'a>(
    threads: impl ExactSizeIterator<Item = &'a Entry>,
    titles: &HashMap<ThreadId, String>,
) -> String {
    let more = threads.len().saturating_sub(LISTED);
    let mut lines: Vec<String> = threads
        .take(LISTED)
        .map(|e| {
            let title = titles.get(&e.thread_id).map(|t| format!("\"{}\"  ", excerpt(t, 48)));
            let (id, at, route) = (&e.thread_id, time(e.stored_at), e.route());
            format!("  {id}  {at}  {}{route}: {}", title.unwrap_or_default(), excerpt(&e.text, 48))
        })
        .collect();
    if more > 0 {
        lines.push(format!("  and {more} more"));
    }
    lines.join("\n")
}
