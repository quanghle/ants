//! `ants thread`: the threads in the ANTS stream, across inboxes (both ways), channel posts, notes, escalations, tool
//! requests and their decisions, dead letters, and the tool calls and prompts ended with no reply ants stored on their
//! timelines.
//!
//! Read back by a human by message gets (batched direct gets, [`Reading::Gets`]), so nothing is consumed or acked
//! and no consumer is left behind. Only notes subjects name their thread, so a read goes through every message stored
//! in its time window: `--since` bounds it, and `ants thread show` of a thread with a time-ordered id starts at the
//! thread's start.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ants_daemon::stream::reading::{self, Reading};
use ants_proto::{Mark, NoReplyWhy, ThreadId, ThreadMarks, Timeline, Timelines, UserId, Uuid, subject};
use anyhow::{Result, anyhow, bail};
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, Utc};
use serde::Serialize;

use self::summaries::Done;
use crate::connection::Nats;
use crate::{sealed, thread_meta};

pub use self::entry::{Entry, Kind, last_reopening};
pub use self::parser::Parser;
pub use self::pick::{Pick, recent_threads, resolve};
pub use self::summaries::Summary;
pub(crate) use self::summaries::{Select, Threads};
pub(crate) use self::text::{answer_line, time, took};
pub use self::text::{answers_to, render, table};
pub use message::{MessagePick, find as find_message, recent_messages};

mod entry;
mod message;
mod parser;
mod pick;
mod summaries;
mod text;

/// How long the opening message's line in `ants thread list` may be.
const OPENING: usize = 72;
/// How far back a start of a thread id looks for the threads of the CLI's user.
pub const RECENT: Duration = Duration::from_hours(7 * 24);
/// How long after its time-ordered id is made a thread's first message may be stored, clocks' skew included, for a start
/// of an id matched among threads of any user.
const LATE: Duration = Duration::from_mins(10);
/// How many threads an error on a start of a thread id lists.
const LISTED: usize = 10;
/// How far back `ants thread list` reads by default.
const SINCE: Duration = Duration::from_hours(24);

/// A tool call an ant stored on its timeline, as a [`Kind::Tool`] entry has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolCall {
    /// The call's id, unique within its prompt.
    pub call_id: String,
    /// The message whose prompt made the call.
    pub message: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
}

/// A prompt an ant ended with no reply sent, from its timeline, as a [`Kind::NoReply`] entry has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NoReply {
    /// The message whose prompt ended so.
    pub message: Uuid,
    pub why: NoReplyWhy,
    /// The reason the ant gave to `complete`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// What of its ants' timelines [`history`] adds to a thread's messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    /// Only the messages.
    None,
    /// The prompts ended with no reply sent.
    NoReplies,
    /// The tool calls and the prompts ended with no reply sent.
    All,
}

/// Where a read of the last `since` starts: everything kept for zero.
pub fn window(since: Duration) -> consumer::DeliverPolicy {
    match SystemTime::now().checked_sub(since) {
        Some(start) if !since.is_zero() => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        _ => consumer::DeliverPolicy::All,
    }
}

/// The time threads last active then or before count as archived, `auto_archive` before `now`; None when
/// `auto_archive` is.
pub fn idle_before(auto_archive: Option<Duration>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    now.checked_sub_signed(chrono::TimeDelta::from_std(auto_archive?).ok()?)
}

/// Where `ants thread show` reads `thread` from: as `since` says, else the thread's start when its id is
/// time-ordered, else everything kept.
pub(crate) fn show_start(thread: &ThreadId, since: Option<Duration>) -> consumer::DeliverPolicy {
    match (since, ants_daemon::channel::thread_start(thread)) {
        (Some(since), _) => window(since),
        (None, Some(start)) => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        (None, None) => consumer::DeliverPolicy::All,
    }
}

/// Hand `take` the entry of every message stored from `start` on, oldest first.
pub async fn scan(js: &jetstream::Context, start: consumer::DeliverPolicy, take: impl FnMut(Entry)) -> Result<()> {
    scan_opening(js, start, None, take).await
}

/// [`scan`], opening sealed messages with `keyring`, which looks up a sending device added since it was loaded.
pub async fn scan_opening(
    js: &jetstream::Context,
    start: consumer::DeliverPolicy,
    keyring: Option<Arc<sealed::Keyring>>,
    mut take: impl FnMut(Entry),
) -> Result<()> {
    let mut parser = Parser::opening(keyring);
    reading::read_back_awaiting(js, "", Reading::Gets, subject::ALL, start, async |seq, stored, msg| {
        match parser.read(seq, stored.into(), &msg.subject, &msg.payload).await {
            Some(entry) => take(entry),
            None => tracing::debug!(seq, subject = %msg.subject, "skipping a message on no thread"),
        }
        ControlFlow::Continue(())
    })
    .await
}

/// Connect as the CLI's human: only a human's credentials get stream messages by sequence.
pub async fn connect(nats: &Nats<'_>) -> Result<jetstream::Context> {
    if !nats.me.role.is_human() {
        bail!(
            "the credentials of {} are an ant's, which read no other inbox; `ants thread` reads them all with a \
             human's",
            nats.me.id
        );
    }
    Ok(nats.connect("ants-thread").await?.1)
}

/// `ants thread list`: the threads with messages stored within `since` (by default [`SINCE`], or for `--pinned` and
/// `--archived` since the earliest thread marked so started, or all the stream keeps for `--archived` when idle
/// threads count as archived), as `select` keeps them, with their titles.
pub async fn list(nats: &Nats<'_>, since: Option<Duration>, select: &Select, json: bool) -> Result<()> {
    let (threads, omitted) = listed(nats, since, select).await?;
    if json {
        for thread in &threads {
            println!("{}", serde_json::to_string(thread)?);
        }
    } else if threads.is_empty() {
        println!("no threads");
    } else {
        println!("{}", table(&threads));
    }
    if omitted > 0 {
        eprintln!("{omitted} less recently active threads left out: raise --limit or narrow --since");
    }
    Ok(())
}

/// The threads [`list`] lists, and how many less recently active ones `select`'s limit left out.
pub async fn listed(nats: &Nats<'_>, since: Option<Duration>, select: &Select) -> Result<(Vec<Summary>, usize)> {
    let auto_archive = crate::defaults::auto_archive()?;
    let select = &Select { idle_before: idle_before(auto_archive, Utc::now()), ..select.clone() };
    let js = connect(nats).await?;
    let meta = match thread_meta::open(nats, &js).await {
        Ok(meta) => Some(meta),
        Err(e) if !select.pinned && !select.archived => {
            tracing::warn!("not showing titles, pinned or archived threads: {e:#}");
            None
        }
        Err(e) => return Err(e),
    };
    let marks = match &meta {
        Some(meta) => meta.marks().await?,
        None => ThreadMarks::default(),
    };
    let start = match since {
        Some(since) => Some(window(since)),
        None if select.pinned => marked_start(marks.pinned.keys()),
        None if select.archived && auto_archive.is_some() => Some(consumer::DeliverPolicy::All),
        None if select.archived => marked_start(marks.archived.keys()),
        None => Some(window(SINCE)),
    };
    let mut threads = Threads::new(select.grep.as_deref());
    if select.escalated {
        threads = threads.escalated(crate::humans::load(&js, &nats.me).await);
    }
    if let Some(start) = start {
        let keyring = sealed::Keyring::load(nats, &js).await.map(Arc::new);
        scan_opening(&js, start, keyring, |entry| threads.add(&entry)).await?;
    }
    let present: HashMap<ThreadId, DateTime<Utc>> =
        threads.summaries().map(|t| (t.thread_id.clone(), t.last_at)).collect();
    let (mut threads, omitted) = threads.finish(select, &marks);
    if let Some(meta) = &meta {
        let mut titles = meta.titles(threads.iter().map(|t| &t.thread_id)).await?;
        let mut dones = meta.dones(threads.iter().map(|t| &t.thread_id)).await?;
        let behalf = crate::behalf::Checker::new(nats, &js);
        for thread in &mut threads {
            thread.title = titles.remove(&thread.thread_id).map(|t| t.title);
            let done = dones.remove(&thread.thread_id).filter(|d| d.holds(thread.last_reopening));
            thread.done = match done {
                Some(d) => {
                    Some(Done { acting: behalf.check(&d.by, d.on_behalf_of.as_ref()).await, by: d.by, at: d.at })
                }
                None => None,
            };
        }
        adopt_suggested(meta, &mut threads).await;
    }
    if let (Some(meta), Some(start)) = (&meta, &start) {
        prune(meta, &marks, start, &present).await;
    }
    Ok((threads, omitted))
}

/// Forget the marks of the CLI's human and the titles of the threads a read from `start` found no message on though
/// it read every message stored on them ([`covers`]), set more than [`thread_meta::KEPT`] ago, and the human's keeps
/// of threads `present` (each with its last message's time) since active again; a failure is only warned of. Titles
/// are looked through only when the read went back that far.
pub async fn prune(
    meta: &thread_meta::Meta,
    marks: &ThreadMarks,
    start: &consumer::DeliverPolicy,
    present: &HashMap<ThreadId, DateTime<Utc>>,
) {
    let titles = match start {
        consumer::DeliverPolicy::All => true,
        consumer::DeliverPolicy::ByStartTime { start_time } => {
            SystemTime::now().checked_sub(thread_meta::KEPT).is_some_and(|kept| SystemTime::from(*start_time) <= kept)
        }
        _ => false,
    };
    let gone = |thread: &ThreadId| !present.contains_key(thread) && covers(start, thread);
    let active = |thread: &ThreadId, kept| present.get(thread).is_some_and(|last_at| *last_at >= kept);
    match meta.prune(marks, titles, gone, active).await {
        Ok((0, 0)) => {}
        Ok((marks, titles)) => tracing::debug!(marks, titles, "forgot the marks and titles of threads with no message"),
        Err(e) => tracing::warn!("not forgetting the marks and titles of threads with no message: {e:#}"),
    }
}

/// Whether a read from `start` reads every message stored on `thread`: one from the thread's start on, when its id
/// is time-ordered.
fn covers(start: &consumer::DeliverPolicy, thread: &ThreadId) -> bool {
    match start {
        consumer::DeliverPolicy::All => true,
        consumer::DeliverPolicy::ByStartTime { start_time } => {
            ants_daemon::channel::thread_start(thread).is_some_and(|at| at >= SystemTime::from(*start_time))
        }
        _ => false,
    }
}

/// Title each of `threads` without a title as an ant suggested, if it never had one
/// ([`thread_meta::Meta::adopt_title`]) and no sealed message is on it.
async fn adopt_suggested(meta: &thread_meta::Meta, threads: &mut [Summary]) {
    for thread in threads.iter_mut().filter(|t| t.title.is_none() && !t.sealed) {
        let Some((by, text)) = &thread.suggested else { continue };
        match meta.adopt_title(&thread.thread_id, text, by).await {
            Ok(title) => thread.title = title.map(|t| t.title),
            Err(e) => tracing::warn!(thread = %thread.thread_id, "not storing the title {by} suggested: {e:#}"),
        }
    }
}

/// Where to read threads marked so from: the earliest of their starts, or everything kept when an id is not
/// time-ordered; None when no thread is marked so.
fn marked_start<'a>(threads: impl Iterator<Item = &'a ThreadId>) -> Option<consumer::DeliverPolicy> {
    let mut earliest = None;
    for thread in threads {
        let Some(start) = ants_daemon::channel::thread_start(thread) else {
            return Some(consumer::DeliverPolicy::All);
        };
        earliest = Some(earliest.map_or(start, |e: SystemTime| e.min(start)));
    }
    earliest.map(|start| consumer::DeliverPolicy::ByStartTime { start_time: start.into() })
}

/// Every message stored on `thread`, in stream order, with the reactions to each: read from `since` ago, else from the
/// thread's start when its id is time-ordered, else from all the stream keeps. With `activity`, what its ants stored on
/// their timelines too, each tool call and prompt ended with no reply once as its latest record has it, a call placed
/// by when it started and an end by when its record was made. Sealed messages are opened with `keyring`, which looks
/// up a sending device added since it was loaded.
pub async fn history(
    js: &jetstream::Context,
    thread: &ThreadId,
    since: Option<Duration>,
    activity: Activity,
    keyring: Option<Arc<sealed::Keyring>>,
) -> Result<Vec<Entry>> {
    history_with(js, thread, since, activity, Parser::opening(keyring)).await
}

/// [`history`], read with `parser`.
pub async fn history_with(
    js: &jetstream::Context,
    thread: &ThreadId,
    since: Option<Duration>,
    activity: Activity,
    mut parser: Parser,
) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    let mut timelines = Timelines::default();
    let mut seqs = HashMap::new();
    let start = show_start(thread, since);
    reading::read_back_awaiting(js, "", Reading::Gets, subject::ALL, start, async |seq, stored, msg| {
        let (subject, payload) = (msg.subject.as_str(), &msg.payload[..]);
        if let Some((on, _)) = subject::parse_timeline(subject) {
            if activity != Activity::None && on == *thread {
                add_timeline(&mut timelines, &mut seqs, seq, subject, payload);
            }
        } else if let Some(entry) = parser.read(seq, stored.into(), subject, payload).await
            && entry.thread_id == *thread
        {
            entries.push(entry);
        }
        ControlFlow::Continue(())
    })
    .await?;
    if !timelines.is_empty() {
        let seq = |key| seqs.get(&key).copied().unwrap_or_default();
        let calls = timelines
            .calls()
            .iter()
            .filter(|_| activity == Activity::All)
            .map(|call| Entry::tool(seq((call.ant.clone(), call.message, Some(call.call.id.clone()))), call));
        entries.extend(calls);
        let ends =
            timelines.no_replies().iter().map(|end| Entry::no_reply(seq((end.ant.clone(), end.message, None)), end));
        entries.extend(ends);
        entries.sort_by_key(|e| e.stored_at);
    }
    let ids: Vec<Uuid> = entries.iter().filter_map(|e| e.id).collect();
    let mut reactions = crate::inbox::reactions_or_none(js, &ids).await;
    if let Some(behalf) = &parser.behalf {
        behalf.reactions(&mut reactions).await;
    }
    for entry in &mut entries {
        if let Some(id) = entry.id {
            entry.reactions = reactions.remove(&id).unwrap_or_default();
        }
    }
    Ok(entries)
}

/// The tool calls and the end with no reply of the timeline record stored at `seq` on `subject`, entries of kind tool
/// and no reply as that record has them; none for a record that does not parse or that its subject does not name.
pub fn timeline_entries(seq: u64, subject: &str, payload: &[u8]) -> Vec<Entry> {
    let mut timelines = Timelines::default();
    let Ok(record) = serde_json::from_slice::<Timeline>(payload) else { return Vec::new() };
    if !timelines.add(subject, record) {
        return Vec::new();
    }
    let calls = timelines.calls().iter().map(|c| Entry::tool(seq, c));
    calls.chain(timelines.no_replies().iter().map(|end| Entry::no_reply(seq, end))).collect()
}

/// Merge the timeline record stored at `seq` on `subject`, noting it as the last of each call and end it holds (an
/// end keyed with no call id).
fn add_timeline(
    timelines: &mut Timelines,
    seqs: &mut HashMap<(UserId, Uuid, Option<String>), u64>,
    seq: u64,
    subject: &str,
    payload: &[u8],
) {
    let Ok(record) = serde_json::from_slice::<Timeline>(payload) else {
        tracing::debug!(seq, subject, "skipping a timeline record that does not parse");
        return;
    };
    let calls: Vec<_> = record
        .entries
        .iter()
        .filter_map(|e| match e {
            ants_proto::TimelineEntry::Tool(call) => Some((record.ant.clone(), record.message, Some(call.id.clone()))),
            ants_proto::TimelineEntry::NoReply { .. } => Some((record.ant.clone(), record.message, None)),
            _ => None,
        })
        .collect();
    if timelines.add(subject, record) {
        seqs.extend(calls.into_iter().map(|key| (key, seq)));
    } else {
        tracing::debug!(seq, subject, "skipping a timeline record its subject does not name");
    }
}

/// When the last message stored on `thread` was, reading from the thread's start when its id is time-ordered, else
/// from all the stream keeps; None when none is left.
pub async fn last_activity(js: &jetstream::Context, thread: &ThreadId) -> Result<Option<DateTime<Utc>>> {
    let mut last = None;
    scan(js, show_start(thread, None), |entry| {
        if entry.thread_id == *thread {
            last = Some(entry.stored_at);
        }
    })
    .await?;
    Ok(last)
}

/// `ants thread show THREAD`: every message stored on the thread `pick` picks, in the order the stream stored them,
/// the prompts its ants ended with no reply sent, and unless `no_tools` the tool calls its ants made.
pub async fn show(nats: &Nats<'_>, pick: Pick, since: Option<Duration>, json: bool, no_tools: bool) -> Result<()> {
    let auto_archive = crate::defaults::auto_archive()?;
    let js = connect(nats).await?;
    let thread = &pick.resolve(nats, &js).await?;
    let keyring = sealed::Keyring::load(nats, &js).await.map(Arc::new);
    let behalf = Arc::new(crate::behalf::Checker::new(nats, &js));
    let parser = Parser::opening(keyring).checking(Some(behalf.clone()));
    let activity = if no_tools { Activity::NoReplies } else { Activity::All };
    let entries = history_with(&js, thread, since, activity, parser).await?;
    let sealed = entries.iter().any(|e| e.sealed.is_some());
    let suggested = entries.iter().find_map(|e| Some((e.from.as_ref()?, e.suggested_title.as_deref()?)));
    let suggested = suggested.filter(|_| !sealed);
    let thread_meta::About { title, mark, done } = if json {
        thread_meta::About::default()
    } else {
        let messages: Vec<Entry> = entries.iter().filter(|e| !e.kind.on_timeline()).cloned().collect();
        thread_meta::of_or_none(nats, &js, thread, &messages, suggested, auto_archive).await
    };
    if json {
        for entry in &entries {
            println!("{}", serde_json::to_string(entry)?);
        }
    } else if entries.is_empty() {
        println!("no messages on thread {thread}");
    } else {
        let owner = entries.iter().find_map(|e| e.owner.as_ref());
        match owner {
            Some(owner) => println!("thread {thread}, owned by {owner}"),
            None => println!("thread {thread}"),
        }
        if let Some(title) = title {
            let by_ant = entries.iter().any(|e| e.suggested_title.is_some() && e.from.as_ref() == Some(&title.by));
            let acting = behalf.check(&title.by, title.on_behalf_of.as_ref()).await;
            let suggested = if by_ant {
                format!(" (suggested by {})", title.by)
            } else if !acting.is_own() {
                format!(" (set by {})", acting.label(title.by.as_str()))
            } else {
                String::new()
            };
            println!("title: {}{suggested}", title.title);
        }
        if let Some(done) = done {
            let by = behalf.check(&done.by, done.on_behalf_of.as_ref()).await.label(done.by.as_str());
            println!("done by {by} {}", time(done.at));
        }
        match mark {
            Some(Mark::Pinned) => println!("pinned"),
            Some(Mark::Archived { new_activity: true }) => println!("archived, with messages since"),
            Some(Mark::Archived { new_activity: false }) => println!("archived"),
            Some(Mark::Idle) => {
                let idle = ants_proto::duration::format(auto_archive.unwrap_or_default());
                println!("auto-archived: no message for {idle} (`ants thread unarchive` keeps it)");
            }
            None => {}
        }
        print!("{}", render(&entries));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
