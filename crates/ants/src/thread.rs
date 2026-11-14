//! `ants thread`: the threads in the ANTS stream, across inboxes (both ways), channel posts, notes, escalations, tool
//! requests and their decisions, and dead letters.
//!
//! Read back by a human by message gets (batched direct gets, [`Reading::Gets`]), so nothing is consumed or acked
//! and no consumer is left behind. Only notes subjects name their thread, so a read goes through every message stored
//! in its time window: `--since` bounds it, and `ants thread show` of a thread with a time-ordered id starts at the
//! thread's start.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_daemon::channel::CLOCK_SLACK;
use ants_daemon::reading::{self, Reading};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, Envelope, EscalationReason, Humans,
    NotesPost, ThreadId, UserId, Uuid, subject,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::LastRawMessageErrorKind;
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::tools;

/// How long the opening message's line in `ants thread list` may be.
const OPENING: usize = 72;
/// How far back a start of a thread id looks for the threads of the CLI's user.
pub const RECENT: Duration = Duration::from_hours(7 * 24);
/// How long after its time-ordered id is made a thread's first message may be stored, clocks' skew included, for a start
/// of an id matched among threads of any user.
const LATE: Duration = Duration::from_mins(10);
/// How many threads an error on a start of a thread id lists.
const LISTED: usize = 10;

/// What a stored message on a thread is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// An inbox message, between ants or between an ant and a human.
    Message,
    /// An inbox message carrying an escalation.
    Escalation,
    /// An inbox message telling a human member of a channel post that @mentioned it.
    Mention,
    /// A channel post.
    Post,
    /// A post to the thread's notes.
    Note,
    /// A tool request an ant escalated to the humans.
    Approval,
    /// A decision on a tool request: a human's, or an agent's of the requester's chain.
    Decision,
    /// A message an ant gave up on.
    Dead,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Escalation => "escalation",
            Self::Mention => "mention",
            Self::Post => "post",
            Self::Note => "note",
            Self::Approval => "approval",
            Self::Decision => "decision",
            Self::Dead => "dead",
        }
    }
}

/// One stored message on a thread, as `ants thread show` prints it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    pub seq: u64,
    /// When the stream stored it.
    pub stored_at: DateTime<Utc>,
    pub kind: Kind,
    pub thread_id: ThreadId,
    /// The sender; none for a human's decision on a tool request, which does not say which human decided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<UserId>,
    /// The recipient of an inbox message, the ant a decision or dead letter is for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<UserId>,
    /// The channel of a post or of the post a mention tells of.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    /// The thread's owner as the message names it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// What the kind adds: an escalation's level and chain, a tool request's id, a dead letter's error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub text: String,
    /// The subject it was stored on names another sender (or ant) than the message: the message is forged.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub forged: bool,
    /// The reason of the escalation an inbox message carries, which the note spells out.
    #[serde(skip)]
    pub reason: Option<EscalationReason>,
}

impl Entry {
    fn new(seq: u64, stored_at: DateTime<Utc>, kind: Kind, thread_id: ThreadId) -> Self {
        Self {
            seq,
            stored_at,
            kind,
            thread_id,
            from: None,
            to: None,
            channel: None,
            owner: None,
            note: None,
            text: String::new(),
            forged: false,
            reason: None,
        }
    }

    /// Who sent it to whom: `alice -> dev-a`, `dev-a -> #ops`, `dev-a -> notes`, `(a human) -> dev-a`.
    fn route(&self) -> String {
        let from = self.from.as_ref().map_or("(a human)", UserId::as_str);
        match (self.kind, &self.to, &self.channel) {
            (Kind::Post, _, Some(channel)) => format!("{from} -> #{channel}"),
            (Kind::Note, ..) => format!("{from} -> notes"),
            (Kind::Approval, ..) => format!("{from} -> humans"),
            (_, Some(to), _) => format!("{from} -> {to}"),
            (_, None, _) => from.to_owned(),
        }
    }

    /// The users it was between.
    fn users(&self) -> impl Iterator<Item = &UserId> {
        self.from.iter().chain(&self.to)
    }
}

/// Turns stored messages into [`Entry`]s, in stream order. A human's decision does not name its request's thread, so
/// the threads of the requests read so far are kept to place it.
#[derive(Default)]
pub struct Parser {
    requests: HashMap<(UserId, Uuid), ThreadId>,
}

impl Parser {
    /// The entry of the message stored at `seq` on `subject`, None for one that is not ants' or does not parse.
    pub fn entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        if subject::parse_inbox(subject).is_some() {
            let e: Envelope = serde_json::from_slice(payload).ok()?;
            let forged = e.check_subject(subject).is_err();
            let (kind, note, text) = if let Some(d) = &e.approval {
                let text = format!("{} on {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
                (Kind::Decision, None, text)
            } else if let Some(x) = &e.escalation {
                (Kind::Escalation, Some(crate::escalation_note(x)), e.text)
            } else if e.mention.is_some() {
                (Kind::Mention, None, e.text)
            } else {
                (Kind::Message, None, e.text)
            };
            let channel = e.mention.map(|m| m.channel);
            return Some(Entry {
                from: Some(e.from),
                to: Some(e.to),
                channel,
                owner: e.owner,
                note,
                text,
                forged,
                reason: e.escalation.map(|x| x.reason),
                ..entry(kind, e.thread_id)
            });
        }
        if subject::parse_chan(subject).is_some() {
            let post: ChannelPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            return Some(Entry {
                from: Some(post.from),
                channel: Some(post.channel),
                owner: post.owner,
                note,
                text: post.text,
                forged,
                ..entry(Kind::Post, post.thread_id)
            });
        }
        if subject::parse_notes(subject).is_some() {
            let post: NotesPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            return Some(Entry {
                from: Some(post.from),
                note,
                text: post.text,
                forged,
                ..entry(Kind::Note, post.thread_id)
            });
        }
        if let Some(rest) = subject.strip_prefix("ants.approvals.") {
            if rest.contains('.') {
                let d: ApprovalDecision = serde_json::from_slice(payload).ok()?;
                let thread = self.requests.get(&(d.ant.clone(), d.request_id))?.clone();
                let forged = d.subject() != subject;
                let text = format!("{} on {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
                return Some(Entry { to: Some(d.ant), text, forged, ..entry(Kind::Decision, thread) });
            }
            let r: ApprovalRequest = serde_json::from_slice(payload).ok()?;
            let forged = r.check_subject(subject).is_err();
            if !forged {
                self.requests.insert((r.ant.clone(), r.id), r.thread_id.clone());
            }
            let options = r.options.iter().map(|v| tools::spelling(*v)).collect::<Vec<_>>().join("|");
            let note = Some(format!("{}/{} ({options})", r.ant, r.id));
            let text = format!("{}: {}", r.tool.kind, r.tool.title);
            return Some(Entry { from: Some(r.ant), note, text, forged, ..entry(Kind::Approval, r.thread_id) });
        }
        if let Some(ant) = subject.strip_prefix("ants.dead.") {
            let d: DeadLetter = serde_json::from_slice(payload).ok()?;
            let forged = d.ant.as_str() != ant;
            let note = Some(format!("after {} deliveries: {}", d.deliveries, d.error));
            let e = d.envelope?;
            return Some(Entry {
                from: Some(e.from),
                to: Some(d.ant),
                owner: e.owner,
                note,
                text: e.text,
                forged,
                ..entry(Kind::Dead, e.thread_id)
            });
        }
        None
    }
}

/// One thread as `ants thread list` prints it, from the messages read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub thread_id: ThreadId,
    /// The thread's owner, as its first message naming one says.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// Every ant and human that sent or received a message on it.
    pub users: BTreeSet<UserId>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub channels: BTreeSet<ChannelId>,
    pub first_at: DateTime<Utc>,
    pub last_at: DateTime<Utc>,
    pub messages: usize,
    /// The first line of the first message read.
    pub opening: String,
    /// The first line matching `--grep`, after its sender.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched: Option<String>,
    /// With `--escalated`, the escalation that reached a human and no human's message followed: when it first did,
    /// to whom, and the escalation's note.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub escalated: Option<String>,
}

/// What `ants thread list` keeps of the threads read.
#[derive(Debug, Default, Clone)]
pub struct Select {
    /// Only threads this user sent or received a message on.
    pub ant: Option<UserId>,
    /// Only threads this human owns.
    pub owner: Option<UserId>,
    /// Only threads with a message whose text has this, whatever the case.
    pub grep: Option<String>,
    /// Only the most recently active threads, this many.
    pub limit: Option<usize>,
    /// Only threads with an open escalation ([`Threads::escalated`]).
    pub escalated: bool,
}

/// The threads of the entries handed to [`Threads::add`].
#[derive(Debug, Default)]
pub struct Threads {
    by_id: HashMap<ThreadId, Summary>,
    /// `--grep`'s text, lowercased.
    grep: Option<String>,
    /// With `--escalated`, the team's humans.
    humans: Option<Humans>,
}

impl Threads {
    pub fn new(grep: Option<&str>) -> Self {
        Self { by_id: HashMap::new(), grep: grep.map(str::to_lowercase), humans: None }
    }

    /// Keep each thread's open escalation: the first of the escalations reaching one of `humans` since a human's
    /// last message or post on the thread, other than a tool request's. A human's message on the thread ends it, as
    /// it ends the escalation in the daemon.
    pub fn escalated(self, humans: Humans) -> Self {
        Self { humans: Some(humans), ..self }
    }

    pub fn add(&mut self, entry: &Entry) {
        let summary = self.by_id.entry(entry.thread_id.clone()).or_insert_with(|| Summary {
            thread_id: entry.thread_id.clone(),
            owner: None,
            users: BTreeSet::new(),
            channels: BTreeSet::new(),
            first_at: entry.stored_at,
            last_at: entry.stored_at,
            messages: 0,
            opening: crate::excerpt(&entry.text, OPENING),
            matched: None,
            escalated: None,
        });
        summary.messages += 1;
        summary.first_at = summary.first_at.min(entry.stored_at);
        summary.last_at = summary.last_at.max(entry.stored_at);
        summary.users.extend(entry.users().cloned());
        summary.channels.extend(entry.channel.iter().cloned());
        if summary.owner.is_none() {
            summary.owner.clone_from(&entry.owner);
        }
        if let (Some(grep), None) = (&self.grep, &summary.matched) {
            summary.matched = entry.text.lines().find(|line| line.to_lowercase().contains(grep.as_str())).map(|line| {
                let from = entry.from.as_ref().map_or("(a human)", UserId::as_str);
                format!("{from}: {}", crate::excerpt(line.trim(), OPENING))
            });
        }
        if let Some(humans) = self.humans.as_ref().filter(|_| !entry.forged) {
            let human = |user: &Option<UserId>| user.as_ref().is_some_and(|u| humans.contains(u));
            match entry.kind {
                Kind::Escalation if summary.escalated.is_none() && human(&entry.to) => {
                    if entry.reason.is_some_and(|r| r != EscalationReason::Approval) {
                        let note = entry.note.as_deref().unwrap_or_default();
                        summary.escalated = Some(format!("{} {}: {note}", time(entry.stored_at), entry.route()));
                    }
                }
                Kind::Message | Kind::Post if human(&entry.from) => summary.escalated = None,
                _ => {}
            }
        }
    }

    /// The threads `select` keeps, least recently active first, and how many more it left out for its limit.
    pub fn finish(self, select: &Select) -> (Vec<Summary>, usize) {
        let mut threads: Vec<Summary> = self
            .by_id
            .into_values()
            .filter(|t| select.ant.as_ref().is_none_or(|ant| t.users.contains(ant)))
            .filter(|t| select.owner.is_none() || t.owner == select.owner)
            .filter(|t| select.grep.is_none() || t.matched.is_some())
            .filter(|t| !select.escalated || t.escalated.is_some())
            .collect();
        threads.sort_by(|a, b| (a.last_at, &a.thread_id).cmp(&(b.last_at, &b.thread_id)));
        let omitted = select.limit.map_or(0, |limit| threads.len().saturating_sub(limit));
        threads.drain(..omitted);
        (threads, omitted)
    }
}

fn time(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// The threads as a table: one row per thread under a header, columns padded to line up, and under a thread
/// matching `--grep` its matching line, under one with an open escalation that escalation.
pub fn table(threads: &[Summary]) -> String {
    let header = ["THREAD", "OWNER", "USERS", "FIRST", "LAST", "MSGS", "OPENING"].map(str::to_owned);
    let rows: Vec<[String; 7]> = std::iter::once(header)
        .chain(threads.iter().map(|t| {
            let users = t.users.iter().map(UserId::as_str);
            let channels = t.channels.iter().map(|c| format!("#{c}"));
            [
                t.thread_id.to_string(),
                t.owner.as_ref().map_or("-", UserId::as_str).to_owned(),
                users.map(str::to_owned).chain(channels).collect::<Vec<_>>().join(","),
                time(t.first_at),
                time(t.last_at),
                t.messages.to_string(),
                t.opening.clone(),
            ]
        }))
        .collect();
    let mut widths = [0; 7];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut lines = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let cells: Vec<String> = row.iter().zip(widths).map(|(cell, width)| format!("{cell:width$}")).collect();
        lines.push(cells.join("  ").trim_end().to_owned());
        if let Some(thread) = i.checked_sub(1).map(|i| &threads[i]) {
            for line in thread.matched.iter().chain(&thread.escalated) {
                lines.push(format!("    {line}"));
            }
        }
    }
    lines.join("\n")
}

/// The entries of a thread as `ants thread show` prints them: time, kind, route, note and text, one entry per line,
/// its text's further lines indented under it.
pub fn render(entries: &[Entry]) -> String {
    let mut out = String::new();
    for e in entries {
        let forged = if e.forged { " (forged)" } else { "" };
        let note = e.note.as_ref().map(|n| format!(" [{n}]")).unwrap_or_default();
        let mut lines = e.text.lines();
        let first = lines.next().unwrap_or("");
        let kind = e.kind.as_str();
        let _ = writeln!(out, "{}  {kind:<10}  {}{forged}{note}: {first}", time(e.stored_at), e.route());
        for line in lines {
            let _ = writeln!(out, "    {line}");
        }
    }
    out
}

/// Where a read of the last `since` starts: everything kept for zero.
fn window(since: Duration) -> consumer::DeliverPolicy {
    match SystemTime::now().checked_sub(since) {
        Some(start) if !since.is_zero() => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        _ => consumer::DeliverPolicy::All,
    }
}

/// Where `ants thread show` reads `thread` from: as `since` says, else the thread's start when its id is
/// time-ordered, else everything kept.
fn show_start(thread: &ThreadId, since: Option<Duration>) -> consumer::DeliverPolicy {
    match (since, ants_daemon::channel::thread_start(thread)) {
        (Some(since), _) => window(since),
        (None, Some(start)) => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        (None, None) => consumer::DeliverPolicy::All,
    }
}

/// Hand `take` the entry of every message stored from `start` on, oldest first.
pub async fn scan(js: &jetstream::Context, start: consumer::DeliverPolicy, mut take: impl FnMut(Entry)) -> Result<()> {
    let mut parser = Parser::default();
    reading::read_back(js, "", Reading::Gets, subject::ALL, start, |seq, stored, subject, payload| {
        match parser.entry(seq, stored.into(), subject, payload) {
            Some(entry) => take(entry),
            None => tracing::debug!(seq, subject, "skipping a message on no thread"),
        }
        ControlFlow::Continue(())
    })
    .await
}

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
    /// The thread this picks for `me`, reading the stream with `js` unless given a whole thread id. Only a human's
    /// credentials get stream messages by sequence, so only a human picks by a start of an id or `last`.
    pub async fn resolve(self, js: &jetstream::Context, me: &crate::humans::Me) -> Result<ThreadId> {
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
        if let Some(thread) = select(&own, &prefix, &me.id)? {
            return Ok(thread);
        }
        select_any(&any_recent(js, &prefix).await?, &own, &prefix, &me.id)
    }
}

/// [`Pick::resolve`] for an optional pick.
pub async fn resolve(pick: Option<Pick>, js: &jetstream::Context, me: &crate::humans::Me) -> Result<Option<ThreadId>> {
    match pick {
        Some(pick) => Ok(Some(pick.resolve(js, me).await?)),
        None => Ok(None),
    }
}

/// The subjects of the messages `user` took part in: its inbox both ways, its channel posts and its notes posts.
fn own_subjects(user: &UserId) -> [String; 4] {
    [subject::inbox_of(user), subject::inbox_from(user), subject::chan_from(user), subject::notes_from(user)]
}

/// The thread of the latest message to or from `user` ([`own_subjects`]), however old.
async fn last(js: &jetstream::Context, user: &UserId) -> Result<ThreadId> {
    let stream = js.get_stream(ants_proto::STREAM).await.context("opening the ANTS stream")?;
    let mut parser = Parser::default();
    let mut latest: Option<Entry> = None;
    for filter in own_subjects(user) {
        let msg = match stream.get_last_raw_message_by_subject(&filter).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => continue,
            Err(e) => return Err(e).with_context(|| format!("reading the last message on {filter}")),
        };
        let stored = DateTime::<Utc>::from(SystemTime::from(msg.time));
        let entry = parser.entry(msg.sequence, stored, &msg.subject, &msg.payload).filter(|e| !e.forged);
        if let Some(entry) = entry.filter(|e| latest.as_ref().is_none_or(|l| l.seq < e.seq)) {
            latest = Some(entry);
        }
    }
    latest.map(|e| e.thread_id).ok_or_else(|| anyhow!("no message to or from {user} in the stream, so no last thread"))
}

/// The latest message of each thread `user` took part in ([`own_subjects`]) stored from `start` on, most recent first.
async fn recent(js: &jetstream::Context, user: &UserId, start: consumer::DeliverPolicy) -> Result<Vec<Entry>> {
    let mut parser = Parser::default();
    let mut latest: HashMap<ThreadId, Entry> = HashMap::new();
    for filter in own_subjects(user) {
        reading::read_back(js, "", Reading::Gets, &filter, start, |seq, stored, subject, payload| {
            if let Some(entry) = parser.entry(seq, stored.into(), subject, payload).filter(|e| !e.forged) {
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
/// part in it, most recent first. For a start of a time-ordered id, only the messages stored while such an id was
/// made, give or take the clocks' skew ([`CLOCK_SLACK`]) and up to [`LATE`] after, are read: a thread's first message
/// is.
async fn any_recent(js: &jetstream::Context, prefix: &str) -> Result<Vec<Entry>> {
    let now = SystemTime::now();
    let floor = now.checked_sub(RECENT).unwrap_or(SystemTime::UNIX_EPOCH);
    let (from, to) = match made_within(prefix) {
        Some((first, last)) => (first.checked_sub(CLOCK_SLACK).unwrap_or(first).max(floor), (last + LATE).min(now)),
        None => (floor, now),
    };
    if from > to {
        return Ok(Vec::new());
    }
    let mut threads: HashMap<ThreadId, Entry> = HashMap::new();
    let mut parser = Parser::default();
    let start = consumer::DeliverPolicy::ByStartTime { start_time: from.into() };
    reading::read_back(js, "", Reading::Gets, subject::ALL, start, |seq, stored, subject, payload| {
        if stored > to {
            return ControlFlow::Break(());
        }
        let entry = parser.entry(seq, stored.into(), subject, payload);
        if let Some(entry) = entry.filter(|e| !e.forged && e.thread_id.as_str().starts_with(prefix)) {
            threads.insert(entry.thread_id.clone(), entry);
        }
        ControlFlow::Continue(())
    })
    .await?;
    let mut threads: Vec<Entry> = threads.into_values().collect();
    threads.sort_by_key(|e| std::cmp::Reverse(e.seq));
    Ok(threads)
}

/// The span in which a time-ordered (version 7 UUID) thread id starting with `prefix` was made, from the time its
/// first 12 hex digits carry; None when no such id starts so, or the start leaves the time open.
fn made_within(prefix: &str) -> Option<(SystemTime, SystemTime)> {
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
fn matching<'a>(threads: &'a [Entry], prefix: &str) -> Vec<&'a Entry> {
    match threads.iter().find(|e| e.thread_id.as_str() == prefix) {
        Some(exact) => vec![exact],
        None => threads.iter().filter(|e| e.thread_id.as_str().starts_with(prefix)).collect(),
    }
}

/// The one thread among `threads` `user` took part in (the latest message of each, most recent first) whose id is
/// `prefix`, else starts with it; None when none does. The error on several lists them.
fn select(threads: &[Entry], prefix: &str, user: &UserId) -> Result<Option<ThreadId>> {
    let found = matching(threads, prefix);
    match found.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(one.thread_id.clone())),
        _ => bail!(
            "{} threads {user} took part in over the last {} days start with {prefix}; give more of the id:\n{}",
            found.len(),
            RECENT.as_secs() / 86_400,
            listing(found.iter().copied())
        ),
    }
}

/// The one thread among `threads` (any user's, the latest message of each, most recent first) whose id is `prefix`,
/// else starts with it, for a `user` who took part in none of them. The error on none lists `own`, the threads `user`
/// took part in, and the one on several lists those found.
fn select_any(threads: &[Entry], own: &[Entry], prefix: &str, user: &UserId) -> Result<ThreadId> {
    let found = matching(threads, prefix);
    let days = RECENT.as_secs() / 86_400;
    match found.as_slice() {
        [one] => Ok(one.thread_id.clone()),
        [] if own.is_empty() => {
            bail!("no thread over the last {days} days starts with {prefix}: give the whole thread id")
        }
        [] => bail!(
            "no thread over the last {days} days starts with {prefix}; give the whole id of an older one, or pick one \
             of the most recent {user} took part in:\n{}",
            listing(own.iter())
        ),
        _ => bail!(
            "{} threads over the last {days} days start with {prefix}, none of them with {user}; give more of the id:\n{}",
            found.len(),
            listing(found.iter().copied())
        ),
    }
}

/// Threads as an error lists them, one per line: id, time and route of the latest message, and its first line.
fn listing<'a>(threads: impl ExactSizeIterator<Item = &'a Entry>) -> String {
    let more = threads.len().saturating_sub(LISTED);
    let mut lines: Vec<String> = threads
        .take(LISTED)
        .map(|e| format!("  {}  {}  {}: {}", e.thread_id, time(e.stored_at), e.route(), crate::excerpt(&e.text, 48)))
        .collect();
    if more > 0 {
        lines.push(format!("  and {more} more"));
    }
    lines.join("\n")
}

/// Connect as the CLI's human: only a human's credentials get stream messages by sequence.
async fn connect(nats: &crate::Nats<'_>) -> Result<jetstream::Context> {
    if !nats.me.role.is_human() {
        bail!(
            "the credentials of {} are an ant's, which read no other inbox; `ants thread` reads them all with a \
             human's",
            nats.me.id
        );
    }
    Ok(nats.connect("ants-thread").await?.1)
}

/// `ants thread list`: the threads with messages stored within `since`, as `select` keeps them.
pub async fn list(nats: &crate::Nats<'_>, since: Duration, select: &Select, json: bool) -> Result<()> {
    let js = connect(nats).await?;
    let mut threads = Threads::new(select.grep.as_deref());
    if select.escalated {
        threads = threads.escalated(crate::humans::load(&js, &nats.me).await);
    }
    scan(&js, window(since), |entry| threads.add(&entry)).await?;
    let (threads, omitted) = threads.finish(select);
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

/// `ants thread show THREAD`: every message stored on the thread `pick` picks, in the order the stream stored them.
pub async fn show(nats: &crate::Nats<'_>, pick: Pick, since: Option<Duration>, json: bool) -> Result<()> {
    let js = connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let mut entries = Vec::new();
    scan(&js, show_start(thread, since), |entry| {
        if entry.thread_id == *thread {
            entries.push(entry);
        }
    })
    .await?;
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
        print!("{}", render(&entries));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use ants_proto::{ApprovalRequest, Escalation, EscalationReason, ToolCall, Verdict};

    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn t(s: &str) -> ThreadId {
        s.parse().unwrap()
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    fn json<T: Serialize>(value: &T) -> Vec<u8> {
        serde_json::to_vec(value).unwrap()
    }

    /// The entries of `messages`, each a subject and a payload, stored a second apart.
    fn entries(messages: &[(String, Vec<u8>)]) -> Vec<Entry> {
        let mut parser = Parser::default();
        let parsed = messages.iter().enumerate();
        parsed
            .filter_map(|(i, (subject, payload))| parser.entry(i as u64 + 1, at(i as i64), subject, payload))
            .collect()
    }

    #[test]
    fn every_kind_of_stored_message_lands_on_its_thread() {
        let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
        let ask = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "fix it").with_owner(Some(alice));
        let reply = ask.reply(dev_a.clone(), "done\nsee the diff", ants_proto::Role::Human).unwrap();
        let post = ChannelPost::on_thread("ops".parse().unwrap(), t("t1"), dev_b.clone(), "@dev-a nice");
        let note = NotesPost::new(t("t1"), dev_a.clone(), "plan");
        let escalation = Escalation::new(dev_a.clone(), t("t1"), EscalationReason::TurnBudget);
        let up = Envelope::on_thread(t("t1"), dev_a.clone(), dev_b, "help").with_escalation(Some(escalation));
        let request = ApprovalRequest::new(
            dev_a.clone(),
            t("t1"),
            ToolCall { kind: "execute".into(), title: "rm -rf build".into(), ..Default::default() },
            vec![Verdict::AllowOnce],
            Duration::from_secs(60),
        );
        let decision = request.decide(Verdict::AllowOnce);
        let dead = DeadLetter::of_envelope(dev_a.clone(), 3, 5, "harness died", ask.clone());
        let unknown = ApprovalDecision::new(Uuid::now_v7(), dev_a, Verdict::Reject);
        let messages = [
            (ask.subject(), json(&ask)),
            (reply.subject(), json(&reply)),
            (post.subject(), json(&post)),
            (note.subject(), json(&note)),
            (up.subject(), json(&up)),
            (request.subject(), json(&request)),
            (decision.subject(), json(&decision)),
            (dead.subject(), json(&dead)),
            // A decision on a request not read, and a payload of no kind, are on no thread known.
            (unknown.subject(), json(&unknown)),
            ("ants.inbox.dev-a.alice".to_owned(), b"not json".to_vec()),
        ];
        let entries = entries(&messages);
        let kinds: Vec<Kind> = entries.iter().map(|e| e.kind).collect();
        let expected = [
            Kind::Message,
            Kind::Message,
            Kind::Post,
            Kind::Note,
            Kind::Escalation,
            Kind::Approval,
            Kind::Decision,
            Kind::Dead,
        ];
        assert_eq!(kinds, expected);
        assert!(entries.iter().all(|e| e.thread_id == t("t1") && !e.forged), "{entries:?}");
        assert_eq!(entries[1].route(), "dev-a -> alice");
        assert_eq!(entries[2].route(), "dev-b -> #ops");
        assert_eq!(entries[3].route(), "dev-a -> notes");
        assert!(entries[4].note.as_deref().unwrap().contains("turn_budget"), "{:?}", entries[4]);
        assert_eq!(entries[6].route(), "(a human) -> dev-a");
        assert!(entries[6].text.starts_with("allow-once on dev-a/"), "{}", entries[6].text);
        assert_eq!(entries[7].note.as_deref(), Some("after 5 deliveries: harness died"));

        let shown = render(&entries[..2]);
        assert!(shown.contains("message     alice -> dev-a: fix it\n"), "{shown}");
        assert!(shown.ends_with("dev-a -> alice: done\n    see the diff\n"), "{shown}");
    }

    #[test]
    fn a_message_on_another_sender_s_subject_is_marked_forged() {
        let e = Envelope::on_thread(t("t1"), p("alice"), p("dev-a"), "hi");
        let entries = entries(&[("ants.inbox.dev-a.mallory".to_owned(), json(&e))]);
        assert!(entries[0].forged);
        assert!(render(&entries).contains("alice -> dev-a (forged): hi"));
    }

    #[test]
    fn threads_sum_up_their_users_owner_times_and_opening() {
        let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
        let one = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "first line\nmore");
        let two = Envelope::on_thread(t("t1"), dev_a, dev_b.clone(), "Deploy failed").with_owner(Some(alice));
        let other = ChannelPost::on_thread("ops".parse().unwrap(), t("t2"), dev_b.clone(), "unrelated");
        let three = two.reply(dev_b, "retrying", ants_proto::Role::Ant).unwrap();
        let messages = [
            (one.subject(), json(&one)),
            (two.subject(), json(&two)),
            (other.subject(), json(&other)),
            (three.subject(), json(&three)),
        ];
        let read = entries(&messages);
        let threads = |select: &Select| {
            let mut threads = Threads::new(select.grep.as_deref());
            read.iter().for_each(|e| threads.add(e));
            threads.finish(select)
        };

        let (all, omitted) = threads(&Select::default());
        assert_eq!(omitted, 0);
        let ids: Vec<&str> = all.iter().map(|t| t.thread_id.as_str()).collect();
        assert_eq!(ids, ["t2", "t1"], "least recently active first");
        let t1 = &all[1];
        assert_eq!(t1.owner, Some(p("alice")));
        assert_eq!(t1.users.iter().map(UserId::as_str).collect::<Vec<_>>(), ["alice", "dev-a", "dev-b"]);
        assert_eq!((t1.first_at, t1.last_at, t1.messages), (at(0), at(3), 3));
        assert_eq!(t1.opening, "first line…");
        assert_eq!(all[0].channels.iter().map(ChannelId::as_str).collect::<Vec<_>>(), ["ops"]);

        let ids = |(threads, _): (Vec<Summary>, usize)| -> Vec<String> {
            threads.into_iter().map(|t| t.thread_id.to_string()).collect()
        };
        assert_eq!(ids(threads(&Select { ant: Some(p("alice")), ..Default::default() })), ["t1"]);
        assert_eq!(ids(threads(&Select { ant: Some(p("dev-b")), ..Default::default() })), ["t2", "t1"]);
        assert_eq!(ids(threads(&Select { owner: Some(p("alice")), ..Default::default() })), ["t1"]);
        assert!(ids(threads(&Select { owner: Some(p("bob")), ..Default::default() })).is_empty());
        let (limited, omitted) = threads(&Select { limit: Some(1), ..Default::default() });
        assert_eq!((limited[0].thread_id.as_str(), omitted), ("t1", 1));

        let (found, _) = threads(&Select { grep: Some("deploy FAILED".into()), ..Default::default() });
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].matched.as_deref(), Some("dev-a: Deploy failed"));
        let table = table(&found);
        assert!(table.starts_with("THREAD  OWNER  USERS"), "{table}");
        assert!(table.ends_with("\n    dev-a: Deploy failed"), "{table}");
    }

    #[test]
    fn an_escalation_reaching_a_human_stays_open_until_a_human_s_message_on_the_thread() {
        let (alice, dev_a, lead) = (p("alice"), p("dev-a"), p("lead"));
        let escalate = |thread: &str, to: &UserId, reason| {
            let escalation = Escalation::new(dev_a.clone(), t(thread), reason);
            Envelope::on_thread(t(thread), dev_a.clone(), to.clone(), "[ants] stuck").with_escalation(Some(escalation))
        };
        let messages: Vec<Envelope> = vec![
            escalate("t1", &alice, EscalationReason::TurnBudget),
            escalate("t2", &UserId::human(), EscalationReason::Approval),
            escalate("t3", &lead, EscalationReason::Cap),
            escalate("t4", &alice, EscalationReason::Asked),
            Envelope::on_thread(t("t4"), alice.clone(), dev_a.clone(), "go on"),
            Envelope::on_thread(t("t1"), dev_a.clone(), alice.clone(), "still here"),
            escalate("t1", &alice, EscalationReason::TurnBudget),
        ];
        let read = entries(&messages.iter().map(|e| (e.subject(), json(e))).collect::<Vec<_>>());
        let mut humans = Humans::default();
        humans.add("alice", ants_proto::HumanEntry::new(alice)).unwrap();
        let mut threads = Threads::new(None).escalated(humans);
        read.iter().for_each(|e| threads.add(e));
        let (open, _) = threads.finish(&Select { escalated: true, ..Default::default() });

        assert_eq!(open.iter().map(|t| t.thread_id.as_str()).collect::<Vec<_>>(), ["t1"]);
        let since = open[0].escalated.as_deref().unwrap();
        let first = "2023-11-14 22:13:20 dev-a -> alice: escalation level 0, turn_budget: dev-a stuck on t1";
        assert!(since.starts_with(first), "the first since a human's message: {since}");
        assert!(table(&open).ends_with(&format!("\n    {since}")), "{}", table(&open));
    }

    #[test]
    fn show_starts_at_a_time_ordered_thread_s_start_or_reads_everything() {
        assert_matches!(show_start(&t("t1"), None), consumer::DeliverPolicy::All);
        assert_matches!(show_start(&ThreadId::generate(), None), consumer::DeliverPolicy::ByStartTime { .. });
        assert_matches!(
            show_start(&t("t1"), Some(Duration::from_secs(60))),
            consumer::DeliverPolicy::ByStartTime { .. }
        );
        assert_matches!(window(Duration::ZERO), consumer::DeliverPolicy::All);
    }

    #[test]
    fn a_pick_is_a_whole_uuid_a_start_of_an_id_or_last() {
        let id = ThreadId::generate();
        let pick = |s: &str| s.parse::<Pick>();
        assert_eq!(pick(&id.as_str().to_uppercase()).unwrap(), Pick::Id(id.clone()));
        assert_eq!(pick(&id.as_str()[..8]).unwrap(), Pick::Prefix(id.as_str()[..8].to_owned()));
        assert_eq!(pick("t1").unwrap(), Pick::Prefix("t1".to_owned()));
        assert_eq!(pick("LAST").unwrap(), Pick::Last);
        assert!(pick("no_such").is_err());
    }

    #[test]
    fn a_start_of_an_id_picks_one_recent_thread_or_lists_them() {
        let alice = p("alice");
        let entry = |thread: &str, secs| Entry {
            from: Some(alice.clone()),
            to: Some(p("dev-a")),
            text: format!("on {thread}\nmore"),
            ..Entry::new(secs as u64, at(secs), Kind::Message, t(thread))
        };
        // The latest message of each thread, most recent first.
        let threads = [entry("ab12", 3), entry("ab1", 2), entry("cd34", 1)];
        assert_eq!(select(&threads, "ab1", &alice).unwrap(), Some(t("ab1")), "a whole id wins");
        assert_eq!(select(&threads, "c", &alice).unwrap(), Some(t("cd34")));
        assert_eq!(select(&threads, "ef", &alice).unwrap(), None);

        let several = select(&threads, "ab", &alice).unwrap_err().to_string();
        assert!(several.starts_with("2 threads alice took part in over the last 7 days start with ab"), "{several}");
        assert!(several.contains("\n  ab12  2023-11-14 22:13:23  alice -> dev-a: on ab12…\n  ab1  "), "{several}");
        assert!(!several.contains("cd34"), "{several}");

        // Among any user's threads, when none of alice's matches.
        let others = [entry("ef56", 5), entry("ef5", 4)];
        assert_eq!(select_any(&others, &threads, "ef5", &alice).unwrap(), t("ef5"), "a whole id wins");
        let several = select_any(&others, &threads, "ef", &alice).unwrap_err().to_string();
        assert!(several.starts_with("2 threads over the last 7 days start with ef, none of them with alice"));
        assert!(several.contains("\n  ef56 ") && !several.contains("ab12"), "{several}");
        let none = select_any(&[], &threads, "ef", &alice).unwrap_err().to_string();
        assert!(none.contains("starts with ef; give the whole id of an older one"), "{none}");
        assert!(none.contains("\n  ab12 ") && none.contains("\n  cd34 "), "{none}");
        let many: Vec<Entry> = (0..12).map(|i| entry(&format!("t{i}"), i)).collect();
        assert!(select_any(&[], &many, "x", &alice).unwrap_err().to_string().ends_with("\n  and 2 more"));
        assert!(select_any(&[], &[], "ab", &alice).unwrap_err().to_string().contains("give the whole thread id"));
    }

    #[test]
    fn a_start_of_a_time_ordered_id_bounds_when_it_was_made() {
        let id = ThreadId::generate();
        let made = ants_daemon::channel::thread_start(&id).unwrap() + CLOCK_SLACK;
        for len in [1, 8, 9, 11, 13, 20, 36] {
            let (first, last) = made_within(&id.as_str()[..len]).unwrap();
            assert!(first <= made && made < last, "{len}");
        }
        let (first, last) = made_within(&id.as_str()[..8]).unwrap();
        assert_eq!(last.duration_since(first).unwrap(), Duration::from_millis(1 << 16));
        let (first, last) = made_within(&id.as_str()[..13]).unwrap();
        assert_eq!(last.duration_since(first).unwrap(), Duration::from_millis(1));
        let v4 = Uuid::new_v4().to_string();
        assert_eq!(made_within(&v4[..15]), None, "no version 7 UUID");
        assert_eq!(made_within("t1"), None);
        assert_eq!(made_within("0199a3b4c"), None, "no hyphen after 8 digits");
    }

    #[tokio::test]
    async fn a_human_picks_its_threads_by_a_start_of_their_id_or_the_last() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
        let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        ants_daemon::bus::init_stream(&js).await.unwrap();
        let me = crate::humans::Me { id: alice.clone(), role: ants_proto::Role::Human };
        let resolve = |pick: &str| pick.parse::<Pick>().unwrap().resolve(&js, &me);
        assert!(resolve("last").await.unwrap_err().to_string().contains("no message to or from alice"));

        let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
        let posted = ChannelPost::new("ops".parse().unwrap(), alice.clone(), "deploy?");
        let others = Envelope::new(dev_a.clone(), dev_b.clone(), "between ants");
        let reply = asked.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
        // Two threads between ants made in the same millisecond a minute ago, so the same 13 characters start them.
        let millis = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - 60_000;
        let made = |tail: u128| t(&Uuid::from_u128((millis << 80) | (0x7 << 76) | (0x8 << 60) | tail).to_string());
        let twins =
            [0x1_0000, 0x2_0000].map(|tail| Envelope::on_thread(made(tail), dev_b.clone(), dev_a.clone(), "twin"));
        for (subject, payload) in [
            (asked.subject(), json(&asked)),
            (posted.subject(), json(&posted)),
            (reply.subject(), json(&reply)),
            (others.subject(), json(&others)),
            (twins[0].subject(), json(&twins[0])),
            (twins[1].subject(), json(&twins[1])),
        ] {
            js.publish(subject, payload.into()).await.unwrap().await.unwrap();
        }

        assert_eq!(resolve("last").await.unwrap(), asked.thread_id, "the reply is the latest of alice's");
        let start = |thread: &ThreadId| thread.as_str()[..thread.as_str().len() - 4].to_owned();
        assert_eq!(resolve(&start(&posted.thread_id)).await.unwrap(), posted.thread_id);
        assert_eq!(resolve(&start(&others.thread_id)).await.unwrap(), others.thread_id, "a thread between ants");
        assert_eq!(resolve(&start(&twins[1].thread_id)).await.unwrap(), twins[1].thread_id);
        let twin = resolve(&twins[0].thread_id.as_str()[..13]).await.unwrap_err().to_string();
        assert!(twin.starts_with("2 threads over the last 7 days start with"), "{twin}");
        assert!(twin.contains(&format!("\n  {} ", twins[1].thread_id)), "{twin}");
        let unknown = resolve("ffff").await.unwrap_err().to_string();
        assert!(unknown.contains("no thread over the last 7 days starts with ffff"), "{unknown}");
        assert!(unknown.contains(&format!("\n  {} ", asked.thread_id)), "{unknown}");
        assert!(unknown.contains(&format!("\n  {} ", posted.thread_id)), "{unknown}");
        assert!(!unknown.contains(&format!("\n  {} ", others.thread_id)), "{unknown}");
        // A whole id needs no read, whoever's thread it is.
        assert_eq!(resolve(others.thread_id.as_str()).await.unwrap(), others.thread_id);

        let ant = crate::humans::Me { id: dev_a, role: ants_proto::Role::Ant };
        let refused = Pick::Last.resolve(&js, &ant).await.unwrap_err().to_string();
        assert!(refused.contains("give the whole thread id"), "{refused}");
    }

    #[tokio::test]
    async fn a_human_reads_threads_back_without_consuming_them() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a) = (p("alice"), p("dev-a"));
        let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        ants_daemon::bus::init_stream(&js).await.unwrap();
        let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it").with_owner(Some(alice.clone()));
        let reply = ask.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
        let note = NotesPost::new(ask.thread_id.clone(), alice.clone(), "remember the tests");
        let other = Envelope::new(alice.clone(), dev_a.clone(), "another thing");
        for (subject, payload) in [
            (ask.subject(), json(&ask)),
            (reply.subject(), json(&reply)),
            (note.subject(), json(&note)),
            (other.subject(), json(&other)),
        ] {
            js.publish(subject, payload.into()).await.unwrap().await.unwrap();
        }

        let mut threads = Threads::new(None);
        scan(&js, window(Duration::from_secs(3600)), |e| threads.add(&e)).await.unwrap();
        let (threads, _) = threads.finish(&Select::default());
        assert_eq!(threads.len(), 2);
        let built = threads.iter().find(|t| t.thread_id == ask.thread_id).unwrap();
        assert_eq!((built.messages, built.owner.as_ref()), (3, Some(&alice)));

        let mut shown = Vec::new();
        scan(&js, show_start(&ask.thread_id, None), |e| {
            if e.thread_id == ask.thread_id {
                shown.push(e.kind);
            }
        })
        .await
        .unwrap();
        assert_eq!(shown, [Kind::Message, Kind::Message, Kind::Note]);

        let stream = js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().clone();
        assert_eq!((stream.state.messages, stream.state.consumer_count), (4, 0));
    }
}
