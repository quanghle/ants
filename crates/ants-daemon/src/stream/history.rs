//! A thread's history, read back from the ANTS stream, to rebuild the context of a harness session that lost it.
//!
//! An ant reads its own inbox and the notes through spares of its readers, so as not to get in the way of the reads
//! its daemon makes through the readers themselves, and the messages it sent through its own [`sent_reader`]; an
//! assistant reads them all by direct gets.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::ControlFlow;
use std::pin::Pin;
use std::time::SystemTime;

use ants_proto::{
    ChannelId, Envelope, EscalationReason, MergedCall, MergedEnd, MergedRejection, NotesPost, ThreadId, UserId, Uuid,
    inbox_consumer, notes_consumer, sent_reader, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};

use crate::files::earlier_lines;
use crate::stream::reading::{self, Reading};

pub mod cloned;
mod timeline;

/// The history of a thread as a prompt shows it, read when the prompt needs it and shown within the bytes given.
pub type History = Box<dyn FnOnce(usize) -> Pin<Box<dyn Future<Output = String> + Send>> + Send>;

/// The [`History`] `read` makes for the budget given.
pub fn from_fn<F>(read: impl FnOnce(usize) -> F + Send + 'static) -> History
where
    F: Future<Output = String> + Send + 'static,
{
    Box::new(move |budget| Box::pin(read(budget)))
}

/// Why a session's context is rebuilt from the thread's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rebuild {
    /// The thread's earlier session could not be resumed, so a new one started.
    Lost,
    /// The session outgrew the model's context window, so the prompt runs again in a new one.
    Overflow,
    /// The thread's earlier session ran in another harness, the ant's own or a fallback ([`crate::limits::fallback`]).
    Moved,
    /// The ant is a clone of another, whose session of the thread could not be forked ([`crate::clone`]).
    Cloned,
}

/// Most bytes of history a prompt shows when neither a budget nor the context window is known; older entries are
/// left out.
pub const DEFAULT_BUDGET: usize = 64 * 1024;
/// The share of a budget one entry's text may take; the rest of the text is left out.
const ENTRY_SHARE: usize = 8;
/// Roughly how many bytes of text a token stands for, to size budgets by a context window counted in tokens.
const BYTES_PER_TOKEN: u64 = 4;
/// The share of the context window a budget derived from it takes.
const WINDOW_SHARE: u64 = 12;
/// The share of the context window no budget exceeds.
const WINDOW_CAP: u64 = 4;
/// Most senders the line about the entries left out names.
const SENDERS_NAMED: usize = 8;

/// The bytes of history a rebuilt session is seeded with: `set` when a budget is set, else a twelfth of the context
/// `window` (in tokens) when it is known, else [`DEFAULT_BUDGET`]; never more than a quarter of the window.
pub fn budget(set: Option<usize>, window: Option<u64>) -> usize {
    let bytes =
        |share: u64| window.map(|w| usize::try_from(w.saturating_mul(BYTES_PER_TOKEN) / share).unwrap_or(usize::MAX));
    let budget = set.or_else(|| bytes(WINDOW_SHARE)).unwrap_or(DEFAULT_BUDGET);
    bytes(WINDOW_CAP).map_or(budget, |cap| budget.min(cap))
}

/// One read through an ant's sent reader at a time, of a thread's history, of the messages that keep waiting
/// ([`crate::delivery::unserved`]) or of the ant's timeline: each deletes the consumer a read before left.
pub(crate) static SENT_READS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What a prompt in a session rebuilt for `why` starts with: why, and the thread's history within `budget` bytes, or
/// half of it after a context overflow so that the new session has room to spare.
pub(crate) async fn seed(why: Rebuild, history: Option<History>, budget: usize) -> String {
    let (what, budget) = match why {
        Rebuild::Lost => ("The earlier session of this thread could not be resumed, so this one starts over", budget),
        Rebuild::Overflow => {
            ("The session of this thread outgrew the model's context window, so this one starts over", budget / 2)
        }
        Rebuild::Moved => {
            ("The earlier session of this thread ran in another harness, so this one starts over", budget)
        }
        Rebuild::Cloned => (
            "This ant is a clone of another, whose session of this thread could not be forked, so this one starts over",
            budget,
        ),
    };
    match history {
        Some(history) => {
            format!("[ants] {what}, its context rebuilt from the thread's history.\n\n{}", history(budget).await)
        }
        None => format!("[ants] {what}, without the context of the thread."),
    }
}

/// A thread as an ant's daemon reads its history back: what the ant received on it before the message at stream
/// sequence `before`, what it sent on it, the thread's notes and, in `channel`, the thread's posts (those of others
/// before `before`). In a clone, the copy of the history in its `home` ([`cloned`]) comes first.
pub struct Thread {
    pub js: jetstream::Context,
    pub ant: UserId,
    pub thread: ThreadId,
    pub channel: Option<ChannelId>,
    pub before: u64,
    /// How the daemon reads through the ant's own readers ([`Reading::daemon`]).
    pub reading: Reading,
    pub home: Option<std::path::PathBuf>,
}

impl Thread {
    /// The thread's history, oldest first, the most recent entries within the budget given.
    pub fn history(self) -> History {
        from_fn(move |budget| async move {
            let (entries, failed) = self.read().await;
            let earlier = match &self.home {
                Some(home) => cloned::earlier(home, &self.thread).await.unwrap_or_else(|e| {
                    tracing::warn!(thread = %self.thread, error = format!("{e:#}"), "reading the copied history");
                    None
                }),
                None => None,
            };
            if failed == SOURCES.len() - usize::from(self.channel.is_none()) && earlier.is_none() {
                return "[ants] The thread's history could not be read back, so its earlier context is lost."
                    .to_owned();
            }
            let entries = match earlier {
                Some((earlier, cut)) => cloned::then(earlier, cut, entries.into_values()),
                None => entries.into_values().collect(),
            };
            tracing::info!(thread = %self.thread, entries = entries.len(), budget, "read back the thread's history");
            let history = format(entries, budget);
            match failed {
                0 => history,
                _ => format!("[ants] Part of the thread's history could not be read back.\n{history}"),
            }
        })
    }

    /// The entries, by stream sequence, and how many sources failed to read.
    async fn read(&self) -> (BTreeMap<u64, Entry>, usize) {
        let mut entries = BTreeMap::new();
        let mut failed = 0;
        for source in SOURCES {
            let read = match source {
                Source::Received => self.received(&mut entries).await,
                Source::Sent => self.sent(&mut entries).await,
                Source::Notes => self.notes(&mut entries).await,
                Source::Channel => match &self.channel {
                    Some(channel) => self.posts(channel, &mut entries).await,
                    None => continue,
                },
            };
            if let Err(e) = read {
                tracing::warn!(thread = %self.thread, error = format!("{e:#}"), "reading back the thread's history");
                failed += 1;
            }
        }
        (entries, failed)
    }

    /// From the thread's start when its id tells it, else from the oldest message kept.
    fn start(&self) -> consumer::DeliverPolicy {
        match crate::channel::thread_start(&self.thread) {
            Some(time) => consumer::DeliverPolicy::ByStartTime { start_time: time.into() },
            None => consumer::DeliverPolicy::All,
        }
    }

    async fn received(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let (reader, filter) = (inbox_consumer(&self.ant), subject::inbox_of(&self.ant));
        let spare = self.reading.beside();
        reading::read_back(&self.js, &reader, spare, &filter, self.start(), |seq, at, subject, payload| {
            if seq >= self.before {
                return ControlFlow::Break(());
            }
            self.take_envelope(seq, at, subject, payload, entries);
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the messages received")
    }

    async fn sent(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let _one = if self.reading == Reading::Consumer { Some(SENT_READS.lock().await) } else { None };
        let (reader, filter) = (sent_reader(&self.ant), subject::inbox_from(&self.ant));
        reading::read_back(&self.js, &reader, self.reading, &filter, self.start(), |seq, at, subject, payload| {
            self.take_envelope(seq, at, subject, payload, entries);
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the messages sent")
    }

    fn take_envelope(
        &self,
        seq: u64,
        at: SystemTime,
        subject: &str,
        payload: &[u8],
        entries: &mut BTreeMap<u64, Entry>,
    ) {
        let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) else { return };
        if envelope.thread_id == self.thread && envelope.check_subject(subject).is_ok() {
            let attachments = earlier_lines(&envelope.attachments, |id| envelope.attachment_handle(id));
            let place = Place::Inbox(envelope.to);
            entries.insert(seq, Entry { from: envelope.from, place, text: envelope.text, attachments, at });
        }
    }

    async fn notes(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let (reader, filter) = (notes_consumer(&self.ant), subject::notes_of(&self.thread));
        let all = consumer::DeliverPolicy::All;
        reading::read_back(&self.js, &reader, self.reading.beside(), &filter, all, |seq, at, subject, payload| {
            if let Ok(post) = serde_json::from_slice::<NotesPost>(payload)
                && post.check_subject(subject).is_ok()
            {
                let attachments = earlier_lines(&post.attachments, |id| post.attachment_handle(id));
                entries.insert(seq, Entry { from: post.from, place: Place::Notes, text: post.text, attachments, at });
            }
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the thread notes")
    }

    async fn posts(&self, channel: &ChannelId, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let room = subject::chan_room(channel);
        crate::channel::read_back(
            &self.js,
            &self.ant,
            self.reading.beside(),
            &room,
            self.start(),
            |seq, at, _, post| {
                if let Ok(post) = post
                    && post.thread_id == self.thread
                    && (seq < self.before || post.from == self.ant)
                {
                    let attachments = earlier_lines(&post.attachments, |id| post.attachment_handle(id));
                    let place = Place::Channel(post.channel);
                    entries.insert(seq, Entry { from: post.from, place, text: post.text, attachments, at });
                }
                ControlFlow::Continue(())
            },
        )
        .await
        .with_context(|| format!("reading back the thread's posts in channel {channel}"))
    }
}

/// A thread of an ant active over a time, as [`active`] reads it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Active {
    pub thread: ThreadId,
    /// What the ant received and sent on it and its notes, as a prompt shows a history.
    pub history: String,
    /// The tool-call timeline the ant stored on it, oldest first: empty when it stored none, or it could not be read.
    pub timeline: String,
    /// The ant's messages and notes posts on it, oldest first.
    pub sent: Vec<Sent>,
    /// The ant's tool requests rejected on it, as its timeline keeps them: empty when it could not be read.
    pub rejections: Vec<MergedRejection>,
    /// The ant's tool calls on it, as its timeline keeps them: empty when it could not be read.
    pub calls: Vec<MergedCall>,
    /// How the ant's prompts on it ended, as its timeline keeps them: empty when it could not be read.
    pub ends: Vec<MergedEnd>,
    /// The issues the ant escalated on it: each reason, and tool request, once, however many messages it took. Tool
    /// requests and questions to humans are not counted.
    pub escalations: u32,
}

/// A message the ant sent on a thread, or posted to its notes, as [`active`] read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sent {
    pub id: Uuid,
    /// The start of its text, on one line, at most [`OPENING_BYTES`] bytes.
    pub opening: String,
}

/// The most bytes of a message's text [`Sent::opening`] keeps.
const OPENING_BYTES: usize = 80;

impl Sent {
    fn new(id: Uuid, text: &str) -> Self {
        let line = text.trim().replace(['\n', '\r'], " ");
        let opening = if line.len() <= OPENING_BYTES {
            line
        } else {
            format!("{}…", &line[..line.floor_char_boundary(OPENING_BYTES)])
        };
        Self { id, opening }
    }
}

/// The threads of `ant` active from `since` to `until`, the most recently active first, each with what the ant received
/// and sent on it and its notes from `since` on, as a prompt shows a history within `budget` bytes, the messages and
/// notes posts of the ant among them, and the timeline it stored on it meanwhile. Read as its daemon reads
/// ([`Reading::daemon`]).
pub(crate) async fn active(
    js: &jetstream::Context,
    ant: &UserId,
    reading: Reading,
    since: SystemTime,
    until: SystemTime,
    budget: usize,
) -> Result<Vec<Active>> {
    let start = || consumer::DeliverPolicy::ByStartTime { start_time: since.into() };
    let mut threads: BTreeMap<ThreadId, BTreeMap<u64, Entry>> = BTreeMap::new();
    // The ids of the ant's own messages and notes posts on each thread, by stream sequence.
    let mut own: BTreeMap<ThreadId, BTreeMap<u64, Uuid>> = BTreeMap::new();
    let mut escalated: BTreeMap<ThreadId, HashSet<(EscalationReason, Option<Uuid>)>> = BTreeMap::new();
    let mut take = |seq, at, subject: &str, payload: &[u8]| {
        if at > until {
            return ControlFlow::Break(());
        }
        if let Ok(envelope) = serde_json::from_slice::<Envelope>(payload)
            && envelope.check_subject(subject).is_ok()
        {
            if envelope.from == *ant {
                own.entry(envelope.thread_id.clone()).or_default().insert(seq, envelope.id);
                if let Some(escalation) = envelope.escalation.as_deref()
                    && escalation.stuck == *ant
                    && !matches!(escalation.reason, EscalationReason::Approval | EscalationReason::Question)
                {
                    let issue = (escalation.reason, escalation.request_id);
                    escalated.entry(escalation.thread_id.clone()).or_default().insert(issue);
                }
            }
            let attachments = earlier_lines(&envelope.attachments, |id| envelope.attachment_handle(id));
            let entry =
                Entry { from: envelope.from, place: Place::Inbox(envelope.to), text: envelope.text, attachments, at };
            threads.entry(envelope.thread_id).or_default().insert(seq, entry);
        }
        ControlFlow::Continue(())
    };
    let (reader, filter) = (inbox_consumer(ant), subject::inbox_of(ant));
    reading::read_back(js, &reader, reading.beside(), &filter, start(), &mut take)
        .await
        .context("reading back the messages received")?;
    {
        let _one = if reading == Reading::Consumer { Some(SENT_READS.lock().await) } else { None };
        let (reader, filter) = (sent_reader(ant), subject::inbox_from(ant));
        reading::read_back(js, &reader, reading, &filter, start(), &mut take)
            .await
            .context("reading back the messages sent")?;
    }
    let reader = notes_consumer(ant);
    for (thread, entries) in &mut threads {
        let filter = subject::notes_of(thread);
        reading::read_back(js, &reader, reading.beside(), &filter, start(), |seq, at, subject, payload| {
            if at > until {
                return ControlFlow::Break(());
            }
            if let Ok(post) = serde_json::from_slice::<NotesPost>(payload)
                && post.check_subject(subject).is_ok()
            {
                if post.from == *ant {
                    own.entry(thread.clone()).or_default().insert(seq, post.id);
                }
                let attachments = earlier_lines(&post.attachments, |id| post.attachment_handle(id));
                entries.insert(seq, Entry { from: post.from, place: Place::Notes, text: post.text, attachments, at });
            }
            ControlFlow::Continue(())
        })
        .await
        .with_context(|| format!("reading back the notes of thread {thread}"))?;
    }
    // Timeline records are best effort, so a thread is shown without them rather than not at all.
    let mut timelines = timeline::read(js, ant, reading, start(), until, |thread| threads.contains_key(thread))
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(%ant, error = format!("{e:#}"), "reading back the timeline of the active threads");
            BTreeMap::new()
        });
    let mut threads: Vec<_> = threads.into_iter().collect();
    threads.sort_by_key(|(_, entries)| std::cmp::Reverse(entries.values().map(|entry| entry.at).max()));
    let active = threads.into_iter().map(|(thread, entries)| {
        let stored = timelines.remove(&thread);
        let timeline = stored.as_ref().map_or_default(|stored| stored.show(ant, budget / ENTRY_SHARE));
        let rejections = stored.as_ref().map_or_default(|stored| stored.rejections().to_vec());
        let calls = stored.as_ref().map_or_default(|stored| stored.calls().to_vec());
        let ends = stored.map_or_default(|stored| stored.ends().to_vec());
        let escalations = escalated.remove(&thread).map_or(0, |issues| u32::try_from(issues.len()).unwrap_or(u32::MAX));
        let own = own.remove(&thread).unwrap_or_default().into_iter();
        let sent = own.filter_map(|(seq, id)| entries.get(&seq).map(|entry| Sent::new(id, &entry.text))).collect();
        let history = format(entries.into_values().collect(), budget);
        Active { thread, history, timeline, sent, rejections, calls, ends, escalations }
    });
    Ok(active.collect())
}

#[derive(Debug, Clone, Copy)]
enum Source {
    Received,
    Sent,
    Notes,
    Channel,
}

const SOURCES: [Source; 4] = [Source::Received, Source::Sent, Source::Notes, Source::Channel];

/// One message of a thread's history.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    from: UserId,
    place: Place,
    text: String,
    /// A line for each file attached to it ([`earlier_lines`]).
    attachments: Vec<String>,
    /// When the stream stored it.
    at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Place {
    /// A message to that user's inbox.
    Inbox(UserId),
    Notes,
    Channel(ChannelId),
}

impl Entry {
    /// The entry as a prompt shows it, its text cut to `most` bytes, then a line for each attachment.
    fn show(&self, most: usize) -> String {
        let from = &self.from;
        let heading = match &self.place {
            Place::Inbox(to) => format!("{from} to {to}:"),
            Place::Notes => format!("{from} in the thread notes:"),
            Place::Channel(channel) => format!("{from} in channel {channel}:"),
        };
        let text = self.text.trim();
        let mut shown = if text.len() <= most {
            format!("{heading}\n{text}")
        } else {
            let kept = text.floor_char_boundary(most);
            format!("{heading}\n{} … ({} bytes left out)", &text[..kept], text.len() - kept)
        };
        for line in &self.attachments {
            shown.push('\n');
            shown.push_str(line);
        }
        shown
    }
}

/// `entries`, oldest first, as a prompt shows them: the most recent that fit in `budget` bytes, each cut to an eighth
/// of it, after a line telling what older ones were left out.
fn format(entries: Vec<Entry>, budget: usize) -> String {
    if entries.is_empty() {
        return "[ants] The thread's history holds no earlier messages.".to_owned();
    }
    let mut kept = Vec::new();
    let mut used = 0;
    for entry in entries.iter().rev() {
        let shown = entry.show(budget / ENTRY_SHARE);
        used += shown.len();
        if used > budget {
            break;
        }
        kept.push(shown);
    }
    let mut history = "[ants] The thread's history, read back from ants, oldest first:".to_owned();
    if kept.len() < entries.len() {
        history.push('\n');
        history.push_str(&left_out(&entries[..entries.len() - kept.len()]));
    }
    for shown in kept.iter().rev() {
        history.push_str("\n\n");
        history.push_str(shown);
    }
    history.push_str("\n\n[ants] End of the thread's history.");
    history
}

/// The line telling what `dropped`, not empty, holds: how many entries of each kind, from whom and when.
fn left_out(dropped: &[Entry]) -> String {
    let count = |n: usize, one: &str| if n == 1 { format!("1 {one}") } else { format!("{n} {one}s") };
    let of_kind = |kind: fn(&Place) -> bool| dropped.iter().filter(|e| kind(&e.place)).count();
    let kinds = [
        (of_kind(|p| matches!(p, Place::Inbox(_))), "message"),
        (of_kind(|p| matches!(p, Place::Notes)), "thread note"),
        (of_kind(|p| matches!(p, Place::Channel(_))), "channel post"),
    ];
    let kinds: Vec<_> = kinds.into_iter().filter(|(n, _)| *n > 0).map(|(n, one)| count(n, one)).collect();
    let senders: BTreeSet<_> = dropped.iter().map(|e| e.from.as_str()).collect();
    let mut named: Vec<_> = senders.iter().take(SENDERS_NAMED).map(|s| (*s).to_owned()).collect();
    if senders.len() > named.len() {
        named.push(count(senders.len() - named.len(), "other"));
    }
    let minute = |at: SystemTime| chrono::DateTime::<chrono::Utc>::from(at).format("%Y-%m-%d %H:%M").to_string();
    let first = dropped.iter().map(|e| e.at).min().map_or_default(minute);
    let last = dropped.iter().map(|e| e.at).max().map_or_default(minute);
    let when = if first == last { format!("at {first} UTC") } else { format!("between {first} and {last} UTC") };
    let (n, verb) = match dropped.len() {
        1 => ("1 older entry".to_owned(), "is"),
        n => (format!("{n} older entries"), "are"),
    };
    format!("[ants] {n} {verb} left out: {} from {}, {when}.", and_list(&kinds), and_list(&named))
}

/// `a`, `a and b`, `a, b and c`.
fn and_list(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    /// `minutes` past 2026-11-14 09:00 UTC.
    fn at(minutes: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_794_646_800 + minutes * 60)
    }

    fn entry(from: &str, place: Place, text: &str, minutes: u64) -> Entry {
        Entry { from: p(from), place, text: text.to_owned(), attachments: Vec::new(), at: at(minutes) }
    }

    fn to(from: &str, to: &str, text: &str) -> Entry {
        entry(from, Place::Inbox(p(to)), text, 0)
    }

    #[test]
    fn the_most_recent_entries_within_the_budget_are_shown_oldest_first() {
        let entries = vec![
            to("dev-b", "dev-a", "first, long ago"),
            entry("dev-a", Place::Notes, "a plan", 5),
            entry("dev-b", Place::Channel("ops".parse().unwrap()), "on it", 70),
            to("dev-a", "dev-b", "done\n"),
        ];
        let all = format(entries.clone(), DEFAULT_BUDGET);
        assert_eq!(
            all,
            "[ants] The thread's history, read back from ants, oldest first:\n\ndev-b to dev-a:\nfirst, long ago\n\n\
             dev-a in the thread notes:\na plan\n\ndev-b in channel ops:\non it\n\ndev-a to dev-b:\ndone\n\n\
             [ants] End of the thread's history."
        );
        let budget = entries[1..].iter().map(|e| e.show(usize::MAX).len()).sum();
        let recent = format(entries.clone(), budget);
        let one = "\n[ants] 1 older entry is left out: 1 message from dev-b, at 2026-11-14 09:00 UTC.\n\n\
                   dev-a in the thread notes:";
        assert!(recent.contains(one), "{recent}");
        assert!(!recent.contains("long ago"), "{recent}");
        let two = "[ants] 2 older entries are left out: 1 message and 1 thread note from dev-a and dev-b, between \
                   2026-11-14 09:00 and 2026-11-14 09:05 UTC.";
        assert!(format(entries.clone(), budget - 1).contains(two));
        let none = "[ants] 4 older entries are left out: 2 messages, 1 thread note and 1 channel post from dev-a and \
                    dev-b, between 2026-11-14 09:00 and 2026-11-14 10:10 UTC.";
        assert!(format(entries, 0).contains(none));
        assert_eq!(format(Vec::new(), DEFAULT_BUDGET), "[ants] The thread's history holds no earlier messages.");
    }

    #[tokio::test]
    async fn the_threads_active_since_a_time_come_most_recent_first_with_their_notes_and_timeline() {
        let Some(server) = ants_test_support::NatsServer::start() else { return };
        let js = async_nats::jetstream::new(async_nats::connect(&server.url).await.unwrap());
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        let (one, two): (ThreadId, ThreadId) = ("t1".parse().unwrap(), "t2".parse().unwrap());
        let publish = async |subject: String, payload: Vec<u8>| {
            js.publish(subject, payload.into()).await.unwrap().await.unwrap();
        };
        let message = async |thread: &ThreadId, from: &str, to: &str, text: &str| {
            let envelope = Envelope::on_thread(thread.clone(), p(from), p(to), text);
            publish(envelope.subject(), serde_json::to_vec(&envelope).unwrap()).await;
        };
        message(&one, "alice", "dev-a", "long ago").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let since = SystemTime::now();
        message(&one, "alice", "dev-a", "fix the build").await;
        let note = NotesPost::new(one.clone(), p("dev-a"), "the plan");
        publish(note.subject(), serde_json::to_vec(&note).unwrap()).await;
        message(&two, "dev-a", "bob", "a question").await;
        let escalate = async |text: &str, reason, stuck: &str| {
            let escalation = ants_proto::Escalation::new(p(stuck), two.clone(), reason);
            let envelope =
                Envelope::on_thread(two.clone(), p("dev-a"), p("bob"), text).with_escalation(Some(escalation));
            publish(envelope.subject(), serde_json::to_vec(&envelope).unwrap()).await;
        };
        escalate("capped", EscalationReason::Cap, "dev-a").await;
        escalate("still capped", EscalationReason::Cap, "dev-a").await;
        escalate("a tool request", EscalationReason::Approval, "dev-a").await;
        escalate("passed on", EscalationReason::Asked, "dev-c").await;
        message(&one, "alice", "bob", "not to dev-a").await;
        let record = async |thread: &ThreadId, ant: &str, entries: Vec<ants_proto::TimelineEntry>| {
            let record = ants_proto::Timeline::new(p(ant), thread.clone(), ants_proto::Uuid::nil(), entries, 0);
            publish(record.subject(), serde_json::to_vec(&record).unwrap()).await;
        };
        record(&one, "dev-a", call("cargo build")).await;
        record(&one, "dev-a", ended(ants_proto::PromptEnd::Cancelled)).await;
        let rejection = ants_proto::TimelineRejection {
            request: ants_proto::Uuid::from_u128(9),
            kind: "execute".to_owned(),
            title: "rm -rf target".to_owned(),
            by: ants_proto::DecidedBy::Expiry,
            decider: None,
            via: None,
        };
        let rejected = vec![ants_proto::TimelineEntry::Rejected(rejection)];
        let alone = ants_proto::Timeline::new(p("dev-a"), one.clone(), ants_proto::Uuid::from_u128(8), rejected, 0);
        publish(alone.subject(), serde_json::to_vec(&alone).unwrap()).await;
        record(&one, "bob", call("not dev-a's")).await;
        record(&"t3".parse().unwrap(), "dev-a", call("no thread active")).await;
        let until = SystemTime::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        message(&two, "bob", "dev-a", "too late").await;
        record(&two, "dev-a", call("cargo late")).await;

        let threads = active(&js, &p("dev-a"), Reading::Gets, since, until, DEFAULT_BUDGET).await.unwrap();
        assert_eq!(threads.iter().map(|active| active.thread.clone()).collect::<Vec<_>>(), [two, one]);
        assert!(threads[0].history.contains("a question") && !threads[0].history.contains("too late"));
        assert_eq!(threads[0].timeline, "", "{threads:?}");
        let first = &threads[1].history;
        assert!(first.contains("fix the build") && first.contains("the plan"), "{first}");
        assert!(!first.contains("long ago") && !first.contains("not to dev-a"), "{first}");
        let timeline = &threads[1].timeline;
        assert!(timeline.contains("execute: cargo build (completed)\n") && !timeline.contains("dev-a's"), "{timeline}");
        assert!(timeline.contains(" cancelled by a human after 1 s\n"), "{timeline}");
        let titles: Vec<_> = threads[1].calls.iter().map(|c| c.call.title.as_str()).collect();
        assert_eq!((titles, threads[1].ends.len()), (vec!["cargo build"], 1));
        assert!(threads[0].calls.is_empty() && threads[0].ends.is_empty());
        assert_eq!((threads[0].escalations, threads[1].escalations), (1, 0), "a reason once, tool requests aside");
        assert_eq!(
            timeline.matches("the prompt of message").count(),
            1,
            "a rejection alone shows no prompt: {timeline}"
        );
        let rejected: Vec<_> = threads[1].rejections.iter().map(|r| r.rejection.title.as_str()).collect();
        assert_eq!(rejected, ["rm -rf target"]);
        assert!(threads[0].rejections.is_empty());
        let sent = |active: &Active| active.sent.iter().map(|sent| sent.opening.clone()).collect::<Vec<_>>();
        assert_eq!(
            (sent(&threads[0]), sent(&threads[1])),
            (
                ["a question", "capped", "still capped", "a tool request", "passed on"].map(str::to_owned).to_vec(),
                vec!["the plan".to_owned()]
            )
        );
        assert_eq!(threads[1].sent[0].id, note.id);
    }

    /// A timeline's entries: a completed call of `title`.
    fn call(title: &str) -> Vec<ants_proto::TimelineEntry> {
        let call = ants_proto::TimelineCall {
            id: title.to_owned(),
            tool_kind: Some("execute".to_owned()),
            title: title.to_owned(),
            status: Some("completed".to_owned()),
            started: chrono::Utc::now(),
            duration_ms: None,
            paths: Vec::new(),
        };
        vec![ants_proto::TimelineEntry::Tool(call)]
    }

    /// A timeline's entries: a prompt that ended `how` after a second.
    fn ended(how: ants_proto::PromptEnd) -> Vec<ants_proto::TimelineEntry> {
        let end = ants_proto::TimelineEnd { how, duration_ms: 1_000, tokens: 0, cost: 0.0, asked: 0, rebuilt: false };
        vec![ants_proto::TimelineEntry::Ended(end)]
    }

    #[test]
    fn the_opening_of_a_message_sent_is_one_short_line() {
        let id = ants_proto::Uuid::nil();
        assert_eq!(Sent::new(id, "  fixed\nthe build \n").opening, "fixed the build");
        let long = format!("{}é and more", "x".repeat(OPENING_BYTES - 1));
        assert_eq!(Sent::new(id, &long).opening, format!("{}…", "x".repeat(OPENING_BYTES - 1)));
    }

    #[test]
    fn the_line_about_entries_left_out_names_a_few_senders() {
        let dropped: Vec<_> = (0..10).map(|i| entry(&format!("dev-{i}"), Place::Notes, "x", 1)).collect();
        assert_eq!(
            left_out(&dropped),
            "[ants] 10 older entries are left out: 10 thread notes from dev-0, dev-1, dev-2, dev-3, dev-4, dev-5, \
             dev-6, dev-7 and 2 others, at 2026-11-14 09:01 UTC."
        );
    }

    #[test]
    fn an_entry_names_its_attachments_within_the_budget() {
        let mut message = Envelope::new(p("alice"), p("dev-a"), "the logs");
        let log = crate::files::reference("build.log", "text/plain", b"error").unwrap();
        message.attachments = vec![log.clone(), log.clone()];
        let attachments = earlier_lines(&message.attachments, |id| message.attachment_handle(id));
        let entry = Entry { attachments, ..to("alice", "dev-a", "the logs") };
        let line = format!("[ants] Attached: \"build.log\" (text/plain, 5 bytes), handle alice.inbox.dev-a.{}", log.id);
        assert_eq!(
            entry.show(DEFAULT_BUDGET),
            format!(
                "alice to dev-a:\nthe logs\n{line}; call read_attachment with the handle to read it.\n{line}: \
                 unavailable (its id is repeated)."
            )
        );
        let shown = entry.show(usize::MAX).len();
        let older = to("dev-b", "dev-a", "older");
        assert!(format(vec![older.clone(), entry.clone()], shown).contains(&line));
        let left_out = format(vec![older, entry], shown - 1);
        assert!(!left_out.contains(&line), "the lines count toward the budget: {left_out}");
    }

    #[test]
    fn a_long_entry_is_cut_to_an_eighth_of_the_budget() {
        let most = DEFAULT_BUDGET / ENTRY_SHARE;
        let long = format!("{}é tail", "x".repeat(most - 1));
        let cut = format!("dev-b to dev-a:\n{} … (7 bytes left out)", "x".repeat(most - 1));
        let history = format(vec![to("dev-b", "dev-a", &long)], DEFAULT_BUDGET);
        assert!(history.contains(&format!("\n\n{cut}\n\n")), "{history}");
    }

    #[test]
    fn the_budget_is_set_or_follows_the_context_window() {
        assert_eq!(budget(None, None), DEFAULT_BUDGET);
        assert_eq!(budget(Some(1000), None), 1000);
        assert_eq!(budget(None, Some(200_000)), 66_666, "a twelfth of 800 kB");
        assert_eq!(budget(None, Some(1_000_000)), 333_333);
        assert_eq!(budget(Some(1 << 20), Some(200_000)), 200_000, "at most a quarter of the window");
        assert_eq!(budget(Some(0), Some(200_000)), 0);
    }

    #[tokio::test]
    async fn a_seed_after_an_overflow_takes_half_the_budget() {
        let history = || Some(from_fn(|budget| async move { budget.to_string() }));
        assert!(seed(Rebuild::Lost, history(), 1000).await.ends_with("history.\n\n1000"));
        assert!(seed(Rebuild::Overflow, history(), 1000).await.ends_with("history.\n\n500"));
    }
}
