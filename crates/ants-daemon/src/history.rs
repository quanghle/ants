//! A thread's history, read back from the ANTS stream, to rebuild the context of a harness session that lost it.
//!
//! An ant reads its own inbox and the notes through spares of its readers, so as not to get in the way of the reads
//! its daemon makes through the readers themselves, and the messages it sent through its own [`sent_reader`].

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::pin::Pin;
use std::time::SystemTime;

use ants_proto::{
    ChannelId, Envelope, NotesPost, ThreadId, UserId, inbox_consumer, notes_consumer, sent_reader, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};

use crate::reading::{self, Reading};

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
    /// The thread's earlier session ran in another harness, the ant's own or a fallback ([`crate::fallback`]).
    Moved,
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

/// One read through an ant's sent reader at a time, of a thread's history or of the messages that keep waiting
/// ([`crate::unserved`]): each deletes the consumer a read before left.
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
/// before `before`).
pub struct Thread {
    pub js: jetstream::Context,
    pub ant: UserId,
    pub thread: ThreadId,
    pub channel: Option<ChannelId>,
    pub before: u64,
}

impl Thread {
    /// The thread's history, oldest first, the most recent entries within the budget given.
    pub fn history(self) -> History {
        from_fn(move |budget| async move {
            let (entries, failed) = self.read().await;
            if failed == SOURCES.len() - usize::from(self.channel.is_none()) {
                return "[ants] The thread's history could not be read back, so its earlier context is lost."
                    .to_owned();
            }
            tracing::info!(thread = %self.thread, entries = entries.len(), budget, "read back the thread's history");
            let history = format(entries.into_values().collect(), budget);
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
        reading::read_back(&self.js, &reader, Reading::Spare, &filter, self.start(), |seq, at, subject, payload| {
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
        let _one = SENT_READS.lock().await;
        let (reader, filter) = (sent_reader(&self.ant), subject::inbox_from(&self.ant));
        reading::read_back(&self.js, &reader, Reading::Consumer, &filter, self.start(), |seq, at, subject, payload| {
            self.take_envelope(seq, at, subject, payload, entries);
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the messages sent (credentials from before permissions version 3 cannot)")
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
            let place = Place::Inbox(envelope.to);
            entries.insert(seq, Entry { from: envelope.from, place, text: envelope.text, at });
        }
    }

    async fn notes(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let (reader, filter) = (notes_consumer(&self.ant), subject::notes_of(&self.thread));
        let all = consumer::DeliverPolicy::All;
        reading::read_back(&self.js, &reader, Reading::Spare, &filter, all, |seq, at, subject, payload| {
            if let Ok(post) = serde_json::from_slice::<NotesPost>(payload)
                && post.check_subject(subject).is_ok()
            {
                entries.insert(seq, Entry { from: post.from, place: Place::Notes, text: post.text, at });
            }
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the thread notes")
    }

    async fn posts(&self, channel: &ChannelId, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let room = subject::chan_room(channel);
        crate::channel::read_back(&self.js, &self.ant, Reading::Spare, &room, self.start(), |seq, at, _, post| {
            if let Ok(post) = post
                && post.thread_id == self.thread
                && (seq < self.before || post.from == self.ant)
            {
                let place = Place::Channel(post.channel);
                entries.insert(seq, Entry { from: post.from, place, text: post.text, at });
            }
            ControlFlow::Continue(())
        })
        .await
        .with_context(|| format!("reading back the thread's posts in channel {channel}"))
    }
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
    /// The entry as a prompt shows it, its text cut to `most` bytes.
    fn show(&self, most: usize) -> String {
        let from = &self.from;
        let heading = match &self.place {
            Place::Inbox(to) => format!("{from} to {to}:"),
            Place::Notes => format!("{from} in the thread notes:"),
            Place::Channel(channel) => format!("{from} in channel {channel}:"),
        };
        let text = self.text.trim();
        if text.len() <= most {
            return format!("{heading}\n{text}");
        }
        let kept = text.floor_char_boundary(most);
        format!("{heading}\n{} … ({} bytes left out)", &text[..kept], text.len() - kept)
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
        Entry { from: p(from), place, text: text.to_owned(), at: at(minutes) }
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
