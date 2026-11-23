//! A thread's history, read back from the ANTS stream, to rebuild the context of a harness session that lost it.
//!
//! An ant reads its own inbox and the notes through spares of its readers, so as not to get in the way of the reads
//! its daemon makes through the readers themselves, and the messages it sent through its own [`sent_reader`]; an
//! assistant reads them all by direct gets.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::SystemTime;

use ants_proto::{
    ChannelId, Envelope, NotesPost, ThreadId, UserId, inbox_consumer, notes_consumer, sent_reader, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};

pub(crate) use self::active::{Active, Reopening, ReviewRecords, Sent, active};
use self::entry::{Entry, Place, format};
use crate::channel::from_start;
use crate::stream::reading::{self, Reading};

mod active;
pub mod cloned;
mod entry;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rebuild {
    /// The thread's earlier session could not be resumed, so a new one started.
    Lost,
    /// The session outgrew the model's context window, so the prompt runs again in a new one.
    Overflow,
    /// The thread's earlier session ran in another harness, the ant's own or a fallback (`crate::limits::fallback`).
    Moved,
    /// The ant is a clone of another, whose session of the thread could not be forked ([`crate::clone`]).
    Cloned,
    /// The thread's earlier session worked in the directory given, where the thread no longer works (`crate::pool`).
    Relocated(PathBuf),
}

/// Most bytes of history a prompt shows when neither a budget nor the context window is known; older entries are
/// left out.
pub const DEFAULT_BUDGET: usize = 64 * 1024;
/// Roughly how many bytes of text a token stands for, to size budgets by a context window counted in tokens.
const BYTES_PER_TOKEN: u64 = 4;
/// The share of the context window a budget derived from it takes.
const WINDOW_SHARE: u64 = 12;
/// The share of the context window no budget exceeds.
const WINDOW_CAP: u64 = 4;

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

/// The turn at [`SENT_READS`] a read through an ant's sent reader waits for, as `reading` reads: none unless through
/// the consumer.
pub(crate) async fn sent_turn(reading: Reading) -> Option<tokio::sync::MutexGuard<'static, ()>> {
    if reading == Reading::Consumer { Some(SENT_READS.lock().await) } else { None }
}

/// What a prompt in a session rebuilt for `why` starts with: why, and the thread's history within `budget` bytes, or
/// half of it after a context overflow so that the new session has room to spare.
pub(crate) async fn seed(why: &Rebuild, history: Option<History>, budget: usize) -> String {
    let relocated;
    let (what, budget) = match why {
        Rebuild::Relocated(from) => {
            let from = from.display();
            relocated = format!(
                "The earlier session of this thread worked in another directory, {from}, so this one starts over"
            );
            (relocated.as_str(), budget)
        }
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

    async fn received(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let (reader, filter) = (inbox_consumer(&self.ant), subject::inbox_of(&self.ant));
        let spare = self.reading.beside();
        reading::read_back(&self.js, &reader, spare, &filter, from_start(&self.thread), |seq, at, subject, payload| {
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
        let _one = sent_turn(self.reading).await;
        let (reader, filter) = (sent_reader(&self.ant), subject::inbox_from(&self.ant));
        reading::read_back(
            &self.js,
            &reader,
            self.reading,
            &filter,
            from_start(&self.thread),
            |seq, at, subject, payload| {
                self.take_envelope(seq, at, subject, payload, entries);
                ControlFlow::Continue(())
            },
        )
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
            entries.insert(seq, Entry::received(envelope, at));
        }
    }

    async fn notes(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let (reader, filter) = (notes_consumer(&self.ant), subject::notes_of(&self.thread));
        let all = consumer::DeliverPolicy::All;
        reading::read_back(&self.js, &reader, self.reading.beside(), &filter, all, |seq, at, subject, payload| {
            if let Ok(post) = serde_json::from_slice::<NotesPost>(payload)
                && post.check_subject(subject).is_ok()
            {
                entries.insert(seq, Entry::note(post, at));
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
            from_start(&self.thread),
            |seq, at, _, post| {
                if let Ok(post) = post
                    && post.thread_id == self.thread
                    && (seq < self.before || post.from == self.ant)
                {
                    entries.insert(seq, Entry::posted(post, at));
                }
                ControlFlow::Continue(())
            },
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) use ants_test_support::user as p;

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
        assert!(seed(&Rebuild::Lost, history(), 1000).await.ends_with("history.\n\n1000"));
        assert!(seed(&Rebuild::Overflow, history(), 1000).await.ends_with("history.\n\n500"));
    }
}
