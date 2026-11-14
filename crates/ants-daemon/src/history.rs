//! A thread's history, read back from the ANTS stream, to rebuild the context of a harness session that lost it.
//!
//! An ant reads its own inbox and the notes through spares of its readers, so as not to get in the way of the reads
//! its daemon makes through the readers themselves, and the messages it sent through its own [`sent_reader`].

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::pin::Pin;

use ants_proto::{
    ChannelId, Envelope, NotesPost, ThreadId, UserId, inbox_consumer, notes_consumer, sent_reader, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};

use crate::reading::{self, Reading};

/// The history of a thread as a prompt shows it, read when the prompt needs it.
pub type History = Pin<Box<dyn Future<Output = String> + Send>>;

/// Why a session's context is rebuilt from the thread's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rebuild {
    /// The thread's earlier session could not be resumed, so a new one started.
    Lost,
    /// The session outgrew the model's context window, so the prompt runs again in a new one.
    Overflow,
}

/// Most bytes of history a prompt shows; older entries are left out.
pub const BUDGET: usize = 64 * 1024;
/// Most bytes of one entry's text a prompt shows; the rest is left out.
const ENTRY_BUDGET: usize = 8 * 1024;

/// One read through the ant's sent reader at a time: each deletes the consumer a read before left.
static SENT_READS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What a prompt in a session rebuilt for `why` starts with: why, and the thread's history.
pub(crate) async fn seed(why: Rebuild, history: Option<History>) -> String {
    let what = match why {
        Rebuild::Lost => "The earlier session of this thread could not be resumed, so this one starts over",
        Rebuild::Overflow => "The session of this thread outgrew the model's context window, so this one starts over",
    };
    match history {
        Some(history) => format!("[ants] {what}, its context rebuilt from the thread's history.\n\n{}", history.await),
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
    /// The thread's history, oldest first, the most recent entries within [`BUDGET`].
    pub fn history(self) -> History {
        Box::pin(async move {
            let (entries, failed) = self.read().await;
            if failed == SOURCES.len() - usize::from(self.channel.is_none()) {
                return "[ants] The thread's history could not be read back, so its earlier context is lost."
                    .to_owned();
            }
            tracing::info!(thread = %self.thread, entries = entries.len(), "read back the thread's history");
            let history = format(entries.into_values().collect(), BUDGET);
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
        reading::read_back(&self.js, &reader, Reading::Spare, &filter, self.start(), |seq, _, subject, payload| {
            if seq >= self.before {
                return ControlFlow::Break(());
            }
            self.take_envelope(seq, subject, payload, entries);
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the messages received")
    }

    async fn sent(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let _one = SENT_READS.lock().await;
        let (reader, filter) = (sent_reader(&self.ant), subject::inbox_from(&self.ant));
        reading::read_back(&self.js, &reader, Reading::Consumer, &filter, self.start(), |seq, _, subject, payload| {
            self.take_envelope(seq, subject, payload, entries);
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the messages sent (credentials from before permissions version 3 cannot)")
    }

    fn take_envelope(&self, seq: u64, subject: &str, payload: &[u8], entries: &mut BTreeMap<u64, Entry>) {
        let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) else { return };
        if envelope.thread_id == self.thread && envelope.check_subject(subject).is_ok() {
            entries.insert(seq, Entry { from: envelope.from, place: Place::Inbox(envelope.to), text: envelope.text });
        }
    }

    async fn notes(&self, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let (reader, filter) = (notes_consumer(&self.ant), subject::notes_of(&self.thread));
        let all = consumer::DeliverPolicy::All;
        reading::read_back(&self.js, &reader, Reading::Spare, &filter, all, |seq, _, subject, payload| {
            if let Ok(post) = serde_json::from_slice::<NotesPost>(payload)
                && post.check_subject(subject).is_ok()
            {
                entries.insert(seq, Entry { from: post.from, place: Place::Notes, text: post.text });
            }
            ControlFlow::Continue(())
        })
        .await
        .context("reading back the thread notes")
    }

    async fn posts(&self, channel: &ChannelId, entries: &mut BTreeMap<u64, Entry>) -> Result<()> {
        let room = subject::chan_room(channel);
        crate::channel::read_back(&self.js, &self.ant, Reading::Spare, &room, self.start(), |seq, _, _, post| {
            if let Ok(post) = post
                && post.thread_id == self.thread
                && (seq < self.before || post.from == self.ant)
            {
                entries.insert(seq, Entry { from: post.from, place: Place::Channel(post.channel), text: post.text });
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Place {
    /// A message to that user's inbox.
    Inbox(UserId),
    Notes,
    Channel(ChannelId),
}

impl Entry {
    /// The entry as a prompt shows it, its text cut to [`ENTRY_BUDGET`].
    fn show(&self) -> String {
        let from = &self.from;
        let heading = match &self.place {
            Place::Inbox(to) => format!("{from} to {to}:"),
            Place::Notes => format!("{from} in the thread notes:"),
            Place::Channel(channel) => format!("{from} in channel {channel}:"),
        };
        let text = self.text.trim();
        if text.len() <= ENTRY_BUDGET {
            return format!("{heading}\n{text}");
        }
        let kept = text.floor_char_boundary(ENTRY_BUDGET);
        format!("{heading}\n{} … ({} bytes left out)", &text[..kept], text.len() - kept)
    }
}

/// `entries`, oldest first, as a prompt shows them: the most recent that fit in `budget` bytes, after a line telling
/// how many older ones were left out.
fn format(entries: Vec<Entry>, budget: usize) -> String {
    if entries.is_empty() {
        return "[ants] The thread's history holds no earlier messages.".to_owned();
    }
    let mut kept = Vec::new();
    let mut used = 0;
    for entry in entries.iter().rev() {
        let shown = entry.show();
        used += shown.len();
        if used > budget {
            break;
        }
        kept.push(shown);
    }
    let mut history = "[ants] The thread's history, read back from ants, oldest first:".to_owned();
    match entries.len() - kept.len() {
        0 => {}
        1 => history.push_str("\n[ants] 1 older entry is left out."),
        n => history = format!("{history}\n[ants] {n} older entries are left out."),
    }
    for shown in kept.iter().rev() {
        history.push_str("\n\n");
        history.push_str(shown);
    }
    history.push_str("\n\n[ants] End of the thread's history.");
    history
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn to(from: &str, to: &str, text: &str) -> Entry {
        Entry { from: p(from), place: Place::Inbox(p(to)), text: text.to_owned() }
    }

    #[test]
    fn the_most_recent_entries_within_the_budget_are_shown_oldest_first() {
        let entries = vec![
            to("dev-b", "dev-a", "first, long ago"),
            Entry { from: p("dev-a"), place: Place::Notes, text: "a plan".to_owned() },
            Entry { from: p("dev-b"), place: Place::Channel("ops".parse().unwrap()), text: "on it".to_owned() },
            to("dev-a", "dev-b", "done\n"),
        ];
        let all = format(entries.clone(), BUDGET);
        assert_eq!(
            all,
            "[ants] The thread's history, read back from ants, oldest first:\n\ndev-b to dev-a:\nfirst, long ago\n\n\
             dev-a in the thread notes:\na plan\n\ndev-b in channel ops:\non it\n\ndev-a to dev-b:\ndone\n\n\
             [ants] End of the thread's history."
        );
        let budget = entries[1..].iter().map(|e| e.show().len()).sum();
        let recent = format(entries.clone(), budget);
        assert!(recent.contains("\n[ants] 1 older entry is left out.\n\ndev-a in the thread notes:"), "{recent}");
        assert!(!recent.contains("long ago"), "{recent}");
        assert!(format(entries, budget - 1).contains("[ants] 2 older entries are left out."));
        assert_eq!(format(Vec::new(), BUDGET), "[ants] The thread's history holds no earlier messages.");
    }

    #[test]
    fn a_long_entry_is_cut() {
        let long = format!("{}é tail", "x".repeat(ENTRY_BUDGET - 1));
        let shown = to("dev-b", "dev-a", &long).show();
        let cut = format!("dev-b to dev-a:\n{} … (7 bytes left out)", "x".repeat(ENTRY_BUDGET - 1));
        assert_eq!(shown, cut);
    }
}
