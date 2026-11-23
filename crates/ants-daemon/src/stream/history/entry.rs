//! The entries of a thread's history, and how a prompt shows them within a budget.

use std::collections::BTreeSet;
use std::time::SystemTime;

use ants_proto::{ChannelId, ChannelPost, Envelope, NotesPost, ThreadId, UserId};

use crate::files::earlier_lines;

/// The share of a budget one entry's text may take; the rest of the text is left out.
pub(super) const ENTRY_SHARE: usize = 8;
/// Most senders the line about the entries left out names.
const SENDERS_NAMED: usize = 8;

/// One message of a thread's history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Entry {
    pub(super) from: UserId,
    pub(super) place: Place,
    pub(super) text: String,
    /// A line for each file attached to it ([`earlier_lines`]).
    pub(super) attachments: Vec<String>,
    /// When the stream stored it.
    pub(super) at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Place {
    /// A message to that user's inbox.
    Inbox(UserId),
    Notes,
    Channel(ChannelId),
    ChannelLink(ChannelId, ThreadId),
}

impl Place {
    /// What an entry in the place is.
    pub(super) fn noun(&self) -> &'static str {
        match self {
            Self::Inbox(_) => "message",
            Self::Notes => "thread note",
            Self::Channel(_) | Self::ChannelLink(..) => "channel post",
        }
    }
}

impl Entry {
    /// The message `envelope`, stored at `at`.
    pub(super) fn received(envelope: Envelope, at: SystemTime) -> Self {
        let attachments = earlier_lines(&envelope.attachments, |id| envelope.attachment_handle(id));
        Self { from: envelope.from, place: Place::Inbox(envelope.to), text: envelope.text, attachments, at }
    }

    /// The notes post `post`, stored at `at`.
    pub(super) fn note(post: NotesPost, at: SystemTime) -> Self {
        let attachments = earlier_lines(&post.attachments, |id| post.attachment_handle(id));
        Self { from: post.from, place: Place::Notes, text: post.text, attachments, at }
    }

    /// The channel post `post`, stored at `at`.
    pub(super) fn posted(post: ChannelPost, at: SystemTime) -> Self {
        let attachments = earlier_lines(&post.attachments, |id| post.attachment_handle(id));
        Self { from: post.from, place: Place::Channel(post.channel), text: post.text, attachments, at }
    }

    /// The entry as a prompt shows it, its text cut to `most` bytes, then a line for each attachment.
    pub(super) fn show(&self, most: usize) -> String {
        let from = &self.from;
        let heading = match &self.place {
            Place::Inbox(to) => format!("{from} to {to}:"),
            Place::Notes => format!("{from} in the thread notes:"),
            Place::Channel(channel) => format!("{from} in channel {channel}:"),
            Place::ChannelLink(channel, thread) => format!("{from} started thread {thread} in channel {channel}:"),
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
pub(super) fn format(entries: Vec<Entry>, budget: usize) -> String {
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
        (of_kind(|p| matches!(p, Place::Channel(_) | Place::ChannelLink(..))), "channel post"),
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
    use crate::stream::history::DEFAULT_BUDGET;
    use crate::stream::history::tests::p;

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
}
