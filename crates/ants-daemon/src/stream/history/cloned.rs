//! A clone's copy of a thread's history: what the ant cloned had on the thread when cloned, written by the clone order
//! to `history/THREAD.jsonl` in the clone's home ([`crate::clone`]), a header line then an entry per line, oldest
//! first. The clone's own read-back of the thread puts it first, then only what the stream stored after it.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use ants_proto::{ChannelId, ThreadId, UserId};
use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Entry, Place, SOURCES, Thread};

/// The file in `home` holding the copy of `thread`'s history.
pub fn path(home: &Path, thread: &ThreadId) -> PathBuf {
    home.join("history").join(format!("{thread}.jsonl"))
}

#[derive(Serialize, Deserialize)]
struct Header {
    cloned_from: UserId,
    /// When the copy was read: the stream's later entries are the clone's own.
    at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct Line {
    from: UserId,
    #[serde(flatten)]
    place: Where,
    text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<String>,
    at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "place", rename_all = "lowercase")]
enum Where {
    Inbox { to: UserId },
    Notes,
    Channel { channel: ChannelId },
}

impl Thread {
    /// The thread's whole history as the ant reads it back, as the lines of its copy for a clone, read at `at`: an
    /// error when any part of it could not be read.
    pub async fn snapshot(&self, at: DateTime<Utc>) -> Result<String> {
        let (entries, failed) = self.read().await;
        if failed > 0 {
            let sources = SOURCES.len() - usize::from(self.channel.is_none());
            bail!("{failed} of the {sources} parts of thread {}'s history could not be read back", self.thread);
        }
        let mut lines = serde_json::to_string(&Header { cloned_from: self.ant.clone(), at })?;
        for entry in entries.into_values() {
            let place = match entry.place {
                Place::Inbox(to) => Where::Inbox { to },
                Place::Notes => Where::Notes,
                Place::Channel(channel) | Place::ChannelLink(channel, _) => Where::Channel { channel },
            };
            let line =
                Line { from: entry.from, place, text: entry.text, attachments: entry.attachments, at: entry.at.into() };
            lines.push('\n');
            lines.push_str(&serde_json::to_string(&line)?);
        }
        lines.push('\n');
        Ok(lines)
    }
}

/// The entries of the copy of `thread`'s history in `home`, oldest first, and when it was read; none without a copy.
pub(super) async fn earlier(home: &Path, thread: &ThreadId) -> Result<Option<(Vec<Entry>, SystemTime)>> {
    let path = path(home, thread);
    let text = match tokio::fs::read_to_string(&path).await {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let header: Header = match lines.next() {
        Some(line) => {
            serde_json::from_str(line).with_context(|| format!("reading the header of {}", path.display()))?
        }
        None => bail!("{} is empty", path.display()),
    };
    let mut entries = Vec::new();
    for line in lines {
        let line: Line = serde_json::from_str(line).with_context(|| format!("reading {}", path.display()))?;
        let place = match line.place {
            Where::Inbox { to } => Place::Inbox(to),
            Where::Notes => Place::Notes,
            Where::Channel { channel } => Place::Channel(channel),
        };
        entries.push(Entry {
            from: line.from,
            place,
            text: line.text,
            attachments: line.attachments,
            at: line.at.into(),
        });
    }
    Ok(Some((entries, header.at.into())))
}

/// The `earlier` entries copied at `cut`, then those of the clone's `own` the stream stored after it: those before are
/// the cloned ant's, in the copy already.
pub(super) fn then(mut earlier: Vec<Entry>, cut: SystemTime, own: impl Iterator<Item = Entry>) -> Vec<Entry> {
    earlier.extend(own.filter(|entry| entry.at > cut));
    earlier
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::stream::history::format;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn entry(from: &str, place: Place, text: &str, at: SystemTime) -> Entry {
        Entry { from: p(from), place, text: text.to_owned(), attachments: Vec::new(), at }
    }

    #[tokio::test]
    async fn a_copy_reads_back_as_written_before_the_clones_own_entries() {
        let home = ants_test_support::scratch_dir("history-cloned");
        let thread = ThreadId::generate();
        let start = SystemTime::UNIX_EPOCH + Duration::from_hours(498_513);
        let copied = [
            Line {
                from: p("alice"),
                place: Where::Inbox { to: p("dev-a") },
                text: "plan it".into(),
                attachments: Vec::new(),
                at: start.into(),
            },
            Line {
                from: p("dev-a"),
                place: Where::Notes,
                text: "the plan".into(),
                attachments: vec!["[ants] Attached".into()],
                at: (start + Duration::from_secs(60)).into(),
            },
        ];
        let cut = start + Duration::from_secs(120);
        let mut text = serde_json::to_string(&Header { cloned_from: p("dev-a"), at: cut.into() }).unwrap();
        for line in &copied {
            text.push('\n');
            text.push_str(&serde_json::to_string(line).unwrap());
        }
        std::fs::create_dir(home.as_path().join("history")).unwrap();
        std::fs::write(path(home.as_path(), &thread), text).unwrap();

        let (entries, at) = earlier(home.as_path(), &thread).await.unwrap().unwrap();
        assert_eq!(at, cut);
        let expected = vec![
            entry("alice", Place::Inbox(p("dev-a")), "plan it", start),
            Entry {
                attachments: vec!["[ants] Attached".into()],
                ..entry("dev-a", Place::Notes, "the plan", start + Duration::from_secs(60))
            },
        ];
        assert_eq!(entries, expected);
        let own = [
            entry("dev-a", Place::Notes, "the plan", start + Duration::from_secs(60)),
            entry("dev-b", Place::Inbox(p("alice")), "on it", cut + Duration::from_secs(1)),
        ];
        let entries = then(entries, cut, own.into_iter());
        assert_eq!(entries.len(), 3, "the note read back again is left out: {entries:?}");
        let shown = format(entries, usize::MAX);
        let read = "alice to dev-a:\nplan it\n\ndev-a in the thread notes:\nthe plan\n[ants] Attached\n\ndev-b to alice:\non it";
        assert!(shown.contains(read), "{shown}");
        assert!(earlier(home.as_path(), &ThreadId::generate()).await.unwrap().is_none(), "no copy of another thread");
        std::fs::remove_dir_all(&home).unwrap();
    }
}
