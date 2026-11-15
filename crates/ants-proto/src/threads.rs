use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{Envelope, STREAM, THREADS_BUCKET, marks_key, summary_key, title_key};
use crate::{Error, MAX_TITLE, ThreadId, UserId};

/// The title of a thread, under its [`title_key`] in [`THREADS_BUCKET`]: one per thread, shared by every human, which
/// any human sets, renames or clears. `by` is the human that last set it, as its CLI says, or the ant whose suggestion
/// ([`Envelope::title`]) a human's CLI stored on a thread that never had a title.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadTitle {
    pub thread_id: ThreadId,
    pub title: String,
    pub by: UserId,
    pub at: DateTime<Utc>,
}

impl ThreadTitle {
    /// `title`, trimmed, set on `thread_id` by `by` now: one line of 1 to [`MAX_TITLE`] characters, no control
    /// characters or Unicode line and paragraph separators.
    pub fn new(thread_id: ThreadId, title: &str, by: UserId) -> Result<Self, Error> {
        let trimmed = title.trim();
        let count = trimmed.chars().count();
        if count == 0 || count > MAX_TITLE || trimmed.chars().any(breaks_title) {
            return Err(Error::InvalidTitle(title.to_owned()));
        }
        Ok(Self { thread_id, title: trimmed.to_owned(), by, at: Utc::now() })
    }
}

/// The TL;DR of a thread, under its [`summary_key`] in [`THREADS_BUCKET`]: the text of `by`'s reply to the summary
/// request ([`Envelope::summary`]) of `asker` (`for` on the wire), covering the thread's entries up to the [`STREAM`]
/// sequence `upto`, and the tokens the prompt spent. Any human's CLI puts or deletes it, for every human; no ant reads
/// or writes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadSummary {
    pub thread_id: ThreadId,
    pub text: String,
    /// The ant that wrote it.
    pub by: UserId,
    /// The human whose CLI asked for it.
    #[serde(rename = "for")]
    pub asker: UserId,
    pub at: DateTime<Utc>,
    pub upto: u64,
    #[serde(default)]
    pub tokens: u64,
}

impl ThreadSummary {
    /// Whether it still covers the thread whose last message, post or note is at [`STREAM`] sequence `last_seq`: no
    /// entry came after `upto`.
    pub fn is_current(&self, last_seq: u64) -> bool {
        last_seq <= self.upto
    }
}

fn breaks_title(c: char) -> bool {
    c.is_control() || matches!(c, '\u{2028}' | '\u{2029}')
}

/// A human's pinned and archived threads, under its [`marks_key`] in [`THREADS_BUCKET`], shared by all its devices,
/// which update it with compare-and-set. A thread is pinned or archived, never both: pinning one unarchives it and
/// archiving one unpins it. Marks change nothing else: an archived thread keeps its messages, its work goes on and
/// its replies reach the inbox as before. A thread with neither mark may count as archived for having been idle
/// ([`ThreadMarks::idle`]), unless the human kept it since.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadMarks {
    /// When each pinned thread was pinned.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub pinned: std::collections::BTreeMap<ThreadId, DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub archived: std::collections::BTreeMap<ThreadId, Archived>,
    /// When each thread the human unarchived while it was idle was kept: it is idle again only once idle since then.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub kept: std::collections::BTreeMap<ThreadId, DateTime<Utc>>,
}

/// When a thread was archived, and the last [`STREAM`] sequence then: a message on the thread stored after it is new
/// activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Archived {
    pub at: DateTime<Utc>,
    pub after: u64,
}

/// How a human marked a thread ([`ThreadMarks::mark`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Pinned,
    /// Archived, and whether a message was stored on the thread since.
    Archived {
        new_activity: bool,
    },
    /// Neither, but archived for having been idle ([`ThreadMarks::idle`]).
    Idle,
}

impl ThreadMarks {
    /// Pin `thread`, unarchiving it. Whether that changed anything.
    pub fn pin(&mut self, thread: &ThreadId) -> bool {
        self.archived.remove(thread);
        self.kept.remove(thread);
        if self.pinned.contains_key(thread) {
            return false;
        }
        self.pinned.insert(thread.clone(), Utc::now());
        true
    }

    /// Unpin `thread`. Whether it was pinned.
    pub fn unpin(&mut self, thread: &ThreadId) -> bool {
        self.pinned.remove(thread).is_some()
    }

    /// Archive `thread` as of `after`, the last [`STREAM`] sequence now, unpinning it. Whether that changed anything:
    /// archiving it again once a message was stored on it since moves `after` on, so that message is no longer new.
    pub fn archive(&mut self, thread: &ThreadId, after: u64) -> bool {
        let unpinned = self.pinned.remove(thread).is_some();
        self.kept.remove(thread);
        if self.archived.get(thread).is_some_and(|a| a.after >= after) {
            return unpinned;
        }
        self.archived.insert(thread.clone(), Archived { at: Utc::now(), after });
        true
    }

    /// Unarchive `thread`. Whether it was archived.
    pub fn unarchive(&mut self, thread: &ThreadId) -> bool {
        self.archived.remove(thread).is_some()
    }

    /// Forget the marks placed before `before` on the threads `gone` says have no message left in [`STREAM`], and the
    /// keeps ([`ThreadMarks::keep`]) of threads `active` says had a message stored at or after they were kept, which
    /// then count as idle from that message on anyway. How many it forgot.
    pub fn prune(
        &mut self,
        before: DateTime<Utc>,
        gone: impl Fn(&ThreadId) -> bool,
        active: impl Fn(&ThreadId, DateTime<Utc>) -> bool,
    ) -> usize {
        let had = self.pinned.len() + self.archived.len() + self.kept.len();
        self.pinned.retain(|thread, at| *at >= before || !gone(thread));
        self.archived.retain(|thread, archived| archived.at >= before || !gone(thread));
        self.kept.retain(|thread, at| (*at >= before || !gone(thread)) && !active(thread, *at));
        had - self.pinned.len() - self.archived.len() - self.kept.len()
    }

    /// How `thread`, whose last message read has [`STREAM`] sequence `last`, is marked.
    pub fn mark(&self, thread: &ThreadId, last: u64) -> Option<Mark> {
        if self.pinned.contains_key(thread) {
            return Some(Mark::Pinned);
        }
        self.archived.get(thread).map(|a| Mark::Archived { new_activity: last > a.after })
    }

    /// Keep `thread` from counting as idle until it is idle again from now on, as unarchiving a thread archived for
    /// having been idle does. Whether that changed anything: a pinned thread is never idle.
    pub fn keep(&mut self, thread: &ThreadId) -> bool {
        if self.pinned.contains_key(thread) {
            return false;
        }
        self.kept.insert(thread.clone(), Utc::now());
        true
    }

    /// Whether `thread`, whose last message was stored at `last_at`, is archived for having been idle since `cutoff`
    /// or before: neither pinned nor archived, with no message stored and not kept ([`ThreadMarks::keep`]) after
    /// `cutoff`. A new message makes it active again, as does keeping it.
    pub fn idle(&self, thread: &ThreadId, last_at: DateTime<Utc>, cutoff: DateTime<Utc>) -> bool {
        let active_at = self.kept.get(thread).map_or(last_at, |kept| last_at.max(*kept));
        !self.pinned.contains_key(thread) && !self.archived.contains_key(thread) && active_at <= cutoff
    }

    /// How `thread`, whose last message read has [`STREAM`] sequence `last` and was stored at `last_at`, is marked,
    /// [`Mark::Idle`] included when threads idle since `cutoff` count as archived.
    pub fn mark_at(
        &self,
        thread: &ThreadId,
        last: u64,
        last_at: DateTime<Utc>,
        cutoff: Option<DateTime<Utc>>,
    ) -> Option<Mark> {
        self.mark(thread, last).or_else(|| cutoff.is_some_and(|c| self.idle(thread, last_at, c)).then_some(Mark::Idle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{marks_key, p, title_key};

    #[test]
    fn a_thread_is_pinned_or_archived_and_new_messages_show_on_an_archived_one() {
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        let mut marks = ThreadMarks::default();
        assert!(marks.pin(&t1) && !marks.pin(&t1));
        assert_eq!(marks.mark(&t1, 5), Some(Mark::Pinned));
        assert!(marks.archive(&t1, 10), "archiving unpins");
        assert!(!marks.pinned.contains_key(&t1));
        assert!(!marks.archive(&t1, 10), "archived already");
        assert_eq!(marks.mark(&t1, 10), Some(Mark::Archived { new_activity: false }));
        assert_eq!(marks.mark(&t1, 11), Some(Mark::Archived { new_activity: true }));
        assert!(marks.archive(&t1, 11), "archiving again moves past the new message");
        assert_eq!(marks.mark(&t1, 11), Some(Mark::Archived { new_activity: false }));
        assert!(marks.pin(&t1) && marks.archived.is_empty(), "pinning unarchives");
        assert!(marks.unpin(&t1) && !marks.unpin(&t1));
        assert!(!marks.unarchive(&t2));
        assert_eq!(marks.mark(&t2, 1), None);
        let json = serde_json::to_string(&marks).unwrap();
        assert_eq!(json, "{}");
        assert_eq!(serde_json::from_str::<ThreadMarks>(&json).unwrap(), marks);
    }

    #[test]
    fn an_unmarked_thread_idle_since_the_cutoff_is_archived_until_a_message_or_keeping_it() {
        let thread = ThreadId::generate();
        let mut marks = ThreadMarks::default();
        let now = Utc::now();
        let (cutoff, old, recent) = (now - chrono::TimeDelta::days(14), now - chrono::TimeDelta::days(20), now);
        assert!(marks.idle(&thread, old, cutoff) && marks.idle(&thread, cutoff, cutoff));
        assert!(!marks.idle(&thread, recent, cutoff), "a new message makes it active");
        assert_eq!(marks.mark_at(&thread, 1, old, Some(cutoff)), Some(Mark::Idle));
        assert_eq!(marks.mark_at(&thread, 1, old, None), None, "auto-archiving off");

        assert!(marks.keep(&thread));
        assert!(!marks.idle(&thread, old, cutoff), "kept from now on");
        assert!(marks.idle(&thread, old, now + chrono::TimeDelta::days(1)), "idle again once idle since keeping it");
        assert!(marks.archive(&thread, 1) && marks.kept.is_empty(), "archiving drops the keep");
        assert_eq!(marks.mark_at(&thread, 1, old, Some(cutoff)), Some(Mark::Archived { new_activity: false }));
        assert!(!marks.idle(&thread, old, cutoff), "archived by hand instead");
        assert!(marks.keep(&thread) && marks.pin(&thread) && marks.kept.is_empty(), "pinning drops the keep");
        assert!(!marks.idle(&thread, old, cutoff) && !marks.keep(&thread), "a pinned thread is never idle");
        assert_eq!(marks.mark_at(&thread, 1, old, Some(cutoff)), Some(Mark::Pinned));
    }

    #[test]
    fn only_old_marks_of_threads_gone_are_forgotten() {
        let (gone, old_gone, kept) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut marks = ThreadMarks::default();
        let before = Utc::now() - chrono::Duration::days(30);
        let long_ago = before - chrono::Duration::seconds(1);
        marks.pin(&gone);
        marks.pinned.insert(old_gone.clone(), long_ago);
        marks.archived.insert(kept.clone(), Archived { at: long_ago, after: 1 });
        let mut pruned = marks.clone();
        let inactive = |_: &ThreadId, _| false;
        assert_eq!(pruned.prune(before, |t| *t != kept, inactive), 1, "only the old mark of a thread gone");
        assert!(pruned.pinned.contains_key(&gone) && !pruned.pinned.contains_key(&old_gone));
        assert!(pruned.archived.contains_key(&kept));
        assert_eq!(marks.prune(before, |_| true, inactive), 2);
        assert_eq!(marks.pinned.keys().collect::<Vec<_>>(), [&gone], "a recent mark stays");

        let (old_keep, recent_keep, outdone, idle) =
            (ThreadId::generate(), ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let now = Utc::now();
        marks.kept = [(&old_keep, long_ago), (&recent_keep, now), (&outdone, now), (&idle, now)]
            .map(|(t, at)| (t.clone(), at))
            .into();
        let active = |t: &ThreadId, at| *t == outdone || (*t == idle && at > now);
        assert_eq!(marks.prune(before, |t| *t != idle, active), 2, "the old keep of a thread gone, one outdone");
        assert!(marks.kept.len() == 2 && marks.kept.contains_key(&recent_keep) && marks.kept.contains_key(&idle));
    }

    #[test]
    fn a_title_is_one_trimmed_line_of_bounded_length() {
        let title = ThreadTitle::new(ThreadId::generate(), "  Deploy fix ", p("alice")).unwrap();
        assert_eq!(title.title, "Deploy fix");
        assert_eq!(title_key(&title.thread_id), format!("title.{}", title.thread_id));
        assert_eq!(marks_key(&p("alice")), "marks.alice");
        for bad in ["", "  ", "two\nlines", "two\u{2028}lines", "two\u{2029}paragraphs", &"x".repeat(MAX_TITLE + 1)] {
            assert!(ThreadTitle::new(ThreadId::generate(), bad, p("alice")).is_err(), "{bad:?}");
        }
        assert!(ThreadTitle::new(ThreadId::generate(), &"é".repeat(MAX_TITLE), p("alice")).is_ok());
    }
}
