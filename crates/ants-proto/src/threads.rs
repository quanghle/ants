use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{
    Envelope, STREAM, THREADS_BUCKET, assisted_marks_key, assisted_snoozed_key, done_key, marks_key, snoozed_key,
    summary_key, title_key,
};
use crate::{Error, MAX_TITLE, Sealed, ThreadId, UserId};

/// The title of a thread, under its [`title_key`] in [`THREADS_BUCKET`]: one per thread, shared by every human, which
/// any human sets, renames or clears. `by` is the human that last set it, as its CLI says, or the ant whose suggestion
/// ([`Envelope::title`]) a human's CLI stored on a thread that never had a title.
///
/// On a thread between two humans whose messages are sealed, the title is sealed like a message for the devices of
/// both: `title` is then empty and `sealed` holds a [`SealedTitle`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadTitle {
    pub thread_id: ThreadId,
    pub title: String,
    pub by: UserId,
    pub at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<Box<Sealed>>,
    /// The human whose assistant, `by`, set the title for it ([`crate::Acting`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
}

/// What the [`Sealed::body`] of a sealed [`ThreadTitle`] holds: its title, with the thread and the human that set
/// it, which a reader checks against the title it came in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedTitle {
    pub thread_id: ThreadId,
    pub by: UserId,
    pub title: String,
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
        Ok(Self { thread_id, title: trimmed.to_owned(), by, at: Utc::now(), sealed: None, on_behalf_of: None })
    }

    /// The same, set for `human` by its assistant ([`ThreadTitle::on_behalf_of`]).
    #[must_use]
    pub fn on_behalf(self, human: Option<UserId>) -> Self {
        Self { on_behalf_of: human, ..self }
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

/// A thread marked done, under its [`done_key`] in [`THREADS_BUCKET`]: one state per thread, shared by every human,
/// which any human sets or clears and no ant reads or writes. `by` is the human that marked it, `after` the [`STREAM`]
/// sequence of the last message, channel post or note on the thread it saw: one stored above it reopens the thread
/// ([`ThreadDone::holds`]) without a write, as new activity shows on an archived thread. Being done changes no ant's
/// work, reply or notification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadDone {
    pub thread_id: ThreadId,
    pub by: UserId,
    pub at: DateTime<Utc>,
    pub after: u64,
    /// The human whose assistant, `by`, marked the thread done for it ([`crate::Acting`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
}

impl ThreadDone {
    /// `thread_id` marked done by `by` now, as of sequence `after`.
    pub fn new(thread_id: ThreadId, by: UserId, after: u64) -> Self {
        Self { thread_id, by, at: Utc::now(), after, on_behalf_of: None }
    }

    /// The same, marked for `human` by its assistant ([`ThreadDone::on_behalf_of`]).
    #[must_use]
    pub fn on_behalf(self, human: Option<UserId>) -> Self {
        Self { on_behalf_of: human, ..self }
    }

    /// Whether the thread is still done, the last message, channel post or note stored on it having sequence `last`.
    pub fn holds(&self, last: u64) -> bool {
        last <= self.after
    }
}

/// Longest a thread stays snoozed on a human's home ([`ThreadSnoozes`]).
pub const SNOOZE_MAX: chrono::TimeDelta = chrono::TimeDelta::days(7);

/// The threads a human snoozed on home, under its [`snoozed_key`] in [`THREADS_BUCKET`], shared by all its devices,
/// which update it with compare-and-set. A snooze set `at` hides the thread from home until `until`, at most
/// [`SNOOZE_MAX`] later, or until an item for the human arrives on the thread after `at`. Snoozing changes no ant's
/// work, reply or notification. The human's assistant snoozes threads for it under its own key
/// ([`assisted_snoozed_key`]), which readers take in only while the human's grant names it
/// ([`ThreadSnoozes::with_assisted`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ThreadSnoozes(pub std::collections::BTreeMap<ThreadId, Snooze>);

/// When a thread was snoozed, and until when.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snooze {
    pub at: DateTime<Utc>,
    pub until: DateTime<Utc>,
}

impl ThreadSnoozes {
    /// Snooze `thread` from `now` until `until`, cut to [`SNOOZE_MAX`] from now, dropping the snoozes already past.
    pub fn snooze(&mut self, thread: &ThreadId, now: DateTime<Utc>, until: DateTime<Utc>) {
        self.prune(now);
        self.0.insert(thread.clone(), Snooze { at: now, until: until.min(now + SNOOZE_MAX) });
    }

    /// End the snooze of `thread`, dropping the snoozes already past. Whether it was snoozed.
    pub fn wake(&mut self, thread: &ThreadId, now: DateTime<Utc>) -> bool {
        self.prune(now);
        self.0.remove(thread).is_some()
    }

    /// The snooze of `thread` in force at `now`, if any: neither past nor longer than [`SNOOZE_MAX`].
    pub fn get(&self, thread: &ThreadId, now: DateTime<Utc>) -> Option<&Snooze> {
        self.0.get(thread).filter(|s| now < s.until && s.until - s.at <= SNOOZE_MAX)
    }

    /// Drop the snoozes past at `now`.
    pub fn prune(&mut self, now: DateTime<Utc>) {
        self.0.retain(|_, s| now < s.until);
    }

    /// The human's own snoozes with those its assistant set, `assisted`, on the threads it has no snooze of its own in
    /// force on at `now`: the human's own always win.
    #[must_use]
    pub fn with_assisted(mut self, assisted: &Self, now: DateTime<Utc>) -> Self {
        for (thread, snooze) in &assisted.0 {
            if self.get(thread, now).is_none() {
                self.0.insert(thread.clone(), *snooze);
            }
        }
        self
    }
}

/// A human's pinned and archived threads, under its [`marks_key`] in [`THREADS_BUCKET`], shared by all its devices,
/// which update it with compare-and-set. A thread is pinned or archived, never both: pinning one unarchives it and
/// archiving one unpins it. Marks change nothing else: an archived thread keeps its messages, its work goes on and
/// its replies reach the inbox as before. A thread with neither mark may count as archived for having been idle
/// ([`ThreadMarks::idle`]), unless the human kept it since. The human's assistant marks threads for it under its own
/// key ([`assisted_marks_key`]), which readers take in only while the human's grant names it
/// ([`ThreadMarks::with_assisted`]).
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

    /// Whether `thread` is pinned or archived.
    pub fn marked(&self, thread: &ThreadId) -> bool {
        self.pinned.contains_key(thread) || self.archived.contains_key(thread)
    }

    /// The human's own marks with the pins and archives its assistant set, `assisted`, on the threads it neither
    /// pinned nor archived itself: the human's own always win.
    #[must_use]
    pub fn with_assisted(mut self, assisted: &Self) -> Self {
        for (thread, at) in &assisted.pinned {
            if !self.marked(thread) {
                self.pinned.insert(thread.clone(), *at);
            }
        }
        for (thread, archived) in &assisted.archived {
            if !self.marked(thread) {
                self.archived.insert(thread.clone(), *archived);
            }
        }
        self
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
    use crate::{done_key, marks_key, p, snoozed_key, title_key};

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
    fn an_assistant_s_marks_and_snoozes_count_only_where_the_human_has_none_of_its_own() {
        let (t1, t2, t3) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let (mut own, mut assisted) = (ThreadMarks::default(), ThreadMarks::default());
        assert!(own.pin(&t1) && own.archive(&t2, 5));
        assert!(assisted.archive(&t1, 7) && assisted.pin(&t2) && assisted.pin(&t3));
        assisted.kept.insert(t3.clone(), Utc::now());
        let merged = own.clone().with_assisted(&assisted);
        assert_eq!(merged.mark(&t1, 9), Some(Mark::Pinned), "the human's own pin wins");
        assert_eq!(merged.mark(&t2, 9), Some(Mark::Archived { new_activity: true }), "as does its own archive");
        assert_eq!(merged.mark(&t3, 9), Some(Mark::Pinned), "the assistant's pin counts where the human has none");
        assert!(merged.kept.is_empty(), "the assistant keeps no thread");
        assert_eq!(own.clone().with_assisted(&ThreadMarks::default()), own);

        let now = Utc::now();
        let hour = chrono::TimeDelta::hours(1);
        let (mut own, mut assisted) = (ThreadSnoozes::default(), ThreadSnoozes::default());
        own.snooze(&t1, now, now + hour);
        own.0.insert(t2.clone(), Snooze { at: now - hour * 3, until: now - hour });
        assisted.snooze(&t1, now, now + hour * 5);
        assisted.snooze(&t2, now, now + hour * 2);
        let merged = own.with_assisted(&assisted, now);
        assert_eq!(merged.get(&t1, now).unwrap().until, now + hour, "the human's own snooze in force wins");
        assert_eq!(merged.get(&t2, now).unwrap().until, now + hour * 2, "the assistant's, over one past");
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
    fn a_thread_is_done_until_a_message_is_stored_after_the_one_seen() {
        let done = ThreadDone::new(ThreadId::generate(), p("alice"), 10);
        assert_eq!(done_key(&done.thread_id), format!("done.{}", done.thread_id));
        assert!(done.holds(3) && done.holds(10));
        assert!(!done.holds(11), "a later message reopens it");
    }

    #[test]
    fn a_snooze_holds_until_its_end_at_most_seven_days_on_and_writes_drop_those_past() {
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        let now = Utc::now();
        let mut snoozes = ThreadSnoozes::default();
        snoozes.snooze(&t1, now, now + chrono::TimeDelta::hours(1));
        snoozes.snooze(&t2, now, now + chrono::TimeDelta::days(30));
        assert_eq!(snoozes.0[&t2].until, now + SNOOZE_MAX, "cut to seven days");
        assert!(snoozes.get(&t1, now + chrono::TimeDelta::minutes(59)).is_some());
        assert!(snoozes.get(&t1, now + chrono::TimeDelta::hours(1)).is_none(), "ended");
        snoozes.snooze(&t2, now + chrono::TimeDelta::hours(2), now + chrono::TimeDelta::hours(3));
        assert!(!snoozes.0.contains_key(&t1), "a write drops the snoozes past");
        assert!(snoozes.wake(&t2, now) && !snoozes.wake(&t2, now));
        let json = serde_json::to_string(&ThreadSnoozes(
            [(t1.clone(), Snooze { at: now, until: now + chrono::TimeDelta::days(8) })].into(),
        ))
        .unwrap();
        assert!(json.starts_with(&format!("{{\"{t1}\":{{\"at\":")), "{json}");
        let read: ThreadSnoozes = serde_json::from_str(&json).unwrap();
        assert!(read.get(&t1, now).is_none(), "longer than seven days holds not");
        assert_eq!(snoozed_key(&p("alice")), "snoozed.alice");
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
