//! The replies on a thread folded into one notice: each waits until the thread has been quiet for the human's
//! `completion_window`, and at most [`MAX_WINDOWS`] windows after the first, so an ant that works in several replies
//! pushes once, when it seems to be done.

use std::collections::HashMap;
use std::time::Duration;

use ants_proto::ThreadId;
use chrono::{DateTime, Utc};

use super::notices::{About, Notice, counted};

/// How many windows after its first reply a thread's notice waits at most, however busy the thread stays.
const MAX_WINDOWS: u32 = 5;

/// The replies waiting, by thread.
#[derive(Debug, Default)]
pub(super) struct Replies(HashMap<ThreadId, Waiting>);

#[derive(Debug)]
struct Waiting {
    /// The notice of the latest reply.
    notice: Notice,
    /// The stream sequence of the latest reply.
    seq: u64,
    first: DateTime<Utc>,
    last: DateTime<Utc>,
    count: u32,
}

impl Replies {
    /// Add the completion `notice` of the message at `seq`, taken at `now`.
    pub(super) fn add(&mut self, notice: Notice, seq: u64, now: DateTime<Utc>) {
        let About::Completion { thread, .. } = &notice.about else { return };
        let thread = thread.clone();
        let count = self.0.get(&thread).map_or(0, |waiting| waiting.count).saturating_add(1);
        let first = self.0.get(&thread).map_or(now, |waiting| waiting.first);
        self.0.insert(thread, Waiting { notice, seq, first, last: now, count });
    }

    /// Take the notices due at `now` with `window`, each with the stream sequence of its latest reply: those of
    /// threads quiet for `window`, or waiting for [`MAX_WINDOWS`] of it.
    pub(super) fn due(&mut self, now: DateTime<Utc>, window: Duration) -> Vec<(Notice, u64)> {
        let window = chrono::TimeDelta::from_std(window).unwrap_or(chrono::TimeDelta::hours(1));
        let due = |w: &Waiting| now >= w.last + window || now >= w.first + window * MAX_WINDOWS.cast_signed();
        let threads: Vec<ThreadId> = self.0.iter().filter(|(_, w)| due(w)).map(|(thread, _)| thread.clone()).collect();
        threads.into_iter().filter_map(|thread| self.0.remove(&thread)).map(Waiting::folded).collect()
    }
}

impl Waiting {
    /// The notice of the latest reply, telling how many replies it folds.
    fn folded(mut self) -> (Notice, u64) {
        if let About::Completion { from, replies, .. } = &mut self.notice.about {
            *replies = self.count;
            if self.count > 1 {
                self.notice.title = format!("{from} replied ({} on the thread)", counted("completion", self.count));
            }
        }
        (self.notice, self.seq)
    }
}
