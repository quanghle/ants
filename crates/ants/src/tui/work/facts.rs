//! The state the messages read put each thread in ([`ThreadState`]), which the work view and the nav's counts show,
//! from what they tell of it ([`ants_client::facts`]) and what waits on the human there ([`ants_client::home`]).

use std::collections::HashMap;

use ants_client::facts::{Alert, Asked, Inboxed};
use ants_client::home;
use ants_proto::{ThreadDone, ThreadId, UserId, Uuid};

use super::super::model::Data;

/// What a thread waits on, as its messages and the ants' progress tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadState {
    /// A tool request, an unread message to the human, or an ant's answer to the human's request.
    WaitingOnMe,
    /// Escalated to a human, or a message on it given up on, and no human said anything since; or an ant's harness
    /// could not serve it, and the ant answered nothing since.
    Stuck,
    /// An ant is busy on it.
    Working,
    /// An ant has a message on it it did not answer yet.
    WaitingOnAnt,
    Idle,
    /// A human marked it done, and nothing was said on it since.
    Done,
}

impl ThreadState {
    /// The states, as the nav lists them.
    pub const ALL: [Self; 6] =
        [Self::WaitingOnMe, Self::Working, Self::WaitingOnAnt, Self::Stuck, Self::Idle, Self::Done];

    pub fn label(self) -> &'static str {
        match self {
            Self::WaitingOnMe => "waiting on me",
            Self::Stuck => "stuck",
            Self::Working => "working",
            Self::WaitingOnAnt => "waiting on ant",
            Self::Idle => "idle",
            Self::Done => "done",
        }
    }

    /// Where threads in this state sort, the most urgent first.
    pub(super) fn rank(self) -> u8 {
        match self {
            Self::WaitingOnMe => 0,
            Self::Stuck => 1,
            Self::Working => 2,
            Self::WaitingOnAnt => 3,
            Self::Idle => 4,
            Self::Done => 5,
        }
    }
}

impl Data {
    /// Why the thread is stuck ([`home::stuck`]).
    pub fn stuck(&self, thread: &ThreadId) -> Option<&Alert> {
        home::stuck(self, thread)
    }

    /// The latest dead letter read on the thread, unless replayed or dropped here.
    pub fn dead_letter(&self, thread: &ThreadId) -> Option<&Alert> {
        home::dead_letter(self, thread)
    }

    /// Whether `ant` is on the thread where a human reaches it ([`ants_client::facts::Fact::reaches`]): a reply there
    /// goes to another ant otherwise ([`ants_client::sending::bar::reply_to`]).
    pub(super) fn reaches(&self, thread: &ThreadId, ant: &UserId) -> bool {
        self.facts.get(thread).is_some_and(|fact| fact.reaches(ant, |u| self.is_human(u)))
    }

    /// The thread's shared done state, while nothing said on it since reopened it.
    pub fn done(&self, thread: &ThreadId) -> Option<&ThreadDone> {
        let last = self.live.summary(thread)?.last_reopening;
        self.cards.done.get(thread).filter(|d| d.holds(last))
    }

    /// Whether a question to the human on the thread has no answer yet.
    pub fn asks(&self, thread: &ThreadId) -> bool {
        self.question(thread).is_some()
    }

    /// The latest question to the human on the thread with no answer yet, read or sent here: the one the bar's
    /// `/answer` answers.
    pub fn question(&self, thread: &ThreadId) -> Option<&Asked> {
        home::question(self, thread)
    }

    /// The question to the human asked in the message `id` on the thread, while it has no answer yet, read or sent
    /// here.
    pub fn asked(&self, thread: &ThreadId, id: Uuid) -> Option<&Asked> {
        home::asked(self, thread, id)
    }

    /// Whether an ant answered the human on a thread the human owns and is done with it ([`home::finished`]).
    pub fn finished(&self, thread: &ThreadId) -> bool {
        home::finished(self, thread)
    }

    /// The human's unread messages on the thread.
    pub fn unread_on(&self, thread: &ThreadId) -> impl Iterator<Item = &Inboxed> {
        home::unread_on(self, thread)
    }

    pub fn state(&self, thread: &ThreadId) -> ThreadState {
        if self.done(thread).is_some() {
            return ThreadState::Done;
        }
        if self.stuck(thread).is_some() {
            return ThreadState::Stuck;
        }
        if self.live.approvals.iter().any(|r| r.thread_id == *thread)
            || self.asks(thread)
            || self.unread_on(thread).next().is_some()
            || self.finished(thread)
        {
            return ThreadState::WaitingOnMe;
        }
        if self.live.progress.contains_key(thread) {
            return ThreadState::Working;
        }
        if self.facts.get(thread).is_some_and(|f| home::waiting_on_ant(self, f)) {
            return ThreadState::WaitingOnAnt;
        }
        ThreadState::Idle
    }

    /// The threads read but an assistant's activity threads: the work.
    pub fn work(&self) -> impl Iterator<Item = &ThreadId> {
        self.live.order.iter().filter(|t| !t.is_activity())
    }

    /// The number of threads in each state, but an assistant's activity threads.
    pub fn counts(&self) -> HashMap<ThreadState, usize> {
        let mut counts = HashMap::new();
        for thread in self.work() {
            *counts.entry(self.state(thread)).or_default() += 1;
        }
        counts
    }
}
