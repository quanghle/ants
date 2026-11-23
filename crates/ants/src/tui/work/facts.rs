//! What the messages read tell of each thread ([`Facts`]) and the state they put it in ([`ThreadState`]), which the
//! work view, home and the nav's counts show.

use std::collections::{HashMap, HashSet};

use ants_client::entry::{Entry, Kind};
use ants_client::inbox;
use ants_proto::{ChannelId, EscalationReason, NoticeKind, Questions, ThreadDone, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};

use super::super::dm;
use super::super::model::Data;

/// What a thread waits on, as its messages and the ants' progress tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadState {
    /// A tool request, an unread message to the human, or an ant's answer to the human's request.
    WaitingOnMe,
    /// Escalated to a human, or a message on it given up on, and no human said anything since.
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

/// Something on a thread that may need the human: an escalation or a dead letter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub text: String,
    /// The dead letter it is, or a daemon's notice of giving up on a message tells of, by stream sequence.
    pub dead: Option<u64>,
}

/// A message on a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Said {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub from: UserId,
    pub to: UserId,
}

/// Questions to the human, asked in one message, no answer was read to yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asked {
    /// The id of the questions' envelope, which an answer names.
    pub id: Uuid,
    pub at: DateTime<Utc>,
    pub from: UserId,
    pub question: Questions,
}

/// A message to the human, in its inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inboxed {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub kind: Kind,
    pub from: UserId,
    pub channel: Option<ChannelId>,
    pub text: String,
}

/// What a thread's messages tell of its state.
#[derive(Debug, Default)]
pub struct Fact {
    /// Each user's last message, post or note on it.
    sent: HashMap<UserId, u64>,
    /// Each user's last inbox message on it, by sequence and id.
    received: HashMap<UserId, (u64, Option<Uuid>)>,
    /// The last message each ant's prompt ended with no reply sent to ([`Kind::NoReply`]): answered all the same.
    ended: HashMap<UserId, Uuid>,
    /// The last escalation to each user, but for tool requests, which [`crate::live::State::approvals`] holds.
    escalated: HashMap<UserId, Alert>,
    /// The last message given up on.
    dead: Option<Alert>,
    pub latest: Option<Said>,
    /// The turns left that its latest message, post or note carries.
    pub turns: Option<u32>,
    /// Who first messaged each user on it, in the order they were first messaged: who delegated to whom.
    pub asked: Vec<(UserId, UserId)>,
    /// Each sender and recipient a message or escalation on it went between.
    between: HashSet<(UserId, UserId)>,
    /// Who posted on it to a channel.
    posted: HashSet<UserId>,
    /// The questions to the human no answer was read to yet.
    questions: Vec<Asked>,
    /// The messages to the human.
    pub inbox: Vec<Inboxed>,
}

/// What the messages read tell of each thread.
#[derive(Debug, Default)]
pub struct Facts(HashMap<ThreadId, Fact>);

impl Facts {
    pub fn add(&mut self, entry: &Entry, me: &UserId) {
        if entry.forged {
            return;
        }
        let fact = self.0.entry(entry.thread_id.clone()).or_default();
        let alert = |dead| Alert { seq: entry.seq, at: entry.stored_at, text: entry.text.clone(), dead };
        if let (true, Some(from)) = (inbox::inboxed(entry, me), &entry.from) {
            fact.inbox.push(Inboxed {
                seq: entry.seq,
                at: entry.stored_at,
                kind: entry.kind,
                from: from.clone(),
                channel: entry.channel.clone(),
                text: dm::unopened(entry.sealed).map_or_else(|| entry.text.clone(), str::to_owned),
            });
        }
        if let Some(notice) = entry.notice {
            // A daemon's notice answers nothing: only one a human has to act on reaches the inbox, and only one holding
            // the thread counts, as what the thread is stuck on.
            if let (true, Some(to)) = (notice.needs_action(), &entry.to) {
                // The notice of giving up on a message follows its dead letter: with none read before it, the
                // letter was replayed or dropped already, and the thread is held no more.
                let gave_up = notice == NoticeKind::GaveUp && entry.in_reply_to.is_some();
                let dead = fact.dead.as_ref().filter(|_| gave_up).map(|d| d.seq);
                if notice.holds_thread() && (!gave_up || dead.is_some()) {
                    fact.escalated.insert(to.clone(), alert(dead));
                }
            }
            return;
        }
        if let Some(from) = &entry.from
            && matches!(entry.kind, Kind::Message | Kind::Post | Kind::Note)
        {
            fact.sent.insert(from.clone(), entry.seq);
            fact.turns = entry.turns.or(fact.turns);
        }
        match (entry.kind, &entry.from, &entry.to) {
            (Kind::Message | Kind::Escalation, Some(from), Some(to)) => {
                fact.between.insert((from.clone(), to.clone()));
            }
            (Kind::Post, Some(from), _) => {
                fact.posted.insert(from.clone());
            }
            _ => {}
        }
        match (entry.kind, &entry.from, &entry.to) {
            (Kind::Message, Some(from), Some(to)) => {
                if !fact.asked.iter().any(|(_, asked)| asked == to) {
                    fact.asked.push((from.clone(), to.clone()));
                }
                fact.received.insert(to.clone(), (entry.seq, entry.id));
                fact.latest = Some(Said { seq: entry.seq, at: entry.stored_at, from: from.clone(), to: to.clone() });
            }
            (Kind::Escalation, _, Some(to)) if entry.reason != Some(EscalationReason::Approval) => {
                fact.escalated.insert(to.clone(), alert(None));
            }
            (Kind::Dead, _, _) => fact.dead = Some(alert(Some(entry.seq))),
            (Kind::NoReply, Some(ant), _) => {
                if let Some(end) = &entry.no_reply {
                    fact.ended.insert(ant.clone(), end.message);
                }
            }
            _ => {}
        }
        if let Some(answer) = &entry.answer {
            fact.questions.retain(|q| q.id != answer.question);
        }
        if inbox::holds(entry, me)
            && let (Some(question), Some(id), Some(from)) = (&entry.question, entry.id, &entry.from)
        {
            fact.questions.push(Asked { id, at: entry.stored_at, from: from.clone(), question: question.clone() });
        }
    }

    pub fn get(&self, thread: &ThreadId) -> Option<&Fact> {
        self.0.get(thread)
    }
}

impl Data {
    fn last_human(&self, fact: &Fact) -> u64 {
        fact.sent.iter().filter(|(u, _)| self.is_human(u)).map(|(_, seq)| *seq).max().unwrap_or(0)
    }

    /// Why the thread is stuck: its latest escalation to a human or dead letter that no human said anything after, but
    /// a dead letter replayed or dropped here, and the notice of it.
    pub fn stuck(&self, thread: &ThreadId) -> Option<&Alert> {
        let fact = self.facts.get(thread)?;
        let after = self.last_human(fact);
        let escalated = fact.escalated.iter().filter(|(to, _)| self.is_human(to)).map(|(_, a)| a);
        escalated.chain(&fact.dead).filter(|a| a.seq > after && !self.cleared(a)).max_by_key(|a| a.seq)
    }

    /// The latest dead letter read on the thread, unless replayed or dropped here.
    pub fn dead_letter(&self, thread: &ThreadId) -> Option<&Alert> {
        self.facts.get(thread)?.dead.as_ref().filter(|a| !self.cleared(a))
    }

    /// Whether `alert` is of a dead letter replayed or dropped here.
    fn cleared(&self, alert: &Alert) -> bool {
        alert.dead.is_some_and(|seq| self.dead_cleared.contains(&seq))
    }

    /// Whether `ant` is on the thread where a human reaches it: it posted there to a channel, or a message or
    /// escalation there went between it and a human. An ant only other ants messaged there is not, as a reply there
    /// goes to another ant ([`super::entries::reply_to`]).
    pub(super) fn reaches(&self, thread: &ThreadId, ant: &UserId) -> bool {
        self.facts.get(thread).is_some_and(|fact| {
            fact.posted.contains(ant)
                || fact
                    .between
                    .iter()
                    .any(|(from, to)| (from == ant && self.is_human(to)) || (to == ant && self.is_human(from)))
        })
    }

    /// Whether an ant has a message on the thread it did not answer yet: sent nothing since, and its prompt did not end
    /// with no reply sent.
    fn waiting_on_ant(&self, fact: &Fact) -> bool {
        fact.received.iter().any(|(u, (seq, id))| {
            !self.is_human(u)
                && fact.sent.get(u).is_none_or(|sent| sent < seq)
                && id.is_none_or(|id| fact.ended.get(u) != Some(&id))
        })
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

    /// The latest question to the human on the thread with no answer yet: the one the bar's `/answer` answers.
    pub fn question(&self, thread: &ThreadId) -> Option<&Asked> {
        self.facts.get(thread)?.questions.last()
    }

    /// The question to the human asked in the message `id` on the thread, while it has no answer yet.
    pub fn asked(&self, thread: &ThreadId, id: Uuid) -> Option<&Asked> {
        self.facts.get(thread)?.questions.iter().find(|q| q.id == id)
    }

    /// Whether an ant answered the human on a thread the human owns and nobody marked done, and is done with it: the
    /// human's turn.
    pub fn finished(&self, thread: &ThreadId) -> bool {
        let (Some(summary), Some(fact)) = (self.live.summary(thread), self.facts.get(thread)) else { return false };
        let Some(latest) = &fact.latest else { return false };
        self.done(thread).is_none()
            && summary.owner.as_ref() == Some(self.me())
            && latest.to == *self.me()
            && !self.is_human(&latest.from)
            && !self.live.progress.contains_key(thread)
            && !self.waiting_on_ant(fact)
    }

    /// The human's unread messages on the thread.
    pub fn unread_on(&self, thread: &ThreadId) -> impl Iterator<Item = &Inboxed> {
        self.facts.get(thread).into_iter().flat_map(|f| &f.inbox).filter(|i| self.unread(i.seq))
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
        if self.facts.get(thread).is_some_and(|f| self.waiting_on_ant(f)) {
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
