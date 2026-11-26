//! What the entries read tell of each thread ([`Facts`]): who said what to whom on it, what it is stuck on, the
//! questions to the human no answer was read to yet and the messages in the human's inbox, which its home
//! ([`crate::home`]) and a client's view of its threads derive what waits on the human from.

use std::collections::{HashMap, HashSet};

use ants_proto::{Acting, ChannelId, EscalationReason, NoticeKind, Questions, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};

use crate::entry::{Entry, Kind};
use crate::{inbox, sealed};

/// Something on a thread that may need the human: an escalation, a daemon's notice holding the thread or a dead letter.
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
    /// The stream sequence of that envelope.
    pub seq: u64,
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
    /// For whom `from` sent it, as the reader checked its claim.
    pub acting: Acting,
    pub channel: Option<ChannelId>,
    /// Its text, or what a sealed one not opened reads in its place ([`sealed::unopened_text`]).
    pub text: String,
}

impl Inboxed {
    /// Who it is from: the human its sender acts for, else its sender.
    pub fn speaker(&self) -> &UserId {
        acting_for(&self.acting).unwrap_or(&self.from)
    }
}

/// What a thread's entries tell of it.
#[derive(Debug, Default)]
pub struct Fact {
    /// Each user's last message, post or note on it, the human an assistant sent one for among them.
    pub(crate) sent: HashMap<UserId, u64>,
    /// Each user's last inbox message on it, by sequence and id.
    pub(crate) received: HashMap<UserId, (u64, Option<Uuid>)>,
    /// The last message each ant's prompt ended with no reply sent to ([`Kind::NoReply`]): answered all the same.
    pub(crate) ended: HashMap<UserId, Uuid>,
    /// The last escalation to each user, but for tool requests, which a client reads apart.
    pub(crate) escalated: HashMap<UserId, Alert>,
    /// The last notice of each ant whose harness could not serve, its messages waiting ([`waits`]), with the user it
    /// reached.
    pub(crate) waiting: HashMap<UserId, (UserId, Alert)>,
    /// Each user's last message, post or note on it, or prompt ended with no reply sent.
    pub(crate) answered: HashMap<UserId, u64>,
    /// The last message given up on.
    pub(crate) dead: Option<Alert>,
    /// The questions to the human no answer was read to yet.
    pub(crate) questions: Vec<Asked>,
    /// Each sender and recipient a message or escalation on it went between.
    between: HashSet<(UserId, UserId)>,
    /// Who posted on it to a channel.
    posted: HashSet<UserId>,
    /// The thread's owner, as the first entry naming one says.
    pub owner: Option<UserId>,
    /// The stream sequence of its last message, post or note read ([`Kind::reopens`]); 0 for none.
    pub last_reopening: u64,
    pub latest: Option<Said>,
    /// The turns left that its latest message, post or note carries.
    pub turns: Option<u32>,
    /// Who first messaged each user on it, in the order they were first messaged: who delegated to whom.
    pub asked: Vec<(UserId, UserId)>,
    /// The messages to the human.
    pub inbox: Vec<Inboxed>,
}

impl Fact {
    /// Whether `ant` is on the thread where a human, as `is_human` tells, reaches it: it posted there to a channel, or
    /// a message or escalation there went between it and a human. An ant only other ants messaged there is not, as a
    /// reply there goes to another ant.
    pub fn reaches(&self, ant: &UserId, is_human: impl Fn(&UserId) -> bool) -> bool {
        self.posted.contains(ant)
            || self.between.iter().any(|(from, to)| (from == ant && is_human(to)) || (to == ant && is_human(from)))
    }
}

/// What the entries read tell of each thread.
#[derive(Debug, Default)]
pub struct Facts(HashMap<ThreadId, Fact>);

impl Facts {
    /// Take in `entry`, read by `me`, after those read before it in stream order. A forged one tells nothing.
    pub fn add(&mut self, entry: &Entry, me: &UserId) {
        if entry.forged {
            return;
        }
        let fact = self.0.entry(entry.thread_id.clone()).or_default();
        if fact.owner.is_none() {
            fact.owner.clone_from(&entry.owner);
        }
        if entry.kind.reopens() {
            fact.last_reopening = fact.last_reopening.max(entry.seq);
        }
        let alert = |dead| Alert { seq: entry.seq, at: entry.stored_at, text: entry.text.clone(), dead };
        if let (true, Some(from)) = (inbox::inboxed(entry, me), &entry.from) {
            fact.inbox.push(Inboxed {
                seq: entry.seq,
                at: entry.stored_at,
                kind: entry.kind,
                from: from.clone(),
                acting: entry.acting.clone(),
                channel: entry.channel.clone(),
                text: sealed::unopened_text(entry.sealed).map_or_else(|| entry.text.clone(), str::to_owned),
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
                if let (true, Some(from)) = (waits(notice), &entry.from) {
                    fact.waiting.insert(from.clone(), (to.clone(), alert(None)));
                } else if notice.holds_thread() && (!gave_up || dead.is_some()) {
                    fact.escalated.insert(to.clone(), alert(dead));
                }
            }
            return;
        }
        if let Some(from) = &entry.from
            && matches!(entry.kind, Kind::Message | Kind::Post | Kind::Note)
        {
            fact.sent.insert(from.clone(), entry.seq);
            if let Some(human) = acting_for(&entry.acting) {
                fact.sent.insert(human.clone(), entry.seq);
            }
            fact.answered.insert(from.clone(), entry.seq);
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
                fact.answered.insert(ant.clone(), entry.seq);
            }
            _ => {}
        }
        if let Some(answer) = &entry.answer {
            fact.questions.retain(|q| q.id != answer.question);
        }
        if inbox::holds(entry, me)
            && let (Some(question), Some(id), Some(from)) = (&entry.question, entry.id, &entry.from)
        {
            let asked =
                Asked { id, seq: entry.seq, at: entry.stored_at, from: from.clone(), question: question.clone() };
            fact.questions.push(asked);
        }
    }

    pub fn get(&self, thread: &ThreadId) -> Option<&Fact> {
        self.0.get(thread)
    }

    /// The threads any entry was read on.
    pub fn threads(&self) -> impl Iterator<Item = &ThreadId> {
        self.0.keys()
    }
}

/// The human an act is for, when its claim stands.
fn acting_for(acting: &Acting) -> Option<&UserId> {
    match acting {
        Acting::For(human) => Some(human),
        Acting::Own | Acting::Forged(_) => None,
    }
}

/// Whether a notice of `kind` tells of an ant's harness unable to serve, its messages waiting until it serves again.
fn waits(kind: NoticeKind) -> bool {
    matches!(kind, NoticeKind::SignedOut | NoticeKind::Quota | NoticeKind::Failing | NoticeKind::NoFallback)
}
