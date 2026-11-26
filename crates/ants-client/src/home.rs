//! What waits on a human at a time, its home ([`items`]): one item per thread under its most pressing reason
//! ([`Why`]), an ant's pending tool requests on a thread one group, the threads a snooze holds left out ([`snoozed`]).
//! Every client of the human derives it the same way from what it read ([`Source`]): the facts of its entries
//! ([`Facts`]), the tool requests waiting for the human, its read marker, done marks, reminders and snoozes.
//!
//! A thread is stuck ([`stuck`]) on an escalation to a human, a daemon's notice holding it or a dead letter that no
//! human, nor an assistant for one, said anything after, or on a notice of an ant's harness unable to serve that
//! neither a human nor that ant said anything after; finished ([`finished`]) when an ant answered the human on a
//! thread the human owns and no ant is left to answer on it.

use std::collections::HashMap;

use ants_proto::{ApprovalRequest, Questions, Reminders, ThreadId, ThreadSnoozes, UserId, Uuid, Verdict};
use chrono::{DateTime, Utc};

use crate::entry::Kind;
use crate::facts::{Alert, Asked, Fact, Facts, Inboxed};

/// What a client read that the home derives from, and what it knows beside: who the human is, who else is human,
/// what the human read, marked done or snoozed, and what the client did itself that is yet to be read back.
pub trait Source {
    /// How a tool request settled lately was settled, as the client tells it.
    type Settled: Clone;

    /// The human whose home it is.
    fn me(&self) -> &UserId;
    /// What the entries read tell of each thread.
    fn facts(&self) -> &Facts;
    /// The threads to look on, in the order the client keeps them.
    fn threads(&self) -> impl Iterator<Item = &ThreadId>;
    fn is_human(&self, user: &UserId) -> bool;
    /// Whether the human's inbox message at `seq` is unread.
    fn unread(&self, seq: u64) -> bool;
    /// Whether a done mark of the thread holds: nothing said on it reopened it since.
    fn done(&self, thread: &ThreadId) -> bool;
    /// The thread's owner.
    fn owner(&self, thread: &ThreadId) -> Option<&UserId> {
        self.facts().get(thread)?.owner.as_ref()
    }
    /// Whether an ant is busy on the thread now; none is to a client that does not hear the ants' progress.
    fn working(&self, thread: &ThreadId) -> bool {
        let _ = thread;
        false
    }
    /// Whether the dead letter at `seq` was replayed or dropped by the client, though no read tells so yet.
    fn cleared(&self, seq: u64) -> bool {
        let _ = seq;
        false
    }
    /// Whether the client sent, or is sending, an answer to the question asked in message `id`.
    fn answering(&self, id: Uuid) -> bool {
        let _ = id;
        false
    }
    /// The tool requests waiting for the human to decide, and those settled lately with how, unordered.
    fn requests(&self, now: DateTime<Utc>) -> Vec<(ApprovalRequest, Option<Self::Settled>)>;
    /// The human's reminders.
    fn reminders(&self) -> &Reminders;
    /// The snoozes keeping threads off the home: the human's own and those its assistant set for it.
    fn snoozes(&self) -> &ThreadSnoozes;
    /// What an ant asks to run with `request`, in a line.
    fn asks(&self, request: &ApprovalRequest) -> String {
        asks(request)
    }
    /// The thread's title.
    fn title(&self, thread: &ThreadId) -> String {
        thread.to_string()
    }
}

/// What an ant asks to run with `request`, in a line: `dev-a asks to run execute: git push`.
pub fn asks(request: &ApprovalRequest) -> String {
    format!("{} asks to run {}: {}", request.ant, request.tool.kind, request.tool.title)
}

/// Why a thread is on home, in the order home lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Why {
    /// A tool request waits for a decision.
    Approval,
    /// A question to the human waits for an answer.
    Question,
    /// Stuck on the human ([`stuck`]).
    Stuck,
    /// An unread message from another human.
    Message,
    /// An unread post @mentioning the human.
    Mention,
    /// A reminder the human set is due.
    Reminder,
    /// An ant answered on a thread the human owns, and the human has not read the answer yet ([`finished`]).
    Finished,
}

impl Why {
    /// Every reason, in the order home lists them.
    pub const ALL: [Self; 7] =
        [Self::Approval, Self::Question, Self::Stuck, Self::Message, Self::Mention, Self::Reminder, Self::Finished];

    pub fn label(self) -> &'static str {
        match self {
            Self::Approval => "approve",
            Self::Question => "question",
            Self::Stuck => "stuck",
            Self::Message => "message",
            Self::Mention => "mention",
            Self::Reminder => "reminder",
            Self::Finished => "finished",
        }
    }

    /// The badge of `n` reasons of this kind on a thread listed under another: `question`, `2 mentions`.
    pub fn badge(self, n: usize) -> String {
        match (self, n) {
            (Self::Message | Self::Mention, 2..) => format!("{n} {}s", self.label()),
            _ => self.label().to_owned(),
        }
    }
}

/// One thing waiting on the human, `S` how the client tells a settled tool request was settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item<S> {
    pub why: Why,
    pub thread: ThreadId,
    pub at: DateTime<Utc>,
    /// When the latest of what it stands for arrived: the last unread message or mention of several, else `at`.
    pub latest: DateTime<Utc>,
    /// Who or what, in a line.
    pub text: String,
    /// Who the line starts with.
    pub who: Option<UserId>,
    /// The stream sequence of the message it stands for: the question's, the one the thread is stuck on, the first
    /// unread message or mention, the answer read of a finished thread; none for a tool request or a reminder.
    pub seq: Option<u64>,
    /// The tool request of an approval, and how it was settled lately, if it was: of a group, its oldest.
    pub request: Option<(ApprovalRequest, Option<S>)>,
    /// The pending tool requests an approval stands for, oldest first: those of its ant on its thread, a group of
    /// them when there are several.
    pub requests: Vec<ApprovalRequest>,
    /// Whether it heads its group's requests listed one by one, as a client may list them.
    pub expanded: bool,
    /// The question asked, and the id of the message asking it, which an answer names.
    pub question: Option<(Uuid, Questions)>,
    /// How many of its reason wait on the thread: unread messages or mentions, or the requests of a group.
    pub count: usize,
    /// The thread's other, less pressing reasons, with how many of each: its badges.
    pub also: Vec<(Why, usize)>,
}

/// What an item is, whatever else changes on it: its thread, or an approval's tool request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Id {
    Thread(ThreadId),
    Request(UserId, Uuid),
    /// A group of an ant's tool requests on a thread.
    Requests(UserId, ThreadId),
}

impl<S> Item<S> {
    fn new(why: Why, thread: &ThreadId, at: DateTime<Utc>, text: String, who: Option<&UserId>) -> Self {
        Self {
            why,
            thread: thread.clone(),
            at,
            latest: at,
            text,
            who: who.cloned(),
            seq: None,
            request: None,
            requests: Vec::new(),
            expanded: false,
            question: None,
            count: 1,
            also: Vec::new(),
        }
    }

    pub fn id(&self) -> Id {
        match &self.request {
            Some((request, _)) if self.requests.len() > 1 => Id::Requests(request.ant.clone(), self.thread.clone()),
            Some((request, _)) => Id::Request(request.ant.clone(), request.id),
            None => Id::Thread(self.thread.clone()),
        }
    }

    /// Whether it is a tool request settled lately.
    pub fn settled(&self) -> bool {
        self.request.as_ref().is_some_and(|(_, settled)| settled.is_some())
    }

    /// The pending tool request a decision on it answers: none on the head of a group listed one by one.
    pub fn pending(&self) -> Option<&ApprovalRequest> {
        self.request.as_ref().filter(|(_, settled)| settled.is_none() && !self.expanded).map(|(r, _)| r)
    }

    /// The pending requests of its group of the oldest's tool kind, oldest first, which may be allowed once each: none
    /// off a group, or when the oldest does not offer allow once.
    pub fn of_kind(&self) -> Option<Vec<ApprovalRequest>> {
        let oldest = self.requests.first().filter(|_| self.requests.len() > 1)?;
        let once = |r: &ApprovalRequest| r.options.contains(&Verdict::AllowOnce);
        once(oldest)
            .then(|| self.requests.iter().filter(|r| r.tool.kind == oldest.tool.kind && once(r)).cloned().collect())
    }

    /// Whether it stands for a pending tool request of the same ant on the same thread as `other`'s.
    fn groups(&self, other: &Self) -> bool {
        fn pending<S>(i: &Item<S>) -> Option<&ApprovalRequest> {
            i.request.as_ref().filter(|(_, settled)| settled.is_none()).map(|(r, _)| r)
        }
        pending(self).zip(pending(other)).is_some_and(|(a, b)| a.ant == b.ant && a.thread_id == b.thread_id)
    }
}

/// What waits on the human at `now`, in the order home lists it: by reason ([`Why`]), the requests waiting before
/// those settled lately, oldest first.
pub fn items<H: Source>(source: &H, now: DateTime<Utc>) -> Vec<Item<H::Settled>> {
    let mut items = reasons(source, now);
    let held = held(source, &items, now);
    items.retain(|i| !held.contains_key(&i.thread));
    items.sort_by_key(|i| (i.why, i.settled(), i.at));
    grouped(items)
}

/// What waits on the human at `now`: the items but the requests settled lately.
pub fn waiting<H: Source>(source: &H, now: DateTime<Utc>) -> Vec<Item<H::Settled>> {
    items(source, now).into_iter().filter(|i| !i.settled()).collect()
}

/// The threads a snooze keeps off home at `now`, with when each snooze ends.
pub fn snoozed(source: &impl Source, now: DateTime<Utc>) -> HashMap<ThreadId, DateTime<Utc>> {
    held(source, &reasons(source, now), now)
}

/// `reasons`, sorted, as one item per thread under its most pressing reason, the others its badges; approvals stay
/// items of their own, an ant's pending requests on a thread one group.
fn grouped<S>(reasons: Vec<Item<S>>) -> Vec<Item<S>> {
    let mut items: Vec<Item<S>> = Vec::new();
    for reason in reasons {
        if let Some(group) = items.iter_mut().find(|i| i.groups(&reason)) {
            group.requests.extend(reason.requests);
            continue;
        }
        let on = |i: &&mut Item<S>| i.why != Why::Approval && i.thread == reason.thread;
        match items.iter_mut().find(on) {
            Some(top) if reason.why != Why::Approval => top.also.push((reason.why, reason.count)),
            _ => items.push(reason),
        }
    }
    for group in items.iter_mut().filter(|i| i.requests.len() > 1) {
        let oldest = &group.requests[0];
        let (n, tool) = (group.requests.len(), &oldest.tool);
        group.text = format!("{} asks to run {n} tools, the oldest {}: {}", oldest.ant, tool.kind, tool.title);
        group.count = n;
    }
    items
}

/// The threads a snooze keeps off home among `items`: it holds until an item for the human arrives on the thread
/// after it was set ([`ThreadSnoozes::holds`]).
fn held<S>(source: &impl Source, items: &[Item<S>], now: DateTime<Utc>) -> HashMap<ThreadId, DateTime<Utc>> {
    let snoozes = source.snoozes();
    let latest = |thread: &ThreadId| items.iter().filter(|i| i.thread == *thread).map(|i| i.latest).max();
    snoozes
        .0
        .keys()
        .filter(|thread| snoozes.holds(thread, now, latest(thread)))
        .filter_map(|thread| snoozes.get(thread, now).map(|s| (thread.clone(), s.until)))
        .collect()
}

/// What would wait on the human at `now` were no thread snoozed, one item for each reason, unordered. A thread marked
/// done lists only its tool requests and its reminder.
pub fn reasons<H: Source>(source: &H, now: DateTime<Utc>) -> Vec<Item<H::Settled>> {
    let mut items: Vec<Item<H::Settled>> = source
        .requests(now)
        .into_iter()
        .map(|(request, settled)| {
            let (thread, at, text) = (&request.thread_id, request.requested_at, source.asks(&request));
            let item = Item::new(Why::Approval, thread, at, text, Some(&request.ant));
            let requests = if settled.is_none() { vec![request.clone()] } else { Vec::new() };
            Item { requests, request: Some((request, settled)), ..item }
        })
        .collect();
    let me = source.me();
    for thread in source.threads().filter(|t| !t.is_activity()) {
        if source.done(thread) {
            continue;
        }
        if let Some(asked) = question(source, thread) {
            let text = format!("{}: {}", asked.from, crate::text::asking(&asked.question));
            items.push(Item {
                seq: Some(asked.seq),
                question: Some((asked.id, asked.question.clone())),
                ..Item::new(Why::Question, thread, asked.at, text, Some(&asked.from))
            });
        }
        if let Some(alert) = stuck(source, thread) {
            items.push(Item {
                seq: Some(alert.seq),
                ..Item::new(Why::Stuck, thread, alert.at, alert.text.clone(), None)
            });
        }
        let messages: Vec<&Inboxed> = unread_on(source, thread)
            .filter(|i| i.kind == Kind::Message && i.speaker() != me && source.is_human(i.speaker()))
            .collect();
        if let Some(m) = messages.first() {
            let text = format!("{}: {}", m.from, m.text);
            let item = Item::new(Why::Message, thread, m.at, text, Some(&m.from));
            let latest = messages.iter().map(|m| m.at).max().unwrap_or(m.at);
            items.push(Item { seq: Some(m.seq), count: messages.len(), latest, ..item });
        }
        let mentions: Vec<&Inboxed> = unread_on(source, thread).filter(|i| i.kind == Kind::Mention).collect();
        if let Some(m) = mentions.first() {
            let text = format!("{} in #{}: {}", m.from, m.channel.as_ref().map_or("?", |c| c.as_str()), m.text);
            let item = Item::new(Why::Mention, thread, m.at, text, Some(&m.from));
            let latest = mentions.iter().map(|m| m.at).max().unwrap_or(m.at);
            items.push(Item { seq: Some(m.seq), count: mentions.len(), latest, ..item });
        }
        if finished(source, thread)
            && let Some(latest) = source.facts().get(thread).and_then(|f| f.latest.as_ref())
            && source.unread(latest.seq)
        {
            let text = format!("{} answered", latest.from);
            let item = Item::new(Why::Finished, thread, latest.at, text, Some(&latest.from));
            items.push(Item { seq: Some(latest.seq), ..item });
        }
    }
    for (thread, reminder) in source.reminders().due(now) {
        let title = source.title(thread);
        let text = if reminder.note.is_empty() { title } else { format!("{} · {title}", reminder.note) };
        items.push(Item::new(Why::Reminder, thread, reminder.due, text, None));
    }
    items
}

/// The sequence of the last message, post or note a human, or an assistant for one, sent on the thread; 0 for none.
fn last_human(source: &impl Source, fact: &Fact) -> u64 {
    fact.sent.iter().filter(|(u, _)| source.is_human(u)).map(|(_, seq)| *seq).max().unwrap_or(0)
}

/// What the thread is stuck on: its latest escalation to a human, daemon's notice to one holding it or dead letter
/// that no human said anything after, but a dead letter replayed or dropped ([`Source::cleared`]), and the notice of
/// it; or its latest notice to a human of an ant's harness unable to serve that neither a human nor that ant said
/// anything after, the ant answering once it serves again.
pub fn stuck<'a>(source: &'a impl Source, thread: &ThreadId) -> Option<&'a Alert> {
    let fact = source.facts().get(thread)?;
    let after = last_human(source, fact);
    let escalated = fact.escalated.iter().filter(|(to, _)| source.is_human(to)).map(|(_, a)| a);
    let waiting = fact.waiting.iter().filter(|(ant, (to, a))| {
        source.is_human(to) && fact.answered.get(*ant).is_none_or(|answered| *answered < a.seq)
    });
    let cleared = |a: &Alert| a.dead.is_some_and(|seq| source.cleared(seq));
    let held = escalated.chain(&fact.dead).filter(|a| !cleared(a));
    held.chain(waiting.map(|(_, (_, a))| a)).filter(|a| a.seq > after).max_by_key(|a| a.seq)
}

/// The latest dead letter read on the thread, unless replayed or dropped ([`Source::cleared`]).
pub fn dead_letter<'a>(source: &'a impl Source, thread: &ThreadId) -> Option<&'a Alert> {
    source.facts().get(thread)?.dead.as_ref().filter(|a| a.dead.is_none_or(|seq| !source.cleared(seq)))
}

/// Whether an ant has a message on the thread it did not answer yet: sent nothing since, and its prompt did not end
/// with no reply sent.
pub fn waiting_on_ant(source: &impl Source, fact: &Fact) -> bool {
    fact.received.iter().any(|(u, (seq, id))| {
        !source.is_human(u)
            && fact.sent.get(u).is_none_or(|sent| sent < seq)
            && id.is_none_or(|id| fact.ended.get(u) != Some(&id))
    })
}

/// The latest question to the human on the thread with no answer yet, read or being sent ([`Source::answering`]).
pub fn question<'a>(source: &'a impl Source, thread: &ThreadId) -> Option<&'a Asked> {
    source.facts().get(thread)?.questions.iter().rev().find(|q| !source.answering(q.id))
}

/// The question to the human asked in the message `id` on the thread, while it has no answer yet, read or being sent.
pub fn asked<'a>(source: &'a impl Source, thread: &ThreadId, id: Uuid) -> Option<&'a Asked> {
    source.facts().get(thread)?.questions.iter().find(|q| q.id == id && !source.answering(q.id))
}

/// Whether an ant answered the human on a thread the human owns and nobody marked done, and is done with it: the
/// human's turn.
pub fn finished(source: &impl Source, thread: &ThreadId) -> bool {
    let Some(fact) = source.facts().get(thread) else { return false };
    let Some(latest) = &fact.latest else { return false };
    let me = source.me();
    !source.done(thread)
        && source.owner(thread) == Some(me)
        && latest.to == *me
        && !source.is_human(&latest.from)
        && !source.working(thread)
        && !waiting_on_ant(source, fact)
}

/// The human's unread messages on the thread.
pub fn unread_on<'a>(source: &'a impl Source, thread: &ThreadId) -> impl Iterator<Item = &'a Inboxed> {
    source.facts().get(thread).into_iter().flat_map(|f| &f.inbox).filter(|i| source.unread(i.seq))
}

#[cfg(test)]
mod tests;
