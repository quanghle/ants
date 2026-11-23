//! What waits on the human at a time ([`items`]): one item per thread under its most pressing reason ([`Why`]), an
//! ant's pending tool requests on a thread one group, the threads a snooze holds left out ([`snoozed`]).

use std::collections::HashMap;
use std::fmt::Write as _;

use ants_proto::{ApprovalRequest, Questions, ThreadId, UserId, Uuid, Verdict};
use chrono::{DateTime, Utc};

use super::super::model::Data;
use super::super::{approvals, question};
use crate::threads::thread::Kind;
use crate::tools::State;

/// Why a thread is on home, in the order home lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Why {
    /// A tool request waits for a decision.
    Approval,
    /// A question to the human waits for an answer.
    Question,
    /// Escalated to a human, or a message given up on.
    Stuck,
    /// An unread message from another human.
    Message,
    /// An unread post @mentioning the human.
    Mention,
    /// A reminder the human set is due.
    Reminder,
    /// An ant answered on a thread the human owns, and the human has not read the answer yet.
    Finished,
}

impl Why {
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

    /// The item's one-key actions but Enter, as home and the compact pane show them.
    pub fn keys(self) -> &'static str {
        match self {
            Self::Approval => "a A r answer · n reject with a note · z snooze",
            Self::Question => "1-9 answer · r reply · d done · z snooze",
            Self::Stuck => "g grant · x cancel · d done · z snooze",
            Self::Message | Self::Mention => "r reply · d done · z snooze",
            Self::Finished => "r follow up · d mark done · z snooze",
            Self::Reminder => "d done · b remind again",
        }
    }

    /// What Enter does on the item, in home.
    pub(super) fn enter(self) -> &'static str {
        match self {
            Self::Approval => "Enter details",
            Self::Finished => "Enter review",
            Self::Question => "Enter choose",
            Self::Stuck | Self::Message | Self::Mention | Self::Reminder => "Enter open",
        }
    }

    /// The badge of `n` reasons of this kind on a thread listed under another: `question`, `2 mentions`.
    pub(super) fn badge(self, n: usize) -> String {
        match (self, n) {
            (Self::Message | Self::Mention, 2..) => format!("{n} {}s", self.label()),
            _ => self.label().to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub why: Why,
    pub thread: ThreadId,
    pub at: DateTime<Utc>,
    /// Who or what, in a line.
    pub text: String,
    /// Who the line starts with, whose face goes before it.
    pub who: Option<UserId>,
    /// The tool request of an approval, and how it was settled lately, if it was: of a group, its oldest.
    pub request: Option<(ApprovalRequest, Option<State>)>,
    /// The pending tool requests an approval stands for, oldest first: those of its ant on its thread, a group of
    /// them when there are several.
    pub requests: Vec<ApprovalRequest>,
    /// Whether it heads its group's requests listed one by one.
    pub expanded: bool,
    /// The question asked, and the id of the message asking it, which its keys answer.
    pub question: Option<(Uuid, Questions)>,
    /// How many of its reason wait on the thread: unread messages or mentions.
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

impl Item {
    pub fn id(&self) -> Id {
        match &self.request {
            Some((request, _)) if self.requests.len() > 1 => Id::Requests(request.ant.clone(), self.thread.clone()),
            Some((request, _)) => Id::Request(request.ant.clone(), request.id),
            None => Id::Thread(self.thread.clone()),
        }
    }

    /// The pending tool request its keys answer: none on the head of a group listed one by one.
    pub fn pending(&self) -> Option<&ApprovalRequest> {
        self.request.as_ref().filter(|(_, settled)| settled.is_none() && !self.expanded).map(|(r, _)| r)
    }

    /// The pending requests of its group of the oldest's tool kind, oldest first, which `l` allows once each: none off
    /// a group, or when the oldest does not offer allow once.
    pub fn of_kind(&self) -> Option<Vec<ApprovalRequest>> {
        let oldest = self.requests.first().filter(|_| self.requests.len() > 1)?;
        let once = |r: &ApprovalRequest| r.options.contains(&Verdict::AllowOnce);
        once(oldest)
            .then(|| self.requests.iter().filter(|r| r.tool.kind == oldest.tool.kind && once(r)).cloned().collect())
    }

    /// Whether it stands for a pending tool request of the same ant on the same thread as `other`'s.
    fn groups(&self, other: &Item) -> bool {
        fn pending(i: &Item) -> Option<&ApprovalRequest> {
            i.request.as_ref().filter(|(_, settled)| settled.is_none()).map(|(r, _)| r)
        }
        pending(self).zip(pending(other)).is_some_and(|(a, b)| a.ant == b.ant && a.thread_id == b.thread_id)
    }
}

/// What waits on the human at `now`.
pub fn items(data: &Data, now: DateTime<Utc>) -> Vec<Item> {
    let mut items = unsnoozed(data, now);
    let held = held(data, &items, now);
    items.retain(|i| !held.contains_key(&i.thread));
    // The requests waiting for the human before those settled lately.
    items.sort_by_key(|i| (i.why, i.request.as_ref().is_some_and(|(_, settled)| settled.is_some()), i.at));
    grouped(items)
}

/// `reasons`, sorted, as one item per thread under its most pressing reason, the others its badges; approvals stay
/// items of their own, an ant's pending requests on a thread one group.
fn grouped(reasons: Vec<Item>) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::new();
    for reason in reasons {
        if let Some(group) = items.iter_mut().find(|i| i.groups(&reason)) {
            group.requests.extend(reason.requests);
            continue;
        }
        let on = |i: &&mut Item| i.why != Why::Approval && i.thread == reason.thread;
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

/// What an ant asks to run with `request`, and what assistants decided on it for their humans, which waits for the
/// humans' consent.
pub(super) fn asks(data: &Data, request: &ApprovalRequest) -> String {
    let text = format!("{} asks to run {}: {}", request.ant, request.tool.kind, request.tool.title);
    decided(data, request, text)
}

/// Who asks on `item`, the card the keys answer showing what `request`, its oldest, asks.
pub(super) fn asker(data: &Data, item: &Item, request: &ApprovalRequest) -> String {
    match item.requests.len() {
        0 | 1 => decided(data, request, format!("{} asks to run:", request.ant)),
        n => format!("{} asks to run {n} tools, the oldest:", request.ant),
    }
}

/// `text`, then what assistants decided on `request` for their humans: the human's own assistant's to confirm on the
/// page `o` opens.
fn decided(data: &Data, request: &ApprovalRequest, mut text: String) -> String {
    for held in data.live.held.get(&(request.ant.clone(), request.id)).into_iter().flatten() {
        let (assistant, human, verdict) = (&held.assistant, &held.human, ants_client::text::spelling(held.verdict));
        let _ = write!(text, " · {assistant} chose {verdict} for {human}");
        if human == data.me() {
            text.push_str(": o lists it");
        }
    }
    text
}

/// The threads a snooze keeps off home at `now`, with when each snooze ends.
pub fn snoozed(data: &Data, now: DateTime<Utc>) -> HashMap<ThreadId, DateTime<Utc>> {
    held(data, &unsnoozed(data, now), now)
}

/// The threads a snooze keeps off home among `items`: it holds until an item for the human arrives on the thread
/// after it was set.
fn held(data: &Data, items: &[Item], now: DateTime<Utc>) -> HashMap<ThreadId, DateTime<Utc>> {
    data.cards
        .snoozed
        .0
        .keys()
        .filter_map(|thread| data.cards.snoozed.get(thread, now).map(|s| (thread, s)))
        .filter(|(thread, s)| !items.iter().any(|i| i.thread == **thread && i.at > s.at))
        .map(|(thread, s)| (thread.clone(), s.until))
        .collect()
}

/// What would wait on the human at `now` were no thread snoozed, unordered.
fn unsnoozed(data: &Data, now: DateTime<Utc>) -> Vec<Item> {
    let mut items: Vec<Item> = approvals::cards(data, None, now)
        .into_iter()
        .map(|card| {
            let request = card.request;
            Item {
                why: Why::Approval,
                thread: request.thread_id.clone(),
                at: request.requested_at,
                text: asks(data, request),
                who: Some(request.ant.clone()),
                request: Some((request.clone(), card.settled.cloned())),
                requests: card.settled.map_or_else(|| vec![request.clone()], |_| Vec::new()),
                expanded: false,
                question: None,
                count: 1,
                also: Vec::new(),
            }
        })
        .collect();
    let item = |why, thread: &ThreadId, at, text, who: &UserId| Item {
        why,
        thread: thread.clone(),
        at,
        text,
        who: Some(who.clone()),
        request: None,
        requests: Vec::new(),
        expanded: false,
        question: None,
        count: 1,
        also: Vec::new(),
    };
    for thread in data.work() {
        if let Some(asked) = data.question(thread) {
            let text = format!("{}: {}", asked.from, question::asking(&asked.question));
            items.push(Item {
                question: Some((asked.id, asked.question.clone())),
                ..item(Why::Question, thread, asked.at, text, &asked.from)
            });
        }
        if let Some(alert) = data.stuck(thread) {
            items.push(Item { who: None, ..item(Why::Stuck, thread, alert.at, alert.text.clone(), data.me()) });
        }
        let me = data.me();
        let messages: Vec<_> = data
            .unread_on(thread)
            .filter(|i| i.kind == Kind::Message && i.from != *me && data.is_human(&i.from))
            .collect();
        if let Some(m) = messages.first() {
            let text = format!("{}: {}", m.from, m.text);
            items.push(Item { count: messages.len(), ..item(Why::Message, thread, m.at, text, &m.from) });
        }
        let mentions: Vec<_> = data.unread_on(thread).filter(|i| i.kind == Kind::Mention).collect();
        if let Some(m) = mentions.first() {
            let text = format!("{} in #{}: {}", m.from, m.channel.as_ref().map_or("?", |c| c.as_str()), m.text);
            items.push(Item { count: mentions.len(), ..item(Why::Mention, thread, m.at, text, &m.from) });
        }
        if data.finished(thread)
            && let Some(latest) = data.facts.get(thread).and_then(|f| f.latest.as_ref())
            && data.unread(latest.seq)
        {
            items.push(item(Why::Finished, thread, latest.at, format!("{} answered", latest.from), &latest.from));
        }
    }
    for (thread, reminder) in data.cards.reminders.due(now) {
        let title = data.title(thread);
        let text = if reminder.note.is_empty() { title } else { format!("{} · {title}", reminder.note) };
        items.push(Item { who: None, ..item(Why::Reminder, thread, reminder.due, text, data.me()) });
    }
    items
}

/// What waits on the human at `now`: the items but the requests settled lately.
pub fn waiting(data: &Data, now: DateTime<Utc>) -> Vec<Item> {
    items(data, now).into_iter().filter(|i| i.request.as_ref().is_none_or(|(_, settled)| settled.is_none())).collect()
}
