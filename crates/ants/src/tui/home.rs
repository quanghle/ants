//! Home, the triage queue: what waits on the human, one item per thread under its most pressing reason with badges for
//! the others (per tool request for approvals, as cards answered where they show, [`approvals`]), the most pressing
//! reasons first and the oldest first within one, each with the one-key actions of its reason: answer ([`question`]),
//! grant, reply, follow up, cancel, mark done, snooze ([`super::snooze`]), remind ([`super::remind`]) or open. A
//! snoozed thread's items stay off home until the snooze ends or an item for the human arrives on it.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use ants_proto::{ApprovalRequest, Questions, ThreadId, UserId, Uuid, Verdict};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{ListState, Paragraph};

use super::acts::Change;
use super::approvals;
use super::command::{Conversation, GRANT};
use super::exec::Job;
use super::model::{Action, Data, Key, Model, To};
use super::select::Selection;
use super::style::{self, ago};
use super::{catchup, question};
use crate::text::excerpt;
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
    fn enter(self) -> &'static str {
        match self {
            Self::Approval => "Enter details",
            Self::Finished => "Enter review",
            Self::Question => "Enter choose",
            Self::Stuck | Self::Message | Self::Mention | Self::Reminder => "Enter open",
        }
    }

    /// The badge of `n` reasons of this kind on a thread listed under another: `question`, `2 mentions`.
    fn badge(self, n: usize) -> String {
        match (self, n) {
            (Self::Message | Self::Mention, 2..) => format!("{n} {}s", self.label()),
            _ => self.label().to_owned(),
        }
    }

    pub fn color(self) -> Color {
        match self {
            Self::Approval => Color::Yellow,
            Self::Question => Color::Cyan,
            Self::Stuck => Color::Red,
            Self::Message => Color::Blue,
            Self::Mention => Color::Magenta,
            Self::Reminder => Color::LightCyan,
            Self::Finished => Color::Green,
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
fn asks(data: &Data, request: &ApprovalRequest) -> String {
    let text = format!("{} asks to run {}: {}", request.ant, request.tool.kind, request.tool.title);
    decided(data, request, text)
}

/// Who asks on `item`, the card the keys answer showing what `request`, its oldest, asks.
fn asker(data: &Data, item: &Item, request: &ApprovalRequest) -> String {
    match item.requests.len() {
        0 | 1 => decided(data, request, format!("{} asks to run:", request.ant)),
        n => format!("{} asks to run {n} tools, the oldest:", request.ant),
    }
}

/// `text`, then what assistants decided on `request` for their humans.
fn decided(data: &Data, request: &ApprovalRequest, mut text: String) -> String {
    for held in data.live.held.get(&(request.ant.clone(), request.id)).into_iter().flatten() {
        let (assistant, human, verdict) = (&held.assistant, &held.human, crate::tools::spelling(held.verdict));
        let id = crate::tools::qualified(request);
        let _ = write!(text, " · {assistant} chose {verdict} for {human}: ants assistant confirm {id}");
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

#[derive(Debug)]
pub struct Page {
    /// The selected item: the top one until a key moves it, then kept on it as items arrive and leave.
    selected: Selection<Id>,
    /// The groups of tool requests listed one by one, by ant and thread.
    expanded: HashSet<(UserId, ThreadId)>,
    /// The cursor on the selected question item's card, while Enter has focused it.
    answering: Option<question::Cursor>,
    /// How wide the cards were drawn last, which tells whether a tool request shows whole on its card.
    width: Cell<Option<usize>>,
}

impl Default for Page {
    fn default() -> Self {
        Self { selected: Selection::on_top(), expanded: HashSet::new(), answering: None, width: Cell::new(None) }
    }
}

impl Page {
    pub const KEYS: &str =
        "j/k move · Enter open · f forward · d done · z snooze · u unread · b remind · t tree · s digest · c creds";

    /// The selected item's thread.
    pub fn thread(&self, data: &Data) -> Option<ThreadId> {
        let items = self.rows(data, Utc::now());
        items.get(self.at(&items)).map(|i| i.thread.clone())
    }

    /// The items listed at `now`: each expanded group of tool requests followed by its requests, one by one.
    fn rows(&self, data: &Data, now: DateTime<Utc>) -> Vec<Item> {
        let mut rows = Vec::new();
        for item in items(data, now) {
            let Some(group) = item.requests.first().filter(|_| item.requests.len() > 1) else {
                rows.push(item);
                continue;
            };
            if !self.expanded.contains(&(group.ant.clone(), item.thread.clone())) {
                rows.push(item);
                continue;
            }
            let one = |request: &ApprovalRequest| Item {
                at: request.requested_at,
                text: asks(data, request),
                request: Some((request.clone(), None)),
                requests: vec![request.clone()],
                count: 1,
                ..item.clone()
            };
            let ones: Vec<Item> = item.requests.iter().map(one).collect();
            let text = format!("{} asks to run {} tools", group.ant, item.requests.len());
            rows.push(Item { text, expanded: true, ..item });
            rows.extend(ones);
        }
        rows
    }

    /// Where the selected item is among `items`.
    fn at(&self, items: &[Item]) -> usize {
        self.selected.at(&items.iter().map(Item::id).collect::<Vec<_>>())
    }

    /// The cursor on the focused question card, and the thread of its question, while the question is still the
    /// selected item's.
    pub fn card(&self, data: &Data, now: DateTime<Utc>) -> Option<(&question::Cursor, ThreadId)> {
        let cursor = self.answering.as_ref()?;
        let items = self.rows(data, now);
        let item = items.get(self.at(&items))?;
        (item.question.as_ref()?.0 == cursor.question).then(|| (cursor, item.thread.clone()))
    }

    /// Take `key` on the focused question card, if any: where it leads, if anywhere.
    pub fn card_key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<Option<To>> {
        let Some((_, thread)) = self.card(data, now) else {
            self.answering = None;
            return None;
        };
        question::focused(&mut self.answering, key, &thread)
    }

    /// Type `text` pasted on the focused question card, if any: whether it took it.
    pub fn card_paste(&mut self, text: &str, data: &Data, now: DateTime<Utc>) -> bool {
        self.card(data, now).is_some() && self.answering.as_mut().is_some_and(|c| c.paste(text))
    }

    /// Forget the cards' width: the compact pane shows the top request on one line.
    pub fn compacted(&self) {
        self.width.set(None);
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let items = self.rows(data, now);
        let at = self.at(&items);
        let selected = items.get(at);
        let pending = selected
            .filter(|i| !i.expanded)
            .and_then(|i| i.request.as_ref())
            .filter(|(_, s)| s.is_none())
            .map(|(r, _)| r.clone());
        let verdict = match key {
            Key::Char('a') => Some(Verdict::AllowOnce),
            Key::Char('A') => Some(Verdict::AllowAlways),
            Key::Char('r') => Some(Verdict::Reject),
            _ => None,
        };
        let thread = || selected.map(|i| i.thread.clone());
        let reminded = selected.and_then(|i| data.cards.reminders.remind.get(&i.thread)).and_then(|r| r.message);
        let reminder = selected.filter(|i| i.why == Why::Reminder).map(|i| i.thread.clone());
        let to = match key {
            Key::Enter if selected.is_some_and(|i| i.requests.len() > 1) => {
                let group = selected?;
                let key = (group.requests[0].ant.clone(), group.thread.clone());
                if !self.expanded.remove(&key) {
                    self.expanded.insert(key);
                }
                return None;
            }
            Key::Enter if selected.is_some_and(|i| i.question.is_some()) => {
                let (id, asked) = selected?.question.as_ref()?;
                self.answering = Some(question::Cursor::new(*id, asked));
                return None;
            }
            Key::Enter => {
                return match (pending, reminder.zip(reminded)) {
                    (Some(request), _) => Some(To::Ask(request, None)),
                    (None, Some((thread, message))) => Some(To::Message(thread, message)),
                    (None, None) => thread().map(To::Thread),
                };
            }
            Key::Char('d') if reminder.is_some() => return reminder.map(To::Unremind),
            Key::Char('b') => return thread().map(|t| To::Remind(t, reminded)),
            Key::Char('a' | 'A' | 'r') if let Some(request) = pending => {
                // A request its card does not show whole opens in full, where the keys answer it.
                let verdict =
                    verdict.filter(|_| approvals::fits(&request, self.width.get().map(|w| (w, approvals::CARD_LINES))));
                return Some(To::Ask(request, verdict));
            }
            Key::Char('a' | 'A') => return None,
            Key::Char('n') => return pending.map(To::Note),
            Key::Char('1'..='9') if selected.is_some_and(|i| i.question.is_some()) => {
                let item = selected?;
                let (id, asked) = item.question.as_ref()?;
                return match question::key(key, &item.thread, *id, asked)? {
                    question::Keyed::To(to) => Some(*to),
                    question::Keyed::Focus(cursor) => {
                        self.answering = Some(cursor);
                        None
                    }
                };
            }
            Key::Char('r') => return thread().map(To::Reply),
            Key::Char('f') => return thread().map(|_| To::Bar("/forward @".to_owned())),
            Key::Char('g') => {
                let stuck = selected.filter(|i| i.why == Why::Stuck)?;
                let job = Job::Grant { thread: stuck.thread.clone(), text: GRANT.to_owned() };
                return Some(To::Do(Action::Run(job, String::new())));
            }
            Key::Char('x') => return thread().map(To::Interrupt),
            Key::Char('d') => {
                let thread = thread()?;
                let last = data.live.summary(&thread).map_or(0, |s| s.last_reopening);
                return Some(To::Do(Action::Change(thread, Change::Done(true), last)));
            }
            Key::Char('z') => return thread().map(To::Snooze),
            Key::Char('u') => return thread().map(|t| To::Unread(t, None)),
            Key::Char('t') => return thread().map(To::Tree),
            Key::Char('s') => return Some(To::Digest),
            Key::Char('c') => return Some(To::Credentials),
            Key::Char('o') => return Some(To::Held),
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at + 1,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        };
        self.selected.select(&items.iter().map(Item::id).collect::<Vec<_>>(), to);
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let data = &model.data;
        let block = style::block(format!(" Home ({}) ", items(data, now).len()), focused);
        let items = self.rows(data, now);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let mut first = vec![catchup::since_line(model, now)];
        first.extend(data.credentials.line());
        first.extend(data.held.line());
        let tall = u16::try_from(first.len() + 1).unwrap_or(u16::MAX);
        let [top, area] = Layout::vertical([Constraint::Length(tall), Constraint::Min(0)]).areas(inner);
        let mut first: Vec<Line> = first.into_iter().map(|l| style::clip(l, usize::from(top.width))).collect();
        first.push(Line::styled("─".repeat(usize::from(top.width)), style::dim()));
        frame.render_widget(Paragraph::new(first), top);
        if items.is_empty() {
            let text = match data.live.stored_at {
                None => vec![Line::raw("reading the stream…")],
                Some(_) if data.live.order.is_empty() => what_to_type(),
                Some(_) => vec![Line::raw("nothing waits on a human")],
            };
            frame.render_widget(Paragraph::new(text), area);
            return;
        }
        let selected = self.selected.drawn(&items.iter().map(Item::id).collect::<Vec<_>>());
        // The selection's bar takes the first column.
        let (full, width) = (usize::from(area.width.saturating_sub(1)), usize::from(area.width.saturating_sub(10)));
        self.width.set(Some(width));
        let cursor = self.card(data, now).map(|(cursor, _)| cursor);
        let rows: Vec<Text> = items
            .iter()
            .enumerate()
            .map(|(at, i)| {
                // The card the keys answer shows what its request asks, under who asks.
                let keyed =
                    i.request.as_ref().filter(|(_, settled)| at == selected && settled.is_none() && !i.expanded);
                let text = keyed.map_or_else(|| i.text.clone(), |(request, _)| asker(data, i, request));
                let mut lines = vec![
                    style::clip(heading(data, i, width, now), full),
                    style::clip(
                        Line::from(
                            [Span::raw("         ")]
                                .into_iter()
                                .chain(i.who.as_ref().and_then(|who| data.face(who)).into_iter().flatten())
                                .chain([Span::styled(excerpt(&text, 160), style::dim())])
                                .collect::<Vec<_>>(),
                        ),
                        full,
                    ),
                ];
                let cursor = cursor.filter(|_| at == selected);
                if let Some((id, question)) = &i.question {
                    if cursor.is_some() {
                        let rows = question::body(question, *id, cursor, width);
                        lines.extend(rows.into_iter().map(|l| Line::styled(format!("         {l}"), l.style)));
                    } else {
                        let summary = format!("         {}", excerpt(&question::summary(question), 160));
                        lines.push(Line::raw(style::fit(&summary, full)));
                    }
                }
                if let Some(cursor) = cursor {
                    lines.push(Line::styled(format!("         {}", style::hints(cursor.keys(), width)), style::dim()));
                } else if i.expanded {
                    lines.push(Line::styled("         Enter collapses", style::dim()));
                } else if let Some((request, settled)) = &i.request {
                    let card = approvals::Card { request, settled: settled.as_ref() };
                    let card = approvals::lines(
                        card,
                        model.asking.as_ref(),
                        at == selected,
                        now,
                        (width, approvals::CARD_LINES),
                    );
                    let enter = format!("Enter lists all {}", i.requests.len());
                    // Off the card the keys answer, the tool shows on the line before.
                    lines.extend(card.into_iter().skip(usize::from(keyed.is_none())).map(|line| {
                        let mut spans = vec![Span::raw("         ")];
                        spans.extend(line.spans.into_iter().map(|mut span| {
                            if i.requests.len() > 1 {
                                span.content = span.content.replace("Enter details", &enter).into();
                                span.content = span.content.replace("Enter shows them", "a, A or r shows them").into();
                            }
                            span
                        }));
                        Line::from(spans).style(line.style)
                    }));
                } else {
                    let keys = format!("{} · {}", i.why.keys(), i.why.enter());
                    lines.push(Line::styled(format!("         {}", style::hints(&keys, width)), style::dim()));
                }
                Text::from(lines)
            })
            .collect();
        let list = style::list(rows, Some(selected), focused);
        frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(Some(selected)));
    }
}

/// The first line of item `i`'s row: its reason, its thread's title cut to leave `width` columns to the draft's
/// mark, the other reasons' badges and its age.
fn heading(data: &Data, i: &Item, width: usize, now: DateTime<Utc>) -> Line<'static> {
    let mut after: Vec<Span> = data.drafts.mark(&Conversation::Thread(i.thread.clone())).into_iter().collect();
    for (why, n) in &i.also {
        after.push(Span::styled(" · ", style::dim()));
        after.push(Span::styled(why.badge(*n), Style::new().fg(why.color())));
    }
    after.push(Span::styled(format!("  {}", ago(i.at, now)), style::dim()));
    let room = width.saturating_sub(after.iter().map(Span::width).sum());
    let mut first = vec![
        Span::styled(format!("{:<9}", i.why.label()), Style::new().fg(i.why.color())),
        Span::raw(style::fit(&data.title(&i.thread), room.max(10))),
    ];
    first.extend(after);
    Line::from(first)
}

/// What home says with no thread read: what can be typed.
fn what_to_type() -> Vec<Line<'static>> {
    let row = |keys: &str, does: &str| {
        Line::from(vec![Span::styled(format!("  {keys:<21}"), Style::new().bold()), Span::raw(does.to_owned())])
    };
    vec![
        Line::raw("No thread read yet. Type:"),
        row("@ant and a text", "to ask an ant"),
        row("#channel and a text", "to post in a channel"),
        row("Ctrl-K and a text", "for the assistant or the messenger"),
        row("?", "for the keys"),
    ]
}

#[cfg(test)]
mod tests {
    use ants_proto::{ApprovalRequest, Envelope, Mention, Uuid, Verdict, subject};

    use super::*;
    use crate::live::Event;
    use crate::test_support::live::{at, message, request, user};
    use crate::threads::thread::Parser;
    use crate::tools::{State, Update};
    use crate::tui::model::Action;
    use crate::tui::test_support::model;

    #[test]
    fn a_due_reminder_is_a_home_item_opening_its_message_until_done() {
        let (thread, message) = (ThreadId::generate(), Uuid::now_v7());
        let mut m = model();
        let reminder =
            ants_proto::Reminder { at: at(0), due: at(60), message: Some(message), note: "look".into(), pushed: false };
        m.data.cards.reminders.remind(&thread, reminder);
        assert!(items(&m.data, at(59)).is_empty(), "not due yet");
        let found: Vec<(Why, String)> = items(&m.data, at(60)).into_iter().map(|i| (i.why, i.text)).collect();
        assert_eq!(found, [(Why::Reminder, format!("look · {}", m.data.title(&thread)))]);
        let now = at(60);
        m.key(Key::Enter, now);
        assert_eq!(m.page().name(), "thread", "Enter opens the message");
        m.key(Key::Esc, now);
        assert_eq!(m.key(Key::Char('d'), now), Some(Action::Change(thread, Change::Remind(None), 0)));
        assert!(items(&m.data, now).is_empty(), "done, it leaves home at once");
    }

    #[test]
    fn home_lists_approvals_then_stuck_then_mentions_then_finished_and_opens_them() {
        let (asked, mentioned, done) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut opening = message(1, &done, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        let mention = Envelope {
            thread_id: mentioned.clone(),
            mention: Some(Mention { channel: "ops".parse().unwrap(), post: Uuid::now_v7(), author: None }),
            ..Envelope::new(user("dev-b"), user("alice"), "@alice look")
        };
        let mention = Parser::default()
            .entry(3, at(3), &subject::inbox(&mention.to, &mention.from), &serde_json::to_vec(&mention).unwrap())
            .unwrap();
        m.stored(vec![opening, message(2, &done, "dev-a", "alice", "deployed"), mention], at(3));
        let push =
            ApprovalRequest { thread_id: asked.clone(), ..request("dev-a", "git push", vec![Verdict::AllowOnce]) };
        m.data.live.approval(&Update::Pending(push.clone()));
        let second = ApprovalRequest { id: Uuid::now_v7(), ..push.clone() };
        m.data.live.approval(&Update::Pending(second.clone()));
        let held = crate::tools::HeldAct {
            human: user("alice"),
            assistant: user("pa"),
            verdict: Verdict::AllowOnce,
            sent_at: at(3),
        };
        m.data.live.approval(&Update::Held(push.clone(), vec![held]));
        let whys: Vec<(Why, ThreadId)> = items(&m.data, at(4)).into_iter().map(|i| (i.why, i.thread)).collect();
        assert_eq!(
            whys,
            [(Why::Approval, asked.clone()), (Why::Mention, mentioned), (Why::Finished, done.clone())],
            "the ant's two requests on the thread one group"
        );
        assert_eq!(waiting(&m.data, at(4)).len(), 3);
        let screen = shown(&m, at(4));
        assert!(screen.contains("dev-a asks to run 2 tools, the oldest:"), "{screen}");
        assert!(screen.contains("         execute: git push"), "{screen}");
        assert!(screen.contains("a allow · r reject · n reject with a note · Enter lists all 2"), "{screen}");

        assert_eq!(m.key(Key::Enter, at(4)), None);
        assert!(m.asking.is_none(), "Enter lists the group's requests");
        let screen = shown(&m, at(4));
        let expected = format!("pa chose allow-once for alice: ants assistant confirm dev-a/{}", push.id);
        assert!(screen.contains("dev-a asks to run 2 tools") && screen.contains("Enter collapses"), "{screen}");
        let crate::tui::model::Page::Home(home) = m.page() else { panic!("home") };
        let rows: Vec<(Id, bool)> =
            home.rows(&m.data, at(4)).into_iter().map(|i| (i.id(), i.text.ends_with(&expected))).collect();
        assert_eq!(
            rows[..3],
            [
                (Id::Requests(user("dev-a"), asked), false),
                (Id::Request(user("dev-a"), push.id), true),
                (Id::Request(user("dev-a"), second.id), false)
            ],
            "the assistant's decision waits for consent"
        );
        assert_eq!(m.key(Key::Char('a'), at(4)), None);
        assert!(m.asking.is_none(), "the group's head answers nothing");

        m.key(Key::Down, at(4));
        m.key(Key::Down, at(4));
        assert_eq!(m.key(Key::Char('r'), at(4)), Some(Action::Decide(Box::new(second.clone()), Verdict::Reject)));
        assert!(shown(&m, at(4)).contains("storing the decision…"), "the card says so until stored");
        m.key(Key::Up, at(4));
        assert_eq!(m.key(Key::Char('a'), at(4)), None, "one decision stored at a time");
        assert!(m.notice_at(at(4)).is_some_and(|n| n.text.contains("answer again once it is stored")));
        m.take(Event::Decided(Ok("rejected".to_owned())), at(5));
        assert!(m.asking.is_none(), "the decision stored ends the answering");
        m.take(Event::Approval(Update::Resolved(second, State::Expired)), at(5));
        assert_eq!(items(&m.data, at(5)).len(), 4, "the settled request keeps its card a while");
        assert_eq!(waiting(&m.data, at(5)).len(), 3);
        assert_eq!(items(&m.data, at(500)).len(), 3);

        m.key(Key::Home, at(5));
        assert_eq!(m.key(Key::Enter, at(5)), None);
        assert!(m.asking.as_ref().is_some_and(|a| a.full && a.request == push), "Enter shows the request in full");
        m.key(Key::Esc, at(5));
        let mut decision = ants_proto::ApprovalDecision::new(push.id, push.ant.clone(), Verdict::AllowOnce);
        decision.by = Some(user("alice"));
        m.data.settled.deciding(&push);
        m.take(Event::Approval(Update::Resolved(push, State::Decided(decision))), at(6));
        assert_eq!(items(&m.data, at(6)).len(), 3, "a request decided here keeps no card");
        m.key(Key::End, at(500));
        assert_eq!(m.key(Key::Enter, at(500)), Some(Action::Open(done)));
    }

    fn escalation(seq: u64, thread: &ThreadId, from: &str, to: &str) -> crate::threads::thread::Entry {
        let x = ants_proto::Escalation::new(user(from), thread.clone(), ants_proto::EscalationReason::TurnBudget);
        let envelope = Envelope {
            thread_id: thread.clone(),
            escalation: Some(Box::new(x)),
            ..Envelope::new(user(from), user(to), "help")
        };
        let payload = serde_json::to_vec(&envelope).unwrap();
        Parser::default().entry(seq, at(seq.cast_signed()), &subject::inbox(&user(to), &user(from)), &payload).unwrap()
    }

    /// A request to run `command`, its input as the daemon sends it: cut at 2,000 characters.
    fn running(command: &str) -> ApprovalRequest {
        let mut push = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::Reject]);
        let input = serde_json::json!({ "command": command }).to_string();
        push.tool.input = Some(if input.len() > 2000 { format!("{}…", &input[..2000]) } else { input });
        push
    }

    #[test]
    fn the_card_the_keys_answer_shows_its_whole_command_else_its_keys_show_it_in_full() {
        let words: Vec<String> = (0..42).map(|n| format!("w{n:03}")).collect();
        let command = format!("echo {} x", words.join(" "));
        assert_eq!(command.len(), 216);
        let mut m = model();
        let push = running(&command);
        m.data.live.approvals = vec![push.clone()];
        let screen = crate::test_support::render::rendered(100, 30, |f| crate::tui::frame::draw(f, &m, at(0)));
        assert!(screen.contains("dev-a asks to run:"), "{screen}");
        assert!(words.iter().all(|w| screen.matches(w.as_str()).count() == 1), "the command shown once: {screen}");
        assert!(!screen.contains("more line"), "{screen}");
        assert_eq!(m.key(Key::Char('a'), at(0)), Some(Action::Decide(Box::new(push), Verdict::AllowOnce)));

        let words: Vec<String> = (0..500).map(|n| format!("w{n:03}")).collect();
        let mut m = model();
        let long = running(&format!("echo {}", words.join(" ")));
        m.data.live.approvals = vec![long.clone()];
        let screen = crate::test_support::render::rendered(100, 30, |f| crate::tui::frame::draw(f, &m, at(0)));
        assert!(screen.contains("more lines: Enter shows them"), "{screen}");
        assert_eq!(m.key(Key::Char('a'), at(0)), None);
        assert!(m.asking.as_ref().is_some_and(|a| a.full), "a shows the request in full");
        assert_eq!(m.key(Key::Char('a'), at(0)), Some(Action::Decide(Box::new(long), Verdict::AllowOnce)));
    }

    fn shown(m: &crate::tui::model::Model, now: DateTime<Utc>) -> String {
        crate::test_support::render::rendered(140, 30, |f| crate::tui::frame::draw(f, m, now))
    }

    #[test]
    fn a_question_comes_after_the_approvals_answers_by_number_and_enter_focuses_its_card() {
        use crate::tui::exec::Job;

        let (asked, stuck) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut question = message(2, &asked, "dev-b", "alice", "Which retry policy?");
        let options = ["exponential".to_owned(), "fixed 5s".to_owned()];
        question.question = Some(ants_proto::Question::new("Which retry policy?", &options, true).unwrap().into());
        m.stored(vec![escalation(1, &stuck, "dev-a", "alice"), question.clone()], at(2));
        let whys: Vec<Why> = items(&m.data, at(3)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Question, Why::Stuck]);
        let screen = shown(&m, at(3));
        assert!(screen.contains("question Which retry policy?"), "{screen}");
        assert!(screen.contains("         1 exponential  2 fixed 5s "), "{screen}");
        assert!(screen.contains("1-9 answer · r reply · d done · z snooze · Enter choose"), "{screen}");

        assert_eq!(m.key(Key::Char('3'), at(3)), None, "no third option");
        let answer = |pick| Job::Answer { thread: asked.clone(), question: question.id.unwrap(), picks: vec![pick] };
        let option = |n| Some(Action::Run(answer(ants_proto::Pick::option(n)), String::new()));
        assert_eq!(m.key(Key::Char('2'), at(3)), option(2), "a digit answers at once");

        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert_eq!(m.page().name(), "home", "Enter focuses the card");
        let screen = shown(&m, at(3));
        assert!(screen.contains("› 1 exponential") && screen.contains("  2 fixed 5s"), "{screen}");
        assert!(screen.contains("↑↓ move · Enter pick · 1-9 answer · Esc leave"), "{screen}");
        m.key(Key::Down, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), option(2), "Enter picks the highlighted option");
        assert!(!shown(&m, at(3)).contains("› "), "the card leaves the focus once answered");

        m.key(Key::Enter, at(3));
        assert_eq!(m.key(Key::Esc, at(3)), None);
        assert!(!shown(&m, at(3)).contains("› "), "Esc leaves the card");
        assert_eq!(m.key(Key::Down, at(3)), None);
        assert!(shown(&m, at(3)).contains("stuck    help"), "and ↑↓ move between the items again");
        m.key(Key::Home, at(3));

        m.key(Key::Enter, at(3));
        for _ in 0..2 {
            m.key(Key::Down, at(3));
        }
        assert_eq!(m.key(Key::Char('q'), at(3)), None, "q types rather than quits");
        m.paste("/later", at(3));
        assert!(shown(&m, at(3)).contains("› type an answer: q/later▏"), "keys and pastes type on the last row");
        let typed = Some(Action::Run(answer(ants_proto::Pick::Typed("q/later".into())), String::new()));
        assert_eq!(m.key(Key::Enter, at(3)), typed);
        m.key(Key::Enter, at(3));
        m.key(Key::Down, at(3));
        m.key(Key::Down, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert!(m.bar.focused && m.bar.text() == "/answer ", "an empty typed answer goes through the bar");
        m.key(Key::Esc, at(3));

        assert_eq!(m.key(Key::Char('r'), at(3)), Some(Action::Open(asked.clone())), "r replies on the thread");
        assert!(m.bar.focused);
        m.key(Key::Esc, at(3));
        m.key(Key::Esc, at(3));

        let mut answer = message(3, &asked, "alice", "dev-b", "fixed 5s");
        answer.answer =
            Some(ants_proto::Answer { question: question.id.unwrap(), picks: vec![ants_proto::Pick::option(2)] });
        m.stored(vec![answer], at(3));
        let whys: Vec<Why> = items(&m.data, at(4)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Stuck], "answered");
    }

    #[test]
    fn a_digit_on_several_questions_focuses_their_card_and_enter_on_its_last_tab_sends_every_pick() {
        use ants_proto::{Pick, Question, Questions};

        use crate::tui::exec::Job;

        let asked = ThreadId::generate();
        let mut m = model();
        let mut question = message(1, &asked, "dev-b", "alice", "two questions");
        let options = ["yes".to_owned(), "no".to_owned()];
        let (ship, tag) = (Question::new("Ship it?", &options, false), Question::new("Tag it?", &options, true));
        question.question = Some(Questions::new(vec![ship.unwrap().headed("Ship"), tag.unwrap()]).unwrap());
        m.stored(vec![question.clone()], at(1));
        let screen = shown(&m, at(2));
        assert!(screen.contains("dev-b: Ship it? (+1 more)"), "{screen}");
        assert!(screen.contains("         Ship: 1 yes  2 no · Q2: 1 yes  2 no"), "{screen}");

        assert_eq!(m.key(Key::Char('2'), at(2)), None, "a digit picks for the first question");
        let screen = shown(&m, at(2));
        assert!(screen.contains("Ship ✓  [Q2]  Submit") && screen.contains("Tag it?"), "{screen}");
        assert_eq!(m.key(Key::Char('1'), at(2)), None, "and for the next");
        assert!(shown(&m, at(2)).contains("  Q2: yes"), "the last tab shows every pick");
        let picks = vec![Pick::option(2), Pick::option(1)];
        let job = Job::Answer { thread: asked, question: question.id.unwrap(), picks };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, String::new())), "one answer to both");
    }

    #[test]
    fn home_items_carry_their_keys_and_a_snooze_holds_until_a_new_item_arrives() {
        use crate::tui::command::GRANT;
        use crate::tui::exec::Job;

        let (stuck, done) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut opening = message(1, &done, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(
            vec![
                opening,
                message(2, &done, "dev-a", "alice", "deployed"),
                message(3, &stuck, "alice", "dev-b", "review"),
                escalation(4, &stuck, "dev-b", "alice"),
            ],
            at(4),
        );
        let whys: Vec<Why> = items(&m.data, at(5)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Stuck, Why::Finished]);
        let screen = shown(&m, at(5));
        assert!(screen.contains("g grant · x cancel · d done · z snooze · Enter open"), "{screen}");
        assert!(screen.contains("r follow up · d mark done · z snooze · Enter review"), "{screen}");

        let grant = Job::Grant { thread: stuck.clone(), text: GRANT.into() };
        assert_eq!(m.key(Key::Char('g'), at(5)), Some(Action::Run(grant, String::new())));
        let cancel = Job::Cancel { thread: stuck.clone(), ants: vec![user("dev-b")] };
        assert_eq!(m.key(Key::Char('x'), at(5)), Some(Action::Run(cancel, String::new())));
        let last = m.data.live.summary(&stuck).unwrap().last_reopening;
        assert_eq!(m.key(Key::Char('d'), at(5)), Some(Action::Change(stuck.clone(), Change::Done(true), last)));

        assert_eq!(m.key(Key::Char('z'), at(5)), None);
        assert!(shown(&m, at(5)).contains("for (at most 7d): 1h"), "the footer asks how long");
        let snooze = Change::Snooze(Some(at(5 + 3600)));
        assert_eq!(m.key(Key::Enter, at(5)), Some(Action::Change(stuck.clone(), snooze, last)));
        let whys: Vec<Why> = items(&m.data, at(6)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Finished], "snoozed off home");
        assert_eq!(items(&m.data, at(5 + 3600)).len(), 2, "until the snooze ends");
        m.stored(vec![escalation(7, &stuck, "dev-b", "alice")], at(7));
        assert_eq!(items(&m.data, at(8)).len(), 2, "or a new item arrives on the thread");

        m.key(Key::End, at(8));
        assert_eq!(m.key(Key::Char('r'), at(8)), Some(Action::Open(done)));
        assert!(m.bar.focused, "the bar open to follow up on the thread");
    }

    #[test]
    fn f_opens_the_bar_to_forward_the_selected_item_s_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut opening = message(1, &thread, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(2, &thread, "dev-a", "alice", "deployed")], at(2));
        assert_eq!(m.key(Key::Char('f'), at(3)), None);
        assert_eq!((m.bar.text(), m.bar.focused, m.page().name()), ("/forward @".to_owned(), true, "home"));
        for c in "dev-b".chars() {
            m.key(Key::Char(c), at(3));
        }
        assert_eq!(m.key(Key::Enter, at(3)), None, "no TL;DR kept yet");
        let refused = &m.bar.status.as_ref().unwrap().text;
        assert!(refused.starts_with(&format!("no TL;DR of thread {thread} yet")), "{refused}");
    }

    fn mention(seq: u64, thread: &ThreadId, from: &str) -> crate::threads::thread::Entry {
        let envelope = Envelope {
            thread_id: thread.clone(),
            mention: Some(Mention { channel: "ops".parse().unwrap(), post: Uuid::now_v7(), author: None }),
            ..Envelope::new(user(from), user("alice"), "@alice look")
        };
        let subject = subject::inbox(&envelope.to, &envelope.from);
        Parser::default().entry(seq, at(seq.cast_signed()), &subject, &serde_json::to_vec(&envelope).unwrap()).unwrap()
    }

    #[test]
    fn a_thread_shows_once_under_its_most_pressing_reason_with_badges_for_the_others() {
        use crate::tui::exec::Job;

        let busy = ThreadId::generate();
        let mut m = model();
        let mut question = message(2, &busy, "dev-b", "alice", "Which retry policy?");
        question.question = Some(
            ants_proto::Question::new("Which retry policy?", &["fixed".to_owned(), "none".to_owned()], false)
                .unwrap()
                .into(),
        );
        m.stored(
            vec![
                escalation(1, &busy, "dev-a", "alice"),
                question.clone(),
                mention(3, &busy, "dev-c"),
                mention(4, &busy, "dev-d"),
            ],
            at(4),
        );
        let found: Vec<(Why, Vec<(Why, usize)>)> = items(&m.data, at(5)).into_iter().map(|i| (i.why, i.also)).collect();
        assert_eq!(found, [(Why::Question, vec![(Why::Stuck, 1), (Why::Mention, 2)])]);
        let screen = shown(&m, at(5));
        assert!(screen.contains("question help · stuck · 2 mentions"), "{screen}");
        assert!(
            screen.contains("1-9 answer · r reply · d done · z snooze · Enter choose"),
            "the top reason's: {screen}"
        );
        let picks = vec![ants_proto::Pick::option(1)];
        let job = Job::Answer { thread: busy.clone(), question: question.id.unwrap(), picks };
        assert_eq!(m.key(Key::Char('1'), at(5)), Some(Action::Run(job, String::new())));

        let mut answer = message(5, &busy, "alice", "dev-b", "fixed");
        answer.answer =
            Some(ants_proto::Answer { question: question.id.unwrap(), picks: vec![ants_proto::Pick::option(1)] });
        m.stored(vec![answer], at(5));
        let found: Vec<(Why, Vec<(Why, usize)>)> = items(&m.data, at(6)).into_iter().map(|i| (i.why, i.also)).collect();
        assert_eq!(found, [(Why::Mention, vec![])], "answered, and the human's message ends the escalation");
    }

    #[test]
    fn a_finished_thread_leaves_home_once_its_answer_is_read_and_stays_in_work() {
        let done = ThreadId::generate();
        let mut m = model();
        let mut opening = message(1, &done, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(2, &done, "dev-a", "alice", "deployed")], at(2));
        let whys: Vec<Why> = items(&m.data, at(3)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Finished]);

        m.data.marker = Some(ants_proto::ReadMarker { below: 2, read: [].into(), unread: [].into() });
        assert!(items(&m.data, at(3)).is_empty(), "read on any device, it leaves home");
        assert!(m.data.finished(&done));
        assert_eq!(m.data.state(&done), crate::tui::work::ThreadState::WaitingOnMe);

        m.data.marker.as_mut().unwrap().unmark(2);
        let whys: Vec<Why> = items(&m.data, at(3)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Finished], "marked unread, it is back");
    }

    #[test]
    fn home_opens_on_its_top_item_until_a_key_moves_the_selection() {
        let (done, asked) = (ThreadId::generate(), ThreadId::generate());
        let selected = |m: &crate::tui::model::Model| {
            let crate::tui::model::Page::Home(home) = m.page() else { panic!("home") };
            home.thread(&m.data)
        };
        let finished = || {
            let mut m = model();
            let mut opening = message(1, &done, "alice", "dev-a", "deploy");
            opening.owner = Some(user("alice"));
            m.stored(vec![opening, message(2, &done, "dev-a", "alice", "deployed")], at(2));
            shown(&m, at(3));
            assert_eq!(selected(&m), Some(done.clone()), "the finished thread on top");
            m
        };
        let push =
            ApprovalRequest { thread_id: asked.clone(), ..request("dev-a", "git push", vec![Verdict::AllowOnce]) };

        let mut m = finished();
        m.data.live.approval(&Update::Pending(push.clone()));
        let screen = shown(&m, at(3));
        let (approve, below) = (screen.find("│▌approve "), screen.find("│ finished "));
        assert!(approve.zip(below).is_some_and(|(a, f)| a < f), "{screen}");
        assert_eq!(selected(&m), Some(asked), "the request listed after the first draw selected on top");
        assert_eq!(m.key(Key::Char('a'), at(3)), Some(Action::Decide(Box::new(push.clone()), Verdict::AllowOnce)));

        let mut m = finished();
        m.key(Key::Home, at(3));
        m.data.live.approval(&Update::Pending(push));
        shown(&m, at(3));
        assert_eq!(selected(&m), Some(done), "moved by a key, it stays with its item");
    }

    #[test]
    fn the_selection_stays_on_its_item_as_items_arrive_above_and_leave() {
        let (done, stuck, other) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut opening = message(1, &done, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(2, &done, "dev-a", "alice", "deployed")], at(2));
        m.stored(vec![message(3, &stuck, "alice", "dev-b", "review"), escalation(4, &stuck, "dev-b", "alice")], at(4));
        m.key(Key::End, at(5));
        assert!(shown(&m, at(5)).contains("r follow up · d mark done"), "the finished thread selected");

        m.stored(vec![message(6, &other, "alice", "dev-c", "build"), escalation(7, &other, "dev-c", "alice")], at(7));
        let whys: Vec<Why> = items(&m.data, at(8)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Stuck, Why::Stuck, Why::Finished], "an item arrived above the selection");
        let last = m.data.live.summary(&done).unwrap().last_reopening;
        assert_eq!(m.key(Key::Char('d'), at(8)), Some(Action::Change(done, Change::Done(true), last)));

        let selected = |m: &crate::tui::model::Model| {
            let crate::tui::model::Page::Home(home) = m.page() else { panic!("home") };
            home.thread(&m.data)
        };
        m.key(Key::Home, at(8));
        m.key(Key::Down, at(8));
        assert_eq!(selected(&m), Some(other.clone()));
        m.data.cards.snoozed.snooze(&stuck, Utc::now(), Utc::now() + chrono::TimeDelta::hours(1));
        assert_eq!(selected(&m), Some(other), "on its item as one above leaves");
    }

    #[test]
    fn a_title_a_human_set_shows_ahead_of_the_suggested_one_and_the_opening_line() {
        let (done, unlisted) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut opening = message(1, &done, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        let mut answer = message(2, &done, "dev-a", "alice", "deployed");
        answer.suggested_title = Some("Site deploy".to_owned());
        m.stored(vec![opening, answer], at(2));
        assert_eq!(m.data.title(&done), "Site deploy");
        assert!(shown(&m, at(3)).contains("Site deploy"));

        m.data.cards.titles.insert(done.clone(), "Release 1.2".to_owned());
        m.data.cards.titles.insert(unlisted.clone(), "[sealed]".to_owned());
        assert_eq!(m.data.title(&done), "Release 1.2");
        assert_eq!(m.data.title(&unlisted), "[sealed]", "even without a message read");
        let screen = shown(&m, at(3));
        assert!(screen.contains("Release 1.2") && !screen.contains("Site deploy"), "{screen}");
    }

    #[test]
    fn an_unread_message_from_another_human_is_a_home_item_after_stuck_ones() {
        let (asked, stuck, ants) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.stored(
            vec![
                message(1, &asked, "bob", "alice", "lunch?"),
                message(2, &stuck, "alice", "dev-b", "review"),
                escalation(3, &stuck, "dev-b", "alice"),
                message(4, &ants, "dev-a", "alice", "fyi"),
                message(5, &asked, "alice", "alice", "note to self"),
            ],
            at(5),
        );
        let found: Vec<(Why, ThreadId, String)> =
            items(&m.data, at(6)).into_iter().map(|i| (i.why, i.thread, i.text)).collect();
        assert_eq!(found[1..], [(Why::Message, asked.clone(), "bob: lunch?".to_owned())]);
        assert_eq!(found[0].0, Why::Stuck);
        let screen = shown(&m, at(6));
        assert!(screen.contains("message"), "{screen}");
        assert!(screen.contains("r reply · d done · z snooze · Enter open"), "{screen}");

        m.key(Key::End, at(6));
        let last = m.data.live.summary(&asked).unwrap().last_reopening;
        assert_eq!(m.key(Key::Char('d'), at(6)), Some(Action::Change(asked.clone(), Change::Done(true), last)));
        assert_eq!(m.key(Key::Enter, at(6)), Some(Action::Open(asked)));
        m.key(Key::Esc, at(6));
        m.data.marker = Some(ants_proto::ReadMarker { below: 0, read: [1].into(), ..Default::default() });
        assert_eq!(items(&m.data, at(6)).len(), 1, "read, it leaves home");
    }
}
