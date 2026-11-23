//! Home, the triage queue: what waits on the human, one item per thread under its most pressing reason with badges for
//! the others (per tool request for approvals, as cards answered where they show, [`approvals`]), the most pressing
//! reasons first and the oldest first within one, each with the one-key actions of its reason: answer ([`question`]),
//! grant, reply, follow up, cancel, mark done, snooze ([`super::snooze`]), remind ([`super::remind`]) or open. A
//! snoozed thread's items stay off home until the snooze ends or an item for the human arrives on it.

use std::cell::Cell;
use std::collections::HashSet;

use ants_proto::{ApprovalRequest, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Paragraph;

use super::acts::Change;
use super::approvals;
use super::armed::Armed;
use super::command::{Conversation, GRANT};
use super::exec::Job;
use super::model::{Action, Data, Key, Model, To, Ui};
use super::select::Selection;
use super::style::{self, ago};
use super::{catchup, question};
use crate::text::excerpt;

mod items;

pub use items::{Id, Item, Why, items, snoozed, waiting};
use items::{asker, asks};

impl Why {
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

#[derive(Debug)]
pub struct Page {
    /// The selected item: the top one until a key moves it, then kept on it as items arrive and leave.
    selected: Selection<Id>,
    /// The groups of tool requests listed one by one, by ant and thread.
    expanded: HashSet<(UserId, ThreadId)>,
    /// The focus on the selected question item's card.
    answering: question::Focus,
    /// How wide the card the keys answer was drawn last and over how many lines what its request asks, which tells
    /// whether the request shows whole on screen; none on the compact pane.
    room: Cell<Option<(usize, usize)>>,
    /// The request of the card the keys answer, as last drawn.
    carded: Cell<Option<Uuid>>,
    /// The requests of a group `l` asked to allow once each, which `y` next confirms.
    armed: Armed<Vec<ApprovalRequest>>,
}

impl Default for Page {
    fn default() -> Self {
        Self {
            selected: Selection::on_top(),
            expanded: HashSet::new(),
            answering: question::Focus::default(),
            room: Cell::new(None),
            carded: Cell::new(None),
            armed: Armed::default(),
        }
    }
}

impl Page {
    pub fn mouse_question(&mut self, question: Uuid, at: usize, data: &Data, now: DateTime<Utc>) -> bool {
        if self.card(data, now).is_none() {
            self.key(Key::Enter, data, now);
        }
        self.answering.mouse_select(question, at)
    }

    /// The selected item's thread at `now`.
    pub fn thread(&self, data: &Data, now: DateTime<Utc>) -> Option<ThreadId> {
        let items = self.rows(data, now);
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

    /// The selected item at `now`, if any.
    pub fn selected(&self, data: &Data, now: DateTime<Utc>) -> Option<Item> {
        let items = self.rows(data, now);
        let at = self.at(&items);
        items.into_iter().nth(at)
    }

    /// Where the selected item is among `items`.
    fn at(&self, items: &[Item]) -> usize {
        self.selected.at(&items.iter().map(Item::id).collect::<Vec<_>>())
    }

    /// The cursor on the focused question card, and the thread of its question, while the question is still the
    /// selected item's.
    pub fn card(&self, data: &Data, now: DateTime<Utc>) -> Option<(&question::Cursor, ThreadId)> {
        let cursor = self.answering.cursor()?;
        let items = self.rows(data, now);
        let item = items.get(self.at(&items))?;
        (item.question.as_ref()?.0 == cursor.question).then(|| (cursor, item.thread.clone()))
    }

    /// The focused card's question on the selected thread, else the thread's latest waiting question; never a newer
    /// question in place of one answered elsewhere while its typed answer was being composed.
    pub fn question<'a>(&self, data: &'a Data, now: DateTime<Utc>) -> Option<&'a super::work::Asked> {
        let thread = self.thread(data, now)?;
        match self.answering.cursor() {
            Some(cursor) => data.asked(&thread, cursor.question),
            None => data.question(&thread),
        }
    }

    /// Take `key` on the focused question card, if any: where it leads, if anywhere.
    pub fn card_key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<Option<To>> {
        let thread = self.card(data, now).map(|(_, thread)| thread);
        self.answering.key(key, thread.as_ref())
    }

    /// Type `text` pasted on the focused question card, if any: whether it took it.
    pub fn card_paste(&mut self, text: &str, data: &Data, now: DateTime<Utc>) -> bool {
        let shown = self.card(data, now).is_some();
        self.answering.paste(text, shown)
    }

    /// The keys of the allow-all `l` asked to confirm, while it waits.
    pub fn prompt(&self) -> Option<String> {
        let batch = self.armed.get()?;
        let oldest = batch.first()?;
        let (n, what) = (batch.len(), if batch.len() == 1 { "request" } else { "requests" });
        Some(format!(
            "y allows once {n} {} {what} of {} on the thread · any other key leaves them waiting",
            oldest.tool.kind, oldest.ant
        ))
    }

    /// Drop the allow-all asked to confirm, if any: whether there was one.
    pub fn disarm(&mut self) -> bool {
        self.armed.disarm()
    }

    /// Forget the cards' width: the compact pane shows the top request on one line.
    pub fn compacted(&self) {
        self.room.set(None);
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let armed = self.armed.take();
        if key == Key::Char('y')
            && let Some(batch) = armed
        {
            return Some(To::AllowAll(batch));
        }
        let items = self.rows(data, now);
        let at = self.at(&items);
        let selected = items.get(at);
        let pending = selected.and_then(Item::pending).cloned();
        let thread = || selected.map(|i| i.thread.clone());
        let reminded = selected.and_then(|i| data.cards.reminders.remind.get(&i.thread)).and_then(|r| r.message);
        let reminder = selected.filter(|i| i.why == Why::Reminder).map(|i| i.thread.clone());
        match key {
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
                self.answering.set(Some(question::Cursor::new(*id, asked)));
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
                let drawn = self.carded.get() == Some(request.id);
                let room = self.room.get().map(|(width, rows)| (width, if drawn { rows } else { 0 }));
                return Some(approvals::keyed(request, key, room));
            }
            Key::Char('a' | 'A') => return None,
            Key::Char('l') => {
                let Some(batch) = selected.and_then(Item::of_kind) else {
                    return Some(To::Refused("l allows all of a kind on a group of tool requests".to_owned()));
                };
                self.armed.arm(Some(batch));
                return None;
            }
            Key::Char('n') => return pending.map(To::Note),
            Key::Char('1'..='9') if selected.is_some_and(|i| i.question.is_some()) => {
                let item = selected?;
                let (id, asked) = item.question.as_ref()?;
                return self.answering.keyed(question::key(key, &item.thread, *id, asked));
            }
            Key::Char('r') => return thread().map(To::Reply),
            Key::Char('f') => return thread().map(|_| To::Forward),
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
            _ => _ = self.selected.step(key, &items.iter().map(Item::id).collect::<Vec<_>>(), None),
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let (data, ui) = (&model.data, &model.ui);
        let block = style::block(format!(" Home ({}) ", items(data, now).len()), focused);
        let items = self.rows(data, now);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let mut first = vec![catchup::since_line(model, now)];
        first.extend(data.credentials.line());
        first.extend(super::held::line(data));
        let tall = u16::try_from(first.len() + 1).unwrap_or(u16::MAX);
        let [top, area] = Layout::vertical([Constraint::Length(tall), Constraint::Min(0)]).areas(inner);
        let mut first: Vec<Line> = first.into_iter().map(|l| style::clip(l, usize::from(top.width))).collect();
        first.push(Line::styled("─".repeat(usize::from(top.width)), style::dim()));
        frame.render_widget(Paragraph::new(first), top);
        if items.is_empty() {
            let text = match data.live.stored_at {
                None => vec![Line::raw(style::READING)],
                Some(_) if data.live.order.is_empty() => what_to_type(),
                Some(_) => vec![Line::raw("nothing waits on a human")],
            };
            frame.render_widget(Paragraph::new(text), area);
            return;
        }
        let selected = self.selected.drawn(&items.iter().map(Item::id).collect::<Vec<_>>());
        // The selection's bar takes the first column.
        let (full, width) = (usize::from(area.width.saturating_sub(1)), usize::from(area.width.saturating_sub(10)));
        let (mut room, mut carded) = ((width, 0), None);
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
                    style::clip(heading(data, ui, i, width, now), full),
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
                    let enter = (i.requests.len() > 1).then(|| group_keys(i));
                    let card = |rows| {
                        let (keyed, room) = ((at == selected, false), (width, rows));
                        approvals::lines(card, model.asking(), keyed, now, room, enter.as_deref())
                    };
                    // The card the keys answer gives what its request asks the lines left on screen by its others,
                    // none when even those do not show.
                    let rows = if keyed.is_some() {
                        let height = usize::from(area.height);
                        let fit = (0..=approvals::CARD_LINES).rev().find(|r| lines.len() + card(*r).len() <= height);
                        (room, carded) = ((width, fit.unwrap_or(0)), Some(request.id));
                        room.1
                    } else {
                        approvals::CARD_LINES
                    };
                    // Off the card the keys answer, the tool shows on the line before.
                    lines.extend(card(rows).into_iter().skip(usize::from(keyed.is_none())).map(|line| {
                        let spans: Vec<Span> = [Span::raw("         ")].into_iter().chain(line.spans).collect();
                        Line::from(spans).style(line.style)
                    }));
                } else {
                    let keys = format!("{} · {}", i.why.keys(), i.why.enter());
                    lines.push(Line::styled(format!("         {}", style::hints(&keys, width)), style::dim()));
                }
                Text::from(lines)
            })
            .collect();
        self.room.set(Some(room));
        self.carded.set(carded);
        draw_list(frame, area, &ui.mouse, rows, selected, focused, cursor.map(|c| c.question));
    }
}

fn draw_list(
    frame: &mut Frame,
    area: Rect,
    mouse: &super::mouse::Regions,
    rows: Vec<Text<'static>>,
    selected: usize,
    focused: bool,
    question: Option<Uuid>,
) {
    let question_lines = question.map(|id| {
        let start: usize = rows.iter().take(selected).map(|row| row.lines.len()).sum();
        (id, start, rows[selected].lines.clone())
    });
    let top = super::mouse::list(frame, area, mouse, super::mouse::Viewport::Home, rows, selected, focused);
    if let Some((id, start, lines)) = question_lines {
        let offset = u16::try_from(start.saturating_sub(top)).unwrap_or(u16::MAX);
        let area = Rect { y: area.y.saturating_add(offset), height: area.height.saturating_sub(offset), ..area };
        question::mouse_regions(mouse, area, &lines, top.saturating_sub(start), id);
    }
}

/// What a group's card offers past a, A, r and n: `l` allowing all of the oldest's kind, and Enter listing them.
fn group_keys(i: &Item) -> String {
    let all = i.requests.len();
    match i.of_kind() {
        Some(batch) => format!("l allow {} {} · Enter lists all {all}", batch.len(), batch[0].tool.kind),
        None => format!("Enter lists all {all}"),
    }
}

/// The first line of item `i`'s row: its reason, its thread's title cut to leave `width` columns to the draft's
/// mark, the other reasons' badges and its age.
fn heading(data: &Data, ui: &Ui, i: &Item, width: usize, now: DateTime<Utc>) -> Line<'static> {
    let mut after: Vec<Span> = ui.drafts.mark(&Conversation::Thread(i.thread.clone())).into_iter().collect();
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
    use crate::tui::test_support::{model, screen};

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
        let shown = screen(&m, 140, 30, at(4));
        assert!(shown.contains("dev-a asks to run 2 tools, the oldest:"), "{shown}");
        assert!(shown.contains("         execute: git push"), "{shown}");
        assert!(
            shown.contains("a allow · r reject · n reject with a note · l allow 2 execute · Enter lists all 2"),
            "{shown}"
        );

        assert_eq!(m.key(Key::Enter, at(4)), None);
        assert!(m.asking().is_none(), "Enter lists the group's requests");
        let shown = screen(&m, 140, 30, at(4));
        let expected = "pa chose allow-once for alice: o lists it";
        assert!(shown.contains("dev-a asks to run 2 tools") && shown.contains("Enter collapses"), "{shown}");
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
        assert!(m.asking().is_none(), "the group's head answers nothing");

        m.key(Key::Down, at(4));
        m.key(Key::Down, at(4));
        assert_eq!(m.key(Key::Char('r'), at(4)), None, "a card not drawn yet shows its request in full");
        m.key(Key::Esc, at(4));
        screen(&m, 140, 30, at(4));
        assert_eq!(m.key(Key::Char('r'), at(4)), Some(Action::Decide(Box::new(second.clone()), Verdict::Reject)));
        assert!(screen(&m, 140, 30, at(4)).contains("storing the decision…"), "the card says so until stored");
        m.key(Key::Up, at(4));
        screen(&m, 140, 30, at(4));
        assert_eq!(m.key(Key::Char('a'), at(4)), None, "one decision stored at a time");
        assert!(m.notice_at(at(4)).is_some_and(|n| n.text.contains("answer again once it is stored")));
        m.take(Event::Decided(Ok("rejected".to_owned())), at(5));
        assert!(m.asking().is_none(), "the decision stored ends the answering");
        m.take(Event::Approval(Update::Resolved(second, State::Expired)), at(5));
        assert_eq!(items(&m.data, at(5)).len(), 4, "the settled request keeps its card a while");
        assert_eq!(waiting(&m.data, at(5)).len(), 3);
        assert_eq!(items(&m.data, at(500)).len(), 3);

        m.key(Key::Home, at(5));
        assert_eq!(m.key(Key::Enter, at(5)), None);
        assert!(m.asking().is_some_and(|a| a.full && a.request == push), "Enter shows the request in full");
        m.key(Key::Esc, at(5));
        let mut decision = ants_proto::ApprovalDecision::new(push.id, push.ant.clone(), Verdict::AllowOnce);
        decision.by = Some(user("alice"));
        m.data.settled.deciding(&push);
        m.take(Event::Approval(Update::Resolved(push, State::Decided(decision))), at(6));
        assert_eq!(items(&m.data, at(6)).len(), 3, "a request decided here keeps no card");
        m.key(Key::End, at(500));
        assert_eq!(m.key(Key::Enter, at(500)), Some(Action::Open(done)));
    }

    #[test]
    fn l_asks_to_allow_once_each_request_of_the_oldest_s_kind_in_a_group_and_y_confirms_one_batch() {
        let thread = ThreadId::generate();
        let mut m = model();
        let on = |title: &str, kind: &str, n: i64| {
            let mut r =
                ApprovalRequest { thread_id: thread.clone(), ..request("dev-a", title, vec![Verdict::AllowOnce]) };
            r.tool.kind = kind.to_owned();
            r.requested_at = at(n);
            r
        };
        let (push, edit, test) =
            (on("git push", "execute", 0), on("main.rs", "edit", 1), on("cargo test", "execute", 2));
        for r in [&push, &edit, &test] {
            m.data.live.approval(&Update::Pending(r.clone()));
        }
        let lone = request("dev-b", "ls", vec![Verdict::AllowOnce]);
        m.data.live.approval(&Update::Pending(ApprovalRequest { requested_at: at(3), ..lone }));
        let shown = screen(&m, 140, 30, at(4));
        assert!(shown.contains("l allow 2 execute · Enter lists all 3"), "{shown}");

        assert_eq!(m.key(Key::Char('l'), at(4)), None);
        let shown = screen(&m, 140, 30, at(4));
        assert!(shown.contains("y allows once 2 execute requests of dev-a on the thread"), "{shown}");
        assert!(m.asking().is_none(), "nothing stored before y");
        m.key(Key::Esc, at(4));
        assert!(!screen(&m, 140, 30, at(4)).contains("y allows once"), "Esc drops it");
        assert_eq!(m.key(Key::Char('y'), at(4)), None, "y alone allows nothing");

        m.key(Key::Char('l'), at(4));
        assert_eq!(m.key(Key::Char('y'), at(4)), Some(Action::AllowAll(vec![push.clone(), test.clone()])));
        let asking = m.asking().expect("the batch being stored");
        assert!(asking.storing && asking.request == push && asking.batch == [test], "{asking:?}");
        assert!(
            screen(&m, 140, 30, at(4)).contains("allowing them once, one by one…"),
            "the card says so until stored"
        );
        assert_eq!(m.key(Key::Char('a'), at(4)), None, "one decision stored at a time");
        assert!(m.notice_at(at(4)).is_some_and(|n| n.text.contains("answer again once it is stored")));
        m.take(Event::Approval(Update::Resolved(push, State::Expired)), at(5));
        assert!(m.asking().is_some(), "word of a request of the batch keeps it storing");
        let line = "allowed once 1 of 2 execute requests of dev-a; 1 answered already";
        m.key(Key::Up, at(5));
        m.take(Event::Decided(Ok(line.to_owned())), at(5));
        assert!(m.asking().is_none(), "the batch stored ends the answering");
        assert_eq!(m.notice_at(at(5)).map(|n| n.text.clone()), Some(line.to_owned()), "one combined line");

        m.key(Key::End, at(5));
        assert_eq!(m.key(Key::Char('l'), at(5)), None, "a lone request is no group");
        assert!(!screen(&m, 140, 30, at(5)).contains("y allows once"));
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
        let shown = crate::test_support::render::rendered(100, 30, |f| crate::tui::frame::draw(f, &m, at(0)));
        assert!(shown.contains("dev-a asks to run:"), "{shown}");
        assert!(words.iter().all(|w| shown.matches(w.as_str()).count() == 1), "the command shown once: {shown}");
        assert!(!shown.contains("more line"), "{shown}");
        assert_eq!(m.key(Key::Char('a'), at(0)), Some(Action::Decide(Box::new(push), Verdict::AllowOnce)));

        let words: Vec<String> = (0..500).map(|n| format!("w{n:03}")).collect();
        let mut m = model();
        let long = running(&format!("echo {}", words.join(" ")));
        m.data.live.approvals = vec![long.clone()];
        let shown = crate::test_support::render::rendered(100, 30, |f| crate::tui::frame::draw(f, &m, at(0)));
        assert!(shown.contains("more lines: Enter shows them"), "{shown}");
        assert_eq!(m.key(Key::Char('a'), at(0)), None);
        assert!(m.asking().is_some_and(|a| a.full), "a shows the request in full");
        assert_eq!(m.key(Key::Char('a'), at(0)), Some(Action::Decide(Box::new(long), Verdict::AllowOnce)));
    }

    #[test]
    fn a_request_its_card_does_not_show_whole_on_a_short_screen_opens_in_full_on_its_keys() {
        let words: Vec<String> = (0..60).map(|n| format!("w{n:03}")).collect();
        let command = format!("echo {}", words.join(" "));
        let push = running(&command);
        let mut m = model();
        m.data.live.approvals = vec![push.clone()];
        let shown = crate::test_support::render::rendered(100, 30, |f| crate::tui::frame::draw(f, &m, at(0)));
        assert!(words.iter().all(|w| shown.contains(w.as_str())), "whole on a tall shown: {shown}");
        assert_eq!(m.key(Key::Char('a'), at(0)), Some(Action::Decide(Box::new(push.clone()), Verdict::AllowOnce)));
        for height in [16, 9, 6] {
            let mut m = model();
            m.data.live.approvals = vec![push.clone()];
            let shown = crate::test_support::render::rendered(100, height, |f| crate::tui::frame::draw(f, &m, at(0)));
            assert!(!shown.contains("w059"), "{shown}");
            assert_eq!(m.key(Key::Char('a'), at(0)), None, "{shown}");
            assert!(m.asking().is_some_and(|a| a.full), "a shows the request in full: {shown}");
        }
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
        let shown = screen(&m, 140, 30, at(3));
        assert!(shown.contains("question Which retry policy?"), "{shown}");
        assert!(shown.contains("         1 exponential  2 fixed 5s "), "{shown}");
        assert!(shown.contains("1-9 answer · r reply · d done · z snooze · Enter choose"), "{shown}");

        assert_eq!(m.key(Key::Char('3'), at(3)), None, "no third option");
        let answer = |pick| Job::Answer { thread: asked.clone(), question: question.id.unwrap(), picks: vec![pick] };
        let option = |n| Some(Action::Run(answer(ants_proto::Pick::option(n)), String::new()));
        assert_eq!(m.key(Key::Char('2'), at(3)), option(2), "a digit answers at once");

        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert_eq!(m.page().name(), "home", "Enter focuses the card");
        let shown = screen(&m, 140, 30, at(3));
        assert!(shown.contains("› 1 exponential") && shown.contains("  2 fixed 5s"), "{shown}");
        assert!(shown.contains("↑↓ move · Enter pick · 1-9 answer · Esc leave"), "{shown}");
        m.key(Key::Down, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), option(2), "Enter picks the highlighted option");
        assert!(!screen(&m, 140, 30, at(3)).contains("› "), "the card leaves the focus once answered");

        m.key(Key::Enter, at(3));
        assert_eq!(m.key(Key::Esc, at(3)), None);
        assert!(!screen(&m, 140, 30, at(3)).contains("› "), "Esc leaves the card");
        assert_eq!(m.key(Key::Down, at(3)), None);
        assert!(screen(&m, 140, 30, at(3)).contains("stuck    help"), "and ↑↓ move between the items again");
        m.key(Key::Home, at(3));

        m.key(Key::Enter, at(3));
        for _ in 0..2 {
            m.key(Key::Down, at(3));
        }
        assert_eq!(m.key(Key::Char('q'), at(3)), None, "q types rather than quits");
        m.paste("/later", at(3));
        assert!(
            screen(&m, 140, 30, at(3)).contains("› type an answer: q/later▏"),
            "keys and pastes type on the last row"
        );
        let typed = Some(Action::Run(answer(ants_proto::Pick::Typed("q/later".into())), String::new()));
        assert_eq!(m.key(Key::Enter, at(3)), typed);
        m.key(Key::Enter, at(3));
        m.key(Key::Down, at(3));
        m.key(Key::Down, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert!(m.ui.bar.focused && m.ui.bar.text() == "/answer ", "an empty typed answer goes through the bar");
        m.key(Key::Esc, at(3));
        assert!(
            screen(&m, 140, 30, at(3)).contains("› type an answer"),
            "the card keeps the focus once the bar leaves"
        );
        m.key(Key::Esc, at(5));

        assert_eq!(m.key(Key::Char('r'), at(3)), Some(Action::Open(asked.clone())), "r replies on the thread");
        assert!(m.ui.bar.focused);
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
        let shown = screen(&m, 140, 30, at(2));
        assert!(shown.contains("dev-b: Ship it? (+1 more)"), "{shown}");
        assert!(shown.contains("         Ship: 1 yes  2 no · Q2: 1 yes  2 no"), "{shown}");

        assert_eq!(m.key(Key::Char('2'), at(2)), None, "a digit picks for the first question");
        let shown = screen(&m, 140, 30, at(2));
        assert!(shown.contains("Ship ✓  [Q2]  Submit") && shown.contains("Tag it?"), "{shown}");
        assert_eq!(m.key(Key::Char('1'), at(2)), None, "and for the next");
        assert!(screen(&m, 140, 30, at(2)).contains("  Q2: yes"), "the last tab shows every pick");
        let picks = vec![Pick::option(2), Pick::option(1)];
        let job = Job::Answer { thread: asked, question: question.id.unwrap(), picks };
        assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, String::new())), "one answer to both");
    }

    #[test]
    fn a_focused_card_keeps_the_focus_through_the_bar_and_a_card_gone_takes_the_key_meant_for_it() {
        let (first, second) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let asking = |seq, thread: &ThreadId, text: &str| {
            let mut asks = message(seq, thread, "dev-b", "alice", text);
            asks.question = Some(ants_proto::Question::new(text, &["yes".into(), "no".into()], true).unwrap().into());
            asks
        };
        let questions = [asking(1, &first, "Ship?"), asking(2, &second, "Tag?")];
        m.stored(questions.to_vec(), at(2));
        let selected = items(&m.data, at(3)).into_iter().next().unwrap().thread;
        let [focused, other] = if selected == first { questions } else { [questions[1].clone(), questions[0].clone()] };

        m.key(Key::Enter, at(3));
        m.key(Key::Down, at(3));
        m.key(Key::Down, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert!(m.ui.bar.focused && m.ui.bar.text() == "/answer ", "an empty typed answer goes through the bar");
        m.key(Key::Esc, at(3));
        assert!(!m.ui.bar.focused);
        let shown = screen(&m, 140, 30, at(3));
        assert!(shown.contains("› type an answer"), "the card keeps the focus: {shown}");

        let mut answer = message(3, &focused.thread_id, "alice", "dev-b", "yes");
        answer.answer =
            Some(ants_proto::Answer { question: focused.id.unwrap(), picks: vec![ants_proto::Pick::option(1)] });
        m.stored(vec![answer], at(3));
        assert_eq!(items(&m.data, at(4))[0].thread, other.thread_id, "the other question is selected now");
        assert_eq!(m.key(Key::Char('1'), at(4)), None, "a key for the card gone answers no other");
        let job = crate::tui::exec::Job::Answer {
            thread: other.thread_id.clone(),
            question: other.id.unwrap(),
            picks: vec![ants_proto::Pick::option(1)],
        };
        assert_eq!(m.key(Key::Char('1'), at(4)), Some(Action::Run(job, String::new())), "the next key answers it");
    }

    #[test]
    fn a_typed_answer_on_home_goes_to_the_focused_card_not_a_newer_question() {
        let thread = ThreadId::generate();
        let mut m = model();
        let asking = |seq, text: &str| {
            let mut asks = message(seq, &thread, "dev-b", "alice", text);
            asks.question = Some(ants_proto::Question::new(text, &["yes".into(), "no".into()], true).unwrap().into());
            asks
        };
        let first = asking(1, "Ship?");
        m.stored(vec![first.clone()], at(2));
        m.key(Key::Enter, at(3));
        m.key(Key::Down, at(3));
        m.key(Key::Down, at(3));
        assert_eq!(m.key(Key::Enter, at(3)), None);
        assert_eq!(m.ui.bar.text(), "/answer ");
        m.stored(vec![asking(2, "Tag?")], at(3));
        for c in "later".chars() {
            m.key(Key::Char(c), at(3));
        }
        let job = crate::tui::exec::Job::Answer {
            thread: thread.clone(),
            question: first.id.unwrap(),
            picks: vec![ants_proto::Pick::Typed("later".into())],
        };
        assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Run(job, "/answer later".into())));
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
        let shown = screen(&m, 140, 30, at(5));
        assert!(shown.contains("g grant · x cancel · d done · z snooze · Enter open"), "{shown}");
        assert!(shown.contains("r follow up · d mark done · z snooze · Enter review"), "{shown}");

        let grant = Job::Grant { thread: stuck.clone(), text: GRANT.into() };
        assert_eq!(m.key(Key::Char('g'), at(5)), Some(Action::Run(grant, String::new())));
        let cancel = Job::Cancel { thread: stuck.clone(), ants: vec![user("dev-b")] };
        assert_eq!(m.key(Key::Char('x'), at(5)), Some(Action::Run(cancel, String::new())));
        let last = m.data.live.summary(&stuck).unwrap().last_reopening;
        assert_eq!(m.key(Key::Char('d'), at(5)), Some(Action::Change(stuck.clone(), Change::Done(true), last)));

        assert_eq!(m.key(Key::Char('z'), at(5)), None);
        assert!(screen(&m, 140, 30, at(5)).contains("for (at most 7d): 1h"), "the footer asks how long");
        let snooze = Change::Snooze(Some(at(5 + 3600)));
        assert_eq!(m.key(Key::Enter, at(5)), Some(Action::Change(stuck.clone(), snooze, last)));
        let whys: Vec<Why> = items(&m.data, at(6)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Finished], "snoozed off home");
        assert_eq!(items(&m.data, at(5 + 3600)).len(), 2, "until the snooze ends");
        m.stored(vec![escalation(7, &stuck, "dev-b", "alice")], at(7));
        assert_eq!(items(&m.data, at(8)).len(), 2, "or a new item arrives on the thread");

        m.key(Key::End, at(8));
        assert_eq!(m.key(Key::Char('r'), at(8)), Some(Action::Open(done)));
        assert!(m.ui.bar.focused, "the bar open to follow up on the thread");
    }

    #[test]
    fn f_opens_the_bar_to_forward_the_selected_item_s_thread() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut opening = message(1, &thread, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(2, &thread, "dev-a", "alice", "deployed")], at(2));
        assert_eq!(m.key(Key::Char('f'), at(3)), None);
        assert_eq!((m.ui.bar.text(), m.ui.bar.focused, m.page().name()), ("/forward @".to_owned(), true, "home"));
        for c in "dev-b".chars() {
            m.key(Key::Char(c), at(3));
        }
        assert_eq!(m.key(Key::Enter, at(3)), None, "no TL;DR kept yet");
        let refused = &m.ui.bar.status.as_ref().unwrap().text;
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
        let shown = screen(&m, 140, 30, at(5));
        assert!(shown.contains("question help · stuck · 2 mentions"), "{shown}");
        assert!(shown.contains("1-9 answer · r reply · d done · z snooze · Enter choose"), "the top reason's: {shown}");
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
            home.thread(&m.data, at(3))
        };
        let finished = || {
            let mut m = model();
            let mut opening = message(1, &done, "alice", "dev-a", "deploy");
            opening.owner = Some(user("alice"));
            m.stored(vec![opening, message(2, &done, "dev-a", "alice", "deployed")], at(2));
            screen(&m, 140, 30, at(3));
            assert_eq!(selected(&m), Some(done.clone()), "the finished thread on top");
            m
        };
        let push =
            ApprovalRequest { thread_id: asked.clone(), ..request("dev-a", "git push", vec![Verdict::AllowOnce]) };

        let mut m = finished();
        m.data.live.approval(&Update::Pending(push.clone()));
        let shown = screen(&m, 140, 30, at(3));
        let (approve, below) = (shown.find("│▌approve "), shown.find("│ finished "));
        assert!(approve.zip(below).is_some_and(|(a, f)| a < f), "{shown}");
        assert_eq!(selected(&m), Some(asked), "the request listed after the first draw selected on top");
        assert_eq!(m.key(Key::Char('a'), at(3)), Some(Action::Decide(Box::new(push.clone()), Verdict::AllowOnce)));

        let mut m = finished();
        m.key(Key::Home, at(3));
        m.data.live.approval(&Update::Pending(push));
        screen(&m, 140, 30, at(3));
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
        assert!(screen(&m, 140, 30, at(5)).contains("r follow up · d mark done"), "the finished thread selected");

        m.stored(vec![message(6, &other, "alice", "dev-c", "build"), escalation(7, &other, "dev-c", "alice")], at(7));
        let whys: Vec<Why> = items(&m.data, at(8)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Stuck, Why::Stuck, Why::Finished], "an item arrived above the selection");
        let last = m.data.live.summary(&done).unwrap().last_reopening;
        assert_eq!(m.key(Key::Char('d'), at(8)), Some(Action::Change(done, Change::Done(true), last)));

        let selected = |m: &crate::tui::model::Model| {
            let crate::tui::model::Page::Home(home) = m.page() else { panic!("home") };
            home.thread(&m.data, at(8))
        };
        m.key(Key::Home, at(8));
        m.key(Key::Down, at(8));
        assert_eq!(selected(&m), Some(other.clone()));
        m.data.cards.snoozed.snooze(&stuck, at(8), at(8) + chrono::TimeDelta::hours(1));
        assert_eq!(selected(&m), Some(other), "on its item as one above leaves");
    }

    #[test]
    fn a_slash_command_acts_on_the_row_selected_at_its_time_not_at_the_clock_s() {
        use crate::tui::exec::Job;

        let (stuck, other) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        m.stored(vec![message(1, &stuck, "alice", "dev-b", "review"), escalation(2, &stuck, "dev-b", "alice")], at(2));
        m.stored(vec![message(3, &other, "alice", "dev-c", "build"), escalation(4, &other, "dev-c", "alice")], at(4));
        m.data.cards.snoozed.snooze(&stuck, at(5), at(20));
        let done =
            |thread: &ThreadId| Some(Action::Run(Job::Done { thread: thread.clone(), done: true }, "/done".into()));

        assert_eq!(items(&m.data, at(10)).len(), 1, "the snoozed thread is off home");
        screen(&m, 140, 30, at(10));
        assert_eq!(m.submit("/done".to_owned(), at(10)), done(&other), "the top row while the snooze holds");

        assert_eq!(items(&m.data, at(30)).len(), 2, "the snooze ended, the thread is back on top");
        screen(&m, 140, 30, at(30));
        assert_eq!(m.submit("/done".to_owned(), at(30)), done(&stuck), "the top row once it ended");
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
        assert!(screen(&m, 140, 30, at(3)).contains("Site deploy"));

        m.data.cards.titles.insert(done.clone(), "Release 1.2".to_owned());
        m.data.cards.titles.insert(unlisted.clone(), "[sealed]".to_owned());
        assert_eq!(m.data.title(&done), "Release 1.2");
        assert_eq!(m.data.title(&unlisted), "[sealed]", "even without a message read");
        let shown = screen(&m, 140, 30, at(3));
        assert!(shown.contains("Release 1.2") && !shown.contains("Site deploy"), "{shown}");
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
        let shown = screen(&m, 140, 30, at(6));
        assert!(shown.contains("message"), "{shown}");
        assert!(shown.contains("r reply · d done · z snooze · Enter open"), "{shown}");

        m.key(Key::End, at(6));
        let last = m.data.live.summary(&asked).unwrap().last_reopening;
        assert_eq!(m.key(Key::Char('d'), at(6)), Some(Action::Change(asked.clone(), Change::Done(true), last)));
        assert_eq!(m.key(Key::Enter, at(6)), Some(Action::Open(asked)));
        m.key(Key::Esc, at(6));
        m.data.marker = Some(ants_proto::ReadMarker { below: 0, read: [1].into(), ..Default::default() });
        assert_eq!(items(&m.data, at(6)).len(), 1, "read, it leaves home");
    }
}
