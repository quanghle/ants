//! Home, the triage queue: what waits on the human, one item per thread and reason (per tool request for approvals,
//! as cards answered where they show, [`approvals`]), the most pressing reasons first and the oldest first within
//! one, each with its one-key actions: answer ([`question`]), grant, reply, follow up, cancel, mark done, snooze ([`super::snooze`])
//! or open. A snoozed thread's items stay off home until the snooze ends or an item for the human arrives on it.

use std::collections::HashMap;

use ants_proto::{ApprovalRequest, Question, ThreadId, Uuid, Verdict};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};

use super::acts::Change;
use super::approvals;
use super::command::GRANT;
use super::exec::Job;
use super::frame::{self, ago};
use super::interrupt::Intent;
use super::model::{Action, Data, Key, Model, To};
use super::select::Selection;
use super::{catchup, question};
use crate::thread::Kind;
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
    /// An ant answered on a thread the human owns.
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
            Self::Finished => "finished",
        }
    }

    /// The item's one-key actions but Enter, as home and the compact pane show them.
    pub fn keys(self) -> &'static str {
        match self {
            Self::Approval => "a A r answer · z snooze",
            Self::Question => "1-9 answer · d done · z snooze",
            Self::Stuck => "g grant · x cancel · d done · z snooze",
            Self::Message | Self::Mention => "r reply · d done · z snooze",
            Self::Finished => "f follow up · d mark done · z snooze",
        }
    }

    /// What Enter does on the item, in home.
    fn enter(self) -> &'static str {
        match self {
            Self::Approval => "Enter details",
            Self::Finished => "Enter review",
            Self::Question | Self::Stuck | Self::Message | Self::Mention => "Enter open",
        }
    }

    pub fn color(self) -> Color {
        match self {
            Self::Approval => Color::Yellow,
            Self::Question => Color::Cyan,
            Self::Stuck => Color::Red,
            Self::Message => Color::Blue,
            Self::Mention => Color::Magenta,
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
    /// The tool request of an approval, and how it was settled lately, if it was.
    pub request: Option<(ApprovalRequest, Option<State>)>,
    /// The question asked.
    pub question: Option<Question>,
}

/// What an item is, whatever else changes on it: its reason and thread, and the id of an approval's tool request.
pub type Id = (Why, ThreadId, Option<Uuid>);

impl Item {
    pub fn id(&self) -> Id {
        (self.why, self.thread.clone(), self.request.as_ref().map(|(r, _)| r.id))
    }
}

/// What waits on the human at `now`.
pub fn items(data: &Data, now: DateTime<Utc>) -> Vec<Item> {
    let mut items = unsnoozed(data, now);
    let held = held(data, &items, now);
    items.retain(|i| !held.contains_key(&i.thread));
    // The requests waiting for the human before those settled lately.
    items.sort_by_key(|i| (i.why, i.request.as_ref().is_some_and(|(_, settled)| settled.is_some()), i.at));
    items
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
                text: format!("{} asks to run {}: {}", request.ant, request.tool.kind, request.tool.title),
                request: Some((request.clone(), card.settled.cloned())),
                question: None,
            }
        })
        .collect();
    let item = |why, thread: &ThreadId, at, text| Item {
        why,
        thread: thread.clone(),
        at,
        text,
        request: None,
        question: None,
    };
    for thread in &data.live.order {
        if let Some(asked) = data.question(thread) {
            let text = format!("{}: {}", asked.from, asked.question.text);
            items.push(Item { question: Some(asked.question.clone()), ..item(Why::Question, thread, asked.at, text) });
        }
        if let Some(alert) = data.stuck(thread) {
            items.push(item(Why::Stuck, thread, alert.at, alert.text.clone()));
        }
        let me = data.me();
        if let Some(m) =
            data.unread_on(thread).find(|i| i.kind == Kind::Message && i.from != *me && data.is_human(&i.from))
        {
            items.push(item(Why::Message, thread, m.at, format!("{}: {}", m.from, m.text)));
        }
        if let Some(m) = data.unread_on(thread).find(|i| i.kind == Kind::Mention) {
            let text = format!("{} in #{}: {}", m.from, m.channel.as_ref().map_or("?", |c| c.as_str()), m.text);
            items.push(item(Why::Mention, thread, m.at, text));
        }
        if data.finished(thread)
            && let Some(latest) = data.facts.get(thread).and_then(|f| f.latest.as_ref())
        {
            items.push(item(Why::Finished, thread, latest.at, format!("{} answered", latest.from)));
        }
    }
    items
}

/// What waits on the human at `now`: the items but the requests settled lately.
pub fn waiting(data: &Data, now: DateTime<Utc>) -> Vec<Item> {
    items(data, now).into_iter().filter(|i| i.request.as_ref().is_none_or(|(_, settled)| settled.is_none())).collect()
}

#[derive(Debug, Default)]
pub struct Page {
    /// The selected item, kept on it as items arrive and leave.
    selected: Selection<Id>,
}

impl Page {
    pub const KEYS: &str = "j/k move · Enter open · d done · z snooze · t tree · s digest · c creds";

    /// The selected item's thread.
    pub fn thread(&self, data: &Data) -> Option<ThreadId> {
        let items = items(data, Utc::now());
        items.get(self.at(&items)).map(|i| i.thread.clone())
    }

    /// Where the selected item is among `items`.
    fn at(&self, items: &[Item]) -> usize {
        self.selected.at(&items.iter().map(Item::id).collect::<Vec<_>>())
    }

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let items = items(data, now);
        let at = self.at(&items);
        let selected = items.get(at);
        let pending = selected.and_then(|i| i.request.as_ref()).filter(|(_, s)| s.is_none()).map(|(r, _)| r.clone());
        let verdict = match key {
            Key::Char('a') => Some(Verdict::AllowOnce),
            Key::Char('A') => Some(Verdict::AllowAlways),
            Key::Char('r') => Some(Verdict::Reject),
            _ => None,
        };
        let thread = || selected.map(|i| i.thread.clone());
        let to = match key {
            Key::Enter => {
                return match pending {
                    Some(request) => Some(To::Ask(request, None)),
                    None => thread().map(To::Thread),
                };
            }
            Key::Char('a' | 'A') => return pending.map(|request| To::Ask(request, verdict)),
            Key::Char('r') if pending.is_some() => return pending.map(|request| To::Ask(request, verdict)),
            Key::Char('1'..='9' | 'r') if selected.is_some_and(|i| i.question.is_some()) => {
                let item = selected?;
                return question::key(key, &item.thread, item.question.as_ref()?);
            }
            Key::Char('r' | 'f') => return thread().map(To::Reply),
            Key::Char('g') => {
                let stuck = selected.filter(|i| i.why == Why::Stuck)?;
                let job = Job::Grant { thread: stuck.thread.clone(), text: GRANT.to_owned() };
                return Some(To::Do(Action::Run(job, String::new())));
            }
            Key::Char('x') => return thread().map(|thread| To::Interrupt(Intent::Cancel, thread)),
            Key::Char('d') => {
                let thread = thread()?;
                let last = data.live.summary(&thread).map_or(0, |s| s.last_reopening);
                return Some(To::Do(Action::Change(thread, Change::Done(true), last)));
            }
            Key::Char('z') => return thread().map(To::Snooze),
            Key::Char('t') => return thread().map(To::Tree),
            Key::Char('s') => return Some(To::Digest),
            Key::Char('c') => return Some(To::Credentials),
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
        let items = items(data, now);
        let block = frame::block(format!(" Home ({}) ", items.len()), focused);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let mut first = vec![catchup::since_line(model, now)];
        first.extend(data.credentials.line());
        let tall = u16::try_from(first.len() + 1).unwrap_or(u16::MAX);
        let [top, area] = Layout::vertical([Constraint::Length(tall), Constraint::Min(0)]).areas(inner);
        first.push(Line::styled("─".repeat(usize::from(top.width)), frame::dim()));
        frame.render_widget(Paragraph::new(first), top);
        if items.is_empty() {
            let text = if data.live.stored_at.is_some() { "nothing waits on a human" } else { "reading the stream…" };
            frame.render_widget(Paragraph::new(text), area);
            return;
        }
        let selected = self.at(&items);
        let width = usize::from(area.width.saturating_sub(9));
        let rows: Vec<ListItem> = items
            .iter()
            .enumerate()
            .map(|(at, i)| {
                let mut lines = vec![
                    Line::from(vec![
                        Span::styled(format!("{:<9}", i.why.label()), Style::new().fg(i.why.color())),
                        Span::raw(data.title(&i.thread)),
                        Span::styled(format!("  {}", ago(i.at, now)), frame::dim()),
                    ]),
                    Line::styled(format!("         {}", crate::excerpt(&i.text, 160)), frame::dim()),
                ];
                if let Some(question) = &i.question {
                    lines.push(Line::raw(format!("         {}", crate::excerpt(&question::options(question), 160))));
                }
                if i.request.is_none() {
                    lines.push(Line::styled(format!("         {} · {}", i.why.keys(), i.why.enter()), frame::dim()));
                }
                if let Some((request, settled)) = &i.request {
                    let card = approvals::Card { request, settled: settled.as_ref() };
                    let card = approvals::lines(card, model.asking.as_ref(), at == selected, now, width);
                    // The tool shows on the line before.
                    lines.extend(card.into_iter().skip(1).map(|line| {
                        let mut spans = vec![Span::raw("         ")];
                        spans.extend(line.spans);
                        Line::from(spans).style(line.style)
                    }));
                }
                ListItem::new(lines)
            })
            .collect();
        let list = List::new(rows).highlight_style(frame::highlight(focused));
        frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(Some(selected)));
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ApprovalRequest, Envelope, Mention, Uuid, Verdict, subject};

    use super::*;
    use crate::live::Event;
    use crate::live::tests::{at, message, request, user};
    use crate::thread::Parser;
    use crate::tools::{State, Update};
    use crate::tui::model::Action;
    use crate::tui::model::tests::model;

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
        let whys: Vec<(Why, ThreadId)> = items(&m.data, at(4)).into_iter().map(|i| (i.why, i.thread)).collect();
        assert_eq!(
            whys,
            [
                (Why::Approval, asked.clone()),
                (Why::Approval, asked),
                (Why::Mention, mentioned),
                (Why::Finished, done.clone())
            ]
        );
        assert_eq!(waiting(&m.data, at(4)).len(), 4);

        m.key(Key::Down, at(4));
        assert_eq!(m.key(Key::Char('r'), at(4)), None);
        assert_eq!(m.key(Key::Char('y'), at(4)), Some(Action::Decide(Box::new(second.clone()), Verdict::Reject)));
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
        m.key(Key::End, at(500));
        assert_eq!(m.key(Key::Enter, at(500)), Some(Action::Open(done)));
    }

    fn escalation(seq: u64, thread: &ThreadId, from: &str, to: &str) -> crate::thread::Entry {
        let x = ants_proto::Escalation::new(user(from), thread.clone(), ants_proto::EscalationReason::TurnBudget);
        let envelope = Envelope {
            thread_id: thread.clone(),
            escalation: Some(Box::new(x)),
            ..Envelope::new(user(from), user(to), "help")
        };
        let payload = serde_json::to_vec(&envelope).unwrap();
        Parser::default().entry(seq, at(seq.cast_signed()), &subject::inbox(&user(to), &user(from)), &payload).unwrap()
    }

    fn shown(m: &crate::tui::model::Model, now: DateTime<Utc>) -> String {
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 30)).unwrap();
        terminal.draw(|f| crate::tui::frame::draw(f, m, now)).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        cells.chunks(140).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn a_question_comes_after_the_approvals_and_answers_by_number_or_through_the_bar() {
        use crate::tui::exec::Job;

        let (asked, stuck) = (ThreadId::generate(), ThreadId::generate());
        let mut m = model();
        let mut question = message(2, &asked, "dev-b", "alice", "Which retry policy?");
        let options = ["exponential".to_owned(), "fixed 5s".to_owned()];
        question.question = Some(ants_proto::Question::new("Which retry policy?", &options, true).unwrap());
        m.stored(vec![escalation(1, &stuck, "dev-a", "alice"), question.clone()], at(2));
        let whys: Vec<Why> = items(&m.data, at(3)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Question, Why::Stuck]);
        let screen = shown(&m, at(3));
        assert!(screen.contains("question Which retry policy?"), "{screen}");
        assert!(screen.contains("1 exponential  2 fixed 5s  or r types an answer"), "{screen}");
        assert!(screen.contains("1-9 answer · d done · z snooze · Enter open"), "{screen}");

        assert_eq!(m.key(Key::Char('3'), at(3)), None, "no third option");
        let job = Job::Answer { thread: asked.clone(), choice: ants_proto::Choice::Option(2) };
        assert_eq!(m.key(Key::Char('2'), at(3)), Some(Action::Run(job, String::new())));
        assert_eq!(m.key(Key::Char('r'), at(3)), None);
        assert!(m.bar.focused && m.bar.text() == "/answer ", "r types an answer in the bar");
        m.key(Key::Esc, at(3));

        let mut answer = message(3, &asked, "alice", "dev-b", "fixed 5s");
        answer.answer = Some(ants_proto::Answer { question: question.id.unwrap(), option: Some(2) });
        m.stored(vec![answer], at(3));
        let whys: Vec<Why> = items(&m.data, at(4)).into_iter().map(|i| i.why).collect();
        assert_eq!(whys, [Why::Stuck], "answered");
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
        assert!(screen.contains("f follow up · d mark done · z snooze · Enter review"), "{screen}");

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
        assert_eq!(m.key(Key::Char('f'), at(8)), Some(Action::Open(done)));
        assert!(m.bar.focused, "the bar open to follow up on the thread");
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
        assert!(shown(&m, at(5)).contains("f follow up · d mark done"), "the finished thread selected");

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
        m.data.marker = Some(ants_proto::ReadMarker { below: 0, read: [1].into() });
        assert_eq!(items(&m.data, at(6)).len(), 1, "read, it leaves home");
    }
}
