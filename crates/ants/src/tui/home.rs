//! Home: what waits on the human, one row per thread and reason (per tool request for approvals, as cards answered
//! where they show, [`approvals`]), the most pressing reasons first and the oldest first within one. Questions
//! (ants-ofmi.21) and humans' messages (ants-ofmi.26) are to join the reasons.

use ants_proto::{ApprovalRequest, ThreadId, Verdict};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};

use super::approvals;
use super::frame::{self, ago};
use super::model::{Data, Key, Model, To};
use crate::thread::Kind;
use crate::tools::State;

/// Why a thread is on home, in the order home lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Why {
    /// A tool request waits for a decision.
    Approval,
    /// Escalated to a human, or a message given up on.
    Stuck,
    /// An unread post @mentioning the human.
    Mention,
    /// An ant answered on a thread the human owns.
    Finished,
}

impl Why {
    fn label(self) -> &'static str {
        match self {
            Self::Approval => "approve",
            Self::Stuck => "stuck",
            Self::Mention => "mention",
            Self::Finished => "finished",
        }
    }

    fn color(self) -> Color {
        match self {
            Self::Approval => Color::Yellow,
            Self::Stuck => Color::Red,
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
}

/// What waits on the human at `now`.
pub fn items(data: &Data, now: DateTime<Utc>) -> Vec<Item> {
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
            }
        })
        .collect();
    let item = |why, thread: &ThreadId, at, text| Item { why, thread: thread.clone(), at, text, request: None };
    for thread in &data.live.order {
        if let Some(alert) = data.stuck(thread) {
            items.push(item(Why::Stuck, thread, alert.at, alert.text.clone()));
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
    items.sort_by_key(|i| (i.why, i.at));
    items
}

/// How many things wait on the human at `now`: the items but the requests settled lately.
pub fn waiting(data: &Data, now: DateTime<Utc>) -> usize {
    items(data, now).iter().filter(|i| i.request.as_ref().is_none_or(|(_, settled)| settled.is_none())).count()
}

#[derive(Debug, Default)]
pub struct Page {
    selected: usize,
}

impl Page {
    pub const KEYS: &str = "↑↓ item · Enter open · a A r answer a tool request";

    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        let items = items(data, now);
        let selected = items.get(self.selected);
        let pending = selected.and_then(|i| i.request.as_ref()).filter(|(_, s)| s.is_none()).map(|(r, _)| r.clone());
        let verdict = match key {
            Key::Char('a') => Some(Verdict::AllowOnce),
            Key::Char('A') => Some(Verdict::AllowAlways),
            Key::Char('r') => Some(Verdict::Reject),
            _ => None,
        };
        self.selected = match key {
            Key::Enter => {
                return match pending {
                    Some(request) => Some(To::Ask(request, None)),
                    None => selected.map(|i| To::Thread(i.thread.clone())),
                };
            }
            Key::Char('a' | 'A' | 'r') => return pending.map(|request| To::Ask(request, verdict)),
            Key::Up | Key::Char('k') => self.selected.saturating_sub(1),
            Key::Down | Key::Char('j') => self.selected + 1,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        }
        .min(items.len().saturating_sub(1));
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let data = &model.data;
        let items = items(data, now);
        let block = frame::block(format!(" Home ({}) ", items.len()), focused);
        if items.is_empty() {
            let text = if data.live.stored_at.is_some() { "nothing waits on a human" } else { "reading the stream…" };
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }
        let selected = self.selected.min(items.len() - 1);
        let width = usize::from(area.width.saturating_sub(11));
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
        let list = List::new(rows).block(block).highlight_style(frame::highlight(focused));
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
        assert_eq!(waiting(&m.data, at(4)), 4);

        m.key(Key::Down, at(4));
        assert_eq!(m.key(Key::Char('r'), at(4)), None);
        assert_eq!(m.key(Key::Char('y'), at(4)), Some(Action::Decide(second.clone(), Verdict::Reject)));
        m.take(Event::Decided(Ok("rejected".to_owned())), at(5));
        assert!(m.asking.is_none(), "the decision stored ends the answering");
        m.take(Event::Approval(Update::Resolved(second, State::Expired)), at(5));
        assert_eq!(items(&m.data, at(5)).len(), 4, "the settled request keeps its card a while");
        assert_eq!(waiting(&m.data, at(5)), 3);
        assert_eq!(items(&m.data, at(500)).len(), 3);

        m.key(Key::Home, at(5));
        assert_eq!(m.key(Key::Enter, at(5)), None);
        assert!(m.asking.as_ref().is_some_and(|a| a.full && a.request == push), "Enter shows the request in full");
        m.key(Key::Esc, at(5));
        m.key(Key::End, at(500));
        assert_eq!(m.key(Key::Enter, at(500)), Some(Action::Open(done)));
    }
}
