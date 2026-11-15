//! Home: what waits on the human, one row per thread and reason, the most pressing reasons first and the oldest first
//! within one. Questions (ants-ofmi.21) and humans' messages (ants-ofmi.26) are to join the reasons.

use ants_proto::ThreadId;
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};

use super::frame::{self, ago};
use super::model::{Data, Key, To};
use crate::thread::Kind;

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
}

/// What waits on the human.
pub fn items(data: &Data) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::new();
    for request in &data.live.approvals {
        if !items.iter().any(|i| i.thread == request.thread_id) {
            let text = format!("{}: {}", request.ant, request.tool.title.replace('\n', " "));
            items.push(Item { why: Why::Approval, thread: request.thread_id.clone(), at: request.requested_at, text });
        }
    }
    for thread in &data.live.order {
        if let Some(alert) = data.stuck(thread) {
            items.push(Item { why: Why::Stuck, thread: thread.clone(), at: alert.at, text: alert.text.clone() });
        }
        if let Some(m) = data.unread_on(thread).find(|i| i.kind == Kind::Mention) {
            let text = format!("{} in #{}: {}", m.from, m.channel.as_ref().map_or("?", |c| c.as_str()), m.text);
            items.push(Item { why: Why::Mention, thread: thread.clone(), at: m.at, text });
        }
        if data.finished(thread)
            && let Some(latest) = data.facts.get(thread).and_then(|f| f.latest.as_ref())
        {
            let text = format!("{} answered", latest.from);
            items.push(Item { why: Why::Finished, thread: thread.clone(), at: latest.at, text });
        }
    }
    items.sort_by_key(|i| (i.why, i.at));
    items
}

#[derive(Debug, Default)]
pub struct Page {
    selected: usize,
}

impl Page {
    pub const KEYS: &str = "↑↓ item · Enter open its thread";

    /// The selected item's thread.
    pub fn thread(&self, data: &Data) -> Option<ThreadId> {
        let items = items(data);
        items.get(self.selected.min(items.len().saturating_sub(1))).map(|i| i.thread.clone())
    }

    pub fn key(&mut self, key: Key, data: &Data, _now: DateTime<Utc>) -> Option<To> {
        let items = items(data);
        self.selected = match key {
            Key::Enter => return items.get(self.selected).map(|i| To::Thread(i.thread.clone())),
            Key::Up | Key::Char('k') => self.selected.saturating_sub(1),
            Key::Down | Key::Char('j') => self.selected + 1,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        }
        .min(items.len().saturating_sub(1));
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let items = items(data);
        let block = frame::block(format!(" Home ({}) ", items.len()), focused);
        if items.is_empty() {
            let text = if data.live.stored_at.is_some() { "nothing waits on a human" } else { "reading the stream…" };
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }
        let rows: Vec<ListItem> = items
            .iter()
            .map(|i| {
                ListItem::new(vec![
                    Line::from(vec![
                        Span::styled(format!("{:<9}", i.why.label()), Style::new().fg(i.why.color())),
                        Span::raw(data.title(&i.thread)),
                        Span::styled(format!("  {}", ago(i.at, now)), frame::dim()),
                    ]),
                    Line::styled(format!("         {}", crate::excerpt(&i.text, 160)), frame::dim()),
                ])
            })
            .collect();
        let list = List::new(rows).block(block).highlight_style(frame::highlight(focused));
        let selected = self.selected.min(items.len() - 1);
        frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(Some(selected)));
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ApprovalRequest, Envelope, Mention, Uuid, Verdict, subject};

    use super::*;
    use crate::live::tests::{at, message, request, user};
    use crate::thread::Parser;
    use crate::tools::Update;
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
        m.data.live.approval(&Update::Pending(ApprovalRequest { id: Uuid::now_v7(), ..push }));
        let whys: Vec<(Why, ThreadId)> = items(&m.data).into_iter().map(|i| (i.why, i.thread)).collect();
        assert_eq!(whys, [(Why::Approval, asked), (Why::Mention, mentioned), (Why::Finished, done.clone())]);
        m.key(Key::End, at(4));
        assert_eq!(m.key(Key::Enter, at(4)), Some(Action::Open(done)));
    }
}
