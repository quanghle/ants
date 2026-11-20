//! A thread's waiting cards listed individually, opening the existing request details or question cursor.

use std::cell::Cell;

use ants_proto::{ApprovalRequest, UserId, Uuid};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Text};
use ratatui::widgets::Paragraph;

use super::Page;
use crate::tui::model::{Data, Key, To};
use crate::tui::select::Selection;
use crate::tui::{keymap, mouse, question, style};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Id {
    Request(UserId, Uuid),
    Question(Uuid),
}

enum Item<'a> {
    Request(&'a ApprovalRequest),
    Question(question::Card<'a>),
}

impl Item<'_> {
    fn id(&self) -> Id {
        match self {
            Self::Request(request) => Id::Request(request.ant.clone(), request.id),
            Self::Question(card) => Id::Question(card.asked.id.expect("only questions with ids are listed")),
        }
    }

    fn line(&self, width: usize) -> Line<'static> {
        let text = match self {
            Self::Request(request) => {
                format!("tool · {} · {} ({})", request.tool.title.replace('\n', " "), request.ant, request.tool.kind)
            }
            Self::Question(card) => {
                let from = card.asked.from.as_ref().map_or("an ant", UserId::as_str);
                format!("question · {} · {from}", question::asking(card.question).replace('\n', " "))
            }
        };
        Line::raw(style::fit(&text, width))
    }
}

fn items<'a>(page: &'a Page, data: &'a Data, now: DateTime<Utc>) -> Vec<Item<'a>> {
    let mut requests: Vec<_> = data.live.approvals.iter().filter(|r| r.thread_id == page.history.thread).collect();
    requests.sort_by_key(|r| r.requested_at);
    let questions = question::cards(data, &page.history.entries, now)
        .into_iter()
        .filter(|card| card.answers.is_empty() && card.asked.id.is_some())
        .map(Item::Question);
    requests.into_iter().map(Item::Request).chain(questions).collect()
}

#[derive(Debug, Default)]
pub(super) struct List {
    selected: Selection<Id>,
    height: Cell<usize>,
}

impl List {
    pub(super) fn count(page: &Page, data: &Data, now: DateTime<Utc>) -> usize {
        items(page, data, now).len()
    }

    pub(super) fn draw(&self, frame: &mut Frame, area: Rect, page: &Page, data: &Data, now: DateTime<Utc>) {
        let items = items(page, data, now);
        let block = style::block(format!(" Waiting ({}) ", items.len()), true).title_bottom(keymap::prompt(
            "Enter open · Esc close · ↑↓ select",
            area.width.saturating_sub(2),
            false,
        ));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        self.height.set(usize::from(inner.height));
        if items.is_empty() {
            frame.render_widget(Paragraph::new("nothing waits on the thread"), inner);
            return;
        }
        let selected = self.selected.drawn(&items.iter().map(Item::id).collect::<Vec<_>>());
        let rows = items.iter().map(|item| Text::from(item.line(usize::from(inner.width.saturating_sub(1))))).collect();
        mouse::list(frame, inner, data, mouse::Viewport::ThreadWaiting, rows, selected, true);
    }
}

impl Page {
    pub fn listing(&self) -> bool {
        self.listing.is_some()
    }

    pub fn waiting(&self, data: &Data, now: DateTime<Utc>) -> usize {
        List::count(self, data, now)
    }

    pub fn toggle_waiting(&mut self) {
        self.listing = self.listing.is_none().then(List::default);
        self.answering = None;
    }

    pub(super) fn waiting_key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<Option<To>> {
        let list = self.listing.as_ref()?;
        if matches!(key, Key::Char('?') | Key::Ctrl('k') | Key::Tab) {
            return None;
        }
        let items = items(self, data, now);
        let ids: Vec<_> = items.iter().map(Item::id).collect();
        let at = list.selected.at(&ids);
        let half = (list.height.get() / 2).max(1);
        let to = match key {
            Key::Esc | Key::Char('q') | Key::Ctrl('l') => {
                self.listing = None;
                return Some(None);
            }
            Key::Enter => {
                return Some(match items.get(at) {
                    Some(Item::Request(request)) => Some(To::Ask((*request).clone(), None)),
                    Some(Item::Question(card)) => {
                        self.answering = Some(question::Cursor::new(card.asked.id?, card.question));
                        self.listing = None;
                        None
                    }
                    None => None,
                });
            }
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at.saturating_add(1),
            Key::PageUp => at.saturating_sub(half),
            Key::PageDown => at.saturating_add(half),
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return Some(None),
        };
        self.listing.as_mut()?.selected.select(&ids, to);
        Some(None)
    }
}
