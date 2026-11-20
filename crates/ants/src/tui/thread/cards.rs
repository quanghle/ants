//! The thread's cards: its tool requests and questions under the conversation, the one the keys answer whole.

use ants_proto::{ThreadId, Uuid};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use super::Page;
use crate::threads::thread::{Entry, Kind};
use crate::tui::model::{Data, Key, Model, To};
use crate::tui::{approvals, question, style};

impl Page {
    /// The cursor on the focused question card, while its question is still the one the keys answer.
    pub fn card(&self, data: &Data, now: DateTime<Utc>) -> Option<&question::Cursor> {
        let cursor = self.answering.as_ref()?;
        question::cards(data, &self.history.entries, now)
            .iter()
            .any(|card| card.asked.id == Some(cursor.question) && card.answers.is_empty())
            .then_some(cursor)
    }

    /// The focused question's exact message, else the latest waiting question; never a newer question in place of
    /// one answered elsewhere while its typed answer was being composed.
    pub fn question<'a>(&'a self, data: &Data, now: DateTime<Utc>) -> Option<(Uuid, &'a ants_proto::Questions)> {
        if self.listing() {
            return None;
        }
        if let Some(cursor) = &self.answering {
            self.card(data, now)?;
            let asked = self.history.entries.iter().find(|e| e.id == Some(cursor.question))?;
            return Some((cursor.question, asked.question.as_ref()?));
        }
        question::keyed(data, &self.history.entries, now)
    }

    /// Take `key` on the focused question card, if any: where it leads, if anywhere.
    pub fn card_key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<Option<To>> {
        if self.listing() {
            return self.waiting_key(key, data, now);
        }
        let shown = self.card(data, now).is_some();
        question::focused(&mut self.answering, key, shown.then_some(&self.history.thread))
    }

    /// Type `text` pasted on the focused question card, if any: whether it took it.
    pub fn card_paste(&mut self, text: &str, data: &Data, now: DateTime<Utc>) -> bool {
        self.card(data, now).is_some() && self.answering.as_mut().is_some_and(|c| c.paste(text))
    }
}

/// What drawing a thread's cards left: the area above them, the room the oldest pending tool request had on its card
/// (`(width, lines)`, no lines when not drawn), and the message whose card was drawn, by its sequence.
pub(super) struct Cards {
    pub(super) rest: Rect,
    pub(super) room: (usize, usize),
    pub(super) carded: Option<u64>,
}

/// The card a thread shows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shows {
    Request(usize),
    Question(usize),
}

/// Draw the cards of `thread` at the bottom of `area`: the one the keys answer whole, in a third of `area` where it
/// fits: a focused question's, else the oldest pending tool request's, else the question's the keys answer, else the
/// first one; its bottom border counting the others, which the conversation shows in full.
pub(super) fn draw_cards(
    frame: &mut Frame,
    area: Rect,
    model: &Model,
    thread: &ThreadId,
    entries: &[Entry],
    cursor: Option<&question::Cursor>,
    now: DateTime<Utc>,
) -> Cards {
    let width = usize::from(area.width.saturating_sub(2));
    let writing = model.typing();
    let requests = approvals::cards(&model.data, Some(thread), now);
    let mut questions = question::cards(&model.data, entries, now);
    if let Some(cursor) = cursor {
        for card in &mut questions {
            card.keys = card.asked.id == Some(cursor.question);
        }
    }
    let pending = requests.iter().position(|c| c.settled.is_none()).map(Shows::Request);
    let keyed = questions.iter().position(|c| c.keys).map(Shows::Question);
    let first = if requests.is_empty() {
        (!questions.is_empty()).then_some(Shows::Question(0))
    } else {
        Some(Shows::Request(0))
    };
    let shows = cursor.and(keyed).or(pending).or(keyed).or(first);
    let Some(shows) = shows else {
        return Cards { rest: area, room: (width, 0), carded: None };
    };
    // Two borders, then a line of what is asked and one of keys at least.
    let room = usize::from(area.height / 3).max(4) - 2;
    let (title, lines, style, rows, carded) = match shows {
        Shows::Request(at) => {
            let card = requests[at];
            let lines = |rows| approvals::lines(card, model.asking.as_ref(), (true, writing), now, (width, rows));
            let rows = (1..=approvals::CARD_LINES).rev().find(|rows| lines(*rows).len() <= room).unwrap_or(1);
            // On a view too short for the card, it gives the request fewer lines, none when even its others do not
            // show.
            let height = usize::from(area.height);
            let rows = (0..=rows).rev().find(|rows| lines(*rows).len() + 2 <= height).unwrap_or(0);
            let style = if card.settled.is_some() { style::dim() } else { Style::new().fg(style::PENDING) };
            let id = format!("{}/{} ", card.request.ant, card.request.id);
            let asked = entries
                .iter()
                .rfind(|e| e.kind == Kind::Approval && e.note.as_ref().is_some_and(|n| n.starts_with(&id)));
            (approvals::title(card), lines(rows), style, rows, asked.map(|e| e.seq))
        }
        Shows::Question(at) => {
            let card = &questions[at];
            // Enter shows a pending tool request before it focuses a question.
            let lines = card.lines(width, cursor, (pending.is_none(), writing));
            (card.title(), scrolled(lines, room, cursor.is_some()), card.style(), 0, Some(card.asked.seq))
        }
    };
    let mut others = [0; 3];
    for (at, card) in requests.iter().enumerate() {
        if shows != Shows::Request(at) {
            others[if card.settled.is_none() { 0 } else { 2 }] += 1;
        }
    }
    for (at, card) in questions.iter().enumerate() {
        if shows != Shows::Question(at) {
            others[if card.answers.is_empty() { 1 } else { 2 }] += 1;
        }
    }
    let tall = u16::try_from(lines.len() + 2).unwrap_or(u16::MAX).min(area.height);
    let rest = Rect { height: area.height - tall, ..area };
    let below = Rect { y: area.y + rest.height, height: tall, ..area };
    let mut block = ratatui::widgets::Block::bordered().title(title).border_style(style);
    if let Some(folded) = folded(others, width) {
        block = block.title_bottom(Line::styled(folded, style::dim()));
    }
    if let Shows::Question(at) = shows
        && let Some(id) = questions[at].asked.id
    {
        question::mouse_regions(&model.ui.mouse, block.inner(below), &lines, 0, id);
    }
    frame.render_widget(Paragraph::new(lines).block(block), below);
    let room = (width, if pending.is_some() && Some(shows) == pending { rows } else { 0 });
    Cards { rest, room, carded }
}

/// What the bottom border of a thread's card says of the others, `[requests, questions]` waiting and those settled or
/// answered lately, with the key listing every waiting card.
fn folded([requests, questions, settled]: [usize; 3], width: usize) -> Option<String> {
    let count = |n: usize, what: &str| (n > 0).then(|| format!("{n} {what}{}", if n == 1 { "" } else { "s" }));
    let wait: Vec<String> =
        [count(requests, "tool request"), count(questions, "question")].into_iter().flatten().collect();
    let verb = if requests + questions == 1 { "waits" } else { "wait" };
    let text = match (wait.is_empty(), settled) {
        (true, 0) => None,
        (true, n) => Some(format!(" {n} more settled or answered lately above ")),
        (false, 0) => Some(format!(" Ctrl-L lists all · {} also {verb} above ", wait.join(" and "))),
        (false, n) => Some(format!(
            " Ctrl-L lists all · {} also {verb} above · {n} settled or answered lately ",
            wait.join(" and ")
        )),
    }?;
    if !wait.is_empty() && Line::raw(&text).width() > width {
        return Some(style::fit(&format!(" Ctrl-L: {} ", wait.join(", ")), width));
    }
    Some(text)
}

/// The `lines` of a question's card in `room` lines: its last, the keys, always, and above it those around the
/// focused row, else the first ones and a line counting those left out.
fn scrolled(mut lines: Vec<Line<'static>>, room: usize, focused: bool) -> Vec<Line<'static>> {
    if lines.len() <= room || room < 2 {
        return lines;
    }
    let keys = lines.pop().unwrap_or_default();
    let above = room - 1;
    if focused {
        let at = lines.iter().position(|l| l.spans.first().is_some_and(|s| s.content.starts_with('›')));
        let top = (at.unwrap_or_default() + 1).saturating_sub(above);
        let mut shown: Vec<_> = lines.into_iter().skip(top).take(above).collect();
        shown.push(keys);
        return shown;
    }
    let left = lines.len() - (above - 1);
    lines.truncate(above - 1);
    lines.push(Line::styled(format!("… {left} more lines: Enter shows them"), style::dim()));
    lines.push(keys);
    lines
}
