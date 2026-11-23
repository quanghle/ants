//! The help over the screen: the keys of the views on screen and of the command bar, then those that act everywhere,
//! from the key table ([`keymap`]), wrapped to the width, those that cannot act now dim with why. `↑↓` `j` `k` `PgUp`
//! `PgDn` `Home` `End` scroll it ([`select::step`](super::select::step)); `Esc`, `q` or `?` close it; other keys do
//! nothing.

use std::cell::Cell;

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use super::keymap::{self, Entry, View};
use super::markdown;
use super::model::{Key, Model};
use super::select::{PAGE, step};
use super::style::{block, dim};

/// The help's last line, under the rows scrolled.
pub const CLOSE: &str = "↑↓ j k PgUp PgDn Home End scroll · Esc, q or ? closes this help";
/// How wide the help is at most.
const WIDTH: u16 = 100;
/// How wide the column of keys is.
const LABEL: usize = 11;

#[derive(Debug, Default)]
pub struct Help {
    /// How many rows the help is scrolled down.
    scroll: u16,
    /// How far it could scroll when last drawn.
    scrollable: Cell<u16>,
}

impl Help {
    /// Take `key`: whether the help stays open.
    pub fn key(&mut self, key: Key) -> bool {
        if let Key::Esc | Key::Char('q' | '?') = key {
            return false;
        }
        if let Some(to) = step(key, usize::from(self.scroll), Some(PAGE)) {
            self.scroll = u16::try_from(to).unwrap_or(u16::MAX).min(self.scrollable.get());
        }
        true
    }

    /// Draw the help between the header and the footer.
    pub fn draw(&self, frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
        let screen = frame.area();
        let between = Rect { y: screen.y + 1, height: screen.height.saturating_sub(2), ..screen };
        let width = WIDTH.min(between.width);
        let rows = lines(model, now, usize::from(width.saturating_sub(2)));
        let tall = u16::try_from(rows.len()).unwrap_or(u16::MAX).saturating_add(3);
        let area = centered(between, width, tall);
        let outer = block(" Keys ".to_owned(), true);
        let inner = outer.inner(area);
        let [body, close] = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(inner);
        let scrollable = u16::try_from(rows.len()).unwrap_or(u16::MAX).saturating_sub(body.height);
        self.scrollable.set(scrollable);
        frame.render_widget(Clear, area);
        frame.render_widget(outer, area);
        frame.render_widget(Paragraph::new(rows).scroll((self.scroll.min(scrollable), 0)), body);
        frame.render_widget(Paragraph::new(Line::styled(format!(" {CLOSE}"), dim())), close);
    }
}

/// The views whose keys the help lists on `model`, those on screen first: on an ant, its other panes' too; then the
/// command bar's.
fn views(model: &Model) -> Vec<View> {
    let mut views = keymap::views(model);
    if views.contains(&View::Ant) {
        let panes = [View::Work, View::AntQueue, View::Memories, View::Mcp, View::Settings];
        views.extend(panes.into_iter().filter(|v| !views.contains(v)).collect::<Vec<_>>());
    }
    views.push(View::Bar);
    views
}

/// The help's rows at most `width` wide: a heading for each view, then its keys, then those of everywhere.
fn lines(model: &Model, now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
    let shown = keymap::views(model);
    let mut lines = Vec::new();
    let sections = views(model)
        .into_iter()
        .map(|v| (format!("In {}", v.title()), keymap::entries(v), v == View::Bar || shown.contains(&v)));
    for (heading, entries, on) in sections.chain([("Everywhere".to_owned(), keymap::GLOBAL, true)]) {
        if !lines.is_empty() {
            lines.push(Line::raw(""));
        }
        lines.push(Line::styled(format!(" {heading}"), Style::new().bold()));
        for entry in entries {
            let why = if on { entry.applies(model, now).err() } else { Some("on another pane") };
            lines.extend(row(entry, why, width));
        }
    }
    lines
}

/// `entry`'s row wrapped to `width`, dim with `why` when it cannot act now.
fn row(entry: &Entry, why: Option<&str>, width: usize) -> Vec<Line<'static>> {
    let text = match why {
        Some(why) => format!("{} ({why})", entry.long),
        None => entry.long.to_owned(),
    };
    let style = if why.is_some() { dim() } else { Style::new() };
    let wrapped = markdown::wrap(&[Span::styled(text, style)], width.saturating_sub(LABEL + 3).max(10));
    let label = if why.is_some() { dim() } else { Style::new().bold() };
    wrapped
        .into_iter()
        .enumerate()
        .map(|(i, spans)| {
            let key = if i == 0 { entry.label } else { "" };
            let mut line = vec![Span::styled(format!(" {key:>LABEL$}  "), label)];
            line.extend(spans);
            Line::from(line)
        })
        .collect()
}

/// An area of at most `width` by `height` in the middle of `area`.
pub fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let (width, height) = (width.min(area.width), height.min(area.height));
    Rect { x: area.x + (area.width - width) / 2, y: area.y + (area.height - height) / 2, width, height }
}

#[cfg(test)]
mod tests;
