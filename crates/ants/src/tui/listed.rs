//! A list under lines about it, `a` or `r` on its selected item asking to confirm what to do with it, which `y` then
//! does: the credentials asked for ([`super::credentials`]) and what is held for the human's consent
//! ([`super::held`]).

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Paragraph, Wrap};

use super::armed::Armed;
use super::model::{Data, Key};
use super::select::Selection;
use super::style;

#[derive(Debug)]
pub struct Listed<K, T> {
    selected: Selection<K>,
    /// What the next `y` does.
    armed: Armed<T>,
}

impl<K, T> Default for Listed<K, T> {
    fn default() -> Self {
        Self { selected: Selection::default(), armed: Armed::default() }
    }
}

/// What a list shows of its items.
pub struct Shown<'a, K, T> {
    pub title: String,
    /// The lines about the list.
    pub about: Vec<Line<'static>>,
    /// What is being done, under them.
    pub busy: Option<&'a T>,
    /// How what is done reads after "y to".
    pub describe: fn(&T) -> String,
    /// The line saying the list is empty, if it says so.
    pub empty: Option<&'static str>,
    /// The rows of the items, and the items' keys.
    pub rows: Vec<Text<'static>>,
    pub keys: &'a [K],
    /// The list's name, as the mouse knows it.
    pub name: &'static str,
}

impl<K: PartialEq + Clone, T> Listed<K, T> {
    /// What `y` does, while it waits.
    pub fn armed(&self) -> Option<&T> {
        self.armed.get()
    }

    /// Drop what `y` would do, if anything: whether there was something.
    pub fn disarm(&mut self) -> bool {
        self.armed.disarm()
    }

    /// Take `key` on the list of the items `keys`: `a` or `r` asks to confirm what `asked` makes of the selected item,
    /// at, `r` saying so, unless `busy`; `y` then does it, and Up, Down, Home and End move. What `y` does.
    pub fn key(&mut self, key: Key, keys: &[K], busy: bool, asked: impl FnOnce(usize, bool) -> Option<T>) -> Option<T> {
        let at = self.selected.at(keys);
        let armed = self.armed.take();
        match key {
            Key::Char('a' | 'r') if !busy => self.armed.arm(asked(at, key == Key::Char('r'))),
            Key::Char('y') => return armed,
            _ => _ = self.selected.step(key, keys, None),
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, shown: Shown<K, T>, focused: bool) {
        let block = style::block(shown.title, focused);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let mut about = shown.about;
        if let Some(busy) = shown.busy {
            about.push(Line::styled(format!("{}…", (shown.describe)(busy)), Style::new().fg(Color::Cyan)));
        }
        if let Some(armed) = self.armed.get() {
            let line = format!("y to {}", (shown.describe)(armed));
            about.push(Line::styled(line, Style::new().fg(Color::Yellow).bold()));
        }
        if shown.keys.is_empty() {
            about.extend(shown.empty.map(Line::raw));
            frame.render_widget(Paragraph::new(about).wrap(Wrap { trim: false }), inner);
            return;
        }
        // The lines but a long reason for a failed gathering fit one row each.
        let tall = u16::try_from(about.len() * 2).unwrap_or(u16::MAX);
        let [top, list] = Layout::vertical([Constraint::Max(tall), Constraint::Min(0)]).areas(inner);
        frame.render_widget(Paragraph::new(about).wrap(Wrap { trim: false }), top);
        let selected = self.selected.at(shown.keys);
        super::mouse::list(frame, list, data, shown.name, shown.rows, selected, focused);
    }
}
