//! The notices of the run over the screen, the latest first, each in full with its time, an error after
//! [`style::ERROR_MARK`]. `↑↓` `j` `k` `PgUp` `PgDn` `Home` `End` scroll
//! them ([`select::step`](super::select::step)); `Esc`, `q` or `N` close them; other keys do nothing.

use std::cell::Cell;

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use super::help::centered;
use super::markdown;
use super::model::{Key, Level, Notice};
use super::select::{PAGE, step};
use super::style::{self, block, dim};

/// The last line, under the notices scrolled.
pub const CLOSE: &str = "↑↓ j k PgUp PgDn Home End scroll · Esc, q or N closes the notices";
/// How many notices of the run are kept, the earliest dropped first.
pub const KEPT: usize = 200;
/// How wide the notices are at most.
const WIDTH: u16 = 100;
/// How wide the column of times is.
const TIME: usize = 8;

#[derive(Debug, Default)]
pub struct Log {
    /// How many rows the notices are scrolled down.
    scroll: u16,
    /// How far they could scroll when last drawn.
    scrollable: Cell<u16>,
}

impl Log {
    /// Take `key`: whether the notices stay open.
    pub fn key(&mut self, key: Key) -> bool {
        if let Key::Esc | Key::Char('q' | 'N') = key {
            return false;
        }
        if let Some(to) = step(key, usize::from(self.scroll), Some(PAGE)) {
            self.scroll = u16::try_from(to).unwrap_or(u16::MAX).min(self.scrollable.get());
        }
        true
    }

    /// Draw `notices`, the earliest first, between the header and the footer.
    pub fn draw(&self, frame: &mut Frame, notices: &[Notice]) {
        let screen = frame.area();
        let between = Rect { y: screen.y + 1, height: screen.height.saturating_sub(2), ..screen };
        let width = WIDTH.min(between.width);
        let rows = lines(notices, usize::from(width.saturating_sub(2)));
        let tall = u16::try_from(rows.len()).unwrap_or(u16::MAX).saturating_add(3);
        let area = centered(between, width, tall);
        let outer = block(" Notices ".to_owned(), true);
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

/// The rows of `notices` at most `width` wide, the latest first: its local time, then its text wrapped beside it.
fn lines(notices: &[Notice], width: usize) -> Vec<Line<'static>> {
    if notices.is_empty() {
        return vec![Line::styled(" no notice yet", dim())];
    }
    let mut lines = Vec::new();
    for notice in notices.iter().rev() {
        let (text, color) = match notice.level {
            Level::Error => (format!("{}{}", style::ERROR_MARK, notice.text), style::ERROR),
            Level::Info => (notice.text.clone(), style::INFO),
        };
        let time = notice.at.with_timezone(&chrono::Local).format("%H:%M:%S").to_string();
        let room = width.saturating_sub(TIME + 3).max(10);
        let rows = text.split('\n').flat_map(|line| markdown::wrap(&[Span::raw(line.to_owned())], room));
        for (i, row) in rows.enumerate() {
            let time = if i == 0 { time.as_str() } else { "" };
            let mut line = vec![Span::styled(format!(" {time:>TIME$}  "), dim())];
            line.extend(row.into_iter().map(|s| s.style(Style::new().fg(color))));
            lines.push(Line::from(line));
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::at;
    use crate::tui::model::{Action, Level};
    use crate::tui::test_support::{model, screen};

    #[test]
    fn n_lists_the_notices_of_the_run_latest_first_each_in_full_with_its_time() {
        let mut m = model();
        let long = "the daemon refused the order: ".to_owned() + &"far too long a reason ".repeat(8);
        m.notify(long.trim_end(), Level::Error, at(1));
        m.key(Key::Down, at(2));
        m.notify("tool calls: none", Level::Info, at(3));
        assert_eq!(m.key(Key::Char('N'), at(4)), None);
        let shown = screen(&m, 80, 24, at(9));
        let (latest, earlier) = (shown.find("tool calls: none").unwrap(), shown.find("✗ the daemon").unwrap());
        assert!(latest < earlier, "the latest first: {shown}");
        let time = |t| at(t).with_timezone(&chrono::Local).format("%H:%M:%S").to_string();
        assert!(shown.contains(&format!("{}  tool calls: none", time(3))), "{shown}");
        assert!(shown.contains(&time(1)), "{shown}");
        assert!(shown.contains("far too long a reason far too long a reason"), "in full: {shown}");
        assert!(shown.contains("           reason "), "none of it cut: {shown}");
        assert!(shown.contains(CLOSE), "{shown}");
        assert_eq!(m.key(Key::Char('x'), at(5)), None, "other keys do nothing");
        assert!(m.overlays.iter().any(|o| matches!(o, crate::tui::model::Overlay::Log(_))));
        assert_eq!(m.key(Key::Char('q'), at(5)), None, "q closes the notices");
        assert!(m.overlays.is_empty());
        assert_eq!(m.key(Key::Char('q'), at(5)), None, "and does not quit right after");
        m.key(Key::Char('N'), at(9));
        assert_eq!(m.key(Key::Interrupt, at(9)), None);
        assert!(m.overlays.is_empty(), "Ctrl-C closes them too");
        assert_eq!(m.key(Key::Char('q'), at(9)), Some(Action::Quit));
    }

    #[test]
    fn a_paste_while_the_notices_show_is_dropped() {
        let mut m = model();
        m.notify("tool calls: none", Level::Info, at(1));
        m.key(Key::Char('N'), at(2));
        m.paste("hello", at(2));
        assert_eq!((m.ui.bar.text(), m.ui.bar.focused), (String::new(), false), "the bar takes none of it");
        assert!(!m.overlays.is_empty(), "the notices still show");
    }

    #[test]
    fn the_notices_scroll_by_row_by_page_and_to_either_end_within_their_rows() {
        let mut log = Log::default();
        log.scrollable.set(25);
        let steps = [
            (Key::Up, 0),
            (Key::Char('j'), 1),
            (Key::Down, 2),
            (Key::Char('k'), 1),
            (Key::PageDown, 11),
            (Key::PageUp, 1),
            (Key::End, 25),
            (Key::Down, 25),
            (Key::Char('x'), 25),
            (Key::Home, 0),
        ];
        for (key, scroll) in steps {
            assert!(log.key(key), "{key:?} keeps the notices");
            assert_eq!(log.scroll, scroll, "{key:?}");
        }
    }

    #[test]
    fn only_the_latest_notices_are_kept() {
        let mut m = model();
        for i in 0..=KEPT {
            m.notify(format!("notice {i}"), Level::Info, at(1));
        }
        assert_eq!(m.ui.notices.len(), KEPT);
        assert_eq!(m.ui.notices[0].text, "notice 1");
    }
}
