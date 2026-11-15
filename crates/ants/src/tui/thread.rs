//! An open thread: its title, channels, state and the ants' progress on it, then its messages as `ants thread show`
//! renders them, the latest at the bottom unless scrolled up. The timeline (ants-ofmi.5) and markdown (ants-ofmi.10)
//! are to replace the plain rendering.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use super::frame::{self, ago};
use super::model::{Data, Key, To};
use super::work;
use crate::live::History;

#[derive(Debug)]
pub struct Page {
    pub history: History,
    /// How many lines up from the latest the view is scrolled.
    pub scroll: usize,
}

impl Page {
    pub fn new(thread: ants_proto::ThreadId) -> Self {
        Self { history: History::new(thread), scroll: 0 }
    }

    pub const KEYS: &str = "↑↓ PgUp/PgDn scroll · End latest · Esc back";

    pub fn key(&mut self, key: Key) -> Option<To> {
        self.scroll = match key {
            Key::Up | Key::Char('k') => self.scroll + 1,
            Key::Down | Key::Char('j') => self.scroll.saturating_sub(1),
            Key::PageUp => self.scroll + 10,
            Key::PageDown => self.scroll.saturating_sub(10),
            Key::Home => crate::thread::render(&self.history.entries).lines().count(),
            Key::End => 0,
            _ => return None,
        };
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let thread = &self.history.thread;
        let summary = data.live.summary(thread);
        let mut title = format!(" {} ", data.title(thread));
        if let Some(s) = summary.filter(|s| !s.channels.is_empty()) {
            let channels: Vec<String> = s.channels.iter().map(|c| format!("#{c}")).collect();
            title = format!("{title}· {} ", channels.join(" "));
        }
        let width = usize::from(area.width.saturating_sub(2));
        let mut lines = vec![work::why(data, thread)];
        if let Some((line, at)) = data.live.progress_of(thread, width.saturating_sub(16)) {
            lines.push(Line::styled(format!("▶ {line} ({})", ago(at, now)), Style::new().fg(Color::Green)));
        }
        if self.history.loading {
            lines.push(Line::styled("reading the thread…", frame::dim()));
        }
        let rendered = crate::thread::render(&self.history.entries);
        lines.extend(rendered.lines().flat_map(|l| frame::wrap(l, width)).map(Line::raw));
        let height = usize::from(area.height.saturating_sub(2));
        let top = lines.len().saturating_sub(height).saturating_sub(self.scroll);
        let paragraph = Paragraph::new(lines)
            .block(frame::block(title, focused))
            .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;

    #[test]
    fn scrolling_goes_up_from_the_latest_and_back() {
        let mut page = Page::new(ThreadId::generate());
        page.key(Key::PageUp);
        page.key(Key::Up);
        assert_eq!(page.scroll, 11);
        page.key(Key::PageDown);
        page.key(Key::Down);
        page.key(Key::Down);
        assert_eq!(page.scroll, 0, "not below the latest");
        page.key(Key::Home);
        page.key(Key::End);
        assert_eq!(page.scroll, 0);
        assert_eq!(page.key(Key::Enter), None);
    }
}
