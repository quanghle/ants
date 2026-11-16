//! The command bar's text, the cursor in it and its status line, as keys edit them, and how the bar draws.

use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use ants_proto::ThreadId;

use super::COMMANDS;
use crate::tui::exec::Quoted;
use crate::tui::model::Key;
use crate::tui::{attach, complete, frame};

/// The most lines of text the bar shows at once.
const MAX_LINES: usize = 6;
/// What the empty bar shows.
const HINT: &str = "Ctrl-K commands · @ ant · # channel";

/// How the bar's status line reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// Something runs.
    Busy,
    Done,
    /// Done, with warnings.
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub text: String,
    pub tone: Tone,
}

/// What a key did to the bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Edited {
    /// Nothing for the UI to do.
    Done,
    /// Enter on this text.
    Submit(String),
    /// Ctrl-E on this text: edit it in the editor.
    Editor(String),
}

/// The bar's text, the cursor in it, and its status line.
#[derive(Debug, Default)]
pub struct Bar {
    /// Whether keys go to the bar.
    pub focused: bool,
    chars: Vec<char>,
    /// Where the cursor is, in characters.
    cursor: usize,
    pub status: Option<Status>,
    /// Whether the list of commands shows above the bar.
    pub list: bool,
    /// The completion selected in the list of those of the word at the cursor.
    pub pick: complete::Pick,
    /// The files `/attach` staged for the next message.
    pub staged: attach::Staged,
    /// The message `>` picked on a thread for the next message there to reply to, quoting it.
    pub quoting: Option<(ThreadId, Quoted)>,
}

impl Bar {
    pub fn text(&self) -> String {
        self.chars.iter().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.chars.is_empty()
    }

    /// Replace the text with `text`, the cursor at its end.
    pub fn set_text(&mut self, text: &str) {
        self.chars = text.chars().collect();
        self.cursor = self.chars.len();
    }

    pub fn insert(&mut self, c: char) {
        self.chars.insert(self.cursor, c);
        self.cursor += 1;
    }

    /// Where the word at the cursor starts, and its text up to the cursor.
    pub fn word(&self) -> (usize, String) {
        let start = self.chars[..self.cursor].iter().rposition(|c| c.is_whitespace()).map_or(0, |at| at + 1);
        (start, self.chars[start..self.cursor].iter().collect())
    }

    /// Put `word` and a space in place of the word at the cursor, the cursor after them.
    pub fn complete(&mut self, word: &str) {
        let (start, _) = self.word();
        let end = self.chars[self.cursor..]
            .iter()
            .position(|c| c.is_whitespace())
            .map_or(self.chars.len(), |at| self.cursor + at);
        let spaced = self.chars.get(end).is_some_and(|c| *c == ' ');
        let mut put: Vec<char> = word.chars().collect();
        // A directory completed goes on to its entries.
        if !spaced && !word.ends_with('/') {
            put.push(' ');
        }
        self.cursor = start + put.len() + usize::from(spaced);
        self.chars.splice(start..end, put);
        self.pick = complete::Pick::default();
    }

    pub fn set_status(&mut self, text: impl Into<String>, tone: Tone) {
        self.status = Some(Status { text: text.into(), tone });
    }

    /// Take `key`, the bar being focused.
    pub fn key(&mut self, key: Key) -> Edited {
        match key {
            Key::Char(c) => self.insert(c),
            Key::Newline => self.insert('\n'),
            Key::Backspace if self.cursor > 0 => {
                self.cursor -= 1;
                self.chars.remove(self.cursor);
            }
            Key::Delete if self.cursor < self.chars.len() => {
                self.chars.remove(self.cursor);
            }
            Key::Left => self.cursor = self.cursor.saturating_sub(1),
            Key::Right => self.cursor = (self.cursor + 1).min(self.chars.len()),
            Key::Home | Key::Ctrl('a') => self.cursor = 0,
            Key::End => self.cursor = self.chars.len(),
            Key::Ctrl('u') => self.set_text(""),
            Key::Ctrl('w') => {
                let mut start = self.cursor;
                while start > 0 && self.chars[start - 1].is_whitespace() {
                    start -= 1;
                }
                while start > 0 && !self.chars[start - 1].is_whitespace() {
                    start -= 1;
                }
                self.chars.drain(start..self.cursor);
                self.cursor = start;
            }
            Key::Ctrl('k') => self.list = !self.list,
            Key::Ctrl('e') => return Edited::Editor(self.text()),
            Key::Enter if !self.text().trim().is_empty() => return Edited::Submit(self.text()),
            Key::Esc => {
                self.focused = false;
                self.list = false;
            }
            _ => {}
        }
        Edited::Done
    }

    /// The lines the text takes at `width`, and the cursor's line and column among them.
    fn lines(&self, width: usize) -> (Vec<String>, (usize, usize)) {
        let width = width.max(1);
        let (mut lines, mut line) = (Vec::new(), String::new());
        let (mut column, mut cursor) = (0, (0, 0));
        for (at, c) in self.chars.iter().enumerate() {
            if at == self.cursor {
                cursor = (lines.len(), column);
            }
            if *c == '\n' {
                lines.push(std::mem::take(&mut line));
                column = 0;
                continue;
            }
            if column == width {
                lines.push(std::mem::take(&mut line));
                column = 0;
                if at == self.cursor {
                    cursor = (lines.len(), 0);
                }
            }
            line.push(*c);
            column += 1;
        }
        if self.cursor == self.chars.len() {
            if column == width {
                lines.push(std::mem::take(&mut line));
                column = 0;
            }
            cursor = (lines.len(), column);
        }
        lines.push(line);
        (lines, cursor)
    }

    /// How high the bar is at `width`, its border included.
    pub fn height(&self, width: u16) -> u16 {
        let (lines, _) = self.lines(usize::from(width.saturating_sub(2)));
        u16::try_from(lines.len().min(MAX_LINES)).unwrap_or(1) + 2
    }

    /// Draw the bar in `area`, and above it the `completions` of the word at the cursor, else the list of commands
    /// when shown.
    pub fn draw(&self, frame: &mut Frame, area: Rect, completions: &[complete::Candidate]) {
        let width = usize::from(area.width.saturating_sub(2));
        let (lines, (row, column)) = self.lines(width);
        let top = (row + 1).saturating_sub(MAX_LINES);
        let mut title = match self.staged.names()[..] {
            [] => " > ".to_owned(),
            ref names => format!(" > 📎 {} ", names.join(", ")),
        };
        if let Some((_, quoted)) = &self.quoting {
            let room = usize::from(area.width.saturating_sub(4)).saturating_sub(title.chars().count());
            let line: String = quoted.quote.line().chars().take(room.saturating_sub(1)).collect();
            title = format!("{title}{line} ");
        }
        let mut block = frame::block(title, self.focused);
        if let Some(status) = &self.status {
            let style = match status.tone {
                Tone::Busy => Style::new().fg(Color::Cyan),
                Tone::Done => Style::new().fg(Color::Green),
                Tone::Warn => Style::new().fg(Color::Yellow),
                Tone::Error => Style::new().fg(Color::Red),
            };
            let room = usize::from(area.width.saturating_sub(4));
            let text: String = status.text.replace('\n', " ").chars().take(room).collect();
            block = block.title_bottom(Line::styled(format!(" {text} "), style));
        }
        let shown: Vec<Line> = if self.chars.is_empty() {
            let pad = width.saturating_sub(HINT.chars().count());
            vec![Line::from(vec![Span::raw(" ".repeat(pad)), Span::styled(HINT, frame::dim())])]
        } else {
            lines.into_iter().skip(top).take(MAX_LINES).map(Line::raw).collect()
        };
        let inner = block.inner(area);
        frame.render_widget(Paragraph::new(shown).block(block), area);
        if self.focused {
            let x = inner.x.saturating_add(u16::try_from(column).unwrap_or(u16::MAX));
            let y = inner.y.saturating_add(u16::try_from(row - top).unwrap_or(u16::MAX));
            frame.set_cursor_position(Position { x: x.min(inner.right().saturating_sub(1)), y });
        }
        if !completions.is_empty() {
            complete::draw(frame, area, completions, self.pick.selected);
        } else if self.list {
            draw_list(frame, area);
        }
    }
}

/// The slash commands, over the view just above the bar at `bar`.
fn draw_list(frame: &mut Frame, bar: Rect) {
    let lines: Vec<Line> = COMMANDS
        .iter()
        .map(|(name, args, what)| {
            Line::from(vec![
                Span::styled(format!("/{name:<10}"), Style::new().bold()),
                Span::raw(format!("{args:<24}")),
                Span::styled(*what, frame::dim()),
            ])
        })
        .collect();
    let height = (u16::try_from(lines.len()).unwrap_or(u16::MAX) + 2).min(bar.y);
    let area = Rect { x: bar.x, y: bar.y.saturating_sub(height), width: bar.width.min(84), height };
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).block(frame::block(" commands ".to_owned(), true)), area);
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bar_edits_its_text() {
        let mut bar = Bar::default();
        for c in "say hi there".chars() {
            bar.key(Key::Char(c));
        }
        bar.key(Key::Ctrl('w'));
        assert_eq!(bar.text(), "say hi ");
        bar.key(Key::Backspace);
        bar.key(Key::Left);
        bar.key(Key::Left);
        bar.key(Key::Char('X'));
        bar.key(Key::Delete);
        assert_eq!(bar.text(), "say Xi");
        bar.key(Key::End);
        bar.key(Key::Newline);
        bar.key(Key::Char('!'));
        assert_eq!(bar.key(Key::Enter), Edited::Submit("say Xi\n!".into()));
        assert_eq!(bar.key(Key::Ctrl('e')), Edited::Editor("say Xi\n!".into()));
        bar.key(Key::Ctrl('u'));
        assert_eq!(bar.key(Key::Enter), Edited::Done, "nothing to send");
    }

    #[test]
    fn the_text_wraps_with_the_cursor_where_it_is() {
        let mut bar = Bar::default();
        bar.set_text("abcdef\ngh");
        assert_eq!(bar.lines(4), (vec!["abcd".into(), "ef".into(), "gh".into()], (2, 2)));
        bar.set_text("abcd");
        assert_eq!(bar.lines(4), (vec!["abcd".into(), String::new()], (1, 0)), "a full line moves the cursor on");
        assert_eq!(bar.height(6), 4);
        bar.key(Key::Home);
        assert_eq!(bar.lines(4).1, (0, 0));
        assert_eq!(bar.height(6), 3);
    }
}
