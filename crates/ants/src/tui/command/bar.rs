//! The command bar's text, the cursor in it and its status line, as keys edit them, and how the bar draws.

use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use ants_proto::{ApprovalRequest, ThreadId};

use super::COMMANDS;
use crate::tui::exec::Quoted;
use crate::tui::model::Key;
use crate::tui::{attach, complete, style};

/// The most lines of text the bar shows at once.
const MAX_LINES: usize = 6;
/// A paste of more characters than this, or of more lines than `PASTE_LINES`, shows as a placeholder.
const PASTE_CHARS: usize = 800;
const PASTE_LINES: usize = 3;
/// The first of the private-use characters standing in the text for the pastes shown as placeholders.
const PASTED: u32 = 0xF_0000;
/// How many pastes there may be placeholders for: the characters of the Supplementary Private Use Area-A.
const PASTES: usize = 0xFFFE;

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
    /// The pastes shown as placeholders, each standing in `chars` as the character `PASTED` plus its index.
    pastes: Vec<String>,
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
    /// The tool request the text is a note about, n on its card having opened the bar.
    pub noting: Option<Note>,
    /// The notes being sent, by their text, each with its request, to come back as a reply if sending fails.
    notes: Vec<(String, Box<ApprovalRequest>)>,
}

/// What Enter does with the note in the bar on a tool request.
#[derive(Debug, Clone, PartialEq)]
pub enum Note {
    /// Reject the request, then send the note to its ant on its thread once the rejection is stored.
    Reject(Box<ApprovalRequest>),
    /// Send the note to the request's ant on its thread, the rejection having been refused.
    Reply(Box<ApprovalRequest>),
}

impl Note {
    pub fn request(&self) -> &ApprovalRequest {
        match self {
            Self::Reject(request) | Self::Reply(request) => request,
        }
    }
}

impl Bar {
    /// Remember that `text` being sent is the note on `request`.
    pub fn sending_note(&mut self, text: &str, request: Box<ApprovalRequest>) {
        self.notes.push((text.to_owned(), request));
    }

    /// Take that `text` was sent or failed: the request it was the note on, if it was one.
    pub fn note_sent(&mut self, text: &str) -> Option<Box<ApprovalRequest>> {
        let at = self.notes.iter().position(|(sent, _)| sent == text)?;
        Some(self.notes.remove(at).1)
    }

    /// The text, its pastes in full.
    pub fn text(&self) -> String {
        let mut text = String::with_capacity(self.chars.len());
        for c in &self.chars {
            match self.pasted(*c) {
                Some(paste) => text.push_str(paste),
                None => text.push(*c),
            }
        }
        text
    }

    /// The text with its pastes as the characters standing for them, which `set_raw` takes back.
    pub fn raw(&self) -> String {
        self.chars.iter().collect()
    }

    /// The paste `c` stands for, if any.
    fn pasted(&self, c: char) -> Option<&str> {
        let at = u32::from(c).checked_sub(PASTED)?;
        self.pastes.get(usize::try_from(at).ok()?).map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.chars.is_empty()
    }

    /// Replace the text with `text`, the cursor at its end.
    pub fn set_text(&mut self, text: &str) {
        self.pastes.clear();
        self.set_raw(text);
    }

    /// Replace the text with `raw`, as `raw` gave it, keeping the pastes.
    pub fn set_raw(&mut self, raw: &str) {
        self.chars = raw.chars().collect();
        self.cursor = self.chars.len();
    }

    pub fn insert(&mut self, c: char) {
        self.chars.insert(self.cursor, c);
        self.cursor += 1;
    }

    /// Put `text` at the cursor, the cursor after it: as is when short, else as a placeholder that the cursor and
    /// deleting take as one character and that sending puts back.
    pub fn paste(&mut self, text: &str) {
        let long = text.chars().count() > PASTE_CHARS || text.lines().count() > PASTE_LINES;
        if long && self.pastes.len() < PASTES {
            let c = u32::try_from(self.pastes.len()).ok().and_then(|at| char::from_u32(PASTED + at));
            if let Some(c) = c {
                self.pastes.push(text.to_owned());
                self.insert(c);
                return;
            }
        }
        // Shown as spaces, since the bar counts a character a column.
        let text = text.replace('\t', "    ");
        let at = self.cursor;
        self.chars.splice(at..at, text.chars());
        self.cursor = at + text.chars().count();
    }

    /// Whether `c` ends a word: whitespace, or a paste's placeholder.
    fn breaks(&self, c: char) -> bool {
        c.is_whitespace() || self.pasted(c).is_some()
    }

    /// Where the word at the cursor starts, and its text up to the cursor.
    pub fn word(&self) -> (usize, String) {
        let start = self.chars[..self.cursor].iter().rposition(|c| self.breaks(*c)).map_or(0, |at| at + 1);
        (start, self.chars[start..self.cursor].iter().collect())
    }

    /// Put `word` and a space in place of the word at the cursor, the cursor after them.
    pub fn complete(&mut self, word: &str) {
        let (start, _) = self.word();
        let end = self.chars[self.cursor..]
            .iter()
            .position(|c| self.breaks(*c))
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
            let mut one = [0; 4];
            let label = self.pasted(*c).map(|paste| placeholder(u32::from(*c) - PASTED + 1, paste));
            let shown = label.as_deref().unwrap_or_else(|| c.encode_utf8(&mut one));
            for (nth, c) in shown.chars().enumerate() {
                if column == width {
                    lines.push(std::mem::take(&mut line));
                    column = 0;
                    if at == self.cursor && nth == 0 {
                        cursor = (lines.len(), 0);
                    }
                }
                line.push(c);
                column += 1;
            }
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
    /// Draw the bar in `area`, `hint` on it while it is empty.
    pub fn draw(&self, frame: &mut Frame, area: Rect, completions: &[complete::Candidate], hint: &str) {
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
        match &self.noting {
            Some(Note::Reject(request)) => {
                title = format!("{title}reject {} with a note to {} ", crate::tools::qualified(request), request.ant);
            }
            Some(Note::Reply(request)) => title = format!("{title}to {} on the request's thread ", request.ant),
            None => {}
        }
        let mut block = style::block(title, self.focused);
        if let Some(status) = &self.status {
            let style = match status.tone {
                Tone::Busy => Style::new().fg(style::INFO),
                Tone::Done => Style::new().fg(style::CONNECTED),
                Tone::Warn => Style::new().fg(style::WARNING),
                Tone::Error => Style::new().fg(style::ERROR),
            };
            let room = usize::from(area.width.saturating_sub(4));
            let text: String = status.text.replace('\n', " ").chars().take(room).collect();
            block = block.title_bottom(Line::styled(format!(" {text} "), style));
        }
        let shown: Vec<Line> = if self.chars.is_empty() {
            let hint = style::hints(hint, width);
            let pad = width.saturating_sub(hint.chars().count());
            vec![Line::from(vec![Span::raw(" ".repeat(pad)), Span::styled(hint, style::dim())])]
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

/// How the bar shows the `nth` paste, `paste`.
fn placeholder(nth: u32, paste: &str) -> String {
    match paste.lines().count() {
        0 | 1 => format!("[Pasted text #{nth} {} chars]", paste.chars().count()),
        lines => format!("[Pasted text #{nth} +{lines} lines]"),
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
                Span::styled(*what, style::dim()),
            ])
        })
        .collect();
    let height = (u16::try_from(lines.len()).unwrap_or(u16::MAX) + 2).min(bar.y);
    let area = Rect { x: bar.x, y: bar.y.saturating_sub(height), width: bar.width.min(84), height };
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).block(style::block(" commands ".to_owned(), true)), area);
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
    fn a_long_paste_shows_as_a_placeholder_taken_as_one_character_and_sent_in_full() {
        let mut bar = Bar::default();
        bar.paste("a\nb\nc");
        assert_eq!(bar.text(), "a\nb\nc", "a short paste goes in as is");
        bar.set_text("see ");
        let log = "line\n".repeat(120);
        bar.paste(&log);
        bar.paste(&"x".repeat(801));
        bar.insert('!');
        assert_eq!(bar.lines(80).0, vec!["see [Pasted text #1 +120 lines][Pasted text #2 801 chars]!".to_owned()]);
        assert_eq!(bar.lines(80).1, (0, 58));
        assert_eq!(bar.text(), format!("see {log}{}!", "x".repeat(801)));
        assert_eq!(bar.key(Key::Ctrl('e')), Edited::Editor(bar.text()));
        bar.key(Key::Left);
        bar.key(Key::Backspace);
        assert_eq!(bar.key(Key::Enter), Edited::Submit(format!("see {log}!")), "deleted whole");
        bar.set_raw(&format!("@dev-a {}", bar.raw()));
        assert_eq!(bar.text(), format!("@dev-a see {log}!"), "the raw text keeps its pastes");
        bar.set_text("");
        bar.paste(&log);
        assert_eq!(bar.lines(80).0, vec!["[Pasted text #1 +120 lines]".to_owned()], "numbered afresh");
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
