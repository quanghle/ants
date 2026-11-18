//! The command bar's text, the cursor in it and its status line, as keys edit them, and how the bar draws.

use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use ants_proto::{ApprovalRequest, ThreadId};
use unicode_width::UnicodeWidthChar as _;

use super::COMMANDS;
use crate::tui::exec::Quoted;
use crate::tui::model::Key;
use crate::tui::{attach, complete, style};

/// The most lines of text the bar shows at once.
const MAX_LINES: usize = 6;
/// What the empty bar shows.
const HINT: &str = "/ commands · @ ant · # channel · Ctrl-J new line";
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
    /// Ctrl-G or Ctrl-X Ctrl-E on this text: edit it in the editor.
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
    /// What a key deleted last, for Ctrl-Y to put back.
    killed: Vec<Killed>,
    /// Whether Ctrl-X came last, so that Ctrl-E after it opens the editor.
    ctrl_x: bool,
}

/// A character deleted, or a paste whose placeholder was.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Killed {
    Char(char),
    Paste(String),
}

/// What the bar holds for a conversation not shown: its text, with the cursor, and what Enter does with it beyond
/// where the conversation sends.
#[derive(Debug, Default)]
pub struct Draft {
    chars: Vec<char>,
    pastes: Vec<String>,
    cursor: usize,
    quoting: Option<(ThreadId, Quoted)>,
    noting: Option<Note>,
}

impl Draft {
    /// The draft of `text`, replying to the message `quoting` or the note `noting` if any.
    pub fn of(text: &str, quoting: Option<(ThreadId, Quoted)>, noting: Option<Note>) -> Self {
        let chars: Vec<char> = text.chars().collect();
        Self { cursor: chars.len(), chars, pastes: Vec::new(), quoting, noting }
    }

    pub fn is_empty(&self) -> bool {
        self.chars.is_empty() && self.quoting.is_none() && self.noting.is_none()
    }

    /// The text, its pastes in full.
    pub fn text(&self) -> String {
        let paste = |c: &char| self.pastes.get(usize::try_from(u32::from(*c).checked_sub(PASTED)?).ok()?);
        self.chars.iter().fold(String::new(), |mut text, c| {
            match paste(c) {
                Some(paste) => text.push_str(paste),
                None => text.push(*c),
            }
            text
        })
    }
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
    /// Hold `draft` in place of the draft held, which is returned.
    pub fn swap(&mut self, draft: Draft) -> Draft {
        let held = Draft {
            chars: std::mem::replace(&mut self.chars, draft.chars),
            pastes: std::mem::replace(&mut self.pastes, draft.pastes),
            cursor: std::mem::replace(&mut self.cursor, draft.cursor),
            quoting: std::mem::replace(&mut self.quoting, draft.quoting),
            noting: std::mem::replace(&mut self.noting, draft.noting),
        };
        self.pick = complete::Pick::default();
        held
    }

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
        if long && let Some(c) = self.stand_in(text) {
            self.insert(c);
            return;
        }
        // Shown as spaces, a tab having no width of its own.
        let text = text.replace('\t', "    ");
        let at = self.cursor;
        self.chars.splice(at..at, text.chars());
        self.cursor = at + text.chars().count();
    }

    /// The placeholder character for the paste `text`, one already standing for it or a new one; None once there are
    /// no more.
    fn stand_in(&mut self, text: &str) -> Option<char> {
        let at = match self.pastes.iter().position(|paste| paste == text) {
            Some(at) => at,
            None if self.pastes.len() < PASTES => {
                self.pastes.push(text.to_owned());
                self.pastes.len() - 1
            }
            None => return None,
        };
        u32::try_from(at).ok().and_then(|at| char::from_u32(PASTED + at))
    }

    /// Where the line of the cursor starts.
    fn line_start(&self) -> usize {
        self.chars[..self.cursor].iter().rposition(|c| *c == '\n').map_or(0, |at| at + 1)
    }

    /// Where the line of the cursor ends, before its line break.
    fn line_end(&self) -> usize {
        self.chars[self.cursor..].iter().position(|c| *c == '\n').map_or(self.chars.len(), |at| self.cursor + at)
    }

    /// Where the word before the cursor starts, as Alt-B goes: past what is no letter or digit, then past those that
    /// are.
    fn word_back(&self) -> usize {
        let mut at = self.cursor;
        while at > 0 && !self.chars[at - 1].is_alphanumeric() {
            at -= 1;
        }
        while at > 0 && self.chars[at - 1].is_alphanumeric() {
            at -= 1;
        }
        at
    }

    /// Where the word after the cursor ends, as Alt-F goes.
    fn word_forward(&self) -> usize {
        let mut at = self.cursor;
        while at < self.chars.len() && !self.chars[at].is_alphanumeric() {
            at += 1;
        }
        while at < self.chars.len() && self.chars[at].is_alphanumeric() {
            at += 1;
        }
        at
    }

    /// Delete the characters in `range`, kept for Ctrl-Y, the cursor where they were.
    fn kill(&mut self, range: std::ops::Range<usize>) {
        if range.is_empty() {
            return;
        }
        self.killed = self.chars[range.clone()]
            .iter()
            .map(|c| self.pasted(*c).map_or(Killed::Char(*c), |paste| Killed::Paste(paste.to_owned())))
            .collect();
        self.cursor = range.start;
        self.chars.drain(range);
    }

    /// Put back at the cursor what was deleted last.
    fn yank(&mut self) {
        for killed in self.killed.clone() {
            match killed {
                Killed::Char(c) => self.insert(c),
                Killed::Paste(text) => match self.stand_in(&text) {
                    Some(c) => self.insert(c),
                    None => text.chars().for_each(|c| self.insert(c)),
                },
            }
        }
    }

    /// Move the cursor to the line above (`up`) or below, if any, as near its column as that line allows.
    fn line_up_down(&mut self, up: bool) {
        let (start, end) = (self.line_start(), self.line_end());
        let column = self.cursor - start;
        let (from, to) = if up {
            let Some(before) = start.checked_sub(1) else { return };
            let from = self.chars[..before].iter().rposition(|c| *c == '\n').map_or(0, |at| at + 1);
            (from, before)
        } else {
            if end == self.chars.len() {
                return;
            }
            let to = self.chars[end + 1..].iter().position(|c| *c == '\n').map_or(self.chars.len(), |at| end + 1 + at);
            (end + 1, to)
        };
        self.cursor = from + column.min(to - from);
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

    /// Take `key`, the bar being focused: it edits as a shell's line does.
    pub fn key(&mut self, key: Key) -> Edited {
        let after_ctrl_x = std::mem::take(&mut self.ctrl_x);
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
            Key::Up => self.line_up_down(true),
            Key::Down => self.line_up_down(false),
            Key::Home | Key::Ctrl('a') => self.cursor = self.line_start(),
            Key::Ctrl('e') if after_ctrl_x => return Edited::Editor(self.text()),
            Key::End | Key::Ctrl('e') => self.cursor = self.line_end(),
            Key::Alt('b' | 'B') | Key::WordLeft => self.cursor = self.word_back(),
            Key::Alt('f' | 'F') | Key::WordRight => self.cursor = self.word_forward(),
            // At the end of a line, Ctrl-K takes its line break, and Ctrl-U at its start the one before it.
            Key::Ctrl('k') => {
                let end = self.line_end();
                let end = if end == self.cursor { (end + 1).min(self.chars.len()) } else { end };
                self.kill(self.cursor..end);
            }
            Key::Ctrl('u') => {
                let start = self.line_start();
                let start = if start == self.cursor { start.saturating_sub(1) } else { start };
                self.kill(start..self.cursor);
            }
            Key::Ctrl('w') => {
                let mut start = self.cursor;
                while start > 0 && self.chars[start - 1].is_whitespace() {
                    start -= 1;
                }
                while start > 0 && !self.chars[start - 1].is_whitespace() {
                    start -= 1;
                }
                self.kill(start..self.cursor);
            }
            Key::Alt('d' | 'D') => self.kill(self.cursor..self.word_forward()),
            Key::AltBackspace => self.kill(self.word_back()..self.cursor),
            Key::Ctrl('y') => self.yank(),
            Key::Ctrl('x') => self.ctrl_x = true,
            Key::Ctrl('g') => return Edited::Editor(self.text()),
            // A backslash before Enter makes it a new line, as in a shell.
            Key::Enter if self.cursor > 0 && self.chars[self.cursor - 1] == '\\' => self.chars[self.cursor - 1] = '\n',
            Key::Enter if !self.text().trim().is_empty() => return Edited::Submit(self.text()),
            Key::Esc => {
                self.focused = false;
                self.list = false;
            }
            _ => {}
        }
        Edited::Done
    }

    /// The lines the text takes at `width` columns, wrapped after a space where a word does not fit and within the
    /// word where it is longer than the line, and the cursor's line and column among them. A character of no width,
    /// such as a combining mark, goes with the one before it; spaces past the edge hang there.
    fn lines(&self, width: usize) -> (Vec<String>, (usize, usize)) {
        /// A character shown, the index in the text of the one it shows, and its columns.
        struct Cell {
            at: usize,
            c: char,
            columns: usize,
        }
        let width = width.max(1);
        let (mut rows, mut row): (Vec<Vec<Cell>>, Vec<Cell>) = (Vec::new(), Vec::new());
        // The row's column, and where it may break: after its last space.
        let (mut column, mut after_space) = (0, None);
        for (at, c) in self.chars.iter().enumerate() {
            if *c == '\n' {
                row.push(Cell { at, c: '\n', columns: 0 });
                rows.push(std::mem::take(&mut row));
                (column, after_space) = (0, None);
                continue;
            }
            let mut one = [0; 4];
            let label = self.pasted(*c).map(|paste| placeholder(u32::from(*c) - PASTED + 1, paste));
            let shown = label.as_deref().unwrap_or_else(|| c.encode_utf8(&mut one));
            for c in shown.chars() {
                let columns = c.width().unwrap_or(0);
                if columns > 0 && column > 0 && column + columns > width && !c.is_whitespace() {
                    let moved = row.split_off(after_space.unwrap_or(row.len()));
                    column = moved.iter().map(|cell| cell.columns).sum();
                    rows.push(std::mem::replace(&mut row, moved));
                    after_space = None;
                }
                row.push(Cell { at, c, columns });
                column += columns;
                if c.is_whitespace() {
                    after_space = Some(row.len());
                }
            }
        }
        rows.push(row);
        let found = rows.iter().enumerate().find_map(|(nth, row)| {
            let at = row.iter().position(|cell| cell.at >= self.cursor)?;
            Some((nth, row[..at].iter().map(|cell| cell.columns).sum()))
        });
        let cursor = found.unwrap_or_else(|| {
            if column >= width {
                rows.push(Vec::new());
                (rows.len() - 1, 0)
            } else {
                (rows.len() - 1, column)
            }
        });
        let lines = rows.iter().map(|row| row.iter().map(|cell| cell.c).filter(|c| *c != '\n').collect()).collect();
        (lines, cursor)
    }

    /// How high the bar is at `width`, its border included.
    pub fn height(&self, width: u16) -> u16 {
        let (lines, _) = self.lines(usize::from(width.saturating_sub(2)));
        u16::try_from(lines.len().min(MAX_LINES)).unwrap_or(1) + 2
    }

    /// Draw the bar in `area`, its title naming where Enter sends (`to`), and above it the `completions` of the word
    /// at the cursor, else the list of commands when shown.
    pub fn draw(&self, frame: &mut Frame, area: Rect, completions: &[complete::Candidate], to: &str) {
        let width = usize::from(area.width.saturating_sub(2));
        let (lines, (row, column)) = self.lines(width);
        let top = (row + 1).saturating_sub(MAX_LINES);
        let mut title = if to.is_empty() { " > ".to_owned() } else { format!(" > {to} ") };
        if let names @ [_, ..] = &self.staged.names()[..] {
            title = format!("{title}📎 {} ", names.join(", "));
        }
        if let Some((_, quoted)) = &self.quoting {
            let room = usize::from(area.width.saturating_sub(4)).saturating_sub(title.chars().count());
            let line: String = quoted.quote.line().chars().take(room.saturating_sub(1)).collect();
            title = format!("{title}{line} ");
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
            let pad = width.saturating_sub(HINT.chars().count());
            vec![Line::from(vec![Span::raw(" ".repeat(pad)), Span::styled(HINT, style::dim())])]
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
        assert_eq!(bar.key(Key::Ctrl('g')), Edited::Editor("say Xi\n!".into()));
        bar.set_text("");
        assert_eq!(bar.key(Key::Enter), Edited::Done, "nothing to send");
    }

    /// A bar with `text`, the cursor at `|` in it.
    fn at(text: &str) -> Bar {
        let mut bar = Bar::default();
        bar.set_text(&text.replace('|', ""));
        bar.cursor = text.chars().position(|c| c == '|').unwrap_or_else(|| unreachable!());
        bar
    }

    /// The bar's text with `|` at its cursor.
    fn shown(bar: &Bar) -> String {
        let mut chars = bar.chars.clone();
        chars.insert(bar.cursor, '|');
        chars.into_iter().collect()
    }

    fn keyed(text: &str, keys: &[Key]) -> String {
        let mut bar = at(text);
        for key in keys {
            assert_eq!(bar.key(*key), Edited::Done, "{key:?} on {text:?}");
        }
        shown(&bar)
    }

    #[test]
    fn ctrl_a_and_e_and_home_and_end_go_to_the_ends_of_the_line() {
        assert_eq!(keyed("one\ntw|o\nthree", &[Key::Ctrl('a')]), "one\n|two\nthree");
        assert_eq!(keyed("one\ntw|o\nthree", &[Key::Home]), "one\n|two\nthree");
        assert_eq!(keyed("one\nt|wo\nthree", &[Key::Ctrl('e')]), "one\ntwo|\nthree");
        assert_eq!(keyed("o|ne", &[Key::End]), "one|");
    }

    #[test]
    fn ctrl_k_and_u_delete_to_the_ends_of_the_line_and_ctrl_y_puts_it_back() {
        assert_eq!(keyed("one\nt|wo\nthree", &[Key::Ctrl('k')]), "one\nt|\nthree");
        assert_eq!(keyed("one|\ntwo", &[Key::Ctrl('k')]), "one|two", "the line break at the end of a line");
        assert_eq!(keyed("one\ntw|o", &[Key::Ctrl('u')]), "one\n|o");
        assert_eq!(keyed("one\n|two", &[Key::Ctrl('u')]), "one|two", "the line break at its start");
        assert_eq!(keyed("say |hi", &[Key::Ctrl('u'), Key::End, Key::Ctrl('y')]), "hisay |");
        assert_eq!(keyed("|", &[Key::Ctrl('k'), Key::Ctrl('u'), Key::Ctrl('y')]), "|", "nothing to delete");
    }

    #[test]
    fn ctrl_w_deletes_the_word_before_the_cursor() {
        assert_eq!(keyed("say hi.there  |x", &[Key::Ctrl('w')]), "say |x");
        assert_eq!(keyed("say hi |", &[Key::Ctrl('w'), Key::Ctrl('a'), Key::Ctrl('y')]), "hi |say ");
    }

    #[test]
    fn ctrl_y_puts_back_a_paste_as_its_placeholder() {
        let mut bar = Bar::default();
        let log = "line\n".repeat(120);
        bar.insert('a');
        bar.paste(&log);
        bar.key(Key::Ctrl('u'));
        assert!(bar.is_empty());
        bar.key(Key::Ctrl('y'));
        bar.key(Key::Ctrl('y'));
        assert_eq!(bar.text(), format!("a{log}a{log}"));
        assert_eq!(bar.chars.len(), 4, "a character each");
        bar.set_text("");
        bar.paste(&"x".repeat(900));
        bar.key(Key::Ctrl('y'));
        assert_eq!(bar.lines(80).0, vec!["[Pasted text #1 900 chars]a[Pasted text #2 +120 lines]".to_owned()]);
    }

    #[test]
    fn alt_b_and_f_and_ctrl_arrows_move_by_words_writing_nothing() {
        assert_eq!(keyed("one two|", &[Key::Alt('b')]), "one |two");
        assert_eq!(keyed("one two|", &[Key::Alt('b'), Key::Alt('b')]), "|one two");
        assert_eq!(keyed("one, two|", &[Key::WordLeft, Key::WordLeft]), "|one, two");
        assert_eq!(keyed("|one, two", &[Key::Alt('f')]), "one|, two");
        assert_eq!(keyed("one|, two", &[Key::WordRight]), "one, two|");
    }

    #[test]
    fn alt_d_and_alt_backspace_delete_a_word() {
        assert_eq!(keyed("one| two, three", &[Key::Alt('d')]), "one|, three");
        assert_eq!(keyed("one two-|three", &[Key::AltBackspace]), "one |three");
        assert_eq!(keyed("one two|", &[Key::AltBackspace, Key::Ctrl('a'), Key::Ctrl('y')]), "two|one ");
    }

    #[test]
    fn an_unknown_alt_key_does_nothing() {
        assert_eq!(keyed("one| two", &[Key::Alt('z'), Key::Alt('.')]), "one| two");
    }

    #[test]
    fn up_and_down_move_between_the_lines_of_the_text() {
        assert_eq!(keyed("one\ntwo|", &[Key::Up]), "one|\ntwo");
        assert_eq!(keyed("o|ne\nlonger\nx", &[Key::Down, Key::Down]), "one\nlonger\nx|", "as near its column");
        assert_eq!(keyed("o|ne\ntwo", &[Key::Up]), "o|ne\ntwo", "none above the first line");
        assert_eq!(keyed("one\nt|wo", &[Key::Down]), "one\nt|wo", "none below the last");
    }

    #[test]
    fn ctrl_g_or_ctrl_x_ctrl_e_edit_the_text_in_the_editor() {
        let mut bar = at("draft|");
        assert_eq!(bar.key(Key::Ctrl('g')), Edited::Editor("draft".into()));
        assert_eq!(bar.key(Key::Ctrl('x')), Edited::Done);
        assert_eq!(bar.key(Key::Ctrl('e')), Edited::Editor("draft".into()));
        bar.key(Key::Ctrl('a'));
        bar.key(Key::Ctrl('x'));
        bar.key(Key::Char('!'));
        assert_eq!(bar.key(Key::Ctrl('e')), Edited::Done, "Ctrl-X only right before");
        assert_eq!(shown(&bar), "!draft|");
    }

    #[test]
    fn a_backslash_before_enter_makes_it_a_new_line() {
        let mut bar = Bar::default();
        bar.set_text("one\\");
        assert_eq!(bar.key(Key::Enter), Edited::Done);
        bar.insert('2');
        assert_eq!(bar.text(), "one\n2");
        bar.set_text("a\\b");
        bar.key(Key::Left);
        bar.key(Key::Enter);
        bar.key(Key::End);
        assert_eq!(bar.key(Key::Enter), Edited::Submit("a\nb".into()), "the backslash before the cursor");
        bar.set_text("a\\ b");
        assert_eq!(bar.key(Key::Enter), Edited::Submit("a\\ b".into()), "sent when not right before it");
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
        assert_eq!(bar.key(Key::Ctrl('g')), Edited::Editor(bar.text()));
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

    #[test]
    fn lines_wrap_at_words() {
        let bar = at("one two three|");
        assert_eq!(bar.lines(9), (vec!["one two ".into(), "three".into()], (1, 5)));
        let bar = at("one two  |three");
        assert_eq!(bar.lines(8), (vec!["one two  ".into(), "three".into()], (1, 0)), "spaces hang at the edge");
        let bar = at("a verylongword|");
        assert_eq!(bar.lines(5), (vec!["a ".into(), "veryl".into(), "ongwo".into(), "rd".into()], (3, 2)));
    }

    #[test]
    fn wide_characters_take_two_columns_and_a_combining_mark_none() {
        let mut bar = Bar::default();
        bar.set_text("héllo 👍 wide 日本語 end");
        assert_eq!(bar.lines(110).1, (0, 24), "the cursor where the text ends");
        bar.set_text("a日本\u{301}b");
        assert_eq!(bar.lines(4), (vec!["a日".into(), "本\u{301}b".into()], (1, 3)), "wrapped at 4 columns");
        bar.key(Key::Left);
        assert_eq!(bar.lines(4).1, (1, 2), "the mark with the character before it");
        bar.set_text("e\u{301}");
        assert_eq!(bar.lines(1), (vec!["e\u{301}".into(), String::new()], (1, 0)));
    }
}
