//! A text being edited and the cursor in it, taking keys as a shell's line does: the command bar's text
//! ([`super::command::Bar`], which adds lines between, moving up and down them, and long pastes shown as placeholders)
//! and every other text input's, which takes pastes on one line.

use unicode_width::UnicodeWidthChar as _;

use super::model::Key;

/// The first of the private-use characters standing in the text for the pastes shown as placeholders.
pub const PASTED: u32 = 0xF_0000;
/// How many pastes there may be placeholders for: the characters of the Supplementary Private Use Area-A.
const PASTES: usize = 0xFFFE;
/// What shows where the cursor is, in an input drawn as text.
const CARET: char = '▏';

/// A character deleted, or a paste whose placeholder was.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Killed {
    Char(char),
    Paste(String),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Input {
    chars: Vec<char>,
    /// The pastes shown as placeholders, each standing in `chars` as the character `PASTED` plus its index.
    pastes: Vec<String>,
    /// Where the cursor is, in characters.
    cursor: usize,
    /// What a key deleted last, for Ctrl-Y to put back.
    killed: Vec<Killed>,
}

impl From<&str> for Input {
    /// `text`, the cursor at its end.
    fn from(text: &str) -> Self {
        let mut input = Self::default();
        input.set_text(text);
        input
    }
}

impl Input {
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

    pub fn chars(&self) -> &[char] {
        &self.chars
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    #[cfg(test)]
    pub fn set_cursor(&mut self, at: usize) {
        self.cursor = at.min(self.chars.len());
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

    /// Take over what `from` deleted last, for Ctrl-Y here.
    pub fn keep_killed(&mut self, from: &mut Self) {
        self.killed = std::mem::take(&mut from.killed);
    }

    pub fn insert(&mut self, c: char) {
        self.chars.insert(self.cursor, c);
        self.cursor += 1;
    }

    /// Put `text` at the cursor as is, the cursor after it.
    pub fn insert_str(&mut self, text: &str) {
        let at = self.cursor;
        self.chars.splice(at..at, text.chars());
        self.cursor = at + text.chars().count();
    }

    /// Put `text` pasted at the cursor on one line, its line breaks and tabs as spaces.
    pub fn paste(&mut self, text: &str) {
        self.insert_str(&text.replace("\r\n", " ").replace(['\r', '\n', '\t'], " "));
    }

    /// The paste `c` stands for, if any.
    pub fn pasted(&self, c: char) -> Option<&str> {
        let at = u32::from(c).checked_sub(PASTED)?;
        self.pastes.get(usize::try_from(at).ok()?).map(String::as_str)
    }

    /// The placeholder character for the paste `text`, one already standing for it or a new one; None once there are
    /// no more.
    pub fn stand_in(&mut self, text: &str) -> Option<char> {
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
    pub fn line_start(&self) -> usize {
        self.chars[..self.cursor].iter().rposition(|c| *c == '\n').map_or(0, |at| at + 1)
    }

    /// Where the line of the cursor ends, before its line break.
    pub fn line_end(&self) -> usize {
        self.chars[self.cursor..].iter().position(|c| *c == '\n').map_or(self.chars.len(), |at| self.cursor + at)
    }

    /// Move the cursor to `at`, in characters.
    pub fn go(&mut self, at: usize) {
        self.cursor = at.min(self.chars.len());
    }

    /// Put `c` in place of the character before the cursor, if any.
    pub fn replace_before(&mut self, c: char) {
        if let Some(before) = self.cursor.checked_sub(1) {
            self.chars[before] = c;
        }
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

    /// Whether `c` ends a word: whitespace, or a paste's placeholder.
    fn breaks(&self, c: char) -> bool {
        c.is_whitespace() || self.pasted(c).is_some()
    }

    /// Where the word at the cursor starts, and its text up to the cursor.
    pub fn word(&self) -> (usize, String) {
        let start = self.chars[..self.cursor].iter().rposition(|c| self.breaks(*c)).map_or(0, |at| at + 1);
        (start, self.chars[start..self.cursor].iter().collect())
    }

    /// Put `word` in place of the word at the cursor, then a space when `space` and none follows, the cursor after
    /// them.
    pub fn complete(&mut self, word: &str, space: bool) {
        let (start, _) = self.word();
        let end = self.chars[self.cursor..]
            .iter()
            .position(|c| self.breaks(*c))
            .map_or(self.chars.len(), |at| self.cursor + at);
        let spaced = self.chars.get(end).is_some_and(|c| *c == ' ');
        let mut put: Vec<char> = word.chars().collect();
        if space && !spaced {
            put.push(' ');
        }
        self.cursor = start + put.len() + usize::from(spaced);
        self.chars.splice(start..end, put);
    }

    /// Take `key` if it edits a line: a character, Backspace and Delete, the cursor's moves (←→, Home End or Ctrl-A
    /// Ctrl-E, by words Alt-B Alt-F or Ctrl-← Ctrl-→) and the kills (Ctrl-K Ctrl-U to the line's ends, Ctrl-W
    /// Alt-Backspace and Alt-D a word) Ctrl-Y puts back. Whether it did.
    pub fn key(&mut self, key: Key) -> bool {
        match key {
            Key::Char(c) => self.insert(c),
            Key::Backspace => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.chars.remove(self.cursor);
                }
            }
            Key::Delete => {
                if self.cursor < self.chars.len() {
                    self.chars.remove(self.cursor);
                }
            }
            Key::Left => self.cursor = self.cursor.saturating_sub(1),
            Key::Right => self.cursor = (self.cursor + 1).min(self.chars.len()),
            Key::Home | Key::Ctrl('a') => self.cursor = self.line_start(),
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
            _ => return false,
        }
        true
    }

    /// The text on one line with [`CARET`] at the cursor, at most `room` columns of it: as much of the text before the
    /// cursor as fits, then of the text after it.
    pub fn caret(&self, room: usize) -> String {
        let room = room.max(1) - 1;
        let width = |c: &char| c.width().unwrap_or(0);
        let shown = |c: &char| if *c == '\n' { ' ' } else { *c };
        let (before, after) = self.chars.split_at(self.cursor);
        let mut used = 0;
        let start = before
            .iter()
            .rposition(|c| {
                used += width(c);
                used > room
            })
            .map_or(0, |at| at + 1);
        let mut line: String = before[start..].iter().map(shown).collect();
        line.push(CARET);
        for c in after {
            used += width(c);
            if used > room {
                break;
            }
            line.push(shown(c));
        }
        line
    }
}

/// A key or a paste, as [`exercise`] gives an input.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub enum Typed<'a> {
    Key(Key),
    Paste(&'a str),
}

/// Run on an input the keys every input takes alike: "say hi" typed, Left twice, Ctrl-W, "a b " pasted, Right, Ctrl-A
/// and "X" typed, which leave "Xa b hi".
#[cfg(test)]
pub fn exercise(mut input: impl FnMut(Typed)) -> &'static str {
    "say hi".chars().for_each(|c| input(Typed::Key(Key::Char(c))));
    input(Typed::Key(Key::Left));
    input(Typed::Key(Key::Left));
    input(Typed::Key(Key::Ctrl('w')));
    input(Typed::Paste("a b "));
    input(Typed::Key(Key::Right));
    input(Typed::Key(Key::Ctrl('a')));
    input(Typed::Key(Key::Char('X')));
    "Xa b hi"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keys_every_input_takes_edit_one_line() {
        let mut input = Input::default();
        let want = exercise(|typed| match typed {
            Typed::Key(key) => _ = input.key(key),
            Typed::Paste(text) => input.paste(text),
        });
        assert_eq!(input.text(), want);
        assert_eq!(input.caret(80), "X▏a b hi");
        input.paste("1\r\n2\t3");
        assert_eq!(input.text(), "X1 2 3a b hi", "a paste on one line");
        assert!(!input.key(Key::Enter) && !input.key(Key::Esc), "left to the input's owner");
    }

    #[test]
    fn the_caret_shows_where_the_cursor_is_within_the_room() {
        let mut input = Input::from("abcdef");
        assert_eq!(input.caret(4), "def▏");
        input.key(Key::Ctrl('a'));
        assert_eq!(input.caret(4), "▏abc");
        input.key(Key::Right);
        input.key(Key::Right);
        input.key(Key::Right);
        input.key(Key::Right);
        assert_eq!(input.caret(4), "bcd▏");
        let mut wide = Input::from("日本語");
        wide.key(Key::Left);
        assert_eq!(wide.caret(7), "日本▏語", "wide characters take two columns");
        assert_eq!(wide.caret(4), "本▏");
    }
}
