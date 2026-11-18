//! The command bar's text, the cursor in it and its status line, as keys edit them, and how the bar draws.

use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use ants_proto::{ApprovalRequest, ThreadId};
use unicode_width::UnicodeWidthChar as _;

use crate::tui::exec::Quoted;
use crate::tui::input::{Input, PASTED};
use crate::tui::model::Key;
use crate::tui::{attach, complete, style};

/// The most lines of text the bar shows at once.
const MAX_LINES: usize = 6;
/// A paste of more characters than this, or of more lines than `PASTE_LINES`, shows as a placeholder.
const PASTE_CHARS: usize = 800;
const PASTE_LINES: usize = 3;

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
    input: Input,
    pub status: Option<Status>,
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
    /// Whether Ctrl-X came last, so that Ctrl-E after it opens the editor.
    ctrl_x: bool,
}

/// What the bar holds for a conversation not shown: its text, with the cursor, and what Enter does with it beyond
/// where the conversation sends.
#[derive(Debug, Default)]
pub struct Draft {
    input: Input,
    quoting: Option<(ThreadId, Quoted)>,
    noting: Option<Note>,
}

impl Draft {
    /// The draft of `text`, replying to the message `quoting` or the note `noting` if any.
    pub fn of(text: &str, quoting: Option<(ThreadId, Quoted)>, noting: Option<Note>) -> Self {
        Self { input: Input::from(text), quoting, noting }
    }

    pub fn is_empty(&self) -> bool {
        self.input.is_empty() && self.quoting.is_none() && self.noting.is_none()
    }

    /// The text, its pastes in full.
    pub fn text(&self) -> String {
        self.input.text()
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
        let mut input = draft.input;
        input.keep_killed(&mut self.input);
        let held = Draft {
            input: std::mem::replace(&mut self.input, input),
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
        self.input.text()
    }

    /// The text with its pastes as the characters standing for them, which `set_raw` takes back.
    pub fn raw(&self) -> String {
        self.input.raw()
    }

    /// Empty the bar, dropping the reply `>`, the note set up and the list picked from.
    pub fn clear(&mut self) {
        self.set_text("");
        self.pick = complete::Pick::default();
        self.quoting = None;
        self.noting = None;
    }

    pub fn is_empty(&self) -> bool {
        self.input.is_empty()
    }

    /// Replace the text with `text`, the cursor at its end.
    pub fn set_text(&mut self, text: &str) {
        self.input.set_text(text);
    }

    /// Replace the text with `raw`, as `raw` gave it, keeping the pastes.
    pub fn set_raw(&mut self, raw: &str) {
        self.input.set_raw(raw);
    }

    pub fn insert(&mut self, c: char) {
        self.input.insert(c);
    }

    /// Put `text` at the cursor, the cursor after it: as is when short, else as a placeholder that the cursor and
    /// deleting take as one character and that sending puts back.
    pub fn paste(&mut self, text: &str) {
        let long = text.chars().count() > PASTE_CHARS || text.lines().count() > PASTE_LINES;
        if long && let Some(c) = self.input.stand_in(text) {
            self.input.insert(c);
            return;
        }
        // Shown as spaces, a tab having no width of its own.
        self.input.insert_str(&text.replace('\t', "    "));
    }

    /// Move the cursor to the line above (`up`) or below, if any, as near its column as that line allows.
    fn line_up_down(&mut self, up: bool) {
        let (start, end) = (self.input.line_start(), self.input.line_end());
        let (chars, column) = (self.input.chars(), self.input.cursor() - start);
        let (from, to) = if up {
            let Some(before) = start.checked_sub(1) else { return };
            let from = chars[..before].iter().rposition(|c| *c == '\n').map_or(0, |at| at + 1);
            (from, before)
        } else {
            if end == chars.len() {
                return;
            }
            let to = chars[end + 1..].iter().position(|c| *c == '\n').map_or(chars.len(), |at| end + 1 + at);
            (end + 1, to)
        };
        self.input.go(from + column.min(to - from));
    }

    /// Where the word at the cursor starts, and its text up to the cursor.
    pub fn word(&self) -> (usize, String) {
        self.input.word()
    }

    /// Put `word` and a space in place of the word at the cursor, the cursor after them.
    pub fn complete(&mut self, word: &str) {
        // A directory completed goes on to its entries.
        self.input.complete(word, !word.ends_with('/'));
        self.pick = complete::Pick::default();
    }

    pub fn set_status(&mut self, text: impl Into<String>, tone: Tone) {
        self.status = Some(Status { text: text.into(), tone });
    }

    /// Take `key`, the bar being focused: it edits as a shell's line does ([`Input::key`]), with lines.
    pub fn key(&mut self, key: Key) -> Edited {
        let after_ctrl_x = std::mem::take(&mut self.ctrl_x);
        let cursor = self.input.cursor();
        match key {
            Key::Ctrl('e') if after_ctrl_x => return Edited::Editor(self.text()),
            Key::Newline => self.input.insert('\n'),
            Key::Up => self.line_up_down(true),
            Key::Down => self.line_up_down(false),
            Key::Ctrl('x') => self.ctrl_x = true,
            Key::Ctrl('g') => return Edited::Editor(self.text()),
            // A backslash before Enter makes it a new line, as in a shell.
            Key::Enter if cursor > 0 && self.input.chars()[cursor - 1] == '\\' => self.input.replace_before('\n'),
            Key::Enter if !self.text().trim().is_empty() => return Edited::Submit(self.text()),
            Key::Esc => self.focused = false,
            _ => _ = self.input.key(key),
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
        for (at, c) in self.input.chars().iter().enumerate() {
            if *c == '\n' {
                row.push(Cell { at, c: '\n', columns: 0 });
                rows.push(std::mem::take(&mut row));
                (column, after_space) = (0, None);
                continue;
            }
            let mut one = [0; 4];
            let label = self.input.pasted(*c).map(|paste| placeholder(u32::from(*c) - PASTED + 1, paste));
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
            let at = row.iter().position(|cell| cell.at >= self.input.cursor())?;
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

    /// Draw the bar in `area`, its title naming where Enter sends (`to`) and `hint` on it while it is empty, and above
    /// it the `completions` of the word at the cursor.
    pub fn draw(&self, frame: &mut Frame, area: Rect, completions: &[complete::Candidate], to: &str, hint: &str) {
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
            let mark = if status.tone == Tone::Error { style::ERROR_MARK } else { "" };
            let text = style::fit(&format!("{mark}{}", status.text.replace('\n', " ")), room);
            block = block.title_bottom(Line::styled(format!(" {text} "), style));
        }
        let shown: Vec<Line> = if self.input.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_cut_at_the_width_ends_in_an_ellipsis_and_an_error_begins_with_its_mark() {
        let mut bar = Bar::default();
        bar.set_status("refused: ".to_owned() + &"reasons far too long ".repeat(4), Tone::Error);
        let shown = crate::test_support::render::rendered(40, 4, |f| bar.draw(f, f.area(), &[], "", ""));
        let bottom = shown.lines().nth(3).unwrap();
        assert!(bottom.starts_with("└ ✗ refused: reasons") && bottom.ends_with("… ┘"), "{shown}");
    }

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
        bar.input.set_cursor(text.chars().position(|c| c == '|').unwrap_or_else(|| unreachable!()));
        bar
    }

    /// The bar's text with `|` at its cursor.
    fn shown(bar: &Bar) -> String {
        let mut chars = bar.input.chars().to_vec();
        chars.insert(bar.input.cursor(), '|');
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
        assert_eq!(bar.input.chars().len(), 4, "a character each");
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
