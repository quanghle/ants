//! Questions from ants as cards, on home and at the bottom of their thread: the question, its numbered options, and
//! once an answer is stored, who answered and with what. `1`-`9` answers the card the keys answer with that option at
//! once. Enter focuses that card ([`Cursor`]), listing its options one per line under a cursor: ↑↓ move it, Enter picks
//! the option highlighted, and where the question allows a typed answer, a last row takes the text in place, Enter
//! sending it as `ants answer` does (or, empty, opening the command bar on `/answer`); Esc leaves the card.

use ants_proto::{Choice, Question, ThreadId, Uuid};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;

use super::exec::Job;
use super::model::{Action, Data, Key, To};
use super::style;
use crate::text::excerpt;
use crate::threads::thread::{Entry, answer_line, answers_to};

/// How long a card stays at the bottom of its thread once answered.
const ANSWERED_FOR: TimeDelta = TimeDelta::minutes(2);

/// The row of a focused card that takes a typed answer, before the text typed.
const TYPE: &str = "type an answer";

/// The options in a line, numbered: `1 exponential  2 fixed 5s`.
pub fn numbered(question: &Question) -> String {
    let options: Vec<String> = question.options.iter().enumerate().map(|(n, o)| format!("{} {o}", n + 1)).collect();
    options.join("  ")
}

/// Where `key` leads on `question`, asked on `thread` in the message `id`: an option's number answers that question
/// with it at once, whatever else was asked since.
pub fn key(key: Key, thread: &ThreadId, id: Uuid, question: &Question) -> Option<To> {
    let Key::Char(c @ '1'..='9') = key else { return None };
    let n = c.to_digit(10)?;
    if usize::try_from(n).ok()? > question.options.len() {
        return None;
    }
    Some(answer(thread, id, Choice::Option(n)))
}

fn answer(thread: &ThreadId, question: Uuid, choice: Choice) -> To {
    To::Do(Action::Run(Job::Answer { thread: thread.clone(), question, choice }, String::new()))
}

/// The cursor on a focused question card: the row highlighted, an option or the one typing an answer, and the text
/// typed there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    /// The message asking the question.
    pub question: Uuid,
    options: usize,
    free_text: bool,
    at: usize,
    typed: String,
}

impl Cursor {
    /// The cursor on the first option of `question`, asked in the message `id`.
    pub fn new(id: Uuid, question: &Question) -> Self {
        Self {
            question: id,
            options: question.options.len(),
            free_text: question.free_text,
            at: 0,
            typed: String::new(),
        }
    }

    /// Whether the cursor is on the row typing an answer, where keys type.
    pub fn typing(&self) -> bool {
        self.free_text && self.at == self.options
    }

    fn rows(&self) -> usize {
        self.options + usize::from(self.free_text)
    }

    /// Take `key` on the card of the question asked on `thread`: none while the card keeps the focus, else where the
    /// key leads as the card leaves it, if anywhere.
    pub fn key(&mut self, key: Key, thread: &ThreadId) -> Option<Option<To>> {
        let typing = self.typing();
        match key {
            Key::Esc => return Some(None),
            Key::Up => self.at = self.at.saturating_sub(1),
            Key::Char('k') if !typing => self.at = self.at.saturating_sub(1),
            Key::Down => self.at = (self.at + 1).min(self.rows().saturating_sub(1)),
            Key::Char('j') if !typing => self.at = (self.at + 1).min(self.rows().saturating_sub(1)),
            Key::Enter if typing && self.typed.trim().is_empty() => {
                return Some(Some(To::Bar("/answer ".to_owned())));
            }
            Key::Enter if typing => {
                let typed = std::mem::take(&mut self.typed).trim().to_owned();
                return Some(Some(answer(thread, self.question, Choice::Typed(typed))));
            }
            Key::Enter => {
                let n = u32::try_from(self.at + 1).unwrap_or(u32::MAX);
                return Some(Some(answer(thread, self.question, Choice::Option(n))));
            }
            Key::Backspace if typing => _ = self.typed.pop(),
            Key::Char(c) if typing => self.typed.push(c),
            Key::Char(c @ '1'..='9') => {
                let n = c.to_digit(10).unwrap_or(0);
                if usize::try_from(n).is_ok_and(|n| n <= self.options) {
                    return Some(Some(answer(thread, self.question, Choice::Option(n))));
                }
            }
            _ => {}
        }
        None
    }

    /// Type `text` pasted on the row typing an answer, the cursor moving there: whether the question takes one.
    pub fn paste(&mut self, text: &str) -> bool {
        if !self.free_text {
            return false;
        }
        self.at = self.options;
        self.typed.push_str(text);
        true
    }

    /// The keys the card takes, as the footer shows them.
    pub fn keys(&self) -> &'static str {
        if self.typing() {
            "type an answer · Enter send (empty: the bar on /answer) · ↑↓ move · Esc leave"
        } else {
            "↑↓ move · Enter pick · 1-9 answer · Esc leave"
        }
    }
}

/// Take `key` on the card `focus` holds the cursor of, of a question asked on `thread`: where it leads, if the card
/// takes it, the focus ending once it leaves.
pub fn focused(focus: &mut Option<Cursor>, key: Key, thread: &ThreadId) -> Option<Option<To>> {
    let Some(to) = focus.as_mut()?.key(key, thread) else { return Some(None) };
    *focus = None;
    Some(to)
}

/// The options of `question` one per line, numbered and wrapped to `width`, then where allowed the row typing an
/// answer; with `cursor`, its row marked and highlighted.
pub fn rows(question: &Question, cursor: Option<&Cursor>, width: usize) -> Vec<Line<'static>> {
    let at = cursor.map(|c| c.at);
    let mut lines = Vec::new();
    let mut row = |n: usize, head: String, text: &str, style: Style| {
        let here = at == Some(n);
        let head = format!("{}{head}", if here { "› " } else { "  " });
        let indent = head.chars().count();
        let style = if here { style.fg(Color::Cyan).add_modifier(Modifier::BOLD) } else { style };
        for (i, part) in wrap(text, width.saturating_sub(indent)).into_iter().enumerate() {
            let start = if i == 0 { head.clone() } else { " ".repeat(indent) };
            lines.push(Line::styled(format!("{start}{part}"), style));
        }
    };
    for (n, option) in question.options.iter().enumerate() {
        row(n, format!("{} ", n + 1), option, Style::new());
    }
    if question.free_text {
        let typed = cursor.map_or("", |c| c.typed.as_str());
        let caret = if at == Some(question.options.len()) { "▏" } else { "" };
        let text =
            if typed.is_empty() && caret.is_empty() { TYPE.to_owned() } else { format!("{TYPE}: {typed}{caret}") };
        row(question.options.len(), String::new(), &text, style::dim());
    }
    lines
}

/// `text` in lines at most `width` wide, broken between words where it can be.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        let (have, more) = (line.chars().count(), word.chars().count());
        if have > 0 && have + 1 + more > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        let mut chars = word.chars().peekable();
        while chars.peek().is_some() {
            let room = width - line.chars().count();
            if room == 0 {
                lines.push(std::mem::take(&mut line));
                continue;
            }
            line.extend(chars.by_ref().take(room));
        }
    }
    if !line.is_empty() || lines.is_empty() {
        lines.push(line);
    }
    lines
}

/// A question card at the bottom of a thread.
pub struct Card<'a> {
    pub asked: &'a Entry,
    pub question: &'a Question,
    /// The answers stored to it, the first one first.
    pub answers: Vec<String>,
    /// Whether the keys answer it: the latest card with no answer stored.
    pub keys: bool,
}

impl Card<'_> {
    pub fn title(&self) -> String {
        let from = self.asked.from.as_ref().map_or("an ant", |f| f.as_str());
        format!(" question from {from} ")
    }

    pub fn style(&self) -> Style {
        if self.answers.is_empty() { Style::new().fg(Color::Cyan) } else { style::dim() }
    }

    /// The question, then its options in a line and its answers, at most `width` wide; on the card the keys answer, its
    /// options one per line, wrapped, under `cursor` while focused, then the keys it takes: Enter focusing it with
    /// `enter`.
    pub fn lines(&self, width: usize, cursor: Option<&Cursor>, enter: bool) -> Vec<Line<'static>> {
        let mut lines = vec![Line::raw(excerpt(&self.question.text, width))];
        if self.keys {
            lines.extend(rows(self.question, cursor, width));
            let keys = match cursor {
                Some(cursor) => cursor.keys(),
                None if enter => "1-9 answer · Enter choose",
                None => "1-9 answer",
            };
            lines.push(Line::styled(keys, style::dim()));
        } else {
            lines.push(Line::raw(excerpt(&numbered(self.question), width)));
        }
        lines.extend(self.answers.iter().map(|a| Line::styled(excerpt(a, width), Style::new().fg(Color::Green))));
        lines
    }
}

/// The question cards of a thread at `now`, oldest first: the questions to the human with no answer stored, and those
/// answered lately.
pub fn cards<'a>(data: &Data, entries: &'a [Entry], now: DateTime<Utc>) -> Vec<Card<'a>> {
    let mut cards: Vec<Card> = entries
        .iter()
        .filter(|e| e.to.as_ref() == Some(data.me()) && !e.forged)
        .filter_map(|asked| {
            let question = asked.question.as_ref()?;
            let answers: Vec<(&Entry, _)> = answers_to(asked, entries).collect();
            if answers.first().is_some_and(|(by, _)| now - by.stored_at >= ANSWERED_FOR) {
                return None;
            }
            let answers = answers.into_iter().map(|(by, answered)| answer_line(by, answered)).collect();
            Some(Card { asked, question, answers, keys: false })
        })
        .collect();
    if let Some(card) = cards.iter_mut().rev().find(|c| c.answers.is_empty() && c.asked.id.is_some()) {
        card.keys = true;
    }
    cards
}

/// The question of the card the keys answer in a thread showing `entries`, and its envelope's id.
pub fn keyed<'a>(data: &Data, entries: &'a [Entry], now: DateTime<Utc>) -> Option<(Uuid, &'a Question)> {
    let card = cards(data, entries, now).into_iter().find(|c| c.keys)?;
    Some((card.asked.id?, card.question))
}

/// Where `key` leads in a thread showing `entries`: on the card it answers.
pub fn thread_key(key: Key, data: &Data, thread: &ThreadId, entries: &[Entry], now: DateTime<Utc>) -> Option<To> {
    let (id, question) = keyed(data, entries, now)?;
    self::key(key, thread, id, question)
}

#[cfg(test)]
mod tests {
    use ants_proto::Answer;

    use super::*;
    use crate::test_support::live::{at, message};
    use crate::tui::test_support::model;

    #[test]
    fn a_card_lists_the_options_one_per_line_answers_by_number_and_shows_who_answered() {
        let thread = ThreadId::generate();
        let m = model();
        let mut asked = message(1, &thread, "dev-b", "alice", "Which retry policy?");
        let options = ["exponential".to_owned(), "fixed 5s".to_owned()];
        asked.question = Some(Question::new("Which retry policy?", &options, true).unwrap());
        let mut entries = vec![asked.clone()];
        let card = &cards(&m.data, &entries, at(2))[0];
        assert_eq!(card.title(), " question from dev-b ");
        let lines: Vec<String> = card.lines(80, None, true).iter().map(ToString::to_string).collect();
        assert_eq!(
            lines,
            ["Which retry policy?", "  1 exponential", "  2 fixed 5s", "  type an answer", "1-9 answer · Enter choose"]
        );

        let answer = |n| {
            let job = Job::Answer { thread: thread.clone(), question: asked.id.unwrap(), choice: Choice::Option(n) };
            Some(To::Do(Action::Run(job, String::new())))
        };
        assert_eq!(thread_key(Key::Char('2'), &m.data, &thread, &entries, at(2)), answer(2));
        assert_eq!(thread_key(Key::Char('3'), &m.data, &thread, &entries, at(2)), None, "no third option");

        let mut reply = message(2, &thread, "alice", "dev-b", "fixed 5s");
        reply.answer = Some(Answer { question: asked.id.unwrap(), option: Some(2) });
        entries.push(reply);
        let card = &cards(&m.data, &entries, at(3))[0];
        assert_eq!(card.answers, ["answered by alice: option 2, fixed 5s"]);
        let lines: Vec<String> = card.lines(80, None, true).iter().map(ToString::to_string).collect();
        assert_eq!(
            lines,
            ["Which retry policy?", "1 exponential  2 fixed 5s", "answered by alice: option 2, fixed 5s"]
        );
        assert_eq!(thread_key(Key::Char('1'), &m.data, &thread, &entries, at(3)), None, "answered");
        assert!(cards(&m.data, &entries, at(200)).is_empty(), "the answered card goes after a while");
    }

    #[test]
    fn a_focused_card_wraps_its_options_under_a_cursor_that_enter_picks() {
        let (thread, id) = (ThreadId::generate(), Uuid::now_v7());
        let options = ["exponential backoff from one second up to a minute".to_owned(), "none".to_owned()];
        let question = Question::new("Which retry policy?", &options, true).unwrap();
        let mut cursor = Cursor::new(id, &question);
        let shown = |cursor: &Cursor| -> Vec<String> {
            rows(&question, Some(cursor), 24).iter().map(ToString::to_string).collect()
        };
        assert_eq!(
            shown(&cursor),
            ["› 1 exponential backoff", "    from one second up", "    to a minute", "  2 none", "  type an answer"]
        );
        let answer = |choice| Some(Some(super::answer(&thread, id, choice)));
        assert_eq!(cursor.key(Key::Up, &thread), None, "at the top already");
        assert_eq!(cursor.key(Key::Down, &thread), None);
        assert_eq!(shown(&cursor)[3], "› 2 none");
        assert_eq!(cursor.clone().key(Key::Enter, &thread), answer(Choice::Option(2)));
        assert_eq!(cursor.clone().key(Key::Char('1'), &thread), answer(Choice::Option(1)), "digits answer at once");
        assert_eq!(cursor.key(Key::Char('3'), &thread), None, "no third option");
        assert_eq!(cursor.key(Key::Esc, &thread), Some(None));
    }

    #[test]
    fn the_last_row_of_a_focused_card_takes_a_typed_answer_in_place() {
        let (thread, id) = (ThreadId::generate(), Uuid::now_v7());
        let question = Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
        let mut cursor = Cursor::new(id, &question);
        for _ in 0..3 {
            cursor.key(Key::Down, &thread);
        }
        assert!(cursor.typing(), "the cursor stops on the last row");
        assert_eq!(cursor.clone().key(Key::Enter, &thread), Some(Some(To::Bar("/answer ".into()))), "empty");
        for c in "after 2pm".chars() {
            assert_eq!(cursor.key(Key::Char(c), &thread), None, "digits and j k type here");
        }
        cursor.key(Key::Backspace, &thread);
        let lines: Vec<String> = rows(&question, Some(&cursor), 80).iter().map(ToString::to_string).collect();
        assert_eq!(lines, ["  1 yes", "  2 no", "› type an answer: after 2p▏"]);
        let typed = Some(Some(answer(&thread, id, Choice::Typed("after 2p".into()))));
        assert_eq!(cursor.key(Key::Enter, &thread), typed);

        let closed = Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], false).unwrap();
        let mut cursor = Cursor::new(id, &closed);
        assert!(!cursor.paste("maybe"), "no typed answer to this one");
        cursor.key(Key::Down, &thread);
        cursor.key(Key::Down, &thread);
        assert!(!cursor.typing());
        assert_eq!(rows(&closed, Some(&cursor), 80).len(), 2, "no row to type in");
    }

    #[test]
    fn of_several_questions_waiting_only_the_latest_card_shows_the_keys_it_takes() {
        let thread = ThreadId::generate();
        let m = model();
        let options = ["yes".to_owned(), "no".to_owned()];
        let mut entries = Vec::new();
        for (seq, text) in [(1, "Ship it?"), (2, "Tag it?")] {
            let mut asked = message(seq, &thread, "dev-b", "alice", text);
            asked.question = Some(Question::new(text, &options, false).unwrap());
            entries.push(asked);
        }
        let shown: Vec<Vec<String>> = cards(&m.data, &entries, at(3))
            .iter()
            .map(|c| c.lines(80, None, false).iter().map(ToString::to_string).collect())
            .collect();
        assert_eq!(shown, [vec!["Ship it?", "1 yes  2 no"], vec!["Tag it?", "  1 yes", "  2 no", "1-9 answer"]]);
        let job = Job::Answer { thread: thread.clone(), question: entries[1].id.unwrap(), choice: Choice::Option(1) };
        let key = thread_key(Key::Char('1'), &m.data, &thread, &entries, at(3));
        assert_eq!(key, Some(To::Do(Action::Run(job, String::new()))), "the key answers the card showing it");
    }
}
