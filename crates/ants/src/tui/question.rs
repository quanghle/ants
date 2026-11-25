//! Questions from ants as cards, on home and at the bottom of their thread: the questions of a message, their numbered
//! options, and once an answer is stored, who answered and with what. `1`-`9` answers the card the keys answer with
//! that option at once when it asks one question; on several it picks for the first and focuses the card. Enter
//! focuses that card ([`Cursor`]): with several questions, a tab for each (its header, else `Q1`…) and a last one to
//! submit, ←→ and Tab moving between them; it lists the current question's options one per line under a cursor: ↑↓ move
//! it, Enter picks the option highlighted, digits pick at once, or where several may be picked, Space and digits
//! toggle options and Enter confirms them; where the question allows a typed answer, a last row takes the text in
//! place, edited as in the command bar (←→ moving in it, Tab alone then moving between questions), Enter picking it (or, empty on a card of one question, opening the command bar on `/answer`). A pick sends
//! the answer of a card of one question, as `ants answer` does, and moves on to the next tab of several, the last
//! sending every pick at once with Enter; Esc leaves the card.

use std::collections::BTreeSet;

use ants_client::entry::Entry;
use ants_proto::{Answered, Pick, Question, Questions, ThreadId, Uuid};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::UnicodeWidthStr as _;

use super::exec::Job;
use super::input::Input;
use super::model::{Action, Data, Key, To};
use super::style;
use crate::text::excerpt;
use crate::threads::thread::{answer_line, answers_to};

/// How long a card stays at the bottom of its thread once answered.
const ANSWERED_FOR: TimeDelta = TimeDelta::minutes(2);

/// What a card answered here shows until the answer is read back.
const ANSWERING: &str = "answer sent";

/// The row of a focused card that takes a typed answer, before the text typed.
const TYPE: &str = "type an answer";

/// The last tab of a card asking several questions, sending the answer.
const SUBMIT: &str = "Submit";

/// The options in a line, numbered: `1 exponential  2 fixed 5s`.
pub fn numbered(question: &Question) -> String {
    let options: Vec<String> =
        question.options.iter().enumerate().map(|(n, o)| format!("{} {}", n + 1, o.label)).collect();
    options.join("  ")
}

/// The options of `questions` in a line: those of a single question numbered, else those of each after its title:
/// `Regions: 1 eu  2 us · Q2: 1 yes  2 no`.
pub fn summary(questions: &Questions) -> String {
    match questions.questions.as_slice() {
        [one] => numbered(one),
        several => {
            let each: Vec<String> =
                (1..).zip(several).map(|(n, q)| format!("{}: {}", q.title(n), numbered(q))).collect();
            each.join(" · ")
        }
    }
}

/// What `questions` ask in a line: the first question's text, and how many more follow.
pub fn asking(questions: &Questions) -> String {
    let first = questions.questions.first().map_or("", |q| q.text.as_str());
    match questions.questions.len() {
        0 | 1 => first.to_owned(),
        n => format!("{first} (+{} more)", n - 1),
    }
}

/// Where a key on a card that is not focused leads: somewhere, or to the card focused with a pick made.
#[derive(Debug, PartialEq, Eq)]
pub enum Keyed {
    To(Box<To>),
    Focus(Cursor),
}

/// Where `key` leads on the card of `questions`, asked on `thread` in the message `id`, not focused: an option's
/// number answers a single question with it at once, whatever else was asked since; on several, it picks that option
/// for the first and focuses the card.
pub fn key(key: Key, thread: &ThreadId, id: Uuid, questions: &Questions) -> Option<Keyed> {
    let Key::Char('1'..='9') = key else { return None };
    let mut cursor = Cursor::new(id, questions);
    let fresh = cursor.clone();
    match cursor.key(key, thread) {
        Some(Some(to)) => Some(Keyed::To(Box::new(to))),
        Some(None) => None,
        None => (cursor != fresh).then_some(Keyed::Focus(cursor)),
    }
}

fn answer(thread: &ThreadId, question: Uuid, picks: Vec<Pick>) -> To {
    To::Do(Action::Run(Job::Answer { thread: thread.clone(), question, picks }, String::new()))
}

/// The cursor on a focused question card: the tab shown, a question or, after several, the one submitting them, and
/// on each question the row highlighted, an option or the one typing an answer, the options toggled, the text typed
/// and the pick made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    /// The message asking the questions.
    pub question: Uuid,
    tabs: Vec<Tab>,
    tab: usize,
}

/// Where a focused card is on one of its questions.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Tab {
    options: usize,
    free_text: bool,
    multi: bool,
    at: usize,
    toggled: BTreeSet<u32>,
    typed: Input,
    pick: Option<Pick>,
}

impl Tab {
    fn rows(&self) -> usize {
        self.options + usize::from(self.free_text)
    }

    fn typing(&self) -> bool {
        self.free_text && self.at == self.options
    }

    /// The option the digit `c` numbers, if any.
    fn number(&self, c: char) -> Option<u32> {
        let n = c.to_digit(10)?;
        (n >= 1 && usize::try_from(n).ok()? <= self.options).then_some(n)
    }

    fn toggle(&mut self, n: u32) {
        if !self.toggled.remove(&n) {
            self.toggled.insert(n);
        }
    }
}

impl Cursor {
    pub fn mouse_select(&mut self, at: usize) {
        if let Some(tab) = self.tabs.get_mut(self.tab) {
            tab.at = at.min(tab.rows().saturating_sub(1));
        }
    }

    /// The cursor on the first option of the first of `questions`, asked in the message `id`.
    pub fn new(id: Uuid, questions: &Questions) -> Self {
        let tabs = questions
            .questions
            .iter()
            .map(|q| Tab {
                options: q.options.len(),
                free_text: q.free_text,
                multi: q.multi_select,
                at: 0,
                toggled: BTreeSet::new(),
                typed: Input::default(),
                pick: None,
            })
            .collect();
        Self { question: id, tabs, tab: 0 }
    }

    fn several(&self) -> bool {
        self.tabs.len() > 1
    }

    #[cfg(test)]
    fn typing(&self) -> bool {
        self.tabs.get(self.tab).is_some_and(Tab::typing)
    }

    fn picks(&self) -> Vec<Pick> {
        self.tabs.iter().filter_map(|t| t.pick.clone()).collect()
    }

    /// Take `key` on the card of the questions asked on `thread`: none while the card keeps the focus, else where the
    /// key leads as the card leaves it, if anywhere.
    pub fn key(&mut self, key: Key, thread: &ThreadId) -> Option<Option<To>> {
        let (several, last) = (self.several(), if self.several() { self.tabs.len() } else { 0 });
        let typing = self.tabs.get(self.tab).is_some_and(Tab::typing);
        match key {
            Key::Esc => return Some(None),
            Key::Left if !typing => self.tab = self.tab.saturating_sub(1),
            Key::Right if !typing => self.tab = (self.tab + 1).min(last),
            Key::Tab => self.tab = if self.tab >= last { 0 } else { self.tab + 1 },
            _ => {}
        }
        let Some(tab) = self.tabs.get_mut(self.tab) else { return self.submit(key, thread) };
        let typing = tab.typing();
        let pick = match key {
            Key::Up => {
                tab.at = tab.at.saturating_sub(1);
                None
            }
            Key::Char('k') if !typing => {
                tab.at = tab.at.saturating_sub(1);
                None
            }
            Key::Down => {
                tab.at = (tab.at + 1).min(tab.rows().saturating_sub(1));
                None
            }
            Key::Char('j') if !typing => {
                tab.at = (tab.at + 1).min(tab.rows().saturating_sub(1));
                None
            }
            Key::Enter if typing && tab.typed.text().trim().is_empty() => {
                return (!several).then(|| Some(To::Bar("/answer ".to_owned())));
            }
            Key::Enter if typing => Some(Pick::Typed(tab.typed.text().trim().to_owned())),
            Key::Enter if tab.multi && !tab.toggled.is_empty() => {
                Some(Pick::Options(tab.toggled.iter().copied().collect()))
            }
            Key::Enter => Some(Pick::option(u32::try_from(tab.at + 1).unwrap_or(u32::MAX))),
            key if typing => {
                tab.typed.key(key);
                None
            }
            Key::Char(' ') if tab.multi => {
                tab.toggle(u32::try_from(tab.at + 1).unwrap_or(u32::MAX));
                None
            }
            Key::Char(c @ '1'..='9') => match tab.number(c) {
                Some(n) if tab.multi => {
                    tab.toggle(n);
                    tab.at = usize::try_from(n - 1).unwrap_or_default();
                    None
                }
                n => n.map(Pick::option),
            },
            _ => None,
        };
        self.picked(pick?, thread)
    }

    /// Take `pick` for the question shown: the answer of a single question, else on to the next tab.
    fn picked(&mut self, pick: Pick, thread: &ThreadId) -> Option<Option<To>> {
        let tab = self.tabs.get_mut(self.tab)?;
        tab.toggled = match &pick {
            Pick::Options(numbers) => numbers.iter().copied().collect(),
            Pick::Typed(_) => BTreeSet::new(),
        };
        tab.pick = Some(pick);
        if !self.several() {
            return Some(Some(answer(thread, self.question, self.picks())));
        }
        self.tab += 1;
        None
    }

    /// Take `key` on the tab submitting several questions: Enter sends the answer once every question has a pick,
    /// else goes to the first without one.
    fn submit(&mut self, key: Key, thread: &ThreadId) -> Option<Option<To>> {
        if key != Key::Enter {
            return None;
        }
        match self.tabs.iter().position(|t| t.pick.is_none()) {
            Some(open) => {
                self.tab = open;
                None
            }
            None => Some(Some(answer(thread, self.question, self.picks()))),
        }
    }

    /// Type `text` pasted on the row typing an answer, the cursor moving there: whether the question shown takes one.
    pub fn paste(&mut self, text: &str) -> bool {
        let Some(tab) = self.tabs.get_mut(self.tab).filter(|t| t.free_text) else { return false };
        tab.at = tab.options;
        tab.typed.paste(text);
        true
    }

    /// The keys the card takes, as the footer shows them.
    pub fn keys(&self) -> &'static str {
        let Some(tab) = self.tabs.get(self.tab) else {
            return if self.tabs.iter().all(|t| t.pick.is_some()) {
                "Enter send · ←→ questions · Esc leave"
            } else {
                "Enter next unanswered · ←→ questions · Esc leave"
            };
        };
        match (self.several(), tab.typing(), tab.multi) {
            (false, true, _) => "type an answer · Enter send (empty: the bar on /answer) · ↑↓ move · Esc leave",
            (false, false, false) => "↑↓ move · Enter pick · 1-9 answer · Esc leave",
            (false, false, true) => "↑↓ move · Space, 1-9 toggle · Enter send · Esc leave",
            (true, true, _) => "type an answer · Enter pick · ↑↓ move · Tab questions · Esc leave",
            (true, false, false) => "↑↓ move · Enter, 1-9 pick · ←→ questions · Esc leave",
            (true, false, true) => "↑↓ move · Space, 1-9 toggle · Enter confirm · ←→ questions · Esc leave",
        }
    }
}

/// The focus on a question card of a view: the cursor on it, once Enter or a number focused it.
#[derive(Debug, Default)]
pub struct Focus(Option<Cursor>);

impl Focus {
    pub fn cursor(&self) -> Option<&Cursor> {
        self.0.as_ref()
    }

    /// Whether a card has the focus, still showing or not.
    pub fn on(&self) -> bool {
        self.0.is_some()
    }

    /// Focus the card `cursor` is on, or none.
    pub fn set(&mut self, cursor: Option<Cursor>) {
        self.0 = cursor;
    }

    pub fn leave(&mut self) {
        self.0 = None;
    }

    /// Take `key` on the card it holds the cursor of, of questions asked on `thread` while the card still shows,
    /// `None` once it is gone: where it leads, if the card takes it. A card gone takes the key, meant for it rather
    /// than for whatever shows now, and ends the focus; the card keeps it while the bar takes its answer, and ends it
    /// once it leaves for anywhere else.
    pub fn key(&mut self, key: Key, thread: Option<&ThreadId>) -> Option<Option<To>> {
        let Some(thread) = thread else { return self.0.take().map(|_| None) };
        match self.0.as_mut()?.key(key, thread) {
            None => Some(None),
            Some(Some(to @ To::Bar(_))) => Some(Some(to)),
            Some(to) => {
                self.0 = None;
                Some(to)
            }
        }
    }

    /// Where a number led on a card not focused ([`key`], [`thread_key`]): somewhere, or nowhere with the card
    /// focused on the pick made.
    pub fn keyed(&mut self, keyed: Option<Keyed>) -> Option<To> {
        match keyed? {
            Keyed::To(to) => Some(*to),
            Keyed::Focus(cursor) => {
                self.0 = Some(cursor);
                None
            }
        }
    }

    /// Type `text` pasted on the focused card, while `shown`: whether it took it.
    pub fn paste(&mut self, text: &str, shown: bool) -> bool {
        shown && self.0.as_mut().is_some_and(|cursor| cursor.paste(text))
    }

    /// Move to the row `at` of the card of the message `question`, as a click on it does: whether that card has the
    /// focus.
    pub fn mouse_select(&mut self, question: Uuid, at: usize) -> bool {
        let cursor = self.0.as_mut().filter(|cursor| cursor.question == question);
        cursor.map(|cursor| cursor.mouse_select(at)).is_some()
    }
}

/// What a card the keys answer shows of `questions`, asked in the message `id`, at most `width` wide: with several, a
/// line of tabs, then the current question's text; its options one per line, numbered, wrapped, toggled where several
/// may be picked, then where allowed the row typing an answer; or on the last tab, the pick of each question. With
/// `cursor`, its tab and row; without, the first tab with no row marked.
pub fn body(questions: &Questions, id: Uuid, cursor: Option<&Cursor>, width: usize) -> Vec<Line<'static>> {
    let fresh;
    let (cursor, focused) = match cursor {
        Some(cursor) => (cursor, true),
        None => {
            fresh = Cursor::new(id, questions);
            (&fresh, false)
        }
    };
    let mut lines = Vec::new();
    if cursor.several() {
        lines.push(tabs(questions, cursor));
    }
    match (questions.questions.get(cursor.tab), cursor.tabs.get(cursor.tab)) {
        (Some(question), Some(tab)) => {
            if cursor.several() {
                lines.extend(wrapped(&question.text, width));
            }
            lines.extend(rows(question, tab, focused, width));
        }
        _ => lines.extend(review(questions, cursor, width)),
    }
    lines
}

/// The tabs of a card asking several questions: each question's title, marked once picked, then the one submitting
/// them; the current one in brackets.
fn tabs(questions: &Questions, cursor: &Cursor) -> Line<'static> {
    let titles = (1..)
        .zip(&questions.questions)
        .zip(&cursor.tabs)
        .map(|((n, q), t)| if t.pick.is_some() { format!("{} ✓", q.title(n)) } else { q.title(n) })
        .chain([SUBMIT.to_owned()]);
    let mut spans = Vec::new();
    for (i, title) in titles.enumerate() {
        if i > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(if i == cursor.tab {
            Span::styled(format!("[{title}]"), Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        } else {
            Span::styled(title, style::dim())
        });
    }
    Line::from(spans)
}

/// The pick of each question, on the tab submitting several.
fn review(questions: &Questions, cursor: &Cursor, width: usize) -> Vec<Line<'static>> {
    (1..)
        .zip(&questions.questions)
        .zip(&cursor.tabs)
        .map(|((n, question), t)| {
            let words = t
                .pick
                .as_ref()
                .map_or_else(|| "(no answer yet)".to_owned(), |pick| Answered { question, pick }.words());
            Line::raw(excerpt(&format!("  {}: {words}", question.title(n)), width))
        })
        .collect()
}

/// The options of `question` one per line, numbered, wrapped to `width` and where several may be picked toggled as
/// `tab` has them, the one picked marked; then where allowed the row typing an answer; while `focused`, the row of
/// `tab` marked and highlighted.
fn rows(question: &Question, tab: &Tab, focused: bool, width: usize) -> Vec<Line<'static>> {
    let at = focused.then_some(tab.at);
    let mut lines = Vec::new();
    let mut row = |n: usize, head: String, text: &str, style: Style| {
        let here = at == Some(n);
        let head = format!("{}{head}", if here { "› " } else { "  " });
        let indent = head.width();
        let style = if here { style.fg(Color::Cyan).add_modifier(Modifier::BOLD) } else { style };
        for (i, part) in style::rows(text, width.saturating_sub(indent)).into_iter().enumerate() {
            let start = if i == 0 { head.clone() } else { " ".repeat(indent) };
            lines.push(Line::styled(format!("{start}{part}"), style));
        }
    };
    for (n, option) in (1..).zip(&question.options) {
        let toggled = tab.toggled.contains(&n);
        let (head, text) = match (tab.multi, toggled) {
            (true, _) => (format!("[{}] {n} ", if toggled { "x" } else { " " }), option.line()),
            (false, true) => (format!("{n} "), format!("{} ✓", option.line())),
            (false, false) => (format!("{n} "), option.line()),
        };
        row(usize::try_from(n - 1).unwrap_or_default(), head, &text, Style::new());
    }
    if question.free_text {
        let text = match (at == Some(tab.options), tab.typed.is_empty()) {
            // The whole answer, wrapped as the row is.
            (true, _) => format!("{TYPE}: {}", tab.typed.caret(usize::MAX)),
            (false, true) => TYPE.to_owned(),
            (false, false) => format!("{TYPE}: {}", tab.typed.text()),
        };
        row(tab.options, String::new(), &text, style::dim());
    }
    lines
}

pub fn mouse_regions(
    mouse: &super::mouse::Regions,
    area: ratatui::layout::Rect,
    lines: &[Line<'_>],
    top: usize,
    question: Uuid,
) {
    for (row, line) in lines.iter().enumerate().skip(top).take(usize::from(area.height)) {
        let text = line.to_string();
        let text = text.trim_start().strip_prefix("› ").unwrap_or(text.trim_start()).trim_start();
        let text = if text.starts_with("[ ] ") || text.starts_with("[x] ") { &text[4..] } else { text };
        if text.starts_with(TYPE) {
            mouse.add(
                ratatui::layout::Rect { y: area.y + (row - top) as u16, height: 1, ..area },
                super::mouse::Target::Question(question, usize::MAX),
            );
            continue;
        }
        let Some((number, _)) = text.split_once(' ') else { continue };
        let Some(at) = number.parse::<usize>().ok().and_then(|n| n.checked_sub(1)) else { continue };
        mouse.add(
            ratatui::layout::Rect { y: area.y + (row - top) as u16, height: 1, ..area },
            super::mouse::Target::Question(question, at),
        );
    }
}

/// A question's `text` in lines at most `width` wide, each of its lines wrapped.
fn wrapped(text: &str, width: usize) -> impl Iterator<Item = Line<'static>> {
    text.lines().flat_map(move |line| style::rows(line, width)).map(Line::raw)
}

/// A card of questions at the bottom of a thread.
pub struct Card<'a> {
    pub asked: &'a Entry,
    pub question: &'a Questions,
    /// The answers stored to it, the first one first.
    pub answers: Vec<String>,
    /// Whether the keys answer it: the latest card with no answer stored.
    pub keys: bool,
}

impl Card<'_> {
    pub fn title(&self) -> String {
        let from = self.asked.from.as_ref().map_or("an ant", |f| f.as_str());
        let questions = if self.question.questions.len() > 1 { "questions" } else { "question" };
        format!(" {questions} from {from} ")
    }

    pub fn style(&self) -> Style {
        if self.answers.is_empty() { Style::new().fg(Color::Cyan) } else { style::dim() }
    }

    /// The questions, each with its options in a line, then the answers, at most `width` wide; on the card the keys
    /// answer, its [`body`] under `cursor` while focused, then the keys it takes: Enter focusing it with `enter`, the
    /// digits only once ↑ selects a message while `writing` in its thread.
    pub fn lines(&self, width: usize, cursor: Option<&Cursor>, (enter, writing): (bool, bool)) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        let questions = self.question.questions.as_slice();
        match (self.keys, self.asked.id) {
            (true, Some(id)) => {
                if let [one] = questions {
                    lines.extend(wrapped(&one.text, width));
                }
                lines.extend(body(self.question, id, cursor, width));
                let keys = match cursor {
                    Some(cursor) => cursor.keys(),
                    None if writing && enter => "Enter choose · ↑ then 1-9 answer",
                    None if writing => "↑ then 1-9 answer",
                    None if enter => "1-9 answer · Enter choose",
                    None => "1-9 answer",
                };
                lines.push(Line::styled(style::hints(keys, width), style::dim()));
            }
            _ => {
                for (n, question) in (1..).zip(questions) {
                    let text = if questions.len() > 1 {
                        format!("{}: {}", question.title(n), question.text)
                    } else {
                        question.text.clone()
                    };
                    lines.push(Line::raw(excerpt(&text, width)));
                    lines.push(Line::raw(excerpt(&numbered(question), width)));
                }
            }
        }
        lines.extend(self.answers.iter().map(|a| Line::styled(excerpt(a, width), Style::new().fg(Color::Green))));
        lines
    }
}

/// The question cards of a thread at `now`, oldest first: the questions to the human with no answer stored, and those
/// answered lately; one answered here, its answer not read back yet, shows as answered.
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
            let mut answers: Vec<String> =
                answers.into_iter().map(|(by, answered)| answer_line(by, answered)).collect();
            if answers.is_empty() && asked.id.is_some_and(|id| data.answering.contains(&id)) {
                answers.push(ANSWERING.to_owned());
            }
            Some(Card { asked, question, answers, keys: false })
        })
        .collect();
    if let Some(card) = cards.iter_mut().rev().find(|c| c.answers.is_empty() && c.asked.id.is_some()) {
        card.keys = true;
    }
    cards
}

/// The questions of the card the keys answer in a thread showing `entries`, and their envelope's id.
pub fn keyed<'a>(data: &Data, entries: &'a [Entry], now: DateTime<Utc>) -> Option<(Uuid, &'a Questions)> {
    let card = cards(data, entries, now).into_iter().find(|c| c.keys)?;
    Some((card.asked.id?, card.question))
}

/// Where `key` leads in a thread showing `entries`: on the card it answers.
pub fn thread_key(key: Key, data: &Data, thread: &ThreadId, entries: &[Entry], now: DateTime<Utc>) -> Option<Keyed> {
    let (id, questions) = keyed(data, entries, now)?;
    self::key(key, thread, id, questions)
}

#[cfg(test)]
mod tests {
    use ants_proto::Answer;

    use super::*;
    use crate::test_support::live::{at, message};
    use crate::tui::input::{Typed, exercise};
    use crate::tui::test_support::model;

    fn question(text: &str, options: &[&str], free_text: bool) -> Question {
        let options: Vec<String> = options.iter().map(|o| (*o).to_owned()).collect();
        Question::new(text, &options, free_text).unwrap()
    }

    fn shown(lines: &[Line]) -> Vec<String> {
        lines.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_card_lists_the_options_one_per_line_answers_by_number_and_shows_who_answered() {
        let thread = ThreadId::generate();
        let m = model();
        let mut asked = message(1, &thread, "dev-b", "alice", "Which retry policy?");
        asked.question = Some(question("Which retry policy?", &["exponential", "fixed 5s"], true).into());
        let mut entries = vec![asked.clone()];
        let card = &cards(&m.data, &entries, at(2))[0];
        assert_eq!(card.title(), " question from dev-b ");
        assert_eq!(
            shown(&card.lines(80, None, (true, false))),
            ["Which retry policy?", "  1 exponential", "  2 fixed 5s", "  type an answer", "1-9 answer · Enter choose"]
        );

        let answer = |n| Some(Keyed::To(Box::new(super::answer(&thread, asked.id.unwrap(), vec![Pick::option(n)]))));
        assert_eq!(thread_key(Key::Char('2'), &m.data, &thread, &entries, at(2)), answer(2));
        assert_eq!(thread_key(Key::Char('3'), &m.data, &thread, &entries, at(2)), None, "no third option");

        let mut reply = message(2, &thread, "alice", "dev-b", "fixed 5s");
        reply.answer = Some(Answer { question: asked.id.unwrap(), picks: vec![Pick::option(2)] });
        entries.push(reply);
        let card = &cards(&m.data, &entries, at(3))[0];
        assert_eq!(card.answers, ["answered by alice: option 2, fixed 5s"]);
        assert_eq!(
            shown(&card.lines(80, None, (true, false))),
            ["Which retry policy?", "1 exponential  2 fixed 5s", "answered by alice: option 2, fixed 5s"]
        );
        assert_eq!(thread_key(Key::Char('1'), &m.data, &thread, &entries, at(3)), None, "answered");
        assert!(cards(&m.data, &entries, at(200)).is_empty(), "the answered card goes after a while");
    }

    #[test]
    fn a_focused_card_wraps_its_options_under_a_cursor_that_enter_picks() {
        let (thread, id) = (ThreadId::generate(), Uuid::now_v7());
        let options = ["exponential backoff from one second up to a minute", "none"];
        let questions: Questions = question("Which retry policy?", &options, true).into();
        let mut cursor = Cursor::new(id, &questions);
        let lines = |cursor: &Cursor| shown(&body(&questions, id, Some(cursor), 24));
        assert_eq!(
            lines(&cursor),
            ["› 1 exponential backoff", "    from one second up", "    to a minute", "  2 none", "  type an answer"]
        );
        let answer = |n| Some(Some(super::answer(&thread, id, vec![Pick::option(n)])));
        assert_eq!(cursor.key(Key::Up, &thread), None, "at the top already");
        assert_eq!(cursor.key(Key::Down, &thread), None);
        assert_eq!(lines(&cursor)[3], "› 2 none");
        assert_eq!(cursor.clone().key(Key::Enter, &thread), answer(2));
        assert_eq!(cursor.clone().key(Key::Char('1'), &thread), answer(1), "digits answer at once");
        assert_eq!(cursor.key(Key::Char('3'), &thread), None, "no third option");
        assert_eq!(cursor.key(Key::Esc, &thread), Some(None));
    }

    #[test]
    fn the_last_row_of_a_focused_card_takes_a_typed_answer_in_place() {
        let (thread, id) = (ThreadId::generate(), Uuid::now_v7());
        let questions: Questions = question("Ship it?", &["yes", "no"], true).into();
        let mut cursor = Cursor::new(id, &questions);
        for _ in 0..3 {
            cursor.key(Key::Down, &thread);
        }
        assert!(cursor.typing(), "the cursor stops on the last row");
        assert_eq!(cursor.clone().key(Key::Enter, &thread), Some(Some(To::Bar("/answer ".into()))), "empty");
        for c in "after 2pm".chars() {
            assert_eq!(cursor.key(Key::Char(c), &thread), None, "digits and j k type here");
        }
        cursor.key(Key::Backspace, &thread);
        assert_eq!(
            shown(&body(&questions, id, Some(&cursor), 80)),
            ["  1 yes", "  2 no", "› type an answer: after 2p▏"]
        );
        let typed = Some(Some(answer(&thread, id, vec![Pick::Typed("after 2p".into())])));
        assert_eq!(cursor.key(Key::Enter, &thread), typed);

        let closed: Questions = question("Ship it?", &["yes", "no"], false).into();
        let mut cursor = Cursor::new(id, &closed);
        assert!(!cursor.paste("maybe"), "no typed answer to this one");
        cursor.key(Key::Down, &thread);
        cursor.key(Key::Down, &thread);
        assert!(!cursor.typing());
        assert_eq!(body(&closed, id, Some(&cursor), 80).len(), 2, "no row to type in");
    }

    #[test]
    fn the_typed_answer_edits_as_the_bar_does_tab_moving_between_questions() {
        let (thread, id) = (ThreadId::generate(), Uuid::now_v7());
        let mut questions: Questions = question("Ship it?", &["yes", "no"], true).into();
        questions.questions.push(question("When?", &["now", "later"], false));
        let mut cursor = Cursor::new(id, &questions);
        cursor.key(Key::Down, &thread);
        cursor.key(Key::Down, &thread);
        let want = exercise(|typed| match typed {
            Typed::Key(key) => assert_eq!(cursor.key(key, &thread), None),
            Typed::Paste(text) => assert!(cursor.paste(text)),
        });
        assert_eq!(cursor.tab, 0, "←→ moved in the text");
        assert_eq!(shown(&body(&questions, id, Some(&cursor), 80))[4], "› type an answer: X▏a b hi");
        assert_eq!(cursor.tabs[0].typed.text(), want);
        cursor.key(Key::Tab, &thread);
        assert_eq!(cursor.tab, 1);
    }

    #[test]
    fn options_of_a_question_taking_several_are_toggled_then_sent_with_enter() {
        let (thread, id) = (ThreadId::generate(), Uuid::now_v7());
        let regions = question("Which regions?", &["eu", "us", "ap"], false).multi();
        let questions: Questions = regions.into();
        let mut cursor = Cursor::new(id, &questions);
        assert_eq!(cursor.key(Key::Char('3'), &thread), None, "a digit toggles");
        assert_eq!(cursor.key(Key::Up, &thread), None);
        assert_eq!(cursor.key(Key::Char(' '), &thread), None, "Space toggles the row highlighted");
        assert_eq!(shown(&body(&questions, id, Some(&cursor), 80)), ["  [ ] 1 eu", "› [x] 2 us", "  [x] 3 ap"]);
        assert_eq!(cursor.keys(), "↑↓ move · Space, 1-9 toggle · Enter send · Esc leave");
        let sent = Some(Some(answer(&thread, id, vec![Pick::Options(vec![2, 3])])));
        assert_eq!(cursor.clone().key(Key::Enter, &thread), sent);
        cursor.key(Key::Char('2'), &thread);
        cursor.key(Key::Char('3'), &thread);
        let row = Some(Some(answer(&thread, id, vec![Pick::option(3)])));
        assert_eq!(cursor.key(Key::Enter, &thread), row, "none toggled: the row highlighted");
    }

    #[test]
    fn a_card_of_several_questions_has_a_tab_for_each_and_sends_every_pick_at_once() {
        let (thread, id) = (ThreadId::generate(), Uuid::now_v7());
        let regions = question("Which regions?", &["eu", "us", "ap"], false).multi().headed("Regions");
        let ship = question("Ship it?", &["yes", "no"], true);
        let questions = Questions::new(vec![regions, ship]).unwrap();
        let mut cursor = Cursor::new(id, &questions);
        let lines = |cursor: &Cursor| shown(&body(&questions, id, Some(cursor), 80));
        assert_eq!(
            lines(&cursor),
            ["[Regions]  Q2  Submit", "Which regions?", "› [ ] 1 eu", "  [ ] 2 us", "  [ ] 3 ap"]
        );
        cursor.key(Key::Char('1'), &thread);
        cursor.key(Key::Char('3'), &thread);
        assert_eq!(cursor.key(Key::Enter, &thread), None, "a pick moves on to the next question");
        assert_eq!(lines(&cursor)[..3], ["Regions ✓  [Q2]  Submit", "Ship it?", "› 1 yes"]);
        assert_eq!(cursor.key(Key::Right, &thread), None);
        assert_eq!(lines(&cursor), ["Regions ✓  Q2  [Submit]", "  Regions: eu, ap", "  Q2: (no answer yet)"]);
        assert_eq!(cursor.keys(), "Enter next unanswered · ←→ questions · Esc leave");
        assert_eq!(cursor.key(Key::Enter, &thread), None, "Enter goes to the question with no pick");
        assert_eq!(lines(&cursor)[0], "Regions ✓  [Q2]  Submit");
        assert_eq!(cursor.key(Key::Char('2'), &thread), None, "a digit picks and moves on");
        assert_eq!(lines(&cursor)[1..], ["  Regions: eu, ap", "  Q2: no"]);
        cursor.key(Key::Left, &thread);
        assert_eq!(lines(&cursor)[3], "  2 no ✓", "the pick shows going back");
        cursor.key(Key::Tab, &thread);
        let picks = vec![Pick::Options(vec![1, 3]), Pick::option(2)];
        assert_eq!(cursor.key(Key::Enter, &thread), Some(Some(answer(&thread, id, picks))), "one answer to both");
    }

    #[test]
    fn a_digit_on_a_card_of_several_questions_focuses_it_with_the_pick_made() {
        let thread = ThreadId::generate();
        let m = model();
        let mut asked = message(1, &thread, "dev-b", "alice", "two questions");
        let questions = Questions::new(vec![
            question("Ship it?", &["yes", "no"], false),
            question("Tag it?", &["yes", "no"], false),
        ]);
        asked.question = Some(questions.unwrap());
        let entries = vec![asked.clone()];
        let card = &cards(&m.data, &entries, at(2))[0];
        assert_eq!(card.title(), " questions from dev-b ");
        assert_eq!(
            shown(&card.lines(80, None, (false, false))),
            ["[Q1]  Q2  Submit", "Ship it?", "  1 yes", "  2 no", "1-9 answer"]
        );
        let Some(Keyed::Focus(cursor)) = thread_key(Key::Char('2'), &m.data, &thread, &entries, at(2)) else {
            panic!("a digit focuses the card")
        };
        let lines = shown(&body(asked.question.as_ref().unwrap(), asked.id.unwrap(), Some(&cursor), 80));
        assert_eq!(lines[..2], ["Q1 ✓  [Q2]  Submit", "Tag it?"]);
        assert_eq!(thread_key(Key::Char('3'), &m.data, &thread, &entries, at(2)), None, "no third option");
    }

    #[test]
    fn of_several_questions_waiting_only_the_latest_card_shows_the_keys_it_takes() {
        let thread = ThreadId::generate();
        let m = model();
        let mut entries = Vec::new();
        for (seq, text) in [(1, "Ship it?"), (2, "Tag it?")] {
            let mut asked = message(seq, &thread, "dev-b", "alice", text);
            asked.question = Some(question(text, &["yes", "no"], false).into());
            entries.push(asked);
        }
        let shown: Vec<Vec<String>> =
            cards(&m.data, &entries, at(3)).iter().map(|c| shown(&c.lines(80, None, (false, false)))).collect();
        assert_eq!(shown, [vec!["Ship it?", "1 yes  2 no"], vec!["Tag it?", "  1 yes", "  2 no", "1-9 answer"]]);
        let job =
            Job::Answer { thread: thread.clone(), question: entries[1].id.unwrap(), picks: vec![Pick::option(1)] };
        let key = thread_key(Key::Char('1'), &m.data, &thread, &entries, at(3));
        assert_eq!(
            key,
            Some(Keyed::To(Box::new(To::Do(Action::Run(job, String::new()))))),
            "the key answers the card showing it"
        );
    }

    #[test]
    fn a_card_answered_here_shows_its_answer_sent_and_takes_no_key() {
        let thread = ThreadId::generate();
        let mut m = model();
        let mut asked = message(1, &thread, "dev-b", "alice", "Ship it?");
        asked.question = Some(question("Ship it?", &["yes", "no"], false).into());
        let entries = vec![asked.clone()];
        m.data.answering.insert(asked.id.unwrap());
        let card = &cards(&m.data, &entries, at(2))[0];
        assert!(!card.keys);
        assert_eq!(shown(&card.lines(80, None, (false, false))), ["Ship it?", "1 yes  2 no", ANSWERING]);
        assert_eq!(thread_key(Key::Char('1'), &m.data, &thread, &entries, at(2)), None);
    }
}
