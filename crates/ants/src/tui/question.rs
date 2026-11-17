//! Questions from ants as cards, on home and at the bottom of their thread: the question, its numbered options, and
//! once an answer is stored, who answered and with what. `1`-`9` answers the card shown with that option and `r` opens
//! the command bar on `/answer` for a typed answer where the question allows one, as `ants answer` does.

use ants_proto::{Choice, Question, ThreadId, Uuid};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::exec::Job;
use super::model::{Action, Data, Key, To};
use super::style;
use crate::text::excerpt;
use crate::threads::thread::{Entry, answer_line, answers_to};

/// How long a card stays at the bottom of its thread once answered.
const ANSWERED_FOR: TimeDelta = TimeDelta::minutes(2);

/// The options in a line, numbered: `1 exponential  2 fixed 5s`.
fn numbered(question: &Question) -> String {
    let options: Vec<String> = question.options.iter().enumerate().map(|(n, o)| format!("{} {o}", n + 1)).collect();
    options.join("  ")
}

/// The options in a line, numbered, then how to type an answer where allowed.
pub fn options(question: &Question) -> String {
    let typed = if question.free_text { "  or r types an answer" } else { "" };
    format!("{}{typed}", numbered(question))
}

/// Where `key` leads on `question`, asked on `thread` in the message `id`: an option's number answers that question
/// with it, whatever else was asked since; `r` opens the bar on `/answer` where an answer may be typed.
pub fn key(key: Key, thread: &ThreadId, id: Uuid, question: &Question) -> Option<To> {
    match key {
        Key::Char(c @ '1'..='9') => {
            let n = c.to_digit(10)?;
            if usize::try_from(n).ok()? > question.options.len() {
                return None;
            }
            let job = Job::Answer { thread: thread.clone(), question: id, choice: Choice::Option(n) };
            Some(To::Do(Action::Run(job, String::new())))
        }
        Key::Char('r') if question.free_text => Some(To::Bar("/answer ".to_owned())),
        _ => None,
    }
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

    /// The question, its options, then its answers or, on the card they answer, the keys, each at most `width` wide.
    pub fn lines(&self, width: usize) -> Vec<Line<'static>> {
        let options = if self.keys { options(self.question) } else { numbered(self.question) };
        let mut lines = vec![Line::raw(excerpt(&self.question.text, width)), Line::raw(excerpt(&options, width))];
        if self.keys {
            lines.push(Line::styled("1-9 answer", style::dim()));
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
    fn a_card_numbers_the_options_answers_by_number_and_shows_who_answered() {
        let thread = ThreadId::generate();
        let m = model();
        let mut asked = message(1, &thread, "dev-b", "alice", "Which retry policy?");
        let options = ["exponential".to_owned(), "fixed 5s".to_owned()];
        asked.question = Some(Question::new("Which retry policy?", &options, true).unwrap());
        let mut entries = vec![asked.clone()];
        let card = &cards(&m.data, &entries, at(2))[0];
        assert_eq!(card.title(), " question from dev-b ");
        let lines: Vec<String> = card.lines(80).iter().map(ToString::to_string).collect();
        assert_eq!(lines, ["Which retry policy?", "1 exponential  2 fixed 5s  or r types an answer", "1-9 answer"]);

        let answer = |n| {
            let job = Job::Answer { thread: thread.clone(), question: asked.id.unwrap(), choice: Choice::Option(n) };
            Some(To::Do(Action::Run(job, String::new())))
        };
        assert_eq!(thread_key(Key::Char('2'), &m.data, &thread, &entries, at(2)), answer(2));
        assert_eq!(thread_key(Key::Char('3'), &m.data, &thread, &entries, at(2)), None, "no third option");
        let typed = thread_key(Key::Char('r'), &m.data, &thread, &entries, at(2));
        assert_eq!(typed, Some(To::Bar("/answer ".into())));

        let mut reply = message(2, &thread, "alice", "dev-b", "fixed 5s");
        reply.answer = Some(Answer { question: asked.id.unwrap(), option: Some(2) });
        entries.push(reply);
        let card = &cards(&m.data, &entries, at(3))[0];
        assert_eq!(card.answers, ["answered by alice: option 2, fixed 5s"]);
        assert_eq!(thread_key(Key::Char('1'), &m.data, &thread, &entries, at(3)), None, "answered");
        assert!(cards(&m.data, &entries, at(200)).is_empty(), "the answered card goes after a while");
    }

    #[test]
    fn of_several_questions_waiting_only_the_latest_card_shows_the_keys_it_takes() {
        let thread = ThreadId::generate();
        let m = model();
        let options = ["yes".to_owned(), "no".to_owned()];
        let mut entries = Vec::new();
        for (seq, text) in [(1, "Ship it?"), (2, "Tag it?")] {
            let mut asked = message(seq, &thread, "dev-b", "alice", text);
            asked.question = Some(Question::new(text, &options, true).unwrap());
            entries.push(asked);
        }
        let shown: Vec<Vec<String>> = cards(&m.data, &entries, at(3))
            .iter()
            .map(|c| c.lines(80).iter().map(ToString::to_string).collect())
            .collect();
        assert_eq!(
            shown,
            [vec!["Ship it?", "1 yes  2 no"], vec!["Tag it?", "1 yes  2 no  or r types an answer", "1-9 answer"]]
        );
        let job = Job::Answer { thread: thread.clone(), question: entries[1].id.unwrap(), choice: Choice::Option(1) };
        let key = thread_key(Key::Char('1'), &m.data, &thread, &entries, at(3));
        assert_eq!(key, Some(To::Do(Action::Run(job, String::new()))), "the key answers the card showing it");
    }
}
