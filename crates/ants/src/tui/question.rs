//! Questions from ants as cards, on home and at the bottom of their thread: the question, its numbered options, and
//! once an answer is stored, who answered and with what. `1`-`9` answers with that option and `r` opens the command
//! bar on `/answer` for a typed answer where the question allows one, as `ants answer` does.

use ants_proto::{Choice, Question, ThreadId};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::exec::Job;
use super::model::{Action, Data, Key, To};
use super::style;
use crate::thread::{Entry, answer_line, answers_to};

/// How long a card stays at the bottom of its thread once answered.
const ANSWERED_FOR: TimeDelta = TimeDelta::minutes(2);

/// The options in a line, numbered: `1 exponential  2 fixed 5s`, then how to type an answer where allowed.
pub fn options(question: &Question) -> String {
    let options: Vec<String> = question.options.iter().enumerate().map(|(n, o)| format!("{} {o}", n + 1)).collect();
    let typed = if question.free_text { "  or r types an answer" } else { "" };
    format!("{}{typed}", options.join("  "))
}

/// Where `key` leads on `question`, asked on `thread`: an option's number answers with it; `r` opens the bar on
/// `/answer` where an answer may be typed.
pub fn key(key: Key, thread: &ThreadId, question: &Question) -> Option<To> {
    match key {
        Key::Char(c @ '1'..='9') => {
            let n = c.to_digit(10)?;
            if usize::try_from(n).ok()? > question.options.len() {
                return None;
            }
            let job = Job::Answer { thread: thread.clone(), choice: Choice::Option(n) };
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
}

impl Card<'_> {
    pub fn title(&self) -> String {
        let from = self.asked.from.as_ref().map_or("an ant", |f| f.as_str());
        format!(" question from {from} ")
    }

    pub fn style(&self) -> Style {
        if self.answers.is_empty() { Style::new().fg(Color::Cyan) } else { style::dim() }
    }

    /// The question, its options, then its answers or the keys, each at most `width` wide.
    pub fn lines(&self, width: usize) -> Vec<Line<'static>> {
        let mut lines = vec![
            Line::raw(crate::excerpt(&self.question.text, width)),
            Line::raw(crate::excerpt(&options(self.question), width)),
        ];
        if self.answers.is_empty() {
            lines.push(Line::styled("1-9 answer", style::dim()));
        }
        lines
            .extend(self.answers.iter().map(|a| Line::styled(crate::excerpt(a, width), Style::new().fg(Color::Green))));
        lines
    }
}

/// The question cards of a thread at `now`, oldest first: the questions to the human with no answer stored, and those
/// answered lately.
pub fn cards<'a>(data: &Data, entries: &'a [Entry], now: DateTime<Utc>) -> Vec<Card<'a>> {
    entries
        .iter()
        .filter(|e| e.to.as_ref() == Some(data.me()) && !e.forged)
        .filter_map(|asked| {
            let question = asked.question.as_ref()?;
            let answers: Vec<(&Entry, _)> = answers_to(asked, entries).collect();
            if answers.first().is_some_and(|(by, _)| now - by.stored_at >= ANSWERED_FOR) {
                return None;
            }
            let answers = answers.into_iter().map(|(by, answered)| answer_line(by, answered)).collect();
            Some(Card { asked, question, answers })
        })
        .collect()
}

/// Where `key` leads in a thread showing `entries`: on its latest question to the human with no answer stored.
pub fn thread_key(key: Key, data: &Data, thread: &ThreadId, entries: &[Entry], now: DateTime<Utc>) -> Option<To> {
    let card = cards(data, entries, now).into_iter().rev().find(|c| c.answers.is_empty())?;
    self::key(key, thread, card.question)
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
            let job = Job::Answer { thread: thread.clone(), choice: Choice::Option(n) };
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
}
