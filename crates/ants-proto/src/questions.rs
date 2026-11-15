use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Envelope, Error, ThreadId, UserId};

/// Most characters a question's text has.
pub const MAX_QUESTION: usize = 500;
/// Most characters one option of a question has.
pub const MAX_OPTION: usize = 200;
/// Fewest options a question offers.
pub const MIN_OPTIONS: usize = 2;
/// Most options a question offers, so one digit picks any.
pub const MAX_OPTIONS: usize = 9;

/// A question an ant asks a human, with options to pick from by number ([`Envelope::question`]): its id is that of
/// the envelope carrying it, whose text repeats it with the options numbered ([`Question::render`]), so a reader that
/// does not know the field still shows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    /// 1 to [`MAX_QUESTION`] characters.
    pub text: String,
    /// [`MIN_OPTIONS`] to [`MAX_OPTIONS`] options, each one line of 1 to [`MAX_OPTION`] characters.
    pub options: Vec<String>,
    /// Whether an answer may be typed instead of an option picked.
    #[serde(default = "typed_by_default")]
    pub free_text: bool,
}

fn typed_by_default() -> bool {
    true
}

/// What an envelope answers a question with ([`Envelope::answer`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    /// The id of the question's envelope.
    pub question: Uuid,
    /// The option picked, from 1; none for a typed answer, the envelope's text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub option: Option<u32>,
}

/// How a human answers a question: an option by its number, from 1, or a typed text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Choice {
    Option(u32),
    Typed(String),
}

impl Choice {
    /// The choice `input` makes: a number picks that option, anything else is a typed answer.
    pub fn parse(input: &str) -> Self {
        let input = input.trim();
        match input.parse() {
            Ok(n) if input.bytes().all(|b| b.is_ascii_digit()) => Self::Option(n),
            _ => Self::Typed(input.to_owned()),
        }
    }
}

/// What a valid answer picked ([`Envelope::answers`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answered<'a> {
    /// That option, from 1, with its text.
    Option(u32, &'a str),
    /// A typed answer, the text of the answer's envelope.
    Typed,
}

impl Question {
    /// `text` with `options`, each trimmed, checked as [`Question::check`] does.
    pub fn new(text: &str, options: &[String], free_text: bool) -> Result<Self, Error> {
        let question = Self {
            text: text.trim().to_owned(),
            options: options.iter().map(|o| o.trim().to_owned()).collect(),
            free_text,
        };
        question.check()?;
        Ok(question)
    }

    /// That the text has 1 to [`MAX_QUESTION`] characters, and there are [`MIN_OPTIONS`] to [`MAX_OPTIONS`] options,
    /// each one line of 1 to [`MAX_OPTION`] characters with no control characters. A reader takes a question that
    /// fails it as none.
    pub fn check(&self) -> Result<(), Error> {
        let invalid = |why: String| Err(Error::InvalidQuestion(why));
        let length = self.text.trim().chars().count();
        if length == 0 || length > MAX_QUESTION {
            return invalid(format!("the question has {length} characters; use 1-{MAX_QUESTION}"));
        }
        if !(MIN_OPTIONS..=MAX_OPTIONS).contains(&self.options.len()) {
            return invalid(format!("{} options; use {MIN_OPTIONS}-{MAX_OPTIONS}", self.options.len()));
        }
        for (n, option) in self.options.iter().enumerate() {
            let length = option.trim().chars().count();
            if length == 0 || length > MAX_OPTION || option.chars().any(breaks_line) {
                return invalid(format!("option {} {option:?}: use one line of 1-{MAX_OPTION} characters", n + 1));
            }
        }
        Ok(())
    }

    /// The question with its options numbered from 1, one a line, and whether a typed answer is taken: the text of
    /// the envelope carrying it.
    pub fn render(&self) -> String {
        let mut text = self.text.clone();
        text.push('\n');
        for line in self.option_lines() {
            text.push('\n');
            text.push_str(&line);
        }
        text.push_str("\n\n");
        text.push_str(self.typed_note());
        text
    }

    /// The options numbered from 1, as `1. option`.
    pub fn option_lines(&self) -> impl Iterator<Item = String> + '_ {
        self.options.iter().zip(1..).map(|(option, n)| format!("{n}. {option}"))
    }

    /// Whether a typed answer is taken, as a reader tells it.
    pub fn typed_note(&self) -> &'static str {
        if self.free_text {
            "Answer with an option's number or in own words."
        } else {
            "Answer with an option's number."
        }
    }

    /// The answer `choice` gives the question whose envelope's id is `question`, with its text: the option's text, or
    /// the typed one, trimmed. Fails for an option the question lacks, an empty text, or any text where only options
    /// are taken.
    pub fn answer(&self, question: Uuid, choice: Choice) -> Result<(Answer, String), Error> {
        match choice {
            Choice::Option(n) => {
                let option = self
                    .option(n)
                    .ok_or_else(|| Error::InvalidAnswer(format!("no option {n}; pick 1-{}", self.options.len())))?;
                Ok((Answer { question, option: Some(n) }, option.to_owned()))
            }
            Choice::Typed(_) if !self.free_text => Err(Error::InvalidAnswer(format!(
                "the question takes only an option's number, 1-{}",
                self.options.len()
            ))),
            Choice::Typed(text) if text.trim().is_empty() => Err(Error::InvalidAnswer("an empty answer".to_owned())),
            Choice::Typed(text) => Ok((Answer { question, option: None }, text.trim().to_owned())),
        }
    }

    /// What `answer` picks of the question: one of its options, or a typed answer where taken; None for anything
    /// else. It does not check that `answer` names the question.
    pub fn answered(&self, answer: &Answer) -> Option<Answered<'_>> {
        match answer.option {
            Some(n) => self.option(n).map(|option| Answered::Option(n, option)),
            None => self.free_text.then_some(Answered::Typed),
        }
    }

    /// Option `n`, from 1.
    pub fn option(&self, n: u32) -> Option<&str> {
        let index = usize::try_from(n).ok()?.checked_sub(1)?;
        self.options.get(index).map(String::as_str)
    }
}

fn breaks_line(c: char) -> bool {
    c.is_control() || matches!(c, '\u{2028}' | '\u{2029}')
}

impl Envelope {
    /// `from` asks `to` `question` on `thread_id`, its text the question rendered ([`Question::render`]).
    pub fn asking(thread_id: ThreadId, from: UserId, to: UserId, question: Question) -> Self {
        let asking = Self::on_thread(thread_id, from, to, question.render());
        Self { question: Some(question), ..asking }
    }

    /// The question the envelope asks, if valid ([`Question::check`]).
    pub fn asks(&self) -> Option<&Question> {
        self.question.as_ref().filter(|q| q.check().is_ok())
    }

    /// What the envelope answers `question` with, when it carries a valid answer to it: `question` asks a valid
    /// question ([`Envelope::asks`]) of the envelope's sender, sent by its recipient on its thread, and the answer
    /// picks one of its options, or is typed where that is taken. Anything else is no answer, and the envelope an
    /// ordinary message. Trust it only once both envelopes passed [`Envelope::check_subject`].
    pub fn answers<'q>(&self, question: &'q Envelope) -> Option<Answered<'q>> {
        self.answers_as(question, &self.from)
    }

    /// [`Envelope::answers`], taking `answerer` for the one who answers rather than the sender: the human its
    /// `on_behalf_of` names once a reader verified that claim, as an assistant answers for its human.
    pub fn answers_as<'q>(&self, question: &'q Envelope, answerer: &UserId) -> Option<Answered<'q>> {
        let answer = self.answer.as_ref()?;
        let asked = question.asks()?;
        let ours = answer.question == question.id
            && question.thread_id == self.thread_id
            && question.from == self.to
            && question.to == *answerer;
        if !ours {
            return None;
        }
        asked.answered(answer)
    }

    /// `from`'s answer `choice` to `question`, the envelope asking it, on its thread to its sender. Fails as
    /// [`Question::answer`] does, or when `question` asks none.
    pub fn answering(question: &Envelope, from: UserId, choice: Choice) -> Result<Self, Error> {
        let asked =
            question.asks().ok_or_else(|| Error::InvalidAnswer(format!("message {} asks no question", question.id)))?;
        let (answer, text) = asked.answer(question.id, choice)?;
        let answering = Self::on_thread(question.thread_id.clone(), from, question.from.clone(), text);
        Ok(Self { answer: Some(answer), ..answering })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    fn options(options: &[&str]) -> Vec<String> {
        options.iter().map(|&o| o.to_owned()).collect()
    }

    fn question(free_text: bool) -> Question {
        Question::new("  Ship it? ", &options(&[" yes ", "no"]), free_text).unwrap()
    }

    #[test]
    fn a_question_has_one_to_500_characters_and_two_to_nine_one_line_options() {
        let q = question(true);
        assert_eq!((q.text.as_str(), q.options.as_slice()), ("Ship it?", options(&["yes", "no"]).as_slice()));
        let long = "x".repeat(MAX_QUESTION + 1);
        assert!(Question::new(&long, &options(&["a", "b"]), true).is_err());
        assert!(Question::new(" ", &options(&["a", "b"]), true).is_err());
        assert!(Question::new("q", &options(&["a"]), true).is_err(), "one option");
        let ten: Vec<String> = (0..10).map(|n| n.to_string()).collect();
        assert!(Question::new("q", &ten, true).is_err());
        assert!(Question::new("q", &ten[..9], true).is_ok());
        assert!(Question::new("q", &options(&["a", "two\nlines"]), true).is_err());
        assert!(Question::new("q", &options(&["a", " "]), true).is_err());
        assert!(Question::new("q", &options(&["a", &"x".repeat(MAX_OPTION + 1)]), true).is_err());
        assert!(Question::new("multi\nline question", &options(&["a", "b"]), false).is_ok());
    }

    #[test]
    fn free_text_defaults_to_true_on_the_wire() {
        let q: Question = serde_json::from_str(r#"{"text":"q","options":["a","b"]}"#).unwrap();
        assert!(q.free_text);
    }

    #[test]
    fn the_envelope_text_repeats_the_question_and_the_numbered_options() {
        let asking = Envelope::asking("t1".parse().unwrap(), p("dev-a"), p("alice"), question(false));
        assert_eq!(asking.text, "Ship it?\n\n1. yes\n2. no\n\nAnswer with an option's number.");
        assert!(question(true).render().ends_with("or in own words."));
    }

    #[test]
    fn a_choice_is_an_option_number_or_typed_text() {
        assert_eq!(Choice::parse(" 2 "), Choice::Option(2));
        assert_eq!(Choice::parse("+2"), Choice::Typed("+2".to_owned()));
        assert_eq!(Choice::parse("2 please"), Choice::Typed("2 please".to_owned()));
    }

    #[test]
    fn an_answer_picks_an_option_or_types_where_allowed() {
        let asking = Envelope::asking("t1".parse().unwrap(), p("dev-a"), p("alice"), question(false));
        let answer = Envelope::answering(&asking, p("alice"), Choice::Option(2)).unwrap();
        assert_eq!((answer.to.as_str(), answer.text.as_str()), ("dev-a", "no"));
        assert_eq!(answer.thread_id, asking.thread_id);
        assert_eq!(answer.answers(&asking), Some(Answered::Option(2, "no")));
        assert!(Envelope::answering(&asking, p("alice"), Choice::Option(3)).is_err());
        assert!(Envelope::answering(&asking, p("alice"), Choice::Option(0)).is_err());
        assert!(Envelope::answering(&asking, p("alice"), Choice::Typed("maybe".to_owned())).is_err());

        let open = Envelope::asking("t1".parse().unwrap(), p("dev-a"), p("alice"), question(true));
        let typed = Envelope::answering(&open, p("alice"), Choice::Typed(" later ".to_owned())).unwrap();
        assert_eq!(typed.text, "later");
        assert_eq!(typed.answers(&open), Some(Answered::Typed));
        assert!(Envelope::answering(&open, p("alice"), Choice::Typed(" ".to_owned())).is_err());
        let plain = Envelope::new(p("dev-a"), p("alice"), "hi");
        assert!(Envelope::answering(&plain, p("alice"), Choice::Option(1)).is_err());
    }

    #[test]
    fn an_invalid_answer_is_no_answer() {
        let asking = Envelope::asking("t1".parse().unwrap(), p("dev-a"), p("alice"), question(false));
        let answer = Envelope::answering(&asking, p("alice"), Choice::Option(1)).unwrap();
        let other = Envelope::asking("t1".parse().unwrap(), p("dev-a"), p("alice"), question(false));
        assert_eq!(answer.answers(&other), None, "another question");
        let elsewhere = Envelope { thread_id: "t2".parse().unwrap(), ..answer.clone() };
        assert_eq!(elsewhere.answers(&asking), None, "another thread");
        let by_bob = Envelope { from: p("bob"), ..answer.clone() };
        assert_eq!(by_bob.answers(&asking), None, "not asked of the sender");
        let option = Envelope { answer: Some(Answer { question: asking.id, option: Some(3) }), ..answer.clone() };
        assert_eq!(option.answers(&asking), None, "no such option");
        let typed = Envelope { answer: Some(Answer { question: asking.id, option: None }), ..answer };
        assert_eq!(typed.answers(&asking), None, "typed where not allowed");
        let mut broken = asking;
        broken.question.as_mut().unwrap().options.truncate(1);
        assert_eq!(broken.asks(), None);
    }
}
