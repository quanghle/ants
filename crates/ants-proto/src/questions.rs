//! Questions an ant asks a human, 1 to [`MAX_QUESTIONS`] at once, each with options to pick from: the [`Questions`] an
//! [`crate::Envelope`] carries, and the [`Answer`] of the reply, which gives each question a [`Pick`]: options by
//! their numbers, or a typed text.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Envelope, Error, ThreadId, UserId};

/// Most questions one envelope asks, answered together.
pub const MAX_QUESTIONS: usize = 4;
/// Most characters a question's text has.
pub const MAX_QUESTION: usize = 500;
/// Most characters a question's header has, the short label its tab shows.
pub const MAX_HEADER: usize = 12;
/// Most characters an option's label has.
pub const MAX_OPTION: usize = 200;
/// Most characters an option's description has.
pub const MAX_DESCRIPTION: usize = 300;
/// Fewest options a question offers.
pub const MIN_OPTIONS: usize = 2;
/// Most options a question offers, so one digit picks any.
pub const MAX_OPTIONS: usize = 9;
/// Most characters a typed answer to one question has.
pub const MAX_TYPED: usize = 1000;

/// The questions an ant asks a human in one message ([`Envelope::question`]), answered together by one message: its
/// id is that of the envelope carrying them, whose text repeats them with their options numbered
/// ([`Questions::render`]), so a reader that does not know the field still shows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Questions {
    /// 1 to [`MAX_QUESTIONS`] questions, in the order they are answered.
    #[serde(default)]
    pub questions: Vec<Question>,
}

/// One question of [`Questions`], with options to pick from by number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    /// 1 to [`MAX_QUESTION`] characters.
    pub text: String,
    /// A short label for the question, one line of 1 to [`MAX_HEADER`] characters: the tab naming it among several.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// [`MIN_OPTIONS`] to [`MAX_OPTIONS`] options.
    pub options: Vec<QuestionOption>,
    /// Whether an answer may pick several options.
    #[serde(default)]
    pub multi_select: bool,
    /// Whether an answer may be typed instead of options picked.
    #[serde(default = "typed_by_default")]
    pub free_text: bool,
}

fn typed_by_default() -> bool {
    true
}

/// An option of a [`Question`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionOption {
    /// One line of 1 to [`MAX_OPTION`] characters.
    pub label: String,
    /// What picking it means, one line of 1 to [`MAX_DESCRIPTION`] characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// What an envelope answers questions with ([`Envelope::answer`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    /// The id of the questions' envelope.
    pub question: Uuid,
    /// A pick for each question, in their order.
    #[serde(default)]
    pub picks: Vec<Pick>,
}

/// How one question is answered: options by their numbers, from 1, several only where the question takes several, or
/// a typed text, one line of 1 to [`MAX_TYPED`] characters, where it takes one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pick {
    Options(Vec<u32>),
    Typed(String),
}

impl Pick {
    /// Option `n`, from 1.
    pub fn option(n: u32) -> Self {
        Self::Options(vec![n])
    }

    /// The pick `input` makes: a number picks that option, numbers joined by commas pick those, anything else is a
    /// typed answer.
    pub fn parse(input: &str) -> Self {
        let input = input.trim();
        let numbers: Option<Vec<u32>> = input
            .split(',')
            .map(str::trim)
            .map(|n| n.bytes().all(|b| b.is_ascii_digit()).then(|| n.parse().ok()).flatten())
            .collect();
        match numbers {
            Some(numbers) => Self::Options(numbers),
            None => Self::Typed(input.to_owned()),
        }
    }
}

/// What a valid answer picked for one question ([`Envelope::answers`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Answered<'a> {
    pub question: &'a Question,
    pub pick: &'a Pick,
}

impl Answered<'_> {
    /// The answer in words: the labels of the options picked, or the typed text.
    pub fn words(&self) -> String {
        match self.pick {
            Pick::Options(numbers) => {
                let labels: Vec<&str> =
                    numbers.iter().filter_map(|&n| self.question.option(n)).map(|o| o.label.as_str()).collect();
                labels.join(", ")
            }
            Pick::Typed(text) => text.clone(),
        }
    }

    /// What was picked: `option 2, fixed`, `options 1, eu and 3, us`, or `typed: after lunch`.
    pub fn picked(&self) -> String {
        match self.pick {
            Pick::Options(numbers) => {
                let picked: Vec<String> =
                    numbers.iter().filter_map(|&n| Some(format!("{n}, {}", self.question.option(n)?.label))).collect();
                let s = if picked.len() == 1 { "" } else { "s" };
                format!("option{s} {}", picked.join(" and "))
            }
            Pick::Typed(text) => format!("typed: {text}"),
        }
    }
}

impl QuestionOption {
    /// An option with `label`, trimmed, and no description.
    pub fn new(label: &str) -> Self {
        Self { label: label.trim().to_owned(), description: None }
    }

    /// An option with `label` and `description`, trimmed, an empty one being none.
    pub fn described(label: &str, description: &str) -> Self {
        let description = Some(description.trim().to_owned()).filter(|d| !d.is_empty());
        Self { description, ..Self::new(label) }
    }

    /// The label, then the description if any: `fixed — retry every 5s`.
    pub fn line(&self) -> String {
        match &self.description {
            Some(description) => format!("{} — {description}", self.label),
            None => self.label.clone(),
        }
    }
}

impl Question {
    /// `text` with `options`, each trimmed, one of which an answer picks, checked as [`Question::check`] does.
    pub fn new(text: &str, options: &[String], free_text: bool) -> Result<Self, Error> {
        let options = options.iter().map(|o| QuestionOption::new(o)).collect();
        let question = Self { text: text.to_owned(), header: None, options, multi_select: false, free_text }.trimmed();
        question.check().map_err(Error::InvalidQuestion)?;
        Ok(question)
    }

    /// The question with `header`, trimmed, an empty one being none.
    #[must_use]
    pub fn headed(self, header: &str) -> Self {
        Self { header: Some(header.trim().to_owned()).filter(|h| !h.is_empty()), ..self }
    }

    /// The question taking several options in an answer.
    #[must_use]
    pub fn multi(self) -> Self {
        Self { multi_select: true, ..self }
    }

    /// The question with its text, header and options trimmed, an empty header or description being none.
    #[must_use]
    pub fn trimmed(self) -> Self {
        let header = self.header.as_deref().map(str::trim).filter(|h| !h.is_empty()).map(str::to_owned);
        let options = self
            .options
            .iter()
            .map(|o| QuestionOption::described(&o.label, o.description.as_deref().unwrap_or_default()))
            .collect();
        Self { text: self.text.trim().to_owned(), header, options, ..self }
    }

    /// That the text has 1 to [`MAX_QUESTION`] characters, a header is one line of 1 to [`MAX_HEADER`], and there are
    /// [`MIN_OPTIONS`] to [`MAX_OPTIONS`] options, each label one line of 1 to [`MAX_OPTION`] characters and each
    /// description one line of 1 to [`MAX_DESCRIPTION`].
    pub fn check(&self) -> Result<(), String> {
        let length = self.text.trim().chars().count();
        if length == 0 || length > MAX_QUESTION {
            return Err(format!("the question has {length} characters; use 1-{MAX_QUESTION}"));
        }
        if let Some(header) = &self.header
            && !one_line(header, MAX_HEADER)
        {
            return Err(format!("header {header:?}: use one line of 1-{MAX_HEADER} characters"));
        }
        if !(MIN_OPTIONS..=MAX_OPTIONS).contains(&self.options.len()) {
            return Err(format!("{} options; use {MIN_OPTIONS}-{MAX_OPTIONS}", self.options.len()));
        }
        for (n, option) in (1..).zip(&self.options) {
            let label = &option.label;
            if !one_line(label, MAX_OPTION) {
                return Err(format!("option {n} {label:?}: use one line of 1-{MAX_OPTION} characters"));
            }
            if let Some(description) = &option.description
                && !one_line(description, MAX_DESCRIPTION)
            {
                return Err(format!(
                    "the description of option {n} {label:?}: use one line of 1-{MAX_DESCRIPTION} characters"
                ));
            }
        }
        Ok(())
    }

    /// The options numbered from 1, as `1. option`, with their descriptions.
    pub fn option_lines(&self) -> impl Iterator<Item = String> + '_ {
        self.options.iter().zip(1..).map(|(option, n)| format!("{n}. {}", option.line()))
    }

    /// How an answer is given, as a reader tells it: one option or several, or a typed text where taken.
    pub fn typed_note(&self) -> &'static str {
        match (self.multi_select, self.free_text) {
            (false, true) => "Answer with an option's number or in own words.",
            (false, false) => "Answer with an option's number.",
            (true, true) => "Answer with one or more options' numbers or in own words.",
            (true, false) => "Answer with one or more options' numbers.",
        }
    }

    /// The tab naming the question, `n`th from 1, among several: its header, else `Q<n>`.
    pub fn title(&self, n: usize) -> String {
        self.header.clone().unwrap_or_else(|| format!("Q{n}"))
    }

    /// Option `n`, from 1.
    pub fn option(&self, n: u32) -> Option<&QuestionOption> {
        let index = usize::try_from(n).ok()?.checked_sub(1)?;
        self.options.get(index)
    }

    /// `pick` as an answer to the question takes it: its options in order, each once, or its text trimmed. Fails for
    /// no option, one the question lacks, several where it takes one, or a text where it takes none, an empty one or
    /// one longer than a line of [`MAX_TYPED`] characters.
    pub fn pick(&self, pick: Pick) -> Result<Pick, String> {
        match pick {
            Pick::Options(mut numbers) => {
                numbers.sort_unstable();
                numbers.dedup();
                self.check_pick(&Pick::Options(numbers.clone()))?;
                Ok(Pick::Options(numbers))
            }
            Pick::Typed(text) => {
                let pick = Pick::Typed(text.trim().to_owned());
                self.check_pick(&pick)?;
                Ok(pick)
            }
        }
    }

    /// That `pick` answers the question, as [`Question::pick`] takes it.
    fn check_pick(&self, pick: &Pick) -> Result<(), String> {
        let count = self.options.len();
        match pick {
            Pick::Options(numbers) if numbers.is_empty() => Err(format!("no option picked; pick 1-{count}")),
            Pick::Options(numbers) => {
                if let Some(n) = numbers.iter().find(|&&n| self.option(n).is_none()) {
                    return Err(format!("no option {n}; pick 1-{count}"));
                }
                let distinct = numbers.windows(2).all(|w| w[0] < w[1]);
                if !distinct {
                    return Err("an option picked twice".to_owned());
                }
                if numbers.len() > 1 && !self.multi_select {
                    return Err(format!("{} options picked; the question takes one", numbers.len()));
                }
                Ok(())
            }
            Pick::Typed(_) if !self.free_text => {
                let what = if self.multi_select { "options' numbers" } else { "an option's number" };
                Err(format!("the question takes only {what}, 1-{count}"))
            }
            Pick::Typed(text) if text.trim().is_empty() => Err("an empty answer".to_owned()),
            Pick::Typed(text) if !one_line(text, MAX_TYPED) => {
                Err(format!("a typed answer is one line of 1-{MAX_TYPED} characters"))
            }
            Pick::Typed(_) => Ok(()),
        }
    }
}

/// Whether `text` is one line of 1 to `max` characters, without spaces around it.
fn one_line(text: &str, max: usize) -> bool {
    let length = text.chars().count();
    (1..=max).contains(&length) && text.trim() == text && !text.chars().any(breaks_line)
}

pub(crate) fn breaks_line(c: char) -> bool {
    c.is_control() || matches!(c, '\u{2028}' | '\u{2029}')
}

impl From<Question> for Questions {
    fn from(question: Question) -> Self {
        Self { questions: vec![question] }
    }
}

impl Questions {
    /// `questions`, each trimmed ([`Question::trimmed`]), checked as [`Questions::check`] does.
    pub fn new(questions: Vec<Question>) -> Result<Self, Error> {
        let questions = Self { questions: questions.into_iter().map(Question::trimmed).collect() };
        questions.check()?;
        Ok(questions)
    }

    /// That there are 1 to [`MAX_QUESTIONS`] questions, each valid ([`Question::check`]). A reader takes questions
    /// that fail it as none.
    pub fn check(&self) -> Result<(), Error> {
        let count = self.questions.len();
        if !(1..=MAX_QUESTIONS).contains(&count) {
            return Err(Error::InvalidQuestion(format!("{count} questions; ask 1-{MAX_QUESTIONS}")));
        }
        for (n, question) in (1..).zip(&self.questions) {
            question.check().map_err(|why| Error::InvalidQuestion(self.of(n, why)))?;
        }
        Ok(())
    }

    /// `why` about question `n`, from 1, naming it among several.
    fn of(&self, n: usize, why: String) -> String {
        if self.questions.len() == 1 { why } else { format!("question {n}: {why}") }
    }

    /// Each question with its options numbered from 1, one a line, and how an answer is given; among several, each
    /// headed `Question N of M`, with its header: the text of the envelope carrying them.
    pub fn render(&self) -> String {
        let count = self.questions.len();
        let rendered: Vec<String> = (1..)
            .zip(&self.questions)
            .map(|(n, question)| {
                let mut text = match (count, &question.header) {
                    (1, _) => question.text.clone(),
                    (_, Some(header)) => format!("Question {n} of {count}, {header}: {}", question.text),
                    (_, None) => format!("Question {n} of {count}: {}", question.text),
                };
                text.push('\n');
                for line in question.option_lines() {
                    text.push('\n');
                    text.push_str(&line);
                }
                text.push_str("\n\n");
                text.push_str(question.typed_note());
                text
            })
            .collect();
        rendered.join("\n\n")
    }

    /// The picks `words` make, as a command line gives them: with one question, all the words make its pick
    /// ([`Pick::parse`]); with several, each word is the pick of the next question.
    pub fn picks(&self, words: &[String]) -> Result<Vec<Pick>, Error> {
        if self.questions.len() == 1 {
            return Ok(vec![Pick::parse(&words.join(" "))]);
        }
        self.count(words.len())?;
        Ok(words.iter().map(|w| Pick::parse(w)).collect())
    }

    /// That `picks` picks answer every question, one each.
    fn count(&self, picks: usize) -> Result<(), Error> {
        let count = self.questions.len();
        if picks == count {
            return Ok(());
        }
        Err(Error::InvalidAnswer(format!("{picks} answers to {count} questions; give one to each, in their order")))
    }

    /// The answer `picks`, one for each question in order, give the questions whose envelope's id is `question`, with
    /// its text ([`Questions::answer_text`]). Fails for a pick that does not answer its question ([`Question::pick`]),
    /// or a count of picks other than the questions'.
    pub fn answer(&self, question: Uuid, picks: Vec<Pick>) -> Result<(Answer, String), Error> {
        self.count(picks.len())?;
        let picks: Vec<Pick> = (1..)
            .zip(self.questions.iter().zip(picks))
            .map(|(n, (asked, pick))| asked.pick(pick).map_err(|why| Error::InvalidAnswer(self.of(n, why))))
            .collect::<Result<_, _>>()?;
        let answer = Answer { question, picks };
        let answered = self.answered(&answer).ok_or_else(|| Error::InvalidAnswer("no answer".to_owned()))?;
        let text = self.answer_text(&answered);
        Ok((answer, text))
    }

    /// The text of an answer's envelope: the answer in words ([`Answered::words`]); among several questions, one line
    /// for each, `title: words` ([`Question::title`]).
    pub fn answer_text(&self, answered: &[Answered<'_>]) -> String {
        if let [one] = answered {
            return one.words();
        }
        let lines: Vec<String> =
            (1..).zip(answered).map(|(n, a)| format!("{}: {}", a.question.title(n), a.words())).collect();
        lines.join("\n")
    }

    /// What `answer` picks for each question, if it answers them all validly ([`Question::pick`]); None for anything
    /// else. It does not check that `answer` names these questions.
    pub fn answered<'a>(&'a self, answer: &'a Answer) -> Option<Vec<Answered<'a>>> {
        if answer.picks.len() != self.questions.len() {
            return None;
        }
        self.questions
            .iter()
            .zip(&answer.picks)
            .map(|(question, pick)| question.check_pick(pick).ok().map(|()| Answered { question, pick }))
            .collect()
    }
}

impl Envelope {
    /// `from` asks `to` `questions` on `thread_id`, its text the questions rendered ([`Questions::render`]).
    pub fn asking(thread_id: ThreadId, from: UserId, to: UserId, questions: impl Into<Questions>) -> Self {
        let questions = questions.into();
        let asking = Self::on_thread(thread_id, from, to, questions.render());
        Self { question: Some(questions), ..asking }
    }

    /// The questions the envelope asks, if valid ([`Questions::check`]).
    pub fn asks(&self) -> Option<&Questions> {
        self.question.as_ref().filter(|q| q.check().is_ok())
    }

    /// What the envelope answers `question` with, when it carries a valid answer to it: `question` asks valid
    /// questions ([`Envelope::asks`]) of the envelope's sender, sent by its recipient on its thread, and the answer
    /// picks options of each, or is typed where that is taken. Anything else is no answer, and the envelope an
    /// ordinary message. Trust it only once both envelopes passed [`Envelope::check_subject`].
    pub fn answers<'a>(&'a self, question: &'a Envelope) -> Option<Vec<Answered<'a>>> {
        self.answers_as(question, &self.from)
    }

    /// [`Envelope::answers`], taking `answerer` for the one who answers rather than the sender: the human its
    /// `on_behalf_of` names once a reader verified that claim, as an assistant answers for its human.
    pub fn answers_as<'a>(&'a self, question: &'a Envelope, answerer: &UserId) -> Option<Vec<Answered<'a>>> {
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

    /// `from`'s answer `picks`, one for each question `question`, the envelope asking them, asks, on its thread to its
    /// sender. Fails as [`Questions::answer`] does, or when `question` asks none.
    pub fn answering(question: &Envelope, from: UserId, picks: Vec<Pick>) -> Result<Self, Error> {
        let asked =
            question.asks().ok_or_else(|| Error::InvalidAnswer(format!("message {} asks no question", question.id)))?;
        let (answer, text) = asked.answer(question.id, picks)?;
        let answering = Self::on_thread(question.thread_id.clone(), from, question.from.clone(), text);
        Ok(Self { answer: Some(answer), ..answering })
    }
}

#[cfg(test)]
mod tests;
