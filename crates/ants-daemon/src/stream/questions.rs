//! Questions ants ask humans ([`Envelope::question`]) and the answers to them ([`Envelope::answer`]), read back from
//! the stream: an ant's daemon finds the question an answer names among the messages the ant sent, a human's CLI
//! among those to the human.

use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_proto::{Acting, Answered, Envelope, UserId, Uuid, sent_reader, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};

use crate::channel::CLOCK_SLACK;
use crate::stream::reading::{self, Reading};

/// Most characters of a question an `[ants]` line about its answer repeats.
const QUOTED: usize = 80;

/// The message `id` stored on `filter`, read as `reading` says through `reader`, if its subject is its own
/// ([`Envelope::check_subject`]). Only messages stored from the time in the id, a version 7 UUID as every message
/// id is, less the clocks' skew ([`CLOCK_SLACK`]), are read, up to the one found.
pub async fn find(
    js: &jetstream::Context,
    reader: &str,
    reading: Reading,
    filter: &str,
    id: Uuid,
) -> Result<Option<Envelope>> {
    let Some(made) = made_at(id) else { return Ok(None) };
    if made > SystemTime::now() + CLOCK_SLACK {
        return Ok(None);
    }
    let start = made.checked_sub(CLOCK_SLACK).unwrap_or(SystemTime::UNIX_EPOCH);
    let start = consumer::DeliverPolicy::ByStartTime { start_time: start.into() };
    let mut found = None;
    reading::read_back(js, reader, reading, filter, start, |_, _, subject, payload| {
        match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) if envelope.id == id && envelope.check_subject(subject).is_ok() => {
                found = Some(envelope);
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    })
    .await?;
    Ok(found)
}

fn made_at(id: Uuid) -> Option<SystemTime> {
    let (secs, nanos) = id.get_timestamp()?.to_unix();
    Some(SystemTime::UNIX_EPOCH + Duration::new(secs, nanos))
}

/// The question `ant` asked that `envelope`, a message to it, answers validly ([`Envelope::answers`]), or answers for
/// the human its `on_behalf_of` names, which the caller verified ([`answerer`]), read back through the ant's
/// [`sent_reader`], or as `reading` says; None for a message carrying no answer, or an invalid one.
pub async fn asked(
    js: &jetstream::Context,
    ant: &UserId,
    reading: Reading,
    envelope: &Envelope,
) -> Result<Option<Envelope>> {
    let Some(answer) = &envelope.answer else { return Ok(None) };
    let question = {
        let _one =
            if reading == Reading::Consumer { Some(crate::stream::history::SENT_READS.lock().await) } else { None };
        find(js, &sent_reader(ant), reading, &subject::inbox_from(ant), answer.question)
            .await
            .context("reading back the question answered")?
    };
    Ok(question.filter(|question| envelope.answers_as(question, answerer(envelope)).is_some()))
}

/// Who answers with `envelope`: the human its `on_behalf_of` names, an assistant answering for its human, else its
/// sender. Trust it only once the claim was verified, a forged one cleared.
pub fn answerer(envelope: &Envelope) -> &UserId {
    envelope.on_behalf_of.as_ref().unwrap_or(&envelope.from)
}

/// The `[ants]` line a prompt with `answer`, a valid answer to `question`, starts with: who answers which question,
/// and with what option, or that the answer is typed.
pub fn answer_line(answer: &Envelope, question: &Envelope) -> Option<String> {
    let answered = answer.answers_as(question, answerer(answer))?;
    let asked = question.asks()?;
    let quoted = quote(&asked.text);
    let with = match answered {
        Answered::Option(n, option) => format!("option {n}, {option:?}; the message is that option's text"),
        Answered::Typed => "a typed answer, the message".to_owned(),
    };
    Some(format!(
        "[ants] {} answers the question asked with ask_human as message {} ({quoted:?}) with {with}.",
        by(answer),
        question.id
    ))
}

/// Who answered with `answer`, as the `[ants]` line names it: `alice via pa` for an assistant answering for its human.
fn by(answer: &Envelope) -> String {
    match &answer.on_behalf_of {
        Some(human) => Acting::For(human.clone()).label(answer.from.as_str()),
        None => answer.from.to_string(),
    }
}

/// The first line of `text`, cut to [`QUOTED`] characters.
fn quote(text: &str) -> String {
    let line = text.lines().next().unwrap_or_default().trim();
    if line.chars().count() <= QUOTED && !text.trim().contains('\n') {
        return line.to_owned();
    }
    let cut: String = line.chars().take(QUOTED - 1).collect();
    format!("{}…", cut.trim_end())
}

#[cfg(test)]
mod tests {
    use ants_proto::{Choice, Question};

    use super::*;

    fn user(name: &str) -> UserId {
        name.parse().unwrap()
    }

    fn question(text: &str, free_text: bool) -> Envelope {
        let options = ["yes".to_owned(), "not yet".to_owned()];
        let question = Question::new(text, &options, free_text).unwrap();
        Envelope::asking("t1".parse().unwrap(), user("dev-a"), user("alice"), question)
    }

    #[test]
    fn the_answer_line_names_the_question_and_the_option_or_a_typed_answer() {
        let asked = question("Ship it?", true);
        let picked = Envelope::answering(&asked, user("alice"), Choice::Option(2)).unwrap();
        assert_eq!(
            answer_line(&picked, &asked).unwrap(),
            format!(
                "[ants] alice answers the question asked with ask_human as message {} (\"Ship it?\") with option 2, \
                 \"not yet\"; the message is that option's text.",
                asked.id
            )
        );
        let typed = Envelope::answering(&asked, user("alice"), Choice::Typed("after lunch".to_owned())).unwrap();
        assert!(answer_line(&typed, &asked).unwrap().ends_with("with a typed answer, the message."));
        let plain = Envelope::on_thread(asked.thread_id.clone(), user("alice"), user("dev-a"), "2");
        assert_eq!(answer_line(&plain, &asked), None);
    }

    #[test]
    fn an_assistant_answers_for_the_human_asked_only() {
        let asked = question("Ship it?", true);
        let by_pa = Envelope::answering(&asked, user("pa"), Choice::Option(1)).unwrap();
        assert_eq!(answer_line(&by_pa, &asked), None, "pa was not asked");
        let line = answer_line(&by_pa.clone().on_behalf(Some(user("alice"))), &asked).unwrap();
        assert!(line.starts_with("[ants] alice via pa answers the question"), "{line}");
        assert_eq!(answer_line(&by_pa.on_behalf(Some(user("bob"))), &asked), None, "bob was not asked");
    }

    #[test]
    fn a_long_question_is_cut_to_its_first_line() {
        assert_eq!(quote("Ship it?\nThe build is green."), "Ship it?…");
        let long = "x".repeat(QUOTED + 5);
        assert_eq!(quote(&long).chars().count(), QUOTED);
    }
}
