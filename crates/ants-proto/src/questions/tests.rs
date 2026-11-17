use super::*;
use crate::p;

fn options(options: &[&str]) -> Vec<String> {
    options.iter().map(|&o| o.to_owned()).collect()
}

fn question(free_text: bool) -> Question {
    Question::new("  Ship it? ", &options(&[" yes ", "no"]), free_text).unwrap()
}

/// Three questions: one option or a typed answer, several options, one option only.
fn three() -> Questions {
    let database =
        Question::new("Which database?", &options(&["Postgres", "SQLite"]), true).unwrap().headed("Database");
    let mut regions = Question::new("Which regions?", &options(&["eu", "us", "ap"]), false).unwrap().multi();
    regions.options[0] = QuestionOption::described("eu", " Frankfurt ");
    let ship = Question::new("Ship today?", &options(&["yes", "no"]), false).unwrap();
    Questions::new(vec![database, regions.headed(" Regions "), ship]).unwrap()
}

fn asking(questions: impl Into<Questions>) -> Envelope {
    Envelope::asking("t1".parse().unwrap(), p("dev-a"), p("alice"), questions)
}

#[test]
fn a_question_has_one_to_500_characters_and_two_to_nine_one_line_options() {
    let q = question(true);
    assert_eq!(q.text, "Ship it?");
    assert_eq!(q.options, [QuestionOption::new("yes"), QuestionOption::new("no")]);
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
fn one_to_four_questions_with_short_one_line_headers_and_descriptions() {
    let q = question(true);
    assert_eq!(three().questions[1].header.as_deref(), Some("Regions"), "trimmed");
    assert_eq!(three().questions[1].options[0].description.as_deref(), Some("Frankfurt"));
    assert!(Questions::new(Vec::new()).is_err(), "no question");
    assert!(Questions::new(vec![q.clone(); MAX_QUESTIONS]).is_ok());
    let five = Questions::new(vec![q.clone(); MAX_QUESTIONS + 1]).unwrap_err().to_string();
    assert!(five.contains("5 questions; ask 1-4"), "{five}");
    assert!(Questions::new(vec![q.clone().headed(&"x".repeat(MAX_HEADER))]).is_ok());
    let long = Questions::new(vec![q.clone(), q.clone().headed(&"x".repeat(MAX_HEADER + 1))]).unwrap_err();
    assert!(long.to_string().contains("question 2: header"), "{long}");
    assert_eq!(q.clone().headed("  ").header, None, "an empty header is none");
    let mut described = q.clone();
    described.options[1].description = Some("x".repeat(MAX_DESCRIPTION + 1));
    assert!(Questions::new(vec![described.clone()]).is_err());
    described.options[1].description = Some("wait\nfor it".to_owned());
    assert!(Questions::new(vec![described]).is_err(), "two lines");
    let mut broken = Questions::from(q);
    broken.questions[0].header = Some("a\tb".to_owned());
    assert!(broken.check().is_err(), "a reader takes it as none");
}

#[test]
fn free_text_defaults_to_true_and_multi_select_to_false_on_the_wire() {
    let q: Questions =
        serde_json::from_str(r#"{"questions":[{"text":"q","options":[{"label":"a"},{"label":"b"}]}]}"#).unwrap();
    assert!(q.questions[0].free_text);
    assert!(!q.questions[0].multi_select);
    assert!(q.check().is_ok());
}

#[test]
fn the_envelope_text_repeats_each_question_and_its_numbered_options() {
    assert_eq!(asking(question(false)).text, "Ship it?\n\n1. yes\n2. no\n\nAnswer with an option's number.");
    assert!(Questions::from(question(true)).render().ends_with("or in own words."));
    assert_eq!(
        asking(three()).text,
        "Question 1 of 3, Database: Which database?\n\n1. Postgres\n2. SQLite\n\nAnswer with an option's number or in \
         own words.\n\nQuestion 2 of 3, Regions: Which regions?\n\n1. eu — Frankfurt\n2. us\n3. ap\n\nAnswer with one \
         or more options' numbers.\n\nQuestion 3 of 3: Ship today?\n\n1. yes\n2. no\n\nAnswer with an option's number."
    );
}

#[test]
fn a_pick_is_an_option_number_numbers_joined_by_commas_or_typed_text() {
    assert_eq!(Pick::parse(" 2 "), Pick::option(2));
    assert_eq!(Pick::parse("1, 3"), Pick::Options(vec![1, 3]));
    assert_eq!(Pick::parse("+2"), Pick::Typed("+2".to_owned()));
    assert_eq!(Pick::parse("2 please"), Pick::Typed("2 please".to_owned()));
    assert_eq!(Pick::parse("1,,2"), Pick::Typed("1,,2".to_owned()));
}

#[test]
fn a_command_line_gives_one_question_all_its_words_and_several_one_word_each() {
    let one = Questions::from(question(true));
    let words = options(&["after", "lunch"]);
    assert_eq!(one.picks(&words).unwrap(), [Pick::Typed("after lunch".to_owned())]);
    assert_eq!(three().picks(&options(&["2", "1,3", "1"])).unwrap()[1], Pick::Options(vec![1, 3]));
    let short = three().picks(&words).unwrap_err().to_string();
    assert!(short.contains("2 answers to 3 questions; give one to each"), "{short}");
}

#[test]
fn an_answer_picks_an_option_or_types_where_allowed() {
    let asked = asking(question(false));
    let answer = Envelope::answering(&asked, p("alice"), vec![Pick::option(2)]).unwrap();
    assert_eq!((answer.to.as_str(), answer.text.as_str()), ("dev-a", "no"));
    assert_eq!(answer.thread_id, asked.thread_id);
    let answered = answer.answers(&asked).unwrap();
    assert_eq!((answered[0].picked(), answered[0].words()), ("option 2, no".to_owned(), "no".to_owned()));
    for refused in [Pick::option(3), Pick::option(0), Pick::Options(vec![1, 2]), Pick::Typed("maybe".to_owned())] {
        assert!(Envelope::answering(&asked, p("alice"), vec![refused.clone()]).is_err(), "{refused:?}");
    }
    assert!(Envelope::answering(&asked, p("alice"), Vec::new()).is_err(), "no pick");

    let open = asking(question(true));
    let typed = Envelope::answering(&open, p("alice"), vec![Pick::Typed(" later ".to_owned())]).unwrap();
    assert_eq!(typed.text, "later");
    assert_eq!(typed.answers(&open).unwrap()[0].picked(), "typed: later");
    for refused in [" ", "two\nlines", &"x".repeat(MAX_TYPED + 1)] {
        assert!(Envelope::answering(&open, p("alice"), vec![Pick::Typed(refused.to_owned())]).is_err());
    }
    let plain = Envelope::new(p("dev-a"), p("alice"), "hi");
    assert!(Envelope::answering(&plain, p("alice"), vec![Pick::option(1)]).is_err());
}

#[test]
fn one_answer_picks_for_every_question_several_options_only_where_taken() {
    let asked = asking(three());
    let picks = vec![Pick::Typed("MySQL".to_owned()), Pick::Options(vec![3, 1, 3]), Pick::option(1)];
    let answer = Envelope::answering(&asked, p("alice"), picks).unwrap();
    let carried = answer.answer.as_ref().unwrap();
    assert_eq!(carried.picks[1], Pick::Options(vec![1, 3]), "in order, each once");
    assert_eq!(answer.text, "Database: MySQL\nRegions: eu, ap\nQ3: yes");
    let picked: Vec<String> = answer.answers(&asked).unwrap().iter().map(Answered::picked).collect();
    assert_eq!(picked, ["typed: MySQL", "options 1, eu and 3, ap", "option 1, yes"]);

    let refuse = |picks: Vec<Pick>| Envelope::answering(&asked, p("alice"), picks).unwrap_err().to_string();
    let typed = refuse(vec![Pick::option(1), Pick::Typed("all".to_owned()), Pick::option(1)]);
    assert!(typed.contains("question 2: the question takes only options' numbers, 1-3"), "{typed}");
    let several = refuse(vec![Pick::option(1), Pick::option(1), Pick::Options(vec![1, 2])]);
    assert!(several.contains("question 3: 2 options picked; the question takes one"), "{several}");
    let fewer = refuse(vec![Pick::option(1)]);
    assert!(fewer.contains("1 answers to 3 questions"), "{fewer}");
}

#[test]
fn an_invalid_answer_is_no_answer() {
    let asked = asking(question(false));
    let answer = Envelope::answering(&asked, p("alice"), vec![Pick::option(1)]).unwrap();
    let other = asking(question(false));
    assert_eq!(answer.answers(&other), None, "another question");
    let elsewhere = Envelope { thread_id: "t2".parse().unwrap(), ..answer.clone() };
    assert_eq!(elsewhere.answers(&asked), None, "another thread");
    let by_bob = Envelope { from: p("bob"), ..answer.clone() };
    assert_eq!(by_bob.answers(&asked), None, "not asked of the sender");
    let with = |picks: Vec<Pick>| Envelope { answer: Some(Answer { question: asked.id, picks }), ..answer.clone() };
    assert_eq!(with(vec![Pick::option(3)]).answers(&asked), None, "no such option");
    assert_eq!(with(vec![Pick::Typed("x".to_owned())]).answers(&asked), None, "typed where not allowed");
    assert_eq!(with(vec![Pick::Options(vec![1, 2])]).answers(&asked), None, "two where one is taken");
    assert_eq!(with(Vec::new()).answers(&asked), None, "no pick");
    assert_eq!(with(vec![Pick::option(1), Pick::option(1)]).answers(&asked), None, "a pick too many");
    let mut json = serde_json::to_value(&answer).unwrap();
    json["answer"] = serde_json::json!({ "question": asked.id });
    let shapeless: Envelope = serde_json::from_value(json).unwrap();
    assert_eq!(shapeless.answers(&asked), None, "read, as an ordinary message");
    let mut broken = asked;
    broken.question.as_mut().unwrap().questions[0].options.truncate(1);
    assert_eq!(broken.asks(), None);
}
