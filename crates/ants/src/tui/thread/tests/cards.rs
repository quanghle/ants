//! Tests of the thread view's cards: tool requests answered from their cards, the card the keys answer, and question
//! cards, their focus, options, answers and wrapping.

use super::*;

#[test]
fn the_threads_tool_requests_show_as_cards_under_it_and_answer_to_a_and_enter() {
    let thread = ThreadId::generate();
    let mut m = model();
    let entries = vec![message(1, &thread, "alice", "dev-a", "deploy the site")];
    m.stored(entries.clone(), at(1));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let request = |command| {
        let request = crate::test_support::live::request("dev-a", command, vec![Verdict::AllowOnce]);
        ants_proto::ApprovalRequest { thread_id: thread.clone(), ..request }
    };
    let (push, tag) = (request("git push"), request("git tag v1"));
    m.data.live.approvals = vec![push.clone(), tag, crate::test_support::live::request("dev-b", "ls", vec![])];
    assert_eq!(page.key(Key::Char('A'), &m.data, at(2)), Some(To::Ask(push.clone(), Some(Verdict::AllowAlways))));
    assert_eq!(page.key(Key::Enter, &m.data, at(2)), Some(To::Ask(push.clone(), None)));
    assert_eq!(page.key(Key::Char('n'), &m.data, at(2)), Some(To::Note(push.clone())), "a note on the oldest");

    let shown = screen(&page, &m, 100, at(2));
    assert!(shown.contains("deploy the site"), "{shown}");
    assert!(shown.contains("dev-a asks to run a tool") && shown.contains("execute: git push"), "{shown}");
    assert!(
        shown.contains("a allow · r reject · n reject with a note · Enter details"),
        "the oldest takes the keys: {shown}"
    );
    assert!(shown.contains("1 tool request also waits above") && !shown.contains("execute: git tag v1"), "{shown}");
    assert!(!shown.contains("execute: ls"), "{shown}");

    let mut long = request("git push");
    let words: Vec<String> = (0..500).map(|n| format!("w{n:03}")).collect();
    long.tool.input = Some(format!("{}…", &serde_json::json!({ "command": words.join(" ") }).to_string()[..2000]));
    m.data.live.approvals = vec![long.clone()];
    let shown = screen(&page, &m, 100, at(2));
    assert!(shown.contains("w000 w001") && shown.contains("more lines: Enter shows them"), "{shown}");
    assert_eq!(page.key(Key::Char('a'), &m.data, at(2)), Some(To::Ask(long, None)), "too long to answer at once");

    m.data.live.approvals = vec![push.clone()];
    let shown = crate::test_support::render::rendered(100, 3, |f| page.draw(f, f.area(), &m, at(2), true));
    assert!(!shown.contains("a allow"), "{shown}");
    assert_eq!(page.key(Key::Char('a'), &m.data, at(2)), Some(To::Ask(push.clone(), None)), "its keys cut off");

    m.data.live.approvals.clear();
    assert_eq!(page.key(Key::Char('a'), &m.data, at(2)), None, "nothing pending, nothing to answer");
    m.data.settled.add(push, crate::tools::State::Expired, at(2));
    let shown = screen(&page, &m, 100, at(3));
    assert!(shown.contains("expired unanswered: dev-a rejected it"), "{shown}");
}

#[test]
fn the_card_the_keys_answer_shows_whole_the_others_counted_and_the_conversation_gives_it_one_line() {
    let thread = ThreadId::generate();
    let mut m = model();
    let request = |command: &str, secs| {
        let request = crate::test_support::live::request("dev-a", command, vec![Verdict::AllowOnce]);
        ants_proto::ApprovalRequest { thread_id: thread.clone(), requested_at: at(secs), ..request }
    };
    let (push, tag) = (request("git push origin main", 2), request("git tag v1", 3));
    let asked = |seq, r: &ants_proto::ApprovalRequest| {
        let mut parser = crate::threads::thread::Parser::default();
        parser.entry(seq, at(2), &r.subject(), &serde_json::to_vec(r).unwrap()).unwrap()
    };
    let mut question = message(4, &thread, "dev-a", "alice", "Backfill the table?");
    let options = ["yes".to_owned(), "no".to_owned()];
    question.question = Some(Question::new("Backfill the table?", &options, false).unwrap().into());
    let entries = vec![message(1, &thread, "alice", "dev-a", "deploy"), asked(2, &push), asked(3, &tag), question];
    m.stored(entries.clone(), at(4));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    m.pages.push(crate::tui::model::Page::Thread(page));
    m.data.live.approvals = vec![tag, push.clone()];
    for height in [24, 30] {
        let shown = crate::test_support::render::rendered(100, height, |f| crate::tui::frame::draw(f, &m, at(5)));
        assert!(shown.contains("│execute: git push origin main"), "{height}: {shown}");
        assert!(shown.contains("Enter details · ↑ then a r n answer"), "{height}: {shown}");
        assert!(shown.contains("1 tool request and 1 question also wait"), "{height}: {shown}");
        assert!(shown.contains("tool request  execute: git pu… · card below"), "one line: {shown}");
        assert!(shown.contains("execute: git tag v1") && shown.contains("1 yes"), "the others in full: {shown}");
    }
    m.key(Key::Up, at(5));
    let shown = crate::test_support::render::rendered(100, 24, |f| crate::tui::frame::draw(f, &m, at(5)));
    assert!(shown.contains("a allow · r reject"), "a message selected: {shown}");
    assert_eq!(m.key(Key::Char('1'), at(5)), None, "the question's card is not on screen");
    assert!(m.asking.is_none());
    assert_eq!(m.key(Key::Char('a'), at(5)), Some(Action::Decide(Box::new(push), Verdict::AllowOnce)));

    m.data.live.approvals.clear();
    let shown = crate::test_support::render::rendered(100, 20, |f| crate::tui::frame::draw(f, &m, at(5)));
    assert!(shown.contains("│Backfill the table?") && shown.contains("│1-9 answer · Enter choose"), "{shown}");
    m.key(Key::End, at(5));
    let shown = crate::test_support::render::rendered(100, 20, |f| crate::tui::frame::draw(f, &m, at(5)));
    assert!(shown.contains("│Enter choose · ↑ then 1-9 answer"), "writing: {shown}");
    assert!(shown.contains("asks  Backfill the table? · card below"), "{shown}");
}

#[test]
fn enter_focuses_the_question_card_once_no_tool_request_waits_and_esc_leaves_it() {
    let thread = ThreadId::generate();
    let mut m = model();
    let mut asks = message(1, &thread, "dev-a", "alice", "Backfill?");
    let options = ["default every missing row to zero first".to_owned(), "skip".to_owned()];
    asks.question = Some(Question::new("Backfill?", &options, false).unwrap().into());
    let entries = vec![asks.clone()];
    m.stored(entries.clone(), at(1));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    m.pages.push(crate::tui::model::Page::Thread(page));
    let shown = |m: &Model| match m.pages.last() {
        Some(crate::tui::model::Page::Thread(page)) => screen(page, m, 40, at(2)),
        _ => unreachable!(),
    };
    let request = crate::test_support::live::request("dev-a", "git push", vec![Verdict::AllowOnce]);
    let push = ants_proto::ApprovalRequest { thread_id: thread.clone(), ..request };
    m.data.live.approvals = vec![push.clone()];
    let screen = shown(&m);
    assert!(
        screen.contains("Ctrl-L: 1 question") && !screen.contains("1-9 answer"),
        "the request's card shows, the question's folds: {screen}"
    );
    m.key(Key::Up, at(2));
    assert_eq!(m.key(Key::Char('1'), at(2)), None, "no key answers a card not on screen");
    assert!(m.asking.is_none());
    m.key(Key::Enter, at(2));
    assert!(m.asking.as_ref().is_some_and(|a| a.request == push), "the tool request first");
    m.key(Key::Esc, at(2));
    m.data.live.approvals.clear();

    let screen = shown(&m);
    assert!(screen.contains("│  1 default every missing row to zero │"), "one option a line: {screen}");
    assert!(screen.contains("│    first ") && screen.contains("│  2 skip "), "wrapped: {screen}");
    assert!(screen.contains("│1-9 answer · Enter choose"), "{screen}");
    assert_eq!(m.key(Key::Enter, at(2)), None);
    assert!(shown(&m).contains("│› 1 default every missing row to zero"), "{}", shown(&m));
    assert_eq!(m.key(Key::Esc, at(2)), None);
    assert_eq!(m.page().name(), "thread", "Esc leaves the card, not the thread");
    assert!(!shown(&m).contains("› 1"), "{}", shown(&m));

    m.key(Key::Enter, at(2));
    m.key(Key::Down, at(2));
    m.key(Key::Down, at(2));
    let job = Job::Answer { thread: thread.clone(), question: asks.id.unwrap(), picks: vec![Pick::option(2)] };
    assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, String::new())), "no typed row below the last");
}

#[test]
fn a_question_shows_its_options_and_answers() {
    let thread = ThreadId::generate();
    let mut asks = message(1, &thread, "dev-a", "alice", "Backfill strategy?");
    let options = ["default 0".to_owned(), "skip".to_owned()];
    asks.question = Some(Question::new("Backfill strategy?", &options, true).unwrap().into());
    let mut answer = message(2, &thread, "alice", "dev-a", "skip");
    answer.answer = Some(Answer { question: asks.id.unwrap(), picks: vec![Pick::option(2)] });
    let entries = [asks, answer];
    assert_eq!(
        body(&entries[0], &entries),
        "Backfill strategy?\n1 default 0\n2 skip\nor type an answer\nanswered by alice: option 2, skip"
    );

    let mut asks = message(3, &thread, "dev-a", "alice", "two questions");
    let regions = ["eu".to_owned(), "us".to_owned()];
    let regions = Question::new("Which regions?", &regions, false).unwrap().multi().headed("Regions");
    let ship = Question::new("Ship it?", &options, false).unwrap();
    asks.question = Some(ants_proto::Questions::new(vec![regions, ship]).unwrap());
    assert_eq!(
        body(&asks, &[]),
        "Regions: Which regions?\n1 eu\n2 us\npick one or more\n\nQ2: Ship it?\n1 default 0\n2 skip"
    );
}

#[test]
fn a_question_wraps_at_words() {
    let thread = ThreadId::generate();
    let said = "Run a backfill now, even though it locks the orders table for an hour or so?";
    // Asked of bob: a question to the human on screen would show whole on its card instead.
    let mut asks = message(1, &thread, "dev-a", "bob", said);
    asks.question = Some(Question::new(said, &["yes".to_owned(), "no".to_owned()], true).unwrap().into());
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(vec![asks])).unwrap();
    let m = model();
    for width in 60..=110 {
        let shown = screen(&page, &m, width, at(2));
        let conversation = shown.split('└').next().unwrap();
        let words: Vec<&str> = conversation.split(|c: char| c.is_whitespace() || c == '│').collect();
        for word in ["locks", "the", "orders", "table"] {
            assert!(words.contains(&word), "{word:?} cut at {width}:\n{shown}");
        }
    }
}
