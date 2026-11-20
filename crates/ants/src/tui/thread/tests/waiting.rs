use super::*;

fn waiting_model() -> (Model, [ApprovalRequest; 2], [Entry; 2]) {
    let thread = ThreadId::generate();
    let requests = ["git push", "git tag v1"].map(|command| {
        let request = crate::test_support::live::request("dev-a", command, vec![Verdict::AllowOnce]);
        ApprovalRequest { thread_id: thread.clone(), ..request }
    });
    let mut questions = ["Backfill?", "Ship?"].map(|text| message(1, &thread, "dev-a", "alice", text));
    for (n, entry) in questions.iter_mut().enumerate() {
        entry.seq = n as u64 + 1;
        entry.question = Some(Question::new(&entry.text, &["yes".into(), "no".into()], true).unwrap().into());
    }
    let mut m = model();
    m.stored(questions.to_vec(), at(3));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(questions.to_vec())).unwrap();
    m.pages.push(crate::tui::model::Page::Thread(page));
    m.focus = crate::tui::model::Focus::View;
    m.data.live.approvals = requests.to_vec();
    (m, requests, questions)
}

fn waiting_screen(m: &Model, width: u16, height: u16) -> String {
    crate::test_support::render::rendered(width, height, |f| crate::tui::frame::draw(f, m, at(10)))
}

#[test]
fn ctrl_l_lists_every_waiting_card_and_enter_opens_the_selected_request_not_the_oldest() {
    let (mut m, [push, tag], questions) = waiting_model();
    let thread = push.thread_id.clone();
    let mut answered = message(3, &thread, "dev-a", "alice", "already answered");
    answered.question = questions[0].question.clone();
    let mut answer = message(4, &thread, "alice", "dev-a", "yes");
    answer.answer = Some(Answer { question: answered.id.unwrap(), picks: vec![Pick::option(1)] });
    let mut forged = message(5, &thread, "dev-a", "alice", "forged question");
    forged.question = questions[0].question.clone();
    forged.forged = true;
    let mut other_human = message(6, &thread, "dev-a", "bob", "question for bob");
    other_human.question = questions[0].question.clone();
    m.stored(vec![answered, answer, forged, other_human], at(6));
    m.data.live.approvals.push(crate::test_support::live::request("dev-b", "another thread", vec![]));
    m.data.settled.add(ApprovalRequest { id: Uuid::now_v7(), ..push.clone() }, crate::tools::State::Expired, at(6));

    assert_eq!(m.key(Key::Enter, at(10)), None);
    assert_eq!(m.asking.as_ref().unwrap().request, push, "Enter without the list still opens the oldest");
    m.key(Key::Esc, at(10));
    m.key(Key::Ctrl('l'), at(10));
    for (width, height) in [(100, 20), (100, 30), (40, 12)] {
        let shown = waiting_screen(&m, width, height);
        for text in ["Waiting (4)", "git push", "git tag v1", "Backfill?", "Ship?", "Enter open"] {
            assert!(shown.contains(text), "{width}x{height}, {text}: {shown}");
        }
        assert!(!shown.contains("another thread") && !shown.contains("already answered"), "{shown}");
    }
    m.key(Key::Down, at(10));
    m.key(Key::Enter, at(10));
    assert_eq!(m.asking.as_ref().unwrap().request, tag);
    m.key(Key::Esc, at(10));
    let earlier = ApprovalRequest { requested_at: at(-1), id: Uuid::now_v7(), ..push };
    m.data.live.approvals.push(earlier);
    waiting_screen(&m, 100, 20);
    m.key(Key::Enter, at(10));
    assert_eq!(m.asking.as_ref().unwrap().request, tag, "an arrival does not move the selection");
}

#[test]
fn the_list_opens_an_older_question_visibly_even_while_tools_wait_and_keeps_its_answer_pinned() {
    for (width, height) in [(100, 20), (100, 30), (40, 12)] {
        let (mut m, _, [older, newer]) = waiting_model();
        m.key(Key::Ctrl('l'), at(10));
        for _ in 0..2 {
            m.key(Key::Down, at(10));
        }
        m.key(Key::Enter, at(10));
        let shown = waiting_screen(&m, width, height);
        assert!(shown.contains("Backfill?") && shown.contains("› 1 yes"), "{width}x{height}: {shown}");
        assert!(m.asking.is_none());
        let mut latest = newer;
        latest.id = Some(Uuid::now_v7());
        latest.seq = 3;
        m.stored(vec![latest], at(10));
        m.key(Key::Down, at(10));
        let job = Job::Answer { thread: older.thread_id, question: older.id.unwrap(), picks: vec![Pick::option(2)] };
        assert_eq!(m.key(Key::Enter, at(10)), Some(Action::Run(job, String::new())));
    }
}

#[test]
fn every_waiting_row_scrolls_into_a_narrow_list_and_an_empty_list_answers_nothing() {
    for width in [24, 40, 100] {
        let (mut m, [request, _], _) = waiting_model();
        if let crate::tui::model::Page::Thread(page) = m.pages.last_mut().unwrap() {
            page.history.entries.clear();
        }
        m.data.live.approvals = (0..12)
            .map(|n| {
                let mut request = ApprovalRequest { id: Uuid::now_v7(), requested_at: at(n), ..request.clone() };
                request.tool.title = format!("command-{n:02}");
                request
            })
            .collect();
        m.key(Key::Ctrl('l'), at(10));
        for n in 0..12 {
            let shown = waiting_screen(&m, width, 10);
            assert!(shown.contains(&format!("command-{n:02}")), "{width}, row {n}: {shown}");
            m.key(Key::Down, at(10));
        }
        m.key(Key::Enter, at(10));
        assert_eq!(m.asking.as_ref().unwrap().request.tool.title, "command-11");
        m.key(Key::Esc, at(10));
        m.data.live.approvals.clear();
        let shown = waiting_screen(&m, width, 10);
        assert!(shown.contains("Waiting (0)"), "{shown}");
        assert_eq!(m.key(Key::Enter, at(10)), None);
        assert!(m.asking.is_none());
        m.key(Key::Esc, at(10));
        assert!(m.typing());
    }
}

#[test]
fn a_listed_questions_typed_answer_stays_on_that_question_and_refuses_one_answered_elsewhere() {
    for settled in [false, true] {
        let (mut m, _, [older, _]) = waiting_model();
        m.key(Key::Ctrl('l'), at(10));
        for _ in 0..2 {
            m.key(Key::Down, at(10));
        }
        m.key(Key::Enter, at(10));
        for _ in 0..2 {
            m.key(Key::Down, at(10));
        }
        m.key(Key::Enter, at(10));
        assert_eq!(m.bar.text(), "/answer ");
        if settled {
            let mut answer = message(3, &older.thread_id, "alice", "dev-a", "no");
            answer.answer = Some(Answer { question: older.id.unwrap(), picks: vec![Pick::option(2)] });
            m.stored(vec![answer], at(10));
        }
        for c in "after lunch".chars() {
            m.key(Key::Char(c), at(10));
        }
        let result = m.key(Key::Enter, at(10));
        if settled {
            assert_eq!(result, None, "never answer the newer question instead");
            assert_eq!(m.bar.text(), "/answer after lunch", "keep the refused answer");
            assert!(m.bar.status.as_ref().is_some_and(|s| s.tone == crate::tui::command::Tone::Error));
        } else {
            let job = Job::Answer {
                thread: older.thread_id,
                question: older.id.unwrap(),
                picks: vec![Pick::Typed("after lunch".into())],
            };
            assert_eq!(result, Some(Action::Run(job, "/answer after lunch".into())));
        }
    }
}

#[test]
fn a_listed_question_takes_its_typed_answer_instead_of_the_threads_find() {
    let (mut m, _, [older, _]) = waiting_model();
    m.key(Key::Ctrl('f'), at(10));
    m.key(Key::Char('n'), at(10));
    m.key(Key::Ctrl('l'), at(10));
    for _ in 0..2 {
        m.key(Key::Down, at(10));
    }
    m.key(Key::Enter, at(10));
    for _ in 0..2 {
        m.key(Key::Down, at(10));
    }
    for c in "noon".chars() {
        m.key(Key::Char(c), at(10));
    }
    let job =
        Job::Answer { thread: older.thread_id, question: older.id.unwrap(), picks: vec![Pick::Typed("noon".into())] };
    assert_eq!(m.key(Key::Enter, at(10)), Some(Action::Run(job, String::new())));
}

#[test]
fn the_waiting_list_keeps_composition_and_uses_its_own_help_and_palette_keys() {
    let (mut m, _, _) = waiting_model();
    for c in "draft".chars() {
        m.key(Key::Char(c), at(10));
    }
    assert!(m.bar.focused);
    m.key(Key::Ctrl('l'), at(10));
    assert!(!m.bar.focused && !m.typing());
    assert_eq!(m.bar.text(), "draft");
    m.key(Key::Char('a'), at(10));
    m.paste("not an answer", at(10));
    assert_eq!(m.bar.text(), "draft");
    assert!(m.asking.is_none(), "a hidden request is not answered");
    assert_eq!(crate::tui::keymap::views(&m), vec![crate::tui::keymap::View::Pending]);
    m.key(Key::Char('?'), at(10));
    assert!(m.help.is_some());
    m.key(Key::Esc, at(10));
    for close in [Key::Esc, Key::Char('q'), Key::Interrupt, Key::Ctrl('l')] {
        assert_eq!(m.key(close, at(10)), None);
        assert!(m.typing() && m.bar.text() == "draft", "{close:?}");
        m.key(Key::Ctrl('l'), at(10));
    }
    m.key(Key::Esc, at(10));
    m.key(Key::Char('!'), at(10));
    assert_eq!(m.bar.text(), "draft!", "type-to-compose resumes");

    m.bar.set_text("");
    m.bar.focused = false;
    m.key(Key::Ctrl('k'), at(10));
    for c in "waiting".chars() {
        m.key(Key::Char(c), at(10));
    }
    assert!(m.palette(at(10)).iter().any(|e| e.word == "Ctrl-L" && e.why.is_none()));
    m.key(Key::Enter, at(10));
    assert_eq!(crate::tui::keymap::views(&m), vec![crate::tui::keymap::View::Pending]);
    m.key(Key::Ctrl('k'), at(10));
    for c in "close".chars() {
        m.key(Key::Char(c), at(10));
    }
    m.key(Key::Enter, at(10));
    assert!(m.typing(), "the palette's close key acts on the list");
}
