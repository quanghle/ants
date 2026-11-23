//! Tests of the slash commands: what each acts on, what it does there, and what it is refused for want of.

use super::*;

#[test]
fn slash_commands_act_on_the_selection_or_open_thread() {
    let thread = ThreadId::generate();
    let mut m = model();
    let run = |m: &mut Model, text: &str| {
        m.follow_drafts();
        m.ui.bar.focused = true;
        m.ui.bar.set_text(text);
        m.key(Key::Enter, at(0))
    };
    assert_eq!(run(&mut m, "/done"), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "no thread open or selected");
    assert_eq!(m.ui.bar.text(), "/done", "a command that fails keeps its text");
    m.select(nav::Item::Work(None));
    m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
    let done = Job::Done { thread: thread.clone(), done: true };
    assert_eq!(run(&mut m, "/done"), Some(Action::Run(done, "/done".into())));
    assert!(!m.ui.bar.focused, "a slash command leaves the bar");
    assert_eq!(run(&mut m, "/cancel"), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, format!("no ant is at work on thread {thread}"));
    let working = StatusEvent::new(user("dev-a"), thread.clone(), StatusUpdate::Thought { text: "hm".into() });
    m.take(live::Event::Status(working), at(2));
    let cancel = Job::Cancel { thread: thread.clone(), ants: vec![user("dev-a")] };
    assert_eq!(run(&mut m, "/cancel"), Some(Action::Run(cancel, "/cancel".into())));
    let grant = Job::Grant { thread: thread.clone(), text: GRANT.into() };
    assert_eq!(run(&mut m, "/grant"), Some(Action::Run(grant, "/grant".into())));
    let snooze = Change::Snooze(Some(at(2 * 3600)));
    assert_eq!(run(&mut m, "/snooze 2h"), Some(Action::Change(thread.clone(), snooze, 1)));
    assert!(m.data.cards.snoozed.get(&thread, at(1)).is_some(), "snoozed at once");
    assert_eq!(run(&mut m, "/unsnooze"), Some(Action::Change(thread.clone(), Change::Snooze(None), 1)));
    assert!(m.data.cards.snoozed.get(&thread, at(1)).is_none(), "woken at once");
    assert_eq!(run(&mut m, "/nope"), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "no command /nope: the palette lists them");
    assert_eq!(run(&mut m, "/quit"), Some(Action::Quit));
}

#[test]
fn answer_answers_the_question_of_the_card_the_keys_answer() {
    let thread = ThreadId::generate();
    let mut m = model();
    let ask = |seq, text: &str| {
        let mut asked = message(seq, &thread, "dev-a", "alice", text);
        let options = ["yes".to_owned(), "no".to_owned()];
        asked.question = Some(ants_proto::Question::new(text, &options, true).unwrap().into());
        asked
    };
    let (first, second) = (ask(1, "Ship it?"), ask(2, "Tag it?"));
    let mut page = thread::Page::new(thread.clone());
    page.history.loaded(&thread, Ok(vec![first.clone()])).unwrap();
    m.pages.push(Page::Thread(page));
    let answer = |m: &mut Model| {
        m.follow_drafts();
        m.ui.bar.focused = true;
        m.ui.bar.set_text("/answer after lunch");
        match m.key(Key::Enter, at(2)) {
            Some(Action::Run(Job::Answer { question, .. }, _)) => question,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(answer(&mut m), first.id.unwrap());
    m.stored(vec![second.clone()], at(2));
    assert_eq!(answer(&mut m), second.id.unwrap(), "the keys go to the latest card shown");
}

#[test]
fn answer_takes_a_pick_for_each_of_several_questions() {
    let thread = ThreadId::generate();
    let mut m = model();
    let mut asked = message(1, &thread, "dev-a", "alice", "two questions");
    let options = ["eu".to_owned(), "us".to_owned(), "ap".to_owned()];
    let regions = ants_proto::Question::new("Which regions?", &options, false).unwrap().multi();
    let ship = ants_proto::Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
    asked.question = Some(Questions::new(vec![regions, ship]).unwrap());
    let mut page = thread::Page::new(thread.clone());
    page.history.loaded(&thread, Ok(vec![asked.clone()])).unwrap();
    m.pages.push(Page::Thread(page));
    let answer = |m: &mut Model, text: &str| {
        m.follow_drafts();
        m.ui.bar.focused = true;
        m.ui.bar.set_text(text);
        m.key(Key::Enter, at(2))
    };
    let picks = vec![ants_proto::Pick::Options(vec![1, 3]), ants_proto::Pick::Typed("after lunch".into())];
    let job = Job::Answer { thread: thread.clone(), question: asked.id.unwrap(), picks };
    let sent = answer(&mut m, "/answer 3,1 'after lunch'");
    assert!(matches!(sent, Some(Action::Run(ref sent, _)) if *sent == job), "{sent:?}");
    assert_eq!(answer(&mut m, "/answer 1"), None);
    let status = &m.ui.bar.status.as_ref().unwrap().text;
    assert!(status.contains("1 answers to 2 questions"), "{status}");
    assert_eq!(answer(&mut m, "/answer 4 no"), None);
    let status = &m.ui.bar.status.as_ref().unwrap().text;
    assert!(status.contains("question 1: no option 4"), "{status}");
}

#[test]
fn approve_answers_the_request_on_the_thread_or_the_one_named() {
    let mut m = model();
    let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
    m.take(live::Event::Approval(tools::Update::Pending(push.clone())), at(0));
    let run = |m: &mut Model, text: &str| {
        m.follow_drafts();
        m.ui.bar.focused = true;
        m.ui.bar.set_text(text);
        m.key(Key::Enter, at(0))
    };
    assert_eq!(
        run(&mut m, "/approve"),
        Some(Action::Decide(Box::new(push.clone()), Verdict::AllowOnce)),
        "home selects it"
    );
    assert_eq!(run(&mut m, "/approve --always"), None);
    assert!(m.ui.bar.status.as_ref().unwrap().text.starts_with("allow-always is not offered"));
    let id = push.id.to_string();
    let named = format!("/reject dev-a/{}", &id[..8]);
    assert_eq!(run(&mut m, &named), Some(Action::Decide(Box::new(push), Verdict::Reject)));
    m.decided(Ok("answered request".into()));
    assert_eq!(m.ui.bar.status, Some(Status { text: "answered request".into(), tone: Tone::Done }));
    assert_eq!(run(&mut m, "/reject 0000"), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "no tool request 0000");
}

#[test]
fn goto_shows_a_thread_a_channel_or_a_user() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
    let ops = ChannelEntry {
        channel: "ops".parse().unwrap(),
        members: vec![user("alice")],
        messenger: None,
        owner: None,
        description: None,
    };
    m.read(Read::Directory(Ok((vec![ops], ants_proto::Humans::default()))), at(1));
    let run = |m: &mut Model, text: &str| {
        m.follow_drafts();
        m.ui.bar.focused = true;
        m.ui.bar.set_text(text);
        m.key(Key::Enter, at(1))
    };
    assert_eq!(run(&mut m, &format!("/goto {}", &thread.as_str()[..10])), Some(Action::Open(thread)));
    assert_eq!(m.page().name(), "thread");
    assert!(matches!(run(&mut m, "/goto #ops"), Some(Action::Reactions(_))));
    assert_eq!(m.nav, nav::Item::Channel("ops".parse().unwrap()));
    assert_eq!(run(&mut m, "/goto #sales"), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "not a member of #sales");
    run(&mut m, "/goto @dev-a");
    assert_eq!(m.nav, nav::Item::Ant(user("dev-a")));
    assert_eq!(run(&mut m, "/goto ffff"), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "no thread ffff read");
}

#[test]
fn tree_shows_the_delegation_tree_of_the_thread_open() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
    m.follow_drafts();
    m.ui.bar.focused = true;
    m.ui.bar.set_text("/tree");
    assert_eq!(m.key(Key::Enter, at(1)), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "no thread open or selected");
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    m.follow_drafts();
    m.ui.bar.set_text("/tree");
    m.key(Key::Enter, at(1));
    assert_eq!(m.page().name(), "tree");
    assert_eq!(m.tree_shown().unwrap().0, thread);
}

#[test]
fn save_saves_the_marked_attachment_of_the_open_thread() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    assert_eq!(m.command(Slash::Save(None), at(0)), Err("no attachment on the selected message".into()));
    let mut sent = message(1, &thread, "dev-a", "alice", "the log");
    let envelope = ants_proto::Envelope::new(user("dev-a"), user("alice"), "x");
    let log = ants_daemon::files::reference("build.log", "text/plain", b"ok").unwrap();
    sent.attachments = crate::messages::attachments::listed(&[log], |id| envelope.attachment_handle(id));
    let Page::Thread(page) = m.pages.last_mut().unwrap() else { panic!() };
    page.history.loaded(&thread, Ok(vec![sent.clone()])).unwrap();
    let Ok(Command::Slash(save)) = parse("/save ~/logs") else { panic!() };
    let saved = m.command(save, at(0)).unwrap();
    let to = std::env::home_dir().map(|home| home.join("logs"));
    assert_eq!(saved, Some(Action::Run(Job::Save(Box::new(sent.attachments[0].clone()), to), String::new())));
}

/// A model showing the thread open with `entries` read.
fn reading(thread: &ThreadId, entries: Vec<crate::threads::thread::Entry>) -> Model {
    let mut m = model();
    m.stored(entries.clone(), at(1));
    let mut page = thread::Page::new(thread.clone());
    page.history.loaded(thread, Ok(entries)).unwrap();
    m.pages.push(Page::Thread(page));
    m
}

/// Views differing in what slash commands need of them: each need but [`Needs::Nothing`] is met in one and not
/// in another.
fn views() -> Vec<(&'static str, Model)> {
    let thread = ThreadId::generate();
    let ask = message(1, &thread, "alice", "dev-a", "deploy");
    let mut selected = model();
    selected.select(nav::Item::Work(None));
    selected.stored(vec![ask.clone()], at(1));
    let mut requested = model();
    let push = request("dev-a", "git push", vec![Verdict::AllowOnce, Verdict::Reject]);
    requested.take(live::Event::Approval(tools::Update::Pending(push)), at(1));
    let mut busy = reading(&thread, vec![ask.clone()]);
    let working = StatusEvent::new(user("dev-a"), thread.clone(), StatusUpdate::Thought { text: "hm".into() });
    busy.take(live::Event::Status(working), at(2));
    let mut asked = message(2, &thread, "dev-a", "alice", "Ship it?");
    let options = ["yes".to_owned(), "no".to_owned()];
    asked.question = Some(ants_proto::Question::new("Ship it?", &options, true).unwrap().into());
    let mut sent = message(2, &thread, "dev-a", "alice", "the log");
    let envelope = ants_proto::Envelope::new(user("dev-a"), user("alice"), "x");
    let log = ants_daemon::files::reference("build.log", "text/plain", b"ok").unwrap();
    sent.attachments = attachments::listed(&[log], |id| envelope.attachment_handle(id));
    let mut dead = message(2, &thread, "alice", "dev-a", "deploy");
    (dead.kind, dead.id) = (crate::threads::thread::Kind::Dead, None);
    vec![
        ("nothing read", model()),
        ("a thread selected", selected),
        ("a thread open", reading(&thread, vec![ask.clone()])),
        ("a tool request selected", requested),
        ("an ant at work on the thread open", busy),
        ("a question on the thread open", reading(&thread, vec![ask.clone(), asked])),
        ("an attachment on the thread open", reading(&thread, vec![ask.clone(), sent])),
        ("a dead letter on the thread open", reading(&thread, vec![ask, dead])),
    ]
}

#[test]
fn every_listed_command_is_refused_for_want_of_what_its_row_says_it_needs_and_of_nothing_else() {
    const NEEDS: [Needs; 8] = [
        Needs::Nothing,
        Needs::Thread,
        Needs::Request,
        Needs::BusyAnt,
        Needs::OpenThread,
        Needs::Question,
        Needs::Attachment,
        Needs::Letter,
    ];
    let now = at(9);
    let told = |m: &Model, needs| m.need(needs, now).err().map(String::from);
    for needs in NEEDS.into_iter().filter(|needs| *needs != Needs::Nothing) {
        let met: Vec<bool> = views().iter().map(|(_, m)| told(m, needs).is_none()).collect();
        assert!(met.contains(&true) && met.contains(&false), "{needs:?} met in a view and not in another: {met:?}");
    }
    let lines = COMMANDS.iter().flat_map(|row| row.args.split(" | ").map(move |form| (row, form)));
    for (row, line) in lines.map(|(row, form)| (row, parse::tests::bare(row.name, form))) {
        for (view, mut m) in views() {
            let unmet = told(&m, row.needs);
            let wants: Vec<String> = NEEDS.into_iter().filter_map(|needs| told(&m, needs)).collect();
            m.ui.bar.focused = true;
            m.ui.bar.set_text(&line);
            m.submit(line.clone(), now);
            let refused = m.ui.bar.status.take().filter(|s| s.tone == Tone::Error).map(|s| s.text);
            match unmet {
                Some(why) => assert_eq!(refused, Some(why), "{line} with {view}: its row needs {:?}", row.needs),
                None => assert!(
                    refused.as_ref().is_none_or(|why| !wants.contains(why)),
                    "{line} with {view}: its row needs {:?}, met there, yet it is refused: {refused:?}",
                    row.needs
                ),
            }
        }
    }
}
