//! Tests of replying to a message quoted with `>` and forwarding a message or a thread with `f`.

use super::*;

#[test]
fn gt_in_a_thread_replies_to_the_selected_message_quoting_it_and_mentioning_its_ant() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    let entries = vec![
        message(1, &thread, "alice", "dev-a", "deploy"),
        message(2, &thread, "dev-a", "alice", "deployed\n  to staging"),
        message(3, &thread, "dev-b", "alice", "tests pass"),
    ];
    let ids: Vec<_> = entries.iter().map(|e| e.id.unwrap()).collect();
    m.stored(entries, at(3));
    m.focus = Focus::View;
    m.key(Key::Up, at(3));
    m.key(Key::Up, at(3));
    m.key(Key::Char('>'), at(3));
    assert!(m.ui.bar.focused);
    assert_eq!(m.ui.bar.text(), "@dev-a ", "the bar addresses the author of the message selected");
    typed(&mut m, "why staging?");
    let quote =
        Some(Quoted { id: ids[1], quote: ants_proto::Quote::new(user("dev-a"), "deployed to staging"), sealed: false });
    let job =
        Job::Ask { to: user("dev-a"), thread: Some(thread.clone()), text: "why staging?".into(), quote, team: None };
    let sent = Some(Action::Run(job.clone(), "@dev-a why staging?".into()));
    assert_eq!(m.key(Key::Enter, at(3)), sent, "on the thread");
    assert_eq!(m.ui.bar.quoting, None, "sent once");
    let failed = Outcome {
        result: Err(anyhow::anyhow!("no route")),
        text: "@dev-a why staging?".into(),
        quoting: job.quoting(),
        answered: None,
    };
    m.outcome(failed);
    assert_eq!(m.ui.bar.text(), "@dev-a why staging?");
    assert_eq!(
        m.ui.bar.quoting.as_ref().map(|(t, q)| (t, q.id)),
        Some((&thread, ids[1])),
        "a failed send quotes again"
    );
    m.key(Key::Ctrl('u'), at(3));
    m.key(Key::Esc, at(3));
    assert_eq!(m.ui.bar.quoting, None);
    m.ui.bar.focused = true;

    m.key(Key::Esc, at(3));
    m.key(Key::Up, at(3));
    m.key(Key::Up, at(3));
    m.ui.bar.set_text("@dev-b and the docs");
    m.key(Key::Char('>'), at(3));
    assert_eq!(m.ui.bar.text(), "and the docs", "a human's message gets no mention, and the one typed goes");
    let quote = Some(Quoted { id: ids[0], quote: ants_proto::Quote::new(user("alice"), "deploy"), sealed: false });
    let reply = Job::Reply { thread, to: None, text: "and the docs".into(), quote };
    assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Run(reply, "and the docs".into())));

    m.key(Key::Esc, at(3));
    m.key(Key::Up, at(3));
    m.key(Key::Char('>'), at(3));
    assert!(m.ui.bar.quoting.is_some());
    m.key(Key::Ctrl('u'), at(3));
    m.key(Key::Esc, at(3));
    assert_eq!(m.ui.bar.quoting, None, "leaving the bar empty drops the reply");
}

#[test]
fn f_forwards_the_selected_message_else_the_thread_s_tldr() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    let entries = vec![message(1, &thread, "alice", "dev-a", "deploy"), message(2, &thread, "dev-a", "alice", "done")];
    m.stored(entries.clone(), at(2));
    m.focus = Focus::View;
    m.ui.bar.focused = true;
    typed(&mut m, "/forward @");
    assert_eq!((m.ui.bar.text(), m.ui.bar.focused), ("/forward @".to_owned(), true));
    typed(&mut m, "dev-b");
    assert_eq!(m.key(Key::Enter, at(2)), None, "following the latest, the thread goes, by its TL;DR");
    let refused = &m.ui.bar.status.as_ref().unwrap().text;
    assert_eq!(*refused, format!("no TL;DR of thread {thread} yet: S or /tldr asks for one, then /forward again"));
    let dimmed = Some("no message selected and no TL;DR of the thread yet".to_owned());
    assert_eq!(m.unfit("forward", at(2)), dimmed, "as the palette dims it");
    let tldr = ants_proto::ThreadSummary {
        thread_id: thread.clone(),
        text: "deployed".into(),
        by: user("messenger"),
        asker: user("alice"),
        at: at(2),
        upto: 2,
        tokens: 9,
    };
    m.data.cards.tldr.insert(thread, tldr.clone());
    assert_eq!(m.unfit("forward", at(2)), None);
    let source = forward::Source::Thread(Box::new(tldr));
    let job = Job::Forward { to: user("dev-b"), source, note: String::new(), team: None };
    assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, "/forward @dev-b".into())));

    m.key(Key::Up, at(2));
    m.key(Key::Char('f'), at(2));
    typed(&mut m, "dev-b over to you");
    let source = forward::Source::Message(Box::new(entries[1].clone()));
    let job = Job::Forward { to: user("dev-b"), source, note: "over to you".into(), team: None };
    assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, "/forward @dev-b over to you".into())));
}

#[test]
fn forward_to_a_team_hands_it_to_the_team_s_lead_carrying_the_team() {
    let thread = ThreadId::generate();
    let mut m = model();
    let mut directory = ants_proto::TeamDirectory::default();
    directory.create(ants_proto::Team::new(user("backend"), user("dev-a"), user("alice")), None).unwrap();
    directory.create(ants_proto::Team::unled(user("web"), user("alice")), None).unwrap();
    m.read(Read::Teams(Ok(Some(directory))), at(0));
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    let entries = vec![message(1, &thread, "alice", "dev-b", "deploy"), message(2, &thread, "dev-b", "alice", "done")];
    m.stored(entries.clone(), at(2));
    m.focus = Focus::View;
    m.key(Key::Up, at(2));
    m.key(Key::Char('f'), at(2));
    typed(&mut m, "web take it");
    assert_eq!(m.key(Key::Enter, at(2)), None, "nobody to hand it to");
    let status = m.ui.bar.status.as_ref().unwrap();
    assert!(status.text.starts_with("team web has no lead, nor has any team above it"), "{}", status.text);

    m.ui.bar.set_text("/forward @backend take it");
    let source = forward::Source::Message(Box::new(entries[1].clone()));
    let job = Job::Forward { to: user("dev-a"), source, note: "take it".into(), team: Some(user("backend")) };
    assert_eq!(m.key(Key::Enter, at(2)), Some(Action::Run(job, "/forward @backend take it".into())));
}
