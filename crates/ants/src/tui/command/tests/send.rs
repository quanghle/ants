//! Tests of sending what the bar holds: the keys focusing the bar and a paste into it, where plain text, an `@user` and `/later` go, and a failed send or a message withdrawn coming back into the bar.

use super::*;

#[test]
fn a_paste_goes_into_the_bar_whole_its_line_breaks_new_lines_rather_than_enter() {
    let mut m = model();
    m.paste("q\r\nnow\tplease\x1b\rdone", at(0));
    assert!(m.ui.bar.focused, "a paste where nothing is typed focuses the bar");
    assert_eq!(m.ui.bar.text(), "q\nnow    please\ndone");
    m.key(Key::Home, at(0));
    m.paste("so\n", at(0));
    assert_eq!(m.ui.bar.text(), "q\nnow    please\nso\ndone", "pasted at the cursor, Home going to its line's start");
    m.key(Key::Esc, at(0));
    m.overlay(crate::tui::model::Overlay::Help(crate::tui::help::Help::default()));
    m.paste("q", at(0));
    assert!(m.help().is_some() && !m.ui.bar.focused, "help takes no paste");
}

#[test]
fn ctrl_k_or_at_or_hash_focus_the_bar_which_then_takes_every_key() {
    let mut m = model();
    assert_eq!(m.key(Key::Ctrl('k'), at(0)), None);
    assert!(m.ui.bar.focused && m.ui.bar.text() == "/", "on the palette");
    m.key(Key::Backspace, at(0));
    assert_eq!(m.key(Key::Char('q'), at(0)), None, "q types");
    assert_eq!(m.key(Key::Esc, at(0)), None);
    assert!(!m.ui.bar.focused && m.ui.bar.text() == "q", "Esc leaves the bar, keeping its text");
    m.ui.bar.set_text("");
    m.key(Key::Char('@'), at(0));
    assert!(m.ui.bar.focused && m.ui.bar.text() == "@");
    typed(&mut m, "dev-a deploy");
    let action = m.key(Key::Enter, at(0));
    let job = Job::Ask { to: user("dev-a"), thread: None, text: "deploy".into(), quote: None, team: None };
    assert_eq!(action, Some(Action::Run(job, "@dev-a deploy".into())));
    assert!(m.ui.bar.focused && m.ui.bar.is_empty(), "the bar stays for the next message");
    assert_eq!(m.ui.bar.status.as_ref().map(|s| s.tone), Some(Tone::Busy));
    m.key(Key::Esc, at(0));
    m.key(Key::Tab, at(0));
    m.key(Key::Char('#'), at(0));
    assert!(!m.ui.bar.focused, "# in the nav is no bar's");
}

#[test]
fn plain_text_replies_on_the_open_thread_and_else_goes_unaddressed() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.ui.bar.focused = true;
    typed(&mut m, "who owns billing?");
    let job = Job::Unaddressed { text: "who owns billing?".into() };
    assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "who owns billing?".into())));
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    typed(&mut m, "go on");
    let reply = Job::Reply { thread: thread.clone(), to: None, text: "go on".into(), quote: None };
    assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(reply, "go on".into())));
    let mut post = message(1, &thread, "dev-a", "ops", "hi");
    (post.kind, post.channel) = (ants_client::entry::Kind::Post, Some("ops".parse().unwrap()));
    m.stored(vec![post], at(1));
    typed(&mut m, "go on");
    let post =
        Job::Post { channel: "ops".parse().unwrap(), thread: Some(thread.clone()), text: "go on".into(), quote: None };
    assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(post, "go on".into())), "a channel thread gets a post");
    typed(&mut m, "@dev-b take a look");
    let text = "@dev-b take a look";
    let post = Job::Post { channel: "ops".parse().unwrap(), thread: Some(thread), text: text.into(), quote: None };
    assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(post, text.into())), "a leading @mention posts there");
    m.pages.push(Page::Channel(crate::tui::channel::Page::new("ops".parse().unwrap())));
    for text in ["status?", "@dev-b take a look"] {
        typed(&mut m, text);
        let post = Job::Post { channel: "ops".parse().unwrap(), thread: None, text: text.into(), quote: None };
        assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(post, text.into())), "the feed's channel");
    }
}

#[test]
fn n_on_a_dm_focuses_the_bar_which_asks_its_person() {
    let mut m = model();
    m.pages.push(Page::Dm(crate::tui::dm::Page::new(user("bob"))));
    m.key(Key::Up, at(0));
    typed(&mut m, "n");
    assert!(m.ui.bar.focused && m.ui.bar.is_empty(), "n on a thread selected writes in the bar");
    typed(&mut m, "n");
    assert_eq!(m.ui.bar.text(), "n", "the focused bar takes the keys of the DM");
    let job = Job::Ask { to: user("bob"), thread: None, text: "n".into(), quote: None, team: None };
    assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "n".into())));
}

#[test]
fn later_replies_on_the_open_thread_waiting_its_turn() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.ui.bar.focused = true;
    typed(&mut m, "/later then the docs");
    assert_eq!(m.key(Key::Enter, at(0)), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "/later replies on an open thread: open one first");
    m.ui.bar.set_text("");
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    typed(&mut m, "/later then the docs");
    let reply = Job::Reply { thread, to: None, text: "then the docs".into(), quote: None };
    let job = Job::Later(Box::new(reply));
    assert!(job.sends());
    assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(job, "/later then the docs".into())));
    assert!(m.ui.bar.focused, "the bar stays for the next message, as after a send");
}

#[test]
fn i_in_a_thread_is_written_and_with_a_message_selected_focuses_the_bar_to_reply_there() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy")], at(1));
    m.focus = Focus::View;
    m.key(Key::Char('i'), at(1));
    assert!(!m.ui.bar.focused, "home has no conversation to write in");
    m.pages.push(Page::Thread(thread::Page::new(thread)));
    m.key(Key::Char('i'), at(1));
    assert_eq!((m.ui.bar.text(), m.ui.bar.focused), ("i".to_owned(), true), "written");
    m.key(Key::Ctrl('u'), at(1));
    m.key(Key::Esc, at(1));
    m.key(Key::Up, at(1));
    assert!(!m.typing());
    m.key(Key::Char('i'), at(1));
    assert!(m.ui.bar.focused && m.typing());
    assert_eq!(m.ui.bar.text(), "", "i is not written then");
}

#[test]
fn a_failed_send_comes_back_into_the_bar_and_a_new_thread_opens() {
    let mut m = model();
    let failed = Outcome {
        result: Err(anyhow::anyhow!("no route")),
        text: "@dev-a deploy".into(),
        quoting: None,
        answered: None,
    };
    assert_eq!(m.outcome(failed), None);
    assert_eq!((m.ui.bar.text(), m.ui.bar.focused), ("@dev-a deploy".into(), true));
    assert_eq!(m.ui.bar.status, Some(Status { text: "no route".into(), tone: Tone::Error }));
    let thread = ThreadId::generate();
    let warnings = vec!["dev-a is served by no daemon".to_owned()];
    let done = Done { line: "sent to dev-a".into(), warnings, open: Some(thread.clone()), ..Done::default() };
    let sent = Outcome { result: Ok(done), text: "@dev-a deploy".into(), quoting: None, answered: None };
    assert_eq!(m.outcome(sent), Some(Action::Open(thread)));
    assert_eq!(m.page().name(), "thread");
    assert_eq!(m.ui.bar.text(), "@dev-a deploy", "a success leaves the bar's text alone");
    let status = m.ui.bar.status.as_ref().unwrap();
    assert_eq!((status.text.as_str(), status.tone), ("sent to dev-a · dev-a is served by no daemon", Tone::Warn));
}

#[test]
fn a_message_withdrawn_to_edit_comes_back_into_the_bar_with_its_files() {
    let dir = ants_test_support::scratch_dir("bar-withdrawn");
    std::fs::write(dir.join("notes.txt"), b"notes").unwrap();
    let mut m = model();
    let withdrawn = ants_proto::Uuid::new_v4();
    let edit = Some(("deploy the site".to_owned(), vec![dir.join("notes.txt"), dir.join("gone.txt")]));
    let done = Done { line: "withdrew".into(), withdrew: Some(withdrawn), edit, ..Done::default() };
    m.outcome(Outcome { result: Ok(done), text: String::new(), quoting: None, answered: None });
    assert!(m.data.withdrawn.contains(&withdrawn));
    assert!(m.ui.bar.focused && m.ui.bar.text() == "deploy the site");
    assert_eq!(m.ui.bar.staged.names(), ["notes.txt"]);
    let status = m.ui.bar.status.as_ref().unwrap();
    assert!(status.tone == Tone::Warn && status.text.starts_with("withdrew · "), "{}", status.text);

    let thread = ThreadId::generate();
    let entries = vec![message(1, &thread, "alice", "dev-a", "deploy the docs")];
    m.stored(entries.clone(), at(1));
    let mut page = thread::Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    m.pages.push(Page::Thread(page));
    m.key(Key::Esc, at(1));
    assert!(m.ui.bar.is_empty(), "the draft stays with home");
    m.ui.bar.set_text("deploy the docs too");
    m.key(Key::Up, at(1));
    assert_eq!(m.key(Key::Char('E'), at(1)), None, "a draft in the bar is kept");
    assert_eq!(m.ui.bar.text(), "deploy the docs too");
    assert!(m.ui.bar.status.as_ref().unwrap().text.starts_with("the bar holds a draft"));
}
