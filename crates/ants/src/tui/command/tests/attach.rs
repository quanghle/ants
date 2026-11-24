//! Tests of attaching files to the next message: a long paste or dropped images, `/attach` and `/detach`, and the files going with `/notes`.

use super::*;

#[test]
fn a_paste_too_long_for_the_bar_or_of_dropped_images_is_attached() {
    let mut m = model();
    let log = "a\tlog line\n".repeat(7000);
    m.paste(&log, at(0));
    assert!(m.ui.bar.focused && m.ui.bar.is_empty());
    assert_eq!(m.ui.bar.staged.names(), ["paste-1.txt"]);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "attached paste-1.txt (75 KiB): sent with the next message");
    let files = m.ui.bar.staged.take("see").unwrap();
    assert_eq!((files[0].bytes.as_slice(), files[0].attachment.mime.as_str()), (log.as_bytes(), "text/plain"));

    let dir = ants_test_support::scratch_dir("pasted");
    let shot = dir.join("shot.png");
    std::fs::write(&shot, b"png").unwrap();
    m.paste(&format!("'{}' ", shot.display()), at(0));
    assert!(m.ui.bar.is_empty());
    assert_eq!(m.ui.bar.staged.names(), ["shot.png"]);
    m.paste(&format!("{} ", shot.display()), at(0));
    assert_eq!(m.ui.bar.staged.names(), ["shot.png", "shot.png"]);
    m.paste(&"x".repeat(9 << 20), at(0));
    let status = m.ui.bar.status.as_ref().unwrap();
    assert!(status.tone == Tone::Error && status.text.contains("an attachment is at most"), "{status:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn attach_stages_files_for_the_next_message_and_a_failed_send_stages_them_again() {
    let dir = ants_test_support::scratch_dir("bar-attach");
    std::fs::write(dir.join("notes.txt"), b"notes").unwrap();
    let mut m = model();
    m.ui.bar.focused = true;
    typed(&mut m, &format!("/attach {}/no", dir.display()));
    assert_eq!(m.completions()[0].word, format!("{}/notes.txt", dir.display()), "paths complete");
    typed(&mut m, "tes.txt");
    assert_eq!(m.key(Key::Enter, at(0)), None);
    assert!(m.ui.bar.focused && m.ui.bar.is_empty(), "the bar stays open to write the message");
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "attached notes.txt (5 B): sent with the next message");
    assert_eq!(m.ui.bar.staged.names(), ["notes.txt"]);
    typed(&mut m, &format!("/attach {}/missing", dir.display()));
    m.key(Key::Enter, at(0));
    assert_eq!(m.ui.bar.status.as_ref().unwrap().tone, Tone::Error);
    m.ui.bar.set_text("");

    typed(&mut m, "@dev-a read this");
    let Some(Action::Run(Job::Attached { job, files }, text)) = m.key(Key::Enter, at(0)) else { panic!() };
    assert_eq!(*job, Job::Ask { to: user("dev-a"), thread: None, text: "read this".into(), quote: None, team: None });
    assert_eq!((files.len(), text.as_str()), (1, "@dev-a read this"));
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "sending with 1 file…");
    assert!(m.ui.bar.staged.names().is_empty(), "the files go with one message");
    m.outcome(Outcome { result: Err(anyhow::anyhow!("no route")), text, quoting: None });
    assert_eq!((m.ui.bar.text(), m.ui.bar.staged.names()), ("@dev-a read this".to_owned(), vec!["notes.txt"]));

    let thread = ThreadId::generate();
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    m.follow_drafts();
    m.ui.bar.set_text("/done");
    assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Run(Job::Done { thread, done: true }, "/done".into())));
    assert_eq!(m.ui.bar.staged.names(), ["notes.txt"], "only a message takes the files");
    m.ui.bar.focused = true;
    typed(&mut m, "/detach");
    m.key(Key::Enter, at(0));
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "dropped the file attached");
    assert!(m.ui.bar.staged.names().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn notes_posts_to_the_notes_of_the_thread_with_the_files_attached() {
    let dir = ants_test_support::scratch_dir("bar-notes");
    std::fs::write(dir.join("plan.md"), b"# plan").unwrap();
    let thread = ThreadId::generate();
    let mut m = model();
    let run = |m: &mut Model, text: &str| {
        m.follow_drafts();
        m.ui.bar.focused = true;
        m.ui.bar.set_text(text);
        m.key(Key::Enter, at(0))
    };
    assert_eq!(run(&mut m, "/notes split the work"), None);
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "no thread open or selected");
    m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
    let notes = Job::Notes { thread: thread.clone(), text: "split the work".into() };
    assert_eq!(run(&mut m, "/notes split the work"), Some(Action::Run(notes, "/notes split the work".into())));
    assert!(!m.ui.bar.focused, "a slash command leaves the bar");
    run(&mut m, &format!("/attach {}/plan.md", dir.display()));
    let Some(Action::Run(Job::Attached { job, files }, _)) = run(&mut m, "/notes the plan") else { panic!() };
    assert_eq!((*job, files.len()), (Job::Notes { thread, text: "the plan".into() }, 1));
    assert_eq!(m.ui.bar.status.as_ref().unwrap().text, "sending with 1 file…");
    let _ = std::fs::remove_dir_all(&dir);
}
