//! Tests of typing in a conversation: text sent on the thread, keys acting on a selected message, `q` and `?` on the
//! bar, and palette letters while writing.

use ants_proto::ThreadId;

use super::*;
use crate::test_support::live::{at, message};
use crate::tui::acts::Change;
use crate::tui::exec::Job;
use crate::tui::test_support::model;
use crate::tui::thread;

fn opened(thread: &ThreadId) -> Model {
    let mut m = model();
    let entries =
        vec![message(1, thread, "alice", "dev-a", "deploy"), message(2, thread, "dev-a", "alice", "deployed")];
    m.stored(entries.clone(), at(2));
    let mut page = thread::Page::new(thread.clone());
    page.history.loaded(thread, Ok(entries)).unwrap();
    m.pages.push(Page::Thread(page));
    m.focus = Focus::View;
    m
}

#[test]
fn what_is_typed_in_a_thread_is_sent_there_and_no_mark_of_the_thread_changes() {
    let thread = ThreadId::generate();
    let mut m = opened(&thread);
    let text = "please also run the linter";
    for c in text.chars() {
        let action = m.key(Key::Char(c), at(3));
        assert!(!matches!(action, Some(Action::Change(..))), "{c}: {action:?}");
    }
    assert_eq!(m.ui.bar.text(), text);
    let Some(Action::Run(Job::Reply { thread: on, text: sent, .. }, _)) = m.key(Key::Enter, at(3)) else {
        panic!("not a reply")
    };
    assert_eq!((on, sent.as_str()), (thread, text));
}

#[test]
fn with_a_message_selected_the_thread_keys_act_on_it_as_before() {
    let thread = ThreadId::generate();
    let mut m = opened(&thread);
    m.key(Key::Up, at(3));
    assert!(!m.typing());
    let change = |m: &mut Model, c| match m.key(Key::Char(c), at(3)) {
        Some(Action::Change(on, change, _)) if on == thread => change,
        other => panic!("{c}: {other:?}"),
    };
    assert_eq!(change(&mut m, 'p'), Change::Pin(true));
    assert_eq!(change(&mut m, 'm'), Change::Mute(true));
    assert_eq!(change(&mut m, 'e'), Change::Archive(true));
    assert!(matches!(change(&mut m, 'd'), Change::Done(true)));
    assert_eq!(m.key(Key::Char('y'), at(3)), Some(Action::Copy("deployed".into())), "the latest, selected by ↑");
    m.key(Key::Up, at(3));
    assert_eq!(m.key(Key::Char('y'), at(3)), Some(Action::Copy("deploy".into())));
    m.key(Key::Char('t'), at(3));
    assert_eq!(m.page().name(), "tree");
    m.key(Key::Esc, at(3));
    assert!(m.typing(), "the selection is left with the view");

    m.key(Key::Up, at(3));
    m.key(Key::Char('i'), at(3));
    assert!(m.ui.bar.focused && m.ui.bar.is_empty() && m.typing(), "i writes, not written");
    m.key(Key::Esc, at(3));
    for back in [Key::End, Key::Esc] {
        m.key(Key::Up, at(3));
        m.key(back, at(3));
        assert!(m.typing() && !m.ui.bar.focused, "{back:?}");
        assert_eq!(m.page().name(), "thread", "{back:?}");
    }
}

#[test]
fn q_is_written_in_a_conversation_and_a_question_mark_on_an_empty_bar_shows_the_help() {
    let thread = ThreadId::generate();
    let mut m = opened(&thread);
    m.key(Key::Char('?'), at(3));
    assert!(m.help().is_some() && m.ui.bar.is_empty(), "not written");
    m.key(Key::Esc, at(3));
    assert_eq!(m.key(Key::Char('q'), at(3)), None);
    m.key(Key::Char('?'), at(3));
    assert_eq!(m.ui.bar.text(), "q?", "written");
    assert!(m.help().is_none());
}

#[test]
fn a_letter_picked_in_the_palette_while_writing_acts_on_the_latest_message() {
    let thread = ThreadId::generate();
    let mut m = opened(&thread);
    m.key(Key::Ctrl('k'), at(3));
    for c in "copy".chars() {
        m.key(Key::Char(c), at(3));
    }
    assert_eq!(m.key(Key::Enter, at(3)), Some(Action::Copy("deployed".into())));
    assert!(m.typing() && !m.ui.bar.focused);
}
