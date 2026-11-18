//! Typing in a conversation: in a thread, a channel's feed and a DM, a printable key is written in the bar, Enter on an
//! empty bar acting as the view's Enter and `?` showing the help; Up selects the latest message or post, the view's
//! single keys then acting on it until `i`, End or Esc come back to typing.

use chrono::{DateTime, Utc};

use super::{Action, Focus, Key, Model, Page};
use crate::tui::command::Conversation;

impl Model {
    /// Whether a printable key is written in the bar: a thread, a channel's feed or a DM is shown with nothing of it
    /// selected by Up.
    pub fn typing(&self) -> bool {
        let here = self.conversation();
        self.focus == Focus::View && here != Conversation::None && self.selecting.as_ref() != Some(&here)
    }

    /// Take `key` while the bar is not focused in a conversation: written in the bar or selecting while typing, back to
    /// typing while selecting. None when the key is the view's.
    pub(super) fn conversation_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        if self.bar.focused || self.focus != Focus::View || self.conversation() == Conversation::None {
            return None;
        }
        if !self.typing() {
            return match key {
                Key::Char('i') | Key::Esc | Key::End => {
                    self.unselect(key == Key::End);
                    self.bar.focused = key == Key::Char('i');
                    Some(None)
                }
                _ => None,
            };
        }
        let thread = matches!(self.page(), Page::Thread(_));
        match key {
            Key::Char('?') if self.bar.is_empty() => None,
            Key::Char(_) => self.write(key, now),
            Key::Enter | Key::Backspace | Key::Delete if !self.bar.is_empty() => self.write(key, now),
            Key::Up => {
                self.select_latest();
                Some(None)
            }
            // The latest post or message of a channel or a DM is the first listed.
            Key::End if !thread => {
                self.select_latest();
                self.selecting = None;
                Some(None)
            }
            Key::Down | Key::Home | Key::End | Key::PageUp | Key::PageDown => {
                let action = self.view_key(key, now);
                if !thread || matches!(self.page(), Page::Thread(page) if page.selecting()) {
                    self.selecting = Some(self.conversation());
                }
                Some(action)
            }
            _ => None,
        }
    }

    /// Write `key` in the bar, focusing it.
    fn write(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        self.bar.focused = true;
        self.bar_key(key, now)
    }

    /// Up while typing: select the latest message or post.
    fn select_latest(&mut self) {
        self.selecting = Some(self.conversation());
        let data = &self.data;
        match self.pages.last_mut() {
            Some(Page::Thread(page)) => page.select(),
            Some(Page::Channel(page)) => _ = page.key(Key::Home, data),
            Some(Page::Dm(page)) => _ = page.key(Key::Home, data),
            _ => {}
        }
    }

    /// Forget a selection left behind in another view, and write once the bar is focused from one, as >, f or E focus
    /// it: the message stays marked for the command written, and Up comes back to it.
    pub(super) fn settle(&mut self) {
        if self.bar.focused || self.selecting.as_ref().is_some_and(|here| *here != self.conversation()) {
            self.selecting = None;
        }
    }

    /// Back to typing, a thread's view where it is, or following its latest message when `latest`.
    pub fn unselect(&mut self, latest: bool) {
        self.selecting = None;
        if let Some(Page::Thread(page)) = self.pages.last_mut() {
            page.unselect(latest);
        }
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(m.bar.text(), text);
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
        assert!(m.bar.focused && m.bar.is_empty() && m.typing(), "i writes, not written");
        m.key(Key::Esc, at(3));
        for back in [Key::End, Key::Esc] {
            m.key(Key::Up, at(3));
            m.key(back, at(3));
            assert!(m.typing() && !m.bar.focused, "{back:?}");
            assert_eq!(m.page().name(), "thread", "{back:?}");
        }
    }

    #[test]
    fn q_is_written_in_a_conversation_and_a_question_mark_on_an_empty_bar_shows_the_help() {
        let thread = ThreadId::generate();
        let mut m = opened(&thread);
        m.key(Key::Char('?'), at(3));
        assert!(m.help.is_some() && m.bar.is_empty(), "not written");
        m.key(Key::Esc, at(3));
        assert_eq!(m.key(Key::Char('q'), at(3)), None);
        m.key(Key::Char('?'), at(3));
        assert_eq!(m.bar.text(), "q?", "written");
        assert!(m.help.is_none());
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
        assert!(m.typing() && !m.bar.focused);
    }
}
