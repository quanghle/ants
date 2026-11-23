//! Typing in a conversation: in a thread, a channel's feed and a DM, a printable key is written in the bar, Enter on an
//! empty bar acting as the view's Enter and `?` showing the help; Up selects the latest message or post, the view's
//! single keys then acting on it until `i`, End or Esc come back to typing.

use chrono::{DateTime, Utc};

use super::{Action, Focus, Key, Model, Page};
use crate::tui::command::Conversation;

impl Model {
    /// Whether a printable key is written in the bar: a thread, a channel's feed or a DM is shown with nothing of it
    /// selected by Up, and no run of a trail waits to start.
    pub fn typing(&self) -> bool {
        let here = self.conversation();
        self.focus == Focus::View
            && !self.prompt.as_ref().is_some_and(super::Prompt::covers)
            && here != Conversation::None
            && !matches!(self.page(), Page::Thread(page) if page.listing())
            && self.ui.selecting.as_ref() != Some(&here)
    }

    /// Take `key` while the bar is not focused in a conversation: written in the bar or selecting while typing, back to
    /// typing while selecting. None when the key is the view's.
    pub(super) fn conversation_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        if self.ui.bar.focused || self.focus != Focus::View || self.conversation() == Conversation::None {
            return None;
        }
        if !self.typing() {
            return match key {
                Key::Char('i') | Key::Esc | Key::End => {
                    self.unselect(key == Key::End);
                    self.ui.bar.focused = key == Key::Char('i');
                    Some(None)
                }
                _ => None,
            };
        }
        let thread = matches!(self.page(), Page::Thread(_));
        match key {
            Key::Char('?') if self.ui.bar.is_empty() => None,
            Key::Char(_) => self.write(key, now),
            Key::Enter | Key::Backspace | Key::Delete if !self.ui.bar.is_empty() => self.write(key, now),
            Key::Up => {
                self.select_latest(now);
                Some(None)
            }
            // The latest post or message of a channel or a DM is the first listed.
            Key::End if !thread => {
                self.select_latest(now);
                self.ui.selecting = None;
                Some(None)
            }
            Key::Down | Key::Home | Key::End | Key::PageUp | Key::PageDown => {
                let action = self.view_key(key, now);
                if !thread || matches!(self.page(), Page::Thread(page) if page.selecting()) {
                    self.ui.selecting = Some(self.conversation());
                }
                Some(action)
            }
            _ => None,
        }
    }

    /// Write `key` in the bar, focusing it.
    fn write(&mut self, key: Key, now: DateTime<Utc>) -> Option<Option<Action>> {
        self.ui.bar.focused = true;
        self.bar_key(key, now)
    }

    /// Up while typing: select the latest message or post.
    fn select_latest(&mut self, now: DateTime<Utc>) {
        self.ui.selecting = Some(self.conversation());
        let (data, ui) = (&self.data, &self.ui);
        match self.pages.last_mut() {
            Some(Page::Thread(page)) => page.select(),
            Some(Page::Channel(page)) => _ = page.key(Key::Home, data, ui, now),
            Some(Page::Dm(page)) => _ = page.key(Key::Home, data, now),
            _ => {}
        }
    }

    /// Forget a selection left behind in another view, and write once the bar is focused from one, as >, f or E focus
    /// it: the message stays marked for the command written, and Up comes back to it.
    pub(super) fn settle(&mut self) {
        if self.ui.bar.focused || self.ui.selecting.as_ref().is_some_and(|here| *here != self.conversation()) {
            self.ui.selecting = None;
        }
    }

    /// Back to typing, a thread's view where it is, or following its latest message when `latest`.
    pub fn unselect(&mut self, latest: bool) {
        self.ui.selecting = None;
        if let Some(Page::Thread(page)) = self.pages.last_mut() {
            page.unselect(latest);
        }
    }
}

#[cfg(test)]
mod tests;
