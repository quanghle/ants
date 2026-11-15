//! Snoozing a thread from home: `z` on a home item asks in the footer for how long, `1h` unless edited, at most
//! [`SNOOZE_MAX`], and Enter keeps the thread off home on all the human's devices until then, or until an item for
//! the human arrives on it ([`ThreadSnoozes`]).

#[cfg(doc)]
use ants_proto::ThreadSnoozes;
use ants_proto::{SNOOZE_MAX, ThreadId};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::acts::Change;
use super::model::{Action, Data, Key, Model};

/// How long a snooze lasts unless edited.
const DEFAULT: &str = "1h";

/// The footer asking how long to snooze a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    thread: ThreadId,
    text: String,
    /// Whether a key changed the text: the first one typed replaces the default.
    edited: bool,
    /// Why the text was refused.
    error: Option<String>,
}

impl Prompt {
    pub fn new(thread: ThreadId) -> Self {
        Self { thread, text: DEFAULT.to_owned(), edited: false, error: None }
    }

    /// The footer: the duration typed so far, and the keys.
    pub fn line(&self, data: &Data) -> Line<'static> {
        let title = data.title(&self.thread);
        let tail = match &self.error {
            Some(error) => format!(" · {error}"),
            None => " · Enter snooze · Esc keep it on home".to_owned(),
        };
        let text = format!(" snooze {} for (at most 7d): {}▏{tail}", crate::excerpt(&title, 40), self.text);
        let color = if self.error.is_some() { Color::Red } else { Color::Yellow };
        Line::styled(text, Style::new().fg(color))
    }

    /// Until when the text says to snooze, from `now`.
    fn until(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
        let duration = ants_proto::duration::parse(self.text.trim())?;
        let duration = TimeDelta::from_std(duration).map_err(|_| "too long: at most 7d".to_owned())?;
        if duration <= TimeDelta::zero() {
            return Err("a snooze lasts longer than 0".to_owned());
        }
        if duration > SNOOZE_MAX {
            return Err("too long: at most 7d".to_owned());
        }
        Ok(now + duration)
    }
}

impl Model {
    /// Ask how long to snooze `thread`.
    pub fn snooze(&mut self, thread: ThreadId) {
        self.snoozing = Some(Prompt::new(thread));
    }

    /// Take `key` while the footer asks how long to snooze.
    pub fn snooze_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let prompt = self.snoozing.as_mut()?;
        match key {
            Key::Esc => self.snoozing = None,
            Key::Backspace => {
                prompt.text.pop();
                (prompt.edited, prompt.error) = (true, None);
            }
            Key::Char(c) => {
                if !prompt.edited {
                    prompt.text.clear();
                }
                prompt.text.push(c);
                (prompt.edited, prompt.error) = (true, None);
            }
            Key::Enter => match prompt.until(now) {
                Ok(until) => {
                    let thread = prompt.thread.clone();
                    self.snoozing = None;
                    // Off home at once, not once the snooze is read back.
                    self.data.cards.snoozed.snooze(&thread, now, until);
                    let last = self.data.live.summary(&thread).map_or(0, |s| s.last_reopening);
                    return Some(Action::Change(thread, Change::Snooze(Some(until)), last));
                }
                Err(error) => prompt.error = Some(error),
            },
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::tests::at;
    use crate::tui::model::tests::model;

    #[test]
    fn z_asks_how_long_one_hour_unless_edited_and_refuses_over_seven_days() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.snooze(thread.clone());
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Change(thread.clone(), Change::Snooze(Some(at(3600))), 0)));
        assert!(m.snoozing.is_none());
        assert!(m.data.cards.snoozed.get(&thread, at(1)).is_some(), "off home at once");

        m.snooze(thread);
        for c in "8d".chars() {
            m.key(Key::Char(c), at(0));
        }
        assert_eq!(m.key(Key::Enter, at(0)), None);
        let line = m.snoozing.as_ref().unwrap().line(&m.data);
        assert!(line.to_string().contains("too long: at most 7d"), "{line}");
        m.key(Key::Backspace, at(0));
        m.key(Key::Backspace, at(0));
        m.key(Key::Char('0'), at(0));
        assert_eq!(m.key(Key::Enter, at(0)), None, "zero refused");
        m.key(Key::Esc, at(0));
        assert!(m.snoozing.is_none(), "Esc keeps it on home");
    }
}
