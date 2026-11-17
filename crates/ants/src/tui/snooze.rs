//! Snoozing a thread from home: `z` on a home item, or on a thread in the work or thread view, asks in the footer
//! for how long, `1h` unless edited, at most [`SNOOZE_MAX`], and Enter keeps the thread off home on all the human's
//! devices until then, or until an item for the human arrives on it ([`ThreadSnoozes`]). `z` on a thread a snooze
//! holds off home wakes it instead, as `/unsnooze` does; `/snooze [DURATION]` snoozes without asking. The same footer
//! asks when to be reminded of a thread ([`super::remind`]).

#[cfg(doc)]
use ants_proto::ThreadSnoozes;
use ants_proto::{SNOOZE_MAX, ThreadId, Uuid};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::acts::Change;
use super::home;
use super::model::{Action, Data, Key, Model};

/// How long a snooze lasts unless edited.
const DEFAULT: &str = "1h";

/// The footer asking how long to snooze a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    thread: ThreadId,
    /// To be reminded of the thread, or of this message on it, rather than to snooze it.
    remind: Option<Option<Uuid>>,
    text: String,
    /// Whether a key changed the text: the first one typed replaces the default.
    edited: bool,
    /// Why the text was refused.
    error: Option<String>,
}

impl Prompt {
    pub fn new(thread: ThreadId) -> Self {
        Self { thread, remind: None, text: DEFAULT.to_owned(), edited: false, error: None }
    }

    /// The footer asking when to be reminded of `thread`, or of `message` on it, and what to note.
    pub fn remind(thread: ThreadId, message: Option<Uuid>) -> Self {
        Self { remind: Some(message), ..Self::new(thread) }
    }

    /// The footer: the duration typed so far, and the keys.
    pub fn line(&self, data: &Data) -> Line<'static> {
        let title = crate::excerpt(&data.title(&self.thread), 40);
        let tail = match (&self.error, self.remind) {
            (Some(error), _) => format!(" · {error}"),
            (None, None) => " · Enter snooze · Esc keep it on home".to_owned(),
            (None, Some(_)) => " · Enter set · Esc cancel".to_owned(),
        };
        let text = match self.remind {
            None => format!(" snooze {title} for (at most 7d): {}▏{tail}", self.text),
            Some(None) => format!(" remind of {title} in (1h, HH:MM, then a note): {}▏{tail}", self.text),
            Some(Some(_)) => {
                format!(" remind of a message on {title} in (1h, HH:MM, then a note): {}▏{tail}", self.text)
            }
        };
        let color = if self.error.is_some() { Color::Red } else { Color::Yellow };
        Line::styled(text, Style::new().fg(color))
    }

    /// Until when the text says to snooze, from `now`.
    fn until(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
        duration(&self.text).map(|d| now + d)
    }
}

/// How long snoozing for `text`, a duration such as `1h`, lasts; `1h` for an empty one.
pub fn duration(text: &str) -> Result<TimeDelta, String> {
    let text = match text.trim() {
        "" => DEFAULT,
        text => text,
    };
    let duration = ants_proto::duration::parse(text).map_err(|e| e.to_string())?;
    let duration = TimeDelta::from_std(duration).map_err(|_| "too long: at most 7d".to_owned())?;
    if duration <= TimeDelta::zero() {
        return Err("a snooze lasts longer than 0".to_owned());
    }
    if duration > SNOOZE_MAX {
        return Err("too long: at most 7d".to_owned());
    }
    Ok(duration)
}

impl Model {
    /// `z` on `thread`: wake it when a snooze holds it off home, else ask how long to snooze it.
    pub fn snooze(&mut self, thread: ThreadId, now: DateTime<Utc>) -> Option<Action> {
        if home::snoozed(&self.data, now).contains_key(&thread) {
            return Some(self.set_snooze(thread, None, now));
        }
        self.snoozing = Some(Prompt::new(thread));
        None
    }

    /// Snooze `thread` until `until`, or wake it for none: here at once, not once the snooze is read back, and on
    /// all the human's devices through the action.
    pub fn set_snooze(&mut self, thread: ThreadId, until: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Action {
        match until {
            Some(until) => self.data.cards.snoozed.snooze(&thread, now, until),
            None => _ = self.data.cards.snoozed.wake(&thread, now),
        }
        let last = self.data.live.summary(&thread).map_or(0, |s| s.last_reopening);
        Action::Change(thread, Change::Snooze(until), last)
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
            Key::Enter if let Some(message) = prompt.remind => {
                let (thread, text) = (prompt.thread.clone(), prompt.text.clone());
                match self.remind_as(thread, message, &text, now) {
                    Ok(action) => {
                        self.snoozing = None;
                        return Some(action);
                    }
                    Err(error) => self.snoozing.as_mut()?.error = Some(error),
                }
            }
            Key::Enter => match prompt.until(now) {
                Ok(until) => {
                    let thread = prompt.thread.clone();
                    self.snoozing = None;
                    return Some(self.set_snooze(thread, Some(until), now));
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
        assert_eq!(m.snooze(thread.clone(), at(0)), None);
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Change(thread.clone(), Change::Snooze(Some(at(3600))), 0)));
        assert!(m.snoozing.is_none());
        assert!(m.data.cards.snoozed.get(&thread, at(1)).is_some(), "off home at once");

        assert_eq!(m.snooze(thread.clone(), at(1)), Some(Action::Change(thread.clone(), Change::Snooze(None), 0)));
        assert!(m.data.cards.snoozed.get(&thread, at(1)).is_none(), "z on a snoozed thread wakes it at once");

        m.snooze(thread, at(0));
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
