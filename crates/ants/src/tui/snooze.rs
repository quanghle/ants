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
use super::input::Input;
use super::model::{self, Action, Data, Key, Model};
use super::style;

/// How long a snooze lasts unless edited.
const DEFAULT: &str = "1h";

/// The footer asking how long to snooze a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    thread: ThreadId,
    /// To be reminded of the thread, or of this message on it, rather than to snooze it.
    remind: Option<Option<Uuid>>,
    text: Input,
    /// Whether a key changed the text: the first one typed replaces the default.
    edited: bool,
    /// Why the text was refused.
    error: Option<String>,
}

impl Prompt {
    pub fn new(thread: ThreadId) -> Self {
        Self { thread, remind: None, text: Input::from(DEFAULT), edited: false, error: None }
    }

    /// The footer asking when to be reminded of `thread`, or of `message` on it, and what to note.
    pub fn remind(thread: ThreadId, message: Option<Uuid>) -> Self {
        Self { remind: Some(message), ..Self::new(thread) }
    }

    /// The footer: the duration typed so far, and the keys.
    pub fn line(&self, data: &Data) -> Line<'static> {
        let title = crate::text::excerpt(&data.title(&self.thread), 40);
        let tail = match (&self.error, self.remind) {
            (Some(error), _) => format!(" · {error}"),
            (None, None) => " · Enter snooze · Esc keep it on home".to_owned(),
            (None, Some(_)) => " · Enter set · Esc cancel".to_owned(),
        };
        let typed = self.text.caret(60);
        let text = match self.remind {
            None => format!(" snooze {title} for (at most 7d): {typed}{tail}"),
            Some(None) => format!(" remind of {title} in (1h, HH:MM, then a note): {typed}{tail}"),
            Some(Some(_)) => format!(" remind of a message on {title} in (1h, HH:MM, then a note): {typed}{tail}"),
        };
        let color = if self.error.is_some() { style::ERROR } else { Color::Yellow };
        Line::styled(text, Style::new().fg(color))
    }

    /// Until when the text says to snooze, from `now`.
    fn until(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
        duration(&self.text.text()).map(|d| now + d)
    }

    /// Take `key` while the footer asks how long to snooze: what is done, and the prompt if it still asks.
    pub fn key(mut self, key: Key, model: &mut Model, now: DateTime<Utc>) -> (Option<Self>, Option<Action>) {
        match key {
            Key::Esc => return (None, None),
            Key::Enter if let Some(message) = self.remind => {
                match model.remind_as(self.thread.clone(), message, &self.text.text(), now) {
                    Ok(action) => return (None, Some(action)),
                    Err(error) => self.error = Some(error),
                }
            }
            Key::Enter => match self.until(now) {
                Ok(until) => return (None, Some(model.set_snooze(self.thread, Some(until), now))),
                Err(error) => self.error = Some(error),
            },
            key => {
                if !self.edited && matches!(key, Key::Char(_)) {
                    self.text = Input::default();
                }
                if self.text.key(key) {
                    (self.edited, self.error) = (true, None);
                }
            }
        }
        (Some(self), None)
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
        self.prompt = Some(model::Prompt::Snooze(Prompt::new(thread)));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::at;
    use crate::tui::input::{Typed, exercise};
    use crate::tui::test_support::model;

    #[test]
    fn z_asks_how_long_one_hour_unless_edited_and_refuses_over_seven_days() {
        let thread = ThreadId::generate();
        let mut m = model();
        assert_eq!(m.snooze(thread.clone(), at(0)), None);
        assert_eq!(m.key(Key::Enter, at(0)), Some(Action::Change(thread.clone(), Change::Snooze(Some(at(3600))), 0)));
        assert!(m.prompt.is_none());
        assert!(m.data.cards.snoozed.get(&thread, at(1)).is_some(), "off home at once");

        assert_eq!(m.snooze(thread.clone(), at(1)), Some(Action::Change(thread.clone(), Change::Snooze(None), 0)));
        assert!(m.data.cards.snoozed.get(&thread, at(1)).is_none(), "z on a snoozed thread wakes it at once");

        m.snooze(thread, at(0));
        for c in "8d".chars() {
            m.key(Key::Char(c), at(0));
        }
        assert_eq!(m.key(Key::Enter, at(0)), None);
        let line = m.prompt_line().unwrap();
        assert!(line.to_string().contains("too long: at most 7d"), "{line}");
        m.key(Key::Backspace, at(0));
        m.key(Key::Backspace, at(0));
        m.key(Key::Char('0'), at(0));
        assert_eq!(m.key(Key::Enter, at(0)), None, "zero refused");
        m.key(Key::Esc, at(0));
        assert!(m.prompt.is_none(), "Esc keeps it on home");
    }

    #[test]
    fn the_prompt_edits_as_the_bar_does() {
        let thread = ThreadId::generate();
        for remind in [false, true] {
            let mut m = model();
            let asked = if remind { m.remind(thread.clone(), None) } else { m.snooze(thread.clone(), at(0)) };
            assert_eq!(asked, None);
            let want = exercise(|typed| match typed {
                Typed::Key(key) => _ = m.key(key, at(0)),
                Typed::Paste(text) => m.paste(text, at(0)),
            });
            let Some(model::Prompt::Snooze(prompt)) = &m.prompt else { panic!("the prompt") };
            assert_eq!(prompt.text.text(), want, "remind: {remind}");
            assert!(m.prompt_line().unwrap().to_string().contains(": X▏a b hi ·"));
        }
    }
}
