//! Reminders: `b` on a message in the thread view, or on a thread on home or in work, asks in the footer when to be
//! reminded of it and what to note, `1h` unless edited, as `/remind DURATION|TIME [NOTE]` sets without asking. Once
//! due, the reminder is a home item on all the human's devices, and `ants webhook serve` pushes it once
//! ([`Reminders`]); `d` on the item or `/unremind` clears it.

#[cfg(doc)]
use ants_proto::Reminders;
use ants_proto::{Reminder, ThreadId, Uuid};
use chrono::{DateTime, Local, Utc};

use super::acts::Change;
use super::model::{Action, Model};
use super::snooze;

impl Model {
    /// `b` on `thread`, or on `message` on it: ask when to be reminded of it.
    pub fn remind(&mut self, thread: ThreadId, message: Option<Uuid>) -> Option<Action> {
        self.snoozing = Some(snooze::Prompt::remind(thread, message));
        None
    }

    /// Remind of `thread`, or of `message` on it, as `text` (`DURATION|TIME [NOTE]`) says, from `now`.
    pub fn remind_as(
        &mut self,
        thread: ThreadId,
        message: Option<Uuid>,
        text: &str,
        now: DateTime<Utc>,
    ) -> Result<Action, String> {
        let (due, note) = crate::remind::parse(text, now, &Local)?;
        let reminder = Reminder { at: now, due, message, note, pushed: false };
        Ok(self.set_reminder(thread, Some(reminder)))
    }

    /// Remind of `thread` with `reminder`, or clear its reminder for none: here at once, not once the reminders are
    /// read back, and on all the human's devices through the action.
    pub fn set_reminder(&mut self, thread: ThreadId, reminder: Option<Reminder>) -> Action {
        match &reminder {
            Some(reminder) => self.data.cards.reminders.remind(&thread, reminder.clone()),
            None => _ = self.data.cards.reminders.clear(&thread),
        }
        let last = self.data.live.summary(&thread).map_or(0, |s| s.last_reopening);
        Action::Change(thread, Change::Remind(reminder), last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::at;
    use crate::tui::model::Key;
    use crate::tui::test_support::model;

    #[test]
    fn b_asks_when_and_sets_the_reminder_with_its_note_at_once() {
        let (thread, message) = (ThreadId::generate(), Uuid::now_v7());
        let mut m = model();
        assert_eq!(m.remind(thread.clone(), Some(message)), None);
        for c in "2h look again".chars() {
            m.key(Key::Char(c), at(0));
        }
        let line = m.snoozing.as_ref().unwrap().line(&m.data).to_string();
        assert!(line.contains("remind of") && line.contains("2h look again"), "{line}");
        let reminder =
            Reminder { at: at(0), due: at(7200), message: Some(message), note: "look again".into(), pushed: false };
        let set = Action::Change(thread.clone(), Change::Remind(Some(reminder.clone())), 0);
        assert_eq!(m.key(Key::Enter, at(0)), Some(set));
        assert!(m.snoozing.is_none());
        assert_eq!(m.data.cards.reminders.remind.get(&thread), Some(&reminder), "set at once");
        assert_eq!(m.set_reminder(thread.clone(), None), Action::Change(thread, Change::Remind(None), 0));
        assert!(m.data.cards.reminders.remind.is_empty(), "cleared at once");
    }
}
