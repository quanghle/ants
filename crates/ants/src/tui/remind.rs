//! Reminders: `b` on a message in the thread view, or on a thread on home or in work, asks in the footer when to be
//! reminded of it and what to note, `1h` unless edited, as `/remind DURATION|TIME [NOTE]` sets without asking. Once
//! due, the reminder is a home item on all the human's devices, and `ants webhook serve` pushes it once
//! ([`Reminders`]); `d` on the item or `/unremind` clears it.

#[cfg(doc)]
use ants_proto::Reminders;
use ants_proto::{Reminder, ThreadId, Uuid};
use chrono::{DateTime, Local, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Utc};

use super::acts::Change;
use super::model::{Action, Model};
use super::snooze;

/// When `text`, `DURATION|TIME [NOTE]`, says to be reminded from `now`, and the note: a duration such as `1h30m`
/// from now, a time of day `HH:MM` (the next one) or a date and time `YYYY-MM-DDTHH:MM`, both in `zone`.
pub fn parse<Z: TimeZone>(text: &str, now: DateTime<Utc>, zone: &Z) -> Result<(DateTime<Utc>, String), String> {
    let text = text.trim();
    let (when, note) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    if when.is_empty() {
        return Err("when to remind: a duration such as 1h, a time HH:MM or YYYY-MM-DDTHH:MM".to_owned());
    }
    let local = now.with_timezone(zone).naive_local();
    let at = if let Ok(time) = NaiveTime::parse_from_str(when, "%H:%M") {
        let today = local.date().and_time(time);
        let day = if today > local { today } else { today + TimeDelta::days(1) };
        utc(zone, day, when)?
    } else if let Ok(at) = NaiveDateTime::parse_from_str(when, "%Y-%m-%dT%H:%M") {
        utc(zone, at, when)?
    } else {
        let duration = ants_proto::duration::parse(when)?;
        now + TimeDelta::from_std(duration).map_err(|_| format!("too long: {when}"))?
    };
    if at <= now {
        return Err(format!("{when} is past: a reminder is due later"));
    }
    Ok((at, note.trim().to_owned()))
}

/// `at` in `zone`, in UTC; the earlier of two in a repeated hour.
fn utc<Z: TimeZone>(zone: &Z, at: NaiveDateTime, when: &str) -> Result<DateTime<Utc>, String> {
    let at = zone.from_local_datetime(&at).earliest().ok_or_else(|| format!("no such local time: {when}"))?;
    Ok(at.with_timezone(&Utc))
}

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
        let (due, note) = parse(text, now, &Local)?;
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
    use crate::live::tests::at;
    use crate::tui::model::Key;
    use crate::tui::model::tests::model;

    #[test]
    fn a_reminder_is_due_after_a_duration_at_the_next_time_of_day_or_at_a_date() {
        let now = at(0);
        assert_eq!(parse("90m check CI", now, &Utc), Ok((now + TimeDelta::minutes(90), "check CI".to_owned())));
        let utc = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        assert_eq!(now, utc("2027-01-15T08:00:00Z"));
        assert_eq!(parse("09:30", now, &Utc).unwrap().0, utc("2027-01-15T09:30:00Z"), "later today");
        assert_eq!(parse("07:00", now, &Utc).unwrap().0, utc("2027-01-16T07:00:00Z"), "tomorrow");
        let east = chrono::FixedOffset::east_opt(2 * 3600).unwrap();
        assert_eq!(parse("09:30", now, &east).unwrap().0, utc("2027-01-16T07:30:00Z"), "in the zone");
        let date = (now + TimeDelta::days(3)).format("%Y-%m-%dT%H:%M").to_string();
        assert_eq!(parse(&date, now, &Utc).unwrap().0, now + TimeDelta::days(3));
        assert!(parse("2000-01-01T00:00", now, &Utc).unwrap_err().contains("past"));
        assert!(parse("", now, &Utc).is_err());
        assert!(parse("soon", now, &Utc).is_err());
    }

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
        assert_eq!(m.data.cards.reminders.0.get(&thread), Some(&reminder), "set at once");
        assert_eq!(m.set_reminder(thread.clone(), None), Action::Change(thread, Change::Remind(None), 0));
        assert!(m.data.cards.reminders.0.is_empty(), "cleared at once");
    }
}
