//! Reminders: `b` on a message in the thread view, or on a thread on home or in work, asks in the footer when to be
//! reminded of it and what to note, `1h` unless edited, as `/remind DURATION|TIME [NOTE]` sets without asking. Once
//! due, the reminder is a home item on all the human's devices, and `ants webhook serve` pushes it once
//! ([`Reminders`]); `d` on the item or `/unremind` clears it. `/reminders` shows every reminder, those not due yet
//! too, in the pager as `ants remind list` prints them ([`listed`]).

#[cfg(doc)]
use ants_proto::Reminders;
use ants_proto::{Reminder, ThreadId, Uuid};
use anyhow::Result;
use chrono::{DateTime, Local, Utc};

use super::acts::Change;
use super::exec::{Ctx, Done};
use super::model::{Action, Model};
use super::snooze;
use crate::threads::remind;

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
        let (due, note) = crate::threads::remind::parse(text, now, &Local)?;
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

/// The human's reminders as `ants remind list` prints them, for the pager.
pub async fn listed(ctx: &Ctx) -> Result<Done> {
    let (reminders, titles) = remind::read(ctx.meta()?).await?;
    if reminders.remind.is_empty() {
        return Ok(Done { line: "no reminders".to_owned(), ..Done::default() });
    }
    let now = Utc::now();
    let due = reminders.remind.values().filter(|r| r.due <= now).count();
    let line = match reminders.remind.len() {
        1 => format!("1 reminder, {due} due: the pager lists it"),
        n => format!("{n} reminders, {due} due: the pager lists them"),
    };
    Ok(Done { line, page: Some(remind::table(&reminders, &titles, now)), ..Done::default() })
}

#[cfg(test)]
mod tests;
