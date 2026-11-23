//! The deadlines of the ant's escalations: one whose leader did not answer in time goes one level higher, and one that
//! reached a human is reminded of, unless the human heeded it.

use std::time::Duration;

use ants_proto::{Envelope, Escalation, ThreadId};
use anyhow::Result;

use super::Escalator;
use super::text::{duration, reason_text, what_passes};
use super::time::{millis, now_ms};
use crate::limits::caps::Reminder;
use crate::records::heed::Heeded;

/// The waits before each reminder after the first, backing off; the reminders stop after the last.
const REMIND_AGAIN: [Duration; 2] = [Duration::from_hours(24), Duration::from_hours(3 * 24)];
/// Pause after a failed look at the deadlines.
const RETRY: Duration = Duration::from_secs(30);
/// Longest a look at what the human did about an escalation may take before its reminder goes out regardless.
const HEED_WAIT: Duration = Duration::from_secs(10);

impl Escalator {
    /// Send every escalation whose leader did not answer by its deadline one level higher, and every reminder due, as
    /// deadlines pass. Runs until dropped.
    pub async fn deadlines(&self) {
        loop {
            let next = match self.ledger.deadlines().await {
                Ok(deadlines) => deadlines.values().min().copied(),
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "reading escalation deadlines");
                    Some(now_ms() + millis(RETRY))
                }
            };
            let sleep = async {
                match next {
                    Some(at) => {
                        tokio::time::sleep(Duration::from_millis(at.saturating_sub(now_ms()).max(0) as u64)).await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = self.deadline_set.notified() => continue,
                () = sleep => {}
            }
            if let Err(e) = self.overdue().await {
                tracing::warn!(error = format!("{e:#}"), "escalating past a deadline");
                tokio::time::sleep(RETRY).await;
            }
        }
    }

    /// Send the reminders due and escalate one level higher every escalation past its deadline, dropping stale
    /// deadlines.
    pub(super) async fn overdue(&self) -> Result<()> {
        let now = now_ms();
        for (thread, at) in self.ledger.deadlines().await? {
            if at > now {
                continue;
            }
            let _busy = self.busy.lock().await;
            if let Some(reminder) = self.ledger.reminder(&thread).await?.filter(|r| r.at <= now) {
                self.remind(&thread, reminder, now).await?;
            }
            let pending = self.ledger.pending(&thread).await?;
            let due =
                pending.filter(|p| p.deadline.is_some_and(|d| d <= now) && p.renewed_by.is_none() && !p.at_owner());
            let Some(pending) = due else {
                self.ledger.recheck(&thread).await?;
                continue;
            };
            let escalation = &pending.escalation;
            let waited = self.chain.after.map_or_else(String::new, |after| format!(" within {}", duration(after)));
            let id = format!("overdue-{}-{thread}-{}", self.ant, escalation.level);
            if let Some(human) = &escalation.assistant_of {
                let text = format!(
                    "[ants] No answer from {}, the assistant of {human}, on thread {thread}{waited}; {} goes on to \
                     {human}.\n\n{}",
                    escalation.leader,
                    what_passes(&pending),
                    pending.summary
                );
                let stuck = self.passing_on(&pending, text, id);
                let (escalated, _) = self.climb(escalation.clone(), &pending.summary, &stuck, true).await?;
                tracing::info!(%thread, %escalated, "passed on past the deadline");
                continue;
            }
            let text = format!(
                "[ants] No answer from {} on thread {thread}{waited}; the escalation goes one level higher.\n\n{}",
                escalation.leader, pending.summary
            );
            let human = self.humans.owner_or_default(pending.owner.as_ref());
            let stuck = self.following(thread.clone(), &pending, human, text, id);
            let (escalated, _) = self.climb(escalation.clone(), &pending.summary, &stuck, true).await?;
            tracing::info!(%thread, %escalated, "escalated past the deadline");
        }
        Ok(())
    }

    /// Remind the human `reminder` names of the thread's escalation, on the thread, carrying it, under a message id
    /// of its own, then schedule the next reminder, backing off, unless this one is the last ([`REMIND_AGAIN`]).
    /// Unless the human heeded it ([`Escalator::heeded`]): one marked done, read or muted ends the reminders, one
    /// snoozed holds the next back until the snooze ends.
    pub(super) async fn remind(&self, thread: &ThreadId, reminder: Reminder, now: i64) -> Result<()> {
        match self.heeded(thread, &reminder).await {
            Heeded::No => {}
            Heeded::Snoozed(until) => {
                tracing::info!(%thread, human = %reminder.to, %until, "holding the reminder back while snoozed");
                return self.ledger.postpone_reminder(thread, until.timestamp_millis().max(now + 1)).await;
            }
            heeded => {
                tracing::info!(%thread, human = %reminder.to, ?heeded, "the human heeded the escalation; no more reminders");
                return self.ledger.end_reminder(thread).await;
            }
        }
        let Reminder { escalation, to, owner, creator, origin_thread, summary, since, sent, .. } = reminder;
        let (ant, stuck) = (&self.ant, &escalation.stuck);
        let waited = chrono::DateTime::from_timestamp_millis(since)
            .map_or_else(String::new, |at| format!(" since {}", at.format("%Y-%m-%d %H:%M UTC")));
        let again = REMIND_AGAIN.get(sent as usize);
        let last = if again.is_none() { " This is the last reminder." } else { "" };
        let text = format!(
            "[ants] Reminder: the escalation of {stuck} on thread {thread} ({}) has waited for a human{waited}. A \
             human's message to {ant} on the thread ends it: `ants ask --thread {thread} @{ant} TEXT`.{last}\n\n\
             {summary}",
            reason_text(escalation.reason),
        );
        let escalation = Escalation { stand_in: self.chain.stand_in.clone(), ..escalation };
        let notice = Envelope::on_thread(thread.clone(), ant.clone(), to, text)
            .with_owner(owner)
            .with_creator(creator)
            .with_origin_thread(origin_thread)
            .with_escalation(Some(escalation));
        let human = notice.to.clone();
        let seq = (self.send)(notice, format!("remind-{ant}-{thread}-{since}-{}", sent + 1)).await?;
        self.ledger.reminded(thread, again.map(|after| now + millis(*after)), seq).await?;
        tracing::info!(%thread, %human, sent = sent + 1, "reminded of the escalation");
        Ok(())
    }

    /// What the human `reminder` is for did about the thread's escalation; [`Heeded::No`] without a way to look, or
    /// when looking fails, so a reminder too many beats one missed.
    async fn heeded(&self, thread: &ThreadId, reminder: &Reminder) -> Heeded {
        let Some(heed) = &self.heed else { return Heeded::No };
        let look = heed(reminder.to.clone(), thread.clone(), reminder.seqs.clone());
        match tokio::time::timeout(HEED_WAIT, look).await {
            Ok(Ok(heeded)) => heeded,
            Ok(Err(e)) => {
                tracing::warn!(%thread, error = format!("{e:#}"), "looking up what the human did; reminding");
                Heeded::No
            }
            Err(_) => {
                tracing::warn!(%thread, "no answer in time looking up what the human did; reminding");
                Heeded::No
            }
        }
    }
}
