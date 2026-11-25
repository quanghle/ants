//! The escalation and reminder side of the [`Ledger`]: where a thread's escalation stands ([`Pending`]), the reminder
//! of one that reached a human ([`Reminder`]), both kept with the thread's spend, and the deadlines they are due at.

use std::collections::BTreeMap;

use ants_proto::{ChannelId, Envelope, Escalation, ThreadId, UserId};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Ledger, forget_spend, today};

/// Where a thread's escalation stands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pending {
    /// The escalation as it last went out.
    pub escalation: Escalation,
    /// Whom it went to, its leader first; empty when it went to the thread's owner, else the default human.
    #[serde(default)]
    pub to: Vec<UserId>,
    /// The leader's message that gave the thread a fresh budget and allowance at this level, if one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renewed_by: Option<Uuid>,
    /// When it goes one level higher unless its leader answers first, in Unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<i64>,
    /// The thread's owner, a human, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The ant that created the thread with a channel post, if one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    /// The channel thread the thread was spun off, whose creator it has
    /// ([`ants_proto::Envelope::origin_thread`]), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_thread: Option<ThreadId>,
    /// What the escalation is about, for the escalations that follow it up.
    #[serde(default)]
    pub summary: String,
    /// The channel of the thread, when the ant got stuck on a post there: the leader's answer is posted back to it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    /// The ask_human question the escalation is, asked of the human's assistant first: asked of the human when it
    /// goes on ([`ants_proto::EscalationReason::Question`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<Box<ants_proto::Questions>>,
    /// The message the escalation last went out under, if it went out at this level: the stuck ant escalating the
    /// thread again for want of turns or allowance, under another id, sends nothing more until the escalation is
    /// answered, or, out of a daily or pool cap's allowance, until the next UTC day.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent: Option<Sent>,
}

/// A message id an escalation went out under ([`crate::escalation::Stuck::id`]), and on which UTC day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sent {
    pub id: String,
    /// `YYYY-MM-DD`, UTC.
    pub day: String,
}

impl Sent {
    /// Sent under `id` today.
    pub fn now(id: &str) -> Self {
        Self { id: id.to_owned(), day: today() }
    }
}

impl Pending {
    /// `escalation` gone to `to`, about `summary`, for the thread of `notice`, which names its owner and creator
    /// and may ask a question: not renewed, on no channel, and sent under no message id yet.
    pub fn of(
        escalation: Escalation,
        to: Vec<UserId>,
        deadline: Option<i64>,
        notice: &Envelope,
        summary: String,
    ) -> Self {
        Self {
            escalation,
            to,
            renewed_by: None,
            deadline,
            owner: notice.owner.clone(),
            creator: notice.creator.clone(),
            origin_thread: notice.origin_thread.clone(),
            summary,
            channel: None,
            question: notice.question.clone().map(Box::new),
            sent: None,
        }
    }

    /// Whether it went to the thread's owner rather than up a chain.
    pub fn at_owner(&self) -> bool {
        self.to.is_empty()
    }
}

/// When to remind a human of a thread's escalation that reached it, until a human's message on the thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reminder {
    /// The escalation as it last reached the human.
    pub escalation: Escalation,
    /// The human it reached: the thread's owner, else the default human.
    pub to: UserId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_thread: Option<ThreadId>,
    /// What the escalation is about.
    #[serde(default)]
    pub summary: String,
    /// When an escalation of the thread first reached a human, in Unix milliseconds.
    pub since: i64,
    /// When the next reminder is due, in Unix milliseconds.
    pub at: i64,
    /// Reminders sent so far.
    #[serde(default)]
    pub sent: u32,
    /// The hub stream's sequences of the escalation and the reminders as they reached the human, oldest first: what
    /// the human's read marker and the thread's done state are checked against ([`crate::records::heed`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seqs: Vec<u64>,
}

/// What a human's message ended on a thread ([`Ledger::reset`]).
#[derive(Debug, Default, PartialEq)]
pub struct Ended {
    pub escalation: Option<Pending>,
    pub reminder: Option<Reminder>,
    /// The human the escalation reached without a reminder ([`Ledger::reached_human`]).
    pub reached: Option<UserId>,
}

impl Ended {
    /// Whether the thread had an escalation, a reminder or a human its escalation reached.
    pub fn any(&self) -> bool {
        self.escalation.is_some() || self.reminder.is_some() || self.reached.is_some()
    }
}

impl Ledger {
    /// The reminder of the thread's escalation, if it has one.
    pub async fn reminder(&self, thread: &ThreadId) -> Result<Option<Reminder>> {
        Ok(self.thread(thread).await?.reminder)
    }

    /// Remind of the thread's escalation as `reminder` says, keeping when one first reached a human and the reminders
    /// sent from the reminder the thread has, if any.
    pub async fn arm_reminder(&self, thread: &ThreadId, reminder: Reminder) -> Result<()> {
        self.update(thread, |spent| {
            let kept = spent.reminder.as_ref().map(|r| (r.since, r.sent));
            let (since, sent) = kept.unwrap_or((reminder.since, reminder.sent));
            spent.reminder = Some(Reminder { since, sent, ..reminder });
            (true, ())
        })
        .await
    }

    /// The thread's escalation reached `human` without a reminder armed: keep whom it reached, until a human's
    /// message ([`Ledger::reset`]). Sets no deadline.
    pub async fn reached_human(&self, thread: &ThreadId, human: UserId) -> Result<()> {
        self.update(thread, |spent| {
            let changed = spent.reached.as_ref() != Some(&human);
            spent.reached = Some(human);
            (changed, ())
        })
        .await
    }

    /// A reminder of the thread's escalation was sent, stored at hub stream sequence `seq` if known: the next one is
    /// due `next`, or none is for None. A thread whose escalation a human ended meanwhile stays without one.
    pub async fn reminded(&self, thread: &ThreadId, next: Option<i64>, seq: Option<u64>) -> Result<()> {
        self.update(thread, |spent| match next {
            Some(at) => match &mut spent.reminder {
                Some(r) => {
                    (r.sent, r.at) = (r.sent + 1, at);
                    r.seqs.extend(seq);
                    (true, ())
                }
                None => (false, ()),
            },
            None => (spent.reminder.take().is_some(), ()),
        })
        .await
    }

    /// The escalation the thread's reminder is of reached the human at hub stream sequence `seq`.
    pub async fn reached_at(&self, thread: &ThreadId, seq: u64) -> Result<()> {
        self.update(thread, |spent| match &mut spent.reminder {
            Some(r) if !r.seqs.contains(&seq) => {
                r.seqs.push(seq);
                (true, ())
            }
            _ => (false, ()),
        })
        .await
    }

    /// Hold the thread's next reminder back until `at`, without counting one sent.
    pub async fn postpone_reminder(&self, thread: &ThreadId, at: i64) -> Result<()> {
        self.update(thread, |spent| match &mut spent.reminder {
            Some(r) if r.at != at => {
                r.at = at;
                (true, ())
            }
            _ => (false, ()),
        })
        .await
    }

    /// Send no more reminders of the thread's escalation, keeping whom it reached until a human's message, as
    /// [`Ledger::reached_human`] does.
    pub async fn end_reminder(&self, thread: &ThreadId) -> Result<()> {
        self.update(thread, |spent| match spent.reminder.take() {
            Some(r) => {
                spent.reached = Some(r.to);
                (true, ())
            }
            None => (false, ()),
        })
        .await
    }

    /// The thread's escalation, if it has one.
    pub async fn pending(&self, thread: &ThreadId) -> Result<Option<Pending>> {
        Ok(self.thread(thread).await?.escalation)
    }

    /// Keep `pending` as the thread's escalation.
    pub async fn set_pending(&self, thread: &ThreadId, pending: Option<Pending>) -> Result<()> {
        self.update(thread, |spent| {
            let changed = spent.escalation != pending;
            spent.escalation = pending;
            (changed, ())
        })
        .await
    }

    /// A message from `leader` answering the thread's escalation: the first one at the escalation's level gives the
    /// thread a fresh allowance and ends the deadline. Returns whether `by`, that message, did (again, when it is
    /// delivered again).
    pub async fn renew(&self, thread: &ThreadId, leader: &UserId, by: Uuid) -> Result<bool> {
        self.update(thread, |spent| match &mut spent.escalation {
            Some(p) if p.escalation.is_leader(leader) && !p.at_owner() && p.renewed_by.is_none_or(|id| id == by) => {
                let changed = p.renewed_by != Some(by) || p.deadline.is_some();
                (p.renewed_by, p.deadline) = (Some(by), None);
                (forget_spend(spent) || changed, true)
            }
            _ => (false, false),
        })
        .await
    }

    /// A member sent the thread's escalation up to `escalation`, a level above the thread's, with members `to`: it
    /// becomes the thread's, waiting for its leader until `deadline`. Returns whether it did; an escalation a human
    /// ended, or one at or below the thread's level, stays as it is.
    pub async fn climbed(
        &self,
        thread: &ThreadId,
        escalation: &Escalation,
        to: Vec<UserId>,
        deadline: Option<i64>,
    ) -> Result<bool> {
        self.update(thread, |spent| match &mut spent.escalation {
            Some(p) if escalation.level > p.escalation.level => {
                (p.escalation, p.to, p.renewed_by, p.deadline) = (escalation.clone(), to, None, deadline);
                // A member sent this level, not the stuck ant: the ant's next stuck message sends it once.
                p.sent = None;
                (true, true)
            }
            _ => (false, false),
        })
        .await
    }

    /// The escalation deadlines and reminders due by thread, the earlier of the two. One may be stale:
    /// [`Ledger::recheck`] drops it.
    pub async fn deadlines(&self) -> Result<BTreeMap<ThreadId, i64>> {
        let mut index = self.threads.lock().await;
        Ok(self.load_index(&mut index).await?.clone())
    }

    /// Bring the thread's deadline in [`Ledger::deadlines`] in line with its escalation and reminder.
    pub async fn recheck(&self, thread: &ThreadId) -> Result<()> {
        self.update(thread, |_| (false, ())).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ants_proto::thread_spend_key;

    use super::super::tests::{DAY, ant, thread};
    use super::super::{Caps, Store, ThreadSpend};
    use super::*;
    use crate::harness::Spend;

    fn escalated(to: &[&str], deadline: Option<i64>) -> Pending {
        let start = Escalation::new(ant(), thread(1), ants_proto::EscalationReason::TurnBudget);
        let chain: Vec<UserId> = ["lead", "dev-b"].iter().map(|p| p.parse().unwrap()).collect();
        let ants_proto::Hop::Chain { escalation, .. } = start.up(&chain, 3) else { unreachable!() };
        let to = to.iter().map(|p| p.parse().unwrap()).collect();
        let summary = "stuck".to_owned();
        Pending {
            escalation: *escalation,
            to,
            renewed_by: None,
            deadline,
            owner: None,
            creator: None,
            origin_thread: None,
            summary,
            channel: None,
            question: None,
            sent: None,
        }
    }

    #[tokio::test]
    async fn the_leader_renews_once_per_level_and_a_human_ends_the_escalation() {
        let caps = Caps { thread_tokens: Some(100), ..Caps::default() };
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), caps);
        let (t, lead, dev_b) = (thread(1), "lead".parse().unwrap(), "dev-b".parse().unwrap());
        ledger.record_on(&t, &Spend::Tokens(120), DAY).await.unwrap();
        ledger.set_pending(&t, Some(escalated(&["lead", "dev-b"], Some(5)))).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t.clone(), 5)]));

        let (answer, again) = (Uuid::new_v4(), Uuid::new_v4());
        assert!(!ledger.renew(&t, &dev_b, answer).await.unwrap(), "only the leader renews");
        assert!(ledger.renew(&t, &lead, answer).await.unwrap());
        assert_eq!(ledger.reached_on(&t, DAY).await.unwrap(), None, "a fresh allowance");
        assert!(ledger.deadlines().await.unwrap().is_empty(), "the deadline is over");
        assert!(ledger.renew(&t, &lead, answer).await.unwrap(), "a redelivery of the answer still renews");
        assert!(!ledger.renew(&t, &lead, again).await.unwrap(), "once per level");

        // A restart keeps the level and that its renewal was used.
        let ledger = Ledger::with_store(store, ant(), caps);
        let pending = ledger.pending(&t).await.unwrap().unwrap();
        assert_eq!((pending.escalation.level, pending.renewed_by), (1, Some(answer)));
        assert!(ledger.reset(&t).await.unwrap().any(), "a human ends it");
        assert_eq!(ledger.pending(&t).await.unwrap(), None);
        assert!(!ledger.reset(&t).await.unwrap().any());

        ledger.set_pending(&t, Some(escalated(&[], None))).await.unwrap();
        assert!(!ledger.renew(&t, &lead, again).await.unwrap(), "once with the owner, only a human answers");
    }

    #[tokio::test]
    async fn a_stale_deadline_is_dropped_on_a_recheck() {
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), Caps::default());
        let (t1, t2) = (thread(1), thread(2));
        ledger.set_pending(&t1, Some(escalated(&["lead"], Some(7)))).await.unwrap();
        ledger.set_pending(&t2, Some(escalated(&["lead"], Some(9)))).await.unwrap();
        // As a crash between listing the deadline and storing the escalation leaves it.
        store.put(thread_spend_key(&ant(), &t2), &ThreadSpend::default()).await.unwrap();

        let ledger = Ledger::with_store(store, ant(), Caps::default());
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t1.clone(), 7), (t2.clone(), 9)]));
        ledger.recheck(&t2).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t1, 7)]));
    }

    #[tokio::test]
    async fn a_reminder_is_due_before_a_later_deadline_keeps_its_start_and_ends_with_the_escalation() {
        let ledger = Ledger::memory(ant(), Caps::default());
        let t = thread(1);
        let pending = escalated(&["lead"], Some(9));
        let reminder = |since, at| Reminder {
            escalation: pending.escalation.clone(),
            to: ants_test_support::human(),
            owner: None,
            creator: None,
            origin_thread: None,
            summary: String::new(),
            since,
            at,
            sent: 0,
            seqs: Vec::new(),
        };
        ledger.set_pending(&t, Some(pending.clone())).await.unwrap();
        ledger.arm_reminder(&t, reminder(1, 5)).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t.clone(), 5)]), "the earlier one");
        ledger.reminded(&t, Some(20), None).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t.clone(), 9)]));

        // Reaching a human again postpones the reminder, still counting from the first time.
        ledger.arm_reminder(&t, reminder(15, 30)).await.unwrap();
        let armed = ledger.reminder(&t).await.unwrap().unwrap();
        assert_eq!((armed.since, armed.at, armed.sent), (1, 30, 1));

        assert!(ledger.reset(&t).await.unwrap().reminder.is_some(), "a human ends it");
        assert_eq!(ledger.reminder(&t).await.unwrap(), None);
        assert!(ledger.deadlines().await.unwrap().is_empty());
        ledger.reminded(&t, Some(40), None).await.unwrap();
        assert_eq!(ledger.reminder(&t).await.unwrap(), None, "a reminder sent meanwhile does not bring it back");
    }

    #[tokio::test]
    async fn a_reminder_postponed_keeps_its_count_and_one_ended_keeps_whom_it_reached() {
        let ledger = Ledger::memory(ant(), Caps::default());
        let t = thread(1);
        let reminder = Reminder {
            escalation: escalated(&[], None).escalation,
            to: ants_test_support::human(),
            owner: None,
            creator: None,
            origin_thread: None,
            summary: String::new(),
            since: 1,
            at: 5,
            sent: 0,
            seqs: Vec::new(),
        };
        ledger.arm_reminder(&t, reminder).await.unwrap();
        ledger.reached_at(&t, 7).await.unwrap();
        ledger.reminded(&t, Some(10), Some(9)).await.unwrap();
        ledger.postpone_reminder(&t, 50).await.unwrap();
        let armed = ledger.reminder(&t).await.unwrap().unwrap();
        assert_eq!((armed.at, armed.sent, armed.seqs), (50, 1, vec![7, 9]));
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t.clone(), 50)]));

        ledger.end_reminder(&t).await.unwrap();
        assert_eq!(ledger.reminder(&t).await.unwrap(), None);
        assert!(ledger.deadlines().await.unwrap().is_empty());
        assert_eq!(
            ledger.reset(&t).await.unwrap().reached,
            Some(ants_test_support::human()),
            "for telling of another's answer"
        );
    }
}
