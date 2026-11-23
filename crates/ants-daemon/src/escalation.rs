//! Escalations up the thread's origin chain.
//!
//! What an ant cannot settle on a thread (its turn budget spent, a limit or cap reached, its thread notes out of
//! turns, a dead letter) goes to the thread's origin chain before any human: every member gets it, the first one
//! leads. The origin is the ant that created the thread with a channel post, when another ant did (its creator
//! leads level 0 in the stuck ant's place, [`Escalation::new_from`]), else the stuck ant: level 1 is the origin's
//! `escalate_to`.
//! The members coordinate in the thread's notes; only the leader resolves it, by answering the ant on the
//! thread, or escalates it further. The leader's answer gives the thread a fresh turn budget and allowance once per
//! level ([`Escalator::arrived`]); on a channel thread, the ant's answer to it goes back to the channel with that
//! budget ([`Pending::channel`]). The next escalation goes one level higher, to the leader's own chain, which the
//! colony publishes and the chains bucket keeps once the leader is offline ([`crate::records::chains`]). With no chain, a chain
//! that loops back or past `max_escalation_hops`, it goes to the thread's owner, else the default human, as it did
//! before chains. Only a human's message ends an escalation.
//!
//! Each level that goes up a chain posts to the thread's notes with a fresh turn budget, so the members have turns
//! to coordinate there even when the notes ran out. A member's prompt says who leads ([`group_note`]); the leader's
//! reply goes to the stuck ant, a non-leader's to the thread notes. A member that sends the escalation one level
//! higher also tells the stuck ant, whose daemon takes that level as the thread's ([`Escalator::took_climb`]).
//!
//! Without an answer from the leader within `escalate_after`, the stuck ant sends it one level higher
//! ([`Escalator::deadlines`]). The thread's escalation lives with its spend in `ants_sessions` ([`Pending`]), so
//! restarts keep its level and deadline.
//!
//! What would reach a human goes first to the human's assistant, when one stands for it and can act
//! (`AssistantFirst`): one level up, led by the assistant alone ([`Escalation::to_assistant`]). The assistant settles
//! it by answering the stuck ant on the thread, or acting for its human, or passes it on to its human with the escalate
//! tool, which sends the stuck ant the escalation one level up, led by the human ([`Escalation::passed_on`]); the
//! stuck ant's daemon then sends it to the human ([`Escalator::took_climb`]), as it does without an answer from the
//! assistant within `escalate_after`. An ask_human question goes the same way ([`Escalator::ask_first`]): the human
//! gets it as a question once passed on; should the ant get stuck while the assistant has its question, the stuck
//! escalation takes the question's place, with the question in its text. While the assistant cannot act, it all goes
//! to the human directly; a human's message still ends it at any point.
//!
//! An escalation that reaches a human carries the escalation, so `ants webhook serve` pushes it, and without a
//! human's message on the thread by `escalation_reminder` the same human gets a reminder on the thread, carrying it
//! too, then a day later and three days after that, the last ([`Reminder`]). The reminder is kept with the
//! thread's spend and its due time in the deadlines, so the daemon serving the ant sends it, once, across restarts.
//! No more go out once the human marked the thread done after the latest, read the escalation or a reminder, or muted
//! the thread; while the human snoozed the thread, the next one waits for the snooze to end ([`crate::records::heed`]).

use std::sync::Arc;
use std::time::Duration;

use ants_proto::{
    ChannelId, DEFAULT_MAX_ESCALATION_HOPS, DEFAULT_TURN_BUDGET, Envelope, Escalation, EscalationReason, Hop,
    NotesPost, OwnerBecause, Role, ThreadId, UserId,
};
use anyhow::Result;
use futures::future::BoxFuture;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::bus;
use crate::limits::caps::{Ended, Ledger, Pending, Reminder};
use crate::mcp::Colony;
use crate::monitoring::status;
use crate::people::assistant_first::AssistantFirst;
use crate::people::humans::Registry;
use crate::records::chains::Chains;
use crate::records::heed::{Heed, Heeded};
use crate::records::links;

mod route;
mod step;
mod text;
mod time;

pub use route::{Route, group, members_at, route};
use step::{
    Fresh, Step, chain_of, next_step, passes_on, repeats, settled_question, supersede, superseded, waits_for_turns,
};
use text::{chain_text, duration, join, notes_text, owner_text, reason_text, what_passes, why_text};
pub use text::{fit, group_note};
use time::{millis, now_ms};

/// Default of `escalate_after`: as long as a prompt may run by default.
pub const DEFAULT_ESCALATE_AFTER: Duration = Duration::from_mins(30);
/// Default of `escalation_reminder`: how long an escalation that reached a human waits for a human's message before
/// the first reminder.
pub const DEFAULT_ESCALATION_REMINDER: Duration = Duration::from_hours(4);
/// The waits before each reminder after the first, backing off; the reminders stop after the last.
const REMIND_AGAIN: [Duration; 2] = [Duration::from_hours(24), Duration::from_hours(3 * 24)];
/// Pause after a failed look at the deadlines.
const RETRY: Duration = Duration::from_secs(30);
/// Most of an escalation's text kept for the escalations that follow it up.
const SUMMARY_CHARS: usize = 4000;
/// Longest a look at what the human did about an escalation may take before its reminder goes out regardless.
const HEED_WAIT: Duration = Duration::from_secs(10);

/// Stores an envelope under a message id, resolving once JetStream has it, with the hub stream's sequence of it when
/// the hub stored it rather than the machine's outbox.
pub type Sender = Arc<dyn Fn(Envelope, String) -> BoxFuture<'static, Result<Option<u64>>> + Send + Sync>;

/// Stores through JetStream ([`bus::store_acked`]), cut to fit the server's `max_payload` ([`fit`]), first recording
/// the link to a channel thread the envelope passes on, if any ([`links::pass_on`]), then tells of the escalation or
/// question on the ant's status (`status::escalated`).
pub fn jetstream_sender(js: async_nats::jetstream::Context) -> Sender {
    Arc::new(move |envelope, id| {
        let js = js.clone();
        Box::pin(async move {
            let envelope = fit(envelope, bus::max_payload(&js.client()));
            links::passing_on(&js, &envelope).await;
            let ack = bus::store_acked(&js, envelope.subject(), &envelope, &id).await?;
            status::escalated(&js.client(), &envelope);
            Ok(ack.map(|ack| ack.sequence))
        })
    })
}

/// Stores a post to thread notes under a message id, resolving once JetStream has it.
pub type NotesSender = Arc<dyn Fn(NotesPost, String) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Stores posts to thread notes through JetStream ([`bus::store`]).
pub fn jetstream_notes_sender(js: async_nats::jetstream::Context) -> NotesSender {
    Arc::new(move |post, id| {
        let js = js.clone();
        Box::pin(async move { bus::store(&js, post.subject(), &post, &id).await.map(drop) })
    })
}

/// The ant's escalation chain and its limits, from the config file.
#[derive(Debug, Clone, PartialEq)]
pub struct Chain {
    /// Who gets the ant's escalations, the first one leading; empty sends them to the thread's owner.
    pub escalate_to: Vec<UserId>,
    /// Levels an escalation climbs before it goes to the thread's owner.
    pub max_hops: u32,
    /// How long an escalation waits for its leader before going one level higher; None waits for good.
    pub after: Option<Duration>,
    /// How long an escalation that reached a human waits for a human's message before a reminder; None never reminds.
    pub remind: Option<Duration>,
    /// The human who also gets what reaches a human while that one is away: stamped on the escalations and tool
    /// requests, for that human's notifications to relay.
    pub stand_in: Option<UserId>,
}

impl Default for Chain {
    fn default() -> Self {
        Self {
            escalate_to: Vec::new(),
            max_hops: DEFAULT_MAX_ESCALATION_HOPS,
            after: Some(DEFAULT_ESCALATE_AFTER),
            remind: Some(DEFAULT_ESCALATION_REMINDER),
            stand_in: None,
        }
    }
}

/// What the ant cannot settle on a thread.
#[derive(Debug, Clone)]
pub struct Stuck {
    pub reason: EscalationReason,
    /// The tool request awaiting a decision, for [`EscalationReason::Approval`].
    pub request_id: Option<Uuid>,
    /// What the thread's owner, else the default human, gets when the escalation goes there: a message from the
    /// ant on the stuck thread. Up a chain, every member gets its text after the escalation's.
    pub notice: Envelope,
    /// Message id, the same for every attempt, so a retry stores nothing twice; each chain member's copy adds its name.
    pub id: String,
    /// The escalation the message the ant got stuck on carries, if any.
    pub carried: Option<Escalation>,
    /// The channel of the post the ant got stuck on, if it was one; else that of the thread's escalation.
    pub channel: Option<ChannelId>,
}

impl Stuck {
    /// `notice` about `reason`, under message id `id`, from the ant stuck on `about`, which may carry an
    /// escalation.
    pub fn new(reason: EscalationReason, notice: Envelope, id: String, about: Option<&Envelope>) -> Self {
        let carried = about.and_then(|e| e.escalation.as_deref().cloned());
        Self { reason, request_id: None, notice, id, carried, channel: None }
    }
}

/// Where an escalation went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Escalated {
    /// Up a chain, to every ant of `to`, `leader` first.
    Chain { level: u32, leader: UserId, to: Vec<UserId> },
    /// To this human, the thread's owner or the default one.
    Owner(UserId),
    /// To the assistant of the human it would have reached, before that human.
    Assistant { assistant: UserId, human: UserId },
}

impl std::fmt::Display for Escalated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chain { level, leader, to } => {
                write!(f, "{} at level {level}, led by {leader}", join(to))
            }
            Self::Owner(human) => write!(f, "{human}"),
            Self::Assistant { assistant, human } => write!(f, "{assistant}, the assistant of {human}"),
        }
    }
}

/// Sends an ant's escalations up its chain and keeps where each thread's stands.
pub struct Escalator {
    ant: UserId,
    chain: Chain,
    colony: Colony,
    /// The chains kept past the colony, for a leader that is offline.
    chains: Option<Chains>,
    humans: Registry,
    ledger: Arc<Ledger>,
    send: Sender,
    /// Gives the thread's notes a fresh budget as an escalation goes up a level; None leaves the notes as they are.
    notes: Option<NotesSender>,
    /// Finds the assistant that gets first what would reach a human; None sends it to the human.
    assistants: Option<AssistantFirst>,
    /// Looks up what the human an escalation reached did about it, before each reminder; None reminds regardless.
    heed: Option<Heed>,
    /// Held while an escalation is decided and stored, so two on one thread do not climb from the same level.
    busy: Mutex<()>,
    /// A deadline was set.
    deadline_set: Notify,
}

impl Escalator {
    /// Escalations of `ant` up `chain`, the chains of others read from `colony`, kept in `ledger`.
    pub fn new(ant: UserId, chain: Chain, colony: Colony, humans: Registry, ledger: Arc<Ledger>, send: Sender) -> Self {
        let (busy, deadline_set) = (Mutex::default(), Notify::new());
        Self {
            ant,
            chain,
            colony,
            chains: None,
            humans,
            ledger,
            send,
            notes: None,
            assistants: None,
            heed: None,
            busy,
            deadline_set,
        }
    }

    /// Send what would reach a human to the assistant `assistants` finds for it first.
    #[must_use]
    pub fn with_assistants(self, assistants: AssistantFirst) -> Self {
        Self { assistants: Some(assistants), ..self }
    }

    /// Look up through `heed`, before each reminder, what the human did about the escalation: one marked done, read or
    /// muted sends no more; one snoozed waits for the snooze to end.
    #[must_use]
    pub fn with_heed(self, heed: Heed) -> Self {
        Self { heed: Some(heed), ..self }
    }

    /// Follow the chain of a leader missing from the colony through `chains`.
    #[must_use]
    pub fn with_chains(mut self, chains: Chains) -> Self {
        self.chains = Some(chains);
        self
    }

    /// Post to the thread's notes through `notes` as each escalation goes up a level, with a fresh budget.
    #[must_use]
    pub fn with_notes(self, notes: NotesSender) -> Self {
        Self { notes: Some(notes), ..self }
    }

    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// Escalate `stuck`, the ant's issue on the thread of its notice, one level up and return where it went. The
    /// first escalation of a thread goes to its origin chain: that of the thread's creator the notice names, when
    /// another ant, else the ant's own. One that carries an escalation, met while working
    /// on it for another ant, goes one level above it, to its leader's chain. A thread whose escalation was
    /// answered by its leader goes one level higher too; one still waiting for its leader goes to the same members
    /// again, unless the ant is stuck for want of turns again on another message (`step::repeats`): it waits then,
    /// sending nothing. The escalation is stored before it is sent, so a retry sends the same messages.
    pub async fn escalate(&self, stuck: Stuck) -> Result<Escalated> {
        self.escalate_explained(stuck).await.map(|(escalated, _)| escalated)
    }

    /// [`Escalator::escalate`], also returning why the escalation went to the thread's owner rather than up a chain,
    /// when it went there just now.
    pub async fn escalate_explained(&self, stuck: Stuck) -> Result<(Escalated, Option<String>)> {
        let _busy = self.busy.lock().await;
        let thread = stuck.notice.thread_id.clone();
        let pending = match self.ledger.pending(&thread).await? {
            Some(asked) if superseded(&asked, stuck.reason) => {
                let pending = supersede(asked, &stuck);
                self.ledger.set_pending(&thread, Some(pending.clone())).await?;
                Some(pending)
            }
            pending => pending,
        };
        let channel = stuck.channel.clone().or_else(|| pending.as_ref().and_then(|p| p.channel.clone()));
        let stuck = Stuck { channel, ..stuck };
        let origin = self.humans.ant_creator(stuck.notice.creator.as_ref());
        let fresh = Fresh { ant: &self.ant, origin: origin.as_ref(), thread: &thread, reason: stuck.reason };
        let step = next_step(fresh, stuck.request_id, stuck.carried.as_ref(), pending.as_ref());
        tracing::info!(%thread, reason = stuck.reason.as_str(), ?step, "escalating");
        match step {
            Step::Again(pending) if !pending.at_owner() && !self.assistant_leads(&pending.escalation).await => {
                let summary = stuck.notice.text.clone();
                self.climb(pending.escalation, &summary, &stuck, true).await
            }
            Step::Again(pending) if repeats(&pending, &stuck) => {
                tracing::info!(%thread, reason = stuck.reason.as_str(), "the thread's escalation waits; not sent again");
                let escalated = if pending.at_owner() {
                    Escalated::Owner(stuck.notice.to.clone())
                } else {
                    chain_of(&pending.escalation, pending.to)
                };
                Ok((escalated, None))
            }
            Step::Again(pending) => {
                // Stored before it is sent, as a climb is, so a retry sends it again and a later post does not.
                let pending = Pending { sent_as: Some(stuck.id.clone()), ..*pending };
                self.ledger.set_pending(&thread, Some(pending.clone())).await?;
                if pending.at_owner() {
                    return self.to_owner(&pending.escalation, None, &stuck).await;
                }
                self.to_chain(&pending.escalation, &pending.to, &stuck.notice.text, &stuck).await?;
                Ok((chain_of(&pending.escalation, pending.to), None))
            }
            Step::Climb(from) => {
                let summary = stuck.notice.text.clone();
                self.climb(*from, &summary, &stuck, pending.is_some()).await
            }
        }
    }

    /// A message the ant is about to answer, from a `sender` in that role: a human's ends the thread's escalation and
    /// gives the thread a fresh allowance. One from the leader of the thread's escalation, carrying it, is the leader's
    /// answer: the first one at the escalation's level gives the thread a fresh allowance and `envelope` a fresh turn
    /// budget, and stops the deadline. Returns whether it did; a delivery of the same message again does again.
    pub async fn arrived(&self, envelope: &mut Envelope, sender: Role) -> Result<bool> {
        let thread = envelope.thread_id.clone();
        if sender.is_human() {
            let ended = self.ledger.reset(&thread).await?;
            if ended.any() {
                tracing::info!(%thread, from = %envelope.from, "a human ended the thread's escalation");
                self.tell_answered(envelope, ended).await;
            }
            return Ok(false);
        }
        let Some(carried) = envelope.escalation.as_deref() else { return Ok(false) };
        if carried.thread_id != thread || !carried.is_leader(&envelope.from) {
            return Ok(false);
        }
        let _busy = self.busy.lock().await;
        if carried.stuck == self.ant {
            // The leader of a level a member climbed to, should its notice not have come first.
            self.ledger.climbed(&thread, carried, members_at(carried).to_vec(), None).await?;
        }
        let renewed = self.ledger.renew(&thread, &envelope.from, envelope.id).await?;
        if renewed {
            tracing::info!(%thread, leader = %envelope.from, "the leader answered the escalation; fresh budget");
            envelope.turn_budget = envelope.turn_budget.max(DEFAULT_TURN_BUDGET);
        }
        Ok(renewed)
    }

    /// Tell the human the thread's escalation `ended` had reached, if it reached one, that another human answered it
    /// with `envelope`: the stand-in it went to while that human was away, say. On a thread of its own, as
    /// [`crate::approvals`] tells of a tool request decided by another human; failing to, it only warns.
    async fn tell_answered(&self, envelope: &Envelope, ended: Ended) {
        let reached = match (ended.reminder, ended.reached, ended.escalation) {
            (Some(reminder), _, _) => reminder.to,
            (None, Some(reached), _) => reached,
            (None, None, Some(pending)) if pending.at_owner() => self.humans.owner_or_default(pending.owner.as_ref()),
            _ => return,
        };
        if reached == envelope.from || !self.humans.is_human(&reached) {
            return;
        }
        let (ant, thread, from) = (&self.ant, &envelope.thread_id, &envelope.from);
        let text = format!(
            "[ants] {from} answered the escalation of {ant} on thread {thread} in place of {reached}: {}",
            envelope.text.chars().take(SUMMARY_CHARS).collect::<String>(),
        );
        let notice = Envelope::new(ant.clone(), reached.clone(), text)
            .with_owner(Some(reached))
            .as_notice(ants_proto::NoticeKind::InPlace);
        let to = notice.to.clone();
        match (self.send)(notice, format!("answered-{ant}-{}", envelope.id)).await {
            Ok(_) => tracing::info!(%thread, %to, %from, "told of the answer of another human"),
            Err(e) => tracing::warn!(%thread, %to, error = format!("{e:#}"), "telling of the answer of another human"),
        }
    }

    /// Whether `envelope` tells the ant that a member sent the ant's own escalation one level higher: it
    /// carries that escalation on the stuck thread, from someone other than its leader. A level above the thread's
    /// becomes the thread's, with a fresh deadline, so the next escalation climbs from there. Such a message is for
    /// the daemon, not the harness.
    pub async fn took_climb(&self, envelope: &Envelope) -> Result<bool> {
        let Some(carried) = envelope.escalation.as_deref() else { return Ok(false) };
        let thread = &envelope.thread_id;
        if self.humans.is_human(&carried.leader) && passes_on(&self.ant, envelope, carried) {
            self.passed_on(envelope, carried).await?;
            return Ok(true);
        }
        if carried.thread_id != *thread
            || carried.reason == EscalationReason::Approval
            || carried.stuck != self.ant
            || envelope.from == self.ant
            || carried.is_leader(&envelope.from)
            || self.humans.is_human(&carried.leader)
        {
            return Ok(false);
        }
        let _busy = self.busy.lock().await;
        let deadline = self.chain.after.map(|after| now_ms() + millis(after));
        if self.ledger.climbed(thread, carried, members_at(carried).to_vec(), deadline).await? {
            tracing::info!(%thread, by = %envelope.from, level = carried.level, "a member escalated the thread higher");
            if deadline.is_some() {
                self.deadline_set.notify_one();
            }
        }
        Ok(true)
    }

    /// `envelope` from the assistant leading the thread's escalation passes it on to its human, the leader of
    /// `carried`: the escalation goes to that human, unless it is no longer the assistant's to pass on.
    async fn passed_on(&self, envelope: &Envelope, carried: &Escalation) -> Result<()> {
        let thread = &envelope.thread_id;
        let _busy = self.busy.lock().await;
        let pending = self.ledger.pending(thread).await?.filter(|p| {
            p.escalation.assistant_of.as_ref() == Some(&carried.leader)
                && p.escalation.is_leader(&envelope.from)
                && !p.at_owner()
        });
        let Some(pending) = pending else {
            tracing::info!(%thread, from = %envelope.from, "ignoring a pass-on of an escalation no longer the assistant's");
            return Ok(());
        };
        let (assistant, human) = (&envelope.from, &carried.leader);
        let what = what_passes(&pending);
        let text = format!(
            "[ants] {assistant}, the assistant of {human}, passes {what} of {} on thread {thread} on to {human}: {}\n\n{}",
            self.ant, envelope.text, pending.summary
        );
        let stuck =
            self.passing_on(&pending, text, format!("passed-{}-{thread}-{}", self.ant, pending.escalation.level));
        let (escalated, _) = self.climb(pending.escalation.clone(), &pending.summary, &stuck, true).await?;
        tracing::info!(%thread, %assistant, %escalated, "the assistant passed the escalation on");
        Ok(())
    }

    /// What the thread's escalation `pending`, led by an assistant, sends its human with `text` under message id `id`.
    fn passing_on(&self, pending: &Pending, text: String, id: String) -> Stuck {
        let escalation = &pending.escalation;
        let human =
            escalation.assistant_of.clone().unwrap_or_else(|| self.humans.owner_or_default(pending.owner.as_ref()));
        let mut notice = Envelope::on_thread(escalation.thread_id.clone(), self.ant.clone(), human, text)
            .with_owner(pending.owner.clone())
            .with_creator(pending.creator.clone())
            .with_origin_thread(pending.origin_thread.clone());
        notice.question = pending.question.as_deref().cloned();
        Stuck {
            reason: escalation.reason,
            request_id: escalation.request_id,
            notice,
            id,
            carried: None,
            channel: pending.channel.clone(),
        }
    }

    /// Ask `notice`, an ask_human question to a human, of that human's assistant first, when one stands for it and can
    /// act, and the thread has no escalation going but a question settled: the question is the thread's escalation
    /// then, going on to the human without the assistant's answer within `escalate_after`. Returns the assistant;
    /// None leaves the question for the caller to send to the human.
    pub async fn ask_first(&self, notice: &Envelope) -> Result<Option<UserId>> {
        let Some(assistants) = &self.assistants else { return Ok(None) };
        let (thread, human) = (&notice.thread_id, &notice.to);
        let _busy = self.busy.lock().await;
        if self.ledger.pending(thread).await?.is_some_and(|p| !settled_question(&p)) {
            return Ok(None);
        }
        let Some(assistant) = assistants.of(human, &self.ant).await else { return Ok(None) };
        let escalation = Escalation::new(self.ant.clone(), thread.clone(), EscalationReason::Question)
            .to_assistant(assistant.clone(), human.clone());
        let deadline = self.chain.after.map(|after| now_ms() + millis(after));
        let pending = Pending {
            escalation: escalation.clone(),
            to: vec![assistant.clone()],
            renewed_by: None,
            deadline,
            owner: notice.owner.clone(),
            creator: notice.creator.clone(),
            origin_thread: notice.origin_thread.clone(),
            summary: notice.text.chars().take(SUMMARY_CHARS).collect(),
            channel: None,
            question: notice.question.clone().map(Box::new),
            sent_as: None,
        };
        self.ledger.set_pending(thread, Some(pending)).await?;
        if deadline.is_some() {
            self.deadline_set.notify_one();
        }
        let text = chain_text(&escalation, std::slice::from_ref(&assistant), &notice.text);
        let mut asked = Envelope { to: assistant.clone(), text, question: None, ..notice.clone() };
        asked.escalation = Some(Box::new(escalation));
        (self.send)(asked, format!("question-{}", notice.id)).await?;
        tracing::info!(%thread, %human, %assistant, "asked the human's assistant first");
        Ok(Some(assistant))
    }

    /// Whether the assistant leading `escalation`, if one does, still stands for its human and can act.
    async fn assistant_leads(&self, escalation: &Escalation) -> bool {
        let (Some(human), Some(assistants)) = (&escalation.assistant_of, &self.assistants) else { return true };
        assistants.of(human, &self.ant).await.as_ref() == Some(&escalation.leader)
    }

    /// The assistant of the human `stuck` would reach that gets `from`'s escalation first, if any: not a tool
    /// request's, which [`crate::approvals`] sends, nor one the assistant had already.
    async fn assistant_first(&self, from: &Escalation, stuck: &Stuck) -> Option<(UserId, UserId)> {
        let assistants = self.assistants.as_ref()?;
        if from.reason == EscalationReason::Approval || from.assistant_of.is_some() {
            return None;
        }
        let human = &stuck.notice.to;
        let assistant = assistants.of(human, &self.ant).await?;
        (!from.is_visited(&assistant)).then(|| (assistant, human.clone()))
    }

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
    async fn overdue(&self) -> Result<()> {
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
            let notice = Envelope::on_thread(thread.clone(), self.ant.clone(), human, text)
                .with_owner(pending.owner.clone())
                .with_creator(pending.creator.clone())
                .with_origin_thread(pending.origin_thread.clone());
            let stuck = Stuck {
                reason: escalation.reason,
                request_id: escalation.request_id,
                notice,
                id,
                carried: None,
                channel: pending.channel.clone(),
            };
            let (escalated, _) = self.climb(escalation.clone(), &pending.summary, &stuck, true).await?;
            tracing::info!(%thread, %escalated, "escalated past the deadline");
        }
        Ok(())
    }

    /// Send the escalation `from` one level up, keeping `summary` for the escalations that follow it up. `had_pending`
    /// says whether the thread had an escalation already. A member climbing another ant's escalation sets no
    /// deadline: the stuck ant's daemon keeps it ([`Escalator::took_climb`]).
    async fn climb(
        &self,
        from: Escalation,
        summary: &str,
        stuck: &Stuck,
        had_pending: bool,
    ) -> Result<(Escalated, Option<String>)> {
        let thread = &stuck.notice.thread_id;
        let chain = self.chain_of(&from.leader).await;
        let summary: String = summary.chars().take(SUMMARY_CHARS).collect();
        let (owner, creator) = (stuck.notice.owner.clone(), stuck.notice.creator.clone());
        let origin_thread = stuck.notice.origin_thread.clone();
        let mut hop = from.up(&chain, self.chain.max_hops);
        if matches!(hop, Hop::Owner(_))
            && let Some((assistant, human)) = self.assistant_first(&from, stuck).await
        {
            hop = Hop::Chain { escalation: Box::new(from.to_assistant(assistant.clone(), human)), to: vec![assistant] };
        }
        match hop {
            Hop::Chain { escalation, to } => {
                let ours = escalation.stuck == self.ant;
                let deadline = self.chain.after.filter(|_| ours).map(|after| now_ms() + millis(after));
                // Before the escalation is stored: a retry stores the post again under the same id. An assistant
                // settles alone, with no group to coordinate in the notes.
                if let Some(notes) = self.notes.as_ref().filter(|_| escalation.assistant_of.is_none()) {
                    let post = NotesPost::new(thread.clone(), self.ant.clone(), notes_text(&escalation, &to));
                    notes(post, format!("{}/notes", stuck.id)).await?;
                }
                let pending = Pending {
                    escalation: (*escalation).clone(),
                    to: to.clone(),
                    renewed_by: None,
                    deadline,
                    owner,
                    creator,
                    origin_thread,
                    summary,
                    channel: stuck.channel.clone(),
                    question: stuck.notice.question.clone().map(Box::new),
                    sent_as: Some(stuck.id.clone()),
                };
                self.ledger.set_pending(thread, Some(pending)).await?;
                if deadline.is_some() {
                    self.deadline_set.notify_one();
                }
                self.to_chain(&escalation, &to, &stuck.notice.text, stuck).await?;
                Ok((chain_of(&escalation, to), None))
            }
            Hop::Owner(because) => {
                let from = match because {
                    OwnerBecause::PassedOn => from.passed_on().unwrap_or(from),
                    _ => from,
                };
                if from.level > 0 || had_pending || waits_for_turns(stuck.reason) {
                    let pending = Pending {
                        escalation: from.clone(),
                        to: Vec::new(),
                        renewed_by: None,
                        deadline: None,
                        owner,
                        creator,
                        origin_thread,
                        summary,
                        channel: stuck.channel.clone(),
                        question: stuck.notice.question.clone().map(Box::new),
                        sent_as: Some(stuck.id.clone()),
                    };
                    self.ledger.set_pending(thread, Some(pending)).await?;
                }
                self.to_owner(&from, Some(because), stuck).await
            }
        }
    }

    /// Remind the human `reminder` names of the thread's escalation, on the thread, carrying it, under a message id
    /// of its own, then schedule the next reminder, backing off, unless this one is the last ([`REMIND_AGAIN`]).
    /// Unless the human heeded it ([`Escalator::heeded`]): one marked done, read or muted ends the reminders, one
    /// snoozed holds the next back until the snooze ends.
    async fn remind(&self, thread: &ThreadId, reminder: Reminder, now: i64) -> Result<()> {
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

    /// Send `escalation` to every ant of `to`, with `summary`. Another ant's escalation goes to it as well, first,
    /// so its daemon follows the level ([`Escalator::took_climb`]).
    async fn to_chain(&self, escalation: &Escalation, to: &[UserId], summary: &str, stuck: &Stuck) -> Result<()> {
        let allow = match &stuck.notice.allow {
            Some(allow) => Some(allow.clone()),
            None => self.ledger.narrowed(&escalation.thread_id, None).await?,
        };
        if escalation.stuck != self.ant {
            let (thread, leader) = (&escalation.thread_id, &escalation.leader);
            let text = format!(
                "[ants] {} escalated thread {thread} to level {}: sent to {}; {leader} leads.",
                self.ant,
                escalation.level,
                join(to)
            );
            let notice = Envelope::on_thread(thread.clone(), self.ant.clone(), escalation.stuck.clone(), text)
                .with_owner(stuck.notice.owner.clone())
                .with_creator(stuck.notice.creator.clone())
                .with_origin_thread(stuck.notice.origin_thread.clone())
                .with_escalation(Some(escalation.clone()))
                .with_allow(allow.clone());
            (self.send)(notice, format!("{}-{}", stuck.id, escalation.stuck)).await?;
        }
        let text = chain_text(escalation, to, summary);
        for target in to {
            let mut envelope =
                Envelope::on_thread(escalation.thread_id.clone(), self.ant.clone(), target.clone(), text.clone())
                    .with_owner(stuck.notice.owner.clone())
                    .with_creator(stuck.notice.creator.clone())
                    .with_origin_thread(stuck.notice.origin_thread.clone())
                    .with_escalation(Some(escalation.clone()))
                    .with_allow(allow.clone());
            envelope.in_reply_to = stuck.notice.in_reply_to;
            (self.send)(envelope, format!("{}-{target}", stuck.id)).await?;
        }
        Ok(())
    }

    /// Send the notice of `stuck` to the thread's owner, else the default human, with `escalation`, and `because` it
    /// did not go up a chain, which it returns. The human gets a reminder after [`Chain::remind`] without a human's
    /// message on the thread; with no reminder, the ledger keeps whom it reached alone, for
    /// [`Escalator::tell_answered`]. A tool request's escalation keeps neither: [`crate::approvals`] tells of its
    /// decision.
    async fn to_owner(
        &self,
        escalation: &Escalation,
        because: Option<OwnerBecause>,
        stuck: &Stuck,
    ) -> Result<(Escalated, Option<String>)> {
        let mut notice = stuck.notice.clone();
        if let Some(why) = because.as_ref().and_then(|because| owner_text(escalation, because)) {
            notice.text = format!("{}\n\n{why}", notice.text);
        }
        let escalation = &Escalation { stand_in: self.chain.stand_in.clone(), ..escalation.clone() };
        let human = notice.to.clone();
        let why = because.map(|because| why_text(escalation, &because));
        if stuck.reason == EscalationReason::Question && notice.question.is_some() {
            // Asked of the human as the question it is, which the human answers as any ask_human question.
            (self.send)(Envelope { escalation: None, ..notice }, stuck.id.clone()).await?;
            return Ok((Escalated::Owner(human), why));
        }
        let notice = notice.with_escalation(Some(escalation.clone()));
        let remind = self.chain.remind.filter(|_| escalation.reason != EscalationReason::Approval);
        if let Some(remind) = remind {
            let now = now_ms();
            let reminder = Reminder {
                escalation: escalation.clone(),
                to: human.clone(),
                owner: notice.owner.clone(),
                creator: notice.creator.clone(),
                origin_thread: notice.origin_thread.clone(),
                summary: notice.text.chars().take(SUMMARY_CHARS).collect(),
                since: now,
                at: now + millis(remind),
                sent: 0,
                seqs: Vec::new(),
            };
            self.ledger.arm_reminder(&notice.thread_id, reminder).await?;
            self.deadline_set.notify_one();
        } else if escalation.reason != EscalationReason::Approval {
            self.ledger.reached_human(&notice.thread_id, human.clone()).await?;
        }
        let thread = notice.thread_id.clone();
        if let Some(seq) = (self.send)(notice, stuck.id.clone()).await?.filter(|_| remind.is_some()) {
            self.ledger.reached_at(&thread, seq).await?;
        }
        Ok((Escalated::Owner(human), why))
    }

    /// The escalate_to of `leader`: the ant's own, else what the colony lists, else the chain kept for it once its
    /// colony entry expired, else none.
    async fn chain_of(&self, leader: &UserId) -> Vec<UserId> {
        if *leader == self.ant {
            return self.chain.escalate_to.clone();
        }
        match (self.colony)().await {
            Ok(entries) => {
                if let Some(entry) = entries.into_iter().find(|e| e.ant == *leader) {
                    return entry.escalate_to;
                }
            }
            Err(e) => tracing::warn!(%leader, error = format!("{e:#}"), "reading the colony"),
        }
        let Some(chains) = &self.chains else { return Vec::new() };
        match chains(leader.clone()).await {
            Ok(chain) => chain.unwrap_or_default(),
            Err(e) => {
                tracing::warn!(%leader, error = format!("{e:#}"), "reading the chains; the escalation goes to the owner");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests;
