//! Escalations up the ant's chain.
//!
//! What an ant cannot settle on a thread (its turn budget spent, a limit or cap reached, its thread notes out of
//! turns, a dead letter) goes to its `escalate_to` chain before any human: every member gets it, the first one leads.
//! The members coordinate in the thread's notes; only the leader resolves it, by answering the ant on the
//! thread, or escalates it further. The leader's answer gives the thread a fresh turn budget and allowance once per
//! level ([`Escalator::arrived`]); on a channel thread, the ant's answer to it goes back to the channel with that
//! budget ([`Pending::channel`]). The next escalation goes one level higher, to the leader's own chain, which the
//! roster publishes and the chains bucket keeps once the leader is offline ([`crate::chains`]). With no chain, a chain
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

use std::sync::Arc;
use std::time::Duration;

use ants_proto::{
    ChannelId, DEFAULT_MAX_ESCALATION_HOPS, DEFAULT_TURN_BUDGET, Envelope, Escalation, EscalationReason, Hop,
    NotesPost, OwnerBecause, ThreadId, UserId,
};
use anyhow::Result;
use futures::future::BoxFuture;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::bus;
use crate::caps::{Ledger, Pending};
use crate::chains::Chains;
use crate::humans::Registry;
use crate::mcp::Roster;

/// Default of `escalate_after_secs`: as long as a prompt may run by default.
pub const DEFAULT_ESCALATE_AFTER: Duration = Duration::from_secs(1800);
/// Pause after a failed look at the deadlines.
const RETRY: Duration = Duration::from_secs(30);
/// Most of an escalation's text kept for the escalations that follow it up.
const SUMMARY_CHARS: usize = 4000;

/// Stores an envelope under a message id, resolving once JetStream has it.
pub type Sender = Arc<dyn Fn(Envelope, String) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Stores through JetStream ([`bus::store`]).
pub fn jetstream_sender(js: async_nats::jetstream::Context) -> Sender {
    Arc::new(move |envelope, id| {
        let js = js.clone();
        Box::pin(async move { bus::store(&js, envelope.subject(), &envelope, &id).await.map(drop) })
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
}

impl Default for Chain {
    fn default() -> Self {
        Self { escalate_to: Vec::new(), max_hops: DEFAULT_MAX_ESCALATION_HOPS, after: Some(DEFAULT_ESCALATE_AFTER) }
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
}

impl std::fmt::Display for Escalated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chain { level, leader, to } => {
                write!(f, "{} at level {level}, led by {leader}", join(to))
            }
            Self::Owner(human) => write!(f, "{human}"),
        }
    }
}

/// Sends an ant's escalations up its chain and keeps where each thread's stands.
pub struct Escalator {
    ant: UserId,
    chain: Chain,
    roster: Roster,
    /// The chains kept past the roster, for a leader that is offline.
    chains: Option<Chains>,
    humans: Registry,
    ledger: Arc<Ledger>,
    send: Sender,
    /// Gives the thread's notes a fresh budget as an escalation goes up a level; None leaves the notes as they are.
    notes: Option<NotesSender>,
    /// Held while an escalation is decided and stored, so two on one thread do not climb from the same level.
    busy: Mutex<()>,
    /// A deadline was set.
    deadline_set: Notify,
}

impl Escalator {
    /// Escalations of `ant` up `chain`, the chains of others read from `roster`, kept in `ledger`.
    pub fn new(ant: UserId, chain: Chain, roster: Roster, humans: Registry, ledger: Arc<Ledger>, send: Sender) -> Self {
        let (busy, deadline_set) = (Mutex::default(), Notify::new());
        Self { ant, chain, roster, chains: None, humans, ledger, send, notes: None, busy, deadline_set }
    }

    /// Follow the chain of a leader missing from the roster through `chains`.
    pub fn with_chains(mut self, chains: Chains) -> Self {
        self.chains = Some(chains);
        self
    }

    /// Post to the thread's notes through `notes` as each escalation goes up a level, with a fresh budget.
    pub fn with_notes(self, notes: NotesSender) -> Self {
        Self { notes: Some(notes), ..self }
    }

    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// Escalate `stuck`, the ant's issue on the thread of its notice, one level up and return where it went. The
    /// first escalation of a thread goes to the ant's own chain. One that carries an escalation, met while working
    /// on it for another ant, goes one level above it, to its leader's chain. A thread whose escalation was
    /// answered by its leader goes one level higher too; one still waiting for its leader goes to the same members
    /// again. The escalation is stored before it is sent, so a retry sends the same messages.
    pub async fn escalate(&self, stuck: Stuck) -> Result<Escalated> {
        self.escalate_explained(stuck).await.map(|(escalated, _)| escalated)
    }

    /// [`Escalator::escalate`], also returning why the escalation went to the thread's owner rather than up a chain,
    /// when it went there just now.
    pub async fn escalate_explained(&self, stuck: Stuck) -> Result<(Escalated, Option<String>)> {
        let _busy = self.busy.lock().await;
        let thread = stuck.notice.thread_id.clone();
        let pending = self.ledger.pending(&thread).await?;
        let channel = stuck.channel.clone().or_else(|| pending.as_ref().and_then(|p| p.channel.clone()));
        let stuck = Stuck { channel, ..stuck };
        let step =
            next_step(&self.ant, &thread, stuck.reason, stuck.request_id, stuck.carried.as_ref(), pending.as_ref());
        tracing::info!(%thread, reason = stuck.reason.as_str(), ?step, "escalating");
        match step {
            Step::Again(pending) if pending.at_owner() => self.to_owner(&pending.escalation, None, &stuck).await,
            Step::Again(pending) => {
                self.to_chain(&pending.escalation, &pending.to, &stuck.notice.text, &stuck).await?;
                Ok((chain_of(&pending.escalation, pending.to), None))
            }
            Step::Climb(from) => {
                let summary = stuck.notice.text.clone();
                self.climb(from, &summary, &stuck, pending.is_some()).await
            }
        }
    }

    /// A message the ant is about to answer: a human's ends the thread's escalation and gives the thread a fresh
    /// allowance. One from the leader of the thread's escalation, carrying it, is the leader's answer: the first one
    /// at the escalation's level gives the thread a fresh allowance and `envelope` a fresh turn budget, and stops the
    /// deadline. Returns whether it did; a delivery of the same message again does again.
    pub async fn arrived(&self, envelope: &mut Envelope, human: bool) -> Result<bool> {
        let thread = envelope.thread_id.clone();
        if human {
            if self.ledger.reset(&thread).await? {
                tracing::info!(%thread, from = %envelope.from, "a human ended the thread's escalation");
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

    /// Whether `envelope` tells the ant that a member sent the ant's own escalation one level higher: it
    /// carries that escalation on the stuck thread, from someone other than its leader. A level above the thread's
    /// becomes the thread's, with a fresh deadline, so the next escalation climbs from there. Such a message is for
    /// the daemon, not the harness.
    pub async fn took_climb(&self, envelope: &Envelope) -> Result<bool> {
        let Some(carried) = envelope.escalation.as_deref() else { return Ok(false) };
        let thread = &envelope.thread_id;
        if carried.thread_id != *thread
            || carried.reason == EscalationReason::Approval
            || carried.stuck != self.ant
            || envelope.from == self.ant
            || carried.is_leader(&envelope.from)
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

    /// Send every escalation whose leader did not answer by its deadline one level higher, as deadlines pass. Runs
    /// until dropped.
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
                        tokio::time::sleep(Duration::from_millis(at.saturating_sub(now_ms()).max(0) as u64)).await
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

    /// Escalate one level higher every escalation past its deadline, dropping stale deadlines.
    async fn overdue(&self) -> Result<()> {
        let now = now_ms();
        for (thread, at) in self.ledger.deadlines().await? {
            if at > now {
                continue;
            }
            let _busy = self.busy.lock().await;
            let pending = self.ledger.pending(&thread).await?;
            let due =
                pending.filter(|p| p.deadline.is_some_and(|d| d <= now) && p.renewed_by.is_none() && !p.at_owner());
            let Some(pending) = due else {
                self.ledger.recheck(&thread).await?;
                continue;
            };
            let escalation = &pending.escalation;
            let waited = self.chain.after.map_or_else(String::new, |after| format!(" within {}", duration(after)));
            let text = format!(
                "[ants] No answer from {} on thread {thread}{waited}; the escalation goes one level higher.\n\n{}",
                escalation.leader, pending.summary
            );
            let human = self.humans.owner_or_default(pending.owner.as_ref());
            let notice =
                Envelope::on_thread(thread.clone(), self.ant.clone(), human, text).with_owner(pending.owner.clone());
            let stuck = Stuck {
                reason: escalation.reason,
                request_id: escalation.request_id,
                notice,
                id: format!("overdue-{}-{thread}-{}", self.ant, escalation.level),
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
        let owner = stuck.notice.owner.clone();
        match from.up(&chain, self.chain.max_hops) {
            Hop::Chain { escalation, to } => {
                let ours = escalation.stuck == self.ant;
                let deadline = self.chain.after.filter(|_| ours).map(|after| now_ms() + millis(after));
                // Before the escalation is stored: a retry stores the post again under the same id.
                if let Some(notes) = &self.notes {
                    let post = NotesPost::new(thread.clone(), self.ant.clone(), notes_text(&escalation, &to));
                    notes(post, format!("{}/notes", stuck.id)).await?;
                }
                let pending = Pending {
                    escalation: escalation.clone(),
                    to: to.clone(),
                    renewed_by: None,
                    deadline,
                    owner,
                    summary,
                    channel: stuck.channel.clone(),
                };
                self.ledger.set_pending(thread, Some(pending)).await?;
                if deadline.is_some() {
                    self.deadline_set.notify_one();
                }
                self.to_chain(&escalation, &to, &stuck.notice.text, stuck).await?;
                Ok((chain_of(&escalation, to), None))
            }
            Hop::Owner(because) => {
                if from.level > 0 || had_pending {
                    let pending = Pending {
                        escalation: from.clone(),
                        to: Vec::new(),
                        renewed_by: None,
                        deadline: None,
                        owner,
                        summary,
                        channel: stuck.channel.clone(),
                    };
                    self.ledger.set_pending(thread, Some(pending)).await?;
                }
                self.to_owner(&from, Some(because), stuck).await
            }
        }
    }

    /// Send `escalation` to every ant of `to`, with `summary`. Another ant's escalation goes to it as well, first,
    /// so its daemon follows the level ([`Escalator::took_climb`]).
    async fn to_chain(&self, escalation: &Escalation, to: &[UserId], summary: &str, stuck: &Stuck) -> Result<()> {
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
                .with_escalation(Some(escalation.clone()));
            (self.send)(notice, format!("{}-{}", stuck.id, escalation.stuck)).await?;
        }
        let text = chain_text(escalation, to, summary);
        for target in to {
            let mut envelope =
                Envelope::on_thread(escalation.thread_id.clone(), self.ant.clone(), target.clone(), text.clone())
                    .with_owner(stuck.notice.owner.clone())
                    .with_escalation(Some(escalation.clone()));
            envelope.in_reply_to = stuck.notice.in_reply_to;
            (self.send)(envelope, format!("{}-{target}", stuck.id)).await?;
        }
        Ok(())
    }

    /// Send the notice of `stuck` to the thread's owner, else the default human, with `escalation` when it went up a
    /// chain first, and `because` it did not go up one, which it returns.
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
        if escalation.level > 0 {
            notice = notice.with_escalation(Some(escalation.clone()));
        }
        let human = notice.to.clone();
        (self.send)(notice, stuck.id.clone()).await?;
        Ok((Escalated::Owner(human), because.map(|because| why_text(escalation, &because))))
    }

    /// The escalate_to of `leader`: the ant's own, else what the roster lists, else the chain kept for it once its
    /// roster entry expired, else none.
    async fn chain_of(&self, leader: &UserId) -> Vec<UserId> {
        if *leader == self.ant {
            return self.chain.escalate_to.clone();
        }
        match (self.roster)().await {
            Ok(entries) => {
                if let Some(entry) = entries.into_iter().find(|e| e.ant == *leader) {
                    return entry.escalate_to;
                }
            }
            Err(e) => tracing::warn!(%leader, error = format!("{e:#}"), "reading the roster"),
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

/// What an escalation does next.
#[derive(Debug, Clone, PartialEq)]
enum Step {
    /// Go to the same members again, at the same level: the leader has not answered yet.
    Again(Pending),
    /// Go one level above this escalation.
    Climb(Escalation),
}

/// What `ant`'s escalation for `reason` on `thread` does next, given the escalation `carried` by the message it
/// got stuck on and the thread's `pending` one. A carried escalation the ant started itself, with none pending,
/// was ended by a human: it starts afresh.
fn next_step(
    ant: &UserId,
    thread: &ThreadId,
    reason: EscalationReason,
    request_id: Option<Uuid>,
    carried: Option<&Escalation>,
    pending: Option<&Pending>,
) -> Step {
    let carried = carried.filter(|c| &c.thread_id == thread);
    let retarget = |e: &Escalation| Escalation { reason, request_id, ..e.clone() };
    match (carried, pending) {
        (Some(c), Some(p)) if c.level > p.escalation.level => Step::Climb(retarget(c)),
        (Some(c), None) if c.stuck != *ant => Step::Climb(retarget(c)),
        (_, Some(p)) if p.renewed_by.is_some() => Step::Climb(retarget(&p.escalation)),
        (_, Some(p)) => Step::Again(p.clone()),
        _ => Step::Climb(retarget(&Escalation::new(ant.clone(), thread.clone(), reason))),
    }
}

fn chain_of(escalation: &Escalation, to: Vec<UserId>) -> Escalated {
    Escalated::Chain { level: escalation.level, leader: escalation.leader.clone(), to }
}

/// The text every member of a chain gets: the escalation, how the members settle it, and `summary`.
fn chain_text(escalation: &Escalation, to: &[UserId], summary: &str) -> String {
    let (thread, leader, stuck) = (&escalation.thread_id, &escalation.leader, &escalation.stuck);
    format!(
        "[ants] Escalation on thread {thread}, level {}: {stuck} is stuck ({}). Sent to {}; {leader} leads. The members \
         coordinate in the notes of thread {thread} (read_notes, post_notes). Only {leader} resolves it, by \
         answering {stuck} on thread {thread}, or escalates it further (escalate).\n\n{summary}",
        escalation.level,
        reason_text(escalation.reason),
        join(to),
    )
}

/// What an escalation going up to `to` posts to the thread's notes, with a fresh budget.
fn notes_text(escalation: &Escalation, to: &[UserId]) -> String {
    format!(
        "[ants] Escalation of {}'s thread, level {}: sent to {}; {} leads. The notes have a fresh budget of {} turns.",
        escalation.stuck,
        escalation.level,
        join(to),
        escalation.leader,
        DEFAULT_TURN_BUDGET
    )
}

/// The members of `escalation`'s level: its leader and the ants sent it with the leader, in chain order.
pub fn members_at(escalation: &Escalation) -> &[UserId] {
    let from = escalation.visited.iter().position(|p| *p == escalation.leader).unwrap_or(escalation.visited.len());
    &escalation.visited[from..]
}

/// Where a member's reply to a message carrying another ant's escalation goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// To the stuck ant: the leader resolves the escalation.
    Stuck(UserId),
    /// To the thread's notes, for the leader: a member other than the leader does not answer the stuck ant.
    Notes,
    /// To the sender, a fellow member of the group, as for any message.
    Sender,
}

/// The escalation `envelope` carries, when it is another ant's on the message's thread, which makes `ant` a
/// member of its group. Not an approval request's: any member decides one, and it goes no higher.
pub fn group<'a>(ant: &UserId, envelope: &'a Envelope) -> Option<&'a Escalation> {
    envelope
        .escalation
        .as_deref()
        .filter(|e| e.thread_id == envelope.thread_id && e.stuck != *ant && e.reason != EscalationReason::Approval)
}

/// The escalation of the group `ant` is a member of on `envelope` ([`group`]), and where its reply goes.
pub fn route<'a>(ant: &UserId, envelope: &'a Envelope) -> Option<(&'a Escalation, Route)> {
    let e = group(ant, envelope)?;
    let route = if members_at(e).contains(&envelope.from) {
        Route::Sender
    } else if e.is_leader(ant) {
        Route::Stuck(e.stuck.clone())
    } else {
        Route::Notes
    };
    Some((e, route))
}

/// What `ant`'s harness is told about the escalation `envelope` carries, when it is another ant's on the
/// message's thread: who is in the group, who leads, and where the reply goes.
pub fn group_note(ant: &UserId, envelope: &Envelope) -> Option<String> {
    let (e, route) = route(ant, envelope)?;
    let (thread, leader, stuck) = (&e.thread_id, &e.leader, &e.stuck);
    let role = if e.is_leader(ant) {
        format!(
            "{ant} leads: it alone resolves the escalation, by answering {stuck} on thread {thread}, or sends it \
             one level higher (escalate tool) when the group cannot. The other members post to the thread notes."
        )
    } else {
        format!(
            "Only {leader} resolves it (answers {stuck} on thread {thread}) or escalates it further (escalate tool)."
        )
    };
    let reply = match route {
        Route::Stuck(_) => format!("The reply to this message goes to {stuck}."),
        Route::Notes => format!("The reply to this message goes to the thread notes for {leader}, not to {stuck}."),
        Route::Sender => format!("The reply to this message goes to {}.", envelope.from),
    };
    Some(format!(
        "[ants] {ant} is part of an escalation group for {stuck}'s thread {thread}, level {}; members: {}. The group \
         shares the thread's notes (read_notes, post_notes). {role} {reply}",
        e.level,
        join(members_at(e))
    ))
}

/// Why an escalation went to the owner, for the ant that asked for it.
fn why_text(escalation: &Escalation, because: &OwnerBecause) -> String {
    owner_text(escalation, because)
        .unwrap_or_else(|| format!("{} has no escalate_to chain.", escalation.leader))
        .trim_start_matches("[ants] ")
        .to_owned()
}

/// Why an escalation went to the owner, unless it never went up a chain for want of one, as before chains existed.
fn owner_text(escalation: &Escalation, because: &OwnerBecause) -> Option<String> {
    let (stuck, level) = (&escalation.stuck, escalation.level);
    match because {
        OwnerBecause::NoChain if level == 0 => None,
        OwnerBecause::NoChain => Some(format!(
            "[ants] The escalation of {stuck} reached level {level}; {} has no escalate_to above it.",
            escalation.leader
        )),
        OwnerBecause::Visited(ant) => Some(format!(
            "[ants] The escalation of {stuck} reached level {level}; {ant} had it already, so the chains loop."
        )),
        OwnerBecause::HopLimit(max) => {
            Some(format!("[ants] The escalation of {stuck} reached the hop limit (max_escalation_hops = {max})."))
        }
    }
}

fn reason_text(reason: EscalationReason) -> &'static str {
    match reason {
        EscalationReason::TurnBudget => "turn budget spent",
        EscalationReason::Cap => "limit or cap reached",
        EscalationReason::Notes => "thread notes out of turns",
        EscalationReason::DeadLetter => "a message became a dead letter",
        EscalationReason::Approval => "a tool request awaits a decision",
        EscalationReason::Asked => "asked for help",
        EscalationReason::Unknown => "reason unknown",
    }
}

fn join(ants: &[UserId]) -> String {
    ants.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ")
}

fn duration(d: Duration) -> String {
    match d.as_secs() {
        s if s >= 60 && s % 60 == 0 => format!("{} min", s / 60),
        s if s > 0 => format!("{s} s"),
        _ => format!("{} ms", d.as_millis()),
    }
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(name: &str) -> UserId {
        name.parse().unwrap()
    }

    fn t() -> ThreadId {
        "t1".parse().unwrap()
    }

    fn pending(escalation: Escalation, to: &[&str], renewed: bool) -> Pending {
        Pending {
            escalation,
            to: to.iter().map(|s| p(s)).collect(),
            renewed_by: renewed.then(Uuid::new_v4),
            deadline: None,
            owner: None,
            summary: String::new(),
            channel: None,
        }
    }

    fn level(e: &Escalation, chain: &[&str]) -> Escalation {
        let chain: Vec<_> = chain.iter().map(|s| p(s)).collect();
        match e.up(&chain, 3) {
            Hop::Chain { escalation, .. } => escalation,
            Hop::Owner(because) => panic!("to the owner: {because:?}"),
        }
    }

    fn step(ant: &str, carried: Option<&Escalation>, pending: Option<&Pending>) -> Step {
        next_step(&p(ant), &t(), EscalationReason::Cap, None, carried, pending)
    }

    #[test]
    fn a_first_escalation_starts_at_the_stuck_ant() {
        let new = Escalation::new(p("dev-a"), t(), EscalationReason::Cap);
        assert_eq!(step("dev-a", None, None), Step::Climb(new));
    }

    #[test]
    fn one_answered_by_its_leader_climbs_and_one_waiting_goes_again() {
        let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
        let one = level(&start, &["lead", "dev-b"]);
        let waiting = pending(one.clone(), &["lead", "dev-b"], false);
        assert_eq!(step("dev-a", None, Some(&waiting)), Step::Again(waiting.clone()));
        let climb = step("dev-a", Some(&one), Some(&pending(one.clone(), &["lead"], true)));
        assert_eq!(climb, Step::Climb(Escalation { reason: EscalationReason::Cap, ..one.clone() }));
        let owner = pending(one.clone(), &[], false);
        assert_eq!(step("dev-a", None, Some(&owner)), Step::Again(owner.clone()), "a human has it until answering");
    }

    #[test]
    fn a_member_stuck_on_an_escalation_climbs_from_it() {
        let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
        let one = level(&start, &["lead", "dev-b"]);
        let expected = Escalation { reason: EscalationReason::Cap, ..one.clone() };
        assert_eq!(step("lead", Some(&one), None), Step::Climb(expected.clone()));
        assert_eq!(step("dev-b", Some(&one), None), Step::Climb(expected.clone()));
        let two = level(&one, &["boss"]);
        let lower = pending(one.clone(), &["lead"], false);
        assert_eq!(
            step("dev-a", Some(&two), Some(&lower)),
            Step::Climb(Escalation { reason: EscalationReason::Cap, ..two })
        );
        let other: ThreadId = "t2".parse().unwrap();
        let elsewhere = Escalation { thread_id: other, ..one.clone() };
        assert_eq!(step("lead", Some(&elsewhere), None), step("lead", None, None), "another thread's is no record");
    }

    #[test]
    fn a_carried_escalation_a_human_ended_starts_afresh() {
        let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
        let one = level(&start, &["lead"]);
        assert_eq!(
            step("dev-a", Some(&one), None),
            Step::Climb(Escalation::new(p("dev-a"), t(), EscalationReason::Cap))
        );
    }

    #[test]
    fn only_the_leader_answers_the_stuck_ant() {
        let one = level(&Escalation::new(p("dev-a"), t(), EscalationReason::Asked), &["lead", "dev-b", "dev-c"]);
        let to = |from: &str, to: &str| {
            let envelope = Envelope::on_thread(t(), p(from), p(to), "help").with_escalation(Some(one.clone()));
            route(&p(to), &envelope).map(|(_, route)| route)
        };
        assert_eq!(to("dev-a", "lead"), Some(Route::Stuck(p("dev-a"))));
        assert_eq!(to("dev-a", "dev-b"), Some(Route::Notes));
        assert_eq!(to("dev-b", "dev-c"), Some(Route::Sender), "between members, as any message");
        assert_eq!(to("dev-b", "lead"), Some(Route::Sender));
        assert_eq!(to("lead", "dev-a"), None, "the stuck ant's own escalation");
        assert_eq!(members_at(&one), [p("lead"), p("dev-b"), p("dev-c")]);

        // Any member decides an approval request: no group, no leader.
        let approval = Escalation { level: 1, ..one.clone() };
        let approval = Escalation { reason: EscalationReason::Approval, ..approval };
        let asked = Envelope::on_thread(t(), p("dev-a"), p("dev-b"), "may it?").with_escalation(Some(approval));
        assert_eq!(route(&p("dev-b"), &asked), None);
        assert_eq!(group_note(&p("dev-b"), &asked), None);
    }

    #[test]
    fn notices_say_who_leads_and_why_the_owner_has_it() {
        let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
        let one = level(&start, &["lead", "dev-b"]);
        let text = chain_text(&one, &[p("lead"), p("dev-b")], "Turn budget spent; needs a decision.");
        assert!(text.starts_with("[ants] Escalation on thread t1, level 1: dev-a is stuck (turn budget spent)."));
        assert!(text.contains("Sent to lead, dev-b; lead leads."), "{text}");
        assert!(text.contains("Only lead resolves it, by answering dev-a on thread t1"), "{text}");
        assert!(text.ends_with("\n\nTurn budget spent; needs a decision."));
        assert_eq!(owner_text(&start, &OwnerBecause::NoChain), None, "as before chains");
        assert!(owner_text(&one, &OwnerBecause::HopLimit(1)).unwrap().contains("hop limit (max_escalation_hops = 1)"));
        assert!(owner_text(&one, &OwnerBecause::Visited(p("dev-a"))).unwrap().contains("dev-a had it already"));
    }
}
