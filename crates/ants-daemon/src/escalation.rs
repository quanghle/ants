//! Escalations up the persona's chain.
//!
//! What a persona cannot settle on a thread (its turn budget spent, a limit or cap reached, its plan room out of
//! turns, a dead letter) goes to its `escalate_to` chain before any human: every member gets it, the first one leads.
//! The members coordinate in the thread's plan room; only the leader resolves it, by answering the persona on the
//! thread, or escalates it further. The leader's answer gives the thread a fresh turn budget and allowance once per
//! level ([`Escalator::arrived`]); on a channel thread, the persona's answer to it goes back to the channel with that
//! budget ([`Pending::channel`]). The next escalation goes one level higher, to the leader's own chain, which the
//! roster publishes and the chains bucket keeps once the leader is offline ([`crate::chains`]). With no chain, a chain
//! that loops back or past `max_escalation_hops`, it goes to the thread's owner, else the default human, as it did
//! before chains. Only a human's message ends an escalation.
//!
//! Without an answer from the leader within `escalate_after`, the persona that escalated sends it one level higher
//! ([`Escalator::deadlines`]). The thread's escalation lives with its spend in `ants_sessions` ([`Pending`]), so
//! restarts keep its level and deadline.

use std::sync::Arc;
use std::time::Duration;

use ants_proto::{
    ChannelId, DEFAULT_MAX_ESCALATION_HOPS, DEFAULT_TURN_BUDGET, Envelope, Escalation, EscalationReason, Hop,
    OwnerBecause, PersonaId, ThreadId,
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

/// The persona's escalation chain and its limits, from the config file.
#[derive(Debug, Clone, PartialEq)]
pub struct Chain {
    /// Who gets the persona's escalations, the first one leading; empty sends them to the thread's owner.
    pub escalate_to: Vec<PersonaId>,
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

/// What the persona cannot settle on a thread.
#[derive(Debug, Clone)]
pub struct Stuck {
    pub reason: EscalationReason,
    /// The tool request awaiting a decision, for [`EscalationReason::Approval`].
    pub request_id: Option<Uuid>,
    /// What the thread's owner, else the default human, gets when the escalation goes there: a message from the
    /// persona on the stuck thread. Up a chain, every member gets its text after the escalation's.
    pub notice: Envelope,
    /// Message id, the same for every attempt, so a retry stores nothing twice; each chain member's copy adds its name.
    pub id: String,
    /// The escalation the message the persona got stuck on carries, if any.
    pub carried: Option<Escalation>,
    /// The channel of the post the persona got stuck on, if it was one; else that of the thread's escalation.
    pub channel: Option<ChannelId>,
}

impl Stuck {
    /// `notice` about `reason`, under message id `id`, from the persona stuck on `about`, which may carry an
    /// escalation.
    pub fn new(reason: EscalationReason, notice: Envelope, id: String, about: Option<&Envelope>) -> Self {
        let carried = about.and_then(|e| e.escalation.as_deref().cloned());
        Self { reason, request_id: None, notice, id, carried, channel: None }
    }
}

/// Where an escalation went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Escalated {
    /// Up a chain, to every persona of `to`, `leader` first.
    Chain { level: u32, leader: PersonaId, to: Vec<PersonaId> },
    /// To this human, the thread's owner or the default one.
    Owner(PersonaId),
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

/// Sends a persona's escalations up its chain and keeps where each thread's stands.
pub struct Escalator {
    persona: PersonaId,
    chain: Chain,
    roster: Roster,
    /// The chains kept past the roster, for a leader that is offline.
    chains: Option<Chains>,
    humans: Registry,
    ledger: Arc<Ledger>,
    send: Sender,
    /// Held while an escalation is decided and stored, so two on one thread do not climb from the same level.
    busy: Mutex<()>,
    /// A deadline was set.
    deadline_set: Notify,
}

impl Escalator {
    /// Escalations of `persona` up `chain`, the chains of others read from `roster`, kept in `ledger`.
    pub fn new(
        persona: PersonaId,
        chain: Chain,
        roster: Roster,
        humans: Registry,
        ledger: Arc<Ledger>,
        send: Sender,
    ) -> Self {
        let (busy, deadline_set) = (Mutex::default(), Notify::new());
        Self { persona, chain, roster, chains: None, humans, ledger, send, busy, deadline_set }
    }

    /// Follow the chain of a leader missing from the roster through `chains`.
    pub fn with_chains(mut self, chains: Chains) -> Self {
        self.chains = Some(chains);
        self
    }

    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// Escalate `stuck`, the persona's issue on the thread of its notice, one level up and return where it went. The
    /// first escalation of a thread goes to the persona's own chain. One that carries an escalation, met while working
    /// on it for another persona, goes one level above it, to its leader's chain. A thread whose escalation was
    /// answered by its leader goes one level higher too; one still waiting for its leader goes to the same members
    /// again. The escalation is stored before it is sent, so a retry sends the same messages.
    pub async fn escalate(&self, stuck: Stuck) -> Result<Escalated> {
        let _busy = self.busy.lock().await;
        let thread = stuck.notice.thread_id.clone();
        let pending = self.ledger.pending(&thread).await?;
        let channel = stuck.channel.clone().or_else(|| pending.as_ref().and_then(|p| p.channel.clone()));
        let stuck = Stuck { channel, ..stuck };
        let step =
            next_step(&self.persona, &thread, stuck.reason, stuck.request_id, stuck.carried.as_ref(), pending.as_ref());
        tracing::info!(%thread, reason = stuck.reason.as_str(), ?step, "escalating");
        match step {
            Step::Again(pending) if pending.at_owner() => self.to_owner(&pending.escalation, None, &stuck).await,
            Step::Again(pending) => {
                self.to_chain(&pending.escalation, &pending.to, &stuck.notice.text, &stuck).await?;
                Ok(chain_of(&pending.escalation, pending.to))
            }
            Step::Climb(from) => {
                let summary = stuck.notice.text.clone();
                self.climb(from, &summary, &stuck, pending.is_some()).await
            }
        }
    }

    /// A message the persona is about to answer: a human's ends the thread's escalation and gives the thread a fresh
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
        let renewed = self.ledger.renew(&thread, &envelope.from, envelope.id).await?;
        if renewed {
            tracing::info!(%thread, leader = %envelope.from, "the leader answered the escalation; fresh budget");
            envelope.turn_budget = envelope.turn_budget.max(DEFAULT_TURN_BUDGET);
        }
        Ok(renewed)
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
            let notice = Envelope::on_thread(thread.clone(), self.persona.clone(), human, text)
                .with_owner(pending.owner.clone());
            let stuck = Stuck {
                reason: escalation.reason,
                request_id: escalation.request_id,
                notice,
                id: format!("overdue-{}-{thread}-{}", self.persona, escalation.level),
                carried: None,
                channel: pending.channel.clone(),
            };
            let escalated = self.climb(escalation.clone(), &pending.summary, &stuck, true).await?;
            tracing::info!(%thread, %escalated, "escalated past the deadline");
        }
        Ok(())
    }

    /// Send the escalation `from` one level up, keeping `summary` for the escalations that follow it up. `had_pending`
    /// says whether the thread had an escalation already.
    async fn climb(&self, from: Escalation, summary: &str, stuck: &Stuck, had_pending: bool) -> Result<Escalated> {
        let thread = &stuck.notice.thread_id;
        let chain = self.chain_of(&from.leader).await;
        let summary: String = summary.chars().take(SUMMARY_CHARS).collect();
        let owner = stuck.notice.owner.clone();
        match from.up(&chain, self.chain.max_hops) {
            Hop::Chain { escalation, to } => {
                let deadline = self.chain.after.map(|after| now_ms() + millis(after));
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
                Ok(chain_of(&escalation, to))
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

    /// Send `escalation` to every persona of `to`, with `summary`.
    async fn to_chain(&self, escalation: &Escalation, to: &[PersonaId], summary: &str, stuck: &Stuck) -> Result<()> {
        let text = chain_text(&self.persona, escalation, to, summary);
        for target in to {
            let mut envelope =
                Envelope::on_thread(escalation.thread_id.clone(), self.persona.clone(), target.clone(), text.clone())
                    .with_owner(stuck.notice.owner.clone())
                    .with_escalation(Some(escalation.clone()));
            envelope.in_reply_to = stuck.notice.in_reply_to;
            (self.send)(envelope, format!("{}-{target}", stuck.id)).await?;
        }
        Ok(())
    }

    /// Send the notice of `stuck` to the thread's owner, else the default human, with `escalation` when it went up a
    /// chain first, and `because` it did not go up one.
    async fn to_owner(
        &self,
        escalation: &Escalation,
        because: Option<OwnerBecause>,
        stuck: &Stuck,
    ) -> Result<Escalated> {
        let mut notice = stuck.notice.clone();
        if let Some(why) = because.and_then(|because| owner_text(escalation, &because)) {
            notice.text = format!("{}\n\n{why}", notice.text);
        }
        if escalation.level > 0 {
            notice = notice.with_escalation(Some(escalation.clone()));
        }
        let human = notice.to.clone();
        (self.send)(notice, stuck.id.clone()).await?;
        Ok(Escalated::Owner(human))
    }

    /// The escalate_to of `leader`: the persona's own, else what the roster lists, else the chain kept for it once its
    /// roster entry expired, else none.
    async fn chain_of(&self, leader: &PersonaId) -> Vec<PersonaId> {
        if *leader == self.persona {
            return self.chain.escalate_to.clone();
        }
        match (self.roster)().await {
            Ok(entries) => {
                if let Some(entry) = entries.into_iter().find(|e| e.persona == *leader) {
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

/// What `persona`'s escalation for `reason` on `thread` does next, given the escalation `carried` by the message it
/// got stuck on and the thread's `pending` one. A carried escalation the persona started itself, with none pending,
/// was ended by a human: it starts afresh.
fn next_step(
    persona: &PersonaId,
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
        (Some(c), None) if c.stuck != *persona => Step::Climb(retarget(c)),
        (_, Some(p)) if p.renewed_by.is_some() => Step::Climb(retarget(&p.escalation)),
        (_, Some(p)) => Step::Again(p.clone()),
        _ => Step::Climb(retarget(&Escalation::new(persona.clone(), thread.clone(), reason))),
    }
}

fn chain_of(escalation: &Escalation, to: Vec<PersonaId>) -> Escalated {
    Escalated::Chain { level: escalation.level, leader: escalation.leader.clone(), to }
}

/// The text every member of a chain gets: the escalation, how the members settle it, and `summary`.
fn chain_text(from: &PersonaId, escalation: &Escalation, to: &[PersonaId], summary: &str) -> String {
    let (thread, leader) = (&escalation.thread_id, &escalation.leader);
    format!(
        "[ants] Escalation on thread {thread}, level {}: {} is stuck ({}). Sent to {}; {leader} leads. The members \
         coordinate in the plan room of thread {thread} (read_plan, post_plan). Only {leader} resolves it, by \
         answering {from} on thread {thread} (a reply to this message does), or escalates it further.\n\n{summary}",
        escalation.level,
        escalation.stuck,
        reason_text(escalation.reason),
        join(to),
    )
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
        OwnerBecause::Visited(persona) => Some(format!(
            "[ants] The escalation of {stuck} reached level {level}; {persona} had it already, so the chains loop."
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
        EscalationReason::PlanRoom => "plan room out of turns",
        EscalationReason::DeadLetter => "a message became a dead letter",
        EscalationReason::Approval => "a tool request awaits a decision",
        EscalationReason::Asked => "asked for help",
        EscalationReason::Unknown => "reason unknown",
    }
}

fn join(personas: &[PersonaId]) -> String {
    personas.iter().map(PersonaId::as_str).collect::<Vec<_>>().join(", ")
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

    fn p(name: &str) -> PersonaId {
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

    fn step(persona: &str, carried: Option<&Escalation>, pending: Option<&Pending>) -> Step {
        next_step(&p(persona), &t(), EscalationReason::Cap, None, carried, pending)
    }

    #[test]
    fn a_first_escalation_starts_at_the_stuck_persona() {
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
    fn notices_say_who_leads_and_why_the_owner_has_it() {
        let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
        let one = level(&start, &["lead", "dev-b"]);
        let text = chain_text(&p("dev-a"), &one, &[p("lead"), p("dev-b")], "Turn budget spent; needs a decision.");
        assert!(text.starts_with("[ants] Escalation on thread t1, level 1: dev-a is stuck (turn budget spent)."));
        assert!(text.contains("Sent to lead, dev-b; lead leads."), "{text}");
        assert!(text.contains("Only lead resolves it, by answering dev-a on thread t1"), "{text}");
        assert!(text.ends_with("\n\nTurn budget spent; needs a decision."));
        assert_eq!(owner_text(&start, &OwnerBecause::NoChain), None, "as before chains");
        assert!(owner_text(&one, &OwnerBecause::HopLimit(1)).unwrap().contains("hop limit (max_escalation_hops = 1)"));
        assert!(owner_text(&one, &OwnerBecause::Visited(p("dev-a"))).unwrap().contains("dev-a had it already"));
    }
}
