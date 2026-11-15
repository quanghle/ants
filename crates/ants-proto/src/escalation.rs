use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ApprovalRequest, Error, ThreadId, UserId};
#[cfg(doc)]
use crate::{CHAINS_BUCKET, Envelope, storage_warning_key};

/// An ant's escalation chain in the [`CHAINS_BUCKET`], under the key of its id: its `escalate_to`, as the
/// roster entry publishes it, kept after the entry expires. Its daemon puts it on start; readers skip an entry naming
/// another ant than its key ([`ChainEntry::check_key`]) and treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainEntry {
    pub ant: UserId,
    /// The ant's escalation chain, in order, the first leading; empty for none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<UserId>,
    /// When the daemon put it.
    pub updated_at: DateTime<Utc>,
}

impl ChainEntry {
    /// The chain `escalate_to` of `ant`, put now.
    pub fn new(ant: UserId, escalate_to: Vec<UserId>) -> Self {
        Self { ant, escalate_to, updated_at: Utc::now() }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.ant.as_str()
    }

    /// Check that the entry was read from its own key: credentials pin each ant to its own key, while the body is
    /// whatever the writer put there, so an entry naming another ant is forged.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "chain entry", user: self.ant.clone(), at: format!("key {key}") })
    }
}

/// Why an ant escalates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationReason {
    /// The thread's turn budget is spent.
    TurnBudget,
    /// A prompt limit or a spending cap was reached.
    Cap,
    /// The thread's notes are out of turns.
    Notes,
    /// A message on the thread became a dead letter.
    DeadLetter,
    /// A tool request needs a decision ([`Escalation::request_id`]).
    Approval,
    /// The ant asked for help with the escalate tool.
    Asked,
    /// A prompt does not fit the model's context window, even in a fresh session.
    ContextWindow,
    /// The ant's harness keeps failing to start, so messages wait for a human to fix it.
    Harness,
    /// JetStream storage passed its warning share ([`storage_warning_key`]); the stuck ant is the one that noticed.
    Storage,
    /// A value from a newer daemon.
    #[serde(other)]
    Unknown,
}

impl EscalationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TurnBudget => "turn_budget",
            Self::Cap => "cap",
            Self::Notes => "notes",
            Self::DeadLetter => "dead_letter",
            Self::Approval => "approval",
            Self::Asked => "asked",
            Self::ContextWindow => "context_window",
            Self::Harness => "harness",
            Self::Storage => "storage",
            Self::Unknown => "unknown",
        }
    }
}

/// An issue an ant could not settle on a thread, as it climbs escalation chains on [`Envelope::escalation`].
///
/// Level 0 is the stuck ant itself ([`Escalation::new`]); each [`Escalation::up`] goes to the escalate_to of the
/// current leader, one level higher, and every member of that chain gets it. Only the leader resolves or escalates
/// further. The details travel in the envelope's text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Escalation {
    /// The ant that got stuck.
    pub stuck: UserId,
    /// The thread the escalation is about, the stuck ant's.
    pub thread_id: ThreadId,
    pub reason: EscalationReason,
    /// The approval request awaiting a decision, for [`EscalationReason::Approval`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    /// The chain level: 1 for the escalate_to of the thread's origin (its creator when another ant, else the stuck
    /// ant: [`Escalation::new_from`]), 0 before the escalation went anywhere.
    pub level: u32,
    /// Who leads at this level: the first of the chain it went to, the origin at level 0.
    pub leader: UserId,
    /// Every ant the escalation went through, in order, the stuck one first and the origin next when another ant.
    pub visited: Vec<UserId>,
}

/// Where an escalation goes next ([`Escalation::up`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hop {
    /// To every ant of `to`, the leader's chain without those already visited, its leader first, carrying
    /// `escalation`.
    Chain { escalation: Escalation, to: Vec<UserId> },
    /// To the thread's owner, else the default human.
    Owner(OwnerBecause),
}

/// Why an escalation goes to the thread's owner rather than up a chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerBecause {
    /// The leader has no escalate_to.
    NoChain,
    /// The next leader was already visited: the chains loop.
    Visited(UserId),
    /// The next level would pass the hop limit.
    HopLimit(u32),
}

impl Escalation {
    /// `stuck`'s issue on `thread_id`, before it goes anywhere.
    pub fn new(stuck: UserId, thread_id: ThreadId, reason: EscalationReason) -> Self {
        Self {
            leader: stuck.clone(),
            visited: vec![stuck.clone()],
            stuck,
            thread_id,
            reason,
            request_id: None,
            level: 0,
        }
    }

    /// `stuck`'s issue on `thread_id`, before it goes anywhere, from the thread's `origin`: an ant other than `stuck`
    /// that created the thread leads level 0 in its place and follows it in the ants visited, so level 1 is the
    /// origin's escalate_to and a chain back to either goes to the owner. Else the same as [`Escalation::new`].
    pub fn new_from(stuck: UserId, thread_id: ThreadId, reason: EscalationReason, origin: Option<&UserId>) -> Self {
        let mut escalation = Self::new(stuck, thread_id, reason);
        if let Some(origin) = origin.filter(|o| **o != escalation.stuck) {
            escalation.leader = origin.clone();
            escalation.visited.push(origin.clone());
        }
        escalation
    }

    /// The escalation of the tool request `request`, before it goes anywhere.
    pub fn of_request(request: &ApprovalRequest) -> Self {
        Self {
            request_id: Some(request.id),
            ..Self::new(request.ant.clone(), request.thread_id.clone(), EscalationReason::Approval)
        }
    }

    pub fn is_visited(&self, ant: &UserId) -> bool {
        self.visited.contains(ant)
    }

    pub fn is_leader(&self, ant: &UserId) -> bool {
        &self.leader == ant
    }

    /// Where the escalation goes one level up, given `chain`, the escalate_to of its [`Escalation::leader`]: up the
    /// chain, or to the owner when the chain is empty, its leader was visited, or the level would pass `max_hops`.
    /// Members already visited are left out.
    pub fn up(&self, chain: &[UserId], max_hops: u32) -> Hop {
        let Some(leader) = chain.first() else { return Hop::Owner(OwnerBecause::NoChain) };
        if self.is_visited(leader) {
            return Hop::Owner(OwnerBecause::Visited(leader.clone()));
        }
        let level = self.level.saturating_add(1);
        if level > max_hops {
            return Hop::Owner(OwnerBecause::HopLimit(max_hops));
        }
        let mut to: Vec<UserId> = Vec::with_capacity(chain.len());
        for ant in chain {
            if !self.is_visited(ant) && !to.contains(ant) {
                to.push(ant.clone());
            }
        }
        let mut visited = self.visited.clone();
        visited.extend(to.iter().cloned());
        let escalation = Self { level, leader: leader.clone(), visited, ..self.clone() };
        Hop::Chain { escalation, to }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_MAX_ESCALATION_HOPS, Envelope, Role, p};

    #[test]
    fn escalations_are_left_out_when_none_and_round_trip_otherwise() {
        let msg = Envelope::new(p("dev-a"), p("lead"), "stuck");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("escalation") && !json.contains("approval"), "{json}");
        let stuck = Escalation::new(p("dev-a"), msg.thread_id.clone(), EscalationReason::TurnBudget);
        let Hop::Chain { escalation, .. } = stuck.up(&[p("lead")], DEFAULT_MAX_ESCALATION_HOPS) else { panic!() };
        let escalated = msg.with_escalation(Some(escalation));
        let json = serde_json::to_string(&escalated).unwrap();
        assert!(
            json.contains(r#""reason":"turn_budget","level":1,"leader":"lead","visited":["dev-a","lead"]"#),
            "{json}"
        );
        assert!(!json.contains("request_id"), "{json}");
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), escalated);
        let reply = escalated.reply(p("lead"), "done", Role::Ant).unwrap();
        assert_eq!((reply.escalation, escalated.follow_up(p("dev-a"), p("dev-b"), "x").escalation), (None, None));

        let older = r#"{"id":"0190a5c4-1d2e-7f00-8000-000000000001","thread_id":"t1","from":"human","to":"dev-a",
            "text":"hi","turn_budget":8,"sent_at":"2026-01-01T00:00:00Z"}"#;
        let older = serde_json::from_str::<Envelope>(older).unwrap();
        assert_eq!((older.escalation, older.approval), (None, None));
        let newer = r#"{"stuck":"dev-a","thread_id":"t1","reason":"melted","level":2,"leader":"lead",
            "visited":["dev-a","lead"],"since":"2026-01-01T00:00:00Z"}"#;
        assert_eq!(serde_json::from_str::<Escalation>(newer).unwrap().reason, EscalationReason::Unknown);
    }

    #[test]
    fn escalations_climb_chains_until_a_loop_or_the_hop_limit_sends_them_to_the_owner() {
        let thread: ThreadId = "t1".parse().unwrap();
        let stuck = Escalation::new(p("dev-a"), thread.clone(), EscalationReason::Asked);
        assert_eq!((stuck.level, &stuck.leader, &stuck.visited[..]), (0, &p("dev-a"), &[p("dev-a")][..]));
        assert_eq!(stuck.up(&[], 3), Hop::Owner(OwnerBecause::NoChain));

        // The stuck ant in its own group is left out of the fan-out.
        let Hop::Chain { escalation: one, to } = stuck.up(&[p("lead"), p("dev-a"), p("dev-b"), p("lead")], 3) else {
            panic!("level 1 goes up the chain")
        };
        assert_eq!(to, [p("lead"), p("dev-b")]);
        assert_eq!((one.level, &one.leader, &one.stuck, &one.thread_id), (1, &p("lead"), &p("dev-a"), &thread));
        assert_eq!(one.visited, [p("dev-a"), p("lead"), p("dev-b")]);
        assert!(one.is_leader(&p("lead")) && !one.is_leader(&p("dev-b")));
        assert!(one.is_visited(&p("dev-b")) && !one.is_visited(&p("boss")));

        // A member already visited is skipped, a leader already visited is a loop.
        let Hop::Chain { escalation: two, to } = one.up(&[p("boss"), p("dev-b")], 3) else { panic!("level 2") };
        assert_eq!((two.level, &to[..]), (2, &[p("boss")][..]));
        assert_eq!(two.up(&[p("lead"), p("cto")], 3), Hop::Owner(OwnerBecause::Visited(p("lead"))));

        let Hop::Chain { escalation: three, .. } = two.up(&[p("cto")], 3) else { panic!("level 3 is within 3 hops") };
        assert_eq!(three.up(&[p("board")], 3), Hop::Owner(OwnerBecause::HopLimit(3)));
        assert_eq!(stuck.up(&[p("lead")], 0), Hop::Owner(OwnerBecause::HopLimit(0)));
    }

    #[test]
    fn an_escalation_from_the_thread_s_creator_climbs_the_creator_s_chain() {
        let thread: ThreadId = "t1".parse().unwrap();
        let reason = EscalationReason::TurnBudget;
        let new = Escalation::new(p("dev-a"), thread.clone(), reason);
        assert_eq!(Escalation::new_from(p("dev-a"), thread.clone(), reason, None), new);
        assert_eq!(Escalation::new_from(p("dev-a"), thread.clone(), reason, Some(&p("dev-a"))), new, "its own");

        let from = Escalation::new_from(p("dev-a"), thread, reason, Some(&p("lead")));
        assert_eq!((from.level, &from.stuck, &from.leader), (0, &p("dev-a"), &p("lead")));
        assert_eq!(from.visited, [p("dev-a"), p("lead")]);
        let Hop::Chain { escalation: one, to } = from.up(&[p("boss"), p("dev-a")], 3) else { panic!("level 1") };
        assert_eq!((one.level, &one.leader, &to[..]), (1, &p("boss"), &[p("boss")][..]), "the stuck ant is no member");
        assert_eq!(one.visited, [p("dev-a"), p("lead"), p("boss")]);
        // A chain back to the stuck ant or to the creator loops; the hop limit counts from the creator's chain.
        assert_eq!(from.up(&[p("dev-a")], 3), Hop::Owner(OwnerBecause::Visited(p("dev-a"))));
        assert_eq!(one.up(&[p("lead")], 3), Hop::Owner(OwnerBecause::Visited(p("lead"))));
        assert_eq!(one.up(&[p("cto")], 1), Hop::Owner(OwnerBecause::HopLimit(1)));
    }
}
