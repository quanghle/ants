//! The ants wire contract: what travels over NATS and where.
//!
//! Every subject sits under `ants.`, so one NATS account can grant a persona `ants.>` scoped to its own inbox, and a hub
//! can carry other traffic beside ants.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// JetStream stream holding every durable ants message.
pub const STREAM: &str = "ANTS";
/// Subjects captured by [`STREAM`]. Status updates are core NATS only and stay out of it.
pub const STREAM_SUBJECTS: [&str; 3] = ["ants.inbox.>", "ants.plan.>", "ants.approvals.>"];
/// KV bucket where each daemon registers its personas.
pub const ROSTER_BUCKET: &str = "ants_roster";
/// KV bucket mapping a thread to its harness session, for resuming after a restart.
pub const SESSIONS_BUCKET: &str = "ants_sessions";
/// Turns a new thread may spend on agent-to-agent replies before it goes back to the human.
pub const DEFAULT_TURN_BUDGET: u32 = 8;

/// Errors from parsing ants identifiers and building replies.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("invalid {kind} {value:?}: use 1-64 of a-z, 0-9 and '-', starting with a letter or digit")]
    InvalidId { kind: &'static str, value: String },
    #[error("thread {0} has no turns left")]
    BudgetExhausted(ThreadId),
}

/// Whether `s` is safe as one NATS subject token and as a JetStream consumer name.
fn valid_token(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
}

macro_rules! token_id {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self, Error> {
                if valid_token(s) {
                    Ok(Self(s.to_owned()))
                } else {
                    Err(Error::InvalidId { kind: $kind, value: s.to_owned() })
                }
            }
        }

        impl TryFrom<String> for $name {
            type Error = Error;
            fn try_from(s: String) -> Result<Self, Error> {
                s.parse()
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

token_id!(PersonaId, "persona id");
token_id!(ThreadId, "thread id");

impl PersonaId {
    /// The human at the terminal.
    pub fn human() -> Self {
        Self("human".to_owned())
    }

    /// The router persona that takes requests without an explicit recipient.
    pub fn messenger() -> Self {
        Self("messenger".to_owned())
    }

    pub fn is_human(&self) -> bool {
        self.0 == "human"
    }
}

impl ThreadId {
    /// A new, time-ordered thread id.
    pub fn generate() -> Self {
        Self(Uuid::now_v7().to_string())
    }
}

/// Subject names. Each takes typed ids, so a subject never carries a wildcard or a stray dot.
pub mod subject {
    use super::{PersonaId, ThreadId};

    /// Durable inbox of a persona (the human's is `ants.inbox.human`).
    pub fn inbox(persona: &PersonaId) -> String {
        format!("ants.inbox.{persona}")
    }

    /// Shared planning room of a thread.
    pub fn plan(thread: &ThreadId) -> String {
        format!("ants.plan.{thread}")
    }

    /// Permission requests a persona escalates to the human.
    pub fn approvals(persona: &PersonaId) -> String {
        format!("ants.approvals.{persona}")
    }

    /// Live progress of a persona on a thread (core NATS, not stored).
    pub fn status(persona: &PersonaId, thread: &ThreadId) -> String {
        format!("ants.status.{persona}.{thread}")
    }

    /// Everything, for `ants watch`.
    pub const ALL: &str = "ants.>";
}

/// Durable JetStream consumer name of a persona's inbox.
pub fn inbox_consumer(persona: &PersonaId) -> String {
    format!("inbox-{persona}")
}

/// Prefix of the reply subjects a persona's connections use, so its credentials need not let it read others' replies.
pub fn inbox_prefix(persona: &PersonaId) -> String {
    format!("_INBOX.{persona}")
}

/// Subjects a set of credentials may publish and subscribe to. Everything else is denied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permissions {
    pub publish: Vec<String>,
    pub subscribe: Vec<String>,
}

impl Permissions {
    /// What the credentials of `persona` allow.
    ///
    /// Any persona may write to every inbox and plan room, but speaks only for itself on status and approvals, reads
    /// only its own inbox consumer and its own replies. The human (the CLI user) also watches all ants traffic and
    /// may create the stream; its inbox consumer keeps the replies `ants inbox` has not shown yet.
    pub fn for_persona(persona: &PersonaId) -> Self {
        let consumer = inbox_consumer(persona);
        let mut publish = vec![
            "ants.inbox.*".to_owned(),
            "ants.plan.>".to_owned(),
            format!("$JS.API.STREAM.INFO.{STREAM}"),
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{consumer}"),
            // The filter is part of the subject, so the consumer cannot be pointed at another inbox.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{consumer}.{}", subject::inbox(persona)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{consumer}"),
            format!("$JS.ACK.{STREAM}.{consumer}.>"),
        ];
        let mut subscribe = vec![format!("{}.>", inbox_prefix(persona))];
        if persona.is_human() {
            publish.push(format!("$JS.API.STREAM.CREATE.{STREAM}"));
            subscribe.push(subject::ALL.to_owned());
        } else {
            publish.extend([subject::approvals(persona), format!("ants.status.{persona}.*")]);
            subscribe.push("ants.plan.>".to_owned());
        }
        Self { publish, subscribe }
    }
}

/// One message between personas, or between a persona and the human.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: PersonaId,
    pub to: PersonaId,
    pub text: String,
    /// Agent-to-agent replies left on this thread. At zero the thread goes back to the human.
    pub turn_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    pub sent_at: DateTime<Utc>,
}

impl Envelope {
    /// Start a new thread.
    pub fn new(from: PersonaId, to: PersonaId, text: impl Into<String>) -> Self {
        Self::on_thread(ThreadId::generate(), from, to, text)
    }

    /// A message on an existing thread, with a fresh budget.
    pub fn on_thread(thread_id: ThreadId, from: PersonaId, to: PersonaId, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            thread_id,
            from,
            to,
            text: text.into(),
            turn_budget: DEFAULT_TURN_BUDGET,
            in_reply_to: None,
            sent_at: Utc::now(),
        }
    }

    /// Reply to the sender. Replies to the human are free; agent-to-agent replies spend one turn, and fail when none
    /// are left, so the caller escalates to the human instead of looping.
    pub fn reply(&self, from: PersonaId, text: impl Into<String>) -> Result<Self, Error> {
        let turn_budget = if self.from.is_human() {
            self.turn_budget
        } else {
            self.turn_budget.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(self.thread_id.clone()))?
        };
        Ok(Self {
            id: Uuid::now_v7(),
            thread_id: self.thread_id.clone(),
            to: self.from.clone(),
            from,
            text: text.into(),
            turn_budget,
            in_reply_to: Some(self.id),
            sent_at: Utc::now(),
        })
    }

    pub fn subject(&self) -> String {
        subject::inbox(&self.to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> PersonaId {
        s.parse().unwrap()
    }

    #[test]
    fn ids_reject_subject_metacharacters() {
        for bad in ["", "a.b", "*", ">", "dev a", "Dev", "-dev", &"x".repeat(65)] {
            assert!(bad.parse::<PersonaId>().is_err(), "{bad:?} should be rejected");
        }
        assert_eq!(p("dev-b2").as_str(), "dev-b2");
        assert!(serde_json::from_str::<PersonaId>(r#""a.b""#).is_err());
    }

    #[test]
    fn generated_thread_ids_are_valid_tokens() {
        let t = ThreadId::generate();
        assert_eq!(t.as_str().parse::<ThreadId>().unwrap(), t);
    }

    #[test]
    fn subjects_are_scoped_under_ants() {
        let t: ThreadId = "t1".parse().unwrap();
        assert_eq!(subject::inbox(&p("dev-a")), "ants.inbox.dev-a");
        assert_eq!(subject::plan(&t), "ants.plan.t1");
        assert_eq!(subject::approvals(&p("dev-a")), "ants.approvals.dev-a");
        assert_eq!(subject::status(&p("dev-a"), &t), "ants.status.dev-a.t1");
        assert_eq!(inbox_consumer(&p("dev-a")), "inbox-dev-a");
    }

    #[test]
    fn reply_swaps_parties_and_links_the_message() {
        let ask = Envelope::new(PersonaId::human(), p("dev-a"), "hi");
        let reply = ask.reply(p("dev-a"), "hello").unwrap();
        assert_eq!((reply.from.as_str(), reply.to.as_str()), ("dev-a", "human"));
        assert_eq!(reply.thread_id, ask.thread_id);
        assert_eq!(reply.in_reply_to, Some(ask.id));
        assert_eq!(reply.turn_budget, ask.turn_budget, "replying to the human spends no turn");
        assert_eq!(reply.subject(), "ants.inbox.human");
    }

    #[test]
    fn agent_replies_spend_the_budget_until_exhausted() {
        let mut msg = Envelope::new(p("dev-a"), p("dev-b"), "plan?");
        msg.turn_budget = 1;
        let reply = msg.reply(p("dev-b"), "draft").unwrap();
        assert_eq!(reply.turn_budget, 0);
        assert_eq!(reply.reply(p("dev-a"), "again"), Err(Error::BudgetExhausted(msg.thread_id.clone())));
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let msg = Envelope::new(PersonaId::human(), PersonaId::messenger(), "route this");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("in_reply_to"));
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), msg);
    }
}
