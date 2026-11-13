//! The ants wire contract: what travels over NATS and where.
//!
//! Every subject sits under `ants.`, so one NATS account can grant a persona `ants.>` scoped to its own inbox, and a hub
//! can carry other traffic beside ants.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
pub use uuid::Uuid;

/// JetStream stream holding every durable ants message.
pub const STREAM: &str = "ANTS";
/// Subjects captured by [`STREAM`]. Status updates are core NATS only and stay out of it.
pub const STREAM_SUBJECTS: [&str; 4] = ["ants.inbox.>", "ants.plan.>", "ants.approvals.>", "ants.dead.>"];
/// JetStream stream on a machine's leaf node (in the leaf's own JetStream domain, the machine name) that keeps what
/// the machine's clients send while the hub's [`STREAM`] is out of reach, under [`subject::outbox`] subjects.
pub const OUTBOX_STREAM: &str = "ANTS_OUT";
/// Subjects captured by [`OUTBOX_STREAM`]. The leaf's uplink credentials never carry them to the hub.
pub const OUTBOX_SUBJECTS: &str = "ants.out.>";
/// Hub stream every leaf's [`OUTBOX_STREAM`] is sourced into, each under its machine's prefix
/// ([`subject::intake`]), whatever subjects the leaf claims. [`STREAM`] sources from it only the subjects the personas of
/// each machine may publish ([`Machine::admitted`]).
pub const INTAKE_STREAM: &str = "ANTS_IN";
/// KV bucket where each daemon registers its personas: one [`RosterEntry`] per persona, keyed by the persona id.
pub const ROSTER_BUCKET: &str = "ants_roster";
/// How often a daemon re-puts its roster entry.
pub const ROSTER_HEARTBEAT: Duration = Duration::from_secs(10);
/// Max age of the roster bucket: an entry not refreshed for this long is gone, so a persona whose daemon died drops
/// out of the roster after at most a few missed heartbeats.
pub const ROSTER_TTL: Duration = Duration::from_secs(30);
/// KV bucket mapping a thread to its harness session, for resuming after a restart.
pub const SESSIONS_BUCKET: &str = "ants_sessions";
/// KV bucket where a daemon marks each attempt at an inbox message it started, and the reply it stored, so a
/// redelivery knows what an earlier attempt may already have done.
pub const ATTEMPTS_BUCKET: &str = "ants_attempts";
/// Turns a new thread may spend on agent-to-agent replies before it goes back to the human.
pub const DEFAULT_TURN_BUDGET: u32 = 8;

/// Errors from parsing ants identifiers and building replies.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("invalid {kind} {value:?}: use 1-64 of a-z, 0-9 and '-', starting with a letter or digit")]
    InvalidId { kind: &'static str, value: String },
    #[error("thread {0} has no turns left")]
    BudgetExhausted(ThreadId),
    #[error("envelope from {from} to {to} arrived on {subject}")]
    WrongSubject { subject: String, from: PersonaId, to: PersonaId },
    /// A body naming another persona than the key or subject it was stored under, which credentials pin.
    #[error("{kind} of {persona} found at {at}")]
    ForeignPersona { kind: &'static str, persona: PersonaId, at: String },
    #[error("plan post from {from} on thread {thread} arrived on {subject}")]
    WrongPlanSubject { subject: String, from: PersonaId, thread: ThreadId },
    #[error("invalid machine name {0:?}: use 1-64 of letters, digits, '-' and '_', starting with a letter or digit")]
    InvalidMachine(String),
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
// The JetStream domain of the hub (`jetstream { domain: ... }`). Clients on a leaf node reach the hub's JetStream only
// through it, as `$JS.<domain>.API`: the plain `$JS.API` stays on the leaf.
token_id!(JsDomain, "JetStream domain");

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

/// A machine's name: the `server_name` and JetStream domain of its leaf node, which keeps the machine's outbox
/// ([`OUTBOX_STREAM`]), and the name of its uplink credentials. One subject token, so it can name a domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Machine(String);

impl Machine {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The JetStream API prefix of the machine's leaf node, as the hub reaches it.
    pub fn api_prefix(&self) -> String {
        format!("$JS.{self}.API")
    }

    /// Prefix of the subjects the leaf node delivers its outbox to the hub on. Each machine has its own, so its uplink
    /// credentials can feed only its own source.
    pub fn deliver_prefix(&self) -> String {
        format!("$JS.S.{self}")
    }

    /// Reply prefix of the hub's own requests for the machine's outbox, with nats-server's `js_api_reply_v2` feature
    /// flag on the hub: `$JSC.R.<domain>.<account hash>.<stream>.`, then the consumer and a unique token.
    pub fn source_replies(&self) -> String {
        format!("$JSC.R.{self}.*.{OUTBOX_STREAM}")
    }

    /// Inbox prefix of the human's connection that sets up the machine's outbox (`ants leaf outbox`): the leaf node
    /// answers on it, so it is the machine's own, apart from the replies of any other request of the human.
    pub fn setup_inbox(&self) -> String {
        format!("{}.leaf.{self}", inbox_prefix(&PersonaId::human()))
    }

    /// What the hub's [`STREAM`] admits from the machine's outbox for `persona`, as pairs of an [`INTAKE_STREAM`]
    /// subject filter and the subject it is stored under: exactly the inbox and plan subjects the persona may publish
    /// itself ([`subject::sendable`]). With `inbox` false, only its plan posts: the human's admission takes every
    /// inbox subject of a machine it is on, and the filters of one stream's sources may not overlap.
    pub fn admitted(&self, persona: &PersonaId, inbox: bool) -> Vec<(String, String)> {
        let [own_inbox, own_plan] = subject::sendable(persona);
        let patterns = if inbox { vec![own_inbox, own_plan] } else { vec![own_plan] };
        patterns
            .into_iter()
            .map(|pattern| {
                let filter = format!("ants.in.{self}.{}", subject::outbox(&pattern).expect("an outbox subject"));
                let mut n = 0;
                let destination: Vec<String> = pattern
                    .split('.')
                    .map(|token| match token {
                        "*" => {
                            n += 1;
                            format!("{{{{wildcard({n})}}}}")
                        }
                        token => token.to_owned(),
                    })
                    .collect();
                (filter, destination.join("."))
            })
            .collect()
    }
}

impl FromStr for Machine {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        let valid = (1..=64).contains(&s.len())
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            && s.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric());
        if valid { Ok(Self(s.to_owned())) } else { Err(Error::InvalidMachine(s.to_owned())) }
    }
}

impl TryFrom<String> for Machine {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Error> {
        s.parse()
    }
}

impl From<Machine> for String {
    fn from(machine: Machine) -> String {
        machine.0
    }
}

impl fmt::Display for Machine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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

    /// Durable inbox of `to`, for messages from `from`. The sender is part of the subject because credentials can pin
    /// it there ([`inbox_from`]), while anything in the payload is whatever the publisher wrote.
    pub fn inbox(to: &PersonaId, from: &PersonaId) -> String {
        format!("ants.inbox.{to}.{from}")
    }

    /// Every message to `persona`, whoever sent it (the human's is `ants.inbox.human.*`).
    pub fn inbox_of(persona: &PersonaId) -> String {
        format!("ants.inbox.{persona}.*")
    }

    /// Every inbox subject `persona` may send on.
    pub fn inbox_from(persona: &PersonaId) -> String {
        format!("ants.inbox.*.{persona}")
    }

    /// The recipient and the sender an inbox subject names, or `None` when it is no inbox subject.
    pub fn parse_inbox(subject: &str) -> Option<(PersonaId, PersonaId)> {
        let (to, from) = subject.strip_prefix("ants.inbox.")?.split_once('.')?;
        Some((to.parse().ok()?, from.parse().ok()?))
    }

    /// A post by `from` to the planning room of `thread`. Like an inbox subject it names the sender, so credentials can
    /// pin it there ([`plan_from`]).
    pub fn plan(thread: &ThreadId, from: &PersonaId) -> String {
        format!("ants.plan.{thread}.{from}")
    }

    /// Every post to the planning room of `thread`, whoever wrote it.
    pub fn plan_room(thread: &ThreadId) -> String {
        format!("ants.plan.{thread}.*")
    }

    /// Every plan subject `persona` may post on.
    pub fn plan_from(persona: &PersonaId) -> String {
        format!("ants.plan.*.{persona}")
    }

    /// The thread and the sender a plan subject names, or `None` when it is no plan subject.
    pub fn parse_plan(subject: &str) -> Option<(ThreadId, PersonaId)> {
        let (thread, from) = subject.strip_prefix("ants.plan.")?.split_once('.')?;
        Some((thread.parse().ok()?, from.parse().ok()?))
    }

    /// Every planning room.
    pub const ALL_PLANS: &str = "ants.plan.>";

    /// Permission requests a persona escalates to the human ([`ApprovalRequest`](super::ApprovalRequest)).
    pub fn approvals(persona: &PersonaId) -> String {
        format!("ants.approvals.{persona}")
    }

    /// Messages a persona gave up on, as [`DeadLetter`](super::DeadLetter)s.
    pub fn dead(persona: &PersonaId) -> String {
        format!("ants.dead.{persona}")
    }

    /// Every persona's dead letters.
    pub const DEAD_ALL: &str = "ants.dead.>";

    /// The inbox and plan subjects `persona` may publish on: every inbox for the human (to replay dead letters as they
    /// were), else [`inbox_from`]; and [`plan_from`].
    pub fn sendable(persona: &PersonaId) -> [String; 2] {
        let inbox = if persona.is_human() { "ants.inbox.>".to_owned() } else { inbox_from(persona) };
        [inbox, plan_from(persona)]
    }

    /// Where a client on a leaf node puts an inbox or plan subject (or pattern) while the hub's stream is out of
    /// reach: `ants.out.` and the rest of it, kept by the leaf's outbox ([`super::OUTBOX_STREAM`]). `None` for other
    /// subjects, which are not buffered.
    pub fn outbox(subject: &str) -> Option<String> {
        let rest = subject.strip_prefix("ants.")?;
        (rest.starts_with("inbox.") || rest.starts_with("plan.")).then(|| format!("ants.out.{rest}"))
    }

    /// Everything the hub's intake ([`super::INTAKE_STREAM`]) took from the outbox of `machine`: the leaf's subjects,
    /// whatever they are, under the machine's prefix.
    pub fn intake(machine: &super::Machine) -> String {
        format!("ants.in.{machine}.>")
    }

    /// The human's answer to one approval request ([`ApprovalDecision`](super::ApprovalDecision)). Only the human may
    /// publish here; the persona that asked subscribes before it publishes the request.
    pub fn approval_decision(persona: &PersonaId, request: &super::Uuid) -> String {
        format!("ants.approvals.{persona}.{request}")
    }

    /// Every approval request, of all personas.
    pub const ALL_APPROVAL_REQUESTS: &str = "ants.approvals.*";
    /// Every approval decision, of all personas.
    pub const ALL_APPROVAL_DECISIONS: &str = "ants.approvals.*.*";

    /// Live progress of a persona on a thread (core NATS, not stored).
    pub fn status(persona: &PersonaId, thread: &ThreadId) -> String {
        format!("ants.status.{persona}.{thread}")
    }

    /// The roster entry of a persona: the subject its daemon puts [`super::RosterEntry`] on.
    pub fn roster(persona: &PersonaId) -> String {
        format!("$KV.{}.{persona}", super::ROSTER_BUCKET)
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

/// Key of a thread's harness session in [`SESSIONS_BUCKET`]. Keys start with the persona, so its credentials can be
/// confined to its own.
pub fn session_key(persona: &PersonaId, thread: &ThreadId) -> String {
    format!("{persona}.{thread}")
}

/// Key of what a thread has spent since the human's last message on it, in [`SESSIONS_BUCKET`] beside its session.
pub fn thread_spend_key(persona: &PersonaId, thread: &ThreadId) -> String {
    format!("{persona}.{thread}.spend")
}

/// Key of what a persona has spent on `day` (`YYYY-MM-DD`, UTC), in [`SESSIONS_BUCKET`].
pub fn daily_spend_key(persona: &PersonaId, day: &str) -> String {
    format!("{persona}.day.{day}")
}

/// Key of the attempt marker of the inbox message at stream sequence `seq` in [`ATTEMPTS_BUCKET`]. Keys start with the
/// persona, so its credentials can be confined to its own.
pub fn attempt_key(persona: &PersonaId, seq: u64) -> String {
    format!("{persona}.{seq}")
}

/// JetStream consumer a persona reads plan rooms through, one room at a time. Its name is fixed, so its credentials
/// need not let it pull from other consumers.
pub fn plan_consumer(persona: &PersonaId) -> String {
    format!("plan-{persona}")
}

/// JetStream stream backing a KV bucket.
fn kv_stream(bucket: &str) -> String {
    format!("KV_{bucket}")
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
    /// Any persona may write to every inbox and every plan room, but only as itself (the sender is the last token of
    /// inbox and plan subjects). It speaks only for itself on status, approvals and dead letters, reads only its own inbox
    /// (consumer and live), every plan room (live, and from the stream through its own plan consumer), its own replies and the decisions on its own approval requests, reads the roster to find
    /// personas by capability but writes only its own entry, and reads and writes only its own keys in the sessions and
    /// attempts buckets. The human (the CLI user) also watches all ants traffic, decides approval requests, may create and
    /// upgrade the stream and create the roster, sessions and attempts buckets, and reads and removes dead letters by sequence; its inbox
    /// consumer keeps the replies `ants inbox` has not shown yet. The human may send under any sender, to replay a dead
    /// letter as it was. A persona gets no `STREAM.MSG.GET`: it takes any sequence, so it would expose every inbox.
    ///
    /// Each inbox and plan subject a persona may publish it may also publish in its outbox form
    /// ([`subject::outbox`]), which only a leaf node's outbox keeps. Only the human sets up leaf outboxes and the hub's
    /// intake: a stream's configuration can copy, transform and republish any subject of the account, so no persona
    /// may create or change one.
    pub fn for_persona(persona: &PersonaId) -> Self {
        let consumer = inbox_consumer(persona);
        let plan = plan_consumer(persona);
        let roster_stream = kv_stream(ROSTER_BUCKET);
        let sessions = kv_stream(SESSIONS_BUCKET);
        let attempts = kv_stream(ATTEMPTS_BUCKET);
        let sendable = subject::sendable(persona);
        // The same subjects in the leaf's outbox, for when the hub is out of reach.
        let outbox = sendable.iter().map(|s| subject::outbox(s).expect("an outbox subject"));
        let mut publish: Vec<String> = sendable.iter().cloned().chain(outbox).collect();
        publish.extend([
            format!("$JS.API.STREAM.INFO.{STREAM}"),
            format!("$JS.API.STREAM.INFO.{roster_stream}"),
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{consumer}"),
            // The filter is part of the subject, so the consumer cannot be pointed at another inbox.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{consumer}.{}", subject::inbox_of(persona)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{consumer}"),
            format!("$JS.ACK.{STREAM}.{consumer}.>"),
            // Reading plan rooms back through its own consumer, which only ever filters plan subjects.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{plan}.{}", subject::ALL_PLANS),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{plan}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{plan}"),
            format!("$JS.API.STREAM.INFO.{sessions}"),
            format!("$JS.API.STREAM.INFO.{attempts}"),
            // Reading the roster: single entries, and the ordered consumers behind listing and watching it.
            format!("$JS.API.DIRECT.GET.{roster_stream}.>"),
            format!("$JS.API.CONSUMER.CREATE.{roster_stream}"),
            format!("$JS.FC.{roster_stream}.>"),
        ]);
        let mut subscribe = vec![format!("{}.>", inbox_prefix(persona))];
        if persona.is_human() {
            publish.extend([
                format!("$JS.API.STREAM.CREATE.{STREAM}"),
                // Creating a KV bucket first looks up the account's JetStream limits.
                "$JS.API.INFO".to_owned(),
                format!("$JS.API.STREAM.CREATE.{roster_stream}"),
                format!("$JS.API.STREAM.CREATE.{sessions}"),
                format!("$JS.API.STREAM.CREATE.{attempts}"),
                format!("$JS.API.STREAM.UPDATE.{STREAM}"),
                format!("$JS.API.STREAM.MSG.GET.{STREAM}"),
                format!("$JS.API.STREAM.MSG.DELETE.{STREAM}"),
                subject::ALL_APPROVAL_DECISIONS.to_owned(),
                // Setting up the store and forward of leaf nodes: the hub's intake, and each leaf's outbox through the
                // leaf's own domain (the plain form is what a leaf maps its own domain to).
                format!("$JS.API.STREAM.CREATE.{INTAKE_STREAM}"),
                format!("$JS.API.STREAM.UPDATE.{INTAKE_STREAM}"),
                format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"),
            ]);
            for op in ["CREATE", "UPDATE", "INFO"] {
                publish.push(format!("$JS.API.STREAM.{op}.{OUTBOX_STREAM}"));
                publish.push(format!("$JS.*.API.STREAM.{op}.{OUTBOX_STREAM}"));
            }
            subscribe.push(subject::ALL.to_owned());
        } else {
            let own_keys = format!("$KV.{SESSIONS_BUCKET}.{persona}.>");
            let own_attempts = format!("$KV.{ATTEMPTS_BUCKET}.{persona}.>");
            publish.extend([
                // Get and put of its own sessions.
                format!("$JS.API.DIRECT.GET.{sessions}.{own_keys}"),
                own_keys,
                // Get, put and delete of the markers of its own attempts.
                format!("$JS.API.DIRECT.GET.{attempts}.{own_attempts}"),
                own_attempts,
                // Exactly its requests subject: a persona cannot answer its own requests one level below.
                subject::approvals(persona),
                subject::dead(persona),
                format!("ants.status.{persona}.*"),
                // Put and delete of its own roster entry.
                subject::roster(persona),
            ]);
            subscribe.extend([
                subject::ALL_PLANS.to_owned(),
                format!("{}.*", subject::approvals(persona)),
                // Live replies to its ask_agent calls, which the inbox consumer holds back while the asking prompt runs.
                subject::inbox_of(persona),
            ]);
        }
        Self { publish, subscribe }
    }
}

impl Permissions {
    /// The same permissions, also for clients that use the JetStream API of `domain` (`$JS.<domain>.API.…`), as every
    /// ants process does when the hub has a domain. KV puts and deletes then go through that prefix too
    /// (`$JS.<domain>.API.$KV.…`). The plain subjects stay allowed: the hub maps the domain prefix onto them and checks
    /// the mapped subject against the permissions as well. Acks, flow control and ants subjects are the same in both.
    /// Without a domain the permissions are unchanged.
    pub fn with_js_domain(mut self, domain: Option<&JsDomain>) -> Self {
        let Some(domain) = domain else { return self };
        let api = format!("$JS.{domain}.API.");
        let in_domain: Vec<String> = self
            .publish
            .iter()
            .filter_map(|s| match s.strip_prefix("$JS.API.") {
                Some(rest) => Some(format!("{api}{rest}")),
                None if s.starts_with("$KV.") => Some(format!("{api}{s}")),
                None => None,
            })
            .collect();
        self.publish.extend(in_domain);
        self
    }

    /// What the uplink credentials of `machine`'s leaf node allow: everything any of `personas` may do, since every
    /// message of a persona on the machine crosses that one connection, except the outbox subjects, which stay on the
    /// leaf. Include [`PersonaId::human`] when the machine runs the CLI.
    ///
    /// Besides, the hub's intake may source the leaf's outbox through the machine's own JetStream domain: the hub
    /// sends the leaf requests to create, update and look up the outbox and to create and delete the consumers that
    /// read it, and flow control. The leaf delivers the outbox under [`Machine::deliver_prefix`] and answers under
    /// [`Machine::source_replies`] and [`Machine::setup_inbox`] only, all of them the machine's own. nats-server
    /// carries a reply across a leaf connection only if the leaf may publish it: the interest behind it reaches the
    /// leaf only then.
    pub fn for_leaf<'a>(machine: &Machine, personas: impl IntoIterator<Item = &'a PersonaId>) -> Self {
        let mut leaf = Self { publish: Vec::new(), subscribe: Vec::new() };
        for persona in personas {
            let Self { publish, subscribe } = Self::for_persona(persona);
            for (all, new) in [(&mut leaf.publish, publish), (&mut leaf.subscribe, subscribe)] {
                for s in new {
                    if !all.contains(&s) && !s.starts_with("ants.out.") {
                        all.push(s);
                    }
                }
            }
        }
        let api = machine.api_prefix();
        leaf.publish.extend([
            format!("{}.>", machine.deliver_prefix()),
            format!("{}.>", machine.source_replies()),
            format!("{}.>", machine.setup_inbox()),
        ]);
        leaf.subscribe.extend([
            format!("{api}.STREAM.CREATE.{OUTBOX_STREAM}"),
            format!("{api}.STREAM.UPDATE.{OUTBOX_STREAM}"),
            format!("{api}.STREAM.INFO.{OUTBOX_STREAM}"),
            // One filter puts the consumer's name and filter in the subject, several leave them out.
            format!("{api}.CONSUMER.CREATE.{OUTBOX_STREAM}"),
            format!("{api}.CONSUMER.CREATE.{OUTBOX_STREAM}.>"),
            format!("{api}.CONSUMER.DELETE.{OUTBOX_STREAM}.>"),
            format!("$JS.FC.{OUTBOX_STREAM}.>"),
        ]);
        leaf
    }
}

/// A persona's entry in the [`ROSTER_BUCKET`], under the key of its persona id.
///
/// The serving daemon puts it on start, re-puts it every [`ROSTER_HEARTBEAT`] and deletes it on a clean stop. The
/// bucket's max age is [`ROSTER_TTL`], so the entry of a daemon that died expires on its own. Readers should treat
/// unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterEntry {
    pub persona: PersonaId,
    /// Host name of the machine the daemon runs on.
    pub machine: String,
    /// Agent harness behind the persona, such as `copilot`.
    pub harness: String,
    /// Free-form tags the persona is good for (`rust`, `frontend`, `billing`), for routing requests to it.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Role instructions advertised to other personas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whoami: Option<String>,
    /// Directory the harness works in, on `machine`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// When the daemon started serving the persona.
    pub started_at: DateTime<Utc>,
    /// When the daemon last refreshed the entry.
    pub heartbeat_at: DateTime<Utc>,
}

impl RosterEntry {
    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.persona.as_str()
    }

    /// Check that the entry was read from its own key. Credentials pin each persona to its own key, while the body is
    /// whatever the writer put there, so an entry naming another persona is forged.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignPersona { kind: "roster entry", persona: self.persona.clone(), at: format!("key {key}") })
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
        subject::inbox(&self.to, &self.from)
    }

    /// Check that the envelope arrived on its own [`Envelope::subject`]. Credentials pin the sender token of an inbox
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongSubject { subject: subject.to_owned(), from: self.from.clone(), to: self.to.clone() })
    }
}

/// A post to the planning room of a thread, on [`subject::plan`]. Personas on a thread plan there together; every one
/// of them reads the whole room.
///
/// Posts spend the thread's turn budget like messages between agents do, and each carries what is left. The post that
/// spends the last turn hands the thread to the human, and the room takes no more agent posts until the human posts
/// there with a fresh budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPost {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: PersonaId,
    pub text: String,
    /// Turns left in the room after this post.
    pub turn_budget: u32,
    pub sent_at: DateTime<Utc>,
}

impl PlanPost {
    /// A post with a fresh budget, as the human posts.
    pub fn new(thread_id: ThreadId, from: PersonaId, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            thread_id,
            from,
            text: text.into(),
            turn_budget: DEFAULT_TURN_BUDGET,
            sent_at: Utc::now(),
        }
    }

    /// An agent's post, spending one turn of what is left: the lower of `budget`, the turns of the message the agent
    /// is handling, and those the room's `last` post left. Fails when none are left.
    pub fn spend(
        thread_id: ThreadId,
        from: PersonaId,
        text: impl Into<String>,
        budget: u32,
        last: Option<&PlanPost>,
    ) -> Result<Self, Error> {
        let left = last.map_or(budget, |last| last.turn_budget.min(budget));
        let turn_budget = left.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(thread_id.clone()))?;
        Ok(Self { turn_budget, ..Self::new(thread_id, from, text) })
    }

    pub fn subject(&self) -> String {
        subject::plan(&self.thread_id, &self.from)
    }

    /// Check that the post arrived on its own [`PlanPost::subject`]. Credentials pin the sender token of a plan
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongPlanSubject {
            subject: subject.to_owned(),
            from: self.from.clone(),
            thread: self.thread_id.clone(),
        })
    }
}

/// A message a persona gave up on: it failed on every allowed delivery, or was not an envelope. Kept on
/// [`subject::dead`] until the human replays or drops it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// The persona whose inbox held the message.
    pub persona: PersonaId,
    /// Stream sequence of the original message.
    pub stream_seq: u64,
    /// Deliveries the message got.
    pub deliveries: u64,
    /// Why it was given up on.
    pub error: String,
    pub died_at: DateTime<Utc>,
    /// The original message, ready to replay; `None` when the payload was not an envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<Envelope>,
    /// The start of a payload that was not an envelope, for inspection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl DeadLetter {
    /// Most bytes of an unparsable payload kept in [`DeadLetter::payload`].
    pub const PAYLOAD_EXCERPT: usize = 4096;

    /// The dead letter of an envelope that failed `deliveries` times.
    pub fn of_envelope(
        persona: PersonaId,
        stream_seq: u64,
        deliveries: u64,
        error: impl Into<String>,
        envelope: Envelope,
    ) -> Self {
        Self {
            persona,
            stream_seq,
            deliveries,
            error: error.into(),
            died_at: Utc::now(),
            envelope: Some(envelope),
            payload: None,
        }
    }

    /// The dead letter of a payload that is not an envelope; keeps at most [`Self::PAYLOAD_EXCERPT`] bytes of it.
    pub fn of_payload(
        persona: PersonaId,
        stream_seq: u64,
        deliveries: u64,
        error: impl Into<String>,
        payload: &[u8],
    ) -> Self {
        // A cut through a multi-byte character leaves a replacement character, which is fine for inspection.
        let payload = String::from_utf8_lossy(&payload[..payload.len().min(Self::PAYLOAD_EXCERPT)]).into_owned();
        Self {
            persona,
            stream_seq,
            deliveries,
            error: error.into(),
            died_at: Utc::now(),
            envelope: None,
            payload: Some(payload),
        }
    }

    /// `Nats-Msg-Id` of the dead letter, so a daemon that crashes after storing it does not store it twice.
    pub fn msg_id(&self) -> String {
        format!("dead-{}", self.stream_seq)
    }

    pub fn subject(&self) -> String {
        subject::dead(&self.persona)
    }
}

/// What the human may answer to an approval request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Let the tool run this once.
    AllowOnce,
    /// Let the tool run, and let the harness remember that for the rest of its session.
    AllowAlways,
    /// Refuse the tool. Also the outcome when no decision arrives before the request expires.
    Reject,
}

/// The tool call a harness asks permission for, as the human gets to see it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolCall {
    /// ACP tool kind: read, edit, delete, move, search, execute, think, fetch, switch_mode or other.
    pub kind: String,
    pub title: String,
    /// Files the call touches.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<String>,
    /// The harness's raw input for the tool (JSON), shortened if long.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
}

/// A tool request outside a persona's allowlist, published to `ants.approvals.<persona>`. It is pending until a
/// [`ApprovalDecision`] arrives on [`ApprovalRequest::decision_subject`] or `expires_at` passes; then the persona
/// rejects it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: Uuid,
    pub persona: PersonaId,
    pub thread_id: ThreadId,
    pub tool: ToolCall,
    /// The verdicts the harness offers. [`Verdict::Reject`] is always among them.
    pub options: Vec<Verdict>,
    pub requested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ApprovalRequest {
    pub fn new(
        persona: PersonaId,
        thread_id: ThreadId,
        tool: ToolCall,
        mut options: Vec<Verdict>,
        timeout: std::time::Duration,
    ) -> Self {
        if !options.contains(&Verdict::Reject) {
            options.push(Verdict::Reject);
        }
        let requested_at = Utc::now();
        let expires_at = chrono::TimeDelta::from_std(timeout)
            .ok()
            .and_then(|t| requested_at.checked_add_signed(t))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        Self { id: Uuid::now_v7(), persona, thread_id, tool, options, requested_at, expires_at }
    }

    pub fn subject(&self) -> String {
        subject::approvals(&self.persona)
    }

    pub fn decision_subject(&self) -> String {
        subject::approval_decision(&self.persona, &self.id)
    }

    /// Check that the request arrived on its own [`ApprovalRequest::subject`]. Credentials pin each persona to its own
    /// subject, while the body is whatever the publisher wrote, so a request naming another persona is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignPersona { kind: "approval request", persona: self.persona.clone(), at: subject.to_owned() })
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// The human's answer. A verdict the harness did not offer counts as [`Verdict::Reject`].
    pub fn decide(&self, verdict: Verdict) -> ApprovalDecision {
        ApprovalDecision {
            request_id: self.id,
            persona: self.persona.clone(),
            verdict: if self.options.contains(&verdict) { verdict } else { Verdict::Reject },
            decided_at: Utc::now(),
        }
    }
}

/// The human's answer to an [`ApprovalRequest`], published to `ants.approvals.<persona>.<request id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub request_id: Uuid,
    pub persona: PersonaId,
    pub verdict: Verdict,
    pub decided_at: DateTime<Utc>,
}

impl ApprovalDecision {
    pub fn subject(&self) -> String {
        subject::approval_decision(&self.persona, &self.request_id)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    fn a_js_domain_adds_the_domain_form_of_the_jetstream_api() {
        let domain: JsDomain = "hub".parse().unwrap();
        let plain = Permissions::for_persona(&p("dev-a"));
        assert_eq!(plain.clone().with_js_domain(None), plain);
        let Permissions { publish, subscribe } = plain.clone().with_js_domain(Some(&domain));
        assert_eq!(subscribe, plain.subscribe);
        assert_eq!(publish[..plain.publish.len()], plain.publish, "the hub checks the mapped subjects too");
        for s in [
            "$JS.hub.API.CONSUMER.MSG.NEXT.ANTS.inbox-dev-a",
            "$JS.hub.API.DIRECT.GET.KV_ants_sessions.$KV.ants_sessions.dev-a.>",
            "$JS.hub.API.$KV.ants_sessions.dev-a.>",
            "$JS.hub.API.$KV.ants_roster.dev-a",
            "$JS.hub.API.CONSUMER.MSG.NEXT.ANTS.plan-dev-a",
            "$JS.ACK.ANTS.inbox-dev-a.>",
            "ants.plan.*.dev-a",
        ] {
            assert!(publish.iter().any(|have| have == s), "{s} missing from {publish:?}");
        }
        assert!("hub.x".parse::<JsDomain>().is_err());
    }

    #[test]
    fn leaf_permissions_cover_each_persona_once() {
        let (human, a, b) = (PersonaId::human(), p("dev-a"), p("dev-b"));
        let m1: Machine = "m1".parse().unwrap();
        let leaf = Permissions::for_leaf(&m1, [&a, &b, &human]);
        for persona in [&a, &b, &human] {
            let own = Permissions::for_persona(persona);
            let crossing = own.publish.iter().filter(|s| !s.starts_with("ants.out."));
            assert!(crossing.into_iter().all(|s| leaf.publish.contains(s)), "{persona} publish");
            assert!(own.subscribe.iter().all(|s| leaf.subscribe.contains(s)), "{persona} subscribe");
        }
        assert_eq!(leaf.publish.iter().filter(|s| *s == "$JS.API.STREAM.INFO.ANTS").count(), 1);
        assert!(leaf.publish.iter().all(|s| !s.starts_with("ants.out.")), "the outbox stays on the leaf");
        // The hub may source the outbox of m1, and the leaf deliver it on m1's subjects only.
        let only_a = Permissions::for_leaf(&m1, [&a]);
        let own = Permissions::for_persona(&a);
        let extra: Vec<_> = only_a.publish.iter().filter(|s| !own.publish.contains(s)).collect();
        assert_eq!(extra, ["$JS.S.m1.>", "$JSC.R.m1.*.ANTS_OUT.>", "_INBOX.human.leaf.m1.>"]);
        let extra: Vec<_> = only_a.subscribe.iter().filter(|s| !own.subscribe.contains(s)).collect();
        assert!(extra.iter().all(|s| s.starts_with("$JS.m1.API.") || s.starts_with("$JS.FC.ANTS_OUT.")), "{extra:?}");
        assert!(extra.iter().all(|s| s.contains(".ANTS_OUT")), "{extra:?}");
    }

    #[test]
    fn machine_names_are_one_token() {
        for bad in ["", "a.b", "m*", ">", "dev a", "-m", "_m", &"x".repeat(65)] {
            assert!(bad.parse::<Machine>().is_err(), "{bad:?} should be rejected");
        }
        assert_eq!("Mac_mini-2".parse::<Machine>().unwrap().as_str(), "Mac_mini-2");
    }

    #[test]
    fn outboxes_take_inbox_and_plan_subjects_only() {
        let t: ThreadId = "t1".parse().unwrap();
        assert_eq!(subject::outbox(&subject::inbox(&p("dev-a"), &p("dev-b"))).unwrap(), "ants.out.inbox.dev-a.dev-b");
        assert_eq!(subject::outbox(&subject::plan(&t, &p("dev-a"))).unwrap(), "ants.out.plan.t1.dev-a");
        for other in [subject::dead(&p("dev-a")), subject::approvals(&p("dev-a")), "ants.outbox.x".to_owned()] {
            assert_eq!(subject::outbox(&other), None, "{other}");
        }
        let a = Permissions::for_persona(&p("dev-a"));
        for s in ["ants.out.inbox.*.dev-a", "ants.out.plan.*.dev-a"] {
            assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
        }
        let human = Permissions::for_persona(&PersonaId::human());
        assert!(human.publish.iter().any(|s| s == "ants.out.inbox.>"));
    }

    #[test]
    fn the_hub_admits_from_a_machine_what_its_personas_may_send() {
        let m1: Machine = "m1".parse().unwrap();
        assert_eq!(
            m1.admitted(&p("dev-a"), true),
            [
                ("ants.in.m1.ants.out.inbox.*.dev-a".to_owned(), "ants.inbox.{{wildcard(1)}}.dev-a".to_owned()),
                ("ants.in.m1.ants.out.plan.*.dev-a".to_owned(), "ants.plan.{{wildcard(1)}}.dev-a".to_owned()),
            ]
        );
        assert_eq!(
            m1.admitted(&PersonaId::human(), true)[0],
            ("ants.in.m1.ants.out.inbox.>".to_owned(), "ants.inbox.>".to_owned())
        );
        assert_eq!(m1.admitted(&p("dev-a"), false).len(), 1, "plan posts only");
        assert_eq!(subject::intake(&m1), "ants.in.m1.>");
    }

    #[test]
    fn subjects_are_scoped_under_ants() {
        let t: ThreadId = "t1".parse().unwrap();
        assert_eq!(subject::inbox(&p("dev-a"), &PersonaId::human()), "ants.inbox.dev-a.human");
        assert_eq!(subject::inbox_of(&p("dev-a")), "ants.inbox.dev-a.*");
        assert_eq!(subject::inbox_from(&p("dev-a")), "ants.inbox.*.dev-a");
        assert_eq!(subject::plan(&t, &p("dev-a")), "ants.plan.t1.dev-a");
        assert_eq!(subject::plan_room(&t), "ants.plan.t1.*");
        assert_eq!(subject::plan_from(&p("dev-a")), "ants.plan.*.dev-a");
        assert!(STREAM_SUBJECTS.contains(&subject::ALL_PLANS), "plan rooms are durable");
        assert_eq!(subject::approvals(&p("dev-a")), "ants.approvals.dev-a");
        assert_eq!(subject::status(&p("dev-a"), &t), "ants.status.dev-a.t1");
        assert_eq!(subject::dead(&p("dev-a")), "ants.dead.dev-a");
        assert!(STREAM_SUBJECTS.contains(&subject::DEAD_ALL), "dead letters are durable");
        assert_eq!(inbox_consumer(&p("dev-a")), "inbox-dev-a");
        let id = Uuid::now_v7();
        assert_eq!(subject::approval_decision(&p("dev-a"), &id), format!("ants.approvals.dev-a.{id}"));
        assert_eq!(session_key(&p("dev-a"), &t), "dev-a.t1");
    }

    #[test]
    fn only_offered_verdicts_are_decided() {
        let tool = ToolCall { kind: "execute".into(), title: "rm -rf build".into(), ..Default::default() };
        let ask = ApprovalRequest::new(
            p("dev-a"),
            "t1".parse().unwrap(),
            tool,
            vec![Verdict::AllowOnce],
            Duration::from_secs(60),
        );
        assert_eq!(ask.options, [Verdict::AllowOnce, Verdict::Reject]);
        assert!(!ask.is_expired(ask.requested_at) && ask.is_expired(ask.expires_at));
        assert_eq!(ask.decide(Verdict::AllowOnce).verdict, Verdict::AllowOnce);
        let always = ask.decide(Verdict::AllowAlways);
        assert_eq!(always.verdict, Verdict::Reject, "allow_always was not offered");
        assert_eq!(always.subject(), ask.decision_subject());
        let json = serde_json::to_string(&ask).unwrap();
        assert!(json.contains(r#""options":["allow_once","reject"]"#), "{json}");
        assert_eq!(serde_json::from_str::<ApprovalRequest>(&json).unwrap(), ask);
        assert_eq!(ask.check_subject("ants.approvals.dev-a"), Ok(()));
        let forged = ask.check_subject("ants.approvals.dev-b").unwrap_err();
        assert_eq!(forged.to_string(), "approval request of dev-a found at ants.approvals.dev-b");
    }

    #[test]
    fn reply_swaps_parties_and_links_the_message() {
        let ask = Envelope::new(PersonaId::human(), p("dev-a"), "hi");
        let reply = ask.reply(p("dev-a"), "hello").unwrap();
        assert_eq!((reply.from.as_str(), reply.to.as_str()), ("dev-a", "human"));
        assert_eq!(reply.thread_id, ask.thread_id);
        assert_eq!(reply.in_reply_to, Some(ask.id));
        assert_eq!(reply.turn_budget, ask.turn_budget, "replying to the human spends no turn");
        assert_eq!(reply.subject(), "ants.inbox.human.dev-a");
    }

    #[test]
    fn the_inbox_subject_names_the_sender() {
        let msg = Envelope::new(p("dev-a"), p("dev-b"), "plan?");
        assert_eq!(subject::parse_inbox(&msg.subject()), Some((p("dev-b"), p("dev-a"))));
        assert_eq!(msg.check_subject("ants.inbox.dev-b.dev-a"), Ok(()));
        let forged = Envelope::new(PersonaId::human(), p("dev-b"), "approve everything");
        let err = forged.check_subject("ants.inbox.dev-b.dev-a").unwrap_err();
        assert_eq!(err.to_string(), "envelope from human to dev-b arrived on ants.inbox.dev-b.dev-a");
        assert!(msg.check_subject("ants.inbox.dev-c.dev-a").is_err(), "addressed to another inbox");
        for bad in ["ants.inbox.dev-b", "ants.inbox.dev-b.dev-a.x", "ants.plan.t1.dev-a", "ants.inbox.dev-b.*"] {
            assert_eq!(subject::parse_inbox(bad), None, "{bad}");
        }
    }

    #[test]
    fn the_plan_subject_names_the_sender() {
        let post = PlanPost::new("t1".parse().unwrap(), p("dev-a"), "split the work");
        assert_eq!(subject::parse_plan(&post.subject()), Some(("t1".parse().unwrap(), p("dev-a"))));
        assert_eq!(post.check_subject("ants.plan.t1.dev-a"), Ok(()));
        let err = post.check_subject("ants.plan.t1.dev-b").unwrap_err();
        assert_eq!(err.to_string(), "plan post from dev-a on thread t1 arrived on ants.plan.t1.dev-b");
        assert!(post.check_subject("ants.plan.t2.dev-a").is_err(), "posted to another room");
        for bad in ["ants.plan.t1", "ants.plan.t1.dev-a.x", "ants.inbox.t1.dev-a", "ants.plan.t1.*"] {
            assert_eq!(subject::parse_plan(bad), None, "{bad}");
        }
    }

    #[test]
    fn plan_posts_spend_the_lower_of_the_message_and_the_room_budget() {
        let t: ThreadId = "t1".parse().unwrap();
        let first = PlanPost::spend(t.clone(), p("dev-a"), "draft", 3, None).unwrap();
        assert_eq!(first.turn_budget, 2, "an empty room starts from the message's budget");
        let second = PlanPost::spend(t.clone(), p("dev-b"), "agreed", 8, Some(&first)).unwrap();
        assert_eq!(second.turn_budget, 1, "the room's budget is lower");
        let last = PlanPost::spend(t.clone(), p("dev-a"), "done", 1, Some(&second)).unwrap();
        assert_eq!(last.turn_budget, 0);
        let refused = PlanPost::spend(t.clone(), p("dev-b"), "more", 8, Some(&last));
        assert_eq!(refused, Err(Error::BudgetExhausted(t.clone())));
        let human = PlanPost::new(t.clone(), PersonaId::human(), "go on");
        assert_eq!(
            PlanPost::spend(t, p("dev-b"), "more", 8, Some(&human)).unwrap().turn_budget,
            DEFAULT_TURN_BUDGET - 1
        );
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

    #[test]
    fn roster_entry_round_trips_and_tolerates_additions() {
        let entry = RosterEntry {
            persona: p("dev-a"),
            machine: "build-1".to_owned(),
            harness: "copilot".to_owned(),
            capabilities: vec!["rust".to_owned(), "nats".to_owned()],
            whoami: Some("Maintain the Rust backend.\nReview NATS changes.".to_owned()),
            workdir: None,
            started_at: Utc::now(),
            heartbeat_at: Utc::now(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("workdir"));
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), entry);
        assert_eq!(entry.key(), "dev-a");
        assert_eq!(entry.check_key("dev-a"), Ok(()));
        assert_eq!(entry.check_key("dev-b").unwrap_err().to_string(), "roster entry of dev-a found at key dev-b");
        assert_eq!(subject::roster(&entry.persona), "$KV.ants_roster.dev-a");

        let minimal = r#"{"persona":"dev-b","machine":"m","harness":"copilot","state":"idle",
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: RosterEntry = serde_json::from_str(minimal).unwrap();
        assert!(entry.capabilities.is_empty());
        assert_eq!(entry.whoami, None);
    }

    #[test]
    fn dead_letters_keep_the_envelope_or_an_excerpt_of_the_payload() {
        let msg = Envelope::new(PersonaId::human(), p("dev-a"), "build it");
        let dead = DeadLetter::of_envelope(p("dev-a"), 7, 5, "harness crashed", msg.clone());
        assert_eq!((dead.subject().as_str(), dead.msg_id().as_str()), ("ants.dead.dev-a", "dead-7"));
        let json = serde_json::to_string(&dead).unwrap();
        assert!(!json.contains("payload"));
        assert_eq!(serde_json::from_str::<DeadLetter>(&json).unwrap().envelope, Some(msg));

        let junk = vec![b'x'; DeadLetter::PAYLOAD_EXCERPT + 10];
        let dead = DeadLetter::of_payload(p("dev-a"), 8, 1, "not an envelope", &junk);
        assert_eq!(dead.envelope, None);
        assert_eq!(dead.payload.unwrap().len(), DeadLetter::PAYLOAD_EXCERPT);
    }
}
