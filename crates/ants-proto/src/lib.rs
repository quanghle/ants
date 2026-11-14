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
pub const STREAM_SUBJECTS: [&str; 5] =
    ["ants.inbox.>", "ants.plan.>", "ants.approvals.>", "ants.dead.>", "ants.chan.>"];
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
/// KV bucket of the channels: one [`ChannelEntry`] per channel, keyed by the channel id. Only humans write it; personas
/// read it.
pub const CHANNELS_BUCKET: &str = "ants_channels";
/// KV bucket of the humans: one [`HumanEntry`] per human, keyed by its persona id. Only humans write it; personas read
/// it. [`PersonaId::human`] is a human without an entry.
pub const HUMANS_BUCKET: &str = "ants_humans";
/// KV bucket of the escalation chains: one [`ChainEntry`] per persona, keyed by the persona id, put by its daemon on
/// start. Unlike the roster's, entries never expire, so a chain can be followed past a persona that is offline. Each
/// persona writes only its own; every persona and human reads them.
pub const CHAINS_BUCKET: &str = "ants_chains";
/// Turns a new thread may spend on agent-to-agent replies before it goes back to a human.
pub const DEFAULT_TURN_BUDGET: u32 = 8;
/// Chain levels an escalation may climb before it goes to the thread's owner ([`Escalation::up`]).
pub const DEFAULT_MAX_ESCALATION_HOPS: u32 = 3;

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
    #[error("channel post from {from} in channel {channel} arrived on {subject}")]
    WrongChannelSubject { subject: String, from: PersonaId, channel: ChannelId },
    /// A body naming another channel than the key it was stored under.
    #[error("{kind} of channel {channel} found at {at}")]
    ForeignChannel { kind: &'static str, channel: ChannelId, at: String },
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
// A channel: a named group of personas, with the same syntax as a persona id.
token_id!(ChannelId, "channel id");
// The JetStream domain of the hub (`jetstream { domain: ... }`). Clients on a leaf node reach the hub's JetStream only
// through it, as `$JS.<domain>.API`: the plain `$JS.API` stays on the leaf.
token_id!(JsDomain, "JetStream domain");

impl PersonaId {
    /// The default human: a human whether or not [`HUMANS_BUCKET`] lists it, and the one escalations go to when a
    /// thread has no owner.
    pub fn human() -> Self {
        Self("human".to_owned())
    }

    /// The router persona that takes requests without an explicit recipient.
    pub fn messenger() -> Self {
        Self("messenger".to_owned())
    }

    /// Whether this is the default human, [`PersonaId::human`]. Other humans are known only from [`HUMANS_BUCKET`]
    /// ([`Humans`]).
    pub fn is_human(&self) -> bool {
        self.0 == "human"
    }
}

/// What a set of credentials, or the sender of a message, is: a human, with every human right, or a persona.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Human,
    Persona,
}

impl Role {
    /// The role of `persona` without the [`HUMANS_BUCKET`]: human for the default human, [`PersonaId::human`], else
    /// persona.
    pub fn by_default(persona: &PersonaId) -> Self {
        if persona.is_human() { Self::Human } else { Self::Persona }
    }

    pub fn is_human(self) -> bool {
        self == Self::Human
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

    /// Inbox prefix of the connection of `human` that sets up the machine's outbox (`ants leaf outbox`): the leaf node
    /// answers on it, so it is the machine's own, apart from the replies of any other request of that human.
    pub fn setup_inbox(&self, human: &PersonaId) -> String {
        format!("{}.leaf.{self}", inbox_prefix(human))
    }

    /// What the hub's [`STREAM`] admits from the machine's outbox for `persona` in `role`, as pairs of an
    /// [`INTAKE_STREAM`] subject filter and the subject it is stored under: exactly the inbox, plan and channel subjects
    /// the persona may publish itself ([`subject::sendable`]). With `inbox` false, only its plan and channel posts: a
    /// human's admission takes every inbox subject of a machine it is on, and the filters of one stream's sources may
    /// not overlap.
    pub fn admitted(&self, persona: &PersonaId, role: Role, inbox: bool) -> Vec<(String, String)> {
        let [own_inbox, own_plan, own_chan] = subject::sendable(persona, role);
        let patterns = if inbox { vec![own_inbox, own_plan, own_chan] } else { vec![own_plan, own_chan] };
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
    use super::{ChannelId, PersonaId, Role, ThreadId};

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

    /// A post by `from` to the notes of `thread`. Like an inbox subject it names the sender, so credentials can
    /// pin it there ([`plan_from`]).
    pub fn plan(thread: &ThreadId, from: &PersonaId) -> String {
        format!("ants.plan.{thread}.{from}")
    }

    /// Every post to the notes of `thread`, whoever wrote it.
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

    /// The notes of every thread.
    pub const ALL_PLANS: &str = "ants.plan.>";

    /// A post by `from` to `channel`. Like an inbox subject it names the sender, so credentials can pin it there
    /// ([`chan_from`]).
    pub fn chan(channel: &ChannelId, from: &PersonaId) -> String {
        format!("ants.chan.{channel}.{from}")
    }

    /// Every post to `channel`, whoever wrote it.
    pub fn chan_room(channel: &ChannelId) -> String {
        format!("ants.chan.{channel}.*")
    }

    /// Every channel subject `persona` may post on.
    pub fn chan_from(persona: &PersonaId) -> String {
        format!("ants.chan.*.{persona}")
    }

    /// The channel and the sender a channel subject names, or `None` when it is no channel subject.
    pub fn parse_chan(subject: &str) -> Option<(ChannelId, PersonaId)> {
        let (channel, from) = subject.strip_prefix("ants.chan.")?.split_once('.')?;
        Some((channel.parse().ok()?, from.parse().ok()?))
    }

    /// Every channel.
    pub const ALL_CHANNELS: &str = "ants.chan.>";

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

    /// JetStream's advisories that a consumer of the `ANTS` stream stopped delivering a message after its last allowed
    /// delivery went unacked (core NATS, not stored).
    pub fn max_deliveries_advisories() -> String {
        format!("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{}.*", super::STREAM)
    }

    /// The inbox, plan and channel subjects `persona` in `role` may publish on: every inbox for a human (to replay dead
    /// letters as they were), else [`inbox_from`]; [`plan_from`]; and [`chan_from`].
    pub fn sendable(persona: &PersonaId, role: Role) -> [String; 3] {
        let inbox = if role.is_human() { "ants.inbox.>".to_owned() } else { inbox_from(persona) };
        [inbox, plan_from(persona), chan_from(persona)]
    }

    /// Where a client on a leaf node puts an inbox, plan or channel subject (or pattern) while the hub's stream is out
    /// of reach: `ants.out.` and the rest of it, kept by the leaf's outbox ([`super::OUTBOX_STREAM`]). `None` for
    /// other subjects, which are not buffered.
    pub fn outbox(subject: &str) -> Option<String> {
        let rest = subject.strip_prefix("ants.")?;
        ["inbox.", "plan.", "chan."].iter().any(|kind| rest.starts_with(kind)).then(|| format!("ants.out.{rest}"))
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

    /// Live progress of a persona on a thread ([`StatusEvent`](super::StatusEvent)s; core NATS, not stored).
    pub fn status(persona: &PersonaId, thread: &ThreadId) -> String {
        format!("ants.status.{persona}.{thread}")
    }

    /// Live progress of a persona on every thread.
    pub fn status_of(persona: &PersonaId) -> String {
        format!("ants.status.{persona}.*")
    }

    /// The roster entry of a persona: the subject its daemon puts [`super::RosterEntry`] on.
    pub fn roster(persona: &PersonaId) -> String {
        format!("$KV.{}.{persona}", super::ROSTER_BUCKET)
    }

    /// The chain of a persona in [`super::CHAINS_BUCKET`]: the subject its daemon puts [`super::ChainEntry`] on.
    pub fn chain(persona: &PersonaId) -> String {
        format!("$KV.{}.{persona}", super::CHAINS_BUCKET)
    }

    /// The entry of a human in [`super::HUMANS_BUCKET`]: the subject a human puts [`super::HumanEntry`] on.
    pub fn human_entry(persona: &PersonaId) -> String {
        format!("$KV.{}.{persona}", super::HUMANS_BUCKET)
    }

    /// The entry of a channel in [`super::CHANNELS_BUCKET`]: the subject the human puts [`super::ChannelEntry`] on.
    pub fn channel_entry(channel: &ChannelId) -> String {
        format!("$KV.{}.{channel}", super::CHANNELS_BUCKET)
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

/// Key of what a thread has spent since the human's last message on it, and of its escalation, in [`SESSIONS_BUCKET`]
/// beside its session.
pub fn thread_spend_key(persona: &PersonaId, thread: &ThreadId) -> String {
    format!("{persona}.{thread}.spend")
}

/// Key of the stream sequence of the last post on a channel thread the persona was shown, in [`SESSIONS_BUCKET`] beside
/// the thread's session: its next prompt on the thread shows the posts after it.
pub fn channel_seen_key(persona: &PersonaId, thread: &ThreadId) -> String {
    format!("{persona}.{thread}.seen")
}

/// Key of the persona's escalation deadlines, the threads whose escalation awaits its leader until a time, in
/// [`SESSIONS_BUCKET`]. Each thread's escalation itself is kept with its spend ([`thread_spend_key`]).
pub fn escalation_deadlines_key(persona: &PersonaId) -> String {
    format!("{persona}.escalations.deadlines")
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

/// Durable JetStream consumer a persona takes channel posts from, to be woken by those meant for it. Its filter is
/// pinned under [`subject::ALL_CHANNELS`], so it cannot be pointed at an inbox.
pub fn chan_consumer(persona: &PersonaId) -> String {
    format!("chan-{persona}")
}

/// JetStream consumer a persona reads a channel (or a thread of it) back through, one read at a time, like
/// [`plan_consumer`]. Its filter is pinned under [`subject::ALL_CHANNELS`] too.
pub fn chan_reader(persona: &PersonaId) -> String {
    format!("chanread-{persona}")
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
    /// What the credentials of `persona` in `role` allow.
    ///
    /// Any persona may write to every inbox, every plan room and every channel, but only as itself (the sender is the
    /// last token of inbox, plan and channel subjects). It speaks only for itself on status, approvals and dead letters,
    /// reads only its own inbox (consumer and live), every plan room (live, and from the stream through its own plan
    /// consumer), every channel (live, and from the stream through its own channel consumer and reader), its own
    /// replies and the decisions on its own approval requests (live, and as stored), reads the roster to find personas
    /// by capability but writes only its own entry, reads every escalation chain by key but writes only its own, reads
    /// the channels and humans buckets but never writes them, and
    /// reads and writes only its own keys in the sessions and attempts buckets. The filter of each of its consumers is
    /// part of the subject that creates it, so none can be pointed at another inbox: a consumer with several filters
    /// has none in that subject, so no persona may create one. A human (a CLI user; [`Role::Human`], whatever its
    /// name) also watches all ants traffic, decides approval requests, may create and upgrade the stream and create the roster, sessions,
    /// attempts, channels, humans and chains buckets, writes the channels and humans buckets, and reads and removes dead
    /// letters by sequence; its inbox consumer keeps the replies `ants inbox` has not shown yet. A human may send under
    /// any sender, to replay a dead letter as it was. A persona gets no `STREAM.MSG.GET`: it takes any sequence, so it
    /// would expose every inbox. So only a human hears the max-deliveries advisories of the stream's consumers and
    /// stores the dead letters of the messages they name, on any persona's dead letter subject.
    ///
    /// Each inbox, plan and channel subject a persona may publish it may also publish in its outbox form
    /// ([`subject::outbox`]), which only a leaf node's outbox keeps. Only a human sets up leaf outboxes and the hub's
    /// intake: a stream's configuration can copy, transform and republish any subject of the account, so no persona
    /// may create or change one.
    pub fn for_persona(persona: &PersonaId, role: Role) -> Self {
        let consumer = inbox_consumer(persona);
        let plan = plan_consumer(persona);
        let (chan, chan_read) = (chan_consumer(persona), chan_reader(persona));
        let roster_stream = kv_stream(ROSTER_BUCKET);
        let channels = kv_stream(CHANNELS_BUCKET);
        let humans = kv_stream(HUMANS_BUCKET);
        let chains = kv_stream(CHAINS_BUCKET);
        let sessions = kv_stream(SESSIONS_BUCKET);
        let attempts = kv_stream(ATTEMPTS_BUCKET);
        let sendable = subject::sendable(persona, role);
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
            // Channel posts: its durable consumer, and reading channels back; both only ever filter channel subjects.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{chan}.{}", subject::ALL_CHANNELS),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{chan}"),
            format!("$JS.ACK.{STREAM}.{chan}.>"),
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{chan_read}.{}", subject::ALL_CHANNELS),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{chan_read}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{chan_read}"),
            format!("$JS.API.STREAM.INFO.{sessions}"),
            format!("$JS.API.STREAM.INFO.{attempts}"),
            // Reading the roster: single entries, and the ordered consumers behind listing and watching it.
            format!("$JS.API.DIRECT.GET.{roster_stream}.>"),
            format!("$JS.API.CONSUMER.CREATE.{roster_stream}"),
            format!("$JS.FC.{roster_stream}.>"),
            // Reading the channels the same way.
            format!("$JS.API.STREAM.INFO.{channels}"),
            format!("$JS.API.DIRECT.GET.{channels}.>"),
            format!("$JS.API.CONSUMER.CREATE.{channels}"),
            format!("$JS.FC.{channels}.>"),
            // And the humans.
            format!("$JS.API.STREAM.INFO.{humans}"),
            format!("$JS.API.DIRECT.GET.{humans}.>"),
            format!("$JS.API.CONSUMER.CREATE.{humans}"),
            format!("$JS.FC.{humans}.>"),
            // And single escalation chains.
            format!("$JS.API.STREAM.INFO.{chains}"),
            format!("$JS.API.DIRECT.GET.{chains}.>"),
        ]);
        let mut subscribe = vec![format!("{}.>", inbox_prefix(persona))];
        if role.is_human() {
            publish.extend([
                format!("$JS.API.STREAM.CREATE.{STREAM}"),
                // Creating a KV bucket first looks up the account's JetStream limits.
                "$JS.API.INFO".to_owned(),
                format!("$JS.API.STREAM.CREATE.{roster_stream}"),
                format!("$JS.API.STREAM.CREATE.{sessions}"),
                format!("$JS.API.STREAM.CREATE.{attempts}"),
                format!("$JS.API.STREAM.CREATE.{channels}"),
                format!("$JS.API.STREAM.CREATE.{humans}"),
                format!("$JS.API.STREAM.CREATE.{chains}"),
                // Put and delete of every channel's and every human's entry.
                format!("$KV.{CHANNELS_BUCKET}.>"),
                format!("$KV.{HUMANS_BUCKET}.>"),
                format!("$JS.API.STREAM.UPDATE.{STREAM}"),
                format!("$JS.API.STREAM.MSG.GET.{STREAM}"),
                format!("$JS.API.STREAM.MSG.DELETE.{STREAM}"),
                subject::ALL_APPROVAL_DECISIONS.to_owned(),
                subject::DEAD_ALL.to_owned(),
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
            subscribe.extend([subject::ALL.to_owned(), subject::max_deliveries_advisories()]);
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
                // Reading the decision stored on one of its requests: a direct get naming the subject in its own
                // subject returns only the last message on that subject, so nothing else is readable this way.
                format!("$JS.API.DIRECT.GET.{STREAM}.{}.*", subject::approvals(persona)),
                subject::dead(persona),
                subject::status_of(persona),
                // Put and delete of its own roster entry, and of its own escalation chain.
                subject::roster(persona),
                subject::chain(persona),
            ]);
            subscribe.extend([
                subject::ALL_PLANS.to_owned(),
                subject::ALL_CHANNELS.to_owned(),
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

    /// What the uplink credentials of `machine`'s leaf node allow: everything any of `personas` may do in its role,
    /// since every message of a persona on the machine crosses that one connection, except the outbox subjects, which
    /// stay on the leaf. Include each human that runs the CLI on the machine, as [`Role::Human`].
    ///
    /// Besides, the hub's intake may source the leaf's outbox through the machine's own JetStream domain: the hub
    /// sends the leaf requests to create, update and look up the outbox and to create and delete the consumers that
    /// read it, and flow control. The leaf delivers the outbox under [`Machine::deliver_prefix`] and answers under
    /// [`Machine::source_replies`] and the [`Machine::setup_inbox`] of the default human and of each human among
    /// `personas` only, all of them the machine's own. nats-server
    /// carries a reply across a leaf connection only if the leaf may publish it: the interest behind it reaches the
    /// leaf only then.
    ///
    /// For each pattern of a persona's inbox, plan and channel subjects, which has a wildcard before the sender, the
    /// uplink also gets one literal subject of it (`ants.inbox.dev-a.dev-a`): nats-server lets the hub stream's interest
    /// cross the uplink only when it finds such a subject, and the pattern allows it anyway.
    pub fn for_leaf<'a>(machine: &Machine, personas: impl IntoIterator<Item = (&'a PersonaId, Role)>) -> Self {
        let mut leaf = Self { publish: Vec::new(), subscribe: Vec::new() };
        let mut setup = vec![PersonaId::human()];
        for (persona, role) in personas {
            if role.is_human() && !setup.contains(persona) {
                setup.push(persona.clone());
            }
            let Self { publish, subscribe } = Self::for_persona(persona, role);
            // nats-server lets the hub stream's interest (`ants.inbox.>`, `ants.plan.>`, ...) cross the uplink only if
            // `ReverseMatch` finds a publish allowance under it, and that skips allowances with `*` before the last
            // token, such as `ants.inbox.*.dev-a`. One literal subject each of them already allows is found.
            let found = subject::sendable(persona, role).map(|s| s.replace('*', persona.as_str()));
            for (all, new) in
                [(&mut leaf.publish, publish.into_iter().chain(found).collect()), (&mut leaf.subscribe, subscribe)]
            {
                for s in new {
                    if !all.contains(&s) && !s.starts_with("ants.out.") {
                        all.push(s);
                    }
                }
            }
        }
        let api = machine.api_prefix();
        leaf.publish.extend([format!("{}.>", machine.deliver_prefix()), format!("{}.>", machine.source_replies())]);
        leaf.publish.extend(setup.iter().map(|human| format!("{}.>", machine.setup_inbox(human))));
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
    /// Whether the persona was handling a message at the last refresh. Missing from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<Activity>,
    /// Messages being handled at the last refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handling: Option<u32>,
    /// Live harness processes (sessions) at the last refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harnesses: Option<u32>,
    /// The persona's escalation chain, in order: what it cannot settle goes to these personas before a human. The first
    /// leads. Published so any daemon can follow the chain while the persona is offline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<PersonaId>,
}

/// What a persona is doing, as its roster entry reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Activity {
    /// No message in hand.
    Idle,
    /// Handling at least one message.
    Working,
    /// A value from a newer daemon.
    #[serde(other)]
    Unknown,
}

impl Activity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Unknown => "unknown",
        }
    }
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

    /// The leader of the persona's escalation chain, if it has one.
    pub fn leader(&self) -> Option<&PersonaId> {
        self.escalate_to.first()
    }
}

/// A persona's escalation chain in the [`CHAINS_BUCKET`], under the key of its persona id: its `escalate_to`, as the
/// roster entry publishes it, kept after the entry expires. Its daemon puts it on start; readers skip an entry naming
/// another persona than its key ([`ChainEntry::check_key`]) and treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainEntry {
    pub persona: PersonaId,
    /// The persona's escalation chain, in order, the first leading; empty for none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<PersonaId>,
    /// When the daemon put it.
    pub updated_at: DateTime<Utc>,
}

impl ChainEntry {
    /// The chain `escalate_to` of `persona`, put now.
    pub fn new(persona: PersonaId, escalate_to: Vec<PersonaId>) -> Self {
        Self { persona, escalate_to, updated_at: Utc::now() }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.persona.as_str()
    }

    /// Check that the entry was read from its own key: credentials pin each persona to its own key, while the body is
    /// whatever the writer put there, so an entry naming another persona is forged.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignPersona { kind: "chain entry", persona: self.persona.clone(), at: format!("key {key}") })
    }
}

/// A human's entry in the [`HUMANS_BUCKET`], under the key of its persona id. Only humans write it.
///
/// Readers should treat unknown fields as additions, not errors, and skip an entry naming another persona than its key
/// ([`HumanEntry::check_key`], [`Humans::add`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanEntry {
    pub persona: PersonaId,
    /// When the human was registered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered_at: Option<DateTime<Utc>>,
}

impl HumanEntry {
    /// The entry of `persona`, registered now.
    pub fn new(persona: PersonaId) -> Self {
        Self { persona, registered_at: Some(Utc::now()) }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.persona.as_str()
    }

    /// Check that the entry was read from its own key; a reader skips one naming another persona.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignPersona { kind: "human entry", persona: self.persona.clone(), at: format!("key {key}") })
    }
}

/// The humans of a team: [`PersonaId::human`], always, and every persona [`HUMANS_BUCKET`] lists under its own key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Humans(std::collections::BTreeSet<PersonaId>);

impl Default for Humans {
    fn default() -> Self {
        Self([PersonaId::human()].into())
    }
}

impl Humans {
    /// The default human and the humans of `entries`, as pairs of a KV key and the entry read from it. Entries naming
    /// another persona than their key are skipped.
    pub fn from_entries<'a>(entries: impl IntoIterator<Item = (&'a str, HumanEntry)>) -> Self {
        let mut humans = Self::default();
        for (key, entry) in entries {
            let _ = humans.add(key, entry);
        }
        humans
    }

    /// Add the human of `entry`, read from `key`; an entry naming another persona is refused and changes nothing.
    pub fn add(&mut self, key: &str, entry: HumanEntry) -> Result<(), Error> {
        entry.check_key(key)?;
        self.0.insert(entry.persona);
        Ok(())
    }

    /// Drop `persona`, whose entry was deleted. The default human stays a human.
    pub fn remove(&mut self, persona: &PersonaId) {
        if !persona.is_human() {
            self.0.remove(persona);
        }
    }

    pub fn contains(&self, persona: &PersonaId) -> bool {
        self.0.contains(persona)
    }

    /// The role of `persona`: [`Role::Human`] for a human, else [`Role::Persona`].
    pub fn role(&self, persona: &PersonaId) -> Role {
        if self.contains(persona) { Role::Human } else { Role::Persona }
    }

    pub fn iter(&self) -> impl Iterator<Item = &PersonaId> {
        self.0.iter()
    }
}

/// One message between personas, or between a persona and a human.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: PersonaId,
    pub to: PersonaId,
    pub text: String,
    /// Agent-to-agent replies left on this thread. At zero the thread goes back to a human: its owner, else the
    /// default one.
    pub turn_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    /// The human who started the thread. Replies and the messages an agent sends on or from the thread keep it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<PersonaId>,
    /// The escalation the message carries up a chain, or answers. Never copied by [`Envelope::reply`] or
    /// [`Envelope::follow_up`]: the daemon sets it with [`Envelope::with_escalation`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<Box<Escalation>>,
    /// An agent's decision on an approval request of `to`, for its daemon rather than its harness. The subject pins the
    /// sender, and the requester takes it only from its own chain ([`Envelope::chain_decision`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<Box<ApprovalDecision>>,
    pub sent_at: DateTime<Utc>,
}

impl Envelope {
    /// Start a new thread.
    pub fn new(from: PersonaId, to: PersonaId, text: impl Into<String>) -> Self {
        Self::on_thread(ThreadId::generate(), from, to, text)
    }

    /// A message on an existing thread, with a fresh budget and no owner.
    pub fn on_thread(thread_id: ThreadId, from: PersonaId, to: PersonaId, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            thread_id,
            from,
            to,
            text: text.into(),
            turn_budget: DEFAULT_TURN_BUDGET,
            in_reply_to: None,
            owner: None,
            escalation: None,
            approval: None,
            sent_at: Utc::now(),
        }
    }

    /// The envelope with `owner` as the human who started its thread.
    pub fn with_owner(self, owner: Option<PersonaId>) -> Self {
        Self { owner, ..self }
    }

    /// The envelope carrying `escalation`.
    pub fn with_escalation(self, escalation: Option<Escalation>) -> Self {
        Self { escalation: escalation.map(Box::new), ..self }
    }

    /// `from`'s decision on an approval request, to the requesting persona on `thread_id`, the request's thread.
    pub fn decision(thread_id: ThreadId, from: PersonaId, decision: ApprovalDecision) -> Self {
        let text = format!("{} on approval request {}", decision.verdict.as_str(), decision.request_id);
        let to = decision.persona.clone();
        Self { approval: Some(Box::new(decision)), ..Self::on_thread(thread_id, from, to, text) }
    }

    /// Whether the sender is in `chain`, the escalate_to of the persona the envelope is for. Trust it only once the
    /// envelope passed [`Envelope::check_subject`].
    pub fn is_from_chain(&self, chain: &[PersonaId]) -> bool {
        chain.contains(&self.from)
    }

    /// The approval decision the envelope carries on `request`, if it comes from `chain`, the requester's escalate_to,
    /// and is for that request. A verdict the request did not offer counts as [`Verdict::Reject`]. Trust it only once
    /// the envelope passed [`Envelope::check_subject`].
    pub fn chain_decision(&self, request: &ApprovalRequest, chain: &[PersonaId]) -> Option<ApprovalDecision> {
        let decision = self.approval.as_ref()?;
        let ours = self.to == request.persona && decision.persona == request.persona;
        (ours && decision.request_id == request.id && self.is_from_chain(chain))
            .then(|| request.decide(decision.verdict))
    }

    /// A message on this envelope's thread, with a fresh budget and the same owner.
    pub fn follow_up(&self, from: PersonaId, to: PersonaId, text: impl Into<String>) -> Self {
        Self::on_thread(self.thread_id.clone(), from, to, text).with_owner(self.owner.clone())
    }

    /// Reply to the sender, whose role is `to`, keeping the thread's owner. Replies to a human are free; agent-to-agent
    /// replies spend one turn, and fail when none are left, so the caller escalates to a human instead of looping.
    pub fn reply(&self, from: PersonaId, text: impl Into<String>, to: Role) -> Result<Self, Error> {
        let turn_budget = if to.is_human() {
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
            owner: self.owner.clone(),
            escalation: None,
            approval: None,
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

/// Why a persona escalates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationReason {
    /// The thread's turn budget is spent.
    TurnBudget,
    /// A prompt limit or a spending cap was reached.
    Cap,
    /// The thread's notes are out of turns.
    PlanRoom,
    /// A message on the thread became a dead letter.
    DeadLetter,
    /// A tool request needs a decision ([`Escalation::request_id`]).
    Approval,
    /// The agent asked for help with the escalate tool.
    Asked,
    /// A value from a newer daemon.
    #[serde(other)]
    Unknown,
}

impl EscalationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TurnBudget => "turn_budget",
            Self::Cap => "cap",
            Self::PlanRoom => "plan_room",
            Self::DeadLetter => "dead_letter",
            Self::Approval => "approval",
            Self::Asked => "asked",
            Self::Unknown => "unknown",
        }
    }
}

/// An issue a persona could not settle on a thread, as it climbs escalation chains on [`Envelope::escalation`].
///
/// Level 0 is the stuck persona itself ([`Escalation::new`]); each [`Escalation::up`] goes to the escalate_to of the
/// current leader, one level higher, and every member of that chain gets it. Only the leader resolves or escalates
/// further. The details travel in the envelope's text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Escalation {
    /// The persona that got stuck.
    pub stuck: PersonaId,
    /// The thread the escalation is about, the stuck persona's.
    pub thread_id: ThreadId,
    pub reason: EscalationReason,
    /// The approval request awaiting a decision, for [`EscalationReason::Approval`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    /// The chain level: 1 for the stuck persona's own escalate_to, 0 before the escalation went anywhere.
    pub level: u32,
    /// Who leads at this level: the first of the chain it went to, the stuck persona at level 0.
    pub leader: PersonaId,
    /// Every persona the escalation went through, in order, the stuck one first.
    pub visited: Vec<PersonaId>,
}

/// Where an escalation goes next ([`Escalation::up`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hop {
    /// To every persona of `to`, the leader's chain without those already visited, its leader first, carrying
    /// `escalation`.
    Chain { escalation: Escalation, to: Vec<PersonaId> },
    /// To the thread's owner, else the default human.
    Owner(OwnerBecause),
}

/// Why an escalation goes to the thread's owner rather than up a chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerBecause {
    /// The leader has no escalate_to.
    NoChain,
    /// The next leader was already visited: the chains loop.
    Visited(PersonaId),
    /// The next level would pass the hop limit.
    HopLimit(u32),
}

impl Escalation {
    /// `stuck`'s issue on `thread_id`, before it goes anywhere.
    pub fn new(stuck: PersonaId, thread_id: ThreadId, reason: EscalationReason) -> Self {
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

    /// The escalation of the tool request `request`, before it goes anywhere.
    pub fn of_request(request: &ApprovalRequest) -> Self {
        Self {
            request_id: Some(request.id),
            ..Self::new(request.persona.clone(), request.thread_id.clone(), EscalationReason::Approval)
        }
    }

    pub fn is_visited(&self, persona: &PersonaId) -> bool {
        self.visited.contains(persona)
    }

    pub fn is_leader(&self, persona: &PersonaId) -> bool {
        &self.leader == persona
    }

    /// Where the escalation goes one level up, given `chain`, the escalate_to of its [`Escalation::leader`]: up the
    /// chain, or to the owner when the chain is empty, its leader was visited, or the level would pass `max_hops`.
    /// Members already visited are left out.
    pub fn up(&self, chain: &[PersonaId], max_hops: u32) -> Hop {
        let Some(leader) = chain.first() else { return Hop::Owner(OwnerBecause::NoChain) };
        if self.is_visited(leader) {
            return Hop::Owner(OwnerBecause::Visited(leader.clone()));
        }
        let level = self.level.saturating_add(1);
        if level > max_hops {
            return Hop::Owner(OwnerBecause::HopLimit(max_hops));
        }
        let mut to: Vec<PersonaId> = Vec::with_capacity(chain.len());
        for persona in chain {
            if !self.is_visited(persona) && !to.contains(persona) {
                to.push(persona.clone());
            }
        }
        let mut visited = self.visited.clone();
        visited.extend(to.iter().cloned());
        let escalation = Self { level, leader: leader.clone(), visited, ..self.clone() };
        Hop::Chain { escalation, to }
    }
}

/// A post to the notes of a thread, on [`subject::plan`]: one shared board per thread, where the personas on it plan
/// together; a post wakes nobody, and each of them can read the whole board.
///
/// Posts spend the thread's turn budget like messages between agents do, and each carries what is left. The post that
/// spends the last turn hands the thread to the human, and the notes take no more agent posts until the human posts
/// there with a fresh budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPost {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: PersonaId,
    pub text: String,
    /// Turns left in the notes after this post.
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

/// The personas `text` @mentions, in the order of their first mention.
///
/// A mention is `@` and a persona id, with the `@` at the start of the text or after whitespace or an opening bracket
/// or quote, so an address such as `a@b` mentions nobody. Punctuation after the id (`,.:;!?)` and the like) ends it;
/// a letter, `_`, `@` or a dot followed by a letter or digit (`@host.example`) means it was no persona id.
pub fn parse_mentions(text: &str) -> Vec<PersonaId> {
    let mut found: Vec<PersonaId> = Vec::new();
    let mut prev: Option<char> = None;
    for (i, c) in text.char_indices() {
        if c == '@' && prev.is_none_or(|p| p.is_whitespace() || "([{<\"'`".contains(p)) {
            let rest = &text[i + 1..];
            let end =
                rest.find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')).unwrap_or(rest.len());
            let (name, after) = rest.split_at(end);
            let mut after = after.chars();
            let glued = match after.next() {
                Some(c) if c.is_alphanumeric() || c == '_' || c == '@' => true,
                Some('.') => after.next().is_some_and(char::is_alphanumeric),
                _ => false,
            };
            if !glued
                && let Ok(id) = name.parse::<PersonaId>()
                && !found.contains(&id)
            {
                found.push(id);
            }
        }
        prev = Some(c);
    }
    found
}

/// A channel's entry in the [`CHANNELS_BUCKET`], under the key of its channel id. Only humans write it.
///
/// Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelEntry {
    pub channel: ChannelId,
    /// The personas in the channel, in the order they were added.
    #[serde(default)]
    pub members: Vec<PersonaId>,
    /// The persona that triages humans' posts that @mention nobody, when not the default `messenger`
    /// ([`ChannelEntry::messenger`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messenger: Option<PersonaId>,
}

impl ChannelEntry {
    /// A channel without members or messenger.
    pub fn new(channel: ChannelId) -> Self {
        Self { channel, members: Vec::new(), messenger: None }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.channel.as_str()
    }

    /// Check that the entry was read from its own key; a reader skips one naming another channel.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignChannel { kind: "channel entry", channel: self.channel.clone(), at: format!("key {key}") })
    }

    pub fn is_member(&self, persona: &PersonaId) -> bool {
        self.members.contains(persona)
    }

    /// The persona that triages humans' posts that @mention nobody: the one assigned, or else the default
    /// [`PersonaId::messenger`]. It need not be a member.
    pub fn messenger(&self) -> PersonaId {
        self.messenger.clone().unwrap_or_else(PersonaId::messenger)
    }

    /// Add `persona` after the other members; false when it already is one.
    pub fn add_member(&mut self, persona: PersonaId) -> bool {
        if self.is_member(&persona) {
            return false;
        }
        self.members.push(persona);
        true
    }

    /// Remove `persona` from the members; false when it was none.
    pub fn remove_member(&mut self, persona: &PersonaId) -> bool {
        let before = self.members.len();
        self.members.retain(|m| m != persona);
        self.members.len() != before
    }

    /// The personas `post` wakes: the members it @mentions, except its author and the `humans`; mentions of anyone
    /// else wake nobody. A post of a human that mentions nobody wakes the channel's [`ChannelEntry::messenger`]
    /// instead, to triage it; an agent's post that mentions nobody wakes nobody. A post to another channel wakes nobody
    /// here.
    pub fn woken(&self, post: &ChannelPost, humans: &Humans) -> Vec<PersonaId> {
        if post.channel != self.channel {
            return Vec::new();
        }
        if post.mentions.is_empty() {
            return if humans.contains(&post.from) { vec![self.messenger()] } else { Vec::new() };
        }
        post.mentions.iter().filter(|m| self.is_member(m) && **m != post.from && !humans.contains(m)).cloned().collect()
    }
}

/// A post to a channel, on [`subject::chan`]: like a Slack message, either a top-level post that starts a thread or a
/// post on an existing thread.
///
/// Posts spend the thread's turn budget like messages between agents do: every agent's post spends a turn of what the
/// thread has left ([`ChannelPost::turns_left`]), and at zero the thread goes to its owner, the human whose post started
/// it, else the default human; any human's post gives the thread a fresh budget. Each post carries the turns the thread
/// has left after it, and the thread's owner when known.
///
/// Who is a human is the caller's to say, from the [`HUMANS_BUCKET`], as [`Humans`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelPost {
    pub id: Uuid,
    pub channel: ChannelId,
    /// The thread the post is on. A top-level post starts a thread with its own id ([`ChannelPost::starts_thread`]).
    pub thread_id: ThreadId,
    pub from: PersonaId,
    pub text: String,
    /// The personas the post @mentions, in order ([`parse_mentions`] of the text, when built here).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<PersonaId>,
    /// Agent-to-agent turns left on the thread after this post.
    pub turn_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    /// The human who started the thread. Replies keep it ([`ChannelPost::reply`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<PersonaId>,
    pub sent_at: DateTime<Utc>,
}

impl ChannelPost {
    /// A top-level post with a fresh budget and no owner: it starts a new thread, whose id is the post's own.
    pub fn new(channel: ChannelId, from: PersonaId, text: impl Into<String>) -> Self {
        let id = Uuid::now_v7();
        Self::build(id, channel, ThreadId(id.to_string()), from, text.into())
    }

    /// A post on an existing thread, with a fresh budget and no owner, as a human posts.
    pub fn on_thread(channel: ChannelId, thread_id: ThreadId, from: PersonaId, text: impl Into<String>) -> Self {
        Self::build(Uuid::now_v7(), channel, thread_id, from, text.into())
    }

    fn build(id: Uuid, channel: ChannelId, thread_id: ThreadId, from: PersonaId, text: String) -> Self {
        Self {
            id,
            channel,
            thread_id,
            mentions: parse_mentions(&text),
            from,
            text,
            turn_budget: DEFAULT_TURN_BUDGET,
            in_reply_to: None,
            owner: None,
            sent_at: Utc::now(),
        }
    }

    /// The post with `owner` as the human who started its thread.
    pub fn with_owner(self, owner: Option<PersonaId>) -> Self {
        Self { owner, ..self }
    }

    /// The owner of the post's thread, one of the `humans`: the one it carries, else its author, as for a post stored
    /// before posts carried owners. An owner that is no human is ignored: only a human owns a thread.
    pub fn thread_owner(&self, humans: &Humans) -> Option<PersonaId> {
        let human = |persona: &PersonaId| humans.contains(persona).then(|| persona.clone());
        self.owner.as_ref().and_then(human).or_else(|| human(&self.from))
    }

    /// The post as an agent's, with what is left of `budget`, the turns its thread has left: one turn less, whoever it
    /// wakes, which fails when none are left.
    pub fn spend(self, budget: u32) -> Result<Self, Error> {
        let turn_budget = budget.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(self.thread_id.clone()))?;
        Ok(Self { turn_budget, ..self })
    }

    /// The turns the thread has left for an answer to this post, given the posts stored on the thread `later` than it,
    /// in order: a post of one of the `humans` gives the thread a fresh budget, and any other leaves the lower of what
    /// was left and what it carries. So the agents answering one post spend from the thread's turns together, not each
    /// from the post's.
    pub fn turns_left(&self, later: &[ChannelPost], humans: &Humans) -> u32 {
        later.iter().filter(|post| post.thread_id == self.thread_id).fold(self.turn_budget, |left, post| {
            if humans.contains(&post.from) { post.turn_budget } else { left.min(post.turn_budget) }
        })
    }

    /// A reply on the same channel and thread, after the posts stored on it `later` than this one, keeping the
    /// thread's owner ([`ChannelPost::thread_owner`]); a human's reply on a thread with no known owner takes it. A
    /// human's reply has a fresh budget; an agent's spends one of the [`ChannelPost::turns_left`].
    pub fn reply(
        &self,
        from: PersonaId,
        text: impl Into<String>,
        later: &[ChannelPost],
        humans: &Humans,
    ) -> Result<Self, Error> {
        let mut reply = Self::on_thread(self.channel.clone(), self.thread_id.clone(), from, text);
        reply.in_reply_to = Some(self.id);
        reply.owner = self.thread_owner(humans).or_else(|| reply.thread_owner(humans));
        if humans.contains(&reply.from) { Ok(reply) } else { reply.spend(self.turns_left(later, humans)) }
    }

    /// Whether the post is a top-level one, which started its thread.
    pub fn starts_thread(&self) -> bool {
        self.thread_id.as_str() == self.id.to_string()
    }

    /// Whether the post @mentions `persona`.
    pub fn mentions(&self, persona: &PersonaId) -> bool {
        self.mentions.contains(persona)
    }

    /// Whether the post @mentions a persona other than its author and the `humans`: an agent it would wake.
    pub fn wakes_agents(&self, humans: &Humans) -> bool {
        self.mentions.iter().any(|m| *m != self.from && !humans.contains(m))
    }

    pub fn subject(&self) -> String {
        subject::chan(&self.channel, &self.from)
    }

    /// Check that the post arrived on its own [`ChannelPost::subject`]. Credentials pin the sender token of a channel
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongChannelSubject {
            subject: subject.to_owned(),
            from: self.from.clone(),
            channel: self.channel.clone(),
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

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AllowOnce => "allow_once",
            Self::AllowAlways => "allow_always",
            Self::Reject => "reject",
        }
    }
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

/// The answer to an [`ApprovalRequest`]: a human's, published to `ants.approvals.<persona>.<request id>`, or an agent's
/// of the persona's chain, on [`Envelope::approval`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub request_id: Uuid,
    pub persona: PersonaId,
    pub verdict: Verdict,
    pub decided_at: DateTime<Utc>,
}

impl ApprovalDecision {
    /// A decision on the request `request_id` of `persona`, made now, as an agent of its chain makes one from the
    /// [`Escalation`] alone. The requester maps a verdict it did not offer to [`Verdict::Reject`].
    pub fn new(request_id: Uuid, persona: PersonaId, verdict: Verdict) -> Self {
        Self { request_id, persona, verdict, decided_at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::approval_decision(&self.persona, &self.request_id)
    }
}

/// Where a persona stands on a thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// A prompt is running.
    Working,
    /// A tool request waits for the human's decision.
    WaitingApproval,
    /// The reply is stored; nothing runs on the thread.
    Idle,
    /// An attempt failed; it may be retried.
    Failed,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Working => "working",
            State::WaitingApproval => "waiting_approval",
            State::Idle => "idle",
            State::Failed => "failed",
        }
    }
}

/// One step of a harness's plan, as of its last [`StatusUpdate::Plan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    pub content: String,
    /// ACP plan entry status: pending, in_progress or completed.
    pub status: String,
}

/// What changed on a thread while a persona works on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StatusUpdate {
    State {
        state: State,
    },
    /// More of the reply text, as the harness streams it. Consecutive updates join up to the reply so far.
    Message {
        text: String,
    },
    /// More of the harness's reasoning, where it shares it.
    Thought {
        text: String,
    },
    /// A tool call started, or its title or status changed. Absent fields did not change.
    Tool {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// ACP tool kind, as in [`ToolCall::kind`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_kind: Option<String>,
        /// ACP tool call status: pending, in_progress, completed or failed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
    },
    /// The harness's whole plan, replacing the previous one.
    Plan {
        steps: Vec<PlanStep>,
    },
}

/// Live progress of a persona on a thread, published on [`subject::status`] with core NATS: never stored, so a
/// subscriber sees only what happens while it listens, and an update may be dropped. Readers should treat unknown
/// update types as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEvent {
    pub persona: PersonaId,
    pub thread_id: ThreadId,
    #[serde(flatten)]
    pub update: StatusUpdate,
    pub at: DateTime<Utc>,
}

impl StatusEvent {
    pub fn new(persona: PersonaId, thread_id: ThreadId, update: StatusUpdate) -> Self {
        Self { persona, thread_id, update, at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::status(&self.persona, &self.thread_id)
    }

    /// Check that the update arrived on its own [`StatusEvent::subject`]. Credentials pin each persona to its own
    /// status subjects, so an update naming another persona or thread is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignPersona {
            kind: "status update",
            persona: self.persona.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
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
        let plain = Permissions::for_persona(&p("dev-a"), Role::Persona);
        assert_eq!(plain.clone().with_js_domain(None), plain);
        let Permissions { publish, subscribe } = plain.clone().with_js_domain(Some(&domain));
        assert_eq!(subscribe, plain.subscribe);
        assert_eq!(publish[..plain.publish.len()], plain.publish, "the hub checks the mapped subjects too");
        for s in [
            "$JS.hub.API.CONSUMER.MSG.NEXT.ANTS.inbox-dev-a",
            "$JS.hub.API.DIRECT.GET.KV_ants_sessions.$KV.ants_sessions.dev-a.>",
            "$JS.hub.API.$KV.ants_sessions.dev-a.>",
            "$JS.hub.API.$KV.ants_chains.dev-a",
            "$JS.hub.API.DIRECT.GET.KV_ants_chains.>",
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
        let all = [(&a, Role::Persona), (&b, Role::Persona), (&human, Role::Human)];
        let leaf = Permissions::for_leaf(&m1, all);
        for (persona, role) in all {
            let own = Permissions::for_persona(persona, role);
            let crossing = own.publish.iter().filter(|s| !s.starts_with("ants.out."));
            assert!(crossing.into_iter().all(|s| leaf.publish.contains(s)), "{persona} publish");
            assert!(own.subscribe.iter().all(|s| leaf.subscribe.contains(s)), "{persona} subscribe");
        }
        assert_eq!(leaf.publish.iter().filter(|s| *s == "$JS.API.STREAM.INFO.ANTS").count(), 1);
        assert!(leaf.publish.iter().all(|s| !s.starts_with("ants.out.")), "the outbox stays on the leaf");
        // The hub may source the outbox of m1, and the leaf deliver it on m1's subjects only.
        let only_a = Permissions::for_leaf(&m1, [(&a, Role::Persona)]);
        let own = Permissions::for_persona(&a, Role::Persona);
        let extra: Vec<_> = only_a.publish.iter().filter(|s| !own.publish.contains(s)).collect();
        assert_eq!(
            extra,
            [
                // Already allowed by `ants.inbox.*.dev-a`, `ants.plan.*.dev-a` and `ants.chan.*.dev-a`.
                "ants.inbox.dev-a.dev-a",
                "ants.plan.dev-a.dev-a",
                "ants.chan.dev-a.dev-a",
                "$JS.S.m1.>",
                "$JSC.R.m1.*.ANTS_OUT.>",
                "_INBOX.human.leaf.m1.>"
            ]
        );
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
    fn outboxes_take_inbox_plan_and_channel_subjects_only() {
        let t: ThreadId = "t1".parse().unwrap();
        let c: ChannelId = "ops".parse().unwrap();
        assert_eq!(subject::outbox(&subject::inbox(&p("dev-a"), &p("dev-b"))).unwrap(), "ants.out.inbox.dev-a.dev-b");
        assert_eq!(subject::outbox(&subject::plan(&t, &p("dev-a"))).unwrap(), "ants.out.plan.t1.dev-a");
        assert_eq!(subject::outbox(&subject::chan(&c, &p("dev-a"))).unwrap(), "ants.out.chan.ops.dev-a");
        for other in [subject::dead(&p("dev-a")), subject::approvals(&p("dev-a")), "ants.outbox.x".to_owned()] {
            assert_eq!(subject::outbox(&other), None, "{other}");
        }
        let a = Permissions::for_persona(&p("dev-a"), Role::Persona);
        for s in ["ants.out.inbox.*.dev-a", "ants.out.plan.*.dev-a", "ants.out.chan.*.dev-a"] {
            assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
        }
        let human = Permissions::for_persona(&PersonaId::human(), Role::Human);
        assert!(human.publish.iter().any(|s| s == "ants.out.inbox.>"));
        assert!(human.publish.iter().any(|s| s == "ants.out.chan.*.human"));
    }

    #[test]
    fn the_hub_admits_from_a_machine_what_its_personas_may_send() {
        let m1: Machine = "m1".parse().unwrap();
        assert_eq!(
            m1.admitted(&p("dev-a"), Role::Persona, true),
            [
                ("ants.in.m1.ants.out.inbox.*.dev-a".to_owned(), "ants.inbox.{{wildcard(1)}}.dev-a".to_owned()),
                ("ants.in.m1.ants.out.plan.*.dev-a".to_owned(), "ants.plan.{{wildcard(1)}}.dev-a".to_owned()),
                ("ants.in.m1.ants.out.chan.*.dev-a".to_owned(), "ants.chan.{{wildcard(1)}}.dev-a".to_owned()),
            ]
        );
        assert_eq!(
            m1.admitted(&PersonaId::human(), Role::Human, true)[0],
            ("ants.in.m1.ants.out.inbox.>".to_owned(), "ants.inbox.>".to_owned())
        );
        let posts: Vec<_> =
            m1.admitted(&p("dev-a"), Role::Persona, false).into_iter().map(|(filter, _)| filter).collect();
        assert_eq!(posts, ["ants.in.m1.ants.out.plan.*.dev-a", "ants.in.m1.ants.out.chan.*.dev-a"], "posts only");
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
    fn status_events_round_trip_flat_and_name_their_subject() {
        let t: ThreadId = "t1".parse().unwrap();
        let state = StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::State { state: State::WaitingApproval });
        let json = serde_json::to_value(&state).unwrap();
        assert_eq!((json["type"].as_str(), json["state"].as_str()), (Some("state"), Some("waiting_approval")));
        assert_eq!(json["thread_id"], "t1");
        let tool = StatusEvent::new(
            p("dev-a"),
            t.clone(),
            StatusUpdate::Tool { id: "c1".into(), title: None, tool_kind: None, status: Some("completed".into()) },
        );
        let json = serde_json::to_string(&tool).unwrap();
        assert!(json.contains(r#""type":"tool""#) && !json.contains("title"), "{json}");
        for event in [state, tool] {
            let json = serde_json::to_string(&event).unwrap();
            assert_eq!(serde_json::from_str::<StatusEvent>(&json).unwrap(), event);
        }

        let chunk = StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Message { text: "hi".into() });
        assert_eq!(chunk.subject(), "ants.status.dev-a.t1");
        assert_eq!(chunk.check_subject("ants.status.dev-a.t1"), Ok(()));
        assert!(chunk.check_subject("ants.status.dev-a.t2").is_err(), "another thread");
        let forged = chunk.check_subject("ants.status.dev-b.t1").unwrap_err();
        assert_eq!(forged.to_string(), "status update of dev-a found at ants.status.dev-b.t1 (thread t1)");
        assert!(
            Permissions::for_persona(&p("dev-a"), Role::Persona).publish.contains(&subject::status_of(&p("dev-a")))
        );
    }

    #[test]
    fn reply_swaps_parties_and_links_the_message() {
        let ask = Envelope::new(PersonaId::human(), p("dev-a"), "hi");
        let reply = ask.reply(p("dev-a"), "hello", Role::Human).unwrap();
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
    fn channel_subjects_name_the_channel_and_the_sender() {
        let c: ChannelId = "ops".parse().unwrap();
        assert_eq!(subject::chan(&c, &p("dev-a")), "ants.chan.ops.dev-a");
        assert_eq!(subject::chan_room(&c), "ants.chan.ops.*");
        assert_eq!(subject::chan_from(&p("dev-a")), "ants.chan.*.dev-a");
        assert_eq!(subject::parse_chan("ants.chan.ops.dev-a"), Some((c.clone(), p("dev-a"))));
        for bad in
            ["ants.chan.ops", "ants.chan.ops.dev-a.x", "ants.plan.ops.dev-a", "ants.chan.ops.*", "ants.chan.Ops.a"]
        {
            assert_eq!(subject::parse_chan(bad), None, "{bad}");
        }
        assert!(STREAM_SUBJECTS.contains(&subject::ALL_CHANNELS), "channel posts are durable");
        assert_eq!(subject::channel_entry(&c), "$KV.ants_channels.ops");
        assert_eq!(
            (chan_consumer(&p("dev-a")), chan_reader(&p("dev-a"))),
            ("chan-dev-a".into(), "chanread-dev-a".into())
        );
        for bad in ["", "a.b", "*", ">", "Ops", "-ops", &"x".repeat(65)] {
            assert!(bad.parse::<ChannelId>().is_err(), "{bad:?} should be rejected");
        }
        assert!(serde_json::from_str::<ChannelId>(r#""a.b""#).is_err());
    }

    #[test]
    fn channel_posts_start_threads_and_are_checked_against_their_subject() {
        let c: ChannelId = "ops".parse().unwrap();
        let top = ChannelPost::new(c.clone(), PersonaId::human(), "@dev-a, @dev-b: deploy? cc @dev-a");
        assert!(top.starts_thread());
        assert_eq!(top.thread_id.as_str(), top.id.to_string());
        assert_eq!(top.mentions, [p("dev-a"), p("dev-b")]);
        assert!(top.mentions(&p("dev-b")) && !top.mentions(&p("dev-c")));
        assert_eq!((top.turn_budget, top.in_reply_to), (DEFAULT_TURN_BUDGET, None));
        assert_eq!(top.subject(), "ants.chan.ops.human");
        assert_eq!(subject::parse_chan(&top.subject()), Some((c.clone(), PersonaId::human())));
        assert_eq!(top.check_subject("ants.chan.ops.human"), Ok(()));
        let err = top.check_subject("ants.chan.ops.dev-a").unwrap_err();
        assert_eq!(err.to_string(), "channel post from human in channel ops arrived on ants.chan.ops.dev-a");
        assert!(top.check_subject("ants.chan.dev.human").is_err(), "posted to another channel");

        let two = ChannelPost::new(c.clone(), PersonaId::human(), "and this");
        assert_ne!(two.thread_id, top.thread_id, "each top-level post starts its own thread");
        assert!(two.mentions.is_empty());
        let on = ChannelPost::on_thread(c.clone(), top.thread_id.clone(), PersonaId::human(), "more");
        assert!(!on.starts_thread());
        assert_eq!(on.thread_id, top.thread_id);

        let json = serde_json::to_string(&two).unwrap();
        assert!(!json.contains("mentions") && !json.contains("in_reply_to"), "{json}");
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), two);
        let json = serde_json::to_string(&top).unwrap();
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), top);
    }

    #[test]
    fn every_agent_channel_post_spends_a_turn() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let ask = ChannelPost::new(c.clone(), PersonaId::human(), "@dev-a deploy?");
        let answer = ask.reply(p("dev-a"), "done, @human", &[], &humans).unwrap();
        assert_eq!((answer.channel.clone(), answer.thread_id.clone()), (c.clone(), ask.thread_id.clone()));
        assert_eq!(answer.in_reply_to, Some(ask.id));
        assert_eq!(answer.turn_budget, DEFAULT_TURN_BUDGET - 1, "waking nobody spends a turn too");
        let mut handoff = ask.reply(p("dev-a"), "@dev-b check it; @dev-a will wait", &[], &humans).unwrap();
        assert_eq!(handoff.turn_budget, DEFAULT_TURN_BUDGET - 1);
        handoff.turn_budget = 1;
        let back = handoff.reply(p("dev-b"), "@dev-a fine", &[], &humans).unwrap();
        assert_eq!(back.turn_budget, 0);
        let spent = Err(Error::BudgetExhausted(ask.thread_id.clone()));
        assert_eq!(back.reply(p("dev-a"), "@dev-b again", &[], &humans), spent);
        assert_eq!(back.reply(p("dev-a"), "@human over to you", &[], &humans), spent);
        let human = back.reply(PersonaId::human(), "@dev-a @dev-b go on", &[], &humans).unwrap();
        assert_eq!(human.turn_budget, DEFAULT_TURN_BUDGET, "the human's post gives a fresh budget");
        assert_eq!(
            handoff.reply(p("dev-c"), "late", &[back.clone(), human], &humans).unwrap().turn_budget,
            7,
            "reset since"
        );
        assert_eq!(
            handoff.reply(p("dev-c"), "late", &[back], &humans),
            spent,
            "an older post spends the thread's turns"
        );
        let own = ChannelPost::new(c, p("dev-a"), "@dev-b status?").spend(3).unwrap();
        assert!(own.starts_thread());
        assert_eq!(own.turn_budget, 2);
    }

    #[test]
    fn agents_woken_by_one_post_spend_the_thread_s_turns_together() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let mut ask = ChannelPost::new(c.clone(), PersonaId::human(), "@dev-a @dev-b @dev-c review");
        ask.turn_budget = 4;
        let a = ask.reply(p("dev-a"), "@dev-b @dev-c fine", &[], &humans).unwrap();
        let b = ask.reply(p("dev-b"), "@dev-a @dev-c fine", std::slice::from_ref(&a), &humans).unwrap();
        let other = ChannelPost::on_thread(c, "other".parse().unwrap(), p("dev-z"), "elsewhere");
        let c_ = ask.reply(p("dev-c"), "@dev-a @dev-b fine", &[a.clone(), other, b.clone()], &humans).unwrap();
        assert_eq!([a.turn_budget, b.turn_budget, c_.turn_budget], [3, 2, 1], "one budget, run down across branches");
        let a2 = a.reply(p("dev-a"), "@dev-b again", &[b.clone(), c_.clone()], &humans).unwrap();
        assert_eq!(a2.turn_budget, 0);
        // Every post of the thread is spent out: no branch gets another turn.
        let thread = [ask, a, b, c_, a2];
        for (i, post) in thread.iter().enumerate() {
            let spent = Err(Error::BudgetExhausted(post.thread_id.clone()));
            assert_eq!(post.reply(p("dev-b"), "more", &thread[i + 1..], &humans), spent, "after {}", post.text);
        }
    }

    #[test]
    fn mentions_are_persona_ids_after_an_at_sign_that_starts_a_word() {
        let ids = |text: &str| parse_mentions(text).into_iter().map(String::from).collect::<Vec<_>>();
        assert_eq!(ids("@dev-a"), ["dev-a"]);
        assert_eq!(
            ids("hey @dev-a, @dev-b. @ops2: (@qa) @rev! @x?; @y;"),
            ["dev-a", "dev-b", "ops2", "qa", "rev", "x", "y"]
        );
        assert_eq!(ids("@dev-b then @dev-a then @dev-b"), ["dev-b", "dev-a"], "first-seen order, no duplicates");
        assert_eq!(ids("\"@dev-a\" [@dev-b] <@dev-c>\n\t@dev-d's"), ["dev-a", "dev-b", "dev-c", "dev-d"]);
        assert!(ids("mail a@b or q.le@outlook.com").is_empty(), "addresses mention nobody");
        assert!(ids("@host.example @Dev @dev_a @a@b @ @@dev-a @-x").is_empty());
        assert_eq!(ids(&format!("@{} @ok", "x".repeat(65))), ["ok"]);
        assert_eq!(ids("end @dev-a."), ["dev-a"]);
    }

    #[test]
    fn channel_entries_list_members_and_wake_those_mentioned() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let mut entry = ChannelEntry::new(c.clone());
        assert!(entry.add_member(p("dev-a")) && entry.add_member(p("dev-b")) && entry.add_member(p("dev-c")));
        assert!(!entry.add_member(p("dev-a")), "already a member");
        assert!(entry.remove_member(&p("dev-c")) && !entry.remove_member(&p("dev-c")));
        assert_eq!(entry.members, [p("dev-a"), p("dev-b")]);
        assert!(entry.is_member(&p("dev-a")) && !entry.is_member(&p("dev-c")));
        assert_eq!(entry.key(), "ops");
        assert_eq!(entry.check_key("ops"), Ok(()));
        assert_eq!(entry.check_key("dev").unwrap_err().to_string(), "channel entry of channel ops found at key dev");

        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("messenger"), "{json}");
        assert_eq!(serde_json::from_str::<ChannelEntry>(&json).unwrap(), entry);
        let minimal: ChannelEntry = serde_json::from_str(r#"{"channel":"ops","topic":"new"}"#).unwrap();
        assert_eq!(minimal, ChannelEntry::new(c.clone()));

        let human = PersonaId::human();
        let untagged = ChannelPost::new(c.clone(), human.clone(), "anyone?");
        assert_eq!(entry.woken(&untagged, &humans), [PersonaId::messenger()], "the default messenger triages");
        entry.messenger = Some(p("dev-c"));
        assert_eq!(entry.woken(&untagged, &humans), [p("dev-c")], "the assigned one, member or not");
        assert!(
            entry.woken(&ChannelPost::new(c.clone(), p("dev-a"), "anyone?"), &humans).is_empty(),
            "agents never fan out"
        );
        let tagged = ChannelPost::new(c.clone(), p("dev-a"), "@dev-b @dev-c @dev-a @human look");
        assert_eq!(entry.woken(&tagged, &humans), [p("dev-b")], "members only, not the author or the human");
        assert!(entry.woken(&ChannelPost::new("dev".parse().unwrap(), human, "@dev-a"), &humans).is_empty());
    }

    #[test]
    fn any_human_s_posts_count_as_human_and_threads_keep_the_owner_who_started_them() {
        let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
        let mut humans = Humans::default();
        for h in [&alice, &bob] {
            humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
        }
        let mut entry = ChannelEntry::new(c.clone());
        entry.add_member(p("dev-a"));
        let top = ChannelPost::new(c.clone(), alice.clone(), "the build is red").with_owner(Some(alice.clone()));
        assert_eq!(entry.woken(&top, &humans), [PersonaId::messenger()], "alice's post goes to the messenger");
        assert!(entry.woken(&top, &Humans::default()).is_empty(), "unless alice is no human");
        let at_alice = ChannelPost::new(c.clone(), p("dev-a"), "@alice @dev-a look");
        assert!(entry.woken(&at_alice, &humans).is_empty() && !at_alice.wakes_agents(&humans));

        let answer = top.reply(p("dev-a"), "@dev-a on it", &[], &humans).unwrap();
        assert_eq!((answer.owner.as_ref(), answer.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET - 1));
        let mut spent = answer.reply(p("dev-b"), "@dev-a done", &[], &humans).unwrap();
        spent.turn_budget = 1;
        let by_bob = spent.reply(bob.clone(), "@dev-a again", &[], &humans).unwrap();
        assert_eq!((by_bob.owner.as_ref(), by_bob.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET), "fresh, alice's");
        assert_eq!(spent.turns_left(std::slice::from_ref(&by_bob), &humans), DEFAULT_TURN_BUDGET);

        // A thread from before owners: its human author owns it, else the first human to answer on it.
        let old = ChannelPost::new(c.clone(), alice.clone(), "old");
        assert_eq!(old.reply(p("dev-a"), "a", &[], &humans).unwrap().owner, Some(alice.clone()));
        let agents = ChannelPost::new(c, p("dev-a"), "@dev-b status?");
        assert_eq!(agents.reply(p("dev-b"), "b", &[], &humans).unwrap().owner, None);
        assert_eq!(agents.reply(bob.clone(), "b", &[], &humans).unwrap().owner, Some(bob));
        // Only a human owns a thread: an owner a persona names is ignored.
        let named = ChannelPost::new("ops".parse().unwrap(), p("dev-a"), "@dev-b go").with_owner(Some(p("dev-a")));
        assert_eq!(named.thread_owner(&humans), None);
        assert_eq!(named.reply(p("dev-b"), "b", &[], &humans).unwrap().owner, None);

        let json = serde_json::to_string(&old).unwrap();
        assert!(!json.contains("owner"), "{json}");
        assert!(serde_json::to_string(&top).unwrap().contains(r#""owner":"alice""#));
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), old, "posts without an owner still read");
    }

    #[test]
    fn channels_are_readable_by_personas_and_written_by_humans_only() {
        let a = Permissions::for_persona(&p("dev-a"), Role::Persona);
        for s in [
            "ants.chan.*.dev-a",
            "ants.out.chan.*.dev-a",
            "$JS.API.STREAM.INFO.KV_ants_channels",
            "$JS.API.DIRECT.GET.KV_ants_channels.>",
            "$JS.API.CONSUMER.CREATE.KV_ants_channels",
            "$JS.API.CONSUMER.CREATE.ANTS.chan-dev-a.ants.chan.>",
            "$JS.API.CONSUMER.CREATE.ANTS.chanread-dev-a.ants.chan.>",
            "$JS.ACK.ANTS.chan-dev-a.>",
        ] {
            assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
        }
        assert!(a.subscribe.iter().any(|s| s == subject::ALL_CHANNELS));
        assert!(!a.publish.iter().any(|s| s.contains("$KV.ants_channels") || s.contains("CREATE.KV_ants_channels.")));
        assert!(!a.publish.iter().any(|s| s == "ants.chan.>" || s == "$JS.API.CONSUMER.CREATE.ANTS.chan-dev-a"));
        let human = Permissions::for_persona(&PersonaId::human(), Role::Human);
        for s in ["ants.chan.*.human", "$KV.ants_channels.>", "$JS.API.STREAM.CREATE.KV_ants_channels"] {
            assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
        }
        let domain: JsDomain = "hub".parse().unwrap();
        let in_domain = human.with_js_domain(Some(&domain));
        assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_channels.>"));
        let alice = Permissions::for_persona(&p("alice"), Role::Human);
        for s in ["ants.chan.*.alice", "$KV.ants_channels.>"] {
            assert!(alice.publish.iter().any(|have| have == s), "any human: {s} missing from {:?}", alice.publish);
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
        let reply = msg.reply(p("dev-b"), "draft", Role::Persona).unwrap();
        assert_eq!(reply.turn_budget, 0);
        assert_eq!(reply.reply(p("dev-a"), "again", Role::Persona), Err(Error::BudgetExhausted(msg.thread_id.clone())));
    }

    #[test]
    fn replies_keep_the_owner_and_any_human_s_reply_is_free() {
        let alice = p("alice");
        let ask = Envelope::new(alice.clone(), p("dev-a"), "build it").with_owner(Some(alice.clone()));
        let answer = ask.reply(p("dev-a"), "built", Role::Human).unwrap();
        assert_eq!((answer.to.clone(), answer.owner.clone()), (alice.clone(), Some(alice.clone())));
        assert_eq!(answer.turn_budget, ask.turn_budget, "replying to a human spends no turn, whatever its name");
        let handoff = ask.reply(p("dev-a"), "check it", Role::Human).unwrap();
        let mut handoff = Envelope { to: p("dev-b"), from: p("dev-a"), ..handoff };
        handoff.turn_budget = 3;
        let back = handoff.reply(p("dev-b"), "fine", Role::Persona).unwrap();
        assert_eq!((back.turn_budget, back.owner.clone()), (2, Some(alice.clone())), "an agent's reply spends one");
        let notice = back.follow_up(p("dev-b"), alice.clone(), "over to you");
        assert_eq!((notice.thread_id.clone(), notice.owner.clone()), (ask.thread_id.clone(), Some(alice)));
        assert_eq!((notice.turn_budget, notice.in_reply_to), (DEFAULT_TURN_BUDGET, None));
        let ownerless = Envelope::new(p("dev-a"), p("dev-b"), "q");
        assert_eq!(ownerless.reply(p("dev-b"), "a", Role::Persona).unwrap().owner, None);
    }

    #[test]
    fn the_owner_is_left_out_when_none_and_older_envelopes_have_none() {
        let msg = Envelope::new(p("dev-a"), p("dev-b"), "q");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("owner"), "{json}");
        let owned = msg.clone().with_owner(Some(p("alice")));
        let json = serde_json::to_string(&owned).unwrap();
        assert!(json.contains(r#""owner":"alice""#), "{json}");
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), owned);
        let older = r#"{"id":"0190a5c4-1d2e-7f00-8000-000000000001","thread_id":"t1","from":"human","to":"dev-a",
            "text":"hi","turn_budget":8,"sent_at":"2026-01-01T00:00:00Z"}"#;
        assert_eq!(serde_json::from_str::<Envelope>(older).unwrap().owner, None);
    }

    #[test]
    fn humans_are_the_default_one_and_those_listed_under_their_own_key() {
        let entry = HumanEntry::new(p("alice"));
        let json = serde_json::to_string(&entry).unwrap();
        assert_eq!(serde_json::from_str::<HumanEntry>(&json).unwrap(), entry);
        let read = |json: &str| serde_json::from_str::<HumanEntry>(json).unwrap();
        let bob = read(r#"{"persona":"bob","team":"ops"}"#);
        assert_eq!(bob.registered_at, None);
        assert_eq!(bob.check_key("bob"), Ok(()));
        assert_eq!(bob.check_key("dev-a").unwrap_err().to_string(), "human entry of bob found at key dev-a");
        assert_eq!(subject::human_entry(&p("bob")), "$KV.ants_humans.bob");

        let mut humans = Humans::from_entries([("alice", entry), ("dev-a", read(r#"{"persona":"carol"}"#))]);
        assert!(humans.contains(&PersonaId::human()), "the default human needs no entry");
        assert!(humans.contains(&p("alice")));
        assert!(!humans.contains(&p("dev-a")) && !humans.contains(&p("carol")), "a mismatched entry is skipped");
        assert_eq!((humans.role(&p("alice")), humans.role(&p("dev-a"))), (Role::Human, Role::Persona));
        assert!(humans.add("dev-b", bob.clone()).is_err() && !humans.contains(&p("bob")));
        humans.add("bob", bob).unwrap();
        humans.remove(&p("bob"));
        humans.remove(&PersonaId::human());
        assert_eq!(humans.iter().map(PersonaId::as_str).collect::<Vec<_>>(), ["alice", "human"]);
    }

    #[test]
    fn every_human_has_the_human_rights_and_only_humans_write_the_humans_bucket() {
        let alice = Permissions::for_persona(&p("alice"), Role::Human);
        let human = Permissions::for_persona(&PersonaId::human(), Role::Human);
        let rename = |s: &String| s.replace(".human", ".alice").replace("-human", "-alice");
        assert_eq!(alice.publish, human.publish.iter().map(rename).collect::<Vec<_>>(), "the same rights by any name");
        assert_eq!(alice.subscribe, human.subscribe.iter().map(rename).collect::<Vec<_>>());
        for s in [
            "ants.inbox.>",
            "$JS.API.STREAM.CREATE.ANTS",
            "$JS.API.STREAM.MSG.GET.ANTS",
            "ants.approvals.*.*",
            "ants.dead.>",
            "$JS.API.STREAM.CREATE.KV_ants_humans",
            "$KV.ants_humans.>",
            "$JS.API.CONSUMER.CREATE.ANTS.inbox-alice.ants.inbox.alice.*",
            "$JS.API.CONSUMER.CREATE.ANTS.plan-alice.ants.plan.>",
        ] {
            assert!(alice.publish.iter().any(|have| have == s), "{s} missing from {:?}", alice.publish);
        }
        assert!(alice.subscribe.iter().any(|s| s == subject::ALL));

        let reads = [
            "$JS.API.STREAM.INFO.KV_ants_humans",
            "$JS.API.DIRECT.GET.KV_ants_humans.>",
            "$JS.API.CONSUMER.CREATE.KV_ants_humans",
            "$JS.FC.KV_ants_humans.>",
        ];
        let named_human = Permissions::for_persona(&PersonaId::human(), Role::Persona);
        for persona in [Permissions::for_persona(&p("dev-a"), Role::Persona), named_human] {
            for s in reads {
                assert!(persona.publish.iter().any(|have| have == s), "{s} missing from {:?}", persona.publish);
            }
            assert!(
                !persona
                    .publish
                    .iter()
                    .any(|s| s.contains("$KV.ants_humans") || s.contains("STREAM.CREATE.KV_ants_humans"))
            );
            assert!(!persona.publish.iter().any(|s| s == "ants.inbox.>" || s == "ants.dead.>"), "no human rights");
        }

        let m1: Machine = "m1".parse().unwrap();
        assert_eq!(m1.setup_inbox(&p("alice")), "_INBOX.alice.leaf.m1");
        let leaf = Permissions::for_leaf(&m1, [(&p("alice"), Role::Human), (&p("dev-a"), Role::Persona)]);
        for s in ["_INBOX.alice.leaf.m1.>", "_INBOX.human.leaf.m1.>", "$KV.ants_humans.>"] {
            assert!(leaf.publish.iter().any(|have| have == s), "{s} missing from {:?}", leaf.publish);
        }
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let msg = Envelope::new(PersonaId::human(), PersonaId::messenger(), "route this");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("in_reply_to"));
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), msg);
    }

    #[test]
    fn personas_write_only_their_own_chain_and_read_every_one() {
        let a = Permissions::for_persona(&p("dev-a"), Role::Persona);
        for s in ["$KV.ants_chains.dev-a", "$JS.API.DIRECT.GET.KV_ants_chains.>", "$JS.API.STREAM.INFO.KV_ants_chains"]
        {
            assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
        }
        assert!(!a.publish.iter().any(|s| s.starts_with("$KV.ants_chains.") && s != "$KV.ants_chains.dev-a"));
        assert!(!a.publish.iter().any(|s| s.contains("CREATE.KV_ants_chains")), "only humans create the bucket");
        let human = Permissions::for_persona(&PersonaId::human(), Role::Human);
        assert!(human.publish.iter().any(|s| s == "$JS.API.STREAM.CREATE.KV_ants_chains"));
        assert!(!human.publish.iter().any(|s| s.starts_with("$KV.ants_chains.")), "only the persona writes its chain");

        let entry = ChainEntry::new(p("dev-a"), vec![p("lead"), p("dev-b")]);
        assert_eq!(subject::chain(&entry.persona), "$KV.ants_chains.dev-a");
        let json = serde_json::to_string(&entry).unwrap();
        assert_eq!(serde_json::from_str::<ChainEntry>(&json).unwrap(), entry);
        let none = ChainEntry::new(p("dev-a"), Vec::new());
        assert!(!serde_json::to_string(&none).unwrap().contains("escalate_to"));
        assert!(entry.check_key("dev-a").is_ok());
        assert!(entry.check_key("dev-b").is_err(), "credentials vouch for the key only");
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
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("workdir") && !json.contains("activity") && !json.contains("harnesses"), "{json}");
        assert!(!json.contains("escalate_to"), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), entry);
        assert_eq!(entry.leader(), None);
        let chained = RosterEntry { escalate_to: vec![p("lead"), p("dev-b")], ..entry.clone() };
        let json = serde_json::to_string(&chained).unwrap();
        assert!(json.contains(r#""escalate_to":["lead","dev-b"]"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), chained);
        assert_eq!(chained.leader(), Some(&p("lead")));
        let busy =
            RosterEntry { activity: Some(Activity::Working), handling: Some(2), harnesses: Some(1), ..entry.clone() };
        let json = serde_json::to_string(&busy).unwrap();
        assert!(json.contains(r#""activity":"working","handling":2,"harnesses":1"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), busy);
        assert_eq!(entry.key(), "dev-a");
        assert_eq!(entry.check_key("dev-a"), Ok(()));
        assert_eq!(entry.check_key("dev-b").unwrap_err().to_string(), "roster entry of dev-a found at key dev-b");
        assert_eq!(subject::roster(&entry.persona), "$KV.ants_roster.dev-a");

        let minimal = r#"{"persona":"dev-b","machine":"m","harness":"copilot","state":"idle",
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: RosterEntry = serde_json::from_str(minimal).unwrap();
        assert!(entry.capabilities.is_empty() && entry.escalate_to.is_empty());
        assert_eq!(entry.whoami, None);
        assert_eq!((entry.activity, entry.handling, entry.harnesses), (None, None, None));

        let newer = r#"{"persona":"dev-b","machine":"m","harness":"copilot","activity":"draining","harnesses":3,
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: RosterEntry = serde_json::from_str(newer).unwrap();
        assert_eq!((entry.activity, entry.harnesses), (Some(Activity::Unknown), Some(3)));
    }

    #[test]
    fn escalations_are_left_out_when_none_and_round_trip_otherwise() {
        let msg = Envelope::new(p("dev-a"), p("lead"), "stuck");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("escalation") && !json.contains("approval"), "{json}");
        let stuck = Escalation::new(p("dev-a"), msg.thread_id.clone(), EscalationReason::TurnBudget);
        let Hop::Chain { escalation, .. } = stuck.up(&[p("lead")], DEFAULT_MAX_ESCALATION_HOPS) else { panic!() };
        let escalated = msg.clone().with_escalation(Some(escalation));
        let json = serde_json::to_string(&escalated).unwrap();
        assert!(
            json.contains(r#""reason":"turn_budget","level":1,"leader":"lead","visited":["dev-a","lead"]"#),
            "{json}"
        );
        assert!(!json.contains("request_id"), "{json}");
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), escalated);
        let reply = escalated.reply(p("lead"), "done", Role::Persona).unwrap();
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

        // The stuck persona in its own group is left out of the fan-out.
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
    fn a_requester_takes_approval_decisions_only_from_its_chain() {
        let request = ApprovalRequest::new(
            p("dev-a"),
            "t1".parse().unwrap(),
            ToolCall::default(),
            vec![Verdict::AllowOnce],
            Duration::from_secs(60),
        );
        let escalation = Escalation::of_request(&request);
        assert_eq!((escalation.reason, escalation.request_id), (EscalationReason::Approval, Some(request.id)));
        let chain = [p("lead"), p("dev-b")];

        let verdict = ApprovalDecision::new(request.id, request.persona.clone(), Verdict::AllowOnce);
        let msg = Envelope::decision(request.thread_id.clone(), p("dev-b"), verdict.clone());
        assert_eq!(msg.subject(), "ants.inbox.dev-a.dev-b", "the subject pins the decider");
        assert_eq!(msg.thread_id, request.thread_id);
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), msg);
        assert!(msg.is_from_chain(&chain));
        assert_eq!(msg.chain_decision(&request, &chain).map(|d| d.verdict), Some(Verdict::AllowOnce));

        assert_eq!(msg.chain_decision(&request, &[p("lead")]), None, "not in the chain");
        let other = ApprovalRequest { id: Uuid::now_v7(), ..request.clone() };
        assert_eq!(msg.chain_decision(&other, &chain), None, "another request");
        let foreign = ApprovalDecision::new(request.id, p("dev-c"), Verdict::AllowOnce);
        let foreign = Envelope { to: p("dev-a"), ..Envelope::decision(request.thread_id.clone(), p("lead"), foreign) };
        assert_eq!(foreign.chain_decision(&request, &chain), None, "a decision on another persona's request");
        assert_eq!(Envelope::new(p("lead"), p("dev-a"), "hi").chain_decision(&request, &chain), None);

        let always = ApprovalDecision::new(request.id, request.persona.clone(), Verdict::AllowAlways);
        let always = Envelope::decision(request.thread_id.clone(), p("lead"), always);
        let decided = always.chain_decision(&request, &chain).unwrap();
        assert_eq!(decided.verdict, Verdict::Reject, "a verdict not offered rejects");
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
