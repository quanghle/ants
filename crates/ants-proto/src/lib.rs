//! The ants wire contract: what travels over NATS and where.
//!
//! Every subject sits under `ants.`, so one NATS account can grant a user `ants.>` scoped to its own inbox, and a hub
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
    ["ants.inbox.>", "ants.notes.>", "ants.approvals.>", "ants.dead.>", "ants.chan.>"];
/// The inbox subjects [`STREAM`] republishes each message it stores on, to [`STORED_INBOX_DESTINATION`]: what it
/// sources from a leaf's outbox it stores without publishing it on its subject.
pub const STORED_INBOX_SOURCE: &str = "ants.inbox.*.*";
/// Where [`STREAM`] republishes a stored inbox message: the recipient's reply inboxes ([`subject::stored_inbox`]), which
/// its credentials already read and no credentials publish to. The headers keep the original subject. The server also
/// answers a direct get, and delivers a push consumer's messages, there for any client that names the subject, with
/// headers that client stored: only a message without a reply subject, and with a single `Nats-Stream` and a single
/// `Nats-Subject` header, the stream's own, is one it republished.
pub const STORED_INBOX_DESTINATION: &str = "_INBOX.{{wildcard(1)}}.stored.{{wildcard(2)}}";
/// JetStream stream on a machine's leaf node (in the leaf's own JetStream domain, the machine name) that keeps what
/// the machine's clients send while the hub's [`STREAM`] is out of reach, under [`subject::outbox`] subjects.
pub const OUTBOX_STREAM: &str = "ANTS_OUT";
/// Subjects captured by [`OUTBOX_STREAM`]. The leaf's uplink credentials never carry them to the hub.
pub const OUTBOX_SUBJECTS: &str = "ants.out.>";
/// Hub stream every leaf's [`OUTBOX_STREAM`] is sourced into, each under its machine's prefix
/// ([`subject::intake`]), whatever subjects the leaf claims. [`STREAM`] sources from it only the subjects the users of
/// each machine may publish ([`Machine::admitted`]).
pub const INTAKE_STREAM: &str = "ANTS_IN";
/// KV bucket where each daemon registers its ants: one [`RosterEntry`] per ant, keyed by its id.
pub const ROSTER_BUCKET: &str = "ants_roster";
/// How often a daemon re-puts its roster entry.
pub const ROSTER_HEARTBEAT: Duration = Duration::from_secs(10);
/// Max age of the roster bucket: an entry not refreshed for this long is gone, so an ant whose daemon died drops
/// out of the roster after at most a few missed heartbeats.
pub const ROSTER_TTL: Duration = Duration::from_secs(30);
/// KV bucket mapping a thread to its harness session, for resuming after a restart.
pub const SESSIONS_BUCKET: &str = "ants_sessions";
/// KV bucket where a daemon marks each attempt at an inbox message it started, and the reply it stored, so a
/// redelivery knows what an earlier attempt may already have done.
pub const ATTEMPTS_BUCKET: &str = "ants_attempts";
/// KV bucket of the channels: one [`ChannelEntry`] per channel, keyed by the channel id. Humans write it, and the
/// channel manager on the requests of a channel's owner ([`ChannelAdminRequest`]); ants read it.
pub const CHANNELS_BUCKET: &str = "ants_channels";
/// How long a [`ChannelAdminRequest`] stays valid after it is made.
pub const CHANADM_TTL: Duration = Duration::from_secs(60);
/// How often an ant repeats a [`ChannelAdminRequest`] while no answer arrives, and for how long in all. Requests are
/// core NATS, so one sent while no channel manager listens is lost.
pub const CHANADM_REPEAT: Duration = Duration::from_secs(2);
pub const CHANADM_WAIT: Duration = Duration::from_secs(10);
/// Most operations one [`ChannelAdminRequest`] may carry.
pub const MAX_CHANNEL_OPS: usize = 32;
/// KV bucket of the humans: one [`HumanEntry`] per human, keyed by its user id. Only humans write it; ants read
/// it. [`UserId::human`] is a human without an entry.
pub const HUMANS_BUCKET: &str = "ants_humans";
/// KV bucket of the escalation chains: one [`ChainEntry`] per ant, keyed by its id, put by its daemon on
/// start. Unlike the roster's, entries never expire, so a chain can be followed past an ant that is offline. Each
/// ant writes only its own; every ant and human reads them.
pub const CHAINS_BUCKET: &str = "ants_chains";
/// KV bucket of the links to threads ants passed on ([`link_key`]): an ant whose message carries a link to a thread
/// ([`Envelope::origin_thread`]), or is on a thread with an owner or creator, records, under its own keys, that it
/// passed the link to that thread on to the recipient ([`Envelope::passes_on`]), so a daemon can check the link of a
/// message from an ant that takes no part in that thread itself. Each ant writes only its own keys; every ant and human
/// reads them all. Entries expire with the stream's messages.
pub const LINKS_BUCKET: &str = "ants_links";
/// KV bucket of the owners of the threads humans and webhooks message ants on, keyed by thread id and recipient
/// ([`owner_key`]): the human that each human's or webhook's message to an ant on the thread names
/// ([`Envelope::owner`]), put before that message is sent, or kept in the machine's outbox until the bucket sources it
/// ([`owner_outbox`]). Only humans and webhooks write it, a webhook for its own ant destinations only, every credential
/// reads it by key, and the first record of a thread to an ant stays: its stream refuses a second record of a key,
/// drops one carrying the first's [`owner_record_id`] within its retention, so none sourced from an outbox is refused,
/// and denies deletes and purges. Daemons take the owner of an ant's message on a thread no channel post started from
/// the record to that ant, or to an ant that passed the link to the thread on to it. Records expire after the
/// stream's retention.
pub const OWNERS_BUCKET: &str = "ants_owners";
/// KV bucket of the events each webhook delivered, keyed `<webhook>.<hash of the event id>` ([`webhook_seen_key`]),
/// so a retried event is not delivered again, of each webhook's marker ([`webhook_marker_key`]), which tells
/// daemons the webhook is one, and of the channel manager's [`Serving`] ([`channel_manager_serving_key`]). Each
/// webhook writes and reads only its own keys, ants read only the markers, humans the markers and the channel
/// manager's key; entries expire after [`WEBHOOK_RETENTION`].
pub const WEBHOOKS_BUCKET: &str = "ants_webhooks";
/// How long [`WEBHOOKS_BUCKET`] remembers an event.
pub const WEBHOOK_RETENTION: Duration = Duration::from_hours(7 * 24);
/// KV bucket of each human's [`ReadMarker`], keyed by its user id: which inbox messages the human has read, on any
/// of its devices. Each human reads and writes only its own key ([`Permissions::for_user`]); entries never expire.
pub const READ_BUCKET: &str = "ants_read";
/// KV bucket of where each human is reading: one [`Presence`] per device of a human following its inbox or the tool
/// requests, keyed by [`presence_key`], put when the following starts, renewed every [`PRESENCE_HEARTBEAT`] and
/// deleted when it ends. Kept in memory with a max age of [`PRESENCE_TTL`], so the presence of a CLI that died expires
/// on its own. Each human reads, by direct gets, and writes only its own keys ([`Permissions::for_user`]); `ants
/// webhook serve` reads them so a notice skips the push targets of a device reading it already.
pub const PRESENCE_BUCKET: &str = "ants_presence";
/// KV bucket of what each human holds back of its notifications for a while ([`NotifyPrefs`]), keyed by its user id: a
/// snooze and the muted threads, set with `ants notify` and `ants thread mute` on any of its devices and read by `ants
/// webhook serve` before each notice. Each human reads and writes only its own key ([`Permissions::for_user`]); entries
/// never expire, what they hold ends at the deadlines it carries.
pub const NOTIFY_BUCKET: &str = "ants_notify";
/// KV bucket of the reactions to messages: one [`ReactionEntry`] per reactor, message and reaction, keyed by
/// [`reaction_key`]. Each user writes, and deletes, only its own keys ([`Permissions::for_user`]), a human's devices sharing
/// the human's; only humans read the others'. Its stream denies deletes, purges and rollups, so no one wipes the others'
/// reactions, and drops each after the stream's retention, as it does messages. Reactions never reach an inbox: no ant
/// wakes for one, and no reply wait ends with one.
pub const REACTIONS_BUCKET: &str = "ants_reactions";
/// KV bucket of what humans make of threads: the title of each thread ([`ThreadTitle`], keyed by [`title_key`]), one
/// for every human, which any human sets, renames or clears, and each human's pinned and archived threads
/// ([`ThreadMarks`], keyed by [`marks_key`]), shared by its devices and read and written by that human alone
/// ([`Permissions::for_user`]). Ants read and write none of it. Entries never expire.
pub const THREADS_BUCKET: &str = "ants_threads";
/// Most characters a thread title has.
pub const MAX_TITLE: usize = 120;
/// How often a following CLI re-puts its device's [`Presence`].
pub const PRESENCE_HEARTBEAT: Duration = Duration::from_secs(10);
/// Max age of [`PRESENCE_BUCKET`]: a presence not renewed for this long is gone.
pub const PRESENCE_TTL: Duration = Duration::from_secs(30);
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
    WrongSubject { subject: String, from: UserId, to: UserId },
    /// A body naming another user than the key or subject it was stored under, which credentials pin.
    #[error("{kind} of {user} found at {at}")]
    ForeignUser { kind: &'static str, user: UserId, at: String },
    #[error("notes post from {from} on thread {thread} arrived on {subject}")]
    WrongNotesSubject { subject: String, from: UserId, thread: ThreadId },
    #[error("channel post from {from} in channel {channel} arrived on {subject}")]
    WrongChannelSubject { subject: String, from: UserId, channel: ChannelId },
    /// A body naming another channel than the key it was stored under.
    #[error("{kind} of channel {channel} found at {at}")]
    ForeignChannel { kind: &'static str, channel: ChannelId, at: String },
    #[error("invalid machine name {0:?}: use 1-64 of letters, digits, '-' and '_', starting with a letter or digit")]
    InvalidMachine(String),
    #[error("invalid thread title {0:?}: use one line of 1-{MAX_TITLE} characters")]
    InvalidTitle(String),
    /// A renewal request naming other credentials than the subject it arrived on, or no credentials renewable over
    /// NATS ([`subject::renewable_name`]).
    #[error("renewal request of {name:?} found at {at}")]
    ForeignRenewal { name: String, at: String },
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

token_id!(UserId, "user id");
token_id!(ThreadId, "thread id");
// A channel: a named group of ants, with the same syntax as a user id.
token_id!(ChannelId, "channel id");
// A reaction to a message, such as `eyes` or `check`: one subject token, with the same syntax as a user id.
token_id!(Reaction, "reaction");
// The JetStream domain of the hub (`jetstream { domain: ... }`). Clients on a leaf node reach the hub's JetStream only
// through it, as `$JS.<domain>.API`: the plain `$JS.API` stays on the leaf.
token_id!(JsDomain, "JetStream domain");

impl UserId {
    /// The default human: a human whether or not [`HUMANS_BUCKET`] lists it, and the one escalations go to when a
    /// thread has no owner.
    pub fn human() -> Self {
        Self("human".to_owned())
    }

    /// The router ant that takes requests without an explicit recipient.
    pub fn messenger() -> Self {
        Self("messenger".to_owned())
    }

    /// The channel manager (`ants channel serve`), which posts in a channel what it changed there. A reserved name:
    /// no ant may take it.
    pub fn channel_manager() -> Self {
        Self("channel-manager".to_owned())
    }

    /// Whether this is the default human, [`UserId::human`]. Other humans are known only from [`HUMANS_BUCKET`]
    /// ([`Humans`]).
    pub fn is_human(&self) -> bool {
        self.0 == "human"
    }
}

/// What a set of credentials, or the sender of a message, is: a human, with every human right, or an ant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Human,
    Ant,
}

impl Role {
    /// The role of `user` without the [`HUMANS_BUCKET`]: human for the default human, [`UserId::human`], else
    /// ant.
    pub fn by_default(user: &UserId) -> Self {
        if user.is_human() { Self::Human } else { Self::Ant }
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
    pub fn setup_inbox(&self, human: &UserId) -> String {
        format!("{}.leaf.{self}", inbox_prefix(human))
    }

    /// What the hub's [`STREAM`] admits from the machine's outbox for `user` in `role`, as pairs of an
    /// [`INTAKE_STREAM`] subject filter and the subject it is stored under: exactly the inbox, notes and channel
    /// subjects the user may publish itself ([`subject::sendable`]), and for an ant its own dead letters.
    /// With `inbox` false, no inbox subjects: a human's admission takes every inbox subject of a machine it is on, and
    /// the filters of one stream's sources may not overlap. A human's dead letters, of any ant, are written from
    /// what only the hub holds, so none are buffered.
    pub fn admitted(&self, user: &UserId, role: Role, inbox: bool) -> Vec<(String, String)> {
        let [own_inbox, own_notes, own_chan] = subject::sendable(user, role);
        let mut patterns = if inbox { vec![own_inbox, own_notes, own_chan] } else { vec![own_notes, own_chan] };
        if !role.is_human() {
            patterns.push(subject::dead(user));
        }
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

    /// What the hub's [`LINKS_BUCKET`] admits from the machine's outbox for ant `user`, as an [`INTAKE_STREAM`] subject
    /// filter and the subject it is stored under: exactly the links `user` passed on ([`link_outbox`]), under its own
    /// keys.
    pub fn admitted_links(&self, user: &UserId) -> (String, String) {
        let filter = format!("ants.in.{self}.ants.out.links.{user}.*.*");
        (filter, format!("$KV.{LINKS_BUCKET}.{user}.{{{{wildcard(1)}}}}.{{{{wildcard(2)}}}}"))
    }

    /// What the hub's [`OWNERS_BUCKET`] admits from the machine's outbox when a human runs on it, as an
    /// [`INTAKE_STREAM`] subject filter and the subject it is stored under: the owner records its humans kept there
    /// ([`owner_outbox`]), under their keys.
    pub fn admitted_owners(&self) -> (String, String) {
        let filter = format!("ants.in.{self}.ants.out.owners.*.*");
        (filter, format!("$KV.{OWNERS_BUCKET}.{{{{wildcard(1)}}}}.{{{{wildcard(2)}}}}"))
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

pub mod duration;

/// Subject names. Each takes typed ids, so a subject never carries a wildcard or a stray dot.
pub mod subject {
    use super::{ChannelId, Machine, Role, ThreadId, UserId};

    /// Durable inbox of `to`, for messages from `from`. The sender is part of the subject because credentials can pin
    /// it there ([`inbox_from`]), while anything in the payload is whatever the publisher wrote.
    pub fn inbox(to: &UserId, from: &UserId) -> String {
        format!("ants.inbox.{to}.{from}")
    }

    /// Every message to `user`, whoever sent it (the human's is `ants.inbox.human.*`).
    pub fn inbox_of(user: &UserId) -> String {
        format!("ants.inbox.{user}.*")
    }

    /// Every message to `user` as the hub's stream stored it, republished there ([`super::STORED_INBOX_SOURCE`]).
    pub fn stored_inbox(user: &UserId) -> String {
        format!("{}.stored.*", super::inbox_prefix(user))
    }

    /// The sender a subject of [`stored_inbox`] of `user` names, or `None` when it is none of them.
    pub fn parse_stored_inbox(user: &UserId, subject: &str) -> Option<UserId> {
        subject.strip_prefix(&super::inbox_prefix(user))?.strip_prefix(".stored.")?.parse().ok()
    }

    /// Every inbox subject `user` may send on.
    pub fn inbox_from(user: &UserId) -> String {
        format!("ants.inbox.*.{user}")
    }

    /// The recipient and the sender an inbox subject names, or `None` when it is no inbox subject.
    pub fn parse_inbox(subject: &str) -> Option<(UserId, UserId)> {
        let (to, from) = subject.strip_prefix("ants.inbox.")?.split_once('.')?;
        Some((to.parse().ok()?, from.parse().ok()?))
    }

    /// A post by `from` to the notes of `thread`. Like an inbox subject it names the sender, so credentials can
    /// pin it there ([`notes_from`]).
    pub fn notes(thread: &ThreadId, from: &UserId) -> String {
        format!("ants.notes.{thread}.{from}")
    }

    /// Every post to the notes of `thread`, whoever wrote it.
    pub fn notes_of(thread: &ThreadId) -> String {
        format!("ants.notes.{thread}.*")
    }

    /// Every notes subject `user` may post on.
    pub fn notes_from(user: &UserId) -> String {
        format!("ants.notes.*.{user}")
    }

    /// The thread and the sender a notes subject names, or `None` when it is no notes subject.
    pub fn parse_notes(subject: &str) -> Option<(ThreadId, UserId)> {
        let (thread, from) = subject.strip_prefix("ants.notes.")?.split_once('.')?;
        Some((thread.parse().ok()?, from.parse().ok()?))
    }

    /// The notes of every thread.
    pub const ALL_NOTES: &str = "ants.notes.>";

    /// A post by `from` to `channel`. Like an inbox subject it names the sender, so credentials can pin it there
    /// ([`chan_from`]).
    pub fn chan(channel: &ChannelId, from: &UserId) -> String {
        format!("ants.chan.{channel}.{from}")
    }

    /// Every post to `channel`, whoever wrote it.
    pub fn chan_room(channel: &ChannelId) -> String {
        format!("ants.chan.{channel}.*")
    }

    /// Every channel subject `user` may post on.
    pub fn chan_from(user: &UserId) -> String {
        format!("ants.chan.*.{user}")
    }

    /// The channel and the sender a channel subject names, or `None` when it is no channel subject.
    pub fn parse_chan(subject: &str) -> Option<(ChannelId, UserId)> {
        let (channel, from) = subject.strip_prefix("ants.chan.")?.split_once('.')?;
        Some((channel.parse().ok()?, from.parse().ok()?))
    }

    /// Every channel.
    pub const ALL_CHANNELS: &str = "ants.chan.>";

    /// Where `ant` asks the channel manager for changes to `channel` ([`ChannelAdminRequest`](super::ChannelAdminRequest);
    /// core NATS, not stored). Like an inbox subject it names the sender, so credentials can pin it there.
    pub fn chanadm(channel: &ChannelId, ant: &UserId) -> String {
        format!("ants.chanadm.{channel}.{ant}")
    }

    /// Every channel management subject `ant` may ask on.
    pub fn chanadm_from(ant: &UserId) -> String {
        format!("ants.chanadm.*.{ant}")
    }

    /// The channel and the ant a channel management subject names, or `None` when it is none.
    pub fn parse_chanadm(subject: &str) -> Option<(ChannelId, UserId)> {
        let (channel, ant) = subject.strip_prefix("ants.chanadm.")?.split_once('.')?;
        Some((channel.parse().ok()?, ant.parse().ok()?))
    }

    /// Every channel management request.
    pub const ALL_CHANADM: &str = "ants.chanadm.*.*";

    /// Permission requests an ant escalates to the human ([`ApprovalRequest`](super::ApprovalRequest)).
    pub fn approvals(ant: &UserId) -> String {
        format!("ants.approvals.{ant}")
    }

    /// Messages an ant gave up on, as [`DeadLetter`](super::DeadLetter)s.
    pub fn dead(ant: &UserId) -> String {
        format!("ants.dead.{ant}")
    }

    /// Every ant's dead letters.
    pub const DEAD_ALL: &str = "ants.dead.>";

    /// JetStream's advisories that a consumer of the `ANTS` stream stopped delivering a message after its last allowed
    /// delivery went unacked (core NATS, not stored).
    pub fn max_deliveries_advisories() -> String {
        format!("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{}.*", super::STREAM)
    }

    /// The inbox, notes and channel subjects `user` in `role` may publish on: every inbox for a human (to replay dead
    /// letters as they were), else [`inbox_from`]; [`notes_from`]; and [`chan_from`].
    pub fn sendable(user: &UserId, role: Role) -> [String; 3] {
        let inbox = if role.is_human() { "ants.inbox.>".to_owned() } else { inbox_from(user) };
        [inbox, notes_from(user), chan_from(user)]
    }

    /// Where a client on a leaf node puts an inbox, notes, channel or dead letter subject (or pattern) while the hub's
    /// stream is out of reach: `ants.out.` and the rest of it, kept by the leaf's outbox ([`super::OUTBOX_STREAM`]).
    /// `None` for other subjects, which are not buffered: approvals wait for a human's answer through the hub.
    pub fn outbox(subject: &str) -> Option<String> {
        let rest = subject.strip_prefix("ants.")?;
        ["inbox.", "notes.", "chan.", "dead."]
            .iter()
            .any(|kind| rest.starts_with(kind))
            .then(|| format!("ants.out.{rest}"))
    }

    /// Everything the hub's intake ([`super::INTAKE_STREAM`]) took from the outbox of `machine`: the leaf's subjects,
    /// whatever they are, under the machine's prefix.
    pub fn intake(machine: &super::Machine) -> String {
        format!("ants.in.{machine}.>")
    }

    /// The human's answer to one approval request ([`ApprovalDecision`](super::ApprovalDecision)). Only the human may
    /// publish here; the ant that asked subscribes before it publishes the request.
    pub fn approval_decision(ant: &UserId, request: &super::Uuid) -> String {
        format!("ants.approvals.{ant}.{request}")
    }

    /// Every approval request, of all ants.
    pub const ALL_APPROVAL_REQUESTS: &str = "ants.approvals.*";
    /// Every approval decision, of all ants.
    pub const ALL_APPROVAL_DECISIONS: &str = "ants.approvals.*.*";

    /// Where `ants add --enroll` asks for the credentials of `ant`
    /// ([`EnrollRequest`](super::EnrollRequest); core NATS, not stored). Only the enrollment credentials of `ant`
    /// publish here.
    pub fn enroll(ant: &UserId) -> String {
        format!("ants.enroll.{ant}")
    }

    /// Where the answer to an enrollment request of `ant` goes ([`EnrollReply`](super::EnrollReply); core NATS).
    pub fn enroll_reply(ant: &UserId) -> String {
        format!("ants.enroll.{ant}.reply")
    }

    /// Every enrollment request, of all ants.
    pub const ALL_ENROLL_REQUESTS: &str = "ants.enroll.*";
    /// Every answer to an enrollment request.
    pub const ALL_ENROLL_REPLIES: &str = "ants.enroll.*.reply";

    /// Where `ants renew` asks for the credentials named `name` renewed for a new key
    /// ([`RenewRequest`](super::RenewRequest); core NATS, not stored): an ant, human, webhook or the channel manager
    /// for its own, a human also for the uplink of its machine's leaf node (`leaf-<machine>`), which connects as no
    /// client. `name` is a [`renewable_name`].
    pub fn renew(name: &str) -> String {
        format!("ants.renew.{name}")
    }

    /// Where the answer to a renewal request of `name` goes ([`RenewReply`](super::RenewReply); core NATS).
    pub fn renew_reply(name: &str) -> String {
        format!("ants.renew.{name}.reply")
    }

    /// Whether `name` is one credentials renewed over NATS may carry: a user id, or `leaf-<machine>` of a leaf node's
    /// uplink. Either is one subject token.
    pub fn renewable_name(name: &str) -> bool {
        name.parse::<UserId>().is_ok() || name.strip_prefix("leaf-").is_some_and(|m| m.parse::<Machine>().is_ok())
    }

    /// Every renewal request.
    pub const ALL_RENEW_REQUESTS: &str = "ants.renew.*";
    /// Every answer to a renewal request.
    pub const ALL_RENEW_REPLIES: &str = "ants.renew.*.reply";

    /// Where a manager ant asks for an ant added from a template (`request_ant`;
    /// [`AntRequest`](super::AntRequest); core NATS, not stored). Only the manager publishes here.
    pub fn antreq(manager: &UserId) -> String {
        format!("ants.antreq.{manager}")
    }

    /// Where the answer to an ant request of `manager` goes ([`AntReply`](super::AntReply); core NATS).
    pub fn antreq_reply(manager: &UserId) -> String {
        format!("ants.antreq.{manager}.reply")
    }

    /// Every ant request, of all managers.
    pub const ALL_ANT_REQUESTS: &str = "ants.antreq.*";
    /// Every answer to an ant request.
    pub const ALL_ANT_REPLIES: &str = "ants.antreq.*.reply";

    /// Live progress of an ant on a thread ([`StatusEvent`](super::StatusEvent)s; core NATS, not stored).
    pub fn status(ant: &UserId, thread: &ThreadId) -> String {
        format!("ants.status.{ant}.{thread}")
    }

    /// Live progress of an ant on every thread.
    pub fn status_of(ant: &UserId) -> String {
        format!("ants.status.{ant}.*")
    }

    /// Live progress of every ant on every thread.
    pub const ALL_STATUS: &str = "ants.status.*.*";

    /// Live progress of every ant on a thread.
    pub fn status_on(thread: &ThreadId) -> String {
        format!("ants.status.*.{thread}")
    }

    /// Where a human asks the daemon of `ant` to cancel the prompt running on `thread`
    /// ([`CancelRequest`](super::CancelRequest); core NATS, not stored). Only humans publish here.
    pub fn cancel(ant: &UserId, thread: &ThreadId) -> String {
        format!("ants.cancel.{ant}.{thread}")
    }

    /// The cancel requests for `ant`, on every thread: what its daemon listens to.
    pub fn cancel_of(ant: &UserId) -> String {
        format!("ants.cancel.{ant}.*")
    }

    /// Every cancel request, of all ants.
    pub const ALL_CANCELS: &str = "ants.cancel.*.*";

    /// Where a daemon of `ant` answers a cancel request on `thread` ([`CancelReply`](super::CancelReply); core NATS).
    pub fn cancelled(ant: &UserId, thread: &ThreadId) -> String {
        format!("ants.cancelled.{ant}.{thread}")
    }

    /// The answers of the daemons of `ant` to cancel requests, on every thread.
    pub fn cancelled_of(ant: &UserId) -> String {
        format!("ants.cancelled.{ant}.*")
    }

    /// The roster entry of an ant: the subject its daemon puts [`super::RosterEntry`] on.
    pub fn roster(ant: &UserId) -> String {
        format!("$KV.{}.{ant}", super::ROSTER_BUCKET)
    }

    /// The chain of an ant in [`super::CHAINS_BUCKET`]: the subject its daemon puts [`super::ChainEntry`] on.
    pub fn chain(ant: &UserId) -> String {
        format!("$KV.{}.{ant}", super::CHAINS_BUCKET)
    }

    /// The entry of a human in [`super::HUMANS_BUCKET`]: the subject a human puts [`super::HumanEntry`] on.
    pub fn human_entry(human: &UserId) -> String {
        format!("$KV.{}.{human}", super::HUMANS_BUCKET)
    }

    /// The [`super::ReadMarker`] of a human in [`super::READ_BUCKET`]: the subject only that human puts it on.
    pub fn read_marker(human: &UserId) -> String {
        format!("$KV.{}.{human}", super::READ_BUCKET)
    }

    /// The [`super::NotifyPrefs`] of a human in [`super::NOTIFY_BUCKET`]: the subject only that human puts them on.
    pub fn notify_prefs(human: &UserId) -> String {
        format!("$KV.{}.{human}", super::NOTIFY_BUCKET)
    }

    /// Every [`super::Presence`] of a human's devices in [`super::PRESENCE_BUCKET`]: the subjects only that human puts
    /// them on.
    pub fn presence_of(human: &UserId) -> String {
        format!("$KV.{}.{human}.>", super::PRESENCE_BUCKET)
    }

    /// The entry of a channel in [`super::CHANNELS_BUCKET`]: the subject a human, or the channel manager, puts
    /// [`super::ChannelEntry`] on.
    pub fn channel_entry(channel: &ChannelId) -> String {
        format!("$KV.{}.{channel}", super::CHANNELS_BUCKET)
    }

    /// Everything, for `ants watch`.
    pub const ALL: &str = "ants.>";
}

/// Durable JetStream consumer name of a user's inbox. The CLI with an ant's credentials reads the ant's inbox back
/// through one of its [`spare_readers`], with no ack, while the ant's daemon takes the inbox from it.
pub fn inbox_consumer(user: &UserId) -> String {
    format!("inbox-{user}")
}

/// Durable JetStream consumer `ants webhook serve` takes a human's notifications from: the tool requests of every ant
/// and the human's own inbox, each delivered to one process listing the human, which pushes it to the human's
/// notification sinks. Only the human's credentials create it.
pub fn notify_consumer(human: &UserId) -> String {
    format!("notify-{human}")
}

/// Prefix of the reply subjects a user's connections use, so its credentials need not let it read others' replies.
pub fn inbox_prefix(user: &UserId) -> String {
    format!("_INBOX.{user}")
}

/// Key of a thread's harness session in [`SESSIONS_BUCKET`]. Keys start with the ant, so its credentials can be
/// confined to its own.
pub fn session_key(ant: &UserId, thread: &ThreadId) -> String {
    format!("{ant}.{thread}")
}

/// Key of what a thread has spent since the human's last message on it, and of its escalation, in [`SESSIONS_BUCKET`]
/// beside its session.
pub fn thread_spend_key(ant: &UserId, thread: &ThreadId) -> String {
    format!("{ant}.{thread}.spend")
}

/// Key of the stream sequence of the last post on a channel thread the ant was shown, in [`SESSIONS_BUCKET`] beside
/// the thread's session: its next prompt on the thread shows the posts after it.
pub fn channel_seen_key(ant: &UserId, thread: &ThreadId) -> String {
    format!("{ant}.{thread}.seen")
}

/// Key of the marker that the mention notice of the ant's own channel post at stream sequence `seq` reached `human`,
/// in [`SESSIONS_BUCKET`] ([`ChannelPost::mention_notice`]).
pub fn mention_key(ant: &UserId, seq: u64, human: &UserId) -> String {
    format!("{ant}.mention.{seq}.{human}")
}

/// Key of the ant's escalation deadlines, the threads whose escalation awaits its leader until a time, in
/// [`SESSIONS_BUCKET`]. Each thread's escalation itself is kept with its spend ([`thread_spend_key`]).
pub fn escalation_deadlines_key(ant: &UserId) -> String {
    format!("{ant}.escalations.deadlines")
}

/// Key of what an ant has spent on `day` (`YYYY-MM-DD`, UTC), in [`SESSIONS_BUCKET`].
pub fn daily_spend_key(ant: &UserId, day: &str) -> String {
    format!("{ant}.day.{day}")
}

/// Key of the attempt marker of the inbox message at stream sequence `seq` in [`ATTEMPTS_BUCKET`]. Keys start with the
/// ant, so its credentials can be confined to its own.
pub fn attempt_key(ant: &UserId, seq: u64) -> String {
    format!("{ant}.{seq}")
}

/// Key in [`LINKS_BUCKET`] of the record that `from` passed the link to channel thread `thread` on to `to`. Keys start
/// with the ant that passed it on, so its credentials can be confined to its own.
pub fn link_key(from: &UserId, thread: &ThreadId, to: &UserId) -> String {
    format!("{from}.{thread}.{to}")
}

/// Where a client on a leaf node records a link passed on ([`link_key`]) while the hub is out of reach: the leaf's
/// outbox ([`OUTBOX_STREAM`]) keeps it, and the hub's [`LINKS_BUCKET`] sources it from the hub's intake under the key,
/// for the ants of the machine only ([`Machine::admitted_links`]).
pub fn link_outbox(from: &UserId, thread: &ThreadId, to: &UserId) -> String {
    format!("ants.out.links.{}", link_key(from, thread, to))
}

/// Key in [`OWNERS_BUCKET`] of the owner of `thread` that a message to `to` on it names.
pub fn owner_key(thread: &ThreadId, to: &UserId) -> String {
    format!("{thread}.{to}")
}

/// The message id (`Nats-Msg-Id`) of every record of `key` in [`OWNERS_BUCKET`], whoever writes it, so the bucket
/// drops a second one, as from an outbox, rather than refuse it.
pub fn owner_record_id(key: &str) -> String {
    format!("owner.{key}")
}

/// Where a human's client on a leaf node records the owner of `thread` that its message to `to` names
/// ([`owner_key`]) while the hub is out of reach: the leaf's outbox ([`OUTBOX_STREAM`]) keeps it, and the hub's
/// [`OWNERS_BUCKET`] sources it from the hub's intake under the key, from machines a human runs on only
/// ([`Machine::admitted_owners`]).
pub fn owner_outbox(thread: &ThreadId, to: &UserId) -> String {
    format!("ants.out.owners.{}", owner_key(thread, to))
}

/// Key in [`REACTIONS_BUCKET`] of `by`'s `reaction` to message `message` ([`Envelope::id`]). Keys start with the
/// reactor, so its credentials can be confined to its own.
pub fn reaction_key(by: &UserId, message: &Uuid, reaction: &Reaction) -> String {
    format!("{by}.{message}.{reaction}")
}

/// Every key in [`REACTIONS_BUCKET`] of a reaction to `message`, whoever reacted.
pub fn reactions_to(message: &Uuid) -> String {
    format!("*.{message}.*")
}

/// Who reacted how to which message, as a [`reaction_key`] names them, or `None` when the key is no reaction.
pub fn parse_reaction_key(key: &str) -> Option<(UserId, Uuid, Reaction)> {
    let mut tokens = key.split('.');
    let (by, message, reaction) = (tokens.next()?, tokens.next()?, tokens.next()?);
    if tokens.next().is_some() {
        return None;
    }
    Some((by.parse().ok()?, message.parse().ok()?, reaction.parse().ok()?))
}

/// Key in [`THREADS_BUCKET`] of the title of `thread`.
pub fn title_key(thread: &ThreadId) -> String {
    format!("title.{thread}")
}

/// Key in [`THREADS_BUCKET`] of the pinned and archived threads of `human`: starting with `marks`, so no thread id
/// or human name collides with a title's key.
pub fn marks_key(human: &UserId) -> String {
    format!("marks.{human}")
}

/// Every key in [`LINKS_BUCKET`] of a link to `thread` passed on, whoever passed it on to whom.
pub fn links_to(thread: &ThreadId) -> String {
    format!("*.{thread}.*")
}

/// Who passed a link on to whom, as a [`link_key`] names them, or `None` when the key is no link of `thread`.
pub fn parse_link_key(key: &str, thread: &ThreadId) -> Option<(UserId, UserId)> {
    let (from, rest) = key.split_once('.')?;
    let to = rest.strip_prefix(thread.as_str())?.strip_prefix('.')?;
    Some((from.parse().ok()?, to.parse().ok()?))
}

/// JetStream consumer an ant's daemon reads thread notes through, one thread at a time. Its name is fixed, so its
/// credentials need not let it pull from other consumers. The CLI with the ant's credentials reads through one of its
/// [`spare_readers`] instead, and a human gets the posts by message gets, with no consumer.
pub fn notes_consumer(user: &UserId) -> String {
    format!("notes-{user}")
}

/// How many [`spare_readers`] each of an ant's reader consumers has.
pub const SPARE_READERS: usize = 3;

/// The spares of an ant's reader consumer `reader` ([`inbox_consumer`], [`notes_consumer`], [`chan_reader`]),
/// `<reader>_<n>`, with the same filter pinned: the CLI with the ant's credentials takes a free one for each read, so it
/// never deletes the consumer its daemon, or another CLI read, is reading through. A user id has no `_`, so no spare is
/// named like any user's own consumer.
pub fn spare_readers(reader: &str) -> impl Iterator<Item = String> + '_ {
    (1..=SPARE_READERS).map(move |n| format!("{reader}_{n}"))
}

/// Durable JetStream consumer an ant takes channel posts from, to be woken by those meant for it. Its filter is
/// pinned under [`subject::ALL_CHANNELS`], so it cannot be pointed at an inbox.
pub fn chan_consumer(ant: &UserId) -> String {
    format!("chan-{ant}")
}

/// JetStream consumer an ant's daemon reads a channel (or a thread of it) back through, one read at a time, like
/// [`notes_consumer`]. Its filter is pinned under [`subject::ALL_CHANNELS`] too. The CLI with the ant's credentials
/// reads through one of its [`spare_readers`], and a human gets the posts by message gets, so no read
/// gets in another's way.
pub fn chan_reader(user: &UserId) -> String {
    format!("chanread-{user}")
}

/// JetStream consumer an ant reads its own dead letters back through, one read at a time, like [`notes_consumer`]. Its
/// filter is pinned to [`subject::dead`] of the ant.
pub fn dead_reader(ant: &UserId) -> String {
    format!("deadread-{ant}")
}

/// JetStream consumer an ant's daemon reads back the messages the ant sent through, one read at a time, like
/// [`notes_consumer`], to rebuild the context of a thread. Its filter is pinned to [`subject::inbox_from`] of the ant.
pub fn sent_reader(ant: &UserId) -> String {
    format!("sentread-{ant}")
}

/// What an observer ([`Permissions::for_observer`]) reads from the `ANTS` stream, each through a consumer of its own
/// ([`Feed::consumer`]) whose filter is pinned in the subject that creates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feed {
    /// Every [`ApprovalRequest`], on [`subject::ALL_APPROVAL_REQUESTS`].
    ApprovalRequests,
    /// Every [`ApprovalDecision`], on [`subject::ALL_APPROVAL_DECISIONS`].
    ApprovalDecisions,
    /// Every [`DeadLetter`], on [`subject::DEAD_ALL`].
    DeadLetters,
}

impl Feed {
    pub const ALL: [Feed; 3] = [Feed::ApprovalRequests, Feed::ApprovalDecisions, Feed::DeadLetters];

    /// The subjects of the feed, its consumer's filter.
    pub fn filter(self) -> &'static str {
        match self {
            Feed::ApprovalRequests => subject::ALL_APPROVAL_REQUESTS,
            Feed::ApprovalDecisions => subject::ALL_APPROVAL_DECISIONS,
            Feed::DeadLetters => subject::DEAD_ALL,
        }
    }

    /// The JetStream consumer `observer` reads the feed through: `observe-<feed>-<observer>`, durable or not, as the
    /// observer creates it.
    pub fn consumer(self, observer: &UserId) -> String {
        let feed = match self {
            Feed::ApprovalRequests => "requests",
            Feed::ApprovalDecisions => "decisions",
            Feed::DeadLetters => "dead",
        };
        format!("observe-{feed}-{observer}")
    }
}

/// Key of the event of `webhook` whose id hashes to `hash` in [`WEBHOOKS_BUCKET`]. Keys start with the webhook, so its
/// credentials can be confined to its own.
pub fn webhook_seen_key(webhook: &UserId, hash: &str) -> String {
    format!("{webhook}.{hash}")
}

/// Key of the marker of `webhook` in [`WEBHOOKS_BUCKET`], which `ants webhook serve` (or the daemon running the
/// schedule of that name) puts on connecting, before it delivers, and every [`SERVING_EVERY`] while it runs. Only the
/// webhook's credentials write it, so a daemon honours [`Envelope::webhook`] only from a sender whose marker is there.
/// Its value is the [`Serving`] of the process that put it; that of older versions, the webhook's name.
pub fn webhook_marker_key(webhook: &UserId) -> String {
    format!("{webhook}.{WEBHOOK_MARKER}")
}

/// Last token of [`webhook_marker_key`]; event keys end in hex digits.
const WEBHOOK_MARKER: &str = "webhook";

/// Key of the [`Serving`] of `ants channel serve` in [`WEBHOOKS_BUCKET`], put on connecting and every
/// [`SERVING_EVERY`] while it runs. `channel-manager` is a reserved name, so no webhook writes it, and it ends in
/// no [`WEBHOOK_MARKER`], so it makes no webhook of the channel manager.
pub fn channel_manager_serving_key() -> String {
    format!("{}.serve", UserId::channel_manager())
}

/// The subject of [`channel_manager_serving_key`] in the stream of [`WEBHOOKS_BUCKET`].
pub fn channel_manager_serving_subject() -> String {
    format!("$KV.{WEBHOOKS_BUCKET}.{}", channel_manager_serving_key())
}

/// The version of ants, the same for every crate of the workspace.
pub const ANTS_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How often `ants webhook serve`, each schedule and `ants channel serve` put their [`Serving`] again.
pub const SERVING_EVERY: Duration = Duration::from_hours(1);

/// What a process that is no daemon, yet uses credentials `ants auth` issues, publishes of the ants it runs: `ants
/// webhook serve` and each schedule as the value of their marker ([`webhook_marker_key`]), `ants channel serve` under
/// [`channel_manager_serving_key`]. Daemons publish the same in their [`RosterEntry`]. Readers should treat unknown
/// fields as additions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Serving {
    /// Host name of the machine the process runs on.
    pub machine: String,
    /// [`ANTS_VERSION`] of the process.
    pub version: String,
    /// [`PERMISSIONS_VERSION`] of the process.
    pub perms: PermissionsVersion,
}

impl Serving {
    /// That of this ants, on `machine`.
    pub fn here(machine: impl Into<String>) -> Self {
        Self { machine: machine.into(), version: ANTS_VERSION.to_owned(), perms: PERMISSIONS_VERSION }
    }
}

/// The subject of the marker of `webhook` ([`webhook_marker_key`]) in the stream of [`WEBHOOKS_BUCKET`].
pub fn webhook_marker_subject(webhook: &UserId) -> String {
    format!("$KV.{WEBHOOKS_BUCKET}.{}", webhook_marker_key(webhook))
}

/// JetStream stream backing a KV bucket.
fn kv_stream(bucket: &str) -> String {
    format!("KV_{bucket}")
}

/// What acks the messages `consumer` of `stream` delivers, in both forms of nats-server's ack subjects (ADR-15): v1
/// `$JS.ACK.<stream>.<consumer>.…` and v2 `$JS.ACK.<domain>.<account hash>.<stream>.<consumer>.…`, the default from
/// nats-server 2.16 (feature flag `js_ack_fc_v2`). The domain token is the JetStream domain of the server holding the
/// stream, `_` without one: any is allowed, so credentials issued without a domain still ack on a hub that has one.
/// So is any account hash: credentials publish only in their own account.
fn ack_subjects(stream: &str, consumer: &str) -> [String; 2] {
    [format!("$JS.ACK.{stream}.{consumer}.>"), format!("$JS.ACK.*.*.{stream}.{consumer}.>")]
}

/// What answers the flow control of the consumers of `stream` in JetStream domain `domain` (`*` for any, as in
/// [`ack_subjects`]), in both forms: v1 `$JS.FC.<stream>.…` and v2 `$JS.FC.<domain>.<account hash>.<stream>.…`.
fn flow_control_subjects(domain: &str, stream: &str) -> [String; 2] {
    [format!("$JS.FC.{stream}.>"), format!("$JS.FC.{domain}.*.{stream}.>")]
}

/// What a human's credentials need for its inbox ([`READ_BUCKET`]): creating the bucket and reading and writing its own
/// [`ReadMarker`] only, by key, so no human reads or moves another's; and deleting its own inbox consumer
/// ([`inbox_consumer`]), which kept a human's read state before the marker, once its state moved into the marker.
fn read_marker_rights(human: &UserId) -> [String; 5] {
    let read = kv_stream(READ_BUCKET);
    let marker = subject::read_marker(human);
    [
        format!("$JS.API.STREAM.INFO.{read}"),
        format!("$JS.API.STREAM.CREATE.{read}"),
        format!("$JS.API.DIRECT.GET.{read}.{marker}"),
        marker,
        format!("$JS.API.CONSUMER.DELETE.{STREAM}.{}", inbox_consumer(human)),
    ]
}

/// What a human's credentials need to say where its devices read ([`PRESENCE_BUCKET`]): creating the bucket, and
/// reading by direct gets and writing its own keys only. No stream info, which with a subjects filter lists every key,
/// so no human sees, let alone moves, where another reads.
fn presence_rights(human: &UserId) -> [String; 3] {
    let presence = kv_stream(PRESENCE_BUCKET);
    let own = subject::presence_of(human);
    [format!("$JS.API.STREAM.CREATE.{presence}"), format!("$JS.API.DIRECT.GET.{presence}.{own}"), own]
}

/// What a human's credentials need to hold back its notifications ([`NOTIFY_BUCKET`]): creating the bucket and reading
/// and writing its own [`NotifyPrefs`] only, by key, so no human snoozes or mutes another's; `ants webhook serve`
/// reads them with the human's own credentials.
fn notify_prefs_rights(human: &UserId) -> [String; 4] {
    let notify = kv_stream(NOTIFY_BUCKET);
    let prefs = subject::notify_prefs(human);
    [
        format!("$JS.API.STREAM.INFO.{notify}"),
        format!("$JS.API.STREAM.CREATE.{notify}"),
        format!("$JS.API.DIRECT.GET.{notify}.{prefs}"),
        prefs,
    ]
}

/// What a user's credentials need for reactions ([`REACTIONS_BUCKET`]): putting, deleting and reading back by direct
/// gets its own keys only, so no one forges or removes another's reaction; and for a human, creating the bucket and
/// reading every reaction, singly and through the ordered consumers behind listing and watching them. An ant reads no
/// one else's: subjects cannot tie a message id to the threads and channels whose messages it may read.
fn reaction_rights(user: &UserId, role: Role) -> Vec<String> {
    let reactions = kv_stream(REACTIONS_BUCKET);
    let own = format!("$KV.{REACTIONS_BUCKET}.{user}.>");
    let mut rights = vec![format!("$JS.API.DIRECT.GET.{reactions}.{own}"), own];
    if role.is_human() {
        rights.extend([
            format!("$JS.API.STREAM.INFO.{reactions}"),
            format!("$JS.API.STREAM.CREATE.{reactions}"),
            format!("$JS.API.DIRECT.GET.{reactions}.>"),
            format!("$JS.API.CONSUMER.CREATE.{reactions}"),
        ]);
        rights.extend(flow_control_subjects("*", &reactions));
    }
    rights
}

/// What a human's credentials need for thread titles and its pins and archives ([`THREADS_BUCKET`]): creating the
/// bucket; reading by direct gets, putting and deleting every title, which any human sets; and reading and writing its
/// own [`ThreadMarks`] only, so no human reads or changes another's.
fn thread_rights(human: &UserId) -> [String; 6] {
    let threads = kv_stream(THREADS_BUCKET);
    let titles = format!("$KV.{THREADS_BUCKET}.title.*");
    let marks = format!("$KV.{THREADS_BUCKET}.{}", marks_key(human));
    [
        format!("$JS.API.STREAM.INFO.{threads}"),
        format!("$JS.API.STREAM.CREATE.{threads}"),
        format!("$JS.API.DIRECT.GET.{threads}.{titles}"),
        titles,
        format!("$JS.API.DIRECT.GET.{threads}.{marks}"),
        marks,
    ]
}

/// Where a webhook delivers its events: an ant's inbox, or a channel.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Destination {
    Ant(UserId),
    Channel(ChannelId),
}

impl Destination {
    /// The subject `webhook` publishes on to reach the destination.
    pub fn subject(&self, webhook: &UserId) -> String {
        match self {
            Self::Ant(ant) => subject::inbox(ant, webhook),
            Self::Channel(channel) => subject::chan(channel, webhook),
        }
    }
}

/// Subjects a set of credentials may publish and subscribe to. Everything else is denied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permissions {
    pub publish: Vec<String>,
    pub subscribe: Vec<String>,
}

impl Permissions {
    /// What the credentials of `user` in `role` allow.
    ///
    /// Any ant may write to every inbox, the notes of every thread and every channel, but only as itself (the sender is
    /// the last token of inbox, notes and channel subjects). It speaks only for itself on status, approvals and dead
    /// letters, reads back only its own dead letters (through its own [`dead_reader`]) and the messages it sent (through
    /// its own [`sent_reader`]), reads only its own inbox
    /// (consumer and its spares, and live), the notes of every thread (live, and from the stream
    /// through its own notes consumer and its spares), every channel (live, and from the stream through its own channel
    /// consumer, and its reader and the reader's spares), its own replies and the decisions on its own approval requests (live, and as stored), reads the roster
    /// to find ants by capability but writes only its own entry, reads every escalation chain by key but writes only
    /// its own, lists the links to a channel thread ants passed on but records only its own ([`LINKS_BUCKET`]), reads
    /// the channels, humans and owners buckets but never writes them, reads which users are webhooks (their
    /// markers in [`WEBHOOKS_BUCKET`], none of their events), asks the channel manager for changes to
    /// a channel only as itself ([`subject::chanadm_from`]), and reads and writes only its own keys in the sessions and attempts buckets,
    /// except that it reads every ant's daily spend there ([`daily_spend_key`]), to hold the ants of a template to
    /// their pool caps. Of
    /// these and the owners bucket it gets no stream info, which lists every key of a bucket (so the threads of other
    /// ants): it reads single keys by direct gets. Nor does it get the stream's info, which lists every subject stored,
    /// so who messages whom: it finds the stream through the info of its own inbox consumer. The filter of each of its consumers is
    /// part of the subject that creates it, so none can be pointed at another inbox: a consumer with several filters
    /// has none in that subject, so no ant may create one. A human (a CLI user; [`Role::Human`], whatever its
    /// name) also watches all ants traffic, decides approval requests, reads every ant's thread and daily spend in the
    /// sessions bucket ([`thread_spend_key`], [`daily_spend_key`]) but none of its other keys there, may create and
    /// upgrade the stream and create the roster, sessions,
    /// attempts, channels, humans, chains, links and owners buckets, writes the channels, humans and owners buckets
    /// (the owners also in its outbox form, [`owner_outbox`]), reads the version the channel manager runs
    /// ([`channel_manager_serving_key`]), reads the stream's messages by sequence, one at a time or in batches of
    /// direct gets, and removes dead letters by sequence; its own read marker in [`READ_BUCKET`], and no other, says which replies it has read on any
    /// of its devices ([`read_marker_rights`]), and its own keys in [`PRESENCE_BUCKET`] where its devices read
    /// ([`presence_rights`]), and its own [`NotifyPrefs`] in [`NOTIFY_BUCKET`] what it holds back of its notifications
    /// ([`notify_prefs_rights`]), and it reads every reaction ([`REACTIONS_BUCKET`]), of which every user, ant or human,
    /// puts and removes only its own ([`reaction_rights`]); it sets every thread's title and its own pinned and
    /// archived threads, no other human's ([`thread_rights`]). A human may send under
    /// any sender, to replay a dead letter as it was. An ant gets no `STREAM.MSG.GET`: it takes any sequence, so it
    /// would expose every inbox. So only a human hears the max-deliveries advisories of the stream's consumers and
    /// stores the dead letters of the messages they name, on any ant's dead letter subject.
    ///
    /// Each inbox, notes and channel subject an ant may publish, and its own dead letter subject, it may also publish
    /// in its outbox form ([`subject::outbox`]), which only a leaf node's outbox keeps, and so the links it passes on
    /// ([`link_outbox`]). Only a human sets up leaf outboxes, the hub's intake and the buckets' sources from it: a
    /// stream's configuration can copy, transform and republish any subject of the account, so no ant may create or
    /// change one.
    pub fn for_user(user: &UserId, role: Role) -> Self {
        let consumer = inbox_consumer(user);
        let notes = notes_consumer(user);
        let (chan, chan_read) = (chan_consumer(user), chan_reader(user));
        let roster_stream = kv_stream(ROSTER_BUCKET);
        let channels = kv_stream(CHANNELS_BUCKET);
        let humans = kv_stream(HUMANS_BUCKET);
        let chains = kv_stream(CHAINS_BUCKET);
        let links = kv_stream(LINKS_BUCKET);
        let owners = kv_stream(OWNERS_BUCKET);
        let webhooks = kv_stream(WEBHOOKS_BUCKET);
        let sessions = kv_stream(SESSIONS_BUCKET);
        let attempts = kv_stream(ATTEMPTS_BUCKET);
        let sendable = subject::sendable(user, role);
        // The same subjects in the leaf's outbox, for when the hub is out of reach.
        let outbox = sendable.iter().map(|s| subject::outbox(s).expect("an outbox subject"));
        let mut publish: Vec<String> = sendable.iter().cloned().chain(outbox).collect();
        publish.extend([
            format!("$JS.API.STREAM.INFO.{roster_stream}"),
            // Also how its connection finds JetStream, and the stream, whose info it does not get.
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{consumer}"),
            // The filter is part of the subject, so the consumer cannot be pointed at another inbox.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{consumer}.{}", subject::inbox_of(user)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{consumer}"),
        ]);
        publish.extend(ack_subjects(STREAM, &consumer));
        publish.extend([
            // Reading thread notes back through its own consumer, which only ever filters notes subjects.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{notes}.{}", subject::ALL_NOTES),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{notes}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{notes}"),
            // Channel posts: its durable consumer, and reading channels back; both only ever filter channel subjects.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{chan}.{}", subject::ALL_CHANNELS),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{chan}"),
        ]);
        publish.extend(ack_subjects(STREAM, &chan));
        publish.extend([
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{chan_read}.{}", subject::ALL_CHANNELS),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{chan_read}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{chan_read}"),
            // Reading the roster: single entries, and the ordered consumers behind listing and watching it.
            format!("$JS.API.DIRECT.GET.{roster_stream}.>"),
            format!("$JS.API.CONSUMER.CREATE.{roster_stream}"),
        ]);
        publish.extend(flow_control_subjects("*", &roster_stream));
        publish.extend([
            // Reading the channels the same way.
            format!("$JS.API.STREAM.INFO.{channels}"),
            format!("$JS.API.DIRECT.GET.{channels}.>"),
            format!("$JS.API.CONSUMER.CREATE.{channels}"),
        ]);
        publish.extend(flow_control_subjects("*", &channels));
        publish.extend([
            // And the humans.
            format!("$JS.API.STREAM.INFO.{humans}"),
            format!("$JS.API.DIRECT.GET.{humans}.>"),
            format!("$JS.API.CONSUMER.CREATE.{humans}"),
        ]);
        publish.extend(flow_control_subjects("*", &humans));
        publish.extend([
            // And single escalation chains.
            format!("$JS.API.STREAM.INFO.{chains}"),
            format!("$JS.API.DIRECT.GET.{chains}.>"),
            // And the links passed on to a channel thread, through ordered consumers filtering that thread's.
            format!("$JS.API.STREAM.INFO.{links}"),
            format!("$JS.API.CONSUMER.CREATE.{links}"),
        ]);
        publish.extend(flow_control_subjects("*", &links));
        publish.extend([
            // And the owners of single threads.
            format!("$JS.API.DIRECT.GET.{owners}.>"),
            // And the webhooks' markers, not the events they delivered.
            format!("$JS.API.DIRECT.GET.{webhooks}.$KV.{WEBHOOKS_BUCKET}.*.{WEBHOOK_MARKER}"),
        ]);
        publish.extend(reaction_rights(user, role));
        let mut subscribe = vec![format!("{}.>", inbox_prefix(user))];
        if role.is_human() {
            let notify = notify_consumer(user);
            publish.extend([
                // Its own renewal, and that of its machine's leaf node uplink, which connects as no client; the
                // request proves the credentials it renews by signature, whoever carries it. It hears the answers
                // with everything under `ants.>`.
                subject::ALL_RENEW_REQUESTS.to_owned(),
                // Stream info lists every subject stored (who messages whom), so only a human may ask for it.
                format!("$JS.API.STREAM.INFO.{STREAM}"),
                // The consumer of its notifications: one filter puts it in the subject, several leave it out.
                format!("$JS.API.CONSUMER.CREATE.{STREAM}.{notify}"),
                format!("$JS.API.CONSUMER.CREATE.{STREAM}.{notify}.>"),
                format!("$JS.API.CONSUMER.INFO.{STREAM}.{notify}"),
                format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{notify}"),
            ]);
            publish.extend(ack_subjects(STREAM, &notify));
            publish.extend([
                format!("$JS.API.STREAM.CREATE.{STREAM}"),
                // Creating a KV bucket first looks up the account's JetStream limits.
                "$JS.API.INFO".to_owned(),
                format!("$JS.API.STREAM.CREATE.{roster_stream}"),
                // Adding what an older version lacks, such as the markers of expired entries, and room for the
                // consumers of every daemon watching the bucket, which a KV bucket gets only by an update.
                format!("$JS.API.STREAM.UPDATE.{roster_stream}"),
                format!("$JS.API.STREAM.UPDATE.{channels}"),
                format!("$JS.API.STREAM.UPDATE.{humans}"),
                // Stream info lists every key of a bucket, so only a human may ask for it of these three.
                format!("$JS.API.STREAM.INFO.{sessions}"),
                format!("$JS.API.STREAM.INFO.{attempts}"),
                format!("$JS.API.STREAM.INFO.{owners}"),
                // Reading what each ant spent, for `ants spend`: its thread and daily spend keys, not its sessions.
                format!("$JS.API.DIRECT.GET.{sessions}.$KV.{SESSIONS_BUCKET}.*.*.spend"),
                format!("$JS.API.DIRECT.GET.{sessions}.$KV.{SESSIONS_BUCKET}.*.day.*"),
                format!("$JS.API.STREAM.CREATE.{sessions}"),
                format!("$JS.API.STREAM.CREATE.{attempts}"),
                format!("$JS.API.STREAM.CREATE.{channels}"),
                format!("$JS.API.STREAM.CREATE.{humans}"),
                format!("$JS.API.STREAM.CREATE.{chains}"),
                format!("$JS.API.STREAM.CREATE.{links}"),
                format!("$JS.API.STREAM.CREATE.{owners}"),
                format!("$JS.API.STREAM.INFO.{webhooks}"),
                format!("$JS.API.STREAM.CREATE.{webhooks}"),
                // The version the channel manager runs, for `ants doctor`.
                format!("$JS.API.DIRECT.GET.{webhooks}.{}", channel_manager_serving_subject()),
                // Put and delete of every channel's and every human's entry.
                format!("$KV.{CHANNELS_BUCKET}.>"),
                format!("$KV.{HUMANS_BUCKET}.>"),
                // Recording the owner of each thread it messages an ant on, and the same in the leaf's outbox.
                format!("$KV.{OWNERS_BUCKET}.*.*"),
                "ants.out.owners.*.*".to_owned(),
                format!("$JS.API.STREAM.UPDATE.{STREAM}"),
                format!("$JS.API.STREAM.MSG.GET.{STREAM}"),
                // Reading the stream back in batches; it reads nothing more than STREAM.MSG.GET, which takes any
                // sequence.
                format!("$JS.API.DIRECT.GET.{STREAM}"),
                format!("$JS.API.STREAM.MSG.DELETE.{STREAM}"),
                subject::ALL_APPROVAL_DECISIONS.to_owned(),
                subject::ALL_CANCELS.to_owned(),
                subject::DEAD_ALL.to_owned(),
                // Setting up the store and forward of leaf nodes: the hub's intake, and each leaf's outbox through the
                // leaf's own domain (the plain form is what a leaf maps its own domain to).
                format!("$JS.API.STREAM.CREATE.{INTAKE_STREAM}"),
                format!("$JS.API.STREAM.UPDATE.{INTAKE_STREAM}"),
                format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"),
                // And the links and owners buckets' sources from the intake.
                format!("$JS.API.STREAM.UPDATE.{links}"),
                format!("$JS.API.STREAM.UPDATE.{owners}"),
            ]);
            for op in ["CREATE", "UPDATE", "INFO"] {
                publish.push(format!("$JS.API.STREAM.{op}.{OUTBOX_STREAM}"));
                publish.push(format!("$JS.*.API.STREAM.{op}.{OUTBOX_STREAM}"));
            }
            publish.extend(read_marker_rights(user));
            publish.extend(presence_rights(user));
            publish.extend(thread_rights(user));
            publish.extend(notify_prefs_rights(user));
            subscribe.extend([subject::ALL.to_owned(), subject::max_deliveries_advisories()]);
        } else {
            let own_keys = format!("$KV.{SESSIONS_BUCKET}.{user}.>");
            let own_attempts = format!("$KV.{ATTEMPTS_BUCKET}.{user}.>");
            let dead_read = dead_reader(user);
            let sent_read = sent_reader(user);
            publish.extend([
                // Get and put of its own sessions.
                format!("$JS.API.DIRECT.GET.{sessions}.{own_keys}"),
                own_keys,
                // Get of every ant's daily spend, which pool caps add up.
                format!("$JS.API.DIRECT.GET.{sessions}.$KV.{SESSIONS_BUCKET}.*.day.*"),
                // Get, put and delete of the markers of its own attempts.
                format!("$JS.API.DIRECT.GET.{attempts}.{own_attempts}"),
                own_attempts,
                // Exactly its requests subject: an ant cannot answer its own requests one level below.
                subject::approvals(user),
                // Reading the decision stored on one of its requests: a direct get naming the subject in its own
                // subject returns only the last message on that subject, so nothing else is readable this way.
                format!("$JS.API.DIRECT.GET.{STREAM}.{}.*", subject::approvals(user)),
                subject::dead(user),
                subject::outbox(&subject::dead(user)).expect("an outbox subject"),
                // Reading its own dead letters back, through its own consumer filtering only those.
                format!("$JS.API.CONSUMER.CREATE.{STREAM}.{dead_read}.{}", subject::dead(user)),
                format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{dead_read}"),
                format!("$JS.API.CONSUMER.DELETE.{STREAM}.{dead_read}"),
                // Reading back the messages it sent, through its own consumer filtering only those.
                format!("$JS.API.CONSUMER.CREATE.{STREAM}.{sent_read}.{}", subject::inbox_from(user)),
                format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{sent_read}"),
                format!("$JS.API.CONSUMER.DELETE.{STREAM}.{sent_read}"),
                subject::status_of(user),
                subject::cancelled_of(user),
                subject::chanadm_from(user),
                // Put and delete of its own roster entry, and of its own escalation chain.
                subject::roster(user),
                subject::chain(user),
                // Put of the links it passed on, and the same in the leaf's outbox.
                format!("$KV.{LINKS_BUCKET}.{user}.>"),
                format!("ants.out.links.{user}.>"),
                subject::renew(user.as_str()),
                // Asking the signer for an ant added from a template, should it manage one.
                subject::antreq(user),
            ]);
            // The spares of its inbox consumer and its notes and channel readers, for the CLI, each with the same
            // filter pinned; no ack, so the spares of the inbox consumer only read it.
            let own_inbox = subject::inbox_of(user);
            for (reader, filter) in
                [(&consumer, own_inbox.as_str()), (&notes, subject::ALL_NOTES), (&chan_read, subject::ALL_CHANNELS)]
            {
                for spare in spare_readers(reader) {
                    publish.extend([
                        format!("$JS.API.CONSUMER.CREATE.{STREAM}.{spare}.{filter}"),
                        format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{spare}"),
                        format!("$JS.API.CONSUMER.DELETE.{STREAM}.{spare}"),
                    ]);
                }
            }
            subscribe.extend([
                subject::ALL_NOTES.to_owned(),
                subject::ALL_CHANNELS.to_owned(),
                format!("{}.*", subject::approvals(user)),
                subject::cancel_of(user),
                // Live replies to its ask_agent calls, which the inbox consumer holds back while the asking prompt runs.
                subject::inbox_of(user),
                subject::renew_reply(user.as_str()),
                subject::antreq_reply(user),
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

    /// What the uplink credentials of `machine`'s leaf node allow: everything any of `users` may do in its role,
    /// since every message of a user on the machine crosses that one connection, except the outbox subjects, which
    /// stay on the leaf. Include each human that runs the CLI on the machine, as [`Role::Human`].
    ///
    /// Besides, the hub's intake may source the leaf's outbox through the machine's own JetStream domain: the hub
    /// sends the leaf requests to create, update and look up the outbox and to create and delete the consumers that
    /// read it, and flow control. The leaf delivers the outbox under [`Machine::deliver_prefix`] and answers under
    /// [`Machine::source_replies`] and the [`Machine::setup_inbox`] of the default human and of each human among
    /// `users` only, all of them the machine's own. nats-server carries a reply across a leaf connection only if the
    /// leaf may publish it: the interest behind it reaches the leaf only then.
    ///
    /// For each pattern of a user's inbox, notes and channel subjects, which has a wildcard before the sender, the
    /// uplink also gets one literal subject of it (`ants.inbox.dev-a.dev-a`): nats-server lets the hub stream's interest
    /// cross the uplink only when it finds such a subject, and the pattern allows it anyway.
    ///
    /// The uplink also carries enrollment requests from the machine ([`subject::ALL_ENROLL_REQUESTS`]) and their
    /// answers to it ([`subject::ALL_ENROLL_REPLIES`]), for an ant new to the machine: never other machines' requests,
    /// nor answers from the machine, which only the approver gives. A request proves its enrollment credentials by
    /// signature ([`EnrollRequest::signed`]), so the uplink cannot ask for an ant itself; an answer holds no secret, the
    /// seed of the key it completes staying on the asking machine. Likewise renewal requests from the machine
    /// ([`subject::ALL_RENEW_REQUESTS`]), its own among them, and their answers ([`subject::ALL_RENEW_REPLIES`]): each
    /// request proves the credentials it renews by signature ([`RenewRequest::signed`]).
    pub fn for_leaf<'a>(machine: &Machine, users: impl IntoIterator<Item = (&'a UserId, Role)>) -> Self {
        let mut leaf = Self { publish: Vec::new(), subscribe: Vec::new() };
        let mut setup = vec![UserId::human()];
        for (user, role) in users {
            if role.is_human() && !setup.contains(user) {
                setup.push(user.clone());
            }
            let Self { publish, subscribe } = Self::for_user(user, role);
            // nats-server lets the hub stream's interest (`ants.inbox.>`, `ants.notes.>`, ...) cross the uplink only if
            // `ReverseMatch` finds a publish allowance under it, and that skips allowances with `*` before the last
            // token, such as `ants.inbox.*.dev-a`. One literal subject each of them already allows is found.
            let chanadm = (!role.is_human()).then(|| subject::chanadm_from(user));
            let found = subject::sendable(user, role).into_iter().chain(chanadm).map(|s| s.replace('*', user.as_str()));
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
        ]);
        // The outbox is in the machine's own domain.
        leaf.subscribe.extend(flow_control_subjects(machine.as_str(), OUTBOX_STREAM));
        for (all, new) in [
            (&mut leaf.publish, [subject::ALL_ENROLL_REQUESTS, subject::ALL_RENEW_REQUESTS]),
            (&mut leaf.subscribe, [subject::ALL_ENROLL_REPLIES, subject::ALL_RENEW_REPLIES]),
        ] {
            for s in new {
                if !all.iter().any(|known| known == s) {
                    all.push(s.to_owned());
                }
            }
        }
        leaf
    }

    /// What the credentials of the channel manager (`ants channel serve`) allow: hearing every channel management
    /// request, reading the roster and the humans, reading and writing the channels bucket, posting in any channel
    /// as [`UserId::channel_manager`], and putting the version it runs ([`channel_manager_serving_key`]). Its JWT also
    /// lets it answer each request it hears once, on the request's reply subject, so it needs no right to publish on
    /// any ant's reply inboxes. It asks for its own renewal ([`subject::renew`]) and hears the answer.
    pub fn for_channel_manager() -> Self {
        let user = UserId::channel_manager();
        let (roster, channels, humans) =
            (kv_stream(ROSTER_BUCKET), kv_stream(CHANNELS_BUCKET), kv_stream(HUMANS_BUCKET));
        let mut publish = vec![
            subject::chan_from(&user),
            format!("$KV.{CHANNELS_BUCKET}.>"),
            channel_manager_serving_subject(),
            // Finding JetStream on connecting, as an ant does, without the stream's info.
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", inbox_consumer(&user)),
        ];
        for stream in [&roster, &channels, &humans] {
            publish.extend([
                format!("$JS.API.STREAM.INFO.{stream}"),
                format!("$JS.API.DIRECT.GET.{stream}.>"),
                format!("$JS.API.CONSUMER.CREATE.{stream}"),
            ]);
            publish.extend(flow_control_subjects("*", stream));
        }
        publish.push(subject::renew(user.as_str()));
        let subscribe = vec![
            subject::ALL_CHANADM.to_owned(),
            format!("{}.>", inbox_prefix(&user)),
            subject::renew_reply(user.as_str()),
        ];
        Self { publish, subscribe }
    }

    /// What the enrollment credentials of `ant` allow: asking for its credentials on [`subject::enroll`] and
    /// hearing the answer on [`subject::enroll_reply`], nothing else, so they cannot ask for another name or hear
    /// another's answer.
    pub fn for_enrollment(ant: &UserId) -> Self {
        Self { publish: vec![subject::enroll(ant)], subscribe: vec![subject::enroll_reply(ant)] }
    }

    /// What the credentials of `webhook` allow: delivering its events to `destinations` only, as itself, on
    /// [`Destination::subject`], remembering the events it delivered, under its own keys of [`WEBHOOKS_BUCKET`]
    /// only, and recording the owner its config names for each thread it starts in an ant's inbox ([`OWNERS_BUCKET`],
    /// under that ant's keys only). It reads the entry of each channel among `destinations`, for the members a
    /// broadcast mentions, and hears its own reply inboxes, for the stream's acks. It asks for its own renewal
    /// ([`subject::renew`]) and hears the answer. Nothing else: it neither writes
    /// `ants_channels` nor `ants_humans`, so it can neither change a channel nor become a human, nor reads any message.
    /// It gets no stream info, which lists every subject stored: it reads and records its events by key alone.
    pub fn for_webhook(webhook: &UserId, destinations: &[Destination]) -> Self {
        let seen = kv_stream(WEBHOOKS_BUCKET);
        let channels = kv_stream(CHANNELS_BUCKET);
        let own = format!("$KV.{WEBHOOKS_BUCKET}.{webhook}.>");
        let mut publish: Vec<String> = Vec::new();
        for destination in destinations {
            let subject = destination.subject(webhook);
            if !publish.contains(&subject) {
                publish.push(subject);
            }
            if let Destination::Channel(channel) = destination {
                publish.push(format!("$JS.API.DIRECT.GET.{channels}.{}", subject::channel_entry(channel)));
            }
        }
        publish.extend([
            // Finding JetStream on connecting: the info of a consumer named after it, which none creates. No stream
            // info, which lists every subject stored, so who messages whom and the events of other webhooks.
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", inbox_consumer(webhook)),
            // Get and create of its own events.
            format!("$JS.API.DIRECT.GET.{seen}.{own}"),
            own,
        ]);
        let ants = destinations.iter().filter_map(|d| match d {
            Destination::Ant(ant) => Some(ant),
            Destination::Channel(_) => None,
        });
        // Recording the owner of the threads it starts in an ant's inbox, under that ant's keys, where no record is.
        publish.extend(ants.map(|ant| format!("$KV.{OWNERS_BUCKET}.*.{ant}")));
        publish.push(subject::renew(webhook.as_str()));
        publish.dedup();
        Self {
            publish,
            subscribe: vec![format!("{}.>", inbox_prefix(webhook)), subject::renew_reply(webhook.as_str())],
        }
    }

    /// What the read-only credentials of `observer` allow, for hooks that act on the team's events from outside
    /// (`ants auth user --observer`): reading every approval request, approval decision and dead letter from the
    /// stream through its own consumers ([`Feed::consumer`], each with its filter pinned in the subject that creates
    /// it), and hearing them live; hearing every ant's status ([`subject::status`]), escalations among them
    /// ([`StatusUpdate::Escalated`]); reading and watching the roster, where each ant's daemon says it serves the ant
    /// and how busy it is. It hears its own reply inboxes and asks for its own renewal ([`subject::renew`]).
    ///
    /// Nothing else: it publishes no message, decides no approval, cancels nothing and writes no bucket, so it can
    /// neither act for an ant or a human nor change what they see. It reads no inbox, notes or channel, so no
    /// conversation as stored, nor the sessions, attempts, owners, links, chains, humans, channels, webhooks, read or
    /// presence buckets: where the devices of humans are is not even for other humans to see. It gets no stream info,
    /// which lists every subject stored, so who messages whom, nor message gets, which take any sequence: it finds the
    /// stream through the info of a consumer named after it, as a webhook does.
    pub fn for_observer(observer: &UserId) -> Self {
        let mut publish = vec![format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", inbox_consumer(observer))];
        for feed in Feed::ALL {
            let consumer = feed.consumer(observer);
            publish.extend([
                format!("$JS.API.CONSUMER.CREATE.{STREAM}.{consumer}.{}", feed.filter()),
                format!("$JS.API.CONSUMER.INFO.{STREAM}.{consumer}"),
                format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{consumer}"),
            ]);
            publish.extend(ack_subjects(STREAM, &consumer));
            publish.push(format!("$JS.API.CONSUMER.DELETE.{STREAM}.{consumer}"));
        }
        let roster = kv_stream(ROSTER_BUCKET);
        publish.extend([
            format!("$JS.API.STREAM.INFO.{roster}"),
            format!("$JS.API.DIRECT.GET.{roster}.>"),
            format!("$JS.API.CONSUMER.CREATE.{roster}"),
        ]);
        publish.extend(flow_control_subjects("*", &roster));
        publish.push(subject::renew(observer.as_str()));
        let subscribe = vec![
            format!("{}.>", inbox_prefix(observer)),
            subject::ALL_APPROVAL_REQUESTS.to_owned(),
            subject::ALL_APPROVAL_DECISIONS.to_owned(),
            subject::DEAD_ALL.to_owned(),
            subject::ALL_STATUS.to_owned(),
            subject::renew_reply(observer.as_str()),
        ];
        Self { publish, subscribe }
    }

    /// What `ants enroll` and `ants signer` allow themselves on the machine holding the account key: hearing every
    /// enrollment, renewal and ant request ([`AntRequest`]) and answering it, asking the humans about an ant request
    /// as an approval request of its manager and reading their decision, and admitting an ant new to a leaf node into
    /// the hub's stream and links bucket from the leaf's outbox, as `ants leaf outbox` does, with the replies of its
    /// JetStream requests under [`ENROLL_INBOX`].
    pub fn for_enroll_approver() -> Self {
        let links = kv_stream(LINKS_BUCKET);
        Self {
            publish: vec![
                subject::ALL_ENROLL_REPLIES.to_owned(),
                subject::ALL_RENEW_REPLIES.to_owned(),
                subject::ALL_ANT_REPLIES.to_owned(),
                // A manager's ant request outside the templates' policy goes to the humans as an approval request
                // of the manager's, whose decision it reads back as the ant would.
                subject::ALL_APPROVAL_REQUESTS.to_owned(),
                format!("$JS.API.DIRECT.GET.{STREAM}.{}", subject::ALL_APPROVAL_DECISIONS),
                format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"),
                format!("$JS.API.STREAM.INFO.{STREAM}"),
                format!("$JS.API.STREAM.UPDATE.{STREAM}"),
                format!("$JS.API.STREAM.INFO.{links}"),
                format!("$JS.API.STREAM.UPDATE.{links}"),
            ],
            subscribe: vec![
                subject::ALL_ENROLL_REQUESTS.to_owned(),
                subject::ALL_RENEW_REQUESTS.to_owned(),
                subject::ALL_ANT_REQUESTS.to_owned(),
                format!("{ENROLL_INBOX}.>"),
            ],
        }
    }
}

/// Reply prefix of the connections of `ants enroll`: its first token starts with `_`, which no user id does, so no
/// user's own reply inboxes ([`inbox_prefix`]) cover it.
pub const ENROLL_INBOX: &str = "_INBOX._enroll";

/// The `name` of the user JWT of `ant`'s enrollment credentials.
pub fn enrollment_user(ant: &UserId) -> String {
    format!("enroll-{ant}")
}

/// How often `ants add --enroll` repeats its [`EnrollRequest`] while it waits for an answer. Requests are core
/// NATS and stored nowhere, so `ants enroll` sees those sent while it listens.
pub const ENROLL_REPEAT: Duration = Duration::from_secs(5);

/// An ant's entry in the [`ROSTER_BUCKET`], under the key of its id.
///
/// The serving daemon puts it on start, re-puts it every [`ROSTER_HEARTBEAT`] and deletes it on a clean stop. The
/// bucket's max age is [`ROSTER_TTL`], so the entry of a daemon that died expires on its own. A daemon that defers to
/// others, as the built-in messenger's does, puts it only over its own entry or none, and stops serving once another
/// daemon puts it. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterEntry {
    pub ant: UserId,
    /// Host name of the machine the daemon runs on.
    pub machine: String,
    /// The daemon run that put the entry, unique to it, so two daemons on one machine tell their entries apart.
    /// Missing from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// Agent harness behind the ant, such as `copilot`.
    pub harness: String,
    /// Free-form tags the ant is good for (`rust`, `frontend`, `billing`), for routing requests to it.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Role instructions advertised to other ants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whoami: Option<String>,
    /// Directory the harness works in, on `machine`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// When the daemon started serving the ant.
    pub started_at: DateTime<Utc>,
    /// When the daemon last refreshed the entry.
    pub heartbeat_at: DateTime<Utc>,
    /// Whether the ant was handling a message at the last refresh. Missing from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<Activity>,
    /// Messages being handled at the last refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handling: Option<u32>,
    /// Live harness processes (sessions) at the last refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harnesses: Option<u32>,
    /// The ant's escalation chain, in order: what it cannot settle goes to these ants before a human. The first
    /// leads. Published so any daemon can follow the chain while the ant is offline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub escalate_to: Vec<UserId>,
    /// The version of ants the daemon runs ([`ANTS_VERSION`]). Missing from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// The permissions version of the ants the daemon runs ([`PERMISSIONS_VERSION`]). Missing from entries of older
    /// daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub perms: Option<PermissionsVersion>,
    /// Set while the ant's harness is signed out: messages to the ant wait, unanswered, until a check finds the
    /// harness signed in again. Missing while it is signed in, and from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_out: Option<SignInNeeded>,
    /// Set while the ant's harness is held back by a provider limit: messages to the ant wait, spending no attempts,
    /// until `until`. Missing otherwise, and from entries of older daemons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limited: Option<HarnessLimit>,
}

/// A provider limit an ant's harness ran into, as its roster entry reports it ([`RosterEntry::limited`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessLimit {
    pub kind: LimitKind,
    /// When the harness first reported a limit since it last answered a message.
    pub since: DateTime<Utc>,
    /// When the held messages are tried again.
    pub until: DateTime<Utc>,
}

/// Which provider limit a harness ran into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    /// A rate limit or an overloaded provider, passing within minutes.
    RateLimit,
    /// An exhausted quota or usage allowance, which may take hours or a plan change.
    Quota,
    /// A value from a newer daemon.
    #[serde(other)]
    Unknown,
}

impl LimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimit => "rate-limited",
            Self::Quota => "out-of-quota",
            Self::Unknown => "limited",
        }
    }
}

/// An ant's harness that is signed out, as its roster entry reports it ([`RosterEntry::signed_out`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInNeeded {
    /// When the harness first said it is signed out.
    pub since: DateTime<Utc>,
    /// The command that signs the harness in, run on the entry's machine as the user its daemon runs as, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_in: Option<String>,
}

/// What an ant is doing, as its roster entry reports it.
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
        self.ant.as_str()
    }

    /// Check that the entry was read from its own key. Credentials pin each ant to its own key, while the body is
    /// whatever the writer put there, so an entry naming another ant is forged.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "roster entry", user: self.ant.clone(), at: format!("key {key}") })
    }

    /// The leader of the ant's escalation chain, if it has one.
    pub fn leader(&self) -> Option<&UserId> {
        self.escalate_to.first()
    }
}

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

/// A human's entry in the [`HUMANS_BUCKET`], under the key of its user id. Only humans write it.
///
/// Readers should treat unknown fields as additions, not errors, and skip an entry naming another human than its key
/// ([`HumanEntry::check_key`], [`Humans::add`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanEntry {
    pub human: UserId,
    /// When the human was registered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered_at: Option<DateTime<Utc>>,
}

impl HumanEntry {
    /// The entry of `human`, registered now.
    pub fn new(human: UserId) -> Self {
        Self { human, registered_at: Some(Utc::now()) }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.human.as_str()
    }

    /// Check that the entry was read from its own key; a reader skips one naming another human.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "human entry", user: self.human.clone(), at: format!("key {key}") })
    }
}

/// A reaction in [`REACTIONS_BUCKET`], under its [`reaction_key`]: `by` reacted `reaction` to message `message`
/// ([`Envelope::id`]). Readers take the reactor, message and reaction from the key, which credentials pin to the
/// reactor, not from the entry, which says when as well.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionEntry {
    pub message: Uuid,
    pub by: UserId,
    pub reaction: Reaction,
    pub at: DateTime<Utc>,
}

impl ReactionEntry {
    /// `by`'s `reaction` to `message`, made now.
    pub fn new(by: UserId, message: Uuid, reaction: Reaction) -> Self {
        Self { message, by, reaction, at: Utc::now() }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> String {
        reaction_key(&self.by, &self.message, &self.reaction)
    }
}

/// Which of a human's inbox messages, by [`STREAM`] sequence, the human has read: every one up to `below`, and those
/// in `read` above it. Kept under the human's key in [`READ_BUCKET`] and shared by all its devices, which update it
/// with compare-and-set, so reading on one device marks read on all of them without consuming the message.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadMarker {
    /// Every inbox message of the human at or below this sequence is read.
    #[serde(default)]
    pub below: u64,
    /// Inbox messages above `below` read too, as when only one thread's were read.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub read: std::collections::BTreeSet<u64>,
}

impl ReadMarker {
    pub fn is_read(&self, seq: u64) -> bool {
        seq <= self.below || self.read.contains(&seq)
    }

    /// Mark the message at `seq` read.
    pub fn mark(&mut self, seq: u64) {
        if seq > self.below {
            self.read.insert(seq);
        }
    }

    /// Move `below` past the read messages that follow it, given `inbox`, the sequences, in order, of every inbox
    /// message of the human stored after some sequence at or below `below`: what is read before the first unread one
    /// needs no entry of its own. Sequences of messages gone from the stream are skipped over as well.
    pub fn compact(&mut self, inbox: impl IntoIterator<Item = u64>) {
        for seq in inbox {
            if seq <= self.below {
                continue;
            }
            if !self.read.contains(&seq) {
                break;
            }
            self.below = seq;
        }
        let below = self.below;
        self.read.retain(|seq| *seq > below);
    }
}

/// The title of a thread, under its [`title_key`] in [`THREADS_BUCKET`]: one per thread, shared by every human, which
/// any human sets, renames or clears. `by` is the human that last set it, as its CLI says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadTitle {
    pub thread_id: ThreadId,
    pub title: String,
    pub by: UserId,
    pub at: DateTime<Utc>,
}

impl ThreadTitle {
    /// `title`, trimmed, set on `thread_id` by `by` now: one line of 1 to [`MAX_TITLE`] characters, no control
    /// characters.
    pub fn new(thread_id: ThreadId, title: &str, by: UserId) -> Result<Self, Error> {
        let trimmed = title.trim();
        let count = trimmed.chars().count();
        if count == 0 || count > MAX_TITLE || trimmed.chars().any(char::is_control) {
            return Err(Error::InvalidTitle(title.to_owned()));
        }
        Ok(Self { thread_id, title: trimmed.to_owned(), by, at: Utc::now() })
    }
}

/// A human's pinned and archived threads, under its [`marks_key`] in [`THREADS_BUCKET`], shared by all its devices,
/// which update it with compare-and-set. A thread is pinned or archived, never both: pinning one unarchives it and
/// archiving one unpins it. Marks change nothing else: an archived thread keeps its messages, its work goes on and
/// its replies reach the inbox as before.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadMarks {
    /// When each pinned thread was pinned.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub pinned: std::collections::BTreeMap<ThreadId, DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub archived: std::collections::BTreeMap<ThreadId, Archived>,
}

/// When a thread was archived, and the last [`STREAM`] sequence then: a message on the thread stored after it is new
/// activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Archived {
    pub at: DateTime<Utc>,
    pub after: u64,
}

/// How a human marked a thread ([`ThreadMarks::mark`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Pinned,
    /// Archived, and whether a message was stored on the thread since.
    Archived {
        new_activity: bool,
    },
}

impl ThreadMarks {
    /// Pin `thread`, unarchiving it. Whether that changed anything.
    pub fn pin(&mut self, thread: &ThreadId) -> bool {
        self.archived.remove(thread);
        if self.pinned.contains_key(thread) {
            return false;
        }
        self.pinned.insert(thread.clone(), Utc::now());
        true
    }

    /// Unpin `thread`. Whether it was pinned.
    pub fn unpin(&mut self, thread: &ThreadId) -> bool {
        self.pinned.remove(thread).is_some()
    }

    /// Archive `thread` as of `after`, the last [`STREAM`] sequence now, unpinning it. Whether that changed anything:
    /// archiving it again once a message was stored on it since moves `after` on, so that message is no longer new.
    pub fn archive(&mut self, thread: &ThreadId, after: u64) -> bool {
        let unpinned = self.pinned.remove(thread).is_some();
        if self.archived.get(thread).is_some_and(|a| a.after >= after) {
            return unpinned;
        }
        self.archived.insert(thread.clone(), Archived { at: Utc::now(), after });
        true
    }

    /// Unarchive `thread`. Whether it was archived.
    pub fn unarchive(&mut self, thread: &ThreadId) -> bool {
        self.archived.remove(thread).is_some()
    }

    /// How `thread`, whose last message read has [`STREAM`] sequence `last`, is marked.
    pub fn mark(&self, thread: &ThreadId, last: u64) -> Option<Mark> {
        if self.pinned.contains_key(thread) {
            return Some(Mark::Pinned);
        }
        self.archived.get(thread).map(|a| Mark::Archived { new_activity: last > a.after })
    }
}

/// What a human holds back of its notifications for a while, on all its devices: kept under the human's key in
/// [`NOTIFY_BUCKET`], updated with compare-and-set by `ants notify snooze|resume` and `ants thread mute|unmute`, and
/// read by `ants webhook serve` before each notice. Each hold ends at its deadline, with no one there to lift it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyPrefs {
    /// Until when every notice the human's config does not let through (`bypass`) is held back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snoozed_until: Option<DateTime<Utc>>,
    /// The muted threads, each until when: none of their notices are sent.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub muted: std::collections::BTreeMap<ThreadId, DateTime<Utc>>,
}

impl NotifyPrefs {
    /// Whether a snooze holds at `now`.
    pub fn snoozed(&self, now: DateTime<Utc>) -> bool {
        self.snoozed_until.is_some_and(|until| now < until)
    }

    /// Whether `thread` is muted at `now`.
    pub fn is_muted(&self, thread: &ThreadId, now: DateTime<Utc>) -> bool {
        self.muted.get(thread).is_some_and(|until| now < *until)
    }

    /// Drop the holds that ended by `now`.
    pub fn prune(&mut self, now: DateTime<Utc>) {
        if !self.snoozed(now) {
            self.snoozed_until = None;
        }
        self.muted.retain(|_, until| now < *until);
    }
}

/// The id of one device of a human: each `ants auth user NAME --human` issues credentials for a new device, whose user
/// JWT carries its id as the tag [`DeviceId::tag`]. Generated (a UUIDv7), never chosen and never reused; a label
/// for display is kept beside it where the credentials were issued. Credentials without one are a legacy device's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceId(Uuid);

impl DeviceId {
    /// Prefix of the user JWT tag carrying the device id.
    pub const TAG: &str = "ants-device:";

    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }

    /// The user JWT tag carrying the id.
    pub fn tag(&self) -> String {
        format!("{}{}", Self::TAG, self.0)
    }

    /// The device id among a user JWT's `tags`, if any.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> Option<Self> {
        tags.into_iter().find_map(|tag| tag.strip_prefix(Self::TAG)?.parse().ok())
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for DeviceId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

/// The version of the permissions `ants auth` gives credentials of every kind (ants, humans, webhooks, leaf node
/// uplinks, the channel manager, enrollment), stamped into each user JWT it issues as the tag
/// [`PermissionsVersion::tag`]. Credentials carrying an older version, or none, were issued before the permissions
/// changed: ants processes using them warn and name the command that renews them.
///
/// Bump it whenever the permissions of any role change: [`Permissions::for_user`], [`Permissions::for_leaf`],
/// [`Permissions::for_webhook`], [`Permissions::for_channel_manager`], [`Permissions::for_enrollment`],
/// [`Permissions::for_enroll_approver`] or [`Permissions::with_js_domain`], or the other claims `ants auth` puts in a
/// user JWT.
pub const PERMISSIONS_VERSION: PermissionsVersion = PermissionsVersion(4);

/// The first permissions version whose credentials ask for their own renewal over NATS ([`subject::renew`],
/// [`RenewRequest`]); older ones are renewed on the machine holding the team keys.
pub const RENEW_SINCE: PermissionsVersion = PermissionsVersion(2);

/// A version of the permissions `ants auth` gives ([`PERMISSIONS_VERSION`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PermissionsVersion(pub u32);

impl PermissionsVersion {
    /// Prefix of the user JWT tag carrying the version.
    pub const TAG: &str = "ants-perms:";

    /// The user JWT tag carrying the version.
    pub fn tag(&self) -> String {
        format!("{}{}", Self::TAG, self.0)
    }

    /// The version among a user JWT's `tags`, if any.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> Option<Self> {
        tags.into_iter().find_map(|tag| tag.strip_prefix(Self::TAG)?.parse().ok().map(Self))
    }
}

impl fmt::Display for PermissionsVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What a device of a human follows as it comes, so it needs no push of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Following {
    /// Every message to the human (`ants inbox --follow`), escalations among them.
    Inbox,
    /// The tool requests of every ant (`ants tools list --follow`, or `ants inbox --follow` as text on every thread).
    Approvals,
}

impl Following {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::Approvals => "approvals",
        }
    }
}

/// Key in [`PRESENCE_BUCKET`] of `device` of `human` following `what`. Keys start with the human, so its credentials
/// can be confined to its own.
pub fn presence_key(human: &UserId, device: &DeviceId, what: Following) -> String {
    format!("{human}.{device}.{}", what.as_str())
}

/// That a device of a human follows something ([`Following`]) right now, kept in [`PRESENCE_BUCKET`] under
/// [`presence_key`] while it does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presence {
    pub human: UserId,
    pub device: DeviceId,
    pub following: Following,
    /// When the device started following.
    pub since: DateTime<Utc>,
}

impl Presence {
    pub fn new(human: UserId, device: DeviceId, following: Following) -> Self {
        Self { human, device, following, since: Utc::now() }
    }

    pub fn key(&self) -> String {
        presence_key(&self.human, &self.device, self.following)
    }
}

/// The humans of a team: [`UserId::human`], always, and every user [`HUMANS_BUCKET`] lists under its own key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Humans(std::collections::BTreeSet<UserId>);

impl Default for Humans {
    fn default() -> Self {
        Self([UserId::human()].into())
    }
}

impl Humans {
    /// The default human and the humans of `entries`, as pairs of a KV key and the entry read from it. Entries naming
    /// another human than their key are skipped.
    pub fn from_entries<'a>(entries: impl IntoIterator<Item = (&'a str, HumanEntry)>) -> Self {
        let mut humans = Self::default();
        for (key, entry) in entries {
            let _ = humans.add(key, entry);
        }
        humans
    }

    /// Add the human of `entry`, read from `key`; an entry naming another human is refused and changes nothing.
    pub fn add(&mut self, key: &str, entry: HumanEntry) -> Result<(), Error> {
        entry.check_key(key)?;
        self.0.insert(entry.human);
        Ok(())
    }

    /// Drop `human`, whose entry was deleted. The default human stays a human.
    pub fn remove(&mut self, human: &UserId) {
        if !human.is_human() {
            self.0.remove(human);
        }
    }

    pub fn contains(&self, user: &UserId) -> bool {
        self.0.contains(user)
    }

    /// The role of `user`: [`Role::Human`] for a human, else [`Role::Ant`].
    pub fn role(&self, user: &UserId) -> Role {
        if self.contains(user) { Role::Human } else { Role::Ant }
    }

    pub fn iter(&self) -> impl Iterator<Item = &UserId> {
        self.0.iter()
    }
}

/// One message between ants, or between an ant and a human.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    pub to: UserId,
    pub text: String,
    /// Agent-to-agent replies left on this thread. At zero the thread goes back to a human: its owner, else the
    /// default one.
    pub turn_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    /// The human who started the thread. Replies and the messages an agent sends on or from the thread keep it. A
    /// daemon takes it as a human or a webhook ([`Envelope::webhook`]) says; from an ant, only as stored state says:
    /// on a channel thread, the thread's posts ([`ThreadOrigin`]); on any other, the thread's record in
    /// [`OWNERS_BUCKET`] for the sender, or for an ant that passed the link to the thread on to it, else that of the
    /// thread it was spun off ([`Envelope::origin_thread`]); else there is none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The ant whose top-level channel post started the thread ([`ChannelPost::thread_creator`]), kept like `owner`.
    /// Its escalate_to is the thread's origin chain ([`Escalation::new_from`]); a creator that is a human is none. A
    /// daemon takes it from an ant or a webhook only as the thread's top-level post kept in the stream says
    /// ([`ThreadOrigin`]): on a thread no channel post started, that of the channel thread an ant's message links to
    /// ([`Envelope::origin_thread`]), else there is none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    /// The thread this message's thread was spun off, as when an ant handling a message on it sends one on a new
    /// thread, set while the message has a creator or an owner: the link a daemon checks the `creator` and `owner` of
    /// an ant's message against on a thread no channel post started. Replies and the messages an agent sends on or
    /// from the thread keep it. A daemon takes a linked channel thread's creator and owner only when a top-level
    /// channel post kept in the stream started it and the sender, a member or the messenger of its channel, posted on
    /// it or was woken by a post on it, or such an ant passed the link on to it ([`LINKS_BUCKET`]); a linked message
    /// thread's owner as [`OWNERS_BUCKET`] records it only when a human or webhook messaged the sender on that thread
    /// ([`owner_key`]), or such an ant passed the link on to it; else drops the link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_thread: Option<ThreadId>,
    /// The escalation the message carries up a chain, or answers. Never copied by [`Envelope::reply`] or
    /// [`Envelope::follow_up`]: the daemon sets it with [`Envelope::with_escalation`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<Box<Escalation>>,
    /// An agent's decision on an approval request of `to`, for its daemon rather than its harness. The subject pins the
    /// sender, and the requester takes it only from its own chain ([`Envelope::chain_decision`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<Box<ApprovalDecision>>,
    /// A webhook's message, as `ants webhook serve` marks it: its sender is no human, whatever the humans bucket says,
    /// and an answer to it goes to the thread's owner, if any, not to the webhook's inbox, which nobody reads
    /// ([`Envelope::sender_role`]). A daemon takes it only from a sender whose marker is in [`WEBHOOKS_BUCKET`]
    /// ([`webhook_marker_key`]), which only a webhook's credentials write: from any other it is cleared, so no ant
    /// sends an answer to the owner it names.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub webhook: bool,
    /// The channel post that @mentioned `to`, a human member of the channel, when the message tells it so
    /// ([`ChannelPost::mention_notice`]): `from` is the post's author, or the channel's messenger relaying a webhook's
    /// post ([`ChannelPost::relayed_notice`]), the thread and text are the post's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mention: Option<Mention>,
    pub sent_at: DateTime<Utc>,
}

/// Where the channel post a mention notice tells of is ([`Envelope::mention`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mention {
    pub channel: ChannelId,
    /// The post's id.
    pub post: Uuid,
    /// The post's author, when the notice is relayed by another ([`ChannelPost::relayed_notice`]): the envelope's
    /// `from` sent it. It is the sender's word only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<UserId>,
}

impl Envelope {
    /// Start a new thread.
    pub fn new(from: UserId, to: UserId, text: impl Into<String>) -> Self {
        Self::on_thread(ThreadId::generate(), from, to, text)
    }

    /// A message on an existing thread, with a fresh budget and no owner.
    pub fn on_thread(thread_id: ThreadId, from: UserId, to: UserId, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            thread_id,
            from,
            to,
            text: text.into(),
            turn_budget: DEFAULT_TURN_BUDGET,
            in_reply_to: None,
            owner: None,
            creator: None,
            origin_thread: None,
            escalation: None,
            approval: None,
            webhook: false,
            mention: None,
            sent_at: Utc::now(),
        }
    }

    /// The envelope with `owner` as the human who started its thread.
    pub fn with_owner(self, owner: Option<UserId>) -> Self {
        Self { owner, ..self }
    }

    /// The envelope as a webhook's message ([`Envelope::webhook`]).
    pub fn from_webhook(self) -> Self {
        Self { webhook: true, ..self }
    }

    /// The role of the sender, given the `humans`: an ant's for a webhook's message, else as `humans` says.
    pub fn sender_role(&self, humans: &Humans) -> Role {
        if self.webhook { Role::Ant } else { humans.role(&self.from) }
    }

    /// The envelope with `creator` as the ant that started its thread with a channel post.
    pub fn with_creator(self, creator: Option<UserId>) -> Self {
        Self { creator, ..self }
    }

    /// The envelope linking to `origin_thread`, the thread its creator or owner comes from
    /// ([`Envelope::origin_thread`]): none without a creator or an owner, set first, or on that thread itself.
    pub fn with_origin_thread(self, origin_thread: Option<ThreadId>) -> Self {
        let origin_thread = origin_thread.filter(|t| self.links_origin() && *t != self.thread_id);
        Self { origin_thread, ..self }
    }

    /// The envelope carrying `escalation`.
    pub fn with_escalation(self, escalation: Option<Escalation>) -> Self {
        Self { escalation: escalation.map(Box::new), ..self }
    }

    /// `from`'s decision on an approval request, to the requesting ant on `thread_id`, the request's thread.
    pub fn decision(thread_id: ThreadId, from: UserId, decision: ApprovalDecision) -> Self {
        let text = format!("{} on approval request {}", decision.verdict.as_str(), decision.request_id);
        let to = decision.ant.clone();
        Self { approval: Some(Box::new(decision)), ..Self::on_thread(thread_id, from, to, text) }
    }

    /// Whether the sender is in `chain`, the escalate_to of the ant the envelope is for. Trust it only once the
    /// envelope passed [`Envelope::check_subject`].
    pub fn is_from_chain(&self, chain: &[UserId]) -> bool {
        chain.contains(&self.from)
    }

    /// The approval decision the envelope carries on `request`, if it comes from `chain`, the requester's escalate_to,
    /// and is for that request. A verdict the request did not offer counts as [`Verdict::Reject`]. Trust it only once
    /// the envelope passed [`Envelope::check_subject`].
    pub fn chain_decision(&self, request: &ApprovalRequest, chain: &[UserId]) -> Option<ApprovalDecision> {
        let decision = self.approval.as_ref()?;
        let ours = self.to == request.ant && decision.ant == request.ant;
        (ours && decision.request_id == request.id && self.is_from_chain(chain))
            .then(|| request.decide(decision.verdict))
    }

    /// A message on this envelope's thread, with a fresh budget and the same owner, creator and origin thread.
    pub fn follow_up(&self, from: UserId, to: UserId, text: impl Into<String>) -> Self {
        let follow_up = Self::on_thread(self.thread_id.clone(), from, to, text)
            .with_owner(self.owner.clone())
            .with_creator(self.creator.clone());
        Self { origin_thread: self.origin_thread.clone(), ..follow_up }
    }

    /// The envelope moved to `thread`, spun off this one's: it keeps the link to the thread its creator or owner
    /// comes from ([`Envelope::origin_thread`]), this thread unless it was spun off one itself.
    pub fn spun_off(self, thread: ThreadId) -> Self {
        let origin = self.origin_thread.clone().unwrap_or_else(|| self.thread_id.clone());
        let origin_thread = (self.links_origin() && origin != thread).then_some(origin);
        Self { thread_id: thread, origin_thread, ..self }
    }

    /// The thread whose link the envelope passes on to its recipient ([`LINKS_BUCKET`]): the one it was spun off
    /// ([`Envelope::origin_thread`]), else its own while it has a creator or an owner.
    pub fn passes_on(&self) -> Option<&ThreadId> {
        self.origin_thread.as_ref().or_else(|| self.links_origin().then_some(&self.thread_id))
    }

    /// Whether the envelope has a creator or an owner for a link to its origin thread to vouch for.
    fn links_origin(&self) -> bool {
        self.creator.is_some() || self.owner.is_some()
    }

    /// Reply to the sender, whose role is `to`, keeping the thread's owner, creator and origin thread. Replies to a human are free;
    /// agent-to-agent replies spend one turn, and fail when none are left, so the caller escalates to a human instead
    /// of looping.
    pub fn reply(&self, from: UserId, text: impl Into<String>, to: Role) -> Result<Self, Error> {
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
            creator: self.creator.clone(),
            origin_thread: self.origin_thread.clone(),
            escalation: None,
            approval: None,
            webhook: false,
            mention: None,
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
    /// The agent asked for help with the escalate tool.
    Asked,
    /// A prompt does not fit the model's context window, even in a fresh session.
    ContextWindow,
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

/// A post to the notes of a thread, on [`subject::notes`]: one shared board per thread, where the ants on it coordinate
/// together; a post wakes nobody, and each of them can read the whole board.
///
/// Posts spend the thread's turn budget like messages between agents do, and each carries what is left. The post that
/// spends the last turn hands the thread to the human, and the notes take no more agent posts until the human posts
/// there with a fresh budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotesPost {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    pub text: String,
    /// Turns left in the notes after this post.
    pub turn_budget: u32,
    pub sent_at: DateTime<Utc>,
}

impl NotesPost {
    /// A post with a fresh budget, as the human posts.
    pub fn new(thread_id: ThreadId, from: UserId, text: impl Into<String>) -> Self {
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
    /// is handling, and those the notes' `last` post left. Fails when none are left.
    pub fn spend(
        thread_id: ThreadId,
        from: UserId,
        text: impl Into<String>,
        budget: u32,
        last: Option<&NotesPost>,
    ) -> Result<Self, Error> {
        let left = last.map_or(budget, |last| last.turn_budget.min(budget));
        let turn_budget = left.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(thread_id.clone()))?;
        Ok(Self { turn_budget, ..Self::new(thread_id, from, text) })
    }

    pub fn subject(&self) -> String {
        subject::notes(&self.thread_id, &self.from)
    }

    /// Check that the post arrived on its own [`NotesPost::subject`]. Credentials pin the sender token of a notes
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongNotesSubject {
            subject: subject.to_owned(),
            from: self.from.clone(),
            thread: self.thread_id.clone(),
        })
    }
}

/// The users `text` @mentions, in the order of their first mention.
///
/// A mention is `@` and a user id, with the `@` at the start of the text or after whitespace or an opening bracket
/// or quote, so an address such as `a@b` mentions nobody. Punctuation after the id (`,.:;!?)` and the like) ends it;
/// a letter, `_`, `@` or a dot followed by a letter or digit (`@host.example`) means it was no user id.
pub fn parse_mentions(text: &str) -> Vec<UserId> {
    let mut found: Vec<UserId> = Vec::new();
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
                && let Ok(id) = name.parse::<UserId>()
                && !found.contains(&id)
            {
                found.push(id);
            }
        }
        prev = Some(c);
    }
    found
}

/// A channel's entry in the [`CHANNELS_BUCKET`], under the key of its channel id. Humans write it, and the channel
/// manager on its owner's requests ([`ChannelEntry::apply`]).
///
/// Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelEntry {
    pub channel: ChannelId,
    /// The users in the channel, ants and humans, in the order they were added.
    #[serde(default)]
    pub members: Vec<UserId>,
    /// The ant that triages humans' posts that @mention nobody, when not the default `messenger`
    /// ([`ChannelEntry::messenger`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messenger: Option<UserId>,
    /// The ant that may change the members and the messenger through the channel manager. Only a human sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
}

impl ChannelEntry {
    /// A channel without members, messenger or owner.
    pub fn new(channel: ChannelId) -> Self {
        Self { channel, members: Vec::new(), messenger: None, owner: None }
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

    pub fn is_member(&self, user: &UserId) -> bool {
        self.members.contains(user)
    }

    /// The ant that triages humans' posts that @mention nobody: the one assigned, or else the default
    /// [`UserId::messenger`]. It need not be a member.
    pub fn messenger(&self) -> UserId {
        self.messenger.clone().unwrap_or_else(UserId::messenger)
    }

    /// Add `user`, an ant or a human, after the other members; false when it already is one.
    pub fn add_member(&mut self, user: UserId) -> bool {
        if self.is_member(&user) {
            return false;
        }
        self.members.push(user);
        true
    }

    /// Remove `user` from the members; false when it was none.
    pub fn remove_member(&mut self, user: &UserId) -> bool {
        let before = self.members.len();
        self.members.retain(|m| m != user);
        self.members.len() != before
    }

    /// The ants `post` wakes: the members it @mentions, except its author and the `humans`; mentions of anyone
    /// else wake nobody. A post of a human or a webhook that mentions nobody wakes the channel's
    /// [`ChannelEntry::messenger`] instead, to triage it ([`ChannelPost::to_triage`]); an agent's post that mentions
    /// nobody wakes nobody. A post to another channel wakes nobody here.
    pub fn woken(&self, post: &ChannelPost, humans: &Humans) -> Vec<UserId> {
        if post.channel != self.channel {
            return Vec::new();
        }
        if post.mentions.is_empty() {
            return if post.to_triage(humans) { vec![self.messenger()] } else { Vec::new() };
        }
        post.mentions.iter().filter(|m| self.is_member(m) && **m != post.from && !humans.contains(m)).cloned().collect()
    }

    /// Who sends the mention notices of `post` ([`ChannelEntry::notified`]): its author, or for a webhook's post the
    /// channel's [`ChannelEntry::messenger`], since a webhook's credentials publish only to its destination
    /// ([`ChannelPost::relayed_notice`]). A reader checks [`ChannelPost::webhook`] against the webhook's marker first.
    pub fn notifier(&self, post: &ChannelPost) -> UserId {
        if post.webhook { self.messenger() } else { post.from.clone() }
    }

    /// The `humans` `post` notifies ([`ChannelPost::mention_notice`]): the members among them it @mentions, except its
    /// author. They are never woken: a mention of a human wakes no agent, and keeps the post from triage. A post to
    /// another channel notifies nobody here.
    pub fn notified(&self, post: &ChannelPost, humans: &Humans) -> Vec<UserId> {
        if post.channel != self.channel {
            return Vec::new();
        }
        post.mentions.iter().filter(|m| self.is_member(m) && **m != post.from && humans.contains(m)).cloned().collect()
    }

    /// Apply the `ops` of `requester`, which must be the channel's owner, leaving the owner as it is. Members may be
    /// ants or `humans`, but none may make a human the messenger: it is an ant. Returns what changed, in order, empty
    /// when nothing did; on a refusal the entry is unchanged.
    pub fn apply(&mut self, requester: &UserId, ops: &[ChannelOp], humans: &Humans) -> Result<Vec<String>, Refusal> {
        if self.owner.as_ref() != Some(requester) {
            return Err(Refusal::NotOwner { owner: self.owner.clone() });
        }
        if let Some(human) = ops.iter().find_map(|op| match op {
            ChannelOp::SetMessenger { ant } if humans.contains(ant) => Some(ant),
            _ => None,
        }) {
            return Err(Refusal::Human(human.clone()));
        }
        let mut changed = Vec::new();
        for op in ops {
            match op {
                ChannelOp::AddMember { ant } => {
                    if self.add_member(ant.clone()) {
                        changed.push(format!("added {ant}"));
                    }
                }
                ChannelOp::RemoveMember { ant } => {
                    if self.remove_member(ant) {
                        changed.push(format!("removed {ant}"));
                    }
                }
                ChannelOp::SetMessenger { ant } => {
                    if self.messenger() != *ant {
                        changed.push(format!("messenger {ant}"));
                    }
                    self.messenger = (*ant != UserId::messenger()).then(|| ant.clone());
                }
                ChannelOp::ClearMessenger => {
                    if self.messenger.take().is_some() {
                        changed.push(format!("messenger back to the default {}", UserId::messenger()));
                    }
                }
            }
        }
        Ok(changed)
    }
}

/// One change an owner asks for in a [`ChannelAdminRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ChannelOp {
    /// Make `ant`, an ant or a human despite the field's name, a member.
    AddMember { ant: UserId },
    /// Remove the member `ant`, an ant or a human.
    RemoveMember { ant: UserId },
    /// Make `ant` the channel's messenger; the default `messenger` clears it.
    SetMessenger { ant: UserId },
    /// Give the channel the default messenger again.
    ClearMessenger,
}

impl ChannelOp {
    /// The user the operation names, if any.
    pub fn ant(&self) -> Option<&UserId> {
        match self {
            Self::AddMember { ant } | Self::RemoveMember { ant } | Self::SetMessenger { ant } => Some(ant),
            Self::ClearMessenger => None,
        }
    }

    /// Whether the operation makes [`ChannelOp::ant`] a member or the messenger, so it must be a human or an ant in
    /// the roster.
    pub fn adds(&self) -> bool {
        matches!(self, Self::AddMember { .. } | Self::SetMessenger { .. })
    }
}

/// An ant's request to the channel manager for changes to a channel it owns, on [`subject::chanadm`]: core NATS, not
/// stored. The ant repeats it with the same id while no answer arrives ([`CHANADM_REPEAT`]); the manager answers a
/// repeat with its first answer. Only the requester's reply inboxes get the answer ([`ChannelAdminRequest::answers_on`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelAdminRequest {
    pub id: Uuid,
    pub channel: ChannelId,
    /// The ant asking, which must own the channel.
    pub ant: UserId,
    /// The thread the ant was handling when it asked, for the record.
    pub thread_id: ThreadId,
    /// What to change, in order: 1 to [`MAX_CHANNEL_OPS`] operations.
    pub ops: Vec<ChannelOp>,
    pub requested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ChannelAdminRequest {
    /// A request expiring [`CHANADM_TTL`] from now.
    pub fn new(channel: ChannelId, ant: UserId, thread_id: ThreadId, ops: Vec<ChannelOp>) -> Self {
        let requested_at = Utc::now();
        let expires_at = requested_at + chrono::TimeDelta::from_std(CHANADM_TTL).expect("a short duration");
        Self { id: Uuid::now_v7(), channel, ant, thread_id, ops, requested_at, expires_at }
    }

    pub fn subject(&self) -> String {
        subject::chanadm(&self.channel, &self.ant)
    }

    /// Check that the request arrived on its own [`ChannelAdminRequest::subject`]. Credentials pin each ant to its own
    /// subjects, while the body is whatever the publisher wrote, so a request naming another ant or channel is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "channel management request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// Whether `reply`, the reply subject the request came with, is one of the requester's own reply inboxes, where
    /// only its credentials (and its leaf's uplink) listen. Not the inbox messages its stream republishes there.
    pub fn answers_on(&self, reply: &str) -> bool {
        let prefix = format!("{}.", inbox_prefix(&self.ant));
        reply.strip_prefix(&prefix).is_some_and(|rest| !rest.is_empty() && !rest.starts_with("stored."))
    }

    /// Why the request is malformed, if it is: no operation, or more than [`MAX_CHANNEL_OPS`].
    pub fn check_ops(&self) -> Result<(), Refusal> {
        match self.ops.len() {
            0 => Err(Refusal::Invalid("no change asked for".to_owned())),
            n if n > MAX_CHANNEL_OPS => Err(Refusal::Invalid(format!("{n} changes; at most {MAX_CHANNEL_OPS}"))),
            _ => Ok(()),
        }
    }
}

/// The channel manager's answer to a [`ChannelAdminRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelAdminReply {
    pub request_id: Uuid,
    pub outcome: ChannelAdminOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ChannelAdminOutcome {
    /// The entry as stored at KV `revision` after the request, and what it changed; nothing changed and nothing was
    /// written when `changed` is empty.
    Applied {
        entry: ChannelEntry,
        revision: u64,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changed: Vec<String>,
    },
    Refused {
        refusal: Refusal,
    },
}

/// Why the channel manager did not apply a [`ChannelAdminRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "refusal", content = "detail", rename_all = "snake_case")]
pub enum Refusal {
    #[error("only the channel's owner may change it; {}", match owner {
        Some(owner) => format!("its owner is {owner}"),
        None => "it has no owner".to_owned(),
    })]
    NotOwner { owner: Option<UserId> },
    #[error("no such channel")]
    NoSuchChannel,
    #[error("the channel's entry is unreadable; a human has to set it again")]
    Unreadable,
    #[error("{0} is a human; the messenger is an ant")]
    Human(UserId),
    #[error("{0} is neither a human nor in the roster; only humans and ants in the roster may be added")]
    NotInRoster(UserId),
    #[error("the request expired")]
    Expired,
    #[error("the channel kept changing meanwhile; try again")]
    Busy,
    #[error("invalid request: {0}")]
    Invalid(String),
}

/// A post to a channel, on [`subject::chan`]: like a Slack message, either a top-level post that starts a thread or a
/// post on an existing thread.
///
/// Posts spend the thread's turn budget like messages between agents do: every agent's post spends a turn of what the
/// thread has left ([`ChannelPost::turns_left`]), and at zero the thread goes to its owner, the human whose post started
/// it, else the default human; any human's post gives the thread a fresh budget. Each post carries the turns the thread
/// has left after it, the thread's owner when known, and the ant that created the thread when an ant did.
///
/// Who is a human is the caller's to say, from the [`HUMANS_BUCKET`], as [`Humans`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelPost {
    pub id: Uuid,
    pub channel: ChannelId,
    /// The thread the post is on. A top-level post starts a thread with its own id ([`ChannelPost::starts_thread`]).
    pub thread_id: ThreadId,
    pub from: UserId,
    pub text: String,
    /// The users the post @mentions, in order ([`parse_mentions`] of the text, when built here).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<UserId>,
    /// Agent-to-agent turns left on the thread after this post.
    pub turn_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    /// The human who started the thread. Replies keep it ([`ChannelPost::reply`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The ant whose top-level post started the thread, never a human. Replies keep it ([`ChannelPost::reply`]); a
    /// reader takes it from a top-level post's author only ([`ChannelPost::thread_creator`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    /// A webhook's post, as `ants webhook serve` marks it: one that mentions nobody goes to the channel's messenger, as
    /// a human's does ([`ChannelPost::to_triage`]). It is the poster's claim: a daemon takes it only from a sender whose
    /// marker is in [`WEBHOOKS_BUCKET`] ([`webhook_marker_key`]) or a human, so no ant names a thread's owner with it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub webhook: bool,
    pub sent_at: DateTime<Utc>,
}

impl ChannelPost {
    /// A top-level post with a fresh budget and no owner: it starts a new thread, whose id is the post's own.
    pub fn new(channel: ChannelId, from: UserId, text: impl Into<String>) -> Self {
        let id = Uuid::now_v7();
        Self::build(id, channel, ThreadId(id.to_string()), from, text.into())
    }

    /// A post on an existing thread, with a fresh budget and no owner, as a human posts.
    pub fn on_thread(channel: ChannelId, thread_id: ThreadId, from: UserId, text: impl Into<String>) -> Self {
        Self::build(Uuid::now_v7(), channel, thread_id, from, text.into())
    }

    fn build(id: Uuid, channel: ChannelId, thread_id: ThreadId, from: UserId, text: String) -> Self {
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
            creator: None,
            webhook: false,
            sent_at: Utc::now(),
        }
    }

    /// The post with `owner` as the human who started its thread.
    pub fn with_owner(self, owner: Option<UserId>) -> Self {
        Self { owner, ..self }
    }

    /// The post with `creator` as the ant that started its thread.
    pub fn with_creator(self, creator: Option<UserId>) -> Self {
        Self { creator, ..self }
    }

    /// The ant that created the post's thread, none of the `humans`, no webhook and not the channel manager: of a
    /// top-level post its author, whatever the post says, so only the author's credentials make an ant a thread's
    /// creator; of a later post the one it carries, which its author chose: a reader takes a thread's creator from
    /// its stored posts instead ([`ThreadOrigin`]). Trust it only once the post passed [`ChannelPost::check_subject`].
    pub fn thread_creator(&self, humans: &Humans) -> Option<UserId> {
        let creator = if self.starts_thread() { (!self.webhook).then_some(&self.from) } else { self.creator.as_ref() };
        creator.filter(|c| !humans.contains(c) && **c != UserId::channel_manager()).cloned()
    }

    /// The post as a webhook's, mentioning exactly `mentions`, whatever its text says.
    pub fn from_webhook(self, mentions: Vec<UserId>) -> Self {
        Self { mentions, webhook: true, ..self }
    }

    /// Whether the post goes to the channel's messenger to triage: it mentions nobody and is a post of one of the
    /// `humans` or a webhook's.
    pub fn to_triage(&self, humans: &Humans) -> bool {
        self.mentions.is_empty() && (self.webhook || humans.contains(&self.from))
    }

    /// Whether the post is a human's: its author is one of the `humans` and it is no webhook's, so a webhook a human's
    /// name was registered for never posts as that human.
    pub fn by_human(&self, humans: &Humans) -> bool {
        !self.webhook && humans.contains(&self.from)
    }

    /// The owner of the post's thread, one of the `humans`: the one it carries, else its author when a human
    /// ([`ChannelPost::by_human`]), as for a post stored before posts carried owners. An owner that is no human is
    /// ignored: only a human owns a thread. The owner a post carries is its author's choice: a reader takes a thread's
    /// owner from its stored posts instead ([`ThreadOrigin`]).
    pub fn thread_owner(&self, humans: &Humans) -> Option<UserId> {
        let carried = self.owner.as_ref().filter(|owner| humans.contains(owner));
        carried.cloned().or_else(|| self.by_human(humans).then(|| self.from.clone()))
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
            if post.by_human(humans) { post.turn_budget } else { left.min(post.turn_budget) }
        })
    }

    /// A reply on the same channel and thread, after the posts stored on it `later` than this one, keeping the
    /// thread's owner ([`ChannelPost::thread_owner`]) and creator ([`ChannelPost::thread_creator`]); a human's reply on
    /// a thread with no known owner takes it. A
    /// human's reply has a fresh budget; an agent's spends one of the [`ChannelPost::turns_left`].
    pub fn reply(
        &self,
        from: UserId,
        text: impl Into<String>,
        later: &[ChannelPost],
        humans: &Humans,
    ) -> Result<Self, Error> {
        let mut reply = Self::on_thread(self.channel.clone(), self.thread_id.clone(), from, text);
        reply.in_reply_to = Some(self.id);
        reply.owner = self.thread_owner(humans).or_else(|| reply.thread_owner(humans));
        reply.creator = self.thread_creator(humans);
        if humans.contains(&reply.from) { Ok(reply) } else { reply.spend(self.turns_left(later, humans)) }
    }

    /// Whether the post is a top-level one, which started its thread.
    pub fn starts_thread(&self) -> bool {
        self.thread_id.as_str() == self.id.to_string()
    }

    /// Whether the post @mentions `user`.
    pub fn mentions(&self, user: &UserId) -> bool {
        self.mentions.contains(user)
    }

    /// The message telling `human`, a human member of the channel it @mentions ([`ChannelEntry::notified`]), of the
    /// post: from its author, on its thread, with its text, and the channel and post in [`Envelope::mention`]. It
    /// spends no turn and names no owner or creator. Sent with [`ChannelPost::mention_msg_id`].
    pub fn mention_notice(&self, human: &UserId) -> Envelope {
        Envelope {
            in_reply_to: None,
            turn_budget: self.turn_budget,
            webhook: self.webhook,
            mention: Some(Mention { channel: self.channel.clone(), post: self.id, author: None }),
            sent_at: self.sent_at,
            ..Envelope::on_thread(self.thread_id.clone(), self.from.clone(), human.clone(), self.text.clone())
        }
    }

    /// The [`ChannelPost::mention_notice`] to `human` as `relay` sends it for the post's author, a webhook
    /// ([`ChannelEntry::notifier`]): from `relay`, no webhook's, naming the author in [`Mention::author`].
    pub fn relayed_notice(&self, human: &UserId, relay: &UserId) -> Envelope {
        let notice = self.mention_notice(human);
        let mention = notice.mention.map(|mention| Mention { author: Some(self.from.clone()), ..mention });
        Envelope { from: relay.clone(), webhook: false, mention, ..notice }
    }

    /// The JetStream msg id of the [`ChannelPost::mention_notice`] to `human`, so a notice sent again is stored once.
    pub fn mention_msg_id(&self, human: &UserId) -> String {
        format!("mention.{}.{human}", self.id)
    }

    /// Whether the post @mentions an ant other than its author and the `humans`: an agent it would wake.
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

/// Who started a channel thread, as its stored posts say. Readers go by it rather than by the `owner` and `creator`
/// a later post carries, which its author chose: an ant could otherwise name any creator, and so the chain the
/// thread's escalations climb, or any owner.
///
/// Fed the posts stored on one thread in stream order ([`ThreadOrigin::push`]), each checked against its subject
/// ([`ChannelPost::check_subject`]):
/// - the thread's top-level post is the first one read, when it starts the thread: a later post with the thread's id
///   only claims to. Its author is the creator when an ant ([`ChannelPost::thread_creator`]);
/// - the owner is the first human to post on the thread ([`ChannelPost::by_human`]), the author of the top-level post
///   when a human; a webhook's top-level post may name the owner, a human, as its config does.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ThreadOrigin {
    /// Whether the thread's top-level post was read.
    pub root: bool,
    /// Whether any post on the thread was read.
    seen: bool,
    pub owner: Option<UserId>,
    pub creator: Option<UserId>,
}

impl ThreadOrigin {
    /// The origin of the thread that `root`, a top-level post, starts, from that post alone.
    pub fn of_root(root: &ChannelPost, humans: &Humans) -> Self {
        let mut origin = Self::default();
        origin.push(root, humans);
        origin
    }

    /// Take `post`, the next post stored on the thread.
    pub fn push(&mut self, post: &ChannelPost, humans: &Humans) {
        let root = !self.seen && post.starts_thread();
        self.seen = true;
        if root {
            self.root = true;
            self.creator = post.thread_creator(humans);
        }
        if self.owner.is_none() {
            self.owner = if post.by_human(humans) {
                Some(post.from.clone())
            } else if root && post.webhook {
                post.owner.clone().filter(|owner| humans.contains(owner))
            } else {
                None
            };
        }
    }

    /// Whether later posts change nothing: the top-level post was read and the thread has its owner.
    pub fn settled(&self) -> bool {
        self.root && self.owner.is_some()
    }

    /// Put the thread's owner and creator on `post`, a post on the thread, in place of those it carries.
    pub fn apply(&self, post: &mut ChannelPost) {
        post.owner.clone_from(&self.owner);
        post.creator.clone_from(&self.creator);
    }
}

/// A message an ant gave up on: it failed on every allowed delivery, or was not an envelope. Kept on
/// [`subject::dead`] until the human replays or drops it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// The ant whose inbox held the message.
    pub ant: UserId,
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
        ant: UserId,
        stream_seq: u64,
        deliveries: u64,
        error: impl Into<String>,
        envelope: Envelope,
    ) -> Self {
        Self {
            ant,
            stream_seq,
            deliveries,
            error: error.into(),
            died_at: Utc::now(),
            envelope: Some(envelope),
            payload: None,
        }
    }

    /// The dead letter of a payload that is not an envelope; keeps at most [`Self::PAYLOAD_EXCERPT`] bytes of it.
    pub fn of_payload(ant: UserId, stream_seq: u64, deliveries: u64, error: impl Into<String>, payload: &[u8]) -> Self {
        // A cut through a multi-byte character leaves a replacement character, which is fine for inspection.
        let payload = String::from_utf8_lossy(&payload[..payload.len().min(Self::PAYLOAD_EXCERPT)]).into_owned();
        Self {
            ant,
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
        subject::dead(&self.ant)
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

/// A tool request outside an ant's allowlist, published to `ants.approvals.<ant>`. It is pending until a
/// [`ApprovalDecision`] arrives on [`ApprovalRequest::decision_subject`] or `expires_at` passes; then the ant
/// rejects it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: Uuid,
    pub ant: UserId,
    pub thread_id: ThreadId,
    pub tool: ToolCall,
    /// The verdicts the harness offers. [`Verdict::Reject`] is always among them.
    pub options: Vec<Verdict>,
    pub requested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ApprovalRequest {
    pub fn new(
        ant: UserId,
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
        Self { id: Uuid::now_v7(), ant, thread_id, tool, options, requested_at, expires_at }
    }

    pub fn subject(&self) -> String {
        subject::approvals(&self.ant)
    }

    pub fn decision_subject(&self) -> String {
        subject::approval_decision(&self.ant, &self.id)
    }

    /// Check that the request arrived on its own [`ApprovalRequest::subject`]. Credentials pin each ant to its own
    /// subject, while the body is whatever the publisher wrote, so a request naming another ant is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "approval request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// The human's answer. A verdict the harness did not offer counts as [`Verdict::Reject`].
    pub fn decide(&self, verdict: Verdict) -> ApprovalDecision {
        ApprovalDecision {
            request_id: self.id,
            ant: self.ant.clone(),
            verdict: if self.options.contains(&verdict) { verdict } else { Verdict::Reject },
            decided_at: Utc::now(),
        }
    }
}

/// The answer to an [`ApprovalRequest`]: a human's, published to `ants.approvals.<ant>.<request id>`, or an agent's
/// of the ant's chain, on [`Envelope::approval`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub request_id: Uuid,
    pub ant: UserId,
    pub verdict: Verdict,
    pub decided_at: DateTime<Utc>,
}

impl ApprovalDecision {
    /// A decision on the request `request_id` of `ant`, made now, as an agent of its chain makes one from the
    /// [`Escalation`] alone. The requester maps a verdict it did not offer to [`Verdict::Reject`].
    pub fn new(request_id: Uuid, ant: UserId, verdict: Verdict) -> Self {
        Self { request_id, ant, verdict, decided_at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::approval_decision(&self.ant, &self.request_id)
    }
}

/// A machine asking for the credentials of `ant` on [`subject::enroll`]: the public key of the user NKey it
/// generated, whose seed stays there. The machine holding the account key signs a user JWT for that key, or refuses,
/// and answers with an [`EnrollReply`].
///
/// The request carries the user JWT of the enrollment credentials it is sent with and their key's `signature` of
/// [`EnrollRequest::signed`], so the approver takes only requests of a holder of the ant's enrollment credentials,
/// whatever connection carried them (a leaf node's uplink may carry anyone's).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollRequest {
    pub ant: UserId,
    /// The user's public NKey, `U...`.
    pub public_key: String,
    /// Host name of the asking machine, as it reports it.
    pub host: String,
    pub sent_at: DateTime<Utc>,
    /// The user JWT of the enrollment credentials of `ant`.
    pub enrollment: String,
    /// Signature of [`EnrollRequest::signed`] by the enrollment credentials' key, base64url without padding.
    pub signature: String,
    /// The leaf node the asking machine reaches the hub through, whose uplink the approval extends to the ant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leaf: Option<EnrollLeaf>,
}

/// The leaf node of an asking machine ([`EnrollRequest::leaf`]): its machine name, the user JWT of its uplink
/// credentials, and their key's signature of [`EnrollRequest::signed`], base64url without padding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollLeaf {
    pub machine: Machine,
    pub jwt: String,
    pub signature: String,
}

impl EnrollRequest {
    /// A request sent now, to be completed with the enrollment credentials' JWT and signatures.
    pub fn new(ant: UserId, public_key: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            ant,
            public_key: public_key.into(),
            host: host.into(),
            sent_at: Utc::now(),
            enrollment: String::new(),
            signature: String::new(),
            leaf: None,
        }
    }

    /// What the enrollment key and the leaf's uplink key sign: every field but the signatures, one per line.
    pub fn signed(&self) -> Vec<u8> {
        let sent_at = self.sent_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let (machine, leaf) = self.leaf.as_ref().map_or(("", ""), |l| (l.machine.as_str(), l.jwt.as_str()));
        let fields =
            ["ants-enroll", self.ant.as_str(), &self.public_key, &sent_at, &self.enrollment, machine, leaf, &self.host];
        fields.join("\n").into_bytes()
    }

    pub fn subject(&self) -> String {
        subject::enroll(&self.ant)
    }

    /// Check that the request arrived on its own [`EnrollRequest::subject`], which enrollment credentials pin.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "enrollment request", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// The answer to an [`EnrollRequest`], on [`subject::enroll_reply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum EnrollReply {
    /// The user JWT of `public_key`, which completes the ant's credentials with the seed kept on its machine, and
    /// for a request through a leaf node ([`EnrollRequest::leaf`]) the renewed user JWT of the leaf's uplink key,
    /// allowing the ant as well.
    Approved {
        ant: UserId,
        public_key: String,
        jwt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        leaf_jwt: Option<String>,
    },
    /// No credentials for `public_key`, or for any key asking for `ant` when it is `None`.
    Rejected {
        ant: UserId,
        public_key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl EnrollReply {
    pub fn ant(&self) -> &UserId {
        match self {
            Self::Approved { ant, .. } | Self::Rejected { ant, .. } => ant,
        }
    }

    pub fn subject(&self) -> String {
        subject::enroll_reply(self.ant())
    }

    /// Whether the answer is meant for the request of `public_key`.
    pub fn answers(&self, public_key: &str) -> bool {
        match self {
            Self::Approved { public_key: key, .. } => key == public_key,
            Self::Rejected { public_key: key, .. } => key.as_deref().is_none_or(|key| key == public_key),
        }
    }
}

/// A holder of credentials asking for them renewed on [`subject::renew`] (`ants renew`): the public key of the user
/// NKey it generated, whose seed stays with it. The machine holding the account key signs a user JWT for that key with
/// the same name and kind as the credentials renewed, with the permissions their kind has now, and answers with a
/// [`RenewReply`].
///
/// The request carries the user JWT of the credentials renewed (`current`) and their key's `signature` of
/// [`RenewRequest::signed`], so the approver renews only credentials whose holder asks, whatever connection carried the
/// request: a human's carries the uplink credentials of its machine's leaf node, which connect as no client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewRequest {
    /// The name the credentials carry ([`subject::renewable_name`]).
    pub name: String,
    /// The new user's public NKey, `U...`.
    pub public_key: String,
    /// Host name of the asking machine, as it reports it.
    pub host: String,
    pub sent_at: DateTime<Utc>,
    /// The user JWT of the credentials renewed.
    pub current: String,
    /// Signature of [`RenewRequest::signed`] by the key of the credentials renewed, base64url without padding.
    pub signature: String,
}

impl RenewRequest {
    /// A request sent now, to be completed with the current credentials' JWT and signature.
    pub fn new(name: impl Into<String>, public_key: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            public_key: public_key.into(),
            host: host.into(),
            sent_at: Utc::now(),
            current: String::new(),
            signature: String::new(),
        }
    }

    /// What the key of the credentials renewed signs: every field but the signature, one per line.
    pub fn signed(&self) -> Vec<u8> {
        let sent_at = self.sent_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        ["ants-renew", &self.name, &self.public_key, &sent_at, &self.current, &self.host].join("\n").into_bytes()
    }

    pub fn subject(&self) -> String {
        subject::renew(&self.name)
    }

    /// Check that the request names credentials renewable over NATS and arrived on its own
    /// [`RenewRequest::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject::renewable_name(&self.name) && subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignRenewal { name: self.name.clone(), at: subject.to_owned() })
    }
}

/// The answer to a [`RenewRequest`], on [`subject::renew_reply`]: the user JWT of `public_key`, which completes the
/// renewed credentials with the seed kept by the asking holder. A request the approver refuses gets no answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewReply {
    pub name: String,
    pub public_key: String,
    pub jwt: String,
}

impl RenewReply {
    pub fn subject(&self) -> String {
        subject::renew_reply(&self.name)
    }
}

/// A manager ant asking on [`subject::antreq`] for an ant added from a template (its `request_ant` tool): the
/// ant's name, the template, the settings asked for within the template's bounds (`request`, the JSON of a template
/// request, as the manager's config file would list them), and the public key of the user NKey the manager's machine
/// generated for the ant, whose seed stays there. The signer on the machine holding the account key (`ants signer`)
/// signs a user JWT for that key when the request keeps within its own copy of the template's policy, asks the humans
/// otherwise, and answers with an [`AntReply`].
///
/// With `retire` set, the manager asks instead to retire ant `name`, one it added (its `retire_ant` tool): the signer
/// revokes the ant's live keys if the team issued them all for the manager to add, and answers
/// [`AntReply::Retired`]; `public_key` is then that of the ant's credentials on the manager's machine, `template` the
/// ant's, `request` and `reason` unused.
///
/// The request carries the manager's user JWT (`credentials`) and its key's `signature` of [`AntRequest::signed`], so
/// the signer takes only requests the manager made, whatever connection carried them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntRequest {
    pub manager: UserId,
    /// The ant asked for.
    pub name: UserId,
    pub template: String,
    /// The settings asked for, a JSON object of the keys a template request may set.
    pub request: String,
    /// Why the manager wants the ant, shown to a human asked to approve.
    pub reason: String,
    /// The manager's thread the request comes from, where a human's approval request appears.
    pub thread_id: ThreadId,
    /// The ant's public NKey, `U...`.
    pub public_key: String,
    /// Host name of the asking machine, as it reports it.
    pub host: String,
    pub sent_at: DateTime<Utc>,
    /// When the manager stops waiting: no answer counts after it, and no human is asked past it.
    pub expires_at: DateTime<Utc>,
    /// The user JWT of the manager's credentials.
    pub credentials: String,
    /// Signature of [`AntRequest::signed`] by the manager's key, base64url without padding.
    pub signature: String,
    /// The leaf node the asking machine reaches the hub through, whose uplink an approval extends to the ant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leaf: Option<EnrollLeaf>,
    /// Retire ant `name` instead of adding it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retire: bool,
}

impl AntRequest {
    /// A request sent now, waiting until `expires_at`, to be completed with the manager's JWT and signatures.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        manager: UserId,
        name: UserId,
        template: impl Into<String>,
        request: impl Into<String>,
        reason: impl Into<String>,
        thread_id: ThreadId,
        public_key: impl Into<String>,
        host: impl Into<String>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            manager,
            name,
            template: template.into(),
            request: request.into(),
            reason: reason.into(),
            thread_id,
            public_key: public_key.into(),
            host: host.into(),
            sent_at: Utc::now(),
            expires_at,
            credentials: String::new(),
            signature: String::new(),
            leaf: None,
            retire: false,
        }
    }

    /// What the manager's key and the leaf's uplink key sign: every field but the signatures, one per line, the
    /// free texts prefixed with their length in bytes, so that none can pass for the next; a retirement under
    /// another first line than an addition, so that neither passes for the other.
    pub fn signed(&self) -> Vec<u8> {
        let at = |t: &DateTime<Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let quoted = |t: &str| format!("{}:{t}", t.len());
        let (machine, leaf) = self.leaf.as_ref().map_or(("", ""), |l| (l.machine.as_str(), l.jwt.as_str()));
        let fields = [
            if self.retire { "ants-antretire" } else { "ants-antreq" },
            self.manager.as_str(),
            self.name.as_str(),
            &quoted(&self.template),
            &quoted(&self.request),
            &quoted(&self.reason),
            self.thread_id.as_str(),
            &self.public_key,
            &quoted(&self.host),
            &at(&self.sent_at),
            &at(&self.expires_at),
            &self.credentials,
            machine,
            leaf,
        ];
        fields.join("\n").into_bytes()
    }

    pub fn subject(&self) -> String {
        subject::antreq(&self.manager)
    }

    /// Check that the request arrived on its manager's own [`AntRequest::subject`], which only the manager's
    /// credentials publish on.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "ant request", user: self.manager.clone(), at: subject.to_owned() })
    }
}

/// The answer to an [`AntRequest`], on [`subject::antreq_reply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AntReply {
    /// The user JWT of `public_key` for ant `name`, which completes its credentials with the seed kept on the
    /// manager's machine, and for a request through a leaf node ([`AntRequest::leaf`]) the renewed user JWT of the
    /// leaf's uplink key, allowing the ant as well.
    Approved {
        manager: UserId,
        name: UserId,
        public_key: String,
        jwt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        leaf_jwt: Option<String>,
    },
    /// No credentials, or for a retirement nothing revoked, and why.
    Refused { manager: UserId, name: UserId, public_key: String, reason: String },
    /// Ant `name` retired ([`AntRequest::retire`]): the keys of it revoked now (none when an earlier request revoked
    /// them all), and the error of pushing the revocations to the hub, if it failed; the hub then refuses the keys
    /// only once it reloads its `server.conf`.
    Retired {
        manager: UserId,
        name: UserId,
        public_key: String,
        revoked: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        push_error: Option<String>,
    },
}

impl AntReply {
    pub fn manager(&self) -> &UserId {
        match self {
            Self::Approved { manager, .. } | Self::Refused { manager, .. } | Self::Retired { manager, .. } => manager,
        }
    }

    pub fn subject(&self) -> String {
        subject::antreq_reply(self.manager())
    }

    /// Whether the answer is meant for the request of ant `name` with `public_key`.
    pub fn answers(&self, name: &UserId, public_key: &str) -> bool {
        match self {
            Self::Approved { name: n, public_key: key, .. }
            | Self::Refused { name: n, public_key: key, .. }
            | Self::Retired { name: n, public_key: key, .. } => n == name && key == public_key,
        }
    }
}

/// Where an ant stands on a thread.
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
    /// The harness is signed out: the thread's messages wait for it to be signed in.
    SignedOut,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Working => "working",
            State::WaitingApproval => "waiting_approval",
            State::Idle => "idle",
            State::Failed => "failed",
            State::SignedOut => "signed_out",
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

/// What changed on a thread while an ant works on it.
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
    /// Message `message` waits on the thread behind `ahead` others: the one in flight and those that came before it.
    /// Sent as it arrives and as the queue moves; `ahead` 0 means its turn came.
    Queued {
        message: Uuid,
        ahead: usize,
    },
    /// The tool request `<ant>/<request_id>` waits for a decision, as `ants tools` names it; sent just before the
    /// [`State::WaitingApproval`] state.
    Approval {
        request_id: Uuid,
    },
    /// The ant escalated what it could not settle: the message carrying `escalation` to `to`, an ant of a chain or the
    /// thread's owner (else the default human), is stored. Sent for each such message, reminders included; the
    /// message's text is not part of it.
    Escalated {
        to: UserId,
        escalation: Escalation,
    },
}

/// Live progress of an ant on a thread, published on [`subject::status`] with core NATS: never stored, so a
/// subscriber sees only what happens while it listens, and an update may be dropped. Readers should treat unknown
/// update types as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEvent {
    pub ant: UserId,
    pub thread_id: ThreadId,
    #[serde(flatten)]
    pub update: StatusUpdate,
    pub at: DateTime<Utc>,
}

impl StatusEvent {
    pub fn new(ant: UserId, thread_id: ThreadId, update: StatusUpdate) -> Self {
        Self { ant, thread_id, update, at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::status(&self.ant, &self.thread_id)
    }

    /// Check that the update arrived on its own [`StatusEvent::subject`]. Credentials pin each ant to its own
    /// status subjects, so an update naming another ant or thread is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser {
            kind: "status update",
            user: self.ant.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
    }
}

/// A human asking the daemons of `ant` to cancel the prompt running on `thread_id`, on [`subject::cancel`] with
/// core NATS: never stored, so only daemons connected at the time hear it. Each one answers with a [`CancelReply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub id: Uuid,
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// The human asking, as the request names it: only human credentials publish cancel requests.
    pub by: UserId,
    pub sent_at: DateTime<Utc>,
}

impl CancelRequest {
    pub fn new(ant: UserId, thread_id: ThreadId, by: UserId) -> Self {
        Self { id: Uuid::now_v7(), ant, thread_id, by, sent_at: Utc::now() }
    }

    pub fn subject(&self) -> String {
        subject::cancel(&self.ant, &self.thread_id)
    }

    pub fn reply_subject(&self) -> String {
        subject::cancelled(&self.ant, &self.thread_id)
    }

    /// Check that the request arrived on its own [`CancelRequest::subject`], so that a request naming another ant or
    /// thread than its subject is not acted on.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser {
            kind: "cancel request",
            user: self.ant.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
    }

    /// A daemon's answer: whether it had a prompt running on the thread, which it then cancelled.
    pub fn answer(&self, cancelled: bool) -> CancelReply {
        CancelReply { request_id: self.id, ant: self.ant.clone(), thread_id: self.thread_id.clone(), cancelled }
    }
}

/// The answer of one daemon of `ant` to a [`CancelRequest`], on [`subject::cancelled`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelReply {
    pub request_id: Uuid,
    pub ant: UserId,
    pub thread_id: ThreadId,
    /// Whether a prompt was running on the thread and is now cancelled. Its partial reply follows on the thread.
    pub cancelled: bool,
}

impl CancelReply {
    pub fn subject(&self) -> String {
        subject::cancelled(&self.ant, &self.thread_id)
    }

    /// Check that the answer arrived on its own [`CancelReply::subject`], which credentials pin to the ant's own.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser {
            kind: "cancel answer",
            user: self.ant.clone(),
            at: format!("{subject} (thread {})", self.thread_id),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[test]
    fn ids_reject_subject_metacharacters() {
        for bad in ["", "a.b", "*", ">", "dev a", "Dev", "-dev", &"x".repeat(65)] {
            assert!(bad.parse::<UserId>().is_err(), "{bad:?} should be rejected");
        }
        assert_eq!(p("dev-b2").as_str(), "dev-b2");
        assert!(serde_json::from_str::<UserId>(r#""a.b""#).is_err());
    }

    #[test]
    fn generated_thread_ids_are_valid_tokens() {
        let t = ThreadId::generate();
        assert_eq!(t.as_str().parse::<ThreadId>().unwrap(), t);
    }

    #[test]
    fn enrollment_answers_name_the_key_they_answer() {
        let request = EnrollRequest::new(p("dev-a"), "UKEY", "macbook");
        assert_eq!(request.check_subject("ants.enroll.dev-a"), Ok(()));
        assert!(request.check_subject("ants.enroll.dev-b").is_err());

        let approved =
            EnrollReply::Approved { ant: p("dev-a"), public_key: "UKEY".into(), jwt: "j".into(), leaf_jwt: None };
        let json = serde_json::to_value(&approved).unwrap();
        assert_eq!(json["outcome"], "approved");
        assert!(json.get("leaf_jwt").is_none());
        assert_eq!(serde_json::from_value::<EnrollReply>(json).unwrap(), approved);
        assert_eq!(approved.subject(), "ants.enroll.dev-a.reply");
        assert!(approved.answers("UKEY") && !approved.answers("UOTHER"));

        let all: EnrollReply =
            serde_json::from_str(r#"{"outcome":"rejected","ant":"dev-a","public_key":null}"#).unwrap();
        assert!(all.answers("UKEY") && all.answers("UOTHER"));
        let one = EnrollReply::Rejected { ant: p("dev-a"), public_key: Some("UOTHER".into()), reason: None };
        assert!(!one.answers("UKEY"));
    }

    #[test]
    fn an_enrollment_request_signs_all_but_its_signatures() {
        let mut request = EnrollRequest::new(p("dev-a"), "UKEY", "macbook");
        request.enrollment = "e.j.s".into();
        let bare = request.signed();
        let json = serde_json::to_string(&request).unwrap();
        assert!(!json.contains("\"leaf\""), "{json}");
        let back: EnrollRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.signed(), bare, "the same bytes after the wire");
        request.signature = "sig".into();
        assert_eq!(request.signed(), bare);
        let leaf = EnrollLeaf { machine: "m1".parse().unwrap(), jwt: "l.j.s".into(), signature: String::new() };
        request.leaf = Some(leaf.clone());
        let with_leaf = request.signed();
        assert_ne!(with_leaf, bare, "the leaf is signed");
        request.leaf = Some(EnrollLeaf { signature: "other".into(), ..leaf });
        assert_eq!(request.signed(), with_leaf);
        for change in [
            |r: &mut EnrollRequest| r.public_key = "UOTHER".into(),
            |r: &mut EnrollRequest| r.ant = "dev-b".parse().unwrap(),
            |r: &mut EnrollRequest| r.host = "other".into(),
            |r: &mut EnrollRequest| r.enrollment = "x.y.z".into(),
            |r: &mut EnrollRequest| r.sent_at += chrono::TimeDelta::nanoseconds(1),
            |r: &mut EnrollRequest| r.leaf.as_mut().unwrap().machine = "m2".parse().unwrap(),
        ] {
            let mut changed = request.clone();
            change(&mut changed);
            assert_ne!(changed.signed(), with_leaf);
        }
    }

    #[test]
    fn a_renewal_request_signs_all_but_its_signature_and_names_renewable_credentials() {
        let mut request = RenewRequest::new("dev-a", "UKEY", "macbook");
        request.current = "c.j.s".into();
        let bare = request.signed();
        let back: RenewRequest = serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(back.signed(), bare, "the same bytes after the wire");
        request.signature = "sig".into();
        assert_eq!(request.signed(), bare);
        for change in [
            |r: &mut RenewRequest| r.public_key = "UOTHER".into(),
            |r: &mut RenewRequest| r.name = "dev-b".into(),
            |r: &mut RenewRequest| r.host = "other".into(),
            |r: &mut RenewRequest| r.current = "x.y.z".into(),
            |r: &mut RenewRequest| r.sent_at += chrono::TimeDelta::nanoseconds(1),
        ] {
            let mut changed = request.clone();
            change(&mut changed);
            assert_ne!(changed.signed(), bare);
        }
        assert_eq!(request.check_subject("ants.renew.dev-a"), Ok(()));
        assert!(request.check_subject("ants.renew.dev-b").is_err());
        let leaf = RenewRequest::new("leaf-Mac_1", "UKEY", "macbook");
        assert_eq!(leaf.check_subject("ants.renew.leaf-Mac_1"), Ok(()));
        for name in ["a.b", "*", ">", "", "leaf-a.b", "Upper"] {
            let odd = RenewRequest::new(name, "UKEY", "macbook");
            assert!(odd.check_subject(&odd.subject()).is_err(), "{name:?}");
        }
        let reply = RenewReply { name: "leaf-m1".into(), public_key: "UKEY".into(), jwt: "j".into() };
        assert_eq!(reply.subject(), "ants.renew.leaf-m1.reply");
    }

    #[test]
    fn credentials_ask_only_for_their_own_renewal_and_leaf_nodes_carry_any() {
        let (dev_a, alice) = (p("dev-a"), p("alice"));
        let ant = Permissions::for_user(&dev_a, Role::Ant);
        let renewals = |list: &[String]| -> Vec<String> {
            list.iter().filter(|s| s.starts_with("ants.renew.") || *s == subject::ALL).cloned().collect()
        };
        assert_eq!(renewals(&ant.publish), ["ants.renew.dev-a"]);
        assert_eq!(renewals(&ant.subscribe), ["ants.renew.dev-a.reply"]);
        // A human carries the renewal of its machine's leaf node uplink, and hears every answer anyway.
        let human = Permissions::for_user(&alice, Role::Human);
        assert_eq!(renewals(&human.publish), [subject::ALL_RENEW_REQUESTS]);
        assert_eq!(renewals(&human.subscribe), [subject::ALL]);
        let manager = Permissions::for_channel_manager();
        assert_eq!(
            (renewals(&manager.publish), renewals(&manager.subscribe)),
            (vec!["ants.renew.channel-manager".to_owned()], vec!["ants.renew.channel-manager.reply".to_owned()])
        );
        let enrollment = Permissions::for_enrollment(&dev_a);
        assert!(renewals(&enrollment.publish).is_empty() && renewals(&enrollment.subscribe).is_empty());
        let m1: Machine = "m1".parse().unwrap();
        let leaf = Permissions::for_leaf(&m1, [(&dev_a, Role::Ant), (&alice, Role::Human)]);
        assert_eq!(leaf.publish.iter().filter(|s| *s == subject::ALL_RENEW_REQUESTS).count(), 1);
        assert!(leaf.subscribe.iter().any(|s| s == subject::ALL_RENEW_REPLIES));
        let approver = Permissions::for_enroll_approver();
        assert!(approver.subscribe.iter().any(|s| s == subject::ALL_RENEW_REQUESTS));
        assert!(approver.publish.iter().any(|s| s == subject::ALL_RENEW_REPLIES));
        assert!(!approver.publish.iter().any(|s| s == subject::ALL_RENEW_REQUESTS));
    }

    #[test]
    fn ants_ask_only_for_their_own_ant_requests_and_the_approver_answers_them() {
        let (lead, other) = (p("lead"), p("dev-b"));
        let ant = Permissions::for_user(&lead, Role::Ant);
        let asks = |list: &[String]| -> Vec<String> {
            list.iter().filter(|s| s.starts_with("ants.antreq.")).cloned().collect()
        };
        assert_eq!(asks(&ant.publish), ["ants.antreq.lead"]);
        assert_eq!(asks(&ant.subscribe), ["ants.antreq.lead.reply"]);
        let m1: Machine = "m1".parse().unwrap();
        let leaf = Permissions::for_leaf(&m1, [(&lead, Role::Ant), (&other, Role::Ant)]);
        assert_eq!(asks(&leaf.publish), ["ants.antreq.lead", "ants.antreq.dev-b"]);
        let approver = Permissions::for_enroll_approver();
        assert_eq!(asks(&approver.subscribe), [subject::ALL_ANT_REQUESTS]);
        assert_eq!(asks(&approver.publish), [subject::ALL_ANT_REPLIES]);
        assert!(approver.publish.iter().any(|s| s == subject::ALL_APPROVAL_REQUESTS));
        assert!(
            !approver.publish.iter().any(|s| s == subject::ALL_APPROVAL_DECISIONS),
            "only humans and the escalation chain decide"
        );
        let request = AntRequest::new(
            lead.clone(),
            p("lead-w1"),
            "worker",
            "{}",
            "more hands",
            "t1".parse().unwrap(),
            "UKEY",
            "host",
            Utc::now(),
        );
        assert_eq!(request.subject(), "ants.antreq.lead");
        assert!(request.check_subject("ants.antreq.dev-b").is_err());
        let mut moved = request.clone();
        moved.reason = "more hands\n".into();
        assert_ne!(moved.signed(), request.signed());
        let retiring = AntRequest { retire: true, ..request.clone() };
        assert_ne!(retiring.signed(), request.signed(), "an addition never passes for a retirement");
        let reply =
            AntReply::Refused { manager: lead, name: p("lead-w1"), public_key: "UKEY".into(), reason: "no".into() };
        assert_eq!(reply.subject(), "ants.antreq.lead.reply");
        assert!(reply.answers(&p("lead-w1"), "UKEY") && !reply.answers(&p("lead-w2"), "UKEY"));
    }

    #[test]
    fn a_js_domain_adds_the_domain_form_of_the_jetstream_api() {
        let domain: JsDomain = "hub".parse().unwrap();
        let plain = Permissions::for_user(&p("dev-a"), Role::Ant);
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
            "$JS.hub.API.CONSUMER.MSG.NEXT.ANTS.notes-dev-a",
            "$JS.ACK.ANTS.inbox-dev-a.>",
            // The v2 form of nats-server 2.16, whatever the domain.
            "$JS.ACK.*.*.ANTS.inbox-dev-a.>",
            "ants.notes.*.dev-a",
        ] {
            assert!(publish.iter().any(|have| have == s), "{s} missing from {publish:?}");
        }
        assert!("hub.x".parse::<JsDomain>().is_err());
    }

    #[test]
    fn leaf_permissions_cover_each_user_once() {
        let (human, a, b) = (UserId::human(), p("dev-a"), p("dev-b"));
        let m1: Machine = "m1".parse().unwrap();
        let all = [(&a, Role::Ant), (&b, Role::Ant), (&human, Role::Human)];
        let leaf = Permissions::for_leaf(&m1, all);
        for (user, role) in all {
            let own = Permissions::for_user(user, role);
            let crossing = own.publish.iter().filter(|s| !s.starts_with("ants.out."));
            assert!(crossing.into_iter().all(|s| leaf.publish.contains(s)), "{user} publish");
            assert!(own.subscribe.iter().all(|s| leaf.subscribe.contains(s)), "{user} subscribe");
        }
        assert_eq!(leaf.publish.iter().filter(|s| *s == "$JS.API.STREAM.INFO.ANTS").count(), 1);
        assert!(leaf.publish.iter().all(|s| !s.starts_with("ants.out.")), "the outbox stays on the leaf");
        // The hub may source the outbox of m1, and the leaf deliver it on m1's subjects only.
        let only_a = Permissions::for_leaf(&m1, [(&a, Role::Ant)]);
        let own = Permissions::for_user(&a, Role::Ant);
        let extra: Vec<_> = only_a.publish.iter().filter(|s| !own.publish.contains(s)).collect();
        assert_eq!(
            extra,
            [
                // Already allowed by `ants.inbox.*.dev-a`, `ants.notes.*.dev-a`, `ants.chan.*.dev-a` and
                // `ants.chanadm.*.dev-a`.
                "ants.inbox.dev-a.dev-a",
                "ants.notes.dev-a.dev-a",
                "ants.chan.dev-a.dev-a",
                "ants.chanadm.dev-a.dev-a",
                "$JS.S.m1.>",
                "$JSC.R.m1.*.ANTS_OUT.>",
                "_INBOX.human.leaf.m1.>",
                // Asking for a new ant's credentials, and for any renewal on the machine.
                "ants.enroll.*",
                "ants.renew.*",
            ]
        );
        let mut extra: Vec<_> = only_a.subscribe.iter().filter(|s| !own.subscribe.contains(s)).collect();
        assert_eq!(extra.pop().map(String::as_str), Some("ants.renew.*.reply"), "hearing the renewals");
        assert_eq!(extra.pop().map(String::as_str), Some("ants.enroll.*.reply"), "hearing the answer");
        let fc = ["$JS.FC.ANTS_OUT.>", "$JS.FC.m1.*.ANTS_OUT.>"];
        assert!(fc.iter().all(|s| extra.contains(&&s.to_string())), "both forms in m1's domain: {extra:?}");
        assert!(extra.iter().all(|s| s.starts_with("$JS.m1.API.") || fc.contains(&s.as_str())), "{extra:?}");
        assert!(extra.iter().all(|s| s.contains(".ANTS_OUT")), "{extra:?}");
        // Never hearing the requests of other machines, nor answering them, without a human (who watches all).
        assert!(!only_a.subscribe.iter().any(|s| s == subject::ALL_ENROLL_REQUESTS || s == ">" || s == "ants.>"));
        assert!(!leaf.publish.iter().any(|s| s == subject::ALL_ENROLL_REPLIES || s == ">" || s == "ants.>"));
    }

    #[test]
    fn machine_names_are_one_token() {
        for bad in ["", "a.b", "m*", ">", "dev a", "-m", "_m", &"x".repeat(65)] {
            assert!(bad.parse::<Machine>().is_err(), "{bad:?} should be rejected");
        }
        assert_eq!("Mac_mini-2".parse::<Machine>().unwrap().as_str(), "Mac_mini-2");
    }

    #[test]
    fn outboxes_take_inbox_notes_channel_and_dead_letter_subjects_only() {
        let t: ThreadId = "t1".parse().unwrap();
        let c: ChannelId = "ops".parse().unwrap();
        assert_eq!(subject::outbox(&subject::inbox(&p("dev-a"), &p("dev-b"))).unwrap(), "ants.out.inbox.dev-a.dev-b");
        assert_eq!(subject::outbox(&subject::notes(&t, &p("dev-a"))).unwrap(), "ants.out.notes.t1.dev-a");
        assert_eq!(subject::outbox(&subject::chan(&c, &p("dev-a"))).unwrap(), "ants.out.chan.ops.dev-a");
        assert_eq!(subject::outbox(&subject::dead(&p("dev-a"))).unwrap(), "ants.out.dead.dev-a");
        for other in [subject::approvals(&p("dev-a")), subject::roster(&p("dev-a")), "ants.outbox.x".to_owned()] {
            assert_eq!(subject::outbox(&other), None, "{other}");
        }
        let a = Permissions::for_user(&p("dev-a"), Role::Ant);
        for s in ["ants.out.inbox.*.dev-a", "ants.out.notes.*.dev-a", "ants.out.chan.*.dev-a", "ants.out.dead.dev-a"] {
            assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
        }
        assert!(!a.publish.iter().any(|s| s.starts_with("ants.out.approvals")));
        let human = Permissions::for_user(&UserId::human(), Role::Human);
        assert!(human.publish.iter().any(|s| s == "ants.out.inbox.>"));
        assert!(human.publish.iter().any(|s| s == "ants.out.chan.*.human"));
        assert!(!human.publish.iter().any(|s| s.starts_with("ants.out.dead")), "a human's dead letters need the hub");
    }

    #[test]
    fn the_hub_admits_from_a_machine_what_its_users_may_send() {
        let m1: Machine = "m1".parse().unwrap();
        assert_eq!(
            m1.admitted(&p("dev-a"), Role::Ant, true),
            [
                ("ants.in.m1.ants.out.inbox.*.dev-a".to_owned(), "ants.inbox.{{wildcard(1)}}.dev-a".to_owned()),
                ("ants.in.m1.ants.out.notes.*.dev-a".to_owned(), "ants.notes.{{wildcard(1)}}.dev-a".to_owned()),
                ("ants.in.m1.ants.out.chan.*.dev-a".to_owned(), "ants.chan.{{wildcard(1)}}.dev-a".to_owned()),
                ("ants.in.m1.ants.out.dead.dev-a".to_owned(), "ants.dead.dev-a".to_owned()),
            ]
        );
        let human = m1.admitted(&UserId::human(), Role::Human, true);
        assert_eq!(human[0], ("ants.in.m1.ants.out.inbox.>".to_owned(), "ants.inbox.>".to_owned()));
        assert!(human.iter().all(|(filter, _)| !filter.contains(".dead.")), "no dead letters of a human");
        let rest: Vec<_> = m1.admitted(&p("dev-a"), Role::Ant, false).into_iter().map(|(filter, _)| filter).collect();
        assert_eq!(
            rest,
            ["ants.in.m1.ants.out.notes.*.dev-a", "ants.in.m1.ants.out.chan.*.dev-a", "ants.in.m1.ants.out.dead.dev-a"],
            "no inbox subjects"
        );
        assert_eq!(
            m1.admitted_owners(),
            ("ants.in.m1.ants.out.owners.*.*".to_owned(), "$KV.ants_owners.{{wildcard(1)}}.{{wildcard(2)}}".to_owned())
        );
        assert_eq!(subject::intake(&m1), "ants.in.m1.>");
    }

    #[test]
    fn subjects_are_scoped_under_ants() {
        let t: ThreadId = "t1".parse().unwrap();
        assert_eq!(subject::inbox(&p("dev-a"), &UserId::human()), "ants.inbox.dev-a.human");
        assert_eq!(subject::inbox_of(&p("dev-a")), "ants.inbox.dev-a.*");
        assert_eq!(subject::inbox_from(&p("dev-a")), "ants.inbox.*.dev-a");
        assert_eq!(subject::notes(&t, &p("dev-a")), "ants.notes.t1.dev-a");
        assert_eq!(subject::notes_of(&t), "ants.notes.t1.*");
        assert_eq!(subject::notes_from(&p("dev-a")), "ants.notes.*.dev-a");
        assert!(STREAM_SUBJECTS.contains(&subject::ALL_NOTES), "thread notes are durable");
        assert_eq!(subject::approvals(&p("dev-a")), "ants.approvals.dev-a");
        assert_eq!(subject::status(&p("dev-a"), &t), "ants.status.dev-a.t1");
        assert_eq!(subject::status_on(&t), "ants.status.*.t1");
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
        let queued = StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Queued { message: Uuid::nil(), ahead: 2 });
        let json = serde_json::to_value(&queued).unwrap();
        assert_eq!((json["type"].as_str(), json["ahead"].as_u64()), (Some("queued"), Some(2)));
        let approval = StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Approval { request_id: Uuid::nil() });
        let escalation = Escalation::new(p("dev-a"), t.clone(), EscalationReason::Asked);
        let escalated =
            StatusEvent::new(p("dev-a"), t.clone(), StatusUpdate::Escalated { to: UserId::human(), escalation });
        let json = serde_json::to_value(&escalated).unwrap();
        assert_eq!((json["type"].as_str(), json["to"].as_str()), (Some("escalated"), Some("human")));
        assert_eq!(
            (json["escalation"]["stuck"].as_str(), json["escalation"]["reason"].as_str()),
            (Some("dev-a"), Some("asked"))
        );
        for event in [state, tool, queued, approval, escalated] {
            let json = serde_json::to_string(&event).unwrap();
            assert_eq!(serde_json::from_str::<StatusEvent>(&json).unwrap(), event);
        }

        let chunk = StatusEvent::new(p("dev-a"), t, StatusUpdate::Message { text: "hi".into() });
        assert_eq!(chunk.subject(), "ants.status.dev-a.t1");
        assert_eq!(chunk.check_subject("ants.status.dev-a.t1"), Ok(()));
        assert!(chunk.check_subject("ants.status.dev-a.t2").is_err(), "another thread");
        let forged = chunk.check_subject("ants.status.dev-b.t1").unwrap_err();
        assert_eq!(forged.to_string(), "status update of dev-a found at ants.status.dev-b.t1 (thread t1)");
        assert!(Permissions::for_user(&p("dev-a"), Role::Ant).publish.contains(&subject::status_of(&p("dev-a"))));
    }

    #[test]
    fn humans_ask_and_only_the_ant_hears_and_answers_cancel_requests() {
        let t: ThreadId = "t1".parse().unwrap();
        let request = CancelRequest::new(p("dev-a"), t, UserId::human());
        assert_eq!(
            (request.subject(), request.reply_subject()),
            ("ants.cancel.dev-a.t1".into(), "ants.cancelled.dev-a.t1".into())
        );
        assert_eq!(request.check_subject("ants.cancel.dev-a.t1"), Ok(()));
        assert!(request.check_subject("ants.cancel.dev-a.t2").is_err(), "another thread");
        let forged = request.check_subject("ants.cancel.dev-b.t1").unwrap_err();
        assert_eq!(forged.to_string(), "cancel request of dev-a found at ants.cancel.dev-b.t1 (thread t1)");
        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(serde_json::from_str::<CancelRequest>(&json).unwrap(), request);
        let reply = request.answer(true);
        assert_eq!((reply.request_id, reply.cancelled), (request.id, true));
        assert_eq!(reply.subject(), request.reply_subject());
        assert_eq!(reply.check_subject("ants.cancelled.dev-a.t1"), Ok(()));
        assert!(reply.check_subject("ants.cancelled.dev-b.t1").is_err());
        let json = serde_json::to_string(&reply).unwrap();
        assert_eq!(serde_json::from_str::<CancelReply>(&json).unwrap(), reply);

        let human = Permissions::for_user(&UserId::human(), Role::Human);
        assert!(human.publish.iter().any(|s| s == subject::ALL_CANCELS));
        assert!(human.subscribe.iter().any(|s| s == subject::ALL), "the answers");
        let a = Permissions::for_user(&p("dev-a"), Role::Ant);
        assert_eq!(
            a.subscribe.iter().filter(|s| s.starts_with("ants.cancel")).collect::<Vec<_>>(),
            ["ants.cancel.dev-a.*"]
        );
        assert_eq!(
            a.publish.iter().filter(|s| s.starts_with("ants.cancel")).collect::<Vec<_>>(),
            ["ants.cancelled.dev-a.*"]
        );
        assert_eq!(subject::outbox(&request.subject()), None, "never stored, so never buffered");
    }

    #[test]
    fn reply_swaps_parties_and_links_the_message() {
        let ask = Envelope::new(UserId::human(), p("dev-a"), "hi");
        let reply = ask.reply(p("dev-a"), "hello", Role::Human).unwrap();
        assert_eq!((reply.from.as_str(), reply.to.as_str()), ("dev-a", "human"));
        assert_eq!(reply.thread_id, ask.thread_id);
        assert_eq!(reply.in_reply_to, Some(ask.id));
        assert_eq!(reply.turn_budget, ask.turn_budget, "replying to the human spends no turn");
        assert_eq!(reply.subject(), "ants.inbox.human.dev-a");
    }

    #[test]
    fn a_spun_off_thread_links_to_the_channel_thread_its_creator_comes_from() {
        let (channel_thread, a, b): (ThreadId, ThreadId, ThreadId) =
            ("t0".parse().unwrap(), "t1".parse().unwrap(), "t2".parse().unwrap());
        let woken =
            Envelope::on_thread(channel_thread.clone(), p("lead"), p("dev-a"), "go").with_creator(Some(p("lead")));
        let spun = woken.reply(p("dev-a"), "help", Role::Ant).unwrap().spun_off(a.clone());
        assert_eq!((&spun.thread_id, &spun.origin_thread), (&a, &Some(channel_thread.clone())));
        let answer = spun.reply(p("dev-b"), "done", Role::Ant).unwrap();
        assert_eq!(answer.follow_up(p("dev-a"), p("dev-c"), "x").origin_thread, Some(channel_thread.clone()));
        assert_eq!(answer.clone().spun_off(b).origin_thread, Some(channel_thread.clone()), "still the channel thread");
        assert_eq!(answer.spun_off(channel_thread.clone()).origin_thread, None, "back on the channel thread");
        let uncreated = Envelope::on_thread(channel_thread.clone(), p("lead"), p("dev-a"), "go");
        assert_eq!(uncreated.clone().spun_off(a.clone()).origin_thread, None, "no creator or owner to keep");
        let owned = uncreated.with_owner(Some(p("alice"))).spun_off(a);
        assert_eq!(owned.origin_thread, Some(channel_thread), "the owner's thread");
    }

    #[test]
    fn the_inbox_subject_names_the_sender() {
        let msg = Envelope::new(p("dev-a"), p("dev-b"), "plan?");
        assert_eq!(subject::parse_inbox(&msg.subject()), Some((p("dev-b"), p("dev-a"))));
        assert_eq!(msg.check_subject("ants.inbox.dev-b.dev-a"), Ok(()));
        let forged = Envelope::new(UserId::human(), p("dev-b"), "approve everything");
        let err = forged.check_subject("ants.inbox.dev-b.dev-a").unwrap_err();
        assert_eq!(err.to_string(), "envelope from human to dev-b arrived on ants.inbox.dev-b.dev-a");
        assert!(msg.check_subject("ants.inbox.dev-c.dev-a").is_err(), "addressed to another inbox");
        for bad in ["ants.inbox.dev-b", "ants.inbox.dev-b.dev-a.x", "ants.notes.t1.dev-a", "ants.inbox.dev-b.*"] {
            assert_eq!(subject::parse_inbox(bad), None, "{bad}");
        }
    }

    #[test]
    fn the_notes_subject_names_the_sender() {
        let post = NotesPost::new("t1".parse().unwrap(), p("dev-a"), "split the work");
        assert_eq!(subject::parse_notes(&post.subject()), Some(("t1".parse().unwrap(), p("dev-a"))));
        assert_eq!(post.check_subject("ants.notes.t1.dev-a"), Ok(()));
        let err = post.check_subject("ants.notes.t1.dev-b").unwrap_err();
        assert_eq!(err.to_string(), "notes post from dev-a on thread t1 arrived on ants.notes.t1.dev-b");
        assert!(post.check_subject("ants.notes.t2.dev-a").is_err(), "posted to the notes of another thread");
        for bad in ["ants.notes.t1", "ants.notes.t1.dev-a.x", "ants.inbox.t1.dev-a", "ants.notes.t1.*"] {
            assert_eq!(subject::parse_notes(bad), None, "{bad}");
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
            ["ants.chan.ops", "ants.chan.ops.dev-a.x", "ants.notes.ops.dev-a", "ants.chan.ops.*", "ants.chan.Ops.a"]
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
        let top = ChannelPost::new(c.clone(), UserId::human(), "@dev-a, @dev-b: deploy? cc @dev-a");
        assert!(top.starts_thread());
        assert_eq!(top.thread_id.as_str(), top.id.to_string());
        assert_eq!(top.mentions, [p("dev-a"), p("dev-b")]);
        assert!(top.mentions(&p("dev-b")) && !top.mentions(&p("dev-c")));
        assert_eq!((top.turn_budget, top.in_reply_to), (DEFAULT_TURN_BUDGET, None));
        assert_eq!(top.subject(), "ants.chan.ops.human");
        assert_eq!(subject::parse_chan(&top.subject()), Some((c.clone(), UserId::human())));
        assert_eq!(top.check_subject("ants.chan.ops.human"), Ok(()));
        let err = top.check_subject("ants.chan.ops.dev-a").unwrap_err();
        assert_eq!(err.to_string(), "channel post from human in channel ops arrived on ants.chan.ops.dev-a");
        assert!(top.check_subject("ants.chan.dev.human").is_err(), "posted to another channel");

        let two = ChannelPost::new(c.clone(), UserId::human(), "and this");
        assert_ne!(two.thread_id, top.thread_id, "each top-level post starts its own thread");
        assert!(two.mentions.is_empty());
        let on = ChannelPost::on_thread(c, top.thread_id.clone(), UserId::human(), "more");
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
        let ask = ChannelPost::new(c.clone(), UserId::human(), "@dev-a deploy?");
        let answer = ask.reply(p("dev-a"), "done, @human", &[], &humans).unwrap();
        assert_eq!((answer.channel.clone(), answer.thread_id.clone()), (c.clone(), ask.thread_id.clone()));
        assert_eq!(answer.in_reply_to, Some(ask.id));
        assert_eq!(answer.turn_budget, DEFAULT_TURN_BUDGET - 1, "waking nobody spends a turn too");
        let mut handoff = ask.reply(p("dev-a"), "@dev-b check it; @dev-a will wait", &[], &humans).unwrap();
        assert_eq!(handoff.turn_budget, DEFAULT_TURN_BUDGET - 1);
        handoff.turn_budget = 1;
        let back = handoff.reply(p("dev-b"), "@dev-a fine", &[], &humans).unwrap();
        assert_eq!(back.turn_budget, 0);
        let spent = Err(Error::BudgetExhausted(ask.thread_id));
        assert_eq!(back.reply(p("dev-a"), "@dev-b again", &[], &humans), spent);
        assert_eq!(back.reply(p("dev-a"), "@human over to you", &[], &humans), spent);
        let human = back.reply(UserId::human(), "@dev-a @dev-b go on", &[], &humans).unwrap();
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
        let mut ask = ChannelPost::new(c.clone(), UserId::human(), "@dev-a @dev-b @dev-c review");
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
    fn mentions_are_user_ids_after_an_at_sign_that_starts_a_word() {
        let ids = |text: &str| parse_mentions(text).into_iter().map(String::from).collect::<Vec<_>>();
        assert_eq!(ids("@dev-a"), ["dev-a"]);
        assert_eq!(
            ids("hey @dev-a, @dev-b. @ops2: (@qa) @rev! @x?; @y;"),
            ["dev-a", "dev-b", "ops2", "qa", "rev", "x", "y"]
        );
        assert_eq!(ids("@dev-b then @dev-a then @dev-b"), ["dev-b", "dev-a"], "first-seen order, no duplicates");
        assert_eq!(ids("\"@dev-a\" [@dev-b] <@dev-c>\n\t@dev-d's"), ["dev-a", "dev-b", "dev-c", "dev-d"]);
        assert!(ids("mail a@b or someone@example.com").is_empty(), "addresses mention nobody");
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

        let human = UserId::human();
        let untagged = ChannelPost::new(c.clone(), human.clone(), "anyone?");
        assert_eq!(entry.woken(&untagged, &humans), [UserId::messenger()], "the default messenger triages");
        entry.messenger = Some(p("dev-c"));
        assert_eq!(entry.woken(&untagged, &humans), [p("dev-c")], "the assigned one, member or not");
        assert!(
            entry.woken(&ChannelPost::new(c.clone(), p("dev-a"), "anyone?"), &humans).is_empty(),
            "agents never fan out"
        );
        let tagged = ChannelPost::new(c, p("dev-a"), "@dev-b @dev-c @dev-a @human look");
        assert_eq!(entry.woken(&tagged, &humans), [p("dev-b")], "members only, not the author or the human");
        assert!(entry.woken(&ChannelPost::new("dev".parse().unwrap(), human, "@dev-a"), &humans).is_empty());
    }

    #[test]
    fn a_mention_notifies_human_members_and_wakes_ant_members() {
        let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
        let mut humans = Humans::default();
        for h in [&alice, &bob] {
            humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
        }
        let mut entry = ChannelEntry::new(c.clone());
        for member in [p("dev-a"), p("dev-b"), alice.clone()] {
            entry.add_member(member);
        }
        let mixed =
            ChannelPost::new(c.clone(), p("dev-a"), "@alice @bob @dev-b @dev-c look").with_owner(Some(bob.clone()));
        assert_eq!(entry.woken(&mixed, &humans), [p("dev-b")]);
        assert_eq!(entry.notified(&mixed, &humans), std::slice::from_ref(&alice), "members only: bob is none");
        let human_only = ChannelPost::new(c.clone(), p("dev-a"), "@alice see");
        assert!(entry.woken(&human_only, &humans).is_empty(), "no agent, nor the messenger");
        let by_alice = ChannelPost::new(c.clone(), alice.clone(), "@alice @dev-b note");
        assert!(entry.notified(&by_alice, &humans).is_empty(), "not its author");
        assert_eq!(entry.woken(&by_alice, &humans), [p("dev-b")]);
        assert!(entry.notified(&ChannelPost::new("dev".parse().unwrap(), p("dev-a"), "@alice"), &humans).is_empty());

        let notice = mixed.mention_notice(&alice);
        assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&p("dev-a"), &alice, &mixed.thread_id));
        assert_eq!((notice.text.as_str(), notice.turn_budget), (mixed.text.as_str(), mixed.turn_budget));
        assert_eq!((&notice.owner, &notice.creator), (&None, &None), "a notice claims no thread");
        assert_eq!(notice.mention, Some(Mention { channel: c, post: mixed.id, author: None }));
        assert_eq!(notice.subject(), subject::inbox(&alice, &p("dev-a")));
        assert_eq!(mixed.mention_msg_id(&alice), format!("mention.{}.alice", mixed.id));
        assert_eq!(mention_key(&p("dev-a"), 7, &alice), "dev-a.mention.7.alice");

        let json = serde_json::to_value(&notice).unwrap();
        assert_eq!(json["mention"]["channel"], "ops");
        let mut old = json;
        old.as_object_mut().unwrap().remove("mention");
        let read: Envelope = serde_json::from_value(old).unwrap();
        assert_eq!(read, Envelope { mention: None, ..notice }, "an envelope from before mentions reads as a message");
        let plain = serde_json::to_string(&Envelope::new(p("dev-a"), alice, "hi")).unwrap();
        assert!(!plain.contains("mention"), "{plain}");
    }

    #[test]
    fn the_messenger_relays_the_mention_notices_of_a_webhook_s_post() {
        let (c, alice) = ("ops".parse::<ChannelId>().unwrap(), p("alice"));
        let mut humans = Humans::default();
        humans.add("alice", HumanEntry::new(alice.clone())).unwrap();
        let mut entry = ChannelEntry::new(c.clone());
        entry.add_member(alice.clone());
        let hook = ChannelPost::new(c.clone(), p("github"), "deploy failed").from_webhook(vec![alice.clone()]);
        assert_eq!(entry.notified(&hook, &humans), std::slice::from_ref(&alice));
        assert_eq!(entry.notifier(&hook), UserId::messenger());
        entry.messenger = Some(p("dev-a"));
        assert_eq!(entry.notifier(&hook), p("dev-a"));
        let own = ChannelPost::new(c.clone(), p("dev-b"), "@alice look");
        assert_eq!(entry.notifier(&own), p("dev-b"), "an ant's or a human's post: its author");

        let notice = hook.relayed_notice(&alice, &p("dev-a"));
        assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&p("dev-a"), &alice, &hook.thread_id));
        assert_eq!((notice.text.as_str(), notice.webhook), (hook.text.as_str(), false));
        assert_eq!(notice.mention, Some(Mention { channel: c, post: hook.id, author: Some(p("github")) }));
        assert_eq!(notice.subject(), subject::inbox(&alice, &p("dev-a")));
        let json = serde_json::to_value(&notice).unwrap();
        assert_eq!(json["mention"]["author"], "github");
        assert!(serde_json::to_value(hook.mention_notice(&alice)).unwrap()["mention"].get("author").is_none());
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
        assert_eq!(entry.woken(&top, &humans), [UserId::messenger()], "alice's post goes to the messenger");
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
        // Only a human owns a thread: an owner an ant names is ignored.
        let named = ChannelPost::new("ops".parse().unwrap(), p("dev-a"), "@dev-b go").with_owner(Some(p("dev-a")));
        assert_eq!(named.thread_owner(&humans), None);
        assert_eq!(named.reply(p("dev-b"), "b", &[], &humans).unwrap().owner, None);

        let json = serde_json::to_string(&old).unwrap();
        assert!(!json.contains("owner"), "{json}");
        assert!(serde_json::to_string(&top).unwrap().contains(r#""owner":"alice""#));
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), old, "posts without an owner still read");
    }

    #[test]
    fn a_webhook_s_post_that_mentions_nobody_is_triaged_and_its_mentions_come_from_the_ingress() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let mut entry = ChannelEntry::new(c.clone());
        entry.add_member(p("dev-a"));
        let hook = p("github");
        let untagged = ChannelPost::new(c.clone(), hook.clone(), "@dev-a push to main").from_webhook(Vec::new());
        assert!(untagged.mentions.is_empty(), "never parsed from the text");
        assert!(untagged.to_triage(&humans));
        assert_eq!(entry.woken(&untagged, &humans), [UserId::messenger()]);
        let tagged = ChannelPost::new(c, hook, "push").from_webhook(vec![p("dev-a"), p("dev-b")]);
        assert_eq!(entry.woken(&tagged, &humans), [p("dev-a")], "members only");

        let json = serde_json::to_string(&untagged).unwrap();
        assert!(json.contains(r#""webhook":true"#), "{json}");
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), untagged);
        let reply = untagged.reply(p("dev-a"), "on it", &[], &humans).unwrap();
        assert!(
            !reply.webhook && !serde_json::to_string(&reply).unwrap().contains("webhook"),
            "replies are no webhook's"
        );
    }

    #[test]
    fn a_webhook_under_a_human_s_name_is_no_human() {
        let (c, bob) = ("ops".parse::<ChannelId>().unwrap(), p("bob"));
        let mut humans = Humans::default();
        humans.add(bob.as_str(), HumanEntry::new(bob.clone())).unwrap();
        let hook = ChannelPost::new(c.clone(), bob.clone(), "red").from_webhook(Vec::new());
        assert!(!hook.by_human(&humans) && ChannelPost::new(c.clone(), bob.clone(), "hi").by_human(&humans));
        assert_eq!(hook.thread_owner(&humans), None, "it owns no thread");
        let mut spent = hook.clone();
        spent.turn_budget = 1;
        let again = ChannelPost::on_thread(c, hook.thread_id, bob.clone(), "red").from_webhook(Vec::new());
        assert_eq!(spent.turns_left(&[again], &humans), 1, "nor renews its turns");

        let message = Envelope::new(bob.clone(), p("dev-a"), "red");
        assert_eq!(
            (message.sender_role(&humans), message.clone().from_webhook().sender_role(&humans)),
            (Role::Human, Role::Ant)
        );
        assert!(!serde_json::to_string(&message).unwrap().contains("webhook"));
        let json = serde_json::to_string(&message.clone().from_webhook()).unwrap();
        assert!(json.contains(r#""webhook":true"#), "{json}");
        assert!(serde_json::from_str::<Envelope>(&json).unwrap().webhook);
        let reply = message.from_webhook().reply(p("dev-a"), "on it", Role::Human).unwrap();
        assert!(!reply.webhook, "replies are no webhook's");
    }

    #[test]
    fn a_thread_s_origin_comes_from_its_posts_not_from_what_a_later_post_carries() {
        let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
        let mut humans = Humans::default();
        for h in [&alice, &bob] {
            humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
        }
        let on =
            |from: &UserId, thread: &ThreadId| ChannelPost::on_thread(c.clone(), thread.clone(), from.clone(), "x");
        let root = ChannelPost::new(c.clone(), p("lead"), "@dev-a fix it").with_owner(Some(bob.clone()));
        let thread = root.thread_id.clone();
        let forged = on(&p("dev-a"), &thread).with_owner(Some(bob.clone())).with_creator(Some(p("boss")));
        let mut origin = ThreadOrigin::default();
        origin.push(&root, &humans);
        assert_eq!((&origin.owner, &origin.creator, origin.root), (&None, &Some(p("lead")), true), "an ant owns none");
        origin.push(&forged, &humans);
        // A later post with the thread's id as its own only claims to start it.
        let mut fake = on(&p("dev-b"), &thread);
        fake.id = root.id;
        origin.push(&fake, &humans);
        assert_eq!((&origin.owner, &origin.creator, origin.settled()), (&None, &Some(p("lead")), false));
        // The first human to post owns the thread.
        origin.push(&on(&alice, &thread), &humans);
        origin.push(&on(&bob, &thread), &humans);
        assert_eq!((&origin.owner, origin.settled()), (&Some(alice.clone()), true));
        let mut post = forged.clone();
        origin.apply(&mut post);
        assert_eq!((post.owner, post.creator), (Some(alice.clone()), Some(p("lead"))));

        // With its top-level post no longer kept, the thread has no creator.
        let mut lost = ThreadOrigin::default();
        lost.push(&forged, &humans);
        assert_eq!((&lost.owner, &lost.creator, lost.root), (&None, &None, false));
        // A webhook's top-level post names the owner its config does, a human; a human's owns its thread.
        let hook = ChannelPost::new(c.clone(), p("ci"), "red").from_webhook(Vec::new());
        let of = |post: ChannelPost| {
            let origin = ThreadOrigin::of_root(&post, &humans);
            (origin.owner, origin.creator)
        };
        assert_eq!(of(hook.clone().with_owner(Some(alice.clone()))), (Some(alice.clone()), None));
        assert_eq!(of(hook.with_owner(Some(p("dev-a")))), (None, None));
        assert_eq!(of(ChannelPost::new(c, bob.clone(), "hi").with_owner(Some(alice))), (Some(bob), None));
    }

    #[test]
    fn a_webhook_may_post_only_to_its_destinations_and_keep_only_its_own_events() {
        let hook = p("github");
        let ops: ChannelId = "ops".parse().unwrap();
        let perms = Permissions::for_webhook(&hook, &[Destination::Ant(p("dev-a")), Destination::Channel(ops)]);
        assert_eq!(
            perms.publish,
            [
                "ants.inbox.dev-a.github",
                "ants.chan.ops.github",
                "$JS.API.DIRECT.GET.KV_ants_channels.$KV.ants_channels.ops",
                "$JS.API.CONSUMER.INFO.ANTS.inbox-github",
                "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.github.>",
                "$KV.ants_webhooks.github.>",
                "$KV.ants_owners.*.dev-a",
                "ants.renew.github",
            ]
        );
        assert_eq!(perms.subscribe, ["_INBOX.github.>", "ants.renew.github.reply"]);
        let to_channel = Permissions::for_webhook(&hook, &[Destination::Channel("ops".parse().unwrap())]);
        assert!(!to_channel.publish.iter().any(|s| s.contains("ants_owners")), "posts' owners come from the posts");
        let domain: JsDomain = "hub".parse().unwrap();
        let in_domain = perms.with_js_domain(Some(&domain));
        assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_webhooks.github.>"));
        let human = Permissions::for_user(&UserId::human(), Role::Human);
        assert!(human.publish.iter().any(|s| s == "$JS.API.STREAM.CREATE.KV_ants_webhooks"), "ants init creates it");
    }

    #[test]
    fn an_observer_publishes_nothing_but_reads_its_feeds_status_and_the_roster() {
        let perms = Permissions::for_observer(&p("audit"));
        let ants: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("ants.")).collect();
        assert_eq!(ants, ["ants.renew.audit"], "no message, approval decision or cancel request");
        assert!(!perms.publish.iter().any(|s| s.starts_with("$KV.")), "no bucket writes");
        let creates: Vec<&str> =
            perms.publish.iter().filter_map(|s| s.strip_prefix("$JS.API.CONSUMER.CREATE.ANTS.")).collect();
        assert_eq!(
            creates,
            [
                "observe-requests-audit.ants.approvals.*",
                "observe-decisions-audit.ants.approvals.*.*",
                "observe-dead-audit.ants.dead.>"
            ]
        );
        assert!(!perms.publish.iter().any(|s| s.contains("STREAM.INFO.ANTS") || s.contains("DIRECT.GET.ANTS")));
        assert!(!perms.publish.iter().any(|s| s.contains("STREAM.MSG.GET")));
        let buckets: Vec<&String> = perms.publish.iter().filter(|s| s.contains("KV_")).collect();
        assert!(buckets.iter().all(|s| s.contains("KV_ants_roster")), "{buckets:?}");
        let acks: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("$JS.ACK.")).collect();
        assert_eq!(acks.len(), 6, "v1 and v2 acks of its own consumers only: {acks:?}");
        assert!(acks.iter().all(|s| s.contains(".ANTS.observe-") && s.ends_with("-audit.>")), "{acks:?}");
        let fc: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("$JS.FC.")).collect();
        assert_eq!(fc, ["$JS.FC.KV_ants_roster.>", "$JS.FC.*.*.KV_ants_roster.>"]);
        assert_eq!(
            perms.subscribe,
            [
                "_INBOX.audit.>",
                "ants.approvals.*",
                "ants.approvals.*.*",
                "ants.dead.>",
                "ants.status.*.*",
                "ants.renew.audit.reply"
            ]
        );
    }

    #[test]
    fn only_humans_get_the_info_of_the_stream_and_of_the_webhooks_bucket() {
        let infos = |perms: &Permissions| -> Vec<String> {
            perms
                .publish
                .iter()
                .filter(|s| s.ends_with(".STREAM.INFO.ANTS") || s.ends_with(".STREAM.INFO.KV_ants_webhooks"))
                .cloned()
                .collect()
        };
        let hook = Permissions::for_webhook(&p("github"), &[Destination::Ant(p("dev-a"))]);
        for perms in [Permissions::for_user(&p("dev-a"), Role::Ant), hook, Permissions::for_channel_manager()] {
            let domain: JsDomain = "hub".parse().unwrap();
            assert_eq!(infos(&perms.with_js_domain(Some(&domain))), Vec::<String>::new());
        }
        let human = Permissions::for_user(&UserId::human(), Role::Human);
        assert_eq!(infos(&human), ["$JS.API.STREAM.INFO.ANTS", "$JS.API.STREAM.INFO.KV_ants_webhooks"]);
        // Each finds JetStream on connecting through the info of a consumer named after it.
        let manager = Permissions::for_channel_manager();
        assert!(manager.publish.iter().any(|s| s == "$JS.API.CONSUMER.INFO.ANTS.inbox-channel-manager"));
        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        assert!(ant.publish.iter().any(|s| s == "$JS.API.CONSUMER.INFO.ANTS.inbox-dev-a"));
    }

    #[test]
    fn channels_are_readable_by_ants_and_written_by_humans_only() {
        let a = Permissions::for_user(&p("dev-a"), Role::Ant);
        for s in [
            "ants.chan.*.dev-a",
            "ants.out.chan.*.dev-a",
            "$JS.API.STREAM.INFO.KV_ants_channels",
            "$JS.API.DIRECT.GET.KV_ants_channels.>",
            "$JS.API.CONSUMER.CREATE.KV_ants_channels",
            "$JS.API.CONSUMER.CREATE.ANTS.chan-dev-a.ants.chan.>",
            "$JS.API.CONSUMER.CREATE.ANTS.chanread-dev-a.ants.chan.>",
            "$JS.ACK.ANTS.chan-dev-a.>",
            "$JS.ACK.*.*.ANTS.chan-dev-a.>",
        ] {
            assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
        }
        assert!(a.subscribe.iter().any(|s| s == subject::ALL_CHANNELS));
        assert!(!a.publish.iter().any(|s| s.contains("$KV.ants_channels") || s.contains("CREATE.KV_ants_channels.")));
        assert!(!a.publish.iter().any(|s| s == "ants.chan.>" || s == "$JS.API.CONSUMER.CREATE.ANTS.chan-dev-a"));
        let human = Permissions::for_user(&UserId::human(), Role::Human);
        for s in ["ants.chan.*.human", "$KV.ants_channels.>", "$JS.API.STREAM.CREATE.KV_ants_channels"] {
            assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
        }
        let domain: JsDomain = "hub".parse().unwrap();
        let in_domain = human.with_js_domain(Some(&domain));
        assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_channels.>"));
        let alice = Permissions::for_user(&p("alice"), Role::Human);
        for s in ["ants.chan.*.alice", "$KV.ants_channels.>"] {
            assert!(alice.publish.iter().any(|have| have == s), "any human: {s} missing from {:?}", alice.publish);
        }
    }

    #[test]
    fn only_ants_get_spare_readers_pinned_like_their_own() {
        let a = Permissions::for_user(&p("dev-a"), Role::Ant);
        assert_eq!(
            spare_readers("chanread-dev-a").collect::<Vec<_>>(),
            ["chanread-dev-a_1", "chanread-dev-a_2", "chanread-dev-a_3"]
        );
        for (reader, filter) in
            [("inbox-dev-a", "ants.inbox.dev-a.*"), ("notes-dev-a", "ants.notes.>"), ("chanread-dev-a", "ants.chan.>")]
        {
            for spare in spare_readers(reader) {
                for s in [
                    format!("$JS.API.CONSUMER.CREATE.ANTS.{spare}.{filter}"),
                    format!("$JS.API.CONSUMER.MSG.NEXT.ANTS.{spare}"),
                    format!("$JS.API.CONSUMER.DELETE.ANTS.{spare}"),
                ] {
                    assert!(a.publish.contains(&s), "{s} missing from {:?}", a.publish);
                }
            }
        }
        let spares = a.publish.iter().filter(|s| s.contains("-dev-a_")).count();
        assert_eq!(spares, 3 * 3 * SPARE_READERS, "no spare with any other filter, and none acks");
        let human = Permissions::for_user(&UserId::human(), Role::Human);
        assert!(!human.publish.iter().any(|s| s.contains("-human_")));
        let named_like =
            |spare: String| spare.strip_prefix("chanread-").is_some_and(|user| user.parse::<UserId>().is_ok());
        assert!(!spare_readers("chanread-dev-a").any(named_like), "no spare is another user's reader");
    }

    #[test]
    fn only_the_owner_changes_members_and_messenger_and_never_the_owner() {
        let mut humans = Humans::default();
        humans.add("alice", HumanEntry::new(p("alice"))).unwrap();
        let mut entry = ChannelEntry::new("ops".parse().unwrap());
        entry.add_member(p("dev-a"));
        let ops = [
            ChannelOp::AddMember { ant: p("dev-b") },
            ChannelOp::AddMember { ant: p("dev-a") },
            ChannelOp::RemoveMember { ant: p("dev-a") },
            ChannelOp::SetMessenger { ant: p("dev-c") },
        ];
        let before = entry.clone();
        assert_eq!(entry.apply(&p("lead"), &ops, &humans), Err(Refusal::NotOwner { owner: None }));
        entry.owner = Some(p("lead"));
        let refused = entry.apply(&p("dev-a"), &ops, &humans).unwrap_err();
        assert_eq!(refused.to_string(), "only the channel's owner may change it; its owner is lead");
        let human = [ChannelOp::AddMember { ant: p("dev-d") }, ChannelOp::SetMessenger { ant: p("alice") }];
        assert_eq!(entry.apply(&p("lead"), &human, &humans), Err(Refusal::Human(p("alice"))));
        assert_eq!(entry, ChannelEntry { owner: Some(p("lead")), ..before }, "a refusal changes nothing");
        let member = [ChannelOp::AddMember { ant: p("alice") }];
        assert_eq!(entry.apply(&p("lead"), &member, &humans).unwrap(), ["added alice"], "a human may be a member");
        let member = [ChannelOp::RemoveMember { ant: p("alice") }];
        assert_eq!(entry.apply(&p("lead"), &member, &humans).unwrap(), ["removed alice"]);

        let changed = entry.apply(&p("lead"), &ops, &humans).unwrap();
        assert_eq!(changed, ["added dev-b", "removed dev-a", "messenger dev-c"]);
        assert_eq!((entry.members.as_slice(), entry.messenger.as_ref()), ([p("dev-b")].as_slice(), Some(&p("dev-c"))));
        assert_eq!(entry.owner, Some(p("lead")));
        assert!(entry.apply(&p("lead"), &ops[..1], &humans).unwrap().is_empty(), "nothing to change");
        let cleared = entry.apply(&p("lead"), &[ChannelOp::ClearMessenger], &humans).unwrap();
        assert_eq!(
            (cleared, entry.messenger.as_ref()),
            (vec!["messenger back to the default messenger".to_owned()], None)
        );
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains(r#""owner":"lead""#), "{json}");
        assert!(!serde_json::to_string(&ChannelEntry::new(entry.channel.clone())).unwrap().contains("owner"));
    }

    #[test]
    fn channel_admin_requests_match_their_subject_and_answer_the_requester_only() {
        let ops = vec![ChannelOp::AddMember { ant: p("dev-b") }, ChannelOp::ClearMessenger];
        let request = ChannelAdminRequest::new("ops".parse().unwrap(), p("lead"), "t1".parse().unwrap(), ops);
        assert_eq!(request.subject(), "ants.chanadm.ops.lead");
        assert_eq!(subject::parse_chanadm("ants.chanadm.ops.lead"), Some(("ops".parse().unwrap(), p("lead"))));
        assert_eq!(request.check_subject("ants.chanadm.ops.lead"), Ok(()));
        assert!(request.check_subject("ants.chanadm.ops.dev-a").is_err());
        assert!(request.check_subject("ants.chanadm.dev.lead").is_err());
        assert!(!request.is_expired(request.requested_at) && request.is_expired(request.requested_at + CHANADM_TTL));
        assert!(request.answers_on("_INBOX.lead.abc.1"));
        for forged in ["_INBOX.dev-a.abc", "_INBOX.lead.stored.dev-a", "_INBOX.lead.", "_INBOX.leader.x", "ants.x"] {
            assert!(!request.answers_on(forged), "{forged}");
        }
        assert_eq!(request.check_ops(), Ok(()));
        let none = ChannelAdminRequest { ops: Vec::new(), ..request.clone() };
        assert!(none.check_ops().is_err());
        let many = ChannelAdminRequest { ops: vec![ChannelOp::ClearMessenger; MAX_CHANNEL_OPS + 1], ..request.clone() };
        assert!(many.check_ops().is_err());

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["ops"][0], serde_json::json!({"op": "add_member", "ant": "dev-b"}));
        assert_eq!(json["ops"][1], serde_json::json!({"op": "clear_messenger"}));
        assert_eq!(serde_json::from_value::<ChannelAdminRequest>(json).unwrap(), request);
        let refused = ChannelAdminReply {
            request_id: request.id,
            outcome: ChannelAdminOutcome::Refused { refusal: Refusal::NotInRoster(p("dev-z")) },
        };
        let json = serde_json::to_value(&refused).unwrap();
        assert_eq!(
            json["outcome"],
            serde_json::json!({"outcome": "refused", "refusal": {"refusal": "not_in_roster", "detail": "dev-z"}})
        );
        assert_eq!(serde_json::from_value::<ChannelAdminReply>(json).unwrap(), refused);
    }

    #[test]
    fn the_channel_manager_hears_requests_and_writes_channels_only() {
        let manager = Permissions::for_channel_manager();
        assert_eq!(
            manager.subscribe,
            ["ants.chanadm.*.*", "_INBOX.channel-manager.>", "ants.renew.channel-manager.reply"]
        );
        for s in [
            "ants.chan.*.channel-manager",
            "$KV.ants_channels.>",
            "$JS.API.DIRECT.GET.KV_ants_channels.>",
            "$JS.API.DIRECT.GET.KV_ants_roster.>",
            "$JS.API.CONSUMER.CREATE.KV_ants_humans",
        ] {
            assert!(manager.publish.iter().any(|have| have == s), "{s} missing from {:?}", manager.publish);
        }
        let writes: Vec<_> =
            manager.publish.iter().filter(|s| s.starts_with("$KV.") || s.starts_with("ants.")).collect();
        assert_eq!(
            writes,
            [
                "ants.chan.*.channel-manager",
                "$KV.ants_channels.>",
                "$KV.ants_webhooks.channel-manager.serve",
                "ants.renew.channel-manager"
            ]
        );
        assert!(!manager.publish.iter().any(|s| s.starts_with("_INBOX") || s.contains("STREAM.CREATE")));
        // Humans read its version; ants, which read only markers (`*.webhook`), do not take it for a webhook.
        let read = "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.channel-manager.serve";
        assert!(Permissions::for_user(&UserId::human(), Role::Human).publish.iter().any(|s| s == read));
        assert!(!channel_manager_serving_key().ends_with(&format!(".{WEBHOOK_MARKER}")));
        let marker = Serving::here("m1");
        let json = serde_json::to_string(&marker).unwrap();
        assert_eq!(json, format!(r#"{{"machine":"m1","version":"{ANTS_VERSION}","perms":{PERMISSIONS_VERSION}}}"#));
        assert_eq!(serde_json::from_str::<Serving>(&json).unwrap(), marker);
        assert!(serde_json::from_slice::<Serving>(b"github").is_err(), "the marker of an older webhook holds its name");

        let a = Permissions::for_user(&p("dev-a"), Role::Ant);
        assert!(a.publish.iter().any(|s| s == "ants.chanadm.*.dev-a"));
        assert!(!a.publish.iter().any(|s| s.starts_with("ants.out.chanadm")), "no outbox form");
        assert!(!a.subscribe.iter().any(|s| s.starts_with("ants.chanadm")));
    }

    #[test]
    fn notes_posts_spend_the_lower_of_the_message_and_the_notes_budget() {
        let t: ThreadId = "t1".parse().unwrap();
        let first = NotesPost::spend(t.clone(), p("dev-a"), "draft", 3, None).unwrap();
        assert_eq!(first.turn_budget, 2, "empty notes start from the message's budget");
        let second = NotesPost::spend(t.clone(), p("dev-b"), "agreed", 8, Some(&first)).unwrap();
        assert_eq!(second.turn_budget, 1, "the notes' budget is lower");
        let last = NotesPost::spend(t.clone(), p("dev-a"), "done", 1, Some(&second)).unwrap();
        assert_eq!(last.turn_budget, 0);
        let refused = NotesPost::spend(t.clone(), p("dev-b"), "more", 8, Some(&last));
        assert_eq!(refused, Err(Error::BudgetExhausted(t.clone())));
        let human = NotesPost::new(t.clone(), UserId::human(), "go on");
        assert_eq!(
            NotesPost::spend(t, p("dev-b"), "more", 8, Some(&human)).unwrap().turn_budget,
            DEFAULT_TURN_BUDGET - 1
        );
    }

    #[test]
    fn agent_replies_spend_the_budget_until_exhausted() {
        let mut msg = Envelope::new(p("dev-a"), p("dev-b"), "plan?");
        msg.turn_budget = 1;
        let reply = msg.reply(p("dev-b"), "draft", Role::Ant).unwrap();
        assert_eq!(reply.turn_budget, 0);
        assert_eq!(reply.reply(p("dev-a"), "again", Role::Ant), Err(Error::BudgetExhausted(msg.thread_id)));
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
        let back = handoff.reply(p("dev-b"), "fine", Role::Ant).unwrap();
        assert_eq!((back.turn_budget, back.owner.clone()), (2, Some(alice.clone())), "an agent's reply spends one");
        let notice = back.follow_up(p("dev-b"), alice.clone(), "over to you");
        assert_eq!((notice.thread_id.clone(), notice.owner.clone()), (ask.thread_id, Some(alice)));
        assert_eq!((notice.turn_budget, notice.in_reply_to), (DEFAULT_TURN_BUDGET, None));
        let ownerless = Envelope::new(p("dev-a"), p("dev-b"), "q");
        assert_eq!(ownerless.reply(p("dev-b"), "a", Role::Ant).unwrap().owner, None);
    }

    #[test]
    fn the_owner_is_left_out_when_none_and_older_envelopes_have_none() {
        let msg = Envelope::new(p("dev-a"), p("dev-b"), "q");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("owner"), "{json}");
        let owned = msg.with_owner(Some(p("alice")));
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
        let bob = read(r#"{"human":"bob","team":"ops"}"#);
        assert_eq!(bob.registered_at, None);
        assert_eq!(bob.check_key("bob"), Ok(()));
        assert_eq!(bob.check_key("dev-a").unwrap_err().to_string(), "human entry of bob found at key dev-a");
        assert_eq!(subject::human_entry(&p("bob")), "$KV.ants_humans.bob");

        let mut humans = Humans::from_entries([("alice", entry), ("dev-a", read(r#"{"human":"carol"}"#))]);
        assert!(humans.contains(&UserId::human()), "the default human needs no entry");
        assert!(humans.contains(&p("alice")));
        assert!(!humans.contains(&p("dev-a")) && !humans.contains(&p("carol")), "a mismatched entry is skipped");
        assert_eq!((humans.role(&p("alice")), humans.role(&p("dev-a"))), (Role::Human, Role::Ant));
        assert!(humans.add("dev-b", bob.clone()).is_err() && !humans.contains(&p("bob")));
        humans.add("bob", bob).unwrap();
        humans.remove(&p("bob"));
        humans.remove(&UserId::human());
        assert_eq!(humans.iter().map(UserId::as_str).collect::<Vec<_>>(), ["alice", "human"]);
    }

    #[test]
    fn every_human_has_the_human_rights_and_only_humans_write_the_humans_bucket() {
        let alice = Permissions::for_user(&p("alice"), Role::Human);
        let human = Permissions::for_user(&UserId::human(), Role::Human);
        let rename = |s: &String| s.replace(".human", ".alice").replace("-human", "-alice");
        assert_eq!(alice.publish, human.publish.iter().map(rename).collect::<Vec<_>>(), "the same rights by any name");
        assert_eq!(alice.subscribe, human.subscribe.iter().map(rename).collect::<Vec<_>>());
        for s in [
            "ants.inbox.>",
            "$JS.API.STREAM.CREATE.ANTS",
            "$JS.API.STREAM.MSG.GET.ANTS",
            "$JS.API.DIRECT.GET.ANTS",
            "ants.approvals.*.*",
            "ants.dead.>",
            "$JS.API.STREAM.CREATE.KV_ants_humans",
            "$KV.ants_humans.>",
            "$JS.API.CONSUMER.CREATE.ANTS.inbox-alice.ants.inbox.alice.*",
            "$JS.API.CONSUMER.CREATE.ANTS.notes-alice.ants.notes.>",
        ] {
            assert!(alice.publish.iter().any(|have| have == s), "{s} missing from {:?}", alice.publish);
        }
        assert!(alice.subscribe.iter().any(|s| s == subject::ALL));

        let reads = [
            "$JS.API.STREAM.INFO.KV_ants_humans",
            "$JS.API.DIRECT.GET.KV_ants_humans.>",
            "$JS.API.CONSUMER.CREATE.KV_ants_humans",
            "$JS.FC.KV_ants_humans.>",
            "$JS.FC.*.*.KV_ants_humans.>",
        ];
        let named_human = Permissions::for_user(&UserId::human(), Role::Ant);
        for ant in [Permissions::for_user(&p("dev-a"), Role::Ant), named_human] {
            for s in reads {
                assert!(ant.publish.iter().any(|have| have == s), "{s} missing from {:?}", ant.publish);
            }
            assert!(
                !ant.publish
                    .iter()
                    .any(|s| s.contains("$KV.ants_humans") || s.contains("STREAM.CREATE.KV_ants_humans"))
            );
            let human_only = ["ants.inbox.>", "ants.dead.>", "$JS.API.DIRECT.GET.ANTS"];
            assert!(!ant.publish.iter().any(|s| human_only.contains(&s.as_str())), "no human rights");
        }

        let m1: Machine = "m1".parse().unwrap();
        assert_eq!(m1.setup_inbox(&p("alice")), "_INBOX.alice.leaf.m1");
        let leaf = Permissions::for_leaf(&m1, [(&p("alice"), Role::Human), (&p("dev-a"), Role::Ant)]);
        for s in ["_INBOX.alice.leaf.m1.>", "_INBOX.human.leaf.m1.>", "$KV.ants_humans.>"] {
            assert!(leaf.publish.iter().any(|have| have == s), "{s} missing from {:?}", leaf.publish);
        }
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let msg = Envelope::new(UserId::human(), UserId::messenger(), "route this");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("in_reply_to"));
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), msg);
    }

    #[test]
    fn a_link_key_names_who_passed_the_link_to_its_thread_on_to_whom() {
        let (thread, other) = (ThreadId::generate(), ThreadId::generate());
        let key = link_key(&p("dev-a"), &thread, &p("dev-b"));
        assert!(key.starts_with("dev-a."), "credentials confine an ant to its own keys");
        assert_eq!(parse_link_key(&key, &thread), Some((p("dev-a"), p("dev-b"))));
        assert_eq!(parse_link_key(&key, &other), None);
        for malformed in [format!("dev-a.{thread}"), format!("dev-a.{thread}.dev-b.x"), format!("{thread}.dev-b")] {
            assert_eq!(parse_link_key(&malformed, &thread), None, "{malformed}");
        }
    }

    #[test]
    fn a_reaction_key_names_the_reactor_first_then_the_message_and_reaction() {
        let message = Uuid::now_v7();
        let eyes: Reaction = "eyes".parse().unwrap();
        let key = reaction_key(&p("dev-a"), &message, &eyes);
        assert_eq!(key, format!("dev-a.{message}.eyes"), "credentials confine a reactor to its own keys");
        assert_eq!(ReactionEntry::new(p("dev-a"), message, eyes.clone()).key(), key);
        assert_eq!(parse_reaction_key(&key), Some((p("dev-a"), message, eyes)));
        for malformed in [format!("dev-a.{message}"), format!("dev-a.{message}.eyes.x"), "dev-a.t1.eyes".to_owned()] {
            assert_eq!(parse_reaction_key(&malformed), None, "{malformed}");
        }
        assert!("Eyes".parse::<Reaction>().is_err() && "a.b".parse::<Reaction>().is_err());
    }

    #[test]
    fn humans_set_every_title_but_only_their_own_pins_and_archives_and_ants_neither() {
        let human = Permissions::for_user(&p("alice"), Role::Human);
        for s in [
            "$KV.ants_threads.title.*",
            "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.title.*",
            "$KV.ants_threads.marks.alice",
            "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.marks.alice",
            "$JS.API.STREAM.CREATE.KV_ants_threads",
        ] {
            assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
        }
        let threads: Vec<&String> = human.publish.iter().filter(|s| s.contains("ants_threads")).collect();
        assert!(threads.iter().all(|s| !s.contains("marks") || s.ends_with("marks.alice")), "{threads:?}");
        assert!(!threads.iter().any(|s| s.ends_with("ants_threads.>") || s.contains("CONSUMER")), "{threads:?}");
        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        let observer = Permissions::for_observer(&p("watcher"));
        for none in [ant, observer] {
            assert!(!none.publish.iter().chain(&none.subscribe).any(|s| s.contains("ants_threads")));
        }
    }

    #[test]
    fn a_thread_is_pinned_or_archived_and_new_messages_show_on_an_archived_one() {
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        let mut marks = ThreadMarks::default();
        assert!(marks.pin(&t1) && !marks.pin(&t1));
        assert_eq!(marks.mark(&t1, 5), Some(Mark::Pinned));
        assert!(marks.archive(&t1, 10), "archiving unpins");
        assert!(!marks.pinned.contains_key(&t1));
        assert!(!marks.archive(&t1, 10), "archived already");
        assert_eq!(marks.mark(&t1, 10), Some(Mark::Archived { new_activity: false }));
        assert_eq!(marks.mark(&t1, 11), Some(Mark::Archived { new_activity: true }));
        assert!(marks.archive(&t1, 11), "archiving again moves past the new message");
        assert_eq!(marks.mark(&t1, 11), Some(Mark::Archived { new_activity: false }));
        assert!(marks.pin(&t1) && marks.archived.is_empty(), "pinning unarchives");
        assert!(marks.unpin(&t1) && !marks.unpin(&t1));
        assert!(!marks.unarchive(&t2));
        assert_eq!(marks.mark(&t2, 1), None);
        let json = serde_json::to_string(&marks).unwrap();
        assert_eq!(json, "{}");
        assert_eq!(serde_json::from_str::<ThreadMarks>(&json).unwrap(), marks);
    }

    #[test]
    fn a_title_is_one_trimmed_line_of_bounded_length() {
        let title = ThreadTitle::new(ThreadId::generate(), "  Deploy fix ", p("alice")).unwrap();
        assert_eq!(title.title, "Deploy fix");
        assert_eq!(title_key(&title.thread_id), format!("title.{}", title.thread_id));
        assert_eq!(marks_key(&p("alice")), "marks.alice");
        for bad in ["", "  ", "two\nlines", &"x".repeat(MAX_TITLE + 1)] {
            assert!(ThreadTitle::new(ThreadId::generate(), bad, p("alice")).is_err(), "{bad:?}");
        }
        assert!(ThreadTitle::new(ThreadId::generate(), &"é".repeat(MAX_TITLE), p("alice")).is_ok());
    }

    #[test]
    fn everyone_writes_only_their_own_reactions_and_only_humans_read_others() {
        let own = "$KV.ants_reactions.dev-a.>";
        let others = ["$JS.API.DIRECT.GET.KV_ants_reactions.>", "$JS.API.CONSUMER.CREATE.KV_ants_reactions"];
        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        assert!(ant.publish.iter().any(|s| s == own), "{:?}", ant.publish);
        assert!(ant.publish.iter().any(|s| s == "$JS.API.DIRECT.GET.KV_ants_reactions.$KV.ants_reactions.dev-a.>"));
        let reactions: Vec<&String> = ant.publish.iter().filter(|s| s.contains("ants_reactions")).collect();
        assert_eq!(reactions.len(), 2, "nothing of others' reactions: {reactions:?}");
        let human = Permissions::for_user(&p("alice"), Role::Human);
        assert!(human.publish.iter().any(|s| s == "$KV.ants_reactions.alice.>"));
        assert!(!human.publish.iter().any(|s| s == "$KV.ants_reactions.>" || s == "$KV.ants_reactions.*.>"));
        for s in others.into_iter().chain(["$JS.API.STREAM.CREATE.KV_ants_reactions"]) {
            assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
        }
        let observer = Permissions::for_observer(&p("watcher"));
        assert!(!observer.publish.iter().chain(&observer.subscribe).any(|s| s.contains("ants_reactions")));
    }

    #[test]
    fn ants_write_only_their_own_chain_and_read_every_one() {
        let a = Permissions::for_user(&p("dev-a"), Role::Ant);
        for s in ["$KV.ants_chains.dev-a", "$JS.API.DIRECT.GET.KV_ants_chains.>", "$JS.API.STREAM.INFO.KV_ants_chains"]
        {
            assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
        }
        assert!(!a.publish.iter().any(|s| s.starts_with("$KV.ants_chains.") && s != "$KV.ants_chains.dev-a"));
        assert!(!a.publish.iter().any(|s| s.contains("CREATE.KV_ants_chains")), "only humans create the bucket");
        let human = Permissions::for_user(&UserId::human(), Role::Human);
        assert!(human.publish.iter().any(|s| s == "$JS.API.STREAM.CREATE.KV_ants_chains"));
        assert!(!human.publish.iter().any(|s| s.starts_with("$KV.ants_chains.")), "only the ant writes its chain");

        let entry = ChainEntry::new(p("dev-a"), vec![p("lead"), p("dev-b")]);
        assert_eq!(subject::chain(&entry.ant), "$KV.ants_chains.dev-a");
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
            ant: p("dev-a"),
            machine: "build-1".to_owned(),
            instance: None,
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
            version: None,
            perms: None,
            signed_out: None,
            limited: None,
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("workdir") && !json.contains("activity") && !json.contains("harnesses"), "{json}");
        assert!(!json.contains("escalate_to") && !json.contains("instance"), "{json}");
        assert!(!json.contains("version") && !json.contains("perms"), "{json}");
        assert!(!json.contains("signed_out") && !json.contains("limited"), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), entry);
        assert_eq!(entry.leader(), None);
        let versioned =
            RosterEntry { version: Some("0.3.1".to_owned()), perms: Some(PermissionsVersion(2)), ..entry.clone() };
        let json = serde_json::to_string(&versioned).unwrap();
        assert!(json.contains(r#""version":"0.3.1","perms":2"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), versioned);
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
        let since = "2026-01-01T00:00:00Z".parse().unwrap();
        let out = RosterEntry {
            signed_out: Some(SignInNeeded { since, sign_in: Some("copilot login".to_owned()) }),
            ..entry.clone()
        };
        let json = serde_json::to_string(&out).unwrap();
        assert!(json.contains(r#""signed_out":{"since":"2026-01-01T00:00:00Z","sign_in":"copilot login"}"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), out);
        let unknown = RosterEntry { signed_out: Some(SignInNeeded { since, sign_in: None }), ..entry.clone() };
        let json = serde_json::to_string(&unknown).unwrap();
        assert!(json.contains(r#""signed_out":{"since":"2026-01-01T00:00:00Z"}"#), "{json}");
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), unknown);
        let until = "2026-01-01T00:15:00Z".parse().unwrap();
        let limited =
            RosterEntry { limited: Some(HarnessLimit { kind: LimitKind::Quota, since, until }), ..entry.clone() };
        let json = serde_json::to_string(&limited).unwrap();
        assert!(
            json.contains(
                r#""limited":{"kind":"quota","since":"2026-01-01T00:00:00Z","until":"2026-01-01T00:15:00Z"}"#
            ),
            "{json}"
        );
        assert_eq!(serde_json::from_str::<RosterEntry>(&json).unwrap(), limited);
        assert_eq!(entry.key(), "dev-a");
        assert_eq!(entry.check_key("dev-a"), Ok(()));
        assert_eq!(entry.check_key("dev-b").unwrap_err().to_string(), "roster entry of dev-a found at key dev-b");
        assert_eq!(subject::roster(&entry.ant), "$KV.ants_roster.dev-a");

        let minimal = r#"{"ant":"dev-b","machine":"m","harness":"copilot","state":"idle",
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: RosterEntry = serde_json::from_str(minimal).unwrap();
        assert!(entry.capabilities.is_empty() && entry.escalate_to.is_empty());
        assert_eq!((entry.whoami, entry.instance), (None, None));
        assert_eq!((entry.version, entry.perms), (None, None), "older daemons publish no version");
        assert_eq!((entry.activity, entry.handling, entry.harnesses), (None, None, None));
        assert_eq!(entry.signed_out, None, "an older daemon's entry");
        assert_eq!(entry.limited, None, "an older daemon's entry");

        let newer = r#"{"ant":"dev-b","machine":"m","harness":"copilot","activity":"draining","harnesses":3,
            "signed_out":{"since":"2026-01-01T00:00:05Z","sign_in":"x login","reason":"expired"},
            "limited":{"kind":"daily_cap","since":"2026-01-01T00:00:05Z","until":"2026-01-01T01:00:00Z"},
            "started_at":"2026-01-01T00:00:00Z","heartbeat_at":"2026-01-01T00:00:10Z"}"#;
        let entry: RosterEntry = serde_json::from_str(newer).unwrap();
        assert_eq!((entry.activity, entry.harnesses), (Some(Activity::Unknown), Some(3)));
        assert_eq!(entry.signed_out.and_then(|s| s.sign_in).as_deref(), Some("x login"));
        assert_eq!(entry.limited.map(|l| l.kind), Some(LimitKind::Unknown));
    }

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

    #[test]
    fn threads_keep_the_ant_that_created_them_and_only_its_own_post_makes_it_so() {
        let (c, alice) = ("ops".parse::<ChannelId>().unwrap(), p("alice"));
        let mut humans = Humans::default();
        humans.add(alice.as_str(), HumanEntry::new(alice.clone())).unwrap();
        // A top-level post's creator is its author, whatever it says.
        let top = ChannelPost::new(c.clone(), p("lead"), "@dev-a fix it").with_creator(Some(p("boss")));
        assert_eq!(top.thread_creator(&humans), Some(p("lead")));
        assert_eq!(ChannelPost::new(c.clone(), alice.clone(), "hi").thread_creator(&humans), None, "humans own");
        let hook = ChannelPost::new(c.clone(), p("ci"), "red").from_webhook(vec![p("dev-a")]);
        assert_eq!(hook.thread_creator(&humans), None, "a webhook is no ant");
        let manager = ChannelPost::new(c.clone(), UserId::channel_manager(), "dev-b joined");
        assert_eq!(manager.thread_creator(&humans), None, "nor is the channel manager");
        let answer = top.reply(p("dev-a"), "@dev-b help", &[], &humans).unwrap();
        assert_eq!((answer.creator.as_ref(), answer.owner.as_ref()), (Some(&p("lead")), None));
        let by_alice = answer.reply(alice.clone(), "@dev-a go on", &[], &humans).unwrap();
        assert_eq!((by_alice.creator.as_ref(), by_alice.owner.as_ref()), (Some(&p("lead")), Some(&alice)));
        assert_eq!(by_alice.reply(p("dev-b"), "ok", &[], &humans).unwrap().creator, Some(p("lead")));
        // A later post keeps the creator it carries, unless a human.
        let named = ChannelPost::on_thread(c, top.thread_id.clone(), p("dev-b"), "x").with_creator(Some(alice.clone()));
        assert_eq!(named.thread_creator(&humans), None);

        // Messages on or from the thread keep it too.
        let msg = Envelope::new(p("lead"), p("dev-a"), "q").with_creator(Some(p("lead")));
        assert_eq!(msg.reply(p("dev-a"), "a", Role::Ant).unwrap().creator, Some(p("lead")));
        assert_eq!(msg.follow_up(p("dev-a"), p("human"), "n").creator, Some(p("lead")));

        let json = serde_json::to_string(&ChannelPost::new("ops".parse().unwrap(), p("lead"), "x")).unwrap();
        assert!(!json.contains("creator"), "{json}");
        assert!(serde_json::to_string(&top).unwrap().contains(r#""creator":"boss""#));
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap().creator, None, "older posts read");
        let older = r#"{"id":"0190a5c4-1d2e-7f00-8000-000000000001","thread_id":"t1","from":"human","to":"dev-a",
            "text":"hi","turn_budget":8,"sent_at":"2026-01-01T00:00:00Z"}"#;
        assert_eq!(serde_json::from_str::<Envelope>(older).unwrap().creator, None);
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

        let verdict = ApprovalDecision::new(request.id, request.ant.clone(), Verdict::AllowOnce);
        let msg = Envelope::decision(request.thread_id.clone(), p("dev-b"), verdict);
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
        assert_eq!(foreign.chain_decision(&request, &chain), None, "a decision on another ant's request");
        assert_eq!(Envelope::new(p("lead"), p("dev-a"), "hi").chain_decision(&request, &chain), None);

        let always = ApprovalDecision::new(request.id, request.ant.clone(), Verdict::AllowAlways);
        let always = Envelope::decision(request.thread_id.clone(), p("lead"), always);
        let decided = always.chain_decision(&request, &chain).unwrap();
        assert_eq!(decided.verdict, Verdict::Reject, "a verdict not offered rejects");
    }

    #[test]
    fn dead_letters_keep_the_envelope_or_an_excerpt_of_the_payload() {
        let msg = Envelope::new(UserId::human(), p("dev-a"), "build it");
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

    #[test]
    fn a_read_marker_keeps_selective_reads_until_the_unread_before_them_are_read() {
        let mut marker = ReadMarker::default();
        // Inbox messages at 3, 5, 8 and 9; 5 and 9 are read first, as on one thread.
        for seq in [5, 9] {
            marker.mark(seq);
        }
        marker.compact([3, 5, 8, 9]);
        assert_eq!((marker.below, marker.read.iter().copied().collect::<Vec<_>>()), (0, vec![5, 9]));
        assert!(!marker.is_read(3) && marker.is_read(5) && !marker.is_read(8));
        marker.mark(3);
        marker.compact([3, 5, 8, 9]);
        assert_eq!((marker.below, marker.read.len()), (5, 1), "3 and 5 read, 8 not");
        // 8 left the stream before it was read: nothing holds the marker back any more.
        marker.compact([9]);
        assert_eq!(marker, ReadMarker { below: 9, read: Default::default() });
        marker.mark(2);
        assert!(marker.read.is_empty(), "already read");
        let json = serde_json::to_string(&marker).unwrap();
        assert_eq!(json, r#"{"below":9}"#);
        assert_eq!(serde_json::from_str::<ReadMarker>("{}").unwrap(), ReadMarker::default());
    }

    #[test]
    fn a_device_id_travels_as_a_user_jwt_tag() {
        let device = DeviceId::generate();
        assert_ne!(device, DeviceId::generate(), "never reused");
        let tag = device.tag();
        assert!(tag.starts_with("ants-device:"));
        assert_eq!(DeviceId::from_tags(["team", tag.as_str()]), Some(device));
        assert_eq!(DeviceId::from_tags(["ants-device:not-a-uuid"]), None);
        assert_eq!(device.to_string().parse::<DeviceId>().unwrap(), device);
    }

    #[test]
    fn the_permissions_version_travels_as_a_user_jwt_tag() {
        let tag = PERMISSIONS_VERSION.tag();
        assert_eq!(tag, format!("ants-perms:{}", PERMISSIONS_VERSION.0));
        let device = DeviceId::generate().tag();
        assert_eq!(PermissionsVersion::from_tags([device.as_str(), tag.as_str()]), Some(PERMISSIONS_VERSION));
        assert_eq!(PermissionsVersion::from_tags([device.as_str()]), None);
        assert_eq!(PermissionsVersion::from_tags(["ants-perms:x"]), None);
        assert!(PermissionsVersion(0) < PERMISSIONS_VERSION);
    }
}
