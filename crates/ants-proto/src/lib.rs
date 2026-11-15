//! The ants wire contract: what travels over NATS and where.
//!
//! Every subject sits under `ants.`, so one NATS account can grant a user `ants.>` scoped to its own inbox, and a hub
//! can carry other traffic beside ants.

use std::time::Duration;

pub use uuid::Uuid;

mod ant_request;
mod approvals;
mod attachments;
mod chanadm;
mod channels;
mod dead;
pub mod duration;
mod enroll;
mod envelope;
mod error;
mod escalation;
mod humans;
mod ids;
mod inbox;
mod keys;
mod login;
mod permissions;
mod presence;
mod questions;
mod queue;
mod reactions;
mod roster;
mod sealed;
mod status;
pub mod subject;
mod threads;
mod timeline;

pub use ant_request::*;
pub use approvals::*;
pub use attachments::*;
pub use chanadm::*;
pub use channels::*;
pub use dead::*;
pub use enroll::*;
pub use envelope::*;
pub use error::*;
pub use escalation::*;
pub use humans::*;
pub use ids::*;
pub use inbox::*;
pub use keys::*;
pub use login::*;
pub use permissions::*;
pub use presence::*;
pub use questions::*;
pub use queue::*;
pub use reactions::*;
pub use roster::*;
pub use sealed::*;
pub use status::*;
pub use threads::*;
pub use timeline::*;

/// JetStream stream holding every durable ants message.
pub const STREAM: &str = "ANTS";
/// Subjects captured by [`STREAM`]. Status updates are core NATS only and stay out of it.
pub const STREAM_SUBJECTS: [&str; 6] =
    ["ants.inbox.>", "ants.notes.>", "ants.approvals.>", "ants.dead.>", "ants.chan.>", subject::ALL_TIMELINE];
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
/// of its devices; and of its [`ChannelReadMarkers`], keyed by [`channel_reads_key`]: which channel posts it has read.
/// Each human reads and writes only its own keys ([`Permissions::for_user`]); entries never expire.
pub const READ_BUCKET: &str = "ants_read";
/// KV bucket of where each human is reading: one [`Presence`] per device of a human following its inbox or the tool
/// requests, keyed by [`presence_key`], put when the following starts, renewed every [`PRESENCE_HEARTBEAT`] and
/// deleted when it ends. Kept in memory with a max age of [`PRESENCE_TTL`], so the presence of a CLI that died expires
/// on its own. Each human writes only its own keys and reads every human's, by direct gets and a watch, so a CLI
/// shows which humans have a device following; no ant reads any ([`Permissions::for_user`]). `ants webhook serve`
/// reads them so a notice skips the push targets of a device reading it already.
pub const PRESENCE_BUCKET: &str = "ants_presence";
/// KV bucket of the directory of human devices' encryption keys: the user JWT of each device of a human whose
/// credentials carry an encryption key ([`DeviceKey`]), keyed by [`device_entry_key`] and put by its CLI on connecting
/// when it differs, and a [`DeviceRevocation`] for each revoked key of a human's credentials, keyed by
/// [`revoked_device_key`]. Each human reads all of it and writes only its own keys; only the short-lived credentials
/// `ants auth revoke` and `ants auth push` issue from the account key put the revocation records
/// ([`Permissions::for_device_revoker`]). A reader takes a device's key only from a JWT it verifies itself. Entries
/// never expire.
pub const DEVICES_BUCKET: &str = "ants_devices";
/// KV bucket of what each human holds back of its notifications for a while ([`NotifyPrefs`]), keyed by its user id: a
/// snooze and the muted threads, set with `ants notify` and `ants thread mute` on any of its devices and read by `ants
/// webhook serve` before each notice; and of what a snooze or quiet hours held back from it ([`HeldNotices`]), keyed by
/// [`held_key`], for the digest `ants webhook serve` sends when they end. Each human reads and writes only its own keys
/// ([`Permissions::for_user`]); entries never expire, what they hold ends at the deadlines it carries.
pub const NOTIFY_BUCKET: &str = "ants_notify";
/// KV bucket of the reactions to messages: one [`ReactionEntry`] per reactor, message and reaction, keyed by
/// [`reaction_key`]. Each user writes, and deletes, only its own keys ([`Permissions::for_user`]), a human's devices sharing
/// the human's; only humans read the others', and ants those to messages they read, by asking ([`ReactionsRequest`]).
/// Its stream denies deletes, purges and rollups, so no one wipes the others' reactions, and drops each after the
/// stream's retention, as it does messages. Reactions never reach an inbox: no ant wakes for one, and no reply wait
/// ends with one.
pub const REACTIONS_BUCKET: &str = "ants_reactions";
/// How often an ant repeats a [`ReactionsRequest`] while no answer arrives; requests are core NATS, so one sent while
/// nothing answers is lost.
pub const REACTIONS_REPEAT: Duration = Duration::from_secs(2);
/// How long an ant waits for the answer to a [`ReactionsRequest`] in all.
pub const REACTIONS_WAIT: Duration = Duration::from_secs(10);
/// KV bucket of what humans make of threads: the title of each thread ([`ThreadTitle`], keyed by [`title_key`]), one
/// for every human, which any human sets, renames or clears, its done state ([`ThreadDone`], keyed by [`done_key`]),
/// one for every human too, which any human sets or clears, and each human's pinned and archived threads
/// ([`ThreadMarks`], keyed by [`marks_key`]) and the threads it snoozed on home ([`ThreadSnoozes`], keyed by
/// [`snoozed_key`]), each shared by its devices and read and written by that human alone
/// ([`Permissions::for_user`]), and the TL;DR of each thread a human's CLI asked an ant for ([`ThreadSummary`], keyed
/// by [`summary_key`]), which any human puts or deletes. Ants read and write none of it: a title an ant suggests
/// ([`Envelope::title`], [`ChannelPost::title`], [`NotesPost::title`]) and a TL;DR an ant replies with
/// ([`Envelope::summary`]) get there through a human's CLI. Entries never expire.
pub const THREADS_BUCKET: &str = "ants_threads";
/// Most characters a thread title has.
pub const MAX_TITLE: usize = 120;
/// How often a following CLI re-puts its device's [`Presence`].
pub const PRESENCE_HEARTBEAT: Duration = Duration::from_secs(10);
/// Max age of [`PRESENCE_BUCKET`]: a presence not renewed for this long is gone.
pub const PRESENCE_TTL: Duration = Duration::from_secs(30);
/// Turns a new thread may spend on ant-to-ant replies before it goes back to a human.
pub const DEFAULT_TURN_BUDGET: u32 = 8;
/// The tool rules a thread a webhook or schedule starts is narrowed to when its config sets no `allow`
/// ([`Envelope::allow`]): reading files in the workdir, searching, thinking, and the ants tools that talk to other ants,
/// whose messages carry the narrowing on, or ask a human a question.
pub const WEBHOOK_ALLOW: [&str; 15] = [
    "read:*",
    "search",
    "think",
    "other:list_ants",
    "other:check_inbox",
    "other:read_notes",
    "other:post_notes",
    "other:send_message",
    "other:ask_ant",
    "other:route",
    "other:escalate",
    "other:react",
    "other:list_reactions",
    "other:suggest_thread_title",
    "other:ask_human",
];
/// Chain levels an escalation may climb before it goes to the thread's owner ([`Escalation::up`]).
pub const DEFAULT_MAX_ESCALATION_HOPS: u32 = 3;

#[cfg(test)]
fn p(s: &str) -> UserId {
    s.parse().unwrap()
}
