//! The ants wire contract: what travels over NATS and where.
//!
//! Every subject sits under `ants.`, so one NATS account can grant a user `ants.>` scoped to its own inbox, and a hub
//! can carry other traffic beside ants, but for KV buckets' and object stores' own and sense's event logs
//! ([`subject::sense`]), which no human may hear.

use std::time::Duration;

pub use uuid::Uuid;

mod acp;
mod admin;
mod ant_request;
mod approvals;
mod attachments;
mod behalf;
mod chanadm;
mod channels;
mod colony;
mod consent;
mod dead;
pub mod duration;
mod enroll;
mod envelope;
mod error;
mod escalation;
mod grant;
mod humans;
mod identity;
mod ids;
mod inbox;
mod keys;
mod login;
mod machines;
mod mcp;
mod memories;
mod permissions;
mod presence;
pub mod provider;
mod questions;
mod queue;
mod reactions;
mod roles;
mod sealed;
mod sense;
mod shared;
mod status;
pub mod subject;
#[cfg(test)]
mod test_support;
mod text;
mod threads;
mod timeline;
mod trails;

pub use acp::{call_ended, call_status, tool_kind};
pub use admin::{
    ADMIN_PAYLOADS, ASSISTANT_ORDER_KINDS, AdminOp, AdminOrder, AdminOutcome, AdminPayload, FieldChange,
    HARNESSES_FIELD, Hidden, MAX_ADMIN_PAYLOAD, ORDER_CLOCK_SKEW, ORDER_HOLD, ORDER_RULE, ORDER_SIGNING_CONTEXT,
    OrderError, OrderState, ProviderExport, Settings, admin_outcome_key, admin_outcome_of, canonical_json,
    covers_order, held_orders_key,
};
pub use ant_request::{AntReply, AntRequest, AskedAnt};
pub use approvals::{ApprovalDecision, ApprovalRequest, MAX_LABEL, ToolCall, Verdict};
pub use attachments::{
    ATTACHMENT_CHUNK, ATTACHMENT_HEADER, ATTACHMENT_HEADROOM, Attachment, AttachmentHandle, AttachmentScope,
    FILES_BUCKET, FILES_STREAM, IMAGE_MIMES, MAX_ATTACHMENT_NAME, MAX_ATTACHMENT_SIZE, MAX_ATTACHMENTS,
    SealedAttachment, check_attachments, guess_mime,
};
pub use behalf::{ASSISTANT_RECORD, Acting, AssistantRecord, RecordError, assistant_record_key};
pub use chanadm::{ChannelAdminOutcome, ChannelAdminReply, ChannelAdminRequest, ChannelOp, Refusal};
pub use channels::{ChannelEntry, ChannelPost, ThreadOrigin};
pub use colony::{
    Activity, AntModel, ColonyEntry, HarnessAgent, HarnessFailing, HarnessLimit, LimitKind, MemoryLow, Paused,
    ServingFallback, SignInNeeded, Unserved,
};
pub use consent::{
    Act, ActAnswer, CONFIRMATIONS, Confirmations, ConfirmedAct, ConsentApplied, ConsentError, ConsentKey, Consents,
    MAX_CONFIRMED, MAX_PRE_ALLOW, PRE_ALLOW, PreAllow, confirmations_key, pre_allow_key, pre_allow_rule,
};
pub use dead::DeadLetter;
pub use enroll::{
    ENROLL_INBOX, ENROLL_REPEAT, EnrollHost, EnrollLeaf, EnrollReply, EnrollRequest, RenewReply, RenewRequest,
    enrollment_user,
};
pub use envelope::{
    Envelope, FindingOutcome, FindingVerdict, MAX_OUTCOME_REASON, MAX_REVIEW_FINDINGS_BYTES, Mention, NotesPost,
    NoticeKind, Quote, ReviewCommit, ReviewOutcomes, SummaryOf, SummaryRequest, parse_mentions,
};
pub use error::Error;
pub use escalation::{Away, ChainEntry, Escalation, EscalationReason, Hop, OwnerBecause};
pub use grant::{
    ASSISTANT_GRANT, Applied, AssistantGrant, GRANT_CLOCK_SKEW, GrantError, Grants, SealingDevice, UserJwt,
    UserJwtError, VerifiedGrant, assistant_grant_key, assistant_grant_of,
};
pub use humans::{HumanEntry, Humans, writes_human_entries};
pub use identity::{
    AntIdentity, Avatar, Cell, ClonedFrom, FACE_WIDTH, Frame, Grid, HEAD_COLUMNS, HEAD_ROWS, HUE_NAMES, Mood, Motion,
    MovedMark, Palette, Pixel, Rgb, SeedSource, Side, Stage, Theme, hue_named,
};
pub use ids::{BuiltinAnt, ChannelId, JsDomain, Machine, Reaction, Role, RunId, ThreadId, TipId, TrailName, UserId};
pub use inbox::{ChannelNotify, ChannelReadMarkers, HeldNotices, NotifyPrefs, ReadMarker};
pub(crate) use keys::STORAGE_WARNING_PREFIX;
pub use keys::{
    ANTS_VERSION, Feed, SERVING_EVERY, SPARE_READERS, Serving, TRAIL_ANSWERING, TRAIL_RUNS_INDEX, TRAIL_STEPS,
    assisted_marks_key, assisted_owner_key, assisted_reminders_key, assisted_withdrawal_key, attempt_key,
    chan_consumer, chan_reader, channel_manager_serving_key, channel_reads_key, channel_seen_key, daily_spend_key,
    dead_reader, done_key, done_mark_key, done_reader, escalation_deadlines_key, held_key, inbox_consumer,
    inbox_prefix, link_key, link_outbox, links_to, marks_key, mention_key, message_mark_key, notes_consumer,
    notify_consumer, owner_key, owner_outbox, owner_record_id, parse_daily_spend_key, parse_link_key,
    parse_reaction_key, parse_thread_spend_key, parse_trail_run_key, parse_webhook_marker_key, reaction_key,
    reactions_to, reminders_key, sent_reader, session_key, spare_readers, storage_warning_key, summary_key,
    thread_origin_key, thread_spend_key, title_key, trail_answering_key, trail_call_key, trail_run_key,
    trail_runs_index_key, trail_runs_reader, trail_session_key, trail_step_key, unserved_key, webhook_marker_key,
    webhook_seen_key,
};
pub use keys::{
    TIP_RETIRED, TRAIL_STEPS_ON, trail_rating_key, trail_thread_step_key, trail_thread_steps_prefix, trail_tip_key,
    trail_tip_retired_key, trail_tip_vote_key,
};
pub use login::{
    BOOTSTRAP_MAX_PAYLOAD, BOOTSTRAP_USER, LOGIN_REPEAT, LOGIN_TTL, LoginAnswer, LoginReply, LoginRequest,
    MAX_REGISTRATIONS, REGISTERED_TAG, tagged_registered,
};
pub use machines::{
    ACCOUNT_HOLDER_KEY, AccountHolder, MachineRecord, admin_consumer, host_inbox_prefix, host_user, machines_of,
};
pub use mcp::{
    MCP_FRESH, MCP_SERVERS_CAP, MCP_SIGN_IN_FOR, MCP_TEXT_CAP, McpReport, McpServerState, McpSignIn, McpStatus,
};
pub use memories::{
    BdMemories, BdMemory, CoreMemories, MEMORIES_CAP, MEMORIES_FRESH, MEMORY_FILE_CAP, Memories, MemoryFile,
    ProviderMemories, Skipped,
};
pub use permissions::{
    Destination, LeafRole, PERMISSIONS_VERSION, Permissions, PermissionsVersion, WEB_TAG, is_web, js_domain_of,
};
pub use presence::{
    DeviceId, DeviceKey, DeviceRevocation, Following, Presence, REVOKED_DEVICES, device_entry_key, device_entry_of,
    presence_key, revoked_device_key,
};
pub use questions::{
    Answer, Answered, MAX_DESCRIPTION, MAX_HEADER, MAX_OPTION, MAX_OPTIONS, MAX_QUESTION, MAX_QUESTIONS, MAX_TYPED,
    MIN_OPTIONS, Pick, Question, QuestionOption, Questions,
};
pub use queue::{
    CancelReply, CancelRequest, ConsumerPlace, Hold, MessageMark, QueueReply, QueueRequest, QueuedMessage,
};
pub use reactions::{
    Reacted, ReactionChange, ReactionEntry, ReactionsOutcome, ReactionsReply, ReactionsRequest, ReactionsResponder,
};
pub use roles::{
    ADMIN_TAG, AdminRole, AssistantOf, JwtClaims, JwtError, VerifiedUser, is_admin, verified_jwt, verify_admin,
    verify_assistant_of, verify_user_jwt,
};
pub use sealed::{SEALED_VERSION, Sealed, SealedBody, SealedKey};
pub use sense::{SENSE_STREAM, SENSE_SUBJECTS, SenseId};
pub use shared::{
    INCARNATION_KEY, SETTINGS_SIGNING_CONTEXT, SHARED_FIELDS, SettingsError, SharedSettings, incarnation,
    is_shared_field,
};
pub use status::{DecidedBy, MAX_TOOL_PATHS, PlanStep, State, StatusEvent, StatusUpdate, StepStage};
pub use text::{lines, one_line, shown};
pub use threads::{
    Archived, Mark, Reminder, Reminders, SNOOZE_MAX, SealedTitle, Snooze, THREAD_UNDONE, ThreadDone, ThreadMarks,
    ThreadSnoozes, ThreadSummary, ThreadTitle,
};
pub use timeline::{
    CallCount, MAX_TIMELINE_CALLS, MAX_TIMELINE_ENTRIES, MAX_TIMELINE_PATH, MAX_TIMELINE_STEPS, MAX_TIMELINE_TITLE,
    MergedCall, MergedEnd, MergedNoReply, MergedRejection, NoReplyWhy, PromptEnd, TIMELINE_EVERY, Timeline,
    TimelineCall, TimelineEnd, TimelineEntry, TimelineRejection, Timelines,
};
pub use trails::{AgentArgs, GotoArgs, RunLimit, TrailArgs, Unresumable};
pub use trails::{
    CallAnswer, JournalEntry, MAX_STEP_LABEL, MAX_TRAIL_ARGS, MAX_TRAIL_DESCRIPTION, MAX_TRAIL_PHASES,
    MAX_TRAIL_SCRIPT, MAX_TRAIL_WAYPOINTS, PendingCall, QueuedCall, RunState, SavedTrail, Saver, SpendsFrom,
    StepBounds, StepGrant, TRAIL_CLOCK_SKEW, TRAIL_RUN_RETENTION, TrailAction, TrailAnswering, TrailFunction, TrailLeg,
    TrailLimits, TrailOutcome, TrailParent, TrailReply, TrailRequest, TrailRun, TrailRunIndex, TrailSession,
    TrailSource, TrailSpent, TrailStart, TrailStep, TrailStepThread, WaitingStep, leg_entry,
};
pub use trails::{
    Counts, FIND_TRAILS_LIMIT, Finder, Found, LATEST_RATINGS, MAX_FIND_TRAILS, MAX_RATING_TASK, MAX_RATING_WHY, Query,
    RATING_FULL_WEIGHT, RATING_MATCH_WORDS, RatingOutcome, RatingSummary, TrailRating, serves, stem,
};
pub use trails::{
    DEFAULT_TIPS_FOUND, MAX_LIVE_TIPS, MAX_TIP_REASON, MAX_TIP_TEXT, MAX_TIPS_BLOCK, MAX_TIPS_CARRIED, MAX_TIPS_FOUND,
    Ranked, Retirement, Similarity, TIP_HALF_LIFE, TIP_RELEVANCE, TIP_RELEVANT_WORDS, TIP_SIMILARITY, TIP_UNUSED,
    Tally, TipBoard, TipRetired, TipStatus, TipVerdict, TipVote, TrailTip, check_tip_key, script_sha256, search_words,
    tip_words,
};
pub use trails::{SAVE_TRAIL_REPEAT, SAVE_TRAIL_WAIT, SaveTrailOutcome, SaveTrailReply, SaveTrailRequest, TrailChange};

/// JetStream stream holding every durable ants message.
pub const STREAM: &str = "ANTS";
/// Subjects captured by [`STREAM`]. Status updates are core NATS only and stay out of it.
pub const STREAM_SUBJECTS: [&str; 7] = [
    "ants.inbox.>",
    "ants.notes.>",
    "ants.approvals.>",
    "ants.dead.>",
    "ants.chan.>",
    subject::ALL_TIMELINE,
    subject::ALL_ADMIN_ORDERS,
];
/// The inbox subjects [`STREAM`] republishes each message it stores on, to [`STORED_INBOX_DESTINATION`]: what it
/// sources from a leaf's outbox it stores without publishing it on its subject.
pub const STORED_INBOX_SOURCE: &str = "ants.inbox.*.>";
/// Where [`STREAM`] republishes a stored inbox message: the recipient's reply inboxes ([`subject::stored_inbox`]), which
/// its credentials already read and no credentials publish to. The headers keep the original subject. The server also
/// answers a direct get, and delivers a push consumer's messages, there for any client that names the subject, with
/// headers that client stored: only a message without a reply subject, and with a single `Nats-Stream` and a single
/// `Nats-Subject` header, the stream's own, is one it republished.
pub const STORED_INBOX_DESTINATION: &str = "_INBOX.{{wildcard(1)}}.stored.>";
/// JetStream stream on a machine's leaf node (in the leaf's own JetStream domain, the machine name) that keeps what
/// the machine's clients send while the hub's [`STREAM`] is out of reach, under [`subject::outbox`] subjects.
pub const OUTBOX_STREAM: &str = "ANTS_OUT";
/// Subjects captured by [`OUTBOX_STREAM`]. The leaf's uplink credentials never carry them to the hub.
pub const OUTBOX_SUBJECTS: &str = "ants.out.>";
/// Hub stream every leaf's [`OUTBOX_STREAM`] is sourced into, each under its machine's prefix
/// ([`subject::intake`]), whatever subjects the leaf claims. [`STREAM`] sources from it only the subjects the users of
/// each machine may publish ([`Machine::admitted`]).
pub const INTAKE_STREAM: &str = "ANTS_IN";
/// Durable consumer on a leaf's [`OUTBOX_STREAM`] through which the hub's [`INTAKE_STREAM`] sources it, delivering on
/// [`Machine::outbox_delivery`]: nats-server resumes such a source as soon as the leaf node delivers again.
pub const OUTBOX_CONSUMER: &str = "ANTS_IN";
/// KV bucket where each daemon registers its ants: one [`ColonyEntry`] per ant, keyed by its id.
pub const COLONY_BUCKET: &str = "ants_colony";
/// How often a daemon re-puts its colony entry.
pub const COLONY_HEARTBEAT: Duration = Duration::from_secs(10);
/// Max age of the colony bucket: an entry not refreshed for this long is gone, so an ant whose daemon died drops
/// out of the colony after at most a few missed heartbeats.
pub const COLONY_TTL: Duration = Duration::from_secs(30);
/// KV bucket of the machines: one [`MachineRecord`] per machine running a daemon, keyed by its name, listing the ants
/// the daemon serves. Unlike the colony's, records never expire, so an ant whose daemon stopped still has a machine,
/// where admin orders to it wait ([`subject::admin_orders`]). Each machine's daemon writes only its own record, with
/// the machine's host credentials ([`Permissions::for_host`]); humans read them all; ants read none. Beside its record,
/// under keys starting with its name, the machine's daemon records the outcome of each admin order to its ants
/// ([`admin_outcome_key`]).
pub const MACHINES_BUCKET: &str = "ants_machines";
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
/// Most characters a channel's description ([`ChannelEntry::description`]) has.
pub const MAX_CHANNEL_DESCRIPTION: usize = 1000;
/// KV bucket of the humans: one [`HumanEntry`] per human, keyed by its user id. Only humans write it; ants read
/// it. [`UserId::human`] is a human without an entry.
pub const HUMANS_BUCKET: &str = "ants_humans";
/// KV bucket of the escalation chains: one [`ChainEntry`] per ant, keyed by its id, put by its daemon on
/// start. Unlike the colony's, entries never expire, so a chain can be followed past an ant that is offline. Each
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
/// ([`owner_outbox`]). Only humans and webhooks write it, a webhook for its own ant destinations only, and assistants,
/// each only for its human, under keys naming that human ([`assisted_owner_key`], never outboxed); every credential
/// reads it by key, and the first record of a thread to an ant stays: its stream refuses a second record of a key,
/// drops one carrying the first's [`owner_record_id`] within its retention, so none sourced from an outbox is refused,
/// and denies deletes and purges. Daemons take the owner of an ant's message on a thread no channel post started from
/// the record to that ant, or to an ant that passed the link to the thread on to it. Records expire after the
/// stream's retention.
pub const OWNERS_BUCKET: &str = "ants_owners";
/// KV bucket of the events each webhook delivered, keyed `<webhook>.<hash of the event id>` ([`webhook_seen_key`]),
/// so a retried event is not delivered again, of each webhook's marker ([`webhook_marker_key`]), the version
/// it runs, and of the channel manager's [`Serving`] ([`channel_manager_serving_key`]). Each webhook writes and reads
/// only its own keys, ants read nothing of it, humans the markers and the channel manager's key; what tells a webhook
/// from an ant is the subject of its messages ([`subject::webhook_inbox`]), never this bucket; entries expire after [`WEBHOOK_RETENTION`].
pub const WEBHOOKS_BUCKET: &str = "ants_webhooks";
/// How long [`WEBHOOKS_BUCKET`] remembers an event.
pub const WEBHOOK_RETENTION: Duration = Duration::from_hours(7 * 24);
/// KV bucket of each human's [`ReadMarker`], keyed by its user id: which inbox messages the human has read, on any
/// of its devices; and of its [`ChannelReadMarkers`], keyed by [`channel_reads_key`]: which channel posts it has read.
/// Each human reads and writes only its own keys ([`Permissions::for_user`]); an ant's daemon reads a human's
/// [`ReadMarker`] alone, by key, to stop reminding it of an escalation it read. Entries never expire.
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
/// ([`Permissions::for_user`]); an ant's daemon reads a human's [`NotifyPrefs`] alone, by key, to stop reminding it of
/// an escalation on a thread it muted. Entries never expire, what they hold ends at the deadlines it carries.
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
/// one for every human too, which any human sets or clears, with each marking of a thread done under a key of its own
/// ([`done_mark_key`]) for the ants' daemons to hear of, and each human's pinned and archived threads
/// ([`ThreadMarks`], keyed by [`marks_key`]) and its due times, its reminders and the threads it snoozed on home
/// ([`Reminders`], keyed by [`reminders_key`]), each shared by its devices and read and written by that human alone
/// ([`Permissions::for_user`]), with the marks and snoozes its assistant sets for it under keys naming the assistant
/// ([`assisted_marks_key`], [`assisted_reminders_key`]), and the TL;DR of each thread a human's CLI asked an ant for ([`ThreadSummary`], keyed
/// by [`summary_key`]), which any human puts or deletes. Ants write none of it and read, by key, only titles, done
/// states and a human's own reminders entry, the last two to stop reminding a human of an escalation: a title an ant suggests
/// ([`Envelope::title`], [`ChannelPost::title`], [`NotesPost::title`]) and a TL;DR an ant replies with
/// ([`Envelope::summary`]) get there through a human's CLI. Entries never expire.
pub const THREADS_BUCKET: &str = "ants_threads";
/// KV bucket of the ants' identities: one [`AntIdentity`] per ant, keyed by its name ([`subject::identity`]), what
/// makes it the same ant on every machine, its avatar included. Its daemon creates it on start only when absent, with a
/// KV create, so the first creator wins, and the daemon serving the ant counts its completed prompts in it by
/// compare-and-set. Each ant writes only its own key; humans read all and set an ant's avatar and colour; the
/// short-lived credentials of `ants auth revoke` delete the record of an ant revoked whole
/// ([`Permissions::for_device_revoker`]). Beside it, under `<ant>.settings`, the ant's [`SharedSettings`], which only
/// host credentials write ([`Permissions::for_host`]). Entries never expire.
pub const IDENTITIES_BUCKET: &str = "ants_identities";
/// KV bucket of the saved trails: one [`SavedTrail`] per trail, keyed by its name. Humans put and delete any. An ant
/// saves new ones and replaces or removes only those it saved ([`Saver`]) by asking `ants signer`
/// ([`SaveTrailRequest`]), whose credentials alone write it for ants ([`Permissions::for_trail_keeper`]): no subject
/// ties a name to its saver, so ant credentials, which a harness can read, write none of it. Every ant and human reads
/// them, by direct gets naming no subject ([`Permissions::for_user`]). Entries never expire.
pub const TRAILS_BUCKET: &str = "ants_trails";
/// KV bucket of the trail runs, each under its conductor's keys: its [`TrailRun`] ([`trail_run_key`]), its
/// [`TrailSession`] ([`trail_session_key`]), the [`JournalEntry`] of each call answered ([`trail_call_key`]), and the
/// conductor's [`TrailRunIndex`] ([`trail_runs_index_key`]) and the [`TrailStepThread`] of each step's thread
/// ([`trail_step_key`]), kept again under the thread of the step's run ([`trail_thread_step_key`]). Each ant reads and
/// writes only its own keys
/// ([`subject::trail_runs`]), by direct gets naming them; humans read all, by direct gets naming no subject, and write
/// none: a run is started, stopped and resumed by its conductor. Entries expire [`TRAIL_RUN_RETENTION`] and a run's most
/// wall time ([`TrailLimits::MAX`]) after their last put, so a run's entries outlive its end by
/// [`TRAIL_RUN_RETENTION`] at least.
pub const TRAIL_RUNS_BUCKET: &str = "ants_trail_runs";
/// KV bucket of the trail tips: what an ant answering a step of a saved trail learnt, for the ants answering its later
/// steps. Each [`TrailTip`] is keyed by its trail, writer and id ([`trail_tip_key`]), and each ant's [`TipVote`] on it
/// by the trail, the voter, then the tip's writer and id ([`trail_tip_vote_key`]), so a trail's tips and votes are the
/// keys under `<trail>.>`, and an ant's own under `<trail>.<ant>.>`, which is all its credentials let it write here
/// (`$KV.ants_trail_tips.*.<ant>.>`, one subject per ant). Each ant creates only its own tips and puts only its own
/// votes, voting again replacing its vote; a human retires a tip under a key no ant writes ([`trail_tip_retired_key`])
/// and purges a trail's tips when removing the trail. Every ant and human reads them all, by direct gets naming no subject
/// ([`Permissions::for_user`]), which a reader ranks with a [`TipBoard`]. Entries never expire.
pub const TRAIL_TIPS_BUCKET: &str = "ants_trail_tips";
/// KV bucket of the trail ratings: each conductor's judgement of whether a run of a saved trail served the task it was
/// run for, which a search for trails ranks them by ([`Finder`]). Each [`TrailRating`] is keyed by its trail, conductor
/// and run ([`trail_rating_key`]), so an ant puts only keys naming itself (`$KV.ants_trail_ratings.*.<ant>.*`, one
/// subject per ant), rating a run again replacing its rating. Every ant and human reads them all, by direct gets naming
/// no subject ([`Permissions::for_user`]); humans purge a trail's ratings when removing the trail. Entries never
/// expire.
pub const TRAIL_RATINGS_BUCKET: &str = "ants_trail_ratings";
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
/// whose messages carry the narrowing on, ask a human a question, end a turn with no reply, search a trail's tips,
/// find saved trails for a task, or rate a run the thread reports on (which the ants MCP server allows only there).
pub const WEBHOOK_ALLOW: [&str; 19] = [
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
    "other:complete",
    "other:ask_human",
    "other:trail_tips",
    "other:find_trails",
    "other:rate_trail",
];
/// The tool rules the review thread of a dream is narrowed to on the sage's side (requirement ants-x39hk.12.4): reading
/// files, searching, thinking, and the ants tools that only read; the ants MCP server refuses the others on it, the
/// sage's daemon rejects its harness's requests for them unasked, and the sage answers with its reply alone. Running
/// commands stays out: no rule tells a read-only command apart.
pub const REVIEW_ALLOW: [&str; 8] = [
    "read:*",
    "search",
    "think",
    "other:list_ants",
    "other:read_notes",
    "other:read_attachment",
    "other:list_reactions",
    "other:suggest_thread_title",
];

/// Whether `allow` narrows a thread to a review ([`REVIEW_ALLOW`]).
pub fn is_review(allow: Option<&[String]>) -> bool {
    allow.is_some_and(|allow| allow.iter().map(String::as_str).eq(REVIEW_ALLOW))
}
/// Chain levels an escalation may climb before it goes to the thread's owner ([`Escalation::up`]).
pub const DEFAULT_MAX_ESCALATION_HOPS: u32 = 3;

#[cfg(test)]
fn p(s: &str) -> UserId {
    s.parse().unwrap()
}
