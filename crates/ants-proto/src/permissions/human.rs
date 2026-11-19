//! The rights a human's credentials hold that an ant's do not, by the streams and buckets they reach, which
//! [`Permissions::for_user`] gives a human and of which [`Permissions::for_assistant`] gives an assistant some.

use super::{ack_subjects, kv_stream, ordered_consumer_subjects};
use crate::{
    ATTEMPTS_BUCKET, CHAINS_BUCKET, CHANNELS_BUCKET, COLONY_BUCKET, DEVICES_BUCKET, HUMANS_BUCKET, IDENTITIES_BUCKET,
    INTAKE_STREAM, LINKS_BUCKET, MACHINES_BUCKET, NOTIFY_BUCKET, OUTBOX_CONSUMER, OUTBOX_STREAM, OWNERS_BUCKET,
    PRESENCE_BUCKET, REACTIONS_BUCKET, READ_BUCKET, SESSIONS_BUCKET, STREAM, THREADS_BUCKET, TRAIL_RATINGS_BUCKET,
    TRAIL_RUNS_BUCKET, TRAIL_TIPS_BUCKET, TRAILS_BUCKET, UserId, WEBHOOKS_BUCKET, channel_manager_serving_subject,
    notify_consumer, subject,
};
#[cfg(doc)]
use {
    super::Permissions,
    crate::{
        ChannelReadMarkers, HeldNotices, NotifyPrefs, ThreadSummary, assisted_marks_key, assisted_reminders_key,
        marks_key, reminders_key,
    },
};

/// What a human's credentials need of [`MACHINES_BUCKET`]: looking it up and creating it, and reading every machine's
/// record by batched direct gets naming no subject (`ants machines`), writing none. No consumer, as a human's JWT has
/// little room left.
pub(super) fn machine_record_rights() -> [String; 3] {
    let machines = kv_stream(MACHINES_BUCKET);
    [
        format!("$JS.API.STREAM.INFO.{machines}"),
        format!("$JS.API.STREAM.CREATE.{machines}"),
        format!("$JS.API.DIRECT.GET.{machines}"),
    ]
}

/// What a human's credentials need of [`IDENTITIES_BUCKET`]: creating it, reading every ant's identity by batched
/// direct gets naming no subject, and setting an ant's avatar and colour by compare-and-set on its key. No stream info
/// or consumer, as a human's JWT has little room left.
pub(super) fn identity_rights() -> [String; 3] {
    let identities = kv_stream(IDENTITIES_BUCKET);
    [
        format!("$JS.API.STREAM.CREATE.{identities}"),
        format!("$JS.API.DIRECT.GET.{identities}"),
        format!("$KV.{IDENTITIES_BUCKET}.*"),
    ]
}

/// What a human's credentials need of the trails: creating and looking up [`TRAILS_BUCKET`], [`TRAIL_RUNS_BUCKET`],
/// [`TRAIL_TIPS_BUCKET`] and [`TRAIL_RATINGS_BUCKET`], whose stream info lists the saved trails, every conductor's
/// runs, the trails' tips and their ratings,
/// putting and deleting every saved trail, which every user reads, and reading every run's records by direct gets
/// naming no subject (`ants trail runs`, and in `ants chat` the steps a conductor keeps under one thread), writing
/// none: a run's conductor alone writes them, and stops or resumes a run on a human's request. Of the tips, which
/// every user reads, it retires any under the tip's retirement key, which no ant writes, and purges a removed trail's. Of the ratings, which every user
/// reads and only their conductors write, it purges a removed trail's.
pub(super) fn trail_rights() -> [String; 13] {
    let (trails, runs, tips) = (kv_stream(TRAILS_BUCKET), kv_stream(TRAIL_RUNS_BUCKET), kv_stream(TRAIL_TIPS_BUCKET));
    let ratings = kv_stream(TRAIL_RATINGS_BUCKET);
    [
        format!("$JS.API.STREAM.CREATE.{trails}"),
        format!("$JS.API.STREAM.INFO.{trails}"),
        format!("$KV.{TRAILS_BUCKET}.*"),
        format!("$JS.API.STREAM.CREATE.{runs}"),
        format!("$JS.API.STREAM.INFO.{runs}"),
        format!("$JS.API.DIRECT.GET.{runs}"),
        format!("$JS.API.STREAM.CREATE.{tips}"),
        format!("$JS.API.STREAM.INFO.{tips}"),
        format!("$KV.{TRAIL_TIPS_BUCKET}.>"),
        format!("$JS.API.STREAM.PURGE.{tips}"),
        format!("$JS.API.STREAM.CREATE.{ratings}"),
        format!("$JS.API.STREAM.INFO.{ratings}"),
        format!("$JS.API.STREAM.PURGE.{ratings}"),
    ]
}

/// What a human's credentials need for what it read ([`READ_BUCKET`]): creating the bucket and reading and writing its
/// own [`ReadMarker`] and [`ChannelReadMarkers`] only, by key, so no human reads or moves another's.
pub(super) fn read_marker_rights(human: &UserId) -> [String; 6] {
    let read = kv_stream(READ_BUCKET);
    let marker = subject::read_marker(human);
    let channels = subject::channel_reads(human);
    [
        format!("$JS.API.STREAM.INFO.{read}"),
        format!("$JS.API.STREAM.CREATE.{read}"),
        format!("$JS.API.DIRECT.GET.{read}.{marker}"),
        marker,
        format!("$JS.API.DIRECT.GET.{read}.{channels}"),
        channels,
    ]
}

/// What a human's credentials need to say where its devices read ([`PRESENCE_BUCKET`]): creating the bucket, writing
/// its own keys only, so no human moves where another reads, and reading every key, by direct gets and through the
/// ordered consumers behind listing and watching them, so a CLI shows which humans have a device following.
pub(super) fn presence_rights(human: &UserId) -> Vec<String> {
    let presence = kv_stream(PRESENCE_BUCKET);
    let mut rights = vec![
        format!("$JS.API.STREAM.CREATE.{presence}"),
        format!("$JS.API.DIRECT.GET.{presence}.>"),
        subject::presence_of(human),
    ];
    rights.extend(ordered_consumer_subjects(&presence));
    rights
}

/// What a human's credentials need for the directory of human devices' encryption keys ([`DEVICES_BUCKET`]): creating
/// the bucket; reading all of it, singly and through the ordered consumers behind listing its keys, since a sender
/// seals for every device of every human it writes to; and putting its own devices' entries only
/// ([`subject::devices_of`]), so no human puts a key for another's device. The revocation records
/// ([`subject::revoked_devices`]) only [`Permissions::for_device_revoker`] puts.
pub(super) fn device_directory_rights(human: &UserId) -> Vec<String> {
    let devices = kv_stream(DEVICES_BUCKET);
    let mut rights = vec![
        format!("$JS.API.STREAM.INFO.{devices}"),
        format!("$JS.API.STREAM.CREATE.{devices}"),
        format!("$JS.API.DIRECT.GET.{devices}.>"),
        subject::devices_of(human),
    ];
    rights.extend(ordered_consumer_subjects(&devices));
    rights
}

/// What a human's credentials need to hold back its notifications ([`NOTIFY_BUCKET`]): creating the bucket and reading
/// and writing its own [`NotifyPrefs`] and [`HeldNotices`] only, by key, so no human snoozes or mutes another's;
/// `ants webhook serve` reads them, and counts and claims the held notices, with the human's own credentials.
pub(super) fn notify_prefs_rights(human: &UserId) -> [String; 6] {
    let notify = kv_stream(NOTIFY_BUCKET);
    let prefs = subject::notify_prefs(human);
    let held = subject::notify_held(human);
    [
        format!("$JS.API.STREAM.INFO.{notify}"),
        format!("$JS.API.STREAM.CREATE.{notify}"),
        format!("$JS.API.DIRECT.GET.{notify}.{prefs}"),
        prefs,
        format!("$JS.API.DIRECT.GET.{notify}.{held}"),
        held,
    ]
}

/// What a human's credentials need for reactions ([`REACTIONS_BUCKET`]): putting, deleting and reading back by direct
/// gets its own keys only, so no human forges or removes another's reaction; creating the bucket; and reading every
/// reaction, singly and through the ordered consumers behind listing and watching them. An ant neither reads nor
/// writes the bucket: subjects cannot tie a message id to the threads and channels whose messages it may read. It asks
/// instead, only as itself, for the reactions to a message ([`subject::reactions`]), which a human's
/// `ants webhook serve` or `ants signer` answers, and to add or remove its own ([`subject::react`]), which only
/// `ants signer` does ([`Permissions::for_reactions_responder`]); each on the ant's own [`subject::reactions_reply`],
/// once it found the ant reads the message.
pub(super) fn human_reaction_rights(human: &UserId) -> Vec<String> {
    let reactions = kv_stream(REACTIONS_BUCKET);
    let own = format!("$KV.{REACTIONS_BUCKET}.{human}.>");
    let mut rights = vec![
        own,
        format!("$JS.API.STREAM.INFO.{reactions}"),
        format!("$JS.API.STREAM.CREATE.{reactions}"),
        format!("$JS.API.DIRECT.GET.{reactions}.>"),
    ];
    rights.extend(ordered_consumer_subjects(&reactions));
    rights
}

/// What a human's credentials need for thread titles, TL;DRs and done states and its pins and archives
/// ([`THREADS_BUCKET`]): creating the bucket; reading by direct gets, putting and deleting every title, every TL;DR
/// ([`ThreadSummary`]) and every done state, which any human sets; and reading and writing its own [`ThreadMarks`]
/// and [`crate::Reminders`] only, so no human reads or changes another's, with those its assistants set for it
/// ([`assisted_marks_key`], [`assisted_reminders_key`]), which it clears as it marks a thread itself. The keys naming
/// the human second ([`marks_key`], [`reminders_key`]) or second of three are all its own, and one pattern each covers
/// them, to keep the credentials short: no other key of the bucket has a user id there.
pub(super) fn thread_rights(human: &UserId) -> [String; 12] {
    let threads = kv_stream(THREADS_BUCKET);
    let titles = format!("$KV.{THREADS_BUCKET}.title.*");
    let summaries = format!("$KV.{THREADS_BUCKET}.summary.*");
    let done = format!("$KV.{THREADS_BUCKET}.done.*");
    let own = format!("$KV.{THREADS_BUCKET}.*.{human}");
    let assisted = format!("{own}.*");
    [
        format!("$JS.API.STREAM.INFO.{threads}"),
        format!("$JS.API.STREAM.CREATE.{threads}"),
        format!("$JS.API.DIRECT.GET.{threads}.{titles}"),
        titles,
        format!("$JS.API.DIRECT.GET.{threads}.{summaries}"),
        summaries,
        format!("$JS.API.DIRECT.GET.{threads}.{done}"),
        done,
        format!("$JS.API.DIRECT.GET.{threads}.{own}"),
        own,
        format!("$JS.API.DIRECT.GET.{threads}.{assisted}"),
        assisted,
    ]
}

/// A human's renewals, the stream's info, and the consumer of its notifications.
pub(super) fn human_notify_rights(human: &UserId) -> Vec<String> {
    let notify = notify_consumer(human);
    let mut publish = vec![
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
    ];
    publish.extend(ack_subjects(STREAM, &notify));
    publish
}

/// The streams and buckets a human sets up, and what of them it reads and writes that an ant may not.
pub(super) fn human_store_rights(human: &UserId) -> [String; 47] {
    let colony_stream = kv_stream(COLONY_BUCKET);
    let channels = kv_stream(CHANNELS_BUCKET);
    let humans = kv_stream(HUMANS_BUCKET);
    let chains = kv_stream(CHAINS_BUCKET);
    let links = kv_stream(LINKS_BUCKET);
    let owners = kv_stream(OWNERS_BUCKET);
    let webhooks = kv_stream(WEBHOOKS_BUCKET);
    let sessions = kv_stream(SESSIONS_BUCKET);
    let attempts = kv_stream(ATTEMPTS_BUCKET);
    [
        format!("$JS.API.STREAM.CREATE.{STREAM}"),
        // Creating a KV bucket first looks up the account's JetStream limits.
        "$JS.API.INFO".to_owned(),
        format!("$JS.API.STREAM.CREATE.{colony_stream}"),
        // Room for the consumers of every daemon watching the bucket, which a KV bucket gets only by an update
        // right after creating it.
        format!("$JS.API.STREAM.UPDATE.{colony_stream}"),
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
        // Put and delete of every channel's and every human's entry, and of the keys under its own name, such
        // as its assistant grant, which no one else writes.
        format!("$KV.{CHANNELS_BUCKET}.>"),
        format!("$KV.{HUMANS_BUCKET}.*"),
        format!("$KV.{HUMANS_BUCKET}.{human}.>"),
        // Recording the owner of each thread it messages an ant on, and the same in the leaf's outbox.
        format!("$KV.{OWNERS_BUCKET}.*.*"),
        "ants.out.owners.*.*".to_owned(),
        // Sourcing leaf outboxes into the stream (`ants leaf outbox`).
        format!("$JS.API.STREAM.UPDATE.{STREAM}"),
        format!("$JS.API.STREAM.MSG.GET.{STREAM}"),
        // Reading the stream back in batches; it reads nothing more than STREAM.MSG.GET, which takes any
        // sequence.
        format!("$JS.API.DIRECT.GET.{STREAM}"),
        format!("$JS.API.STREAM.MSG.DELETE.{STREAM}"),
        subject::ALL_APPROVAL_DECISIONS.to_owned(),
        subject::ALL_CANCELS.to_owned(),
        subject::ALL_QUEUE_REQUESTS.to_owned(),
        // Starting, stopping and resuming an ant's trail runs.
        subject::ALL_TRAIL_REQUESTS.to_owned(),
        // Sending admin orders, which only an admin's signature makes a daemon carry out.
        subject::ALL_ADMIN_ORDERS_SENT.to_owned(),
        // Answering an ant's request for the reactions to a message it reads.
        subject::ALL_REACTIONS_REPLIES.to_owned(),
        // Withdrawing a message from an ant's queue: creating its mark, and reading the mark that won.
        format!("$KV.{ATTEMPTS_BUCKET}.*.msg.*"),
        format!("$JS.API.DIRECT.GET.{attempts}.$KV.{ATTEMPTS_BUCKET}.*.msg.*"),
        // Its own markers of the messages it was told keep waiting for an ant no daemon serves.
        format!("$KV.{ATTEMPTS_BUCKET}.{human}.unserved.*"),
        format!("$JS.API.DIRECT.GET.{attempts}.$KV.{ATTEMPTS_BUCKET}.{human}.unserved.*"),
        subject::DEAD_ALL.to_owned(),
        // Setting up the store and forward of leaf nodes: the hub's intake, and each leaf's outbox through the
        // leaf's own domain (the plain form is what a leaf maps its own domain to).
        format!("$JS.API.STREAM.CREATE.{INTAKE_STREAM}"),
        format!("$JS.API.STREAM.UPDATE.{INTAKE_STREAM}"),
        format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"),
        // Where the intake is in an outbox, to start the outbox's durable consumer after it.
        format!("$JS.API.STREAM.MSG.GET.{INTAKE_STREAM}"),
        // And the links and owners buckets' sources from the intake.
        format!("$JS.API.STREAM.UPDATE.{links}"),
        format!("$JS.API.STREAM.UPDATE.{owners}"),
    ]
}

/// Setting up a leaf's outbox, through the hub's domain or the leaf's own.
pub(super) fn outbox_setup_rights() -> Vec<String> {
    let mut publish = Vec::new();
    for op in ["CREATE", "UPDATE", "INFO"] {
        publish.push(format!("$JS.API.STREAM.{op}.{OUTBOX_STREAM}"));
        publish.push(format!("$JS.*.API.STREAM.{op}.{OUTBOX_STREAM}"));
    }
    // The outbox's durable consumer: the uplink carries no more of it than its creation, lookup, reset and deletion
    // ([`Permissions::for_leaf`]); a leaf node maps its own domain to the plain form for a CLI connected to it, and
    // checks both forms.
    publish.push(format!("$JS.*.API.CONSUMER.*.{OUTBOX_STREAM}.{OUTBOX_CONSUMER}"));
    publish.push(format!("$JS.API.CONSUMER.*.{OUTBOX_STREAM}.{OUTBOX_CONSUMER}"));
    publish
}
