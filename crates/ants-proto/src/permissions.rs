use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    ATTEMPTS_BUCKET, CHAINS_BUCKET, CHANNELS_BUCKET, ChannelId, DEVICES_BUCKET, ENROLL_INBOX, FILES_BUCKET,
    FILES_STREAM, Feed, HUMANS_BUCKET, INTAKE_STREAM, JsDomain, LINKS_BUCKET, MACHINES_BUCKET, Machine, NOTIFY_BUCKET,
    OUTBOX_STREAM, OWNERS_BUCKET, PRESENCE_BUCKET, REACTIONS_BUCKET, READ_BUCKET, ROSTER_BUCKET, Role, SESSIONS_BUCKET,
    STORAGE_WARNING_PREFIX, STREAM, THREADS_BUCKET, UserId, WEBHOOK_MARKER, WEBHOOKS_BUCKET, admin_consumer,
    chan_consumer, chan_reader, channel_manager_serving_subject, dead_reader, host_inbox_prefix, inbox_consumer,
    inbox_prefix, marks_key, notes_consumer, notify_consumer, sent_reader, snoozed_key, spare_readers, subject,
};
#[cfg(doc)]
use crate::{
    AntRequest, AssistantOf, AttachmentHandle, ChannelReadMarkers, EnrollRequest, HeldNotices, NotifyPrefs,
    QueueRequest, ReactionsRequest, RenewRequest, StatusUpdate, ThreadSummary, channel_manager_serving_key,
    daily_spend_key, link_outbox, message_mark_key, owner_outbox, storage_warning_key, thread_spend_key, unserved_key,
};

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

/// What a human's credentials need of [`MACHINES_BUCKET`]: looking it up and creating it, and reading every machine's
/// record by batched direct gets naming no subject (`ants machines`), writing none. No consumer, as a human's JWT has
/// little room left.
fn machine_record_rights() -> [String; 3] {
    let machines = kv_stream(MACHINES_BUCKET);
    [
        format!("$JS.API.STREAM.INFO.{machines}"),
        format!("$JS.API.STREAM.CREATE.{machines}"),
        format!("$JS.API.DIRECT.GET.{machines}"),
    ]
}

/// What a human's credentials need for what it read ([`READ_BUCKET`]): creating the bucket and reading and writing its
/// own [`ReadMarker`] and [`ChannelReadMarkers`] only, by key, so no human reads or moves another's.
fn read_marker_rights(human: &UserId) -> [String; 6] {
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
fn presence_rights(human: &UserId) -> Vec<String> {
    let presence = kv_stream(PRESENCE_BUCKET);
    let mut rights = vec![
        format!("$JS.API.STREAM.CREATE.{presence}"),
        format!("$JS.API.DIRECT.GET.{presence}.>"),
        format!("$JS.API.CONSUMER.CREATE.{presence}"),
        subject::presence_of(human),
    ];
    rights.extend(flow_control_subjects("*", &presence));
    rights
}

/// What a human's credentials need for the directory of human devices' encryption keys ([`DEVICES_BUCKET`]): creating
/// the bucket; reading all of it, singly and through the ordered consumers behind listing its keys, since a sender
/// seals for every device of every human it writes to; and putting its own devices' entries only
/// ([`subject::devices_of`]), so no human puts a key for another's device. The revocation records
/// ([`subject::revoked_devices`]) only [`Permissions::for_device_revoker`] puts.
fn device_directory_rights(human: &UserId) -> Vec<String> {
    let devices = kv_stream(DEVICES_BUCKET);
    let mut rights = vec![
        format!("$JS.API.STREAM.INFO.{devices}"),
        format!("$JS.API.STREAM.CREATE.{devices}"),
        format!("$JS.API.DIRECT.GET.{devices}.>"),
        format!("$JS.API.CONSUMER.CREATE.{devices}"),
        subject::devices_of(human),
    ];
    rights.extend(flow_control_subjects("*", &devices));
    rights
}

/// What a human's credentials need to hold back its notifications ([`NOTIFY_BUCKET`]): creating the bucket and reading
/// and writing its own [`NotifyPrefs`] and [`HeldNotices`] only, by key, so no human snoozes or mutes another's;
/// `ants webhook serve` reads them, and counts and claims the held notices, with the human's own credentials.
fn notify_prefs_rights(human: &UserId) -> [String; 6] {
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
fn human_reaction_rights(human: &UserId) -> Vec<String> {
    let reactions = kv_stream(REACTIONS_BUCKET);
    let own = format!("$KV.{REACTIONS_BUCKET}.{human}.>");
    let mut rights = vec![
        format!("$JS.API.DIRECT.GET.{reactions}.{own}"),
        own,
        format!("$JS.API.STREAM.INFO.{reactions}"),
        format!("$JS.API.STREAM.CREATE.{reactions}"),
        format!("$JS.API.DIRECT.GET.{reactions}.>"),
        format!("$JS.API.CONSUMER.CREATE.{reactions}"),
    ];
    rights.extend(flow_control_subjects("*", &reactions));
    rights
}

/// What a user's credentials need for attachments ([`FILES_BUCKET`]): writing the chunks of those of the messages it
/// sends only, with itself as the sender of their handles ([`AttachmentHandle`]), one subject for every kind of message
/// to keep leaf uplink credentials small, a human's with any, as it sends under any sender to replay dead letters as
/// they were; each also in its outbox form ([`AttachmentHandle::outbox_chunk_subject`]). A human reads every chunk by
/// direct gets and may create the store, look it up and update it, to source leaf outboxes into it. An ant reads by
/// direct gets naming their subject, as [`subject::readable_by`] says it reads the messages carrying them, the chunks
/// of the attachments sent to it and by it, and of every notes and channel post: no stream info, which lists every
/// subject stored, and no consumer, so none of the attachments between other users.
fn attachment_rights(user: &UserId, role: Role) -> Vec<String> {
    let chunks = format!("$O.{FILES_BUCKET}.C");
    let get = format!("$JS.API.DIRECT.GET.{FILES_STREAM}");
    if role.is_human() {
        return vec![
            format!("{chunks}.>"),
            "ants.out.files.>".to_owned(),
            format!("$JS.API.STREAM.INFO.{FILES_STREAM}"),
            format!("$JS.API.STREAM.CREATE.{FILES_STREAM}"),
            format!("$JS.API.STREAM.UPDATE.{FILES_STREAM}"),
            format!("{get}.{chunks}.>"),
        ];
    }
    vec![
        format!("{chunks}.{user}.>"),
        format!("ants.out.files.{user}.>"),
        format!("{get}.{chunks}.*.inbox.{user}.>"),
        format!("{get}.{chunks}.{user}.inbox.>"),
        format!("{get}.{chunks}.*.notes.>"),
        format!("{get}.{chunks}.*.chan.>"),
    ]
}

/// What a human's credentials need for thread titles, TL;DRs and done states and its pins and archives
/// ([`THREADS_BUCKET`]): creating the bucket; reading by direct gets, putting and deleting every title, every TL;DR
/// ([`ThreadSummary`]) and every done state, which any human sets; and reading and writing its own [`ThreadMarks`]
/// only, so no human reads or changes another's.
fn thread_rights(human: &UserId) -> [String; 12] {
    let threads = kv_stream(THREADS_BUCKET);
    let titles = format!("$KV.{THREADS_BUCKET}.title.*");
    let summaries = format!("$KV.{THREADS_BUCKET}.summary.*");
    let done = format!("$KV.{THREADS_BUCKET}.done.*");
    let marks = format!("$KV.{THREADS_BUCKET}.{}", marks_key(human));
    let snoozed = format!("$KV.{THREADS_BUCKET}.{}", snoozed_key(human));
    [
        format!("$JS.API.STREAM.INFO.{threads}"),
        format!("$JS.API.STREAM.CREATE.{threads}"),
        format!("$JS.API.DIRECT.GET.{threads}.{titles}"),
        titles,
        format!("$JS.API.DIRECT.GET.{threads}.{summaries}"),
        summaries,
        format!("$JS.API.DIRECT.GET.{threads}.{done}"),
        done,
        format!("$JS.API.DIRECT.GET.{threads}.{marks}"),
        marks,
        format!("$JS.API.DIRECT.GET.{threads}.{snoozed}"),
        snoozed,
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
    /// their pool caps, and every message's mark ([`message_mark_key`]), to tell which of those it sent keep waiting,
    /// and reads and writes the shared notes of the storage warnings ([`storage_warning_key`]), of the usage it reads in
    /// the account's JetStream info. Every user lists the streams, with their sizes and no subject stored. Of
    /// these and the owners bucket it gets no stream info, which lists every key of a bucket (so the threads of other
    /// ants): it reads single keys by direct gets. Nor does it get the stream's info, which lists every subject stored,
    /// so who messages whom: it finds the stream through the info of its own inbox consumer. The filter of each of its consumers is
    /// part of the subject that creates it, so none can be pointed at another inbox: a consumer with several filters
    /// has none in that subject, so no ant may create one. A human (a CLI user; [`Role::Human`], whatever its
    /// name) also watches all ants traffic, decides approval requests, asks for and withdraws the messages waiting in
    /// an ant's queue ([`QueueRequest`], [`message_mark_key`]: with its own [`unserved_key`]s, the only keys of the
    /// attempts bucket it writes or reads),
    /// reads every ant's thread and daily spend in the
    /// sessions bucket ([`thread_spend_key`], [`daily_spend_key`]) but none of its other keys there, may create the
    /// stream and the roster, sessions,
    /// attempts, channels, humans, chains, links and owners buckets, writes the channels, humans and owners buckets
    /// (the owners also in its outbox form, [`owner_outbox`]), reads the version the channel manager runs
    /// ([`channel_manager_serving_key`]), reads the stream's messages by sequence, one at a time or in batches of
    /// direct gets, and removes dead letters by sequence; its own read markers in [`READ_BUCKET`], and no other's,
    /// say which replies and channel posts it has read on any of its devices ([`read_marker_rights`]), and its own
    /// keys in [`PRESENCE_BUCKET`] where its devices read, reading every human's there ([`presence_rights`]), and its own [`NotifyPrefs`] in [`NOTIFY_BUCKET`] what it holds back of its notifications,
    /// and its own [`HeldNotices`] there what was held back ([`notify_prefs_rights`]), and it reads every reaction ([`REACTIONS_BUCKET`]), of which it
    /// puts and removes only its own ([`human_reaction_rights`]), while an ant only asks for those ([`subject::reactions`],
    /// [`subject::react`]); it sets every thread's title, TL;DR and done state and its own pinned,
    /// archived and snoozed threads, no other human's ([`thread_rights`]); it reads every device's encryption key in
    /// [`DEVICES_BUCKET`] and puts only its own devices' ([`device_directory_rights`]). A human may send under
    /// any sender, to replay a dead letter as it was. An ant gets no `STREAM.MSG.GET`: it takes any sequence, so it
    /// would expose every inbox. So only a human hears the max-deliveries advisories of the stream's consumers and
    /// stores the dead letters of the messages they name, on any ant's dead letter subject. Every user writes the
    /// attachments of the messages it sends only, under its own name, and an ant reads only those of the messages it
    /// reads, by direct gets; a human reads them all and may create their store ([`attachment_rights`]).
    ///
    /// An ant stores the tool-call timeline of its own work ([`subject::timeline_from`]) and reads none back; a human
    /// reads every timeline by message gets.
    ///
    /// Each inbox, notes and channel subject an ant may publish, and its own dead letter and timeline subjects, it may
    /// also publish in its outbox form ([`subject::outbox`]), which only a leaf node's outbox keeps, and so the links it passes on
    /// ([`link_outbox`]). Only a human sets up leaf outboxes, the hub's intake and the buckets' sources from it: a
    /// stream's configuration can copy, transform and republish any subject of the account, so no ant may create or
    /// change one.
    pub fn for_user(user: &UserId, role: Role) -> Self {
        let mut publish = Self::shared_publish(user, role);
        let mut subscribe = vec![format!("{}.>", inbox_prefix(user))];
        if role.is_human() {
            Self::human_rights(user, &mut publish, &mut subscribe);
        } else {
            Self::ant_rights(user, &mut publish, &mut subscribe);
        }
        Self { publish, subscribe }
    }

    /// What the credentials of `assistant`, the assistant of `human` ([`AssistantOf`]), allow: everything an ant may do,
    /// as it is one run by a daemon, and what a human may read and keep of its own besides ([`Self::for_user`]): it
    /// watches all ants traffic, so its human's home queue, reads the stream and the buckets a human reads, sets
    /// titles, TL;DRs and done states, puts its own reactions, pins, archives and read markers, and reads every
    /// attachment and every device's encryption key. Of its human's own keys it reads, by direct gets, never writes,
    /// the read markers, pins, archives and snoozed threads ([`read_marker_rights`], [`thread_rights`]) and the markers
    /// of messages kept waiting ([`unserved_key`]), so it sees what its human has read and set aside.
    ///
    /// It sends only as itself, never under any sender as a human may, nor writes attachments under another's name.
    /// It gets none of a human's rights whose subjects do not name who acts, which readers could not tell from its
    /// human's: deciding approval requests, cancelling, queueing or withdrawing an ant's messages, recording a
    /// thread's owner, putting dead letters, answering an ant's reactions requests or another's renewal. Nor does it
    /// change the team: no entry of [`HUMANS_BUCKET`] (where its human's grant stands) or [`CHANNELS_BUCKET`], no
    /// stream created, updated or purged of a message. It puts no device encryption key, no presence and no
    /// notification holds, which only a human's devices have, and it gets no power of the account key: it signs no
    /// credentials and records no revocation.
    pub fn for_assistant(assistant: &UserId, human: &UserId) -> Self {
        let mut publish = Self::shared_publish(assistant, Role::Ant);
        let mut subscribe = vec![format!("{}.>", inbox_prefix(assistant))];
        Self::ant_rights(assistant, &mut publish, &mut subscribe);
        let mut as_human = Vec::new();
        Self::human_rights(assistant, &mut as_human, &mut subscribe);
        let devices_only = [subject::devices_of(assistant), subject::presence_of(assistant)];
        let notify = notify_prefs_rights(assistant);
        let unattributed = [
            subject::ALL_APPROVAL_DECISIONS.to_owned(),
            subject::ALL_CANCELS.to_owned(),
            subject::ALL_QUEUE_REQUESTS.to_owned(),
            subject::ALL_REACTIONS_REPLIES.to_owned(),
            subject::ALL_RENEW_REQUESTS.to_owned(),
            subject::DEAD_ALL.to_owned(),
            format!("$KV.{ATTEMPTS_BUCKET}.*.msg.*"),
            format!("$KV.{OWNERS_BUCKET}.*.*"),
            "ants.out.owners.*.*".to_owned(),
            format!("$KV.{CHANNELS_BUCKET}.>"),
            format!("$JS.API.STREAM.INFO.{OUTBOX_STREAM}"),
            format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"),
        ];
        let humans = format!("$KV.{HUMANS_BUCKET}.");
        let withheld = |s: &str| {
            s.starts_with(&humans)
                || ["$JS.API.STREAM.CREATE.", "$JS.API.STREAM.UPDATE.", "$JS.API.STREAM.MSG.DELETE.", "$JS.*.API."]
                    .iter()
                    .any(|prefix| s.starts_with(prefix))
        };
        as_human
            .retain(|s| !devices_only.contains(s) && !notify.contains(s) && !unattributed.contains(s) && !withheld(s));
        publish.extend(as_human);
        let chunks = format!("$O.{FILES_BUCKET}.C");
        publish.push(format!("$JS.API.DIRECT.GET.{FILES_STREAM}.{chunks}.>"));
        let gets_only = |rights: &[String]| -> Vec<String> {
            rights.iter().filter(|s| s.starts_with("$JS.API.DIRECT.GET.")).cloned().collect()
        };
        publish.extend(gets_only(&read_marker_rights(human)));
        publish.extend(gets_only(&thread_rights(human)));
        let attempts = kv_stream(ATTEMPTS_BUCKET);
        publish.push(format!("$JS.API.DIRECT.GET.{attempts}.$KV.{ATTEMPTS_BUCKET}.{human}.unserved.*"));
        let mut seen = std::collections::HashSet::new();
        publish.retain(|s| seen.insert(s.clone()));
        let mut seen = std::collections::HashSet::new();
        subscribe.retain(|s| seen.insert(s.clone()));
        Self { publish, subscribe }
    }

    /// The publish rights of every user, ant or human.
    fn shared_publish(user: &UserId, role: Role) -> Vec<String> {
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
        let sendable = subject::sendable(user, role);
        // The same subjects in the leaf's outbox, for when the hub is out of reach.
        let outbox = sendable.iter().map(|s| subject::outbox(s).expect("an outbox subject"));
        let mut publish: Vec<String> = sendable.iter().cloned().chain(outbox).collect();
        publish.extend([
            format!("$JS.API.STREAM.INFO.{roster_stream}"),
            // The size of every stream, for the storage warnings: a stream list carries no subject stored.
            "$JS.API.STREAM.LIST".to_owned(),
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
        publish.extend(attachment_rights(user, role));
        publish
    }

    /// What a human may do besides.
    fn human_rights(user: &UserId, publish: &mut Vec<String>, subscribe: &mut Vec<String>) {
        let roster_stream = kv_stream(ROSTER_BUCKET);
        let channels = kv_stream(CHANNELS_BUCKET);
        let humans = kv_stream(HUMANS_BUCKET);
        let chains = kv_stream(CHAINS_BUCKET);
        let links = kv_stream(LINKS_BUCKET);
        let owners = kv_stream(OWNERS_BUCKET);
        let webhooks = kv_stream(WEBHOOKS_BUCKET);
        let sessions = kv_stream(SESSIONS_BUCKET);
        let attempts = kv_stream(ATTEMPTS_BUCKET);
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
            // Room for the consumers of every daemon watching the bucket, which a KV bucket gets only by an update
            // right after creating it.
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
            // Put and delete of every channel's and every human's entry, and of the keys under its own name, such
            // as its assistant grant, which no one else writes.
            format!("$KV.{CHANNELS_BUCKET}.>"),
            format!("$KV.{HUMANS_BUCKET}.*"),
            format!("$KV.{HUMANS_BUCKET}.{user}.>"),
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
            // Answering an ant's request for the reactions to a message it reads.
            subject::ALL_REACTIONS_REPLIES.to_owned(),
            // Withdrawing a message from an ant's queue: creating its mark, and reading the mark that won.
            format!("$KV.{ATTEMPTS_BUCKET}.*.msg.*"),
            format!("$JS.API.DIRECT.GET.{attempts}.$KV.{ATTEMPTS_BUCKET}.*.msg.*"),
            // Its own markers of the messages it was told keep waiting for an ant no daemon serves.
            format!("$KV.{ATTEMPTS_BUCKET}.{user}.unserved.*"),
            format!("$JS.API.DIRECT.GET.{attempts}.$KV.{ATTEMPTS_BUCKET}.{user}.unserved.*"),
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
        publish.extend(machine_record_rights());
        publish.extend(read_marker_rights(user));
        publish.extend(presence_rights(user));
        publish.extend(thread_rights(user));
        publish.extend(notify_prefs_rights(user));
        publish.extend(human_reaction_rights(user));
        publish.extend(device_directory_rights(user));
        subscribe.extend([subject::ALL.to_owned(), subject::max_deliveries_advisories()]);
    }

    /// What an ant may do besides.
    fn ant_rights(user: &UserId, publish: &mut Vec<String>, subscribe: &mut Vec<String>) {
        let consumer = inbox_consumer(user);
        let notes = notes_consumer(user);
        let chan_read = chan_reader(user);
        let sessions = kv_stream(SESSIONS_BUCKET);
        let attempts = kv_stream(ATTEMPTS_BUCKET);
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
            // The account's JetStream usage, and the shared notes of the storage warnings given.
            "$JS.API.INFO".to_owned(),
            format!("$JS.API.DIRECT.GET.{sessions}.$KV.{SESSIONS_BUCKET}.{STORAGE_WARNING_PREFIX}.*.*.warned"),
            format!("$KV.{SESSIONS_BUCKET}.{STORAGE_WARNING_PREFIX}.*.*.warned"),
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
            // Storing the timeline of its own work, which no ant reads back.
            subject::timeline_from(user),
            subject::outbox(&subject::timeline_from(user)).expect("an outbox subject"),
            // Reading its own dead letters back, through its own consumer filtering only those.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{dead_read}.{}", subject::dead(user)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{dead_read}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{dead_read}"),
            // Reading back the messages it sent, through its own consumer filtering only those.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{sent_read}.{}", subject::inbox_from(user)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{sent_read}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{sent_read}"),
            // Get of the mark of every message to an ant, to tell whether those it sent still wait.
            format!("$JS.API.DIRECT.GET.{attempts}.$KV.{ATTEMPTS_BUCKET}.*.msg.*"),
            subject::status_of(user),
            subject::cancelled_of(user),
            subject::queued(user),
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
            // Asking for the reactions to a message it reads, and to add or remove its own.
            subject::reactions(user),
            subject::react(user),
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
            subject::queue(user),
            // Live replies to its ask_ant calls, which the inbox consumer holds back while the asking prompt runs.
            subject::inbox_of(user),
            subject::renew_reply(user.as_str()),
            subject::antreq_reply(user),
        ]);
    }
}

impl Permissions {
    /// The same permissions, also for clients that use the JetStream API of `domain` (`$JS.<domain>.API.…`), as every
    /// ants process does when the hub has a domain. KV puts and deletes then go through that prefix too
    /// (`$JS.<domain>.API.$KV.…`). The plain subjects stay allowed: the hub maps the domain prefix onto them and checks
    /// the mapped subject against the permissions as well. Acks, flow control and ants subjects are the same in both.
    /// Without a domain the permissions are unchanged.
    #[must_use]
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

    /// The same permissions, for a leaf node's uplink to a hub in JetStream domain `domain`: the JetStream API and KV
    /// only in their domain form (`$JS.<domain>.API.…`, `$JS.<domain>.API.$KV.…`), the plain ones dropped. Their
    /// domains differ (the leaf's own JetStream is in the machine's), so nats-server denies `$JS.API.>`, `$KV.>` and
    /// `$OBJ.>` on the leaf connection both ways, and checks a message from the leaf against the uplink's permissions
    /// before mapping the domain prefix only: the plain forms never cross the uplink, and listing them would only
    /// take room in its JWT ([`Self::for_leaf`]). Without a domain the permissions are unchanged.
    #[must_use]
    pub fn with_uplink_js_domain(mut self, domain: Option<&JsDomain>) -> Self {
        let Some(domain) = domain else { return self };
        let api = format!("$JS.{domain}.API.");
        for s in &mut self.publish {
            if let Some(rest) = s.strip_prefix("$JS.API.") {
                *s = format!("{api}{rest}");
            } else if s.starts_with("$KV.") {
                *s = format!("{api}{s}");
            }
        }
        let mut seen = std::collections::HashSet::new();
        self.publish.retain(|s| seen.insert(s.clone()));
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
            let own = (!role.is_human()).then(|| [subject::chanadm_from(user), subject::timeline_from(user)]);
            let found = subject::sendable(user, role).into_iter().chain(own.into_iter().flatten());
            let found = found.map(|s| s.replace('*', user.as_str()));
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
        // The machine's host connection, which its daemon makes through the leaf node.
        let host = Self::for_host(machine);
        leaf.publish.extend(host.publish);
        leaf.subscribe.extend(host.subscribe);
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

    /// What the host credentials of `machine` allow (`ants auth host`), with which the machine's daemon makes one
    /// connection besides its ants': putting the machine's own record in [`MACHINES_BUCKET`] ([`subject::machine_record`]),
    /// and taking the admin orders to the machine's ants from the stream through its own consumer
    /// ([`admin_consumer`]), whose filter, in the subject, is pinned to the machine's orders ([`subject::admin_orders`]),
    /// and acking them. It hears its own replies under [`host_inbox_prefix`], apart from every user's. Nothing else: no
    /// other machine's record, no message to or from an ant. No ant gets any of it.
    pub fn for_host(machine: &Machine) -> Self {
        let record = subject::machine_record(machine);
        let consumer = admin_consumer(machine);
        let mut publish = vec![
            record,
            // Finding JetStream on connecting, as an ant does with its inbox consumer, without the stream's info.
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{consumer}"),
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{consumer}.{}", subject::admin_orders(machine)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{consumer}"),
        ];
        publish.extend(ack_subjects(STREAM, &consumer));
        Self { publish, subscribe: vec![format!("{}.>", host_inbox_prefix(machine))] }
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
    /// conversation as stored, nor the sessions, attempts, owners, links, chains, humans, channels, webhooks, read,
    /// devices or presence buckets: where the devices of humans are is for humans alone to see. It gets no stream info,
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
    /// the hub's stream, links bucket and attachments' store from the leaf's outbox, as `ants leaf outbox` does, with
    /// the replies of its JetStream requests under [`ENROLL_INBOX`].
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
                format!("$JS.API.STREAM.INFO.{FILES_STREAM}"),
                format!("$JS.API.STREAM.UPDATE.{FILES_STREAM}"),
            ],
            subscribe: vec![
                subject::ALL_ENROLL_REQUESTS.to_owned(),
                subject::ALL_RENEW_REQUESTS.to_owned(),
                subject::ALL_ANT_REQUESTS.to_owned(),
                format!("{ENROLL_INBOX}.>"),
            ],
        }
    }

    /// What `ants signer` also allows itself, on the machine holding the account key, to answer ants' reactions
    /// requests ([`ReactionsRequest`]): hearing every request for the reactions to a message
    /// ([`subject::ALL_REACTIONS_REQUESTS`]) and to change an ant's own ([`subject::ALL_REACT_REQUESTS`]); reading the
    /// stream back by batched direct gets, from its info, to find whether the asking ant reads the message; reading
    /// every reaction, singly and through ordered consumers; putting and deleting any user's reaction, which only it
    /// does for an ant, once it found the ant reads the message; and answering on the ant's own
    /// [`subject::reactions_reply`]. The replies of its JetStream requests come under [`ENROLL_INBOX`], as those of
    /// [`Permissions::for_enroll_approver`], with which it is issued.
    pub fn for_reactions_responder() -> Self {
        let reactions = kv_stream(REACTIONS_BUCKET);
        let mut publish = vec![
            format!("$JS.API.STREAM.INFO.{STREAM}"),
            format!("$JS.API.DIRECT.GET.{STREAM}"),
            format!("$JS.API.DIRECT.GET.{reactions}.>"),
            format!("$JS.API.CONSUMER.CREATE.{reactions}"),
            format!("$KV.{REACTIONS_BUCKET}.>"),
            subject::ALL_REACTIONS_REPLIES.to_owned(),
        ];
        publish.extend(flow_control_subjects("*", &reactions));
        Self {
            publish,
            subscribe: vec![
                subject::ALL_REACTIONS_REQUESTS.to_owned(),
                subject::ALL_REACT_REQUESTS.to_owned(),
                format!("{ENROLL_INBOX}.>"),
            ],
        }
    }

    /// What the short-lived credentials `ants auth revoke` and `ants auth push` issue themselves from the account key
    /// allow, to say in [`DEVICES_BUCKET`] which keys of humans' credentials are revoked: creating the bucket, reading
    /// and putting the revocation records ([`subject::revoked_devices`]) and nothing else of it, with the replies of
    /// its JetStream requests under [`ENROLL_INBOX`]. No other credentials put a revocation record.
    pub fn for_device_revoker() -> Self {
        let devices = kv_stream(DEVICES_BUCKET);
        let revoked = subject::revoked_devices();
        Self {
            publish: vec![
                "$JS.API.INFO".to_owned(),
                format!("$JS.API.STREAM.INFO.{devices}"),
                format!("$JS.API.STREAM.CREATE.{devices}"),
                format!("$JS.API.DIRECT.GET.{devices}.{revoked}"),
                revoked,
            ],
            subscribe: vec![format!("{ENROLL_INBOX}.>")],
        }
    }
}

impl Permissions {
    /// What the team's public bootstrap credentials allow (`ants auth bootstrap`): asking for the credentials of a
    /// human's new device on [`subject::login`] and hearing the answers, nothing else, JetStream included.
    pub fn for_bootstrap() -> Self {
        Self {
            publish: vec![subject::ALL_LOGIN_REQUESTS.to_owned()],
            subscribe: vec![subject::ALL_LOGIN_REPLIES.to_owned()],
        }
    }

    /// What `ants signer` allows itself on top of [`Permissions::for_enroll_approver`]: hearing every registration
    /// request and answering it. Leaf uplinks never carry these, as registration happens on the hub only.
    pub fn for_registrar() -> Self {
        Self {
            publish: vec![subject::ALL_LOGIN_REPLIES.to_owned()],
            subscribe: vec![subject::ALL_LOGIN_REQUESTS.to_owned()],
        }
    }
}

/// The version of the permissions `ants auth` gives credentials of every kind (ants, humans, webhooks, leaf node
/// uplinks, the channel manager, enrollment), stamped into each user JWT it issues as the tag
/// [`PermissionsVersion::tag`]. Credentials carrying an older version were issued before the permissions changed:
/// ants processes using them warn and name the command that renews them.
///
/// Bump it whenever the permissions of any role change: [`Permissions::for_user`], [`Permissions::for_assistant`], [`Permissions::for_leaf`],
/// [`Permissions::for_webhook`], [`Permissions::for_channel_manager`], [`Permissions::for_host`],
/// [`Permissions::for_enrollment`],
/// [`Permissions::for_enroll_approver`], [`Permissions::for_reactions_responder`], [`Permissions::for_device_revoker`]
/// or [`Permissions::with_js_domain`], or
/// the other claims `ants auth` puts in a
/// user JWT.
pub const PERMISSIONS_VERSION: PermissionsVersion = PermissionsVersion(3);

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

#[cfg(test)]
mod tests;
