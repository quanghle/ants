//! What the credentials of an ant or a human allow ([`Permissions::for_user`]): the rights every user holds, those of
//! an ant and those of a human, and the attachments each writes and reads.

use super::human::{
    device_directory_rights, human_notify_rights, human_reaction_rights, human_store_rights, identity_rights,
    machine_record_rights, notify_prefs_rights, outbox_setup_rights, presence_rights, read_marker_rights,
    rollup_denial_rights, team_rights, thread_rights, trail_rights,
};
use super::{Permissions, ack_subjects, kv_stream, ordered_consumer_subjects};
use crate::{
    ATTEMPTS_BUCKET, CHAINS_BUCKET, CHANNELS_BUCKET, COLONY_BUCKET, DEVICES_BUCKET, FILES_BUCKET, FILES_STREAM,
    HUMANS_BUCKET, IDENTITIES_BUCKET, LINKS_BUCKET, NOTIFY_BUCKET, OWNERS_BUCKET, READ_BUCKET, Role, SESSIONS_BUCKET,
    STORAGE_WARNING_PREFIX, STREAM, TEAMS_BUCKET, THREADS_BUCKET, TRAIL_RATINGS_BUCKET, TRAIL_RUNS_BUCKET,
    TRAIL_TIPS_BUCKET, TRAILS_BUCKET, UserId, chan_consumer, chan_reader, dead_reader, done_reader, inbox_consumer,
    inbox_prefix, notes_consumer, sent_reader, spare_readers, subject, trail_runs_reader,
};
#[cfg(doc)]
use crate::{
    AttachmentHandle, HeldNotices, NotifyPrefs, PRESENCE_BUCKET, QueueRequest, REACTIONS_BUCKET, ReadMarker,
    TrailRequest, channel_manager_serving_key, daily_spend_key, link_outbox, message_mark_key, owner_outbox,
    storage_warning_key, thread_spend_key, unserved_key,
};

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

/// What an ant asks `ants signer` for, each on a subject naming it: an ant added from a template, should it manage one;
/// the reactions to a message it reads, and adding or removing its own; saving a trail as its own or removing one it
/// saved; and changing a team it manages.
fn signer_requests(user: &UserId) -> [String; 5] {
    [
        subject::antreq(user),
        subject::reactions(user),
        subject::react(user),
        subject::save_trail(user),
        subject::teamadm(user),
    ]
}

impl Permissions {
    /// What the credentials of `user` in `role` allow.
    ///
    /// Any ant may write to every inbox, the notes of every thread and every channel, but only as itself (the sender is
    /// the last token of inbox, notes and channel subjects). It speaks only for itself on status, approvals and dead
    /// letters, reads back only its own dead letters (through its own [`dead_reader`]) and the messages it sent
    /// (through its own [`sent_reader`]), reads only its own inbox (consumer and its spares, and live), the notes of
    /// every thread (live, and from the stream through its own notes consumer and its spares), every channel (live, and
    /// from the stream through its own channel consumer, and its reader and the reader's spares), its own replies and
    /// the decisions on its own approval requests (live, and as stored), reads the colony to find ants by capability
    /// but writes only its own entry, reads every escalation chain by key but writes only its own, lists the links to a
    /// channel thread ants passed on but records only its own ([`LINKS_BUCKET`]), reads the channels, humans and owners
    /// buckets but never writes them, reads nothing of the webhooks bucket, asks the channel manager for changes to a
    /// channel only as itself ([`subject::chanadm_from`]), and reads and writes only its own keys in the sessions and
    /// attempts buckets, except that it reads every ant's daily spend there ([`daily_spend_key`]), to hold the ants of
    /// a template to their pool caps, and every message's mark ([`message_mark_key`]), to tell which of those it sent
    /// keep waiting, and reads and writes the shared notes of the storage warnings ([`storage_warning_key`]), of the
    /// usage it reads in the account's JetStream info. Every user lists the streams, with their sizes and no subject
    /// stored. Of these and the owners bucket it gets no stream info, which lists every key of a bucket (so the threads
    /// of other ants): it reads single keys by direct gets. Nor does it get the stream's info, which lists every
    /// subject stored, so who messages whom: it finds the stream through the info of its own inbox consumer, and reads
    /// where that one and its channel consumer stand through theirs, for `ants queue`. The filter
    /// of each of its consumers is part of the subject that creates it, so none can be pointed at another inbox: a
    /// consumer with several filters has none in that subject, so no ant may create one. A human (a CLI user;
    /// [`Role::Human`], whatever its name) also watches all ants traffic, decides approval requests, sends admin orders
    /// to any machine ([`subject::admin_order`]), which a daemon carries out only when an admin signed them, asks for
    /// and withdraws the messages waiting in an ant's queue ([`QueueRequest`], [`message_mark_key`]: with its own
    /// [`unserved_key`]s, the only keys of the attempts bucket it writes or reads), reads every ant's thread and daily
    /// spend in the sessions bucket ([`thread_spend_key`], [`daily_spend_key`]) but none of its other keys there, may
    /// create the stream and the colony, sessions, attempts, channels, humans, chains, links and owners buckets, writes
    /// the channels, humans and owners buckets (the owners also in its outbox form, [`owner_outbox`]), reads the
    /// version the channel manager runs ([`channel_manager_serving_key`]), reads the stream's messages by sequence, one
    /// at a time or in batches of direct gets, and removes dead letters by sequence; its own read markers in
    /// [`READ_BUCKET`], and no other's, say which replies and channel posts it has read on any of its devices
    /// (`read_marker_rights`), and its own keys in [`PRESENCE_BUCKET`] where its devices read, reading every human's
    /// there (`presence_rights`), and its own [`NotifyPrefs`] in [`NOTIFY_BUCKET`] what it holds back of its
    /// notifications, and its own [`HeldNotices`] there what was held back (`notify_prefs_rights`), and it reads every
    /// reaction ([`REACTIONS_BUCKET`]), of which it puts and removes only its own (`human_reaction_rights`), while an
    /// ant only asks for those ([`subject::reactions`], [`subject::react`]); it sets every thread's title, TL;DR and
    /// done state and its own pinned, archived and snoozed threads, no other human's (`thread_rights`), while an ant
    /// only reads, one key at a time, a thread's title and done state and a human's own [`Reminders`](crate::Reminders)
    /// entry (its snoozes and reminder notes), and hears of every thread marked done through its own consumer
    /// ([`done_reader`]), which it deletes, with its channel consumer ([`chan_consumer`]), as its retirement stops it,
    /// nothing else of [`THREADS_BUCKET`], and of [`READ_BUCKET`] and [`NOTIFY_BUCKET`] only a human's inbox
    /// [`ReadMarker`] and [`NotifyPrefs`], to stop reminding a human of an escalation it heeded; it reads every
    /// device's encryption key in [`DEVICES_BUCKET`] and puts only its own devices' (`device_directory_rights`). A
    /// human may send under any sender, to replay a dead letter as it was. An ant gets no `STREAM.MSG.GET`: it takes
    /// any sequence, so it would expose every inbox. So only a human hears the max-deliveries advisories of the
    /// stream's consumers and stores the dead letters of the messages they name, on any ant's dead letter subject.
    /// Every user writes the attachments of the messages it sends only, under its own name, and an ant reads only those
    /// of the messages it reads, by direct gets; a human reads them all and may create their store
    /// (`attachment_rights`).
    ///
    /// An ant stores the tool-call timeline of its own work ([`subject::timeline_from`]) and reads back its own only,
    /// through its own [`sent_reader`] filtering only those, for its dreams' review packets; a human reads every
    /// timeline by message gets.
    ///
    /// An ant creates and updates its own identity record in [`IDENTITIES_BUCKET`] ([`subject::identity`]), no other's,
    /// and reads every ant's, which say nothing secret; a human reads every ant's and sets their avatars and colours
    /// (`identity_rights`).
    ///
    /// Every ant and human reads the saved trails ([`TRAILS_BUCKET`]) by direct gets naming no subject, and a human puts
    /// and deletes any. A trail's key is its name, so no subject can name its saver: an ant writes none, but asks
    /// `ants signer` to save a trail as its own or remove one it saved ([`crate::SaveTrailRequest`], on
    /// [`subject::save_trail`]), which writes only new trails and replaces or removes only those the ant saved
    /// ([`crate::Saver`], [`Permissions::for_trail_keeper`]). An ant gets, puts and deletes only the records of the trail runs it conducts
    /// ([`subject::trail_runs`]), and reads one back past messages a stream relayed onto it through its own consumer
    /// ([`trail_runs_reader`]); a human reads every run's and writes none (`trail_rights`), but asks an ant's daemon to
    /// start, stop or resume the runs it conducts ([`TrailRequest`], on [`subject::trail`], which the ant alone hears
    /// and answers on [`subject::trailed`]). Every ant and human reads the trails' tips ([`TRAIL_TIPS_BUCKET`]) by
    /// direct gets naming no subject; an ant puts only its own tips and its own votes, under keys naming it; a human
    /// retires any tip, under a key no ant writes ([`crate::trail_tip_retired_key`]), and purges a trail's. Every ant and human reads the trails' ratings
    /// ([`TRAIL_RATINGS_BUCKET`]) the same way; an ant puts only its own, under keys naming it, and a human writes none
    /// but purges a removed trail's.
    ///
    /// Every ant and human reads the team directory ([`TEAMS_BUCKET`]) by direct gets naming no subject; a human
    /// creates the bucket and puts the directory by compare-and-set (`team_rights`). The directory is one entry for
    /// every team, so no subject can hold an ant to the teams it manages: an ant writes none of it, but asks
    /// `ants signer` to change a team it manages ([`crate::TeamAdminRequest`], on [`subject::teamadm`],
    /// [`Permissions::for_team_keeper`]).
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

    /// The publish rights of every user, ant or human.
    pub(super) fn shared_publish(user: &UserId, role: Role) -> Vec<String> {
        let consumer = inbox_consumer(user);
        let notes = notes_consumer(user);
        let (chan, chan_read) = (chan_consumer(user), chan_reader(user));
        let colony_stream = kv_stream(COLONY_BUCKET);
        let channels = kv_stream(CHANNELS_BUCKET);
        let humans = kv_stream(HUMANS_BUCKET);
        let chains = kv_stream(CHAINS_BUCKET);
        let links = kv_stream(LINKS_BUCKET);
        let owners = kv_stream(OWNERS_BUCKET);
        let sendable = subject::sendable(user, role);
        // The same subjects in the leaf's outbox, for when the hub is out of reach.
        let outbox = sendable.iter().map(|s| subject::outbox(s).expect("an outbox subject"));
        let mut publish: Vec<String> = sendable.iter().cloned().chain(outbox).collect();
        publish.extend([
            format!("$JS.API.STREAM.INFO.{colony_stream}"),
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
            // Reading the colony: single entries, the last of each by batched direct gets (as the page lists it), and
            // the ordered consumers behind listing and watching it.
            format!("$JS.API.DIRECT.GET.{colony_stream}.>"),
            format!("$JS.API.DIRECT.GET.{colony_stream}"),
        ]);
        publish.extend(ordered_consumer_subjects(&colony_stream));
        publish.extend([
            // Reading the channels the same way.
            format!("$JS.API.STREAM.INFO.{channels}"),
            format!("$JS.API.DIRECT.GET.{channels}.>"),
        ]);
        publish.extend(ordered_consumer_subjects(&channels));
        publish.extend([
            // And the humans.
            format!("$JS.API.STREAM.INFO.{humans}"),
            format!("$JS.API.DIRECT.GET.{humans}.>"),
        ]);
        publish.extend(ordered_consumer_subjects(&humans));
        publish.extend([
            // And single escalation chains.
            format!("$JS.API.STREAM.INFO.{chains}"),
            format!("$JS.API.DIRECT.GET.{chains}.>"),
            // And the links passed on to a channel thread, through ordered consumers filtering that thread's.
            format!("$JS.API.STREAM.INFO.{links}"),
        ]);
        publish.extend(ordered_consumer_subjects(&links));
        publish.extend([
            // And the owners of single threads.
            format!("$JS.API.DIRECT.GET.{owners}.>"),
            // And the saved trails, by direct gets naming no subject, which list them too.
            format!("$JS.API.DIRECT.GET.{}", kv_stream(TRAILS_BUCKET)),
            // And the trails' tips and votes, the same way.
            format!("$JS.API.DIRECT.GET.{}", kv_stream(TRAIL_TIPS_BUCKET)),
            // And the trails' ratings, the same way.
            format!("$JS.API.DIRECT.GET.{}", kv_stream(TRAIL_RATINGS_BUCKET)),
            // And the team directory, the same way, past a message a stream relayed onto its key.
            format!("$JS.API.DIRECT.GET.{}", kv_stream(TEAMS_BUCKET)),
        ]);
        publish.extend(attachment_rights(user, role));
        publish
    }

    /// What a human may do besides.
    pub(super) fn human_rights(user: &UserId, publish: &mut Vec<String>, subscribe: &mut Vec<String>) {
        publish.extend(human_notify_rights(user));
        publish.extend(human_store_rights(user));
        publish.extend(outbox_setup_rights());
        publish.extend(machine_record_rights());
        publish.extend(identity_rights());
        publish.extend(read_marker_rights(user));
        publish.extend(presence_rights(user));
        publish.extend(thread_rights(user));
        publish.extend(notify_prefs_rights(user));
        publish.extend(human_reaction_rights(user));
        publish.extend(device_directory_rights(user));
        publish.extend(trail_rights());
        publish.extend(team_rights());
        publish.extend(rollup_denial_rights());
        subscribe.extend([subject::ALL.to_owned(), subject::max_deliveries_advisories()]);
    }

    /// What an ant may do besides.
    pub(super) fn ant_rights(user: &UserId, publish: &mut Vec<String>, subscribe: &mut Vec<String>) {
        let consumer = inbox_consumer(user);
        let notes = notes_consumer(user);
        let chan_read = chan_reader(user);
        let sessions = kv_stream(SESSIONS_BUCKET);
        let attempts = kv_stream(ATTEMPTS_BUCKET);
        let own_keys = subject::kv(SESSIONS_BUCKET, format_args!("{user}.>"));
        let own_attempts = subject::kv(ATTEMPTS_BUCKET, format_args!("{user}.>"));
        let own_runs = subject::trail_runs(user);
        let (runs, runs_read) = (kv_stream(TRAIL_RUNS_BUCKET), trail_runs_reader(user));
        let dead_read = dead_reader(user);
        let sent_read = sent_reader(user);
        publish.extend([
            // Get and put of its own sessions.
            format!("$JS.API.DIRECT.GET.{sessions}.{own_keys}"),
            own_keys,
            // Get of every ant's daily spend, which pool caps add up.
            subject::kv_get(SESSIONS_BUCKET, "*.day.*"),
            // The account's JetStream usage, and the shared notes of the storage warnings given.
            "$JS.API.INFO".to_owned(),
            subject::kv_get(SESSIONS_BUCKET, format_args!("{STORAGE_WARNING_PREFIX}.*.*.warned")),
            subject::kv(SESSIONS_BUCKET, format_args!("{STORAGE_WARNING_PREFIX}.*.*.warned")),
            // Get, put and delete of the markers of its own attempts.
            format!("$JS.API.DIRECT.GET.{attempts}.{own_attempts}"),
            own_attempts,
            // Get, put and delete of the records of the trails it conducts.
            format!("$JS.API.DIRECT.GET.{runs}.{own_runs}"),
            // Reading one of them back past messages a stream relayed onto it, through its own consumer, whose
            // filter, part of the subject, is one of its own keys.
            format!("$JS.API.CONSUMER.CREATE.{runs}.{runs_read}.{own_runs}"),
            format!("$JS.API.CONSUMER.MSG.NEXT.{runs}.{runs_read}"),
            format!("$JS.API.CONSUMER.DELETE.{runs}.{runs_read}"),
            own_runs,
            // Put of its own tips on trails and its own votes on tips.
            subject::kv(TRAIL_TIPS_BUCKET, format_args!("*.{user}.>")),
            // Put of its own ratings of the trails its runs took.
            subject::kv(TRAIL_RATINGS_BUCKET, format_args!("*.{user}.*")),
            // Exactly its requests subject: an ant cannot answer its own requests one level below.
            subject::approvals(user),
            // Reading the decision stored on one of its requests: a direct get naming the subject in its own
            // subject returns only the last message on that subject, so nothing else is readable this way.
            format!("$JS.API.DIRECT.GET.{STREAM}.{}.*", subject::approvals(user)),
            subject::dead(user),
            subject::outbox(&subject::dead(user)).expect("an outbox subject"),
            // Storing the timeline of its own work.
            subject::timeline_from(user),
            subject::outbox(&subject::timeline_from(user)).expect("an outbox subject"),
            // Reading it back, for its dreams, through its sent reader filtering only its own timeline, of every
            // thread or of one (the `*` of the filter allows both).
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{sent_read}.{}", subject::timeline_from(user)),
            // Reading its own dead letters back, through its own consumer filtering only those.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{dead_read}.{}", subject::dead(user)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{dead_read}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{dead_read}"),
            // Reading back the messages it sent, through its own consumer filtering only those.
            format!("$JS.API.CONSUMER.CREATE.{STREAM}.{sent_read}.{}", subject::inbox_from(user)),
            format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{sent_read}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{sent_read}"),
            // Get of the mark of every message to an ant, to tell whether those it sent still wait.
            subject::kv_get(ATTEMPTS_BUCKET, "*.msg.*"),
            subject::status_of(user),
            subject::cancelled_of(user),
            subject::queued(user),
            subject::trailed(user),
            subject::chanadm_from(user),
            // Put and delete of its own colony entry, and of its own escalation chain.
            subject::colony(user),
            subject::chain(user),
            // Get of every ant's identity record, by direct gets naming no subject, one right shared by every ant
            // behind a leaf's uplink; create and update of its own only.
            format!("$JS.API.DIRECT.GET.{}", kv_stream(IDENTITIES_BUCKET)),
            subject::identity(user),
            // Put of the links it passed on, and the same in the leaf's outbox.
            subject::kv(LINKS_BUCKET, format_args!("{user}.>")),
            format!("ants.out.links.{user}.>"),
            subject::renew(user.as_str()),
            // Get of the assistants' records and of the revocations, to check what an assistant does for its human.
            format!("$JS.API.DIRECT.GET.{}.{}", kv_stream(DEVICES_BUCKET), subject::assistant_records()),
            format!("$JS.API.DIRECT.GET.{}.{}", kv_stream(DEVICES_BUCKET), subject::revoked_devices()),
            // Get of a thread's title, to title the pull request of the thread's branch: one key at a time, which
            // it must know, as it gets no stream info listing them.
            subject::kv_get(THREADS_BUCKET, "title.*"),
            // Get of what tells whether the human an escalation reached heeded it, to stop reminding it: a thread's
            // done state, a human's own reminders entry with its snoozes (not those its assistant set, a key deeper),
            // its inbox read marker (not its channel reads) and the notifications it holds back (not those held). One
            // key at a time, as for titles.
            subject::kv_get(THREADS_BUCKET, "done.*"),
            subject::kv_get(THREADS_BUCKET, "reminders.*"),
            subject::kv_get(READ_BUCKET, "*"),
            subject::kv_get(NOTIFY_BUCKET, "*"),
        ]);
        // Hearing of the threads marked done, to tell its provider: its own durable consumer, its filter pinned to
        // the markings of threads done, which so lists the threads marked done, as the notes it reads list the threads
        // noted.
        let (threads, done) = (kv_stream(THREADS_BUCKET), done_reader(user));
        publish.extend([
            format!("$JS.API.CONSUMER.CREATE.{threads}.{done}.{}", subject::kv(THREADS_BUCKET, "done.*.*")),
            format!("$JS.API.CONSUMER.MSG.NEXT.{threads}.{done}"),
        ]);
        publish.extend(ack_subjects(&threads, &done));
        publish.extend(signer_requests(user));
        // Deleting that consumer and its channel consumer as its retirement stops it, so neither keeps piling up; and
        // where the channel consumer stands, as `ants queue` lists it.
        publish.extend([
            format!("$JS.API.CONSUMER.DELETE.{threads}.{done}"),
            format!("$JS.API.CONSUMER.DELETE.{STREAM}.{}", chan_consumer(user)),
            format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", chan_consumer(user)),
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
            // Cancel and queue requests, from a human or naming their sender, an assistant.
            subject::all_cancels_of(user),
            subject::queue(user),
            subject::queues_by_of(user),
            // A human's requests to start, stop and resume the trail runs it conducts.
            subject::trail(user),
            // Live replies to its ask_ant calls, which the inbox consumer holds back while the asking prompt runs.
            subject::inbox_of(user),
            subject::renew_reply(user.as_str()),
            subject::antreq_reply(user),
        ]);
    }
}
