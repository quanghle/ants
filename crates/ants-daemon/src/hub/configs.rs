//! The configuration of each object as this version creates it: the `ANTS` stream, the KV buckets, those kept as
//! streams of their own settings, and the attachments' store.

use std::time::Duration;

use ants_proto::{
    ATTACHMENT_CHUNK, ATTACHMENT_HEADROOM, CHAINS_BUCKET, CHANNELS_BUCKET, COLONY_BUCKET, DEVICES_BUCKET, FILES_BUCKET,
    FILES_STREAM, HUMANS_BUCKET, IDENTITIES_BUCKET, MACHINES_BUCKET, NOTIFY_BUCKET, OWNERS_BUCKET, PRESENCE_BUCKET,
    PRESENCE_TTL, REACTIONS_BUCKET, READ_BUCKET, STORED_INBOX_DESTINATION, STORED_INBOX_SOURCE, STREAM,
    STREAM_SUBJECTS, TEAMS_BUCKET, THREADS_BUCKET, TRAIL_RATINGS_BUCKET, TRAIL_RUN_RETENTION, TRAIL_RUNS_BUCKET,
    TRAIL_TIPS_BUCKET, TRAILS_BUCKET, TrailLimits, WEBHOOK_RETENTION, WEBHOOKS_BUCKET, subject,
};
use async_nats::jetstream::{kv, stream};

use crate::bus::{DUPLICATE_WINDOW, RETENTION};

/// The `ANTS` stream as created.
pub fn stream_config() -> stream::Config {
    stream::Config {
        name: STREAM.to_owned(),
        subjects: STREAM_SUBJECTS.iter().map(|s| (*s).to_owned()).collect(),
        storage: stream::StorageType::File,
        max_age: RETENTION,
        duplicate_window: DUPLICATE_WINDOW,
        // An ant reads the decision stored on its approval requests with a direct get (`approvals`).
        allow_direct: true,
        republish: Some(stored_inbox()),
        max_consumers: MAX_CONSUMERS,
        ..Default::default()
    }
}

/// The consumers the `ANTS` stream takes. Each ant has two durable ones and up to about fifteen more while reading,
/// each human one for its notifications: room for several hundred ants all reading at once. The buckets every daemon
/// watches ([`super::Object::watched`]) take as many, room for thousands of daemons. Set explicitly, as nats-server gives a
/// stream without a limit of its own 1000 (its `default_max_consumers`), and -1 does not lift that default.
pub const MAX_CONSUMERS: i32 = 10_000;

/// How the `ANTS` stream republishes the inbox messages it stores, so a waiter hears those it sourced from a leaf's
/// outbox too.
pub fn stored_inbox() -> stream::Republish {
    stream::Republish {
        source: STORED_INBOX_SOURCE.to_owned(),
        destination: STORED_INBOX_DESTINATION.to_owned(),
        headers_only: false,
    }
}

/// The colony bucket ([`COLONY_BUCKET`]), whose entries expire `ttl` after their last put. An expiry leaves no marker:
/// nats-server places such markers only on a stream allowing rollups, which would let an ant remove every other ant's
/// entry. A watcher waiting for an entry to go reckons its expiry from its last put
/// ([`crate::colony::wait_for_turn`]).
pub fn colony_config(ttl: Duration) -> kv::Config {
    kv::Config {
        bucket: COLONY_BUCKET.to_owned(),
        description: "Ants being served, refreshed by daemon heartbeats".to_owned(),
        history: 1,
        max_age: ttl,
        // Entries are rebuilt by the next heartbeat, so they need not survive a server restart.
        storage: stream::StorageType::Memory,
        ..Default::default()
    }
}

/// A file-backed bucket whose entries expire with the stream's messages: sessions, attempts and links.
pub fn kept_config(bucket: &str) -> kv::Config {
    kv::Config {
        bucket: bucket.to_owned(),
        history: 1,
        max_age: RETENTION,
        storage: stream::StorageType::File,
        ..Default::default()
    }
}

/// A file-backed bucket whose entries never expire.
fn lasting_config(bucket: &str, description: &str) -> kv::Config {
    kv::Config {
        bucket: bucket.to_owned(),
        description: description.to_owned(),
        history: 1,
        storage: stream::StorageType::File,
        ..Default::default()
    }
}

/// The channels bucket ([`CHANNELS_BUCKET`]): a channel lasts until the human removes it.
pub fn channels_config() -> kv::Config {
    lasting_config(CHANNELS_BUCKET, "Channels: their members and messenger, written by the human")
}

/// The escalation chains bucket ([`CHAINS_BUCKET`]): a chain outlives its ant's colony entry.
pub fn chains_config() -> kv::Config {
    lasting_config(CHAINS_BUCKET, "Escalation chains of the ants, each written by its daemon")
}

/// The humans bucket ([`HUMANS_BUCKET`]): a human stays one until removed.
pub fn humans_config() -> kv::Config {
    lasting_config(HUMANS_BUCKET, "Humans of the team, written by humans")
}

/// The directory of human devices' encryption keys ([`DEVICES_BUCKET`]): a device's entry lasts until its CLI puts a
/// newer one, a revocation record for good.
pub fn devices_config() -> kv::Config {
    lasting_config(DEVICES_BUCKET, "User JWTs binding the encryption keys of human devices, and revoked keys")
}

/// The machines bucket ([`MACHINES_BUCKET`]): a machine's record outlives the colony entries of its ants.
pub fn machines_config() -> kv::Config {
    lasting_config(MACHINES_BUCKET, "The ants each machine's daemon serves, each record written by its machine")
}

/// The identities bucket ([`IDENTITIES_BUCKET`]): an ant's record lasts until `ants auth revoke` of the ant deletes it.
pub fn identities_config() -> kv::Config {
    lasting_config(IDENTITIES_BUCKET, "Each ant's identity: its id, birth, avatar and prompts, written by its daemon")
}

/// The stream of a trails' bucket: a KV bucket that keeps every message of each key, not only its last, and denies
/// rollups. A stream stores a message it or the server sent on any key ([`crate::bus::relayed`]), which in a bucket
/// of history 1 would replace the entry its writer put; kept, that entry is still read, past the relayed messages
/// ([`crate::records::direct_kv::DirectKv::past_relays`]), however many are relayed onto the key.
fn trails_bucket_config(bucket: &str, description: &str, max_age: Duration) -> stream::Config {
    stream::Config {
        name: subject::kv_stream(bucket),
        description: Some(description.to_owned()),
        subjects: vec![subject::kv_all(bucket)],
        // Unlimited, as a count: KV clients refuse to open a bucket of history below 1.
        max_messages_per_subject: i64::MAX,
        max_age,
        discard: stream::DiscardPolicy::New,
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        ..Default::default()
    }
}

/// The saved trails ([`TRAILS_BUCKET`]): a trail lasts until a human deletes it.
pub fn trails_config() -> stream::Config {
    trails_bucket_config(TRAILS_BUCKET, "Saved trails, written by humans and ants", Duration::ZERO)
}

/// The trail runs ([`TRAIL_RUNS_BUCKET`]): each message expires [`TRAIL_RUN_RETENTION`] and a run's most wall time
/// after it was put. A run's entries are put while it runs, which is never longer than that wall time, so those of a
/// finished run last [`TRAIL_RUN_RETENTION`] at least after its end, and none of a running one expires. A stream max
/// age needs no write to refresh old entries, as per-message TTLs would.
pub fn trail_runs_config() -> stream::Config {
    trails_bucket_config(
        TRAIL_RUNS_BUCKET,
        "Trail runs: each run's header, session and answered calls, written by its conductor",
        TRAIL_RUN_RETENTION + TrailLimits::MAX.wall_time(),
    )
}

/// The trails' tips and their votes ([`TRAIL_TIPS_BUCKET`]): a tip lasts until a human purges its trail's.
pub fn trail_tips_config() -> stream::Config {
    trails_bucket_config(TRAIL_TIPS_BUCKET, "Trail tips and their votes, each written by its ant", Duration::ZERO)
}

/// The trails' ratings ([`TRAIL_RATINGS_BUCKET`]): a rating lasts until a human purges its trail's.
pub fn trail_ratings_config() -> stream::Config {
    trails_bucket_config(TRAIL_RATINGS_BUCKET, "Trail ratings, each written by its run's conductor", Duration::ZERO)
}

/// The team directory ([`TEAMS_BUCKET`]): one key, every team, lasting until a human changes it.
pub fn teams_config() -> stream::Config {
    trails_bucket_config(TEAMS_BUCKET, "The team directory, written by humans and ants signer", Duration::ZERO)
}

/// The read markers of the humans ([`READ_BUCKET`]).
pub fn read_config() -> kv::Config {
    lasting_config(READ_BUCKET, "What each human has read of its inbox, on any of its devices")
}

/// The stream of the bucket of thread titles and each human's pinned and archived threads ([`THREADS_BUCKET`]): a KV
/// bucket of history 1 whose entries never expire, but which takes per-message TTLs, so the delete markers of titles
/// `ants thread list` prunes go too. It places no markers of its own when one expires, and denies rollups, which would
/// remove others' entries as well. It has room for [`MAX_CONSUMERS`], one for each ant's done states
/// ([`ants_proto::done_reader`]).
pub fn threads_config() -> stream::Config {
    stream::Config {
        name: subject::kv_stream(THREADS_BUCKET),
        description: Some("Thread titles, and the threads each human pinned or archived".into()),
        subjects: vec![subject::kv_all(THREADS_BUCKET)],
        max_messages_per_subject: 1,
        discard: stream::DiscardPolicy::New,
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        allow_message_ttl: true,
        max_consumers: MAX_CONSUMERS,
        ..Default::default()
    }
}

/// What each human holds back of its notifications ([`NOTIFY_BUCKET`]): entries last, their holds end at the deadlines
/// they carry.
pub fn notify_config() -> kv::Config {
    lasting_config(NOTIFY_BUCKET, "Each human's notification snooze and muted threads, on all its devices")
}

/// Where the devices of humans read ([`PRESENCE_BUCKET`]), whose entries expire [`PRESENCE_TTL`] after their last put.
pub fn presence_config() -> kv::Config {
    kv::Config {
        bucket: PRESENCE_BUCKET.to_owned(),
        description: "Devices of humans following their inbox or the tool requests, refreshed while they do".to_owned(),
        history: 1,
        max_age: PRESENCE_TTL,
        // Entries are rebuilt by the next heartbeat, so they need not survive a server restart.
        storage: stream::StorageType::Memory,
        ..Default::default()
    }
}

/// The webhooks bucket ([`WEBHOOKS_BUCKET`]), whose records expire after [`WEBHOOK_RETENTION`].
pub fn webhooks_config() -> kv::Config {
    kv::Config {
        bucket: WEBHOOKS_BUCKET.to_owned(),
        description: "Events each webhook delivered, so retries deliver nothing new, and the webhooks' markers"
            .to_owned(),
        history: 1,
        max_age: WEBHOOK_RETENTION,
        storage: stream::StorageType::File,
        ..Default::default()
    }
}

/// The stream of the owners bucket ([`OWNERS_BUCKET`]), which keeps the first record of each key, denies deletes and
/// purges, and drops records with the stream's messages. It drops, rather than refuses, a repeat of a record's message
/// id ([`ants_proto::owner_record_id`]) while the first is kept: a refused message stalls the source it came from, a
/// machine's outbox.
pub fn owners_config() -> stream::Config {
    stream::Config {
        name: crate::records::owners::owners_stream(),
        description: Some("The owner of each thread humans and webhooks message ants on, first record kept".into()),
        subjects: vec![subject::kv_all(OWNERS_BUCKET)],
        max_age: RETENTION,
        duplicate_window: RETENTION,
        max_messages_per_subject: 1,
        discard: stream::DiscardPolicy::New,
        discard_new_per_subject: true,
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        deny_purge: true,
        ..Default::default()
    }
}

/// The stream of the reactions bucket ([`REACTIONS_BUCKET`]): a KV bucket of history 1 whose stream denies deletes,
/// purges and rollups, which would remove others' reactions as well, and drops reactions with the stream's messages.
pub fn reactions_config() -> stream::Config {
    stream::Config {
        name: subject::kv_stream(REACTIONS_BUCKET),
        description: Some("Reactions to messages, each under its reactor's key".into()),
        subjects: vec![subject::kv_all(REACTIONS_BUCKET)],
        max_age: RETENTION,
        max_messages_per_subject: 1,
        discard: stream::DiscardPolicy::New,
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        deny_purge: true,
        ..Default::default()
    }
}

/// The stream of the attachments' store ([`FILES_BUCKET`]): the chunks of each attachment, on the subjects of its
/// handle ([`ants_proto::AttachmentHandle::chunk_subject`]), each stored once as its writer expects none before it,
/// and dropped with the stream's messages. It denies deletes and purges, which would remove others' attachments as
/// well, and serves direct gets, as ants read by subject without consumers. No object store metadata is kept: its
/// subjects, named after base64 names, could not be confined to their writers. Like the `ANTS` stream, it drops a copy
/// of a chunk a leaf's outbox kept ([`crate::files::put`]) as a duplicate of the chunk stored directly within a day,
/// rather than refuse it, which would stall the source it came from.
pub fn files_config() -> stream::Config {
    stream::Config {
        name: FILES_STREAM.to_owned(),
        description: Some("Attachments of messages, each chunk under its sender's subject".into()),
        subjects: vec![format!("$O.{FILES_BUCKET}.C.>")],
        max_age: RETENTION,
        duplicate_window: DUPLICATE_WINDOW,
        max_message_size: (ATTACHMENT_CHUNK + ATTACHMENT_HEADROOM) as i32,
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        deny_purge: true,
        ..Default::default()
    }
}
