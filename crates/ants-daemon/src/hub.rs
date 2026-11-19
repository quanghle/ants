//! The JetStream objects `ants init` sets up on the hub, the `ANTS` stream, the KV buckets and the attachments' store.
//! [`check`] finds those missing and [`apply`] creates one with this version's configuration; an object already there
//! is left as it is.
//!
//! An object whose stream info the credentials do not get, as the presence and identities buckets for every human's, cannot be
//! checked: [`init`] creates it all the same, which changes nothing when it exists with the same settings. Creating the
//! identities bucket also writes its incarnation marker when absent ([`mark_incarnation`]).

use std::fmt;
use std::time::Duration;

use ants_proto::{
    ATTACHMENT_CHUNK, ATTACHMENT_HEADROOM, ATTEMPTS_BUCKET, CHAINS_BUCKET, CHANNELS_BUCKET, COLONY_BUCKET, COLONY_TTL,
    DEVICES_BUCKET, FILES_BUCKET, FILES_STREAM, HUMANS_BUCKET, IDENTITIES_BUCKET, INCARNATION_KEY, LINKS_BUCKET,
    MACHINES_BUCKET, NOTIFY_BUCKET, OWNERS_BUCKET, PRESENCE_BUCKET, PRESENCE_TTL, REACTIONS_BUCKET, READ_BUCKET,
    SESSIONS_BUCKET, STORED_INBOX_DESTINATION, STORED_INBOX_SOURCE, STREAM, STREAM_SUBJECTS, THREADS_BUCKET,
    TRAIL_RATINGS_BUCKET, TRAIL_RUN_RETENTION, TRAIL_RUNS_BUCKET, TRAIL_TIPS_BUCKET, TRAILS_BUCKET, TrailLimits,
    WEBHOOK_RETENTION, WEBHOOKS_BUCKET,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind};
use async_nats::jetstream::stream::DirectGetErrorKind;
use async_nats::jetstream::{self, ErrorCode, kv, stream};

use crate::bus::{DUPLICATE_WINDOW, RETENTION};

/// A JetStream object `ants init` sets up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Object {
    /// The `ANTS` stream.
    Stream,
    Colony,
    Sessions,
    Attempts,
    Channels,
    Humans,
    Chains,
    Links,
    Owners,
    Webhooks,
    Reactions,
    Threads,
    Read,
    Presence,
    Notify,
    Devices,
    Machines,
    Identities,
    Trails,
    TrailRuns,
    TrailTips,
    TrailRatings,
    /// The attachments' store ([`FILES_BUCKET`]).
    Files,
}

impl Object {
    /// Every object, the stream first.
    pub const ALL: [Self; 23] = [
        Self::Stream,
        Self::Colony,
        Self::Sessions,
        Self::Attempts,
        Self::Channels,
        Self::Humans,
        Self::Chains,
        Self::Links,
        Self::Owners,
        Self::Webhooks,
        Self::Reactions,
        Self::Threads,
        Self::Read,
        Self::Presence,
        Self::Notify,
        Self::Devices,
        Self::Machines,
        Self::Identities,
        Self::Trails,
        Self::TrailRuns,
        Self::TrailTips,
        Self::TrailRatings,
        Self::Files,
    ];

    /// The KV bucket's name; None for the stream and the attachments' store.
    pub fn bucket(self) -> Option<&'static str> {
        Some(match self {
            Self::Stream | Self::Files => return None,
            Self::Colony => COLONY_BUCKET,
            Self::Sessions => SESSIONS_BUCKET,
            Self::Attempts => ATTEMPTS_BUCKET,
            Self::Channels => CHANNELS_BUCKET,
            Self::Humans => HUMANS_BUCKET,
            Self::Chains => CHAINS_BUCKET,
            Self::Links => LINKS_BUCKET,
            Self::Owners => OWNERS_BUCKET,
            Self::Webhooks => WEBHOOKS_BUCKET,
            Self::Reactions => REACTIONS_BUCKET,
            Self::Threads => THREADS_BUCKET,
            Self::Read => READ_BUCKET,
            Self::Presence => PRESENCE_BUCKET,
            Self::Notify => NOTIFY_BUCKET,
            Self::Devices => DEVICES_BUCKET,
            Self::Machines => MACHINES_BUCKET,
            Self::Identities => IDENTITIES_BUCKET,
            Self::Trails => TRAILS_BUCKET,
            Self::TrailRuns => TRAIL_RUNS_BUCKET,
            Self::TrailTips => TRAIL_TIPS_BUCKET,
            Self::TrailRatings => TRAIL_RATINGS_BUCKET,
        })
    }

    /// The name of its JetStream stream: `ANTS`, `KV_<bucket>`, or `OBJ_ants_files`.
    pub fn stream_name(self) -> String {
        match (self, self.bucket()) {
            (Self::Files, _) => FILES_STREAM.to_owned(),
            (_, Some(bucket)) => format!("KV_{bucket}"),
            (_, None) => STREAM.to_owned(),
        }
    }

    /// The JetStream API subject asking for its stream info, which only a human's credentials may ask of some.
    pub fn info_subject(self) -> String {
        format!("$JS.API.STREAM.INFO.{}", self.stream_name())
    }

    fn spec(self) -> Spec {
        match self {
            Self::Stream => Spec::Stream(Box::new(stream_config())),
            Self::Owners => Spec::Stream(Box::new(owners_config())),
            Self::Reactions => Spec::Stream(Box::new(reactions_config())),
            Self::Files => Spec::Stream(Box::new(files_config())),
            Self::Colony => Spec::Bucket(Box::new(colony_config(COLONY_TTL))),
            Self::Sessions | Self::Attempts | Self::Links => {
                Spec::Bucket(Box::new(kept_config(self.bucket().unwrap_or_default())))
            }
            Self::Channels => Spec::Bucket(Box::new(channels_config())),
            Self::Humans => Spec::Bucket(Box::new(humans_config())),
            Self::Chains => Spec::Bucket(Box::new(chains_config())),
            Self::Webhooks => Spec::Bucket(Box::new(webhooks_config())),
            Self::Threads => Spec::Stream(Box::new(threads_config())),
            Self::Read => Spec::Bucket(Box::new(read_config())),
            Self::Presence => Spec::Bucket(Box::new(presence_config())),
            Self::Notify => Spec::Bucket(Box::new(notify_config())),
            Self::Devices => Spec::Bucket(Box::new(devices_config())),
            Self::Machines => Spec::Bucket(Box::new(machines_config())),
            Self::Identities => Spec::Bucket(Box::new(identities_config())),
            Self::Trails => Spec::Bucket(Box::new(trails_config())),
            Self::TrailRuns => Spec::Bucket(Box::new(trail_runs_config())),
            Self::TrailTips => Spec::Bucket(Box::new(trail_tips_config())),
            Self::TrailRatings => Spec::Bucket(Box::new(trail_ratings_config())),
        }
    }

    /// Whether a bucket is watched by every daemon, channel manager or standby daemon, each watch an ordered consumer of
    /// its stream: the colony (each standby daemon's entry), the channels and the humans; or, the threads, read by
    /// every ant's durable consumer of the done states ([`ants_proto::done_reader`]). Such a bucket needs room for
    /// [`MAX_CONSUMERS`] too.
    pub fn watched(self) -> bool {
        matches!(self, Self::Colony | Self::Channels | Self::Humans | Self::Threads)
    }

    async fn check(self, js: &jetstream::Context) -> Option<Problem> {
        match js.get_stream(self.stream_name()).await {
            Ok(_) => None,
            Err(e) if not_found(&e) => Some(Problem::Missing),
            Err(e) => Some(Problem::Unchecked(e.to_string())),
        }
    }

    /// Create the object. A watched bucket's room for [`MAX_CONSUMERS`], which a KV configuration cannot hold, follows
    /// by an update of its stream; the identities bucket's incarnation marker, when absent, by a write
    /// ([`mark_incarnation`]).
    async fn create(self, js: &jetstream::Context) -> Result<()> {
        let store = match self.spec() {
            Spec::Stream(config) => {
                return js.create_stream(*config).await.map(drop).with_context(|| format!("creating {self}"));
            }
            Spec::Bucket(config) => js.create_key_value(*config).await.with_context(|| format!("creating {self}"))?,
        };
        if self == Self::Identities {
            mark_incarnation(&store, false).await?;
        }
        if self.watched() {
            let mut config = store.stream.cached_info().config.clone();
            config.max_consumers = MAX_CONSUMERS;
            js.update_stream(&config)
                .await
                .with_context(|| format!("giving {self} room for {MAX_CONSUMERS} consumers"))?;
        }
        Ok(())
    }
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self, self.bucket()) {
            (Self::Files, _) => write!(f, "store {FILES_BUCKET}"),
            (_, Some(bucket)) => write!(f, "bucket {bucket}"),
            (_, None) => write!(f, "stream {STREAM}"),
        }
    }
}

enum Spec {
    Stream(Box<stream::Config>),
    Bucket(Box<kv::Config>),
}

fn not_found(e: &GetStreamError) -> bool {
    matches!(e.kind(), GetStreamErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND)
}

/// What is wrong with an object on the hub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    Missing,
    /// Its stream info could not be had, for this reason.
    Unchecked(String),
}

/// An object [`check`] found missing or impossible to check.
#[derive(Debug, Clone)]
pub struct Finding {
    pub object: Object,
    pub problem: Problem,
}

impl Finding {
    /// The JetStream API subjects [`apply`] publishes to: credentials lacking any cannot fix it.
    pub fn rights(&self) -> Vec<String> {
        let name = self.object.stream_name();
        match (&self.problem, self.object.spec()) {
            (Problem::Missing, Spec::Stream(_)) => vec![format!("$JS.API.STREAM.CREATE.{name}")],
            // Creating a bucket first looks up the account's JetStream limits.
            (Problem::Missing, Spec::Bucket(_)) => {
                let mut rights = vec!["$JS.API.INFO".to_owned(), format!("$JS.API.STREAM.CREATE.{name}")];
                if self.object.watched() {
                    rights.push(format!("$JS.API.STREAM.UPDATE.{name}"));
                }
                rights
            }
            (Problem::Unchecked(_), _) => Vec::new(),
        }
    }

    /// What [`apply`] did about it, as `created stream ANTS`.
    pub fn fixed(&self) -> String {
        format!("created {}", self.object)
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let object = self.object;
        match &self.problem {
            Problem::Missing => write!(f, "{object} is missing"),
            Problem::Unchecked(e) => write!(f, "{object} could not be checked: {e}"),
        }
    }
}

/// Look up the hub's objects whose stream info `may` says the credentials get (by its JetStream API subject,
/// [`Object::info_subject`]), all at once: one round trip. Returns the objects missing or impossible to check, in the
/// order of [`Object::ALL`].
pub async fn check(js: &jetstream::Context, may: impl Fn(&str) -> bool) -> Vec<Finding> {
    let checks = Object::ALL
        .into_iter()
        .filter(|object| may(&object.info_subject()))
        .map(async |object| object.check(js).await.map(|problem| Finding { object, problem }));
    futures::future::join_all(checks).await.into_iter().flatten().collect()
}

/// Fix what `finding` found: create the missing object.
pub async fn apply(js: &jetstream::Context, finding: &Finding) -> Result<()> {
    let object = finding.object;
    match &finding.problem {
        Problem::Missing => Box::pin(object.create(js)).await,
        Problem::Unchecked(e) => bail!("{object} could not be checked: {e}"),
    }
}

/// `ants init`: check every object and create each one found missing, failing at once when `may` says the credentials
/// may not (by JetStream API subject), as denied requests would only time out. An object whose stream info they do not
/// get is created all the same, which changes nothing if it is there with the same settings. Returns what it created
/// among the objects checked.
pub async fn init(js: &jetstream::Context, may: impl Fn(&str) -> bool) -> Result<Vec<Finding>> {
    for object in Object::ALL.into_iter().filter(|object| !may(&object.info_subject())) {
        let unseen = Finding { object, problem: Problem::Missing };
        if let Some(denied) = unseen.rights().into_iter().find(|subject| !may(subject)) {
            bail!("the credentials may neither look up nor create {object} ({denied})");
        }
        object.create(js).await?;
    }
    let findings = check(js, &may).await;
    for finding in &findings {
        if let Some(denied) = finding.rights().into_iter().find(|subject| !may(subject)) {
            bail!("{finding}, and the credentials may not create it ({denied})");
        }
        apply(js, finding).await?;
    }
    Ok(findings)
}

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
/// watches ([`Object::watched`]) take as many, room for thousands of daemons. Set explicitly, as nats-server gives a
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

/// The colony bucket ([`COLONY_BUCKET`]), whose entries expire `ttl` after their last put, each leaving a marker kept
/// as long, so watchers see it go.
pub fn colony_config(ttl: Duration) -> kv::Config {
    kv::Config {
        bucket: COLONY_BUCKET.to_owned(),
        description: "Ants being served, refreshed by daemon heartbeats".to_owned(),
        history: 1,
        max_age: ttl,
        limit_markers: Some(ttl),
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

/// Create the identities bucket ([`IDENTITIES_BUCKET`]) as `ants init` does: with its incarnation marker.
pub async fn create_identities(js: &jetstream::Context) -> Result<()> {
    Object::Identities.create(js).await
}

/// Start a new incarnation of the identities bucket ([`IDENTITIES_BUCKET`]), creating it if missing: write its marker
/// anew, so that no shared settings record signed before verifies any more ([`ants_proto::incarnation`]), as after
/// restoring the bucket from a backup (`ants init --identities-restored`).
pub async fn new_incarnation(js: &jetstream::Context) -> Result<()> {
    let store = js.create_key_value(identities_config()).await.context("creating bucket ants_identities")?;
    mark_incarnation(&store, true).await
}

/// Write the incarnation marker of the identities bucket ([`INCARNATION_KEY`]) when it has none (also when deleted),
/// or, `anew`, in any case. Read by a direct get naming no subject, which a human's credentials allow.
async fn mark_incarnation(identities: &kv::Store, anew: bool) -> Result<()> {
    let value = chrono::Utc::now().to_rfc3339().into();
    let written = if anew {
        identities.put(INCARNATION_KEY, value).await.map(drop).map_err(anyhow::Error::from)
    } else {
        let subject = format!("$KV.{IDENTITIES_BUCKET}.{INCARNATION_KEY}");
        let last = match identities.stream.direct_get_next_for_subject(subject, None).await {
            Ok(msg) if !msg.headers.get("KV-Operation").is_some_and(|op| ["DEL", "PURGE"].contains(&op.as_str())) => {
                return Ok(());
            }
            Ok(deleted) => deleted.sequence,
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => 0,
            Err(e) => return Err(e).context("reading the incarnation marker of bucket ants_identities"),
        };
        match identities.update(INCARNATION_KEY, value, last).await {
            Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => Ok(()),
            written => written.map(drop).map_err(anyhow::Error::from),
        }
    };
    written.context("writing the incarnation marker of bucket ants_identities")
}

/// The saved trails ([`TRAILS_BUCKET`]): a trail lasts until a human deletes it.
pub fn trails_config() -> kv::Config {
    lasting_config(TRAILS_BUCKET, "Saved trails, written by humans")
}

/// The trail runs ([`TRAIL_RUNS_BUCKET`]): each entry expires [`TRAIL_RUN_RETENTION`] and a run's most wall time after
/// its last put. A run's entries are put while it runs, which is never longer than that wall time, so those of a
/// finished run last [`TRAIL_RUN_RETENTION`] at least after its end, and none of a running one expires. A bucket max
/// age needs no write to refresh old entries, as per-message TTLs would.
pub fn trail_runs_config() -> kv::Config {
    kv::Config {
        bucket: TRAIL_RUNS_BUCKET.to_owned(),
        description: "Trail runs: each run's header, session and answered calls, written by its conductor".to_owned(),
        history: 1,
        max_age: TRAIL_RUN_RETENTION + TrailLimits::MAX.wall_time(),
        storage: stream::StorageType::File,
        ..Default::default()
    }
}

/// The trails' tips and their votes ([`TRAIL_TIPS_BUCKET`]): a tip lasts until a human purges its trail's.
pub fn trail_tips_config() -> kv::Config {
    lasting_config(TRAIL_TIPS_BUCKET, "Trail tips and their votes, each written by its ant")
}

/// The trails' ratings ([`TRAIL_RATINGS_BUCKET`]): a rating lasts until a human purges its trail's.
pub fn trail_ratings_config() -> kv::Config {
    lasting_config(TRAIL_RATINGS_BUCKET, "Trail ratings, each written by its run's conductor")
}

/// The read markers of the humans ([`READ_BUCKET`]).
pub fn read_config() -> kv::Config {
    lasting_config(READ_BUCKET, "What each human has read of its inbox, on any of its devices")
}

/// The stream of the bucket of thread titles and each human's pinned and archived threads ([`THREADS_BUCKET`]): a KV
/// bucket of history 1 whose entries never expire, but which takes per-message TTLs, so the delete markers of titles
/// `ants thread list` prunes go too. It places no markers of its own when one expires. It has room for
/// [`MAX_CONSUMERS`], one for each ant's done states ([`ants_proto::done_reader`]).
pub fn threads_config() -> stream::Config {
    stream::Config {
        name: format!("KV_{THREADS_BUCKET}"),
        description: Some("Thread titles, and the threads each human pinned or archived".into()),
        subjects: vec![format!("$KV.{THREADS_BUCKET}.>")],
        max_messages_per_subject: 1,
        discard: stream::DiscardPolicy::New,
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        allow_rollup: true,
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
        subjects: vec![format!("$KV.{OWNERS_BUCKET}.>")],
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
        name: format!("KV_{REACTIONS_BUCKET}"),
        description: Some("Reactions to messages, each under its reactor's key".into()),
        subjects: vec![format!("$KV.{REACTIONS_BUCKET}.>")],
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

#[cfg(test)]
mod tests {
    use ants_proto::{UserId, subject};
    use ants_test_support::NatsServer;

    use super::*;
    use crate::bus;

    fn problems(findings: &[Finding]) -> Vec<Object> {
        let missing = |f: &Finding| match &f.problem {
            Problem::Missing => f.object,
            Problem::Unchecked(e) => panic!("{e}"),
        };
        findings.iter().map(missing).collect()
    }

    #[tokio::test]
    async fn init_creates_what_is_missing_once() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        assert_eq!(problems(&check(&js, |_| true).await), Object::ALL);
        assert!(check(&js, |s| s == Object::Read.info_subject()).await.iter().all(|f| f.object == Object::Read));
        let read_create = "$JS.API.STREAM.CREATE.KV_ants_read";
        let unseen = init(&js, |s| s != Object::Read.info_subject() && s != read_create).await.unwrap_err();
        assert_eq!(
            unseen.to_string(),
            format!("the credentials may neither look up nor create bucket ants_read ({read_create})")
        );
        assert_eq!(check(&js, |_| true).await.len(), Object::ALL.len(), "nothing created");
        let denied = init(&js, |s| s != read_create).await.unwrap_err();
        assert_eq!(
            denied.to_string(),
            format!("bucket ants_read is missing, and the credentials may not create it ({read_create})")
        );
        let left = [
            Object::Read,
            Object::Presence,
            Object::Notify,
            Object::Devices,
            Object::Machines,
            Object::Identities,
            Object::Trails,
            Object::TrailRuns,
            Object::TrailTips,
            Object::TrailRatings,
            Object::Files,
        ];
        assert_eq!(problems(&check(&js, |_| true).await), left, "the rest is created");
        // As a human's credentials, which get no stream info of the presence and identities buckets but may create them.
        let human = |s: &str| s != Object::Presence.info_subject() && s != Object::Identities.info_subject();
        assert_eq!(
            problems(&init(&js, human).await.unwrap()),
            [
                Object::Read,
                Object::Notify,
                Object::Devices,
                Object::Machines,
                Object::Trails,
                Object::TrailRuns,
                Object::TrailTips,
                Object::TrailRatings,
                Object::Files
            ]
        );
        assert!(init(&js, human).await.unwrap().is_empty(), "creating those buckets again changes nothing");
        assert!(check(&js, |_| true).await.is_empty(), "all there");
        for object in Object::ALL.into_iter().filter(|o| o.bucket().is_some() && *o != Object::Owners) {
            let kv = js.get_key_value(object.bucket().unwrap()).await.unwrap();
            let max = kv.stream.cached_info().config.max_consumers;
            assert_eq!(max == MAX_CONSUMERS, object.watched(), "{object} created with room for {max} consumers");
        }
        assert!(init(&js, |_| true).await.unwrap().is_empty());
    }

    /// Creating the identities bucket marks its incarnation, which a creation again leaves and a deleted marker or
    /// `ants init --identities-restored` replaces; a bucket created otherwise has none.
    #[tokio::test]
    async fn the_identities_bucket_carries_an_incarnation_marker_that_init_writes() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let identities = crate::records::direct_kv::DirectKv::new(&js, None, IDENTITIES_BUCKET).await.unwrap();
        let incarnation = || crate::profile::shared::incarnation(&identities);

        js.create_key_value(identities_config()).await.unwrap();
        assert_eq!(incarnation().await.unwrap(), None, "as `nats kv add` creates it");
        create_identities(&js).await.unwrap();
        let first = incarnation().await.unwrap().expect("marked");
        create_identities(&js).await.unwrap();
        init(&js, |s| s != Object::Identities.info_subject()).await.unwrap();
        assert_eq!(incarnation().await.unwrap().as_ref(), Some(&first), "left as it is");

        new_incarnation(&js).await.unwrap();
        let restored = incarnation().await.unwrap().expect("marked");
        assert_ne!(restored, first);
        js.get_key_value(IDENTITIES_BUCKET).await.unwrap().delete(INCARNATION_KEY).await.unwrap();
        assert_eq!(incarnation().await.unwrap(), None);
        create_identities(&js).await.unwrap();
        let again = incarnation().await.unwrap().expect("marked again");
        assert!(![&first, &restored].contains(&&again));
    }

    /// An object already there stays as it is, such as the sources `ants leaf outbox` adds to the stream.
    #[tokio::test]
    async fn init_leaves_existing_objects_as_they_are() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        js.create_stream(stream::Config {
            name: "OUTBOX".into(),
            subjects: vec!["out.>".into()],
            ..Default::default()
        })
        .await
        .unwrap();
        let source = stream::Source { name: "OUTBOX".into(), ..Default::default() };
        js.create_stream(stream::Config { sources: Some(vec![source]), ..stream_config() }).await.unwrap();
        js.create_key_value(channels_config()).await.unwrap();

        let created = init(&js, |_| true).await.unwrap();
        assert!(created.iter().all(|f| f.object != Object::Stream && f.object != Object::Channels));
        assert_eq!(created.len(), Object::ALL.len() - 2);
        let config = js.get_stream(STREAM).await.unwrap().cached_info().config.clone();
        assert_eq!(config.sources.unwrap()[0].name, "OUTBOX", "sources stay");
        let channels = js.get_stream(Object::Channels.stream_name()).await.unwrap().cached_info().config.clone();
        assert_ne!(channels.max_consumers, MAX_CONSUMERS, "not changed");
    }

    /// A copy kept in a leaf's outbox can land long after its twin stored directly: the stream drops it for a day.
    #[tokio::test]
    async fn the_stream_drops_repeated_message_ids_for_a_day() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let stream = bus::ensure_stream(&js).await.unwrap();
        assert_eq!(stream.cached_info().config.duplicate_window, DUPLICATE_WINDOW);

        let publish = async |id: &str| {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(async_nats::header::NATS_MESSAGE_ID, id);
            let subject = subject::inbox(&"dev-a".parse().unwrap(), &UserId::human());
            js.publish_with_headers(subject, headers, "{}".into()).await.unwrap().await.unwrap()
        };
        let first = publish("m-1").await;
        let again = publish("m-1").await;
        assert!(again.duplicate && again.sequence == first.sequence);
    }
}
