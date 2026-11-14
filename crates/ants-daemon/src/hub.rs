//! The JetStream objects `ants init` sets up on the hub, the `ANTS` stream and the KV buckets, each as this version
//! expects it. [`check`] compares the hub's objects with them and [`apply`] creates what is missing and upgrades what
//! is out of date, changing only the settings `ants init` owns: the rest of a stream's configuration, such as the
//! sources `ants leaf outbox` adds, stays as it is.
//!
//! An object whose stream info the credentials do not get, as the presence bucket for every human's, cannot be
//! checked: [`init`] creates it all the same, which changes nothing when it exists with the same settings.

use std::fmt;
use std::time::Duration;

use ants_proto::{
    ATTEMPTS_BUCKET, CHAINS_BUCKET, CHANNELS_BUCKET, HUMANS_BUCKET, LINKS_BUCKET, OWNERS_BUCKET, PRESENCE_BUCKET,
    PRESENCE_TTL, REACTIONS_BUCKET, READ_BUCKET, ROSTER_BUCKET, ROSTER_TTL, SESSIONS_BUCKET, STORED_INBOX_DESTINATION,
    STORED_INBOX_SOURCE, STREAM, STREAM_SUBJECTS, THREADS_BUCKET, WEBHOOK_RETENTION, WEBHOOKS_BUCKET,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::context::{
    CreateKeyValueError, CreateKeyValueErrorKind, GetStreamError, GetStreamErrorKind,
};
use async_nats::jetstream::{self, ErrorCode, kv, stream};

use crate::bus::{DUPLICATE_WINDOW, RETENTION};

/// A JetStream object `ants init` sets up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Object {
    /// The `ANTS` stream.
    Stream,
    Roster,
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
}

impl Object {
    /// Every object, the stream first.
    pub const ALL: [Self; 14] = [
        Self::Stream,
        Self::Roster,
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
    ];

    /// The bucket's name; None for the stream.
    pub fn bucket(self) -> Option<&'static str> {
        Some(match self {
            Self::Stream => return None,
            Self::Roster => ROSTER_BUCKET,
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
        })
    }

    /// The name of its JetStream stream: `ANTS`, or `KV_<bucket>`.
    pub fn stream_name(self) -> String {
        match self.bucket() {
            Some(bucket) => format!("KV_{bucket}"),
            None => STREAM.to_owned(),
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
            Self::Roster => Spec::Bucket(Box::new(roster_config(ROSTER_TTL))),
            Self::Sessions | Self::Attempts | Self::Links => {
                Spec::Bucket(Box::new(kept_config(self.bucket().unwrap_or_default())))
            }
            Self::Channels => Spec::Bucket(Box::new(channels_config())),
            Self::Humans => Spec::Bucket(Box::new(humans_config())),
            Self::Chains => Spec::Bucket(Box::new(chains_config())),
            Self::Webhooks => Spec::Bucket(Box::new(webhooks_config())),
            Self::Threads => Spec::Bucket(Box::new(threads_config())),
            Self::Read => Spec::Bucket(Box::new(read_config())),
            Self::Presence => Spec::Bucket(Box::new(presence_config())),
        }
    }

    /// Whether a bucket is watched by every daemon, channel manager or standby daemon, each watch an ordered consumer of
    /// its stream: the roster (each standby daemon's entry), the channels and the humans. Such a bucket needs room for
    /// [`MAX_CONSUMERS`] too.
    pub fn watched(self) -> bool {
        matches!(self, Self::Roster | Self::Channels | Self::Humans)
    }

    /// Bring `config`, the hub's configuration of the object, up to date in what `ants init` owns of it, and say what
    /// was out of date.
    fn upgrade(self, config: &mut stream::Config) -> Vec<Drift> {
        let mut drift = match self {
            Self::Stream => return upgrade_stream(config),
            Self::Owners if crate::owners::first_records_only(config) => vec![Drift::FirstRecordsOnly],
            Self::Roster if add_limit_markers(config) => vec![Drift::LimitMarkers],
            _ => Vec::new(),
        };
        if self.watched() {
            drift.extend(raise_max_consumers(config));
        }
        drift
    }

    async fn check(self, js: &jetstream::Context) -> Option<Problem> {
        match js.get_stream(self.stream_name()).await {
            Ok(stream) => {
                let mut config = stream.cached_info().config.clone();
                let drift = self.upgrade(&mut config);
                (!drift.is_empty()).then_some(Problem::Outdated { drift })
            }
            Err(e) if not_found(&e) => Some(Problem::Missing),
            Err(e) => Some(Problem::Unchecked(e.to_string())),
        }
    }

    /// Create the object; a bucket's settings that a KV configuration cannot hold, such as room for more consumers,
    /// follow by an update of its stream.
    async fn create(self, js: &jetstream::Context) -> Result<()> {
        let config = match self.spec() {
            Spec::Stream(config) => {
                return js.create_stream(*config).await.map(drop).with_context(|| format!("creating {self}"));
            }
            Spec::Bucket(config) => create_bucket(js, *config).await.with_context(|| format!("creating {self}"))?,
        };
        let mut config = config.stream.cached_info().config.clone();
        if !self.upgrade(&mut config).is_empty() {
            js.update_stream(&config).await.with_context(|| format!("upgrading {self}"))?;
        }
        Ok(())
    }
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.bucket() {
            Some(bucket) => write!(f, "bucket {bucket}"),
            None => write!(f, "stream {STREAM}"),
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

/// What of an object's configuration `ants init` would change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drift {
    /// The stream captures none of these subjects of [`STREAM_SUBJECTS`].
    Subjects(Vec<String>),
    /// The stream answers no direct gets.
    DirectGets,
    /// The stream does not republish inbox messages it stores ([`stored_inbox`]).
    Republish,
    /// The stream drops repeated message ids for less than this long ([`duplicate_window`]).
    DuplicateWindow(Duration),
    /// The owners bucket does not keep one record per key, dropping a repeat of its message id for as long as it
    /// keeps the first ([`crate::owners::first_records_only`]): its duplicate window, among others.
    FirstRecordsOnly,
    /// The stream takes fewer consumers than [`MAX_CONSUMERS`].
    MaxConsumers(i32),
    /// The roster bucket leaves no marker when an entry expires ([`add_limit_markers`]).
    LimitMarkers,
}

impl fmt::Display for Drift {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Subjects(subjects) => write!(f, "subjects {}", subjects.join(", ")),
            Self::DirectGets => write!(f, "direct gets"),
            Self::Republish => write!(f, "the republishing of stored inbox messages"),
            Self::DuplicateWindow(window) => {
                write!(f, "a duplicate window of {}", ants_proto::duration::format(*window))
            }
            Self::FirstRecordsOnly => write!(f, "a duplicate window as long as its retention, keeping first records"),
            Self::MaxConsumers(max) => write!(f, "room for {max} consumers"),
            Self::LimitMarkers => write!(f, "markers of expired entries"),
        }
    }
}

/// What is wrong with an object on the hub.
#[derive(Debug, Clone)]
pub enum Problem {
    Missing,
    /// It lacks `drift`.
    Outdated {
        drift: Vec<Drift>,
    },
    /// Its stream info could not be had, for this reason.
    Unchecked(String),
}

/// An object [`check`] found missing, out of date or impossible to check.
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
            (Problem::Outdated { .. }, _) => vec![format!("$JS.API.STREAM.UPDATE.{name}")],
            (Problem::Unchecked(_), _) => Vec::new(),
        }
    }

    /// What [`apply`] did about it.
    pub fn fixed(&self) -> String {
        match &self.problem {
            Problem::Missing => format!("created {}", self.object),
            Problem::Outdated { drift, .. } => format!("upgraded {}, adding {}", self.object, list(drift)),
            Problem::Unchecked(_) => format!("{self}"),
        }
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let object = self.object;
        match &self.problem {
            Problem::Missing => write!(f, "{object} is missing"),
            Problem::Outdated { drift, .. } => write!(f, "{object} lacks {}", list(drift)),
            Problem::Unchecked(e) => write!(f, "{object} could not be checked: {e}"),
        }
    }
}

fn list(drift: &[Drift]) -> String {
    drift.iter().map(ToString::to_string).collect::<Vec<_>>().join("; ")
}

/// Compare the hub's objects with what this version expects, those whose stream info `may` says the credentials
/// get (by its JetStream API subject, [`Object::info_subject`]), all at once: one round trip. Returns the objects
/// missing, out of date or impossible to check, in the order of [`Object::ALL`].
pub async fn check(js: &jetstream::Context, may: impl Fn(&str) -> bool) -> Vec<Finding> {
    let checks = Object::ALL
        .into_iter()
        .filter(|object| may(&object.info_subject()))
        .map(async |object| object.check(js).await.map(|problem| Finding { object, problem }));
    futures::future::join_all(checks).await.into_iter().flatten().collect()
}

/// Fix what `finding` found: create the missing object, or upgrade its configuration. The upgrade starts from the
/// configuration read again just before, so a change made since the check, such as a source `ants leaf outbox` added,
/// is kept; one made between that read and the update, a round trip later, would be lost.
pub async fn apply(js: &jetstream::Context, finding: &Finding) -> Result<()> {
    let object = finding.object;
    match &finding.problem {
        Problem::Missing => Box::pin(object.create(js)).await,
        Problem::Outdated { .. } => {
            let stream = js.get_stream(object.stream_name()).await.with_context(|| format!("looking up {object}"))?;
            let mut config = stream.cached_info().config.clone();
            if !object.upgrade(&mut config).is_empty() {
                js.update_stream(&config).await.with_context(|| format!("upgrading {object}"))?;
            }
            Ok(())
        }
        Problem::Unchecked(e) => bail!("{object} could not be checked: {e}"),
    }
}

/// `ants init`: check every object and fix each one found missing or out of date, failing at once when `may` says
/// the credentials may not (by JetStream API subject), as denied requests would only time out. An object whose stream
/// info they do not get is created all the same, which changes nothing if it is there with the same settings. Returns
/// what it fixed among the objects checked.
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
            bail!("{finding}, and the credentials may not fix it ({denied})");
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
/// watches ([`Object::watched`]) take as many, room for thousands of daemons. Set explicitly, as since nats-server
/// 2.15 a stream without a limit of its own takes 1000 (its `default_max_consumers`), and -1 does not lift that
/// default.
pub const MAX_CONSUMERS: i32 = 10_000;

/// Give `config`, a stream's, room for [`MAX_CONSUMERS`] if it takes fewer, keeping a higher limit; the drift, if any.
fn raise_max_consumers(config: &mut stream::Config) -> Option<Drift> {
    (config.max_consumers < MAX_CONSUMERS).then(|| {
        config.max_consumers = MAX_CONSUMERS;
        Drift::MaxConsumers(MAX_CONSUMERS)
    })
}

/// The duplicate window the `ANTS` stream configured as `config` should have: [`DUPLICATE_WINDOW`], or its retention
/// if shorter, as JetStream allows no window longer than that.
pub fn duplicate_window(config: &stream::Config) -> Duration {
    match config.max_age {
        Duration::ZERO => DUPLICATE_WINDOW,
        max_age => DUPLICATE_WINDOW.min(max_age),
    }
}

/// How the `ANTS` stream republishes the inbox messages it stores, so a waiter hears those it sourced from a leaf's
/// outbox too.
pub fn stored_inbox() -> stream::Republish {
    stream::Republish {
        source: STORED_INBOX_SOURCE.to_owned(),
        destination: STORED_INBOX_DESTINATION.to_owned(),
        headers_only: false,
    }
}

/// Add to `config`, the `ANTS` stream's, the subjects of [`STREAM_SUBJECTS`] it lacks, direct gets, the
/// republishing of stored inbox messages, a [`duplicate_window`] as long as it should be and room for
/// [`MAX_CONSUMERS`], keeping its other subjects and settings, a longer window and more room too. Returns what it
/// lacked.
pub fn upgrade_stream(config: &mut stream::Config) -> Vec<Drift> {
    let mut drift = Vec::new();
    let missing = crate::bus::missing_subjects(&config.subjects);
    if !missing.is_empty() {
        config.subjects.extend(missing.iter().cloned());
        drift.push(Drift::Subjects(missing));
    }
    if !config.allow_direct {
        config.allow_direct = true;
        drift.push(Drift::DirectGets);
    }
    if config.republish.as_ref() != Some(&stored_inbox()) {
        config.republish = Some(stored_inbox());
        drift.push(Drift::Republish);
    }
    let window = duplicate_window(config);
    if config.duplicate_window < window {
        config.duplicate_window = window;
        drift.push(Drift::DuplicateWindow(window));
    }
    drift.extend(raise_max_consumers(config));
    drift
}

/// Whether the hub answering with `config`, a stream's, runs nats-server 2.11 or later (JetStream API level 1),
/// which can leave a marker when an entry expires. Such a server names its level in the metadata of every stream
/// info; an older one names none.
pub fn supports_limit_markers(config: &stream::Config) -> bool {
    let level = config.metadata.get("_nats.level").and_then(|level| level.parse::<u32>().ok());
    level.is_some_and(|level| level >= 1)
}

/// Whether `config`, a bucket's, leaves a marker when an entry expires.
pub fn has_limit_markers(config: &stream::Config) -> bool {
    config.allow_message_ttl && config.subject_delete_marker_ttl.is_some()
}

/// Make `config`, a bucket's, leave a marker when an entry expires, kept as long as an entry lives (at least the
/// second the server allows), if the hub supports it ([`supports_limit_markers`]) and the bucket leaves none yet:
/// then watchers see the expiry as a purge. Returns whether it changed `config`. Once on, the server does not let it
/// be turned off.
pub fn add_limit_markers(config: &mut stream::Config) -> bool {
    if has_limit_markers(config) || !supports_limit_markers(config) {
        return false;
    }
    config.allow_message_ttl = true;
    config.subject_delete_marker_ttl = Some(config.max_age.max(Duration::from_secs(1)));
    true
}

/// Create the bucket configured as `config`, without the markers of expired entries if the hub runs a nats-server
/// older than 2.11, which leaves none.
pub async fn create_bucket(js: &jetstream::Context, mut config: kv::Config) -> Result<kv::Store, CreateKeyValueError> {
    match js.create_key_value(config.clone()).await {
        Err(e) if e.kind() == CreateKeyValueErrorKind::LimitMarkersNotSupported => {
            tracing::debug!(
                bucket = config.bucket,
                "the hub runs nats-server older than 2.11: no markers of expired entries"
            );
            config.limit_markers = None;
            js.create_key_value(config).await
        }
        created => created,
    }
}

/// The roster bucket ([`ROSTER_BUCKET`]), whose entries expire `ttl` after their last put, each leaving a marker kept
/// as long, so watchers see it go.
pub fn roster_config(ttl: Duration) -> kv::Config {
    kv::Config {
        bucket: ROSTER_BUCKET.to_owned(),
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

/// The escalation chains bucket ([`CHAINS_BUCKET`]): a chain outlives its ant's roster entry.
pub fn chains_config() -> kv::Config {
    lasting_config(CHAINS_BUCKET, "Escalation chains of the ants, each written by its daemon")
}

/// The humans bucket ([`HUMANS_BUCKET`]): a human stays one until removed.
pub fn humans_config() -> kv::Config {
    lasting_config(HUMANS_BUCKET, "Humans of the team, written by humans")
}

/// The read markers of the humans ([`READ_BUCKET`]).
pub fn read_config() -> kv::Config {
    lasting_config(READ_BUCKET, "What each human has read of its inbox, on any of its devices")
}

/// Thread titles and each human's pinned and archived threads ([`THREADS_BUCKET`]).
pub fn threads_config() -> kv::Config {
    lasting_config(THREADS_BUCKET, "Thread titles, and the threads each human pinned or archived")
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

/// The stream of the owners bucket ([`OWNERS_BUCKET`]), which keeps the first record of each key
/// ([`crate::owners::first_records_only`]), denies deletes and purges, and drops records with the stream's messages.
pub fn owners_config() -> stream::Config {
    let mut config = stream::Config {
        name: crate::owners::owners_stream(),
        description: Some("The owner of each thread humans and webhooks message ants on, first record kept".into()),
        subjects: vec![format!("$KV.{OWNERS_BUCKET}.>")],
        max_age: RETENTION,
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        deny_purge: true,
        ..Default::default()
    };
    crate::owners::first_records_only(&mut config);
    config
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

#[cfg(test)]
mod tests {
    use ants_proto::{UserId, subject};
    use ants_test_support::NatsServer;

    use super::*;
    use crate::bus;

    fn problems(findings: &[Finding]) -> Vec<(Object, Option<Vec<Drift>>)> {
        let drift = |f: &Finding| match &f.problem {
            Problem::Missing => None,
            Problem::Outdated { drift, .. } => Some(drift.clone()),
            Problem::Unchecked(e) => panic!("{e}"),
        };
        findings.iter().map(|f| (f.object, drift(f))).collect()
    }

    #[tokio::test]
    async fn init_creates_what_is_missing_once() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let missing: Vec<_> = Object::ALL.into_iter().map(|o| (o, None)).collect();
        assert_eq!(problems(&check(&js, |_| true).await), missing);
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
            format!("bucket ants_read is missing, and the credentials may not fix it ({read_create})")
        );
        let left = [(Object::Read, None), (Object::Presence, None)];
        assert_eq!(problems(&check(&js, |_| true).await), left, "the rest is created");
        // As a human's credentials, which get no stream info of the presence bucket but may create it.
        let human = |s: &str| s != Object::Presence.info_subject();
        assert_eq!(problems(&init(&js, human).await.unwrap()), [(Object::Read, None)]);
        assert!(init(&js, human).await.unwrap().is_empty(), "creating the presence bucket again changes nothing");
        assert!(check(&js, |_| true).await.is_empty(), "all there and up to date");
        for object in Object::ALL.into_iter().filter(|o| *o != Object::Stream && *o != Object::Owners) {
            let kv = js.get_key_value(object.bucket().unwrap()).await.unwrap();
            let max = kv.stream.cached_info().config.max_consumers;
            assert_eq!(max == MAX_CONSUMERS, object.watched(), "{object} created with room for {max} consumers");
        }
        assert!(init(&js, |_| true).await.unwrap().is_empty());
    }

    /// Only what `ants init` owns changes: other subjects, sources and settings stay.
    #[tokio::test]
    async fn an_outdated_stream_and_buckets_are_upgraded_keeping_the_rest() {
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
        let old: Vec<String> =
            STREAM_SUBJECTS.iter().filter(|s| **s != subject::DEAD_ALL).map(|s| (*s).to_owned()).collect();
        js.create_stream(stream::Config {
            name: STREAM.to_owned(),
            subjects: [old.clone(), vec!["ants.plan.>".into()]].concat(),
            max_age: Duration::from_secs(3600),
            sources: Some(vec![source]),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut owners = owners_config();
        owners.duplicate_window = Duration::from_secs(120);
        js.create_stream(owners).await.unwrap();
        let roster =
            js.create_key_value(kv::Config { limit_markers: None, ..roster_config(ROSTER_TTL) }).await.unwrap();
        assert!(!has_limit_markers(&roster.stream.cached_info().config), "as created before");

        let findings = check(&js, |_| true).await;
        let stream_drift = vec![
            Drift::Subjects(vec![subject::DEAD_ALL.to_owned()]),
            Drift::DirectGets,
            Drift::Republish,
            Drift::DuplicateWindow(Duration::from_secs(3600)),
            Drift::MaxConsumers(MAX_CONSUMERS),
        ];
        let outdated: Vec<_> = problems(&findings).into_iter().filter(|(_, drift)| drift.is_some()).collect();
        assert_eq!(
            outdated,
            [
                (Object::Stream, Some(stream_drift)),
                (Object::Roster, Some(vec![Drift::LimitMarkers, Drift::MaxConsumers(MAX_CONSUMERS)])),
                (Object::Owners, Some(vec![Drift::FirstRecordsOnly]))
            ]
        );
        assert_eq!(
            findings[0].to_string(),
            "stream ANTS lacks subjects ants.dead.>; direct gets; the republishing of stored inbox messages; a \
             duplicate window of 1h; room for 10000 consumers"
        );
        assert_eq!(
            findings[1].fixed(),
            "upgraded bucket ants_roster, adding markers of expired entries; room for 10000 consumers"
        );
        for finding in &findings {
            apply(&js, finding).await.unwrap();
        }
        assert!(check(&js, |_| true).await.is_empty());

        let config = js.get_stream(STREAM).await.unwrap().cached_info().config.clone();
        assert!(bus::missing_subjects(&config.subjects).is_empty());
        assert!(config.subjects.iter().any(|s| s == "ants.plan.>"), "older subjects stay");
        assert_eq!(config.sources.unwrap()[0].name, "OUTBOX", "sources stay");
        assert_eq!(config.max_age, Duration::from_secs(3600), "and the retention");
        assert_eq!(config.duplicate_window, Duration::from_secs(3600), "no longer than the retention");
        assert!(config.allow_direct && config.republish == Some(stored_inbox()));
        assert_eq!(config.max_consumers, MAX_CONSUMERS);
        let roster = js.get_stream(Object::Roster.stream_name()).await.unwrap().cached_info().config.clone();
        assert!(has_limit_markers(&roster), "added to the existing bucket");
        assert_eq!((roster.max_age, roster.subject_delete_marker_ttl), (ROSTER_TTL, Some(ROSTER_TTL)));
        assert_eq!(roster.max_consumers, MAX_CONSUMERS);
        let mut more = roster;
        more.max_consumers = MAX_CONSUMERS * 2;
        js.update_stream(&more).await.unwrap();
        let mut more = js.get_stream(STREAM).await.unwrap().cached_info().config.clone();
        more.max_consumers = MAX_CONSUMERS * 2;
        js.update_stream(&more).await.unwrap();
        assert!(check(&js, |_| true).await.is_empty(), "room for more consumers stays");
        let owners = js.get_stream(crate::owners::owners_stream()).await.unwrap().cached_info().config.clone();
        assert_eq!(owners.duplicate_window, RETENTION);
    }

    /// A copy kept in a leaf's outbox can land long after its twin stored directly: the stream drops it for a day.
    #[tokio::test]
    async fn the_stream_drops_repeated_message_ids_for_a_day() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let window = |stream: &stream::Stream| stream.cached_info().config.duplicate_window;
        let stream = bus::init_stream(&js).await.unwrap();
        assert_eq!(window(&stream), DUPLICATE_WINDOW, "created with it");
        js.delete_stream(STREAM).await.unwrap();

        // As created before, with JetStream's default window of two minutes.
        let mut old = stream_config();
        old.duplicate_window = Duration::ZERO;
        let old = js.create_stream(old).await.unwrap();
        assert_eq!(window(&old), Duration::from_secs(120));
        assert_eq!(
            problems(&check(&js, |_| true).await)[0],
            (Object::Stream, Some(vec![Drift::DuplicateWindow(DUPLICATE_WINDOW)]))
        );
        let stream = bus::init_stream(&js).await.unwrap();
        assert_eq!(window(&stream), DUPLICATE_WINDOW, "upgraded");
        assert_eq!(js.get_stream(STREAM).await.unwrap().cached_info().config.duplicate_window, DUPLICATE_WINDOW);

        let publish = async |id: &str| {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(async_nats::header::NATS_MESSAGE_ID, id);
            let subject = subject::inbox(&"dev-a".parse().unwrap(), &UserId::human());
            js.publish_with_headers(subject, headers, "{}".into()).await.unwrap().await.unwrap()
        };
        let first = publish("m-1").await;
        let again = publish("m-1").await;
        assert!(again.duplicate && again.sequence == first.sequence);

        let mut longer = js.get_stream(STREAM).await.unwrap().cached_info().config.clone();
        longer.duplicate_window = DUPLICATE_WINDOW * 2;
        js.update_stream(&longer).await.unwrap();
        assert!(check(&js, |_| true).await.iter().all(|f| f.object != Object::Stream), "a longer window stays");
    }
}
