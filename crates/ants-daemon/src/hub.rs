//! The JetStream objects `ants init` sets up on the hub, the `ANTS` stream, the KV buckets and the attachments' store.
//! [`check`] finds those missing or allowing rollups and [`apply`] creates one with this version's configuration
//! (`configs`), or denies its rollups; an object already there is otherwise left as it is.
//!
//! Every object denies rollups: a message with a `Nats-Rollup` header removes every other message of its subject or of
//! the whole stream, so a writer allowed one key of a bucket could otherwise remove the entries of every other writer.
//! A KV bucket is created allowing them, as async-nats creates every bucket, and denies them by a stream update right
//! after.
//!
//! An object whose stream info the credentials do not get, as the presence and identities buckets for every human's, cannot be checked:
//! [`init`] creates it all the same, which changes nothing when it exists as this version makes it, and denies the
//! rollups of one created allowing them. Creating the identities bucket also writes its incarnation marker when absent
//! (`mark_incarnation`), as does `ants init` for one already there as this version makes it if the credentials get
//! its stream info.

mod configs;

use std::fmt;
use std::time::Duration;

use ants_proto::{
    ATTEMPTS_BUCKET, CHAINS_BUCKET, CHANNELS_BUCKET, COLONY_BUCKET, COLONY_TTL, DEVICES_BUCKET, FILES_BUCKET,
    FILES_STREAM, HUMANS_BUCKET, IDENTITIES_BUCKET, INCARNATION_KEY, JsDomain, LINKS_BUCKET, MACHINES_BUCKET,
    NOTIFY_BUCKET, OWNERS_BUCKET, PRESENCE_BUCKET, REACTIONS_BUCKET, READ_BUCKET, SESSIONS_BUCKET, STREAM,
    TEAMS_BUCKET, THREADS_BUCKET, TRAIL_RATINGS_BUCKET, TRAIL_RUNS_BUCKET, TRAIL_TIPS_BUCKET, TRAILS_BUCKET,
    WEBHOOKS_BUCKET, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::context::{
    CreateKeyValueError, CreateStreamError, CreateStreamErrorKind, GetStreamError, GetStreamErrorKind,
};
use async_nats::jetstream::stream::DirectGetErrorKind;
use async_nats::jetstream::{self, ErrorCode, kv, stream};

use crate::records::direct_kv::DirectKv;

pub use configs::{
    MAX_CONSUMERS, chains_config, channels_config, colony_config, devices_config, files_config, humans_config,
    identities_config, kept_config, machines_config, notify_config, owners_config, presence_config, reactions_config,
    read_config, stored_inbox, stream_config, teams_config, threads_config, trail_ratings_config, trail_runs_config,
    trail_tips_config, trails_config, webhooks_config,
};

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
    Teams,
    /// The attachments' store ([`FILES_BUCKET`]).
    Files,
}

impl Object {
    /// Every object, the stream first.
    pub const ALL: [Self; 24] = [
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
        Self::Teams,
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
            Self::Teams => TEAMS_BUCKET,
        })
    }

    /// The name of its JetStream stream: `ANTS`, `KV_<bucket>`, or `OBJ_ants_files`.
    pub fn stream_name(self) -> String {
        match (self, self.bucket()) {
            (Self::Files, _) => FILES_STREAM.to_owned(),
            (_, Some(bucket)) => subject::kv_stream(bucket),
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
            Self::Trails => Spec::Stream(Box::new(trails_config())),
            Self::TrailRuns => Spec::Stream(Box::new(trail_runs_config())),
            Self::TrailTips => Spec::Stream(Box::new(trail_tips_config())),
            Self::TrailRatings => Spec::Stream(Box::new(trail_ratings_config())),
            Self::Teams => Spec::Stream(Box::new(teams_config())),
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
            Ok(stream) if stream.cached_info().config.allow_rollup => Some(Problem::AllowsRollups),
            Ok(_) => None,
            Err(e) if not_found(&e) => Some(Problem::Missing),
            Err(e) => Some(Problem::Unchecked(e.to_string())),
        }
    }

    /// Create the object. A bucket's denial of rollups and a watched bucket's room for [`MAX_CONSUMERS`], which a KV
    /// configuration cannot hold, follow by an update of its stream; the identities bucket's incarnation marker, when
    /// absent, by a write ([`mark_incarnation`]). A bucket already there as this version leaves it, which differs from
    /// its KV configuration, stays as it is, but for the identities bucket's marker when `seen`, the credentials getting
    /// its stream info.
    async fn create(self, js: &jetstream::Context, seen: bool) -> Result<()> {
        let store = match self.spec() {
            Spec::Stream(config) => {
                return js.create_stream(*config).await.map(drop).with_context(|| format!("creating {self}"));
            }
            Spec::Bucket(config) => match js.create_key_value(*config).await {
                Ok(store) => store,
                Err(e) if exists_otherwise(&e) => {
                    if let Some(bucket) = self.bucket().filter(|_| self == Self::Identities && seen) {
                        let store = js.get_key_value(bucket).await.with_context(|| format!("looking up {self}"))?;
                        mark_incarnation(&store).await?;
                    }
                    return Ok(());
                }
                Err(e) => return Err(e).with_context(|| format!("creating {self}")),
            },
        };
        if self == Self::Identities {
            mark_incarnation(&store).await?;
        }
        self.settle(js, &store).await
    }

    /// Deny the rollups of bucket `store`, just created as this object, and give a watched one room for
    /// [`MAX_CONSUMERS`], which its KV configuration cannot say.
    async fn settle(self, js: &jetstream::Context, store: &kv::Store) -> Result<()> {
        let mut config = store.stream.cached_info().config.clone();
        config.allow_rollup = false;
        if self.watched() {
            config.max_consumers = MAX_CONSUMERS;
        }
        js.update_stream(&config).await.with_context(|| format!("updating {self}"))?;
        Ok(())
    }

    /// Deny the rollups of the object, its other settings left as they are.
    async fn deny_rollups(self, js: &jetstream::Context) -> Result<()> {
        let stream = js.get_stream(self.stream_name()).await.with_context(|| format!("looking up {self}"))?;
        let mut config = stream.cached_info().config.clone();
        config.allow_rollup = false;
        js.update_stream(&config).await.with_context(|| format!("denying the rollups of {self}"))?;
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

/// Whether creating a bucket failed for its stream being there with other settings.
fn exists_otherwise(e: &CreateKeyValueError) -> bool {
    let source = std::error::Error::source(e).and_then(|e| e.downcast_ref::<CreateStreamError>());
    matches!(source.map(CreateStreamError::kind), Some(CreateStreamErrorKind::JetStream(e)) if e.error_code() == ErrorCode::STREAM_NAME_EXIST)
}

/// What is wrong with an object on the hub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    Missing,
    /// Its stream allows rollups: one message with a `Nats-Rollup` header removes the others of its subject or stream.
    AllowsRollups,
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
            // Creating a bucket first looks up the account's JetStream limits; its rollups are denied by an update.
            (Problem::Missing, Spec::Bucket(_)) => vec![
                "$JS.API.INFO".to_owned(),
                format!("$JS.API.STREAM.CREATE.{name}"),
                format!("$JS.API.STREAM.UPDATE.{name}"),
            ],
            (Problem::AllowsRollups, _) => {
                vec![format!("$JS.API.STREAM.INFO.{name}"), format!("$JS.API.STREAM.UPDATE.{name}")]
            }
            (Problem::Unchecked(_), _) => Vec::new(),
        }
    }

    /// What [`apply`] did about it, as `created stream ANTS`.
    pub fn fixed(&self) -> String {
        match self.problem {
            Problem::AllowsRollups => format!("denied the rollups of {}", self.object),
            _ => format!("created {}", self.object),
        }
    }
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let object = self.object;
        match &self.problem {
            Problem::Missing => write!(f, "{object} is missing"),
            Problem::AllowsRollups => write!(
                f,
                "{object} allows rollups: one message with a Nats-Rollup header removes the entries of every other \
                 writer"
            ),
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

/// Fix what `finding` found: create the missing object, or deny its rollups.
pub async fn apply(js: &jetstream::Context, finding: &Finding) -> Result<()> {
    let object = finding.object;
    match &finding.problem {
        Problem::Missing => Box::pin(object.create(js, true)).await,
        Problem::AllowsRollups => Box::pin(object.deny_rollups(js)).await,
        Problem::Unchecked(e) => bail!("{object} could not be checked: {e}"),
    }
}

/// `ants init`: check every object, create each one found missing and deny the rollups of each allowing them, failing at
/// once when `may` says the credentials may not (by JetStream API subject), as denied requests would only time out. An
/// object whose stream info they do not get is created all the same, which changes nothing if it is there as this
/// version makes it. Returns what it fixed among the objects checked.
pub async fn init(js: &jetstream::Context, may: impl Fn(&str) -> bool) -> Result<Vec<Finding>> {
    for object in Object::ALL.into_iter().filter(|object| !may(&object.info_subject())) {
        let unseen = Finding { object, problem: Problem::Missing };
        if let Some(denied) = unseen.rights().into_iter().find(|subject| !may(subject)) {
            bail!("the credentials may neither look up nor create {object} ({denied})");
        }
        object.create(js, false).await?;
    }
    let findings = check(js, &may).await;
    for finding in &findings {
        if let Some(denied) = finding.rights().into_iter().find(|subject| !may(subject)) {
            let fix = if finding.problem == Problem::AllowsRollups { "update" } else { "create" };
            bail!("{finding}, and the credentials may not {fix} it ({denied})");
        }
        apply(js, finding).await?;
    }
    Ok(findings)
}

/// Create the identities bucket ([`IDENTITIES_BUCKET`]) as `ants init` does: with its incarnation marker.
pub async fn create_identities(js: &jetstream::Context) -> Result<()> {
    Object::Identities.create(js, true).await
}

/// Start a new incarnation of the identities bucket ([`IDENTITIES_BUCKET`]), creating it if missing: write its marker
/// anew, so that no shared settings record signed before verifies any more ([`ants_proto::incarnation`]), as after
/// restoring the bucket from a backup (`ants init --identities-restored`). `js_domain` is the domain `js` was made for.
pub async fn new_incarnation(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<()> {
    Box::pin(Object::Identities.create(js, false)).await?;
    let identities = DirectKv::new(js, js_domain, IDENTITIES_BUCKET).await?;
    let value = chrono::Utc::now().to_rfc3339();
    identities.put(INCARNATION_KEY, value).await.context("writing the incarnation marker of bucket ants_identities")
}

/// Create the KV bucket of `object` as [`init`] does for credentials that do not get its stream info: one already there
/// as this version leaves it, its rollups denied, stays as it is, and one there allowing them gets them denied.
pub async fn ensure_unseen_bucket(js: &jetstream::Context, object: Object) -> Result<()> {
    if object.bucket().is_none() {
        bail!("{object} is no KV bucket");
    }
    Box::pin(object.create(js, false)).await
}

/// The buckets held in memory, whose entries their writers' next heartbeats put again: a restart of nats-server drops
/// them, and nothing an ant or a web page holds may create them again ([`ensure_in_memory`]).
pub const IN_MEMORY: [Object; 2] = [Object::Colony, Object::Presence];

/// Create each bucket of [`IN_MEMORY`] that is missing, as [`ensure_unseen_bucket`] does, so that the heartbeats of
/// daemons, CLIs and web pages are stored again after a restart of nats-server: what a human's CLI does when its
/// presence cannot be put, and `ants signer` on the hub at every beat. One already there costs two requests and stays
/// as it is. Fails with the first error, after trying each.
pub async fn ensure_in_memory(js: &jetstream::Context) -> Result<()> {
    let mut first = None;
    for object in IN_MEMORY {
        if let Err(e) = ensure_unseen_bucket(js, object).await {
            first.get_or_insert(e);
        }
    }
    first.map_or(Ok(()), Err)
}

/// Create the colony bucket as `ants init` does, but with entries expiring `ttl` after their last put.
pub async fn create_colony(js: &jetstream::Context, ttl: Duration) -> Result<kv::Store> {
    let store = js.create_key_value(colony_config(ttl)).await.context("creating bucket ants_colony")?;
    Object::Colony.settle(js, &store).await?;
    Ok(store)
}

/// The KV bucket of `object`, created as `ants init` does when missing. One JetStream does not answer for, as through
/// a leaf node whose uplink is down, is not missing: the error says so ([`crate::bus::unanswered`]).
pub async fn ensure_bucket(js: &jetstream::Context, object: Object) -> Result<kv::Store> {
    let bucket = object.bucket().with_context(|| format!("{object} is no KV bucket"))?;
    match js.get_key_value(bucket).await {
        Ok(store) => return Ok(store),
        Err(e) => {
            let e = anyhow::Error::from(e);
            if crate::bus::unanswered(&e) {
                return Err(e.context(format!("looking up {object}: JetStream did not answer")));
            }
        }
    }
    Box::pin(object.create(js, true)).await?;
    js.get_key_value(bucket).await.with_context(|| format!("looking up {object}"))
}

/// Write the incarnation marker of the identities bucket ([`INCARNATION_KEY`]) when it has none (also when deleted).
/// Read by a direct get naming no subject, which a human's credentials allow.
async fn mark_incarnation(identities: &kv::Store) -> Result<()> {
    let value = chrono::Utc::now().to_rfc3339().into();
    let subject = subject::identities_incarnation();
    let last = match identities.stream.direct_get_next_for_subject(subject, None).await {
        Ok(msg) if !msg.headers.get("KV-Operation").is_some_and(|op| ["DEL", "PURGE"].contains(&op.as_str())) => {
            return Ok(());
        }
        Ok(deleted) => deleted.sequence,
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => 0,
        Err(e) => return Err(e).context("reading the incarnation marker of bucket ants_identities"),
    };
    let written = match identities.update(INCARNATION_KEY, value, last).await {
        Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => Ok(()),
        written => written.map(drop).map_err(anyhow::Error::from),
    };
    written.context("writing the incarnation marker of bucket ants_identities")
}

#[cfg(test)]
mod tests {
    use ants_proto::subject;
    use ants_test_support::NatsServer;

    use super::*;
    use crate::bus::{self, DUPLICATE_WINDOW};

    fn problems(findings: &[Finding]) -> Vec<Object> {
        let missing = |f: &Finding| match &f.problem {
            Problem::Missing => f.object,
            Problem::AllowsRollups => panic!("{f}"),
            Problem::Unchecked(e) => panic!("{e}"),
        };
        findings.iter().map(missing).collect()
    }

    #[tokio::test]
    async fn init_creates_what_is_missing_once() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
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
            Object::Teams,
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
                Object::Teams,
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
        for object in Object::ALL {
            let config = js.get_stream(object.stream_name()).await.unwrap().cached_info().config.clone();
            assert!(!config.allow_rollup, "{object} denies rollups");
            assert_eq!(config.subject_delete_marker_ttl, None, "{object} places no expiry markers");
        }
        assert!(init(&js, |_| true).await.unwrap().is_empty());
    }

    /// A rollup to one key of a bucket would remove every other key's entry: each bucket refuses it, the colony's too.
    #[tokio::test]
    async fn every_bucket_refuses_a_rollup_and_keeps_the_entries_of_others() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
        init(&js, |_| true).await.unwrap();
        for object in Object::ALL.into_iter().filter(|o| o.bucket().is_some()) {
            let prefix = format!("$KV.{}", object.bucket().unwrap());
            let before = js.get_stream(object.stream_name()).await.unwrap().cached_info().state.messages;
            for key in ["a.a.a", "b.b.b"] {
                js.publish(format!("{prefix}.{key}"), "{}".into()).await.unwrap().await.unwrap();
            }
            for rollup in ["all", "sub"] {
                let mut headers = async_nats::HeaderMap::new();
                headers.insert("Nats-Rollup", rollup);
                let sent = js.publish_with_headers(format!("{prefix}.a.a.a"), headers, "".into()).await.unwrap();
                assert!(sent.await.is_err(), "{object} refuses a rollup of {rollup}");
            }
            let state = js.get_stream(object.stream_name()).await.unwrap().cached_info().state.clone();
            assert_eq!(state.messages, before + 2, "{object} keeps every entry");
        }
    }

    /// Buckets created allowing rollups, as by `nats kv add`, are found and fixed by `ants init`,
    /// their entries and other settings kept, also those whose stream info the credentials do not get.
    #[tokio::test]
    async fn init_denies_the_rollups_of_buckets_created_allowing_them() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
        let allowing_rollups = [Object::TrailTips, Object::Presence, Object::Humans];
        for object in allowing_rollups {
            match object.spec() {
                Spec::Bucket(config) => drop(js.create_key_value(*config).await.unwrap()),
                Spec::Stream(config) => {
                    drop(js.create_stream(stream::Config { allow_rollup: true, ..*config }).await.unwrap());
                }
            }
            js.get_key_value(object.bucket().unwrap()).await.unwrap().put("a.b", "{}".into()).await.unwrap();
        }
        js.create_stream(stream::Config { allow_rollup: true, ..threads_config() }).await.unwrap();
        let found = check(&js, |_| true).await;
        let allowing: Vec<_> = found.iter().filter(|f| f.problem == Problem::AllowsRollups).map(|f| f.object).collect();
        assert_eq!(allowing, [Object::Humans, Object::Threads, Object::Presence, Object::TrailTips]);
        let tips = found.iter().find(|f| f.object == Object::TrailTips).unwrap();
        assert_eq!(
            tips.to_string(),
            "bucket ants_trail_tips allows rollups: one message with a Nats-Rollup header removes the entries of every \
             other writer"
        );
        assert_eq!(tips.fixed(), "denied the rollups of bucket ants_trail_tips");
        assert_eq!(
            tips.rights(),
            ["$JS.API.STREAM.INFO.KV_ants_trail_tips", "$JS.API.STREAM.UPDATE.KV_ants_trail_tips"]
        );

        let denied = init(&js, |s| s != "$JS.API.STREAM.UPDATE.KV_ants_humans").await.unwrap_err();
        assert!(denied.to_string().starts_with("bucket ants_humans allows rollups"), "{denied}");
        let human = |s: &str| s != Object::Presence.info_subject();
        let fixed = init(&js, human).await.unwrap();
        let fixed: Vec<_> = fixed.iter().filter(|f| f.problem == Problem::AllowsRollups).map(|f| f.object).collect();
        assert_eq!(fixed, [Object::Humans, Object::Threads, Object::TrailTips], "presence is fixed unseen");
        assert!(check(&js, |_| true).await.is_empty(), "{:?}", check(&js, |_| true).await);
        for object in allowing_rollups {
            let kv = js.get_key_value(object.bucket().unwrap()).await.unwrap();
            assert!(!kv.stream.cached_info().config.allow_rollup);
            assert_eq!(kv.get("a.b").await.unwrap().as_deref(), Some(&b"{}"[..]), "{object} keeps its entries");
        }
        let humans = js.get_stream(Object::Humans.stream_name()).await.unwrap().cached_info().config.clone();
        assert_ne!(humans.max_consumers, MAX_CONSUMERS, "other settings stay");
    }

    /// Creating the identities bucket marks its incarnation, which a creation again leaves and a deleted marker or
    /// `ants init --identities-restored` replaces; a bucket created otherwise has none.
    #[tokio::test]
    async fn the_identities_bucket_carries_an_incarnation_marker_that_init_writes() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
        let identities = crate::records::direct_kv::DirectKv::new(&js, None, IDENTITIES_BUCKET).await.unwrap();
        let incarnation = || crate::profile::shared::incarnation(&identities);

        js.create_key_value(identities_config()).await.unwrap();
        assert_eq!(incarnation().await.unwrap(), None, "as `nats kv add` creates it");
        create_identities(&js).await.unwrap();
        let first = incarnation().await.unwrap().expect("marked");
        create_identities(&js).await.unwrap();
        init(&js, |s| s != Object::Identities.info_subject()).await.unwrap();
        assert_eq!(incarnation().await.unwrap().as_ref(), Some(&first), "left as it is");

        new_incarnation(&js, None).await.unwrap();
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
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
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
        assert!(created.iter().all(|f| f.object != Object::Stream));
        let channels: Vec<_> = created.iter().filter(|f| f.object == Object::Channels).map(|f| &f.problem).collect();
        assert_eq!(channels, [&Problem::AllowsRollups], "only its rollups denied");
        assert_eq!(created.len(), Object::ALL.len() - 1);
        let config = js.get_stream(STREAM).await.unwrap().cached_info().config.clone();
        assert_eq!(config.sources.unwrap()[0].name, "OUTBOX", "sources stay");
        let channels = js.get_stream(Object::Channels.stream_name()).await.unwrap().cached_info().config.clone();
        assert_ne!(channels.max_consumers, MAX_CONSUMERS, "not changed");
    }

    /// A copy kept in a leaf's outbox can land long after its twin stored directly: the stream drops it for a day.
    #[tokio::test]
    async fn the_stream_drops_repeated_message_ids_for_a_day() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
        let stream = bus::ensure_stream(&js).await.unwrap();
        assert_eq!(stream.cached_info().config.duplicate_window, DUPLICATE_WINDOW);

        let publish = async |id: &str| {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(async_nats::header::NATS_MESSAGE_ID, id);
            let subject = subject::inbox(&"dev-a".parse().unwrap(), &ants_test_support::human());
            js.publish_with_headers(subject, headers, "{}".into()).await.unwrap().await.unwrap()
        };
        let first = publish("m-1").await;
        let again = publish("m-1").await;
        assert!(again.duplicate && again.sequence == first.sequence);
    }
}
