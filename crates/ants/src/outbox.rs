//! `ants leaf outbox`: store and forward between a machine's leaf node and the hub.
//!
//! A message the hub's stream cannot take while the leaf's uplink is down waits in the leaf's own JetStream, in the
//! machine's outbox ([`OUTBOX_STREAM`]), and reaches the hub's stream in two hops once the uplink is back:
//!
//! 1. The hub's intake ([`INTAKE_STREAM`]) sources each machine's outbox through the machine's JetStream domain and
//!    stores everything under the machine's prefix ([`subject::intake`]), so a machine cannot pass for another.
//! 2. The hub's [`STREAM`] sources from the intake, per machine and user (ant or human), only the subjects the user
//!    may publish itself ([`Machine::admitted`]), back on their usual subjects. Anything else a machine claims stays in
//!    the intake.
//!
//! The links to channel threads the machine's ants pass on ([`LINKS_BUCKET`]) take the same way, the links bucket
//! sourcing from the intake, per ant, only the keys the ant may write itself ([`Machine::admitted_links`]); so do the
//! owners its humans record for the threads they message ants on ([`OWNERS_BUCKET`]), the owners bucket sourcing them
//! from the intake only when a human runs on the machine ([`Machine::admitted_owners`]). The owners bucket keeps every
//! record it sources, a thread's first staying its owner: a record it refused would stall its source.
//!
//! A machine's credentials thus never decide what the hub's stream stores: only a human, who sets up both hops here,
//! does. Each message keeps its `Nats-Msg-Id`, so the hub's stream drops a buffered copy of a message it already took
//! within its duplicate window, a day ([`ants_daemon::bus::DUPLICATE_WINDOW`]). Only the hub's stream needs that
//! window: a message stored directly never passes through the outbox or the intake, so its twins meet there.

use std::path::Path;
use std::time::Duration;

use ants_daemon::owners;
use ants_proto::{
    INTAKE_STREAM, LINKS_BUCKET, Machine, OUTBOX_STREAM, OUTBOX_SUBJECTS, OWNERS_BUCKET, Role, STREAM, UserId, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind};
use async_nats::jetstream::stream::{self, External, Source, SubjectTransform};
use async_nats::jetstream::{self, Context};

/// How long an outbox and the intake keep messages: as long as the hub's stream.
const RETENTION: Duration = Duration::from_hours(30 * 24);
/// How long to wait for the leaf node to answer through the hub.
const LEAF_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the hub may take to hear from an outbox it sources: a source that works is active within a second.
pub const SOURCE_WAIT: Duration = Duration::from_secs(10);

/// Connect as `human` to set up the outbox of `machine`, with the machine's own inbox prefix
/// ([`Machine::setup_inbox`]): the leaf node may answer there and nowhere else.
pub async fn connect(url: &str, creds: Option<&Path>, human: &UserId, machine: &Machine) -> Result<async_nats::Client> {
    ants_daemon::bus::check_url(url)?;
    let mut options =
        async_nats::ConnectOptions::new().name("ants-cli").custom_inbox_prefix(machine.setup_inbox(human));
    if let Some(path) = creds {
        options = ants_daemon::bus::authenticate(options, path, ants_daemon::keystore::os())?;
    }
    options.connect(url).await.with_context(|| format!("connecting to NATS at {url}"))
}

/// Set up store and forward for `machine`, on which `members` run, ants and humans each in its role
/// ([`crate::auth::members`]): create or update the machine's outbox on its leaf node (which must be connected),
/// through `leaf` from [`connect`], and have the hub's stream take from it what those members may send, the links
/// bucket the links its ants pass on, and the owners bucket the owners its humans record, once `ants init` created
/// them. Admissions of members no longer listed are dropped; those already in place are kept as they are.
pub async fn admit(
    leaf: &async_nats::Client,
    hub: &Context,
    machine: &Machine,
    members: &[(UserId, Role)],
) -> Result<()> {
    anyhow::ensure!(!members.is_empty(), "a machine needs at least one ant or human");
    let mut leaf = jetstream::with_domain(leaf.clone(), machine.as_str());
    leaf.set_timeout(LEAF_TIMEOUT);
    leaf.create_or_update_stream(outbox_config()).await.with_context(|| {
        format!("setting up the outbox on the leaf node of {machine} (it must be up, with JetStream, and connected)")
    })?;

    // The hub's stream takes from the intake before the intake takes from the outbox, so nothing passes unseen.
    let start = match hub.get_stream(INTAKE_STREAM).await {
        Ok(mut intake) => intake.info().await?.state.last_sequence + 1,
        Err(e) if not_found(&e) => 1,
        Err(e) => return Err(e).context("looking up the hub's intake"),
    };
    let mut ants = hub.get_stream(STREAM).await.context("looking up the hub's stream (`ants init` creates it)")?;
    let mut config = ants.info().await?.config.clone();
    admit_into(&mut config, machine, admissions(machine, members, start));
    hub.update_stream(&config).await.context("admitting the machine's outbox into the hub's stream")?;
    if let Some(mut config) = bucket_config(hub, LINKS_BUCKET).await? {
        admit_into(&mut config, machine, link_admissions(machine, members, start));
        hub.update_stream(&config).await.context("admitting the machine's outbox into the links bucket")?;
    }
    if let Some(mut config) = bucket_config(hub, OWNERS_BUCKET).await? {
        owners::first_records_only(&mut config);
        admit_into(&mut config, machine, owner_admissions(machine, members, start));
        hub.update_stream(&config).await.context("admitting the machine's outbox into the owners bucket")?;
    }

    let mut intake = intake_config(hub).await?;
    let sources = intake.sources.get_or_insert_default();
    if !sources.iter().any(|s| outbox_of(s, machine)) {
        sources.push(intake_source(machine));
    }
    hub.create_or_update_stream(intake).await.context("sourcing the machine's outbox into the hub's intake")?;
    Ok(())
}

/// Admit `ant`, new to `machine`, into the hub's stream from the machine's outbox, and into the links bucket once
/// `ants init` created it, as [`admit`] would with the ant listed too, leaving the other admissions as they are; for
/// `ants enroll approve`, which connects through `hub` with [`Permissions::for_enroll_approver`]. Returns false,
/// changing nothing, when the hub's intake does not source the machine's outbox (`ants leaf outbox` never ran for it).
///
/// [`Permissions::for_enroll_approver`]: ants_proto::Permissions::for_enroll_approver
pub async fn admit_ant(hub: &Context, machine: &Machine, ant: &UserId) -> Result<bool> {
    let mut intake = match hub.get_stream(INTAKE_STREAM).await {
        Ok(intake) => intake,
        Err(e) if not_found(&e) => return Ok(false),
        Err(e) => return Err(e).context("looking up the hub's intake"),
    };
    let info = intake.info().await?;
    if !info.config.sources.iter().flatten().any(|s| outbox_of(s, machine)) {
        return Ok(false);
    }
    let start = info.state.last_sequence + 1;
    let mut ants = hub.get_stream(STREAM).await.context("looking up the hub's stream")?;
    let mut config = ants.info().await?.config.clone();
    // A human's admission may take every inbox subject of the machine already, which the ant's may not overlap.
    let every_inbox = machine.admitted(&UserId::human(), Role::Human, true).swap_remove(0).0;
    let sources = config.sources.get_or_insert_default();
    let inbox = !sources
        .iter()
        .any(|s| from_machine(s, machine) && s.subject_transforms.iter().any(|t| t.source == every_inbox));
    if add_source(sources, member_source(machine, ant, Role::Ant, inbox, start)) {
        hub.update_stream(&config).await.context("admitting the ant into the hub's stream")?;
    }
    if let Some(mut config) = bucket_config(hub, LINKS_BUCKET).await?
        && add_source(config.sources.get_or_insert_default(), link_source(machine, ant, start))
    {
        hub.update_stream(&config).await.context("admitting the ant into the links bucket")?;
    }
    Ok(true)
}

/// Add `source` to `sources` unless one with the same subject transforms is there; whether it was added.
fn add_source(sources: &mut Vec<Source>, source: Source) -> bool {
    let there = sources.iter().any(|s| s.name == source.name && s.subject_transforms == source.subject_transforms);
    if !there {
        sources.push(source);
    }
    !there
}

/// Wait up to `wait` for the hub's intake to hear from the outbox of `machine`, which the hub then sources.
///
/// Sourcing an outbox needs nats-server's `js_api_reply_v2` feature flag on the hub ([`Machine::source_replies`]):
/// without it the leaf node cannot answer the hub, and the source stalls without an error, never active.
pub async fn sourced(hub: &Context, machine: &Machine, wait: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let info: serde_json::Value =
            hub.request(format!("STREAM.INFO.{INTAKE_STREAM}"), &()).await.context("looking up the hub's intake")?;
        match hearing(&info, machine) {
            Hearing::Heard => return Ok(()),
            Hearing::Failed(error) => anyhow::bail!("the hub cannot source the outbox of {machine}: {error}"),
            Hearing::Silent if tokio::time::Instant::now() >= deadline => anyhow::bail!(
                "the hub has not heard from the outbox of {machine} within {}s, so nothing kept there reaches the \
                 hub's stream. The hub's nats-server needs `feature_flags {{ js_api_reply_v2: true }}` in its \
                 configuration, then a restart; uplink credentials issued before store and forward existed need \
                 `ants auth leaf` again. The setup is in place: the hub takes from the outbox once it hears from it",
                wait.as_secs()
            ),
            Hearing::Silent => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

/// Whether the hub's intake hears from a machine's outbox.
#[derive(Debug, PartialEq)]
pub enum Hearing {
    Heard,
    /// Sourced, never heard from yet.
    Silent,
    /// Not sourced, or failing, for this reason.
    Failed(String),
}

/// How the hub takes from a machine's outbox ([`sourcing`]).
#[derive(Debug, PartialEq)]
pub struct Sourcing {
    pub hearing: Hearing,
    /// The members the hub's stream takes nothing from the outbox for.
    pub unadmitted: Vec<UserId>,
}

/// Whether the hub's intake sources the outbox of `machine` and has heard from it, and which of `members`, ants and
/// humans each in its role, the hub's stream takes nothing from it for, as [`admit`] would set it up. Reads the intake's
/// and the stream's info, which only a human's credentials get; changes nothing.
pub async fn sourcing(hub: &Context, machine: &Machine, members: &[(UserId, Role)]) -> Result<Sourcing> {
    let info: serde_json::Value =
        hub.request(format!("STREAM.INFO.{INTAKE_STREAM}"), &()).await.context("looking up the hub's intake")?;
    let hearing = hearing(&info, machine);
    let mut ants = hub.get_stream(STREAM).await.context("looking up the hub's stream")?;
    let sources = ants.info().await?.config.sources.clone().unwrap_or_default();
    let unadmitted = admitted_members(machine, members, 0)
        .into_iter()
        .filter(|(_, wanted)| {
            !sources.iter().any(|s| s.name == wanted.name && s.subject_transforms == wanted.subject_transforms)
        })
        .map(|(user, _)| user)
        .collect();
    Ok(Sourcing { hearing, unadmitted })
}

/// Whether the intake's source for the outbox of `machine`, in the intake's stream info (`STREAM.INFO`), has heard
/// from the leaf node: async-nats leaves out a source's `external` and `error`, so this reads the JSON.
fn hearing(info: &serde_json::Value, machine: &Machine) -> Hearing {
    let error = |e: &serde_json::Value| e["description"].as_str().unwrap_or("unknown error").to_owned();
    if let Some(e) = info.get("error") {
        return Hearing::Failed(error(e));
    }
    let api = machine.api_prefix();
    let sources = info["sources"].as_array().map(Vec::as_slice).unwrap_or_default();
    let Some(source) = sources.iter().find(|s| s["name"] == OUTBOX_STREAM && s["external"]["api"] == api.as_str())
    else {
        return Hearing::Failed("the hub's intake has no source for it".to_owned());
    };
    if let Some(e) = source.get("error") {
        return Hearing::Failed(error(e));
    }
    // `active` is how long ago the source last heard from the leaf, -1 if it never did.
    if source["active"].as_i64().is_some_and(|active| active >= 0) { Hearing::Heard } else { Hearing::Silent }
}

/// Stop taking anything from the outbox of `machine`. What its intake already holds stays there.
pub async fn remove(hub: &Context, machine: &Machine) -> Result<()> {
    let mut ants = hub.get_stream(STREAM).await.context("looking up the hub's stream")?;
    let mut config = ants.info().await?.config.clone();
    admit_into(&mut config, machine, Vec::new());
    hub.update_stream(&config).await.context("dropping the machine from the hub's stream")?;
    for bucket in [LINKS_BUCKET, OWNERS_BUCKET] {
        if let Some(mut config) = bucket_config(hub, bucket).await? {
            admit_into(&mut config, machine, Vec::new());
            hub.update_stream(&config)
                .await
                .with_context(|| format!("dropping the machine from the {bucket} bucket"))?;
        }
    }
    let mut intake = intake_config(hub).await?;
    if let Some(sources) = intake.sources.as_mut() {
        sources.retain(|s| !outbox_of(s, machine));
    }
    hub.create_or_update_stream(intake).await.context("dropping the machine from the hub's intake")?;
    Ok(())
}

/// Replace the sources of `config` from the intake for `machine` by `wanted`, keeping each one already in place as it
/// is, so it goes on from where it is.
fn admit_into(config: &mut stream::Config, machine: &Machine, wanted: Vec<Source>) {
    let mut sources: Vec<Source> = config.sources.take().unwrap_or_default();
    let kept: Vec<Source> = sources.extract_if(.., |s| from_machine(s, machine)).collect();
    for source in wanted {
        let same = kept.iter().find(|k| k.subject_transforms == source.subject_transforms);
        sources.push(same.cloned().unwrap_or(source));
    }
    config.sources = (!sources.is_empty()).then_some(sources);
}

/// The configuration of the stream of `bucket`, or `None` while `ants init` has not created it.
async fn bucket_config(hub: &Context, bucket: &str) -> Result<Option<stream::Config>> {
    match hub.get_stream(format!("KV_{bucket}")).await {
        Ok(mut stream) => Ok(Some(stream.info().await?.config.clone())),
        Err(e) if not_found(&e) => Ok(None),
        Err(e) => Err(e).with_context(|| format!("looking up the {bucket} bucket")),
    }
}

pub(crate) fn not_found(e: &GetStreamError) -> bool {
    matches!(e.kind(), GetStreamErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND)
}

fn outbox_config() -> stream::Config {
    stream::Config {
        name: OUTBOX_STREAM.to_owned(),
        description: Some("Messages kept while the hub was out of reach, for the hub to source".to_owned()),
        subjects: vec![OUTBOX_SUBJECTS.to_owned()],
        storage: stream::StorageType::File,
        max_age: RETENTION,
        ..Default::default()
    }
}

/// The intake's configuration as it is, or as it is created.
async fn intake_config(hub: &Context) -> Result<stream::Config> {
    match hub.get_stream(INTAKE_STREAM).await {
        Ok(mut intake) => Ok(intake.info().await?.config.clone()),
        Err(e) if not_found(&e) => Ok(stream::Config {
            name: INTAKE_STREAM.to_owned(),
            description: Some("Leaf node outboxes, each under its machine's prefix".to_owned()),
            storage: stream::StorageType::File,
            max_age: RETENTION,
            ..Default::default()
        }),
        Err(e) => Err(e).context("looking up the hub's intake"),
    }
}

/// The intake's source for the outbox of `machine`: everything in it, under the machine's prefix.
fn intake_source(machine: &Machine) -> Source {
    let prefixed = subject::intake(machine);
    Source {
        name: OUTBOX_STREAM.to_owned(),
        external: Some(External { api_prefix: machine.api_prefix(), delivery_prefix: Some(machine.deliver_prefix()) }),
        subject_transforms: vec![SubjectTransform { source: ">".to_owned(), destination: prefixed }],
        ..Default::default()
    }
}

fn outbox_of(source: &Source, machine: &Machine) -> bool {
    source.name == OUTBOX_STREAM && source.external.as_ref().is_some_and(|e| e.api_prefix == machine.api_prefix())
}

/// The hub stream's sources for `machine`: one per member, so admitting or dropping one leaves the others as they
/// are. The filters of one stream's sources may not overlap, so when humans run on the machine, the admission of the
/// first of them by name takes every inbox subject and the others' only their notes and channel posts.
fn admissions(machine: &Machine, members: &[(UserId, Role)], start: u64) -> Vec<Source> {
    admitted_members(machine, members, start).into_iter().map(|(_, source)| source).collect()
}

/// [`admissions`], each with its member.
fn admitted_members(machine: &Machine, members: &[(UserId, Role)], start: u64) -> Vec<(UserId, Source)> {
    let mut members: Vec<&(UserId, Role)> = members.iter().collect();
    members.sort_by(|(a, _), (b, _)| a.cmp(b));
    members.dedup_by(|(a, _), (b, _)| a == b);
    let inboxes = members.iter().find(|(_, role)| role.is_human()).map(|(human, _)| human);
    members
        .iter()
        .map(|(user, role)| {
            (user.clone(), member_source(machine, user, *role, inboxes.is_none_or(|human| human == user), start))
        })
        .collect()
}

/// The hub stream's source for `user` in `role` on `machine`, with its inbox subjects if `inbox`.
fn member_source(machine: &Machine, user: &UserId, role: Role, inbox: bool, start: u64) -> Source {
    Source {
        name: INTAKE_STREAM.to_owned(),
        start_sequence: Some(start),
        subject_transforms: machine
            .admitted(user, role, inbox)
            .into_iter()
            .map(|(source, destination)| SubjectTransform { source, destination })
            .collect(),
        ..Default::default()
    }
}

/// The links bucket's sources for `machine`: one per ant, taking only the links it passed on.
fn link_admissions(machine: &Machine, members: &[(UserId, Role)], start: u64) -> Vec<Source> {
    let mut ants: Vec<&UserId> = members.iter().filter(|(_, role)| !role.is_human()).map(|(ant, _)| ant).collect();
    ants.sort();
    ants.dedup();
    ants.into_iter().map(|ant| link_source(machine, ant, start)).collect()
}

fn link_source(machine: &Machine, ant: &UserId, start: u64) -> Source {
    let (source, destination) = machine.admitted_links(ant);
    Source {
        name: INTAKE_STREAM.to_owned(),
        start_sequence: Some(start),
        subject_transforms: vec![SubjectTransform { source, destination }],
        ..Default::default()
    }
}

/// The owners bucket's source for `machine`, when a human runs on it: the owners its humans record.
fn owner_admissions(machine: &Machine, members: &[(UserId, Role)], start: u64) -> Vec<Source> {
    if !members.iter().any(|(_, role)| role.is_human()) {
        return Vec::new();
    }
    let (source, destination) = machine.admitted_owners();
    vec![Source {
        name: INTAKE_STREAM.to_owned(),
        start_sequence: Some(start),
        subject_transforms: vec![SubjectTransform { source, destination }],
        ..Default::default()
    }]
}

fn from_machine(source: &Source, machine: &Machine) -> bool {
    let prefix = format!("ants.in.{machine}.");
    let transforms = &source.subject_transforms;
    source.name == INTAKE_STREAM && !transforms.is_empty() && transforms.iter().all(|t| t.source.starts_with(&prefix))
}

#[cfg(test)]
pub(crate) mod tests;
