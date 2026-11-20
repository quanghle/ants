//! `ants leaf outbox`: store and forward between a machine's leaf node and the hub.
//!
//! A message the hub's stream cannot take while the leaf's uplink is down waits in the leaf's own JetStream, in the
//! machine's outbox ([`OUTBOX_STREAM`]), and reaches the hub's stream in two hops once the uplink is back:
//!
//! 1. The hub's intake ([`INTAKE_STREAM`]) sources each machine's outbox through the machine's JetStream domain and
//!    stores everything under the machine's prefix ([`subject::intake`]), so a machine cannot pass for another. It
//!    takes it through a durable consumer on the outbox ([`OUTBOX_CONSUMER`]), which nats-server resets as soon as the
//!    leaf node answers again, so the intake goes on within seconds of the leaf node or the hub coming back.
//! 2. The hub's [`STREAM`] sources from the intake, per machine and user (ant or human), only the subjects the user
//!    may publish itself ([`Machine::admitted`]), back on their usual subjects. Anything else a machine claims stays in
//!    the intake.
//!
//! The links to channel threads the machine's ants pass on ([`LINKS_BUCKET`]) take the same way, the links bucket
//! sourcing from the intake, per ant, only the keys the ant may write itself ([`Machine::admitted_links`]); so do the
//! owners its humans record for the threads they message ants on ([`OWNERS_BUCKET`]), the owners bucket sourcing them
//! from the intake only when a human runs on the machine ([`Machine::admitted_owners`]). The owners bucket keeps every
//! record it sources, a thread's first staying its owner: a record it refused would stall its source. The chunks of
//! the attachments of messages kept in the outbox, kept there ahead of them, reach the attachments' store
//! ([`FILES_STREAM`]) likewise, the store sourcing from the intake, per ant, only the chunks under its own handles, and
//! when humans run on the machine, the chunks of every sender for the first of them by name
//! ([`Machine::admitted_files`]). Each chunk keeps its `Nats-Msg-Id`, so the store drops a buffered copy of a chunk it
//! took directly within its duplicate window, as the hub's stream does, rather than refuse it and stall its source.
//!
//! A machine's credentials thus never decide what the hub's stream stores: only a human, who sets up both hops here,
//! does. Each message keeps its `Nats-Msg-Id`, so the hub's stream drops a buffered copy of a message it already took
//! within its duplicate window, a day ([`ants_daemon::bus::DUPLICATE_WINDOW`]). Only the hub's stream needs that
//! window: a message stored directly never passes through the outbox or the intake, so its twins meet there.

use std::path::Path;
use std::time::Duration;

use ants_proto::{
    FILES_STREAM, INTAKE_STREAM, LINKS_BUCKET, Machine, OUTBOX_CONSUMER, OUTBOX_STREAM, OUTBOX_SUBJECTS, OWNERS_BUCKET,
    Role, STREAM, UserId, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind};
use async_nats::jetstream::response::Response;
use async_nats::jetstream::stream::{
    self, External, LastRawMessageErrorKind, Source, StreamConsumerSource, SubjectTransform,
};
use async_nats::jetstream::{self, Context, ErrorCode, consumer};

use crate::node::members::{self, Change, Members};

/// How long an outbox and the intake keep messages: as long as the hub's stream.
const RETENTION: Duration = Duration::from_hours(30 * 24);
/// How long to wait for the leaf node to answer through the hub.
const LEAF_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the hub may take to hear from an outbox it sources: a source that works is active within a second.
pub const SOURCE_WAIT: Duration = Duration::from_secs(10);
/// How long a source the hub heard from may stay silent before it counts as stalled: the leaf node sends a heartbeat
/// every second, and nats-server checks its sources this often.
const STALE: Duration = Duration::from_secs(10);

/// Connect as `human` to set up the outbox of `machine` on its leaf node, through the hub: the leaf node answers
/// through the store account's uplink, on the reply subject the team account's service import gave the request.
pub async fn connect(url: &str, creds: Option<&Path>, human: &UserId) -> Result<async_nats::Client> {
    ants_daemon::bus::check_url(url)?;
    let mut options =
        async_nats::ConnectOptions::new().name("ants-cli").custom_inbox_prefix(ants_proto::inbox_prefix(human));
    if let Some(path) = creds {
        options = ants_daemon::bus::authenticate(options, path, ants_daemon::secrets::keystore::os())?;
    }
    options.connect(url).await.with_context(|| format!("connecting to NATS at {url}"))
}

/// Set up store and forward for `machine`, on which `members` run, ants and humans each in its role
/// ([`crate::auth::members`]): create or update the machine's outbox on its leaf node (which must be connected),
/// through `leaf` from [`connect`], and have the hub's stream take from it what those members may send, the links
/// bucket the links its ants pass on, the owners bucket the owners its humans record, and the attachments' store the
/// chunks of the attachments they send, once `ants init` created them. Admissions of members no longer listed are
/// dropped; those already in place are kept as they are.
///
/// The intake sources the outbox through its durable consumer ([`OUTBOX_CONSUMER`]), created here when the leaf node
/// lacks it (never created, or lost with the leaf node's store) to start after the last message the intake took from
/// the outbox, if the outbox still holds it.
///
/// When the intake sourced the outbox already but has not heard from it lately, its source is restarted
/// ([`restart_source`]).
pub async fn admit(
    leaf: &async_nats::Client,
    hub: &Context,
    machine: &Machine,
    members: &[(UserId, Role)],
) -> Result<()> {
    anyhow::ensure!(!members.is_empty(), "a machine needs at least one ant or human");
    let mut leaf = jetstream::with_domain(leaf.clone(), machine.as_str());
    leaf.set_timeout(LEAF_TIMEOUT);
    let outbox = leaf.create_or_update_stream(outbox_config()).await.with_context(|| {
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
        admit_into(&mut config, machine, owner_admissions(machine, members, start));
        hub.update_stream(&config).await.context("admitting the machine's outbox into the owners bucket")?;
    }
    if let Some(mut config) = stream_config(hub, FILES_STREAM).await? {
        let wanted = file_admissions(machine, members, start).into_iter().map(|(_, source)| source).collect();
        admit_into(&mut config, machine, wanted);
        hub.update_stream(&config).await.context("admitting the machine's outbox into the attachments' store")?;
    }

    let mut intake = intake_config(hub).await?;
    let current = intake.sources.iter().flatten().find(|s| outbox_of(s, machine)).cloned();
    let wanted = if has_outbox_consumer(&leaf).await? {
        current.is_none().then(|| intake_source(machine))
    } else {
        if current.is_some() {
            // The intake takes nothing more from the outbox before it finds where it is.
            drop_source(&mut intake, machine);
            intake = hub.update_stream(&intake).await.context("dropping the machine's outbox")?.config;
        }
        let after = intake_position(hub, machine).await?.filter(|&seq| seq <= outbox.state.last_sequence);
        create_outbox_consumer(&leaf, machine, after).await?;
        Some(intake_source(machine))
    };
    let kept = wanted.is_none();
    if let Some(source) = wanted {
        drop_source(&mut intake, machine);
        intake.sources.get_or_insert_default().push(source);
    }
    hub.create_or_update_stream(intake).await.context("sourcing the machine's outbox into the hub's intake")?;
    if kept && hearing(&intake_info(hub).await?, machine) != Hearing::Heard {
        restart_source(hub, machine).await?;
    }
    Ok(())
}

/// Whether the outbox's durable consumer is there, as the leaf node answers through `leaf`.
async fn has_outbox_consumer(leaf: &Context) -> Result<bool> {
    let subject = format!("CONSUMER.INFO.{OUTBOX_STREAM}.{OUTBOX_CONSUMER}");
    match leaf.request::<_, _, Response<consumer::Info>>(subject, &serde_json::json!({})).await {
        Ok(Response::Ok(_)) => Ok(true),
        Ok(Response::Err { error }) if error.error_code() == ErrorCode::CONSUMER_NOT_FOUND => Ok(false),
        Ok(Response::Err { error }) => Err(error).context("looking up the outbox's durable consumer"),
        Err(e) => Err(e).context("looking up the outbox's durable consumer on the leaf node"),
    }
}

/// Create the outbox's durable consumer, delivering to the hub what follows sequence `after` in the outbox, or all of
/// it. nats-server's durable sourcing takes a push consumer acked by flow control, with a 1 s heartbeat.
async fn create_outbox_consumer(leaf: &Context, machine: &Machine, after: Option<u64>) -> Result<()> {
    let deliver_policy = match after {
        Some(seq) => consumer::DeliverPolicy::ByStartSequence { start_sequence: seq + 1 },
        None => consumer::DeliverPolicy::All,
    };
    let config = consumer::push::Config {
        durable_name: Some(OUTBOX_CONSUMER.to_owned()),
        description: Some("The hub's intake sourcing the outbox".to_owned()),
        deliver_subject: machine.outbox_delivery(),
        deliver_policy,
        ack_policy: consumer::AckPolicy::FlowControl,
        flow_control: true,
        idle_heartbeat: Duration::from_secs(1),
        max_deliver: -1,
        ..Default::default()
    };
    leaf.create_consumer_on_stream(config, OUTBOX_STREAM)
        .await
        .context("creating the outbox's durable consumer on the leaf node")?;
    Ok(())
}

/// The sequence in the outbox of `machine` of the last message the hub's intake took from it, from the
/// `Nats-Stream-Source` header nats-server gives each message it sources (`<source> <sequence> ...`); `None` when the
/// intake holds none.
async fn intake_position(hub: &Context, machine: &Machine) -> Result<Option<u64>> {
    let intake = hub.get_stream_no_info(INTAKE_STREAM).await.context("looking up the hub's intake")?;
    let last = match intake.get_last_raw_message_by_subject(&subject::intake(machine)).await {
        Ok(last) => last,
        Err(e) => match e.kind() {
            LastRawMessageErrorKind::NoMessageFound => return Ok(None),
            LastRawMessageErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND => return Ok(None),
            _ => return Err(e).context("reading the last message the hub's intake took from the outbox"),
        },
    };
    let source = last.headers.get("Nats-Stream-Source").map_or_default(|v| v.as_str().to_owned());
    let seq = source.split(' ').nth(1).and_then(|seq| seq.parse().ok());
    seq.map(Some).with_context(|| format!("no outbox sequence in the intake's message {}", last.sequence))
}

/// Have the hub's intake ask the leaf node of `machine` for its outbox again at once, dropping its source and adding
/// it back: nats-server has no other way to cut short the wait between its requests. The source added back goes on
/// after the last message the intake took from it, which the intake finds among its messages, or from where its
/// durable consumer is.
async fn restart_source(hub: &Context, machine: &Machine) -> Result<()> {
    let mut intake = intake_config(hub).await?;
    let ours = drop_source(&mut intake, machine);
    hub.update_stream(&intake).await.context("dropping the machine's outbox from the hub's intake")?;
    intake.sources.get_or_insert_default().extend(ours);
    hub.update_stream(&intake).await.context("sourcing the machine's outbox into the hub's intake again")?;
    Ok(())
}

/// Take the source of the outbox of `machine` out of the intake's `config`, returning it.
fn drop_source(config: &mut stream::Config, machine: &Machine) -> Vec<Source> {
    let mut sources = config.sources.take().unwrap_or_default();
    let ours = sources.extract_if(.., |s| outbox_of(s, machine)).collect();
    config.sources = (!sources.is_empty()).then_some(sources);
    ours
}

/// The intake's stream info (`STREAM.INFO`), as JSON: async-nats leaves out a source's `external` and `error`.
async fn intake_info(hub: &Context) -> Result<serde_json::Value> {
    hub.request(format!("STREAM.INFO.{INTAKE_STREAM}"), &()).await.context("looking up the hub's intake")
}

/// Admit `ant`, new to `machine`, into the hub's stream from the machine's outbox, and into the links bucket and the
/// attachments' store once `ants init` created them, as [`admit`] would with the ant listed too, leaving the other admissions as they are; for
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
    if let Some(mut config) = stream_config(hub, FILES_STREAM).await? {
        // A human's admission may take the chunks of every sender already, which the ant's may not overlap.
        let every_sender = machine.admitted_files(&UserId::human(), Role::Human).0;
        let sources = config.sources.get_or_insert_default();
        let taken = sources
            .iter()
            .any(|s| from_machine(s, machine) && s.subject_transforms.iter().any(|t| t.source == every_sender));
        if !taken && add_source(sources, file_source(machine, ant, Role::Ant, start)) {
            hub.update_stream(&config).await.context("admitting the ant into the attachments' store")?;
        }
    }
    Ok(true)
}

/// Drop `ant`, retired from `machine`, from what the hub's stream, the links bucket and the attachments' store take
/// from the machine's outbox, as [`admit`] would without the ant listed, leaving the other admissions as they are; for
/// `ants signer`, as [`admit_ant`]. Returns false, changing nothing, when the hub's intake does not source the
/// machine's outbox.
pub async fn drop_ant(hub: &Context, machine: &Machine, ant: &UserId) -> Result<bool> {
    let mut intake = match hub.get_stream(INTAKE_STREAM).await {
        Ok(intake) => intake,
        Err(e) if not_found(&e) => return Ok(false),
        Err(e) => return Err(e).context("looking up the hub's intake"),
    };
    if !intake.info().await?.config.sources.iter().flatten().any(|s| outbox_of(s, machine)) {
        return Ok(false);
    }
    let mut ants = hub.get_stream(STREAM).await.context("looking up the hub's stream")?;
    let mut config = ants.info().await?.config.clone();
    if drop_sources(&mut config, &ant_sources(machine, ant)) {
        hub.update_stream(&config).await.context("dropping the ant from the hub's stream")?;
    }
    if let Some(mut config) = bucket_config(hub, LINKS_BUCKET).await?
        && drop_sources(&mut config, &[link_source(machine, ant, 0)])
    {
        hub.update_stream(&config).await.context("dropping the ant from the links bucket")?;
    }
    if let Some(mut config) = stream_config(hub, FILES_STREAM).await?
        && drop_sources(&mut config, &[file_source(machine, ant, Role::Ant, 0)])
    {
        hub.update_stream(&config).await.context("dropping the ant from the attachments' store")?;
    }
    Ok(true)
}

/// Take out of the ants `change` removes from `kept`, the members of `machine` its uplink credentials carry, those
/// that left them already ([`Change::left_already`]), and return them: those the registry of the team directory `dir`
/// ever kept among the members, or the hub's stream still takes from the outbox for. [`admit`] without them drops
/// what the hub takes for them all the same.
pub async fn left_already(
    hub: &Context,
    machine: &Machine,
    dir: &Path,
    change: &mut Change,
    kept: Option<&Members>,
) -> Result<Vec<UserId>> {
    if change.remove.ants.is_empty() {
        return Ok(Vec::new());
    }
    let ever = members::ever(dir, machine)?;
    let config = stream_config(hub, STREAM).await?.unwrap_or_default();
    Ok(change.left_already(kept, |ant| ever.contains(ant) || sources_ant(&config, machine, ant)))
}

/// Whether `config` sources from the outbox of `machine` what `ant` sends.
fn sources_ant(config: &stream::Config, machine: &Machine, ant: &UserId) -> bool {
    let theirs = ant_sources(machine, ant);
    let ours = |s: &Source| theirs.iter().any(|t| s.name == t.name && s.subject_transforms == t.subject_transforms);
    config.sources.iter().flatten().any(ours)
}

/// The sources of the hub's stream from the outbox of `machine` for `ant`, with or without its inbox subjects.
fn ant_sources(machine: &Machine, ant: &UserId) -> [Source; 2] {
    [true, false].map(|inbox| member_source(machine, ant, Role::Ant, inbox, 0))
}

/// Drop from the sources of `config` those with the subject transforms of one of `theirs`; whether any was there.
fn drop_sources(config: &mut stream::Config, theirs: &[Source]) -> bool {
    let Some(sources) = config.sources.as_mut() else { return false };
    let before = sources.len();
    sources.retain(|s| !theirs.iter().any(|t| s.name == t.name && s.subject_transforms == t.subject_transforms));
    sources.len() != before
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
        let late = tokio::time::Instant::now() >= deadline;
        match hearing(&intake_info(hub).await?, machine) {
            Hearing::Heard => return Ok(()),
            Hearing::Failed(error) => anyhow::bail!("the hub cannot source the outbox of {machine}: {error}"),
            Hearing::Stale(ago) if late => anyhow::bail!(
                "the hub last heard from the outbox of {machine} {}s ago, so nothing kept there reaches the hub's \
                 stream: check that the leaf node is up and connected to the hub, then run this again",
                ago.as_secs()
            ),
            Hearing::Silent if late => anyhow::bail!(
                "the hub has not heard from the outbox of {machine} within {}s, so nothing kept there reaches the \
                 hub's stream. The hub's nats-server needs `feature_flags {{ js_api_reply_v2: true }}` in its \
                 configuration, then a restart. The setup is in place: the hub takes from the outbox once it hears \
                 from it",
                wait.as_secs()
            ),
            Hearing::Silent | Hearing::Stale(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

/// Whether the hub's intake hears from a machine's outbox.
#[derive(Debug, PartialEq)]
pub enum Hearing {
    /// Heard from within the last few seconds ([`STALE`]).
    Heard,
    /// Sourced, not heard from since the hub's nats-server started, or ever.
    Silent,
    /// Heard from, but not for this long: the uplink is down.
    Stale(Duration),
    /// Not sourced, or failing, for this reason.
    Failed(String),
}

/// How the hub takes from a machine's outbox ([`sourcing`]).
#[derive(Debug, PartialEq)]
pub struct Sourcing {
    pub hearing: Hearing,
    /// The members the hub's stream, or the attachments' store, takes nothing from the outbox for.
    pub unadmitted: Vec<UserId>,
}

/// Whether the hub's intake sources the outbox of `machine` and has heard from it, and which of `members`, ants and
/// humans each in its role, the hub's stream, or the attachments' store once `ants init` created it, takes nothing from
/// it for, as [`admit`] would set them up. Reads the intake's, the stream's and the store's info, which only a human's
/// credentials get; changes nothing.
pub async fn sourcing(hub: &Context, machine: &Machine, members: &[(UserId, Role)]) -> Result<Sourcing> {
    let info = intake_info(hub).await?;
    let hearing = hearing(&info, machine);
    let mut ants = hub.get_stream(STREAM).await.context("looking up the hub's stream")?;
    let sources = ants.info().await?.config.sources.clone().unwrap_or_default();
    let files = stream_config(hub, FILES_STREAM).await?;
    let file_sources = files.as_ref().and_then(|config| config.sources.clone()).unwrap_or_default();
    let missing = |sources: &[Source], (user, wanted): (UserId, Source)| {
        let there = sources.iter().any(|s| s.name == wanted.name && s.subject_transforms == wanted.subject_transforms);
        (!there).then_some(user)
    };
    let mut unadmitted: Vec<UserId> =
        admitted_members(machine, members, 0).into_iter().filter_map(|m| missing(&sources, m)).collect();
    if files.is_some() {
        for user in file_admissions(machine, members, 0).into_iter().filter_map(|m| missing(&file_sources, m)) {
            if !unadmitted.contains(&user) {
                unadmitted.push(user);
            }
        }
        unadmitted.sort();
    }
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
    let sources = info["sources"].as_array().map_or_default(Vec::as_slice);
    let Some(source) = sources.iter().find(|s| s["name"] == OUTBOX_STREAM && s["external"]["api"] == api.as_str())
    else {
        return Hearing::Failed("the hub's intake has no source for it".to_owned());
    };
    if let Some(e) = source.get("error") {
        return Hearing::Failed(error(e));
    }
    // `active` is how long ago, in nanoseconds, the source last heard from the leaf, -1 if it never did.
    match source["active"].as_i64().and_then(|active| u64::try_from(active).ok()) {
        Some(active) if Duration::from_nanos(active) < STALE => Hearing::Heard,
        Some(active) => Hearing::Stale(Duration::from_nanos(active)),
        None => Hearing::Silent,
    }
}

/// Stop taking anything from the outbox of `machine`. What its intake already holds stays there, and so does the
/// outbox's durable consumer, from which [`admit`] goes on.
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
    if let Some(mut config) = stream_config(hub, FILES_STREAM).await? {
        admit_into(&mut config, machine, Vec::new());
        hub.update_stream(&config).await.context("dropping the machine from the attachments' store")?;
    }
    let mut intake = intake_config(hub).await?;
    drop_source(&mut intake, machine);
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
    stream_config(hub, &format!("KV_{bucket}")).await
}

/// The configuration of stream `name`, or `None` while `ants init` has not created it.
async fn stream_config(hub: &Context, name: &str) -> Result<Option<stream::Config>> {
    match hub.get_stream(name).await {
        Ok(mut stream) => Ok(Some(stream.info().await?.config.clone())),
        Err(e) if not_found(&e) => Ok(None),
        Err(e) => Err(e).with_context(|| format!("looking up stream {name}")),
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

/// The intake's source for the outbox of `machine`, through its durable consumer: everything in it, under the
/// machine's prefix.
fn intake_source(machine: &Machine) -> Source {
    let prefixed = subject::intake(machine);
    let consumer = Some(StreamConsumerSource::new(OUTBOX_CONSUMER.to_owned(), machine.outbox_delivery()));
    Source {
        name: OUTBOX_STREAM.to_owned(),
        external: Some(External { api_prefix: machine.api_prefix(), delivery_prefix: Some(machine.deliver_prefix()) }),
        subject_transforms: vec![SubjectTransform { source: ">".to_owned(), destination: prefixed }],
        consumer,
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

/// The attachments' store's sources for `machine`, each with its member: when humans run on it, one taking the chunks
/// of every sender for the first of them by name, as a human writes under any sender and the filters of one stream's
/// sources may not overlap; else one per ant, taking the chunks under its own handles.
fn file_admissions(machine: &Machine, members: &[(UserId, Role)], start: u64) -> Vec<(UserId, Source)> {
    let mut members: Vec<&(UserId, Role)> = members.iter().collect();
    members.sort_by(|(a, _), (b, _)| a.cmp(b));
    members.dedup_by(|(a, _), (b, _)| a == b);
    if let Some(human) = members.iter().find(|(_, role)| role.is_human()) {
        members = vec![*human];
    }
    members.into_iter().map(|(user, role)| (user.clone(), file_source(machine, user, *role, start))).collect()
}

fn file_source(machine: &Machine, user: &UserId, role: Role, start: u64) -> Source {
    let (source, destination) = machine.admitted_files(user, role);
    Source {
        name: INTAKE_STREAM.to_owned(),
        start_sequence: Some(start),
        subject_transforms: vec![SubjectTransform { source, destination }],
        ..Default::default()
    }
}

fn from_machine(source: &Source, machine: &Machine) -> bool {
    let prefix = format!("ants.in.{machine}.");
    let transforms = &source.subject_transforms;
    source.name == INTAKE_STREAM && !transforms.is_empty() && transforms.iter().all(|t| t.source.starts_with(&prefix))
}

#[cfg(test)]
mod tests;
