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
//! within its duplicate window.

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
const RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
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
        options =
            options.credentials_file(path).await.with_context(|| format!("reading credentials {}", path.display()))?;
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
pub(crate) mod tests {
    use std::process::Command;

    use ants_daemon::{bus, links, notes};
    use ants_proto::{ChannelId, ChannelPost, DeadLetter, Envelope, JsDomain, NotesPost, ThreadId};
    use ants_test_support::NatsServer;
    use async_nats::jetstream::message::{PublishMessage, StreamMessage};
    use futures::StreamExt as _;

    use super::*;
    use crate::humans::Me;
    use crate::{auth, channel, leaf};

    pub(crate) fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    pub(crate) async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
        for _ in 0..600 {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{what}: timed out");
    }

    /// The next message a waiter takes.
    async fn next(replies: &mut bus::LiveInbox) -> Envelope {
        let msg = tokio::time::timeout(Duration::from_secs(60), replies.next()).await.expect("no message").unwrap();
        let envelope: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        envelope.check_subject(&msg.subject).unwrap();
        envelope
    }

    async fn messages(js: &Context, name: &str) -> u64 {
        js.get_stream(name).await.unwrap().info().await.unwrap().state.messages
    }

    /// A real hub and leaf node: what an ant on the leaf sends while the uplink is down waits in the leaf's
    /// outbox and lands in the hub's stream once, after the uplink is back, and reaches a live waiter then; what the
    /// machine may not send stays out.
    #[tokio::test]
    async fn the_hub_takes_what_a_leaf_kept_while_its_uplink_was_down() {
        let (leaf_port, dead_port) = (free_port(), free_port());
        let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else {
            return;
        };
        let team = hub.dir().join("auth");
        let domain: JsDomain = "hub".parse().unwrap();
        let m1: Machine = "m1".parse().unwrap();
        let (human, dev_a, dev_b): (UserId, UserId, UserId) =
            (UserId::human(), "dev-a".parse().unwrap(), "dev-b".parse().unwrap());
        let creds = |p: &UserId| {
            let out = hub.dir().join(format!("{p}.creds"));
            auth::issue(&team, p, Role::by_default(p), Some(&domain), &out).unwrap();
            out
        };
        let connect = async |url: &str, p: &UserId| {
            let (client, mut js) = bus::connect(url, Some(&creds(p)), Some(&domain), p, "test").await.unwrap();
            js.set_timeout(Duration::from_secs(2));
            (client, js)
        };
        let (h, h_js) = connect(&hub.url, &human).await;
        bus::init_stream(&h_js).await.unwrap();
        bus::ensure_links(&h_js).await.unwrap();
        bus::ensure_owners(&h_js).await.unwrap();

        let leaf_creds = hub.dir().join("leaf-m1.creds");
        // The CLI runs on m1 too.
        let alice: UserId = "alice".parse().unwrap();
        let on_m1 = auth::members(&[dev_a.clone(), human.clone()], std::slice::from_ref(&alice));
        auth::issue_leaf(&team, &m1, &on_m1, &[], false, Some(&domain), &leaf_creds).unwrap();
        let server_conf = team.join(auth::SERVER_CONF);
        let listen = format!("127.0.0.1:{}", free_port());
        let configure = |dir: &Path, port: u16| {
            let uplinks = [format!("nats-leaf://127.0.0.1:{port}")];
            let config = leaf::Leaf {
                machine: &m1,
                hub_domain: Some(&domain),
                uplinks: &uplinks,
                creds: &leaf_creds,
                system_creds: None,
                server_conf: &server_conf,
                ca_file: None,
                proxy: None,
                listen: &listen,
            };
            leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        };
        let leaf_node = NatsServer::spawn_with(|dir| {
            configure(dir, leaf_port);
            "include \"leaf.conf\"\n".to_owned()
        })
        .unwrap();
        let reload = |port: u16| {
            configure(leaf_node.dir(), port);
            let status = Command::new("kill").arg("-HUP").arg(leaf_node.pid().to_string()).status().unwrap();
            assert!(status.success());
        };
        let (_a, a_js) = connect(&leaf_node.url, &dev_a).await;
        let (_b, b_js) = connect(&leaf_node.url, &dev_b).await;
        eventually("the leaf reaches the hub", async || bus::open_stream(&a_js, &dev_a).await.is_ok()).await;
        // The human waits for replies as `ants ask --wait` does, off the machine and on it; on it, core NATS also
        // delivers what the hub never got.
        let (h_leaf, h_leaf_js) = connect(&leaf_node.url, &human).await;
        let mut waiters = [
            bus::caught_up_inbox(&h_leaf, &h_leaf_js, &human, Role::Human).await.unwrap(),
            bus::caught_up_inbox(&h, &h_js, &human, Role::Human).await.unwrap(),
        ];
        h.flush().await.unwrap();
        h_leaf.flush().await.unwrap();
        // So does dev-a's CLI on the machine, with dev-a's credentials.
        let (a_cli, a_cli_js) = connect(&leaf_node.url, &dev_a).await;
        let mut ant_waiter = bus::caught_up_inbox(&a_cli, &a_cli_js, &dev_a, Role::Ant).await.unwrap();
        a_cli.flush().await.unwrap();

        // Before the outbox exists, a publish that cannot reach the hub fails outright.
        outbox_missing_fails(&b_js).await;
        let setup = super::connect(&hub.url, Some(&creds(&human)), &human, &m1).await.unwrap();
        admit(&setup, &h_js, &m1, &on_m1).await.unwrap();
        sourced(&h_js, &m1, Duration::from_secs(5)).await.unwrap();
        let found = sourcing(&h_js, &m1, &on_m1).await.unwrap();
        assert_eq!(found, Sourcing { hearing: Hearing::Heard, unadmitted: Vec::new() });
        let more = [on_m1.clone(), vec![(dev_b.clone(), Role::Ant)]].concat();
        assert_eq!(sourcing(&h_js, &m1, &more).await.unwrap().unadmitted, std::slice::from_ref(&dev_b));
        // Any human on the machine may set it up again.
        let alice_creds = hub.dir().join("alice.creds");
        auth::issue(&team, &alice, Role::Human, Some(&domain), &alice_creds).unwrap();
        let (_alice, alice_js) =
            bus::connect(&hub.url, Some(&alice_creds), Some(&domain), &alice, "test").await.unwrap();
        let setup = super::connect(&hub.url, Some(&alice_creds), &alice, &m1).await.unwrap();
        admit(&setup, &alice_js, &m1, &on_m1).await.unwrap();

        // Stored directly, and a copy of it in the outbox is dropped as a duplicate.
        let direct = Envelope::new(dev_a.clone(), human.clone(), "direct");
        assert_eq!(bus::publish(&a_js, &direct).await.unwrap(), bus::Stored::Hub);
        let copy = PublishMessage::build()
            .payload(serde_json::to_vec(&direct).unwrap().into())
            .message_id(direct.id.to_string());
        let outboxed = subject::outbox(&direct.subject()).unwrap();
        a_js.send_publish(outboxed, copy).await.unwrap().await.unwrap();
        eventually("the copy reaches the intake", async || messages(&h_js, INTAKE_STREAM).await == 1).await;

        for replies in &mut waiters {
            assert_eq!(next(replies).await, direct);
        }

        reload(dead_port);
        eventually("the uplink is down", async || bus::open_stream(&a_js, &dev_a).await.is_err()).await;
        let kept = Envelope::new(dev_a.clone(), human.clone(), "kept");
        assert_eq!(bus::publish(&a_js, &kept).await.unwrap(), bus::Stored::Outbox);
        let thread: ThreadId = "t1".parse().unwrap();
        assert!(notes::publish(&a_js, &NotesPost::new(thread.clone(), dev_a.clone(), "notes"), None).await.unwrap());
        let chan = ChannelPost::new("ops".parse().unwrap(), dev_a.clone(), "@dev-b kept");
        let stored = bus::store(&a_js, chan.subject(), &chan, &chan.id.to_string()).await.unwrap();
        assert_eq!(stored, bus::Stored::Outbox);
        let dead = DeadLetter::of_envelope(dev_a.clone(), 1, 5, "harness crashed", direct.clone());
        let stored = bus::store(&a_js, dead.subject(), &dead, &dead.msg_id()).await.unwrap();
        assert_eq!(stored, bus::Stored::Outbox);
        // dev-b may post these itself, but is not admitted from m1.
        let stray = NotesPost::new(thread.clone(), dev_b.clone(), "stray");
        assert!(notes::publish(&b_js, &stray, None).await.unwrap());
        let stray_dead = DeadLetter::of_envelope(dev_b.clone(), 2, 5, "stray", direct.clone());
        let stored = bus::store(&b_js, stray_dead.subject(), &stray_dead, &stray_dead.msg_id()).await.unwrap();
        assert_eq!(stored, bus::Stored::Outbox);
        // Links passed on wait in the outbox too; dev-b's stay out of the links bucket like its posts.
        let channel_thread = ThreadId::generate();
        let spun = |from: &UserId, to: &UserId| {
            Envelope::on_thread(channel_thread.clone(), from.clone(), to.clone(), "")
                .with_creator(Some(dev_b.clone()))
                .spun_off(ThreadId::generate())
        };
        links::pass_on(&a_js, &spun(&dev_a, &dev_b)).await.unwrap();
        links::pass_on(&b_js, &spun(&dev_b, &dev_a)).await.unwrap();
        // So do the owners a human records, ahead of the message: a second record of a thread is dropped,
        // and does not hold up those after it.
        let (taken, asked, after) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        owners::record(&h_js, Some(&domain), &taken, &dev_a, &alice).await.unwrap();
        owners::record(&h_leaf_js, Some(&domain), &taken, &dev_a, &human).await.unwrap();
        owners::record(&h_leaf_js, Some(&domain), &asked, &dev_b, &human).await.unwrap();
        let ask =
            Envelope::on_thread(asked.clone(), human.clone(), dev_b.clone(), "asked").with_owner(Some(human.clone()));
        assert_eq!(bus::publish(&h_leaf_js, &ask).await.unwrap(), bus::Stored::Outbox);
        owners::record(&h_leaf_js, Some(&domain), &after, &dev_a, &human).await.unwrap();
        assert!(messages(&h_js, STREAM).await == 1);

        reload(leaf_port);
        eventually("the intake takes the outbox", async || messages(&h_js, INTAKE_STREAM).await == 13).await;
        eventually("the hub's stream takes what dev-a sent", async || messages(&h_js, STREAM).await == 6).await;
        let ants = h_js.get_stream(STREAM).await.unwrap();
        let last = ants.get_last_raw_message_by_subject(&kept.subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&last.payload).unwrap(), kept);
        ants.get_last_raw_message_by_subject(&subject::notes(&thread, &dev_a)).await.unwrap();
        let last = ants.get_last_raw_message_by_subject(&chan.subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<ChannelPost>(&last.payload).unwrap(), chan);
        let last = ants.get_last_raw_message_by_subject(&dead.subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<DeadLetter>(&last.payload).unwrap(), dead);
        assert!(ants.get_last_raw_message_by_subject(&stray.subject()).await.is_err());
        assert!(ants.get_last_raw_message_by_subject(&stray_dead.subject()).await.is_err());
        for replies in &mut waiters {
            assert_eq!(next(replies).await, kept, "a waiter gets the kept message, once the hub stores it");
        }
        let passed_on = async || links::passed_on(&h_js, &channel_thread).await.unwrap();
        eventually("the links bucket takes dev-a's link", async || !passed_on().await.is_empty()).await;
        assert_eq!(passed_on().await, [(dev_a.clone(), dev_b.clone())]);
        let read = owners::Owners::new(h_js.clone());
        eventually("the owners bucket takes the owners", async || {
            read.recorded(&after, &dev_a).await.unwrap() == Some(human.clone())
        })
        .await;
        assert_eq!(read.recorded(&asked, &dev_b).await.unwrap(), Some(human.clone()));
        assert_eq!(read.recorded(&taken, &dev_a).await.unwrap(), Some(alice.clone()), "the first record stays");
        // How a daemon tells a message from an outbox.
        let source = |msg: StreamMessage| msg.headers.get("Nats-Stream-Source").is_some();
        assert!(source(ants.get_last_raw_message_by_subject(&ask.subject()).await.unwrap()));
        let first = ants.get_raw_message(1).await.unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&first.payload).unwrap(), direct);
        assert!(!source(first));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(messages(&h_js, STREAM).await, 6, "stored once");
        for replies in &mut waiters {
            assert!(futures::FutureExt::now_or_never(replies.next()).is_none(), "each once, live or stored");
        }

        // What the hub stores while the uplink is down never comes live on the leaf; its waiter reads it back.
        reload(dead_port);
        eventually("the uplink is down again", async || bus::open_stream(&a_js, &dev_a).await.is_err()).await;
        let (_b_hub, b_hub_js) = connect(&hub.url, &dev_b).await;
        let away = Envelope::new(dev_b.clone(), human.clone(), "away");
        assert_eq!(bus::publish(&b_hub_js, &away).await.unwrap(), bus::Stored::Hub);
        let away_from_a = Envelope::new(dev_b.clone(), dev_a.clone(), "away");
        assert_eq!(bus::publish(&b_hub_js, &away_from_a).await.unwrap(), bus::Stored::Hub);
        assert_eq!(next(&mut waiters[1]).await, away);
        reload(leaf_port);
        assert_eq!(next(&mut waiters[0]).await, away, "the waiter on the leaf gets it once the uplink is back");
        assert_eq!(next(&mut ant_waiter).await, away_from_a, "and so does an ant's");
        let [on_leaf, on_hub] = &mut waiters;
        let wait = bus::CATCH_UP * 2;
        let (on_leaf, on_hub, ant) = tokio::join!(
            tokio::time::timeout(wait, on_leaf.next()),
            tokio::time::timeout(wait, on_hub.next()),
            tokio::time::timeout(wait, ant_waiter.next())
        );
        assert!(on_leaf.is_err() && on_hub.is_err() && ant.is_err(), "each once, live or read back");

        remove(&h_js, &m1).await.unwrap();
        for name in [STREAM.to_owned(), format!("KV_{LINKS_BUCKET}"), format!("KV_{OWNERS_BUCKET}")] {
            let info = h_js.get_stream(name).await.unwrap().info().await.unwrap().config.clone();
            assert!(info.sources.unwrap_or_default().is_empty());
        }
    }

    /// A real hub and leaf node: `ants post --wait` gets each reply once, also those the hub stores while the leaf's
    /// uplink is down and those it takes from a leaf's outbox, which never come live, read back with a human's
    /// credentials and an ant's; for a post kept in the outbox, only what the hub stores after it.
    #[tokio::test]
    async fn a_post_waiter_reads_back_what_never_comes_live() {
        let (leaf_port, dead_port) = (free_port(), free_port());
        let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else {
            return;
        };
        let team = hub.dir().join("auth");
        let domain: JsDomain = "hub".parse().unwrap();
        let m1: Machine = "m1".parse().unwrap();
        let (human, dev_a, dev_b): (UserId, UserId, UserId) =
            (UserId::human(), "dev-a".parse().unwrap(), "dev-b".parse().unwrap());
        let connect = async |url: &str, p: &UserId| {
            let creds = hub.dir().join(format!("{p}.creds"));
            auth::issue(&team, p, Role::by_default(p), Some(&domain), &creds).unwrap();
            let (client, mut js) = bus::connect(url, Some(&creds), Some(&domain), p, "test").await.unwrap();
            js.set_timeout(Duration::from_secs(2));
            (client, js)
        };
        let (h, h_js) = connect(&hub.url, &human).await;
        bus::init_stream(&h_js).await.unwrap();
        let (_b, b_js) = connect(&hub.url, &dev_b).await;

        let leaf_creds = hub.dir().join("leaf-m1.creds");
        let on_m1 = auth::members(&[dev_a.clone(), human.clone()], &[]);
        auth::issue_leaf(&team, &m1, &on_m1, &[], false, Some(&domain), &leaf_creds).unwrap();
        let server_conf = team.join(auth::SERVER_CONF);
        let listen = format!("127.0.0.1:{}", free_port());
        let configure = |dir: &Path, port: u16| {
            let uplinks = [format!("nats-leaf://127.0.0.1:{port}")];
            let config = leaf::Leaf {
                machine: &m1,
                hub_domain: Some(&domain),
                uplinks: &uplinks,
                creds: &leaf_creds,
                system_creds: None,
                server_conf: &server_conf,
                ca_file: None,
                proxy: None,
                listen: &listen,
            };
            leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        };
        let leaf_node = NatsServer::spawn_with(|dir| {
            configure(dir, leaf_port);
            "include \"leaf.conf\"\n".to_owned()
        })
        .unwrap();
        let reload = async |port: u16, up: bool| {
            configure(leaf_node.dir(), port);
            let status = Command::new("kill").arg("-HUP").arg(leaf_node.pid().to_string()).status().unwrap();
            assert!(status.success());
            let (_, js) = connect(&leaf_node.url, &dev_a).await;
            eventually("the uplink goes up or down", async || bus::open_stream(&js, &dev_a).await.is_ok() == up).await;
        };
        let (h_leaf, h_leaf_js) = connect(&leaf_node.url, &human).await;
        let (a_leaf, a_leaf_js) = connect(&leaf_node.url, &dev_a).await;
        eventually("the leaf reaches the hub", async || bus::open_stream(&a_leaf_js, &dev_a).await.is_ok()).await;
        let setup = super::connect(&hub.url, Some(&hub.dir().join("human.creds")), &human, &m1).await.unwrap();
        admit(&setup, &h_js, &m1, &on_m1).await.unwrap();
        sourced(&h_js, &m1, Duration::from_secs(5)).await.unwrap();

        // Each poster waits as `ants post --wait` does: subscribed before it posts, reading back after its post.
        let ops: ChannelId = "ops".parse().unwrap();
        let wait = async |client: &async_nats::Client, js: &Context, post: &ChannelPost| {
            let room = client.subscribe(subject::chan_room(&ops)).await.unwrap();
            client.flush().await.unwrap();
            let stored = bus::store_acked(js, post.subject(), post, &post.id.to_string()).await.unwrap();
            let me = Me { id: post.from.clone(), role: Role::by_default(&post.from) };
            let posts = channel::caught_up_posts(room, js, &me, post, stored.as_ref().map(|ack| ack.sequence));
            (stored.is_some(), Box::pin(channel::thread_posts(posts, post, true)))
        };
        let next = async |waiter: &mut (dyn futures::Stream<Item = ChannelPost> + Unpin)| {
            tokio::time::timeout(Duration::from_secs(60), waiter.next()).await.expect("no post").unwrap()
        };
        let top = ChannelPost::new(ops.clone(), human.clone(), "on the leaf");
        let (stored, mut on_leaf) = wait(&h_leaf, &h_leaf_js, &top).await;
        assert!(stored);
        let ant_top = ChannelPost::new(ops.clone(), dev_a.clone(), "an ant on the leaf");
        let (stored, mut ant_on_leaf) = wait(&a_leaf, &a_leaf_js, &ant_top).await;
        assert!(stored);
        let hub_top = ChannelPost::new(ops.clone(), human.clone(), "on the hub");
        let (stored, mut on_hub) = wait(&h, &h_js, &hub_top).await;
        assert!(stored);

        reload(dead_port, false).await;
        let reply = |to: &ChannelPost, from: &UserId, text: &str| {
            channel::human_post(ops.clone(), Some(to.thread_id.clone()), from.clone(), None, text.to_owned())
        };
        let (away, ant_away) = (reply(&top, &dev_b, "away"), reply(&ant_top, &dev_b, "ant away"));
        for post in [&away, &ant_away] {
            assert_eq!(bus::store(&b_js, post.subject(), post, &post.id.to_string()).await.unwrap(), bus::Stored::Hub);
        }
        // Kept in the outbox, so never published on its subject.
        let outboxed = reply(&hub_top, &dev_a, "from the outbox");
        let stored = bus::store(&a_leaf_js, outboxed.subject(), &outboxed, &outboxed.id.to_string()).await.unwrap();
        assert_eq!(stored, bus::Stored::Outbox);
        let kept = ChannelPost::new(ops.clone(), human.clone(), "kept");
        let (stored, mut on_kept) = wait(&h_leaf, &h_leaf_js, &kept).await;
        assert!(!stored);
        let early = reply(&kept, &dev_b, "before the post reaches the hub");
        bus::store(&b_js, early.subject(), &early, &early.id.to_string()).await.unwrap();

        reload(leaf_port, true).await;
        assert_eq!(next(&mut on_leaf).await, away, "read back by message gets");
        assert_eq!(next(&mut ant_on_leaf).await, ant_away, "read back through a spare reader");
        assert_eq!(next(&mut on_hub).await, outboxed, "read back once the hub takes it from the outbox");
        let ants = h_js.get_stream(STREAM).await.unwrap();
        eventually("the hub takes the kept post", async || {
            ants.get_last_raw_message_by_subject(&kept.subject()).await.is_ok()
        })
        .await;

        reload(dead_port, false).await;
        let late = reply(&kept, &dev_b, "after the post");
        bus::store(&b_js, late.subject(), &late, &late.id.to_string()).await.unwrap();
        reload(leaf_port, true).await;
        assert_eq!(next(&mut on_kept).await, late, "only what the hub stored after the post kept in the outbox");

        let again = channel::CATCH_UP * 2;
        let quiet = async |waiter: &mut (dyn futures::Stream<Item = ChannelPost> + Unpin)| {
            tokio::time::timeout(again, waiter.next()).await.is_err()
        };
        let quiet = tokio::join!(quiet(&mut on_leaf), quiet(&mut ant_on_leaf), quiet(&mut on_hub), quiet(&mut on_kept));
        assert_eq!(quiet, (true, true, true, true), "each once, live or read back");
    }

    pub(crate) fn spawn_hub(leaf_port: u16, extra: &str) -> Option<NatsServer> {
        NatsServer::spawn_with(|dir| {
            auth::init(&dir.join("auth")).unwrap();
            format!(
                "jetstream {{ store_dir: {:?}, domain: hub }}\ninclude \"auth/{}\"\n\
                 leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n{extra}\n",
                dir.join("js"),
                auth::SERVER_CONF
            )
        })
    }

    /// Without `js_api_reply_v2` the hub's intake never hears from a leaf's outbox, and setting it up says so.
    #[tokio::test]
    async fn a_hub_without_js_api_reply_v2_is_reported() {
        let leaf_port = free_port();
        let Some(hub) = spawn_hub(leaf_port, "") else {
            return;
        };
        let team = hub.dir().join("auth");
        let domain: JsDomain = "hub".parse().unwrap();
        let m1: Machine = "m1".parse().unwrap();
        let human = UserId::human();
        let human_creds = hub.dir().join("human.creds");
        auth::issue(&team, &human, Role::Human, Some(&domain), &human_creds).unwrap();
        let (_h, h_js) = bus::connect(&hub.url, Some(&human_creds), Some(&domain), &human, "test").await.unwrap();
        bus::init_stream(&h_js).await.unwrap();
        let on_m1 = auth::members(std::slice::from_ref(&human), &[]);
        let leaf_creds = hub.dir().join("leaf-m1.creds");
        auth::issue_leaf(&team, &m1, &on_m1, &[], false, Some(&domain), &leaf_creds).unwrap();
        let (server_conf, listen) = (team.join(auth::SERVER_CONF), format!("127.0.0.1:{}", free_port()));
        let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
        let _leaf_node = NatsServer::spawn_with(|dir| {
            let config = leaf::Leaf {
                machine: &m1,
                hub_domain: Some(&domain),
                uplinks: &uplinks,
                creds: &leaf_creds,
                system_creds: None,
                server_conf: &server_conf,
                ca_file: None,
                proxy: None,
                listen: &listen,
            };
            leaf::write(&config, &dir.join("leaf.conf")).unwrap();
            "include \"leaf.conf\"\n".to_owned()
        })
        .unwrap();

        let setup = super::connect(&hub.url, Some(&human_creds), &human, &m1).await.unwrap();
        eventually("the leaf reaches the hub", async || admit(&setup, &h_js, &m1, &on_m1).await.is_ok()).await;
        let error = sourced(&h_js, &m1, Duration::from_secs(3)).await.unwrap_err().to_string();
        assert!(error.contains("feature_flags { js_api_reply_v2: true }"), "{error}");
        assert_eq!(sourcing(&h_js, &m1, &on_m1).await.unwrap().hearing, Hearing::Silent);
    }

    #[test]
    fn a_source_error_or_a_missing_source_is_a_failure() {
        let m1: Machine = "m1".parse().unwrap();
        let info = |source: serde_json::Value| serde_json::json!({ "sources": [source] });
        let ours = serde_json::json!({ "api": "$JS.m1.API", "deliver": "$JS.S.m1" });
        assert_eq!(
            hearing(&info(serde_json::json!({ "name": OUTBOX_STREAM, "external": ours, "active": 12 })), &m1),
            Hearing::Heard
        );
        let failed = info(serde_json::json!({
            "name": OUTBOX_STREAM, "external": ours, "active": -1, "error": { "code": 503, "description": "boom" },
        }));
        assert_eq!(hearing(&failed, &m1), Hearing::Failed("boom".to_owned()));
        let other =
            info(serde_json::json!({ "name": OUTBOX_STREAM, "external": { "api": "$JS.m2.API" }, "active": 1 }));
        assert!(matches!(hearing(&other, &m1), Hearing::Failed(_)));
    }

    async fn outbox_missing_fails(js: &Context) {
        // dev-b's inbox subject for dev-a has no stream on the leaf while the outbox is missing: ants.out.* is local.
        let envelope = Envelope::new("dev-b".parse().unwrap(), "dev-a".parse().unwrap(), "x");
        let outboxed = subject::outbox(&envelope.subject()).unwrap();
        let message = PublishMessage::build().payload(serde_json::to_vec(&envelope).unwrap().into());
        assert!(js.send_publish(outboxed, message).await.unwrap().await.is_err());
    }

    fn filters(sources: &[Source]) -> Vec<&str> {
        sources.iter().flat_map(|s| s.subject_transforms.iter().map(|t| t.source.as_str())).collect()
    }

    #[test]
    fn with_humans_on_a_machine_no_two_admissions_overlap() {
        let m1: Machine = "m1".parse().unwrap();
        let ants = ["dev-a".parse().unwrap(), UserId::human(), "dev-b".parse().unwrap()];
        let sources = admissions(&m1, &auth::members(&ants, &[]), 7);
        assert_eq!(
            filters(&sources),
            [
                "ants.in.m1.ants.out.notes.*.dev-a",
                "ants.in.m1.ants.out.chan.*.dev-a",
                "ants.in.m1.ants.out.dead.dev-a",
                "ants.in.m1.ants.out.notes.*.dev-b",
                "ants.in.m1.ants.out.chan.*.dev-b",
                "ants.in.m1.ants.out.dead.dev-b",
                "ants.in.m1.ants.out.inbox.>",
                "ants.in.m1.ants.out.notes.*.human",
                "ants.in.m1.ants.out.chan.*.human",
            ]
        );
        assert!(sources.iter().all(|s| s.start_sequence == Some(7) && from_machine(s, &m1)));
        assert!(!from_machine(&sources[0], &"m".parse().unwrap()));

        // With a second human, the first by name takes every inbox subject; each human posts under its own name.
        let alice: UserId = "alice".parse().unwrap();
        let sources = admissions(&m1, &auth::members(&ants, &[alice.clone(), alice]), 7);
        assert_eq!(
            filters(&sources),
            [
                "ants.in.m1.ants.out.inbox.>",
                "ants.in.m1.ants.out.notes.*.alice",
                "ants.in.m1.ants.out.chan.*.alice",
                "ants.in.m1.ants.out.notes.*.dev-a",
                "ants.in.m1.ants.out.chan.*.dev-a",
                "ants.in.m1.ants.out.dead.dev-a",
                "ants.in.m1.ants.out.notes.*.dev-b",
                "ants.in.m1.ants.out.chan.*.dev-b",
                "ants.in.m1.ants.out.dead.dev-b",
                "ants.in.m1.ants.out.notes.*.human",
                "ants.in.m1.ants.out.chan.*.human",
            ]
        );
    }
}
