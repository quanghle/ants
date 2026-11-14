//! `ants leaf outbox`: store and forward between a machine's leaf node and the hub.
//!
//! A message the hub's stream cannot take while the leaf's uplink is down waits in the leaf's own JetStream, in the
//! machine's outbox ([`OUTBOX_STREAM`]), and reaches the hub's stream in two hops once the uplink is back:
//!
//! 1. The hub's intake ([`INTAKE_STREAM`]) sources each machine's outbox through the machine's JetStream domain and
//!    stores everything under the machine's prefix ([`subject::intake`]), so a machine cannot pass for another.
//! 2. The hub's [`STREAM`] sources from the intake, per machine and persona, only the subjects the persona may publish
//!    itself ([`Machine::admitted`]), back on their usual subjects. Anything else a machine claims stays in the intake.
//!
//! A machine's credentials thus never decide what the hub's stream stores: only a human, who sets up both hops here,
//! does. Each message keeps its `Nats-Msg-Id`, so the hub's stream drops a buffered copy of a message it already took
//! within its duplicate window.

use std::path::Path;
use std::time::Duration;

use ants_proto::{INTAKE_STREAM, Machine, OUTBOX_STREAM, OUTBOX_SUBJECTS, PersonaId, Role, STREAM, subject};
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
pub async fn connect(
    url: &str,
    creds: Option<&Path>,
    human: &PersonaId,
    machine: &Machine,
) -> Result<async_nats::Client> {
    let mut options =
        async_nats::ConnectOptions::new().name("ants-cli").custom_inbox_prefix(machine.setup_inbox(human));
    if let Some(path) = creds {
        options =
            options.credentials_file(path).await.with_context(|| format!("reading credentials {}", path.display()))?;
    }
    options.connect(url).await.with_context(|| format!("connecting to NATS at {url}"))
}

/// Set up store and forward for `machine`, on which `members` run, personas and humans each in its role
/// ([`crate::auth::members`]): create or update the machine's outbox on its leaf node (which must be connected),
/// through `leaf` from [`connect`], and have the hub's stream take from it what those members may send. Admissions of
/// members no longer listed are dropped; those already in place are kept as they are.
pub async fn admit(
    leaf: &async_nats::Client,
    hub: &Context,
    machine: &Machine,
    members: &[(PersonaId, Role)],
) -> Result<()> {
    anyhow::ensure!(!members.is_empty(), "a machine needs at least one persona or human");
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
    let wanted = admissions(machine, members, start);
    let mut sources: Vec<Source> = config.sources.take().unwrap_or_default();
    let kept: Vec<Source> = sources.extract_if(.., |s| from_machine(s, machine)).collect();
    for source in wanted {
        let same = kept.iter().find(|k| k.subject_transforms == source.subject_transforms);
        sources.push(same.cloned().unwrap_or(source));
    }
    config.sources = Some(sources);
    hub.update_stream(&config).await.context("admitting the machine's outbox into the hub's stream")?;

    let mut intake = intake_config(hub).await?;
    let sources = intake.sources.get_or_insert_default();
    if !sources.iter().any(|s| outbox_of(s, machine)) {
        sources.push(intake_source(machine));
    }
    hub.create_or_update_stream(intake).await.context("sourcing the machine's outbox into the hub's intake")?;
    Ok(())
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

#[derive(Debug, PartialEq)]
enum Hearing {
    Heard,
    Silent,
    Failed(String),
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
    if let Some(sources) = config.sources.as_mut() {
        sources.retain(|s| !from_machine(s, machine));
        if sources.is_empty() {
            config.sources = None;
        }
    }
    hub.update_stream(&config).await.context("dropping the machine from the hub's stream")?;
    let mut intake = intake_config(hub).await?;
    if let Some(sources) = intake.sources.as_mut() {
        sources.retain(|s| !outbox_of(s, machine));
    }
    hub.create_or_update_stream(intake).await.context("dropping the machine from the hub's intake")?;
    Ok(())
}

fn not_found(e: &GetStreamError) -> bool {
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
/// first of them by name takes every inbox subject and the others' only their plan and channel posts.
fn admissions(machine: &Machine, members: &[(PersonaId, Role)], start: u64) -> Vec<Source> {
    let mut members: Vec<&(PersonaId, Role)> = members.iter().collect();
    members.sort_by(|(a, _), (b, _)| a.cmp(b));
    members.dedup_by(|(a, _), (b, _)| a == b);
    let inboxes = members.iter().find(|(_, role)| role.is_human()).map(|(human, _)| human);
    members
        .iter()
        .map(|(persona, role)| Source {
            name: INTAKE_STREAM.to_owned(),
            start_sequence: Some(start),
            subject_transforms: machine
                .admitted(persona, *role, inboxes.is_none_or(|human| human == persona))
                .into_iter()
                .map(|(source, destination)| SubjectTransform { source, destination })
                .collect(),
            ..Default::default()
        })
        .collect()
}

fn from_machine(source: &Source, machine: &Machine) -> bool {
    let prefix = format!("ants.in.{machine}.");
    let transforms = &source.subject_transforms;
    source.name == INTAKE_STREAM && !transforms.is_empty() && transforms.iter().all(|t| t.source.starts_with(&prefix))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use ants_daemon::{bus, plan};
    use ants_proto::{ChannelPost, Envelope, JsDomain, PlanPost, ThreadId};
    use ants_test_support::NatsServer;
    use async_nats::jetstream::message::PublishMessage;

    use super::*;
    use crate::{auth, leaf};

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
        for _ in 0..600 {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{what}: timed out");
    }

    async fn messages(js: &Context, name: &str) -> u64 {
        js.get_stream(name).await.unwrap().info().await.unwrap().state.messages
    }

    /// A real hub and leaf node: what a persona on the leaf sends while the uplink is down waits in the leaf's
    /// outbox and lands in the hub's stream once, after the uplink is back; what the machine may not send stays out.
    #[tokio::test]
    async fn the_hub_takes_what_a_leaf_kept_while_its_uplink_was_down() {
        let (leaf_port, dead_port) = (free_port(), free_port());
        let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else {
            return;
        };
        let team = hub.dir().join("auth");
        let domain: JsDomain = "hub".parse().unwrap();
        let m1: Machine = "m1".parse().unwrap();
        let (human, dev_a, dev_b): (PersonaId, PersonaId, PersonaId) =
            (PersonaId::human(), "dev-a".parse().unwrap(), "dev-b".parse().unwrap());
        let creds = |p: &PersonaId| {
            let out = hub.dir().join(format!("{p}.creds"));
            auth::issue(&team, p, Role::by_default(p), Some(&domain), &out).unwrap();
            out
        };
        let connect = async |url: &str, p: &PersonaId| {
            let (client, mut js) = bus::connect(url, Some(&creds(p)), Some(&domain), p, "test").await.unwrap();
            js.set_timeout(Duration::from_secs(2));
            (client, js)
        };
        let (_h, h_js) = connect(&hub.url, &human).await;
        bus::init_stream(&h_js).await.unwrap();

        let leaf_creds = hub.dir().join("leaf-m1.creds");
        // The CLI runs on m1 too.
        let alice: PersonaId = "alice".parse().unwrap();
        let on_m1 = auth::members(&[dev_a.clone(), human.clone()], std::slice::from_ref(&alice));
        auth::issue_leaf(&team, &m1, &on_m1, Some(&domain), &leaf_creds).unwrap();
        let server_conf = team.join(auth::SERVER_CONF);
        let listen = format!("127.0.0.1:{}", free_port());
        let configure = |dir: &Path, port: u16| {
            let uplinks = [format!("nats-leaf://127.0.0.1:{port}")];
            let config = leaf::Leaf {
                machine: &m1,
                hub_domain: Some(&domain),
                uplinks: &uplinks,
                creds: &leaf_creds,
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
        eventually("the leaf reaches the hub", async || bus::ensure_stream(&a_js).await.is_ok()).await;

        // Before the outbox exists, a publish that cannot reach the hub fails outright.
        outbox_missing_fails(&b_js).await;
        let setup = super::connect(&hub.url, Some(&creds(&human)), &human, &m1).await.unwrap();
        admit(&setup, &h_js, &m1, &on_m1).await.unwrap();
        sourced(&h_js, &m1, Duration::from_secs(5)).await.unwrap();
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

        reload(dead_port);
        eventually("the uplink is down", async || bus::ensure_stream(&a_js).await.is_err()).await;
        let kept = Envelope::new(dev_a.clone(), human.clone(), "kept");
        assert_eq!(bus::publish(&a_js, &kept).await.unwrap(), bus::Stored::Outbox);
        let thread: ThreadId = "t1".parse().unwrap();
        assert!(plan::publish(&a_js, &PlanPost::new(thread.clone(), dev_a.clone(), "plan"), None).await.unwrap());
        let chan = ChannelPost::new("ops".parse().unwrap(), dev_a.clone(), "@dev-b kept");
        let stored = bus::store(&a_js, chan.subject(), &chan, &chan.id.to_string()).await.unwrap();
        assert_eq!(stored, bus::Stored::Outbox);
        // dev-b may post this itself, but is not admitted from m1.
        let stray = PlanPost::new(thread.clone(), dev_b.clone(), "stray");
        assert!(plan::publish(&b_js, &stray, None).await.unwrap());
        assert!(messages(&h_js, STREAM).await == 1);

        reload(leaf_port);
        eventually("the intake takes the outbox", async || messages(&h_js, INTAKE_STREAM).await == 5).await;
        eventually("the hub's stream takes what dev-a sent", async || messages(&h_js, STREAM).await == 4).await;
        let ants = h_js.get_stream(STREAM).await.unwrap();
        let last = ants.get_last_raw_message_by_subject(&kept.subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&last.payload).unwrap(), kept);
        ants.get_last_raw_message_by_subject(&subject::plan(&thread, &dev_a)).await.unwrap();
        let last = ants.get_last_raw_message_by_subject(&chan.subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<ChannelPost>(&last.payload).unwrap(), chan);
        assert!(ants.get_last_raw_message_by_subject(&stray.subject()).await.is_err());
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(messages(&h_js, STREAM).await, 4, "stored once");

        remove(&h_js, &m1).await.unwrap();
        let info = h_js.get_stream(STREAM).await.unwrap().info().await.unwrap().config.clone();
        assert!(info.sources.unwrap_or_default().is_empty());
    }

    fn spawn_hub(leaf_port: u16, extra: &str) -> Option<NatsServer> {
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
        let human = PersonaId::human();
        let human_creds = hub.dir().join("human.creds");
        auth::issue(&team, &human, Role::Human, Some(&domain), &human_creds).unwrap();
        let (_h, h_js) = bus::connect(&hub.url, Some(&human_creds), Some(&domain), &human, "test").await.unwrap();
        bus::init_stream(&h_js).await.unwrap();
        let on_m1 = auth::members(std::slice::from_ref(&human), &[]);
        let leaf_creds = hub.dir().join("leaf-m1.creds");
        auth::issue_leaf(&team, &m1, &on_m1, Some(&domain), &leaf_creds).unwrap();
        let (server_conf, listen) = (team.join(auth::SERVER_CONF), format!("127.0.0.1:{}", free_port()));
        let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
        let _leaf_node = NatsServer::spawn_with(|dir| {
            let config = leaf::Leaf {
                machine: &m1,
                hub_domain: Some(&domain),
                uplinks: &uplinks,
                creds: &leaf_creds,
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
        let personas = ["dev-a".parse().unwrap(), PersonaId::human(), "dev-b".parse().unwrap()];
        let sources = admissions(&m1, &auth::members(&personas, &[]), 7);
        assert_eq!(
            filters(&sources),
            [
                "ants.in.m1.ants.out.plan.*.dev-a",
                "ants.in.m1.ants.out.chan.*.dev-a",
                "ants.in.m1.ants.out.plan.*.dev-b",
                "ants.in.m1.ants.out.chan.*.dev-b",
                "ants.in.m1.ants.out.inbox.>",
                "ants.in.m1.ants.out.plan.*.human",
                "ants.in.m1.ants.out.chan.*.human",
            ]
        );
        assert!(sources.iter().all(|s| s.start_sequence == Some(7) && from_machine(s, &m1)));
        assert!(!from_machine(&sources[0], &"m".parse().unwrap()));

        // With a second human, the first by name takes every inbox subject; each human posts under its own name.
        let alice: PersonaId = "alice".parse().unwrap();
        let sources = admissions(&m1, &auth::members(&personas, &[alice.clone(), alice]), 7);
        assert_eq!(
            filters(&sources),
            [
                "ants.in.m1.ants.out.inbox.>",
                "ants.in.m1.ants.out.plan.*.alice",
                "ants.in.m1.ants.out.chan.*.alice",
                "ants.in.m1.ants.out.plan.*.dev-a",
                "ants.in.m1.ants.out.chan.*.dev-a",
                "ants.in.m1.ants.out.plan.*.dev-b",
                "ants.in.m1.ants.out.chan.*.dev-b",
                "ants.in.m1.ants.out.plan.*.human",
                "ants.in.m1.ants.out.chan.*.human",
            ]
        );
    }
}
