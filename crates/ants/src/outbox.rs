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
//! A machine's credentials thus never decide what the hub's stream stores: only the human, who sets up both hops here,
//! does. Each message keeps its `Nats-Msg-Id`, so the hub's stream drops a buffered copy of a message it already took
//! within its duplicate window.

use std::path::Path;
use std::time::Duration;

use ants_proto::{INTAKE_STREAM, Machine, OUTBOX_STREAM, OUTBOX_SUBJECTS, PersonaId, STREAM, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::ErrorCode;
use async_nats::jetstream::context::{GetStreamError, GetStreamErrorKind};
use async_nats::jetstream::stream::{self, External, Source, SubjectTransform};
use async_nats::jetstream::{self, Context};

/// How long an outbox and the intake keep messages: as long as the hub's stream.
const RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// How long to wait for the leaf node to answer through the hub.
const LEAF_TIMEOUT: Duration = Duration::from_secs(10);

/// Connect as the human to set up the outbox of `machine`, with the machine's own inbox prefix
/// ([`Machine::setup_inbox`]): the leaf node may answer there and nowhere else.
pub async fn connect(url: &str, creds: Option<&Path>, machine: &Machine) -> Result<async_nats::Client> {
    let mut options = async_nats::ConnectOptions::new().name("ants-cli").custom_inbox_prefix(machine.setup_inbox());
    if let Some(path) = creds {
        options =
            options.credentials_file(path).await.with_context(|| format!("reading credentials {}", path.display()))?;
    }
    options.connect(url).await.with_context(|| format!("connecting to NATS at {url}"))
}

/// Set up store and forward for `machine`, on which `personas` run: create or update the machine's outbox on its leaf
/// node (which must be connected), through `leaf` from [`connect`], and have the hub's stream take from it what those
/// personas may send. Admissions of personas no longer listed are dropped; those already in place are kept as they
/// are.
pub async fn admit(leaf: &async_nats::Client, hub: &Context, machine: &Machine, personas: &[PersonaId]) -> Result<()> {
    anyhow::ensure!(!personas.is_empty(), "a machine needs at least one persona");
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
    let wanted = admissions(machine, personas, start);
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

/// The hub stream's sources for `machine`: one per persona, so admitting or dropping one leaves the others as they
/// are. The filters of one stream's sources may not overlap, so when the human runs on the machine, its admission
/// takes every inbox subject and the others' only their plan and channel posts.
fn admissions(machine: &Machine, personas: &[PersonaId], start: u64) -> Vec<Source> {
    let human = personas.iter().any(PersonaId::is_human);
    let mut personas: Vec<&PersonaId> = personas.iter().collect();
    personas.sort();
    personas.dedup();
    personas
        .into_iter()
        .map(|persona| Source {
            name: INTAKE_STREAM.to_owned(),
            start_sequence: Some(start),
            subject_transforms: machine
                .admitted(persona, persona.is_human() || !human)
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
        let Some(hub) = NatsServer::spawn_with(|dir| {
            auth::init(&dir.join("auth")).unwrap();
            format!(
                "jetstream {{ store_dir: {:?}, domain: hub }}\ninclude \"auth/{}\"\n\
                 leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n\
                 feature_flags {{ js_api_reply_v2: true }}\n",
                dir.join("js"),
                auth::SERVER_CONF
            )
        }) else {
            return;
        };
        let team = hub.dir().join("auth");
        let domain: JsDomain = "hub".parse().unwrap();
        let m1: Machine = "m1".parse().unwrap();
        let (human, dev_a, dev_b): (PersonaId, PersonaId, PersonaId) =
            (PersonaId::human(), "dev-a".parse().unwrap(), "dev-b".parse().unwrap());
        let creds = |p: &PersonaId| {
            let out = hub.dir().join(format!("{p}.creds"));
            auth::issue(&team, p, Some(&domain), &out).unwrap();
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
        let on_m1 = [dev_a.clone(), human.clone()];
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
        let setup = super::connect(&hub.url, Some(&creds(&human)), &m1).await.unwrap();
        admit(&setup, &h_js, &m1, &on_m1).await.unwrap();
        admit(&setup, &h_js, &m1, &on_m1).await.unwrap();

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

    async fn outbox_missing_fails(js: &Context) {
        // dev-b's inbox subject for dev-a has no stream on the leaf while the outbox is missing: ants.out.* is local.
        let envelope = Envelope::new("dev-b".parse().unwrap(), "dev-a".parse().unwrap(), "x");
        let outboxed = subject::outbox(&envelope.subject()).unwrap();
        let message = PublishMessage::build().payload(serde_json::to_vec(&envelope).unwrap().into());
        assert!(js.send_publish(outboxed, message).await.unwrap().await.is_err());
    }

    #[test]
    fn with_the_human_on_a_machine_no_two_admissions_overlap() {
        let m1: Machine = "m1".parse().unwrap();
        let personas = ["dev-a".parse().unwrap(), PersonaId::human(), "dev-b".parse().unwrap()];
        let sources = admissions(&m1, &personas, 7);
        let filters: Vec<&str> =
            sources.iter().flat_map(|s| s.subject_transforms.iter().map(|t| t.source.as_str())).collect();
        assert_eq!(
            filters,
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
    }
}
