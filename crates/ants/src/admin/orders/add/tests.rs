//! Tests of add orders: the flags of `ants ant add`, an ant added and retired, other orders decided meanwhile, a leaf's
//! uplink renewed, and concurrent adds.

use std::time::Duration;

use ants_daemon::admin::ConfigFile;
use ants_proto::{AdminOp, OrderState, Permissions, Role, STREAM, THREADS_BUCKET, chan_consumer, done_reader};
use serde_json::{Value, json};

use crate::admin::orders::outcome;
use crate::admin::orders::tests::send;
use crate::auth::{Issue, Registered};
use crate::credentials::issued::{self, Kind};
use crate::credentials::revoke;
use crate::test_support::Server;
use crate::test_support::orders::{SIGNED, WAIT, host_of_m1, p, with_signer, with_signer_at};

/// The settings of `ants ant add` are those its flags give, as an `[[ant]]` table holds them, without the name and,
/// unless given, the workdir.
#[test]
fn the_flags_of_ants_ant_add_give_the_order_s_settings() {
    use clap::Parser as _;
    #[derive(clap::Parser)]
    struct Flags {
        #[command(flatten)]
        options: crate::serve::AntOptions,
    }
    let flags = Flags::parse_from(["add", "--model", "gpt-5", "--capability", "rust,docs", "--idle-timeout", "20m"]);
    let settings = super::settings(&p("dev-b"), &flags.options).unwrap();
    let expected = json!({ "model": "gpt-5", "capabilities": ["rust", "docs"], "idle_timeout": "20m" });
    assert_eq!(Value::Object(settings), expected);
    let flags = Flags::parse_from(["add", "--workdir", "src/app"]);
    let settings = super::settings(&p("dev-b"), &flags.options).unwrap();
    assert_eq!(Value::Object(settings), json!({ "workdir": "src/app" }));
}

/// An add order has the daemon of the machine generate the ant's key, which `ants signer` signs on the order with no
/// one asked, write its credentials and append its table to the config file, which reloads; a retire order of that
/// ant, no template's, removes its table, has the signer revoke its key and deletes its credentials.
#[tokio::test]
async fn an_add_order_adds_an_ant_the_signer_signs_on_it_and_a_retire_order_revokes_it() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a, dev_b) = (p("alice"), p("bob"), p("dev-a"), p("dev-b"));
    let team = server.dir().join("auth");
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    ants_daemon::bus::ensure_colony(&h_js, ants_proto::COLONY_TTL).await.unwrap();
    h_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let threads = format!("KV_{THREADS_BUCKET}");
    let mut rights = Permissions::for_user(&alice, Role::Human);
    rights.publish.extend([
        format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", chan_consumer(&dev_b)),
        format!("$JS.API.CONSUMER.INFO.{threads}.{}", done_reader(&dev_b)),
    ]);
    Issue::new(&team, &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let admin = crate::connection::Nats::new(&server.url, Some(&admin_creds), None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();

    let work = server.dir().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let path = server.dir().join("config.toml");
    let text =
        format!("harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\n", work.display().to_string());
    std::fs::write(&path, &text).unwrap();
    let mut triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path.clone(), triggers.edits());
    let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;
    let m1: ants_proto::Machine = "m1".parse().unwrap();

    let settings = json!({ "workdir": work, "model": "gpt-5" });
    let Value::Object(settings) = settings else { unreachable!() };
    let add = AdminOp::Add { settings, assistant_of: None };
    let (order, added) =
        with_signer(&server, Box::pin(send(&admin, &js, Some(m1.clone()), &dev_b, add, SIGNED))).await.unwrap();
    let added = added.expect("the daemon decides once the signer answers");
    assert_eq!((added.state, added.reason.as_deref()), (OrderState::Applied, None));
    let creds = server.dir().join("dev-b.creds");
    let finding = ants_daemon::secrets::creds::check(&creds).unwrap();
    let result = added.result.unwrap();
    assert_eq!((finding.name.as_str(), Some(finding.key.as_str())), ("dev-b", result["key"].as_str()));
    assert!(!server.dir().join("dev-b.nk").exists(), "the waiting key goes once the credentials hold it");
    let issued = issued::issued(&team).unwrap();
    let signed = issued.iter().find(|i| i.key == finding.key).expect("the signer records the key");
    assert_eq!((signed.name.as_str(), signed.kind, signed.order), ("dev-b", Kind::Ant, Some(order.id)));
    let edited = std::fs::read_to_string(&path).unwrap();
    assert!(edited.starts_with(&text), "the ant's table is appended: {edited}");
    assert!(edited.contains(&format!("# added by alice's order {}\n[[ant]]\nname = \"dev-b\"", order.id)), "{edited}");
    let loaded = ants_daemon::config::load(&path).unwrap();
    let ant = loaded.ants.iter().find(|a| a.name == dev_b).expect("listed");
    assert_eq!((ant.model.as_deref(), ant.creds.as_deref()), (Some("gpt-5"), Some(creds.as_path())));
    let reload = tokio::time::timeout(WAIT, triggers.next()).await.expect("a reload at once");
    assert_eq!(reload, crate::reload::Trigger::Edited);
    let (_, worker) =
        ants_daemon::bus::connect(&server.url, Some(&creds), None, &dev_b, "test").await.expect("dev-b connects");
    let stream = ants_daemon::bus::open_stream(&worker, &dev_b).await.unwrap();
    drop(ants_daemon::bus::chan_consumer(&stream, &dev_b, WAIT, 5, 1).await.unwrap());
    drop(ants_daemon::provider::events::done::consumer(&worker, &dev_b).await.unwrap());
    let channel_stream = js.get_stream_no_info(STREAM).await.unwrap();
    let done_stream = js.get_stream_no_info(threads).await.unwrap();
    channel_stream.consumer_info(chan_consumer(&dev_b)).await.unwrap();
    done_stream.consumer_info(done_reader(&dev_b)).await.unwrap();

    let listed = AdminOp::Add { settings: Default::default(), assistant_of: None };
    let (_, again) = send(&admin, &js, Some(m1.clone()), &dev_a, listed, WAIT).await.unwrap();
    let again = again.expect("refused before the signer is asked");
    assert_eq!(again.state, OrderState::Refused);
    assert!(again.reason.as_deref().unwrap().contains("lists dev-a already"), "{again:?}");

    let (_, retired) =
        with_signer(&server, Box::pin(send(&admin, &js, Some(m1), &dev_b, AdminOp::Retire, SIGNED))).await.unwrap();
    let retired = retired.expect("the daemon decides once the signer answers");
    assert_eq!((retired.state, retired.reason.as_deref()), (OrderState::Applied, None));
    let result = retired.result.unwrap();
    assert_eq!((&result["stopped"], &result["revoked"]), (&json!(true), &json!([finding.key])));
    assert!(channel_stream.consumer_info(chan_consumer(&dev_b)).await.is_err(), "offline ant's channel consumer");
    assert!(done_stream.consumer_info(done_reader(&dev_b)).await.is_err(), "offline ant's done consumer");
    assert!(!creds.exists(), "its credentials go");
    assert!(revoke::revoked(&team).unwrap().iter().any(|r| r.key == finding.key));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "its table goes, with the comment above it");
    assert!(work.exists(), "its workdir stays");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// While an add order waits for `ants signer`, a pause order to another ant of the machine is decided at once, and
/// one to the ant being added waits behind the add, then pauses the ant added (ants-aysv7).
#[tokio::test]
async fn an_order_to_another_ant_is_decided_while_an_add_waits_for_the_signer() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a, dev_b) = (p("alice"), p("bob"), p("dev-a"), p("dev-b"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    ants_daemon::bus::ensure_colony(&h_js, ants_proto::COLONY_TTL).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let admin = crate::connection::Nats::new(&server.url, Some(&admin_creds), None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();

    let work = server.dir().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let path = server.dir().join("config.toml");
    let text =
        format!("harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\n", work.display().to_string());
    std::fs::write(&path, &text).unwrap();
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path.clone(), triggers.edits());
    let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;
    let m1: ants_proto::Machine = "m1".parse().unwrap();

    let Value::Object(settings) = json!({ "workdir": work }) else { unreachable!() };
    let add = AdminOp::Add { settings, assistant_of: None };
    let (add, none) = send(&admin, &js, Some(m1.clone()), &dev_b, add, Duration::ZERO).await.unwrap();
    assert_eq!(none, None, "no signer answers");
    let (behind, _) = send(&admin, &js, Some(m1.clone()), &dev_b, AdminOp::Pause, Duration::ZERO).await.unwrap();
    let (_, paused) = send(&admin, &js, Some(m1.clone()), &dev_a, AdminOp::Pause, WAIT).await.unwrap();
    let paused = paused.expect("decided while the add waits for the signer");
    assert_eq!((paused.state, paused.reason.as_deref()), (OrderState::Applied, None), "{paused:?}");
    assert!(std::fs::read_to_string(&path).unwrap().contains("paused = true"), "dev-a paused");
    assert_eq!(outcome(&js, &add).await.unwrap(), None, "the add waits for the signer");
    assert_eq!(outcome(&js, &behind).await.unwrap(), None, "the pause of dev-b waits behind its add");

    let decided = async {
        loop {
            if let Some(decided) = outcome(&js, &behind).await.unwrap() {
                return decided;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let behind =
        with_signer(&server, Box::pin(tokio::time::timeout(SIGNED, decided))).await.expect("decided after the add");
    let added = outcome(&js, &add).await.unwrap().expect("the add is decided first");
    assert_eq!((added.state, added.reason.as_deref()), (OrderState::Applied, None), "{added:?}");
    assert_eq!((behind.state, behind.reason.as_deref()), (OrderState::Applied, None), "{behind:?}");
    let loaded = ants_daemon::config::load(&path).unwrap();
    let paused: Vec<_> = loaded.ants.iter().map(|ant| (ant.name.as_str(), ant.paused)).collect();
    assert_eq!(paused, [("dev-a", true), ("dev-b", true)]);
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// The leaf node the daemon of [`an_add_order_on_a_leaf_node_s_machine_renews_its_uplink_to_carry_the_ant`] runs
/// beside: its process, which a reload signals.
static LEAF_NODE: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// [`crate::node::leaf::uplink::renew_and_reconnect`], reloading the [`LEAF_NODE`] by SIGHUP rather than by its service.
fn renew_and_signal(conf: &std::path::Path, renewed: &str) -> anyhow::Result<Option<String>> {
    crate::node::leaf::uplink::renew_and_apply(conf, renewed, |_| {
        let pid = LEAF_NODE.get().expect("the leaf node runs").to_string();
        let status = std::process::Command::new("kill").arg("-HUP").arg(pid).status().unwrap();
        assert!(status.success());
        crate::service::Outcome::Done("kill -HUP".to_owned())
    })
}

/// [`crate::node::leaf::uplink::shrink_and_reconnect`], reloading the [`LEAF_NODE`] by SIGHUP rather than by its service.
fn shrink_and_signal(conf: &std::path::Path, asked: &str, shrunk: &str) -> anyhow::Result<Option<String>> {
    crate::node::leaf::uplink::shrink_and_apply(conf, Some(asked), shrunk, |_| {
        let pid = LEAF_NODE.get().expect("the leaf node runs").to_string();
        let status = std::process::Command::new("kill").arg("-HUP").arg(pid).status().unwrap();
        assert!(status.success());
        crate::service::Outcome::Done("kill -HUP".to_owned())
    })
}

/// A real hub and a leaf node: an add order to the leaf's machine has its daemon ask across the uplink with the uplink
/// credentials too, the signer on the hub renew them to carry the ant, and the daemon have the leaf node use them,
/// so the ant reaches the hub through it; a retire order retires it the same way, the signer renewing the uplink
/// without it and dropping it from the leaf's members and the hub's outbox sourcing.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one scenario: the order, the renewed uplink, then the ant behind it")]
async fn an_add_order_on_a_leaf_node_s_machine_renews_its_uplink_to_carry_the_ant() {
    use ants_daemon::admin::LeafNode;
    use ants_daemon::admin::restart::Restarts;
    use ants_daemon::bus;
    use ants_daemon::host::{self, Host};
    use ants_proto::{JsDomain, Machine};
    use ants_test_support::{NatsServer, free_port};

    use crate::auth::{self, issue_host};
    use crate::node::leaf;
    use crate::node::members::{self, EnrollApprover};
    use crate::test_support::{eventually, spawn_hub};

    let leaf_port = free_port();
    let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else { return };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let (alice, bob, dev_a, dev_b) = (p("alice"), p("bob"), p("dev-a"), p("dev-b"));
    let bob_creds = hub.dir().join("bob.creds");
    crate::test_support::issue(&team, &bob, Role::Human, Some(&domain), &bob_creds).unwrap();
    let (_h, h_js) = bus::connect(&hub.url, Some(&bob_creds), Some(&domain), &bob, "test").await.unwrap();
    bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = hub.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human).with_js_domain(Some(&domain));
    Issue::new(&team, &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();

    let uplink_creds = hub.dir().join("leaf-m1.creds");
    let on_m1 = auth::members(std::slice::from_ref(&dev_a), std::slice::from_ref(&alice));
    auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, Some(&domain), &uplink_creds).unwrap();
    let leaf_node = NatsServer::spawn_with(|dir| {
        let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
        let config = leaf::Leaf {
            machine: &m1,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &uplink_creds,
            system_creds: None,
            server_conf: &team.join(auth::SERVER_CONF),
            ca_file: None,
            proxy: None,
            listen: &format!("127.0.0.1:{}", free_port()),
        };
        leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();
    LEAF_NODE.set(leaf_node.pid()).unwrap();

    let host_creds = hub.dir().join("host-m1.creds");
    issue_host(&team, &m1, Some(&domain), &host_creds).unwrap();
    let work = hub.dir().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let path = hub.dir().join("config.toml");
    let text =
        format!("harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\n", work.display().to_string());
    std::fs::write(&path, &text).unwrap();
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path.clone(), triggers.edits());
    let leaf = LeafNode {
        conf: leaf_node.dir().join("leaf.conf"),
        uplink: leaf::read_uplink,
        renew: renew_and_signal,
        shrink: shrink_and_signal,
    };
    let host = Host {
        machine: None,
        fallback: None,
        nats: leaf_node.url.clone(),
        creds: Some(host_creds),
        js_domain: Some(domain.clone()),
        remote_admin: true,
        config: Some(config),
        restarts: Restarts::default(),
        providers: Default::default(),
        leaf: Some(leaf),
    };
    let (ants, watched) = tokio::sync::watch::channel(vec![dev_a.clone()]);
    let task = tokio::spawn(host::run(host, watched));
    eventually("m1's daemon records its ants in the hub", async || {
        !crate::ants::machines::machines_of(&h_js, &dev_a).await.unwrap_or_default().is_empty()
    })
    .await;

    let admin = crate::connection::Nats::new(&hub.url, Some(&admin_creds), Some(&domain)).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();
    let settings = json!({ "workdir": work });
    let Value::Object(settings) = settings else { unreachable!() };
    let add = AdminOp::Add { settings, assistant_of: None };
    let sending = Box::pin(send(&admin, &js, Some(m1.clone()), &dev_b, add, SIGNED));
    let (_, added) = with_signer_at(&hub.url, hub.dir(), Some(&domain), sending).await.unwrap();
    let added = added.expect("the daemon decides once the signer answers");
    assert_eq!((added.state, added.reason.as_deref()), (OrderState::Applied, None), "{added:?}");
    let result = added.result.unwrap();
    let uplink = result["uplink"].as_str().unwrap_or_else(|| panic!("{result}"));
    assert!(uplink.contains("renewed to carry dev-b") && uplink.contains("kill -HUP"), "{uplink}");
    assert_eq!(members::issued(&team, &m1).unwrap().unwrap().ants, [dev_a.clone(), dev_b.clone()]);
    let carried = members::uplink(&leaf_node.dir().join("leaf.conf"), &m1).unwrap();
    assert!(carried.ants.contains(&dev_b), "the uplink credentials carry dev-b: {carried}");
    let creds = hub.dir().join("dev-b.creds");
    let (_b, b_js) = bus::connect(&leaf_node.url, Some(&creds), Some(&domain), &dev_b, "test").await.unwrap();
    eventually("dev-b reaches the hub through the leaf", async || bus::open_stream(&b_js, &dev_b).await.is_ok()).await;

    // The renewal's reload left the pulls in flight through the uplink silent: the daemon pulls anew at once (ants-dbf0s),
    // and an order the stream delivered to the silent request before it dropped it comes again after a short ack wait.
    let retire = Box::pin(send(&admin, &js, Some(m1.clone()), &dev_b, AdminOp::Retire, WAIT));
    let (_, retired) = with_signer_at(&hub.url, hub.dir(), Some(&domain), retire).await.unwrap();
    let retired = retired.expect("the daemon decides once the signer answers");
    assert_eq!((retired.state, retired.reason.as_deref()), (OrderState::Applied, None), "{retired:?}");
    assert!(!creds.exists(), "its credentials go");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "its table goes");
    let result = retired.result.unwrap();
    let uplink = result["uplink"].as_str().unwrap_or_else(|| panic!("{result}"));
    assert!(uplink.contains("renewed without dev-b") && uplink.contains("kill -HUP"), "{uplink}");
    assert_eq!(members::issued(&team, &m1).unwrap().unwrap().ants, std::slice::from_ref(&dev_a));
    let carried = members::uplink(&leaf_node.dir().join("leaf.conf"), &m1).unwrap();
    assert_eq!(carried.ants, std::slice::from_ref(&dev_a), "the uplink credentials no longer carry dev-b: {carried}");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// Several add orders sent at once to one machine leave its daemon responsive: each ant added starts and the ant
/// served before carries on, while the reloads the orders and the new ants' shared settings trigger come back to back
/// (ants-ocu1r). The daemon runs on a current-thread runtime of its own, as `ants daemon` does, whose heartbeat stops
/// should anything block its thread.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one scenario: the daemon, the orders at once, then the ants served")]
async fn add_orders_sent_at_once_leave_the_daemon_serving_every_ant() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use ants_daemon::host::Host;
    use ants_proto::Machine;

    use crate::auth::issue_host;
    use crate::serve;

    let Some(server) = Server::start() else { return };
    let dir = server.dir().to_owned();
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let rights = crate::people::humans::Rights::from_creds(Some(&server.creds_as(&alice, Role::Human))).unwrap();
    ants_daemon::hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let team = dir.join("auth");
    let admin_creds = dir.join("alice-admin.creds");
    let device = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&team, &alice, Kind::Human, rights).with(&device).into_file(&admin_creds).unwrap();
    let admin = crate::connection::Nats::new(&server.url, Some(&admin_creds), None).unwrap();
    let (_, admin_js) = admin.connect("test").await.unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let host_creds = dir.join("host-m1.creds");
    issue_host(&team, &m1, None, &host_creds).unwrap();

    let config = dir.join("config.toml");
    let (a_creds, a_home) = (server.creds_as(&dev_a, Role::Ant), dir.join("homes/dev-a"));
    std::fs::create_dir_all(&a_home).unwrap();
    let text = format!(
        "messenger = false\nsage = \"none\"\nharness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {a_home:?}\n\
         home = {a_home:?}\ncreds = {a_creds:?}\n"
    );
    std::fs::write(&config, text).unwrap();

    let beats = Arc::new(AtomicU64::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let daemon = {
        let (config, url, beats) = (config.clone(), server.url.clone(), beats.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async move {
                tokio::spawn(async move {
                    loop {
                        beats.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                });
                let mut triggers =
                    crate::reload::Triggers::new(config.clone(), None, Duration::from_secs(3600)).unwrap();
                let edited = ConfigFile::new(config.clone(), triggers.edits());
                triggers.watch_shared(edited.shared.subscribe());
                // Reloads besides the orders' and the shared settings', as a busy machine has, while the ants start.
                let edits = triggers.edits();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        edits.notify_one();
                    }
                });
                let served = || {
                    let loaded = ants_daemon::config::load(&config)?;
                    edited.shared.listed(&loaded.ants);
                    let configs = serve::configs(loaded.ants, &loaded.harnesses, &url, None, Some(&edited.shared))?;
                    let host = Host {
                        machine: None,
                        fallback: None,
                        nats: url.clone(),
                        creds: Some(host_creds.clone()),
                        js_domain: None,
                        remote_admin: true,
                        config: Some(edited.clone()),
                        restarts: Default::default(),
                        providers: Default::default(),
                        leaf: None,
                    };
                    let ants = configs.iter().map(|c| c.ant.clone()).collect();
                    let shared = Some(edited.shared.clone());
                    let (schedules, metrics, sense, machine) = (Vec::new(), None, None, None);
                    let host = Some((host, ants));
                    anyhow::Ok(serve::Served { configs, schedules, metrics, host, sense, shared, machine })
                };
                let stopping = async {
                    let _ = stopped.await;
                };
                serve::run(served().unwrap(), served, triggers, stopping, std::future::pending()).await
            })
        })
    };
    let lookup = ants_daemon::mcp::jetstream_lookup(js.clone());
    let listed = async |name: &str| {
        while lookup(p(name)).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let alive = async || {
        // The daemon's heartbeat, checked meanwhile: it stops when anything blocks the daemon's thread.
        loop {
            let before = beats.load(Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_ne!(beats.load(Ordering::Relaxed), before, "the daemon's thread is blocked");
        }
    };
    tokio::select! {
        r = tokio::time::timeout(SIGNED, listed("dev-a")) => r.expect("dev-a served"),
        () = alive() => unreachable!(),
    }

    let names = ["dev-b", "dev-c", "dev-d"];
    let add = |name: &str| {
        let home = dir.join("homes").join(name);
        let Value::Object(settings) = json!({ "home": home }) else { unreachable!() };
        let (admin, admin_js, m1, ant) = (&admin, &admin_js, m1.clone(), p(name));
        async move {
            let op = AdminOp::Add { settings, assistant_of: None };
            send(admin, admin_js, Some(m1), &ant, op, SIGNED * 3).await
        }
    };
    let sending = async {
        let (b, c, d) = tokio::join!(add(names[0]), add(names[1]), add(names[2]));
        [b, c, d]
    };
    let outcomes = tokio::select! {
        outcomes = with_signer(&server, sending) => outcomes,
        () = alive() => unreachable!(),
    };
    for (name, outcome) in names.iter().zip(outcomes) {
        let (_, outcome) = outcome.unwrap();
        let outcome = outcome.unwrap_or_else(|| panic!("the daemon decides the order adding {name}"));
        assert_eq!((outcome.state, outcome.reason.as_deref()), (OrderState::Applied, None), "{name}: {outcome:?}");
    }
    let serving = async {
        for name in names {
            listed(name).await;
        }
    };
    tokio::select! {
        r = tokio::time::timeout(SIGNED, serving) => r.expect("every ant added is served"),
        () = alive() => unreachable!(),
    }
    let before = beats.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_ne!(beats.load(Ordering::Relaxed), before, "the daemon's thread is blocked");
    assert!(lookup(dev_a.clone()).await.unwrap().is_some(), "dev-a carries on");
    stop.send(()).unwrap();
    let ended = tokio::task::spawn_blocking(move || daemon.join()).await.unwrap();
    assert!(matches!(ended, Ok(Ok(serve::Ended::Replaced))), "{ended:?}");
}
