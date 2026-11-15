use std::time::Duration;

use ants_daemon::admin::ConfigFile;
use ants_proto::{AdminOp, OrderState, Permissions, Role};
use serde_json::{Value, json};

use crate::auth::tests::Server;
use crate::auth::{NewDevice, issue_device_with};
use crate::orders::send;
use crate::orders::tests::{WAIT, host_of_m1, p};
use crate::revoke::{self, Kind};

/// How long an order the signer answers may take: the daemon asks again every 5s.
pub(crate) const SIGNED: Duration = Duration::from_secs(30);

/// Run `body` while `ants signer` answers over `server` for the team of `server.dir()/auth`.
pub(crate) async fn with_signer<T>(server: &Server, body: impl Future<Output = T>) -> T {
    with_signer_at(&server.url, server.dir(), None, body).await
}

/// [`with_signer`] over the server at `url` of JetStream domain `js_domain`, for the team of `dir/auth`.
async fn with_signer_at<T>(
    url: &str,
    dir: &std::path::Path,
    js_domain: Option<&ants_proto::JsDomain>,
    body: impl Future<Output = T>,
) -> T {
    let config = dir.join("signer.toml");
    std::fs::write(&config, "").unwrap();
    let options = crate::signer::Options {
        config: &config,
        approval_timeout: SIGNED,
        reconnect: SIGNED * 3,
        tick: Duration::from_millis(200),
    };
    let team = dir.join("auth");
    let serving = crate::signer::serve(url, &team, js_domain, &options, |_, _| {}, std::future::pending());
    tokio::select! {
        r = serving => panic!("the signer stopped: {r:?}"),
        out = body => out,
    }
}

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
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    issue_device_with(&team, &alice, Kind::Human, rights, &admin, &admin_creds).unwrap();
    let admin = crate::Nats::new(&server.url, Some(&admin_creds), None).unwrap();
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
    let finding = ants_daemon::creds::check(&creds).unwrap();
    let result = added.result.unwrap();
    assert_eq!((finding.name.as_str(), Some(finding.key.as_str())), ("dev-b", result["key"].as_str()));
    assert!(!server.dir().join("dev-b.nk").exists(), "the waiting key goes once the credentials hold it");
    let issued = revoke::issued(&team).unwrap();
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
    ants_daemon::bus::connect(&server.url, Some(&creds), None, &dev_b, "test").await.expect("dev-b connects");

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
    assert!(!creds.exists(), "its credentials go");
    assert!(revoke::revoked(&team).unwrap().iter().any(|r| r.key == finding.key));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "its table goes, with the comment above it");
    assert!(work.exists(), "its workdir stays");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// The leaf node the daemon of [`an_add_order_on_a_leaf_node_s_machine_renews_its_uplink_to_carry_the_ant`] runs
/// beside: its process, which a reload signals.
static LEAF_NODE: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// [`crate::add::renew_and_reconnect`], reloading the [`LEAF_NODE`] by SIGHUP rather than by its service.
fn renew_and_signal(conf: &std::path::Path, renewed: &str) -> anyhow::Result<Option<String>> {
    crate::add::renew_and_apply(conf, renewed, |_| {
        let pid = LEAF_NODE.get().expect("the leaf node runs").to_string();
        let status = std::process::Command::new("kill").arg("-HUP").arg(pid).status().unwrap();
        assert!(status.success());
        crate::service::Outcome::Done("kill -HUP".to_owned())
    })
}

/// A real hub and a leaf node: an add order to the leaf's machine has its daemon ask across the uplink with the uplink
/// credentials too, the signer on the hub renew them to carry the ant, and the daemon have the leaf node use them,
/// so the ant reaches the hub through it; a retire order retires it the same way.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn an_add_order_on_a_leaf_node_s_machine_renews_its_uplink_to_carry_the_ant() {
    use ants_daemon::admin::LeafNode;
    use ants_daemon::admin::restart::Restarts;
    use ants_daemon::host::{self, Host};
    use ants_proto::{JsDomain, Machine};
    use ants_test_support::NatsServer;

    use crate::auth::{self, issue_host};
    use crate::members::{self, EnrollApprover};
    use crate::outbox::tests::{eventually, free_port, spawn_hub};
    use crate::{bus, leaf};

    let leaf_port = free_port();
    let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else { return };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let (alice, bob, dev_a, dev_b) = (p("alice"), p("bob"), p("dev-a"), p("dev-b"));
    let bob_creds = hub.dir().join("bob.creds");
    auth::issue(&team, &bob, Role::Human, Some(&domain), &bob_creds).unwrap();
    let (_h, h_js) = bus::connect(&hub.url, Some(&bob_creds), Some(&domain), &bob, "test").await.unwrap();
    bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = hub.dir().join("alice-admin.creds");
    let admin = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
    let rights = Permissions::for_user(&alice, Role::Human).with_js_domain(Some(&domain));
    issue_device_with(&team, &alice, Kind::Human, rights, &admin, &admin_creds).unwrap();

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
    let leaf = LeafNode { conf: leaf_node.dir().join("leaf.conf"), uplink: leaf::read_uplink, renew: renew_and_signal };
    let host = Host {
        machine: None,
        nats: leaf_node.url.clone(),
        creds: Some(host_creds),
        js_domain: Some(domain.clone()),
        remote_admin: true,
        config: Some(config),
        restarts: Restarts::default(),
        leaf: Some(leaf),
    };
    let (ants, watched) = tokio::sync::watch::channel(vec![dev_a.clone()]);
    let task = tokio::spawn(host::run(host, watched));
    eventually("m1's daemon records its ants in the hub", async || {
        !crate::machines::machines_of(&h_js, &dev_a).await.unwrap_or_default().is_empty()
    })
    .await;

    let admin = crate::Nats::new(&hub.url, Some(&admin_creds), Some(&domain)).unwrap();
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

    let retire = Box::pin(send(&admin, &js, Some(m1), &dev_b, AdminOp::Retire, SIGNED));
    let (_, retired) = with_signer_at(&hub.url, hub.dir(), Some(&domain), retire).await.unwrap();
    let retired = retired.expect("the daemon decides once the signer answers");
    assert_eq!((retired.state, retired.reason.as_deref()), (OrderState::Applied, None), "{retired:?}");
    assert!(!creds.exists(), "its credentials go");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "its table goes");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}
