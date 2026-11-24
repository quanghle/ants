//! Tests of admin orders end to end: each decided once with why, sent through a leaf's uplink, retire, restart, set,
//! pause, resume and read memories orders, and the orders table.

use ants_daemon::admin::ConfigFile;
use ants_daemon::admin::restart::{Restarted, Restarts};
use ants_daemon::host::{self, Host};
use ants_daemon::records::direct_kv::DirectKv;
use ants_proto::{FieldChange, MACHINES_BUCKET, Permissions, Role};

use super::*;
use crate::auth::{Issue, Registered, issue_host};
use crate::credentials::issued::Kind;
use crate::test_support::Server;
use crate::test_support::orders::{WAIT, host_of_m1, host_of_m1_serving, p};

/// Send `op` as [`Admin::send`] does, signed with the admin credentials of `nats`: the order, and its outcome as the
/// daemon recorded it.
pub(crate) async fn send(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
    op: AdminOp,
    wait: Duration,
) -> Result<(AdminOrder, Option<AdminOutcome>)> {
    Admin::of(nats)?.outcome_of(js, machine, ant, op, wait).await
}

/// An admin's order reaches the daemon of the ant's machine, which records its outcome; one a human without the
/// role signed, it refuses with the reason, and every order while its config says `[admin] remote = false`. An
/// order to a machine whose daemon is away waits, pending. Only humans send orders, and only the machine's host
/// credentials record their outcomes, and only of orders to the machine. An order to an ant no machine lists names
/// `ants machines`, and while no machine recorded itself, host credentials.
#[tokio::test]
async fn a_machine_s_daemon_decides_each_order_to_its_ants_once_and_records_why() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let admin = crate::connection::Nats::new(&server.url, Some(&admin_creds), None, None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();
    let unrecorded = send(&admin, &js, None, &dev_a, AdminOp::Pause, WAIT).await.unwrap_err().to_string();
    assert!(unrecorded.contains("host credentials"), "no machine recorded itself: {unrecorded}");
    let (ants, task) = host_of_m1(&server, &js, true, None).await;
    let unlisted = send(&admin, &js, None, &p("dev-z"), AdminOp::Pause, WAIT).await.unwrap_err().to_string();
    assert!(unlisted.contains("no machine lists dev-z"), "{unlisted}");
    assert!(unlisted.contains("`ants machines`") && !unlisted.contains("host credentials"), "{unlisted}");

    let (pause, paused) = send(&admin, &js, None, &dev_a, AdminOp::Pause, WAIT).await.unwrap();
    let paused = paused.expect("the daemon decides at once");
    assert_eq!((paused.state, paused.reason.as_deref()), (OrderState::Refused, Some(ants_daemon::admin::NO_CONFIG)));
    assert_eq!((paused.id, paused.admin.as_ref(), paused.op.as_ref()), (pause.id, Some(&alice), Some(&AdminOp::Pause)));

    let bob_creds = server.creds_as(&bob, Role::Human);
    let bob_nats = crate::connection::Nats::new(&server.url, Some(&bob_creds), None, None).unwrap();
    let refused = send(&bob_nats, &js, None, &dev_a, AdminOp::Resume, WAIT).await.unwrap_err();
    assert!(refused.to_string().contains("no admin role"), "{refused:#}");
    let (jwt, key) = auth::read_creds(&bob_creds).unwrap();
    let mut forged = AdminOrder::new(bob.clone(), "m1".parse().unwrap(), dev_a.clone(), AdminOp::Retire, jwt);
    forged.signature = auth::sign(&key, &forged.signed()).unwrap();
    publish(&js, &forged).await.unwrap();
    let away = send(&admin, &js, Some("m2".parse().unwrap()), &dev_a, AdminOp::Resume, Duration::ZERO).await;
    let (away, none) = away.unwrap();
    assert_eq!(none, None, "no daemon of m2 takes it");

    let decided = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(outcome) = outcome(&js, &forged).await.unwrap() {
                return outcome;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let decided = decided.await.expect("refused");
    assert_eq!(decided.state, OrderState::Refused);
    assert!(decided.reason.as_deref().unwrap().contains("admin"), "{decided:?}");
    let listed = orders_of(&js, &dev_a).await.unwrap();
    let states: Vec<_> = listed.iter().map(|o| (o.id, o.machine.as_str(), o.state)).collect();
    assert_eq!(
        states,
        [
            (pause.id, "m1", OrderState::Refused),
            (forged.id, "m1", OrderState::Refused),
            (away.id, "m2", OrderState::Pending)
        ]
    );

    // Only humans send orders; only m1's host credentials record outcomes, of m1's orders.
    let (_, a_js) = server.connect_as(&dev_a, Role::Ant).await;
    let by_ant = AdminOrder { id: Uuid::now_v7(), ..pause.clone() };
    assert!(publish(&a_js, &by_ant).await.is_err(), "an ant sends no order");
    let as_human = DirectKv::new(&h_js, None, MACHINES_BUCKET).await.unwrap();
    assert!(as_human.put(by_ant.outcome_key(), b"{}".to_vec()).await.is_err(), "a human records no outcome");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();

    let creds = server.dir().join("host-m1.creds");
    let mut m_js = jetstream::new(
        async_nats::ConnectOptions::with_credentials_file(&creds)
            .await
            .unwrap()
            .custom_inbox_prefix(ants_proto::host_inbox_prefix(&"m1".parse().unwrap()))
            .connect(&server.url)
            .await
            .unwrap(),
    );
    m_js.set_timeout(Duration::from_secs(1));
    let as_host = DirectKv::new(&m_js, None, MACHINES_BUCKET).await.unwrap();
    assert!(as_host.put(away.outcome_key(), b"{}".to_vec()).await.is_err(), "m1 records no outcome of m2's");

    let (ants, task) = host_of_m1(&server, &js, false, None).await;
    let (_, off) = send(&admin, &js, None, &dev_a, AdminOp::Pause, WAIT).await.unwrap();
    let off = off.expect("the daemon decides at once");
    assert_eq!((off.state, off.reason.as_deref()), (OrderState::Refused, Some(ants_daemon::admin::REMOTE_OFF)));
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// A real hub and leaf node m1, whose daemon and an admin both connect to the leaf: the admin's order crosses the
/// uplink to the hub's stream and back to m1's daemon, which decides it. While the uplink is down, sending one
/// from m1 fails at once, saying the hub is out of reach, and nothing sends it later; one sent from the hub
/// waits, pending, until the uplink is back.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one scenario: orders through the uplink, then none while it is down")]
async fn an_admin_behind_a_leaf_node_sends_orders_through_its_uplink_and_none_while_it_is_down() {
    use std::process::Command;

    use ants_proto::JsDomain;
    use ants_test_support::{NatsServer, free_port};

    use crate::node::leaf;
    use crate::node::members::EnrollApprover;
    use crate::test_support::{eventually, spawn_hub};

    let (leaf_port, dead_port) = (free_port(), free_port());
    let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else { return };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
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

    let leaf_creds = hub.dir().join("leaf-m1.creds");
    let on_m1 = auth::members(std::slice::from_ref(&dev_a), std::slice::from_ref(&alice));
    auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap();
    let (server_conf, listen) = (team.join(auth::SERVER_CONF), format!("127.0.0.1:{}", free_port()));
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

    let host_creds = hub.dir().join("host-m1.creds");
    issue_host(&team, &m1, Some(&domain), &host_creds).unwrap();
    let host = Host {
        machine: None,
        fallback: None,
        nats: leaf_node.url.clone(),
        creds: Some(host_creds),
        js_domain: Some(domain.clone()),
        remote_admin: true,
        config: None,
        restarts: Restarts::default(),
        providers: Default::default(),
        leaf: None,
    };
    let (ants, watched) = tokio::sync::watch::channel(vec![dev_a.clone()]);
    let task = tokio::spawn(host::run(host, watched));
    eventually("m1's daemon records its ants in the hub", async || {
        !machines::machines_of(&h_js, &dev_a).await.unwrap_or_default().is_empty()
    })
    .await;

    let on_leaf = crate::connection::Nats::new(&leaf_node.url, Some(&admin_creds), Some(&domain), None).unwrap();
    let (_, js) = on_leaf.connect("test").await.unwrap();
    let (pause, paused) = send(&on_leaf, &js, None, &dev_a, AdminOp::Pause, WAIT * 4).await.unwrap();
    let paused = paused.expect("m1's daemon decides it");
    assert_eq!((paused.state, paused.reason.as_deref()), (OrderState::Refused, Some(ants_daemon::admin::NO_CONFIG)));
    let stored = orders_of(&h_js, &dev_a).await.unwrap();
    assert_eq!(stored.iter().map(|o| o.id).collect::<Vec<_>>(), [pause.id], "the hub's stream stored it");

    reload(dead_port);
    eventually("the uplink is down", async || js.get_stream(ants_proto::STREAM).await.is_err()).await;
    let started = std::time::Instant::now();
    let down = send(&on_leaf, &js, Some(m1.clone()), &dev_a, AdminOp::Resume, WAIT).await.unwrap_err();
    assert!(started.elapsed() < WAIT, "it fails at once");
    let said = format!("{down:#}");
    assert!(said.contains("the hub is out of reach") && said.contains("no outbox keeps"), "{said}");
    let on_hub = crate::connection::Nats::new(&hub.url, Some(&admin_creds), Some(&domain), None).unwrap();
    let (_, hub_js) = on_hub.connect("test").await.unwrap();
    let (waiting, none) = send(&on_hub, &hub_js, None, &dev_a, AdminOp::Resume, Duration::ZERO).await.unwrap();
    assert_eq!(none, None, "m1's daemon is out of reach");

    reload(leaf_port);
    let resumed = tokio::time::timeout(WAIT * 12, async {
        loop {
            if let Some(outcome) = outcome(&h_js, &waiting).await.unwrap() {
                return outcome;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let resumed = resumed.await.expect("m1's daemon decides it once the uplink is back");
    assert_eq!(resumed.state, OrderState::Refused);
    let stored = orders_of(&h_js, &dev_a).await.unwrap();
    assert_eq!(
        stored.iter().map(|o| (o.id, o.state)).collect::<Vec<_>>(),
        [(pause.id, OrderState::Refused), (waiting.id, OrderState::Refused)],
        "nothing stored the order sent while the uplink was down"
    );
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// While a retire order waits for its ant to leave the colony, a settings read and a pause of another ant of the
/// machine are decided at once (ants-aysv7).
#[tokio::test]
async fn orders_to_other_ants_are_decided_while_a_retire_waits_for_its_ant_to_stop() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a, dev_b) = (p("alice"), p("bob"), p("dev-a"), p("dev-b"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    ants_daemon::bus::ensure_colony(&h_js, ants_proto::COLONY_TTL).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let device = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&device).into_file(&admin_creds).unwrap();
    let nats = crate::connection::Nats::new(&server.url, Some(&admin_creds), None, None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();

    let path = server.dir().join("config.toml");
    let b_creds = server.creds_as(&dev_b, Role::Ant);
    let text = format!(
        "harness = \"copilot\"\nhuman = \"human\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n\n[[ant]]\nname = \"dev-b\"\n\
         workdir = \".\"\ncreds = {b_creds:?}\n"
    );
    std::fs::write(&path, text).unwrap();
    let (_, b_js) = server.connect_as(&dev_b, Role::Ant).await;
    let colony = b_js.get_key_value(ants_proto::COLONY_BUCKET).await.unwrap();
    colony.put(dev_b.as_str(), b"{}".to_vec().into()).await.expect("dev-b in the colony, as its daemon puts it");
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path.clone(), triggers.edits());
    let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;

    let (retire, none) =
        send(&nats, &js, Some("m1".parse().unwrap()), &dev_b, AdminOp::Retire, Duration::ZERO).await.unwrap();
    assert_eq!(none, None);
    let (_, read) = send(&nats, &js, None, &dev_a, AdminOp::ReadSettings, WAIT).await.unwrap();
    let read = read.expect("decided while the retire waits for dev-b to stop");
    assert_eq!((read.state, read.reason.as_deref()), (OrderState::Applied, None), "{read:?}");
    let (_, paused) = send(&nats, &js, None, &dev_a, AdminOp::Pause, WAIT).await.unwrap();
    let paused = paused.expect("decided while the retire waits for dev-b to stop");
    assert_eq!((paused.state, paused.reason.as_deref()), (OrderState::Applied, None), "{paused:?}");
    assert_eq!(outcome(&js, &retire).await.unwrap(), None, "the retire waits for dev-b to leave the colony");
    let names: Vec<_> = ants_daemon::config::load(&path)
        .unwrap()
        .ants
        .into_iter()
        .filter(|a| a.builtin.is_none())
        .map(|a| (a.name, a.paused))
        .collect();
    assert_eq!(names, [(dev_a, true)], "dev-b unlisted before the wait, dev-a paused during it");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// A restart order reaches the daemon serving the ant, whose outcome tells what it stopped; one to an ant the
/// machine does not serve is refused.
#[tokio::test]
async fn a_restart_order_restarts_the_harnesses_of_an_ant_its_machine_serves() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let admin = crate::connection::Nats::new(&server.url, Some(&admin_creds), None, None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();
    let restarts = Restarts::default();
    let (ants, task) = host_of_m1_serving(&server, &js, true, None, restarts.clone()).await;
    let restart = AdminOp::Restart { now: true };

    let (_, refused) = send(&admin, &js, None, &dev_a, restart.clone(), WAIT).await.unwrap();
    let refused = refused.expect("the daemon decides at once");
    assert_eq!(
        (refused.state, refused.reason.as_deref()),
        (OrderState::Refused, Some("machine m1 serves no ant dev-a"))
    );

    let _served = restarts.serve(&dev_a);
    let mut listening = restarts.listen(&dev_a);
    let daemon = async {
        let request = listening.next().await;
        assert_eq!((&request.by, request.now), (&alice, true));
        request.done.send(Restarted { stopped: 1, after_prompt: 1, cancelled: 1 }).unwrap();
    };
    let (sent, ()) = tokio::join!(send(&admin, &js, None, &dev_a, restart, WAIT), daemon);
    let (order, applied) = sent.unwrap();
    let applied = applied.expect("the daemon decides once the restart is under way");
    assert_eq!((applied.state, applied.reason.as_deref()), (OrderState::Applied, None));
    let told = crate::admin::restart::told(Sent::of(order, Some(applied))).unwrap();
    assert_eq!(
        told,
        "restarting dev-a on m1: 1 idle harness stopped, 1 working stops once the prompt ends (1 cancelled)"
    );
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// A set order edits the ant's table in the machine's config file in place, comments kept, and has the daemon
/// reload it at once; one expecting a value the field no longer holds is refused naming the field, and one the
/// config loader rejects is refused too, both leaving the file as it was.
#[tokio::test]
async fn a_set_order_edits_the_config_in_place_unless_a_field_changed_or_the_file_would_not_load() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let admin = crate::connection::Nats::new(&server.url, Some(&admin_creds), None, None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();

    let path = server.dir().join("config.toml");
    let text = "harness = \"copilot\"\nhuman = \"human\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nmodel = \"gpt-5\"  # fast \
                enough\n\n# Keep dev-b on its own.\n[[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n";
    std::fs::write(&path, text).unwrap();
    let mut triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path.clone(), triggers.edits());
    let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;
    let set = |field: &str, expected: Option<Value>, value: Option<Value>| AdminOp::Set {
        changes: vec![FieldChange { field: vec![field.to_owned()], expected, expected_digest: None, value }],
    };

    let model = set("model", Some("gpt-5".into()), Some("o3".into()));
    let (_, applied) = send(&admin, &js, None, &dev_a, model, WAIT).await.unwrap();
    let applied = applied.expect("the daemon decides at once");
    assert_eq!((applied.state, applied.reason), (OrderState::Applied, None));
    let edited = std::fs::read_to_string(&path).unwrap();
    assert_eq!(edited, text.replace("\"gpt-5\"", "\"o3\""), "only the value changes, comments kept");
    let reload = tokio::time::timeout(WAIT, triggers.next()).await.expect("a reload at once");
    assert_eq!(reload, crate::reload::Trigger::Edited);

    let stale = set("model", Some("gpt-5".into()), Some("o4".into()));
    let (_, conflict) = send(&admin, &js, None, &dev_a, stale, WAIT).await.unwrap();
    let conflict = conflict.expect("the daemon decides at once");
    assert_eq!(conflict.state, OrderState::Refused);
    let reason = conflict.reason.unwrap();
    assert!(reason.starts_with("model changed since the order was sent"), "{reason}");

    let invalid = set("max_harnesses", None, Some("many".into()));
    let (_, invalid) = send(&admin, &js, None, &dev_a, invalid, WAIT).await.unwrap();
    let invalid = invalid.expect("the daemon decides at once");
    assert_eq!(invalid.state, OrderState::Refused);
    assert!(invalid.reason.as_deref().unwrap().contains("max_harnesses"), "{invalid:?}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), edited, "refusals leave the file alone");
    let quiet = tokio::time::timeout(Duration::from_millis(200), triggers.next()).await;
    assert!(quiet.is_err(), "no reload without an edit");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// A pause order sets `paused = true` in the ant's table of the machine's config file, the rest of the file as
/// it was, has the daemon reload it at once and credits the admin; a resume order removes it, and one finding
/// nothing to change is applied without an edit.
#[tokio::test]
async fn a_pause_order_sets_paused_in_the_config_and_a_resume_order_removes_it() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let device = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&device).into_file(&admin_creds).unwrap();
    let nats = crate::connection::Nats::new(&server.url, Some(&admin_creds), None, None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    let admin = Admin::of(&nats).unwrap();

    let path = server.dir().join("config.toml");
    let text = "harness = \"copilot\"\nhuman = \"human\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"  # here\n";
    std::fs::write(&path, text).unwrap();
    let mut triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path.clone(), triggers.edits());
    let pausers = config.pausers.clone();
    let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;

    let line = crate::admin::pause::order(&admin, &js, &dev_a, None, true, WAIT).await.unwrap();
    assert!(line.starts_with("dev-a paused on m1"), "{line}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), format!("{text}paused = true\n"));
    let reload = tokio::time::timeout(WAIT, triggers.next()).await.expect("a reload at once");
    assert_eq!(reload, crate::reload::Trigger::Edited);
    assert_eq!(pausers.take(&dev_a), Some(alice), "credited in the colony once reloaded");

    let line = crate::admin::pause::order(&admin, &js, &dev_a, None, true, WAIT).await.unwrap();
    assert!(line.starts_with("dev-a paused on m1"), "already paused: {line}");
    let quiet = tokio::time::timeout(Duration::from_millis(200), triggers.next()).await;
    assert!(quiet.is_err(), "no reload without an edit");

    let line = crate::admin::pause::order(&admin, &js, &dev_a, None, false, WAIT).await.unwrap();
    assert_eq!(line, "dev-a resumed on m1");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
    let reload = tokio::time::timeout(WAIT, triggers.next()).await.expect("a reload at once");
    assert_eq!(reload, crate::reload::Trigger::Edited);
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

/// A read_memories order has the daemon answer with the ant's core memories from its config file and its
/// harness's memory files in the workdir, which `ants ant memories` shows.
#[tokio::test]
async fn a_read_memories_order_answers_with_the_ant_s_core_and_harness_memories() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&admin).into_file(&admin_creds).unwrap();
    let admin = crate::connection::Nats::new(&server.url, Some(&admin_creds), None, None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();

    let workdir = server.dir().join("work");
    std::fs::create_dir_all(&workdir).unwrap();
    std::fs::write(workdir.join("AGENTS.md"), "Run the checks.\n").unwrap();
    let path = server.dir().join("config.toml");
    let text = format!(
        "harness = \"codex\"\nhuman = \"human\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\nwhoami = \"A tester.\"\ntasks = \
         \"Test.\"\n",
        workdir.display().to_string()
    );
    std::fs::write(&path, text).unwrap();
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let config = ConfigFile::new(path, triggers.edits());
    let (ants, task) = host_of_m1(&server, &js, true, Some(config)).await;

    let memories = crate::admin::memories::read(&admin, &js, None, &dev_a).await.unwrap();
    assert_eq!(memories.core.whoami.as_deref(), Some("A tester."));
    assert_eq!((memories.core.memory.as_deref(), memories.core.tasks.as_deref()), (None, Some("Test.")));
    assert_eq!(memories.harness.as_deref(), Some("codex"));
    let files: Vec<_> = memories.files.iter().map(|f| (Path::new(&f.path).file_name(), f.text.as_str())).collect();
    assert_eq!(files, [(Some("AGENTS.md".as_ref()), "Run the checks.\n")]);
    let shown = crate::admin::memories::render(&dev_a, &memories);
    assert!(shown.contains("whoami\n  A tester.\nmemory (unset)\n"), "{shown}");
    assert!(shown.contains("── AGENTS.md (16 B)\n  Run the checks.\n"), "{shown}");
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}

#[test]
fn the_table_lists_each_order_with_where_it_stands() {
    let now = Utc::now();
    let mut order = AdminOrder::new(
        "alice".parse().unwrap(),
        "box".parse().unwrap(),
        "dev-a".parse().unwrap(),
        AdminOp::Restart { now: true },
        "",
    );
    order.sent_at = now - chrono::Duration::minutes(5);
    let pending = Listed::pending(order.clone(), order.machine.clone(), order.ant.clone());
    let outcome = AdminOutcome::of(&order, OrderState::Refused, Some("not supported yet".into()), None);
    let table = table(&[pending, outcome.into()], now);
    let lines: Vec<&str> = table.lines().collect();
    assert_eq!(lines[1], "5m ago  alice  box      restart now  pending", "{table}");
    assert_eq!(lines[2], "5m ago  alice  box      restart now  refused: not supported yet", "{table}");
}
