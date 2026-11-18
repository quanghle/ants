mod trails;

use ants_daemon::config::Ant;
use ants_daemon::monitoring::storage;
use ants_daemon::monitoring::storage::Usage;
use ants_daemon::secrets::creds::Seed;
use ants_daemon::secrets::keystore;
use ants_daemon::secrets::keystore::Store as _;
use ants_daemon::{bus, hub};
use ants_proto::{Machine, PERMISSIONS_VERSION, Role, STREAM, UserId};

use super::colony::{avatars, drift, harness_versions};
use super::credentials::{check_key, check_xkey};
use super::server::{consumer_limit, consumers, storage_check, supported_server};
use super::transports::leafnode_info;
use super::*;
use crate::avatars::{Depth, Faces};
use crate::credentials::{backup, revoke};
#[cfg(unix)]
use crate::node::bridge;
use crate::node::leaf;
use crate::node::members::EnrollApprover;
use crate::release::versions;
#[cfg(unix)]
use crate::service::{Action, Service};
use crate::test_support::Server;

fn setup(server: &Server, creds: Option<PathBuf>) -> Setup {
    Setup {
        nats: server.url.clone(),
        creds,
        js_domain: None,
        config: server.dir().join("no-config.toml"),
        leaf_config: server.dir().join("no-leaf.conf"),
        hub_config: server.dir().join("no-nats.conf"),
        harnesses: false,
        team: server.dir().join("no-team"),
        webhooks: server.dir().join("no-webhooks.toml"),
        home: None,
        worker: None,
    }
}

fn named<'a>(checks: &'a [Check], name: &str) -> Vec<&'a Check> {
    checks.iter().filter(|c| c.check == name).collect()
}

#[tokio::test]
async fn a_fresh_hub_is_missing_its_stream_until_init() {
    let Some(server) = Server::start() else { return };
    let alice = "alice".parse::<UserId>().unwrap();
    let setup = setup(&server, Some(server.creds_as(&alice, Role::Human)));

    let checks = Box::pin(run(&setup)).await;
    assert_eq!(named(&checks, "server")[0].status, Status::Ok, "{checks:#?}");
    assert_eq!(named(&checks, "jetstream")[0].status, Status::Ok, "{checks:#?}");
    let hub = named(&checks, "hub");
    let stream = hub.iter().find(|c| c.detail == format!("stream {STREAM} is missing")).expect("missing stream");
    assert_eq!((stream.status, stream.fix.as_slice()), (Status::Fail, ["ants init".to_owned()].as_slice()));
    assert_eq!(named(&checks, "config")[0].status, Status::Skip);
    assert_eq!(named(&checks, "leaf")[0].status, Status::Skip);

    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let rights = Rights::from_creds(setup.creds.as_deref()).unwrap();
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let checks = Box::pin(run(&setup)).await;
    assert!(checks.iter().all(|c| matches!(c.status, Status::Ok | Status::Skip)), "{checks:#?}");
    let hub = named(&checks, "hub");
    assert_eq!(hub.len(), 1, "{checks:#?}");
    assert!(hub[0].detail.contains("ants_presence, bucket ants_identities not checked"), "{}", hub[0]);
}

#[test]
fn a_server_older_than_nats_server_2_15_is_unsupported() {
    assert!(supported_server("2.15.0-beta.1"));
    assert!(supported_server("2.16.1"));
    assert!(supported_server("3.0.0"));
    assert!(!supported_server("2.14.9"));
    assert!(!supported_server("2.10.0"));
    assert!(supported_server("unknown"));
}

#[test]
fn consumers_warn_near_the_limit_the_hub_enforces() {
    let mut config = jetstream::stream::Config::default();
    assert_eq!(consumer_limit(&config), 1000);
    config.max_consumers = -1;
    assert_eq!(consumer_limit(&config), 1000, "-1 leaves the default");
    config.max_consumers = hub::MAX_CONSUMERS;
    assert_eq!(consumer_limit(&config), 10_000);

    let stream = hub::Object::Stream;
    assert_eq!(consumers(stream, 799, 1000).status, Status::Ok);
    let near = consumers(stream, 800, 1000);
    assert_eq!(near.status, Status::Warn);
    assert_eq!(near.fix, [format!("nats stream edit {STREAM} --max-consumers 2000")]);
    let full = consumers(stream, 9_000, 10_000);
    assert_eq!(full.fix, [format!("nats stream edit {STREAM} --max-consumers 20000")]);
    let humans = consumers(hub::Object::Humans, 900, 1000);
    assert_eq!(
        humans.detail,
        "900 on stream KV_ants_humans, which takes 1000: once full, no daemon can watch bucket ants_humans"
    );
    assert_eq!(humans.fix, ["nats stream edit KV_ants_humans --max-consumers 2000"]);
}

#[test]
fn storage_warns_past_the_share_naming_the_largest_streams() {
    let config = Path::new("ants.toml");
    let usage = |used, limit| Usage { domain: None, leaf: false, store: storage::Store::File, used, limit };
    assert_eq!(storage_check(&usage(79, Some(100)), &[], 80, config).status, Status::Ok);
    let full = storage_check(&usage(90, Some(100)), &[("ANTS".to_owned(), 70)], 80, config);
    assert_eq!(full.status, Status::Warn);
    assert!(full.detail.ends_with("past 80%: once full, JetStream refuses new messages; using most: ANTS 70 B"));
    assert!(full.fix[0].contains("max_file_store"), "{:?}", full.fix);
    let unknown = storage_check(&usage(90, None), &[], 80, config);
    assert_eq!(unknown.status, Status::Ok);
    assert!(
        unknown
            .detail
            .ends_with("limit unknown: set max_file_store in [storage] of ants.toml to be warned before it fills")
    );
}

#[tokio::test]
async fn the_versions_check_waits_for_every_component_to_allow_renewal() {
    let Some(server) = Server::start() else { return };
    let alice = "alice".parse::<UserId>().unwrap();
    let setup = setup(&server, Some(server.creds_as(&alice, Role::Human)));
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let rights = Rights::from_creds(setup.creds.as_deref()).unwrap();
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let versions =
        async || Box::pin(run(&setup)).await.into_iter().find(|c| c.check == "versions").expect("a versions check");

    let dev_a = "dev-a".parse::<UserId>().unwrap();
    let (_, a_js) = server.connect_as(&dev_a, Role::Ant).await;
    let colony = a_js.get_key_value(ants_proto::COLONY_BUCKET).await.unwrap();
    let now = chrono::Utc::now();
    let mut entry = serde_json::json!({
        "ant": "dev-a", "machine": "m1", "instance": "run-1", "harness": "copilot", "capabilities": [],
        "started_at": now, "heartbeat_at": now, "activity": "idle", "handling": 0, "harnesses": 0,
        "version": "0.0.1", "perms": PERMISSIONS_VERSION.0 - 1,
    });
    colony.put("dev-a", serde_json::to_vec(&entry).unwrap().into()).await.unwrap();

    let manager = UserId::channel_manager();
    let creds = server.dir().join("channel-manager.creds");
    auth::issue_channel_manager(&server.dir().join("auth"), None, &creds).unwrap();
    let (_, m_js) = bus::connect(&server.url, Some(&creds), None, &manager, "test").await.unwrap();
    let published =
        ants_daemon::records::direct_kv::DirectKv::new(&m_js, None, ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    let key = ants_proto::channel_manager_serving_key();
    published.put(&key, versions::serving().await).await.expect("the channel manager publishes its version");

    let hook = "github".parse::<UserId>().unwrap();
    let creds = server.dir().join("github.creds");
    let to = [ants_proto::Destination::Ant(dev_a.clone())];
    let permissions = ants_proto::Permissions::for_webhook(&hook, &to);
    auth::issue_with(&server.dir().join("auth"), &hook, crate::credentials::revoke::Kind::Webhook, permissions, &creds)
        .unwrap();
    let _bus = crate::webhook::Bus::connect(&hook, Some(&creds), &server.url, None, "test").await.unwrap();

    let may = |s: &str| rights.may_publish(s);
    for _ in 0..50 {
        let running = versions::running(&js, may, chrono::Utc::now()).await.unwrap();
        if running.components.iter().any(|c| c.what.starts_with("webhook github on")) {
            assert_eq!(running.components.len(), 3, "{running:?}");
            assert!(running.unseen.is_empty(), "{running:?}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let check = versions().await;
    assert_eq!(check.status, Status::Warn, "{check}");
    let old = format!("the daemon of dev-a on m1 (ants 0.0.1, permissions {})", PERMISSIONS_VERSION.0 - 1);
    assert!(check.detail.starts_with(&format!("older than permissions version {PERMISSIONS_VERSION}: {old}")));
    assert!(check.detail.contains("mixed: "), "{check}");
    assert!(!check.detail.contains("the channel manager on") && !check.detail.contains("webhook github on"));
    assert!(check.fix[0].starts_with("upgrade ants where they run"), "{check}");

    entry["version"] = ants_proto::ANTS_VERSION.into();
    entry["perms"] = PERMISSIONS_VERSION.0.into();
    colony.put("dev-a", serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let check = versions().await;
    assert_eq!(check.status, Status::Ok, "{check}");
    assert!(check.detail.starts_with("3 running components"), "{check}");
}

#[tokio::test]
async fn an_ants_credentials_skip_the_hub_check() {
    let Some(server) = Server::start() else { return };
    let ant = "dev-a".parse::<UserId>().unwrap();
    let checks = Box::pin(run(&setup(&server, Some(server.creds_as(&ant, Role::Ant))))).await;
    assert_eq!(named(&checks, "jetstream")[0].status, Status::Ok, "{checks:#?}");
    assert_eq!(named(&checks, "hub")[0].status, Status::Skip, "{checks:#?}");
}

#[test]
fn ants_looking_alike_are_warned_of() {
    let now = chrono::Utc::now();
    let entries: Vec<ants_proto::ColonyEntry> =
        ["dev-a", "dev-b"].map(|ant| crate::test_support::colony::entry(ant, 60, 2, now)).into();
    let check = avatars(&entries, Faces::new(Default::default(), Depth::Plain, ants_proto::Theme::Dark));
    assert_eq!(
        (check.status, check.detail.as_str()),
        (Status::Ok, "2 ants in the colony, each with a face of its own")
    );
    let mut identities = crate::avatars::Identities::new();
    for e in &entries {
        let mut record = ants_proto::AntIdentity::new(e.ant.clone(), format!("{}-id", e.ant), now);
        record.avatar = Some("twin".to_owned());
        identities.insert(e.ant.clone(), record);
    }
    let check = avatars(&entries, Faces::new(identities, Depth::Plain, ants_proto::Theme::Dark));
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.fix[0].starts_with("dev-a and dev-b share the face "), "{check}");
}

#[test]
fn harness_versions_warn_of_drift_across_machines() {
    let now = chrono::Utc::now();
    let entry = |ant: &str, machine: &str, version: Option<&str>| {
        let mut json = serde_json::json!({
            "ant": ant, "machine": machine, "instance": "run-1", "harness": "copilot", "capabilities": [],
            "started_at": now, "heartbeat_at": now, "activity": "idle", "handling": 0, "harnesses": 0,
            "version": ants_proto::ANTS_VERSION, "perms": ants_proto::PERMISSIONS_VERSION,
        });
        if let Some(version) = version {
            json["agent"] = serde_json::json!({"name": "copilot", "version": version});
        }
        serde_json::from_value::<ants_proto::ColonyEntry>(json).unwrap()
    };
    let check = harness_versions(&[entry("dev-a", "m1", None)], now);
    assert_eq!(check.status, Status::Ok, "{check}");
    assert!(check.detail.starts_with("no live ant names its harness agent"), "{check}");
    let mut entries = vec![entry("dev-a", "m1", Some("1.0.3")), entry("dev-b", "m2", Some("1.0.3"))];
    let check = harness_versions(&entries, now);
    assert_eq!((check.status, check.detail.as_str()), (Status::Ok, "live ants run copilot 1.0.3 (2)"));
    entries.push(entry("dev-c", "m2", Some("1.0.2")));
    let check = harness_versions(&entries, now);
    assert_eq!(check.status, Status::Warn, "{check}");
    assert_eq!(
        check.detail,
        "different versions across machines: copilot at 1.0.3 (dev-a on m1, dev-b on m2), 1.0.2 (dev-c on m2)"
    );
}

#[test]
fn drift_of_shared_settings_is_listed_by_field_and_machine() {
    let record = |machine: &str, drift: &[(&str, &[&str])]| ants_proto::MachineRecord {
        machine: machine.parse().unwrap(),
        ants: Vec::new(),
        hostname: machine.to_owned(),
        version: ants_proto::ANTS_VERSION.to_owned(),
        updated_at: chrono::Utc::now(),
        drift: drift
            .iter()
            .map(|(ant, fields)| (ant.parse().unwrap(), fields.iter().map(|f| (*f).to_owned()).collect()))
            .collect(),
    };
    let check = drift(&[record("m1", &[]), record("m2", &[])]);
    assert_eq!((check.status, check.detail.as_str()), (Status::Ok, "no drift on 2 machines"));
    let check = drift(&[record("m1", &[("dev-a", &["model"])]), record("m2", &[("dev-a", &["model", "whoami"])])]);
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.ends_with("which they ignore: dev-a: model on m1; dev-a: model, whoami on m2"), "{check}");
}

#[tokio::test]
async fn a_missing_harness_fails_with_its_fix() {
    let dir = ants_test_support::scratch_dir("doctor-harness").canonicalize().unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "messenger = false\nsage = \"none\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"ghost\"\n\
             [harnesses.ghost]\ncommand = \"/nonexistent-ants-bin/acp\"\n",
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = Setup {
        nats: "nats://127.0.0.1:1".to_owned(),
        creds: None,
        js_domain: None,
        config: config.clone(),
        leaf_config: dir.join("leaf.conf"),
        hub_config: dir.join("no-nats.conf"),
        harnesses: true,
        team: dir.join("no-team"),
        webhooks: dir.join("no-webhooks.toml"),
        home: None,
        worker: None,
    };
    let checks = check_ants(&loaded.ants, &loaded.harnesses, &setup, 0).await;
    let harness = named(&checks, "ant dev-a harness")[0];
    assert_eq!(harness.status, Status::Fail, "{checks:#?}");
    assert!(harness.detail.contains("/nonexistent-ants-bin/acp is not installed"), "{harness}");
    let fix =
        format!("install /nonexistent-ants-bin/acp, or set the `command` of [harnesses.ghost] in {}", config.display());
    assert_eq!(harness.fix, [fix]);
    assert_eq!(named(&checks, "ant dev-a credentials")[0].status, Status::Skip);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Each ant's reactions requests are asked as the ant: answered by nothing, by `ants webhook serve` listing only, or
/// by `ants signer`; an ant without credentials asks no one.
#[tokio::test]
async fn the_reactions_check_names_who_answers_an_ants_requests() {
    use ants_daemon::records::reactions::{Answering, serve};
    let Some(server) = Server::start() else { return };
    let dev_a: UserId = "dev-a".parse().unwrap();
    let creds = server.creds_as(&dev_a, Role::Ant);
    let config = server.dir().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "messenger = false\nharness = \"copilot\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\ncreds = {creds:?}\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n"
        ),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = setup(&server, None);
    // Past the first repeat of a request, which a responder subscribing late still hears.
    let wait = ants_proto::REACTIONS_REPEAT + Duration::from_millis(500);
    let checked = async || check_reactions(&loaded.ants, &setup, wait).await;

    let checks = checked().await;
    let open = named(&checks, "ant dev-b reactions")[0];
    assert_eq!(open.status, Status::Ok, "{open}");
    assert!(open.detail.contains("the daemon answers list_reactions and react itself"), "{open}");
    let none = named(&checks, "ant dev-a reactions")[0];
    assert_eq!(none.status, Status::Warn, "{none}");
    assert!(none.detail.starts_with("nothing answers its reactions requests within 2s"), "{none}");
    assert!(none.fix[0].starts_with("ants signer") && none.fix[1].contains("ants webhook serve"), "{none}");

    let (human, js) = server.connect_as(&"alice".parse().unwrap(), Role::Human).await;
    let webhook = tokio::spawn(serve(human, js, None, Answering::Lists));
    let checks = checked().await;
    let lists = named(&checks, "ant dev-a reactions")[0];
    assert_eq!(lists.status, Status::Warn, "{lists}");
    assert!(
        lists.detail.starts_with("ants webhook serve answers list_reactions, but nothing answers react"),
        "{lists}"
    );
    webhook.abort();

    let approver =
        crate::credentials::enroll::Approver::connect_signer(&server.url, &server.dir().join("auth"), None, wait)
            .await
            .unwrap();
    let signer = tokio::spawn(serve(approver.client.clone(), approver.hub().clone(), None, Answering::ListsAndChanges));
    let checks = checked().await;
    let both = named(&checks, "ant dev-a reactions")[0];
    assert_eq!((both.status, both.detail.as_str()), (Status::Ok, "ants signer answers list_reactions and react"));
    signer.abort();
}

#[test]
fn ants_sharing_a_git_workdir_with_worktrees_turned_off_are_warned() {
    let dir = ants_test_support::scratch_dir("doctor-shared-workdir").canonicalize().unwrap();
    std::fs::create_dir(dir.join(".git")).unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "messenger = false\nharness = \"copilot\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nworktrees = false\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nworktrees = true\n\
             [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nmax_harnesses = 1\nworktrees = false\n\
             [[ant]]\nname = \"dev-d\"\nworkdir = \".\"\n",
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let checks = check_shared_workdirs(&loaded.ants, &config);
    assert_eq!(
        checks.iter().map(|c| (c.check.as_str(), c.status)).collect::<Vec<_>>(),
        [("ant dev-a workdir", Status::Warn)]
    );
    let fix = format!("worktrees = true in the [[ant]] named dev-a of {}", config.display());
    assert!(checks[0].fix[0].starts_with(&fix), "{}", checks[0]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_break_in_an_ant_s_sense_chain_fails_and_a_log_over_its_size_limit_warns() {
    use ants_sense::log::{Break, ChainStatus, SIZE_LIMIT, write_status};

    let dir = ants_test_support::scratch_dir("doctor-sense-chain").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let ants = ["dev-a", "dev-b", "dev-c"]
        .map(|name| format!("[[ant]]\nname = \"{name}\"\nworkdir = \".\"\nhome = \"{name}\"\n"));
    std::fs::write(&config, format!("messenger = false\nsage = \"none\"\nharness = \"copilot\"\n{}", ants.concat()))
        .unwrap();
    let loaded = config::load(&config).unwrap();
    let checked_at = "2026-01-02T03:04:05Z".parse().unwrap();
    let status = ChainStatus {
        sense: ants_proto::SenseId::new(),
        incarnation: "i".into(),
        checked_at,
        events: 2,
        bytes: SIZE_LIMIT,
        seen: None,
        broken: None,
        ant: None,
    };
    write_status(&dir.join("dev-a"), &status).unwrap();
    let broken = Break { index: 3, sequence: Some(7), reason: "not signed by dev-b's credentials".into() };
    write_status(&dir.join("dev-b"), &ChainStatus { broken: Some(broken), bytes: SIZE_LIMIT + 1, ..status.clone() })
        .unwrap();

    let checks = check_sense_logs(&loaded.ants);
    assert_eq!(
        checks.iter().map(|c| (c.check.as_str(), c.status, c.detail.as_str())).collect::<Vec<_>>(),
        [
            ("ant dev-a sense chain", Status::Ok, "2 events verified at 2026-01-02 03:04:05 UTC"),
            ("ant dev-a sense size", Status::Ok, "32.0 MiB (33554432 bytes) of sense data, within the 32 MiB limit"),
            (
                "ant dev-b sense chain",
                Status::Fail,
                &*format!(
                    "sense identity {} breaks at event 3 (stream sequence 7): not signed by dev-b's credentials, as \
                     verified at 2026-01-02 03:04:05 UTC",
                    status.sense
                )
            ),
            (
                "ant dev-b sense size",
                Status::Warn,
                &*format!(
                    "32.0 MiB (33554433 bytes) of sense data in sense identity {}, over the 32 MiB limit",
                    status.sense
                )
            ),
        ],
        "nothing for an ant whose log was never read"
    );
    assert!(checks[3].fix[0].contains("`ants ant memories dev-b`"), "{}", checks[3]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_leaf_reports_an_ant_its_uplink_misses_and_a_node_not_listening() {
    let dir = ants_test_support::scratch_dir("doctor-leaf").canonicalize().unwrap();
    auth::init(&dir).unwrap();
    let machine: Machine = "m1".parse().unwrap();
    let uplink = dir.join("leaf-m1.creds");
    auth::issue_leaf(
        &dir,
        &machine,
        &[("dev-a".parse().unwrap(), Role::Ant)],
        &[],
        EnrollApprover::Without,
        None,
        &uplink,
    )
    .unwrap();
    let leaf_config = dir.join("leaf.conf");
    let uplinks = ["nats-leaf://127.0.0.1:7422".to_owned()];
    let leaf = leaf::Leaf {
        machine: &machine,
        hub_domain: None,
        uplinks: &uplinks,
        creds: &uplink,
        system_creds: None,
        server_conf: &dir.join(auth::SERVER_CONF),
        ca_file: None,
        proxy: None,
        listen: "127.0.0.1:1",
    };
    leaf::write(&leaf, &leaf_config).unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "messenger = false\nharness = \"copilot\"\nnats = \"nats://127.0.0.1:1\"\n[[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n",
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = Setup {
        nats: "nats://127.0.0.1:1".to_owned(),
        creds: None,
        js_domain: None,
        config,
        leaf_config: leaf_config.clone(),
        hub_config: dir.join("no-nats.conf"),
        harnesses: false,
        team: dir.clone(),
        webhooks: dir.join("no-webhooks.toml"),
        home: None,
        worker: None,
    };

    let checks = check_leaf(&setup, Leaf::read(&leaf_config), &loaded.ants, None, None, None, 0).await;
    assert_eq!(named(&checks, "leaf")[0].status, Status::Ok, "{checks:#?}");
    let carried = named(&checks, "leaf uplink")[0];
    assert_eq!(carried.status, Status::Fail, "{carried}");
    assert!(carried.detail.contains("no rights for dev-b"), "{carried}");
    assert!(carried.fix[0].starts_with("ants auth leaf m1") && carried.fix[0].contains("--ant dev-b"), "{carried}");
    assert_eq!(named(&checks, "leaf system uplink")[0].status, Status::Warn, "{checks:#?}");
    let node = named(&checks, "leaf node")[0];
    assert_eq!(node.status, Status::Fail, "{node}");
    assert!(node.detail.starts_with("nothing listens on 127.0.0.1:1"), "{node}");
    assert_eq!(named(&checks, "outbox")[0].status, Status::Skip, "{checks:#?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// With the team keys, the hub is asked whether it keeps pushed accounts: a warning without a `full` resolver, and
/// for a hub of another team, which refuses the system account.
#[tokio::test]
async fn the_hub_resolver_check_warns_of_a_hub_keeping_pushed_accounts_in_memory() {
    for full in [false, true] {
        let Some(hub) = ants_test_support::NatsServer::spawn_with(|dir| {
            auth::init(&dir.join("auth")).unwrap();
            let resolver =
                if full { format!("resolver {{ type: full, dir: {:?} }}\n", dir.join("jwt")) } else { String::new() };
            format!("include \"auth/{}\"\n{resolver}", auth::SERVER_CONF)
        }) else {
            return;
        };
        let setup = Setup {
            nats: hub.url.clone(),
            creds: None,
            js_domain: None,
            config: hub.dir().join("no-config.toml"),
            leaf_config: hub.dir().join("no-leaf.conf"),
            hub_config: hub.dir().join("no-nats.conf"),
            harnesses: false,
            team: hub.dir().join("auth"),
            webhooks: hub.dir().join("no-webhooks.toml"),
            home: None,
            worker: None,
        };
        let check = check_resolver(&setup, false).await;
        if full {
            assert_eq!(check.status, Status::Ok, "{check}");
        } else {
            assert_eq!(check.status, Status::Warn, "{check}");
            assert!(check.fix[0].starts_with("add `resolver { type: full"), "{check}");
        }
        assert_eq!(check_resolver(&setup, true).await.status, Status::Skip, "through the leaf node");

        auth::init(&hub.dir().join("other")).unwrap();
        let other = Setup { team: hub.dir().join("other"), ..setup };
        let check = check_resolver(&other, false).await;
        assert_eq!(check.status, Status::Warn, "{check}");
        assert!(check.detail.contains("refuses the team's system account"), "{check}");
        let none = Setup { team: hub.dir().join("no-team"), ..other };
        assert_eq!(check_resolver(&none, false).await.status, Status::Skip);
    }
}

#[test]
fn the_backup_check_warns_until_a_backup_holds_every_key_issued() {
    let root = ants_test_support::scratch_dir("doctor-backup");
    let team = root.join("auth");
    assert_eq!(check_backup(&team), None, "no team keys here");
    auth::init(&team).unwrap();
    let check = check_backup(&team).unwrap();
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.contains("no backup") && check.fix[0].contains("ants auth backup FILE"), "{check}");
    backup::backup(&team, &root.join("team.age"), "passphrase", 10).unwrap();
    assert_eq!(check_backup(&team).unwrap().status, Status::Ok);
    let key = nkeys::KeyPair::new_user().public_key();
    revoke::record(&team, &revoke::Issued::new(&key, "dev-a", revoke::Kind::Ant, None).unwrap()).unwrap();
    let check = check_backup(&team).unwrap();
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.contains("predates 1 issued.jsonl entry"), "{check}");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn the_host_check_wants_host_credentials_of_the_machine_once_ants_have_credentials() {
    let dir = ants_test_support::scratch_dir("doctor-host");
    let team = dir.join("team");
    auth::init(&team).unwrap();
    let mut ant = Ant::new("dev-a".parse().unwrap(), dir.clone());
    let mut host = config::HostSettings::default();
    let check = |host: &config::HostSettings, ant: &Ant| check_host(host, std::slice::from_ref(ant), "box.local", 0);
    assert_eq!(check(&host, &ant).status, Status::Skip, "no credentials anywhere: the host connects as no one");
    ant.creds = Some(dir.join("dev-a.creds"));
    let off = check(&host, &ant);
    assert!(off.status == Status::Warn && off.to_string().contains("ants auth host box "), "{off}");
    host.top_machine = Some("dev-vm1".to_owned());
    let named = check(&host, &ant);
    assert!(named.to_string().contains("ants auth host dev-vm1 "), "the top-level machine names it: {named}");
    let path = dir.join("host-m1.creds");
    auth::issue_host(&team, &"m1".parse().unwrap(), None, &path).unwrap();
    host.creds = Some(path);
    assert_eq!(check(&host, &ant).status, Status::Ok, "{}", check(&host, &ant));
    host.machine = Some("m2".parse().unwrap());
    let other = check(&host, &ant);
    assert!(other.status == Status::Fail && other.to_string().contains("ants auth host m2 "), "{other}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn sense_keeping_none_of_its_ants_data_fails_naming_the_fixes() {
    let ant = |name: &str, creds: bool, setting| Ant {
        creds: creds.then(|| PathBuf::from(format!("{name}.creds"))),
        provider_setting: setting,
        ..Ant::new(name.parse().unwrap(), PathBuf::from("/w"))
    };
    let (sense, none) = (ants_daemon::provider::Setting::Sense, ants_daemon::provider::Setting::None);
    let mut host = config::HostSettings::default();
    let check =
        |host: &config::HostSettings, ants: &[Ant]| check_sense_kept(host, ants, "box.local", &[], chrono::Utc::now());
    assert!(check(&host, &[ant("dev-a", false, sense.clone())]).is_none(), "no authentication: kept as no one");
    let ants = [ant("dev-a", true, sense.clone()), ant("dev-b", true, none.clone()), ant("dev-c", true, sense)];
    let failed = check(&host, &ants).unwrap();
    assert_eq!((failed.status, failed.check.as_str()), (Status::Fail, "sense data"), "{failed}");
    assert!(failed.detail.contains("none of the data of dev-a, dev-c: no host credentials"), "{failed}");
    assert!(failed.fix[0].starts_with("ants auth host box ") && failed.fix[1].contains("provider = \"none\""));
    assert!(check(&host, &[ant("dev-b", true, none)]).is_none(), "no ant uses sense");
    host.creds = Some(PathBuf::from("host.creds"));
    assert!(check(&host, &ants).is_none(), "sense writes the logs with the host credentials");
}

#[test]
fn sense_keeping_none_of_the_data_of_an_ant_its_colony_entry_names_fails_without_a_config_file() {
    let now = chrono::Utc::now();
    let entry = |ant: &str, machine: &str, unkept: Option<&str>, heartbeat: chrono::DateTime<chrono::Utc>| {
        let json = serde_json::json!({
            "ant": ant, "machine": machine, "instance": "run-1", "harness": "copilot", "capabilities": [],
            "started_at": heartbeat, "heartbeat_at": heartbeat, "activity": "idle", "handling": 0, "harnesses": 0,
            "version": ants_proto::ANTS_VERSION, "perms": PERMISSIONS_VERSION, "sense_unkept": unkept,
        });
        serde_json::from_value::<ants_proto::ColonyEntry>(json).unwrap()
    };
    let colony = [
        entry("dev-a", "box.local", Some("no host credentials on box"), now),
        entry("dev-b", "box.local", None, now),
        entry("dev-c", "other.local", Some("no host credentials on other"), now),
        entry("dev-d", "box.local", Some("no host credentials on box"), now - chrono::Duration::hours(1)),
    ];
    let mut host = config::HostSettings::default();
    let failed = check_sense_kept(&host, &[], "box.local", &colony, now).unwrap();
    assert_eq!((failed.status, failed.check.as_str()), (Status::Fail, "sense data"), "{failed}");
    let told = "the colony entry of dev-a says sense keeps none of its data: no host credentials on box";
    assert_eq!(failed.detail, told, "a live entry of this machine alone");
    assert!(failed.fix[0].starts_with("ants auth host box ") && failed.fix.len() == 1, "{failed}");
    assert!(check_sense_kept(&host, &[], "box.local", &colony[1..], now).is_none());
    host.top_machine = Some("other.local".to_owned());
    let failed = check_sense_kept(&host, &[], "box.local", &colony[1..], now).unwrap();
    assert!(failed.detail.contains("entry of dev-c "), "the config's machine names this one too: {failed}");
    let ant =
        Ant { creds: Some(PathBuf::from("dev-a.creds")), ..Ant::new("dev-a".parse().unwrap(), PathBuf::from("/w")) };
    let failed = check_sense_kept(&host, &[ant], "box.local", &colony[..1], now).unwrap();
    assert!(failed.detail.starts_with("sense keeps none of the data of dev-a:"), "{failed}");
    assert!(!failed.detail.contains("colony entry"), "an ant named once: {failed}");
}

#[test]
fn a_top_level_machine_no_machine_name_is_warned_of_unless_the_host_table_names_one() {
    let mut host = config::HostSettings::default();
    assert!(check_machine_name(&host, "box.local").is_none());
    host.top_machine = Some("dev-vm1".to_owned());
    assert!(check_machine_name(&host, "box.local").is_none());
    host.top_machine = Some("Dev VM.1".to_owned());
    let check = check_machine_name(&host, "box.local").unwrap();
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.contains("name the machine box ") && check.fix[0].contains("\"Dev-VM-1\""), "{check}");
    host.machine = Some("m1".parse().unwrap());
    assert!(check_machine_name(&host, "box.local").is_none(), "the [host] table names the machine");
}

#[test]
fn the_key_check_finds_a_stored_seed_locked_missing_or_of_another_key() {
    let dir = ants_test_support::scratch_dir("doctor-key");
    let team = dir.join("team");
    auth::init(&team).unwrap();
    let user = nkeys::KeyPair::new_user();
    let (key, seed) = (user.public_key(), user.seed().unwrap());
    let jwt = auth::sign_user(&team, &"dev-a".parse().unwrap(), Role::Ant, None, &key).unwrap();
    let path = dir.join("dev-a.creds");
    let store = keystore::Memory::default();
    let check = |store: &keystore::Memory| {
        let check = check_key("ant dev-a key", &path, vec!["ants add dev-a".to_owned()], store).unwrap();
        let shown = check.to_string();
        assert!(!shown.contains(&seed), "{shown}");
        (check.status, shown)
    };

    auth::write_creds(&path, &jwt, &user).unwrap();
    assert!(check_key("ant dev-a key", &path, Vec::new(), &store).is_none(), "the seed is in the file");
    std::fs::write(&path, creds::stored_text(&jwt, &key)).unwrap();
    store.set(&key, &seed).unwrap();
    assert_eq!(check(&store).0, Status::Ok);
    store.lock();
    let (status, shown) = check(&store);
    assert!(status == Status::Fail && shown.contains("locked") && shown.contains("unlock"), "{shown}");
    store.unlock();
    store.set(&key, &auth::seed(&nkeys::KeyPair::new_user()).unwrap()).unwrap();
    let (status, shown) = check(&store);
    assert!(status == Status::Fail && shown.contains("another key") && shown.contains("ants add"), "{shown}");
    store.delete(&key).unwrap();
    let (status, shown) = check(&store);
    assert!(status == Status::Fail && shown.contains("holds no seed") && shown.contains("backup"), "{shown}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn the_xkey_check_warns_of_a_human_device_binding_none_and_fails_on_a_lost_seed() {
    let dir = ants_test_support::scratch_dir("doctor-xkey");
    let team = dir.join("team");
    auth::init(&team).unwrap();
    let alice: UserId = "alice".parse().unwrap();
    let path = dir.join("alice.creds");
    let store = keystore::Memory::default();
    let rights = || ants_proto::Permissions::for_user(&alice, Role::Human);
    let check = |path: &Path| {
        let text = std::fs::read_to_string(path).unwrap();
        let check = check_xkey("credentials xkey", path, &creds::Finding::from_creds(&text).unwrap(), &store).unwrap();
        let shown = check.to_string();
        if let Some(Seed::File(seed)) = creds::xkey_of(&text) {
            assert!(!shown.contains(seed), "{shown}");
        }
        (check.status, shown)
    };

    auth::issue_device(&team, &alice, revoke::Kind::Human, rights(), None, &path).unwrap();
    let (status, shown) = check(&path);
    assert!(status == Status::Ok && shown.contains("in the file"), "{shown}");
    let text = std::fs::read_to_string(&path).unwrap();
    let Some(Seed::File(seed)) = creds::xkey_of(&text) else { panic!("{text}") };
    let other = auth::xseed(&nkeys::XKey::new()).unwrap();
    std::fs::write(&path, text.replace(seed, &other)).unwrap();
    let (status, shown) = check(&path);
    assert!(status == Status::Fail && shown.contains("not that of the key") && shown.contains("backup"), "{shown}");

    let user = nkeys::KeyPair::new_user();
    let (jwt, _) =
        auth::sign_device(&team, &alice, revoke::Kind::Human, rights(), None, &user.public_key(), None).unwrap();
    auth::write_creds(&path, &jwt, &user).unwrap();
    let (status, shown) = check(&path);
    assert!(
        status == Status::Warn && shown.contains("bind no encryption key") && shown.contains("ants renew"),
        "{shown}"
    );
    let ant = auth::sign_user(&team, &"dev-a".parse().unwrap(), Role::Ant, None, &user.public_key()).unwrap();
    let finding = creds::Finding::from_jwt(&ant).unwrap();
    assert!(check_xkey("credentials xkey", &path, &finding, &store).is_none(), "an ant's bind none");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_check_renders_on_one_line_with_its_fixes_indented() {
    let check = Check::fail("hub", "stream ANTS is missing", vec!["ants init".to_owned()]);
    assert_eq!(check.to_string(), "fail  hub: stream ANTS is missing\n      fix: ants init");
    assert_eq!(Check::ok("server", "connected").to_string(), "ok    server: connected");
    assert_eq!(
        serde_json::to_string(&check).unwrap(),
        r#"{"check":"hub","status":"fail","detail":"stream ANTS is missing","fix":["ants init"]}"#
    );
    assert_eq!(
        serde_json::to_string(&Check::skip("leaf", "none")).unwrap(),
        r#"{"check":"leaf","status":"skip","detail":"none"}"#
    );
}

#[test]
fn the_cli_targets_the_leaf_by_its_listen_address() {
    assert!(same_address("nats://127.0.0.1:4222", "127.0.0.1:4222"));
    assert!(same_address("nats://localhost:4222", "127.0.0.1:4222"));
    assert!(same_address("127.0.0.1", "127.0.0.1:4222"));
    assert!(!same_address("nats://127.0.0.1:4223", "127.0.0.1:4222"));
    assert!(!same_address("wss://hub.example/nats", "127.0.0.1:4222"));
}

#[cfg(unix)]
fn private(path: &Path, text: &str) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[cfg(unix)]
fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(unix)]
/// The unit files scripts/install-units.sh writes for `args`, installed under `home` for this platform's
/// manager; false where it has none.
fn install_units(home: &Path, args: &[&str]) -> bool {
    let leaf = home.join("leaf.conf");
    let mut all = vec!["--nats-server", "/bin/sh", "--no-daemon", "--nats-conf", leaf.to_str().unwrap()];
    all.extend(args);
    install_units_as(home, &all)
}

#[cfg(unix)]
/// As [`install_units`], with `args` alone.
fn install_units_as(home: &Path, args: &[&str]) -> bool {
    let dest = match std::env::consts::OS {
        "macos" => home.join("Library/LaunchAgents"),
        "linux" => home.join(".config/systemd/user"),
        _ => return false,
    };
    let out = std::process::Command::new("sh")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/install-units.sh"))
        .arg("--out")
        .arg(&dest)
        .args(args)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    true
}

#[cfg(unix)]
/// The services under `home`, every one running.
fn running(home: &Path) -> Services {
    Services::answering(home, |command| {
        let stdout = match command {
            "id -u" => "501\n",
            c if c.starts_with("launchctl print") => "\tstate = running\n\tpid = 7\n",
            _ => "",
        };
        crate::service::Ran { ok: true, stdout: stdout.to_owned(), stderr: String::new() }
    })
}

#[cfg(unix)]
async fn transports(home: &Path) -> Vec<Check> {
    let installed = installed_transports(&mut running(home));
    check_transports(installed, auth::now().unwrap()).await
}

#[cfg(unix)]
fn one<'a>(checks: &'a [Check], name: &str) -> &'a Check {
    let found = named(checks, name);
    assert_eq!(found.len(), 1, "one {name} check: {checks:#?}");
    found[0]
}

#[cfg(unix)]
#[tokio::test]
async fn a_dev_tunnel_needs_a_login_or_a_connect_token_far_from_expiring() {
    use std::os::unix::fs::PermissionsExt as _;
    let home = ants_test_support::scratch_dir("doctor-devtunnel").canonicalize().unwrap();
    assert!(transports(&home).await.is_empty(), "nothing without the services");
    let devtunnel = home.join("devtunnel");
    let user_show = |json: &str| {
        std::fs::write(&devtunnel, format!("#!/bin/sh\nprintf '%s\\n' '{json}'\n")).unwrap();
        std::fs::set_permissions(&devtunnel, std::fs::Permissions::from_mode(0o755)).unwrap();
    };
    user_show(r#"{"status": "Not logged in"}"#);
    let token = home.join("connect-token");
    let now = auth::now().unwrap();
    let jwt = |exp: u64| {
        let payload = data_encoding::BASE64URL_NOPAD.encode(format!("{{\"exp\":{exp}}}").as_bytes());
        format!("eyJhbGciOiJSUzI1NiJ9.{payload}.c2ln\n")
    };
    private(&token, &jwt(now + 3 * 3600 + 1800));
    let (bin, file) = (devtunnel.to_str().unwrap(), token.to_str().unwrap());
    let args = ["--devtunnel", "hub", "--devtunnel-bin", bin, "--devtunnel-token", file, "--devtunnel-host", "hub"];
    if !install_units(&home, &args) {
        return;
    }
    let restart = running(&home).by_hand(Service::DevTunnel, Action::Restart).unwrap();

    let checks = transports(&home).await;
    assert_eq!(one(&checks, "devtunnel service").status, Status::Ok, "{checks:#?}");
    assert_eq!(one(&checks, "devtunnel host service").status, Status::Ok, "{checks:#?}");
    let expiring = one(&checks, "devtunnel token");
    assert_eq!((expiring.status, expiring.detail.as_str()), (Status::Warn, &*format!("{file}: expiring in 3h")));
    let renew = format!("devtunnel token hub --scopes connect, its token into {file} (mode 0600), then {restart}");
    assert_eq!(expiring.fix, std::slice::from_ref(&renew));
    let login = one(&checks, "devtunnel login");
    assert_eq!(
        (login.status, login.detail.as_str()),
        (Status::Fail, "Not logged in: the tunnel goes down at its next connection")
    );
    assert!(login.fix[0].starts_with("devtunnel user login"), "{login}");

    let in_days = (chrono::Utc::now() + chrono::Duration::hours(80)).to_rfc3339();
    user_show(&format!(r#"{{"status": "Logged in", "username": "alice", "expiration": "{in_days}"}}"#));
    private(&token, &jwt(now + 10 * 3600));
    let checks = transports(&home).await;
    assert_eq!(one(&checks, "devtunnel token").status, Status::Ok, "{checks:#?}");
    let login = one(&checks, "devtunnel login");
    assert_eq!((login.status, login.detail.as_str()), (Status::Ok, "logged in as alice for 3 days"));
    let in_hours = (chrono::Utc::now() + chrono::Duration::hours(5)).to_rfc3339();
    user_show(&format!(r#"{{"status": "Logged in", "expiration": "{in_hours}"}}"#));
    assert_eq!(one(&transports(&home).await, "devtunnel login").status, Status::Warn);

    private(&token, &jwt(now - 1));
    assert_eq!(one(&transports(&home).await, "devtunnel token").status, Status::Fail);
    private(&token, "an-opaque-token\n");
    let written = one(&transports(&home).await, "devtunnel token").clone();
    assert_eq!(
        (written.status, written.detail),
        (Status::Ok, format!("{file}: written 0m ago, and a connect token lasts a day"))
    );
    chmod(&token, 0o644);
    let exposed = one(&transports(&home).await, "devtunnel token").clone();
    assert_eq!((exposed.status, exposed.fix), (Status::Warn, vec![format!("chmod 600 {file}")]));
    std::fs::remove_file(&token).unwrap();
    let missing = one(&transports(&home).await, "devtunnel token").clone();
    assert_eq!((missing.status, missing.fix), (Status::Fail, vec![renew]));
    std::fs::remove_dir_all(&home).unwrap();
}

#[cfg(unix)]
/// The `ants bridge serve` unit, with `serve`, else the `ants bridge connect` one, installed under `home`, run with
/// `flags` after its subcommand, as an edit of the unit by hand does: the first of a flag given twice is the one read.
fn bridge_flags(home: &Path, serve: bool, flags: &[&str]) {
    let sub = if serve { "serve" } else { "connect" };
    let (unit, from, to) = match std::env::consts::OS {
        "macos" => {
            let flags: String = flags.iter().map(|flag| format!("<string>{flag}</string>")).collect();
            let label = if serve { "ants.bridge-serve" } else { "ants.bridge" };
            let unit = home.join(format!("Library/LaunchAgents/{label}.plist"));
            (unit, format!("<string>{sub}</string>"), format!("<string>{sub}</string>{flags}"))
        }
        _ => {
            let name = if serve { "ants-bridge-serve" } else { "ants-bridge" };
            let unit = home.join(format!(".config/systemd/user/{name}.service"));
            (unit, format!("\" bridge {sub} "), format!("\" bridge {sub} {} ", flags.join(" ")))
        }
    };
    let text = std::fs::read_to_string(&unit).unwrap();
    assert!(text.contains(&from), "{text}");
    std::fs::write(&unit, text.replacen(&from, &to, 1)).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_bridge_must_answer_and_take_the_token_of_its_file() {
    let home = ants_test_support::scratch_dir("doctor-bridge").canonicalize().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(bridge::Server::new("127.0.0.1:1".to_owned(), Some("s3cret".to_owned())).run(listener));
    let token = home.join("bridge-token");
    private(&token, "s3cret\n");
    let url = format!("http://127.0.0.1:{port}/bridge");
    let file = token.to_str().unwrap();
    let args = ["--ants", "/bin/sh", "--bridge", &url, "--bridge-token", file, "--bridge-serve"];
    if !install_units(&home, &args) {
        return;
    }
    // The bridge this test serves, not the default port.
    bridge_flags(&home, true, &["--listen", &format!("127.0.0.1:{port}")]);
    let restart = running(&home).by_hand(Service::BridgeServe, Action::Restart).unwrap();

    let checks = transports(&home).await;
    for check in ["bridge service", "bridge token", "bridge serve service", "bridge serve token"] {
        assert_eq!(one(&checks, check).status, Status::Ok, "{checks:#?}");
    }
    assert_eq!(one(&checks, "bridge url").detail, format!("{url} answers as a bridge taking the token"));
    let serving = one(&checks, "bridge serve url");
    assert_eq!(serving.detail, format!("http://127.0.0.1:{port} answers as a bridge taking the token"));
    // The bridge answers, but its upstream is no hub's leafnode port.
    let pipe = one(&checks, "bridge pipe");
    let unopened =
        format!("{url} answers but opens no connection to the hub's leafnode port: 502 Bad Gateway upstream");
    assert!(pipe.status == Status::Fail && pipe.detail.starts_with(&unopened), "{pipe}");
    let hub = "on the hub, start nats-server, whose leafnodes listen where `ants bridge serve --upstream` points \
               (127.0.0.1:7422 by default); `ants doctor` there checks the bridge serve pipe";
    assert_eq!(pipe.fix, [hub]);
    let pipe = one(&checks, "bridge serve pipe");
    assert_eq!(pipe.status, Status::Fail, "{pipe}");
    let start = format!(
        "start the hub's nats-server, whose leafnodes listen on 127.0.0.1:7422, or give `ants bridge serve` the \
         --upstream of its leafnode port, then {restart}"
    );
    assert_eq!(pipe.fix, [start]);

    private(&token, "guess\n");
    let checks = transports(&home).await;
    let refused = one(&checks, "bridge url");
    assert_eq!(
        (refused.status, refused.detail.as_str()),
        (Status::Fail, &*format!("{url} refuses the token: missing or wrong bridge token"))
    );
    assert_eq!(one(&checks, "bridge pipe").status, Status::Skip, "{checks:#?}");
    chmod(&token, 0o644);
    let exposed = one(&transports(&home).await, "bridge token").clone();
    assert_eq!((exposed.status, exposed.fix), (Status::Warn, vec![format!("chmod 600 {file}")]));
    std::fs::remove_file(&token).unwrap();
    let checks = transports(&home).await;
    assert_eq!(one(&checks, "bridge token").status, Status::Fail, "{checks:#?}");
    assert_eq!(one(&checks, "bridge url").status, Status::Fail, "{checks:#?}");
    std::fs::remove_dir_all(&home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_bridge_must_carry_the_leaf_node_s_uplink_to_the_hub_s_leafnode_port() {
    let leaf_port = ants_test_support::free_port();
    let Some(hub) = ants_test_support::NatsServer::spawn_with(|_| {
        format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n")
    }) else {
        return;
    };
    let home = ants_test_support::scratch_dir("doctor-bridge-pipe").canonicalize().unwrap();
    let local = async || {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    };
    let (serve, serve_port) = local().await;
    let upstream = format!("127.0.0.1:{leaf_port}");
    tokio::spawn(bridge::Server::new(upstream.clone(), Some("s3cret".to_owned())).run(serve));
    let url = format!("http://127.0.0.1:{serve_port}/bridge");
    // `ants bridge connect` as the unit runs it, and one started before the token it holds was replaced.
    let (connect, connect_port) = local().await;
    tokio::spawn(bridge::Remote::new(&url, Some("s3cret"), &[], None).unwrap().run(connect));
    let (stale, stale_port) = local().await;
    tokio::spawn(bridge::Remote::new(&url, Some("old"), &[], None).unwrap().run(stale));
    let token = home.join("bridge-token");
    private(&token, "s3cret\n");
    let args = ["--ants", "/bin/sh", "--bridge", &url, "--bridge-token", token.to_str().unwrap(), "--bridge-serve"];
    if !install_units(&home, &args) {
        return;
    }
    bridge_flags(&home, true, &["--listen", &format!("127.0.0.1:{serve_port}"), "--upstream", &upstream]);
    bridge_flags(&home, false, &["--listen", &format!("127.0.0.1:{connect_port}")]);
    let restart = running(&home).by_hand(Service::Bridge, Action::Restart).unwrap();

    let checks = transports(&home).await;
    let pipe = one(&checks, "bridge pipe");
    let carried = format!("127.0.0.1:{connect_port} carries the leaf node's uplink through {url} to the leafnode port");
    assert!(pipe.status == Status::Ok && pipe.detail.starts_with(&carried), "{pipe}");
    let pipe = one(&checks, "bridge serve pipe");
    let carried = format!("http://127.0.0.1:{serve_port} carries a session to the leafnode port of nats-server");
    assert!(pipe.status == Status::Ok && pipe.detail.starts_with(&carried), "{pipe}");
    assert!(pipe.detail.ends_with(&format!(" at {upstream}")), "{pipe}");

    bridge_flags(&home, false, &["--listen", &format!("127.0.0.1:{stale_port}")]);
    let pipe = one(&transports(&home).await, "bridge pipe").clone();
    let detail = format!(
        "127.0.0.1:{stale_port} carries nothing, though {url} carries the hub's INFO: the running `ants bridge \
         connect` took another token, URL or proxy at its start"
    );
    assert_eq!((pipe.status, pipe.detail, pipe.fix), (Status::Fail, detail, vec![restart.clone()]));
    let closed = ants_test_support::free_port();
    bridge_flags(&home, false, &["--listen", &format!("127.0.0.1:{closed}")]);
    let pipe = one(&transports(&home).await, "bridge pipe").clone();
    let nothing = format!("nothing carries the leaf node's uplink at 127.0.0.1:{closed}: ");
    assert!(pipe.status == Status::Fail && pipe.detail.starts_with(&nothing), "{pipe}");
    assert_eq!(pipe.fix, [restart]);

    // `ants bridge serve` whose upstream is the hub's client port.
    let (wrong, wrong_port) = local().await;
    let client = hub.url.trim_start_matches("nats://").to_owned();
    tokio::spawn(bridge::Server::new(client.clone(), Some("s3cret".to_owned())).run(wrong));
    bridge_flags(&home, true, &["--listen", &format!("127.0.0.1:{wrong_port}"), "--upstream", &client]);
    let pipe = one(&transports(&home).await, "bridge serve pipe").clone();
    let detail = format!("http://127.0.0.1:{wrong_port} answers but carries the INFO of nats-server ");
    assert!(pipe.status == Status::Fail && pipe.detail.starts_with(&detail), "{pipe}");
    assert!(
        pipe.detail.ends_with("from another port than its leafnode port, not the INFO of the hub's leafnode port"),
        "{pipe}"
    );
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn only_a_leafnode_port_s_info_is_taken_as_the_hub_s() {
    let leaf = r#"INFO {"server_id":"N1","server_name":"hub","version":"2.15.3","proto":3,"auth_required":true,"client_id":6,"nonce":"x","info_on_connect":true} "#;
    assert_eq!(leafnode_info(leaf), Ok("nats-server 2.15.3 (hub)".to_owned()));
    let routed = r#"INFO {"server_name":"hub","version":"2.16.0","client_id":3,"leafnode_urls":["10.0.0.1:7422"]}"#;
    assert_eq!(leafnode_info(routed), Ok("nats-server 2.16.0 (hub)".to_owned()));
    let client = r#"INFO {"server_id":"N1","server_name":"hub","version":"2.15.3","proto":1,"client_id":5,"client_ip":"127.0.0.1"} "#;
    let other = "the INFO of nats-server 2.15.3 (hub) from another port than its leafnode port".to_owned();
    assert_eq!(leafnode_info(client), Err(other));
    assert_eq!(leafnode_info(""), Err("nothing".to_owned()));
    assert_eq!(leafnode_info("HTTP/1.1 400 Bad Request"), Err("\"HTTP/1.1 400 Bad Request\"".to_owned()));
}

#[cfg(unix)]
#[tokio::test]
async fn the_webhook_config_needs_private_secrets_and_a_public_url_reaching_it() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let dir = ants_test_support::scratch_dir("doctor-webhooks").canonicalize().unwrap();
    let path = dir.join("webhooks.toml");
    assert!(check_webhooks(&path).await.is_empty(), "nothing without the file");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut head = [0; 4096];
            let n = stream.read(&mut head).await.unwrap_or_default();
            let page = "<h1>Bad link</h1><p>The link is incomplete.</p>";
            let answer = if head[..n].starts_with(b"GET /hooks/_approval HTTP/1.1") {
                format!("HTTP/1.1 400 Bad Request\r\ncontent-length: {}\r\n\r\n{page}", page.len())
            } else {
                "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n".to_owned()
            };
            let _ = stream.write_all(answer.as_bytes()).await;
        }
    });
    private(&dir.join("action"), "0123456789abcdef-secret\n");
    std::fs::write(dir.join("ntfy"), "tk_secret\n").unwrap();
    chmod(&dir.join("ntfy"), 0o644);
    let config = |public_url: &str| {
        let text = format!(
            "public_url = \"{public_url}\"\naction_secret_file = \"action\"\n[[human]]\nname = \"alice\"\n\
                 [[human.notify]]\nkind = \"ntfy\"\ntopic = \"a\"\nsecret_file = \"ntfy\"\n"
        );
        std::fs::write(&path, text).unwrap();
    };
    config(&format!("http://{addr}/hooks"));

    let checks = check_webhooks(&path).await;
    let exposed = one(&checks, "webhook secrets");
    let ntfy = dir.join("ntfy").display().to_string();
    assert_eq!(exposed.status, Status::Fail);
    assert_eq!(exposed.detail, format!("human alice notify #1: {ntfy} has mode 0644, readable by others"));
    assert_eq!(exposed.fix, [format!("chmod 600 {ntfy}")]);
    assert_eq!(one(&checks, "webhooks").status, Status::Skip);

    chmod(&dir.join("ntfy"), 0o600);
    let checks = check_webhooks(&path).await;
    assert!(checks.iter().all(|c| c.status == Status::Ok), "{checks:#?}");
    assert!(one(&checks, "webhooks").detail.contains("notifies alice"), "{checks:#?}");
    assert_eq!(
        one(&checks, "public_url").detail,
        format!("http://{addr}/hooks/_approval answers as `ants webhook serve` does")
    );

    config(&format!("http://{addr}/elsewhere"));
    assert_eq!(one(&check_webhooks(&path).await, "public_url").status, Status::Warn);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn the_programs_of_exec_sinks_must_be_there_to_run() {
    let dir = ants_test_support::scratch_dir("doctor-exec").canonicalize().unwrap();
    let path = dir.join("webhooks.toml");
    private(&dir.join("hook"), "0123456789abcdef-hook\n");
    std::fs::write(
        &path,
        "[[human]]\nname = \"alice\"\n\
             [[human.notify]]\nkind = \"exec\"\ncommand = [\"bin/notify\", \"--quiet\"]\n\
             [[human.notify]]\nkind = \"exec\"\ncommand = [\"no-such-program-of-ants\"]\n\
             [[human.notify]]\nkind = \"http\"\nurl = \"https://hooks.example/a\"\nsecret_file = \"hook\"\n",
    )
    .unwrap();
    let program = dir.join("bin/notify");
    let commands = |checks: &[Check]| -> Vec<(Status, String)> {
        checks.iter().filter(|c| c.check == "notify commands").map(|c| (c.status, c.detail.clone())).collect()
    };
    let checks = check_webhooks(&path).await;
    let missing = format!("human alice notify #1: {} is no executable file", program.display());
    let off_path = "human alice notify #2: no-such-program-of-ants is not on PATH here".to_owned();
    assert_eq!(commands(&checks), [(Status::Fail, missing), (Status::Warn, off_path.clone())]);
    assert_eq!(one(&checks, "webhook secrets").status, Status::Ok, "the http sink's secret file: {checks:#?}");

    std::fs::create_dir_all(dir.join("bin")).unwrap();
    std::fs::write(&program, "#!/bin/sh\ncat > /dev/null\n").unwrap();
    chmod(&program, 0o755);
    let ready = format!("human alice notify #1: {} is executable", program.display());
    assert_eq!(commands(&check_webhooks(&path).await), [(Status::Ok, ready), (Status::Warn, off_path)]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[cfg(unix)]
#[test]
fn the_hub_service_of_ants_setup_must_be_installed_and_running() {
    let home = ants_test_support::scratch_dir("doctor-hub-service").canonicalize().unwrap();
    let hub_config = home.join("nats.conf");
    let setup = Setup {
        nats: "nats://127.0.0.1:4222".to_owned(),
        creds: None,
        js_domain: None,
        config: home.join("no-config.toml"),
        leaf_config: home.join("leaf.conf"),
        hub_config: hub_config.clone(),
        harnesses: false,
        team: home.join("no-team"),
        webhooks: home.join("no-webhooks.toml"),
        home: Some(home.clone()),
        worker: None,
    };
    let stopped = || {
        Services::answering(&home, |command| {
            let (ok, stdout) = match command {
                "id -u" => (true, "501\n"),
                c if c.starts_with("launchctl print") => (true, "\tstate = not running\n"),
                _ => (false, ""),
            };
            crate::service::Ran { ok, stdout: stdout.to_owned(), stderr: String::new() }
        })
    };
    assert!(
        named(&check_services(&setup, &mut stopped(), None), "hub service").is_empty(),
        "nothing without nats.conf"
    );

    std::fs::write(&hub_config, "listen: 127.0.0.1:4222\n").unwrap();
    let checks = check_services(&setup, &mut stopped(), None);
    let missing = one(&checks, "hub service");
    assert_eq!(missing.status, Status::Warn, "{missing}");
    assert_eq!(missing.fix, [format!("scripts/install-units.sh --nats-conf {}", hub_config.display())]);
    assert_eq!(one(&checks, "leaf node service").status, Status::Skip, "{checks:#?}");

    if !install_units(&home, &["--nats-conf", hub_config.to_str().unwrap()]) {
        return;
    }
    let checks = check_services(&setup, &mut stopped(), None);
    let down = one(&checks, "hub service");
    assert_eq!(down.status, Status::Fail, "{down}");
    assert_eq!(down.detail, format!("installed for {}, not running", hub_config.display()));
    let start = if cfg!(target_os = "macos") {
        "launchctl kickstart gui/501/ants.nats"
    } else {
        "systemctl --user start ants-nats"
    };
    assert_eq!(down.fix, [start]);
    assert_eq!(one(&check_services(&setup, &mut running(&home), None), "hub service").status, Status::Ok);

    std::fs::write(home.join("leaf.conf"), "").unwrap();
    let checks = check_services(&setup, &mut running(&home), None);
    assert!(named(&checks, "hub service").is_empty(), "the leaf node is the machine's nats-server: {checks:#?}");
    assert_eq!(one(&checks, "leaf node service").status, Status::Warn, "{checks:#?}");
    std::fs::remove_dir_all(&home).unwrap();
}

#[cfg(unix)]
#[test]
fn a_nats_server_run_by_the_daemon_has_the_daemon_service_as_its_one_owner() {
    let home = ants_test_support::scratch_dir("doctor-managed-nats").canonicalize().unwrap();
    let (leaf, config) = (home.join("leaf.conf"), home.join("config.toml"));
    std::fs::write(&leaf, "listen: 127.0.0.1:4222\n").unwrap();
    std::fs::write(&config, format!("[nats_server]\nconfig = {leaf:?}\n")).unwrap();
    let setup = Setup {
        nats: "nats://127.0.0.1:4222".to_owned(),
        creds: None,
        js_domain: None,
        config: config.clone(),
        leaf_config: leaf.clone(),
        hub_config: home.join("nats.conf"),
        harnesses: false,
        team: home.join("no-team"),
        webhooks: home.join("no-webhooks.toml"),
        home: Some(home.clone()),
        worker: None,
    };
    let managed = Some(leaf.as_path());
    let run_by = format!("{} is run by `ants daemon` ([nats_server] of {})", leaf.display(), config.display());
    let checks = check_services(&setup, &mut running(&home), managed);
    let missing = one(&checks, "leaf node service");
    assert_eq!((missing.status, &missing.detail), (Status::Warn, &format!("{run_by}; no daemon service installed")));
    assert_eq!(missing.fix, [format!("scripts/install-units.sh --managed-nats --config {}", config.display())]);

    if !install_units_as(&home, &["--managed-nats", "--ants", "/bin/sh", "--config", config.to_str().unwrap()]) {
        return;
    }
    let checks = check_services(&setup, &mut running(&home), managed);
    let owned = one(&checks, "leaf node service");
    assert_eq!((owned.status, &owned.detail), (Status::Ok, &format!("{run_by}, whose service runs")));
    assert_eq!(one(&checks, "daemon service").status, Status::Ok, "{checks:#?}");

    assert!(install_units(&home, &[]));
    let checks = check_services(&setup, &mut running(&home), managed);
    let two = one(&checks, "leaf node service");
    assert_eq!(two.status, Status::Fail, "{two}");
    let table = format!("[nats_server] of {}", config.display());
    let owners =
        format!("two owners of {}: an ants-nats service runs it, and `ants daemon` too, from {table}", leaf.display());
    assert_eq!(two.detail, owners);
    let remove = if cfg!(target_os = "macos") {
        format!(
            "launchctl bootout gui/$(id -u)/ants.nats && rm {}",
            home.join("Library/LaunchAgents/ants.nats.plist").display()
        )
    } else {
        format!(
            "systemctl --user disable --now ants-nats && rm {}",
            home.join(".config/systemd/user/ants-nats.service").display()
        )
    };
    assert_eq!(two.fix, [remove, format!("or remove {table}")]);
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn ants_whose_harness_runs_tools_unasked_are_warned_of_rules_that_miss_them() {
    let dir = ants_test_support::scratch_dir("doctor-bypassed");
    let config = dir.join("ants.toml");
    let creds = dir.join("nightly.creds");
    std::fs::write(
        &config,
        format!(
            "messenger = false\nharness = \"claude\"\n\
             [harnesses.gated]\ncommand = \"claude-agent-acp\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nallow = [\"read:*\"]\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n\
             [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nharness = \"gated\"\nallow = [\"read:*\"]\n\
             [[schedule]]\nname = \"nightly\"\ncreds = {creds:?}\nevery = \"4h\"\nant = \"dev-b\"\ntext = \"t\"\nallow = [\"read:*\"]\n"
        ),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let checks = check_bypassed(&loaded, &[]);
    assert_eq!(
        checks.iter().map(|c| (c.check.as_str(), c.status)).collect::<Vec<_>>(),
        [("ant dev-a tool permissions", Status::Warn), ("ant dev-b tool permissions", Status::Warn)]
    );
    assert!(checks[1].detail.contains("schedule nightly"), "{}", checks[1]);

    std::fs::write(&config, "messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n").unwrap();
    let mut checks = Vec::new();
    assert!(check_config(&config, &mut checks).is_none());
    assert!(checks[0].fix[0].starts_with("add `harness = \"NAME\"` at the top of"), "{}", checks[0]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_config_leaving_every_ant_to_another_machine_fails_naming_the_builtin_ones() {
    let auth = std::env::home_dir().unwrap().join(".config/ants/auth");
    if ["system-messenger", "system-sage"].iter().any(|name| auth.join(format!("{name}.creds")).is_file()) {
        return;
    }
    let dir = ants_test_support::scratch_dir("doctor-elsewhere");
    let config = dir.join("ants.toml");
    let check = |text: &str| {
        std::fs::write(&config, format!("harness = \"copilot\"\n[host]\ncreds = \"h.creds\"\n{text}")).unwrap();
        let mut checks = Vec::new();
        assert!(check_config(&config, &mut checks).is_some());
        checks.remove(0)
    };
    let nothing = check("");
    assert_eq!(nothing.status, Status::Fail, "{nothing}");
    let messenger = format!(
        "the built-in system-messenger, without credentials at {} here",
        auth.join("system-messenger.creds").display()
    );
    assert!(nothing.detail.contains(&messenger) && nothing.detail.ends_with("nothing to serve here"), "{nothing}");
    let some = check("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n");
    assert_eq!(some.status, Status::Ok, "{some}");
    assert!(some.detail.contains(" lists dev-a; the built-in system-messenger, without credentials at"), "{some}");
    assert!(some.detail.ends_with("system-sage.creds here, is left to another machine"), "{some}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_sage_on_a_harness_that_cannot_hold_reviews_is_warned_of_declining_them() {
    let dir = ants_test_support::scratch_dir("doctor-unheld-reviews");
    let config = dir.join("ants.toml");
    let reviews = |harness: &str| {
        std::fs::write(
            &config,
            format!("messenger = false\nharness = \"{harness}\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n"),
        )
        .unwrap();
        let checks = check_bypassed(&config::load(&config).unwrap(), &[]);
        checks.into_iter().filter(|c| c.check == "ant system-sage reviews").collect::<Vec<_>>()
    };
    let warned = reviews("antigravity");
    assert_eq!(warned.len(), 1);
    assert!(warned[0].detail.contains("harness antigravity runs its tools unasked"), "{}", warned[0]);
    assert!(warned[0].fix[0].starts_with("set review in [harnesses.antigravity]"), "{}", warned[0]);
    assert!(reviews("claude").is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}
