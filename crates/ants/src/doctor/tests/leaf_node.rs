//! Tests of the doctor's leaf node checks: its uplinks and the ants they miss, a node not listening, and the
//! address the CLI reaches it at.

use super::*;

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
        "messenger = false\nharness = \"copilot\"\nhuman = \"human\"\nnats = \"nats://127.0.0.1:1\"\n[[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n",
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = Setup {
        nats: "nats://127.0.0.1:1".to_owned(),
        creds: None,
        js_domain: None,
        human: None,
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
    let store = named(&checks, "leaf store uplink")[0];
    assert_eq!(store.status, Status::Ok, "{store}");
    assert!(store.detail.starts_with("the store account uplink of the leaf node of m1"), "{store}");
    assert!(store.detail.ends_with("never expiring"), "{store}");
    assert_eq!(named(&checks, "leaf system uplink")[0].status, Status::Warn, "{checks:#?}");
    let node = named(&checks, "leaf node")[0];
    assert_eq!(node.status, Status::Fail, "{node}");
    assert!(node.detail.starts_with("nothing listens on 127.0.0.1:1"), "{node}");
    assert_eq!(named(&checks, "outbox")[0].status, Status::Skip, "{checks:#?}");
    assert!(named(&checks, "leaf uplink ants").is_empty(), "{checks:#?}");

    let none = check_leaf(&setup, Leaf::read(&leaf_config), &[], None, None, None, 0).await;
    assert_eq!(named(&none, "leaf uplink")[0].status, Status::Ok, "{none:#?}");
    let skipped = named(&none, "leaf uplink ants")[0];
    assert_eq!(skipped.status, Status::Skip, "{skipped}");
    assert!(skipped.detail.starts_with("no ants served here"), "{skipped}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// `ants add --enroll` and `ants renew` respell the uplink's credentials in leaf.conf to reconnect it; the store and
/// system account uplinks, still named as written, pass.
#[tokio::test]
async fn a_leaf_s_store_and_system_uplinks_pass_after_the_uplink_is_respelled() {
    let dir = ants_test_support::scratch_dir("doctor-leaf-respelled").canonicalize().unwrap();
    auth::init(&dir).unwrap();
    let machine: Machine = "m1".parse().unwrap();
    let uplink = dir.join("leaf-m1.creds");
    let ants = [("dev-a".parse().unwrap(), Role::Ant)];
    auth::issue_leaf(&dir, &machine, &ants, &[], EnrollApprover::Without, None, &uplink).unwrap();
    let leaf_config = dir.join("leaf.conf");
    let uplinks = ["nats-leaf://127.0.0.1:7422".to_owned()];
    let system_creds = auth::leaf_system_creds(&uplink);
    let leaf = leaf::Leaf {
        machine: &machine,
        hub_domain: None,
        uplinks: &uplinks,
        creds: &uplink,
        system_creds: Some(&system_creds),
        server_conf: &dir.join(auth::SERVER_CONF),
        ca_file: None,
        proxy: None,
        listen: "127.0.0.1:1",
    };
    leaf::write(&leaf, &leaf_config).unwrap();
    leaf::use_renewed_uplink(&leaf_config, |_| crate::service::Outcome::NotRunning);
    assert!(std::fs::read_to_string(&leaf_config).unwrap().contains("/./leaf-m1.creds"), "respelled");
    let setup = Setup {
        nats: "nats://127.0.0.1:1".to_owned(),
        creds: None,
        js_domain: None,
        human: None,
        config: dir.join("no-config.toml"),
        leaf_config: leaf_config.clone(),
        hub_config: dir.join("no-nats.conf"),
        harnesses: false,
        team: dir.clone(),
        webhooks: dir.join("no-webhooks.toml"),
        home: None,
        worker: None,
    };

    let checks = check_leaf(&setup, Leaf::read(&leaf_config), &[], None, None, None, 0).await;
    for name in ["leaf uplink", "leaf store uplink", "leaf system uplink"] {
        let check = named(&checks, name)[0];
        assert_eq!(check.status, Status::Ok, "{check}");
        assert!(!check.detail.contains("/./"), "{check}");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn the_cli_targets_the_leaf_by_its_listen_address() {
    assert!(same_address("nats://127.0.0.1:4222", "127.0.0.1:4222"));
    assert!(same_address("nats://localhost:4222", "127.0.0.1:4222"));
    assert!(same_address("127.0.0.1", "127.0.0.1:4222"));
    assert!(!same_address("nats://127.0.0.1:4223", "127.0.0.1:4222"));
    assert!(!same_address("wss://hub.example/nats", "127.0.0.1:4222"));
}
