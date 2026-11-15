use ants_proto::Role;

use super::*;
use crate::auth::tests::Server;
use crate::members::EnrollApprover;

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
    assert!(hub[0].detail.contains("presence not checked"), "{}", hub[0]);
}

#[test]
fn consumers_warn_near_the_limit_the_hub_enforces() {
    let mut config = jetstream::stream::Config::default();
    assert_eq!(consumer_limit(&config), None, "no limit before nats-server 2.15");
    config.metadata.insert("_nats.ver".to_owned(), "2.14.9".to_owned());
    assert_eq!(consumer_limit(&config), None);
    config.metadata.insert("_nats.ver".to_owned(), "2.15.0-beta.1".to_owned());
    config.max_consumers = -1;
    assert_eq!(consumer_limit(&config), Some(1000), "-1 leaves the default");
    config.max_consumers = hub::MAX_CONSUMERS;
    assert_eq!(consumer_limit(&config), Some(10_000));

    let stream = hub::Object::Stream;
    assert_eq!(consumers(stream, 799, Some(1000)).status, Status::Ok);
    let near = consumers(stream, 800, Some(1000));
    assert_eq!((near.status, near.fix.as_slice()), (Status::Warn, ["ants init".to_owned()].as_slice()));
    let full = consumers(stream, 9_000, Some(10_000));
    assert_eq!(full.fix, [format!("nats stream edit {STREAM} --max-consumers 20000")]);
    assert_eq!(consumers(stream, 5_000, None).status, Status::Ok);
    let humans = consumers(hub::Object::Humans, 900, Some(1000));
    assert_eq!(
        humans.detail,
        "900 on stream KV_ants_humans, which takes 1000: once full, no daemon can watch bucket ants_humans"
    );
    assert_eq!(humans.fix, ["ants init"]);
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
    let roster = a_js.get_key_value(ants_proto::ROSTER_BUCKET).await.unwrap();
    let now = chrono::Utc::now();
    let mut entry = serde_json::json!({
        "ant": "dev-a", "machine": "m1", "harness": "copilot", "started_at": now, "heartbeat_at": now,
    });
    roster.put("dev-a", serde_json::to_vec(&entry).unwrap().into()).await.unwrap();

    let manager = UserId::channel_manager();
    let creds = server.dir().join("channel-manager.creds");
    auth::issue_channel_manager(&server.dir().join("auth"), None, &creds).unwrap();
    let (_, m_js) = bus::connect(&server.url, Some(&creds), None, &manager, "test").await.unwrap();
    let published = ants_daemon::direct_kv::DirectKv::new(&m_js, None, ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    let key = ants_proto::channel_manager_serving_key();
    published.put(&key, versions::serving().await).await.expect("the channel manager publishes its version");

    let hook = "github".parse::<UserId>().unwrap();
    let creds = server.dir().join("github.creds");
    let to = [ants_proto::Destination::Ant(dev_a.clone())];
    let permissions = ants_proto::Permissions::for_webhook(&hook, &to);
    auth::issue_with(&server.dir().join("auth"), &hook, crate::revoke::Kind::Webhook, permissions, &creds).unwrap();
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
    let old = "the daemon of dev-a on m1 (no version published";
    assert!(check.detail.starts_with(&format!("older than permissions version {PERMISSIONS_VERSION}: {old}")));
    assert!(check.detail.contains("mixed: "), "{check}");
    assert!(!check.detail.contains("the channel manager on") && !check.detail.contains("webhook github on"));
    assert!(check.fix[0].starts_with("upgrade ants where they run"), "{check}");

    entry["version"] = ants_proto::ANTS_VERSION.into();
    entry["perms"] = PERMISSIONS_VERSION.0.into();
    roster.put("dev-a", serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
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
fn harness_versions_warn_of_drift_across_machines() {
    let now = chrono::Utc::now();
    let entry = |ant: &str, machine: &str, version: Option<&str>| {
        let mut json = serde_json::json!({
            "ant": ant, "machine": machine, "harness": "copilot", "started_at": now, "heartbeat_at": now,
        });
        if let Some(version) = version {
            json["agent"] = serde_json::json!({"name": "copilot", "version": version});
        }
        serde_json::from_value::<ants_proto::RosterEntry>(json).unwrap()
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

#[tokio::test]
async fn a_missing_harness_fails_with_its_fix() {
    let dir = ants_test_support::scratch_dir("doctor-harness").canonicalize().unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"ghost\"\n\
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

#[test]
fn ants_sharing_a_git_workdir_with_worktrees_turned_off_are_warned() {
    let dir = ants_test_support::scratch_dir("doctor-shared-workdir").canonicalize().unwrap();
    std::fs::create_dir(dir.join(".git")).unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "messenger = false\n\
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
        "messenger = false\nnats = \"nats://127.0.0.1:1\"\n[[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n",
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
    };

    let checks = check_leaf(&setup, Leaf::read(&leaf_config), &loaded.ants, None, None, 0).await;
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
    let dest = match std::env::consts::OS {
        "macos" => home.join("Library/LaunchAgents"),
        "linux" => home.join(".config/systemd/user"),
        _ => return false,
    };
    let out = std::process::Command::new("sh")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/install-units.sh"))
        .arg("--out")
        .arg(&dest)
        .args(["--nats-server", "/bin/sh", "--no-daemon", "--nats-conf"])
        .arg(home.join("leaf.conf"))
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
    let (serve, from, to) = match std::env::consts::OS {
        "macos" => (
            home.join("Library/LaunchAgents/ants.bridge-serve.plist"),
            "<string>serve</string>".to_owned(),
            format!("<string>serve</string><string>--listen</string><string>127.0.0.1:{port}</string>"),
        ),
        _ => (
            home.join(".config/systemd/user/ants-bridge-serve.service"),
            "bridge serve --token-file".to_owned(),
            format!("bridge serve --listen 127.0.0.1:{port} --token-file"),
        ),
    };
    let text = std::fs::read_to_string(&serve).unwrap();
    assert!(text.contains(&from), "{text}");
    std::fs::write(&serve, text.replace(&from, &to)).unwrap();

    let checks = transports(&home).await;
    for check in ["bridge service", "bridge token", "bridge serve service", "bridge serve token"] {
        assert_eq!(one(&checks, check).status, Status::Ok, "{checks:#?}");
    }
    assert_eq!(one(&checks, "bridge url").detail, format!("{url} answers as a bridge taking the token"));
    let serving = one(&checks, "bridge serve url");
    assert_eq!(serving.detail, format!("http://127.0.0.1:{port} answers as a bridge taking the token"));

    private(&token, "guess\n");
    let checks = transports(&home).await;
    let refused = one(&checks, "bridge url");
    assert_eq!(
        (refused.status, refused.detail.as_str()),
        (Status::Fail, &*format!("{url} refuses the token: missing or wrong bridge token"))
    );
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
    assert!(named(&check_services(&setup, &mut stopped()), "hub service").is_empty(), "nothing without nats.conf");

    std::fs::write(&hub_config, "listen: 127.0.0.1:4222\n").unwrap();
    let checks = check_services(&setup, &mut stopped());
    let missing = one(&checks, "hub service");
    assert_eq!(missing.status, Status::Warn, "{missing}");
    assert_eq!(missing.fix, [format!("scripts/install-units.sh --nats-conf {}", hub_config.display())]);
    assert_eq!(one(&checks, "leaf node service").status, Status::Skip, "{checks:#?}");

    if !install_units(&home, &["--nats-conf", hub_config.to_str().unwrap()]) {
        return;
    }
    let checks = check_services(&setup, &mut stopped());
    let down = one(&checks, "hub service");
    assert_eq!(down.status, Status::Fail, "{down}");
    assert_eq!(down.detail, format!("installed for {}, not running", hub_config.display()));
    let start = if cfg!(target_os = "macos") {
        "launchctl kickstart gui/501/ants.nats"
    } else {
        "systemctl --user start ants-nats"
    };
    assert_eq!(down.fix, [start]);
    assert_eq!(one(&check_services(&setup, &mut running(&home)), "hub service").status, Status::Ok);

    std::fs::write(home.join("leaf.conf"), "").unwrap();
    let checks = check_services(&setup, &mut running(&home));
    assert!(named(&checks, "hub service").is_empty(), "the leaf node is the machine's nats-server: {checks:#?}");
    assert_eq!(one(&checks, "leaf node service").status, Status::Warn, "{checks:#?}");
    std::fs::remove_dir_all(&home).unwrap();
}
