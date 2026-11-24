//! Tests of the doctor's hub checks: the stream and buckets `ants init` makes, the server's version, its
//! consumers and storage, the versions allowing renewal, the resolver and the store account.

use super::*;

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

#[tokio::test]
async fn a_bucket_allowing_rollups_fails_the_hub_check_until_init() {
    let Some(server) = Server::start() else { return };
    let alice = "alice".parse::<UserId>().unwrap();
    let setup = setup(&server, Some(server.creds_as(&alice, Role::Human)));
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let rights = Rights::from_creds(setup.creds.as_deref()).unwrap();
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let stream = js.get_stream(hub::Object::TrailRuns.stream_name()).await.unwrap();
    let config = jetstream::stream::Config { allow_rollup: true, ..stream.cached_info().config.clone() };
    js.update_stream(&config).await.unwrap();

    let checks = Box::pin(run(&setup)).await;
    let hub = named(&checks, "hub");
    assert_eq!(hub.len(), 1, "{checks:#?}");
    assert_eq!(hub[0].status, Status::Fail);
    assert!(hub[0].detail.starts_with("bucket ants_trail_runs allows rollups"), "{}", hub[0]);
    assert_eq!(hub[0].fix, ["ants init"]);
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let checks = Box::pin(run(&setup)).await;
    assert_eq!(named(&checks, "hub")[0].status, Status::Ok, "{checks:#?}");
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
    crate::test_support::issue_with(
        &server.dir().join("auth"),
        &hook,
        crate::credentials::issued::Kind::Webhook,
        permissions,
        &creds,
    )
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

/// The store account check fails on a hub whose team's account holds JetStream, as before the store account, and
/// passes once its streams are moved into the store account; a team without a store account fails it locally.
#[tokio::test]
async fn the_store_account_check_fails_until_the_hub_keeps_jetstream_in_the_store_account() {
    let Some(mut hub) = Server::start_before_store() else { return };
    let setup = |hub: &Server| Setup {
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
    let check = check_store_account(&setup(&hub), false).await;
    assert_eq!(check.status, Status::Skip, "no member connected yet: {check}");
    let human = UserId::human();
    let (client, _js) = hub.connect(&human).await;
    let check = check_store_account(&setup(&hub), false).await;
    assert_eq!(check.status, Status::Fail, "{check}");
    assert!(check.detail.contains("the team's account holds JetStream"), "{check}");
    assert!(check.fix[0].contains("ants auth move-stores --store-dir DIR"), "{check}");
    assert_eq!(check_store_account(&setup(&hub), true).await.status, Status::Skip, "through the leaf node");

    drop(client);
    hub.stop();
    let team = hub.dir().join("auth");
    auth::write_server_conf(&team, &auth::Revocations::default()).unwrap();
    auth::move_stores(&hub.dir().join("js"), &team.join(auth::SERVER_CONF), false).unwrap();
    hub.start_again();
    let (_client, _js) = hub.connect(&human).await;
    let check = check_store_account(&setup(&hub), false).await;
    assert_eq!(check.status, Status::Ok, "{check}");

    std::fs::remove_file(team.join(auth::STORE_SEED)).unwrap();
    let check = check_store_account(&setup(&hub), false).await;
    assert_eq!(check.status, Status::Fail, "{check}");
    assert!(check.detail.contains("has no store account") && check.fix[0].starts_with("ants auth init"), "{check}");
}
